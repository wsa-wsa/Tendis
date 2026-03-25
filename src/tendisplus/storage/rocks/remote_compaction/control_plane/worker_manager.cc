// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include "worker_manager.h"

#include <algorithm>
#include <iostream>
#include <random>
#include <sstream>
#include <set>

#include "control_plane.grpc.pb.h"

namespace tendisplus {
namespace control_plane {

// ============================================================================
// WorkerResources 实现
// ============================================================================

std::string WorkerResources::ToString() const {
  std::ostringstream oss;
  oss << "Resources{tasks=" << active_tasks << "/" << max_concurrent_tasks
      << ", load=" << (LoadRatio() * 100) << "%"
      << ", cpu=" << total_cpu_cores << ", mem=" << total_memory_mb << "MB}";
  return oss.str();
}

// ============================================================================
// WorkerInfo 实现
// ============================================================================

std::string WorkerInfo::ToString() const {
  std::ostringstream oss;
  oss << "Worker{id=" << worker_id << ", addr=" << address
      << ", status=" << WorkerStatusToString(status)
      << ", " << resources.ToString()
      << ", completed=" << total_completed << ", failed=" << total_failed
      << "}";
  return oss.str();
}

// ============================================================================
// WorkerManager 实现
// ============================================================================

WorkerManager::WorkerManager(const WorkerManagerConfig& config)
  : config_(config) {}

WorkerManager::~WorkerManager() {
  Stop();
}

void WorkerManager::Start() {
  if (running_.exchange(true)) {
    return;  // 已经在运行
  }

  // 启动健康检查线程
  health_check_thread_ = std::make_unique<std::thread>([this]() {
    HealthCheckLoop();
  });

  std::cout << "[WorkerManager] Control plane WorkerManager started"
            << std::endl;
}

void WorkerManager::Stop() {
  if (!running_.exchange(false)) {
    return;
  }

  {
    std::lock_guard<std::mutex> lock(cv_mutex_);
    cv_.notify_all();
  }

  if (health_check_thread_ && health_check_thread_->joinable()) {
    health_check_thread_->join();
  }

  std::cout << "[WorkerManager] Control plane WorkerManager stopped"
            << std::endl;
}

std::string WorkerManager::RegisterWorker(
  const std::string& address,
  const WorkerResources& resources,
  const std::map<std::string, std::string>& labels,
  const std::string& requested_id) {
  std::lock_guard<std::mutex> lock(workers_mutex_);

  // 生成或使用请求的 worker_id
  std::string worker_id =
    requested_id.empty() ? GenerateWorkerId(address) : requested_id;

  // 检查是否已存在
  auto it = workers_.find(worker_id);
  if (it != workers_.end()) {
    // 更新现有 Worker
    auto& worker = it->second;
    worker->address = address;
    worker->resources = resources;
    worker->labels = labels;
    worker->status = WorkerStatus::kOnline;
    worker->last_heartbeat = std::chrono::steady_clock::now();
    worker->consecutive_failures = 0;

    std::cout << "[WorkerManager] Worker re-registered: " << worker->ToString()
              << std::endl;
    NotifyEvent(*worker, WorkerEvent::kOnline);
    return worker_id;
  }

  // 创建新 Worker
  auto worker = std::make_shared<WorkerInfo>();
  worker->worker_id = worker_id;
  worker->address = address;
  worker->resources = resources;
  worker->resources.max_concurrent_tasks =
    resources.max_concurrent_tasks > 0 ? resources.max_concurrent_tasks
                                       : config_.default_max_concurrent;
  worker->labels = labels;
  worker->status = WorkerStatus::kOnline;
  worker->register_time = std::chrono::steady_clock::now();
  worker->last_heartbeat = worker->register_time;

  workers_[worker_id] = worker;

  std::cout << "[WorkerManager] Worker registered: " << worker->ToString()
            << std::endl;
  NotifyEvent(*worker, WorkerEvent::kRegistered);

  return worker_id;
}

bool WorkerManager::UnregisterWorker(const std::string& worker_id,
                                     const std::string& reason) {
  std::lock_guard<std::mutex> lock(workers_mutex_);

  auto it = workers_.find(worker_id);
  if (it == workers_.end()) {
    return false;
  }

  auto worker = it->second;
  workers_.erase(it);

  std::cout << "[WorkerManager] Worker unregistered: " << worker->address
            << ", reason: " << reason << std::endl;
  NotifyEvent(*worker, WorkerEvent::kUnregistered);

  return true;
}

std::vector<std::string> WorkerManager::ProcessHeartbeat(
  const std::string& worker_id,
  const WorkerResources& resources,
  const std::vector<std::string>& active_task_ids) {
  std::vector<std::string> tasks_to_cancel;

  std::lock_guard<std::mutex> lock(workers_mutex_);

  auto it = workers_.find(worker_id);
  if (it == workers_.end()) {
    std::cerr << "[WorkerManager] Heartbeat from unknown worker: " << worker_id
              << std::endl;
    return tasks_to_cancel;
  }

  auto& worker = it->second;

  // 更新资源信息
  worker->resources = resources;
  worker->last_heartbeat = std::chrono::steady_clock::now();
  worker->consecutive_failures = 0;

  // 检查任务一致性
  // 找出 Worker 不知道但控制平面认为还在运行的任务
  std::set<std::string> worker_tasks(active_task_ids.begin(),
                                     active_task_ids.end());
  for (const auto& task_id : worker->active_task_ids) {
    if (worker_tasks.find(task_id) == worker_tasks.end()) {
      // 任务可能已丢失，需要重新调度
      tasks_to_cancel.push_back(task_id);
    }
  }

  // 更新活跃任务列表
  worker->active_task_ids = active_task_ids;

  // 更新状态
  if (worker->status == WorkerStatus::kOffline) {
    worker->status = WorkerStatus::kOnline;
    std::cout << "[WorkerManager] Worker back online: " << worker->address
              << std::endl;
    NotifyEvent(*worker, WorkerEvent::kOnline);
  }

  // 检查是否忙碌
  if (worker->resources.AvailableSlots() == 0) {
    if (worker->status != WorkerStatus::kBusy) {
      worker->status = WorkerStatus::kBusy;
      NotifyEvent(*worker, WorkerEvent::kBusy);
    }
  } else if (worker->status == WorkerStatus::kBusy) {
    worker->status = WorkerStatus::kOnline;
    NotifyEvent(*worker, WorkerEvent::kIdle);
  }

  return tasks_to_cancel;
}

std::shared_ptr<WorkerInfo> WorkerManager::GetWorker(
  const std::string& worker_id) const {
  std::lock_guard<std::mutex> lock(workers_mutex_);
  auto it = workers_.find(worker_id);
  if (it != workers_.end()) {
    return it->second;
  }
  return nullptr;
}

std::vector<std::shared_ptr<WorkerInfo>> WorkerManager::GetAllWorkers() const {
  std::vector<std::shared_ptr<WorkerInfo>> result;
  std::lock_guard<std::mutex> lock(workers_mutex_);
  result.reserve(workers_.size());
  for (const auto& pair : workers_) {
    result.push_back(pair.second);
  }
  return result;
}

std::vector<std::shared_ptr<WorkerInfo>>
WorkerManager::GetAvailableWorkers() const {
  std::vector<std::shared_ptr<WorkerInfo>> result;
  std::lock_guard<std::mutex> lock(workers_mutex_);
  for (const auto& pair : workers_) {
    if (pair.second->IsAvailable()) {
      result.push_back(pair.second);
    }
  }
  return result;
}

std::vector<std::shared_ptr<WorkerInfo>> WorkerManager::GetWorkersByStatus(
  WorkerStatus status) const {
  std::vector<std::shared_ptr<WorkerInfo>> result;
  std::lock_guard<std::mutex> lock(workers_mutex_);
  for (const auto& pair : workers_) {
    if (pair.second->status == status) {
      result.push_back(pair.second);
    }
  }
  return result;
}

std::shared_ptr<WorkerInfo> WorkerManager::SelectBestWorker() {
  auto available = GetAvailableWorkers();
  if (available.empty()) {
    return nullptr;
  }

  // 使用最小负载策略
  auto it = std::min_element(
    available.begin(),
    available.end(),
    [](const std::shared_ptr<WorkerInfo>& a,
       const std::shared_ptr<WorkerInfo>& b) {
      return a->resources.LoadRatio() < b->resources.LoadRatio();
    });

  return *it;
}

bool WorkerManager::AssignTask(const std::string& worker_id,
                               const std::string& task_id) {
  std::lock_guard<std::mutex> lock(workers_mutex_);

  auto it = workers_.find(worker_id);
  if (it == workers_.end()) {
    return false;
  }

  auto& worker = it->second;
  if (!worker->IsAvailable()) {
    return false;
  }

  worker->active_task_ids.push_back(task_id);
  worker->resources.active_tasks++;

  // 检查是否变为忙碌
  if (worker->resources.AvailableSlots() == 0) {
    worker->status = WorkerStatus::kBusy;
    NotifyEvent(*worker, WorkerEvent::kBusy);
  }

  return true;
}

bool WorkerManager::ReleaseTask(const std::string& worker_id,
                                const std::string& task_id,
                                bool success) {
  std::lock_guard<std::mutex> lock(workers_mutex_);

  auto it = workers_.find(worker_id);
  if (it == workers_.end()) {
    return false;
  }

  auto& worker = it->second;

  // 移除任务
  auto task_it = std::find(
    worker->active_task_ids.begin(), worker->active_task_ids.end(), task_id);
  if (task_it != worker->active_task_ids.end()) {
    worker->active_task_ids.erase(task_it);
    if (worker->resources.active_tasks > 0) {
      worker->resources.active_tasks--;
    }
  }

  // 更新统计
  if (success) {
    worker->total_completed++;
  } else {
    worker->total_failed++;
  }

  // 检查是否变为空闲
  if (worker->status == WorkerStatus::kBusy &&
      worker->resources.AvailableSlots() > 0) {
    worker->status = WorkerStatus::kOnline;
    NotifyEvent(*worker, WorkerEvent::kIdle);
  }

  return true;
}

size_t WorkerManager::GetWorkerCount() const {
  std::lock_guard<std::mutex> lock(workers_mutex_);
  return workers_.size();
}

size_t WorkerManager::GetOnlineWorkerCount() const {
  std::lock_guard<std::mutex> lock(workers_mutex_);
  size_t count = 0;
  for (const auto& pair : workers_) {
    if (pair.second->status == WorkerStatus::kOnline ||
        pair.second->status == WorkerStatus::kBusy) {
      count++;
    }
  }
  return count;
}

size_t WorkerManager::GetTotalAvailableSlots() const {
  std::lock_guard<std::mutex> lock(workers_mutex_);
  size_t total = 0;
  for (const auto& pair : workers_) {
    if (pair.second->IsAvailable()) {
      total += pair.second->resources.AvailableSlots();
    }
  }
  return total;
}

void WorkerManager::RegisterEventCallback(WorkerEventCallback callback) {
  std::lock_guard<std::mutex> lock(callbacks_mutex_);
  event_callbacks_.push_back(std::move(callback));
}

void WorkerManager::HealthCheckLoop() {
  std::cout << "[WorkerManager] Health check thread started" << std::endl;

  while (running_.load()) {
    {
      std::lock_guard<std::mutex> lock(workers_mutex_);
      for (auto& pair : workers_) {
        CheckWorkerTimeout(*pair.second);
      }
    }

    // 等待下一次检查
    {
      std::unique_lock<std::mutex> lock(cv_mutex_);
      cv_.wait_for(
        lock,
        std::chrono::seconds(config_.health_check_interval_sec),
        [this]() { return !running_.load(); });
    }
  }

  std::cout << "[WorkerManager] Health check thread stopped" << std::endl;
}

void WorkerManager::CheckWorkerTimeout(WorkerInfo& worker) {
  auto now = std::chrono::steady_clock::now();
  auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                   now - worker.last_heartbeat)
                   .count();

  if (elapsed > config_.heartbeat_timeout_sec) {
    if (worker.status != WorkerStatus::kOffline) {
      worker.status = WorkerStatus::kOffline;
      std::cout << "[WorkerManager] Worker timeout (no heartbeat for "
                << elapsed << "s): " << worker.address << std::endl;
      NotifyEvent(worker, WorkerEvent::kOffline);
    }
  }
}

void WorkerManager::NotifyEvent(const WorkerInfo& worker, WorkerEvent event) {
  std::vector<WorkerEventCallback> callbacks;
  {
    std::lock_guard<std::mutex> lock(callbacks_mutex_);
    callbacks = event_callbacks_;
  }

  for (const auto& callback : callbacks) {
    if (callback) {
      callback(worker, event);
    }
  }
}

std::string WorkerManager::GenerateWorkerId(const std::string& address) {
  uint64_t id = worker_id_counter_.fetch_add(1);
  std::string addr_part = address;
  std::replace(addr_part.begin(), addr_part.end(), ':', '_');
  std::replace(addr_part.begin(), addr_part.end(), '.', '_');
  return "worker_" + addr_part + "_" + std::to_string(id);
}

// ============================================================================
// CaaS-LSM: 推送模式实现
// ============================================================================

std::shared_ptr<grpc::Channel> WorkerManager::GetOrCreateCSAChannel(
  const std::string& address) {
  std::lock_guard<std::mutex> lock(channels_mutex_);
  
  auto it = csa_channels_.find(address);
  if (it != csa_channels_.end()) {
    return it->second;
  }
  
  // 创建新的 gRPC 连接
  auto channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
  csa_channels_[address] = channel;
  
  std::cout << "[WorkerManager] Created gRPC channel to CSA: " << address
            << std::endl;
  
  return channel;
}

bool WorkerManager::CheckCSAStatus(const std::string& worker_id) {
  auto worker = GetWorker(worker_id);
  if (!worker) {
    return false;
  }
  
  auto channel = GetOrCreateCSAChannel(worker->address);
  if (!channel) {
    return false;
  }
  
  // 检查连接状态
  auto state = channel->GetState(true);
  if (state == GRPC_CHANNEL_SHUTDOWN || state == GRPC_CHANNEL_TRANSIENT_FAILURE) {
    std::cout << "[WorkerManager] CSA channel unhealthy: " << worker->address
              << std::endl;
    return false;
  }
  
  // 通过 gRPC 调用 CSAService::CheckCSAStatus 获取详细状态
  auto stub = ::control_plane::CSAService::NewStub(channel);
  if (!stub) {
    std::cerr << "[WorkerManager] Failed to create CSAService stub for: "
              << worker->address << std::endl;
    return false;
  }

  ::control_plane::CSAStatusRequest request;
  ::control_plane::CSAStatusResponse response;
  grpc::ClientContext context;

  auto deadline =
    std::chrono::system_clock::now() + std::chrono::seconds(5);
  context.set_deadline(deadline);

  grpc::Status grpc_status =
    stub->CheckCSAStatus(&context, request, &response);

  if (!grpc_status.ok()) {
    std::cerr << "[WorkerManager] CheckCSAStatus RPC failed: "
              << grpc_status.error_message()
              << " (worker=" << worker_id << ")" << std::endl;
    return false;
  }

  // 更新 Worker 的资源信息
  {
    std::lock_guard<std::mutex> lock(workers_mutex_);
    auto it = workers_.find(worker_id);
    if (it != workers_.end()) {
      auto& w = it->second;
      w->resources.active_tasks = response.local_task_nums();
      if (response.total_memory_mb() > 0) {
        w->resources.total_memory_mb = response.total_memory_mb();
        w->resources.used_memory_mb = response.used_memory_mb();
      }
      if (response.total_disk_mb() > 0) {
        w->resources.total_disk_mb = response.total_disk_mb();
        w->resources.used_disk_mb = response.used_disk_mb();
      }
    }
  }

  return response.is_healthy();
}

bool WorkerManager::DistributeJobToCSA(const std::string& worker_id,
                                       const std::string& task_id,
                                       const std::string& compaction_args,
                                       const std::string& compaction_addition_info,
                                       const std::string& shared_fs_uri,
                                       int32_t start_level,
                                       double score) {
  auto worker = GetWorker(worker_id);
  if (!worker) {
    std::cerr << "[WorkerManager] Worker not found: " << worker_id << std::endl;
    return false;
  }
  
  if (worker->status != WorkerStatus::kOnline) {
    std::cerr << "[WorkerManager] Worker not online: " << worker_id << std::endl;
    return false;
  }
  
  auto channel = GetOrCreateCSAChannel(worker->address);
  if (!channel) {
    std::cerr << "[WorkerManager] Failed to create channel to CSA: "
              << worker->address << std::endl;
    return false;
  }
  
  // 创建 CSAService::Stub 并调用 DistributeCompactionJob RPC
  auto stub = ::control_plane::CSAService::NewStub(channel);
  if (!stub) {
    std::cerr << "[WorkerManager] Failed to create CSAService stub for: "
              << worker->address << std::endl;
    return false;
  }

  // 构建请求
  ::control_plane::DistributeJobRequest request;
  request.set_task_id(task_id);
  request.set_compaction_args(compaction_args);
  request.set_compaction_addition_info(compaction_addition_info);
  request.set_shared_fs_uri(shared_fs_uri);
  request.set_start_level(start_level);
  request.set_score(score);

  // 设置超时
  grpc::ClientContext context;
  auto deadline =
    std::chrono::system_clock::now() + std::chrono::seconds(30);
  context.set_deadline(deadline);

  // 调用 RPC
  ::control_plane::DistributeJobResponse response;
  grpc::Status grpc_status =
    stub->DistributeCompactionJob(&context, request, &response);

  if (!grpc_status.ok()) {
    std::cerr << "[WorkerManager] DistributeCompactionJob RPC failed: "
              << grpc_status.error_message()
              << " (worker=" << worker_id << ")" << std::endl;
    return false;
  }

  if (!response.accepted()) {
    std::cerr << "[WorkerManager] CSA rejected task: " << task_id
              << ", reason: " << response.error_message() << std::endl;
    return false;
  }
  
  std::cout << "[WorkerManager] DistributeJobToCSA success: task=" << task_id
            << ", worker=" << worker_id << ", start_level=" << start_level
            << ", score=" << score << std::endl;
  
  return true;
}

bool WorkerManager::DistributeBulkLoadShardToCSA(
  const std::string& worker_id,
  const std::string& task_id,
  const std::string& shard_id,
  uint32_t shard_index,
  int32_t source_type,
  const std::string& source_path,
  int32_t data_format,
  const std::string& key_range_start,
  const std::string& key_range_end,
  uint32_t slot_start,
  uint32_t slot_end,
  const std::string& shared_fs_uri,
  const std::string& sst_output_dir,
  int32_t compression,
  uint64_t target_sst_size,
  bool generate_binlog,
  uint32_t target_store_id,
  const std::string& target_db_path,
  int64_t rate_limit_bytes_per_sec,
  uint32_t timeout_sec) {
  auto worker = GetWorker(worker_id);
  if (!worker) {
    std::cerr << "[WorkerManager] BulkLoad: Worker not found: " << worker_id
              << std::endl;
    return false;
  }

  if (worker->status != WorkerStatus::kOnline) {
    std::cerr << "[WorkerManager] BulkLoad: Worker not online: " << worker_id
              << std::endl;
    return false;
  }

  auto channel = GetOrCreateCSAChannel(worker->address);
  if (!channel) {
    std::cerr << "[WorkerManager] BulkLoad: Failed to create channel to CSA: "
              << worker->address << std::endl;
    return false;
  }

  // 创建 CSAService::Stub 并调用 ExecuteBulkLoadShard RPC
  auto stub = ::control_plane::CSAService::NewStub(channel);
  if (!stub) {
    std::cerr << "[WorkerManager] BulkLoad: Failed to create stub for: "
              << worker->address << std::endl;
    return false;
  }

  // 构建 BulkLoadShardRequest
  ::control_plane::BulkLoadShardRequest request;
  request.set_task_id(task_id);
  request.set_shard_id(shard_id);
  request.set_shard_index(shard_index);
  request.set_source_type(
    static_cast<::control_plane::DataSourceType>(source_type));
  request.set_source_path(source_path);
  request.set_data_format(
    static_cast<::control_plane::DataFormat>(data_format));

  // Key 范围
  auto* key_range = request.mutable_key_range();
  key_range->set_start_key(key_range_start);
  key_range->set_end_key(key_range_end);
  key_range->set_slot_start(slot_start);
  key_range->set_slot_end(slot_end);

  // SST 配置
  request.set_shared_fs_uri(shared_fs_uri);
  request.set_sst_output_dir(sst_output_dir);
  request.set_compression(
    static_cast<::control_plane::CompressionType>(compression));
  request.set_target_sst_size(target_sst_size);
  request.set_generate_binlog(generate_binlog);
  request.set_target_store_id(target_store_id);
  request.set_target_db_path(target_db_path);

  // 资源控制
  request.set_rate_limit_bytes_per_sec(rate_limit_bytes_per_sec);
  request.set_timeout_sec(timeout_sec);

  // 设置 gRPC 超时 (Bulk Load 比 Compaction 可能更耗时)
  grpc::ClientContext context;
  auto deadline =
    std::chrono::system_clock::now() + std::chrono::seconds(60);
  context.set_deadline(deadline);

  // 调用 RPC
  ::control_plane::BulkLoadShardResponse response;
  grpc::Status grpc_status =
    stub->ExecuteBulkLoadShard(&context, request, &response);

  if (!grpc_status.ok()) {
    std::cerr << "[WorkerManager] ExecuteBulkLoadShard RPC failed: "
              << grpc_status.error_message()
              << " (worker=" << worker_id << ", task=" << task_id << ")"
              << std::endl;
    return false;
  }

  if (!response.accepted()) {
    std::cerr << "[WorkerManager] CSA rejected Bulk Load shard: "
              << shard_id << ", reason: " << response.error_message()
              << std::endl;
    return false;
  }

  std::cout << "[WorkerManager] DistributeBulkLoadShardToCSA success:"
            << " task=" << task_id
            << ", shard=" << shard_id
            << ", worker=" << worker_id
            << ", rows=" << response.total_rows_processed()
            << ", sst_files=" << response.generated_sst_files_size()
            << ", time_ms=" << response.execution_time_ms() << std::endl;

  return true;
}

bool WorkerManager::CancelCSATask(const std::string& worker_id,
                                  const std::string& task_id) {
  auto worker = GetWorker(worker_id);
  if (!worker) {
    std::cerr << "[WorkerManager] CancelCSATask: worker not found: "
              << worker_id << std::endl;
    return false;
  }
  
  auto channel = GetOrCreateCSAChannel(worker->address);
  if (!channel) {
    std::cerr << "[WorkerManager] CancelCSATask: failed to create channel to: "
              << worker->address << std::endl;
    return false;
  }
  
  // 创建 CSAService::Stub 并调用 CancelRunningTask RPC
  auto stub = ::control_plane::CSAService::NewStub(channel);
  if (!stub) {
    std::cerr << "[WorkerManager] CancelCSATask: failed to create stub for: "
              << worker->address << std::endl;
    return false;
  }

  // 构建请求
  ::control_plane::CancelRunningTaskRequest request;
  request.set_task_id(task_id);
  request.set_reason("Cancelled by Control Plane");

  // 设置超时
  grpc::ClientContext context;
  auto deadline =
    std::chrono::system_clock::now() + std::chrono::seconds(10);
  context.set_deadline(deadline);

  // 调用 RPC
  ::control_plane::CancelRunningTaskResponse response;
  grpc::Status grpc_status =
    stub->CancelRunningTask(&context, request, &response);

  if (!grpc_status.ok()) {
    std::cerr << "[WorkerManager] CancelRunningTask RPC failed: "
              << grpc_status.error_message()
              << " (worker=" << worker_id << ", task=" << task_id << ")"
              << std::endl;
    return false;
  }

  if (!response.success()) {
    std::cerr << "[WorkerManager] CSA failed to cancel task: " << task_id
              << ", reason: " << response.error_message() << std::endl;
    return false;
  }

  std::cout << "[WorkerManager] CancelCSATask success: task=" << task_id
            << ", worker=" << worker_id << std::endl;
  
  return true;
}

}  // namespace control_plane
}  // namespace tendisplus
