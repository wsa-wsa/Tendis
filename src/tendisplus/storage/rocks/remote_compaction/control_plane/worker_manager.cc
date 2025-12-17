// Copyright (c) 2024-present, Tencent Inc. All rights reserved.
// Worker 管理器实现

#include "worker_manager.h"

#include <algorithm>
#include <iostream>
#include <sstream>

namespace tendisplus {
namespace control_plane {

// ============================================================================
// WorkerInfo 辅助方法实现
// ============================================================================

bool WorkerInfo::CanAcceptTask(const ResourceRequirement& requirement) const {
  if (status != WorkerStatus::kOnline) {
    return false;
  }
  
  return resources.AvailableCpuCores() >= requirement.cpu_cores &&
         resources.AvailableMemoryMb() >= requirement.memory_mb &&
         resources.AvailableDiskMb() >= requirement.disk_mb;
}

std::string WorkerInfo::ToString() const {
  std::ostringstream oss;
  oss << "Worker{id=" << worker_id
      << ", addr=" << address
      << ", status=" << WorkerStatusToString(status)
      << ", tasks=" << running_task_ids.size()
      << ", load=" << load.GetLoadScore() << "%"
      << "}";
  return oss.str();
}

std::string WorkerResources::ToString() const {
  std::ostringstream oss;
  oss << "Resources{cpu=" << used_cpu_cores << "/" << total_cpu_cores
      << ", mem=" << used_memory_mb << "/" << total_memory_mb << "MB"
      << ", disk=" << used_disk_mb << "/" << total_disk_mb << "MB"
      << "}";
  return oss.str();
}

std::string WorkerLoad::ToString() const {
  std::ostringstream oss;
  oss << "Load{cpu=" << cpu_usage_percent << "%"
      << ", mem=" << memory_usage_percent << "%"
      << ", disk_io=" << disk_io_percent << "%"
      << ", net_io=" << network_io_percent << "%"
      << ", tasks=" << active_task_count
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
  
  std::cout << "[WorkerManager] Starting worker manager" << std::endl;
  
  // 启动健康检查线程
  health_check_thread_ = std::make_unique<std::thread>([this]() {
    HealthCheckLoop();
  });
  
  // 启动服务发现线程 (如果启用)
  if (config_.enable_auto_discovery) {
    discovery_thread_ = std::make_unique<std::thread>([this]() {
      DiscoveryLoop();
    });
  }
}

void WorkerManager::Stop() {
  if (!running_.exchange(false)) {
    return;  // 已经停止
  }
  
  cv_.notify_all();
  
  if (health_check_thread_ && health_check_thread_->joinable()) {
    health_check_thread_->join();
  }
  if (discovery_thread_ && discovery_thread_->joinable()) {
    discovery_thread_->join();
  }
  
  std::cout << "[WorkerManager] Worker manager stopped" << std::endl;
}

bool WorkerManager::RegisterWorker(const WorkerInfo& worker) {
  std::lock_guard<std::mutex> lock(workers_mutex_);
  
  if (workers_.size() >= config_.max_workers) {
    std::cerr << "[WorkerManager] Max workers reached, cannot register: "
              << worker.worker_id << std::endl;
    return false;
  }
  
  WorkerInfo new_worker = worker;
  new_worker.registered_at = std::chrono::system_clock::now();
  new_worker.last_heartbeat = new_worker.registered_at;
  new_worker.status = WorkerStatus::kOnline;
  
  workers_[worker.worker_id] = new_worker;
  
  std::cout << "[WorkerManager] Worker registered: " << worker.worker_id
            << " at " << worker.address << std::endl;
  
  NotifyEvent(new_worker, WorkerEvent::kRegistered);
  return true;
}

bool WorkerManager::UnregisterWorker(const std::string& worker_id) {
  std::lock_guard<std::mutex> lock(workers_mutex_);
  
  auto it = workers_.find(worker_id);
  if (it == workers_.end()) {
    return false;
  }
  
  WorkerInfo worker = it->second;
  workers_.erase(it);
  
  std::cout << "[WorkerManager] Worker unregistered: " << worker_id << std::endl;
  
  // 通知事件 (解锁后)
  {
    std::lock_guard<std::mutex> cb_lock(callbacks_mutex_);
    for (const auto& callback : event_callbacks_) {
      callback(worker, WorkerEvent::kLost);
    }
  }
  
  return true;
}

bool WorkerManager::ProcessHeartbeat(const std::string& worker_id,
                                     const WorkerLoad& load,
                                     const WorkerResources& resources) {
  std::lock_guard<std::mutex> lock(workers_mutex_);
  
  auto it = workers_.find(worker_id);
  if (it == workers_.end()) {
    std::cerr << "[WorkerManager] Heartbeat from unknown worker: " 
              << worker_id << std::endl;
    return false;
  }
  
  it->second.last_heartbeat = std::chrono::system_clock::now();
  it->second.heartbeat_count++;
  it->second.load = load;
  it->second.resources = resources;
  
  // 更新状态
  if (it->second.status == WorkerStatus::kOffline) {
    it->second.status = WorkerStatus::kOnline;
    NotifyEvent(it->second, WorkerEvent::kStatusChanged);
  }
  
  return true;
}

std::optional<WorkerInfo> WorkerManager::GetWorker(
    const std::string& worker_id) const {
  std::lock_guard<std::mutex> lock(workers_mutex_);
  
  auto it = workers_.find(worker_id);
  if (it != workers_.end()) {
    return it->second;
  }
  return std::nullopt;
}

std::vector<WorkerInfo> WorkerManager::GetAllWorkers() const {
  std::lock_guard<std::mutex> lock(workers_mutex_);
  
  std::vector<WorkerInfo> result;
  result.reserve(workers_.size());
  for (const auto& [id, worker] : workers_) {
    result.push_back(worker);
  }
  return result;
}

std::vector<WorkerInfo> WorkerManager::GetAvailableWorkers() const {
  std::lock_guard<std::mutex> lock(workers_mutex_);
  
  std::vector<WorkerInfo> result;
  for (const auto& [id, worker] : workers_) {
    if (worker.IsAvailable()) {
      result.push_back(worker);
    }
  }
  return result;
}

std::vector<WorkerInfo> WorkerManager::GetWorkersByStatus(
    WorkerStatus status) const {
  std::lock_guard<std::mutex> lock(workers_mutex_);
  
  std::vector<WorkerInfo> result;
  for (const auto& [id, worker] : workers_) {
    if (worker.status == status) {
      result.push_back(worker);
    }
  }
  return result;
}

bool WorkerManager::UpdateWorkerStatus(const std::string& worker_id,
                                       WorkerStatus status) {
  std::lock_guard<std::mutex> lock(workers_mutex_);
  
  auto it = workers_.find(worker_id);
  if (it == workers_.end()) {
    return false;
  }
  
  WorkerStatus old_status = it->second.status;
  it->second.status = status;
  
  if (old_status != status) {
    std::cout << "[WorkerManager] Worker " << worker_id 
              << " status changed: " << WorkerStatusToString(old_status)
              << " -> " << WorkerStatusToString(status) << std::endl;
    NotifyEvent(it->second, WorkerEvent::kStatusChanged);
  }
  
  return true;
}

bool WorkerManager::AssignTaskToWorker(const std::string& worker_id,
                                       const std::string& task_id,
                                       const ResourceRequirement& requirement) {
  std::lock_guard<std::mutex> lock(workers_mutex_);
  
  auto it = workers_.find(worker_id);
  if (it == workers_.end()) {
    return false;
  }
  
  // 更新资源使用
  it->second.resources.used_cpu_cores += requirement.cpu_cores;
  it->second.resources.used_memory_mb += requirement.memory_mb;
  it->second.resources.used_disk_mb += requirement.disk_mb;
  
  // 添加到运行任务列表
  it->second.running_task_ids.push_back(task_id);
  
  // 更新负载
  it->second.load.active_task_count = it->second.running_task_ids.size();
  
  std::cout << "[WorkerManager] Task " << task_id 
            << " assigned to worker " << worker_id << std::endl;
  
  NotifyEvent(it->second, WorkerEvent::kTaskAssigned);
  return true;
}

bool WorkerManager::ReleaseTaskFromWorker(const std::string& worker_id,
                                          const std::string& task_id,
                                          const ResourceRequirement& requirement) {
  std::lock_guard<std::mutex> lock(workers_mutex_);
  
  auto it = workers_.find(worker_id);
  if (it == workers_.end()) {
    return false;
  }
  
  // 释放资源
  it->second.resources.used_cpu_cores = 
      std::max(0u, it->second.resources.used_cpu_cores - requirement.cpu_cores);
  it->second.resources.used_memory_mb = 
      std::max(0ul, it->second.resources.used_memory_mb - requirement.memory_mb);
  it->second.resources.used_disk_mb = 
      std::max(0ul, it->second.resources.used_disk_mb - requirement.disk_mb);
  
  // 从运行任务列表中移除
  auto& tasks = it->second.running_task_ids;
  tasks.erase(std::remove(tasks.begin(), tasks.end(), task_id), tasks.end());
  
  // 更新负载
  it->second.load.active_task_count = tasks.size();
  
  // 更新统计
  it->second.total_tasks_completed++;
  
  std::cout << "[WorkerManager] Task " << task_id 
            << " released from worker " << worker_id << std::endl;
  
  NotifyEvent(it->second, WorkerEvent::kTaskCompleted);
  return true;
}

size_t WorkerManager::GetWorkerCount() const {
  std::lock_guard<std::mutex> lock(workers_mutex_);
  return workers_.size();
}

size_t WorkerManager::GetOnlineWorkerCount() const {
  std::lock_guard<std::mutex> lock(workers_mutex_);
  
  size_t count = 0;
  for (const auto& [id, worker] : workers_) {
    if (worker.status == WorkerStatus::kOnline) {
      count++;
    }
  }
  return count;
}

WorkerResources WorkerManager::GetTotalResources() const {
  std::lock_guard<std::mutex> lock(workers_mutex_);
  
  WorkerResources total;
  for (const auto& [id, worker] : workers_) {
    if (worker.status == WorkerStatus::kOnline) {
      total.total_cpu_cores += worker.resources.total_cpu_cores;
      total.total_memory_mb += worker.resources.total_memory_mb;
      total.total_disk_mb += worker.resources.total_disk_mb;
    }
  }
  return total;
}

WorkerResources WorkerManager::GetAvailableResources() const {
  std::lock_guard<std::mutex> lock(workers_mutex_);
  
  WorkerResources available;
  for (const auto& [id, worker] : workers_) {
    if (worker.status == WorkerStatus::kOnline) {
      available.total_cpu_cores += worker.resources.AvailableCpuCores();
      available.total_memory_mb += worker.resources.AvailableMemoryMb();
      available.total_disk_mb += worker.resources.AvailableDiskMb();
    }
  }
  return available;
}

void WorkerManager::RegisterEventCallback(WorkerEventCallback callback) {
  std::lock_guard<std::mutex> lock(callbacks_mutex_);
  event_callbacks_.push_back(std::move(callback));
}

std::optional<std::string> WorkerManager::SelectBestWorker(
    const ResourceRequirement& requirement,
    const std::map<std::string, std::string>& preferred_labels) const {
  
  std::lock_guard<std::mutex> lock(workers_mutex_);
  
  const WorkerInfo* best_worker = nullptr;
  double best_score = std::numeric_limits<double>::max();
  
  for (const auto& [id, worker] : workers_) {
    if (!worker.CanAcceptTask(requirement)) {
      continue;
    }
    
    // 计算评分 (负载越低越好)
    double score = worker.load.GetLoadScore();
    
    // 标签匹配加分
    if (!preferred_labels.empty()) {
      int label_matches = 0;
      for (const auto& [key, value] : preferred_labels) {
        auto it = worker.labels.find(key);
        if (it != worker.labels.end() && it->second == value) {
          label_matches++;
        }
      }
      // 每匹配一个标签减少 10 分
      score -= label_matches * 10;
    }
    
    if (score < best_score) {
      best_score = score;
      best_worker = &worker;
    }
  }
  
  if (best_worker) {
    return best_worker->worker_id;
  }
  return std::nullopt;
}

void WorkerManager::HealthCheckLoop() {
  while (running_.load()) {
    auto now = std::chrono::system_clock::now();
    std::vector<std::string> offline_workers;
    
    {
      std::lock_guard<std::mutex> lock(workers_mutex_);
      
      for (auto& [id, worker] : workers_) {
        if (worker.status == WorkerStatus::kOffline) {
          continue;
        }
        
        auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
            now - worker.last_heartbeat).count();
        
        if (elapsed > config_.heartbeat_timeout_sec) {
          offline_workers.push_back(id);
        }
      }
    }
    
    // 标记离线 Worker
    for (const auto& worker_id : offline_workers) {
      MarkWorkerOffline(worker_id);
    }
    
    // 等待下一次检查
    std::unique_lock<std::mutex> lock(workers_mutex_);
    cv_.wait_for(lock, std::chrono::seconds(config_.health_check_interval_sec),
                 [this]() { return !running_.load(); });
  }
}

void WorkerManager::DiscoveryLoop() {
  while (running_.load()) {
    // TODO: 实现服务发现逻辑
    // 从服务发现端点获取 Worker 列表并注册
    
    std::this_thread::sleep_for(std::chrono::seconds(30));
  }
}

void WorkerManager::NotifyEvent(const WorkerInfo& worker, WorkerEvent event) {
  std::lock_guard<std::mutex> lock(callbacks_mutex_);
  for (const auto& callback : event_callbacks_) {
    try {
      callback(worker, event);
    } catch (const std::exception& e) {
      std::cerr << "[WorkerManager] Callback exception: " << e.what() << std::endl;
    }
  }
}

void WorkerManager::MarkWorkerOffline(const std::string& worker_id) {
  std::lock_guard<std::mutex> lock(workers_mutex_);
  
  auto it = workers_.find(worker_id);
  if (it == workers_.end()) {
    return;
  }
  
  if (it->second.status != WorkerStatus::kOffline) {
    std::cout << "[WorkerManager] Worker " << worker_id 
              << " marked as offline (heartbeat timeout)" << std::endl;
    
    it->second.status = WorkerStatus::kOffline;
    NotifyEvent(it->second, WorkerEvent::kLost);
  }
}

// ============================================================================
// WorkerClient 实现
// ============================================================================

WorkerClient::WorkerClient(const std::string& address)
    : address_(address) {
  // TODO: 初始化 gRPC channel
}

WorkerClient::~WorkerClient() = default;

bool WorkerClient::SendTask(const BackgroundTask& task) {
  // TODO: 实现 gRPC 调用
  std::cout << "[WorkerClient] Sending task " << task.task_id 
            << " to " << address_ << std::endl;
  return true;
}

bool WorkerClient::CancelTask(const std::string& task_id) {
  // TODO: 实现 gRPC 调用
  std::cout << "[WorkerClient] Cancelling task " << task_id 
            << " on " << address_ << std::endl;
  return true;
}

std::optional<TaskStatus> WorkerClient::QueryTaskStatus(
    const std::string& task_id) {
  // TODO: 实现 gRPC 调用
  return std::nullopt;
}

bool WorkerClient::HealthCheck() {
  // TODO: 实现 gRPC 调用
  return true;
}

std::optional<WorkerInfo> WorkerClient::GetWorkerInfo() {
  // TODO: 实现 gRPC 调用
  return std::nullopt;
}

}  // namespace control_plane
}  // namespace tendisplus
