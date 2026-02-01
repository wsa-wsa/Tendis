// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include "tendisplus/storage/rocks/remote_compaction/worker_manager.h"

#include <algorithm>
#include <iostream>
#include <random>
#include <sstream>

#include "csa.grpc.pb.h"  // NOLINT(build/include_subdir)

namespace tendisplus {
namespace remote_compaction {

// ============================================================================
// WorkerInfo 实现
// ============================================================================

std::string WorkerInfo::ToString() const {
  std::ostringstream oss;
  oss << "WorkerInfo{"
      << "id=" << worker_id << ", address=" << address
      << ", status=" << WorkerStatusToString(status)
      << ", active_tasks=" << active_tasks << "/" << max_concurrent
      << ", load=" << (LoadRatio() * 100) << "%"
      << ", completed=" << total_completed << ", failed=" << total_failed
      << ", avg_latency=" << avg_latency_ms << "ms"
      << "}";
  return oss.str();
}

// ============================================================================
// WorkerManager 实现
// ============================================================================

WorkerManager::WorkerManager(const WorkerManagerConfig& config)
  : config_(config) {
  // 解析初始 Worker 地址列表
  if (!config_.worker_addresses.empty()) {
    auto addresses = ParseAddresses(config_.worker_addresses);
    for (const auto& addr : addresses) {
      AddWorker(addr);
    }
  }
}

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

  std::cout << "[WorkerManager] Started with " << GetWorkerCount()
            << " workers" << std::endl;
}

void WorkerManager::Stop() {
  if (!running_.exchange(false)) {
    return;  // 已经停止
  }

  // 通知健康检查线程退出
  {
    std::lock_guard<std::mutex> lock(cv_mutex_);
    cv_.notify_all();
  }

  // 等待健康检查线程结束
  if (health_check_thread_ && health_check_thread_->joinable()) {
    health_check_thread_->join();
  }

  std::cout << "[WorkerManager] Stopped" << std::endl;
}

bool WorkerManager::AddWorker(const std::string& address) {
  if (address.empty()) {
    return false;
  }

  std::string worker_id = GenerateWorkerId(address);

  std::lock_guard<std::mutex> lock(workers_mutex_);

  // 检查是否已存在
  if (workers_.find(worker_id) != workers_.end()) {
    std::cout << "[WorkerManager] Worker already exists: " << address
              << std::endl;
    return false;
  }

  // 创建新 Worker
  auto worker = std::make_shared<WorkerInfo>();
  worker->worker_id = worker_id;
  worker->address = address;
  worker->status = WorkerStatus::kUnknown;
  worker->max_concurrent = config_.default_max_concurrent;
  worker->last_heartbeat = std::chrono::steady_clock::now();

  // 创建 gRPC channel
  grpc::ChannelArguments channel_args;
  channel_args.SetMaxReceiveMessageSize(
    static_cast<int>(config_.grpc_max_message_size));
  channel_args.SetMaxSendMessageSize(
    static_cast<int>(config_.grpc_max_message_size));
  worker->channel = grpc::CreateCustomChannel(
    address, grpc::InsecureChannelCredentials(), channel_args);

  workers_[worker_id] = worker;

  std::cout << "[WorkerManager] Added worker: " << address << " (id="
            << worker_id << ")" << std::endl;

  NotifyEvent(*worker, WorkerEvent::kRegistered);
  return true;
}

bool WorkerManager::RemoveWorker(const std::string& worker_id) {
  std::lock_guard<std::mutex> lock(workers_mutex_);

  auto it = workers_.find(worker_id);
  if (it == workers_.end()) {
    return false;
  }

  auto worker = it->second;
  workers_.erase(it);

  std::cout << "[WorkerManager] Removed worker: " << worker->address
            << std::endl;

  NotifyEvent(*worker, WorkerEvent::kUnregistered);
  return true;
}

void WorkerManager::UpdateWorkerList(const std::string& addresses) {
  auto new_addresses = ParseAddresses(addresses);
  std::set<std::string> new_addr_set(new_addresses.begin(),
                                     new_addresses.end());

  // 获取当前 Workers
  std::vector<std::string> current_ids;
  {
    std::lock_guard<std::mutex> lock(workers_mutex_);
    for (const auto& pair : workers_) {
      current_ids.push_back(pair.first);
    }
  }

  // 移除不在新列表中的 Workers
  for (const auto& id : current_ids) {
    auto worker = GetWorker(id);
    if (worker && new_addr_set.find(worker->address) == new_addr_set.end()) {
      RemoveWorker(id);
    }
  }

  // 添加新 Workers
  for (const auto& addr : new_addresses) {
    AddWorker(addr);  // AddWorker 会检查是否已存在
  }
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

std::shared_ptr<WorkerInfo> WorkerManager::SelectWorker() {
  switch (config_.load_balance_policy) {
    case LoadBalancePolicy::kRoundRobin:
      return SelectRoundRobin();
    case LoadBalancePolicy::kLeastLoaded:
      return SelectLeastLoaded();
    case LoadBalancePolicy::kRandom:
      return SelectRandom();
    case LoadBalancePolicy::kWeightedRandom:
      return SelectWeightedRandom();
    default:
      return SelectLeastLoaded();
  }
}

std::shared_ptr<WorkerInfo> WorkerManager::SelectRoundRobin() {
  auto available = GetAvailableWorkers();
  if (available.empty()) {
    return nullptr;
  }

  uint64_t index = round_robin_counter_.fetch_add(1) % available.size();
  return available[index];
}

std::shared_ptr<WorkerInfo> WorkerManager::SelectLeastLoaded() {
  auto available = GetAvailableWorkers();
  if (available.empty()) {
    return nullptr;
  }

  // 选择负载最低的 Worker
  auto it = std::min_element(
    available.begin(),
    available.end(),
    [](const std::shared_ptr<WorkerInfo>& a,
       const std::shared_ptr<WorkerInfo>& b) {
      return a->LoadRatio() < b->LoadRatio();
    });

  return *it;
}

std::shared_ptr<WorkerInfo> WorkerManager::SelectRandom() {
  auto available = GetAvailableWorkers();
  if (available.empty()) {
    return nullptr;
  }

  static thread_local std::random_device rd;
  static thread_local std::mt19937 gen(rd());
  std::uniform_int_distribution<size_t> dist(0, available.size() - 1);

  return available[dist(gen)];
}

std::shared_ptr<WorkerInfo> WorkerManager::SelectWeightedRandom() {
  auto available = GetAvailableWorkers();
  if (available.empty()) {
    return nullptr;
  }

  // 计算权重（可用容量）
  std::vector<double> weights;
  double total_weight = 0;
  for (const auto& worker : available) {
    double weight = worker->max_concurrent - worker->active_tasks;
    if (weight < 1)
      weight = 1;
    weights.push_back(weight);
    total_weight += weight;
  }

  // 加权随机选择
  static thread_local std::random_device rd;
  static thread_local std::mt19937 gen(rd());
  std::uniform_real_distribution<double> dist(0, total_weight);

  double random_value = dist(gen);
  double cumulative = 0;
  for (size_t i = 0; i < available.size(); ++i) {
    cumulative += weights[i];
    if (random_value <= cumulative) {
      return available[i];
    }
  }

  return available.back();
}

void WorkerManager::OnTaskAssigned(const std::string& worker_id) {
  auto worker = GetWorker(worker_id);
  if (worker) {
    worker->active_tasks++;
    if (worker->active_tasks >= worker->max_concurrent) {
      worker->status = WorkerStatus::kBusy;
      NotifyEvent(*worker, WorkerEvent::kBusy);
    }
  }
}

void WorkerManager::OnTaskCompleted(const std::string& worker_id, bool success) {
  auto worker = GetWorker(worker_id);
  if (worker) {
    if (worker->active_tasks > 0) {
      worker->active_tasks--;
    }
    if (success) {
      worker->total_completed++;
    } else {
      worker->total_failed++;
    }

    // 如果之前是 Busy 状态，现在有空余则变为 Online
    if (worker->status == WorkerStatus::kBusy &&
        worker->active_tasks < worker->max_concurrent) {
      worker->status = WorkerStatus::kOnline;
      NotifyEvent(*worker, WorkerEvent::kIdle);
    }
  }
}

void WorkerManager::MarkWorkerBusy(const std::string& worker_id) {
  auto worker = GetWorker(worker_id);
  if (worker && worker->status == WorkerStatus::kOnline) {
    worker->status = WorkerStatus::kBusy;
    // 设置 active_tasks 为 max，这样 LoadRatio() 返回 100%
    worker->active_tasks = worker->max_concurrent;
    NotifyEvent(*worker, WorkerEvent::kBusy);
  }
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

size_t WorkerManager::GetTotalActiveTasks() const {
  std::lock_guard<std::mutex> lock(workers_mutex_);
  size_t total = 0;
  for (const auto& pair : workers_) {
    total += pair.second->active_tasks;
  }
  return total;
}

void WorkerManager::SetEventCallback(WorkerEventCallback callback) {
  std::lock_guard<std::mutex> lock(callback_mutex_);
  event_callback_ = std::move(callback);
}

void WorkerManager::UpdateConfig(const WorkerManagerConfig& config) {
  config_ = config;

  // 更新 Worker 列表
  if (!config.worker_addresses.empty()) {
    UpdateWorkerList(config.worker_addresses);
  }
}

std::shared_ptr<grpc::Channel> WorkerManager::GetOrCreateChannel(
  const std::string& address) {
  std::string worker_id = GenerateWorkerId(address);
  auto worker = GetWorker(worker_id);
  if (worker && worker->channel) {
    return worker->channel;
  }

  // 创建新 channel
  grpc::ChannelArguments channel_args;
  channel_args.SetMaxReceiveMessageSize(
    static_cast<int>(config_.grpc_max_message_size));
  channel_args.SetMaxSendMessageSize(
    static_cast<int>(config_.grpc_max_message_size));
  return grpc::CreateCustomChannel(
    address, grpc::InsecureChannelCredentials(), channel_args);
}

void WorkerManager::HealthCheckLoop() {
  std::cout << "[WorkerManager] Health check thread started" << std::endl;

  while (running_.load()) {
    // 获取所有 Workers
    auto workers = GetAllWorkers();

    for (auto& worker : workers) {
      if (!running_.load())
        break;

      bool healthy = CheckWorkerHealth(*worker);

      if (healthy) {
        worker->consecutive_failures = 0;
        if (worker->status == WorkerStatus::kOffline ||
            worker->status == WorkerStatus::kUnknown) {
          worker->status = WorkerStatus::kOnline;
          std::cout << "[WorkerManager] Worker online: " << worker->address
                    << std::endl;
          NotifyEvent(*worker, WorkerEvent::kOnline);
        }
      } else {
        worker->consecutive_failures++;
        if (worker->consecutive_failures >= config_.max_consecutive_failures) {
          if (worker->status != WorkerStatus::kOffline) {
            worker->status = WorkerStatus::kOffline;
            std::cout << "[WorkerManager] Worker offline: " << worker->address
                      << " (consecutive failures: "
                      << worker->consecutive_failures << ")" << std::endl;
            NotifyEvent(*worker, WorkerEvent::kOffline);
          }
        }
      }

      worker->last_heartbeat = std::chrono::steady_clock::now();
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

bool WorkerManager::CheckWorkerHealth(WorkerInfo& worker) {
  if (!worker.channel) {
    return false;
  }

  // 检查 channel 连接状态
  auto state = worker.channel->GetState(true);  // try_to_connect = true

  // 等待连接
  auto deadline =
    std::chrono::system_clock::now() +
    std::chrono::milliseconds(config_.health_check_timeout_ms);

  bool connected = worker.channel->WaitForConnected(deadline);

  if (!connected) {
    return false;
  }

  // 可选：发送一个简单的 ping 请求
  // 这里简单地检查连接状态即可
  state = worker.channel->GetState(false);
  return (state == GRPC_CHANNEL_READY || state == GRPC_CHANNEL_IDLE);
}

void WorkerManager::NotifyEvent(const WorkerInfo& worker, WorkerEvent event) {
  WorkerEventCallback callback;
  {
    std::lock_guard<std::mutex> lock(callback_mutex_);
    callback = event_callback_;
  }

  if (callback) {
    callback(worker, event);
  }
}

std::vector<std::string> WorkerManager::ParseAddresses(
  const std::string& addresses) {
  std::vector<std::string> result;
  std::stringstream ss(addresses);
  std::string addr;

  while (std::getline(ss, addr, ',')) {
    // 去除首尾空格
    size_t start = addr.find_first_not_of(" \t");
    size_t end = addr.find_last_not_of(" \t");
    if (start != std::string::npos && end != std::string::npos) {
      addr = addr.substr(start, end - start + 1);
      if (!addr.empty()) {
        result.push_back(addr);
      }
    }
  }

  return result;
}

std::string WorkerManager::GenerateWorkerId(const std::string& address) {
  // 使用地址作为 ID（去除特殊字符）
  std::string id = address;
  std::replace(id.begin(), id.end(), ':', '_');
  std::replace(id.begin(), id.end(), '.', '_');
  return "worker_" + id;
}

}  // namespace remote_compaction
}  // namespace tendisplus
