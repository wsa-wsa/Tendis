// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <grpc/grpc.h>
#include <grpcpp/channel.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

namespace tendisplus {
namespace remote_compaction {

// ============================================================================
// Worker 状态
// ============================================================================
enum class WorkerStatus {
  kUnknown = 0,
  kOnline = 1,    // 在线可用
  kOffline = 2,   // 离线
  kBusy = 3,      // 忙碌（超过阈值）
  kDraining = 4,  // 排空中（不接受新任务）
};

inline const char* WorkerStatusToString(WorkerStatus status) {
  switch (status) {
    case WorkerStatus::kOnline:
      return "Online";
    case WorkerStatus::kOffline:
      return "Offline";
    case WorkerStatus::kBusy:
      return "Busy";
    case WorkerStatus::kDraining:
      return "Draining";
    default:
      return "Unknown";
  }
}

// ============================================================================
// Worker 信息
// ============================================================================
struct WorkerInfo {
  std::string worker_id;   // Worker 唯一标识
  std::string address;     // gRPC 地址 (host:port)
  WorkerStatus status = WorkerStatus::kUnknown;

  // 负载信息
  int64_t active_tasks = 0;       // 当前活跃任务数
  int64_t max_concurrent = 0;     // 最大并发任务数（从 CSA 获取或配置）
  int64_t total_completed = 0;    // 累计完成任务数
  int64_t total_failed = 0;       // 累计失败任务数

  // 健康检查
  std::chrono::steady_clock::time_point last_heartbeat;
  int32_t consecutive_failures = 0;  // 连续失败次数
  int64_t avg_latency_ms = 0;        // 平均响应延迟

  // gRPC channel (cached)
  std::shared_ptr<grpc::Channel> channel;

  // 计算负载率
  double LoadRatio() const {
    if (max_concurrent <= 0)
      return 1.0;
    return static_cast<double>(active_tasks) / max_concurrent;
  }

  // 是否可用
  bool IsAvailable() const {
    return status == WorkerStatus::kOnline && active_tasks < max_concurrent;
  }

  std::string ToString() const;
};

// ============================================================================
// 负载均衡策略
// ============================================================================
enum class LoadBalancePolicy {
  kRoundRobin = 0,    // 轮询
  kLeastLoaded = 1,   // 最小负载
  kRandom = 2,        // 随机
  kWeightedRandom = 3 // 加权随机（根据可用容量）
};

inline const char* LoadBalancePolicyToString(LoadBalancePolicy policy) {
  switch (policy) {
    case LoadBalancePolicy::kRoundRobin:
      return "RoundRobin";
    case LoadBalancePolicy::kLeastLoaded:
      return "LeastLoaded";
    case LoadBalancePolicy::kRandom:
      return "Random";
    case LoadBalancePolicy::kWeightedRandom:
      return "WeightedRandom";
    default:
      return "Unknown";
  }
}

// ============================================================================
// Worker 管理器配置
// ============================================================================
struct WorkerManagerConfig {
  // Worker 地址列表（逗号分隔）
  std::string worker_addresses;

  // 负载均衡策略
  LoadBalancePolicy load_balance_policy = LoadBalancePolicy::kLeastLoaded;

  // 健康检查配置
  int32_t health_check_interval_sec = 10;  // 健康检查间隔
  int32_t health_check_timeout_ms = 3000;  // 健康检查超时
  int32_t max_consecutive_failures = 3;    // 最大连续失败次数后标记离线

  // 默认 Worker 配置
  int64_t default_max_concurrent = 5;  // 默认最大并发任务数

  // gRPC 配置
  int64_t grpc_max_message_size = 16 * 1024 * 1024;  // 默认 16MB
};

// ============================================================================
// Worker 事件回调
// ============================================================================
enum class WorkerEvent {
  kRegistered,    // 新 Worker 注册
  kUnregistered,  // Worker 注销
  kOnline,        // Worker 上线
  kOffline,       // Worker 离线
  kBusy,          // Worker 忙碌
  kIdle,          // Worker 空闲
};

using WorkerEventCallback =
  std::function<void(const WorkerInfo& worker, WorkerEvent event)>;

// ============================================================================
// Worker 管理器
// ============================================================================
class WorkerManager {
 public:
  explicit WorkerManager(const WorkerManagerConfig& config);
  ~WorkerManager();

  // 禁止拷贝
  WorkerManager(const WorkerManager&) = delete;
  WorkerManager& operator=(const WorkerManager&) = delete;

  // 生命周期管理
  void Start();
  void Stop();
  bool IsRunning() const {
    return running_.load();
  }

  // Worker 管理
  bool AddWorker(const std::string& address);
  bool RemoveWorker(const std::string& worker_id);
  void UpdateWorkerList(const std::string& addresses);

  // 获取 Worker
  std::shared_ptr<WorkerInfo> GetWorker(const std::string& worker_id) const;
  std::vector<std::shared_ptr<WorkerInfo>> GetAllWorkers() const;
  std::vector<std::shared_ptr<WorkerInfo>> GetAvailableWorkers() const;

  // 负载均衡：选择最佳 Worker
  std::shared_ptr<WorkerInfo> SelectWorker();

  // 任务分配/完成通知
  void OnTaskAssigned(const std::string& worker_id);
  void OnTaskCompleted(const std::string& worker_id, bool success);

  // 标记 Worker 繁忙（CSA 返回 busy 时调用）
  void MarkWorkerBusy(const std::string& worker_id);

  // 统计信息
  size_t GetWorkerCount() const;
  size_t GetOnlineWorkerCount() const;
  size_t GetTotalActiveTasks() const;

  // 设置事件回调
  void SetEventCallback(WorkerEventCallback callback);

  // 更新配置
  void UpdateConfig(const WorkerManagerConfig& config);

  // 获取或创建 gRPC channel
  std::shared_ptr<grpc::Channel> GetOrCreateChannel(
    const std::string& address);

 private:
  void HealthCheckLoop();
  bool CheckWorkerHealth(WorkerInfo& worker);
  void NotifyEvent(const WorkerInfo& worker, WorkerEvent event);

  // 负载均衡实现
  std::shared_ptr<WorkerInfo> SelectRoundRobin();
  std::shared_ptr<WorkerInfo> SelectLeastLoaded();
  std::shared_ptr<WorkerInfo> SelectRandom();
  std::shared_ptr<WorkerInfo> SelectWeightedRandom();

  // 解析地址列表
  std::vector<std::string> ParseAddresses(const std::string& addresses);

  // 生成 Worker ID
  std::string GenerateWorkerId(const std::string& address);

  WorkerManagerConfig config_;
  std::atomic<bool> running_{false};

  // Worker 注册表
  std::unordered_map<std::string, std::shared_ptr<WorkerInfo>> workers_;
  mutable std::mutex workers_mutex_;

  // 轮询计数器
  std::atomic<uint64_t> round_robin_counter_{0};

  // 事件回调
  WorkerEventCallback event_callback_;
  std::mutex callback_mutex_;

  // 健康检查线程
  std::unique_ptr<std::thread> health_check_thread_;
  std::condition_variable cv_;
  std::mutex cv_mutex_;
};

}  // namespace remote_compaction
}  // namespace tendisplus
