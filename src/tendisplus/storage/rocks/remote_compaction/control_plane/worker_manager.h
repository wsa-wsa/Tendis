// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

// Worker Manager for Control Plane
// Based on CaaS-LSM architecture

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

#include <grpcpp/grpcpp.h>

#include "task_model.h"

namespace tendisplus {
namespace control_plane {

// ============================================================================
// Worker 状态
// ============================================================================
enum class WorkerStatus {
  kUnknown = 0,
  kOnline = 1,       // 在线可用
  kOffline = 2,      // 离线
  kBusy = 3,         // 忙碌（达到最大任务数）
  kDraining = 4,     // 排空中（不接受新任务）
  kMaintenance = 5   // 维护中
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
    case WorkerStatus::kMaintenance:
      return "Maintenance";
    default:
      return "Unknown";
  }
}

// ============================================================================
// Worker 资源信息
// ============================================================================
struct WorkerResources {
  uint32_t total_cpu_cores = 0;
  uint64_t total_memory_mb = 0;
  uint64_t total_disk_mb = 0;
  uint32_t max_concurrent_tasks = 5;
  uint32_t active_tasks = 0;

  // CaaS-LSM: 已使用的资源
  uint64_t used_memory_mb = 0;
  uint64_t used_disk_mb = 0;

  // 可用容量
  uint32_t AvailableSlots() const {
    return max_concurrent_tasks > active_tasks
             ? max_concurrent_tasks - active_tasks
             : 0;
  }

  // 负载率
  double LoadRatio() const {
    if (max_concurrent_tasks == 0)
      return 1.0;
    return static_cast<double>(active_tasks) / max_concurrent_tasks;
  }

  // CaaS-LSM: 内存空闲率
  double MemoryFreeRatio() const {
    if (total_memory_mb == 0)
      return 0.0;
    uint64_t free_memory = total_memory_mb > used_memory_mb
                             ? total_memory_mb - used_memory_mb
                             : 0;
    return static_cast<double>(free_memory) / total_memory_mb;
  }

  std::string ToString() const;
};

// ============================================================================
// Worker 信息
// ============================================================================
struct WorkerInfo {
  std::string worker_id;
  std::string address;   // host:port
  WorkerStatus status = WorkerStatus::kUnknown;
  WorkerResources resources;

  // 时间信息
  std::chrono::steady_clock::time_point last_heartbeat;
  std::chrono::steady_clock::time_point register_time;

  // 统计信息
  uint64_t total_completed = 0;
  uint64_t total_failed = 0;
  int32_t consecutive_failures = 0;

  // 当前运行的任务
  std::vector<std::string> active_task_ids;

  // 标签（用于亲和性调度）
  std::map<std::string, std::string> labels;

  // 辅助方法
  bool IsAvailable() const {
    return status == WorkerStatus::kOnline && resources.AvailableSlots() > 0;
  }

  // CaaS-LSM: 获取内存空闲率
  double GetMemoryFreeRatio() const {
    return resources.MemoryFreeRatio();
  }

  std::string ToString() const;
};

// ============================================================================
// Worker 事件
// ============================================================================
enum class WorkerEvent {
  kRegistered,     // 新 Worker 注册
  kUnregistered,   // Worker 注销
  kOnline,         // Worker 上线
  kOffline,        // Worker 离线
  kBusy,           // Worker 忙碌
  kIdle,           // Worker 空闲
  kHeartbeat       // 心跳更新
};

using WorkerEventCallback =
  std::function<void(const WorkerInfo& worker, WorkerEvent event)>;

// ============================================================================
// Worker Manager 配置
// ============================================================================
struct WorkerManagerConfig {
  // 心跳配置
  int32_t heartbeat_timeout_sec = 30;      // 心跳超时
  int32_t heartbeat_interval_sec = 10;     // 期望的心跳间隔

  // 健康检查配置
  int32_t health_check_interval_sec = 5;   // 健康检查间隔
  int32_t max_consecutive_failures = 3;    // 最大连续失败次数

  // 默认资源配置
  uint32_t default_max_concurrent = 5;
};

// ============================================================================
// Worker Manager - 管理所有 CSA Worker
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

  // =========================================================================
  // Worker 注册管理 (由 gRPC 服务调用)
  // =========================================================================

  // Worker 注册
  // 返回分配的 worker_id (如果请求中未提供)
  std::string RegisterWorker(const std::string& address,
                             const WorkerResources& resources,
                             const std::map<std::string, std::string>& labels =
                               {},
                             const std::string& requested_id = "");

  // Worker 注销
  bool UnregisterWorker(const std::string& worker_id,
                        const std::string& reason = "");

  // Worker 心跳
  // 返回需要取消的任务列表
  std::vector<std::string> ProcessHeartbeat(
    const std::string& worker_id,
    const WorkerResources& resources,
    const std::vector<std::string>& active_task_ids);

  // =========================================================================
  // Worker 查询
  // =========================================================================

  std::shared_ptr<WorkerInfo> GetWorker(const std::string& worker_id) const;
  std::vector<std::shared_ptr<WorkerInfo>> GetAllWorkers() const;
  std::vector<std::shared_ptr<WorkerInfo>> GetAvailableWorkers() const;
  std::vector<std::shared_ptr<WorkerInfo>> GetWorkersByStatus(
    WorkerStatus status) const;

  // =========================================================================
  // 任务分配 (由 TaskScheduler 调用)
  // =========================================================================

  // 选择最佳 Worker
  std::shared_ptr<WorkerInfo> SelectBestWorker();

  // 分配任务给 Worker
  bool AssignTask(const std::string& worker_id, const std::string& task_id);

  // 释放任务
  bool ReleaseTask(const std::string& worker_id,
                   const std::string& task_id,
                   bool success);

  // =========================================================================
  // 统计信息
  // =========================================================================

  size_t GetWorkerCount() const;
  size_t GetOnlineWorkerCount() const;
  size_t GetTotalAvailableSlots() const;

  // =========================================================================
  // CaaS-LSM: 推送模式支持
  // =========================================================================

  // 主动检查 CSA 状态
  bool CheckCSAStatus(const std::string& worker_id);

  // 分发任务到 CSA (推送模式)
  bool DistributeJobToCSA(const std::string& worker_id,
                          const std::string& task_id,
                          const std::string& compaction_args,
                          const std::string& compaction_addition_info,
                          const std::string& shared_fs_uri,
                          int32_t start_level,
                          double score);

  // 取消 CSA 上正在执行的任务
  bool CancelCSATask(const std::string& worker_id, const std::string& task_id);

  // =========================================================================
  // 事件回调
  // =========================================================================

  void RegisterEventCallback(WorkerEventCallback callback);

 private:
  void HealthCheckLoop();
  void CheckWorkerTimeout(WorkerInfo& worker);
  void NotifyEvent(const WorkerInfo& worker, WorkerEvent event);
  std::string GenerateWorkerId(const std::string& address);

  WorkerManagerConfig config_;
  std::atomic<bool> running_{false};

  // Worker 注册表
  std::unordered_map<std::string, std::shared_ptr<WorkerInfo>> workers_;
  mutable std::mutex workers_mutex_;

  // 事件回调
  std::vector<WorkerEventCallback> event_callbacks_;
  std::mutex callbacks_mutex_;

  // 健康检查线程
  std::unique_ptr<std::thread> health_check_thread_;
  std::condition_variable cv_;
  std::mutex cv_mutex_;

  // ID 生成计数器
  std::atomic<uint64_t> worker_id_counter_{0};

  // CaaS-LSM: CSA gRPC 客户端连接
  std::unordered_map<std::string, std::shared_ptr<grpc::Channel>> csa_channels_;
  mutable std::mutex channels_mutex_;

  // 创建或获取 CSA 连接
  std::shared_ptr<grpc::Channel> GetOrCreateCSAChannel(const std::string& address);
};

}  // namespace control_plane
}  // namespace tendisplus
