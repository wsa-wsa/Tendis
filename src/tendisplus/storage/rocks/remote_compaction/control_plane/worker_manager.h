// Copyright (c) 2024-present, Tencent Inc. All rights reserved.
// Worker 管理器定义

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

#include "task_model.h"

namespace tendisplus {
namespace control_plane {

// ============================================================================
// Worker 状态
// ============================================================================
enum class WorkerStatus {
  kUnknown = 0,
  kOnline = 1,       // 在线
  kOffline = 2,      // 离线
  kBusy = 3,         // 忙碌
  kDraining = 4,     // 排空中 (不接受新任务)
  kMaintenance = 5   // 维护中
};

inline const char* WorkerStatusToString(WorkerStatus status) {
  switch (status) {
    case WorkerStatus::kOnline: return "Online";
    case WorkerStatus::kOffline: return "Offline";
    case WorkerStatus::kBusy: return "Busy";
    case WorkerStatus::kDraining: return "Draining";
    case WorkerStatus::kMaintenance: return "Maintenance";
    default: return "Unknown";
  }
}

// ============================================================================
// Worker 资源信息
// ============================================================================
struct WorkerResources {
  // 总资源
  uint32_t total_cpu_cores = 0;
  uint64_t total_memory_mb = 0;
  uint64_t total_disk_mb = 0;
  uint64_t network_bandwidth_mbps = 0;
  
  // 已使用资源
  uint32_t used_cpu_cores = 0;
  uint64_t used_memory_mb = 0;
  uint64_t used_disk_mb = 0;
  
  // 可用资源
  uint32_t AvailableCpuCores() const { 
    return total_cpu_cores > used_cpu_cores ? total_cpu_cores - used_cpu_cores : 0; 
  }
  uint64_t AvailableMemoryMb() const { 
    return total_memory_mb > used_memory_mb ? total_memory_mb - used_memory_mb : 0; 
  }
  uint64_t AvailableDiskMb() const { 
    return total_disk_mb > used_disk_mb ? total_disk_mb - used_disk_mb : 0; 
  }
  
  // 资源利用率
  double CpuUtilization() const {
    return total_cpu_cores > 0 ? 
           static_cast<double>(used_cpu_cores) / total_cpu_cores : 0.0;
  }
  double MemoryUtilization() const {
    return total_memory_mb > 0 ? 
           static_cast<double>(used_memory_mb) / total_memory_mb : 0.0;
  }
  double DiskUtilization() const {
    return total_disk_mb > 0 ? 
           static_cast<double>(used_disk_mb) / total_disk_mb : 0.0;
  }
  
  std::string ToString() const;
};

// ============================================================================
// Worker 负载信息
// ============================================================================
struct WorkerLoad {
  double cpu_usage_percent = 0.0;
  double memory_usage_percent = 0.0;
  double disk_io_percent = 0.0;
  double network_io_percent = 0.0;
  uint32_t active_task_count = 0;
  uint32_t pending_task_count = 0;
  uint64_t bytes_processed_per_sec = 0;
  
  // 综合负载评分 (0-100, 越低越好)
  double GetLoadScore() const {
    return (cpu_usage_percent * 0.4 + 
            memory_usage_percent * 0.3 + 
            disk_io_percent * 0.2 + 
            network_io_percent * 0.1);
  }
  
  std::string ToString() const;
};

// ============================================================================
// Worker 信息
// ============================================================================
struct WorkerInfo {
  std::string worker_id;
  std::string address;              // gRPC 地址 (host:port)
  WorkerStatus status = WorkerStatus::kUnknown;
  WorkerResources resources;
  WorkerLoad load;
  
  // 心跳信息
  std::chrono::system_clock::time_point last_heartbeat;
  std::chrono::system_clock::time_point registered_at;
  uint64_t heartbeat_count = 0;
  
  // 任务统计
  uint64_t total_tasks_completed = 0;
  uint64_t total_tasks_failed = 0;
  uint64_t total_bytes_processed = 0;
  
  // 标签 (用于调度亲和性)
  std::map<std::string, std::string> labels;
  
  // 当前运行的任务
  std::vector<std::string> running_task_ids;
  
  // 辅助方法
  bool IsAvailable() const {
    return status == WorkerStatus::kOnline && 
           running_task_ids.size() < resources.total_cpu_cores;
  }
  
  bool CanAcceptTask(const ResourceRequirement& requirement) const;
  
  std::string ToString() const;
};

// ============================================================================
// Worker 事件
// ============================================================================
enum class WorkerEvent {
  kRegistered,       // Worker 注册
  kHeartbeat,        // 心跳
  kStatusChanged,    // 状态变更
  kResourceUpdated,  // 资源更新
  kTaskAssigned,     // 任务分配
  kTaskCompleted,    // 任务完成
  kLost              // Worker 丢失
};

using WorkerEventCallback = std::function<void(
    const WorkerInfo& worker, WorkerEvent event)>;

// ============================================================================
// Worker 管理器配置
// ============================================================================
struct WorkerManagerConfig {
  uint32_t heartbeat_interval_sec = 10;     // 心跳间隔
  uint32_t heartbeat_timeout_sec = 30;      // 心跳超时
  uint32_t health_check_interval_sec = 5;   // 健康检查间隔
  uint32_t max_workers = 1000;              // 最大 Worker 数量
  bool enable_auto_discovery = false;       // 是否启用自动发现
  std::string discovery_endpoint;           // 服务发现端点
};

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
  
  // 启动/停止
  void Start();
  void Stop();
  bool IsRunning() const { return running_.load(); }
  
  // Worker 注册/注销
  bool RegisterWorker(const WorkerInfo& worker);
  bool UnregisterWorker(const std::string& worker_id);
  
  // 心跳处理
  bool ProcessHeartbeat(const std::string& worker_id, 
                        const WorkerLoad& load,
                        const WorkerResources& resources);
  
  // 获取 Worker 信息
  std::optional<WorkerInfo> GetWorker(const std::string& worker_id) const;
  std::vector<WorkerInfo> GetAllWorkers() const;
  std::vector<WorkerInfo> GetAvailableWorkers() const;
  std::vector<WorkerInfo> GetWorkersByStatus(WorkerStatus status) const;
  
  // 更新 Worker 状态
  bool UpdateWorkerStatus(const std::string& worker_id, WorkerStatus status);
  
  // 任务分配/完成通知
  bool AssignTaskToWorker(const std::string& worker_id, 
                          const std::string& task_id,
                          const ResourceRequirement& requirement);
  bool ReleaseTaskFromWorker(const std::string& worker_id,
                             const std::string& task_id,
                             const ResourceRequirement& requirement);
  
  // 获取统计信息
  size_t GetWorkerCount() const;
  size_t GetOnlineWorkerCount() const;
  WorkerResources GetTotalResources() const;
  WorkerResources GetAvailableResources() const;
  
  // 注册事件回调
  void RegisterEventCallback(WorkerEventCallback callback);
  
  // 选择最佳 Worker (供调度器使用)
  std::optional<std::string> SelectBestWorker(
      const ResourceRequirement& requirement,
      const std::map<std::string, std::string>& preferred_labels = {}) const;

 private:
  void HealthCheckLoop();
  void DiscoveryLoop();
  void NotifyEvent(const WorkerInfo& worker, WorkerEvent event);
  void MarkWorkerOffline(const std::string& worker_id);
  
  WorkerManagerConfig config_;
  std::atomic<bool> running_{false};
  
  // Worker 注册表
  std::unordered_map<std::string, WorkerInfo> workers_;
  mutable std::mutex workers_mutex_;
  
  // 事件回调
  std::vector<WorkerEventCallback> event_callbacks_;
  std::mutex callbacks_mutex_;
  
  // 后台线程
  std::unique_ptr<std::thread> health_check_thread_;
  std::unique_ptr<std::thread> discovery_thread_;
  std::condition_variable cv_;
};

// ============================================================================
// Worker 客户端 (用于与 Worker 通信)
// ============================================================================
class WorkerClient {
 public:
  explicit WorkerClient(const std::string& address);
  ~WorkerClient();
  
  // 发送任务
  bool SendTask(const BackgroundTask& task);
  
  // 取消任务
  bool CancelTask(const std::string& task_id);
  
  // 查询任务状态
  std::optional<TaskStatus> QueryTaskStatus(const std::string& task_id);
  
  // 健康检查
  bool HealthCheck();
  
  // 获取 Worker 信息
  std::optional<WorkerInfo> GetWorkerInfo();

 private:
  std::string address_;
  // gRPC channel 和 stub (实际实现中添加)
};

}  // namespace control_plane
}  // namespace tendisplus
