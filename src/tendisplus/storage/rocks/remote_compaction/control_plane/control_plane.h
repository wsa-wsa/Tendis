// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

// Control Plane Service - 独立控制平面服务
// Based on CaaS-LSM architecture: Compaction-as-a-Service

#pragma once

#include <atomic>
#include <memory>
#include <string>

#include "task_model.h"
#include "task_scheduler.h"
#include "worker_manager.h"

namespace tendisplus {
namespace control_plane {

// ============================================================================
// 控制平面配置
// ============================================================================
struct ControlPlaneConfig {
  // gRPC 服务配置
  std::string listen_address = "0.0.0.0:50051";
  uint32_t grpc_max_threads = 10;
  int64_t grpc_max_message_size = 64 * 1024 * 1024;  // 64MB

  // 调度器配置
  SchedulerConfig scheduler_config;

  // Worker 管理器配置
  WorkerManagerConfig worker_manager_config;

  // 任务配置
  uint32_t default_task_timeout_sec = 3600;
  uint32_t max_task_retries = 3;

  // 监控配置
  bool enable_metrics = true;
  uint32_t metrics_port = 9090;
};

// ============================================================================
// 控制平面服务
// ============================================================================
class ControlPlane {
 public:
  explicit ControlPlane(const ControlPlaneConfig& config);
  ~ControlPlane();

  // 禁止拷贝
  ControlPlane(const ControlPlane&) = delete;
  ControlPlane& operator=(const ControlPlane&) = delete;

  // 生命周期管理
  void Start();
  void Stop();
  bool IsRunning() const {
    return running_.load();
  }

  // 阻塞等待服务停止
  void Wait();

  // =========================================================================
  // Tendisplus Node API - 任务管理
  // =========================================================================

  // 提交 Compaction 任务
  std::string SubmitCompactionTask(const std::string& source_node_id,
                                   const std::string& db_name,
                                   uint32_t store_id,
                                   uint64_t job_id,
                                   const std::string& compaction_input,
                                   const std::string& shared_fs_uri,
                                   TaskPriority priority = TaskPriority::kNormal,
                                   uint32_t timeout_sec = 0);

  // 查询任务状态
  std::shared_ptr<TaskInfo> QueryTask(const std::string& task_id) const;

  // 取消任务
  bool CancelTask(const std::string& task_id, const std::string& reason = "");

  // 等待任务结果
  bool WaitForTaskResult(const std::string& task_id,
                         TaskResult* result,
                         uint32_t timeout_ms);

  // =========================================================================
  // CSA Worker API - Worker 管理
  // =========================================================================

  // Worker 注册
  std::string RegisterWorker(const std::string& address,
                             const WorkerResources& resources,
                             const std::map<std::string, std::string>& labels = {},
                             const std::string& requested_id = "");

  // Worker 心跳
  std::vector<std::string> ProcessWorkerHeartbeat(
    const std::string& worker_id,
    const WorkerResources& resources,
    const std::vector<std::string>& active_task_ids);

  // Worker 注销
  bool UnregisterWorker(const std::string& worker_id,
                        const std::string& reason = "");

  // 上报任务结果
  void ReportTaskResult(const std::string& worker_id,
                        const std::string& task_id,
                        const TaskResult& result);

  // 获取待执行任务 (Worker 拉取模式)
  std::vector<std::shared_ptr<TaskInfo>> FetchTasks(
    const std::string& worker_id,
    uint32_t max_tasks);

  // 标记任务开始执行
  void MarkTaskRunning(const std::string& task_id);

  // =========================================================================
  // 监控 API
  // =========================================================================

  // 获取集群状态
  struct ClusterStatus {
    uint32_t total_workers = 0;
    uint32_t online_workers = 0;
    uint32_t pending_tasks = 0;
    uint32_t running_tasks = 0;
    uint64_t total_completed = 0;
    uint64_t total_failed = 0;
  };
  ClusterStatus GetClusterStatus() const;

  // 获取 Worker 列表
  std::vector<std::shared_ptr<WorkerInfo>> GetWorkers() const;

  // 获取任务统计
  const TaskStatistics& GetTaskStatistics() const;

  // =========================================================================
  // 内部组件访问
  // =========================================================================

  TaskScheduler& GetScheduler() {
    return *scheduler_;
  }
  WorkerManager& GetWorkerManager() {
    return *worker_manager_;
  }

 private:
  void InitializeComponents();
  void StartGrpcServer();
  void StopGrpcServer();

  ControlPlaneConfig config_;
  std::atomic<bool> running_{false};

  // 核心组件
  std::shared_ptr<WorkerManager> worker_manager_;
  std::unique_ptr<TaskScheduler> scheduler_;

  // gRPC 服务器 (在实现文件中定义)
  class GrpcServer;
  std::unique_ptr<GrpcServer> grpc_server_;

  // 等待条件变量
  std::condition_variable shutdown_cv_;
  std::mutex shutdown_mutex_;
};

}  // namespace control_plane
}  // namespace tendisplus
