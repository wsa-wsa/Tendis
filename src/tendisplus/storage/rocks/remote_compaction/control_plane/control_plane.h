// Copyright (c) 2024-present, Tencent Inc. All rights reserved.
// 控制平面主入口

#pragma once

#include <memory>
#include <string>

#include "task_model.h"
#include "task_scheduler.h"
#include "task_state_machine.h"
#include "worker_manager.h"

namespace tendisplus {
namespace control_plane {

// ============================================================================
// 控制平面配置
// ============================================================================
struct ControlPlaneConfig {
  // 调度器配置
  SchedulerConfig scheduler_config;
  
  // Worker 管理器配置
  WorkerManagerConfig worker_manager_config;
  
  // 服务配置
  std::string listen_address = "0.0.0.0:50051";
  uint32_t grpc_max_threads = 10;
  
  // 持久化配置
  bool enable_persistence = true;
  std::string persistence_path = "/var/lib/tendis/control_plane";
  uint32_t checkpoint_interval_sec = 60;
  
  // 高可用配置
  bool enable_ha = false;
  std::string ha_peers;  // 逗号分隔的对等节点地址
  
  // 监控配置
  bool enable_metrics = true;
  std::string metrics_endpoint = "/metrics";
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
  
  // 单例访问
  static ControlPlane& Instance();
  static void Initialize(const ControlPlaneConfig& config);
  static void Shutdown();
  
  // 生命周期管理
  void Start();
  void Stop();
  bool IsRunning() const;
  
  // =========================================================================
  // 任务管理 API
  // =========================================================================
  
  // 提交 Compaction 任务
  std::string SubmitCompactionTask(
      const std::string& source_node_id,
      const std::string& db_name,
      uint32_t store_id,
      const CompactionTaskParams& params,
      TaskPriority priority = TaskPriority::kNormal);
  
  // 提交 Bulk Load 任务
  std::string SubmitBulkLoadTask(
      const std::string& source_node_id,
      const std::string& db_name,
      uint32_t store_id,
      const BulkLoadTaskParams& params,
      TaskPriority priority = TaskPriority::kNormal);
  
  // 取消任务
  bool CancelTask(const std::string& task_id, const std::string& reason = "");
  
  // 查询任务
  std::shared_ptr<BackgroundTask> GetTask(const std::string& task_id) const;
  std::vector<std::shared_ptr<BackgroundTask>> QueryTasks(
      const TaskFilter& filter) const;
  
  // 获取任务统计
  TaskStatistics GetTaskStatistics() const;
  
  // =========================================================================
  // Worker 管理 API
  // =========================================================================
  
  // 获取 Worker 列表
  std::vector<WorkerInfo> GetWorkers() const;
  std::vector<WorkerInfo> GetAvailableWorkers() const;
  
  // 获取 Worker 统计
  size_t GetWorkerCount() const;
  size_t GetOnlineWorkerCount() const;
  
  // =========================================================================
  // 内部组件访问
  // =========================================================================
  
  TaskScheduler& GetScheduler() { return *scheduler_; }
  WorkerManager& GetWorkerManager() { return *worker_manager_; }
  TaskStateMachine& GetStateMachine() { return TaskStateMachine::Instance(); }

 private:
  void InitializeComponents();
  void SetupCallbacks();
  void StartGrpcServer();
  void StopGrpcServer();
  
  // 任务完成处理
  void OnTaskCompleted(const BackgroundTask& task);
  void OnTaskFailed(const BackgroundTask& task);
  
  // Worker 事件处理
  void OnWorkerEvent(const WorkerInfo& worker, WorkerEvent event);
  
  ControlPlaneConfig config_;
  std::atomic<bool> running_{false};
  
  // 核心组件
  std::unique_ptr<TaskScheduler> scheduler_;
  std::shared_ptr<WorkerManager> worker_manager_;
  
  // gRPC 服务器 (实际实现中添加)
  // std::unique_ptr<grpc::Server> grpc_server_;
  
  // 单例实例
  static std::unique_ptr<ControlPlane> instance_;
  static std::mutex instance_mutex_;
};

// ============================================================================
// 控制平面 gRPC 服务实现
// ============================================================================
// 在实际实现中，这里会定义 gRPC 服务的实现类
// 用于接收来自 TendisPlus 节点和 Worker 节点的请求

/*
class ControlPlaneServiceImpl : public ControlPlaneService::Service {
 public:
  explicit ControlPlaneServiceImpl(ControlPlane& control_plane);
  
  // 任务管理 RPC
  grpc::Status SubmitTask(grpc::ServerContext* context,
                          const SubmitTaskRequest* request,
                          SubmitTaskResponse* response) override;
  
  grpc::Status CancelTask(grpc::ServerContext* context,
                          const CancelTaskRequest* request,
                          CancelTaskResponse* response) override;
  
  grpc::Status QueryTask(grpc::ServerContext* context,
                         const QueryTaskRequest* request,
                         QueryTaskResponse* response) override;
  
  // Worker 管理 RPC
  grpc::Status RegisterWorker(grpc::ServerContext* context,
                              const RegisterWorkerRequest* request,
                              RegisterWorkerResponse* response) override;
  
  grpc::Status WorkerHeartbeat(grpc::ServerContext* context,
                               const HeartbeatRequest* request,
                               HeartbeatResponse* response) override;
  
  grpc::Status ReportTaskResult(grpc::ServerContext* context,
                                const TaskResultRequest* request,
                                TaskResultResponse* response) override;

 private:
  ControlPlane& control_plane_;
};
*/

}  // namespace control_plane
}  // namespace tendisplus
