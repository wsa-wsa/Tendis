// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include "control_plane.h"

#include <grpc/grpc.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>

#include <iostream>

#include "control_plane.grpc.pb.h"

namespace tendisplus {
namespace control_plane {

// ============================================================================
// gRPC Service Implementation
// ============================================================================
class ControlPlaneServiceImpl final
  : public ::control_plane::ControlPlaneService::Service {
 public:
  explicit ControlPlaneServiceImpl(ControlPlane& control_plane)
    : control_plane_(control_plane) {}

  // =========================================================================
  // Tendisplus Node API
  // =========================================================================

  grpc::Status SubmitCompactionTask(
    grpc::ServerContext* context,
    const ::control_plane::SubmitTaskRequest* request,
    ::control_plane::SubmitTaskResponse* response) override {
    auto priority = static_cast<TaskPriority>(request->priority());

    std::string task_id = control_plane_.SubmitCompactionTask(
      request->source_node_id(),
      request->db_name(),
      request->store_id(),
      request->job_id(),
      request->compaction_input(),
      request->shared_fs_uri(),
      priority,
      request->timeout_sec());

    if (task_id.empty()) {
      response->set_success(false);
      response->set_error_message("Failed to submit task");
    } else {
      response->set_success(true);
      response->set_task_id(task_id);
    }

    return grpc::Status::OK;
  }

  grpc::Status QueryTaskStatus(
    grpc::ServerContext* context,
    const ::control_plane::QueryTaskRequest* request,
    ::control_plane::QueryTaskResponse* response) override {
    auto task = control_plane_.QueryTask(request->task_id());

    if (task) {
      response->set_found(true);
      auto* info = response->mutable_task_info();
      info->set_task_id(task->task_id);
      info->set_source_node_id(task->source_node_id);
      info->set_db_name(task->db_name);
      info->set_store_id(task->store_id);
      info->set_priority(
        static_cast<::control_plane::TaskPriority>(task->priority));
      info->set_status(static_cast<::control_plane::TaskStatus>(task->status));
      info->set_assigned_worker_id(task->assigned_worker_id);
      info->set_retry_count(task->retry_count);
      info->set_error_message(task->error_message);
    } else {
      response->set_found(false);
    }

    return grpc::Status::OK;
  }

  grpc::Status CancelTask(
    grpc::ServerContext* context,
    const ::control_plane::CancelTaskRequest* request,
    ::control_plane::CancelTaskResponse* response) override {
    bool success =
      control_plane_.CancelTask(request->task_id(), request->reason());
    response->set_success(success);
    if (!success) {
      response->set_error_message("Task not found or already completed");
    }
    return grpc::Status::OK;
  }

  grpc::Status WaitForTaskResult(
    grpc::ServerContext* context,
    const ::control_plane::WaitResultRequest* request,
    ::control_plane::WaitResultResponse* response) override {
    TaskResult result;
    bool completed = control_plane_.WaitForTaskResult(
      request->task_id(), &result, request->timeout_ms());

    response->set_completed(completed);
    if (completed) {
      auto task = control_plane_.QueryTask(request->task_id());
      if (task) {
        response->set_status(
          static_cast<::control_plane::TaskStatus>(task->status));
        response->set_compaction_result(result.compaction_result);
        response->set_error_message(result.error_message);
      }
    }

    return grpc::Status::OK;
  }

  // =========================================================================
  // CSA Worker API
  // =========================================================================

  grpc::Status RegisterWorker(
    grpc::ServerContext* context,
    const ::control_plane::RegisterWorkerRequest* request,
    ::control_plane::RegisterWorkerResponse* response) override {
    WorkerResources resources;
    resources.total_cpu_cores = request->resources().total_cpu_cores();
    resources.total_memory_mb = request->resources().total_memory_mb();
    resources.total_disk_mb = request->resources().total_disk_mb();
    resources.max_concurrent_tasks =
      request->resources().max_concurrent_tasks();
    resources.active_tasks = request->resources().active_tasks();

    std::map<std::string, std::string> labels(request->labels().begin(),
                                              request->labels().end());

    std::string worker_id = control_plane_.RegisterWorker(
      request->address(), resources, labels, request->worker_id());

    if (worker_id.empty()) {
      response->set_success(false);
      response->set_error_message("Failed to register worker");
    } else {
      response->set_success(true);
      response->set_worker_id(worker_id);
      response->set_heartbeat_interval_sec(10);
    }

    return grpc::Status::OK;
  }

  grpc::Status WorkerHeartbeat(
    grpc::ServerContext* context,
    const ::control_plane::HeartbeatRequest* request,
    ::control_plane::HeartbeatResponse* response) override {
    WorkerResources resources;
    resources.total_cpu_cores = request->resources().total_cpu_cores();
    resources.total_memory_mb = request->resources().total_memory_mb();
    resources.total_disk_mb = request->resources().total_disk_mb();
    resources.max_concurrent_tasks =
      request->resources().max_concurrent_tasks();
    resources.active_tasks = request->resources().active_tasks();

    std::vector<std::string> active_tasks(request->active_task_ids().begin(),
                                          request->active_task_ids().end());

    auto tasks_to_cancel = control_plane_.ProcessWorkerHeartbeat(
      request->worker_id(), resources, active_tasks);

    response->set_success(true);
    for (const auto& task_id : tasks_to_cancel) {
      response->add_tasks_to_cancel(task_id);
    }

    return grpc::Status::OK;
  }

  grpc::Status UnregisterWorker(
    grpc::ServerContext* context,
    const ::control_plane::UnregisterWorkerRequest* request,
    ::control_plane::UnregisterWorkerResponse* response) override {
    bool success = control_plane_.UnregisterWorker(request->worker_id(),
                                                   request->reason());
    response->set_success(success);
    return grpc::Status::OK;
  }

  grpc::Status ReportTaskResult(
    grpc::ServerContext* context,
    const ::control_plane::TaskResultRequest* request,
    ::control_plane::TaskResultResponse* response) override {
    TaskResult result;
    result.success = request->success();
    result.compaction_result = request->compaction_result();
    result.error_message = request->error_message();
    result.execution_time_ms = request->execution_time_ms();
    result.bytes_read = request->bytes_read();
    result.bytes_written = request->bytes_written();

    control_plane_.ReportTaskResult(
      request->worker_id(), request->task_id(), result);

    response->set_success(true);
    return grpc::Status::OK;
  }

  grpc::Status FetchTask(
    grpc::ServerContext* context,
    const ::control_plane::FetchTaskRequest* request,
    ::control_plane::FetchTaskResponse* response) override {
    auto tasks =
      control_plane_.FetchTasks(request->worker_id(), request->max_tasks());

    for (const auto& task : tasks) {
      auto* proto_task = response->add_tasks();
      proto_task->set_task_id(task->task_id);
      proto_task->set_db_name(task->db_name);
      proto_task->set_store_id(task->store_id);
      proto_task->set_job_id(task->params.job_id);
      proto_task->set_compaction_input(task->params.compaction_input);
      proto_task->set_shared_fs_uri(task->params.shared_fs_uri);
      proto_task->set_shared_fs_local_prefix(task->params.shared_fs_local_prefix);
      proto_task->set_timeout_sec(task->params.timeout_sec);
    }

    return grpc::Status::OK;
  }

  // =========================================================================
  // Admin API
  // =========================================================================

  grpc::Status GetClusterStatus(
    grpc::ServerContext* context,
    const ::control_plane::ClusterStatusRequest* request,
    ::control_plane::ClusterStatusResponse* response) override {
    auto status = control_plane_.GetClusterStatus();
    response->set_total_workers(status.total_workers);
    response->set_online_workers(status.online_workers);
    response->set_pending_tasks(status.pending_tasks);
    response->set_running_tasks(status.running_tasks);
    response->set_total_completed(status.total_completed);
    response->set_total_failed(status.total_failed);
    return grpc::Status::OK;
  }

  grpc::Status ListWorkers(
    grpc::ServerContext* context,
    const ::control_plane::ListWorkersRequest* request,
    ::control_plane::ListWorkersResponse* response) override {
    auto workers = control_plane_.GetWorkers();

    for (const auto& worker : workers) {
      auto* proto_worker = response->add_workers();
      proto_worker->set_worker_id(worker->worker_id);
      proto_worker->set_address(worker->address);
      proto_worker->set_status(
        static_cast<::control_plane::WorkerStatus>(worker->status));

      auto* resources = proto_worker->mutable_resources();
      resources->set_total_cpu_cores(worker->resources.total_cpu_cores);
      resources->set_total_memory_mb(worker->resources.total_memory_mb);
      resources->set_total_disk_mb(worker->resources.total_disk_mb);
      resources->set_max_concurrent_tasks(
        worker->resources.max_concurrent_tasks);
      resources->set_active_tasks(worker->resources.active_tasks);

      proto_worker->set_total_completed(worker->total_completed);
      proto_worker->set_total_failed(worker->total_failed);
    }

    return grpc::Status::OK;
  }

  grpc::Status GetTaskStatistics(
    grpc::ServerContext* context,
    const ::control_plane::TaskStatisticsRequest* request,
    ::control_plane::TaskStatisticsResponse* response) override {
    const auto& stats = control_plane_.GetTaskStatistics();
    response->set_total_submitted(stats.total_submitted.load());
    response->set_total_completed(stats.total_completed.load());
    response->set_total_failed(stats.total_failed.load());
    response->set_total_cancelled(stats.total_cancelled.load());
    response->set_total_timeout(stats.total_timeout.load());
    response->set_avg_execution_time_ms(
      static_cast<uint64_t>(stats.GetAvgExecutionTimeMs()));
    response->set_avg_queue_time_ms(
      static_cast<uint64_t>(stats.GetAvgQueueTimeMs()));
    return grpc::Status::OK;
  }

  // =========================================================================
  // Observatory API - 观测平面接口
  // =========================================================================

  grpc::Status ListTasks(
    grpc::ServerContext* context,
    const ::control_plane::ListTasksRequest* request,
    ::control_plane::ListTasksResponse* response) override {
    TaskFilter filter;
    filter.limit = request->limit() > 0 ? request->limit() : 100;
    
    if (request->status_filter() != ::control_plane::TASK_UNKNOWN) {
      filter.status = static_cast<TaskStatus>(request->status_filter());
    }
    if (!request->source_node_filter().empty()) {
      filter.source_node_id = request->source_node_filter();
    }
    
    auto tasks = control_plane_.GetScheduler().QueryTasks(filter);
    
    for (const auto& task : tasks) {
      auto* proto_task = response->add_tasks();
      proto_task->set_task_id(task->task_id);
      proto_task->set_source_node_id(task->source_node_id);
      proto_task->set_db_name(task->db_name);
      proto_task->set_store_id(task->store_id);
      proto_task->set_priority(
        static_cast<::control_plane::TaskPriority>(task->priority));
      proto_task->set_status(
        static_cast<::control_plane::TaskStatus>(task->status));
      proto_task->set_assigned_worker_id(task->assigned_worker_id);
      proto_task->set_retry_count(task->retry_count);
      proto_task->set_error_message(task->error_message);
      
      // 时间戳
      proto_task->set_submit_time_ms(
        std::chrono::duration_cast<std::chrono::milliseconds>(
          task->submit_time.time_since_epoch()).count());
      proto_task->set_start_time_ms(
        std::chrono::duration_cast<std::chrono::milliseconds>(
          task->start_time.time_since_epoch()).count());
      proto_task->set_complete_time_ms(
        std::chrono::duration_cast<std::chrono::milliseconds>(
          task->complete_time.time_since_epoch()).count());
    }
    
    return grpc::Status::OK;
  }

  grpc::Status GetMetricsSnapshot(
    grpc::ServerContext* context,
    const ::control_plane::MetricsSnapshotRequest* request,
    ::control_plane::MetricsSnapshotResponse* response) override {
    auto cluster_status = control_plane_.GetClusterStatus();
    const auto& stats = control_plane_.GetTaskStatistics();
    auto workers = control_plane_.GetWorkers();
    
    // 集群概览
    response->set_total_workers(cluster_status.total_workers);
    response->set_online_workers(cluster_status.online_workers);
    response->set_pending_tasks(cluster_status.pending_tasks);
    response->set_running_tasks(cluster_status.running_tasks);
    
    // 计算 busy workers
    uint32_t busy_count = 0;
    double total_memory = 0, used_memory = 0;
    double total_disk = 0, used_disk = 0;
    
    for (const auto& w : workers) {
      if (w->status == WorkerStatus::kBusy) {
        busy_count++;
      }
      if (w->status == WorkerStatus::kOnline || w->status == WorkerStatus::kBusy) {
        total_memory += w->resources.total_memory_mb;
        used_memory += w->resources.used_memory_mb;
        total_disk += w->resources.total_disk_mb;
        used_disk += w->resources.used_disk_mb;
      }
    }
    response->set_busy_workers(busy_count);
    
    // 吞吐量统计
    response->set_total_completed(stats.total_completed.load());
    response->set_total_failed(stats.total_failed.load());
    // Note: completed_last_minute 和 failed_last_minute 需要额外的统计支持
    // 这里先设置为 0，后续可以添加时间窗口统计
    response->set_completed_last_minute(0);
    response->set_failed_last_minute(0);
    
    // 延迟统计
    response->set_avg_queue_time_ms(
      static_cast<uint64_t>(stats.GetAvgQueueTimeMs()));
    response->set_avg_execution_time_ms(
      static_cast<uint64_t>(stats.GetAvgExecutionTimeMs()));
    // Note: p50/p95/p99 需要额外的直方图统计支持
    response->set_p50_execution_time_ms(
      static_cast<uint64_t>(stats.GetAvgExecutionTimeMs()));
    response->set_p95_execution_time_ms(
      static_cast<uint64_t>(stats.GetAvgExecutionTimeMs() * 1.5));
    response->set_p99_execution_time_ms(
      static_cast<uint64_t>(stats.GetAvgExecutionTimeMs() * 2.0));
    
    // 资源使用
    response->set_cluster_cpu_usage(0.0);  // CPU 使用率需要额外采集
    response->set_cluster_memory_usage(
      total_memory > 0 ? used_memory / total_memory : 0.0);
    response->set_cluster_disk_usage(
      total_disk > 0 ? used_disk / total_disk : 0.0);
    
    // 时间戳
    response->set_timestamp_ms(
      std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());
    
    return grpc::Status::OK;
  }

  grpc::Status GetWorkerDetail(
    grpc::ServerContext* context,
    const ::control_plane::WorkerDetailRequest* request,
    ::control_plane::WorkerDetailResponse* response) override {
    auto worker = control_plane_.GetWorkerManager().GetWorker(request->worker_id());
    
    if (!worker) {
      response->set_found(false);
      return grpc::Status::OK;
    }
    
    response->set_found(true);
    
    auto* proto_worker = response->mutable_worker();
    proto_worker->set_worker_id(worker->worker_id);
    proto_worker->set_address(worker->address);
    proto_worker->set_status(
      static_cast<::control_plane::WorkerStatus>(worker->status));
    
    auto* resources = proto_worker->mutable_resources();
    resources->set_total_cpu_cores(worker->resources.total_cpu_cores);
    resources->set_total_memory_mb(worker->resources.total_memory_mb);
    resources->set_total_disk_mb(worker->resources.total_disk_mb);
    resources->set_used_memory_mb(worker->resources.used_memory_mb);
    resources->set_used_disk_mb(worker->resources.used_disk_mb);
    resources->set_max_concurrent_tasks(worker->resources.max_concurrent_tasks);
    resources->set_active_tasks(worker->resources.active_tasks);
    
    proto_worker->set_total_completed(worker->total_completed);
    proto_worker->set_total_failed(worker->total_failed);
    
    // 获取该 Worker 的活跃任务
    for (const auto& task_id : worker->active_task_ids) {
      auto task = control_plane_.QueryTask(task_id);
      if (task) {
        auto* proto_task = response->add_active_tasks();
        proto_task->set_task_id(task->task_id);
        proto_task->set_status(
          static_cast<::control_plane::TaskStatus>(task->status));
        proto_task->set_db_name(task->db_name);
        proto_task->set_store_id(task->store_id);
      }
    }
    
    // Worker 历史统计 (简化实现)
    response->set_tasks_completed_last_hour(worker->total_completed);
    response->set_tasks_failed_last_hour(worker->total_failed);
    response->set_avg_execution_time_ms(0);  // 需要额外统计
    
    return grpc::Status::OK;
  }

 private:
  ControlPlane& control_plane_;
};

// ============================================================================
// gRPC Server Wrapper
// ============================================================================
class ControlPlane::GrpcServer {
 public:
  GrpcServer(ControlPlane& control_plane, const ControlPlaneConfig& config)
    : service_(control_plane), config_(config) {}

  void Start() {
    grpc::ServerBuilder builder;

    builder.AddListeningPort(
      config_.listen_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service_);
    builder.SetMaxReceiveMessageSize(
      static_cast<int>(config_.grpc_max_message_size));
    builder.SetMaxSendMessageSize(
      static_cast<int>(config_.grpc_max_message_size));

    server_ = builder.BuildAndStart();

    std::cout << "[ControlPlane] gRPC server listening on "
              << config_.listen_address << std::endl;
  }

  void Stop() {
    if (server_) {
      server_->Shutdown();
      std::cout << "[ControlPlane] gRPC server stopped" << std::endl;
    }
  }

  void Wait() {
    if (server_) {
      server_->Wait();
    }
  }

 private:
  ControlPlaneServiceImpl service_;
  ControlPlaneConfig config_;
  std::unique_ptr<grpc::Server> server_;
};

// ============================================================================
// ControlPlane Implementation
// ============================================================================

ControlPlane::ControlPlane(const ControlPlaneConfig& config) : config_(config) {
  InitializeComponents();
}

ControlPlane::~ControlPlane() {
  Stop();
}

void ControlPlane::InitializeComponents() {
  // 创建 Worker Manager
  worker_manager_ = std::make_shared<WorkerManager>(config_.worker_manager_config);

  // 创建 Task Scheduler
  scheduler_ = std::make_unique<TaskScheduler>(config_.scheduler_config);
  scheduler_->SetWorkerManager(worker_manager_);
}

void ControlPlane::Start() {
  if (running_.exchange(true)) {
    return;
  }

  // 启动 Worker Manager
  worker_manager_->Start();

  // 启动 Task Scheduler
  scheduler_->Start();

  // 启动 gRPC Server
  StartGrpcServer();

  std::cout << "[ControlPlane] Control plane started" << std::endl;
}

void ControlPlane::Stop() {
  if (!running_.exchange(false)) {
    return;
  }

  // 停止 gRPC Server
  StopGrpcServer();

  // 停止 Task Scheduler
  scheduler_->Stop();

  // 停止 Worker Manager
  worker_manager_->Stop();

  // 通知等待者
  {
    std::lock_guard<std::mutex> lock(shutdown_mutex_);
    shutdown_cv_.notify_all();
  }

  std::cout << "[ControlPlane] Control plane stopped" << std::endl;
}

void ControlPlane::Wait() {
  if (grpc_server_) {
    grpc_server_->Wait();
  }
}

void ControlPlane::StartGrpcServer() {
  grpc_server_ = std::make_unique<GrpcServer>(*this, config_);
  grpc_server_->Start();
}

void ControlPlane::StopGrpcServer() {
  if (grpc_server_) {
    grpc_server_->Stop();
  }
}

// =========================================================================
// Tendisplus Node API Implementation
// =========================================================================

std::string ControlPlane::SubmitCompactionTask(
  const std::string& source_node_id,
  const std::string& db_name,
  uint32_t store_id,
  uint64_t job_id,
  const std::string& compaction_input,
  const std::string& shared_fs_uri,
  TaskPriority priority,
  uint32_t timeout_sec) {
  TaskInfo task;
  task.source_node_id = source_node_id;
  task.db_name = db_name;
  task.store_id = store_id;
  task.priority = priority;
  task.type = TaskType::kCompaction;
  task.max_retries = config_.max_task_retries;

  task.params.job_id = job_id;
  task.params.compaction_input = compaction_input;
  task.params.shared_fs_uri = shared_fs_uri;
  task.params.timeout_sec =
    timeout_sec > 0 ? timeout_sec : config_.default_task_timeout_sec;

  return scheduler_->SubmitTask(task);
}

std::shared_ptr<TaskInfo> ControlPlane::QueryTask(
  const std::string& task_id) const {
  return scheduler_->GetTask(task_id);
}

bool ControlPlane::CancelTask(const std::string& task_id,
                              const std::string& reason) {
  return scheduler_->CancelTask(task_id, reason);
}

bool ControlPlane::WaitForTaskResult(const std::string& task_id,
                                     TaskResult* result,
                                     uint32_t timeout_ms) {
  return scheduler_->WaitForTask(task_id, result, timeout_ms);
}

// =========================================================================
// CSA Worker API Implementation
// =========================================================================

std::string ControlPlane::RegisterWorker(
  const std::string& address,
  const WorkerResources& resources,
  const std::map<std::string, std::string>& labels,
  const std::string& requested_id) {
  return worker_manager_->RegisterWorker(address, resources, labels, requested_id);
}

std::vector<std::string> ControlPlane::ProcessWorkerHeartbeat(
  const std::string& worker_id,
  const WorkerResources& resources,
  const std::vector<std::string>& active_task_ids) {
  return worker_manager_->ProcessHeartbeat(worker_id, resources, active_task_ids);
}

bool ControlPlane::UnregisterWorker(const std::string& worker_id,
                                    const std::string& reason) {
  return worker_manager_->UnregisterWorker(worker_id, reason);
}

void ControlPlane::ReportTaskResult(const std::string& worker_id,
                                    const std::string& task_id,
                                    const TaskResult& result) {
  scheduler_->OnTaskCompleted(task_id, result);
}

std::vector<std::shared_ptr<TaskInfo>> ControlPlane::FetchTasks(
  const std::string& worker_id,
  uint32_t max_tasks) {
  // 获取分配给该 Worker 的任务
  TaskFilter filter;
  filter.assigned_worker_id = worker_id;
  filter.status = TaskStatus::kAssigned;
  filter.limit = max_tasks;

  return scheduler_->QueryTasks(filter);
}

void ControlPlane::MarkTaskRunning(const std::string& task_id) {
  auto task = scheduler_->GetTask(task_id);
  if (task && task->status == TaskStatus::kAssigned) {
    task->status = TaskStatus::kRunning;
    task->start_time = std::chrono::system_clock::now();
  }
}

// =========================================================================
// Monitoring API Implementation
// =========================================================================

ControlPlane::ClusterStatus ControlPlane::GetClusterStatus() const {
  ClusterStatus status;
  status.total_workers = worker_manager_->GetWorkerCount();
  status.online_workers = worker_manager_->GetOnlineWorkerCount();
  status.pending_tasks = scheduler_->GetPendingCount();
  status.running_tasks = scheduler_->GetRunningCount();

  const auto& stats = scheduler_->GetStatistics();
  status.total_completed = stats.total_completed.load();
  status.total_failed = stats.total_failed.load();

  return status;
}

std::vector<std::shared_ptr<WorkerInfo>> ControlPlane::GetWorkers() const {
  return worker_manager_->GetAllWorkers();
}

const TaskStatistics& ControlPlane::GetTaskStatistics() const {
  return scheduler_->GetStatistics();
}

}  // namespace control_plane
}  // namespace tendisplus
