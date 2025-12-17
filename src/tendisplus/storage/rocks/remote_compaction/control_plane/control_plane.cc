// Copyright (c) 2024-present, Tencent Inc. All rights reserved.
// 控制平面主入口实现

#include "control_plane.h"
#include "../observability/observability.h"

#include <iostream>
#include <sstream>

namespace tendisplus {
namespace control_plane {

// ============================================================================
// 静态成员初始化
// ============================================================================

std::unique_ptr<ControlPlane> ControlPlane::instance_;
std::mutex ControlPlane::instance_mutex_;

// ============================================================================
// ControlPlane 实现
// ============================================================================

ControlPlane::ControlPlane(const ControlPlaneConfig& config)
    : config_(config) {
  InitializeComponents();
}

ControlPlane::~ControlPlane() {
  Stop();
}

ControlPlane& ControlPlane::Instance() {
  std::lock_guard<std::mutex> lock(instance_mutex_);
  if (!instance_) {
    throw std::runtime_error("ControlPlane not initialized. Call Initialize() first.");
  }
  return *instance_;
}

void ControlPlane::Initialize(const ControlPlaneConfig& config) {
  std::lock_guard<std::mutex> lock(instance_mutex_);
  if (!instance_) {
    instance_ = std::make_unique<ControlPlane>(config);
  }
}

void ControlPlane::Shutdown() {
  std::lock_guard<std::mutex> lock(instance_mutex_);
  if (instance_) {
    instance_->Stop();
    instance_.reset();
  }
}

void ControlPlane::InitializeComponents() {
  // 初始化 Worker 管理器
  worker_manager_ = std::make_shared<WorkerManager>(config_.worker_manager_config);
  
  // 初始化任务调度器
  scheduler_ = std::make_unique<TaskScheduler>(config_.scheduler_config, worker_manager_);
  
  // 设置回调
  SetupCallbacks();
  
  std::cout << "[ControlPlane] Components initialized" << std::endl;
}

void ControlPlane::SetupCallbacks() {
  // 设置任务完成回调
  scheduler_->SetTaskCompletedCallback([this](const BackgroundTask& task) {
    OnTaskCompleted(task);
  });
  
  scheduler_->SetTaskFailedCallback([this](const BackgroundTask& task) {
    OnTaskFailed(task);
  });
  
  // 设置 Worker 事件回调
  worker_manager_->SetWorkerEventCallback([this](const WorkerInfo& worker, WorkerEvent event) {
    OnWorkerEvent(worker, event);
  });
}

void ControlPlane::Start() {
  if (running_.exchange(true)) {
    return;  // 已经在运行
  }
  
  // 启动 Worker 管理器
  worker_manager_->Start();
  
  // 启动任务调度器
  scheduler_->Start();
  
  // 启动 gRPC 服务器
  if (!config_.listen_address.empty()) {
    StartGrpcServer();
  }
  
  std::cout << "[ControlPlane] Started on " << config_.listen_address << std::endl;
}

void ControlPlane::Stop() {
  if (!running_.exchange(false)) {
    return;  // 已经停止
  }
  
  // 停止 gRPC 服务器
  StopGrpcServer();
  
  // 停止任务调度器
  if (scheduler_) {
    scheduler_->Stop();
  }
  
  // 停止 Worker 管理器
  if (worker_manager_) {
    worker_manager_->Stop();
  }
  
  std::cout << "[ControlPlane] Stopped" << std::endl;
}

bool ControlPlane::IsRunning() const {
  return running_.load();
}

void ControlPlane::StartGrpcServer() {
  // TODO: 实现 gRPC 服务器启动
  // 在实际实现中，这里会创建并启动 gRPC 服务器
}

void ControlPlane::StopGrpcServer() {
  // TODO: 实现 gRPC 服务器停止
}

// ============================================================================
// 任务管理 API
// ============================================================================

std::string ControlPlane::SubmitCompactionTask(
    const std::string& source_node_id,
    const std::string& db_name,
    uint32_t store_id,
    const CompactionTaskParams& params,
    TaskPriority priority) {
  
  auto task = CreateCompactionTask(source_node_id, db_name, store_id, params, priority);
  
  // 记录任务创建指标
  observability::TaskMetrics::Instance().RecordTaskCreated(
      "compaction", source_node_id);
  
  // 记录日志
  observability::LogEntry entry(observability::LogLevel::kInfo, "Compaction task submitted");
  entry.WithTaskId(task->task_id)
       .WithNodeId(source_node_id)
       .WithField("db_name", db_name)
       .WithField("store_id", static_cast<int64_t>(store_id))
       .WithField("input_files_count", static_cast<int64_t>(params.input_files.size()));
  observability::Logger::Instance().Log(std::move(entry));
  
  // 提交到调度器
  if (scheduler_->SubmitTask(task)) {
    return task->task_id;
  }
  
  return "";
}

std::string ControlPlane::SubmitBulkLoadTask(
    const std::string& source_node_id,
    const std::string& db_name,
    uint32_t store_id,
    const BulkLoadTaskParams& params,
    TaskPriority priority) {
  
  auto task = CreateBulkLoadTask(source_node_id, db_name, store_id, params, priority);
  
  // 记录任务创建指标
  observability::TaskMetrics::Instance().RecordTaskCreated(
      "bulk_load", source_node_id);
  
  // 记录日志
  observability::LogEntry entry(observability::LogLevel::kInfo, "Bulk load task submitted");
  entry.WithTaskId(task->task_id)
       .WithNodeId(source_node_id)
       .WithField("db_name", db_name)
       .WithField("store_id", static_cast<int64_t>(store_id))
       .WithField("source_path", params.source_path);
  observability::Logger::Instance().Log(std::move(entry));
  
  // 提交到调度器
  if (scheduler_->SubmitTask(task)) {
    return task->task_id;
  }
  
  return "";
}

bool ControlPlane::CancelTask(const std::string& task_id, const std::string& reason) {
  bool result = scheduler_->CancelTask(task_id);
  
  if (result) {
    observability::TaskMetrics::Instance().RecordTaskCancelled("unknown", reason);
    
    observability::LogEntry entry(observability::LogLevel::kInfo, "Task cancelled");
    entry.WithTaskId(task_id)
         .WithField("reason", reason);
    observability::Logger::Instance().Log(std::move(entry));
  }
  
  return result;
}

std::shared_ptr<BackgroundTask> ControlPlane::GetTask(const std::string& task_id) const {
  return scheduler_->GetTask(task_id);
}

std::vector<std::shared_ptr<BackgroundTask>> ControlPlane::QueryTasks(
    const TaskFilter& filter) const {
  return scheduler_->QueryTasks(filter);
}

TaskStatistics ControlPlane::GetTaskStatistics() const {
  return scheduler_->GetStatistics();
}

// ============================================================================
// Worker 管理 API
// ============================================================================

std::vector<WorkerInfo> ControlPlane::GetWorkers() const {
  return worker_manager_->GetAllWorkers();
}

std::vector<WorkerInfo> ControlPlane::GetAvailableWorkers() const {
  return worker_manager_->GetAvailableWorkers();
}

size_t ControlPlane::GetWorkerCount() const {
  return worker_manager_->GetWorkerCount();
}

size_t ControlPlane::GetOnlineWorkerCount() const {
  return worker_manager_->GetOnlineWorkerCount();
}

// ============================================================================
// 事件处理
// ============================================================================

void ControlPlane::OnTaskCompleted(const BackgroundTask& task) {
  // 计算执行时间
  double duration_seconds = 0;
  if (task.result.has_value()) {
    duration_seconds = task.result->execution_time_ms / 1000.0;
  }
  
  observability::TaskMetrics::Instance().RecordTaskCompleted(
      TaskTypeToString(task.task_type), "success");
  observability::TaskMetrics::Instance().RecordTotalDuration(
      TaskTypeToString(task.task_type), "completed", duration_seconds);
  
  observability::LogEntry entry(observability::LogLevel::kInfo, "Task completed");
  entry.WithTaskId(task.task_id)
       .WithWorkerId(task.assigned_worker_id)
       .WithField("task_type", TaskTypeToString(task.task_type))
       .WithDuration(duration_seconds);
  
  if (task.result.has_value()) {
    entry.WithField("bytes_read", static_cast<int64_t>(task.result->bytes_read))
         .WithField("bytes_written", static_cast<int64_t>(task.result->bytes_written));
  }
  
  observability::Logger::Instance().Log(std::move(entry));
}

void ControlPlane::OnTaskFailed(const BackgroundTask& task) {
  std::string error_message = task.result.has_value() ? task.result->error_message : "Unknown error";
  
  observability::TaskMetrics::Instance().RecordTaskFailed(
      TaskTypeToString(task.task_type), "execution_error");
  
  observability::LogEntry entry(observability::LogLevel::kError, "Task failed");
  entry.WithTaskId(task.task_id)
       .WithWorkerId(task.assigned_worker_id)
       .WithField("task_type", TaskTypeToString(task.task_type))
       .WithError(error_message)
       .WithField("retry_count", static_cast<int64_t>(task.retry_count));
  observability::Logger::Instance().Log(std::move(entry));
}

void ControlPlane::OnWorkerEvent(const WorkerInfo& worker, WorkerEvent event) {
  std::string event_name;
  observability::LogLevel log_level = observability::LogLevel::kInfo;
  
  switch (event) {
    case WorkerEvent::kRegistered:
      event_name = "registered";
      break;
    case WorkerEvent::kUnregistered:
      event_name = "unregistered";
      break;
    case WorkerEvent::kOnline:
      event_name = "online";
      break;
    case WorkerEvent::kOffline:
      event_name = "offline";
      log_level = observability::LogLevel::kWarn;
      break;
    case WorkerEvent::kOverloaded:
      event_name = "overloaded";
      log_level = observability::LogLevel::kWarn;
      break;
    default:
      event_name = "unknown";
      break;
  }
  
  observability::RecordWorkerEvent(worker.worker_id, event_name, {
      {"address", worker.address},
      {"status", WorkerStatusToString(worker.status)}
  });
}

}  // namespace control_plane
}  // namespace tendisplus
