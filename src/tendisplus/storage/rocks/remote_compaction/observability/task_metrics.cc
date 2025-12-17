// Copyright (c) 2024-present, Tencent Inc. All rights reserved.
// 任务指标实现

#include "task_metrics.h"

namespace tendisplus {
namespace observability {

// ============================================================================
// TaskMetrics 实现
// ============================================================================

TaskMetrics& TaskMetrics::Instance() {
  static TaskMetrics instance;
  return instance;
}

TaskMetrics::TaskMetrics() {
  // 注册所有任务相关指标
  RegisterMetrics();
}

void TaskMetrics::RegisterMetrics() {
  auto& registry = MetricsRegistry::Instance();
  
  // 任务计数指标
  task_created_total_ = registry.RegisterOrGet<CounterVec>(
      "tendis_task_created_total",
      "Total number of tasks created",
      {"task_type", "source_node"});
  
  task_completed_total_ = registry.RegisterOrGet<CounterVec>(
      "tendis_task_completed_total",
      "Total number of tasks completed",
      {"task_type", "status"});
  
  task_failed_total_ = registry.RegisterOrGet<CounterVec>(
      "tendis_task_failed_total",
      "Total number of tasks failed",
      {"task_type", "error_type"});
  
  task_retried_total_ = registry.RegisterOrGet<CounterVec>(
      "tendis_task_retried_total",
      "Total number of task retries",
      {"task_type"});
  
  task_cancelled_total_ = registry.RegisterOrGet<CounterVec>(
      "tendis_task_cancelled_total",
      "Total number of tasks cancelled",
      {"task_type", "reason"});
  
  // 任务状态指标
  tasks_in_queue_ = registry.RegisterOrGet<GaugeVec>(
      "tendis_tasks_in_queue",
      "Number of tasks currently in queue",
      {"task_type", "priority"});
  
  tasks_running_ = registry.RegisterOrGet<GaugeVec>(
      "tendis_tasks_running",
      "Number of tasks currently running",
      {"task_type", "worker_id"});
  
  // 延迟指标
  task_queue_duration_seconds_ = registry.RegisterOrGet<HistogramVec>(
      "tendis_task_queue_duration_seconds",
      "Time spent in queue before scheduling",
      {"task_type"},
      Histogram::ExponentialBuckets(0.001, 2, 15));
  
  task_schedule_duration_seconds_ = registry.RegisterOrGet<HistogramVec>(
      "tendis_task_schedule_duration_seconds",
      "Time spent in scheduling phase",
      {"task_type"},
      Histogram::ExponentialBuckets(0.0001, 2, 12));
  
  task_execution_duration_seconds_ = registry.RegisterOrGet<HistogramVec>(
      "tendis_task_execution_duration_seconds",
      "Time spent executing task",
      {"task_type", "worker_id"},
      Histogram::ExponentialBuckets(0.01, 2, 18));
  
  task_total_duration_seconds_ = registry.RegisterOrGet<HistogramVec>(
      "tendis_task_total_duration_seconds",
      "Total time from creation to completion",
      {"task_type", "status"},
      Histogram::ExponentialBuckets(0.01, 2, 18));
  
  // 数据传输指标
  data_transferred_bytes_ = registry.RegisterOrGet<CounterVec>(
      "tendis_data_transferred_bytes_total",
      "Total bytes transferred",
      {"direction", "task_type"});
  
  data_transfer_duration_seconds_ = registry.RegisterOrGet<HistogramVec>(
      "tendis_data_transfer_duration_seconds",
      "Duration of data transfer operations",
      {"direction", "task_type"},
      Histogram::ExponentialBuckets(0.001, 2, 15));
  
  // Compaction 特定指标
  compaction_input_bytes_ = registry.RegisterOrGet<CounterVec>(
      "tendis_compaction_input_bytes_total",
      "Total input bytes for compaction",
      {"level", "node_id"});
  
  compaction_output_bytes_ = registry.RegisterOrGet<CounterVec>(
      "tendis_compaction_output_bytes_total",
      "Total output bytes from compaction",
      {"level", "node_id"});
  
  compaction_input_files_ = registry.RegisterOrGet<CounterVec>(
      "tendis_compaction_input_files_total",
      "Total input files for compaction",
      {"level"});
  
  compaction_output_files_ = registry.RegisterOrGet<CounterVec>(
      "tendis_compaction_output_files_total",
      "Total output files from compaction",
      {"level"});
}

void TaskMetrics::RecordTaskCreated(const std::string& task_type,
                                    const std::string& source_node) {
  if (task_created_total_) {
    task_created_total_->Add({{"task_type", task_type}, 
                               {"source_node", source_node}}, 1);
  }
}

void TaskMetrics::RecordTaskCompleted(const std::string& task_type,
                                       const std::string& status) {
  if (task_completed_total_) {
    task_completed_total_->Add({{"task_type", task_type}, 
                                 {"status", status}}, 1);
  }
}

void TaskMetrics::RecordTaskFailed(const std::string& task_type,
                                    const std::string& error_type) {
  if (task_failed_total_) {
    task_failed_total_->Add({{"task_type", task_type}, 
                              {"error_type", error_type}}, 1);
  }
}

void TaskMetrics::RecordTaskRetried(const std::string& task_type) {
  if (task_retried_total_) {
    task_retried_total_->Add({{"task_type", task_type}}, 1);
  }
}

void TaskMetrics::RecordTaskCancelled(const std::string& task_type,
                                       const std::string& reason) {
  if (task_cancelled_total_) {
    task_cancelled_total_->Add({{"task_type", task_type}, 
                                 {"reason", reason}}, 1);
  }
}

void TaskMetrics::SetTasksInQueue(const std::string& task_type,
                                   const std::string& priority,
                                   int64_t count) {
  if (tasks_in_queue_) {
    tasks_in_queue_->Set({{"task_type", task_type}, 
                          {"priority", priority}}, 
                         static_cast<double>(count));
  }
}

void TaskMetrics::SetTasksRunning(const std::string& task_type,
                                   const std::string& worker_id,
                                   int64_t count) {
  if (tasks_running_) {
    tasks_running_->Set({{"task_type", task_type}, 
                         {"worker_id", worker_id}}, 
                        static_cast<double>(count));
  }
}

void TaskMetrics::RecordQueueDuration(const std::string& task_type,
                                       double duration_seconds) {
  if (task_queue_duration_seconds_) {
    task_queue_duration_seconds_->Observe({{"task_type", task_type}}, 
                                           duration_seconds);
  }
}

void TaskMetrics::RecordScheduleDuration(const std::string& task_type,
                                          double duration_seconds) {
  if (task_schedule_duration_seconds_) {
    task_schedule_duration_seconds_->Observe({{"task_type", task_type}}, 
                                              duration_seconds);
  }
}

void TaskMetrics::RecordExecutionDuration(const std::string& task_type,
                                           const std::string& worker_id,
                                           double duration_seconds) {
  if (task_execution_duration_seconds_) {
    task_execution_duration_seconds_->Observe(
        {{"task_type", task_type}, {"worker_id", worker_id}}, 
        duration_seconds);
  }
}

void TaskMetrics::RecordTotalDuration(const std::string& task_type,
                                       const std::string& status,
                                       double duration_seconds) {
  if (task_total_duration_seconds_) {
    task_total_duration_seconds_->Observe(
        {{"task_type", task_type}, {"status", status}}, 
        duration_seconds);
  }
}

void TaskMetrics::RecordDataTransferred(const std::string& direction,
                                         const std::string& task_type,
                                         uint64_t bytes) {
  if (data_transferred_bytes_) {
    data_transferred_bytes_->Add(
        {{"direction", direction}, {"task_type", task_type}}, 
        static_cast<double>(bytes));
  }
}

void TaskMetrics::RecordDataTransferDuration(const std::string& direction,
                                              const std::string& task_type,
                                              double duration_seconds) {
  if (data_transfer_duration_seconds_) {
    data_transfer_duration_seconds_->Observe(
        {{"direction", direction}, {"task_type", task_type}}, 
        duration_seconds);
  }
}

void TaskMetrics::RecordCompactionInput(const std::string& level,
                                         const std::string& node_id,
                                         uint64_t bytes,
                                         uint32_t files) {
  if (compaction_input_bytes_) {
    compaction_input_bytes_->Add(
        {{"level", level}, {"node_id", node_id}}, 
        static_cast<double>(bytes));
  }
  if (compaction_input_files_) {
    compaction_input_files_->Add({{"level", level}}, 
                                  static_cast<double>(files));
  }
}

void TaskMetrics::RecordCompactionOutput(const std::string& level,
                                          const std::string& node_id,
                                          uint64_t bytes,
                                          uint32_t files) {
  if (compaction_output_bytes_) {
    compaction_output_bytes_->Add(
        {{"level", level}, {"node_id", node_id}}, 
        static_cast<double>(bytes));
  }
  if (compaction_output_files_) {
    compaction_output_files_->Add({{"level", level}}, 
                                   static_cast<double>(files));
  }
}

// ============================================================================
// WorkerMetrics 实现
// ============================================================================

WorkerMetrics& WorkerMetrics::Instance() {
  static WorkerMetrics instance;
  return instance;
}

WorkerMetrics::WorkerMetrics() {
  RegisterMetrics();
}

void WorkerMetrics::RegisterMetrics() {
  auto& registry = MetricsRegistry::Instance();
  
  // Worker 状态指标
  workers_total_ = registry.RegisterOrGet<GaugeVec>(
      "tendis_workers_total",
      "Total number of workers",
      {"status"});
  
  worker_cpu_usage_ = registry.RegisterOrGet<GaugeVec>(
      "tendis_worker_cpu_usage",
      "CPU usage percentage of worker",
      {"worker_id"});
  
  worker_memory_usage_ = registry.RegisterOrGet<GaugeVec>(
      "tendis_worker_memory_usage_bytes",
      "Memory usage of worker in bytes",
      {"worker_id"});
  
  worker_disk_usage_ = registry.RegisterOrGet<GaugeVec>(
      "tendis_worker_disk_usage_bytes",
      "Disk usage of worker in bytes",
      {"worker_id"});
  
  worker_task_slots_ = registry.RegisterOrGet<GaugeVec>(
      "tendis_worker_task_slots",
      "Number of task slots on worker",
      {"worker_id", "slot_type"});
  
  // Worker 任务指标
  worker_tasks_executed_ = registry.RegisterOrGet<CounterVec>(
      "tendis_worker_tasks_executed_total",
      "Total tasks executed by worker",
      {"worker_id", "task_type", "status"});
  
  worker_task_duration_seconds_ = registry.RegisterOrGet<HistogramVec>(
      "tendis_worker_task_duration_seconds",
      "Task execution duration on worker",
      {"worker_id", "task_type"},
      Histogram::ExponentialBuckets(0.01, 2, 18));
  
  // 心跳指标
  worker_heartbeat_latency_seconds_ = registry.RegisterOrGet<HistogramVec>(
      "tendis_worker_heartbeat_latency_seconds",
      "Worker heartbeat latency",
      {"worker_id"},
      Histogram::ExponentialBuckets(0.0001, 2, 12));
  
  worker_last_heartbeat_timestamp_ = registry.RegisterOrGet<GaugeVec>(
      "tendis_worker_last_heartbeat_timestamp",
      "Timestamp of last heartbeat from worker",
      {"worker_id"});
}

void WorkerMetrics::SetWorkersTotal(const std::string& status, int64_t count) {
  if (workers_total_) {
    workers_total_->Set({{"status", status}}, static_cast<double>(count));
  }
}

void WorkerMetrics::SetWorkerCpuUsage(const std::string& worker_id, double usage) {
  if (worker_cpu_usage_) {
    worker_cpu_usage_->Set({{"worker_id", worker_id}}, usage);
  }
}

void WorkerMetrics::SetWorkerMemoryUsage(const std::string& worker_id, uint64_t bytes) {
  if (worker_memory_usage_) {
    worker_memory_usage_->Set({{"worker_id", worker_id}}, 
                               static_cast<double>(bytes));
  }
}

void WorkerMetrics::SetWorkerDiskUsage(const std::string& worker_id, uint64_t bytes) {
  if (worker_disk_usage_) {
    worker_disk_usage_->Set({{"worker_id", worker_id}}, 
                             static_cast<double>(bytes));
  }
}

void WorkerMetrics::SetWorkerTaskSlots(const std::string& worker_id,
                                        const std::string& slot_type,
                                        int64_t count) {
  if (worker_task_slots_) {
    worker_task_slots_->Set({{"worker_id", worker_id}, 
                              {"slot_type", slot_type}}, 
                             static_cast<double>(count));
  }
}

void WorkerMetrics::RecordWorkerTaskExecuted(const std::string& worker_id,
                                              const std::string& task_type,
                                              const std::string& status) {
  if (worker_tasks_executed_) {
    worker_tasks_executed_->Add({{"worker_id", worker_id}, 
                                  {"task_type", task_type}, 
                                  {"status", status}}, 1);
  }
}

void WorkerMetrics::RecordWorkerTaskDuration(const std::string& worker_id,
                                              const std::string& task_type,
                                              double duration_seconds) {
  if (worker_task_duration_seconds_) {
    worker_task_duration_seconds_->Observe(
        {{"worker_id", worker_id}, {"task_type", task_type}}, 
        duration_seconds);
  }
}

void WorkerMetrics::RecordHeartbeatLatency(const std::string& worker_id,
                                            double latency_seconds) {
  if (worker_heartbeat_latency_seconds_) {
    worker_heartbeat_latency_seconds_->Observe(
        {{"worker_id", worker_id}}, latency_seconds);
  }
}

void WorkerMetrics::SetLastHeartbeatTimestamp(const std::string& worker_id,
                                               uint64_t timestamp) {
  if (worker_last_heartbeat_timestamp_) {
    worker_last_heartbeat_timestamp_->Set(
        {{"worker_id", worker_id}}, static_cast<double>(timestamp));
  }
}

// ============================================================================
// SystemMetrics 实现
// ============================================================================

SystemMetrics& SystemMetrics::Instance() {
  static SystemMetrics instance;
  return instance;
}

SystemMetrics::SystemMetrics() {
  RegisterMetrics();
}

void SystemMetrics::RegisterMetrics() {
  auto& registry = MetricsRegistry::Instance();
  
  // 控制平面指标
  scheduler_queue_size_ = registry.RegisterOrGet<GaugeVec>(
      "tendis_scheduler_queue_size",
      "Number of tasks in scheduler queue",
      {"priority"});
  
  scheduler_decisions_total_ = registry.RegisterOrGet<CounterVec>(
      "tendis_scheduler_decisions_total",
      "Total scheduling decisions made",
      {"decision_type"});
  
  scheduler_decision_duration_seconds_ = registry.RegisterOrGet<HistogramVec>(
      "tendis_scheduler_decision_duration_seconds",
      "Time to make scheduling decision",
      {},
      Histogram::ExponentialBuckets(0.0001, 2, 12));
  
  // 系统健康指标
  system_healthy_ = registry.RegisterOrGet<Gauge>(
      "tendis_system_healthy",
      "Whether the system is healthy (1=healthy, 0=unhealthy)");
  
  control_plane_uptime_seconds_ = registry.RegisterOrGet<Counter>(
      "tendis_control_plane_uptime_seconds",
      "Control plane uptime in seconds");
  
  // 错误指标
  errors_total_ = registry.RegisterOrGet<CounterVec>(
      "tendis_errors_total",
      "Total number of errors",
      {"component", "error_type"});
}

void SystemMetrics::SetSchedulerQueueSize(const std::string& priority, int64_t size) {
  if (scheduler_queue_size_) {
    scheduler_queue_size_->Set({{"priority", priority}}, 
                                static_cast<double>(size));
  }
}

void SystemMetrics::RecordSchedulerDecision(const std::string& decision_type) {
  if (scheduler_decisions_total_) {
    scheduler_decisions_total_->Add({{"decision_type", decision_type}}, 1);
  }
}

void SystemMetrics::RecordSchedulerDecisionDuration(double duration_seconds) {
  if (scheduler_decision_duration_seconds_) {
    scheduler_decision_duration_seconds_->Observe({}, duration_seconds);
  }
}

void SystemMetrics::SetSystemHealthy(bool healthy) {
  if (system_healthy_) {
    system_healthy_->Set(healthy ? 1.0 : 0.0);
  }
}

void SystemMetrics::IncrementUptime(double seconds) {
  if (control_plane_uptime_seconds_) {
    control_plane_uptime_seconds_->Add(seconds);
  }
}

void SystemMetrics::RecordError(const std::string& component,
                                 const std::string& error_type) {
  if (errors_total_) {
    errors_total_->Add({{"component", component}, 
                         {"error_type", error_type}}, 1);
  }
}

}  // namespace observability
}  // namespace tendisplus
