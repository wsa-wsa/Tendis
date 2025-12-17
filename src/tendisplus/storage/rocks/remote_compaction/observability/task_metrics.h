// Copyright (c) 2024-present, Tencent Inc. All rights reserved.
// 任务相关监控指标

#pragma once

#include <memory>
#include <string>

#include "metrics.h"
#include "../control_plane/task_model.h"

namespace tendisplus {
namespace observability {

using namespace control_plane;

// ============================================================================
// 任务监控指标集合
// ============================================================================
class TaskMetrics {
 public:
  TaskMetrics();
  ~TaskMetrics() = default;
  
  // 单例访问
  static TaskMetrics& Instance();
  
  // =========================================================================
  // 任务生命周期指标
  // =========================================================================
  
  // 任务提交
  void RecordTaskSubmitted(TaskType type, TaskPriority priority);
  
  // 任务入队
  void RecordTaskQueued(TaskType type);
  
  // 任务调度
  void RecordTaskScheduled(TaskType type, const std::string& worker_id);
  
  // 任务开始执行
  void RecordTaskStarted(TaskType type, const std::string& worker_id);
  
  // 任务完成
  void RecordTaskCompleted(TaskType type, const std::string& worker_id,
                           double duration_sec, uint64_t bytes_processed);
  
  // 任务失败
  void RecordTaskFailed(TaskType type, const std::string& worker_id,
                        const std::string& error_type);
  
  // 任务重试
  void RecordTaskRetry(TaskType type, uint32_t retry_count);
  
  // 任务取消
  void RecordTaskCancelled(TaskType type);
  
  // =========================================================================
  // 状态转换指标
  // =========================================================================
  
  // 记录状态转换
  void RecordStateTransition(TaskType type, TaskStatus from, TaskStatus to,
                             const std::string& reason);
  
  // =========================================================================
  // 队列指标
  // =========================================================================
  
  // 更新队列长度
  void UpdateQueueLength(TaskPriority priority, size_t length);
  
  // 记录队列等待时间
  void RecordQueueWaitTime(TaskType type, double wait_time_sec);
  
  // =========================================================================
  // 资源指标
  // =========================================================================
  
  // 记录资源使用
  void RecordResourceUsage(const std::string& worker_id,
                           double cpu_usage, double memory_usage,
                           double disk_usage, double network_usage);
  
  // =========================================================================
  // 数据传输指标
  // =========================================================================
  
  // 记录文件上传
  void RecordFileUpload(uint64_t bytes, double duration_sec);
  
  // 记录文件下载
  void RecordFileDownload(uint64_t bytes, double duration_sec);
  
  // =========================================================================
  // Compaction 特定指标
  // =========================================================================
  
  // 记录 Compaction 输入/输出
  void RecordCompactionIO(int input_level, int output_level,
                          uint64_t bytes_read, uint64_t bytes_written,
                          uint64_t records_processed);
  
  // 记录 Compaction 放大因子
  void RecordCompactionAmplification(double write_amplification);
  
  // =========================================================================
  // Bulk Load 特定指标
  // =========================================================================
  
  // 记录 Bulk Load 数据量
  void RecordBulkLoadData(uint64_t bytes_loaded, uint64_t files_loaded);

 private:
  // 任务计数器
  std::shared_ptr<CounterVec> tasks_submitted_total_;
  std::shared_ptr<CounterVec> tasks_completed_total_;
  std::shared_ptr<CounterVec> tasks_failed_total_;
  std::shared_ptr<CounterVec> tasks_retried_total_;
  std::shared_ptr<CounterVec> tasks_cancelled_total_;
  
  // 状态转换计数器
  std::shared_ptr<CounterVec> state_transitions_total_;
  
  // 当前活跃任务数
  std::shared_ptr<GaugeVec> active_tasks_;
  
  // 队列长度
  std::shared_ptr<GaugeVec> queue_length_;
  
  // 任务执行时间直方图
  std::shared_ptr<HistogramVec> task_duration_seconds_;
  
  // 队列等待时间直方图
  std::shared_ptr<HistogramVec> queue_wait_seconds_;
  
  // 数据处理量
  std::shared_ptr<CounterVec> bytes_processed_total_;
  std::shared_ptr<CounterVec> records_processed_total_;
  
  // 文件传输指标
  std::shared_ptr<CounterVec> file_transfer_bytes_total_;
  std::shared_ptr<HistogramVec> file_transfer_duration_seconds_;
  
  // Compaction 特定指标
  std::shared_ptr<CounterVec> compaction_bytes_read_total_;
  std::shared_ptr<CounterVec> compaction_bytes_written_total_;
  std::shared_ptr<HistogramVec> compaction_write_amplification_;
  
  // Bulk Load 特定指标
  std::shared_ptr<CounterVec> bulk_load_bytes_total_;
  std::shared_ptr<CounterVec> bulk_load_files_total_;
  
  // Worker 资源使用
  std::shared_ptr<GaugeVec> worker_cpu_usage_;
  std::shared_ptr<GaugeVec> worker_memory_usage_;
  std::shared_ptr<GaugeVec> worker_disk_usage_;
  std::shared_ptr<GaugeVec> worker_network_usage_;
};

// ============================================================================
// Worker 监控指标
// ============================================================================
class WorkerMetrics {
 public:
  WorkerMetrics();
  ~WorkerMetrics() = default;
  
  static WorkerMetrics& Instance();
  
  // Worker 注册/注销
  void RecordWorkerRegistered(const std::string& worker_id);
  void RecordWorkerUnregistered(const std::string& worker_id);
  
  // Worker 状态变更
  void RecordWorkerStatusChange(const std::string& worker_id,
                                const std::string& old_status,
                                const std::string& new_status);
  
  // 心跳
  void RecordHeartbeat(const std::string& worker_id);
  void RecordHeartbeatTimeout(const std::string& worker_id);
  
  // 更新 Worker 数量
  void UpdateWorkerCount(size_t total, size_t online, size_t busy);
  
  // 更新资源统计
  void UpdateTotalResources(uint32_t cpu_cores, uint64_t memory_mb, 
                            uint64_t disk_mb);
  void UpdateAvailableResources(uint32_t cpu_cores, uint64_t memory_mb,
                                uint64_t disk_mb);

 private:
  std::shared_ptr<CounterVec> worker_registrations_total_;
  std::shared_ptr<CounterVec> worker_unregistrations_total_;
  std::shared_ptr<CounterVec> worker_status_changes_total_;
  std::shared_ptr<CounterVec> worker_heartbeats_total_;
  std::shared_ptr<CounterVec> worker_heartbeat_timeouts_total_;
  
  std::shared_ptr<Gauge> workers_total_;
  std::shared_ptr<Gauge> workers_online_;
  std::shared_ptr<Gauge> workers_busy_;
  
  std::shared_ptr<Gauge> total_cpu_cores_;
  std::shared_ptr<Gauge> total_memory_mb_;
  std::shared_ptr<Gauge> total_disk_mb_;
  std::shared_ptr<Gauge> available_cpu_cores_;
  std::shared_ptr<Gauge> available_memory_mb_;
  std::shared_ptr<Gauge> available_disk_mb_;
};

// ============================================================================
// 调度器监控指标
// ============================================================================
class SchedulerMetrics {
 public:
  SchedulerMetrics();
  ~SchedulerMetrics() = default;
  
  static SchedulerMetrics& Instance();
  
  // 调度决策
  void RecordSchedulingDecision(bool success, const std::string& reason);
  
  // 调度延迟
  void RecordSchedulingLatency(double latency_sec);
  
  // 调度周期
  void RecordSchedulingCycle(double duration_sec, size_t tasks_scheduled);
  
  // 资源分配
  void RecordResourceAllocation(uint32_t cpu_cores, uint64_t memory_mb);
  void RecordResourceRelease(uint32_t cpu_cores, uint64_t memory_mb);

 private:
  std::shared_ptr<CounterVec> scheduling_decisions_total_;
  std::shared_ptr<Histogram> scheduling_latency_seconds_;
  std::shared_ptr<Histogram> scheduling_cycle_duration_seconds_;
  std::shared_ptr<Counter> tasks_scheduled_total_;
  
  std::shared_ptr<Gauge> allocated_cpu_cores_;
  std::shared_ptr<Gauge> allocated_memory_mb_;
};

}  // namespace observability
}  // namespace tendisplus
