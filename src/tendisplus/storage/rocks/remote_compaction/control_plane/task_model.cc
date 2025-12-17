// Copyright (c) 2024-present, Tencent Inc. All rights reserved.
// 统一后台任务模型实现

#include "task_model.h"

#include <sstream>
#include <iomanip>
#include <chrono>
#include <random>

namespace tendisplus {
namespace control_plane {

// ============================================================================
// 辅助函数
// ============================================================================

static std::string GenerateTaskId() {
  static thread_local std::random_device rd;
  static thread_local std::mt19937_64 gen(rd());
  static thread_local std::uniform_int_distribution<uint64_t> dis;
  
  auto now = std::chrono::system_clock::now();
  auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
      now.time_since_epoch()).count();
  
  uint64_t random_part = dis(gen);
  
  std::ostringstream oss;
  oss << std::hex << std::setfill('0')
      << std::setw(12) << timestamp << "-"
      << std::setw(16) << random_part;
  
  return oss.str();
}

// ============================================================================
// ResourceRequirement 实现
// ============================================================================

std::string ResourceRequirement::ToString() const {
  std::ostringstream oss;
  oss << "{cpu_cores=" << cpu_cores
      << ", memory_mb=" << memory_mb
      << ", disk_mb=" << disk_mb
      << ", network_bandwidth_mbps=" << network_bandwidth_mbps
      << ", estimated_duration_sec=" << estimated_duration_sec << "}";
  return oss.str();
}

// ============================================================================
// StateTransition 实现
// ============================================================================

std::string StateTransition::ToString() const {
  std::ostringstream oss;
  oss << TaskStatusToString(from_status) << " -> " 
      << TaskStatusToString(to_status)
      << " [reason: " << reason << ", operator: " << operator_id << "]";
  return oss.str();
}

// ============================================================================
// TaskResult 实现
// ============================================================================

std::string TaskResult::ToString() const {
  std::ostringstream oss;
  oss << "{success=" << (success ? "true" : "false");
  if (!success) {
    oss << ", error_message=\"" << error_message << "\"";
  }
  oss << ", bytes_read=" << bytes_read
      << ", bytes_written=" << bytes_written
      << ", records_processed=" << records_processed
      << ", execution_time_ms=" << execution_time_ms
      << ", output_files_count=" << output_files.size() << "}";
  return oss.str();
}

// ============================================================================
// BackgroundTask 实现
// ============================================================================

std::string BackgroundTask::ToString() const {
  std::ostringstream oss;
  oss << "Task{id=" << task_id
      << ", type=" << TaskTypeToString(task_type)
      << ", status=" << TaskStatusToString(status)
      << ", priority=" << TaskPriorityToString(priority)
      << ", source_node=" << source_node_id
      << ", worker=" << assigned_worker_id
      << ", retry_count=" << retry_count << "/" << retry_policy.max_retries
      << "}";
  return oss.str();
}

bool BackgroundTask::CanTransitionTo(TaskStatus new_status) const {
  // 定义合法的状态转换
  switch (status) {
    case TaskStatus::kCreated:
      return new_status == TaskStatus::kQueued ||
             new_status == TaskStatus::kCancelled;
    
    case TaskStatus::kQueued:
      return new_status == TaskStatus::kScheduled ||
             new_status == TaskStatus::kCancelled;
    
    case TaskStatus::kScheduled:
      return new_status == TaskStatus::kRunning ||
             new_status == TaskStatus::kQueued ||  // 重新入队
             new_status == TaskStatus::kCancelled;
    
    case TaskStatus::kRunning:
      return new_status == TaskStatus::kCompleted ||
             new_status == TaskStatus::kFailed ||
             new_status == TaskStatus::kCancelled;
    
    case TaskStatus::kFailed:
      return new_status == TaskStatus::kRetrying ||
             new_status == TaskStatus::kCancelled;
    
    case TaskStatus::kRetrying:
      return new_status == TaskStatus::kQueued ||
             new_status == TaskStatus::kCancelled;
    
    case TaskStatus::kCompleted:
    case TaskStatus::kCancelled:
      // 终态，不能再转换
      return false;
    
    default:
      return false;
  }
}

void BackgroundTask::RecordStateTransition(TaskStatus new_status,
                                            const std::string& reason,
                                            const std::string& operator_id) {
  StateTransition transition;
  transition.from_status = status;
  transition.to_status = new_status;
  transition.timestamp = std::chrono::system_clock::now();
  transition.reason = reason;
  transition.operator_id = operator_id;
  
  state_history.push_back(std::move(transition));
  
  // 更新时间戳
  switch (new_status) {
    case TaskStatus::kQueued:
      queued_at = std::chrono::system_clock::now();
      break;
    case TaskStatus::kScheduled:
      scheduled_at = std::chrono::system_clock::now();
      break;
    case TaskStatus::kRunning:
      started_at = std::chrono::system_clock::now();
      break;
    case TaskStatus::kCompleted:
    case TaskStatus::kFailed:
    case TaskStatus::kCancelled:
      completed_at = std::chrono::system_clock::now();
      break;
    default:
      break;
  }
  
  // 更新状态
  status = new_status;
  version++;
}

uint64_t BackgroundTask::GetElapsedTimeMs() const {
  auto now = std::chrono::system_clock::now();
  return std::chrono::duration_cast<std::chrono::milliseconds>(
      now - created_at).count();
}

// ============================================================================
// TaskStatistics 实现
// ============================================================================

std::string TaskStatistics::ToString() const {
  std::ostringstream oss;
  oss << "TaskStatistics{"
      << "total=" << total_tasks
      << ", active=" << active_tasks
      << ", completed=" << completed_tasks
      << ", failed=" << failed_tasks
      << ", avg_execution_time_ms=" << std::fixed << std::setprecision(2) 
      << avg_execution_time_ms
      << ", avg_queue_time_ms=" << avg_queue_time_ms << "}";
  return oss.str();
}

// ============================================================================
// 任务工厂函数
// ============================================================================

std::shared_ptr<BackgroundTask> CreateCompactionTask(
    const std::string& source_node_id,
    const std::string& db_name,
    uint32_t store_id,
    const CompactionTaskParams& params,
    TaskPriority priority) {
  
  auto task = std::make_shared<BackgroundTask>();
  task->task_id = GenerateTaskId();
  task->task_type = TaskType::kCompaction;
  task->status = TaskStatus::kCreated;
  task->priority = priority;
  task->source_node_id = source_node_id;
  task->source_db_name = db_name;
  task->store_id = store_id;
  task->compaction_params = params;
  task->created_at = std::chrono::system_clock::now();
  
  // 根据输入文件数量估算资源需求
  task->resource_requirement.cpu_cores = 2;
  task->resource_requirement.memory_mb = 512 + params.input_files.size() * 64;
  task->resource_requirement.disk_mb = 0;
  for (const auto& file : params.input_files) {
    task->resource_requirement.disk_mb += file.file_size / (1024 * 1024);
  }
  task->resource_requirement.disk_mb *= 2;  // 输出可能需要同样的空间
  
  return task;
}

std::shared_ptr<BackgroundTask> CreateBulkLoadTask(
    const std::string& source_node_id,
    const std::string& db_name,
    uint32_t store_id,
    const BulkLoadTaskParams& params,
    TaskPriority priority) {
  
  auto task = std::make_shared<BackgroundTask>();
  task->task_id = GenerateTaskId();
  task->task_type = TaskType::kBulkLoad;
  task->status = TaskStatus::kCreated;
  task->priority = priority;
  task->source_node_id = source_node_id;
  task->source_db_name = db_name;
  task->store_id = store_id;
  task->bulk_load_params = params;
  task->created_at = std::chrono::system_clock::now();
  
  // Bulk Load 通常需要更多资源
  task->resource_requirement.cpu_cores = 4;
  task->resource_requirement.memory_mb = 1024;
  task->resource_requirement.disk_mb = 2048;
  
  return task;
}

}  // namespace control_plane
}  // namespace tendisplus
