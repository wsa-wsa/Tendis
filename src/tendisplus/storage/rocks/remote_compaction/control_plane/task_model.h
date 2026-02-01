// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

// Task Model - 任务模型定义
// Based on CaaS-LSM architecture

#pragma once

#include <atomic>
#include <chrono>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace tendisplus {
namespace control_plane {

// ============================================================================
// 任务优先级
// ============================================================================
enum class TaskPriority {
  kLow = 0,
  kNormal = 1,
  kHigh = 2,
  kUrgent = 3
};

inline const char* TaskPriorityToString(TaskPriority priority) {
  switch (priority) {
    case TaskPriority::kLow:
      return "Low";
    case TaskPriority::kNormal:
      return "Normal";
    case TaskPriority::kHigh:
      return "High";
    case TaskPriority::kUrgent:
      return "Urgent";
    default:
      return "Unknown";
  }
}

// ============================================================================
// 任务状态
// ============================================================================
enum class TaskStatus {
  kUnknown = 0,
  kPending = 1,     // 等待调度
  kAssigned = 2,    // 已分配给 Worker
  kRunning = 3,     // 正在执行
  kCompleted = 4,   // 成功完成
  kFailed = 5,      // 执行失败
  kCancelled = 6,   // 已取消
  kTimeout = 7      // 超时
};

inline const char* TaskStatusToString(TaskStatus status) {
  switch (status) {
    case TaskStatus::kPending:
      return "Pending";
    case TaskStatus::kAssigned:
      return "Assigned";
    case TaskStatus::kRunning:
      return "Running";
    case TaskStatus::kCompleted:
      return "Completed";
    case TaskStatus::kFailed:
      return "Failed";
    case TaskStatus::kCancelled:
      return "Cancelled";
    case TaskStatus::kTimeout:
      return "Timeout";
    default:
      return "Unknown";
  }
}

// ============================================================================
// 任务类型
// ============================================================================
enum class TaskType {
  kCompaction = 0,   // Compaction 任务
  kBulkLoad = 1      // Bulk Load 任务
};

// ============================================================================
// 任务结果
// ============================================================================
struct TaskResult {
  bool success = false;
  std::string compaction_result;  // 序列化的 compaction 结果
  std::string error_message;

  // 执行统计
  uint64_t execution_time_ms = 0;
  uint64_t bytes_read = 0;
  uint64_t bytes_written = 0;
};

// ============================================================================
// Compaction 任务参数
// ============================================================================
struct CompactionTaskParams {
  uint64_t job_id = 0;
  std::string compaction_input;   // 序列化的 compaction 输入
  std::string shared_fs_uri;
  std::string shared_fs_local_prefix;
  uint32_t timeout_sec = 3600;    // 默认 1 小时

  // CaaS-LSM 调度参数
  int32_t start_level = 0;        // compaction 起始层级 (越低优先级越高)
  double score = 0.0;             // compaction 紧迫度分数 (越高优先级越高)
  std::string compaction_args;    // 原始 compaction 参数 (用于 CSA 执行)
  std::string compaction_addition_info;  // 附加信息
};

// ============================================================================
// 任务信息
// ============================================================================
struct TaskInfo {
  // 基本信息
  std::string task_id;
  TaskType type = TaskType::kCompaction;
  TaskStatus status = TaskStatus::kPending;
  TaskPriority priority = TaskPriority::kNormal;

  // 来源信息
  std::string source_node_id;     // 来源 Tendisplus 节点
  std::string db_name;
  uint32_t store_id = 0;

  // 分配信息
  std::string assigned_worker_id;
  int32_t retry_count = 0;
  int32_t max_retries = 3;
  int32_t reschedule_count = 0;   // CaaS-LSM: 重调度次数

  // 时间信息
  std::chrono::system_clock::time_point submit_time;
  std::chrono::system_clock::time_point assign_time;
  std::chrono::system_clock::time_point start_time;
  std::chrono::system_clock::time_point complete_time;

  // 任务参数
  CompactionTaskParams params;

  // 执行结果
  TaskResult result;

  // 错误信息
  std::string error_message;

  // CaaS-LSM: 是否应该降级到本地执行
  bool should_fallback = false;

  // 辅助方法
  bool IsTerminal() const {
    return status == TaskStatus::kCompleted || status == TaskStatus::kFailed ||
           status == TaskStatus::kCancelled || status == TaskStatus::kTimeout;
  }

  bool CanRetry() const {
    return retry_count < max_retries &&
           (status == TaskStatus::kFailed || status == TaskStatus::kTimeout);
  }

  int64_t GetQueueTimeMs() const {
    if (assign_time > submit_time) {
      return std::chrono::duration_cast<std::chrono::milliseconds>(
               assign_time - submit_time)
        .count();
    }
    return 0;
  }

  int64_t GetExecutionTimeMs() const {
    if (complete_time > start_time) {
      return std::chrono::duration_cast<std::chrono::milliseconds>(
               complete_time - start_time)
        .count();
    }
    return 0;
  }

  std::string ToString() const;
};

// ============================================================================
// 任务过滤器
// ============================================================================
struct TaskFilter {
  std::optional<TaskStatus> status;
  std::optional<TaskPriority> priority;
  std::optional<std::string> source_node_id;
  std::optional<std::string> assigned_worker_id;
  std::optional<std::string> db_name;
  int64_t since_time_ms = 0;      // 提交时间过滤
  uint32_t limit = 100;           // 最大返回数量

  bool Matches(const TaskInfo& task) const;
};

// ============================================================================
// 任务统计
// ============================================================================
struct TaskStatistics {
  std::atomic<uint64_t> total_submitted{0};
  std::atomic<uint64_t> total_completed{0};
  std::atomic<uint64_t> total_failed{0};
  std::atomic<uint64_t> total_cancelled{0};
  std::atomic<uint64_t> total_timeout{0};

  // 当前状态
  std::atomic<uint64_t> pending_count{0};
  std::atomic<uint64_t> running_count{0};

  // 性能统计
  std::atomic<uint64_t> total_execution_time_ms{0};
  std::atomic<uint64_t> total_queue_time_ms{0};

  double GetAvgExecutionTimeMs() const {
    uint64_t completed = total_completed.load();
    if (completed == 0)
      return 0;
    return static_cast<double>(total_execution_time_ms.load()) / completed;
  }

  double GetAvgQueueTimeMs() const {
    uint64_t completed = total_completed.load();
    if (completed == 0)
      return 0;
    return static_cast<double>(total_queue_time_ms.load()) / completed;
  }

  std::string ToString() const;
};

}  // namespace control_plane
}  // namespace tendisplus
