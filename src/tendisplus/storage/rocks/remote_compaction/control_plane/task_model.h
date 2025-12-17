// Copyright (c) 2024-present, Tencent Inc. All rights reserved.
// 统一后台任务模型定义

#pragma once

#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace tendisplus {
namespace control_plane {

// ============================================================================
// 任务类型定义
// ============================================================================
enum class TaskType {
  kCompaction = 0,     // Compaction 任务
  kBulkLoad = 1,       // Bulk Load 任务
  kBackup = 2,         // 备份任务
  kRestore = 3,        // 恢复任务
  kUnknown = 99
};

inline const char* TaskTypeToString(TaskType type) {
  switch (type) {
    case TaskType::kCompaction: return "Compaction";
    case TaskType::kBulkLoad: return "BulkLoad";
    case TaskType::kBackup: return "Backup";
    case TaskType::kRestore: return "Restore";
    default: return "Unknown";
  }
}

// ============================================================================
// 任务状态定义 (状态机)
// ============================================================================
enum class TaskStatus {
  kCreated = 0,      // 已创建 - 任务刚创建
  kQueued = 1,       // 已入队 - 任务已进入调度队列
  kScheduled = 2,    // 已调度 - 任务已分配给 Worker
  kRunning = 3,      // 执行中 - Worker 正在执行任务
  kCompleted = 4,    // 已完成 - 任务成功完成
  kFailed = 5,       // 已失败 - 任务执行失败
  kRetrying = 6,     // 重试中 - 任务正在重试
  kCancelled = 7     // 已取消 - 任务被取消
};

inline const char* TaskStatusToString(TaskStatus status) {
  switch (status) {
    case TaskStatus::kCreated: return "Created";
    case TaskStatus::kQueued: return "Queued";
    case TaskStatus::kScheduled: return "Scheduled";
    case TaskStatus::kRunning: return "Running";
    case TaskStatus::kCompleted: return "Completed";
    case TaskStatus::kFailed: return "Failed";
    case TaskStatus::kRetrying: return "Retrying";
    case TaskStatus::kCancelled: return "Cancelled";
    default: return "Unknown";
  }
}

// ============================================================================
// 任务优先级定义
// ============================================================================
enum class TaskPriority {
  kLow = 0,          // 低优先级
  kNormal = 1,       // 普通优先级
  kHigh = 2,         // 高优先级
  kUrgent = 3        // 紧急优先级
};

inline const char* TaskPriorityToString(TaskPriority priority) {
  switch (priority) {
    case TaskPriority::kLow: return "Low";
    case TaskPriority::kNormal: return "Normal";
    case TaskPriority::kHigh: return "High";
    case TaskPriority::kUrgent: return "Urgent";
    default: return "Unknown";
  }
}

// ============================================================================
// 资源需求定义
// ============================================================================
struct ResourceRequirement {
  uint32_t cpu_cores = 1;           // CPU 核心数
  uint64_t memory_mb = 512;         // 内存需求 (MB)
  uint64_t disk_mb = 1024;          // 磁盘空间需求 (MB)
  uint64_t network_bandwidth_mbps = 100;  // 网络带宽需求 (Mbps)
  uint32_t estimated_duration_sec = 60;   // 预估执行时间 (秒)
  
  std::string ToString() const;
};

// ============================================================================
// 重试策略定义
// ============================================================================
struct RetryPolicy {
  uint32_t max_retries = 3;              // 最大重试次数
  uint32_t initial_delay_ms = 1000;      // 初始重试延迟 (毫秒)
  uint32_t max_delay_ms = 60000;         // 最大重试延迟 (毫秒)
  double backoff_multiplier = 2.0;       // 指数退避系数
  bool retry_on_timeout = true;          // 超时是否重试
  bool retry_on_worker_failure = true;   // Worker 故障是否重试
  
  // 计算第 n 次重试的延迟
  uint32_t GetRetryDelay(uint32_t retry_count) const {
    if (retry_count == 0) return 0;
    uint32_t delay = initial_delay_ms;
    for (uint32_t i = 1; i < retry_count && delay < max_delay_ms; ++i) {
      delay = static_cast<uint32_t>(delay * backoff_multiplier);
    }
    return std::min(delay, max_delay_ms);
  }
};

// ============================================================================
// 状态转换记录
// ============================================================================
struct StateTransition {
  TaskStatus from_status;
  TaskStatus to_status;
  std::chrono::system_clock::time_point timestamp;
  std::string reason;
  std::string operator_id;  // 操作者 ID (系统/用户)
  
  std::string ToString() const;
};

// ============================================================================
// SST 文件信息
// ============================================================================
struct SstFileInfo {
  std::string file_name;
  uint64_t file_number = 0;
  int level = 0;
  uint64_t file_size = 0;
  std::string smallest_key;
  std::string largest_key;
  uint64_t smallest_seqno = 0;
  uint64_t largest_seqno = 0;
  uint64_t epoch_number = 0;
  std::string checksum;
};

// ============================================================================
// Compaction 任务特定参数
// ============================================================================
struct CompactionTaskParams {
  int input_level = 0;               // 输入层级
  int output_level = 1;              // 输出层级
  std::vector<SstFileInfo> input_files;   // 输入 SST 文件列表
  std::string db_path;               // 数据库路径
  std::string output_path;           // 输出路径
  std::string compaction_input;      // RocksDB compaction input 序列化数据
  uint64_t target_file_size = 0;     // 目标文件大小
  bool is_manual = false;            // 是否手动触发
};

// ============================================================================
// Bulk Load 任务特定参数
// ============================================================================
struct BulkLoadTaskParams {
  std::string source_path;           // 数据源路径
  std::string target_db_path;        // 目标数据库路径
  std::vector<std::string> data_files;  // 数据文件列表
  bool verify_checksum = true;       // 是否校验校验和
  bool move_files = false;           // 是否移动文件 (vs 复制)
};

// ============================================================================
// 任务执行结果
// ============================================================================
struct TaskResult {
  bool success = false;
  std::string error_message;
  std::vector<SstFileInfo> output_files;  // 输出文件列表
  
  // 统计信息
  uint64_t bytes_read = 0;
  uint64_t bytes_written = 0;
  uint64_t records_processed = 0;
  uint64_t execution_time_ms = 0;
  
  // 校验信息
  std::map<std::string, std::string> file_checksums;
  
  std::string ToString() const;
};

// ============================================================================
// 统一后台任务模型
// ============================================================================
struct BackgroundTask {
  // 基本信息
  std::string task_id;               // 全局唯一任务 ID
  TaskType task_type = TaskType::kUnknown;
  TaskStatus status = TaskStatus::kCreated;
  TaskPriority priority = TaskPriority::kNormal;
  
  // 来源信息
  std::string source_node_id;        // 发起任务的 TendisPlus 节点 ID
  std::string source_db_name;        // 数据库名称
  uint32_t store_id = 0;             // Store ID
  
  // 资源与调度
  ResourceRequirement resource_requirement;
  std::string assigned_worker_id;    // 分配的 Worker ID
  
  // 依赖关系
  std::vector<std::string> depends_on;  // 依赖的任务 ID 列表
  std::vector<std::string> blocked_by;  // 被阻塞的任务 ID 列表
  
  // 重试策略
  RetryPolicy retry_policy;
  uint32_t retry_count = 0;          // 当前重试次数
  
  // 时间信息
  std::chrono::system_clock::time_point created_at;
  std::chrono::system_clock::time_point queued_at;
  std::chrono::system_clock::time_point scheduled_at;
  std::chrono::system_clock::time_point started_at;
  std::chrono::system_clock::time_point completed_at;
  std::optional<std::chrono::system_clock::time_point> deadline;  // 可选的截止时间
  
  // 状态转换历史
  std::vector<StateTransition> state_history;
  
  // 任务特定参数 (使用 variant 或 union)
  std::optional<CompactionTaskParams> compaction_params;
  std::optional<BulkLoadTaskParams> bulk_load_params;
  
  // 执行结果
  std::optional<TaskResult> result;
  
  // 元数据
  std::map<std::string, std::string> metadata;
  
  // 版本控制
  uint64_t version = 0;              // 乐观锁版本号
  
  // 辅助方法
  std::string ToString() const;
  bool CanTransitionTo(TaskStatus new_status) const;
  void RecordStateTransition(TaskStatus new_status, const std::string& reason,
                             const std::string& operator_id = "system");
  uint64_t GetElapsedTimeMs() const;
  bool IsTerminal() const {
    return status == TaskStatus::kCompleted || 
           status == TaskStatus::kFailed ||
           status == TaskStatus::kCancelled;
  }
  bool CanRetry() const {
    return status == TaskStatus::kFailed && 
           retry_count < retry_policy.max_retries;
  }
};

// ============================================================================
// 任务过滤条件
// ============================================================================
struct TaskFilter {
  std::optional<TaskType> task_type;
  std::optional<TaskStatus> status;
  std::optional<TaskPriority> priority;
  std::optional<std::string> source_node_id;
  std::optional<std::string> assigned_worker_id;
  std::optional<std::chrono::system_clock::time_point> created_after;
  std::optional<std::chrono::system_clock::time_point> created_before;
  uint32_t limit = 100;
  uint32_t offset = 0;
};

// ============================================================================
// 任务统计信息
// ============================================================================
struct TaskStatistics {
  std::map<TaskStatus, uint64_t> status_counts;
  std::map<TaskType, uint64_t> type_counts;
  uint64_t total_tasks = 0;
  uint64_t active_tasks = 0;
  uint64_t completed_tasks = 0;
  uint64_t failed_tasks = 0;
  double avg_execution_time_ms = 0;
  double avg_queue_time_ms = 0;
  
  std::string ToString() const;
};

}  // namespace control_plane
}  // namespace tendisplus
