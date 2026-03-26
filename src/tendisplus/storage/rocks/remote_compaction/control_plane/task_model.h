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
#include <mutex>
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
  kTimeout = 7,     // 超时
  kRetrying = 8     // 重试中 (CaaS-LSM: 任务失败/超时后等待重新调度)
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
    case TaskStatus::kRetrying:
      return "Retrying";
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

inline const char* TaskTypeToString(TaskType type) {
  switch (type) {
    case TaskType::kCompaction:
      return "Compaction";
    case TaskType::kBulkLoad:
      return "BulkLoad";
    default:
      return "Unknown";
  }
}

// ============================================================================
// Bulk Load 数据源类型
// ============================================================================
enum class DataSourceType {
  kKVFile = 0,          // 原始 KV 文件
  kSSTFile = 1,         // 已有 SST 文件（直接注入）
  kRDBFile = 2,         // Redis RDB 文件
  kTendisDump = 3,      // TendisPlus dump 文件
  kSnapshot = 4         // 另一个 TendisPlus 实例的快照
};

// ============================================================================
// Bulk Load 数据格式
// ============================================================================
enum class DataFormat {
  kTendisplusEncoded = 0,   // TendisPlus RecordKey/RecordValue 编码
  kRawKV = 1,               // 原始 key-value（需要编码转换）
  kSSTNative = 2            // 原生 RocksDB SST 文件
};

// ============================================================================
// Bulk Load 分片策略
// ============================================================================
enum class ShardingStrategy {
  kByKeyRange = 0,    // 按 key 范围切分
  kBySlot = 1,        // 按 TendisPlus slot 切分
  kBySize = 2,        // 按数据量均分
  kAuto = 3           // 自动选择
};

// ============================================================================
// Bulk Load 压缩类型
// ============================================================================
enum class CompressionType {
  kNone = 0,
  kSnappy = 1,
  kZlib = 2,
  kLZ4 = 3,
  kZSTD = 4
};

// ============================================================================
// Bulk Load 子任务阶段
// ============================================================================
enum class BulkLoadPhase {
  kCreated = 0,          // 任务已创建
  kPlanning = 1,         // 分片规划中
  kQueued = 2,           // 子任务已入队
  kSSTGenerating = 3,    // SST 文件生成中
  kIngesting = 4,        // SST 文件注入中
  kCompleted = 5,        // 完成
  kFailed = 6,           // 失败
  kCancelled = 7         // 已取消
};

inline const char* BulkLoadPhaseToString(BulkLoadPhase phase) {
  switch (phase) {
    case BulkLoadPhase::kCreated:
      return "Created";
    case BulkLoadPhase::kPlanning:
      return "Planning";
    case BulkLoadPhase::kQueued:
      return "Queued";
    case BulkLoadPhase::kSSTGenerating:
      return "SSTGenerating";
    case BulkLoadPhase::kIngesting:
      return "Ingesting";
    case BulkLoadPhase::kCompleted:
      return "Completed";
    case BulkLoadPhase::kFailed:
      return "Failed";
    case BulkLoadPhase::kCancelled:
      return "Cancelled";
    default:
      return "Unknown";
  }
}

// ============================================================================
// Key 范围（用于 Bulk Load 分片）
// ============================================================================
struct KeyRange {
  std::string start_key;    // 起始 key (inclusive)
  std::string end_key;      // 结束 key (exclusive)
  uint32_t slot_start = 0;  // 起始 slot
  uint32_t slot_end = 0;    // 结束 slot
};

// ============================================================================
// SST 文件元数据（由 Worker 生成后上报）
// ============================================================================
struct SSTFileMetadata {
  std::string file_path;         // 共享存储上的 SST 文件路径
  std::string column_family;     // 目标 CF 名称 ("default" / "binlog")
  uint64_t file_size = 0;        // 文件大小 (bytes)
  uint64_t num_entries = 0;      // KV 条目数
  std::string smallest_key;      // 最小 key
  std::string largest_key;       // 最大 key
  std::string checksum;          // 文件校验和 (SHA256)
};

// ============================================================================
// Bulk Load 分片信息（单个子任务描述）
// ============================================================================
struct BulkLoadShardInfo {
  std::string shard_id;
  uint32_t shard_index = 0;      // 分片序号
  KeyRange key_range;            // 负责的 key 范围
  std::string source_path;       // 数据源路径（分片后的子路径）
  uint64_t estimated_size = 0;   // 预估数据量 (bytes)
  uint64_t estimated_rows = 0;   // 预估行数

  // 执行状态
  TaskStatus status = TaskStatus::kPending;
  std::string assigned_worker_id;
  std::vector<SSTFileMetadata> generated_sst_files;  // 生成的 SST 文件列表
  std::string error_message;
};

// ============================================================================
// Bulk Load 任务参数
// ============================================================================
struct BulkLoadTaskParams {
  // 数据源配置
  DataSourceType source_type = DataSourceType::kKVFile;
  std::string source_path;               // 数据源路径/URI
  DataFormat data_format = DataFormat::kTendisplusEncoded;

  // 分片配置
  ShardingStrategy sharding_strategy = ShardingStrategy::kAuto;
  int32_t shard_count = 0;               // 目标分片数 (0 = 自动)
  std::vector<KeyRange> key_ranges;      // 手动指定的 key 范围

  // 目标配置
  uint32_t target_store_id = 0;          // 目标 store ID
  std::string target_db_path;            // 目标 DB 路径
  std::string shared_fs_uri;             // 共享文件系统 URI
  std::string sst_output_dir;            // SST 输出目录（共享存储上）

  // 执行配置
  CompressionType compression = CompressionType::kLZ4;
  uint64_t target_sst_size = 64 * 1024 * 1024;  // 目标 SST 文件大小 (64MB)
  bool generate_binlog = false;          // 是否生成 binlog SST
  bool verify_checksum = true;           // 注入前校验
  uint32_t timeout_sec = 7200;           // 默认 2 小时

  // 资源控制
  int64_t rate_limit_bytes_per_sec = 0;  // 速率限制 (0 = 不限制)
  int32_t max_concurrent_ingests = 1;    // 最大并发注入数

  // Bulk Load 专属状态
  BulkLoadPhase phase = BulkLoadPhase::kCreated;
  std::vector<BulkLoadShardInfo> shards; // 分片列表
  uint32_t completed_shards = 0;         // 已完成分片数
  uint32_t failed_shards = 0;            // 失败分片数
  std::vector<SSTFileMetadata> all_sst_files;  // 所有生成的 SST 文件
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

  // Bulk Load 结果
  std::vector<SSTFileMetadata> sst_files;  // 生成的 SST 文件列表
  uint64_t total_rows_processed = 0;       // 处理的总行数
  uint32_t sst_files_count = 0;            // SST 文件数
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
  // Per-task mutex: 保护此 TaskInfo 的并发访问
  // (mutable 允许在 const 方法中加锁)
  mutable std::mutex mtx;

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

  // 任务参数 (Compaction)
  CompactionTaskParams params;

  // 任务参数 (Bulk Load)
  BulkLoadTaskParams bulk_load_params;

  // 执行结果
  TaskResult result;

  // 错误信息
  std::string error_message;

  // CaaS-LSM: 是否应该降级到本地执行
  bool should_fallback = false;

  // Bulk Load 父任务 ID (分片子任务用来关联父任务)
  std::string parent_task_id;

  // 默认构造函数
  TaskInfo() = default;

  // 自定义拷贝构造函数: 拷贝所有字段但跳过 mutex（新对象获得新锁）
  // 问题3 修复: 加锁 other.mtx 防止并发修改导致 torn read UB
  TaskInfo(const TaskInfo& other) {
    std::lock_guard<std::mutex> lock(other.mtx);
    task_id = other.task_id;
    type = other.type;
    status = other.status;
    priority = other.priority;
    source_node_id = other.source_node_id;
    db_name = other.db_name;
    store_id = other.store_id;
    assigned_worker_id = other.assigned_worker_id;
    retry_count = other.retry_count;
    max_retries = other.max_retries;
    reschedule_count = other.reschedule_count;
    submit_time = other.submit_time;
    assign_time = other.assign_time;
    start_time = other.start_time;
    complete_time = other.complete_time;
    params = other.params;
    bulk_load_params = other.bulk_load_params;
    result = other.result;
    error_message = other.error_message;
    should_fallback = other.should_fallback;
    parent_task_id = other.parent_task_id;
  }

  // 自定义拷贝赋值运算符
  // 问题3 修复: 同时加锁 this->mtx 和 other.mtx，使用地址顺序避免死锁
  TaskInfo& operator=(const TaskInfo& other) {
    if (this != &other) {
      // 按地址顺序加锁，防止两个 TaskInfo 互相赋值时死锁
      std::mutex* first = &mtx;
      std::mutex* second = &other.mtx;
      if (first > second) {
        std::swap(first, second);
      }
      std::lock_guard<std::mutex> lock1(*first);
      std::lock_guard<std::mutex> lock2(*second);

      task_id = other.task_id;
      type = other.type;
      status = other.status;
      priority = other.priority;
      source_node_id = other.source_node_id;
      db_name = other.db_name;
      store_id = other.store_id;
      assigned_worker_id = other.assigned_worker_id;
      retry_count = other.retry_count;
      max_retries = other.max_retries;
      reschedule_count = other.reschedule_count;
      submit_time = other.submit_time;
      assign_time = other.assign_time;
      start_time = other.start_time;
      complete_time = other.complete_time;
      params = other.params;
      bulk_load_params = other.bulk_load_params;
      result = other.result;
      error_message = other.error_message;
      should_fallback = other.should_fallback;
      parent_task_id = other.parent_task_id;
    }
    return *this;
  }

  // 辅助方法
  bool IsTerminal() const {
    return status == TaskStatus::kCompleted || status == TaskStatus::kFailed ||
           status == TaskStatus::kCancelled || status == TaskStatus::kTimeout;
  }

  bool CanRetry() const {
    return retry_count < max_retries &&
           (status == TaskStatus::kFailed || status == TaskStatus::kTimeout ||
            status == TaskStatus::kRetrying);
  }

  bool IsRetrying() const {
    return status == TaskStatus::kRetrying;
  }

  // 原子化状态转换 (CAS pattern): 仅当当前状态为 expected 时才转换为 new_status
  // 返回 true 表示转换成功，false 表示状态已被其他线程修改
  // 调用者必须先持有 mtx 锁
  bool TryTransition(TaskStatus expected, TaskStatus new_status) {
    if (status == expected) {
      status = new_status;
      return true;
    }
    return false;
  }

  // 原子化状态转换: 当前状态为 expected1 或 expected2 时转换
  bool TryTransitionFrom(TaskStatus expected1, TaskStatus expected2,
                         TaskStatus new_status) {
    if (status == expected1 || status == expected2) {
      status = new_status;
      return true;
    }
    return false;
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
  std::optional<TaskType> task_type;    // 按任务类型过滤
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
