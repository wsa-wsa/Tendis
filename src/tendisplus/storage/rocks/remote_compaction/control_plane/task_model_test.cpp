// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

// Unit tests for Task Model (task_model.h/cc)
// Tests: enum conversions, TaskInfo helpers, TaskFilter matching,
//        TaskStatistics calculations, WorkerResources computations

#include "gtest/gtest.h"
#include "task_model.h"

#include <chrono>
#include <string>
#include <thread>

namespace tendisplus {
namespace control_plane {

// ============================================================================
// TaskPriority 枚举转换测试
// ============================================================================
TEST(TaskModel, TaskPriorityToString) {
  EXPECT_STREQ(TaskPriorityToString(TaskPriority::kLow), "Low");
  EXPECT_STREQ(TaskPriorityToString(TaskPriority::kNormal), "Normal");
  EXPECT_STREQ(TaskPriorityToString(TaskPriority::kHigh), "High");
  EXPECT_STREQ(TaskPriorityToString(TaskPriority::kUrgent), "Urgent");
  // 非法值返回 Unknown
  EXPECT_STREQ(TaskPriorityToString(static_cast<TaskPriority>(99)), "Unknown");
}

// ============================================================================
// TaskStatus 枚举转换测试
// ============================================================================
TEST(TaskModel, TaskStatusToString) {
  EXPECT_STREQ(TaskStatusToString(TaskStatus::kUnknown), "Unknown");
  EXPECT_STREQ(TaskStatusToString(TaskStatus::kPending), "Pending");
  EXPECT_STREQ(TaskStatusToString(TaskStatus::kAssigned), "Assigned");
  EXPECT_STREQ(TaskStatusToString(TaskStatus::kRunning), "Running");
  EXPECT_STREQ(TaskStatusToString(TaskStatus::kCompleted), "Completed");
  EXPECT_STREQ(TaskStatusToString(TaskStatus::kFailed), "Failed");
  EXPECT_STREQ(TaskStatusToString(TaskStatus::kCancelled), "Cancelled");
  EXPECT_STREQ(TaskStatusToString(TaskStatus::kTimeout), "Timeout");
  EXPECT_STREQ(TaskStatusToString(TaskStatus::kRetrying), "Retrying");
}

// ============================================================================
// TaskType 枚举转换测试
// ============================================================================
TEST(TaskModel, TaskTypeToString) {
  EXPECT_STREQ(TaskTypeToString(TaskType::kCompaction), "Compaction");
  EXPECT_STREQ(TaskTypeToString(TaskType::kBulkLoad), "BulkLoad");
  EXPECT_STREQ(TaskTypeToString(static_cast<TaskType>(99)), "Unknown");
}

// ============================================================================
// BulkLoadPhase 枚举转换测试
// ============================================================================
TEST(TaskModel, BulkLoadPhaseToString) {
  EXPECT_STREQ(BulkLoadPhaseToString(BulkLoadPhase::kCreated), "Created");
  EXPECT_STREQ(BulkLoadPhaseToString(BulkLoadPhase::kPlanning), "Planning");
  EXPECT_STREQ(BulkLoadPhaseToString(BulkLoadPhase::kQueued), "Queued");
  EXPECT_STREQ(
    BulkLoadPhaseToString(BulkLoadPhase::kSSTGenerating), "SSTGenerating");
  EXPECT_STREQ(BulkLoadPhaseToString(BulkLoadPhase::kIngesting), "Ingesting");
  EXPECT_STREQ(BulkLoadPhaseToString(BulkLoadPhase::kCompleted), "Completed");
  EXPECT_STREQ(BulkLoadPhaseToString(BulkLoadPhase::kFailed), "Failed");
  EXPECT_STREQ(BulkLoadPhaseToString(BulkLoadPhase::kCancelled), "Cancelled");
  EXPECT_STREQ(
    BulkLoadPhaseToString(static_cast<BulkLoadPhase>(99)), "Unknown");
}

// ============================================================================
// TaskInfo::IsTerminal 测试
// ============================================================================
TEST(TaskInfo, IsTerminal) {
  TaskInfo task;

  // 终态测试
  task.status = TaskStatus::kCompleted;
  EXPECT_TRUE(task.IsTerminal());

  task.status = TaskStatus::kFailed;
  EXPECT_TRUE(task.IsTerminal());

  task.status = TaskStatus::kCancelled;
  EXPECT_TRUE(task.IsTerminal());

  task.status = TaskStatus::kTimeout;
  EXPECT_TRUE(task.IsTerminal());

  // 非终态测试
  task.status = TaskStatus::kPending;
  EXPECT_FALSE(task.IsTerminal());

  task.status = TaskStatus::kAssigned;
  EXPECT_FALSE(task.IsTerminal());

  task.status = TaskStatus::kRunning;
  EXPECT_FALSE(task.IsTerminal());

  task.status = TaskStatus::kRetrying;
  EXPECT_FALSE(task.IsTerminal());

  task.status = TaskStatus::kUnknown;
  EXPECT_FALSE(task.IsTerminal());
}

// ============================================================================
// TaskInfo::CanRetry 测试
// ============================================================================
TEST(TaskInfo, CanRetry) {
  TaskInfo task;
  task.max_retries = 3;

  // 可重试的状态 + 重试次数未用尽
  task.retry_count = 0;
  task.status = TaskStatus::kFailed;
  EXPECT_TRUE(task.CanRetry());

  task.status = TaskStatus::kTimeout;
  EXPECT_TRUE(task.CanRetry());

  task.status = TaskStatus::kRetrying;
  EXPECT_TRUE(task.CanRetry());

  // 重试次数用尽
  task.retry_count = 3;
  task.status = TaskStatus::kFailed;
  EXPECT_FALSE(task.CanRetry());

  // 不可重试的状态
  task.retry_count = 0;
  task.status = TaskStatus::kCompleted;
  EXPECT_FALSE(task.CanRetry());

  task.status = TaskStatus::kCancelled;
  EXPECT_FALSE(task.CanRetry());

  task.status = TaskStatus::kPending;
  EXPECT_FALSE(task.CanRetry());

  task.status = TaskStatus::kRunning;
  EXPECT_FALSE(task.CanRetry());
}

// ============================================================================
// TaskInfo::IsRetrying 测试
// ============================================================================
TEST(TaskInfo, IsRetrying) {
  TaskInfo task;
  task.status = TaskStatus::kRetrying;
  EXPECT_TRUE(task.IsRetrying());

  task.status = TaskStatus::kFailed;
  EXPECT_FALSE(task.IsRetrying());

  task.status = TaskStatus::kRunning;
  EXPECT_FALSE(task.IsRetrying());
}

// ============================================================================
// TaskInfo::GetQueueTimeMs 测试
// ============================================================================
TEST(TaskInfo, GetQueueTimeMs) {
  TaskInfo task;

  // assign_time > submit_time → 正常返回排队时间
  task.submit_time = std::chrono::system_clock::now();
  task.assign_time = task.submit_time + std::chrono::milliseconds(150);
  EXPECT_GE(task.GetQueueTimeMs(), 149);
  EXPECT_LE(task.GetQueueTimeMs(), 151);

  // assign_time == submit_time → 0
  task.assign_time = task.submit_time;
  EXPECT_EQ(task.GetQueueTimeMs(), 0);

  // assign_time 未设置（默认epoch） → 0
  TaskInfo task2;
  task2.submit_time = std::chrono::system_clock::now();
  EXPECT_EQ(task2.GetQueueTimeMs(), 0);
}

// ============================================================================
// TaskInfo::GetExecutionTimeMs 测试
// ============================================================================
TEST(TaskInfo, GetExecutionTimeMs) {
  TaskInfo task;

  // complete_time > start_time → 正常返回执行时间
  task.start_time = std::chrono::system_clock::now();
  task.complete_time = task.start_time + std::chrono::milliseconds(500);
  EXPECT_GE(task.GetExecutionTimeMs(), 499);
  EXPECT_LE(task.GetExecutionTimeMs(), 501);

  // complete_time == start_time → 0
  task.complete_time = task.start_time;
  EXPECT_EQ(task.GetExecutionTimeMs(), 0);

  // complete_time 未设置 → 0
  TaskInfo task2;
  task2.start_time = std::chrono::system_clock::now();
  EXPECT_EQ(task2.GetExecutionTimeMs(), 0);
}

// ============================================================================
// TaskInfo::ToString 测试
// ============================================================================
TEST(TaskInfo, ToString) {
  TaskInfo task;
  task.task_id = "test-001";
  task.status = TaskStatus::kRunning;
  task.priority = TaskPriority::kHigh;
  task.source_node_id = "node-1";
  task.db_name = "db0";
  task.store_id = 3;
  task.retry_count = 1;
  task.max_retries = 3;
  task.assigned_worker_id = "worker-1";

  std::string str = task.ToString();
  EXPECT_NE(str.find("test-001"), std::string::npos);
  EXPECT_NE(str.find("Running"), std::string::npos);
  EXPECT_NE(str.find("High"), std::string::npos);
  EXPECT_NE(str.find("node-1"), std::string::npos);
  EXPECT_NE(str.find("worker-1"), std::string::npos);
  EXPECT_NE(str.find("1/3"), std::string::npos);
}

TEST(TaskInfo, ToStringWithoutWorker) {
  TaskInfo task;
  task.task_id = "test-002";
  task.status = TaskStatus::kPending;

  std::string str = task.ToString();
  EXPECT_NE(str.find("test-002"), std::string::npos);
  EXPECT_NE(str.find("Pending"), std::string::npos);
  // 没有 assigned_worker_id 时不应该包含 "worker="
  EXPECT_EQ(str.find("worker="), std::string::npos);
}

// ============================================================================
// TaskFilter::Matches 测试
// ============================================================================
TEST(TaskFilter, MatchesByStatus) {
  TaskInfo task;
  task.status = TaskStatus::kRunning;

  TaskFilter filter;
  filter.status = TaskStatus::kRunning;
  EXPECT_TRUE(filter.Matches(task));

  filter.status = TaskStatus::kPending;
  EXPECT_FALSE(filter.Matches(task));
}

TEST(TaskFilter, MatchesByTaskType) {
  TaskInfo task;
  task.type = TaskType::kCompaction;

  TaskFilter filter;
  filter.task_type = TaskType::kCompaction;
  EXPECT_TRUE(filter.Matches(task));

  filter.task_type = TaskType::kBulkLoad;
  EXPECT_FALSE(filter.Matches(task));
}

TEST(TaskFilter, MatchesByPriority) {
  TaskInfo task;
  task.priority = TaskPriority::kHigh;

  TaskFilter filter;
  filter.priority = TaskPriority::kHigh;
  EXPECT_TRUE(filter.Matches(task));

  filter.priority = TaskPriority::kLow;
  EXPECT_FALSE(filter.Matches(task));
}

TEST(TaskFilter, MatchesBySourceNodeId) {
  TaskInfo task;
  task.source_node_id = "node-42";

  TaskFilter filter;
  filter.source_node_id = "node-42";
  EXPECT_TRUE(filter.Matches(task));

  filter.source_node_id = "node-99";
  EXPECT_FALSE(filter.Matches(task));
}

TEST(TaskFilter, MatchesByAssignedWorkerId) {
  TaskInfo task;
  task.assigned_worker_id = "worker-7";

  TaskFilter filter;
  filter.assigned_worker_id = "worker-7";
  EXPECT_TRUE(filter.Matches(task));

  filter.assigned_worker_id = "worker-1";
  EXPECT_FALSE(filter.Matches(task));
}

TEST(TaskFilter, MatchesByDbName) {
  TaskInfo task;
  task.db_name = "mydb";

  TaskFilter filter;
  filter.db_name = "mydb";
  EXPECT_TRUE(filter.Matches(task));

  filter.db_name = "otherdb";
  EXPECT_FALSE(filter.Matches(task));
}

TEST(TaskFilter, MatchesBySinceTime) {
  TaskInfo task;
  task.submit_time = std::chrono::system_clock::now();

  auto submit_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                     task.submit_time.time_since_epoch())
                     .count();

  TaskFilter filter;
  // 时间在提交之前 → 匹配
  filter.since_time_ms = submit_ms - 1000;
  EXPECT_TRUE(filter.Matches(task));

  // 时间在提交之后 → 不匹配
  filter.since_time_ms = submit_ms + 1000;
  EXPECT_FALSE(filter.Matches(task));
}

TEST(TaskFilter, MatchesMultipleConditions) {
  TaskInfo task;
  task.status = TaskStatus::kRunning;
  task.type = TaskType::kCompaction;
  task.priority = TaskPriority::kHigh;
  task.source_node_id = "node-1";

  // 所有条件都匹配
  TaskFilter filter;
  filter.status = TaskStatus::kRunning;
  filter.task_type = TaskType::kCompaction;
  filter.priority = TaskPriority::kHigh;
  filter.source_node_id = "node-1";
  EXPECT_TRUE(filter.Matches(task));

  // 一个条件不匹配 → 整体不匹配
  filter.priority = TaskPriority::kLow;
  EXPECT_FALSE(filter.Matches(task));
}

TEST(TaskFilter, EmptyFilterMatchesAll) {
  TaskInfo task;
  task.status = TaskStatus::kRunning;
  task.type = TaskType::kBulkLoad;
  task.priority = TaskPriority::kUrgent;
  task.source_node_id = "any-node";

  TaskFilter filter;  // 无任何过滤条件
  EXPECT_TRUE(filter.Matches(task));
}

// ============================================================================
// TaskStatistics 统计计算测试
// ============================================================================
TEST(TaskStatistics, GetAvgExecutionTimeMs) {
  TaskStatistics stats;

  // 0 个完成 → 0
  EXPECT_DOUBLE_EQ(stats.GetAvgExecutionTimeMs(), 0.0);

  // 5 个完成，总时间 1000ms → 平均 200ms
  stats.total_completed.store(5);
  stats.total_execution_time_ms.store(1000);
  EXPECT_DOUBLE_EQ(stats.GetAvgExecutionTimeMs(), 200.0);

  // 1 个完成，总时间 50ms → 平均 50ms
  stats.total_completed.store(1);
  stats.total_execution_time_ms.store(50);
  EXPECT_DOUBLE_EQ(stats.GetAvgExecutionTimeMs(), 50.0);
}

TEST(TaskStatistics, GetAvgQueueTimeMs) {
  TaskStatistics stats;

  // 0 个完成 → 0
  EXPECT_DOUBLE_EQ(stats.GetAvgQueueTimeMs(), 0.0);

  // 10 个完成，总排队时间 5000ms → 平均 500ms
  stats.total_completed.store(10);
  stats.total_queue_time_ms.store(5000);
  EXPECT_DOUBLE_EQ(stats.GetAvgQueueTimeMs(), 500.0);
}

TEST(TaskStatistics, ToString) {
  TaskStatistics stats;
  stats.total_submitted.store(100);
  stats.total_completed.store(90);
  stats.total_failed.store(5);
  stats.total_cancelled.store(3);
  stats.total_timeout.store(2);
  stats.pending_count.store(10);
  stats.running_count.store(5);
  stats.total_execution_time_ms.store(9000);
  stats.total_queue_time_ms.store(4500);

  std::string str = stats.ToString();
  EXPECT_NE(str.find("submitted=100"), std::string::npos);
  EXPECT_NE(str.find("completed=90"), std::string::npos);
  EXPECT_NE(str.find("failed=5"), std::string::npos);
  EXPECT_NE(str.find("pending=10"), std::string::npos);
  EXPECT_NE(str.find("running=5"), std::string::npos);
}

// ============================================================================
// 数据结构默认值测试
// ============================================================================
TEST(TaskModel, DefaultValues) {
  TaskInfo task;
  EXPECT_EQ(task.type, TaskType::kCompaction);
  EXPECT_EQ(task.status, TaskStatus::kPending);
  EXPECT_EQ(task.priority, TaskPriority::kNormal);
  EXPECT_EQ(task.retry_count, 0);
  EXPECT_EQ(task.max_retries, 3);
  EXPECT_EQ(task.reschedule_count, 0);
  EXPECT_EQ(task.store_id, 0u);
  EXPECT_FALSE(task.should_fallback);
  EXPECT_TRUE(task.task_id.empty());
  EXPECT_TRUE(task.assigned_worker_id.empty());
}

TEST(TaskModel, TaskResultDefaults) {
  TaskResult result;
  EXPECT_FALSE(result.success);
  EXPECT_TRUE(result.compaction_result.empty());
  EXPECT_TRUE(result.error_message.empty());
  EXPECT_EQ(result.execution_time_ms, 0u);
  EXPECT_EQ(result.bytes_read, 0u);
  EXPECT_EQ(result.bytes_written, 0u);
  EXPECT_TRUE(result.sst_files.empty());
  EXPECT_EQ(result.total_rows_processed, 0u);
  EXPECT_EQ(result.sst_files_count, 0u);
}

TEST(TaskModel, BulkLoadTaskParamsDefaults) {
  BulkLoadTaskParams params;
  EXPECT_EQ(params.source_type, DataSourceType::kKVFile);
  EXPECT_EQ(params.data_format, DataFormat::kTendisplusEncoded);
  EXPECT_EQ(params.sharding_strategy, ShardingStrategy::kAuto);
  EXPECT_EQ(params.shard_count, 0);
  EXPECT_EQ(params.target_store_id, 0u);
  EXPECT_EQ(params.compression, CompressionType::kLZ4);
  EXPECT_EQ(params.target_sst_size, 64u * 1024 * 1024);
  EXPECT_FALSE(params.generate_binlog);
  EXPECT_TRUE(params.verify_checksum);
  EXPECT_EQ(params.timeout_sec, 7200u);
  EXPECT_EQ(params.rate_limit_bytes_per_sec, 0);
  EXPECT_EQ(params.max_concurrent_ingests, 1);
  EXPECT_EQ(params.phase, BulkLoadPhase::kCreated);
  EXPECT_TRUE(params.shards.empty());
  EXPECT_EQ(params.completed_shards, 0u);
  EXPECT_EQ(params.failed_shards, 0u);
}

TEST(TaskModel, CompactionTaskParamsDefaults) {
  CompactionTaskParams params;
  EXPECT_EQ(params.job_id, 0u);
  EXPECT_TRUE(params.compaction_input.empty());
  EXPECT_EQ(params.timeout_sec, 3600u);
  EXPECT_EQ(params.start_level, 0);
  EXPECT_DOUBLE_EQ(params.score, 0.0);
}

TEST(TaskModel, SSTFileMetadataDefaults) {
  SSTFileMetadata meta;
  EXPECT_TRUE(meta.file_path.empty());
  EXPECT_TRUE(meta.column_family.empty());
  EXPECT_EQ(meta.file_size, 0u);
  EXPECT_EQ(meta.num_entries, 0u);
  EXPECT_TRUE(meta.smallest_key.empty());
  EXPECT_TRUE(meta.largest_key.empty());
  EXPECT_TRUE(meta.checksum.empty());
}

TEST(TaskModel, KeyRangeDefaults) {
  KeyRange range;
  EXPECT_TRUE(range.start_key.empty());
  EXPECT_TRUE(range.end_key.empty());
  EXPECT_EQ(range.slot_start, 0u);
  EXPECT_EQ(range.slot_end, 0u);
}

TEST(TaskModel, BulkLoadShardInfoDefaults) {
  BulkLoadShardInfo shard;
  EXPECT_TRUE(shard.shard_id.empty());
  EXPECT_EQ(shard.shard_index, 0u);
  EXPECT_EQ(shard.estimated_size, 0u);
  EXPECT_EQ(shard.estimated_rows, 0u);
  EXPECT_EQ(shard.status, TaskStatus::kPending);
  EXPECT_TRUE(shard.assigned_worker_id.empty());
  EXPECT_TRUE(shard.generated_sst_files.empty());
  EXPECT_TRUE(shard.error_message.empty());
}

}  // namespace control_plane
}  // namespace tendisplus
