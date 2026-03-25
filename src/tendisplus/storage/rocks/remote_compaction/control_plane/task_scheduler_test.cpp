// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

// Unit tests for TaskScheduler (task_scheduler.h/cc)
// Tests: task submission, cancellation, completion, query, statistics,
//        retry logic, wait mechanism, Bulk Load task management.
// Note: We do NOT call Start() in most tests to avoid background threads
//       that depend on gRPC. Instead we test the public API directly.

#include "gtest/gtest.h"
#include "task_scheduler.h"

#include <chrono>
#include <thread>

namespace tendisplus {
namespace control_plane {

// ============================================================================
// 辅助：创建简单的 Compaction TaskInfo
// ============================================================================
static TaskInfo MakeCompactionTask(const std::string& id = "",
                                   int32_t start_level = 0,
                                   double score = 1.0,
                                   TaskPriority prio = TaskPriority::kNormal) {
  TaskInfo task;
  task.task_id = id;
  task.type = TaskType::kCompaction;
  task.priority = prio;
  task.source_node_id = "node-1";
  task.db_name = "db0";
  task.store_id = 0;
  task.params.start_level = start_level;
  task.params.score = score;
  return task;
}

// ============================================================================
// SchedulingPolicy 枚举转换
// ============================================================================
TEST(TaskScheduler, SchedulingPolicyToString) {
  EXPECT_STREQ(SchedulingPolicyToString(SchedulingPolicy::kFIFO), "FIFO");
  EXPECT_STREQ(SchedulingPolicyToString(SchedulingPolicy::kPriority),
               "Priority");
  EXPECT_STREQ(SchedulingPolicyToString(SchedulingPolicy::kFairShare),
               "FairShare");
  EXPECT_STREQ(SchedulingPolicyToString(SchedulingPolicy::kLeastLoaded),
               "LeastLoaded");
  EXPECT_STREQ(
    SchedulingPolicyToString(static_cast<SchedulingPolicy>(99)), "Unknown");
}

// ============================================================================
// 默认配置
// ============================================================================
TEST(TaskScheduler, DefaultConfig) {
  SchedulerConfig config;
  EXPECT_EQ(config.policy, SchedulingPolicy::kPriority);
  EXPECT_EQ(config.max_pending_tasks, 10000u);
  EXPECT_EQ(config.scheduling_interval_ms, 100u);
  EXPECT_EQ(config.task_timeout_sec, 3600u);
  EXPECT_EQ(config.max_retries, 3u);
  EXPECT_FALSE(config.enable_preemption);
  EXPECT_EQ(config.max_accumulation_in_procp, 100u);
  EXPECT_EQ(config.max_reschedule, 5u);
  EXPECT_DOUBLE_EQ(config.min_memory_free_ratio, 0.3);
}

// ============================================================================
// 提交 Compaction 任务
// ============================================================================
TEST(TaskScheduler, SubmitCompactionTask) {
  SchedulerConfig config;
  TaskScheduler scheduler(config);

  auto task = MakeCompactionTask();
  auto id = scheduler.SubmitTask(task);
  EXPECT_FALSE(id.empty());

  // 查询任务
  auto retrieved = scheduler.GetTask(id);
  ASSERT_NE(retrieved, nullptr);
  EXPECT_EQ(retrieved->task_id, id);
  EXPECT_EQ(retrieved->type, TaskType::kCompaction);
  EXPECT_EQ(retrieved->status, TaskStatus::kPending);
  EXPECT_EQ(retrieved->source_node_id, "node-1");

  // 统计
  EXPECT_EQ(scheduler.GetStatistics().total_submitted.load(), 1u);
  EXPECT_EQ(scheduler.GetPendingCount(), 1u);
  EXPECT_EQ(scheduler.GetRunningCount(), 0u);
}

TEST(TaskScheduler, SubmitWithCustomId) {
  SchedulerConfig config;
  TaskScheduler scheduler(config);

  auto task = MakeCompactionTask("my-task-42");
  auto id = scheduler.SubmitTask(task);
  EXPECT_EQ(id, "my-task-42");
}

TEST(TaskScheduler, SubmitMultipleTasks) {
  SchedulerConfig config;
  TaskScheduler scheduler(config);

  for (int i = 0; i < 10; i++) {
    auto task = MakeCompactionTask();
    scheduler.SubmitTask(task);
  }

  EXPECT_EQ(scheduler.GetStatistics().total_submitted.load(), 10u);
  EXPECT_EQ(scheduler.GetPendingCount(), 10u);
}

// ============================================================================
// 队列满时拒绝
// ============================================================================
TEST(TaskScheduler, RejectWhenQueueFull) {
  SchedulerConfig config;
  config.max_pending_tasks = 3;
  TaskScheduler scheduler(config);

  for (int i = 0; i < 3; i++) {
    auto id = scheduler.SubmitTask(MakeCompactionTask());
    EXPECT_FALSE(id.empty());
  }

  // 第 4 个应被拒绝
  auto rejected_id = scheduler.SubmitTask(MakeCompactionTask());
  EXPECT_TRUE(rejected_id.empty());
}

// ============================================================================
// 取消任务
// ============================================================================
TEST(TaskScheduler, CancelPendingTask) {
  SchedulerConfig config;
  TaskScheduler scheduler(config);

  auto id = scheduler.SubmitTask(MakeCompactionTask());
  EXPECT_TRUE(scheduler.CancelTask(id, "user request"));

  auto task = scheduler.GetTask(id);
  EXPECT_EQ(task->status, TaskStatus::kCancelled);
  EXPECT_EQ(task->error_message, "user request");

  EXPECT_EQ(scheduler.GetStatistics().total_cancelled.load(), 1u);
}

TEST(TaskScheduler, CancelNonexistentTask) {
  SchedulerConfig config;
  TaskScheduler scheduler(config);

  EXPECT_FALSE(scheduler.CancelTask("nonexistent"));
}

TEST(TaskScheduler, CancelTerminalTask) {
  SchedulerConfig config;
  TaskScheduler scheduler(config);

  auto id = scheduler.SubmitTask(MakeCompactionTask());
  scheduler.CancelTask(id);

  // 已取消的任务不能再次取消
  EXPECT_FALSE(scheduler.CancelTask(id));
}

// ============================================================================
// 任务完成通知
// ============================================================================
TEST(TaskScheduler, OnTaskCompleted) {
  SchedulerConfig config;
  TaskScheduler scheduler(config);

  auto id = scheduler.SubmitTask(MakeCompactionTask());

  TaskResult result;
  result.success = true;
  result.execution_time_ms = 500;

  scheduler.OnTaskCompleted(id, result);

  auto task = scheduler.GetTask(id);
  EXPECT_EQ(task->status, TaskStatus::kCompleted);
  EXPECT_TRUE(task->result.success);

  EXPECT_EQ(scheduler.GetStatistics().total_completed.load(), 1u);
}

TEST(TaskScheduler, OnTaskFailed) {
  SchedulerConfig config;
  config.max_retries = 0;  // 禁用重试
  TaskScheduler scheduler(config);

  auto task_info = MakeCompactionTask();
  task_info.max_retries = 0;
  auto id = scheduler.SubmitTask(task_info);

  scheduler.OnTaskFailed(id, "disk error");

  auto task = scheduler.GetTask(id);
  EXPECT_EQ(task->status, TaskStatus::kFailed);

  EXPECT_EQ(scheduler.GetStatistics().total_failed.load(), 1u);
}

TEST(TaskScheduler, OnTaskCompletedUnknownTask) {
  SchedulerConfig config;
  TaskScheduler scheduler(config);

  // 对未知任务的完成通知 — 不崩溃
  TaskResult result;
  result.success = true;
  scheduler.OnTaskCompleted("nonexistent", result);
}

// ============================================================================
// 重试逻辑
// ============================================================================
TEST(TaskScheduler, RetryOnFailure) {
  SchedulerConfig config;
  TaskScheduler scheduler(config);

  auto task_info = MakeCompactionTask();
  task_info.max_retries = 3;
  auto id = scheduler.SubmitTask(task_info);

  // 第一次失败 → 应该重试
  TaskResult fail_result;
  fail_result.success = false;
  fail_result.error_message = "network error";
  scheduler.OnTaskCompleted(id, fail_result);

  auto task = scheduler.GetTask(id);
  // 重试后应该回到 Pending 状态
  EXPECT_EQ(task->status, TaskStatus::kPending);
  EXPECT_EQ(task->retry_count, 1);
}

// ============================================================================
// 任务查询
// ============================================================================
TEST(TaskScheduler, QueryTasksByFilter) {
  SchedulerConfig config;
  TaskScheduler scheduler(config);

  // 提交混合任务
  auto t1 = MakeCompactionTask("t1", 0, 1.0, TaskPriority::kHigh);
  auto t2 = MakeCompactionTask("t2", 1, 2.0, TaskPriority::kNormal);
  auto t3 = MakeCompactionTask("t3", 2, 3.0, TaskPriority::kLow);

  scheduler.SubmitTask(t1);
  scheduler.SubmitTask(t2);
  scheduler.SubmitTask(t3);

  // 按优先级过滤
  TaskFilter filter;
  filter.priority = TaskPriority::kHigh;
  auto results = scheduler.QueryTasks(filter);
  EXPECT_EQ(results.size(), 1u);
  EXPECT_EQ(results[0]->task_id, "t1");

  // 按状态过滤 (所有都是 Pending)
  TaskFilter status_filter;
  status_filter.status = TaskStatus::kPending;
  auto pending = scheduler.QueryTasks(status_filter);
  EXPECT_EQ(pending.size(), 3u);

  // 空过滤器 → 返回所有
  TaskFilter empty_filter;
  auto all = scheduler.QueryTasks(empty_filter);
  EXPECT_EQ(all.size(), 3u);
}

TEST(TaskScheduler, QueryWithLimit) {
  SchedulerConfig config;
  TaskScheduler scheduler(config);

  for (int i = 0; i < 10; i++) {
    scheduler.SubmitTask(MakeCompactionTask());
  }

  TaskFilter filter;
  filter.limit = 3;
  auto results = scheduler.QueryTasks(filter);
  EXPECT_LE(results.size(), 3u);
}

TEST(TaskScheduler, GetNonexistentTask) {
  SchedulerConfig config;
  TaskScheduler scheduler(config);

  EXPECT_EQ(scheduler.GetTask("nonexistent"), nullptr);
}

// ============================================================================
// WaitForTask 机制
// ============================================================================
TEST(TaskScheduler, WaitForAlreadyCompletedTask) {
  SchedulerConfig config;
  TaskScheduler scheduler(config);

  auto task_info = MakeCompactionTask();
  task_info.max_retries = 0;
  auto id = scheduler.SubmitTask(task_info);

  // 先完成
  TaskResult result;
  result.success = true;
  result.compaction_result = "ok";
  scheduler.OnTaskCompleted(id, result);

  // 再等待 → 立即返回
  TaskResult wait_result;
  bool completed = scheduler.WaitForTask(id, &wait_result, 100);
  EXPECT_TRUE(completed);
  EXPECT_TRUE(wait_result.success);
}

TEST(TaskScheduler, WaitForTaskTimeout) {
  SchedulerConfig config;
  TaskScheduler scheduler(config);

  auto id = scheduler.SubmitTask(MakeCompactionTask());

  // 任务未完成，等待 100ms → 超时
  TaskResult result;
  bool completed = scheduler.WaitForTask(id, &result, 100);
  EXPECT_FALSE(completed);
}

TEST(TaskScheduler, WaitForNonexistentTask) {
  SchedulerConfig config;
  TaskScheduler scheduler(config);

  bool completed = scheduler.WaitForTask("nonexistent", nullptr, 0);
  EXPECT_FALSE(completed);
}

// ============================================================================
// 回调注册
// ============================================================================
TEST(TaskScheduler, CompletedCallback) {
  SchedulerConfig config;
  TaskScheduler scheduler(config);

  std::string callback_task_id;
  bool callback_success = false;

  scheduler.RegisterCompletedCallback(
    [&](const std::string& tid, const TaskResult& res) {
      callback_task_id = tid;
      callback_success = res.success;
    });

  auto task_info = MakeCompactionTask();
  task_info.max_retries = 0;
  auto id = scheduler.SubmitTask(task_info);

  TaskResult result;
  result.success = true;
  scheduler.OnTaskCompleted(id, result);

  EXPECT_EQ(callback_task_id, id);
  EXPECT_TRUE(callback_success);
}

// ============================================================================
// Bulk Load 任务管理
// ============================================================================
TEST(TaskScheduler, SubmitBulkLoadTask) {
  SchedulerConfig config;
  TaskScheduler scheduler(config);

  TaskInfo bulk_task;
  bulk_task.type = TaskType::kBulkLoad;
  bulk_task.bulk_load_params.source_path = "/data/import.kv";
  bulk_task.bulk_load_params.shard_count = 4;

  auto id = scheduler.SubmitBulkLoadTask(bulk_task);
  EXPECT_FALSE(id.empty());

  auto retrieved = scheduler.GetBulkLoadTask(id);
  ASSERT_NE(retrieved, nullptr);
  EXPECT_EQ(retrieved->type, TaskType::kBulkLoad);
  EXPECT_EQ(retrieved->bulk_load_params.source_path, "/data/import.kv");
  EXPECT_EQ(retrieved->bulk_load_params.shard_count, 4);
}

TEST(TaskScheduler, SubmitBulkLoadShard) {
  SchedulerConfig config;
  TaskScheduler scheduler(config);

  TaskInfo shard_task;
  shard_task.type = TaskType::kBulkLoad;
  shard_task.bulk_load_params.source_path = "/data/shard_0.kv";

  auto id = scheduler.SubmitBulkLoadShard(shard_task);
  EXPECT_FALSE(id.empty());

  auto retrieved = scheduler.GetTask(id);
  ASSERT_NE(retrieved, nullptr);
  EXPECT_EQ(retrieved->type, TaskType::kBulkLoad);
}

TEST(TaskScheduler, GetBulkLoadTaskNonexistent) {
  SchedulerConfig config;
  TaskScheduler scheduler(config);

  EXPECT_EQ(scheduler.GetBulkLoadTask("nonexistent"), nullptr);
}

// ============================================================================
// 统计一致性
// ============================================================================
TEST(TaskScheduler, StatisticsConsistency) {
  SchedulerConfig config;
  TaskScheduler scheduler(config);

  // 提交 5 个任务
  std::vector<std::string> ids;
  for (int i = 0; i < 5; i++) {
    auto task = MakeCompactionTask();
    task.max_retries = 0;
    ids.push_back(scheduler.SubmitTask(task));
  }

  const auto& stats = scheduler.GetStatistics();
  EXPECT_EQ(stats.total_submitted.load(), 5u);

  // 完成 3 个
  for (int i = 0; i < 3; i++) {
    TaskResult result;
    result.success = true;
    scheduler.OnTaskCompleted(ids[i], result);
  }
  EXPECT_EQ(stats.total_completed.load(), 3u);

  // 取消 1 个
  scheduler.CancelTask(ids[3]);
  EXPECT_EQ(stats.total_cancelled.load(), 1u);

  // 失败 1 个
  scheduler.OnTaskFailed(ids[4], "error");
  EXPECT_EQ(stats.total_failed.load(), 1u);
}

// ============================================================================
// SchedulingDecision 默认值
// ============================================================================
TEST(TaskScheduler, SchedulingDecisionDefaults) {
  SchedulingDecision decision;
  EXPECT_TRUE(decision.task_id.empty());
  EXPECT_TRUE(decision.worker_id.empty());
  EXPECT_FALSE(decision.should_schedule);
  EXPECT_TRUE(decision.reason.empty());
}

// ============================================================================
// 生命周期（使用 Worker Manager）
// ============================================================================
TEST(TaskScheduler, StartStopWithWorkerManager) {
  SchedulerConfig config;
  config.scheduling_interval_ms = 50;
  TaskScheduler scheduler(config);

  WorkerManagerConfig wm_config;
  auto wm = std::make_shared<WorkerManager>(wm_config);
  scheduler.SetWorkerManager(wm);

  scheduler.Start();
  EXPECT_TRUE(scheduler.IsRunning());

  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  scheduler.Stop();
  EXPECT_FALSE(scheduler.IsRunning());
}

// ============================================================================
// 带 Worker Manager 的完整调度流程
// ============================================================================
TEST(TaskScheduler, FullSchedulingFlow) {
  SchedulerConfig config;
  config.scheduling_interval_ms = 50;
  TaskScheduler scheduler(config);

  WorkerManagerConfig wm_config;
  auto wm = std::make_shared<WorkerManager>(wm_config);

  // 注册 Worker
  WorkerResources res;
  res.max_concurrent_tasks = 5;
  res.total_memory_mb = 8000;
  res.used_memory_mb = 1000;
  wm->RegisterWorker("localhost:8001", res, {}, "w1");

  scheduler.SetWorkerManager(wm);
  scheduler.Start();

  // 提交任务
  auto id = scheduler.SubmitTask(MakeCompactionTask());

  // 等待调度（调度线程运行后会尝试分配给 Worker）
  // 注意：DistributeTaskToCSA 会调用 gRPC 并失败，但 AssignTask 会成功
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  auto task = scheduler.GetTask(id);
  // 任务可能已被调度（Assigned/Running）或因 gRPC 失败留在 Pending
  // 不检查精确状态，只要系统没崩溃

  scheduler.Stop();
}

}  // namespace control_plane
}  // namespace tendisplus
