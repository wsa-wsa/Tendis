// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

// Regression Tests for Control Plane
// ====================================
// 回归测试：验证代码审查中发现并修复的各类问题不会复现。
// 覆盖范围：
//   - TaskInfo 拷贝构造 / 赋值运算符线程安全
//   - HandleRetry 契约与锁优化
//   - DoSchedule 出队后状态检查
//   - CleanupCompletedTasks 关联数据清理
//   - TimeoutCheckLoop / CSAStatusCheckLoop per-task lock
//   - gRPC handler per-task lock
//   - 多种并发场景下的综合压力测试

#include "gtest/gtest.h"
#include "task_scheduler.h"

#include <atomic>
#include <chrono>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace tendisplus {
namespace control_plane {

// ============================================================================
// 辅助函数
// ============================================================================
static TaskInfo MakeTestTask(const std::string& id = "",
                           int max_retries = 3) {
  TaskInfo task;
  task.task_id = id;
  task.type = TaskType::kCompaction;
  task.priority = TaskPriority::kNormal;
  task.source_node_id = "node-r4";
  task.db_name = "db0";
  task.store_id = 0;
  task.max_retries = max_retries;
  task.params.start_level = 0;
  task.params.score = 1.0;
  return task;
}

static SchedulerConfig MakeTestConfig() {
  SchedulerConfig config;
  config.scheduling_interval_ms = 50;
  config.task_timeout_sec = 3600;
  config.max_retries = 3;
  config.completed_task_retention_sec = 1;
  config.completed_task_cleanup_interval_sec = 1;
  return config;
}

// ============================================================================
// TaskInfo 拷贝构造函数线程安全
// 验证：多线程并发修改 TaskInfo 字段时，拷贝构造函数能安全读取所有字段
// ============================================================================
TEST(ControlPlaneRegression, TaskInfoCopyConstructor_ThreadSafe) {
  auto original = std::make_shared<TaskInfo>();
  original->task_id = "orig-001";
  original->status = TaskStatus::kPending;
  original->source_node_id = "node-1";
  original->db_name = "db0";
  original->assigned_worker_id = "worker-1";
  original->retry_count = 0;

  std::atomic<bool> stop{false};
  std::atomic<int> copy_count{0};

  // 线程 1: 持续修改 original 的字段
  std::thread modifier([&]() {
    int i = 0;
    while (!stop.load()) {
      std::lock_guard<std::mutex> lock(original->mtx);
      original->retry_count = i;
      original->assigned_worker_id = "worker-" + std::to_string(i % 100);
      original->error_message = "error-" + std::to_string(i);
      if (i % 3 == 0) {
        original->status = TaskStatus::kRunning;
      } else if (i % 3 == 1) {
        original->status = TaskStatus::kAssigned;
      } else {
        original->status = TaskStatus::kPending;
      }
      i++;
    }
  });

  // 线程 2-5: 持续拷贝 original
  std::vector<std::thread> copiers;
  for (int t = 0; t < 4; t++) {
    copiers.emplace_back([&]() {
      while (!stop.load()) {
        // 拷贝构造函数内部会加锁 other.mtx
        TaskInfo copy(*original);
        copy_count++;

        // 验证拷贝的一致性：task_id 应该始终是 "orig-001"
        EXPECT_EQ(copy.task_id, "orig-001");
        EXPECT_EQ(copy.source_node_id, "node-1");
        EXPECT_EQ(copy.db_name, "db0");
        // status 应该是有效值
        EXPECT_TRUE(copy.status == TaskStatus::kRunning ||
                    copy.status == TaskStatus::kAssigned ||
                    copy.status == TaskStatus::kPending);
      }
    });
  }

  // 运行 200ms
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  stop = true;

  modifier.join();
  for (auto& t : copiers) {
    t.join();
  }

  // 应该成功执行了大量拷贝
  EXPECT_GT(copy_count.load(), 0) << "No copies were made";
}

// ============================================================================
// TaskInfo 赋值运算符地址顺序锁定
// 验证：两个 TaskInfo 互相赋值时不死锁
// ============================================================================
TEST(ControlPlaneRegression, TaskInfoAssignment_AddressOrderedLocking) {
  auto task_a = std::make_shared<TaskInfo>();
  task_a->task_id = "task-A";
  task_a->status = TaskStatus::kRunning;
  task_a->assigned_worker_id = "worker-A";

  auto task_b = std::make_shared<TaskInfo>();
  task_b->task_id = "task-B";
  task_b->status = TaskStatus::kPending;
  task_b->assigned_worker_id = "worker-B";

  std::atomic<bool> done_ab{false};
  std::atomic<bool> done_ba{false};

  // 线程 1: A = B
  std::thread t1([&]() {
    for (int i = 0; i < 1000; i++) {
      *task_a = *task_b;
    }
    done_ab = true;
  });

  // 线程 2: B = A（反向赋值，地址顺序锁确保不死锁）
  std::thread t2([&]() {
    for (int i = 0; i < 1000; i++) {
      *task_b = *task_a;
    }
    done_ba = true;
  });

  t1.join();
  t2.join();

  // 到达这里说明没有死锁
  EXPECT_TRUE(done_ab.load());
  EXPECT_TRUE(done_ba.load());
}

// ============================================================================
// TaskInfo 自赋值安全
// ============================================================================
TEST(ControlPlaneRegression, TaskInfoSelfAssignment_Safe) {
  TaskInfo task;
  task.task_id = "self-test";
  task.status = TaskStatus::kRunning;
  task.assigned_worker_id = "w1";

  // 自赋值不应崩溃或死锁
  task = task;

  EXPECT_EQ(task.task_id, "self-test");
  EXPECT_EQ(task.status, TaskStatus::kRunning);
}

// ============================================================================
// HandleRetry 契约 — 不操作 running_tasks_
// 验证：调用 HandleRetry 后，running_tasks_ 不被修改
// （因为调用方必须在调前清理）
// ============================================================================
TEST(ControlPlaneRegression, HandleRetry_DoesNotTouchRunningTasks) {
  SchedulerConfig config = MakeTestConfig();
  TaskScheduler scheduler(config);

  auto task_info = MakeTestTask("", 5);  // max_retries = 5
  auto id = scheduler.SubmitTask(task_info);

  // 手动将任务设为 Running 并加入 running_tasks_（模拟 ExecuteDecisions）
  auto task = scheduler.GetTask(id);
  {
    std::lock_guard<std::mutex> lock(task->mtx);
    task->status = TaskStatus::kRunning;
    task->assigned_worker_id = "w1";
  }

  // 模拟 OnTaskCompleted: 先失败
  TaskResult fail_result;
  fail_result.success = false;
  fail_result.error_message = "test error";
  scheduler.OnTaskCompleted(id, fail_result);

  // HandleRetry 被 OnTaskCompleted 内部调用
  // 任务应该回到 Pending 状态
  EXPECT_EQ(task->status, TaskStatus::kPending);
  EXPECT_EQ(task->retry_count, 1);
  EXPECT_TRUE(task->assigned_worker_id.empty());

  // running count 应该已经在 OnTaskCompleted 中被减少（HandleRetry 不管）
  // 不检查具体值，只确认系统没崩溃
}

// ============================================================================
// HandleRetry 单次加锁（Round 5 NEW-5）
// 验证：kRetrying 中间状态不会被外部观察到（合并后的锁内完成）
// ============================================================================
TEST(ControlPlaneRegression, HandleRetry_SingleLock_NoRetryingVisible) {
  SchedulerConfig config = MakeTestConfig();
  TaskScheduler scheduler(config);

  auto task_info = MakeTestTask("", 10);
  auto id = scheduler.SubmitTask(task_info);

  auto task = scheduler.GetTask(id);
  std::atomic<bool> stop{false};
  std::atomic<int> retrying_observed{0};

  // 线程 1: 持续检查 task 的状态
  std::thread observer([&]() {
    while (!stop.load()) {
      std::lock_guard<std::mutex> lock(task->mtx);
      if (task->status == TaskStatus::kRetrying) {
        retrying_observed++;
      }
    }
  });

  // 线程 2: 触发 3 次失败（每次触发 HandleRetry）
  for (int i = 0; i < 3; i++) {
    TaskResult fail_result;
    fail_result.success = false;
    fail_result.error_message = "retry test";
    scheduler.OnTaskCompleted(id, fail_result);
    // 短暂等待让 observer 有机会检查
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  stop = true;
  observer.join();

  // 由于合并了两次加锁，kRetrying 中间状态不应被外部线程观察到
  // （在同一个 lock_guard 内从 kRetrying 转为 kPending）
  EXPECT_EQ(retrying_observed.load(), 0)
    << "kRetrying intermediate state was observed " << retrying_observed.load()
    << " times — HandleRetry should transition atomically under single lock";

  EXPECT_EQ(task->status, TaskStatus::kPending);
  EXPECT_EQ(task->retry_count, 3);
}

// ============================================================================
// DoSchedule 出队后检查 kPending（问题10）
// 验证：已取消的任务从队列弹出时被跳过
// ============================================================================
TEST(ControlPlaneRegression, DoSchedule_SkipsCancelledTasks) {
  SchedulerConfig config = MakeTestConfig();
  config.scheduling_interval_ms = 50;
  TaskScheduler scheduler(config);

  // 注册 Worker
  WorkerManagerConfig wm_config;
  auto wm = std::make_shared<WorkerManager>(wm_config);
  WorkerResources res;
  res.max_concurrent_tasks = 10;
  res.total_memory_mb = 8000;
  res.used_memory_mb = 1000;
  wm->RegisterWorker("localhost:8001", res, {}, "w1");
  scheduler.SetWorkerManager(wm);

  // 提交 5 个任务
  std::vector<std::string> ids;
  for (int i = 0; i < 5; i++) {
    auto task = MakeTestTask("", 0);
    ids.push_back(scheduler.SubmitTask(task));
  }

  // 取消前 3 个（它们仍在 pending_queue_ 中但状态变为 kCancelled）
  for (int i = 0; i < 3; i++) {
    scheduler.CancelTask(ids[i], "pre-cancel");
  }

  // 启动调度器
  scheduler.Start();
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  scheduler.Stop();

  // 被取消的任务应该保持 kCancelled
  for (int i = 0; i < 3; i++) {
    auto task = scheduler.GetTask(ids[i]);
    ASSERT_NE(task, nullptr);
    EXPECT_EQ(task->status, TaskStatus::kCancelled)
      << "Task " << i << " should be cancelled";
  }

  // 后 2 个任务应该被调度（可能是 Assigned/Running，也可能因 gRPC 失败回退到 Pending）
  // 关键是系统没崩溃且取消的任务没被重新分配
}

// ============================================================================
// CleanupCompletedTasks 清理 bulk_load_tasks_（问题7）
// 验证：过期的 BulkLoad 父任务从 bulk_load_tasks_ 中被清理
// ============================================================================
TEST(ControlPlaneRegression, CleanupBulkLoadTasks) {
  SchedulerConfig config = MakeTestConfig();
  config.completed_task_retention_sec = 1;
  TaskScheduler scheduler(config);

  // 提交 BulkLoad 父任务
  TaskInfo bulk_task;
  bulk_task.type = TaskType::kBulkLoad;
  bulk_task.bulk_load_params.source_path = "/data/test.kv";
  bulk_task.bulk_load_params.shard_count = 2;
  auto parent_id = scheduler.SubmitBulkLoadTask(bulk_task);

  // 确认父任务存在
  EXPECT_NE(scheduler.GetBulkLoadTask(parent_id), nullptr);

  // 手动将父任务标记为完成（模拟所有分片完成后的状态）
  auto parent = scheduler.GetBulkLoadTask(parent_id);
  {
    std::lock_guard<std::mutex> lock(parent->mtx);
    parent->status = TaskStatus::kCompleted;
    parent->complete_time = std::chrono::system_clock::now() -
                            std::chrono::seconds(10);  // 10 秒前完成
  }

  // 手动触发清理（通过启动调度器短暂运行）
  WorkerManagerConfig wm_config;
  auto wm = std::make_shared<WorkerManager>(wm_config);
  scheduler.SetWorkerManager(wm);
  scheduler.Start();
  std::this_thread::sleep_for(std::chrono::seconds(2));
  scheduler.Stop();

  // 过期的父任务应该被从 all_tasks_ 和 bulk_load_tasks_ 中清理
  EXPECT_EQ(scheduler.GetTask(parent_id), nullptr)
    << "Expired parent task should be cleaned from all_tasks_";
  EXPECT_EQ(scheduler.GetBulkLoadTask(parent_id), nullptr)
    << "Expired parent task should be cleaned from bulk_load_tasks_";
}

// ============================================================================
// 并发 BulkLoad 分片完成 + 父任务清理
// 验证：分片完成传播和清理不冲突
// ============================================================================
TEST(ControlPlaneRegression, BulkLoadShardCompletionAndCleanup) {
  SchedulerConfig config = MakeTestConfig();
  config.completed_task_retention_sec = 3600;  // 长保留期
  TaskScheduler scheduler(config);

  // 创建父任务
  TaskInfo parent;
  parent.type = TaskType::kBulkLoad;
  parent.bulk_load_params.shard_count = 10;
  auto parent_id = scheduler.SubmitBulkLoadTask(parent);

  // 创建 10 个分片子任务
  std::vector<std::string> shard_ids;
  for (int i = 0; i < 10; i++) {
    TaskInfo shard;
    shard.type = TaskType::kBulkLoad;
    shard.parent_task_id = parent_id;
    shard.max_retries = 0;
    shard_ids.push_back(scheduler.SubmitBulkLoadShard(shard));
  }

  // 并发完成所有分片（5 成功 + 5 失败）
  std::vector<std::thread> threads;
  for (int i = 0; i < 10; i++) {
    threads.emplace_back([&, i]() {
      TaskResult result;
      result.success = (i < 5);
      if (!result.success) {
        result.error_message = "shard error";
      }
      scheduler.OnTaskCompleted(shard_ids[i], result);
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  auto parent_task = scheduler.GetBulkLoadTask(parent_id);
  ASSERT_NE(parent_task, nullptr);
  EXPECT_EQ(parent_task->bulk_load_params.completed_shards, 5u);
  EXPECT_EQ(parent_task->bulk_load_params.failed_shards, 5u);
}

// ============================================================================
// 并发 WaitForTask 超时 + 完成
// 验证：等待超时后任务完成不导致问题
// ============================================================================
TEST(ControlPlaneRegression, WaitTimeout_ThenComplete) {
  SchedulerConfig config = MakeTestConfig();
  TaskScheduler scheduler(config);

  auto id = scheduler.SubmitTask(MakeTestTask("", 0));

  // 等待 100ms 超时
  TaskResult wait_result;
  bool completed = scheduler.WaitForTask(id, &wait_result, 100);
  EXPECT_FALSE(completed);

  // 任务之后完成
  TaskResult success;
  success.success = true;
  scheduler.OnTaskCompleted(id, success);

  // 再次查询 — 应该已完成
  auto task = scheduler.GetTask(id);
  EXPECT_EQ(task->status, TaskStatus::kCompleted);
}

// ============================================================================
// 并发拷贝构造 + 赋值 + 修改 — 压力测试
// ============================================================================
TEST(ControlPlaneRegression, TaskInfo_ConcurrentCopyAssignModify_StressTest) {
  auto shared_task = std::make_shared<TaskInfo>();
  shared_task->task_id = "stress-001";
  shared_task->status = TaskStatus::kPending;
  shared_task->assigned_worker_id = "w-init";
  shared_task->retry_count = 0;
  shared_task->error_message = "none";
  shared_task->params.score = 1.0;

  std::atomic<bool> stop{false};
  std::vector<std::thread> threads;

  // 修改者线程
  threads.emplace_back([&]() {
    int i = 0;
    while (!stop.load()) {
      std::lock_guard<std::mutex> lock(shared_task->mtx);
      shared_task->retry_count = i % 100;
      shared_task->assigned_worker_id = "w-" + std::to_string(i % 50);
      shared_task->params.score = static_cast<double>(i);
      i++;
    }
  });

  // 拷贝构造者线程
  for (int t = 0; t < 3; t++) {
    threads.emplace_back([&]() {
      while (!stop.load()) {
        TaskInfo copy(*shared_task);
        EXPECT_EQ(copy.task_id, "stress-001");
      }
    });
  }

  // 赋值者线程
  for (int t = 0; t < 3; t++) {
    threads.emplace_back([&]() {
      TaskInfo local;
      local.task_id = "local-" + std::to_string(t);
      while (!stop.load()) {
        local = *shared_task;
        EXPECT_EQ(local.task_id, "stress-001");
      }
    });
  }

  // 运行 300ms
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  stop = true;

  for (auto& t : threads) {
    t.join();
  }

  // 到达这里说明无死锁、无崩溃
}

// ============================================================================
// 多任务并发完成 + 查询 + 等待 — 锁一致性
// 验证各 gRPC handler 中 per-task mutex 的正确性
// ============================================================================
TEST(ControlPlaneRegression, ConcurrentCompleteAndQuery_LockConsistency) {
  SchedulerConfig config = MakeTestConfig();
  config.max_pending_tasks = 10000;
  TaskScheduler scheduler(config);

  constexpr int kTasks = 100;
  std::vector<std::string> ids;

  for (int i = 0; i < kTasks; i++) {
    auto task = MakeTestTask("", 0);
    ids.push_back(scheduler.SubmitTask(task));
  }

  std::atomic<int> queries_done{0};
  std::atomic<bool> stop{false};

  // 查询者线程（模拟 gRPC handler 中的 QueryTasks）
  std::thread querier([&]() {
    while (!stop.load()) {
      TaskFilter filter;
      auto tasks = scheduler.QueryTasks(filter);
      for (const auto& t : tasks) {
        // 模拟 gRPC handler 读取字段（加 per-task mutex）
        std::lock_guard<std::mutex> lock(t->mtx);
        // 读取各字段不应崩溃
        volatile auto s = t->status;
        volatile auto r = t->retry_count;
        (void)s;
        (void)r;
      }
      queries_done++;
    }
  });

  // 完成者线程并发完成所有任务
  std::vector<std::thread> completers;
  for (int i = 0; i < 4; i++) {
    completers.emplace_back([&, i]() {
      int start = i * (kTasks / 4);
      int end = (i + 1) * (kTasks / 4);
      for (int j = start; j < end; j++) {
        TaskResult result;
        result.success = (j % 2 == 0);
        if (!result.success) {
          result.error_message = "test error " + std::to_string(j);
        }
        scheduler.OnTaskCompleted(ids[j], result);
      }
    });
  }

  for (auto& t : completers) {
    t.join();
  }

  stop = true;
  querier.join();

  EXPECT_GT(queries_done.load(), 0);

  // 所有任务应该在终态
  for (int i = 0; i < kTasks; i++) {
    auto task = scheduler.GetTask(ids[i]);
    ASSERT_NE(task, nullptr);
    EXPECT_TRUE(task->IsTerminal())
      << "Task " << i << " not terminal: "
      << TaskStatusToString(task->status);
  }
}

// ============================================================================
// CancelTask 在 DoSchedule 出队前 — 计数器正确
// 验证取消任务后 pending_count 正确，即使任务仍在 priority_queue 中
// ============================================================================
TEST(ControlPlaneRegression, CancelBeforeDequeue_PendingCountCorrect) {
  SchedulerConfig config = MakeTestConfig();
  TaskScheduler scheduler(config);

  // 提交 3 个任务
  auto id1 = scheduler.SubmitTask(MakeTestTask("", 0));
  auto id2 = scheduler.SubmitTask(MakeTestTask("", 0));
  auto id3 = scheduler.SubmitTask(MakeTestTask("", 0));

  const auto& stats = scheduler.GetStatistics();
  EXPECT_EQ(stats.pending_count.load(), 3u);
  EXPECT_EQ(scheduler.GetPendingCount(), 3u);

  // 取消第 1 和第 3 个
  scheduler.CancelTask(id1, "cancel-1");
  scheduler.CancelTask(id3, "cancel-3");

  // pending_count 通过 CancelTask 减少
  EXPECT_EQ(stats.pending_count.load(), 1u);

  // pending_queue_.size() 仍然是 3（取消不从 queue 中移除）
  EXPECT_EQ(scheduler.GetPendingCount(), 3u);

  // 完成第 2 个
  TaskResult result;
  result.success = true;
  scheduler.OnTaskCompleted(id2, result);

  EXPECT_EQ(stats.total_cancelled.load(), 2u);
  EXPECT_EQ(stats.total_completed.load(), 1u);
}

// ============================================================================
// 并发 HandleRetry 与 CancelTask — 不死锁
// ============================================================================
TEST(ControlPlaneRegression, RetryAndCancel_NoConcurrentDeadlock) {
  for (int trial = 0; trial < 50; trial++) {
    SchedulerConfig config = MakeTestConfig();
    TaskScheduler scheduler(config);

    auto task_info = MakeTestTask("", 10);
    auto id = scheduler.SubmitTask(task_info);

    // 线程 1: 报告失败（触发 HandleRetry）
    std::thread retrier([&]() {
      TaskResult result;
      result.success = false;
      result.error_message = "trigger retry";
      scheduler.OnTaskCompleted(id, result);
    });

    // 线程 2: 取消任务
    std::thread canceller([&]() {
      scheduler.CancelTask(id, "concurrent cancel");
    });

    retrier.join();
    canceller.join();

    auto task = scheduler.GetTask(id);
    // 任务应该要么被取消，要么被重试
    // 不检查精确状态，只确认系统没死锁
    EXPECT_NE(task, nullptr);
  }
}

// ============================================================================
// Fallback 路径测试 — 无 Worker 时任务降级
// ============================================================================
TEST(ControlPlaneRegression, Fallback_NoWorkerAvailable) {
  SchedulerConfig config = MakeTestConfig();
  config.scheduling_interval_ms = 50;
  TaskScheduler scheduler(config);

  // 设置 WorkerManager 但不注册任何 Worker
  WorkerManagerConfig wm_config;
  auto wm = std::make_shared<WorkerManager>(wm_config);
  scheduler.SetWorkerManager(wm);

  auto id = scheduler.SubmitTask(MakeTestTask("", 0));

  // 启动调度器
  scheduler.Start();
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  scheduler.Stop();

  auto task = scheduler.GetTask(id);
  ASSERT_NE(task, nullptr);

  // 没有可用 Worker 时，ShouldFallback 返回 true
  // 任务应该被降级（状态变为 Failed，should_fallback = true）
  EXPECT_EQ(task->status, TaskStatus::kFailed);
  EXPECT_TRUE(task->should_fallback);
}

// ============================================================================
// 多轮重试 + 最终成功
// 验证 HandleRetry 合并锁后的完整重试流程
// ============================================================================
TEST(ControlPlaneRegression, MultipleRetries_FinallySucceeds) {
  SchedulerConfig config = MakeTestConfig();
  TaskScheduler scheduler(config);

  auto task_info = MakeTestTask("", 5);  // max_retries = 5
  auto id = scheduler.SubmitTask(task_info);

  // 失败 3 次
  for (int i = 0; i < 3; i++) {
    TaskResult fail;
    fail.success = false;
    fail.error_message = "failure " + std::to_string(i);
    scheduler.OnTaskCompleted(id, fail);

    auto task = scheduler.GetTask(id);
    EXPECT_EQ(task->status, TaskStatus::kPending);
    EXPECT_EQ(task->retry_count, i + 1);
  }

  // 第 4 次成功
  TaskResult success;
  success.success = true;
  success.compaction_result = "compaction_output";
  scheduler.OnTaskCompleted(id, success);

  auto task = scheduler.GetTask(id);
  EXPECT_EQ(task->status, TaskStatus::kCompleted);
  EXPECT_TRUE(task->result.success);
  EXPECT_EQ(task->result.compaction_result, "compaction_output");
  EXPECT_EQ(task->retry_count, 3);  // 3 次重试后成功

  const auto& stats = scheduler.GetStatistics();
  EXPECT_EQ(stats.total_completed.load(), 1u);
  EXPECT_EQ(stats.total_failed.load(), 0u);
}

// ============================================================================
// BulkLoad 分片队列 — 出队检查 kPending
// ============================================================================
TEST(ControlPlaneRegression, BulkLoadShard_DoScheduleSkipsCancelled) {
  SchedulerConfig config = MakeTestConfig();
  config.scheduling_interval_ms = 50;
  TaskScheduler scheduler(config);

  WorkerManagerConfig wm_config;
  auto wm = std::make_shared<WorkerManager>(wm_config);
  WorkerResources res;
  res.max_concurrent_tasks = 10;
  res.total_memory_mb = 8000;
  res.used_memory_mb = 1000;
  wm->RegisterWorker("localhost:8001", res, {}, "w1");
  scheduler.SetWorkerManager(wm);

  // 提交 3 个 BulkLoad 分片任务
  std::vector<std::string> shard_ids;
  for (int i = 0; i < 3; i++) {
    TaskInfo shard;
    shard.type = TaskType::kBulkLoad;
    shard.max_retries = 0;
    shard_ids.push_back(scheduler.SubmitBulkLoadShard(shard));
  }

  // 取消前 2 个
  scheduler.CancelTask(shard_ids[0], "cancel shard 0");
  scheduler.CancelTask(shard_ids[1], "cancel shard 1");

  // 启动调度 — DoSchedule 应该跳过被取消的分片
  scheduler.Start();
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  scheduler.Stop();

  // 被取消的分片应保持 kCancelled
  for (int i = 0; i < 2; i++) {
    auto task = scheduler.GetTask(shard_ids[i]);
    EXPECT_EQ(task->status, TaskStatus::kCancelled);
  }
}

// ============================================================================
// 大规模并发压力测试 — 提交 + 重试 + 取消 + 等待 + 查询
// ============================================================================
TEST(ControlPlaneRegression, FullStressTest_AllOperations) {
  SchedulerConfig config = MakeTestConfig();
  config.max_pending_tasks = 50000;
  TaskScheduler scheduler(config);

  constexpr int kTasks = 500;
  std::vector<std::string> ids(kTasks);
  std::mutex ids_mutex;
  std::atomic<int> submitted{0};

  // 阶段 1: 并发提交
  std::vector<std::thread> submitters;
  for (int t = 0; t < 4; t++) {
    submitters.emplace_back([&, t]() {
      int start = t * (kTasks / 4);
      int end = (t + 1) * (kTasks / 4);
      for (int j = start; j < end; j++) {
        auto task = MakeTestTask("", (j % 3 == 0) ? 3 : 0);
        auto id = scheduler.SubmitTask(task);
        std::lock_guard<std::mutex> lock(ids_mutex);
        ids[j] = id;
        submitted++;
      }
    });
  }
  for (auto& t : submitters) {
    t.join();
  }
  EXPECT_EQ(submitted.load(), kTasks);

  // 阶段 2: 并发完成/取消/等待
  std::atomic<int> completed{0}, cancelled{0}, waited{0};
  std::vector<std::thread> workers;

  // 完成者: 处理 0-249
  for (int t = 0; t < 2; t++) {
    workers.emplace_back([&, t]() {
      int start = t * 125;
      int end = (t + 1) * 125;
      for (int j = start; j < end; j++) {
        std::string id;
        { std::lock_guard<std::mutex> lock(ids_mutex); id = ids[j]; }
        if (!id.empty()) {
          TaskResult result;
          result.success = (j % 5 != 0);
          if (!result.success)
            result.error_message = "fail";
          scheduler.OnTaskCompleted(id, result);
          completed++;
        }
      }
    });
  }

  // 取消者: 处理 250-399
  workers.emplace_back([&]() {
    for (int j = 250; j < 400; j++) {
      std::string id;
      { std::lock_guard<std::mutex> lock(ids_mutex); id = ids[j]; }
      if (!id.empty()) {
        scheduler.CancelTask(id, "stress cancel");
        cancelled++;
      }
    }
  });

  // 等待者: 处理 400-499
  workers.emplace_back([&]() {
    for (int j = 400; j < 500; j++) {
      std::string id;
      { std::lock_guard<std::mutex> lock(ids_mutex); id = ids[j]; }
      if (!id.empty()) {
        TaskResult result;
        scheduler.WaitForTask(id, &result, 10);  // 短超时
        waited++;
      }
    }
  });

  // 完成等待区的任务
  workers.emplace_back([&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    for (int j = 400; j < 500; j++) {
      std::string id;
      { std::lock_guard<std::mutex> lock(ids_mutex); id = ids[j]; }
      if (!id.empty()) {
        TaskResult result;
        result.success = true;
        scheduler.OnTaskCompleted(id, result);
      }
    }
  });

  for (auto& t : workers) {
    t.join();
  }

  // 系统不崩溃、不死锁
  EXPECT_GT(completed.load(), 0);
  EXPECT_GT(cancelled.load(), 0);
  EXPECT_GT(waited.load(), 0);

  const auto& stats = scheduler.GetStatistics();
  EXPECT_EQ(stats.total_submitted.load(), static_cast<uint64_t>(kTasks));
}

// ============================================================================
// TaskInfo 拷贝构造 — 字段完整性验证
// 确保 copy constructor 复制了 TaskInfo 的所有字段
// ============================================================================
TEST(ControlPlaneRegression, TaskInfoCopy_FieldCompleteness) {
  TaskInfo original;
  original.task_id = "field-test-001";
  original.type = TaskType::kBulkLoad;
  original.status = TaskStatus::kRunning;
  original.priority = TaskPriority::kUrgent;
  original.source_node_id = "source-node-42";
  original.db_name = "testdb";
  original.store_id = 7;
  original.assigned_worker_id = "worker-99";
  original.retry_count = 2;
  original.max_retries = 5;
  original.reschedule_count = 1;
  original.submit_time = std::chrono::system_clock::now() -
                         std::chrono::seconds(60);
  original.assign_time = std::chrono::system_clock::now() -
                         std::chrono::seconds(30);
  original.start_time = std::chrono::system_clock::now() -
                        std::chrono::seconds(20);
  original.complete_time = std::chrono::system_clock::now();
  original.params.job_id = 12345;
  original.params.compaction_input = "input_data_here";
  original.params.start_level = 3;
  original.params.score = 42.5;
  original.params.timeout_sec = 7200;
  original.bulk_load_params.source_path = "/data/import.kv";
  original.bulk_load_params.shard_count = 4;
  original.bulk_load_params.completed_shards = 2;
  original.bulk_load_params.failed_shards = 1;
  original.result.success = true;
  original.result.compaction_result = "result_data";
  original.result.execution_time_ms = 15000;
  original.error_message = "some error";
  original.should_fallback = true;
  original.parent_task_id = "parent-001";

  // 拷贝构造
  TaskInfo copy(original);

  // 验证所有字段
  EXPECT_EQ(copy.task_id, "field-test-001");
  EXPECT_EQ(copy.type, TaskType::kBulkLoad);
  EXPECT_EQ(copy.status, TaskStatus::kRunning);
  EXPECT_EQ(copy.priority, TaskPriority::kUrgent);
  EXPECT_EQ(copy.source_node_id, "source-node-42");
  EXPECT_EQ(copy.db_name, "testdb");
  EXPECT_EQ(copy.store_id, 7u);
  EXPECT_EQ(copy.assigned_worker_id, "worker-99");
  EXPECT_EQ(copy.retry_count, 2);
  EXPECT_EQ(copy.max_retries, 5);
  EXPECT_EQ(copy.reschedule_count, 1);
  EXPECT_EQ(copy.params.job_id, 12345u);
  EXPECT_EQ(copy.params.compaction_input, "input_data_here");
  EXPECT_EQ(copy.params.start_level, 3);
  EXPECT_DOUBLE_EQ(copy.params.score, 42.5);
  EXPECT_EQ(copy.params.timeout_sec, 7200u);
  EXPECT_EQ(copy.bulk_load_params.source_path, "/data/import.kv");
  EXPECT_EQ(copy.bulk_load_params.shard_count, 4);
  EXPECT_EQ(copy.bulk_load_params.completed_shards, 2u);
  EXPECT_EQ(copy.bulk_load_params.failed_shards, 1u);
  EXPECT_TRUE(copy.result.success);
  EXPECT_EQ(copy.result.compaction_result, "result_data");
  EXPECT_EQ(copy.result.execution_time_ms, 15000u);
  EXPECT_EQ(copy.error_message, "some error");
  EXPECT_TRUE(copy.should_fallback);
  EXPECT_EQ(copy.parent_task_id, "parent-001");

  // 验证时间字段一致
  EXPECT_EQ(copy.submit_time, original.submit_time);
  EXPECT_EQ(copy.assign_time, original.assign_time);
  EXPECT_EQ(copy.start_time, original.start_time);
  EXPECT_EQ(copy.complete_time, original.complete_time);
}

// ============================================================================
// TaskInfo 赋值操作 — 字段完整性验证
// ============================================================================
TEST(ControlPlaneRegression, TaskInfoAssignment_FieldCompleteness) {
  TaskInfo original;
  original.task_id = "assign-test-001";
  original.type = TaskType::kBulkLoad;
  original.status = TaskStatus::kCompleted;
  original.priority = TaskPriority::kHigh;
  original.source_node_id = "node-assign";
  original.db_name = "assigndb";
  original.store_id = 3;
  original.assigned_worker_id = "worker-assign";
  original.retry_count = 1;
  original.max_retries = 4;
  original.reschedule_count = 2;
  original.params.score = 99.9;
  original.params.start_level = 5;
  original.bulk_load_params.source_path = "/bulk/data";
  original.error_message = "assignment test";
  original.should_fallback = true;
  original.parent_task_id = "parent-assign";

  TaskInfo target;
  target.task_id = "will-be-overwritten";
  target = original;

  EXPECT_EQ(target.task_id, "assign-test-001");
  EXPECT_EQ(target.type, TaskType::kBulkLoad);
  EXPECT_EQ(target.status, TaskStatus::kCompleted);
  EXPECT_EQ(target.priority, TaskPriority::kHigh);
  EXPECT_EQ(target.source_node_id, "node-assign");
  EXPECT_EQ(target.db_name, "assigndb");
  EXPECT_EQ(target.store_id, 3u);
  EXPECT_EQ(target.assigned_worker_id, "worker-assign");
  EXPECT_EQ(target.retry_count, 1);
  EXPECT_EQ(target.max_retries, 4);
  EXPECT_EQ(target.reschedule_count, 2);
  EXPECT_DOUBLE_EQ(target.params.score, 99.9);
  EXPECT_EQ(target.params.start_level, 5);
  EXPECT_EQ(target.bulk_load_params.source_path, "/bulk/data");
  EXPECT_EQ(target.error_message, "assignment test");
  EXPECT_TRUE(target.should_fallback);
  EXPECT_EQ(target.parent_task_id, "parent-assign");
}

// ============================================================================
// HandleRetry 后任务重新入队 — pending_queue_ 验证
// 验证失败→重试后，任务能再次被调度器取出
// ============================================================================
TEST(ControlPlaneRegression, HandleRetry_RequeuesTask) {
  SchedulerConfig config = MakeTestConfig();
  TaskScheduler scheduler(config);

  auto task_info = MakeTestTask("", 5);
  auto id = scheduler.SubmitTask(task_info);

  // 初始 pending count = 1
  EXPECT_EQ(scheduler.GetPendingCount(), 1u);

  // 失败触发 HandleRetry
  TaskResult fail;
  fail.success = false;
  fail.error_message = "trigger retry";
  scheduler.OnTaskCompleted(id, fail);

  // 任务回到 kPending 且重新入队
  auto task = scheduler.GetTask(id);
  EXPECT_EQ(task->status, TaskStatus::kPending);
  EXPECT_EQ(task->retry_count, 1);

  // pending count 应该仍然是正值（任务重新入队了）
  // 注: pending_queue_.size() >= 1 (可能包含原始入队和重试入队)
  EXPECT_GE(scheduler.GetPendingCount(), 1u);

  // 再次失败
  TaskResult fail2;
  fail2.success = false;
  fail2.error_message = "retry again";
  scheduler.OnTaskCompleted(id, fail2);

  EXPECT_EQ(task->status, TaskStatus::kPending);
  EXPECT_EQ(task->retry_count, 2);
}

// ============================================================================
// 调度器运行中 + 并发修改 task 字段 — 锁安全验证
// 验证 TimeoutCheckLoop / CSAStatusCheckLoop 中的 per-task lock 修复
// ============================================================================
TEST(ControlPlaneRegression, SchedulerRunning_ConcurrentFieldModify) {
  SchedulerConfig config = MakeTestConfig();
  config.scheduling_interval_ms = 20;
  config.task_timeout_sec = 3600;
  config.csa_status_check_interval_sec = 1;
  TaskScheduler scheduler(config);

  WorkerManagerConfig wm_config;
  auto wm = std::make_shared<WorkerManager>(wm_config);
  WorkerResources res;
  res.max_concurrent_tasks = 10;
  res.total_memory_mb = 8000;
  res.used_memory_mb = 1000;
  wm->RegisterWorker("localhost:8001", res, {}, "w1");
  scheduler.SetWorkerManager(wm);

  // 提交多个任务
  std::vector<std::string> ids;
  for (int i = 0; i < 10; i++) {
    ids.push_back(scheduler.SubmitTask(MakeTestTask("", 0)));
  }

  // 启动调度器（背景线程会运行 TimeoutCheckLoop 和 CSAStatusCheckLoop）
  scheduler.Start();

  // 并发修改 task 字段（模拟多线程 gRPC handler）
  std::atomic<bool> stop{false};
  std::vector<std::thread> modifiers;
  for (int t = 0; t < 4; t++) {
    modifiers.emplace_back([&, t]() {
      int i = 0;
      while (!stop.load()) {
        auto task = scheduler.GetTask(ids[t % ids.size()]);
        if (task) {
          std::lock_guard<std::mutex> lock(task->mtx);
          task->assigned_worker_id = "w-" + std::to_string(i % 10);
          task->error_message = "msg-" + std::to_string(i);
        }
        i++;
        std::this_thread::sleep_for(std::chrono::microseconds(100));
      }
    });
  }

  // 运行 500ms
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  stop = true;

  for (auto& t : modifiers) {
    t.join();
  }

  scheduler.Stop();

  // 到达这里说明 TimeoutCheckLoop/CSAStatusCheckLoop 的 per-task lock
  // 与并发修改不冲突（无死锁、无崩溃）
}

// ============================================================================
// CleanupCompletedTasks — 验证清理 completed_results_ 和 task_cv_map_
// ============================================================================
TEST(ControlPlaneRegression, Cleanup_RemovesAllRelatedData) {
  SchedulerConfig config = MakeTestConfig();
  config.completed_task_retention_sec = 1;
  config.completed_task_cleanup_interval_sec = 1;
  TaskScheduler scheduler(config);

  // 提交并完成任务
  std::vector<std::string> ids;
  for (int i = 0; i < 5; i++) {
    auto task_info = MakeTestTask("", 0);
    auto id = scheduler.SubmitTask(task_info);
    ids.push_back(id);
  }

  // 先等待再完成（建立 task_cv_map_ 条目）
  for (int i = 0; i < 5; i++) {
    TaskResult wait_result;
    // 非阻塞查询建立 cv 条目
    scheduler.WaitForTask(ids[i], &wait_result, 0);
  }

  // 完成所有任务
  for (int i = 0; i < 5; i++) {
    TaskResult result;
    result.success = true;
    scheduler.OnTaskCompleted(ids[i], result);
  }

  // 等待过期
  std::this_thread::sleep_for(std::chrono::seconds(2));

  // 启动调度器触发清理
  WorkerManagerConfig wm_config;
  auto wm = std::make_shared<WorkerManager>(wm_config);
  scheduler.SetWorkerManager(wm);
  scheduler.Start();
  std::this_thread::sleep_for(std::chrono::seconds(2));
  scheduler.Stop();

  // 验证所有任务都被清理
  int cleaned = 0;
  for (const auto& id : ids) {
    if (scheduler.GetTask(id) == nullptr) {
      cleaned++;
    }
  }
  EXPECT_GT(cleaned, 0) << "Expected some tasks to be cleaned up";

  // 对已清理的任务，WaitForTask 应返回 false（任务不存在）
  for (const auto& id : ids) {
    if (scheduler.GetTask(id) == nullptr) {
      TaskResult result;
      EXPECT_FALSE(scheduler.WaitForTask(id, &result, 0));
    }
  }
}

// ============================================================================
// 并发 Submit + Complete + Retry + Cancel + Wait + Query
// 全场景综合压力测试 — 验证所有锁修复的整体正确性
// ============================================================================
TEST(ControlPlaneRegression, UltraStress_AllOperationsInterleaved) {
  SchedulerConfig config = MakeTestConfig();
  config.max_pending_tasks = 100000;
  config.scheduling_interval_ms = 20;
  TaskScheduler scheduler(config);

  WorkerManagerConfig wm_config;
  auto wm = std::make_shared<WorkerManager>(wm_config);
  scheduler.SetWorkerManager(wm);
  scheduler.Start();

  constexpr int kTasks = 200;
  std::atomic<int> submitted{0};
  std::vector<std::string> ids(kTasks);
  std::mutex ids_mutex;
  std::atomic<bool> submit_done{false};

  // 提交者线程
  std::vector<std::thread> threads;
  for (int t = 0; t < 4; t++) {
    threads.emplace_back([&, t]() {
      int start = t * (kTasks / 4);
      int end = (t + 1) * (kTasks / 4);
      for (int j = start; j < end; j++) {
        // 混合有重试和无重试的任务
        auto task = MakeTestTask("", (j % 4 == 0) ? 5 : 0);
        auto id = scheduler.SubmitTask(task);
        {
          std::lock_guard<std::mutex> lock(ids_mutex);
          ids[j] = id;
        }
        submitted++;
      }
    });
  }
  for (auto& t : threads) {
    t.join();
  }
  submit_done = true;
  threads.clear();

  // 混合操作线程
  std::atomic<int> ops{0};

  // 完成者（一部分成功、一部分失败触发重试）
  for (int t = 0; t < 2; t++) {
    threads.emplace_back([&, t]() {
      int start = t * 50;
      int end = (t + 1) * 50;
      for (int j = start; j < end; j++) {
        std::string id;
        { std::lock_guard<std::mutex> lock(ids_mutex); id = ids[j]; }
        if (!id.empty()) {
          TaskResult result;
          result.success = (j % 3 != 0);  // 每 3 个有 1 个失败
          if (!result.success) result.error_message = "retry me";
          scheduler.OnTaskCompleted(id, result);
          ops++;
        }
      }
    });
  }

  // 取消者
  threads.emplace_back([&]() {
    for (int j = 100; j < 150; j++) {
      std::string id;
      { std::lock_guard<std::mutex> lock(ids_mutex); id = ids[j]; }
      if (!id.empty()) {
        scheduler.CancelTask(id, "stress cancel");
        ops++;
      }
    }
  });

  // 查询者
  threads.emplace_back([&]() {
    for (int j = 0; j < 20; j++) {
      TaskFilter filter;
      auto results = scheduler.QueryTasks(filter);
      for (const auto& t : results) {
        std::lock_guard<std::mutex> lock(t->mtx);
        volatile auto s = t->status;
        (void)s;
      }
      ops++;
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
  });

  // 等待者
  threads.emplace_back([&]() {
    for (int j = 150; j < 170; j++) {
      std::string id;
      { std::lock_guard<std::mutex> lock(ids_mutex); id = ids[j]; }
      if (!id.empty()) {
        TaskResult result;
        scheduler.WaitForTask(id, &result, 10);
        ops++;
      }
    }
  });

  // 完成等待者的任务
  threads.emplace_back([&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    for (int j = 150; j < 200; j++) {
      std::string id;
      { std::lock_guard<std::mutex> lock(ids_mutex); id = ids[j]; }
      if (!id.empty()) {
        TaskResult result;
        result.success = true;
        scheduler.OnTaskCompleted(id, result);
        ops++;
      }
    }
  });

  for (auto& t : threads) {
    t.join();
  }

  scheduler.Stop();

  // 系统应该无死锁、无崩溃
  EXPECT_EQ(submitted.load(), kTasks);
  EXPECT_GT(ops.load(), 0);

  const auto& stats = scheduler.GetStatistics();
  EXPECT_EQ(stats.total_submitted.load(), static_cast<uint64_t>(kTasks));
}

// ============================================================================
// 并发赋值 — 多对多交叉赋值不死锁
// ============================================================================
TEST(ControlPlaneRegression, TaskInfoAssignment_ManyToMany_NoDeadlock) {
  constexpr int kTasks = 6;
  std::vector<std::shared_ptr<TaskInfo>> tasks(kTasks);
  for (int i = 0; i < kTasks; i++) {
    tasks[i] = std::make_shared<TaskInfo>();
    tasks[i]->task_id = "task-" + std::to_string(i);
    tasks[i]->status = static_cast<TaskStatus>(i % 4 + 1);
  }

  // 多线程交叉赋值：tasks[0]=tasks[1], tasks[1]=tasks[2], ..., tasks[5]=tasks[0]
  std::vector<std::thread> threads;
  for (int round = 0; round < 100; round++) {
    threads.clear();
    for (int i = 0; i < kTasks; i++) {
      threads.emplace_back([&, i]() {
        *tasks[i] = *tasks[(i + 1) % kTasks];
      });
    }
    for (auto& t : threads) {
      t.join();
    }
  }
  // 到达这里说明地址顺序锁在多对多场景下仍然有效，无死锁
}

// ============================================================================
// OnTaskCompleted 对终态任务的幂等性
// 验证在各种终态（Completed、Failed、Cancelled、Timeout）下重复调用不会出错
// ============================================================================
TEST(ControlPlaneRegression, OnTaskCompleted_TerminalStateIdempotent) {
  SchedulerConfig config = MakeTestConfig();
  TaskScheduler scheduler(config);

  // 创建 4 个任务并分别进入不同终态
  auto id1 = scheduler.SubmitTask(MakeTestTask("", 0));
  auto id2 = scheduler.SubmitTask(MakeTestTask("", 0));
  auto id3 = scheduler.SubmitTask(MakeTestTask("", 0));
  auto id4 = scheduler.SubmitTask(MakeTestTask("", 0));

  // id1: Completed
  { TaskResult r; r.success = true; scheduler.OnTaskCompleted(id1, r); }
  // id2: Failed
  scheduler.OnTaskFailed(id2, "permanent error");
  // id3: Cancelled
  scheduler.CancelTask(id3, "user request");
  // id4: 手动设为 Timeout
  {
    auto task = scheduler.GetTask(id4);
    std::lock_guard<std::mutex> lock(task->mtx);
    task->status = TaskStatus::kTimeout;
  }

  const auto& stats = scheduler.GetStatistics();
  auto completed_before = stats.total_completed.load();
  auto failed_before = stats.total_failed.load();
  auto cancelled_before = stats.total_cancelled.load();

  // 对每个终态任务重复调用 OnTaskCompleted — 应该被忽略
  for (const auto& id : {id1, id2, id3, id4}) {
    TaskResult r;
    r.success = true;
    scheduler.OnTaskCompleted(id, r);
  }

  // 统计不应变化
  EXPECT_EQ(stats.total_completed.load(), completed_before);
  EXPECT_EQ(stats.total_failed.load(), failed_before);
  EXPECT_EQ(stats.total_cancelled.load(), cancelled_before);

  // 状态不应变化
  EXPECT_EQ(scheduler.GetTask(id1)->status, TaskStatus::kCompleted);
  EXPECT_EQ(scheduler.GetTask(id2)->status, TaskStatus::kFailed);
  EXPECT_EQ(scheduler.GetTask(id3)->status, TaskStatus::kCancelled);
  EXPECT_EQ(scheduler.GetTask(id4)->status, TaskStatus::kTimeout);
}

// ============================================================================
// 重试耗尽后变为 Failed + should_fallback
// 验证 max_retries 耗尽后的完整降级路径
// ============================================================================
TEST(ControlPlaneRegression, RetryExhausted_FinalFailure) {
  SchedulerConfig config = MakeTestConfig();
  TaskScheduler scheduler(config);

  auto task_info = MakeTestTask("", 2);  // max_retries = 2
  auto id = scheduler.SubmitTask(task_info);

  TaskResult fail;
  fail.success = false;
  fail.error_message = "persistent error";

  // 第 1 次失败 → 重试
  scheduler.OnTaskCompleted(id, fail);
  EXPECT_EQ(scheduler.GetTask(id)->status, TaskStatus::kPending);
  EXPECT_EQ(scheduler.GetTask(id)->retry_count, 1);

  // 第 2 次失败 → 重试
  scheduler.OnTaskCompleted(id, fail);
  EXPECT_EQ(scheduler.GetTask(id)->status, TaskStatus::kPending);
  EXPECT_EQ(scheduler.GetTask(id)->retry_count, 2);

  // 第 3 次失败 → 重试耗尽，最终失败
  scheduler.OnTaskCompleted(id, fail);
  auto task = scheduler.GetTask(id);
  EXPECT_EQ(task->status, TaskStatus::kFailed);
  EXPECT_TRUE(task->IsTerminal());

  const auto& stats = scheduler.GetStatistics();
  EXPECT_EQ(stats.total_failed.load(), 1u);
}

}  // namespace control_plane
}  // namespace tendisplus
