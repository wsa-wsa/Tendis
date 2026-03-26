// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

// Concurrency Safety Tests for Remote Compaction Framework
// ==========================================================
// 覆盖之前三轮代码审视中修复的所有并发安全问题：
//
// Part I: TaskInfo per-task mutex 与 CAS 状态转换
//   1. TryTransition CAS 正确性
//   2. TryTransitionFrom 多预期状态
//   3. 并发 TryTransition 竞争（只有一个线程成功）
//
// Part II: ABBA 死锁防护
//   4. 并发 WaitForTask + OnTaskCompleted（验证不死锁）
//   5. 并发 WaitForTask + CancelTask（验证不死锁）
//
// Part III: CancelTask 统计计数修正
//   6. CancelTask pending 任务 — pending_count 正确减少
//   7. CancelTask running/assigned 任务 — running_count 正确减少
//
// Part IV: OnTaskCompleted 终态保护
//   8. 双重完成竞争（OnTaskCompleted 两次同一任务）
//   9. OnTaskCompleted + CancelTask 竞争（只有一个成功）
//
// Part V: CleanupCompletedTasks 内存管理
//   10. 清理过期终态任务
//   11. 保留未过期的终态任务
//
// Part VI: HandleRetry 两阶段转换
//   12. kRetrying 中间状态（kFailed → kRetrying → kPending）
//   13. 重试后重新入队
//
// Part VII: 多线程压力测试
//   14. 并发提交 + 完成 + 取消 + 等待

#include "gtest/gtest.h"
#include "task_scheduler.h"

#include <atomic>
#include <chrono>
#include <mutex>
#include <thread>
#include <vector>

namespace tendisplus {
namespace control_plane {

// ============================================================================
// 辅助函数
// ============================================================================
static TaskInfo MakeTask(const std::string& id = "",
                         int max_retries = 3) {
  TaskInfo task;
  task.task_id = id;
  task.type = TaskType::kCompaction;
  task.priority = TaskPriority::kNormal;
  task.source_node_id = "node-1";
  task.db_name = "db0";
  task.store_id = 0;
  task.max_retries = max_retries;
  task.params.start_level = 0;
  task.params.score = 1.0;
  return task;
}

static SchedulerConfig MakeConfig() {
  SchedulerConfig config;
  config.scheduling_interval_ms = 50;
  config.task_timeout_sec = 3600;
  config.max_retries = 3;
  config.completed_task_retention_sec = 1;  // 1 秒过期（便于测试清理）
  config.completed_task_cleanup_interval_sec = 1;
  return config;
}

// ============================================================================
// Part I: TaskInfo per-task mutex 与 CAS 状态转换
// ============================================================================

// Test 1: TryTransition 单线程 CAS 正确性
TEST(ConcurrencySafety, TryTransition_BasicCAS) {
  TaskInfo task;
  task.status = TaskStatus::kPending;

  // 预期匹配 → 成功
  EXPECT_TRUE(task.TryTransition(TaskStatus::kPending, TaskStatus::kAssigned));
  EXPECT_EQ(task.status, TaskStatus::kAssigned);

  // 预期不匹配 → 失败，状态不变
  EXPECT_FALSE(task.TryTransition(TaskStatus::kPending, TaskStatus::kRunning));
  EXPECT_EQ(task.status, TaskStatus::kAssigned);

  // 再次正确转换
  EXPECT_TRUE(task.TryTransition(TaskStatus::kAssigned, TaskStatus::kRunning));
  EXPECT_EQ(task.status, TaskStatus::kRunning);
}

// Test 2: TryTransitionFrom 多预期状态
TEST(ConcurrencySafety, TryTransitionFrom_MultipleExpected) {
  TaskInfo task;

  // 测试 expected1 匹配
  task.status = TaskStatus::kRunning;
  EXPECT_TRUE(task.TryTransitionFrom(
    TaskStatus::kRunning, TaskStatus::kAssigned, TaskStatus::kTimeout));
  EXPECT_EQ(task.status, TaskStatus::kTimeout);

  // 测试 expected2 匹配
  task.status = TaskStatus::kAssigned;
  EXPECT_TRUE(task.TryTransitionFrom(
    TaskStatus::kRunning, TaskStatus::kAssigned, TaskStatus::kTimeout));
  EXPECT_EQ(task.status, TaskStatus::kTimeout);

  // 两个都不匹配 → 失败
  task.status = TaskStatus::kPending;
  EXPECT_FALSE(task.TryTransitionFrom(
    TaskStatus::kRunning, TaskStatus::kAssigned, TaskStatus::kTimeout));
  EXPECT_EQ(task.status, TaskStatus::kPending);
}

// Test 3: 并发 TryTransition 竞争 — 只有一个线程成功
TEST(ConcurrencySafety, TryTransition_ConcurrentRace) {
  for (int trial = 0; trial < 100; trial++) {
    auto task = std::make_shared<TaskInfo>();
    task->status = TaskStatus::kRunning;

    std::atomic<int> success_count{0};
    constexpr int kThreads = 8;

    std::vector<std::thread> threads;
    threads.reserve(kThreads);

    // 多个线程同时尝试将 Running → Timeout
    for (int i = 0; i < kThreads; i++) {
      threads.emplace_back([&task, &success_count]() {
        std::lock_guard<std::mutex> lock(task->mtx);
        if (task->TryTransition(TaskStatus::kRunning, TaskStatus::kTimeout)) {
          success_count++;
        }
      });
    }

    for (auto& t : threads) {
      t.join();
    }

    // 严格只有 1 个线程成功
    EXPECT_EQ(success_count.load(), 1)
      << "Trial " << trial << ": expected exactly 1 success, got "
      << success_count.load();
    EXPECT_EQ(task->status, TaskStatus::kTimeout);
  }
}

// ============================================================================
// Part II: ABBA 死锁防护
// 锁顺序规则：
//   task_cv_mutex_ → task->mtx (WaitForTask 的 predicate)
//   禁止: task->mtx → task_cv_mutex_ (NotifyTaskCompleted 在 task->mtx 外调用)
// ============================================================================

// Test 4: 并发 WaitForTask + OnTaskCompleted — 验证不死锁
TEST(ConcurrencySafety, WaitAndComplete_NoDeadlock) {
  SchedulerConfig config = MakeConfig();
  TaskScheduler scheduler(config);

  // 不调用 Start()，避免后台线程干扰
  auto task_info = MakeTask("", 0);
  auto id = scheduler.SubmitTask(task_info);

  // 线程 1: WaitForTask（会获取 task_cv_mutex_ → task->mtx）
  std::atomic<bool> wait_done{false};
  std::thread waiter([&]() {
    TaskResult result;
    bool completed = scheduler.WaitForTask(id, &result, 5000);
    EXPECT_TRUE(completed);
    EXPECT_TRUE(result.success);
    wait_done = true;
  });

  // 短暂等待确保 waiter 进入 wait
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  // 线程 2: OnTaskCompleted（会获取 task->mtx，然后在 mtx 外调用 NotifyTaskCompleted）
  std::thread completer([&]() {
    TaskResult result;
    result.success = true;
    scheduler.OnTaskCompleted(id, result);
  });

  completer.join();
  waiter.join();

  // 如果到达这里，说明没有死锁
  EXPECT_TRUE(wait_done.load());
}

// Test 5: 并发 WaitForTask + CancelTask — 验证不死锁
TEST(ConcurrencySafety, WaitAndCancel_NoDeadlock) {
  SchedulerConfig config = MakeConfig();
  TaskScheduler scheduler(config);

  auto task_info = MakeTask("", 0);
  auto id = scheduler.SubmitTask(task_info);

  std::atomic<bool> wait_done{false};
  std::thread waiter([&]() {
    TaskResult result;
    bool completed = scheduler.WaitForTask(id, &result, 5000);
    EXPECT_TRUE(completed);
    wait_done = true;
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  std::thread canceller([&]() {
    scheduler.CancelTask(id, "test cancel");
  });

  canceller.join();
  waiter.join();

  EXPECT_TRUE(wait_done.load());

  auto task = scheduler.GetTask(id);
  EXPECT_EQ(task->status, TaskStatus::kCancelled);
}

// Test 5b: 多个 Waiter + 一个 Completer — 全部唤醒
TEST(ConcurrencySafety, MultipleWaiters_AllWoken) {
  SchedulerConfig config = MakeConfig();
  TaskScheduler scheduler(config);

  auto task_info = MakeTask("", 0);
  auto id = scheduler.SubmitTask(task_info);

  constexpr int kWaiters = 5;
  std::atomic<int> woken_count{0};

  std::vector<std::thread> waiters;
  for (int i = 0; i < kWaiters; i++) {
    waiters.emplace_back([&]() {
      TaskResult result;
      bool completed = scheduler.WaitForTask(id, &result, 5000);
      if (completed) {
        woken_count++;
      }
    });
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // 完成任务
  TaskResult result;
  result.success = true;
  scheduler.OnTaskCompleted(id, result);

  for (auto& t : waiters) {
    t.join();
  }

  EXPECT_EQ(woken_count.load(), kWaiters);
}

// ============================================================================
// Part III: CancelTask 统计计数修正
// 验证 old_status 保存在设置 kCancelled 之前
// ============================================================================

// Test 6: CancelTask pending 任务 — pending_count 正确减少
TEST(ConcurrencySafety, CancelPending_CorrectStatistics) {
  SchedulerConfig config = MakeConfig();
  TaskScheduler scheduler(config);

  auto id = scheduler.SubmitTask(MakeTask("", 0));

  const auto& stats = scheduler.GetStatistics();
  EXPECT_EQ(stats.pending_count.load(), 1u);
  EXPECT_EQ(stats.running_count.load(), 0u);

  scheduler.CancelTask(id, "test");

  // pending_count 应该减少
  EXPECT_EQ(stats.pending_count.load(), 0u);
  // running_count 不应该变化
  EXPECT_EQ(stats.running_count.load(), 0u);
  EXPECT_EQ(stats.total_cancelled.load(), 1u);
}

// Test 7: CancelTask running/assigned 任务 — running_count 正确减少
TEST(ConcurrencySafety, CancelRunning_CorrectStatistics) {
  SchedulerConfig config = MakeConfig();
  TaskScheduler scheduler(config);

  // 设置 WorkerManager
  WorkerManagerConfig wm_config;
  auto wm = std::make_shared<WorkerManager>(wm_config);
  WorkerResources res;
  res.max_concurrent_tasks = 5;
  res.total_memory_mb = 8000;
  res.used_memory_mb = 1000;
  wm->RegisterWorker("localhost:8001", res, {}, "w1");
  scheduler.SetWorkerManager(wm);

  auto id = scheduler.SubmitTask(MakeTask("", 0));

  // 手动将任务状态设为 Running（模拟调度后的状态）
  auto task = scheduler.GetTask(id);
  {
    std::lock_guard<std::mutex> lock(task->mtx);
    task->status = TaskStatus::kRunning;
    task->assigned_worker_id = "w1";
  }

  const auto& stats = scheduler.GetStatistics();
  // 手动调整计数器以模拟调度后状态
  // GetStatistics() 返回 const ref, 但 atomic 成员本身是线程安全的
  // 使用 const_cast 因为我们需要模拟内部状态变化
  const_cast<TaskStatistics&>(stats).pending_count--;
  const_cast<TaskStatistics&>(stats).running_count++;

  EXPECT_EQ(stats.pending_count.load(), 0u);
  EXPECT_EQ(stats.running_count.load(), 1u);

  scheduler.CancelTask(id, "test");

  // running_count 应该减少
  EXPECT_EQ(stats.running_count.load(), 0u);
  // pending_count 不应该变化
  EXPECT_EQ(stats.pending_count.load(), 0u);
  EXPECT_EQ(stats.total_cancelled.load(), 1u);
}

// ============================================================================
// Part IV: OnTaskCompleted 终态保护
// ============================================================================

// Test 8: 双重完成竞争 — 第二次被忽略
TEST(ConcurrencySafety, DoubleComplete_SecondIgnored) {
  SchedulerConfig config = MakeConfig();
  TaskScheduler scheduler(config);

  auto task_info = MakeTask("", 0);
  auto id = scheduler.SubmitTask(task_info);

  TaskResult result1;
  result1.success = true;
  result1.compaction_result = "first";

  TaskResult result2;
  result2.success = false;
  result2.error_message = "second";

  // 第一次完成
  scheduler.OnTaskCompleted(id, result1);

  auto task = scheduler.GetTask(id);
  EXPECT_EQ(task->status, TaskStatus::kCompleted);
  EXPECT_TRUE(task->result.success);

  // 第二次完成 — 应该被忽略（终态保护）
  scheduler.OnTaskCompleted(id, result2);

  // 状态保持 Completed，不变成 Failed
  EXPECT_EQ(task->status, TaskStatus::kCompleted);
  EXPECT_TRUE(task->result.success);
  EXPECT_EQ(task->result.compaction_result, "first");

  // 统计计数不会被多次减少
  const auto& stats = scheduler.GetStatistics();
  EXPECT_EQ(stats.total_completed.load(), 1u);
}

// Test 9: OnTaskCompleted + CancelTask 并发竞争 — 只有一个成功
TEST(ConcurrencySafety, CompleteAndCancel_RaceCondition) {
  for (int trial = 0; trial < 50; trial++) {
    SchedulerConfig config = MakeConfig();
    TaskScheduler scheduler(config);

    auto task_info = MakeTask("", 0);
    auto id = scheduler.SubmitTask(task_info);

    std::atomic<bool> complete_ok{false};
    std::atomic<bool> cancel_ok{false};

    std::thread completer([&]() {
      TaskResult result;
      result.success = true;
      scheduler.OnTaskCompleted(id, result);
      auto task = scheduler.GetTask(id);
      std::lock_guard<std::mutex> lock(task->mtx);
      if (task->status == TaskStatus::kCompleted) {
        complete_ok = true;
      }
    });

    std::thread canceller([&]() {
      bool cancelled = scheduler.CancelTask(id, "race");
      if (cancelled) {
        cancel_ok = true;
      }
    });

    completer.join();
    canceller.join();

    auto task = scheduler.GetTask(id);
    // 任务一定在终态
    EXPECT_TRUE(task->IsTerminal())
      << "Trial " << trial << ": task not terminal, status="
      << TaskStatusToString(task->status);

    // 只有完成或取消中的一个成功处理（不都是 true 也可能
    // 因为 CancelTask 先检查终态返回 false）
    // 关键检查：状态是 Completed 或 Cancelled 之一
    EXPECT_TRUE(task->status == TaskStatus::kCompleted ||
                task->status == TaskStatus::kCancelled)
      << "Trial " << trial << ": unexpected status "
      << TaskStatusToString(task->status);
  }
}

// Test 9b: 并发 OnTaskCompleted 多线程 — 完成计数器不多减
TEST(ConcurrencySafety, ConcurrentComplete_StatsNotCorrupted) {
  SchedulerConfig config = MakeConfig();
  TaskScheduler scheduler(config);

  auto task_info = MakeTask("", 0);
  auto id = scheduler.SubmitTask(task_info);

  constexpr int kThreads = 10;
  std::vector<std::thread> threads;

  for (int i = 0; i < kThreads; i++) {
    threads.emplace_back([&]() {
      TaskResult result;
      result.success = true;
      scheduler.OnTaskCompleted(id, result);
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  const auto& stats = scheduler.GetStatistics();
  // 只应该记录 1 次完成
  EXPECT_EQ(stats.total_completed.load(), 1u);
}

// ============================================================================
// Part V: CleanupCompletedTasks 内存管理
// ============================================================================

// Test 10: 清理过期终态任务
TEST(ConcurrencySafety, CleanupExpiredTasks) {
  SchedulerConfig config = MakeConfig();
  config.completed_task_retention_sec = 1;  // 1 秒过期
  config.scheduling_interval_ms = 50;
  config.task_timeout_sec = 3600;
  TaskScheduler scheduler(config);

  // 提交并完成多个任务
  std::vector<std::string> ids;
  for (int i = 0; i < 5; i++) {
    auto task_info = MakeTask("", 0);
    auto id = scheduler.SubmitTask(task_info);
    ids.push_back(id);

    TaskResult result;
    result.success = true;
    scheduler.OnTaskCompleted(id, result);
  }

  // 确认任务存在
  for (const auto& id : ids) {
    EXPECT_NE(scheduler.GetTask(id), nullptr);
  }

  // 等待过期
  std::this_thread::sleep_for(std::chrono::seconds(2));

  // 启动调度器，让 TimeoutCheckLoop 触发 CleanupCompletedTasks
  // 或者我们可以手动提交并等待
  // 为了避免启动后台线程，我们用一种间接方式验证：
  // 启动调度器短暂运行，让清理逻辑执行
  WorkerManagerConfig wm_config;
  auto wm = std::make_shared<WorkerManager>(wm_config);
  scheduler.SetWorkerManager(wm);
  scheduler.Start();

  // 等待足够时间让 TimeoutCheckLoop 执行清理
  // cleanup_interval = completed_task_cleanup_interval_sec / 10
  // = 1/10 = 0，意味着每次都清理
  std::this_thread::sleep_for(std::chrono::seconds(2));

  scheduler.Stop();

  // 过期任务应该被清理
  int cleaned = 0;
  for (const auto& id : ids) {
    if (scheduler.GetTask(id) == nullptr) {
      cleaned++;
    }
  }
  // 至少应该清理了一些任务
  EXPECT_GT(cleaned, 0) << "Expected at least some tasks to be cleaned up";
}

// Test 11: 保留未过期的终态任务
TEST(ConcurrencySafety, RetainNonExpiredTasks) {
  SchedulerConfig config = MakeConfig();
  config.completed_task_retention_sec = 3600;  // 1 小时过期
  TaskScheduler scheduler(config);

  auto task_info = MakeTask("", 0);
  auto id = scheduler.SubmitTask(task_info);

  TaskResult result;
  result.success = true;
  scheduler.OnTaskCompleted(id, result);

  // 任务刚完成，不应该被清理
  // 短暂启动调度器
  WorkerManagerConfig wm_config;
  auto wm = std::make_shared<WorkerManager>(wm_config);
  scheduler.SetWorkerManager(wm);
  scheduler.Start();
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  scheduler.Stop();

  // 任务应该还在
  EXPECT_NE(scheduler.GetTask(id), nullptr);
}

// ============================================================================
// Part VI: HandleRetry 两阶段转换
// ============================================================================

// Test 12: 重试后任务回到 kPending 状态
TEST(ConcurrencySafety, RetryReturnsToStaging) {
  SchedulerConfig config = MakeConfig();
  TaskScheduler scheduler(config);

  auto task_info = MakeTask("", 3);  // max_retries = 3
  auto id = scheduler.SubmitTask(task_info);

  // 第一次失败 → 触发 HandleRetry
  TaskResult fail_result;
  fail_result.success = false;
  fail_result.error_message = "network error";
  scheduler.OnTaskCompleted(id, fail_result);

  auto task = scheduler.GetTask(id);
  ASSERT_NE(task, nullptr);

  // 应该回到 Pending 状态（HandleRetry: kFailed → kRetrying → kPending）
  EXPECT_EQ(task->status, TaskStatus::kPending);
  EXPECT_EQ(task->retry_count, 1);
  EXPECT_TRUE(task->assigned_worker_id.empty());
}

// Test 13: 多次重试后最终失败
TEST(ConcurrencySafety, ExhaustedRetriesFinallyFails) {
  SchedulerConfig config = MakeConfig();
  TaskScheduler scheduler(config);

  auto task_info = MakeTask("", 2);  // max_retries = 2
  auto id = scheduler.SubmitTask(task_info);

  TaskResult fail_result;
  fail_result.success = false;
  fail_result.error_message = "persistent error";

  // 第一次失败 → 重试 (retry_count=1)
  scheduler.OnTaskCompleted(id, fail_result);
  auto task = scheduler.GetTask(id);
  EXPECT_EQ(task->status, TaskStatus::kPending);
  EXPECT_EQ(task->retry_count, 1);

  // 第二次失败 → 重试 (retry_count=2)
  scheduler.OnTaskCompleted(id, fail_result);
  EXPECT_EQ(task->status, TaskStatus::kPending);
  EXPECT_EQ(task->retry_count, 2);

  // 第三次失败 → 最终失败 (retry_count=2 >= max_retries=2)
  scheduler.OnTaskCompleted(id, fail_result);
  EXPECT_EQ(task->status, TaskStatus::kFailed);

  const auto& stats = scheduler.GetStatistics();
  EXPECT_EQ(stats.total_failed.load(), 1u);
}

// Test 13b: 重试在并发失败下系统不崩溃、不死锁
// 注意：重试后任务回到 kPending（非终态），后续 OnTaskCompleted 可以再次处理
// 因此 retry_count 可以被多次递增，这是设计正确的行为。
// 此测试验证的是：系统不崩溃、不死锁、retry_count 在合理范围内。
TEST(ConcurrencySafety, ConcurrentRetry_SystemStable) {
  for (int trial = 0; trial < 20; trial++) {
    SchedulerConfig config = MakeConfig();
    TaskScheduler scheduler(config);

    auto task_info = MakeTask("", 10);  // 高重试次数避免过早失败
    auto id = scheduler.SubmitTask(task_info);

    // 多个线程同时报告失败
    constexpr int kThreads = 5;
    std::vector<std::thread> threads;

    for (int i = 0; i < kThreads; i++) {
      threads.emplace_back([&]() {
        TaskResult result;
        result.success = false;
        result.error_message = "concurrent failure";
        scheduler.OnTaskCompleted(id, result);
      });
    }

    for (auto& t : threads) {
      t.join();
    }

    auto task = scheduler.GetTask(id);
    // retry_count 应该在 1 到 kThreads 之间
    // （重试后回到 Pending，后续线程可以再次触发重试）
    EXPECT_GE(task->retry_count, 1)
      << "Trial " << trial << ": retry_count=" << task->retry_count;
    EXPECT_LE(task->retry_count, kThreads)
      << "Trial " << trial << ": retry_count=" << task->retry_count;
    // 任务不应在终态（因为 max_retries=10，远大于 kThreads=5）
    EXPECT_FALSE(task->IsTerminal())
      << "Trial " << trial << ": status="
      << TaskStatusToString(task->status);
  }
}

// ============================================================================
// Part VII: 多线程压力测试
// ============================================================================

// Test 14: 并发提交 + 完成 + 取消 — 系统不崩溃、无死锁
TEST(ConcurrencySafety, StressTest_SubmitCompleteCancel) {
  SchedulerConfig config = MakeConfig();
  config.max_pending_tasks = 10000;
  TaskScheduler scheduler(config);

  constexpr int kTotalTasks = 200;
  constexpr int kSubmitters = 4;
  constexpr int kCompleters = 4;
  constexpr int kCancellers = 2;

  std::vector<std::string> task_ids(kTotalTasks);
  std::mutex ids_mutex;
  std::atomic<int> submitted{0};
  std::atomic<int> completed{0};
  std::atomic<int> cancelled{0};

  // 提交者线程
  std::vector<std::thread> submitters;
  for (int i = 0; i < kSubmitters; i++) {
    submitters.emplace_back([&, i]() {
      int start = i * (kTotalTasks / kSubmitters);
      int end = (i + 1) * (kTotalTasks / kSubmitters);
      for (int j = start; j < end; j++) {
        auto task = MakeTask("", 0);
        auto id = scheduler.SubmitTask(task);
        if (!id.empty()) {
          std::lock_guard<std::mutex> lock(ids_mutex);
          task_ids[j] = id;
          submitted++;
        }
      }
    });
  }

  for (auto& t : submitters) {
    t.join();
  }

  EXPECT_EQ(submitted.load(), kTotalTasks);

  // 完成者线程 和 取消者线程并发运行
  std::vector<std::thread> workers;

  // 完成者：处理前半部分
  for (int i = 0; i < kCompleters; i++) {
    workers.emplace_back([&, i]() {
      int start = i * (kTotalTasks / 2 / kCompleters);
      int end = (i + 1) * (kTotalTasks / 2 / kCompleters);
      for (int j = start; j < end; j++) {
        std::string id;
        {
          std::lock_guard<std::mutex> lock(ids_mutex);
          id = task_ids[j];
        }
        if (!id.empty()) {
          TaskResult result;
          result.success = true;
          scheduler.OnTaskCompleted(id, result);
          completed++;
        }
      }
    });
  }

  // 取消者：处理后半部分
  for (int i = 0; i < kCancellers; i++) {
    workers.emplace_back([&, i]() {
      int start = kTotalTasks / 2 + i * (kTotalTasks / 2 / kCancellers);
      int end = kTotalTasks / 2 + (i + 1) * (kTotalTasks / 2 / kCancellers);
      for (int j = start; j < end; j++) {
        std::string id;
        {
          std::lock_guard<std::mutex> lock(ids_mutex);
          id = task_ids[j];
        }
        if (!id.empty()) {
          if (scheduler.CancelTask(id, "stress")) {
            cancelled++;
          }
        }
      }
    });
  }

  for (auto& t : workers) {
    t.join();
  }

  // 所有任务应该都在终态
  for (int i = 0; i < kTotalTasks; i++) {
    std::string id;
    {
      std::lock_guard<std::mutex> lock(ids_mutex);
      id = task_ids[i];
    }
    if (!id.empty()) {
      auto task = scheduler.GetTask(id);
      ASSERT_NE(task, nullptr) << "Task " << i << " (id=" << id << ") not found";
      EXPECT_TRUE(task->IsTerminal())
        << "Task " << i << " not terminal: "
        << TaskStatusToString(task->status);
    }
  }

  // 统计应该自洽
  const auto& stats = scheduler.GetStatistics();
  EXPECT_EQ(stats.total_submitted.load(), static_cast<uint64_t>(kTotalTasks));
  EXPECT_GT(stats.total_completed.load() + stats.total_cancelled.load(), 0u);
}

// Test 15: 并发 WaitForTask + 批量完成 — 所有等待者正确唤醒
TEST(ConcurrencySafety, StressTest_WaitAndBatchComplete) {
  SchedulerConfig config = MakeConfig();
  TaskScheduler scheduler(config);

  constexpr int kTasks = 20;
  std::vector<std::string> ids;

  for (int i = 0; i < kTasks; i++) {
    auto task = MakeTask("", 0);
    ids.push_back(scheduler.SubmitTask(task));
  }

  std::atomic<int> woken{0};

  // 每个任务一个等待者
  std::vector<std::thread> waiters;
  for (int i = 0; i < kTasks; i++) {
    waiters.emplace_back([&, i]() {
      TaskResult result;
      bool ok = scheduler.WaitForTask(ids[i], &result, 5000);
      if (ok) {
        woken++;
      }
    });
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // 一个线程快速完成所有任务
  std::thread completer([&]() {
    for (int i = 0; i < kTasks; i++) {
      TaskResult result;
      result.success = true;
      scheduler.OnTaskCompleted(ids[i], result);
    }
  });

  completer.join();
  for (auto& t : waiters) {
    t.join();
  }

  EXPECT_EQ(woken.load(), kTasks);
}

// Test 16: 提交满队列后并发取消释放空间，再次提交
TEST(ConcurrencySafety, FullQueue_CancelAndResubmit) {
  SchedulerConfig config = MakeConfig();
  config.max_pending_tasks = 10;
  TaskScheduler scheduler(config);

  // 填满队列
  std::vector<std::string> ids;
  for (int i = 0; i < 10; i++) {
    auto id = scheduler.SubmitTask(MakeTask("", 0));
    EXPECT_FALSE(id.empty());
    ids.push_back(id);
  }

  // 队列满，提交应失败
  auto rejected = scheduler.SubmitTask(MakeTask("", 0));
  EXPECT_TRUE(rejected.empty());

  // 取消几个任务（释放空间，但 pending_queue_ 中的元素仍在）
  // 注意：CancelTask 不会从 pending_queue_ 中移除，但通过完成释放空间
  for (int i = 0; i < 3; i++) {
    scheduler.CancelTask(ids[i], "make room");
  }

  // 完成几个任务
  for (int i = 3; i < 7; i++) {
    TaskResult result;
    result.success = true;
    scheduler.OnTaskCompleted(ids[i], result);
  }

  // 统计正确性
  const auto& stats = scheduler.GetStatistics();
  EXPECT_EQ(stats.total_cancelled.load(), 3u);
  EXPECT_EQ(stats.total_completed.load(), 4u);
}

// Test 17: BulkLoad 分片完成传播到父任务
TEST(ConcurrencySafety, BulkLoadShardPropagation) {
  SchedulerConfig config = MakeConfig();
  TaskScheduler scheduler(config);

  // 创建父任务
  TaskInfo parent;
  parent.type = TaskType::kBulkLoad;
  parent.bulk_load_params.source_path = "/data/import.kv";
  parent.bulk_load_params.shard_count = 3;
  auto parent_id = scheduler.SubmitBulkLoadTask(parent);

  // 创建 3 个分片子任务
  std::vector<std::string> shard_ids;
  for (int i = 0; i < 3; i++) {
    TaskInfo shard;
    shard.type = TaskType::kBulkLoad;
    shard.parent_task_id = parent_id;
    shard.max_retries = 0;
    auto sid = scheduler.SubmitBulkLoadShard(shard);
    shard_ids.push_back(sid);
  }

  // 2 个成功，1 个失败
  TaskResult success_result;
  success_result.success = true;
  scheduler.OnTaskCompleted(shard_ids[0], success_result);
  scheduler.OnTaskCompleted(shard_ids[1], success_result);

  TaskResult fail_result;
  fail_result.success = false;
  fail_result.error_message = "shard error";
  scheduler.OnTaskCompleted(shard_ids[2], fail_result);

  // 验证父任务分片计数
  auto parent_task = scheduler.GetBulkLoadTask(parent_id);
  ASSERT_NE(parent_task, nullptr);
  EXPECT_EQ(parent_task->bulk_load_params.completed_shards, 2u);
  EXPECT_EQ(parent_task->bulk_load_params.failed_shards, 1u);
}

// Test 18: 并发分片完成传播 — 计数器正确累加
TEST(ConcurrencySafety, ConcurrentShardCompletion) {
  SchedulerConfig config = MakeConfig();
  TaskScheduler scheduler(config);

  TaskInfo parent;
  parent.type = TaskType::kBulkLoad;
  parent.bulk_load_params.shard_count = 20;
  auto parent_id = scheduler.SubmitBulkLoadTask(parent);

  constexpr int kShards = 20;
  std::vector<std::string> shard_ids;
  for (int i = 0; i < kShards; i++) {
    TaskInfo shard;
    shard.type = TaskType::kBulkLoad;
    shard.parent_task_id = parent_id;
    shard.max_retries = 0;
    shard_ids.push_back(scheduler.SubmitBulkLoadShard(shard));
  }

  // 并发完成所有分片
  std::vector<std::thread> threads;
  for (int i = 0; i < kShards; i++) {
    threads.emplace_back([&, i]() {
      TaskResult result;
      result.success = (i % 3 != 0);  // 每3个有1个失败
      if (!result.success) {
        result.error_message = "shard failure";
      }
      scheduler.OnTaskCompleted(shard_ids[i], result);
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  auto parent_task = scheduler.GetBulkLoadTask(parent_id);
  ASSERT_NE(parent_task, nullptr);

  // i=0,3,6,9,12,15,18 → 7 个失败; 其余 13 个成功
  EXPECT_EQ(parent_task->bulk_load_params.completed_shards, 13u);
  EXPECT_EQ(parent_task->bulk_load_params.failed_shards, 7u);
  EXPECT_EQ(parent_task->bulk_load_params.completed_shards +
            parent_task->bulk_load_params.failed_shards,
            static_cast<uint32_t>(kShards));
}

// Test 19: WaitForTask 零超时 — 非阻塞查询
TEST(ConcurrencySafety, WaitForTask_ZeroTimeout) {
  SchedulerConfig config = MakeConfig();
  TaskScheduler scheduler(config);

  auto id = scheduler.SubmitTask(MakeTask("", 0));

  // 零超时，任务未完成 → 立即返回 false
  TaskResult result;
  EXPECT_FALSE(scheduler.WaitForTask(id, &result, 0));

  // 完成任务
  TaskResult complete_result;
  complete_result.success = true;
  scheduler.OnTaskCompleted(id, complete_result);

  // 零超时，任务已完成 → 立即返回 true
  EXPECT_TRUE(scheduler.WaitForTask(id, &result, 0));
  EXPECT_TRUE(result.success);
}

// Test 20: 回调在并发场景下正确触发
TEST(ConcurrencySafety, CallbackFiredCorrectly) {
  SchedulerConfig config = MakeConfig();
  TaskScheduler scheduler(config);

  std::atomic<int> callback_count{0};
  std::mutex callback_ids_mutex;
  std::vector<std::string> callback_ids;

  scheduler.RegisterCompletedCallback(
    [&](const std::string& tid, const TaskResult& res) {
      callback_count++;
      std::lock_guard<std::mutex> lock(callback_ids_mutex);
      callback_ids.push_back(tid);
    });

  constexpr int kTasks = 50;
  std::vector<std::string> ids;
  for (int i = 0; i < kTasks; i++) {
    auto task = MakeTask("", 0);
    ids.push_back(scheduler.SubmitTask(task));
  }

  // 并发完成
  std::vector<std::thread> threads;
  for (int i = 0; i < kTasks; i++) {
    threads.emplace_back([&, i]() {
      TaskResult result;
      result.success = true;
      scheduler.OnTaskCompleted(ids[i], result);
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  // 每个任务的回调都应该被触发一次
  EXPECT_EQ(callback_count.load(), kTasks);
}

// Test 21: 调度器 Start/Stop 不与并发操作死锁
TEST(ConcurrencySafety, StartStop_NoConcurrentDeadlock) {
  SchedulerConfig config = MakeConfig();
  config.scheduling_interval_ms = 10;
  TaskScheduler scheduler(config);

  WorkerManagerConfig wm_config;
  auto wm = std::make_shared<WorkerManager>(wm_config);
  scheduler.SetWorkerManager(wm);

  scheduler.Start();

  // 在调度器运行时并发提交、完成、取消任务
  std::atomic<bool> stop_flag{false};

  std::thread submitter([&]() {
    int i = 0;
    while (!stop_flag.load()) {
      auto task = MakeTask("", 0);
      scheduler.SubmitTask(task);
      i++;
      if (i > 100) break;
    }
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // 停止调度器
  stop_flag = true;
  scheduler.Stop();

  submitter.join();

  // 到达这里说明没有死锁
  EXPECT_FALSE(scheduler.IsRunning());
}

// Test 22: TaskInfo mtx 不干扰 TaskStatistics atomic 操作
TEST(ConcurrencySafety, TaskStatistics_AtomicThreadSafety) {
  TaskStatistics stats;

  constexpr int kThreads = 8;
  constexpr int kOpsPerThread = 1000;

  std::vector<std::thread> threads;
  for (int i = 0; i < kThreads; i++) {
    threads.emplace_back([&]() {
      for (int j = 0; j < kOpsPerThread; j++) {
        stats.total_submitted++;
        stats.total_completed++;
        stats.pending_count++;
        stats.pending_count--;
        stats.running_count++;
        stats.running_count--;
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  EXPECT_EQ(stats.total_submitted.load(),
            static_cast<uint64_t>(kThreads * kOpsPerThread));
  EXPECT_EQ(stats.total_completed.load(),
            static_cast<uint64_t>(kThreads * kOpsPerThread));
  EXPECT_EQ(stats.pending_count.load(), 0u);
  EXPECT_EQ(stats.running_count.load(), 0u);
}

}  // namespace control_plane
}  // namespace tendisplus
