// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

// Unit tests for WorkerManager (worker_manager.h/cc)
// Tests: registration, heartbeat, selection, assignment,
//        status transitions, statistics
// Note: gRPC push methods (DistributeJobToCSA etc.) are NOT tested here
//       as they require real gRPC connections. Only local logic is tested.

#include "gtest/gtest.h"
#include "worker_manager.h"

#include <thread>
#include <vector>

namespace tendisplus {
namespace control_plane {

// ============================================================================
// 枚举转换测试
// ============================================================================
TEST(WorkerManager, WorkerStatusToString) {
  EXPECT_STREQ(WorkerStatusToString(WorkerStatus::kOnline), "Online");
  EXPECT_STREQ(WorkerStatusToString(WorkerStatus::kOffline), "Offline");
  EXPECT_STREQ(WorkerStatusToString(WorkerStatus::kBusy), "Busy");
  EXPECT_STREQ(WorkerStatusToString(WorkerStatus::kDraining), "Draining");
  EXPECT_STREQ(
    WorkerStatusToString(WorkerStatus::kMaintenance), "Maintenance");
  EXPECT_STREQ(WorkerStatusToString(WorkerStatus::kUnknown), "Unknown");
}

// ============================================================================
// WorkerResources 计算方法测试
// ============================================================================
TEST(WorkerResources, AvailableSlots) {
  WorkerResources res;
  res.max_concurrent_tasks = 5;

  res.active_tasks = 0;
  EXPECT_EQ(res.AvailableSlots(), 5u);

  res.active_tasks = 3;
  EXPECT_EQ(res.AvailableSlots(), 2u);

  res.active_tasks = 5;
  EXPECT_EQ(res.AvailableSlots(), 0u);

  // 超出情况（理论上不应发生，但不崩溃）
  res.active_tasks = 10;
  EXPECT_EQ(res.AvailableSlots(), 0u);
}

TEST(WorkerResources, LoadRatio) {
  WorkerResources res;
  res.max_concurrent_tasks = 10;

  res.active_tasks = 0;
  EXPECT_DOUBLE_EQ(res.LoadRatio(), 0.0);

  res.active_tasks = 5;
  EXPECT_DOUBLE_EQ(res.LoadRatio(), 0.5);

  res.active_tasks = 10;
  EXPECT_DOUBLE_EQ(res.LoadRatio(), 1.0);

  // max = 0 → 100% 负载
  res.max_concurrent_tasks = 0;
  EXPECT_DOUBLE_EQ(res.LoadRatio(), 1.0);
}

TEST(WorkerResources, MemoryFreeRatio) {
  WorkerResources res;
  res.total_memory_mb = 1000;

  res.used_memory_mb = 0;
  EXPECT_DOUBLE_EQ(res.MemoryFreeRatio(), 1.0);

  res.used_memory_mb = 300;
  EXPECT_DOUBLE_EQ(res.MemoryFreeRatio(), 0.7);

  res.used_memory_mb = 1000;
  EXPECT_DOUBLE_EQ(res.MemoryFreeRatio(), 0.0);

  // used > total (保护)
  res.used_memory_mb = 1500;
  EXPECT_DOUBLE_EQ(res.MemoryFreeRatio(), 0.0);

  // total = 0
  res.total_memory_mb = 0;
  EXPECT_DOUBLE_EQ(res.MemoryFreeRatio(), 0.0);
}

// ============================================================================
// WorkerInfo 辅助方法测试
// ============================================================================
TEST(WorkerInfo, IsAvailable) {
  WorkerInfo worker;
  worker.resources.max_concurrent_tasks = 5;
  worker.resources.active_tasks = 0;

  // Online + 有空闲槽位 → 可用
  worker.status = WorkerStatus::kOnline;
  EXPECT_TRUE(worker.IsAvailable());

  // Busy → 不可用
  worker.status = WorkerStatus::kBusy;
  EXPECT_FALSE(worker.IsAvailable());

  // Offline → 不可用
  worker.status = WorkerStatus::kOffline;
  EXPECT_FALSE(worker.IsAvailable());

  // Online 但没有空闲槽位 → 不可用
  worker.status = WorkerStatus::kOnline;
  worker.resources.active_tasks = 5;
  EXPECT_FALSE(worker.IsAvailable());
}

TEST(WorkerInfo, GetMemoryFreeRatio) {
  WorkerInfo worker;
  worker.resources.total_memory_mb = 2000;
  worker.resources.used_memory_mb = 500;
  EXPECT_DOUBLE_EQ(worker.GetMemoryFreeRatio(), 0.75);
}

// ============================================================================
// Worker 注册与注销
// ============================================================================
TEST(WorkerManager, RegisterAndGet) {
  WorkerManagerConfig config;
  WorkerManager mgr(config);

  WorkerResources res;
  res.total_cpu_cores = 8;
  res.total_memory_mb = 16384;
  res.max_concurrent_tasks = 5;

  auto id = mgr.RegisterWorker("localhost:8001", res);
  EXPECT_FALSE(id.empty());

  auto worker = mgr.GetWorker(id);
  ASSERT_NE(worker, nullptr);
  EXPECT_EQ(worker->address, "localhost:8001");
  EXPECT_EQ(worker->status, WorkerStatus::kOnline);
  EXPECT_EQ(worker->resources.total_cpu_cores, 8u);
  EXPECT_EQ(worker->resources.max_concurrent_tasks, 5u);

  EXPECT_EQ(mgr.GetWorkerCount(), 1u);
  EXPECT_EQ(mgr.GetOnlineWorkerCount(), 1u);
}

TEST(WorkerManager, RegisterWithRequestedId) {
  WorkerManagerConfig config;
  WorkerManager mgr(config);

  WorkerResources res;
  res.max_concurrent_tasks = 3;

  auto id = mgr.RegisterWorker("host:9001", res, {}, "my-worker-1");
  EXPECT_EQ(id, "my-worker-1");

  auto worker = mgr.GetWorker("my-worker-1");
  ASSERT_NE(worker, nullptr);
  EXPECT_EQ(worker->address, "host:9001");
}

TEST(WorkerManager, ReRegisterUpdatesExisting) {
  WorkerManagerConfig config;
  WorkerManager mgr(config);

  WorkerResources res;
  res.max_concurrent_tasks = 3;

  mgr.RegisterWorker("host:9001", res, {}, "w1");
  EXPECT_EQ(mgr.GetWorkerCount(), 1u);

  // 重新注册同一个 ID
  WorkerResources new_res;
  new_res.max_concurrent_tasks = 10;
  mgr.RegisterWorker("host:9002", new_res, {}, "w1");

  // 不应增加 Worker 数
  EXPECT_EQ(mgr.GetWorkerCount(), 1u);

  auto worker = mgr.GetWorker("w1");
  EXPECT_EQ(worker->address, "host:9002");
  EXPECT_EQ(worker->resources.max_concurrent_tasks, 10u);
}

TEST(WorkerManager, Unregister) {
  WorkerManagerConfig config;
  WorkerManager mgr(config);

  WorkerResources res;
  mgr.RegisterWorker("host:8001", res, {}, "w1");
  EXPECT_EQ(mgr.GetWorkerCount(), 1u);

  bool removed = mgr.UnregisterWorker("w1", "shutdown");
  EXPECT_TRUE(removed);
  EXPECT_EQ(mgr.GetWorkerCount(), 0u);
  EXPECT_EQ(mgr.GetWorker("w1"), nullptr);

  // 再次注销 → false
  EXPECT_FALSE(mgr.UnregisterWorker("w1"));
}

// ============================================================================
// 心跳处理
// ============================================================================
TEST(WorkerManager, Heartbeat) {
  WorkerManagerConfig config;
  WorkerManager mgr(config);

  WorkerResources res;
  res.max_concurrent_tasks = 5;
  res.active_tasks = 2;
  mgr.RegisterWorker("host:8001", res, {}, "w1");

  // 发送心跳并更新资源
  WorkerResources updated_res;
  updated_res.max_concurrent_tasks = 5;
  updated_res.active_tasks = 4;

  auto tasks_to_cancel = mgr.ProcessHeartbeat(
    "w1", updated_res, {"task-1", "task-2", "task-3", "task-4"});

  auto worker = mgr.GetWorker("w1");
  EXPECT_EQ(worker->resources.active_tasks, 4u);

  // 未知 Worker 的心跳
  auto cancel2 = mgr.ProcessHeartbeat("unknown", updated_res, {});
  EXPECT_TRUE(cancel2.empty());
}

TEST(WorkerManager, HeartbeatBusyTransition) {
  WorkerManagerConfig config;
  WorkerManager mgr(config);

  WorkerResources res;
  res.max_concurrent_tasks = 2;
  res.active_tasks = 0;
  mgr.RegisterWorker("host:8001", res, {}, "w1");

  // 满载 → Busy
  WorkerResources full_res;
  full_res.max_concurrent_tasks = 2;
  full_res.active_tasks = 2;
  mgr.ProcessHeartbeat("w1", full_res, {"t1", "t2"});

  auto worker = mgr.GetWorker("w1");
  EXPECT_EQ(worker->status, WorkerStatus::kBusy);

  // 释放 → Online
  WorkerResources free_res;
  free_res.max_concurrent_tasks = 2;
  free_res.active_tasks = 1;
  mgr.ProcessHeartbeat("w1", free_res, {"t1"});

  worker = mgr.GetWorker("w1");
  EXPECT_EQ(worker->status, WorkerStatus::kOnline);
}

// ============================================================================
// Worker 选择（最小负载策略）
// ============================================================================
TEST(WorkerManager, SelectBestWorker) {
  WorkerManagerConfig config;
  WorkerManager mgr(config);

  // Worker 1: 负载 80%
  WorkerResources res1;
  res1.max_concurrent_tasks = 10;
  res1.active_tasks = 8;
  mgr.RegisterWorker("host:8001", res1, {}, "w1");

  // Worker 2: 负载 20%（最佳选择）
  WorkerResources res2;
  res2.max_concurrent_tasks = 10;
  res2.active_tasks = 2;
  mgr.RegisterWorker("host:8002", res2, {}, "w2");

  // Worker 3: 负载 50%
  WorkerResources res3;
  res3.max_concurrent_tasks = 10;
  res3.active_tasks = 5;
  mgr.RegisterWorker("host:8003", res3, {}, "w3");

  auto best = mgr.SelectBestWorker();
  ASSERT_NE(best, nullptr);
  EXPECT_EQ(best->worker_id, "w2");  // 最小负载
}

TEST(WorkerManager, SelectBestWorkerNoAvailable) {
  WorkerManagerConfig config;
  WorkerManager mgr(config);

  // 无 Worker → nullptr
  EXPECT_EQ(mgr.SelectBestWorker(), nullptr);

  // 所有 Worker 满载
  WorkerResources full_res;
  full_res.max_concurrent_tasks = 1;
  full_res.active_tasks = 1;
  mgr.RegisterWorker("host:8001", full_res, {}, "w1");

  EXPECT_EQ(mgr.SelectBestWorker(), nullptr);
}

// ============================================================================
// 任务分配与释放
// ============================================================================
TEST(WorkerManager, AssignAndReleaseTask) {
  WorkerManagerConfig config;
  WorkerManager mgr(config);

  WorkerResources res;
  res.max_concurrent_tasks = 2;
  res.active_tasks = 0;
  mgr.RegisterWorker("host:8001", res, {}, "w1");

  // 分配任务
  EXPECT_TRUE(mgr.AssignTask("w1", "task-1"));
  auto worker = mgr.GetWorker("w1");
  EXPECT_EQ(worker->resources.active_tasks, 1u);
  EXPECT_EQ(worker->active_task_ids.size(), 1u);

  // 分配第二个任务（满载 → Busy）
  EXPECT_TRUE(mgr.AssignTask("w1", "task-2"));
  worker = mgr.GetWorker("w1");
  EXPECT_EQ(worker->resources.active_tasks, 2u);
  EXPECT_EQ(worker->status, WorkerStatus::kBusy);

  // 分配第三个 → 失败（满载不可用）
  EXPECT_FALSE(mgr.AssignTask("w1", "task-3"));

  // 释放任务（成功）
  EXPECT_TRUE(mgr.ReleaseTask("w1", "task-1", true));
  worker = mgr.GetWorker("w1");
  EXPECT_EQ(worker->resources.active_tasks, 1u);
  EXPECT_EQ(worker->status, WorkerStatus::kOnline);
  EXPECT_EQ(worker->total_completed, 1u);
  EXPECT_EQ(worker->total_failed, 0u);

  // 释放任务（失败）
  EXPECT_TRUE(mgr.ReleaseTask("w1", "task-2", false));
  worker = mgr.GetWorker("w1");
  EXPECT_EQ(worker->total_failed, 1u);

  // 分配给不存在的 Worker → 失败
  EXPECT_FALSE(mgr.AssignTask("nonexistent", "task-x"));
  EXPECT_FALSE(mgr.ReleaseTask("nonexistent", "task-x", true));
}

// ============================================================================
// Worker 查询
// ============================================================================
TEST(WorkerManager, GetAllWorkers) {
  WorkerManagerConfig config;
  WorkerManager mgr(config);

  WorkerResources res;
  mgr.RegisterWorker("host:8001", res, {}, "w1");
  mgr.RegisterWorker("host:8002", res, {}, "w2");
  mgr.RegisterWorker("host:8003", res, {}, "w3");

  auto all = mgr.GetAllWorkers();
  EXPECT_EQ(all.size(), 3u);
}

TEST(WorkerManager, GetAvailableWorkers) {
  WorkerManagerConfig config;
  WorkerManager mgr(config);

  // w1: Online + 有空位
  WorkerResources res1;
  res1.max_concurrent_tasks = 5;
  res1.active_tasks = 0;
  mgr.RegisterWorker("host:8001", res1, {}, "w1");

  // w2: Online 但满载
  WorkerResources res2;
  res2.max_concurrent_tasks = 1;
  res2.active_tasks = 1;
  mgr.RegisterWorker("host:8002", res2, {}, "w2");

  auto available = mgr.GetAvailableWorkers();
  EXPECT_EQ(available.size(), 1u);
  EXPECT_EQ(available[0]->worker_id, "w1");
}

TEST(WorkerManager, GetWorkersByStatus) {
  WorkerManagerConfig config;
  WorkerManager mgr(config);

  WorkerResources res;
  res.max_concurrent_tasks = 5;
  mgr.RegisterWorker("host:8001", res, {}, "w1");
  mgr.RegisterWorker("host:8002", res, {}, "w2");

  auto online = mgr.GetWorkersByStatus(WorkerStatus::kOnline);
  EXPECT_EQ(online.size(), 2u);

  auto offline = mgr.GetWorkersByStatus(WorkerStatus::kOffline);
  EXPECT_EQ(offline.size(), 0u);
}

// ============================================================================
// 统计信息
// ============================================================================
TEST(WorkerManager, Statistics) {
  WorkerManagerConfig config;
  WorkerManager mgr(config);

  EXPECT_EQ(mgr.GetWorkerCount(), 0u);
  EXPECT_EQ(mgr.GetOnlineWorkerCount(), 0u);
  EXPECT_EQ(mgr.GetTotalAvailableSlots(), 0u);

  WorkerResources res;
  res.max_concurrent_tasks = 5;
  res.active_tasks = 2;
  mgr.RegisterWorker("host:8001", res, {}, "w1");

  res.active_tasks = 1;
  mgr.RegisterWorker("host:8002", res, {}, "w2");

  EXPECT_EQ(mgr.GetWorkerCount(), 2u);
  EXPECT_EQ(mgr.GetOnlineWorkerCount(), 2u);
  // w1: 5-2=3, w2: 5-1=4, total=7
  EXPECT_EQ(mgr.GetTotalAvailableSlots(), 7u);
}

// ============================================================================
// 事件回调
// ============================================================================
TEST(WorkerManager, EventCallback) {
  WorkerManagerConfig config;
  WorkerManager mgr(config);

  std::vector<WorkerEvent> events;
  mgr.RegisterEventCallback(
    [&events](const WorkerInfo& worker, WorkerEvent event) {
      events.push_back(event);
    });

  WorkerResources res;
  mgr.RegisterWorker("host:8001", res, {}, "w1");

  // 应该触发 kRegistered 事件
  EXPECT_EQ(events.size(), 1u);
  EXPECT_EQ(events[0], WorkerEvent::kRegistered);

  // 注销应触发 kUnregistered 事件
  mgr.UnregisterWorker("w1");
  EXPECT_EQ(events.size(), 2u);
  EXPECT_EQ(events[1], WorkerEvent::kUnregistered);
}

// ============================================================================
// 默认值验证
// ============================================================================
TEST(WorkerManager, DefaultConfig) {
  WorkerManagerConfig config;
  EXPECT_EQ(config.heartbeat_timeout_sec, 30);
  EXPECT_EQ(config.heartbeat_interval_sec, 10);
  EXPECT_EQ(config.health_check_interval_sec, 5);
  EXPECT_EQ(config.max_consecutive_failures, 3);
  EXPECT_EQ(config.default_max_concurrent, 5u);
}

TEST(WorkerManager, DefaultMaxConcurrentApplied) {
  WorkerManagerConfig config;
  config.default_max_concurrent = 8;
  WorkerManager mgr(config);

  WorkerResources res;
  res.max_concurrent_tasks = 0;  // 不指定，使用默认值
  mgr.RegisterWorker("host:8001", res, {}, "w1");

  auto worker = mgr.GetWorker("w1");
  EXPECT_EQ(worker->resources.max_concurrent_tasks, 8u);
}

// ============================================================================
// WorkerResources / WorkerInfo ToString
// ============================================================================
TEST(WorkerManager, ToString) {
  WorkerResources res;
  res.total_cpu_cores = 4;
  res.total_memory_mb = 8192;
  res.max_concurrent_tasks = 5;
  res.active_tasks = 2;
  auto str = res.ToString();
  EXPECT_NE(str.find("tasks=2/5"), std::string::npos);
  EXPECT_NE(str.find("cpu=4"), std::string::npos);

  WorkerInfo worker;
  worker.worker_id = "test-worker";
  worker.address = "host:8001";
  worker.status = WorkerStatus::kOnline;
  worker.resources = res;
  auto wstr = worker.ToString();
  EXPECT_NE(wstr.find("test-worker"), std::string::npos);
  EXPECT_NE(wstr.find("Online"), std::string::npos);
}

// ============================================================================
// Labels 标签支持
// ============================================================================
TEST(WorkerManager, WorkerLabels) {
  WorkerManagerConfig config;
  WorkerManager mgr(config);

  WorkerResources res;
  std::map<std::string, std::string> labels = {
    {"zone", "us-east-1"},
    {"gpu", "nvidia-a100"}
  };

  mgr.RegisterWorker("host:8001", res, labels, "w1");

  auto worker = mgr.GetWorker("w1");
  EXPECT_EQ(worker->labels.size(), 2u);
  EXPECT_EQ(worker->labels["zone"], "us-east-1");
  EXPECT_EQ(worker->labels["gpu"], "nvidia-a100");
}

}  // namespace control_plane
}  // namespace tendisplus
