// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

// Integration tests for ControlPlane (control_plane.h/cc)
// Tests: complete lifecycle without gRPC server,
//        component access, cluster status, task statistics,
//        Prometheus metrics generation, WorkerManager + TaskScheduler interaction

#include "gtest/gtest.h"
#include "control_plane.h"

#include <chrono>
#include <thread>

namespace tendisplus {
namespace control_plane {

// ============================================================================
// 辅助函数
// ============================================================================
static ControlPlaneConfig MakeTestConfig() {
  ControlPlaneConfig config;
  config.scheduler_config.scheduling_interval_ms = 50;
  config.scheduler_config.max_pending_tasks = 100;
  config.scheduler_config.max_retries = 2;
  config.worker_manager_config.heartbeat_timeout_sec = 5;
  config.worker_manager_config.health_check_interval_sec = 1;
  return config;
}

// ============================================================================
// 基本构造和组件访问
// ============================================================================
TEST(ControlPlane, Construction) {
  auto config = MakeTestConfig();
  ControlPlane cp(config);

  // 组件返回引用，通过调用方法验证可用性
  auto& scheduler = cp.GetScheduler();
  auto& worker_mgr = cp.GetWorkerManager();
  // 验证返回的引用可正常使用（不崩溃即可）
  (void)scheduler;
  EXPECT_EQ(worker_mgr.GetOnlineWorkerCount(), 0u);
}

// ============================================================================
// Worker 注册与集群状态
// ============================================================================
TEST(ControlPlane, RegisterWorkerAndClusterStatus) {
  auto config = MakeTestConfig();
  ControlPlane cp(config);

  // 注册 Worker
  WorkerResources res;
  res.max_concurrent_tasks = 5;
  res.total_cpu_cores = 8;
  res.total_memory_mb = 16384;
  auto wid = cp.RegisterWorker("localhost:8001", res);
  EXPECT_FALSE(wid.empty());

  // 集群状态
  auto status = cp.GetClusterStatus();
  EXPECT_GE(status.total_workers, 1u);
  EXPECT_GE(status.online_workers, 1u);
}

// ============================================================================
// 提交 Compaction 任务并查询
// ============================================================================
TEST(ControlPlane, SubmitAndQueryCompactionTask) {
  auto config = MakeTestConfig();
  ControlPlane cp(config);

  auto task_id = cp.SubmitCompactionTask(
      "tendisplus-1",       // source_node_id
      "db0",                // db_name
      0,                    // store_id
      1001,                 // job_id
      "compaction_input_1", // compaction_input
      "nfs://localhost/shared/test",  // shared_fs_uri
      TaskPriority::kNormal,
      0);
  EXPECT_FALSE(task_id.empty());

  // 查询任务
  auto queried = cp.QueryTask(task_id);
  ASSERT_NE(queried, nullptr);
  EXPECT_EQ(queried->type, TaskType::kCompaction);
  EXPECT_EQ(queried->source_node_id, "tendisplus-1");
  EXPECT_EQ(queried->status, TaskStatus::kPending);
}

// ============================================================================
// 取消任务
// ============================================================================
TEST(ControlPlane, CancelTask) {
  auto config = MakeTestConfig();
  ControlPlane cp(config);

  auto task_id = cp.SubmitCompactionTask(
      "node-cancel", "db0", 0, 2001, "input", "nfs://localhost/shared");

  bool cancelled = cp.CancelTask(task_id, "test cancel");
  EXPECT_TRUE(cancelled);

  auto queried = cp.QueryTask(task_id);
  EXPECT_EQ(queried->status, TaskStatus::kCancelled);
}

// ============================================================================
// 任务统计
// ============================================================================
TEST(ControlPlane, TaskStatistics) {
  auto config = MakeTestConfig();
  ControlPlane cp(config);

  // 提交 3 个任务
  for (int i = 0; i < 3; i++) {
    cp.SubmitCompactionTask(
        "node-stats", "db0", 0, 3000 + i, "input", "nfs://localhost/shared");
  }

  const auto& stats = cp.GetTaskStatistics();
  EXPECT_EQ(stats.total_submitted.load(), 3u);
}

// ============================================================================
// 获取 Workers 列表
// ============================================================================
TEST(ControlPlane, GetWorkers) {
  auto config = MakeTestConfig();
  ControlPlane cp(config);

  WorkerResources res;
  res.max_concurrent_tasks = 5;
  cp.RegisterWorker("host1:8001", res);
  cp.RegisterWorker("host2:8002", res);

  auto workers = cp.GetWorkers();
  EXPECT_EQ(workers.size(), 2u);
}

// ============================================================================
// Worker 心跳与注销
// ============================================================================
TEST(ControlPlane, WorkerHeartbeatAndUnregister) {
  auto config = MakeTestConfig();
  ControlPlane cp(config);

  WorkerResources res;
  res.max_concurrent_tasks = 3;
  auto wid = cp.RegisterWorker("host1:8001", res, {}, "w1");

  // 心跳
  WorkerResources updated_res;
  updated_res.max_concurrent_tasks = 3;
  updated_res.active_tasks = 1;
  cp.ProcessWorkerHeartbeat("w1", updated_res, {"task-1"});

  auto w = cp.GetWorkerManager().GetWorker("w1");
  ASSERT_NE(w, nullptr);
  EXPECT_EQ(w->resources.active_tasks, 1u);

  // 注销
  cp.UnregisterWorker("w1");
  EXPECT_EQ(cp.GetWorkerManager().GetWorker("w1"), nullptr);
}

// ============================================================================
// Prometheus 指标生成
// ============================================================================
TEST(ControlPlane, GeneratePrometheusMetrics) {
  auto config = MakeTestConfig();
  ControlPlane cp(config);

  // 注册 Worker 以产生数据
  WorkerResources res;
  res.max_concurrent_tasks = 5;
  res.total_memory_mb = 8000;
  res.used_memory_mb = 2000;
  res.total_disk_mb = 100000;
  res.used_disk_mb = 30000;
  cp.RegisterWorker("host1:8001", res, {}, "w1");

  // 提交任务以产生统计
  cp.SubmitCompactionTask(
      "node-prom", "db0", 0, 5001, "input", "nfs://localhost/shared");

  auto metrics = cp.GeneratePrometheusMetrics();
  EXPECT_FALSE(metrics.empty());

  // 检查关键指标存在
  EXPECT_NE(metrics.find("caas_lsm_workers_total"), std::string::npos);
  EXPECT_NE(metrics.find("caas_lsm_tasks_submitted_total"), std::string::npos);
  EXPECT_NE(metrics.find("caas_lsm_tasks_pending"), std::string::npos);
}

// ============================================================================
// Bulk Load 任务提交
// ============================================================================
TEST(ControlPlane, SubmitBulkLoadTask) {
  auto config = MakeTestConfig();
  ControlPlane cp(config);

  BulkLoadTaskParams params;
  params.source_path = "/data/import.kv";
  params.source_type = DataSourceType::kKVFile;
  params.shard_count = 4;
  params.target_store_id = 1;

  auto task_id = cp.SubmitBulkLoadTask(
      "tendisplus-2",  // source_node_id
      "db0",           // db_name
      params,
      TaskPriority::kNormal);
  EXPECT_FALSE(task_id.empty());
}

// ============================================================================
// 观测平面组件访问
// ============================================================================
TEST(ControlPlane, ObservationComponents) {
  auto config = MakeTestConfig();
  ControlPlane cp(config);

  // AlertManager, MetricsCollector, TaskTracer 返回引用
  // 不调用 Start() 时这些组件可能未初始化（unique_ptr 为空），
  // 解引用空指针会崩溃。因此只在 Start() 后测试。
  // 这里仅验证 ControlPlane 构造不崩溃。
  EXPECT_FALSE(cp.IsRunning());
}

// ============================================================================
// 多任务并发提交
// ============================================================================
TEST(ControlPlane, ConcurrentSubmission) {
  auto config = MakeTestConfig();
  config.scheduler_config.max_pending_tasks = 1000;
  ControlPlane cp(config);

  const int kThreads = 4;
  const int kTasksPerThread = 50;

  std::vector<std::thread> threads;
  std::vector<std::string> all_ids;
  std::mutex ids_mutex;

  for (int t = 0; t < kThreads; t++) {
    threads.emplace_back([&cp, &all_ids, &ids_mutex, t]() {
      for (int i = 0; i < kTasksPerThread; i++) {
        auto id = cp.SubmitCompactionTask(
            "node-" + std::to_string(t),   // source_node_id
            "db0",                          // db_name
            0,                              // store_id
            static_cast<uint64_t>(t * kTasksPerThread + i),  // job_id
            "concurrent_input",             // compaction_input
            "nfs://localhost/shared",       // shared_fs_uri
            TaskPriority::kNormal,
            0);
        if (!id.empty()) {
          std::lock_guard<std::mutex> lock(ids_mutex);
          all_ids.push_back(id);
        }
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  EXPECT_EQ(all_ids.size(), static_cast<size_t>(kThreads * kTasksPerThread));

  const auto& stats = cp.GetTaskStatistics();
  EXPECT_EQ(stats.total_submitted.load(),
            static_cast<uint64_t>(kThreads * kTasksPerThread));
}

}  // namespace control_plane
}  // namespace tendisplus
