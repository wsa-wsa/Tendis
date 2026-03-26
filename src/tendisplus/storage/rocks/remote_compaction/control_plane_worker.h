// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

// CSA Worker with Control Plane Integration
// Based on CaaS-LSM architecture

#pragma once

#include <grpc/grpc.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

namespace tendisplus {
namespace remote_compaction {

// ============================================================================
// Worker 配置
// ============================================================================
struct WorkerConfig {
  // Worker 基本配置
  std::string worker_address;              // Worker 自身地址 (host:port)
  std::string worker_id;                   // Worker ID (可选，由控制平面分配)

  // 控制平面配置
  std::string control_plane_address;       // 控制平面地址

  // 资源配置
  uint32_t max_concurrent_tasks = 5;
  uint32_t cpu_cores = 0;                  // 0 = 自动检测
  uint64_t memory_mb = 0;                  // 0 = 自动检测
  uint64_t disk_mb = 0;

  // 心跳配置
  int32_t heartbeat_interval_sec = 10;
  int32_t reconnect_interval_sec = 5;

  // gRPC 配置
  int64_t grpc_max_message_size = 64 * 1024 * 1024;
};

// ============================================================================
// 任务信息（从控制平面获取）
// ============================================================================
struct CompactionTaskInfo {
  std::string task_id;
  std::string db_name;
  uint32_t store_id = 0;
  uint64_t job_id = 0;
  std::string compaction_input;
  std::string shared_fs_uri;
  std::string shared_fs_local_prefix;
  uint32_t timeout_sec = 0;
};

// ============================================================================
// 任务执行结果
// ============================================================================
struct TaskExecutionResult {
  bool success = false;
  std::string compaction_result;
  std::string error_message;
  uint64_t execution_time_ms = 0;
  uint64_t bytes_read = 0;
  uint64_t bytes_written = 0;
};

// ============================================================================
// Bulk Load 分片任务信息
// ============================================================================
struct BulkLoadShardTaskInfo {
  std::string task_id;
  std::string shard_id;
  uint32_t shard_index = 0;

  // 数据源
  int32_t source_type = 0;
  std::string source_path;
  int32_t data_format = 0;

  // Key 范围
  std::string key_range_start;
  std::string key_range_end;
  uint32_t slot_start = 0;
  uint32_t slot_end = 0;

  // SST 配置
  std::string shared_fs_uri;
  std::string sst_output_dir;
  int32_t compression = 3;
  uint64_t target_sst_size = 64 * 1024 * 1024;
  bool generate_binlog = false;
  uint32_t target_store_id = 0;
  std::string target_db_path;

  // 资源控制
  int64_t rate_limit_bytes_per_sec = 0;
  uint32_t timeout_sec = 7200;
};

// ============================================================================
// Bulk Load 执行结果
// ============================================================================
struct BulkLoadShardResult {
  bool success = false;
  std::string error_message;
  uint64_t execution_time_ms = 0;
  uint64_t total_rows_processed = 0;
  uint64_t total_bytes_written = 0;
  uint32_t sst_files_count = 0;

  // SST 文件元数据 (简化: 路径列表)
  struct SSTMeta {
    std::string file_path;
    std::string column_family;
    uint64_t file_size = 0;
    uint64_t num_entries = 0;
  };
  std::vector<SSTMeta> sst_files;
};

// ============================================================================
// Control Plane Worker - CSA Worker 与控制平面集成
// ============================================================================
class ControlPlaneWorker {
 public:
  explicit ControlPlaneWorker(const WorkerConfig& config);
  ~ControlPlaneWorker();

  // 禁止拷贝
  ControlPlaneWorker(const ControlPlaneWorker&) = delete;
  ControlPlaneWorker& operator=(const ControlPlaneWorker&) = delete;

  // 生命周期管理
  void Start();
  void Stop();
  bool IsRunning() const {
    return running_.load();
  }

  // 获取分配的 Worker ID
  std::string GetWorkerId() const {
    return worker_id_;
  }

  // 获取当前活跃任务数
  uint32_t GetActiveTaskCount() const {
    return active_tasks_.load();
  }

  // 检查某个任务是否已被取消
  bool IsTaskCancelled(const std::string& task_id) const {
    std::lock_guard<std::mutex> lock(cancel_mutex_);
    return cancelled_task_ids_.count(task_id) > 0;
  }

 private:
  // 向控制平面注册
  bool RegisterWithControlPlane();

  // 心跳循环
  void HeartbeatLoop();

  // 任务拉取循环
  void TaskFetchLoop();

  // 执行 Compaction 任务
  TaskExecutionResult ExecuteCompaction(const CompactionTaskInfo& task);

  // 执行 Bulk Load 分片任务
  BulkLoadShardResult ExecuteBulkLoadShard(const BulkLoadShardTaskInfo& task);

  // 上报任务结果
  void ReportTaskResult(const std::string& task_id,
                        const TaskExecutionResult& result);

  // 上报 Bulk Load 分片结果
  void ReportBulkLoadResult(const std::string& task_id,
                            const BulkLoadShardResult& result);

  // 标记任务开始执行
  void MarkTaskRunning(const std::string& task_id);

  // 创建 gRPC channel
  std::shared_ptr<grpc::Channel> CreateChannel();

  // 获取系统资源信息
  void DetectSystemResources();

  WorkerConfig config_;
  std::string worker_id_;
  std::atomic<bool> running_{false};
  std::atomic<bool> registered_{false};
  std::atomic<uint32_t> active_tasks_{0};

  // 问题13 修复: 心跳连续失败计数（超过阈值才触发重注册）
  uint32_t heartbeat_consecutive_failures_{0};
  static constexpr uint32_t kMaxHeartbeatFailures = 3;

  // 当前运行的任务 ID 列表
  std::vector<std::string> active_task_ids_;
  std::mutex tasks_mutex_;

  // 已取消的任务 ID 集合（由心跳线程写入，任务执行线程读取）
  std::unordered_set<std::string> cancelled_task_ids_;
  mutable std::mutex cancel_mutex_;

  // gRPC
  std::shared_ptr<grpc::Channel> channel_;
  // 问题12: 缓存的 gRPC stub（避免每次调用 NewStub）
  std::shared_ptr<void> cached_stub_;

  // 线程
  std::unique_ptr<std::thread> heartbeat_thread_;
  std::unique_ptr<std::thread> task_fetch_thread_;
  std::condition_variable cv_;
  std::mutex cv_mutex_;

  // 第五轮修复 (NEW-12): 任务执行线程池（可 join 的线程替代 detach）
  // Stop() 时 join 所有任务线程，防止 use-after-free
  std::vector<std::thread> task_worker_threads_;
  std::mutex task_workers_mutex_;

  // 系统资源信息
  uint32_t cpu_cores_ = 0;
  uint64_t memory_mb_ = 0;
  uint64_t disk_mb_ = 0;
};

}  // namespace remote_compaction
}  // namespace tendisplus
