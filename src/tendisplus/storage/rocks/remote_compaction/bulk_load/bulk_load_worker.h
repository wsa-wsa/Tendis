// Copyright (c) 2024-present, Tencent Inc. All rights reserved.
// Bulk Load Worker 实现

#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "bulk_load_task.h"
#include "sst_builder.h"
#include "sst_ingest.h"

namespace tendisplus {
namespace bulk_load {

// ============================================================================
// Worker 配置
// ============================================================================
struct BulkLoadWorkerConfig {
  // 工作目录
  std::string work_directory = "/tmp/bulk_load";
  std::string output_directory;
  
  // 资源限制
  uint64_t memory_limit_bytes = 1024 * 1024 * 1024;  // 1GB
  uint32_t max_concurrent_tasks = 4;
  uint64_t rate_limit_bytes_per_sec = 0;
  
  // SST 构建配置
  SstBuilderConfig sst_builder_config;
  
  // 上传配置
  std::string upload_destination;  // 共享存储路径
  uint32_t upload_threads = 4;
  bool upload_verify = true;
  
  // 清理配置
  bool cleanup_on_success = true;
  bool cleanup_on_failure = false;
  uint32_t temp_file_ttl_hours = 24;
};

// ============================================================================
// 任务执行上下文
// ============================================================================
struct TaskExecutionContext {
  std::string task_id;
  std::string shard_id;
  BulkLoadParams params;
  DataShard shard;
  
  // 工作目录
  std::string work_dir;
  std::string output_dir;
  
  // 进度追踪
  std::atomic<double> progress{0.0};
  std::string status;
  
  // 取消标志
  std::atomic<bool> cancelled{false};
  
  // 结果
  BulkLoadSubTask::Result result;
};

// ============================================================================
// Bulk Load Worker
// ============================================================================
class BulkLoadWorker {
 public:
  explicit BulkLoadWorker(const BulkLoadWorkerConfig& config);
  ~BulkLoadWorker();
  
  // 禁止拷贝
  BulkLoadWorker(const BulkLoadWorker&) = delete;
  BulkLoadWorker& operator=(const BulkLoadWorker&) = delete;
  
  // 启动/停止
  void Start();
  void Stop();
  bool IsRunning() const { return running_.load(); }
  
  // 执行任务
  BulkLoadSubTask::Result Execute(const BulkLoadSubTask& task);
  
  // 异步执行
  void ExecuteAsync(const BulkLoadSubTask& task,
                    std::function<void(const BulkLoadSubTask::Result&)> callback);
  
  // 取消任务
  bool Cancel(const std::string& task_id);
  
  // 获取任务状态
  struct TaskStatus {
    std::string task_id;
    std::string shard_id;
    BulkLoadSubTask::State state;
    double progress = 0.0;
    std::string status_message;
  };
  
  TaskStatus GetTaskStatus(const std::string& task_id) const;
  std::vector<TaskStatus> GetAllTaskStatus() const;
  
  // 获取 Worker 状态
  struct WorkerStatus {
    bool running = false;
    uint32_t active_tasks = 0;
    uint32_t completed_tasks = 0;
    uint32_t failed_tasks = 0;
    uint64_t bytes_processed = 0;
    double cpu_usage = 0.0;
    double memory_usage = 0.0;
  };
  
  WorkerStatus GetWorkerStatus() const;

 private:
  // 执行流程
  BulkLoadSubTask::Result ExecuteInternal(TaskExecutionContext& ctx);
  
  // 阶段 1: 读取数据
  bool ReadData(TaskExecutionContext& ctx, std::vector<KeyValue>& data);
  
  // 阶段 2: 排序数据
  bool SortData(TaskExecutionContext& ctx, std::vector<KeyValue>& data);
  
  // 阶段 3: 构建 SST
  bool BuildSst(TaskExecutionContext& ctx, 
                const std::vector<KeyValue>& data,
                SstFileMetadata& metadata);
  
  // 阶段 4: 上传 SST
  bool UploadSst(TaskExecutionContext& ctx, 
                 const std::string& local_path,
                 std::string& remote_path);
  
  // 清理临时文件
  void Cleanup(TaskExecutionContext& ctx);
  
  // 创建工作目录
  std::string CreateWorkDir(const std::string& task_id);
  
  BulkLoadWorkerConfig config_;
  std::atomic<bool> running_{false};
  
  // 活跃任务
  std::map<std::string, std::shared_ptr<TaskExecutionContext>> active_tasks_;
  mutable std::mutex tasks_mutex_;
  
  // 统计
  std::atomic<uint32_t> completed_tasks_{0};
  std::atomic<uint32_t> failed_tasks_{0};
  std::atomic<uint64_t> bytes_processed_{0};
  
  // 线程池
  struct ThreadPool;
  std::unique_ptr<ThreadPool> thread_pool_;
};

// ============================================================================
// Bulk Load 控制器 (控制平面侧)
// ============================================================================
class BulkLoadController {
 public:
  explicit BulkLoadController();
  ~BulkLoadController();
  
  // 单例访问
  static BulkLoadController& Instance();
  
  // 提交 Bulk Load 任务
  std::string SubmitBulkLoad(const BulkLoadParams& params);
  
  // 查询任务状态
  struct BulkLoadStatus {
    std::string task_id;
    enum class State {
      kPreparing = 0,
      kSplitting = 1,
      kProcessing = 2,
      kIngesting = 3,
      kCompleted = 4,
      kFailed = 5,
      kCancelled = 6
    };
    State state = State::kPreparing;
    
    // 进度
    double overall_progress = 0.0;
    uint32_t shards_total = 0;
    uint32_t shards_completed = 0;
    uint32_t shards_failed = 0;
    
    // 统计
    uint64_t bytes_processed = 0;
    uint64_t records_processed = 0;
    uint64_t sst_files_created = 0;
    
    // 时间
    std::chrono::system_clock::time_point start_time;
    std::chrono::system_clock::time_point end_time;
    double elapsed_sec = 0;
    double eta_sec = 0;
    
    // 错误
    std::string error_message;
    std::vector<std::pair<std::string, std::string>> shard_errors;
  };
  
  BulkLoadStatus GetStatus(const std::string& task_id) const;
  
  // 取消任务
  bool Cancel(const std::string& task_id);
  
  // 暂停/恢复任务
  bool Pause(const std::string& task_id);
  bool Resume(const std::string& task_id);
  
  // 获取所有任务
  std::vector<BulkLoadStatus> GetAllTasks() const;
  
  // 设置回调
  using CompletionCallback = std::function<void(const std::string& task_id,
                                                 const BulkLoadResult& result)>;
  void SetCompletionCallback(CompletionCallback callback);

 private:
  // 任务处理流程
  void ProcessTask(const std::string& task_id);
  
  // 阶段处理
  void PrepareTask(BulkLoadTask& task);
  void SplitData(BulkLoadTask& task);
  void DispatchShards(BulkLoadTask& task);
  void WaitForCompletion(BulkLoadTask& task);
  void IngestResults(BulkLoadTask& task);
  void FinalizeTask(BulkLoadTask& task);
  
  // 分片调度
  void ScheduleShard(BulkLoadTask& task, DataShard& shard);
  
  // 处理分片结果
  void HandleShardResult(const std::string& task_id,
                         const std::string& shard_id,
                         const BulkLoadSubTask::Result& result);
  
  std::map<std::string, std::shared_ptr<BulkLoadTask>> tasks_;
  mutable std::mutex tasks_mutex_;
  
  CompletionCallback completion_callback_;
  
  // 分片器
  std::unique_ptr<DataShardSplitter> shard_splitter_;
  
  // Ingest 协调器
  std::unique_ptr<IngestCoordinator> ingest_coordinator_;
};

// ============================================================================
// Bulk Load 服务 (gRPC 服务)
// ============================================================================
class BulkLoadService {
 public:
  BulkLoadService();
  ~BulkLoadService();
  
  // 启动服务
  void Start(const std::string& listen_address);
  void Stop();
  
  // RPC 接口
  struct SubmitRequest {
    BulkLoadParams params;
  };
  
  struct SubmitResponse {
    bool success = false;
    std::string task_id;
    std::string error_message;
  };
  
  SubmitResponse Submit(const SubmitRequest& request);
  
  struct StatusRequest {
    std::string task_id;
  };
  
  struct StatusResponse {
    bool found = false;
    BulkLoadController::BulkLoadStatus status;
  };
  
  StatusResponse GetStatus(const StatusRequest& request);
  
  struct CancelRequest {
    std::string task_id;
    std::string reason;
  };
  
  struct CancelResponse {
    bool success = false;
    std::string error_message;
  };
  
  CancelResponse Cancel(const CancelRequest& request);

 private:
  std::atomic<bool> running_{false};
  // gRPC 服务器
  // std::unique_ptr<grpc::Server> server_;
};

}  // namespace bulk_load
}  // namespace tendisplus
