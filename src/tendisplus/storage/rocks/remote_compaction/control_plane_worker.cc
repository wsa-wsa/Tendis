// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include "control_plane_worker.h"

#include <algorithm>
#include <iostream>
#include <mutex>
#include <thread>
#include <unordered_map>

#include "control_plane.grpc.pb.h"
#include "rocksdb/db.h"
#include "rocksdb/db/compaction/compaction_job.h"
#include "rocksdb/env.h"

#include "tendisplus/storage/rocks/shared_filesystem.h"
#include "bulk_load_executor.h"

namespace tendisplus {
namespace remote_compaction {

// ============================================================================
// SharedFileSystemCache - 共享文件系统缓存 (复用 csa_server.cc 逻辑)
// 缓存已创建的共享文件系统实例，避免每次任务执行时重复创建
// ============================================================================
class SharedFileSystemCache {
 public:
  static SharedFileSystemCache& Instance() {
    static SharedFileSystemCache instance;
    return instance;
  }

  struct CachedFS {
    std::shared_ptr<rocksdb::FileSystem> fs;
    std::unique_ptr<rocksdb::Env> env;
    std::string uri;
    std::string local_prefix;
  };

  // 从 URI 获取或创建共享文件系统环境 (URI 模式)
  // 返回 Env 指针 (nullptr 表示使用默认 Env)
  rocksdb::Env* GetOrCreateEnvFromURI(const std::string& uri) {
    if (uri.empty()) {
      return nullptr;
    }

    std::string cache_key = "URI:" + uri;

    {
      std::lock_guard<std::mutex> lock(mutex_);
      auto it = cache_.find(cache_key);
      if (it != cache_.end()) {
        std::cout << "[CPWorker-FSCache] Reusing cached FileSystem for URI: "
                  << uri << std::endl;
        return it->second.env.get();
      }
    }

    // 使用统一接口创建共享文件系统
    // Worker 端使用纯 URI 模式 (无本地路径映射), local_prefix 为空
    std::shared_ptr<rocksdb::FileSystem> shared_fs;
    rocksdb::Status status = rocksdb::CreateSharedFileSystem(
      rocksdb::FileSystem::Default(), uri, "" /* local_prefix */, &shared_fs);
    if (!status.ok() || !shared_fs) {
      std::cerr << "[CPWorker-FSCache] Failed to create shared filesystem "
                << "from URI: " << uri
                << ", error: " << status.ToString() << std::endl;
      return nullptr;
    }

    std::unique_ptr<rocksdb::Env> env = rocksdb::NewCompositeEnv(shared_fs);
    if (!env) {
      std::cerr << "[CPWorker-FSCache] Failed to create Env from URI: "
                << uri << std::endl;
      return nullptr;
    }

    std::cout << "[CPWorker-FSCache] Created new FileSystem from URI: "
              << uri << std::endl;

    std::lock_guard<std::mutex> lock(mutex_);
    // 双重检查：获取锁后再次检查
    auto it = cache_.find(cache_key);
    if (it != cache_.end()) {
      return it->second.env.get();
    }

    CachedFS cached;
    cached.fs = shared_fs;
    cached.env = std::move(env);
    cached.uri = uri;
    cached.local_prefix = "";  // URI 模式不需要 local_prefix

    rocksdb::Env* result = cached.env.get();
    cache_[cache_key] = std::move(cached);
    return result;
  }

 private:
  SharedFileSystemCache() = default;
  ~SharedFileSystemCache() = default;
  SharedFileSystemCache(const SharedFileSystemCache&) = delete;
  SharedFileSystemCache& operator=(const SharedFileSystemCache&) = delete;

  std::unordered_map<std::string, CachedFS> cache_;
  std::mutex mutex_;
};

ControlPlaneWorker::ControlPlaneWorker(const WorkerConfig& config)
  : config_(config) {
  DetectSystemResources();
}

ControlPlaneWorker::~ControlPlaneWorker() {
  Stop();
}

void ControlPlaneWorker::DetectSystemResources() {
  // 自动检测系统资源
  if (config_.cpu_cores == 0) {
    cpu_cores_ = std::thread::hardware_concurrency();
    if (cpu_cores_ == 0)
      cpu_cores_ = 4;  // 默认值
  } else {
    cpu_cores_ = config_.cpu_cores;
  }

  if (config_.memory_mb == 0) {
    // 简单估算：假设 8GB
    memory_mb_ = 8192;
  } else {
    memory_mb_ = config_.memory_mb;
  }

  if (config_.disk_mb == 0) {
    // 简单估算：假设 100GB
    disk_mb_ = 102400;
  } else {
    disk_mb_ = config_.disk_mb;
  }

  std::cout << "[ControlPlaneWorker] Detected resources: CPU=" << cpu_cores_
            << " cores, Memory=" << memory_mb_ << " MB, Disk=" << disk_mb_
            << " MB" << std::endl;
}

void ControlPlaneWorker::Start() {
  if (running_.exchange(true)) {
    return;
  }

  // 创建 channel
  channel_ = CreateChannel();
  if (!channel_) {
    std::cerr << "[ControlPlaneWorker] Failed to create gRPC channel"
              << std::endl;
    running_.store(false);
    return;
  }

  // 注册到控制平面
  if (!RegisterWithControlPlane()) {
    std::cerr << "[ControlPlaneWorker] Failed to register with control plane"
              << std::endl;
    // 继续运行，后台线程会重试
  }

  // 启动心跳线程
  heartbeat_thread_ = std::make_unique<std::thread>([this]() {
    HeartbeatLoop();
  });

  // 启动任务拉取线程
  task_fetch_thread_ = std::make_unique<std::thread>([this]() {
    TaskFetchLoop();
  });

  std::cout << "[ControlPlaneWorker] Started, address=" << config_.worker_address
            << std::endl;
}

void ControlPlaneWorker::Stop() {
  if (!running_.exchange(false)) {
    return;
  }

  {
    std::lock_guard<std::mutex> lock(cv_mutex_);
    cv_.notify_all();
  }

  if (heartbeat_thread_ && heartbeat_thread_->joinable()) {
    heartbeat_thread_->join();
  }
  if (task_fetch_thread_ && task_fetch_thread_->joinable()) {
    task_fetch_thread_->join();
  }

  std::cout << "[ControlPlaneWorker] Stopped" << std::endl;
}

std::shared_ptr<grpc::Channel> ControlPlaneWorker::CreateChannel() {
  grpc::ChannelArguments channel_args;
  channel_args.SetMaxReceiveMessageSize(
    static_cast<int>(config_.grpc_max_message_size));
  channel_args.SetMaxSendMessageSize(
    static_cast<int>(config_.grpc_max_message_size));

  return grpc::CreateCustomChannel(config_.control_plane_address,
                                   grpc::InsecureChannelCredentials(),
                                   channel_args);
}

bool ControlPlaneWorker::RegisterWithControlPlane() {
  if (!channel_) {
    return false;
  }

  auto stub = control_plane::ControlPlaneService::NewStub(channel_);

  control_plane::RegisterWorkerRequest request;
  if (!config_.worker_id.empty()) {
    request.set_worker_id(config_.worker_id);
  }
  request.set_address(config_.worker_address);

  auto* resources = request.mutable_resources();
  resources->set_total_cpu_cores(cpu_cores_);
  resources->set_total_memory_mb(memory_mb_);
  resources->set_total_disk_mb(disk_mb_);
  resources->set_max_concurrent_tasks(config_.max_concurrent_tasks);
  resources->set_active_tasks(active_tasks_.load());

  control_plane::RegisterWorkerResponse response;
  grpc::ClientContext context;

  auto deadline =
    std::chrono::system_clock::now() + std::chrono::seconds(10);
  context.set_deadline(deadline);

  grpc::Status status = stub->RegisterWorker(&context, request, &response);

  if (!status.ok()) {
    std::cerr << "[ControlPlaneWorker] Registration failed: "
              << status.error_message() << std::endl;
    return false;
  }

  if (!response.success()) {
    std::cerr << "[ControlPlaneWorker] Registration rejected: "
              << response.error_message() << std::endl;
    return false;
  }

  worker_id_ = response.worker_id();
  registered_.store(true);

  std::cout << "[ControlPlaneWorker] Registered with control plane, id="
            << worker_id_ << ", heartbeat_interval="
            << response.heartbeat_interval_sec() << "s" << std::endl;

  return true;
}

void ControlPlaneWorker::HeartbeatLoop() {
  std::cout << "[ControlPlaneWorker] Heartbeat thread started" << std::endl;

  while (running_.load()) {
    if (!registered_.load()) {
      // 尝试重新注册
      if (RegisterWithControlPlane()) {
        std::cout << "[ControlPlaneWorker] Re-registered with control plane"
                  << std::endl;
      }
    } else {
      // 发送心跳
      auto stub = control_plane::ControlPlaneService::NewStub(channel_);

      control_plane::HeartbeatRequest request;
      request.set_worker_id(worker_id_);

      auto* resources = request.mutable_resources();
      resources->set_total_cpu_cores(cpu_cores_);
      resources->set_total_memory_mb(memory_mb_);
      resources->set_total_disk_mb(disk_mb_);
      resources->set_max_concurrent_tasks(config_.max_concurrent_tasks);
      resources->set_active_tasks(active_tasks_.load());

      // 添加活跃任务列表
      {
        std::lock_guard<std::mutex> lock(tasks_mutex_);
        for (const auto& task_id : active_task_ids_) {
          request.add_active_task_ids(task_id);
        }
      }

      control_plane::HeartbeatResponse response;
      grpc::ClientContext context;

      auto deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(5);
      context.set_deadline(deadline);

      grpc::Status status = stub->WorkerHeartbeat(&context, request, &response);

      if (!status.ok()) {
        std::cerr << "[ControlPlaneWorker] Heartbeat failed: "
                  << status.error_message() << std::endl;
        registered_.store(false);
      } else if (response.success()) {
        // 处理需要取消的任务
        for (const auto& task_id : response.tasks_to_cancel()) {
          std::cout << "[ControlPlaneWorker] Task cancelled by control plane: "
                    << task_id << std::endl;
          // 将取消的任务 ID 加入取消集合
          {
            std::lock_guard<std::mutex> lock(cancel_mutex_);
            cancelled_task_ids_.insert(task_id);
          }
        }
      }
    }

    // 等待下次心跳
    {
      std::unique_lock<std::mutex> lock(cv_mutex_);
      cv_.wait_for(
        lock,
        std::chrono::seconds(config_.heartbeat_interval_sec),
        [this]() { return !running_.load(); });
    }
  }

  std::cout << "[ControlPlaneWorker] Heartbeat thread stopped" << std::endl;
}

void ControlPlaneWorker::TaskFetchLoop() {
  std::cout << "[ControlPlaneWorker] Task fetch thread started" << std::endl;

  while (running_.load()) {
    if (!registered_.load()) {
      // 等待注册完成
      std::unique_lock<std::mutex> lock(cv_mutex_);
      cv_.wait_for(lock, std::chrono::seconds(1), [this]() {
        return !running_.load() || registered_.load();
      });
      continue;
    }

    // 检查是否有空闲槽位
    if (active_tasks_.load() >= config_.max_concurrent_tasks) {
      std::unique_lock<std::mutex> lock(cv_mutex_);
      cv_.wait_for(lock, std::chrono::milliseconds(100), [this]() {
        return !running_.load() ||
               active_tasks_.load() < config_.max_concurrent_tasks;
      });
      continue;
    }

    // 拉取任务
    auto stub = control_plane::ControlPlaneService::NewStub(channel_);

    control_plane::FetchTaskRequest request;
    request.set_worker_id(worker_id_);
    request.set_max_tasks(config_.max_concurrent_tasks - active_tasks_.load());

    control_plane::FetchTaskResponse response;
    grpc::ClientContext context;

    auto deadline =
      std::chrono::system_clock::now() + std::chrono::seconds(5);
    context.set_deadline(deadline);

    grpc::Status status = stub->FetchTask(&context, request, &response);

    if (!status.ok()) {
      std::cerr << "[ControlPlaneWorker] FetchTask failed: "
                << status.error_message() << std::endl;
      std::unique_lock<std::mutex> lock(cv_mutex_);
      cv_.wait_for(lock, std::chrono::seconds(1), [this]() {
        return !running_.load();
      });
      continue;
    }

    // 执行获取到的任务
    for (const auto& proto_task : response.tasks()) {
      if (!running_.load())
        break;

      CompactionTaskInfo task;
      task.task_id = proto_task.task_id();
      task.db_name = proto_task.db_name();
      task.store_id = proto_task.store_id();
      task.job_id = proto_task.job_id();
      task.compaction_input = proto_task.compaction_input();
      task.shared_fs_uri = proto_task.shared_fs_uri();
      task.shared_fs_local_prefix = proto_task.shared_fs_local_prefix();
      task.timeout_sec = proto_task.timeout_sec();

      std::cout << "[ControlPlaneWorker] Executing task: " << task.task_id
                << std::endl;

      // 检查任务在执行前是否已被取消
      if (IsTaskCancelled(task.task_id)) {
        std::cout << "[ControlPlaneWorker] Task already cancelled, skipping: "
                  << task.task_id << std::endl;
        // 上报取消结果
        TaskExecutionResult cancel_result;
        cancel_result.success = false;
        cancel_result.error_message = "Task cancelled before execution";
        ReportTaskResult(task.task_id, cancel_result);
        // 清理取消标志
        {
          std::lock_guard<std::mutex> lock(cancel_mutex_);
          cancelled_task_ids_.erase(task.task_id);
        }
        continue;
      }

      // 增加活跃任务计数
      active_tasks_++;
      {
        std::lock_guard<std::mutex> lock(tasks_mutex_);
        active_task_ids_.push_back(task.task_id);
      }

      // 标记任务开始执行
      MarkTaskRunning(task.task_id);

      // 执行任务
      auto result = ExecuteCompaction(task);

      // 上报结果
      ReportTaskResult(task.task_id, result);

      // 减少活跃任务计数
      {
        std::lock_guard<std::mutex> lock(tasks_mutex_);
        active_task_ids_.erase(
          std::remove(active_task_ids_.begin(), active_task_ids_.end(),
                      task.task_id),
          active_task_ids_.end());
      }
      active_tasks_--;

      // 清理取消集合中的条目（如果有）
      {
        std::lock_guard<std::mutex> lock(cancel_mutex_);
        cancelled_task_ids_.erase(task.task_id);
      }

      std::cout << "[ControlPlaneWorker] Task completed: " << task.task_id
                << ", success=" << result.success << std::endl;
    }

    // 如果没有获取到任务，等待一段时间再试
    if (response.tasks().empty()) {
      std::unique_lock<std::mutex> lock(cv_mutex_);
      cv_.wait_for(lock, std::chrono::milliseconds(500), [this]() {
        return !running_.load();
      });
    }
  }

  std::cout << "[ControlPlaneWorker] Task fetch thread stopped" << std::endl;
}

TaskExecutionResult ControlPlaneWorker::ExecuteCompaction(
  const CompactionTaskInfo& task) {
  TaskExecutionResult result;
  auto start_time = std::chrono::steady_clock::now();

  // 设置 compaction options
  ROCKSDB_NAMESPACE::CompactionServiceOptionsOverride options_override;

  // 设置共享文件系统环境 (复用 SharedFileSystemCache 单例)
  // 如果任务携带了 shared_fs_uri，则创建对应的共享文件系统 Env
  // 使 OpenAndCompact 能够通过共享存储读写 SST 文件
  if (!task.shared_fs_uri.empty()) {
    rocksdb::Env* shared_env =
      SharedFileSystemCache::Instance().GetOrCreateEnvFromURI(
        task.shared_fs_uri);
    if (shared_env) {
      options_override.env = shared_env;
      std::cout << "[ControlPlaneWorker] Using shared filesystem env for task: "
                << task.task_id << ", URI: " << task.shared_fs_uri << std::endl;
    } else {
      std::cerr << "[ControlPlaneWorker] WARNING: Failed to create shared "
                << "filesystem env for URI: " << task.shared_fs_uri
                << ", falling back to default env" << std::endl;
    }
  }

  std::string compaction_output;
  // Strip file:// URI prefix if present (file:// is local filesystem, not shared)
  std::string db_name = rocksdb::StripFileURIPrefix(task.db_name);
  std::string output_dir = db_name + "/" + std::to_string(task.job_id);

  ROCKSDB_NAMESPACE::Status s = ROCKSDB_NAMESPACE::DB::OpenAndCompact(
    db_name,
    output_dir,
    task.compaction_input,
    &compaction_output,
    options_override);

  auto end_time = std::chrono::steady_clock::now();
  result.execution_time_ms =
    std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time)
      .count();

  if (s.ok()) {
    result.success = true;
    result.compaction_result = std::move(compaction_output);
  } else {
    result.success = false;
    result.error_message = s.ToString();
  }

  return result;
}

void ControlPlaneWorker::MarkTaskRunning(const std::string& task_id) {
  // 通知控制平面任务开始执行
  // 这是可选的，控制平面会通过心跳了解任务状态
}

void ControlPlaneWorker::ReportTaskResult(const std::string& task_id,
                                          const TaskExecutionResult& result) {
  if (!channel_) {
    return;
  }

  auto stub = control_plane::ControlPlaneService::NewStub(channel_);

  control_plane::TaskResultRequest request;
  request.set_worker_id(worker_id_);
  request.set_task_id(task_id);
  request.set_success(result.success);
  request.set_compaction_result(result.compaction_result);
  request.set_error_message(result.error_message);
  request.set_execution_time_ms(result.execution_time_ms);
  request.set_bytes_read(result.bytes_read);
  request.set_bytes_written(result.bytes_written);

  control_plane::TaskResultResponse response;
  grpc::ClientContext context;

  auto deadline =
    std::chrono::system_clock::now() + std::chrono::seconds(10);
  context.set_deadline(deadline);

  grpc::Status status = stub->ReportTaskResult(&context, request, &response);

  if (!status.ok()) {
    std::cerr << "[ControlPlaneWorker] ReportTaskResult failed: "
              << status.error_message() << std::endl;
  }
}

// ============================================================================
// Bulk Load 分片执行
// ============================================================================
BulkLoadShardResult ControlPlaneWorker::ExecuteBulkLoadShard(
  const BulkLoadShardTaskInfo& task) {
  BulkLoadShardResult result;

  std::cout << "[ControlPlaneWorker] Executing Bulk Load shard:"
            << " task_id=" << task.task_id
            << ", shard_id=" << task.shard_id
            << ", source=" << task.source_path << std::endl;

  // 构建 BulkLoadExecutor 参数
  BulkLoadExecuteParams exec_params;
  exec_params.task_id = task.task_id;
  exec_params.shard_id = task.shard_id;
  exec_params.shard_index = task.shard_index;
  exec_params.source_type = task.source_type;
  exec_params.source_path = task.source_path;
  exec_params.data_format = task.data_format;
  exec_params.key_range_start = task.key_range_start;
  exec_params.key_range_end = task.key_range_end;
  exec_params.slot_start = task.slot_start;
  exec_params.slot_end = task.slot_end;
  exec_params.shared_fs_uri = task.shared_fs_uri;
  exec_params.sst_output_dir = task.sst_output_dir;
  exec_params.compression = task.compression;
  exec_params.target_sst_size = task.target_sst_size;
  exec_params.generate_binlog = task.generate_binlog;
  exec_params.target_store_id = task.target_store_id;
  exec_params.target_db_path = task.target_db_path;
  exec_params.rate_limit_bytes_per_sec = task.rate_limit_bytes_per_sec;
  exec_params.timeout_sec = task.timeout_sec;

  // 执行
  BulkLoadExecutor executor;
  auto exec_result = executor.Execute(exec_params);

  // 转换结果
  result.success = exec_result.success;
  result.error_message = exec_result.error_message;
  result.execution_time_ms = exec_result.execution_time_ms;
  result.total_rows_processed = exec_result.total_rows_processed;
  result.total_bytes_written = exec_result.total_bytes_written;

  for (const auto& sst : exec_result.sst_files) {
    BulkLoadShardResult::SSTMeta meta;
    meta.file_path = sst.file_path;
    meta.column_family = sst.column_family;
    meta.file_size = sst.file_size;
    meta.num_entries = sst.num_entries;
    result.sst_files.push_back(std::move(meta));
  }
  result.sst_files_count = result.sst_files.size();

  return result;
}

// ============================================================================
// 上报 Bulk Load 分片结果
// ============================================================================
void ControlPlaneWorker::ReportBulkLoadResult(
  const std::string& task_id,
  const BulkLoadShardResult& result) {
  if (!channel_) {
    return;
  }

  auto stub = control_plane::ControlPlaneService::NewStub(channel_);

  // 复用 TaskResultRequest 上报，通过 SST 文件信息区分 Bulk Load 结果
  control_plane::TaskResultRequest request;
  request.set_worker_id(worker_id_);
  request.set_task_id(task_id);
  request.set_success(result.success);
  request.set_error_message(result.error_message);
  request.set_execution_time_ms(result.execution_time_ms);
  request.set_bytes_written(result.total_bytes_written);
  request.set_bytes_read(0);

  // compaction_result 字段复用为 Bulk Load 的 SST 文件统计信息
  std::ostringstream oss;
  oss << "bulk_load_result:rows=" << result.total_rows_processed
      << ",sst_count=" << result.sst_files_count;
  for (const auto& sst : result.sst_files) {
    oss << "|" << sst.file_path << ":" << sst.file_size;
  }
  request.set_compaction_result(oss.str());

  control_plane::TaskResultResponse response;
  grpc::ClientContext context;
  auto deadline =
    std::chrono::system_clock::now() + std::chrono::seconds(10);
  context.set_deadline(deadline);

  grpc::Status status = stub->ReportTaskResult(&context, request, &response);
  if (!status.ok()) {
    std::cerr << "[ControlPlaneWorker] ReportBulkLoadResult failed: "
              << status.error_message() << std::endl;
  }
}

}  // namespace remote_compaction
}  // namespace tendisplus
