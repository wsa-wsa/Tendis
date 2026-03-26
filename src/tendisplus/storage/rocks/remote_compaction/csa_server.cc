// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.
#include <grpc/grpc.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>

#include <cassert>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <ostream>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "csa.grpc.pb.h"  // NOLINT(build/include_subdir)
#include "control_plane.grpc.pb.h"
#include "rocksdb/db.h"
#include "rocksdb/db/compaction/compaction_job.h"
#include "rocksdb/db/dbformat.h"
#include "rocksdb/db/log_writer.h"
#include "rocksdb/db/version_edit.h"
#include "rocksdb/env.h"
#include "rocksdb/file/writable_file_writer.h"
#include "rocksdb/options.h"
#include "rocksdb/sst_file_reader.h"
#include "util/string_util.h"

#include "tendisplus/storage/rocks/shared_filesystem.h"
#include "bulk_load_executor.h"
#ifdef HDFS
#include "plugin/hdfs/env_hdfs.h"
#endif

#include "rocksdb/statistics.h"

#include "tendisplus/storage/rocks/nfs_filesystem.h"
#include "tendisplus/storage/rocks/remote_compaction/def.h"

namespace fs = std::filesystem;

ROCKSDB_NAMESPACE::RemoteOpenAndCompactOptions compaction_service_options;

std::atomic<int64_t> local_task_nums_ = std::atomic<int64_t>(0);

// Get max concurrent tasks with default value
int64_t GetMaxConcurrentTasks() {
  return compaction_service_options.GetMaxConcurrentTasks();
}

// Shared FileSystem Cache
// Cache shared file systems that have been created to avoid recreating them
// every time
class SharedFileSystemCache {
 public:
  struct CachedFS {
    std::shared_ptr<rocksdb::FileSystem> fs;
    std::unique_ptr<rocksdb::Env> env;
    std::string uri;
    std::string local_prefix;
  };

  // Get or create a shared file system from URI (URI mode)
  // Returns the Env pointer (possibly nullptr means using the default)
  rocksdb::Env* GetOrCreateEnvFromURI(const std::string& uri) {
    if (uri.empty()) {
      return nullptr;
    }

    std::string cache_key = "URI:" + uri;

    {
      std::lock_guard<std::mutex> lock(mutex_);
      auto it = cache_.find(cache_key);
      if (it != cache_.end()) {
        std::cout << "[FSCache] Reusing cached FileSystem for URI: " << uri
                  << std::endl;
        return it->second.env.get();
      }
    }

    // A new FileSystem needs to be created from URI
    // Use unified interface to create shared filesystem
    // Supports: nfs://, hdfs://, s3://, etc.
    // CSA server uses pure URI mode (no local path mapping), so local_prefix is empty
    std::shared_ptr<rocksdb::FileSystem> shared_fs;
    rocksdb::Status status = rocksdb::CreateSharedFileSystem(
      rocksdb::FileSystem::Default(), uri, "" /* local_prefix */, &shared_fs);
    if (!status.ok() || !shared_fs) {
      std::cerr << "[FSCache] Failed to create shared filesystem from URI: "
                << uri << ", error: " << status.ToString() << std::endl;
      return nullptr;
    }
    std::unique_ptr<rocksdb::Env> env = rocksdb::NewCompositeEnv(shared_fs);

    if (!env) {
      std::cerr << "[FSCache] Failed to create Env from URI: " << uri
                << std::endl;
      return nullptr;
    }

    std::cout << "[FSCache] Created new FileSystem from URI: " << uri
              << std::endl;

    std::lock_guard<std::mutex> lock(mutex_);
    // Double-check after acquiring lock
    auto it = cache_.find(cache_key);
    if (it != cache_.end()) {
      return it->second.env.get();
    }

    CachedFS cached;
    cached.fs = shared_fs;
    cached.env = std::move(env);
    cached.uri = uri;
    cached.local_prefix = "";  // URI mode doesn't need local_prefix

    rocksdb::Env* result = cached.env.get();
    cache_[cache_key] = std::move(cached);
    return result;
  }

  // Get or create a shared file system (legacy path matching mode)
  // Returns the Env pointer (possibly nullptr means using the default)
  rocksdb::Env* GetOrCreateEnv(const std::string& uri,
                               const std::string& local_prefix) {
    if (uri.empty() || local_prefix.empty()) {
      return nullptr;
    }

    std::string cache_key = uri + "|" + local_prefix;

    {
      std::lock_guard<std::mutex> lock(mutex_);
      auto it = cache_.find(cache_key);
      if (it != cache_.end()) {
        std::cout << "[FSCache] Reusing cached FileSystem for: " << uri
                  << std::endl;
        return it->second.env.get();
      }
    }

    // A new FileSystem needs to be created
    std::shared_ptr<rocksdb::FileSystem> fs;
    std::unique_ptr<rocksdb::Env> env;

    // Create the corresponding file system according to the URI scheme
    if (uri.find("nfs://") == 0) {
      std::cout << "[FSCache] Creating NFS FileSystem for: " << uri
                << ", local_prefix: " << local_prefix << std::endl;
      // rocksdb::Status status = rocksdb::NewNFSFileSystemSimple(uri,
      // local_prefix, &fs);
      rocksdb::Status status =
        rocksdb::NewNFSFileSystem(uri, local_prefix, &fs);
      if (!status.ok() || !fs) {
        std::cerr << "[FSCache] Failed to create NFS FileSystem: "
                  << status.ToString() << std::endl;
        return nullptr;
      }
      env = rocksdb::NewCompositeEnv(fs);
    }
#ifdef HDFS
    if (uri.find("hdfs://") == 0) {
      rocksdb::NewHdfsEnv(uri, &env);
    }
#endif
    // More storage types can be added in the future:
    // else if (uri.find("s3://") == 0) { ... }
    // else if (uri.find("gcs://") == 0) { ... }
    // else {
    //   std::cerr << "[FSCache] Unsupported URI scheme: " << uri << std::endl;
    //   return nullptr;
    // }

    if (!env) {
      std::cerr << "[FSCache] Failed to create Env for: " << uri << std::endl;
      return nullptr;
    }

    std::cout << "[FSCache] Created new FileSystem for: " << uri
              << ", local_prefix: " << local_prefix << std::endl;

    std::lock_guard<std::mutex> lock(mutex_);
    // Double-check after acquiring lock
    auto it = cache_.find(cache_key);
    if (it != cache_.end()) {
      return it->second.env.get();
    }

    CachedFS cached;
    cached.fs = std::move(fs);
    cached.env = std::move(env);
    cached.uri = uri;
    cached.local_prefix = local_prefix;

    rocksdb::Env* result = cached.env.get();
    cache_[cache_key] = std::move(cached);
    return result;
  }

  // Remove the specified cache (for configuration changes)
  void Invalidate(const std::string& uri, const std::string& local_prefix) {
    std::string cache_key = uri + "|" + local_prefix;
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = cache_.find(cache_key);
    if (it != cache_.end()) {
      std::cout << "[FSCache] Invalidated FileSystem cache for: " << uri
                << std::endl;
      cache_.erase(it);
    }
  }

  // Empty all caches
  void Clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    cache_.clear();
    std::cout << "[FSCache] Cleared all cached FileSystems" << std::endl;
  }

  static SharedFileSystemCache& Instance() {
    static SharedFileSystemCache instance;
    return instance;
  }

 private:
  SharedFileSystemCache() = default;
  std::mutex mutex_;
  std::unordered_map<std::string, CachedFS> cache_;
};

// Custom status codes for CSA server
// These are returned via CompactionReply.code field
// Note: Code 5 is used by RocksDB for IOError, so we use higher numbers for
// CSA-specific codes
constexpr int kCSACodeBusy =
  100;  // CSA server is busy, client should use local compaction
constexpr int kCSACodeStaleTask =
  101;  // Input files not found, this is a stale task

// CSA Service Implementation
class CSAImpl final : public csa::CSAService::Service {
 public:
  // Execute compaction task (shared storage mode only)
  grpc::Status ExecuteCompactionTask(
    grpc::ServerContext* context,
    const csa::CompactionArgs* compaction_args,
    csa::CompactionReply* compaction_reply) override {
    int64_t max_tasks = GetMaxConcurrentTasks();
    int64_t current = local_task_nums_.fetch_add(1);

    // If server is busy (at or over capacity), reject immediately
    // This prevents blocking gRPC threads and reduces stale task probability
    if (current >= max_tasks) {
      local_task_nums_.fetch_sub(1);
      std::cout << "[CSA] Server busy, rejecting task (current: " << current
                << ", max: " << max_tasks
                << "). Client should use local compaction." << std::endl;
      compaction_reply->set_code(kCSACodeBusy);
      compaction_reply->set_result("CSA server busy");
      return ::grpc::Status::OK;
    }

    std::cout << "CSA ExecuteCompactionTask() concurrency: " << (current + 1)
              << "/" << max_tasks << std::endl;

    std::string compaction_service_result;
    rocksdb::CompactionServiceOptionsOverride options_override;
    ROCKSDB_NAMESPACE::Options options_;

    std::string db_path = compaction_args->name();
    std::string output_directory = compaction_args->output_directory();
    std::string compaction_input = compaction_args->input();

    // Strip file:// URI prefix if present (file:// is local filesystem, not shared)
    db_path = rocksdb::StripFileURIPrefix(db_path);
    output_directory = rocksdb::StripFileURIPrefix(output_directory);

    // Shared storage mode: use shared filesystem (read-only)
    // CSA server reads from shared storage via URI, writes output to shared
    // storage Tendisplus will install results from output directory to final
    // location
    const std::string& shared_fs_uri = compaction_args->shared_fs_uri();

    std::cout << "[CSA] Received compaction request:"
              << " db_path=" << db_path
              << ", output_directory=" << output_directory
              << ", shared_fs_uri=" << shared_fs_uri << std::endl;

    // URI mode: db_path should be a URI (nfs://server/path/to/db)
    // Use shared_fs_uri as the base mount point for NFS FileSystem
    rocksdb::Env* shared_env = nullptr;

    if (!shared_fs_uri.empty() && shared_fs_uri.find("nfs://") == 0) {
      // Use shared_fs_uri directly as the mount point
      // This is the base path where NFS is mounted (e.g.,
      // nfs://server/shared/rocksdb)
      std::cout << "[CSA] URI mode: using shared_fs_uri as mount point: "
                << shared_fs_uri << std::endl;
      shared_env =
        SharedFileSystemCache::Instance().GetOrCreateEnvFromURI(shared_fs_uri);
    } else if (db_path.find("nfs://") == 0) {
      // db_path is URI but shared_fs_uri not set, extract base from db_path
      // Extract mount point: nfs://server/path/to/db -> nfs://server/path
      size_t proto_end = db_path.find("://");
      if (proto_end != std::string::npos) {
        size_t path_start = db_path.find('/', proto_end + 3);
        if (path_start != std::string::npos) {
          // Find first path component as mount point
          size_t first_slash = path_start;
          size_t second_slash = db_path.find('/', first_slash + 1);
          if (second_slash != std::string::npos) {
            std::string base_uri = db_path.substr(0, second_slash);
            std::cout << "[CSA] URI mode: extracted base_uri from db_path: "
                      << base_uri << std::endl;
            shared_env =
              SharedFileSystemCache::Instance().GetOrCreateEnvFromURI(base_uri);
          } else {
            // Only one path component, use as-is
            shared_env =
              SharedFileSystemCache::Instance().GetOrCreateEnvFromURI(db_path);
          }
        } else {
          // No path, use as-is
          shared_env =
            SharedFileSystemCache::Instance().GetOrCreateEnvFromURI(db_path);
        }
      }
    }

    if (shared_env) {
      options_override.env = shared_env;
      std::cout << "[CSA] Using shared storage FileSystem (read-only for "
                   "input, writable for output)"
                << std::endl;
    } else {
      std::cerr << "[CSA] WARNING: Failed to create shared filesystem env."
                << " db_path=" << db_path << ", shared_fs_uri=" << shared_fs_uri
                << ". Falling back to default filesystem." << std::endl;
    }

    // ===== Early stale task detection =====
    // Parse compaction input to get input files, then check if they exist
    // This avoids expensive OpenAndCompact calls for stale tasks
    ROCKSDB_NAMESPACE::CompactionServiceInput csi;
    rocksdb::Status parse_status =
      ROCKSDB_NAMESPACE::CompactionServiceInput::Read(compaction_input, &csi);
    if (parse_status.ok() && !csi.input_files.empty() && shared_env) {
      // Check if at least the first input file exists
      // If not, this is likely a stale task - return early
      const std::string& first_file = csi.input_files[0];
      std::string file_path = db_path + "/" + first_file;
      rocksdb::Env* env_to_check = shared_env;

      uint64_t file_size = 0;
      rocksdb::Status file_status =
        env_to_check->GetFileSize(file_path, &file_size);
      if (!file_status.ok()) {
        std::cout << "[CSA] Early stale task detection: file " << file_path
                  << " does not exist. Returning stale task code immediately."
                  << std::endl;
        local_task_nums_ -= 1;
        compaction_reply->set_code(kCSACodeStaleTask);
        compaction_reply->set_result("Stale task: input file not found");
        return ::grpc::Status::OK;
      }
    }

    options_override.file_checksum_gen_factory =
      options_.file_checksum_gen_factory;
    options_override.comparator = options_.comparator;
    options_override.merge_operator = options_.merge_operator;
    options_override.compaction_filter = options_.compaction_filter;
    options_override.compaction_filter_factory =
      options_.compaction_filter_factory;
    options_override.prefix_extractor = options_.prefix_extractor;
    options_override.table_factory = options_.table_factory;
    options_override.sst_partitioner_factory = options_.sst_partitioner_factory;
    options_override.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();

    std::cout << "[CSA] Calling DB::OpenAndCompact with:"
              << " db_path=" << db_path
              << ", output_directory=" << output_directory << ", using_env="
              << (options_override.env ? "shared_env" : "default") << std::endl;

    rocksdb::Status s =
      ROCKSDB_NAMESPACE::DB::OpenAndCompact(db_path,
                                            output_directory,
                                            compaction_input,
                                            &compaction_service_result,
                                            options_override);

    std::cout << "[CSA] OpenAndCompact result: " << s.ToString()
              << ", result_size=" << compaction_service_result.size()
              << std::endl;

    if (!s.ok()) {
      std::cerr << "[CSA] Compaction failed with error: " << s.ToString()
                << std::endl;
      std::cerr << "[CSA] Debug info:"
                << " db_path=" << db_path
                << ", output_directory=" << output_directory
                << ", shared_fs_uri=" << shared_fs_uri
                << ", env=" << (options_override.env ? "set" : "null")
                << std::endl;
    }

    compaction_reply->set_code(s.code());

    if (!s.ok()) {
      std::cerr << "Compaction failed: " << s.ToString() << std::endl;
      compaction_reply->set_result(compaction_service_result);
    } else {
      compaction_reply->set_result(std::move(compaction_service_result));
      std::cout << "Compaction completed successfully" << std::endl;
    }

    local_task_nums_ -= 1;
    std::cout << "Compaction finished with code: " << static_cast<int>(s.code())
              << std::endl;
    return ::grpc::Status::OK;
  }
};

// ============================================================================
// CSA Bulk Load Service - 支持 Bulk Load 分片执行
// 实现 control_plane.proto 中定义的 CSAService
// ============================================================================
class CSAServiceImpl final : public ::control_plane::CSAService::Service {
 public:
  explicit CSAServiceImpl(CSAImpl* csa_impl) : csa_impl_(csa_impl) {}

  // 执行 Bulk Load 分片（SST 生成）
  grpc::Status ExecuteBulkLoadShard(
    grpc::ServerContext* context,
    const ::control_plane::BulkLoadShardRequest* request,
    ::control_plane::BulkLoadShardResponse* response) override {

    int64_t max_tasks = GetMaxConcurrentTasks();
    int64_t current = local_task_nums_.fetch_add(1);

    if (current >= max_tasks) {
      local_task_nums_.fetch_sub(1);
      std::cout << "[CSA-BulkLoad] Server busy, rejecting Bulk Load shard "
                << "(current: " << current << ", max: " << max_tasks << ")"
                << std::endl;
      response->set_accepted(false);
      response->set_error_message("CSA server busy");
      return grpc::Status::OK;
    }

    std::cout << "[CSA-BulkLoad] Executing Bulk Load shard:"
              << " task_id=" << request->task_id()
              << ", shard_id=" << request->shard_id()
              << ", source=" << request->source_path()
              << ", output=" << request->sst_output_dir() << std::endl;

    // 构建执行参数
    tendisplus::remote_compaction::BulkLoadExecuteParams params;
    params.task_id = request->task_id();
    params.shard_id = request->shard_id();
    params.shard_index = request->shard_index();
    params.source_type = static_cast<int32_t>(request->source_type());
    params.source_path = request->source_path();
    params.data_format = static_cast<int32_t>(request->data_format());
    params.shared_fs_uri = request->shared_fs_uri();
    params.sst_output_dir = request->sst_output_dir();
    params.compression = static_cast<int32_t>(request->compression());
    params.target_sst_size = request->target_sst_size();
    params.generate_binlog = request->generate_binlog();
    params.target_store_id = request->target_store_id();
    params.target_db_path = request->target_db_path();
    params.rate_limit_bytes_per_sec = request->rate_limit_bytes_per_sec();
    params.timeout_sec = request->timeout_sec();

    // Key 范围
    if (request->has_key_range()) {
      params.key_range_start = request->key_range().start_key();
      params.key_range_end = request->key_range().end_key();
      params.slot_start = request->key_range().slot_start();
      params.slot_end = request->key_range().slot_end();
    }

    // 执行 Bulk Load
    tendisplus::remote_compaction::BulkLoadExecutor executor;
    auto result = executor.Execute(params);

    local_task_nums_.fetch_sub(1);

    // 填充响应
    response->set_accepted(result.success);
    if (!result.success) {
      response->set_error_message(result.error_message);
      std::cerr << "[CSA-BulkLoad] Shard execution failed: "
                << result.error_message << std::endl;
      return grpc::Status::OK;
    }

    // 上报 SST 文件元数据
    for (const auto& sst : result.sst_files) {
      auto* proto_sst = response->add_generated_sst_files();
      proto_sst->set_file_path(sst.file_path);
      proto_sst->set_column_family(sst.column_family);
      proto_sst->set_file_size(sst.file_size);
      proto_sst->set_num_entries(sst.num_entries);
      proto_sst->set_smallest_key(sst.smallest_key);
      proto_sst->set_largest_key(sst.largest_key);
      proto_sst->set_checksum(sst.checksum);
    }

    response->set_total_rows_processed(result.total_rows_processed);
    response->set_total_bytes_written(result.total_bytes_written);
    response->set_execution_time_ms(result.execution_time_ms);

    std::cout << "[CSA-BulkLoad] Shard completed:"
              << " task_id=" << request->task_id()
              << ", shard_id=" << request->shard_id()
              << ", rows=" << result.total_rows_processed
              << ", sst_files=" << result.sst_files.size()
              << ", time_ms=" << result.execution_time_ms << std::endl;

    return grpc::Status::OK;
  }

  // 分发 Compaction 任务 (CaaS-LSM 推送模式)
  grpc::Status DistributeCompactionJob(
    grpc::ServerContext* context,
    const ::control_plane::DistributeJobRequest* request,
    ::control_plane::DistributeJobResponse* response) override {
    if (!csa_impl_) {
      response->set_accepted(false);
      response->set_error_message("CSA compaction service not initialized");
      return grpc::Status::OK;
    }

    std::string task_id = request->task_id();
    std::cout << "[CSA-Push] Received DistributeCompactionJob: task_id="
              << task_id << std::endl;

    // 跟踪任务
    {
      std::lock_guard<std::mutex> lock(running_tasks_mutex_);
      // 检查任务是否在加入之前就已被取消
      if (cancelled_task_ids_.count(task_id) > 0) {
        cancelled_task_ids_.erase(task_id);
        response->set_accepted(false);
        response->set_error_message("Task already cancelled: " + task_id);
        return grpc::Status::OK;
      }
      running_task_ids_.insert(task_id);
    }

    // 构建 CompactionArgs 并转发到 CSAImpl
    csa::CompactionArgs compaction_args;
    compaction_args.set_name(request->db_name());
    compaction_args.set_input(request->compaction_args());
    // 构建 output_directory: db_name/job_id
    std::string output_dir = request->db_name() + "/" +
                             std::to_string(request->job_id());
    compaction_args.set_output_directory(output_dir);
    compaction_args.set_shared_fs_uri(request->shared_fs_uri());
    compaction_args.set_shared_fs_local_prefix(
      request->shared_fs_local_prefix());

    csa::CompactionReply compaction_reply;

    // 转发到 CSAImpl 执行
    grpc::Status status =
      csa_impl_->ExecuteCompactionTask(context, &compaction_args,
                                       &compaction_reply);

    // 移除任务跟踪并清理取消标志
    {
      std::lock_guard<std::mutex> lock(running_tasks_mutex_);
      running_task_ids_.erase(task_id);
      cancelled_task_ids_.erase(task_id);
    }

    if (!status.ok()) {
      response->set_accepted(false);
      response->set_error_message("gRPC error: " + status.error_message());
      return grpc::Status::OK;
    }

    if (compaction_reply.code() != 0) {
      response->set_accepted(false);
      response->set_error_message(
        "Compaction failed with code " +
        std::to_string(compaction_reply.code()) +
        ": " + compaction_reply.result());
      return grpc::Status::OK;
    }

    response->set_accepted(true);
    std::cout << "[CSA-Push] DistributeCompactionJob completed: task_id="
              << task_id << std::endl;
    return grpc::Status::OK;
  }

  // 检查 CSA 状态
  grpc::Status CheckCSAStatus(
    grpc::ServerContext* context,
    const ::control_plane::CSAStatusRequest* request,
    ::control_plane::CSAStatusResponse* response) override {
    // local_task_nums_ 包括 Legacy + Push 模式的所有任务
    response->set_local_task_nums(local_task_nums_.load());
    response->set_is_healthy(true);

    return grpc::Status::OK;
  }

  // 取消正在执行的任务
  grpc::Status CancelRunningTask(
    grpc::ServerContext* context,
    const ::control_plane::CancelRunningTaskRequest* request,
    ::control_plane::CancelRunningTaskResponse* response) override {
    std::string task_id = request->task_id();
    std::string reason = request->reason();

    std::cout << "[CSA] CancelRunningTask: task_id=" << task_id
              << ", reason=" << reason << std::endl;

    // 检查任务是否在运行
    bool found = false;
    {
      std::lock_guard<std::mutex> lock(running_tasks_mutex_);
      found = running_task_ids_.count(task_id) > 0;
      if (found) {
        // 标记为已取消（将 task_id 加入取消集合）
        cancelled_task_ids_.insert(task_id);
      }
    }

    if (!found) {
      response->set_success(false);
      response->set_error_message(
        "Task not found in running tasks: " + task_id);
      return grpc::Status::OK;
    }

    // 注：RocksDB 的 OpenAndCompact 是同步阻塞操作，无法主动中断。
    // 取消标志会在下一次任务调度检查时生效。
    // 对于正在执行中的 compaction，只能等待其自然完成。
    response->set_success(true);
    std::cout << "[CSA] Task marked for cancellation: " << task_id << std::endl;
    return grpc::Status::OK;
  }

  // 检查某个任务是否已被取消
  bool IsTaskCancelled(const std::string& task_id) {
    std::lock_guard<std::mutex> lock(running_tasks_mutex_);
    return cancelled_task_ids_.count(task_id) > 0;
  }

 private:
  CSAImpl* csa_impl_ = nullptr;  // 引用 CSAImpl 以转发 compaction 请求

  // 正在运行的任务跟踪（Push 模式）
  std::unordered_set<std::string> running_task_ids_;
  std::unordered_set<std::string> cancelled_task_ids_;
  std::mutex running_tasks_mutex_;
};

// Parse command line arguments
void ParseCommandLine(int argc, char* argv[]) {
  for (int i = 1; i < argc; i++) {
    std::string arg = argv[i];
    if (arg == "--csa_address" || arg == "-a") {
      if (i + 1 < argc) {
        compaction_service_options.csa_address = argv[++i];
      } else {
        std::cerr << "[CSA] Error: --csa_address requires a value" << std::endl;
        exit(1);
      }
    } else if (arg == "--max_concurrent_tasks" || arg == "-t") {
      if (i + 1 < argc) {
        compaction_service_options.csa_max_concurrent_tasks =
          std::stoll(argv[++i]);
      } else {
        std::cerr << "[CSA] Error: --max_concurrent_tasks requires a value"
                  << std::endl;
        exit(1);
      }
    } else if (arg == "--grpc_max_message_size" || arg == "-m") {
      if (i + 1 < argc) {
        compaction_service_options.grpc_max_message_size =
          std::stoll(argv[++i]);
      } else {
        std::cerr << "[CSA] Error: --grpc_max_message_size requires a value"
                  << std::endl;
        exit(1);
      }
    } else if (arg == "--help" || arg == "-h") {
      std::cout << "CSA Server - Compaction Service Agent\n"
                << "Usage: " << argv[0] << " [OPTIONS]\n\n"
                << "Options:\n"
                << "  --csa_address, -a ADDRESS     CSA server address "
                   "(REQUIRED, format: host:port)\n"
                << "  --max_concurrent_tasks, -t N   Maximum concurrent "
                   "compaction tasks (default: 5)\n"
                << "  --grpc_max_message_size, -m N  Max gRPC message size in "
                   "bytes (default: 16777216)\n"
                << "  --help, -h                    Show this help message\n\n"
                << "Examples:\n"
                << "  " << argv[0] << " --csa_address localhost:8010\n"
                << "  " << argv[0] << " -a 0.0.0.0:8010 -t 10 -m 33554432\n"
                << std::endl;
      exit(0);
    } else {
      std::cerr << "[CSA] Error: Unknown option: " << arg << std::endl;
      std::cerr << "Use --help for usage information" << std::endl;
      exit(1);
    }
  }

  // Also check environment variables as fallback
  const char* env_csa_address = std::getenv("CSA_ADDRESS");
  if (compaction_service_options.csa_address.empty() && env_csa_address) {
    compaction_service_options.csa_address = env_csa_address;
  }

  const char* env_max_tasks = std::getenv("CSA_MAX_CONCURRENT_TASKS");
  if (compaction_service_options.csa_max_concurrent_tasks == 0 &&
      env_max_tasks) {
    compaction_service_options.csa_max_concurrent_tasks =
      std::stoll(env_max_tasks);
  }

  const char* env_max_msg = std::getenv("CSA_GRPC_MAX_MESSAGE_SIZE");
  if (compaction_service_options.grpc_max_message_size == 0 && env_max_msg) {
    compaction_service_options.grpc_max_message_size = std::stoll(env_max_msg);
  }
}

int main(int argc, char* argv[]) {
  // Parse command line arguments
  ParseCommandLine(argc, argv);

  std::string server_address(compaction_service_options.csa_address);

  // Validate server address
  if (server_address.empty()) {
    std::cerr << "[CSA] Error: CSA address not configured." << std::endl;
    std::cerr << "[CSA] Please provide --csa_address option or set CSA_ADDRESS "
                 "environment variable."
              << std::endl;
    std::cerr << "[CSA] Example: " << argv[0] << " --csa_address localhost:8010"
              << std::endl;
    return 1;
  }

  CSAImpl service;
  CSAServiceImpl bulk_load_service(&service);

  // Get max gRPC message size from configuration (with default)
  int64_t max_msg_size = compaction_service_options.GetGrpcMaxMessageSize();

  grpc::ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  builder.RegisterService(&bulk_load_service);

  // Set max message size for receiving and sending
  builder.SetMaxReceiveMessageSize(static_cast<int>(max_msg_size));
  builder.SetMaxSendMessageSize(static_cast<int>(max_msg_size));

  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  if (!server) {
    std::cerr << "[CSA] Error: Failed to start server on " << server_address
              << std::endl;
    return 1;
  }

  std::cout << "[CSA] Server listening on " << server_address
            << " (max message size: " << max_msg_size / 1024 / 1024 << "MB"
            << ", max concurrent tasks: " << GetMaxConcurrentTasks() << ")"
            << std::endl;
  server->Wait();
  return 0;
}
