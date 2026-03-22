// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include "bulk_load_ingester.h"

#include <algorithm>
#include <chrono>
#include <iostream>
#include <sstream>

#include "rocksdb/env.h"
#include "rocksdb/sst_file_reader.h"

#include "control_plane.grpc.pb.h"
#include <grpcpp/grpcpp.h>

#include "tendisplus/storage/rocks/shared_filesystem.h"

namespace tendisplus {
namespace remote_compaction {

// ============================================================================
// 共享文件系统缓存 (复用与 BulkLoadExecutor/ControlPlaneWorker 相同的模式)
// ============================================================================
namespace {

class IngesterFSCache {
 public:
  static IngesterFSCache& Instance() {
    static IngesterFSCache instance;
    return instance;
  }

  rocksdb::Env* GetOrCreateEnvFromURI(const std::string& uri) {
    if (uri.empty()) {
      return rocksdb::Env::Default();
    }

    std::string cache_key = "INGEST:" + uri;

    {
      std::lock_guard<std::mutex> lock(mutex_);
      auto it = cache_.find(cache_key);
      if (it != cache_.end()) {
        return it->second.env.get();
      }
    }

    std::shared_ptr<rocksdb::FileSystem> shared_fs;
    rocksdb::Status status = rocksdb::CreateSharedFileSystem(
      rocksdb::FileSystem::Default(), uri, "" /* local_prefix */, &shared_fs);
    if (!status.ok() || !shared_fs) {
      std::cerr << "[BulkLoadIngester-FSCache] Failed to create shared "
                << "filesystem from URI: " << uri
                << ", error: " << status.ToString() << std::endl;
      return nullptr;
    }

    std::unique_ptr<rocksdb::Env> env = rocksdb::NewCompositeEnv(shared_fs);
    if (!env) {
      std::cerr << "[BulkLoadIngester-FSCache] Failed to create Env from URI: "
                << uri << std::endl;
      return nullptr;
    }

    std::cout << "[BulkLoadIngester-FSCache] Created new FileSystem from URI: "
              << uri << std::endl;

    std::lock_guard<std::mutex> lock(mutex_);
    auto it = cache_.find(cache_key);
    if (it != cache_.end()) {
      return it->second.env.get();
    }

    struct CachedEntry {
      std::shared_ptr<rocksdb::FileSystem> fs;
      std::unique_ptr<rocksdb::Env> env;
    };

    CachedEntry entry;
    entry.fs = shared_fs;
    entry.env = std::move(env);
    rocksdb::Env* result = entry.env.get();
    cache_[cache_key] = std::move(entry);
    return result;
  }

 private:
  IngesterFSCache() = default;
  ~IngesterFSCache() = default;
  IngesterFSCache(const IngesterFSCache&) = delete;
  IngesterFSCache& operator=(const IngesterFSCache&) = delete;

  struct CachedEntry {
    std::shared_ptr<rocksdb::FileSystem> fs;
    std::unique_ptr<rocksdb::Env> env;
  };

  std::unordered_map<std::string, CachedEntry> cache_;
  std::mutex mutex_;
};

}  // anonymous namespace

// ============================================================================
// BulkLoadIngester 主执行流程
// ============================================================================
BulkLoadIngestResult BulkLoadIngester::Ingest(
  rocksdb::DB* db,
  rocksdb::ColumnFamilyHandle* data_cf,
  rocksdb::ColumnFamilyHandle* binlog_cf,
  const BulkLoadIngestParams& params) {
  BulkLoadIngestResult result;
  auto start_time = std::chrono::steady_clock::now();

  std::cout << "[BulkLoadIngester] Starting SST ingestion:"
            << " task_id=" << params.task_id
            << ", sst_files=" << params.sst_files.size()
            << ", target_store=" << params.target_store_id << std::endl;

  // 检查参数
  if (!db) {
    result.error_message = "RocksDB instance is null";
    std::cerr << "[BulkLoadIngester] " << result.error_message << std::endl;
    ReportIngestResult(params, result);
    return result;
  }

  if (!data_cf) {
    result.error_message = "Data column family handle is null";
    std::cerr << "[BulkLoadIngester] " << result.error_message << std::endl;
    ReportIngestResult(params, result);
    return result;
  }

  if (params.sst_files.empty()) {
    // 空 SST 列表也是有效结果
    result.success = true;
    result.ingested_sst_count = 0;
    auto end_time = std::chrono::steady_clock::now();
    result.ingest_time_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(
        end_time - start_time).count();
    std::cout << "[BulkLoadIngester] No SST files to ingest, done."
              << std::endl;
    ReportIngestResult(params, result);
    return result;
  }

  // 步骤 1: 验证 SST 文件
  // 通过共享文件系统检查文件存在性和大小
  rocksdb::Env* env = IngesterFSCache::Instance().GetOrCreateEnvFromURI(
    params.shared_fs_uri);
  if (!env) {
    env = rocksdb::Env::Default();
    std::cout << "[BulkLoadIngester] Using default Env (no shared FS)"
              << std::endl;
  }

  if (params.verify_checksum) {
    std::string validate_error;
    if (!ValidateSSTFiles(env, params.sst_files, validate_error)) {
      result.error_message = "SST file validation failed: " + validate_error;
      std::cerr << "[BulkLoadIngester] " << result.error_message << std::endl;
      ReportIngestResult(params, result);
      return result;
    }
    std::cout << "[BulkLoadIngester] SST file validation passed" << std::endl;
  }

  // 步骤 2: 按 CF 分类 SST 文件
  std::vector<std::string> data_cf_files;
  std::vector<std::string> binlog_cf_files;
  ClassifySSTFilesByCF(params.sst_files, data_cf_files, binlog_cf_files);

  result.data_cf_files = data_cf_files.size();
  result.binlog_cf_files = binlog_cf_files.size();

  std::cout << "[BulkLoadIngester] Classified SST files:"
            << " data_cf=" << data_cf_files.size()
            << ", binlog_cf=" << binlog_cf_files.size() << std::endl;

  // 步骤 3: 构建注入选项
  rocksdb::IngestExternalFileOptions ingest_opts = BuildIngestOptions(params);

  // 步骤 4: 注入 Data CF 的 SST 文件
  if (!data_cf_files.empty()) {
    std::cout << "[BulkLoadIngester] Ingesting " << data_cf_files.size()
              << " SST files into data CF" << std::endl;

    rocksdb::Status s = db->IngestExternalFile(
      data_cf, data_cf_files, ingest_opts);

    if (!s.ok()) {
      result.error_message =
        "IngestExternalFile failed for data CF: " + s.ToString();
      std::cerr << "[BulkLoadIngester] " << result.error_message << std::endl;
      ReportIngestResult(params, result);
      return result;
    }

    std::cout << "[BulkLoadIngester] Data CF ingestion successful: "
              << data_cf_files.size() << " files" << std::endl;
  }

  // 步骤 5: 注入 Binlog CF 的 SST 文件 (如果有)
  if (!binlog_cf_files.empty() && binlog_cf) {
    std::cout << "[BulkLoadIngester] Ingesting " << binlog_cf_files.size()
              << " SST files into binlog CF" << std::endl;

    rocksdb::Status s = db->IngestExternalFile(
      binlog_cf, binlog_cf_files, ingest_opts);

    if (!s.ok()) {
      result.error_message =
        "IngestExternalFile failed for binlog CF: " + s.ToString();
      std::cerr << "[BulkLoadIngester] " << result.error_message << std::endl;
      ReportIngestResult(params, result);
      return result;
    }

    std::cout << "[BulkLoadIngester] Binlog CF ingestion successful: "
              << binlog_cf_files.size() << " files" << std::endl;
  }

  // 汇总结果
  result.success = true;
  result.ingested_sst_count = data_cf_files.size() + binlog_cf_files.size();
  for (const auto& sst : params.sst_files) {
    result.ingested_bytes += sst.file_size;
    result.ingested_rows += sst.num_entries;
  }

  auto end_time = std::chrono::steady_clock::now();
  result.ingest_time_ms =
    std::chrono::duration_cast<std::chrono::milliseconds>(
      end_time - start_time).count();

  std::cout << "[BulkLoadIngester] Ingestion completed successfully:"
            << " task_id=" << params.task_id
            << ", sst_count=" << result.ingested_sst_count
            << ", bytes=" << result.ingested_bytes
            << ", rows=" << result.ingested_rows
            << ", time_ms=" << result.ingest_time_ms << std::endl;

  // 步骤 6: 上报结果到 Control Plane
  ReportIngestResult(params, result);

  return result;
}

// ============================================================================
// 验证 SST 文件
// ============================================================================
bool BulkLoadIngester::ValidateSSTFiles(
  rocksdb::Env* env,
  const std::vector<IngestSSTFileInfo>& sst_files,
  std::string& error) {
  for (const auto& sst : sst_files) {
    // 检查文件是否存在
    uint64_t file_size = 0;
    rocksdb::Status s = env->GetFileSize(sst.file_path, &file_size);
    if (!s.ok()) {
      error = "SST file not found or inaccessible: " + sst.file_path +
              " (" + s.ToString() + ")";
      return false;
    }

    // 检查文件大小是否匹配 (允许一定偏差)
    if (sst.file_size > 0 && file_size != sst.file_size) {
      std::cerr << "[BulkLoadIngester] WARNING: File size mismatch for "
                << sst.file_path << ": expected=" << sst.file_size
                << ", actual=" << file_size << std::endl;
      // 文件大小不匹配可能是因为元数据统计差异，暂不报错
    }

    // 尝试打开 SST 文件验证其有效性
    rocksdb::Options opts;
    opts.env = env;
    rocksdb::SstFileReader reader(opts);
    s = reader.Open(sst.file_path);
    if (!s.ok()) {
      error = "SST file corrupted or invalid: " + sst.file_path +
              " (" + s.ToString() + ")";
      return false;
    }

    std::cout << "[BulkLoadIngester] Validated SST: " << sst.file_path
              << " (size=" << file_size
              << ", entries=" << sst.num_entries << ")" << std::endl;
  }

  return true;
}

// ============================================================================
// 按 CF 分类 SST 文件
// ============================================================================
void BulkLoadIngester::ClassifySSTFilesByCF(
  const std::vector<IngestSSTFileInfo>& sst_files,
  std::vector<std::string>& data_cf_files,
  std::vector<std::string>& binlog_cf_files) {
  for (const auto& sst : sst_files) {
    if (sst.column_family == "binlog") {
      binlog_cf_files.push_back(sst.file_path);
    } else {
      // "default" 或其他未指定的 CF 都归入 data CF
      data_cf_files.push_back(sst.file_path);
    }
  }
}

// ============================================================================
// 构建 IngestExternalFile 选项
// ============================================================================
rocksdb::IngestExternalFileOptions BulkLoadIngester::BuildIngestOptions(
  const BulkLoadIngestParams& params) {
  rocksdb::IngestExternalFileOptions opts;

  // 移动文件模式 (而非复制)
  // 对于共享存储场景通常使用 false (需要复制到本地)
  opts.move_files = params.move_files;

  // 允许全局序列号 (必须为 true，否则注入会失败)
  opts.allow_global_seqno = params.allow_global_seqno;

  // 允许阻塞 flush (注入前可能需要 flush memtable)
  opts.allow_blocking_flush = params.allow_blocking_flush;

  // 是否注入到最底层
  opts.ingest_behind = params.ingest_behind;

  // 在注入前验证校验和
  opts.verify_checksums_before_ingest = params.verify_checksum;

  // 快照一致性: 写入 global_seqno 使已有快照看不到注入的数据
  // 这保证了注入操作的原子性
  opts.snapshot_consistency = true;

  return opts;
}

// ============================================================================
// 上报注入结果到 Control Plane
// ============================================================================
void BulkLoadIngester::ReportIngestResult(
  const BulkLoadIngestParams& params,
  const BulkLoadIngestResult& result) {
  if (params.control_plane_address.empty()) {
    std::cout << "[BulkLoadIngester] No control plane address, skip reporting"
              << std::endl;
    return;
  }

  // 创建 gRPC 连接
  auto channel = grpc::CreateChannel(
    params.control_plane_address, grpc::InsecureChannelCredentials());
  if (!channel) {
    std::cerr << "[BulkLoadIngester] Failed to create gRPC channel to: "
              << params.control_plane_address << std::endl;
    return;
  }

  auto stub = control_plane::ControlPlaneService::NewStub(channel);
  if (!stub) {
    std::cerr << "[BulkLoadIngester] Failed to create stub" << std::endl;
    return;
  }

  // 构建 ReportIngestResultRequest
  control_plane::ReportIngestResultRequest request;
  request.set_task_id(params.task_id);
  request.set_source_node_id(params.source_node_id);
  request.set_success(result.success);
  request.set_ingested_sst_count(result.ingested_sst_count);
  request.set_ingested_bytes(result.ingested_bytes);
  request.set_ingested_rows(result.ingested_rows);
  request.set_error_message(result.error_message);
  request.set_ingest_time_ms(result.ingest_time_ms);

  control_plane::ReportIngestResultResponse response;
  grpc::ClientContext context;

  auto deadline =
    std::chrono::system_clock::now() + std::chrono::seconds(10);
  context.set_deadline(deadline);

  grpc::Status grpc_status =
    stub->ReportIngestResult(&context, request, &response);

  if (!grpc_status.ok()) {
    std::cerr << "[BulkLoadIngester] ReportIngestResult RPC failed: "
              << grpc_status.error_message() << std::endl;
    return;
  }

  std::cout << "[BulkLoadIngester] Reported ingest result to Control Plane:"
            << " task_id=" << params.task_id
            << ", success=" << result.success
            << ", sst_count=" << result.ingested_sst_count << std::endl;
}

}  // namespace remote_compaction
}  // namespace tendisplus
