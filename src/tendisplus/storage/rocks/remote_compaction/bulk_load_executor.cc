// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include "bulk_load_executor.h"

#include <algorithm>
#include <chrono>
#include <fstream>
#include <iostream>
#include <sstream>

#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/sst_file_reader.h"
#include "rocksdb/sst_file_writer.h"
#include "rocksdb/table.h"

#include "tendisplus/storage/rocks/shared_filesystem.h"

namespace tendisplus {
namespace remote_compaction {

// ============================================================================
// SharedFileSystemCache (复用 control_plane_worker.cc 中的缓存逻辑)
// ============================================================================
namespace {

class BulkLoadFSCache {
 public:
  static BulkLoadFSCache& Instance() {
    static BulkLoadFSCache instance;
    return instance;
  }

  rocksdb::Env* GetOrCreateEnvFromURI(const std::string& uri) {
    if (uri.empty()) {
      return rocksdb::Env::Default();
    }

    std::string cache_key = "BLURI:" + uri;

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
      std::cerr << "[BulkLoadExecutor-FSCache] Failed to create shared "
                << "filesystem from URI: " << uri
                << ", error: " << status.ToString() << std::endl;
      return nullptr;
    }

    std::unique_ptr<rocksdb::Env> env = rocksdb::NewCompositeEnv(shared_fs);
    if (!env) {
      std::cerr << "[BulkLoadExecutor-FSCache] Failed to create Env from URI: "
                << uri << std::endl;
      return nullptr;
    }

    std::cout << "[BulkLoadExecutor-FSCache] Created new FileSystem from URI: "
              << uri << std::endl;

    std::lock_guard<std::mutex> lock(mutex_);
    auto it = cache_.find(cache_key);
    if (it != cache_.end()) {
      return it->second.env.get();
    }

    CachedEntry entry;
    entry.fs = shared_fs;
    entry.env = std::move(env);
    rocksdb::Env* result = entry.env.get();
    cache_[cache_key] = std::move(entry);
    return result;
  }

 private:
  BulkLoadFSCache() = default;
  ~BulkLoadFSCache() = default;
  BulkLoadFSCache(const BulkLoadFSCache&) = delete;
  BulkLoadFSCache& operator=(const BulkLoadFSCache&) = delete;

  struct CachedEntry {
    std::shared_ptr<rocksdb::FileSystem> fs;
    std::unique_ptr<rocksdb::Env> env;
  };

  std::unordered_map<std::string, CachedEntry> cache_;
  std::mutex mutex_;
};

}  // anonymous namespace

// ============================================================================
// BulkLoadExecutor 主执行流程
// ============================================================================
BulkLoadExecuteResult BulkLoadExecutor::Execute(
  const BulkLoadExecuteParams& params) {
  BulkLoadExecuteResult result;
  auto start_time = std::chrono::steady_clock::now();

  std::cout << "[BulkLoadExecutor] Starting execution:"
            << " task_id=" << params.task_id
            << ", shard_id=" << params.shard_id
            << ", source=" << params.source_path
            << ", output=" << params.sst_output_dir << std::endl;

  // 步骤 1: 从数据源读取 KV 对
  std::vector<std::pair<std::string, std::string>> kv_pairs;
  std::string error;

  if (!ReadDataSource(params, kv_pairs, error)) {
    result.success = false;
    result.error_message = "Failed to read data source: " + error;
    std::cerr << "[BulkLoadExecutor] " << result.error_message << std::endl;
    return result;
  }

  std::cout << "[BulkLoadExecutor] Read " << kv_pairs.size()
            << " KV pairs from data source" << std::endl;

  if (kv_pairs.empty()) {
    // 空数据源也是有效结果
    result.success = true;
    result.total_rows_processed = 0;
    auto end_time = std::chrono::steady_clock::now();
    result.execution_time_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(
        end_time - start_time).count();
    std::cout << "[BulkLoadExecutor] Empty data source, completed successfully"
              << std::endl;
    return result;
  }

  // 步骤 2: 排序
  SortKeyValuePairs(kv_pairs);
  std::cout << "[BulkLoadExecutor] Sorted " << kv_pairs.size()
            << " KV pairs" << std::endl;

  // 步骤 3: 生成 SST 文件
  if (!GenerateSSTFiles(params, kv_pairs, result.sst_files, error)) {
    result.success = false;
    result.error_message = "Failed to generate SST files: " + error;
    std::cerr << "[BulkLoadExecutor] " << result.error_message << std::endl;
    return result;
  }

  // 汇总统计
  result.success = true;
  result.total_rows_processed = kv_pairs.size();
  for (const auto& kv : kv_pairs) {
    result.total_bytes_read += kv.first.size() + kv.second.size();
  }
  for (const auto& sst : result.sst_files) {
    result.total_bytes_written += sst.file_size;
  }

  auto end_time = std::chrono::steady_clock::now();
  result.execution_time_ms =
    std::chrono::duration_cast<std::chrono::milliseconds>(
      end_time - start_time).count();

  std::cout << "[BulkLoadExecutor] Completed successfully:"
            << " rows=" << result.total_rows_processed
            << ", sst_files=" << result.sst_files.size()
            << ", bytes_written=" << result.total_bytes_written
            << ", time_ms=" << result.execution_time_ms << std::endl;

  return result;
}

// ============================================================================
// 步骤 1: 从数据源读取 KV 对
// ============================================================================
bool BulkLoadExecutor::ReadDataSource(
  const BulkLoadExecuteParams& params,
  std::vector<std::pair<std::string, std::string>>& kv_pairs,
  std::string& error) {

  // 获取共享文件系统 Env
  rocksdb::Env* env = BulkLoadFSCache::Instance().GetOrCreateEnvFromURI(
    params.shared_fs_uri);
  if (!env) {
    env = rocksdb::Env::Default();
    std::cout << "[BulkLoadExecutor] Using default Env (no shared FS)"
              << std::endl;
  }

  // 根据数据源类型分发
  switch (params.source_type) {
    case 0:  // DATA_SOURCE_KV_FILE
      return ReadFromKVFile(params.source_path, env, kv_pairs, error);

    case 1:  // DATA_SOURCE_SST_FILE
      return ReadFromSSTFile(params.source_path, env, kv_pairs, error);

    case 2:  // DATA_SOURCE_RDB_FILE
      error = "RDB file source not yet supported";
      return false;

    case 3:  // DATA_SOURCE_TENDIS_DUMP
      error = "TendisPlus dump source not yet supported";
      return false;

    case 4:  // DATA_SOURCE_SNAPSHOT
      error = "Snapshot source not yet supported";
      return false;

    default:
      error = "Unknown data source type: " + std::to_string(params.source_type);
      return false;
  }
}

// ============================================================================
// 步骤 2: 排序 KV 对
// ============================================================================
void BulkLoadExecutor::SortKeyValuePairs(
  std::vector<std::pair<std::string, std::string>>& kv_pairs) {
  // RocksDB 默认使用 BytewiseComparator（字典序排序）
  // SstFileWriter 要求 Key 必须严格按 comparator 顺序递增添加
  std::sort(kv_pairs.begin(), kv_pairs.end(),
            [](const std::pair<std::string, std::string>& a,
               const std::pair<std::string, std::string>& b) {
              return a.first < b.first;
            });

  // 去重：保留最后出现的 value（等价于 REPLACE 模式）
  // 参考 SQLEngine TDBulkLoadExternalSSTBuilder 的 BULK_LOAD_DUP_CHECK_REPLACE
  if (kv_pairs.size() > 1) {
    auto write_it = kv_pairs.begin();
    for (auto read_it = kv_pairs.begin() + 1; read_it != kv_pairs.end();
         ++read_it) {
      if (read_it->first != write_it->first) {
        ++write_it;
        if (write_it != read_it) {
          *write_it = std::move(*read_it);
        }
      } else {
        // 同一个 key，保留后面出现的 value（REPLACE 语义）
        write_it->second = std::move(read_it->second);
      }
    }
    kv_pairs.erase(write_it + 1, kv_pairs.end());
  }
}

// ============================================================================
// 步骤 3: 使用 SstFileWriter 生成 SST 文件
// ============================================================================
bool BulkLoadExecutor::GenerateSSTFiles(
  const BulkLoadExecuteParams& params,
  const std::vector<std::pair<std::string, std::string>>& kv_pairs,
  std::vector<SSTFileMeta>& sst_files,
  std::string& error) {

  if (kv_pairs.empty()) {
    return true;  // 无数据，不生成 SST
  }

  // 获取共享文件系统 Env
  rocksdb::Env* env = BulkLoadFSCache::Instance().GetOrCreateEnvFromURI(
    params.shared_fs_uri);
  if (!env) {
    env = rocksdb::Env::Default();
  }

  // 确保输出目录存在
  rocksdb::Status dir_status = env->CreateDirIfMissing(params.sst_output_dir);
  if (!dir_status.ok()) {
    // 目录可能已经存在，不是致命错误
    std::cout << "[BulkLoadExecutor] CreateDirIfMissing: "
              << dir_status.ToString() << std::endl;
  }

  // 配置 SstFileWriter
  rocksdb::EnvOptions env_options;
  rocksdb::Options options;
  options.compression = GetRocksDBCompression(params.compression);
  options.env = env;

  // 自动切分: 当 SST 文件大小超过 target_sst_size 时切分到新文件
  // 参考 SQLEngine TDBulkLoadExternalSSTBuilder 的自动切分逻辑
  uint32_t file_index = 0;
  size_t kv_index = 0;

  while (kv_index < kv_pairs.size()) {
    std::string sst_path = BuildSSTFilePath(
      params.sst_output_dir, params.shard_id, file_index, "default");

    // 创建 SstFileWriter
    rocksdb::SstFileWriter sst_writer(env_options, options);
    rocksdb::Status s = sst_writer.Open(sst_path);
    if (!s.ok()) {
      error = "Failed to open SstFileWriter at " + sst_path +
              ": " + s.ToString();
      return false;
    }

    SSTFileMeta meta;
    meta.file_path = sst_path;
    meta.column_family = "default";
    meta.num_entries = 0;

    uint64_t current_file_size = 0;
    bool first_key = true;

    // 写入 KV 对到 SST 文件
    while (kv_index < kv_pairs.size()) {
      const auto& kv = kv_pairs[kv_index];

      s = sst_writer.Put(rocksdb::Slice(kv.first), rocksdb::Slice(kv.second));
      if (!s.ok()) {
        error = "SstFileWriter::Put failed at key index " +
                std::to_string(kv_index) + ": " + s.ToString();
        return false;
      }

      if (first_key) {
        meta.smallest_key = kv.first;
        first_key = false;
      }
      meta.largest_key = kv.first;
      meta.num_entries++;
      current_file_size += kv.first.size() + kv.second.size();
      kv_index++;

      // 检查是否需要切分到新 SST 文件
      // 确保同一个 key 不会被切分到不同 SST 文件
      // (参考 SQLEngine TDBulkLoadExternalSSTBuilder)
      if (current_file_size >= params.target_sst_size &&
          kv_index < kv_pairs.size() &&
          kv_pairs[kv_index].first != kv.first) {
        break;
      }
    }

    // 完成当前 SST 文件
    s = sst_writer.Finish();
    if (!s.ok()) {
      error = "SstFileWriter::Finish failed for " + sst_path +
              ": " + s.ToString();
      return false;
    }

    // 获取文件大小
    uint64_t actual_file_size = 0;
    rocksdb::Status size_status = env->GetFileSize(sst_path, &actual_file_size);
    if (size_status.ok()) {
      meta.file_size = actual_file_size;
    } else {
      meta.file_size = sst_writer.FileSize();
    }

    sst_files.push_back(std::move(meta));

    std::cout << "[BulkLoadExecutor] Generated SST: " << sst_path
              << " (entries=" << sst_files.back().num_entries
              << ", size=" << sst_files.back().file_size << ")" << std::endl;

    file_index++;
  }

  return true;
}

// ============================================================================
// 辅助方法
// ============================================================================

rocksdb::CompressionType BulkLoadExecutor::GetRocksDBCompression(
  int32_t compression_type) {
  switch (compression_type) {
    case 0:  // COMPRESS_NONE
      return rocksdb::kNoCompression;
    case 1:  // COMPRESS_SNAPPY
      return rocksdb::kSnappyCompression;
    case 2:  // COMPRESS_ZLIB
      return rocksdb::kZlibCompression;
    case 3:  // COMPRESS_LZ4
      return rocksdb::kLZ4Compression;
    case 4:  // COMPRESS_ZSTD
      return rocksdb::kZSTD;
    default:
      return rocksdb::kLZ4Compression;
  }
}

std::string BulkLoadExecutor::BuildSSTFilePath(
  const std::string& output_dir,
  const std::string& shard_id,
  uint32_t file_index,
  const std::string& cf_name) {
  std::ostringstream oss;
  oss << output_dir;
  if (!output_dir.empty() && output_dir.back() != '/') {
    oss << "/";
  }
  oss << shard_id << "_" << cf_name << "_" << file_index << ".external.sst";
  return oss.str();
}

bool BulkLoadExecutor::ReadFromKVFile(
  const std::string& path,
  rocksdb::Env* env,
  std::vector<std::pair<std::string, std::string>>& kv_pairs,
  std::string& error) {

  // KV 文件格式：每行一个 KV 对，key 和 value 用 tab 分隔
  // 或者是二进制格式：[key_len:4bytes][key_data][value_len:4bytes][value_data]
  std::unique_ptr<rocksdb::SequentialFile> file;
  rocksdb::Status s = env->NewSequentialFile(path, &file, rocksdb::EnvOptions());
  if (!s.ok()) {
    error = "Failed to open KV file: " + path + ", " + s.ToString();
    return false;
  }

  // 读取整个文件到缓冲区
  uint64_t file_size = 0;
  s = env->GetFileSize(path, &file_size);
  if (!s.ok()) {
    error = "Failed to get file size: " + path + ", " + s.ToString();
    return false;
  }

  if (file_size == 0) {
    return true;  // 空文件
  }

  // 按二进制格式读取：[4B key_len][key_data][4B value_len][value_data]
  std::string buffer;
  buffer.resize(file_size);
  rocksdb::Slice result;
  s = file->Read(file_size, &result, &buffer[0]);
  if (!s.ok()) {
    error = "Failed to read file: " + path + ", " + s.ToString();
    return false;
  }

  // 尝试二进制格式解析
  const char* data = result.data();
  size_t remaining = result.size();
  size_t offset = 0;

  while (offset + 4 <= remaining) {
    // 读取 key 长度 (4 字节小端)
    uint32_t key_len = 0;
    memcpy(&key_len, data + offset, sizeof(uint32_t));
    offset += 4;

    if (offset + key_len > remaining) {
      break;  // 数据不完整
    }

    std::string key(data + offset, key_len);
    offset += key_len;

    if (offset + 4 > remaining) {
      break;  // 数据不完整
    }

    // 读取 value 长度 (4 字节小端)
    uint32_t value_len = 0;
    memcpy(&value_len, data + offset, sizeof(uint32_t));
    offset += 4;

    if (offset + value_len > remaining) {
      break;  // 数据不完整
    }

    std::string value(data + offset, value_len);
    offset += value_len;

    kv_pairs.emplace_back(std::move(key), std::move(value));
  }

  if (kv_pairs.empty() && file_size > 0) {
    // 二进制格式解析失败，尝试文本格式 (key\tvalue\n)
    offset = 0;
    std::string line;
    while (offset < remaining) {
      // 找到行尾
      size_t eol = offset;
      while (eol < remaining && data[eol] != '\n') {
        eol++;
      }

      line.assign(data + offset, eol - offset);
      offset = eol + 1;

      if (line.empty()) continue;

      // 查找 tab 分隔符
      size_t tab_pos = line.find('\t');
      if (tab_pos != std::string::npos) {
        std::string key = line.substr(0, tab_pos);
        std::string value = line.substr(tab_pos + 1);
        kv_pairs.emplace_back(std::move(key), std::move(value));
      }
    }
  }

  std::cout << "[BulkLoadExecutor] Read " << kv_pairs.size()
            << " KV pairs from KV file: " << path << std::endl;
  return true;
}

bool BulkLoadExecutor::ReadFromSSTFile(
  const std::string& path,
  rocksdb::Env* env,
  std::vector<std::pair<std::string, std::string>>& kv_pairs,
  std::string& error) {

  // 使用 RocksDB SstFileReader 读取已有 SST 文件
  rocksdb::Options options;
  options.env = env;

  rocksdb::SstFileReader reader(options);
  rocksdb::Status s = reader.Open(path);
  if (!s.ok()) {
    error = "Failed to open SST file: " + path + ", " + s.ToString();
    return false;
  }

  // 使用迭代器遍历
  rocksdb::ReadOptions read_opts;
  std::unique_ptr<rocksdb::Iterator> iter(reader.NewIterator(read_opts));

  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    kv_pairs.emplace_back(iter->key().ToString(), iter->value().ToString());
  }

  if (!iter->status().ok()) {
    error = "SST file iteration error: " + iter->status().ToString();
    return false;
  }

  std::cout << "[BulkLoadExecutor] Read " << kv_pairs.size()
            << " KV pairs from SST file: " << path << std::endl;
  return true;
}

}  // namespace remote_compaction
}  // namespace tendisplus
