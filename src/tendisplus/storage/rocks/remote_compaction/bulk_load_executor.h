// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

// BulkLoadExecutor - Worker 端 SST 文件生成器
// Based on CaaS-LSM architecture
//
// 职责：
//   ① 从共享存储上的数据源读取 KV 对
//   ② 按 RocksDB comparator 顺序排序
//   ③ 使用 SstFileWriter 生成 SST 文件
//   ④ 写入 SST 到共享存储指定目录
//   ⑤ 上报 SST 文件元数据（路径/大小/key范围/校验和）
//
// 参考设计：
//   SQLEngine/TDStore TDBulkLoadExternalSSTBuilder 模式
//   （简化版：不含 2PC、外部归并排序、Raft 日志注入）

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/sst_file_writer.h"

namespace tendisplus {
namespace remote_compaction {

// ============================================================================
// Bulk Load 执行参数
// ============================================================================
struct BulkLoadExecuteParams {
  // 任务标识
  std::string task_id;
  std::string shard_id;
  uint32_t shard_index = 0;

  // 数据源
  int32_t source_type = 0;       // DataSourceType 枚举值
  std::string source_path;       // 数据源路径 (共享存储上)
  int32_t data_format = 0;       // DataFormat 枚举值

  // Key 范围
  std::string key_range_start;
  std::string key_range_end;
  uint32_t slot_start = 0;
  uint32_t slot_end = 0;

  // SST 输出配置
  std::string shared_fs_uri;     // 共享文件系统 URI
  std::string sst_output_dir;    // SST 输出目录
  int32_t compression = 3;       // CompressionType 枚举值 (默认 LZ4)
  uint64_t target_sst_size = 64 * 1024 * 1024;  // 64MB

  // 目标信息
  uint32_t target_store_id = 0;
  std::string target_db_path;
  bool generate_binlog = false;

  // 资源控制
  int64_t rate_limit_bytes_per_sec = 0;
  uint32_t timeout_sec = 7200;
};

// ============================================================================
// SST 文件元数据（生成结果）
// ============================================================================
struct SSTFileMeta {
  std::string file_path;         // SST 文件在共享存储上的路径
  std::string column_family;     // 目标 CF ("default" 或 "binlog")
  uint64_t file_size = 0;        // 文件大小 (bytes)
  uint64_t num_entries = 0;      // KV 条目数
  std::string smallest_key;      // 最小 key
  std::string largest_key;       // 最大 key
  std::string checksum;          // SHA256 校验和
};

// ============================================================================
// Bulk Load 执行结果
// ============================================================================
struct BulkLoadExecuteResult {
  bool success = false;
  std::string error_message;

  // 生成的 SST 文件
  std::vector<SSTFileMeta> sst_files;

  // 统计信息
  uint64_t total_rows_processed = 0;
  uint64_t total_bytes_read = 0;
  uint64_t total_bytes_written = 0;
  uint64_t execution_time_ms = 0;
};

// ============================================================================
// BulkLoadExecutor - Worker 端 SST 文件生成执行器
// ============================================================================
class BulkLoadExecutor {
 public:
  BulkLoadExecutor() = default;
  ~BulkLoadExecutor() = default;

  // 禁止拷贝
  BulkLoadExecutor(const BulkLoadExecutor&) = delete;
  BulkLoadExecutor& operator=(const BulkLoadExecutor&) = delete;

  // 执行 Bulk Load 分片任务
  // 完整流程：读取数据源 → 排序 → 生成 SST → 返回元数据
  BulkLoadExecuteResult Execute(const BulkLoadExecuteParams& params);

 private:
  // =========================================================================
  // 内部处理流程
  // =========================================================================

  // 步骤 1: 从数据源读取 KV 对
  // 支持: KV 文件 / SST 文件 / RDB 文件 / TendisPlus dump
  bool ReadDataSource(const BulkLoadExecuteParams& params,
                      std::vector<std::pair<std::string, std::string>>& kv_pairs,
                      std::string& error);

  // 步骤 2: 按 RocksDB bytewise comparator 排序
  void SortKeyValuePairs(
    std::vector<std::pair<std::string, std::string>>& kv_pairs);

  // 步骤 3: 使用 SstFileWriter 生成 SST 文件
  // 当单个 SST 超过 target_sst_size 时自动切分
  bool GenerateSSTFiles(
    const BulkLoadExecuteParams& params,
    const std::vector<std::pair<std::string, std::string>>& kv_pairs,
    std::vector<SSTFileMeta>& sst_files,
    std::string& error);

  // =========================================================================
  // 辅助方法
  // =========================================================================

  // 获取 RocksDB 压缩类型
  rocksdb::CompressionType GetRocksDBCompression(int32_t compression_type);

  // 构建 SST 输出文件路径
  std::string BuildSSTFilePath(const std::string& output_dir,
                               const std::string& shard_id,
                               uint32_t file_index,
                               const std::string& cf_name);

  // 从 KV 文件读取（按行 key\tvalue 格式）
  bool ReadFromKVFile(const std::string& path,
                      rocksdb::Env* env,
                      std::vector<std::pair<std::string, std::string>>& kv_pairs,
                      std::string& error);

  // 从已有 SST 文件读取
  bool ReadFromSSTFile(const std::string& path,
                       rocksdb::Env* env,
                       std::vector<std::pair<std::string, std::string>>& kv_pairs,
                       std::string& error);
};

}  // namespace remote_compaction
}  // namespace tendisplus
