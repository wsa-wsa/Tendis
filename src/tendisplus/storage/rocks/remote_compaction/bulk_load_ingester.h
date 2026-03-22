// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

// BulkLoadIngester - TendisPlus 端 SST 文件注入引擎
// Based on CaaS-LSM architecture
//
// 职责：
//   ① 从共享存储下载/验证 SST 文件
//   ② 通过 RocksDB IngestExternalFile API 原子注入
//   ③ 注入结果上报 Control Plane
//
// 注入路径：
//   RocksKVStore::getBaseDB() → IngestExternalFile(cfHandle, sst_files, opts)

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"

namespace tendisplus {
namespace remote_compaction {

// ============================================================================
// SST 文件信息 (来自 Control Plane 的 QueryBulkLoadResponse)
// ============================================================================
struct IngestSSTFileInfo {
  std::string file_path;       // 共享存储上的 SST 文件路径
  std::string column_family;   // 目标 CF 名称 ("default" / "binlog")
  uint64_t file_size = 0;      // 文件大小 (bytes)
  uint64_t num_entries = 0;    // KV 条目数
  std::string smallest_key;    // 最小 key
  std::string largest_key;     // 最大 key
  std::string checksum;        // SHA256 校验和
};

// ============================================================================
// Bulk Load 注入参数
// ============================================================================
struct BulkLoadIngestParams {
  // 任务标识
  std::string task_id;
  std::string source_node_id;      // TendisPlus 节点标识

  // 目标信息
  uint32_t target_store_id = 0;
  std::string target_db_path;

  // SST 文件列表 (由 Control Plane 返回)
  std::vector<IngestSSTFileInfo> sst_files;

  // 共享文件系统 URI
  std::string shared_fs_uri;

  // 注入选项
  bool verify_checksum = true;       // 注入前校验文件校验和
  bool move_files = false;           // 是否移动文件 (而非复制)
  bool allow_global_seqno = true;    // 允许全局序列号
  bool allow_blocking_flush = true;  // 允许阻塞 flush
  bool ingest_behind = false;        // 是否注入到最底层

  // 控制平面地址 (用于上报结果)
  std::string control_plane_address;
};

// ============================================================================
// 注入执行结果
// ============================================================================
struct BulkLoadIngestResult {
  bool success = false;
  std::string error_message;

  // 统计信息
  uint32_t ingested_sst_count = 0;
  uint64_t ingested_bytes = 0;
  uint64_t ingested_rows = 0;
  uint64_t ingest_time_ms = 0;

  // 各 CF 的注入详情
  uint32_t data_cf_files = 0;
  uint32_t binlog_cf_files = 0;
};

// ============================================================================
// BulkLoadIngester - TendisPlus 端 SST 注入执行器
// ============================================================================
class BulkLoadIngester {
 public:
  BulkLoadIngester() = default;
  ~BulkLoadIngester() = default;

  // 禁止拷贝
  BulkLoadIngester(const BulkLoadIngester&) = delete;
  BulkLoadIngester& operator=(const BulkLoadIngester&) = delete;

  // 执行 SST 文件注入
  // db: RocksDB 底层指针 (通过 RocksKVStore::getBaseDB() 获取)
  // data_cf: 数据 CF 句柄 (通过 getDataColumnFamilyHandle() 获取)
  // binlog_cf: Binlog CF 句柄 (通过 getBinlogColumnFamilyHandle() 获取)
  BulkLoadIngestResult Ingest(
    rocksdb::DB* db,
    rocksdb::ColumnFamilyHandle* data_cf,
    rocksdb::ColumnFamilyHandle* binlog_cf,
    const BulkLoadIngestParams& params);

 private:
  // 验证 SST 文件存在性和完整性
  bool ValidateSSTFiles(
    rocksdb::Env* env,
    const std::vector<IngestSSTFileInfo>& sst_files,
    std::string& error);

  // 按 CF 分类 SST 文件
  void ClassifySSTFilesByCF(
    const std::vector<IngestSSTFileInfo>& sst_files,
    std::vector<std::string>& data_cf_files,
    std::vector<std::string>& binlog_cf_files);

  // 构建 IngestExternalFile 选项
  rocksdb::IngestExternalFileOptions BuildIngestOptions(
    const BulkLoadIngestParams& params);

  // 上报注入结果到 Control Plane
  void ReportIngestResult(const BulkLoadIngestParams& params,
                          const BulkLoadIngestResult& result);
};

}  // namespace remote_compaction
}  // namespace tendisplus
