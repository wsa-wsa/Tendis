// Copyright (c) 2024-present, Tencent Inc. All rights reserved.
// 远程 Bulk Load 任务定义

#pragma once

#include <chrono>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "../control_plane/task_model.h"

namespace tendisplus {
namespace bulk_load {

using namespace control_plane;

// ============================================================================
// 数据分片信息
// ============================================================================
struct DataShard {
  std::string shard_id;
  std::string source_path;          // 源数据路径
  std::string start_key;            // 起始 Key
  std::string end_key;              // 结束 Key
  uint64_t estimated_size_bytes = 0;
  uint64_t estimated_records = 0;
  uint32_t target_store_id = 0;     // 目标 Store ID
  
  // 分片状态
  enum class State {
    kPending = 0,
    kProcessing = 1,
    kCompleted = 2,
    kFailed = 3
  };
  State state = State::kPending;
  
  // 处理结果
  std::string output_sst_path;
  std::string checksum;
  uint64_t actual_size_bytes = 0;
  uint64_t actual_records = 0;
  std::string error_message;
};

// ============================================================================
// SST 文件元数据
// ============================================================================
struct SstFileMetadata {
  std::string file_path;
  std::string file_name;
  uint64_t file_size = 0;
  std::string smallest_key;
  std::string largest_key;
  uint64_t num_entries = 0;
  uint64_t num_deletions = 0;
  int target_level = -1;            // -1 表示自动选择
  std::string checksum;
  std::string checksum_type = "crc32c";
  
  // 压缩信息
  std::string compression_type;
  uint64_t raw_size = 0;
  
  // 时间戳
  uint64_t creation_time = 0;
  uint64_t oldest_key_time = 0;
  uint64_t newest_key_time = 0;
  
  // 序列化
  std::string Serialize() const;
  static SstFileMetadata Deserialize(const std::string& data);
};

// ============================================================================
// Bulk Load 任务参数
// ============================================================================
struct BulkLoadParams {
  // 数据源配置
  std::string source_type = "file";  // file, hdfs, s3, nfs
  std::string source_path;
  std::map<std::string, std::string> source_options;
  
  // 数据格式
  std::string data_format = "sst";   // sst, csv, json, binary
  std::string key_format;
  std::string value_format;
  std::string delimiter = "\t";
  
  // 分片配置
  uint32_t num_shards = 0;           // 0 表示自动计算
  uint64_t shard_size_bytes = 256 * 1024 * 1024;  // 256MB
  std::vector<DataShard> shards;
  
  // 目标配置
  std::string target_db;
  uint32_t target_store_id = 0;
  std::vector<uint32_t> target_store_ids;  // 多 Store 导入
  
  // 处理配置
  bool sort_data = true;
  bool verify_checksum = true;
  bool allow_duplicates = false;
  std::string compression = "lz4";
  uint32_t block_size = 4096;
  
  // 限速配置
  uint64_t rate_limit_bytes_per_sec = 0;  // 0 表示不限速
  
  // Ingest 配置
  bool move_files = false;           // 是否移动而非复制
  bool ingest_behind = false;        // 是否插入到最底层
  bool allow_global_seqno = true;
  bool allow_blocking_flush = true;
  
  // 回调配置
  std::string callback_url;          // 完成后回调 URL
};

// ============================================================================
// Bulk Load 任务结果
// ============================================================================
struct BulkLoadResult {
  bool success = false;
  std::string error_message;
  
  // 统计信息
  uint64_t total_bytes_processed = 0;
  uint64_t total_records_processed = 0;
  uint64_t total_sst_files = 0;
  uint64_t total_sst_bytes = 0;
  
  // 分片结果
  uint32_t shards_total = 0;
  uint32_t shards_completed = 0;
  uint32_t shards_failed = 0;
  
  // SST 文件列表
  std::vector<SstFileMetadata> sst_files;
  
  // 时间统计
  std::chrono::system_clock::time_point start_time;
  std::chrono::system_clock::time_point end_time;
  double duration_sec = 0;
  double throughput_bytes_per_sec = 0;
  
  // 序列化
  std::string Serialize() const;
  static BulkLoadResult Deserialize(const std::string& data);
};

// ============================================================================
// Bulk Load 任务
// ============================================================================
class BulkLoadTask : public BackgroundTask {
 public:
  BulkLoadTask(const std::string& source_node_id,
               const std::string& db_name,
               uint32_t store_id,
               const BulkLoadParams& params);
  ~BulkLoadTask() override = default;
  
  // 获取参数
  const BulkLoadParams& GetParams() const { return bulk_load_params_; }
  
  // 获取/设置结果
  const BulkLoadResult& GetBulkLoadResult() const { return bulk_load_result_; }
  void SetBulkLoadResult(const BulkLoadResult& result);
  
  // 分片管理
  void AddShard(const DataShard& shard);
  void UpdateShardState(const std::string& shard_id, DataShard::State state);
  const std::vector<DataShard>& GetShards() const;
  DataShard* GetShard(const std::string& shard_id);
  
  // 进度计算
  double GetProgress() const override;
  
  // 获取待处理的分片
  std::vector<DataShard*> GetPendingShards();
  
  // 获取 SST 文件列表
  std::vector<SstFileMetadata> GetCompletedSstFiles() const;

 private:
  BulkLoadParams bulk_load_params_;
  BulkLoadResult bulk_load_result_;
  std::vector<DataShard> shards_;
  mutable std::mutex shards_mutex_;
};

// ============================================================================
// Bulk Load 子任务 (Worker 执行的单个分片任务)
// ============================================================================
class BulkLoadSubTask {
 public:
  BulkLoadSubTask(const std::string& parent_task_id,
                  const DataShard& shard,
                  const BulkLoadParams& params);
  ~BulkLoadSubTask() = default;
  
  const std::string& TaskId() const { return task_id_; }
  const std::string& ParentTaskId() const { return parent_task_id_; }
  const DataShard& GetShard() const { return shard_; }
  const BulkLoadParams& GetParams() const { return params_; }
  
  // 执行状态
  enum class State {
    kPending = 0,
    kReading = 1,
    kSorting = 2,
    kBuilding = 3,
    kUploading = 4,
    kCompleted = 5,
    kFailed = 6
  };
  
  State GetState() const { return state_.load(); }
  void SetState(State state) { state_.store(state); }
  
  // 结果
  struct Result {
    bool success = false;
    std::string error_message;
    SstFileMetadata sst_metadata;
    uint64_t bytes_read = 0;
    uint64_t records_read = 0;
    double duration_sec = 0;
  };
  
  const Result& GetResult() const { return result_; }
  void SetResult(const Result& result) { result_ = result; }

 private:
  std::string task_id_;
  std::string parent_task_id_;
  DataShard shard_;
  BulkLoadParams params_;
  std::atomic<State> state_{State::kPending};
  Result result_;
};

// ============================================================================
// 数据分片器
// ============================================================================
class DataShardSplitter {
 public:
  DataShardSplitter() = default;
  ~DataShardSplitter() = default;
  
  // 根据配置自动分片
  std::vector<DataShard> Split(const BulkLoadParams& params);
  
  // 按大小分片
  std::vector<DataShard> SplitBySize(const std::string& source_path,
                                      uint64_t shard_size_bytes);
  
  // 按 Key 范围分片
  std::vector<DataShard> SplitByKeyRange(const std::string& source_path,
                                          uint32_t num_shards);
  
  // 按目标 Store 分片
  std::vector<DataShard> SplitByStore(const std::string& source_path,
                                       const std::vector<uint32_t>& store_ids);

 private:
  // 扫描数据获取 Key 分布
  struct KeyDistribution {
    std::string min_key;
    std::string max_key;
    uint64_t total_size = 0;
    uint64_t total_records = 0;
    std::vector<std::pair<std::string, uint64_t>> key_samples;
  };
  
  KeyDistribution ScanKeyDistribution(const std::string& source_path);
  
  // 计算分片边界
  std::vector<std::string> ComputeShardBoundaries(
      const KeyDistribution& dist, uint32_t num_shards);
};

}  // namespace bulk_load
}  // namespace tendisplus
