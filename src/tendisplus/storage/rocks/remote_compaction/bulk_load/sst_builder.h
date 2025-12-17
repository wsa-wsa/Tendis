// Copyright (c) 2024-present, Tencent Inc. All rights reserved.
// SST 文件构建器

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "bulk_load_task.h"

namespace tendisplus {
namespace bulk_load {

// ============================================================================
// SST 构建配置
// ============================================================================
struct SstBuilderConfig {
  // 文件配置
  std::string output_directory;
  std::string file_name_prefix = "bulk_load";
  uint64_t max_file_size = 64 * 1024 * 1024;  // 64MB
  
  // 压缩配置
  std::string compression = "lz4";
  int compression_level = -1;  // -1 使用默认
  
  // Block 配置
  uint32_t block_size = 4096;
  bool enable_bloom_filter = true;
  int bloom_filter_bits_per_key = 10;
  
  // 其他配置
  bool compute_checksum = true;
  std::string checksum_type = "crc32c";
  bool verify_after_build = true;
};

// ============================================================================
// Key-Value 对
// ============================================================================
struct KeyValue {
  std::string key;
  std::string value;
  uint64_t sequence_number = 0;
  enum class Type {
    kPut = 0,
    kDelete = 1,
    kMerge = 2
  };
  Type type = Type::kPut;
  
  bool operator<(const KeyValue& other) const {
    return key < other.key;
  }
};

// ============================================================================
// SST 文件构建器
// ============================================================================
class SstFileBuilder {
 public:
  explicit SstFileBuilder(const SstBuilderConfig& config);
  ~SstFileBuilder();
  
  // 禁止拷贝
  SstFileBuilder(const SstFileBuilder&) = delete;
  SstFileBuilder& operator=(const SstFileBuilder&) = delete;
  
  // 开始构建新文件
  bool Open(const std::string& file_path);
  
  // 添加 Key-Value (必须按 Key 排序添加)
  bool Add(const KeyValue& kv);
  bool Add(const std::string& key, const std::string& value);
  
  // 完成构建
  bool Finish();
  
  // 放弃构建
  void Abandon();
  
  // 获取构建的文件元数据
  SstFileMetadata GetMetadata() const;
  
  // 获取当前文件大小
  uint64_t GetFileSize() const;
  
  // 获取已添加的记录数
  uint64_t GetNumEntries() const;
  
  // 是否已满 (达到 max_file_size)
  bool IsFull() const;
  
  // 获取错误信息
  const std::string& GetError() const { return error_; }

 private:
  SstBuilderConfig config_;
  std::string current_file_path_;
  std::string error_;
  
  // RocksDB SstFileWriter 封装
  // 实际实现中使用 rocksdb::SstFileWriter
  struct Impl;
  std::unique_ptr<Impl> impl_;
  
  // 统计信息
  uint64_t num_entries_ = 0;
  uint64_t num_deletions_ = 0;
  uint64_t file_size_ = 0;
  std::string smallest_key_;
  std::string largest_key_;
};

// ============================================================================
// 批量 SST 构建器 (自动分割文件)
// ============================================================================
class BatchSstBuilder {
 public:
  explicit BatchSstBuilder(const SstBuilderConfig& config);
  ~BatchSstBuilder();
  
  // 添加 Key-Value (自动处理文件分割)
  bool Add(const KeyValue& kv);
  bool Add(const std::string& key, const std::string& value);
  
  // 批量添加 (数据必须已排序)
  bool AddBatch(const std::vector<KeyValue>& kvs);
  
  // 完成所有文件构建
  bool Finish();
  
  // 获取所有构建的文件元数据
  std::vector<SstFileMetadata> GetAllMetadata() const;
  
  // 获取统计信息
  uint64_t GetTotalBytes() const;
  uint64_t GetTotalEntries() const;
  uint32_t GetFileCount() const;
  
  // 获取错误信息
  const std::string& GetError() const { return error_; }

 private:
  bool RotateFile();
  std::string GenerateFileName();
  
  SstBuilderConfig config_;
  std::unique_ptr<SstFileBuilder> current_builder_;
  std::vector<SstFileMetadata> completed_files_;
  std::string error_;
  uint32_t file_counter_ = 0;
  uint64_t total_bytes_ = 0;
  uint64_t total_entries_ = 0;
};

// ============================================================================
// 数据排序器
// ============================================================================
class DataSorter {
 public:
  struct Config {
    uint64_t memory_limit_bytes = 256 * 1024 * 1024;  // 256MB
    std::string temp_directory = "/tmp";
    uint32_t num_threads = 4;
    bool remove_duplicates = false;
  };
  
  explicit DataSorter(const Config& config);
  ~DataSorter();
  
  // 添加数据
  void Add(const KeyValue& kv);
  void Add(const std::string& key, const std::string& value);
  
  // 排序并获取迭代器
  class Iterator {
   public:
    virtual ~Iterator() = default;
    virtual bool Valid() const = 0;
    virtual void Next() = 0;
    virtual const KeyValue& Current() const = 0;
  };
  
  std::unique_ptr<Iterator> Sort();
  
  // 获取统计信息
  uint64_t GetTotalEntries() const { return total_entries_; }
  uint64_t GetTotalBytes() const { return total_bytes_; }
  bool IsSpilled() const { return spilled_; }

 private:
  void SpillToDisk();
  void MergeSpilledFiles();
  
  Config config_;
  std::vector<KeyValue> buffer_;
  uint64_t buffer_bytes_ = 0;
  std::vector<std::string> spill_files_;
  bool spilled_ = false;
  uint64_t total_entries_ = 0;
  uint64_t total_bytes_ = 0;
};

// ============================================================================
// 数据读取器接口
// ============================================================================
class DataReader {
 public:
  virtual ~DataReader() = default;
  
  // 打开数据源
  virtual bool Open(const std::string& path,
                    const std::map<std::string, std::string>& options) = 0;
  
  // 读取下一条记录
  virtual bool Read(KeyValue* kv) = 0;
  
  // 是否还有数据
  virtual bool HasNext() const = 0;
  
  // 关闭
  virtual void Close() = 0;
  
  // 获取错误信息
  virtual const std::string& GetError() const = 0;
  
  // 获取进度 (0.0 - 1.0)
  virtual double GetProgress() const = 0;
};

// ============================================================================
// 文件数据读取器
// ============================================================================
class FileDataReader : public DataReader {
 public:
  FileDataReader() = default;
  ~FileDataReader() override;
  
  bool Open(const std::string& path,
            const std::map<std::string, std::string>& options) override;
  bool Read(KeyValue* kv) override;
  bool HasNext() const override;
  void Close() override;
  const std::string& GetError() const override { return error_; }
  double GetProgress() const override;

 private:
  std::string path_;
  std::string error_;
  uint64_t file_size_ = 0;
  uint64_t bytes_read_ = 0;
  
  // 文件句柄
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

// ============================================================================
// SST 数据读取器
// ============================================================================
class SstDataReader : public DataReader {
 public:
  SstDataReader() = default;
  ~SstDataReader() override;
  
  bool Open(const std::string& path,
            const std::map<std::string, std::string>& options) override;
  bool Read(KeyValue* kv) override;
  bool HasNext() const override;
  void Close() override;
  const std::string& GetError() const override { return error_; }
  double GetProgress() const override;

 private:
  std::string path_;
  std::string error_;
  uint64_t total_entries_ = 0;
  uint64_t entries_read_ = 0;
  
  // RocksDB SstFileReader 封装
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

// ============================================================================
// CSV 数据读取器
// ============================================================================
class CsvDataReader : public DataReader {
 public:
  CsvDataReader() = default;
  ~CsvDataReader() override;
  
  bool Open(const std::string& path,
            const std::map<std::string, std::string>& options) override;
  bool Read(KeyValue* kv) override;
  bool HasNext() const override;
  void Close() override;
  const std::string& GetError() const override { return error_; }
  double GetProgress() const override;

 private:
  std::string path_;
  std::string error_;
  std::string delimiter_ = ",";
  int key_column_ = 0;
  int value_column_ = 1;
  bool has_header_ = false;
  uint64_t file_size_ = 0;
  uint64_t bytes_read_ = 0;
  
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

// ============================================================================
// 数据读取器工厂
// ============================================================================
class DataReaderFactory {
 public:
  static std::unique_ptr<DataReader> Create(const std::string& format);
  
  // 注册自定义读取器
  using CreatorFunc = std::function<std::unique_ptr<DataReader>()>;
  static void Register(const std::string& format, CreatorFunc creator);

 private:
  static std::map<std::string, CreatorFunc>& GetRegistry();
};

}  // namespace bulk_load
}  // namespace tendisplus
