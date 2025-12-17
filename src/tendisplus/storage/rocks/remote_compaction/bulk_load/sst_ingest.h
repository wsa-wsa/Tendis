// Copyright (c) 2024-present, Tencent Inc. All rights reserved.
// SST 文件 Ingest 机制

#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "bulk_load_task.h"

namespace tendisplus {
namespace bulk_load {

// ============================================================================
// Ingest 配置
// ============================================================================
struct IngestConfig {
  // 基本配置
  bool move_files = false;           // 移动而非复制
  bool allow_global_seqno = true;    // 允许全局序列号
  bool allow_blocking_flush = true;  // 允许阻塞 flush
  bool ingest_behind = false;        // 插入到最底层
  bool snapshot_consistency = true;  // 保证快照一致性
  
  // 校验配置
  bool verify_checksums = true;
  bool verify_file_size = true;
  bool verify_key_range = true;
  
  // 限速配置
  uint64_t rate_limit_bytes_per_sec = 0;  // 0 表示不限速
  
  // 重试配置
  uint32_t max_retries = 3;
  uint32_t retry_delay_ms = 1000;
  
  // 超时配置
  uint32_t ingest_timeout_sec = 3600;
  
  // 并发配置
  uint32_t max_concurrent_ingests = 1;
};

// ============================================================================
// Ingest 结果
// ============================================================================
struct IngestResult {
  bool success = false;
  std::string error_message;
  
  // 统计信息
  uint64_t files_ingested = 0;
  uint64_t bytes_ingested = 0;
  uint64_t records_ingested = 0;
  
  // 时间统计
  double duration_sec = 0;
  double verify_duration_sec = 0;
  double ingest_duration_sec = 0;
  
  // 详细结果
  struct FileResult {
    std::string file_path;
    bool success = false;
    std::string error_message;
    int target_level = -1;
    uint64_t file_size = 0;
  };
  std::vector<FileResult> file_results;
};

// ============================================================================
// SST 文件校验器
// ============================================================================
class SstFileVerifier {
 public:
  SstFileVerifier() = default;
  ~SstFileVerifier() = default;
  
  // 校验单个文件
  struct VerifyResult {
    bool valid = false;
    std::string error_message;
    
    // 文件信息
    uint64_t file_size = 0;
    std::string smallest_key;
    std::string largest_key;
    uint64_t num_entries = 0;
    std::string checksum;
    bool checksum_valid = false;
  };
  
  VerifyResult Verify(const std::string& file_path,
                      const SstFileMetadata& expected_metadata);
  
  // 批量校验
  std::vector<VerifyResult> VerifyBatch(
      const std::vector<std::pair<std::string, SstFileMetadata>>& files);
  
  // 校验 Key 范围是否与现有数据冲突
  bool CheckKeyRangeConflict(const std::string& db_path,
                             const std::string& smallest_key,
                             const std::string& largest_key,
                             std::string* conflict_info);
  
  // 校验文件完整性
  bool VerifyFileIntegrity(const std::string& file_path);
  
  // 计算校验和
  std::string ComputeChecksum(const std::string& file_path,
                              const std::string& checksum_type = "crc32c");
};

// ============================================================================
// SST 文件 Ingest 执行器
// ============================================================================
class SstIngestExecutor {
 public:
  explicit SstIngestExecutor(const IngestConfig& config);
  ~SstIngestExecutor();
  
  // 禁止拷贝
  SstIngestExecutor(const SstIngestExecutor&) = delete;
  SstIngestExecutor& operator=(const SstIngestExecutor&) = delete;
  
  // Ingest 单个文件
  IngestResult IngestFile(const std::string& db_path,
                          uint32_t cf_id,
                          const std::string& sst_file_path);
  
  // Ingest 多个文件
  IngestResult IngestFiles(const std::string& db_path,
                           uint32_t cf_id,
                           const std::vector<std::string>& sst_file_paths);
  
  // 带元数据校验的 Ingest
  IngestResult IngestWithVerify(const std::string& db_path,
                                uint32_t cf_id,
                                const std::vector<SstFileMetadata>& files);
  
  // 取消正在进行的 Ingest
  void Cancel();
  
  // 是否正在执行
  bool IsRunning() const { return running_.load(); }
  
  // 获取进度
  double GetProgress() const;
  
  // 设置进度回调
  using ProgressCallback = std::function<void(double progress, 
                                               const std::string& status)>;
  void SetProgressCallback(ProgressCallback callback);

 private:
  // 准备 Ingest
  bool PrepareIngest(const std::string& db_path,
                     const std::vector<std::string>& files);
  
  // 执行 Ingest
  bool ExecuteIngest(const std::string& db_path,
                     uint32_t cf_id,
                     const std::vector<std::string>& files,
                     IngestResult* result);
  
  // 回滚
  void Rollback(const std::vector<std::string>& files);
  
  // 限速
  void ApplyRateLimit(uint64_t bytes_processed);
  
  IngestConfig config_;
  std::atomic<bool> running_{false};
  std::atomic<bool> cancelled_{false};
  std::atomic<double> progress_{0.0};
  ProgressCallback progress_callback_;
  
  // 限速相关
  std::chrono::steady_clock::time_point rate_limit_start_;
  uint64_t rate_limit_bytes_ = 0;
};

// ============================================================================
// Ingest 协调器 (协调多个 Store 的 Ingest)
// ============================================================================
class IngestCoordinator {
 public:
  explicit IngestCoordinator(const IngestConfig& config);
  ~IngestCoordinator();
  
  // 提交 Ingest 请求
  struct IngestRequest {
    std::string request_id;
    std::string db_path;
    uint32_t store_id;
    std::vector<SstFileMetadata> files;
    std::chrono::system_clock::time_point submit_time;
  };
  
  std::string Submit(const IngestRequest& request);
  
  // 查询状态
  struct IngestStatus {
    std::string request_id;
    enum class State {
      kPending = 0,
      kVerifying = 1,
      kIngesting = 2,
      kCompleted = 3,
      kFailed = 4,
      kCancelled = 5
    };
    State state = State::kPending;
    double progress = 0.0;
    std::string message;
    IngestResult result;
  };
  
  IngestStatus GetStatus(const std::string& request_id) const;
  
  // 取消请求
  bool Cancel(const std::string& request_id);
  
  // 等待完成
  IngestResult Wait(const std::string& request_id,
                    std::chrono::seconds timeout = std::chrono::seconds(3600));
  
  // 启动/停止
  void Start();
  void Stop();

 private:
  void ProcessLoop();
  void ProcessRequest(const IngestRequest& request);
  
  IngestConfig config_;
  std::atomic<bool> running_{false};
  
  std::map<std::string, IngestRequest> pending_requests_;
  std::map<std::string, IngestStatus> status_map_;
  mutable std::mutex mutex_;
  std::condition_variable cv_;
  
  std::unique_ptr<std::thread> process_thread_;
};

// ============================================================================
// Ingest 与 Compaction 协调
// ============================================================================
class IngestCompactionCoordinator {
 public:
  IngestCompactionCoordinator() = default;
  ~IngestCompactionCoordinator() = default;
  
  // 检查是否可以 Ingest (不会导致过高写放大)
  struct IngestAdvice {
    bool should_proceed = true;
    bool should_wait_for_compaction = false;
    std::string reason;
    int suggested_level = -1;
    std::chrono::seconds suggested_wait{0};
  };
  
  IngestAdvice CheckIngestCondition(const std::string& db_path,
                                     uint32_t cf_id,
                                     uint64_t ingest_size_bytes);
  
  // 暂停 Compaction
  void PauseCompaction(const std::string& db_path);
  
  // 恢复 Compaction
  void ResumeCompaction(const std::string& db_path);
  
  // 触发 Compaction (Ingest 后)
  void TriggerCompaction(const std::string& db_path,
                         uint32_t cf_id,
                         int level);
  
  // 等待 Compaction 完成
  bool WaitForCompaction(const std::string& db_path,
                         std::chrono::seconds timeout);
  
  // 获取当前 LSM 状态
  struct LsmStatus {
    uint32_t num_levels = 0;
    std::vector<uint64_t> level_sizes;
    std::vector<uint32_t> level_file_counts;
    uint64_t pending_compaction_bytes = 0;
    bool compaction_running = false;
  };
  
  LsmStatus GetLsmStatus(const std::string& db_path, uint32_t cf_id);

 private:
  std::map<std::string, bool> compaction_paused_;
  std::mutex mutex_;
};

// ============================================================================
// Bulk Load 与在线业务协同
// ============================================================================
class BulkLoadThrottler {
 public:
  struct Config {
    // 时间窗口配置
    bool enable_time_window = false;
    std::string allowed_start_time = "00:00";  // HH:MM 格式
    std::string allowed_end_time = "06:00";
    
    // 限速配置
    uint64_t max_bytes_per_sec = 0;  // 0 表示不限速
    uint64_t max_iops = 0;
    
    // 并发配置
    uint32_t max_concurrent_tasks = 1;
    
    // 业务影响阈值
    double max_latency_impact_percent = 10.0;  // 最大延迟影响
    double max_qps_impact_percent = 5.0;       // 最大 QPS 影响
  };
  
  explicit BulkLoadThrottler(const Config& config);
  ~BulkLoadThrottler() = default;
  
  // 检查是否可以执行
  bool CanExecute() const;
  
  // 获取当前限速值
  uint64_t GetCurrentRateLimit() const;
  
  // 申请执行配额
  bool AcquireQuota(uint64_t bytes);
  
  // 释放配额
  void ReleaseQuota(uint64_t bytes);
  
  // 报告业务指标
  void ReportBusinessMetrics(double current_latency_ms,
                             double baseline_latency_ms,
                             double current_qps,
                             double baseline_qps);
  
  // 动态调整限速
  void AdjustRateLimit();

 private:
  bool IsInTimeWindow() const;
  
  Config config_;
  std::atomic<uint64_t> current_rate_limit_;
  std::atomic<uint64_t> bytes_in_flight_{0};
  std::atomic<uint32_t> tasks_in_flight_{0};
  
  // 业务指标
  std::atomic<double> latency_impact_percent_{0.0};
  std::atomic<double> qps_impact_percent_{0.0};
  
  mutable std::mutex mutex_;
};

}  // namespace bulk_load
}  // namespace tendisplus
