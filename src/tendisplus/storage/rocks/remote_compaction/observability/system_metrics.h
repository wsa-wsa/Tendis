// Copyright (c) 2024-present, Tencent Inc. All rights reserved.
// 系统级监控指标采集

#pragma once

#include <atomic>
#include <chrono>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "metrics.h"

namespace tendisplus {
namespace observability {

// ============================================================================
// 系统资源信息
// ============================================================================
struct CpuInfo {
  uint32_t num_cores = 0;
  double usage_percent = 0.0;       // 总体 CPU 使用率
  double user_percent = 0.0;        // 用户态使用率
  double system_percent = 0.0;      // 内核态使用率
  double iowait_percent = 0.0;      // IO 等待
  std::vector<double> per_core_usage;  // 每核使用率
  double load_avg_1min = 0.0;
  double load_avg_5min = 0.0;
  double load_avg_15min = 0.0;
};

struct MemoryInfo {
  uint64_t total_bytes = 0;
  uint64_t available_bytes = 0;
  uint64_t used_bytes = 0;
  uint64_t cached_bytes = 0;
  uint64_t buffers_bytes = 0;
  double usage_percent = 0.0;
  
  // Swap
  uint64_t swap_total_bytes = 0;
  uint64_t swap_used_bytes = 0;
  double swap_usage_percent = 0.0;
};

struct DiskInfo {
  std::string device;
  std::string mount_point;
  std::string filesystem;
  uint64_t total_bytes = 0;
  uint64_t available_bytes = 0;
  uint64_t used_bytes = 0;
  double usage_percent = 0.0;
  
  // IO 统计
  uint64_t read_bytes_per_sec = 0;
  uint64_t write_bytes_per_sec = 0;
  uint64_t read_iops = 0;
  uint64_t write_iops = 0;
  double io_util_percent = 0.0;
  double avg_queue_length = 0.0;
  double avg_wait_ms = 0.0;
};

struct NetworkInfo {
  std::string interface;
  uint64_t rx_bytes_per_sec = 0;
  uint64_t tx_bytes_per_sec = 0;
  uint64_t rx_packets_per_sec = 0;
  uint64_t tx_packets_per_sec = 0;
  uint64_t rx_errors = 0;
  uint64_t tx_errors = 0;
  uint64_t rx_dropped = 0;
  uint64_t tx_dropped = 0;
  
  // TCP 连接统计
  uint32_t tcp_established = 0;
  uint32_t tcp_time_wait = 0;
  uint32_t tcp_close_wait = 0;
};

struct ProcessInfo {
  int pid = 0;
  std::string name;
  double cpu_percent = 0.0;
  uint64_t memory_bytes = 0;
  double memory_percent = 0.0;
  uint32_t num_threads = 0;
  uint32_t num_fds = 0;
  uint64_t read_bytes = 0;
  uint64_t write_bytes = 0;
  std::chrono::seconds uptime{0};
};

// ============================================================================
// 系统指标采集器
// ============================================================================
class SystemMetricsCollector {
 public:
  SystemMetricsCollector();
  ~SystemMetricsCollector();
  
  // 单例访问
  static SystemMetricsCollector& Instance();
  
  // 启动/停止采集
  void Start(std::chrono::milliseconds interval = std::chrono::seconds(10));
  void Stop();
  bool IsRunning() const { return running_.load(); }
  
  // 获取最新指标
  CpuInfo GetCpuInfo() const;
  MemoryInfo GetMemoryInfo() const;
  std::vector<DiskInfo> GetDiskInfo() const;
  std::vector<NetworkInfo> GetNetworkInfo() const;
  ProcessInfo GetProcessInfo() const;
  
  // 手动触发采集
  void Collect();

 private:
  void CollectLoop();
  
  // 采集各类指标
  void CollectCpuMetrics();
  void CollectMemoryMetrics();
  void CollectDiskMetrics();
  void CollectNetworkMetrics();
  void CollectProcessMetrics();
  
  // 读取 /proc 文件
  std::string ReadProcFile(const std::string& path) const;
  std::vector<std::string> ReadProcLines(const std::string& path) const;
  
  std::atomic<bool> running_{false};
  std::chrono::milliseconds interval_{10000};
  std::unique_ptr<std::thread> collect_thread_;
  
  // 缓存的指标数据
  CpuInfo cpu_info_;
  MemoryInfo memory_info_;
  std::vector<DiskInfo> disk_info_;
  std::vector<NetworkInfo> network_info_;
  ProcessInfo process_info_;
  mutable std::mutex mutex_;
  
  // 用于计算增量的历史数据
  struct CpuStat {
    uint64_t user = 0, nice = 0, system = 0, idle = 0;
    uint64_t iowait = 0, irq = 0, softirq = 0;
  };
  CpuStat prev_cpu_stat_;
  std::vector<CpuStat> prev_per_core_stat_;
  
  struct DiskStat {
    uint64_t read_sectors = 0;
    uint64_t write_sectors = 0;
    uint64_t read_ios = 0;
    uint64_t write_ios = 0;
    uint64_t io_ticks = 0;
    uint64_t time_in_queue = 0;
  };
  std::map<std::string, DiskStat> prev_disk_stat_;
  
  struct NetStat {
    uint64_t rx_bytes = 0;
    uint64_t tx_bytes = 0;
    uint64_t rx_packets = 0;
    uint64_t tx_packets = 0;
  };
  std::map<std::string, NetStat> prev_net_stat_;
  
  std::chrono::steady_clock::time_point prev_collect_time_;
};

// ============================================================================
// 系统指标 Prometheus 导出
// ============================================================================
class SystemMetricsExporter {
 public:
  SystemMetricsExporter();
  ~SystemMetricsExporter() = default;
  
  static SystemMetricsExporter& Instance();
  
  // 更新指标 (从 SystemMetricsCollector 获取数据并更新 Prometheus 指标)
  void Update();
  
  // 注册所有系统指标
  void RegisterMetrics();

 private:
  // CPU 指标
  std::shared_ptr<Gauge> cpu_usage_percent_;
  std::shared_ptr<GaugeVec> cpu_usage_per_core_;
  std::shared_ptr<Gauge> cpu_load_1min_;
  std::shared_ptr<Gauge> cpu_load_5min_;
  std::shared_ptr<Gauge> cpu_load_15min_;
  
  // 内存指标
  std::shared_ptr<Gauge> memory_total_bytes_;
  std::shared_ptr<Gauge> memory_available_bytes_;
  std::shared_ptr<Gauge> memory_used_bytes_;
  std::shared_ptr<Gauge> memory_usage_percent_;
  std::shared_ptr<Gauge> swap_total_bytes_;
  std::shared_ptr<Gauge> swap_used_bytes_;
  
  // 磁盘指标
  std::shared_ptr<GaugeVec> disk_total_bytes_;
  std::shared_ptr<GaugeVec> disk_available_bytes_;
  std::shared_ptr<GaugeVec> disk_usage_percent_;
  std::shared_ptr<GaugeVec> disk_read_bytes_per_sec_;
  std::shared_ptr<GaugeVec> disk_write_bytes_per_sec_;
  std::shared_ptr<GaugeVec> disk_io_util_percent_;
  
  // 网络指标
  std::shared_ptr<GaugeVec> network_rx_bytes_per_sec_;
  std::shared_ptr<GaugeVec> network_tx_bytes_per_sec_;
  std::shared_ptr<GaugeVec> network_rx_errors_;
  std::shared_ptr<GaugeVec> network_tx_errors_;
  std::shared_ptr<Gauge> tcp_connections_established_;
  std::shared_ptr<Gauge> tcp_connections_time_wait_;
  
  // 进程指标
  std::shared_ptr<Gauge> process_cpu_percent_;
  std::shared_ptr<Gauge> process_memory_bytes_;
  std::shared_ptr<Gauge> process_num_threads_;
  std::shared_ptr<Gauge> process_num_fds_;
  std::shared_ptr<Gauge> process_uptime_seconds_;
};

// ============================================================================
// 业务级指标
// ============================================================================
class BusinessMetrics {
 public:
  BusinessMetrics();
  ~BusinessMetrics() = default;
  
  static BusinessMetrics& Instance();
  
  // QPS 影响
  void RecordQpsImpact(double baseline_qps, double current_qps);
  
  // 延迟影响
  void RecordLatencyImpact(double baseline_p99_ms, double current_p99_ms);
  
  // SLA 达成率
  void RecordSlaCompliance(bool met);
  
  // 数据处理量
  void RecordDataProcessed(uint64_t bytes, uint64_t records);
  
  // 空间节省
  void RecordSpaceSaved(uint64_t bytes_before, uint64_t bytes_after);
  
  // 远程/本地任务比例
  void RecordTaskLocation(bool is_remote);
  
  // 卸载效率
  void RecordOffloadEfficiency(double cpu_saved_percent, double io_saved_percent);
  
  // 可用性
  void RecordAvailability(bool available);
  
  // 资源成本
  void RecordResourceCost(double cpu_hours, double memory_gb_hours, 
                          double storage_gb_hours);

 private:
  // QPS 相关
  std::shared_ptr<Gauge> qps_baseline_;
  std::shared_ptr<Gauge> qps_current_;
  std::shared_ptr<Gauge> qps_impact_percent_;
  
  // 延迟相关
  std::shared_ptr<Gauge> latency_p99_baseline_ms_;
  std::shared_ptr<Gauge> latency_p99_current_ms_;
  std::shared_ptr<Gauge> latency_impact_percent_;
  
  // SLA
  std::shared_ptr<Counter> sla_checks_total_;
  std::shared_ptr<Counter> sla_met_total_;
  std::shared_ptr<Gauge> sla_compliance_rate_;
  
  // 数据处理
  std::shared_ptr<Counter> data_bytes_processed_total_;
  std::shared_ptr<Counter> data_records_processed_total_;
  std::shared_ptr<Gauge> data_throughput_bytes_per_sec_;
  
  // 空间节省
  std::shared_ptr<Counter> space_saved_bytes_total_;
  std::shared_ptr<Gauge> space_saving_ratio_;
  
  // 任务分布
  std::shared_ptr<Counter> tasks_remote_total_;
  std::shared_ptr<Counter> tasks_local_total_;
  std::shared_ptr<Gauge> remote_task_ratio_;
  
  // 卸载效率
  std::shared_ptr<Gauge> offload_cpu_saved_percent_;
  std::shared_ptr<Gauge> offload_io_saved_percent_;
  
  // 可用性
  std::shared_ptr<Counter> availability_checks_total_;
  std::shared_ptr<Counter> availability_up_total_;
  std::shared_ptr<Gauge> availability_rate_;
  
  // 成本
  std::shared_ptr<Counter> cost_cpu_hours_total_;
  std::shared_ptr<Counter> cost_memory_gb_hours_total_;
  std::shared_ptr<Counter> cost_storage_gb_hours_total_;
};

// ============================================================================
// 性能指标
// ============================================================================
class PerformanceMetrics {
 public:
  PerformanceMetrics();
  ~PerformanceMetrics() = default;
  
  static PerformanceMetrics& Instance();
  
  // 端到端延迟
  void RecordEndToEndLatency(double latency_sec);
  
  // 数据传输速率
  void RecordTransferRate(uint64_t bytes, double duration_sec);
  
  // Compaction 速率
  void RecordCompactionRate(uint64_t bytes, double duration_sec);
  
  // Ingest 速率
  void RecordIngestRate(uint64_t bytes, double duration_sec);
  
  // SST 文件大小
  void RecordSstFileSize(uint64_t bytes);
  
  // 读/写/空间放大
  void RecordReadAmplification(double ratio);
  void RecordWriteAmplification(double ratio);
  void RecordSpaceAmplification(double ratio);
  
  // 缓存命中率
  void RecordCacheHit(bool hit);

 private:
  // 延迟
  std::shared_ptr<Histogram> end_to_end_latency_seconds_;
  std::shared_ptr<Gauge> end_to_end_latency_p99_;
  
  // 传输速率
  std::shared_ptr<Gauge> transfer_rate_bytes_per_sec_;
  std::shared_ptr<Counter> transfer_bytes_total_;
  
  // Compaction 速率
  std::shared_ptr<Gauge> compaction_rate_bytes_per_sec_;
  std::shared_ptr<Histogram> compaction_duration_seconds_;
  
  // Ingest 速率
  std::shared_ptr<Gauge> ingest_rate_bytes_per_sec_;
  std::shared_ptr<Histogram> ingest_duration_seconds_;
  
  // SST 文件
  std::shared_ptr<Histogram> sst_file_size_bytes_;
  std::shared_ptr<Counter> sst_files_created_total_;
  
  // 放大因子
  std::shared_ptr<Gauge> read_amplification_;
  std::shared_ptr<Gauge> write_amplification_;
  std::shared_ptr<Gauge> space_amplification_;
  std::shared_ptr<Histogram> write_amplification_histogram_;
  
  // 缓存
  std::shared_ptr<Counter> cache_hits_total_;
  std::shared_ptr<Counter> cache_misses_total_;
  std::shared_ptr<Gauge> cache_hit_rate_;
};

}  // namespace observability
}  // namespace tendisplus
