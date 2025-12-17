// Copyright (c) 2024-present, Tencent Inc. All rights reserved.
// 实验评估框架

#pragma once

#include <string>
#include <vector>
#include <memory>
#include <functional>
#include <chrono>
#include <unordered_map>
#include <atomic>
#include <mutex>

namespace tendisplus {
namespace observability {

// ============================================================================
// 实验场景类型
// ============================================================================
enum class ExperimentScenario {
  kHighWritePressure = 0,      // 高写入压力场景
  kReadWriteMixed = 1,         // 读写混合场景
  kLargeBulkLoad = 2,          // 大规模 Bulk Load 场景
  kMultiTaskMixed = 3,         // 多任务混合场景
  kCustom = 4                  // 自定义场景
};

// ============================================================================
// 实验配置
// ============================================================================
struct ExperimentConfig {
  std::string name;
  std::string description;
  ExperimentScenario scenario;
  
  // 通用配置
  std::chrono::seconds duration;
  std::chrono::seconds warmup_duration;
  std::chrono::seconds cooldown_duration;
  
  // 负载配置
  uint64_t target_qps;
  double read_ratio;  // 0.0 - 1.0
  double write_ratio;
  
  // 数据配置
  uint64_t key_count;
  uint32_t key_size;
  uint32_t value_size;
  
  // 远程 Compaction 配置
  bool enable_remote_compaction;
  double remote_compaction_threshold_mb;
  uint32_t max_remote_workers;
  
  // Bulk Load 配置
  uint64_t bulk_load_data_size_mb;
  uint32_t bulk_load_concurrency;
  
  // 资源限制
  double cpu_limit_percent;
  double memory_limit_percent;
  double io_limit_mbps;
  
  ExperimentConfig() 
      : scenario(ExperimentScenario::kCustom),
        duration(3600),
        warmup_duration(300),
        cooldown_duration(60),
        target_qps(10000),
        read_ratio(0.5),
        write_ratio(0.5),
        key_count(10000000),
        key_size(32),
        value_size(256),
        enable_remote_compaction(true),
        remote_compaction_threshold_mb(64),
        max_remote_workers(4),
        bulk_load_data_size_mb(10240),
        bulk_load_concurrency(4),
        cpu_limit_percent(80),
        memory_limit_percent(80),
        io_limit_mbps(500) {}
};

// ============================================================================
// 实验指标
// ============================================================================
struct ExperimentMetrics {
  // 延迟指标 (微秒)
  double latency_avg_us;
  double latency_p50_us;
  double latency_p95_us;
  double latency_p99_us;
  double latency_max_us;
  
  // 吞吐量指标
  double qps_avg;
  double qps_peak;
  double throughput_mbps;
  
  // 任务指标
  uint64_t tasks_submitted;
  uint64_t tasks_completed;
  uint64_t tasks_failed;
  uint64_t tasks_retried;
  double task_success_rate;
  double avg_task_duration_sec;
  double avg_queue_wait_sec;
  
  // 资源指标
  double cpu_usage_avg;
  double cpu_usage_peak;
  double memory_usage_avg;
  double memory_usage_peak;
  double disk_io_avg_mbps;
  double disk_io_peak_mbps;
  double network_io_avg_mbps;
  
  // 放大因子
  double write_amplification;
  double read_amplification;
  double space_amplification;
  
  // 业务影响
  double qps_impact_percent;
  double latency_impact_percent;
  double sla_compliance_rate;
  
  // Bulk Load 特定
  double bulk_load_throughput_mbps;
  double bulk_load_duration_sec;
  uint64_t sst_files_generated;
  uint64_t sst_files_ingested;
  
  // 卸载效率
  double cpu_offload_percent;
  double io_offload_percent;
  double remote_task_ratio;
  
  ExperimentMetrics() { memset(this, 0, sizeof(*this)); }
};

// ============================================================================
// 实验结果
// ============================================================================
struct ExperimentResult {
  std::string experiment_id;
  ExperimentConfig config;
  ExperimentMetrics baseline_metrics;  // 远程 Compaction 关闭
  ExperimentMetrics experiment_metrics; // 远程 Compaction 开启
  
  std::chrono::system_clock::time_point start_time;
  std::chrono::system_clock::time_point end_time;
  
  bool success;
  std::string error_message;
  std::vector<std::string> warnings;
  
  // 对比分析
  double latency_improvement_percent;
  double throughput_improvement_percent;
  double resource_saving_percent;
  
  std::string GenerateReport() const;
};

// ============================================================================
// 预定义实验场景
// ============================================================================
class ExperimentScenarios {
 public:
  // =========================================================================
  // 场景1：高写入压力场景
  // =========================================================================
  static ExperimentConfig GetHighWritePressureConfig() {
    ExperimentConfig config;
    config.name = "HighWritePressure";
    config.description = "高写入吞吐下对比远程Compaction开关前后的业务延迟、写放大与任务完成时间";
    config.scenario = ExperimentScenario::kHighWritePressure;
    
    config.duration = std::chrono::seconds(3600);
    config.warmup_duration = std::chrono::seconds(300);
    config.cooldown_duration = std::chrono::seconds(120);
    
    // 高写入配置
    config.target_qps = 50000;
    config.read_ratio = 0.1;
    config.write_ratio = 0.9;
    
    config.key_count = 50000000;
    config.key_size = 32;
    config.value_size = 512;
    
    config.enable_remote_compaction = true;
    config.remote_compaction_threshold_mb = 32;
    config.max_remote_workers = 8;
    
    return config;
  }
  
  // =========================================================================
  // 场景2：读写混合场景
  // =========================================================================
  static ExperimentConfig GetReadWriteMixedConfig() {
    ExperimentConfig config;
    config.name = "ReadWriteMixed";
    config.description = "读写混合负载下考察统一调度与限速策略对业务P95/P99延迟和后台任务完成时间的影响";
    config.scenario = ExperimentScenario::kReadWriteMixed;
    
    config.duration = std::chrono::seconds(3600);
    config.warmup_duration = std::chrono::seconds(300);
    config.cooldown_duration = std::chrono::seconds(120);
    
    // 读写混合配置
    config.target_qps = 30000;
    config.read_ratio = 0.7;
    config.write_ratio = 0.3;
    
    config.key_count = 100000000;
    config.key_size = 32;
    config.value_size = 256;
    
    config.enable_remote_compaction = true;
    config.remote_compaction_threshold_mb = 64;
    config.max_remote_workers = 4;
    
    return config;
  }
  
  // =========================================================================
  // 场景3：大规模 Bulk Load 场景
  // =========================================================================
  static ExperimentConfig GetLargeBulkLoadConfig() {
    ExperimentConfig config;
    config.name = "LargeBulkLoad";
    config.description = "模拟历史数据回灌或跨集群迁移场景，对比传统Bulk Load与远程Bulk Load的性能差异与资源开销";
    config.scenario = ExperimentScenario::kLargeBulkLoad;
    
    config.duration = std::chrono::seconds(7200);
    config.warmup_duration = std::chrono::seconds(60);
    config.cooldown_duration = std::chrono::seconds(300);
    
    // Bulk Load 配置
    config.target_qps = 5000;  // 背景负载
    config.read_ratio = 0.8;
    config.write_ratio = 0.2;
    
    config.key_count = 500000000;
    config.key_size = 32;
    config.value_size = 512;
    
    config.enable_remote_compaction = true;
    config.bulk_load_data_size_mb = 102400;  // 100GB
    config.bulk_load_concurrency = 8;
    config.max_remote_workers = 16;
    
    return config;
  }
  
  // =========================================================================
  // 场景4：多任务混合场景
  // =========================================================================
  static ExperimentConfig GetMultiTaskMixedConfig() {
    ExperimentConfig config;
    config.name = "MultiTaskMixed";
    config.description = "多个后台任务并发执行时，评估资源感知调度策略在任务完成时间、节点负载均衡度和业务稳定性方面的表现";
    config.scenario = ExperimentScenario::kMultiTaskMixed;
    
    config.duration = std::chrono::seconds(5400);
    config.warmup_duration = std::chrono::seconds(300);
    config.cooldown_duration = std::chrono::seconds(180);
    
    // 混合负载配置
    config.target_qps = 20000;
    config.read_ratio = 0.5;
    config.write_ratio = 0.5;
    
    config.key_count = 200000000;
    config.key_size = 32;
    config.value_size = 256;
    
    config.enable_remote_compaction = true;
    config.remote_compaction_threshold_mb = 48;
    config.bulk_load_data_size_mb = 20480;  // 20GB
    config.bulk_load_concurrency = 4;
    config.max_remote_workers = 12;
    
    return config;
  }
  
  // =========================================================================
  // 获取所有预定义场景
  // =========================================================================
  static std::vector<ExperimentConfig> GetAllScenarios() {
    return {
      GetHighWritePressureConfig(),
      GetReadWriteMixedConfig(),
      GetLargeBulkLoadConfig(),
      GetMultiTaskMixedConfig()
    };
  }
};

// ============================================================================
// 负载生成器
// ============================================================================
class WorkloadGenerator {
 public:
  struct WorkloadStats {
    std::atomic<uint64_t> operations_completed{0};
    std::atomic<uint64_t> operations_failed{0};
    std::atomic<uint64_t> total_latency_us{0};
    std::atomic<uint64_t> max_latency_us{0};
  };
  
  WorkloadGenerator() : running_(false) {}
  virtual ~WorkloadGenerator() { Stop(); }
  
  virtual void Start(const ExperimentConfig& config) = 0;
  virtual void Stop() { running_ = false; }
  virtual WorkloadStats GetStats() const = 0;
  
 protected:
  std::atomic<bool> running_;
};

// ============================================================================
// 指标收集器
// ============================================================================
class MetricsCollector {
 public:
  MetricsCollector() : collecting_(false) {}
  virtual ~MetricsCollector() = default;
  
  virtual void StartCollection(std::chrono::milliseconds interval) = 0;
  virtual void StopCollection() { collecting_ = false; }
  virtual ExperimentMetrics GetAggregatedMetrics() const = 0;
  
 protected:
  std::atomic<bool> collecting_;
};

// ============================================================================
// 实验执行器
// ============================================================================
class ExperimentRunner {
 public:
  using ProgressCallback = std::function<void(double progress, const std::string& status)>;
  
  ExperimentRunner() = default;
  ~ExperimentRunner() = default;
  
  // 设置进度回调
  void SetProgressCallback(ProgressCallback callback) {
    progress_callback_ = std::move(callback);
  }
  
  // 运行实验
  ExperimentResult RunExperiment(const ExperimentConfig& config) {
    ExperimentResult result;
    result.config = config;
    result.start_time = std::chrono::system_clock::now();
    
    try {
      // 阶段1：预热
      ReportProgress(0.0, "Warmup phase");
      RunWarmup(config);
      
      // 阶段2：基线测试（关闭远程 Compaction）
      ReportProgress(0.1, "Running baseline test");
      ExperimentConfig baseline_config = config;
      baseline_config.enable_remote_compaction = false;
      result.baseline_metrics = RunPhase(baseline_config);
      
      // 阶段3：实验测试（开启远程 Compaction）
      ReportProgress(0.5, "Running experiment test");
      result.experiment_metrics = RunPhase(config);
      
      // 阶段4：冷却
      ReportProgress(0.9, "Cooldown phase");
      RunCooldown(config);
      
      // 计算对比结果
      CalculateComparison(result);
      
      result.success = true;
      ReportProgress(1.0, "Experiment completed");
      
    } catch (const std::exception& e) {
      result.success = false;
      result.error_message = e.what();
    }
    
    result.end_time = std::chrono::system_clock::now();
    return result;
  }
  
 private:
  ProgressCallback progress_callback_;
  
  void ReportProgress(double progress, const std::string& status) {
    if (progress_callback_) {
      progress_callback_(progress, status);
    }
  }
  
  void RunWarmup(const ExperimentConfig& config) {
    // 预热阶段实现
    std::this_thread::sleep_for(config.warmup_duration);
  }
  
  void RunCooldown(const ExperimentConfig& config) {
    // 冷却阶段实现
    std::this_thread::sleep_for(config.cooldown_duration);
  }
  
  ExperimentMetrics RunPhase(const ExperimentConfig& config) {
    ExperimentMetrics metrics;
    // 实际实现中需要启动负载生成器和指标收集器
    // 这里只是框架定义
    return metrics;
  }
  
  void CalculateComparison(ExperimentResult& result) {
    auto& baseline = result.baseline_metrics;
    auto& experiment = result.experiment_metrics;
    
    // 延迟改善
    if (baseline.latency_p99_us > 0) {
      result.latency_improvement_percent = 
          (baseline.latency_p99_us - experiment.latency_p99_us) / baseline.latency_p99_us * 100;
    }
    
    // 吞吐量改善
    if (baseline.qps_avg > 0) {
      result.throughput_improvement_percent = 
          (experiment.qps_avg - baseline.qps_avg) / baseline.qps_avg * 100;
    }
    
    // 资源节省
    if (baseline.cpu_usage_avg > 0) {
      result.resource_saving_percent = 
          (baseline.cpu_usage_avg - experiment.cpu_usage_avg) / baseline.cpu_usage_avg * 100;
    }
  }
};

// ============================================================================
// 实验报告生成
// ============================================================================
inline std::string ExperimentResult::GenerateReport() const {
  std::string report;
  
  report += "=== 实验报告 ===\n\n";
  report += "实验名称: " + config.name + "\n";
  report += "实验描述: " + config.description + "\n";
  report += "实验状态: " + std::string(success ? "成功" : "失败") + "\n";
  if (!success) {
    report += "错误信息: " + error_message + "\n";
  }
  report += "\n";
  
  report += "--- 配置参数 ---\n";
  report += "目标 QPS: " + std::to_string(config.target_qps) + "\n";
  report += "读写比例: " + std::to_string(config.read_ratio) + "/" + std::to_string(config.write_ratio) + "\n";
  report += "远程 Compaction: " + std::string(config.enable_remote_compaction ? "开启" : "关闭") + "\n";
  report += "远程 Worker 数: " + std::to_string(config.max_remote_workers) + "\n";
  report += "\n";
  
  report += "--- 基线指标 (远程 Compaction 关闭) ---\n";
  report += "平均延迟: " + std::to_string(baseline_metrics.latency_avg_us) + " us\n";
  report += "P99 延迟: " + std::to_string(baseline_metrics.latency_p99_us) + " us\n";
  report += "平均 QPS: " + std::to_string(baseline_metrics.qps_avg) + "\n";
  report += "CPU 使用率: " + std::to_string(baseline_metrics.cpu_usage_avg) + "%\n";
  report += "写放大: " + std::to_string(baseline_metrics.write_amplification) + "\n";
  report += "\n";
  
  report += "--- 实验指标 (远程 Compaction 开启) ---\n";
  report += "平均延迟: " + std::to_string(experiment_metrics.latency_avg_us) + " us\n";
  report += "P99 延迟: " + std::to_string(experiment_metrics.latency_p99_us) + " us\n";
  report += "平均 QPS: " + std::to_string(experiment_metrics.qps_avg) + "\n";
  report += "CPU 使用率: " + std::to_string(experiment_metrics.cpu_usage_avg) + "%\n";
  report += "写放大: " + std::to_string(experiment_metrics.write_amplification) + "\n";
  report += "远程任务比例: " + std::to_string(experiment_metrics.remote_task_ratio * 100) + "%\n";
  report += "CPU 卸载效率: " + std::to_string(experiment_metrics.cpu_offload_percent) + "%\n";
  report += "\n";
  
  report += "--- 对比分析 ---\n";
  report += "延迟改善: " + std::to_string(latency_improvement_percent) + "%\n";
  report += "吞吐量改善: " + std::to_string(throughput_improvement_percent) + "%\n";
  report += "资源节省: " + std::to_string(resource_saving_percent) + "%\n";
  
  return report;
}

}  // namespace observability
}  // namespace tendisplus
