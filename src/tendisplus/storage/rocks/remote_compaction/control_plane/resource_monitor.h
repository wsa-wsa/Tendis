// Copyright (c) 2024-present, Tencent Inc. All rights reserved.
// 资源监控与感知

#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace tendisplus {
namespace control_plane {

// ============================================================================
// 资源类型
// ============================================================================
enum class ResourceType {
  kCpu = 0,
  kMemory = 1,
  kDisk = 2,
  kNetwork = 3,
  kIops = 4
};

inline const char* ResourceTypeToString(ResourceType type) {
  switch (type) {
    case ResourceType::kCpu: return "CPU";
    case ResourceType::kMemory: return "Memory";
    case ResourceType::kDisk: return "Disk";
    case ResourceType::kNetwork: return "Network";
    case ResourceType::kIops: return "IOPS";
    default: return "Unknown";
  }
}

// ============================================================================
// 资源快照
// ============================================================================
struct ResourceSnapshot {
  std::string node_id;
  std::chrono::system_clock::time_point timestamp;
  
  // CPU
  uint32_t cpu_cores = 0;
  double cpu_usage_percent = 0.0;
  double cpu_user_percent = 0.0;
  double cpu_system_percent = 0.0;
  double cpu_iowait_percent = 0.0;
  double load_avg_1min = 0.0;
  double load_avg_5min = 0.0;
  double load_avg_15min = 0.0;
  
  // 内存
  uint64_t memory_total_bytes = 0;
  uint64_t memory_available_bytes = 0;
  uint64_t memory_used_bytes = 0;
  double memory_usage_percent = 0.0;
  
  // 磁盘
  uint64_t disk_total_bytes = 0;
  uint64_t disk_available_bytes = 0;
  uint64_t disk_used_bytes = 0;
  double disk_usage_percent = 0.0;
  uint64_t disk_read_bytes_per_sec = 0;
  uint64_t disk_write_bytes_per_sec = 0;
  double disk_io_util_percent = 0.0;
  
  // 网络
  uint64_t network_rx_bytes_per_sec = 0;
  uint64_t network_tx_bytes_per_sec = 0;
  double network_usage_percent = 0.0;
  
  // 任务队列
  uint32_t pending_tasks = 0;
  uint32_t running_tasks = 0;
  
  // 计算综合负载
  double ComputeOverallLoad() const;
  
  // 序列化
  std::string Serialize() const;
  static ResourceSnapshot Deserialize(const std::string& data);
};

// ============================================================================
// 资源趋势
// ============================================================================
struct ResourceTrend {
  ResourceType type;
  std::string node_id;
  
  // 趋势数据点
  std::vector<std::pair<std::chrono::system_clock::time_point, double>> data_points;
  
  // 统计
  double min_value = 0.0;
  double max_value = 0.0;
  double avg_value = 0.0;
  double std_dev = 0.0;
  
  // 趋势方向 (-1: 下降, 0: 稳定, 1: 上升)
  int trend_direction = 0;
  double trend_slope = 0.0;
  
  // 预测
  double predicted_value = 0.0;
  std::chrono::system_clock::time_point prediction_time;
};

// ============================================================================
// 资源阈值配置
// ============================================================================
struct ResourceThresholds {
  // CPU 阈值
  double cpu_warning_percent = 70.0;
  double cpu_critical_percent = 90.0;
  
  // 内存阈值
  double memory_warning_percent = 75.0;
  double memory_critical_percent = 90.0;
  
  // 磁盘阈值
  double disk_warning_percent = 80.0;
  double disk_critical_percent = 95.0;
  
  // IO 阈值
  double io_util_warning_percent = 70.0;
  double io_util_critical_percent = 90.0;
  
  // 网络阈值
  double network_warning_percent = 70.0;
  double network_critical_percent = 90.0;
  
  // 队列阈值
  uint32_t queue_warning_length = 100;
  uint32_t queue_critical_length = 500;
};

// ============================================================================
// 资源告警
// ============================================================================
struct ResourceAlert {
  std::string alert_id;
  std::string node_id;
  ResourceType resource_type;
  
  enum class Severity {
    kInfo = 0,
    kWarning = 1,
    kCritical = 2
  };
  Severity severity = Severity::kInfo;
  
  std::string message;
  double current_value = 0.0;
  double threshold_value = 0.0;
  
  std::chrono::system_clock::time_point triggered_at;
  std::chrono::system_clock::time_point resolved_at;
  bool resolved = false;
};

// ============================================================================
// 资源监控器
// ============================================================================
class ResourceMonitor {
 public:
  struct Config {
    std::chrono::milliseconds collect_interval{5000};
    std::chrono::hours history_retention{24};
    uint32_t max_data_points = 1000;
    ResourceThresholds thresholds;
  };
  
  explicit ResourceMonitor(const Config& config);
  ~ResourceMonitor();
  
  // 单例访问
  static ResourceMonitor& Instance();
  static void Initialize(const Config& config);
  
  // 启动/停止
  void Start();
  void Stop();
  bool IsRunning() const { return running_.load(); }
  
  // =========================================================================
  // 数据上报
  // =========================================================================
  
  // 上报资源快照
  void ReportSnapshot(const ResourceSnapshot& snapshot);
  
  // =========================================================================
  // 数据查询
  // =========================================================================
  
  // 获取最新快照
  ResourceSnapshot GetLatestSnapshot(const std::string& node_id) const;
  
  // 获取所有节点的最新快照
  std::vector<ResourceSnapshot> GetAllSnapshots() const;
  
  // 获取历史快照
  std::vector<ResourceSnapshot> GetHistory(
      const std::string& node_id,
      std::chrono::system_clock::time_point start,
      std::chrono::system_clock::time_point end) const;
  
  // 获取资源趋势
  ResourceTrend GetTrend(const std::string& node_id,
                         ResourceType type,
                         std::chrono::hours duration) const;
  
  // =========================================================================
  // 负载分析
  // =========================================================================
  
  // 获取节点负载排名
  std::vector<std::pair<std::string, double>> GetLoadRanking() const;
  
  // 获取负载最低的节点
  std::string GetLeastLoadedNode() const;
  
  // 获取负载最高的节点
  std::string GetMostLoadedNode() const;
  
  // 计算负载均衡度 (0-1, 1 表示完全均衡)
  double ComputeLoadBalanceScore() const;
  
  // 检查节点是否过载
  bool IsOverloaded(const std::string& node_id) const;
  
  // =========================================================================
  // 告警管理
  // =========================================================================
  
  // 获取活跃告警
  std::vector<ResourceAlert> GetActiveAlerts() const;
  
  // 获取节点告警
  std::vector<ResourceAlert> GetNodeAlerts(const std::string& node_id) const;
  
  // 设置告警回调
  using AlertCallback = std::function<void(const ResourceAlert&)>;
  void SetAlertCallback(AlertCallback callback);
  
  // =========================================================================
  // 资源预测
  // =========================================================================
  
  // 预测未来资源使用
  double PredictResourceUsage(const std::string& node_id,
                              ResourceType type,
                              std::chrono::minutes ahead) const;
  
  // 预测是否会超阈值
  bool WillExceedThreshold(const std::string& node_id,
                           ResourceType type,
                           std::chrono::minutes ahead) const;

 private:
  void CollectLoop();
  void CheckThresholds(const ResourceSnapshot& snapshot);
  void CleanupHistory();
  
  // 趋势分析
  void AnalyzeTrend(const std::string& node_id, ResourceType type);
  
  // 线性回归预测
  double LinearRegression(const std::vector<std::pair<double, double>>& points,
                          double x) const;
  
  Config config_;
  std::atomic<bool> running_{false};
  
  // 快照存储
  std::map<std::string, ResourceSnapshot> latest_snapshots_;
  std::map<std::string, std::vector<ResourceSnapshot>> snapshot_history_;
  mutable std::mutex snapshots_mutex_;
  
  // 告警存储
  std::vector<ResourceAlert> active_alerts_;
  std::vector<ResourceAlert> alert_history_;
  mutable std::mutex alerts_mutex_;
  AlertCallback alert_callback_;
  
  // 线程
  std::unique_ptr<std::thread> collect_thread_;
  std::condition_variable cv_;
  mutable std::mutex mutex_;
  
  // 单例
  static std::unique_ptr<ResourceMonitor> instance_;
  static std::mutex instance_mutex_;
};

// ============================================================================
// 容量规划器
// ============================================================================
class CapacityPlanner {
 public:
  CapacityPlanner() = default;
  ~CapacityPlanner() = default;
  
  // 容量建议
  struct CapacityRecommendation {
    bool needs_scaling = false;
    
    enum class ScaleDirection {
      kNone = 0,
      kScaleUp = 1,
      kScaleDown = 2
    };
    ScaleDirection direction = ScaleDirection::kNone;
    
    uint32_t recommended_workers = 0;
    std::string reason;
    
    // 资源细节
    uint32_t additional_cpu_cores = 0;
    uint64_t additional_memory_mb = 0;
    uint64_t additional_disk_mb = 0;
  };
  
  // 分析容量需求
  CapacityRecommendation Analyze(const std::vector<ResourceSnapshot>& snapshots,
                                  uint32_t current_workers);
  
  // 预测未来容量需求
  CapacityRecommendation PredictFutureNeeds(
      const std::vector<ResourceSnapshot>& history,
      std::chrono::hours prediction_horizon);
  
  // 计算最优 Worker 数量
  uint32_t ComputeOptimalWorkerCount(
      const std::vector<ResourceSnapshot>& snapshots,
      double target_utilization = 0.7);

 private:
  double ComputeAverageUtilization(
      const std::vector<ResourceSnapshot>& snapshots) const;
};

}  // namespace control_plane
}  // namespace tendisplus
