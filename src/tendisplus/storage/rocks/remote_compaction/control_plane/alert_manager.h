// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

// AlertManager - 告警规则引擎
// 基于 CaaS-LSM 观测平面设计
//
// 功能：
// 1. 告警规则定义与管理
// 2. 周期性阈值检测
// 3. 告警触发与恢复
// 4. 告警历史记录
// 5. 告警级别分类 (Info / Warning / Critical)

#pragma once

#include <atomic>
#include <chrono>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace tendisplus {
namespace control_plane {

// ============================================================================
// 告警级别
// ============================================================================
enum class AlertSeverity {
  kInfo = 0,       // 信息
  kWarning = 1,    // 警告
  kCritical = 2    // 严重
};

inline const char* AlertSeverityToString(AlertSeverity severity) {
  switch (severity) {
    case AlertSeverity::kInfo: return "Info";
    case AlertSeverity::kWarning: return "Warning";
    case AlertSeverity::kCritical: return "Critical";
    default: return "Unknown";
  }
}

// ============================================================================
// 告警状态
// ============================================================================
enum class AlertState {
  kInactive = 0,   // 未触发
  kFiring = 1,     // 触发中
  kResolved = 2    // 已恢复
};

inline const char* AlertStateToString(AlertState state) {
  switch (state) {
    case AlertState::kInactive: return "Inactive";
    case AlertState::kFiring: return "Firing";
    case AlertState::kResolved: return "Resolved";
    default: return "Unknown";
  }
}

// ============================================================================
// 告警指标类型
// ============================================================================
enum class AlertMetric {
  kPendingTaskCount = 0,       // 待处理任务数
  kRunningTaskCount = 1,       // 运行中任务数
  kFailedTaskRate = 2,         // 任务失败率 (%)
  kAvgQueueTimeMs = 3,         // 平均排队时间 (ms)
  kAvgExecutionTimeMs = 4,     // 平均执行时间 (ms)
  kP99ExecutionTimeMs = 5,     // P99 执行延迟 (ms)
  kOnlineWorkerCount = 6,      // 在线 Worker 数
  kClusterMemoryUsage = 7,     // 集群内存使用率 (%)
  kClusterDiskUsage = 8,       // 集群磁盘使用率 (%)
  kTaskTimeoutCount = 9,       // 任务超时数
  kWorkerOfflineCount = 10,    // 离线 Worker 数
  kBulkLoadFailedShards = 11   // Bulk Load 失败分片数
};

inline const char* AlertMetricToString(AlertMetric metric) {
  switch (metric) {
    case AlertMetric::kPendingTaskCount: return "pending_task_count";
    case AlertMetric::kRunningTaskCount: return "running_task_count";
    case AlertMetric::kFailedTaskRate: return "failed_task_rate";
    case AlertMetric::kAvgQueueTimeMs: return "avg_queue_time_ms";
    case AlertMetric::kAvgExecutionTimeMs: return "avg_execution_time_ms";
    case AlertMetric::kP99ExecutionTimeMs: return "p99_execution_time_ms";
    case AlertMetric::kOnlineWorkerCount: return "online_worker_count";
    case AlertMetric::kClusterMemoryUsage: return "cluster_memory_usage";
    case AlertMetric::kClusterDiskUsage: return "cluster_disk_usage";
    case AlertMetric::kTaskTimeoutCount: return "task_timeout_count";
    case AlertMetric::kWorkerOfflineCount: return "worker_offline_count";
    case AlertMetric::kBulkLoadFailedShards: return "bulk_load_failed_shards";
    default: return "unknown";
  }
}

// ============================================================================
// 告警比较操作符
// ============================================================================
enum class AlertOperator {
  kGreaterThan = 0,        // >
  kGreaterThanOrEqual = 1, // >=
  kLessThan = 2,           // <
  kLessThanOrEqual = 3,    // <=
  kEqual = 4               // ==
};

inline const char* AlertOperatorToString(AlertOperator op) {
  switch (op) {
    case AlertOperator::kGreaterThan: return ">";
    case AlertOperator::kGreaterThanOrEqual: return ">=";
    case AlertOperator::kLessThan: return "<";
    case AlertOperator::kLessThanOrEqual: return "<=";
    case AlertOperator::kEqual: return "==";
    default: return "?";
  }
}

// ============================================================================
// 告警规则
// ============================================================================
struct AlertRule {
  std::string rule_id;             // 规则 ID
  std::string name;                // 规则名称
  std::string description;         // 规则描述
  AlertMetric metric;              // 监控指标
  AlertOperator op;                // 比较操作符
  double threshold;                // 阈值
  AlertSeverity severity;          // 告警级别
  uint32_t duration_sec;           // 持续时间阈值 (触发需连续超限多少秒)
  bool enabled = true;             // 是否启用

  // 运行时状态 (不持久化)
  AlertState state = AlertState::kInactive;
  int64_t first_triggered_ms = 0;  // 首次触发时间
  int64_t last_checked_ms = 0;     // 最后检查时间
  double last_value = 0.0;         // 最后检查值
  uint32_t consecutive_triggers = 0;  // 连续触发次数
};

// ============================================================================
// 告警事件
// ============================================================================
struct AlertEvent {
  std::string event_id;            // 事件 ID
  std::string rule_id;             // 关联规则 ID
  std::string rule_name;           // 规则名称
  AlertSeverity severity;          // 告警级别
  AlertState state;                // 事件状态 (Firing / Resolved)
  AlertMetric metric;              // 触发指标
  double current_value;            // 当前值
  double threshold;                // 阈值
  AlertOperator op;                // 操作符
  int64_t timestamp_ms;            // 事件时间
  std::string message;             // 告警消息
};

// ============================================================================
// 指标快照 (用于告警规则评估)
// ============================================================================
struct MetricsSnapshot {
  uint32_t pending_tasks = 0;
  uint32_t running_tasks = 0;
  uint64_t total_completed = 0;
  uint64_t total_failed = 0;
  double failed_rate = 0.0;        // 失败率 (%)
  double avg_queue_time_ms = 0.0;
  double avg_execution_time_ms = 0.0;
  double p99_execution_time_ms = 0.0;
  uint32_t online_workers = 0;
  uint32_t total_workers = 0;
  double cluster_memory_usage = 0.0;   // 0.0 ~ 1.0
  double cluster_disk_usage = 0.0;     // 0.0 ~ 1.0
  uint64_t task_timeout_count = 0;
  uint32_t worker_offline_count = 0;
  uint32_t bulk_load_failed_shards = 0;
};

// ============================================================================
// AlertManager 配置
// ============================================================================
struct AlertManagerConfig {
  uint32_t check_interval_sec = 10;    // 告警检查间隔 (秒)
  uint32_t max_history_size = 1000;    // 最大告警历史记录数
  bool enable_default_rules = true;    // 启用默认告警规则
};

// ============================================================================
// AlertManager - 告警规则引擎
// ============================================================================
class AlertManager {
 public:
  // 指标获取回调 (由 ControlPlane 提供)
  using MetricsProvider = std::function<MetricsSnapshot()>;

  explicit AlertManager(const AlertManagerConfig& config);
  ~AlertManager();

  // 禁止拷贝
  AlertManager(const AlertManager&) = delete;
  AlertManager& operator=(const AlertManager&) = delete;

  // 生命周期管理
  void Start(MetricsProvider provider);
  void Stop();
  bool IsRunning() const { return running_.load(); }

  // 告警规则管理
  void AddRule(const AlertRule& rule);
  void RemoveRule(const std::string& rule_id);
  void EnableRule(const std::string& rule_id, bool enabled);
  std::vector<AlertRule> GetAllRules() const;
  const AlertRule* GetRule(const std::string& rule_id) const;

  // 告警查询
  std::vector<AlertEvent> GetActiveAlerts() const;
  std::vector<AlertEvent> GetAlertHistory(uint32_t limit = 100) const;
  uint32_t GetActiveAlertCount() const;
  uint32_t GetAlertCountBySeverity(AlertSeverity severity) const;

 private:
  // 初始化默认告警规则
  void InitDefaultRules();

  // 检查循环
  void CheckLoop();

  // 评估单条规则
  void EvaluateRule(AlertRule& rule, const MetricsSnapshot& snapshot);

  // 获取指标值
  double GetMetricValue(AlertMetric metric,
                        const MetricsSnapshot& snapshot) const;

  // 比较操作
  bool CompareValue(double value, AlertOperator op, double threshold) const;

  // 触发告警
  void FireAlert(AlertRule& rule, double value);

  // 恢复告警
  void ResolveAlert(AlertRule& rule, double value);

  // 生成告警消息
  std::string BuildAlertMessage(const AlertRule& rule, double value) const;

  // 生成唯一 ID
  std::string GenerateEventId();

  AlertManagerConfig config_;
  MetricsProvider metrics_provider_;
  std::atomic<bool> running_{false};

  // 告警规则
  mutable std::mutex rules_mutex_;
  std::unordered_map<std::string, AlertRule> rules_;

  // 活跃告警
  mutable std::mutex active_mutex_;
  std::unordered_map<std::string, AlertEvent> active_alerts_;  // rule_id → event

  // 告警历史
  mutable std::mutex history_mutex_;
  std::deque<AlertEvent> alert_history_;

  // 检查线程
  std::unique_ptr<std::thread> check_thread_;

  // 事件 ID 计数器
  std::atomic<uint64_t> event_counter_{0};
};

}  // namespace control_plane
}  // namespace tendisplus
