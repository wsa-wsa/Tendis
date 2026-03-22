// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include "alert_manager.h"

#include <algorithm>
#include <cmath>
#include <iostream>
#include <sstream>

namespace tendisplus {
namespace control_plane {

AlertManager::AlertManager(const AlertManagerConfig& config)
    : config_(config) {}

AlertManager::~AlertManager() {
  Stop();
}

void AlertManager::Start(MetricsProvider provider) {
  if (running_.load()) {
    return;
  }

  metrics_provider_ = std::move(provider);

  if (config_.enable_default_rules) {
    InitDefaultRules();
  }

  running_.store(true);
  check_thread_ = std::make_unique<std::thread>(&AlertManager::CheckLoop, this);

  std::cout << "[AlertManager] Started with " << rules_.size()
            << " rules, check interval: " << config_.check_interval_sec
            << "s" << std::endl;
}

void AlertManager::Stop() {
  if (!running_.exchange(false)) {
    return;
  }

  if (check_thread_ && check_thread_->joinable()) {
    check_thread_->join();
  }

  std::cout << "[AlertManager] Stopped" << std::endl;
}

// ============================================================================
// 默认告警规则初始化
// ============================================================================
void AlertManager::InitDefaultRules() {
  // 规则 1: 待处理任务堆积 - Warning
  {
    AlertRule rule;
    rule.rule_id = "pending_tasks_high";
    rule.name = "Pending Tasks High";
    rule.description = "Pending task count exceeds threshold";
    rule.metric = AlertMetric::kPendingTaskCount;
    rule.op = AlertOperator::kGreaterThan;
    rule.threshold = 100;
    rule.severity = AlertSeverity::kWarning;
    rule.duration_sec = 30;
    AddRule(rule);
  }

  // 规则 2: 待处理任务严重堆积 - Critical
  {
    AlertRule rule;
    rule.rule_id = "pending_tasks_critical";
    rule.name = "Pending Tasks Critical";
    rule.description = "Pending task count critically high";
    rule.metric = AlertMetric::kPendingTaskCount;
    rule.op = AlertOperator::kGreaterThan;
    rule.threshold = 500;
    rule.severity = AlertSeverity::kCritical;
    rule.duration_sec = 10;
    AddRule(rule);
  }

  // 规则 3: 任务失败率过高 - Warning
  {
    AlertRule rule;
    rule.rule_id = "failed_rate_high";
    rule.name = "Task Failure Rate High";
    rule.description = "Task failure rate exceeds 10%";
    rule.metric = AlertMetric::kFailedTaskRate;
    rule.op = AlertOperator::kGreaterThan;
    rule.threshold = 10.0;
    rule.severity = AlertSeverity::kWarning;
    rule.duration_sec = 60;
    AddRule(rule);
  }

  // 规则 4: 任务失败率严重 - Critical
  {
    AlertRule rule;
    rule.rule_id = "failed_rate_critical";
    rule.name = "Task Failure Rate Critical";
    rule.description = "Task failure rate exceeds 30%";
    rule.metric = AlertMetric::kFailedTaskRate;
    rule.op = AlertOperator::kGreaterThan;
    rule.threshold = 30.0;
    rule.severity = AlertSeverity::kCritical;
    rule.duration_sec = 30;
    AddRule(rule);
  }

  // 规则 5: P99 延迟过高 - Warning
  {
    AlertRule rule;
    rule.rule_id = "p99_latency_high";
    rule.name = "P99 Execution Latency High";
    rule.description = "P99 execution time exceeds 60 seconds";
    rule.metric = AlertMetric::kP99ExecutionTimeMs;
    rule.op = AlertOperator::kGreaterThan;
    rule.threshold = 60000;
    rule.severity = AlertSeverity::kWarning;
    rule.duration_sec = 60;
    AddRule(rule);
  }

  // 规则 6: 排队时间过长 - Warning
  {
    AlertRule rule;
    rule.rule_id = "queue_time_high";
    rule.name = "Queue Time High";
    rule.description = "Average queue time exceeds 30 seconds";
    rule.metric = AlertMetric::kAvgQueueTimeMs;
    rule.op = AlertOperator::kGreaterThan;
    rule.threshold = 30000;
    rule.severity = AlertSeverity::kWarning;
    rule.duration_sec = 60;
    AddRule(rule);
  }

  // 规则 7: Worker 全部离线 - Critical
  {
    AlertRule rule;
    rule.rule_id = "no_workers_online";
    rule.name = "No Workers Online";
    rule.description = "All workers are offline";
    rule.metric = AlertMetric::kOnlineWorkerCount;
    rule.op = AlertOperator::kEqual;
    rule.threshold = 0;
    rule.severity = AlertSeverity::kCritical;
    rule.duration_sec = 10;
    AddRule(rule);
  }

  // 规则 8: 内存使用率过高 - Warning
  {
    AlertRule rule;
    rule.rule_id = "memory_usage_high";
    rule.name = "Cluster Memory Usage High";
    rule.description = "Cluster memory usage exceeds 85%";
    rule.metric = AlertMetric::kClusterMemoryUsage;
    rule.op = AlertOperator::kGreaterThan;
    rule.threshold = 0.85;
    rule.severity = AlertSeverity::kWarning;
    rule.duration_sec = 60;
    AddRule(rule);
  }

  // 规则 9: 磁盘使用率过高 - Critical
  {
    AlertRule rule;
    rule.rule_id = "disk_usage_critical";
    rule.name = "Cluster Disk Usage Critical";
    rule.description = "Cluster disk usage exceeds 90%";
    rule.metric = AlertMetric::kClusterDiskUsage;
    rule.op = AlertOperator::kGreaterThan;
    rule.threshold = 0.90;
    rule.severity = AlertSeverity::kCritical;
    rule.duration_sec = 30;
    AddRule(rule);
  }

  std::cout << "[AlertManager] Initialized " << rules_.size()
            << " default alert rules" << std::endl;
}

// ============================================================================
// 告警规则管理
// ============================================================================
void AlertManager::AddRule(const AlertRule& rule) {
  std::lock_guard<std::mutex> lock(rules_mutex_);
  rules_[rule.rule_id] = rule;
}

void AlertManager::RemoveRule(const std::string& rule_id) {
  std::lock_guard<std::mutex> lock(rules_mutex_);
  rules_.erase(rule_id);

  // 同时清除活跃告警
  std::lock_guard<std::mutex> lock2(active_mutex_);
  active_alerts_.erase(rule_id);
}

void AlertManager::EnableRule(const std::string& rule_id, bool enabled) {
  std::lock_guard<std::mutex> lock(rules_mutex_);
  auto it = rules_.find(rule_id);
  if (it != rules_.end()) {
    it->second.enabled = enabled;
    if (!enabled) {
      it->second.state = AlertState::kInactive;
      it->second.consecutive_triggers = 0;
    }
  }
}

std::vector<AlertRule> AlertManager::GetAllRules() const {
  std::lock_guard<std::mutex> lock(rules_mutex_);
  std::vector<AlertRule> result;
  result.reserve(rules_.size());
  for (const auto& [id, rule] : rules_) {
    result.push_back(rule);
  }
  return result;
}

const AlertRule* AlertManager::GetRule(const std::string& rule_id) const {
  std::lock_guard<std::mutex> lock(rules_mutex_);
  auto it = rules_.find(rule_id);
  return it != rules_.end() ? &it->second : nullptr;
}

// ============================================================================
// 告警查询
// ============================================================================
std::vector<AlertEvent> AlertManager::GetActiveAlerts() const {
  std::lock_guard<std::mutex> lock(active_mutex_);
  std::vector<AlertEvent> result;
  result.reserve(active_alerts_.size());
  for (const auto& [rule_id, event] : active_alerts_) {
    result.push_back(event);
  }
  // 按严重程度降序排列
  std::sort(result.begin(), result.end(),
    [](const AlertEvent& a, const AlertEvent& b) {
      return static_cast<int>(a.severity) > static_cast<int>(b.severity);
    });
  return result;
}

std::vector<AlertEvent> AlertManager::GetAlertHistory(uint32_t limit) const {
  std::lock_guard<std::mutex> lock(history_mutex_);
  std::vector<AlertEvent> result;
  uint32_t count = std::min(limit, static_cast<uint32_t>(alert_history_.size()));
  result.reserve(count);
  // 返回最近的 N 条 (deque 尾部是最新的)
  auto it = alert_history_.end();
  for (uint32_t i = 0; i < count; ++i) {
    --it;
    result.push_back(*it);
  }
  return result;
}

uint32_t AlertManager::GetActiveAlertCount() const {
  std::lock_guard<std::mutex> lock(active_mutex_);
  return static_cast<uint32_t>(active_alerts_.size());
}

uint32_t AlertManager::GetAlertCountBySeverity(AlertSeverity severity) const {
  std::lock_guard<std::mutex> lock(active_mutex_);
  uint32_t count = 0;
  for (const auto& [rule_id, event] : active_alerts_) {
    if (event.severity == severity) {
      count++;
    }
  }
  return count;
}

// ============================================================================
// 检查循环
// ============================================================================
void AlertManager::CheckLoop() {
  while (running_.load()) {
    if (metrics_provider_) {
      try {
        auto snapshot = metrics_provider_();

        std::lock_guard<std::mutex> lock(rules_mutex_);
        for (auto& [rule_id, rule] : rules_) {
          if (rule.enabled) {
            EvaluateRule(rule, snapshot);
          }
        }
      } catch (const std::exception& e) {
        std::cerr << "[AlertManager] Check error: " << e.what() << std::endl;
      }
    }

    // 等待下一次检查
    for (uint32_t i = 0;
         i < config_.check_interval_sec * 10 && running_.load(); ++i) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
  }
}

// ============================================================================
// 规则评估
// ============================================================================
void AlertManager::EvaluateRule(AlertRule& rule,
                                const MetricsSnapshot& snapshot) {
  double value = GetMetricValue(rule.metric, snapshot);
  rule.last_value = value;
  rule.last_checked_ms =
    std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch()).count();

  bool threshold_exceeded = CompareValue(value, rule.op, rule.threshold);

  if (threshold_exceeded) {
    rule.consecutive_triggers++;

    if (rule.first_triggered_ms == 0) {
      rule.first_triggered_ms = rule.last_checked_ms;
    }

    // 检查是否满足持续时间要求
    int64_t duration_ms = rule.last_checked_ms - rule.first_triggered_ms;
    if (duration_ms >= static_cast<int64_t>(rule.duration_sec) * 1000) {
      if (rule.state != AlertState::kFiring) {
        FireAlert(rule, value);
      }
    }
  } else {
    // 阈值未超限
    if (rule.state == AlertState::kFiring) {
      ResolveAlert(rule, value);
    }
    rule.consecutive_triggers = 0;
    rule.first_triggered_ms = 0;
  }
}

double AlertManager::GetMetricValue(AlertMetric metric,
                                    const MetricsSnapshot& snapshot) const {
  switch (metric) {
    case AlertMetric::kPendingTaskCount:
      return static_cast<double>(snapshot.pending_tasks);
    case AlertMetric::kRunningTaskCount:
      return static_cast<double>(snapshot.running_tasks);
    case AlertMetric::kFailedTaskRate:
      return snapshot.failed_rate;
    case AlertMetric::kAvgQueueTimeMs:
      return snapshot.avg_queue_time_ms;
    case AlertMetric::kAvgExecutionTimeMs:
      return snapshot.avg_execution_time_ms;
    case AlertMetric::kP99ExecutionTimeMs:
      return snapshot.p99_execution_time_ms;
    case AlertMetric::kOnlineWorkerCount:
      return static_cast<double>(snapshot.online_workers);
    case AlertMetric::kClusterMemoryUsage:
      return snapshot.cluster_memory_usage;
    case AlertMetric::kClusterDiskUsage:
      return snapshot.cluster_disk_usage;
    case AlertMetric::kTaskTimeoutCount:
      return static_cast<double>(snapshot.task_timeout_count);
    case AlertMetric::kWorkerOfflineCount:
      return static_cast<double>(snapshot.worker_offline_count);
    case AlertMetric::kBulkLoadFailedShards:
      return static_cast<double>(snapshot.bulk_load_failed_shards);
    default:
      return 0.0;
  }
}

bool AlertManager::CompareValue(double value, AlertOperator op,
                                double threshold) const {
  switch (op) {
    case AlertOperator::kGreaterThan:
      return value > threshold;
    case AlertOperator::kGreaterThanOrEqual:
      return value >= threshold;
    case AlertOperator::kLessThan:
      return value < threshold;
    case AlertOperator::kLessThanOrEqual:
      return value <= threshold;
    case AlertOperator::kEqual:
      return std::fabs(value - threshold) < 1e-6;
    default:
      return false;
  }
}

// ============================================================================
// 告警触发与恢复
// ============================================================================
void AlertManager::FireAlert(AlertRule& rule, double value) {
  rule.state = AlertState::kFiring;

  AlertEvent event;
  event.event_id = GenerateEventId();
  event.rule_id = rule.rule_id;
  event.rule_name = rule.name;
  event.severity = rule.severity;
  event.state = AlertState::kFiring;
  event.metric = rule.metric;
  event.current_value = value;
  event.threshold = rule.threshold;
  event.op = rule.op;
  event.timestamp_ms = rule.last_checked_ms;
  event.message = BuildAlertMessage(rule, value);

  // 添加到活跃告警
  {
    std::lock_guard<std::mutex> lock(active_mutex_);
    active_alerts_[rule.rule_id] = event;
  }

  // 添加到历史记录
  {
    std::lock_guard<std::mutex> lock(history_mutex_);
    alert_history_.push_back(event);
    while (alert_history_.size() > config_.max_history_size) {
      alert_history_.pop_front();
    }
  }

  std::cout << "[AlertManager] 🔥 ALERT FIRED: [" 
            << AlertSeverityToString(rule.severity) << "] "
            << rule.name << " - " << event.message << std::endl;
}

void AlertManager::ResolveAlert(AlertRule& rule, double value) {
  rule.state = AlertState::kResolved;
  rule.first_triggered_ms = 0;
  rule.consecutive_triggers = 0;

  AlertEvent event;
  event.event_id = GenerateEventId();
  event.rule_id = rule.rule_id;
  event.rule_name = rule.name;
  event.severity = rule.severity;
  event.state = AlertState::kResolved;
  event.metric = rule.metric;
  event.current_value = value;
  event.threshold = rule.threshold;
  event.op = rule.op;
  event.timestamp_ms =
    std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch()).count();
  event.message = "Resolved: " + rule.name + " (value=" +
                  std::to_string(value) + ")";

  // 从活跃告警中移除
  {
    std::lock_guard<std::mutex> lock(active_mutex_);
    active_alerts_.erase(rule.rule_id);
  }

  // 添加到历史记录
  {
    std::lock_guard<std::mutex> lock(history_mutex_);
    alert_history_.push_back(event);
    while (alert_history_.size() > config_.max_history_size) {
      alert_history_.pop_front();
    }
  }

  std::cout << "[AlertManager] ✅ ALERT RESOLVED: " << rule.name
            << " (value=" << value << ")" << std::endl;

  // 恢复后切换到 Inactive 状态
  rule.state = AlertState::kInactive;
}

std::string AlertManager::BuildAlertMessage(const AlertRule& rule,
                                            double value) const {
  std::ostringstream oss;
  oss << rule.name << ": " << AlertMetricToString(rule.metric)
      << " = " << value << " " << AlertOperatorToString(rule.op)
      << " " << rule.threshold;
  return oss.str();
}

std::string AlertManager::GenerateEventId() {
  uint64_t count = event_counter_.fetch_add(1);
  auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
    std::chrono::system_clock::now().time_since_epoch()).count();
  return "alert_" + std::to_string(now) + "_" + std::to_string(count);
}

}  // namespace control_plane
}  // namespace tendisplus
