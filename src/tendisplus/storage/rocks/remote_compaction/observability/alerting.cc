// Copyright (c) 2024-present, Tencent Inc. All rights reserved.
// 告警系统实现

#include "alerting.h"

#include <chrono>
#include <iomanip>
#include <sstream>

namespace tendisplus {
namespace observability {

// ============================================================================
// Alert 实现
// ============================================================================

std::string Alert::SeverityToString(AlertSeverity severity) {
  switch (severity) {
    case AlertSeverity::kInfo: return "INFO";
    case AlertSeverity::kWarning: return "WARNING";
    case AlertSeverity::kCritical: return "CRITICAL";
    default: return "UNKNOWN";
  }
}

std::string Alert::StateToString(AlertState state) {
  switch (state) {
    case AlertState::kPending: return "PENDING";
    case AlertState::kFiring: return "FIRING";
    case AlertState::kResolved: return "RESOLVED";
    default: return "UNKNOWN";
  }
}

std::string Alert::ToJson() const {
  std::ostringstream oss;
  oss << "{";
  oss << "\"name\":\"" << name << "\",";
  oss << "\"severity\":\"" << SeverityToString(severity) << "\",";
  oss << "\"state\":\"" << StateToString(state) << "\",";
  oss << "\"message\":\"" << message << "\",";
  oss << "\"timestamp\":" << timestamp << ",";
  oss << "\"labels\":{";
  
  bool first = true;
  for (const auto& [key, value] : labels) {
    if (!first) oss << ",";
    oss << "\"" << key << "\":\"" << value << "\"";
    first = false;
  }
  oss << "},";
  
  oss << "\"annotations\":{";
  first = true;
  for (const auto& [key, value] : annotations) {
    if (!first) oss << ",";
    oss << "\"" << key << "\":\"" << value << "\"";
    first = false;
  }
  oss << "}";
  
  oss << "}";
  return oss.str();
}

// ============================================================================
// AlertRule 实现
// ============================================================================

AlertRule::AlertRule(const std::string& name,
                     AlertSeverity severity,
                     const std::string& message,
                     std::function<bool()> condition)
    : name_(name),
      severity_(severity),
      message_(message),
      condition_(std::move(condition)),
      for_duration_(std::chrono::seconds(0)),
      enabled_(true),
      pending_since_(std::chrono::steady_clock::time_point::min()),
      is_firing_(false) {
}

AlertRule& AlertRule::WithLabel(const std::string& key, const std::string& value) {
  labels_[key] = value;
  return *this;
}

AlertRule& AlertRule::WithAnnotation(const std::string& key, const std::string& value) {
  annotations_[key] = value;
  return *this;
}

AlertRule& AlertRule::WithForDuration(std::chrono::seconds duration) {
  for_duration_ = duration;
  return *this;
}

std::optional<Alert> AlertRule::Evaluate() {
  if (!enabled_) {
    return std::nullopt;
  }
  
  bool condition_met = false;
  try {
    condition_met = condition_();
  } catch (...) {
    // 条件评估失败，视为不满足
    condition_met = false;
  }
  
  auto now = std::chrono::steady_clock::now();
  
  if (condition_met) {
    if (pending_since_ == std::chrono::steady_clock::time_point::min()) {
      // 开始 pending
      pending_since_ = now;
    }
    
    // 检查是否满足 for 持续时间
    auto pending_duration = std::chrono::duration_cast<std::chrono::seconds>(
        now - pending_since_);
    
    if (pending_duration >= for_duration_) {
      // 触发告警
      if (!is_firing_) {
        is_firing_ = true;
        return CreateAlert(AlertState::kFiring);
      }
    } else {
      // 还在 pending 状态
      return CreateAlert(AlertState::kPending);
    }
  } else {
    // 条件不满足
    if (is_firing_) {
      // 从 firing 变为 resolved
      is_firing_ = false;
      pending_since_ = std::chrono::steady_clock::time_point::min();
      return CreateAlert(AlertState::kResolved);
    }
    
    // 重置 pending
    pending_since_ = std::chrono::steady_clock::time_point::min();
  }
  
  return std::nullopt;
}

void AlertRule::Reset() {
  pending_since_ = std::chrono::steady_clock::time_point::min();
  is_firing_ = false;
}

Alert AlertRule::CreateAlert(AlertState state) const {
  Alert alert;
  alert.name = name_;
  alert.severity = severity_;
  alert.state = state;
  alert.message = message_;
  alert.timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch()).count();
  alert.labels = labels_;
  alert.annotations = annotations_;
  return alert;
}

// ============================================================================
// AlertManager 实现
// ============================================================================

AlertManager& AlertManager::Instance() {
  static AlertManager instance;
  return instance;
}

AlertManager::AlertManager()
    : running_(false),
      evaluation_interval_(std::chrono::seconds(15)) {
}

AlertManager::~AlertManager() {
  Stop();
}

void AlertManager::Start() {
  std::lock_guard<std::mutex> lock(mutex_);
  
  if (running_) return;
  
  running_ = true;
  evaluation_thread_ = std::thread(&AlertManager::EvaluationLoop, this);
}

void AlertManager::Stop() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!running_) return;
    running_ = false;
  }
  
  cv_.notify_all();
  
  if (evaluation_thread_.joinable()) {
    evaluation_thread_.join();
  }
}

void AlertManager::AddRule(std::shared_ptr<AlertRule> rule) {
  std::lock_guard<std::mutex> lock(mutex_);
  rules_[rule->Name()] = rule;
}

void AlertManager::RemoveRule(const std::string& name) {
  std::lock_guard<std::mutex> lock(mutex_);
  rules_.erase(name);
}

std::shared_ptr<AlertRule> AlertManager::GetRule(const std::string& name) const {
  std::lock_guard<std::mutex> lock(mutex_);
  
  auto it = rules_.find(name);
  if (it != rules_.end()) {
    return it->second;
  }
  return nullptr;
}

std::vector<std::shared_ptr<AlertRule>> AlertManager::GetAllRules() const {
  std::lock_guard<std::mutex> lock(mutex_);
  
  std::vector<std::shared_ptr<AlertRule>> result;
  result.reserve(rules_.size());
  for (const auto& [name, rule] : rules_) {
    result.push_back(rule);
  }
  return result;
}

void AlertManager::AddNotifier(std::shared_ptr<AlertNotifier> notifier) {
  std::lock_guard<std::mutex> lock(mutex_);
  notifiers_.push_back(notifier);
}

std::vector<Alert> AlertManager::GetActiveAlerts() const {
  std::lock_guard<std::mutex> lock(mutex_);
  
  std::vector<Alert> result;
  for (const auto& [key, alert] : active_alerts_) {
    result.push_back(alert);
  }
  return result;
}

std::vector<Alert> AlertManager::GetAlertHistory(size_t limit) const {
  std::lock_guard<std::mutex> lock(mutex_);
  
  std::vector<Alert> result;
  size_t count = 0;
  
  for (auto it = alert_history_.rbegin(); 
       it != alert_history_.rend() && count < limit; 
       ++it, ++count) {
    result.push_back(*it);
  }
  
  return result;
}

void AlertManager::SetEvaluationInterval(std::chrono::seconds interval) {
  evaluation_interval_.store(interval);
}

void AlertManager::EvaluationLoop() {
  while (running_) {
    {
      std::unique_lock<std::mutex> lock(mutex_);
      cv_.wait_for(lock, evaluation_interval_.load(), [this] {
        return !running_;
      });
    }
    
    if (!running_) break;
    
    EvaluateRules();
  }
}

void AlertManager::EvaluateRules() {
  std::vector<Alert> new_alerts;
  
  {
    std::lock_guard<std::mutex> lock(mutex_);
    
    for (auto& [name, rule] : rules_) {
      auto alert_opt = rule->Evaluate();
      if (alert_opt.has_value()) {
        new_alerts.push_back(alert_opt.value());
      }
    }
  }
  
  // 处理告警
  for (const auto& alert : new_alerts) {
    ProcessAlert(alert);
  }
}

void AlertManager::ProcessAlert(const Alert& alert) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  std::string alert_key = alert.name;
  for (const auto& [key, value] : alert.labels) {
    alert_key += "|" + key + "=" + value;
  }
  
  if (alert.state == AlertState::kFiring) {
    active_alerts_[alert_key] = alert;
  } else if (alert.state == AlertState::kResolved) {
    active_alerts_.erase(alert_key);
  }
  
  // 添加到历史记录
  alert_history_.push_back(alert);
  
  // 限制历史记录大小
  while (alert_history_.size() > 10000) {
    alert_history_.pop_front();
  }
  
  // 发送通知
  if (alert.state == AlertState::kFiring || alert.state == AlertState::kResolved) {
    for (auto& notifier : notifiers_) {
      notifier->Notify(alert);
    }
  }
}

// ============================================================================
// LogAlertNotifier 实现
// ============================================================================

void LogAlertNotifier::Notify(const Alert& alert) {
  std::string level_str = (alert.severity == AlertSeverity::kCritical) ? "ERROR" :
                          (alert.severity == AlertSeverity::kWarning) ? "WARN" : "INFO";
  
  std::cout << "[ALERT] [" << level_str << "] " 
            << Alert::StateToString(alert.state) << " - "
            << alert.name << ": " << alert.message << std::endl;
}

// ============================================================================
// WebhookAlertNotifier 实现
// ============================================================================

WebhookAlertNotifier::WebhookAlertNotifier(const std::string& url)
    : url_(url) {
}

void WebhookAlertNotifier::Notify(const Alert& alert) {
  // 实际实现需要使用 HTTP 客户端发送 POST 请求
  // 这里只是示例框架
  std::string payload = alert.ToJson();
  
  // TODO: 发送 HTTP POST 请求到 url_
  (void)payload;
}

// ============================================================================
// 预定义告警规则工厂
// ============================================================================

namespace AlertRules {

std::shared_ptr<AlertRule> TaskQueueTooLong(
    std::function<size_t()> queue_size_getter,
    size_t threshold) {
  auto rule = std::make_shared<AlertRule>(
      "TaskQueueTooLong",
      AlertSeverity::kWarning,
      "Task queue size exceeds threshold",
      [queue_size_getter, threshold]() {
        return queue_size_getter() > threshold;
      });
  
  rule->WithLabel("component", "scheduler")
      .WithAnnotation("threshold", std::to_string(threshold))
      .WithForDuration(std::chrono::seconds(60));
  
  return rule;
}

std::shared_ptr<AlertRule> WorkerUnhealthy(
    const std::string& worker_id,
    std::function<bool()> health_checker) {
  auto rule = std::make_shared<AlertRule>(
      "WorkerUnhealthy",
      AlertSeverity::kCritical,
      "Worker is unhealthy",
      [health_checker]() {
        return !health_checker();
      });
  
  rule->WithLabel("worker_id", worker_id)
      .WithLabel("component", "worker")
      .WithForDuration(std::chrono::seconds(30));
  
  return rule;
}

std::shared_ptr<AlertRule> TaskFailureRateHigh(
    std::function<double()> failure_rate_getter,
    double threshold) {
  auto rule = std::make_shared<AlertRule>(
      "TaskFailureRateHigh",
      AlertSeverity::kCritical,
      "Task failure rate exceeds threshold",
      [failure_rate_getter, threshold]() {
        return failure_rate_getter() > threshold;
      });
  
  rule->WithLabel("component", "task_executor")
      .WithAnnotation("threshold", std::to_string(threshold))
      .WithForDuration(std::chrono::seconds(120));
  
  return rule;
}

std::shared_ptr<AlertRule> CompactionLatencyHigh(
    std::function<double()> latency_getter,
    double threshold_seconds) {
  auto rule = std::make_shared<AlertRule>(
      "CompactionLatencyHigh",
      AlertSeverity::kWarning,
      "Compaction latency exceeds threshold",
      [latency_getter, threshold_seconds]() {
        return latency_getter() > threshold_seconds;
      });
  
  rule->WithLabel("component", "compaction")
      .WithAnnotation("threshold_seconds", std::to_string(threshold_seconds))
      .WithForDuration(std::chrono::seconds(300));
  
  return rule;
}

std::shared_ptr<AlertRule> NoAvailableWorkers(
    std::function<size_t()> available_workers_getter) {
  auto rule = std::make_shared<AlertRule>(
      "NoAvailableWorkers",
      AlertSeverity::kCritical,
      "No available workers for task execution",
      [available_workers_getter]() {
        return available_workers_getter() == 0;
      });
  
  rule->WithLabel("component", "worker_manager")
      .WithForDuration(std::chrono::seconds(60));
  
  return rule;
}

}  // namespace AlertRules

}  // namespace observability
}  // namespace tendisplus
