// Copyright (c) 2024-present, Tencent Inc. All rights reserved.
// 告警系统

#pragma once

#include <chrono>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace tendisplus {
namespace observability {

// ============================================================================
// 告警级别
// ============================================================================
enum class AlertSeverity {
  kInfo = 0,       // 信息
  kWarning = 1,    // 警告
  kCritical = 2,   // 严重
  kEmergency = 3   // 紧急
};

inline const char* AlertSeverityToString(AlertSeverity severity) {
  switch (severity) {
    case AlertSeverity::kInfo: return "INFO";
    case AlertSeverity::kWarning: return "WARNING";
    case AlertSeverity::kCritical: return "CRITICAL";
    case AlertSeverity::kEmergency: return "EMERGENCY";
    default: return "UNKNOWN";
  }
}

// ============================================================================
// 告警状态
// ============================================================================
enum class AlertState {
  kPending = 0,    // 待处理
  kFiring = 1,     // 触发中
  kResolved = 2    // 已恢复
};

inline const char* AlertStateToString(AlertState state) {
  switch (state) {
    case AlertState::kPending: return "PENDING";
    case AlertState::kFiring: return "FIRING";
    case AlertState::kResolved: return "RESOLVED";
    default: return "UNKNOWN";
  }
}

// ============================================================================
// 告警定义
// ============================================================================
struct Alert {
  std::string alert_id;
  std::string name;
  std::string description;
  AlertSeverity severity;
  AlertState state;
  
  // 标签
  std::map<std::string, std::string> labels;
  
  // 注解
  std::map<std::string, std::string> annotations;
  
  // 时间信息
  std::chrono::system_clock::time_point starts_at;
  std::chrono::system_clock::time_point ends_at;
  std::chrono::system_clock::time_point updated_at;
  
  // 指纹 (用于去重)
  std::string fingerprint;
  
  // 生成器 URL
  std::string generator_url;
  
  std::string ToJson() const;
};

// ============================================================================
// 告警规则
// ============================================================================
struct AlertRule {
  std::string name;
  std::string description;
  AlertSeverity severity;
  
  // 表达式 (类似 PromQL)
  std::string expr;
  
  // 持续时间 (告警触发前需要持续的时间)
  std::chrono::seconds for_duration{0};
  
  // 标签
  std::map<std::string, std::string> labels;
  
  // 注解模板
  std::map<std::string, std::string> annotations;
  
  // 是否启用
  bool enabled = true;
};

// ============================================================================
// 告警通知渠道接口
// ============================================================================
class AlertNotifier {
 public:
  virtual ~AlertNotifier() = default;
  
  // 发送告警
  virtual bool Send(const Alert& alert) = 0;
  
  // 获取渠道名称
  virtual std::string Name() const = 0;
};

// ============================================================================
// Webhook 通知器
// ============================================================================
class WebhookNotifier : public AlertNotifier {
 public:
  explicit WebhookNotifier(const std::string& url);
  
  bool Send(const Alert& alert) override;
  std::string Name() const override { return "webhook"; }

 private:
  std::string url_;
};

// ============================================================================
// 企业微信通知器
// ============================================================================
class WeChatWorkNotifier : public AlertNotifier {
 public:
  explicit WeChatWorkNotifier(const std::string& webhook_url);
  
  bool Send(const Alert& alert) override;
  std::string Name() const override { return "wechat_work"; }

 private:
  std::string webhook_url_;
};

// ============================================================================
// 邮件通知器
// ============================================================================
class EmailNotifier : public AlertNotifier {
 public:
  EmailNotifier(const std::string& smtp_server, int port,
                const std::string& username, const std::string& password,
                const std::vector<std::string>& recipients);
  
  bool Send(const Alert& alert) override;
  std::string Name() const override { return "email"; }

 private:
  std::string smtp_server_;
  int port_;
  std::string username_;
  std::string password_;
  std::vector<std::string> recipients_;
};

// ============================================================================
// 告警管理器
// ============================================================================
class AlertManager {
 public:
  AlertManager();
  ~AlertManager();
  
  // 单例访问
  static AlertManager& Instance();
  
  // 启动/停止
  void Start();
  void Stop();
  
  // 添加告警规则
  void AddRule(const AlertRule& rule);
  void RemoveRule(const std::string& name);
  std::vector<AlertRule> GetRules() const;
  
  // 添加通知渠道
  void AddNotifier(std::shared_ptr<AlertNotifier> notifier);
  void RemoveNotifier(const std::string& name);
  
  // 手动触发告警
  void Fire(const std::string& name, const std::string& description,
            AlertSeverity severity,
            const std::map<std::string, std::string>& labels = {},
            const std::map<std::string, std::string>& annotations = {});
  
  // 解除告警
  void Resolve(const std::string& fingerprint);
  
  // 获取当前告警
  std::vector<Alert> GetActiveAlerts() const;
  std::vector<Alert> GetAlertHistory(size_t limit = 100) const;
  
  // 静默告警
  void Silence(const std::string& fingerprint, 
               std::chrono::system_clock::time_point until);
  void Unsilence(const std::string& fingerprint);
  
  // 配置
  void SetRepeatInterval(std::chrono::seconds interval) {
    repeat_interval_ = interval;
  }
  void SetGroupWait(std::chrono::seconds wait) {
    group_wait_ = wait;
  }

 private:
  void EvaluateLoop();
  void NotifyLoop();
  std::string GenerateFingerprint(const std::string& name,
                                  const std::map<std::string, std::string>& labels);
  void NotifyAlert(const Alert& alert);
  
  std::vector<AlertRule> rules_;
  std::map<std::string, Alert> active_alerts_;
  std::vector<Alert> alert_history_;
  std::map<std::string, std::chrono::system_clock::time_point> silenced_;
  std::vector<std::shared_ptr<AlertNotifier>> notifiers_;
  
  std::chrono::seconds repeat_interval_{3600};
  std::chrono::seconds group_wait_{30};
  
  std::atomic<bool> running_{false};
  std::unique_ptr<std::thread> evaluate_thread_;
  std::unique_ptr<std::thread> notify_thread_;
  std::condition_variable cv_;
  mutable std::mutex mutex_;
};

// ============================================================================
// 预定义告警规则
// ============================================================================
namespace PredefinedAlerts {

// 任务相关告警
AlertRule TaskQueueHighRule();
AlertRule TaskFailureRateHighRule();
AlertRule TaskTimeoutRule();
AlertRule TaskRetryExhaustedRule();

// Worker 相关告警
AlertRule WorkerOfflineRule();
AlertRule WorkerHighLoadRule();
AlertRule WorkerResourceExhaustedRule();

// 调度器相关告警
AlertRule SchedulerBacklogHighRule();
AlertRule SchedulerLatencyHighRule();

// 数据传输相关告警
AlertRule FileTransferSlowRule();
AlertRule FileTransferFailureRule();

}  // namespace PredefinedAlerts

}  // namespace observability
}  // namespace tendisplus
