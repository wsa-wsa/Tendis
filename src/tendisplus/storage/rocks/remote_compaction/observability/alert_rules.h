// Copyright (c) 2024-present, Tencent Inc. All rights reserved.
// 告警规则配置

#pragma once

#include <string>
#include <vector>
#include <chrono>
#include <functional>
#include <memory>
#include <unordered_map>

namespace tendisplus {
namespace observability {

// ============================================================================
// 告警级别
// ============================================================================
enum class AlertSeverity {
  kInfo = 0,
  kWarning = 1,
  kCritical = 2,
  kEmergency = 3
};

// ============================================================================
// 告警状态
// ============================================================================
enum class AlertState {
  kInactive = 0,
  kPending = 1,
  kFiring = 2,
  kResolved = 3
};

// ============================================================================
// 告警规则定义
// ============================================================================
struct AlertRule {
  std::string name;
  std::string description;
  std::string expr;  // PromQL 表达式
  AlertSeverity severity;
  std::chrono::seconds for_duration;  // 持续时间阈值
  std::unordered_map<std::string, std::string> labels;
  std::unordered_map<std::string, std::string> annotations;
  
  AlertRule() : severity(AlertSeverity::kWarning), for_duration(60) {}
};

// ============================================================================
// 告警实例
// ============================================================================
struct Alert {
  std::string rule_name;
  AlertSeverity severity;
  AlertState state;
  std::chrono::system_clock::time_point started_at;
  std::chrono::system_clock::time_point fired_at;
  std::chrono::system_clock::time_point resolved_at;
  std::unordered_map<std::string, std::string> labels;
  std::unordered_map<std::string, std::string> annotations;
  double value;
};

// ============================================================================
// 通知渠道类型
// ============================================================================
enum class NotificationChannel {
  kEmail = 0,
  kSlack = 1,
  kPagerDuty = 2,
  kWebhook = 3,
  kWechat = 4,
  kDingTalk = 5
};

// ============================================================================
// 告警规则管理器
// ============================================================================
class AlertRuleManager {
 public:
  AlertRuleManager() = default;
  ~AlertRuleManager() = default;
  
  // =========================================================================
  // 预定义告警规则 - 资源类
  // =========================================================================
  static std::vector<AlertRule> GetResourceAlertRules() {
    std::vector<AlertRule> rules;
    
    // CPU 高水位告警
    AlertRule cpu_high;
    cpu_high.name = "HighCPUUsage";
    cpu_high.description = "Worker CPU usage is above 80%";
    cpu_high.expr = "tendis_worker_cpu_usage > 80";
    cpu_high.severity = AlertSeverity::kWarning;
    cpu_high.for_duration = std::chrono::seconds(300);
    cpu_high.labels["category"] = "resource";
    cpu_high.annotations["summary"] = "High CPU usage detected on {{ $labels.worker_id }}";
    cpu_high.annotations["description"] = "Worker {{ $labels.worker_id }} CPU usage is {{ $value }}%";
    rules.push_back(cpu_high);
    
    // CPU 紧急告警
    AlertRule cpu_critical;
    cpu_critical.name = "CriticalCPUUsage";
    cpu_critical.description = "Worker CPU usage is above 95%";
    cpu_critical.expr = "tendis_worker_cpu_usage > 95";
    cpu_critical.severity = AlertSeverity::kCritical;
    cpu_critical.for_duration = std::chrono::seconds(60);
    cpu_critical.labels["category"] = "resource";
    rules.push_back(cpu_critical);
    
    // 内存高水位告警
    AlertRule memory_high;
    memory_high.name = "HighMemoryUsage";
    memory_high.description = "Worker memory usage is above 80%";
    memory_high.expr = "tendis_worker_memory_usage > 80";
    memory_high.severity = AlertSeverity::kWarning;
    memory_high.for_duration = std::chrono::seconds(300);
    memory_high.labels["category"] = "resource";
    rules.push_back(memory_high);
    
    // 磁盘空间告警
    AlertRule disk_high;
    disk_high.name = "HighDiskUsage";
    disk_high.description = "Worker disk usage is above 85%";
    disk_high.expr = "tendis_worker_disk_usage > 85";
    disk_high.severity = AlertSeverity::kWarning;
    disk_high.for_duration = std::chrono::seconds(300);
    disk_high.labels["category"] = "resource";
    rules.push_back(disk_high);
    
    // 磁盘空间紧急告警
    AlertRule disk_critical;
    disk_critical.name = "CriticalDiskUsage";
    disk_critical.description = "Worker disk usage is above 95%";
    disk_critical.expr = "tendis_worker_disk_usage > 95";
    disk_critical.severity = AlertSeverity::kCritical;
    disk_critical.for_duration = std::chrono::seconds(60);
    disk_critical.labels["category"] = "resource";
    rules.push_back(disk_critical);
    
    // 网络带宽告警
    AlertRule network_high;
    network_high.name = "HighNetworkUsage";
    network_high.description = "Network bandwidth usage is above 80%";
    network_high.expr = "tendis_network_bandwidth_usage > 80";
    network_high.severity = AlertSeverity::kWarning;
    network_high.for_duration = std::chrono::seconds(300);
    network_high.labels["category"] = "resource";
    rules.push_back(network_high);
    
    return rules;
  }
  
  // =========================================================================
  // 预定义告警规则 - 任务类
  // =========================================================================
  static std::vector<AlertRule> GetTaskAlertRules() {
    std::vector<AlertRule> rules;
    
    // 任务失败率告警
    AlertRule failure_rate;
    failure_rate.name = "HighTaskFailureRate";
    failure_rate.description = "Task failure rate is above 5%";
    failure_rate.expr = "sum(rate(tendis_tasks_failed_total[5m])) / sum(rate(tendis_tasks_submitted_total[5m])) * 100 > 5";
    failure_rate.severity = AlertSeverity::kWarning;
    failure_rate.for_duration = std::chrono::seconds(300);
    failure_rate.labels["category"] = "task";
    rules.push_back(failure_rate);
    
    // 任务失败率紧急告警
    AlertRule failure_critical;
    failure_critical.name = "CriticalTaskFailureRate";
    failure_critical.description = "Task failure rate is above 20%";
    failure_critical.expr = "sum(rate(tendis_tasks_failed_total[5m])) / sum(rate(tendis_tasks_submitted_total[5m])) * 100 > 20";
    failure_critical.severity = AlertSeverity::kCritical;
    failure_critical.for_duration = std::chrono::seconds(60);
    failure_critical.labels["category"] = "task";
    rules.push_back(failure_critical);
    
    // 队列积压告警
    AlertRule queue_backlog;
    queue_backlog.name = "TaskQueueBacklog";
    queue_backlog.description = "Task queue length is above 500";
    queue_backlog.expr = "sum(tendis_queue_length) > 500";
    queue_backlog.severity = AlertSeverity::kWarning;
    queue_backlog.for_duration = std::chrono::seconds(300);
    queue_backlog.labels["category"] = "task";
    rules.push_back(queue_backlog);
    
    // 队列严重积压告警
    AlertRule queue_critical;
    queue_critical.name = "CriticalTaskQueueBacklog";
    queue_critical.description = "Task queue length is above 2000";
    queue_critical.expr = "sum(tendis_queue_length) > 2000";
    queue_critical.severity = AlertSeverity::kCritical;
    queue_critical.for_duration = std::chrono::seconds(60);
    queue_critical.labels["category"] = "task";
    rules.push_back(queue_critical);
    
    // 任务执行超时告警
    AlertRule task_timeout;
    task_timeout.name = "TaskExecutionTimeout";
    task_timeout.description = "Task execution time exceeds threshold";
    task_timeout.expr = "histogram_quantile(0.99, sum(rate(tendis_task_duration_seconds_bucket[5m])) by (le)) > 3600";
    task_timeout.severity = AlertSeverity::kWarning;
    task_timeout.for_duration = std::chrono::seconds(300);
    task_timeout.labels["category"] = "task";
    rules.push_back(task_timeout);
    
    // 排队时间过长告警
    AlertRule queue_wait;
    queue_wait.name = "LongQueueWaitTime";
    queue_wait.description = "Task queue wait time is too long";
    queue_wait.expr = "histogram_quantile(0.95, sum(rate(tendis_queue_wait_seconds_bucket[5m])) by (le)) > 600";
    queue_wait.severity = AlertSeverity::kWarning;
    queue_wait.for_duration = std::chrono::seconds(300);
    queue_wait.labels["category"] = "task";
    rules.push_back(queue_wait);
    
    return rules;
  }
  
  // =========================================================================
  // 预定义告警规则 - Worker 类
  // =========================================================================
  static std::vector<AlertRule> GetWorkerAlertRules() {
    std::vector<AlertRule> rules;
    
    // Worker 离线告警
    AlertRule worker_offline;
    worker_offline.name = "WorkerOffline";
    worker_offline.description = "Worker is offline";
    worker_offline.expr = "tendis_worker_status == 0";
    worker_offline.severity = AlertSeverity::kCritical;
    worker_offline.for_duration = std::chrono::seconds(60);
    worker_offline.labels["category"] = "worker";
    rules.push_back(worker_offline);
    
    // 心跳超时告警
    AlertRule heartbeat_timeout;
    heartbeat_timeout.name = "WorkerHeartbeatTimeout";
    heartbeat_timeout.description = "Worker heartbeat timeout detected";
    heartbeat_timeout.expr = "rate(tendis_worker_heartbeat_timeouts_total[5m]) > 0";
    heartbeat_timeout.severity = AlertSeverity::kWarning;
    heartbeat_timeout.for_duration = std::chrono::seconds(60);
    heartbeat_timeout.labels["category"] = "worker";
    rules.push_back(heartbeat_timeout);
    
    // Worker 数量不足告警
    AlertRule worker_count;
    worker_count.name = "InsufficientWorkers";
    worker_count.description = "Number of online workers is below threshold";
    worker_count.expr = "sum(tendis_workers_online) < 2";
    worker_count.severity = AlertSeverity::kCritical;
    worker_count.for_duration = std::chrono::seconds(60);
    worker_count.labels["category"] = "worker";
    rules.push_back(worker_count);
    
    // 负载不均衡告警
    AlertRule load_imbalance;
    load_imbalance.name = "WorkerLoadImbalance";
    load_imbalance.description = "Worker load is severely imbalanced";
    load_imbalance.expr = "tendis_load_balance_score < 0.5";
    load_imbalance.severity = AlertSeverity::kWarning;
    load_imbalance.for_duration = std::chrono::seconds(600);
    load_imbalance.labels["category"] = "worker";
    rules.push_back(load_imbalance);
    
    return rules;
  }
  
  // =========================================================================
  // 预定义告警规则 - 性能类
  // =========================================================================
  static std::vector<AlertRule> GetPerformanceAlertRules() {
    std::vector<AlertRule> rules;
    
    // 端到端延迟告警
    AlertRule latency_high;
    latency_high.name = "HighEndToEndLatency";
    latency_high.description = "End-to-end latency P99 is above threshold";
    latency_high.expr = "histogram_quantile(0.99, sum(rate(tendis_end_to_end_latency_seconds_bucket[5m])) by (le)) > 60";
    latency_high.severity = AlertSeverity::kWarning;
    latency_high.for_duration = std::chrono::seconds(300);
    latency_high.labels["category"] = "performance";
    rules.push_back(latency_high);
    
    // 写放大告警
    AlertRule write_amp;
    write_amp.name = "HighWriteAmplification";
    write_amp.description = "Write amplification is above threshold";
    write_amp.expr = "tendis_write_amplification > 20";
    write_amp.severity = AlertSeverity::kWarning;
    write_amp.for_duration = std::chrono::seconds(600);
    write_amp.labels["category"] = "performance";
    rules.push_back(write_amp);
    
    // 数据传输速率低告警
    AlertRule transfer_slow;
    transfer_slow.name = "SlowDataTransfer";
    transfer_slow.description = "Data transfer rate is below threshold";
    transfer_slow.expr = "sum(rate(tendis_file_transfer_bytes_total[5m])) < 10485760";  // 10MB/s
    transfer_slow.severity = AlertSeverity::kWarning;
    transfer_slow.for_duration = std::chrono::seconds(300);
    transfer_slow.labels["category"] = "performance";
    rules.push_back(transfer_slow);
    
    return rules;
  }
  
  // =========================================================================
  // 预定义告警规则 - 业务类
  // =========================================================================
  static std::vector<AlertRule> GetBusinessAlertRules() {
    std::vector<AlertRule> rules;
    
    // SLA 违约告警
    AlertRule sla_violation;
    sla_violation.name = "SLAViolation";
    sla_violation.description = "SLA compliance rate is below 99%";
    sla_violation.expr = "tendis_sla_compliance_rate < 0.99";
    sla_violation.severity = AlertSeverity::kCritical;
    sla_violation.for_duration = std::chrono::seconds(300);
    sla_violation.labels["category"] = "business";
    rules.push_back(sla_violation);
    
    // QPS 影响告警
    AlertRule qps_impact;
    qps_impact.name = "QPSImpact";
    qps_impact.description = "QPS dropped significantly during remote tasks";
    qps_impact.expr = "(tendis_qps_baseline - tendis_qps_current) / tendis_qps_baseline * 100 > 10";
    qps_impact.severity = AlertSeverity::kWarning;
    qps_impact.for_duration = std::chrono::seconds(300);
    qps_impact.labels["category"] = "business";
    rules.push_back(qps_impact);
    
    // 业务延迟影响告警
    AlertRule latency_impact;
    latency_impact.name = "BusinessLatencyImpact";
    latency_impact.description = "Business P99 latency increased significantly";
    latency_impact.expr = "(tendis_latency_p99_current_ms - tendis_latency_p99_baseline_ms) / tendis_latency_p99_baseline_ms * 100 > 20";
    latency_impact.severity = AlertSeverity::kWarning;
    latency_impact.for_duration = std::chrono::seconds(300);
    latency_impact.labels["category"] = "business";
    rules.push_back(latency_impact);
    
    return rules;
  }
  
  // =========================================================================
  // 获取所有告警规则
  // =========================================================================
  static std::vector<AlertRule> GetAllAlertRules() {
    std::vector<AlertRule> all_rules;
    
    auto resource_rules = GetResourceAlertRules();
    all_rules.insert(all_rules.end(), resource_rules.begin(), resource_rules.end());
    
    auto task_rules = GetTaskAlertRules();
    all_rules.insert(all_rules.end(), task_rules.begin(), task_rules.end());
    
    auto worker_rules = GetWorkerAlertRules();
    all_rules.insert(all_rules.end(), worker_rules.begin(), worker_rules.end());
    
    auto perf_rules = GetPerformanceAlertRules();
    all_rules.insert(all_rules.end(), perf_rules.begin(), perf_rules.end());
    
    auto biz_rules = GetBusinessAlertRules();
    all_rules.insert(all_rules.end(), biz_rules.begin(), biz_rules.end());
    
    return all_rules;
  }
  
  // =========================================================================
  // 生成 Prometheus AlertManager 配置
  // =========================================================================
  static std::string GeneratePrometheusAlertRulesYaml() {
    std::string yaml = "groups:\n";
    yaml += "  - name: tendis_remote_compaction\n";
    yaml += "    rules:\n";
    
    auto rules = GetAllAlertRules();
    for (const auto& rule : rules) {
      yaml += "      - alert: " + rule.name + "\n";
      yaml += "        expr: " + rule.expr + "\n";
      yaml += "        for: " + std::to_string(rule.for_duration.count()) + "s\n";
      yaml += "        labels:\n";
      yaml += "          severity: " + SeverityToString(rule.severity) + "\n";
      for (const auto& label : rule.labels) {
        yaml += "          " + label.first + ": " + label.second + "\n";
      }
      yaml += "        annotations:\n";
      yaml += "          description: \"" + rule.description + "\"\n";
      for (const auto& ann : rule.annotations) {
        yaml += "          " + ann.first + ": \"" + ann.second + "\"\n";
      }
    }
    
    return yaml;
  }
  
 private:
  static std::string SeverityToString(AlertSeverity severity) {
    switch (severity) {
      case AlertSeverity::kInfo: return "info";
      case AlertSeverity::kWarning: return "warning";
      case AlertSeverity::kCritical: return "critical";
      case AlertSeverity::kEmergency: return "emergency";
      default: return "unknown";
    }
  }
};

// ============================================================================
// 告警处理器
// ============================================================================
class AlertHandler {
 public:
  using AlertCallback = std::function<void(const Alert&)>;
  
  AlertHandler() = default;
  ~AlertHandler() = default;
  
  // 注册告警回调
  void RegisterCallback(AlertSeverity min_severity, AlertCallback callback) {
    callbacks_[min_severity] = std::move(callback);
  }
  
  // 处理告警
  void HandleAlert(const Alert& alert) {
    for (const auto& pair : callbacks_) {
      if (alert.severity >= pair.first) {
        pair.second(alert);
      }
    }
  }
  
 private:
  std::unordered_map<AlertSeverity, AlertCallback> callbacks_;
};

}  // namespace observability
}  // namespace tendisplus
