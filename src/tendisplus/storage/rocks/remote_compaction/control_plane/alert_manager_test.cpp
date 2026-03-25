// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

// Unit tests for AlertManager (alert_manager.h/cc)
// Tests: rule management, severity/state enums, threshold evaluation,
//        alert firing/resolution, default rules, metrics provider mock

#include "gtest/gtest.h"
#include "alert_manager.h"

#include <chrono>
#include <thread>

namespace tendisplus {
namespace control_plane {

// ============================================================================
// 枚举转换测试
// ============================================================================
TEST(AlertManager, AlertSeverityToString) {
  EXPECT_STREQ(AlertSeverityToString(AlertSeverity::kInfo), "Info");
  EXPECT_STREQ(AlertSeverityToString(AlertSeverity::kWarning), "Warning");
  EXPECT_STREQ(AlertSeverityToString(AlertSeverity::kCritical), "Critical");
  EXPECT_STREQ(
    AlertSeverityToString(static_cast<AlertSeverity>(99)), "Unknown");
}

TEST(AlertManager, AlertStateToString) {
  EXPECT_STREQ(AlertStateToString(AlertState::kInactive), "Inactive");
  EXPECT_STREQ(AlertStateToString(AlertState::kFiring), "Firing");
  EXPECT_STREQ(AlertStateToString(AlertState::kResolved), "Resolved");
  EXPECT_STREQ(AlertStateToString(static_cast<AlertState>(99)), "Unknown");
}

TEST(AlertManager, AlertMetricToString) {
  EXPECT_STREQ(AlertMetricToString(AlertMetric::kPendingTaskCount),
               "pending_task_count");
  EXPECT_STREQ(AlertMetricToString(AlertMetric::kFailedTaskRate),
               "failed_task_rate");
  EXPECT_STREQ(AlertMetricToString(AlertMetric::kOnlineWorkerCount),
               "online_worker_count");
  EXPECT_STREQ(AlertMetricToString(AlertMetric::kClusterDiskUsage),
               "cluster_disk_usage");
}

TEST(AlertManager, AlertOperatorToString) {
  EXPECT_STREQ(AlertOperatorToString(AlertOperator::kGreaterThan), ">");
  EXPECT_STREQ(AlertOperatorToString(AlertOperator::kGreaterThanOrEqual), ">=");
  EXPECT_STREQ(AlertOperatorToString(AlertOperator::kLessThan), "<");
  EXPECT_STREQ(AlertOperatorToString(AlertOperator::kLessThanOrEqual), "<=");
  EXPECT_STREQ(AlertOperatorToString(AlertOperator::kEqual), "==");
}

// ============================================================================
// 规则管理测试
// ============================================================================
TEST(AlertManager, AddAndGetRule) {
  AlertManagerConfig config;
  config.enable_default_rules = false;
  AlertManager mgr(config);

  AlertRule rule;
  rule.rule_id = "test_rule_1";
  rule.name = "Test Rule 1";
  rule.metric = AlertMetric::kPendingTaskCount;
  rule.op = AlertOperator::kGreaterThan;
  rule.threshold = 50;
  rule.severity = AlertSeverity::kWarning;
  rule.duration_sec = 10;

  mgr.AddRule(rule);

  auto rules = mgr.GetAllRules();
  EXPECT_EQ(rules.size(), 1u);
  EXPECT_EQ(rules[0].rule_id, "test_rule_1");
  EXPECT_EQ(rules[0].name, "Test Rule 1");
  EXPECT_DOUBLE_EQ(rules[0].threshold, 50.0);

  auto* found = mgr.GetRule("test_rule_1");
  EXPECT_NE(found, nullptr);
  EXPECT_EQ(found->rule_id, "test_rule_1");

  // 查询不存在的规则
  auto* notfound = mgr.GetRule("nonexistent");
  EXPECT_EQ(notfound, nullptr);
}

TEST(AlertManager, RemoveRule) {
  AlertManagerConfig config;
  config.enable_default_rules = false;
  AlertManager mgr(config);

  AlertRule rule;
  rule.rule_id = "to_remove";
  rule.name = "To Remove";
  rule.metric = AlertMetric::kRunningTaskCount;
  rule.op = AlertOperator::kGreaterThan;
  rule.threshold = 10;
  rule.severity = AlertSeverity::kInfo;
  rule.duration_sec = 5;
  mgr.AddRule(rule);

  EXPECT_EQ(mgr.GetAllRules().size(), 1u);

  mgr.RemoveRule("to_remove");
  EXPECT_EQ(mgr.GetAllRules().size(), 0u);

  // 移除不存在的规则 — 不崩溃
  mgr.RemoveRule("nonexistent");
}

TEST(AlertManager, EnableDisableRule) {
  AlertManagerConfig config;
  config.enable_default_rules = false;
  AlertManager mgr(config);

  AlertRule rule;
  rule.rule_id = "toggle_rule";
  rule.name = "Toggle Rule";
  rule.metric = AlertMetric::kPendingTaskCount;
  rule.op = AlertOperator::kGreaterThan;
  rule.threshold = 100;
  rule.severity = AlertSeverity::kWarning;
  rule.duration_sec = 10;
  rule.enabled = true;
  mgr.AddRule(rule);

  // 禁用
  mgr.EnableRule("toggle_rule", false);
  auto* r = mgr.GetRule("toggle_rule");
  EXPECT_NE(r, nullptr);
  EXPECT_FALSE(r->enabled);

  // 重新启用
  mgr.EnableRule("toggle_rule", true);
  r = mgr.GetRule("toggle_rule");
  EXPECT_TRUE(r->enabled);

  // 启用不存在的规则 — 不崩溃
  mgr.EnableRule("nonexistent", true);
}

// ============================================================================
// 默认规则初始化测试
// ============================================================================
TEST(AlertManager, DefaultRulesInitialized) {
  AlertManagerConfig config;
  config.enable_default_rules = true;
  AlertManager mgr(config);

  // 启动 AlertManager 触发默认规则加载
  MetricsSnapshot snapshot;
  mgr.Start([snapshot]() { return snapshot; });

  // 等待启动
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  auto rules = mgr.GetAllRules();
  // 应该有 9 条默认规则
  EXPECT_EQ(rules.size(), 9u);

  // 验证关键规则存在
  bool found_pending_high = false;
  bool found_no_workers = false;
  bool found_disk_critical = false;
  for (const auto& r : rules) {
    if (r.rule_id == "pending_tasks_high") found_pending_high = true;
    if (r.rule_id == "no_workers_online") found_no_workers = true;
    if (r.rule_id == "disk_usage_critical") found_disk_critical = true;
  }
  EXPECT_TRUE(found_pending_high);
  EXPECT_TRUE(found_no_workers);
  EXPECT_TRUE(found_disk_critical);

  mgr.Stop();
}

TEST(AlertManager, NoDefaultRulesWhenDisabled) {
  AlertManagerConfig config;
  config.enable_default_rules = false;
  AlertManager mgr(config);

  MetricsSnapshot snapshot;
  mgr.Start([snapshot]() { return snapshot; });
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  EXPECT_EQ(mgr.GetAllRules().size(), 0u);

  mgr.Stop();
}

// ============================================================================
// 告警触发与恢复测试
// ============================================================================
TEST(AlertManager, AlertFiringOnThresholdExceeded) {
  AlertManagerConfig config;
  config.enable_default_rules = false;
  config.check_interval_sec = 1;  // 1秒检查一次
  AlertManager mgr(config);

  // 添加规则：pending > 50, 持续 0 秒 → 立即触发
  AlertRule rule;
  rule.rule_id = "test_pending";
  rule.name = "Test Pending";
  rule.metric = AlertMetric::kPendingTaskCount;
  rule.op = AlertOperator::kGreaterThan;
  rule.threshold = 50;
  rule.severity = AlertSeverity::kWarning;
  rule.duration_sec = 0;  // 立即触发
  mgr.AddRule(rule);

  // Mock 指标 — pending = 200 (超过阈值)
  MetricsSnapshot snapshot;
  snapshot.pending_tasks = 200;

  mgr.Start([snapshot]() { return snapshot; });

  // 等待检查循环执行
  std::this_thread::sleep_for(std::chrono::milliseconds(1500));

  // 应该有活跃告警
  EXPECT_GE(mgr.GetActiveAlertCount(), 1u);

  auto alerts = mgr.GetActiveAlerts();
  EXPECT_GE(alerts.size(), 1u);
  if (!alerts.empty()) {
    EXPECT_EQ(alerts[0].rule_id, "test_pending");
    EXPECT_EQ(alerts[0].state, AlertState::kFiring);
    EXPECT_EQ(alerts[0].severity, AlertSeverity::kWarning);
    EXPECT_GT(alerts[0].current_value, 50.0);
  }

  mgr.Stop();
}

TEST(AlertManager, AlertResolution) {
  AlertManagerConfig config;
  config.enable_default_rules = false;
  config.check_interval_sec = 1;
  AlertManager mgr(config);

  // 添加规则：pending > 50, 持续 0 秒
  AlertRule rule;
  rule.rule_id = "resolve_test";
  rule.name = "Resolve Test";
  rule.metric = AlertMetric::kPendingTaskCount;
  rule.op = AlertOperator::kGreaterThan;
  rule.threshold = 50;
  rule.severity = AlertSeverity::kWarning;
  rule.duration_sec = 0;
  mgr.AddRule(rule);

  // 阶段1：pending = 200 → 触发告警
  MetricsSnapshot high_snapshot;
  high_snapshot.pending_tasks = 200;
  std::atomic<bool> use_high{true};

  mgr.Start([&]() {
    if (use_high.load()) return high_snapshot;
    MetricsSnapshot low;
    low.pending_tasks = 10;
    return low;
  });

  // 等待告警触发
  std::this_thread::sleep_for(std::chrono::milliseconds(1500));
  EXPECT_GE(mgr.GetActiveAlertCount(), 1u);

  // 阶段2：pending = 10 → 告警应恢复
  use_high.store(false);
  std::this_thread::sleep_for(std::chrono::milliseconds(1500));
  EXPECT_EQ(mgr.GetActiveAlertCount(), 0u);

  // 历史应有记录（触发 + 恢复）
  auto history = mgr.GetAlertHistory(10);
  EXPECT_GE(history.size(), 2u);

  mgr.Stop();
}

// ============================================================================
// 按严重级别统计
// ============================================================================
TEST(AlertManager, AlertCountBySeverity) {
  AlertManagerConfig config;
  config.enable_default_rules = false;
  config.check_interval_sec = 1;
  AlertManager mgr(config);

  // 添加 Warning 规则
  AlertRule warn_rule;
  warn_rule.rule_id = "warn_1";
  warn_rule.name = "Warning 1";
  warn_rule.metric = AlertMetric::kPendingTaskCount;
  warn_rule.op = AlertOperator::kGreaterThan;
  warn_rule.threshold = 50;
  warn_rule.severity = AlertSeverity::kWarning;
  warn_rule.duration_sec = 0;
  mgr.AddRule(warn_rule);

  // 添加 Critical 规则
  AlertRule crit_rule;
  crit_rule.rule_id = "crit_1";
  crit_rule.name = "Critical 1";
  crit_rule.metric = AlertMetric::kClusterDiskUsage;
  crit_rule.op = AlertOperator::kGreaterThan;
  crit_rule.threshold = 0.9;
  crit_rule.severity = AlertSeverity::kCritical;
  crit_rule.duration_sec = 0;
  mgr.AddRule(crit_rule);

  // 两个指标都超阈值
  MetricsSnapshot snapshot;
  snapshot.pending_tasks = 200;
  snapshot.cluster_disk_usage = 0.95;

  mgr.Start([snapshot]() { return snapshot; });
  std::this_thread::sleep_for(std::chrono::milliseconds(1500));

  EXPECT_GE(mgr.GetAlertCountBySeverity(AlertSeverity::kWarning), 1u);
  EXPECT_GE(mgr.GetAlertCountBySeverity(AlertSeverity::kCritical), 1u);
  EXPECT_EQ(mgr.GetAlertCountBySeverity(AlertSeverity::kInfo), 0u);

  mgr.Stop();
}

// ============================================================================
// 告警历史记录
// ============================================================================
TEST(AlertManager, AlertHistory) {
  AlertManagerConfig config;
  config.enable_default_rules = false;
  config.check_interval_sec = 1;
  config.max_history_size = 5;  // 最多保留 5 条
  AlertManager mgr(config);

  AlertRule rule;
  rule.rule_id = "history_test";
  rule.name = "History Test";
  rule.metric = AlertMetric::kPendingTaskCount;
  rule.op = AlertOperator::kGreaterThan;
  rule.threshold = 10;
  rule.severity = AlertSeverity::kInfo;
  rule.duration_sec = 0;
  mgr.AddRule(rule);

  MetricsSnapshot snapshot;
  snapshot.pending_tasks = 100;
  mgr.Start([snapshot]() { return snapshot; });
  std::this_thread::sleep_for(std::chrono::milliseconds(1500));

  auto history = mgr.GetAlertHistory(10);
  EXPECT_GT(history.size(), 0u);
  EXPECT_LE(history.size(), 5u);  // 受 max_history_size 限制

  mgr.Stop();
}

// ============================================================================
// 初始状态测试
// ============================================================================
TEST(AlertManager, InitialState) {
  AlertManagerConfig config;
  config.enable_default_rules = false;
  AlertManager mgr(config);

  EXPECT_FALSE(mgr.IsRunning());
  EXPECT_EQ(mgr.GetActiveAlertCount(), 0u);
  EXPECT_EQ(mgr.GetAllRules().size(), 0u);
  EXPECT_TRUE(mgr.GetActiveAlerts().empty());
  EXPECT_TRUE(mgr.GetAlertHistory().empty());
}

// ============================================================================
// 生命周期管理
// ============================================================================
TEST(AlertManager, StartStop) {
  AlertManagerConfig config;
  config.enable_default_rules = false;
  config.check_interval_sec = 1;
  AlertManager mgr(config);

  MetricsSnapshot snapshot;
  mgr.Start([snapshot]() { return snapshot; });
  EXPECT_TRUE(mgr.IsRunning());

  // 重复 Start 不崩溃
  mgr.Start([snapshot]() { return snapshot; });
  EXPECT_TRUE(mgr.IsRunning());

  mgr.Stop();
  EXPECT_FALSE(mgr.IsRunning());

  // 重复 Stop 不崩溃
  mgr.Stop();
  EXPECT_FALSE(mgr.IsRunning());
}

// ============================================================================
// 默认值验证
// ============================================================================
TEST(AlertManager, DefaultConfigValues) {
  AlertManagerConfig config;
  EXPECT_EQ(config.check_interval_sec, 10u);
  EXPECT_EQ(config.max_history_size, 1000u);
  EXPECT_TRUE(config.enable_default_rules);
}

TEST(AlertManager, MetricsSnapshotDefaults) {
  MetricsSnapshot snapshot;
  EXPECT_EQ(snapshot.pending_tasks, 0u);
  EXPECT_EQ(snapshot.running_tasks, 0u);
  EXPECT_EQ(snapshot.total_completed, 0u);
  EXPECT_EQ(snapshot.total_failed, 0u);
  EXPECT_DOUBLE_EQ(snapshot.failed_rate, 0.0);
  EXPECT_DOUBLE_EQ(snapshot.avg_queue_time_ms, 0.0);
  EXPECT_DOUBLE_EQ(snapshot.cluster_memory_usage, 0.0);
  EXPECT_DOUBLE_EQ(snapshot.cluster_disk_usage, 0.0);
  EXPECT_EQ(snapshot.online_workers, 0u);
}

TEST(AlertManager, AlertRuleDefaults) {
  AlertRule rule;
  EXPECT_TRUE(rule.enabled);
  EXPECT_EQ(rule.state, AlertState::kInactive);
  EXPECT_EQ(rule.first_triggered_ms, 0);
  EXPECT_EQ(rule.consecutive_triggers, 0u);
}

}  // namespace control_plane
}  // namespace tendisplus
