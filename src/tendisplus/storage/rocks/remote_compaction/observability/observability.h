// Copyright (c) 2024-present, Tencent Inc. All rights reserved.
// 观测平面主入口

#pragma once

#include <memory>
#include <string>

#include "alerting.h"
#include "logging.h"
#include "metrics.h"
#include "task_metrics.h"
#include "tracing.h"

namespace tendisplus {
namespace observability {

// ============================================================================
// 观测平面配置
// ============================================================================
struct ObservabilityConfig {
  // 指标配置
  bool enable_metrics = true;
  std::string metrics_endpoint = "/metrics";
  uint16_t metrics_port = 9090;
  
  // 追踪配置
  bool enable_tracing = true;
  std::string tracing_endpoint;  // OTLP endpoint
  double tracing_sample_rate = 1.0;  // 采样率 (0.0-1.0)
  
  // 日志配置
  bool enable_structured_logging = true;
  LogLevel log_level = LogLevel::kInfo;
  std::string log_path = "/var/log/tendis";
  size_t log_max_size_mb = 100;
  int log_max_files = 10;
  bool log_to_console = true;
  bool log_json_format = false;
  
  // 告警配置
  bool enable_alerting = true;
  std::string alertmanager_url;
  std::vector<std::string> webhook_urls;
  std::string wechat_work_webhook;
  
  // 健康检查配置
  bool enable_health_check = true;
  std::string health_endpoint = "/health";
  uint16_t health_port = 8080;
};

// ============================================================================
// 健康检查结果
// ============================================================================
struct HealthCheckResult {
  bool healthy = true;
  std::string status;  // "healthy", "degraded", "unhealthy"
  std::map<std::string, bool> components;
  std::map<std::string, std::string> details;
  std::chrono::system_clock::time_point timestamp;
  
  std::string ToJson() const;
};

// ============================================================================
// 健康检查器
// ============================================================================
class HealthChecker {
 public:
  using CheckFunction = std::function<bool(std::string& detail)>;
  
  HealthChecker();
  ~HealthChecker() = default;
  
  // 单例访问
  static HealthChecker& Instance();
  
  // 注册检查项
  void RegisterCheck(const std::string& name, CheckFunction check);
  void UnregisterCheck(const std::string& name);
  
  // 执行健康检查
  HealthCheckResult Check() const;
  
  // 检查单个组件
  bool CheckComponent(const std::string& name, std::string& detail) const;
  
  // 获取所有检查项
  std::vector<std::string> GetCheckNames() const;

 private:
  std::map<std::string, CheckFunction> checks_;
  mutable std::mutex mutex_;
};

// ============================================================================
// 观测平面服务
// ============================================================================
class Observability {
 public:
  explicit Observability(const ObservabilityConfig& config);
  ~Observability();
  
  // 禁止拷贝
  Observability(const Observability&) = delete;
  Observability& operator=(const Observability&) = delete;
  
  // 单例访问
  static Observability& Instance();
  static void Initialize(const ObservabilityConfig& config);
  static void Shutdown();
  
  // 生命周期管理
  void Start();
  void Stop();
  bool IsRunning() const { return running_.load(); }
  
  // =========================================================================
  // 组件访问
  // =========================================================================
  
  MetricsRegistry& GetMetricsRegistry() { return MetricsRegistry::Instance(); }
  Tracer& GetTracer() { return Tracer::Instance(); }
  LogManager& GetLogManager() { return LogManager::Instance(); }
  AlertManager& GetAlertManager() { return AlertManager::Instance(); }
  HealthChecker& GetHealthChecker() { return HealthChecker::Instance(); }
  
  TaskMetrics& GetTaskMetrics() { return TaskMetrics::Instance(); }
  WorkerMetrics& GetWorkerMetrics() { return WorkerMetrics::Instance(); }
  SchedulerMetrics& GetSchedulerMetrics() { return SchedulerMetrics::Instance(); }
  
  // =========================================================================
  // 便捷方法
  // =========================================================================
  
  // 导出所有指标 (Prometheus 格式)
  std::string ExportMetrics() const;
  
  // 获取健康状态
  HealthCheckResult GetHealth() const;
  
  // 获取活跃告警
  std::vector<Alert> GetActiveAlerts() const;
  
  // =========================================================================
  // HTTP 服务器 (用于暴露 /metrics, /health 等端点)
  // =========================================================================
  
  // 启动 HTTP 服务器
  void StartHttpServer();
  void StopHttpServer();

 private:
  void InitializeMetrics();
  void InitializeTracing();
  void InitializeLogging();
  void InitializeAlerting();
  void InitializeHealthChecks();
  void SetupDefaultAlertRules();
  
  ObservabilityConfig config_;
  std::atomic<bool> running_{false};
  
  // HTTP 服务器 (实际实现中添加)
  // std::unique_ptr<HttpServer> http_server_;
  
  // 单例实例
  static std::unique_ptr<Observability> instance_;
  static std::mutex instance_mutex_;
};

// ============================================================================
// 仪表盘数据
// ============================================================================
struct DashboardData {
  // 任务概览
  struct TaskOverview {
    uint64_t total_submitted = 0;
    uint64_t total_completed = 0;
    uint64_t total_failed = 0;
    uint64_t active_tasks = 0;
    uint64_t queued_tasks = 0;
    double avg_execution_time_sec = 0;
    double success_rate = 0;
  } tasks;
  
  // Worker 概览
  struct WorkerOverview {
    uint64_t total_workers = 0;
    uint64_t online_workers = 0;
    uint64_t busy_workers = 0;
    double avg_cpu_usage = 0;
    double avg_memory_usage = 0;
  } workers;
  
  // 资源概览
  struct ResourceOverview {
    uint32_t total_cpu_cores = 0;
    uint32_t available_cpu_cores = 0;
    uint64_t total_memory_mb = 0;
    uint64_t available_memory_mb = 0;
    uint64_t total_disk_mb = 0;
    uint64_t available_disk_mb = 0;
  } resources;
  
  // 性能概览
  struct PerformanceOverview {
    double tasks_per_second = 0;
    double bytes_per_second = 0;
    double avg_queue_wait_sec = 0;
    double avg_scheduling_latency_sec = 0;
  } performance;
  
  // 告警概览
  struct AlertOverview {
    uint64_t active_alerts = 0;
    uint64_t critical_alerts = 0;
    uint64_t warning_alerts = 0;
  } alerts;
  
  std::chrono::system_clock::time_point timestamp;
  
  std::string ToJson() const;
};

// ============================================================================
// 仪表盘数据收集器
// ============================================================================
class DashboardCollector {
 public:
  DashboardCollector();
  ~DashboardCollector() = default;
  
  static DashboardCollector& Instance();
  
  // 收集仪表盘数据
  DashboardData Collect() const;
  
  // 获取历史数据 (用于图表)
  std::vector<DashboardData> GetHistory(
      std::chrono::system_clock::time_point start,
      std::chrono::system_clock::time_point end,
      std::chrono::seconds interval) const;

 private:
  std::vector<DashboardData> history_;
  mutable std::mutex mutex_;
};

}  // namespace observability
}  // namespace tendisplus
