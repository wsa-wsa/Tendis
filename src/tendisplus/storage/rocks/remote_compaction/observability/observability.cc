// Copyright (c) 2024-present, Tencent Inc. All rights reserved.
// 观测平面统一入口实现

#include "observability.h"

#include <sstream>

namespace tendisplus {
namespace observability {

// ============================================================================
// Observability 实现
// ============================================================================

Observability& Observability::Instance() {
  static Observability instance;
  return instance;
}

Observability::Observability() : initialized_(false) {
}

Observability::~Observability() {
  Shutdown();
}

void Observability::Initialize(const ObservabilityConfig& config) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  if (initialized_) {
    return;
  }
  
  config_ = config;
  
  // 初始化日志系统
  InitializeLogging();
  
  // 初始化指标系统
  InitializeMetrics();
  
  // 初始化追踪系统
  InitializeTracing();
  
  // 初始化告警系统
  InitializeAlerting();
  
  initialized_ = true;
}

void Observability::Shutdown() {
  std::lock_guard<std::mutex> lock(mutex_);
  
  if (!initialized_) {
    return;
  }
  
  // 停止告警系统
  AlertManager::Instance().Stop();
  
  // 停止追踪系统
  Tracer::Instance().Stop();
  
  // 停止日志系统
  Logger::Instance().Stop();
  
  initialized_ = false;
}

void Observability::InitializeLogging() {
  auto& logger = Logger::Instance();
  
  // 设置日志级别
  logger.SetMinLevel(config_.log_level);
  
  // 设置日志格式
  logger.SetFormat(config_.log_format);
  
  // 添加控制台输出
  if (config_.enable_console_log) {
    logger.AddSink(std::make_shared<ConsoleSink>());
  }
  
  // 添加文件输出
  if (!config_.log_file.empty()) {
    auto file_sink = std::make_shared<FileSink>(config_.log_file);
    file_sink->SetMaxSize(config_.log_max_size);
    logger.AddSink(file_sink);
  }
  
  // 启动日志系统
  logger.Start();
}

void Observability::InitializeMetrics() {
  // 指标系统使用静态单例，无需特殊初始化
  // 各个组件会自动注册指标
}

void Observability::InitializeTracing() {
  auto& tracer = Tracer::Instance();
  
  // 设置采样率
  tracer.SetSamplingRate(config_.trace_sampling_rate);
  
  // 添加日志导出器
  if (config_.enable_trace_log) {
    tracer.AddExporter(std::make_shared<LogSpanExporter>());
  }
  
  // 添加 Jaeger 导出器
  if (!config_.jaeger_endpoint.empty()) {
    tracer.AddExporter(std::make_shared<JaegerExporter>(
        config_.jaeger_endpoint, config_.service_name));
  }
  
  // 启动追踪系统
  tracer.Start();
}

void Observability::InitializeAlerting() {
  auto& alert_manager = AlertManager::Instance();
  
  // 设置评估间隔
  alert_manager.SetEvaluationInterval(config_.alert_evaluation_interval);
  
  // 添加日志通知器
  if (config_.enable_alert_log) {
    alert_manager.AddNotifier(std::make_shared<LogAlertNotifier>());
  }
  
  // 添加 Webhook 通知器
  if (!config_.alert_webhook_url.empty()) {
    alert_manager.AddNotifier(std::make_shared<WebhookAlertNotifier>(
        config_.alert_webhook_url));
  }
  
  // 启动告警系统
  alert_manager.Start();
}

// ============================================================================
// MetricsHttpHandler 实现
// ============================================================================

MetricsHttpHandler::MetricsHttpHandler() : running_(false), port_(9090) {
}

MetricsHttpHandler::~MetricsHttpHandler() {
  Stop();
}

void MetricsHttpHandler::Start(int port) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  if (running_) return;
  
  port_ = port;
  running_ = true;
  
  // TODO: 实际实现需要启动 HTTP 服务器
  // 这里只是示例框架
  server_thread_ = std::thread(&MetricsHttpHandler::ServerLoop, this);
}

void MetricsHttpHandler::Stop() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!running_) return;
    running_ = false;
  }
  
  if (server_thread_.joinable()) {
    server_thread_.join();
  }
}

std::string MetricsHttpHandler::HandleRequest(const std::string& path) {
  if (path == "/metrics") {
    return MetricsRegistry::Instance().ExportPrometheus();
  } else if (path == "/health") {
    return "{\"status\":\"ok\"}";
  } else if (path == "/alerts") {
    return GetAlertsJson();
  }
  
  return "Not Found";
}

std::string MetricsHttpHandler::GetAlertsJson() {
  auto alerts = AlertManager::Instance().GetActiveAlerts();
  
  std::ostringstream oss;
  oss << "[";
  
  bool first = true;
  for (const auto& alert : alerts) {
    if (!first) oss << ",";
    oss << alert.ToJson();
    first = false;
  }
  
  oss << "]";
  return oss.str();
}

void MetricsHttpHandler::ServerLoop() {
  // TODO: 实际实现 HTTP 服务器
  // 这里只是占位
  while (running_) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
}

// ============================================================================
// 便捷函数实现
// ============================================================================

void RecordTaskEvent(const std::string& task_id,
                     const std::string& task_type,
                     const std::string& event,
                     const std::map<std::string, std::string>& extra_fields) {
  LogEntry entry(LogLevel::kInfo, "Task event: " + event);
  entry.WithTaskId(task_id);
  entry.WithField("task_type", task_type);
  entry.WithField("event", event);
  
  for (const auto& [key, value] : extra_fields) {
    entry.WithField(key, value);
  }
  
  Logger::Instance().Log(std::move(entry));
}

void RecordWorkerEvent(const std::string& worker_id,
                       const std::string& event,
                       const std::map<std::string, std::string>& extra_fields) {
  LogEntry entry(LogLevel::kInfo, "Worker event: " + event);
  entry.WithWorkerId(worker_id);
  entry.WithField("event", event);
  
  for (const auto& [key, value] : extra_fields) {
    entry.WithField(key, value);
  }
  
  Logger::Instance().Log(std::move(entry));
}

void RecordCompactionMetrics(const std::string& task_id,
                             const std::string& node_id,
                             uint64_t input_bytes,
                             uint64_t output_bytes,
                             double duration_seconds,
                             bool success) {
  auto& task_metrics = TaskMetrics::Instance();
  
  // 记录输入输出字节数
  task_metrics.RecordCompactionInput("0", node_id, input_bytes, 0);
  task_metrics.RecordCompactionOutput("0", node_id, output_bytes, 0);
  
  // 记录执行时间
  task_metrics.RecordExecutionDuration("compaction", "", duration_seconds);
  
  // 记录完成状态
  if (success) {
    task_metrics.RecordTaskCompleted("compaction", "success");
  } else {
    task_metrics.RecordTaskFailed("compaction", "execution_error");
  }
  
  // 记录日志
  LogEntry entry(success ? LogLevel::kInfo : LogLevel::kError,
                 success ? "Compaction completed" : "Compaction failed");
  entry.WithTaskId(task_id)
       .WithNodeId(node_id)
       .WithField("input_bytes", static_cast<int64_t>(input_bytes))
       .WithField("output_bytes", static_cast<int64_t>(output_bytes))
       .WithDuration(duration_seconds);
  
  Logger::Instance().Log(std::move(entry));
}

}  // namespace observability
}  // namespace tendisplus
