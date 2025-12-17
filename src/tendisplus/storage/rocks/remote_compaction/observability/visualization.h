// Copyright (c) 2024-present, Tencent Inc. All rights reserved.
// 可视化与仪表盘支持

#pragma once

#include <chrono>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "metrics.h"
#include "task_metrics.h"

namespace tendisplus {
namespace observability {

// ============================================================================
// HTTP 服务器配置
// ============================================================================
struct HttpServerConfig {
  std::string bind_address = "0.0.0.0";
  uint16_t port = 9090;
  uint32_t max_connections = 100;
  uint32_t read_timeout_ms = 5000;
  uint32_t write_timeout_ms = 5000;
  bool enable_cors = true;
  std::string cors_origin = "*";
};

// ============================================================================
// HTTP 请求/响应
// ============================================================================
struct HttpRequest {
  std::string method;
  std::string path;
  std::map<std::string, std::string> headers;
  std::map<std::string, std::string> query_params;
  std::string body;
};

struct HttpResponse {
  int status_code = 200;
  std::map<std::string, std::string> headers;
  std::string body;
  std::string content_type = "text/plain";
  
  static HttpResponse Ok(const std::string& body, 
                         const std::string& content_type = "text/plain");
  static HttpResponse Json(const std::string& json);
  static HttpResponse NotFound();
  static HttpResponse InternalError(const std::string& message);
};

// ============================================================================
// HTTP 路由处理器
// ============================================================================
using HttpHandler = std::function<HttpResponse(const HttpRequest&)>;

class HttpRouter {
 public:
  HttpRouter() = default;
  ~HttpRouter() = default;
  
  // 注册路由
  void Get(const std::string& path, HttpHandler handler);
  void Post(const std::string& path, HttpHandler handler);
  void Put(const std::string& path, HttpHandler handler);
  void Delete(const std::string& path, HttpHandler handler);
  
  // 路由匹配
  HttpResponse Route(const HttpRequest& request) const;

 private:
  std::map<std::string, std::map<std::string, HttpHandler>> routes_;
};

// ============================================================================
// HTTP 服务器
// ============================================================================
class HttpServer {
 public:
  explicit HttpServer(const HttpServerConfig& config);
  ~HttpServer();
  
  // 禁止拷贝
  HttpServer(const HttpServer&) = delete;
  HttpServer& operator=(const HttpServer&) = delete;
  
  // 生命周期
  void Start();
  void Stop();
  bool IsRunning() const { return running_.load(); }
  
  // 获取路由器
  HttpRouter& Router() { return router_; }

 private:
  void AcceptLoop();
  void HandleConnection(int client_fd);
  
  HttpServerConfig config_;
  HttpRouter router_;
  std::atomic<bool> running_{false};
  int server_fd_ = -1;
  std::unique_ptr<std::thread> accept_thread_;
};

// ============================================================================
// Prometheus 指标端点
// ============================================================================
class PrometheusEndpoint {
 public:
  PrometheusEndpoint() = default;
  ~PrometheusEndpoint() = default;
  
  // 注册到 HTTP 服务器
  void RegisterRoutes(HttpRouter& router);
  
  // 导出指标
  std::string ExportMetrics() const;

 private:
  HttpResponse HandleMetrics(const HttpRequest& request);
};

// ============================================================================
// 健康检查端点
// ============================================================================
class HealthEndpoint {
 public:
  HealthEndpoint() = default;
  ~HealthEndpoint() = default;
  
  void RegisterRoutes(HttpRouter& router);

 private:
  HttpResponse HandleHealth(const HttpRequest& request);
  HttpResponse HandleLiveness(const HttpRequest& request);
  HttpResponse HandleReadiness(const HttpRequest& request);
};

// ============================================================================
// 仪表盘 API 端点
// ============================================================================
class DashboardEndpoint {
 public:
  DashboardEndpoint() = default;
  ~DashboardEndpoint() = default;
  
  void RegisterRoutes(HttpRouter& router);

 private:
  // 概览数据
  HttpResponse HandleOverview(const HttpRequest& request);
  
  // 任务相关
  HttpResponse HandleTasks(const HttpRequest& request);
  HttpResponse HandleTaskDetail(const HttpRequest& request);
  HttpResponse HandleTaskStats(const HttpRequest& request);
  
  // Worker 相关
  HttpResponse HandleWorkers(const HttpRequest& request);
  HttpResponse HandleWorkerDetail(const HttpRequest& request);
  
  // 资源相关
  HttpResponse HandleResources(const HttpRequest& request);
  
  // 告警相关
  HttpResponse HandleAlerts(const HttpRequest& request);
  
  // 历史数据 (用于图表)
  HttpResponse HandleTimeSeries(const HttpRequest& request);
};

// ============================================================================
// WebSocket 支持 (实时数据推送)
// ============================================================================
class WebSocketSession {
 public:
  WebSocketSession(int fd, const std::string& session_id);
  ~WebSocketSession();
  
  void Send(const std::string& message);
  void Close();
  
  const std::string& SessionId() const { return session_id_; }
  bool IsAlive() const { return alive_.load(); }

 private:
  int fd_;
  std::string session_id_;
  std::atomic<bool> alive_{true};
};

class WebSocketServer {
 public:
  explicit WebSocketServer(uint16_t port);
  ~WebSocketServer();
  
  void Start();
  void Stop();
  
  // 广播消息给所有客户端
  void Broadcast(const std::string& message);
  
  // 发送给特定客户端
  void Send(const std::string& session_id, const std::string& message);
  
  // 获取连接数
  size_t GetConnectionCount() const;

 private:
  void AcceptLoop();
  void HandleUpgrade(int client_fd);
  
  uint16_t port_;
  std::atomic<bool> running_{false};
  std::map<std::string, std::shared_ptr<WebSocketSession>> sessions_;
  mutable std::mutex sessions_mutex_;
  std::unique_ptr<std::thread> accept_thread_;
};

// ============================================================================
// 实时数据推送器
// ============================================================================
class RealtimePusher {
 public:
  explicit RealtimePusher(std::shared_ptr<WebSocketServer> ws_server);
  ~RealtimePusher();
  
  void Start();
  void Stop();
  
  // 设置推送间隔
  void SetInterval(std::chrono::milliseconds interval);

 private:
  void PushLoop();
  
  // 构建推送数据
  std::string BuildOverviewData() const;
  std::string BuildTaskUpdateData() const;
  std::string BuildAlertData() const;
  
  std::shared_ptr<WebSocketServer> ws_server_;
  std::chrono::milliseconds interval_{1000};
  std::atomic<bool> running_{false};
  std::unique_ptr<std::thread> push_thread_;
};

// ============================================================================
// Grafana 仪表盘配置生成器
// ============================================================================
class GrafanaDashboardGenerator {
 public:
  GrafanaDashboardGenerator() = default;
  ~GrafanaDashboardGenerator() = default;
  
  // 生成完整仪表盘 JSON
  std::string GenerateMainDashboard() const;
  std::string GenerateTaskDashboard() const;
  std::string GenerateWorkerDashboard() const;
  std::string GenerateResourceDashboard() const;
  std::string GenerateAlertDashboard() const;

 private:
  // 生成面板
  std::string GeneratePanel(const std::string& title,
                            const std::string& type,
                            const std::vector<std::string>& queries,
                            int grid_x, int grid_y,
                            int width, int height) const;
  
  // 生成行
  std::string GenerateRow(const std::string& title,
                          const std::vector<std::string>& panels) const;
};

// ============================================================================
// 时序数据存储接口
// ============================================================================
class TimeSeriesStorage {
 public:
  virtual ~TimeSeriesStorage() = default;
  
  // 写入数据点
  virtual void Write(const std::string& metric_name,
                     const Labels& labels,
                     double value,
                     std::chrono::system_clock::time_point timestamp) = 0;
  
  // 查询数据
  struct QueryResult {
    std::string metric_name;
    Labels labels;
    std::vector<std::pair<std::chrono::system_clock::time_point, double>> values;
  };
  
  virtual std::vector<QueryResult> Query(
      const std::string& metric_name,
      const Labels& label_matchers,
      std::chrono::system_clock::time_point start,
      std::chrono::system_clock::time_point end,
      std::chrono::seconds step) = 0;
};

// ============================================================================
// 内存时序存储 (用于开发/测试)
// ============================================================================
class InMemoryTimeSeriesStorage : public TimeSeriesStorage {
 public:
  InMemoryTimeSeriesStorage(size_t max_samples_per_series = 10000);
  ~InMemoryTimeSeriesStorage() override = default;
  
  void Write(const std::string& metric_name,
             const Labels& labels,
             double value,
             std::chrono::system_clock::time_point timestamp) override;
  
  std::vector<QueryResult> Query(
      const std::string& metric_name,
      const Labels& label_matchers,
      std::chrono::system_clock::time_point start,
      std::chrono::system_clock::time_point end,
      std::chrono::seconds step) override;
  
  // 清理过期数据
  void Cleanup(std::chrono::hours retention);

 private:
  struct Series {
    std::string metric_name;
    Labels labels;
    std::vector<std::pair<std::chrono::system_clock::time_point, double>> samples;
  };
  
  std::string MakeSeriesKey(const std::string& metric_name, 
                            const Labels& labels) const;
  
  std::map<std::string, Series> series_;
  size_t max_samples_per_series_;
  mutable std::mutex mutex_;
};

// ============================================================================
// VictoriaMetrics 远程写入
// ============================================================================
class VictoriaMetricsWriter : public TimeSeriesStorage {
 public:
  explicit VictoriaMetricsWriter(const std::string& endpoint);
  ~VictoriaMetricsWriter() override = default;
  
  void Write(const std::string& metric_name,
             const Labels& labels,
             double value,
             std::chrono::system_clock::time_point timestamp) override;
  
  std::vector<QueryResult> Query(
      const std::string& metric_name,
      const Labels& label_matchers,
      std::chrono::system_clock::time_point start,
      std::chrono::system_clock::time_point end,
      std::chrono::seconds step) override;
  
  // 批量写入
  void Flush();

 private:
  std::string endpoint_;
  std::vector<std::string> buffer_;
  mutable std::mutex mutex_;
};

}  // namespace observability
}  // namespace tendisplus
