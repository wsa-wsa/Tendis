// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include "observatory.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <sstream>

namespace tendisplus {
namespace control_plane {

Observatory::Observatory(const ObservatoryConfig& config)
    : config_(config) {}

Observatory::~Observatory() {
  Stop();
}

void Observatory::Start() {
  if (running_.load()) {
    return;
  }

  running_.store(true);
  should_stop_.store(false);

  // 启动 gRPC 连接线程
  connect_thread_ = std::make_unique<std::thread>(&Observatory::ConnectLoop, this);

  // 启动 HTTP 服务器线程
  http_thread_ = std::make_unique<std::thread>(&Observatory::HttpServerLoop, this);

  // 启动指标采集线程
  metrics_thread_ = std::make_unique<std::thread>(&Observatory::MetricsCollectLoop, this);

  std::cout << "[Observatory] Started" << std::endl;
  std::cout << "[Observatory] HTTP server: http://" << config_.bind_address 
            << ":" << config_.http_port << std::endl;
  std::cout << "[Observatory] Control Plane: " << config_.control_plane_address << std::endl;
}

void Observatory::Stop() {
  if (!running_.load()) {
    return;
  }

  running_.store(false);
  should_stop_.store(true);

  // 关闭 socket
  if (server_fd_ >= 0) {
    shutdown(server_fd_, SHUT_RDWR);
    close(server_fd_);
    server_fd_ = -1;
  }

  if (connect_thread_ && connect_thread_->joinable()) {
    connect_thread_->join();
  }
  if (http_thread_ && http_thread_->joinable()) {
    http_thread_->join();
  }
  if (metrics_thread_ && metrics_thread_->joinable()) {
    metrics_thread_->join();
  }

  std::cout << "[Observatory] Stopped" << std::endl;
}

void Observatory::Wait() {
  while (running_.load() && !should_stop_.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
}

std::string Observatory::GetHttpAddress() const {
  return "http://" + config_.bind_address + ":" + std::to_string(config_.http_port);
}

// ============================================================================
// gRPC 客户端管理
// ============================================================================

void Observatory::ConnectLoop() {
  while (running_.load() && !should_stop_.load()) {
    if (!connected_.load()) {
      if (TryConnect()) {
        std::cout << "[Observatory] Connected to Control Plane: " 
                  << config_.control_plane_address << std::endl;
        connected_.store(true);
      } else {
        std::cout << "[Observatory] Failed to connect to Control Plane, retry in "
                  << config_.reconnect_interval_sec << "s" << std::endl;
      }
    } else {
      // 检查连接状态
      grpc_connectivity_state state = channel_->GetState(false);
      if (state == GRPC_CHANNEL_SHUTDOWN || 
          state == GRPC_CHANNEL_TRANSIENT_FAILURE) {
        std::cout << "[Observatory] Connection lost, reconnecting..." << std::endl;
        connected_.store(false);
      }
    }

    // 等待下一次检查
    for (uint32_t i = 0; i < config_.reconnect_interval_sec * 10 && running_.load(); ++i) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
  }
}

bool Observatory::TryConnect() {
  try {
    std::lock_guard<std::mutex> lock(grpc_mutex_);
    
    channel_ = grpc::CreateChannel(
        config_.control_plane_address,
        grpc::InsecureChannelCredentials());
    
    // 等待连接建立
    auto deadline = std::chrono::system_clock::now() + 
                   std::chrono::milliseconds(config_.grpc_timeout_ms);
    if (!channel_->WaitForConnected(deadline)) {
      return false;
    }
    
    stub_ = ::control_plane::ControlPlaneService::NewStub(channel_);
    
    // 测试连接 - 获取集群状态
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + 
                        std::chrono::milliseconds(config_.grpc_timeout_ms));
    
    ::control_plane::ClusterStatusRequest request;
    ::control_plane::ClusterStatusResponse response;
    
    grpc::Status status = stub_->GetClusterStatus(&context, request, &response);
    return status.ok();
  } catch (const std::exception& e) {
    std::cerr << "[Observatory] Connect error: " << e.what() << std::endl;
    return false;
  }
}

// ============================================================================
// HTTP 服务器
// ============================================================================

void Observatory::HttpServerLoop() {
  // 创建 socket
  server_fd_ = socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd_ < 0) {
    std::cerr << "[Observatory] Failed to create socket" << std::endl;
    return;
  }

  // 设置 socket 选项
  int opt = 1;
  setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

  // 绑定地址
  struct sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = INADDR_ANY;
  addr.sin_port = htons(config_.http_port);

  if (bind(server_fd_, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
    std::cerr << "[Observatory] Failed to bind port " << config_.http_port << std::endl;
    close(server_fd_);
    server_fd_ = -1;
    return;
  }

  // 监听
  if (listen(server_fd_, 10) < 0) {
    std::cerr << "[Observatory] Failed to listen" << std::endl;
    close(server_fd_);
    server_fd_ = -1;
    return;
  }

  while (running_.load()) {
    struct sockaddr_in client_addr;
    socklen_t client_len = sizeof(client_addr);

    int client_fd = accept(server_fd_, (struct sockaddr*)&client_addr, &client_len);
    if (client_fd < 0) {
      if (running_.load()) {
        std::cerr << "[Observatory] Accept failed" << std::endl;
      }
      continue;
    }

    // 设置读取超时
    struct timeval tv;
    tv.tv_sec = 5;
    tv.tv_usec = 0;
    setsockopt(client_fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    // 读取请求
    char buffer[8192];
    ssize_t bytes_read = read(client_fd, buffer, sizeof(buffer) - 1);
    if (bytes_read > 0) {
      buffer[bytes_read] = '\0';
      std::string request(buffer);

      // 解析请求行
      std::string method, path;
      std::istringstream iss(request);
      iss >> method >> path;

      // 解析请求体 (对于 POST)
      std::string body;
      size_t body_start = request.find("\r\n\r\n");
      if (body_start != std::string::npos) {
        body = request.substr(body_start + 4);
      }

      // 处理请求
      std::string response = HandleRequest(method, path, body);

      // 发送响应
      write(client_fd, response.c_str(), response.size());
    }

    close(client_fd);
  }
}

std::string Observatory::HandleRequest(const std::string& method,
                                        const std::string& path,
                                        const std::string& body) {
  // 连接状态 API
  if (path == "/api/status" || path == "/api/status/") {
    return BuildJsonResponse(HandleApiConnectionStatus());
  }

  // API 路由
  if (path == "/api/metrics" || path == "/api/metrics/") {
    return BuildJsonResponse(HandleApiMetrics());
  }
  if (path == "/api/metrics/history" || path == "/api/metrics/history/") {
    return BuildJsonResponse(HandleApiMetricsHistory());
  }
  if (path == "/api/workers" || path == "/api/workers/") {
    return BuildJsonResponse(HandleApiWorkers());
  }
  if (path.find("/api/workers/") == 0 && path.size() > 13) {
    std::string worker_id = path.substr(13);
    // 移除尾部斜杠
    if (!worker_id.empty() && worker_id.back() == '/') {
      worker_id.pop_back();
    }
    return BuildJsonResponse(HandleApiWorkerDetail(worker_id));
  }
  if (path == "/api/tasks" || path == "/api/tasks/") {
    return BuildJsonResponse(HandleApiTasks(""));
  }
  if (path.find("/api/tasks?") == 0) {
    // 解析查询参数
    size_t pos = path.find("status=");
    std::string status_filter;
    if (pos != std::string::npos) {
      status_filter = path.substr(pos + 7);
      size_t end = status_filter.find('&');
      if (end != std::string::npos) {
        status_filter = status_filter.substr(0, end);
      }
    }
    return BuildJsonResponse(HandleApiTasks(status_filter));
  }
  if (path.find("/api/task/") == 0 && path.size() > 10) {
    std::string task_id = path.substr(10);
    if (!task_id.empty() && task_id.back() == '/') {
      task_id.pop_back();
    }
    return BuildJsonResponse(HandleApiTaskDetail(task_id));
  }
  if (path == "/api/cluster" || path == "/api/cluster/") {
    return BuildJsonResponse(HandleApiClusterStatus());
  }

  // 增强观测 API
  if (path == "/api/alerts" || path == "/api/alerts/") {
    return BuildJsonResponse(HandleApiAlerts());
  }
  if (path == "/api/alert-rules" || path == "/api/alert-rules/") {
    return BuildJsonResponse(HandleApiAlertRules());
  }
  if (path == "/api/traces" || path == "/api/traces/") {
    return BuildJsonResponse(HandleApiTraces("recent"));
  }
  if (path.find("/api/traces?") == 0) {
    std::string query_type = "recent";
    if (path.find("type=slow") != std::string::npos) {
      query_type = "slow";
    } else if (path.find("type=failed") != std::string::npos) {
      query_type = "failed";
    }
    return BuildJsonResponse(HandleApiTraces(query_type));
  }
  if (path.find("/api/trace/") == 0 && path.size() > 11) {
    std::string task_id = path.substr(11);
    if (!task_id.empty() && task_id.back() == '/') {
      task_id.pop_back();
    }
    return BuildJsonResponse(HandleApiTraceDetail(task_id));
  }
  if (path == "/api/prometheus/metrics" || path == "/api/prometheus/metrics/") {
    return BuildHttpResponse(200, "text/plain; charset=utf-8",
                             HandleApiPrometheusMetrics());
  }
  if (path.find("/api/bulkload/") == 0 && path.size() > 14) {
    std::string task_id = path.substr(14);
    if (!task_id.empty() && task_id.back() == '/') {
      task_id.pop_back();
    }
    return BuildJsonResponse(HandleApiBulkLoadStatus(task_id));
  }

  // 静态文件
  if (path == "/" || path == "/index.html") {
    return BuildHttpResponse(200, "text/html; charset=utf-8", GetEmbeddedHtml());
  }

  // 404
  return BuildErrorResponse(404, "Not Found");
}

// ============================================================================
// API 处理器 (通过 gRPC 获取数据)
// ============================================================================

std::string Observatory::HandleApiConnectionStatus() {
  std::ostringstream json;
  json << "{";
  json << R"("connected":)" << (connected_.load() ? "true" : "false") << ",";
  json << R"("control_plane_address":")" << JsonEscape(config_.control_plane_address) << "\",";
  json << R"("timestamp_ms":)" << GetCurrentTimeMs();
  json << "}";
  return json.str();
}

std::string Observatory::HandleApiMetrics() {
  if (!connected_.load() || !stub_) {
    return R"({"error": "Not connected to Control Plane", "connected": false})";
  }

  try {
    std::lock_guard<std::mutex> lock(grpc_mutex_);
    
    // 获取集群状态
    grpc::ClientContext ctx1;
    ctx1.set_deadline(std::chrono::system_clock::now() + 
                     std::chrono::milliseconds(config_.grpc_timeout_ms));
    ::control_plane::ClusterStatusRequest cluster_req;
    ::control_plane::ClusterStatusResponse cluster_resp;
    auto status1 = stub_->GetClusterStatus(&ctx1, cluster_req, &cluster_resp);
    
    // 获取任务统计
    grpc::ClientContext ctx2;
    ctx2.set_deadline(std::chrono::system_clock::now() + 
                     std::chrono::milliseconds(config_.grpc_timeout_ms));
    ::control_plane::TaskStatisticsRequest stats_req;
    ::control_plane::TaskStatisticsResponse stats_resp;
    auto status2 = stub_->GetTaskStatistics(&ctx2, stats_req, &stats_resp);
    
    // 获取指标快照
    grpc::ClientContext ctx3;
    ctx3.set_deadline(std::chrono::system_clock::now() + 
                     std::chrono::milliseconds(config_.grpc_timeout_ms));
    ::control_plane::MetricsSnapshotRequest metrics_req;
    ::control_plane::MetricsSnapshotResponse metrics_resp;
    auto status3 = stub_->GetMetricsSnapshot(&ctx3, metrics_req, &metrics_resp);
    
    if (!status1.ok() && !status2.ok()) {
      return R"({"error": "Failed to fetch metrics from Control Plane"})";
    }

    std::ostringstream json;
    json << "{";
    json << R"("connected":true,)";
    json << R"("timestamp_ms":)" << GetCurrentTimeMs() << ",";
    json << R"("cluster":{)";
    json << R"("total_workers":)" << cluster_resp.total_workers() << ",";
    json << R"("online_workers":)" << cluster_resp.online_workers() << ",";
    json << R"("pending_tasks":)" << cluster_resp.pending_tasks() << ",";
    json << R"("running_tasks":)" << cluster_resp.running_tasks();
    json << "},";
    json << R"("throughput":{)";
    json << R"("total_completed":)" << stats_resp.total_completed() << ",";
    json << R"("total_failed":)" << stats_resp.total_failed() << ",";
    json << R"("total_cancelled":)" << stats_resp.total_cancelled() << ",";
    json << R"("total_timeout":)" << stats_resp.total_timeout() << ",";
    json << R"("completed_per_min":)" << metrics_resp.completed_last_minute() << ",";
    json << R"("failed_per_min":)" << metrics_resp.failed_last_minute();
    json << "},";
    json << R"("latency":{)";
    json << R"("avg_queue_ms":)" << stats_resp.avg_queue_time_ms() << ",";
    json << R"("avg_execution_ms":)" << stats_resp.avg_execution_time_ms() << ",";
    json << R"("p50_ms":)" << metrics_resp.p50_execution_time_ms() << ",";
    json << R"("p95_ms":)" << metrics_resp.p95_execution_time_ms() << ",";
    json << R"("p99_ms":)" << metrics_resp.p99_execution_time_ms();
    json << "},";
    json << R"("resources":{)";
    json << R"("memory_usage":)" << std::fixed << std::setprecision(3) 
         << metrics_resp.cluster_memory_usage() << ",";
    json << R"("disk_usage":)" << std::fixed << std::setprecision(3) 
         << metrics_resp.cluster_disk_usage();
    json << "}";
    json << "}";

    return json.str();
  } catch (const std::exception& e) {
    return R"({"error": ")" + JsonEscape(e.what()) + R"("})";
  }
}

std::string Observatory::HandleApiMetricsHistory() {
  std::lock_guard<std::mutex> lock(metrics_mutex_);

  std::ostringstream json;
  json << "{\"history\":[";

  bool first = true;
  for (const auto& dp : metrics_history_) {
    if (!first) json << ",";
    first = false;

    json << "{";
    json << R"("ts":)" << dp.timestamp_ms << ",";
    json << R"("workers":)" << dp.online_workers << ",";
    json << R"("pending":)" << dp.pending_tasks << ",";
    json << R"("running":)" << dp.running_tasks << ",";
    json << R"("completed":)" << dp.completed_count << ",";
    json << R"("failed":)" << dp.failed_count << ",";
    json << R"("queue_ms":)" << dp.avg_queue_time_ms << ",";
    json << R"("exec_ms":)" << dp.avg_execution_time_ms << ",";
    json << R"("mem":)" << std::fixed << std::setprecision(3) << dp.memory_usage;
    json << "}";
  }

  json << "]}";
  return json.str();
}

std::string Observatory::HandleApiWorkers() {
  if (!connected_.load() || !stub_) {
    return R"({"error": "Not connected to Control Plane", "workers": []})";
  }

  try {
    std::lock_guard<std::mutex> lock(grpc_mutex_);
    
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + 
                        std::chrono::milliseconds(config_.grpc_timeout_ms));
    
    ::control_plane::ListWorkersRequest request;
    ::control_plane::ListWorkersResponse response;
    
    auto status = stub_->ListWorkers(&context, request, &response);
    if (!status.ok()) {
      return R"({"error": "Failed to fetch workers", "workers": []})";
    }

    std::ostringstream json;
    json << "{\"workers\":[";

    bool first = true;
    for (const auto& w : response.workers()) {
      if (!first) json << ",";
      first = false;

      json << "{";
      json << R"("id":")" << JsonEscape(w.worker_id()) << "\",";
      json << R"("address":")" << JsonEscape(w.address()) << "\",";
      json << R"("status":")" << WorkerStatusToJsonString(w.status()) << "\",";
      json << R"("active_tasks":)" << w.resources().active_tasks() << ",";
      json << R"("max_tasks":)" << w.resources().max_concurrent_tasks() << ",";
      json << R"("total_completed":)" << w.total_completed() << ",";
      json << R"("total_failed":)" << w.total_failed() << ",";
      json << R"("cpu_cores":)" << w.resources().total_cpu_cores() << ",";
      json << R"("memory_mb":)" << w.resources().total_memory_mb() << ",";
      json << R"("memory_used_mb":)" << w.resources().used_memory_mb() << ",";
      json << R"("disk_mb":)" << w.resources().total_disk_mb() << ",";
      json << R"("disk_used_mb":)" << w.resources().used_disk_mb();
      json << "}";
    }

    json << "]}";
    return json.str();
  } catch (const std::exception& e) {
    return R"({"error": ")" + JsonEscape(e.what()) + R"(", "workers": []})";
  }
}

std::string Observatory::HandleApiWorkerDetail(const std::string& worker_id) {
  if (!connected_.load() || !stub_) {
    return R"({"error": "Not connected to Control Plane", "found": false})";
  }

  try {
    std::lock_guard<std::mutex> lock(grpc_mutex_);
    
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + 
                        std::chrono::milliseconds(config_.grpc_timeout_ms));
    
    ::control_plane::WorkerDetailRequest request;
    request.set_worker_id(worker_id);
    ::control_plane::WorkerDetailResponse response;
    
    auto status = stub_->GetWorkerDetail(&context, request, &response);
    if (!status.ok()) {
      return R"({"error": "Failed to fetch worker detail", "found": false})";
    }
    
    if (!response.found()) {
      return R"({"found": false})";
    }

    const auto& worker = response.worker();
    std::ostringstream json;
    json << "{\"found\":true,\"worker\":{";
    json << R"("id":")" << JsonEscape(worker.worker_id()) << "\",";
    json << R"("address":")" << JsonEscape(worker.address()) << "\",";
    json << R"("status":")" << WorkerStatusToJsonString(worker.status()) << "\",";
    json << R"("active_tasks":)" << worker.resources().active_tasks() << ",";
    json << R"("max_tasks":)" << worker.resources().max_concurrent_tasks() << ",";
    json << R"("total_completed":)" << worker.total_completed() << ",";
    json << R"("total_failed":)" << worker.total_failed() << ",";
    json << R"("cpu_cores":)" << worker.resources().total_cpu_cores() << ",";
    json << R"("memory_mb":)" << worker.resources().total_memory_mb() << ",";
    json << R"("memory_used_mb":)" << worker.resources().used_memory_mb() << ",";
    json << R"("disk_mb":)" << worker.resources().total_disk_mb() << ",";
    json << R"("disk_used_mb":)" << worker.resources().used_disk_mb() << ",";
    json << R"("avg_execution_time_ms":)" << response.avg_execution_time_ms() << ",";
    json << R"("tasks_completed_last_hour":)" << response.tasks_completed_last_hour() << ",";
    json << R"("tasks_failed_last_hour":)" << response.tasks_failed_last_hour() << ",";
    json << R"("active_task_ids":[)";
    
    bool first = true;
    for (const auto& task : response.active_tasks()) {
      if (!first) json << ",";
      first = false;
      json << "\"" << JsonEscape(task.task_id()) << "\"";
    }
    json << "]}}";

    return json.str();
  } catch (const std::exception& e) {
    return R"({"error": ")" + JsonEscape(e.what()) + R"(", "found": false})";
  }
}

std::string Observatory::HandleApiTasks(const std::string& status_filter) {
  if (!connected_.load() || !stub_) {
    return R"({"error": "Not connected to Control Plane", "tasks": []})";
  }

  try {
    std::lock_guard<std::mutex> lock(grpc_mutex_);
    
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + 
                        std::chrono::milliseconds(config_.grpc_timeout_ms));
    
    ::control_plane::ListTasksRequest request;
    request.set_limit(100);
    
    // 解析状态过滤器
    if (!status_filter.empty()) {
      if (status_filter == "pending") {
        request.set_status_filter(::control_plane::TASK_PENDING);
      } else if (status_filter == "running") {
        request.set_status_filter(::control_plane::TASK_RUNNING);
      } else if (status_filter == "completed") {
        request.set_status_filter(::control_plane::TASK_COMPLETED);
      } else if (status_filter == "failed") {
        request.set_status_filter(::control_plane::TASK_FAILED);
      }
    }
    
    ::control_plane::ListTasksResponse response;
    auto status = stub_->ListTasks(&context, request, &response);
    if (!status.ok()) {
      return R"({"error": "Failed to fetch tasks", "tasks": []})";
    }

    std::ostringstream json;
    json << "{\"tasks\":[";

    bool first = true;
    for (const auto& t : response.tasks()) {
      if (!first) json << ",";
      first = false;

      int64_t elapsed_ms = 0;
      if (t.status() == ::control_plane::TASK_RUNNING && t.start_time_ms() > 0) {
        elapsed_ms = GetCurrentTimeMs() - t.start_time_ms();
      }

      json << "{";
      json << R"("id":")" << JsonEscape(t.task_id()) << "\",";
      json << R"("status":")" << TaskStatusToJsonString(t.status()) << "\",";
      json << R"("priority":")" << TaskPriorityToJsonString(t.priority()) << "\",";
      json << R"("source":")" << JsonEscape(t.source_node_id()) << "\",";
      json << R"("db_name":")" << JsonEscape(t.db_name()) << "\",";
      json << R"("store_id":)" << t.store_id() << ",";
      json << R"("worker":")" << JsonEscape(t.assigned_worker_id()) << "\",";
      json << R"("submit_time_ms":)" << t.submit_time_ms() << ",";
      json << R"("start_time_ms":)" << t.start_time_ms() << ",";
      json << R"("elapsed_ms":)" << elapsed_ms << ",";
      json << R"("retry_count":)" << t.retry_count();
      json << "}";
    }

    json << "]}";
    return json.str();
  } catch (const std::exception& e) {
    return R"({"error": ")" + JsonEscape(e.what()) + R"(", "tasks": []})";
  }
}

std::string Observatory::HandleApiTaskDetail(const std::string& task_id) {
  if (!connected_.load() || !stub_) {
    return R"({"error": "Not connected to Control Plane", "found": false})";
  }

  try {
    std::lock_guard<std::mutex> lock(grpc_mutex_);
    
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + 
                        std::chrono::milliseconds(config_.grpc_timeout_ms));
    
    ::control_plane::QueryTaskRequest request;
    request.set_task_id(task_id);
    ::control_plane::QueryTaskResponse response;
    
    auto status = stub_->QueryTaskStatus(&context, request, &response);
    if (!status.ok()) {
      return R"({"error": "Failed to fetch task detail", "found": false})";
    }
    
    if (!response.found()) {
      return R"({"found": false})";
    }

    const auto& task = response.task_info();
    std::ostringstream json;
    json << "{\"found\":true,\"task\":{";
    json << R"("id":")" << JsonEscape(task.task_id()) << "\",";
    json << R"("status":")" << TaskStatusToJsonString(task.status()) << "\",";
    json << R"("priority":")" << TaskPriorityToJsonString(task.priority()) << "\",";
    json << R"("source":")" << JsonEscape(task.source_node_id()) << "\",";
    json << R"("db_name":")" << JsonEscape(task.db_name()) << "\",";
    json << R"("store_id":)" << task.store_id() << ",";
    json << R"("worker":")" << JsonEscape(task.assigned_worker_id()) << "\",";
    json << R"("submit_time_ms":)" << task.submit_time_ms() << ",";
    json << R"("start_time_ms":)" << task.start_time_ms() << ",";
    json << R"("complete_time_ms":)" << task.complete_time_ms() << ",";
    json << R"("retry_count":)" << task.retry_count() << ",";
    json << R"("error_message":")" << JsonEscape(task.error_message()) << "\"";
    json << "}}";

    return json.str();
  } catch (const std::exception& e) {
    return R"({"error": ")" + JsonEscape(e.what()) + R"(", "found": false})";
  }
}

std::string Observatory::HandleApiClusterStatus() {
  if (!connected_.load() || !stub_) {
    return R"({"error": "Not connected to Control Plane", "connected": false})";
  }

  try {
    std::lock_guard<std::mutex> lock(grpc_mutex_);
    
    // 获取集群状态
    grpc::ClientContext ctx1;
    ctx1.set_deadline(std::chrono::system_clock::now() + 
                     std::chrono::milliseconds(config_.grpc_timeout_ms));
    ::control_plane::ClusterStatusRequest cluster_req;
    ::control_plane::ClusterStatusResponse cluster_resp;
    auto status1 = stub_->GetClusterStatus(&ctx1, cluster_req, &cluster_resp);
    
    // 获取任务统计
    grpc::ClientContext ctx2;
    ctx2.set_deadline(std::chrono::system_clock::now() + 
                     std::chrono::milliseconds(config_.grpc_timeout_ms));
    ::control_plane::TaskStatisticsRequest stats_req;
    ::control_plane::TaskStatisticsResponse stats_resp;
    auto status2 = stub_->GetTaskStatistics(&ctx2, stats_req, &stats_resp);
    
    if (!status1.ok()) {
      return R"({"error": "Failed to fetch cluster status"})";
    }

    std::ostringstream json;
    json << "{";
    json << R"("connected":true,)";
    json << R"("total_workers":)" << cluster_resp.total_workers() << ",";
    json << R"("online_workers":)" << cluster_resp.online_workers() << ",";
    json << R"("pending_tasks":)" << cluster_resp.pending_tasks() << ",";
    json << R"("running_tasks":)" << cluster_resp.running_tasks() << ",";
    json << R"("total_completed":)" << cluster_resp.total_completed() << ",";
    json << R"("total_failed":)" << cluster_resp.total_failed() << ",";
    json << R"("total_submitted":)" << stats_resp.total_submitted() << ",";
    json << R"("total_cancelled":)" << stats_resp.total_cancelled() << ",";
    json << R"("total_timeout":)" << stats_resp.total_timeout() << ",";
    json << R"("avg_queue_time_ms":)" << stats_resp.avg_queue_time_ms() << ",";
    json << R"("avg_execution_time_ms":)" << stats_resp.avg_execution_time_ms();
    json << "}";

    return json.str();
  } catch (const std::exception& e) {
    return R"({"error": ")" + JsonEscape(e.what()) + R"("})";
  }
}

// ============================================================================
// 增强观测 API 处理器
// ============================================================================

std::string Observatory::HandleApiAlerts() {
  if (!connected_.load() || !stub_) {
    return R"({"error": "Not connected to Control Plane"})";
  }

  try {
    std::lock_guard<std::mutex> lock(grpc_mutex_);

    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() +
                        std::chrono::milliseconds(config_.grpc_timeout_ms));

    ::control_plane::GetAlertsRequest request;
    request.set_active_only(false);
    request.set_history_limit(50);
    ::control_plane::GetAlertsResponse response;

    auto status = stub_->GetAlerts(&context, request, &response);
    if (!status.ok()) {
      return R"({"error": "Failed to fetch alerts"})";
    }

    std::ostringstream json;
    json << "{";
    json << R"("total_active":)" << response.total_active() << ",";
    json << R"("info_count":)" << response.info_count() << ",";
    json << R"("warning_count":)" << response.warning_count() << ",";
    json << R"("critical_count":)" << response.critical_count() << ",";
    json << R"("active_alerts":[)";

    bool first = true;
    for (const auto& a : response.active_alerts()) {
      if (!first) json << ",";
      first = false;
      json << "{";
      json << R"("event_id":")" << JsonEscape(a.event_id()) << "\",";
      json << R"("rule_name":")" << JsonEscape(a.rule_name()) << "\",";
      json << R"("severity":")" << JsonEscape(
        a.severity() == ::control_plane::ALERT_CRITICAL ? "Critical" :
        a.severity() == ::control_plane::ALERT_WARNING ? "Warning" : "Info") << "\",";
      json << R"("metric":")" << JsonEscape(a.metric()) << "\",";
      json << R"("current_value":)" << a.current_value() << ",";
      json << R"("threshold":)" << a.threshold() << ",";
      json << R"("operator":")" << JsonEscape(a.operator_str()) << "\",";
      json << R"("timestamp_ms":)" << a.timestamp_ms() << ",";
      json << R"("message":")" << JsonEscape(a.message()) << "\"";
      json << "}";
    }
    json << "],";

    json << R"("alert_history":[)";
    first = true;
    for (const auto& a : response.alert_history()) {
      if (!first) json << ",";
      first = false;
      json << "{";
      json << R"("event_id":")" << JsonEscape(a.event_id()) << "\",";
      json << R"("rule_name":")" << JsonEscape(a.rule_name()) << "\",";
      json << R"("severity":")" << JsonEscape(
        a.severity() == ::control_plane::ALERT_CRITICAL ? "Critical" :
        a.severity() == ::control_plane::ALERT_WARNING ? "Warning" : "Info") << "\",";
      json << R"("state":")" << JsonEscape(
        a.state() == ::control_plane::ALERT_FIRING ? "Firing" :
        a.state() == ::control_plane::ALERT_RESOLVED ? "Resolved" : "Inactive") << "\",";
      json << R"("metric":")" << JsonEscape(a.metric()) << "\",";
      json << R"("current_value":)" << a.current_value() << ",";
      json << R"("timestamp_ms":)" << a.timestamp_ms() << ",";
      json << R"("message":")" << JsonEscape(a.message()) << "\"";
      json << "}";
    }
    json << "]}";

    return json.str();
  } catch (const std::exception& e) {
    return R"({"error": ")" + JsonEscape(e.what()) + R"("})";
  }
}

std::string Observatory::HandleApiAlertRules() {
  if (!connected_.load() || !stub_) {
    return R"({"error": "Not connected to Control Plane", "rules": []})";
  }

  try {
    std::lock_guard<std::mutex> lock(grpc_mutex_);

    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() +
                        std::chrono::milliseconds(config_.grpc_timeout_ms));

    ::control_plane::GetAlertRulesRequest request;
    ::control_plane::GetAlertRulesResponse response;

    auto status = stub_->GetAlertRules(&context, request, &response);
    if (!status.ok()) {
      return R"({"error": "Failed to fetch alert rules", "rules": []})";
    }

    std::ostringstream json;
    json << "{\"rules\":[";

    bool first = true;
    for (const auto& r : response.rules()) {
      if (!first) json << ",";
      first = false;
      json << "{";
      json << R"("rule_id":")" << JsonEscape(r.rule_id()) << "\",";
      json << R"("name":")" << JsonEscape(r.name()) << "\",";
      json << R"("description":")" << JsonEscape(r.description()) << "\",";
      json << R"("metric":")" << JsonEscape(r.metric()) << "\",";
      json << R"("operator":")" << JsonEscape(r.operator_str()) << "\",";
      json << R"("threshold":)" << r.threshold() << ",";
      json << R"("severity":")" << JsonEscape(
        r.severity() == ::control_plane::ALERT_CRITICAL ? "Critical" :
        r.severity() == ::control_plane::ALERT_WARNING ? "Warning" : "Info") << "\",";
      json << R"("duration_sec":)" << r.duration_sec() << ",";
      json << R"("enabled":)" << (r.enabled() ? "true" : "false") << ",";
      json << R"("state":")" << JsonEscape(
        r.state() == ::control_plane::ALERT_FIRING ? "Firing" :
        r.state() == ::control_plane::ALERT_RESOLVED ? "Resolved" : "Inactive") << "\",";
      json << R"("last_value":)" << r.last_value();
      json << "}";
    }
    json << "]}";

    return json.str();
  } catch (const std::exception& e) {
    return R"({"error": ")" + JsonEscape(e.what()) + R"(", "rules": []})";
  }
}

std::string Observatory::HandleApiTraces(const std::string& query_type) {
  if (!connected_.load() || !stub_) {
    return R"({"error": "Not connected to Control Plane", "traces": []})";
  }

  try {
    std::lock_guard<std::mutex> lock(grpc_mutex_);

    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() +
                        std::chrono::milliseconds(config_.grpc_timeout_ms));

    ::control_plane::GetRecentTracesRequest request;
    request.set_limit(50);
    if (query_type == "slow") {
      request.set_slow_only(true);
      request.set_slow_threshold_ms(10000);
    } else if (query_type == "failed") {
      request.set_failed_only(true);
    }

    ::control_plane::GetRecentTracesResponse response;
    auto status = stub_->GetRecentTraces(&context, request, &response);
    if (!status.ok()) {
      return R"({"error": "Failed to fetch traces", "traces": []})";
    }

    std::ostringstream json;
    json << "{";
    json << R"("total_traces":)" << response.total_traces() << ",";
    json << R"("active_traces":)" << response.active_traces() << ",";
    json << R"("traces":[)";

    bool first = true;
    for (const auto& t : response.traces()) {
      if (!first) json << ",";
      first = false;
      json << "{";
      json << R"("trace_id":")" << JsonEscape(t.trace_id()) << "\",";
      json << R"("task_type":")" << JsonEscape(t.task_type()) << "\",";
      json << R"("source_node":")" << JsonEscape(t.source_node_id()) << "\",";
      json << R"("start_time_ms":)" << t.start_time_ms() << ",";
      json << R"("total_duration_ms":)" << t.total_duration_ms() << ",";
      json << R"("is_complete":)" << (t.is_complete() ? "true" : "false") << ",";
      json << R"("final_status":")" << JsonEscape(t.final_status()) << "\",";
      json << R"("queue_duration_ms":)" << t.queue_duration_ms() << ",";
      json << R"("schedule_duration_ms":)" << t.schedule_duration_ms() << ",";
      json << R"("execute_duration_ms":)" << t.execute_duration_ms() << ",";
      json << R"("transfer_duration_ms":)" << t.transfer_duration_ms() << ",";
      json << R"("span_count":)" << t.spans_size();
      json << "}";
    }
    json << "]}";

    return json.str();
  } catch (const std::exception& e) {
    return R"({"error": ")" + JsonEscape(e.what()) + R"(", "traces": []})";
  }
}

std::string Observatory::HandleApiTraceDetail(const std::string& task_id) {
  if (!connected_.load() || !stub_) {
    return R"({"error": "Not connected to Control Plane", "found": false})";
  }

  try {
    std::lock_guard<std::mutex> lock(grpc_mutex_);

    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() +
                        std::chrono::milliseconds(config_.grpc_timeout_ms));

    ::control_plane::GetTaskTraceRequest request;
    request.set_task_id(task_id);
    ::control_plane::GetTaskTraceResponse response;

    auto status = stub_->GetTaskTrace(&context, request, &response);
    if (!status.ok()) {
      return R"({"error": "Failed to fetch trace", "found": false})";
    }

    if (!response.found()) {
      return R"({"found": false})";
    }

    const auto& t = response.trace();
    std::ostringstream json;
    json << "{\"found\":true,\"trace\":{";
    json << R"("trace_id":")" << JsonEscape(t.trace_id()) << "\",";
    json << R"("task_type":")" << JsonEscape(t.task_type()) << "\",";
    json << R"("source_node":")" << JsonEscape(t.source_node_id()) << "\",";
    json << R"("start_time_ms":)" << t.start_time_ms() << ",";
    json << R"("end_time_ms":)" << t.end_time_ms() << ",";
    json << R"("total_duration_ms":)" << t.total_duration_ms() << ",";
    json << R"("is_complete":)" << (t.is_complete() ? "true" : "false") << ",";
    json << R"("final_status":")" << JsonEscape(t.final_status()) << "\",";
    json << R"("queue_duration_ms":)" << t.queue_duration_ms() << ",";
    json << R"("schedule_duration_ms":)" << t.schedule_duration_ms() << ",";
    json << R"("execute_duration_ms":)" << t.execute_duration_ms() << ",";
    json << R"("transfer_duration_ms":)" << t.transfer_duration_ms() << ",";
    json << R"("spans":[)";

    bool first = true;
    for (const auto& s : t.spans()) {
      if (!first) json << ",";
      first = false;
      json << "{";
      json << R"("span_id":")" << JsonEscape(s.span_id()) << "\",";
      json << R"("parent_span_id":")" << JsonEscape(s.parent_span_id()) << "\",";
      json << R"("operation":")" << JsonEscape(s.operation()) << "\",";
      json << R"("component":")" << JsonEscape(s.component()) << "\",";
      json << R"("worker_id":")" << JsonEscape(s.worker_id()) << "\",";
      json << R"("start_time_ms":)" << s.start_time_ms() << ",";
      json << R"("end_time_ms":)" << s.end_time_ms() << ",";
      json << R"("duration_ms":)" << s.duration_ms() << ",";
      json << R"("status":")" << JsonEscape(s.status()) << "\",";
      json << R"("error_message":")" << JsonEscape(s.error_message()) << "\"";
      json << "}";
    }
    json << "]}}";

    return json.str();
  } catch (const std::exception& e) {
    return R"({"error": ")" + JsonEscape(e.what()) + R"(", "found": false})";
  }
}

std::string Observatory::HandleApiPrometheusMetrics() {
  if (!connected_.load() || !stub_) {
    return "# Observatory not connected to Control Plane\n";
  }

  try {
    std::lock_guard<std::mutex> lock(grpc_mutex_);

    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() +
                        std::chrono::milliseconds(config_.grpc_timeout_ms));

    ::control_plane::PrometheusMetricsRequest request;
    ::control_plane::PrometheusMetricsResponse response;

    auto status = stub_->GetPrometheusMetrics(&context, request, &response);
    if (!status.ok()) {
      return "# Failed to fetch Prometheus metrics from Control Plane\n";
    }

    return response.metrics_text();
  } catch (const std::exception& e) {
    return std::string("# Error: ") + e.what() + "\n";
  }
}

std::string Observatory::HandleApiBulkLoadStatus(const std::string& task_id) {
  if (!connected_.load() || !stub_) {
    return R"({"error": "Not connected to Control Plane", "found": false})";
  }

  try {
    std::lock_guard<std::mutex> lock(grpc_mutex_);

    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() +
                        std::chrono::milliseconds(config_.grpc_timeout_ms));

    ::control_plane::QueryBulkLoadRequest request;
    request.set_task_id(task_id);
    ::control_plane::QueryBulkLoadResponse response;

    auto status = stub_->QueryBulkLoadStatus(&context, request, &response);
    if (!status.ok()) {
      return R"({"error": "Failed to fetch Bulk Load status", "found": false})";
    }

    if (!response.found()) {
      return R"({"found": false})";
    }

    std::ostringstream json;
    json << "{\"found\":true,";
    json << R"("task_id":")" << JsonEscape(response.task_id()) << "\",";
    json << R"("status":")" << TaskStatusToJsonString(response.overall_status()) << "\",";
    json << R"("phase":)" << response.phase() << ",";
    json << R"("total_shards":)" << response.total_shards() << ",";
    json << R"("completed_shards":)" << response.completed_shards() << ",";
    json << R"("failed_shards":)" << response.failed_shards() << ",";
    json << R"("running_shards":)" << response.running_shards() << ",";
    json << R"("progress_percent":)" << std::fixed << std::setprecision(1)
         << response.progress_percent() << ",";
    json << R"("shards":[)";

    bool first = true;
    for (const auto& s : response.shards()) {
      if (!first) json << ",";
      first = false;
      json << "{";
      json << R"("shard_id":")" << JsonEscape(s.shard_id()) << "\",";
      json << R"("shard_index":)" << s.shard_index() << ",";
      json << R"("status":")" << TaskStatusToJsonString(s.status()) << "\",";
      json << R"("worker":")" << JsonEscape(s.assigned_worker_id()) << "\",";
      json << R"("sst_count":)" << s.generated_sst_files_size();
      json << "}";
    }
    json << "]}";

    return json.str();
  } catch (const std::exception& e) {
    return R"({"error": ")" + JsonEscape(e.what()) + R"(", "found": false})";
  }
}

// ============================================================================
// 指标采集
// ============================================================================

void Observatory::MetricsCollectLoop() {
  while (running_.load()) {
    CollectMetrics();

    // 等待下一个采集周期
    for (uint32_t i = 0; i < config_.metrics_collect_interval_sec * 10 && running_.load(); ++i) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
  }
}

void Observatory::CollectMetrics() {
  if (!connected_.load() || !stub_) return;

  try {
    std::lock_guard<std::mutex> lock(grpc_mutex_);
    
    // 获取集群状态
    grpc::ClientContext ctx1;
    ctx1.set_deadline(std::chrono::system_clock::now() + 
                     std::chrono::milliseconds(config_.grpc_timeout_ms));
    ::control_plane::ClusterStatusRequest cluster_req;
    ::control_plane::ClusterStatusResponse cluster_resp;
    auto status1 = stub_->GetClusterStatus(&ctx1, cluster_req, &cluster_resp);
    
    // 获取任务统计
    grpc::ClientContext ctx2;
    ctx2.set_deadline(std::chrono::system_clock::now() + 
                     std::chrono::milliseconds(config_.grpc_timeout_ms));
    ::control_plane::TaskStatisticsRequest stats_req;
    ::control_plane::TaskStatisticsResponse stats_resp;
    auto status2 = stub_->GetTaskStatistics(&ctx2, stats_req, &stats_resp);
    
    // 获取指标快照
    grpc::ClientContext ctx3;
    ctx3.set_deadline(std::chrono::system_clock::now() + 
                     std::chrono::milliseconds(config_.grpc_timeout_ms));
    ::control_plane::MetricsSnapshotRequest metrics_req;
    ::control_plane::MetricsSnapshotResponse metrics_resp;
    auto status3 = stub_->GetMetricsSnapshot(&ctx3, metrics_req, &metrics_resp);
    
    if (!status1.ok()) return;

    MetricsDataPoint dp;
    dp.timestamp_ms = GetCurrentTimeMs();
    dp.online_workers = cluster_resp.online_workers();
    dp.pending_tasks = cluster_resp.pending_tasks();
    dp.running_tasks = cluster_resp.running_tasks();
    dp.completed_count = cluster_resp.total_completed();
    dp.failed_count = cluster_resp.total_failed();
    dp.avg_queue_time_ms = stats_resp.avg_queue_time_ms();
    dp.avg_execution_time_ms = stats_resp.avg_execution_time_ms();
    dp.memory_usage = metrics_resp.cluster_memory_usage();

    {
      std::lock_guard<std::mutex> lock2(metrics_mutex_);
      metrics_history_.push_back(dp);
      while (metrics_history_.size() > config_.metrics_history_size) {
        metrics_history_.pop_front();
      }
    }
  } catch (const std::exception& e) {
    std::cerr << "[Observatory] Metrics collect error: " << e.what() << std::endl;
  }
}

// ============================================================================
// 辅助方法
// ============================================================================

std::string Observatory::JsonEscape(const std::string& s) {
  std::ostringstream o;
  for (char c : s) {
    switch (c) {
      case '"': o << "\\\""; break;
      case '\\': o << "\\\\"; break;
      case '\b': o << "\\b"; break;
      case '\f': o << "\\f"; break;
      case '\n': o << "\\n"; break;
      case '\r': o << "\\r"; break;
      case '\t': o << "\\t"; break;
      default:
        if ('\x00' <= c && c <= '\x1f') {
          o << "\\u" << std::hex << std::setw(4) << std::setfill('0') << (int)c;
        } else {
          o << c;
        }
    }
  }
  return o.str();
}

std::string Observatory::TaskStatusToJsonString(::control_plane::TaskStatus status) {
  switch (status) {
    case ::control_plane::TASK_PENDING: return "Pending";
    case ::control_plane::TASK_ASSIGNED: return "Assigned";
    case ::control_plane::TASK_RUNNING: return "Running";
    case ::control_plane::TASK_COMPLETED: return "Completed";
    case ::control_plane::TASK_FAILED: return "Failed";
    case ::control_plane::TASK_CANCELLED: return "Cancelled";
    case ::control_plane::TASK_TIMEOUT: return "Timeout";
    default: return "Unknown";
  }
}

std::string Observatory::WorkerStatusToJsonString(::control_plane::WorkerStatus status) {
  switch (status) {
    case ::control_plane::WORKER_ONLINE: return "Online";
    case ::control_plane::WORKER_OFFLINE: return "Offline";
    case ::control_plane::WORKER_BUSY: return "Busy";
    case ::control_plane::WORKER_DRAINING: return "Draining";
    case ::control_plane::WORKER_MAINTENANCE: return "Maintenance";
    default: return "Unknown";
  }
}

std::string TaskPriorityToJsonString(::control_plane::TaskPriority priority) {
  switch (priority) {
    case ::control_plane::PRIORITY_LOW: return "Low";
    case ::control_plane::PRIORITY_NORMAL: return "Normal";
    case ::control_plane::PRIORITY_HIGH: return "High";
    case ::control_plane::PRIORITY_URGENT: return "Urgent";
    default: return "Unknown";
  }
}

int64_t Observatory::GetCurrentTimeMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
    std::chrono::system_clock::now().time_since_epoch()).count();
}

std::string Observatory::BuildHttpResponse(int status_code,
                                           const std::string& content_type,
                                           const std::string& body) {
  std::ostringstream response;
  response << "HTTP/1.1 " << status_code << " ";
  switch (status_code) {
    case 200: response << "OK"; break;
    case 400: response << "Bad Request"; break;
    case 404: response << "Not Found"; break;
    case 500: response << "Internal Server Error"; break;
    default: response << "Unknown"; break;
  }
  response << "\r\n";
  response << "Content-Type: " << content_type << "\r\n";
  response << "Content-Length: " << body.size() << "\r\n";
  if (config_.enable_cors) {
    response << "Access-Control-Allow-Origin: *\r\n";
    response << "Access-Control-Allow-Methods: GET, POST, OPTIONS\r\n";
    response << "Access-Control-Allow-Headers: Content-Type\r\n";
  }
  response << "Connection: close\r\n";
  response << "\r\n";
  response << body;
  return response.str();
}

std::string Observatory::BuildJsonResponse(const std::string& json) {
  return BuildHttpResponse(200, "application/json; charset=utf-8", json);
}

std::string Observatory::BuildErrorResponse(int status_code, const std::string& message) {
  std::ostringstream json;
  json << R"({"error":")" << JsonEscape(message) << "\"}";
  return BuildHttpResponse(status_code, "application/json; charset=utf-8", json.str());
}

std::string Observatory::GetEmbeddedHtml() {
  // 内嵌的 HTML 页面 - 支持显示连接状态
  return R"HTML(<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Remote Compaction Observatory</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
            background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
            color: #e0e0e0;
            min-height: 100vh;
            padding: 20px;
        }
        .header {
            text-align: center;
            padding: 20px 0 30px;
        }
        .header h1 {
            font-size: 2em;
            color: #4fc3f7;
            text-shadow: 0 0 20px rgba(79, 195, 247, 0.3);
        }
        .header .subtitle {
            color: #90a4ae;
            margin-top: 5px;
        }
        .connection-status {
            display: inline-flex;
            align-items: center;
            gap: 8px;
            padding: 8px 16px;
            background: rgba(0,0,0,0.3);
            border-radius: 20px;
            margin-top: 10px;
            font-size: 0.9em;
        }
        .connection-status.connected { border: 1px solid #81c784; }
        .connection-status.disconnected { border: 1px solid #e57373; }
        .connection-dot {
            width: 10px;
            height: 10px;
            border-radius: 50%;
            animation: pulse 2s infinite;
        }
        .connection-dot.connected { background: #81c784; }
        .connection-dot.disconnected { background: #e57373; }
        .dashboard {
            max-width: 1400px;
            margin: 0 auto;
        }
        .status-bar {
            display: flex;
            gap: 15px;
            margin-bottom: 20px;
            flex-wrap: wrap;
        }
        .status-item {
            background: rgba(255,255,255,0.05);
            border-radius: 12px;
            padding: 15px 25px;
            flex: 1;
            min-width: 150px;
            text-align: center;
            border: 1px solid rgba(255,255,255,0.1);
            transition: all 0.3s;
        }
        .status-item:hover {
            background: rgba(255,255,255,0.08);
            transform: translateY(-2px);
        }
        .status-item .value {
            font-size: 2em;
            font-weight: bold;
            color: #4fc3f7;
        }
        .status-item .label {
            color: #90a4ae;
            font-size: 0.9em;
            margin-top: 5px;
        }
        .status-item.success .value { color: #81c784; }
        .status-item.warning .value { color: #ffb74d; }
        .status-item.error .value { color: #e57373; }
        .grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(400px, 1fr));
            gap: 20px;
            margin-bottom: 20px;
        }
        .card {
            background: rgba(255,255,255,0.05);
            border-radius: 12px;
            padding: 20px;
            border: 1px solid rgba(255,255,255,0.1);
        }
        .card h3 {
            color: #4fc3f7;
            margin-bottom: 15px;
            font-size: 1.1em;
            display: flex;
            align-items: center;
            gap: 8px;
        }
        .card h3::before {
            content: '';
            width: 4px;
            height: 20px;
            background: #4fc3f7;
            border-radius: 2px;
        }
        .chart-container {
            height: 200px;
            position: relative;
        }
        .task-list {
            max-height: 400px;
            overflow-y: auto;
        }
        .task-item {
            background: rgba(0,0,0,0.2);
            border-radius: 8px;
            padding: 12px 15px;
            margin-bottom: 10px;
            display: flex;
            justify-content: space-between;
            align-items: center;
            transition: background 0.2s;
        }
        .task-item:hover {
            background: rgba(0,0,0,0.3);
        }
        .task-info {
            flex: 1;
        }
        .task-id {
            font-family: 'Monaco', 'Consolas', monospace;
            font-size: 0.85em;
            color: #80cbc4;
        }
        .task-meta {
            font-size: 0.8em;
            color: #90a4ae;
            margin-top: 4px;
        }
        .task-status {
            padding: 4px 10px;
            border-radius: 12px;
            font-size: 0.75em;
            font-weight: 600;
            text-transform: uppercase;
        }
        .task-status.Pending { background: #5c6bc0; }
        .task-status.Running { background: #4fc3f7; color: #1a1a2e; }
        .task-status.Completed { background: #81c784; color: #1a1a2e; }
        .task-status.Failed { background: #e57373; }
        .task-status.Timeout { background: #ffb74d; color: #1a1a2e; }
        .worker-list {
            max-height: 300px;
            overflow-y: auto;
        }
        .worker-item {
            background: rgba(0,0,0,0.2);
            border-radius: 8px;
            padding: 12px 15px;
            margin-bottom: 10px;
        }
        .worker-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 8px;
        }
        .worker-name {
            font-family: 'Monaco', 'Consolas', monospace;
            font-size: 0.9em;
            color: #80cbc4;
        }
        .worker-status {
            padding: 3px 8px;
            border-radius: 10px;
            font-size: 0.7em;
            font-weight: 600;
        }
        .worker-status.Online { background: #81c784; color: #1a1a2e; }
        .worker-status.Offline { background: #e57373; }
        .worker-status.Busy { background: #ffb74d; color: #1a1a2e; }
        .worker-progress {
            background: rgba(255,255,255,0.1);
            border-radius: 4px;
            height: 6px;
            margin-top: 8px;
            overflow: hidden;
        }
        .worker-progress-bar {
            height: 100%;
            background: linear-gradient(90deg, #4fc3f7, #81c784);
            border-radius: 4px;
            transition: width 0.3s;
        }
        .worker-stats {
            display: flex;
            gap: 15px;
            font-size: 0.8em;
            color: #90a4ae;
            margin-top: 6px;
        }
        .latency-grid {
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 15px;
            text-align: center;
        }
        .latency-item .value {
            font-size: 1.5em;
            font-weight: bold;
            color: #4fc3f7;
        }
        .latency-item .label {
            font-size: 0.85em;
            color: #90a4ae;
        }
        .refresh-info {
            text-align: center;
            color: #607d8b;
            font-size: 0.8em;
            margin-top: 20px;
        }
        .health-indicator {
            display: inline-block;
            width: 10px;
            height: 10px;
            border-radius: 50%;
            margin-right: 8px;
            animation: pulse 2s infinite;
        }
        .health-indicator.healthy { background: #81c784; }
        .health-indicator.warning { background: #ffb74d; }
        .health-indicator.critical { background: #e57373; }
        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.5; }
        }
        ::-webkit-scrollbar {
            width: 6px;
        }
        ::-webkit-scrollbar-track {
            background: rgba(255,255,255,0.05);
            border-radius: 3px;
        }
        ::-webkit-scrollbar-thumb {
            background: rgba(255,255,255,0.2);
            border-radius: 3px;
        }
        .tab-buttons {
            display: flex;
            gap: 10px;
            margin-bottom: 15px;
        }
        .tab-btn {
            padding: 8px 16px;
            border: none;
            background: rgba(255,255,255,0.1);
            color: #90a4ae;
            border-radius: 8px;
            cursor: pointer;
            transition: all 0.2s;
        }
        .tab-btn.active {
            background: #4fc3f7;
            color: #1a1a2e;
        }
        .tab-btn:hover:not(.active) {
            background: rgba(255,255,255,0.15);
        }
        .alert-badge {
            padding: 6px 14px;
            border-radius: 12px;
            font-size: 0.85em;
            font-weight: 600;
        }
        .alert-badge.info { background: rgba(79,195,247,0.2); color: #4fc3f7; }
        .alert-badge.warning { background: rgba(255,183,77,0.2); color: #ffb74d; }
        .alert-badge.critical { background: rgba(229,115,115,0.2); color: #e57373; }
        .alert-item {
            background: rgba(0,0,0,0.2);
            border-radius: 8px;
            padding: 10px 15px;
            margin-bottom: 8px;
            border-left: 4px solid #4fc3f7;
        }
        .alert-item.Warning { border-left-color: #ffb74d; }
        .alert-item.Critical { border-left-color: #e57373; }
        .alert-item .alert-title {
            font-weight: 600;
            font-size: 0.9em;
        }
        .alert-item .alert-detail {
            font-size: 0.8em;
            color: #90a4ae;
            margin-top: 4px;
        }
        .trace-phases {
            display: flex;
            gap: 2px;
            margin-top: 6px;
            height: 6px;
        }
        .trace-phase {
            border-radius: 3px;
            min-width: 4px;
        }
        .trace-phase.queue { background: #ce93d8; }
        .trace-phase.schedule { background: #4fc3f7; }
        .trace-phase.execute { background: #81c784; }
        .trace-phase.transfer { background: #ffb74d; }
        .disconnected-overlay {
            position: fixed;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            background: rgba(0,0,0,0.7);
            display: flex;
            justify-content: center;
            align-items: center;
            z-index: 1000;
        }
        .disconnected-overlay.hidden { display: none; }
        .disconnected-message {
            background: rgba(30,30,50,0.95);
            padding: 40px;
            border-radius: 16px;
            text-align: center;
            border: 1px solid #e57373;
        }
        .disconnected-message h2 {
            color: #e57373;
            margin-bottom: 15px;
        }
        .disconnected-message p {
            color: #90a4ae;
            margin-bottom: 20px;
        }
        .retry-spinner {
            width: 30px;
            height: 30px;
            border: 3px solid rgba(255,255,255,0.1);
            border-top-color: #4fc3f7;
            border-radius: 50%;
            animation: spin 1s linear infinite;
            margin: 0 auto;
        }
        @keyframes spin {
            to { transform: rotate(360deg); }
        }
    </style>
</head>
<body>
    <!-- Disconnected Overlay -->
    <div class="disconnected-overlay hidden" id="disconnectedOverlay">
        <div class="disconnected-message">
            <h2>⚠️ Connection Lost</h2>
            <p>Unable to connect to Control Plane<br><span id="cpAddress">-</span></p>
            <div class="retry-spinner"></div>
            <p style="margin-top:15px;font-size:0.9em;">Reconnecting...</p>
        </div>
    </div>

    <div class="header">
        <h1>🔭 Remote Compaction Observatory</h1>
        <p class="subtitle">
            <span class="health-indicator healthy" id="healthIndicator"></span>
            CaaS-LSM Real-time Monitoring Dashboard
        </p>
        <div class="connection-status connected" id="connectionStatus">
            <span class="connection-dot connected" id="connectionDot"></span>
            <span id="connectionText">Connected to Control Plane</span>
        </div>
    </div>

    <div class="dashboard">
        <!-- Status Bar -->
        <div class="status-bar">
            <div class="status-item">
                <div class="value" id="onlineWorkers">-</div>
                <div class="label">Online Workers</div>
            </div>
            <div class="status-item warning">
                <div class="value" id="pendingTasks">-</div>
                <div class="label">Pending Tasks</div>
            </div>
            <div class="status-item">
                <div class="value" id="runningTasks">-</div>
                <div class="label">Running Tasks</div>
            </div>
            <div class="status-item success">
                <div class="value" id="completedTasks">-</div>
                <div class="label">Completed</div>
            </div>
            <div class="status-item error">
                <div class="value" id="failedTasks">-</div>
                <div class="label">Failed</div>
            </div>
        </div>

        <!-- Charts Grid -->
        <div class="grid">
            <!-- Throughput Chart -->
            <div class="card">
                <h3>Throughput</h3>
                <div class="chart-container">
                    <canvas id="throughputChart"></canvas>
                </div>
            </div>

            <!-- Tasks Chart -->
            <div class="card">
                <h3>Task Queue</h3>
                <div class="chart-container">
                    <canvas id="tasksChart"></canvas>
                </div>
            </div>

            <!-- Latency -->
            <div class="card">
                <h3>Latency Distribution</h3>
                <div class="latency-grid">
                    <div class="latency-item">
                        <div class="value" id="latencyP50">-</div>
                        <div class="label">P50</div>
                    </div>
                    <div class="latency-item">
                        <div class="value" id="latencyP95">-</div>
                        <div class="label">P95</div>
                    </div>
                    <div class="latency-item">
                        <div class="value" id="latencyP99">-</div>
                        <div class="label">P99</div>
                    </div>
                </div>
                <div class="chart-container" style="height:120px;margin-top:15px;">
                    <canvas id="latencyChart"></canvas>
                </div>
            </div>

            <!-- Resource Usage -->
            <div class="card">
                <h3>Resource Usage</h3>
                <div class="chart-container">
                    <canvas id="resourceChart"></canvas>
                </div>
            </div>
        </div>

        <!-- Alert Panel -->
        <div class="card" id="alertPanel" style="margin-bottom:20px;">
            <h3>🔔 Alerts</h3>
            <div class="alert-summary" style="display:flex;gap:15px;margin-bottom:15px;">
                <div class="alert-badge info" id="alertInfoCount">0 Info</div>
                <div class="alert-badge warning" id="alertWarningCount">0 Warning</div>
                <div class="alert-badge critical" id="alertCriticalCount">0 Critical</div>
            </div>
            <div class="alert-list" id="alertList" style="max-height:200px;overflow-y:auto;">
                <div style="color:#607d8b;padding:10px;">No active alerts</div>
            </div>
        </div>

        <!-- Bottom Grid -->
        <div class="grid">
            <!-- Task List -->
            <div class="card">
                <h3>Active Tasks</h3>
                <div class="tab-buttons">
                    <button class="tab-btn active" data-status="">All</button>
                    <button class="tab-btn" data-status="running">Running</button>
                    <button class="tab-btn" data-status="pending">Pending</button>
                    <button class="tab-btn" data-status="failed">Failed</button>
                </div>
                <div class="task-list" id="taskList">
                    <div class="task-item">
                        <div class="task-info">Loading...</div>
                    </div>
                </div>
            </div>

            <!-- Worker List -->
            <div class="card">
                <h3>Workers</h3>
                <div class="worker-list" id="workerList">
                    <div class="worker-item">Loading...</div>
                </div>
            </div>
        </div>

        <!-- Traces Grid -->
        <div class="card" style="margin-bottom:20px;">
            <h3>🔍 Task Traces</h3>
            <div class="tab-buttons">
                <button class="tab-btn active" data-trace-type="recent" onclick="switchTraceTab(this,'recent')">Recent</button>
                <button class="tab-btn" data-trace-type="slow" onclick="switchTraceTab(this,'slow')">Slow</button>
                <button class="tab-btn" data-trace-type="failed" onclick="switchTraceTab(this,'failed')">Failed</button>
            </div>
            <div style="display:flex;gap:10px;margin-bottom:10px;font-size:0.85em;color:#90a4ae;">
                <span>Total Traces: <strong id="totalTraces">0</strong></span>
                <span>Active: <strong id="activeTraces">0</strong></span>
            </div>
            <div class="task-list" id="traceList" style="max-height:300px;">
                <div class="task-item"><div class="task-info">Loading...</div></div>
            </div>
        </div>

        <div class="refresh-info">
            Auto-refresh every 2 seconds | Last updated: <span id="lastUpdate">-</span>
        </div>
    </div>

    <script>
        // Chart configurations
        const chartOptions = {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: {
                x: { display: false },
                y: { 
                    beginAtZero: true,
                    grid: { color: 'rgba(255,255,255,0.1)' },
                    ticks: { color: '#90a4ae' }
                }
            }
        };

        // Initialize charts
        let throughputChart, tasksChart, latencyChart, resourceChart;
        let currentStatusFilter = '';
        let isConnected = false;

        function initCharts() {
            const ctx1 = document.getElementById('throughputChart').getContext('2d');
            throughputChart = new Chart(ctx1, {
                type: 'line',
                data: {
                    labels: [],
                    datasets: [
                        { label: 'Completed', data: [], borderColor: '#81c784', backgroundColor: 'rgba(129,199,132,0.1)', fill: true, tension: 0.4 },
                        { label: 'Failed', data: [], borderColor: '#e57373', backgroundColor: 'rgba(229,115,115,0.1)', fill: true, tension: 0.4 }
                    ]
                },
                options: { ...chartOptions, plugins: { legend: { display: true, labels: { color: '#90a4ae' } } } }
            });

            const ctx2 = document.getElementById('tasksChart').getContext('2d');
            tasksChart = new Chart(ctx2, {
                type: 'line',
                data: {
                    labels: [],
                    datasets: [
                        { label: 'Pending', data: [], borderColor: '#ffb74d', tension: 0.4 },
                        { label: 'Running', data: [], borderColor: '#4fc3f7', tension: 0.4 }
                    ]
                },
                options: { ...chartOptions, plugins: { legend: { display: true, labels: { color: '#90a4ae' } } } }
            });

            const ctx3 = document.getElementById('latencyChart').getContext('2d');
            latencyChart = new Chart(ctx3, {
                type: 'line',
                data: {
                    labels: [],
                    datasets: [
                        { label: 'Queue', data: [], borderColor: '#ce93d8', tension: 0.4 },
                        { label: 'Execution', data: [], borderColor: '#4fc3f7', tension: 0.4 }
                    ]
                },
                options: { ...chartOptions, plugins: { legend: { display: true, labels: { color: '#90a4ae', boxWidth: 12 } } } }
            });

            const ctx4 = document.getElementById('resourceChart').getContext('2d');
            resourceChart = new Chart(ctx4, {
                type: 'line',
                data: {
                    labels: [],
                    datasets: [
                        { label: 'Memory', data: [], borderColor: '#f48fb1', backgroundColor: 'rgba(244,143,177,0.1)', fill: true, tension: 0.4 }
                    ]
                },
                options: { 
                    ...chartOptions, 
                    scales: { ...chartOptions.scales, y: { ...chartOptions.scales.y, max: 1, ticks: { callback: v => (v*100)+'%', color: '#90a4ae' } } },
                    plugins: { legend: { display: true, labels: { color: '#90a4ae' } } }
                }
            });
        }

        function updateConnectionStatus(connected, address) {
            isConnected = connected;
            const status = document.getElementById('connectionStatus');
            const dot = document.getElementById('connectionDot');
            const text = document.getElementById('connectionText');
            const overlay = document.getElementById('disconnectedOverlay');
            
            if (connected) {
                status.className = 'connection-status connected';
                dot.className = 'connection-dot connected';
                text.textContent = 'Connected to Control Plane';
                overlay.classList.add('hidden');
            } else {
                status.className = 'connection-status disconnected';
                dot.className = 'connection-dot disconnected';
                text.textContent = 'Disconnected';
                document.getElementById('cpAddress').textContent = address || '-';
                overlay.classList.remove('hidden');
            }
        }

        async function fetchStatus() {
            try {
                const res = await fetch('/api/status');
                const data = await res.json();
                updateConnectionStatus(data.connected, data.control_plane_address);
                return data.connected;
            } catch (e) {
                updateConnectionStatus(false);
                return false;
            }
        }

        async function fetchMetrics() {
            try {
                const res = await fetch('/api/metrics');
                const data = await res.json();
                
                if (data.error && !data.connected) {
                    updateConnectionStatus(false);
                    return;
                }
                
                updateConnectionStatus(true);
                
                // Update status bar
                document.getElementById('onlineWorkers').textContent = data.cluster?.online_workers || 0;
                document.getElementById('pendingTasks').textContent = data.cluster?.pending_tasks || 0;
                document.getElementById('runningTasks').textContent = data.cluster?.running_tasks || 0;
                document.getElementById('completedTasks').textContent = formatNumber(data.throughput?.total_completed || 0);
                document.getElementById('failedTasks').textContent = formatNumber(data.throughput?.total_failed || 0);

                // Update latency
                document.getElementById('latencyP50').textContent = (data.latency?.p50_ms || 0) + 'ms';
                document.getElementById('latencyP95').textContent = (data.latency?.p95_ms || 0) + 'ms';
                document.getElementById('latencyP99').textContent = (data.latency?.p99_ms || 0) + 'ms';

                // Update health indicator
                const health = document.getElementById('healthIndicator');
                if (!data.cluster || data.cluster.online_workers === 0) {
                    health.className = 'health-indicator critical';
                } else if (data.cluster.pending_tasks > 100) {
                    health.className = 'health-indicator warning';
                } else {
                    health.className = 'health-indicator healthy';
                }

            } catch (e) {
                console.error('Failed to fetch metrics:', e);
            }
        }

        async function fetchHistory() {
            try {
                const res = await fetch('/api/metrics/history');
                const data = await res.json();
                
                const h = data.history || [];
                const labels = h.map((_, i) => '');
                
                // Calculate throughput diff
                const completedDiff = [];
                const failedDiff = [];
                for (let i = 1; i < h.length; i++) {
                    completedDiff.push(h[i].completed - h[i-1].completed);
                    failedDiff.push(h[i].failed - h[i-1].failed);
                }

                throughputChart.data.labels = labels.slice(1);
                throughputChart.data.datasets[0].data = completedDiff;
                throughputChart.data.datasets[1].data = failedDiff;
                throughputChart.update('none');

                tasksChart.data.labels = labels;
                tasksChart.data.datasets[0].data = h.map(d => d.pending);
                tasksChart.data.datasets[1].data = h.map(d => d.running);
                tasksChart.update('none');

                latencyChart.data.labels = labels;
                latencyChart.data.datasets[0].data = h.map(d => d.queue_ms);
                latencyChart.data.datasets[1].data = h.map(d => d.exec_ms);
                latencyChart.update('none');

                resourceChart.data.labels = labels;
                resourceChart.data.datasets[0].data = h.map(d => d.mem);
                resourceChart.update('none');

            } catch (e) {
                console.error('Failed to fetch history:', e);
            }
        }

        async function fetchTasks() {
            try {
                const url = currentStatusFilter ? `/api/tasks?status=${currentStatusFilter}` : '/api/tasks';
                const res = await fetch(url);
                const data = await res.json();
                
                const list = document.getElementById('taskList');
                if (!data.tasks || data.tasks.length === 0) {
                    list.innerHTML = '<div class="task-item"><div class="task-info" style="color:#607d8b">No tasks</div></div>';
                    return;
                }

                list.innerHTML = data.tasks.slice(0, 20).map(t => `
                    <div class="task-item">
                        <div class="task-info">
                            <div class="task-id">${t.id}</div>
                            <div class="task-meta">
                                ${t.source} | Store ${t.store_id} | ${t.worker || 'Unassigned'}
                                ${t.elapsed_ms > 0 ? ' | ' + formatDuration(t.elapsed_ms) : ''}
                            </div>
                        </div>
                        <div class="task-status ${t.status}">${t.status}</div>
                    </div>
                `).join('');

            } catch (e) {
                console.error('Failed to fetch tasks:', e);
            }
        }

        async function fetchWorkers() {
            try {
                const res = await fetch('/api/workers');
                const data = await res.json();
                
                const list = document.getElementById('workerList');
                if (!data.workers || data.workers.length === 0) {
                    list.innerHTML = '<div class="worker-item" style="color:#607d8b">No workers registered</div>';
                    return;
                }

                list.innerHTML = data.workers.map(w => {
                    const loadPct = w.max_tasks > 0 ? (w.active_tasks / w.max_tasks * 100) : 0;
                    const memPct = w.memory_mb > 0 ? (w.memory_used_mb / w.memory_mb * 100) : 0;
                    return `
                        <div class="worker-item">
                            <div class="worker-header">
                                <span class="worker-name">${w.id}</span>
                                <span class="worker-status ${w.status}">${w.status}</span>
                            </div>
                            <div class="worker-stats">
                                <span>📍 ${w.address}</span>
                                <span>✓ ${w.total_completed}</span>
                                <span>✗ ${w.total_failed}</span>
                                <span>🔧 ${w.active_tasks}/${w.max_tasks}</span>
                            </div>
                            <div class="worker-progress">
                                <div class="worker-progress-bar" style="width:${loadPct}%"></div>
                            </div>
                        </div>
                    `;
                }).join('');

            } catch (e) {
                console.error('Failed to fetch workers:', e);
            }
        }

        function formatNumber(n) {
            if (n >= 1000000) return (n/1000000).toFixed(1) + 'M';
            if (n >= 1000) return (n/1000).toFixed(1) + 'K';
            return n.toString();
        }

        function formatDuration(ms) {
            if (ms < 1000) return ms + 'ms';
            if (ms < 60000) return (ms/1000).toFixed(1) + 's';
            return (ms/60000).toFixed(1) + 'm';
        }

        let currentTraceType = 'recent';

        async function fetchAlerts() {
            try {
                const res = await fetch('/api/alerts');
                const data = await res.json();
                if (data.error) return;

                document.getElementById('alertInfoCount').textContent = (data.info_count || 0) + ' Info';
                document.getElementById('alertWarningCount').textContent = (data.warning_count || 0) + ' Warning';
                document.getElementById('alertCriticalCount').textContent = (data.critical_count || 0) + ' Critical';

                const list = document.getElementById('alertList');
                const alerts = data.active_alerts || [];
                if (alerts.length === 0) {
                    list.innerHTML = '<div style="color:#607d8b;padding:10px;">✅ No active alerts</div>';
                    return;
                }

                list.innerHTML = alerts.map(a => `
                    <div class="alert-item ${a.severity}">
                        <div class="alert-title">${a.severity === 'Critical' ? '🔴' : a.severity === 'Warning' ? '🟡' : 'ℹ️'} ${a.rule_name}</div>
                        <div class="alert-detail">${a.message} (${a.metric}: ${a.current_value.toFixed(1)} ${a.operator} ${a.threshold})</div>
                    </div>
                `).join('');
            } catch (e) {
                console.error('Failed to fetch alerts:', e);
            }
        }

        async function fetchTraces(type) {
            try {
                const url = type === 'recent' ? '/api/traces' : `/api/traces?type=${type}`;
                const res = await fetch(url);
                const data = await res.json();
                if (data.error) return;

                document.getElementById('totalTraces').textContent = data.total_traces || 0;
                document.getElementById('activeTraces').textContent = data.active_traces || 0;

                const list = document.getElementById('traceList');
                const traces = data.traces || [];
                if (traces.length === 0) {
                    list.innerHTML = '<div class="task-item"><div class="task-info" style="color:#607d8b">No traces</div></div>';
                    return;
                }

                list.innerHTML = traces.slice(0, 30).map(t => {
                    const total = t.total_duration_ms || 1;
                    const qPct = Math.max(2, t.queue_duration_ms / total * 100);
                    const sPct = Math.max(2, t.schedule_duration_ms / total * 100);
                    const ePct = Math.max(2, t.execute_duration_ms / total * 100);
                    const tPct = Math.max(2, t.transfer_duration_ms / total * 100);
                    return `
                        <div class="task-item">
                            <div class="task-info" style="flex:1">
                                <div class="task-id">${t.trace_id}</div>
                                <div class="task-meta">
                                    ${t.task_type} | ${t.source_node} | ${t.span_count} spans | ${formatDuration(t.total_duration_ms)}
                                </div>
                                <div class="trace-phases">
                                    <div class="trace-phase queue" style="flex:${qPct}" title="Queue: ${formatDuration(t.queue_duration_ms)}"></div>
                                    <div class="trace-phase schedule" style="flex:${sPct}" title="Schedule: ${formatDuration(t.schedule_duration_ms)}"></div>
                                    <div class="trace-phase execute" style="flex:${ePct}" title="Execute: ${formatDuration(t.execute_duration_ms)}"></div>
                                    <div class="trace-phase transfer" style="flex:${tPct}" title="Transfer: ${formatDuration(t.transfer_duration_ms)}"></div>
                                </div>
                            </div>
                            <div class="task-status ${t.final_status}">${t.final_status}</div>
                        </div>
                    `;
                }).join('');
            } catch (e) {
                console.error('Failed to fetch traces:', e);
            }
        }

        function switchTraceTab(btn, type) {
            document.querySelectorAll('[data-trace-type]').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            currentTraceType = type;
            fetchTraces(type);
        }

        async function updateAll() {
            const connected = await fetchStatus();
            if (connected) {
                fetchMetrics();
                fetchHistory();
                fetchTasks();
                fetchWorkers();
                fetchAlerts();
                fetchTraces(currentTraceType);
            }
            document.getElementById('lastUpdate').textContent = new Date().toLocaleTimeString();
        }

        // Tab buttons
        document.querySelectorAll('.tab-btn').forEach(btn => {
            btn.addEventListener('click', () => {
                document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
                btn.classList.add('active');
                currentStatusFilter = btn.dataset.status;
                fetchTasks();
            });
        });

        // Initialize
        initCharts();
        updateAll();
        setInterval(updateAll, 2000);
    </script>
</body>
</html>
)HTML";
}

}  // namespace control_plane
}  // namespace tendisplus
