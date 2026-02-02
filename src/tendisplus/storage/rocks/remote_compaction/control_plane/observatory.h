// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

// Observatory - 独立可视化观测平面
// 通过 gRPC 连接 Control Plane 获取数据，提供 HTTP API 和 Web 界面
//
// 架构：
// ┌─────────────────┐        ┌─────────────────────────────────────┐
// │   Observatory   │  gRPC  │         Control Plane               │
// │  (独立进程)      │◄──────►│                                     │
// │                 │        │  - GetClusterStatus()               │
// │  HTTP: 8080     │        │  - GetTaskStatistics()              │
// │  - /api/*       │        │  - ListWorkers()                    │
// │  - /            │        │  - ListTasks()                      │
// └─────────────────┘        │  - GetMetricsSnapshot()             │
//         │                  │  - GetWorkerDetail()                │
//         │                  └─────────────────────────────────────┘
//         ▼
//    Web Browser

#pragma once

#include <atomic>
#include <chrono>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <grpcpp/grpcpp.h>
#include "control_plane.grpc.pb.h"

namespace tendisplus {
namespace control_plane {

// ============================================================================
// 历史数据点 - 用于绘制时序图表
// ============================================================================
struct MetricsDataPoint {
  int64_t timestamp_ms = 0;
  
  // 集群状态
  uint32_t online_workers = 0;
  uint32_t pending_tasks = 0;
  uint32_t running_tasks = 0;
  
  // 吞吐量
  uint64_t completed_count = 0;
  uint64_t failed_count = 0;
  
  // 延迟 (毫秒)
  uint64_t avg_queue_time_ms = 0;
  uint64_t avg_execution_time_ms = 0;
  
  // 资源使用
  double cpu_usage = 0.0;
  double memory_usage = 0.0;
};

// ============================================================================
// 观测平面配置
// ============================================================================
struct ObservatoryConfig {
  // gRPC 连接配置
  std::string control_plane_address = "localhost:50051";  // Control Plane 地址
  uint32_t grpc_timeout_ms = 5000;                        // gRPC 调用超时
  uint32_t reconnect_interval_sec = 5;                    // 重连间隔
  
  // HTTP 服务配置
  uint16_t http_port = 8080;                   // HTTP 服务端口
  std::string bind_address = "0.0.0.0";        // 绑定地址
  std::string static_files_path = "";          // 静态文件路径 (留空则使用内嵌)
  
  // 指标采集配置
  uint32_t metrics_history_size = 300;         // 保留最近 N 个数据点
  uint32_t metrics_collect_interval_sec = 2;   // 指标采集间隔
  bool enable_cors = true;                     // 启用 CORS
};

// ============================================================================
// Observatory - 独立观测平面服务
// ============================================================================
class Observatory {
 public:
  explicit Observatory(const ObservatoryConfig& config);
  ~Observatory();

  // 禁止拷贝
  Observatory(const Observatory&) = delete;
  Observatory& operator=(const Observatory&) = delete;

  // 生命周期管理
  void Start();
  void Stop();
  void Wait();  // 阻塞等待停止
  bool IsRunning() const { return running_.load(); }
  bool IsConnected() const { return connected_.load(); }

  // 获取服务地址
  std::string GetHttpAddress() const;
  std::string GetControlPlaneAddress() const { return config_.control_plane_address; }

 private:
  // gRPC 客户端管理
  void ConnectLoop();
  bool TryConnect();
  
  // HTTP 请求处理
  void HttpServerLoop();
  std::string HandleRequest(const std::string& method,
                            const std::string& path,
                            const std::string& body);
  
  // API 处理器 (通过 gRPC 获取数据)
  std::string HandleApiMetrics();
  std::string HandleApiMetricsHistory();
  std::string HandleApiWorkers();
  std::string HandleApiWorkerDetail(const std::string& worker_id);
  std::string HandleApiTasks(const std::string& status_filter);
  std::string HandleApiTaskDetail(const std::string& task_id);
  std::string HandleApiClusterStatus();
  std::string HandleApiConnectionStatus();
  
  // 静态文件处理
  std::string HandleStaticFile(const std::string& path);
  std::string GetEmbeddedHtml();
  
  // 指标采集
  void MetricsCollectLoop();
  void CollectMetrics();
  
  // 辅助方法
  std::string JsonEscape(const std::string& s);
  std::string TaskStatusToJsonString(::control_plane::TaskStatus status);
  std::string WorkerStatusToJsonString(::control_plane::WorkerStatus status);
  int64_t GetCurrentTimeMs();
  
  // HTTP 响应构建
  std::string BuildHttpResponse(int status_code,
                                const std::string& content_type,
                                const std::string& body);
  std::string BuildJsonResponse(const std::string& json);
  std::string BuildErrorResponse(int status_code, const std::string& message);

  ObservatoryConfig config_;
  std::atomic<bool> running_{false};
  std::atomic<bool> connected_{false};
  std::atomic<bool> should_stop_{false};

  // gRPC 客户端
  std::shared_ptr<grpc::Channel> channel_;
  std::unique_ptr<::control_plane::ControlPlaneService::Stub> stub_;
  std::unique_ptr<std::thread> connect_thread_;
  mutable std::mutex grpc_mutex_;

  // HTTP 服务器
  int server_fd_ = -1;
  std::unique_ptr<std::thread> http_thread_;

  // 指标采集
  std::unique_ptr<std::thread> metrics_thread_;
  std::deque<MetricsDataPoint> metrics_history_;
  mutable std::mutex metrics_mutex_;

  // 用于计算吞吐量
  uint64_t last_completed_count_ = 0;
  uint64_t last_failed_count_ = 0;
  
  // 延迟直方图 (用于计算 p50/p95/p99)
  std::vector<uint64_t> recent_execution_times_;
  mutable std::mutex latency_mutex_;
};

}  // namespace control_plane
}  // namespace tendisplus
