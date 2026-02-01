// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

// Control Plane Client - Tendisplus 端使用的控制平面客户端
// Based on CaaS-LSM architecture

#pragma once

#include <grpc/grpc.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <string>

namespace tendisplus {
namespace remote_compaction {

// ============================================================================
// 控制平面客户端配置
// ============================================================================
struct ControlPlaneClientConfig {
  std::string control_plane_address;          // 控制平面地址
  int64_t grpc_max_message_size = 64 * 1024 * 1024;  // 64MB
  int32_t connect_timeout_ms = 5000;          // 连接超时
  int32_t request_timeout_ms = 30000;         // 请求超时
  int32_t wait_result_timeout_ms = 60000;     // 等待结果超时
  bool enable_retry = true;                   // 是否启用重试
  int32_t max_retries = 3;                    // 最大重试次数
};

// ============================================================================
// 任务状态（简化版，用于客户端）
// ============================================================================
enum class RemoteTaskStatus {
  kUnknown = 0,
  kPending = 1,
  kAssigned = 2,
  kRunning = 3,
  kCompleted = 4,
  kFailed = 5,
  kCancelled = 6,
  kTimeout = 7
};

// ============================================================================
// 提交任务结果
// ============================================================================
struct SubmitResult {
  bool success = false;
  std::string task_id;
  std::string error_message;
};

// ============================================================================
// 任务结果
// ============================================================================
struct TaskResultInfo {
  bool completed = false;
  RemoteTaskStatus status = RemoteTaskStatus::kUnknown;
  std::string compaction_result;
  std::string error_message;
};

// ============================================================================
// Control Plane Client - 控制平面客户端
// ============================================================================
class ControlPlaneClient {
 public:
  explicit ControlPlaneClient(const ControlPlaneClientConfig& config);
  ~ControlPlaneClient();

  // 禁止拷贝
  ControlPlaneClient(const ControlPlaneClient&) = delete;
  ControlPlaneClient& operator=(const ControlPlaneClient&) = delete;

  // 连接管理
  bool Connect();
  bool IsConnected() const;
  void Disconnect();

  // =========================================================================
  // 任务管理 API
  // =========================================================================

  // 提交 Compaction 任务
  // source_node_id: 本节点 ID（通常是 IP:Port）
  // db_name: 数据库路径
  // store_id: Store ID
  // job_id: RocksDB job ID
  // compaction_input: 序列化的 compaction 输入
  // shared_fs_uri: 共享文件系统 URI
  // priority: 任务优先级 (0-3, 0=低, 3=紧急)
  SubmitResult SubmitCompactionTask(
    const std::string& source_node_id,
    const std::string& db_name,
    uint32_t store_id,
    uint64_t job_id,
    const std::string& compaction_input,
    const std::string& shared_fs_uri,
    int32_t priority = 1);

  // 查询任务状态
  RemoteTaskStatus QueryTaskStatus(const std::string& task_id);

  // 取消任务
  bool CancelTask(const std::string& task_id, const std::string& reason = "");

  // 等待任务结果
  // timeout_ms: 等待超时时间，0 表示立即返回当前状态
  TaskResultInfo WaitForTaskResult(const std::string& task_id,
                                   uint32_t timeout_ms = 0);

  // =========================================================================
  // 监控 API
  // =========================================================================

  struct ClusterStatus {
    uint32_t total_workers = 0;
    uint32_t online_workers = 0;
    uint32_t pending_tasks = 0;
    uint32_t running_tasks = 0;
  };

  ClusterStatus GetClusterStatus();

 private:
  bool EnsureConnected();
  std::shared_ptr<grpc::Channel> CreateChannel();

  ControlPlaneClientConfig config_;
  std::shared_ptr<grpc::Channel> channel_;
  std::atomic<bool> connected_{false};
  mutable std::mutex mutex_;
};

// ============================================================================
// 全局控制平面客户端管理
// ============================================================================

// 初始化全局控制平面客户端
void InitControlPlaneClient(const ControlPlaneClientConfig& config);

// 获取全局控制平面客户端
ControlPlaneClient* GetControlPlaneClient();

// 关闭全局控制平面客户端
void ShutdownControlPlaneClient();

// 检查控制平面是否已启用
bool IsControlPlaneEnabled();

}  // namespace remote_compaction
}  // namespace tendisplus
