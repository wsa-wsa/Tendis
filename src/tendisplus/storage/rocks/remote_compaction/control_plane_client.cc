// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include "control_plane_client.h"

#include <iostream>

#include "control_plane.grpc.pb.h"

namespace tendisplus {
namespace remote_compaction {

// 全局控制平面客户端实例
static std::unique_ptr<ControlPlaneClient> g_control_plane_client;
static std::mutex g_client_mutex;

// ============================================================================
// ControlPlaneClient Implementation
// ============================================================================

ControlPlaneClient::ControlPlaneClient(const ControlPlaneClientConfig& config)
  : config_(config) {}

ControlPlaneClient::~ControlPlaneClient() {
  Disconnect();
}

bool ControlPlaneClient::Connect() {
  std::lock_guard<std::mutex> lock(mutex_);

  if (connected_.load()) {
    return true;
  }

  if (config_.control_plane_address.empty()) {
    std::cerr << "[ControlPlaneClient] Control plane address not configured"
              << std::endl;
    return false;
  }

  channel_ = CreateChannel();
  if (!channel_) {
    std::cerr << "[ControlPlaneClient] Failed to create gRPC channel"
              << std::endl;
    return false;
  }

  // 检查连接状态
  auto state = channel_->GetState(true);
  if (state == GRPC_CHANNEL_SHUTDOWN) {
    std::cerr << "[ControlPlaneClient] gRPC channel is shutdown"
              << std::endl;
    return false;
  }
  auto deadline = std::chrono::system_clock::now() +
                  std::chrono::milliseconds(config_.connect_timeout_ms);

  if (channel_->WaitForConnected(deadline)) {
    connected_.store(true);
    std::cout << "[ControlPlaneClient] Connected to control plane: "
              << config_.control_plane_address << std::endl;
    return true;
  }

  std::cerr << "[ControlPlaneClient] Failed to connect to control plane: "
            << config_.control_plane_address << std::endl;
  return false;
}

bool ControlPlaneClient::IsConnected() const {
  return connected_.load();
}

void ControlPlaneClient::Disconnect() {
  std::lock_guard<std::mutex> lock(mutex_);
  connected_.store(false);
  channel_.reset();
}

bool ControlPlaneClient::EnsureConnected() {
  if (connected_.load()) {
    // 检查连接是否仍然有效
    if (channel_) {
      auto state = channel_->GetState(false);
      if (state == GRPC_CHANNEL_READY || state == GRPC_CHANNEL_IDLE) {
        return true;
      }
    }
    connected_.store(false);
  }
  return Connect();
}

std::shared_ptr<grpc::Channel> ControlPlaneClient::CreateChannel() {
  grpc::ChannelArguments channel_args;
  channel_args.SetMaxReceiveMessageSize(
    static_cast<int>(config_.grpc_max_message_size));
  channel_args.SetMaxSendMessageSize(
    static_cast<int>(config_.grpc_max_message_size));

  return grpc::CreateCustomChannel(config_.control_plane_address,
                                   grpc::InsecureChannelCredentials(),
                                   channel_args);
}

SubmitResult ControlPlaneClient::SubmitCompactionTask(
  const std::string& source_node_id,
  const std::string& db_name,
  uint32_t store_id,
  uint64_t job_id,
  const std::string& compaction_input,
  const std::string& shared_fs_uri,
  int32_t priority) {
  SubmitResult result;

  if (!EnsureConnected()) {
    result.error_message = "Not connected to control plane";
    return result;
  }

  auto stub = control_plane::ControlPlaneService::NewStub(channel_);

  control_plane::SubmitTaskRequest request;
  request.set_source_node_id(source_node_id);
  request.set_db_name(db_name);
  request.set_store_id(store_id);
  request.set_job_id(job_id);
  request.set_compaction_input(compaction_input);
  request.set_shared_fs_uri(shared_fs_uri);
  request.set_priority(
    static_cast<control_plane::TaskPriority>(priority));

  control_plane::SubmitTaskResponse response;
  grpc::ClientContext context;

  auto deadline = std::chrono::system_clock::now() +
                  std::chrono::milliseconds(config_.request_timeout_ms);
  context.set_deadline(deadline);

  grpc::Status status = stub->SubmitCompactionTask(&context, request, &response);

  if (!status.ok()) {
    result.error_message = "gRPC error: " + status.error_message();
    std::cerr << "[ControlPlaneClient] SubmitCompactionTask failed: "
              << result.error_message << std::endl;
    return result;
  }

  result.success = response.success();
  result.task_id = response.task_id();
  result.error_message = response.error_message();

  if (result.success) {
    std::cout << "[ControlPlaneClient] Task submitted: " << result.task_id
              << std::endl;
  }

  return result;
}

RemoteTaskStatus ControlPlaneClient::QueryTaskStatus(
  const std::string& task_id) {
  if (!EnsureConnected()) {
    return RemoteTaskStatus::kUnknown;
  }

  auto stub = control_plane::ControlPlaneService::NewStub(channel_);

  control_plane::QueryTaskRequest request;
  request.set_task_id(task_id);

  control_plane::QueryTaskResponse response;
  grpc::ClientContext context;

  auto deadline = std::chrono::system_clock::now() +
                  std::chrono::milliseconds(config_.request_timeout_ms);
  context.set_deadline(deadline);

  grpc::Status status = stub->QueryTaskStatus(&context, request, &response);

  if (!status.ok() || !response.found()) {
    return RemoteTaskStatus::kUnknown;
  }

  return static_cast<RemoteTaskStatus>(response.task_info().status());
}

bool ControlPlaneClient::CancelTask(const std::string& task_id,
                                    const std::string& reason) {
  if (!EnsureConnected()) {
    return false;
  }

  auto stub = control_plane::ControlPlaneService::NewStub(channel_);

  control_plane::CancelTaskRequest request;
  request.set_task_id(task_id);
  request.set_reason(reason);

  control_plane::CancelTaskResponse response;
  grpc::ClientContext context;

  auto deadline = std::chrono::system_clock::now() +
                  std::chrono::milliseconds(config_.request_timeout_ms);
  context.set_deadline(deadline);

  grpc::Status status = stub->CancelTask(&context, request, &response);

  return status.ok() && response.success();
}

TaskResultInfo ControlPlaneClient::WaitForTaskResult(const std::string& task_id,
                                                     uint32_t timeout_ms) {
  TaskResultInfo result;

  if (!EnsureConnected()) {
    result.error_message = "Not connected to control plane";
    return result;
  }

  auto stub = control_plane::ControlPlaneService::NewStub(channel_);

  control_plane::WaitResultRequest request;
  request.set_task_id(task_id);
  request.set_timeout_ms(timeout_ms > 0 ? timeout_ms
                                        : config_.wait_result_timeout_ms);

  control_plane::WaitResultResponse response;
  grpc::ClientContext context;

  // 等待时间较长，使用更大的超时
  auto deadline = std::chrono::system_clock::now() +
                  std::chrono::milliseconds(request.timeout_ms() + 5000);
  context.set_deadline(deadline);

  grpc::Status status = stub->WaitForTaskResult(&context, request, &response);

  if (!status.ok()) {
    result.error_message = "gRPC error: " + status.error_message();
    return result;
  }

  result.completed = response.completed();
  result.status = static_cast<RemoteTaskStatus>(response.status());
  result.compaction_result = response.compaction_result();
  result.error_message = response.error_message();

  return result;
}

ControlPlaneClient::ClusterStatus ControlPlaneClient::GetClusterStatus() {
  ClusterStatus status;

  if (!EnsureConnected()) {
    return status;
  }

  auto stub = control_plane::ControlPlaneService::NewStub(channel_);

  control_plane::ClusterStatusRequest request;
  control_plane::ClusterStatusResponse response;
  grpc::ClientContext context;

  auto deadline = std::chrono::system_clock::now() +
                  std::chrono::milliseconds(config_.request_timeout_ms);
  context.set_deadline(deadline);

  grpc::Status grpc_status =
    stub->GetClusterStatus(&context, request, &response);

  if (grpc_status.ok()) {
    status.total_workers = response.total_workers();
    status.online_workers = response.online_workers();
    status.pending_tasks = response.pending_tasks();
    status.running_tasks = response.running_tasks();
  }

  return status;
}

// ============================================================================
// 全局客户端管理
// ============================================================================

void InitControlPlaneClient(const ControlPlaneClientConfig& config) {
  std::lock_guard<std::mutex> lock(g_client_mutex);

  if (g_control_plane_client) {
    g_control_plane_client->Disconnect();
  }

  g_control_plane_client = std::make_unique<ControlPlaneClient>(config);

  if (!config.control_plane_address.empty()) {
    g_control_plane_client->Connect();
  }
}

ControlPlaneClient* GetControlPlaneClient() {
  std::lock_guard<std::mutex> lock(g_client_mutex);
  return g_control_plane_client.get();
}

void ShutdownControlPlaneClient() {
  std::lock_guard<std::mutex> lock(g_client_mutex);
  if (g_control_plane_client) {
    g_control_plane_client->Disconnect();
    g_control_plane_client.reset();
  }
}

bool IsControlPlaneEnabled() {
  std::lock_guard<std::mutex> lock(g_client_mutex);
  return g_control_plane_client && g_control_plane_client->IsConnected();
}

}  // namespace remote_compaction
}  // namespace tendisplus
