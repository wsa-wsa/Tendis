// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include "control_plane.h"

#include <grpc/grpc.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>

#include <iostream>
#include <sstream>

#include "control_plane.grpc.pb.h"

namespace tendisplus {
namespace control_plane {

// ============================================================================
// gRPC Service Implementation
// ============================================================================
class ControlPlaneServiceImpl final
  : public ::control_plane::ControlPlaneService::Service {
 public:
  explicit ControlPlaneServiceImpl(ControlPlane& control_plane)
    : control_plane_(control_plane) {}

  // =========================================================================
  // Tendisplus Node API
  // =========================================================================

  grpc::Status SubmitCompactionTask(
    grpc::ServerContext* context,
    const ::control_plane::SubmitTaskRequest* request,
    ::control_plane::SubmitTaskResponse* response) override {
    auto priority = static_cast<TaskPriority>(request->priority());

    std::string task_id = control_plane_.SubmitCompactionTask(
      request->source_node_id(),
      request->db_name(),
      request->store_id(),
      request->job_id(),
      request->compaction_input(),
      request->shared_fs_uri(),
      priority,
      request->timeout_sec());

    if (task_id.empty()) {
      response->set_success(false);
      response->set_error_message("Failed to submit task");
    } else {
      response->set_success(true);
      response->set_task_id(task_id);
    }

    return grpc::Status::OK;
  }

  grpc::Status QueryTaskStatus(
    grpc::ServerContext* context,
    const ::control_plane::QueryTaskRequest* request,
    ::control_plane::QueryTaskResponse* response) override {
    auto task = control_plane_.QueryTask(request->task_id());

    if (task) {
      response->set_found(true);
      auto* info = response->mutable_task_info();
      // 问题5 修复: 加 per-task mutex 防止读取不一致的中间状态
      std::lock_guard<std::mutex> task_lock(task->mtx);
      info->set_task_id(task->task_id);
      info->set_source_node_id(task->source_node_id);
      info->set_db_name(task->db_name);
      info->set_store_id(task->store_id);
      info->set_priority(
        static_cast<::control_plane::TaskPriority>(task->priority));
      info->set_status(static_cast<::control_plane::TaskStatus>(task->status));
      info->set_assigned_worker_id(task->assigned_worker_id);
      info->set_retry_count(task->retry_count);
      info->set_error_message(task->error_message);
    } else {
      response->set_found(false);
    }

    return grpc::Status::OK;
  }

  grpc::Status CancelTask(
    grpc::ServerContext* context,
    const ::control_plane::CancelTaskRequest* request,
    ::control_plane::CancelTaskResponse* response) override {
    bool success =
      control_plane_.CancelTask(request->task_id(), request->reason());
    response->set_success(success);
    if (!success) {
      response->set_error_message("Task not found or already completed");
    }
    return grpc::Status::OK;
  }

  grpc::Status WaitForTaskResult(
    grpc::ServerContext* context,
    const ::control_plane::WaitResultRequest* request,
    ::control_plane::WaitResultResponse* response) override {
    TaskResult result;
    bool completed = control_plane_.WaitForTaskResult(
      request->task_id(), &result, request->timeout_ms());

    response->set_completed(completed);
    if (completed) {
      auto task = control_plane_.QueryTask(request->task_id());
      if (task) {
        // 第五轮修复 (NEW-8): 加 per-task mutex 读取 task->status
        std::lock_guard<std::mutex> task_lock(task->mtx);
        response->set_status(
          static_cast<::control_plane::TaskStatus>(task->status));
        response->set_compaction_result(result.compaction_result);
        response->set_error_message(result.error_message);
      }
    }

    return grpc::Status::OK;
  }

  // =========================================================================
  // CSA Worker API
  // =========================================================================

  grpc::Status RegisterWorker(
    grpc::ServerContext* context,
    const ::control_plane::RegisterWorkerRequest* request,
    ::control_plane::RegisterWorkerResponse* response) override {
    WorkerResources resources;
    resources.total_cpu_cores = request->resources().total_cpu_cores();
    resources.total_memory_mb = request->resources().total_memory_mb();
    resources.total_disk_mb = request->resources().total_disk_mb();
    resources.max_concurrent_tasks =
      request->resources().max_concurrent_tasks();
    resources.active_tasks = request->resources().active_tasks();

    std::map<std::string, std::string> labels(request->labels().begin(),
                                              request->labels().end());

    std::string worker_id = control_plane_.RegisterWorker(
      request->address(), resources, labels, request->worker_id());

    if (worker_id.empty()) {
      response->set_success(false);
      response->set_error_message("Failed to register worker");
    } else {
      response->set_success(true);
      response->set_worker_id(worker_id);
      response->set_heartbeat_interval_sec(10);
    }

    return grpc::Status::OK;
  }

  grpc::Status WorkerHeartbeat(
    grpc::ServerContext* context,
    const ::control_plane::HeartbeatRequest* request,
    ::control_plane::HeartbeatResponse* response) override {
    WorkerResources resources;
    resources.total_cpu_cores = request->resources().total_cpu_cores();
    resources.total_memory_mb = request->resources().total_memory_mb();
    resources.total_disk_mb = request->resources().total_disk_mb();
    resources.max_concurrent_tasks =
      request->resources().max_concurrent_tasks();
    resources.active_tasks = request->resources().active_tasks();

    std::vector<std::string> active_tasks(request->active_task_ids().begin(),
                                          request->active_task_ids().end());

    auto tasks_to_cancel = control_plane_.ProcessWorkerHeartbeat(
      request->worker_id(), resources, active_tasks);

    response->set_success(true);
    for (const auto& task_id : tasks_to_cancel) {
      response->add_tasks_to_cancel(task_id);
    }

    return grpc::Status::OK;
  }

  grpc::Status UnregisterWorker(
    grpc::ServerContext* context,
    const ::control_plane::UnregisterWorkerRequest* request,
    ::control_plane::UnregisterWorkerResponse* response) override {
    bool success = control_plane_.UnregisterWorker(request->worker_id(),
                                                   request->reason());
    response->set_success(success);
    return grpc::Status::OK;
  }

  grpc::Status ReportTaskResult(
    grpc::ServerContext* context,
    const ::control_plane::TaskResultRequest* request,
    ::control_plane::TaskResultResponse* response) override {
    TaskResult result;
    result.success = request->success();
    result.compaction_result = request->compaction_result();
    result.error_message = request->error_message();
    result.execution_time_ms = request->execution_time_ms();
    result.bytes_read = request->bytes_read();
    result.bytes_written = request->bytes_written();

    control_plane_.ReportTaskResult(
      request->worker_id(), request->task_id(), result);

    response->set_success(true);
    return grpc::Status::OK;
  }

  grpc::Status FetchTask(
    grpc::ServerContext* context,
    const ::control_plane::FetchTaskRequest* request,
    ::control_plane::FetchTaskResponse* response) override {
    auto tasks =
      control_plane_.FetchTasks(request->worker_id(), request->max_tasks());

    for (const auto& task : tasks) {
      auto* proto_task = response->add_tasks();
      // 第五轮修复 (NEW-10): 加 per-task mutex 读取 task 字段
      std::lock_guard<std::mutex> task_lock(task->mtx);
      proto_task->set_task_id(task->task_id);
      proto_task->set_db_name(task->db_name);
      proto_task->set_store_id(task->store_id);
      proto_task->set_job_id(task->params.job_id);
      proto_task->set_compaction_input(task->params.compaction_input);
      proto_task->set_shared_fs_uri(task->params.shared_fs_uri);
      proto_task->set_shared_fs_local_prefix(task->params.shared_fs_local_prefix);
      proto_task->set_timeout_sec(task->params.timeout_sec);
    }

    return grpc::Status::OK;
  }

  // =========================================================================
  // Admin API
  // =========================================================================

  grpc::Status GetClusterStatus(
    grpc::ServerContext* context,
    const ::control_plane::ClusterStatusRequest* request,
    ::control_plane::ClusterStatusResponse* response) override {
    auto status = control_plane_.GetClusterStatus();
    response->set_total_workers(status.total_workers);
    response->set_online_workers(status.online_workers);
    response->set_pending_tasks(status.pending_tasks);
    response->set_running_tasks(status.running_tasks);
    response->set_total_completed(status.total_completed);
    response->set_total_failed(status.total_failed);
    return grpc::Status::OK;
  }

  grpc::Status ListWorkers(
    grpc::ServerContext* context,
    const ::control_plane::ListWorkersRequest* request,
    ::control_plane::ListWorkersResponse* response) override {
    auto workers = control_plane_.GetWorkers();

    for (const auto& worker : workers) {
      auto* proto_worker = response->add_workers();
      proto_worker->set_worker_id(worker->worker_id);
      proto_worker->set_address(worker->address);
      proto_worker->set_status(
        static_cast<::control_plane::WorkerStatus>(worker->status));

      auto* resources = proto_worker->mutable_resources();
      resources->set_total_cpu_cores(worker->resources.total_cpu_cores);
      resources->set_total_memory_mb(worker->resources.total_memory_mb);
      resources->set_total_disk_mb(worker->resources.total_disk_mb);
      resources->set_max_concurrent_tasks(
        worker->resources.max_concurrent_tasks);
      resources->set_active_tasks(worker->resources.active_tasks);

      proto_worker->set_total_completed(worker->total_completed);
      proto_worker->set_total_failed(worker->total_failed);
    }

    return grpc::Status::OK;
  }

  grpc::Status GetTaskStatistics(
    grpc::ServerContext* context,
    const ::control_plane::TaskStatisticsRequest* request,
    ::control_plane::TaskStatisticsResponse* response) override {
    const auto& stats = control_plane_.GetTaskStatistics();
    response->set_total_submitted(stats.total_submitted.load());
    response->set_total_completed(stats.total_completed.load());
    response->set_total_failed(stats.total_failed.load());
    response->set_total_cancelled(stats.total_cancelled.load());
    response->set_total_timeout(stats.total_timeout.load());
    response->set_avg_execution_time_ms(
      static_cast<uint64_t>(stats.GetAvgExecutionTimeMs()));
    response->set_avg_queue_time_ms(
      static_cast<uint64_t>(stats.GetAvgQueueTimeMs()));
    return grpc::Status::OK;
  }

  // =========================================================================
  // Observatory API - 观测平面接口
  // =========================================================================

  grpc::Status ListTasks(
    grpc::ServerContext* context,
    const ::control_plane::ListTasksRequest* request,
    ::control_plane::ListTasksResponse* response) override {
    TaskFilter filter;
    filter.limit = request->limit() > 0 ? request->limit() : 100;
    
    if (request->status_filter() != ::control_plane::TASK_UNKNOWN) {
      filter.status = static_cast<TaskStatus>(request->status_filter());
    }
    if (!request->source_node_filter().empty()) {
      filter.source_node_id = request->source_node_filter();
    }
    
    auto tasks = control_plane_.GetScheduler().QueryTasks(filter);
    
    for (const auto& task : tasks) {
      auto* proto_task = response->add_tasks();
      // 问题5 修复: 加 per-task mutex 防止读取不一致的中间状态
      std::lock_guard<std::mutex> task_lock(task->mtx);
      proto_task->set_task_id(task->task_id);
      proto_task->set_source_node_id(task->source_node_id);
      proto_task->set_db_name(task->db_name);
      proto_task->set_store_id(task->store_id);
      proto_task->set_priority(
        static_cast<::control_plane::TaskPriority>(task->priority));
      proto_task->set_status(
        static_cast<::control_plane::TaskStatus>(task->status));
      proto_task->set_assigned_worker_id(task->assigned_worker_id);
      proto_task->set_retry_count(task->retry_count);
      proto_task->set_error_message(task->error_message);
      
      // 时间戳
      proto_task->set_submit_time_ms(
        std::chrono::duration_cast<std::chrono::milliseconds>(
          task->submit_time.time_since_epoch()).count());
      proto_task->set_start_time_ms(
        std::chrono::duration_cast<std::chrono::milliseconds>(
          task->start_time.time_since_epoch()).count());
      proto_task->set_complete_time_ms(
        std::chrono::duration_cast<std::chrono::milliseconds>(
          task->complete_time.time_since_epoch()).count());
    }
    
    return grpc::Status::OK;
  }

  grpc::Status GetMetricsSnapshot(
    grpc::ServerContext* context,
    const ::control_plane::MetricsSnapshotRequest* request,
    ::control_plane::MetricsSnapshotResponse* response) override {
    auto cluster_status = control_plane_.GetClusterStatus();
    const auto& stats = control_plane_.GetTaskStatistics();
    auto workers = control_plane_.GetWorkers();
    
    // 集群概览
    response->set_total_workers(cluster_status.total_workers);
    response->set_online_workers(cluster_status.online_workers);
    response->set_pending_tasks(cluster_status.pending_tasks);
    response->set_running_tasks(cluster_status.running_tasks);
    
    // 计算 busy workers
    uint32_t busy_count = 0;
    double total_memory = 0, used_memory = 0;
    double total_disk = 0, used_disk = 0;
    
    for (const auto& w : workers) {
      if (w->status == WorkerStatus::kBusy) {
        busy_count++;
      }
      if (w->status == WorkerStatus::kOnline || w->status == WorkerStatus::kBusy) {
        total_memory += w->resources.total_memory_mb;
        used_memory += w->resources.used_memory_mb;
        total_disk += w->resources.total_disk_mb;
        used_disk += w->resources.used_disk_mb;
      }
    }
    response->set_busy_workers(busy_count);
    
    // 吞吐量统计
    response->set_total_completed(stats.total_completed.load());
    response->set_total_failed(stats.total_failed.load());
    // 使用 MetricsCollector 的真实滑动窗口统计
    auto& mc = control_plane_.GetMetricsCollector();
    response->set_completed_last_minute(mc.GetCompletedLastMinute());
    response->set_failed_last_minute(mc.GetFailedLastMinute());
    
    // 延迟统计
    response->set_avg_queue_time_ms(
      static_cast<uint64_t>(stats.GetAvgQueueTimeMs()));
    response->set_avg_execution_time_ms(
      static_cast<uint64_t>(stats.GetAvgExecutionTimeMs()));
    // 使用 MetricsCollector 的真实百分位统计
    auto exec_latency = mc.GetExecutionLatencyPercentiles();
    response->set_p50_execution_time_ms(exec_latency.p50_ms);
    response->set_p95_execution_time_ms(exec_latency.p95_ms);
    response->set_p99_execution_time_ms(exec_latency.p99_ms);
    
    // 资源使用
    response->set_cluster_cpu_usage(0.0);  // CPU 使用率需要额外采集
    response->set_cluster_memory_usage(
      total_memory > 0 ? used_memory / total_memory : 0.0);
    response->set_cluster_disk_usage(
      total_disk > 0 ? used_disk / total_disk : 0.0);
    
    // 时间戳
    response->set_timestamp_ms(
      std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());
    
    return grpc::Status::OK;
  }

  grpc::Status GetWorkerDetail(
    grpc::ServerContext* context,
    const ::control_plane::WorkerDetailRequest* request,
    ::control_plane::WorkerDetailResponse* response) override {
    auto worker = control_plane_.GetWorkerManager().GetWorker(request->worker_id());
    
    if (!worker) {
      response->set_found(false);
      return grpc::Status::OK;
    }
    
    response->set_found(true);
    
    auto* proto_worker = response->mutable_worker();
    proto_worker->set_worker_id(worker->worker_id);
    proto_worker->set_address(worker->address);
    proto_worker->set_status(
      static_cast<::control_plane::WorkerStatus>(worker->status));
    
    auto* resources = proto_worker->mutable_resources();
    resources->set_total_cpu_cores(worker->resources.total_cpu_cores);
    resources->set_total_memory_mb(worker->resources.total_memory_mb);
    resources->set_total_disk_mb(worker->resources.total_disk_mb);
    resources->set_used_memory_mb(worker->resources.used_memory_mb);
    resources->set_used_disk_mb(worker->resources.used_disk_mb);
    resources->set_max_concurrent_tasks(worker->resources.max_concurrent_tasks);
    resources->set_active_tasks(worker->resources.active_tasks);
    
    proto_worker->set_total_completed(worker->total_completed);
    proto_worker->set_total_failed(worker->total_failed);
    
    // 获取该 Worker 的活跃任务
    // 第五轮修复 (NEW-9): 加 per-task mutex 读取 task 字段
    for (const auto& task_id : worker->active_task_ids) {
      auto task = control_plane_.QueryTask(task_id);
      if (task) {
        auto* proto_task = response->add_active_tasks();
        std::lock_guard<std::mutex> task_lock(task->mtx);
        proto_task->set_task_id(task->task_id);
        proto_task->set_status(
          static_cast<::control_plane::TaskStatus>(task->status));
        proto_task->set_db_name(task->db_name);
        proto_task->set_store_id(task->store_id);
      }
    }
    
    // Worker 历史统计 (简化实现)
    response->set_tasks_completed_last_hour(worker->total_completed);
    response->set_tasks_failed_last_hour(worker->total_failed);
    response->set_avg_execution_time_ms(0);  // 需要额外统计
    
    return grpc::Status::OK;
  }

  // =========================================================================
  // Bulk Load API - 批量加载接口
  // =========================================================================

  grpc::Status SubmitBulkLoadTask(
    grpc::ServerContext* context,
    const ::control_plane::SubmitBulkLoadRequest* request,
    ::control_plane::SubmitBulkLoadResponse* response) override {
    // 转换 Proto 参数到内部类型
    BulkLoadTaskParams params;
    if (request->has_params()) {
      const auto& p = request->params();
      params.source_type = static_cast<DataSourceType>(p.source_type());
      params.source_path = p.source_path();
      params.data_format = static_cast<DataFormat>(p.data_format());
      params.sharding_strategy = static_cast<ShardingStrategy>(p.sharding_strategy());
      params.shard_count = p.shard_count();
      params.target_store_id = p.target_store_id();
      params.target_db_path = p.target_db_path();
      params.shared_fs_uri = p.shared_fs_uri();
      params.sst_output_dir = p.sst_output_dir();
      params.compression = static_cast<CompressionType>(p.compression());
      params.target_sst_size = p.target_sst_size();
      params.generate_binlog = p.generate_binlog();
      params.verify_checksum = p.verify_checksum();
      params.timeout_sec = p.timeout_sec();
      params.rate_limit_bytes_per_sec = p.rate_limit_bytes_per_sec();
      params.max_concurrent_ingests = p.max_concurrent_ingests();

      // 转换 key ranges
      for (const auto& kr : p.key_ranges()) {
        KeyRange range;
        range.start_key = kr.start_key();
        range.end_key = kr.end_key();
        range.slot_start = kr.slot_start();
        range.slot_end = kr.slot_end();
        params.key_ranges.push_back(range);
      }
    }

    auto priority = static_cast<TaskPriority>(request->priority());
    std::string task_id = control_plane_.SubmitBulkLoadTask(
      request->source_node_id(),
      request->db_name(),
      params,
      priority);

    if (task_id.empty()) {
      response->set_success(false);
      response->set_error_message("Failed to submit Bulk Load task");
    } else {
      response->set_success(true);
      response->set_task_id(task_id);
    }

    return grpc::Status::OK;
  }

  grpc::Status QueryBulkLoadStatus(
    grpc::ServerContext* context,
    const ::control_plane::QueryBulkLoadRequest* request,
    ::control_plane::QueryBulkLoadResponse* response) override {
    auto task = control_plane_.QueryBulkLoadStatus(request->task_id());

    if (!task) {
      response->set_found(false);
      return grpc::Status::OK;
    }

    response->set_found(true);
    // 问题5 修复: 加 per-task mutex 防止读取不一致的中间状态
    std::lock_guard<std::mutex> task_lock(task->mtx);
    response->set_task_id(task->task_id);
    response->set_overall_status(
      static_cast<::control_plane::TaskStatus>(task->status));
    response->set_phase(
      static_cast<::control_plane::BulkLoadPhase>(task->bulk_load_params.phase));

    // 分片统计
    const auto& bl = task->bulk_load_params;
    response->set_total_shards(bl.shards.size());
    response->set_completed_shards(bl.completed_shards);
    response->set_failed_shards(bl.failed_shards);

    uint32_t running_shards = 0;
    for (const auto& shard : bl.shards) {
      auto* proto_shard = response->add_shards();
      proto_shard->set_shard_id(shard.shard_id);
      proto_shard->set_shard_index(shard.shard_index);
      proto_shard->set_status(
        static_cast<::control_plane::TaskStatus>(shard.status));
      proto_shard->set_assigned_worker_id(shard.assigned_worker_id);
      proto_shard->set_estimated_size(shard.estimated_size);
      proto_shard->set_error_message(shard.error_message);

      if (shard.status == TaskStatus::kRunning) {
        running_shards++;
      }

      // SST 文件信息
      for (const auto& sst : shard.generated_sst_files) {
        auto* proto_sst = proto_shard->add_generated_sst_files();
        proto_sst->set_file_path(sst.file_path);
        proto_sst->set_column_family(sst.column_family);
        proto_sst->set_file_size(sst.file_size);
        proto_sst->set_num_entries(sst.num_entries);
        proto_sst->set_checksum(sst.checksum);
      }
    }
    response->set_running_shards(running_shards);

    // 进度百分比
    double total = bl.shards.size();
    double done = bl.completed_shards;
    response->set_progress_percent(total > 0 ? (done / total) * 100.0 : 0.0);

    return grpc::Status::OK;
  }

  grpc::Status CancelBulkLoad(
    grpc::ServerContext* context,
    const ::control_plane::CancelBulkLoadRequest* request,
    ::control_plane::CancelBulkLoadResponse* response) override {
    bool success = control_plane_.CancelBulkLoad(
      request->task_id(), request->reason());
    response->set_success(success);
    if (!success) {
      response->set_error_message("Bulk Load task not found or already completed");
    }
    return grpc::Status::OK;
  }

  grpc::Status ReportIngestResult(
    grpc::ServerContext* context,
    const ::control_plane::ReportIngestResultRequest* request,
    ::control_plane::ReportIngestResultResponse* response) override {
    control_plane_.ReportIngestResult(
      request->task_id(),
      request->source_node_id(),
      request->success(),
      request->ingested_sst_count(),
      request->ingested_bytes(),
      request->ingested_rows(),
      request->error_message());

    response->set_success(true);
    return grpc::Status::OK;
  }

  // =========================================================================
  // Observatory Enhanced API - 增强观测接口
  // =========================================================================

  grpc::Status GetAlerts(
    grpc::ServerContext* context,
    const ::control_plane::GetAlertsRequest* request,
    ::control_plane::GetAlertsResponse* response) override {
    auto& am = control_plane_.GetAlertManager();

    // 活跃告警
    auto active = am.GetActiveAlerts();
    for (const auto& evt : active) {
      auto* proto_alert = response->add_active_alerts();
      proto_alert->set_event_id(evt.event_id);
      proto_alert->set_rule_id(evt.rule_id);
      proto_alert->set_rule_name(evt.rule_name);
      proto_alert->set_severity(
        static_cast<::control_plane::AlertSeverity>(evt.severity));
      proto_alert->set_state(
        static_cast<::control_plane::AlertState>(evt.state));
      proto_alert->set_metric(AlertMetricToString(evt.metric));
      proto_alert->set_current_value(evt.current_value);
      proto_alert->set_threshold(evt.threshold);
      proto_alert->set_operator_str(AlertOperatorToString(evt.op));
      proto_alert->set_timestamp_ms(evt.timestamp_ms);
      proto_alert->set_message(evt.message);
    }

    // 告警历史
    if (!request->active_only()) {
      uint32_t limit = request->history_limit() > 0 ?
        request->history_limit() : 50;
      auto history = am.GetAlertHistory(limit);
      for (const auto& evt : history) {
        auto* proto_alert = response->add_alert_history();
        proto_alert->set_event_id(evt.event_id);
        proto_alert->set_rule_id(evt.rule_id);
        proto_alert->set_rule_name(evt.rule_name);
        proto_alert->set_severity(
          static_cast<::control_plane::AlertSeverity>(evt.severity));
        proto_alert->set_state(
          static_cast<::control_plane::AlertState>(evt.state));
        proto_alert->set_metric(AlertMetricToString(evt.metric));
        proto_alert->set_current_value(evt.current_value);
        proto_alert->set_threshold(evt.threshold);
        proto_alert->set_operator_str(AlertOperatorToString(evt.op));
        proto_alert->set_timestamp_ms(evt.timestamp_ms);
        proto_alert->set_message(evt.message);
      }
    }

    response->set_total_active(am.GetActiveAlertCount());
    response->set_info_count(am.GetAlertCountBySeverity(AlertSeverity::kInfo));
    response->set_warning_count(
      am.GetAlertCountBySeverity(AlertSeverity::kWarning));
    response->set_critical_count(
      am.GetAlertCountBySeverity(AlertSeverity::kCritical));

    return grpc::Status::OK;
  }

  grpc::Status GetAlertRules(
    grpc::ServerContext* context,
    const ::control_plane::GetAlertRulesRequest* request,
    ::control_plane::GetAlertRulesResponse* response) override {
    auto& am = control_plane_.GetAlertManager();
    auto rules = am.GetAllRules();

    for (const auto& rule : rules) {
      auto* proto_rule = response->add_rules();
      proto_rule->set_rule_id(rule.rule_id);
      proto_rule->set_name(rule.name);
      proto_rule->set_description(rule.description);
      proto_rule->set_metric(AlertMetricToString(rule.metric));
      proto_rule->set_operator_str(AlertOperatorToString(rule.op));
      proto_rule->set_threshold(rule.threshold);
      proto_rule->set_severity(
        static_cast<::control_plane::AlertSeverity>(rule.severity));
      proto_rule->set_duration_sec(rule.duration_sec);
      proto_rule->set_enabled(rule.enabled);
      proto_rule->set_state(
        static_cast<::control_plane::AlertState>(rule.state));
      proto_rule->set_last_value(rule.last_value);
    }

    return grpc::Status::OK;
  }

  grpc::Status GetTaskTrace(
    grpc::ServerContext* context,
    const ::control_plane::GetTaskTraceRequest* request,
    ::control_plane::GetTaskTraceResponse* response) override {
    auto& tracer = control_plane_.GetTaskTracer();
    auto trace = tracer.GetTrace(request->task_id());

    if (!trace) {
      response->set_found(false);
      return grpc::Status::OK;
    }

    response->set_found(true);
    FillTaskTraceProto(response->mutable_trace(), *trace);

    return grpc::Status::OK;
  }

  grpc::Status GetRecentTraces(
    grpc::ServerContext* context,
    const ::control_plane::GetRecentTracesRequest* request,
    ::control_plane::GetRecentTracesResponse* response) override {
    auto& tracer = control_plane_.GetTaskTracer();
    uint32_t limit = request->limit() > 0 ? request->limit() : 50;

    std::vector<std::shared_ptr<TaskTrace>> traces;
    if (request->slow_only()) {
      uint64_t threshold = request->slow_threshold_ms() > 0 ?
        request->slow_threshold_ms() : 10000;
      traces = tracer.GetSlowTraces(threshold, limit);
    } else if (request->failed_only()) {
      traces = tracer.GetFailedTraces(limit);
    } else {
      traces = tracer.GetRecentTraces(limit);
    }

    for (const auto& trace : traces) {
      FillTaskTraceProto(response->add_traces(), *trace);
    }

    response->set_total_traces(tracer.GetTraceCount());
    response->set_active_traces(tracer.GetActiveTraceCount());

    return grpc::Status::OK;
  }

  grpc::Status GetPrometheusMetrics(
    grpc::ServerContext* context,
    const ::control_plane::PrometheusMetricsRequest* request,
    ::control_plane::PrometheusMetricsResponse* response) override {
    response->set_metrics_text(control_plane_.GeneratePrometheusMetrics());
    return grpc::Status::OK;
  }

 private:
  void FillTaskTraceProto(::control_plane::TaskTraceProto* proto,
                          const TaskTrace& trace) {
    proto->set_trace_id(trace.trace_id);
    proto->set_task_type(trace.task_type);
    proto->set_source_node_id(trace.source_node_id);
    proto->set_start_time_ms(trace.start_time_ms);
    proto->set_end_time_ms(trace.end_time_ms);
    proto->set_total_duration_ms(trace.total_duration_ms);
    proto->set_is_complete(trace.is_complete);
    proto->set_final_status(SpanStatusToString(trace.final_status));
    proto->set_queue_duration_ms(trace.queue_duration_ms);
    proto->set_schedule_duration_ms(trace.schedule_duration_ms);
    proto->set_execute_duration_ms(trace.execute_duration_ms);
    proto->set_transfer_duration_ms(trace.transfer_duration_ms);

    for (const auto& span : trace.spans) {
      auto* proto_span = proto->add_spans();
      proto_span->set_span_id(span.span_id);
      proto_span->set_trace_id(span.trace_id);
      proto_span->set_parent_span_id(span.parent_span_id);
      proto_span->set_operation(span.operation);
      proto_span->set_component(span.component);
      proto_span->set_worker_id(span.worker_id);
      proto_span->set_start_time_ms(span.start_time_ms);
      proto_span->set_end_time_ms(span.end_time_ms);
      proto_span->set_duration_ms(span.duration_ms);
      proto_span->set_status(SpanStatusToString(span.status));
      proto_span->set_error_message(span.error_message);
      for (const auto& [k, v] : span.tags) {
        (*proto_span->mutable_tags())[k] = v;
      }
    }
  }

  ControlPlane& control_plane_;
};

// ============================================================================
// gRPC Server Wrapper
// ============================================================================
class ControlPlane::GrpcServer {
 public:
  GrpcServer(ControlPlane& control_plane, const ControlPlaneConfig& config)
    : service_(control_plane), config_(config) {}

  void Start() {
    grpc::ServerBuilder builder;

    builder.AddListeningPort(
      config_.listen_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service_);
    builder.SetMaxReceiveMessageSize(
      static_cast<int>(config_.grpc_max_message_size));
    builder.SetMaxSendMessageSize(
      static_cast<int>(config_.grpc_max_message_size));

    server_ = builder.BuildAndStart();

    std::cout << "[ControlPlane] gRPC server listening on "
              << config_.listen_address << std::endl;
  }

  void Stop() {
    if (server_) {
      server_->Shutdown();
      std::cout << "[ControlPlane] gRPC server stopped" << std::endl;
    }
  }

  void Wait() {
    if (server_) {
      server_->Wait();
    }
  }

 private:
  ControlPlaneServiceImpl service_;
  ControlPlaneConfig config_;
  std::unique_ptr<grpc::Server> server_;
};

// ============================================================================
// ControlPlane Implementation
// ============================================================================

ControlPlane::ControlPlane(const ControlPlaneConfig& config) : config_(config) {
  InitializeComponents();
}

ControlPlane::~ControlPlane() {
  Stop();
}

void ControlPlane::InitializeComponents() {
  // 创建 Worker Manager
  worker_manager_ = std::make_shared<WorkerManager>(config_.worker_manager_config);

  // 创建 Task Scheduler
  scheduler_ = std::make_unique<TaskScheduler>(config_.scheduler_config);
  scheduler_->SetWorkerManager(worker_manager_);

  // 创建 Bulk Load Coordinator
  bulk_load_coordinator_ = std::make_unique<BulkLoadCoordinatorImpl>(*this);

  // 创建增强观测组件
  metrics_collector_ = std::make_unique<MetricsCollector>();
  task_tracer_ = std::make_unique<TaskTracer>();
  alert_manager_ = std::make_unique<AlertManager>(AlertManagerConfig());
}

void ControlPlane::Start() {
  if (running_.exchange(true)) {
    return;
  }

  // 启动 Worker Manager
  worker_manager_->Start();

  // 启动 Task Scheduler
  scheduler_->Start();

  // 启动 gRPC Server
  StartGrpcServer();

  // 启动 AlertManager (使用 lambda 回调获取指标)
  if (alert_manager_) {
    alert_manager_->Start([this]() -> MetricsSnapshot {
      MetricsSnapshot snap;
      auto cluster = GetClusterStatus();
      const auto& stats = GetTaskStatistics();
      snap.pending_tasks = cluster.pending_tasks;
      snap.running_tasks = cluster.running_tasks;
      snap.total_completed = cluster.total_completed;
      snap.total_failed = cluster.total_failed;
      snap.online_workers = cluster.online_workers;
      snap.total_workers = cluster.total_workers;
      snap.worker_offline_count = cluster.total_workers - cluster.online_workers;
      snap.avg_queue_time_ms = stats.GetAvgQueueTimeMs();
      snap.avg_execution_time_ms = stats.GetAvgExecutionTimeMs();
      snap.task_timeout_count = stats.total_timeout.load();
      // 失败率
      uint64_t total = stats.total_completed.load() + stats.total_failed.load();
      snap.failed_rate = total > 0 ?
        (static_cast<double>(stats.total_failed.load()) / total * 100.0) : 0.0;
      // P99 from metrics collector
      if (metrics_collector_) {
        auto latency = metrics_collector_->GetExecutionLatencyPercentiles();
        snap.p99_execution_time_ms = static_cast<double>(latency.p99_ms);
      }
      // 集群资源使用率
      auto workers = GetWorkers();
      double total_mem = 0, used_mem = 0, total_disk = 0, used_disk = 0;
      for (const auto& w : workers) {
        if (w->status == WorkerStatus::kOnline || w->status == WorkerStatus::kBusy) {
          total_mem += w->resources.total_memory_mb;
          used_mem += w->resources.used_memory_mb;
          total_disk += w->resources.total_disk_mb;
          used_disk += w->resources.used_disk_mb;
        }
      }
      snap.cluster_memory_usage = total_mem > 0 ? used_mem / total_mem : 0.0;
      snap.cluster_disk_usage = total_disk > 0 ? used_disk / total_disk : 0.0;
      return snap;
    });
  }

  std::cout << "[ControlPlane] Control plane started" << std::endl;
}

void ControlPlane::Stop() {
  if (!running_.exchange(false)) {
    return;
  }

  // 停止 gRPC Server
  StopGrpcServer();

  // 停止 AlertManager
  if (alert_manager_) {
    alert_manager_->Stop();
  }

  // 停止 Task Scheduler
  scheduler_->Stop();

  // 停止 Worker Manager
  worker_manager_->Stop();

  // 通知等待者
  {
    std::lock_guard<std::mutex> lock(shutdown_mutex_);
    shutdown_cv_.notify_all();
  }

  std::cout << "[ControlPlane] Control plane stopped" << std::endl;
}

void ControlPlane::Wait() {
  if (grpc_server_) {
    grpc_server_->Wait();
  }
}

void ControlPlane::StartGrpcServer() {
  grpc_server_ = std::make_unique<GrpcServer>(*this, config_);
  grpc_server_->Start();
}

void ControlPlane::StopGrpcServer() {
  if (grpc_server_) {
    grpc_server_->Stop();
  }
}

// =========================================================================
// Tendisplus Node API Implementation
// =========================================================================

std::string ControlPlane::SubmitCompactionTask(
  const std::string& source_node_id,
  const std::string& db_name,
  uint32_t store_id,
  uint64_t job_id,
  const std::string& compaction_input,
  const std::string& shared_fs_uri,
  TaskPriority priority,
  uint32_t timeout_sec) {
  TaskInfo task;
  task.source_node_id = source_node_id;
  task.db_name = db_name;
  task.store_id = store_id;
  task.priority = priority;
  task.type = TaskType::kCompaction;
  task.max_retries = config_.max_task_retries;

  task.params.job_id = job_id;
  task.params.compaction_input = compaction_input;
  task.params.shared_fs_uri = shared_fs_uri;
  task.params.timeout_sec =
    timeout_sec > 0 ? timeout_sec : config_.default_task_timeout_sec;

  auto task_id = scheduler_->SubmitTask(task);

  // 记录任务提交追踪
  if (task_tracer_ && !task_id.empty()) {
    auto span_id = task_tracer_->TraceTaskSubmit(
      task_id, "Compaction", source_node_id);
    task_tracer_->FinishSpan(task_id, span_id);
    // 记录调度 Span
    auto sched_span = task_tracer_->TraceTaskSchedule(task_id, span_id);
    task_tracer_->FinishSpan(task_id, sched_span);
  }

  return task_id;
}

std::shared_ptr<TaskInfo> ControlPlane::QueryTask(
  const std::string& task_id) const {
  return scheduler_->GetTask(task_id);
}

bool ControlPlane::CancelTask(const std::string& task_id,
                              const std::string& reason) {
  return scheduler_->CancelTask(task_id, reason);
}

bool ControlPlane::WaitForTaskResult(const std::string& task_id,
                                     TaskResult* result,
                                     uint32_t timeout_ms) {
  return scheduler_->WaitForTask(task_id, result, timeout_ms);
}

// =========================================================================
// CSA Worker API Implementation
// =========================================================================

std::string ControlPlane::RegisterWorker(
  const std::string& address,
  const WorkerResources& resources,
  const std::map<std::string, std::string>& labels,
  const std::string& requested_id) {
  return worker_manager_->RegisterWorker(address, resources, labels, requested_id);
}

std::vector<std::string> ControlPlane::ProcessWorkerHeartbeat(
  const std::string& worker_id,
  const WorkerResources& resources,
  const std::vector<std::string>& active_task_ids) {
  return worker_manager_->ProcessHeartbeat(worker_id, resources, active_task_ids);
}

bool ControlPlane::UnregisterWorker(const std::string& worker_id,
                                    const std::string& reason) {
  return worker_manager_->UnregisterWorker(worker_id, reason);
}

void ControlPlane::ReportTaskResult(const std::string& worker_id,
                                    const std::string& task_id,
                                    const TaskResult& result) {
  scheduler_->OnTaskCompleted(task_id, result);

  // 记录指标到 MetricsCollector
  if (metrics_collector_) {
    if (result.execution_time_ms > 0) {
      metrics_collector_->RecordExecutionLatency(result.execution_time_ms);
    }
    if (result.success) {
      metrics_collector_->RecordTaskCompleted();
    } else {
      metrics_collector_->RecordTaskFailed();
    }
    // 记录排队延迟
    auto task = scheduler_->GetTask(task_id);
    if (task) {
      auto queue_ms = task->GetQueueTimeMs();
      if (queue_ms > 0) {
        metrics_collector_->RecordQueueLatency(
          static_cast<uint64_t>(queue_ms));
      }
    }
  }

  // 记录追踪
  if (task_tracer_) {
    task_tracer_->TraceTaskComplete(task_id, "",
                                    result.success, result.error_message);
  }
}

std::vector<std::shared_ptr<TaskInfo>> ControlPlane::FetchTasks(
  const std::string& worker_id,
  uint32_t max_tasks) {
  // 获取分配给该 Worker 的任务
  TaskFilter filter;
  filter.assigned_worker_id = worker_id;
  filter.status = TaskStatus::kAssigned;
  filter.limit = max_tasks;

  return scheduler_->QueryTasks(filter);
}

void ControlPlane::MarkTaskRunning(const std::string& task_id) {
  auto task = scheduler_->GetTask(task_id);
  if (task) {
    std::lock_guard<std::mutex> task_lock(task->mtx);
    if (task->status == TaskStatus::kAssigned) {
      task->status = TaskStatus::kRunning;
      task->start_time = std::chrono::system_clock::now();
    }
  }
}

// =========================================================================
// Monitoring API Implementation
// =========================================================================

ControlPlane::ClusterStatus ControlPlane::GetClusterStatus() const {
  ClusterStatus status;
  status.total_workers = worker_manager_->GetWorkerCount();
  status.online_workers = worker_manager_->GetOnlineWorkerCount();
  status.pending_tasks = scheduler_->GetPendingCount();
  status.running_tasks = scheduler_->GetRunningCount();

  const auto& stats = scheduler_->GetStatistics();
  status.total_completed = stats.total_completed.load();
  status.total_failed = stats.total_failed.load();

  return status;
}

std::vector<std::shared_ptr<WorkerInfo>> ControlPlane::GetWorkers() const {
  return worker_manager_->GetAllWorkers();
}

const TaskStatistics& ControlPlane::GetTaskStatistics() const {
  return scheduler_->GetStatistics();
}

// =========================================================================
// BulkLoadCoordinator Implementation (内嵌在 control_plane.cc 中)
// =========================================================================
class ControlPlane::BulkLoadCoordinatorImpl {
 public:
  explicit BulkLoadCoordinatorImpl(ControlPlane& cp) : control_plane_(cp) {}

  // 执行分片规划（当前简化实现，后续 Commit 3.2 中完善）
  void PlanShards(std::shared_ptr<TaskInfo> task) {
    // 问题6 修复: 修改 bulk_load_params 前加 per-task mutex
    std::lock_guard<std::mutex> task_lock(task->mtx);
    auto& params = task->bulk_load_params;
    params.phase = BulkLoadPhase::kPlanning;

    int32_t shard_count = params.shard_count > 0 ? params.shard_count : 4;

    // 根据分片策略生成分片
    for (int32_t i = 0; i < shard_count; i++) {
      BulkLoadShardInfo shard;
      shard.shard_id = task->task_id + "_shard_" + std::to_string(i);
      shard.shard_index = i;
      shard.status = TaskStatus::kPending;

      // 简化分片：均匀分配 key 范围
      if (!params.key_ranges.empty() && i < static_cast<int32_t>(params.key_ranges.size())) {
        shard.key_range = params.key_ranges[i];
      }

      shard.source_path = params.source_path + "/shard_" + std::to_string(i);
      params.shards.push_back(shard);
    }

    params.phase = BulkLoadPhase::kQueued;

    std::cout << "[BulkLoadCoordinator] Planned " << shard_count
              << " shards for task " << task->task_id << std::endl;
  }

  // 将分片作为子任务提交到调度器
  void SubmitShardTasks(std::shared_ptr<TaskInfo> parent_task) {
    // 问题6 修复: 先在锁内读取所需参数，锁外提交子任务，再锁内更新 phase
    struct ShardSubmitInfo {
      TaskInfo shard_task;
      size_t shard_index;  // 在 shards 数组中的索引
    };
    std::vector<ShardSubmitInfo> shard_submissions;

    {
      std::lock_guard<std::mutex> task_lock(parent_task->mtx);
      auto& params = parent_task->bulk_load_params;

      for (size_t i = 0; i < params.shards.size(); i++) {
        ShardSubmitInfo info;
        info.shard_index = i;

        info.shard_task.type = TaskType::kBulkLoad;
        info.shard_task.priority = parent_task->priority;
        info.shard_task.source_node_id = parent_task->source_node_id;
        info.shard_task.db_name = parent_task->db_name;
        info.shard_task.store_id = params.target_store_id;
        info.shard_task.max_retries = parent_task->max_retries;
        info.shard_task.parent_task_id = parent_task->task_id;

        // 复制 Bulk Load 参数到分片任务
        info.shard_task.bulk_load_params = params;
        info.shard_task.bulk_load_params.shards.clear();

        shard_submissions.push_back(std::move(info));
      }
    }
    // ---- per-task mutex 已释放 ----

    // 在锁外提交子任务（SubmitBulkLoadShard 内部会获取其他锁）
    struct ShardResult {
      size_t shard_index;
      std::string shard_task_id;
      bool success;
    };
    std::vector<ShardResult> results;

    for (auto& sub : shard_submissions) {
      auto shard_task_id = control_plane_.GetScheduler().SubmitBulkLoadShard(
        sub.shard_task);
      ShardResult r;
      r.shard_index = sub.shard_index;
      r.shard_task_id = shard_task_id;
      r.success = !shard_task_id.empty();
      results.push_back(r);

      if (r.success) {
        std::cout << "[BulkLoadCoordinator] Shard submitted as task "
                  << shard_task_id << std::endl;
      }
    }

    // 锁内更新分片状态和 phase
    {
      std::lock_guard<std::mutex> task_lock(parent_task->mtx);
      auto& params = parent_task->bulk_load_params;
      for (const auto& r : results) {
        if (r.shard_index < params.shards.size()) {
          if (r.success) {
            params.shards[r.shard_index].status = TaskStatus::kPending;
          } else {
            params.shards[r.shard_index].status = TaskStatus::kFailed;
            params.shards[r.shard_index].error_message = "Failed to submit shard task";
            params.failed_shards++;
          }
        }
      }
      params.phase = BulkLoadPhase::kSSTGenerating;
    }
  }

  // 处理 SST 注入结果
  void OnIngestResult(const std::string& task_id,
                      bool success,
                      uint32_t ingested_sst_count,
                      uint64_t ingested_bytes,
                      uint64_t ingested_rows,
                      const std::string& error_message) {
    auto task = control_plane_.GetScheduler().GetBulkLoadTask(task_id);
    if (!task) {
      std::cerr << "[BulkLoadCoordinator] Unknown task for ingest result: "
                << task_id << std::endl;
      return;
    }

    std::lock_guard<std::mutex> task_lock(task->mtx);

    if (success) {
      task->bulk_load_params.phase = BulkLoadPhase::kCompleted;
      task->status = TaskStatus::kCompleted;
      task->complete_time = std::chrono::system_clock::now();
      task->result.success = true;
      task->result.total_rows_processed = ingested_rows;
      task->result.sst_files_count = ingested_sst_count;
      task->result.bytes_written = ingested_bytes;
      std::cout << "[BulkLoadCoordinator] Task " << task_id
                << " ingest completed successfully" << std::endl;
    } else {
      task->bulk_load_params.phase = BulkLoadPhase::kFailed;
      task->status = TaskStatus::kFailed;
      task->complete_time = std::chrono::system_clock::now();
      task->error_message = error_message;
      task->result.success = false;
      task->result.error_message = error_message;
      std::cerr << "[BulkLoadCoordinator] Task " << task_id
                << " ingest failed: " << error_message << std::endl;
    }
  }

 private:
  ControlPlane& control_plane_;
};

// =========================================================================
// Bulk Load API Implementation
// =========================================================================

std::string ControlPlane::SubmitBulkLoadTask(
  const std::string& source_node_id,
  const std::string& db_name,
  const BulkLoadTaskParams& params,
  TaskPriority priority) {
  TaskInfo task;
  task.source_node_id = source_node_id;
  task.db_name = db_name;
  task.store_id = params.target_store_id;
  task.priority = priority;
  task.type = TaskType::kBulkLoad;
  task.max_retries = config_.max_task_retries;
  task.bulk_load_params = params;

  // 提交到调度器
  std::string task_id = scheduler_->SubmitBulkLoadTask(task);
  if (task_id.empty()) {
    return "";
  }

  // 执行分片规划并提交子任务
  auto submitted_task = scheduler_->GetBulkLoadTask(task_id);
  if (submitted_task && bulk_load_coordinator_) {
    bulk_load_coordinator_->PlanShards(submitted_task);
    bulk_load_coordinator_->SubmitShardTasks(submitted_task);
  }

  return task_id;
}

std::shared_ptr<TaskInfo> ControlPlane::QueryBulkLoadStatus(
  const std::string& task_id) const {
  return scheduler_->GetBulkLoadTask(task_id);
}

bool ControlPlane::CancelBulkLoad(const std::string& task_id,
                                  const std::string& reason) {
  auto task = scheduler_->GetBulkLoadTask(task_id);
  if (!task) {
    return false;
  }

  // 收集需要通知取消的 running 分片及其 worker_id
  struct ShardCancelInfo {
    std::string shard_id;
    std::string worker_id;
  };
  std::vector<ShardCancelInfo> shards_to_cancel_on_workers;

  // 取消所有分片 — 在 per-task mutex 保护下修改
  uint32_t cancelled = 0;
  {
    std::lock_guard<std::mutex> task_lock(task->mtx);

    // 检查任务是否已在终态
    if (task->IsTerminal()) {
      // 幂等性：已取消的任务再次取消返回 true
      if (task->status == TaskStatus::kCancelled) {
        return true;
      }
      return false;
    }

    for (auto& shard : task->bulk_load_params.shards) {
      if (shard.status == TaskStatus::kPending ||
          shard.status == TaskStatus::kRunning) {
        // 问题13: 记录 running 分片的 worker 信息，稍后通知 Worker 取消
        if (shard.status == TaskStatus::kRunning &&
            !shard.assigned_worker_id.empty()) {
          shards_to_cancel_on_workers.push_back(
            {shard.shard_id, shard.assigned_worker_id});
        }
        shard.status = TaskStatus::kCancelled;
        cancelled++;
      }
    }

    task->bulk_load_params.phase = BulkLoadPhase::kCancelled;
    task->status = TaskStatus::kCancelled;
    task->error_message = reason;
    task->complete_time = std::chrono::system_clock::now();
  }

  // 问题13: 通知 CSA Worker 取消正在执行的分片任务
  if (worker_manager_ && !shards_to_cancel_on_workers.empty()) {
    for (const auto& info : shards_to_cancel_on_workers) {
      worker_manager_->CancelCSATask(info.worker_id, info.shard_id);
      std::cout << "[ControlPlane] Notified worker " << info.worker_id
                << " to cancel shard: " << info.shard_id << std::endl;
    }
  }

  std::cout << "[ControlPlane] Bulk Load cancelled: " << task_id
            << " (" << cancelled << " shards cancelled)" << std::endl;

  return true;
}

void ControlPlane::ReportIngestResult(
  const std::string& task_id,
  const std::string& source_node_id,
  bool success,
  uint32_t ingested_sst_count,
  uint64_t ingested_bytes,
  uint64_t ingested_rows,
  const std::string& error_message) {
  if (bulk_load_coordinator_) {
    bulk_load_coordinator_->OnIngestResult(
      task_id, success, ingested_sst_count,
      ingested_bytes, ingested_rows, error_message);
  }
}

// =========================================================================
// Prometheus Metrics Generation
// =========================================================================

std::string ControlPlane::GeneratePrometheusMetrics() const {
  std::ostringstream out;

  auto cluster = GetClusterStatus();
  const auto& stats = GetTaskStatistics();

  // 集群状态
  out << "# HELP caas_lsm_workers_total Total number of registered workers\n";
  out << "# TYPE caas_lsm_workers_total gauge\n";
  out << "caas_lsm_workers_total " << cluster.total_workers << "\n";

  out << "# HELP caas_lsm_workers_online Number of online workers\n";
  out << "# TYPE caas_lsm_workers_online gauge\n";
  out << "caas_lsm_workers_online " << cluster.online_workers << "\n";

  out << "# HELP caas_lsm_tasks_pending Number of pending tasks\n";
  out << "# TYPE caas_lsm_tasks_pending gauge\n";
  out << "caas_lsm_tasks_pending " << cluster.pending_tasks << "\n";

  out << "# HELP caas_lsm_tasks_running Number of running tasks\n";
  out << "# TYPE caas_lsm_tasks_running gauge\n";
  out << "caas_lsm_tasks_running " << cluster.running_tasks << "\n";

  // 任务计数器 (累积)
  out << "# HELP caas_lsm_tasks_submitted_total Total submitted tasks\n";
  out << "# TYPE caas_lsm_tasks_submitted_total counter\n";
  out << "caas_lsm_tasks_submitted_total " << stats.total_submitted.load() << "\n";

  out << "# HELP caas_lsm_tasks_completed_total Total completed tasks\n";
  out << "# TYPE caas_lsm_tasks_completed_total counter\n";
  out << "caas_lsm_tasks_completed_total " << stats.total_completed.load() << "\n";

  out << "# HELP caas_lsm_tasks_failed_total Total failed tasks\n";
  out << "# TYPE caas_lsm_tasks_failed_total counter\n";
  out << "caas_lsm_tasks_failed_total " << stats.total_failed.load() << "\n";

  out << "# HELP caas_lsm_tasks_cancelled_total Total cancelled tasks\n";
  out << "# TYPE caas_lsm_tasks_cancelled_total counter\n";
  out << "caas_lsm_tasks_cancelled_total " << stats.total_cancelled.load() << "\n";

  out << "# HELP caas_lsm_tasks_timeout_total Total timeout tasks\n";
  out << "# TYPE caas_lsm_tasks_timeout_total counter\n";
  out << "caas_lsm_tasks_timeout_total " << stats.total_timeout.load() << "\n";

  // 延迟统计
  out << "# HELP caas_lsm_queue_time_ms Average queue time in milliseconds\n";
  out << "# TYPE caas_lsm_queue_time_ms gauge\n";
  out << "caas_lsm_queue_time_ms " << stats.GetAvgQueueTimeMs() << "\n";

  out << "# HELP caas_lsm_execution_time_ms Average execution time in ms\n";
  out << "# TYPE caas_lsm_execution_time_ms gauge\n";
  out << "caas_lsm_execution_time_ms " << stats.GetAvgExecutionTimeMs() << "\n";

  // 百分位延迟 (来自 MetricsCollector)
  if (metrics_collector_) {
    auto exec_latency = metrics_collector_->GetExecutionLatencyPercentiles();
    out << "# HELP caas_lsm_execution_latency_p50_ms P50 execution latency\n";
    out << "# TYPE caas_lsm_execution_latency_p50_ms gauge\n";
    out << "caas_lsm_execution_latency_p50_ms " << exec_latency.p50_ms << "\n";

    out << "# HELP caas_lsm_execution_latency_p95_ms P95 execution latency\n";
    out << "# TYPE caas_lsm_execution_latency_p95_ms gauge\n";
    out << "caas_lsm_execution_latency_p95_ms " << exec_latency.p95_ms << "\n";

    out << "# HELP caas_lsm_execution_latency_p99_ms P99 execution latency\n";
    out << "# TYPE caas_lsm_execution_latency_p99_ms gauge\n";
    out << "caas_lsm_execution_latency_p99_ms " << exec_latency.p99_ms << "\n";

    auto queue_latency = metrics_collector_->GetQueueLatencyPercentiles();
    out << "# HELP caas_lsm_queue_latency_p50_ms P50 queue latency\n";
    out << "# TYPE caas_lsm_queue_latency_p50_ms gauge\n";
    out << "caas_lsm_queue_latency_p50_ms " << queue_latency.p50_ms << "\n";

    out << "# HELP caas_lsm_queue_latency_p99_ms P99 queue latency\n";
    out << "# TYPE caas_lsm_queue_latency_p99_ms gauge\n";
    out << "caas_lsm_queue_latency_p99_ms " << queue_latency.p99_ms << "\n";

    // 滑动窗口吞吐量
    out << "# HELP caas_lsm_completed_last_minute Tasks completed in last minute\n";
    out << "# TYPE caas_lsm_completed_last_minute gauge\n";
    out << "caas_lsm_completed_last_minute " << metrics_collector_->GetCompletedLastMinute() << "\n";

    out << "# HELP caas_lsm_failed_last_minute Tasks failed in last minute\n";
    out << "# TYPE caas_lsm_failed_last_minute gauge\n";
    out << "caas_lsm_failed_last_minute " << metrics_collector_->GetFailedLastMinute() << "\n";
  }

  // 资源使用率
  auto workers = GetWorkers();
  double total_mem = 0, used_mem = 0, total_disk = 0, used_disk = 0;
  uint32_t busy_count = 0;
  for (const auto& w : workers) {
    if (w->status == WorkerStatus::kOnline || w->status == WorkerStatus::kBusy) {
      total_mem += w->resources.total_memory_mb;
      used_mem += w->resources.used_memory_mb;
      total_disk += w->resources.total_disk_mb;
      used_disk += w->resources.used_disk_mb;
    }
    if (w->status == WorkerStatus::kBusy) {
      busy_count++;
    }
  }

  out << "# HELP caas_lsm_workers_busy Number of busy workers\n";
  out << "# TYPE caas_lsm_workers_busy gauge\n";
  out << "caas_lsm_workers_busy " << busy_count << "\n";

  out << "# HELP caas_lsm_cluster_memory_usage Cluster memory usage ratio\n";
  out << "# TYPE caas_lsm_cluster_memory_usage gauge\n";
  out << "caas_lsm_cluster_memory_usage "
      << (total_mem > 0 ? used_mem / total_mem : 0.0) << "\n";

  out << "# HELP caas_lsm_cluster_disk_usage Cluster disk usage ratio\n";
  out << "# TYPE caas_lsm_cluster_disk_usage gauge\n";
  out << "caas_lsm_cluster_disk_usage "
      << (total_disk > 0 ? used_disk / total_disk : 0.0) << "\n";

  // 告警统计
  if (alert_manager_) {
    out << "# HELP caas_lsm_alerts_active Number of active alerts\n";
    out << "# TYPE caas_lsm_alerts_active gauge\n";
    out << "caas_lsm_alerts_active " << alert_manager_->GetActiveAlertCount() << "\n";

    out << "# HELP caas_lsm_alerts_by_severity Active alerts by severity\n";
    out << "# TYPE caas_lsm_alerts_by_severity gauge\n";
    out << "caas_lsm_alerts_by_severity{severity=\"info\"} "
        << alert_manager_->GetAlertCountBySeverity(AlertSeverity::kInfo) << "\n";
    out << "caas_lsm_alerts_by_severity{severity=\"warning\"} "
        << alert_manager_->GetAlertCountBySeverity(AlertSeverity::kWarning) << "\n";
    out << "caas_lsm_alerts_by_severity{severity=\"critical\"} "
        << alert_manager_->GetAlertCountBySeverity(AlertSeverity::kCritical) << "\n";
  }

  // 追踪统计
  if (task_tracer_) {
    out << "# HELP caas_lsm_traces_total Total trace count\n";
    out << "# TYPE caas_lsm_traces_total gauge\n";
    out << "caas_lsm_traces_total " << task_tracer_->GetTraceCount() << "\n";

    out << "# HELP caas_lsm_traces_active Active trace count\n";
    out << "# TYPE caas_lsm_traces_active gauge\n";
    out << "caas_lsm_traces_active " << task_tracer_->GetActiveTraceCount() << "\n";
  }

  return out.str();
}

}  // namespace control_plane
}  // namespace tendisplus
