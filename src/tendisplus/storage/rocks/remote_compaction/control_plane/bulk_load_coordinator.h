// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

// Bulk Load Coordinator - 批量加载协调器
// Based on CaaS-LSM architecture
//
// 职责：
//   ① 数据源扫描与分片规划
//   ② 子任务调度与进度管理
//   ③ SST 注入编排
//   ④ 异常处理（Worker 故障重分配、注入失败回滚、超时处理）

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "task_model.h"

namespace tendisplus {
namespace control_plane {

// ============================================================================
// Bulk Load 任务摘要（用于外部查询）
// ============================================================================
struct BulkLoadTaskSummary {
  std::string task_id;
  BulkLoadPhase phase = BulkLoadPhase::kCreated;
  TaskStatus overall_status = TaskStatus::kPending;

  // 分片进度
  uint32_t total_shards = 0;
  uint32_t completed_shards = 0;
  uint32_t failed_shards = 0;
  uint32_t running_shards = 0;
  uint32_t pending_shards = 0;

  // 数据量统计
  uint64_t total_estimated_bytes = 0;
  uint64_t total_processed_bytes = 0;
  uint64_t total_sst_files = 0;
  uint64_t total_rows_processed = 0;

  // 时间信息
  int64_t submit_time_ms = 0;
  int64_t start_time_ms = 0;
  int64_t complete_time_ms = 0;

  // 进度百分比
  double progress_percent = 0.0;

  std::string error_message;
};

// ============================================================================
// Bulk Load Coordinator 接口
// ============================================================================
class BulkLoadCoordinator {
 public:
  virtual ~BulkLoadCoordinator() = default;

  // 执行分片规划
  virtual void PlanShards(std::shared_ptr<TaskInfo> task) = 0;

  // 将分片作为子任务提交到调度器
  virtual void SubmitShardTasks(std::shared_ptr<TaskInfo> parent_task) = 0;

  // 处理 SST 注入结果
  virtual void OnIngestResult(const std::string& task_id,
                              bool success,
                              uint32_t ingested_sst_count,
                              uint64_t ingested_bytes,
                              uint64_t ingested_rows,
                              const std::string& error_message) = 0;

  // 获取 Bulk Load 任务摘要
  virtual BulkLoadTaskSummary GetTaskSummary(
    const std::string& task_id) const = 0;

  // 获取所有活跃的 Bulk Load 任务摘要
  virtual std::vector<BulkLoadTaskSummary> GetActiveTasks() const = 0;
};

}  // namespace control_plane
}  // namespace tendisplus
