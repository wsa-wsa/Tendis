// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include "task_model.h"

#include <sstream>

namespace tendisplus {
namespace control_plane {

std::string TaskInfo::ToString() const {
  std::ostringstream oss;
  oss << "TaskInfo{"
      << "id=" << task_id << ", status=" << TaskStatusToString(status)
      << ", priority=" << TaskPriorityToString(priority)
      << ", source=" << source_node_id << ", db=" << db_name
      << ", store=" << store_id;
  if (!assigned_worker_id.empty()) {
    oss << ", worker=" << assigned_worker_id;
  }
  oss << ", retries=" << retry_count << "/" << max_retries << "}";
  return oss.str();
}

bool TaskFilter::Matches(const TaskInfo& task) const {
  if (status.has_value() && task.status != status.value()) {
    return false;
  }
  if (task_type.has_value() && task.type != task_type.value()) {
    return false;
  }
  if (priority.has_value() && task.priority != priority.value()) {
    return false;
  }
  if (source_node_id.has_value() &&
      task.source_node_id != source_node_id.value()) {
    return false;
  }
  if (assigned_worker_id.has_value() &&
      task.assigned_worker_id != assigned_worker_id.value()) {
    return false;
  }
  if (db_name.has_value() && task.db_name != db_name.value()) {
    return false;
  }
  if (since_time_ms > 0) {
    auto submit_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                       task.submit_time.time_since_epoch())
                       .count();
    if (submit_ms < since_time_ms) {
      return false;
    }
  }
  return true;
}

std::string TaskStatistics::ToString() const {
  std::ostringstream oss;
  oss << "TaskStatistics{"
      << "submitted=" << total_submitted.load()
      << ", completed=" << total_completed.load()
      << ", failed=" << total_failed.load()
      << ", cancelled=" << total_cancelled.load()
      << ", timeout=" << total_timeout.load()
      << ", pending=" << pending_count.load()
      << ", running=" << running_count.load()
      << ", avg_exec_ms=" << GetAvgExecutionTimeMs()
      << ", avg_queue_ms=" << GetAvgQueueTimeMs() << "}";
  return oss.str();
}

}  // namespace control_plane
}  // namespace tendisplus
