// Copyright (c) 2024-present, Tencent Inc. All rights reserved.
// 任务状态机实现

#include "task_state_machine.h"

#include <iostream>
#include <sstream>

namespace tendisplus {
namespace control_plane {

// ============================================================================
// TaskStateMachine 实现
// ============================================================================

TaskStateMachine::TaskStateMachine() {
  InitializeTransitionRules();
}

TaskStateMachine& TaskStateMachine::Instance() {
  static TaskStateMachine instance;
  return instance;
}

void TaskStateMachine::InitializeTransitionRules() {
  // 定义合法的状态转换规则
  // 格式: {from_status, event, to_status, guard}
  
  transition_rules_ = {
    // Created -> Queued (入队)
    {TaskStatus::kCreated, TaskEvent::kEnqueue, TaskStatus::kQueued, nullptr},
    
    // Created -> Cancelled (取消)
    {TaskStatus::kCreated, TaskEvent::kCancel, TaskStatus::kCancelled, nullptr},
    
    // Queued -> Scheduled (调度)
    {TaskStatus::kQueued, TaskEvent::kSchedule, TaskStatus::kScheduled, nullptr},
    
    // Queued -> Cancelled (取消)
    {TaskStatus::kQueued, TaskEvent::kCancel, TaskStatus::kCancelled, nullptr},
    
    // Scheduled -> Running (开始执行)
    {TaskStatus::kScheduled, TaskEvent::kStart, TaskStatus::kRunning, nullptr},
    
    // Scheduled -> Queued (Worker 拒绝，重新入队)
    {TaskStatus::kScheduled, TaskEvent::kWorkerLost, TaskStatus::kQueued, nullptr},
    
    // Scheduled -> Cancelled (取消)
    {TaskStatus::kScheduled, TaskEvent::kCancel, TaskStatus::kCancelled, nullptr},
    
    // Running -> Completed (完成)
    {TaskStatus::kRunning, TaskEvent::kComplete, TaskStatus::kCompleted, nullptr},
    
    // Running -> Failed (失败)
    {TaskStatus::kRunning, TaskEvent::kFail, TaskStatus::kFailed, nullptr},
    
    // Running -> Failed (超时)
    {TaskStatus::kRunning, TaskEvent::kTimeout, TaskStatus::kFailed, nullptr},
    
    // Running -> Failed (Worker 丢失)
    {TaskStatus::kRunning, TaskEvent::kWorkerLost, TaskStatus::kFailed, nullptr},
    
    // Running -> Cancelled (取消)
    {TaskStatus::kRunning, TaskEvent::kCancel, TaskStatus::kCancelled, nullptr},
    
    // Failed -> Retrying (重试，需要检查重试次数)
    {TaskStatus::kFailed, TaskEvent::kRetry, TaskStatus::kRetrying, 
     [](const BackgroundTask& task) {
       return task.retry_count < task.retry_policy.max_retries;
     }},
    
    // Retrying -> Queued (重新入队)
    {TaskStatus::kRetrying, TaskEvent::kEnqueue, TaskStatus::kQueued, nullptr},
    
    // Retrying -> Cancelled (取消)
    {TaskStatus::kRetrying, TaskEvent::kCancel, TaskStatus::kCancelled, nullptr},
  };
}

bool TaskStateMachine::ProcessEvent(BackgroundTask& task, TaskEvent event,
                                    const std::string& reason,
                                    const std::string& operator_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  // 查找匹配的转换规则
  for (const auto& rule : transition_rules_) {
    if (rule.from_status == task.status && rule.event == event) {
      // 检查守卫条件
      if (rule.guard && !rule.guard(task)) {
        std::cerr << "[StateMachine] Guard condition failed for task " 
                  << task.task_id << ", event: " << TaskEventToString(event) 
                  << std::endl;
        return false;
      }
      
      // 记录旧状态
      TaskStatus old_status = task.status;
      
      // 执行状态转换
      task.status = rule.to_status;
      
      // 记录状态转换历史
      task.RecordStateTransition(rule.to_status, reason, operator_id);
      
      // 更新时间戳
      auto now = std::chrono::system_clock::now();
      switch (rule.to_status) {
        case TaskStatus::kQueued:
          task.queued_at = now;
          break;
        case TaskStatus::kScheduled:
          task.scheduled_at = now;
          break;
        case TaskStatus::kRunning:
          task.started_at = now;
          break;
        case TaskStatus::kCompleted:
        case TaskStatus::kFailed:
        case TaskStatus::kCancelled:
          task.completed_at = now;
          break;
        case TaskStatus::kRetrying:
          task.retry_count++;
          break;
        default:
          break;
      }
      
      // 通知回调
      NotifyCallbacks(task, old_status, rule.to_status, event, reason);
      
      std::cout << "[StateMachine] Task " << task.task_id 
                << " transitioned: " << TaskStatusToString(old_status)
                << " -> " << TaskStatusToString(rule.to_status)
                << " (event: " << TaskEventToString(event) << ")"
                << std::endl;
      
      return true;
    }
  }
  
  std::cerr << "[StateMachine] Invalid transition for task " << task.task_id
            << ": " << TaskStatusToString(task.status) 
            << " + " << TaskEventToString(event) << std::endl;
  return false;
}

bool TaskStateMachine::CanTransition(TaskStatus from_status, 
                                     TaskEvent event) const {
  std::lock_guard<std::mutex> lock(mutex_);
  
  for (const auto& rule : transition_rules_) {
    if (rule.from_status == from_status && rule.event == event) {
      return true;
    }
  }
  return false;
}

std::optional<TaskStatus> TaskStateMachine::GetTargetStatus(
    TaskStatus from_status, TaskEvent event) const {
  std::lock_guard<std::mutex> lock(mutex_);
  
  for (const auto& rule : transition_rules_) {
    if (rule.from_status == from_status && rule.event == event) {
      return rule.to_status;
    }
  }
  return std::nullopt;
}

void TaskStateMachine::RegisterCallback(StateTransitionCallback callback) {
  std::lock_guard<std::mutex> lock(mutex_);
  callbacks_.push_back(std::move(callback));
}

std::vector<TaskEvent> TaskStateMachine::GetValidEvents(TaskStatus status) const {
  std::lock_guard<std::mutex> lock(mutex_);
  
  std::vector<TaskEvent> events;
  for (const auto& rule : transition_rules_) {
    if (rule.from_status == status) {
      events.push_back(rule.event);
    }
  }
  return events;
}

std::string TaskStateMachine::GetStateDiagram() const {
  std::lock_guard<std::mutex> lock(mutex_);
  
  std::ostringstream oss;
  oss << "digraph TaskStateMachine {\n";
  oss << "  rankdir=LR;\n";
  oss << "  node [shape=box];\n";
  
  for (const auto& rule : transition_rules_) {
    oss << "  " << TaskStatusToString(rule.from_status) 
        << " -> " << TaskStatusToString(rule.to_status)
        << " [label=\"" << TaskEventToString(rule.event) << "\"];\n";
  }
  
  oss << "}\n";
  return oss.str();
}

void TaskStateMachine::NotifyCallbacks(const BackgroundTask& task,
                                       TaskStatus old_status,
                                       TaskStatus new_status,
                                       TaskEvent event,
                                       const std::string& reason) {
  for (const auto& callback : callbacks_) {
    try {
      callback(task, old_status, new_status, event, reason);
    } catch (const std::exception& e) {
      std::cerr << "[StateMachine] Callback exception: " << e.what() << std::endl;
    }
  }
}

// ============================================================================
// BackgroundTask 辅助方法实现
// ============================================================================

bool BackgroundTask::CanTransitionTo(TaskStatus new_status) const {
  // 简化版本，实际应该使用状态机
  switch (status) {
    case TaskStatus::kCreated:
      return new_status == TaskStatus::kQueued || 
             new_status == TaskStatus::kCancelled;
    case TaskStatus::kQueued:
      return new_status == TaskStatus::kScheduled || 
             new_status == TaskStatus::kCancelled;
    case TaskStatus::kScheduled:
      return new_status == TaskStatus::kRunning || 
             new_status == TaskStatus::kQueued ||
             new_status == TaskStatus::kCancelled;
    case TaskStatus::kRunning:
      return new_status == TaskStatus::kCompleted || 
             new_status == TaskStatus::kFailed ||
             new_status == TaskStatus::kCancelled;
    case TaskStatus::kFailed:
      return new_status == TaskStatus::kRetrying;
    case TaskStatus::kRetrying:
      return new_status == TaskStatus::kQueued ||
             new_status == TaskStatus::kCancelled;
    default:
      return false;
  }
}

void BackgroundTask::RecordStateTransition(TaskStatus new_status,
                                           const std::string& reason,
                                           const std::string& operator_id) {
  StateTransition transition;
  transition.from_status = status;
  transition.to_status = new_status;
  transition.timestamp = std::chrono::system_clock::now();
  transition.reason = reason;
  transition.operator_id = operator_id;
  state_history.push_back(std::move(transition));
}

uint64_t BackgroundTask::GetElapsedTimeMs() const {
  auto now = std::chrono::system_clock::now();
  auto start = created_at;
  return std::chrono::duration_cast<std::chrono::milliseconds>(now - start).count();
}

std::string BackgroundTask::ToString() const {
  std::ostringstream oss;
  oss << "Task{id=" << task_id
      << ", type=" << TaskTypeToString(task_type)
      << ", status=" << TaskStatusToString(status)
      << ", priority=" << TaskPriorityToString(priority)
      << ", source=" << source_node_id
      << ", worker=" << assigned_worker_id
      << ", retry=" << retry_count << "/" << retry_policy.max_retries
      << "}";
  return oss.str();
}

std::string StateTransition::ToString() const {
  std::ostringstream oss;
  oss << TaskStatusToString(from_status) << " -> " << TaskStatusToString(to_status)
      << " (reason: " << reason << ", by: " << operator_id << ")";
  return oss.str();
}

std::string ResourceRequirement::ToString() const {
  std::ostringstream oss;
  oss << "Resources{cpu=" << cpu_cores
      << ", mem=" << memory_mb << "MB"
      << ", disk=" << disk_mb << "MB"
      << ", net=" << network_bandwidth_mbps << "Mbps"
      << ", est_time=" << estimated_duration_sec << "s}";
  return oss.str();
}

std::string TaskResult::ToString() const {
  std::ostringstream oss;
  oss << "Result{success=" << (success ? "true" : "false")
      << ", files=" << output_files.size()
      << ", read=" << bytes_read
      << ", written=" << bytes_written
      << ", records=" << records_processed
      << ", time=" << execution_time_ms << "ms";
  if (!success) {
    oss << ", error=" << error_message;
  }
  oss << "}";
  return oss.str();
}

}  // namespace control_plane
}  // namespace tendisplus
