// Copyright (c) 2024-present, Tencent Inc. All rights reserved.
// 任务状态机定义

#pragma once

#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "task_model.h"

namespace tendisplus {
namespace control_plane {

// ============================================================================
// 状态转换事件
// ============================================================================
enum class TaskEvent {
  kEnqueue,          // 入队
  kSchedule,         // 调度
  kStart,            // 开始执行
  kComplete,         // 完成
  kFail,             // 失败
  kRetry,            // 重试
  kCancel,           // 取消
  kTimeout,          // 超时
  kWorkerLost        // Worker 丢失
};

inline const char* TaskEventToString(TaskEvent event) {
  switch (event) {
    case TaskEvent::kEnqueue: return "Enqueue";
    case TaskEvent::kSchedule: return "Schedule";
    case TaskEvent::kStart: return "Start";
    case TaskEvent::kComplete: return "Complete";
    case TaskEvent::kFail: return "Fail";
    case TaskEvent::kRetry: return "Retry";
    case TaskEvent::kCancel: return "Cancel";
    case TaskEvent::kTimeout: return "Timeout";
    case TaskEvent::kWorkerLost: return "WorkerLost";
    default: return "Unknown";
  }
}

// ============================================================================
// 状态转换回调类型
// ============================================================================
using StateTransitionCallback = std::function<void(
    const BackgroundTask& task,
    TaskStatus old_status,
    TaskStatus new_status,
    TaskEvent event,
    const std::string& reason)>;

// ============================================================================
// 状态转换规则
// ============================================================================
struct TransitionRule {
  TaskStatus from_status;
  TaskEvent event;
  TaskStatus to_status;
  std::function<bool(const BackgroundTask&)> guard;  // 可选的守卫条件
};

// ============================================================================
// 任务状态机
// ============================================================================
class TaskStateMachine {
 public:
  TaskStateMachine();
  ~TaskStateMachine() = default;
  
  // 禁止拷贝
  TaskStateMachine(const TaskStateMachine&) = delete;
  TaskStateMachine& operator=(const TaskStateMachine&) = delete;
  
  // 单例访问
  static TaskStateMachine& Instance();
  
  // 状态转换
  // 返回 true 表示转换成功，false 表示转换不合法
  bool ProcessEvent(BackgroundTask& task, TaskEvent event, 
                    const std::string& reason = "",
                    const std::string& operator_id = "system");
  
  // 检查是否可以转换
  bool CanTransition(TaskStatus from_status, TaskEvent event) const;
  
  // 获取目标状态
  std::optional<TaskStatus> GetTargetStatus(TaskStatus from_status, 
                                            TaskEvent event) const;
  
  // 注册状态转换回调
  void RegisterCallback(StateTransitionCallback callback);
  
  // 获取状态的合法事件列表
  std::vector<TaskEvent> GetValidEvents(TaskStatus status) const;
  
  // 获取状态转换图 (用于可视化)
  std::string GetStateDiagram() const;

 private:
  void InitializeTransitionRules();
  void NotifyCallbacks(const BackgroundTask& task, TaskStatus old_status,
                       TaskStatus new_status, TaskEvent event,
                       const std::string& reason);
  
  std::vector<TransitionRule> transition_rules_;
  std::vector<StateTransitionCallback> callbacks_;
  mutable std::mutex mutex_;
};

// ============================================================================
// 状态机事件处理器 (用于异步处理)
// ============================================================================
class TaskEventProcessor {
 public:
  struct EventRecord {
    std::string task_id;
    TaskEvent event;
    std::string reason;
    std::string operator_id;
    std::chrono::system_clock::time_point timestamp;
  };
  
  TaskEventProcessor();
  ~TaskEventProcessor();
  
  // 提交事件 (异步处理)
  void SubmitEvent(const std::string& task_id, TaskEvent event,
                   const std::string& reason = "",
                   const std::string& operator_id = "system");
  
  // 启动/停止处理器
  void Start();
  void Stop();
  
  // 获取待处理事件数量
  size_t GetPendingEventCount() const;

 private:
  void ProcessLoop();
  
  std::vector<EventRecord> pending_events_;
  mutable std::mutex mutex_;
  std::condition_variable cv_;
  std::atomic<bool> running_{false};
  std::unique_ptr<std::thread> processor_thread_;
};

}  // namespace control_plane
}  // namespace tendisplus
