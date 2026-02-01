// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

// Task Scheduler for Control Plane
// Based on CaaS-LSM architecture

#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "task_model.h"
#include "worker_manager.h"

namespace tendisplus {
namespace control_plane {

// ============================================================================
// 调度策略
// ============================================================================
enum class SchedulingPolicy {
  kFIFO = 0,           // 先进先出
  kPriority = 1,       // 优先级调度
  kFairShare = 2,      // 公平共享
  kLeastLoaded = 3     // 最小负载
};

inline const char* SchedulingPolicyToString(SchedulingPolicy policy) {
  switch (policy) {
    case SchedulingPolicy::kFIFO:
      return "FIFO";
    case SchedulingPolicy::kPriority:
      return "Priority";
    case SchedulingPolicy::kFairShare:
      return "FairShare";
    case SchedulingPolicy::kLeastLoaded:
      return "LeastLoaded";
    default:
      return "Unknown";
  }
}

// ============================================================================
// 调度决策
// ============================================================================
struct SchedulingDecision {
  std::string task_id;
  std::string worker_id;
  bool should_schedule = false;
  std::string reason;
};

// ============================================================================
// 调度器配置
// ============================================================================
struct SchedulerConfig {
  SchedulingPolicy policy = SchedulingPolicy::kPriority;
  uint32_t max_pending_tasks = 10000;        // 最大待处理任务数
  uint32_t scheduling_interval_ms = 100;     // 调度间隔
  uint32_t task_timeout_sec = 3600;          // 任务超时时间
  uint32_t max_retries = 3;                  // 最大重试次数
  bool enable_preemption = false;            // 是否启用抢占

  // CaaS-LSM 特有配置
  uint32_t max_accumulation_in_procp = 100;  // 队列积压阈值，超过则 fallback
  uint32_t max_reschedule = 5;               // 最大重调度次数
  double min_memory_free_ratio = 0.3;        // CSA 最小内存空闲率
  uint32_t csa_status_check_interval_sec = 5;// CSA 状态检查间隔
};

// ============================================================================
// 任务完成回调
// ============================================================================
using TaskCompletedCallback =
  std::function<void(const std::string& task_id, const TaskResult& result)>;

// ============================================================================
// Task Scheduler - 任务调度器
// ============================================================================
class TaskScheduler {
 public:
  explicit TaskScheduler(const SchedulerConfig& config);
  ~TaskScheduler();

  // 禁止拷贝
  TaskScheduler(const TaskScheduler&) = delete;
  TaskScheduler& operator=(const TaskScheduler&) = delete;

  // 生命周期管理
  void Start();
  void Stop();
  bool IsRunning() const {
    return running_.load();
  }

  // 设置 Worker Manager
  void SetWorkerManager(std::shared_ptr<WorkerManager> worker_manager);

  // =========================================================================
  // 任务管理
  // =========================================================================

  // 提交任务
  std::string SubmitTask(const TaskInfo& task_info);

  // 取消任务
  bool CancelTask(const std::string& task_id, const std::string& reason = "");

  // 任务完成通知 (由 Worker 上报结果时调用)
  void OnTaskCompleted(const std::string& task_id, const TaskResult& result);

  // 任务失败通知
  void OnTaskFailed(const std::string& task_id, const std::string& error);

  // =========================================================================
  // 任务查询
  // =========================================================================

  std::shared_ptr<TaskInfo> GetTask(const std::string& task_id) const;
  std::vector<std::shared_ptr<TaskInfo>> QueryTasks(
    const TaskFilter& filter) const;

  // 等待任务完成
  // 返回 true 表示任务已完成，false 表示超时
  bool WaitForTask(const std::string& task_id,
                   TaskResult* result,
                   uint32_t timeout_ms);

  // =========================================================================
  // 统计信息
  // =========================================================================

  const TaskStatistics& GetStatistics() const {
    return statistics_;
  }
  size_t GetPendingCount() const;
  size_t GetRunningCount() const;

  // =========================================================================
  // 回调注册
  // =========================================================================

  void RegisterCompletedCallback(TaskCompletedCallback callback);

 private:
  void ScheduleLoop();
  void TimeoutCheckLoop();

  // CaaS-LSM: CSA 状态检查线程
  void CSAStatusCheckLoop();

  // 调度实现
  std::vector<SchedulingDecision> DoSchedule();
  void ExecuteDecisions(const std::vector<SchedulingDecision>& decisions);

  // 选择 Worker
  std::shared_ptr<WorkerInfo> SelectWorker(const TaskInfo& task);

  // 任务重试
  void HandleRetry(std::shared_ptr<TaskInfo> task);

  // CaaS-LSM: 检查是否应该降级
  bool ShouldFallback(const std::shared_ptr<TaskInfo>& task);

  // CaaS-LSM: 处理降级
  void HandleFallback(std::shared_ptr<TaskInfo> task, const std::string& reason);

  // CaaS-LSM: 任务分发到 CSA (推送模式)
  bool DistributeTaskToCSA(const std::string& worker_id,
                           std::shared_ptr<TaskInfo> task);

  // ID 生成
  std::string GenerateTaskId();

  // 通知任务完成
  void NotifyTaskCompleted(const std::string& task_id, const TaskResult& result);

  SchedulerConfig config_;
  std::atomic<bool> running_{false};

  // Worker Manager
  std::shared_ptr<WorkerManager> worker_manager_;

  // 待处理任务队列 (按 CaaS-LSM 策略排序)
  // CaaS-LSM 排序规则: 优先处理 start_level 较低的任务，同层级按 score 排序
  struct TaskComparator {
    bool operator()(const std::shared_ptr<TaskInfo>& a,
                    const std::shared_ptr<TaskInfo>& b) const {
      // CaaS-LSM 风格：先按 start_level 排序（越低优先级越高）
      if (a->params.start_level != b->params.start_level) {
        return a->params.start_level > b->params.start_level;  // 小的在前
      }
      // 同层级按 score 排序（越高优先级越高）
      if (a->params.score != b->params.score) {
        return a->params.score < b->params.score;  // 大的在前
      }
      // 最后按提交时间排序
      return a->submit_time > b->submit_time;
    }
  };
  std::priority_queue<std::shared_ptr<TaskInfo>,
                      std::vector<std::shared_ptr<TaskInfo>>,
                      TaskComparator>
    pending_queue_;
  mutable std::mutex pending_mutex_;

  // 所有任务索引 (用于查询)
  std::unordered_map<std::string, std::shared_ptr<TaskInfo>> all_tasks_;
  mutable std::mutex tasks_mutex_;

  // 运行中的任务
  std::unordered_map<std::string, std::shared_ptr<TaskInfo>> running_tasks_;
  mutable std::mutex running_mutex_;

  // 任务完成等待
  std::unordered_map<std::string, std::condition_variable> task_cv_map_;
  std::mutex task_cv_mutex_;

  // 回调
  std::vector<TaskCompletedCallback> completed_callbacks_;
  std::mutex callbacks_mutex_;

  // 线程
  std::unique_ptr<std::thread> scheduler_thread_;
  std::unique_ptr<std::thread> timeout_thread_;
  std::unique_ptr<std::thread> csa_status_thread_;  // CaaS-LSM: CSA 状态检查线程
  std::condition_variable cv_;
  mutable std::mutex cv_mutex_;

  // CaaS-LSM: 已完成任务结果存储 (供客户端查询)
  std::unordered_map<std::string, TaskResult> completed_results_;
  mutable std::mutex results_mutex_;

  // 统计
  TaskStatistics statistics_;
  std::atomic<uint64_t> task_id_counter_{0};
};

}  // namespace control_plane
}  // namespace tendisplus
