// Copyright (c) 2024-present, Tencent Inc. All rights reserved.
// 任务调度器定义

#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
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
  kFIFO = 0,              // 先进先出
  kPriority = 1,          // 优先级调度
  kFairShare = 2,         // 公平共享
  kResourceAware = 3,     // 资源感知调度
  kLocalityAware = 4      // 数据本地性感知调度
};

inline const char* SchedulingPolicyToString(SchedulingPolicy policy) {
  switch (policy) {
    case SchedulingPolicy::kFIFO: return "FIFO";
    case SchedulingPolicy::kPriority: return "Priority";
    case SchedulingPolicy::kFairShare: return "FairShare";
    case SchedulingPolicy::kResourceAware: return "ResourceAware";
    case SchedulingPolicy::kLocalityAware: return "LocalityAware";
    default: return "Unknown";
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
  std::chrono::system_clock::time_point decision_time;
};

// ============================================================================
// 调度器配置
// ============================================================================
struct SchedulerConfig {
  SchedulingPolicy policy = SchedulingPolicy::kPriority;
  uint32_t max_concurrent_tasks = 100;      // 最大并发任务数
  uint32_t scheduling_interval_ms = 100;    // 调度间隔 (毫秒)
  uint32_t task_timeout_sec = 3600;         // 任务超时时间 (秒)
  uint32_t worker_heartbeat_timeout_sec = 30;  // Worker 心跳超时
  bool enable_preemption = false;           // 是否启用抢占
  bool enable_task_affinity = true;         // 是否启用任务亲和性
  double resource_overcommit_ratio = 1.2;   // 资源超分比例
};

// ============================================================================
// 任务队列 (优先级队列)
// ============================================================================
class TaskQueue {
 public:
  TaskQueue();
  ~TaskQueue() = default;
  
  // 入队
  void Enqueue(std::shared_ptr<BackgroundTask> task);
  
  // 出队 (返回优先级最高的任务)
  std::shared_ptr<BackgroundTask> Dequeue();
  
  // 查看队首任务
  std::shared_ptr<BackgroundTask> Peek() const;
  
  // 按 ID 移除任务
  bool Remove(const std::string& task_id);
  
  // 按 ID 查找任务
  std::shared_ptr<BackgroundTask> Find(const std::string& task_id) const;
  
  // 更新任务优先级
  bool UpdatePriority(const std::string& task_id, TaskPriority new_priority);
  
  // 队列大小
  size_t Size() const;
  
  // 是否为空
  bool Empty() const;
  
  // 清空队列
  void Clear();
  
  // 获取队列中按优先级分组的任务数量
  std::map<TaskPriority, size_t> GetPriorityDistribution() const;

 private:
  struct TaskComparator {
    bool operator()(const std::shared_ptr<BackgroundTask>& a,
                    const std::shared_ptr<BackgroundTask>& b) const;
  };
  
  std::priority_queue<std::shared_ptr<BackgroundTask>,
                      std::vector<std::shared_ptr<BackgroundTask>>,
                      TaskComparator> queue_;
  std::unordered_map<std::string, std::shared_ptr<BackgroundTask>> task_map_;
  mutable std::mutex mutex_;
};

// ============================================================================
// 任务调度器
// ============================================================================
class TaskScheduler {
 public:
  using TaskAssignCallback = std::function<void(const SchedulingDecision&)>;
  
  explicit TaskScheduler(const SchedulerConfig& config);
  ~TaskScheduler();
  
  // 禁止拷贝
  TaskScheduler(const TaskScheduler&) = delete;
  TaskScheduler& operator=(const TaskScheduler&) = delete;
  
  // 启动/停止调度器
  void Start();
  void Stop();
  bool IsRunning() const { return running_.load(); }
  
  // 提交任务
  std::string SubmitTask(std::shared_ptr<BackgroundTask> task);
  
  // 取消任务
  bool CancelTask(const std::string& task_id, const std::string& reason = "");
  
  // 获取任务状态
  std::shared_ptr<BackgroundTask> GetTask(const std::string& task_id) const;
  
  // 获取任务列表
  std::vector<std::shared_ptr<BackgroundTask>> GetTasks(
      const TaskFilter& filter) const;
  
  // 任务完成回调 (由 Worker 调用)
  void OnTaskCompleted(const std::string& task_id, const TaskResult& result);
  
  // 任务失败回调
  void OnTaskFailed(const std::string& task_id, const std::string& error);
  
  // 注册 Worker 管理器
  void SetWorkerManager(std::shared_ptr<WorkerManager> worker_manager);
  
  // 注册任务分配回调
  void RegisterAssignCallback(TaskAssignCallback callback);
  
  // 获取调度器统计信息
  TaskStatistics GetStatistics() const;
  
  // 获取队列长度
  size_t GetQueueLength() const;
  
  // 获取活跃任务数
  size_t GetActiveTaskCount() const;
  
  // 更新配置
  void UpdateConfig(const SchedulerConfig& config);

 private:
  void ScheduleLoop();
  void TimeoutCheckLoop();
  
  // 调度策略实现
  std::vector<SchedulingDecision> ScheduleFIFO();
  std::vector<SchedulingDecision> SchedulePriority();
  std::vector<SchedulingDecision> ScheduleFairShare();
  std::vector<SchedulingDecision> ScheduleResourceAware();
  std::vector<SchedulingDecision> ScheduleLocalityAware();
  
  // 选择最佳 Worker
  std::optional<std::string> SelectWorker(
      const BackgroundTask& task,
      const std::vector<WorkerInfo>& available_workers);
  
  // 检查资源是否满足
  bool CheckResourceRequirement(const WorkerInfo& worker,
                                const ResourceRequirement& requirement) const;
  
  // 执行调度决策
  void ExecuteDecisions(const std::vector<SchedulingDecision>& decisions);
  
  // 处理任务重试
  void HandleRetry(std::shared_ptr<BackgroundTask> task);
  
  // 生成任务 ID
  std::string GenerateTaskId();
  
  SchedulerConfig config_;
  std::atomic<bool> running_{false};
  
  // 任务队列
  TaskQueue pending_queue_;
  
  // 活跃任务 (正在执行的任务)
  std::unordered_map<std::string, std::shared_ptr<BackgroundTask>> active_tasks_;
  mutable std::mutex active_tasks_mutex_;
  
  // 已完成任务 (用于查询历史)
  std::unordered_map<std::string, std::shared_ptr<BackgroundTask>> completed_tasks_;
  mutable std::mutex completed_tasks_mutex_;
  
  // Worker 管理器
  std::shared_ptr<WorkerManager> worker_manager_;
  
  // 回调
  std::vector<TaskAssignCallback> assign_callbacks_;
  
  // 调度线程
  std::unique_ptr<std::thread> scheduler_thread_;
  std::unique_ptr<std::thread> timeout_thread_;
  std::condition_variable cv_;
  mutable std::mutex mutex_;
  
  // 统计信息
  std::atomic<uint64_t> total_submitted_{0};
  std::atomic<uint64_t> total_completed_{0};
  std::atomic<uint64_t> total_failed_{0};
  std::atomic<uint64_t> task_id_counter_{0};
};

}  // namespace control_plane
}  // namespace tendisplus
