// Copyright (c) 2024-present, Tencent Inc. All rights reserved.
// 任务调度器实现

#include "task_scheduler.h"
#include "task_state_machine.h"

#include <algorithm>
#include <iomanip>
#include <iostream>
#include <sstream>

namespace tendisplus {
namespace control_plane {

// ============================================================================
// TaskQueue 实现
// ============================================================================

TaskQueue::TaskQueue() = default;

bool TaskQueue::TaskComparator::operator()(
    const std::shared_ptr<BackgroundTask>& a,
    const std::shared_ptr<BackgroundTask>& b) const {
  // 优先级高的排前面
  if (a->priority != b->priority) {
    return static_cast<int>(a->priority) < static_cast<int>(b->priority);
  }
  // 同优先级按创建时间排序 (FIFO)
  return a->created_at > b->created_at;
}

void TaskQueue::Enqueue(std::shared_ptr<BackgroundTask> task) {
  std::lock_guard<std::mutex> lock(mutex_);
  task_map_[task->task_id] = task;
  queue_.push(task);
}

std::shared_ptr<BackgroundTask> TaskQueue::Dequeue() {
  std::lock_guard<std::mutex> lock(mutex_);
  
  while (!queue_.empty()) {
    auto task = queue_.top();
    queue_.pop();
    
    // 检查任务是否仍在 map 中 (可能已被移除)
    auto it = task_map_.find(task->task_id);
    if (it != task_map_.end()) {
      task_map_.erase(it);
      return task;
    }
  }
  return nullptr;
}

std::shared_ptr<BackgroundTask> TaskQueue::Peek() const {
  std::lock_guard<std::mutex> lock(mutex_);
  
  if (queue_.empty()) {
    return nullptr;
  }
  return queue_.top();
}

bool TaskQueue::Remove(const std::string& task_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  auto it = task_map_.find(task_id);
  if (it != task_map_.end()) {
    task_map_.erase(it);
    // 注意：优先队列不支持直接删除，任务会在 Dequeue 时被跳过
    return true;
  }
  return false;
}

std::shared_ptr<BackgroundTask> TaskQueue::Find(const std::string& task_id) const {
  std::lock_guard<std::mutex> lock(mutex_);
  
  auto it = task_map_.find(task_id);
  if (it != task_map_.end()) {
    return it->second;
  }
  return nullptr;
}

bool TaskQueue::UpdatePriority(const std::string& task_id, 
                               TaskPriority new_priority) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  auto it = task_map_.find(task_id);
  if (it != task_map_.end()) {
    it->second->priority = new_priority;
    // 注意：优先队列不会自动重新排序，需要重建队列
    // 这里简化处理，实际应该使用支持更新的数据结构
    return true;
  }
  return false;
}

size_t TaskQueue::Size() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return task_map_.size();
}

bool TaskQueue::Empty() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return task_map_.empty();
}

void TaskQueue::Clear() {
  std::lock_guard<std::mutex> lock(mutex_);
  task_map_.clear();
  while (!queue_.empty()) {
    queue_.pop();
  }
}

std::map<TaskPriority, size_t> TaskQueue::GetPriorityDistribution() const {
  std::lock_guard<std::mutex> lock(mutex_);
  
  std::map<TaskPriority, size_t> distribution;
  for (const auto& [id, task] : task_map_) {
    distribution[task->priority]++;
  }
  return distribution;
}

// ============================================================================
// TaskScheduler 实现
// ============================================================================

TaskScheduler::TaskScheduler(const SchedulerConfig& config)
    : config_(config) {}

TaskScheduler::~TaskScheduler() {
  Stop();
}

void TaskScheduler::Start() {
  if (running_.exchange(true)) {
    return;  // 已经在运行
  }
  
  std::cout << "[Scheduler] Starting task scheduler with policy: "
            << SchedulingPolicyToString(config_.policy) << std::endl;
  
  // 启动调度线程
  scheduler_thread_ = std::make_unique<std::thread>([this]() {
    ScheduleLoop();
  });
  
  // 启动超时检查线程
  timeout_thread_ = std::make_unique<std::thread>([this]() {
    TimeoutCheckLoop();
  });
}

void TaskScheduler::Stop() {
  if (!running_.exchange(false)) {
    return;  // 已经停止
  }
  
  cv_.notify_all();
  
  if (scheduler_thread_ && scheduler_thread_->joinable()) {
    scheduler_thread_->join();
  }
  if (timeout_thread_ && timeout_thread_->joinable()) {
    timeout_thread_->join();
  }
  
  std::cout << "[Scheduler] Task scheduler stopped" << std::endl;
}

std::string TaskScheduler::SubmitTask(std::shared_ptr<BackgroundTask> task) {
  // 生成任务 ID
  if (task->task_id.empty()) {
    task->task_id = GenerateTaskId();
  }
  
  // 设置创建时间
  task->created_at = std::chrono::system_clock::now();
  
  // 状态转换: Created -> Queued
  TaskStateMachine::Instance().ProcessEvent(*task, TaskEvent::kEnqueue, 
                                            "Task submitted");
  
  // 入队
  pending_queue_.Enqueue(task);
  
  total_submitted_.fetch_add(1);
  
  std::cout << "[Scheduler] Task submitted: " << task->task_id 
            << ", type: " << TaskTypeToString(task->task_type)
            << ", priority: " << TaskPriorityToString(task->priority)
            << std::endl;
  
  // 通知调度线程
  cv_.notify_one();
  
  return task->task_id;
}

bool TaskScheduler::CancelTask(const std::string& task_id, 
                               const std::string& reason) {
  // 尝试从队列中移除
  if (pending_queue_.Remove(task_id)) {
    std::cout << "[Scheduler] Task cancelled from queue: " << task_id << std::endl;
    return true;
  }
  
  // 尝试从活跃任务中取消
  {
    std::lock_guard<std::mutex> lock(active_tasks_mutex_);
    auto it = active_tasks_.find(task_id);
    if (it != active_tasks_.end()) {
      auto task = it->second;
      TaskStateMachine::Instance().ProcessEvent(*task, TaskEvent::kCancel, reason);
      
      // 移动到已完成任务
      {
        std::lock_guard<std::mutex> lock2(completed_tasks_mutex_);
        completed_tasks_[task_id] = task;
      }
      active_tasks_.erase(it);
      
      std::cout << "[Scheduler] Active task cancelled: " << task_id << std::endl;
      return true;
    }
  }
  
  return false;
}

std::shared_ptr<BackgroundTask> TaskScheduler::GetTask(
    const std::string& task_id) const {
  // 检查队列
  auto task = pending_queue_.Find(task_id);
  if (task) return task;
  
  // 检查活跃任务
  {
    std::lock_guard<std::mutex> lock(active_tasks_mutex_);
    auto it = active_tasks_.find(task_id);
    if (it != active_tasks_.end()) {
      return it->second;
    }
  }
  
  // 检查已完成任务
  {
    std::lock_guard<std::mutex> lock(completed_tasks_mutex_);
    auto it = completed_tasks_.find(task_id);
    if (it != completed_tasks_.end()) {
      return it->second;
    }
  }
  
  return nullptr;
}

std::vector<std::shared_ptr<BackgroundTask>> TaskScheduler::GetTasks(
    const TaskFilter& filter) const {
  std::vector<std::shared_ptr<BackgroundTask>> result;
  
  // 收集所有任务
  std::vector<std::shared_ptr<BackgroundTask>> all_tasks;
  
  // TODO: 从队列、活跃任务、已完成任务中收集
  // 这里简化实现
  
  return result;
}

void TaskScheduler::OnTaskCompleted(const std::string& task_id, 
                                    const TaskResult& result) {
  std::lock_guard<std::mutex> lock(active_tasks_mutex_);
  
  auto it = active_tasks_.find(task_id);
  if (it == active_tasks_.end()) {
    std::cerr << "[Scheduler] Task not found for completion: " << task_id << std::endl;
    return;
  }
  
  auto task = it->second;
  task->result = result;
  
  if (result.success) {
    TaskStateMachine::Instance().ProcessEvent(*task, TaskEvent::kComplete,
                                              "Task completed successfully");
    total_completed_.fetch_add(1);
  } else {
    TaskStateMachine::Instance().ProcessEvent(*task, TaskEvent::kFail,
                                              result.error_message);
    total_failed_.fetch_add(1);
    
    // 检查是否需要重试
    if (task->CanRetry()) {
      HandleRetry(task);
    }
  }
  
  // 释放 Worker 资源
  if (worker_manager_ && !task->assigned_worker_id.empty()) {
    worker_manager_->ReleaseTaskFromWorker(
        task->assigned_worker_id, task_id, task->resource_requirement);
  }
  
  // 移动到已完成任务
  {
    std::lock_guard<std::mutex> lock2(completed_tasks_mutex_);
    completed_tasks_[task_id] = task;
  }
  active_tasks_.erase(it);
  
  std::cout << "[Scheduler] Task " << task_id << " completed: " 
            << (result.success ? "success" : "failed") << std::endl;
}

void TaskScheduler::OnTaskFailed(const std::string& task_id, 
                                 const std::string& error) {
  TaskResult result;
  result.success = false;
  result.error_message = error;
  OnTaskCompleted(task_id, result);
}

void TaskScheduler::SetWorkerManager(std::shared_ptr<WorkerManager> worker_manager) {
  worker_manager_ = std::move(worker_manager);
}

void TaskScheduler::RegisterAssignCallback(TaskAssignCallback callback) {
  assign_callbacks_.push_back(std::move(callback));
}

TaskStatistics TaskScheduler::GetStatistics() const {
  TaskStatistics stats;
  
  stats.total_tasks = total_submitted_.load();
  stats.completed_tasks = total_completed_.load();
  stats.failed_tasks = total_failed_.load();
  stats.active_tasks = GetActiveTaskCount();
  
  // TODO: 计算更多统计信息
  
  return stats;
}

size_t TaskScheduler::GetQueueLength() const {
  return pending_queue_.Size();
}

size_t TaskScheduler::GetActiveTaskCount() const {
  std::lock_guard<std::mutex> lock(active_tasks_mutex_);
  return active_tasks_.size();
}

void TaskScheduler::UpdateConfig(const SchedulerConfig& config) {
  std::lock_guard<std::mutex> lock(mutex_);
  config_ = config;
}

void TaskScheduler::ScheduleLoop() {
  while (running_.load()) {
    std::vector<SchedulingDecision> decisions;
    
    // 根据调度策略生成调度决策
    switch (config_.policy) {
      case SchedulingPolicy::kFIFO:
        decisions = ScheduleFIFO();
        break;
      case SchedulingPolicy::kPriority:
        decisions = SchedulePriority();
        break;
      case SchedulingPolicy::kFairShare:
        decisions = ScheduleFairShare();
        break;
      case SchedulingPolicy::kResourceAware:
        decisions = ScheduleResourceAware();
        break;
      case SchedulingPolicy::kLocalityAware:
        decisions = ScheduleLocalityAware();
        break;
    }
    
    // 执行调度决策
    if (!decisions.empty()) {
      ExecuteDecisions(decisions);
    }
    
    // 等待下一个调度周期
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait_for(lock, std::chrono::milliseconds(config_.scheduling_interval_ms),
                 [this]() { return !running_.load(); });
  }
}

void TaskScheduler::TimeoutCheckLoop() {
  while (running_.load()) {
    auto now = std::chrono::system_clock::now();
    
    std::vector<std::string> timed_out_tasks;
    
    {
      std::lock_guard<std::mutex> lock(active_tasks_mutex_);
      for (const auto& [task_id, task] : active_tasks_) {
        if (task->deadline.has_value() && now > task->deadline.value()) {
          timed_out_tasks.push_back(task_id);
        } else {
          // 检查执行超时
          auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
              now - task->started_at).count();
          if (elapsed > config_.task_timeout_sec) {
            timed_out_tasks.push_back(task_id);
          }
        }
      }
    }
    
    // 处理超时任务
    for (const auto& task_id : timed_out_tasks) {
      std::cout << "[Scheduler] Task timeout: " << task_id << std::endl;
      OnTaskFailed(task_id, "Task execution timeout");
    }
    
    // 等待下一次检查
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
}

std::vector<SchedulingDecision> TaskScheduler::ScheduleFIFO() {
  return SchedulePriority();  // FIFO 是优先级调度的特例
}

std::vector<SchedulingDecision> TaskScheduler::SchedulePriority() {
  std::vector<SchedulingDecision> decisions;
  
  if (!worker_manager_) {
    return decisions;
  }
  
  // 检查是否达到最大并发
  if (GetActiveTaskCount() >= config_.max_concurrent_tasks) {
    return decisions;
  }
  
  // 获取可用 Worker
  auto available_workers = worker_manager_->GetAvailableWorkers();
  if (available_workers.empty()) {
    return decisions;
  }
  
  // 从队列中取出任务并调度
  while (!pending_queue_.Empty() && 
         GetActiveTaskCount() < config_.max_concurrent_tasks) {
    auto task = pending_queue_.Dequeue();
    if (!task) break;
    
    // 选择最佳 Worker
    auto worker_id = SelectWorker(*task, available_workers);
    if (!worker_id.has_value()) {
      // 没有合适的 Worker，放回队列
      pending_queue_.Enqueue(task);
      break;
    }
    
    SchedulingDecision decision;
    decision.task_id = task->task_id;
    decision.worker_id = worker_id.value();
    decision.should_schedule = true;
    decision.decision_time = std::chrono::system_clock::now();
    decisions.push_back(decision);
    
    // 更新 Worker 状态
    worker_manager_->AssignTaskToWorker(
        worker_id.value(), task->task_id, task->resource_requirement);
    
    // 更新任务状态
    task->assigned_worker_id = worker_id.value();
    TaskStateMachine::Instance().ProcessEvent(*task, TaskEvent::kSchedule,
                                              "Scheduled to " + worker_id.value());
    
    // 移动到活跃任务
    {
      std::lock_guard<std::mutex> lock(active_tasks_mutex_);
      active_tasks_[task->task_id] = task;
    }
    
    // 更新可用 Worker 列表
    available_workers = worker_manager_->GetAvailableWorkers();
  }
  
  return decisions;
}

std::vector<SchedulingDecision> TaskScheduler::ScheduleFairShare() {
  // TODO: 实现公平共享调度
  return SchedulePriority();
}

std::vector<SchedulingDecision> TaskScheduler::ScheduleResourceAware() {
  // TODO: 实现资源感知调度
  return SchedulePriority();
}

std::vector<SchedulingDecision> TaskScheduler::ScheduleLocalityAware() {
  // TODO: 实现数据本地性感知调度
  return SchedulePriority();
}

std::optional<std::string> TaskScheduler::SelectWorker(
    const BackgroundTask& task,
    const std::vector<WorkerInfo>& available_workers) {
  
  if (available_workers.empty()) {
    return std::nullopt;
  }
  
  // 简单策略：选择负载最低的 Worker
  const WorkerInfo* best_worker = nullptr;
  double best_score = std::numeric_limits<double>::max();
  
  for (const auto& worker : available_workers) {
    if (!CheckResourceRequirement(worker, task.resource_requirement)) {
      continue;
    }
    
    double score = worker.load.GetLoadScore();
    if (score < best_score) {
      best_score = score;
      best_worker = &worker;
    }
  }
  
  if (best_worker) {
    return best_worker->worker_id;
  }
  return std::nullopt;
}

bool TaskScheduler::CheckResourceRequirement(
    const WorkerInfo& worker,
    const ResourceRequirement& requirement) const {
  
  return worker.resources.AvailableCpuCores() >= requirement.cpu_cores &&
         worker.resources.AvailableMemoryMb() >= requirement.memory_mb &&
         worker.resources.AvailableDiskMb() >= requirement.disk_mb;
}

void TaskScheduler::ExecuteDecisions(
    const std::vector<SchedulingDecision>& decisions) {
  
  for (const auto& decision : decisions) {
    std::cout << "[Scheduler] Executing decision: task=" << decision.task_id
              << ", worker=" << decision.worker_id << std::endl;
    
    // 通知回调
    for (const auto& callback : assign_callbacks_) {
      callback(decision);
    }
  }
}

void TaskScheduler::HandleRetry(std::shared_ptr<BackgroundTask> task) {
  TaskStateMachine::Instance().ProcessEvent(*task, TaskEvent::kRetry,
                                            "Retry triggered");
  
  // 计算重试延迟
  uint32_t delay_ms = task->retry_policy.GetRetryDelay(task->retry_count);
  
  std::cout << "[Scheduler] Task " << task->task_id 
            << " will retry in " << delay_ms << "ms"
            << " (attempt " << task->retry_count << "/"
            << task->retry_policy.max_retries << ")" << std::endl;
  
  // 延迟后重新入队
  // TODO: 使用定时器实现延迟
  TaskStateMachine::Instance().ProcessEvent(*task, TaskEvent::kEnqueue,
                                            "Re-queued for retry");
  pending_queue_.Enqueue(task);
}

std::string TaskScheduler::GenerateTaskId() {
  auto id = task_id_counter_.fetch_add(1);
  auto now = std::chrono::system_clock::now();
  auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
      now.time_since_epoch()).count();
  
  std::ostringstream oss;
  oss << "task-" << std::hex << timestamp << "-" << std::dec << id;
  return oss.str();
}

std::string TaskStatistics::ToString() const {
  std::ostringstream oss;
  oss << "TaskStatistics{"
      << "total=" << total_tasks
      << ", active=" << active_tasks
      << ", completed=" << completed_tasks
      << ", failed=" << failed_tasks
      << ", avg_exec_time=" << std::fixed << std::setprecision(2) 
      << avg_execution_time_ms << "ms"
      << ", avg_queue_time=" << avg_queue_time_ms << "ms"
      << "}";
  return oss.str();
}

}  // namespace control_plane
}  // namespace tendisplus
