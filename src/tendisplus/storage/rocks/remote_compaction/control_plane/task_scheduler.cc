// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include "task_scheduler.h"

#include <iostream>
#include <sstream>

namespace tendisplus {
namespace control_plane {

TaskScheduler::TaskScheduler(const SchedulerConfig& config) : config_(config) {}

TaskScheduler::~TaskScheduler() {
  Stop();
}

void TaskScheduler::Start() {
  if (running_.exchange(true)) {
    return;
  }

  // 启动调度线程
  scheduler_thread_ = std::make_unique<std::thread>([this]() {
    ScheduleLoop();
  });

  // 启动超时检查线程
  timeout_thread_ = std::make_unique<std::thread>([this]() {
    TimeoutCheckLoop();
  });

  // CaaS-LSM: 启动 CSA 状态检查线程
  csa_status_thread_ = std::make_unique<std::thread>([this]() {
    CSAStatusCheckLoop();
  });

  std::cout << "[TaskScheduler] Started with policy: "
            << SchedulingPolicyToString(config_.policy) << std::endl;
}

void TaskScheduler::Stop() {
  if (!running_.exchange(false)) {
    return;
  }

  {
    std::lock_guard<std::mutex> lock(cv_mutex_);
    cv_.notify_all();
  }

  if (scheduler_thread_ && scheduler_thread_->joinable()) {
    scheduler_thread_->join();
  }
  if (timeout_thread_ && timeout_thread_->joinable()) {
    timeout_thread_->join();
  }
  // CaaS-LSM: 停止 CSA 状态检查线程
  if (csa_status_thread_ && csa_status_thread_->joinable()) {
    csa_status_thread_->join();
  }

  std::cout << "[TaskScheduler] Stopped" << std::endl;
}

void TaskScheduler::SetWorkerManager(
  std::shared_ptr<WorkerManager> worker_manager) {
  worker_manager_ = std::move(worker_manager);
}

std::string TaskScheduler::SubmitTask(const TaskInfo& task_info) {
  auto task = std::make_shared<TaskInfo>(task_info);

  // 生成任务 ID
  if (task->task_id.empty()) {
    task->task_id = GenerateTaskId();
  }

  // 设置时间
  task->submit_time = std::chrono::system_clock::now();
  task->status = TaskStatus::kPending;

  // 检查队列限制
  {
    std::lock_guard<std::mutex> lock(pending_mutex_);
    if (pending_queue_.size() >= config_.max_pending_tasks) {
      std::cerr << "[TaskScheduler] Task queue full, rejecting task"
                << std::endl;
      return "";
    }
    pending_queue_.push(task);
  }

  // 添加到索引
  {
    std::lock_guard<std::mutex> lock(tasks_mutex_);
    all_tasks_[task->task_id] = task;
  }

  // 更新统计
  statistics_.total_submitted++;
  statistics_.pending_count++;

  std::cout << "[TaskScheduler] Task submitted: " << task->ToString()
            << std::endl;

  // 唤醒调度线程
  cv_.notify_one();

  return task->task_id;
}

bool TaskScheduler::CancelTask(const std::string& task_id,
                               const std::string& reason) {
  std::shared_ptr<TaskInfo> task;

  // 查找任务
  {
    std::lock_guard<std::mutex> lock(tasks_mutex_);
    auto it = all_tasks_.find(task_id);
    if (it == all_tasks_.end()) {
      return false;
    }
    task = it->second;
  }

  // 检查状态
  if (task->IsTerminal()) {
    return false;  // 已经结束的任务不能取消
  }

  // 更新状态
  task->status = TaskStatus::kCancelled;
  task->error_message = reason;
  task->complete_time = std::chrono::system_clock::now();

  // 从运行队列移除
  if (!task->assigned_worker_id.empty()) {
    std::lock_guard<std::mutex> lock(running_mutex_);
    running_tasks_.erase(task_id);

    // 通知 Worker Manager 释放任务
    if (worker_manager_) {
      worker_manager_->ReleaseTask(task->assigned_worker_id, task_id, false);
    }
  }

  // 更新统计
  statistics_.total_cancelled++;
  if (task->status == TaskStatus::kPending) {
    statistics_.pending_count--;
  } else {
    statistics_.running_count--;
  }

  std::cout << "[TaskScheduler] Task cancelled: " << task_id
            << ", reason: " << reason << std::endl;

  // 通知等待者
  NotifyTaskCompleted(task_id, task->result);

  return true;
}

void TaskScheduler::OnTaskCompleted(const std::string& task_id,
                                    const TaskResult& result) {
  std::shared_ptr<TaskInfo> task;

  {
    std::lock_guard<std::mutex> lock(tasks_mutex_);
    auto it = all_tasks_.find(task_id);
    if (it == all_tasks_.end()) {
      std::cerr << "[TaskScheduler] Unknown task completed: " << task_id
                << std::endl;
      return;
    }
    task = it->second;
  }

  // 从运行队列移除
  {
    std::lock_guard<std::mutex> lock(running_mutex_);
    running_tasks_.erase(task_id);
  }

  // 通知 Worker Manager
  if (worker_manager_ && !task->assigned_worker_id.empty()) {
    worker_manager_->ReleaseTask(
      task->assigned_worker_id, task_id, result.success);
  }

  // 更新任务状态
  task->result = result;
  task->complete_time = std::chrono::system_clock::now();

  if (result.success) {
    task->status = TaskStatus::kCompleted;
    statistics_.total_completed++;
    statistics_.total_execution_time_ms += task->GetExecutionTimeMs();
    statistics_.total_queue_time_ms += task->GetQueueTimeMs();
  } else {
    // 检查是否需要重试
    if (task->CanRetry()) {
      HandleRetry(task);
      return;
    }
    task->status = TaskStatus::kFailed;
    task->error_message = result.error_message;
    statistics_.total_failed++;
  }

  statistics_.running_count--;

  std::cout << "[TaskScheduler] Task completed: " << task_id
            << ", success=" << result.success << std::endl;

  // 通知等待者
  NotifyTaskCompleted(task_id, result);
}

void TaskScheduler::OnTaskFailed(const std::string& task_id,
                                 const std::string& error) {
  TaskResult result;
  result.success = false;
  result.error_message = error;
  OnTaskCompleted(task_id, result);
}

std::shared_ptr<TaskInfo> TaskScheduler::GetTask(
  const std::string& task_id) const {
  std::lock_guard<std::mutex> lock(tasks_mutex_);
  auto it = all_tasks_.find(task_id);
  if (it != all_tasks_.end()) {
    return it->second;
  }
  return nullptr;
}

std::vector<std::shared_ptr<TaskInfo>> TaskScheduler::QueryTasks(
  const TaskFilter& filter) const {
  std::vector<std::shared_ptr<TaskInfo>> result;
  std::lock_guard<std::mutex> lock(tasks_mutex_);

  for (const auto& pair : all_tasks_) {
    if (filter.Matches(*pair.second)) {
      result.push_back(pair.second);
      if (result.size() >= filter.limit) {
        break;
      }
    }
  }

  return result;
}

bool TaskScheduler::WaitForTask(const std::string& task_id,
                                TaskResult* result,
                                uint32_t timeout_ms) {
  auto task = GetTask(task_id);
  if (!task) {
    return false;
  }

  // 如果已完成，直接返回
  if (task->IsTerminal()) {
    if (result) {
      *result = task->result;
    }
    return true;
  }

  // 等待完成
  std::unique_lock<std::mutex> lock(task_cv_mutex_);
  auto& cv = task_cv_map_[task_id];

  bool completed = false;
  if (timeout_ms > 0) {
    completed = cv.wait_for(lock, std::chrono::milliseconds(timeout_ms), [&]() {
      auto t = GetTask(task_id);
      return t && t->IsTerminal();
    });
  } else {
    // 立即返回当前状态
    task = GetTask(task_id);
    completed = task && task->IsTerminal();
  }

  if (completed && result) {
    task = GetTask(task_id);
    if (task) {
      *result = task->result;
    }
  }

  return completed;
}

size_t TaskScheduler::GetPendingCount() const {
  std::lock_guard<std::mutex> lock(pending_mutex_);
  return pending_queue_.size();
}

size_t TaskScheduler::GetRunningCount() const {
  std::lock_guard<std::mutex> lock(running_mutex_);
  return running_tasks_.size();
}

void TaskScheduler::RegisterCompletedCallback(TaskCompletedCallback callback) {
  std::lock_guard<std::mutex> lock(callbacks_mutex_);
  completed_callbacks_.push_back(std::move(callback));
}

void TaskScheduler::ScheduleLoop() {
  std::cout << "[TaskScheduler] Scheduler thread started" << std::endl;

  while (running_.load()) {
    // 执行调度
    auto decisions = DoSchedule();
    if (!decisions.empty()) {
      ExecuteDecisions(decisions);
    }

    // 等待
    {
      std::unique_lock<std::mutex> lock(cv_mutex_);
      cv_.wait_for(
        lock,
        std::chrono::milliseconds(config_.scheduling_interval_ms),
        [this]() { return !running_.load(); });
    }
  }

  std::cout << "[TaskScheduler] Scheduler thread stopped" << std::endl;
}

void TaskScheduler::TimeoutCheckLoop() {
  std::cout << "[TaskScheduler] Timeout check thread started" << std::endl;

  while (running_.load()) {
    auto now = std::chrono::system_clock::now();

    // 检查运行中的任务
    std::vector<std::string> timeout_tasks;
    {
      std::lock_guard<std::mutex> lock(running_mutex_);
      for (const auto& pair : running_tasks_) {
        auto& task = pair.second;
        auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                         now - task->start_time)
                         .count();
        if (elapsed > config_.task_timeout_sec) {
          timeout_tasks.push_back(task->task_id);
        }
      }
    }

    // 处理超时任务
    for (const auto& task_id : timeout_tasks) {
      std::cout << "[TaskScheduler] Task timeout: " << task_id << std::endl;

      auto task = GetTask(task_id);
      if (task) {
        task->status = TaskStatus::kTimeout;
        statistics_.total_timeout++;

        // 检查是否需要重试
        if (task->CanRetry()) {
          HandleRetry(task);
        } else {
          OnTaskFailed(task_id, "Task timeout");
        }
      }
    }

    // 等待
    {
      std::unique_lock<std::mutex> lock(cv_mutex_);
      cv_.wait_for(lock, std::chrono::seconds(10), [this]() {
        return !running_.load();
      });
    }
  }

  std::cout << "[TaskScheduler] Timeout check thread stopped" << std::endl;
}

// CaaS-LSM: CSA 状态检查线程
void TaskScheduler::CSAStatusCheckLoop() {
  std::cout << "[TaskScheduler] CSA status check thread started" << std::endl;

  while (running_.load()) {
    if (worker_manager_) {
      // 主动检查所有 CSA 的状态
      auto workers = worker_manager_->GetAllWorkers();
      for (const auto& worker : workers) {
        if (worker->status == WorkerStatus::kOnline) {
          // 检查内存空闲率
          double memory_free_ratio = worker->GetMemoryFreeRatio();
          if (memory_free_ratio < config_.min_memory_free_ratio) {
            std::cout << "[TaskScheduler] Worker " << worker->worker_id
                      << " memory low (free ratio: " << memory_free_ratio
                      << " < " << config_.min_memory_free_ratio << ")"
                      << std::endl;
          }
        }
      }

      // 检查离线的 Worker 上的任务，重新调度
      std::vector<std::string> tasks_to_reschedule;
      {
        std::lock_guard<std::mutex> lock(running_mutex_);
        for (const auto& pair : running_tasks_) {
          auto& task = pair.second;
          auto worker = worker_manager_->GetWorker(task->assigned_worker_id);
          if (!worker || worker->status == WorkerStatus::kOffline) {
            tasks_to_reschedule.push_back(task->task_id);
          }
        }
      }

      // 重新调度这些任务
      for (const auto& task_id : tasks_to_reschedule) {
        auto task = GetTask(task_id);
        if (task) {
          std::cout << "[TaskScheduler] Worker offline, rescheduling task: "
                    << task_id << std::endl;
          task->reschedule_count++;
          if (ShouldFallback(task)) {
            HandleFallback(task, "Worker offline and reschedule limit reached");
          } else {
            HandleRetry(task);
          }
        }
      }
    }

    // 等待
    {
      std::unique_lock<std::mutex> lock(cv_mutex_);
      cv_.wait_for(
        lock,
        std::chrono::seconds(config_.csa_status_check_interval_sec),
        [this]() { return !running_.load(); });
    }
  }

  std::cout << "[TaskScheduler] CSA status check thread stopped" << std::endl;
}

std::vector<SchedulingDecision> TaskScheduler::DoSchedule() {
  std::vector<SchedulingDecision> decisions;

  if (!worker_manager_) {
    return decisions;
  }

  // CaaS-LSM: 检查队列积压是否超过阈值
  size_t pending_size = 0;
  {
    std::lock_guard<std::mutex> lock(pending_mutex_);
    pending_size = pending_queue_.size();
  }

  // 获取可用 Worker (CaaS-LSM: 需要检查内存空闲率)
  auto available_workers = worker_manager_->GetAvailableWorkers();
  
  // CaaS-LSM: 过滤掉内存不足的 Worker
  available_workers.erase(
    std::remove_if(available_workers.begin(), available_workers.end(),
                   [this](const std::shared_ptr<WorkerInfo>& w) {
                     return w->GetMemoryFreeRatio() < config_.min_memory_free_ratio;
                   }),
    available_workers.end());

  // 从队列中取出任务进行调度
  std::lock_guard<std::mutex> lock(pending_mutex_);

  // CaaS-LSM: 处理需要降级的任务
  std::vector<std::shared_ptr<TaskInfo>> fallback_tasks;

  while (!pending_queue_.empty()) {
    auto task = pending_queue_.top();

    // CaaS-LSM: 检查是否应该降级
    if (ShouldFallback(task)) {
      pending_queue_.pop();
      fallback_tasks.push_back(task);
      continue;
    }

    // 如果没有可用 Worker，停止调度
    if (available_workers.empty()) {
      break;
    }

    // 选择 Worker
    auto worker = SelectWorker(*task);
    if (!worker) {
      break;  // 没有可用 Worker
    }

    pending_queue_.pop();

    SchedulingDecision decision;
    decision.task_id = task->task_id;
    decision.worker_id = worker->worker_id;
    decision.should_schedule = true;

    decisions.push_back(decision);

    // 更新可用 Worker 列表
    available_workers.erase(
      std::remove_if(available_workers.begin(),
                     available_workers.end(),
                     [&worker](const std::shared_ptr<WorkerInfo>& w) {
                       return w->worker_id == worker->worker_id &&
                              w->resources.AvailableSlots() <= 1;
                     }),
      available_workers.end());
  }

  // 在锁外处理降级任务
  // 注意：这里需要释放锁后处理
  for (auto& task : fallback_tasks) {
    HandleFallback(task, "Fallback condition met");
  }

  return decisions;
}

void TaskScheduler::ExecuteDecisions(
  const std::vector<SchedulingDecision>& decisions) {
  for (const auto& decision : decisions) {
    if (!decision.should_schedule) {
      continue;
    }

    auto task = GetTask(decision.task_id);
    if (!task) {
      continue;
    }

    // 分配给 Worker
    if (worker_manager_->AssignTask(decision.worker_id, decision.task_id)) {
      task->assigned_worker_id = decision.worker_id;
      task->assign_time = std::chrono::system_clock::now();
      task->status = TaskStatus::kAssigned;

      // 添加到运行队列
      {
        std::lock_guard<std::mutex> lock(running_mutex_);
        running_tasks_[task->task_id] = task;
      }

      statistics_.pending_count--;
      statistics_.running_count++;

      std::cout << "[TaskScheduler] Task assigned: " << task->task_id
                << " -> " << decision.worker_id << std::endl;
    } else {
      // 分配失败，重新入队
      std::lock_guard<std::mutex> lock(pending_mutex_);
      pending_queue_.push(task);
    }
  }
}

std::shared_ptr<WorkerInfo> TaskScheduler::SelectWorker(const TaskInfo& task) {
  if (!worker_manager_) {
    return nullptr;
  }

  // 使用 WorkerManager 的选择策略
  return worker_manager_->SelectBestWorker();
}

void TaskScheduler::HandleRetry(std::shared_ptr<TaskInfo> task) {
  task->retry_count++;

  // CaaS-LSM: 先标记为 Retrying 状态，表示正在等待重新调度
  task->status = TaskStatus::kRetrying;
  task->assigned_worker_id.clear();

  std::cout << "[TaskScheduler] Task retrying: " << task->task_id
            << " (attempt " << task->retry_count << "/" << task->max_retries
            << ")" << std::endl;

  // 从运行队列移除（如果存在）
  {
    std::lock_guard<std::mutex> lock(running_mutex_);
    running_tasks_.erase(task->task_id);
  }

  // 转为 Pending 状态并重新入队
  task->status = TaskStatus::kPending;

  {
    std::lock_guard<std::mutex> lock(pending_mutex_);
    pending_queue_.push(task);
  }

  statistics_.pending_count++;

  // 唤醒调度线程
  cv_.notify_one();
}

// CaaS-LSM: 检查是否应该降级到本地执行
bool TaskScheduler::ShouldFallback(const std::shared_ptr<TaskInfo>& task) {
  if (!worker_manager_) {
    return true;  // 没有 WorkerManager，直接降级
  }

  // 条件 1: 没有可用的 CSA
  if (worker_manager_->GetOnlineWorkerCount() == 0) {
    std::cout << "[TaskScheduler] Fallback: No available CSA for task "
              << task->task_id << std::endl;
    return true;
  }

  // 条件 2: 队列积压超过阈值
  size_t pending_size = 0;
  {
    std::lock_guard<std::mutex> lock(pending_mutex_);
    pending_size = pending_queue_.size();
  }
  if (pending_size > config_.max_accumulation_in_procp) {
    std::cout << "[TaskScheduler] Fallback: Queue accumulation ("
              << pending_size << " > " << config_.max_accumulation_in_procp
              << ") for task " << task->task_id << std::endl;
    return true;
  }

  // 条件 3: 重调度次数超过限制
  if (task->reschedule_count >= static_cast<int32_t>(config_.max_reschedule)) {
    std::cout << "[TaskScheduler] Fallback: Reschedule limit reached ("
              << task->reschedule_count << " >= " << config_.max_reschedule
              << ") for task " << task->task_id << std::endl;
    return true;
  }

  return false;
}

// CaaS-LSM: 处理降级
void TaskScheduler::HandleFallback(std::shared_ptr<TaskInfo> task,
                                   const std::string& reason) {
  task->should_fallback = true;
  task->status = TaskStatus::kFailed;
  task->error_message = "Fallback to local: " + reason;
  task->complete_time = std::chrono::system_clock::now();

  // 设置结果
  task->result.success = false;
  task->result.error_message = task->error_message;

  // 存储结果供客户端查询
  {
    std::lock_guard<std::mutex> lock(results_mutex_);
    completed_results_[task->task_id] = task->result;
  }

  statistics_.total_failed++;
  statistics_.pending_count--;

  std::cout << "[TaskScheduler] Task fallback: " << task->task_id
            << ", reason: " << reason << std::endl;

  // 通知等待者 (客户端可以根据 should_fallback 标志执行本地 compaction)
  NotifyTaskCompleted(task->task_id, task->result);
}

// CaaS-LSM: 任务分发到 CSA (推送模式)
bool TaskScheduler::DistributeTaskToCSA(const std::string& worker_id,
                                        std::shared_ptr<TaskInfo> task) {
  if (!worker_manager_) {
    return false;
  }

  auto worker = worker_manager_->GetWorker(worker_id);
  if (!worker) {
    std::cerr << "[TaskScheduler] Worker not found: " << worker_id << std::endl;
    return false;
  }

  // 检查 Worker 状态
  if (worker->status != WorkerStatus::kOnline) {
    std::cerr << "[TaskScheduler] Worker not online: " << worker_id << std::endl;
    return false;
  }

  // 检查内存空闲率
  if (worker->GetMemoryFreeRatio() < config_.min_memory_free_ratio) {
    std::cerr << "[TaskScheduler] Worker memory low: " << worker_id
              << " (free ratio: " << worker->GetMemoryFreeRatio() << ")"
              << std::endl;
    return false;
  }

  // 通过 WorkerManager 调用 CSA 的 DistributeCompactionJob gRPC 接口（推送模式）
  bool success = worker_manager_->DistributeJobToCSA(
    worker_id,
    task->task_id,
    task->params.compaction_args,
    task->params.compaction_addition_info,
    task->params.shared_fs_uri,
    task->params.start_level,
    task->params.score);

  if (!success) {
    std::cerr << "[TaskScheduler] Failed to distribute task to CSA: "
              << task->task_id << " -> " << worker_id << std::endl;
    return false;
  }

  // 更新任务状态为 Running
  task->start_time = std::chrono::system_clock::now();
  task->status = TaskStatus::kRunning;

  std::cout << "[TaskScheduler] Task distributed to CSA: " << task->task_id
            << " -> " << worker_id << std::endl;

  return true;
}

std::string TaskScheduler::GenerateTaskId() {
  uint64_t id = task_id_counter_.fetch_add(1);
  auto now = std::chrono::system_clock::now();
  auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
              now.time_since_epoch())
              .count();
  std::ostringstream oss;
  oss << "task_" << ms << "_" << id;
  return oss.str();
}

void TaskScheduler::NotifyTaskCompleted(const std::string& task_id,
                                        const TaskResult& result) {
  // 通知等待者
  {
    std::lock_guard<std::mutex> lock(task_cv_mutex_);
    auto it = task_cv_map_.find(task_id);
    if (it != task_cv_map_.end()) {
      it->second.notify_all();
    }
  }

  // 调用回调
  std::vector<TaskCompletedCallback> callbacks;
  {
    std::lock_guard<std::mutex> lock(callbacks_mutex_);
    callbacks = completed_callbacks_;
  }

  for (const auto& callback : callbacks) {
    if (callback) {
      callback(task_id, result);
    }
  }
}

}  // namespace control_plane
}  // namespace tendisplus
