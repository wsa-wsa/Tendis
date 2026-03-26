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

// =========================================================================
// Bulk Load 任务管理
// =========================================================================

std::string TaskScheduler::SubmitBulkLoadTask(const TaskInfo& task_info) {
  auto task = std::make_shared<TaskInfo>(task_info);

  // 生成任务 ID
  if (task->task_id.empty()) {
    task->task_id = GenerateTaskId();
  }

  // 设置时间和类型
  task->submit_time = std::chrono::system_clock::now();
  task->status = TaskStatus::kPending;
  task->type = TaskType::kBulkLoad;

  // 添加到 Bulk Load 任务索引
  {
    std::lock_guard<std::mutex> lock(bulk_load_tasks_mutex_);
    bulk_load_tasks_[task->task_id] = task;
  }

  // 添加到全局索引
  {
    std::lock_guard<std::mutex> lock(tasks_mutex_);
    all_tasks_[task->task_id] = task;
  }

  statistics_.total_submitted++;

  std::cout << "[TaskScheduler] Bulk Load task submitted: " << task->task_id
            << " (source: " << task->bulk_load_params.source_path
            << ", shards: " << task->bulk_load_params.shard_count << ")"
            << std::endl;

  return task->task_id;
}

std::string TaskScheduler::SubmitBulkLoadShard(const TaskInfo& shard_task) {
  auto task = std::make_shared<TaskInfo>(shard_task);

  // 生成分片任务 ID
  if (task->task_id.empty()) {
    task->task_id = GenerateTaskId();
  }

  task->submit_time = std::chrono::system_clock::now();
  task->status = TaskStatus::kPending;
  task->type = TaskType::kBulkLoad;

  // 检查队列限制
  {
    std::lock_guard<std::mutex> lock(bulk_load_pending_mutex_);
    if (bulk_load_pending_queue_.size() >= config_.max_pending_tasks) {
      std::cerr << "[TaskScheduler] Bulk Load queue full, rejecting shard task"
                << std::endl;
      return "";
    }
    bulk_load_pending_queue_.push(task);
  }

  // 添加到全局索引
  {
    std::lock_guard<std::mutex> lock(tasks_mutex_);
    all_tasks_[task->task_id] = task;
  }

  statistics_.pending_count++;

  std::cout << "[TaskScheduler] Bulk Load shard submitted: " << task->task_id
            << std::endl;

  // 唤醒调度线程
  cv_.notify_one();

  return task->task_id;
}

std::shared_ptr<TaskInfo> TaskScheduler::GetBulkLoadTask(
  const std::string& task_id) const {
  std::lock_guard<std::mutex> lock(bulk_load_tasks_mutex_);
  auto it = bulk_load_tasks_.find(task_id);
  if (it != bulk_load_tasks_.end()) {
    return it->second;
  }
  return nullptr;
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

  TaskResult notify_result;

  {
    // Per-task mutex 保护
    std::lock_guard<std::mutex> task_lock(task->mtx);

    // 检查状态
    if (task->IsTerminal()) {
      return false;  // 已经结束的任务不能取消
    }

    // 保存旧状态（用于正确更新统计计数器）
    TaskStatus old_status = task->status;

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

    // 更新统计（根据旧状态决定减少哪个计数器）
    statistics_.total_cancelled++;
    if (old_status == TaskStatus::kPending) {
      statistics_.pending_count--;
    } else if (old_status == TaskStatus::kRunning ||
               old_status == TaskStatus::kAssigned) {
      statistics_.running_count--;
    }

    notify_result = task->result;
  }
  // ---- per-task mutex 已释放 ----

  std::cout << "[TaskScheduler] Task cancelled: " << task_id
            << ", reason: " << reason << std::endl;

  // 通知等待者 — 在 task->mtx 锁外调用，避免 ABBA 死锁
  NotifyTaskCompleted(task_id, notify_result);

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

  // ---- 以下操作在 per-task mutex 保护下进行 ----
  bool need_notify = false;
  bool need_retry = false;
  std::string parent_task_id;
  bool is_bulk_load_shard = false;
  bool shard_success = false;

  {
    std::lock_guard<std::mutex> task_lock(task->mtx);

    // 原子化检查: 如果任务已经是终态，说明已被其他线程处理过（问题4 竞争保护）
    if (task->IsTerminal()) {
      std::cout << "[TaskScheduler] Task " << task_id
                << " already in terminal state: "
                << TaskStatusToString(task->status) << ", skipping" << std::endl;
      return;
    }

    // 保存旧状态（用于正确更新计数器，问题2 下溢保护）
    TaskStatus old_status = task->status;

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
      // 先标记为 Failed，再检查是否需要重试
      task->status = TaskStatus::kFailed;
      task->error_message = result.error_message;

      if (task->CanRetry()) {
        // 仅当旧状态为 kRunning/kAssigned 时减少 running_count
        if (old_status == TaskStatus::kRunning ||
            old_status == TaskStatus::kAssigned) {
          statistics_.running_count--;
        }
        need_retry = true;
      } else {
        statistics_.total_failed++;
      }
    }

    if (!need_retry) {
      // 仅当旧状态为 kRunning/kAssigned 时减少 running_count（问题2 下溢保护）
      if (old_status == TaskStatus::kRunning ||
          old_status == TaskStatus::kAssigned) {
        statistics_.running_count--;
      }
      need_notify = true;
    }

    // 记录 BulkLoad 分片信息（稍后在锁外更新父任务）
    if (task->type == TaskType::kBulkLoad && !task->parent_task_id.empty()) {
      is_bulk_load_shard = true;
      parent_task_id = task->parent_task_id;
      shard_success = result.success;
    }
  }
  // ---- per-task mutex 已释放 ----

  // 重试分支（在 task mutex 外处理，HandleRetry 内部会获取锁）
  if (need_retry) {
    HandleRetry(task);
    return;
  }

  std::cout << "[TaskScheduler] Task completed: " << task_id
            << ", success=" << result.success << std::endl;

  // 问题7: 如果这是一个 BulkLoad 分片子任务，更新父任务的分片计数
  // 注意：获取 parent_task->mtx 时不持有子任务的 mtx，避免嵌套锁
  if (is_bulk_load_shard) {
    std::shared_ptr<TaskInfo> parent_task;
    {
      std::lock_guard<std::mutex> bl_lock(bulk_load_tasks_mutex_);
      auto it = bulk_load_tasks_.find(parent_task_id);
      if (it != bulk_load_tasks_.end()) {
        parent_task = it->second;
      }
    }
    if (parent_task) {
      std::lock_guard<std::mutex> parent_lock(parent_task->mtx);
      if (shard_success) {
        parent_task->bulk_load_params.completed_shards++;
        std::cout << "[TaskScheduler] BulkLoad shard completed: " << task_id
                  << " (parent=" << parent_task_id
                  << ", completed=" << parent_task->bulk_load_params.completed_shards
                  << "/" << parent_task->bulk_load_params.shards.size() << ")"
                  << std::endl;
      } else {
        parent_task->bulk_load_params.failed_shards++;
        std::cerr << "[TaskScheduler] BulkLoad shard failed: " << task_id
                  << " (parent=" << parent_task_id
                  << ", failed=" << parent_task->bulk_load_params.failed_shards
                  << ")" << std::endl;
      }
    }
  }

  // 通知等待者 — 在 task->mtx 锁外调用，避免与 WaitForTask 形成 ABBA 死锁
  // (WaitForTask: task_cv_mutex_ -> task->mtx, 这里: 无 task->mtx -> task_cv_mutex_)
  if (need_notify) {
    NotifyTaskCompleted(task_id, result);
  }
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
  {
    std::lock_guard<std::mutex> task_lock(task->mtx);
    if (task->IsTerminal()) {
      if (result) {
        *result = task->result;
      }
      return true;
    }
  }

  // 获取或创建条件变量 (使用 unique_ptr 避免 map rehash UB)
  std::condition_variable* cv_ptr = nullptr;
  {
    std::lock_guard<std::mutex> lock(task_cv_mutex_);
    auto& cv_uptr = task_cv_map_[task_id];
    if (!cv_uptr) {
      cv_uptr = std::make_unique<std::condition_variable>();
    }
    cv_ptr = cv_uptr.get();
  }

  // 等待完成 — predicate 中不再获取 tasks_mutex_，
  // 而是使用 per-task mutex，避免死锁
  bool completed = false;
  if (timeout_ms > 0) {
    std::unique_lock<std::mutex> lock(task_cv_mutex_);
    completed = cv_ptr->wait_for(
      lock, std::chrono::milliseconds(timeout_ms), [&]() {
        // 使用 per-task mutex 检查状态，不获取 tasks_mutex_
        std::lock_guard<std::mutex> task_lock(task->mtx);
        return task->IsTerminal();
      });
  } else {
    // 立即返回当前状态
    std::lock_guard<std::mutex> task_lock(task->mtx);
    completed = task->IsTerminal();
  }

  if (completed && result) {
    std::lock_guard<std::mutex> task_lock(task->mtx);
    *result = task->result;
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

  uint32_t cleanup_counter = 0;
  const uint32_t cleanup_interval =
    config_.completed_task_cleanup_interval_sec / 10;  // 每 N 个循环清理一次

  while (running_.load()) {
    auto now = std::chrono::system_clock::now();

    // 检查运行中的任务
    // 第五轮修复 (NEW-4): 读取 task->start_time/task_id 时加 per-task mutex，
    // 防止 std::string task_id 的 torn read
    std::vector<std::string> timeout_tasks;
    {
      std::lock_guard<std::mutex> lock(running_mutex_);
      for (const auto& pair : running_tasks_) {
        auto& task = pair.second;
        std::lock_guard<std::mutex> task_lock(task->mtx);
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
      if (!task) {
        continue;
      }

      // 在 per-task mutex 内完成状态转换和信息收集
      bool need_retry = false;
      bool need_notify = false;
      TaskResult notify_result;
      {
        std::lock_guard<std::mutex> task_lock(task->mtx);

        // 原子化状态转换: 仅当任务仍在运行中时才标记超时
        if (!task->TryTransitionFrom(TaskStatus::kRunning,
                                     TaskStatus::kAssigned,
                                     TaskStatus::kTimeout)) {
          std::cout << "[TaskScheduler] Task " << task_id
                    << " already transitioned to "
                    << TaskStatusToString(task->status) << ", skip timeout"
                    << std::endl;
          continue;
        }
        statistics_.total_timeout++;

        // 从运行队列移除
        {
          std::lock_guard<std::mutex> lock(running_mutex_);
          running_tasks_.erase(task_id);
        }

        // 释放 Worker 任务槽
        if (worker_manager_ && !task->assigned_worker_id.empty()) {
          worker_manager_->ReleaseTask(task->assigned_worker_id, task_id, false);
        }

        // 更新计数器
        statistics_.running_count--;

        // 检查是否需要重试
        if (task->CanRetry()) {
          need_retry = true;
        } else {
          task->status = TaskStatus::kFailed;
          task->error_message = "Task timeout";
          task->complete_time = std::chrono::system_clock::now();
          statistics_.total_failed++;
          need_notify = true;
          notify_result = task->result;
        }
      }
      // ---- per-task mutex 已释放 ----

      // 在锁外调用，避免递归锁和 ABBA 死锁
      if (need_retry) {
        HandleRetry(task);
      } else if (need_notify) {
        NotifyTaskCompleted(task_id, notify_result);
      }
    }

    // 定期清理已完成任务（问题5: 防止 all_tasks_ 无限增长）
    if (++cleanup_counter >= cleanup_interval) {
      cleanup_counter = 0;
      CleanupCompletedTasks();
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
      // 第五轮修复 (NEW-3): 读取 task->assigned_worker_id/task_id 时加 per-task mutex
      std::vector<std::string> tasks_to_reschedule;
      {
        std::lock_guard<std::mutex> lock(running_mutex_);
        for (const auto& pair : running_tasks_) {
          auto& task = pair.second;
          std::string worker_id;
          std::string task_id;
          {
            std::lock_guard<std::mutex> task_lock(task->mtx);
            worker_id = task->assigned_worker_id;
            task_id = task->task_id;
          }
          auto worker = worker_manager_->GetWorker(worker_id);
          if (!worker || worker->status == WorkerStatus::kOffline) {
            tasks_to_reschedule.push_back(task_id);
          }
        }
      }

      // 重新调度这些任务
      for (const auto& task_id : tasks_to_reschedule) {
        auto task = GetTask(task_id);
        if (task) {
          // 问题2 修复: 先在 task->mtx 外获取 pending count，
          // 避免持有 task->mtx 时调 GetPendingCount() 导致 ABBA 死锁
          // (DoSchedule 路径: pending_mutex_ -> task->mtx)
          size_t cur_pending = GetPendingCount();

          // 使用 per-task mutex 保护状态检查和修改
          bool should_retry = false;
          bool should_fallback = false;
          {
            std::lock_guard<std::mutex> task_lock(task->mtx);
            // 检查任务是否仍需处理（可能已被 OnTaskCompleted 处理）
            if (task->IsTerminal()) {
              continue;
            }
            std::cout << "[TaskScheduler] Worker offline, rescheduling task: "
                      << task_id << std::endl;
            task->reschedule_count++;
            if (ShouldFallback(task, cur_pending)) {
              should_fallback = true;
            } else {
              should_retry = true;
            }
          }
          // 在 per-task mutex 外调用（内部会获取其他锁）
          if (should_fallback) {
            HandleFallback(task, "Worker offline and reschedule limit reached");
          } else if (should_retry) {
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
  // CaaS-LSM: 处理需要降级的任务
  std::vector<std::shared_ptr<TaskInfo>> fallback_tasks;

  {
    std::lock_guard<std::mutex> lock(pending_mutex_);

    while (!pending_queue_.empty()) {
      auto task = pending_queue_.top();

      // 问题10 修复: 检查任务是否仍为 kPending（可能已被 Cancel 修改状态）
      {
        std::lock_guard<std::mutex> task_lock(task->mtx);
        if (task->status != TaskStatus::kPending) {
          pending_queue_.pop();
          // 如果已被取消，更新 pending_count
          statistics_.pending_count--;
          continue;
        }
      }

      // CaaS-LSM: 检查是否应该降级（传入 pending_size 避免死锁）
      if (ShouldFallback(task, pending_queue_.size())) {
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
  }  // pending_mutex_ 在此释放

  // 在锁外处理降级任务（避免长时间持有 pending_mutex_）
  for (auto& task : fallback_tasks) {
    HandleFallback(task, "Fallback condition met");
  }

  // =====================================================================
  // CaaS-LSM: Bulk Load 分片队列调度
  // Bulk Load 分片优先级低于高紧急 Compaction，但仍按优先级和 FIFO 排序
  // =====================================================================
  if (!available_workers.empty()) {
    std::lock_guard<std::mutex> bl_lock(bulk_load_pending_mutex_);
    while (!bulk_load_pending_queue_.empty() && !available_workers.empty()) {
      auto shard_task = bulk_load_pending_queue_.top();

      // 问题10 修复: 检查 BulkLoad 分片任务状态
      {
        std::lock_guard<std::mutex> task_lock(shard_task->mtx);
        if (shard_task->status != TaskStatus::kPending) {
          bulk_load_pending_queue_.pop();
          statistics_.pending_count--;
          continue;
        }
      }

      // 选择可用 Worker
      std::shared_ptr<WorkerInfo> bl_worker = nullptr;
      for (const auto& w : available_workers) {
        if (w->IsAvailable()) {
          bl_worker = w;
          break;
        }
      }
      if (!bl_worker) {
        break;
      }

      bulk_load_pending_queue_.pop();

      SchedulingDecision decision;
      decision.task_id = shard_task->task_id;
      decision.worker_id = bl_worker->worker_id;
      decision.should_schedule = true;
      decision.reason = "BulkLoad shard scheduling";

      decisions.push_back(decision);

      // 更新可用 Worker 列表
      available_workers.erase(
        std::remove_if(available_workers.begin(),
                       available_workers.end(),
                       [&bl_worker](const std::shared_ptr<WorkerInfo>& w) {
                         return w->worker_id == bl_worker->worker_id &&
                                w->resources.AvailableSlots() <= 1;
                       }),
        available_workers.end());
    }
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
      {
        std::lock_guard<std::mutex> task_lock(task->mtx);
        task->assigned_worker_id = decision.worker_id;
        task->assign_time = std::chrono::system_clock::now();
        task->status = TaskStatus::kAssigned;
      }

      // 添加到运行队列
      {
        std::lock_guard<std::mutex> lock(running_mutex_);
        running_tasks_[task->task_id] = task;
      }

      statistics_.pending_count--;
      statistics_.running_count++;

      // CaaS-LSM: 根据任务类型选择推送模式
      bool distribute_success = false;
      if (task->type == TaskType::kBulkLoad) {
        // Bulk Load 分片通过推送模式分发
        distribute_success = DistributeBulkLoadShardToCSA(decision.worker_id, task);
      } else {
        // Compaction 任务通过推送模式分发
        distribute_success = DistributeTaskToCSA(decision.worker_id, task);
      }

      // 问题6: 推送失败时回滚 — 将任务回退到 pending 队列
      if (!distribute_success) {
        std::cerr << "[TaskScheduler] Distribute failed, rolling back task: "
                  << task->task_id << " -> " << decision.worker_id << std::endl;

        // 从运行队列移除
        {
          std::lock_guard<std::mutex> lock(running_mutex_);
          running_tasks_.erase(task->task_id);
        }

        // 释放 Worker 任务槽
        if (worker_manager_) {
          worker_manager_->ReleaseTask(decision.worker_id, task->task_id, false);
        }

        // 回退计数器
        statistics_.running_count--;
        statistics_.pending_count++;

        // 重新入队
        {
          std::lock_guard<std::mutex> task_lock(task->mtx);
          task->status = TaskStatus::kPending;
          task->assigned_worker_id.clear();
        }
        if (task->type == TaskType::kBulkLoad) {
          std::lock_guard<std::mutex> bl_lock(bulk_load_pending_mutex_);
          bulk_load_pending_queue_.push(task);
        } else {
          std::lock_guard<std::mutex> lock(pending_mutex_);
          pending_queue_.push(task);
        }
      }

      std::cout << "[TaskScheduler] Task assigned: " << task->task_id
                << " [" << TaskTypeToString(task->type) << "]"
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
  // 问题1 修复: HandleRetry 不再操作 running_tasks_。
  // 契约: 所有调用方（OnTaskCompleted, TimeoutCheckLoop, CSAStatusCheckLoop）
  // 在调用 HandleRetry 前必须已经从 running_tasks_ 中移除该任务并更新 running_count。
  //
  // 第五轮修复 (NEW-5): 合并两次加锁为一次，避免不必要的锁释放/获取开销
  {
    std::lock_guard<std::mutex> task_lock(task->mtx);
    task->retry_count++;

    // CaaS-LSM: 先标记为 Retrying 状态，表示正在等待重新调度
    task->status = TaskStatus::kRetrying;
    task->assigned_worker_id.clear();

    std::cout << "[TaskScheduler] Task retrying: " << task->task_id
              << " (attempt " << task->retry_count << "/" << task->max_retries
              << ")" << std::endl;

    // 转为 Pending 状态（在同一锁内完成，避免中间状态被其他线程观察到后产生歧义）
    task->status = TaskStatus::kPending;
  }

  {
    std::lock_guard<std::mutex> lock(pending_mutex_);
    pending_queue_.push(task);
  }

  statistics_.pending_count++;

  // 唤醒调度线程
  cv_.notify_one();
}

// CaaS-LSM: 检查是否应该降级到本地执行
bool TaskScheduler::ShouldFallback(const std::shared_ptr<TaskInfo>& task,
                                   size_t pending_size) {
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
  // NOTE: pending_size 由调用者传入，因为调用者已持有 pending_mutex_ 锁
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
  std::string task_id;
  TaskResult task_result;

  {
    std::lock_guard<std::mutex> task_lock(task->mtx);
    task->should_fallback = true;
    task->status = TaskStatus::kFailed;
    task->error_message = "Fallback to local: " + reason;
    task->complete_time = std::chrono::system_clock::now();

    // 设置结果
    task->result.success = false;
    task->result.error_message = task->error_message;

    task_id = task->task_id;
    task_result = task->result;
  }

  // 存储结果供客户端查询
  {
    std::lock_guard<std::mutex> lock(results_mutex_);
    completed_results_[task_id] = task_result;
  }

  statistics_.total_failed++;
  statistics_.pending_count--;

  std::cout << "[TaskScheduler] Task fallback: " << task_id
            << ", reason: " << reason << std::endl;

  // 通知等待者 (客户端可以根据 should_fallback 标志执行本地 compaction)
  // 在 task->mtx 外调用，避免死锁
  NotifyTaskCompleted(task_id, task_result);
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
  {
    std::lock_guard<std::mutex> task_lock(task->mtx);
    task->start_time = std::chrono::system_clock::now();
    task->status = TaskStatus::kRunning;
  }

  std::cout << "[TaskScheduler] Task distributed to CSA: " << task->task_id
            << " -> " << worker_id << std::endl;

  return true;
}

// CaaS-LSM: Bulk Load 分片分发到 CSA (推送模式)
bool TaskScheduler::DistributeBulkLoadShardToCSA(
  const std::string& worker_id,
  std::shared_ptr<TaskInfo> shard_task) {
  if (!worker_manager_) {
    return false;
  }

  auto worker = worker_manager_->GetWorker(worker_id);
  if (!worker) {
    std::cerr << "[TaskScheduler] Worker not found for Bulk Load shard: "
              << worker_id << std::endl;
    return false;
  }

  // 检查 Worker 状态
  if (worker->status != WorkerStatus::kOnline) {
    std::cerr << "[TaskScheduler] Worker not online for Bulk Load: "
              << worker_id << std::endl;
    return false;
  }

  // 通过 gRPC 调用 CSA 的 ExecuteBulkLoadShard RPC (推送模式)
  // 从 TaskInfo 的 bulk_load_params 中提取分片参数
  const auto& bl_params = shard_task->bulk_load_params;

  // 找到当前分片信息
  // 子任务的 bulk_load_params 中包含分片信息（第一个 shard 或通过 shard_index 匹配）
  std::string shard_id;
  uint32_t shard_index = 0;
  std::string source_path = bl_params.source_path;
  std::string key_range_start;
  std::string key_range_end;
  uint32_t slot_start = 0;
  uint32_t slot_end = 0;

  // 查找匹配的分片信息 — 使用精确的 shard_id 匹配（问题15 修复）
  // 优先通过 parent_task_id 找到父任务并精确匹配
  if (!shard_task->parent_task_id.empty()) {
    std::shared_ptr<TaskInfo> parent_task;
    {
      std::lock_guard<std::mutex> bl_lock(bulk_load_tasks_mutex_);
      auto it = bulk_load_tasks_.find(shard_task->parent_task_id);
      if (it != bulk_load_tasks_.end()) {
        parent_task = it->second;
      }
    }
    if (parent_task) {
      for (const auto& shard : parent_task->bulk_load_params.shards) {
        // 使用精确的 shard_id == task_id 后缀匹配
        if (shard_task->task_id == shard.shard_id ||
            shard_task->db_name == shard.shard_id) {
          shard_id = shard.shard_id;
          shard_index = shard.shard_index;
          source_path = shard.source_path.empty() ? bl_params.source_path
                                                   : shard.source_path;
          key_range_start = shard.key_range.start_key;
          key_range_end = shard.key_range.end_key;
          slot_start = shard.key_range.slot_start;
          slot_end = shard.key_range.slot_end;
          break;
        }
      }
    }
  }

  // 回退: 从子任务自身的 shards 中查找
  if (shard_id.empty()) {
    for (const auto& shard : bl_params.shards) {
      if (shard_task->task_id == shard.shard_id) {
        shard_id = shard.shard_id;
        shard_index = shard.shard_index;
        source_path = shard.source_path.empty() ? bl_params.source_path
                                                 : shard.source_path;
        key_range_start = shard.key_range.start_key;
        key_range_end = shard.key_range.end_key;
        slot_start = shard.key_range.slot_start;
        slot_end = shard.key_range.slot_end;
        break;
      }
    }
  }

  if (shard_id.empty()) {
    shard_id = shard_task->task_id;
  }

  bool success = worker_manager_->DistributeBulkLoadShardToCSA(
    worker_id,
    shard_task->task_id,
    shard_id,
    shard_index,
    static_cast<int32_t>(bl_params.source_type),
    source_path,
    static_cast<int32_t>(bl_params.data_format),
    key_range_start,
    key_range_end,
    slot_start,
    slot_end,
    bl_params.shared_fs_uri,
    bl_params.sst_output_dir,
    static_cast<int32_t>(bl_params.compression),
    bl_params.target_sst_size,
    bl_params.generate_binlog,
    bl_params.target_store_id,
    bl_params.target_db_path,
    bl_params.rate_limit_bytes_per_sec,
    bl_params.timeout_sec);

  if (!success) {
    std::cerr << "[TaskScheduler] Failed to distribute Bulk Load shard to CSA: "
              << shard_task->task_id << " -> " << worker_id << std::endl;
    return false;
  }

  std::cout << "[TaskScheduler] Bulk Load shard distributed to CSA: "
            << shard_task->task_id << " -> " << worker_id << std::endl;

  // 更新任务状态为 Running
  {
    std::lock_guard<std::mutex> task_lock(shard_task->mtx);
    shard_task->start_time = std::chrono::system_clock::now();
    shard_task->status = TaskStatus::kRunning;
  }

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
  // 通知等待者并清理 cv 条目（问题14: 防止内存泄漏）
  {
    std::lock_guard<std::mutex> lock(task_cv_mutex_);
    auto it = task_cv_map_.find(task_id);
    if (it != task_cv_map_.end()) {
      if (it->second) {
        it->second->notify_all();
      }
      // 延迟删除: notify_all 后等待者会被唤醒并检查 predicate
      // 条目会在 CleanupCompletedTasks 中被定期清理
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

// 问题5 + 问题14: 清理已完成的任务和 cv 条目，防止内存无限增长
void TaskScheduler::CleanupCompletedTasks() {
  auto now = std::chrono::system_clock::now();
  auto retention = std::chrono::seconds(config_.completed_task_retention_sec);

  std::vector<std::string> tasks_to_remove;

  // 找出过期的终态任务
  {
    std::lock_guard<std::mutex> lock(tasks_mutex_);
    for (const auto& pair : all_tasks_) {
      const auto& task = pair.second;
      std::lock_guard<std::mutex> task_lock(task->mtx);
      if (task->IsTerminal()) {
        auto elapsed = now - task->complete_time;
        if (elapsed > retention) {
          tasks_to_remove.push_back(pair.first);
        }
      }
    }

    // 移除过期任务
    for (const auto& task_id : tasks_to_remove) {
      all_tasks_.erase(task_id);
    }
  }

  // 清理对应的 cv 条目
  if (!tasks_to_remove.empty()) {
    std::lock_guard<std::mutex> lock(task_cv_mutex_);
    for (const auto& task_id : tasks_to_remove) {
      task_cv_map_.erase(task_id);
    }
  }

  // 清理对应的 completed_results 条目
  if (!tasks_to_remove.empty()) {
    std::lock_guard<std::mutex> lock(results_mutex_);
    for (const auto& task_id : tasks_to_remove) {
      completed_results_.erase(task_id);
    }
  }

  // 问题7 修复: 清理对应的 bulk_load_tasks_ 条目（BulkLoad 父任务索引）
  if (!tasks_to_remove.empty()) {
    std::lock_guard<std::mutex> lock(bulk_load_tasks_mutex_);
    for (const auto& task_id : tasks_to_remove) {
      bulk_load_tasks_.erase(task_id);
    }
  }

  if (!tasks_to_remove.empty()) {
    std::cout << "[TaskScheduler] Cleaned up " << tasks_to_remove.size()
              << " completed tasks" << std::endl;
  }
}

}  // namespace control_plane
}  // namespace tendisplus
