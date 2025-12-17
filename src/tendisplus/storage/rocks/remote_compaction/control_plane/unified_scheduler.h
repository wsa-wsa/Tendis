// Copyright (c) 2024-present, Tencent Inc. All rights reserved.
// 统一任务调度器

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
#include <vector>

#include "task_model.h"
#include "worker_manager.h"

namespace tendisplus {
namespace control_plane {

// ============================================================================
// 调度策略决策
// ============================================================================
enum class ExecutionMode {
  kLocal = 0,              // 本地执行
  kRemoteSingle = 1,       // 单 Worker 远程执行
  kRemoteParallel = 2,     // 多 Worker 并行执行
  kDeferred = 3            // 延迟执行
};

inline const char* ExecutionModeToString(ExecutionMode mode) {
  switch (mode) {
    case ExecutionMode::kLocal: return "Local";
    case ExecutionMode::kRemoteSingle: return "RemoteSingle";
    case ExecutionMode::kRemoteParallel: return "RemoteParallel";
    case ExecutionMode::kDeferred: return "Deferred";
    default: return "Unknown";
  }
}

// ============================================================================
// 调度策略决策结果
// ============================================================================
struct SchedulingStrategy {
  ExecutionMode mode = ExecutionMode::kLocal;
  std::string reason;
  
  // 远程执行相关
  std::vector<std::string> selected_workers;
  uint32_t parallelism = 1;
  
  // 延迟执行相关
  std::chrono::system_clock::time_point defer_until;
  
  // 资源分配
  ResourceRequirement allocated_resources;
  
  // 限速参数
  uint64_t rate_limit_bytes_per_sec = 0;
};

// ============================================================================
// 统一调度器配置
// ============================================================================
struct UnifiedSchedulerConfig {
  // 基本配置
  uint32_t scheduling_interval_ms = 100;
  uint32_t max_concurrent_tasks = 100;
  
  // 本地/远程决策阈值
  uint64_t local_threshold_bytes = 64 * 1024 * 1024;  // 64MB 以下本地执行
  double local_load_threshold = 0.7;  // 本地负载超过 70% 时考虑远程
  
  // 并行执行阈值
  uint64_t parallel_threshold_bytes = 1024 * 1024 * 1024;  // 1GB 以上并行
  uint32_t max_parallelism = 8;
  
  // 资源感知配置
  double cpu_weight = 0.3;
  double memory_weight = 0.2;
  double io_weight = 0.3;
  double affinity_weight = 0.2;
  
  // 负载均衡配置
  double load_balance_threshold = 0.3;  // 负载差异超过 30% 触发迁移
  uint32_t migration_cooldown_sec = 60;
  
  // 队列配置
  uint32_t high_priority_ratio = 50;    // 高优先级任务占比
  uint32_t normal_priority_ratio = 40;
  uint32_t low_priority_ratio = 10;
};

// ============================================================================
// 多级任务队列
// ============================================================================
class MultiLevelQueue {
 public:
  MultiLevelQueue();
  ~MultiLevelQueue() = default;
  
  // 入队
  void Enqueue(std::shared_ptr<BackgroundTask> task);
  
  // 出队 (按优先级比例)
  std::shared_ptr<BackgroundTask> Dequeue();
  
  // 按类型出队
  std::shared_ptr<BackgroundTask> DequeueByType(TaskType type);
  
  // 查看队首
  std::shared_ptr<BackgroundTask> Peek(TaskPriority priority) const;
  
  // 移除任务
  bool Remove(const std::string& task_id);
  
  // 更新优先级
  bool UpdatePriority(const std::string& task_id, TaskPriority new_priority);
  
  // 队列统计
  struct QueueStats {
    size_t total_size = 0;
    size_t high_priority_size = 0;
    size_t normal_priority_size = 0;
    size_t low_priority_size = 0;
    std::map<TaskType, size_t> by_type;
    std::chrono::milliseconds avg_wait_time{0};
    std::chrono::milliseconds max_wait_time{0};
  };
  
  QueueStats GetStats() const;
  
  // 清空
  void Clear();

 private:
  struct TaskEntry {
    std::shared_ptr<BackgroundTask> task;
    std::chrono::system_clock::time_point enqueue_time;
    
    bool operator<(const TaskEntry& other) const;
  };
  
  std::priority_queue<TaskEntry> high_priority_queue_;
  std::priority_queue<TaskEntry> normal_priority_queue_;
  std::priority_queue<TaskEntry> low_priority_queue_;
  
  std::map<std::string, std::shared_ptr<BackgroundTask>> task_map_;
  mutable std::mutex mutex_;
  
  // 轮询计数器 (用于按比例出队)
  uint32_t dequeue_counter_ = 0;
};

// ============================================================================
// 资源感知调度算法
// ============================================================================
class ResourceAwareScheduler {
 public:
  explicit ResourceAwareScheduler(const UnifiedSchedulerConfig& config);
  ~ResourceAwareScheduler() = default;
  
  // 计算节点评分
  struct NodeScore {
    std::string worker_id;
    double total_score = 0.0;
    double load_score = 0.0;
    double affinity_score = 0.0;
    double resource_score = 0.0;
    double priority_score = 0.0;
  };
  
  NodeScore ComputeScore(const WorkerInfo& worker,
                         const BackgroundTask& task) const;
  
  // 选择最佳节点
  std::vector<NodeScore> RankWorkers(
      const std::vector<WorkerInfo>& workers,
      const BackgroundTask& task) const;
  
  // 选择 Top-K 节点 (用于并行执行)
  std::vector<std::string> SelectTopK(
      const std::vector<WorkerInfo>& workers,
      const BackgroundTask& task,
      uint32_t k) const;
  
  // 计算数据亲和性
  double ComputeAffinity(const WorkerInfo& worker,
                         const BackgroundTask& task) const;
  
  // 计算负载评分 (负载越低分数越高)
  double ComputeLoadScore(const WorkerInfo& worker) const;
  
  // 计算资源评分
  double ComputeResourceScore(const WorkerInfo& worker,
                              const ResourceRequirement& requirement) const;

 private:
  UnifiedSchedulerConfig config_;
};

// ============================================================================
// 调度策略决策器
// ============================================================================
class SchedulingDecisionMaker {
 public:
  explicit SchedulingDecisionMaker(const UnifiedSchedulerConfig& config);
  ~SchedulingDecisionMaker() = default;
  
  // 决定执行模式
  SchedulingStrategy Decide(const BackgroundTask& task,
                            const std::vector<WorkerInfo>& available_workers,
                            double local_load);
  
  // 针对 Compaction 任务的决策
  SchedulingStrategy DecideForCompaction(
      const BackgroundTask& task,
      const std::vector<WorkerInfo>& available_workers,
      double local_load);
  
  // 针对 Bulk Load 任务的决策
  SchedulingStrategy DecideForBulkLoad(
      const BackgroundTask& task,
      const std::vector<WorkerInfo>& available_workers,
      double local_load);
  
  // 是否应该本地执行
  bool ShouldExecuteLocally(const BackgroundTask& task,
                            double local_load) const;
  
  // 是否应该并行执行
  bool ShouldExecuteInParallel(const BackgroundTask& task,
                               uint32_t available_workers) const;
  
  // 计算最佳并行度
  uint32_t ComputeOptimalParallelism(const BackgroundTask& task,
                                      uint32_t available_workers) const;

 private:
  UnifiedSchedulerConfig config_;
  std::unique_ptr<ResourceAwareScheduler> resource_scheduler_;
};

// ============================================================================
// 任务迁移管理器
// ============================================================================
class TaskMigrationManager {
 public:
  explicit TaskMigrationManager(const UnifiedSchedulerConfig& config);
  ~TaskMigrationManager() = default;
  
  // 检查是否需要迁移
  struct MigrationDecision {
    bool should_migrate = false;
    std::string task_id;
    std::string source_worker;
    std::string target_worker;
    std::string reason;
  };
  
  std::vector<MigrationDecision> CheckMigration(
      const std::vector<WorkerInfo>& workers,
      const std::map<std::string, std::shared_ptr<BackgroundTask>>& active_tasks);
  
  // 执行迁移
  bool Migrate(const MigrationDecision& decision);
  
  // 检查是否可以迁移
  bool CanMigrate(const BackgroundTask& task) const;
  
  // 获取迁移历史
  struct MigrationRecord {
    std::string task_id;
    std::string source_worker;
    std::string target_worker;
    std::chrono::system_clock::time_point timestamp;
    bool success;
    std::string reason;
  };
  
  std::vector<MigrationRecord> GetMigrationHistory(
      std::chrono::system_clock::time_point since) const;

 private:
  UnifiedSchedulerConfig config_;
  std::vector<MigrationRecord> migration_history_;
  std::map<std::string, std::chrono::system_clock::time_point> migration_cooldown_;
  mutable std::mutex mutex_;
};

// ============================================================================
// 统一任务调度器
// ============================================================================
class UnifiedScheduler {
 public:
  explicit UnifiedScheduler(const UnifiedSchedulerConfig& config);
  ~UnifiedScheduler();
  
  // 禁止拷贝
  UnifiedScheduler(const UnifiedScheduler&) = delete;
  UnifiedScheduler& operator=(const UnifiedScheduler&) = delete;
  
  // 单例访问
  static UnifiedScheduler& Instance();
  static void Initialize(const UnifiedSchedulerConfig& config);
  static void Shutdown();
  
  // 启动/停止
  void Start();
  void Stop();
  bool IsRunning() const { return running_.load(); }
  
  // =========================================================================
  // 任务提交
  // =========================================================================
  
  // 提交任务
  std::string Submit(std::shared_ptr<BackgroundTask> task);
  
  // 提交 Compaction 任务
  std::string SubmitCompaction(const std::string& source_node_id,
                               const std::string& db_name,
                               uint32_t store_id,
                               const CompactionTaskParams& params,
                               TaskPriority priority = TaskPriority::kNormal);
  
  // 提交 Bulk Load 任务
  std::string SubmitBulkLoad(const std::string& source_node_id,
                             const std::string& db_name,
                             uint32_t store_id,
                             const BulkLoadTaskParams& params,
                             TaskPriority priority = TaskPriority::kNormal);
  
  // =========================================================================
  // 任务管理
  // =========================================================================
  
  // 取消任务
  bool Cancel(const std::string& task_id, const std::string& reason = "");
  
  // 暂停/恢复任务
  bool Pause(const std::string& task_id);
  bool Resume(const std::string& task_id);
  
  // 更新优先级
  bool UpdatePriority(const std::string& task_id, TaskPriority priority);
  
  // 查询任务
  std::shared_ptr<BackgroundTask> GetTask(const std::string& task_id) const;
  std::vector<std::shared_ptr<BackgroundTask>> QueryTasks(
      const TaskFilter& filter) const;
  
  // =========================================================================
  // 任务回调
  // =========================================================================
  
  // 任务完成回调
  void OnTaskCompleted(const std::string& task_id, const TaskResult& result);
  
  // 任务失败回调
  void OnTaskFailed(const std::string& task_id, const std::string& error);
  
  // 任务进度回调
  void OnTaskProgress(const std::string& task_id, double progress);
  
  // =========================================================================
  // Worker 管理
  // =========================================================================
  
  void SetWorkerManager(std::shared_ptr<WorkerManager> worker_manager);
  
  // =========================================================================
  // 统计信息
  // =========================================================================
  
  struct SchedulerStats {
    // 任务统计
    uint64_t total_submitted = 0;
    uint64_t total_completed = 0;
    uint64_t total_failed = 0;
    uint64_t total_cancelled = 0;
    
    // 当前状态
    size_t queue_length = 0;
    size_t active_tasks = 0;
    
    // 执行模式分布
    uint64_t local_executions = 0;
    uint64_t remote_single_executions = 0;
    uint64_t remote_parallel_executions = 0;
    
    // 性能统计
    double avg_scheduling_latency_ms = 0;
    double avg_queue_wait_ms = 0;
    double avg_execution_time_sec = 0;
    
    // 迁移统计
    uint64_t migrations_total = 0;
    uint64_t migrations_success = 0;
  };
  
  SchedulerStats GetStats() const;
  
  // 获取队列统计
  MultiLevelQueue::QueueStats GetQueueStats() const;

 private:
  void ScheduleLoop();
  void MigrationCheckLoop();
  
  // 调度一轮
  void ScheduleRound();
  
  // 执行调度决策
  void ExecuteSchedulingDecision(std::shared_ptr<BackgroundTask> task,
                                  const SchedulingStrategy& strategy);
  
  // 分发任务到 Worker
  bool DispatchToWorker(std::shared_ptr<BackgroundTask> task,
                        const std::string& worker_id);
  
  // 处理重试
  void HandleRetry(std::shared_ptr<BackgroundTask> task);
  
  // 生成任务 ID
  std::string GenerateTaskId();
  
  UnifiedSchedulerConfig config_;
  std::atomic<bool> running_{false};
  
  // 任务队列
  std::unique_ptr<MultiLevelQueue> task_queue_;
  
  // 活跃任务
  std::map<std::string, std::shared_ptr<BackgroundTask>> active_tasks_;
  mutable std::mutex active_tasks_mutex_;
  
  // 已完成任务
  std::map<std::string, std::shared_ptr<BackgroundTask>> completed_tasks_;
  mutable std::mutex completed_tasks_mutex_;
  
  // 组件
  std::shared_ptr<WorkerManager> worker_manager_;
  std::unique_ptr<SchedulingDecisionMaker> decision_maker_;
  std::unique_ptr<TaskMigrationManager> migration_manager_;
  
  // 线程
  std::unique_ptr<std::thread> scheduler_thread_;
  std::unique_ptr<std::thread> migration_thread_;
  std::condition_variable cv_;
  mutable std::mutex mutex_;
  
  // 统计
  std::atomic<uint64_t> total_submitted_{0};
  std::atomic<uint64_t> total_completed_{0};
  std::atomic<uint64_t> total_failed_{0};
  std::atomic<uint64_t> task_id_counter_{0};
  std::atomic<uint64_t> local_executions_{0};
  std::atomic<uint64_t> remote_single_executions_{0};
  std::atomic<uint64_t> remote_parallel_executions_{0};
  
  // 单例
  static std::unique_ptr<UnifiedScheduler> instance_;
  static std::mutex instance_mutex_;
};

}  // namespace control_plane
}  // namespace tendisplus
