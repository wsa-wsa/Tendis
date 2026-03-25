// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

// TaskTracer - 任务链路追踪
// 基于 CaaS-LSM 观测平面设计
//
// 设计思路：
// 1. 以 TaskID 作为 TraceID，贯穿 TendisPlus → ControlPlane → Worker 全链路
// 2. 每个关键操作生成一个 Span（提交、调度、分配、执行、完成）
// 3. 支持 Span 层级关系（Parent-Child）
// 4. 提供任务级的端到端链路可视化
//
// 与 OpenTelemetry/Jaeger 的对应关系：
// - TraceID = TaskID（task_id 即为全局唯一追踪标识）
// - SpanID = 自动生成的操作标识
// - ParentSpan = 上一个操作阶段
//
// 例如一个 Compaction 任务的 Trace：
//   [task_submit] → [task_schedule] → [task_assign] → [task_execute] → [task_complete]
//       10ms            5ms              2ms             5000ms           1ms

#pragma once

#include <atomic>
#include <chrono>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace tendisplus {
namespace control_plane {

// ============================================================================
// Span 状态
// ============================================================================
enum class SpanStatus {
  kOk = 0,        // 正常完成
  kError = 1,     // 执行出错
  kTimeout = 2    // 超时
};

inline const char* SpanStatusToString(SpanStatus status) {
  switch (status) {
    case SpanStatus::kOk: return "OK";
    case SpanStatus::kError: return "Error";
    case SpanStatus::kTimeout: return "Timeout";
    default: return "Unknown";
  }
}

// ============================================================================
// Span - 追踪跨度
// ============================================================================
struct TraceSpan {
  std::string span_id;              // Span ID
  std::string trace_id;             // Trace ID (= task_id)
  std::string parent_span_id;       // 父 Span ID (空 = 根 Span)
  std::string operation;            // 操作名称 (e.g. "task_submit")
  std::string component;            // 组件名 (e.g. "ControlPlane", "Worker")
  std::string worker_id;            // 执行的 Worker ID (如果适用)

  int64_t start_time_ms = 0;        // 开始时间
  int64_t end_time_ms = 0;          // 结束时间
  int64_t duration_ms = 0;          // 持续时间

  SpanStatus status = SpanStatus::kOk;
  std::string error_message;

  // 标签 (key-value 扩展属性)
  std::unordered_map<std::string, std::string> tags;

  // 日志事件
  struct LogEvent {
    int64_t timestamp_ms = 0;
    std::string message;
  };
  std::vector<LogEvent> logs;

  bool IsFinished() const { return end_time_ms > 0; }
};

// ============================================================================
// TaskTrace - 任务完整追踪链路
// ============================================================================
struct TaskTrace {
  std::string trace_id;             // = task_id
  std::string task_type;            // "Compaction" / "BulkLoad"
  std::string source_node_id;       // 来源节点
  int64_t start_time_ms = 0;       // 链路开始时间
  int64_t end_time_ms = 0;         // 链路结束时间
  int64_t total_duration_ms = 0;   // 端到端耗时
  bool is_complete = false;
  SpanStatus final_status = SpanStatus::kOk;

  std::vector<TraceSpan> spans;    // 所有 Span (按时间排序)

  // 关键阶段耗时
  int64_t queue_duration_ms = 0;    // 排队阶段
  int64_t schedule_duration_ms = 0; // 调度阶段
  int64_t execute_duration_ms = 0;  // 执行阶段
  int64_t transfer_duration_ms = 0; // 数据传输阶段
};

// ============================================================================
// TaskTracer 配置
// ============================================================================
struct TaskTracerConfig {
  uint32_t max_traces = 5000;          // 最大保留追踪链路数
  uint32_t max_spans_per_trace = 100;  // 单个 Trace 最大 Span 数
  bool enabled = true;                  // 是否启用
};

// ============================================================================
// TaskTracer - 任务链路追踪器
// ============================================================================
class TaskTracer {
 public:
  explicit TaskTracer(const TaskTracerConfig& config = TaskTracerConfig());

  // 是否启用
  bool IsEnabled() const { return config_.enabled; }

  // =========================================================================
  // Span 创建与管理
  // =========================================================================

  // 开始一个新的 Span
  // 返回 span_id
  std::string StartSpan(const std::string& trace_id,
                        const std::string& operation,
                        const std::string& component,
                        const std::string& parent_span_id = "");

  // 结束 Span
  void FinishSpan(const std::string& trace_id,
                  const std::string& span_id,
                  SpanStatus status = SpanStatus::kOk,
                  const std::string& error_message = "");

  // 为 Span 添加标签
  void SetSpanTag(const std::string& trace_id,
                  const std::string& span_id,
                  const std::string& key,
                  const std::string& value);

  // 为 Span 添加日志事件
  void AddSpanLog(const std::string& trace_id,
                  const std::string& span_id,
                  const std::string& message);

  // =========================================================================
  // 便捷方法 - 预定义的关键操作
  // =========================================================================

  // 任务提交 (TendisPlus → ControlPlane)
  std::string TraceTaskSubmit(const std::string& task_id,
                              const std::string& task_type,
                              const std::string& source_node_id);

  // 任务入队调度
  std::string TraceTaskSchedule(const std::string& task_id,
                                const std::string& parent_span_id = "");

  // 任务分配给 Worker
  std::string TraceTaskAssign(const std::string& task_id,
                              const std::string& worker_id,
                              const std::string& parent_span_id = "");

  // 任务开始执行 (Worker 侧)
  std::string TraceTaskExecute(const std::string& task_id,
                               const std::string& worker_id,
                               const std::string& parent_span_id = "");

  // 任务完成
  void TraceTaskComplete(const std::string& task_id,
                         const std::string& span_id,
                         bool success,
                         const std::string& error_message = "");

  // SST 文件传输 (Bulk Load 专用)
  std::string TraceSSTTransfer(const std::string& task_id,
                               const std::string& parent_span_id = "");

  // SST 文件注入 (Bulk Load 专用)
  std::string TraceSSTIngest(const std::string& task_id,
                             const std::string& parent_span_id = "");

  // =========================================================================
  // 查询接口
  // =========================================================================

  // 获取单个任务的完整追踪链路
  std::shared_ptr<TaskTrace> GetTrace(const std::string& trace_id) const;

  // 获取最近的追踪链路列表
  std::vector<std::shared_ptr<TaskTrace>> GetRecentTraces(
    uint32_t limit = 50) const;

  // 获取指定时间范围内的追踪链路
  std::vector<std::shared_ptr<TaskTrace>> GetTracesByTimeRange(
    int64_t start_ms, int64_t end_ms, uint32_t limit = 100) const;

  // 获取慢查询追踪 (执行时间 > threshold_ms)
  std::vector<std::shared_ptr<TaskTrace>> GetSlowTraces(
    uint64_t threshold_ms, uint32_t limit = 50) const;

  // 获取失败的追踪链路
  std::vector<std::shared_ptr<TaskTrace>> GetFailedTraces(
    uint32_t limit = 50) const;

  // 统计信息
  uint32_t GetTraceCount() const;
  uint32_t GetActiveTraceCount() const;

 private:
  // 确保 Trace 存在 (不存在则创建)
  std::shared_ptr<TaskTrace> EnsureTrace(const std::string& trace_id);

  // 查找 Span
  TraceSpan* FindSpan(TaskTrace& trace, const std::string& span_id);

  // 生成 Span ID
  std::string GenerateSpanId();

  // 当前时间
  int64_t NowMs() const;

  // 修剪过期追踪
  void PruneOldTraces();

  // 更新 Trace 的汇总信息
  void UpdateTraceSummary(TaskTrace& trace);

  TaskTracerConfig config_;

  // 追踪数据
  mutable std::mutex traces_mutex_;
  std::unordered_map<std::string, std::shared_ptr<TaskTrace>> traces_;

  // 按时间排序的 Trace ID 列表 (用于 LRU 淘汰)
  std::deque<std::string> trace_order_;

  // Span ID 计数器
  std::atomic<uint64_t> span_counter_{0};
};

}  // namespace control_plane
}  // namespace tendisplus
