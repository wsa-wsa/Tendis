// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include "task_tracer.h"

#include <algorithm>
#include <iostream>
#include <sstream>

namespace tendisplus {
namespace control_plane {

TaskTracer::TaskTracer(const TaskTracerConfig& config)
    : config_(config) {}

int64_t TaskTracer::NowMs() const {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
    std::chrono::system_clock::now().time_since_epoch()).count();
}

std::string TaskTracer::GenerateSpanId() {
  uint64_t count = span_counter_.fetch_add(1);
  return "span_" + std::to_string(count);
}

// ============================================================================
// Span 创建与管理
// ============================================================================

std::string TaskTracer::StartSpan(const std::string& trace_id,
                                  const std::string& operation,
                                  const std::string& component,
                                  const std::string& parent_span_id) {
  if (!config_.enabled) return "";

  auto trace = EnsureTrace(trace_id);
  if (!trace) return "";

  std::lock_guard<std::mutex> lock(traces_mutex_);

  // 检查 Span 数量限制
  if (trace->spans.size() >= config_.max_spans_per_trace) {
    return "";
  }

  TraceSpan span;
  span.span_id = GenerateSpanId();
  span.trace_id = trace_id;
  span.parent_span_id = parent_span_id;
  span.operation = operation;
  span.component = component;
  span.start_time_ms = NowMs();

  trace->spans.push_back(span);

  return span.span_id;
}

void TaskTracer::FinishSpan(const std::string& trace_id,
                            const std::string& span_id,
                            SpanStatus status,
                            const std::string& error_message) {
  if (!config_.enabled) return;

  std::lock_guard<std::mutex> lock(traces_mutex_);

  auto it = traces_.find(trace_id);
  if (it == traces_.end()) return;

  auto* span = FindSpan(*it->second, span_id);
  if (!span) return;

  span->end_time_ms = NowMs();
  span->duration_ms = span->end_time_ms - span->start_time_ms;
  span->status = status;
  span->error_message = error_message;

  // 更新 Trace 汇总
  UpdateTraceSummary(*it->second);
}

void TaskTracer::SetSpanTag(const std::string& trace_id,
                            const std::string& span_id,
                            const std::string& key,
                            const std::string& value) {
  if (!config_.enabled) return;

  std::lock_guard<std::mutex> lock(traces_mutex_);

  auto it = traces_.find(trace_id);
  if (it == traces_.end()) return;

  auto* span = FindSpan(*it->second, span_id);
  if (span) {
    span->tags[key] = value;
  }
}

void TaskTracer::AddSpanLog(const std::string& trace_id,
                            const std::string& span_id,
                            const std::string& message) {
  if (!config_.enabled) return;

  std::lock_guard<std::mutex> lock(traces_mutex_);

  auto it = traces_.find(trace_id);
  if (it == traces_.end()) return;

  auto* span = FindSpan(*it->second, span_id);
  if (span) {
    TraceSpan::LogEvent log;
    log.timestamp_ms = NowMs();
    log.message = message;
    span->logs.push_back(log);
  }
}

// ============================================================================
// 便捷方法 - 预定义的关键操作
// ============================================================================

std::string TaskTracer::TraceTaskSubmit(const std::string& task_id,
                                       const std::string& task_type,
                                       const std::string& source_node_id) {
  auto span_id = StartSpan(task_id, "task_submit", "ControlPlane");

  if (!span_id.empty()) {
    SetSpanTag(task_id, span_id, "task_type", task_type);
    SetSpanTag(task_id, span_id, "source_node", source_node_id);

    std::lock_guard<std::mutex> lock(traces_mutex_);
    auto it = traces_.find(task_id);
    if (it != traces_.end()) {
      it->second->task_type = task_type;
      it->second->source_node_id = source_node_id;
      it->second->start_time_ms = NowMs();
    }
  }

  return span_id;
}

std::string TaskTracer::TraceTaskSchedule(const std::string& task_id,
                                          const std::string& parent_span_id) {
  return StartSpan(task_id, "task_schedule", "Scheduler", parent_span_id);
}

std::string TaskTracer::TraceTaskAssign(const std::string& task_id,
                                        const std::string& worker_id,
                                        const std::string& parent_span_id) {
  auto span_id = StartSpan(task_id, "task_assign", "WorkerManager",
                            parent_span_id);
  if (!span_id.empty()) {
    SetSpanTag(task_id, span_id, "worker_id", worker_id);

    std::lock_guard<std::mutex> lock(traces_mutex_);
    auto it = traces_.find(task_id);
    if (it != traces_.end()) {
      auto* span = FindSpan(*it->second, span_id);
      if (span) span->worker_id = worker_id;
    }
  }
  return span_id;
}

std::string TaskTracer::TraceTaskExecute(const std::string& task_id,
                                         const std::string& worker_id,
                                         const std::string& parent_span_id) {
  auto span_id = StartSpan(task_id, "task_execute", "CSAWorker",
                            parent_span_id);
  if (!span_id.empty()) {
    SetSpanTag(task_id, span_id, "worker_id", worker_id);

    std::lock_guard<std::mutex> lock(traces_mutex_);
    auto it = traces_.find(task_id);
    if (it != traces_.end()) {
      auto* span = FindSpan(*it->second, span_id);
      if (span) span->worker_id = worker_id;
    }
  }
  return span_id;
}

void TaskTracer::TraceTaskComplete(const std::string& task_id,
                                   const std::string& span_id,
                                   bool success,
                                   const std::string& error_message) {
  auto status = success ? SpanStatus::kOk : SpanStatus::kError;
  FinishSpan(task_id, span_id, status, error_message);

  // 更新 Trace 完成状态
  std::lock_guard<std::mutex> lock(traces_mutex_);
  auto it = traces_.find(task_id);
  if (it != traces_.end()) {
    it->second->is_complete = true;
    it->second->end_time_ms = NowMs();
    it->second->total_duration_ms =
      it->second->end_time_ms - it->second->start_time_ms;
    it->second->final_status = status;
  }
}

std::string TaskTracer::TraceSSTTransfer(const std::string& task_id,
                                         const std::string& parent_span_id) {
  return StartSpan(task_id, "sst_transfer", "SharedFS", parent_span_id);
}

std::string TaskTracer::TraceSSTIngest(const std::string& task_id,
                                       const std::string& parent_span_id) {
  return StartSpan(task_id, "sst_ingest", "TendisPlus", parent_span_id);
}

// ============================================================================
// 查询接口
// ============================================================================

std::shared_ptr<TaskTrace> TaskTracer::GetTrace(
    const std::string& trace_id) const {
  std::lock_guard<std::mutex> lock(traces_mutex_);
  auto it = traces_.find(trace_id);
  if (it != traces_.end()) {
    return it->second;
  }
  return nullptr;
}

std::vector<std::shared_ptr<TaskTrace>> TaskTracer::GetRecentTraces(
    uint32_t limit) const {
  std::lock_guard<std::mutex> lock(traces_mutex_);

  std::vector<std::shared_ptr<TaskTrace>> result;
  uint32_t count = 0;

  // 从最新的开始遍历
  for (auto it = trace_order_.rbegin();
       it != trace_order_.rend() && count < limit; ++it) {
    auto trace_it = traces_.find(*it);
    if (trace_it != traces_.end()) {
      result.push_back(trace_it->second);
      count++;
    }
  }

  return result;
}

std::vector<std::shared_ptr<TaskTrace>> TaskTracer::GetTracesByTimeRange(
    int64_t start_ms, int64_t end_ms, uint32_t limit) const {
  std::lock_guard<std::mutex> lock(traces_mutex_);

  std::vector<std::shared_ptr<TaskTrace>> result;

  for (const auto& [id, trace] : traces_) {
    if (trace->start_time_ms >= start_ms &&
        trace->start_time_ms <= end_ms) {
      result.push_back(trace);
      if (result.size() >= limit) break;
    }
  }

  // 按开始时间降序排列
  std::sort(result.begin(), result.end(),
    [](const auto& a, const auto& b) {
      return a->start_time_ms > b->start_time_ms;
    });

  return result;
}

std::vector<std::shared_ptr<TaskTrace>> TaskTracer::GetSlowTraces(
    uint64_t threshold_ms, uint32_t limit) const {
  std::lock_guard<std::mutex> lock(traces_mutex_);

  std::vector<std::shared_ptr<TaskTrace>> result;

  for (const auto& [id, trace] : traces_) {
    if (trace->is_complete &&
        trace->total_duration_ms > static_cast<int64_t>(threshold_ms)) {
      result.push_back(trace);
    }
  }

  // 按耗时降序排列
  std::sort(result.begin(), result.end(),
    [](const auto& a, const auto& b) {
      return a->total_duration_ms > b->total_duration_ms;
    });

  if (result.size() > limit) {
    result.resize(limit);
  }

  return result;
}

std::vector<std::shared_ptr<TaskTrace>> TaskTracer::GetFailedTraces(
    uint32_t limit) const {
  std::lock_guard<std::mutex> lock(traces_mutex_);

  std::vector<std::shared_ptr<TaskTrace>> result;

  for (const auto& [id, trace] : traces_) {
    if (trace->final_status == SpanStatus::kError ||
        trace->final_status == SpanStatus::kTimeout) {
      result.push_back(trace);
      if (result.size() >= limit) break;
    }
  }

  // 按时间降序排列
  std::sort(result.begin(), result.end(),
    [](const auto& a, const auto& b) {
      return a->start_time_ms > b->start_time_ms;
    });

  return result;
}

uint32_t TaskTracer::GetTraceCount() const {
  std::lock_guard<std::mutex> lock(traces_mutex_);
  return static_cast<uint32_t>(traces_.size());
}

uint32_t TaskTracer::GetActiveTraceCount() const {
  std::lock_guard<std::mutex> lock(traces_mutex_);
  uint32_t count = 0;
  for (const auto& [id, trace] : traces_) {
    if (!trace->is_complete) count++;
  }
  return count;
}

// ============================================================================
// 内部实现
// ============================================================================

std::shared_ptr<TaskTrace> TaskTracer::EnsureTrace(
    const std::string& trace_id) {
  std::lock_guard<std::mutex> lock(traces_mutex_);

  auto it = traces_.find(trace_id);
  if (it != traces_.end()) {
    return it->second;
  }

  // 修剪旧追踪
  PruneOldTraces();

  auto trace = std::make_shared<TaskTrace>();
  trace->trace_id = trace_id;
  trace->start_time_ms = NowMs();

  traces_[trace_id] = trace;
  trace_order_.push_back(trace_id);

  return trace;
}

TraceSpan* TaskTracer::FindSpan(TaskTrace& trace,
                                const std::string& span_id) {
  for (auto& span : trace.spans) {
    if (span.span_id == span_id) {
      return &span;
    }
  }
  return nullptr;
}

void TaskTracer::PruneOldTraces() {
  while (traces_.size() >= config_.max_traces && !trace_order_.empty()) {
    const auto& oldest_id = trace_order_.front();
    traces_.erase(oldest_id);
    trace_order_.pop_front();
  }
}

void TaskTracer::UpdateTraceSummary(TaskTrace& trace) {
  // 计算各阶段耗时
  trace.queue_duration_ms = 0;
  trace.schedule_duration_ms = 0;
  trace.execute_duration_ms = 0;
  trace.transfer_duration_ms = 0;

  for (const auto& span : trace.spans) {
    if (!span.IsFinished()) continue;

    if (span.operation == "task_schedule") {
      trace.schedule_duration_ms += span.duration_ms;
    } else if (span.operation == "task_execute") {
      trace.execute_duration_ms += span.duration_ms;
    } else if (span.operation == "sst_transfer") {
      trace.transfer_duration_ms += span.duration_ms;
    }
  }

  // 排队时间 = 从 submit 结束到 execute 开始
  // 简化计算：从 task_submit 结束到 task_execute 开始
  int64_t submit_end = 0;
  int64_t execute_start = 0;
  for (const auto& span : trace.spans) {
    if (span.operation == "task_submit" && span.end_time_ms > 0) {
      submit_end = span.end_time_ms;
    }
    if (span.operation == "task_execute" && span.start_time_ms > 0) {
      execute_start = span.start_time_ms;
    }
  }
  if (submit_end > 0 && execute_start > submit_end) {
    trace.queue_duration_ms = execute_start - submit_end;
  }

  // 检查是否所有 Span 都已完成
  bool all_finished = true;
  for (const auto& span : trace.spans) {
    if (!span.IsFinished()) {
      all_finished = false;
      break;
    }
  }

  if (all_finished && !trace.spans.empty()) {
    trace.is_complete = true;
    trace.end_time_ms = NowMs();
    trace.total_duration_ms = trace.end_time_ms - trace.start_time_ms;

    // 最终状态取自最后一个 Span
    trace.final_status = trace.spans.back().status;
  }
}

}  // namespace control_plane
}  // namespace tendisplus
