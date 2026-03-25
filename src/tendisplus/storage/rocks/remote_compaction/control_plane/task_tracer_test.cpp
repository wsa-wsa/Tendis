// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

// Unit tests for TaskTracer (task_tracer.h/cc)
// Tests: span lifecycle, convenience methods, queries,
//        LRU pruning, disabled mode, concurrent access

#include "gtest/gtest.h"
#include "task_tracer.h"

#include <thread>
#include <vector>

namespace tendisplus {
namespace control_plane {

// ============================================================================
// 基础 Span 创建与结束
// ============================================================================
TEST(TaskTracer, BasicSpanLifecycle) {
  TaskTracer tracer;

  auto span_id = tracer.StartSpan("task-1", "test_op", "TestComponent");
  EXPECT_FALSE(span_id.empty());

  // Trace 应该存在
  auto trace = tracer.GetTrace("task-1");
  ASSERT_NE(trace, nullptr);
  EXPECT_EQ(trace->trace_id, "task-1");
  EXPECT_EQ(trace->spans.size(), 1u);
  EXPECT_EQ(trace->spans[0].operation, "test_op");
  EXPECT_EQ(trace->spans[0].component, "TestComponent");
  EXPECT_GT(trace->spans[0].start_time_ms, 0);
  EXPECT_FALSE(trace->spans[0].IsFinished());

  // 结束 Span
  tracer.FinishSpan("task-1", span_id, SpanStatus::kOk);

  trace = tracer.GetTrace("task-1");
  EXPECT_TRUE(trace->spans[0].IsFinished());
  EXPECT_GT(trace->spans[0].end_time_ms, 0);
  EXPECT_GE(trace->spans[0].duration_ms, 0);
  EXPECT_EQ(trace->spans[0].status, SpanStatus::kOk);
}

// ============================================================================
// Span 带错误信息
// ============================================================================
TEST(TaskTracer, SpanWithError) {
  TaskTracer tracer;

  auto span_id = tracer.StartSpan("task-2", "error_op", "TestComp");
  tracer.FinishSpan("task-2", span_id, SpanStatus::kError, "disk full");

  auto trace = tracer.GetTrace("task-2");
  ASSERT_NE(trace, nullptr);
  EXPECT_EQ(trace->spans[0].status, SpanStatus::kError);
  EXPECT_EQ(trace->spans[0].error_message, "disk full");
}

// ============================================================================
// Span 标签和日志
// ============================================================================
TEST(TaskTracer, SpanTagsAndLogs) {
  TaskTracer tracer;

  auto span_id = tracer.StartSpan("task-3", "tagged_op", "Comp");

  tracer.SetSpanTag("task-3", span_id, "key1", "value1");
  tracer.SetSpanTag("task-3", span_id, "key2", "value2");
  tracer.AddSpanLog("task-3", span_id, "step 1 done");
  tracer.AddSpanLog("task-3", span_id, "step 2 done");

  auto trace = tracer.GetTrace("task-3");
  ASSERT_NE(trace, nullptr);
  EXPECT_EQ(trace->spans[0].tags.size(), 2u);
  EXPECT_EQ(trace->spans[0].tags["key1"], "value1");
  EXPECT_EQ(trace->spans[0].tags["key2"], "value2");
  EXPECT_EQ(trace->spans[0].logs.size(), 2u);
  EXPECT_EQ(trace->spans[0].logs[0].message, "step 1 done");
  EXPECT_EQ(trace->spans[0].logs[1].message, "step 2 done");
}

// ============================================================================
// 多 Span 父子关系
// ============================================================================
TEST(TaskTracer, ParentChildSpans) {
  TaskTracer tracer;

  auto parent_id = tracer.StartSpan("task-4", "parent_op", "ParentComp");
  auto child_id =
    tracer.StartSpan("task-4", "child_op", "ChildComp", parent_id);

  auto trace = tracer.GetTrace("task-4");
  ASSERT_NE(trace, nullptr);
  EXPECT_EQ(trace->spans.size(), 2u);
  EXPECT_TRUE(trace->spans[0].parent_span_id.empty());  // 根 Span
  EXPECT_EQ(trace->spans[1].parent_span_id, parent_id);  // 子 Span
}

// ============================================================================
// 便捷方法：完整 Compaction 任务追踪链路
// ============================================================================
TEST(TaskTracer, CompactionTraceFullLifecycle) {
  TaskTracer tracer;

  // 1. 提交
  auto submit_id =
    tracer.TraceTaskSubmit("compact-1", "Compaction", "node-1");
  EXPECT_FALSE(submit_id.empty());

  // 2. 调度
  auto schedule_id = tracer.TraceTaskSchedule("compact-1", submit_id);
  EXPECT_FALSE(schedule_id.empty());
  tracer.FinishSpan("compact-1", schedule_id);

  // 3. 分配
  auto assign_id =
    tracer.TraceTaskAssign("compact-1", "worker-1", schedule_id);
  EXPECT_FALSE(assign_id.empty());
  tracer.FinishSpan("compact-1", assign_id);

  // 4. 执行
  auto exec_id =
    tracer.TraceTaskExecute("compact-1", "worker-1", assign_id);
  EXPECT_FALSE(exec_id.empty());

  // 5. 完成
  tracer.TraceTaskComplete("compact-1", exec_id, true);

  // 验证 Trace
  auto trace = tracer.GetTrace("compact-1");
  ASSERT_NE(trace, nullptr);
  EXPECT_EQ(trace->task_type, "Compaction");
  EXPECT_EQ(trace->source_node_id, "node-1");
  EXPECT_TRUE(trace->is_complete);
  EXPECT_EQ(trace->final_status, SpanStatus::kOk);
  EXPECT_GE(trace->spans.size(), 4u);  // submit, schedule, assign, execute

  // 结束 submit span（模拟补充结束）
  tracer.FinishSpan("compact-1", submit_id);
}

// ============================================================================
// 便捷方法：Bulk Load 任务追踪（含 SST 阶段）
// ============================================================================
TEST(TaskTracer, BulkLoadTraceWithSST) {
  TaskTracer tracer;

  auto submit_id =
    tracer.TraceTaskSubmit("bulk-1", "BulkLoad", "node-2");

  auto exec_id = tracer.TraceTaskExecute("bulk-1", "worker-2", submit_id);

  // SST 传输
  auto transfer_id = tracer.TraceSSTTransfer("bulk-1", exec_id);
  EXPECT_FALSE(transfer_id.empty());
  tracer.FinishSpan("bulk-1", transfer_id);

  // SST 注入
  auto ingest_id = tracer.TraceSSTIngest("bulk-1", transfer_id);
  EXPECT_FALSE(ingest_id.empty());
  tracer.FinishSpan("bulk-1", ingest_id);

  auto trace = tracer.GetTrace("bulk-1");
  ASSERT_NE(trace, nullptr);
  EXPECT_EQ(trace->task_type, "BulkLoad");

  // 检查 SST 相关 Span 存在
  bool has_transfer = false, has_ingest = false;
  for (const auto& span : trace->spans) {
    if (span.operation == "sst_transfer") has_transfer = true;
    if (span.operation == "sst_ingest") has_ingest = true;
  }
  EXPECT_TRUE(has_transfer);
  EXPECT_TRUE(has_ingest);
}

// ============================================================================
// 失败任务追踪
// ============================================================================
TEST(TaskTracer, FailedTaskTrace) {
  TaskTracer tracer;

  auto submit_id =
    tracer.TraceTaskSubmit("fail-1", "Compaction", "node-3");
  auto exec_id =
    tracer.TraceTaskExecute("fail-1", "worker-3", submit_id);

  tracer.TraceTaskComplete("fail-1", exec_id, false, "OOM killed");

  auto trace = tracer.GetTrace("fail-1");
  ASSERT_NE(trace, nullptr);
  EXPECT_TRUE(trace->is_complete);
  EXPECT_EQ(trace->final_status, SpanStatus::kError);
}

// ============================================================================
// GetRecentTraces 查询
// ============================================================================
TEST(TaskTracer, GetRecentTraces) {
  TaskTracer tracer;

  // 创建 5 个 Trace
  for (int i = 0; i < 5; i++) {
    tracer.TraceTaskSubmit("recent-" + std::to_string(i), "Compaction",
                           "node-" + std::to_string(i));
  }

  auto recent = tracer.GetRecentTraces(3);
  EXPECT_EQ(recent.size(), 3u);

  // 应该返回最近的 3 个（倒序）
  EXPECT_EQ(recent[0]->trace_id, "recent-4");
  EXPECT_EQ(recent[1]->trace_id, "recent-3");
  EXPECT_EQ(recent[2]->trace_id, "recent-2");
}

// ============================================================================
// GetSlowTraces 查询
// ============================================================================
TEST(TaskTracer, GetSlowTraces) {
  TaskTracer tracer;

  // 创建一个快任务和一个慢任务
  auto fast_submit = tracer.TraceTaskSubmit("fast-1", "Compaction", "n1");
  auto fast_exec = tracer.TraceTaskExecute("fast-1", "w1", fast_submit);
  tracer.TraceTaskComplete("fast-1", fast_exec, true);
  tracer.FinishSpan("fast-1", fast_submit);

  auto slow_submit = tracer.TraceTaskSubmit("slow-1", "Compaction", "n2");
  auto slow_exec = tracer.TraceTaskExecute("slow-1", "w2", slow_submit);

  // 模拟慢操作 (等待一点时间)
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  tracer.TraceTaskComplete("slow-1", slow_exec, true);
  tracer.FinishSpan("slow-1", slow_submit);

  // 阈值 30ms → slow-1 应该被返回
  auto slow = tracer.GetSlowTraces(30, 10);
  EXPECT_GE(slow.size(), 1u);

  bool found_slow = false;
  for (const auto& t : slow) {
    if (t->trace_id == "slow-1") {
      found_slow = true;
      EXPECT_GE(t->total_duration_ms, 30);
    }
  }
  EXPECT_TRUE(found_slow);
}

// ============================================================================
// GetFailedTraces 查询
// ============================================================================
TEST(TaskTracer, GetFailedTraces) {
  TaskTracer tracer;

  // 成功任务
  auto ok_submit = tracer.TraceTaskSubmit("ok-1", "Compaction", "n1");
  auto ok_exec = tracer.TraceTaskExecute("ok-1", "w1", ok_submit);
  tracer.TraceTaskComplete("ok-1", ok_exec, true);

  // 失败任务
  auto fail_submit = tracer.TraceTaskSubmit("err-1", "Compaction", "n2");
  auto fail_exec = tracer.TraceTaskExecute("err-1", "w2", fail_submit);
  tracer.TraceTaskComplete("err-1", fail_exec, false, "network error");

  auto failed = tracer.GetFailedTraces(10);
  EXPECT_GE(failed.size(), 1u);

  bool found_failed = false;
  for (const auto& t : failed) {
    if (t->trace_id == "err-1") {
      found_failed = true;
      EXPECT_EQ(t->final_status, SpanStatus::kError);
    }
  }
  EXPECT_TRUE(found_failed);
}

// ============================================================================
// GetTraceCount / GetActiveTraceCount
// ============================================================================
TEST(TaskTracer, TraceCountStatistics) {
  TaskTracer tracer;

  EXPECT_EQ(tracer.GetTraceCount(), 0u);
  EXPECT_EQ(tracer.GetActiveTraceCount(), 0u);

  // 创建 3 个 Trace
  auto s1 = tracer.TraceTaskSubmit("t1", "Compaction", "n1");
  auto s2 = tracer.TraceTaskSubmit("t2", "Compaction", "n2");
  auto s3 = tracer.TraceTaskSubmit("t3", "BulkLoad", "n3");

  EXPECT_EQ(tracer.GetTraceCount(), 3u);
  EXPECT_EQ(tracer.GetActiveTraceCount(), 3u);

  // 完成其中 1 个
  auto e1 = tracer.TraceTaskExecute("t1", "w1", s1);
  tracer.TraceTaskComplete("t1", e1, true);
  tracer.FinishSpan("t1", s1);

  EXPECT_EQ(tracer.GetTraceCount(), 3u);
  // t1 is_complete = true，但需要所有 span 都 finish
  // TraceTaskComplete 已标记 is_complete = true
  EXPECT_EQ(tracer.GetActiveTraceCount(), 2u);
}

// ============================================================================
// LRU 淘汰测试（max_traces）
// ============================================================================
TEST(TaskTracer, LRUPruning) {
  TaskTracerConfig config;
  config.max_traces = 5;
  TaskTracer tracer(config);

  // 创建 8 个 Trace，应该淘汰前 3 个
  for (int i = 0; i < 8; i++) {
    tracer.TraceTaskSubmit(
      "lru-" + std::to_string(i), "Compaction", "node");
  }

  EXPECT_LE(tracer.GetTraceCount(), 5u);

  // 最早创建的应该被淘汰
  EXPECT_EQ(tracer.GetTrace("lru-0"), nullptr);
  EXPECT_EQ(tracer.GetTrace("lru-1"), nullptr);
  EXPECT_EQ(tracer.GetTrace("lru-2"), nullptr);

  // 较新的应该保留
  EXPECT_NE(tracer.GetTrace("lru-5"), nullptr);
  EXPECT_NE(tracer.GetTrace("lru-6"), nullptr);
  EXPECT_NE(tracer.GetTrace("lru-7"), nullptr);
}

// ============================================================================
// Span 数量限制（max_spans_per_trace）
// ============================================================================
TEST(TaskTracer, MaxSpansPerTrace) {
  TaskTracerConfig config;
  config.max_spans_per_trace = 3;
  TaskTracer tracer(config);

  // 尝试创建 5 个 Span，但最多允许 3 个
  auto id1 = tracer.StartSpan("limited-1", "op1", "comp");
  auto id2 = tracer.StartSpan("limited-1", "op2", "comp");
  auto id3 = tracer.StartSpan("limited-1", "op3", "comp");
  auto id4 = tracer.StartSpan("limited-1", "op4", "comp");  // 应该被拒绝
  auto id5 = tracer.StartSpan("limited-1", "op5", "comp");  // 应该被拒绝

  EXPECT_FALSE(id1.empty());
  EXPECT_FALSE(id2.empty());
  EXPECT_FALSE(id3.empty());
  EXPECT_TRUE(id4.empty());
  EXPECT_TRUE(id5.empty());

  auto trace = tracer.GetTrace("limited-1");
  EXPECT_EQ(trace->spans.size(), 3u);
}

// ============================================================================
// 禁用模式
// ============================================================================
TEST(TaskTracer, DisabledMode) {
  TaskTracerConfig config;
  config.enabled = false;
  TaskTracer tracer(config);

  EXPECT_FALSE(tracer.IsEnabled());

  auto span_id = tracer.StartSpan("disabled-1", "op", "comp");
  EXPECT_TRUE(span_id.empty());

  auto submit_id =
    tracer.TraceTaskSubmit("disabled-2", "Compaction", "node");
  EXPECT_TRUE(submit_id.empty());

  EXPECT_EQ(tracer.GetTraceCount(), 0u);
  EXPECT_EQ(tracer.GetTrace("disabled-1"), nullptr);
}

// ============================================================================
// 不存在的 Trace/Span 操作不崩溃
// ============================================================================
TEST(TaskTracer, NonExistentOperations) {
  TaskTracer tracer;

  // 对不存在的 Trace 进行操作 — 不应崩溃
  tracer.FinishSpan("nonexistent", "span-0");
  tracer.SetSpanTag("nonexistent", "span-0", "key", "value");
  tracer.AddSpanLog("nonexistent", "span-0", "message");

  // 查询不存在的 Trace
  EXPECT_EQ(tracer.GetTrace("nonexistent"), nullptr);
}

// ============================================================================
// SpanStatus 枚举转换
// ============================================================================
TEST(TaskTracer, SpanStatusToString) {
  EXPECT_STREQ(SpanStatusToString(SpanStatus::kOk), "OK");
  EXPECT_STREQ(SpanStatusToString(SpanStatus::kError), "Error");
  EXPECT_STREQ(SpanStatusToString(SpanStatus::kTimeout), "Timeout");
  EXPECT_STREQ(SpanStatusToString(static_cast<SpanStatus>(99)), "Unknown");
}

// ============================================================================
// TraceSpan 默认值
// ============================================================================
TEST(TaskTracer, TraceSpanDefaults) {
  TraceSpan span;
  EXPECT_TRUE(span.span_id.empty());
  EXPECT_TRUE(span.trace_id.empty());
  EXPECT_TRUE(span.parent_span_id.empty());
  EXPECT_EQ(span.start_time_ms, 0);
  EXPECT_EQ(span.end_time_ms, 0);
  EXPECT_EQ(span.duration_ms, 0);
  EXPECT_EQ(span.status, SpanStatus::kOk);
  EXPECT_TRUE(span.tags.empty());
  EXPECT_TRUE(span.logs.empty());
  EXPECT_FALSE(span.IsFinished());
}

// ============================================================================
// TaskTrace 默认值
// ============================================================================
TEST(TaskTracer, TaskTraceDefaults) {
  TaskTrace trace;
  EXPECT_TRUE(trace.trace_id.empty());
  EXPECT_TRUE(trace.task_type.empty());
  EXPECT_EQ(trace.start_time_ms, 0);
  EXPECT_EQ(trace.end_time_ms, 0);
  EXPECT_EQ(trace.total_duration_ms, 0);
  EXPECT_FALSE(trace.is_complete);
  EXPECT_EQ(trace.final_status, SpanStatus::kOk);
  EXPECT_TRUE(trace.spans.empty());
  EXPECT_EQ(trace.queue_duration_ms, 0);
  EXPECT_EQ(trace.schedule_duration_ms, 0);
  EXPECT_EQ(trace.execute_duration_ms, 0);
  EXPECT_EQ(trace.transfer_duration_ms, 0);
}

// ============================================================================
// 并发安全性基本验证
// ============================================================================
TEST(TaskTracer, ConcurrentAccess) {
  TaskTracer tracer;
  const int kThreads = 4;
  const int kOpsPerThread = 50;

  std::vector<std::thread> threads;
  for (int t = 0; t < kThreads; t++) {
    threads.emplace_back([&tracer, t]() {
      for (int i = 0; i < kOpsPerThread; i++) {
        std::string task_id =
          "concurrent-" + std::to_string(t) + "-" + std::to_string(i);
        auto submit_id =
          tracer.TraceTaskSubmit(task_id, "Compaction", "node");
        auto exec_id = tracer.TraceTaskExecute(task_id, "worker", submit_id);
        tracer.TraceTaskComplete(task_id, exec_id, true);
        tracer.FinishSpan(task_id, submit_id);
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  // 不检查精确数量（LRU 可能已淘汰），只要不崩溃
  EXPECT_GT(tracer.GetTraceCount(), 0u);

  auto recent = tracer.GetRecentTraces(10);
  EXPECT_GT(recent.size(), 0u);
}

}  // namespace control_plane
}  // namespace tendisplus
