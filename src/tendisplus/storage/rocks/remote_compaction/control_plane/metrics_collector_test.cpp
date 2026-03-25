// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

// Unit tests for MetricsCollector (metrics_collector.h/cc)
// Tests: percentile computation, sliding window throughput,
//        boundary conditions, sample pruning

#include "gtest/gtest.h"
#include "metrics_collector.h"

#include <thread>
#include <vector>

namespace tendisplus {
namespace control_plane {

// ============================================================================
// 空状态测试
// ============================================================================
TEST(MetricsCollector, EmptyState) {
  MetricsCollector collector;

  auto exec = collector.GetExecutionLatencyPercentiles();
  EXPECT_EQ(exec.sample_count, 0u);
  EXPECT_EQ(exec.p50_ms, 0u);
  EXPECT_EQ(exec.p95_ms, 0u);
  EXPECT_EQ(exec.p99_ms, 0u);
  EXPECT_EQ(exec.min_ms, 0u);
  EXPECT_EQ(exec.max_ms, 0u);
  EXPECT_DOUBLE_EQ(exec.avg_ms, 0.0);

  auto queue = collector.GetQueueLatencyPercentiles();
  EXPECT_EQ(queue.sample_count, 0u);

  EXPECT_EQ(collector.GetCompletedLastMinute(), 0u);
  EXPECT_EQ(collector.GetFailedLastMinute(), 0u);
  EXPECT_EQ(collector.GetCompletedLastHour(), 0u);
  EXPECT_EQ(collector.GetFailedLastHour(), 0u);
}

// ============================================================================
// 单个样本的百分位计算
// ============================================================================
TEST(MetricsCollector, SingleSamplePercentiles) {
  MetricsCollector collector;

  collector.RecordExecutionLatency(100);

  auto exec = collector.GetExecutionLatencyPercentiles();
  EXPECT_EQ(exec.sample_count, 1u);
  EXPECT_EQ(exec.p50_ms, 100u);
  EXPECT_EQ(exec.p95_ms, 100u);
  EXPECT_EQ(exec.p99_ms, 100u);
  EXPECT_EQ(exec.min_ms, 100u);
  EXPECT_EQ(exec.max_ms, 100u);
  EXPECT_DOUBLE_EQ(exec.avg_ms, 100.0);
}

// ============================================================================
// 多个样本的百分位计算 — 验证 P50/P95/P99 排序算法
// ============================================================================
TEST(MetricsCollector, MultiSamplePercentiles) {
  MetricsCollector collector;

  // 录入 100 个样本: 1, 2, 3, ..., 100
  for (uint64_t i = 1; i <= 100; i++) {
    collector.RecordExecutionLatency(i);
  }

  auto exec = collector.GetExecutionLatencyPercentiles();
  EXPECT_EQ(exec.sample_count, 100u);
  EXPECT_EQ(exec.min_ms, 1u);
  EXPECT_EQ(exec.max_ms, 100u);

  // nearest-rank 百分位计算:
  // P50 = ceil(50/100 * 100) - 1 = 49 -> values[49] = 50
  EXPECT_EQ(exec.p50_ms, 50u);

  // P95 = ceil(95/100 * 100) - 1 = 94 -> values[94] = 95
  EXPECT_EQ(exec.p95_ms, 95u);

  // P99 = ceil(99/100 * 100) - 1 = 98 -> values[98] = 99
  EXPECT_EQ(exec.p99_ms, 99u);

  // 平均值 = (1+2+...+100)/100 = 50.5
  EXPECT_NEAR(exec.avg_ms, 50.5, 0.01);
}

// ============================================================================
// 排队延迟百分位
// ============================================================================
TEST(MetricsCollector, QueueLatencyPercentiles) {
  MetricsCollector collector;

  // 录入 50 个样本: 10, 20, 30, ..., 500
  for (uint64_t i = 1; i <= 50; i++) {
    collector.RecordQueueLatency(i * 10);
  }

  auto queue = collector.GetQueueLatencyPercentiles();
  EXPECT_EQ(queue.sample_count, 50u);
  EXPECT_EQ(queue.min_ms, 10u);
  EXPECT_EQ(queue.max_ms, 500u);

  // P50 = ceil(50/100 * 50) - 1 = 24 -> values[24] = 250
  EXPECT_EQ(queue.p50_ms, 250u);
}

// ============================================================================
// 任务完成/失败事件计数 — 滑动窗口
// ============================================================================
TEST(MetricsCollector, CompletedAndFailedCounting) {
  MetricsCollector collector;

  // 记录 10 个完成事件
  for (int i = 0; i < 10; i++) {
    collector.RecordTaskCompleted();
  }

  // 记录 3 个失败事件
  for (int i = 0; i < 3; i++) {
    collector.RecordTaskFailed();
  }

  // 都在最近 1 分钟内
  EXPECT_EQ(collector.GetCompletedLastMinute(), 10u);
  EXPECT_EQ(collector.GetFailedLastMinute(), 3u);

  // 也在最近 1 小时内
  EXPECT_EQ(collector.GetCompletedLastHour(), 10u);
  EXPECT_EQ(collector.GetFailedLastHour(), 3u);
}

// ============================================================================
// GetWindowThroughput 综合查询
// ============================================================================
TEST(MetricsCollector, WindowThroughput) {
  MetricsCollector collector;

  for (int i = 0; i < 5; i++) {
    collector.RecordTaskCompleted();
  }
  for (int i = 0; i < 2; i++) {
    collector.RecordTaskFailed();
  }

  auto tp = collector.GetWindowThroughput();
  EXPECT_EQ(tp.completed_last_minute, 5u);
  EXPECT_EQ(tp.failed_last_minute, 2u);
  EXPECT_EQ(tp.completed_last_hour, 5u);
  EXPECT_EQ(tp.failed_last_hour, 2u);
}

// ============================================================================
// 自定义配置
// ============================================================================
TEST(MetricsCollector, CustomConfig) {
  MetricsCollectorConfig config;
  config.latency_window_sec = 10;     // 10 秒窗口
  config.max_latency_samples = 5;     // 最多 5 个样本
  config.throughput_window_sec = 60;  // 1 分钟窗口

  MetricsCollector collector(config);

  // 录入 10 个样本，但最大只保留 5 个
  for (uint64_t i = 1; i <= 10; i++) {
    collector.RecordExecutionLatency(i * 100);
  }

  auto exec = collector.GetExecutionLatencyPercentiles();
  // 由于 max_latency_samples = 5，前面的样本被挤出
  EXPECT_LE(exec.sample_count, 5u);
}

// ============================================================================
// 乱序延迟样本的百分位正确性（内部排序后计算）
// ============================================================================
TEST(MetricsCollector, UnsortedSamples) {
  MetricsCollector collector;

  // 乱序录入
  collector.RecordExecutionLatency(500);
  collector.RecordExecutionLatency(100);
  collector.RecordExecutionLatency(300);
  collector.RecordExecutionLatency(200);
  collector.RecordExecutionLatency(400);

  auto exec = collector.GetExecutionLatencyPercentiles();
  EXPECT_EQ(exec.sample_count, 5u);
  EXPECT_EQ(exec.min_ms, 100u);
  EXPECT_EQ(exec.max_ms, 500u);

  // 排序后: [100, 200, 300, 400, 500]
  // P50 = ceil(50/100 * 5) - 1 = 2 -> values[2] = 300
  EXPECT_EQ(exec.p50_ms, 300u);
}

// ============================================================================
// 大量样本高精度百分位测试
// ============================================================================
TEST(MetricsCollector, HighPrecisionPercentiles) {
  MetricsCollector collector;

  // 1000 个样本
  for (uint64_t i = 1; i <= 1000; i++) {
    collector.RecordExecutionLatency(i);
  }

  auto exec = collector.GetExecutionLatencyPercentiles();
  EXPECT_EQ(exec.sample_count, 1000u);
  EXPECT_EQ(exec.min_ms, 1u);
  EXPECT_EQ(exec.max_ms, 1000u);

  // P50 = 500, P95 = 950, P99 = 990
  EXPECT_EQ(exec.p50_ms, 500u);
  EXPECT_EQ(exec.p95_ms, 950u);
  EXPECT_EQ(exec.p99_ms, 990u);
}

// ============================================================================
// 默认配置值验证
// ============================================================================
TEST(MetricsCollector, DefaultConfig) {
  MetricsCollectorConfig config;
  EXPECT_EQ(config.latency_window_sec, 300u);
  EXPECT_EQ(config.max_latency_samples, 10000u);
  EXPECT_EQ(config.throughput_bucket_sec, 10u);
  EXPECT_EQ(config.throughput_window_sec, 3600u);
}

// ============================================================================
// PercentileLatency 默认值
// ============================================================================
TEST(MetricsCollector, PercentileLatencyDefaults) {
  PercentileLatency pl;
  EXPECT_EQ(pl.p50_ms, 0u);
  EXPECT_EQ(pl.p95_ms, 0u);
  EXPECT_EQ(pl.p99_ms, 0u);
  EXPECT_EQ(pl.min_ms, 0u);
  EXPECT_EQ(pl.max_ms, 0u);
  EXPECT_DOUBLE_EQ(pl.avg_ms, 0.0);
  EXPECT_EQ(pl.sample_count, 0u);
}

// ============================================================================
// WindowThroughput 默认值
// ============================================================================
TEST(MetricsCollector, WindowThroughputDefaults) {
  WindowThroughput wt;
  EXPECT_EQ(wt.completed_last_minute, 0u);
  EXPECT_EQ(wt.failed_last_minute, 0u);
  EXPECT_EQ(wt.completed_last_hour, 0u);
  EXPECT_EQ(wt.failed_last_hour, 0u);
}

// ============================================================================
// 并发安全性基本验证
// ============================================================================
TEST(MetricsCollector, ConcurrentRecording) {
  MetricsCollector collector;
  const int kThreads = 4;
  const int kRecordsPerThread = 100;

  std::vector<std::thread> threads;
  for (int t = 0; t < kThreads; t++) {
    threads.emplace_back([&collector, t]() {
      for (int i = 0; i < kRecordsPerThread; i++) {
        collector.RecordExecutionLatency(t * 100 + i);
        collector.RecordQueueLatency(t * 50 + i);
        if (i % 2 == 0) {
          collector.RecordTaskCompleted();
        } else {
          collector.RecordTaskFailed();
        }
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  // 不检查精确值，只要不崩溃且数据合理
  auto exec = collector.GetExecutionLatencyPercentiles();
  EXPECT_GT(exec.sample_count, 0u);
  EXPECT_LE(exec.sample_count, static_cast<uint32_t>(kThreads * kRecordsPerThread));

  auto queue = collector.GetQueueLatencyPercentiles();
  EXPECT_GT(queue.sample_count, 0u);

  // 总完成数 = 4线程 * 50个/线程 = 200
  EXPECT_EQ(collector.GetCompletedLastMinute(),
            static_cast<uint64_t>(kThreads * kRecordsPerThread / 2));
  EXPECT_EQ(collector.GetFailedLastMinute(),
            static_cast<uint64_t>(kThreads * kRecordsPerThread / 2));
}

}  // namespace control_plane
}  // namespace tendisplus
