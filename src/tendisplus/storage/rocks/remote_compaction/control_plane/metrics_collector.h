// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

// MetricsCollector - 滑动窗口指标收集器
// 基于 CaaS-LSM 观测平面设计
//
// 功能：
// 1. 真实的延迟百分位计算 (P50/P95/P99) — 基于排序统计
// 2. 滑动窗口吞吐量 (每分钟完成/失败数)
// 3. Worker 小时级统计
// 4. 为 AlertManager 提供 MetricsSnapshot

#pragma once

#include <atomic>
#include <chrono>
#include <deque>
#include <mutex>
#include <string>
#include <vector>

namespace tendisplus {
namespace control_plane {

// ============================================================================
// 延迟样本
// ============================================================================
struct LatencySample {
  int64_t timestamp_ms = 0;   // 采样时间
  uint64_t value_ms = 0;      // 延迟值 (毫秒)
};

// ============================================================================
// 滑动窗口计数器
// ============================================================================
struct WindowCounter {
  int64_t timestamp_ms = 0;
  uint64_t count = 0;
};

// ============================================================================
// 百分位延迟结果
// ============================================================================
struct PercentileLatency {
  uint64_t p50_ms = 0;
  uint64_t p95_ms = 0;
  uint64_t p99_ms = 0;
  uint64_t min_ms = 0;
  uint64_t max_ms = 0;
  double avg_ms = 0.0;
  uint32_t sample_count = 0;
};

// ============================================================================
// 滑动窗口吞吐量
// ============================================================================
struct WindowThroughput {
  uint64_t completed_last_minute = 0;
  uint64_t failed_last_minute = 0;
  uint64_t completed_last_hour = 0;
  uint64_t failed_last_hour = 0;
};

// ============================================================================
// MetricsCollector 配置
// ============================================================================
struct MetricsCollectorConfig {
  uint32_t latency_window_sec = 300;       // 延迟采样窗口 (5 分钟)
  uint32_t max_latency_samples = 10000;    // 最大延迟采样数
  uint32_t throughput_bucket_sec = 10;     // 吞吐量桶大小 (秒)
  uint32_t throughput_window_sec = 3600;   // 吞吐量窗口 (1 小时)
};

// ============================================================================
// MetricsCollector - 滑动窗口指标收集器
// ============================================================================
class MetricsCollector {
 public:
  explicit MetricsCollector(const MetricsCollectorConfig& config =
                              MetricsCollectorConfig());

  // 记录执行延迟样本
  void RecordExecutionLatency(uint64_t latency_ms);

  // 记录排队延迟样本
  void RecordQueueLatency(uint64_t latency_ms);

  // 记录任务完成事件
  void RecordTaskCompleted();

  // 记录任务失败事件
  void RecordTaskFailed();

  // 获取执行延迟百分位
  PercentileLatency GetExecutionLatencyPercentiles() const;

  // 获取排队延迟百分位
  PercentileLatency GetQueueLatencyPercentiles() const;

  // 获取滑动窗口吞吐量
  WindowThroughput GetWindowThroughput() const;

  // 获取最近 1 分钟完成数
  uint64_t GetCompletedLastMinute() const;

  // 获取最近 1 分钟失败数
  uint64_t GetFailedLastMinute() const;

  // 获取最近 1 小时完成数
  uint64_t GetCompletedLastHour() const;

  // 获取最近 1 小时失败数
  uint64_t GetFailedLastHour() const;

 private:
  // 计算百分位
  PercentileLatency ComputePercentiles(
    const std::deque<LatencySample>& samples) const;

  // 清理过期样本
  void PruneExpiredSamples(std::deque<LatencySample>& samples) const;

  // 清理过期计数器
  void PruneExpiredCounters(std::deque<WindowCounter>& counters) const;

  // 统计窗口内计数
  uint64_t CountInWindow(const std::deque<WindowCounter>& counters,
                         int64_t window_ms) const;

  // 当前时间戳
  int64_t NowMs() const;

  MetricsCollectorConfig config_;

  // 执行延迟样本
  mutable std::mutex exec_latency_mutex_;
  std::deque<LatencySample> exec_latency_samples_;

  // 排队延迟样本
  mutable std::mutex queue_latency_mutex_;
  std::deque<LatencySample> queue_latency_samples_;

  // 完成事件计数
  mutable std::mutex completed_mutex_;
  std::deque<WindowCounter> completed_events_;

  // 失败事件计数
  mutable std::mutex failed_mutex_;
  std::deque<WindowCounter> failed_events_;
};

}  // namespace control_plane
}  // namespace tendisplus
