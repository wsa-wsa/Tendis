// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include "metrics_collector.h"

#include <algorithm>
#include <numeric>

namespace tendisplus {
namespace control_plane {

MetricsCollector::MetricsCollector(const MetricsCollectorConfig& config)
    : config_(config) {}

int64_t MetricsCollector::NowMs() const {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
    std::chrono::system_clock::now().time_since_epoch()).count();
}

// ============================================================================
// 记录接口
// ============================================================================

void MetricsCollector::RecordExecutionLatency(uint64_t latency_ms) {
  LatencySample sample;
  sample.timestamp_ms = NowMs();
  sample.value_ms = latency_ms;

  std::lock_guard<std::mutex> lock(exec_latency_mutex_);
  exec_latency_samples_.push_back(sample);

  // 清理过期样本
  PruneExpiredSamples(exec_latency_samples_);

  // 限制最大样本数
  while (exec_latency_samples_.size() > config_.max_latency_samples) {
    exec_latency_samples_.pop_front();
  }
}

void MetricsCollector::RecordQueueLatency(uint64_t latency_ms) {
  LatencySample sample;
  sample.timestamp_ms = NowMs();
  sample.value_ms = latency_ms;

  std::lock_guard<std::mutex> lock(queue_latency_mutex_);
  queue_latency_samples_.push_back(sample);

  PruneExpiredSamples(queue_latency_samples_);

  while (queue_latency_samples_.size() > config_.max_latency_samples) {
    queue_latency_samples_.pop_front();
  }
}

void MetricsCollector::RecordTaskCompleted() {
  WindowCounter counter;
  counter.timestamp_ms = NowMs();
  counter.count = 1;

  std::lock_guard<std::mutex> lock(completed_mutex_);
  completed_events_.push_back(counter);
  PruneExpiredCounters(completed_events_);
}

void MetricsCollector::RecordTaskFailed() {
  WindowCounter counter;
  counter.timestamp_ms = NowMs();
  counter.count = 1;

  std::lock_guard<std::mutex> lock(failed_mutex_);
  failed_events_.push_back(counter);
  PruneExpiredCounters(failed_events_);
}

// ============================================================================
// 查询接口
// ============================================================================

PercentileLatency MetricsCollector::GetExecutionLatencyPercentiles() const {
  std::lock_guard<std::mutex> lock(exec_latency_mutex_);
  return ComputePercentiles(exec_latency_samples_);
}

PercentileLatency MetricsCollector::GetQueueLatencyPercentiles() const {
  std::lock_guard<std::mutex> lock(queue_latency_mutex_);
  return ComputePercentiles(queue_latency_samples_);
}

WindowThroughput MetricsCollector::GetWindowThroughput() const {
  WindowThroughput result;
  result.completed_last_minute = GetCompletedLastMinute();
  result.failed_last_minute = GetFailedLastMinute();
  result.completed_last_hour = GetCompletedLastHour();
  result.failed_last_hour = GetFailedLastHour();
  return result;
}

uint64_t MetricsCollector::GetCompletedLastMinute() const {
  std::lock_guard<std::mutex> lock(completed_mutex_);
  return CountInWindow(completed_events_, 60 * 1000);
}

uint64_t MetricsCollector::GetFailedLastMinute() const {
  std::lock_guard<std::mutex> lock(failed_mutex_);
  return CountInWindow(failed_events_, 60 * 1000);
}

uint64_t MetricsCollector::GetCompletedLastHour() const {
  std::lock_guard<std::mutex> lock(completed_mutex_);
  return CountInWindow(completed_events_, 3600 * 1000);
}

uint64_t MetricsCollector::GetFailedLastHour() const {
  std::lock_guard<std::mutex> lock(failed_mutex_);
  return CountInWindow(failed_events_, 3600 * 1000);
}

// ============================================================================
// 内部实现
// ============================================================================

PercentileLatency MetricsCollector::ComputePercentiles(
    const std::deque<LatencySample>& samples) const {
  PercentileLatency result;

  if (samples.empty()) {
    return result;
  }

  // 窗口内有效样本
  int64_t now = NowMs();
  int64_t window_ms = static_cast<int64_t>(config_.latency_window_sec) * 1000;
  int64_t cutoff = now - window_ms;

  std::vector<uint64_t> values;
  values.reserve(samples.size());

  for (const auto& s : samples) {
    if (s.timestamp_ms >= cutoff) {
      values.push_back(s.value_ms);
    }
  }

  if (values.empty()) {
    return result;
  }

  // 排序以计算百分位
  std::sort(values.begin(), values.end());

  result.sample_count = static_cast<uint32_t>(values.size());
  result.min_ms = values.front();
  result.max_ms = values.back();

  // 平均值
  uint64_t total = std::accumulate(values.begin(), values.end(), 0ULL);
  result.avg_ms = static_cast<double>(total) / values.size();

  // 百分位 (使用最近排名法)
  auto percentile = [&values](double p) -> uint64_t {
    size_t n = values.size();
    if (n == 0) return 0;
    size_t idx = static_cast<size_t>(std::ceil(p / 100.0 * n)) - 1;
    if (idx >= n) idx = n - 1;
    return values[idx];
  };

  result.p50_ms = percentile(50.0);
  result.p95_ms = percentile(95.0);
  result.p99_ms = percentile(99.0);

  return result;
}

void MetricsCollector::PruneExpiredSamples(
    std::deque<LatencySample>& samples) const {
  int64_t cutoff = NowMs() -
    static_cast<int64_t>(config_.latency_window_sec) * 1000;
  while (!samples.empty() && samples.front().timestamp_ms < cutoff) {
    samples.pop_front();
  }
}

void MetricsCollector::PruneExpiredCounters(
    std::deque<WindowCounter>& counters) const {
  int64_t cutoff = NowMs() -
    static_cast<int64_t>(config_.throughput_window_sec) * 1000;
  while (!counters.empty() && counters.front().timestamp_ms < cutoff) {
    counters.pop_front();
  }
}

uint64_t MetricsCollector::CountInWindow(
    const std::deque<WindowCounter>& counters,
    int64_t window_ms) const {
  int64_t cutoff = NowMs() - window_ms;
  uint64_t total = 0;
  for (auto it = counters.rbegin(); it != counters.rend(); ++it) {
    if (it->timestamp_ms < cutoff) break;
    total += it->count;
  }
  return total;
}

}  // namespace control_plane
}  // namespace tendisplus
