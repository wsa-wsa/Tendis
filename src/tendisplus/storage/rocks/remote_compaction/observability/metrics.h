// Copyright (c) 2024-present, Tencent Inc. All rights reserved.
// 监控指标定义

#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace tendisplus {
namespace observability {

// ============================================================================
// 指标类型
// ============================================================================
enum class MetricType {
  kCounter = 0,      // 计数器 (只增不减)
  kGauge = 1,        // 仪表盘 (可增可减)
  kHistogram = 2,    // 直方图 (分布统计)
  kSummary = 3       // 摘要 (分位数统计)
};

inline const char* MetricTypeToString(MetricType type) {
  switch (type) {
    case MetricType::kCounter: return "counter";
    case MetricType::kGauge: return "gauge";
    case MetricType::kHistogram: return "histogram";
    case MetricType::kSummary: return "summary";
    default: return "unknown";
  }
}

// ============================================================================
// 标签
// ============================================================================
using Labels = std::map<std::string, std::string>;

inline std::string LabelsToString(const Labels& labels) {
  if (labels.empty()) return "";
  std::string result = "{";
  bool first = true;
  for (const auto& [key, value] : labels) {
    if (!first) result += ",";
    result += key + "=\"" + value + "\"";
    first = false;
  }
  result += "}";
  return result;
}

// ============================================================================
// 指标基类
// ============================================================================
class Metric {
 public:
  Metric(const std::string& name, const std::string& help, MetricType type)
      : name_(name), help_(help), type_(type) {}
  virtual ~Metric() = default;
  
  const std::string& Name() const { return name_; }
  const std::string& Help() const { return help_; }
  MetricType Type() const { return type_; }
  
  // 导出为 Prometheus 格式
  virtual std::string ExportPrometheus() const = 0;
  
  // 重置指标
  virtual void Reset() = 0;

 protected:
  std::string name_;
  std::string help_;
  MetricType type_;
};

// ============================================================================
// 计数器
// ============================================================================
class Counter : public Metric {
 public:
  Counter(const std::string& name, const std::string& help)
      : Metric(name, help, MetricType::kCounter), value_(0) {}
  
  void Inc() { value_.fetch_add(1, std::memory_order_relaxed); }
  void Add(double delta) { 
    if (delta < 0) return;  // Counter 只能增加
    value_.fetch_add(static_cast<uint64_t>(delta), std::memory_order_relaxed); 
  }
  
  double Value() const { return static_cast<double>(value_.load()); }
  
  std::string ExportPrometheus() const override;
  void Reset() override { value_.store(0); }

 private:
  std::atomic<uint64_t> value_;
};

// ============================================================================
// 带标签的计数器
// ============================================================================
class CounterVec : public Metric {
 public:
  CounterVec(const std::string& name, const std::string& help,
             const std::vector<std::string>& label_names)
      : Metric(name, help, MetricType::kCounter), label_names_(label_names) {}
  
  void Inc(const Labels& labels) { Add(labels, 1); }
  void Add(const Labels& labels, double delta);
  
  double Value(const Labels& labels) const;
  
  std::string ExportPrometheus() const override;
  void Reset() override;

 private:
  std::string MakeKey(const Labels& labels) const;
  
  std::vector<std::string> label_names_;
  std::map<std::string, std::atomic<uint64_t>> values_;
  std::map<std::string, Labels> label_cache_;
  mutable std::mutex mutex_;
};

// ============================================================================
// 仪表盘
// ============================================================================
class Gauge : public Metric {
 public:
  Gauge(const std::string& name, const std::string& help)
      : Metric(name, help, MetricType::kGauge), value_(0) {}
  
  void Set(double value) { value_.store(value, std::memory_order_relaxed); }
  void Inc() { value_.fetch_add(1, std::memory_order_relaxed); }
  void Dec() { value_.fetch_sub(1, std::memory_order_relaxed); }
  void Add(double delta) { value_.fetch_add(delta, std::memory_order_relaxed); }
  void Sub(double delta) { value_.fetch_sub(delta, std::memory_order_relaxed); }
  
  double Value() const { return value_.load(); }
  
  std::string ExportPrometheus() const override;
  void Reset() override { value_.store(0); }

 private:
  std::atomic<double> value_;
};

// ============================================================================
// 带标签的仪表盘
// ============================================================================
class GaugeVec : public Metric {
 public:
  GaugeVec(const std::string& name, const std::string& help,
           const std::vector<std::string>& label_names)
      : Metric(name, help, MetricType::kGauge), label_names_(label_names) {}
  
  void Set(const Labels& labels, double value);
  void Inc(const Labels& labels);
  void Dec(const Labels& labels);
  void Add(const Labels& labels, double delta);
  void Sub(const Labels& labels, double delta);
  
  double Value(const Labels& labels) const;
  
  std::string ExportPrometheus() const override;
  void Reset() override;

 private:
  std::string MakeKey(const Labels& labels) const;
  
  std::vector<std::string> label_names_;
  std::map<std::string, std::atomic<double>> values_;
  std::map<std::string, Labels> label_cache_;
  mutable std::mutex mutex_;
};

// ============================================================================
// 直方图
// ============================================================================
class Histogram : public Metric {
 public:
  Histogram(const std::string& name, const std::string& help,
            const std::vector<double>& buckets = DefaultBuckets())
      : Metric(name, help, MetricType::kHistogram), 
        buckets_(buckets), 
        bucket_counts_(buckets.size() + 1, 0),
        sum_(0), count_(0) {}
  
  void Observe(double value);
  
  double Sum() const { return sum_.load(); }
  uint64_t Count() const { return count_.load(); }
  const std::vector<double>& Buckets() const { return buckets_; }
  std::vector<uint64_t> BucketCounts() const;
  
  std::string ExportPrometheus() const override;
  void Reset() override;
  
  static std::vector<double> DefaultBuckets() {
    return {0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0};
  }
  
  static std::vector<double> LinearBuckets(double start, double width, int count);
  static std::vector<double> ExponentialBuckets(double start, double factor, int count);

 private:
  std::vector<double> buckets_;
  std::vector<std::atomic<uint64_t>> bucket_counts_;
  std::atomic<double> sum_;
  std::atomic<uint64_t> count_;
  mutable std::mutex mutex_;
};

// ============================================================================
// 带标签的直方图
// ============================================================================
class HistogramVec : public Metric {
 public:
  HistogramVec(const std::string& name, const std::string& help,
               const std::vector<std::string>& label_names,
               const std::vector<double>& buckets = Histogram::DefaultBuckets())
      : Metric(name, help, MetricType::kHistogram),
        label_names_(label_names), buckets_(buckets) {}
  
  void Observe(const Labels& labels, double value);
  
  std::string ExportPrometheus() const override;
  void Reset() override;

 private:
  std::shared_ptr<Histogram> GetOrCreate(const Labels& labels);
  std::string MakeKey(const Labels& labels) const;
  
  std::vector<std::string> label_names_;
  std::vector<double> buckets_;
  std::map<std::string, std::shared_ptr<Histogram>> histograms_;
  std::map<std::string, Labels> label_cache_;
  mutable std::mutex mutex_;
};

// ============================================================================
// 计时器 (基于直方图)
// ============================================================================
class Timer {
 public:
  explicit Timer(Histogram& histogram) 
      : histogram_(histogram), 
        start_(std::chrono::high_resolution_clock::now()) {}
  
  ~Timer() {
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration<double>(end - start_).count();
    histogram_.Observe(duration);
  }
  
  // 禁止拷贝
  Timer(const Timer&) = delete;
  Timer& operator=(const Timer&) = delete;

 private:
  Histogram& histogram_;
  std::chrono::high_resolution_clock::time_point start_;
};

// ============================================================================
// 指标注册表
// ============================================================================
class MetricsRegistry {
 public:
  MetricsRegistry() = default;
  ~MetricsRegistry() = default;
  
  // 单例访问
  static MetricsRegistry& Instance();
  
  // 注册指标
  template<typename T>
  std::shared_ptr<T> Register(std::shared_ptr<T> metric) {
    std::lock_guard<std::mutex> lock(mutex_);
    metrics_[metric->Name()] = metric;
    return metric;
  }
  
  // 获取指标
  std::shared_ptr<Metric> Get(const std::string& name) const;
  
  // 获取所有指标
  std::vector<std::shared_ptr<Metric>> GetAll() const;
  
  // 导出所有指标 (Prometheus 格式)
  std::string ExportPrometheus() const;
  
  // 重置所有指标
  void ResetAll();
  
  // 移除指标
  bool Remove(const std::string& name);
  
  // 清空所有指标
  void Clear();

 private:
  std::map<std::string, std::shared_ptr<Metric>> metrics_;
  mutable std::mutex mutex_;
};

// ============================================================================
// 便捷宏定义
// ============================================================================
#define DEFINE_COUNTER(name, help) \
  static auto name = MetricsRegistry::Instance().Register( \
      std::make_shared<Counter>(#name, help))

#define DEFINE_GAUGE(name, help) \
  static auto name = MetricsRegistry::Instance().Register( \
      std::make_shared<Gauge>(#name, help))

#define DEFINE_HISTOGRAM(name, help, buckets) \
  static auto name = MetricsRegistry::Instance().Register( \
      std::make_shared<Histogram>(#name, help, buckets))

}  // namespace observability
}  // namespace tendisplus
