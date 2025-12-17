// Copyright (c) 2024-present, Tencent Inc. All rights reserved.
// 监控指标实现

#include "metrics.h"

#include <iomanip>
#include <sstream>

namespace tendisplus {
namespace observability {

// ============================================================================
// Counter 实现
// ============================================================================

std::string Counter::ExportPrometheus() const {
  std::ostringstream oss;
  oss << "# HELP " << name_ << " " << help_ << "\n";
  oss << "# TYPE " << name_ << " counter\n";
  oss << name_ << " " << std::fixed << std::setprecision(6) << Value() << "\n";
  return oss.str();
}

// ============================================================================
// CounterVec 实现
// ============================================================================

void CounterVec::Add(const Labels& labels, double delta) {
  if (delta < 0) return;
  
  std::lock_guard<std::mutex> lock(mutex_);
  std::string key = MakeKey(labels);
  
  auto it = values_.find(key);
  if (it == values_.end()) {
    values_[key].store(static_cast<uint64_t>(delta));
    label_cache_[key] = labels;
  } else {
    it->second.fetch_add(static_cast<uint64_t>(delta), std::memory_order_relaxed);
  }
}

double CounterVec::Value(const Labels& labels) const {
  std::lock_guard<std::mutex> lock(mutex_);
  std::string key = MakeKey(labels);
  
  auto it = values_.find(key);
  if (it != values_.end()) {
    return static_cast<double>(it->second.load());
  }
  return 0;
}

std::string CounterVec::ExportPrometheus() const {
  std::lock_guard<std::mutex> lock(mutex_);
  
  std::ostringstream oss;
  oss << "# HELP " << name_ << " " << help_ << "\n";
  oss << "# TYPE " << name_ << " counter\n";
  
  for (const auto& [key, value] : values_) {
    auto label_it = label_cache_.find(key);
    if (label_it != label_cache_.end()) {
      oss << name_ << LabelsToString(label_it->second) << " "
          << std::fixed << std::setprecision(6) 
          << static_cast<double>(value.load()) << "\n";
    }
  }
  
  return oss.str();
}

void CounterVec::Reset() {
  std::lock_guard<std::mutex> lock(mutex_);
  for (auto& [key, value] : values_) {
    value.store(0);
  }
}

std::string CounterVec::MakeKey(const Labels& labels) const {
  std::ostringstream oss;
  for (const auto& name : label_names_) {
    auto it = labels.find(name);
    if (it != labels.end()) {
      oss << it->second << "|";
    } else {
      oss << "|";
    }
  }
  return oss.str();
}

// ============================================================================
// Gauge 实现
// ============================================================================

std::string Gauge::ExportPrometheus() const {
  std::ostringstream oss;
  oss << "# HELP " << name_ << " " << help_ << "\n";
  oss << "# TYPE " << name_ << " gauge\n";
  oss << name_ << " " << std::fixed << std::setprecision(6) << Value() << "\n";
  return oss.str();
}

// ============================================================================
// GaugeVec 实现
// ============================================================================

void GaugeVec::Set(const Labels& labels, double value) {
  std::lock_guard<std::mutex> lock(mutex_);
  std::string key = MakeKey(labels);
  values_[key].store(value);
  label_cache_[key] = labels;
}

void GaugeVec::Inc(const Labels& labels) {
  Add(labels, 1);
}

void GaugeVec::Dec(const Labels& labels) {
  Sub(labels, 1);
}

void GaugeVec::Add(const Labels& labels, double delta) {
  std::lock_guard<std::mutex> lock(mutex_);
  std::string key = MakeKey(labels);
  
  auto it = values_.find(key);
  if (it == values_.end()) {
    values_[key].store(delta);
    label_cache_[key] = labels;
  } else {
    it->second.fetch_add(delta, std::memory_order_relaxed);
  }
}

void GaugeVec::Sub(const Labels& labels, double delta) {
  Add(labels, -delta);
}

double GaugeVec::Value(const Labels& labels) const {
  std::lock_guard<std::mutex> lock(mutex_);
  std::string key = MakeKey(labels);
  
  auto it = values_.find(key);
  if (it != values_.end()) {
    return it->second.load();
  }
  return 0;
}

std::string GaugeVec::ExportPrometheus() const {
  std::lock_guard<std::mutex> lock(mutex_);
  
  std::ostringstream oss;
  oss << "# HELP " << name_ << " " << help_ << "\n";
  oss << "# TYPE " << name_ << " gauge\n";
  
  for (const auto& [key, value] : values_) {
    auto label_it = label_cache_.find(key);
    if (label_it != label_cache_.end()) {
      oss << name_ << LabelsToString(label_it->second) << " "
          << std::fixed << std::setprecision(6) << value.load() << "\n";
    }
  }
  
  return oss.str();
}

void GaugeVec::Reset() {
  std::lock_guard<std::mutex> lock(mutex_);
  for (auto& [key, value] : values_) {
    value.store(0);
  }
}

std::string GaugeVec::MakeKey(const Labels& labels) const {
  std::ostringstream oss;
  for (const auto& name : label_names_) {
    auto it = labels.find(name);
    if (it != labels.end()) {
      oss << it->second << "|";
    } else {
      oss << "|";
    }
  }
  return oss.str();
}

// ============================================================================
// Histogram 实现
// ============================================================================

void Histogram::Observe(double value) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  // 更新桶计数
  for (size_t i = 0; i < buckets_.size(); ++i) {
    if (value <= buckets_[i]) {
      bucket_counts_[i].fetch_add(1, std::memory_order_relaxed);
    }
  }
  // +Inf 桶
  bucket_counts_.back().fetch_add(1, std::memory_order_relaxed);
  
  // 更新总和和计数
  sum_.fetch_add(value, std::memory_order_relaxed);
  count_.fetch_add(1, std::memory_order_relaxed);
}

std::vector<uint64_t> Histogram::BucketCounts() const {
  std::lock_guard<std::mutex> lock(mutex_);
  
  std::vector<uint64_t> counts;
  counts.reserve(bucket_counts_.size());
  for (const auto& bc : bucket_counts_) {
    counts.push_back(bc.load());
  }
  return counts;
}

std::string Histogram::ExportPrometheus() const {
  std::lock_guard<std::mutex> lock(mutex_);
  
  std::ostringstream oss;
  oss << "# HELP " << name_ << " " << help_ << "\n";
  oss << "# TYPE " << name_ << " histogram\n";
  
  uint64_t cumulative = 0;
  for (size_t i = 0; i < buckets_.size(); ++i) {
    cumulative += bucket_counts_[i].load();
    oss << name_ << "_bucket{le=\"" << std::fixed << std::setprecision(6) 
        << buckets_[i] << "\"} " << cumulative << "\n";
  }
  cumulative += bucket_counts_.back().load();
  oss << name_ << "_bucket{le=\"+Inf\"} " << cumulative << "\n";
  
  oss << name_ << "_sum " << std::fixed << std::setprecision(6) 
      << sum_.load() << "\n";
  oss << name_ << "_count " << count_.load() << "\n";
  
  return oss.str();
}

void Histogram::Reset() {
  std::lock_guard<std::mutex> lock(mutex_);
  
  for (auto& bc : bucket_counts_) {
    bc.store(0);
  }
  sum_.store(0);
  count_.store(0);
}

std::vector<double> Histogram::LinearBuckets(double start, double width, int count) {
  std::vector<double> buckets;
  buckets.reserve(count);
  for (int i = 0; i < count; ++i) {
    buckets.push_back(start + i * width);
  }
  return buckets;
}

std::vector<double> Histogram::ExponentialBuckets(double start, double factor, int count) {
  std::vector<double> buckets;
  buckets.reserve(count);
  double value = start;
  for (int i = 0; i < count; ++i) {
    buckets.push_back(value);
    value *= factor;
  }
  return buckets;
}

// ============================================================================
// HistogramVec 实现
// ============================================================================

void HistogramVec::Observe(const Labels& labels, double value) {
  auto histogram = GetOrCreate(labels);
  histogram->Observe(value);
}

std::shared_ptr<Histogram> HistogramVec::GetOrCreate(const Labels& labels) {
  std::lock_guard<std::mutex> lock(mutex_);
  std::string key = MakeKey(labels);
  
  auto it = histograms_.find(key);
  if (it != histograms_.end()) {
    return it->second;
  }
  
  auto histogram = std::make_shared<Histogram>(name_, help_, buckets_);
  histograms_[key] = histogram;
  label_cache_[key] = labels;
  return histogram;
}

std::string HistogramVec::ExportPrometheus() const {
  std::lock_guard<std::mutex> lock(mutex_);
  
  std::ostringstream oss;
  oss << "# HELP " << name_ << " " << help_ << "\n";
  oss << "# TYPE " << name_ << " histogram\n";
  
  for (const auto& [key, histogram] : histograms_) {
    auto label_it = label_cache_.find(key);
    if (label_it == label_cache_.end()) continue;
    
    const Labels& labels = label_it->second;
    std::string label_str = LabelsToString(labels);
    
    auto counts = histogram->BucketCounts();
    uint64_t cumulative = 0;
    
    for (size_t i = 0; i < buckets_.size(); ++i) {
      cumulative += counts[i];
      // 构建带 le 标签的标签字符串
      Labels bucket_labels = labels;
      std::ostringstream le_oss;
      le_oss << std::fixed << std::setprecision(6) << buckets_[i];
      bucket_labels["le"] = le_oss.str();
      oss << name_ << "_bucket" << LabelsToString(bucket_labels) 
          << " " << cumulative << "\n";
    }
    
    cumulative += counts.back();
    Labels inf_labels = labels;
    inf_labels["le"] = "+Inf";
    oss << name_ << "_bucket" << LabelsToString(inf_labels) 
        << " " << cumulative << "\n";
    
    oss << name_ << "_sum" << label_str << " " 
        << std::fixed << std::setprecision(6) << histogram->Sum() << "\n";
    oss << name_ << "_count" << label_str << " " << histogram->Count() << "\n";
  }
  
  return oss.str();
}

void HistogramVec::Reset() {
  std::lock_guard<std::mutex> lock(mutex_);
  for (auto& [key, histogram] : histograms_) {
    histogram->Reset();
  }
}

std::string HistogramVec::MakeKey(const Labels& labels) const {
  std::ostringstream oss;
  for (const auto& name : label_names_) {
    auto it = labels.find(name);
    if (it != labels.end()) {
      oss << it->second << "|";
    } else {
      oss << "|";
    }
  }
  return oss.str();
}

// ============================================================================
// MetricsRegistry 实现
// ============================================================================

MetricsRegistry& MetricsRegistry::Instance() {
  static MetricsRegistry instance;
  return instance;
}

std::shared_ptr<Metric> MetricsRegistry::Get(const std::string& name) const {
  std::lock_guard<std::mutex> lock(mutex_);
  
  auto it = metrics_.find(name);
  if (it != metrics_.end()) {
    return it->second;
  }
  return nullptr;
}

std::vector<std::shared_ptr<Metric>> MetricsRegistry::GetAll() const {
  std::lock_guard<std::mutex> lock(mutex_);
  
  std::vector<std::shared_ptr<Metric>> result;
  result.reserve(metrics_.size());
  for (const auto& [name, metric] : metrics_) {
    result.push_back(metric);
  }
  return result;
}

std::string MetricsRegistry::ExportPrometheus() const {
  std::lock_guard<std::mutex> lock(mutex_);
  
  std::ostringstream oss;
  for (const auto& [name, metric] : metrics_) {
    oss << metric->ExportPrometheus();
  }
  return oss.str();
}

void MetricsRegistry::ResetAll() {
  std::lock_guard<std::mutex> lock(mutex_);
  for (auto& [name, metric] : metrics_) {
    metric->Reset();
  }
}

bool MetricsRegistry::Remove(const std::string& name) {
  std::lock_guard<std::mutex> lock(mutex_);
  return metrics_.erase(name) > 0;
}

void MetricsRegistry::Clear() {
  std::lock_guard<std::mutex> lock(mutex_);
  metrics_.clear();
}

}  // namespace observability
}  // namespace tendisplus
