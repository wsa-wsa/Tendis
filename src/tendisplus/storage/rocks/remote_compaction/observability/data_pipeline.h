// Copyright (c) 2024-present, Tencent Inc. All rights reserved.
// 数据采集与处理管道

#pragma once

#include <string>
#include <vector>
#include <memory>
#include <functional>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <atomic>
#include <chrono>
#include <unordered_map>

namespace tendisplus {
namespace observability {

// ============================================================================
// 数据点类型
// ============================================================================
enum class DataPointType {
  kMetric = 0,
  kLog = 1,
  kTrace = 2,
  kEvent = 3
};

// ============================================================================
// 通用数据点
// ============================================================================
struct DataPoint {
  DataPointType type;
  std::string name;
  std::chrono::system_clock::time_point timestamp;
  std::unordered_map<std::string, std::string> labels;
  double value;
  std::string string_value;
  
  DataPoint() : type(DataPointType::kMetric), value(0.0) {
    timestamp = std::chrono::system_clock::now();
  }
};

// ============================================================================
// 数据采集器接口
// ============================================================================
class DataCollector {
 public:
  virtual ~DataCollector() = default;
  virtual std::vector<DataPoint> Collect() = 0;
  virtual std::string GetName() const = 0;
  virtual std::chrono::milliseconds GetInterval() const = 0;
};

// ============================================================================
// 数据处理器接口
// ============================================================================
class DataProcessor {
 public:
  virtual ~DataProcessor() = default;
  virtual std::vector<DataPoint> Process(const std::vector<DataPoint>& input) = 0;
  virtual std::string GetName() const = 0;
};

// ============================================================================
// 数据输出器接口
// ============================================================================
class DataExporter {
 public:
  virtual ~DataExporter() = default;
  virtual bool Export(const std::vector<DataPoint>& data) = 0;
  virtual std::string GetName() const = 0;
};

// ============================================================================
// 聚合处理器 - 支持多种聚合操作
// ============================================================================
class AggregationProcessor : public DataProcessor {
 public:
  enum class AggregationType {
    kSum = 0,
    kAvg = 1,
    kMin = 2,
    kMax = 3,
    kCount = 4,
    kRate = 5,
    kPercentile = 6
  };
  
  struct AggregationConfig {
    std::string metric_name;
    AggregationType type;
    std::chrono::seconds window;
    std::vector<std::string> group_by_labels;
    double percentile_value;  // 用于分位数计算
  };
  
  AggregationProcessor() = default;
  ~AggregationProcessor() override = default;
  
  void AddAggregation(const AggregationConfig& config) {
    configs_.push_back(config);
  }
  
  std::vector<DataPoint> Process(const std::vector<DataPoint>& input) override {
    std::vector<DataPoint> output;
    
    for (const auto& config : configs_) {
      // 过滤匹配的数据点
      std::vector<DataPoint> matching;
      for (const auto& dp : input) {
        if (dp.name == config.metric_name) {
          matching.push_back(dp);
        }
      }
      
      if (matching.empty()) continue;
      
      // 按标签分组
      std::unordered_map<std::string, std::vector<DataPoint>> groups;
      for (const auto& dp : matching) {
        std::string group_key;
        for (const auto& label : config.group_by_labels) {
          auto it = dp.labels.find(label);
          if (it != dp.labels.end()) {
            group_key += it->second + ":";
          }
        }
        groups[group_key].push_back(dp);
      }
      
      // 对每个分组进行聚合
      for (const auto& group : groups) {
        DataPoint result;
        result.type = DataPointType::kMetric;
        result.name = config.metric_name + "_" + AggregationTypeToString(config.type);
        result.timestamp = std::chrono::system_clock::now();
        
        switch (config.type) {
          case AggregationType::kSum:
            result.value = CalculateSum(group.second);
            break;
          case AggregationType::kAvg:
            result.value = CalculateAvg(group.second);
            break;
          case AggregationType::kMin:
            result.value = CalculateMin(group.second);
            break;
          case AggregationType::kMax:
            result.value = CalculateMax(group.second);
            break;
          case AggregationType::kCount:
            result.value = static_cast<double>(group.second.size());
            break;
          case AggregationType::kPercentile:
            result.value = CalculatePercentile(group.second, config.percentile_value);
            break;
          default:
            break;
        }
        
        output.push_back(result);
      }
    }
    
    return output;
  }
  
  std::string GetName() const override { return "AggregationProcessor"; }
  
 private:
  std::vector<AggregationConfig> configs_;
  
  double CalculateSum(const std::vector<DataPoint>& data) {
    double sum = 0;
    for (const auto& dp : data) sum += dp.value;
    return sum;
  }
  
  double CalculateAvg(const std::vector<DataPoint>& data) {
    if (data.empty()) return 0;
    return CalculateSum(data) / data.size();
  }
  
  double CalculateMin(const std::vector<DataPoint>& data) {
    if (data.empty()) return 0;
    double min_val = data[0].value;
    for (const auto& dp : data) {
      if (dp.value < min_val) min_val = dp.value;
    }
    return min_val;
  }
  
  double CalculateMax(const std::vector<DataPoint>& data) {
    if (data.empty()) return 0;
    double max_val = data[0].value;
    for (const auto& dp : data) {
      if (dp.value > max_val) max_val = dp.value;
    }
    return max_val;
  }
  
  double CalculatePercentile(const std::vector<DataPoint>& data, double p) {
    if (data.empty()) return 0;
    std::vector<double> values;
    for (const auto& dp : data) values.push_back(dp.value);
    std::sort(values.begin(), values.end());
    size_t idx = static_cast<size_t>(p * values.size());
    if (idx >= values.size()) idx = values.size() - 1;
    return values[idx];
  }
  
  std::string AggregationTypeToString(AggregationType type) {
    switch (type) {
      case AggregationType::kSum: return "sum";
      case AggregationType::kAvg: return "avg";
      case AggregationType::kMin: return "min";
      case AggregationType::kMax: return "max";
      case AggregationType::kCount: return "count";
      case AggregationType::kRate: return "rate";
      case AggregationType::kPercentile: return "percentile";
      default: return "unknown";
    }
  }
};

// ============================================================================
// Prometheus 输出器
// ============================================================================
class PrometheusExporter : public DataExporter {
 public:
  PrometheusExporter(const std::string& endpoint) : endpoint_(endpoint) {}
  ~PrometheusExporter() override = default;
  
  bool Export(const std::vector<DataPoint>& data) override {
    // 转换为 Prometheus 格式并推送
    std::string payload;
    for (const auto& dp : data) {
      if (dp.type != DataPointType::kMetric) continue;
      
      payload += dp.name;
      if (!dp.labels.empty()) {
        payload += "{";
        bool first = true;
        for (const auto& label : dp.labels) {
          if (!first) payload += ",";
          payload += label.first + "=\"" + label.second + "\"";
          first = false;
        }
        payload += "}";
      }
      payload += " " + std::to_string(dp.value) + "\n";
    }
    
    // 实际实现中需要 HTTP 客户端推送到 Pushgateway
    return !payload.empty();
  }
  
  std::string GetName() const override { return "PrometheusExporter"; }
  
 private:
  std::string endpoint_;
};

// ============================================================================
// 数据管道
// ============================================================================
class DataPipeline {
 public:
  DataPipeline() : running_(false) {}
  ~DataPipeline() { Stop(); }
  
  // 添加采集器
  void AddCollector(std::shared_ptr<DataCollector> collector) {
    collectors_.push_back(collector);
  }
  
  // 添加处理器
  void AddProcessor(std::shared_ptr<DataProcessor> processor) {
    processors_.push_back(processor);
  }
  
  // 添加输出器
  void AddExporter(std::shared_ptr<DataExporter> exporter) {
    exporters_.push_back(exporter);
  }
  
  // 启动管道
  void Start() {
    if (running_.exchange(true)) return;
    
    // 为每个采集器启动采集线程
    for (auto& collector : collectors_) {
      collector_threads_.emplace_back([this, collector]() {
        while (running_) {
          auto data = collector->Collect();
          if (!data.empty()) {
            std::lock_guard<std::mutex> lock(queue_mutex_);
            for (auto& dp : data) {
              data_queue_.push(std::move(dp));
            }
            queue_cv_.notify_one();
          }
          std::this_thread::sleep_for(collector->GetInterval());
        }
      });
    }
    
    // 启动处理线程
    process_thread_ = std::thread([this]() {
      while (running_) {
        std::vector<DataPoint> batch;
        {
          std::unique_lock<std::mutex> lock(queue_mutex_);
          queue_cv_.wait_for(lock, std::chrono::seconds(1), [this]() {
            return !data_queue_.empty() || !running_;
          });
          
          while (!data_queue_.empty() && batch.size() < 1000) {
            batch.push_back(std::move(data_queue_.front()));
            data_queue_.pop();
          }
        }
        
        if (!batch.empty()) {
          ProcessAndExport(batch);
        }
      }
    });
  }
  
  // 停止管道
  void Stop() {
    if (!running_.exchange(false)) return;
    
    queue_cv_.notify_all();
    
    for (auto& t : collector_threads_) {
      if (t.joinable()) t.join();
    }
    collector_threads_.clear();
    
    if (process_thread_.joinable()) {
      process_thread_.join();
    }
  }
  
 private:
  void ProcessAndExport(std::vector<DataPoint>& data) {
    // 依次通过处理器
    for (auto& processor : processors_) {
      auto processed = processor->Process(data);
      data.insert(data.end(), processed.begin(), processed.end());
    }
    
    // 输出到所有输出器
    for (auto& exporter : exporters_) {
      exporter->Export(data);
    }
  }
  
  std::atomic<bool> running_;
  std::vector<std::shared_ptr<DataCollector>> collectors_;
  std::vector<std::shared_ptr<DataProcessor>> processors_;
  std::vector<std::shared_ptr<DataExporter>> exporters_;
  
  std::queue<DataPoint> data_queue_;
  std::mutex queue_mutex_;
  std::condition_variable queue_cv_;
  
  std::vector<std::thread> collector_threads_;
  std::thread process_thread_;
};

}  // namespace observability
}  // namespace tendisplus
