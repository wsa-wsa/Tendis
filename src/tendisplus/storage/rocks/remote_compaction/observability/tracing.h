// Copyright (c) 2024-present, Tencent Inc. All rights reserved.
// 分布式链路追踪

#pragma once

#include <chrono>
#include <map>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <vector>

namespace tendisplus {
namespace observability {

// ============================================================================
// Span 状态
// ============================================================================
enum class SpanStatus {
  kUnset = 0,
  kOk = 1,
  kError = 2
};

inline const char* SpanStatusToString(SpanStatus status) {
  switch (status) {
    case SpanStatus::kOk: return "OK";
    case SpanStatus::kError: return "ERROR";
    default: return "UNSET";
  }
}

// ============================================================================
// Span 事件
// ============================================================================
struct SpanEvent {
  std::string name;
  std::chrono::system_clock::time_point timestamp;
  std::map<std::string, std::string> attributes;
};

// ============================================================================
// Span 上下文
// ============================================================================
struct SpanContext {
  std::string trace_id;      // 16 字节 hex 编码
  std::string span_id;       // 8 字节 hex 编码
  std::string parent_span_id;
  bool sampled = true;
  
  bool IsValid() const {
    return !trace_id.empty() && !span_id.empty();
  }
  
  std::string ToTraceParent() const {
    // W3C Trace Context 格式: 00-{trace_id}-{span_id}-{flags}
    return "00-" + trace_id + "-" + span_id + (sampled ? "-01" : "-00");
  }
  
  static SpanContext FromTraceParent(const std::string& trace_parent);
};

// ============================================================================
// Span
// ============================================================================
class Span {
 public:
  Span(const std::string& name, const SpanContext& context);
  ~Span();
  
  // 禁止拷贝
  Span(const Span&) = delete;
  Span& operator=(const Span&) = delete;
  
  // 设置属性
  void SetAttribute(const std::string& key, const std::string& value);
  void SetAttribute(const std::string& key, int64_t value);
  void SetAttribute(const std::string& key, double value);
  void SetAttribute(const std::string& key, bool value);
  
  // 添加事件
  void AddEvent(const std::string& name,
                const std::map<std::string, std::string>& attributes = {});
  
  // 设置状态
  void SetStatus(SpanStatus status, const std::string& description = "");
  
  // 结束 Span
  void End();
  
  // 获取上下文
  const SpanContext& Context() const { return context_; }
  
  // 获取名称
  const std::string& Name() const { return name_; }
  
  // 是否已结束
  bool IsEnded() const { return ended_; }
  
  // 获取持续时间 (微秒)
  int64_t DurationMicros() const;
  
  // 导出为 JSON
  std::string ToJson() const;

 private:
  std::string name_;
  SpanContext context_;
  std::chrono::system_clock::time_point start_time_;
  std::chrono::system_clock::time_point end_time_;
  std::map<std::string, std::string> attributes_;
  std::vector<SpanEvent> events_;
  SpanStatus status_ = SpanStatus::kUnset;
  std::string status_description_;
  bool ended_ = false;
  mutable std::mutex mutex_;
};

// ============================================================================
// Tracer
// ============================================================================
class Tracer {
 public:
  Tracer(const std::string& service_name);
  ~Tracer() = default;
  
  // 单例访问
  static Tracer& Instance();
  static void Initialize(const std::string& service_name);
  
  // 创建新的 Span
  std::shared_ptr<Span> StartSpan(const std::string& name);
  
  // 创建子 Span
  std::shared_ptr<Span> StartSpan(const std::string& name,
                                  const SpanContext& parent_context);
  
  // 从 trace parent 创建 Span
  std::shared_ptr<Span> StartSpanFromTraceParent(const std::string& name,
                                                  const std::string& trace_parent);
  
  // 获取当前活跃的 Span
  std::shared_ptr<Span> GetCurrentSpan() const;
  
  // 设置当前 Span
  void SetCurrentSpan(std::shared_ptr<Span> span);
  
  // 获取服务名称
  const std::string& ServiceName() const { return service_name_; }

 private:
  std::string GenerateTraceId();
  std::string GenerateSpanId();
  
  std::string service_name_;
  std::mt19937_64 rng_;
  mutable std::mutex mutex_;
  
  // 线程本地存储当前 Span
  static thread_local std::shared_ptr<Span> current_span_;
  
  static std::unique_ptr<Tracer> instance_;
  static std::mutex instance_mutex_;
};

// ============================================================================
// Span 作用域守卫
// ============================================================================
class SpanScope {
 public:
  explicit SpanScope(std::shared_ptr<Span> span);
  ~SpanScope();
  
  // 禁止拷贝
  SpanScope(const SpanScope&) = delete;
  SpanScope& operator=(const SpanScope&) = delete;
  
  Span& GetSpan() { return *span_; }

 private:
  std::shared_ptr<Span> span_;
  std::shared_ptr<Span> previous_span_;
};

// ============================================================================
// 便捷宏
// ============================================================================
#define TRACE_SPAN(name) \
  auto _span = Tracer::Instance().StartSpan(name); \
  SpanScope _scope(_span)

#define TRACE_SPAN_WITH_PARENT(name, parent) \
  auto _span = Tracer::Instance().StartSpan(name, parent); \
  SpanScope _scope(_span)

// ============================================================================
// Span 导出器接口
// ============================================================================
class SpanExporter {
 public:
  virtual ~SpanExporter() = default;
  
  // 导出 Span
  virtual void Export(const std::vector<std::shared_ptr<Span>>& spans) = 0;
  
  // 关闭导出器
  virtual void Shutdown() = 0;
};

// ============================================================================
// 控制台导出器 (用于调试)
// ============================================================================
class ConsoleSpanExporter : public SpanExporter {
 public:
  void Export(const std::vector<std::shared_ptr<Span>>& spans) override;
  void Shutdown() override {}
};

// ============================================================================
// OTLP 导出器 (OpenTelemetry Protocol)
// ============================================================================
class OtlpSpanExporter : public SpanExporter {
 public:
  explicit OtlpSpanExporter(const std::string& endpoint);
  
  void Export(const std::vector<std::shared_ptr<Span>>& spans) override;
  void Shutdown() override;

 private:
  std::string endpoint_;
  bool shutdown_ = false;
};

// ============================================================================
// 任务追踪上下文
// ============================================================================
struct TaskTraceContext {
  std::string trace_id;
  std::string parent_span_id;
  std::map<std::string, std::string> baggage;
  
  // 序列化/反序列化
  std::string Serialize() const;
  static TaskTraceContext Deserialize(const std::string& data);
};

}  // namespace observability
}  // namespace tendisplus
