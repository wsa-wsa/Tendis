// Copyright (c) 2024-present, Tencent Inc. All rights reserved.
// 分布式链路追踪实现

#include "tracing.h"

#include <iomanip>
#include <sstream>
#include <random>
#include <chrono>

namespace tendisplus {
namespace observability {

// ============================================================================
// Span 实现
// ============================================================================

Span::Span(const std::string& trace_id,
           const std::string& span_id,
           const std::string& parent_span_id,
           const std::string& operation_name)
    : trace_id_(trace_id),
      span_id_(span_id),
      parent_span_id_(parent_span_id),
      operation_name_(operation_name),
      start_time_(std::chrono::steady_clock::now()),
      finished_(false) {
}

void Span::SetTag(const std::string& key, const std::string& value) {
  std::lock_guard<std::mutex> lock(mutex_);
  tags_[key] = value;
}

void Span::SetTag(const std::string& key, int64_t value) {
  SetTag(key, std::to_string(value));
}

void Span::SetTag(const std::string& key, double value) {
  std::ostringstream oss;
  oss << std::fixed << std::setprecision(6) << value;
  SetTag(key, oss.str());
}

void Span::SetTag(const std::string& key, bool value) {
  SetTag(key, value ? "true" : "false");
}

void Span::Log(const std::string& message) {
  Log({{"message", message}});
}

void Span::Log(const std::map<std::string, std::string>& fields) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  LogEntry entry;
  entry.timestamp = std::chrono::steady_clock::now();
  entry.fields = fields;
  logs_.push_back(std::move(entry));
}

void Span::SetBaggageItem(const std::string& key, const std::string& value) {
  std::lock_guard<std::mutex> lock(mutex_);
  baggage_[key] = value;
}

std::string Span::GetBaggageItem(const std::string& key) const {
  std::lock_guard<std::mutex> lock(mutex_);
  
  auto it = baggage_.find(key);
  if (it != baggage_.end()) {
    return it->second;
  }
  return "";
}

void Span::Finish() {
  std::lock_guard<std::mutex> lock(mutex_);
  
  if (!finished_) {
    end_time_ = std::chrono::steady_clock::now();
    finished_ = true;
    
    // 上报到 Tracer
    Tracer::Instance().ReportSpan(shared_from_this());
  }
}

void Span::Finish(SpanStatus status) {
  SetTag("status", SpanStatusToString(status));
  if (status == SpanStatus::kError) {
    SetTag("error", "true");
  }
  Finish();
}

void Span::FinishWithError(const std::string& error_message) {
  SetTag("error", "true");
  SetTag("error.message", error_message);
  Finish(SpanStatus::kError);
}

uint64_t Span::DurationMicros() const {
  std::lock_guard<std::mutex> lock(mutex_);
  
  auto end = finished_ ? end_time_ : std::chrono::steady_clock::now();
  return std::chrono::duration_cast<std::chrono::microseconds>(
      end - start_time_).count();
}

std::string Span::ToJson() const {
  std::lock_guard<std::mutex> lock(mutex_);
  
  std::ostringstream oss;
  oss << "{";
  oss << "\"trace_id\":\"" << trace_id_ << "\",";
  oss << "\"span_id\":\"" << span_id_ << "\",";
  oss << "\"parent_span_id\":\"" << parent_span_id_ << "\",";
  oss << "\"operation_name\":\"" << operation_name_ << "\",";
  oss << "\"start_time\":" << std::chrono::duration_cast<std::chrono::microseconds>(
      start_time_.time_since_epoch()).count() << ",";
  oss << "\"duration_micros\":" << DurationMicros() << ",";
  
  // Tags
  oss << "\"tags\":{";
  bool first = true;
  for (const auto& [key, value] : tags_) {
    if (!first) oss << ",";
    oss << "\"" << key << "\":\"" << value << "\"";
    first = false;
  }
  oss << "},";
  
  // Logs
  oss << "\"logs\":[";
  first = true;
  for (const auto& log : logs_) {
    if (!first) oss << ",";
    oss << "{\"timestamp\":" 
        << std::chrono::duration_cast<std::chrono::microseconds>(
            log.timestamp.time_since_epoch()).count()
        << ",\"fields\":{";
    bool field_first = true;
    for (const auto& [key, value] : log.fields) {
      if (!field_first) oss << ",";
      oss << "\"" << key << "\":\"" << value << "\"";
      field_first = false;
    }
    oss << "}}";
    first = false;
  }
  oss << "]";
  
  oss << "}";
  return oss.str();
}

std::string Span::SpanStatusToString(SpanStatus status) {
  switch (status) {
    case SpanStatus::kOk: return "OK";
    case SpanStatus::kError: return "ERROR";
    case SpanStatus::kCancelled: return "CANCELLED";
    default: return "UNKNOWN";
  }
}

// ============================================================================
// Tracer 实现
// ============================================================================

Tracer& Tracer::Instance() {
  static Tracer instance;
  return instance;
}

Tracer::Tracer() : running_(false), sampling_rate_(1.0) {
}

Tracer::~Tracer() {
  Stop();
}

void Tracer::Start() {
  std::lock_guard<std::mutex> lock(mutex_);
  
  if (running_) return;
  
  running_ = true;
  export_thread_ = std::thread(&Tracer::ExportLoop, this);
}

void Tracer::Stop() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!running_) return;
    running_ = false;
  }
  
  cv_.notify_all();
  
  if (export_thread_.joinable()) {
    export_thread_.join();
  }
}

std::shared_ptr<Span> Tracer::StartSpan(const std::string& operation_name) {
  return StartSpan(operation_name, nullptr);
}

std::shared_ptr<Span> Tracer::StartSpan(const std::string& operation_name,
                                         std::shared_ptr<Span> parent) {
  // 采样检查
  if (!ShouldSample()) {
    return nullptr;
  }
  
  std::string trace_id = parent ? parent->TraceId() : GenerateId();
  std::string span_id = GenerateId();
  std::string parent_span_id = parent ? parent->SpanId() : "";
  
  auto span = std::make_shared<Span>(trace_id, span_id, parent_span_id, operation_name);
  
  // 继承 baggage
  if (parent) {
    // 这里简化处理，实际需要复制 parent 的 baggage
  }
  
  return span;
}

std::shared_ptr<Span> Tracer::StartSpan(const std::string& operation_name,
                                         const SpanContext& context) {
  if (!ShouldSample()) {
    return nullptr;
  }
  
  std::string span_id = GenerateId();
  
  auto span = std::make_shared<Span>(
      context.trace_id, span_id, context.span_id, operation_name);
  
  // 恢复 baggage
  for (const auto& [key, value] : context.baggage) {
    span->SetBaggageItem(key, value);
  }
  
  return span;
}

SpanContext Tracer::Extract(const std::map<std::string, std::string>& carrier) {
  SpanContext context;
  
  auto trace_it = carrier.find("x-trace-id");
  if (trace_it != carrier.end()) {
    context.trace_id = trace_it->second;
  }
  
  auto span_it = carrier.find("x-span-id");
  if (span_it != carrier.end()) {
    context.span_id = span_it->second;
  }
  
  // 提取 baggage
  for (const auto& [key, value] : carrier) {
    if (key.find("x-baggage-") == 0) {
      std::string baggage_key = key.substr(10);  // 去掉 "x-baggage-" 前缀
      context.baggage[baggage_key] = value;
    }
  }
  
  return context;
}

void Tracer::Inject(const SpanContext& context,
                    std::map<std::string, std::string>& carrier) {
  carrier["x-trace-id"] = context.trace_id;
  carrier["x-span-id"] = context.span_id;
  
  for (const auto& [key, value] : context.baggage) {
    carrier["x-baggage-" + key] = value;
  }
}

void Tracer::ReportSpan(std::shared_ptr<Span> span) {
  if (!span) return;
  
  std::lock_guard<std::mutex> lock(mutex_);
  pending_spans_.push_back(span);
  cv_.notify_one();
}

void Tracer::SetSamplingRate(double rate) {
  sampling_rate_.store(std::max(0.0, std::min(1.0, rate)));
}

void Tracer::AddExporter(std::shared_ptr<SpanExporter> exporter) {
  std::lock_guard<std::mutex> lock(mutex_);
  exporters_.push_back(exporter);
}

void Tracer::ExportLoop() {
  while (running_) {
    std::vector<std::shared_ptr<Span>> spans_to_export;
    
    {
      std::unique_lock<std::mutex> lock(mutex_);
      cv_.wait_for(lock, std::chrono::seconds(1), [this] {
        return !running_ || !pending_spans_.empty();
      });
      
      if (!pending_spans_.empty()) {
        spans_to_export.swap(pending_spans_);
      }
    }
    
    if (!spans_to_export.empty()) {
      for (auto& exporter : exporters_) {
        exporter->Export(spans_to_export);
      }
    }
  }
  
  // 导出剩余的 spans
  std::lock_guard<std::mutex> lock(mutex_);
  if (!pending_spans_.empty()) {
    for (auto& exporter : exporters_) {
      exporter->Export(pending_spans_);
    }
    pending_spans_.clear();
  }
}

std::string Tracer::GenerateId() {
  static thread_local std::random_device rd;
  static thread_local std::mt19937_64 gen(rd());
  static thread_local std::uniform_int_distribution<uint64_t> dis;
  
  uint64_t id = dis(gen);
  
  std::ostringstream oss;
  oss << std::hex << std::setfill('0') << std::setw(16) << id;
  return oss.str();
}

bool Tracer::ShouldSample() {
  static thread_local std::random_device rd;
  static thread_local std::mt19937 gen(rd());
  static thread_local std::uniform_real_distribution<double> dis(0.0, 1.0);
  
  return dis(gen) < sampling_rate_.load();
}

// ============================================================================
// LogSpanExporter 实现
// ============================================================================

void LogSpanExporter::Export(const std::vector<std::shared_ptr<Span>>& spans) {
  for (const auto& span : spans) {
    std::cout << "[TRACE] " << span->ToJson() << std::endl;
  }
}

// ============================================================================
// JaegerExporter 实现
// ============================================================================

JaegerExporter::JaegerExporter(const std::string& endpoint,
                               const std::string& service_name)
    : endpoint_(endpoint), service_name_(service_name) {
}

void JaegerExporter::Export(const std::vector<std::shared_ptr<Span>>& spans) {
  // 实际实现需要使用 Jaeger 的 Thrift 或 HTTP 协议
  // 这里只是示例框架
  for (const auto& span : spans) {
    // TODO: 发送到 Jaeger collector
    (void)span;
  }
}

// ============================================================================
// ScopedSpan 实现
// ============================================================================

ScopedSpan::ScopedSpan(const std::string& operation_name)
    : span_(Tracer::Instance().StartSpan(operation_name)) {
}

ScopedSpan::ScopedSpan(const std::string& operation_name,
                       std::shared_ptr<Span> parent)
    : span_(Tracer::Instance().StartSpan(operation_name, parent)) {
}

ScopedSpan::~ScopedSpan() {
  if (span_) {
    span_->Finish();
  }
}

void ScopedSpan::SetTag(const std::string& key, const std::string& value) {
  if (span_) {
    span_->SetTag(key, value);
  }
}

void ScopedSpan::Log(const std::string& message) {
  if (span_) {
    span_->Log(message);
  }
}

void ScopedSpan::SetError(const std::string& error_message) {
  if (span_) {
    span_->SetTag("error", "true");
    span_->SetTag("error.message", error_message);
  }
}

}  // namespace observability
}  // namespace tendisplus
