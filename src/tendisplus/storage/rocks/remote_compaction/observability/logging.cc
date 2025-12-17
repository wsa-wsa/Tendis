// Copyright (c) 2024-present, Tencent Inc. All rights reserved.
// 结构化日志实现

#include "logging.h"

#include <chrono>
#include <ctime>
#include <iomanip>
#include <sstream>

namespace tendisplus {
namespace observability {

// ============================================================================
// LogEntry 实现
// ============================================================================

LogEntry::LogEntry(LogLevel level, const std::string& message)
    : level_(level),
      message_(message),
      timestamp_(std::chrono::system_clock::now()) {
}

LogEntry& LogEntry::WithField(const std::string& key, const std::string& value) {
  fields_[key] = value;
  return *this;
}

LogEntry& LogEntry::WithField(const std::string& key, int64_t value) {
  fields_[key] = std::to_string(value);
  return *this;
}

LogEntry& LogEntry::WithField(const std::string& key, uint64_t value) {
  fields_[key] = std::to_string(value);
  return *this;
}

LogEntry& LogEntry::WithField(const std::string& key, double value) {
  std::ostringstream oss;
  oss << std::fixed << std::setprecision(6) << value;
  fields_[key] = oss.str();
  return *this;
}

LogEntry& LogEntry::WithField(const std::string& key, bool value) {
  fields_[key] = value ? "true" : "false";
  return *this;
}

LogEntry& LogEntry::WithError(const std::string& error) {
  fields_["error"] = error;
  return *this;
}

LogEntry& LogEntry::WithTaskId(const std::string& task_id) {
  fields_["task_id"] = task_id;
  return *this;
}

LogEntry& LogEntry::WithWorkerId(const std::string& worker_id) {
  fields_["worker_id"] = worker_id;
  return *this;
}

LogEntry& LogEntry::WithNodeId(const std::string& node_id) {
  fields_["node_id"] = node_id;
  return *this;
}

LogEntry& LogEntry::WithTraceId(const std::string& trace_id) {
  fields_["trace_id"] = trace_id;
  return *this;
}

LogEntry& LogEntry::WithSpanId(const std::string& span_id) {
  fields_["span_id"] = span_id;
  return *this;
}

LogEntry& LogEntry::WithDuration(double seconds) {
  std::ostringstream oss;
  oss << std::fixed << std::setprecision(6) << seconds << "s";
  fields_["duration"] = oss.str();
  return *this;
}

LogEntry& LogEntry::WithBytes(uint64_t bytes) {
  fields_["bytes"] = std::to_string(bytes);
  return *this;
}

std::string LogEntry::ToJson() const {
  std::ostringstream oss;
  oss << "{";
  oss << "\"timestamp\":\"" << FormatTimestamp() << "\",";
  oss << "\"level\":\"" << LogLevelToString(level_) << "\",";
  oss << "\"message\":\"" << EscapeJson(message_) << "\"";
  
  for (const auto& [key, value] : fields_) {
    oss << ",\"" << key << "\":\"" << EscapeJson(value) << "\"";
  }
  
  oss << "}";
  return oss.str();
}

std::string LogEntry::ToText() const {
  std::ostringstream oss;
  oss << "[" << FormatTimestamp() << "] ";
  oss << "[" << LogLevelToString(level_) << "] ";
  oss << message_;
  
  if (!fields_.empty()) {
    oss << " {";
    bool first = true;
    for (const auto& [key, value] : fields_) {
      if (!first) oss << ", ";
      oss << key << "=" << value;
      first = false;
    }
    oss << "}";
  }
  
  return oss.str();
}

std::string LogEntry::FormatTimestamp() const {
  auto time_t = std::chrono::system_clock::to_time_t(timestamp_);
  auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
      timestamp_.time_since_epoch()) % 1000;
  
  std::ostringstream oss;
  oss << std::put_time(std::localtime(&time_t), "%Y-%m-%d %H:%M:%S");
  oss << "." << std::setfill('0') << std::setw(3) << ms.count();
  return oss.str();
}

std::string LogEntry::EscapeJson(const std::string& str) {
  std::ostringstream oss;
  for (char c : str) {
    switch (c) {
      case '"': oss << "\\\""; break;
      case '\\': oss << "\\\\"; break;
      case '\n': oss << "\\n"; break;
      case '\r': oss << "\\r"; break;
      case '\t': oss << "\\t"; break;
      default: oss << c; break;
    }
  }
  return oss.str();
}

std::string LogEntry::LogLevelToString(LogLevel level) {
  switch (level) {
    case LogLevel::kTrace: return "TRACE";
    case LogLevel::kDebug: return "DEBUG";
    case LogLevel::kInfo: return "INFO";
    case LogLevel::kWarn: return "WARN";
    case LogLevel::kError: return "ERROR";
    case LogLevel::kFatal: return "FATAL";
    default: return "UNKNOWN";
  }
}

// ============================================================================
// Logger 实现
// ============================================================================

Logger& Logger::Instance() {
  static Logger instance;
  return instance;
}

Logger::Logger()
    : min_level_(LogLevel::kInfo),
      format_(LogFormat::kText),
      running_(false) {
}

Logger::~Logger() {
  Stop();
}

void Logger::Start() {
  std::lock_guard<std::mutex> lock(mutex_);
  
  if (running_) return;
  
  running_ = true;
  log_thread_ = std::thread(&Logger::LogLoop, this);
}

void Logger::Stop() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!running_) return;
    running_ = false;
  }
  
  cv_.notify_all();
  
  if (log_thread_.joinable()) {
    log_thread_.join();
  }
}

void Logger::SetMinLevel(LogLevel level) {
  min_level_.store(level);
}

void Logger::SetFormat(LogFormat format) {
  format_.store(format);
}

void Logger::AddSink(std::shared_ptr<LogSink> sink) {
  std::lock_guard<std::mutex> lock(mutex_);
  sinks_.push_back(sink);
}

void Logger::Log(LogEntry entry) {
  if (entry.Level() < min_level_.load()) {
    return;
  }
  
  std::lock_guard<std::mutex> lock(mutex_);
  pending_entries_.push_back(std::move(entry));
  cv_.notify_one();
}

void Logger::Trace(const std::string& message) {
  Log(LogEntry(LogLevel::kTrace, message));
}

void Logger::Debug(const std::string& message) {
  Log(LogEntry(LogLevel::kDebug, message));
}

void Logger::Info(const std::string& message) {
  Log(LogEntry(LogLevel::kInfo, message));
}

void Logger::Warn(const std::string& message) {
  Log(LogEntry(LogLevel::kWarn, message));
}

void Logger::Error(const std::string& message) {
  Log(LogEntry(LogLevel::kError, message));
}

void Logger::Fatal(const std::string& message) {
  Log(LogEntry(LogLevel::kFatal, message));
}

LogEntry Logger::WithLevel(LogLevel level, const std::string& message) {
  return LogEntry(level, message);
}

void Logger::LogLoop() {
  while (running_) {
    std::vector<LogEntry> entries_to_write;
    
    {
      std::unique_lock<std::mutex> lock(mutex_);
      cv_.wait_for(lock, std::chrono::milliseconds(100), [this] {
        return !running_ || !pending_entries_.empty();
      });
      
      if (!pending_entries_.empty()) {
        entries_to_write.swap(pending_entries_);
      }
    }
    
    if (!entries_to_write.empty()) {
      LogFormat format = format_.load();
      
      for (const auto& entry : entries_to_write) {
        std::string formatted = (format == LogFormat::kJson) 
            ? entry.ToJson() 
            : entry.ToText();
        
        for (auto& sink : sinks_) {
          sink->Write(entry, formatted);
        }
      }
    }
  }
  
  // 写入剩余的日志
  std::lock_guard<std::mutex> lock(mutex_);
  LogFormat format = format_.load();
  
  for (const auto& entry : pending_entries_) {
    std::string formatted = (format == LogFormat::kJson) 
        ? entry.ToJson() 
        : entry.ToText();
    
    for (auto& sink : sinks_) {
      sink->Write(entry, formatted);
    }
  }
  pending_entries_.clear();
}

// ============================================================================
// ConsoleSink 实现
// ============================================================================

void ConsoleSink::Write(const LogEntry& entry, const std::string& formatted) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  if (entry.Level() >= LogLevel::kError) {
    std::cerr << formatted << std::endl;
  } else {
    std::cout << formatted << std::endl;
  }
}

// ============================================================================
// FileSink 实现
// ============================================================================

FileSink::FileSink(const std::string& filename)
    : filename_(filename), current_size_(0), max_size_(100 * 1024 * 1024) {
  file_.open(filename, std::ios::app);
}

FileSink::~FileSink() {
  if (file_.is_open()) {
    file_.close();
  }
}

void FileSink::Write(const LogEntry& entry, const std::string& formatted) {
  (void)entry;  // unused
  
  std::lock_guard<std::mutex> lock(mutex_);
  
  if (!file_.is_open()) {
    return;
  }
  
  file_ << formatted << std::endl;
  current_size_ += formatted.size() + 1;
  
  if (current_size_ >= max_size_) {
    Rotate();
  }
}

void FileSink::SetMaxSize(size_t max_size) {
  std::lock_guard<std::mutex> lock(mutex_);
  max_size_ = max_size;
}

void FileSink::Rotate() {
  file_.close();
  
  // 重命名当前文件
  auto now = std::chrono::system_clock::now();
  auto time_t = std::chrono::system_clock::to_time_t(now);
  
  std::ostringstream oss;
  oss << filename_ << "." << std::put_time(std::localtime(&time_t), "%Y%m%d%H%M%S");
  std::string rotated_name = oss.str();
  
  std::rename(filename_.c_str(), rotated_name.c_str());
  
  // 重新打开文件
  file_.open(filename_, std::ios::app);
  current_size_ = 0;
}

// ============================================================================
// TaskLogger 实现
// ============================================================================

TaskLogger::TaskLogger(const std::string& task_id)
    : task_id_(task_id) {
}

TaskLogger& TaskLogger::WithWorker(const std::string& worker_id) {
  worker_id_ = worker_id;
  return *this;
}

TaskLogger& TaskLogger::WithNode(const std::string& node_id) {
  node_id_ = node_id;
  return *this;
}

TaskLogger& TaskLogger::WithTrace(const std::string& trace_id,
                                   const std::string& span_id) {
  trace_id_ = trace_id;
  span_id_ = span_id;
  return *this;
}

void TaskLogger::Info(const std::string& message) {
  Log(LogLevel::kInfo, message);
}

void TaskLogger::Warn(const std::string& message) {
  Log(LogLevel::kWarn, message);
}

void TaskLogger::Error(const std::string& message) {
  Log(LogLevel::kError, message);
}

void TaskLogger::Log(LogLevel level, const std::string& message) {
  LogEntry entry(level, message);
  entry.WithTaskId(task_id_);
  
  if (!worker_id_.empty()) {
    entry.WithWorkerId(worker_id_);
  }
  if (!node_id_.empty()) {
    entry.WithNodeId(node_id_);
  }
  if (!trace_id_.empty()) {
    entry.WithTraceId(trace_id_);
  }
  if (!span_id_.empty()) {
    entry.WithSpanId(span_id_);
  }
  
  Logger::Instance().Log(std::move(entry));
}

}  // namespace observability
}  // namespace tendisplus
