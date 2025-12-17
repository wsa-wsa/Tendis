// Copyright (c) 2024-present, Tencent Inc. All rights reserved.
// 结构化日志

#pragma once

#include <chrono>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <vector>

namespace tendisplus {
namespace observability {

// ============================================================================
// 日志级别
// ============================================================================
enum class LogLevel {
  kTrace = 0,
  kDebug = 1,
  kInfo = 2,
  kWarn = 3,
  kError = 4,
  kFatal = 5
};

inline const char* LogLevelToString(LogLevel level) {
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
// 日志字段
// ============================================================================
class LogField {
 public:
  enum class Type { kString, kInt, kDouble, kBool };
  
  LogField(const std::string& key, const std::string& value)
      : key_(key), string_value_(value), type_(Type::kString) {}
  LogField(const std::string& key, int64_t value)
      : key_(key), int_value_(value), type_(Type::kInt) {}
  LogField(const std::string& key, double value)
      : key_(key), double_value_(value), type_(Type::kDouble) {}
  LogField(const std::string& key, bool value)
      : key_(key), bool_value_(value), type_(Type::kBool) {}
  
  const std::string& Key() const { return key_; }
  Type GetType() const { return type_; }
  
  std::string AsString() const;
  int64_t AsInt() const { return int_value_; }
  double AsDouble() const { return double_value_; }
  bool AsBool() const { return bool_value_; }
  
  std::string ToJson() const;

 private:
  std::string key_;
  std::string string_value_;
  int64_t int_value_ = 0;
  double double_value_ = 0.0;
  bool bool_value_ = false;
  Type type_;
};

// ============================================================================
// 日志条目
// ============================================================================
struct LogEntry {
  LogLevel level;
  std::chrono::system_clock::time_point timestamp;
  std::string message;
  std::string logger_name;
  std::string file;
  int line = 0;
  std::string function;
  std::vector<LogField> fields;
  
  // 追踪上下文
  std::string trace_id;
  std::string span_id;
  
  std::string ToJson() const;
  std::string ToText() const;
};

// ============================================================================
// 日志输出器接口
// ============================================================================
class LogSink {
 public:
  virtual ~LogSink() = default;
  virtual void Write(const LogEntry& entry) = 0;
  virtual void Flush() = 0;
};

// ============================================================================
// 控制台输出器
// ============================================================================
class ConsoleSink : public LogSink {
 public:
  explicit ConsoleSink(bool use_colors = true);
  void Write(const LogEntry& entry) override;
  void Flush() override;

 private:
  bool use_colors_;
  std::mutex mutex_;
};

// ============================================================================
// 文件输出器
// ============================================================================
class FileSink : public LogSink {
 public:
  explicit FileSink(const std::string& path, 
                    size_t max_size_mb = 100,
                    int max_files = 10);
  ~FileSink();
  
  void Write(const LogEntry& entry) override;
  void Flush() override;

 private:
  void RotateIfNeeded();
  
  std::string path_;
  size_t max_size_mb_;
  int max_files_;
  std::ofstream* file_ = nullptr;
  size_t current_size_ = 0;
  std::mutex mutex_;
};

// ============================================================================
// JSON 文件输出器
// ============================================================================
class JsonFileSink : public LogSink {
 public:
  explicit JsonFileSink(const std::string& path,
                        size_t max_size_mb = 100,
                        int max_files = 10);
  ~JsonFileSink();
  
  void Write(const LogEntry& entry) override;
  void Flush() override;

 private:
  void RotateIfNeeded();
  
  std::string path_;
  size_t max_size_mb_;
  int max_files_;
  std::ofstream* file_ = nullptr;
  size_t current_size_ = 0;
  std::mutex mutex_;
};

// ============================================================================
// 日志构建器
// ============================================================================
class LogBuilder {
 public:
  LogBuilder(const std::string& logger_name, LogLevel level,
             const char* file, int line, const char* function);
  ~LogBuilder();
  
  // 添加字段
  LogBuilder& Field(const std::string& key, const std::string& value);
  LogBuilder& Field(const std::string& key, int64_t value);
  LogBuilder& Field(const std::string& key, double value);
  LogBuilder& Field(const std::string& key, bool value);
  
  // 设置消息
  LogBuilder& Msg(const std::string& message);
  
  // 流式接口
  template<typename T>
  LogBuilder& operator<<(const T& value) {
    std::ostringstream oss;
    oss << value;
    message_stream_ << oss.str();
    return *this;
  }

 private:
  LogEntry entry_;
  std::ostringstream message_stream_;
};

// ============================================================================
// 日志器
// ============================================================================
class Logger {
 public:
  explicit Logger(const std::string& name);
  ~Logger() = default;
  
  // 获取/创建日志器
  static std::shared_ptr<Logger> Get(const std::string& name);
  static std::shared_ptr<Logger> Default();
  
  // 配置
  void SetLevel(LogLevel level) { level_ = level; }
  LogLevel GetLevel() const { return level_; }
  void AddSink(std::shared_ptr<LogSink> sink);
  void ClearSinks();
  
  // 日志方法
  void Log(const LogEntry& entry);
  
  bool IsEnabled(LogLevel level) const { return level >= level_; }
  
  const std::string& Name() const { return name_; }

 private:
  std::string name_;
  LogLevel level_ = LogLevel::kInfo;
  std::vector<std::shared_ptr<LogSink>> sinks_;
  std::mutex mutex_;
  
  static std::map<std::string, std::shared_ptr<Logger>> loggers_;
  static std::mutex loggers_mutex_;
};

// ============================================================================
// 日志管理器
// ============================================================================
class LogManager {
 public:
  static LogManager& Instance();
  
  // 全局配置
  void SetGlobalLevel(LogLevel level);
  void AddGlobalSink(std::shared_ptr<LogSink> sink);
  void ClearGlobalSinks();
  
  // 获取日志器
  std::shared_ptr<Logger> GetLogger(const std::string& name);
  
  // 刷新所有日志
  void FlushAll();
  
  // 关闭日志系统
  void Shutdown();

 private:
  LogManager() = default;
  
  LogLevel global_level_ = LogLevel::kInfo;
  std::vector<std::shared_ptr<LogSink>> global_sinks_;
  std::map<std::string, std::shared_ptr<Logger>> loggers_;
  std::mutex mutex_;
};

// ============================================================================
// 便捷宏
// ============================================================================
#define LOG_TRACE(logger) \
  if ((logger)->IsEnabled(LogLevel::kTrace)) \
    LogBuilder((logger)->Name(), LogLevel::kTrace, __FILE__, __LINE__, __FUNCTION__)

#define LOG_DEBUG(logger) \
  if ((logger)->IsEnabled(LogLevel::kDebug)) \
    LogBuilder((logger)->Name(), LogLevel::kDebug, __FILE__, __LINE__, __FUNCTION__)

#define LOG_INFO(logger) \
  if ((logger)->IsEnabled(LogLevel::kInfo)) \
    LogBuilder((logger)->Name(), LogLevel::kInfo, __FILE__, __LINE__, __FUNCTION__)

#define LOG_WARN(logger) \
  if ((logger)->IsEnabled(LogLevel::kWarn)) \
    LogBuilder((logger)->Name(), LogLevel::kWarn, __FILE__, __LINE__, __FUNCTION__)

#define LOG_ERROR(logger) \
  if ((logger)->IsEnabled(LogLevel::kError)) \
    LogBuilder((logger)->Name(), LogLevel::kError, __FILE__, __LINE__, __FUNCTION__)

#define LOG_FATAL(logger) \
  LogBuilder((logger)->Name(), LogLevel::kFatal, __FILE__, __LINE__, __FUNCTION__)

// 默认日志器的便捷宏
#define TLOG_TRACE LOG_TRACE(Logger::Default())
#define TLOG_DEBUG LOG_DEBUG(Logger::Default())
#define TLOG_INFO LOG_INFO(Logger::Default())
#define TLOG_WARN LOG_WARN(Logger::Default())
#define TLOG_ERROR LOG_ERROR(Logger::Default())
#define TLOG_FATAL LOG_FATAL(Logger::Default())

}  // namespace observability
}  // namespace tendisplus
