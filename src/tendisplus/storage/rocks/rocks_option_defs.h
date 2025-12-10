// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#ifndef SRC_TENDISPLUS_STORAGE_ROCKS_ROCKS_OPTION_DEFS_H_
#define SRC_TENDISPLUS_STORAGE_ROCKS_ROCKS_OPTION_DEFS_H_

#include <map>
#include <set>
#include <string>
#include <vector>

namespace tendisplus {

/**
 * RocksDB Option Architecture Design
 * ==================================
 *
 * RocksDB has two distinct option categories:
 *
 * 1. DBOptions (Database-level)
 *    - Apply to the entire database instance
 *    - Examples: max_background_jobs, max_open_files
 *    - Set via: DB::SetDBOptions()
 *
 * 2. ColumnFamilyOptions (CF-level)
 *    - Apply to specific column families
 *    - Examples: enable_blob_files, min_blob_size
 *    - Set via: DB::SetOptions(cf_handle, ...)
 *
 * Note: rocksdb::Options = DBOptions + ColumnFamilyOptions (combined class)
 *
 * Configuration Format:
 * ---------------------
 * - rocks.<db_option>           -> DB-level option
 * - rocks.<cf_option>           -> CF option applied to ALL column families
 * - rocks.<cfname>.<cf_option>  -> CF option applied to specific CF
 *
 * Where <cfname> is: "defaultcf" or "binlogcf"
 */

// Option scope enumeration
enum class RocksOptionScope {
  DB,  // Database-level option (rocksdb::DBOptions)
  CF,  // ColumnFamily-level option (rocksdb::ColumnFamilyOptions)
};

// Option mutability
enum class RocksOptionMutability {
  STATIC,   // Can only be set at startup
  DYNAMIC,  // Can be changed at runtime via config set
};

// Option value type
enum class RocksOptionType {
  BOOL,
  INT32,
  INT64,
  UINT64,
  DOUBLE,
  STRING,
  ENUM,
};

/**
 * Target column families for applying options
 */
enum class ColumnFamilyTarget {
  ALL,      // Apply to all column families
  DEFAULT,  // Apply to default CF only
  BINLOG,   // Apply to binlog CF only
};

/**
 * Parse result status - replaces multiple boolean flags
 */
enum class OptionParseStatus {
  INVALID,      // Parse failed - not a valid rocks option format
  DB_OPTION,    // Valid DB-level option
  CF_OPTION,    // Valid CF-level option
  UNKNOWN,      // Valid format but unknown option (for backward compatibility)
};

/**
 * Option descriptor - metadata for a single RocksDB option
 */
struct RocksOptionDescriptor {
  std::string name;                  // Option name (e.g., "enable_blob_files")
  RocksOptionScope scope;            // DB or CF level
  RocksOptionMutability mutability;  // Static or dynamic
  RocksOptionType type;              // Value type
  std::string description;           // Human-readable description
  std::string defaultValue;          // Default value as string

  RocksOptionDescriptor(const std::string& n,
                        RocksOptionScope s,
                        RocksOptionMutability m,
                        RocksOptionType t,
                        const std::string& desc = "",
                        const std::string& defVal = "")
    : name(n),
      scope(s),
      mutability(m),
      type(t),
      description(desc),
      defaultValue(defVal) {}
};

/**
 * Parsed RocksDB option - result from parsing "rocks.xxx" format strings
 *
 * Design rationale:
 * - Uses OptionParseStatus enum instead of multiple booleans (isValid/isCFOption/isDBOption)
 * - ColumnFamilyTarget is only meaningful for CF options
 * - Provides helper methods for common queries
 */
struct ParsedRocksOption {
  std::string name;               // Extracted option name (e.g., "enable_blob_files")
  ColumnFamilyTarget cfTarget;    // Target CFs (only meaningful for CF options)
  OptionParseStatus status;       // Parse result status

  ParsedRocksOption()
    : name(),
      cfTarget(ColumnFamilyTarget::ALL),
      status(OptionParseStatus::INVALID) {}

  // Convenience query methods
  bool isValid() const {
    return status != OptionParseStatus::INVALID;
  }

  bool isCFOption() const {
    return status == OptionParseStatus::CF_OPTION;
  }

  bool isDBOption() const {
    return status == OptionParseStatus::DB_OPTION;
  }

  bool isUnknown() const {
    return status == OptionParseStatus::UNKNOWN;
  }

  // Returns CF name string for specific CF targets, empty for ALL
  std::string getCFName() const {
    switch (cfTarget) {
      case ColumnFamilyTarget::DEFAULT:
        return "defaultcf";
      case ColumnFamilyTarget::BINLOG:
        return "binlogcf";
      default:
        return "";
    }
  }
};

/**
 * Centralized RocksDB option definitions and utilities
 *
 * This class provides:
 * 1. Single source of truth for all RocksDB option metadata
 * 2. Option parsing and validation utilities
 * 3. Extensible option registration mechanism
 */
class RocksOptionDefs {
 public:
  // ============================================
  // Constants
  // ============================================
  static constexpr const char* kDefaultCF = "defaultcf";
  static constexpr const char* kBinlogCF = "binlogcf";
  static constexpr const char* kRocksPrefix = "rocks.";
  // Auto-calculated prefix length (compile-time)
  static constexpr size_t kRocksPrefixLen =
    sizeof("rocks.") - 1;  // 6, excluding null terminator

  // ============================================
  // CF Name Utilities
  // ============================================
  static const std::vector<std::string>& getAllCFNames() {
    static const std::vector<std::string> cfNames = {kDefaultCF, kBinlogCF};
    return cfNames;
  }

  static bool isValidCFName(const std::string& cfName) {
    return cfName == kDefaultCF || cfName == kBinlogCF;
  }

  /**
   * Convert CF name string to ColumnFamilyTarget enum
   */
  static ColumnFamilyTarget cfNameToTarget(const std::string& cfName) {
    if (cfName == kDefaultCF) {
      return ColumnFamilyTarget::DEFAULT;
    } else if (cfName == kBinlogCF) {
      return ColumnFamilyTarget::BINLOG;
    }
    return ColumnFamilyTarget::ALL;
  }

  // ============================================
  // Option Registry Access
  // ============================================

  /**
   * Get all registered option descriptors
   */
  static const std::map<std::string, RocksOptionDescriptor>& getAllOptions() {
    return getOptionRegistry();
  }

  /**
   * Get descriptor for a specific option
   * @return nullptr if option not found
   */
  static const RocksOptionDescriptor* getOptionDescriptor(
    const std::string& optionName) {
    const auto& registry = getOptionRegistry();
    auto it = registry.find(optionName);
    if (it != registry.end()) {
      return &it->second;
    }
    return nullptr;
  }

  // ============================================
  // Option Classification Queries
  // ============================================

  static bool isCFOption(const std::string& optionName) {
    auto desc = getOptionDescriptor(optionName);
    return desc != nullptr && desc->scope == RocksOptionScope::CF;
  }

  static bool isDBOption(const std::string& optionName) {
    auto desc = getOptionDescriptor(optionName);
    return desc != nullptr && desc->scope == RocksOptionScope::DB;
  }

  static bool isDynamicOption(const std::string& optionName) {
    auto desc = getOptionDescriptor(optionName);
    return desc != nullptr &&
           desc->mutability == RocksOptionMutability::DYNAMIC;
  }

  static bool isKnownOption(const std::string& optionName) {
    return getOptionDescriptor(optionName) != nullptr;
  }

  static RocksOptionScope getOptionScope(const std::string& optionName) {
    auto desc = getOptionDescriptor(optionName);
    if (desc != nullptr) {
      return desc->scope;
    }
    // Default to DB scope for unknown options (backward compatibility)
    return RocksOptionScope::DB;
  }

  // ============================================
  // Option Sets (for backward compatibility)
  // ============================================

  static const std::set<std::string>& getDynamicDBOptions() {
    static std::set<std::string> options;
    static bool initialized = false;
    if (!initialized) {
      for (const auto& kv : getOptionRegistry()) {
        if (kv.second.scope == RocksOptionScope::DB &&
            kv.second.mutability == RocksOptionMutability::DYNAMIC) {
          options.insert(kv.first);
        }
      }
      initialized = true;
    }
    return options;
  }

  static const std::set<std::string>& getDynamicCFOptions() {
    static std::set<std::string> options;
    static bool initialized = false;
    if (!initialized) {
      for (const auto& kv : getOptionRegistry()) {
        if (kv.second.scope == RocksOptionScope::CF &&
            kv.second.mutability == RocksOptionMutability::DYNAMIC) {
          options.insert(kv.first);
        }
      }
      initialized = true;
    }
    return options;
  }

  static const std::set<std::string>& getCFOptions() {
    static std::set<std::string> options;
    static bool initialized = false;
    if (!initialized) {
      for (const auto& kv : getOptionRegistry()) {
        if (kv.second.scope == RocksOptionScope::CF) {
          options.insert(kv.first);
        }
      }
      initialized = true;
    }
    return options;
  }

  static const std::set<std::string>& getDBOptions() {
    static std::set<std::string> options;
    static bool initialized = false;
    if (!initialized) {
      for (const auto& kv : getOptionRegistry()) {
        if (kv.second.scope == RocksOptionScope::DB) {
          options.insert(kv.first);
        }
      }
      initialized = true;
    }
    return options;
  }

  // ============================================
  // Option Parsing
  // ============================================

  /**
   * Check if string starts with "rocks." prefix
   * Optimized: uses compare() to avoid creating temporary strings
   */
  static bool hasRocksPrefix(const std::string& str) {
    return str.size() > kRocksPrefixLen &&
           str.compare(0, kRocksPrefixLen, kRocksPrefix) == 0;
  }

  /**
   * Parse a rocks option string and extract its components
   *
   * Input formats:
   *   - "rocks.<option>"           -> DB option or CF option for all CFs
   *   - "rocks.<cfname>.<option>"  -> CF option for specific CF
   *
   * @param optionKey The full option string (e.g., "rocks.enable_blob_files")
   * @return ParsedRocksOption with parse results and status
   */
  static ParsedRocksOption parseOption(const std::string& optionKey) {
    ParsedRocksOption result;

    // Must start with "rocks."
    if (!hasRocksPrefix(optionKey)) {
      return result;  // status = INVALID
    }

    // Extract portion after "rocks."
    const size_t afterPrefix = kRocksPrefixLen;
    const size_t dotPos = optionKey.find('.', afterPrefix);

    if (dotPos == std::string::npos) {
      // Format: rocks.<option_name>
      result.name = optionKey.substr(afterPrefix);
      result.cfTarget = ColumnFamilyTarget::ALL;

      // Determine status from registry
      auto desc = getOptionDescriptor(result.name);
      if (desc != nullptr) {
        result.status = (desc->scope == RocksOptionScope::CF)
                          ? OptionParseStatus::CF_OPTION
                          : OptionParseStatus::DB_OPTION;
      } else {
        // Unknown option - treat as DB option for backward compatibility
        result.status = OptionParseStatus::UNKNOWN;
      }
    } else {
      // Format: rocks.<prefix>.<rest> - check if prefix is a CF name
      const std::string prefix = optionKey.substr(afterPrefix, dotPos - afterPrefix);

      if (isValidCFName(prefix)) {
        // Valid CF name: rocks.<cfname>.<option>
        result.name = optionKey.substr(dotPos + 1);
        result.cfTarget = cfNameToTarget(prefix);
        result.status = OptionParseStatus::CF_OPTION;
      } else {
        // Not a CF name - treat entire suffix as option name
        // e.g., rocks.compaction.style -> option name is "compaction.style"
        result.name = optionKey.substr(afterPrefix);
        result.cfTarget = ColumnFamilyTarget::ALL;

        auto desc = getOptionDescriptor(result.name);
        if (desc != nullptr) {
          result.status = (desc->scope == RocksOptionScope::CF)
                            ? OptionParseStatus::CF_OPTION
                            : OptionParseStatus::DB_OPTION;
        } else {
          result.status = OptionParseStatus::UNKNOWN;
        }
      }
    }

    return result;
  }

  /**
   * Build full option key from components
   *
   * @param optionName The option name
   * @param cfTarget Target column family (ALL means no CF prefix)
   * @return Full option key (e.g., "rocks.defaultcf.enable_blob_files")
   */
  static std::string buildOptionKey(const std::string& optionName,
                                    ColumnFamilyTarget cfTarget = ColumnFamilyTarget::ALL) {
    std::string key(kRocksPrefix);
    switch (cfTarget) {
      case ColumnFamilyTarget::DEFAULT:
        key += kDefaultCF;
        key += '.';
        break;
      case ColumnFamilyTarget::BINLOG:
        key += kBinlogCF;
        key += '.';
        break;
      default:
        break;
    }
    key += optionName;
    return key;
  }

  // Overload for backward compatibility with cfName string
  static std::string buildOptionKey(const std::string& optionName,
                                    const std::string& cfName) {
    return buildOptionKey(optionName, cfNameToTarget(cfName));
  }

 private:
  /**
   * Get the option registry (singleton pattern)
   * This is where all options are registered
   */
  static std::map<std::string, RocksOptionDescriptor>& getOptionRegistry() {
    static std::map<std::string, RocksOptionDescriptor> registry =
      initializeRegistry();
    return registry;
  }

  /**
   * Initialize the option registry with all known options
   *
   * To add a new option:
   * 1. Add an entry to this function
   * 2. That's it! The option will automatically be recognized
   */
  static std::map<std::string, RocksOptionDescriptor> initializeRegistry() {
    std::map<std::string, RocksOptionDescriptor> registry;

    // ========================================
    // DB-level Options (rocksdb::DBOptions)
    // ========================================

    // Dynamic DB options
    registry.emplace(
      "max_background_jobs",
      RocksOptionDescriptor("max_background_jobs",
                            RocksOptionScope::DB,
                            RocksOptionMutability::DYNAMIC,
                            RocksOptionType::INT32,
                            "Maximum number of concurrent background jobs",
                            "2"));

    registry.emplace(
      "max_open_files",
      RocksOptionDescriptor("max_open_files",
                            RocksOptionScope::DB,
                            RocksOptionMutability::DYNAMIC,
                            RocksOptionType::INT32,
                            "Maximum number of open files",
                            "-1"));

    registry.emplace(
      "max_subcompactions",
      RocksOptionDescriptor("max_subcompactions",
                            RocksOptionScope::DB,
                            RocksOptionMutability::DYNAMIC,
                            RocksOptionType::INT32,
                            "Maximum subcompactions for L0->L1",
                            "1"));

    // Static DB options (examples, add more as needed)
    registry.emplace(
      "create_if_missing",
      RocksOptionDescriptor("create_if_missing",
                            RocksOptionScope::DB,
                            RocksOptionMutability::STATIC,
                            RocksOptionType::BOOL,
                            "Create DB if it doesn't exist",
                            "true"));

    // ========================================
    // CF-level Options (rocksdb::ColumnFamilyOptions)
    // ========================================

    // Blob storage options (dynamic)
    registry.emplace(
      "enable_blob_files",
      RocksOptionDescriptor("enable_blob_files",
                            RocksOptionScope::CF,
                            RocksOptionMutability::DYNAMIC,
                            RocksOptionType::BOOL,
                            "Enable blob file storage for large values",
                            "false"));

    registry.emplace(
      "min_blob_size",
      RocksOptionDescriptor("min_blob_size",
                            RocksOptionScope::CF,
                            RocksOptionMutability::DYNAMIC,
                            RocksOptionType::UINT64,
                            "Minimum value size to store in blob files",
                            "0"));

    registry.emplace(
      "blob_file_size",
      RocksOptionDescriptor("blob_file_size",
                            RocksOptionScope::CF,
                            RocksOptionMutability::DYNAMIC,
                            RocksOptionType::UINT64,
                            "Target size for blob files",
                            "268435456"));

    registry.emplace(
      "blob_garbage_collection_age_cutoff",
      RocksOptionDescriptor(
        "blob_garbage_collection_age_cutoff",
        RocksOptionScope::CF,
        RocksOptionMutability::DYNAMIC,
        RocksOptionType::DOUBLE,
        "Age cutoff for blob garbage collection (0.0-1.0)",
        "0.25"));

    registry.emplace(
      "blob_garbage_collection_force_threshold",
      RocksOptionDescriptor(
        "blob_garbage_collection_force_threshold",
        RocksOptionScope::CF,
        RocksOptionMutability::DYNAMIC,
        RocksOptionType::DOUBLE,
        "Force GC threshold for blob files (0.0-1.0)",
        "1.0"));

    registry.emplace(
      "blob_compaction_readahead_size",
      RocksOptionDescriptor("blob_compaction_readahead_size",
                            RocksOptionScope::CF,
                            RocksOptionMutability::DYNAMIC,
                            RocksOptionType::UINT64,
                            "Readahead size for blob compaction",
                            "0"));

    registry.emplace(
      "blob_file_starting_level",
      RocksOptionDescriptor("blob_file_starting_level",
                            RocksOptionScope::CF,
                            RocksOptionMutability::DYNAMIC,
                            RocksOptionType::INT32,
                            "Starting level for blob files",
                            "0"));

    registry.emplace(
      "prepopulate_blob_cache",
      RocksOptionDescriptor("prepopulate_blob_cache",
                            RocksOptionScope::CF,
                            RocksOptionMutability::DYNAMIC,
                            RocksOptionType::ENUM,
                            "Prepopulate blob cache strategy",
                            "kDisable"));

    registry.emplace(
      "enable_blob_garbage_collection",
      RocksOptionDescriptor("enable_blob_garbage_collection",
                            RocksOptionScope::CF,
                            RocksOptionMutability::DYNAMIC,
                            RocksOptionType::BOOL,
                            "Enable garbage collection for blob files",
                            "false"));

    registry.emplace(
      "blob_compression_type",
      RocksOptionDescriptor("blob_compression_type",
                            RocksOptionScope::CF,
                            RocksOptionMutability::DYNAMIC,
                            RocksOptionType::ENUM,
                            "Compression type for blob files",
                            "kNoCompression"));

    // Compaction options (dynamic)
    registry.emplace(
      "disable_auto_compactions",
      RocksOptionDescriptor("disable_auto_compactions",
                            RocksOptionScope::CF,
                            RocksOptionMutability::DYNAMIC,
                            RocksOptionType::BOOL,
                            "Disable automatic compactions",
                            "false"));

    registry.emplace(
      "periodic_compaction_seconds",
      RocksOptionDescriptor("periodic_compaction_seconds",
                            RocksOptionScope::CF,
                            RocksOptionMutability::DYNAMIC,
                            RocksOptionType::UINT64,
                            "Periodic compaction interval in seconds",
                            "0"));

    registry.emplace(
      "level0_file_num_compaction_trigger",
      RocksOptionDescriptor(
        "level0_file_num_compaction_trigger",
        RocksOptionScope::CF,
        RocksOptionMutability::DYNAMIC,
        RocksOptionType::INT32,
        "Number of L0 files to trigger compaction",
        "4"));

    registry.emplace(
      "level0_slowdown_writes_trigger",
      RocksOptionDescriptor("level0_slowdown_writes_trigger",
                            RocksOptionScope::CF,
                            RocksOptionMutability::DYNAMIC,
                            RocksOptionType::INT32,
                            "L0 file count to slow down writes",
                            "20"));

    registry.emplace(
      "level0_stop_writes_trigger",
      RocksOptionDescriptor("level0_stop_writes_trigger",
                            RocksOptionScope::CF,
                            RocksOptionMutability::DYNAMIC,
                            RocksOptionType::INT32,
                            "L0 file count to stop writes",
                            "36"));

    // Static CF options (examples)
    registry.emplace(
      "write_buffer_size",
      RocksOptionDescriptor("write_buffer_size",
                            RocksOptionScope::CF,
                            RocksOptionMutability::STATIC,
                            RocksOptionType::UINT64,
                            "Size of a single memtable",
                            "67108864"));

    registry.emplace(
      "max_write_buffer_number",
      RocksOptionDescriptor("max_write_buffer_number",
                            RocksOptionScope::CF,
                            RocksOptionMutability::STATIC,
                            RocksOptionType::INT32,
                            "Maximum number of memtables",
                            "2"));

    registry.emplace(
      "target_file_size_base",
      RocksOptionDescriptor("target_file_size_base",
                            RocksOptionScope::CF,
                            RocksOptionMutability::STATIC,
                            RocksOptionType::UINT64,
                            "Target file size for L1",
                            "67108864"));

    return registry;
  }
};

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_STORAGE_ROCKS_ROCKS_OPTION_DEFS_H_
