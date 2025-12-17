#include <grpc/grpc.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>

#include <cassert>
#include <ostream>
#include <string>
#include <mutex>
#include <unordered_map>
#include <fstream>
#include <filesystem>

#include "csa.grpc.pb.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/sst_file_reader.h"
#include "rocksdb/env.h"
#include "db/compaction/compaction_job.h"
#include "db/version_edit.h"
#include "db/log_writer.h"
#include "file/writable_file_writer.h"
#include "db/dbformat.h"
#include "util/string_util.h"

#ifdef HDFS
#include "plugin/hdfs/env_hdfs.h"
#endif

#include "def.h"
#include "rocksdb/statistics.h"
#include "tendisplus/storage/rocks/nfs_filesystem_simple.h"
#include "tendisplus/storage/rocks/nfs_filesystem.h"

namespace fs = std::filesystem;

ROCKSDB_NAMESPACE::RemoteOpenAndCompactOptions compaction_service_options;

// Helper function to create a minimal MANIFEST file from SST file metadata (方案三)
// This allows CSA to run compaction without accessing Primary's MANIFEST
// Note: This is a simplified implementation that writes MANIFEST in RocksDB's log format
rocksdb::Status CreateManifestFromMetadata(
    const std::string& db_path,
    const google::protobuf::RepeatedPtrField<csa::SstFileMetadata>& file_metadata,
    rocksdb::Env* env = rocksdb::Env::Default()) {
  
  if (file_metadata.empty()) {
    std::cerr << "[CSA] No file metadata provided, cannot create MANIFEST" << std::endl;
    return rocksdb::Status::InvalidArgument("No file metadata provided");
  }
  
  std::cout << "[CSA] Creating MANIFEST from " << file_metadata.size() 
            << " file metadata entries" << std::endl;
  
  // Find the maximum file number and sequence number first
  uint64_t max_file_number = 0;
  uint64_t max_seqno = 0;
  for (const auto& meta : file_metadata) {
    if (meta.file_number() > max_file_number) {
      max_file_number = meta.file_number();
    }
    if (meta.largest_seqno() > max_seqno) {
      max_seqno = meta.largest_seqno();
    }
  }
  
  // Create MANIFEST file using FileSystem API
  std::string manifest_path = db_path + "/MANIFEST-000001";
  
  // Use EnvOptions for file creation
  rocksdb::EnvOptions env_options;
  
  // Create the writable file
  std::unique_ptr<rocksdb::FSWritableFile> fs_file;
  rocksdb::IOStatus ios = env->GetFileSystem()->NewWritableFile(
      manifest_path, rocksdb::FileOptions(env_options), &fs_file, nullptr);
  if (!ios.ok()) {
    std::cerr << "[CSA] Failed to create MANIFEST file: " << ios.ToString() << std::endl;
    return rocksdb::Status::IOError(ios.ToString());
  }
  
  // Wrap in WritableFileWriter
  std::unique_ptr<rocksdb::WritableFileWriter> file_writer(
      new rocksdb::WritableFileWriter(
          std::move(fs_file), manifest_path, rocksdb::FileOptions(env_options)));
  
  // Create log writer for MANIFEST
  std::unique_ptr<rocksdb::log::Writer> log_writer(
      new rocksdb::log::Writer(std::move(file_writer), 0, false));
  
  // Write the first edit: set up basic DB parameters (for default column family)
  // This edit should NOT have column family set (it's for the initial DB setup)
  rocksdb::VersionEdit init_edit;
  init_edit.SetComparatorName("leveldb.BytewiseComparator");  // Default comparator
  init_edit.SetLogNumber(0);
  init_edit.SetNextFile(max_file_number + 1000);
  init_edit.SetLastSequence(max_seqno + 1);
  
  std::string init_record;
  // ts_sz = 0 for BytewiseComparator (no timestamp)
  if (!init_edit.EncodeTo(&init_record, 0 /* ts_sz */)) {
    std::cerr << "[CSA] Failed to encode init edit" << std::endl;
    return rocksdb::Status::Corruption("Failed to encode init edit");
  }
  rocksdb::IOStatus write_status = log_writer->AddRecord(rocksdb::Slice(init_record));
  if (!write_status.ok()) {
    std::cerr << "[CSA] Failed to write init edit to MANIFEST: " << write_status.ToString() << std::endl;
    return rocksdb::Status::IOError(write_status.ToString());
  }
  
  // Write the second edit: add files to default column family (ID=0)
  rocksdb::VersionEdit file_edit;
  file_edit.SetColumnFamily(0);
  
  for (const auto& meta : file_metadata) {
    // Parse smallest and largest keys
    rocksdb::InternalKey smallest, largest;
    smallest.DecodeFrom(rocksdb::Slice(meta.smallest_key()));
    largest.DecodeFrom(rocksdb::Slice(meta.largest_key()));
    
    // Create FileMetaData and add to edit
    file_edit.AddFile(
        meta.level(),                    // level
        meta.file_number(),              // file number
        0,                               // file_path_id
        meta.file_size(),                // file size
        smallest,                        // smallest key
        largest,                         // largest key
        meta.smallest_seqno(),           // smallest seqno
        meta.largest_seqno(),            // largest seqno
        meta.marked_for_compaction(),    // marked_for_compaction
        rocksdb::Temperature::kUnknown,  // temperature
        meta.oldest_blob_file_number(),  // oldest_blob_file_number
        meta.oldest_ancester_time(),     // oldest_ancester_time
        meta.file_creation_time(),       // file_creation_time
        meta.epoch_number(),             // epoch_number
        "",                              // file_checksum
        "",                              // file_checksum_func_name
        {0, 0},                          // unique_id
        0,                               // compensated_range_deletion_size
        0,                               // tail_size
        true                             // user_defined_timestamps_persisted
    );
    
    std::cout << "[CSA] Added file " << meta.file_name() 
              << " (level=" << meta.level() 
              << ", file_number=" << meta.file_number()
              << ", size=" << meta.file_size() << ")" << std::endl;
  }
  
  // Write the file list edit
  std::string file_record;
  // ts_sz = 0 for BytewiseComparator (no timestamp)
  if (!file_edit.EncodeTo(&file_record, 0 /* ts_sz */)) {
    std::cerr << "[CSA] Failed to encode file edit" << std::endl;
    return rocksdb::Status::Corruption("Failed to encode file edit");
  }
  write_status = log_writer->AddRecord(rocksdb::Slice(file_record));
  if (!write_status.ok()) {
    std::cerr << "[CSA] Failed to write file edit to MANIFEST: " << write_status.ToString() << std::endl;
    return rocksdb::Status::IOError(write_status.ToString());
  }
  
  // Sync the log writer
  write_status = log_writer->file()->Sync(false);
  if (!write_status.ok()) {
    std::cerr << "[CSA] Failed to sync MANIFEST: " << write_status.ToString() << std::endl;
    return rocksdb::Status::IOError(write_status.ToString());
  }
  
  // Create CURRENT file pointing to MANIFEST-000001
  std::string current_path = db_path + "/CURRENT";
  rocksdb::Status s = rocksdb::WriteStringToFile(
      env, "MANIFEST-000001\n", current_path, true /* should_sync */);
  if (!s.ok()) {
    std::cerr << "[CSA] Failed to create CURRENT file: " << s.ToString() << std::endl;
    return s;
  }
  
  std::cout << "[CSA] Successfully created MANIFEST with " << file_metadata.size() 
            << " files" << std::endl;
  
  return rocksdb::Status::OK();
}

std::atomic<int64_t> local_task_nums_ = std::atomic<int64_t>(0);
int64_t max_task_nums_ = compaction_service_options.csa_max_concurrent_tasks;

// Shared FileSystem Cache
// Cache shared file systems that have been created to avoid recreating them every time
class SharedFileSystemCache {
 public:
  struct CachedFS {
    std::shared_ptr<rocksdb::FileSystem> fs;
    std::unique_ptr<rocksdb::Env> env;
    std::string uri;
    std::string local_prefix;
  };

  // Get or create a shared file system
  // Returns the Env pointer (possibly nullptr means using the default)
  rocksdb::Env* GetOrCreateEnv(const std::string& uri, 
                                const std::string& local_prefix) {
    if (uri.empty() || local_prefix.empty()) {
      return nullptr;
    }

    std::string cache_key = uri + "|" + local_prefix;
    
    {
      std::lock_guard<std::mutex> lock(mutex_);
      auto it = cache_.find(cache_key);
      if (it != cache_.end()) {
        std::cout << "[FSCache] Reusing cached FileSystem for: " << uri << std::endl;
        return it->second.env.get();
      }
    }

    // A new FileSystem needs to be created
    std::shared_ptr<rocksdb::FileSystem> fs;
    std::unique_ptr<rocksdb::Env> env;
    
    // Create the corresponding file system according to the URI scheme
    if (uri.find("nfs://") == 0) {
      std::cout << "[FSCache] Creating NFS FileSystem for: " << uri 
                  << ", local_prefix: " << local_prefix << std::endl;
      // rocksdb::Status status = rocksdb::NewNFSFileSystemSimple(uri, local_prefix, &fs);
      rocksdb::Status status = rocksdb::NewNFSFileSystem(uri, local_prefix, &fs);
      if (!status.ok() || !fs) {
        std::cerr << "[FSCache] Failed to create NFS FileSystem: " 
                  << status.ToString() << std::endl;
        return nullptr;
      }
      env = rocksdb::NewCompositeEnv(fs);
    }
#ifdef HDFS
    else if (uri.find("hdfs://") == 0) {
      rocksdb::NewHdfsEnv(uri, &env);
    }
#endif
    // More storage types can be added in the future:
    // else if (uri.find("s3://") == 0) { ... }
    // else if (uri.find("gcs://") == 0) { ... }
    else {
      std::cerr << "[FSCache] Unsupported URI scheme: " << uri << std::endl;
      return nullptr;
    }

    if (!env) {
      std::cerr << "[FSCache] Failed to create Env for: " << uri << std::endl;
      return nullptr;
    }

    std::cout << "[FSCache] Created new FileSystem for: " << uri 
              << ", local_prefix: " << local_prefix << std::endl;

    std::lock_guard<std::mutex> lock(mutex_);
    // Double-check after acquiring lock
    auto it = cache_.find(cache_key);
    if (it != cache_.end()) {
      return it->second.env.get();
    }

    CachedFS cached;
    cached.fs = std::move(fs);
    cached.env = std::move(env);
    cached.uri = uri;
    cached.local_prefix = local_prefix;
    
    rocksdb::Env* result = cached.env.get();
    cache_[cache_key] = std::move(cached);
    return result;
  }

  // Remove the specified cache (for configuration changes)
  void Invalidate(const std::string& uri, const std::string& local_prefix) {
    std::string cache_key = uri + "|" + local_prefix;
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = cache_.find(cache_key);
    if (it != cache_.end()) {
      std::cout << "[FSCache] Invalidated FileSystem cache for: " << uri << std::endl;
      cache_.erase(it);
    }
  }

  // Empty all caches
  void Clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    cache_.clear();
    std::cout << "[FSCache] Cleared all cached FileSystems" << std::endl;
  }

  static SharedFileSystemCache& Instance() {
    static SharedFileSystemCache instance;
    return instance;
  }

 private:
  SharedFileSystemCache() = default;
  std::mutex mutex_;
  std::unordered_map<std::string, CachedFS> cache_;
};

// Job file manager for network transfer mode
// Manages uploaded input files and generated output files for each job
class JobFileManager {
 public:
  struct JobFiles {
    std::string job_id;
    std::string work_dir;           // Working directory for this job
    std::vector<std::string> input_files;   // Uploaded input files
    std::vector<std::string> output_files;  // Generated output files
    std::mutex mutex;
  };

  // Get or create job directory
  std::string GetJobDir(const std::string& job_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    std::string job_dir = base_dir_ + "/" + job_id;
    if (!fs::exists(job_dir)) {
      fs::create_directories(job_dir);
      std::cout << "[JobFileManager] Created job directory: " << job_dir << std::endl;
    }
    return job_dir;
  }

  // Register an uploaded input file
  void AddInputFile(const std::string& job_id, const std::string& file_name) {
    std::lock_guard<std::mutex> lock(mutex_);
    jobs_[job_id].input_files.push_back(file_name);
  }

  // Register an output file
  void AddOutputFile(const std::string& job_id, const std::string& file_name) {
    std::lock_guard<std::mutex> lock(mutex_);
    jobs_[job_id].output_files.push_back(file_name);
  }

  // Get output files for a job
  std::vector<std::string> GetOutputFiles(const std::string& job_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = jobs_.find(job_id);
    if (it != jobs_.end()) {
      return it->second.output_files;
    }
    return {};
  }

  // Cleanup job files
  void CleanupJob(const std::string& job_id) {
    std::string job_dir;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      job_dir = base_dir_ + "/" + job_id;
      jobs_.erase(job_id);
    }
    
    if (fs::exists(job_dir)) {
      std::error_code ec;
      fs::remove_all(job_dir, ec);
      if (ec) {
        std::cerr << "[JobFileManager] Failed to cleanup job dir: " << job_dir 
                  << ", error: " << ec.message() << std::endl;
      } else {
        std::cout << "[JobFileManager] Cleaned up job directory: " << job_dir << std::endl;
      }
    }
  }

  // Scan output directory for generated files
  void ScanOutputFiles(const std::string& job_id, const std::string& output_dir) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (!fs::exists(output_dir)) {
      return;
    }
    
    for (const auto& entry : fs::directory_iterator(output_dir)) {
      if (entry.is_regular_file()) {
        std::string file_name = entry.path().filename().string();
        // Only track SST and blob files
        if (file_name.find(".sst") != std::string::npos ||
            file_name.find(".blob") != std::string::npos) {
          jobs_[job_id].output_files.push_back(file_name);
          std::cout << "[JobFileManager] Found output file: " << file_name << std::endl;
        }
      }
    }
  }

  void SetBaseDir(const std::string& dir) {
    base_dir_ = dir;
    if (!fs::exists(base_dir_)) {
      fs::create_directories(base_dir_);
    }
  }

  static JobFileManager& Instance() {
    static JobFileManager instance;
    return instance;
  }

 private:
  JobFileManager() : base_dir_("/tmp/csa_work") {
    if (!fs::exists(base_dir_)) {
      fs::create_directories(base_dir_);
    }
  }
  
  std::string base_dir_;
  std::mutex mutex_;
  std::unordered_map<std::string, JobFiles> jobs_;
};

// CSA Service Implementation
class CSAImpl final : public csa::CSAService::Service {
 public:
  // Execute compaction task (supports both shared storage and network transfer modes)
  grpc::Status ExecuteCompactionTask(
      grpc::ServerContext* context, const csa::CompactionArgs* compaction_args,
      csa::CompactionReply* compaction_reply) override {
    local_task_nums_ += 1;
    while (local_task_nums_ > static_cast<int>(max_task_nums_)) {
      std::cout << "Wait" << std::endl;
      sleep(1);
    }
    std::cout << "CSA ExecuteCompactionTask() concurrency: " << local_task_nums_
              << std::endl;
    
    std::string compaction_service_result;
    rocksdb::CompactionServiceOptionsOverride options_override;
    ROCKSDB_NAMESPACE::Options options_;

    std::string db_path = compaction_args->name();
    std::string output_directory = compaction_args->output_directory();
    
    // Check if using network transfer mode
    bool use_network_transfer = compaction_args->use_network_transfer();
    std::string job_id = compaction_args->job_id();
    std::string compaction_input = compaction_args->input();
    
    if (use_network_transfer && !job_id.empty()) {
      // Network transfer mode: use local job directory
      std::string job_dir = JobFileManager::Instance().GetJobDir(job_id);
      db_path = job_dir;
      output_directory = job_dir + "/output";
      
      // Create output directory
      if (!fs::exists(output_directory)) {
        fs::create_directories(output_directory);
      }
      
      // List files in job directory for debugging
      std::cout << "[CSA] Files in job directory " << job_dir << ":" << std::endl;
      for (const auto& entry : fs::directory_iterator(job_dir)) {
        std::string filename = entry.path().filename().string();
        uint64_t size = 0;
        if (entry.is_regular_file()) {
          size = entry.file_size();
        }
        std::cout << "  " << filename << " (" << size << " bytes)" << std::endl;
      }
      
      // Also list subdirectories
      for (const auto& entry : fs::directory_iterator(job_dir)) {
        if (entry.is_directory()) {
          std::cout << "[CSA] Subdirectory " << entry.path().filename().string() << ":" << std::endl;
          for (const auto& subentry : fs::directory_iterator(entry.path())) {
            std::string subname = subentry.path().filename().string();
            uint64_t subsize = 0;
            if (subentry.is_regular_file()) {
              subsize = subentry.file_size();
            }
            std::cout << "    " << subname << " (" << subsize << " bytes)" << std::endl;
          }
        }
      }
      
      // Modify the compaction input to use local file paths
      // Parse the input, modify file paths, and re-serialize
      rocksdb::CompactionServiceInput parsed_input;
      rocksdb::Status parse_status = rocksdb::CompactionServiceInput::Read(
          compaction_input, &parsed_input);
      
      if (parse_status.ok()) {
        // Update input_files to use just the filename (files are in job_dir root)
        std::vector<std::string> new_input_files;
        for (const auto& file : parsed_input.input_files) {
          // Extract just the filename from the path
          std::string filename = fs::path(file).filename().string();
          new_input_files.push_back(filename);
          std::cout << "[CSA] Remapped input file: " << file << " -> " << filename << std::endl;
        }
        parsed_input.input_files = std::move(new_input_files);
        
        // Re-serialize the modified input
        std::string new_input;
        rocksdb::Status write_status = parsed_input.Write(&new_input);
        if (write_status.ok()) {
          compaction_input = std::move(new_input);
          std::cout << "[CSA] Successfully remapped " << parsed_input.input_files.size() 
                    << " input files" << std::endl;
        } else {
          std::cerr << "[CSA] Failed to re-serialize compaction input: " 
                    << write_status.ToString() << std::endl;
        }
      } else {
        std::cerr << "[CSA] Failed to parse compaction input: " 
                  << parse_status.ToString() << std::endl;
      }
      
      std::cout << "[CSA] Network transfer mode - job_id: " << job_id 
                << ", db_path: " << db_path 
                << ", output_dir: " << output_directory << std::endl;
      
      // Check if file metadata is provided (方案三: preferred method)
      bool has_file_metadata = compaction_args->input_file_metadata_size() > 0;
      
      // Remove any existing metadata files first
      for (const auto& entry : fs::directory_iterator(job_dir)) {
        std::string filename = entry.path().filename().string();
        if (filename.find("MANIFEST-") == 0 || 
            filename == "CURRENT" ||
            filename.find("OPTIONS-") == 0) {
          std::cout << "[CSA] Removing old metadata file: " << filename << std::endl;
          fs::remove(entry.path());
        }
      }
      
      if (has_file_metadata) {
        // 方案三: Create MANIFEST from file metadata (preferred method)
        // This works without shared storage and doesn't require custom comparator on CSA
        std::cout << "[CSA] Using file metadata mode (方案三) with " 
                  << compaction_args->input_file_metadata_size() << " files" << std::endl;
        
        rocksdb::Status manifest_status = CreateManifestFromMetadata(
            job_dir, compaction_args->input_file_metadata());
        
        if (!manifest_status.ok()) {
          std::cerr << "[CSA] Failed to create MANIFEST from metadata: " 
                    << manifest_status.ToString() << std::endl;
          compaction_reply->set_code(manifest_status.code());
          compaction_reply->set_result("CreateManifestFromMetadata failed: " + manifest_status.ToString());
          local_task_nums_ -= 1;
          return grpc::Status::OK;
        }
        
        std::cout << "[CSA] Successfully created MANIFEST from file metadata" << std::endl;
        
        // List files after MANIFEST creation
        std::cout << "[CSA] Files after MANIFEST creation:" << std::endl;
        for (const auto& entry : fs::directory_iterator(job_dir)) {
          std::string filename = entry.path().filename().string();
          uint64_t size = 0;
          if (entry.is_regular_file()) {
            size = entry.file_size();
          }
          std::cout << "  " << filename << " (" << size << " bytes)" << std::endl;
        }
      } else {
        // Fallback: Check if we can use symlink mode (CSA can access primary's filesystem)
        std::string original_db_path = compaction_args->original_db_path();
        bool use_symlink_mode = !original_db_path.empty() && fs::exists(original_db_path);
        
        if (use_symlink_mode) {
          // Symlink mode: create symlinks to original MANIFEST files
          std::cout << "[CSA] Using symlink mode - original_db_path: " << original_db_path << std::endl;
          
          // Create symlinks to original database metadata files
          for (const auto& entry : fs::directory_iterator(original_db_path)) {
            std::string filename = entry.path().filename().string();
            if (filename.find("MANIFEST-") == 0 || 
                filename == "CURRENT" ||
                filename.find("OPTIONS-") == 0 ||
                filename == "IDENTITY") {
              std::string link_path = job_dir + "/" + filename;
              try {
                fs::create_symlink(entry.path(), link_path);
                std::cout << "[CSA] Created symlink: " << filename 
                          << " -> " << entry.path().string() << std::endl;
              } catch (const std::exception& e) {
                std::cerr << "[CSA] Failed to create symlink for " << filename 
                          << ": " << e.what() << std::endl;
              }
            }
          }
          
          // List files after symlink creation
          std::cout << "[CSA] Files after symlink creation:" << std::endl;
          for (const auto& entry : fs::directory_iterator(job_dir)) {
            std::string filename = entry.path().filename().string();
            std::string type = fs::is_symlink(entry.path()) ? " (symlink)" : "";
            uint64_t size = 0;
            if (entry.is_regular_file() || fs::is_symlink(entry.path())) {
              try {
                size = fs::file_size(entry.path());
              } catch (...) {}
            }
            std::cout << "  " << filename << " (" << size << " bytes)" << type << std::endl;
          }
        } else {
          // Last resort: use RepairDB (may not work correctly with custom comparators)
          std::cout << "[CSA] No file metadata and symlink mode not available, "
                    << "falling back to RepairDB..." << std::endl;
          
          // Run RepairDB - NOTE: This may produce incorrect results with custom comparators
          rocksdb::Options repair_options;
          repair_options.create_if_missing = true;
          rocksdb::Status repair_status = rocksdb::RepairDB(db_path, repair_options);
          if (!repair_status.ok()) {
            std::cerr << "[CSA] RepairDB failed: " << repair_status.ToString() << std::endl;
            compaction_reply->set_code(repair_status.code());
            compaction_reply->set_result("RepairDB failed: " + repair_status.ToString());
            local_task_nums_ -= 1;
            return grpc::Status::OK;
          }
          std::cout << "[CSA] RepairDB completed successfully" << std::endl;
          
          // List files after repair
          std::cout << "[CSA] Files after RepairDB:" << std::endl;
          for (const auto& entry : fs::directory_iterator(job_dir)) {
            std::string filename = entry.path().filename().string();
            uint64_t size = 0;
            if (entry.is_regular_file()) {
              size = entry.file_size();
            }
            std::cout << "  " << filename << " (" << size << " bytes)" << std::endl;
          }
        }
      }
    } else {
      // Shared storage mode: use shared filesystem
      const std::string& shared_fs_uri = compaction_args->shared_fs_uri();
      const std::string& shared_fs_local_prefix = compaction_args->shared_fs_local_prefix();
      
      rocksdb::Env* shared_env = SharedFileSystemCache::Instance().GetOrCreateEnv(
          shared_fs_uri, shared_fs_local_prefix);
      
      if (shared_env) {
        options_override.env = shared_env;
        std::cout << "[CSA] Shared storage mode - using FileSystem: " << shared_fs_uri << std::endl;
      }
    }

    options_override.file_checksum_gen_factory =
        options_.file_checksum_gen_factory;
    options_override.comparator = options_.comparator;
    options_override.merge_operator = options_.merge_operator;
    options_override.compaction_filter = options_.compaction_filter;
    options_override.compaction_filter_factory =
        options_.compaction_filter_factory;
    options_override.prefix_extractor = options_.prefix_extractor;
    options_override.table_factory = options_.table_factory;
    options_override.sst_partitioner_factory = options_.sst_partitioner_factory;
    options_override.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();

    rocksdb::Status s = ROCKSDB_NAMESPACE::DB::OpenAndCompact(
        db_path, output_directory,
        compaction_input, &compaction_service_result, options_override);
    
    std::cout << "[CSA] OpenAndCompact result: " << s.ToString() 
              << ", result_size=" << compaction_service_result.size() << std::endl;
    
    // For network transfer mode, scan output files and fix output path
    if (use_network_transfer && !job_id.empty() && s.ok()) {
      JobFileManager::Instance().ScanOutputFiles(job_id, output_directory);
      
      // Fix the output_path in compaction result to use Primary's expected path
      // The result contains CSA's local path, but Primary expects its own path
      std::string primary_output_dir = compaction_args->output_directory();
      
      rocksdb::CompactionServiceResult parsed_result;
      rocksdb::Status parse_status = rocksdb::CompactionServiceResult::Read(
          compaction_service_result, &parsed_result);
      
      if (parse_status.ok()) {
        std::cout << "[CSA] Original output_path in result: " << parsed_result.output_path << std::endl;
        std::cout << "[CSA] Primary's expected output_path: " << primary_output_dir << std::endl;
        
        // Replace CSA's output path with Primary's expected path
        parsed_result.output_path = primary_output_dir;
        
        // Re-serialize the result
        std::string new_result;
        rocksdb::Status write_status = parsed_result.Write(&new_result);
        if (write_status.ok()) {
          compaction_service_result = std::move(new_result);
          std::cout << "[CSA] Successfully fixed output_path to: " << primary_output_dir << std::endl;
        } else {
          std::cerr << "[CSA] Failed to re-serialize compaction result: " 
                    << write_status.ToString() << std::endl;
        }
      } else {
        std::cerr << "[CSA] Failed to parse compaction result: " 
                  << parse_status.ToString() << std::endl;
      }
    }
    
    compaction_reply->set_code(s.code());
    
    if (!s.ok()) {
      std::cerr << "Compaction failed: " << s.ToString() << std::endl;
      compaction_reply->set_result(compaction_service_result);
    } else {
      compaction_reply->set_result(std::move(compaction_service_result));
      std::cout << "Compaction completed successfully" << std::endl;
    }
    
    local_task_nums_ -= 1;
    std::cout << "Compaction finished with code: " << static_cast<int>(s.code()) << std::endl;
    return ::grpc::Status::OK;
  }

  // Upload file from client (streaming)
  grpc::Status UploadFile(grpc::ServerContext* context,
                          grpc::ServerReader<csa::FileChunk>* reader,
                          csa::FileTransferResponse* response) override {
    csa::FileChunk chunk;
    std::string current_job_id;
    std::string current_file_name;
    std::ofstream current_file;
    uint64_t bytes_received = 0;
    
    while (reader->Read(&chunk)) {
      // Check if we need to open a new file
      if (chunk.job_id() != current_job_id || chunk.file_name() != current_file_name) {
        // Close previous file if open
        if (current_file.is_open()) {
          current_file.close();
          JobFileManager::Instance().AddInputFile(current_job_id, current_file_name);
          std::cout << "[CSA] Received file: " << current_file_name 
                    << " (" << bytes_received << " bytes)" << std::endl;
        }
        
        // Open new file
        current_job_id = chunk.job_id();
        current_file_name = chunk.file_name();
        bytes_received = 0;
        
        std::string job_dir = JobFileManager::Instance().GetJobDir(current_job_id);
        std::string file_path = job_dir + "/" + current_file_name;
        
        // Create parent directory if needed
        fs::path parent = fs::path(file_path).parent_path();
        if (!parent.empty() && !fs::exists(parent)) {
          fs::create_directories(parent);
        }
        
        current_file.open(file_path, std::ios::binary);
        if (!current_file.is_open()) {
          response->set_code(1);
          response->set_message("Failed to create file: " + file_path);
          return grpc::Status::OK;
        }
      }
      
      // Write data
      current_file.write(chunk.data().data(), chunk.data().size());
      bytes_received += chunk.data().size();
      
      if (chunk.is_last_chunk()) {
        current_file.close();
        JobFileManager::Instance().AddInputFile(current_job_id, current_file_name);
        std::cout << "[CSA] Received file: " << current_file_name 
                  << " (" << bytes_received << " bytes)" << std::endl;
        current_file_name.clear();
      }
    }
    
    // Close any remaining file
    if (current_file.is_open()) {
      current_file.close();
      JobFileManager::Instance().AddInputFile(current_job_id, current_file_name);
    }
    
    response->set_code(0);
    response->set_message("Upload successful");
    return grpc::Status::OK;
  }

  // Download files to client (streaming)
  grpc::Status DownloadFile(grpc::ServerContext* context,
                            const csa::FileTransferRequest* request,
                            grpc::ServerWriter<csa::FileChunk>* writer) override {
    std::string job_id = request->job_id();
    std::string job_dir = JobFileManager::Instance().GetJobDir(job_id);
    std::string output_dir = job_dir + "/output";
    
    static const size_t kChunkSize = 4 * 1024 * 1024;  // 4MB chunks
    std::vector<char> buffer(kChunkSize);
    
    for (const auto& file_name : request->file_names()) {
      std::string file_path = output_dir + "/" + file_name;
      
      std::ifstream file(file_path, std::ios::binary);
      if (!file.is_open()) {
        std::cerr << "[CSA] Failed to open file for download: " << file_path << std::endl;
        continue;
      }
      
      // Get file size
      file.seekg(0, std::ios::end);
      uint64_t total_size = file.tellg();
      file.seekg(0, std::ios::beg);
      
      uint64_t offset = 0;
      while (file) {
        file.read(buffer.data(), kChunkSize);
        size_t bytes_read = file.gcount();
        if (bytes_read == 0) break;
        
        csa::FileChunk chunk;
        chunk.set_job_id(job_id);
        chunk.set_file_name(file_name);
        chunk.set_offset(offset);
        chunk.set_data(buffer.data(), bytes_read);
        chunk.set_is_last_chunk(!file || file.peek() == EOF);
        chunk.set_total_size(total_size);
        
        if (!writer->Write(chunk)) {
          std::cerr << "[CSA] Failed to write chunk for file: " << file_name << std::endl;
          break;
        }
        
        offset += bytes_read;
      }
      
      std::cout << "[CSA] Sent file: " << file_name 
                << " (" << total_size << " bytes)" << std::endl;
    }
    
    return grpc::Status::OK;
  }

  // Get list of output files
  grpc::Status GetOutputFiles(grpc::ServerContext* context,
                              const csa::FileListRequest* request,
                              csa::FileListResponse* response) override {
    std::string job_id = request->job_id();
    std::string job_dir = JobFileManager::Instance().GetJobDir(job_id);
    std::string output_dir = job_dir + "/output";
    
    response->set_code(0);
    
    if (!fs::exists(output_dir)) {
      return grpc::Status::OK;
    }
    
    for (const auto& entry : fs::directory_iterator(output_dir)) {
      if (entry.is_regular_file()) {
        std::string file_name = entry.path().filename().string();
        response->add_file_names(file_name);
        response->add_file_sizes(entry.file_size());
      }
    }
    
    std::cout << "[CSA] GetOutputFiles for job " << job_id 
              << ": " << response->file_names_size() << " files" << std::endl;
    
    return grpc::Status::OK;
  }

  // Cleanup job files
  grpc::Status CleanupJob(grpc::ServerContext* context,
                          const csa::FileListRequest* request,
                          csa::FileTransferResponse* response) override {
    std::string job_id = request->job_id();
    JobFileManager::Instance().CleanupJob(job_id);
    
    response->set_code(0);
    response->set_message("Cleanup successful");
    return grpc::Status::OK;
  }
};

int main() {
  std::string server_address(compaction_service_options.csa_address);
  CSAImpl service;
  
  // Max gRPC message size (16MB to handle large SST file chunks)
  static const int kMaxGrpcMessageSize = 16 * 1024 * 1024;
  
  grpc::ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  
  // Set max message size for receiving and sending
  builder.SetMaxReceiveMessageSize(kMaxGrpcMessageSize);
  builder.SetMaxSendMessageSize(kMaxGrpcMessageSize);
  
  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  std::cout << "CSA Server listening on " << server_address 
            << " (max message size: " << kMaxGrpcMessageSize / 1024 / 1024 << "MB)" << std::endl;
  server->Wait();
}
