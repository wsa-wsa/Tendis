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

#include "csa.grpc.pb.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"

#ifdef HDFS
#include "plugin/hdfs/env_hdfs.h"
#endif

#include "def.h"
#include "rocksdb/statistics.h"
#include "tendisplus/storage/rocks/nfs_filesystem_simple.h"
#include "tendisplus/storage/rocks/nfs_filesystem.h"

ROCKSDB_NAMESPACE::RemoteOpenAndCompactOptions compaction_service_options;

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

// CSA Service Implementation
class CSAImpl final : public csa::CSAService::Service {
 public:
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

    // Get the shared file system configuration
    const std::string& shared_fs_uri = compaction_args->shared_fs_uri();
    const std::string& shared_fs_local_prefix = compaction_args->shared_fs_local_prefix();
    
    // Get or create a FileSystem from the cache
    rocksdb::Env* shared_env = SharedFileSystemCache::Instance().GetOrCreateEnv(
        shared_fs_uri, shared_fs_local_prefix);
    
    if (shared_env) {
      options_override.env = shared_env;
      std::cout << "Using shared FileSystem: " << shared_fs_uri << std::endl;
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
        compaction_args->name(), compaction_args->output_directory(),
        compaction_args->input(), &compaction_service_result, options_override);
    
    std::cout << "[CSA] OpenAndCompact result: " << s.ToString() 
              << ", result_size=" << compaction_service_result.size() << std::endl;
    
    compaction_reply->set_code(s.code());
    
    if (!s.ok()) {
      std::cerr << "Compaction failed: " << s.ToString() << std::endl;
      // Set result even if it fails (possibly empty)
      compaction_reply->set_result(compaction_service_result);
    } else {
      compaction_reply->set_result(std::move(compaction_service_result));
      std::cout << "Compaction completed successfully" << std::endl;
    }
    
    local_task_nums_ -= 1;
    std::cout << "Compaction finished with code: " << static_cast<int>(s.code()) << std::endl;
    return ::grpc::Status::OK;
  }
};

int main() {
  std::string server_address(compaction_service_options.csa_address);
  CSAImpl service;
  grpc::ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  std::cout << "CSA Server listening on " << server_address << std::endl;
  server->Wait();
}
