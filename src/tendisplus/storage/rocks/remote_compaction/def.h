#pragma once
#include <atomic>
#include <string>
#include "rocksdb/db.h"
#include "rocksdb/options.h"
namespace ROCKSDB_NAMESPACE {

// Remote compaction mode
enum class RemoteCompactionMode {
  kSharedStorage = 0,   // Use shared storage (NFS/HDFS/S3) - both sides access same files
  kNetworkTransfer = 1  // Transfer SST files over network - no shared storage needed
};

struct RemoteOpenAndCompactOptions: public OpenAndCompactOptions {
#ifdef HDFS
  std::string hdfs_address = "hdfs://10.218.107.48:9000/";

  std::string csa_address = "10.218.107.48:8010";

  std::string pro_cp_address = "10.218.107.48:8020";

  int32_t check_time_interval = 1;

  int64_t csa_max_concurrent_tasks = 5;

  uint64_t max_accumulation_in_procp = 5;

  uint64_t max_reschedule = 5;
  
  RemoteCompactionMode mode = RemoteCompactionMode::kSharedStorage;
#else
  // Allows cancellation of an in-progress compaction.
  std::atomic<bool>* canceled = nullptr;

  std::string csa_address = "localhost:8010";

  // Shared FileSystem configuration (supports NFS, HDFS, S3, etc.)
  // URI format: "nfs://host/path", "hdfs://host:port/path", "s3://bucket/prefix"
  std::string shared_fs_uri;
  std::string shared_fs_local_prefix;

  int64_t csa_max_concurrent_tasks = 5;
  
  // Remote compaction mode
  // kSharedStorage: Use shared storage (NFS/HDFS/S3) - both sides access same files
  // kNetworkTransfer: Transfer SST files over network - no shared storage needed
  RemoteCompactionMode mode = RemoteCompactionMode::kSharedStorage;
  
  // Network transfer mode settings
  std::string csa_work_dir = "/tmp/csa_work";  // CSA server working directory for temp files
  size_t file_transfer_chunk_size = 4 * 1024 * 1024;  // 4MB chunks for file transfer
#endif
};
}  // namespace ROCKSDB_NAMESPACE
