#pragma once
#include <atomic>
#include <string>
#include "rocksdb/db.h"
#include "rocksdb/options.h"
namespace ROCKSDB_NAMESPACE {
struct RemoteOpenAndCompactOptions: public OpenAndCompactOptions {
#ifdef HDFS
  std::string hdfs_address = "hdfs://10.218.107.48:9000/";

  std::string csa_address = "10.218.107.48:8010";

  std::string pro_cp_address = "10.218.107.48:8020";

  int32_t check_time_interval = 1;

  int64_t csa_max_concurrent_tasks = 5;

  uint64_t max_accumulation_in_procp = 5;

  uint64_t max_reschedule = 5;
#else
  //  std::string hdfs_address = "hdfs://10.218.106.144:9000/";

  //   std::string csa_address = "10.218.107.48:8010";

  // std::string pro_cp_address = "10.218.107.48:8020";
  // Allows cancellation of an in-progress compaction.
  // NFS 共享路径配置

  std::atomic<bool>* canceled = nullptr;

  // std::string hdfs_address = "hdfs://10.218.106.144:9000/";

  std::string csa_address = "localhost:8010";

  // std::string worker_db_path = "/mnt/rocksdb/db/0";

  // Shared FileSystem 配置（支持 NFS、HDFS、S3 等）
  // URI 格式: "nfs://host/path", "hdfs://host:port/path", "s3://bucket/prefix"
  std::string shared_fs_uri;
  std::string shared_fs_local_prefix;

  int64_t csa_max_concurrent_tasks = 5;
//   std::string shared_db_path = "/shared/rocksdb/db/0";  // Primary 使用
//   std::string worker_db_path = "/mnt/rocksdb/db/0";     // Worker 使用

//   // Remote Compaction 服务地址（本地测试）
//   std::string csa_address = "localhost:8010";
//   std::string pro_cp_address = "localhost:8020";

//   int32_t check_time_interval = 1;

//   int64_t csa_max_concurrent_tasks = 5;

//   uint64_t max_accumulation_in_procp = 5;

//   uint64_t max_reschedule = 5;
#endif
};
}  // namespace test
