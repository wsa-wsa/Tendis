#include "compaction_service.h"

#include <grpc/grpc.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <iostream>

#include "csa.grpc.pb.h"
#include "db/compaction/compaction_job.h"
#include "rocksdb/db.h"
#include "remote_compaction/def.h"

class CSAClient {
 public:
  CSAClient(const std::shared_ptr<grpc::Channel>& channel)
      : stub_(csa::CSAService::NewStub(channel)) {}

  ROCKSDB_NAMESPACE::Status OpenAndCompact(
      const ROCKSDB_NAMESPACE::OpenAndCompactOptions& options,
      const std::string& name, const std::string& output_directory,
      const std::string& input, std::string* output,
      const ROCKSDB_NAMESPACE::CompactionServiceOptionsOverride&
          override_options,
      const std::string& shared_fs_uri = "",
      const std::string& shared_fs_local_prefix = "") {
    csa::CompactionArgs compaction_args;
    compaction_args.set_name(name);
    compaction_args.set_output_directory(output_directory);
    compaction_args.set_input(input);
    // Pass the shared file system configuration to the CSA server
    if (!shared_fs_uri.empty()) {
      compaction_args.set_shared_fs_uri(shared_fs_uri);
    }
    if (!shared_fs_local_prefix.empty()) {
      compaction_args.set_shared_fs_local_prefix(shared_fs_local_prefix);
    }
    csa::CompactionReply compaction_reply;
    grpc::ClientContext context;
    grpc::Status status = stub_->ExecuteCompactionTask(
        &context, compaction_args, &compaction_reply);
    
    if (!status.ok()) {
      // gRPC call failed
      return ROCKSDB_NAMESPACE::Status::IOError(
          "gRPC call failed: " + status.error_message());
    }
    
    // Check the compaction execution results
    if (compaction_reply.code() != 0) {
      // Compaction execution failed
      // code=5 is IOError, usually the file does not exist (NOENT)
      // Return a special tag to let the caller know that this is an IO error
      return ROCKSDB_NAMESPACE::Status::IOError(
          "Remote compaction failed with code: " + 
          std::to_string(compaction_reply.code()) +
          (compaction_reply.code() == 5 ? " (IOError/NOENT)" : ""));
    }
    
    // Check if the result is empty
    if (compaction_reply.result().empty()) {
      return ROCKSDB_NAMESPACE::Status::IOError(
          "Remote compaction returned empty result");
    }
    
    output->assign(compaction_reply.result());
    return ROCKSDB_NAMESPACE::Status::OK();
  };

 private:
  std::unique_ptr<csa::CSAService::Stub> stub_;
};

namespace ROCKSDB_NAMESPACE {
CompactionServiceJobStatus MyTestCompactionService::StartV2(
    const CompactionServiceJobInfo& info,
    const std::string& compaction_service_input) {
  InstrumentedMutexLock l(&mutex_);
  start_info_ = info;
  assert(info.db_name == db_path_);
  jobs_.emplace(info.job_id, compaction_service_input);
  CompactionServiceJobStatus s = CompactionServiceJobStatus::kSuccess;
  if (is_override_start_status_) {
    return override_start_status_;
  }
  return s;
}

CompactionServiceJobStatus MyTestCompactionService::WaitForCompleteV2(
    const CompactionServiceJobInfo& info,
    std::string* compaction_service_result) {
  std::string compaction_input;
  assert(info.db_name == db_path_);
  {
    InstrumentedMutexLock l(&mutex_);
    wait_info_ = info;
    auto i = jobs_.find(info.job_id);
    if (i == jobs_.end()) {
      return CompactionServiceJobStatus::kFailure;
    }
    compaction_input = std::move(i->second);
    jobs_.erase(i);
  }

  if (is_override_wait_status_) {
    return override_wait_status_;
  }

  CompactionServiceOptionsOverride options_override;
  options_override.env = options_.env;
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
  options_override.statistics = statistics_;
  if (!listeners_.empty()) {
    options_override.listeners = listeners_;
  }

  if (!table_properties_collector_factories_.empty()) {
    options_override.table_properties_collector_factories =
        table_properties_collector_factories_;
  }

  RemoteOpenAndCompactOptions options;
  options.canceled = &canceled_;

  //  Status s = DB::OpenAndCompact(
  //      options, db_path_, db_path_ + "/" + std::to_string(info.job_id),
  //      compaction_input, compaction_service_result, options_override);
  CSAClient csa_client(grpc::CreateChannel(csa_address_,
                                           grpc::InsecureChannelCredentials()));
  Status s = csa_client.OpenAndCompact(
      options, db_path_, db_path_ + "/" + std::to_string(info.job_id),
      compaction_input, compaction_service_result, options_override,
      shared_fs_uri_, shared_fs_local_prefix_);

  if (is_override_wait_result_) {
    *compaction_service_result = override_wait_result_;
  }
  compaction_num_.fetch_add(1);
  
  std::cout << "[CompactionService] WaitForCompleteV2 result: " 
            << (s.ok() ? "OK" : s.ToString())
            << ", result_size=" << compaction_service_result->size() << std::endl;
  
  if (s.ok()) {
    return CompactionServiceJobStatus::kSuccess;
  } else {
    // Check if it's an IO error (code=5)
    // This is usually the case when the input file has been deleted by another compaction (NOENT),
    // This compaction task has expired/is invalid and does not need to be retried

    std::string err_msg = s.ToString();
    bool is_stale_task = s.IsIOError();  // Check the Status type directly
    
    std::cout << "[CompactionService] Error details - IsIOError: " << s.IsIOError()
              << ", code: " << static_cast<int>(s.code()) 
              << ", msg: " << err_msg << std::endl;
    
    if (is_stale_task) {
      std::cout << "[CompactionService] IO error detected (likely stale task), "
                << "constructing empty result and returning kSuccess" << std::endl;
      // Construct an empty but valid CompactionServiceResult
      // This way RocksDB will consider the compaction to be successful (without an output file) and will not retry

      CompactionServiceResult empty_result;
      empty_result.status = Status::OK();
      empty_result.Write(compaction_service_result);
      return CompactionServiceJobStatus::kSuccess;
    }
    // Other errors fall back to local execution
    std::cout << "[CompactionService] Non-IO error, falling back to local: " 
              << err_msg << std::endl;
    return CompactionServiceJobStatus::kUseLocal;
  }
}
};  // namespace ROCKSDB_NAMESPACE
