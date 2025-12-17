#pragma once
#include <string>
#include <functional>
#include "monitoring/instrumented_mutex.h"
#include "rocksdb/options.h"
#include "rocksdb/metadata.h"
#include "remote_compaction/def.h"

namespace ROCKSDB_NAMESPACE {

// Callback type for getting live file metadata from DB
// Used in network transfer mode (方案三) to get SST file metadata
using GetLiveFilesMetaDataCallback = std::function<void(std::vector<LiveFileMetaData>*)>;

class MyTestCompactionService : public CompactionService {
 public:
  MyTestCompactionService(
      std::string db_path, Options& options,
      std::shared_ptr<Statistics>& statistics,
      std::vector<std::shared_ptr<EventListener>>& listeners,
      std::vector<std::shared_ptr<TablePropertiesCollectorFactory>>
          table_properties_collector_factories,
      const std::string& shared_fs_uri = "",
      const std::string& shared_fs_local_prefix = "",
      const std::string& csa_address = "",
      RemoteCompactionMode mode = RemoteCompactionMode::kSharedStorage)
      : db_path_(std::move(db_path)),
        options_(options),
        statistics_(statistics),
        start_info_("na", "na", "na", 0, Env::TOTAL),
        wait_info_("na", "na", "na", 0, Env::TOTAL),
        listeners_(listeners),
        table_properties_collector_factories_(
            std::move(table_properties_collector_factories)),
        shared_fs_uri_(shared_fs_uri),
        shared_fs_local_prefix_(shared_fs_local_prefix),
        csa_address_(csa_address),
        mode_(mode) {}

  static const char* kClassName() { return "MyTestCompactionService"; }

  const char* Name() const override { return kClassName(); }

  CompactionServiceJobStatus StartV2(
      const CompactionServiceJobInfo& info,
      const std::string& compaction_service_input) override;

  CompactionServiceJobStatus WaitForCompleteV2(
      const CompactionServiceJobInfo& info,
      std::string* compaction_service_result) override;

  int GetCompactionNum() { return compaction_num_.load(); }

  CompactionServiceJobInfo GetCompactionInfoForStart() { return start_info_; }
  CompactionServiceJobInfo GetCompactionInfoForWait() { return wait_info_; }

  void OverrideStartStatus(CompactionServiceJobStatus s) {
    is_override_start_status_ = true;
    override_start_status_ = s;
  }

  void OverrideWaitStatus(CompactionServiceJobStatus s) {
    is_override_wait_status_ = true;
    override_wait_status_ = s;
  }

  void OverrideWaitResult(std::string str) {
    is_override_wait_result_ = true;
    override_wait_result_ = std::move(str);
  }

  void ResetOverride() {
    is_override_wait_result_ = false;
    is_override_start_status_ = false;
    is_override_wait_status_ = false;
  }

  void SetCanceled(bool canceled) { canceled_ = canceled; }

  // Set callback for getting live file metadata (方案三)
  void SetGetLiveFilesMetaDataCallback(GetLiveFilesMetaDataCallback callback) {
    get_live_files_metadata_callback_ = std::move(callback);
  }

 private:
  InstrumentedMutex mutex_;
  std::atomic_int compaction_num_{0};
  std::map<uint64_t, std::string> jobs_;
  const std::string db_path_;
  Options options_;
  std::shared_ptr<Statistics> statistics_;
  CompactionServiceJobInfo start_info_;
  CompactionServiceJobInfo wait_info_;
  bool is_override_start_status_ = false;
  CompactionServiceJobStatus override_start_status_ =
      CompactionServiceJobStatus::kFailure;
  bool is_override_wait_status_ = false;
  CompactionServiceJobStatus override_wait_status_ =
      CompactionServiceJobStatus::kFailure;
  bool is_override_wait_result_ = false;
  std::string override_wait_result_;
  std::vector<std::shared_ptr<EventListener>> listeners_;
  std::vector<std::shared_ptr<TablePropertiesCollectorFactory>>
      table_properties_collector_factories_;
  std::atomic_bool canceled_{false};
  // Shared FileSystem configuration
  std::string shared_fs_uri_;
  std::string shared_fs_local_prefix_;

  // server_address
  std::string csa_address_;
  
  // Remote compaction mode
  RemoteCompactionMode mode_;
  
  // Callback for getting live file metadata (方案三)
  GetLiveFilesMetaDataCallback get_live_files_metadata_callback_;
  
  // Helper methods for network transfer mode
  // input_files: pairs of {local_path, relative_path}
  Status UploadInputFiles(const std::string& job_id,
                          const std::vector<std::pair<std::string, std::string>>& input_files,
                          uint64_t numeric_job_id);
  Status DownloadOutputFiles(const std::string& job_id,
                             const std::string& output_dir);
  Status CleanupRemoteJob(const std::string& job_id);
};
}  // namespace ROCKSDB_NAMESPACE