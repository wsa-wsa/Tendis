#include "compaction_service.h"

#include <grpc/grpc.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <fstream>
#include <iostream>
#include <filesystem>
#include <thread>
#include <chrono>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <set>

#include "csa.grpc.pb.h"
#include "db/compaction/compaction_job.h"
#include "db/dbformat.h"
#include "rocksdb/db.h"
#include "remote_compaction/def.h"

namespace fs = std::filesystem;

// File transfer chunk size (1MB for better flow control)
static const size_t kFileTransferChunkSize = 1 * 1024 * 1024;

// Max gRPC message size (16MB to be safe)
static const int kMaxGrpcMessageSize = 16 * 1024 * 1024;

// Upload throttling configuration
static const size_t kMaxConcurrentUploads = 2;        // Max concurrent file uploads
static const size_t kUploadDelayBetweenChunksMs = 10; // Small delay between chunks for flow control

// Global upload throttling
static std::atomic<int> g_active_uploads{0};
static std::mutex g_upload_mutex;
static std::condition_variable g_upload_cv;

class CSAClient {
 public:
  CSAClient(const std::shared_ptr<grpc::Channel>& channel)
      : stub_(csa::CSAService::NewStub(channel)) {}

  // Shared storage mode: execute compaction using shared filesystem
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
    compaction_args.set_use_network_transfer(false);
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
      return ROCKSDB_NAMESPACE::Status::IOError(
          "gRPC call failed: " + status.error_message());
    }
    
    if (compaction_reply.code() != 0) {
      return ROCKSDB_NAMESPACE::Status::IOError(
          "Remote compaction failed with code: " + 
          std::to_string(compaction_reply.code()) +
          (compaction_reply.code() == 5 ? " (IOError/NOENT)" : ""));
    }
    
    if (compaction_reply.result().empty()) {
      return ROCKSDB_NAMESPACE::Status::IOError(
          "Remote compaction returned empty result");
    }
    
    output->assign(compaction_reply.result());
    return ROCKSDB_NAMESPACE::Status::OK();
  }

  // Network transfer mode: execute compaction with file transfer
  // file_metadata: contains complete SST file metadata for building MANIFEST on CSA side
  ROCKSDB_NAMESPACE::Status OpenAndCompactWithTransfer(
      const std::string& name, const std::string& output_directory,
      const std::string& input, std::string* output,
      const std::string& job_id,
      const std::string& original_db_path,
      const std::vector<csa::SstFileMetadata>& file_metadata) {
    csa::CompactionArgs compaction_args;
    compaction_args.set_name(name);
    compaction_args.set_output_directory(output_directory);
    compaction_args.set_input(input);
    compaction_args.set_use_network_transfer(true);
    compaction_args.set_job_id(job_id);
    // Pass original database path for symlink mode (fallback)
    compaction_args.set_original_db_path(original_db_path);
    
    // Pass file metadata for building MANIFEST on CSA side (方案三)
    for (const auto& meta : file_metadata) {
      *compaction_args.add_input_file_metadata() = meta;
    }
    
    std::cout << "[CSAClient] Sending " << file_metadata.size() 
              << " file metadata entries to CSA" << std::endl;
    
    csa::CompactionReply compaction_reply;
    grpc::ClientContext context;
    grpc::Status status = stub_->ExecuteCompactionTask(
        &context, compaction_args, &compaction_reply);
    
    if (!status.ok()) {
      return ROCKSDB_NAMESPACE::Status::IOError(
          "gRPC call failed: " + status.error_message());
    }
    
    if (compaction_reply.code() != 0) {
      return ROCKSDB_NAMESPACE::Status::IOError(
          "Remote compaction failed with code: " + 
          std::to_string(compaction_reply.code()) +
          (compaction_reply.code() == 5 ? " (IOError/NOENT)" : ""));
    }
    
    if (compaction_reply.result().empty()) {
      return ROCKSDB_NAMESPACE::Status::IOError(
          "Remote compaction returned empty result");
    }
    
    output->assign(compaction_reply.result());
    return ROCKSDB_NAMESPACE::Status::OK();
  }

  // Upload a single file to CSA server with throttling
  ROCKSDB_NAMESPACE::Status UploadFile(const std::string& job_id,
                                       const std::string& local_path,
                                       const std::string& remote_name) {
    grpc::ClientContext context;
    csa::FileTransferResponse response;
    
    // Set timeout for the upload (10 minutes for large files)
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(600));
    
    std::ifstream file(local_path, std::ios::binary);
    if (!file.is_open()) {
      return ROCKSDB_NAMESPACE::Status::IOError(
          "Failed to open file for upload: " + local_path);
    }
    
    // Get file size
    file.seekg(0, std::ios::end);
    uint64_t total_size = file.tellg();
    file.seekg(0, std::ios::beg);
    
    std::cout << "[CSAClient] Starting upload: " << remote_name 
              << " (" << (total_size / 1024 / 1024) << " MB)" << std::endl;
    
    auto writer = stub_->UploadFile(&context, &response);
    if (!writer) {
      return ROCKSDB_NAMESPACE::Status::IOError(
          "Failed to create upload stream - CSA server may not be running");
    }
    
    std::vector<char> buffer(kFileTransferChunkSize);
    uint64_t offset = 0;
    uint64_t last_progress_report = 0;
    
    while (file) {
      file.read(buffer.data(), kFileTransferChunkSize);
      size_t bytes_read = file.gcount();
      if (bytes_read == 0) break;
      
      csa::FileChunk chunk;
      chunk.set_job_id(job_id);
      chunk.set_file_name(remote_name);
      chunk.set_offset(offset);
      chunk.set_data(buffer.data(), bytes_read);
      chunk.set_is_last_chunk(!file || file.peek() == EOF);
      chunk.set_total_size(total_size);
      
      if (!writer->Write(chunk)) {
        // Get more details about the failure
        writer->WritesDone();
        grpc::Status finish_status = writer->Finish();
        std::string error_msg = "Failed to write chunk for file: " + remote_name;
        if (!finish_status.ok()) {
          error_msg += " (gRPC error: " + finish_status.error_message() + 
                       ", code: " + std::to_string(static_cast<int>(finish_status.error_code())) + ")";
        }
        std::cerr << "[CSAClient] " << error_msg << std::endl;
        return ROCKSDB_NAMESPACE::Status::IOError(error_msg);
      }
      
      offset += bytes_read;
      
      // Progress report every 10MB
      if (offset - last_progress_report >= 10 * 1024 * 1024) {
        int progress = static_cast<int>(offset * 100 / total_size);
        std::cout << "[CSAClient] Upload progress: " << remote_name 
                  << " " << progress << "% (" << (offset / 1024 / 1024) << "/" 
                  << (total_size / 1024 / 1024) << " MB)" << std::endl;
        last_progress_report = offset;
      }
      
      // Small delay for flow control (throttling)
      if (kUploadDelayBetweenChunksMs > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(kUploadDelayBetweenChunksMs));
      }
    }
    
    writer->WritesDone();
    grpc::Status status = writer->Finish();
    
    if (!status.ok()) {
      return ROCKSDB_NAMESPACE::Status::IOError(
          "Upload failed: " + status.error_message());
    }
    
    if (response.code() != 0) {
      return ROCKSDB_NAMESPACE::Status::IOError(
          "Upload failed: " + response.message());
    }
    
    std::cout << "[CSAClient] Uploaded file: " << remote_name 
              << " (" << total_size << " bytes)" << std::endl;
    return ROCKSDB_NAMESPACE::Status::OK();
  }

  // Upload file content from memory (for snapshot data)
  ROCKSDB_NAMESPACE::Status UploadFileFromMemory(const std::string& job_id,
                                                  const std::string& remote_name,
                                                  const std::string& content) {
    grpc::ClientContext context;
    csa::FileTransferResponse response;
    
    // Set timeout
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(60));
    
    auto writer = stub_->UploadFile(&context, &response);
    if (!writer) {
      return ROCKSDB_NAMESPACE::Status::IOError(
          "Failed to create upload stream for memory upload");
    }
    
    uint64_t total_size = content.size();
    uint64_t offset = 0;
    
    // Upload in chunks
    while (offset < total_size) {
      size_t chunk_size = std::min(static_cast<size_t>(kFileTransferChunkSize), 
                                   static_cast<size_t>(total_size - offset));
      
      csa::FileChunk chunk;
      chunk.set_job_id(job_id);
      chunk.set_file_name(remote_name);
      chunk.set_offset(offset);
      chunk.set_data(content.data() + offset, chunk_size);
      chunk.set_is_last_chunk(offset + chunk_size >= total_size);
      chunk.set_total_size(total_size);
      
      if (!writer->Write(chunk)) {
        writer->WritesDone();
        grpc::Status finish_status = writer->Finish();
        return ROCKSDB_NAMESPACE::Status::IOError(
            "Failed to write chunk for memory upload: " + remote_name);
      }
      
      offset += chunk_size;
    }
    
    writer->WritesDone();
    grpc::Status status = writer->Finish();
    
    if (!status.ok()) {
      return ROCKSDB_NAMESPACE::Status::IOError(
          "Memory upload failed: " + status.error_message());
    }
    
    if (response.code() != 0) {
      return ROCKSDB_NAMESPACE::Status::IOError(
          "Memory upload failed: " + response.message());
    }
    
    return ROCKSDB_NAMESPACE::Status::OK();
  }

  // Download files from CSA server
  ROCKSDB_NAMESPACE::Status DownloadFiles(const std::string& job_id,
                                          const std::vector<std::string>& file_names,
                                          const std::string& output_dir) {
    grpc::ClientContext context;
    csa::FileTransferRequest request;
    request.set_job_id(job_id);
    for (const auto& name : file_names) {
      request.add_file_names(name);
    }
    
    auto reader = stub_->DownloadFile(&context, request);
    
    std::unordered_map<std::string, std::ofstream> open_files;
    csa::FileChunk chunk;
    
    while (reader->Read(&chunk)) {
      std::string file_path = output_dir + "/" + chunk.file_name();
      
      // Create parent directory if needed
      fs::path parent = fs::path(file_path).parent_path();
      if (!parent.empty() && !fs::exists(parent)) {
        fs::create_directories(parent);
      }
      
      // Open file if not already open
      if (open_files.find(chunk.file_name()) == open_files.end()) {
        open_files[chunk.file_name()].open(file_path, std::ios::binary);
        if (!open_files[chunk.file_name()].is_open()) {
          return ROCKSDB_NAMESPACE::Status::IOError(
              "Failed to create file: " + file_path);
        }
      }
      
      // Write data
      open_files[chunk.file_name()].write(chunk.data().data(), chunk.data().size());
      
      if (chunk.is_last_chunk()) {
        open_files[chunk.file_name()].close();
        std::cout << "[CSAClient] Downloaded file: " << chunk.file_name() 
                  << " (" << chunk.total_size() << " bytes)" << std::endl;
      }
    }
    
    grpc::Status status = reader->Finish();
    if (!status.ok()) {
      return ROCKSDB_NAMESPACE::Status::IOError(
          "Download failed: " + status.error_message());
    }
    
    return ROCKSDB_NAMESPACE::Status::OK();
  }

  // Get list of output files after compaction
  ROCKSDB_NAMESPACE::Status GetOutputFiles(const std::string& job_id,
                                           std::vector<std::string>* file_names,
                                           std::vector<uint64_t>* file_sizes) {
    grpc::ClientContext context;
    csa::FileListRequest request;
    request.set_job_id(job_id);
    
    csa::FileListResponse response;
    grpc::Status status = stub_->GetOutputFiles(&context, request, &response);
    
    if (!status.ok()) {
      return ROCKSDB_NAMESPACE::Status::IOError(
          "GetOutputFiles failed: " + status.error_message());
    }
    
    if (response.code() != 0) {
      return ROCKSDB_NAMESPACE::Status::IOError("GetOutputFiles failed");
    }
    
    for (const auto& name : response.file_names()) {
      file_names->push_back(name);
    }
    for (const auto& size : response.file_sizes()) {
      file_sizes->push_back(size);
    }
    
    return ROCKSDB_NAMESPACE::Status::OK();
  }

  // Cleanup job files on CSA server
  ROCKSDB_NAMESPACE::Status CleanupJob(const std::string& job_id) {
    grpc::ClientContext context;
    csa::FileListRequest request;
    request.set_job_id(job_id);
    
    csa::FileTransferResponse response;
    grpc::Status status = stub_->CleanupJob(&context, request, &response);
    
    if (!status.ok()) {
      std::cerr << "[CSAClient] CleanupJob failed: " << status.error_message() << std::endl;
    }
    
    return ROCKSDB_NAMESPACE::Status::OK();
  }

 private:
  std::unique_ptr<csa::CSAService::Stub> stub_;
};

namespace ROCKSDB_NAMESPACE {

// Helper function to build file metadata for network transfer mode (方案三)
// Converts LiveFileMetaData to csa::SstFileMetadata for gRPC transfer
static std::vector<csa::SstFileMetadata> BuildFileMetadataForTransfer(
    const std::vector<LiveFileMetaData>& live_files,
    const std::vector<std::string>& input_file_names) {
  std::vector<csa::SstFileMetadata> result;
  
  // Create a set of input file names for quick lookup
  std::set<std::string> input_set;
  for (const auto& name : input_file_names) {
    // Extract just the filename without path
    std::string filename = fs::path(name).filename().string();
    input_set.insert(filename);
  }
  
  std::cout << "[CompactionService] Building metadata for " << input_set.size() 
            << " input files from " << live_files.size() << " live files" << std::endl;
  
  for (const auto& file : live_files) {
    // Check if this file is in the input set
    std::string filename = fs::path(file.name).filename().string();
    if (input_set.find(filename) == input_set.end()) {
      continue;
    }
    
    csa::SstFileMetadata meta;
    meta.set_file_name(filename);
    
    // Extract file number from filename (e.g., "000001.sst" -> 1)
    uint64_t file_number = 0;
    std::string num_str;
    for (char c : filename) {
      if (c >= '0' && c <= '9') {
        num_str += c;
      } else if (c == '.') {
        break;
      }
    }
    if (!num_str.empty()) {
      file_number = std::stoull(num_str);
    }
    meta.set_file_number(file_number);
    
    meta.set_level(file.level);
    meta.set_file_size(file.size);
    
    // Construct InternalKey from user key and sequence numbers
    // InternalKey format: user_key + (seqno << 8 | type)
    // For smallest key: use smallest_seqno and kTypeValue (0x01)
    // For largest key: use largest_seqno and kTypeValue (0x01)
    // Note: GetLiveFilesMetaData only provides smallestkey/largestkey (user keys),
    // not smallest/largest (internal keys), so we need to construct them.
    std::string smallest_internal_key;
    AppendInternalKey(&smallest_internal_key, 
                      ParsedInternalKey(file.smallestkey, file.smallest_seqno, 
                                        kTypeValue));
    
    std::string largest_internal_key;
    AppendInternalKey(&largest_internal_key,
                      ParsedInternalKey(file.largestkey, file.largest_seqno,
                                        kTypeValue));
    
    meta.set_smallest_key(smallest_internal_key);
    meta.set_largest_key(largest_internal_key);
    
    meta.set_smallest_seqno(file.smallest_seqno);
    meta.set_largest_seqno(file.largest_seqno);
    
    meta.set_num_entries(file.num_entries);
    meta.set_num_deletions(file.num_deletions);
    meta.set_oldest_blob_file_number(file.oldest_blob_file_number);
    meta.set_oldest_ancester_time(file.oldest_ancester_time);
    meta.set_file_creation_time(file.file_creation_time);
    meta.set_epoch_number(file.epoch_number);
    meta.set_marked_for_compaction(file.being_compacted);
    
    result.push_back(std::move(meta));
    
    std::cout << "[CompactionService] Added metadata for file: " << filename
              << " (level=" << file.level << ", size=" << file.size 
              << ", seqno=" << file.smallest_seqno << "-" << file.largest_seqno << ")"
              << std::endl;
  }
  
  std::cout << "[CompactionService] Built metadata for " << result.size() 
            << " files" << std::endl;
  
  return result;
}

// Helper function to extract input file paths from compaction input
// Returns pairs of {local_path, relative_path} where relative_path preserves directory structure
static std::vector<std::pair<std::string, std::string>> ExtractInputFiles(
    const std::string& compaction_input,
    const std::string& db_path) {
  std::vector<std::pair<std::string, std::string>> files;  // {local_path, relative_path}
  
  // Parse CompactionServiceInput to get input file names
  CompactionServiceInput input;
  Status s = CompactionServiceInput::Read(compaction_input, &input);
  if (!s.ok()) {
    std::cerr << "[CompactionService] Failed to parse compaction input: " 
              << s.ToString() << std::endl;
    return files;
  }
  
  std::cout << "[CompactionService] Found " << input.input_files.size() 
            << " input files" << std::endl;
  
  // Collect all input files with their relative paths
  for (const auto& file : input.input_files) {
    // file is the relative file path (e.g., "000001.sst" or "0/000001.sst")
    // Construct full path by prepending db_path
    std::string file_path = db_path + "/" + file;
    // Keep the relative path for remote storage to preserve directory structure
    files.push_back({file_path, file});
    std::cout << "[CompactionService] Input file: " << file_path 
              << " (relative: " << file << ")" << std::endl;
  }
  
  return files;
}

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
  
  // In network transfer mode, we no longer need to snapshot MANIFEST files
  // because CSA server will use RepairDB to rebuild MANIFEST from SST files.
  // This simplifies the logic and avoids MANIFEST version mismatch issues.
  if (mode_ == RemoteCompactionMode::kNetworkTransfer) {
    std::cout << "[CompactionService] StartV2: Network transfer mode, job_id=" 
              << info.job_id << std::endl;
  }
  
  return s;
}

// Upload input files for network transfer mode with throttling and scheduling
// input_files: pairs of {local_path, relative_path}
// Note: We only upload SST files. CSA server will use RepairDB to rebuild MANIFEST.
Status MyTestCompactionService::UploadInputFiles(
    const std::string& job_id,
    const std::vector<std::pair<std::string, std::string>>& input_files,
    uint64_t /* numeric_job_id */) {
  
  // Wait for upload slot (throttling)
  {
    std::unique_lock<std::mutex> lock(g_upload_mutex);
    g_upload_cv.wait(lock, []{ 
      return g_active_uploads.load() < static_cast<int>(kMaxConcurrentUploads); 
    });
    g_active_uploads.fetch_add(1);
  }
  
  // RAII guard to release upload slot
  struct UploadSlotGuard {
    ~UploadSlotGuard() {
      g_active_uploads.fetch_sub(1);
      g_upload_cv.notify_one();
    }
  } slot_guard;
  
  std::cout << "[CompactionService] Creating gRPC channel to: " << csa_address_ 
            << " (active uploads: " << g_active_uploads.load() << ")" << std::endl;
  
  // Create channel with increased message size limit
  grpc::ChannelArguments channel_args;
  channel_args.SetMaxReceiveMessageSize(kMaxGrpcMessageSize);
  channel_args.SetMaxSendMessageSize(kMaxGrpcMessageSize);
  
  auto channel = grpc::CreateCustomChannel(
      csa_address_, 
      grpc::InsecureChannelCredentials(),
      channel_args);
  
  // Wait for channel to be ready (with timeout)
  auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(10);
  bool connected = channel->WaitForConnected(deadline);
  if (!connected) {
    std::cerr << "[CompactionService] Failed to connect to CSA server at: " 
              << csa_address_ << std::endl;
    return Status::IOError("Failed to connect to CSA server: " + csa_address_);
  }
  
  std::cout << "[CompactionService] Connected to CSA server, uploading " 
            << input_files.size() << " SST files (CSA will use RepairDB to rebuild MANIFEST)" 
            << std::endl;
  
  CSAClient csa_client(channel);
  
  // Calculate total size for progress reporting
  uint64_t total_bytes = 0;
  for (const auto& [local_path, relative_path] : input_files) {
    try {
      total_bytes += fs::file_size(local_path);
    } catch (...) {
      // Ignore errors
    }
  }
  std::cout << "[CompactionService] Total SST upload size: " 
            << (total_bytes / 1024 / 1024) << " MB" << std::endl;
  
  // Upload SST files with their relative paths preserved
  for (size_t i = 0; i < input_files.size(); ++i) {
    const auto& [local_path, relative_path] = input_files[i];
    
    std::cout << "[CompactionService] Uploading SST file " << (i + 1) << "/" 
              << input_files.size() << ": " << relative_path << std::endl;
    
    // Use relative_path to preserve directory structure on remote
    Status s = csa_client.UploadFile(job_id, local_path, relative_path);
    if (!s.ok()) {
      return s;
    }
  }
  
  std::cout << "[CompactionService] All files uploaded for job " << job_id << std::endl;
  return Status::OK();
}

// Download output files for network transfer mode
Status MyTestCompactionService::DownloadOutputFiles(
    const std::string& job_id,
    const std::string& output_dir) {
  // Create channel with increased message size limit
  grpc::ChannelArguments channel_args;
  channel_args.SetMaxReceiveMessageSize(kMaxGrpcMessageSize);
  channel_args.SetMaxSendMessageSize(kMaxGrpcMessageSize);
  
  auto channel = grpc::CreateCustomChannel(
      csa_address_,
      grpc::InsecureChannelCredentials(),
      channel_args);
  
  CSAClient csa_client(channel);
  
  // Get list of output files
  std::vector<std::string> file_names;
  std::vector<uint64_t> file_sizes;
  Status s = csa_client.GetOutputFiles(job_id, &file_names, &file_sizes);
  if (!s.ok()) {
    return s;
  }
  
  if (file_names.empty()) {
    std::cout << "[CompactionService] No output files to download" << std::endl;
    return Status::OK();
  }
  
  // Download all output files
  return csa_client.DownloadFiles(job_id, file_names, output_dir);
}

// Cleanup remote job files
Status MyTestCompactionService::CleanupRemoteJob(const std::string& job_id) {
  grpc::ChannelArguments channel_args;
  channel_args.SetMaxReceiveMessageSize(kMaxGrpcMessageSize);
  channel_args.SetMaxSendMessageSize(kMaxGrpcMessageSize);
  
  auto channel = grpc::CreateCustomChannel(
      csa_address_,
      grpc::InsecureChannelCredentials(),
      channel_args);
  
  CSAClient csa_client(channel);
  return csa_client.CleanupJob(job_id);
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

  Status s;
  std::string job_id = std::to_string(info.job_id);
  std::string output_dir = db_path_ + "/" + job_id;
  
  // Check if CSA address is configured
  if (csa_address_.empty()) {
    std::cerr << "[CompactionService] CSA address not configured, falling back to local compaction" << std::endl;
    return CompactionServiceJobStatus::kUseLocal;
  }
  
  // Create channel with increased message size limit
  grpc::ChannelArguments channel_args;
  channel_args.SetMaxReceiveMessageSize(kMaxGrpcMessageSize);
  channel_args.SetMaxSendMessageSize(kMaxGrpcMessageSize);
  
  auto channel = grpc::CreateCustomChannel(
      csa_address_,
      grpc::InsecureChannelCredentials(),
      channel_args);
  
  CSAClient csa_client(channel);

  if (mode_ == RemoteCompactionMode::kNetworkTransfer) {
    // Network transfer mode
    std::cout << "[CompactionService] Using network transfer mode for job " << job_id << std::endl;
    
    // Step 1: Extract and upload input files (with relative paths preserved)
    std::vector<std::pair<std::string, std::string>> input_files = ExtractInputFiles(compaction_input, db_path_);
    if (!input_files.empty()) {
      s = UploadInputFiles(job_id, input_files, info.job_id);
      if (!s.ok()) {
        std::cerr << "[CompactionService] Failed to upload input files: " << s.ToString() << std::endl;
        CleanupRemoteJob(job_id);
        return CompactionServiceJobStatus::kUseLocal;
      }
    }
    
    // Step 2: Get file metadata for 方案三 (if callback is set)
    std::vector<csa::SstFileMetadata> file_metadata;
    if (get_live_files_metadata_callback_) {
      std::vector<LiveFileMetaData> live_files;
      get_live_files_metadata_callback_(&live_files);
      
      // Extract input file names for filtering
      std::vector<std::string> input_file_names;
      for (const auto& [local_path, relative_path] : input_files) {
        input_file_names.push_back(relative_path);
      }
      
      file_metadata = BuildFileMetadataForTransfer(live_files, input_file_names);
      std::cout << "[CompactionService] Built " << file_metadata.size() 
                << " file metadata entries for CSA" << std::endl;
    } else {
      std::cout << "[CompactionService] No metadata callback set, CSA will use fallback mode" << std::endl;
    }
    
    // Step 3: Execute compaction on remote CSA
    // Pass db_path_ as original_db_path (fallback for symlink mode)
    // Pass file_metadata for 方案三 (preferred method)
    s = csa_client.OpenAndCompactWithTransfer(
        db_path_, output_dir, compaction_input, compaction_service_result, 
        job_id, db_path_, file_metadata);
    
    if (s.ok()) {
      // Step 4: Download output files
      s = DownloadOutputFiles(job_id, output_dir);
      if (!s.ok()) {
        std::cerr << "[CompactionService] Failed to download output files: " << s.ToString() << std::endl;
      }
    }
    
    // Step 5: Cleanup remote files
    CleanupRemoteJob(job_id);
    
  } else {
    // Shared storage mode (original behavior)
    std::cout << "[CompactionService] Using shared storage mode for job " << job_id << std::endl;
    
    s = csa_client.OpenAndCompact(
        RemoteOpenAndCompactOptions(), db_path_, output_dir,
        compaction_input, compaction_service_result, options_override,
        shared_fs_uri_, shared_fs_local_prefix_);
  }

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
    std::string err_msg = s.ToString();
    bool is_stale_task = s.IsIOError();
    
    std::cout << "[CompactionService] Error details - IsIOError: " << s.IsIOError()
              << ", code: " << static_cast<int>(s.code()) 
              << ", msg: " << err_msg << std::endl;
    
    if (is_stale_task) {
      std::cout << "[CompactionService] IO error detected (likely stale task), "
                << "constructing empty result and returning kSuccess" << std::endl;
      CompactionServiceResult empty_result;
      empty_result.status = Status::OK();
      empty_result.Write(compaction_service_result);
      return CompactionServiceJobStatus::kSuccess;
    }
    
    std::cout << "[CompactionService] Non-IO error, falling back to local: " 
              << err_msg << std::endl;
    return CompactionServiceJobStatus::kUseLocal;
  }
}
}  // namespace ROCKSDB_NAMESPACE
