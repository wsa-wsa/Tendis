#include "nfs_filesystem.h"

#include <stdexcept>
#include <iostream>
#include <cstring>
#include <fcntl.h>  // for O_RDONLY, O_WRONLY, O_CREAT, O_TRUNC
#include <sys/stat.h>  // for S_ISDIR
#include <unistd.h>  // for getcwd
#include <climits>   // for PATH_MAX

#include "logging/env_logger.h"  // for EnvLogger

namespace {
// 将相对路径转换为绝对路径
std::string ToAbsolutePath(const std::string& path) {
  if (path.empty()) {
    return path;
  }
  // 已经是绝对路径
  if (path[0] == '/') {
    return path;
  }
  // 相对路径，转换为绝对路径
  char cwd[PATH_MAX];
  if (getcwd(cwd, sizeof(cwd)) != nullptr) {
    std::string abs_path = std::string(cwd) + "/" + path;
    // 规范化路径：处理 ./ 和 ../
    // 简单处理：移除 ./
    size_t pos;
    while ((pos = abs_path.find("/./")) != std::string::npos) {
      abs_path.erase(pos, 2);
    }
    // 处理开头的 ./
    if (abs_path.find("./") == 0) {
      abs_path = abs_path.substr(2);
    }
    return abs_path;
  }
  return path;  // 获取cwd失败，返回原路径
}
}  // anonymous namespace

namespace ROCKSDB_NAMESPACE {

// NFS sequential reading of files
class NFSSequentialFile : public FSSequentialFile {
 public:
  NFSSequentialFile(struct nfs_context* ctx, struct nfsfh* fh, 
                    const std::string& fname, std::mutex& mutex)
      : nfs_ctx_(ctx), nfs_fh_(fh), filename_(fname), nfs_mutex_(mutex) {}

  ~NFSSequentialFile() override {
    if (nfs_fh_) {
      std::lock_guard<std::mutex> lock(nfs_mutex_);
      nfs_close(nfs_ctx_, nfs_fh_);
    }
  }

  IOStatus Read(size_t n, const IOOptions& options, Slice* result,
                char* scratch, IODebugContext* dbg) override {
    std::lock_guard<std::mutex> lock(nfs_mutex_);
    int bytes_read = nfs_read(nfs_ctx_, nfs_fh_, n, scratch);
    if (bytes_read < 0) {
      return IOStatus::IOError("NFS read failed: " + filename_ +
                               ", error: " + std::string(nfs_get_error(nfs_ctx_)));
    }
    *result = Slice(scratch, bytes_read);
    return IOStatus::OK();
  }

  IOStatus Skip(uint64_t n) override {
    std::lock_guard<std::mutex> lock(nfs_mutex_);
    if (nfs_lseek(nfs_ctx_, nfs_fh_, n, SEEK_CUR, nullptr) < 0) {
      return IOStatus::IOError("NFS lseek failed: " + filename_);
    }
    return IOStatus::OK();
  }

 private:
  struct nfs_context* nfs_ctx_;
  struct nfsfh* nfs_fh_;
  std::string filename_;
  std::mutex& nfs_mutex_;
};

// NFS random reading of files
class NFSRandomAccessFile : public FSRandomAccessFile {
 public:
  NFSRandomAccessFile(struct nfs_context* ctx, struct nfsfh* fh,
                      const std::string& fname, std::mutex* mutex)
      : nfs_ctx_(ctx), nfs_fh_(fh), filename_(fname), nfs_mutex_(mutex) {}

  ~NFSRandomAccessFile() override {
    if (nfs_fh_) {
      std::lock_guard<std::mutex> lock(*nfs_mutex_);
      nfs_close(nfs_ctx_, nfs_fh_);
    }
  }

  IOStatus Read(uint64_t offset, size_t n, const IOOptions& options,
                Slice* result, char* scratch, IODebugContext* dbg) const override {
    std::lock_guard<std::mutex> lock(*nfs_mutex_);
    int bytes_read = nfs_pread(nfs_ctx_, nfs_fh_, offset, n, scratch);
    if (bytes_read < 0) {
      return IOStatus::IOError("NFS pread failed: " + filename_ +
                               ", error: " + std::string(nfs_get_error(nfs_ctx_)));
    }
    *result = Slice(scratch, bytes_read);
    return IOStatus::OK();
  }

 private:
  struct nfs_context* nfs_ctx_;
  struct nfsfh* nfs_fh_;
  std::string filename_;
  std::mutex* nfs_mutex_;
};

// NFS writable files
class NFSWritableFile : public FSWritableFile {
 public:
  NFSWritableFile(struct nfs_context* ctx, struct nfsfh* fh,
                  const std::string& fname, std::mutex& mutex)
      : nfs_ctx_(ctx), nfs_fh_(fh), filename_(fname), nfs_mutex_(mutex),
        filesize_(0) {}

  ~NFSWritableFile() override {
    if (nfs_fh_) {
      std::lock_guard<std::mutex> lock(nfs_mutex_);
      nfs_close(nfs_ctx_, nfs_fh_);
    }
  }

  IOStatus Append(const Slice& data, const IOOptions& options,
                  IODebugContext* dbg) override {
    std::lock_guard<std::mutex> lock(nfs_mutex_);
    const char* src = data.data();
    size_t left = data.size();
    
    while (left > 0) {
      int written = nfs_write(nfs_ctx_, nfs_fh_, left, const_cast<char*>(src));
      if (written < 0) {
        return IOStatus::IOError("NFS write failed: " + filename_ +
                                 ", error: " + std::string(nfs_get_error(nfs_ctx_)));
      }
      left -= written;
      src += written;
    }
    filesize_ += data.size();
    return IOStatus::OK();
  }

  IOStatus Append(const Slice& data, const IOOptions& options,
                  const DataVerificationInfo& verification_info,
                  IODebugContext* dbg) override {
    return Append(data, options, dbg);
  }

  IOStatus PositionedAppend(const Slice& data, uint64_t offset,
                            const IOOptions& options,
                            IODebugContext* dbg) override {
    std::lock_guard<std::mutex> lock(nfs_mutex_);
    const char* src = data.data();
    size_t left = data.size();
    uint64_t pos = offset;
    
    while (left > 0) {
      int written = nfs_pwrite(nfs_ctx_, nfs_fh_, pos, left, const_cast<char*>(src));
      if (written < 0) {
        return IOStatus::IOError("NFS pwrite failed: " + filename_ +
                                 ", error: " + std::string(nfs_get_error(nfs_ctx_)));
      }
      left -= written;
      src += written;
      pos += written;
    }
    if (offset + data.size() > filesize_) {
      filesize_ = offset + data.size();
    }
    return IOStatus::OK();
  }

  IOStatus PositionedAppend(const Slice& data, uint64_t offset,
                            const IOOptions& options,
                            const DataVerificationInfo& verification_info,
                            IODebugContext* dbg) override {
    return PositionedAppend(data, offset, options, dbg);
  }

  IOStatus Truncate(uint64_t size, const IOOptions& options,
                    IODebugContext* dbg) override {
    std::lock_guard<std::mutex> lock(nfs_mutex_);
    if (nfs_ftruncate(nfs_ctx_, nfs_fh_, size) != 0) {
      return IOStatus::IOError("NFS ftruncate failed: " + filename_ +
                               ", error: " + std::string(nfs_get_error(nfs_ctx_)));
    }
    filesize_ = size;
    return IOStatus::OK();
  }

  IOStatus Close(const IOOptions& options, IODebugContext* dbg) override {
    if (nfs_fh_) {
      std::lock_guard<std::mutex> lock(nfs_mutex_);
      if (nfs_close(nfs_ctx_, nfs_fh_) != 0) {
        return IOStatus::IOError("NFS close failed: " + filename_);
      }
      nfs_fh_ = nullptr;
    }
    return IOStatus::OK();
  }

  IOStatus Flush(const IOOptions& options, IODebugContext* dbg) override {
    // There is no explicit flush, and writes are synchronous
    return IOStatus::OK();
  }

  IOStatus Sync(const IOOptions& options, IODebugContext* dbg) override {
    std::lock_guard<std::mutex> lock(nfs_mutex_);
    if (nfs_fsync(nfs_ctx_, nfs_fh_) != 0) {
      return IOStatus::IOError("NFS fsync failed: " + filename_ +
                               ", error: " + std::string(nfs_get_error(nfs_ctx_)));
    }
    return IOStatus::OK();
  }

  IOStatus Fsync(const IOOptions& options, IODebugContext* dbg) override {
    return Sync(options, dbg);
  }

  uint64_t GetFileSize(const IOOptions& options, IODebugContext* dbg) override {
    return filesize_;
  }

  bool IsSyncThreadSafe() const override { return true; }

 private:
  struct nfs_context* nfs_ctx_;
  struct nfsfh* nfs_fh_;
  std::string filename_;
  std::mutex& nfs_mutex_;
  uint64_t filesize_;
};

// NFS directory (for operations like fsync)
class NFSDirectory : public FSDirectory {
 public:
  NFSDirectory(struct nfs_context* ctx, const std::string& dirname, std::mutex& mutex)
      : nfs_ctx_(ctx), dirname_(dirname), nfs_mutex_(mutex) {}

  ~NFSDirectory() override {}

  IOStatus Fsync(const IOOptions& options, IODebugContext* dbg) override {
    // fsync for NFS directories is usually no-op because NFS is synchronous
    // But we can try the fsync directory (if libnfs supports it)
    // For now, simply return to OK
    return IOStatus::OK();
  }

  IOStatus Close(const IOOptions& options, IODebugContext* dbg) override {
    return IOStatus::OK();
  }

 private:
  struct nfs_context* nfs_ctx_;
  std::string dirname_;
  std::mutex& nfs_mutex_;
};

// NFS file lock
class NFSFileLock : public FileLock {
 public:
  NFSFileLock(struct nfs_context* ctx, struct nfsfh* fh, 
              const std::string& fname, std::mutex& mutex)
      : nfs_ctx_(ctx), nfs_fh_(fh), filename_(fname), nfs_mutex_(mutex) {}

  ~NFSFileLock() override {
    // Automatically release the lock when destructuring
    if (nfs_fh_) {
      std::lock_guard<std::mutex> lock(nfs_mutex_);
      nfs_close(nfs_ctx_, nfs_fh_);
    }
  }

  struct nfsfh* GetFileHandle() const { return nfs_fh_; }
  const std::string& GetFilename() const { return filename_; }
  void ClearFileHandle() { nfs_fh_ = nullptr; }

 private:
  struct nfs_context* nfs_ctx_;
  struct nfsfh* nfs_fh_;
  std::string filename_;
  std::mutex& nfs_mutex_;
};


NFSFileSystem::NFSFileSystem(const std::string& nfs_url,
                             const std::string& local_prefix,
                             const std::shared_ptr<FileSystem>& base)
  : FileSystemWrapper(base), 
    nfs_url_(nfs_url), 
    local_prefix_(ToAbsolutePath(local_prefix)),  // 转换为绝对路径
    nfs_ctx_(nullptr) {
  // Ensure that the local_prefix does not end in '/' for easy subsequent processing
  if (!local_prefix_.empty() && local_prefix_.back() == '/') {
    local_prefix_.pop_back();
  }
  InitNFSContext();
  std::cout << "[NFSFileSystem] Initialized with:"
            << "\n  NFS URL: " << nfs_url_
            << "\n  Local Prefix (original): " << local_prefix
            << "\n  Local Prefix (absolute): " << local_prefix_ << std::endl;
}

NFSFileSystem::~NFSFileSystem() {
  if (nfs_ctx_) {
    std::cerr << "Destroying NFSFileSystem(" << nfs_url_ << ")" << std::endl;
    nfs_destroy_context(nfs_ctx_);
  }
}

std::string NFSFileSystem::GetId() const {
  if (nfs_url_.empty()) {
    return kProto;
  } else if (nfs_url_.find(kProto) == 0) {
    return nfs_url_;
  } else {
    std::string id = kProto;
    return id.append("localhost").append(nfs_url_);
  }
}

Status NFSFileSystem::ValidateOptions(const DBOptions& db_opts,
                                      const ColumnFamilyOptions& cf_opts) const {
  if (nfs_ctx_ != nullptr) {
    return FileSystemWrapper::ValidateOptions(db_opts, cf_opts);
  } else {
    return Status::InvalidArgument("Failed to connect to NFS ", nfs_url_);
  }
}

void NFSFileSystem::InitNFSContext() {
  nfs_ctx_ = nfs_init_context();
  if (!nfs_ctx_) {
    throw std::runtime_error("Failed to init NFS context");
  }

  struct nfs_url* url = nfs_parse_url_dir(nfs_ctx_, nfs_url_.c_str());
  if (!url) {
    nfs_destroy_context(nfs_ctx_);
    nfs_ctx_ = nullptr;
    throw std::runtime_error("Invalid NFS URL: " + nfs_url_);
  }

  if (nfs_mount(nfs_ctx_, url->server, url->path) != 0) {
    std::string error_msg =
      "Failed to mount NFS: " + std::string(nfs_get_error(nfs_ctx_));
    nfs_destroy_url(url);
    nfs_destroy_context(nfs_ctx_);
    nfs_ctx_ = nullptr;
    throw std::runtime_error(error_msg);
  }

  nfs_destroy_url(url);
}

bool NFSFileSystem::IsNFSPath(const std::string& path) const {
  // Check if the path should be accessed via NFS
  bool is_nfs = false;
  std::string reason;
  
  // 首先将输入路径也转换为绝对路径进行比较
  std::string abs_path = path;
  if (!path.empty() && path[0] != '/') {
    // 相对路径，转换为绝对路径
    char cwd[PATH_MAX];
    if (getcwd(cwd, sizeof(cwd)) != nullptr) {
      abs_path = std::string(cwd) + "/" + path;
      // 规范化路径：移除 ./
      size_t pos;
      while ((pos = abs_path.find("/./")) != std::string::npos) {
        abs_path.erase(pos, 2);
      }
    }
  }
  
  if (!local_prefix_.empty() && abs_path.find(local_prefix_) == 0) {
    is_nfs = true;
    reason = "matches local_prefix (after abs conversion)";
  } else if (path.find("/nfs/") == 0) {
    is_nfs = true;
    reason = "starts with /nfs/";
  } else if (path.find("nfs://") == 0) {
    is_nfs = true;
    reason = "starts with nfs://";
  } else {
    reason = "no match (path='" + abs_path + "', local_prefix='" + local_prefix_ + "')";
  }
  
  std::cout << "[NFSFileSystem] IsNFSPath: " << path 
            << " -> " << (is_nfs ? "NFS" : "LOCAL") 
            << " (" << reason << ")" << std::endl;
  
  return is_nfs;
}

std::string NFSFileSystem::ConvertToNFSPath(const std::string& path) const {
  // 将本地路径转换为NFS相对路径
  // 
  // 映射关系：local_prefix -> nfs_url (已在InitNFSContext中mount)
  // 所以只需要提取相对路径部分
  //
  // 例如：local_prefix_ = "/mnt/nfs_rocksdb/db"
  //       nfs_url_ = "nfs://192.168.1.100/shared/rocksdb"
  //       path = "/mnt/nfs_rocksdb/db/0/xxx.sst"
  //       
  //       提取相对路径: "0/xxx.sst" (不带前导斜杠)
  //       libnfs 会访问: nfs://192.168.1.100/shared/rocksdb/0/xxx.sst
  
  // 首先将输入路径转换为绝对路径
  std::string abs_path = path;
  if (!path.empty() && path[0] != '/') {
    char cwd[PATH_MAX];
    if (getcwd(cwd, sizeof(cwd)) != nullptr) {
      abs_path = std::string(cwd) + "/" + path;
      size_t pos;
      while ((pos = abs_path.find("/./")) != std::string::npos) {
        abs_path.erase(pos, 2);
      }
    }
  }
  
  if (!local_prefix_.empty() && abs_path.find(local_prefix_) == 0) {
    std::string relative_path = abs_path.substr(local_prefix_.length());
    // Remove the leading slash and return to the relative path
    while (!relative_path.empty() && relative_path[0] == '/') {
      relative_path = relative_path.substr(1);
    }
    std::cout << "[NFSFileSystem] ConvertToNFSPath: " << path 
              << " -> " << (relative_path.empty() ? "." : relative_path) << std::endl;
    return relative_path.empty() ? "." : relative_path;
  }
  
  if (path.find("/nfs/") == 0) {
    std::string rel = path.substr(5);  // Remove the "/nfs/" prefix
    return rel.empty() ? "." : rel;
  }
  
  std::cout << "[NFSFileSystem] ConvertToNFSPath: " << path 
            << " -> " << path << " (unchanged)" << std::endl;
  return path;
}

// Recursively create a catalog
IOStatus NFSFileSystem::CreateDirRecursive(const std::string& nfs_path) {
  // Create step by step starting from the root
  std::string current_path;
  size_t pos = 0;
  
  while (pos < nfs_path.length()) {
    size_t next_slash = nfs_path.find('/', pos + 1);
    if (next_slash == std::string::npos) {
      next_slash = nfs_path.length();
    }
    
    current_path = nfs_path.substr(0, next_slash);
    pos = next_slash;
    
    if (current_path.empty() || current_path == "/") {
      continue;
    }
    
    // Check if the catalog exists
    struct nfs_stat_64 st;
    if (nfs_stat64(nfs_ctx_, current_path.c_str(), &st) != 0) {
      // The directory does not exist, create it
      if (nfs_mkdir(nfs_ctx_, current_path.c_str()) != 0) {
        std::string err = nfs_get_error(nfs_ctx_);
        // Ignore "already exists" error (possibly concurrent creation)
        if (err.find("exist") == std::string::npos && 
            err.find("EXIST") == std::string::npos) {
          return IOStatus::IOError("Failed to create NFS directory: " + current_path +
                                   ", error: " + err);
        }
      }
    }
  }
  
  return IOStatus::OK();
}


IOStatus NFSFileSystem::NewSequentialFile(const std::string& fname,
                                          const FileOptions& options,
                                          std::unique_ptr<FSSequentialFile>* result,
                                          IODebugContext* dbg) {
  if (!IsNFSPath(fname)) {
    return FileSystemWrapper::NewSequentialFile(fname, options, result, dbg);
  }
  
  std::lock_guard<std::mutex> lock(nfs_mutex_);
  
  std::string nfs_path = ConvertToNFSPath(fname);
  std::cout << "[NFSFileSystem] NewSequentialFile: " << fname 
            << " -> NFS path: " << nfs_path << std::endl;
  
  struct nfsfh* fh = nullptr;
  if (nfs_open(nfs_ctx_, nfs_path.c_str(), O_RDONLY, &fh) != 0) {
    return IOStatus::IOError("Failed to open NFS file for reading: " + fname +
                             ", error: " + std::string(nfs_get_error(nfs_ctx_)));
  }
  
  result->reset(new NFSSequentialFile(nfs_ctx_, fh, fname, nfs_mutex_));
  return IOStatus::OK();
}

IOStatus NFSFileSystem::NewRandomAccessFile(const std::string& fname,
                                            const FileOptions& options,
                                            std::unique_ptr<FSRandomAccessFile>* result,
                                            IODebugContext* dbg) {
  if (!IsNFSPath(fname)) {
    return FileSystemWrapper::NewRandomAccessFile(fname, options, result, dbg);
  }
  
  std::lock_guard<std::mutex> lock(nfs_mutex_);
  
  std::string nfs_path = ConvertToNFSPath(fname);
  std::cout << "[NFSFileSystem] NewRandomAccessFile: " << fname 
            << " -> NFS path: " << nfs_path << std::endl;
  
  struct nfsfh* fh = nullptr;
  if (nfs_open(nfs_ctx_, nfs_path.c_str(), O_RDONLY, &fh) != 0) {
    return IOStatus::IOError("Failed to open NFS file for random access: " + fname +
                             ", error: " + std::string(nfs_get_error(nfs_ctx_)));
  }
  
  result->reset(new NFSRandomAccessFile(nfs_ctx_, fh, fname, &nfs_mutex_));
  return IOStatus::OK();
}

IOStatus NFSFileSystem::NewWritableFile(const std::string& fname,
                                        const FileOptions& options,
                                        std::unique_ptr<FSWritableFile>* result,
                                        IODebugContext* dbg) {
  if (!IsNFSPath(fname)) {
    return FileSystemWrapper::NewWritableFile(fname, options, result, dbg);
  }
  
  std::lock_guard<std::mutex> lock(nfs_mutex_);
  
  std::string nfs_path = ConvertToNFSPath(fname);
  std::cout << "[NFSFileSystem] NewWritableFile: " << fname 
            << " -> NFS path: " << nfs_path << std::endl;
  
  // Ensure that the parent directory exists
  size_t last_slash = nfs_path.rfind('/');
  if (last_slash != std::string::npos && last_slash > 0) {
    std::string parent_dir = nfs_path.substr(0, last_slash);
    IOStatus dir_status = CreateDirRecursive(parent_dir);
    if (!dir_status.ok()) {
      return dir_status;
    }
  }
  
  struct nfsfh* fh = nullptr;
  // nfs_creat Create a file using the mode parameter
  int mode = 0644;  // rw-r--r--
  
  if (nfs_creat(nfs_ctx_, nfs_path.c_str(), mode, &fh) != 0) {
    return IOStatus::IOError("Failed to create NFS file: " + fname +
                             ", error: " + std::string(nfs_get_error(nfs_ctx_)));
  }
  
  result->reset(new NFSWritableFile(nfs_ctx_, fh, fname, nfs_mutex_));
  return IOStatus::OK();
}

// Reopen the file in append mode (for LOG files, etc.)
IOStatus NFSFileSystem::ReopenWritableFile(const std::string& fname,
                                           const FileOptions& options,
                                           std::unique_ptr<FSWritableFile>* result,
                                           IODebugContext* dbg) {
  if (!IsNFSPath(fname)) {
    return FileSystemWrapper::ReopenWritableFile(fname, options, result, dbg);
  }
  
  std::lock_guard<std::mutex> lock(nfs_mutex_);
  
  std::string nfs_path = ConvertToNFSPath(fname);
  std::cout << "[NFSFileSystem] ReopenWritableFile (append): " << fname 
            << " -> NFS path: " << nfs_path << std::endl;
  
  // Ensure that the parent directory exists
  size_t last_slash = nfs_path.rfind('/');
  if (last_slash != std::string::npos && last_slash > 0) {
    std::string parent_dir = nfs_path.substr(0, last_slash);
    IOStatus dir_status = CreateDirRecursive(parent_dir);
    if (!dir_status.ok()) {
      return dir_status;
    }
  }
  
  struct nfsfh* fh = nullptr;
  
  // Check if the file exists
  struct nfs_stat_64 st;
  uint64_t initial_size = 0;
  bool file_exists = (nfs_stat64(nfs_ctx_, nfs_path.c_str(), &st) == 0);
  
  if (file_exists) {
    // The file exists, opening in append mode
    if (nfs_open(nfs_ctx_, nfs_path.c_str(), O_WRONLY | O_APPEND, &fh) != 0) {
      return IOStatus::IOError("Failed to open NFS file for appending: " + fname +
                               ", error: " + std::string(nfs_get_error(nfs_ctx_)));
    }
    initial_size = st.nfs_size;
  } else {
    // The file does not exist, create a new file
    int mode = 0644;  // rw-r--r--
    if (nfs_creat(nfs_ctx_, nfs_path.c_str(), mode, &fh) != 0) {
      return IOStatus::IOError("Failed to create NFS file: " + fname +
                               ", error: " + std::string(nfs_get_error(nfs_ctx_)));
    }
  }
  
  result->reset(new NFSWritableFile(nfs_ctx_, fh, fname, nfs_mutex_));
  return IOStatus::OK();
}

IOStatus NFSFileSystem::DeleteFile(const std::string& fname,
                                   const IOOptions& options,
                                   IODebugContext* dbg) {
  if (!IsNFSPath(fname)) {
    return FileSystemWrapper::DeleteFile(fname, options, dbg);
  }

  std::lock_guard<std::mutex> lock(nfs_mutex_);

  std::string nfs_path = ConvertToNFSPath(fname);
  std::cout << "[NFSFileSystem] DeleteFile: " << fname 
            << " -> NFS path: " << nfs_path << std::endl;
            
  if (nfs_unlink(nfs_ctx_, nfs_path.c_str()) != 0) {
    return IOStatus::IOError("Failed to delete NFS file: " + fname + 
                             ", error: " + std::string(nfs_get_error(nfs_ctx_)));
  }

  return IOStatus::OK();
}

IOStatus NFSFileSystem::RenameFile(const std::string& src,
                                   const std::string& target,
                                   const IOOptions& options,
                                   IODebugContext* dbg) {
  bool src_is_nfs = IsNFSPath(src);
  bool target_is_nfs = IsNFSPath(target);
  
  std::cout << "[NFSFileSystem] RenameFile check: src=" << src 
            << " (is_nfs=" << src_is_nfs << "), target=" << target 
            << " (is_nfs=" << target_is_nfs << ")" << std::endl;
  
  if (!src_is_nfs && !target_is_nfs) {
    std::cout << "[NFSFileSystem] RenameFile: Both local, delegating to base FS" << std::endl;
    return FileSystemWrapper::RenameFile(src, target, options, dbg);
  }

  std::lock_guard<std::mutex> lock(nfs_mutex_);

  std::string nfs_src = ConvertToNFSPath(src);
  std::string nfs_target = ConvertToNFSPath(target);
  
  std::cout << "[NFSFileSystem] RenameFile NFS: " << nfs_src << " -> " << nfs_target << std::endl;

  if (nfs_rename(nfs_ctx_, nfs_src.c_str(), nfs_target.c_str()) != 0) {
    std::string err = nfs_get_error(nfs_ctx_);
    std::cerr << "[NFSFileSystem] RenameFile FAILED: " << err << std::endl;
    return IOStatus::IOError("Failed to rename NFS file: " + src + " to " + target +
                             ", error: " + err);
  }
  
  std::cout << "[NFSFileSystem] RenameFile SUCCESS" << std::endl;
  return IOStatus::OK();
}

IOStatus NFSFileSystem::GetFileSize(const std::string& fname,
                                    const IOOptions& options,
                                    uint64_t* file_size,
                                    IODebugContext* dbg) {
  if (!IsNFSPath(fname)) {
    return FileSystemWrapper::GetFileSize(fname, options, file_size, dbg);
  }

  std::lock_guard<std::mutex> lock(nfs_mutex_);

  std::string nfs_path = ConvertToNFSPath(fname);
  struct nfs_stat_64 st;

  if (nfs_stat64(nfs_ctx_, nfs_path.c_str(), &st) != 0) {
    return IOStatus::IOError("Failed to stat NFS file: " + fname +
                             ", error: " + std::string(nfs_get_error(nfs_ctx_)));
  }

  *file_size = st.nfs_size;
  return IOStatus::OK();
}

IOStatus NFSFileSystem::FileExists(const std::string& fname,
                                   const IOOptions& options,
                                   IODebugContext* dbg) {
  if (!IsNFSPath(fname)) {
    return FileSystemWrapper::FileExists(fname, options, dbg);
  }

  std::lock_guard<std::mutex> lock(nfs_mutex_);

  std::string nfs_path = ConvertToNFSPath(fname);
  struct nfs_stat_64 st;

  if (nfs_stat64(nfs_ctx_, nfs_path.c_str(), &st) != 0) {
    return IOStatus::NotFound(fname);
  }

  return IOStatus::OK();
}

IOStatus NFSFileSystem::CreateDir(const std::string& dirname,
                                  const IOOptions& options,
                                  IODebugContext* dbg) {
  if (!IsNFSPath(dirname)) {
    return FileSystemWrapper::CreateDir(dirname, options, dbg);
  }

  std::lock_guard<std::mutex> lock(nfs_mutex_);

  std::string nfs_path = ConvertToNFSPath(dirname);
  std::cout << "[NFSFileSystem] CreateDir: " << dirname 
            << " -> NFS path: " << nfs_path << std::endl;

  if (nfs_mkdir(nfs_ctx_, nfs_path.c_str()) != 0) {
    std::string err = nfs_get_error(nfs_ctx_);
    // If the directory already exists, it is not an error
    if (err.find("exist") != std::string::npos || 
        err.find("EXIST") != std::string::npos) {
      return IOStatus::OK();
    }
    return IOStatus::IOError("Failed to create NFS directory: " + dirname +
                             ", error: " + err);
  }

  return IOStatus::OK();
}

IOStatus NFSFileSystem::CreateDirIfMissing(const std::string& dirname,
                                           const IOOptions& options,
                                           IODebugContext* dbg) {
  if (!IsNFSPath(dirname)) {
    return FileSystemWrapper::CreateDirIfMissing(dirname, options, dbg);
  }

  std::lock_guard<std::mutex> lock(nfs_mutex_);

  std::string nfs_path = ConvertToNFSPath(dirname);
  std::cout << "[NFSFileSystem] CreateDirIfMissing: " << dirname 
            << " -> NFS path: " << nfs_path << std::endl;

  // Check if the directory exists first
  struct nfs_stat_64 st;
  if (nfs_stat64(nfs_ctx_, nfs_path.c_str(), &st) == 0) {
    // The catalog already exists
    return IOStatus::OK();
  }

  // Recursively create a catalog
  return CreateDirRecursive(nfs_path);
}

IOStatus NFSFileSystem::GetChildren(const std::string& dir,
                                    const IOOptions& options,
                                    std::vector<std::string>* result,
                                    IODebugContext* dbg) {
  if (!IsNFSPath(dir)) {
    return FileSystemWrapper::GetChildren(dir, options, result, dbg);
  }

  std::lock_guard<std::mutex> lock(nfs_mutex_);

  std::string nfs_path = ConvertToNFSPath(dir);
  
  struct nfsdir* nfsdir_handle;
  if (nfs_opendir(nfs_ctx_, nfs_path.c_str(), &nfsdir_handle) != 0) {
    return IOStatus::IOError("Failed to open NFS directory: " + dir +
                             ", error: " + std::string(nfs_get_error(nfs_ctx_)));
  }

  result->clear();
  struct nfsdirent* dirent;
  while ((dirent = nfs_readdir(nfs_ctx_, nfsdir_handle)) != nullptr) {
    std::string name = dirent->name;
    if (name != "." && name != "..") {
      result->push_back(name);
    }
  }

  nfs_closedir(nfs_ctx_, nfsdir_handle);
  return IOStatus::OK();
}

IOStatus NFSFileSystem::GetFileModificationTime(const std::string& fname,
                                                const IOOptions& options,
                                                uint64_t* file_mtime,
                                                IODebugContext* dbg) {
  if (!IsNFSPath(fname)) {
    return FileSystemWrapper::GetFileModificationTime(fname, options, file_mtime, dbg);
  }

  std::lock_guard<std::mutex> lock(nfs_mutex_);

  std::string nfs_path = ConvertToNFSPath(fname);
  struct nfs_stat_64 st;

  if (nfs_stat64(nfs_ctx_, nfs_path.c_str(), &st) != 0) {
    return IOStatus::IOError("Failed to stat NFS file: " + fname +
                             ", error: " + std::string(nfs_get_error(nfs_ctx_)));
  }

  *file_mtime = st.nfs_mtime;
  return IOStatus::OK();
}

// Rewrite NewDirectory to open directory handles
IOStatus NFSFileSystem::NewDirectory(const std::string& name,
                                     const IOOptions& io_opts,
                                     std::unique_ptr<FSDirectory>* result,
                                     IODebugContext* dbg) {
  if (!IsNFSPath(name)) {
    return FileSystemWrapper::NewDirectory(name, io_opts, result, dbg);
  }

  std::lock_guard<std::mutex> lock(nfs_mutex_);

  std::string nfs_path = ConvertToNFSPath(name);
  std::cout << "[NFSFileSystem] NewDirectory: " << name 
            << " -> NFS path: " << nfs_path << std::endl;

  // Check if the catalog exists
  struct nfs_stat_64 st;
  if (nfs_stat64(nfs_ctx_, nfs_path.c_str(), &st) != 0) {
    return IOStatus::IOError("Failed to open NFS directory: " + name +
                             ", error: " + std::string(nfs_get_error(nfs_ctx_)));
  }

  // Check if it's a table of contents
  if (!S_ISDIR(st.nfs_mode)) {
    return IOStatus::IOError("Not a directory: " + name);
  }

  result->reset(new NFSDirectory(nfs_ctx_, nfs_path, nfs_mutex_));
  return IOStatus::OK();
}

// Delete directory
IOStatus NFSFileSystem::DeleteDir(const std::string& dirname,
                                  const IOOptions& options,
                                  IODebugContext* dbg) {
  if (!IsNFSPath(dirname)) {
    return FileSystemWrapper::DeleteDir(dirname, options, dbg);
  }

  std::lock_guard<std::mutex> lock(nfs_mutex_);

  std::string nfs_path = ConvertToNFSPath(dirname);
  std::cout << "[NFSFileSystem] DeleteDir: " << dirname 
            << " -> NFS path: " << nfs_path << std::endl;

  if (nfs_rmdir(nfs_ctx_, nfs_path.c_str()) != 0) {
    return IOStatus::IOError("Failed to delete NFS directory: " + dirname +
                             ", error: " + std::string(nfs_get_error(nfs_ctx_)));
  }

  return IOStatus::OK();
}

// Check if path is a directory
IOStatus NFSFileSystem::IsDirectory(const std::string& path,
                                    const IOOptions& options,
                                    bool* is_dir,
                                    IODebugContext* dbg) {
  if (!IsNFSPath(path)) {
    return FileSystemWrapper::IsDirectory(path, options, is_dir, dbg);
  }

  std::lock_guard<std::mutex> lock(nfs_mutex_);

  std::string nfs_path = ConvertToNFSPath(path);
  struct nfs_stat_64 st;

  if (nfs_stat64(nfs_ctx_, nfs_path.c_str(), &st) != 0) {
    return IOStatus::IOError("Failed to stat NFS path: " + path +
                             ", error: " + std::string(nfs_get_error(nfs_ctx_)));
  }

  if (is_dir != nullptr) {
    *is_dir = S_ISDIR(st.nfs_mode);
  }
  return IOStatus::OK();
}

// Rewrite LockFile
IOStatus NFSFileSystem::LockFile(const std::string& fname,
                                 const IOOptions& options,
                                 FileLock** lock,
                                 IODebugContext* dbg) {
  if (!IsNFSPath(fname)) {
    return FileSystemWrapper::LockFile(fname, options, lock, dbg);
  }

  std::lock_guard<std::mutex> lk(nfs_mutex_);

  std::string nfs_path = ConvertToNFSPath(fname);
  std::cout << "[NFSFileSystem] LockFile: " << fname 
            << " -> NFS path: " << nfs_path << std::endl;

  // Ensure that the parent directory exists
  size_t last_slash = nfs_path.rfind('/');
  if (last_slash != std::string::npos && last_slash > 0) {
    std::string parent_dir = nfs_path.substr(0, last_slash);
    IOStatus dir_status = CreateDirRecursive(parent_dir);
    if (!dir_status.ok()) {
      return dir_status;
    }
  }

  // Create or open a lock file
  struct nfsfh* fh = nullptr;
  int mode = 0644;
  
  // Try creating a file if it doesn't exist
  if (nfs_creat(nfs_ctx_, nfs_path.c_str(), mode, &fh) != 0) {
    // If the creation fails, try to open the existing file
    if (nfs_open(nfs_ctx_, nfs_path.c_str(), O_RDWR, &fh) != 0) {
      return IOStatus::IOError("Failed to open lock file: " + fname +
                               ", error: " + std::string(nfs_get_error(nfs_ctx_)));
    }
  }

  // Note: NFS has limited file lock support, here we simply keep the file open
  // True locking requires the use of the NLM (Network Lock Manager) protocol
  // For single-instance scenarios, keeping the file open is sufficient
  
  *lock = new NFSFileLock(nfs_ctx_, fh, fname, nfs_mutex_);
  return IOStatus::OK();
}

// Rewrite the UnlockFile
IOStatus NFSFileSystem::UnlockFile(FileLock* lock,
                                   const IOOptions& options,
                                   IODebugContext* dbg) {
  if (lock == nullptr) {
    return IOStatus::OK();
  }

  NFSFileLock* nfs_lock = dynamic_cast<NFSFileLock*>(lock);
  if (nfs_lock == nullptr) {
    // Not our lock, leave it to the parent to handle
    return FileSystemWrapper::UnlockFile(lock, options, dbg);
  }

  std::cout << "[NFSFileSystem] UnlockFile: " << nfs_lock->GetFilename() << std::endl;

  // NFSFileLock's destructor closes the file handle
  delete nfs_lock;
  return IOStatus::OK();
}

// Rewrite NewLogger, making sure to use our NewWritableFile
IOStatus NFSFileSystem::NewLogger(const std::string& fname,
                                  const IOOptions& io_opts,
                                  std::shared_ptr<Logger>* result,
                                  IODebugContext* dbg) {
  // For NFS paths, we need to use our own NewWritableFile
  // Instead of calling target_->NewLogger (local file system will be used)

  if (!IsNFSPath(fname)) {
    return FileSystemWrapper::NewLogger(fname, io_opts, result, dbg);
  }
  
  std::cout << "[NFSFileSystem] NewLogger: " << fname << std::endl;
  
  FileOptions options;
  options.io_options = io_opts;
  options.writable_file_max_buffer_size = 1024 * 1024;
  
  std::unique_ptr<FSWritableFile> writable_file;
  // Call our own NewWritableFile
  const IOStatus status = NewWritableFile(fname, options, &writable_file, dbg);
  if (!status.ok()) {
    return status;
  }
  
  // Create an EnvLogger (using rocksdb's EnvLogger)
  *result = std::make_shared<EnvLogger>(std::move(writable_file), fname,
                                        options, Env::Default());
  return IOStatus::OK();
}


Status NewNFSFileSystem(const std::string& nfs_url,
                        const std::string& local_prefix,
                        std::shared_ptr<FileSystem>* result) {
  try {
    auto base_fs = FileSystem::Default();
    *result = std::make_shared<NFSFileSystem>(nfs_url, local_prefix, base_fs);
    return Status::OK();
  } catch (const std::exception& e) {
    return Status::IOError("Failed to create NFS FileSystem: " +
                           std::string(e.what()));
  }
}

}  // namespace ROCKSDB_NAMESPACE
