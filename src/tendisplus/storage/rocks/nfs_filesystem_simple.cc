// nfs_filesystem_simple.cc
// Simplified only handles SST/Blob data files
#include "nfs_filesystem_simple.h"

#include <fcntl.h>
#include <iostream>
#include <stdexcept>
#include <unistd.h>  // for getcwd
#include <climits>   // for PATH_MAX

namespace {
// Convert relative path to absolute path
std::string ToAbsolutePath(const std::string& path) {
  if (path.empty()) {
    return path;
  }
  // Already an absolute path
  if (path[0] == '/') {
    return path;
  }
  // Relative path, convert to absolute path
  char cwd[PATH_MAX];
  if (getcwd(cwd, sizeof(cwd)) != nullptr) {
    std::string abs_path = std::string(cwd) + "/" + path;
    // Normalize path: handle ./
    size_t pos;
    while ((pos = abs_path.find("/./")) != std::string::npos) {
      abs_path.erase(pos, 2);
    }
    if (abs_path.find("./") == 0) {
      abs_path = abs_path.substr(2);
    }
    return abs_path;
  }
  return path;
}
}  // anonymous namespace

namespace ROCKSDB_NAMESPACE {

// ============ NFS file class ============

class NFSRandomAccessFileSimple : public FSRandomAccessFile {
 public:
  NFSRandomAccessFileSimple(struct nfs_context* ctx, struct nfsfh* fh,
                            const std::string& fname, std::mutex* mutex)
      : nfs_ctx_(ctx), nfs_fh_(fh), filename_(fname), nfs_mutex_(mutex) {}

  ~NFSRandomAccessFileSimple() override {
    if (nfs_fh_) {
      std::lock_guard<std::mutex> lock(*nfs_mutex_);
      nfs_close(nfs_ctx_, nfs_fh_);
    }
  }

  IOStatus Read(uint64_t offset, size_t n, const IOOptions& options,
                Slice* result, char* scratch,
                IODebugContext* dbg) const override {
    std::lock_guard<std::mutex> lock(*nfs_mutex_);
    int bytes_read = nfs_pread(nfs_ctx_, nfs_fh_, offset, n, scratch);
    if (bytes_read < 0) {
      return IOStatus::IOError("NFS pread failed: " + filename_);
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

class NFSWritableFileSimple : public FSWritableFile {
 public:
  NFSWritableFileSimple(struct nfs_context* ctx, struct nfsfh* fh,
                        const std::string& fname, std::mutex& mutex)
      : nfs_ctx_(ctx), nfs_fh_(fh), filename_(fname), nfs_mutex_(mutex),
        filesize_(0) {}

  ~NFSWritableFileSimple() override {
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
        return IOStatus::IOError("NFS write failed: " + filename_);
      }
      left -= written;
      src += written;
    }
    filesize_ += data.size();
    return IOStatus::OK();
  }

  IOStatus Append(const Slice& data, const IOOptions& options,
                  const DataVerificationInfo&, IODebugContext* dbg) override {
    return Append(data, options, dbg);
  }

  IOStatus Close(const IOOptions&, IODebugContext*) override {
    if (nfs_fh_) {
      std::lock_guard<std::mutex> lock(nfs_mutex_);
      nfs_close(nfs_ctx_, nfs_fh_);
      nfs_fh_ = nullptr;
    }
    return IOStatus::OK();
  }

  IOStatus Flush(const IOOptions&, IODebugContext*) override {
    return IOStatus::OK();
  }

  IOStatus Sync(const IOOptions&, IODebugContext*) override {
    std::lock_guard<std::mutex> lock(nfs_mutex_);
    if (nfs_fh_ && nfs_fsync(nfs_ctx_, nfs_fh_) != 0) {
      return IOStatus::IOError("NFS fsync failed: " + filename_);
    }
    return IOStatus::OK();
  }

  IOStatus Fsync(const IOOptions& opts, IODebugContext* dbg) override {
    return Sync(opts, dbg);
  }

  uint64_t GetFileSize(const IOOptions&, IODebugContext*) override {
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

// ============ NFSFileSystemSimple implementation ============

NFSFileSystemSimple::NFSFileSystemSimple(const std::string& nfs_url,
                                         const std::string& local_prefix,
                                         const std::shared_ptr<FileSystem>& base)
    : FileSystemWrapper(base),
      nfs_url_(nfs_url),
      local_prefix_(ToAbsolutePath(local_prefix)),  // Convert to absolute path
      nfs_ctx_(nullptr) {
  if (!local_prefix_.empty() && local_prefix_.back() == '/') {
    local_prefix_.pop_back();
  }
  InitNFSContext();
  std::cout << "[NFSFileSystemSimple] Initialized - only SST/Blob files go to NFS\n"
            << "  NFS URL: " << nfs_url_ << "\n"
            << "  Local Prefix (original): " << local_prefix << "\n"
            << "  Local Prefix (absolute): " << local_prefix_ << std::endl;
}

NFSFileSystemSimple::~NFSFileSystemSimple() {
  if (nfs_ctx_) {
    nfs_destroy_context(nfs_ctx_);
  }
}

void NFSFileSystemSimple::InitNFSContext() {
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
    std::string err = nfs_get_error(nfs_ctx_);
    nfs_destroy_url(url);
    nfs_destroy_context(nfs_ctx_);
    nfs_ctx_ = nullptr;
    throw std::runtime_error("Failed to mount NFS: " + err);
  }
  nfs_destroy_url(url);
}

bool NFSFileSystemSimple::IsDataFile(const std::string& path) const {
  // Only .sst and .blob files go NFS
  size_t len = path.length();
  if (len > 4 && path.substr(len - 4) == ".sst") return true;
  if (len > 5 && path.substr(len - 5) == ".blob") return true;
  return false;
}

std::string NFSFileSystemSimple::ConvertToNFSPath(const std::string& path) const {
  // First convert input path to absolute path
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
    std::string rel = abs_path.substr(local_prefix_.length());
    // Make sure the path starts with '/' (libnfs requires an absolute path, relative to the mount point)
    if (rel.empty()) {
      rel = "/";
    } else if (rel[0] != '/') {
      rel = "/" + rel;
    }
    return rel;
  }
  // If the path doesn't start with localprefix, make sure it starts with '/'
  if (!path.empty() && path[0] != '/') {
    return "/" + path;
  }
  return path;
}

IOStatus NFSFileSystemSimple::EnsureNFSParentDir(const std::string& nfs_path) {
  size_t pos = nfs_path.rfind('/');
  // No slash or only leading slash means file is in root, no parent dir needed
  if (pos == std::string::npos || pos == 0) return IOStatus::OK();

  std::string parent = nfs_path.substr(0, pos);
  if (parent.empty() || parent == "/") return IOStatus::OK();

  std::string current;
  size_t start = (parent[0] == '/') ? 1 : 0;  // Skip leading slash

  // Create directories step by step
  while (start < parent.length()) {
    size_t next = parent.find('/', start);
    if (next == std::string::npos) next = parent.length();
    
    if (next > start) {
      current = parent.substr(0, next);
      
      struct nfs_stat_64 st;
      if (nfs_stat64(nfs_ctx_, current.c_str(), &st) != 0) {
        if (nfs_mkdir(nfs_ctx_, current.c_str()) != 0) {
          std::string err = nfs_get_error(nfs_ctx_);
          if (err.find("exist") == std::string::npos &&
              err.find("EXIST") == std::string::npos) {
            return IOStatus::IOError("Failed to mkdir: " + current);
          }
        }
      }
    }
    start = next + 1;
  }
  return IOStatus::OK();
}

// ============ Core method overrides ============

IOStatus NFSFileSystemSimple::NewRandomAccessFile(
    const std::string& fname, const FileOptions& options,
    std::unique_ptr<FSRandomAccessFile>* result, IODebugContext* dbg) {
  // Non-data files go to local filesystem
  if (!IsDataFile(fname)) {
    return FileSystemWrapper::NewRandomAccessFile(fname, options, result, dbg);
  }

  std::lock_guard<std::mutex> lock(nfs_mutex_);
  std::string nfs_path = ConvertToNFSPath(fname);
  std::cout << "[NFS-READ] Opening: " << fname << " -> " << nfs_path << std::endl;

  struct nfsfh* fh = nullptr;
  if (nfs_open(nfs_ctx_, nfs_path.c_str(), O_RDONLY, &fh) != 0) {
    std::cerr << "[NFS-READ] FAILED: " << fname << " -> " << nfs_path 
              << ", error: " << nfs_get_error(nfs_ctx_) << std::endl;
    return IOStatus::IOError("NFS open failed: " + fname + " -> " + nfs_path +
                             ", error: " + nfs_get_error(nfs_ctx_));
  }

  result->reset(new NFSRandomAccessFileSimple(nfs_ctx_, fh, fname, &nfs_mutex_));
  return IOStatus::OK();
}

IOStatus NFSFileSystemSimple::NewWritableFile(
    const std::string& fname, const FileOptions& options,
    std::unique_ptr<FSWritableFile>* result, IODebugContext* dbg) {
  // Non-data files go to local filesystem
  if (!IsDataFile(fname)) {
    return FileSystemWrapper::NewWritableFile(fname, options, result, dbg);
  }

  std::lock_guard<std::mutex> lock(nfs_mutex_);
  std::string nfs_path = ConvertToNFSPath(fname);
  std::cout << "[NFS-WRITE] Creating: " << fname << " -> " << nfs_path << std::endl;

  // Ensure parent directory exists
  IOStatus dir_st = EnsureNFSParentDir(nfs_path);
  if (!dir_st.ok()) return dir_st;

  struct nfsfh* fh = nullptr;
  if (nfs_creat(nfs_ctx_, nfs_path.c_str(), 0644, &fh) != 0) {
    std::cerr << "[NFS-WRITE] FAILED: " << fname << " -> " << nfs_path
              << ", error: " << nfs_get_error(nfs_ctx_) << std::endl;
    return IOStatus::IOError("NFS creat failed: " + fname + " -> " + nfs_path +
                             ", error: " + nfs_get_error(nfs_ctx_));
  }

  result->reset(new NFSWritableFileSimple(nfs_ctx_, fh, fname, nfs_mutex_));
  return IOStatus::OK();
}

IOStatus NFSFileSystemSimple::DeleteFile(const std::string& fname,
                                         const IOOptions& options,
                                         IODebugContext* dbg) {
  // Non-data files go to local filesystem
  if (!IsDataFile(fname)) {
    return FileSystemWrapper::DeleteFile(fname, options, dbg);
  }

  std::lock_guard<std::mutex> lock(nfs_mutex_);
  std::string nfs_path = ConvertToNFSPath(fname);

  if (nfs_unlink(nfs_ctx_, nfs_path.c_str()) != 0) {
    std::string err = nfs_get_error(nfs_ctx_);
    // File not found is not an error
    if (err.find("No such file") == std::string::npos) {
      return IOStatus::IOError("NFS unlink failed: " + fname);
    }
  }
  return IOStatus::OK();
}

IOStatus NFSFileSystemSimple::GetFileSize(const std::string& fname,
                                          const IOOptions& options,
                                          uint64_t* file_size,
                                          IODebugContext* dbg) {
  // Non-data files go to local filesystem
  if (!IsDataFile(fname)) {
    return FileSystemWrapper::GetFileSize(fname, options, file_size, dbg);
  }

  std::lock_guard<std::mutex> lock(nfs_mutex_);
  std::string nfs_path = ConvertToNFSPath(fname);
  std::cout << "[NFS-STAT] GetFileSize: " << fname << " -> " << nfs_path << std::endl;

  struct nfs_stat_64 st;
  if (nfs_stat64(nfs_ctx_, nfs_path.c_str(), &st) != 0) {
    std::cerr << "[NFS-STAT] FAILED: " << fname << " -> " << nfs_path
              << ", error: " << nfs_get_error(nfs_ctx_) << std::endl;
    return IOStatus::IOError("NFS stat failed: " + fname + " -> " + nfs_path +
                             ", error: " + nfs_get_error(nfs_ctx_));
  }

  *file_size = st.nfs_size;
  return IOStatus::OK();
}

IOStatus NFSFileSystemSimple::FileExists(const std::string& fname,
                                         const IOOptions& options,
                                         IODebugContext* dbg) {
  // Non-data files go to local filesystem
  if (!IsDataFile(fname)) {
    return FileSystemWrapper::FileExists(fname, options, dbg);
  }

  std::lock_guard<std::mutex> lock(nfs_mutex_);
  std::string nfs_path = ConvertToNFSPath(fname);

  struct nfs_stat_64 st;
  if (nfs_stat64(nfs_ctx_, nfs_path.c_str(), &st) != 0) {
    return IOStatus::NotFound("NFS file not found: " + fname);
  }
  return IOStatus::OK();
}

IOStatus NFSFileSystemSimple::RenameFile(const std::string& src,
                                         const std::string& dst,
                                         const IOOptions& options,
                                         IODebugContext* dbg) {
  bool src_is_data = IsDataFile(src);
  bool dst_is_data = IsDataFile(dst);

  std::cout << "[NFS-RENAME] src=" << src << " (is_data=" << src_is_data << ")"
            << " dst=" << dst << " (is_data=" << dst_is_data << ")" << std::endl;

  // Neither is a data file, use local filesystem
  if (!src_is_data && !dst_is_data) {
    std::cout << "[NFS-RENAME] Both local, delegating to base FS" << std::endl;
    return FileSystemWrapper::RenameFile(src, dst, options, dbg);
  }

  // Both are data files, rename on NFS
  if (src_is_data && dst_is_data) {
    std::lock_guard<std::mutex> lock(nfs_mutex_);
    std::string nfs_src = ConvertToNFSPath(src);
    std::string nfs_dst = ConvertToNFSPath(dst);

    std::cout << "[NFS-RENAME] NFS rename: " << nfs_src << " -> " << nfs_dst << std::endl;

    // Ensure target directory exists
    IOStatus dir_st = EnsureNFSParentDir(nfs_dst);
    if (!dir_st.ok()) {
      std::cerr << "[NFS-RENAME] Failed to ensure parent dir: " << dir_st.ToString() << std::endl;
      return dir_st;
    }

    if (nfs_rename(nfs_ctx_, nfs_src.c_str(), nfs_dst.c_str()) != 0) {
      std::string err = nfs_get_error(nfs_ctx_);
      std::cerr << "[NFS-RENAME] FAILED: " << nfs_src << " -> " << nfs_dst
                << ", error: " << err << std::endl;
      return IOStatus::IOError("NFS rename failed: " + src + " -> " + dst +
                               ", error: " + err);
    }
    std::cout << "[NFS-RENAME] SUCCESS: " << nfs_src << " -> " << nfs_dst << std::endl;
    return IOStatus::OK();
  }

  // Mixed case (one in NFS, one local) should not happen
  std::cerr << "[NFS-RENAME] ERROR: Mixed NFS/local rename not supported" << std::endl;
  return IOStatus::IOError("Cannot rename between NFS and local: " + src + " -> " + dst);
}

// ============ Factory function ============

Status NewNFSFileSystemSimple(const std::string& nfs_url,
                              const std::string& local_prefix,
                              std::shared_ptr<FileSystem>* result) {
  try {
    auto base = FileSystem::Default();
    *result = std::make_shared<NFSFileSystemSimple>(nfs_url, local_prefix, base);
    return Status::OK();
  } catch (const std::exception& e) {
    return Status::IOError("Failed to create NFSFileSystemSimple: " +
                           std::string(e.what()));
  }
}

}  // namespace ROCKSDB_NAMESPACE
