// The simplified version of NFS FileSystem only redirects SST data files to NFS
// Metadata operations such as directories, locks, logs, etc. still use the local file system
#pragma once

#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/status.h"

#if __has_include(<nfsc/libnfs.h>)
#include <nfsc/libnfs.h>
#elif __has_include(<libnfs.h>)
#include <libnfs.h>
#else
#error "libnfs header not found"
#endif

#include <mutex>

namespace ROCKSDB_NAMESPACE {

// A simplified version of the NFS FileSystem
// Only rewrite the read and write operations of data files (.sst, .blob).
// All other operations (directories, locks, logs) use the local file system

class NFSFileSystemSimple : public FileSystemWrapper {
 public:
  NFSFileSystemSimple(const std::string& nfs_url,
                      const std::string& local_prefix,
                      const std::shared_ptr<FileSystem>& base);
  ~NFSFileSystemSimple() override;

  const char* Name() const override { return "NFSFileSystemSimple"; }

  // Only these 3 core methods are rewritten
  IOStatus NewRandomAccessFile(const std::string& fname,
                               const FileOptions& options,
                               std::unique_ptr<FSRandomAccessFile>* result,
                               IODebugContext* dbg) override;

  IOStatus NewWritableFile(const std::string& fname,
                           const FileOptions& options,
                           std::unique_ptr<FSWritableFile>* result,
                           IODebugContext* dbg) override;

  IOStatus DeleteFile(const std::string& fname,
                      const IOOptions& options,
                      IODebugContext* dbg) override;

  // File metadata manipulation (required for SST files)
  IOStatus GetFileSize(const std::string& fname,
                       const IOOptions& options,
                       uint64_t* file_size,
                       IODebugContext* dbg) override;

  IOStatus FileExists(const std::string& fname,
                      const IOOptions& options,
                      IODebugContext* dbg) override;

  // Rename the file (required after compaction is complete)
  IOStatus RenameFile(const std::string& src,
                      const std::string& dst,
                      const IOOptions& options,
                      IODebugContext* dbg) override;

 private:
  std::string nfs_url_;
  std::string local_prefix_;
  struct nfs_context* nfs_ctx_;
  mutable std::mutex nfs_mutex_;

  void InitNFSContext();
  
  // Only .sst and .blob files go NFS
  bool IsDataFile(const std::string& path) const;
  std::string ConvertToNFSPath(const std::string& path) const;
  IOStatus EnsureNFSParentDir(const std::string& nfs_path);
};

// Factory function
Status NewNFSFileSystemSimple(const std::string& nfs_url,
                              const std::string& local_prefix,
                              std::shared_ptr<FileSystem>* result);

}  // namespace ROCKSDB_NAMESPACE
