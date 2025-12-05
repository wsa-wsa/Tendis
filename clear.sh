# 停止 Primary 和 CSA Server
# 清理 NFS 上的数据
rm -rf /shared/rocksdb/0/*
rm -rf /shared/rocksdb/catalog/*

# 清理本地 RocksDB 元数据（MANIFEST, CURRENT, WAL 等）
rm -rf ./home/db/0/*
rm -rf ./home/db/catalog/*

# 重新启动
