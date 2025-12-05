#!/bin/bash
set -e

echo "=== 关闭和清理所有NFS设置 ==="

# 1. 卸载NFS挂载点
echo "卸载NFS挂载点..."
sudo umount -f /mnt/rocksdb 2>/dev/null || true

# 2. 停止NFS服务
echo "停止NFS服务..."
sudo systemctl stop nfs-server nfs-mountd rpcbind 2>/dev/null || true

# 3. 禁用NFS服务开机自启
echo "禁用NFS服务..."
sudo systemctl disable nfs-server nfs-mountd rpcbind 2>/dev/null || true

# 4. 恢复/etc/exports文件
echo "恢复/etc/exports文件..."
if [ -f /etc/exports.backup ]; then
    sudo cp /etc/exports.backup /etc/exports
    echo "✅ 已从备份恢复/etc/exports"
else
    sudo tee /etc/exports > /dev/null << 'EOF'
# /etc/exports: the access control list for filesystems which may be exported
#               to NFS clients.  See exports(5).
EOF
    echo "✅ 已清空/etc/exports"
fi

# 5. 恢复/etc/fstab文件（移除NFS相关条目）
echo "清理/etc/fstab文件..."
sudo cp /etc/fstab /etc/fstab.backup.nfs-cleanup
sudo sed -i '\|/mnt/rocksdb|d' /etc/fstab
sudo sed -i '\|127.0.0.1:/shared/rocksdb|d' /etc/fstab
echo "✅ 已清理/etc/fstab中的NFS条目"

# 6. 杀死可能残留的NFS进程
echo "清理残留进程..."
sudo pkill -f nfsd 2>/dev/null || true
sudo pkill -f rpc.mountd 2>/dev/null || true
sudo pkill -f rpcbind 2>/dev/null || true

# 7. 重新加载systemd配置
echo "重新加载systemd配置..."
sudo systemctl daemon-reload

# 8. 检查清理结果
echo "=== 清理完成检查 ==="
echo "当前挂载点状态:"
mount | grep rocksdb || echo "✅ 无rocksdb相关挂载"

echo "NFS服务状态:"
sudo systemctl status nfs-server 2>/dev/null | grep -E "Active:|Loaded:" || echo "✅ NFS服务已停止"

echo "端口监听状态:"
sudo netstat -tlnp 2>/dev/null | grep -E ":2049|:111|:892" || echo "✅ 无NFS相关端口监听"

# 9. 可选：删除创建的目录（根据需求决定是否执行）
echo "=== 目录清理（可选）==="
read -p "是否删除创建的目录 /shared/rocksdb 和 /mnt/rocksdb？(y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    sudo rm -rf /shared/rocksdb
    sudo rm -rf /mnt/rocksdb
    echo "✅ 已删除创建的目录"
else
    echo "⚠️  保留目录 /shared/rocksdb 和 /mnt/rocksdb"
    echo "   如需手动删除，可执行: sudo rm -rf /shared/rocksdb /mnt/rocksdb"
fi

echo ""
echo "✅ NFS设置已完全关闭并恢复原样"
echo "系统已回到初始状态，所有NFS相关配置已清理"