#!/bin/bash

# 增强版 NFS 设置脚本
set -e

echo "=== 检查系统环境 ==="
# 检查是否为 root 或有 sudo 权限
# 检查系统版本和包管理器

echo "=== 安装 NFS ==="
# 根据系统选择包管理器 (yum/apt/dnf)
sudo yum install -y nfs-utils

# echo "=== 配置防火墙 ==="
# # 开放 NFS 相关端口
# sudo firewall-cmd --permanent --add-service=nfs
# sudo firewall-cmd --permanent --add-service=mountd  
# sudo firewall-cmd --permanent --add-service=rpc-bind
# sudo firewall-cmd --reload

echo "=== 创建共享目录 ==="
sudo mkdir -p /shared/rocksdb
CURRENT_USER=$(id -un)
CURRENT_GROUP=$(id -gn)
sudo chown -R $CURRENT_USER:$CURRENT_GROUP /shared/rocksdb
chmod 755 /shared/rocksdb

echo "=== 配置 NFS 共享 ==="
# 使用更宽松的配置，添加 insecure 选项
echo "/shared/rocksdb *(rw,sync,no_root_squash,no_subtree_check,insecure)" | sudo tee /etc/exports

echo "=== 启动 NFS 服务 ==="
sudo systemctl start rpcbind nfs-server
sudo systemctl enable rpcbind nfs-server

# 等待服务启动
sleep 2

echo "=== 导出共享 ==="
sudo exportfs -arv

echo "=== 验证 NFS 导出 ==="
showmount -e localhost

echo "=== 创建挂载点 ==="
sudo mkdir -p /mnt/rocksdb

echo "=== 挂载 NFS ==="
sudo mount -t nfs localhost:/shared/rocksdb /mnt/rocksdb

echo "=== 验证挂载 ==="
df -h | grep rocksdb
mount | grep rocksdb

echo "=== 测试读写权限 ==="
echo 'NFS test file' > /shared/rocksdb/test.txt
cat /mnt/rocksdb/test.txt
rm /mnt/rocksdb/test.txt

echo ""
echo "✅ NFS 设置完成！"
echo "   服务端路径: /shared/rocksdb"  
echo "   客户端路径: /mnt/rocksdb"
echo "   所有者: $CURRENT_USER:$CURRENT_GROUP"
echo ""
echo "NFS 服务状态："
sudo systemctl status nfs-server --no-pager -l
