# 远程 Bulk Load 设计文档

## 一、可行性评估

### 1.1 技术可行性分析

#### 1.1.1 RocksDB 底层支持 ✅ 完全可行

TendisPlus 使用的 RocksDB 版本已内置完整的 Bulk Load API：

| API | 头文件 | 用途 |
|-----|--------|------|
| `SstFileWriter` | `sst_file_writer.h` | 按顺序写入 KV 对，生成 SST 文件 |
| `IngestExternalFile()` | `db.h` | 将外部 SST 文件注入到 LSM-tree |
| `IngestExternalFiles()` | `db.h` | 多 CF 原子注入 |
| `IngestExternalFileOptions` | `options.h` | 注入行为配置（move_files / snapshot_consistency 等） |

**关键约束**：`SstFileWriter` 要求 Key 必须按 comparator 顺序递增添加，这对 Bulk Load 的数据预排序提出了要求。

#### 1.1.2 TendisPlus 现有基础设施 ✅ 可复用

| 已有能力 | 说明 | Bulk Load 复用方式 |
|----------|------|-------------------|
| `RocksKVStore::getBaseDB()` | 暴露底层 `rocksdb::DB*` | 直接调用 `IngestExternalFile()` |
| `getDataColumnFamilyHandle()` | 获取数据 CF 句柄 | 指定注入目标 CF |
| `SharedFileSystem` | NFS/HDFS 共享文件系统 | SST 文件在共享存储上传输 |
| Control Plane 架构 | 任务调度 + Worker 管理 | 复用统一任务模型和调度框架 |
| `TaskType::kBulkLoad` | 已定义枚举 | 类型系统已预留 |
| gRPC 通信框架 | Control Plane Proto | 扩展 Bulk Load RPC |

#### 1.1.3 现有迁移机制的对比分析

| 维度 | 现有迁移（KV 逐条写入） | Bulk Load（SST 文件注入） |
|------|------------------------|--------------------------|
| 数据路径 | TCP → RecordKey/Value 解码 → Transaction::setKV() → RocksDB Put | SstFileWriter → SST 文件 → IngestExternalFile → LSM-tree |
| 写放大 | **高**: 每条 KV 经过完整写入路径（WAL + MemTable + Flush + Compaction） | **极低**: SST 文件直接插入 L0/Lmax，跳过 WAL 和 MemTable |
| CPU 开销 | **高**: 序列化/反序列化 + 事务锁管理 | **低**: 只需排序和压缩 |
| 对线上干扰 | **大**: 占用写入带宽和 Compaction 资源 | **小**: 注入操作短暂，可控制并发 |
| 适用场景 | 在线 slot 迁移（需要增量追赶） | 离线批量导入（历史数据回灌/跨集群迁移） |

**结论**：两种机制互补，不替代。Bulk Load 针对大规模离线导入场景，远程化后可进一步隔离资源。

#### 1.1.4 风险与挑战

| 风险 | 等级 | 应对策略 |
|------|------|---------|
| TendisPlus 自定义编码兼容性 | 🟡 中 | SST 文件中的 KV 必须使用 RecordKey/RecordValue 编码格式 |
| Binlog 一致性 | 🟡 中 | Ingest 后需要补写 Binlog，保证主从同步正确 |
| 数据 CF + Binlog CF 双写 | 🟡 中 | 需要分别为两个 CF 生成 SST 并原子注入（使用 `IngestExternalFiles`） |
| Key Range 冲突 | 🟢 低 | `IngestExternalFileOptions::allow_blocking_flush = true` 可自动处理 |
| 序列号分配 | 🟢 低 | RocksDB `allow_global_seqno = true` 自动分配 |

**总体可行性评估：✅ 高度可行**

---

### 1.2 实现路径评估

#### 方案对比

**方案 A（推荐）：基于共享文件系统的远程 SST 生成与注入**
```
┌───────────┐   ①任务提交    ┌──────────────┐   ②任务派发   ┌────────────┐
│ TendisPlus│ ──────────── ▶│ Control Plane│ ────────── ▶│  Worker    │
│   Node    │               │  (Scheduler) │              │  (远程)    │
│           │               └──────────────┘              │            │
│           │   ⑤SST Ingest                               │ ③数据排序  │
│           │ ◀── 共享存储 ◀──────────────────────────────│ ④生成SST  │
└───────────┘                                             └────────────┘
                    共享文件系统 (NFS/HDFS)
```
- 优势：复用已有共享文件系统基础设施，与远程 Compaction 架构一致
- 劣势：依赖共享存储

**方案 B：基于网络传输的 SST 文件传输**
```
Worker 生成 SST → gRPC streaming 传输 → TendisPlus 本地写入 → Ingest
```
- 优势：不依赖共享存储
- 劣势：需要额外网络传输逻辑，增加实现复杂度

**结论**：采用方案 A，与远程 Compaction 共用基础设施，保持架构一致性。

---

## 二、系统设计

### 2.1 整体架构

```
┌─────────────────────────────────────────────────────────────────────┐
│                          Control Plane                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────────┐  │
│  │ TaskScheduler│  │WorkerManager │  │  BulkLoadCoordinator     │  │
│  │ (已有+扩展)   │  │   (已有)      │  │  (新增：管理分片+进度)    │  │
│  └──────┬───────┘  └──────┬───────┘  └───────────┬──────────────┘  │
│         │                 │                      │                  │
└─────────┼─────────────────┼──────────────────────┼──────────────────┘
          │                 │                      │
    ┌─────▼─────────────────▼──────────────────────▼──────────────────┐
    │                    gRPC 通信层                                    │
    └─────┬─────────────────┬──────────────────────┬──────────────────┘
          │                 │                      │
  ┌───────▼───────┐  ┌─────▼──────┐       ┌──────▼──────────────────┐
  │  TendisPlus   │  │  Worker 1  │  ...  │  Worker N               │
  │  ┌──────────┐ │  │ ┌────────┐ │       │ ┌────────────────────┐  │
  │  │BulkLoad  │ │  │ │SST     │ │       │ │BulkLoadExecutor    │  │
  │  │Ingester  │ │  │ │Builder │ │       │ │  - DataParser      │  │
  │  │  (新增)   │ │  │ │ (新增)  │ │       │ │  - KeySorter       │  │
  │  └──────────┘ │  │ └────────┘ │       │ │  - SstFileWriter   │  │
  │  │RocksKV   │ │  │            │       │ └────────────────────┘  │
  │  │Store     │ │  │            │       │                         │
  │  └──────────┘ │  └────────────┘       └─────────────────────────┘
  └───────────────┘
          ▲                 ▲                      ▲
          └─────────────────┴──────────────────────┘
                    共享文件系统 (NFS/HDFS)
```

### 2.2 核心数据模型设计

#### 2.2.1 Bulk Load 任务参数

```protobuf
// 在 control_plane.proto 中扩展

message BulkLoadTaskParams {
  // 数据源配置
  DataSourceType source_type = 1;    // 数据源类型
  string source_path = 2;            // 数据源路径/URI
  DataFormat data_format = 3;        // 数据格式

  // 分片配置
  ShardingStrategy sharding_strategy = 4;  // 分片策略
  int32 shard_count = 5;                   // 目标分片数
  repeated KeyRange key_ranges = 6;        // 手动指定的 key 范围

  // 目标配置
  uint32 target_store_id = 7;        // 目标 store ID
  string target_db_path = 8;         // 目标 DB 路径
  string shared_fs_uri = 9;          // 共享文件系统 URI

  // 执行配置
  CompressionType compression = 10;  // SST 压缩算法
  uint64 target_sst_size = 11;      // 目标 SST 文件大小
  bool generate_binlog = 12;         // 是否生成 binlog
  bool verify_checksum = 13;         // 注入前校验

  // 资源控制
  int64 rate_limit_bytes_per_sec = 14;  // 速率限制
  int32 max_concurrent_ingests = 15;    // 最大并发注入数
}

enum DataSourceType {
  DATA_SOURCE_KV_FILE = 0;       // 原始 KV 文件
  DATA_SOURCE_SST_FILE = 1;      // 已有 SST 文件（直接注入）
  DATA_SOURCE_RDB_FILE = 2;      // Redis RDB 文件
  DATA_SOURCE_TENDIS_DUMP = 3;   // TendisPlus dump 文件
  DATA_SOURCE_SNAPSHOT = 4;      // 另一个 TendisPlus 实例的快照
}

enum DataFormat {
  FORMAT_TENDISPLUS_ENCODED = 0;  // TendisPlus RecordKey/RecordValue 编码
  FORMAT_RAW_KV = 1;              // 原始 key-value（需要编码转换）
  FORMAT_SST_NATIVE = 2;         // 原生 RocksDB SST 文件
}

enum ShardingStrategy {
  SHARD_BY_KEY_RANGE = 0;     // 按 key 范围切分
  SHARD_BY_SLOT = 1;          // 按 TendisPlus slot 切分
  SHARD_BY_SIZE = 2;          // 按数据量均分
  SHARD_AUTO = 3;             // 自动选择
}

message KeyRange {
  bytes start_key = 1;     // 起始 key (inclusive)
  bytes end_key = 2;       // 结束 key (exclusive)
  uint32 slot_start = 3;   // 起始 slot
  uint32 slot_end = 4;     // 结束 slot
}
```

#### 2.2.2 Bulk Load 任务生命周期

```
                                ┌────────────┐
                                │  Created   │
                                └─────┬──────┘
                                      │ 数据扫描 & 分片规划
                                      ▼
                                ┌────────────┐
                            ┌───│ Planning   │
                            │   └─────┬──────┘
                            │         │ 分片完成，子任务入队
                            │         ▼
                            │   ┌────────────┐
                            │   │  Queued    │
                            │   └─────┬──────┘
                            │         │ 调度到 Workers
                            │         ▼
                            │   ┌────────────────┐
                            │   │ SST Generating │ ← Workers 并行生成 SST 文件
                            │   └─────┬──────────┘
                            │         │ 所有 SST 生成完毕
                            │         ▼
                            │   ┌────────────────┐
     失败回滚 ◀─────────────┤   │  Ingesting     │ ← TendisPlus 注入 SST 文件
                            │   └─────┬──────────┘
                            │         │ 注入完成 + 校验通过
                            │         ▼
                            │   ┌────────────┐
                            └──▶│ Completed  │
                                └────────────┘
```

### 2.3 关键模块设计

#### 2.3.1 BulkLoadExecutor（Worker 端 - SST 生成器）

```
职责：接收数据分片，排序后生成 SST 文件

输入：
  - 数据分片描述（KeyRange / Slot 范围）
  - 数据源路径（共享存储上的源文件 / 快照）
  - 目标 SST 配置（压缩/大小限制）

处理流程：
  ① 从数据源读取指定范围的 KV 对
  ② 转换为 TendisPlus RecordKey/RecordValue 编码
  ③ 按 RocksDB comparator 排序
  ④ 使用 SstFileWriter 写入 SST 文件到共享存储
  ⑤ 若需要 binlog，为 Binlog CF 生成对应 SST 文件
  ⑥ 计算校验和，上报元数据

输出：
  - SST 文件路径列表（共享存储上）
  - 每个 SST 的 KeyRange / 文件大小 / 校验和
  - 执行统计（耗时/处理行数）
```

#### 2.3.2 BulkLoadIngester（TendisPlus 端 - SST 注入器）

```
职责：将 Worker 生成的 SST 文件注入本地 RocksDB

输入：
  - SST 文件路径列表
  - 注入配置（move_files / verify_checksum 等）
  - 目标 store_id 和 CF 信息

处理流程：
  ① 校验 SST 文件完整性（checksum）
  ② 检查 Key Range 是否与本地数据冲突
  ③ 暂停/限制本地 Compaction（可选）
  ④ 调用 IngestExternalFile() / IngestExternalFiles() 注入
     - 数据 CF: 注入数据 SST
     - Binlog CF: 注入 binlog SST（若存在）
  ⑤ 验证注入结果
  ⑥ 更新 VersionMeta / Binlog 序列号
  ⑦ 恢复本地 Compaction

关键代码路径：
  RocksKVStore::getBaseDB() → rocksdb::DB*
  → IngestExternalFile(getDataColumnFamilyHandle(), sst_files, options)
```

#### 2.3.3 BulkLoadCoordinator（Control Plane 端 - 协调器）

```
职责：协调整个 Bulk Load 任务的执行

核心能力：
  ① 数据源扫描与分片规划
     - 估算数据量
     - 按策略切分为多个子任务
     - 每个子任务对应一个 KeyRange/SlotRange

  ② 子任务调度与进度管理
     - 为每个子任务分配 Worker
     - 跟踪所有子任务的 SST 生成进度
     - 支持子任务级别的重试

  ③ SST 注入编排
     - 所有子任务完成后，通知 TendisPlus 节点注入
     - 控制注入顺序和并发度
     - 协调 Binlog 一致性

  ④ 异常处理
     - Worker 故障：重分配未完成的子任务
     - 注入失败：回滚已注入的 SST（通过 Compaction 清理）
     - 超时处理：子任务级超时和整体超时
```

### 2.4 Proto 接口扩展设计

```protobuf
// 在 ControlPlaneService 中添加 Bulk Load RPC

service ControlPlaneService {
  // ... 已有 RPC ...

  // === Bulk Load API ===

  // 提交 Bulk Load 任务
  rpc SubmitBulkLoadTask(SubmitBulkLoadRequest) returns (SubmitBulkLoadResponse);

  // 查询 Bulk Load 任务状态
  rpc QueryBulkLoadStatus(QueryBulkLoadRequest) returns (QueryBulkLoadResponse);

  // 取消 Bulk Load 任务
  rpc CancelBulkLoad(CancelBulkLoadRequest) returns (CancelBulkLoadResponse);

  // TendisPlus 上报 SST 注入结果
  rpc ReportIngestResult(ReportIngestResultRequest) returns (ReportIngestResultResponse);
}

service CSAService {
  // ... 已有 RPC ...

  // Worker 执行 Bulk Load 分片（SST 生成）
  rpc ExecuteBulkLoadShard(BulkLoadShardRequest) returns (BulkLoadShardResponse);
}
```

### 2.5 与 Compaction 的协调设计

远程 Bulk Load 和远程 Compaction 共存时的资源协调策略：

```
调度优先级矩阵:
  Compaction (高紧急 - L0堆积) > BulkLoad (正常) > Compaction (低紧急)

Worker 资源分配:
  ┌──────────────────────────────────────┐
  │          Worker 资源池               │
  │  ┌─────────────┐ ┌────────────────┐ │
  │  │ Compaction  │ │  Bulk Load     │ │
  │  │ 预留:  60%   │ │  预留: 40%     │ │
  │  │ (可抢占)     │ │  (可被抢占)     │ │
  │  └─────────────┘ └────────────────┘ │
  └──────────────────────────────────────┘

Ingest 期间 Compaction 节流:
  - SST Ingest 进行中 → 暂停本地 Compaction 触发
  - Ingest 完成后 → 允许 Compaction 处理新 L0 文件
```

---

## 三、实现计划（分 3 次提交）

### 提交 3.1：Bulk Load 任务模型 + Proto 扩展 + 调度集成
- 在 `task_model.h` 中添加 `BulkLoadTaskParams`
- 在 `control_plane.proto` 中添加 Bulk Load 消息和 RPC
- 扩展 `TaskScheduler` 支持 Bulk Load 任务排队/调度
- 扩展 `ControlPlane` 添加 Bulk Load API 实现
- 实现 `BulkLoadCoordinator` 框架

### 提交 3.2：Worker 端 SST 生成（BulkLoadExecutor）
- 实现 `BulkLoadExecutor` 类（数据解析 → 排序 → SST 生成）
- 支持 TendisPlus RecordKey/RecordValue 编码格式
- 支持 Data CF + Binlog CF 的 SST 文件分别生成
- 在 `control_plane_worker.cc` 中集成 Bulk Load 执行能力
- 在 CSA Server 中添加 Bulk Load 分片执行能力

### 提交 3.3：TendisPlus 端 SST 注入（BulkLoadIngester）
- 在 `RocksKVStore` 中添加 `IngestExternalSSTFiles()` 方法
- 实现 `BulkLoadIngester` 类（校验 → 注入 → 验证）
- 实现 Binlog 一致性保障（注入后更新 binlog 序列号）
- 实现异常回滚机制
- 端到端集成测试

---

## 四、论文标题呼应性分析

### 论文标题
**《支持远程后台任务的分布式存储系统设计与实现》**
**Design and Implementation of a Distributed Storage System Supporting Remote Background Tasks**

### 标题关键词分解与呼应

| 关键词 | 已有实现 | Bulk Load 的呼应 |
|--------|---------|-----------------|
| **远程** | ✅ 远程 Compaction | ✅ 远程 Bulk Load（SST 在 Worker 生成，远程执行） |
| **后台任务** | ✅ Compaction 作为后台任务 | ✅ Bulk Load 作为后台任务纳入统一模型 |
| **分布式存储系统** | ✅ TendisPlus 分布式 KV | ✅ 跨节点 SST 生成与注入 |
| **设计与实现** | ✅ 三平面架构 | ✅ 完整的设计 + 原型实现 |

### 论文标题的深层呼应分析

#### ① "远程后台任务" = 不止一种任务
论文标题用的是 **"远程后台任务"**（复数概念），而不是 "远程 Compaction"。这意味着：
- 如果**只实现远程 Compaction**，标题显得过于宽泛，名不副实
- **Bulk Load 作为第二种远程后台任务**，恰好补全了 "任务" 的多样性
- 两种任务共用统一模型、统一调度、统一观测，证明架构的**通用性和可扩展性**

#### ② "统一执行架构" 需要多种任务来验证
开题报告核心创新点之一是 **"统一后台任务模型与状态机"**：
- 只有 Compaction 一种任务 → 无法证明 "统一" 的价值
- Compaction + Bulk Load 两种任务 → 证明模型足够通用
- 两种任务的调度策略差异 → 证明调度框架的灵活性

#### ③ 实验评估需要多场景
开题报告承诺了 4 类实验场景（图4-19）：
- 高写入压力场景 → 远程 Compaction 场景 ✅
- 读写混合场景 → 远程 Compaction 场景 ✅
- **大规模 Bulk Load 场景** → 必须实现 Bulk Load ❗
- 多任务混合场景 → Compaction + Bulk Load 并发 ❗

### 结论

| 评估维度 | 仅远程 Compaction | + 远程 Bulk Load |
|---------|------------------|-----------------|
| 标题匹配度 | 🟡 60% (只有一种后台任务) | ✅ 90%+ (多种后台任务) |
| 创新点覆盖 | 🟡 部分 (无法证明"统一"价值) | ✅ 完整 |
| 实验场景覆盖 | 🔴 50% (缺2个场景) | ✅ 100% |
| 答辩说服力 | 🟡 中等 | ✅ 强 |

**⚠️ 核心结论：Bulk Load 不是可选项，是论文标题和开题承诺的必要组成部分。**

没有 Bulk Load，论文标题中的 "远程后台任务"（泛化概念）就退化为 "远程 Compaction"（单一任务），统一执行架构的通用性无法验证，开题报告中 4 类实验场景有 2 类无法执行。

---

## 五、技术风险评估与缓解

### 5.1 核心风险

| 编号 | 风险描述 | 等级 | 缓解策略 |
|------|---------|------|---------|
| R1 | TendisPlus RecordKey/RecordValue 编码复杂性 | 🟡中 | 复用现有 `RecordKey::encode()` / `RecordValue::encode()` 接口 |
| R2 | Binlog CF 同步问题 | 🟡中 | Ingest 后追加写入 binlog 元数据，不在 SST 中包含 binlog |
| R3 | 主从复制一致性 | 🟡中 | 方案简化：Ingest 操作本身被记录为 binlog 事件，从节点通过全量同步获取 |
| R4 | Slot 路由兼容性 | 🟢低 | SST 文件按 slot 范围切分，注入时与本地 slot 映射对齐 |
| R5 | 工作量风险 | 🟡中 | 分三次提交，先实现最简路径（已有 SST 直接注入），再逐步增强 |

### 5.2 简化策略（降低实现复杂度）

**Phase 1 最简实现**（优先完成，论文可用）：
- 仅支持 `DATA_SOURCE_SST_FILE` 类型（外部提供已排序的 SST）
- 仅支持数据 CF 注入（暂不处理 Binlog CF）
- 分片策略固定为 SHARD_BY_KEY_RANGE
- Worker 直接使用 SstFileWriter 生成标准 SST

**Phase 2 增强实现**（有余力时）：
- 支持从 TendisPlus dump/snapshot 导入
- 支持 Binlog CF 同步生成
- 支持自动分片策略

---

## 六、与开题报告的对应关系

| 开题报告章节 | 核心内容 | Bulk Load 设计对应 |
|-------------|---------|-------------------|
| 4.3.4 统一任务模型 | 统一描述 Compaction + Bulk Load | BulkLoadTaskParams + 统一 TaskInfo |
| 4.3.6 远程 Bulk Load | SST 生成 + Ingest 流程 | BulkLoadExecutor + BulkLoadIngester |
| 4.3.7 统一调度 | 多类型任务优先级调度 | 调度器扩展 + 资源协调 |
| 4.3.8 观测平面 | Bulk Load 指标监控 | Observatory 扩展 |
| 4.3.9 实验评估 | 大规模 Bulk Load 场景 | 性能测试场景设计 |
| 图4-11 | 远程 Bulk Load 执行流程 | 完整对应 |
| 图4-12 | SST 文件 Ingest 流程 | 完整对应 |
