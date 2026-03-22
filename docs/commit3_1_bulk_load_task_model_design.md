# Commit 3.1 设计文档：Bulk Load 任务模型 + Proto 扩展 + 调度集成

## 一、概述

### 1.1 目标
在 CaaS-LSM 架构中引入第二种远程后台任务类型 —— **Remote Bulk Load**，实现：
1. 统一任务模型的 Bulk Load 参数扩展
2. gRPC Proto 的 Bulk Load 消息和服务定义
3. TaskScheduler 的多类型任务感知调度
4. ControlPlane 的 Bulk Load API
5. BulkLoadCoordinator 协调器框架

### 1.2 与论文的对应关系

| 论文章节 | 核心内容 | 本 Commit 实现 |
|---------|---------|---------------|
| 4.3.4 统一任务模型 | TaskType + BulkLoadTaskParams | task_model.h 完整类型体系 |
| 4.3.7 统一调度 | 多类型任务优先级调度 | TaskScheduler Bulk Load 队列 |
| 2.4 Proto 扩展 | Bulk Load gRPC API | control_plane.proto 完整定义 |
| 2.3.3 BulkLoadCoordinator | 协调器框架 | BulkLoadCoordinatorImpl |

---

## 二、任务模型设计

### 2.1 Bulk Load 类型体系

```
DataSourceType          DataFormat           ShardingStrategy
├─ kKVFile              ├─ kTendisplusEncoded ├─ kByKeyRange
├─ kSSTFile             ├─ kRawKV             ├─ kBySlot
├─ kRDBFile             └─ kSSTNative         ├─ kBySize
├─ kTendisDump                                └─ kAuto
└─ kSnapshot

CompressionType         BulkLoadPhase
├─ kNone                ├─ kCreated
├─ kSnappy              ├─ kPlanning
├─ kZlib                ├─ kQueued
├─ kLZ4                 ├─ kSSTGenerating
└─ kZSTD                ├─ kIngesting
                        ├─ kCompleted
                        ├─ kFailed
                        └─ kCancelled
```

### 2.2 核心结构体

```
BulkLoadTaskParams
├── 数据源配置
│   ├── source_type: DataSourceType
│   ├── source_path: string
│   └── data_format: DataFormat
├── 分片配置
│   ├── sharding_strategy: ShardingStrategy
│   ├── shard_count: int32
│   └── key_ranges: vector<KeyRange>
├── 目标配置
│   ├── target_store_id: uint32
│   ├── target_db_path: string
│   ├── shared_fs_uri: string
│   └── sst_output_dir: string
├── 执行配置
│   ├── compression: CompressionType
│   ├── target_sst_size: uint64 (64MB)
│   ├── generate_binlog: bool
│   ├── verify_checksum: bool
│   └── timeout_sec: uint32 (7200)
├── 资源控制
│   ├── rate_limit_bytes_per_sec: int64
│   └── max_concurrent_ingests: int32
└── 执行状态
    ├── phase: BulkLoadPhase
    ├── shards: vector<BulkLoadShardInfo>
    ├── completed_shards / failed_shards
    └── all_sst_files: vector<SSTFileMetadata>
```

### 2.3 TaskInfo 扩展

```cpp
struct TaskInfo {
  TaskType type;                  // kCompaction 或 kBulkLoad
  CompactionTaskParams params;    // Compaction 参数
  BulkLoadTaskParams bulk_load_params;  // Bulk Load 参数 (新增)
  // ...
};

struct TaskResult {
  // ... 原有字段 ...
  vector<SSTFileMetadata> sst_files;     // Bulk Load SST 文件列表 (新增)
  uint64_t total_rows_processed;          // 处理行数 (新增)
  uint32_t sst_files_count;               // SST 文件数 (新增)
};

struct TaskFilter {
  optional<TaskType> task_type;           // 按类型过滤 (新增)
  // ...
};
```

---

## 三、Proto 扩展设计

### 3.1 新增枚举类型

| Proto 枚举 | 对应 C++ 枚举 | 值数量 |
|-----------|--------------|--------|
| `DataSourceType` | `DataSourceType` | 5 |
| `DataFormat` | `DataFormat` | 3 |
| `ShardingStrategy` | `ShardingStrategy` | 4 |
| `CompressionType` | `CompressionType` | 5 |
| `BulkLoadPhase` | `BulkLoadPhase` | 8 |

### 3.2 新增消息类型

| 消息 | 用途 |
|------|------|
| `BulkLoadKeyRange` | Key 范围描述 |
| `SSTFileMetadataProto` | SST 文件元数据 |
| `BulkLoadShardInfoProto` | 分片信息 |
| `BulkLoadTaskParamsProto` | Bulk Load 完整参数 |
| `SubmitBulkLoadRequest/Response` | 提交任务 |
| `QueryBulkLoadRequest/Response` | 查询状态（含分片进度） |
| `CancelBulkLoadRequest/Response` | 取消任务 |
| `ReportIngestResultRequest/Response` | SST 注入结果上报 |
| `BulkLoadShardRequest/Response` | CSA 执行分片任务 |

### 3.3 新增 RPC

**ControlPlaneService（4 个新 RPC）：**
```protobuf
rpc SubmitBulkLoadTask(SubmitBulkLoadRequest) returns (SubmitBulkLoadResponse);
rpc QueryBulkLoadStatus(QueryBulkLoadRequest) returns (QueryBulkLoadResponse);
rpc CancelBulkLoad(CancelBulkLoadRequest) returns (CancelBulkLoadResponse);
rpc ReportIngestResult(ReportIngestResultRequest) returns (ReportIngestResultResponse);
```

**CSAService（1 个新 RPC）：**
```protobuf
rpc ExecuteBulkLoadShard(BulkLoadShardRequest) returns (BulkLoadShardResponse);
```

---

## 四、调度器集成设计

### 4.1 双队列架构

```
TaskScheduler
├── pending_queue_ (Compaction)        ← CaaS-LSM 优先级队列
│   排序: start_level → score → submit_time
│
├── bulk_load_pending_queue_ (Bulk Load) ← 优先级 + FIFO 队列
│   排序: priority → submit_time
│
├── running_tasks_ (混合)              ← 所有运行中的任务
│
└── bulk_load_tasks_ (Bulk Load 父任务索引)
```

### 4.2 调度优先级矩阵

```
高紧急 Compaction (L0 堆积)  ──── 最高优先级
      ↓
Bulk Load 分片                ──── 中等优先级
      ↓
低紧急 Compaction (高层级)    ──── 较低优先级
```

### 4.3 DoSchedule 流程

```
DoSchedule()
├─ ① 获取可用 Workers（过滤内存不足的）
├─ ② 调度 Compaction 队列（CaaS-LSM 优先级）
│   ├─ 检查降级条件
│   └─ 按 start_level / score 排序
├─ ③ 调度 Bulk Load 队列（剩余 Worker 资源）
│   └─ 按 priority / FIFO 排序
└─ ④ 返回调度决策列表
```

### 4.4 ExecuteDecisions 类型感知

```
ExecuteDecisions(decisions)
├─ 对每个决策:
│   ├─ AssignTask(worker_id, task_id)
│   ├─ 若 task.type == kBulkLoad:
│   │   └─ DistributeBulkLoadShardToCSA()  [Commit 3.2 完善]
│   └─ 若 task.type == kCompaction:
│       └─ DistributeTaskToCSA()            [已实现]
```

---

## 五、BulkLoadCoordinator 设计

### 5.1 职责

```
BulkLoadCoordinator
├── PlanShards()          ─── 分片规划（数据源扫描 + 均匀切分）
├── SubmitShardTasks()    ─── 将分片作为子任务提交到调度器
└── OnIngestResult()      ─── 处理 TendisPlus 上报的 SST 注入结果
```

### 5.2 Bulk Load 完整生命周期

```
SubmitBulkLoadTask()
    │
    ▼
BulkLoadCoordinator::PlanShards()
    │ → phase: Created → Planning → Queued
    ▼
BulkLoadCoordinator::SubmitShardTasks()
    │ → 每个 shard 作为独立 TaskInfo 提交到 bulk_load_pending_queue_
    ▼
TaskScheduler::DoSchedule()
    │ → 从 bulk_load_pending_queue_ 取出分片，分配给 Worker
    ▼
DistributeBulkLoadShardToCSA()         [Commit 3.2: 实际 gRPC 调用]
    │ → phase: SSTGenerating
    ▼
Worker 生成 SST 文件 → ReportTaskResult  [Commit 3.2]
    │ → phase: Ingesting
    ▼
TendisPlus IngestExternalFile → ReportIngestResult  [Commit 3.3]
    │ → phase: Completed / Failed
    ▼
Done
```

### 5.3 实现方式

BulkLoadCoordinatorImpl 作为 ControlPlane 的内部类实现（位于 `control_plane.cc`），
可直接访问 Scheduler 和 WorkerManager。独立的接口定义在 `bulk_load_coordinator.h` 中，
供外部模块（如 Observatory）使用。

---

## 六、修改文件清单

| 文件 | 修改类型 | 修改内容 |
|------|---------|---------|
| `task_model.h` | 修改 | 新增 6 个枚举、5 个结构体、TaskInfo/TaskResult/TaskFilter 扩展 |
| `task_model.cc` | 修改 | TaskFilter::Matches() 增加 task_type 过滤 |
| `control_plane.proto` | 修改 | 5 个新枚举 + 10 个新消息 + 5 个新 RPC |
| `task_scheduler.h` | 修改 | 新增 Bulk Load 方法声明 + 双队列结构 |
| `task_scheduler.cc` | 修改 | SubmitBulkLoadTask/Shard + DoSchedule Bulk Load 调度 + DistributeBulkLoadShardToCSA |
| `control_plane.h` | 修改 | 新增 4 个 Bulk Load API 方法 + BulkLoadCoordinatorImpl 前向声明 |
| `control_plane.cc` | 修改 | 4 个 gRPC 处理器 + 4 个 API 实现 + BulkLoadCoordinatorImpl 完整实现 |
| `bulk_load_coordinator.h` | **新增** | BulkLoadCoordinator 接口定义 + BulkLoadTaskSummary |
| `bulk_load_coordinator.cc` | **新增** | 占位文件（实现在 control_plane.cc 内嵌） |
| `CMakeLists.txt` | 修改 | 添加 bulk_load_coordinator.cc 到 control_plane_core |

---

## 七、下一步（Commit 3.2）

Commit 3.2 将在 Worker 端实现 `BulkLoadExecutor`：
- 实现 `ExecuteBulkLoadShard` gRPC 处理
- 数据解析 → RecordKey/RecordValue 编码转换
- Key 排序 → SstFileWriter 生成 SST
- SST 写入共享存储 + 校验和计算
- 结果上报
