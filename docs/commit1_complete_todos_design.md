# Commit 1: 补全已有 TODO 设计文档

> 文档版本: v1.0  
> 日期: 2026-03-22  
> 作者: CaaS-LSM 开发组  
> 对应论文章节: 4.3.4 统一任务模型与状态机, 4.3.5 远程 Compaction 执行机制

---

## 1. 概述

### 1.1 背景

在 CaaS-LSM 架构的现有实现中，存在多处 TODO 占位代码，这些代码框架已搭建但核心逻辑未实现。本次提交（Commit 1）的目标是**补全所有已有 TODO**，使已有代码从"框架可编译"提升到"功能可执行"。

### 1.2 改动范围

本次提交涉及 **6 个文件**，修改 **5 处 TODO** 和 **1 项状态机增强**：

| 文件 | 改动内容 | 类型 |
|------|----------|------|
| `control_plane_worker.cc` | 补全共享文件系统环境设置 | 新增实现 |
| `task_model.h` | 添加 `kRetrying` 状态 | 状态机增强 |
| `task_scheduler.cc` | 补全 `DistributeTaskToCSA()` | 新增实现 |
| `task_scheduler.cc` | 更新 `HandleRetry()` 使用新状态 | 逻辑优化 |
| `worker_manager.cc` | 补全 `DistributeJobToCSA()` gRPC 调用 | 新增实现 |
| `worker_manager.cc` | 补全 `CancelCSATask()` gRPC 调用 | 新增实现 |
| `control_plane.proto` | 添加 `TASK_RETRYING` 枚举值 | 协议扩展 |

---

## 2. 设计详情

### 2.1 共享文件系统环境设置 (control_plane_worker.cc)

#### 2.1.1 问题描述

`ControlPlaneWorker::ExecuteCompaction()` 方法调用 `DB::OpenAndCompact()` 执行远程 Compaction，但未设置共享文件系统环境。这导致 Worker 无法通过 NFS/HDFS 等共享存储读写 SST 文件。

#### 2.1.2 解决方案

引入 `SharedFileSystemCache` 单例类（复用 `csa_server.cc` 中的设计模式），在 `ExecuteCompaction()` 中根据任务携带的 `shared_fs_uri` 创建对应的文件系统环境。

```
┌─────────────────────────────────────────────────────────┐
│              ExecuteCompaction(task)                      │
│                                                         │
│  1. task.shared_fs_uri 非空?                              │
│     ├── Yes → SharedFileSystemCache::GetOrCreateEnvFromURI()│
│     │         ├── 缓存命中 → 复用已有 Env                    │
│     │         └── 缓存未命中 → CreateSharedFileSystem()     │
│     │                        → NewCompositeEnv()           │
│     │                        → 缓存并返回                   │
│     └── No  → 使用默认 Env                                 │
│                                                         │
│  2. options_override.env = shared_env                     │
│  3. DB::OpenAndCompact(... options_override)              │
└─────────────────────────────────────────────────────────┘
```

#### 2.1.3 关键设计决策

- **单例模式**: `SharedFileSystemCache` 采用 Meyers 单例，线程安全，生命周期跟进程
- **缓存策略**: 以 `"URI:" + uri` 作为缓存键，双重检查锁避免重复创建
- **URI 模式**: Worker 端使用纯 URI 模式（`local_prefix` 为空），与 `csa_server.cc` 保持一致
- **容错**: 如果共享文件系统创建失败，打印 WARNING 并回退到默认 Env，不阻塞任务执行

### 2.2 任务状态机增强: kRetrying 状态 (task_model.h)

#### 2.2.1 问题描述

开题报告 4.3.4 节定义的状态机包含 `Retrying` 状态，但现有实现中缺失。原实现中，重试时直接从 `Failed/Timeout` 跳转到 `Pending`，缺少中间状态记录。

#### 2.2.2 增强后的状态机

```
              ┌──────────┐
              │ Pending  │
              └────┬─────┘
                   │ 调度分配
              ┌────▼─────┐
              │ Assigned │
              └────┬─────┘
                   │ Worker 开始执行
              ┌────▼─────┐
              │ Running  │
              └────┬─────┘
                   │
          ┌────────┼────────┐
          ▼        ▼        ▼
     ┌─────────┐ ┌─────┐ ┌───────┐
     │Completed│ │Failed│ │Timeout│
     └─────────┘ └──┬──┘ └──┬────┘
                    │       │
                    └───┬───┘
                        │ CanRetry()?
                   ┌────▼────┐
                   │Retrying │  ← 新增状态
                   └────┬────┘
                        │ 重新入队
                   ┌────▼────┐
                   │ Pending │
                   └─────────┘

     任意非终态 ──取消──▶ Cancelled
```

#### 2.2.3 修改点

1. **`TaskStatus` 枚举**: 添加 `kRetrying = 8`
2. **`TaskStatusToString()`**: 添加 `"Retrying"` 映射
3. **`IsTerminal()`**: `kRetrying` **不是** 终态（任务还在生命周期内）
4. **`CanRetry()`**: 扩展条件包含 `kRetrying` 状态
5. **`IsRetrying()`**: 新增辅助方法
6. **`HandleRetry()`**: 先标记 `kRetrying`，再转入 `kPending`
7. **Proto `TaskStatus`**: 添加 `TASK_RETRYING = 8`

### 2.3 推送模式 gRPC 调用实现 (worker_manager.cc)

#### 2.3.1 DistributeJobToCSA()

CaaS-LSM 架构支持两种任务分发模式：
- **拉取模式**: Worker 主动通过 `FetchTask` RPC 获取任务（已实现）
- **推送模式**: Control Plane 主动通过 `DistributeCompactionJob` RPC 推送任务到 Worker（本次补全）

```
┌──────────────┐         DistributeCompactionJob         ┌──────────────┐
│ Control Plane│ ──────────────────────────────────────▶ │  CSA Worker  │
│              │                                         │              │
│ WorkerManager│  DistributeJobRequest:                  │  CSAService  │
│              │   - task_id                              │              │
│              │   - compaction_args                      │              │
│              │   - compaction_addition_info             │              │
│              │   - shared_fs_uri                        │              │
│              │   - start_level, score                   │              │
│              │                                         │              │
│              │  ◀─── DistributeJobResponse:             │              │
│              │        - accepted (bool)                 │              │
│              │        - error_message                   │              │
└──────────────┘                                         └──────────────┘
```

**实现要点**:
- 使用 `GetOrCreateCSAChannel()` 获取到 Worker 的 gRPC 连接（带缓存）
- 创建 `CSAService::Stub` 并发起同步调用
- 设置 30 秒超时，避免 RPC 阻塞
- 完善的错误处理：连接失败、CSA 拒绝等场景

#### 2.3.2 CancelCSATask()

当 Control Plane 需要取消 Worker 上正在执行的任务时（如任务被用户取消、Worker 离线后重调度等），通过 `CancelRunningTask` RPC 通知 Worker。

**实现要点**:
- 同样使用缓存的 gRPC Channel
- 设置 10 秒超时（取消操作应快速完成）
- 携带取消原因 (`"Cancelled by Control Plane"`)

### 2.4 调度器任务分发 (task_scheduler.cc)

#### 2.4.1 DistributeTaskToCSA()

补全调度器中推送模式的最后一环：将调度决策转化为实际的 gRPC 调用。

**调用链**:
```
TaskScheduler::DistributeTaskToCSA()
    ├── 参数验证 (worker 存在、在线、内存充足)  ← 已有
    └── worker_manager_->DistributeJobToCSA()     ← 新增
         └── CSAService::Stub::DistributeCompactionJob()  ← 新增
```

---

## 3. 文件改动清单

### 3.1 control_plane_worker.cc

| 行号范围 | 改动类型 | 描述 |
|----------|----------|------|
| 头部 | 新增 include | 添加 `<mutex>`, `<unordered_map>`, `rocksdb/env.h`, `shared_filesystem.h` |
| 新增类 | 新增 | `SharedFileSystemCache` 单例类 (~90行) |
| ExecuteCompaction() | 替换 TODO | 设置 `options_override.env` (~20行) |

### 3.2 task_model.h

| 行号范围 | 改动类型 | 描述 |
|----------|----------|------|
| TaskStatus enum | 新增枚举值 | `kRetrying = 8` |
| TaskStatusToString() | 新增 case | `"Retrying"` |
| CanRetry() | 扩展条件 | 包含 `kRetrying` |
| IsRetrying() | 新增方法 | 辅助判断方法 |

### 3.3 task_scheduler.cc

| 行号范围 | 改动类型 | 描述 |
|----------|----------|------|
| HandleRetry() | 重写 | 先设置 `kRetrying` 再转 `kPending`；从运行队列移除 |
| DistributeTaskToCSA() | 替换 TODO | 调用 `worker_manager_->DistributeJobToCSA()` |

### 3.4 worker_manager.cc

| 行号范围 | 改动类型 | 描述 |
|----------|----------|------|
| 头部 | 新增 include | `control_plane.grpc.pb.h` |
| DistributeJobToCSA() | 替换 TODO | 完整 gRPC 调用 (~45行) |
| CancelCSATask() | 替换 TODO | 完整 gRPC 调用 (~50行) |

### 3.5 control_plane.proto

| 行号范围 | 改动类型 | 描述 |
|----------|----------|------|
| TaskStatus enum | 新增枚举值 | `TASK_RETRYING = 8` |

---

## 4. 与开题报告的对应关系

| 开题报告章节 | 要求 | 本次实现 |
|-------------|------|----------|
| 4.3.4 统一任务模型与状态机 | 完整状态机包含 Retrying | ✅ 添加 kRetrying 状态 |
| 4.3.5 远程 Compaction 执行机制 | Worker 端通过共享存储执行 Compaction | ✅ 补全共享 FS 环境设置 |
| 4.3.7 统一任务调度与资源感知 | 推送模式分发 | ✅ 补全 DistributeTaskToCSA + DistributeJobToCSA |
| 4.3.8 控制平面 | 控制平面管理 CSA Worker | ✅ 补全 CancelCSATask gRPC 调用 |

---

## 5. 测试建议

1. **共享文件系统缓存测试**: 多次调用 `GetOrCreateEnvFromURI()` 验证缓存命中
2. **状态机测试**: 验证 `Failed → Retrying → Pending → Assigned → Running` 流转
3. **gRPC 集成测试**: 启动 CSA Worker，验证 `DistributeCompactionJob` 和 `CancelRunningTask` 调用
4. **容错测试**: Worker 离线时 gRPC 调用超时，验证错误处理和重调度
