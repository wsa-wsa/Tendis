// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include "bulk_load_coordinator.h"

#include <iostream>

namespace tendisplus {
namespace control_plane {

// BulkLoadCoordinator 的具体实现位于 control_plane.cc 中
// (ControlPlane::BulkLoadCoordinatorImpl)
//
// 本文件预留用于将 BulkLoadCoordinator 从 ControlPlane 中独立出来。
// 在 Commit 3.2 / 3.3 中可能会将实现迁移到这里。
//
// 当前 BulkLoadCoordinatorImpl 作为 ControlPlane 的内部类实现，
// 可以直接访问 ControlPlane 的私有成员（Scheduler、WorkerManager），
// 简化了组件间的通信。

}  // namespace control_plane
}  // namespace tendisplus
