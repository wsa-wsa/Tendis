// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

// Control Plane Server Main Entry
// Based on CaaS-LSM architecture: Compaction-as-a-Service
//
// This is a standalone service that manages compaction tasks for multiple
// Tendisplus nodes. It should be deployed independently from Tendisplus
// and CSA workers.
//
// Architecture:
// ┌─────────────────────────────────────────────────────────────────────────┐
// │                     Control Plane (This Server)                         │
// │  ┌─────────────────┐  ┌──────────────────┐  ┌────────────────────┐     │
// │  │ WorkerManager   │  │ TaskScheduler    │  │ gRPC Service       │     │
// │  │ - Worker注册    │  │ - 任务队列       │  │ - 任务提交         │     │
// │  │ - 健康检查      │  │ - 负载均衡       │  │ - 状态查询         │     │
// │  │ - 资源管理      │  │ - 调度策略       │  │ - Worker管理       │     │
// │  └─────────────────┘  └──────────────────┘  └────────────────────┘     │
// │                              gRPC Server (default: 50051)               │
// └─────────────────────────────────────────────────────────────────────────┘
//          ▲                           │                          ▲
//          │ Submit task               │ Assign task              │ Register
//          │ Query result              ▼                          │ Heartbeat
// ┌─────────────────┐           ┌─────────────────┐         ┌─────────────────┐
// │   Tendisplus    │           │   CSA Worker 1  │         │   CSA Worker N  │
// │   (DB Node)     │           │                 │   ...   │                 │
// └─────────────────┘           └─────────────────┘         └─────────────────┘

#include <signal.h>
#include <unistd.h>

#include <iostream>
#include <memory>
#include <string>

#include "control_plane.h"

using namespace tendisplus::control_plane;

// Global control plane instance for signal handling
std::unique_ptr<ControlPlane> g_control_plane;

void PrintUsage(const char* program) {
  std::cout << "Usage: " << program << " [options]\n"
            << "Options:\n"
            << "  -l <address>  Listen address (default: 0.0.0.0:50051)\n"
            << "  -t <threads>  gRPC thread pool size (default: 10)\n"
            << "  -p <policy>   Scheduling policy: fifo, priority, fair, "
               "least_loaded (default: priority)\n"
            << "  -m <size>     Max pending tasks (default: 10000)\n"
            << "  -T <seconds>  Task timeout in seconds (default: 3600)\n"
            << "  -r <count>    Max task retries (default: 3)\n"
            << "  -H <seconds>  Worker heartbeat timeout (default: 30)\n"
            << "  -h            Show this help\n"
            << std::endl;
}

void SignalHandler(int signum) {
  std::cout << "\n[ControlPlane] Received signal " << signum
            << ", shutting down..." << std::endl;
  if (g_control_plane) {
    g_control_plane->Stop();
  }
}

SchedulingPolicy ParseSchedulingPolicy(const std::string& policy) {
  if (policy == "fifo") {
    return SchedulingPolicy::kFIFO;
  } else if (policy == "priority") {
    return SchedulingPolicy::kPriority;
  } else if (policy == "fair") {
    return SchedulingPolicy::kFairShare;
  } else if (policy == "least_loaded") {
    return SchedulingPolicy::kLeastLoaded;
  }
  return SchedulingPolicy::kPriority;
}

int main(int argc, char* argv[]) {
  ControlPlaneConfig config;
  int opt;

  while ((opt = getopt(argc, argv, "l:t:p:m:T:r:H:h")) != -1) {
    switch (opt) {
      case 'l':
        config.listen_address = optarg;
        break;
      case 't':
        config.grpc_max_threads = std::stoul(optarg);
        break;
      case 'p':
        config.scheduler_config.policy = ParseSchedulingPolicy(optarg);
        break;
      case 'm':
        config.scheduler_config.max_pending_tasks = std::stoul(optarg);
        break;
      case 'T':
        config.default_task_timeout_sec = std::stoul(optarg);
        config.scheduler_config.task_timeout_sec = std::stoul(optarg);
        break;
      case 'r':
        config.max_task_retries = std::stoul(optarg);
        break;
      case 'H':
        config.worker_manager_config.heartbeat_timeout_sec = std::stol(optarg);
        break;
      case 'h':
      default:
        PrintUsage(argv[0]);
        return (opt == 'h') ? 0 : 1;
    }
  }

  // Print configuration
  std::cout << "================================================" << std::endl;
  std::cout << "    CaaS-LSM Control Plane Server" << std::endl;
  std::cout << "    Compaction-as-a-Service for Tendisplus" << std::endl;
  std::cout << "================================================" << std::endl;
  std::cout << "Configuration:" << std::endl;
  std::cout << "  Listen address: " << config.listen_address << std::endl;
  std::cout << "  gRPC threads: " << config.grpc_max_threads << std::endl;
  std::cout << "  Scheduling policy: "
            << SchedulingPolicyToString(config.scheduler_config.policy)
            << std::endl;
  std::cout << "  Max pending tasks: "
            << config.scheduler_config.max_pending_tasks << std::endl;
  std::cout << "  Task timeout: " << config.default_task_timeout_sec << "s"
            << std::endl;
  std::cout << "  Max retries: " << config.max_task_retries << std::endl;
  std::cout << "  Heartbeat timeout: "
            << config.worker_manager_config.heartbeat_timeout_sec << "s"
            << std::endl;
  std::cout << "================================================" << std::endl;

  // Setup signal handlers
  signal(SIGINT, SignalHandler);
  signal(SIGTERM, SignalHandler);

  // Create and start control plane
  try {
    g_control_plane = std::make_unique<ControlPlane>(config);
    g_control_plane->Start();

    std::cout << "[ControlPlane] Server started successfully" << std::endl;
    std::cout << "[ControlPlane] Waiting for connections..." << std::endl;

    // Wait for shutdown
    g_control_plane->Wait();

  } catch (const std::exception& e) {
    std::cerr << "[ControlPlane] Error: " << e.what() << std::endl;
    return 1;
  }

  std::cout << "[ControlPlane] Server shutdown complete" << std::endl;
  return 0;
}
