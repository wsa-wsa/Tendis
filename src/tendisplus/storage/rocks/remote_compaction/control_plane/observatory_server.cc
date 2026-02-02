// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

// Observatory Server Main Entry
// 独立部署的可视化观测平面，通过 gRPC 连接 Control Plane 获取数据
//
// 架构：
// ┌─────────────────┐        ┌─────────────────────────────────────┐
// │   Observatory   │  gRPC  │         Control Plane               │
// │  (This Server)  │◄──────►│         (独立进程)                   │
// │                 │        │                                     │
// │  HTTP: 8080     │        │  gRPC: 50051                        │
// │  - Web UI       │        │  - Task Management                  │
// │  - REST API     │        │  - Worker Management                │
// └─────────────────┘        └─────────────────────────────────────┘
//         │
//         ▼
//    Web Browser
//
// 使用方式：
//   ./observatory_server -c localhost:50051 -p 8080
//
// 参数说明：
//   -c <address>  Control Plane 地址 (默认: localhost:50051)
//   -p <port>     HTTP 端口 (默认: 8080)
//   -b <address>  HTTP 绑定地址 (默认: 0.0.0.0)
//   -t <ms>       gRPC 超时时间 (默认: 5000ms)
//   -r <sec>      重连间隔 (默认: 5s)
//   -h            显示帮助

#include <signal.h>
#include <unistd.h>

#include <iostream>
#include <memory>
#include <string>

#include "observatory.h"

using namespace tendisplus::control_plane;

// Global observatory instance for signal handling
std::unique_ptr<Observatory> g_observatory;

void PrintUsage(const char* program) {
  std::cout << "Usage: " << program << " [options]\n"
            << "Options:\n"
            << "  -c <address>  Control Plane address (default: localhost:50051)\n"
            << "  -p <port>     HTTP server port (default: 8080)\n"
            << "  -b <address>  HTTP bind address (default: 0.0.0.0)\n"
            << "  -t <ms>       gRPC timeout in milliseconds (default: 5000)\n"
            << "  -r <sec>      Reconnect interval in seconds (default: 5)\n"
            << "  -h            Show this help\n"
            << std::endl;
}

void SignalHandler(int signum) {
  std::cout << "\n[Observatory] Received signal " << signum
            << ", shutting down..." << std::endl;
  if (g_observatory) {
    g_observatory->Stop();
  }
}

int main(int argc, char* argv[]) {
  ObservatoryConfig config;
  int opt;

  while ((opt = getopt(argc, argv, "c:p:b:t:r:h")) != -1) {
    switch (opt) {
      case 'c':
        config.control_plane_address = optarg;
        break;
      case 'p':
        config.http_port = static_cast<uint16_t>(std::stoul(optarg));
        break;
      case 'b':
        config.bind_address = optarg;
        break;
      case 't':
        config.grpc_timeout_ms = std::stoul(optarg);
        break;
      case 'r':
        config.reconnect_interval_sec = std::stoul(optarg);
        break;
      case 'h':
      default:
        PrintUsage(argv[0]);
        return (opt == 'h') ? 0 : 1;
    }
  }

  // Print configuration
  std::cout << "================================================" << std::endl;
  std::cout << "    CaaS-LSM Observatory Server" << std::endl;
  std::cout << "    Remote Compaction Monitoring Dashboard" << std::endl;
  std::cout << "================================================" << std::endl;
  std::cout << "Configuration:" << std::endl;
  std::cout << "  Control Plane: " << config.control_plane_address << std::endl;
  std::cout << "  HTTP Server: http://" << config.bind_address 
            << ":" << config.http_port << std::endl;
  std::cout << "  gRPC Timeout: " << config.grpc_timeout_ms << "ms" << std::endl;
  std::cout << "  Reconnect Interval: " << config.reconnect_interval_sec << "s" << std::endl;
  std::cout << "================================================" << std::endl;

  // Setup signal handlers
  signal(SIGINT, SignalHandler);
  signal(SIGTERM, SignalHandler);

  // Create and start observatory
  try {
    g_observatory = std::make_unique<Observatory>(config);
    g_observatory->Start();

    std::cout << "[Observatory] Server started successfully" << std::endl;
    std::cout << "[Observatory] Dashboard: http://localhost:" 
              << config.http_port << std::endl;
    std::cout << "[Observatory] Connecting to Control Plane..." << std::endl;

    // Wait for shutdown
    g_observatory->Wait();

  } catch (const std::exception& e) {
    std::cerr << "[Observatory] Error: " << e.what() << std::endl;
    return 1;
  }

  std::cout << "[Observatory] Server shutdown complete" << std::endl;
  return 0;
}
