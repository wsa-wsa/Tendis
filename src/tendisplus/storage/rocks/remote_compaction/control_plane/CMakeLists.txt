# Control Plane CMakeLists.txt
# Based on CaaS-LSM architecture

# Proto file for control plane
get_filename_component(cp_proto "${CMAKE_CURRENT_SOURCE_DIR}/../protos/control_plane.proto" ABSOLUTE)
get_filename_component(cp_proto_path "${cp_proto}" PATH)

# Generated sources
set(cp_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/control_plane.pb.cc")
set(cp_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/control_plane.pb.h")
set(cp_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/control_plane.grpc.pb.cc")
set(cp_grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/control_plane.grpc.pb.h")

add_custom_command(
    OUTPUT "${cp_proto_srcs}" "${cp_proto_hdrs}" "${cp_grpc_srcs}" "${cp_grpc_hdrs}"
    COMMAND ${_PROTOBUF_PROTOC}
    ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
         --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
         -I "${cp_proto_path}"
         --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
         "${cp_proto}"
    DEPENDS "${cp_proto}")

# Include generated *.pb.h files
include_directories("${CMAKE_CURRENT_BINARY_DIR}")

# Control plane proto library
add_library(control_plane_grpc_proto
    ${cp_grpc_srcs}
    ${cp_grpc_hdrs}
    ${cp_proto_srcs}
    ${cp_proto_hdrs})
target_link_libraries(control_plane_grpc_proto
    ${_REFLECTION}
    ${_GRPC_GRPCPP}
    ${_PROTOBUF_LIBPROTOBUF})

# Control plane core library (不包含 observatory)
add_library(control_plane_core STATIC
    task_model.cc
    worker_manager.cc
    task_scheduler.cc
    control_plane.cc
    bulk_load_coordinator.cc
    alert_manager.cc
    metrics_collector.cc
    task_tracer.cc)
target_link_libraries(control_plane_core
    control_plane_grpc_proto
    ${_REFLECTION}
    ${_GRPC_GRPCPP}
    ${_PROTOBUF_LIBPROTOBUF})

# Control plane server executable
add_executable(control_plane_server
    control_plane_server.cc)
target_link_libraries(control_plane_server
    control_plane_core
    control_plane_grpc_proto
    ${_REFLECTION}
    ${_GRPC_GRPCPP}
    ${_PROTOBUF_LIBPROTOBUF})

# ============================================================================
# Observatory - 独立可视化观测平面
# ============================================================================

# Observatory library (独立的 gRPC 客户端)
add_library(observatory STATIC
    observatory.cc)
target_link_libraries(observatory
    control_plane_grpc_proto
    ${_REFLECTION}
    ${_GRPC_GRPCPP}
    ${_PROTOBUF_LIBPROTOBUF})

# Observatory server executable (独立部署)
add_executable(observatory_server
    observatory_server.cc)
target_link_libraries(observatory_server
    observatory
    control_plane_grpc_proto
    ${_REFLECTION}
    ${_GRPC_GRPCPP}
    ${_PROTOBUF_LIBPROTOBUF})

# ============================================================================
# Client Libraries (for integration)
# ============================================================================

# Control plane client library (for Tendisplus integration)
add_library(control_plane_client STATIC
    ../control_plane_client.cc)
target_link_libraries(control_plane_client
    control_plane_grpc_proto
    ${_REFLECTION}
    ${_GRPC_GRPCPP}
    ${_PROTOBUF_LIBPROTOBUF})

# ============================================================================
# Bulk Load Executor - Worker 端 SST 生成引擎
# ============================================================================
add_library(bulk_load_executor STATIC
    ../bulk_load_executor.cc)
target_link_libraries(bulk_load_executor
    rocksdb
    shared_fs)

# ============================================================================
# Bulk Load Ingester - TendisPlus 端 SST 注入引擎
# ============================================================================
add_library(bulk_load_ingester STATIC
    ../bulk_load_ingester.cc)
target_link_libraries(bulk_load_ingester
    control_plane_grpc_proto
    rocksdb
    shared_fs
    ${_REFLECTION}
    ${_GRPC_GRPCPP}
    ${_PROTOBUF_LIBPROTOBUF})

# Control plane worker library (for CSA worker integration)
add_library(control_plane_worker STATIC
    ../control_plane_worker.cc)
target_link_libraries(control_plane_worker
    control_plane_grpc_proto
    bulk_load_executor
    rocksdb
    shared_fs
    ${_REFLECTION}
    ${_GRPC_GRPCPP}
    ${_PROTOBUF_LIBPROTOBUF})

# ============================================================================
# Unit Tests
# ============================================================================
if(TENDIS_WITH_UNIT_TESTS)

# Layer 1: Pure unit tests (no external dependencies)
add_executable(task_model_test task_model_test.cpp)
target_link_libraries(task_model_test control_plane_core gtest_main ${SYS_LIBS})

add_executable(metrics_collector_test metrics_collector_test.cpp)
target_link_libraries(metrics_collector_test control_plane_core gtest_main ${SYS_LIBS})

add_executable(task_tracer_test task_tracer_test.cpp)
target_link_libraries(task_tracer_test control_plane_core gtest_main ${SYS_LIBS})

# Layer 2: Mock-based unit tests
add_executable(alert_manager_test alert_manager_test.cpp)
target_link_libraries(alert_manager_test control_plane_core gtest_main ${SYS_LIBS})

add_executable(worker_manager_test worker_manager_test.cpp)
target_link_libraries(worker_manager_test control_plane_core gtest_main ${SYS_LIBS})

add_executable(task_scheduler_test task_scheduler_test.cpp)
target_link_libraries(task_scheduler_test control_plane_core gtest_main ${SYS_LIBS})

# Layer 3: Integration test
add_executable(control_plane_test control_plane_test.cpp)
target_link_libraries(control_plane_test control_plane_core gtest_main ${SYS_LIBS})

# Layer 4: Framework validation test (功能正确性 + 设计优势)
add_executable(framework_validation_test framework_validation_test.cpp)
target_link_libraries(framework_validation_test control_plane_core gtest_main ${SYS_LIBS})

# Register tests with CTest
add_test(NAME task_model_test COMMAND task_model_test)
add_test(NAME metrics_collector_test COMMAND metrics_collector_test)
add_test(NAME task_tracer_test COMMAND task_tracer_test)
add_test(NAME alert_manager_test COMMAND alert_manager_test)
add_test(NAME worker_manager_test COMMAND worker_manager_test)
add_test(NAME task_scheduler_test COMMAND task_scheduler_test)
add_test(NAME control_plane_test COMMAND control_plane_test)
add_test(NAME framework_validation_test COMMAND framework_validation_test)

endif()
