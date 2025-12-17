# Copyright (c) 2024-present, Tencent Inc. All rights reserved.
# Bulk Load 模块 CMake 配置

cmake_minimum_required(VERSION 3.10)

set(BULK_LOAD_SOURCES
    bulk_load_task.cc
    sst_builder.cc
    sst_ingest.cc
    bulk_load_worker.cc
)

set(BULK_LOAD_HEADERS
    bulk_load_task.h
    sst_builder.h
    sst_ingest.h
    bulk_load_worker.h
)

add_library(bulk_load STATIC
    ${BULK_LOAD_SOURCES}
    ${BULK_LOAD_HEADERS}
)

target_include_directories(bulk_load PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/..
)

target_link_libraries(bulk_load
    control_plane
    observability
    rocksdb
)
