# Copyright (c) 2024-present, Tencent Inc. All rights reserved.
# Observability module CMakeLists.txt

cmake_minimum_required(VERSION 3.10)

set(OBSERVABILITY_HEADERS
    observability.h
    metrics.h
    task_metrics.h
    tracing.h
    visualization.h
    system_metrics.h
    grafana_dashboards.h
    alert_rules.h
    data_pipeline.h
    experiment_framework.h
)

set(OBSERVABILITY_SOURCES
    # 实现文件（如需要）
)

# 创建头文件库（header-only）
add_library(observability INTERFACE)
target_include_directories(observability INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

# 如果有实现文件，可以创建静态库
# add_library(observability_lib STATIC ${OBSERVABILITY_SOURCES})
# target_include_directories(observability_lib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
# target_link_libraries(observability_lib PUBLIC pthread)

# 安装头文件
install(FILES ${OBSERVABILITY_HEADERS}
        DESTINATION include/tendisplus/storage/rocks/remote_compaction/observability)
