# Copyright (c) 2024-present, Tencent Inc. All rights reserved.
# 控制平面 CMakeLists.txt

set(CONTROL_PLANE_SOURCES
    task_model.cc
    task_state_machine.cc
    task_scheduler.cc
    worker_manager.cc
    control_plane.cc
)

set(CONTROL_PLANE_HEADERS
    task_model.h
    task_state_machine.h
    task_scheduler.h
    worker_manager.h
    control_plane.h
)

add_library(control_plane STATIC
    ${CONTROL_PLANE_SOURCES}
    ${CONTROL_PLANE_HEADERS}
)

target_include_directories(control_plane PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/..
)

target_link_libraries(control_plane
    observability
)
