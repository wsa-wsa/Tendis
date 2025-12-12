add_executable(ldb_tendis ldb_tendis.cpp)
if(APPLE)
    # macOS: no static linking, no -march=native (ARM compatibility)
    target_link_libraries(ldb_tendis record rocksdb "-ldl -lpthread")
elseif(MSVC)
    # Windows
    target_link_libraries(ldb_tendis record rocksdb)
else()
    # Linux/Unix (GCC or Clang): full static linking
    target_link_libraries(ldb_tendis record rocksdb "-static-libgcc -static-libstdc++ -Wl,--no-as-needed -Wl,--whole-archive -ldl -Wl,--no-whole-archive -pthread -march=native")
endif()
