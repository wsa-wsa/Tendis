add_library(rocks_kvstore STATIC rocks_kvstore.cpp rocks_kvttlcompactfilter.cpp)
target_link_libraries(rocks_kvstore utils_common kvstore rocksdb record glog ${SYS_LIBS} snappy lz4_static)

add_library(rocks_kvstore_for_test STATIC rocks_kvstore.cpp rocks_kvttlcompactfilter.cpp)
target_compile_definitions(rocks_kvstore_for_test PRIVATE -DNO_VERSIONEP)
target_link_libraries(rocks_kvstore_for_test utils_common kvstore rocksdb record glog ${SYS_LIBS} snappy lz4_static)

if(TENDIS_WITH_UNIT_TESTS)
add_executable(rocks_kvstore_test rocks_kvstore_test.cpp)
if(APPLE)
    # macOS: use -force_load (ld64 linker)
    target_link_libraries(rocks_kvstore_test utils_common kvstore rocksdb
        session_ctx session lock server
        rocks_kvstore server_params record gtest_main ${SYS_LIBS} snappy lz4_static)
    target_link_libraries(rocks_kvstore_test -Wl,-force_load,$<TARGET_FILE:commands>)
elseif(MSVC)
    # Windows: use /WHOLEARCHIVE
    target_link_libraries(rocks_kvstore_test utils_common kvstore rocksdb
        session_ctx session lock server commands
        rocks_kvstore server_params record gtest_main ${SYS_LIBS} snappy lz4_static)
    set_target_properties(rocks_kvstore_test PROPERTIES LINK_FLAGS "/WHOLEARCHIVE:commands")
else()
    # Linux/Unix (GCC or Clang): use --whole-archive (GNU ld)
    target_link_libraries(rocks_kvstore_test utils_common kvstore rocksdb
        session_ctx session lock server -Wl,--whole-archive commands -Wl,--no-whole-archive
        rocks_kvstore server_params record gtest_main ${SYS_LIBS} snappy lz4_static)
endif()
endif()
