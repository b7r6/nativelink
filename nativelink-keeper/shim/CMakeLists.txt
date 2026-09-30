# Built INSIDE the ClickHouse configure via nlk-inject.cmake (see
# BUILD.md). ClickHouse is the root project; we are a guest target and
# inherit its toolchain, flags and glibc-compat setup for free.
cmake_minimum_required(VERSION 3.20)
add_library(nativelink_keeper_shim STATIC keeper_shim.cpp)

# The shim compiles against ClickHouse internals, so it must use the same
# language mode the tree sets (inherited via the linked targets' INTERFACE
# requirements); pin the floor explicitly anyway.
target_compile_features(nativelink_keeper_shim PRIVATE cxx_std_23)

target_include_directories(nativelink_keeper_shim PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}/../include")

# dbms carries src/ (Coordination, Interpreters/Context, Server); the
# common_zookeeper libs carry ZooKeeperCommon/IKeeper; daemon pulls the base
# runtime bits; nuraft is the raft implementation.
target_link_libraries(nativelink_keeper_shim PRIVATE
    dbms
    clickhouse_common_zookeeper
    clickhouse_common_zookeeper_base
    clickhouse_common_config
    clickhouse_common_io
    daemon
    ch_contrib::nuraft)

install(TARGETS nativelink_keeper_shim ARCHIVE DESTINATION lib)
