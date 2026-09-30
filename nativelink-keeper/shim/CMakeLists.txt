# Copyright 2026 The NativeLink Authors. All rights reserved.
#
# Licensed under the Functional Source License, Version 1.1, Apache 2.0 Future License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    See LICENSE file for details
#
# Superbuild: vendored ClickHouse (for the in-process Keeper) + the C shim.
#
#   cmake -G Ninja -DNLK_CLICKHOUSE_DIR=/path/to/clickhouse <this dir>
#   ninja nativelink_keeper_shim
#
# ClickHouse's own CMake requires clang + lld; see BUILD.md for the exact
# toolchain invocation.

cmake_minimum_required(VERSION 3.25)
project(nativelink_keeper LANGUAGES C CXX ASM)

set(NLK_CLICKHOUSE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../vendor/clickhouse"
    CACHE PATH "Path to the vendored ClickHouse source tree")

if(NOT EXISTS "${NLK_CLICKHOUSE_DIR}/CMakeLists.txt")
    message(FATAL_ERROR "ClickHouse source not found at ${NLK_CLICKHOUSE_DIR} (set -DNLK_CLICKHOUSE_DIR)")
endif()

# Minimal Keeper-only configuration, mirroring ClickHouse's FastTest build:
# everything optional off, keep only what the Keeper path needs.
set(ENABLE_LIBRARIES 0 CACHE BOOL "" FORCE)
set(ENABLE_NURAFT 1 CACHE BOOL "" FORCE)         # raft engine — USE_NURAFT gates the whole Coordination/ tree
set(ENABLE_SIMDJSON 1 CACHE BOOL "" FORCE)
set(ENABLE_JEMALLOC 1 CACHE BOOL "" FORCE)
set(ENABLE_LIBURING 1 CACHE BOOL "" FORCE)
set(ENABLE_YAML_CPP 1 CACHE BOOL "" FORCE)
set(ENABLE_RUST 1 CACHE BOOL "" FORCE)
set(ENABLE_TESTS 0 CACHE BOOL "" FORCE)
set(ENABLE_UTILS 0 CACHE BOOL "" FORCE)
set(ENABLE_THINLTO 0 CACHE BOOL "" FORCE)
set(ENABLE_CLICKHOUSE_KEEPER 1 CACHE BOOL "" FORCE)

add_subdirectory("${NLK_CLICKHOUSE_DIR}" clickhouse EXCLUDE_FROM_ALL)

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
