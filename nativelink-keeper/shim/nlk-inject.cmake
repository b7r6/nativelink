# Injected into ClickHouse's root CMakeLists via
# -DCMAKE_PROJECT_ClickHouse_INCLUDE=<this file>. ClickHouse insists on
# being the root project (CMAKE_SOURCE_DIR-relative scripts abound), so
# the shim rides along inside its configure instead of superbuilding it.
if(NOT DEFINED NLK_SHIM_DIR)
  message(FATAL_ERROR "Pass -DNLK_SHIM_DIR=<nativelink-keeper/shim>")
endif()
add_subdirectory(${NLK_SHIM_DIR} nlk-shim)
