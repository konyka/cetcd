# Minimal CMake for protobuf-c RUNTIME ONLY.
# We do NOT build protoc-gen-c (needs C++ protobuf).
# We only build protobuf-c.c → libprotobuf-c.a for pack/unpack.
cmake_minimum_required(VERSION 3.10)
project(protobuf-c-runtime C)

get_filename_component(MAIN_DIR ${CMAKE_CURRENT_SOURCE_DIR} PATH)

add_library(protobuf-c STATIC ${MAIN_DIR}/protobuf-c/protobuf-c.c)
target_include_directories(protobuf-c PUBLIC
    $<BUILD_INTERFACE:${MAIN_DIR}>
    $<BUILD_INTERFACE:${MAIN_DIR}/protobuf-c>
)
set_target_properties(protobuf-c PROPERTIES
    C_STANDARD 99
    C_STANDARD_REQUIRED ON
    POSITION_INDEPENDENT_CODE ON
)
