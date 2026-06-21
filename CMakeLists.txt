# cetcd — pure-C reimplementation of etcd
# Top-level CMake. C99 floor with required C11 atomics.
cmake_minimum_required(VERSION 3.21)

project(cetcd
    VERSION       0.3.0
    DESCRIPTION   "Pure-C reimplementation of etcd, wire-compatible with v3.5"
    HOMEPAGE_URL  "https://github.com/konyka/cetcd"
    LANGUAGES     C)

# ----------------------------------------------------------------------------
# Standards & global flags
# ----------------------------------------------------------------------------
set(CMAKE_C_STANDARD          99)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS        OFF)

# Even though the standard floor is C99, we *require* C11 atomics
# (<stdatomic.h>) for lock-free queues. Detect early and bail out clearly
# if the compiler doesn't provide them.
include(CheckIncludeFile)
check_include_file(stdatomic.h CETCD_HAVE_STDATOMIC_H)
if(NOT CETCD_HAVE_STDATOMIC_H)
    message(FATAL_ERROR
        "cetcd requires <stdatomic.h> (C11). Your compiler does not provide it.")
endif()

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake")
include(cetcd_helpers)

# Out-of-source builds only.
if(CMAKE_SOURCE_DIR STREQUAL CMAKE_BINARY_DIR)
    message(FATAL_ERROR
        "In-source builds are disabled. Run cmake from a separate build dir, e.g. `cmake -B build`.")
endif()

# Default build type.
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE "Release" CACHE STRING "Build type" FORCE)
    set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS
        "Debug" "Release" "RelWithDebInfo" "MinSizeRel")
endif()

# ----------------------------------------------------------------------------
# Project options
# ----------------------------------------------------------------------------
option(CETCD_BUILD_TESTS         "Build unit + integration tests"                   ON)
option(CETCD_BUILD_FUZZ          "Build libFuzzer fuzz targets (clang required)"    OFF)
option(CETCD_BUILD_BENCH         "Build benchmark suite"                            OFF)
option(CETCD_BUILD_EXAMPLES      "Build example programs"                           OFF)
option(CETCD_ENABLE_LTO          "Enable link-time optimisation in Release builds"  OFF)
option(CETCD_ENABLE_COVERAGE     "Enable gcov-style coverage flags"                 OFF)
option(CETCD_USE_SYSTEM_OPENSSL  "Use system OpenSSL via find_package"              ON)
option(CETCD_USE_SYSTEM_LMDB     "Use system LMDB instead of vendored"              OFF)
option(CETCD_WERROR              "Treat compiler warnings as errors"                OFF)

set(CETCD_SANITIZERS "" CACHE STRING
    "Comma-separated sanitizers: address,undefined,thread,memory")

# ----------------------------------------------------------------------------
# Compiler warnings / hardening (cetcd_compile_options interface lib)
# ----------------------------------------------------------------------------
add_library(cetcd_compile_options INTERFACE)
add_library(cetcd::compile_options ALIAS cetcd_compile_options)

if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(cetcd_compile_options INTERFACE
        -Wall -Wextra -Wpedantic
        -Wshadow -Wpointer-arith -Wcast-qual -Wcast-align
        -Wstrict-prototypes -Wmissing-prototypes
        -Wno-unused-parameter
        -Wformat=2
        -fvisibility=hidden)
    if(CETCD_WERROR)
        target_compile_options(cetcd_compile_options INTERFACE -Werror)
    endif()
elseif(MSVC)
    target_compile_options(cetcd_compile_options INTERFACE
        /W4 /utf-8 /permissive-)
    target_compile_definitions(cetcd_compile_options INTERFACE
        _CRT_SECURE_NO_WARNINGS WIN32_LEAN_AND_MEAN NOMINMAX)
    if(CETCD_WERROR)
        target_compile_options(cetcd_compile_options INTERFACE /WX)
    endif()
endif()

# Sanitizers
if(CETCD_SANITIZERS)
    if(NOT (CMAKE_C_COMPILER_ID MATCHES "GNU|Clang"))
        message(FATAL_ERROR "CETCD_SANITIZERS requires gcc or clang")
    endif()
    string(REPLACE "," ";" _san_list "${CETCD_SANITIZERS}")
    foreach(_san ${_san_list})
        message(STATUS "Enabling sanitizer: ${_san}")
        target_compile_options(cetcd_compile_options INTERFACE
            -fsanitize=${_san} -fno-omit-frame-pointer -g)
        target_link_options(cetcd_compile_options INTERFACE
            -fsanitize=${_san})
    endforeach()
endif()

# Coverage
if(CETCD_ENABLE_COVERAGE)
    if(NOT CMAKE_C_COMPILER_ID MATCHES "GNU|Clang")
        message(FATAL_ERROR "CETCD_ENABLE_COVERAGE requires gcc or clang")
    endif()
    target_compile_options(cetcd_compile_options INTERFACE --coverage)
    target_link_options(cetcd_compile_options INTERFACE --coverage)
endif()

# LTO
if(CETCD_ENABLE_LTO)
    include(CheckIPOSupported)
    check_ipo_supported(RESULT _ipo_ok OUTPUT _ipo_msg)
    if(_ipo_ok)
        set(CMAKE_INTERPROCEDURAL_OPTIMIZATION_RELEASE ON)
    else()
        message(WARNING "LTO requested but not supported: ${_ipo_msg}")
    endif()
endif()

# Platform-specific defines
if(WIN32)
    target_compile_definitions(cetcd_compile_options INTERFACE
        _WIN32_WINNT=0x0A00       # Windows 10
        _CRT_NONSTDC_NO_DEPRECATE)
endif()

# Position-independent code (we may ship shared libs)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# ----------------------------------------------------------------------------
# Output directories
# ----------------------------------------------------------------------------
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# ----------------------------------------------------------------------------
# Subdirectories
# ----------------------------------------------------------------------------
# Phase 0 only ships libcetcd_base; later phases will add more under src/.
add_subdirectory(third_party)
add_subdirectory(src)

if(CETCD_BUILD_TESTS)
    enable_testing()
    include(CTest)
    add_subdirectory(tests)
endif()

if(CETCD_BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()

add_subdirectory(cmd/cetcd)
add_subdirectory(cmd/cetcdctl)
add_subdirectory(cmd/cetcd-migrate)

if(CETCD_BUILD_BENCH)
    add_subdirectory(bench)
endif()

# ----------------------------------------------------------------------------
# Status summary
# ----------------------------------------------------------------------------
message(STATUS "")
message(STATUS "=================== cetcd configuration ===================")
message(STATUS " Version             : ${PROJECT_VERSION}")
message(STATUS " Build type          : ${CMAKE_BUILD_TYPE}")
message(STATUS " C compiler          : ${CMAKE_C_COMPILER_ID} ${CMAKE_C_COMPILER_VERSION}")
message(STATUS " C standard          : C${CMAKE_C_STANDARD}")
message(STATUS " Sanitizers          : ${CETCD_SANITIZERS}")
message(STATUS " LTO                 : ${CETCD_ENABLE_LTO}")
message(STATUS " Coverage            : ${CETCD_ENABLE_COVERAGE}")
message(STATUS " Tests               : ${CETCD_BUILD_TESTS}")
message(STATUS " Fuzz                : ${CETCD_BUILD_FUZZ}")
message(STATUS " Bench               : ${CETCD_BUILD_BENCH}")
message(STATUS " Examples            : ${CETCD_BUILD_EXAMPLES}")
message(STATUS "===========================================================")
message(STATUS "")
