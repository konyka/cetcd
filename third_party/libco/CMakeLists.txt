cmake_minimum_required(VERSION 3.10)
project(libco C)

add_library(libco STATIC coroutine.c)
target_include_directories(libco PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

if(WIN32)
  target_compile_definitions(libco PUBLIC LIBCO_USE_FIBERS)
endif()
