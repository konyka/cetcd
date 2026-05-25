# Helpers used across the build tree.

# cetcd_add_module(name SOURCES src1 src2 ... [PUBLIC_DEPS ...] [PRIVATE_DEPS ...])
function(cetcd_add_module name)
    set(options)
    set(oneValueArgs)
    set(multiValueArgs SOURCES PUBLIC_DEPS PRIVATE_DEPS)
    cmake_parse_arguments(M "${options}" "${oneValueArgs}" "${multiValueArgs}" ${ARGN})

    add_library(cetcd_${name} STATIC ${M_SOURCES})
    add_library(cetcd::${name} ALIAS cetcd_${name})

    target_include_directories(cetcd_${name}
        PUBLIC  $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

    target_link_libraries(cetcd_${name}
        PUBLIC  cetcd::compile_options ${M_PUBLIC_DEPS}
        PRIVATE ${M_PRIVATE_DEPS})

    set_target_properties(cetcd_${name} PROPERTIES
        C_STANDARD          99
        C_STANDARD_REQUIRED ON
        C_EXTENSIONS        OFF)
endfunction()

# cetcd_add_unit_test(name MODULE module SOURCES ...)
function(cetcd_add_unit_test name)
    set(options)
    set(oneValueArgs MODULE)
    set(multiValueArgs SOURCES EXTRA_DEPS)
    cmake_parse_arguments(T "${options}" "${oneValueArgs}" "${multiValueArgs}" ${ARGN})

    add_executable(test_${name} ${T_SOURCES})
    target_link_libraries(test_${name}
        PRIVATE cetcd::${T_MODULE} cetcd::test_harness ${T_EXTRA_DEPS})
    target_include_directories(test_${name} PRIVATE ${CMAKE_SOURCE_DIR}/include)
    add_test(NAME ${name} COMMAND test_${name})
    set_tests_properties(${name} PROPERTIES TIMEOUT 30)
endfunction()
