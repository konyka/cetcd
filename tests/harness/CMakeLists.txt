# In-tree test harness (no third-party dep). See cetcd_test.h for rationale.

add_library(cetcd_test_harness STATIC
    cetcd_test.c
    cetcd_test.h)
add_library(cetcd::test_harness ALIAS cetcd_test_harness)

target_include_directories(cetcd_test_harness
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(cetcd_test_harness
    PUBLIC cetcd::compile_options)

set_target_properties(cetcd_test_harness PROPERTIES
    C_STANDARD          99
    C_STANDARD_REQUIRED ON
    C_EXTENSIONS        OFF)
