# Smoke drivers always run under CTest. libFuzzer binaries are optional
# (`CETCD_BUILD_FUZZ=ON`, clang).

function(cetcd_add_fuzz name)
    set(oneValueArgs)
    set(multiValueArgs DEPS)
    cmake_parse_arguments(F "" "${oneValueArgs}" "${multiValueArgs}" ${ARGN})

    add_executable(fuzz_${name}_smoke fuzz_${name}.c)
    target_compile_definitions(fuzz_${name}_smoke PRIVATE CETCD_FUZZ_DRIVER)
    target_include_directories(fuzz_${name}_smoke PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
    target_link_libraries(fuzz_${name}_smoke PRIVATE ${F_DEPS})
    add_test(NAME fuzz_${name}_smoke COMMAND fuzz_${name}_smoke)
    set_tests_properties(fuzz_${name}_smoke PROPERTIES TIMEOUT 10)

    if(CETCD_BUILD_FUZZ)
        add_executable(fuzz_${name} fuzz_${name}.c)
        target_include_directories(fuzz_${name} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
        target_link_libraries(fuzz_${name} PRIVATE ${F_DEPS})
        target_compile_options(fuzz_${name} PRIVATE -fsanitize=fuzzer)
        target_link_options(fuzz_${name} PRIVATE -fsanitize=fuzzer)
    endif()
endfunction()

if(CETCD_BUILD_FUZZ AND NOT CMAKE_C_COMPILER_ID MATCHES "Clang")
    message(FATAL_ERROR "CETCD_BUILD_FUZZ requires clang")
endif()

cetcd_add_fuzz(wal_decode DEPS cetcd::wal)
cetcd_add_fuzz(proto_rpc DEPS cetcd::proto)
target_include_directories(fuzz_proto_rpc_smoke PRIVATE
    ${CMAKE_SOURCE_DIR}/third_party/protobuf-c/protobuf-c)
if(TARGET fuzz_proto_rpc)
    target_include_directories(fuzz_proto_rpc PRIVATE
        ${CMAKE_SOURCE_DIR}/third_party/protobuf-c/protobuf-c)
endif()
cetcd_add_fuzz(auth_token DEPS cetcd::auth)
