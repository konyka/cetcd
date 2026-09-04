cetcd_add_unit_test(integration_server MODULE server SOURCES test_live_server.c)
target_link_libraries(test_integration_server PRIVATE cetcd::io cetcd::http2)
target_compile_definitions(test_integration_server PRIVATE
    _POSIX_C_SOURCE=200809L
    CETCDCTL_BIN="$<TARGET_FILE:cetcdctl>"
    CETCD_BIN="$<TARGET_FILE:cetcd>")
add_dependencies(test_integration_server cetcdctl cetcd)
