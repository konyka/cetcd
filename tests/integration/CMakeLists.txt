cetcd_add_unit_test(integration_server MODULE server SOURCES test_live_server.c)
target_link_libraries(test_integration_server PRIVATE cetcd::io cetcd::http2)
