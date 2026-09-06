cetcd_add_unit_test(server_sanity MODULE server SOURCES test_server.c)
cetcd_add_unit_test(auto_compact MODULE server SOURCES test_auto_compact.c)
cetcd_add_unit_test(corrupt_check MODULE server SOURCES test_corrupt_check.c)
