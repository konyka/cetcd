cetcd_add_unit_test(v3rpc_sanity MODULE v3rpc SOURCES test_v3rpc.c EXTRA_DEPS cetcd::io)
cetcd_add_unit_test(v3rpc_apply MODULE v3rpc SOURCES test_apply.c EXTRA_DEPS cetcd_backend)
cetcd_add_unit_test(v3rpc_linearizable MODULE v3rpc SOURCES test_linearizable.c)
cetcd_add_unit_test(v3rpc_downgrade MODULE v3rpc SOURCES test_downgrade.c)
