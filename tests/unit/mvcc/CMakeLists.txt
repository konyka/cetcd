cetcd_add_unit_test(mvcc_sanity MODULE mvcc SOURCES test_mvcc.c EXTRA_DEPS cetcd_backend)
cetcd_add_unit_test(mvcc_hash MODULE mvcc SOURCES test_mvcc_hash.c EXTRA_DEPS cetcd_backend)
