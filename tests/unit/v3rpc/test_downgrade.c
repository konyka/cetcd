#include "cetcd/v3rpc.h"
#include "cetcd_test.h"

#include <string.h>

CETCD_TEST_CASE(downgrade_validate_current_ok) {
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_downgrade_ok(CETCD_DOWNGRADE_VALIDATE,
                                                 cetcd_version()), 1);
}

CETCD_TEST_CASE(downgrade_validate_other_fail) {
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_downgrade_ok(CETCD_DOWNGRADE_VALIDATE,
                                                 "0.1.0"), 0);
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_downgrade_ok(CETCD_DOWNGRADE_VALIDATE,
                                                 "3.5.0"), 0);
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_downgrade_ok(CETCD_DOWNGRADE_VALIDATE,
                                                 ""), 0);
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_downgrade_ok(CETCD_DOWNGRADE_VALIDATE,
                                                 NULL), 0);
}

CETCD_TEST_CASE(downgrade_enable_always_fail) {
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_downgrade_ok(CETCD_DOWNGRADE_ENABLE,
                                                 cetcd_version()), 0);
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_downgrade_ok(CETCD_DOWNGRADE_ENABLE,
                                                 "0.1.0"), 0);
}

CETCD_TEST_CASE(downgrade_cancel_always_fail) {
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_downgrade_ok(CETCD_DOWNGRADE_CANCEL,
                                                 cetcd_version()), 0);
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_downgrade_ok(CETCD_DOWNGRADE_CANCEL,
                                                 NULL), 0);
}

CETCD_TEST_CASE(downgrade_unknown_action_fail) {
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_downgrade_ok(99, cetcd_version()), 0);
}

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(downgrade_validate_current_ok),
    CETCD_TEST_ENTRY(downgrade_validate_other_fail),
    CETCD_TEST_ENTRY(downgrade_enable_always_fail),
    CETCD_TEST_ENTRY(downgrade_cancel_always_fail),
    CETCD_TEST_ENTRY(downgrade_unknown_action_fail),
CETCD_TEST_LIST_END

CETCD_TEST_MAIN()
