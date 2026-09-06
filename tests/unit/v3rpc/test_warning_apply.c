#include "cetcd/v3rpc.h"
#include "cetcd_test.h"

CETCD_TEST_CASE(warning_apply_disabled) {
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_apply_should_warn(1, 0), 0);
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_apply_should_warn(1000000000ull, 0), 0);
}

CETCD_TEST_CASE(warning_apply_threshold) {
    uint64_t th = 100000000ull; /* 100ms */
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_apply_should_warn(0, th), 0);
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_apply_should_warn(th - 1, th), 0);
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_apply_should_warn(th, th), 1);
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_apply_should_warn(th + 1, th), 1);
}

CETCD_TEST_CASE(warning_apply_set_get) {
    cetcd_v3rpc_set_warning_apply_ns(0);
    CETCD_ASSERT_TRUE(cetcd_v3rpc_warning_apply_ns() == 0);
    cetcd_v3rpc_set_warning_apply_ns(100000000ull);
    CETCD_ASSERT_TRUE(cetcd_v3rpc_warning_apply_ns() == 100000000ull);
    cetcd_v3rpc_set_warning_apply_ns(0);
}

CETCD_TEST_CASE(warning_unary_set_get) {
    cetcd_v3rpc_set_warning_unary_ns(0);
    CETCD_ASSERT_TRUE(cetcd_v3rpc_warning_unary_ns() == 0);
    cetcd_v3rpc_set_warning_unary_ns(300000000ull);
    CETCD_ASSERT_TRUE(cetcd_v3rpc_warning_unary_ns() == 300000000ull);
    cetcd_v3rpc_set_warning_unary_ns(0);
}

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(warning_apply_disabled),
    CETCD_TEST_ENTRY(warning_apply_threshold),
    CETCD_TEST_ENTRY(warning_apply_set_get),
    CETCD_TEST_ENTRY(warning_unary_set_get),
CETCD_TEST_LIST_END

CETCD_TEST_MAIN()
