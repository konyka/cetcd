#include "cetcd/base.h"
#include "cetcd/tls.h"
#include "cetcd_test.h"

#include <limits.h>

CETCD_TEST_CASE(tls_self_signed_days_default_and_years) {
    int days = 0;
    CETCD_ASSERT_EQ_INT(cetcd_tls_self_signed_days(0, &days), CETCD_OK);
    CETCD_ASSERT_EQ_INT(days, 365);
    CETCD_ASSERT_EQ_INT(cetcd_tls_self_signed_days(1, &days), CETCD_OK);
    CETCD_ASSERT_EQ_INT(days, 365);
    CETCD_ASSERT_EQ_INT(cetcd_tls_self_signed_days(2, &days), CETCD_OK);
    CETCD_ASSERT_EQ_INT(days, 730);
    CETCD_ASSERT_EQ_INT(cetcd_tls_self_signed_days(10, &days), CETCD_OK);
    CETCD_ASSERT_EQ_INT(days, 3650);
}

CETCD_TEST_CASE(tls_parse_version) {
    int v = 99;
    CETCD_ASSERT_EQ_INT(cetcd_parse_tls_version("TLS1.2", &v), CETCD_OK);
    CETCD_ASSERT_EQ_INT(v, CETCD_TLS_VER_1_2);
    CETCD_ASSERT_EQ_INT(cetcd_parse_tls_version("TLS1.3", &v), CETCD_OK);
    CETCD_ASSERT_EQ_INT(v, CETCD_TLS_VER_1_3);
    CETCD_ASSERT_EQ_INT(cetcd_parse_tls_version("TLS1.1", &v), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_tls_version("1.2", &v), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_tls_version("tls1.2", &v), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_tls_version("", &v), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_tls_version(NULL, &v), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_tls_version("TLS1.2", NULL), CETCD_ERR_INVAL);
}

CETCD_TEST_CASE(tls_version_range) {
    CETCD_ASSERT_EQ_INT(cetcd_tls_version_range_ok(0, 0), CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_tls_version_range_ok(CETCD_TLS_VER_1_2, 0), CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_tls_version_range_ok(CETCD_TLS_VER_1_3, 0), CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_tls_version_range_ok(CETCD_TLS_VER_1_2, CETCD_TLS_VER_1_2),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_tls_version_range_ok(CETCD_TLS_VER_1_2, CETCD_TLS_VER_1_3),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_tls_version_range_ok(CETCD_TLS_VER_1_3, CETCD_TLS_VER_1_3),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_tls_version_range_ok(CETCD_TLS_VER_1_3, CETCD_TLS_VER_1_2),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_tls_version_range_ok(9, CETCD_TLS_VER_1_3),
                        CETCD_ERR_INVAL);
}

CETCD_TEST_CASE(tls_self_signed_days_overflow_and_null) {
    int days = 0;
    CETCD_ASSERT_EQ_INT(cetcd_tls_self_signed_days((uint32_t)(INT_MAX / 365) + 1,
                                                   &days),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_tls_self_signed_days(1, NULL), CETCD_ERR_INVAL);
}

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(tls_self_signed_days_default_and_years),
    CETCD_TEST_ENTRY(tls_parse_version),
    CETCD_TEST_ENTRY(tls_version_range),
    CETCD_TEST_ENTRY(tls_self_signed_days_overflow_and_null),
CETCD_TEST_LIST_END

CETCD_TEST_MAIN()
