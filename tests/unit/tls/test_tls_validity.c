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

CETCD_TEST_CASE(tls_self_signed_days_overflow_and_null) {
    int days = 0;
    CETCD_ASSERT_EQ_INT(cetcd_tls_self_signed_days((uint32_t)(INT_MAX / 365) + 1,
                                                   &days),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_tls_self_signed_days(1, NULL), CETCD_ERR_INVAL);
}

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(tls_self_signed_days_default_and_years),
    CETCD_TEST_ENTRY(tls_self_signed_days_overflow_and_null),
CETCD_TEST_LIST_END

CETCD_TEST_MAIN()
