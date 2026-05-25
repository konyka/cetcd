#include "cetcd/base.h"
#include "cetcd_test.h"

#include <string.h>

CETCD_TEST_CASE(strerror_ok_returns_known_string) {
    const char *msg = cetcd_strerror(CETCD_OK);
    CETCD_ASSERT_NOT_NULL(msg);
    CETCD_ASSERT_TRUE(strlen(msg) > 0);
}

CETCD_TEST_CASE(strerror_each_error_has_unique_text) {
    const cetcd_status codes[] = {
        CETCD_OK,            CETCD_ERR_NOMEM,    CETCD_ERR_INVAL,
        CETCD_ERR_RANGE,     CETCD_ERR_NOTFOUND, CETCD_ERR_EXISTS,
        CETCD_ERR_IO,        CETCD_ERR_CORRUPT,  CETCD_ERR_INTERNAL,
        CETCD_ERR_OVERFLOW,  CETCD_ERR_CANCELED, CETCD_ERR_TIMEDOUT,
        CETCD_ERR_UNSUPPORT
    };
    const size_t n = sizeof(codes) / sizeof(codes[0]);

    const char *seen[sizeof(codes) / sizeof(codes[0])];
    for (size_t i = 0; i < n; ++i) {
        seen[i] = cetcd_strerror(codes[i]);
        CETCD_ASSERT_NOT_NULL(seen[i]);
        for (size_t j = 0; j < i; ++j) {
            CETCD_ASSERT_TRUE(strcmp(seen[i], seen[j]) != 0);
        }
    }
}

CETCD_TEST_CASE(strerror_unknown_returns_fallback) {
    const char *msg = cetcd_strerror((cetcd_status)-9999);
    CETCD_ASSERT_NOT_NULL(msg);
    CETCD_ASSERT_TRUE(strlen(msg) > 0);
}

CETCD_TEST_CASE(version_returns_nonempty_string) {
    const char *v = cetcd_version();
    CETCD_ASSERT_NOT_NULL(v);
    CETCD_ASSERT_TRUE(strlen(v) >= 3);
}

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(strerror_ok_returns_known_string),
    CETCD_TEST_ENTRY(strerror_each_error_has_unique_text),
    CETCD_TEST_ENTRY(strerror_unknown_returns_fallback),
    CETCD_TEST_ENTRY(version_returns_nonempty_string),
CETCD_TEST_LIST_END
CETCD_TEST_MAIN()
