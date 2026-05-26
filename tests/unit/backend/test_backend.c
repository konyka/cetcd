#define _POSIX_C_SOURCE 200809L
#include "cetcd/backend.h"
#include "cetcd_test.h"
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

CETCD_TEST_CASE(backend_put_get_del_txn) {
    char path_template[] = "/tmp/cetcd-test-backend-XXXXXX";
    char *path = mkdtemp(path_template);
    CETCD_ASSERT_NOT_NULL(path);
    cetcd_backend_config cfg = {
        .path = path,
        .map_size = 16 * 1024 * 1024,
        .max_dbs = 4
    };
    cetcd_backend *be = cetcd_backend_open(&cfg);
    CETCD_ASSERT_NOT_NULL(be);

    const char bucket[] = "testbucket";
    const uint8_t k[] = {0x01, 0x02, 0x03};
    const uint8_t v[] = {0x0A, 0x0B, 0x0C, 0x0D};
    int rc = cetcd_backend_put(be, bucket, k, sizeof(k), v, sizeof(v));
    CETCD_ASSERT_EQ_INT(rc, CETCD_OK);

    uint8_t *val = NULL; size_t vlen = 0;
    rc = cetcd_backend_get(be, bucket, k, sizeof(k), &val, &vlen);
    CETCD_ASSERT_EQ_INT(rc, CETCD_OK);
    CETCD_ASSERT_NOT_NULL(val);
    CETCD_ASSERT_EQ_INT(vlen, sizeof(v));
    CETCD_ASSERT_TRUE(memcmp(val, v, vlen) == 0);
    free(val);

    rc = cetcd_backend_del(be, bucket, k, sizeof(k));
    CETCD_ASSERT_EQ_INT(rc, CETCD_OK);
    rc = cetcd_backend_get(be, bucket, k, sizeof(k), &val, &vlen);
    CETCD_ASSERT_EQ_INT(rc, CETCD_ERR_NOTFOUND);

    cetcd_txn *txn = cetcd_txn_begin(be, false);
    CETCD_ASSERT_NOT_NULL(txn);
    uint8_t k2[] = {0xAA, 0xBB}; uint8_t v2[] = {0x11, 0x22, 0x33};
    rc = cetcd_txn_put(txn, bucket, k2, sizeof(k2), v2, sizeof(v2));
    CETCD_ASSERT_EQ_INT(rc, CETCD_OK);
    rc = cetcd_txn_commit(txn);
    CETCD_ASSERT_EQ_INT(rc, CETCD_OK);

    rc = cetcd_backend_get(be, bucket, k2, sizeof(k2), &val, &vlen);
    CETCD_ASSERT_EQ_INT(rc, CETCD_OK);
    CETCD_ASSERT_EQ_INT(vlen, sizeof(v2));
    CETCD_ASSERT_TRUE(memcmp(val, v2, vlen) == 0);
    free(val);

    cetcd_backend_close(be);
    rmdir(path);
}

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(backend_put_get_del_txn),
CETCD_TEST_LIST_END

CETCD_TEST_MAIN()
