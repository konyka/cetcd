#define _POSIX_C_SOURCE 200809L
#include "cetcd/server.h"
#include "cetcd_test.h"

#include <stdio.h>
#include <string.h>
#include <unistd.h>

static void write_raw_(const char *path, const char *s) {
    FILE *f = fopen(path, "wb");
    CETCD_ASSERT_NOT_NULL(f);
    if (s && s[0])
        CETCD_ASSERT_TRUE(fwrite(s, 1, strlen(s), f) == strlen(s));
    fclose(f);
}

CETCD_TEST_CASE(corrupt_check_parse_bool) {
    int b = -1;
    CETCD_ASSERT_EQ_INT(cetcd_parse_bool_flag("true", &b), CETCD_OK);
    CETCD_ASSERT_EQ_INT(b, 1);
    CETCD_ASSERT_EQ_INT(cetcd_parse_bool_flag("1", &b), CETCD_OK);
    CETCD_ASSERT_EQ_INT(b, 1);
    CETCD_ASSERT_EQ_INT(cetcd_parse_bool_flag("false", &b), CETCD_OK);
    CETCD_ASSERT_EQ_INT(b, 0);
    CETCD_ASSERT_EQ_INT(cetcd_parse_bool_flag("0", &b), CETCD_OK);
    CETCD_ASSERT_EQ_INT(b, 0);
    CETCD_ASSERT_EQ_INT(cetcd_parse_bool_flag("yes", &b), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_bool_flag("TRUE", &b), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_bool_flag("", &b), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_bool_flag(NULL, &b), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_bool_flag("true", NULL), CETCD_ERR_INVAL);
}

CETCD_TEST_CASE(corrupt_check_store_load_roundtrip) {
    char dir[] = "/tmp/cetcd-hash-XXXXXX";
    CETCD_ASSERT_NOT_NULL(mkdtemp(dir));
    char path[300];
    snprintf(path, sizeof(path), "%s/backend.hash", dir);

    int64_t rev = 0;
    uint32_t hash = 0;
    CETCD_ASSERT_EQ_INT(cetcd_backend_hash_load(path, &rev, &hash),
                        CETCD_ERR_NOTFOUND);
    CETCD_ASSERT_EQ_INT(cetcd_backend_hash_store(path, 42, 0xabcdu), CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_backend_hash_load(path, &rev, &hash), CETCD_OK);
    CETCD_ASSERT_EQ_INT((int)rev, 42);
    CETCD_ASSERT_EQ_INT((int)hash, 0xabcd);

    CETCD_ASSERT_EQ_INT(cetcd_backend_hash_store(NULL, 1, 1), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_backend_hash_store(path, -1, 1), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_backend_hash_load(path, NULL, &hash),
                        CETCD_ERR_INVAL);

    unlink(path);
    rmdir(dir);
}

CETCD_TEST_CASE(corrupt_check_load_rejects_garbage) {
    char dir[] = "/tmp/cetcd-hash-bad-XXXXXX";
    CETCD_ASSERT_NOT_NULL(mkdtemp(dir));
    char path[300];
    snprintf(path, sizeof(path), "%s/backend.hash", dir);

    int64_t rev = 0;
    uint32_t hash = 0;
    write_raw_(path, "");
    CETCD_ASSERT_EQ_INT(cetcd_backend_hash_load(path, &rev, &hash),
                        CETCD_ERR_CORRUPT);
    write_raw_(path, "not-a-hash\n");
    CETCD_ASSERT_EQ_INT(cetcd_backend_hash_load(path, &rev, &hash),
                        CETCD_ERR_CORRUPT);
    write_raw_(path, "1 2 leftover\n");
    CETCD_ASSERT_EQ_INT(cetcd_backend_hash_load(path, &rev, &hash),
                        CETCD_ERR_CORRUPT);
    write_raw_(path, "-1 2\n");
    CETCD_ASSERT_EQ_INT(cetcd_backend_hash_load(path, &rev, &hash),
                        CETCD_ERR_CORRUPT);

    unlink(path);
    rmdir(dir);
}

CETCD_TEST_CASE(corrupt_check_verify_missing_writes) {
    char dir[] = "/tmp/cetcd-hash-miss-XXXXXX";
    CETCD_ASSERT_NOT_NULL(mkdtemp(dir));
    char path[300];
    snprintf(path, sizeof(path), "%s/backend.hash", dir);

    CETCD_ASSERT_EQ_INT(cetcd_backend_hash_verify(path, 3, 99), CETCD_OK);
    int64_t rev = 0;
    uint32_t hash = 0;
    CETCD_ASSERT_EQ_INT(cetcd_backend_hash_load(path, &rev, &hash), CETCD_OK);
    CETCD_ASSERT_EQ_INT((int)rev, 3);
    CETCD_ASSERT_EQ_INT((int)hash, 99);

    unlink(path);
    rmdir(dir);
}

CETCD_TEST_CASE(corrupt_check_verify_fail_closed) {
    char dir[] = "/tmp/cetcd-hash-fc-XXXXXX";
    CETCD_ASSERT_NOT_NULL(mkdtemp(dir));
    char path[300];
    snprintf(path, sizeof(path), "%s/backend.hash", dir);

    CETCD_ASSERT_EQ_INT(cetcd_backend_hash_store(path, 5, 111), CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_backend_hash_verify(path, 5, 222),
                        CETCD_ERR_CORRUPT);
    CETCD_ASSERT_EQ_INT(cetcd_backend_hash_verify(path, 4, 111),
                        CETCD_ERR_CORRUPT);
    CETCD_ASSERT_EQ_INT(cetcd_backend_hash_verify(path, 5, 111), CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_backend_hash_verify(path, 6, 333), CETCD_OK);
    int64_t rev = 0;
    uint32_t hash = 0;
    CETCD_ASSERT_EQ_INT(cetcd_backend_hash_load(path, &rev, &hash), CETCD_OK);
    CETCD_ASSERT_EQ_INT((int)rev, 6);
    CETCD_ASSERT_EQ_INT((int)hash, 333);

    write_raw_(path, "garbage");
    CETCD_ASSERT_EQ_INT(cetcd_backend_hash_verify(path, 1, 1),
                        CETCD_ERR_CORRUPT);

    unlink(path);
    rmdir(dir);
}

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(corrupt_check_parse_bool),
    CETCD_TEST_ENTRY(corrupt_check_store_load_roundtrip),
    CETCD_TEST_ENTRY(corrupt_check_load_rejects_garbage),
    CETCD_TEST_ENTRY(corrupt_check_verify_missing_writes),
    CETCD_TEST_ENTRY(corrupt_check_verify_fail_closed),
CETCD_TEST_LIST_END

CETCD_TEST_MAIN()
