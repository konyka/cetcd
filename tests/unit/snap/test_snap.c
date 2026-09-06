#include "cetcd/base.h"
#include "cetcd/snap.h"
#include "cetcd_test.h"

#include <stdlib.h>
#include <string.h>

CETCD_TEST_CASE(snap_create_destroy) {
    cetcd_snap *s = cetcd_snap_new();
    CETCD_ASSERT_NOT_NULL(s);
    CETCD_ASSERT_EQ_INT((int)cetcd_snap_entry_count(s), 0);
    cetcd_snap_free(s);
}

CETCD_TEST_CASE(snap_add_entries) {
    cetcd_snap *s = cetcd_snap_new();
    CETCD_ASSERT_EQ_INT(cetcd_snap_add_entry(s,
        (const uint8_t *)"key1", 4,
        (const uint8_t *)"val1", 4, 1), 0);
    CETCD_ASSERT_EQ_INT(cetcd_snap_add_entry(s,
        (const uint8_t *)"key2", 4,
        (const uint8_t *)"val2", 4, 2), 0);
    CETCD_ASSERT_EQ_INT((int)cetcd_snap_entry_count(s), 2);

    cetcd_snap_entry *e = cetcd_snap_get_entry(s, 0);
    CETCD_ASSERT_NOT_NULL(e);
    CETCD_ASSERT_EQ_INT((int)e->key_len, 4);
    CETCD_ASSERT_EQ_INT((int)e->value_len, 4);
    CETCD_ASSERT_EQ_INT((int)e->mod_revision, 1);

    e = cetcd_snap_get_entry(s, 1);
    CETCD_ASSERT_NOT_NULL(e);
    CETCD_ASSERT_EQ_INT((int)e->mod_revision, 2);

    CETCD_ASSERT_TRUE(cetcd_snap_get_entry(s, 99) == NULL);

    cetcd_snap_free(s);
}

CETCD_TEST_CASE(snap_encode_decode) {
    cetcd_snap *s = cetcd_snap_new();
    cetcd_snap_add_entry(s,
        (const uint8_t *)"hello", 5,
        (const uint8_t *)"world", 5, 3);
    cetcd_snap_add_entry(s,
        (const uint8_t *)"\x00\x01\x02", 3,
        (const uint8_t *)"\xff\xfe", 2, 5);

    size_t len = 0;
    uint8_t *encoded = cetcd_snap_encode(s, &len);
    CETCD_ASSERT_NOT_NULL(encoded);
    CETCD_ASSERT_TRUE(len > 0);

    cetcd_snap *s2 = cetcd_snap_decode(encoded, len);
    CETCD_ASSERT_NOT_NULL(s2);
    CETCD_ASSERT_EQ_INT((int)cetcd_snap_entry_count(s2), 2);

    cetcd_snap_entry *e0 = cetcd_snap_get_entry(s2, 0);
    CETCD_ASSERT_EQ_INT((int)e0->key_len, 5);
    CETCD_ASSERT_EQ_INT((int)e0->value_len, 5);
    CETCD_ASSERT_EQ_INT((int)e0->mod_revision, 3);
    CETCD_ASSERT_EQ_INT(memcmp(e0->key, "hello", 5), 0);
    CETCD_ASSERT_EQ_INT(memcmp(e0->value, "world", 5), 0);

    cetcd_snap_entry *e1 = cetcd_snap_get_entry(s2, 1);
    CETCD_ASSERT_EQ_INT((int)e1->key_len, 3);
    CETCD_ASSERT_EQ_INT((int)e1->value_len, 2);
    CETCD_ASSERT_EQ_INT((int)e1->mod_revision, 5);
    CETCD_ASSERT_EQ_INT(e1->key[0], 0x00);
    CETCD_ASSERT_EQ_INT(e1->value[0], 0xff);

    free(encoded);
    cetcd_snap_free(s);
    cetcd_snap_free(s2);
}

CETCD_TEST_CASE(snap_empty_roundtrip) {
    cetcd_snap *s = cetcd_snap_new();
    size_t len = 0;
    uint8_t *encoded = cetcd_snap_encode(s, &len);
    CETCD_ASSERT_NOT_NULL(encoded);

    cetcd_snap *s2 = cetcd_snap_decode(encoded, len);
    CETCD_ASSERT_NOT_NULL(s2);
    CETCD_ASSERT_EQ_INT((int)cetcd_snap_entry_count(s2), 0);

    free(encoded);
    cetcd_snap_free(s);
    cetcd_snap_free(s2);
}

CETCD_TEST_CASE(snap_decode_corrupt) {
    uint8_t garbage[] = {0xff, 0xff, 0x00, 0x01};
    cetcd_snap *s = cetcd_snap_decode(garbage, sizeof(garbage));
    CETCD_ASSERT_TRUE(s == NULL);

    cetcd_snap *s2 = cetcd_snap_decode(NULL, 0);
    CETCD_ASSERT_TRUE(s2 == NULL);
}

CETCD_TEST_CASE(snap_decode_kv_pairs_and_header) {
    uint8_t kv[] = {0x01, 'a', 0x01, 'b', 0x03, 'f', 'o', 'o', 0x03, 'b', 'a', 'r'};
    cetcd_snap *s = cetcd_snap_decode_kv(kv, sizeof(kv));
    CETCD_ASSERT_NOT_NULL(s);
    CETCD_ASSERT_EQ_INT((int)cetcd_snap_entry_count(s), 2);
    cetcd_snap_entry *e0 = cetcd_snap_get_entry(s, 0);
    CETCD_ASSERT_EQ_INT((int)e0->key_len, 1);
    CETCD_ASSERT_EQ_INT(e0->key[0], 'a');
    CETCD_ASSERT_EQ_INT((int)e0->value_len, 1);
    CETCD_ASSERT_EQ_INT(e0->value[0], 'b');
    cetcd_snap_entry *e1 = cetcd_snap_get_entry(s, 1);
    CETCD_ASSERT_EQ_INT((int)e1->key_len, 3);
    CETCD_ASSERT_EQ_INT(memcmp(e1->key, "foo", 3), 0);
    CETCD_ASSERT_EQ_INT(memcmp(e1->value, "bar", 3), 0);
    cetcd_snap_free(s);

    uint8_t hdr[16];
    memcpy(hdr, "CTS1", 4);
    memset(hdr + 4, 0, 8);
    hdr[4] = 7;
    memcpy(hdr + 12, kv, 4);
    s = cetcd_snap_decode_kv(hdr, 16);
    CETCD_ASSERT_NOT_NULL(s);
    CETCD_ASSERT_EQ_INT((int)cetcd_snap_entry_count(s), 1);
    CETCD_ASSERT_EQ_INT(cetcd_snap_get_entry(s, 0)->key[0], 'a');
    cetcd_snap_free(s);

    s = cetcd_snap_decode_kv(kv, 0);
    CETCD_ASSERT_NOT_NULL(s);
    CETCD_ASSERT_EQ_INT((int)cetcd_snap_entry_count(s), 0);
    cetcd_snap_free(s);
}

CETCD_TEST_CASE(snap_encode_kv_roundtrip) {
    cetcd_snap *s = cetcd_snap_new();
    CETCD_ASSERT_EQ_INT(cetcd_snap_add_entry(s, (const uint8_t *)"a", 1,
                                            (const uint8_t *)"b", 1, 0), 0);
    size_t len = 0;
    uint8_t *buf = cetcd_snap_encode_kv(s, &len);
    CETCD_ASSERT_NOT_NULL(buf);
    CETCD_ASSERT_TRUE(len > 0);
    cetcd_snap *s2 = cetcd_snap_decode_kv(buf, len);
    CETCD_ASSERT_NOT_NULL(s2);
    CETCD_ASSERT_EQ_INT((int)cetcd_snap_entry_count(s2), 1);
    CETCD_ASSERT_EQ_INT(cetcd_snap_get_entry(s2, 0)->key[0], 'a');
    CETCD_ASSERT_EQ_INT(cetcd_snap_get_entry(s2, 0)->value[0], 'b');
    free(buf);
    cetcd_snap_free(s);
    cetcd_snap_free(s2);
}

CETCD_TEST_CASE(snap_decode_kv_fail_closed) {
    uint8_t trunc[] = {0x01, 'a', 0x02};
    CETCD_ASSERT_TRUE(cetcd_snap_decode_kv(trunc, sizeof(trunc)) == NULL);
    uint8_t leftover[] = {0x01, 'a', 0x01, 'b', 0xff};
    CETCD_ASSERT_TRUE(cetcd_snap_decode_kv(leftover, sizeof(leftover)) == NULL);
    uint8_t huge[] = {0xff, 0xff, 0xff, 0xff, 0x0f};
    CETCD_ASSERT_TRUE(cetcd_snap_decode_kv(huge, sizeof(huge)) == NULL);
}

CETCD_TEST_CASE(snap_cts2_parse_verify_roundtrip) {
    uint8_t kv[] = {0x01, 'a', 0x01, 'b'};
    size_t n = 0;
    uint8_t *buf = cetcd_snap_encode_cts2(kv, sizeof(kv), 42, &n);
    CETCD_ASSERT_NOT_NULL(buf);
    CETCD_ASSERT_EQ_INT((int)n, 20);
    CETCD_ASSERT_EQ_INT(memcmp(buf, "CTS2", 4), 0);

    cetcd_snap_header h;
    CETCD_ASSERT_EQ_INT(cetcd_snap_parse_header(buf, n, &h), CETCD_OK);
    CETCD_ASSERT_EQ_INT((int)h.revision, 42);
    CETCD_ASSERT_EQ_INT(h.has_hash, 1);
    CETCD_ASSERT_EQ_INT((int)h.kv_off, 16);
    CETCD_ASSERT_EQ_INT((int)h.kv_len, 4);
    CETCD_ASSERT_TRUE(h.hash == cetcd_snap_crc32c(kv, sizeof(kv)));
    CETCD_ASSERT_EQ_INT(cetcd_snap_verify(buf, n), CETCD_OK);

    cetcd_snap *s = cetcd_snap_decode_kv(buf, n);
    CETCD_ASSERT_NOT_NULL(s);
    CETCD_ASSERT_EQ_INT((int)cetcd_snap_entry_count(s), 1);
    CETCD_ASSERT_EQ_INT(cetcd_snap_get_entry(s, 0)->key[0], 'a');
    cetcd_snap_free(s);
    free(buf);

    uint8_t *empty = cetcd_snap_encode_cts2(NULL, 0, 1, &n);
    CETCD_ASSERT_NOT_NULL(empty);
    CETCD_ASSERT_EQ_INT((int)n, 16);
    CETCD_ASSERT_EQ_INT(cetcd_snap_verify(empty, n), CETCD_OK);
    s = cetcd_snap_decode_kv(empty, n);
    CETCD_ASSERT_NOT_NULL(s);
    CETCD_ASSERT_EQ_INT((int)cetcd_snap_entry_count(s), 0);
    cetcd_snap_free(s);
    free(empty);
}

CETCD_TEST_CASE(snap_cts2_verify_fail_closed) {
    uint8_t kv[] = {0x01, 'a', 0x01, 'b'};
    size_t n = 0;
    uint8_t *buf = cetcd_snap_encode_cts2(kv, sizeof(kv), 7, &n);
    CETCD_ASSERT_NOT_NULL(buf);
    buf[12] ^= 0xff;
    CETCD_ASSERT_EQ_INT(cetcd_snap_verify(buf, n), CETCD_ERR_CORRUPT);
    cetcd_snap *s = cetcd_snap_decode_kv(buf, n);
    CETCD_ASSERT_NOT_NULL(s);
    CETCD_ASSERT_EQ_INT((int)cetcd_snap_entry_count(s), 1);
    cetcd_snap_free(s);
    free(buf);

    uint8_t trunc[] = {'C', 'T', 'S', '2', 1, 0, 0, 0, 0, 0, 0, 0};
    cetcd_snap_header bad;
    CETCD_ASSERT_EQ_INT(cetcd_snap_parse_header(trunc, sizeof(trunc), &bad),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_snap_verify(trunc, sizeof(trunc)), CETCD_ERR_INVAL);
    CETCD_ASSERT_TRUE(cetcd_snap_decode_kv(trunc, sizeof(trunc)) == NULL);
}

CETCD_TEST_CASE(snap_cts1_and_raw_have_no_hash) {
    uint8_t kv[] = {0x01, 'a', 0x01, 'b'};
    uint8_t cts1[16];
    memcpy(cts1, "CTS1", 4);
    memset(cts1 + 4, 0, 8);
    cts1[4] = 9;
    memcpy(cts1 + 12, kv, 4);

    cetcd_snap_header h;
    CETCD_ASSERT_EQ_INT(cetcd_snap_parse_header(cts1, sizeof(cts1), &h), CETCD_OK);
    CETCD_ASSERT_EQ_INT(h.has_hash, 0);
    CETCD_ASSERT_EQ_INT((int)h.revision, 9);
    CETCD_ASSERT_EQ_INT((int)h.kv_off, 12);
    CETCD_ASSERT_EQ_INT(cetcd_snap_verify(cts1, sizeof(cts1)), CETCD_OK);

    CETCD_ASSERT_EQ_INT(cetcd_snap_parse_header(kv, sizeof(kv), &h), CETCD_OK);
    CETCD_ASSERT_EQ_INT(h.has_hash, 0);
    CETCD_ASSERT_EQ_INT((int)h.kv_off, 0);
    CETCD_ASSERT_EQ_INT(cetcd_snap_verify(kv, sizeof(kv)), CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_snap_verify(NULL, 0), CETCD_OK);
}

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(snap_create_destroy),
    CETCD_TEST_ENTRY(snap_add_entries),
    CETCD_TEST_ENTRY(snap_encode_decode),
    CETCD_TEST_ENTRY(snap_empty_roundtrip),
    CETCD_TEST_ENTRY(snap_decode_corrupt),
    CETCD_TEST_ENTRY(snap_decode_kv_pairs_and_header),
    CETCD_TEST_ENTRY(snap_encode_kv_roundtrip),
    CETCD_TEST_ENTRY(snap_decode_kv_fail_closed),
    CETCD_TEST_ENTRY(snap_cts2_parse_verify_roundtrip),
    CETCD_TEST_ENTRY(snap_cts2_verify_fail_closed),
    CETCD_TEST_ENTRY(snap_cts1_and_raw_have_no_hash),
CETCD_TEST_LIST_END

CETCD_TEST_MAIN()
