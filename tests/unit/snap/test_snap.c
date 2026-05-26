#include "cetcd/base.h"
#include "cetcd/snap.h"
#include "cetcd_test.h"

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

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(snap_create_destroy),
    CETCD_TEST_ENTRY(snap_add_entries),
    CETCD_TEST_ENTRY(snap_encode_decode),
    CETCD_TEST_ENTRY(snap_empty_roundtrip),
    CETCD_TEST_ENTRY(snap_decode_corrupt),
CETCD_TEST_LIST_END

CETCD_TEST_MAIN()
