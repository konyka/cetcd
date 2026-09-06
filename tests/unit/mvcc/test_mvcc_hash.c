#include "cetcd/mvcc.h"
#include "cetcd_test.h"

CETCD_TEST_CASE(mvcc_hash_kv_empty_is_zero) {
    cetcd_mvcc_store *s = cetcd_mvcc_store_new();
    uint32_t h = 0xdeadbeefu;
    CETCD_ASSERT_EQ_INT(cetcd_mvcc_hash_kv(s, 0, &h), CETCD_OK);
    CETCD_ASSERT_EQ_INT((int)h, 0);
    CETCD_ASSERT_EQ_INT(cetcd_mvcc_hash_kv(NULL, 0, &h), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_mvcc_hash_kv(s, 0, NULL), CETCD_ERR_INVAL);
    cetcd_mvcc_store_free(s);
}

CETCD_TEST_CASE(mvcc_hash_kv_same_rev_different_values_differ) {
    cetcd_mvcc_store *a = cetcd_mvcc_store_new();
    cetcd_mvcc_store *b = cetcd_mvcc_store_new();
    CETCD_ASSERT_TRUE(cetcd_mvcc_put(a, (const uint8_t *)"k", 1,
                                     (const uint8_t *)"x", 1, 0).main == 1);
    CETCD_ASSERT_TRUE(cetcd_mvcc_put(b, (const uint8_t *)"k", 1,
                                     (const uint8_t *)"y", 1, 0).main == 1);
    uint32_t ha = 0, hb = 0;
    CETCD_ASSERT_EQ_INT(cetcd_mvcc_hash_kv(a, 1, &ha), CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_mvcc_hash_kv(b, 1, &hb), CETCD_OK);
    CETCD_ASSERT_TRUE(ha != hb);
    cetcd_mvcc_store_free(a);
    cetcd_mvcc_store_free(b);
}

CETCD_TEST_CASE(mvcc_hash_kv_same_set_matches_regardless_of_put_order) {
    cetcd_mvcc_store *a = cetcd_mvcc_store_new();
    cetcd_mvcc_store *b = cetcd_mvcc_store_new();
    cetcd_mvcc_put(a, (const uint8_t *)"a", 1, (const uint8_t *)"1", 1, 0);
    cetcd_mvcc_put(a, (const uint8_t *)"b", 1, (const uint8_t *)"2", 1, 0);
    cetcd_mvcc_put(b, (const uint8_t *)"b", 1, (const uint8_t *)"2", 1, 0);
    cetcd_mvcc_put(b, (const uint8_t *)"a", 1, (const uint8_t *)"1", 1, 0);
    uint32_t ha = 0, hb = 0;
    CETCD_ASSERT_EQ_INT(cetcd_mvcc_hash_kv(a, 0, &ha), CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_mvcc_hash_kv(b, 0, &hb), CETCD_OK);
    CETCD_ASSERT_EQ_INT((int)ha, (int)hb);
    cetcd_mvcc_store_free(a);
    cetcd_mvcc_store_free(b);
}

CETCD_TEST_CASE(mvcc_hash_kv_future_and_compacted_fail) {
    cetcd_mvcc_store *s = cetcd_mvcc_store_new();
    cetcd_mvcc_put(s, (const uint8_t *)"k", 1, (const uint8_t *)"v", 1, 0);
    cetcd_mvcc_put(s, (const uint8_t *)"k", 1, (const uint8_t *)"w", 1, 0);
    uint32_t h = 0;
    CETCD_ASSERT_EQ_INT(cetcd_mvcc_hash_kv(s, 99, &h), CETCD_ERR_RANGE);
    CETCD_ASSERT_EQ_INT(cetcd_mvcc_compact(s, 2), CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_mvcc_hash_kv(s, 1, &h), CETCD_ERR_RANGE);
    CETCD_ASSERT_EQ_INT(cetcd_mvcc_hash_kv(s, 2, &h), CETCD_OK);
    cetcd_mvcc_store_free(s);
}

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(mvcc_hash_kv_empty_is_zero),
    CETCD_TEST_ENTRY(mvcc_hash_kv_same_rev_different_values_differ),
    CETCD_TEST_ENTRY(mvcc_hash_kv_same_set_matches_regardless_of_put_order),
    CETCD_TEST_ENTRY(mvcc_hash_kv_future_and_compacted_fail),
CETCD_TEST_LIST_END

CETCD_TEST_MAIN()
