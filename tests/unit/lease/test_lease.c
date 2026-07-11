#include "cetcd/base.h"
#include "cetcd/lease.h"
#include "cetcd/mvcc.h"
#include "cetcd_test.h"

static int g_expired_count;
static cetcd_lease_id g_expired_id;

static void test_expire_cb(cetcd_lease_id id,
                            const uint8_t *const *keys,
                            const size_t *key_lens,
                            size_t count,
                            void *udata) {
    (void)keys; (void)key_lens; (void)udata;
    g_expired_count = (int)count;
    g_expired_id = id;
}

CETCD_TEST_CASE(lease_grant_revoke) {
    cetcd_lease_mgr *mgr = cetcd_lease_mgr_new(test_expire_cb, NULL);
    CETCD_ASSERT_NOT_NULL(mgr);

    cetcd_lease_id l1 = cetcd_lease_grant(mgr, 10);
    CETCD_ASSERT_TRUE(l1 > 0);
    CETCD_ASSERT_TRUE(cetcd_lease_exists(mgr, l1));
    CETCD_ASSERT_EQ_INT((int)cetcd_lease_mgr_count(mgr), 1);

    cetcd_lease_id l2 = cetcd_lease_grant(mgr, 20);
    CETCD_ASSERT_TRUE(l2 > 0);
    CETCD_ASSERT_TRUE(l2 != l1);
    CETCD_ASSERT_EQ_INT((int)cetcd_lease_mgr_count(mgr), 2);

    CETCD_ASSERT_EQ_INT(cetcd_lease_revoke(mgr, l1), CETCD_OK);
    CETCD_ASSERT_FALSE(cetcd_lease_exists(mgr, l1));
    CETCD_ASSERT_EQ_INT((int)cetcd_lease_mgr_count(mgr), 1);

    CETCD_ASSERT_EQ_INT(cetcd_lease_revoke(mgr, l2), CETCD_OK);
    CETCD_ASSERT_EQ_INT((int)cetcd_lease_mgr_count(mgr), 0);

    cetcd_lease_mgr_free(mgr);
}

CETCD_TEST_CASE(lease_expire) {
    g_expired_count = 0;
    g_expired_id = 0;

    cetcd_lease_mgr *mgr = cetcd_lease_mgr_new(test_expire_cb, NULL);
    cetcd_lease_id l1 = cetcd_lease_grant(mgr, 5);
    CETCD_ASSERT_TRUE(l1 > 0);

    CETCD_ASSERT_TRUE(cetcd_lease_ttl_remaining(mgr, l1) <= 5);

    cetcd_lease_mgr_tick(mgr, 4000);
    CETCD_ASSERT_TRUE(cetcd_lease_exists(mgr, l1));

    cetcd_lease_mgr_tick(mgr, 2000);
    CETCD_ASSERT_FALSE(cetcd_lease_exists(mgr, l1));
    CETCD_ASSERT_TRUE(g_expired_id == l1);

    cetcd_lease_mgr_free(mgr);
}

CETCD_TEST_CASE(lease_keep_alive) {
    g_expired_count = 0;
    g_expired_id = 0;

    cetcd_lease_mgr *mgr = cetcd_lease_mgr_new(test_expire_cb, NULL);
    cetcd_lease_id l1 = cetcd_lease_grant(mgr, 10);

    cetcd_lease_mgr_tick(mgr, 5000);
    CETCD_ASSERT_TRUE(cetcd_lease_exists(mgr, l1));

    cetcd_lease_keep_alive(mgr, l1, 10);
    cetcd_lease_mgr_tick(mgr, 5000);
    CETCD_ASSERT_TRUE(cetcd_lease_exists(mgr, l1));

    cetcd_lease_mgr_tick(mgr, 11000);
    CETCD_ASSERT_FALSE(cetcd_lease_exists(mgr, l1));

    cetcd_lease_mgr_free(mgr);
}

CETCD_TEST_CASE(lease_attach_detach_key) {
    g_expired_count = 0;
    g_expired_id = 0;

    cetcd_lease_mgr *mgr = cetcd_lease_mgr_new(test_expire_cb, NULL);
    cetcd_lease_id l1 = cetcd_lease_grant(mgr, 5);

    const uint8_t k1[] = "key1";
    const uint8_t k2[] = "key2";
    CETCD_ASSERT_EQ_INT(cetcd_lease_attach_key(mgr, l1, k1, 4), CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_lease_attach_key(mgr, l1, k2, 4), CETCD_OK);

    cetcd_lease_mgr_tick(mgr, 6000);
    CETCD_ASSERT_FALSE(cetcd_lease_exists(mgr, l1));
    CETCD_ASSERT_EQ_INT(g_expired_count, 2);
    CETCD_ASSERT_TRUE(g_expired_id == l1);

    cetcd_lease_mgr_free(mgr);
}

CETCD_TEST_CASE(lease_attach_idempotent) {
    cetcd_lease_mgr *mgr = cetcd_lease_mgr_new(test_expire_cb, NULL);
    cetcd_lease_id l1 = cetcd_lease_grant(mgr, 30);
    const uint8_t k[] = "same";

    CETCD_ASSERT_EQ_INT(cetcd_lease_attach_key(mgr, l1, k, 4), CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_lease_attach_key(mgr, l1, k, 4), CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_lease_attach_key(mgr, l1, k, 4), CETCD_OK);

    const uint8_t *const *keys = NULL;
    const size_t *lens = NULL;
    size_t n = cetcd_lease_keys(mgr, l1, &keys, &lens);
    CETCD_ASSERT_EQ_INT((int)n, 1);

    CETCD_ASSERT_EQ_INT(cetcd_lease_detach_key(mgr, l1, k, 4), CETCD_OK);
    n = cetcd_lease_keys(mgr, l1, &keys, &lens);
    CETCD_ASSERT_EQ_INT((int)n, 0);

    cetcd_lease_mgr_free(mgr);
}

CETCD_TEST_CASE(lease_revoke_nonexistent) {
    cetcd_lease_mgr *mgr = cetcd_lease_mgr_new(test_expire_cb, NULL);
    CETCD_ASSERT_NE_INT(cetcd_lease_revoke(mgr, 9999), CETCD_OK);
    CETCD_ASSERT_FALSE(cetcd_lease_exists(mgr, 9999));
    cetcd_lease_mgr_free(mgr);
}

CETCD_TEST_CASE(lease_granted_ttl) {
    cetcd_lease_mgr *mgr = cetcd_lease_mgr_new(test_expire_cb, NULL);

    cetcd_lease_id l1 = cetcd_lease_grant(mgr, 30);
    CETCD_ASSERT_TRUE(l1 > 0);
    CETCD_ASSERT_EQ_INT((int)cetcd_lease_granted_ttl(mgr, l1), 30);

    cetcd_lease_id l2 = cetcd_lease_grant(mgr, 120);
    CETCD_ASSERT_TRUE(l2 > 0);
    CETCD_ASSERT_EQ_INT((int)cetcd_lease_granted_ttl(mgr, l2), 120);

    /* Non-existent lease returns 0 */
    CETCD_ASSERT_EQ_INT((int)cetcd_lease_granted_ttl(mgr, 99999), 0);

    /* NULL mgr returns 0 */
    CETCD_ASSERT_EQ_INT((int)cetcd_lease_granted_ttl(NULL, l1), 0);

    cetcd_lease_mgr_free(mgr);
}

CETCD_TEST_CASE(lease_mgr_leases) {
    cetcd_lease_mgr *mgr = cetcd_lease_mgr_new(test_expire_cb, NULL);

    /* No leases yet */
    CETCD_ASSERT_EQ_INT((int)cetcd_lease_mgr_leases(mgr, NULL, 0), 0);

    cetcd_lease_id l1 = cetcd_lease_grant(mgr, 10);
    cetcd_lease_id l2 = cetcd_lease_grant(mgr, 20);
    cetcd_lease_id l3 = cetcd_lease_grant(mgr, 30);

    /* Query count with cap=0 */
    CETCD_ASSERT_EQ_INT((int)cetcd_lease_mgr_leases(mgr, NULL, 0), 3);

    /* Collect IDs */
    cetcd_lease_id ids[3];
    size_t n = cetcd_lease_mgr_leases(mgr, ids, 3);
    CETCD_ASSERT_EQ_INT((int)n, 3);
    CETCD_ASSERT_TRUE(ids[0] == l1);
    CETCD_ASSERT_TRUE(ids[1] == l2);
    CETCD_ASSERT_TRUE(ids[2] == l3);

    /* Partial fill: cap=2 should return only 2 */
    cetcd_lease_id ids2[2];
    n = cetcd_lease_mgr_leases(mgr, ids2, 2);
    CETCD_ASSERT_EQ_INT((int)n, 2);
    CETCD_ASSERT_TRUE(ids2[0] == l1);
    CETCD_ASSERT_TRUE(ids2[1] == l2);

    /* Revoke one and re-check */
    cetcd_lease_revoke(mgr, l2);
    CETCD_ASSERT_EQ_INT((int)cetcd_lease_mgr_leases(mgr, NULL, 0), 2);
    n = cetcd_lease_mgr_leases(mgr, ids, 3);
    CETCD_ASSERT_EQ_INT((int)n, 2);
    CETCD_ASSERT_TRUE(ids[0] == l1);
    CETCD_ASSERT_TRUE(ids[1] == l3);

    cetcd_lease_mgr_free(mgr);
}

CETCD_TEST_CASE(lease_grant_custom_id) {
    cetcd_lease_mgr *mgr = cetcd_lease_mgr_new(test_expire_cb, NULL);
    CETCD_ASSERT_NOT_NULL(mgr);

    cetcd_lease_id lid = cetcd_lease_grant_id(mgr, 0xBEEF, 30);
    CETCD_ASSERT_EQ_INT((int)lid, 0xBEEF);
    CETCD_ASSERT_TRUE(cetcd_lease_exists(mgr, 0xBEEF));
    CETCD_ASSERT_EQ_INT((int)cetcd_lease_granted_ttl(mgr, 0xBEEF), 30);

    /* Duplicate ID fails closed. */
    CETCD_ASSERT_EQ_INT((int)cetcd_lease_grant_id(mgr, 0xBEEF, 10), 0);
    CETCD_ASSERT_EQ_INT((int)cetcd_lease_mgr_count(mgr), 1);

    /* id==0 / ttl<=0 rejected. */
    CETCD_ASSERT_EQ_INT((int)cetcd_lease_grant_id(mgr, 0, 10), 0);
    CETCD_ASSERT_EQ_INT((int)cetcd_lease_grant_id(mgr, 42, 0), 0);

    cetcd_lease_mgr_free(mgr);
}

CETCD_TEST_CASE(lease_grant_custom_id_bumps_next) {
    cetcd_lease_mgr *mgr = cetcd_lease_mgr_new(test_expire_cb, NULL);
    CETCD_ASSERT_NOT_NULL(mgr);

    CETCD_ASSERT_EQ_INT((int)cetcd_lease_grant_id(mgr, 0x1000, 10), 0x1000);
    cetcd_lease_id auto_id = cetcd_lease_grant(mgr, 10);
    CETCD_ASSERT_EQ_INT((int)auto_id, 0x1001);

    cetcd_lease_mgr_free(mgr);
}

CETCD_TEST_CASE(lease_reindex_from_store) {
    cetcd_lease_mgr *mgr = cetcd_lease_mgr_new(test_expire_cb, NULL);
    cetcd_mvcc_store *store = cetcd_mvcc_store_new();
    CETCD_ASSERT_NOT_NULL(mgr);
    CETCD_ASSERT_NOT_NULL(store);

    const uint8_t k1[] = "lk1", k2[] = "lk2", k3[] = "nolease", v[] = "v";
    cetcd_mvcc_put(store, k1, 3, v, 1, 0x42);
    cetcd_mvcc_put(store, k2, 3, v, 1, 0x42);
    cetcd_mvcc_put(store, k3, 7, v, 1, 0);

    CETCD_ASSERT_EQ_INT(cetcd_lease_reindex_from_store(mgr, store), CETCD_OK);
    CETCD_ASSERT_TRUE(cetcd_lease_exists(mgr, 0x42));
    CETCD_ASSERT_EQ_INT((int)cetcd_lease_mgr_count(mgr), 1);
    CETCD_ASSERT_EQ_INT((int)cetcd_lease_granted_ttl(mgr, 0x42), 300);

    const uint8_t *const *keys = NULL;
    const size_t *lens = NULL;
    size_t n = cetcd_lease_keys(mgr, 0x42, &keys, &lens);
    CETCD_ASSERT_EQ_INT((int)n, 2);

    /* Idempotent reindex. */
    CETCD_ASSERT_EQ_INT(cetcd_lease_reindex_from_store(mgr, store), CETCD_OK);
    CETCD_ASSERT_EQ_INT((int)cetcd_lease_mgr_count(mgr), 1);
    n = cetcd_lease_keys(mgr, 0x42, &keys, &lens);
    CETCD_ASSERT_EQ_INT((int)n, 2);

    cetcd_mvcc_store_free(store);
    cetcd_lease_mgr_free(mgr);
}

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(lease_grant_revoke),
    CETCD_TEST_ENTRY(lease_expire),
    CETCD_TEST_ENTRY(lease_keep_alive),
    CETCD_TEST_ENTRY(lease_attach_detach_key),
    CETCD_TEST_ENTRY(lease_attach_idempotent),
    CETCD_TEST_ENTRY(lease_revoke_nonexistent),
    CETCD_TEST_ENTRY(lease_granted_ttl),
    CETCD_TEST_ENTRY(lease_mgr_leases),
    CETCD_TEST_ENTRY(lease_grant_custom_id),
    CETCD_TEST_ENTRY(lease_grant_custom_id_bumps_next),
    CETCD_TEST_ENTRY(lease_reindex_from_store),
CETCD_TEST_LIST_END

CETCD_TEST_MAIN()
