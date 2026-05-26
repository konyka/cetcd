#include "cetcd/base.h"
#include "cetcd/lease.h"
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

CETCD_TEST_CASE(lease_revoke_nonexistent) {
    cetcd_lease_mgr *mgr = cetcd_lease_mgr_new(test_expire_cb, NULL);
    CETCD_ASSERT_NE_INT(cetcd_lease_revoke(mgr, 9999), CETCD_OK);
    CETCD_ASSERT_FALSE(cetcd_lease_exists(mgr, 9999));
    cetcd_lease_mgr_free(mgr);
}

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(lease_grant_revoke),
    CETCD_TEST_ENTRY(lease_expire),
    CETCD_TEST_ENTRY(lease_keep_alive),
    CETCD_TEST_ENTRY(lease_attach_detach_key),
    CETCD_TEST_ENTRY(lease_revoke_nonexistent),
CETCD_TEST_LIST_END

CETCD_TEST_MAIN()
