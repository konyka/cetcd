#include "cetcd/base.h"
#include "cetcd/peer.h"
#include "cetcd_test.h"

CETCD_TEST_CASE(peer_create_destroy) {
    cetcd_peer *p = cetcd_peer_new(1, "127.0.0.1", 2379);
    CETCD_ASSERT_NOT_NULL(p);
    cetcd_peer_free(p);
}

CETCD_TEST_CASE(cluster_create_add_remove) {
    cetcd_cluster *c = cetcd_cluster_new(1);
    CETCD_ASSERT_NOT_NULL(c);

    cetcd_peer_info p1 = {.id = 2, .addr = "127.0.0.1", .port = 2380};
    cetcd_peer_info p2 = {.id = 3, .addr = "127.0.0.1", .port = 2381};
    CETCD_ASSERT_EQ_INT(cetcd_cluster_add_peer(c, &p1), CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_cluster_add_peer(c, &p2), CETCD_OK);
    CETCD_ASSERT_EQ_INT((int)cetcd_cluster_peer_count(c), 2);

    CETCD_ASSERT_EQ_INT(cetcd_cluster_remove_peer(c, 2), CETCD_OK);
    CETCD_ASSERT_EQ_INT((int)cetcd_cluster_peer_count(c), 1);

    cetcd_cluster_free(c);
}

CETCD_TEST_CASE(cluster_add_duplicate) {
    cetcd_cluster *c = cetcd_cluster_new(1);
    cetcd_peer_info p1 = {.id = 2, .addr = "127.0.0.1", .port = 2380};
    CETCD_ASSERT_EQ_INT(cetcd_cluster_add_peer(c, &p1), CETCD_OK);
    CETCD_ASSERT_NE_INT(cetcd_cluster_add_peer(c, &p1), CETCD_OK);
    cetcd_cluster_free(c);
}

CETCD_TEST_CASE(cluster_remove_nonexistent) {
    cetcd_cluster *c = cetcd_cluster_new(1);
    CETCD_ASSERT_NE_INT(cetcd_cluster_remove_peer(c, 99), CETCD_OK);
    cetcd_cluster_free(c);
}

static int g_send_count;
static uint64_t g_last_to;
static size_t g_last_len;

static void test_send_fn(uint64_t to_id, const uint8_t *data, size_t len, void *udata) {
    (void)data; (void)udata;
    g_send_count++;
    g_last_to = to_id;
    g_last_len = len;
}

CETCD_TEST_CASE(cluster_send_msg) {
    cetcd_cluster *c = cetcd_cluster_new(1);
    cetcd_peer_info p2 = {.id = 2, .addr = "127.0.0.1", .port = 2380};
    cetcd_cluster_add_peer(c, &p2);
    cetcd_cluster_set_sender(c, test_send_fn, NULL);

    g_send_count = 0;
    const uint8_t msg[] = {0x01, 0x02, 0x03};
    int rc = cetcd_cluster_send_msg(c, msg, sizeof(msg), 2);
    CETCD_ASSERT_EQ_INT(rc, CETCD_OK);
    CETCD_ASSERT_EQ_INT(g_send_count, 1);
    CETCD_ASSERT_TRUE(g_last_to == 2);
    CETCD_ASSERT_EQ_INT((int)g_last_len, 3);

    cetcd_cluster_free(c);
}

CETCD_TEST_CASE(msg_encode_decode_roundtrip) {
    const uint8_t msg[] = {0xAA, 0xBB, 0xCC, 0xDD};
    uint8_t *encoded = NULL;
    size_t enc_len = cetcd_msg_encode(msg, sizeof(msg), &encoded);
    CETCD_ASSERT_TRUE(enc_len > sizeof(msg));
    CETCD_ASSERT_NOT_NULL(encoded);

    uint8_t *decoded = NULL;
    size_t dec_len = 0;
    int rc = cetcd_msg_decode(encoded, enc_len, &decoded, &dec_len);
    CETCD_ASSERT_EQ_INT(rc, CETCD_OK);
    CETCD_ASSERT_EQ_INT((int)dec_len, (int)sizeof(msg));
    CETCD_ASSERT_TRUE(memcmp(decoded, msg, dec_len) == 0);

    free(encoded);
    free(decoded);
}

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(peer_create_destroy),
    CETCD_TEST_ENTRY(cluster_create_add_remove),
    CETCD_TEST_ENTRY(cluster_add_duplicate),
    CETCD_TEST_ENTRY(cluster_remove_nonexistent),
    CETCD_TEST_ENTRY(cluster_send_msg),
    CETCD_TEST_ENTRY(msg_encode_decode_roundtrip),
CETCD_TEST_LIST_END

CETCD_TEST_MAIN()
