#define _POSIX_C_SOURCE 200809L
#include "cetcd/base.h"
#include "cetcd/peer.h"
#include "cetcd/backend.h"
#include "cetcd_test.h"

#include <string.h>
#include <unistd.h>
#include <stdio.h>

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

CETCD_TEST_CASE(cluster_get_peer_by_index) {
    cetcd_cluster *c = cetcd_cluster_new(1);
    cetcd_peer_info p1 = {.id = 2, .addr = "10.0.0.1", .port = 2380};
    cetcd_peer_info p2 = {.id = 3, .addr = "10.0.0.2", .port = 2381};
    cetcd_cluster_add_peer(c, &p1);
    cetcd_cluster_add_peer(c, &p2);

    /* Index 0 → first peer */
    const cetcd_peer_info *pi0 = cetcd_cluster_get_peer_by_index(c, 0);
    CETCD_ASSERT_NOT_NULL(pi0);
    CETCD_ASSERT_TRUE(pi0->id == 2);
    CETCD_ASSERT_TRUE(strcmp(pi0->addr, "10.0.0.1") == 0);
    CETCD_ASSERT_EQ_INT((int)pi0->port, 2380);

    /* Index 1 → second peer */
    const cetcd_peer_info *pi1 = cetcd_cluster_get_peer_by_index(c, 1);
    CETCD_ASSERT_NOT_NULL(pi1);
    CETCD_ASSERT_TRUE(pi1->id == 3);

    /* Out-of-range → NULL */
    CETCD_ASSERT_TRUE(cetcd_cluster_get_peer_by_index(c, 2) == NULL);
    CETCD_ASSERT_TRUE(cetcd_cluster_get_peer_by_index(c, 99) == NULL);

    cetcd_cluster_free(c);
}

CETCD_TEST_CASE(cluster_self_id) {
    cetcd_cluster *c = cetcd_cluster_new(42);
    CETCD_ASSERT_TRUE(cetcd_cluster_self_id(c) == 42);
    CETCD_ASSERT_TRUE(cetcd_cluster_self_id(NULL) == 0);
    cetcd_cluster_free(c);
}

CETCD_TEST_CASE(cluster_update_peer) {
    cetcd_cluster *c = cetcd_cluster_new(1);
    cetcd_peer_info p1 = {.id = 2, .addr = "10.0.0.1", .port = 2380};
    cetcd_cluster_add_peer(c, &p1);

    /* Update peer's address and port */
    cetcd_peer_info updated = {.id = 2, .addr = "192.168.1.1", .port = 9999};
    int rc = cetcd_cluster_update_peer(c, 2, &updated);
    CETCD_ASSERT_EQ_INT(rc, CETCD_OK);

    /* Verify the update */
    const cetcd_peer_info *pi = cetcd_cluster_get_peer(c, 2);
    CETCD_ASSERT_NOT_NULL(pi);
    CETCD_ASSERT_TRUE(strcmp(pi->addr, "192.168.1.1") == 0);
    CETCD_ASSERT_EQ_INT((int)pi->port, 9999);

    /* Update non-existent peer should fail */
    rc = cetcd_cluster_update_peer(c, 99, &updated);
    CETCD_ASSERT_NE_INT(rc, CETCD_OK);

    cetcd_cluster_free(c);
}

CETCD_TEST_CASE(cluster_promote_learner) {
    cetcd_cluster *c = cetcd_cluster_new(1);
    cetcd_peer_info p = {.id = 2, .addr = "10.0.0.2", .port = 2380, .is_learner = 1};
    CETCD_ASSERT_EQ_INT(cetcd_cluster_add_peer(c, &p), CETCD_OK);
    const cetcd_peer_info *got = cetcd_cluster_get_peer(c, 2);
    CETCD_ASSERT_NOT_NULL(got);
    CETCD_ASSERT_EQ_INT(got->is_learner, 1);
    CETCD_ASSERT_EQ_INT(cetcd_cluster_promote(c, 2), CETCD_OK);
    got = cetcd_cluster_get_peer(c, 2);
    CETCD_ASSERT_EQ_INT(got->is_learner, 0);
    CETCD_ASSERT_EQ_INT(cetcd_cluster_promote(c, 2), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_cluster_promote(c, 99), CETCD_ERR_NOTFOUND);
    CETCD_ASSERT_EQ_INT((int)cetcd_cluster_alloc_id(c), 3);
    cetcd_cluster_free(c);
}

CETCD_TEST_CASE(cluster_members_persist_roundtrip) {
    char dir[] = "/tmp/cetcd-test-members-XXXXXX";
    CETCD_ASSERT_NOT_NULL(mkdtemp(dir));
    cetcd_backend_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.path = dir;
    cfg.map_size = 1024 * 1024;
    cfg.max_dbs = 8;
    cetcd_backend *be = cetcd_backend_open(&cfg);
    CETCD_ASSERT_NOT_NULL(be);

    cetcd_cluster *c = cetcd_cluster_new(1);
    cetcd_cluster_set_backend(c, be);
    cetcd_peer_info p = {.id = 2, .addr = "10.0.0.2", .port = 2380, .is_learner = 1};
    CETCD_ASSERT_EQ_INT(cetcd_cluster_add_peer(c, &p), CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_cluster_persist_peer(c, &p), CETCD_OK);
    cetcd_cluster_free(c);

    c = cetcd_cluster_new(1);
    CETCD_ASSERT_EQ_INT(cetcd_cluster_load(c, be), CETCD_OK);
    CETCD_ASSERT_EQ_INT((int)cetcd_cluster_peer_count(c), 1);
    const cetcd_peer_info *got = cetcd_cluster_get_peer(c, 2);
    CETCD_ASSERT_NOT_NULL(got);
    CETCD_ASSERT_EQ_INT(got->is_learner, 1);
    CETCD_ASSERT_EQ_INT((int)got->port, 2380);
    CETCD_ASSERT_EQ_INT(strcmp(got->addr, "10.0.0.2"), 0);
    CETCD_ASSERT_EQ_INT(cetcd_cluster_persist_del(c, 2), CETCD_OK);
    cetcd_cluster_free(c);

    c = cetcd_cluster_new(1);
    CETCD_ASSERT_EQ_INT(cetcd_cluster_load(c, be), CETCD_OK);
    CETCD_ASSERT_EQ_INT((int)cetcd_cluster_peer_count(c), 0);
    cetcd_cluster_free(c);
    cetcd_backend_close(be);
}

CETCD_TEST_CASE(cluster_joint_persist_roundtrip) {
    char dir[] = "/tmp/cetcd-test-joint-XXXXXX";
    CETCD_ASSERT_NOT_NULL(mkdtemp(dir));
    cetcd_backend_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.path = dir;
    cfg.map_size = 1024 * 1024;
    cfg.max_dbs = 8;
    cetcd_backend *be = cetcd_backend_open(&cfg);
    CETCD_ASSERT_NOT_NULL(be);

    cetcd_cluster *c = cetcd_cluster_new(1);
    cetcd_cluster_set_backend(c, be);
    uint64_t ids[2] = {1, 2};
    CETCD_ASSERT_EQ_INT(cetcd_cluster_persist_joint(c, ids, 2, 7), CETCD_OK);
    cetcd_cluster_free(c);

    c = cetcd_cluster_new(1);
    CETCD_ASSERT_EQ_INT(cetcd_cluster_load(c, be), CETCD_OK);
    uint64_t got[4];
    uint64_t jidx = 0;
    CETCD_ASSERT_EQ_INT((int)cetcd_cluster_loaded_joint(c, got, 4, &jidx), 2);
    CETCD_ASSERT_TRUE(jidx == 7);
    CETCD_ASSERT_TRUE(got[0] == 1 && got[1] == 2);
    CETCD_ASSERT_EQ_INT(cetcd_cluster_persist_clear_joint(c), CETCD_OK);
    cetcd_cluster_free(c);

    c = cetcd_cluster_new(1);
    CETCD_ASSERT_EQ_INT(cetcd_cluster_load(c, be), CETCD_OK);
    CETCD_ASSERT_EQ_INT((int)cetcd_cluster_loaded_joint(c, got, 4, &jidx), 0);
    cetcd_cluster_free(c);
    cetcd_backend_close(be);
}

CETCD_TEST_CASE(peer_rafthttp_path) {
    CETCD_ASSERT_EQ_INT(cetcd_peer_is_rafthttp_path("/raft"), 1);
    CETCD_ASSERT_EQ_INT(cetcd_peer_is_rafthttp_path("/raft/stream/message"), 0);
    CETCD_ASSERT_EQ_INT(cetcd_peer_is_rafthttp_path("/etcdserverpb.KV/Range"), 0);
    CETCD_ASSERT_EQ_INT(cetcd_peer_is_rafthttp_path(""), 0);
    CETCD_ASSERT_EQ_INT(cetcd_peer_is_rafthttp_path(NULL), 0);
}

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(peer_create_destroy),
    CETCD_TEST_ENTRY(cluster_create_add_remove),
    CETCD_TEST_ENTRY(cluster_add_duplicate),
    CETCD_TEST_ENTRY(cluster_remove_nonexistent),
    CETCD_TEST_ENTRY(cluster_send_msg),
    CETCD_TEST_ENTRY(msg_encode_decode_roundtrip),
    CETCD_TEST_ENTRY(cluster_get_peer_by_index),
    CETCD_TEST_ENTRY(cluster_self_id),
    CETCD_TEST_ENTRY(cluster_update_peer),
    CETCD_TEST_ENTRY(cluster_promote_learner),
    CETCD_TEST_ENTRY(cluster_members_persist_roundtrip),
    CETCD_TEST_ENTRY(cluster_joint_persist_roundtrip),
    CETCD_TEST_ENTRY(peer_rafthttp_path),
CETCD_TEST_LIST_END

CETCD_TEST_MAIN()
