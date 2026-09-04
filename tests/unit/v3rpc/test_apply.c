#define _POSIX_C_SOURCE 200809L
#include "cetcd/v3rpc.h"
#include "cetcd/mvcc.h"
#include "cetcd/lease.h"
#include "cetcd/peer.h"
#include "cetcd/backend.h"
#include "cetcd/auth.h"
#include "cetcd_test.h"

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>

extern cetcd_cluster *g_rpc_cluster;

CETCD_TEST_CASE(apply_put_delete_roundtrip) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();
    CETCD_ASSERT_NOT_NULL(rpc);

    uint8_t *buf = NULL;
    size_t len = 0;
    CETCD_ASSERT_EQ_INT(cetcd_apply_encode_put(&buf, &len,
        (const uint8_t *)"k", 1, (const uint8_t *)"v", 1, 0), 0);
    CETCD_ASSERT_TRUE(len >= 3);
    CETCD_ASSERT_EQ_INT((int)buf[0], CETCD_APPLY_PUT);
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_apply_entry(buf, len), 0);
    free(buf);

    cetcd_kv kv;
    memset(&kv, 0, sizeof(kv));
    CETCD_ASSERT_EQ_INT(cetcd_mvcc_get(cetcd_v3rpc_store(rpc), 0,
                                       (const uint8_t *)"k", 1, &kv), 0);
    CETCD_ASSERT_EQ_INT((int)kv.value.len, 1);
    CETCD_ASSERT_EQ_INT((int)kv.value.data[0], (int)'v');
    free((void *)(uintptr_t)kv.key.data);
    free((void *)(uintptr_t)kv.value.data);

    CETCD_ASSERT_EQ_INT(cetcd_apply_encode_delete(&buf, &len,
        (const uint8_t *)"k", 1), 0);
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_apply_entry(buf, len), 0);
    free(buf);
    CETCD_ASSERT_TRUE(cetcd_mvcc_get(cetcd_v3rpc_store(rpc), 0,
                                     (const uint8_t *)"k", 1, &kv) != 0);

    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(apply_rejects_truncated) {
    uint8_t bad[] = { CETCD_APPLY_PUT, 0x05, 'a' }; /* claims 5-byte key */
    CETCD_ASSERT_TRUE(cetcd_v3rpc_apply_entry(bad, sizeof(bad)) != 0);
    CETCD_ASSERT_TRUE(cetcd_v3rpc_apply_entry(NULL, 0) != 0);
}

CETCD_TEST_CASE(apply_delete_range) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();
    uint8_t *buf = NULL;
    size_t len = 0;
    CETCD_ASSERT_EQ_INT(cetcd_apply_encode_put(&buf, &len,
        (const uint8_t *)"a1", 2, (const uint8_t *)"x", 1, 0), 0);
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_apply_entry(buf, len), 0);
    free(buf);
    CETCD_ASSERT_EQ_INT(cetcd_apply_encode_put(&buf, &len,
        (const uint8_t *)"a2", 2, (const uint8_t *)"y", 1, 0), 0);
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_apply_entry(buf, len), 0);
    free(buf);
    CETCD_ASSERT_EQ_INT(cetcd_apply_encode_put(&buf, &len,
        (const uint8_t *)"b", 1, (const uint8_t *)"z", 1, 0), 0);
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_apply_entry(buf, len), 0);
    free(buf);

    CETCD_ASSERT_EQ_INT(cetcd_apply_encode_delete_range(&buf, &len,
        (const uint8_t *)"a", 1, (const uint8_t *)"b", 1), 0);
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_apply_entry(buf, len), 0);
    free(buf);

    cetcd_kv kv;
    memset(&kv, 0, sizeof(kv));
    CETCD_ASSERT_TRUE(cetcd_mvcc_get(cetcd_v3rpc_store(rpc), 0,
                                     (const uint8_t *)"a1", 2, &kv) != 0);
    CETCD_ASSERT_EQ_INT(cetcd_mvcc_get(cetcd_v3rpc_store(rpc), 0,
                                       (const uint8_t *)"b", 1, &kv), 0);
    free((void *)(uintptr_t)kv.key.data);
    free((void *)(uintptr_t)kv.value.data);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(propose_or_apply_local_without_raft) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();
    uint8_t *buf = NULL;
    size_t len = 0;
    CETCD_ASSERT_EQ_INT(cetcd_apply_encode_put(&buf, &len,
        (const uint8_t *)"p", 1, (const uint8_t *)"q", 1, 0), 0);
    int rc = cetcd_v3rpc_propose_or_apply(buf, len);
    CETCD_ASSERT_TRUE(rc >= 0);
    free(buf);
    cetcd_kv kv;
    memset(&kv, 0, sizeof(kv));
    CETCD_ASSERT_EQ_INT(cetcd_mvcc_get(cetcd_v3rpc_store(rpc), 0,
                                       (const uint8_t *)"p", 1, &kv), 0);
    free((void *)(uintptr_t)kv.key.data);
    free((void *)(uintptr_t)kv.value.data);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(apply_batch_two_puts) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();
    uint8_t *a = NULL, *b = NULL, *batch = NULL;
    size_t alen = 0, blen = 0, batch_len = 0;
    CETCD_ASSERT_EQ_INT(cetcd_apply_encode_put(&a, &alen,
        (const uint8_t *)"b1", 2, (const uint8_t *)"x", 1, 0), 0);
    CETCD_ASSERT_EQ_INT(cetcd_apply_encode_put(&b, &blen,
        (const uint8_t *)"b2", 2, (const uint8_t *)"y", 1, 0), 0);
    const uint8_t *ops[2] = { a, b };
    size_t lens[2] = { alen, blen };
    CETCD_ASSERT_EQ_INT(cetcd_apply_encode_batch(&batch, &batch_len, ops, lens, 2), 0);
    CETCD_ASSERT_EQ_INT((int)batch[0], CETCD_APPLY_BATCH);
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_apply_entry(batch, batch_len), 0);
    free(a); free(b); free(batch);

    cetcd_kv kv;
    memset(&kv, 0, sizeof(kv));
    CETCD_ASSERT_EQ_INT(cetcd_mvcc_get(cetcd_v3rpc_store(rpc), 0,
                                       (const uint8_t *)"b1", 2, &kv), 0);
    CETCD_ASSERT_EQ_INT((int)kv.value.data[0], (int)'x');
    free((void *)(uintptr_t)kv.key.data);
    free((void *)(uintptr_t)kv.value.data);
    CETCD_ASSERT_EQ_INT(cetcd_mvcc_get(cetcd_v3rpc_store(rpc), 0,
                                       (const uint8_t *)"b2", 2, &kv), 0);
    CETCD_ASSERT_EQ_INT((int)kv.value.data[0], (int)'y');
    free((void *)(uintptr_t)kv.key.data);
    free((void *)(uintptr_t)kv.value.data);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(apply_batch_rejects_nesting) {
    uint8_t inner[] = { CETCD_APPLY_BATCH, 0x01, 0x01, 0x01 };
    uint8_t outer[16];
    size_t p = 0;
    outer[p++] = CETCD_APPLY_BATCH;
    outer[p++] = 0x01; /* n=1 */
    outer[p++] = (uint8_t)sizeof(inner);
    memcpy(outer + p, inner, sizeof(inner));
    p += sizeof(inner);
    CETCD_ASSERT_TRUE(cetcd_v3rpc_apply_entry(outer, p) != 0);
}

CETCD_TEST_CASE(apply_member_add_remove) {
    cetcd_cluster *saved = g_rpc_cluster;
    g_rpc_cluster = cetcd_cluster_new(1);
    uint8_t *buf = NULL;
    size_t len = 0;
    CETCD_ASSERT_EQ_INT(cetcd_apply_encode_member_add(&buf, &len, 2, 1,
        "10.0.0.2", 2380), 0);
    CETCD_ASSERT_EQ_INT((int)buf[0], CETCD_APPLY_MEMBER_ADD);
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_apply_entry(buf, len), 0);
    free(buf);
    const cetcd_peer_info *got = cetcd_cluster_get_peer(g_rpc_cluster, 2);
    CETCD_ASSERT_NOT_NULL(got);
    CETCD_ASSERT_EQ_INT(got->is_learner, 1);
    CETCD_ASSERT_EQ_INT(cetcd_apply_encode_member_promote(&buf, &len, 2), 0);
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_apply_entry(buf, len), 0);
    free(buf);
    CETCD_ASSERT_EQ_INT(cetcd_cluster_get_peer(g_rpc_cluster, 2)->is_learner, 0);
    CETCD_ASSERT_EQ_INT(cetcd_apply_encode_member_remove(&buf, &len, 2), 0);
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_apply_entry(buf, len), 0);
    free(buf);
    CETCD_ASSERT_TRUE(cetcd_cluster_get_peer(g_rpc_cluster, 2) == NULL);
    cetcd_cluster_free(g_rpc_cluster);
    g_rpc_cluster = saved;
}

CETCD_TEST_CASE(apply_leave_joint) {
    uint8_t tag = CETCD_APPLY_LEAVE_JOINT;
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_apply_entry(&tag, 1), 0);
}

CETCD_TEST_CASE(apply_lease_revoke_deletes_keys) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();
    CETCD_ASSERT_NOT_NULL(rpc);
    cetcd_lease_mgr *mgr = cetcd_v3rpc_leases(rpc);
    CETCD_ASSERT_NOT_NULL(mgr);
    cetcd_lease_id id = cetcd_lease_grant(mgr, 60);
    CETCD_ASSERT_TRUE(id > 0);

    uint8_t *buf = NULL;
    size_t len = 0;
    CETCD_ASSERT_EQ_INT(cetcd_apply_encode_put(&buf, &len,
        (const uint8_t *)"lk", 2, (const uint8_t *)"lv", 2, (int64_t)id), 0);
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_apply_entry(buf, len), 0);
    free(buf);

    CETCD_ASSERT_EQ_INT(cetcd_apply_encode_lease_revoke(&buf, &len, (uint64_t)id), 0);
    CETCD_ASSERT_EQ_INT((int)buf[0], CETCD_APPLY_LEASE_REVOKE);
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_apply_entry(buf, len), 0);
    free(buf);

    cetcd_kv kv;
    memset(&kv, 0, sizeof(kv));
    CETCD_ASSERT_TRUE(cetcd_mvcc_get(cetcd_v3rpc_store(rpc), 0,
                                     (const uint8_t *)"lk", 2, &kv) != 0);
    CETCD_ASSERT_TRUE(!cetcd_lease_exists(mgr, id));
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(apply_lease_grant_then_exists) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();
    cetcd_lease_mgr *mgr = cetcd_v3rpc_leases(rpc);
    CETCD_ASSERT_EQ_INT((int)cetcd_lease_next_id(mgr), 1);
    uint8_t *buf = NULL;
    size_t len = 0;
    CETCD_ASSERT_TRUE(cetcd_apply_encode_lease_grant(&buf, &len, 0, 60) != 0);
    uint8_t trunc[] = { CETCD_APPLY_LEASE_GRANT, 0x07 };
    CETCD_ASSERT_TRUE(cetcd_v3rpc_apply_entry(trunc, sizeof(trunc)) != 0);

    CETCD_ASSERT_EQ_INT(cetcd_apply_encode_lease_grant(&buf, &len, 7, 60), 0);
    CETCD_ASSERT_EQ_INT((int)buf[0], CETCD_APPLY_LEASE_GRANT);
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_apply_entry(buf, len), 0);
    CETCD_ASSERT_TRUE(cetcd_lease_exists(mgr, 7));
    CETCD_ASSERT_EQ_INT((int)cetcd_lease_granted_ttl(mgr, 7), 60);
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_apply_entry(buf, len), 0);
    free(buf);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(apply_lease_keepalive_extends) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();
    cetcd_lease_mgr *mgr = cetcd_v3rpc_leases(rpc);
    uint8_t *buf = NULL;
    size_t len = 0;
    CETCD_ASSERT_EQ_INT(cetcd_apply_encode_lease_grant(&buf, &len, 7, 10), 0);
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_apply_entry(buf, len), 0);
    free(buf);
    cetcd_lease_mgr_tick(mgr, 5000);
    CETCD_ASSERT_EQ_INT((int)cetcd_lease_ttl_remaining(mgr, 7), 5);

    CETCD_ASSERT_TRUE(cetcd_apply_encode_lease_keepalive(&buf, &len, 0, 10) != 0);
    uint8_t trunc[] = { CETCD_APPLY_LEASE_KEEPALIVE, 0x07 };
    CETCD_ASSERT_TRUE(cetcd_v3rpc_apply_entry(trunc, sizeof(trunc)) != 0);

    CETCD_ASSERT_EQ_INT(cetcd_apply_encode_lease_keepalive(&buf, &len, 7, 10), 0);
    CETCD_ASSERT_EQ_INT((int)buf[0], CETCD_APPLY_LEASE_KEEPALIVE);
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_apply_entry(buf, len), 0);
    CETCD_ASSERT_EQ_INT((int)cetcd_lease_ttl_remaining(mgr, 7), 10);
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_apply_entry(buf, len), 0);
    free(buf);
    CETCD_ASSERT_EQ_INT(cetcd_apply_encode_lease_keepalive(&buf, &len, 99, 10), 0);
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_apply_entry(buf, len), 0);
    free(buf);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(apply_auth_user_add_then_has_user) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();
    cetcd_auth_store *tmp = cetcd_auth_store_new();
    uint8_t hash[64];
    size_t hlen = 0;
    CETCD_ASSERT_EQ_INT(cetcd_auth_hash_password(tmp, "secret", hash, sizeof(hash), &hlen), 0);
    uint8_t *buf = NULL;
    size_t len = 0;
    CETCD_ASSERT_TRUE(cetcd_apply_encode_auth_user_add(&buf, &len, NULL, 0, hash, hlen) != 0);
    CETCD_ASSERT_EQ_INT(cetcd_apply_encode_auth_user_add(&buf, &len,
        (const uint8_t *)"root", 4, hash, hlen), 0);
    CETCD_ASSERT_EQ_INT((int)buf[0], CETCD_APPLY_AUTH_USER_ADD);
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_apply_entry(buf, len), 0);
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_apply_entry(buf, len), 0);
    free(buf);

    uint8_t auth_buf[32];
    size_t pos = 0;
    auth_buf[pos++] = 0x0a; auth_buf[pos++] = 0x04;
    memcpy(auth_buf + pos, "root", 4); pos += 4;
    auth_buf[pos++] = 0x12; auth_buf[pos++] = 0x06;
    memcpy(auth_buf + pos, "secret", 6); pos += 6;
    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Auth/Authenticate", auth_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    cetcd_rpc_bytes_free(&resp);
    cetcd_auth_store_free(tmp);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(apply_auth_user_delete_then_gone) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();
    uint8_t *buf = NULL;
    size_t len = 0;
    CETCD_ASSERT_TRUE(cetcd_apply_encode_auth_user_delete(&buf, &len, NULL, 0) != 0);
    uint8_t trunc[] = { CETCD_APPLY_AUTH_USER_DELETE };
    CETCD_ASSERT_TRUE(cetcd_v3rpc_apply_entry(trunc, sizeof(trunc)) != 0);

    CETCD_ASSERT_EQ_INT(cetcd_apply_encode_auth_user_delete(&buf, &len,
        (const uint8_t *)"root", 4), 0);
    CETCD_ASSERT_EQ_INT((int)buf[0], CETCD_APPLY_AUTH_USER_DELETE);
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_apply_entry(buf, len), 0);
    free(buf);

    cetcd_auth_store *tmp = cetcd_auth_store_new();
    uint8_t hash[64];
    size_t hlen = 0;
    CETCD_ASSERT_EQ_INT(cetcd_auth_hash_password(tmp, "secret", hash, sizeof(hash), &hlen), 0);
    CETCD_ASSERT_EQ_INT(cetcd_apply_encode_auth_user_add(&buf, &len,
        (const uint8_t *)"root", 4, hash, hlen), 0);
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_apply_entry(buf, len), 0);
    free(buf);
    cetcd_auth_store_free(tmp);

    uint8_t auth_buf[32];
    size_t pos = 0;
    auth_buf[pos++] = 0x0a; auth_buf[pos++] = 0x04;
    memcpy(auth_buf + pos, "root", 4); pos += 4;
    auth_buf[pos++] = 0x12; auth_buf[pos++] = 0x06;
    memcpy(auth_buf + pos, "secret", 6); pos += 6;
    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Auth/Authenticate", auth_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    cetcd_rpc_bytes_free(&resp);

    CETCD_ASSERT_EQ_INT(cetcd_apply_encode_auth_user_delete(&buf, &len,
        (const uint8_t *)"root", 4), 0);
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_apply_entry(buf, len), 0);
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_apply_entry(buf, len), 0);
    free(buf);

    resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.Auth/Authenticate", auth_buf, pos);
    CETCD_ASSERT_TRUE(resp.data == NULL || resp.len == 0);
    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(apply_auth_role_add_then_get) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();
    uint8_t *buf = NULL;
    size_t len = 0;
    CETCD_ASSERT_TRUE(cetcd_apply_encode_auth_role_add(&buf, &len, NULL, 0) != 0);
    uint8_t trunc[] = { CETCD_APPLY_AUTH_ROLE_ADD };
    CETCD_ASSERT_TRUE(cetcd_v3rpc_apply_entry(trunc, sizeof(trunc)) != 0);

    CETCD_ASSERT_EQ_INT(cetcd_apply_encode_auth_role_add(&buf, &len,
        (const uint8_t *)"admin", 5), 0);
    CETCD_ASSERT_EQ_INT((int)buf[0], CETCD_APPLY_AUTH_ROLE_ADD);
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_apply_entry(buf, len), 0);
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_apply_entry(buf, len), 0);
    free(buf);

    uint8_t get_buf[16];
    size_t pos = 0;
    get_buf[pos++] = 0x0a; get_buf[pos++] = 0x05;
    memcpy(get_buf + pos, "admin", 5); pos += 5;
    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Auth/RoleGet", get_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(apply_auth_enabled_requires_root) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();
    uint8_t *buf = NULL;
    size_t len = 0;
    CETCD_ASSERT_TRUE(cetcd_apply_encode_auth_enabled(&buf, &len, 2) != 0);
    uint8_t trunc[] = { CETCD_APPLY_AUTH_ENABLED };
    CETCD_ASSERT_TRUE(cetcd_v3rpc_apply_entry(trunc, sizeof(trunc)) != 0);

    CETCD_ASSERT_EQ_INT(cetcd_apply_encode_auth_enabled(&buf, &len, 1), 0);
    CETCD_ASSERT_EQ_INT((int)buf[0], CETCD_APPLY_AUTH_ENABLED);
    CETCD_ASSERT_TRUE(cetcd_v3rpc_apply_entry(buf, len) != 0);
    free(buf);

    cetcd_auth_store *tmp = cetcd_auth_store_new();
    uint8_t hash[64];
    size_t hlen = 0;
    CETCD_ASSERT_EQ_INT(cetcd_auth_hash_password(tmp, "secret", hash, sizeof(hash), &hlen), 0);
    CETCD_ASSERT_EQ_INT(cetcd_apply_encode_auth_user_add(&buf, &len,
        (const uint8_t *)"root", 4, hash, hlen), 0);
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_apply_entry(buf, len), 0);
    free(buf);
    cetcd_auth_store_free(tmp);

    CETCD_ASSERT_EQ_INT(cetcd_apply_encode_auth_enabled(&buf, &len, 1), 0);
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_apply_entry(buf, len), 0);
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_apply_entry(buf, len), 0);
    free(buf);

    uint8_t put_buf[16];
    size_t p = 0;
    put_buf[p++] = 0x0a; put_buf[p++] = 0x01;
    memcpy(put_buf + p, "k", 1); p += 1;
    put_buf[p++] = 0x12; put_buf[p++] = 0x01;
    memcpy(put_buf + p, "v", 1); p += 1;
    cetcd_rpc_bytes put = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", put_buf, p);
    CETCD_ASSERT_TRUE(put.data == NULL || put.len == 0);
    cetcd_rpc_bytes_free(&put);

    CETCD_ASSERT_EQ_INT(cetcd_apply_encode_auth_enabled(&buf, &len, 0), 0);
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_apply_entry(buf, len), 0);
    free(buf);
    put = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", put_buf, p);
    CETCD_ASSERT_NOT_NULL(put.data);
    cetcd_rpc_bytes_free(&put);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(apply_compact_sets_revision) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();
    uint8_t *buf = NULL;
    size_t len = 0;
    CETCD_ASSERT_EQ_INT(cetcd_apply_encode_put(&buf, &len,
        (const uint8_t *)"ck", 2, (const uint8_t *)"cv", 2, 0), 0);
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_apply_entry(buf, len), 0);
    free(buf);
    CETCD_ASSERT_EQ_INT((int)cetcd_mvcc_revision(cetcd_v3rpc_store(rpc)), 1);

    CETCD_ASSERT_TRUE(cetcd_apply_encode_compact(&buf, &len, 0) != 0);
    uint8_t trunc[] = { CETCD_APPLY_COMPACT };
    CETCD_ASSERT_TRUE(cetcd_v3rpc_apply_entry(trunc, sizeof(trunc)) != 0);

    CETCD_ASSERT_EQ_INT(cetcd_apply_encode_compact(&buf, &len, 1), 0);
    CETCD_ASSERT_EQ_INT((int)buf[0], CETCD_APPLY_COMPACT);
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_apply_entry(buf, len), 0);
    CETCD_ASSERT_EQ_INT((int)cetcd_mvcc_compacted_revision(cetcd_v3rpc_store(rpc)), 1);
    /* Replay after LMDB already compacted this rev is idempotent. */
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_apply_entry(buf, len), 0);
    free(buf);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(quota_blocks_put_not_delete) {
    char tmpl[] = "/tmp/cetcd-quota-XXXXXX";
    char *path = mkdtemp(tmpl);
    CETCD_ASSERT_NOT_NULL(path);

    cetcd_backend_config cfg = {
        .path = path,
        .map_size = 16 * 1024 * 1024,
        .max_dbs = 4
    };
    cetcd_backend *be = cetcd_backend_open(&cfg);
    CETCD_ASSERT_NOT_NULL(be);
    const uint8_t k[] = {'q'};
    const uint8_t v[] = {'1'};
    CETCD_ASSERT_EQ_INT(cetcd_backend_put(be, "kv", k, sizeof(k), v, sizeof(v)), 0);
    CETCD_ASSERT_TRUE(cetcd_backend_size(be) > 0);

    cetcd_v3rpc *rpc = cetcd_v3rpc_new();
    CETCD_ASSERT_NOT_NULL(rpc);
    cetcd_v3rpc_set_auth_backend(rpc, be);
    cetcd_v3rpc_set_quota(1);

    uint8_t *buf = NULL;
    size_t len = 0;
    CETCD_ASSERT_EQ_INT(cetcd_apply_encode_put(&buf, &len,
        (const uint8_t *)"qk", 2, (const uint8_t *)"qv", 2, 0), 0);
    CETCD_ASSERT_TRUE(cetcd_v3rpc_propose_or_apply(buf, len) < 0);
    free(buf);
    CETCD_ASSERT_TRUE(cetcd_v3rpc_alarm_is_active(1));

    uint8_t put_req[16];
    size_t pos = 0;
    put_req[pos++] = 0x0a; put_req[pos++] = 0x02;
    memcpy(put_req + pos, "qk", 2); pos += 2;
    put_req[pos++] = 0x12; put_req[pos++] = 0x02;
    memcpy(put_req + pos, "qv", 2); pos += 2;
    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.KV/Put", put_req, pos);
    CETCD_ASSERT_TRUE(resp.data == NULL || resp.len == 0);
    cetcd_rpc_bytes_free(&resp);

    uint8_t get_alarm[] = {0x08, 0x00};
    resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Maintenance/Alarm", get_alarm, sizeof(get_alarm));
    int found_alarm = 0;
    for (size_t i = 0; resp.data && i < resp.len; i++) {
        if (resp.data[i] == 0x12) { found_alarm = 1; break; }
    }
    CETCD_ASSERT_TRUE(found_alarm);
    cetcd_rpc_bytes_free(&resp);

    CETCD_ASSERT_EQ_INT(cetcd_apply_encode_delete(&buf, &len,
        (const uint8_t *)"qk", 2), 0);
    CETCD_ASSERT_TRUE(cetcd_v3rpc_propose_or_apply(buf, len) >= 0);
    free(buf);

    uint8_t disarm[] = {0x08, 0x02, 0x10, 0x00, 0x18, 0x01};
    resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Maintenance/Alarm", disarm, sizeof(disarm));
    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_set_quota(0);
    cetcd_v3rpc_set_auth_backend(rpc, NULL);
    cetcd_v3rpc_free(rpc);
    cetcd_backend_close(be);
    char db[300];
    snprintf(db, sizeof(db), "%s/data.mdb", path);
    unlink(db);
    snprintf(db, sizeof(db), "%s/lock.mdb", path);
    unlink(db);
    rmdir(path);
}

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(apply_put_delete_roundtrip),
    CETCD_TEST_ENTRY(apply_rejects_truncated),
    CETCD_TEST_ENTRY(apply_delete_range),
    CETCD_TEST_ENTRY(propose_or_apply_local_without_raft),
    CETCD_TEST_ENTRY(apply_batch_two_puts),
    CETCD_TEST_ENTRY(apply_batch_rejects_nesting),
    CETCD_TEST_ENTRY(apply_member_add_remove),
    CETCD_TEST_ENTRY(apply_leave_joint),
    CETCD_TEST_ENTRY(apply_lease_revoke_deletes_keys),
    CETCD_TEST_ENTRY(apply_lease_grant_then_exists),
    CETCD_TEST_ENTRY(apply_lease_keepalive_extends),
    CETCD_TEST_ENTRY(apply_auth_user_add_then_has_user),
    CETCD_TEST_ENTRY(apply_auth_user_delete_then_gone),
    CETCD_TEST_ENTRY(apply_auth_role_add_then_get),
    CETCD_TEST_ENTRY(apply_auth_enabled_requires_root),
    CETCD_TEST_ENTRY(apply_compact_sets_revision),
    CETCD_TEST_ENTRY(quota_blocks_put_not_delete),
CETCD_TEST_LIST_END

CETCD_TEST_MAIN()
