#include "cetcd/v3rpc.h"
#include "cetcd/mvcc.h"
#include "cetcd/lease.h"
#include "cetcd/peer.h"
#include "cetcd_test.h"

#include <string.h>
#include <stdlib.h>

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
CETCD_TEST_LIST_END

CETCD_TEST_MAIN()
