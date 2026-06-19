#include "cetcd/base.h"
#include "cetcd/v3rpc.h"
#include "cetcd/auth.h"
#include "cetcd_test.h"

CETCD_TEST_CASE(v3rpc_create_destroy) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();
    CETCD_ASSERT_NOT_NULL(rpc);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_put_range) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();
    CETCD_ASSERT_NOT_NULL(rpc);

    /* Encode a simple PutRequest: key="hello", value="world", lease=0, prev_kv=false */
    /* Using our custom protobuf-like encoding:
       field 1 (key): bytes, tag=0x0a
       field 2 (value): bytes, tag=0x12
       field 3 (lease): varint, tag=0x18
       field 4 (prev_kv): bool, tag=0x20 */
    uint8_t put_buf[64];
    size_t pos = 0;
    put_buf[pos++] = 0x0a; put_buf[pos++] = 0x05;
    memcpy(put_buf + pos, "hello", 5); pos += 5;
    put_buf[pos++] = 0x12; put_buf[pos++] = 0x05;
    memcpy(put_buf + pos, "world", 5); pos += 5;

    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.KV/Put",
        put_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 0);
    cetcd_rpc_bytes_free(&resp);

    /* Now Range for the key */
    uint8_t range_buf[16];
    pos = 0;
    range_buf[pos++] = 0x0a; /* field 1 = key */
    range_buf[pos++] = 0x05;
    memcpy(range_buf + pos, "hello", 5); pos += 5;

    resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.KV/Range",
        range_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 0);
    cetcd_rpc_bytes_free(&resp);

    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_unknown_path) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();
    uint8_t dummy[] = {0x00};
    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.KV/Unknown",
        dummy, 1);
    CETCD_ASSERT_TRUE(resp.data == NULL || resp.len == 0);
    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_lease_grant_revoke) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* LeaseGrantRequest: field 1 (ttl) = 60, field 2 (id) = 0 (auto) */
    uint8_t grant_buf[8];
    size_t pos = 0;
    grant_buf[pos++] = 0x08; /* field 1 = varint */
    grant_buf[pos++] = 0x3c; /* 60 */
    grant_buf[pos++] = 0x10; /* field 2 = varint */
    grant_buf[pos++] = 0x00; /* id = 0 (auto) */

    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Lease/LeaseGrant",
        grant_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 0);
    cetcd_rpc_bytes_free(&resp);

    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_delete_range) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    uint8_t put_buf[64];
    size_t pos = 0;
    put_buf[pos++] = 0x0a; put_buf[pos++] = 0x03;
    memcpy(put_buf + pos, "key", 3); pos += 3;
    put_buf[pos++] = 0x12; put_buf[pos++] = 0x03;
    memcpy(put_buf + pos, "val", 3); pos += 3;
    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", put_buf, pos);
    cetcd_rpc_bytes_free(&resp);

    uint8_t del_buf[16];
    pos = 0;
    del_buf[pos++] = 0x0a; del_buf[pos++] = 0x03;
    memcpy(del_buf + pos, "key", 3); pos += 3;
    resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/DeleteRange", del_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 0);
    cetcd_rpc_bytes_free(&resp);

    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_auth_enable_disable) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();
    uint8_t dummy[] = {0x00};

    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Auth/AuthEnable", dummy, 1);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 0);
    cetcd_rpc_bytes_free(&resp);

    resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Auth/AuthDisable", dummy, 1);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 0);
    cetcd_rpc_bytes_free(&resp);

    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_auth_user_add_authenticate) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* UserAdd: field 1 (name) = "alice", field 2 (password) = "pass123" */
    uint8_t add_buf[32];
    size_t pos = 0;
    add_buf[pos++] = 0x0a; add_buf[pos++] = 0x05;
    memcpy(add_buf + pos, "alice", 5); pos += 5;
    add_buf[pos++] = 0x12; add_buf[pos++] = 0x07;
    memcpy(add_buf + pos, "pass123", 7); pos += 7;

    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Auth/UserAdd", add_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    cetcd_rpc_bytes_free(&resp);

    /* Authenticate: correct password */
    uint8_t auth_buf[32];
    pos = 0;
    auth_buf[pos++] = 0x0a; auth_buf[pos++] = 0x05;
    memcpy(auth_buf + pos, "alice", 5); pos += 5;
    auth_buf[pos++] = 0x12; auth_buf[pos++] = 0x07;
    memcpy(auth_buf + pos, "pass123", 7); pos += 7;

    resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Auth/Authenticate", auth_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 0);
    cetcd_rpc_bytes_free(&resp);

    /* Authenticate: wrong password */
    uint8_t bad_buf[32];
    pos = 0;
    bad_buf[pos++] = 0x0a; bad_buf[pos++] = 0x05;
    memcpy(bad_buf + pos, "alice", 5); pos += 5;
    bad_buf[pos++] = 0x12; bad_buf[pos++] = 0x03;
    memcpy(bad_buf + pos, "bad", 3); pos += 3;

    resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Auth/Authenticate", bad_buf, pos);
    CETCD_ASSERT_TRUE(resp.data == NULL || resp.len == 0);
    cetcd_rpc_bytes_free(&resp);

    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_lease_revoke) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Grant a lease first */
    uint8_t grant_buf[8];
    size_t pos = 0;
    grant_buf[pos++] = 0x08; /* ttl=60 */
    grant_buf[pos++] = 0x3c;
    grant_buf[pos++] = 0x10; /* id=0 (auto) */
    grant_buf[pos++] = 0x00;
    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Lease/LeaseGrant", grant_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 0);
    cetcd_rpc_bytes_free(&resp);

    /* Revoke lease with ID=1 */
    uint8_t revoke_buf[8];
    pos = 0;
    revoke_buf[pos++] = 0x08; /* field 1 = ID */
    revoke_buf[pos++] = 0x01; /* ID = 1 */
    resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Lease/LeaseRevoke", revoke_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 0);
    cetcd_rpc_bytes_free(&resp);

    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_lease_keep_alive) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Grant a lease first */
    uint8_t grant_buf[8];
    size_t pos = 0;
    grant_buf[pos++] = 0x08; grant_buf[pos++] = 0x3c;
    grant_buf[pos++] = 0x10; grant_buf[pos++] = 0x00;
    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Lease/LeaseGrant", grant_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    cetcd_rpc_bytes_free(&resp);

    /* KeepAlive with ID=1 */
    uint8_t ka_buf[8];
    pos = 0;
    ka_buf[pos++] = 0x08; /* field 1 = ID */
    ka_buf[pos++] = 0x01; /* ID = 1 */
    resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Lease/LeaseKeepAlive", ka_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 0);
    cetcd_rpc_bytes_free(&resp);

    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_lease_time_to_live) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Grant a lease first */
    uint8_t grant_buf[8];
    size_t pos = 0;
    grant_buf[pos++] = 0x08; grant_buf[pos++] = 0x3c;
    grant_buf[pos++] = 0x10; grant_buf[pos++] = 0x00;
    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Lease/LeaseGrant", grant_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    cetcd_rpc_bytes_free(&resp);

    /* TimeToLive with ID=1 */
    uint8_t ttl_buf[8];
    pos = 0;
    ttl_buf[pos++] = 0x08; /* field 1 = ID */
    ttl_buf[pos++] = 0x01; /* ID = 1 */
    resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Lease/LeaseTimeToLive", ttl_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 0);
    cetcd_rpc_bytes_free(&resp);

    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_lease_leases) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    uint8_t dummy[] = {0x00};
    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Lease/LeaseLeases", dummy, 1);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 0);
    cetcd_rpc_bytes_free(&resp);

    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_txn) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Build a TxnRequest with one success op (Put key="txkey" value="txval"):
     *   field 2 (success) = RequestOp (length-delimited), tag = 0x12
     *     RequestOp oneof: field 2 = RequestPut, tag = 0x12
     *       PutRequest: field 1 (key) = "txkey", field 2 (value) = "txval"
     */
    uint8_t put_inner[32];
    size_t ppos = 0;
    put_inner[ppos++] = 0x0a; put_inner[ppos++] = 0x06;
    memcpy(put_inner + ppos, "txkey", 6); ppos += 6;
    put_inner[ppos++] = 0x12; put_inner[ppos++] = 0x06;
    memcpy(put_inner + ppos, "txval", 6); ppos += 6;

    uint8_t op_buf[64];
    size_t opos = 0;
    op_buf[opos++] = 0x12; /* RequestPut tag */
    op_buf[opos++] = (uint8_t)ppos; /* length of PutRequest */
    memcpy(op_buf + opos, put_inner, ppos); opos += ppos;

    uint8_t txn_buf[128];
    size_t tpos = 0;
    txn_buf[tpos++] = 0x12; /* field 2 = success ops */
    txn_buf[tpos++] = (uint8_t)opos; /* length of RequestOp */
    memcpy(txn_buf + tpos, op_buf, opos); tpos += opos;

    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.KV/Txn", txn_buf, tpos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 0);
    cetcd_rpc_bytes_free(&resp);

    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_watch) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Build a WatchCreateRequest:
     *   field 1 (request_union) = WatchCreateRequest, tag = 0x0a
     *     WatchCreateRequest: field 1 (key) = "watchkey"
     */
    uint8_t create_inner[32];
    size_t cpos = 0;
    create_inner[cpos++] = 0x0a; /* field 1 = key */
    create_inner[cpos++] = 0x09;
    memcpy(create_inner + cpos, "watchkey", 9); cpos += 9;

    uint8_t watch_buf[64];
    size_t wpos = 0;
    watch_buf[wpos++] = 0x0a; /* field 1 = create request */
    watch_buf[wpos++] = (uint8_t)cpos;
    memcpy(watch_buf + wpos, create_inner, cpos); wpos += cpos;

    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Watch/Watch", watch_buf, wpos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 0);
    cetcd_rpc_bytes_free(&resp);

    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_maintenance_status) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();
    uint8_t dummy[] = {0x00};
    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Maintenance/Status", dummy, 1);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 0);
    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_maintenance_hash) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();
    uint8_t dummy[] = {0x00};
    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Maintenance/Hash", dummy, 1);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 0);
    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_maintenance_hash_kv) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();
    uint8_t dummy[] = {0x00};
    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Maintenance/HashKV", dummy, 1);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 0);
    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_maintenance_defragment) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();
    uint8_t dummy[] = {0x00};
    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Maintenance/Defragment", dummy, 1);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 0);
    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_maintenance_alarm) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();
    uint8_t dummy[] = {0x00};
    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Maintenance/Alarm", dummy, 1);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 0);
    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_maintenance_move_leader) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();
    uint8_t dummy[] = {0x00};
    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Maintenance/MoveLeader", dummy, 1);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 0);
    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

/* --- New RPC tests --- */

CETCD_TEST_CASE(v3rpc_kv_compact) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Put a key first */
    uint8_t put_buf[32];
    size_t pos = 0;
    put_buf[pos++] = 0x0a; put_buf[pos++] = 0x02;
    memcpy(put_buf + pos, "k1", 2); pos += 2;
    put_buf[pos++] = 0x12; put_buf[pos++] = 0x02;
    memcpy(put_buf + pos, "v1", 2); pos += 2;
    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", put_buf, pos);
    cetcd_rpc_bytes_free(&resp);

    /* CompactRequest: field 1 (revision) = 1 */
    uint8_t compact_buf[8];
    pos = 0;
    compact_buf[pos++] = 0x08; /* field 1 = revision */
    compact_buf[pos++] = 0x01; /* revision = 1 */
    resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Compact", compact_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 0);
    cetcd_rpc_bytes_free(&resp);

    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_cluster_member_list) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();
    uint8_t dummy[] = {0x00};
    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Cluster/MemberList", dummy, 1);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 0);
    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_cluster_member_add) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* MemberAddRequest: field 1 (peerURLs) = "127.0.0.1:2380" */
    uint8_t add_buf[32];
    size_t pos = 0;
    add_buf[pos++] = 0x0a; /* field 1 = peerURLs */
    add_buf[pos++] = 0x0e; /* length */
    memcpy(add_buf + pos, "127.0.0.1:2380", 14); pos += 14;

    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Cluster/MemberAdd", add_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 0);
    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_cluster_member_remove) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* MemberRemoveRequest: field 1 (ID) = 1 */
    uint8_t rm_buf[8];
    size_t pos = 0;
    rm_buf[pos++] = 0x08; /* field 1 = ID */
    rm_buf[pos++] = 0x01; /* ID = 1 */

    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Cluster/MemberRemove", rm_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 0);
    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_cluster_member_update) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* MemberUpdateRequest: field 1 (ID) = 1, field 2 (peerURLs) = "127.0.0.1:2380" */
    uint8_t upd_buf[32];
    size_t pos = 0;
    upd_buf[pos++] = 0x08; /* field 1 = ID */
    upd_buf[pos++] = 0x01;
    upd_buf[pos++] = 0x12; /* field 2 = peerURLs */
    upd_buf[pos++] = 0x0e;
    memcpy(upd_buf + pos, "127.0.0.1:2380", 14); pos += 14;

    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Cluster/MemberUpdate", upd_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 0);
    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_cluster_member_promote) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* MemberPromoteRequest: field 1 (ID) = 1 */
    uint8_t prom_buf[8];
    size_t pos = 0;
    prom_buf[pos++] = 0x08;
    prom_buf[pos++] = 0x01;

    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Cluster/MemberPromote", prom_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 0);
    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_auth_status) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();
    uint8_t dummy[] = {0x00};
    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Auth/AuthStatus", dummy, 1);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 0);
    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_auth_user_list) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Add a user first */
    uint8_t add_buf[32];
    size_t pos = 0;
    add_buf[pos++] = 0x0a; add_buf[pos++] = 0x03;
    memcpy(add_buf + pos, "bob", 3); pos += 3;
    add_buf[pos++] = 0x12; add_buf[pos++] = 0x04;
    memcpy(add_buf + pos, "pass", 4); pos += 4;
    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Auth/UserAdd", add_buf, pos);
    cetcd_rpc_bytes_free(&resp);

    /* List users */
    uint8_t dummy[] = {0x00};
    resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Auth/UserList", dummy, 1);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 0);
    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_auth_user_change_password) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Add a user first */
    uint8_t add_buf[32];
    size_t pos = 0;
    add_buf[pos++] = 0x0a; add_buf[pos++] = 0x05;
    memcpy(add_buf + pos, "alice", 5); pos += 5;
    add_buf[pos++] = 0x12; add_buf[pos++] = 0x07;
    memcpy(add_buf + pos, "pass123", 7); pos += 7;
    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Auth/UserAdd", add_buf, pos);
    cetcd_rpc_bytes_free(&resp);

    /* Change password */
    uint8_t chg_buf[32];
    pos = 0;
    chg_buf[pos++] = 0x0a; chg_buf[pos++] = 0x05;
    memcpy(chg_buf + pos, "alice", 5); pos += 5;
    chg_buf[pos++] = 0x12; chg_buf[pos++] = 0x06;
    memcpy(chg_buf + pos, "newpw1", 6); pos += 6;
    resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Auth/UserChangePassword", chg_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 0);
    cetcd_rpc_bytes_free(&resp);

    /* Verify new password works */
    uint8_t auth_buf[32];
    pos = 0;
    auth_buf[pos++] = 0x0a; auth_buf[pos++] = 0x05;
    memcpy(auth_buf + pos, "alice", 5); pos += 5;
    auth_buf[pos++] = 0x12; auth_buf[pos++] = 0x06;
    memcpy(auth_buf + pos, "newpw1", 6); pos += 6;
    resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Auth/Authenticate", auth_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 0);
    cetcd_rpc_bytes_free(&resp);

    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_auth_role_list) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Add a role first */
    uint8_t role_buf[16];
    size_t pos = 0;
    role_buf[pos++] = 0x0a; role_buf[pos++] = 0x05;
    memcpy(role_buf + pos, "admin", 5); pos += 5;
    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Auth/RoleAdd", role_buf, pos);
    cetcd_rpc_bytes_free(&resp);

    /* List roles */
    uint8_t dummy[] = {0x00};
    resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Auth/RoleList", dummy, 1);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 0);
    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_auth_role_delete) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Add a role first */
    uint8_t role_buf[16];
    size_t pos = 0;
    role_buf[pos++] = 0x0a; role_buf[pos++] = 0x04;
    memcpy(role_buf + pos, "root", 4); pos += 4;
    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Auth/RoleAdd", role_buf, pos);
    cetcd_rpc_bytes_free(&resp);

    /* Delete role */
    uint8_t del_buf[16];
    pos = 0;
    del_buf[pos++] = 0x0a; del_buf[pos++] = 0x04;
    memcpy(del_buf + pos, "root", 4); pos += 4;
    resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Auth/RoleDelete", del_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 0);
    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_auth_user_revoke_role) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Add user */
    uint8_t user_buf[32];
    size_t pos = 0;
    user_buf[pos++] = 0x0a; pos++; /* name */
    user_buf[1] = 5; memcpy(user_buf + pos, "carol", 5); pos += 5;
    user_buf[pos++] = 0x12; pos++; /* password */
    user_buf[8] = 4; memcpy(user_buf + pos, "pw56", 4); pos += 4;
    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Auth/UserAdd", user_buf, pos);
    cetcd_rpc_bytes_free(&resp);

    /* Add role */
    uint8_t role_buf[16];
    pos = 0;
    role_buf[pos++] = 0x0a; role_buf[pos++] = 0x05;
    memcpy(role_buf + pos, "guest", 5); pos += 5;
    resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Auth/RoleAdd", role_buf, pos);
    cetcd_rpc_bytes_free(&resp);

    /* Grant role */
    uint8_t grant_buf[32];
    pos = 0;
    grant_buf[pos++] = 0x0a; grant_buf[pos++] = 0x05;
    memcpy(grant_buf + pos, "carol", 5); pos += 5;
    grant_buf[pos++] = 0x12; grant_buf[pos++] = 0x05;
    memcpy(grant_buf + pos, "guest", 5); pos += 5;
    resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Auth/UserGrantRole", grant_buf, pos);
    cetcd_rpc_bytes_free(&resp);

    /* Revoke role */
    resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Auth/UserRevokeRole", grant_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 0);
    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(v3rpc_create_destroy),
    CETCD_TEST_ENTRY(v3rpc_put_range),
    CETCD_TEST_ENTRY(v3rpc_unknown_path),
    CETCD_TEST_ENTRY(v3rpc_lease_grant_revoke),
    CETCD_TEST_ENTRY(v3rpc_delete_range),
    CETCD_TEST_ENTRY(v3rpc_auth_enable_disable),
    CETCD_TEST_ENTRY(v3rpc_auth_user_add_authenticate),
    CETCD_TEST_ENTRY(v3rpc_lease_revoke),
    CETCD_TEST_ENTRY(v3rpc_lease_keep_alive),
    CETCD_TEST_ENTRY(v3rpc_lease_time_to_live),
    CETCD_TEST_ENTRY(v3rpc_lease_leases),
    CETCD_TEST_ENTRY(v3rpc_txn),
    CETCD_TEST_ENTRY(v3rpc_watch),
    CETCD_TEST_ENTRY(v3rpc_maintenance_status),
    CETCD_TEST_ENTRY(v3rpc_maintenance_hash),
    CETCD_TEST_ENTRY(v3rpc_maintenance_hash_kv),
    CETCD_TEST_ENTRY(v3rpc_maintenance_defragment),
    CETCD_TEST_ENTRY(v3rpc_maintenance_alarm),
    CETCD_TEST_ENTRY(v3rpc_maintenance_move_leader),
    CETCD_TEST_ENTRY(v3rpc_kv_compact),
    CETCD_TEST_ENTRY(v3rpc_cluster_member_list),
    CETCD_TEST_ENTRY(v3rpc_cluster_member_add),
    CETCD_TEST_ENTRY(v3rpc_cluster_member_remove),
    CETCD_TEST_ENTRY(v3rpc_cluster_member_update),
    CETCD_TEST_ENTRY(v3rpc_cluster_member_promote),
    CETCD_TEST_ENTRY(v3rpc_auth_status),
    CETCD_TEST_ENTRY(v3rpc_auth_user_list),
    CETCD_TEST_ENTRY(v3rpc_auth_user_change_password),
    CETCD_TEST_ENTRY(v3rpc_auth_role_list),
    CETCD_TEST_ENTRY(v3rpc_auth_role_delete),
    CETCD_TEST_ENTRY(v3rpc_auth_user_revoke_role),
CETCD_TEST_LIST_END

CETCD_TEST_MAIN()
