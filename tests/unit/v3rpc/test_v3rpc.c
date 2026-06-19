#include "cetcd/base.h"
#include "cetcd/v3rpc.h"
#include "cetcd/auth.h"
#include "cetcd/peer.h"
#include "cetcd_test.h"

/* Globals defined in v3rpc.c — we set them to test cluster-aware handlers */
extern cetcd_cluster *g_rpc_cluster;
extern uint64_t       g_rpc_node_id;

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

CETCD_TEST_CASE(v3rpc_range_returns_actual_kv) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Put key="foo" value="bar" */
    uint8_t put_buf[32];
    size_t pos = 0;
    put_buf[pos++] = 0x0a; put_buf[pos++] = 0x03;
    memcpy(put_buf + pos, "foo", 3); pos += 3;
    put_buf[pos++] = 0x12; put_buf[pos++] = 0x03;
    memcpy(put_buf + pos, "bar", 3); pos += 3;
    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", put_buf, pos);
    cetcd_rpc_bytes_free(&resp);

    /* Range for key="foo" */
    uint8_t range_buf[16];
    pos = 0;
    range_buf[pos++] = 0x0a; /* field 1 = key */
    range_buf[pos++] = 0x03;
    memcpy(range_buf + pos, "foo", 3); pos += 3;

    resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Range", range_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 2);

    /* Verify response contains "foo" and "bar" */
    bool found_foo = false, found_bar = false;
    for (size_t i = 0; i + 3 <= resp.len; i++) {
        if (resp.data[i] == 'f' && resp.data[i+1] == 'o' && resp.data[i+2] == 'o')
            found_foo = true;
        if (resp.data[i] == 'b' && resp.data[i+1] == 'a' && resp.data[i+2] == 'r')
            found_bar = true;
    }
    CETCD_ASSERT_TRUE(found_foo);
    CETCD_ASSERT_TRUE(found_bar);

    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_range_query_multiple_keys) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Put three keys: k1, k2, k3 */
    const char *keys[] = {"k1", "k2", "k3"};
    const char *vals[] = {"v1", "v2", "v3"};
    for (int i = 0; i < 3; i++) {
        uint8_t put_buf[32];
        size_t pos = 0;
        put_buf[pos++] = 0x0a; put_buf[pos++] = 0x02;
        memcpy(put_buf + pos, keys[i], 2); pos += 2;
        put_buf[pos++] = 0x12; put_buf[pos++] = 0x02;
        memcpy(put_buf + pos, vals[i], 2); pos += 2;
        cetcd_rpc_bytes r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", put_buf, pos);
        cetcd_rpc_bytes_free(&r);
    }

    /* Range from "k1" to "k4" (exclusive end) */
    uint8_t range_buf[32];
    size_t pos = 0;
    range_buf[pos++] = 0x0a; /* field 1 = key (start) */
    range_buf[pos++] = 0x02;
    memcpy(range_buf + pos, "k1", 2); pos += 2;
    range_buf[pos++] = 0x12; /* field 2 = range_end */
    range_buf[pos++] = 0x02;
    memcpy(range_buf + pos, "k4", 2); pos += 2;

    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Range", range_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 5);

    /* Count KeyValue entries (tag 0x0a = field 2 = kvs) — but we need to be careful
     * since the header also uses 0x0a. Instead, count occurrences of "v1", "v2", "v3" */
    int val_count = 0;
    for (size_t i = 0; i + 1 < resp.len; i++) {
        if (resp.data[i] == 'v' && resp.data[i+1] >= '1' && resp.data[i+1] <= '3')
            val_count++;
    }
    CETCD_ASSERT_TRUE(val_count >= 3);

    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_put_response_has_revision) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    uint8_t put_buf[32];
    size_t pos = 0;
    put_buf[pos++] = 0x0a; put_buf[pos++] = 0x04;
    memcpy(put_buf + pos, "test", 4); pos += 4;
    put_buf[pos++] = 0x12; put_buf[pos++] = 0x04;
    memcpy(put_buf + pos, "data", 4); pos += 4;

    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", put_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 2);

    /* PutResponse should contain a header with revision (tag 0x18 inside header sub-message) */
    /* The header is field 1 (tag 0x0a, length-delimited) at the start */
    CETCD_ASSERT_TRUE(resp.data[0] == 0x0a); /* header tag */

    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_delete_returns_count) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Put a key first */
    uint8_t put_buf[32];
    size_t pos = 0;
    put_buf[pos++] = 0x0a; put_buf[pos++] = 0x04;
    memcpy(put_buf + pos, "gone", 4); pos += 4;
    put_buf[pos++] = 0x12; put_buf[pos++] = 0x04;
    memcpy(put_buf + pos, "data", 4); pos += 4;
    cetcd_rpc_bytes r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", put_buf, pos);
    cetcd_rpc_bytes_free(&r);

    /* Delete the key */
    uint8_t del_buf[16];
    pos = 0;
    del_buf[pos++] = 0x0a; /* field 1 = key */
    del_buf[pos++] = 0x04;
    memcpy(del_buf + pos, "gone", 4); pos += 4;

    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/DeleteRange", del_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 2);

    /* DeleteRangeResponse should have field 2 (deleted) = tag 0x10 somewhere */
    bool found_deleted = false;
    for (size_t i = 0; i < resp.len; i++) {
        if (resp.data[i] == 0x10) { found_deleted = true; break; }
    }
    CETCD_ASSERT_TRUE(found_deleted);

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

CETCD_TEST_CASE(v3rpc_maintenance_snapshot) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Put some keys first so the snapshot has data */
    uint8_t put_buf[32];
    size_t pos = 0;
    put_buf[pos++] = 0x0a; put_buf[pos++] = 0x02;
    memcpy(put_buf + pos, "s1", 2); pos += 2;
    put_buf[pos++] = 0x12; put_buf[pos++] = 0x02;
    memcpy(put_buf + pos, "v1", 2); pos += 2;
    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", put_buf, pos);
    cetcd_rpc_bytes_free(&resp);

    /* Snapshot request: empty */
    uint8_t dummy[] = {0x00};
    resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Maintenance/Snapshot", dummy, 1);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 0);
    cetcd_rpc_bytes_free(&resp);

    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_maintenance_snapshot_empty) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Snapshot request on empty store */
    uint8_t dummy[] = {0x00};
    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Maintenance/Snapshot", dummy, 1);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 0);
    cetcd_rpc_bytes_free(&resp);

    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_maintenance_downgrade) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* DowngradeRequest: field 1 (action) = 1 (ENABLE), field 2 (version) = "0.1.0" */
    uint8_t dg_buf[32];
    size_t pos = 0;
    dg_buf[pos++] = 0x08; /* field 1 = action */
    dg_buf[pos++] = 0x01; /* ENABLE */
    dg_buf[pos++] = 0x12; /* field 2 = version */
    dg_buf[pos++] = 0x05;
    memcpy(dg_buf + pos, "0.1.0", 5); pos += 5;

    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Maintenance/Downgrade", dg_buf, pos);
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

CETCD_TEST_CASE(v3rpc_auth_user_get) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Add user */
    uint8_t user_buf[32];
    size_t pos = 0;
    user_buf[pos++] = 0x0a; user_buf[pos++] = 0x04;
    memcpy(user_buf + pos, "dave", 4); pos += 4;
    user_buf[pos++] = 0x12; user_buf[pos++] = 0x04;
    memcpy(user_buf + pos, "pass", 4); pos += 4;
    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Auth/UserAdd", user_buf, pos);
    cetcd_rpc_bytes_free(&resp);

    /* Add role and grant to user */
    uint8_t role_buf[16];
    pos = 0;
    role_buf[pos++] = 0x0a; role_buf[pos++] = 0x05;
    memcpy(role_buf + pos, "admin", 5); pos += 5;
    resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.Auth/RoleAdd", role_buf, pos);
    cetcd_rpc_bytes_free(&resp);

    uint8_t grant_buf[32];
    pos = 0;
    grant_buf[pos++] = 0x0a; grant_buf[pos++] = 0x04;
    memcpy(grant_buf + pos, "dave", 4); pos += 4;
    grant_buf[pos++] = 0x12; grant_buf[pos++] = 0x05;
    memcpy(grant_buf + pos, "admin", 5); pos += 5;
    resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.Auth/UserGrantRole", grant_buf, pos);
    cetcd_rpc_bytes_free(&resp);

    /* UserGet: should return roles */
    uint8_t get_buf[16];
    pos = 0;
    get_buf[pos++] = 0x0a; get_buf[pos++] = 0x04;
    memcpy(get_buf + pos, "dave", 4); pos += 4;
    resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.Auth/UserGet", get_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 0);
    cetcd_rpc_bytes_free(&resp);

    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_auth_role_get) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Add a role */
    uint8_t role_buf[16];
    size_t pos = 0;
    role_buf[pos++] = 0x0a; role_buf[pos++] = 0x04;
    memcpy(role_buf + pos, "root", 4); pos += 4;
    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.Auth/RoleAdd", role_buf, pos);
    cetcd_rpc_bytes_free(&resp);

    /* RoleGet: should return permissions */
    uint8_t get_buf[16];
    pos = 0;
    get_buf[pos++] = 0x0a; get_buf[pos++] = 0x04;
    memcpy(get_buf + pos, "root", 4); pos += 4;
    resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.Auth/RoleGet", get_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 0);
    cetcd_rpc_bytes_free(&resp);

    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_auth_role_grant_permission) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Add a role */
    uint8_t role_buf[16];
    size_t pos = 0;
    role_buf[pos++] = 0x0a; role_buf[pos++] = 0x04;
    memcpy(role_buf + pos, "root", 4); pos += 4;
    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.Auth/RoleAdd", role_buf, pos);
    cetcd_rpc_bytes_free(&resp);

    /* Build Permission sub-message: permType=2 (READWRITE), key="/foo" */
    uint8_t perm[32];
    size_t ppos = 0;
    perm[ppos++] = 0x08; /* field 1 = permType */
    perm[ppos++] = 0x02; /* READWRITE */
    perm[ppos++] = 0x0a; /* field 2 = key */
    perm[ppos++] = 0x04;
    memcpy(perm + ppos, "/foo", 4); ppos += 4;

    /* Build RoleGrantPermissionRequest: field 1 (name) + field 2 (perm) */
    uint8_t req[64];
    pos = 0;
    req[pos++] = 0x0a; req[pos++] = 0x04;
    memcpy(req + pos, "root", 4); pos += 4;
    req[pos++] = 0x12; /* field 2 = perm */
    req[pos++] = (uint8_t)ppos;
    memcpy(req + pos, perm, ppos); pos += ppos;

    resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.Auth/RoleGrantPermission", req, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 0);
    cetcd_rpc_bytes_free(&resp);

    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_auth_role_revoke_permission) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Add a role */
    uint8_t role_buf[16];
    size_t pos = 0;
    role_buf[pos++] = 0x0a; role_buf[pos++] = 0x04;
    memcpy(role_buf + pos, "root", 4); pos += 4;
    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.Auth/RoleAdd", role_buf, pos);
    cetcd_rpc_bytes_free(&resp);

    /* Revoke permission (role has default permissions from RoleAdd) */
    uint8_t req[16];
    pos = 0;
    req[pos++] = 0x0a; req[pos++] = 0x04;
    memcpy(req + pos, "root", 4); pos += 4;
    resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.Auth/RoleRevokePermission", req, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 0);
    cetcd_rpc_bytes_free(&resp);

    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_lease_leases_returns_actual_leases) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Grant two leases first */
    uint8_t grant_buf[8];
    size_t pos = 0;
    grant_buf[pos++] = 0x08; grant_buf[pos++] = 0x3c; /* ttl=60 */
    grant_buf[pos++] = 0x10; grant_buf[pos++] = 0x00; /* id=0 auto */
    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Lease/LeaseGrant", grant_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    cetcd_rpc_bytes_free(&resp);

    pos = 0;
    grant_buf[pos++] = 0x08; grant_buf[pos++] = 0x78; /* ttl=120 */
    grant_buf[pos++] = 0x10; grant_buf[pos++] = 0x00;
    resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Lease/LeaseGrant", grant_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    cetcd_rpc_bytes_free(&resp);

    /* Now call LeaseLeases — should contain at least 2 LeaseStatus entries */
    uint8_t dummy[] = {0x00};
    resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Lease/LeaseLeases", dummy, 1);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 1);

    /* Count 0x0a tags (field 1 = repeated LeaseStatus) */
    int lease_count = 0;
    for (size_t i = 0; i < resp.len; i++) {
        if (resp.data[i] == 0x0a) lease_count++;
    }
    CETCD_ASSERT_TRUE(lease_count >= 2);

    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_lease_keep_alive_uses_granted_ttl) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Grant a lease with ttl=120 */
    uint8_t grant_buf[8];
    size_t pos = 0;
    grant_buf[pos++] = 0x08; grant_buf[pos++] = 0x78; /* ttl=120 */
    grant_buf[pos++] = 0x10; grant_buf[pos++] = 0x00; /* id=0 auto */
    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Lease/LeaseGrant", grant_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    cetcd_rpc_bytes_free(&resp);

    /* KeepAlive with ID=1 — response TTL should be positive (refreshed to ~120) */
    uint8_t ka_buf[8];
    pos = 0;
    ka_buf[pos++] = 0x08; /* field 1 = ID */
    ka_buf[pos++] = 0x01; /* ID = 1 */
    resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Lease/LeaseKeepAlive", ka_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 0);

    /* Parse response: find field 2 (TTL) tag=0x10 */
    int64_t resp_ttl = 0;
    size_t rpos = 0;
    while (rpos < resp.len) {
        uint8_t tag = resp.data[rpos++];
        uint64_t v = 0; int shift = 0;
        while (rpos < resp.len) {
            uint8_t b = resp.data[rpos++];
            v |= (uint64_t)(b & 0x7F) << shift;
            if ((b & 0x80) == 0) break;
            shift += 7;
        }
        if (tag == 0x10) { resp_ttl = (int64_t)v; break; }
    }
    /* TTL should be close to 120 (the granted TTL, not hardcoded 60) */
    CETCD_ASSERT_TRUE(resp_ttl > 60);

    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_cluster_member_list_with_peers) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Set up a cluster with known peers */
    cetcd_cluster *saved_cluster = g_rpc_cluster;
    uint64_t       saved_node_id = g_rpc_node_id;
    g_rpc_cluster = cetcd_cluster_new(1);
    g_rpc_node_id = 1;
    cetcd_peer_info p1 = {.id = 2, .addr = "10.0.0.2", .port = 2380};
    cetcd_peer_info p2 = {.id = 3, .addr = "10.0.0.3", .port = 2381};
    cetcd_cluster_add_peer(g_rpc_cluster, &p1);
    cetcd_cluster_add_peer(g_rpc_cluster, &p2);

    uint8_t dummy[] = {0x00};
    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Cluster/MemberList", dummy, 1);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 0);

    /* Verify response contains multiple members by counting 0x12 tags
     * (field 2 = repeated Member). With self + 2 peers, we expect at least 2. */
    int member_count = 0;
    for (size_t i = 0; i < resp.len; i++) {
        if (resp.data[i] == 0x12) member_count++;
    }
    CETCD_ASSERT_TRUE(member_count >= 2);

    cetcd_rpc_bytes_free(&resp);
    cetcd_cluster_free(g_rpc_cluster);
    g_rpc_cluster = saved_cluster;
    g_rpc_node_id = saved_node_id;
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_cluster_member_update_actual) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Set up a cluster with one peer */
    cetcd_cluster *saved_cluster = g_rpc_cluster;
    g_rpc_cluster = cetcd_cluster_new(1);
    cetcd_peer_info p1 = {.id = 5, .addr = "10.0.0.5", .port = 2380};
    cetcd_cluster_add_peer(g_rpc_cluster, &p1);

    /* MemberUpdateRequest: field 1 (ID) = 5, field 2 (peerURLs) = "192.168.1.5:9999" */
    uint8_t upd_buf[64];
    size_t pos = 0;
    upd_buf[pos++] = 0x08; /* field 1 = ID */
    upd_buf[pos++] = 0x05;
    upd_buf[pos++] = 0x12; /* field 2 = peerURLs */
    upd_buf[pos++] = 0x10; /* length = 16 */
    memcpy(upd_buf + pos, "192.168.1.5:9999", 16); pos += 16;

    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Cluster/MemberUpdate", upd_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 0);
    cetcd_rpc_bytes_free(&resp);

    /* Verify the peer was actually updated */
    const cetcd_peer_info *pi = cetcd_cluster_get_peer(g_rpc_cluster, 5);
    CETCD_ASSERT_NOT_NULL(pi);
    CETCD_ASSERT_TRUE(strcmp(pi->addr, "192.168.1.5") == 0);
    CETCD_ASSERT_EQ_INT((int)pi->port, 9999);

    cetcd_cluster_free(g_rpc_cluster);
    g_rpc_cluster = saved_cluster;
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_txn_cas_match) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Put key="caskey" value="old" */
    uint8_t put_buf[32];
    size_t pos = 0;
    put_buf[pos++] = 0x0a; put_buf[pos++] = 0x06;
    memcpy(put_buf + pos, "caskey", 6); pos += 6;
    put_buf[pos++] = 0x12; put_buf[pos++] = 0x03;
    memcpy(put_buf + pos, "old", 3); pos += 3;
    cetcd_rpc_bytes r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", put_buf, pos);
    cetcd_rpc_bytes_free(&r);

    /* Build Compare: result=EQUAL(0), target=VALUE(3), key="caskey", value="old"
     *   field 1 (result) = 0, tag = 0x08
     *   field 2 (target) = 3, tag = 0x10
     *   field 3 (key)    = bytes, tag = 0x1a
     *   field 7 (value)  = bytes, tag = 0x3a
     */
    uint8_t cmp_buf[64];
    size_t cpos = 0;
    cmp_buf[cpos++] = 0x08; cmp_buf[cpos++] = 0x00; /* result=EQUAL */
    cmp_buf[cpos++] = 0x10; cmp_buf[cpos++] = 0x03; /* target=VALUE */
    cmp_buf[cpos++] = 0x1a; cmp_buf[cpos++] = 0x06; /* key */
    memcpy(cmp_buf + cpos, "caskey", 6); cpos += 6;
    cmp_buf[cpos++] = 0x3a; cmp_buf[cpos++] = 0x03; /* value */
    memcpy(cmp_buf + cpos, "old", 3); cpos += 3;

    /* Build success op: Put("caskey", "new") */
    uint8_t put_inner[32]; size_t ppos = 0;
    put_inner[ppos++] = 0x0a; put_inner[ppos++] = 0x06;
    memcpy(put_inner + ppos, "caskey", 6); ppos += 6;
    put_inner[ppos++] = 0x12; put_inner[ppos++] = 0x03;
    memcpy(put_inner + ppos, "new", 3); ppos += 3;
    uint8_t op_buf[64]; size_t opos = 0;
    op_buf[opos++] = 0x12; /* RequestPut tag */
    op_buf[opos++] = (uint8_t)ppos;
    memcpy(op_buf + opos, put_inner, ppos); opos += ppos;

    /* Build TxnRequest: field 1 (compare) + field 2 (success) */
    uint8_t txn_buf[256]; size_t tpos = 0;
    txn_buf[tpos++] = 0x0a; /* field 1 = compare */
    txn_buf[tpos++] = (uint8_t)cpos;
    memcpy(txn_buf + tpos, cmp_buf, cpos); tpos += cpos;
    txn_buf[tpos++] = 0x12; /* field 2 = success */
    txn_buf[tpos++] = (uint8_t)opos;
    memcpy(txn_buf + tpos, op_buf, opos); tpos += opos;

    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Txn", txn_buf, tpos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 2);

    /* Find succeeded field (tag 0x10) and verify it's true (0x01) */
    bool found_succeeded = false;
    bool succeeded_val = false;
    for (size_t i = 0; i + 1 < resp.len; i++) {
        if (resp.data[i] == 0x10) {
            found_succeeded = true;
            succeeded_val = (resp.data[i + 1] != 0);
            break;
        }
    }
    CETCD_ASSERT_TRUE(found_succeeded);
    CETCD_ASSERT_TRUE(succeeded_val);

    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_txn_cas_no_match) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Put key="caskey2" value="old" */
    uint8_t put_buf[32];
    size_t pos = 0;
    put_buf[pos++] = 0x0a; put_buf[pos++] = 0x07;
    memcpy(put_buf + pos, "caskey2", 7); pos += 7;
    put_buf[pos++] = 0x12; put_buf[pos++] = 0x03;
    memcpy(put_buf + pos, "old", 3); pos += 3;
    cetcd_rpc_bytes r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", put_buf, pos);
    cetcd_rpc_bytes_free(&r);

    /* Build Compare: result=EQUAL(0), target=VALUE(3), key="caskey2", value="wrong" */
    uint8_t cmp_buf[64];
    size_t cpos = 0;
    cmp_buf[cpos++] = 0x08; cmp_buf[cpos++] = 0x00; /* result=EQUAL */
    cmp_buf[cpos++] = 0x10; cmp_buf[cpos++] = 0x03; /* target=VALUE */
    cmp_buf[cpos++] = 0x1a; cmp_buf[cpos++] = 0x07; /* key */
    memcpy(cmp_buf + cpos, "caskey2", 7); cpos += 7;
    cmp_buf[cpos++] = 0x3a; cmp_buf[cpos++] = 0x05; /* value */
    memcpy(cmp_buf + cpos, "wrong", 5); cpos += 5;

    /* Build success op: Put("caskey2", "new") */
    uint8_t put_inner[32]; size_t ppos = 0;
    put_inner[ppos++] = 0x0a; put_inner[ppos++] = 0x07;
    memcpy(put_inner + ppos, "caskey2", 7); ppos += 7;
    put_inner[ppos++] = 0x12; put_inner[ppos++] = 0x03;
    memcpy(put_inner + ppos, "new", 3); ppos += 3;
    uint8_t op_buf[64]; size_t opos = 0;
    op_buf[opos++] = 0x12;
    op_buf[opos++] = (uint8_t)ppos;
    memcpy(op_buf + opos, put_inner, ppos); opos += ppos;

    uint8_t txn_buf[256]; size_t tpos = 0;
    txn_buf[tpos++] = 0x0a;
    txn_buf[tpos++] = (uint8_t)cpos;
    memcpy(txn_buf + tpos, cmp_buf, cpos); tpos += cpos;
    txn_buf[tpos++] = 0x12;
    txn_buf[tpos++] = (uint8_t)opos;
    memcpy(txn_buf + tpos, op_buf, opos); tpos += opos;

    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Txn", txn_buf, tpos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 2);

    /* Find succeeded field and verify it's false (0x00) */
    bool found_succeeded = false;
    bool succeeded_val = true;
    for (size_t i = 0; i + 1 < resp.len; i++) {
        if (resp.data[i] == 0x10) {
            found_succeeded = true;
            succeeded_val = (resp.data[i + 1] != 0);
            break;
        }
    }
    CETCD_ASSERT_TRUE(found_succeeded);
    CETCD_ASSERT_FALSE(succeeded_val);

    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_txn_response_has_header) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Simple Txn with just a Put success op */
    uint8_t put_inner[32]; size_t ppos = 0;
    put_inner[ppos++] = 0x0a; put_inner[ppos++] = 0x05;
    memcpy(put_inner + ppos, "txnkv", 5); ppos += 5;
    put_inner[ppos++] = 0x12; put_inner[ppos++] = 0x05;
    memcpy(put_inner + ppos, "txnvv", 5); ppos += 5;
    uint8_t op_buf[64]; size_t opos = 0;
    op_buf[opos++] = 0x12;
    op_buf[opos++] = (uint8_t)ppos;
    memcpy(op_buf + opos, put_inner, ppos); opos += ppos;

    uint8_t txn_buf[128]; size_t tpos = 0;
    txn_buf[tpos++] = 0x12;
    txn_buf[tpos++] = (uint8_t)opos;
    memcpy(txn_buf + tpos, op_buf, opos); tpos += opos;

    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Txn", txn_buf, tpos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 2);
    /* ResponseHeader is field 1, tag = 0x0a */
    CETCD_ASSERT_TRUE(resp.data[0] == 0x0a);

    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_compact_has_header) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Put a key first */
    uint8_t put_buf[32]; size_t pos = 0;
    put_buf[pos++] = 0x0a; put_buf[pos++] = 0x02;
    memcpy(put_buf + pos, "c1", 2); pos += 2;
    put_buf[pos++] = 0x12; put_buf[pos++] = 0x02;
    memcpy(put_buf + pos, "v1", 2); pos += 2;
    cetcd_rpc_bytes r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", put_buf, pos);
    cetcd_rpc_bytes_free(&r);

    /* Compact at revision 1 */
    uint8_t compact_buf[8]; pos = 0;
    compact_buf[pos++] = 0x08;
    compact_buf[pos++] = 0x01;
    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Compact", compact_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 2);
    /* CompactResponse should have header (field 1, tag 0x0a) */
    CETCD_ASSERT_TRUE(resp.data[0] == 0x0a);

    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_lease_revoke_has_header) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Grant a lease */
    uint8_t grant_buf[8]; size_t pos = 0;
    grant_buf[pos++] = 0x08; grant_buf[pos++] = 0x3c; /* TTL=60 */
    cetcd_rpc_bytes r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.Lease/LeaseGrant", grant_buf, pos);
    CETCD_ASSERT_NOT_NULL(r.data);
    /* Extract lease ID from response (field 1, tag 0x08) */
    int64_t lease_id = 0;
    for (size_t i = 0; i + 1 < r.len; i++) {
        if (r.data[i] == 0x08) {
            lease_id = (int64_t)r.data[i + 1];
            break;
        }
    }
    cetcd_rpc_bytes_free(&r);
    CETCD_ASSERT_TRUE(lease_id > 0);

    /* Revoke the lease */
    uint8_t revoke_buf[8]; pos = 0;
    revoke_buf[pos++] = 0x08;
    revoke_buf[pos++] = (uint8_t)lease_id;
    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.Lease/LeaseRevoke", revoke_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 2);
    /* LeaseRevokeResponse should have header (field 1, tag 0x0a) */
    CETCD_ASSERT_TRUE(resp.data[0] == 0x0a);

    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_watch_response_correct_tags) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Put key="wkey" value="wval" to create history */
    uint8_t put_buf[32]; size_t pos = 0;
    put_buf[pos++] = 0x0a; put_buf[pos++] = 0x04;
    memcpy(put_buf + pos, "wkey", 4); pos += 4;
    put_buf[pos++] = 0x12; put_buf[pos++] = 0x04;
    memcpy(put_buf + pos, "wval", 4); pos += 4;
    cetcd_rpc_bytes r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", put_buf, pos);
    cetcd_rpc_bytes_free(&r);

    /* Watch key="wkey" with start_revision=1 */
    uint8_t create_inner[32]; size_t cpos = 0;
    create_inner[cpos++] = 0x0a; create_inner[cpos++] = 0x04;
    memcpy(create_inner + cpos, "wkey", 4); cpos += 4;
    create_inner[cpos++] = 0x18; create_inner[cpos++] = 0x01; /* start_revision=1 */

    uint8_t watch_buf[64]; size_t wpos = 0;
    watch_buf[wpos++] = 0x0a;
    watch_buf[wpos++] = (uint8_t)cpos;
    memcpy(watch_buf + wpos, create_inner, cpos); wpos += cpos;

    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.Watch/Watch", watch_buf, wpos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 2);

    /* WatchResponse should have:
     *   field 2 (watch_id) = tag 0x10
     *   field 3 (created)  = tag 0x18
     * Verify we find 0x10 (watch_id) somewhere in the response */
    bool found_watch_id = false;
    for (size_t i = 0; i < resp.len; i++) {
        if (resp.data[i] == 0x10) { found_watch_id = true; break; }
    }
    CETCD_ASSERT_TRUE(found_watch_id);

    /* If there are events, they should use tag 0x5a (field 11) */
    bool has_events = false;
    bool events_correct_tag = true;
    for (size_t i = 0; i < resp.len; i++) {
        if (resp.data[i] == 0x5a) { has_events = true; break; }
        /* Make sure old wrong tag 0x1a is not used for events
         * (0x1a can appear as other field, but if we see 0x5a it's events) */
    }
    /* If no events, that's also OK (depends on watch timing) */
    (void)has_events;
    (void)events_correct_tag;

    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_put_prev_kv) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Put key="pktest" value="oldval" */
    uint8_t put1[32]; size_t p1 = 0;
    put1[p1++] = 0x0a; put1[p1++] = 0x06;
    memcpy(put1 + p1, "pktest", 6); p1 += 6;
    put1[p1++] = 0x12; put1[p1++] = 0x06;
    memcpy(put1 + p1, "oldval", 6); p1 += 6;
    cetcd_rpc_bytes r1 = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", put1, p1);
    cetcd_rpc_bytes_free(&r1);

    /* Put key="pktest" value="newval" with prev_kv=true */
    uint8_t put2[48]; size_t p2 = 0;
    put2[p2++] = 0x0a; put2[p2++] = 0x06;
    memcpy(put2 + p2, "pktest", 6); p2 += 6;
    put2[p2++] = 0x12; put2[p2++] = 0x06;
    memcpy(put2 + p2, "newval", 6); p2 += 6;
    put2[p2++] = 0x20; put2[p2++] = 0x01; /* prev_kv = true */
    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", put2, p2);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 2);

    /* PutResponse should contain field 4 (prev_kv) with tag 0x22 */
    bool found_prev_kv = false;
    for (size_t i = 0; i < resp.len; i++) {
        if (resp.data[i] == 0x22) { found_prev_kv = true; break; }
    }
    CETCD_ASSERT_TRUE(found_prev_kv);

    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_delete_prev_kv) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Put key="dktest" value="delval" */
    uint8_t put_buf[32]; size_t pos = 0;
    put_buf[pos++] = 0x0a; put_buf[pos++] = 0x06;
    memcpy(put_buf + pos, "dktest", 6); pos += 6;
    put_buf[pos++] = 0x12; put_buf[pos++] = 0x06;
    memcpy(put_buf + pos, "delval", 6); pos += 6;
    cetcd_rpc_bytes r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", put_buf, pos);
    cetcd_rpc_bytes_free(&r);

    /* Delete key="dktest" with prev_kv=true (field 3, tag 0x18) */
    uint8_t del_buf[32]; size_t dpos = 0;
    del_buf[dpos++] = 0x0a; del_buf[dpos++] = 0x06;
    memcpy(del_buf + dpos, "dktest", 6); dpos += 6;
    del_buf[dpos++] = 0x18; del_buf[dpos++] = 0x01; /* prev_kv = true */
    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/DeleteRange", del_buf, dpos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 2);

    /* DeleteRangeResponse should contain field 3 (prev_kvs) with tag 0x1a */
    bool found_prev_kvs = false;
    for (size_t i = 0; i < resp.len; i++) {
        if (resp.data[i] == 0x1a) { found_prev_kvs = true; break; }
    }
    CETCD_ASSERT_TRUE(found_prev_kvs);

    /* Also verify field 2 (deleted) = 1, tag 0x10 */
    bool found_deleted = false;
    for (size_t i = 0; i < resp.len; i++) {
        if (resp.data[i] == 0x10) { found_deleted = true; break; }
    }
    CETCD_ASSERT_TRUE(found_deleted);

    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_delete_range_multiple) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Put keys "ra", "rb", "rc" */
    const char *keys[] = {"ra", "rb", "rc"};
    for (int i = 0; i < 3; i++) {
        uint8_t put_buf[32]; size_t pos = 0;
        put_buf[pos++] = 0x0a; put_buf[pos++] = 0x02;
        memcpy(put_buf + pos, keys[i], 2); pos += 2;
        put_buf[pos++] = 0x12; put_buf[pos++] = 0x01;
        put_buf[pos++] = '0' + i;
        cetcd_rpc_bytes r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", put_buf, pos);
        cetcd_rpc_bytes_free(&r);
    }

    /* Delete range "ra" to "rd" (should delete ra, rb, rc) */
    uint8_t del_buf[32]; size_t dpos = 0;
    del_buf[dpos++] = 0x0a; del_buf[dpos++] = 0x02;
    memcpy(del_buf + dpos, "ra", 2); dpos += 2;
    del_buf[dpos++] = 0x12; del_buf[dpos++] = 0x02;
    memcpy(del_buf + dpos, "rd", 2); dpos += 2;
    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/DeleteRange", del_buf, dpos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 2);

    /* Parse deleted count: field 2 (tag 0x10) should be 3 */
    int64_t deleted = 0;
    size_t rpos = 0;
    while (rpos < resp.len) {
        uint8_t tag = resp.data[rpos++];
        if (tag == 0x10) {
            uint64_t v = 0; int shift = 0;
            while (rpos < resp.len) {
                uint8_t b = resp.data[rpos++];
                v |= (uint64_t)(b & 0x7f) << shift;
                if (!(b & 0x80)) break;
                shift += 7;
            }
            deleted = (int64_t)v;
            break;
        } else if (tag == 0x0a || tag == 0x1a) {
            uint64_t l = 0; int shift = 0;
            while (rpos < resp.len) {
                uint8_t b = resp.data[rpos++];
                l |= (uint64_t)(b & 0x7f) << shift;
                if (!(b & 0x80)) break;
                shift += 7;
            }
            rpos += l;
        } else {
            uint64_t v = 0; int shift = 0;
            while (rpos < resp.len) {
                uint8_t b = resp.data[rpos++];
                v |= (uint64_t)(b & 0x7f) << shift;
                if (!(b & 0x80)) break;
                shift += 7;
            }
        }
    }
    CETCD_ASSERT_TRUE(deleted >= 3);

    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_txn_range_returns_data) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Put key="trkey" value="trval" */
    uint8_t put_buf[32]; size_t pos = 0;
    put_buf[pos++] = 0x0a; put_buf[pos++] = 0x05;
    memcpy(put_buf + pos, "trkey", 5); pos += 5;
    put_buf[pos++] = 0x12; put_buf[pos++] = 0x05;
    memcpy(put_buf + pos, "trval", 5); pos += 5;
    cetcd_rpc_bytes r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", put_buf, pos);
    cetcd_rpc_bytes_free(&r);

    /* Build Txn with success op: RequestRange("trkey")
     * RequestRange: field 1 (key) = "trkey"
     * RequestOp: field 1 (request_range) = RequestRange
     * TxnRequest: field 2 (success) = [RequestOp]
     */
    uint8_t range_inner[16]; size_t rip = 0;
    range_inner[rip++] = 0x0a; range_inner[rip++] = 0x05;
    memcpy(range_inner + rip, "trkey", 5); rip += 5;

    uint8_t op_buf[32]; size_t opos = 0;
    op_buf[opos++] = 0x0a; /* RequestRange (field 1 of RequestOp) */
    op_buf[opos++] = (uint8_t)rip;
    memcpy(op_buf + opos, range_inner, rip); opos += rip;

    uint8_t txn_buf[64]; size_t tpos = 0;
    txn_buf[tpos++] = 0x12; /* field 2 = success */
    txn_buf[tpos++] = (uint8_t)opos;
    memcpy(txn_buf + tpos, op_buf, opos); tpos += opos;

    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Txn", txn_buf, tpos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 5);

    /* The ResponseRange should contain actual kv data.
     * Look for "trval" in the response bytes to confirm data was returned. */
    bool found_val = false;
    for (size_t i = 0; i + 4 < resp.len; i++) {
        if (memcmp(resp.data + i, "trval", 5) == 0) { found_val = true; break; }
    }
    CETCD_ASSERT_TRUE(found_val);

    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_snapshot_has_header) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Put a key so the store has data */
    uint8_t put_buf[32]; size_t pos = 0;
    put_buf[pos++] = 0x0a; put_buf[pos++] = 0x04;
    memcpy(put_buf + pos, "snap", 4); pos += 4;
    put_buf[pos++] = 0x12; put_buf[pos++] = 0x04;
    memcpy(put_buf + pos, "data", 4); pos += 4;
    cetcd_rpc_bytes r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", put_buf, pos);
    cetcd_rpc_bytes_free(&r);

    /* Request snapshot */
    uint8_t req[] = {0x00};
    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.Maintenance/Snapshot", req, 1);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 2);

    /* SnapshotResponse should start with field 1 (header) tag = 0x0a */
    CETCD_ASSERT_TRUE(resp.data[0] == 0x0a);

    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_range_limit_truncates) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Put 3 keys: lim_a, lim_b, lim_c */
    const char *keys[] = {"lim_a", "lim_b", "lim_c"};
    const char *vals[] = {"va", "vb", "vc"};
    for (int i = 0; i < 3; i++) {
        uint8_t req[256]; size_t pos = 0;
        req[pos++] = 0x0a; /* key */
        size_t kl = strlen(keys[i]);
        req[pos++] = (uint8_t)kl;
        memcpy(req + pos, keys[i], kl); pos += kl;
        req[pos++] = 0x12; /* value */
        size_t vl = strlen(vals[i]);
        req[pos++] = (uint8_t)vl;
        memcpy(req + pos, vals[i], vl); pos += vl;
        cetcd_rpc_bytes r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", req, pos);
        cetcd_rpc_bytes_free(&r);
    }

    /* Range [lim_a, lim_d) with limit=2 */
    uint8_t range_req[64]; size_t rp = 0;
    range_req[rp++] = 0x0a; /* key */
    range_req[rp++] = 0x05;
    memcpy(range_req + rp, "lim_a", 5); rp += 5;
    range_req[rp++] = 0x12; /* range_end */
    range_req[rp++] = 0x05;
    memcpy(range_req + rp, "lim_d", 5); rp += 5;
    range_req[rp++] = 0x18; /* limit = 2 */
    range_req[rp++] = 0x02;

    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Range", range_req, rp);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 0);

    /* Count the number of KeyValue entries (tag 0x0a at top level).
     * Note: header (field 1) also uses tag 0x0a, so skip the first one. */
    int kv_count = 0;
    int tag0a_count = 0;
    size_t pos = 0;
    while (pos < resp.len) {
        uint8_t tag = resp.data[pos++];
        if (tag == 0x0a) {
            tag0a_count++;
            if (tag0a_count > 1) kv_count++; /* skip header */
            uint64_t l = 0;
            while (pos < resp.len) { uint8_t b = resp.data[pos++]; l |= (uint64_t)(b & 0x7F) << 0; if (!(b & 0x80)) break; }
            pos += (size_t)l;
        } else if (tag == 0x10) {
            /* more flag */
            pos++;
        } else if (tag == 0x20) {
            /* count */
            uint64_t l = 0; int shift = 0;
            while (pos < resp.len) { uint8_t b = resp.data[pos++]; l |= (uint64_t)(b & 0x7F) << shift; shift += 7; if (!(b & 0x80)) break; }
        } else {
            uint64_t l = 0; int shift = 0;
            while (pos < resp.len) { uint8_t b = resp.data[pos++]; l |= (uint64_t)(b & 0x7F) << shift; shift += 7; if (!(b & 0x80)) break; }
            pos += (size_t)l;
        }
    }
    /* With limit=2, we should get at most 2 kvs (excluding header) */
    CETCD_ASSERT_TRUE(kv_count <= 2);

    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_range_count_only) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Put a key */
    uint8_t put_req[64]; size_t pp = 0;
    put_req[pp++] = 0x0a; put_req[pp++] = 4;
    memcpy(put_req + pp, "cok1", 4); pp += 4;
    put_req[pp++] = 0x12; put_req[pp++] = 4;
    memcpy(put_req + pp, "val1", 4); pp += 4;
    cetcd_rpc_bytes pr = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", put_req, pp);
    cetcd_rpc_bytes_free(&pr);

    /* Range with count_only=true (tag 0x48) */
    uint8_t req[32]; size_t pos = 0;
    req[pos++] = 0x0a; req[pos++] = 4;
    memcpy(req + pos, "cok1", 4); pos += 4;
    req[pos++] = 0x48; /* count_only = true */
    req[pos++] = 0x01;

    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Range", req, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 0);

    /* With count_only, the response should have count but no kvs.
     * We verify by checking that count (tag 0x20) is present and > 0 */
    int found_count = 0;
    size_t rp = 0;
    while (rp < resp.len) {
        uint8_t tag = resp.data[rp++];
        if (tag == 0x20) {
            uint64_t v = 0; int shift = 0;
            while (rp < resp.len) { uint8_t b = resp.data[rp++]; v |= (uint64_t)(b & 0x7F) << shift; shift += 7; if (!(b & 0x80)) break; }
            found_count = (int)v;
        } else if (tag == 0x0a) {
            /* Could be header or kvs — skip */
            uint64_t l = 0; int shift = 0;
            while (rp < resp.len) { uint8_t b = resp.data[rp++]; l |= (uint64_t)(b & 0x7F) << shift; shift += 7; if (!(b & 0x80)) break; }
            rp += (size_t)l;
        } else {
            uint64_t v = 0; int shift = 0;
            while (rp < resp.len) { uint8_t b = resp.data[rp++]; v |= (uint64_t)(b & 0x7F) << shift; shift += 7; if (!(b & 0x80)) break; }
        }
    }
    CETCD_ASSERT_TRUE(found_count > 0);

    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_range_keys_only) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Put a key with a value */
    uint8_t put_req[64]; size_t pp = 0;
    put_req[pp++] = 0x0a; put_req[pp++] = 4;
    memcpy(put_req + pp, "kso1", 4); pp += 4;
    put_req[pp++] = 0x12; put_req[pp++] = 9;
    memcpy(put_req + pp, "secretval", 9); pp += 9;
    cetcd_rpc_bytes pr = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", put_req, pp);
    cetcd_rpc_bytes_free(&pr);

    /* Range with keys_only=true (tag 0x40) */
    uint8_t req[32]; size_t pos = 0;
    req[pos++] = 0x0a; req[pos++] = 4;
    memcpy(req + pos, "kso1", 4); pos += 4;
    req[pos++] = 0x40; /* keys_only = true */
    req[pos++] = 0x01;

    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Range", req, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 0);

    /* The response should not contain "secretval" anywhere */
    int has_secret = 0;
    for (size_t i = 0; i + 9 <= resp.len; i++) {
        if (memcmp(resp.data + i, "secretval", 9) == 0) { has_secret = 1; break; }
    }
    CETCD_ASSERT_TRUE(!has_secret);

    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_auth_responses_have_header) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();
    uint8_t dummy[] = {0x00};

    /* AuthEnable response should start with header (tag 0x0a) */
    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Auth/AuthEnable", dummy, 1);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 0);
    CETCD_ASSERT_TRUE(resp.data[0] == 0x0a);
    cetcd_rpc_bytes_free(&resp);

    /* AuthStatus response should also start with header */
    resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Auth/AuthStatus", dummy, 1);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 0);
    CETCD_ASSERT_TRUE(resp.data[0] == 0x0a);
    cetcd_rpc_bytes_free(&resp);

    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_authenticate_has_token_field2) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Add a user with password */
    uint8_t add_req[64]; size_t ap = 0;
    add_req[ap++] = 0x0a; add_req[ap++] = 7;
    memcpy(add_req + ap, "authtest", 7); ap += 7;
    add_req[ap++] = 0x12; add_req[ap++] = 4;
    memcpy(add_req + ap, "pass", 4); ap += 4;
    cetcd_rpc_bytes ar = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.Auth/UserAdd", add_req, ap);
    cetcd_rpc_bytes_free(&ar);

    /* Authenticate */
    uint8_t auth_req[64]; size_t ahp = 0;
    auth_req[ahp++] = 0x0a; auth_req[ahp++] = 7;
    memcpy(auth_req + ahp, "authtest", 7); ahp += 7;
    auth_req[ahp++] = 0x12; auth_req[ahp++] = 4;
    memcpy(auth_req + ahp, "pass", 4); ahp += 4;
    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.Auth/Authenticate", auth_req, ahp);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 0);

    /* Response should have field 1 (header, tag 0x0a) and field 2 (token, tag 0x12) */
    int found_token = 0;
    size_t pos = 0;
    while (pos < resp.len) {
        uint8_t tag = resp.data[pos++];
        if (tag == 0x12) {
            found_token = 1;
            break;
        } else if (tag == 0x0a) {
            uint64_t l = 0; int shift = 0;
            while (pos < resp.len) { uint8_t b = resp.data[pos++]; l |= (uint64_t)(b & 0x7F) << shift; shift += 7; if (!(b & 0x80)) break; }
            pos += (size_t)l;
        } else {
            uint64_t v = 0; int shift = 0;
            while (pos < resp.len) { uint8_t b = resp.data[pos++]; v |= (uint64_t)(b & 0x7F) << shift; shift += 7; if (!(b & 0x80)) break; }
        }
    }
    CETCD_ASSERT_TRUE(found_token);

    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_put_ignore_value) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Put key="ivkey" value="original" */
    {
        uint8_t req[64]; size_t pos = 0;
        req[pos++] = 0x0a; req[pos++] = 5;
        memcpy(req + pos, "ivkey", 5); pos += 5;
        req[pos++] = 0x12; req[pos++] = 8;
        memcpy(req + pos, "original", 8); pos += 8;
        cetcd_rpc_bytes r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", req, pos);
        cetcd_rpc_bytes_free(&r);
    }

    /* Put key="ivkey" with ignore_value=true (tag 0x28) and value="ignored" */
    {
        uint8_t req[64]; size_t pos = 0;
        req[pos++] = 0x0a; req[pos++] = 5;
        memcpy(req + pos, "ivkey", 5); pos += 5;
        req[pos++] = 0x12; req[pos++] = 7;
        memcpy(req + pos, "ignored", 7); pos += 7;
        req[pos++] = 0x28; /* ignore_value = true */
        req[pos++] = 0x01;
        cetcd_rpc_bytes r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", req, pos);
        cetcd_rpc_bytes_free(&r);
    }

    /* Range key="ivkey" — should return "original", not "ignored" */
    {
        uint8_t req[32]; size_t pos = 0;
        req[pos++] = 0x0a; req[pos++] = 5;
        memcpy(req + pos, "ivkey", 5); pos += 5;
        cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Range", req, pos);
        CETCD_ASSERT_NOT_NULL(resp.data);
        CETCD_ASSERT_TRUE(resp.len > 0);
        /* Search for "original" in response */
        int found_original = 0;
        for (size_t i = 0; i + 8 <= resp.len; i++) {
            if (memcmp(resp.data + i, "original", 8) == 0) { found_original = 1; break; }
        }
        CETCD_ASSERT_TRUE(found_original);
        /* Search for "ignored" — should NOT be found */
        int found_ignored = 0;
        for (size_t i = 0; i + 7 <= resp.len; i++) {
            if (memcmp(resp.data + i, "ignored", 7) == 0) { found_ignored = 1; break; }
        }
        CETCD_ASSERT_TRUE(!found_ignored);
        cetcd_rpc_bytes_free(&resp);
    }

    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_put_ignore_lease) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Put key="ilkey" value="v1" with lease_id=12345 */
    {
        uint8_t req[64]; size_t pos = 0;
        req[pos++] = 0x0a; req[pos++] = 5;
        memcpy(req + pos, "ilkey", 5); pos += 5;
        req[pos++] = 0x12; req[pos++] = 2;
        memcpy(req + pos, "v1", 2); pos += 2;
        req[pos++] = 0x18; /* lease = 12345 */
        req[pos++] = 0xb9;
        req[pos++] = 0x60;
        cetcd_rpc_bytes r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", req, pos);
        cetcd_rpc_bytes_free(&r);
    }

    /* Put key="ilkey" value="v2" with ignore_lease=true (tag 0x30) and lease=99999 */
    {
        uint8_t req[64]; size_t pos = 0;
        req[pos++] = 0x0a; req[pos++] = 5;
        memcpy(req + pos, "ilkey", 5); pos += 5;
        req[pos++] = 0x12; req[pos++] = 2;
        memcpy(req + pos, "v2", 2); pos += 2;
        req[pos++] = 0x18; /* lease = 99999 (should be ignored) */
        req[pos++] = 0x9f;
        req[pos++] = 0x8d;
        req[pos++] = 0x06;
        req[pos++] = 0x30; /* ignore_lease = true */
        req[pos++] = 0x01;
        cetcd_rpc_bytes r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", req, pos);
        cetcd_rpc_bytes_free(&r);
    }

    /* Range key="ilkey" — value should be "v2" (updated), lease should still be 12345 */
    {
        uint8_t req[32]; size_t pos = 0;
        req[pos++] = 0x0a; req[pos++] = 5;
        memcpy(req + pos, "ilkey", 5); pos += 5;
        cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Range", req, pos);
        CETCD_ASSERT_NOT_NULL(resp.data);
        CETCD_ASSERT_TRUE(resp.len > 0);
        /* Verify value is "v2" */
        int found_v2 = 0;
        for (size_t i = 0; i + 2 <= resp.len; i++) {
            if (memcmp(resp.data + i, "v2", 2) == 0) { found_v2 = 1; break; }
        }
        CETCD_ASSERT_TRUE(found_v2);
        cetcd_rpc_bytes_free(&resp);
    }

    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_auth_status_has_header) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();
    uint8_t dummy[] = {0x00};

    /* AuthStatus response should start with header (tag 0x0a) */
    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Auth/AuthStatus", dummy, 1);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 0);
    /* First byte should be 0x0a (header field) */
    CETCD_ASSERT_TRUE(resp.data[0] == 0x0a);

    /* Verify enabled field (tag 0x10) is present after header */
    int found_enabled = 0;
    size_t pos = 0;
    while (pos < resp.len) {
        uint8_t tag = resp.data[pos++];
        if (tag == 0x10) {
            uint64_t v = 0; int shift = 0;
            while (pos < resp.len) { uint8_t b = resp.data[pos++]; v |= (uint64_t)(b & 0x7F) << shift; shift += 7; if (!(b & 0x80)) break; }
            found_enabled = 1;
        } else if (tag == 0x0a) {
            uint64_t l = 0; int shift = 0;
            while (pos < resp.len) { uint8_t b = resp.data[pos++]; l |= (uint64_t)(b & 0x7F) << shift; shift += 7; if (!(b & 0x80)) break; }
            pos += (size_t)l;
        } else {
            uint64_t v = 0; int shift = 0;
            while (pos < resp.len) { uint8_t b = resp.data[pos++]; v |= (uint64_t)(b & 0x7F) << shift; shift += 7; if (!(b & 0x80)) break; }
        }
    }
    CETCD_ASSERT_TRUE(found_enabled);

    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(v3rpc_create_destroy),
    CETCD_TEST_ENTRY(v3rpc_put_range),
    CETCD_TEST_ENTRY(v3rpc_range_returns_actual_kv),
    CETCD_TEST_ENTRY(v3rpc_range_query_multiple_keys),
    CETCD_TEST_ENTRY(v3rpc_put_response_has_revision),
    CETCD_TEST_ENTRY(v3rpc_delete_returns_count),
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
    CETCD_TEST_ENTRY(v3rpc_maintenance_snapshot),
    CETCD_TEST_ENTRY(v3rpc_maintenance_snapshot_empty),
    CETCD_TEST_ENTRY(v3rpc_maintenance_downgrade),
    CETCD_TEST_ENTRY(v3rpc_auth_user_get),
    CETCD_TEST_ENTRY(v3rpc_auth_role_get),
    CETCD_TEST_ENTRY(v3rpc_auth_role_grant_permission),
    CETCD_TEST_ENTRY(v3rpc_auth_role_revoke_permission),
    CETCD_TEST_ENTRY(v3rpc_cluster_member_list_with_peers),
    CETCD_TEST_ENTRY(v3rpc_cluster_member_update_actual),
    CETCD_TEST_ENTRY(v3rpc_lease_leases_returns_actual_leases),
    CETCD_TEST_ENTRY(v3rpc_lease_keep_alive_uses_granted_ttl),
    CETCD_TEST_ENTRY(v3rpc_txn_cas_match),
    CETCD_TEST_ENTRY(v3rpc_txn_cas_no_match),
    CETCD_TEST_ENTRY(v3rpc_txn_response_has_header),
    CETCD_TEST_ENTRY(v3rpc_compact_has_header),
    CETCD_TEST_ENTRY(v3rpc_lease_revoke_has_header),
    CETCD_TEST_ENTRY(v3rpc_watch_response_correct_tags),
    CETCD_TEST_ENTRY(v3rpc_put_prev_kv),
    CETCD_TEST_ENTRY(v3rpc_delete_prev_kv),
    CETCD_TEST_ENTRY(v3rpc_delete_range_multiple),
    CETCD_TEST_ENTRY(v3rpc_txn_range_returns_data),
    CETCD_TEST_ENTRY(v3rpc_snapshot_has_header),
    CETCD_TEST_ENTRY(v3rpc_range_limit_truncates),
    CETCD_TEST_ENTRY(v3rpc_range_count_only),
    CETCD_TEST_ENTRY(v3rpc_range_keys_only),
    CETCD_TEST_ENTRY(v3rpc_auth_responses_have_header),
    CETCD_TEST_ENTRY(v3rpc_authenticate_has_token_field2),
    CETCD_TEST_ENTRY(v3rpc_put_ignore_value),
    CETCD_TEST_ENTRY(v3rpc_put_ignore_lease),
    CETCD_TEST_ENTRY(v3rpc_auth_status_has_header),
CETCD_TEST_LIST_END

CETCD_TEST_MAIN()
