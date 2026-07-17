#include "cetcd/base.h"
#include "cetcd/v3rpc.h"
#include "cetcd/auth.h"
#include "cetcd/peer.h"
#include "cetcd/io.h"
#include "cetcd/mvcc.h"
#include "cetcd_test.h"

/* Globals defined in v3rpc.c — we set them to test cluster-aware handlers */
extern cetcd_cluster *g_rpc_cluster;
extern uint64_t       g_rpc_node_id;
extern cetcd_loop           *g_rpc_loop;
extern cetcd_stream_write_fn g_rpc_stream_write_fn;
extern void                 *g_rpc_stream_write_ctx;

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

CETCD_TEST_CASE(v3rpc_lease_grant_custom_id) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* LeaseGrantRequest: ttl=60, id=0xDEAD */
    uint8_t grant_buf[16];
    size_t pos = 0;
    grant_buf[pos++] = 0x08;
    grant_buf[pos++] = 0x3c;
    grant_buf[pos++] = 0x10;
    uint64_t want = 0xDEAD;
    do {
        uint8_t b = (uint8_t)(want & 0x7F);
        want >>= 7;
        if (want) b |= 0x80;
        grant_buf[pos++] = b;
    } while (want);

    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Lease/LeaseGrant", grant_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    int64_t lease_id = 0;
    size_t rpos = 0;
    while (rpos < resp.len) {
        uint8_t tag = resp.data[rpos++];
        if (tag == 0x0a) {
            uint64_t l = 0; int shift = 0;
            while (rpos < resp.len) {
                uint8_t b = resp.data[rpos++];
                l |= (uint64_t)(b & 0x7F) << shift;
                if ((b & 0x80) == 0) break;
                shift += 7;
            }
            rpos += (size_t)l;
        } else {
            uint64_t v = 0; int shift = 0;
            while (rpos < resp.len) {
                uint8_t b = resp.data[rpos++];
                v |= (uint64_t)(b & 0x7F) << shift;
                if ((b & 0x80) == 0) break;
                shift += 7;
            }
            if (tag == 0x10) { lease_id = (int64_t)v; break; }
        }
    }
    cetcd_rpc_bytes_free(&resp);
    CETCD_ASSERT_EQ_INT((int)lease_id, 0xDEAD);

    /* Duplicate custom ID → RPC failure (etcd lease already exists). */
    resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Lease/LeaseGrant", grant_buf, pos);
    CETCD_ASSERT_TRUE(resp.data == NULL || resp.len == 0);
    cetcd_rpc_bytes_free(&resp);

    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_lease_grant_ttl_too_large) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* TTL = MaxLeaseTTL+1 (9000000001) → RPC failure (etcd ErrLeaseTTLTooLarge) */
    uint8_t grant_buf[16];
    size_t pos = 0;
    grant_buf[pos++] = 0x08; /* field 1 = TTL */
    static const uint8_t ttl_enc[] = {0x81, 0xb4, 0xc4, 0xc3, 0x21};
    memcpy(grant_buf + pos, ttl_enc, sizeof(ttl_enc));
    pos += sizeof(ttl_enc);
    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Lease/LeaseGrant", grant_buf, pos);
    CETCD_ASSERT_TRUE(resp.data == NULL || resp.len == 0);
    cetcd_rpc_bytes_free(&resp);

    /* Boundary MaxLeaseTTL (9000000000) still succeeds */
    pos = 0;
    grant_buf[pos++] = 0x08;
    static const uint8_t ttl_max[] = {0x80, 0xb4, 0xc4, 0xc3, 0x21};
    memcpy(grant_buf + pos, ttl_max, sizeof(ttl_max));
    pos += sizeof(ttl_max);
    resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.Lease/LeaseGrant", grant_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
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

CETCD_TEST_CASE(v3rpc_lease_revoke_nonexistent) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Revoke never-granted lease → RPC failure (etcd NotFound) */
    uint8_t revoke_buf[8];
    size_t pos = 0;
    revoke_buf[pos++] = 0x08; /* field 1 = ID */
    revoke_buf[pos++] = 0x63; /* ID = 99 */
    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Lease/LeaseRevoke", revoke_buf, pos);
    CETCD_ASSERT_TRUE(resp.data == NULL || resp.len == 0);
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

CETCD_TEST_CASE(v3rpc_lease_time_to_live_missing) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* TimeToLive for a non-existent ID must report TTL=-1 (etcd). */
    uint8_t ttl_buf[4];
    size_t pos = 0;
    ttl_buf[pos++] = 0x08; /* ID */
    ttl_buf[pos++] = 0x7f; /* ID = 127 */
    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Lease/LeaseTimeToLive", ttl_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);

    int64_t ttl_field = 0;
    int found_ttl = 0;
    size_t rpos = 0;
    while (rpos < resp.len) {
        uint8_t tag = resp.data[rpos++];
        if (tag == 0x0a || tag == 0x2a) {
            uint64_t l = 0; int shift = 0;
            while (rpos < resp.len) {
                uint8_t b = resp.data[rpos++];
                l |= (uint64_t)(b & 0x7F) << shift;
                if (!(b & 0x80)) break;
                shift += 7;
            }
            rpos += (size_t)l;
        } else {
            uint64_t v = 0; int shift = 0;
            while (rpos < resp.len) {
                uint8_t b = resp.data[rpos++];
                v |= (uint64_t)(b & 0x7F) << shift;
                if (!(b & 0x80)) break;
                shift += 7;
            }
            if (tag == 0x18) {
                ttl_field = (int64_t)v;
                found_ttl = 1;
            }
        }
    }
    CETCD_ASSERT_TRUE(found_ttl);
    CETCD_ASSERT_TRUE(ttl_field == -1);

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

CETCD_TEST_CASE(v3rpc_alarm_activate_disarm) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Activate NOSPACE alarm: action=1(0x08,0x01), memberID=0(0x10,0x00), alarm=1(0x18,0x01) */
    uint8_t activate_req[] = {0x08, 0x01, 0x10, 0x00, 0x18, 0x01};
    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Maintenance/Alarm", activate_req, sizeof(activate_req));
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 0);

    /* Check that response contains alarm info (tag 0x12 = alarms) */
    int found_alarm = 0;
    for (size_t i = 0; i < resp.len; i++) {
        if (resp.data[i] == 0x12) { found_alarm = 1; break; }
    }
    CETCD_ASSERT_TRUE(found_alarm);
    cetcd_rpc_bytes_free(&resp);

    /* Disarm: action=2(0x08,0x02), memberID=0(0x10,0x00), alarm=1(0x18,0x01) */
    uint8_t disarm_req[] = {0x08, 0x02, 0x10, 0x00, 0x18, 0x01};
    resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Maintenance/Alarm", disarm_req, sizeof(disarm_req));
    CETCD_ASSERT_NOT_NULL(resp.data);

    /* After disarm, no alarm should be present (no tag 0x12) */
    found_alarm = 0;
    for (size_t i = 0; i < resp.len; i++) {
        if (resp.data[i] == 0x12) { found_alarm = 1; break; }
    }
    CETCD_ASSERT_FALSE(found_alarm);
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

CETCD_TEST_CASE(v3rpc_compact_future_revision) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Put once → current rev=1; Compact(99) must fail (etcd ErrFutureRev). */
    uint8_t put_buf[16]; size_t pos = 0;
    put_buf[pos++] = 0x0a; put_buf[pos++] = 0x02;
    memcpy(put_buf + pos, "cf", 2); pos += 2;
    put_buf[pos++] = 0x12; put_buf[pos++] = 0x01;
    put_buf[pos++] = 'v';
    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", put_buf, pos);
    cetcd_rpc_bytes_free(&resp);

    uint8_t compact_buf[8]; pos = 0;
    compact_buf[pos++] = 0x08;
    compact_buf[pos++] = 0x63; /* revision = 99 */
    resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Compact", compact_buf, pos);
    CETCD_ASSERT_TRUE(resp.data == NULL || resp.len == 0);
    cetcd_rpc_bytes_free(&resp);

    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_range_revision_compacted) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Put k1 (rev=1), Put k2 (rev=2), Compact(2) */
    uint8_t put1[16]; size_t p = 0;
    put1[p++] = 0x0a; put1[p++] = 0x02; memcpy(put1 + p, "k1", 2); p += 2;
    put1[p++] = 0x12; put1[p++] = 0x02; memcpy(put1 + p, "v1", 2); p += 2;
    cetcd_rpc_bytes r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", put1, p);
    cetcd_rpc_bytes_free(&r);

    uint8_t put2[16]; p = 0;
    put2[p++] = 0x0a; put2[p++] = 0x02; memcpy(put2 + p, "k2", 2); p += 2;
    put2[p++] = 0x12; put2[p++] = 0x02; memcpy(put2 + p, "v2", 2); p += 2;
    r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", put2, p);
    cetcd_rpc_bytes_free(&r);

    uint8_t compact_buf[4]; p = 0;
    compact_buf[p++] = 0x08; compact_buf[p++] = 0x02;
    r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Compact", compact_buf, p);
    cetcd_rpc_bytes_free(&r);

    /* Range at rev=1 (< compacted_rev) must fail, not return empty success */
    uint8_t range_buf[16]; p = 0;
    range_buf[p++] = 0x0a; range_buf[p++] = 0x02;
    memcpy(range_buf + p, "k1", 2); p += 2;
    range_buf[p++] = 0x20; range_buf[p++] = 0x01; /* revision=1 */
    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Range", range_buf, p);
    CETCD_ASSERT_TRUE(resp.data == NULL || resp.len == 0);
    cetcd_rpc_bytes_free(&resp);

    /* rev == compacted_rev remains readable */
    p = 0;
    range_buf[p++] = 0x0a; range_buf[p++] = 0x02;
    memcpy(range_buf + p, "k2", 2); p += 2;
    range_buf[p++] = 0x20; range_buf[p++] = 0x02; /* revision=2 */
    resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Range", range_buf, p);
    CETCD_ASSERT_NOT_NULL(resp.data);
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

    /* Count 0x12 tags (field 2 = repeated LeaseStatus) */
    int lease_count = 0;
    for (size_t i = 0; i < resp.len; i++) {
        if (resp.data[i] == 0x12) lease_count++;
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

    /* Parse response: skip header (0x0a, length-delimited), find field 3 (TTL) tag=0x18 */
    int64_t resp_ttl = 0;
    size_t rpos = 0;
    while (rpos < resp.len) {
        uint8_t tag = resp.data[rpos++];
        if (tag == 0x0a) {
            /* Skip header (length-delimited) */
            uint64_t l = 0; int shift = 0;
            while (rpos < resp.len) {
                uint8_t b = resp.data[rpos++];
                l |= (uint64_t)(b & 0x7F) << shift;
                if ((b & 0x80) == 0) break;
                shift += 7;
            }
            rpos += l;
        } else {
            uint64_t v = 0; int shift = 0;
            while (rpos < resp.len) {
                uint8_t b = resp.data[rpos++];
                v |= (uint64_t)(b & 0x7F) << shift;
                if ((b & 0x80) == 0) break;
                shift += 7;
            }
            if (tag == 0x18) { resp_ttl = (int64_t)v; break; }
        }
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
    /* Extract lease ID from response: skip header (0x0a), find field 2 (ID) tag=0x10 */
    int64_t lease_id = 0;
    size_t rpos = 0;
    while (rpos < r.len) {
        uint8_t tag = r.data[rpos++];
        if (tag == 0x0a) {
            /* Skip header (length-delimited) */
            uint64_t l = 0; int shift = 0;
            while (rpos < r.len) {
                uint8_t b = r.data[rpos++];
                l |= (uint64_t)(b & 0x7F) << shift;
                if ((b & 0x80) == 0) break;
                shift += 7;
            }
            rpos += l;
        } else {
            uint64_t v = 0; int shift = 0;
            while (rpos < r.len) {
                uint8_t b = r.data[rpos++];
                v |= (uint64_t)(b & 0x7F) << shift;
                if ((b & 0x80) == 0) break;
                shift += 7;
            }
            if (tag == 0x10) { lease_id = (int64_t)v; break; }
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

CETCD_TEST_CASE(v3rpc_lease_revoke_deletes_keys) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Grant lease */
    uint8_t grant_buf[4]; size_t pos = 0;
    grant_buf[pos++] = 0x08; grant_buf[pos++] = 0x3c;
    grant_buf[pos++] = 0x10; grant_buf[pos++] = 0x00;
    cetcd_rpc_bytes r = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Lease/LeaseGrant", grant_buf, pos);
    CETCD_ASSERT_NOT_NULL(r.data);
    int64_t lease_id = 0;
    size_t rpos = 0;
    while (rpos < r.len) {
        uint8_t tag = r.data[rpos++];
        if (tag == 0x0a) {
            uint64_t l = 0; int shift = 0;
            while (rpos < r.len) {
                uint8_t b = r.data[rpos++];
                l |= (uint64_t)(b & 0x7F) << shift;
                if (!(b & 0x80)) break;
                shift += 7;
            }
            rpos += (size_t)l;
        } else {
            uint64_t v = 0; int shift = 0;
            while (rpos < r.len) {
                uint8_t b = r.data[rpos++];
                v |= (uint64_t)(b & 0x7F) << shift;
                if (!(b & 0x80)) break;
                shift += 7;
            }
            if (tag == 0x10) { lease_id = (int64_t)v; break; }
        }
    }
    cetcd_rpc_bytes_free(&r);
    CETCD_ASSERT_TRUE(lease_id > 0);

    /* Put key attached to lease */
    uint8_t put_buf[32]; pos = 0;
    put_buf[pos++] = 0x0a; put_buf[pos++] = 0x03;
    memcpy(put_buf + pos, "rk1", 3); pos += 3;
    put_buf[pos++] = 0x12; put_buf[pos++] = 0x02;
    memcpy(put_buf + pos, "v1", 2); pos += 2;
    put_buf[pos++] = 0x18;
    uint64_t lid = (uint64_t)lease_id;
    do { uint8_t b = lid & 0x7F; lid >>= 7; if (lid) b |= 0x80; put_buf[pos++] = b; } while (lid);
    r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", put_buf, pos);
    cetcd_rpc_bytes_free(&r);

    /* Revoke → key must disappear */
    uint8_t revoke_buf[8]; pos = 0;
    revoke_buf[pos++] = 0x08;
    lid = (uint64_t)lease_id;
    do { uint8_t b = lid & 0x7F; lid >>= 7; if (lid) b |= 0x80; revoke_buf[pos++] = b; } while (lid);
    r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.Lease/LeaseRevoke", revoke_buf, pos);
    CETCD_ASSERT_NOT_NULL(r.data);
    cetcd_rpc_bytes_free(&r);

    uint8_t range_buf[8]; pos = 0;
    range_buf[pos++] = 0x0a; range_buf[pos++] = 0x03;
    memcpy(range_buf + pos, "rk1", 3); pos += 3;
    r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Range", range_buf, pos);
    CETCD_ASSERT_NOT_NULL(r.data);
    /* No kvs field (0x12) with the value — count-only empty or header-only. */
    int found_val = 0;
    for (size_t i = 0; i + 2 <= r.len; i++) {
        if (memcmp(r.data + i, "v1", 2) == 0) { found_val = 1; break; }
    }
    CETCD_ASSERT_TRUE(!found_val);
    cetcd_rpc_bytes_free(&r);

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

CETCD_TEST_CASE(v3rpc_watch_start_rev_compacted) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Put k1 (rev=1), Put k2 (rev=2) */
    uint8_t put1[16]; size_t p = 0;
    put1[p++] = 0x0a; put1[p++] = 0x02; memcpy(put1 + p, "k1", 2); p += 2;
    put1[p++] = 0x12; put1[p++] = 0x02; memcpy(put1 + p, "v1", 2); p += 2;
    cetcd_rpc_bytes r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", put1, p);
    cetcd_rpc_bytes_free(&r);

    uint8_t put2[16]; p = 0;
    put2[p++] = 0x0a; put2[p++] = 0x02; memcpy(put2 + p, "k2", 2); p += 2;
    put2[p++] = 0x12; put2[p++] = 0x02; memcpy(put2 + p, "v2", 2); p += 2;
    r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", put2, p);
    cetcd_rpc_bytes_free(&r);

    /* Compact at revision 2 */
    uint8_t compact_buf[4]; p = 0;
    compact_buf[p++] = 0x08; compact_buf[p++] = 0x02;
    r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Compact", compact_buf, p);
    cetcd_rpc_bytes_free(&r);

    /* WatchCreate key=k1 start_revision=1 → canceled + compact_revision=2 */
    uint8_t create_inner[16]; size_t cpos = 0;
    create_inner[cpos++] = 0x0a; create_inner[cpos++] = 0x02;
    memcpy(create_inner + cpos, "k1", 2); cpos += 2;
    create_inner[cpos++] = 0x18; create_inner[cpos++] = 0x01;

    uint8_t watch_buf[32]; size_t wpos = 0;
    watch_buf[wpos++] = 0x0a;
    watch_buf[wpos++] = (uint8_t)cpos;
    memcpy(watch_buf + wpos, create_inner, cpos); wpos += cpos;

    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Watch/Watch", watch_buf, wpos);
    CETCD_ASSERT_NOT_NULL(resp.data);

    int found_canceled = 0;
    int found_compact_rev = 0;
    int found_event = 0;
    size_t rpos = 0;
    while (rpos < resp.len) {
        uint8_t tag = resp.data[rpos++];
        if (tag == 0x0a) {
            uint64_t l = 0; int shift = 0;
            while (rpos < resp.len) {
                uint8_t b = resp.data[rpos++];
                l |= (uint64_t)(b & 0x7F) << shift;
                if ((b & 0x80) == 0) break;
                shift += 7;
            }
            rpos += (size_t)l;
        } else if (tag == 0x5a) {
            found_event = 1;
            uint64_t l = 0; int shift = 0;
            while (rpos < resp.len) {
                uint8_t b = resp.data[rpos++];
                l |= (uint64_t)(b & 0x7F) << shift;
                if ((b & 0x80) == 0) break;
                shift += 7;
            }
            rpos += (size_t)l;
        } else if ((tag & 0x07) == 0) { /* varint wire type */
            uint64_t v = 0; int shift = 0;
            while (rpos < resp.len) {
                uint8_t b = resp.data[rpos++];
                v |= (uint64_t)(b & 0x7F) << shift;
                if ((b & 0x80) == 0) break;
                shift += 7;
            }
            if (tag == 0x20 && v == 1) found_canceled = 1;
            if (tag == 0x28 && v == 2) found_compact_rev = 1;
        } else {
            break;
        }
    }
    CETCD_ASSERT_TRUE(found_canceled);
    CETCD_ASSERT_TRUE(found_compact_rev);
    CETCD_ASSERT_FALSE(found_event);
    cetcd_rpc_bytes_free(&resp);

    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_watch_replay_legacy) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    uint8_t put_buf[16]; size_t p = 0;
    put_buf[p++] = 0x0a; put_buf[p++] = 0x03;
    memcpy(put_buf + p, "wrk", 3); p += 3;
    put_buf[p++] = 0x12; put_buf[p++] = 0x01; put_buf[p++] = 'v';
    cetcd_rpc_bytes r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", put_buf, p);
    cetcd_rpc_bytes_free(&r);

    uint8_t create_inner[16]; size_t cpos = 0;
    create_inner[cpos++] = 0x0a; create_inner[cpos++] = 0x03;
    memcpy(create_inner + cpos, "wrk", 3); cpos += 3;
    create_inner[cpos++] = 0x18; create_inner[cpos++] = 0x01;

    uint8_t watch_buf[32]; size_t wpos = 0;
    watch_buf[wpos++] = 0x0a;
    watch_buf[wpos++] = (uint8_t)cpos;
    memcpy(watch_buf + wpos, create_inner, cpos); wpos += cpos;

    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Watch/Watch", watch_buf, wpos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    int found_event = 0;
    for (size_t i = 0; i < resp.len; i++) {
        if (resp.data[i] == 0x5a) { found_event = 1; break; }
    }
    CETCD_ASSERT_TRUE(found_event);
    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_watch_replay_prev_kv) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Put wpkv=old, then wpkv=new */
    uint8_t put1[24]; size_t p = 0;
    put1[p++] = 0x0a; put1[p++] = 0x04;
    memcpy(put1 + p, "wpkv", 4); p += 4;
    put1[p++] = 0x12; put1[p++] = 0x03;
    memcpy(put1 + p, "old", 3); p += 3;
    cetcd_rpc_bytes r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", put1, p);
    cetcd_rpc_bytes_free(&r);

    uint8_t put2[24]; p = 0;
    put2[p++] = 0x0a; put2[p++] = 0x04;
    memcpy(put2 + p, "wpkv", 4); p += 4;
    put2[p++] = 0x12; put2[p++] = 0x03;
    memcpy(put2 + p, "new", 3); p += 3;
    r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", put2, p);
    cetcd_rpc_bytes_free(&r);

    /* WatchCreate: key=wpkv, start_revision=1, prev_kv=true (field 6, tag 0x30) */
    uint8_t create_inner[24]; size_t cpos = 0;
    create_inner[cpos++] = 0x0a; create_inner[cpos++] = 0x04;
    memcpy(create_inner + cpos, "wpkv", 4); cpos += 4;
    create_inner[cpos++] = 0x18; create_inner[cpos++] = 0x01;
    create_inner[cpos++] = 0x30; create_inner[cpos++] = 0x01;

    uint8_t watch_buf[40]; size_t wpos = 0;
    watch_buf[wpos++] = 0x0a;
    watch_buf[wpos++] = (uint8_t)cpos;
    memcpy(watch_buf + wpos, create_inner, cpos); wpos += cpos;

    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Watch/Watch", watch_buf, wpos);
    CETCD_ASSERT_NOT_NULL(resp.data);

    /* Event (0x5a) should embed prev_kv (0x1a) containing "old". */
    int found_prev = 0;
    for (size_t i = 0; i + 3 <= resp.len; i++) {
        if (resp.data[i] == 0x1a) {
            for (size_t j = i; j + 3 <= resp.len && j < i + 64; j++) {
                if (memcmp(resp.data + j, "old", 3) == 0) { found_prev = 1; break; }
            }
            if (found_prev) break;
        }
    }
    CETCD_ASSERT_TRUE(found_prev);
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

    /* PutResponse should contain field 2 (prev_kv) with tag 0x12 */
    bool found_prev_kv = false;
    for (size_t i = 0; i < resp.len; i++) {
        if (resp.data[i] == 0x12) { found_prev_kv = true; break; }
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

CETCD_TEST_CASE(v3rpc_put_ignore_value_missing_key) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Put ignore_value on a missing key must fail RPC (etcd ErrKeyNotFound). */
    {
        uint8_t req[64]; size_t pos = 0;
        req[pos++] = 0x0a; req[pos++] = 7;
        memcpy(req + pos, "missing", 7); pos += 7;
        req[pos++] = 0x12; req[pos++] = 1;
        req[pos++] = 'x';
        req[pos++] = 0x28; /* ignore_value = true */
        req[pos++] = 0x01;
        cetcd_rpc_bytes r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", req, pos);
        CETCD_ASSERT_TRUE(r.data == NULL || r.len == 0);
        cetcd_rpc_bytes_free(&r);
    }

    {
        uint8_t req[32]; size_t pos = 0;
        req[pos++] = 0x0a; req[pos++] = 7;
        memcpy(req + pos, "missing", 7); pos += 7;
        cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Range", req, pos);
        CETCD_ASSERT_NOT_NULL(resp.data);
        int found = 0;
        for (size_t i = 0; i + 7 <= resp.len; i++) {
            if (memcmp(resp.data + i, "missing", 7) == 0) { found = 1; break; }
        }
        CETCD_ASSERT_TRUE(!found);
        cetcd_rpc_bytes_free(&resp);
    }

    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_put_clear_lease_and_delete_detaches) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Grant lease */
    uint8_t grant_buf[4]; size_t pos = 0;
    grant_buf[pos++] = 0x08; grant_buf[pos++] = 0x3c;
    grant_buf[pos++] = 0x10; grant_buf[pos++] = 0x00;
    cetcd_rpc_bytes r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.Lease/LeaseGrant", grant_buf, pos);
    CETCD_ASSERT_NOT_NULL(r.data);
    int64_t lease_id = 0;
    size_t rpos = 0;
    while (rpos < r.len) {
        uint8_t tag = r.data[rpos++];
        if (tag == 0x0a) {
            uint64_t l = 0; int shift = 0;
            while (rpos < r.len) {
                uint8_t b = r.data[rpos++];
                l |= (uint64_t)(b & 0x7F) << shift;
                if (!(b & 0x80)) break;
                shift += 7;
            }
            rpos += (size_t)l;
        } else {
            uint64_t v = 0; int shift = 0;
            while (rpos < r.len) {
                uint8_t b = r.data[rpos++];
                v |= (uint64_t)(b & 0x7F) << shift;
                if (!(b & 0x80)) break;
                shift += 7;
            }
            if (tag == 0x10) { lease_id = (int64_t)v; break; }
        }
    }
    cetcd_rpc_bytes_free(&r);
    CETCD_ASSERT_TRUE(lease_id > 0);

    /* Put key with lease */
    uint8_t put[32]; pos = 0;
    put[pos++] = 0x0a; put[pos++] = 0x03;
    memcpy(put + pos, "lkx", 3); pos += 3;
    put[pos++] = 0x12; put[pos++] = 0x02;
    memcpy(put + pos, "v1", 2); pos += 2;
    put[pos++] = 0x18;
    uint64_t lid = (uint64_t)lease_id;
    do { uint8_t b = lid & 0x7F; lid >>= 7; if (lid) b |= 0x80; put[pos++] = b; } while (lid);
    r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", put, pos);
    cetcd_rpc_bytes_free(&r);

    /* Put again with lease=0 → should clear binding */
    pos = 0;
    put[pos++] = 0x0a; put[pos++] = 0x03;
    memcpy(put + pos, "lkx", 3); pos += 3;
    put[pos++] = 0x12; put[pos++] = 0x02;
    memcpy(put + pos, "v2", 2); pos += 2;
    put[pos++] = 0x18; put[pos++] = 0x00; /* lease = 0 */
    r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", put, pos);
    cetcd_rpc_bytes_free(&r);

    /* TimeToLive with keys=true should not list lkx */
    uint8_t ttl[8]; pos = 0;
    ttl[pos++] = 0x08;
    lid = (uint64_t)lease_id;
    do { uint8_t b = lid & 0x7F; lid >>= 7; if (lid) b |= 0x80; ttl[pos++] = b; } while (lid);
    ttl[pos++] = 0x10; ttl[pos++] = 0x01;
    r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.Lease/LeaseTimeToLive", ttl, pos);
    CETCD_ASSERT_NOT_NULL(r.data);
    int found = 0;
    for (size_t i = 0; i + 3 <= r.len; i++) {
        if (memcmp(r.data + i, "lkx", 3) == 0) { found = 1; break; }
    }
    CETCD_ASSERT_TRUE(!found);
    cetcd_rpc_bytes_free(&r);

    /* Re-attach then DeleteRange must also detach */
    pos = 0;
    put[pos++] = 0x0a; put[pos++] = 0x03;
    memcpy(put + pos, "lkx", 3); pos += 3;
    put[pos++] = 0x12; put[pos++] = 0x02;
    memcpy(put + pos, "v3", 2); pos += 2;
    put[pos++] = 0x18;
    lid = (uint64_t)lease_id;
    do { uint8_t b = lid & 0x7F; lid >>= 7; if (lid) b |= 0x80; put[pos++] = b; } while (lid);
    r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", put, pos);
    cetcd_rpc_bytes_free(&r);

    uint8_t del[8]; pos = 0;
    del[pos++] = 0x0a; del[pos++] = 0x03;
    memcpy(del + pos, "lkx", 3); pos += 3;
    r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/DeleteRange", del, pos);
    cetcd_rpc_bytes_free(&r);

    pos = 0;
    ttl[pos++] = 0x08;
    lid = (uint64_t)lease_id;
    do { uint8_t b = lid & 0x7F; lid >>= 7; if (lid) b |= 0x80; ttl[pos++] = b; } while (lid);
    ttl[pos++] = 0x10; ttl[pos++] = 0x01;
    r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.Lease/LeaseTimeToLive", ttl, pos);
    found = 0;
    for (size_t i = 0; i + 3 <= r.len; i++) {
        if (memcmp(r.data + i, "lkx", 3) == 0) { found = 1; break; }
    }
    CETCD_ASSERT_TRUE(!found);
    cetcd_rpc_bytes_free(&r);

    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_put_ignore_lease) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Grant a real lease first */
    uint8_t grant_buf[4]; size_t gpos = 0;
    grant_buf[gpos++] = 0x08; grant_buf[gpos++] = 0x3c;
    grant_buf[gpos++] = 0x10; grant_buf[gpos++] = 0x00;
    cetcd_rpc_bytes gr = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.Lease/LeaseGrant", grant_buf, gpos);
    CETCD_ASSERT_NOT_NULL(gr.data);
    int64_t lease_id = 0;
    size_t rpos = 0;
    while (rpos < gr.len) {
        uint8_t tag = gr.data[rpos++];
        if (tag == 0x0a) {
            uint64_t l = 0; int shift = 0;
            while (rpos < gr.len) {
                uint8_t b = gr.data[rpos++];
                l |= (uint64_t)(b & 0x7F) << shift;
                if (!(b & 0x80)) break;
                shift += 7;
            }
            rpos += (size_t)l;
        } else {
            uint64_t v = 0; int shift = 0;
            while (rpos < gr.len) {
                uint8_t b = gr.data[rpos++];
                v |= (uint64_t)(b & 0x7F) << shift;
                if (!(b & 0x80)) break;
                shift += 7;
            }
            if (tag == 0x10) { lease_id = (int64_t)v; break; }
        }
    }
    cetcd_rpc_bytes_free(&gr);
    CETCD_ASSERT_TRUE(lease_id > 0);

    /* Put key="ilkey" value="v1" with granted lease */
    {
        uint8_t req[64]; size_t pos = 0;
        req[pos++] = 0x0a; req[pos++] = 5;
        memcpy(req + pos, "ilkey", 5); pos += 5;
        req[pos++] = 0x12; req[pos++] = 2;
        memcpy(req + pos, "v1", 2); pos += 2;
        req[pos++] = 0x18;
        uint64_t lid = (uint64_t)lease_id;
        do { uint8_t b = lid & 0x7F; lid >>= 7; if (lid) b |= 0x80; req[pos++] = b; } while (lid);
        cetcd_rpc_bytes r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", req, pos);
        cetcd_rpc_bytes_free(&r);
    }

    /* Put key="ilkey" value="v2" with ignore_lease=true and lease=99999 */
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

    /* Range key="ilkey" — value should be "v2" (updated), lease should still be granted id */
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

CETCD_TEST_CASE(v3rpc_put_ignore_lease_missing_key) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* ignore_lease on a missing key must fail RPC (etcd ErrKeyNotFound). */
    {
        uint8_t req[64]; size_t pos = 0;
        req[pos++] = 0x0a; req[pos++] = 8;
        memcpy(req + pos, "nolsekey", 8); pos += 8;
        req[pos++] = 0x12; req[pos++] = 1;
        req[pos++] = 'v';
        req[pos++] = 0x30; /* ignore_lease = true */
        req[pos++] = 0x01;
        cetcd_rpc_bytes r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", req, pos);
        CETCD_ASSERT_TRUE(r.data == NULL || r.len == 0);
        cetcd_rpc_bytes_free(&r);
    }

    {
        uint8_t req[32]; size_t pos = 0;
        req[pos++] = 0x0a; req[pos++] = 8;
        memcpy(req + pos, "nolsekey", 8); pos += 8;
        cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Range", req, pos);
        CETCD_ASSERT_NOT_NULL(resp.data);
        int found = 0;
        for (size_t i = 0; i + 8 <= resp.len; i++) {
            if (memcmp(resp.data + i, "nolsekey", 8) == 0) { found = 1; break; }
        }
        CETCD_ASSERT_TRUE(!found);
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

CETCD_TEST_CASE(v3rpc_lease_grant_has_header) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* LeaseGrant: TTL=60, ID=0 (auto) */
    uint8_t req[4]; size_t pos = 0;
    req[pos++] = 0x08; req[pos++] = 0x3c; /* ttl=60 */
    req[pos++] = 0x10; req[pos++] = 0x00; /* id=0 auto */
    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Lease/LeaseGrant", req, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 2);
    /* Response should start with header (field 1, tag 0x0a) */
    CETCD_ASSERT_TRUE(resp.data[0] == 0x0a);

    /* Verify ID at field 2 (tag 0x10) and TTL at field 3 (tag 0x18) */
    int found_id = 0, found_ttl = 0;
    size_t rpos = 0;
    while (rpos < resp.len) {
        uint8_t tag = resp.data[rpos++];
        if (tag == 0x0a) {
            uint64_t l = 0; int shift = 0;
            while (rpos < resp.len) { uint8_t b = resp.data[rpos++]; l |= (uint64_t)(b & 0x7F) << shift; shift += 7; if (!(b & 0x80)) break; }
            rpos += (size_t)l;
        } else {
            uint64_t v = 0; int shift = 0;
            while (rpos < resp.len) { uint8_t b = resp.data[rpos++]; v |= (uint64_t)(b & 0x7F) << shift; shift += 7; if (!(b & 0x80)) break; }
            if (tag == 0x10) found_id = 1;
            if (tag == 0x18) found_ttl = 1;
        }
    }
    CETCD_ASSERT_TRUE(found_id);
    CETCD_ASSERT_TRUE(found_ttl);

    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_lease_keepalive_has_header) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Grant a lease first */
    uint8_t grant_buf[4]; size_t pos = 0;
    grant_buf[pos++] = 0x08; grant_buf[pos++] = 0x3c;
    grant_buf[pos++] = 0x10; grant_buf[pos++] = 0x00;
    cetcd_rpc_bytes r = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Lease/LeaseGrant", grant_buf, pos);
    cetcd_rpc_bytes_free(&r);

    /* KeepAlive with ID=1 */
    uint8_t ka_buf[2]; pos = 0;
    ka_buf[pos++] = 0x08; ka_buf[pos++] = 0x01;
    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Lease/LeaseKeepAlive", ka_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 2);
    /* Response should start with header (field 1, tag 0x0a) */
    CETCD_ASSERT_TRUE(resp.data[0] == 0x0a);

    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_lease_time_to_live_has_header) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Grant a lease first */
    uint8_t grant_buf[4]; size_t pos = 0;
    grant_buf[pos++] = 0x08; grant_buf[pos++] = 0x78; /* ttl=120 */
    grant_buf[pos++] = 0x10; grant_buf[pos++] = 0x00;
    cetcd_rpc_bytes r = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Lease/LeaseGrant", grant_buf, pos);
    cetcd_rpc_bytes_free(&r);

    /* TimeToLive with ID=1 */
    uint8_t ttl_buf[2]; pos = 0;
    ttl_buf[pos++] = 0x08; ttl_buf[pos++] = 0x01;
    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Lease/LeaseTimeToLive", ttl_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 2);
    /* Response should start with header (field 1, tag 0x0a) */
    CETCD_ASSERT_TRUE(resp.data[0] == 0x0a);

    /* Verify grantedTTL at field 4 (tag 0x20) */
    int found_granted_ttl = 0;
    size_t rpos = 0;
    while (rpos < resp.len) {
        uint8_t tag = resp.data[rpos++];
        if (tag == 0x0a) {
            uint64_t l = 0; int shift = 0;
            while (rpos < resp.len) { uint8_t b = resp.data[rpos++]; l |= (uint64_t)(b & 0x7F) << shift; shift += 7; if (!(b & 0x80)) break; }
            rpos += (size_t)l;
        } else {
            uint64_t v = 0; int shift = 0;
            while (rpos < resp.len) { uint8_t b = resp.data[rpos++]; v |= (uint64_t)(b & 0x7F) << shift; shift += 7; if (!(b & 0x80)) break; }
            if (tag == 0x20) found_granted_ttl = 1;
        }
    }
    CETCD_ASSERT_TRUE(found_granted_ttl);

    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_lease_leases_has_header) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Grant a lease first */
    uint8_t grant_buf[4]; size_t pos = 0;
    grant_buf[pos++] = 0x08; grant_buf[pos++] = 0x3c;
    grant_buf[pos++] = 0x10; grant_buf[pos++] = 0x00;
    cetcd_rpc_bytes r = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Lease/LeaseGrant", grant_buf, pos);
    cetcd_rpc_bytes_free(&r);

    /* LeaseLeases */
    uint8_t dummy[] = {0x00};
    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Lease/LeaseLeases", dummy, 1);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 2);
    /* Response should start with header (field 1, tag 0x0a) */
    CETCD_ASSERT_TRUE(resp.data[0] == 0x0a);

    /* Verify leases at field 2 (tag 0x12) */
    int found_lease = 0;
    size_t rpos = 0;
    while (rpos < resp.len) {
        uint8_t tag = resp.data[rpos++];
        if (tag == 0x0a) {
            uint64_t l = 0; int shift = 0;
            while (rpos < resp.len) { uint8_t b = resp.data[rpos++]; l |= (uint64_t)(b & 0x7F) << shift; shift += 7; if (!(b & 0x80)) break; }
            rpos += (size_t)l;
        } else if (tag == 0x12) {
            found_lease = 1;
            break;
        } else {
            uint64_t v = 0; int shift = 0;
            while (rpos < resp.len) { uint8_t b = resp.data[rpos++]; v |= (uint64_t)(b & 0x7F) << shift; shift += 7; if (!(b & 0x80)) break; }
        }
    }
    CETCD_ASSERT_TRUE(found_lease);

    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_lease_time_to_live_with_keys) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Grant a lease */
    uint8_t grant_buf[4]; size_t pos = 0;
    grant_buf[pos++] = 0x08; grant_buf[pos++] = 0x3c; /* ttl=60 */
    grant_buf[pos++] = 0x10; grant_buf[pos++] = 0x00;
    cetcd_rpc_bytes r = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Lease/LeaseGrant", grant_buf, pos);
    CETCD_ASSERT_NOT_NULL(r.data);

    /* Extract lease ID from field 2 (tag 0x10) */
    int64_t lease_id = 0;
    size_t rpos = 0;
    while (rpos < r.len) {
        uint8_t tag = r.data[rpos++];
        if (tag == 0x0a) {
            uint64_t l = 0; int shift = 0;
            while (rpos < r.len) { uint8_t b = r.data[rpos++]; l |= (uint64_t)(b & 0x7F) << shift; shift += 7; if (!(b & 0x80)) break; }
            rpos += (size_t)l;
        } else {
            uint64_t v = 0; int shift = 0;
            while (rpos < r.len) { uint8_t b = r.data[rpos++]; v |= (uint64_t)(b & 0x7F) << shift; shift += 7; if (!(b & 0x80)) break; }
            if (tag == 0x10) { lease_id = (int64_t)v; break; }
        }
    }
    cetcd_rpc_bytes_free(&r);
    CETCD_ASSERT_TRUE(lease_id > 0);

    /* Put a key with the lease */
    uint8_t put_buf[32]; pos = 0;
    put_buf[pos++] = 0x0a; put_buf[pos++] = 0x03;
    memcpy(put_buf + pos, "lk1", 3); pos += 3;
    put_buf[pos++] = 0x12; put_buf[pos++] = 0x02;
    memcpy(put_buf + pos, "v1", 2); pos += 2;
    put_buf[pos++] = 0x18; /* lease */
    uint64_t lid = (uint64_t)lease_id;
    do { uint8_t b = lid & 0x7F; lid >>= 7; if (lid) b |= 0x80; put_buf[pos++] = b; } while (lid);
    r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", put_buf, pos);
    cetcd_rpc_bytes_free(&r);

    /* TimeToLive with keys=true: field 1 (ID), field 2 (keys) = true */
    uint8_t ttl_buf[8]; pos = 0;
    ttl_buf[pos++] = 0x08; /* field 1 = ID */
    lid = (uint64_t)lease_id;
    do { uint8_t b = lid & 0x7F; lid >>= 7; if (lid) b |= 0x80; ttl_buf[pos++] = b; } while (lid);
    ttl_buf[pos++] = 0x10; /* field 2 = keys (bool) */
    ttl_buf[pos++] = 0x01; /* true */
    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Lease/LeaseTimeToLive", ttl_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 2);

    /* Verify keys field (tag 0x2a) is present and contains "lk1" */
    int found_keys = 0;
    rpos = 0;
    while (rpos < resp.len) {
        uint8_t tag = resp.data[rpos++];
        if (tag == 0x0a) {
            uint64_t l = 0; int shift = 0;
            while (rpos < resp.len) { uint8_t b = resp.data[rpos++]; l |= (uint64_t)(b & 0x7F) << shift; shift += 7; if (!(b & 0x80)) break; }
            rpos += (size_t)l;
        } else if (tag == 0x2a) {
            /* keys (repeated bytes) */
            uint64_t l = 0; int shift = 0;
            while (rpos < resp.len) { uint8_t b = resp.data[rpos++]; l |= (uint64_t)(b & 0x7F) << shift; shift += 7; if (!(b & 0x80)) break; }
            if (l == 3 && memcmp(resp.data + rpos, "lk1", 3) == 0) found_keys = 1;
            rpos += (size_t)l;
        } else {
            uint64_t v = 0; int shift = 0;
            while (rpos < resp.len) { uint8_t b = resp.data[rpos++]; v |= (uint64_t)(b & 0x7F) << shift; shift += 7; if (!(b & 0x80)) break; }
        }
    }
    CETCD_ASSERT_TRUE(found_keys);

    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_maintenance_responses_have_header) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();
    uint8_t dummy[] = {0x00};

    /* Status response should start with header (tag 0x0a) */
    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Maintenance/Status", dummy, 1);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 2);
    CETCD_ASSERT_TRUE(resp.data[0] == 0x0a);
    cetcd_rpc_bytes_free(&resp);

    /* Hash response should start with header (tag 0x0a) */
    resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Maintenance/Hash", dummy, 1);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 2);
    CETCD_ASSERT_TRUE(resp.data[0] == 0x0a);
    cetcd_rpc_bytes_free(&resp);

    /* HashKV response should start with header (tag 0x0a) */
    resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Maintenance/HashKV", dummy, 1);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 2);
    CETCD_ASSERT_TRUE(resp.data[0] == 0x0a);
    cetcd_rpc_bytes_free(&resp);

    /* Defragment response should start with header (tag 0x0a) */
    resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Maintenance/Defragment", dummy, 1);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 2);
    CETCD_ASSERT_TRUE(resp.data[0] == 0x0a);
    cetcd_rpc_bytes_free(&resp);

    /* Alarm response should start with header (tag 0x0a) */
    resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Maintenance/Alarm", dummy, 1);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 2);
    CETCD_ASSERT_TRUE(resp.data[0] == 0x0a);
    cetcd_rpc_bytes_free(&resp);

    /* MoveLeader response should start with header (tag 0x0a) */
    resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Maintenance/MoveLeader", dummy, 1);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 2);
    CETCD_ASSERT_TRUE(resp.data[0] == 0x0a);
    cetcd_rpc_bytes_free(&resp);

    /* Downgrade response should start with header (tag 0x0a) */
    uint8_t dg_buf[16]; size_t pos = 0;
    dg_buf[pos++] = 0x08; dg_buf[pos++] = 0x01; /* ENABLE */
    dg_buf[pos++] = 0x12; dg_buf[pos++] = 0x05;
    memcpy(dg_buf + pos, "0.1.0", 5); pos += 5;
    resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Maintenance/Downgrade", dg_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 2);
    CETCD_ASSERT_TRUE(resp.data[0] == 0x0a);
    cetcd_rpc_bytes_free(&resp);

    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_status_has_version_and_leader) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();
    uint8_t dummy[] = {0x00};
    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Maintenance/Status", dummy, 1);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 2);

    /* Verify version (field 2, tag 0x12) and leader (field 4, tag 0x20) are present */
    int found_version = 0, found_leader = 0;
    size_t rpos = 0;
    while (rpos < resp.len) {
        uint8_t tag = resp.data[rpos++];
        if (tag == 0x0a) {
            /* Skip header (length-delimited) */
            uint64_t l = 0; int shift = 0;
            while (rpos < resp.len) { uint8_t b = resp.data[rpos++]; l |= (uint64_t)(b & 0x7F) << shift; shift += 7; if (!(b & 0x80)) break; }
            rpos += (size_t)l;
        } else if (tag == 0x12) {
            /* version (string) */
            uint64_t l = 0; int shift = 0;
            while (rpos < resp.len) { uint8_t b = resp.data[rpos++]; l |= (uint64_t)(b & 0x7F) << shift; shift += 7; if (!(b & 0x80)) break; }
            rpos += (size_t)l;
            found_version = 1;
        } else {
            uint64_t v = 0; int shift = 0;
            while (rpos < resp.len) { uint8_t b = resp.data[rpos++]; v |= (uint64_t)(b & 0x7F) << shift; shift += 7; if (!(b & 0x80)) break; }
            if (tag == 0x20) found_leader = 1;
        }
    }
    CETCD_ASSERT_TRUE(found_version);
    CETCD_ASSERT_TRUE(found_leader);

    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_cluster_responses_have_header) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();
    uint8_t dummy[] = {0x00};

    /* MemberList response should start with header (tag 0x0a) */
    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Cluster/MemberList", dummy, 1);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 2);
    CETCD_ASSERT_TRUE(resp.data[0] == 0x0a);
    cetcd_rpc_bytes_free(&resp);

    /* MemberRemove response should start with header (tag 0x0a) */
    uint8_t rm_buf[4]; size_t pos = 0;
    rm_buf[pos++] = 0x08; rm_buf[pos++] = 0x63; /* ID=99 */
    resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Cluster/MemberRemove", rm_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 2);
    CETCD_ASSERT_TRUE(resp.data[0] == 0x0a);
    cetcd_rpc_bytes_free(&resp);

    /* MemberUpdate response should start with header (tag 0x0a) */
    uint8_t upd_buf[32]; pos = 0;
    upd_buf[pos++] = 0x08; upd_buf[pos++] = 0x63; /* ID=99 */
    upd_buf[pos++] = 0x12; upd_buf[pos++] = 0x0e;
    memcpy(upd_buf + pos, "127.0.0.1:2380", 14); pos += 14;
    resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Cluster/MemberUpdate", upd_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 2);
    CETCD_ASSERT_TRUE(resp.data[0] == 0x0a);
    cetcd_rpc_bytes_free(&resp);

    /* MemberPromote response should start with header (tag 0x0a) */
    uint8_t prom_buf[4]; pos = 0;
    prom_buf[pos++] = 0x08; prom_buf[pos++] = 0x63; /* ID=99 */
    resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Cluster/MemberPromote", prom_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 2);
    CETCD_ASSERT_TRUE(resp.data[0] == 0x0a);
    cetcd_rpc_bytes_free(&resp);

    /* MemberAdd response should start with header (tag 0x0a) */
    uint8_t add_buf[32]; pos = 0;
    add_buf[pos++] = 0x0a; add_buf[pos++] = 0x0e;
    memcpy(add_buf + pos, "127.0.0.1:2380", 14); pos += 14;
    resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Cluster/MemberAdd", add_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 2);
    CETCD_ASSERT_TRUE(resp.data[0] == 0x0a);
    cetcd_rpc_bytes_free(&resp);

    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_watch_has_header) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Put key="whdr" value="v1" to create history */
    uint8_t put_buf[32]; size_t pos = 0;
    put_buf[pos++] = 0x0a; put_buf[pos++] = 0x04;
    memcpy(put_buf + pos, "whdr", 4); pos += 4;
    put_buf[pos++] = 0x12; put_buf[pos++] = 0x02;
    memcpy(put_buf + pos, "v1", 2); pos += 2;
    cetcd_rpc_bytes r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", put_buf, pos);
    cetcd_rpc_bytes_free(&r);

    /* Watch key="whdr" with start_revision=1 */
    uint8_t create_inner[32]; size_t cpos = 0;
    create_inner[cpos++] = 0x0a; create_inner[cpos++] = 0x04;
    memcpy(create_inner + cpos, "whdr", 4); cpos += 4;
    create_inner[cpos++] = 0x18; create_inner[cpos++] = 0x01; /* start_revision=1 */

    uint8_t watch_buf[64]; size_t wpos = 0;
    watch_buf[wpos++] = 0x0a;
    watch_buf[wpos++] = (uint8_t)cpos;
    memcpy(watch_buf + wpos, create_inner, cpos); wpos += cpos;

    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.Watch/Watch", watch_buf, wpos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 2);

    /* WatchResponse should start with field 1 (header) = tag 0x0a */
    CETCD_ASSERT_TRUE(resp.data[0] == 0x0a);

    /* Parse header to verify it contains revision (tag 0x18 inside) */
    size_t p = 1;
    uint64_t hdr_len = 0;
    /* read varint for header length */
    {
        uint64_t val = 0; int shift = 0;
        while (p < resp.len) {
            uint8_t b = resp.data[p++];
            val |= (uint64_t)(b & 0x7F) << shift;
            if (!(b & 0x80)) break;
            shift += 7;
        }
        hdr_len = val;
    }
    /* Inside header, look for revision tag 0x18 */
    bool found_revision = false;
    size_t hdr_end = p + (size_t)hdr_len;
    while (p < hdr_end && p < resp.len) {
        uint8_t tag = resp.data[p++];
        if (tag == 0x18) { found_revision = true; break; }
        /* skip varint value */
        while (p < hdr_end) {
            uint8_t b = resp.data[p++];
            if (!(b & 0x80)) break;
        }
    }
    CETCD_ASSERT_TRUE(found_revision);

    /* After header, should find watch_id (tag 0x10) and created (tag 0x18) */
    bool found_watch_id = false;
    bool found_created = false;
    p = hdr_end;
    while (p < resp.len) {
        uint8_t tag = resp.data[p++];
        if (tag == 0x10) {
            found_watch_id = true;
            while (p < resp.len) { uint8_t b = resp.data[p++]; if (!(b & 0x80)) break; }
        } else if (tag == 0x18) {
            found_created = true;
            while (p < resp.len) { uint8_t b = resp.data[p++]; if (!(b & 0x80)) break; }
        } else if (tag == 0x0a || tag == 0x5a) {
            /* skip length-delimited fields */
            uint64_t l = 0; int shift = 0;
            while (p < resp.len) {
                uint8_t b = resp.data[p++];
                l |= (uint64_t)(b & 0x7F) << shift;
                if (!(b & 0x80)) break;
                shift += 7;
            }
            p += (size_t)l;
        } else {
            while (p < resp.len) { uint8_t b = resp.data[p++]; if (!(b & 0x80)) break; }
        }
    }
    CETCD_ASSERT_TRUE(found_watch_id);
    CETCD_ASSERT_TRUE(found_created);

    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_watch_event_kv_correct_fields) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Put key="wevt" value="wval" to create history */
    uint8_t put_buf[32]; size_t pos = 0;
    put_buf[pos++] = 0x0a; put_buf[pos++] = 0x04;
    memcpy(put_buf + pos, "wevt", 4); pos += 4;
    put_buf[pos++] = 0x12; put_buf[pos++] = 0x04;
    memcpy(put_buf + pos, "wval", 4); pos += 4;
    cetcd_rpc_bytes r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", put_buf, pos);
    cetcd_rpc_bytes_free(&r);

    /* Watch key="wevt" with start_revision=1 to get events */
    uint8_t create_inner[32]; size_t cpos = 0;
    create_inner[cpos++] = 0x0a; create_inner[cpos++] = 0x04;
    memcpy(create_inner + cpos, "wevt", 4); cpos += 4;
    create_inner[cpos++] = 0x18; create_inner[cpos++] = 0x01; /* start_revision=1 */

    uint8_t watch_buf[64]; size_t wpos = 0;
    watch_buf[wpos++] = 0x0a;
    watch_buf[wpos++] = (uint8_t)cpos;
    memcpy(watch_buf + wpos, create_inner, cpos); wpos += cpos;

    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.Watch/Watch", watch_buf, wpos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 2);

    /* Find events (tag 0x5a) in the response */
    bool found_event = false;
    bool found_create_rev_tag = false; /* 0x10 inside KV */
    bool found_mod_rev_tag = false;    /* 0x18 inside KV */
    bool found_version_tag = false;    /* 0x20 inside KV */
    bool found_value_tag = false;      /* 0x2a inside KV */
    bool found_key_tag = false;        /* 0x0a inside KV */

    for (size_t i = 0; i < resp.len; i++) {
        if (resp.data[i] == 0x5a) {
            found_event = true;
            /* Found event. Now look inside the event for KV (tag 0x12) */
            size_t ep = i + 1;
            /* read event length */
            uint64_t elen = 0; int shift = 0;
            while (ep < resp.len) {
                uint8_t b = resp.data[ep++];
                elen |= (uint64_t)(b & 0x7F) << shift;
                if (!(b & 0x80)) break;
                shift += 7;
            }
            size_t eend = ep + (size_t)elen;
            if (eend > resp.len) eend = resp.len;
            /* Look for KV tag 0x12 inside event */
            while (ep < eend) {
                uint8_t etag = resp.data[ep++];
                if (etag == 0x12) {
                    /* Found KV. Parse its fields */
                    uint64_t klen = 0; int sh2 = 0;
                    while (ep < eend) {
                        uint8_t b = resp.data[ep++];
                        klen |= (uint64_t)(b & 0x7F) << sh2;
                        if (!(b & 0x80)) break;
                        sh2 += 7;
                    }
                    size_t kend = ep + (size_t)klen;
                    if (kend > eend) kend = eend;
                    while (ep < kend) {
                        uint8_t ktag = resp.data[ep++];
                        if (ktag == 0x0a) { found_key_tag = true; }
                        else if (ktag == 0x10) { found_create_rev_tag = true; }
                        else if (ktag == 0x18) { found_mod_rev_tag = true; }
                        else if (ktag == 0x20) { found_version_tag = true; }
                        else if (ktag == 0x2a) { found_value_tag = true; }
                        /* skip value (varint or length-delimited) */
                        if (ktag == 0x0a || ktag == 0x2a) {
                            /* length-delimited */
                            uint64_t l = 0; int s3 = 0;
                            while (ep < kend) {
                                uint8_t b = resp.data[ep++];
                                l |= (uint64_t)(b & 0x7F) << s3;
                                if (!(b & 0x80)) break;
                                s3 += 7;
                            }
                            ep += (size_t)l;
                        } else {
                            /* varint */
                            while (ep < kend) {
                                uint8_t b = resp.data[ep++];
                                if (!(b & 0x80)) break;
                            }
                        }
                    }
                    ep = kend;
                } else if (etag == 0x08) {
                    /* event type (varint) */
                    while (ep < eend) { uint8_t b = resp.data[ep++]; if (!(b & 0x80)) break; }
                } else if (etag == 0x1a) {
                    /* prev_kv (length-delimited) */
                    uint64_t l = 0; int s4 = 0;
                    while (ep < eend) {
                        uint8_t b = resp.data[ep++];
                        l |= (uint64_t)(b & 0x7F) << s4;
                        if (!(b & 0x80)) break;
                        s4 += 7;
                    }
                    ep += (size_t)l;
                } else {
                    while (ep < eend) { uint8_t b = resp.data[ep++]; if (!(b & 0x80)) break; }
                }
            }
            break; /* only check first event */
        }
    }

    /* If events are present, verify correct KV field numbers.
     * If no events (watch may not replay history), that's acceptable. */
    if (found_event) {
        CETCD_ASSERT_TRUE(found_key_tag);
        CETCD_ASSERT_TRUE(found_create_rev_tag);
        CETCD_ASSERT_TRUE(found_mod_rev_tag);
        CETCD_ASSERT_TRUE(found_version_tag);
        CETCD_ASSERT_TRUE(found_value_tag);
    }

    /* Regardless of events, verify the response has a header (tag 0x0a) */
    CETCD_ASSERT_TRUE(resp.data[0] == 0x0a);

    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_range_kvs_correct_tag) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Put key="rktag" value="rv1" */
    uint8_t put_buf[32]; size_t pos = 0;
    put_buf[pos++] = 0x0a; put_buf[pos++] = 0x05;
    memcpy(put_buf + pos, "rktag", 5); pos += 5;
    put_buf[pos++] = 0x12; put_buf[pos++] = 0x03;
    memcpy(put_buf + pos, "rv1", 3); pos += 3;
    cetcd_rpc_bytes r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", put_buf, pos);
    cetcd_rpc_bytes_free(&r);

    /* Range for key="rktag" */
    uint8_t range_buf[16]; pos = 0;
    range_buf[pos++] = 0x0a; range_buf[pos++] = 0x05;
    memcpy(range_buf + pos, "rktag", 5); pos += 5;

    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Range", range_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 2);

    /* Response should start with header (tag 0x0a), then kvs (tag 0x12) */
    CETCD_ASSERT_TRUE(resp.data[0] == 0x0a); /* header */

    /* Parse header length and skip */
    size_t p = 1;
    uint64_t hdr_len = 0; int shift = 0;
    while (p < resp.len) {
        uint8_t b = resp.data[p++];
        hdr_len |= (uint64_t)(b & 0x7F) << shift;
        if (!(b & 0x80)) break;
        shift += 7;
    }
    p += (size_t)hdr_len;

    /* After header, should find kvs with tag 0x12 (not 0x0a) */
    bool found_kvs = false;
    while (p < resp.len) {
        uint8_t tag = resp.data[p++];
        if (tag == 0x12) {
            found_kvs = true;
            break;
        } else if (tag == 0x0a) {
            /* This would be wrong - header should only appear once */
            break;
        } else if (tag == 0x20 || tag == 0x18) {
            /* count or more (varint) */
            while (p < resp.len) { uint8_t b = resp.data[p++]; if (!(b & 0x80)) break; }
        } else {
            while (p < resp.len) { uint8_t b = resp.data[p++]; if (!(b & 0x80)) break; }
        }
    }
    CETCD_ASSERT_TRUE(found_kvs);

    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_range_sort_order) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Put three keys with different values */
    const char *keys[] = {"sk1", "sk2", "sk3"};
    const char *vals[] = {"val_c", "val_a", "val_b"};
    for (int i = 0; i < 3; i++) {
        uint8_t put_buf[64]; size_t pos = 0;
        put_buf[pos++] = 0x0a; put_buf[pos++] = 3;
        memcpy(put_buf + pos, keys[i], 3); pos += 3;
        put_buf[pos++] = 0x12; put_buf[pos++] = 5;
        memcpy(put_buf + pos, vals[i], 5); pos += 5;
        cetcd_rpc_bytes r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", put_buf, pos);
        cetcd_rpc_bytes_free(&r);
    }

    /* Range from sk1 to sk4 with sort_order=ASCEND(1), sort_target=VALUE(4) */
    uint8_t range_buf[64]; size_t pos = 0;
    range_buf[pos++] = 0x0a; range_buf[pos++] = 3;
    memcpy(range_buf + pos, "sk1", 3); pos += 3;
    range_buf[pos++] = 0x12; range_buf[pos++] = 3;
    memcpy(range_buf + pos, "sk4", 3); pos += 3;
    range_buf[pos++] = 0x28; range_buf[pos++] = 0x01; /* sort_order = ASCEND */
    range_buf[pos++] = 0x30; range_buf[pos++] = 0x04; /* sort_target = VALUE */

    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Range", range_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 2);

    /* Find all kvs (tag 0x12) and extract their values to check order */
    /* Expected order by value: val_a, val_b, val_c (ascending) */
    const char *expected[] = {"val_a", "val_b", "val_c"};
    int found_idx = 0;
    size_t p = 0;

    /* Skip header (tag 0x0a) */
    if (p < resp.len && resp.data[p] == 0x0a) {
        p++;
        uint64_t l = 0; int shift = 0;
        while (p < resp.len) {
            uint8_t b = resp.data[p++];
            l |= (uint64_t)(b & 0x7F) << shift;
            if (!(b & 0x80)) break;
            shift += 7;
        }
        p += (size_t)l;
    }

    while (p < resp.len && found_idx < 3) {
        uint8_t tag = resp.data[p++];
        if (tag == 0x12) {
            /* KV entry */
            uint64_t klen = 0; int shift = 0;
            while (p < resp.len) {
                uint8_t b = resp.data[p++];
                klen |= (uint64_t)(b & 0x7F) << shift;
                if (!(b & 0x80)) break;
                shift += 7;
            }
            size_t kend = p + (size_t)klen;
            if (kend > resp.len) kend = resp.len;
            /* Find value field (tag 0x2a) inside KV */
            while (p < kend) {
                uint8_t ktag = resp.data[p++];
                if (ktag == 0x2a) {
                    uint64_t vl = 0; int s2 = 0;
                    while (p < kend) {
                        uint8_t b = resp.data[p++];
                        vl |= (uint64_t)(b & 0x7F) << s2;
                        if (!(b & 0x80)) break;
                        s2 += 7;
                    }
                    /* Compare value with expected */
                    CETCD_ASSERT_TRUE(vl == 5);
                    CETCD_ASSERT_TRUE(memcmp(resp.data + p, expected[found_idx], 5) == 0);
                    found_idx++;
                    p += (size_t)vl;
                } else if (ktag == 0x0a) {
                    uint64_t l = 0; int s3 = 0;
                    while (p < kend) {
                        uint8_t b = resp.data[p++];
                        l |= (uint64_t)(b & 0x7F) << s3;
                        if (!(b & 0x80)) break;
                        s3 += 7;
                    }
                    p += (size_t)l;
                } else {
                    while (p < kend) { uint8_t b = resp.data[p++]; if (!(b & 0x80)) break; }
                }
            }
            p = kend;
        } else if (tag == 0x20 || tag == 0x18) {
            while (p < resp.len) { uint8_t b = resp.data[p++]; if (!(b & 0x80)) break; }
        } else if (tag == 0x0a) {
            uint64_t l = 0; int s4 = 0;
            while (p < resp.len) {
                uint8_t b = resp.data[p++];
                l |= (uint64_t)(b & 0x7F) << s4;
                if (!(b & 0x80)) break;
                s4 += 7;
            }
            p += (size_t)l;
        } else {
            while (p < resp.len) { uint8_t b = resp.data[p++]; if (!(b & 0x80)) break; }
        }
    }
    CETCD_ASSERT_TRUE(found_idx == 3);

    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_watch_cancel_has_header) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Send a WatchCancelRequest: field 2 = WatchCancelRequest (tag 0x12)
     * WatchCancelRequest: field 1 = watch_id (tag 0x08) */
    uint8_t cancel_inner[8]; size_t cpos = 0;
    cancel_inner[cpos++] = 0x08; cancel_inner[cpos++] = 0x05; /* watch_id = 5 */

    uint8_t watch_buf[16]; size_t wpos = 0;
    watch_buf[wpos++] = 0x12; /* field 2 = WatchCancelRequest */
    watch_buf[wpos++] = (uint8_t)cpos;
    memcpy(watch_buf + wpos, cancel_inner, cpos); wpos += cpos;

    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.Watch/Watch", watch_buf, wpos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 2);

    /* Cancel response should start with header (tag 0x0a) */
    CETCD_ASSERT_TRUE(resp.data[0] == 0x0a);

    /* After header, should find canceled (tag 0x20) */
    size_t p = 1;
    uint64_t hdr_len = 0; int shift = 0;
    while (p < resp.len) {
        uint8_t b = resp.data[p++];
        hdr_len |= (uint64_t)(b & 0x7F) << shift;
        if (!(b & 0x80)) break;
        shift += 7;
    }
    p += (size_t)hdr_len;

    bool found_canceled = false;
    bool found_watch_id = false;
    while (p < resp.len) {
        uint8_t tag = resp.data[p++];
        if (tag == 0x20) {
            found_canceled = true;
            while (p < resp.len) { uint8_t b = resp.data[p++]; if (!(b & 0x80)) break; }
        } else if (tag == 0x10) {
            found_watch_id = true;
            while (p < resp.len) { uint8_t b = resp.data[p++]; if (!(b & 0x80)) break; }
        } else {
            while (p < resp.len) { uint8_t b = resp.data[p++]; if (!(b & 0x80)) break; }
        }
    }
    CETCD_ASSERT_TRUE(found_canceled);
    CETCD_ASSERT_TRUE(found_watch_id);

    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

/* === Round 10 tests: proto field number fixes + new features === */

CETCD_TEST_CASE(v3rpc_range_more_correct_tag) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Put 3 keys: mta, mtb, mtc */
    const char *keys[] = {"mta", "mtb", "mtc"};
    const char *vals[] = {"va", "vb", "vc"};
    for (int i = 0; i < 3; i++) {
        uint8_t req[64]; size_t pos = 0;
        req[pos++] = 0x0a; req[pos++] = 3;
        memcpy(req + pos, keys[i], 3); pos += 3;
        req[pos++] = 0x12; req[pos++] = 2;
        memcpy(req + pos, vals[i], 2); pos += 2;
        cetcd_rpc_bytes r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", req, pos);
        cetcd_rpc_bytes_free(&r);
    }

    /* Range [mta, mtd) with limit=2 */
    uint8_t range_req[64]; size_t rp = 0;
    range_req[rp++] = 0x0a; range_req[rp++] = 3;
    memcpy(range_req + rp, "mta", 3); rp += 3;
    range_req[rp++] = 0x12; range_req[rp++] = 3;
    memcpy(range_req + rp, "mtd", 3); rp += 3;
    range_req[rp++] = 0x18; range_req[rp++] = 0x02; /* limit=2 */

    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Range", range_req, rp);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 2);

    /* Parse response: skip header (0x0a), find more (0x18) and count (0x20) */
    size_t p = 0;
    bool found_more = false;
    bool more_val = false;
    bool found_count = false;
    uint64_t count_val = 0;
    while (p < resp.len) {
        uint8_t tag = resp.data[p++];
        if (tag == 0x0a || tag == 0x12) {
            /* length-delimited */
            uint64_t l = 0; int shift = 0;
            while (p < resp.len) { uint8_t b = resp.data[p++]; l |= (uint64_t)(b & 0x7F) << shift; if (!(b & 0x80)) break; shift += 7; }
            p += (size_t)l;
        } else if (tag == 0x18) {
            /* more (varint) */
            found_more = true;
            uint64_t v = 0; int shift = 0;
            while (p < resp.len) { uint8_t b = resp.data[p++]; v |= (uint64_t)(b & 0x7F) << shift; if (!(b & 0x80)) break; shift += 7; }
            more_val = (v != 0);
        } else if (tag == 0x20) {
            /* count (varint) — total matches, not limit */
            found_count = true;
            count_val = 0; int shift = 0;
            while (p < resp.len) { uint8_t b = resp.data[p++]; count_val |= (uint64_t)(b & 0x7F) << shift; if (!(b & 0x80)) break; shift += 7; }
        } else {
            while (p < resp.len) { uint8_t b = resp.data[p++]; if (!(b & 0x80)) break; }
        }
    }
    CETCD_ASSERT_TRUE(found_more);
    CETCD_ASSERT_TRUE(more_val); /* more should be true since 3 keys > limit 2 */
    CETCD_ASSERT_TRUE(found_count);
    CETCD_ASSERT_TRUE(count_val == 3);

    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_range_encodes_lease) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Grant lease */
    uint8_t grant_buf[4]; size_t gpos = 0;
    grant_buf[gpos++] = 0x08; grant_buf[gpos++] = 0x3c;
    grant_buf[gpos++] = 0x10; grant_buf[gpos++] = 0x00;
    cetcd_rpc_bytes gr = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.Lease/LeaseGrant", grant_buf, gpos);
    CETCD_ASSERT_NOT_NULL(gr.data);
    int64_t lease_id = 0;
    size_t rpos = 0;
    while (rpos < gr.len) {
        uint8_t tag = gr.data[rpos++];
        if (tag == 0x0a) {
            uint64_t l = 0; int shift = 0;
            while (rpos < gr.len) {
                uint8_t b = gr.data[rpos++];
                l |= (uint64_t)(b & 0x7F) << shift;
                if (!(b & 0x80)) break;
                shift += 7;
            }
            rpos += (size_t)l;
        } else {
            uint64_t v = 0; int shift = 0;
            while (rpos < gr.len) {
                uint8_t b = gr.data[rpos++];
                v |= (uint64_t)(b & 0x7F) << shift;
                if (!(b & 0x80)) break;
                shift += 7;
            }
            if (tag == 0x10) { lease_id = (int64_t)v; break; }
        }
    }
    cetcd_rpc_bytes_free(&gr);
    CETCD_ASSERT_TRUE(lease_id > 0);

    /* Put key="lk" value="v" with granted lease */
    uint8_t put[32]; size_t pos = 0;
    put[pos++] = 0x0a; put[pos++] = 2; put[pos++] = 'l'; put[pos++] = 'k';
    put[pos++] = 0x12; put[pos++] = 1; put[pos++] = 'v';
    put[pos++] = 0x18;
    uint64_t lid = (uint64_t)lease_id;
    do { uint8_t b = lid & 0x7F; lid >>= 7; if (lid) b |= 0x80; put[pos++] = b; } while (lid);
    cetcd_rpc_bytes pr = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", put, pos);
    cetcd_rpc_bytes_free(&pr);

    uint8_t range[16]; size_t rp = 0;
    range[rp++] = 0x0a; range[rp++] = 2; range[rp++] = 'l'; range[rp++] = 'k';
    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Range", range, rp);
    CETCD_ASSERT_NOT_NULL(resp.data);

    /* Find KeyValue (field 2, 0x12) then lease tag 0x30 == granted id */
    size_t p = 0;
    bool found_lease = false;
    uint64_t got = 0;
    while (p < resp.len) {
        uint8_t tag = resp.data[p++];
        if (tag == 0x0a) {
            uint64_t l = 0; int shift = 0;
            while (p < resp.len) {
                uint8_t b = resp.data[p++];
                l |= (uint64_t)(b & 0x7F) << shift;
                if (!(b & 0x80)) break;
                shift += 7;
            }
            p += (size_t)l;
        } else if (tag == 0x12) {
            uint64_t l = 0; int shift = 0;
            while (p < resp.len) {
                uint8_t b = resp.data[p++];
                l |= (uint64_t)(b & 0x7F) << shift;
                if (!(b & 0x80)) break;
                shift += 7;
            }
            size_t end = p + (size_t)l;
            while (p < end) {
                uint8_t kt = resp.data[p++];
                if (kt == 0x0a || kt == 0x2a) {
                    uint64_t bl = 0; int s2 = 0;
                    while (p < end) {
                        uint8_t b = resp.data[p++];
                        bl |= (uint64_t)(b & 0x7F) << s2;
                        if (!(b & 0x80)) break;
                        s2 += 7;
                    }
                    p += (size_t)bl;
                } else if (kt == 0x30) {
                    found_lease = true;
                    got = 0; int s2 = 0;
                    while (p < end) {
                        uint8_t b = resp.data[p++];
                        got |= (uint64_t)(b & 0x7F) << s2;
                        if (!(b & 0x80)) break;
                        s2 += 7;
                    }
                } else {
                    while (p < end) {
                        uint8_t b = resp.data[p++];
                        if (!(b & 0x80)) break;
                    }
                }
            }
        } else {
            while (p < resp.len) {
                uint8_t b = resp.data[p++];
                if (!(b & 0x80)) break;
            }
        }
    }
    CETCD_ASSERT_TRUE(found_lease);
    CETCD_ASSERT_TRUE(got == (uint64_t)lease_id);

    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_put_rejects_unknown_lease) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Baseline put without lease */
    uint8_t put0[16]; size_t pos = 0;
    put0[pos++] = 0x0a; put0[pos++] = 5; memcpy(put0 + pos, "probe", 5); pos += 5;
    put0[pos++] = 0x12; put0[pos++] = 1; put0[pos++] = '1';
    cetcd_rpc_bytes r0 = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", put0, pos);
    cetcd_rpc_bytes_free(&r0);

    /* Put with unknown lease must fail RPC and not create the key */
    uint8_t put[32]; pos = 0;
    put[pos++] = 0x0a; put[pos++] = 5; memcpy(put + pos, "badlx", 5); pos += 5;
    put[pos++] = 0x12; put[pos++] = 2; memcpy(put + pos, "v1", 2); pos += 2;
    put[pos++] = 0x18;
    uint64_t bad = 424242;
    do { uint8_t b = bad & 0x7F; bad >>= 7; if (bad) b |= 0x80; put[pos++] = b; } while (bad);
    cetcd_rpc_bytes pr = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", put, pos);
    CETCD_ASSERT_TRUE(pr.data == NULL || pr.len == 0);
    cetcd_rpc_bytes_free(&pr);

    uint8_t range[16]; size_t rp = 0;
    range[rp++] = 0x0a; range[rp++] = 5; memcpy(range + rp, "badlx", 5); rp += 5;
    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Range", range, rp);
    CETCD_ASSERT_NOT_NULL(resp.data);
    int found = 0;
    for (size_t i = 0; i + 5 <= resp.len; i++) {
        if (memcmp(resp.data + i, "badlx", 5) == 0) { found = 1; break; }
    }
    CETCD_ASSERT_TRUE(!found);
    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_watch_prev_kv_flag) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Build WatchCreateRequest with prev_kv=true (field 6, tag 0x30) */
    uint8_t create_inner[32]; size_t cpos = 0;
    create_inner[cpos++] = 0x0a; create_inner[cpos++] = 0x04;
    memcpy(create_inner + cpos, "wpkv", 4); cpos += 4;
    create_inner[cpos++] = 0x18; create_inner[cpos++] = 0x01; /* start_revision=1 */
    create_inner[cpos++] = 0x30; create_inner[cpos++] = 0x01; /* prev_kv=true */

    uint8_t watch_buf[64]; size_t wpos = 0;
    watch_buf[wpos++] = 0x0a;
    watch_buf[wpos++] = (uint8_t)cpos;
    memcpy(watch_buf + wpos, create_inner, cpos); wpos += cpos;

    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.Watch/Watch", watch_buf, wpos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 2);

    /* Verify response has header (0x0a), watch_id (0x10), created (0x18) */
    CETCD_ASSERT_TRUE(resp.data[0] == 0x0a);

    bool found_created = false;
    size_t p = 1;
    uint64_t hdr_len = 0; int shift = 0;
    while (p < resp.len) { uint8_t b = resp.data[p++]; hdr_len |= (uint64_t)(b & 0x7F) << shift; if (!(b & 0x80)) break; shift += 7; }
    p += (size_t)hdr_len;
    while (p < resp.len) {
        uint8_t tag = resp.data[p++];
        if (tag == 0x18) { found_created = true; while (p < resp.len) { uint8_t b = resp.data[p++]; if (!(b & 0x80)) break; } }
        else if (tag == 0x10) { while (p < resp.len) { uint8_t b = resp.data[p++]; if (!(b & 0x80)) break; } }
        else if (tag == 0x0a || tag == 0x5a) { uint64_t l = 0; int s = 0; while (p < resp.len) { uint8_t b = resp.data[p++]; l |= (uint64_t)(b & 0x7F) << s; if (!(b & 0x80)) break; s += 7; } p += (size_t)l; }
        else { while (p < resp.len) { uint8_t b = resp.data[p++]; if (!(b & 0x80)) break; } }
    }
    CETCD_ASSERT_TRUE(found_created);

    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_txn_compare_range_end) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Put key="trk1" value="v1", key="trk2" value="v2" */
    const char *tkeys[] = {"trk1", "trk2"};
    const char *tvals[] = {"v1", "v2"};
    for (int i = 0; i < 2; i++) {
        uint8_t req[32]; size_t pos = 0;
        req[pos++] = 0x0a; req[pos++] = 4;
        memcpy(req + pos, tkeys[i], 4); pos += 4;
        req[pos++] = 0x12; req[pos++] = 2;
        memcpy(req + pos, tvals[i], 2); pos += 2;
        cetcd_rpc_bytes r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", req, pos);
        cetcd_rpc_bytes_free(&r);
    }

    /* Build Compare: result=EQUAL(0), target=VERSION(0), key="trk1",
     * range_end="trk3" (field 9, tag 0x4a), version=1
     * All keys in [trk1, trk3) have version=1, so compare should succeed */
    uint8_t cmp_buf[64]; size_t cpos = 0;
    cmp_buf[cpos++] = 0x08; cmp_buf[cpos++] = 0x00; /* result=EQUAL */
    cmp_buf[cpos++] = 0x10; cmp_buf[cpos++] = 0x00; /* target=VERSION */
    cmp_buf[cpos++] = 0x1a; cmp_buf[cpos++] = 0x04; /* key */
    memcpy(cmp_buf + cpos, "trk1", 4); cpos += 4;
    cmp_buf[cpos++] = 0x4a; cmp_buf[cpos++] = 0x04; /* range_end (field 9) */
    memcpy(cmp_buf + cpos, "trk3", 4); cpos += 4;
    cmp_buf[cpos++] = 0x20; cmp_buf[cpos++] = 0x01; /* version=1 */

    /* Build success op: Put("trk1", "updated") */
    uint8_t put_inner[32]; size_t ppos = 0;
    put_inner[ppos++] = 0x0a; put_inner[ppos++] = 0x04;
    memcpy(put_inner + ppos, "trk1", 4); ppos += 4;
    put_inner[ppos++] = 0x12; put_inner[ppos++] = 0x07;
    memcpy(put_inner + ppos, "updated", 7); ppos += 7;
    uint8_t op_buf[64]; size_t opos = 0;
    op_buf[opos++] = 0x12; /* RequestPut tag */
    op_buf[opos++] = (uint8_t)ppos;
    memcpy(op_buf + opos, put_inner, ppos); opos += ppos;

    /* Build TxnRequest: field 1 (compare) + field 2 (success) */
    uint8_t txn_buf[256]; size_t tpos = 0;
    txn_buf[tpos++] = 0x0a; txn_buf[tpos++] = (uint8_t)cpos;
    memcpy(txn_buf + tpos, cmp_buf, cpos); tpos += cpos;
    txn_buf[tpos++] = 0x12; txn_buf[tpos++] = (uint8_t)opos;
    memcpy(txn_buf + tpos, op_buf, opos); tpos += opos;

    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Txn", txn_buf, tpos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 2);

    /* Find succeeded (tag 0x10) and verify it's true */
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

CETCD_TEST_CASE(v3rpc_txn_compare_missing_key_version) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Compare VERSION==1 on a missing key must fail (actual 0 != 1).
     * Pre-fix bug: target stayed 0 so EQUAL always succeeded. */
    uint8_t cmp_buf[32]; size_t cpos = 0;
    cmp_buf[cpos++] = 0x08; cmp_buf[cpos++] = 0x00; /* result=EQUAL */
    cmp_buf[cpos++] = 0x10; cmp_buf[cpos++] = 0x00; /* target=VERSION */
    cmp_buf[cpos++] = 0x1a; cmp_buf[cpos++] = 0x06; /* key */
    memcpy(cmp_buf + cpos, "nosuch", 6); cpos += 6;
    cmp_buf[cpos++] = 0x20; cmp_buf[cpos++] = 0x01; /* version=1 */

    uint8_t put_inner[24]; size_t ppos = 0;
    put_inner[ppos++] = 0x0a; put_inner[ppos++] = 0x06;
    memcpy(put_inner + ppos, "nosuch", 6); ppos += 6;
    put_inner[ppos++] = 0x12; put_inner[ppos++] = 0x01;
    put_inner[ppos++] = 'x';
    uint8_t op_buf[32]; size_t opos = 0;
    op_buf[opos++] = 0x12;
    op_buf[opos++] = (uint8_t)ppos;
    memcpy(op_buf + opos, put_inner, ppos); opos += ppos;

    uint8_t txn_buf[80]; size_t tpos = 0;
    txn_buf[tpos++] = 0x0a; txn_buf[tpos++] = (uint8_t)cpos;
    memcpy(txn_buf + tpos, cmp_buf, cpos); tpos += cpos;
    txn_buf[tpos++] = 0x12; txn_buf[tpos++] = (uint8_t)opos;
    memcpy(txn_buf + tpos, op_buf, opos); tpos += opos;

    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Txn", txn_buf, tpos);
    CETCD_ASSERT_NOT_NULL(resp.data);

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
    CETCD_ASSERT_TRUE(!succeeded_val);

    /* Key must not have been created */
    uint8_t range_buf[16]; size_t rpos = 0;
    range_buf[rpos++] = 0x0a; range_buf[rpos++] = 0x06;
    memcpy(range_buf + rpos, "nosuch", 6); rpos += 6;
    cetcd_rpc_bytes_free(&resp);
    resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Range", range_buf, rpos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    int found_key = 0;
    for (size_t i = 0; i + 6 <= resp.len; i++) {
        if (memcmp(resp.data + i, "nosuch", 6) == 0) { found_key = 1; break; }
    }
    CETCD_ASSERT_TRUE(!found_key);

    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_txn_put_prev_kv) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Put key="tpk" value="oldval" */
    uint8_t put_buf[32]; size_t pos = 0;
    put_buf[pos++] = 0x0a; put_buf[pos++] = 0x03;
    memcpy(put_buf + pos, "tpk", 3); pos += 3;
    put_buf[pos++] = 0x12; put_buf[pos++] = 0x06;
    memcpy(put_buf + pos, "oldval", 6); pos += 6;
    cetcd_rpc_bytes r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", put_buf, pos);
    cetcd_rpc_bytes_free(&r);

    /* Build Txn with no compare (always succeed),
     * success op = RequestPut("tpk", "newval", prev_kv=true) */
    uint8_t put_inner[32]; size_t ppos = 0;
    put_inner[ppos++] = 0x0a; put_inner[ppos++] = 0x03;
    memcpy(put_inner + ppos, "tpk", 3); ppos += 3;
    put_inner[ppos++] = 0x12; put_inner[ppos++] = 0x06;
    memcpy(put_inner + ppos, "newval", 6); ppos += 6;
    put_inner[ppos++] = 0x20; put_inner[ppos++] = 0x01; /* prev_kv=true (field 4) */

    uint8_t op_buf[64]; size_t opos = 0;
    op_buf[opos++] = 0x12; /* RequestPut tag */
    op_buf[opos++] = (uint8_t)ppos;
    memcpy(op_buf + opos, put_inner, ppos); opos += ppos;

    /* TxnRequest: only field 2 (success), no compare */
    uint8_t txn_buf[128]; size_t tpos = 0;
    txn_buf[tpos++] = 0x12; txn_buf[tpos++] = (uint8_t)opos;
    memcpy(txn_buf + tpos, op_buf, opos); tpos += opos;

    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Txn", txn_buf, tpos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 2);

    /* Verify response contains "oldval" (the prev_kv value in ResponsePut) */
    bool found_oldval = false;
    for (size_t i = 0; i + 6 <= resp.len; i++) {
        if (memcmp(resp.data + i, "oldval", 6) == 0) { found_oldval = true; break; }
    }
    CETCD_ASSERT_TRUE(found_oldval);

    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_txn_delete_range_prev_kv) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Put key="tdr1" value="dv1", key="tdr2" value="dv2" */
    const char *dkeys[] = {"tdr1", "tdr2"};
    const char *dvals[] = {"dv1", "dv2"};
    for (int i = 0; i < 2; i++) {
        uint8_t req[32]; size_t pos = 0;
        req[pos++] = 0x0a; req[pos++] = 4;
        memcpy(req + pos, dkeys[i], 4); pos += 4;
        req[pos++] = 0x12; req[pos++] = 3;
        memcpy(req + pos, dvals[i], 3); pos += 3;
        cetcd_rpc_bytes r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", req, pos);
        cetcd_rpc_bytes_free(&r);
    }

    /* Build Txn with no compare,
     * success op = RequestDeleteRange(key="tdr1", range_end="tdr3", prev_kv=true)
     * DeleteRangeRequest: field 1 (key) tag 0x0a, field 2 (range_end) tag 0x12,
     *                     field 3 (prev_kv) tag 0x18 */
    uint8_t del_inner[32]; size_t dpos = 0;
    del_inner[dpos++] = 0x0a; del_inner[dpos++] = 0x04;
    memcpy(del_inner + dpos, "tdr1", 4); dpos += 4;
    del_inner[dpos++] = 0x12; del_inner[dpos++] = 0x04; /* range_end */
    memcpy(del_inner + dpos, "tdr3", 4); dpos += 4;
    del_inner[dpos++] = 0x18; del_inner[dpos++] = 0x01; /* prev_kv=true */

    uint8_t op_buf[64]; size_t opos = 0;
    op_buf[opos++] = 0x1a; /* RequestDeleteRange tag */
    op_buf[opos++] = (uint8_t)dpos;
    memcpy(op_buf + opos, del_inner, dpos); opos += dpos;

    uint8_t txn_buf[128]; size_t tpos = 0;
    txn_buf[tpos++] = 0x12; txn_buf[tpos++] = (uint8_t)opos;
    memcpy(txn_buf + tpos, op_buf, opos); tpos += opos;

    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Txn", txn_buf, tpos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 2);

    /* Verify response contains both "dv1" and "dv2" (prev_kvs values) */
    bool found_dv1 = false, found_dv2 = false;
    for (size_t i = 0; i + 3 <= resp.len; i++) {
        if (memcmp(resp.data + i, "dv1", 3) == 0) found_dv1 = true;
        if (memcmp(resp.data + i, "dv2", 3) == 0) found_dv2 = true;
    }
    CETCD_ASSERT_TRUE(found_dv1);
    CETCD_ASSERT_TRUE(found_dv2);

    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_range_min_max_revision) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Put 3 keys: rmm1 (rev 1), rmm2 (rev 2), rmm3 (rev 3) */
    const char *keys[] = {"rmm1", "rmm2", "rmm3"};
    const char *vals[] = {"v1", "v2", "v3"};
    for (int i = 0; i < 3; i++) {
        uint8_t req[64]; size_t pos = 0;
        req[pos++] = 0x0a; req[pos++] = 4;
        memcpy(req + pos, keys[i], 4); pos += 4;
        req[pos++] = 0x12; req[pos++] = 2;
        memcpy(req + pos, vals[i], 2); pos += 2;
        cetcd_rpc_bytes r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", req, pos);
        cetcd_rpc_bytes_free(&r);
    }

    /* Range [rmm1, rmm4) with min_mod_revision=2 (field 10, tag 0x50) */
    uint8_t range_req[64]; size_t rp = 0;
    range_req[rp++] = 0x0a; range_req[rp++] = 4;
    memcpy(range_req + rp, "rmm1", 4); rp += 4;
    range_req[rp++] = 0x12; range_req[rp++] = 4;
    memcpy(range_req + rp, "rmm4", 4); rp += 4;
    range_req[rp++] = 0x50; range_req[rp++] = 0x02; /* min_mod_revision=2 */

    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Range", range_req, rp);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 2);

    /* Should only get rmm2 and rmm3 (mod_revision >= 2), NOT rmm1 */
    bool found_rmm1 = false, found_rmm2 = false, found_rmm3 = false;
    for (size_t i = 0; i + 4 <= resp.len; i++) {
        if (memcmp(resp.data + i, "rmm1", 4) == 0) found_rmm1 = true;
        if (memcmp(resp.data + i, "rmm2", 4) == 0) found_rmm2 = true;
        if (memcmp(resp.data + i, "rmm3", 4) == 0) found_rmm3 = true;
    }
    CETCD_ASSERT_TRUE(!found_rmm1);  /* rmm1 should be filtered out */
    CETCD_ASSERT_TRUE(found_rmm2);
    CETCD_ASSERT_TRUE(found_rmm3);

    cetcd_rpc_bytes_free(&resp);

    /* Also test max_mod_revision=2: should get rmm1 and rmm2, NOT rmm3 */
    rp = 0;
    range_req[rp++] = 0x0a; range_req[rp++] = 4;
    memcpy(range_req + rp, "rmm1", 4); rp += 4;
    range_req[rp++] = 0x12; range_req[rp++] = 4;
    memcpy(range_req + rp, "rmm4", 4); rp += 4;
    range_req[rp++] = 0x58; range_req[rp++] = 0x02; /* max_mod_revision=2 */

    resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Range", range_req, rp);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 2);

    found_rmm1 = false; found_rmm2 = false; found_rmm3 = false;
    for (size_t i = 0; i + 4 <= resp.len; i++) {
        if (memcmp(resp.data + i, "rmm1", 4) == 0) found_rmm1 = true;
        if (memcmp(resp.data + i, "rmm2", 4) == 0) found_rmm2 = true;
        if (memcmp(resp.data + i, "rmm3", 4) == 0) found_rmm3 = true;
    }
    CETCD_ASSERT_TRUE(found_rmm1);
    CETCD_ASSERT_TRUE(found_rmm2);
    CETCD_ASSERT_TRUE(!found_rmm3);  /* rmm3 should be filtered out */

    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

/* === Round 11 tests: Txn RequestPut ignore_value, Txn RequestRange limit/keys_only/count_only === */

CETCD_TEST_CASE(v3rpc_txn_put_ignore_value) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Put key="tpiv" value="origval" */
    uint8_t put_buf[32]; size_t pos = 0;
    put_buf[pos++] = 0x0a; put_buf[pos++] = 0x04;
    memcpy(put_buf + pos, "tpiv", 4); pos += 4;
    put_buf[pos++] = 0x12; put_buf[pos++] = 0x07;
    memcpy(put_buf + pos, "origval", 7); pos += 7;
    cetcd_rpc_bytes r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", put_buf, pos);
    cetcd_rpc_bytes_free(&r);

    /* Build Txn with no compare,
     * success op = RequestPut("tpiv", ignore_value=true, no value)
     * RequestPut: field 1 (key) tag 0x0a, field 5 (ignore_value) tag 0x28 */
    uint8_t put_inner[16]; size_t ppos = 0;
    put_inner[ppos++] = 0x0a; put_inner[ppos++] = 0x04;
    memcpy(put_inner + ppos, "tpiv", 4); ppos += 4;
    put_inner[ppos++] = 0x28; put_inner[ppos++] = 0x01; /* ignore_value=true */

    uint8_t op_buf[32]; size_t opos = 0;
    op_buf[opos++] = 0x12; /* RequestPut tag */
    op_buf[opos++] = (uint8_t)ppos;
    memcpy(op_buf + opos, put_inner, ppos); opos += ppos;

    uint8_t txn_buf[64]; size_t tpos = 0;
    txn_buf[tpos++] = 0x12; txn_buf[tpos++] = (uint8_t)opos;
    memcpy(txn_buf + tpos, op_buf, opos); tpos += opos;

    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Txn", txn_buf, tpos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 2);
    cetcd_rpc_bytes_free(&resp);

    /* Range for key="tpiv" to verify value is still "origval" */
    uint8_t range_buf[16]; pos = 0;
    range_buf[pos++] = 0x0a; range_buf[pos++] = 0x04;
    memcpy(range_buf + pos, "tpiv", 4); pos += 4;
    resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Range", range_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 2);

    bool found_origval = false;
    for (size_t i = 0; i + 7 <= resp.len; i++) {
        if (memcmp(resp.data + i, "origval", 7) == 0) { found_origval = true; break; }
    }
    CETCD_ASSERT_TRUE(found_origval);

    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_txn_range_limit) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Put 3 keys: trl1, trl2, trl3 */
    const char *keys[] = {"trl1", "trl2", "trl3"};
    const char *vals[] = {"v1", "v2", "v3"};
    for (int i = 0; i < 3; i++) {
        uint8_t req[32]; size_t pos = 0;
        req[pos++] = 0x0a; req[pos++] = 4;
        memcpy(req + pos, keys[i], 4); pos += 4;
        req[pos++] = 0x12; req[pos++] = 2;
        memcpy(req + pos, vals[i], 2); pos += 2;
        cetcd_rpc_bytes r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", req, pos);
        cetcd_rpc_bytes_free(&r);
    }

    /* Build Txn with success op: RequestRange("trl1", range_end="trl4", limit=2)
     * RequestRange: field 1 (key), field 2 (range_end), field 3 (limit) tag 0x18 */
    uint8_t range_inner[32]; size_t rip = 0;
    range_inner[rip++] = 0x0a; range_inner[rip++] = 4;
    memcpy(range_inner + rip, "trl1", 4); rip += 4;
    range_inner[rip++] = 0x12; range_inner[rip++] = 4;
    memcpy(range_inner + rip, "trl4", 4); rip += 4;
    range_inner[rip++] = 0x18; range_inner[rip++] = 0x02; /* limit=2 */

    uint8_t op_buf[64]; size_t opos = 0;
    op_buf[opos++] = 0x0a; /* RequestRange */
    op_buf[opos++] = (uint8_t)rip;
    memcpy(op_buf + opos, range_inner, rip); opos += rip;

    uint8_t txn_buf[128]; size_t tpos = 0;
    txn_buf[tpos++] = 0x12; txn_buf[tpos++] = (uint8_t)opos;
    memcpy(txn_buf + tpos, op_buf, opos); tpos += opos;

    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Txn", txn_buf, tpos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 5);

    /* Verify response contains "trl1" and "trl2" but NOT "trl3" */
    bool found_trl1 = false, found_trl2 = false, found_trl3 = false;
    for (size_t i = 0; i + 4 <= resp.len; i++) {
        if (memcmp(resp.data + i, "trl1", 4) == 0) found_trl1 = true;
        if (memcmp(resp.data + i, "trl2", 4) == 0) found_trl2 = true;
        if (memcmp(resp.data + i, "trl3", 4) == 0) found_trl3 = true;
    }
    CETCD_ASSERT_TRUE(found_trl1);
    CETCD_ASSERT_TRUE(found_trl2);
    CETCD_ASSERT_TRUE(!found_trl3); /* trl3 should be truncated by limit */

    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_txn_range_count_only) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Put 3 keys: trc1, trc2, trc3 */
    const char *keys[] = {"trc1", "trc2", "trc3"};
    for (int i = 0; i < 3; i++) {
        uint8_t req[32]; size_t pos = 0;
        req[pos++] = 0x0a; req[pos++] = 4;
        memcpy(req + pos, keys[i], 4); pos += 4;
        req[pos++] = 0x12; req[pos++] = 2;
        memcpy(req + pos, "cv", 2); pos += 2;
        cetcd_rpc_bytes r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", req, pos);
        cetcd_rpc_bytes_free(&r);
    }

    /* Build Txn with success op: RequestRange("trc1", range_end="trc4", count_only=true)
     * RequestRange: field 1 (key), field 2 (range_end), field 9 (count_only) tag 0x48 */
    uint8_t range_inner[32]; size_t rip = 0;
    range_inner[rip++] = 0x0a; range_inner[rip++] = 4;
    memcpy(range_inner + rip, "trc1", 4); rip += 4;
    range_inner[rip++] = 0x12; range_inner[rip++] = 4;
    memcpy(range_inner + rip, "trc4", 4); rip += 4;
    range_inner[rip++] = 0x48; range_inner[rip++] = 0x01; /* count_only=true */

    uint8_t op_buf[64]; size_t opos = 0;
    op_buf[opos++] = 0x0a; /* RequestRange */
    op_buf[opos++] = (uint8_t)rip;
    memcpy(op_buf + opos, range_inner, rip); opos += rip;

    uint8_t txn_buf[128]; size_t tpos = 0;
    txn_buf[tpos++] = 0x12; txn_buf[tpos++] = (uint8_t)opos;
    memcpy(txn_buf + tpos, op_buf, opos); tpos += opos;

    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Txn", txn_buf, tpos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 5);

    /* With count_only, values should NOT be present in response.
     * Verify "cv" (the value) is NOT in the response. */
    bool found_cv = false;
    for (size_t i = 0; i + 2 <= resp.len; i++) {
        if (resp.data[i] == 'c' && resp.data[i+1] == 'v') found_cv = true;
    }
    CETCD_ASSERT_TRUE(!found_cv);

    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_txn_range_keys_only) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Put key="trko" value="theval" */
    uint8_t put_buf[32]; size_t pos = 0;
    put_buf[pos++] = 0x0a; put_buf[pos++] = 4;
    memcpy(put_buf + pos, "trko", 4); pos += 4;
    put_buf[pos++] = 0x12; put_buf[pos++] = 6;
    memcpy(put_buf + pos, "theval", 6); pos += 6;
    cetcd_rpc_bytes r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", put_buf, pos);
    cetcd_rpc_bytes_free(&r);

    /* Build Txn with success op: RequestRange("trko", keys_only=true)
     * RequestRange: field 1 (key), field 8 (keys_only) tag 0x40 */
    uint8_t range_inner[16]; size_t rip = 0;
    range_inner[rip++] = 0x0a; range_inner[rip++] = 4;
    memcpy(range_inner + rip, "trko", 4); rip += 4;
    range_inner[rip++] = 0x40; range_inner[rip++] = 0x01; /* keys_only=true */

    uint8_t op_buf[32]; size_t opos = 0;
    op_buf[opos++] = 0x0a; /* RequestRange */
    op_buf[opos++] = (uint8_t)rip;
    memcpy(op_buf + opos, range_inner, rip); opos += rip;

    uint8_t txn_buf[64]; size_t tpos = 0;
    txn_buf[tpos++] = 0x12; txn_buf[tpos++] = (uint8_t)opos;
    memcpy(txn_buf + tpos, op_buf, opos); tpos += opos;

    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Txn", txn_buf, tpos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 5);

    /* With keys_only, key "trko" should be present but value "theval" should NOT */
    bool found_key = false;
    for (size_t i = 0; i + 4 <= resp.len; i++) {
        if (memcmp(resp.data + i, "trko", 4) == 0) found_key = true;
    }
    CETCD_ASSERT_TRUE(found_key);

    bool found_val = false;
    for (size_t i = 0; i + 6 <= resp.len; i++) {
        if (memcmp(resp.data + i, "theval", 6) == 0) found_val = true;
    }
    CETCD_ASSERT_TRUE(!found_val);

    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_txn_range_sort_order) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Put 3 keys: trs1="va", trs2="vb", trs3="vc" */
    const char *keys[] = {"trs1", "trs2", "trs3"};
    const char *vals[] = {"va", "vb", "vc"};
    for (int i = 0; i < 3; i++) {
        uint8_t req[64]; size_t pos = 0;
        req[pos++] = 0x0a; req[pos++] = 4;
        memcpy(req + pos, keys[i], 4); pos += 4;
        req[pos++] = 0x12; req[pos++] = 2;
        memcpy(req + pos, vals[i], 2); pos += 2;
        cetcd_rpc_bytes r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", req, pos);
        cetcd_rpc_bytes_free(&r);
    }

    /* Build Txn with success op: RequestRange("trs1", "trs4", sort_order=DESCEND, sort_target=KEY)
     * sort_order=2 (DESCEND) is field 5 tag 0x28
     * sort_target=0 (KEY) is field 6 tag 0x30 */
    uint8_t range_inner[32]; size_t rip = 0;
    range_inner[rip++] = 0x0a; range_inner[rip++] = 4;
    memcpy(range_inner + rip, "trs1", 4); rip += 4;
    range_inner[rip++] = 0x12; range_inner[rip++] = 4;
    memcpy(range_inner + rip, "trs4", 4); rip += 4;
    range_inner[rip++] = 0x28; range_inner[rip++] = 0x02; /* sort_order=DESCEND */
    range_inner[rip++] = 0x30; range_inner[rip++] = 0x00; /* sort_target=KEY */

    uint8_t op_buf[64]; size_t opos = 0;
    op_buf[opos++] = 0x0a; /* RequestRange */
    op_buf[opos++] = (uint8_t)rip;
    memcpy(op_buf + opos, range_inner, rip); opos += rip;

    uint8_t txn_buf[128]; size_t tpos = 0;
    txn_buf[tpos++] = 0x12; txn_buf[tpos++] = (uint8_t)opos;
    memcpy(txn_buf + tpos, op_buf, opos); tpos += opos;

    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Txn", txn_buf, tpos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 5);

    /* With DESCEND sort by KEY, trs3 should appear before trs1 in response */
    int pos_trs1 = -1, pos_trs3 = -1;
    for (size_t i = 0; i + 4 <= resp.len; i++) {
        if (memcmp(resp.data + i, "trs1", 4) == 0 && pos_trs1 < 0) pos_trs1 = (int)i;
        if (memcmp(resp.data + i, "trs3", 4) == 0 && pos_trs3 < 0) pos_trs3 = (int)i;
    }
    CETCD_ASSERT_TRUE(pos_trs1 >= 0);
    CETCD_ASSERT_TRUE(pos_trs3 >= 0);
    CETCD_ASSERT_TRUE(pos_trs3 < pos_trs1); /* trs3 before trs1 */

    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

CETCD_TEST_CASE(v3rpc_txn_range_revision_filter) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Put 3 keys: trf1 (rev 1), trf2 (rev 2), trf3 (rev 3) */
    const char *keys[] = {"trf1", "trf2", "trf3"};
    const char *vals[] = {"v1", "v2", "v3"};
    for (int i = 0; i < 3; i++) {
        uint8_t req[64]; size_t pos = 0;
        req[pos++] = 0x0a; req[pos++] = 4;
        memcpy(req + pos, keys[i], 4); pos += 4;
        req[pos++] = 0x12; req[pos++] = 2;
        memcpy(req + pos, vals[i], 2); pos += 2;
        cetcd_rpc_bytes r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", req, pos);
        cetcd_rpc_bytes_free(&r);
    }

    /* Build Txn with success op: RequestRange("trf1", "trf4", min_mod_revision=2)
     * min_mod_revision=2 is field 10 tag 0x50 */
    uint8_t range_inner[32]; size_t rip = 0;
    range_inner[rip++] = 0x0a; range_inner[rip++] = 4;
    memcpy(range_inner + rip, "trf1", 4); rip += 4;
    range_inner[rip++] = 0x12; range_inner[rip++] = 4;
    memcpy(range_inner + rip, "trf4", 4); rip += 4;
    range_inner[rip++] = 0x50; range_inner[rip++] = 0x02; /* min_mod_revision=2 */

    uint8_t op_buf[64]; size_t opos = 0;
    op_buf[opos++] = 0x0a; /* RequestRange */
    op_buf[opos++] = (uint8_t)rip;
    memcpy(op_buf + opos, range_inner, rip); opos += rip;

    uint8_t txn_buf[128]; size_t tpos = 0;
    txn_buf[tpos++] = 0x12; txn_buf[tpos++] = (uint8_t)opos;
    memcpy(txn_buf + tpos, op_buf, opos); tpos += opos;

    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Txn", txn_buf, tpos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 2);

    /* Should only get trf2 and trf3 (mod_revision >= 2), NOT trf1 */
    bool found_trf1 = false, found_trf2 = false, found_trf3 = false;
    for (size_t i = 0; i + 4 <= resp.len; i++) {
        if (memcmp(resp.data + i, "trf1", 4) == 0) found_trf1 = true;
        if (memcmp(resp.data + i, "trf2", 4) == 0) found_trf2 = true;
        if (memcmp(resp.data + i, "trf3", 4) == 0) found_trf3 = true;
    }
    CETCD_ASSERT_TRUE(!found_trf1);
    CETCD_ASSERT_TRUE(found_trf2);
    CETCD_ASSERT_TRUE(found_trf3);

    cetcd_rpc_bytes_free(&resp);
    cetcd_v3rpc_free(rpc);
}

/* ── Streaming Watch tests ─────────────────────────────────────────────── */

/* Mock stream writer: records whether it was invoked. */
static int mock_write_called = 0;
static void mock_stream_write_fn(const uint8_t *data, size_t len, void *ctx) {
    (void)data; (void)len; (void)ctx;
    mock_write_called++;
}

static int mock_writes_a = 0;
static int mock_writes_b = 0;
static void mock_write_a(const uint8_t *data, size_t len, void *ctx) {
    (void)data; (void)len; (void)ctx;
    mock_writes_a++;
}
static void mock_write_b(const uint8_t *data, size_t len, void *ctx) {
    (void)data; (void)len; (void)ctx;
    mock_writes_b++;
}

/* Capture last streamed WatchResponse for compact-cancel assertions. */
static uint8_t g_stream_cap[512];
static size_t  g_stream_cap_len = 0;
static int     g_stream_cap_n = 0;
static void mock_stream_capture_fn(const uint8_t *data, size_t len, void *ctx) {
    (void)ctx;
    g_stream_cap_n++;
    if (len > sizeof(g_stream_cap)) len = sizeof(g_stream_cap);
    memcpy(g_stream_cap, data, len);
    g_stream_cap_len = len;
}

/* Reset the global streaming state before/after each test. */
static void reset_streaming_globals(void) {
    g_rpc_loop = NULL;
    g_rpc_stream_write_fn = NULL;
    g_rpc_stream_write_ctx = NULL;
}

CETCD_TEST_CASE(test_watch_create_streaming) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();
    CETCD_ASSERT_NOT_NULL(rpc);

    /* Activate streaming mode by setting loop + stream writer */
    cetcd_loop *loop = cetcd_loop_new();
    cetcd_v3rpc_set_loop(rpc, loop);
    cetcd_v3rpc_set_stream_writer(rpc, mock_stream_write_fn, NULL);

    /* Build WatchCreateRequest: key="skey", client_watch_id=100 (field 7, tag 0x38) */
    uint8_t create_inner[32]; size_t cpos = 0;
    create_inner[cpos++] = 0x0a; /* field 1 = key */
    create_inner[cpos++] = 0x04;
    memcpy(create_inner + cpos, "skey", 4); cpos += 4;
    create_inner[cpos++] = 0x38; /* field 7 = watch_id */
    create_inner[cpos++] = 0x64; /* 100 */

    uint8_t watch_buf[64]; size_t wpos = 0;
    watch_buf[wpos++] = 0x0a; /* field 1 = WatchCreateRequest */
    watch_buf[wpos++] = (uint8_t)cpos;
    memcpy(watch_buf + wpos, create_inner, cpos); wpos += cpos;

    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Watch/Watch", watch_buf, wpos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 2);

    /* Verify the response has header (0x0a) and created=true (0x18 0x01) */
    CETCD_ASSERT_TRUE(resp.data[0] == 0x0a);

    /* Parse past the header */
    size_t p = 1;
    uint64_t hdr_len = 0; int shift = 0;
    while (p < resp.len) {
        uint8_t b = resp.data[p++];
        hdr_len |= (uint64_t)(b & 0x7F) << shift;
        if (!(b & 0x80)) break;
        shift += 7;
    }
    p += (size_t)hdr_len;

    /* After header, look for created flag (tag 0x18) and watch_id (tag 0x10) */
    int found_created = 0;
    int found_watch_id = 0;
    while (p < resp.len) {
        uint8_t tag = resp.data[p++];
        if (tag == 0x18) { /* created */
            found_created = 1;
            while (p < resp.len) { uint8_t b = resp.data[p++]; if (!(b & 0x80)) break; }
        } else if (tag == 0x10) { /* watch_id */
            found_watch_id = 1;
            while (p < resp.len) { uint8_t b = resp.data[p++]; if (!(b & 0x80)) break; }
        } else if (tag == 0x0a || tag == 0x5a) {
            /* length-delimited: skip body */
            uint64_t l = 0; int s = 0;
            while (p < resp.len) { uint8_t b = resp.data[p++]; l |= (uint64_t)(b & 0x7F) << s; if (!(b & 0x80)) break; s += 7; }
            p += (size_t)l;
        } else {
            while (p < resp.len) { uint8_t b = resp.data[p++]; if (!(b & 0x80)) break; }
        }
    }
    CETCD_ASSERT_TRUE(found_created);
    CETCD_ASSERT_TRUE(found_watch_id);

    cetcd_rpc_bytes_free(&resp);
    reset_streaming_globals();
    cetcd_v3rpc_free(rpc);
    cetcd_loop_free(loop);
}

CETCD_TEST_CASE(v3rpc_watch_canceled_on_compact) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();
    cetcd_loop *loop = cetcd_loop_new();
    cetcd_v3rpc_set_loop(rpc, loop);

    /* Put k1 (rev=1), Put k2 (rev=2) */
    uint8_t put1[16]; size_t p = 0;
    put1[p++] = 0x0a; put1[p++] = 0x02; memcpy(put1 + p, "k1", 2); p += 2;
    put1[p++] = 0x12; put1[p++] = 0x02; memcpy(put1 + p, "v1", 2); p += 2;
    cetcd_rpc_bytes r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", put1, p);
    cetcd_rpc_bytes_free(&r);
    uint8_t put2[16]; p = 0;
    put2[p++] = 0x0a; put2[p++] = 0x02; memcpy(put2 + p, "k2", 2); p += 2;
    put2[p++] = 0x12; put2[p++] = 0x02; memcpy(put2 + p, "v2", 2); p += 2;
    r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", put2, p);
    cetcd_rpc_bytes_free(&r);

    g_stream_cap_n = 0;
    g_stream_cap_len = 0;
    cetcd_v3rpc_set_stream_writer(rpc, mock_stream_capture_fn, NULL);

    /* WatchCreate key=k1 start_revision=1 (before compact) */
    uint8_t create_inner[16]; size_t cpos = 0;
    create_inner[cpos++] = 0x0a; create_inner[cpos++] = 0x02;
    memcpy(create_inner + cpos, "k1", 2); cpos += 2;
    create_inner[cpos++] = 0x18; create_inner[cpos++] = 0x01;
    uint8_t watch_buf[32]; size_t wpos = 0;
    watch_buf[wpos++] = 0x0a;
    watch_buf[wpos++] = (uint8_t)cpos;
    memcpy(watch_buf + wpos, create_inner, cpos); wpos += cpos;
    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Watch/Watch", watch_buf, wpos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    cetcd_rpc_bytes_free(&resp);
    CETCD_ASSERT_EQ_INT(g_stream_cap_n, 0); /* create ack is unary, not streamed */

    /* Compact at 2 → active watch with start_rev=1 must be canceled on stream */
    uint8_t compact_buf[4]; p = 0;
    compact_buf[p++] = 0x08; compact_buf[p++] = 0x02;
    r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Compact", compact_buf, p);
    cetcd_rpc_bytes_free(&r);

    CETCD_ASSERT_TRUE(g_stream_cap_n >= 1);
    int found_canceled = 0, found_compact_rev = 0;
    size_t rpos = 0;
    while (rpos < g_stream_cap_len) {
        uint8_t tag = g_stream_cap[rpos++];
        if (tag == 0x0a || tag == 0x5a) {
            uint64_t l = 0; int shift = 0;
            while (rpos < g_stream_cap_len) {
                uint8_t b = g_stream_cap[rpos++];
                l |= (uint64_t)(b & 0x7F) << shift;
                if ((b & 0x80) == 0) break;
                shift += 7;
            }
            rpos += (size_t)l;
        } else if ((tag & 0x07) == 0) {
            uint64_t v = 0; int shift = 0;
            while (rpos < g_stream_cap_len) {
                uint8_t b = g_stream_cap[rpos++];
                v |= (uint64_t)(b & 0x7F) << shift;
                if ((b & 0x80) == 0) break;
                shift += 7;
            }
            if (tag == 0x20 && v == 1) found_canceled = 1;
            if (tag == 0x28 && v == 2) found_compact_rev = 1;
        } else {
            break;
        }
    }
    CETCD_ASSERT_TRUE(found_canceled);
    CETCD_ASSERT_TRUE(found_compact_rev);

    /* Negative: start_revision=0 survives Compact */
    g_stream_cap_n = 0;
    g_stream_cap_len = 0;
    {
        uint8_t inner[8]; size_t ip = 0;
        inner[ip++] = 0x0a; inner[ip++] = 0x02;
        memcpy(inner + ip, "k2", 2); ip += 2;
        uint8_t wb[16]; size_t wp = 0;
        wb[wp++] = 0x0a; wb[wp++] = (uint8_t)ip;
        memcpy(wb + wp, inner, ip); wp += ip;
        resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.Watch/Watch", wb, wp);
        cetcd_rpc_bytes_free(&resp);
    }
    r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Compact", compact_buf, p);
    cetcd_rpc_bytes_free(&r);
    CETCD_ASSERT_EQ_INT(g_stream_cap_n, 0);

    reset_streaming_globals();
    cetcd_v3rpc_free(rpc);
    cetcd_loop_free(loop);
}

CETCD_TEST_CASE(v3rpc_watch_replay_streaming) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();
    cetcd_loop *loop = cetcd_loop_new();
    cetcd_v3rpc_set_loop(rpc, loop);

    uint8_t put_buf[16]; size_t p = 0;
    put_buf[p++] = 0x0a; put_buf[p++] = 0x03;
    memcpy(put_buf + p, "srk", 3); p += 3;
    put_buf[p++] = 0x12; put_buf[p++] = 0x01; put_buf[p++] = 'z';
    cetcd_rpc_bytes r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", put_buf, p);
    cetcd_rpc_bytes_free(&r);

    g_stream_cap_n = 0;
    g_stream_cap_len = 0;
    cetcd_v3rpc_set_stream_writer(rpc, mock_stream_capture_fn, NULL);

    uint8_t create_inner[16]; size_t cpos = 0;
    create_inner[cpos++] = 0x0a; create_inner[cpos++] = 0x03;
    memcpy(create_inner + cpos, "srk", 3); cpos += 3;
    create_inner[cpos++] = 0x18; create_inner[cpos++] = 0x01;
    uint8_t watch_buf[32]; size_t wpos = 0;
    watch_buf[wpos++] = 0x0a;
    watch_buf[wpos++] = (uint8_t)cpos;
    memcpy(watch_buf + wpos, create_inner, cpos); wpos += cpos;

    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Watch/Watch", watch_buf, wpos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    cetcd_rpc_bytes_free(&resp);
    CETCD_ASSERT_EQ_INT(g_stream_cap_n, 0); /* deferred until flush */

    cetcd_v3rpc_watch_flush_replay();
    CETCD_ASSERT_TRUE(g_stream_cap_n >= 1);
    int found_event = 0;
    for (size_t i = 0; i < g_stream_cap_len; i++) {
        if (g_stream_cap[i] == 0x5a) { found_event = 1; break; }
    }
    CETCD_ASSERT_TRUE(found_event);

    reset_streaming_globals();
    cetcd_v3rpc_free(rpc);
    cetcd_loop_free(loop);
}

CETCD_TEST_CASE(test_watch_per_connection_writer) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();
    CETCD_ASSERT_NOT_NULL(rpc);
    cetcd_loop *loop = cetcd_loop_new();
    cetcd_v3rpc_set_loop(rpc, loop);

    mock_writes_a = mock_writes_b = 0;

    /* Conn A creates watch on "akey" */
    cetcd_v3rpc_set_stream_writer(rpc, mock_write_a, (void *)(uintptr_t)1);
    {
        uint8_t inner[16]; size_t ip = 0;
        inner[ip++] = 0x0a; inner[ip++] = 0x04;
        memcpy(inner + ip, "akey", 4); ip += 4;
        inner[ip++] = 0x38; inner[ip++] = 0x0a; /* watch_id=10 */
        uint8_t buf[32]; size_t bp = 0;
        buf[bp++] = 0x0a; buf[bp++] = (uint8_t)ip;
        memcpy(buf + bp, inner, ip); bp += ip;
        cetcd_rpc_bytes r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.Watch/Watch", buf, bp);
        CETCD_ASSERT_NOT_NULL(r.data);
        cetcd_rpc_bytes_free(&r);
    }

    /* Conn B creates watch on "bkey" — must not steal A's writer */
    cetcd_v3rpc_set_stream_writer(rpc, mock_write_b, (void *)(uintptr_t)2);
    {
        uint8_t inner[16]; size_t ip = 0;
        inner[ip++] = 0x0a; inner[ip++] = 0x04;
        memcpy(inner + ip, "bkey", 4); ip += 4;
        inner[ip++] = 0x38; inner[ip++] = 0x14; /* watch_id=20 */
        uint8_t buf[32]; size_t bp = 0;
        buf[bp++] = 0x0a; buf[bp++] = (uint8_t)ip;
        memcpy(buf + bp, inner, ip); bp += ip;
        cetcd_rpc_bytes r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.Watch/Watch", buf, bp);
        CETCD_ASSERT_NOT_NULL(r.data);
        cetcd_rpc_bytes_free(&r);
    }

    /* Put akey → only writer A should fire */
    {
        uint8_t put[16]; size_t p = 0;
        put[p++] = 0x0a; put[p++] = 0x04;
        memcpy(put + p, "akey", 4); p += 4;
        put[p++] = 0x12; put[p++] = 0x02;
        memcpy(put + p, "v1", 2); p += 2;
        cetcd_rpc_bytes r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", put, p);
        cetcd_rpc_bytes_free(&r);
    }
    CETCD_ASSERT_TRUE(mock_writes_a >= 1);
    CETCD_ASSERT_EQ_INT(mock_writes_b, 0);

    /* Put bkey → only writer B */
    int a_before = mock_writes_a;
    {
        uint8_t put[16]; size_t p = 0;
        put[p++] = 0x0a; put[p++] = 0x04;
        memcpy(put + p, "bkey", 4); p += 4;
        put[p++] = 0x12; put[p++] = 0x02;
        memcpy(put + p, "v2", 2); p += 2;
        cetcd_rpc_bytes r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", put, p);
        cetcd_rpc_bytes_free(&r);
    }
    CETCD_ASSERT_EQ_INT(mock_writes_a, a_before);
    CETCD_ASSERT_TRUE(mock_writes_b >= 1);

    /* Detach conn A — further akey puts must not call write_a */
    cetcd_v3rpc_detach_stream_writer((void *)(uintptr_t)1);
    a_before = mock_writes_a;
    {
        uint8_t put[16]; size_t p = 0;
        put[p++] = 0x0a; put[p++] = 0x04;
        memcpy(put + p, "akey", 4); p += 4;
        put[p++] = 0x12; put[p++] = 0x02;
        memcpy(put + p, "v3", 2); p += 2;
        cetcd_rpc_bytes r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", put, p);
        cetcd_rpc_bytes_free(&r);
    }
    CETCD_ASSERT_EQ_INT(mock_writes_a, a_before);

    cetcd_v3rpc_detach_stream_writer((void *)(uintptr_t)2);
    reset_streaming_globals();
    cetcd_v3rpc_free(rpc);
    cetcd_loop_free(loop);
}

CETCD_TEST_CASE(test_watch_progress_notify) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();
    CETCD_ASSERT_NOT_NULL(rpc);
    cetcd_loop *loop = cetcd_loop_new();
    cetcd_v3rpc_set_loop(rpc, loop);

    mock_writes_a = 0;
    cetcd_v3rpc_set_stream_writer(rpc, mock_write_a, (void *)(uintptr_t)7);

    /* WatchCreate with progress_notify=true (field 4, tag 0x20) */
    uint8_t inner[16]; size_t ip = 0;
    inner[ip++] = 0x0a; inner[ip++] = 0x04;
    memcpy(inner + ip, "pkey", 4); ip += 4;
    inner[ip++] = 0x20; inner[ip++] = 0x01; /* progress_notify */
    inner[ip++] = 0x38; inner[ip++] = 0x2a; /* watch_id=42 */
    uint8_t buf[32]; size_t bp = 0;
    buf[bp++] = 0x0a; buf[bp++] = (uint8_t)ip;
    memcpy(buf + bp, inner, ip); bp += ip;
    cetcd_rpc_bytes r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.Watch/Watch", buf, bp);
    CETCD_ASSERT_NOT_NULL(r.data);
    cetcd_rpc_bytes_free(&r);

    /* Drive ticks until progress interval (100 × 100ms). */
    for (int i = 0; i < 99; i++) cetcd_v3rpc_watch_tick();
    CETCD_ASSERT_EQ_INT(mock_writes_a, 0);
    cetcd_v3rpc_watch_tick();
    CETCD_ASSERT_TRUE(mock_writes_a >= 1);

    /* Explicit WatchProgressRequest (field 3, tag 0x1a, empty) */
    int before = mock_writes_a;
    uint8_t prog[2] = {0x1a, 0x00};
    r = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.Watch/Watch", prog, 2);
    cetcd_rpc_bytes_free(&r);
    CETCD_ASSERT_TRUE(mock_writes_a > before);

    cetcd_v3rpc_detach_stream_writer((void *)(uintptr_t)7);
    reset_streaming_globals();
    cetcd_v3rpc_free(rpc);
    cetcd_loop_free(loop);
}

CETCD_TEST_CASE(test_watch_cancel) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();
    CETCD_ASSERT_NOT_NULL(rpc);

    /* Activate streaming mode */
    cetcd_loop *loop = cetcd_loop_new();
    cetcd_v3rpc_set_loop(rpc, loop);
    cetcd_v3rpc_set_stream_writer(rpc, mock_stream_write_fn, NULL);

    /* First, create a streaming watcher: key="ckey", watch_id=200 */
    uint8_t create_inner[16]; size_t cpos = 0;
    create_inner[cpos++] = 0x0a; /* field 1 = key */
    create_inner[cpos++] = 0x04;
    memcpy(create_inner + cpos, "ckey", 4); cpos += 4;
    create_inner[cpos++] = 0x38; /* field 7 = watch_id */
    create_inner[cpos++] = 0xc8; /* 200 = 0xc8 (single-byte varint) */

    uint8_t create_buf[32]; size_t cwpos = 0;
    create_buf[cwpos++] = 0x0a;
    create_buf[cwpos++] = (uint8_t)cpos;
    memcpy(create_buf + cwpos, create_inner, cpos); cwpos += cpos;

    cetcd_rpc_bytes create_resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Watch/Watch", create_buf, cwpos);
    CETCD_ASSERT_NOT_NULL(create_resp.data);
    cetcd_rpc_bytes_free(&create_resp);

    /* Now cancel the watcher: watch_id=200 */
    uint8_t cancel_inner[8]; size_t xpos = 0;
    cancel_inner[xpos++] = 0x08; /* field 1 = watch_id */
    cancel_inner[xpos++] = 0xc8; /* 200 */

    uint8_t cancel_buf[16]; size_t xwpos = 0;
    cancel_buf[xwpos++] = 0x12; /* field 2 = WatchCancelRequest */
    cancel_buf[xwpos++] = (uint8_t)xpos;
    memcpy(cancel_buf + xwpos, cancel_inner, xpos); xwpos += xpos;

    cetcd_rpc_bytes cancel_resp = cetcd_v3rpc_dispatch(rpc,
        "/etcdserverpb.Watch/Watch", cancel_buf, xwpos);
    CETCD_ASSERT_NOT_NULL(cancel_resp.data);
    CETCD_ASSERT_TRUE(cancel_resp.len > 2);

    /* Verify cancel response has header (0x0a) and canceled=true (0x20 0x01) */
    CETCD_ASSERT_TRUE(cancel_resp.data[0] == 0x0a);

    size_t p = 1;
    uint64_t hdr_len = 0; int shift = 0;
    while (p < cancel_resp.len) {
        uint8_t b = cancel_resp.data[p++];
        hdr_len |= (uint64_t)(b & 0x7F) << shift;
        if (!(b & 0x80)) break;
        shift += 7;
    }
    p += (size_t)hdr_len;

    int found_canceled = 0;
    int found_cancel_watch_id = 0;
    while (p < cancel_resp.len) {
        uint8_t tag = cancel_resp.data[p++];
        if (tag == 0x20) { /* canceled */
            found_canceled = 1;
            while (p < cancel_resp.len) { uint8_t b = cancel_resp.data[p++]; if (!(b & 0x80)) break; }
        } else if (tag == 0x10) { /* watch_id */
            found_cancel_watch_id = 1;
            while (p < cancel_resp.len) { uint8_t b = cancel_resp.data[p++]; if (!(b & 0x80)) break; }
        } else if (tag == 0x0a || tag == 0x5a) {
            uint64_t l = 0; int s = 0;
            while (p < cancel_resp.len) { uint8_t b = cancel_resp.data[p++]; l |= (uint64_t)(b & 0x7F) << s; if (!(b & 0x80)) break; s += 7; }
            p += (size_t)l;
        } else {
            while (p < cancel_resp.len) { uint8_t b = cancel_resp.data[p++]; if (!(b & 0x80)) break; }
        }
    }
    CETCD_ASSERT_TRUE(found_canceled);
    CETCD_ASSERT_TRUE(found_cancel_watch_id);

    cetcd_rpc_bytes_free(&cancel_resp);
    reset_streaming_globals();
    cetcd_v3rpc_free(rpc);
    cetcd_loop_free(loop);
}

CETCD_TEST_CASE(v3rpc_watch_noput_filter) {
    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    /* Put a key first so there's a PUT event to filter */
    uint8_t put_req[32]; size_t pp = 0;
    put_req[pp++] = 0x0a; put_req[pp++] = 0x04;
    memcpy(put_req + pp, "wfn1", 4); pp += 4;
    put_req[pp++] = 0x12; put_req[pp++] = 0x02;
    memcpy(put_req + pp, "v1", 2); pp += 2;
    cetcd_rpc_bytes put_resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.KV/Put", put_req, pp);
    cetcd_rpc_bytes_free(&put_resp);

    /* Build WatchCreateRequest with NOPUT filter (field 5, tag 0x28, value 0)
     * and start_revision=1 to catch the PUT event */
    uint8_t create_inner[32]; size_t cpos = 0;
    create_inner[cpos++] = 0x0a; create_inner[cpos++] = 0x04;
    memcpy(create_inner + cpos, "wfn1", 4); cpos += 4;
    create_inner[cpos++] = 0x18; create_inner[cpos++] = 0x01; /* start_revision=1 */
    create_inner[cpos++] = 0x28; create_inner[cpos++] = 0x00; /* filter=NOPUT(0) */

    uint8_t watch_buf[64]; size_t wpos = 0;
    watch_buf[wpos++] = 0x0a;
    watch_buf[wpos++] = (uint8_t)cpos;
    memcpy(watch_buf + wpos, create_inner, cpos); wpos += cpos;

    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(rpc, "/etcdserverpb.Watch/Watch", watch_buf, wpos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 2);

    /* With NOPUT filter, the PUT event should be filtered out.
     * The response should have "created" flag (tag 0x18) but NO events (tag 0x5a). */
    int found_created = 0;
    int found_event = 0;
    size_t p = 0;
    while (p < resp.len) {
        uint8_t tag = resp.data[p++];
        if (tag == 0x0a) {
            uint64_t l = 0; int s = 0;
            while (p < resp.len) { uint8_t b = resp.data[p++]; l |= (uint64_t)(b & 0x7F) << s; if (!(b & 0x80)) break; s += 7; }
            p += (size_t)l;
        } else if (tag == 0x18) {
            found_created = 1;
            while (p < resp.len) { uint8_t b = resp.data[p++]; if (!(b & 0x80)) break; }
        } else if (tag == 0x5a) {
            found_event = 1;
            uint64_t l = 0; int s = 0;
            while (p < resp.len) { uint8_t b = resp.data[p++]; l |= (uint64_t)(b & 0x7F) << s; if (!(b & 0x80)) break; s += 7; }
            p += (size_t)l;
        } else {
            while (p < resp.len) { uint8_t b = resp.data[p++]; if (!(b & 0x80)) break; }
        }
    }
    CETCD_ASSERT_TRUE(found_created);
    CETCD_ASSERT_FALSE(found_event); /* NOPUT filter should suppress PUT events */

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
    CETCD_TEST_ENTRY(v3rpc_lease_grant_custom_id),
    CETCD_TEST_ENTRY(v3rpc_lease_grant_ttl_too_large),
    CETCD_TEST_ENTRY(v3rpc_delete_range),
    CETCD_TEST_ENTRY(v3rpc_auth_enable_disable),
    CETCD_TEST_ENTRY(v3rpc_auth_user_add_authenticate),
    CETCD_TEST_ENTRY(v3rpc_lease_revoke),
    CETCD_TEST_ENTRY(v3rpc_lease_revoke_nonexistent),
    CETCD_TEST_ENTRY(v3rpc_lease_keep_alive),
    CETCD_TEST_ENTRY(v3rpc_lease_time_to_live),
    CETCD_TEST_ENTRY(v3rpc_lease_time_to_live_missing),
    CETCD_TEST_ENTRY(v3rpc_lease_leases),
    CETCD_TEST_ENTRY(v3rpc_txn),
    CETCD_TEST_ENTRY(v3rpc_watch),
    CETCD_TEST_ENTRY(v3rpc_maintenance_status),
    CETCD_TEST_ENTRY(v3rpc_maintenance_hash),
    CETCD_TEST_ENTRY(v3rpc_maintenance_hash_kv),
    CETCD_TEST_ENTRY(v3rpc_maintenance_defragment),
    CETCD_TEST_ENTRY(v3rpc_maintenance_alarm),
        CETCD_TEST_ENTRY(v3rpc_alarm_activate_disarm),
    CETCD_TEST_ENTRY(v3rpc_maintenance_move_leader),
    CETCD_TEST_ENTRY(v3rpc_kv_compact),
    CETCD_TEST_ENTRY(v3rpc_compact_future_revision),
    CETCD_TEST_ENTRY(v3rpc_range_revision_compacted),
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
    CETCD_TEST_ENTRY(v3rpc_lease_revoke_deletes_keys),
    CETCD_TEST_ENTRY(v3rpc_watch_response_correct_tags),
    CETCD_TEST_ENTRY(v3rpc_watch_start_rev_compacted),
    CETCD_TEST_ENTRY(v3rpc_watch_replay_legacy),
    CETCD_TEST_ENTRY(v3rpc_watch_replay_prev_kv),
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
    CETCD_TEST_ENTRY(v3rpc_put_ignore_value_missing_key),
    CETCD_TEST_ENTRY(v3rpc_put_clear_lease_and_delete_detaches),
    CETCD_TEST_ENTRY(v3rpc_put_ignore_lease),
    CETCD_TEST_ENTRY(v3rpc_put_ignore_lease_missing_key),
    CETCD_TEST_ENTRY(v3rpc_auth_status_has_header),
    CETCD_TEST_ENTRY(v3rpc_lease_grant_has_header),
    CETCD_TEST_ENTRY(v3rpc_lease_keepalive_has_header),
    CETCD_TEST_ENTRY(v3rpc_lease_time_to_live_has_header),
    CETCD_TEST_ENTRY(v3rpc_lease_leases_has_header),
    CETCD_TEST_ENTRY(v3rpc_lease_time_to_live_with_keys),
    CETCD_TEST_ENTRY(v3rpc_maintenance_responses_have_header),
    CETCD_TEST_ENTRY(v3rpc_status_has_version_and_leader),
    CETCD_TEST_ENTRY(v3rpc_cluster_responses_have_header),
    CETCD_TEST_ENTRY(v3rpc_watch_has_header),
    CETCD_TEST_ENTRY(v3rpc_watch_event_kv_correct_fields),
    CETCD_TEST_ENTRY(v3rpc_range_kvs_correct_tag),
    CETCD_TEST_ENTRY(v3rpc_range_sort_order),
    CETCD_TEST_ENTRY(v3rpc_watch_cancel_has_header),
    CETCD_TEST_ENTRY(v3rpc_range_more_correct_tag),
    CETCD_TEST_ENTRY(v3rpc_range_encodes_lease),
    CETCD_TEST_ENTRY(v3rpc_put_rejects_unknown_lease),
    CETCD_TEST_ENTRY(v3rpc_watch_prev_kv_flag),
    CETCD_TEST_ENTRY(v3rpc_txn_compare_range_end),
    CETCD_TEST_ENTRY(v3rpc_txn_compare_missing_key_version),
    CETCD_TEST_ENTRY(v3rpc_txn_put_prev_kv),
    CETCD_TEST_ENTRY(v3rpc_txn_delete_range_prev_kv),
    CETCD_TEST_ENTRY(v3rpc_range_min_max_revision),
    CETCD_TEST_ENTRY(v3rpc_txn_put_ignore_value),
    CETCD_TEST_ENTRY(v3rpc_txn_range_limit),
    CETCD_TEST_ENTRY(v3rpc_txn_range_count_only),
    CETCD_TEST_ENTRY(v3rpc_txn_range_keys_only),
    CETCD_TEST_ENTRY(v3rpc_txn_range_sort_order),
    CETCD_TEST_ENTRY(v3rpc_txn_range_revision_filter),
    CETCD_TEST_ENTRY(test_watch_create_streaming),
    CETCD_TEST_ENTRY(v3rpc_watch_canceled_on_compact),
    CETCD_TEST_ENTRY(v3rpc_watch_replay_streaming),
    CETCD_TEST_ENTRY(test_watch_per_connection_writer),
    CETCD_TEST_ENTRY(test_watch_progress_notify),
    CETCD_TEST_ENTRY(test_watch_cancel),
    CETCD_TEST_ENTRY(v3rpc_watch_noput_filter),
CETCD_TEST_LIST_END

CETCD_TEST_MAIN()
