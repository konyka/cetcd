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

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(v3rpc_create_destroy),
    CETCD_TEST_ENTRY(v3rpc_put_range),
    CETCD_TEST_ENTRY(v3rpc_unknown_path),
    CETCD_TEST_ENTRY(v3rpc_lease_grant_revoke),
    CETCD_TEST_ENTRY(v3rpc_delete_range),
    CETCD_TEST_ENTRY(v3rpc_auth_enable_disable),
    CETCD_TEST_ENTRY(v3rpc_auth_user_add_authenticate),
CETCD_TEST_LIST_END

CETCD_TEST_MAIN()
