#include "cetcd/base.h"
#include "cetcd/server.h"
#include "cetcd/v3rpc.h"
#include "cetcd/raft.h"
#include "cetcd/mvcc.h"
#include "cetcd/lease.h"
#include "cetcd/auth.h"
#include "cetcd/peer.h"
#include "cetcd/snap.h"
#include "cetcd_test.h"

CETCD_TEST_CASE(server_create_destroy) {
    cetcd_server_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.node_id = 1;
    strncpy(cfg.listen_addr, "127.0.0.1", sizeof(cfg.listen_addr) - 1);
    cfg.listen_port = 2379;
    strncpy(cfg.peer_addr, "127.0.0.1", sizeof(cfg.peer_addr) - 1);
    cfg.peer_port = 2380;
    cfg.election_tick = 10;
    cfg.heartbeat_tick = 1;

    cetcd_server *srv = cetcd_server_new(&cfg);
    CETCD_ASSERT_NOT_NULL(srv);
    CETCD_ASSERT_EQ_INT((int)cetcd_server_node_id(srv), 1);
    cetcd_server_free(srv);
}

CETCD_TEST_CASE(server_handle_rpc_put_range) {
    cetcd_server_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.node_id = 1;
    cfg.listen_port = 2379;
    cfg.election_tick = 10;
    cfg.heartbeat_tick = 1;

    cetcd_server *srv = cetcd_server_new(&cfg);
    CETCD_ASSERT_NOT_NULL(srv);

    uint8_t put_buf[64];
    size_t pos = 0;
    put_buf[pos++] = 0x0a; put_buf[pos++] = 0x03;
    memcpy(put_buf + pos, "key", 3); pos += 3;
    put_buf[pos++] = 0x12; put_buf[pos++] = 0x03;
    memcpy(put_buf + pos, "val", 3); pos += 3;

    cetcd_server_rpc_result resp =
        cetcd_server_handle_rpc(srv, "/etcdserverpb.KV/Put", put_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 0);
    cetcd_server_rpc_result_free(&resp);

    uint8_t range_buf[16];
    pos = 0;
    range_buf[pos++] = 0x0a; range_buf[pos++] = 0x03;
    memcpy(range_buf + pos, "key", 3); pos += 3;

    resp = cetcd_server_handle_rpc(srv, "/etcdserverpb.KV/Range", range_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 0);
    cetcd_server_rpc_result_free(&resp);

    CETCD_ASSERT_TRUE(cetcd_server_revision(srv) > 0);

    cetcd_server_free(srv);
}

CETCD_TEST_CASE(server_handle_rpc_auth) {
    cetcd_server_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.node_id = 1;
    cfg.listen_port = 2379;

    cetcd_server *srv = cetcd_server_new(&cfg);
    CETCD_ASSERT_NOT_NULL(srv);

    uint8_t enable_buf[] = {0x00};
    cetcd_server_rpc_result resp =
        cetcd_server_handle_rpc(srv, "/etcdserverpb.Auth/AuthEnable",
                                enable_buf, sizeof(enable_buf));
    CETCD_ASSERT_NOT_NULL(resp.data);
    cetcd_server_rpc_result_free(&resp);

    cetcd_server_free(srv);
}

CETCD_TEST_CASE(server_compact) {
    cetcd_server_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.node_id = 1;
    cfg.listen_port = 2379;

    cetcd_server *srv = cetcd_server_new(&cfg);

    uint8_t put_buf[64];
    size_t pos = 0;
    put_buf[pos++] = 0x0a; put_buf[pos++] = 0x01;
    memcpy(put_buf + pos, "x", 1); pos += 1;
    put_buf[pos++] = 0x12; put_buf[pos++] = 0x01;
    memcpy(put_buf + pos, "1", 1); pos += 1;
    cetcd_server_rpc_result resp =
        cetcd_server_handle_rpc(srv, "/etcdserverpb.KV/Put", put_buf, pos);
    cetcd_server_rpc_result_free(&resp);

    int64_t rev = cetcd_server_revision(srv);
    CETCD_ASSERT_TRUE(rev > 0);

    CETCD_ASSERT_EQ_INT(cetcd_server_compact(srv, rev), 0);

    cetcd_server_free(srv);
}

CETCD_TEST_CASE(server_handle_rpc_empty_key_fails) {
    cetcd_server_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.node_id = 1;
    cfg.listen_port = 2379;
    cfg.election_tick = 10;
    cfg.heartbeat_tick = 1;

    cetcd_server *srv = cetcd_server_new(&cfg);
    CETCD_ASSERT_NOT_NULL(srv);

    /* Put with empty key → domain error {NULL,0} (server must still TCP-reply) */
    uint8_t put_buf[16];
    size_t pos = 0;
    put_buf[pos++] = 0x0a; put_buf[pos++] = 0x00;
    put_buf[pos++] = 0x12; put_buf[pos++] = 0x01; put_buf[pos++] = 'x';
    cetcd_server_rpc_result resp =
        cetcd_server_handle_rpc(srv, "/etcdserverpb.KV/Put", put_buf, pos);
    CETCD_ASSERT_TRUE(resp.data == NULL || resp.len == 0);
    cetcd_server_rpc_result_free(&resp);

    cetcd_server_free(srv);
}

CETCD_TEST_CASE(server_snapshot) {
    cetcd_server_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.node_id = 1;
    cfg.listen_port = 2379;

    cetcd_server *srv = cetcd_server_new(&cfg);

    uint8_t put_buf[64];
    size_t pos = 0;
    put_buf[pos++] = 0x0a; put_buf[pos++] = 0x01;
    memcpy(put_buf + pos, "k", 1); pos += 1;
    put_buf[pos++] = 0x12; put_buf[pos++] = 0x01;
    memcpy(put_buf + pos, "v", 1); pos += 1;
    cetcd_server_rpc_result resp =
        cetcd_server_handle_rpc(srv, "/etcdserverpb.KV/Put", put_buf, pos);
    cetcd_server_rpc_result_free(&resp);

    cetcd_snap *snap = cetcd_server_snapshot(srv);
    CETCD_ASSERT_NOT_NULL(snap);
    CETCD_ASSERT_TRUE(cetcd_snap_entry_count(snap) >= 1);
    cetcd_snap_free(snap);

    cetcd_server_free(srv);
}

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(server_create_destroy),
    CETCD_TEST_ENTRY(server_handle_rpc_put_range),
    CETCD_TEST_ENTRY(server_handle_rpc_auth),
    CETCD_TEST_ENTRY(server_handle_rpc_empty_key_fails),
    CETCD_TEST_ENTRY(server_compact),
    CETCD_TEST_ENTRY(server_snapshot),
CETCD_TEST_LIST_END

CETCD_TEST_MAIN()
