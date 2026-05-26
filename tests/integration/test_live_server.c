#include "cetcd/base.h"
#include "cetcd/server.h"
#include "cetcd/io.h"
#include "cetcd/snap.h"
#include "cetcd_test.h"

CETCD_TEST_CASE(live_server_start_stop) {
    cetcd_server_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.node_id = 1;
    strncpy(cfg.listen_addr, "127.0.0.1", sizeof(cfg.listen_addr) - 1);
    cfg.listen_port = 23790;
    cfg.election_tick = 10;
    cfg.heartbeat_tick = 1;

    cetcd_server *srv = cetcd_server_new(&cfg);
    CETCD_ASSERT_NOT_NULL(srv);
    CETCD_ASSERT_EQ_INT(cetcd_server_start(srv), 0);

    cetcd_server_rpc_result resp = cetcd_server_handle_rpc(srv,
        "/etcdserverpb.KV/Put",
        (const uint8_t *)"\x0a\x01k\x12\x01v", 6);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 0);
    cetcd_server_rpc_result_free(&resp);

    CETCD_ASSERT_TRUE(cetcd_server_revision(srv) > 0);

    cetcd_server_stop(srv);
    cetcd_server_free(srv);
}

CETCD_TEST_CASE(live_server_snapshot_after_writes) {
    cetcd_server_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.node_id = 1;
    cfg.listen_port = 23791;

    cetcd_server *srv = cetcd_server_new(&cfg);
    cetcd_server_start(srv);

    for (int i = 0; i < 10; i++) {
        uint8_t buf[16];
        size_t pos = 0;
        buf[pos++] = 0x0a; buf[pos++] = 0x01;
        buf[pos++] = (uint8_t)('a' + i);
        buf[pos++] = 0x12; buf[pos++] = 0x01;
        buf[pos++] = (uint8_t)('0' + i);
        cetcd_server_rpc_result resp =
            cetcd_server_handle_rpc(srv, "/etcdserverpb.KV/Put", buf, pos);
        cetcd_server_rpc_result_free(&resp);
    }

    int64_t rev = cetcd_server_revision(srv);
    CETCD_ASSERT_TRUE(rev >= 10);

    cetcd_snap *snap = cetcd_server_snapshot(srv);
    CETCD_ASSERT_NOT_NULL(snap);
    CETCD_ASSERT_TRUE(cetcd_snap_entry_count(snap) >= 10);
    cetcd_snap_free(snap);

    CETCD_ASSERT_EQ_INT(cetcd_server_compact(srv, rev), 0);

    cetcd_server_stop(srv);
    cetcd_server_free(srv);
}

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(live_server_start_stop),
    CETCD_TEST_ENTRY(live_server_snapshot_after_writes),
CETCD_TEST_LIST_END

CETCD_TEST_MAIN()
