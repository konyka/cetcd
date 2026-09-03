#define _POSIX_C_SOURCE 200809L
#include "cetcd/base.h"
#include "cetcd/server.h"
#include "cetcd/v3rpc.h"
#include "cetcd/raft.h"
#include "cetcd/mvcc.h"
#include "cetcd/lease.h"
#include "cetcd/auth.h"
#include "cetcd/peer.h"
#include "cetcd/snap.h"
#include "cetcd/wal.h"
#include "cetcd_test.h"

#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <stdio.h>
#include <stdlib.h>

static int make_selfsigned_(char *dir, size_t dirsz) {
    char tmpl[] = "/tmp/cetcd-srv-tls-XXXXXX";
    if (!mkdtemp(tmpl)) return -1;
    snprintf(dir, dirsz, "%s", tmpl);
    char cmd[1024];
    snprintf(cmd, sizeof(cmd),
             "openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 "
             "-days 1 -nodes -subj /CN=localhost "
             "-keyout '%s/key.pem' -out '%s/cert.pem' >/dev/null 2>&1",
             dir, dir);
    return system(cmd) == 0 ? 0 : -1;
}

static void cleanup_selfsigned_(const char *dir) {
    char path[300];
    snprintf(path, sizeof(path), "%s/cert.pem", dir);
    unlink(path);
    snprintf(path, sizeof(path), "%s/key.pem", dir);
    unlink(path);
    rmdir(dir);
}

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
    /* AuthEnable requires a root user (etcd). */
    uint8_t add_buf[32];
    size_t ap = 0;
    add_buf[ap++] = 0x0a; add_buf[ap++] = 0x04;
    memcpy(add_buf + ap, "root", 4); ap += 4;
    add_buf[ap++] = 0x12; add_buf[ap++] = 0x03;
    memcpy(add_buf + ap, "pw1", 3); ap += 3;
    cetcd_server_rpc_result add =
        cetcd_server_handle_rpc(srv, "/etcdserverpb.Auth/UserAdd", add_buf, ap);
    CETCD_ASSERT_NOT_NULL(add.data);
    cetcd_server_rpc_result_free(&add);

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

CETCD_TEST_CASE(server_raft_put_restart_keeps_revision) {
    char data_dir[] = "/tmp/cetcd-test-raftkv-XXXXXX";
    CETCD_ASSERT_NOT_NULL(mkdtemp(data_dir));

    cetcd_server_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.node_id = 1;
    cfg.listen_port = 2379;
    cfg.election_tick = 10;
    cfg.heartbeat_tick = 1;
    strncpy(cfg.data_dir, data_dir, sizeof(cfg.data_dir) - 1);

    cetcd_server *srv = cetcd_server_new(&cfg);
    CETCD_ASSERT_NOT_NULL(srv);
    CETCD_ASSERT_EQ_INT(cetcd_server_start(srv), 0);

    uint8_t put_buf[16];
    size_t pos = 0;
    put_buf[pos++] = 0x0a; put_buf[pos++] = 0x03;
    memcpy(put_buf + pos, "foo", 3); pos += 3;
    put_buf[pos++] = 0x12; put_buf[pos++] = 0x03;
    memcpy(put_buf + pos, "bar", 3); pos += 3;
    cetcd_server_rpc_result resp =
        cetcd_server_handle_rpc(srv, "/etcdserverpb.KV/Put", put_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    cetcd_server_rpc_result_free(&resp);
    int64_t rev = cetcd_server_revision(srv);
    CETCD_ASSERT_TRUE(rev > 0);
    cetcd_server_stop(srv);
    cetcd_server_free(srv);

    memset(&cfg, 0, sizeof(cfg));
    cfg.node_id = 1;
    cfg.listen_port = 2379;
    cfg.election_tick = 10;
    cfg.heartbeat_tick = 1;
    strncpy(cfg.data_dir, data_dir, sizeof(cfg.data_dir) - 1);
    srv = cetcd_server_new(&cfg);
    CETCD_ASSERT_EQ_INT(cetcd_server_start(srv), 0);
    CETCD_ASSERT_EQ_INT((int)cetcd_server_revision(srv), (int)rev);

    uint8_t range_buf[8];
    pos = 0;
    range_buf[pos++] = 0x0a; range_buf[pos++] = 0x03;
    memcpy(range_buf + pos, "foo", 3); pos += 3;
    resp = cetcd_server_handle_rpc(srv, "/etcdserverpb.KV/Range", range_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    int found = 0;
    for (size_t i = 0; i + 3 <= resp.len; i++) {
        if (memcmp(resp.data + i, "bar", 3) == 0) { found = 1; break; }
    }
    CETCD_ASSERT_TRUE(found);
    cetcd_server_rpc_result_free(&resp);
    cetcd_server_free(srv);
}

CETCD_TEST_CASE(server_wal_replay_when_mvcc_empty) {
    char data_dir[] = "/tmp/cetcd-test-walgap-XXXXXX";
    CETCD_ASSERT_NOT_NULL(mkdtemp(data_dir));
    char wal_dir[600];
    snprintf(wal_dir, sizeof(wal_dir), "%s/wal", data_dir);
    CETCD_ASSERT_EQ_INT(mkdir(wal_dir, 0755), 0);

    uint8_t *payload = NULL;
    size_t plen = 0;
    CETCD_ASSERT_EQ_INT(cetcd_apply_encode_put(&payload, &plen,
        (const uint8_t *)"rk", 2, (const uint8_t *)"rv", 2, 0), 0);
    cetcd_entry e;
    memset(&e, 0, sizeof(e));
    e.term = 1;
    e.index = 1;
    e.type = CETCD_ENTRY_NORMAL;
    e.data = cetcd_slice_make(payload, plen);

    cetcd_wal_encoder *enc = cetcd_wal_encoder_create(wal_dir);
    CETCD_ASSERT_NOT_NULL(enc);
    CETCD_ASSERT_EQ_INT(cetcd_wal_encode_entry(enc, &e), 0);
    cetcd_hard_state hs = {1, 1, 1};
    CETCD_ASSERT_EQ_INT(cetcd_wal_encode_hard_state(enc, &hs), 0);
    CETCD_ASSERT_EQ_INT(cetcd_wal_encoder_sync(enc), 0);
    cetcd_wal_encoder_free(enc);
    free(payload);

    cetcd_server_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.node_id = 1;
    cfg.listen_port = 2379;
    cfg.election_tick = 10;
    cfg.heartbeat_tick = 1;
    strncpy(cfg.data_dir, data_dir, sizeof(cfg.data_dir) - 1);
    cetcd_server *srv = cetcd_server_new(&cfg);
    CETCD_ASSERT_EQ_INT(cetcd_server_start(srv), 0);
    CETCD_ASSERT_TRUE(cetcd_server_revision(srv) > 0);

    uint8_t range_buf[8];
    size_t pos = 0;
    range_buf[pos++] = 0x0a; range_buf[pos++] = 0x02;
    memcpy(range_buf + pos, "rk", 2); pos += 2;
    cetcd_server_rpc_result resp =
        cetcd_server_handle_rpc(srv, "/etcdserverpb.KV/Range", range_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    int found = 0;
    for (size_t i = 0; i + 2 <= resp.len; i++) {
        if (memcmp(resp.data + i, "rv", 2) == 0) { found = 1; break; }
    }
    CETCD_ASSERT_TRUE(found);
    cetcd_server_rpc_result_free(&resp);
    cetcd_server_free(srv);
}

CETCD_TEST_CASE(server_txn_put_survives_restart) {
    char data_dir[] = "/tmp/cetcd-test-txnraft-XXXXXX";
    CETCD_ASSERT_NOT_NULL(mkdtemp(data_dir));

    cetcd_server_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.node_id = 1;
    cfg.listen_port = 2379;
    cfg.election_tick = 10;
    cfg.heartbeat_tick = 1;
    strncpy(cfg.data_dir, data_dir, sizeof(cfg.data_dir) - 1);

    cetcd_server *srv = cetcd_server_new(&cfg);
    CETCD_ASSERT_EQ_INT(cetcd_server_start(srv), 0);

    /* Txn success = Put key="tk" value="tv" */
    uint8_t put_inner[16];
    size_t ppos = 0;
    put_inner[ppos++] = 0x0a; put_inner[ppos++] = 0x02;
    memcpy(put_inner + ppos, "tk", 2); ppos += 2;
    put_inner[ppos++] = 0x12; put_inner[ppos++] = 0x02;
    memcpy(put_inner + ppos, "tv", 2); ppos += 2;
    uint8_t op_buf[32];
    size_t opos = 0;
    op_buf[opos++] = 0x12;
    op_buf[opos++] = (uint8_t)ppos;
    memcpy(op_buf + opos, put_inner, ppos); opos += ppos;
    uint8_t txn_buf[64];
    size_t tpos = 0;
    txn_buf[tpos++] = 0x12;
    txn_buf[tpos++] = (uint8_t)opos;
    memcpy(txn_buf + tpos, op_buf, opos); tpos += opos;

    cetcd_server_rpc_result resp =
        cetcd_server_handle_rpc(srv, "/etcdserverpb.KV/Txn", txn_buf, tpos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    cetcd_server_rpc_result_free(&resp);
    CETCD_ASSERT_TRUE(cetcd_server_revision(srv) > 0);
    cetcd_server_stop(srv);
    cetcd_server_free(srv);

    memset(&cfg, 0, sizeof(cfg));
    cfg.node_id = 1;
    cfg.listen_port = 2379;
    cfg.election_tick = 10;
    cfg.heartbeat_tick = 1;
    strncpy(cfg.data_dir, data_dir, sizeof(cfg.data_dir) - 1);
    srv = cetcd_server_new(&cfg);
    CETCD_ASSERT_EQ_INT(cetcd_server_start(srv), 0);

    uint8_t range_buf[8];
    size_t pos = 0;
    range_buf[pos++] = 0x0a; range_buf[pos++] = 0x02;
    memcpy(range_buf + pos, "tk", 2); pos += 2;
    resp = cetcd_server_handle_rpc(srv, "/etcdserverpb.KV/Range", range_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    int found = 0;
    for (size_t i = 0; i + 2 <= resp.len; i++) {
        if (memcmp(resp.data + i, "tv", 2) == 0) { found = 1; break; }
    }
    CETCD_ASSERT_TRUE(found);
    cetcd_server_rpc_result_free(&resp);
    cetcd_server_free(srv);
}

CETCD_TEST_CASE(server_lease_survives_restart) {
    char data_dir[] = "/tmp/cetcd-test-lease-rst-XXXXXX";
    CETCD_ASSERT_NOT_NULL(mkdtemp(data_dir));

    cetcd_server_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.node_id = 1;
    cfg.listen_port = 2379;
    cfg.election_tick = 10;
    cfg.heartbeat_tick = 1;
    strncpy(cfg.data_dir, data_dir, sizeof(cfg.data_dir) - 1);

    cetcd_server *srv = cetcd_server_new(&cfg);
    CETCD_ASSERT_EQ_INT(cetcd_server_start(srv), 0);

    /* LeaseGrant TTL=60 ID=7 */
    uint8_t grant[8];
    size_t gpos = 0;
    grant[gpos++] = 0x08; grant[gpos++] = 60;
    grant[gpos++] = 0x10; grant[gpos++] = 7;
    cetcd_server_rpc_result resp =
        cetcd_server_handle_rpc(srv, "/etcdserverpb.Lease/LeaseGrant", grant, gpos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    cetcd_server_rpc_result_free(&resp);

    /* Put key="lk" value="lv" lease=7 */
    uint8_t put_buf[16];
    size_t pos = 0;
    put_buf[pos++] = 0x0a; put_buf[pos++] = 0x02;
    memcpy(put_buf + pos, "lk", 2); pos += 2;
    put_buf[pos++] = 0x12; put_buf[pos++] = 0x02;
    memcpy(put_buf + pos, "lv", 2); pos += 2;
    put_buf[pos++] = 0x18; put_buf[pos++] = 7;
    resp = cetcd_server_handle_rpc(srv, "/etcdserverpb.KV/Put", put_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    cetcd_server_rpc_result_free(&resp);
    cetcd_server_stop(srv);
    cetcd_server_free(srv);

    memset(&cfg, 0, sizeof(cfg));
    cfg.node_id = 1;
    cfg.listen_port = 2379;
    cfg.election_tick = 10;
    cfg.heartbeat_tick = 1;
    strncpy(cfg.data_dir, data_dir, sizeof(cfg.data_dir) - 1);
    srv = cetcd_server_new(&cfg);
    CETCD_ASSERT_EQ_INT(cetcd_server_start(srv), 0);

    uint8_t ttl_req[4];
    size_t tpos = 0;
    ttl_req[tpos++] = 0x08; ttl_req[tpos++] = 7;
    resp = cetcd_server_handle_rpc(srv, "/etcdserverpb.Lease/LeaseTimeToLive",
                                   ttl_req, tpos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    int granted60 = 0;
    for (size_t i = 0; i + 1 < resp.len; i++) {
        if (resp.data[i] == 0x20 && resp.data[i + 1] == 60) { granted60 = 1; break; }
    }
    CETCD_ASSERT_TRUE(granted60);
    cetcd_server_rpc_result_free(&resp);

    uint8_t range_buf[8];
    pos = 0;
    range_buf[pos++] = 0x0a; range_buf[pos++] = 0x02;
    memcpy(range_buf + pos, "lk", 2); pos += 2;
    resp = cetcd_server_handle_rpc(srv, "/etcdserverpb.KV/Range", range_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    int found = 0;
    for (size_t i = 0; i + 2 <= resp.len; i++) {
        if (memcmp(resp.data + i, "lv", 2) == 0) { found = 1; break; }
    }
    CETCD_ASSERT_TRUE(found);
    cetcd_server_rpc_result_free(&resp);
    cetcd_server_free(srv);
}

CETCD_TEST_CASE(server_snapshot_count_truncates_wal) {
    char data_dir[] = "/tmp/cetcd-test-snapwal-XXXXXX";
    CETCD_ASSERT_NOT_NULL(mkdtemp(data_dir));

    cetcd_server_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.node_id = 1;
    cfg.listen_port = 2379;
    cfg.election_tick = 10;
    cfg.heartbeat_tick = 1;
    cfg.snapshot_count = 1;
    strncpy(cfg.data_dir, data_dir, sizeof(cfg.data_dir) - 1);

    cetcd_server *srv = cetcd_server_new(&cfg);
    CETCD_ASSERT_EQ_INT(cetcd_server_start(srv), 0);

    const char *keys[] = {"s1", "s2"};
    for (int i = 0; i < 2; i++) {
        uint8_t put_buf[16];
        size_t pos = 0;
        put_buf[pos++] = 0x0a; put_buf[pos++] = 0x02;
        memcpy(put_buf + pos, keys[i], 2); pos += 2;
        put_buf[pos++] = 0x12; put_buf[pos++] = 0x01;
        put_buf[pos++] = (uint8_t)('x' + i);
        cetcd_server_rpc_result resp =
            cetcd_server_handle_rpc(srv, "/etcdserverpb.KV/Put", put_buf, pos);
        CETCD_ASSERT_NOT_NULL(resp.data);
        cetcd_server_rpc_result_free(&resp);
    }
    cetcd_server_stop(srv);
    cetcd_server_free(srv);

    char wal_dir[600];
    snprintf(wal_dir, sizeof(wal_dir), "%s/wal", data_dir);
    cetcd_wal_decoder *dec = cetcd_wal_decoder_open(wal_dir);
    CETCD_ASSERT_NOT_NULL(dec);
    cetcd_wal_record rec;
    CETCD_ASSERT_EQ_INT(cetcd_wal_decode(dec, &rec), 0);
    CETCD_ASSERT_EQ_INT(rec.type, CETCD_WAL_SNAPSHOT);
    uint64_t idx = 0, term = 0;
    CETCD_ASSERT_EQ_INT(cetcd_wal_decode_snapshot(rec.data, rec.data_len, &idx, &term), 0);
    CETCD_ASSERT_TRUE(idx >= 1);
    cetcd_wal_record_free(&rec);
    cetcd_wal_decoder_free(dec);

    memset(&cfg, 0, sizeof(cfg));
    cfg.node_id = 1;
    cfg.listen_port = 2379;
    cfg.election_tick = 10;
    cfg.heartbeat_tick = 1;
    cfg.snapshot_count = 1;
    strncpy(cfg.data_dir, data_dir, sizeof(cfg.data_dir) - 1);
    srv = cetcd_server_new(&cfg);
    CETCD_ASSERT_EQ_INT(cetcd_server_start(srv), 0);

    for (int i = 0; i < 2; i++) {
        uint8_t range_buf[8];
        size_t pos = 0;
        range_buf[pos++] = 0x0a; range_buf[pos++] = 0x02;
        memcpy(range_buf + pos, keys[i], 2); pos += 2;
        cetcd_server_rpc_result resp =
            cetcd_server_handle_rpc(srv, "/etcdserverpb.KV/Range", range_buf, pos);
        CETCD_ASSERT_NOT_NULL(resp.data);
        int found = 0;
        uint8_t expect = (uint8_t)('x' + i);
        for (size_t j = 0; j < resp.len; j++) {
            if (resp.data[j] == expect) { found = 1; break; }
        }
        CETCD_ASSERT_TRUE(found);
        cetcd_server_rpc_result_free(&resp);
    }
    cetcd_server_free(srv);
}

CETCD_TEST_CASE(server_member_add_survives_restart) {
    char data_dir[] = "/tmp/cetcd-test-memb-XXXXXX";
    CETCD_ASSERT_NOT_NULL(mkdtemp(data_dir));

    cetcd_server_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.node_id = 1;
    cfg.listen_port = 2379;
    cfg.election_tick = 10;
    cfg.heartbeat_tick = 1;
    strncpy(cfg.data_dir, data_dir, sizeof(cfg.data_dir) - 1);

    cetcd_server *srv = cetcd_server_new(&cfg);
    CETCD_ASSERT_EQ_INT(cetcd_server_start(srv), 0);

    uint8_t add_buf[32];
    size_t pos = 0;
    add_buf[pos++] = 0x0a;
    add_buf[pos++] = 0x0e;
    memcpy(add_buf + pos, "127.0.0.1:2382", 14); pos += 14;
    cetcd_server_rpc_result resp =
        cetcd_server_handle_rpc(srv, "/etcdserverpb.Cluster/MemberAdd", add_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    cetcd_server_rpc_result_free(&resp);
    CETCD_ASSERT_EQ_INT((int)cetcd_server_peer_count(srv), 1);
    cetcd_server_stop(srv);
    cetcd_server_free(srv);

    memset(&cfg, 0, sizeof(cfg));
    cfg.node_id = 1;
    cfg.listen_port = 2379;
    cfg.election_tick = 10;
    cfg.heartbeat_tick = 1;
    strncpy(cfg.data_dir, data_dir, sizeof(cfg.data_dir) - 1);
    srv = cetcd_server_new(&cfg);
    CETCD_ASSERT_EQ_INT(cetcd_server_start(srv), 0);
    CETCD_ASSERT_EQ_INT((int)cetcd_server_peer_count(srv), 1);
    cetcd_server_free(srv);
}

CETCD_TEST_CASE(server_start_rejects_cert_without_key) {
    cetcd_server_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.node_id = 1;
    cfg.listen_port = 2379;
    cfg.election_tick = 10;
    cfg.heartbeat_tick = 1;
    strncpy(cfg.cert_file, "/tmp/cetcd-missing.pem", sizeof(cfg.cert_file) - 1);

    cetcd_server *srv = cetcd_server_new(&cfg);
    CETCD_ASSERT_NOT_NULL(srv);
    CETCD_ASSERT_EQ_INT(cetcd_server_start(srv), CETCD_ERR_INVAL);
    cetcd_server_free(srv);
}

CETCD_TEST_CASE(server_start_rejects_missing_tls_files) {
    cetcd_server_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.node_id = 1;
    cfg.listen_port = 2379;
    cfg.election_tick = 10;
    cfg.heartbeat_tick = 1;
    strncpy(cfg.cert_file, "/nonexistent/cetcd-cert.pem", sizeof(cfg.cert_file) - 1);
    strncpy(cfg.key_file, "/nonexistent/cetcd-key.pem", sizeof(cfg.key_file) - 1);

    cetcd_server *srv = cetcd_server_new(&cfg);
    CETCD_ASSERT_NOT_NULL(srv);
    int rc = cetcd_server_start(srv);
    CETCD_ASSERT_TRUE(rc == CETCD_ERR_IO || rc == CETCD_ERR_UNSUPPORT);
    cetcd_server_free(srv);
}

CETCD_TEST_CASE(server_start_rejects_client_auth_without_ca) {
    cetcd_server_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.node_id = 1;
    cfg.listen_port = 2379;
    cfg.election_tick = 10;
    cfg.heartbeat_tick = 1;
    strncpy(cfg.cert_file, "/nonexistent/cetcd-cert.pem", sizeof(cfg.cert_file) - 1);
    strncpy(cfg.key_file, "/nonexistent/cetcd-key.pem", sizeof(cfg.key_file) - 1);
    cfg.client_cert_auth = true;

    cetcd_server *srv = cetcd_server_new(&cfg);
    CETCD_ASSERT_NOT_NULL(srv);
    CETCD_ASSERT_EQ_INT(cetcd_server_start(srv), CETCD_ERR_INVAL);
    cetcd_server_free(srv);
}

CETCD_TEST_CASE(server_start_loads_peer_tls) {
    char dir[128];
    CETCD_ASSERT_EQ_INT(make_selfsigned_(dir, sizeof(dir)), 0);
    char cert[300], key[300];
    snprintf(cert, sizeof(cert), "%s/cert.pem", dir);
    snprintf(key, sizeof(key), "%s/key.pem", dir);

    cetcd_server_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.node_id = 1;
    cfg.listen_port = 2379;
    cfg.election_tick = 10;
    cfg.heartbeat_tick = 1;
    strncpy(cfg.peer_cert_file, cert, sizeof(cfg.peer_cert_file) - 1);
    strncpy(cfg.peer_key_file, key, sizeof(cfg.peer_key_file) - 1);

    cetcd_server *srv = cetcd_server_new(&cfg);
    CETCD_ASSERT_NOT_NULL(srv);
    CETCD_ASSERT_EQ_INT(cetcd_server_start(srv), 0);
    cetcd_server_free(srv);
    cleanup_selfsigned_(dir);
}

CETCD_TEST_CASE(server_start_rejects_jwt_without_key) {
    cetcd_server_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.node_id = 1;
    cfg.listen_port = 2379;
    cfg.election_tick = 10;
    cfg.heartbeat_tick = 1;
    strncpy(cfg.auth_token, "jwt", sizeof(cfg.auth_token) - 1);

    cetcd_server *srv = cetcd_server_new(&cfg);
    CETCD_ASSERT_NOT_NULL(srv);
    CETCD_ASSERT_EQ_INT(cetcd_server_start(srv), CETCD_ERR_INVAL);
    cetcd_server_free(srv);
}

CETCD_TEST_CASE(server_start_rejects_jwt_rs256) {
    cetcd_server_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.node_id = 1;
    cfg.listen_port = 2379;
    cfg.election_tick = 10;
    cfg.heartbeat_tick = 1;
    strncpy(cfg.auth_token, "jwt,sign-method=RS256,priv-key=/dev/null",
            sizeof(cfg.auth_token) - 1);

    cetcd_server *srv = cetcd_server_new(&cfg);
    CETCD_ASSERT_NOT_NULL(srv);
    CETCD_ASSERT_EQ_INT(cetcd_server_start(srv), CETCD_ERR_UNSUPPORT);
    cetcd_server_free(srv);
}

CETCD_TEST_CASE(server_start_accepts_jwt_hs256) {
    char tmpl[] = "/tmp/cetcd-srv-jwt-XXXXXX";
    CETCD_ASSERT_NOT_NULL(mkdtemp(tmpl));
    char path[300];
    snprintf(path, sizeof(path), "%s/key", tmpl);
    FILE *f = fopen(path, "wb");
    CETCD_ASSERT_NOT_NULL(f);
    const char secret[] = "server-hs256";
    fwrite(secret, 1, sizeof(secret) - 1, f);
    fclose(f);

    cetcd_server_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.node_id = 1;
    cfg.listen_port = 2379;
    cfg.election_tick = 10;
    cfg.heartbeat_tick = 1;
    snprintf(cfg.auth_token, sizeof(cfg.auth_token),
             "jwt,sign-method=HS256,priv-key=%s,ttl=5m", path);

    cetcd_server *srv = cetcd_server_new(&cfg);
    CETCD_ASSERT_NOT_NULL(srv);
    CETCD_ASSERT_EQ_INT(cetcd_server_start(srv), 0);
    cetcd_server_free(srv);
    unlink(path);
    rmdir(tmpl);
}

CETCD_TEST_CASE(server_start_rejects_bad_bcrypt_cost) {
    cetcd_server_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.node_id = 1;
    cfg.listen_port = 2379;
    cfg.election_tick = 10;
    cfg.heartbeat_tick = 1;
    cfg.bcrypt_cost = 3;

    cetcd_server *srv = cetcd_server_new(&cfg);
    CETCD_ASSERT_NOT_NULL(srv);
    CETCD_ASSERT_EQ_INT(cetcd_server_start(srv), CETCD_ERR_INVAL);
    cetcd_server_free(srv);
}

CETCD_TEST_CASE(server_start_accepts_simple_auth_token) {
    cetcd_server_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.node_id = 1;
    cfg.listen_port = 2379;
    cfg.election_tick = 10;
    cfg.heartbeat_tick = 1;
    strncpy(cfg.auth_token, "simple", sizeof(cfg.auth_token) - 1);

    cetcd_server *srv = cetcd_server_new(&cfg);
    CETCD_ASSERT_NOT_NULL(srv);
    CETCD_ASSERT_EQ_INT(cetcd_server_start(srv), 0);
    cetcd_server_free(srv);
}

CETCD_TEST_CASE(server_start_small_max_request_bytes) {
    cetcd_server_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.node_id = 1;
    cfg.listen_port = 2379;
    cfg.election_tick = 10;
    cfg.heartbeat_tick = 1;
    cfg.max_request_bytes = 4096;

    cetcd_server *srv = cetcd_server_new(&cfg);
    CETCD_ASSERT_NOT_NULL(srv);
    CETCD_ASSERT_EQ_INT(cetcd_server_start(srv), 0);

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
    cetcd_server_free(srv);
}

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(server_create_destroy),
    CETCD_TEST_ENTRY(server_handle_rpc_put_range),
    CETCD_TEST_ENTRY(server_handle_rpc_auth),
    CETCD_TEST_ENTRY(server_handle_rpc_empty_key_fails),
    CETCD_TEST_ENTRY(server_compact),
    CETCD_TEST_ENTRY(server_snapshot),
    CETCD_TEST_ENTRY(server_raft_put_restart_keeps_revision),
    CETCD_TEST_ENTRY(server_wal_replay_when_mvcc_empty),
    CETCD_TEST_ENTRY(server_txn_put_survives_restart),
    CETCD_TEST_ENTRY(server_lease_survives_restart),
    CETCD_TEST_ENTRY(server_snapshot_count_truncates_wal),
    CETCD_TEST_ENTRY(server_member_add_survives_restart),
    CETCD_TEST_ENTRY(server_start_rejects_cert_without_key),
    CETCD_TEST_ENTRY(server_start_rejects_missing_tls_files),
    CETCD_TEST_ENTRY(server_start_rejects_client_auth_without_ca),
    CETCD_TEST_ENTRY(server_start_loads_peer_tls),
    CETCD_TEST_ENTRY(server_start_rejects_jwt_without_key),
    CETCD_TEST_ENTRY(server_start_rejects_jwt_rs256),
    CETCD_TEST_ENTRY(server_start_accepts_jwt_hs256),
    CETCD_TEST_ENTRY(server_start_rejects_bad_bcrypt_cost),
    CETCD_TEST_ENTRY(server_start_accepts_simple_auth_token),
    CETCD_TEST_ENTRY(server_start_small_max_request_bytes),
CETCD_TEST_LIST_END

CETCD_TEST_MAIN()
