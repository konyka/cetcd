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

CETCD_TEST_CASE(server_wal_replay_compact) {
    char data_dir[] = "/tmp/cetcd-test-walcompact-XXXXXX";
    CETCD_ASSERT_NOT_NULL(mkdtemp(data_dir));
    char wal_dir[600];
    snprintf(wal_dir, sizeof(wal_dir), "%s/wal", data_dir);
    CETCD_ASSERT_EQ_INT(mkdir(wal_dir, 0755), 0);

    uint8_t *putp = NULL, *cmpp = NULL;
    size_t putl = 0, cmpl = 0;
    CETCD_ASSERT_EQ_INT(cetcd_apply_encode_put(&putp, &putl,
        (const uint8_t *)"ck", 2, (const uint8_t *)"cv", 2, 0), 0);
    CETCD_ASSERT_EQ_INT(cetcd_apply_encode_compact(&cmpp, &cmpl, 1), 0);

    cetcd_wal_encoder *enc = cetcd_wal_encoder_create(wal_dir);
    CETCD_ASSERT_NOT_NULL(enc);
    cetcd_entry e;
    memset(&e, 0, sizeof(e));
    e.term = 1;
    e.index = 1;
    e.type = CETCD_ENTRY_NORMAL;
    e.data = cetcd_slice_make(putp, putl);
    CETCD_ASSERT_EQ_INT(cetcd_wal_encode_entry(enc, &e), 0);
    e.index = 2;
    e.data = cetcd_slice_make(cmpp, cmpl);
    CETCD_ASSERT_EQ_INT(cetcd_wal_encode_entry(enc, &e), 0);
    cetcd_hard_state hs = {1, 1, 2};
    CETCD_ASSERT_EQ_INT(cetcd_wal_encode_hard_state(enc, &hs), 0);
    CETCD_ASSERT_EQ_INT(cetcd_wal_encoder_sync(enc), 0);
    cetcd_wal_encoder_free(enc);
    free(putp);
    free(cmpp);

    cetcd_server_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.node_id = 1;
    cfg.listen_port = 2379;
    cfg.election_tick = 10;
    cfg.heartbeat_tick = 1;
    strncpy(cfg.data_dir, data_dir, sizeof(cfg.data_dir) - 1);
    cetcd_server *srv = cetcd_server_new(&cfg);
    CETCD_ASSERT_EQ_INT(cetcd_server_start(srv), 0);

    uint8_t compact_buf[4];
    size_t pos = 0;
    compact_buf[pos++] = 0x08;
    compact_buf[pos++] = 0x01;
    cetcd_server_rpc_result resp =
        cetcd_server_handle_rpc(srv, "/etcdserverpb.KV/Compact", compact_buf, pos);
    CETCD_ASSERT_TRUE(resp.data == NULL || resp.len == 0);
    cetcd_server_rpc_result_free(&resp);

    uint8_t range_buf[8];
    pos = 0;
    range_buf[pos++] = 0x0a; range_buf[pos++] = 0x02;
    memcpy(range_buf + pos, "ck", 2); pos += 2;
    resp = cetcd_server_handle_rpc(srv, "/etcdserverpb.KV/Range", range_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    int found = 0;
    for (size_t i = 0; i + 2 <= resp.len; i++) {
        if (memcmp(resp.data + i, "cv", 2) == 0) { found = 1; break; }
    }
    CETCD_ASSERT_TRUE(found);
    cetcd_server_rpc_result_free(&resp);
    cetcd_server_free(srv);
}

CETCD_TEST_CASE(server_wal_replay_lease_grant) {
    char data_dir[] = "/tmp/cetcd-test-walgrant-XXXXXX";
    CETCD_ASSERT_NOT_NULL(mkdtemp(data_dir));
    char wal_dir[600];
    snprintf(wal_dir, sizeof(wal_dir), "%s/wal", data_dir);
    CETCD_ASSERT_EQ_INT(mkdir(wal_dir, 0755), 0);

    uint8_t *payload = NULL;
    size_t plen = 0;
    CETCD_ASSERT_EQ_INT(cetcd_apply_encode_lease_grant(&payload, &plen, 9, 60), 0);
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

    uint8_t ttl_req[4];
    size_t tpos = 0;
    ttl_req[tpos++] = 0x08; ttl_req[tpos++] = 9;
    cetcd_server_rpc_result resp =
        cetcd_server_handle_rpc(srv, "/etcdserverpb.Lease/LeaseTimeToLive",
                                ttl_req, tpos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    int granted60 = 0;
    for (size_t i = 0; i + 1 < resp.len; i++) {
        if (resp.data[i] == 0x20 && resp.data[i + 1] == 60) { granted60 = 1; break; }
    }
    CETCD_ASSERT_TRUE(granted60);
    cetcd_server_rpc_result_free(&resp);
    cetcd_server_free(srv);
}

CETCD_TEST_CASE(server_wal_replay_lease_keepalive) {
    char data_dir[] = "/tmp/cetcd-test-walka-XXXXXX";
    CETCD_ASSERT_NOT_NULL(mkdtemp(data_dir));
    char wal_dir[600];
    snprintf(wal_dir, sizeof(wal_dir), "%s/wal", data_dir);
    CETCD_ASSERT_EQ_INT(mkdir(wal_dir, 0755), 0);

    uint8_t *gp = NULL, *kp = NULL;
    size_t gl = 0, kl = 0;
    CETCD_ASSERT_EQ_INT(cetcd_apply_encode_lease_grant(&gp, &gl, 9, 10), 0);
    CETCD_ASSERT_EQ_INT(cetcd_apply_encode_lease_keepalive(&kp, &kl, 9, 10), 0);

    cetcd_wal_encoder *enc = cetcd_wal_encoder_create(wal_dir);
    CETCD_ASSERT_NOT_NULL(enc);
    cetcd_entry e;
    memset(&e, 0, sizeof(e));
    e.term = 1;
    e.index = 1;
    e.type = CETCD_ENTRY_NORMAL;
    e.data = cetcd_slice_make(gp, gl);
    CETCD_ASSERT_EQ_INT(cetcd_wal_encode_entry(enc, &e), 0);
    e.index = 2;
    e.data = cetcd_slice_make(kp, kl);
    CETCD_ASSERT_EQ_INT(cetcd_wal_encode_entry(enc, &e), 0);
    cetcd_hard_state hs = {1, 1, 2};
    CETCD_ASSERT_EQ_INT(cetcd_wal_encode_hard_state(enc, &hs), 0);
    CETCD_ASSERT_EQ_INT(cetcd_wal_encoder_sync(enc), 0);
    cetcd_wal_encoder_free(enc);
    free(gp);
    free(kp);

    cetcd_server_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.node_id = 1;
    cfg.listen_port = 2379;
    cfg.election_tick = 10;
    cfg.heartbeat_tick = 1;
    strncpy(cfg.data_dir, data_dir, sizeof(cfg.data_dir) - 1);
    cetcd_server *srv = cetcd_server_new(&cfg);
    CETCD_ASSERT_EQ_INT(cetcd_server_start(srv), 0);

    uint8_t ttl_req[4];
    size_t tpos = 0;
    ttl_req[tpos++] = 0x08; ttl_req[tpos++] = 9;
    cetcd_server_rpc_result resp =
        cetcd_server_handle_rpc(srv, "/etcdserverpb.Lease/LeaseTimeToLive",
                                ttl_req, tpos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    int granted10 = 0;
    for (size_t i = 0; i + 1 < resp.len; i++) {
        if (resp.data[i] == 0x20 && resp.data[i + 1] == 10) { granted10 = 1; break; }
    }
    CETCD_ASSERT_TRUE(granted10);
    cetcd_server_rpc_result_free(&resp);
    cetcd_server_free(srv);
}

CETCD_TEST_CASE(server_wal_replay_auth_user_add) {
    char data_dir[] = "/tmp/cetcd-test-walauth-XXXXXX";
    CETCD_ASSERT_NOT_NULL(mkdtemp(data_dir));
    char wal_dir[600];
    snprintf(wal_dir, sizeof(wal_dir), "%s/wal", data_dir);
    CETCD_ASSERT_EQ_INT(mkdir(wal_dir, 0755), 0);

    cetcd_auth_store *tmp = cetcd_auth_store_new();
    uint8_t hash[64];
    size_t hlen = 0;
    CETCD_ASSERT_EQ_INT(cetcd_auth_hash_password(tmp, "secret", hash, sizeof(hash), &hlen), 0);
    uint8_t *payload = NULL;
    size_t plen = 0;
    CETCD_ASSERT_EQ_INT(cetcd_apply_encode_auth_user_add(&payload, &plen,
        (const uint8_t *)"root", 4, hash, hlen), 0);
    cetcd_auth_store_free(tmp);

    cetcd_wal_encoder *enc = cetcd_wal_encoder_create(wal_dir);
    CETCD_ASSERT_NOT_NULL(enc);
    cetcd_entry e;
    memset(&e, 0, sizeof(e));
    e.term = 1;
    e.index = 1;
    e.type = CETCD_ENTRY_NORMAL;
    e.data = cetcd_slice_make(payload, plen);
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

    uint8_t auth_buf[32];
    size_t pos = 0;
    auth_buf[pos++] = 0x0a; auth_buf[pos++] = 0x04;
    memcpy(auth_buf + pos, "root", 4); pos += 4;
    auth_buf[pos++] = 0x12; auth_buf[pos++] = 0x06;
    memcpy(auth_buf + pos, "secret", 6); pos += 6;
    cetcd_server_rpc_result resp =
        cetcd_server_handle_rpc(srv, "/etcdserverpb.Auth/Authenticate", auth_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    cetcd_server_rpc_result_free(&resp);
    cetcd_server_free(srv);
}

CETCD_TEST_CASE(server_wal_replay_auth_user_change_pass) {
    char data_dir[] = "/tmp/cetcd-test-walauthpw-XXXXXX";
    CETCD_ASSERT_NOT_NULL(mkdtemp(data_dir));
    char wal_dir[600];
    snprintf(wal_dir, sizeof(wal_dir), "%s/wal", data_dir);
    CETCD_ASSERT_EQ_INT(mkdir(wal_dir, 0755), 0);

    cetcd_auth_store *tmp = cetcd_auth_store_new();
    uint8_t oldh[64], newh[64];
    size_t oldl = 0, newl = 0;
    CETCD_ASSERT_EQ_INT(cetcd_auth_hash_password(tmp, "secret", oldh, sizeof(oldh), &oldl), 0);
    CETCD_ASSERT_EQ_INT(cetcd_auth_hash_password(tmp, "newpw1", newh, sizeof(newh), &newl), 0);
    uint8_t *up = NULL, *cp = NULL;
    size_t ul = 0, cl = 0;
    CETCD_ASSERT_EQ_INT(cetcd_apply_encode_auth_user_add(&up, &ul,
        (const uint8_t *)"root", 4, oldh, oldl), 0);
    CETCD_ASSERT_EQ_INT(cetcd_apply_encode_auth_user_change_pass(&cp, &cl,
        (const uint8_t *)"root", 4, newh, newl), 0);
    cetcd_auth_store_free(tmp);

    cetcd_wal_encoder *enc = cetcd_wal_encoder_create(wal_dir);
    CETCD_ASSERT_NOT_NULL(enc);
    cetcd_entry e;
    memset(&e, 0, sizeof(e));
    e.term = 1;
    e.index = 1;
    e.type = CETCD_ENTRY_NORMAL;
    e.data = cetcd_slice_make(up, ul);
    CETCD_ASSERT_EQ_INT(cetcd_wal_encode_entry(enc, &e), 0);
    e.index = 2;
    e.data = cetcd_slice_make(cp, cl);
    CETCD_ASSERT_EQ_INT(cetcd_wal_encode_entry(enc, &e), 0);
    cetcd_hard_state hs = {1, 1, 2};
    CETCD_ASSERT_EQ_INT(cetcd_wal_encode_hard_state(enc, &hs), 0);
    CETCD_ASSERT_EQ_INT(cetcd_wal_encoder_sync(enc), 0);
    cetcd_wal_encoder_free(enc);
    free(up); free(cp);

    cetcd_server_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.node_id = 1;
    cfg.listen_port = 2379;
    cfg.election_tick = 10;
    cfg.heartbeat_tick = 1;
    strncpy(cfg.data_dir, data_dir, sizeof(cfg.data_dir) - 1);
    cetcd_server *srv = cetcd_server_new(&cfg);
    CETCD_ASSERT_EQ_INT(cetcd_server_start(srv), 0);

    uint8_t auth_buf[32];
    size_t pos = 0;
    auth_buf[pos++] = 0x0a; auth_buf[pos++] = 0x04;
    memcpy(auth_buf + pos, "root", 4); pos += 4;
    auth_buf[pos++] = 0x12; auth_buf[pos++] = 0x06;
    memcpy(auth_buf + pos, "secret", 6); pos += 6;
    cetcd_server_rpc_result resp =
        cetcd_server_handle_rpc(srv, "/etcdserverpb.Auth/Authenticate", auth_buf, pos);
    CETCD_ASSERT_TRUE(resp.data == NULL || resp.len == 0);
    cetcd_server_rpc_result_free(&resp);

    pos = 0;
    auth_buf[pos++] = 0x0a; auth_buf[pos++] = 0x04;
    memcpy(auth_buf + pos, "root", 4); pos += 4;
    auth_buf[pos++] = 0x12; auth_buf[pos++] = 0x06;
    memcpy(auth_buf + pos, "newpw1", 6); pos += 6;
    resp = cetcd_server_handle_rpc(srv, "/etcdserverpb.Auth/Authenticate", auth_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    cetcd_server_rpc_result_free(&resp);
    cetcd_server_free(srv);
}

CETCD_TEST_CASE(server_wal_replay_auth_enabled) {
    char data_dir[] = "/tmp/cetcd-test-walauthen-XXXXXX";
    CETCD_ASSERT_NOT_NULL(mkdtemp(data_dir));
    char wal_dir[600];
    snprintf(wal_dir, sizeof(wal_dir), "%s/wal", data_dir);
    CETCD_ASSERT_EQ_INT(mkdir(wal_dir, 0755), 0);

    cetcd_auth_store *tmp = cetcd_auth_store_new();
    uint8_t hash[64];
    size_t hlen = 0;
    CETCD_ASSERT_EQ_INT(cetcd_auth_hash_password(tmp, "secret", hash, sizeof(hash), &hlen), 0);
    uint8_t *up = NULL, *ep = NULL;
    size_t ul = 0, el = 0;
    CETCD_ASSERT_EQ_INT(cetcd_apply_encode_auth_user_add(&up, &ul,
        (const uint8_t *)"root", 4, hash, hlen), 0);
    CETCD_ASSERT_EQ_INT(cetcd_apply_encode_auth_enabled(&ep, &el, 1), 0);
    cetcd_auth_store_free(tmp);

    cetcd_wal_encoder *enc = cetcd_wal_encoder_create(wal_dir);
    CETCD_ASSERT_NOT_NULL(enc);
    cetcd_entry e;
    memset(&e, 0, sizeof(e));
    e.term = 1;
    e.index = 1;
    e.type = CETCD_ENTRY_NORMAL;
    e.data = cetcd_slice_make(up, ul);
    CETCD_ASSERT_EQ_INT(cetcd_wal_encode_entry(enc, &e), 0);
    e.index = 2;
    e.data = cetcd_slice_make(ep, el);
    CETCD_ASSERT_EQ_INT(cetcd_wal_encode_entry(enc, &e), 0);
    cetcd_hard_state hs = {1, 1, 2};
    CETCD_ASSERT_EQ_INT(cetcd_wal_encode_hard_state(enc, &hs), 0);
    CETCD_ASSERT_EQ_INT(cetcd_wal_encoder_sync(enc), 0);
    cetcd_wal_encoder_free(enc);
    free(up);
    free(ep);

    cetcd_server_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.node_id = 1;
    cfg.listen_port = 2379;
    cfg.election_tick = 10;
    cfg.heartbeat_tick = 1;
    strncpy(cfg.data_dir, data_dir, sizeof(cfg.data_dir) - 1);
    cetcd_server *srv = cetcd_server_new(&cfg);
    CETCD_ASSERT_EQ_INT(cetcd_server_start(srv), 0);

    uint8_t auth_buf[32];
    size_t pos = 0;
    auth_buf[pos++] = 0x0a; auth_buf[pos++] = 0x04;
    memcpy(auth_buf + pos, "root", 4); pos += 4;
    auth_buf[pos++] = 0x12; auth_buf[pos++] = 0x06;
    memcpy(auth_buf + pos, "secret", 6); pos += 6;
    cetcd_server_rpc_result resp =
        cetcd_server_handle_rpc(srv, "/etcdserverpb.Auth/Authenticate", auth_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    cetcd_server_rpc_result_free(&resp);

    uint8_t put_buf[16];
    size_t p = 0;
    put_buf[p++] = 0x0a; put_buf[p++] = 0x01;
    memcpy(put_buf + p, "k", 1); p += 1;
    put_buf[p++] = 0x12; put_buf[p++] = 0x01;
    memcpy(put_buf + p, "v", 1); p += 1;
    resp = cetcd_server_handle_rpc(srv, "/etcdserverpb.KV/Put", put_buf, p);
    CETCD_ASSERT_TRUE(resp.data == NULL || resp.len == 0);
    cetcd_server_rpc_result_free(&resp);
    cetcd_server_free(srv);
}

CETCD_TEST_CASE(server_wal_replay_auth_user_delete) {
    char data_dir[] = "/tmp/cetcd-test-walauthdel-XXXXXX";
    CETCD_ASSERT_NOT_NULL(mkdtemp(data_dir));
    char wal_dir[600];
    snprintf(wal_dir, sizeof(wal_dir), "%s/wal", data_dir);
    CETCD_ASSERT_EQ_INT(mkdir(wal_dir, 0755), 0);

    cetcd_auth_store *tmp = cetcd_auth_store_new();
    uint8_t hash[64];
    size_t hlen = 0;
    CETCD_ASSERT_EQ_INT(cetcd_auth_hash_password(tmp, "secret", hash, sizeof(hash), &hlen), 0);
    uint8_t *up = NULL, *dp = NULL;
    size_t ul = 0, dl = 0;
    CETCD_ASSERT_EQ_INT(cetcd_apply_encode_auth_user_add(&up, &ul,
        (const uint8_t *)"root", 4, hash, hlen), 0);
    CETCD_ASSERT_EQ_INT(cetcd_apply_encode_auth_user_delete(&dp, &dl,
        (const uint8_t *)"root", 4), 0);
    cetcd_auth_store_free(tmp);

    cetcd_wal_encoder *enc = cetcd_wal_encoder_create(wal_dir);
    CETCD_ASSERT_NOT_NULL(enc);
    cetcd_entry e;
    memset(&e, 0, sizeof(e));
    e.term = 1;
    e.index = 1;
    e.type = CETCD_ENTRY_NORMAL;
    e.data = cetcd_slice_make(up, ul);
    CETCD_ASSERT_EQ_INT(cetcd_wal_encode_entry(enc, &e), 0);
    e.index = 2;
    e.data = cetcd_slice_make(dp, dl);
    CETCD_ASSERT_EQ_INT(cetcd_wal_encode_entry(enc, &e), 0);
    cetcd_hard_state hs = {1, 1, 2};
    CETCD_ASSERT_EQ_INT(cetcd_wal_encode_hard_state(enc, &hs), 0);
    CETCD_ASSERT_EQ_INT(cetcd_wal_encoder_sync(enc), 0);
    cetcd_wal_encoder_free(enc);
    free(up);
    free(dp);

    cetcd_server_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.node_id = 1;
    cfg.listen_port = 2379;
    cfg.election_tick = 10;
    cfg.heartbeat_tick = 1;
    strncpy(cfg.data_dir, data_dir, sizeof(cfg.data_dir) - 1);
    cetcd_server *srv = cetcd_server_new(&cfg);
    CETCD_ASSERT_EQ_INT(cetcd_server_start(srv), 0);

    uint8_t auth_buf[32];
    size_t pos = 0;
    auth_buf[pos++] = 0x0a; auth_buf[pos++] = 0x04;
    memcpy(auth_buf + pos, "root", 4); pos += 4;
    auth_buf[pos++] = 0x12; auth_buf[pos++] = 0x06;
    memcpy(auth_buf + pos, "secret", 6); pos += 6;
    cetcd_server_rpc_result resp =
        cetcd_server_handle_rpc(srv, "/etcdserverpb.Auth/Authenticate", auth_buf, pos);
    CETCD_ASSERT_TRUE(resp.data == NULL || resp.len == 0);
    cetcd_server_rpc_result_free(&resp);
    cetcd_server_free(srv);
}

CETCD_TEST_CASE(server_wal_replay_auth_role_add) {
    char data_dir[] = "/tmp/cetcd-test-walauthrole-XXXXXX";
    CETCD_ASSERT_NOT_NULL(mkdtemp(data_dir));
    char wal_dir[600];
    snprintf(wal_dir, sizeof(wal_dir), "%s/wal", data_dir);
    CETCD_ASSERT_EQ_INT(mkdir(wal_dir, 0755), 0);

    uint8_t *payload = NULL;
    size_t plen = 0;
    CETCD_ASSERT_EQ_INT(cetcd_apply_encode_auth_role_add(&payload, &plen,
        (const uint8_t *)"admin", 5), 0);

    cetcd_wal_encoder *enc = cetcd_wal_encoder_create(wal_dir);
    CETCD_ASSERT_NOT_NULL(enc);
    cetcd_entry e;
    memset(&e, 0, sizeof(e));
    e.term = 1;
    e.index = 1;
    e.type = CETCD_ENTRY_NORMAL;
    e.data = cetcd_slice_make(payload, plen);
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

    uint8_t get_buf[16];
    size_t pos = 0;
    get_buf[pos++] = 0x0a; get_buf[pos++] = 0x05;
    memcpy(get_buf + pos, "admin", 5); pos += 5;
    cetcd_server_rpc_result resp =
        cetcd_server_handle_rpc(srv, "/etcdserverpb.Auth/RoleGet", get_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    cetcd_server_rpc_result_free(&resp);
    cetcd_server_free(srv);
}

CETCD_TEST_CASE(server_wal_replay_auth_role_delete) {
    char data_dir[] = "/tmp/cetcd-test-walauthroledel-XXXXXX";
    CETCD_ASSERT_NOT_NULL(mkdtemp(data_dir));
    char wal_dir[600];
    snprintf(wal_dir, sizeof(wal_dir), "%s/wal", data_dir);
    CETCD_ASSERT_EQ_INT(mkdir(wal_dir, 0755), 0);

    uint8_t *ap = NULL, *dp = NULL;
    size_t al = 0, dl = 0;
    CETCD_ASSERT_EQ_INT(cetcd_apply_encode_auth_role_add(&ap, &al,
        (const uint8_t *)"admin", 5), 0);
    CETCD_ASSERT_EQ_INT(cetcd_apply_encode_auth_role_delete(&dp, &dl,
        (const uint8_t *)"admin", 5), 0);

    cetcd_wal_encoder *enc = cetcd_wal_encoder_create(wal_dir);
    CETCD_ASSERT_NOT_NULL(enc);
    cetcd_entry e;
    memset(&e, 0, sizeof(e));
    e.term = 1;
    e.index = 1;
    e.type = CETCD_ENTRY_NORMAL;
    e.data = cetcd_slice_make(ap, al);
    CETCD_ASSERT_EQ_INT(cetcd_wal_encode_entry(enc, &e), 0);
    e.index = 2;
    e.data = cetcd_slice_make(dp, dl);
    CETCD_ASSERT_EQ_INT(cetcd_wal_encode_entry(enc, &e), 0);
    cetcd_hard_state hs = {1, 1, 2};
    CETCD_ASSERT_EQ_INT(cetcd_wal_encode_hard_state(enc, &hs), 0);
    CETCD_ASSERT_EQ_INT(cetcd_wal_encoder_sync(enc), 0);
    cetcd_wal_encoder_free(enc);
    free(ap);
    free(dp);

    cetcd_server_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.node_id = 1;
    cfg.listen_port = 2379;
    cfg.election_tick = 10;
    cfg.heartbeat_tick = 1;
    strncpy(cfg.data_dir, data_dir, sizeof(cfg.data_dir) - 1);
    cetcd_server *srv = cetcd_server_new(&cfg);
    CETCD_ASSERT_EQ_INT(cetcd_server_start(srv), 0);

    uint8_t get_buf[16];
    size_t pos = 0;
    get_buf[pos++] = 0x0a; get_buf[pos++] = 0x05;
    memcpy(get_buf + pos, "admin", 5); pos += 5;
    cetcd_server_rpc_result resp =
        cetcd_server_handle_rpc(srv, "/etcdserverpb.Auth/RoleGet", get_buf, pos);
    CETCD_ASSERT_TRUE(resp.data == NULL || resp.len == 0);
    cetcd_server_rpc_result_free(&resp);
    cetcd_server_free(srv);
}

CETCD_TEST_CASE(server_wal_replay_auth_user_grant_role) {
    char data_dir[] = "/tmp/cetcd-test-walauthgrant-XXXXXX";
    CETCD_ASSERT_NOT_NULL(mkdtemp(data_dir));
    char wal_dir[600];
    snprintf(wal_dir, sizeof(wal_dir), "%s/wal", data_dir);
    CETCD_ASSERT_EQ_INT(mkdir(wal_dir, 0755), 0);

    cetcd_auth_store *tmp = cetcd_auth_store_new();
    uint8_t hash[64];
    size_t hlen = 0;
    CETCD_ASSERT_EQ_INT(cetcd_auth_hash_password(tmp, "secret", hash, sizeof(hash), &hlen), 0);
    uint8_t *up = NULL, *rp = NULL, *gp = NULL;
    size_t ul = 0, rl = 0, gl = 0;
    CETCD_ASSERT_EQ_INT(cetcd_apply_encode_auth_user_add(&up, &ul,
        (const uint8_t *)"root", 4, hash, hlen), 0);
    CETCD_ASSERT_EQ_INT(cetcd_apply_encode_auth_role_add(&rp, &rl,
        (const uint8_t *)"admin", 5), 0);
    CETCD_ASSERT_EQ_INT(cetcd_apply_encode_auth_user_grant_role(&gp, &gl,
        (const uint8_t *)"root", 4, (const uint8_t *)"admin", 5), 0);
    cetcd_auth_store_free(tmp);

    cetcd_wal_encoder *enc = cetcd_wal_encoder_create(wal_dir);
    CETCD_ASSERT_NOT_NULL(enc);
    cetcd_entry e;
    memset(&e, 0, sizeof(e));
    e.term = 1;
    e.index = 1;
    e.type = CETCD_ENTRY_NORMAL;
    e.data = cetcd_slice_make(up, ul);
    CETCD_ASSERT_EQ_INT(cetcd_wal_encode_entry(enc, &e), 0);
    e.index = 2;
    e.data = cetcd_slice_make(rp, rl);
    CETCD_ASSERT_EQ_INT(cetcd_wal_encode_entry(enc, &e), 0);
    e.index = 3;
    e.data = cetcd_slice_make(gp, gl);
    CETCD_ASSERT_EQ_INT(cetcd_wal_encode_entry(enc, &e), 0);
    cetcd_hard_state hs = {1, 1, 3};
    CETCD_ASSERT_EQ_INT(cetcd_wal_encode_hard_state(enc, &hs), 0);
    CETCD_ASSERT_EQ_INT(cetcd_wal_encoder_sync(enc), 0);
    cetcd_wal_encoder_free(enc);
    free(up); free(rp); free(gp);

    cetcd_server_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.node_id = 1;
    cfg.listen_port = 2379;
    cfg.election_tick = 10;
    cfg.heartbeat_tick = 1;
    strncpy(cfg.data_dir, data_dir, sizeof(cfg.data_dir) - 1);
    cetcd_server *srv = cetcd_server_new(&cfg);
    CETCD_ASSERT_EQ_INT(cetcd_server_start(srv), 0);

    uint8_t get_buf[16];
    size_t pos = 0;
    get_buf[pos++] = 0x0a; get_buf[pos++] = 0x04;
    memcpy(get_buf + pos, "root", 4); pos += 4;
    cetcd_server_rpc_result resp =
        cetcd_server_handle_rpc(srv, "/etcdserverpb.Auth/UserGet", get_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    int found = 0;
    for (size_t i = 0; i + 5 <= resp.len; i++) {
        if (memcmp(resp.data + i, "admin", 5) == 0) { found = 1; break; }
    }
    CETCD_ASSERT_TRUE(found);
    cetcd_server_rpc_result_free(&resp);
    cetcd_server_free(srv);
}

CETCD_TEST_CASE(server_wal_replay_auth_user_revoke_role) {
    char data_dir[] = "/tmp/cetcd-test-walauthrevoke-XXXXXX";
    CETCD_ASSERT_NOT_NULL(mkdtemp(data_dir));
    char wal_dir[600];
    snprintf(wal_dir, sizeof(wal_dir), "%s/wal", data_dir);
    CETCD_ASSERT_EQ_INT(mkdir(wal_dir, 0755), 0);

    cetcd_auth_store *tmp = cetcd_auth_store_new();
    uint8_t hash[64];
    size_t hlen = 0;
    CETCD_ASSERT_EQ_INT(cetcd_auth_hash_password(tmp, "secret", hash, sizeof(hash), &hlen), 0);
    uint8_t *up = NULL, *rp = NULL, *gp = NULL, *vp = NULL;
    size_t ul = 0, rl = 0, gl = 0, vl = 0;
    CETCD_ASSERT_EQ_INT(cetcd_apply_encode_auth_user_add(&up, &ul,
        (const uint8_t *)"root", 4, hash, hlen), 0);
    CETCD_ASSERT_EQ_INT(cetcd_apply_encode_auth_role_add(&rp, &rl,
        (const uint8_t *)"admin", 5), 0);
    CETCD_ASSERT_EQ_INT(cetcd_apply_encode_auth_user_grant_role(&gp, &gl,
        (const uint8_t *)"root", 4, (const uint8_t *)"admin", 5), 0);
    CETCD_ASSERT_EQ_INT(cetcd_apply_encode_auth_user_revoke_role(&vp, &vl,
        (const uint8_t *)"root", 4, (const uint8_t *)"admin", 5), 0);
    cetcd_auth_store_free(tmp);

    cetcd_wal_encoder *enc = cetcd_wal_encoder_create(wal_dir);
    CETCD_ASSERT_NOT_NULL(enc);
    cetcd_entry e;
    memset(&e, 0, sizeof(e));
    e.term = 1;
    e.index = 1;
    e.type = CETCD_ENTRY_NORMAL;
    e.data = cetcd_slice_make(up, ul);
    CETCD_ASSERT_EQ_INT(cetcd_wal_encode_entry(enc, &e), 0);
    e.index = 2;
    e.data = cetcd_slice_make(rp, rl);
    CETCD_ASSERT_EQ_INT(cetcd_wal_encode_entry(enc, &e), 0);
    e.index = 3;
    e.data = cetcd_slice_make(gp, gl);
    CETCD_ASSERT_EQ_INT(cetcd_wal_encode_entry(enc, &e), 0);
    e.index = 4;
    e.data = cetcd_slice_make(vp, vl);
    CETCD_ASSERT_EQ_INT(cetcd_wal_encode_entry(enc, &e), 0);
    cetcd_hard_state hs = {1, 1, 4};
    CETCD_ASSERT_EQ_INT(cetcd_wal_encode_hard_state(enc, &hs), 0);
    CETCD_ASSERT_EQ_INT(cetcd_wal_encoder_sync(enc), 0);
    cetcd_wal_encoder_free(enc);
    free(up); free(rp); free(gp); free(vp);

    cetcd_server_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.node_id = 1;
    cfg.listen_port = 2379;
    cfg.election_tick = 10;
    cfg.heartbeat_tick = 1;
    strncpy(cfg.data_dir, data_dir, sizeof(cfg.data_dir) - 1);
    cetcd_server *srv = cetcd_server_new(&cfg);
    CETCD_ASSERT_EQ_INT(cetcd_server_start(srv), 0);

    uint8_t get_buf[16];
    size_t pos = 0;
    get_buf[pos++] = 0x0a; get_buf[pos++] = 0x04;
    memcpy(get_buf + pos, "root", 4); pos += 4;
    cetcd_server_rpc_result resp =
        cetcd_server_handle_rpc(srv, "/etcdserverpb.Auth/UserGet", get_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    int found = 0;
    for (size_t i = 0; i + 5 <= resp.len; i++) {
        if (memcmp(resp.data + i, "admin", 5) == 0) { found = 1; break; }
    }
    CETCD_ASSERT_TRUE(!found);
    cetcd_server_rpc_result_free(&resp);
    cetcd_server_free(srv);
}

CETCD_TEST_CASE(server_wal_replay_auth_role_grant_perm) {
    char data_dir[] = "/tmp/cetcd-test-walauthperm-XXXXXX";
    CETCD_ASSERT_NOT_NULL(mkdtemp(data_dir));
    char wal_dir[600];
    snprintf(wal_dir, sizeof(wal_dir), "%s/wal", data_dir);
    CETCD_ASSERT_EQ_INT(mkdir(wal_dir, 0755), 0);

    uint8_t *ap = NULL, *gp = NULL;
    size_t al = 0, gl = 0;
    CETCD_ASSERT_EQ_INT(cetcd_apply_encode_auth_role_add(&ap, &al,
        (const uint8_t *)"admin", 5), 0);
    CETCD_ASSERT_EQ_INT(cetcd_apply_encode_auth_role_grant_perm(&gp, &gl,
        (const uint8_t *)"admin", 5, (const uint8_t *)"/foo", 4, 2), 0);

    cetcd_wal_encoder *enc = cetcd_wal_encoder_create(wal_dir);
    CETCD_ASSERT_NOT_NULL(enc);
    cetcd_entry e;
    memset(&e, 0, sizeof(e));
    e.term = 1;
    e.index = 1;
    e.type = CETCD_ENTRY_NORMAL;
    e.data = cetcd_slice_make(ap, al);
    CETCD_ASSERT_EQ_INT(cetcd_wal_encode_entry(enc, &e), 0);
    e.index = 2;
    e.data = cetcd_slice_make(gp, gl);
    CETCD_ASSERT_EQ_INT(cetcd_wal_encode_entry(enc, &e), 0);
    cetcd_hard_state hs = {1, 1, 2};
    CETCD_ASSERT_EQ_INT(cetcd_wal_encode_hard_state(enc, &hs), 0);
    CETCD_ASSERT_EQ_INT(cetcd_wal_encoder_sync(enc), 0);
    cetcd_wal_encoder_free(enc);
    free(ap);
    free(gp);

    cetcd_server_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.node_id = 1;
    cfg.listen_port = 2379;
    cfg.election_tick = 10;
    cfg.heartbeat_tick = 1;
    strncpy(cfg.data_dir, data_dir, sizeof(cfg.data_dir) - 1);
    cetcd_server *srv = cetcd_server_new(&cfg);
    CETCD_ASSERT_EQ_INT(cetcd_server_start(srv), 0);

    uint8_t get_buf[16];
    size_t pos = 0;
    get_buf[pos++] = 0x0a; get_buf[pos++] = 0x05;
    memcpy(get_buf + pos, "admin", 5); pos += 5;
    cetcd_server_rpc_result resp =
        cetcd_server_handle_rpc(srv, "/etcdserverpb.Auth/RoleGet", get_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    int found = 0;
    for (size_t i = 0; i + 4 <= resp.len; i++) {
        if (memcmp(resp.data + i, "/foo", 4) == 0) { found = 1; break; }
    }
    CETCD_ASSERT_TRUE(found);
    cetcd_server_rpc_result_free(&resp);
    cetcd_server_free(srv);
}

CETCD_TEST_CASE(server_wal_replay_auth_role_revoke_perm) {
    char data_dir[] = "/tmp/cetcd-test-walauthpermdel-XXXXXX";
    CETCD_ASSERT_NOT_NULL(mkdtemp(data_dir));
    char wal_dir[600];
    snprintf(wal_dir, sizeof(wal_dir), "%s/wal", data_dir);
    CETCD_ASSERT_EQ_INT(mkdir(wal_dir, 0755), 0);

    uint8_t *ap = NULL, *gp = NULL, *vp = NULL;
    size_t al = 0, gl = 0, vl = 0;
    CETCD_ASSERT_EQ_INT(cetcd_apply_encode_auth_role_add(&ap, &al,
        (const uint8_t *)"admin", 5), 0);
    CETCD_ASSERT_EQ_INT(cetcd_apply_encode_auth_role_grant_perm(&gp, &gl,
        (const uint8_t *)"admin", 5, (const uint8_t *)"/foo", 4, 2), 0);
    CETCD_ASSERT_EQ_INT(cetcd_apply_encode_auth_role_revoke_perm(&vp, &vl,
        (const uint8_t *)"admin", 5, NULL, 0), 0);

    cetcd_wal_encoder *enc = cetcd_wal_encoder_create(wal_dir);
    CETCD_ASSERT_NOT_NULL(enc);
    cetcd_entry e;
    memset(&e, 0, sizeof(e));
    e.term = 1;
    e.index = 1;
    e.type = CETCD_ENTRY_NORMAL;
    e.data = cetcd_slice_make(ap, al);
    CETCD_ASSERT_EQ_INT(cetcd_wal_encode_entry(enc, &e), 0);
    e.index = 2;
    e.data = cetcd_slice_make(gp, gl);
    CETCD_ASSERT_EQ_INT(cetcd_wal_encode_entry(enc, &e), 0);
    e.index = 3;
    e.data = cetcd_slice_make(vp, vl);
    CETCD_ASSERT_EQ_INT(cetcd_wal_encode_entry(enc, &e), 0);
    cetcd_hard_state hs = {1, 1, 3};
    CETCD_ASSERT_EQ_INT(cetcd_wal_encode_hard_state(enc, &hs), 0);
    CETCD_ASSERT_EQ_INT(cetcd_wal_encoder_sync(enc), 0);
    cetcd_wal_encoder_free(enc);
    free(ap); free(gp); free(vp);

    cetcd_server_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.node_id = 1;
    cfg.listen_port = 2379;
    cfg.election_tick = 10;
    cfg.heartbeat_tick = 1;
    strncpy(cfg.data_dir, data_dir, sizeof(cfg.data_dir) - 1);
    cetcd_server *srv = cetcd_server_new(&cfg);
    CETCD_ASSERT_EQ_INT(cetcd_server_start(srv), 0);

    uint8_t get_buf[16];
    size_t pos = 0;
    get_buf[pos++] = 0x0a; get_buf[pos++] = 0x05;
    memcpy(get_buf + pos, "admin", 5); pos += 5;
    cetcd_server_rpc_result resp =
        cetcd_server_handle_rpc(srv, "/etcdserverpb.Auth/RoleGet", get_buf, pos);
    CETCD_ASSERT_NOT_NULL(resp.data);
    int found = 0;
    for (size_t i = 0; i + 4 <= resp.len; i++) {
        if (memcmp(resp.data + i, "/foo", 4) == 0) { found = 1; break; }
    }
    CETCD_ASSERT_TRUE(!found);
    cetcd_server_rpc_result_free(&resp);
    cetcd_server_free(srv);
}

CETCD_TEST_CASE(server_wal_replay_alarm) {
    char data_dir[] = "/tmp/cetcd-test-walalarm-XXXXXX";
    CETCD_ASSERT_NOT_NULL(mkdtemp(data_dir));
    char wal_dir[600];
    snprintf(wal_dir, sizeof(wal_dir), "%s/wal", data_dir);
    CETCD_ASSERT_EQ_INT(mkdir(wal_dir, 0755), 0);

    uint8_t *payload = NULL;
    size_t plen = 0;
    CETCD_ASSERT_EQ_INT(cetcd_apply_encode_alarm(&payload, &plen, 1, 2, 9), 0);
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
    CETCD_ASSERT_TRUE(cetcd_v3rpc_alarm_is_active(2));
    CETCD_ASSERT_TRUE(!cetcd_v3rpc_alarm_is_active(1));
    cetcd_server_free(srv);
}

CETCD_TEST_CASE(server_alarm_survives_restart) {
    char data_dir[] = "/tmp/cetcd-test-alarm-XXXXXX";
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

    uint8_t activate[] = {0x08, 0x01, 0x10, 0x00, 0x18, 0x01};
    cetcd_server_rpc_result resp =
        cetcd_server_handle_rpc(srv, "/etcdserverpb.Maintenance/Alarm",
                                activate, sizeof(activate));
    CETCD_ASSERT_NOT_NULL(resp.data);
    cetcd_server_rpc_result_free(&resp);
    cetcd_server_stop(srv);
    cetcd_server_free(srv);
    CETCD_ASSERT_TRUE(!cetcd_v3rpc_alarm_is_active(1));

    memset(&cfg, 0, sizeof(cfg));
    cfg.node_id = 1;
    cfg.listen_port = 2379;
    cfg.election_tick = 10;
    cfg.heartbeat_tick = 1;
    strncpy(cfg.data_dir, data_dir, sizeof(cfg.data_dir) - 1);
    srv = cetcd_server_new(&cfg);
    CETCD_ASSERT_EQ_INT(cetcd_server_start(srv), 0);
    CETCD_ASSERT_TRUE(cetcd_v3rpc_alarm_is_active(1));

    uint8_t get_alarm[] = {0x08, 0x00};
    resp = cetcd_server_handle_rpc(srv, "/etcdserverpb.Maintenance/Alarm",
                                   get_alarm, sizeof(get_alarm));
    CETCD_ASSERT_NOT_NULL(resp.data);
    int found = 0;
    for (size_t i = 0; i < resp.len; i++) {
        if (resp.data[i] == 0x12) { found = 1; break; }
    }
    CETCD_ASSERT_TRUE(found);
    cetcd_server_rpc_result_free(&resp);

    uint8_t disarm[] = {0x08, 0x02, 0x10, 0x00, 0x18, 0x01};
    resp = cetcd_server_handle_rpc(srv, "/etcdserverpb.Maintenance/Alarm",
                                   disarm, sizeof(disarm));
    cetcd_server_rpc_result_free(&resp);
    cetcd_server_stop(srv);
    cetcd_server_free(srv);

    srv = cetcd_server_new(&cfg);
    CETCD_ASSERT_EQ_INT(cetcd_server_start(srv), 0);
    CETCD_ASSERT_TRUE(!cetcd_v3rpc_alarm_is_active(1));
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

CETCD_TEST_CASE(server_start_rejects_cipher_suites_without_tls) {
    cetcd_server_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.node_id = 1;
    cfg.listen_port = 2379;
    cfg.election_tick = 10;
    cfg.heartbeat_tick = 1;
    strncpy(cfg.cipher_suites, "ECDHE-ECDSA-AES256-GCM-SHA384",
            sizeof(cfg.cipher_suites) - 1);

    cetcd_server *srv = cetcd_server_new(&cfg);
    CETCD_ASSERT_NOT_NULL(srv);
    CETCD_ASSERT_EQ_INT(cetcd_server_start(srv), CETCD_ERR_INVAL);
    cetcd_server_free(srv);
}

CETCD_TEST_CASE(server_start_rejects_unknown_cipher_suites) {
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
    strncpy(cfg.cert_file, cert, sizeof(cfg.cert_file) - 1);
    strncpy(cfg.key_file, key, sizeof(cfg.key_file) - 1);
    strncpy(cfg.cipher_suites, "NOT-A-REAL-CIPHER",
            sizeof(cfg.cipher_suites) - 1);

    cetcd_server *srv = cetcd_server_new(&cfg);
    CETCD_ASSERT_NOT_NULL(srv);
    CETCD_ASSERT_EQ_INT(cetcd_server_start(srv), CETCD_ERR_INVAL);
    cetcd_server_free(srv);
    cleanup_selfsigned_(dir);
}

CETCD_TEST_CASE(server_start_accepts_iana_cipher_suites) {
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
    strncpy(cfg.cert_file, cert, sizeof(cfg.cert_file) - 1);
    strncpy(cfg.key_file, key, sizeof(cfg.key_file) - 1);
    strncpy(cfg.cipher_suites, "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
            sizeof(cfg.cipher_suites) - 1);

    cetcd_server *srv = cetcd_server_new(&cfg);
    CETCD_ASSERT_NOT_NULL(srv);
    CETCD_ASSERT_EQ_INT(cetcd_server_start(srv), 0);
    cetcd_server_free(srv);
    cleanup_selfsigned_(dir);
}

CETCD_TEST_CASE(server_start_rejects_https_listen_without_tls) {
    cetcd_server_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.node_id = 1;
    cfg.listen_port = 2379;
    cfg.election_tick = 10;
    cfg.heartbeat_tick = 1;
    cfg.listen_https = true;

    cetcd_server *srv = cetcd_server_new(&cfg);
    CETCD_ASSERT_NOT_NULL(srv);
    CETCD_ASSERT_EQ_INT(cetcd_server_start(srv), CETCD_ERR_INVAL);
    cetcd_server_free(srv);
}

CETCD_TEST_CASE(server_start_rejects_https_peer_listen_without_tls) {
    cetcd_server_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.node_id = 1;
    cfg.listen_port = 2379;
    cfg.election_tick = 10;
    cfg.heartbeat_tick = 1;
    cfg.peer_listen_https = true;

    cetcd_server *srv = cetcd_server_new(&cfg);
    CETCD_ASSERT_NOT_NULL(srv);
    CETCD_ASSERT_EQ_INT(cetcd_server_start(srv), CETCD_ERR_INVAL);
    cetcd_server_free(srv);
}

CETCD_TEST_CASE(server_start_accepts_https_listen_with_certs) {
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
    cfg.listen_https = true;
    cfg.peer_listen_https = true;
    strncpy(cfg.cert_file, cert, sizeof(cfg.cert_file) - 1);
    strncpy(cfg.key_file, key, sizeof(cfg.key_file) - 1);
    strncpy(cfg.peer_cert_file, cert, sizeof(cfg.peer_cert_file) - 1);
    strncpy(cfg.peer_key_file, key, sizeof(cfg.peer_key_file) - 1);

    cetcd_server *srv = cetcd_server_new(&cfg);
    CETCD_ASSERT_NOT_NULL(srv);
    CETCD_ASSERT_EQ_INT(cetcd_server_start(srv), 0);
    cetcd_server_free(srv);
    cleanup_selfsigned_(dir);
}

CETCD_TEST_CASE(server_start_rejects_force_new_cluster) {
    cetcd_server_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.node_id = 1;
    cfg.listen_port = 2379;
    cfg.election_tick = 10;
    cfg.heartbeat_tick = 1;
    cfg.force_new_cluster = true;

    cetcd_server *srv = cetcd_server_new(&cfg);
    CETCD_ASSERT_NOT_NULL(srv);
    CETCD_ASSERT_EQ_INT(cetcd_server_start(srv), CETCD_ERR_INVAL);
    cetcd_server_free(srv);
}

CETCD_TEST_CASE(server_start_rejects_cluster_state_existing) {
    cetcd_server_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.node_id = 1;
    cfg.listen_port = 2379;
    cfg.election_tick = 10;
    cfg.heartbeat_tick = 1;
    strncpy(cfg.initial_cluster_state, "existing",
            sizeof(cfg.initial_cluster_state) - 1);

    cetcd_server *srv = cetcd_server_new(&cfg);
    CETCD_ASSERT_NOT_NULL(srv);
    CETCD_ASSERT_EQ_INT(cetcd_server_start(srv), CETCD_ERR_INVAL);
    cetcd_server_free(srv);
}

CETCD_TEST_CASE(server_start_accepts_cluster_state_new) {
    cetcd_server_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.node_id = 1;
    cfg.listen_port = 2379;
    cfg.election_tick = 10;
    cfg.heartbeat_tick = 1;
    strncpy(cfg.initial_cluster_state, "new",
            sizeof(cfg.initial_cluster_state) - 1);

    cetcd_server *srv = cetcd_server_new(&cfg);
    CETCD_ASSERT_NOT_NULL(srv);
    CETCD_ASSERT_EQ_INT(cetcd_server_start(srv), 0);
    cetcd_server_free(srv);
}

CETCD_TEST_CASE(server_start_rejects_https_initial_cluster_without_peer_tls) {
    cetcd_server_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.node_id = 1;
    cfg.listen_port = 2379;
    cfg.election_tick = 10;
    cfg.heartbeat_tick = 1;
    cfg.initial_cluster_https = true;

    cetcd_server *srv = cetcd_server_new(&cfg);
    CETCD_ASSERT_NOT_NULL(srv);
    CETCD_ASSERT_EQ_INT(cetcd_server_start(srv), CETCD_ERR_INVAL);
    cetcd_server_free(srv);
}

CETCD_TEST_CASE(server_start_accepts_https_initial_cluster_with_peer_tls) {
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
    cfg.initial_cluster_https = true;
    strncpy(cfg.peer_cert_file, cert, sizeof(cfg.peer_cert_file) - 1);
    strncpy(cfg.peer_key_file, key, sizeof(cfg.peer_key_file) - 1);

    cetcd_server *srv = cetcd_server_new(&cfg);
    CETCD_ASSERT_NOT_NULL(srv);
    CETCD_ASSERT_EQ_INT(cetcd_server_start(srv), 0);
    cetcd_server_free(srv);
    cleanup_selfsigned_(dir);
}

CETCD_TEST_CASE(server_start_rejects_grpc_keepalive_timeout_without_time) {
    cetcd_server_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.node_id = 1;
    cfg.listen_port = 2379;
    cfg.election_tick = 10;
    cfg.heartbeat_tick = 1;
    cfg.keepalive_timeout = 5;

    cetcd_server *srv = cetcd_server_new(&cfg);
    CETCD_ASSERT_NOT_NULL(srv);
    CETCD_ASSERT_EQ_INT(cetcd_server_start(srv), CETCD_ERR_INVAL);
    cetcd_server_free(srv);
}

CETCD_TEST_CASE(server_start_accepts_grpc_keepalive_time) {
    cetcd_server_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.node_id = 1;
    cfg.listen_port = 2379;
    cfg.election_tick = 10;
    cfg.heartbeat_tick = 1;
    cfg.keepalive_set = true;
    cfg.keepalive_time = 10;
    cfg.keepalive_timeout = 5;

    cetcd_server *srv = cetcd_server_new(&cfg);
    CETCD_ASSERT_NOT_NULL(srv);
    CETCD_ASSERT_EQ_INT(cetcd_server_start(srv), 0);
    cetcd_server_free(srv);
}

CETCD_TEST_CASE(server_start_rejects_auto_tls_without_certs) {
    cetcd_server_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.node_id = 1;
    cfg.listen_port = 2379;
    cfg.election_tick = 10;
    cfg.heartbeat_tick = 1;
    cfg.auto_tls = true;

    cetcd_server *srv = cetcd_server_new(&cfg);
    CETCD_ASSERT_NOT_NULL(srv);
    CETCD_ASSERT_EQ_INT(cetcd_server_start(srv), CETCD_ERR_INVAL);
    cetcd_server_free(srv);
}

CETCD_TEST_CASE(server_start_rejects_peer_auto_tls_without_certs) {
    cetcd_server_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.node_id = 1;
    cfg.listen_port = 2379;
    cfg.election_tick = 10;
    cfg.heartbeat_tick = 1;
    cfg.peer_auto_tls = true;

    cetcd_server *srv = cetcd_server_new(&cfg);
    CETCD_ASSERT_NOT_NULL(srv);
    CETCD_ASSERT_EQ_INT(cetcd_server_start(srv), CETCD_ERR_INVAL);
    cetcd_server_free(srv);
}

CETCD_TEST_CASE(server_start_accepts_auto_tls_with_certs) {
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
    cfg.auto_tls = true;
    cfg.peer_auto_tls = true;
    strncpy(cfg.cert_file, cert, sizeof(cfg.cert_file) - 1);
    strncpy(cfg.key_file, key, sizeof(cfg.key_file) - 1);
    strncpy(cfg.peer_cert_file, cert, sizeof(cfg.peer_cert_file) - 1);
    strncpy(cfg.peer_key_file, key, sizeof(cfg.peer_key_file) - 1);

    cetcd_server *srv = cetcd_server_new(&cfg);
    CETCD_ASSERT_NOT_NULL(srv);
    CETCD_ASSERT_EQ_INT(cetcd_server_start(srv), 0);
    cetcd_server_free(srv);
    cleanup_selfsigned_(dir);
}

static int bytes_contains_(const uint8_t *data, size_t len, const char *s) {
    size_t n = strlen(s);
    if (n == 0 || n > len) return 0;
    for (size_t i = 0; i + n <= len; i++) {
        if (memcmp(data + i, s, n) == 0) return 1;
    }
    return 0;
}

CETCD_TEST_CASE(server_start_rejects_https_advertise_without_tls) {
    cetcd_server_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.node_id = 1;
    cfg.listen_port = 2379;
    cfg.election_tick = 10;
    cfg.heartbeat_tick = 1;
    strncpy(cfg.advertise_client_urls, "https://127.0.0.1:2379",
            sizeof(cfg.advertise_client_urls) - 1);

    cetcd_server *srv = cetcd_server_new(&cfg);
    CETCD_ASSERT_NOT_NULL(srv);
    CETCD_ASSERT_EQ_INT(cetcd_server_start(srv), CETCD_ERR_INVAL);
    cetcd_server_free(srv);
}

CETCD_TEST_CASE(server_start_rejects_https_peer_advertise_without_tls) {
    cetcd_server_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.node_id = 1;
    cfg.listen_port = 2379;
    cfg.election_tick = 10;
    cfg.heartbeat_tick = 1;
    strncpy(cfg.advertise_peer_urls, "https://127.0.0.1:2380",
            sizeof(cfg.advertise_peer_urls) - 1);

    cetcd_server *srv = cetcd_server_new(&cfg);
    CETCD_ASSERT_NOT_NULL(srv);
    CETCD_ASSERT_EQ_INT(cetcd_server_start(srv), CETCD_ERR_INVAL);
    cetcd_server_free(srv);
}

CETCD_TEST_CASE(server_start_member_list_uses_advertise_urls) {
    cetcd_server_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.node_id = 1;
    strncpy(cfg.listen_addr, "127.0.0.1", sizeof(cfg.listen_addr) - 1);
    cfg.listen_port = 23991;
    strncpy(cfg.peer_addr, "127.0.0.1", sizeof(cfg.peer_addr) - 1);
    cfg.peer_port = 23992;
    cfg.election_tick = 10;
    cfg.heartbeat_tick = 1;

    cetcd_server *srv = cetcd_server_new(&cfg);
    CETCD_ASSERT_NOT_NULL(srv);
    CETCD_ASSERT_EQ_INT(cetcd_server_start(srv), 0);

    uint8_t dummy[] = {0x00};
    cetcd_server_rpc_result resp =
        cetcd_server_handle_rpc(srv, "/etcdserverpb.Cluster/MemberList", dummy, 1);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(bytes_contains_(resp.data, resp.len,
                                      "http://127.0.0.1:23991"));
    CETCD_ASSERT_TRUE(bytes_contains_(resp.data, resp.len,
                                      "http://127.0.0.1:23992"));
    CETCD_ASSERT_TRUE(!bytes_contains_(resp.data, resp.len,
                                       "http://127.0.0.1:2379"));
    cetcd_server_rpc_result_free(&resp);

    strncpy(cfg.advertise_client_urls, "http://10.9.8.7:12345",
            sizeof(cfg.advertise_client_urls) - 1);
    strncpy(cfg.advertise_peer_urls, "http://10.9.8.7:12346",
            sizeof(cfg.advertise_peer_urls) - 1);
    cetcd_server_free(srv);
    srv = cetcd_server_new(&cfg);
    CETCD_ASSERT_NOT_NULL(srv);
    CETCD_ASSERT_EQ_INT(cetcd_server_start(srv), 0);
    resp = cetcd_server_handle_rpc(srv, "/etcdserverpb.Cluster/MemberList", dummy, 1);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(bytes_contains_(resp.data, resp.len, "http://10.9.8.7:12345"));
    CETCD_ASSERT_TRUE(bytes_contains_(resp.data, resp.len, "http://10.9.8.7:12346"));
    cetcd_server_rpc_result_free(&resp);
    cetcd_server_free(srv);
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

CETCD_TEST_CASE(server_start_rejects_jwt_ps256) {
    cetcd_server_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.node_id = 1;
    cfg.listen_port = 2379;
    cfg.election_tick = 10;
    cfg.heartbeat_tick = 1;
    strncpy(cfg.auth_token, "jwt,sign-method=PS256,priv-key=/dev/null",
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

CETCD_TEST_CASE(server_start_accepts_jwt_rs256) {
    char tmpl[] = "/tmp/cetcd-srv-jwt-XXXXXX";
    CETCD_ASSERT_NOT_NULL(mkdtemp(tmpl));
    char path[300], cmd[640];
    snprintf(path, sizeof(path), "%s/key.pem", tmpl);
    snprintf(cmd, sizeof(cmd),
             "openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 "
             "-out '%s' >/dev/null 2>&1", path);
    CETCD_ASSERT_EQ_INT(system(cmd), 0);

    cetcd_server_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.node_id = 1;
    cfg.listen_port = 2379;
    cfg.election_tick = 10;
    cfg.heartbeat_tick = 1;
    snprintf(cfg.auth_token, sizeof(cfg.auth_token),
             "jwt,sign-method=RS256,priv-key=%s,ttl=5m", path);

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

CETCD_TEST_CASE(server_start_rejects_huge_max_txn_ops) {
    cetcd_server_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.node_id = 1;
    cfg.listen_port = 2379;
    cfg.election_tick = 10;
    cfg.heartbeat_tick = 1;
    cfg.max_txn_ops = 129;

    cetcd_server *srv = cetcd_server_new(&cfg);
    CETCD_ASSERT_NOT_NULL(srv);
    CETCD_ASSERT_EQ_INT(cetcd_server_start(srv), CETCD_ERR_INVAL);
    cetcd_server_free(srv);
}

CETCD_TEST_CASE(server_start_applies_max_txn_ops) {
    cetcd_server_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.node_id = 1;
    cfg.listen_port = 2379;
    cfg.election_tick = 10;
    cfg.heartbeat_tick = 1;
    cfg.max_txn_ops = 2;

    cetcd_server *srv = cetcd_server_new(&cfg);
    CETCD_ASSERT_NOT_NULL(srv);
    CETCD_ASSERT_EQ_INT(cetcd_server_start(srv), 0);

    uint8_t txn_buf[256];
    size_t tpos = 0;
    for (int i = 0; i < 3; i++) {
        uint8_t put_inner[8]; size_t p = 0;
        put_inner[p++] = 0x0a; put_inner[p++] = 1; put_inner[p++] = 'k';
        put_inner[p++] = 0x12; put_inner[p++] = 1; put_inner[p++] = 'v';
        uint8_t op[16]; size_t o = 0;
        op[o++] = 0x12;
        op[o++] = (uint8_t)p;
        memcpy(op + o, put_inner, p); o += p;
        txn_buf[tpos++] = 0x12;
        txn_buf[tpos++] = (uint8_t)o;
        memcpy(txn_buf + tpos, op, o); tpos += o;
    }
    cetcd_server_rpc_result resp =
        cetcd_server_handle_rpc(srv, "/etcdserverpb.KV/Txn", txn_buf, tpos);
    CETCD_ASSERT_TRUE(resp.data == NULL || resp.len == 0);
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
    CETCD_TEST_ENTRY(server_wal_replay_compact),
    CETCD_TEST_ENTRY(server_wal_replay_lease_grant),
    CETCD_TEST_ENTRY(server_wal_replay_lease_keepalive),
    CETCD_TEST_ENTRY(server_wal_replay_auth_user_add),
    CETCD_TEST_ENTRY(server_wal_replay_auth_user_change_pass),
    CETCD_TEST_ENTRY(server_wal_replay_auth_enabled),
    CETCD_TEST_ENTRY(server_wal_replay_auth_user_delete),
    CETCD_TEST_ENTRY(server_wal_replay_auth_role_add),
    CETCD_TEST_ENTRY(server_wal_replay_auth_role_delete),
    CETCD_TEST_ENTRY(server_wal_replay_auth_user_grant_role),
    CETCD_TEST_ENTRY(server_wal_replay_auth_user_revoke_role),
    CETCD_TEST_ENTRY(server_wal_replay_auth_role_grant_perm),
    CETCD_TEST_ENTRY(server_wal_replay_auth_role_revoke_perm),
    CETCD_TEST_ENTRY(server_wal_replay_alarm),
    CETCD_TEST_ENTRY(server_alarm_survives_restart),
    CETCD_TEST_ENTRY(server_txn_put_survives_restart),
    CETCD_TEST_ENTRY(server_lease_survives_restart),
    CETCD_TEST_ENTRY(server_snapshot_count_truncates_wal),
    CETCD_TEST_ENTRY(server_member_add_survives_restart),
    CETCD_TEST_ENTRY(server_start_rejects_cert_without_key),
    CETCD_TEST_ENTRY(server_start_rejects_missing_tls_files),
    CETCD_TEST_ENTRY(server_start_rejects_client_auth_without_ca),
    CETCD_TEST_ENTRY(server_start_loads_peer_tls),
    CETCD_TEST_ENTRY(server_start_rejects_cipher_suites_without_tls),
    CETCD_TEST_ENTRY(server_start_rejects_unknown_cipher_suites),
    CETCD_TEST_ENTRY(server_start_accepts_iana_cipher_suites),
    CETCD_TEST_ENTRY(server_start_rejects_https_listen_without_tls),
    CETCD_TEST_ENTRY(server_start_rejects_https_peer_listen_without_tls),
    CETCD_TEST_ENTRY(server_start_accepts_https_listen_with_certs),
    CETCD_TEST_ENTRY(server_start_rejects_force_new_cluster),
    CETCD_TEST_ENTRY(server_start_rejects_cluster_state_existing),
    CETCD_TEST_ENTRY(server_start_accepts_cluster_state_new),
    CETCD_TEST_ENTRY(server_start_rejects_https_initial_cluster_without_peer_tls),
    CETCD_TEST_ENTRY(server_start_accepts_https_initial_cluster_with_peer_tls),
    CETCD_TEST_ENTRY(server_start_rejects_grpc_keepalive_timeout_without_time),
    CETCD_TEST_ENTRY(server_start_accepts_grpc_keepalive_time),
    CETCD_TEST_ENTRY(server_start_rejects_auto_tls_without_certs),
    CETCD_TEST_ENTRY(server_start_rejects_peer_auto_tls_without_certs),
    CETCD_TEST_ENTRY(server_start_accepts_auto_tls_with_certs),
    CETCD_TEST_ENTRY(server_start_rejects_https_advertise_without_tls),
    CETCD_TEST_ENTRY(server_start_rejects_https_peer_advertise_without_tls),
    CETCD_TEST_ENTRY(server_start_member_list_uses_advertise_urls),
    CETCD_TEST_ENTRY(server_start_rejects_jwt_without_key),
    CETCD_TEST_ENTRY(server_start_rejects_jwt_ps256),
    CETCD_TEST_ENTRY(server_start_accepts_jwt_hs256),
    CETCD_TEST_ENTRY(server_start_accepts_jwt_rs256),
    CETCD_TEST_ENTRY(server_start_rejects_bad_bcrypt_cost),
    CETCD_TEST_ENTRY(server_start_accepts_simple_auth_token),
    CETCD_TEST_ENTRY(server_start_small_max_request_bytes),
    CETCD_TEST_ENTRY(server_start_rejects_huge_max_txn_ops),
    CETCD_TEST_ENTRY(server_start_applies_max_txn_ops),
CETCD_TEST_LIST_END

CETCD_TEST_MAIN()
