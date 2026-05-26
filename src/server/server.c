#include "cetcd/server.h"
#include "cetcd/v3rpc.h"
#include "cetcd/raft.h"
#include "cetcd/mvcc.h"
#include "cetcd/lease.h"
#include "cetcd/auth.h"
#include "cetcd/peer.h"
#include "cetcd/snap.h"
#include "cetcd/wal.h"
#include "cetcd/backend.h"
#include "cetcd/io.h"

#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <uv.h>
#include "io_internal.h"

struct cetcd_server {
    cetcd_server_config  cfg;
    cetcd_v3rpc         *rpc;
    cetcd_raft          *raft;
    cetcd_cluster       *cluster;
    cetcd_backend       *backend;
    cetcd_wal_encoder   *wal_enc;
    cetcd_loop          *loop;
    cetcd_tcp           *listener;
    cetcd_tcp           *peer_listener;
    bool                 started;
};

static int ensure_dir(const char *path) {
    struct stat st;
    if (stat(path, &st) == 0) return 0;
    return mkdir(path, 0755);
}

cetcd_server *cetcd_server_new(const cetcd_server_config *cfg) {
    if (!cfg) return NULL;
    cetcd_server *srv = (cetcd_server *)calloc(1, sizeof(*srv));
    if (!srv) return NULL;
    memcpy(&srv->cfg, cfg, sizeof(*cfg));

    srv->rpc = cetcd_v3rpc_new();
    if (!srv->rpc) { free(srv); return NULL; }

    cetcd_raft_config raft_cfg = {
        .id = cfg->node_id,
        .election_tick = cfg->election_tick ? cfg->election_tick : 10,
        .heartbeat_tick = cfg->heartbeat_tick ? cfg->heartbeat_tick : 1,
        .storage = NULL,
        .max_size_per_msg = 1024 * 1024,
        .max_inflight_msgs = 256,
        .check_quorum = true,
        .pre_vote = true,
        .disable_proposal_forwarding = false,
    };
    srv->raft = cetcd_raft_new(&raft_cfg);

    srv->cluster = cetcd_cluster_new(cfg->node_id);

    if (cfg->auth_enabled) {
        extern cetcd_auth_store *g_rpc_auth;
        if (g_rpc_auth) cetcd_auth_set_enabled(g_rpc_auth, true);
    }

    srv->started = false;
    return srv;
}

void cetcd_server_free(cetcd_server *srv) {
    if (!srv) return;
    if (srv->peer_listener) { cetcd_tcp_free(srv->peer_listener); srv->peer_listener = NULL; }
    if (srv->listener) { cetcd_tcp_free(srv->listener); srv->listener = NULL; }
    if (srv->loop) { cetcd_loop_free(srv->loop); srv->loop = NULL; }
    if (srv->wal_enc) { cetcd_wal_encoder_flush(srv->wal_enc); cetcd_wal_encoder_free(srv->wal_enc); }
    if (srv->backend) cetcd_backend_close(srv->backend);
    if (srv->cluster) cetcd_cluster_free(srv->cluster);
    if (srv->raft) cetcd_raft_free(srv->raft);
    if (srv->rpc) cetcd_v3rpc_free(srv->rpc);
    free(srv);
}

int cetcd_server_start(cetcd_server *srv) {
    if (!srv) return CETCD_ERR_INVAL;

    if (srv->cfg.data_dir[0]) {
        ensure_dir(srv->cfg.data_dir);

        if (!srv->backend) {
            cetcd_backend_config be_cfg;
            memset(&be_cfg, 0, sizeof(be_cfg));
            be_cfg.path = srv->cfg.data_dir;
            be_cfg.map_size = 64 * 1024 * 1024;
            be_cfg.max_dbs = 16;
            srv->backend = cetcd_backend_open(&be_cfg);
        }

        if (!srv->wal_enc) {
            char wal_path[600];
            snprintf(wal_path, sizeof(wal_path), "%s/wal", srv->cfg.data_dir);
            ensure_dir(srv->cfg.data_dir);
            srv->wal_enc = cetcd_wal_encoder_create(wal_path);
        }
    }

    for (uint32_t i = 0; i < srv->cfg.n_initial_peers; i++) {
        cetcd_cluster_add_peer(srv->cluster, &srv->cfg.initial_peers[i]);
    }

    srv->started = true;
    return 0;
}

void cetcd_server_stop(cetcd_server *srv) {
    if (srv) srv->started = false;
}

int cetcd_server_apply(cetcd_server *srv) {
    if (!srv || !srv->raft) return CETCD_ERR_INVAL;
    cetcd_ready rd = cetcd_raft_ready(srv->raft);
    cetcd_ready_free(&rd);
    return 0;
}

cetcd_server_rpc_result cetcd_server_handle_rpc(cetcd_server *srv,
                                                  const char *path,
                                                  const uint8_t *req,
                                                  size_t req_len) {
    cetcd_server_rpc_result result = {NULL, 0};
    if (!srv || !srv->rpc || !path) return result;
    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(srv->rpc, path, req, req_len);
    result.data = resp.data;
    result.len = resp.len;
    return result;
}

void cetcd_server_rpc_result_free(cetcd_server_rpc_result *r) {
    if (!r) return;
    if (r->data) { free(r->data); r->data = NULL; }
    r->len = 0;
}

void cetcd_server_tick(cetcd_server *srv) {
    if (!srv || !srv->raft) return;
    cetcd_raft_tick(srv->raft);
}

int cetcd_server_compact(cetcd_server *srv, int64_t rev) {
    if (!srv || !srv->rpc) return CETCD_ERR_INVAL;
    extern cetcd_mvcc_store *g_rpc_store;
    if (!g_rpc_store) return CETCD_ERR_INVAL;
    return cetcd_mvcc_compact(g_rpc_store, rev);
}

cetcd_snap *cetcd_server_snapshot(cetcd_server *srv) {
    if (!srv) return NULL;
    extern cetcd_mvcc_store *g_rpc_store;
    if (!g_rpc_store) return NULL;

    cetcd_snap *snap = cetcd_snap_new();
    if (!snap) return NULL;

    int64_t rev = cetcd_mvcc_revision(g_rpc_store);
    (void)rev;

    cetcd_kv *kvs = NULL;
    size_t kv_count = 0;
    int rc = cetcd_mvcc_range(g_rpc_store, 0,
                              (const uint8_t *)"", 0,
                              (const uint8_t *)"\xff", 1,
                              &kvs, &kv_count);
    if (rc == 0 && kvs) {
        for (size_t i = 0; i < kv_count; i++) {
            cetcd_snap_add_entry(snap,
                                 kvs[i].key.data, kvs[i].key.len,
                                 kvs[i].value.data, kvs[i].value.len,
                                 kvs[i].mod_rev.main);
        }
        cetcd_kv_free_contents(kvs, kv_count);
    }
    return snap;
}

static void on_client_conn(cetcd_tcp *server, cetcd_tcp *client, void *arg) {
    (void)server; (void)client; (void)arg;
}

int cetcd_server_serve(cetcd_server *srv) {
    if (!srv) return CETCD_ERR_INVAL;

    srv->loop = cetcd_loop_new();
    if (!srv->loop) return CETCD_ERR_INTERNAL;

    srv->listener = cetcd_tcp_new(srv->loop);
    if (!srv->listener) { cetcd_loop_free(srv->loop); srv->loop = NULL; return CETCD_ERR_INTERNAL; }

    int rc = cetcd_tcp_bind(srv->listener, srv->cfg.listen_addr, srv->cfg.listen_port);
    if (rc != 0) {
        cetcd_tcp_free(srv->listener); srv->listener = NULL;
        cetcd_loop_free(srv->loop); srv->loop = NULL;
        return CETCD_ERR_IO;
    }

    rc = cetcd_tcp_listen(srv->listener, on_client_conn, srv);
    if (rc != 0) {
        cetcd_tcp_free(srv->listener); srv->listener = NULL;
        cetcd_loop_free(srv->loop); srv->loop = NULL;
        return CETCD_ERR_IO;
    }

    if (srv->cfg.peer_port > 0) {
        const char *peer_addr = srv->cfg.peer_addr[0] ? srv->cfg.peer_addr : srv->cfg.listen_addr;
        srv->peer_listener = cetcd_tcp_new(srv->loop);
        if (srv->peer_listener) {
            rc = cetcd_tcp_bind(srv->peer_listener, peer_addr, srv->cfg.peer_port);
            if (rc == 0) {
                cetcd_tcp_listen(srv->peer_listener, on_client_conn, srv);
            }
        }
    }

    srv->started = true;
    cetcd_loop_run(srv->loop);
    return 0;
}

int64_t cetcd_server_revision(const cetcd_server *srv) {
    if (!srv) return 0;
    extern cetcd_mvcc_store *g_rpc_store;
    if (!g_rpc_store) return 0;
    return cetcd_mvcc_revision(g_rpc_store);
}

bool cetcd_server_is_leader(const cetcd_server *srv) {
    if (!srv || !srv->raft) return false;
    return cetcd_raft_leader(srv->raft) == srv->cfg.node_id;
}

uint64_t cetcd_server_node_id(const cetcd_server *srv) {
    return srv ? srv->cfg.node_id : 0;
}

size_t cetcd_server_peer_count(const cetcd_server *srv) {
    if (!srv || !srv->cluster) return 0;
    return cetcd_cluster_peer_count(srv->cluster);
}

int cetcd_server_add_peer(cetcd_server *srv, const cetcd_peer_info *info) {
    if (!srv || !srv->cluster || !info) return CETCD_ERR_INVAL;
    return cetcd_cluster_add_peer(srv->cluster, info);
}

int cetcd_server_remove_peer(cetcd_server *srv, uint64_t peer_id) {
    if (!srv || !srv->cluster) return CETCD_ERR_INVAL;
    return cetcd_cluster_remove_peer(srv->cluster, peer_id);
}

int cetcd_server_propose_conf_change(cetcd_server *srv, uint64_t peer_id, int change_type) {
    if (!srv || !srv->raft) return CETCD_ERR_INVAL;
    uint8_t buf[16];
    size_t pos = 0;
    buf[pos++] = 0x08;
    buf[pos++] = (uint8_t)change_type;
    buf[pos++] = 0x10;
    do { uint8_t b = peer_id & 0x7F; peer_id >>= 7;
         if (peer_id) b |= 0x80; buf[pos++] = b; } while (peer_id);
    return cetcd_raft_propose_conf_change(srv->raft, buf, pos);
}
