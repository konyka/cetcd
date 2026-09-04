#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#include "cetcd/v3rpc.h"
#include "cetcd/mvcc.h"
#include "cetcd/lease.h"
#include "cetcd/auth.h"
#include "cetcd/peer.h"
#include "cetcd/raft.h"
#include "cetcd/backend.h"

/* Forward declarations for per RPC handlers (defined in separate files) */
cetcd_rpc_bytes kv_handle_put(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes kv_handle_range(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes kv_handle_range_stream(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes kv_handle_delete_range(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes kv_handle_txn(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes lease_handle_grant(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes lease_handle_revoke(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes lease_handle_keep_alive(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes lease_handle_time_to_live(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes lease_handle_leases(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes watch_handle_watch(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes auth_handle_enable(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes auth_handle_disable(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes auth_handle_authenticate(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes auth_handle_user_add(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes auth_handle_user_delete(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes auth_handle_role_add(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes auth_handle_role_grant(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes maint_handle_status(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes maint_handle_defragment(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes maint_handle_hash(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes maint_handle_hash_kv(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes maint_handle_alarm(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes maint_handle_move_leader(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes maint_handle_snapshot(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes maint_handle_downgrade(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);

/* Additional auth handlers (auth_handler.c) */
cetcd_rpc_bytes auth_handle_status(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes auth_handle_user_list(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes auth_handle_user_change_password(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes auth_handle_role_list(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes auth_handle_role_delete(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes auth_handle_user_revoke_role(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes auth_handle_user_get(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes auth_handle_role_get(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes auth_handle_role_grant_permission(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes auth_handle_role_revoke_permission(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);

/* KV/Compact (kv_handler.c) */
cetcd_rpc_bytes kv_handle_compact(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);

/* Cluster handlers (cluster_handler.c) */
cetcd_rpc_bytes cluster_handle_member_list(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes cluster_handle_member_add(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes cluster_handle_member_remove(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes cluster_handle_member_update(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes cluster_handle_member_promote(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);

struct cetcd_v3rpc {
    cetcd_mvcc_store *store;
    cetcd_lease_mgr  *leases;
    cetcd_auth_store *auth;
};

/* Global handles for internal helpers used by RPC handlers in other files */
cetcd_mvcc_store *g_rpc_store = NULL;
cetcd_lease_mgr  *g_rpc_lease_mgr = NULL;
cetcd_auth_store *g_rpc_auth = NULL;
cetcd_cluster    *g_rpc_cluster = NULL;
cetcd_raft       *g_rpc_raft = NULL;
uint64_t          g_rpc_node_id = 0;
const char       *g_rpc_auth_user = NULL;
cetcd_backend    *g_rpc_auth_backend = NULL;
uint64_t          g_rpc_quota_bytes = 0;
uint64_t          g_rpc_max_txn_ops = 128;

/* Streaming support: event loop and write callback for streaming RPCs */
cetcd_loop           *g_rpc_loop = NULL;
cetcd_stream_write_fn g_rpc_stream_write_fn = NULL;
void                 *g_rpc_stream_write_ctx = NULL;

cetcd_v3rpc *cetcd_v3rpc_new(void) {
    cetcd_v3rpc *rpc = (cetcd_v3rpc *)calloc(1, sizeof(*rpc));
    if (!rpc) return NULL;
    rpc->store = cetcd_mvcc_store_new();
    rpc->leases = cetcd_lease_mgr_new(NULL, NULL);
    rpc->auth = cetcd_auth_store_new();
    g_rpc_store = rpc->store;
    g_rpc_lease_mgr = rpc->leases;
    g_rpc_auth = rpc->auth;
    g_rpc_max_txn_ops = 128;
    cetcd_v3rpc_alarm_load(NULL);
    return rpc;
}

void cetcd_v3rpc_free(cetcd_v3rpc *rpc) {
    if (!rpc) return;
    cetcd_v3rpc_alarm_load(NULL);
    if (rpc->store) {
        cetcd_mvcc_store_free(rpc->store);
        rpc->store = NULL;
    }
    if (rpc->leases) {
        cetcd_lease_mgr_free(rpc->leases);
        rpc->leases = NULL;
    }
    if (rpc->auth) {
        cetcd_auth_store_free(rpc->auth);
        rpc->auth = NULL;
    }
    if (g_rpc_auth_backend) g_rpc_auth_backend = NULL;
    g_rpc_max_txn_ops = 128;
    free(rpc);
}

static cetcd_rpc_bytes cetcd_v3rpc_dispatch_inner_(cetcd_v3rpc *rpc,
                                 const char *path,
                                 const uint8_t *req_data,
                                 size_t req_len) {
    /* Dispatch known paths to handlers */
    if (strcmp(path, "/etcdserverpb.KV/Put") == 0) {
        return kv_handle_put(rpc, req_data, req_len);
    }
    if (strcmp(path, "/etcdserverpb.KV/Range") == 0) {
        return kv_handle_range(rpc, req_data, req_len);
    }
    if (strcmp(path, "/etcdserverpb.KV/RangeStream") == 0) {
        return kv_handle_range_stream(rpc, req_data, req_len);
    }
    if (strcmp(path, "/etcdserverpb.KV/DeleteRange") == 0) {
        return kv_handle_delete_range(rpc, req_data, req_len);
    }
    if (strcmp(path, "/etcdserverpb.KV/Txn") == 0) {
        return kv_handle_txn(rpc, req_data, req_len);
    }
    if (strcmp(path, "/etcdserverpb.Lease/LeaseGrant") == 0) {
        return lease_handle_grant(rpc, req_data, req_len);
    }
    if (strcmp(path, "/etcdserverpb.Lease/LeaseRevoke") == 0) {
        return lease_handle_revoke(rpc, req_data, req_len);
    }
    if (strcmp(path, "/etcdserverpb.Lease/LeaseKeepAlive") == 0) {
        return lease_handle_keep_alive(rpc, req_data, req_len);
    }
    if (strcmp(path, "/etcdserverpb.Lease/LeaseTimeToLive") == 0) {
        return lease_handle_time_to_live(rpc, req_data, req_len);
    }
    if (strcmp(path, "/etcdserverpb.Lease/LeaseLeases") == 0) {
        return lease_handle_leases(rpc, req_data, req_len);
    }
    if (strcmp(path, "/etcdserverpb.Watch/Watch") == 0) {
        return watch_handle_watch(rpc, req_data, req_len);
    }
    /* Maintenance */
    if (strcmp(path, "/etcdserverpb.Maintenance/Status") == 0) {
        return maint_handle_status(rpc, req_data, req_len);
    }
    if (strcmp(path, "/etcdserverpb.Maintenance/Defragment") == 0) {
        return maint_handle_defragment(rpc, req_data, req_len);
    }
    if (strcmp(path, "/etcdserverpb.Maintenance/Hash") == 0) {
        return maint_handle_hash(rpc, req_data, req_len);
    }
    if (strcmp(path, "/etcdserverpb.Maintenance/HashKV") == 0) {
        return maint_handle_hash_kv(rpc, req_data, req_len);
    }
    if (strcmp(path, "/etcdserverpb.Maintenance/Alarm") == 0) {
        return maint_handle_alarm(rpc, req_data, req_len);
    }
    if (strcmp(path, "/etcdserverpb.Maintenance/MoveLeader") == 0) {
        return maint_handle_move_leader(rpc, req_data, req_len);
    }
    if (strcmp(path, "/etcdserverpb.Maintenance/Snapshot") == 0) {
        return maint_handle_snapshot(rpc, req_data, req_len);
    }
    if (strcmp(path, "/etcdserverpb.Maintenance/Downgrade") == 0) {
        return maint_handle_downgrade(rpc, req_data, req_len);
    }

    /* KV/Compact */
    if (strcmp(path, "/etcdserverpb.KV/Compact") == 0) {
        return kv_handle_compact(rpc, req_data, req_len);
    }

    /* Cluster */
    if (strcmp(path, "/etcdserverpb.Cluster/MemberList") == 0) {
        return cluster_handle_member_list(rpc, req_data, req_len);
    }
    if (strcmp(path, "/etcdserverpb.Cluster/MemberAdd") == 0) {
        return cluster_handle_member_add(rpc, req_data, req_len);
    }
    if (strcmp(path, "/etcdserverpb.Cluster/MemberRemove") == 0) {
        return cluster_handle_member_remove(rpc, req_data, req_len);
    }
    if (strcmp(path, "/etcdserverpb.Cluster/MemberUpdate") == 0) {
        return cluster_handle_member_update(rpc, req_data, req_len);
    }
    if (strcmp(path, "/etcdserverpb.Cluster/MemberPromote") == 0) {
        return cluster_handle_member_promote(rpc, req_data, req_len);
    }

    /* Auth */
    if (strcmp(path, "/etcdserverpb.Auth/AuthEnable") == 0) {
        return auth_handle_enable(rpc, req_data, req_len);
    }
    if (strcmp(path, "/etcdserverpb.Auth/AuthDisable") == 0) {
        return auth_handle_disable(rpc, req_data, req_len);
    }
    if (strcmp(path, "/etcdserverpb.Auth/Authenticate") == 0) {
        return auth_handle_authenticate(rpc, req_data, req_len);
    }
    if (strcmp(path, "/etcdserverpb.Auth/UserAdd") == 0) {
        return auth_handle_user_add(rpc, req_data, req_len);
    }
    if (strcmp(path, "/etcdserverpb.Auth/UserDelete") == 0) {
        return auth_handle_user_delete(rpc, req_data, req_len);
    }
    if (strcmp(path, "/etcdserverpb.Auth/RoleAdd") == 0) {
        return auth_handle_role_add(rpc, req_data, req_len);
    }
    if (strcmp(path, "/etcdserverpb.Auth/UserGrantRole") == 0) {
        return auth_handle_role_grant(rpc, req_data, req_len);
    }

    /* Additional Auth RPCs */
    if (strcmp(path, "/etcdserverpb.Auth/AuthStatus") == 0) {
        return auth_handle_status(rpc, req_data, req_len);
    }
    if (strcmp(path, "/etcdserverpb.Auth/UserList") == 0) {
        return auth_handle_user_list(rpc, req_data, req_len);
    }
    if (strcmp(path, "/etcdserverpb.Auth/UserChangePassword") == 0) {
        return auth_handle_user_change_password(rpc, req_data, req_len);
    }
    if (strcmp(path, "/etcdserverpb.Auth/RoleList") == 0) {
        return auth_handle_role_list(rpc, req_data, req_len);
    }
    if (strcmp(path, "/etcdserverpb.Auth/RoleDelete") == 0) {
        return auth_handle_role_delete(rpc, req_data, req_len);
    }
    if (strcmp(path, "/etcdserverpb.Auth/UserRevokeRole") == 0) {
        return auth_handle_user_revoke_role(rpc, req_data, req_len);
    }
    if (strcmp(path, "/etcdserverpb.Auth/UserGet") == 0) {
        return auth_handle_user_get(rpc, req_data, req_len);
    }
    if (strcmp(path, "/etcdserverpb.Auth/RoleGet") == 0) {
        return auth_handle_role_get(rpc, req_data, req_len);
    }
    if (strcmp(path, "/etcdserverpb.Auth/RoleGrantPermission") == 0) {
        return auth_handle_role_grant_permission(rpc, req_data, req_len);
    }
    if (strcmp(path, "/etcdserverpb.Auth/RoleRevokePermission") == 0) {
        return auth_handle_role_revoke_permission(rpc, req_data, req_len);
    }

    /* Unknown path */
    cetcd_rpc_bytes empty = {NULL, 0};
    return empty;
}

static int path_is_authenticate_(const char *path) {
    return strcmp(path, "/etcdserverpb.Auth/Authenticate") == 0;
}

static int path_requires_admin_(const char *path) {
    if (strncmp(path, "/etcdserverpb.Auth/", 19) == 0) {
        if (path_is_authenticate_(path)) return 0;
        if (strcmp(path, "/etcdserverpb.Auth/AuthStatus") == 0) return 0;
        return 1;
    }
    if (strcmp(path, "/etcdserverpb.Cluster/MemberAdd") == 0) return 1;
    if (strcmp(path, "/etcdserverpb.Cluster/MemberRemove") == 0) return 1;
    if (strcmp(path, "/etcdserverpb.Cluster/MemberUpdate") == 0) return 1;
    if (strcmp(path, "/etcdserverpb.Cluster/MemberPromote") == 0) return 1;
    if (strncmp(path, "/etcdserverpb.Maintenance/", 26) == 0 &&
        strcmp(path, "/etcdserverpb.Maintenance/Status") != 0)
        return 1;
    return 0;
}

static int path_auth_mutates_(const char *path) {
    return strncmp(path, "/etcdserverpb.Auth/", 19) == 0 &&
           !path_is_authenticate_(path) &&
           strcmp(path, "/etcdserverpb.Auth/AuthStatus") != 0 &&
           strcmp(path, "/etcdserverpb.Auth/UserList") != 0 &&
           strcmp(path, "/etcdserverpb.Auth/UserGet") != 0 &&
           strcmp(path, "/etcdserverpb.Auth/RoleList") != 0 &&
           strcmp(path, "/etcdserverpb.Auth/RoleGet") != 0;
}

void cetcd_v3rpc_set_auth_backend(cetcd_v3rpc *rpc, struct cetcd_backend *be) {
    (void)rpc;
    g_rpc_auth_backend = be;
}

void cetcd_v3rpc_set_quota(uint64_t bytes) {
    g_rpc_quota_bytes = bytes;
}

void cetcd_v3rpc_set_max_txn_ops(uint64_t n) {
    if (n == 0) n = 128;
    if (n > 128) n = 128;
    g_rpc_max_txn_ops = n;
}

void cetcd_v3rpc_auth_persist(void) {
    if (g_rpc_auth && g_rpc_auth_backend)
        (void)cetcd_auth_save(g_rpc_auth, g_rpc_auth_backend);
}

int cetcd_v3rpc_check_key_perm(int want_write, const uint8_t *key, size_t key_len) {
    if (!g_rpc_auth || !cetcd_auth_is_enabled(g_rpc_auth)) return 0;
    if (!g_rpc_auth_user || !key) return -1;
    return cetcd_auth_check_perm(g_rpc_auth, g_rpc_auth_user, key, key_len, want_write)
               ? 0 : -1;
}

cetcd_rpc_bytes cetcd_v3rpc_dispatch_ex(cetcd_v3rpc *rpc,
                                         const char *path,
                                         const uint8_t *req_data,
                                         size_t req_len,
                                         const char *token) {
    cetcd_rpc_bytes empty = {NULL, 0};
    g_rpc_auth_user = NULL;
    if (!rpc || !path || !req_data) return empty;

    if (g_rpc_auth && cetcd_auth_is_enabled(g_rpc_auth) &&
        !path_is_authenticate_(path)) {
        const char *user = cetcd_auth_user_for_token(
            g_rpc_auth, token, cetcd_clock_realtime_ns());
        if (!user) return empty;
        g_rpc_auth_user = user;
        if (path_requires_admin_(path) &&
            !cetcd_auth_is_admin(g_rpc_auth, user)) {
            g_rpc_auth_user = NULL;
            return empty;
        }
    }

    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch_inner_(rpc, path, req_data, req_len);
    if (resp.data && path_auth_mutates_(path))
        cetcd_v3rpc_auth_persist();
    g_rpc_auth_user = NULL;
    return resp;
}

cetcd_rpc_bytes cetcd_v3rpc_dispatch(cetcd_v3rpc *rpc,
                                 const char *path,
                                 const uint8_t *req_data,
                                 size_t req_len) {
    return cetcd_v3rpc_dispatch_ex(rpc, path, req_data, req_len, NULL);
}

void cetcd_rpc_bytes_free(cetcd_rpc_bytes *b) {
    if (!b) return;
    if (b->data) {
        free(b->data);
        b->data = NULL;
    }
    b->len = 0;
}

void cetcd_v3rpc_set_loop(cetcd_v3rpc *rpc, cetcd_loop *loop) {
    if (!rpc) return;
    g_rpc_loop = loop;
    (void)rpc;
}

void cetcd_v3rpc_set_stream_writer(cetcd_v3rpc *rpc,
                                    cetcd_stream_write_fn fn,
                                    void *ctx) {
    if (!rpc) return;
    g_rpc_stream_write_fn = fn;
    g_rpc_stream_write_ctx = ctx;
    (void)rpc;
}

cetcd_mvcc_store *cetcd_v3rpc_store(cetcd_v3rpc *rpc) {
    return rpc ? rpc->store : NULL;
}

cetcd_lease_mgr *cetcd_v3rpc_leases(cetcd_v3rpc *rpc) {
    return rpc ? rpc->leases : NULL;
}

/* cetcd_v3rpc_detach_stream_writer is implemented in watch_handler.c */
