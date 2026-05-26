#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#include "cetcd/v3rpc.h"
#include "cetcd/mvcc.h"
#include "cetcd/lease.h"

/* Forward declarations for per RPC handlers (defined in separate files) */
cetcd_rpc_bytes kv_handle_put(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes kv_handle_range(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes kv_handle_delete_range(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes lease_handle_grant(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);

struct cetcd_v3rpc {
    cetcd_mvcc_store *store;
    cetcd_lease_mgr  *leases;
};

/* Global handles for internal helpers used by RPC handlers in other files */
cetcd_mvcc_store *g_rpc_store = NULL;
cetcd_lease_mgr  *g_rpc_lease_mgr = NULL;

cetcd_v3rpc *cetcd_v3rpc_new(void) {
    cetcd_v3rpc *rpc = (cetcd_v3rpc *)calloc(1, sizeof(*rpc));
    if (!rpc) return NULL;
    rpc->store = cetcd_mvcc_store_new();
    /* Lease manager with no expire callback for tests */
    rpc->leases = cetcd_lease_mgr_new(NULL, NULL);
    /* Expose internals for other modules via globals */
    g_rpc_store = rpc->store;
    g_rpc_lease_mgr = rpc->leases;
    return rpc;
}

void cetcd_v3rpc_free(cetcd_v3rpc *rpc) {
    if (!rpc) return;
    if (rpc->store) {
        cetcd_mvcc_store_free(rpc->store);
        rpc->store = NULL;
    }
    if (rpc->leases) {
        cetcd_lease_mgr_free(rpc->leases);
        rpc->leases = NULL;
    }
    free(rpc);
}

static cetcd_rpc_bytes make_response(const uint8_t *data, size_t len) {
    cetcd_rpc_bytes b;
    if (!data || len == 0) {
        b.data = NULL;
        b.len = 0;
        return b;
    }
    uint8_t *buf = (uint8_t *)malloc(len);
    if (!buf) {
        b.data = NULL;
        b.len = 0;
        return b;
    }
    memcpy(buf, data, len);
    b.data = buf;
    b.len = len;
    return b;
}

cetcd_rpc_bytes cetcd_v3rpc_dispatch(cetcd_v3rpc *rpc,
                                 const char *path,
                                 const uint8_t *req_data,
                                 size_t req_len) {
    if (!rpc || !path || !req_data) {
        cetcd_rpc_bytes empty = {NULL, 0};
        return empty;
    }
    /* Dispatch known paths to handlers */
    if (strcmp(path, "/etcdserverpb.KV/Put") == 0) {
        return kv_handle_put(rpc, req_data, req_len);
    }
    if (strcmp(path, "/etcdserverpb.KV/Range") == 0) {
        return kv_handle_range(rpc, req_data, req_len);
    }
    if (strcmp(path, "/etcdserverpb.KV/DeleteRange") == 0) {
        return kv_handle_delete_range(rpc, req_data, req_len);
    }
    if (strcmp(path, "/etcdserverpb.KV/Txn") == 0) {
        /* Not implemented in tests; return empty */
        cetcd_rpc_bytes empty = {NULL, 0};
        return empty;
    }
    if (strcmp(path, "/etcdserverpb.Lease/LeaseGrant") == 0) {
        return lease_handle_grant(rpc, req_data, req_len);
    }
    if (strcmp(path, "/etcdserverpb.Lease/LeaseRevoke") == 0) {
        cetcd_rpc_bytes empty = {NULL, 0};
        return empty;
    }
    if (strcmp(path, "/etcdserverpb.Lease/LeaseKeepAlive") == 0) {
        cetcd_rpc_bytes empty = {NULL, 0};
        return empty;
    }
    if (strcmp(path, "/etcdserverpb.Watch/Watch") == 0) {
        cetcd_rpc_bytes empty = {NULL, 0};
        return empty;
    }

    /* Unknown path */
    cetcd_rpc_bytes empty = {NULL, 0};
    return empty;
}

void cetcd_rpc_bytes_free(cetcd_rpc_bytes *b) {
    if (!b) return;
    if (b->data) {
        free(b->data);
        b->data = NULL;
    }
    b->len = 0;
}
