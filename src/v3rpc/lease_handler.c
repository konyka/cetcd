#include <stdlib.h>
#include <string.h>

#include "cetcd/v3rpc.h"
#include "cetcd/lease.h"
#include "cetcd/mvcc.h"

extern cetcd_mvcc_store *g_rpc_store;
extern cetcd_lease_mgr  *g_rpc_lease_mgr;

/* Forward declarations for all lease handlers */
cetcd_rpc_bytes lease_handle_grant(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes lease_handle_revoke(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes lease_handle_keep_alive(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes lease_handle_time_to_live(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes lease_handle_leases(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);

/* Helper to read a simple varint from req starting at pos */
static int read_varint_local(const uint8_t *buf, size_t len, size_t *pos, int64_t *out) {
    uint64_t val = 0; int shift = 0;
    while (*pos < len) {
        uint8_t b = buf[*pos]; (*pos)++;
        val |= (uint64_t)(b & 0x7F) << shift;
        if ((b & 0x80) == 0) { *out = (int64_t)val; return 0; }
        shift += 7; if (shift > 63) break;
    }
    return -1;
}

static int write_varint(uint8_t *buf, size_t cap, size_t *pos, uint64_t val) {
    while (*pos < cap) {
        uint8_t b = val & 0x7F;
        val >>= 7;
        if (val) b |= 0x80;
        buf[(*pos)++] = b;
        if (!val) return 0;
    }
    return -1;
}

static cetcd_rpc_bytes make_lease_response(cetcd_lease_id id, int64_t ttl) {
    /* LeaseGrantResponse:
     *   field 1 (ID)   = varint, tag = 0x08
     *   field 2 (TTL)  = varint, tag = 0x10
     *   field 3 (error) = string, tag = 0x1a (omitted on success)
     */
    uint8_t buf[32];
    size_t pos = 0;
    buf[pos++] = 0x08; /* field 1 = ID */
    write_varint(buf, sizeof(buf), &pos, (uint64_t)id);
    buf[pos++] = 0x10; /* field 2 = TTL */
    write_varint(buf, sizeof(buf), &pos, (uint64_t)(ttl > 0 ? ttl : 0));
    uint8_t *out = (uint8_t *)malloc(pos);
    if (!out) return (cetcd_rpc_bytes){NULL, 0};
    memcpy(out, buf, pos);
    return (cetcd_rpc_bytes){out, pos};
}

cetcd_rpc_bytes lease_handle_grant(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len) {
    (void)rpc;
    size_t pos = 0; int64_t ttl = 0; int64_t id = 0;
    while (pos < req_len) {
        uint8_t tag = req[pos++];
        if (tag == 0x08) { /* ttl varint */
            if (read_varint_local(req, req_len, &pos, &ttl) != 0) break;
        } else if (tag == 0x10) { /* id varint */
            if (read_varint_local(req, req_len, &pos, &id) != 0) break;
        } else {
            if (pos < req_len) pos++;
        }
    }
    if (ttl <= 0) ttl = 60; /* default TTL */
    cetcd_lease_id lid = cetcd_lease_grant(g_rpc_lease_mgr, ttl);
    return make_lease_response(lid, ttl);
}

cetcd_rpc_bytes lease_handle_revoke(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len) {
    (void)rpc;
    size_t pos = 0; int64_t id = 0;
    while (pos < req_len) {
        uint8_t tag = req[pos++];
        if (tag == 0x08) { /* field 1 = ID varint */
            if (read_varint_local(req, req_len, &pos, &id) != 0) break;
        } else {
            if (pos < req_len) pos++;
        }
    }
    if (id > 0) {
        cetcd_lease_revoke(g_rpc_lease_mgr, (cetcd_lease_id)id);
    }
    /* LeaseRevokeResponse: header only, minimal empty response */
    uint8_t *out = (uint8_t *)malloc(1);
    if (!out) return (cetcd_rpc_bytes){NULL, 0};
    out[0] = 0;
    return (cetcd_rpc_bytes){out, 1};
}

cetcd_rpc_bytes lease_handle_keep_alive(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len) {
    (void)rpc;
    size_t pos = 0; int64_t id = 0;
    while (pos < req_len) {
        uint8_t tag = req[pos++];
        if (tag == 0x08) { /* field 1 = ID varint */
            if (read_varint_local(req, req_len, &pos, &id) != 0) break;
        } else {
            if (pos < req_len) pos++;
        }
    }
    int64_t remaining_ttl = 0;
    if (id > 0 && cetcd_lease_exists(g_rpc_lease_mgr, (cetcd_lease_id)id)) {
        /* Use the original granted TTL for keep-alive */
        int64_t granted = cetcd_lease_granted_ttl(g_rpc_lease_mgr, (cetcd_lease_id)id);
        if (granted <= 0) granted = 60; /* fallback */
        cetcd_lease_keep_alive(g_rpc_lease_mgr, (cetcd_lease_id)id, granted);
        remaining_ttl = cetcd_lease_ttl_remaining(g_rpc_lease_mgr, (cetcd_lease_id)id);
    }
    /* LeaseKeepAliveResponse:
     *   field 1 (ID)  = varint, tag = 0x08
     *   field 2 (TTL) = varint, tag = 0x10
     */
    uint8_t buf[32];
    size_t bpos = 0;
    buf[bpos++] = 0x08;
    write_varint(buf, sizeof(buf), &bpos, (uint64_t)id);
    buf[bpos++] = 0x10;
    write_varint(buf, sizeof(buf), &bpos, (uint64_t)(remaining_ttl > 0 ? remaining_ttl : 0));
    uint8_t *out = (uint8_t *)malloc(bpos);
    if (!out) return (cetcd_rpc_bytes){NULL, 0};
    memcpy(out, buf, bpos);
    return (cetcd_rpc_bytes){out, bpos};
}

cetcd_rpc_bytes lease_handle_time_to_live(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len) {
    (void)rpc;
    size_t pos = 0; int64_t id = 0;
    while (pos < req_len) {
        uint8_t tag = req[pos++];
        if (tag == 0x08) { /* field 1 = ID varint */
            if (read_varint_local(req, req_len, &pos, &id) != 0) break;
        } else {
            if (pos < req_len) pos++;
        }
    }
    int64_t ttl = 0;
    if (id > 0 && cetcd_lease_exists(g_rpc_lease_mgr, (cetcd_lease_id)id)) {
        ttl = cetcd_lease_ttl_remaining(g_rpc_lease_mgr, (cetcd_lease_id)id);
    }
    /* LeaseTimeToLiveResponse:
     *   field 1 (ID)  = varint, tag = 0x08
     *   field 2 (TTL) = varint (int64), tag = 0x10
     *   field 3 (grantedTTL) = varint, tag = 0x18 (same as TTL for simplicity)
     */
    uint8_t buf[32];
    size_t bpos = 0;
    buf[bpos++] = 0x08;
    write_varint(buf, sizeof(buf), &bpos, (uint64_t)id);
    if (ttl > 0) {
        buf[bpos++] = 0x10;
        write_varint(buf, sizeof(buf), &bpos, (uint64_t)ttl);
        buf[bpos++] = 0x18;
        write_varint(buf, sizeof(buf), &bpos, (uint64_t)ttl);
    }
    uint8_t *out = (uint8_t *)malloc(bpos);
    if (!out) return (cetcd_rpc_bytes){NULL, 0};
    memcpy(out, buf, bpos);
    return (cetcd_rpc_bytes){out, bpos};
}

cetcd_rpc_bytes lease_handle_leases(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len) {
    (void)rpc; (void)req; (void)req_len;
    /* LeaseLeasesResponse:
     *   field 1 (leases) = repeated LeaseStatus, tag = 0x0a (length-delimited)
     *     LeaseStatus: field 1 (ID) = varint, tag = 0x08
     */
    size_t count = cetcd_lease_mgr_count(g_rpc_lease_mgr);
    if (count == 0) {
        uint8_t *out = (uint8_t *)malloc(1);
        if (!out) return (cetcd_rpc_bytes){NULL, 0};
        out[0] = 0;
        return (cetcd_rpc_bytes){out, 1};
    }

    /* Collect lease IDs */
    cetcd_lease_id *ids = (cetcd_lease_id *)malloc(count * sizeof(cetcd_lease_id));
    if (!ids) {
        uint8_t *out = (uint8_t *)malloc(1);
        if (!out) return (cetcd_rpc_bytes){NULL, 0};
        out[0] = 0;
        return (cetcd_rpc_bytes){out, 1};
    }
    size_t n = cetcd_lease_mgr_leases(g_rpc_lease_mgr, ids, count);

    /* Build response */
    uint8_t *buf = (uint8_t *)malloc(n * 16 + 1);
    if (!buf) { free(ids); return (cetcd_rpc_bytes){NULL, 0}; }
    size_t bpos = 0;
    for (size_t i = 0; i < n; i++) {
        /* Build LeaseStatus sub-message: field 1 (ID) = varint */
        uint8_t ls_buf[16];
        size_t ls_pos = 0;
        ls_buf[ls_pos++] = 0x08; /* field 1 = ID */
        uint64_t lid = ids[i];
        do { uint8_t b = lid & 0x7F; lid >>= 7; if (lid) b |= 0x80; ls_buf[ls_pos++] = b; } while (lid);

        /* Write as field 1 (leases) = LeaseStatus (length-delimited), tag = 0x0a */
        buf[bpos++] = 0x0a;
        uint64_t lslen = ls_pos;
        do { uint8_t b = lslen & 0x7F; lslen >>= 7; if (lslen) b |= 0x80; buf[bpos++] = b; } while (lslen);
        memcpy(buf + bpos, ls_buf, ls_pos);
        bpos += ls_pos;
    }
    free(ids);
    return (cetcd_rpc_bytes){buf, bpos};
}
