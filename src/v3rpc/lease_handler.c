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
     *   field 1 (header) = ResponseHeader, tag = 0x0a (length-delimited)
     *   field 2 (ID)     = int64, tag = 0x10
     *   field 3 (TTL)    = int64, tag = 0x18
     *   field 4 (error)  = string, tag = 0x22 (omitted on success)
     */
    int64_t current_rev = g_rpc_store ? cetcd_mvcc_revision(g_rpc_store) : 0;

    /* Build ResponseHeader inner: field 3 (revision) = varint, tag = 0x18 */
    uint8_t hdr_buf[16];
    size_t hpos = 0;
    hdr_buf[hpos++] = 0x18;
    write_varint(hdr_buf, sizeof(hdr_buf), &hpos, (uint64_t)(current_rev > 0 ? current_rev : 1));

    uint8_t buf[64];
    size_t pos = 0;
    /* field 1 = header (length-delimited) */
    buf[pos++] = 0x0a;
    write_varint(buf, sizeof(buf), &pos, (uint64_t)hpos);
    memcpy(buf + pos, hdr_buf, hpos);
    pos += hpos;
    /* field 2 = ID */
    buf[pos++] = 0x10;
    write_varint(buf, sizeof(buf), &pos, (uint64_t)id);
    /* field 3 = TTL */
    buf[pos++] = 0x18;
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
    cetcd_lease_id lid = 0;
    if (id > 0)
        lid = cetcd_lease_grant_id(g_rpc_lease_mgr, (cetcd_lease_id)id, ttl);
    else
        lid = cetcd_lease_grant(g_rpc_lease_mgr, ttl);
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
    if (id > 0 && g_rpc_lease_mgr) {
        /* Match etcd: revoke deletes all keys attached to the lease. */
        const uint8_t *const *keys = NULL;
        const size_t *key_lens = NULL;
        size_t n = cetcd_lease_keys(g_rpc_lease_mgr, (cetcd_lease_id)id,
                                    &keys, &key_lens);
        if (g_rpc_store && keys && key_lens) {
            cetcd_mvcc_delete_keys(g_rpc_store, keys, key_lens, n);
        }
        cetcd_lease_revoke(g_rpc_lease_mgr, (cetcd_lease_id)id);
    }
    /* LeaseRevokeResponse: header with revision */
    int64_t current_rev = g_rpc_store ? cetcd_mvcc_revision(g_rpc_store) : 0;
    uint8_t hdr_buf[32];
    size_t hpos = 0;
    hdr_buf[hpos++] = 0x18; /* field 3 = revision */
    write_varint(hdr_buf, sizeof(hdr_buf), &hpos, (uint64_t)(current_rev > 0 ? current_rev : 1));

    uint8_t resp[64];
    size_t rpos = 0;
    resp[rpos++] = 0x0a; /* field 1 = header */
    write_varint(resp, sizeof(resp), &rpos, (uint64_t)hpos);
    memcpy(resp + rpos, hdr_buf, hpos);
    rpos += hpos;

    uint8_t *out = (uint8_t *)malloc(rpos);
    if (!out) return (cetcd_rpc_bytes){NULL, 0};
    memcpy(out, resp, rpos);
    return (cetcd_rpc_bytes){out, rpos};
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
     *   field 1 (header) = ResponseHeader, tag = 0x0a (length-delimited)
     *   field 2 (ID)     = int64, tag = 0x10
     *   field 3 (TTL)    = int64, tag = 0x18
     */
    int64_t current_rev = g_rpc_store ? cetcd_mvcc_revision(g_rpc_store) : 0;
    uint8_t hdr_buf[16];
    size_t hpos = 0;
    hdr_buf[hpos++] = 0x18;
    write_varint(hdr_buf, sizeof(hdr_buf), &hpos, (uint64_t)(current_rev > 0 ? current_rev : 1));

    uint8_t buf[64];
    size_t bpos = 0;
    /* field 1 = header */
    buf[bpos++] = 0x0a;
    write_varint(buf, sizeof(buf), &bpos, (uint64_t)hpos);
    memcpy(buf + bpos, hdr_buf, hpos);
    bpos += hpos;
    /* field 2 = ID */
    buf[bpos++] = 0x10;
    write_varint(buf, sizeof(buf), &bpos, (uint64_t)id);
    /* field 3 = TTL */
    buf[bpos++] = 0x18;
    write_varint(buf, sizeof(buf), &bpos, (uint64_t)(remaining_ttl > 0 ? remaining_ttl : 0));
    uint8_t *out = (uint8_t *)malloc(bpos);
    if (!out) return (cetcd_rpc_bytes){NULL, 0};
    memcpy(out, buf, bpos);
    return (cetcd_rpc_bytes){out, bpos};
}

cetcd_rpc_bytes lease_handle_time_to_live(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len) {
    (void)rpc;
    size_t pos = 0; int64_t id = 0; int want_keys = 0;
    while (pos < req_len) {
        uint8_t tag = req[pos++];
        if (tag == 0x08) { /* field 1 = ID varint */
            if (read_varint_local(req, req_len, &pos, &id) != 0) break;
        } else if (tag == 0x10) { /* field 2 = keys (bool) */
            int64_t tmp = 0; if (read_varint_local(req, req_len, &pos, &tmp) != 0) break; want_keys = (int)tmp;
        } else {
            if (pos < req_len) pos++;
        }
    }
    int64_t ttl = 0;
    int64_t granted_ttl = 0;
    int exists = (id > 0 && g_rpc_lease_mgr &&
                  cetcd_lease_exists(g_rpc_lease_mgr, (cetcd_lease_id)id));
    if (exists) {
        ttl = cetcd_lease_ttl_remaining(g_rpc_lease_mgr, (cetcd_lease_id)id);
        granted_ttl = cetcd_lease_granted_ttl(g_rpc_lease_mgr, (cetcd_lease_id)id);
    }
    /* LeaseTimeToLiveResponse:
     *   field 1 (header)     = ResponseHeader, tag = 0x0a (length-delimited)
     *   field 2 (ID)         = int64, tag = 0x10
     *   field 3 (TTL)        = int64, tag = 0x18
     *   field 4 (grantedTTL) = int64, tag = 0x20
     *   field 5 (keys)       = repeated bytes, tag = 0x2a (length-delimited)
     */
    int64_t current_rev = g_rpc_store ? cetcd_mvcc_revision(g_rpc_store) : 0;
    uint8_t hdr_buf[16];
    size_t hpos = 0;
    hdr_buf[hpos++] = 0x18;
    write_varint(hdr_buf, sizeof(hdr_buf), &hpos, (uint64_t)(current_rev > 0 ? current_rev : 1));

    /* Collect keys if requested */
    const uint8_t *const *lease_keys = NULL;
    const size_t *lease_key_lens = NULL;
    size_t key_count = 0;
    if (want_keys && exists) {
        key_count = cetcd_lease_keys(g_rpc_lease_mgr, (cetcd_lease_id)id,
                                      &lease_keys, &lease_key_lens);
    }

    /* Estimate needed buffer: header + ID + TTL + grantedTTL + keys */
    size_t keys_size = 0;
    for (size_t i = 0; i < key_count; i++) {
        keys_size += 1 + 5 + lease_key_lens[i]; /* tag + length varint + data */
    }
    uint8_t *buf = (uint8_t *)malloc(128 + keys_size);
    if (!buf) return (cetcd_rpc_bytes){NULL, 0};
    size_t bpos = 0;
    /* field 1 = header */
    buf[bpos++] = 0x0a;
    write_varint(buf, 128 + keys_size, &bpos, (uint64_t)hpos);
    memcpy(buf + bpos, hdr_buf, hpos);
    bpos += hpos;
    /* field 2 = ID */
    buf[bpos++] = 0x10;
    write_varint(buf, 128 + keys_size, &bpos, (uint64_t)id);
    /* field 3 = TTL: missing lease → -1 (etcd); else remaining seconds */
    buf[bpos++] = 0x18;
    if (!exists)
        write_varint(buf, 128 + keys_size, &bpos, (uint64_t)(int64_t)-1);
    else
        write_varint(buf, 128 + keys_size, &bpos, (uint64_t)(ttl > 0 ? ttl : 0));
    if (exists && granted_ttl > 0) {
        /* field 4 = grantedTTL */
        buf[bpos++] = 0x20;
        write_varint(buf, 128 + keys_size, &bpos, (uint64_t)granted_ttl);
    }
    /* field 5 = keys (repeated bytes) */
    for (size_t i = 0; i < key_count; i++) {
        buf[bpos++] = 0x2a;
        write_varint(buf, 128 + keys_size, &bpos, (uint64_t)lease_key_lens[i]);
        memcpy(buf + bpos, lease_keys[i], lease_key_lens[i]);
        bpos += lease_key_lens[i];
    }
    return (cetcd_rpc_bytes){buf, bpos};
}

cetcd_rpc_bytes lease_handle_leases(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len) {
    (void)rpc; (void)req; (void)req_len;
    /* LeaseLeasesResponse:
     *   field 1 (header)  = ResponseHeader, tag = 0x0a (length-delimited)
     *   field 2 (leases)  = repeated LeaseStatus, tag = 0x12 (length-delimited)
     *     LeaseStatus: field 1 (ID) = varint, tag = 0x08
     */
    int64_t current_rev = g_rpc_store ? cetcd_mvcc_revision(g_rpc_store) : 0;
    uint8_t hdr_buf[16];
    size_t hpos = 0;
    hdr_buf[hpos++] = 0x18;
    write_varint(hdr_buf, sizeof(hdr_buf), &hpos, (uint64_t)(current_rev > 0 ? current_rev : 1));

    size_t count = cetcd_lease_mgr_count(g_rpc_lease_mgr);

    /* Build LeaseStatus entries first to know total size */
    uint8_t *ls_data = NULL;
    size_t ls_total = 0;
    if (count > 0) {
        cetcd_lease_id *ids = (cetcd_lease_id *)malloc(count * sizeof(cetcd_lease_id));
        if (!ids) {
            uint8_t *out = (uint8_t *)malloc(1);
            if (!out) return (cetcd_rpc_bytes){NULL, 0};
            out[0] = 0;
            return (cetcd_rpc_bytes){out, 1};
        }
        size_t n = cetcd_lease_mgr_leases(g_rpc_lease_mgr, ids, count);
        ls_data = (uint8_t *)malloc(n * 16 + 1);
        if (!ls_data) { free(ids); return (cetcd_rpc_bytes){NULL, 0}; }
        ls_total = 0;
        for (size_t i = 0; i < n; i++) {
            uint8_t ls_buf[16];
            size_t ls_pos = 0;
            ls_buf[ls_pos++] = 0x08; /* field 1 = ID */
            uint64_t lid = ids[i];
            do { uint8_t b = lid & 0x7F; lid >>= 7; if (lid) b |= 0x80; ls_buf[ls_pos++] = b; } while (lid);

            /* Write as field 2 (leases) = LeaseStatus (length-delimited), tag = 0x12 */
            ls_data[ls_total++] = 0x12;
            uint64_t lslen = ls_pos;
            do { uint8_t b = lslen & 0x7F; lslen >>= 7; if (lslen) b |= 0x80; ls_data[ls_total++] = b; } while (lslen);
            memcpy(ls_data + ls_total, ls_buf, ls_pos);
            ls_total += ls_pos;
        }
        free(ids);
    }

    /* Build final response: header + lease status entries */
    size_t total = 2 + hpos + ls_total; /* header tag + length + header data + leases */
    uint8_t *buf = (uint8_t *)malloc(total);
    if (!buf) { free(ls_data); return (cetcd_rpc_bytes){NULL, 0}; }
    size_t bpos = 0;
    /* field 1 = header */
    buf[bpos++] = 0x0a;
    write_varint(buf, total, &bpos, (uint64_t)hpos);
    memcpy(buf + bpos, hdr_buf, hpos);
    bpos += hpos;
    /* field 2 = leases */
    if (ls_total > 0) {
        memcpy(buf + bpos, ls_data, ls_total);
        bpos += ls_total;
    }
    free(ls_data);
    return (cetcd_rpc_bytes){buf, bpos};
}
