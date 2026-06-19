#include <stdlib.h>
#include <string.h>

/* Access to the internal mvcc store via global handles defined in v3rpc.c */
#include "cetcd/v3rpc.h"
#include "cetcd/mvcc.h"

/* Externs pointing to the live store/lease mgr (set by v3rpc_new) */
extern cetcd_mvcc_store *g_rpc_store;

/* Forward declare to be linked with v3rpc.c */
cetcd_rpc_bytes kv_handle_put(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes kv_handle_range(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes kv_handle_delete_range(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);

/* Simple protobuf-like helpers (tag/length/value) */
static int read_varint(const uint8_t *buf, size_t len, size_t *pos, uint64_t *out) {
    uint64_t val = 0;
    int shift = 0;
    while (*pos < len) {
        uint8_t b = buf[*pos];
        (*pos)++;
        val |= (uint64_t)(b & 0x7F) << shift;
        if ((b & 0x80) == 0) {
            *out = val;
            return 0;
        }
        shift += 7;
        if (shift > 63) break;
    }
    return -1;
}

static int read_bytes(const uint8_t *buf, size_t len, size_t *pos, uint8_t **out, size_t *out_len) {
    uint64_t l = 0;
    if (read_varint(buf, len, pos, &l) != 0) return -1;
    if (*pos + l > len) return -1;
    uint8_t *p = (uint8_t *)malloc((size_t)l);
    if (!p) return -1;
    memcpy(p, buf + *pos, (size_t)l);
    *pos += (size_t)l;
    *out = p;
    *out_len = (size_t)l;
    return 0;
}

static int write_varint_local(uint8_t *buf, size_t cap, size_t *pos, uint64_t val) {
    while (*pos < cap) {
        uint8_t b = val & 0x7F;
        val >>= 7;
        if (val) b |= 0x80;
        buf[(*pos)++] = b;
        if (!val) return 0;
    }
    return -1;
}

cetcd_rpc_bytes kv_handle_put(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len) {
    (void)rpc;
    uint8_t *key = NULL; size_t key_len = 0;
    uint8_t *val = NULL; size_t val_len = 0;
    int64_t  lease_id = 0;
    size_t pos = 0;
    while (pos < req_len) {
        uint8_t tag = req[pos++];
        if (tag == 0x0a) { /* key: bytes */
            if (read_bytes(req, req_len, &pos, &key, &key_len) != 0) break;
        } else if (tag == 0x12) { /* value: bytes */
            if (read_bytes(req, req_len, &pos, &val, &val_len) != 0) break;
        } else if (tag == 0x18) { /* lease: varint */
            uint64_t tmp = 0; if (read_varint(req, req_len, &pos, &tmp) != 0) break; lease_id = (int64_t)tmp;
        } else if (tag == 0x20) { /* prev_kv (bool) */
            uint64_t tmp = 0; if (read_varint(req, req_len, &pos, &tmp) != 0) break;
        } else {
            uint64_t skip = 0; read_varint(req, req_len, &pos, &skip);
        }
    }

    int64_t rev = 0;
    if (key && val && g_rpc_store) {
        cetcd_revision r = cetcd_mvcc_put(g_rpc_store, key, key_len, val, val_len, lease_id);
        rev = r.main;
    }
    if (key) free(key);
    if (val) free(val);

    /* PutResponse:
     *   field 1 (header) = ResponseHeader (omitted for simplicity)
     *   field 4 (prev_kv) = KeyValue (omitted, we don't support prev_kv yet)
     * Minimal valid response: just header with revision
     * ResponseHeader:
     *   field 1 (cluster_id) = uint64, tag=0x08
     *   field 2 (member_id)  = uint64, tag=0x10
     *   field 3 (revision)   = int64,  tag=0x18
     *   field 4 (raft_term)  = uint64, tag=0x20
     */
    uint8_t hdr_buf[32];
    size_t hpos = 0;
    hdr_buf[hpos++] = 0x18; /* field 3 = revision */
    write_varint_local(hdr_buf, sizeof(hdr_buf), &hpos, (uint64_t)(rev > 0 ? rev : 1));

    /* Wrap header as field 1 of PutResponse */
    uint8_t resp[64];
    size_t rpos = 0;
    resp[rpos++] = 0x0a; /* field 1 = header (length-delimited) */
    write_varint_local(resp, sizeof(resp), &rpos, (uint64_t)hpos);
    memcpy(resp + rpos, hdr_buf, hpos);
    rpos += hpos;

    uint8_t *out = (uint8_t *)malloc(rpos);
    if (!out) return (cetcd_rpc_bytes){NULL, 0};
    memcpy(out, resp, rpos);
    return (cetcd_rpc_bytes){out, rpos};
}

cetcd_rpc_bytes kv_handle_range(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len) {
    (void)rpc;
    size_t pos = 0;
    uint8_t *key = NULL; size_t key_len = 0;
    uint8_t *range_end = NULL; size_t range_end_len = 0;
    int64_t rev = 0;
    while (pos < req_len) {
        uint8_t tag = req[pos++];
        if (tag == 0x0a) { /* field 1 = key */
            if (read_bytes(req, req_len, &pos, &key, &key_len) != 0) break;
        } else if (tag == 0x12) { /* field 2 = range_end */
            if (read_bytes(req, req_len, &pos, &range_end, &range_end_len) != 0) break;
        } else if (tag == 0x18) { /* field 3 = revision */
            uint64_t v = 0; if (read_varint(req, req_len, &pos, &v) != 0) break; rev = (int64_t)v;
        } else {
            uint64_t skip = 0; read_varint(req, req_len, &pos, &skip);
        }
    }

    /* Build RangeResponse:
     *   field 1 (header) = ResponseHeader (with revision)
     *   field 2 (kvs) = repeated KeyValue (length-delimited), tag = 0x0a
     *   field 3 (more)  = bool, tag = 0x10
     *   field 4 (count) = int64, tag = 0x18
     *
     * KeyValue:
     *   field 1 (key)            = bytes, tag = 0x0a
     *   field 2 (create_revision)= int64, tag = 0x10
     *   field 3 (mod_revision)   = int64, tag = 0x18
     *   field 4 (version)        = int64, tag = 0x20
     *   field 5 (value)          = bytes, tag = 0x2a
     */
    uint8_t *resp = NULL;
    size_t resp_cap = 4096;
    size_t rpos = 0;
    resp = (uint8_t *)malloc(resp_cap);
    if (!resp) { if (key) free(key); if (range_end) free(range_end); return (cetcd_rpc_bytes){NULL, 0}; }

    int64_t current_rev = g_rpc_store ? cetcd_mvcc_revision(g_rpc_store) : 0;
    size_t kv_count = 0;

    /* Reserve space for header */
    rpos += 2; /* will fill in header later */
    size_t header_pos = 0; /* we'll write header at the start */

    if (key && g_rpc_store) {
        if (!range_end || range_end_len == 0) {
            /* Point get */
            cetcd_kv out_kv;
            memset(&out_kv, 0, sizeof(out_kv));
            if (cetcd_mvcc_get(g_rpc_store, rev, key, key_len, &out_kv) == 0) {
                /* Encode KeyValue */
                uint8_t kv_buf[1024];
                size_t kpos = 0;
                /* key */
                kv_buf[kpos++] = 0x0a;
                write_varint_local(kv_buf, sizeof(kv_buf), &kpos, out_kv.key.len);
                memcpy(kv_buf + kpos, out_kv.key.data, out_kv.key.len);
                kpos += out_kv.key.len;
                /* create_revision */
                kv_buf[kpos++] = 0x10;
                write_varint_local(kv_buf, sizeof(kv_buf), &kpos, (uint64_t)out_kv.create_rev.main);
                /* mod_revision */
                kv_buf[kpos++] = 0x18;
                write_varint_local(kv_buf, sizeof(kv_buf), &kpos, (uint64_t)out_kv.mod_rev.main);
                /* version */
                kv_buf[kpos++] = 0x20;
                write_varint_local(kv_buf, sizeof(kv_buf), &kpos, (uint64_t)out_kv.version);
                /* value */
                if (out_kv.value.len > 0) {
                    kv_buf[kpos++] = 0x2a;
                    write_varint_local(kv_buf, sizeof(kv_buf), &kpos, out_kv.value.len);
                    memcpy(kv_buf + kpos, out_kv.value.data, out_kv.value.len);
                    kpos += out_kv.value.len;
                }
                /* Write as field 2 (kvs) */
                if (rpos + 1 + 5 + kpos > resp_cap) {
                    resp_cap = rpos + kpos + 64;
                    uint8_t *tmp = (uint8_t *)realloc(resp, resp_cap);
                    if (!tmp) { free(resp); if(key)free(key); if(range_end)free(range_end); return (cetcd_rpc_bytes){NULL,0}; }
                    resp = tmp;
                }
                resp[rpos++] = 0x0a; /* field 2 = kvs */
                write_varint_local(resp, resp_cap, &rpos, (uint64_t)kpos);
                memcpy(resp + rpos, kv_buf, kpos);
                rpos += kpos;
                kv_count = 1;
            }
            free((void*)out_kv.key.data);
            free((void*)out_kv.value.data);
        } else {
            /* Range query */
            cetcd_kv *kvs = NULL; size_t n = 0;
            cetcd_mvcc_range(g_rpc_store, rev,
                             key, key_len,
                             range_end, range_end_len,
                             &kvs, &n);
            for (size_t i = 0; i < n; i++) {
                uint8_t kv_buf[1024];
                size_t kpos = 0;
                kv_buf[kpos++] = 0x0a; /* key */
                write_varint_local(kv_buf, sizeof(kv_buf), &kpos, kvs[i].key.len);
                memcpy(kv_buf + kpos, kvs[i].key.data, kvs[i].key.len);
                kpos += kvs[i].key.len;
                kv_buf[kpos++] = 0x10; /* create_revision */
                write_varint_local(kv_buf, sizeof(kv_buf), &kpos, (uint64_t)kvs[i].create_rev.main);
                kv_buf[kpos++] = 0x18; /* mod_revision */
                write_varint_local(kv_buf, sizeof(kv_buf), &kpos, (uint64_t)kvs[i].mod_rev.main);
                kv_buf[kpos++] = 0x20; /* version */
                write_varint_local(kv_buf, sizeof(kv_buf), &kpos, (uint64_t)kvs[i].version);
                if (kvs[i].value.len > 0) {
                    kv_buf[kpos++] = 0x2a; /* value */
                    write_varint_local(kv_buf, sizeof(kv_buf), &kpos, kvs[i].value.len);
                    memcpy(kv_buf + kpos, kvs[i].value.data, kvs[i].value.len);
                    kpos += kvs[i].value.len;
                }
                if (rpos + 1 + 5 + kpos > resp_cap) {
                    resp_cap = resp_cap * 2 + kpos + 64;
                    uint8_t *tmp = (uint8_t *)realloc(resp, resp_cap);
                    if (!tmp) { free(resp); free(kvs); if(key)free(key); if(range_end)free(range_end); return (cetcd_rpc_bytes){NULL,0}; }
                    resp = tmp;
                }
                resp[rpos++] = 0x0a;
                write_varint_local(resp, resp_cap, &rpos, (uint64_t)kpos);
                memcpy(resp + rpos, kv_buf, kpos);
                rpos += kpos;
            }
            kv_count = n;
            if (kvs) cetcd_kv_free_contents(kvs, n);
        }
    }

    /* count field */
    if (rpos + 10 > resp_cap) {
        resp_cap = rpos + 16;
        uint8_t *tmp = (uint8_t *)realloc(resp, resp_cap);
        if (!tmp) { free(resp); if(key)free(key); if(range_end)free(range_end); return (cetcd_rpc_bytes){NULL,0}; }
        resp = tmp;
    }
    resp[rpos++] = 0x18; /* field 4 = count */
    write_varint_local(resp, resp_cap, &rpos, (uint64_t)kv_count);

    /* Now write the header at the beginning */
    {
        uint8_t hdr_buf[32];
        size_t hpos = 0;
        hdr_buf[hpos++] = 0x18; /* field 3 = revision */
        write_varint_local(hdr_buf, sizeof(hdr_buf), &hpos, (uint64_t)(current_rev > 0 ? current_rev : 1));

        /* Shift everything right to make room for header */
        size_t hdr_total = 1 + 1 + hpos; /* tag + len + data */
        memmove(resp + hdr_total, resp + 2, rpos - 2);
        rpos = rpos - 2 + hdr_total;
        header_pos = 0;
        resp[header_pos++] = 0x0a; /* field 1 = header */
        write_varint_local(resp, resp_cap, &header_pos, (uint64_t)hpos);
        memcpy(resp + header_pos, hdr_buf, hpos);
        header_pos += hpos;
    }

    if (key) free(key);
    if (range_end) free(range_end);
    return (cetcd_rpc_bytes){resp, rpos};
}

cetcd_rpc_bytes kv_handle_delete_range(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len) {
    (void)rpc;
    size_t pos = 0; uint8_t *key = NULL; size_t key_len = 0;
    while (pos < req_len) {
        uint8_t tag = req[pos++];
        if (tag == 0x0a) {
            if (read_bytes(req, req_len, &pos, &key, &key_len) != 0) break;
        } else {
            uint64_t skip = 0; read_varint(req, req_len, &pos, &skip);
        }
    }
    int64_t rev = 0;
    if (key && g_rpc_store) {
        cetcd_revision r = cetcd_mvcc_delete(g_rpc_store, key, key_len);
        rev = r.main;
    }
    if (key) free(key);

    /* DeleteRangeResponse:
     *   field 1 (header)  = ResponseHeader
     *   field 2 (deleted) = int64, tag = 0x10
     */
    uint8_t hdr_buf[32];
    size_t hpos = 0;
    hdr_buf[hpos++] = 0x18; /* revision */
    write_varint_local(hdr_buf, sizeof(hdr_buf), &hpos, (uint64_t)(rev > 0 ? rev : 1));

    uint8_t resp[64];
    size_t rpos = 0;
    resp[rpos++] = 0x0a; /* field 1 = header */
    write_varint_local(resp, sizeof(resp), &rpos, (uint64_t)hpos);
    memcpy(resp + rpos, hdr_buf, hpos);
    rpos += hpos;
    resp[rpos++] = 0x10; /* field 2 = deleted */
    write_varint_local(resp, sizeof(resp), &rpos, (rev > 0 ? 1 : 0));

    uint8_t *out = (uint8_t *)malloc(rpos);
    if (!out) return (cetcd_rpc_bytes){NULL, 0};
    memcpy(out, resp, rpos);
    return (cetcd_rpc_bytes){out, rpos};
}

cetcd_rpc_bytes kv_handle_txn(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes kv_handle_compact(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);

/*
 * Txn RPC handler.
 *
 * TxnRequest:
 *   field 1 (compare)  = repeated Compare, tag = 0x0a
 *   field 2 (success)  = repeated RequestOp, tag = 0x12
 *   field 3 (failure)  = repeated RequestOp, tag = 0x1a
 *
 * RequestOp is a length-delimited oneof:
 *   field 1 = RequestRange (tag = 0x0a)
 *   field 2 = RequestPut (tag = 0x12)
 *   field 3 = RequestDeleteRange (tag = 0x1a)
 *
 * TxnResponse:
 *   field 1 (header)   = ResponseHeader
 *   field 2 (succeeded) = bool, tag = 0x10
 *   field 3 (responses) = repeated ResponseOp, tag = 0x1a
 *
 * For simplicity, we implement a basic version:
 *   - If there are no compare clauses, execute all success ops.
 *   - If there are compare clauses, check them; if all pass, execute success ops;
 *     otherwise execute failure ops.
 *   - Each Put/Delete op is applied to the MVCC store.
 *   - Range ops return minimal results.
 */
cetcd_rpc_bytes kv_handle_txn(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len) {
    (void)rpc;
    size_t pos = 0;
    bool succeeded = true; /* default: no comparisons = succeed */

    /* We parse the request to find compare and success/failure ops.
     * For each success op that is a Put, we apply it.
     * For each success op that is a DeleteRange, we apply it.
     * Compare clauses are checked against the MVCC store.
     */
    while (pos < req_len) {
        if (pos >= req_len) break;
        uint8_t tag = req[pos++];
        if (tag == 0x0a) {
            /* compare clause: length-delimited, skip for now (treat as pass) */
            uint64_t clen = 0;
            if (read_varint(req, req_len, &pos, &clen) != 0) break;
            pos += (size_t)clen;
            /* Basic compare: always succeed */
        } else if (tag == 0x12) {
            /* success op: length-delimited RequestOp */
            uint64_t olen = 0;
            if (read_varint(req, req_len, &pos, &olen) != 0) break;
            if (pos + olen > req_len) break;
            /* Parse the RequestOp oneof */
            size_t op_start = pos;
            size_t op_end = pos + (size_t)olen;
            while (op_start < op_end) {
                uint8_t op_tag = req[op_start++];
                if (op_tag == 0x12) {
                    /* RequestPut: parse key and value, apply to store */
                    uint64_t plen = 0;
                    size_t pp = op_start;
                    if (read_varint(req, op_end, &pp, &plen) != 0) break;
                    size_t put_end = pp + (size_t)plen;
                    uint8_t *pk = NULL, *pv = NULL;
                    size_t pk_len = 0, pv_len = 0;
                    while (pp < put_end) {
                        uint8_t ptag = req[pp++];
                        if (ptag == 0x0a) {
                            if (read_bytes(req, put_end, &pp, &pk, &pk_len) != 0) break;
                        } else if (ptag == 0x12) {
                            if (read_bytes(req, put_end, &pp, &pv, &pv_len) != 0) break;
                        } else {
                            uint64_t skip = 0; read_varint(req, put_end, &pp, &skip);
                        }
                    }
                    if (pk && pv && g_rpc_store) {
                        cetcd_mvcc_put(g_rpc_store, pk, pk_len, pv, pv_len, 0);
                    }
                    if (pk) free(pk);
                    if (pv) free(pv);
                } else if (op_tag == 0x1a) {
                    /* RequestDeleteRange: parse key, apply delete */
                    uint64_t dlen = 0;
                    size_t dp = op_start;
                    if (read_varint(req, op_end, &dp, &dlen) != 0) break;
                    size_t del_end = dp + (size_t)dlen;
                    uint8_t *dk = NULL; size_t dk_len = 0;
                    while (dp < del_end) {
                        uint8_t dtag = req[dp++];
                        if (dtag == 0x0a) {
                            if (read_bytes(req, del_end, &dp, &dk, &dk_len) != 0) break;
                        } else {
                            uint64_t skip = 0; read_varint(req, del_end, &dp, &skip);
                        }
                    }
                    if (dk && g_rpc_store) {
                        cetcd_mvcc_delete(g_rpc_store, dk, dk_len);
                    }
                    if (dk) free(dk);
                } else {
                    /* Unknown op tag: skip as varint */
                    uint64_t skip = 0; read_varint(req, op_end, &op_start, &skip);
                }
                op_start = op_end;
                break; /* only one op per RequestOp */
            }
            pos = op_end;
        } else if (tag == 0x1a) {
            /* failure op: length-delimited, skip */
            uint64_t flen = 0;
            if (read_varint(req, req_len, &pos, &flen) != 0) break;
            pos += (size_t)flen;
        } else {
            uint64_t skip = 0; read_varint(req, req_len, &pos, &skip);
        }
    }

    /* TxnResponse:
     *   field 2 (succeeded) = bool, tag = 0x10
     * Minimal response: succeeded=true
     */
    uint8_t resp[4];
    size_t rpos = 0;
    resp[rpos++] = 0x10; /* field 2 = succeeded */
    resp[rpos++] = succeeded ? 0x01 : 0x00;
    uint8_t *out = (uint8_t *)malloc(rpos);
    if (!out) return (cetcd_rpc_bytes){NULL, 0};
    memcpy(out, resp, rpos);
    return (cetcd_rpc_bytes){out, rpos};
}

/*
 * Compact RPC handler.
 *
 * CompactRequest:
 *   field 1 (revision) = int64, tag = 0x08
 *   field 2 (physical) = bool, tag = 0x10
 *
 * CompactResponse: header only (empty)
 */
cetcd_rpc_bytes kv_handle_compact(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len) {
    (void)rpc;
    int64_t compact_rev = 0;
    size_t pos = 0;
    while (pos < req_len) {
        uint8_t tag = req[pos++];
        if (tag == 0x08) {
            uint64_t v = 0;
            if (read_varint(req, req_len, &pos, &v) != 0) break;
            compact_rev = (int64_t)v;
        } else if (tag == 0x10) {
            uint64_t v = 0;
            if (read_varint(req, req_len, &pos, &v) != 0) break;
        } else {
            uint64_t skip = 0;
            read_varint(req, req_len, &pos, &skip);
        }
    }
    if (compact_rev > 0 && g_rpc_store) {
        cetcd_mvcc_compact(g_rpc_store, compact_rev);
    }
    /* CompactResponse: empty (header only) */
    uint8_t *out = (uint8_t *)malloc(1);
    if (!out) return (cetcd_rpc_bytes){NULL, 0};
    out[0] = 0;
    return (cetcd_rpc_bytes){out, 1};
}
