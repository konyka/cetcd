#include <stdlib.h>
#include <string.h>

/* Access to the internal mvcc store via global handles defined in v3rpc.c */
#include "cetcd/v3rpc.h"
#include "cetcd/mvcc.h"
#include "cetcd/lease.h"

/* Externs pointing to the live store/lease mgr (set by v3rpc_new) */
extern cetcd_mvcc_store *g_rpc_store;
extern cetcd_lease_mgr  *g_rpc_lease_mgr;

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
    int      prev_kv_flag = 0;
    int      ignore_value = 0;
    int      ignore_lease = 0;
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
            uint64_t tmp = 0; if (read_varint(req, req_len, &pos, &tmp) != 0) break; prev_kv_flag = (int)tmp;
        } else if (tag == 0x28) { /* ignore_value (bool) */
            uint64_t tmp = 0; if (read_varint(req, req_len, &pos, &tmp) != 0) break; ignore_value = (int)tmp;
        } else if (tag == 0x30) { /* ignore_lease (bool) */
            uint64_t tmp = 0; if (read_varint(req, req_len, &pos, &tmp) != 0) break; ignore_lease = (int)tmp;
        } else {
            uint64_t skip = 0; read_varint(req, req_len, &pos, &skip);
        }
    }

    /* If prev_kv requested, or ignore_value/ignore_lease set, get the old value */
    cetcd_kv old_kv;
    memset(&old_kv, 0, sizeof(old_kv));
    int has_old = 0;
    if ((prev_kv_flag || ignore_value || ignore_lease) && key && g_rpc_store) {
        if (cetcd_mvcc_get(g_rpc_store, 0, key, key_len, &old_kv) == 0) {
            has_old = 1;
        }
    }

    /* If ignore_value and key exists, use the existing value */
    if (ignore_value && has_old) {
        if (val) free(val);
        val = (uint8_t *)malloc(old_kv.value.len + 1);
        if (val) {
            memcpy(val, old_kv.value.data, old_kv.value.len);
            val[old_kv.value.len] = '\0';
            val_len = old_kv.value.len;
        }
    }

    /* If ignore_lease and key exists, use the existing lease */
    if (ignore_lease && has_old) {
        lease_id = old_kv.lease_id;
    }

    int64_t rev = 0;
    if (key && g_rpc_store) {
        /* Allow put even without value if ignore_value is set */
        if (val || ignore_value) {
            /* If overwriting a key with a different lease, detach from old */
            if (g_rpc_lease_mgr && lease_id > 0 && has_old && old_kv.lease_id > 0 && old_kv.lease_id != lease_id) {
                cetcd_lease_detach_key(g_rpc_lease_mgr, (cetcd_lease_id)old_kv.lease_id,
                                        key, key_len);
            }
            cetcd_revision r = cetcd_mvcc_put(g_rpc_store, key, key_len,
                                               val ? val : (const uint8_t*)"", val ? val_len : 0,
                                               lease_id);
            rev = r.main;
            /* Attach key to lease */
            if (g_rpc_lease_mgr && lease_id > 0) {
                cetcd_lease_attach_key(g_rpc_lease_mgr, (cetcd_lease_id)lease_id,
                                        key, key_len);
            }
        }
    }
    if (key) free(key);
    if (val) free(val);

    /* PutResponse:
     *   field 1 (header)  = ResponseHeader, tag = 0x0a
     *   field 4 (prev_kv) = KeyValue, tag = 0x22 (when prev_kv=true in request)
     */
    uint8_t hdr_buf[32];
    size_t hpos = 0;
    hdr_buf[hpos++] = 0x18; /* field 3 = revision */
    write_varint_local(hdr_buf, sizeof(hdr_buf), &hpos, (uint64_t)(rev > 0 ? rev : 1));

    /* Build prev_kv KeyValue if requested and old value existed */
    uint8_t *pkv_buf = NULL;
    size_t pkv_len = 0;
    if (prev_kv_flag && has_old) {
        size_t cap = 256 + old_kv.key.len + old_kv.value.len;
        pkv_buf = (uint8_t *)malloc(cap);
        if (pkv_buf) {
            size_t kp = 0;
            pkv_buf[kp++] = 0x0a; /* key */
            write_varint_local(pkv_buf, cap, &kp, old_kv.key.len);
            memcpy(pkv_buf + kp, old_kv.key.data, old_kv.key.len);
            kp += old_kv.key.len;
            pkv_buf[kp++] = 0x10; /* create_revision */
            write_varint_local(pkv_buf, cap, &kp, (uint64_t)old_kv.create_rev.main);
            pkv_buf[kp++] = 0x18; /* mod_revision */
            write_varint_local(pkv_buf, cap, &kp, (uint64_t)old_kv.mod_rev.main);
            pkv_buf[kp++] = 0x20; /* version */
            write_varint_local(pkv_buf, cap, &kp, (uint64_t)old_kv.version);
            if (old_kv.value.len > 0) {
                pkv_buf[kp++] = 0x2a; /* value */
                write_varint_local(pkv_buf, cap, &kp, old_kv.value.len);
                memcpy(pkv_buf + kp, old_kv.value.data, old_kv.value.len);
                kp += old_kv.value.len;
            }
            pkv_len = kp;
        }
        free((void*)old_kv.key.data);
        free((void*)old_kv.value.data);
    } else if (has_old) {
        /* old_kv was fetched for ignore_value/ignore_lease but not prev_kv */
        free((void*)old_kv.key.data);
        free((void*)old_kv.value.data);
    }

    /* Assemble response dynamically */
    size_t resp_cap = 64 + hpos + (pkv_len > 0 ? 10 + pkv_len : 0);
    uint8_t *resp = (uint8_t *)malloc(resp_cap);
    if (!resp) { if (pkv_buf) free(pkv_buf); return (cetcd_rpc_bytes){NULL, 0}; }
    size_t rpos = 0;
    resp[rpos++] = 0x0a; /* field 1 = header */
    write_varint_local(resp, resp_cap, &rpos, (uint64_t)hpos);
    memcpy(resp + rpos, hdr_buf, hpos);
    rpos += hpos;

    if (pkv_len > 0) {
        resp[rpos++] = 0x22; /* field 4 = prev_kv */
        write_varint_local(resp, resp_cap, &rpos, (uint64_t)pkv_len);
        memcpy(resp + rpos, pkv_buf, pkv_len);
        rpos += pkv_len;
        free(pkv_buf);
    }

    return (cetcd_rpc_bytes){resp, rpos};
}

cetcd_rpc_bytes kv_handle_range(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len) {
    (void)rpc;
    size_t pos = 0;
    uint8_t *key = NULL; size_t key_len = 0;
    uint8_t *range_end = NULL; size_t range_end_len = 0;
    int64_t rev = 0;
    int64_t limit = 0;
    int keys_only = 0;
    int count_only = 0;
    while (pos < req_len) {
        uint8_t tag = req[pos++];
        if (tag == 0x0a) { /* field 1 = key */
            if (read_bytes(req, req_len, &pos, &key, &key_len) != 0) break;
        } else if (tag == 0x12) { /* field 2 = range_end */
            if (read_bytes(req, req_len, &pos, &range_end, &range_end_len) != 0) break;
        } else if (tag == 0x18) { /* field 3 = limit */
            uint64_t v = 0; if (read_varint(req, req_len, &pos, &v) != 0) break; limit = (int64_t)v;
        } else if (tag == 0x20) { /* field 4 = revision */
            uint64_t v = 0; if (read_varint(req, req_len, &pos, &v) != 0) break; rev = (int64_t)v;
        } else if (tag == 0x40) { /* field 8 = keys_only */
            uint64_t v = 0; if (read_varint(req, req_len, &pos, &v) != 0) break; keys_only = (int)v;
        } else if (tag == 0x48) { /* field 9 = count_only */
            uint64_t v = 0; if (read_varint(req, req_len, &pos, &v) != 0) break; count_only = (int)v;
        } else {
            uint64_t skip = 0; read_varint(req, req_len, &pos, &skip);
        }
    }

    /* Build RangeResponse:
     *   field 1 (header) = ResponseHeader (with revision)
     *   field 2 (kvs) = repeated KeyValue (length-delimited), tag = 0x0a
     *   field 3 (more)  = bool, tag = 0x10
     *   field 4 (count) = int64, tag = 0x20
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
                if (!count_only) {
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
                    /* value (skip if keys_only) */
                    if (!keys_only && out_kv.value.len > 0) {
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
                }
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
            size_t effective_limit = (limit > 0) ? (size_t)limit : n;
            for (size_t i = 0; i < n && i < effective_limit; i++) {
                if (count_only) continue;
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
                if (!keys_only && kvs[i].value.len > 0) {
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
            /* Set more flag if limit truncated results */
            if (limit > 0 && n > (size_t)limit) {
                kv_count = (size_t)limit;
                if (rpos + 10 > resp_cap) {
                    resp_cap = rpos + 16;
                    uint8_t *tmp = (uint8_t *)realloc(resp, resp_cap);
                    if (!tmp) { free(resp); free(kvs); if(key)free(key); if(range_end)free(range_end); return (cetcd_rpc_bytes){NULL,0}; }
                    resp = tmp;
                }
                resp[rpos++] = 0x10; /* field 3 = more */
                resp[rpos++] = 0x01; /* true */
            }
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
    resp[rpos++] = 0x20; /* field 4 = count */
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
    size_t pos = 0;
    uint8_t *key = NULL; size_t key_len = 0;
    uint8_t *range_end = NULL; size_t range_end_len = 0;
    int prev_kv_flag = 0;
    while (pos < req_len) {
        uint8_t tag = req[pos++];
        if (tag == 0x0a) { /* key */
            if (read_bytes(req, req_len, &pos, &key, &key_len) != 0) break;
        } else if (tag == 0x12) { /* range_end */
            if (read_bytes(req, req_len, &pos, &range_end, &range_end_len) != 0) break;
        } else if (tag == 0x18) { /* prev_kv (bool) */
            uint64_t tmp = 0; if (read_varint(req, req_len, &pos, &tmp) != 0) break; prev_kv_flag = (int)tmp;
        } else {
            uint64_t skip = 0; read_varint(req, req_len, &pos, &skip);
        }
    }

    /* DeleteRangeResponse:
     *   field 1 (header)   = ResponseHeader, tag = 0x0a
     *   field 2 (deleted)  = int64, tag = 0x10
     *   field 3 (prev_kvs) = repeated KeyValue, tag = 0x1a (when prev_kv=true)
     */
    int64_t rev = 0;
    int64_t deleted_count = 0;

    /* We'll collect prev_kv data here */
    uint8_t *prev_kvs_buf = NULL;
    size_t prev_kvs_len = 0;
    size_t prev_kvs_cap = 0;

    if (key && g_rpc_store) {
        if (!range_end || range_end_len == 0) {
            /* Point delete */
            if (prev_kv_flag) {
                cetcd_kv old_kv;
                memset(&old_kv, 0, sizeof(old_kv));
                if (cetcd_mvcc_get(g_rpc_store, 0, key, key_len, &old_kv) == 0) {
                    deleted_count = 1;
                    /* Encode old_kv as KeyValue, wrapped in field 3 (prev_kvs, tag 0x1a) */
                    uint8_t kv_enc[1024]; size_t ke = 0;
                    kv_enc[ke++] = 0x0a; /* key */
                    write_varint_local(kv_enc, sizeof(kv_enc), &ke, old_kv.key.len);
                    memcpy(kv_enc + ke, old_kv.key.data, old_kv.key.len); ke += old_kv.key.len;
                    kv_enc[ke++] = 0x10; /* create_revision */
                    write_varint_local(kv_enc, sizeof(kv_enc), &ke, (uint64_t)old_kv.create_rev.main);
                    kv_enc[ke++] = 0x18; /* mod_revision */
                    write_varint_local(kv_enc, sizeof(kv_enc), &ke, (uint64_t)old_kv.mod_rev.main);
                    kv_enc[ke++] = 0x20; /* version */
                    write_varint_local(kv_enc, sizeof(kv_enc), &ke, (uint64_t)old_kv.version);
                    if (old_kv.value.len > 0) {
                        kv_enc[ke++] = 0x2a; /* value */
                        write_varint_local(kv_enc, sizeof(kv_enc), &ke, old_kv.value.len);
                        memcpy(kv_enc + ke, old_kv.value.data, old_kv.value.len); ke += old_kv.value.len;
                    }
                    size_t need = ke + 10;
                    prev_kvs_buf = (uint8_t *)malloc(need);
                    if (prev_kvs_buf) {
                        prev_kvs_buf[prev_kvs_len++] = 0x1a; /* field 3 = prev_kvs */
                        write_varint_local(prev_kvs_buf, need, &prev_kvs_len, (uint64_t)ke);
                        memcpy(prev_kvs_buf + prev_kvs_len, kv_enc, ke);
                        prev_kvs_len += ke;
                        prev_kvs_cap = need;
                    }
                    free((void*)old_kv.key.data);
                    free((void*)old_kv.value.data);
                }
            }
            cetcd_revision r = cetcd_mvcc_delete(g_rpc_store, key, key_len);
            rev = r.main;
            if (!prev_kv_flag && rev > 0) deleted_count = 1;
        } else {
            /* Range delete: get all keys, then delete each one */
            cetcd_kv *kvs = NULL; size_t n = 0;
            cetcd_mvcc_range(g_rpc_store, 0, key, key_len, range_end, range_end_len, &kvs, &n);
            deleted_count = (int64_t)n;

            if (prev_kv_flag && n > 0) {
                prev_kvs_cap = n * 256;
                prev_kvs_buf = (uint8_t *)malloc(prev_kvs_cap);
                if (prev_kvs_buf) {
                    for (size_t i = 0; i < n; i++) {
                        uint8_t kv_enc[1024];
                        size_t kp = 0;
                        kv_enc[kp++] = 0x0a; /* key */
                        write_varint_local(kv_enc, sizeof(kv_enc), &kp, kvs[i].key.len);
                        memcpy(kv_enc + kp, kvs[i].key.data, kvs[i].key.len);
                        kp += kvs[i].key.len;
                        kv_enc[kp++] = 0x10; /* create_revision */
                        write_varint_local(kv_enc, sizeof(kv_enc), &kp, (uint64_t)kvs[i].create_rev.main);
                        kv_enc[kp++] = 0x18; /* mod_revision */
                        write_varint_local(kv_enc, sizeof(kv_enc), &kp, (uint64_t)kvs[i].mod_rev.main);
                        kv_enc[kp++] = 0x20; /* version */
                        write_varint_local(kv_enc, sizeof(kv_enc), &kp, (uint64_t)kvs[i].version);
                        if (kvs[i].value.len > 0) {
                            kv_enc[kp++] = 0x2a; /* value */
                            write_varint_local(kv_enc, sizeof(kv_enc), &kp, kvs[i].value.len);
                            memcpy(kv_enc + kp, kvs[i].value.data, kvs[i].value.len);
                            kp += kvs[i].value.len;
                        }
                        /* Append to prev_kvs_buf as field 3 (tag 0x1a) */
                        size_t needed = prev_kvs_len + 1 + 5 + kp;
                        if (needed > prev_kvs_cap) {
                            prev_kvs_cap = needed * 2;
                            uint8_t *tmp = (uint8_t *)realloc(prev_kvs_buf, prev_kvs_cap);
                            if (!tmp) { free(prev_kvs_buf); prev_kvs_buf = NULL; prev_kvs_len = 0; break; }
                            prev_kvs_buf = tmp;
                        }
                        prev_kvs_buf[prev_kvs_len++] = 0x1a; /* field 3 = prev_kvs */
                        write_varint_local(prev_kvs_buf, prev_kvs_cap, &prev_kvs_len, (uint64_t)kp);
                        memcpy(prev_kvs_buf + prev_kvs_len, kv_enc, kp);
                        prev_kvs_len += kp;
                    }
                }
            }

            /* Delete each key */
            for (size_t i = 0; i < n; i++) {
                cetcd_revision r = cetcd_mvcc_delete(g_rpc_store, kvs[i].key.data, kvs[i].key.len);
                if (r.main > rev) rev = r.main;
            }
            if (rev == 0 && g_rpc_store) rev = cetcd_mvcc_revision(g_rpc_store);
            if (kvs) cetcd_kv_free_contents(kvs, n);
        }
    }

    if (key) free(key);
    if (range_end) free(range_end);

    /* Build response */
    uint8_t hdr_buf[32];
    size_t hpos = 0;
    hdr_buf[hpos++] = 0x18; /* revision */
    write_varint_local(hdr_buf, sizeof(hdr_buf), &hpos, (uint64_t)(rev > 0 ? rev : 1));

    size_t resp_cap = 64 + hpos + (prev_kvs_len > 0 ? prev_kvs_len : 0);
    uint8_t *resp = (uint8_t *)malloc(resp_cap);
    if (!resp) { if (prev_kvs_buf) free(prev_kvs_buf); return (cetcd_rpc_bytes){NULL, 0}; }
    size_t rpos = 0;
    resp[rpos++] = 0x0a; /* field 1 = header */
    write_varint_local(resp, resp_cap, &rpos, (uint64_t)hpos);
    memcpy(resp + rpos, hdr_buf, hpos);
    rpos += hpos;
    resp[rpos++] = 0x10; /* field 2 = deleted */
    write_varint_local(resp, resp_cap, &rpos, (uint64_t)deleted_count);

    if (prev_kvs_len > 0 && prev_kvs_buf) {
        memcpy(resp + rpos, prev_kvs_buf, prev_kvs_len);
        rpos += prev_kvs_len;
        free(prev_kvs_buf);
    }

    return (cetcd_rpc_bytes){resp, rpos};
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
 * Compare:
 *   field 1 (result)  = enum (EQUAL=0, GREATER=1, LESS=2, NOT_EQUAL=3), tag = 0x08
 *   field 2 (target)  = enum (VERSION=0, CREATE=1, MOD=2, VALUE=3, LEASE=4), tag = 0x10
 *   field 3 (key)     = bytes, tag = 0x1a
 *   field 4 (version) = int64, tag = 0x20
 *   field 5 (create_revision) = int64, tag = 0x28
 *   field 6 (mod_revision)    = int64, tag = 0x30
 *   field 7 (value)   = bytes, tag = 0x3a
 *   field 8 (lease)   = int64, tag = 0x40
 *
 * TxnResponse:
 *   field 1 (header)    = ResponseHeader
 *   field 2 (succeeded) = bool, tag = 0x10
 *   field 3 (responses) = repeated ResponseOp, tag = 0x1a
 */

#define TXN_MAX_COMPARES 16
#define TXN_MAX_OPS 32

typedef struct {
    uint8_t *key;     size_t key_len;
    int      result;  /* 0=EQUAL, 1=GREATER, 2=LESS, 3=NOT_EQUAL */
    int      target;  /* 0=VERSION, 1=CREATE, 2=MOD, 3=VALUE, 4=LEASE */
    int64_t  version;
    int64_t  create_revision;
    int64_t  mod_revision;
    uint8_t *value;   size_t value_len;
    int64_t  lease;
} txn_compare_t;

typedef struct {
    const uint8_t *data;
    size_t         len;
} txn_op_t;

cetcd_rpc_bytes kv_handle_txn(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len) {
    (void)rpc;
    txn_compare_t compares[TXN_MAX_COMPARES];
    size_t n_compares = 0;
    txn_op_t success_ops[TXN_MAX_OPS];
    size_t n_success = 0;
    txn_op_t failure_ops[TXN_MAX_OPS];
    size_t n_failure = 0;
    memset(compares, 0, sizeof(compares));

    /* --- Phase 1: Parse the request into compares, success ops, failure ops --- */
    size_t pos = 0;
    while (pos < req_len) {
        uint8_t tag = req[pos++];
        if (tag == 0x0a) {
            /* Compare clause */
            uint64_t clen = 0;
            if (read_varint(req, req_len, &pos, &clen) != 0) break;
            size_t cend = pos + (size_t)clen;
            if (cend > req_len) cend = req_len;
            if (n_compares < TXN_MAX_COMPARES) {
                txn_compare_t *c = &compares[n_compares];
                while (pos < cend) {
                    uint8_t ctag = req[pos++];
                    if (ctag == 0x08) {
                        uint64_t v = 0; read_varint(req, cend, &pos, &v); c->result = (int)v;
                    } else if (ctag == 0x10) {
                        uint64_t v = 0; read_varint(req, cend, &pos, &v); c->target = (int)v;
                    } else if (ctag == 0x1a) {
                        read_bytes(req, cend, &pos, &c->key, &c->key_len);
                    } else if (ctag == 0x20) {
                        uint64_t v = 0; read_varint(req, cend, &pos, &v); c->version = (int64_t)v;
                    } else if (ctag == 0x28) {
                        uint64_t v = 0; read_varint(req, cend, &pos, &v); c->create_revision = (int64_t)v;
                    } else if (ctag == 0x30) {
                        uint64_t v = 0; read_varint(req, cend, &pos, &v); c->mod_revision = (int64_t)v;
                    } else if (ctag == 0x3a) {
                        read_bytes(req, cend, &pos, &c->value, &c->value_len);
                    } else if (ctag == 0x40) {
                        uint64_t v = 0; read_varint(req, cend, &pos, &v); c->lease = (int64_t)v;
                    } else {
                        uint64_t skip = 0; read_varint(req, cend, &pos, &skip);
                    }
                }
                n_compares++;
            }
            pos = cend;
        } else if (tag == 0x12) {
            /* Success op */
            uint64_t olen = 0;
            if (read_varint(req, req_len, &pos, &olen) != 0) break;
            if (pos + olen > req_len) break;
            if (n_success < TXN_MAX_OPS) {
                success_ops[n_success].data = req + pos;
                success_ops[n_success].len  = (size_t)olen;
                n_success++;
            }
            pos += (size_t)olen;
        } else if (tag == 0x1a) {
            /* Failure op */
            uint64_t flen = 0;
            if (read_varint(req, req_len, &pos, &flen) != 0) break;
            if (pos + flen > req_len) break;
            if (n_failure < TXN_MAX_OPS) {
                failure_ops[n_failure].data = req + pos;
                failure_ops[n_failure].len  = (size_t)flen;
                n_failure++;
            }
            pos += (size_t)flen;
        } else {
            uint64_t skip = 0; read_varint(req, req_len, &pos, &skip);
        }
    }

    /* --- Phase 2: Evaluate compares against the MVCC store --- */
    bool succeeded = true;
    for (size_t i = 0; i < n_compares; i++) {
        txn_compare_t *c = &compares[i];
        bool cmp_ok = false;

        if (c->key && g_rpc_store) {
            cetcd_kv kv;
            memset(&kv, 0, sizeof(kv));
            int found = cetcd_mvcc_get(g_rpc_store, 0, c->key, c->key_len, &kv);

            if (c->target == 3) { /* VALUE: bytes comparison */
                const uint8_t *act = (found == 0) ? kv.value.data : NULL;
                size_t act_len = (found == 0) ? kv.value.len : 0;
                int cmp = 0;
                size_t min_len = act_len < c->value_len ? act_len : c->value_len;
                if (min_len > 0) cmp = memcmp(act, c->value, min_len);
                if (cmp == 0) {
                    if (act_len < c->value_len) cmp = -1;
                    else if (act_len > c->value_len) cmp = 1;
                }
                switch (c->result) {
                    case 0: cmp_ok = (cmp == 0); break; /* EQUAL */
                    case 1: cmp_ok = (cmp > 0);  break; /* GREATER */
                    case 2: cmp_ok = (cmp < 0);  break; /* LESS */
                    case 3: cmp_ok = (cmp != 0); break; /* NOT_EQUAL */
                }
            } else {
                /* Integer comparison */
                int64_t actual = 0;
                int64_t target_val = 0;
                if (found == 0) {
                    switch (c->target) {
                        case 0: actual = kv.version;        target_val = c->version;          break;
                        case 1: actual = kv.create_rev.main; target_val = c->create_revision; break;
                        case 2: actual = kv.mod_rev.main;    target_val = c->mod_revision;    break;
                        case 4: actual = kv.lease_id;        target_val = c->lease;           break;
                    }
                }
                switch (c->result) {
                    case 0: cmp_ok = (actual == target_val); break; /* EQUAL */
                    case 1: cmp_ok = (actual >  target_val); break; /* GREATER */
                    case 2: cmp_ok = (actual <  target_val); break; /* LESS */
                    case 3: cmp_ok = (actual != target_val); break; /* NOT_EQUAL */
                }
            }
            free((void*)kv.key.data);
            free((void*)kv.value.data);
        } else {
            cmp_ok = true; /* No key or no store: treat as pass */
        }

        if (!cmp_ok) { succeeded = false; break; }
    }

    /* --- Phase 3: Execute the appropriate ops and build response --- */
    txn_op_t *ops = succeeded ? success_ops : failure_ops;
    size_t n_ops  = succeeded ? n_success   : n_failure;

    int64_t final_rev = g_rpc_store ? cetcd_mvcc_revision(g_rpc_store) : 0;

    size_t resp_cap = 4096;
    uint8_t *resp = (uint8_t *)malloc(resp_cap);
    if (!resp) goto txn_cleanup;
    size_t rpos = 0;
    /* Reserve 2 bytes for header placeholder */
    rpos += 2;

    for (size_t i = 0; i < n_ops; i++) {
        const uint8_t *od = ops[i].data;
        size_t ol = ops[i].len;
        if (ol == 0) continue;
        size_t op_pos = 0;
        uint8_t op_tag = od[op_pos++];

        if (op_tag == 0x12) {
            /* RequestPut */
            uint64_t plen = 0;
            if (read_varint(od, ol, &op_pos, &plen) != 0) continue;
            size_t put_end = op_pos + (size_t)plen;
            if (put_end > ol) put_end = ol;
            uint8_t *pk = NULL, *pv = NULL;
            size_t pk_len = 0, pv_len = 0;
            int64_t lease_id = 0;
            while (op_pos < put_end) {
                uint8_t ptag = od[op_pos++];
                if (ptag == 0x0a) {
                    read_bytes(od, put_end, &op_pos, &pk, &pk_len);
                } else if (ptag == 0x12) {
                    read_bytes(od, put_end, &op_pos, &pv, &pv_len);
                } else if (ptag == 0x18) {
                    uint64_t v = 0; read_varint(od, put_end, &op_pos, &v); lease_id = (int64_t)v;
                } else {
                    uint64_t skip = 0; read_varint(od, put_end, &op_pos, &skip);
                }
            }
            if (pk && pv && g_rpc_store) {
                cetcd_revision r = cetcd_mvcc_put(g_rpc_store, pk, pk_len, pv, pv_len, lease_id);
                if (r.main > final_rev) final_rev = r.main;
            }
            if (pk) free(pk);
            if (pv) free(pv);

            /* ResponsePut: header (field 1 = 0x0a, inner: revision 0x18) */
            uint8_t put_inner[32]; size_t pp = 0;
            put_inner[pp++] = 0x18;
            write_varint_local(put_inner, sizeof(put_inner), &pp, (uint64_t)(final_rev > 0 ? final_rev : 1));
            /* ResponseOp wrapping: field 2 (ResponsePut) tag = 0x12 */
            if (rpos + 20 > resp_cap) { resp_cap *= 2; uint8_t *t = realloc(resp, resp_cap); if (!t) { free(resp); goto txn_cleanup; } resp = t; }
            resp[rpos++] = 0x1a; /* field 3 = responses */
            size_t inner_msg_len = 1 + 1 + pp; /* tag(0x12) + len + put_inner */
            write_varint_local(resp, resp_cap, &rpos, (uint64_t)inner_msg_len);
            resp[rpos++] = 0x12; /* field 2 = ResponsePut */
            write_varint_local(resp, resp_cap, &rpos, (uint64_t)pp);
            memcpy(resp + rpos, put_inner, pp); rpos += pp;

        } else if (op_tag == 0x1a) {
            /* RequestDeleteRange */
            uint64_t dlen = 0;
            if (read_varint(od, ol, &op_pos, &dlen) != 0) continue;
            size_t del_end = op_pos + (size_t)dlen;
            if (del_end > ol) del_end = ol;
            uint8_t *dk = NULL; size_t dk_len = 0;
            while (op_pos < del_end) {
                uint8_t dtag = od[op_pos++];
                if (dtag == 0x0a) {
                    read_bytes(od, del_end, &op_pos, &dk, &dk_len);
                } else {
                    uint64_t skip = 0; read_varint(od, del_end, &op_pos, &skip);
                }
            }
            int64_t del_rev = 0;
            if (dk && g_rpc_store) {
                cetcd_revision r = cetcd_mvcc_delete(g_rpc_store, dk, dk_len);
                del_rev = r.main;
                if (del_rev > final_rev) final_rev = del_rev;
            }
            if (dk) free(dk);

            /* ResponseDeleteRange: header + deleted */
            uint8_t del_inner[32]; size_t dp = 0;
            del_inner[dp++] = 0x18;
            write_varint_local(del_inner, sizeof(del_inner), &dp, (uint64_t)(del_rev > 0 ? del_rev : 1));
            del_inner[dp++] = 0x10; /* field 2 = deleted */
            write_varint_local(del_inner, sizeof(del_inner), &dp, (del_rev > 0 ? 1 : 0));
            if (rpos + 20 > resp_cap) { resp_cap *= 2; uint8_t *t = realloc(resp, resp_cap); if (!t) { free(resp); goto txn_cleanup; } resp = t; }
            resp[rpos++] = 0x1a; /* field 3 = responses */
            size_t inner_msg_len = 1 + 1 + dp;
            write_varint_local(resp, resp_cap, &rpos, (uint64_t)inner_msg_len);
            resp[rpos++] = 0x1a; /* field 3 = ResponseDeleteRange */
            write_varint_local(resp, resp_cap, &rpos, (uint64_t)dp);
            memcpy(resp + rpos, del_inner, dp); rpos += dp;

        } else if (op_tag == 0x0a) {
            /* RequestRange — query MVCC store and return actual results */
            uint64_t rlen_val = 0;
            if (read_varint(od, ol, &op_pos, &rlen_val) != 0) continue;
            const uint8_t *rd = od + op_pos;
            size_t rl = (size_t)rlen_val;
            op_pos += (size_t)rlen_val;

            /* Parse RequestRange: key (0x0a), range_end (0x12), revision (0x20) */
            uint8_t *rkey = NULL; size_t rkey_len = 0;
            uint8_t *rrange_end = NULL; size_t rrange_end_len = 0;
            int64_t rrev = 0;
            size_t rp_pos = 0;
            while (rp_pos < rl) {
                uint8_t rtag = rd[rp_pos++];
                if (rtag == 0x0a) {
                    read_bytes(rd, rl, &rp_pos, &rkey, &rkey_len);
                } else if (rtag == 0x12) {
                    read_bytes(rd, rl, &rp_pos, &rrange_end, &rrange_end_len);
                } else if (rtag == 0x20) {
                    uint64_t v = 0; read_varint(rd, rl, &rp_pos, &v); rrev = (int64_t)v;
                } else {
                    uint64_t skip = 0; read_varint(rd, rl, &rp_pos, &skip);
                }
            }

            /* Query MVCC store */
            size_t rng_count = 0;
            uint8_t *kvs_buf = NULL; size_t kvs_len = 0; size_t kvs_cap = 0;

            if (rkey && g_rpc_store) {
                if (!rrange_end || rrange_end_len == 0) {
                    /* Point get */
                    cetcd_kv out_kv;
                    memset(&out_kv, 0, sizeof(out_kv));
                    if (cetcd_mvcc_get(g_rpc_store, rrev, rkey, rkey_len, &out_kv) == 0) {
                        rng_count = 1;
                        uint8_t kv_enc[1024]; size_t kp = 0;
                        kv_enc[kp++] = 0x0a;
                        write_varint_local(kv_enc, sizeof(kv_enc), &kp, out_kv.key.len);
                        memcpy(kv_enc + kp, out_kv.key.data, out_kv.key.len); kp += out_kv.key.len;
                        kv_enc[kp++] = 0x10;
                        write_varint_local(kv_enc, sizeof(kv_enc), &kp, (uint64_t)out_kv.create_rev.main);
                        kv_enc[kp++] = 0x18;
                        write_varint_local(kv_enc, sizeof(kv_enc), &kp, (uint64_t)out_kv.mod_rev.main);
                        kv_enc[kp++] = 0x20;
                        write_varint_local(kv_enc, sizeof(kv_enc), &kp, (uint64_t)out_kv.version);
                        if (out_kv.value.len > 0) {
                            kv_enc[kp++] = 0x2a;
                            write_varint_local(kv_enc, sizeof(kv_enc), &kp, out_kv.value.len);
                            memcpy(kv_enc + kp, out_kv.value.data, out_kv.value.len); kp += out_kv.value.len;
                        }
                        kvs_cap = kp + 10;
                        kvs_buf = (uint8_t *)malloc(kvs_cap);
                        if (kvs_buf) {
                            kvs_buf[kvs_len++] = 0x0a; /* field 2 = kvs */
                            write_varint_local(kvs_buf, kvs_cap, &kvs_len, (uint64_t)kp);
                            memcpy(kvs_buf + kvs_len, kv_enc, kp); kvs_len += kp;
                        }
                        free((void*)out_kv.key.data);
                        free((void*)out_kv.value.data);
                    }
                } else {
                    /* Range query */
                    cetcd_kv *kvs = NULL; size_t n = 0;
                    cetcd_mvcc_range(g_rpc_store, rrev, rkey, rkey_len,
                                     rrange_end, rrange_end_len, &kvs, &n);
                    rng_count = n;
                    kvs_cap = n * 256 + 64;
                    kvs_buf = (uint8_t *)malloc(kvs_cap);
                    if (kvs_buf) {
                        for (size_t i = 0; i < n; i++) {
                            uint8_t kv_enc[1024]; size_t kp = 0;
                            kv_enc[kp++] = 0x0a;
                            write_varint_local(kv_enc, sizeof(kv_enc), &kp, kvs[i].key.len);
                            memcpy(kv_enc + kp, kvs[i].key.data, kvs[i].key.len); kp += kvs[i].key.len;
                            kv_enc[kp++] = 0x10;
                            write_varint_local(kv_enc, sizeof(kv_enc), &kp, (uint64_t)kvs[i].create_rev.main);
                            kv_enc[kp++] = 0x18;
                            write_varint_local(kv_enc, sizeof(kv_enc), &kp, (uint64_t)kvs[i].mod_rev.main);
                            kv_enc[kp++] = 0x20;
                            write_varint_local(kv_enc, sizeof(kv_enc), &kp, (uint64_t)kvs[i].version);
                            if (kvs[i].value.len > 0) {
                                kv_enc[kp++] = 0x2a;
                                write_varint_local(kv_enc, sizeof(kv_enc), &kp, kvs[i].value.len);
                                memcpy(kv_enc + kp, kvs[i].value.data, kvs[i].value.len); kp += kvs[i].value.len;
                            }
                            size_t needed = kvs_len + 1 + 5 + kp;
                            if (needed > kvs_cap) {
                                kvs_cap = needed * 2;
                                uint8_t *tmp = (uint8_t *)realloc(kvs_buf, kvs_cap);
                                if (!tmp) { free(kvs_buf); kvs_buf = NULL; kvs_len = 0; break; }
                                kvs_buf = tmp;
                            }
                            kvs_buf[kvs_len++] = 0x0a; /* field 2 = kvs */
                            write_varint_local(kvs_buf, kvs_cap, &kvs_len, (uint64_t)kp);
                            memcpy(kvs_buf + kvs_len, kv_enc, kp); kvs_len += kp;
                        }
                    }
                    if (kvs) cetcd_kv_free_contents(kvs, n);
                }
            }

            /* Build ResponseRange inner: count + kvs */
            size_t rng_inner_cap = 32 + (kvs_len > 0 ? kvs_len : 0);
            uint8_t *rng_inner = (uint8_t *)malloc(rng_inner_cap);
            size_t rp = 0;
            if (rng_inner) {
                rng_inner[rp++] = 0x20; /* field 4 = count */
                write_varint_local(rng_inner, rng_inner_cap, &rp, (uint64_t)rng_count);
                if (kvs_len > 0 && kvs_buf) {
                    memcpy(rng_inner + rp, kvs_buf, kvs_len);
                    rp += kvs_len;
                }
            }
            if (rkey) free(rkey);
            if (rrange_end) free(rrange_end);
            if (kvs_buf) free(kvs_buf);

            if (rng_inner) {
                if (rpos + 20 + rp > resp_cap) {
                    resp_cap = resp_cap * 2 + rp + 64;
                    uint8_t *t = realloc(resp, resp_cap);
                    if (!t) { free(resp); free(rng_inner); goto txn_cleanup; }
                    resp = t;
                }
                resp[rpos++] = 0x1a; /* field 3 = responses */
                size_t inner_msg_len = 1 + 1 + rp;
                write_varint_local(resp, resp_cap, &rpos, (uint64_t)inner_msg_len);
                resp[rpos++] = 0x0a; /* field 1 = ResponseRange */
                write_varint_local(resp, resp_cap, &rpos, (uint64_t)rp);
                memcpy(resp + rpos, rng_inner, rp); rpos += rp;
                free(rng_inner);
            }
        }
    }

    /* Add succeeded field */
    if (rpos + 2 > resp_cap) { resp_cap += 8; uint8_t *t = realloc(resp, resp_cap); if (!t) { free(resp); goto txn_cleanup; } resp = t; }
    resp[rpos++] = 0x10; /* field 2 = succeeded */
    resp[rpos++] = succeeded ? 0x01 : 0x00;

    /* Write header at the beginning */
    {
        uint8_t hdr_buf[32];
        size_t hpos = 0;
        hdr_buf[hpos++] = 0x18; /* field 3 = revision */
        write_varint_local(hdr_buf, sizeof(hdr_buf), &hpos, (uint64_t)(final_rev > 0 ? final_rev : 1));

        size_t hdr_total = 1 + 1 + hpos; /* tag + len + data (assume hpos < 128) */
        memmove(resp + hdr_total, resp + 2, rpos - 2);
        rpos = rpos - 2 + hdr_total;
        resp[0] = 0x0a; /* field 1 = header */
        resp[1] = (uint8_t)hpos;
        memcpy(resp + 2, hdr_buf, hpos);
    }

    /* Free compares */
    for (size_t i = 0; i < n_compares; i++) {
        free(compares[i].key);
        free(compares[i].value);
    }
    return (cetcd_rpc_bytes){resp, rpos};

txn_cleanup:
    for (size_t i = 0; i < n_compares; i++) {
        free(compares[i].key);
        free(compares[i].value);
    }
    return (cetcd_rpc_bytes){NULL, 0};
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
    /* CompactResponse: header with revision */
    int64_t current_rev = g_rpc_store ? cetcd_mvcc_revision(g_rpc_store) : 0;
    uint8_t hdr_buf[32];
    size_t hpos = 0;
    hdr_buf[hpos++] = 0x18; /* field 3 = revision */
    write_varint_local(hdr_buf, sizeof(hdr_buf), &hpos, (uint64_t)(current_rev > 0 ? current_rev : 1));

    uint8_t resp[64];
    size_t rpos = 0;
    resp[rpos++] = 0x0a; /* field 1 = header */
    write_varint_local(resp, sizeof(resp), &rpos, (uint64_t)hpos);
    memcpy(resp + rpos, hdr_buf, hpos);
    rpos += hpos;

    uint8_t *out = (uint8_t *)malloc(rpos);
    if (!out) return (cetcd_rpc_bytes){NULL, 0};
    memcpy(out, resp, rpos);
    return (cetcd_rpc_bytes){out, rpos};
}
