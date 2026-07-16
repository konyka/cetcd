#include <stdlib.h>
#include <string.h>

/* Access to the internal mvcc store via global handles defined in v3rpc.c */
#include "cetcd/v3rpc.h"
#include "cetcd/mvcc.h"
#include "cetcd/lease.h"

/* Externs pointing to the live store/lease mgr (set by v3rpc_new) */
extern cetcd_mvcc_store *g_rpc_store;
extern cetcd_lease_mgr  *g_rpc_lease_mgr;

/* Detach key from its lease (if any) then delete from MVCC.
 * Lease detach runs only after a successful delete (fail-closed safe). */
static cetcd_revision delete_and_detach_(const uint8_t *key, size_t key_len) {
    cetcd_revision zero = {0, 0};
    if (!g_rpc_store || !key) return zero;
    int64_t lease_id = 0;
    if (g_rpc_lease_mgr) {
        cetcd_kv kv;
        memset(&kv, 0, sizeof(kv));
        if (cetcd_mvcc_get(g_rpc_store, 0, key, key_len, &kv) == 0) {
            lease_id = kv.lease_id;
            free((void *)kv.key.data);
            free((void *)kv.value.data);
        }
    }
    cetcd_revision r = cetcd_mvcc_delete(g_rpc_store, key, key_len);
    if (r.main > 0 && g_rpc_lease_mgr && lease_id > 0) {
        cetcd_lease_detach_key(g_rpc_lease_mgr, (cetcd_lease_id)lease_id,
                                key, key_len);
    }
    return r;
}

/* Batch-delete keys from a prior Range snapshot (one LMDB txn). */
static cetcd_revision delete_kvs_and_detach_(const cetcd_kv *kvs, size_t n) {
    cetcd_revision zero = {0, 0};
    if (!g_rpc_store || !kvs || n == 0) return zero;

    const uint8_t **keys = (const uint8_t **)calloc(n, sizeof(*keys));
    size_t *lens = (size_t *)calloc(n, sizeof(*lens));
    if (!keys || !lens) {
        free(keys);
        free(lens);
        cetcd_revision last = zero;
        for (size_t i = 0; i < n; i++) {
            cetcd_revision r = delete_and_detach_(kvs[i].key.data, kvs[i].key.len);
            if (r.main > last.main) last = r;
        }
        return last;
    }

    for (size_t i = 0; i < n; i++) {
        keys[i] = kvs[i].key.data;
        lens[i] = kvs[i].key.len;
    }
    cetcd_revision r = cetcd_mvcc_delete_keys(g_rpc_store, keys, lens, n);
    if (r.main > 0 && g_rpc_lease_mgr) {
        for (size_t i = 0; i < n; i++) {
            if (kvs[i].lease_id > 0) {
                cetcd_lease_detach_key(g_rpc_lease_mgr,
                                        (cetcd_lease_id)kvs[i].lease_id,
                                        kvs[i].key.data, kvs[i].key.len);
            }
        }
    }
    free(keys);
    free(lens);
    return r;
}

/* True if lease_id is 0 or refers to a live lease. */
static int lease_ok_for_put_(int64_t lease_id) {
    if (lease_id <= 0) return 1;
    return g_rpc_lease_mgr &&
           cetcd_lease_exists(g_rpc_lease_mgr, (cetcd_lease_id)lease_id);
}
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

/* KeyValue field 6 (lease), tag 0x30 — omit when lease_id == 0 (proto3). */
static void append_kv_lease_(uint8_t *buf, size_t cap, size_t *pos, int64_t lease_id) {
    if (!buf || !pos || lease_id <= 0 || *pos + 12 > cap) return;
    buf[(*pos)++] = 0x30;
    write_varint_local(buf, cap, pos, (uint64_t)lease_id);
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

    /* Fetch existing KV when needed for prev_kv / ignore_* / lease rebinding. */
    cetcd_kv old_kv;
    memset(&old_kv, 0, sizeof(old_kv));
    int has_old = 0;
    if (key && g_rpc_store &&
        (prev_kv_flag || ignore_value || ignore_lease || g_rpc_lease_mgr)) {
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
        /* etcd: ignore_value / ignore_lease require an existing key (ErrKeyNotFound). */
        int ignore_missing = (ignore_value || ignore_lease) && !has_old;
        if (!ignore_missing && (val || ignore_value) && lease_ok_for_put_(lease_id)) {
            cetcd_revision r = cetcd_mvcc_put(g_rpc_store, key, key_len,
                                               val ? val : (const uint8_t*)"", val ? val_len : 0,
                                               lease_id);
            rev = r.main;
            /* Lease index updates only after a successful (fail-closed) put. */
            if (r.main > 0 && g_rpc_lease_mgr) {
                if (has_old && old_kv.lease_id > 0 && old_kv.lease_id != lease_id) {
                    cetcd_lease_detach_key(g_rpc_lease_mgr,
                                            (cetcd_lease_id)old_kv.lease_id,
                                            key, key_len);
                }
                if (lease_id > 0) {
                    cetcd_lease_attach_key(g_rpc_lease_mgr, (cetcd_lease_id)lease_id,
                                            key, key_len);
                }
            }
        }
    }
    if (key) free(key);
    if (val) free(val);

    /* PutResponse:
     *   field 1 (header)  = ResponseHeader, tag = 0x0a
     *   field 2 (prev_kv) = KeyValue, tag = 0x12 (when prev_kv=true in request)
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
            append_kv_lease_(pkv_buf, cap, &kp, old_kv.lease_id);
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
        resp[rpos++] = 0x12; /* field 2 = prev_kv */
        write_varint_local(resp, resp_cap, &rpos, (uint64_t)pkv_len);
        memcpy(resp + rpos, pkv_buf, pkv_len);
        rpos += pkv_len;
        free(pkv_buf);
    }

    return (cetcd_rpc_bytes){resp, rpos};
}

/* Comparison function for RangeRequest sorting */
static int range_sort_cmp(const void *a, const void *b, void *ctx) {
    const cetcd_kv *ka = (const cetcd_kv *)a;
    const cetcd_kv *kb = (const cetcd_kv *)b;
    int target = *(const int *)ctx;
    int cmp = 0;
    switch (target) {
        case 0: { /* KEY */
            size_t min_len = ka->key.len < kb->key.len ? ka->key.len : kb->key.len;
            cmp = min_len > 0 ? memcmp(ka->key.data, kb->key.data, min_len) : 0;
            if (cmp == 0) cmp = (ka->key.len < kb->key.len) ? -1 : (ka->key.len > kb->key.len) ? 1 : 0;
            break;
        }
        case 1: /* VERSION */
            cmp = (ka->version < kb->version) ? -1 : (ka->version > kb->version) ? 1 : 0;
            break;
        case 2: /* CREATE */
            cmp = (ka->create_rev.main < kb->create_rev.main) ? -1 : (ka->create_rev.main > kb->create_rev.main) ? 1 : 0;
            break;
        case 3: /* MOD */
            cmp = (ka->mod_rev.main < kb->mod_rev.main) ? -1 : (ka->mod_rev.main > kb->mod_rev.main) ? 1 : 0;
            break;
        case 4: { /* VALUE */
            size_t min_len = ka->value.len < kb->value.len ? ka->value.len : kb->value.len;
            cmp = min_len > 0 ? memcmp(ka->value.data, kb->value.data, min_len) : 0;
            if (cmp == 0) cmp = (ka->value.len < kb->value.len) ? -1 : (ka->value.len > kb->value.len) ? 1 : 0;
            break;
        }
    }
    return cmp;
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
    int sort_order = 0;  /* 0=NONE, 1=ASCEND, 2=DESCEND */
    int sort_target = 0; /* 0=KEY, 1=VERSION, 2=CREATE, 3=MOD, 4=VALUE */
    int64_t min_mod_rev = 0, max_mod_rev = 0;
    int64_t min_create_rev = 0, max_create_rev = 0;
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
        } else if (tag == 0x28) { /* field 5 = sort_order */
            uint64_t v = 0; if (read_varint(req, req_len, &pos, &v) != 0) break; sort_order = (int)v;
        } else if (tag == 0x30) { /* field 6 = sort_target */
            uint64_t v = 0; if (read_varint(req, req_len, &pos, &v) != 0) break; sort_target = (int)v;
        } else if (tag == 0x38) { /* field 7 = serializable (bool, no-op in single-node) */
            uint64_t v = 0; if (read_varint(req, req_len, &pos, &v) != 0) break;
        } else if (tag == 0x40) { /* field 8 = keys_only */
            uint64_t v = 0; if (read_varint(req, req_len, &pos, &v) != 0) break; keys_only = (int)v;
        } else if (tag == 0x48) { /* field 9 = count_only */
            uint64_t v = 0; if (read_varint(req, req_len, &pos, &v) != 0) break; count_only = (int)v;
        } else if (tag == 0x50) { /* field 10 = min_mod_revision */
            uint64_t v = 0; if (read_varint(req, req_len, &pos, &v) != 0) break; min_mod_rev = (int64_t)v;
        } else if (tag == 0x58) { /* field 11 = max_mod_revision */
            uint64_t v = 0; if (read_varint(req, req_len, &pos, &v) != 0) break; max_mod_rev = (int64_t)v;
        } else if (tag == 0x60) { /* field 12 = min_create_revision */
            uint64_t v = 0; if (read_varint(req, req_len, &pos, &v) != 0) break; min_create_rev = (int64_t)v;
        } else if (tag == 0x68) { /* field 13 = max_create_revision */
            uint64_t v = 0; if (read_varint(req, req_len, &pos, &v) != 0) break; max_create_rev = (int64_t)v;
        } else {
            uint64_t skip = 0; read_varint(req, req_len, &pos, &skip);
        }
    }

    /* Build RangeResponse:
     *   field 1 (header) = ResponseHeader (with revision)
     *   field 2 (kvs) = repeated KeyValue (length-delimited), tag = 0x12
     *   field 3 (more)  = bool, tag = 0x18
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
            int get_rc = cetcd_mvcc_get(g_rpc_store, rev, key, key_len, &out_kv);
            if (get_rc == CETCD_ERR_RANGE) {
                /* rev < compacted_rev — surface as RPC error (etcd ErrCompacted). */
                free((void*)out_kv.key.data);
                free((void*)out_kv.value.data);
                free(resp);
                if (key) free(key);
                if (range_end) free(range_end);
                return (cetcd_rpc_bytes){NULL, 0};
            }
            if (get_rc == 0) {
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
                    append_kv_lease_(kv_buf, sizeof(kv_buf), &kpos, out_kv.lease_id);
                    /* Write as field 2 (kvs) */
                    if (rpos + 1 + 5 + kpos > resp_cap) {
                        resp_cap = rpos + kpos + 64;
                        uint8_t *tmp = (uint8_t *)realloc(resp, resp_cap);
                        if (!tmp) { free(resp); if(key)free(key); if(range_end)free(range_end); return (cetcd_rpc_bytes){NULL,0}; }
                        resp = tmp;
                    }
                    resp[rpos++] = 0x12; /* field 2 = kvs */
                    write_varint_local(resp, resp_cap, &rpos, (uint64_t)kpos);
                    memcpy(resp + rpos, kv_buf, kpos);
                    rpos += kpos;
                }
                kv_count = 1;
            }
            free((void*)out_kv.key.data);
            free((void*)out_kv.value.data);
        } else {
            /* Range query (range_end='\0' FromKey handled in MVCC). */
            cetcd_kv *kvs = NULL; size_t n = 0;
            int range_rc = cetcd_mvcc_range(g_rpc_store, rev,
                             key, key_len,
                             range_end, range_end_len,
                             &kvs, &n);
            if (range_rc == CETCD_ERR_RANGE) {
                free(resp);
                if (key) free(key);
                if (range_end) free(range_end);
                return (cetcd_rpc_bytes){NULL, 0};
            }
            /* Apply min/max revision filters (fields 10-13) */
            if (n > 0 && (min_mod_rev > 0 || max_mod_rev > 0 || min_create_rev > 0 || max_create_rev > 0)) {
                size_t w = 0;
                for (size_t i = 0; i < n; i++) {
                    int keep = 1;
                    if (min_mod_rev > 0 && kvs[i].mod_rev.main < min_mod_rev) keep = 0;
                    if (keep && max_mod_rev > 0 && kvs[i].mod_rev.main > max_mod_rev) keep = 0;
                    if (keep && min_create_rev > 0 && kvs[i].create_rev.main < min_create_rev) keep = 0;
                    if (keep && max_create_rev > 0 && kvs[i].create_rev.main > max_create_rev) keep = 0;
                    if (keep) {
                        if (w != i) kvs[w] = kvs[i];
                        w++;
                    } else {
                        free((void*)kvs[i].key.data);
                        free((void*)kvs[i].value.data);
                    }
                }
                n = w;
            }
            /* Sort results if sort_order is specified (1=ASCEND, 2=DESCEND) */
            if (sort_order > 0 && n > 1) {
                /* Simple insertion sort using our comparison function */
                for (size_t i = 1; i < n; i++) {
                    cetcd_kv tmp = kvs[i];
                    size_t j = i;
                    while (j > 0) {
                        int cmp = range_sort_cmp(&kvs[j - 1], &tmp, &sort_target);
                        if (sort_order == 2) cmp = -cmp; /* DESCEND */
                        if (cmp <= 0) break;
                        kvs[j] = kvs[j - 1];
                        j--;
                    }
                    kvs[j] = tmp;
                }
            }
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
                append_kv_lease_(kv_buf, sizeof(kv_buf), &kpos, kvs[i].lease_id);
                if (rpos + 1 + 5 + kpos > resp_cap) {
                    resp_cap = resp_cap * 2 + kpos + 64;
                    uint8_t *tmp = (uint8_t *)realloc(resp, resp_cap);
                    if (!tmp) { free(resp); free(kvs); if(key)free(key); if(range_end)free(range_end); return (cetcd_rpc_bytes){NULL,0}; }
                    resp = tmp;
                }
                resp[rpos++] = 0x12; /* field 2 = kvs */
                write_varint_local(resp, resp_cap, &rpos, (uint64_t)kpos);
                memcpy(resp + rpos, kv_buf, kpos);
                rpos += kpos;
            }
            kv_count = n;
            /* Set more flag if limit truncated results; count stays total matches. */
            if (limit > 0 && n > (size_t)limit) {
                if (rpos + 10 > resp_cap) {
                    resp_cap = rpos + 16;
                    uint8_t *tmp = (uint8_t *)realloc(resp, resp_cap);
                    if (!tmp) { free(resp); free(kvs); if(key)free(key); if(range_end)free(range_end); return (cetcd_rpc_bytes){NULL,0}; }
                    resp = tmp;
                }
                resp[rpos++] = 0x18; /* field 3 = more */
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
                    append_kv_lease_(kv_enc, sizeof(kv_enc), &ke, old_kv.lease_id);
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
            cetcd_revision r = delete_and_detach_(key, key_len);
            rev = r.main;
            if (!prev_kv_flag && rev > 0) deleted_count = 1;
        } else {
            /* Range delete: snapshot keys, then one batch LMDB delete. */
            cetcd_kv *kvs = NULL; size_t n = 0;
            cetcd_mvcc_range(g_rpc_store, 0, key, key_len, range_end, range_end_len, &kvs, &n);

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
                        append_kv_lease_(kv_enc, sizeof(kv_enc), &kp, kvs[i].lease_id);
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

            cetcd_revision r = delete_kvs_and_detach_(kvs, n);
            rev = r.main;
            if (r.main > 0) {
                deleted_count = (int64_t)n;
            } else {
                deleted_count = 0;
                if (prev_kvs_buf) { free(prev_kvs_buf); prev_kvs_buf = NULL; prev_kvs_len = 0; }
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
 *   field 9 (range_end) = bytes, tag = 0x4a
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
    uint8_t *range_end; size_t range_end_len;
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
                    } else if (ctag == 0x4a) {
                        read_bytes(req, cend, &pos, &c->range_end, &c->range_end_len);
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
            if (c->range_end && c->range_end_len > 0) {
                /* Range compare: check all keys in [key, range_end) */
                cetcd_kv *rkv = NULL; size_t rn = 0;
                cetcd_mvcc_range(g_rpc_store, 0, c->key, c->key_len,
                                 c->range_end, c->range_end_len, &rkv, &rn);
                cmp_ok = true; /* vacuously true if no keys */
                for (size_t j = 0; j < rn; j++) {
                    if (c->target == 3) { /* VALUE */
                        int cmp = 0;
                        size_t min_len = rkv[j].value.len < c->value_len ? rkv[j].value.len : c->value_len;
                        if (min_len > 0) cmp = memcmp(rkv[j].value.data, c->value, min_len);
                        if (cmp == 0) {
                            if (rkv[j].value.len < c->value_len) cmp = -1;
                            else if (rkv[j].value.len > c->value_len) cmp = 1;
                        }
                        switch (c->result) {
                            case 0: cmp_ok = (cmp == 0); break;
                            case 1: cmp_ok = (cmp > 0);  break;
                            case 2: cmp_ok = (cmp < 0);  break;
                            case 3: cmp_ok = (cmp != 0); break;
                        }
                    } else {
                        int64_t actual = 0, target_val = 0;
                        switch (c->target) {
                            case 0: actual = rkv[j].version;        target_val = c->version;          break;
                            case 1: actual = rkv[j].create_rev.main; target_val = c->create_revision; break;
                            case 2: actual = rkv[j].mod_rev.main;    target_val = c->mod_revision;    break;
                            case 4: actual = rkv[j].lease_id;        target_val = c->lease;           break;
                        }
                        switch (c->result) {
                            case 0: cmp_ok = (actual == target_val); break;
                            case 1: cmp_ok = (actual >  target_val); break;
                            case 2: cmp_ok = (actual <  target_val); break;
                            case 3: cmp_ok = (actual != target_val); break;
                        }
                    }
                    if (!cmp_ok) break;
                }
                if (rkv) cetcd_kv_free_contents(rkv, rn);
            } else {
                /* Point compare */
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
                    /* Integer comparison: missing key → actual 0 (etcd). */
                    int64_t actual = 0;
                    int64_t target_val = 0;
                    switch (c->target) {
                        case 0:
                            if (found == 0) actual = kv.version;
                            target_val = c->version;
                            break;
                        case 1:
                            if (found == 0) actual = kv.create_rev.main;
                            target_val = c->create_revision;
                            break;
                        case 2:
                            if (found == 0) actual = kv.mod_rev.main;
                            target_val = c->mod_revision;
                            break;
                        case 4:
                            if (found == 0) actual = kv.lease_id;
                            target_val = c->lease;
                            break;
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
            }
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
            int want_prev_kv = 0;
            int ignore_value = 0, ignore_lease = 0;
            while (op_pos < put_end) {
                uint8_t ptag = od[op_pos++];
                if (ptag == 0x0a) {
                    read_bytes(od, put_end, &op_pos, &pk, &pk_len);
                } else if (ptag == 0x12) {
                    read_bytes(od, put_end, &op_pos, &pv, &pv_len);
                } else if (ptag == 0x18) {
                    uint64_t v = 0; read_varint(od, put_end, &op_pos, &v); lease_id = (int64_t)v;
                } else if (ptag == 0x20) {
                    uint64_t v = 0; read_varint(od, put_end, &op_pos, &v); want_prev_kv = (int)v;
                } else if (ptag == 0x28) {
                    uint64_t v = 0; read_varint(od, put_end, &op_pos, &v); ignore_value = (int)v;
                } else if (ptag == 0x30) {
                    uint64_t v = 0; read_varint(od, put_end, &op_pos, &v); ignore_lease = (int)v;
                } else {
                    uint64_t skip = 0; read_varint(od, put_end, &op_pos, &skip);
                }
            }
            /* Capture old KV if prev_kv/ignore_value/ignore_lease requested */
            uint8_t *prev_kv_buf = NULL; size_t prev_kv_len = 0;
            int has_old_key = 0;
            if ((want_prev_kv || ignore_value || ignore_lease) && pk && g_rpc_store) {
                cetcd_kv old_kv;
                memset(&old_kv, 0, sizeof(old_kv));
                if (cetcd_mvcc_get(g_rpc_store, 0, pk, pk_len, &old_kv) == 0) {
                    has_old_key = 1;
                    /* Apply ignore_value: use existing value */
                    if (ignore_value) {
                        if (pv) free(pv);
                        pv = (uint8_t *)malloc(old_kv.value.len + 1);
                        if (pv) { memcpy(pv, old_kv.value.data, old_kv.value.len); pv[old_kv.value.len] = '\0'; pv_len = old_kv.value.len; }
                    }
                    /* Apply ignore_lease: use existing lease */
                    if (ignore_lease) {
                        lease_id = old_kv.lease_id;
                    }
                    size_t cap = 256 + old_kv.key.len + old_kv.value.len;
                    prev_kv_buf = (uint8_t *)malloc(cap);
                    if (prev_kv_buf) {
                        size_t kp = 0;
                        prev_kv_buf[kp++] = 0x0a;
                        write_varint_local(prev_kv_buf, cap, &kp, old_kv.key.len);
                        memcpy(prev_kv_buf + kp, old_kv.key.data, old_kv.key.len); kp += old_kv.key.len;
                        prev_kv_buf[kp++] = 0x10;
                        write_varint_local(prev_kv_buf, cap, &kp, (uint64_t)old_kv.create_rev.main);
                        prev_kv_buf[kp++] = 0x18;
                        write_varint_local(prev_kv_buf, cap, &kp, (uint64_t)old_kv.mod_rev.main);
                        prev_kv_buf[kp++] = 0x20;
                        write_varint_local(prev_kv_buf, cap, &kp, (uint64_t)old_kv.version);
                        if (old_kv.value.len > 0) {
                            prev_kv_buf[kp++] = 0x2a;
                            write_varint_local(prev_kv_buf, cap, &kp, old_kv.value.len);
                            memcpy(prev_kv_buf + kp, old_kv.value.data, old_kv.value.len); kp += old_kv.value.len;
                        }
                        append_kv_lease_(prev_kv_buf, cap, &kp, old_kv.lease_id);
                        prev_kv_len = kp;
                    }
                    free((void*)old_kv.key.data);
                    free((void*)old_kv.value.data);
                }
            }
            /* etcd: ignore_value / ignore_lease require an existing key. */
            int ignore_missing = (ignore_value || ignore_lease) && !has_old_key;
            if (!ignore_missing && pk && (pv || ignore_value) && g_rpc_store && lease_ok_for_put_(lease_id)) {
                int64_t old_lease = 0;
                if (g_rpc_lease_mgr) {
                    cetcd_kv old_kv;
                    memset(&old_kv, 0, sizeof(old_kv));
                    if (cetcd_mvcc_get(g_rpc_store, 0, pk, pk_len, &old_kv) == 0) {
                        old_lease = old_kv.lease_id;
                        free((void *)old_kv.key.data);
                        free((void *)old_kv.value.data);
                    }
                }
                cetcd_revision r = cetcd_mvcc_put(g_rpc_store, pk, pk_len, pv, pv_len, lease_id);
                if (r.main > final_rev) final_rev = r.main;
                if (r.main > 0 && g_rpc_lease_mgr) {
                    if (old_lease > 0 && old_lease != lease_id) {
                        cetcd_lease_detach_key(g_rpc_lease_mgr,
                                                (cetcd_lease_id)old_lease,
                                                pk, pk_len);
                    }
                    if (lease_id > 0) {
                        cetcd_lease_attach_key(g_rpc_lease_mgr, (cetcd_lease_id)lease_id,
                                                pk, pk_len);
                    }
                }
            }
            if (pk) free(pk);
            if (pv) free(pv);

            /* ResponsePut: header (field 1) + prev_kv (field 2, tag 0x12) */
            uint8_t put_inner[256 + (prev_kv_len > 0 ? prev_kv_len : 0)];
            size_t pp = 0;
            /* Build ResponseHeader inner: field 3 = revision */
            uint8_t put_hdr[16]; size_t phip = 0;
            put_hdr[phip++] = 0x18;
            phip = write_varint_local(put_hdr, sizeof(put_hdr), &phip, (uint64_t)(final_rev > 0 ? final_rev : 1));
            /* field 1 = header */
            put_inner[pp++] = 0x0a;
            write_varint_local(put_inner, sizeof(put_inner), &pp, (uint64_t)phip);
            memcpy(put_inner + pp, put_hdr, phip); pp += phip;
            /* field 2 = prev_kv */
            if (prev_kv_len > 0 && prev_kv_buf) {
                put_inner[pp++] = 0x12;
                write_varint_local(put_inner, sizeof(put_inner), &pp, (uint64_t)prev_kv_len);
                memcpy(put_inner + pp, prev_kv_buf, prev_kv_len); pp += prev_kv_len;
                free(prev_kv_buf);
            }
            /* ResponseOp wrapping: field 2 (ResponsePut) tag = 0x12 */
            if (rpos + 20 + pp > resp_cap) { resp_cap = resp_cap * 2 + pp + 64; uint8_t *t = realloc(resp, resp_cap); if (!t) { free(resp); goto txn_cleanup; } resp = t; }
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
            uint8_t *drange_end = NULL; size_t drange_end_len = 0;
            int want_prev_kv = 0;
            while (op_pos < del_end) {
                uint8_t dtag = od[op_pos++];
                if (dtag == 0x0a) {
                    read_bytes(od, del_end, &op_pos, &dk, &dk_len);
                } else if (dtag == 0x12) {
                    read_bytes(od, del_end, &op_pos, &drange_end, &drange_end_len);
                } else if (dtag == 0x18) {
                    uint64_t v = 0; read_varint(od, del_end, &op_pos, &v); want_prev_kv = (int)v;
                } else {
                    uint64_t skip = 0; read_varint(od, del_end, &op_pos, &skip);
                }
            }
            int64_t del_rev = 0;
            int64_t deleted_count = 0;
            uint8_t *prev_kvs_buf = NULL; size_t prev_kvs_len = 0; size_t prev_kvs_cap = 0;
            if (dk && g_rpc_store) {
                if (drange_end && drange_end_len > 0) {
                    /* Range delete: one batch LMDB txn */
                    cetcd_kv *kvs = NULL; size_t n = 0;
                    cetcd_mvcc_range(g_rpc_store, 0, dk, dk_len, drange_end, drange_end_len, &kvs, &n);
                    if (want_prev_kv && n > 0) {
                        prev_kvs_cap = n * 256;
                        prev_kvs_buf = (uint8_t *)malloc(prev_kvs_cap);
                        if (prev_kvs_buf) {
                            for (size_t j = 0; j < n; j++) {
                                uint8_t kv_enc[1024]; size_t kp = 0;
                                kv_enc[kp++] = 0x0a;
                                write_varint_local(kv_enc, sizeof(kv_enc), &kp, kvs[j].key.len);
                                memcpy(kv_enc + kp, kvs[j].key.data, kvs[j].key.len); kp += kvs[j].key.len;
                                kv_enc[kp++] = 0x10;
                                write_varint_local(kv_enc, sizeof(kv_enc), &kp, (uint64_t)kvs[j].create_rev.main);
                                kv_enc[kp++] = 0x18;
                                write_varint_local(kv_enc, sizeof(kv_enc), &kp, (uint64_t)kvs[j].mod_rev.main);
                                kv_enc[kp++] = 0x20;
                                write_varint_local(kv_enc, sizeof(kv_enc), &kp, (uint64_t)kvs[j].version);
                                if (kvs[j].value.len > 0) {
                                    kv_enc[kp++] = 0x2a;
                                    write_varint_local(kv_enc, sizeof(kv_enc), &kp, kvs[j].value.len);
                                    memcpy(kv_enc + kp, kvs[j].value.data, kvs[j].value.len); kp += kvs[j].value.len;
                                }
                                append_kv_lease_(kv_enc, sizeof(kv_enc), &kp, kvs[j].lease_id);
                                size_t needed = prev_kvs_len + 1 + 5 + kp;
                                if (needed > prev_kvs_cap) {
                                    prev_kvs_cap = needed * 2;
                                    uint8_t *tmp = (uint8_t *)realloc(prev_kvs_buf, prev_kvs_cap);
                                    if (!tmp) { free(prev_kvs_buf); prev_kvs_buf = NULL; prev_kvs_len = 0; break; }
                                    prev_kvs_buf = tmp;
                                }
                                prev_kvs_buf[prev_kvs_len++] = 0x1a; /* field 3 = prev_kvs */
                                write_varint_local(prev_kvs_buf, prev_kvs_cap, &prev_kvs_len, (uint64_t)kp);
                                memcpy(prev_kvs_buf + prev_kvs_len, kv_enc, kp); prev_kvs_len += kp;
                            }
                        }
                    }
                    cetcd_revision r = delete_kvs_and_detach_(kvs, n);
                    del_rev = r.main;
                    if (r.main > 0) {
                        deleted_count = (int64_t)n;
                    } else {
                        deleted_count = 0;
                        if (prev_kvs_buf) { free(prev_kvs_buf); prev_kvs_buf = NULL; prev_kvs_len = 0; }
                    }
                    if (kvs) cetcd_kv_free_contents(kvs, n);
                } else {
                    /* Point delete */
                    if (want_prev_kv) {
                        cetcd_kv old_kv;
                        memset(&old_kv, 0, sizeof(old_kv));
                        if (cetcd_mvcc_get(g_rpc_store, 0, dk, dk_len, &old_kv) == 0) {
                            deleted_count = 1;
                            uint8_t kv_enc[1024]; size_t kp = 0;
                            kv_enc[kp++] = 0x0a;
                            write_varint_local(kv_enc, sizeof(kv_enc), &kp, old_kv.key.len);
                            memcpy(kv_enc + kp, old_kv.key.data, old_kv.key.len); kp += old_kv.key.len;
                            kv_enc[kp++] = 0x10;
                            write_varint_local(kv_enc, sizeof(kv_enc), &kp, (uint64_t)old_kv.create_rev.main);
                            kv_enc[kp++] = 0x18;
                            write_varint_local(kv_enc, sizeof(kv_enc), &kp, (uint64_t)old_kv.mod_rev.main);
                            kv_enc[kp++] = 0x20;
                            write_varint_local(kv_enc, sizeof(kv_enc), &kp, (uint64_t)old_kv.version);
                            if (old_kv.value.len > 0) {
                                kv_enc[kp++] = 0x2a;
                                write_varint_local(kv_enc, sizeof(kv_enc), &kp, old_kv.value.len);
                                memcpy(kv_enc + kp, old_kv.value.data, old_kv.value.len); kp += old_kv.value.len;
                            }
                            append_kv_lease_(kv_enc, sizeof(kv_enc), &kp, old_kv.lease_id);
                            prev_kvs_cap = kp + 10;
                            prev_kvs_buf = (uint8_t *)malloc(prev_kvs_cap);
                            if (prev_kvs_buf) {
                                prev_kvs_buf[prev_kvs_len++] = 0x1a;
                                write_varint_local(prev_kvs_buf, prev_kvs_cap, &prev_kvs_len, (uint64_t)kp);
                                memcpy(prev_kvs_buf + prev_kvs_len, kv_enc, kp); prev_kvs_len += kp;
                            }
                            free((void*)old_kv.key.data);
                            free((void*)old_kv.value.data);
                        }
                    }
                    cetcd_revision r = delete_and_detach_(dk, dk_len);
                    del_rev = r.main;
                    if (!want_prev_kv && del_rev > 0) deleted_count = 1;
                }
                if (del_rev > final_rev) final_rev = del_rev;
                if (del_rev == 0 && g_rpc_store) del_rev = cetcd_mvcc_revision(g_rpc_store);
            }
            if (dk) free(dk);
            if (drange_end) free(drange_end);

            /* ResponseDeleteRange: header (field 1) + deleted (field 2) + prev_kvs (field 3) */
            uint8_t del_hdr[16]; size_t dhip = 0;
            del_hdr[dhip++] = 0x18;
            dhip = write_varint_local(del_hdr, sizeof(del_hdr), &dhip, (uint64_t)(del_rev > 0 ? del_rev : 1));
            size_t del_inner_cap = 64 + dhip + (prev_kvs_len > 0 ? prev_kvs_len : 0);
            uint8_t *del_inner = (uint8_t *)malloc(del_inner_cap);
            if (!del_inner) { if (prev_kvs_buf) free(prev_kvs_buf); goto txn_cleanup; }
            size_t dp = 0;
            /* field 1 = header */
            del_inner[dp++] = 0x0a;
            dp = write_varint_local(del_inner, del_inner_cap, &dp, (uint64_t)dhip);
            memcpy(del_inner + dp, del_hdr, dhip); dp += dhip;
            /* field 2 = deleted */
            del_inner[dp++] = 0x10;
            dp = write_varint_local(del_inner, del_inner_cap, &dp, (uint64_t)deleted_count);
            /* field 3 = prev_kvs */
            if (prev_kvs_len > 0 && prev_kvs_buf) {
                memcpy(del_inner + dp, prev_kvs_buf, prev_kvs_len);
                dp += prev_kvs_len;
                free(prev_kvs_buf);
            }
            if (rpos + 20 + dp > resp_cap) { resp_cap = resp_cap * 2 + dp + 64; uint8_t *t = realloc(resp, resp_cap); if (!t) { free(resp); free(del_inner); goto txn_cleanup; } resp = t; }
            resp[rpos++] = 0x1a; /* field 3 = responses */
            size_t inner_msg_len = 1 + 1 + dp;
            write_varint_local(resp, resp_cap, &rpos, (uint64_t)inner_msg_len);
            resp[rpos++] = 0x1a; /* field 3 = ResponseDeleteRange */
            write_varint_local(resp, resp_cap, &rpos, (uint64_t)dp);
            memcpy(resp + rpos, del_inner, dp); rpos += dp;
            free(del_inner);

        } else if (op_tag == 0x0a) {
            /* RequestRange — query MVCC store and return actual results */
            uint64_t rlen_val = 0;
            if (read_varint(od, ol, &op_pos, &rlen_val) != 0) continue;
            const uint8_t *rd = od + op_pos;
            size_t rl = (size_t)rlen_val;
            op_pos += (size_t)rlen_val;

            /* Parse RequestRange: key (0x0a), range_end (0x12), limit (0x18),
             *   revision (0x20), sort_order (0x28), sort_target (0x30),
             *   serializable (0x38), keys_only (0x40), count_only (0x48),
             *   min_mod_rev (0x50), max_mod_rev (0x58),
             *   min_create_rev (0x60), max_create_rev (0x68) */
            uint8_t *rkey = NULL; size_t rkey_len = 0;
            uint8_t *rrange_end = NULL; size_t rrange_end_len = 0;
            int64_t rrev = 0;
            int64_t rlimit = 0;
            int rkeys_only = 0, rcount_only = 0;
            int rsort_order = 0, rsort_target = 0;
            int64_t rmin_mod_rev = 0, rmax_mod_rev = 0;
            int64_t rmin_create_rev = 0, rmax_create_rev = 0;
            size_t rp_pos = 0;
            while (rp_pos < rl) {
                uint8_t rtag = rd[rp_pos++];
                if (rtag == 0x0a) {
                    read_bytes(rd, rl, &rp_pos, &rkey, &rkey_len);
                } else if (rtag == 0x12) {
                    read_bytes(rd, rl, &rp_pos, &rrange_end, &rrange_end_len);
                } else if (rtag == 0x18) {
                    uint64_t v = 0; read_varint(rd, rl, &rp_pos, &v); rlimit = (int64_t)v;
                } else if (rtag == 0x20) {
                    uint64_t v = 0; read_varint(rd, rl, &rp_pos, &v); rrev = (int64_t)v;
                } else if (rtag == 0x28) {
                    uint64_t v = 0; read_varint(rd, rl, &rp_pos, &v); rsort_order = (int)v;
                } else if (rtag == 0x30) {
                    uint64_t v = 0; read_varint(rd, rl, &rp_pos, &v); rsort_target = (int)v;
                } else if (rtag == 0x38) {
                    uint64_t v = 0; read_varint(rd, rl, &rp_pos, &v); /* serializable, no-op */
                } else if (rtag == 0x40) {
                    uint64_t v = 0; read_varint(rd, rl, &rp_pos, &v); rkeys_only = (int)v;
                } else if (rtag == 0x48) {
                    uint64_t v = 0; read_varint(rd, rl, &rp_pos, &v); rcount_only = (int)v;
                } else if (rtag == 0x50) {
                    uint64_t v = 0; read_varint(rd, rl, &rp_pos, &v); rmin_mod_rev = (int64_t)v;
                } else if (rtag == 0x58) {
                    uint64_t v = 0; read_varint(rd, rl, &rp_pos, &v); rmax_mod_rev = (int64_t)v;
                } else if (rtag == 0x60) {
                    uint64_t v = 0; read_varint(rd, rl, &rp_pos, &v); rmin_create_rev = (int64_t)v;
                } else if (rtag == 0x68) {
                    uint64_t v = 0; read_varint(rd, rl, &rp_pos, &v); rmax_create_rev = (int64_t)v;
                } else {
                    uint64_t skip = 0; read_varint(rd, rl, &rp_pos, &skip);
                }
            }

            /* Query MVCC store */
            size_t rng_count = 0;
            bool rng_more = false;
            uint8_t *kvs_buf = NULL; size_t kvs_len = 0; size_t kvs_cap = 0;

            if (rkey && g_rpc_store) {
                if (!rrange_end || rrange_end_len == 0) {
                    /* Point get */
                    cetcd_kv out_kv;
                    memset(&out_kv, 0, sizeof(out_kv));
                    int get_rc = cetcd_mvcc_get(g_rpc_store, rrev, rkey, rkey_len, &out_kv);
                    if (get_rc == CETCD_ERR_RANGE) {
                        free((void*)out_kv.key.data);
                        free((void*)out_kv.value.data);
                        if (rkey) free(rkey);
                        if (rrange_end) free(rrange_end);
                        free(resp);
                        goto txn_cleanup;
                    }
                    if (get_rc == 0) {
                        rng_count = 1;
                        if (!rcount_only) {
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
                            if (!rkeys_only && out_kv.value.len > 0) {
                                kv_enc[kp++] = 0x2a;
                                write_varint_local(kv_enc, sizeof(kv_enc), &kp, out_kv.value.len);
                                memcpy(kv_enc + kp, out_kv.value.data, out_kv.value.len); kp += out_kv.value.len;
                            }
                            append_kv_lease_(kv_enc, sizeof(kv_enc), &kp, out_kv.lease_id);
                            kvs_cap = kp + 10;
                            kvs_buf = (uint8_t *)malloc(kvs_cap);
                            if (kvs_buf) {
                                kvs_buf[kvs_len++] = 0x12; /* field 2 = kvs */
                                write_varint_local(kvs_buf, kvs_cap, &kvs_len, (uint64_t)kp);
                                memcpy(kvs_buf + kvs_len, kv_enc, kp); kvs_len += kp;
                            }
                        }
                        free((void*)out_kv.key.data);
                        free((void*)out_kv.value.data);
                    }
                } else {
                    /* Range query */
                    cetcd_kv *kvs = NULL; size_t n = 0;
                    int range_rc = cetcd_mvcc_range(g_rpc_store, rrev, rkey, rkey_len,
                                     rrange_end, rrange_end_len, &kvs, &n);
                    if (range_rc == CETCD_ERR_RANGE) {
                        if (rkey) free(rkey);
                        if (rrange_end) free(rrange_end);
                        free(resp);
                        goto txn_cleanup;
                    }
                    /* Apply min/max revision filters (fields 10-13) */
                    if (n > 0 && (rmin_mod_rev > 0 || rmax_mod_rev > 0 || rmin_create_rev > 0 || rmax_create_rev > 0)) {
                        size_t w = 0;
                        for (size_t i = 0; i < n; i++) {
                            int keep = 1;
                            if (rmin_mod_rev > 0 && kvs[i].mod_rev.main < rmin_mod_rev) keep = 0;
                            if (keep && rmax_mod_rev > 0 && kvs[i].mod_rev.main > rmax_mod_rev) keep = 0;
                            if (keep && rmin_create_rev > 0 && kvs[i].create_rev.main < rmin_create_rev) keep = 0;
                            if (keep && rmax_create_rev > 0 && kvs[i].create_rev.main > rmax_create_rev) keep = 0;
                            if (keep) {
                                if (w != i) kvs[w] = kvs[i];
                                w++;
                            } else {
                                free((void*)kvs[i].key.data);
                                free((void*)kvs[i].value.data);
                            }
                        }
                        n = w;
                    }
                    /* Sort results if sort_order is specified (1=ASCEND, 2=DESCEND) */
                    if (rsort_order > 0 && n > 1) {
                        for (size_t i = 1; i < n; i++) {
                            cetcd_kv tmp = kvs[i];
                            size_t j = i;
                            while (j > 0) {
                                int cmp = range_sort_cmp(&kvs[j - 1], &tmp, &rsort_target);
                                if (rsort_order == 2) cmp = -cmp;
                                if (cmp <= 0) break;
                                kvs[j] = kvs[j - 1];
                                j--;
                            }
                            kvs[j] = tmp;
                        }
                    }
                    rng_count = n;
                    /* Apply limit */
                    size_t eff_n = n;
                    if (rlimit > 0 && n > (size_t)rlimit) {
                        eff_n = (size_t)rlimit;
                        rng_more = true;
                    }
                    if (!rcount_only) {
                        kvs_cap = eff_n * 256 + 64;
                        kvs_buf = (uint8_t *)malloc(kvs_cap);
                        if (kvs_buf) {
                            for (size_t i = 0; i < eff_n; i++) {
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
                                if (!rkeys_only && kvs[i].value.len > 0) {
                                    kv_enc[kp++] = 0x2a;
                                    write_varint_local(kv_enc, sizeof(kv_enc), &kp, kvs[i].value.len);
                                    memcpy(kv_enc + kp, kvs[i].value.data, kvs[i].value.len); kp += kvs[i].value.len;
                                }
                                append_kv_lease_(kv_enc, sizeof(kv_enc), &kp, kvs[i].lease_id);
                                size_t needed = kvs_len + 1 + 5 + kp;
                                if (needed > kvs_cap) {
                                    kvs_cap = needed * 2;
                                    uint8_t *tmp = (uint8_t *)realloc(kvs_buf, kvs_cap);
                                    if (!tmp) { free(kvs_buf); kvs_buf = NULL; kvs_len = 0; break; }
                                    kvs_buf = tmp;
                                }
                                kvs_buf[kvs_len++] = 0x12; /* field 2 = kvs */
                                write_varint_local(kvs_buf, kvs_cap, &kvs_len, (uint64_t)kp);
                                memcpy(kvs_buf + kvs_len, kv_enc, kp); kvs_len += kp;
                            }
                        }
                    }
                    if (kvs) cetcd_kv_free_contents(kvs, n);
                }
            }

            /* Build ResponseRange inner: header + kvs + more + count */
            size_t rng_inner_cap = 64 + (kvs_len > 0 ? kvs_len : 0);
            uint8_t *rng_inner = (uint8_t *)malloc(rng_inner_cap);
            size_t rp = 0;
            if (rng_inner) {
                /* field 1 = header (ResponseHeader) */
                uint8_t rng_hdr[16]; size_t rhip = 0;
                rng_hdr[rhip++] = 0x18; /* field 3 = revision */
                rhip = write_varint_local(rng_hdr, sizeof(rng_hdr), &rhip,
                                          (uint64_t)(final_rev > 0 ? final_rev : 1));
                rng_inner[rp++] = 0x0a; /* field 1 = header */
                write_varint_local(rng_inner, rng_inner_cap, &rp, (uint64_t)rhip);
                memcpy(rng_inner + rp, rng_hdr, rhip); rp += rhip;
                /* field 2 = kvs */
                if (kvs_len > 0 && kvs_buf) {
                    if (rp + kvs_len > rng_inner_cap) {
                        rng_inner_cap = rp + kvs_len + 32;
                        uint8_t *t = realloc(rng_inner, rng_inner_cap);
                        if (!t) { free(rng_inner); rng_inner = NULL; rp = 0; }
                        else { rng_inner = t; }
                    }
                    if (rng_inner) {
                        memcpy(rng_inner + rp, kvs_buf, kvs_len);
                        rp += kvs_len;
                    }
                }
                /* field 3 = more */
                if (rng_inner && rng_more) {
                    rng_inner[rp++] = 0x18;
                    rng_inner[rp++] = 0x01;
                }
                /* field 4 = count */
                if (rng_inner) {
                    rng_inner[rp++] = 0x20;
                    write_varint_local(rng_inner, rng_inner_cap, &rp, (uint64_t)rng_count);
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
        free(compares[i].range_end);
    }
    return (cetcd_rpc_bytes){resp, rpos};

txn_cleanup:
    for (size_t i = 0; i < n_compares; i++) {
        free(compares[i].key);
        free(compares[i].value);
        free(compares[i].range_end);
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
        if (cetcd_mvcc_compact(g_rpc_store, compact_rev) == CETCD_OK)
            cetcd_v3rpc_watch_cancel_compacted(compact_rev);
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
