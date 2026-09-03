#include "cetcd/v3rpc.h"
#include "cetcd/mvcc.h"
#include "cetcd/lease.h"
#include "cetcd/raft.h"

#include <stdlib.h>
#include <string.h>
#include <stddef.h>
#include <stdint.h>

extern cetcd_mvcc_store *g_rpc_store;
extern cetcd_lease_mgr  *g_rpc_lease_mgr;
extern cetcd_raft       *g_rpc_raft;

static cetcd_ready_flush_fn g_ready_flush_fn = NULL;
static void                *g_ready_flush_ctx = NULL;

void cetcd_v3rpc_set_ready_flush(cetcd_ready_flush_fn fn, void *ctx) {
    g_ready_flush_fn = fn;
    g_ready_flush_ctx = ctx;
}

static int write_varint_(uint8_t *buf, size_t cap, size_t *pos, uint64_t val) {
    while (*pos < cap) {
        uint8_t b = (uint8_t)(val & 0x7F);
        val >>= 7;
        if (val) b |= 0x80;
        buf[(*pos)++] = b;
        if (!val) return 0;
    }
    return -1;
}

static int read_varint_(const uint8_t *buf, size_t len, size_t *pos, uint64_t *out) {
    uint64_t val = 0;
    int shift = 0;
    while (*pos < len) {
        uint8_t b = buf[(*pos)++];
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

static int encode_tagged_(uint8_t tag,
                          const uint8_t *a, size_t alen,
                          const uint8_t *b, size_t blen,
                          uint64_t extra,
                          int has_extra,
                          uint8_t **out, size_t *out_len) {
    if (!out || !out_len) return -1;
    if (alen > SIZE_MAX / 2 || blen > SIZE_MAX / 2) return -1;
    size_t cap = 1 + 10 + alen + 10 + blen + 16;
    uint8_t *buf = (uint8_t *)malloc(cap);
    if (!buf) return -1;
    size_t pos = 0;
    buf[pos++] = tag;
    if (write_varint_(buf, cap, &pos, (uint64_t)alen) != 0) { free(buf); return -1; }
    if (alen) {
        memcpy(buf + pos, a, alen);
        pos += alen;
    }
    if (write_varint_(buf, cap, &pos, (uint64_t)blen) != 0) { free(buf); return -1; }
    if (blen && b) {
        memcpy(buf + pos, b, blen);
        pos += blen;
    }
    if (has_extra) {
        if (write_varint_(buf, cap, &pos, extra) != 0) { free(buf); return -1; }
    }
    *out = buf;
    *out_len = pos;
    return 0;
}

int cetcd_apply_encode_put(uint8_t **out, size_t *out_len,
                           const uint8_t *key, size_t key_len,
                           const uint8_t *val, size_t val_len,
                           int64_t lease_id) {
    if (!key || key_len == 0) return -1;
    return encode_tagged_(CETCD_APPLY_PUT, key, key_len,
                          val, val_len, (uint64_t)lease_id, 1, out, out_len);
}

int cetcd_apply_encode_delete(uint8_t **out, size_t *out_len,
                              const uint8_t *key, size_t key_len) {
    if (!key || key_len == 0) return -1;
    return encode_tagged_(CETCD_APPLY_DELETE, key, key_len,
                          NULL, 0, 0, 0, out, out_len);
}

int cetcd_apply_encode_delete_range(uint8_t **out, size_t *out_len,
                                    const uint8_t *key, size_t key_len,
                                    const uint8_t *range_end, size_t end_len) {
    if (!key || key_len == 0) return -1;
    return encode_tagged_(CETCD_APPLY_DELETE_RANGE, key, key_len,
                          range_end, end_len, 0, 0, out, out_len);
}

int cetcd_apply_encode_batch(uint8_t **out, size_t *out_len,
                             const uint8_t *const *ops,
                             const size_t *op_lens, size_t n) {
    if (!out || !out_len) return -1;
    if (n == 0 || n > 128) return -1;
    if (n == 1) {
        if (!ops || !ops[0] || !op_lens) return -1;
        uint8_t *buf = (uint8_t *)malloc(op_lens[0]);
        if (!buf) return -1;
        memcpy(buf, ops[0], op_lens[0]);
        *out = buf;
        *out_len = op_lens[0];
        return 0;
    }
    size_t cap = 1 + 10;
    for (size_t i = 0; i < n; i++) {
        if (!ops || !ops[i] || !op_lens) return -1;
        cap += 10 + op_lens[i];
    }
    uint8_t *buf = (uint8_t *)malloc(cap);
    if (!buf) return -1;
    size_t pos = 0;
    buf[pos++] = CETCD_APPLY_BATCH;
    if (write_varint_(buf, cap, &pos, (uint64_t)n) != 0) { free(buf); return -1; }
    for (size_t i = 0; i < n; i++) {
        if (write_varint_(buf, cap, &pos, (uint64_t)op_lens[i]) != 0) { free(buf); return -1; }
        if (pos + op_lens[i] > cap) { free(buf); return -1; }
        memcpy(buf + pos, ops[i], op_lens[i]);
        pos += op_lens[i];
    }
    *out = buf;
    *out_len = pos;
    return 0;
}

static void lease_after_put_(const uint8_t *key, size_t key_len,
                             int64_t old_lease, int64_t new_lease) {
    if (!g_rpc_lease_mgr) return;
    if (old_lease > 0 && old_lease != new_lease)
        cetcd_lease_detach_key(g_rpc_lease_mgr, (cetcd_lease_id)old_lease, key, key_len);
    if (new_lease > 0)
        cetcd_lease_attach_key(g_rpc_lease_mgr, (cetcd_lease_id)new_lease, key, key_len);
}

static int apply_put_(const uint8_t *key, size_t klen,
                      const uint8_t *val, size_t vlen, int64_t lease_id) {
    if (!g_rpc_store || !key || klen == 0) return -1;
    int64_t old_lease = 0;
    if (g_rpc_lease_mgr) {
        cetcd_kv kv;
        memset(&kv, 0, sizeof(kv));
        if (cetcd_mvcc_get(g_rpc_store, 0, key, klen, &kv) == 0) {
            old_lease = kv.lease_id;
            free((void *)(uintptr_t)kv.key.data);
            free((void *)(uintptr_t)kv.value.data);
        }
    }
    cetcd_revision r = cetcd_mvcc_put(g_rpc_store, key, klen,
                                       val ? val : (const uint8_t *)"", vlen,
                                       lease_id);
    if (r.main <= 0) return -1;
    lease_after_put_(key, klen, old_lease, lease_id);
    return 0;
}

static int apply_delete_one_(const uint8_t *key, size_t klen) {
    if (!g_rpc_store || !key || klen == 0) return -1;
    int64_t lease_id = 0;
    if (g_rpc_lease_mgr) {
        cetcd_kv kv;
        memset(&kv, 0, sizeof(kv));
        if (cetcd_mvcc_get(g_rpc_store, 0, key, klen, &kv) == 0) {
            lease_id = kv.lease_id;
            free((void *)(uintptr_t)kv.key.data);
            free((void *)(uintptr_t)kv.value.data);
        }
    }
    cetcd_revision r = cetcd_mvcc_delete(g_rpc_store, key, klen);
    if (r.main > 0 && g_rpc_lease_mgr && lease_id > 0)
        cetcd_lease_detach_key(g_rpc_lease_mgr, (cetcd_lease_id)lease_id, key, klen);
    return 0;
}

static int apply_delete_range_(const uint8_t *key, size_t klen,
                               const uint8_t *end, size_t elen) {
    if (!g_rpc_store || !key || klen == 0) return -1;
    if (!end || elen == 0)
        return apply_delete_one_(key, klen);

    cetcd_kv *kvs = NULL;
    size_t n = 0;
    cetcd_mvcc_range(g_rpc_store, 0, key, klen, end, elen, &kvs, &n);
    if (n == 0) {
        if (kvs) cetcd_kv_free_contents(kvs, n);
        return 0;
    }
    const uint8_t **keys = (const uint8_t **)calloc(n, sizeof(*keys));
    size_t *lens = (size_t *)calloc(n, sizeof(*lens));
    if (!keys || !lens) {
        free(keys);
        free(lens);
        for (size_t i = 0; i < n; i++)
            apply_delete_one_(kvs[i].key.data, kvs[i].key.len);
        cetcd_kv_free_contents(kvs, n);
        return 0;
    }
    for (size_t i = 0; i < n; i++) {
        keys[i] = kvs[i].key.data;
        lens[i] = kvs[i].key.len;
    }
    cetcd_revision r = cetcd_mvcc_delete_keys(g_rpc_store, keys, lens, n);
    if (r.main > 0 && g_rpc_lease_mgr) {
        for (size_t i = 0; i < n; i++) {
            if (kvs[i].lease_id > 0)
                cetcd_lease_detach_key(g_rpc_lease_mgr,
                                        (cetcd_lease_id)kvs[i].lease_id,
                                        kvs[i].key.data, kvs[i].key.len);
        }
    }
    free(keys);
    free(lens);
    cetcd_kv_free_contents(kvs, n);
    return 0;
}

int cetcd_v3rpc_apply_entry(const uint8_t *data, size_t len) {
    if (!data || len < 2) return -1;
    uint8_t op = data[0];
    size_t pos = 1;

    if (op == CETCD_APPLY_BATCH) {
        uint64_t n = 0;
        if (read_varint_(data, len, &pos, &n) != 0) return -1;
        if (n == 0 || n > 128) return -1;
        for (uint64_t i = 0; i < n; i++) {
            uint64_t ilen = 0;
            if (read_varint_(data, len, &pos, &ilen) != 0) return -1;
            if (ilen == 0 || ilen > len - pos) return -1;
            if (data[pos] == CETCD_APPLY_BATCH) return -1; /* no nesting */
            if (cetcd_v3rpc_apply_entry(data + pos, (size_t)ilen) != 0) return -1;
            pos += (size_t)ilen;
        }
        return 0;
    }

    uint64_t klen = 0;
    if (read_varint_(data, len, &pos, &klen) != 0) return -1;
    if (klen == 0 || klen > len - pos) return -1;
    const uint8_t *key = data + pos;
    pos += (size_t)klen;

    if (op == CETCD_APPLY_PUT) {
        uint64_t vlen = 0;
        if (read_varint_(data, len, &pos, &vlen) != 0) return -1;
        if (vlen > len - pos) return -1;
        const uint8_t *val = data + pos;
        pos += (size_t)vlen;
        int64_t lease = 0;
        if (pos < len) {
            uint64_t lv = 0;
            if (read_varint_(data, len, &pos, &lv) != 0) return -1;
            lease = (int64_t)lv;
        }
        return apply_put_(key, (size_t)klen, val, (size_t)vlen, lease);
    }
    if (op == CETCD_APPLY_DELETE) {
        return apply_delete_one_(key, (size_t)klen);
    }
    if (op == CETCD_APPLY_DELETE_RANGE) {
        uint64_t elen = 0;
        if (read_varint_(data, len, &pos, &elen) != 0) return -1;
        if (elen > len - pos) return -1;
        const uint8_t *end = elen ? data + pos : NULL;
        return apply_delete_range_(key, (size_t)klen, end, (size_t)elen);
    }
    return -1;
}

int cetcd_v3rpc_propose_or_apply(const uint8_t *data, size_t len) {
    if (!data || len == 0) return -1;
    if (!g_rpc_raft)
        return cetcd_v3rpc_apply_entry(data, len) == 0 ? 1 : -1;
    if (cetcd_raft_state(g_rpc_raft) != CETCD_NODE_LEADER) return -1;
    if (cetcd_raft_propose(g_rpc_raft, data, len) != 0) return -1;
    if (g_ready_flush_fn) g_ready_flush_fn(g_ready_flush_ctx);
    if (cetcd_raft_applied(g_rpc_raft) < cetcd_raft_last_index(g_rpc_raft))
        return -1;
    return 0;
}
