#include "cetcd/v3rpc.h"
#include "cetcd/mvcc.h"
#include "cetcd/lease.h"
#include "cetcd/raft.h"
#include "cetcd/peer.h"
#include "cetcd/auth.h"

#include <stdlib.h>
#include <string.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>

#include "cetcd/backend.h"

extern cetcd_mvcc_store *g_rpc_store;
extern cetcd_lease_mgr  *g_rpc_lease_mgr;
extern cetcd_raft       *g_rpc_raft;
extern cetcd_cluster    *g_rpc_cluster;
extern cetcd_backend    *g_rpc_auth_backend;
extern cetcd_auth_store *g_rpc_auth;
extern uint64_t          g_rpc_quota_bytes;
extern uint64_t          g_rpc_node_id;

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

int cetcd_apply_encode_member_add(uint8_t **out, size_t *out_len,
                                  uint64_t id, int is_learner,
                                  const char *addr, uint16_t port) {
    if (!out || !out_len || id == 0) return -1;
    size_t alen = addr ? strlen(addr) : 0;
    size_t cap = 1 + 10 + 10 + 10 + alen + 10;
    uint8_t *buf = (uint8_t *)malloc(cap);
    if (!buf) return -1;
    size_t pos = 0;
    buf[pos++] = CETCD_APPLY_MEMBER_ADD;
    if (write_varint_(buf, cap, &pos, id) != 0) { free(buf); return -1; }
    if (write_varint_(buf, cap, &pos, is_learner ? 1 : 0) != 0) { free(buf); return -1; }
    if (write_varint_(buf, cap, &pos, (uint64_t)alen) != 0) { free(buf); return -1; }
    if (alen) {
        memcpy(buf + pos, addr, alen);
        pos += alen;
    }
    if (write_varint_(buf, cap, &pos, (uint64_t)port) != 0) { free(buf); return -1; }
    *out = buf;
    *out_len = pos;
    return 0;
}

int cetcd_apply_encode_member_remove(uint8_t **out, size_t *out_len, uint64_t id) {
    if (!out || !out_len || id == 0) return -1;
    uint8_t *buf = (uint8_t *)malloc(16);
    if (!buf) return -1;
    size_t pos = 0;
    buf[pos++] = CETCD_APPLY_MEMBER_REMOVE;
    if (write_varint_(buf, 16, &pos, id) != 0) { free(buf); return -1; }
    *out = buf;
    *out_len = pos;
    return 0;
}

int cetcd_apply_encode_member_promote(uint8_t **out, size_t *out_len, uint64_t id) {
    if (!out || !out_len || id == 0) return -1;
    uint8_t *buf = (uint8_t *)malloc(16);
    if (!buf) return -1;
    size_t pos = 0;
    buf[pos++] = CETCD_APPLY_MEMBER_PROMOTE;
    if (write_varint_(buf, 16, &pos, id) != 0) { free(buf); return -1; }
    *out = buf;
    *out_len = pos;
    return 0;
}

int cetcd_apply_encode_member_update(uint8_t **out, size_t *out_len,
                                     uint64_t id, const char *addr, uint16_t port) {
    if (!out || !out_len || id == 0) return -1;
    size_t alen = addr ? strlen(addr) : 0;
    size_t cap = 1 + 10 + 10 + alen + 10;
    uint8_t *buf = (uint8_t *)malloc(cap);
    if (!buf) return -1;
    size_t pos = 0;
    buf[pos++] = CETCD_APPLY_MEMBER_UPDATE;
    if (write_varint_(buf, cap, &pos, id) != 0) { free(buf); return -1; }
    if (write_varint_(buf, cap, &pos, (uint64_t)alen) != 0) { free(buf); return -1; }
    if (alen) {
        memcpy(buf + pos, addr, alen);
        pos += alen;
    }
    if (write_varint_(buf, cap, &pos, (uint64_t)port) != 0) { free(buf); return -1; }
    *out = buf;
    *out_len = pos;
    return 0;
}

int cetcd_apply_encode_lease_revoke(uint8_t **out, size_t *out_len, uint64_t lease_id) {
    if (!out || !out_len || lease_id == 0) return -1;
    uint8_t *buf = (uint8_t *)malloc(16);
    if (!buf) return -1;
    size_t pos = 0;
    buf[pos++] = CETCD_APPLY_LEASE_REVOKE;
    if (write_varint_(buf, 16, &pos, lease_id) != 0) { free(buf); return -1; }
    *out = buf;
    *out_len = pos;
    return 0;
}

int cetcd_apply_encode_compact(uint8_t **out, size_t *out_len, int64_t revision) {
    if (!out || !out_len || revision <= 0) return -1;
    uint8_t *buf = (uint8_t *)malloc(16);
    if (!buf) return -1;
    size_t pos = 0;
    buf[pos++] = CETCD_APPLY_COMPACT;
    if (write_varint_(buf, 16, &pos, (uint64_t)revision) != 0) { free(buf); return -1; }
    *out = buf;
    *out_len = pos;
    return 0;
}

int cetcd_apply_encode_lease_grant(uint8_t **out, size_t *out_len,
                                   uint64_t lease_id, int64_t ttl) {
    if (!out || !out_len || lease_id == 0 || ttl <= 0 || ttl > CETCD_MAX_LEASE_TTL)
        return -1;
    uint8_t *buf = (uint8_t *)malloc(24);
    if (!buf) return -1;
    size_t pos = 0;
    buf[pos++] = CETCD_APPLY_LEASE_GRANT;
    if (write_varint_(buf, 24, &pos, lease_id) != 0) { free(buf); return -1; }
    if (write_varint_(buf, 24, &pos, (uint64_t)ttl) != 0) { free(buf); return -1; }
    *out = buf;
    *out_len = pos;
    return 0;
}

int cetcd_apply_encode_lease_keepalive(uint8_t **out, size_t *out_len,
                                       uint64_t lease_id, int64_t ttl) {
    if (!out || !out_len || lease_id == 0 || ttl <= 0 || ttl > CETCD_MAX_LEASE_TTL)
        return -1;
    uint8_t *buf = (uint8_t *)malloc(24);
    if (!buf) return -1;
    size_t pos = 0;
    buf[pos++] = CETCD_APPLY_LEASE_KEEPALIVE;
    if (write_varint_(buf, 24, &pos, lease_id) != 0) { free(buf); return -1; }
    if (write_varint_(buf, 24, &pos, (uint64_t)ttl) != 0) { free(buf); return -1; }
    *out = buf;
    *out_len = pos;
    return 0;
}

int cetcd_apply_encode_auth_user_add(uint8_t **out, size_t *out_len,
                                     const uint8_t *name, size_t name_len,
                                     const uint8_t *hash, size_t hash_len) {
    if (!name || name_len == 0 || !hash || hash_len == 0) return -1;
    return encode_tagged_(CETCD_APPLY_AUTH_USER_ADD, name, name_len,
                          hash, hash_len, 0, 0, out, out_len);
}

int cetcd_apply_encode_auth_enabled(uint8_t **out, size_t *out_len, int enabled) {
    if (!out || !out_len || (enabled != 0 && enabled != 1)) return -1;
    uint8_t *buf = (uint8_t *)malloc(8);
    if (!buf) return -1;
    size_t pos = 0;
    buf[pos++] = CETCD_APPLY_AUTH_ENABLED;
    if (write_varint_(buf, 8, &pos, (uint64_t)enabled) != 0) { free(buf); return -1; }
    *out = buf;
    *out_len = pos;
    return 0;
}

int cetcd_apply_encode_auth_user_delete(uint8_t **out, size_t *out_len,
                                        const uint8_t *name, size_t name_len) {
    if (!name || name_len == 0) return -1;
    return encode_tagged_(CETCD_APPLY_AUTH_USER_DELETE, name, name_len,
                          NULL, 0, 0, 0, out, out_len);
}

int cetcd_apply_encode_auth_role_add(uint8_t **out, size_t *out_len,
                                     const uint8_t *name, size_t name_len) {
    if (!name || name_len == 0) return -1;
    return encode_tagged_(CETCD_APPLY_AUTH_ROLE_ADD, name, name_len,
                          NULL, 0, 0, 0, out, out_len);
}

int cetcd_apply_encode_auth_role_delete(uint8_t **out, size_t *out_len,
                                        const uint8_t *name, size_t name_len) {
    if (!name || name_len == 0) return -1;
    return encode_tagged_(CETCD_APPLY_AUTH_ROLE_DELETE, name, name_len,
                          NULL, 0, 0, 0, out, out_len);
}

int cetcd_apply_encode_auth_user_grant_role(uint8_t **out, size_t *out_len,
                                            const uint8_t *user, size_t user_len,
                                            const uint8_t *role, size_t role_len) {
    if (!user || user_len == 0 || !role || role_len == 0) return -1;
    return encode_tagged_(CETCD_APPLY_AUTH_USER_GRANT_ROLE, user, user_len,
                          role, role_len, 0, 0, out, out_len);
}

int cetcd_apply_encode_auth_user_revoke_role(uint8_t **out, size_t *out_len,
                                             const uint8_t *user, size_t user_len,
                                             const uint8_t *role, size_t role_len) {
    if (!user || user_len == 0 || !role || role_len == 0) return -1;
    return encode_tagged_(CETCD_APPLY_AUTH_USER_REVOKE_ROLE, user, user_len,
                          role, role_len, 0, 0, out, out_len);
}

int cetcd_apply_encode_auth_role_grant_perm(uint8_t **out, size_t *out_len,
                                            const uint8_t *role, size_t role_len,
                                            const uint8_t *key, size_t key_len,
                                            int perm_type) {
    if (!role || role_len == 0 || perm_type < 0 || perm_type > 2) return -1;
    if (key_len > 0 && !key) return -1;
    return encode_tagged_(CETCD_APPLY_AUTH_ROLE_GRANT_PERM, role, role_len,
                          key, key_len, (uint64_t)perm_type, 1, out, out_len);
}

int cetcd_apply_encode_auth_role_revoke_perm(uint8_t **out, size_t *out_len,
                                             const uint8_t *role, size_t role_len,
                                             const uint8_t *key, size_t key_len) {
    if (!role || role_len == 0) return -1;
    if (key_len > 0 && !key) return -1;
    return encode_tagged_(CETCD_APPLY_AUTH_ROLE_REVOKE_PERM, role, role_len,
                          key, key_len, 0, 0, out, out_len);
}

int cetcd_apply_encode_auth_user_change_pass(uint8_t **out, size_t *out_len,
                                             const uint8_t *name, size_t name_len,
                                             const uint8_t *hash, size_t hash_len) {
    if (!name || name_len == 0 || !hash || hash_len == 0) return -1;
    return encode_tagged_(CETCD_APPLY_AUTH_USER_CHANGE_PASS, name, name_len,
                          hash, hash_len, 0, 0, out, out_len);
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

static int begin_voter_joint_(void) {
    if (!g_rpc_raft) return 0;
    if (cetcd_raft_enter_joint(g_rpc_raft) != 0) return -1;
    if (!g_rpc_cluster) return 0;
    uint64_t ids[16];
    uint32_t n = cetcd_raft_copy_outgoing(g_rpc_raft, ids, 16);
    uint64_t jidx = cetcd_raft_joint_index(g_rpc_raft);
    if (cetcd_cluster_persist_joint(g_rpc_cluster, ids, n, jidx) != CETCD_OK) {
        (void)cetcd_raft_leave_joint(g_rpc_raft);
        return -1;
    }
    return 0;
}

static int apply_member_(uint8_t op, const uint8_t *data, size_t len) {
    if (!g_rpc_cluster) return -1;
    size_t pos = 1;
    uint64_t id = 0;
    if (read_varint_(data, len, &pos, &id) != 0 || id == 0) return -1;

    if (op == CETCD_APPLY_MEMBER_REMOVE) {
        const cetcd_peer_info *cur = cetcd_cluster_get_peer(g_rpc_cluster, id);
        int was_voter = cur && !cur->is_learner;
        if (was_voter && (!g_rpc_raft || !cetcd_raft_in_joint(g_rpc_raft))) {
            if (begin_voter_joint_() != 0) return -1;
        }
        if (cetcd_cluster_persist_del(g_rpc_cluster, id) != CETCD_OK) return -1;
        (void)cetcd_cluster_remove_peer(g_rpc_cluster, id);
        if (g_rpc_raft) (void)cetcd_raft_remove_peer(g_rpc_raft, id);
        return 0;
    }
    if (op == CETCD_APPLY_MEMBER_PROMOTE) {
        const cetcd_peer_info *cur = cetcd_cluster_get_peer(g_rpc_cluster, id);
        if (!cur) return -1;
        cetcd_peer_info next = *cur;
        if (!next.is_learner) return 0; /* idempotent replay */
        if (!g_rpc_raft || !cetcd_raft_in_joint(g_rpc_raft)) {
            if (begin_voter_joint_() != 0) return -1;
        }
        next.is_learner = 0;
        if (cetcd_cluster_persist_peer(g_rpc_cluster, &next) != CETCD_OK) return -1;
        if (cetcd_cluster_promote(g_rpc_cluster, id) != CETCD_OK) return -1;
        if (g_rpc_raft) (void)cetcd_raft_promote(g_rpc_raft, id);
        return 0;
    }

    uint64_t is_learner = 0;
    if (op == CETCD_APPLY_MEMBER_ADD) {
        if (read_varint_(data, len, &pos, &is_learner) != 0) return -1;
    }
    uint64_t alen = 0;
    if (read_varint_(data, len, &pos, &alen) != 0) return -1;
    if (alen > len - pos) return -1;
    char addr[256];
    memset(addr, 0, sizeof(addr));
    if (alen >= sizeof(addr)) alen = sizeof(addr) - 1;
    if (alen) memcpy(addr, data + pos, (size_t)alen);
    pos += (size_t)alen;
    uint64_t port = 0;
    if (read_varint_(data, len, &pos, &port) != 0) return -1;

    cetcd_peer_info info;
    memset(&info, 0, sizeof(info));
    info.id = id;
    info.is_learner = is_learner ? 1 : 0;
    info.port = (uint16_t)port;
    snprintf(info.addr, sizeof(info.addr), "%s", addr);

    if (op == CETCD_APPLY_MEMBER_UPDATE) {
        const cetcd_peer_info *cur = cetcd_cluster_get_peer(g_rpc_cluster, id);
        if (cur) info.is_learner = cur->is_learner;
        if (cetcd_cluster_persist_peer(g_rpc_cluster, &info) != CETCD_OK) return -1;
        return cetcd_cluster_update_peer(g_rpc_cluster, id, &info) == CETCD_OK ? 0 : -1;
    }

    if (!info.is_learner && (!g_rpc_raft || !cetcd_raft_in_joint(g_rpc_raft))) {
        if (begin_voter_joint_() != 0) return -1;
    }
    if (cetcd_cluster_persist_peer(g_rpc_cluster, &info) != CETCD_OK) return -1;
    int rc = cetcd_cluster_add_peer(g_rpc_cluster, &info);
    if (rc != CETCD_OK && rc != CETCD_ERR_EXISTS) return -1;
    if (g_rpc_raft)
        (void)cetcd_raft_add_peer(g_rpc_raft, info.id, info.is_learner);
    return 0;
}

static int apply_lease_revoke_(uint64_t id) {
    if (id == 0) return -1;
    if (g_rpc_lease_mgr) {
        const uint8_t *const *keys = NULL;
        const size_t *lens = NULL;
        size_t n = cetcd_lease_keys(g_rpc_lease_mgr, (cetcd_lease_id)id, &keys, &lens);
        if (g_rpc_store && keys && lens && n > 0)
            (void)cetcd_mvcc_delete_keys(g_rpc_store, keys, lens, n);
        (void)cetcd_lease_revoke(g_rpc_lease_mgr, (cetcd_lease_id)id);
    }
    return 0;
}

int cetcd_v3rpc_apply_entry(const uint8_t *data, size_t len) {
    if (!data || len < 1) return -1;
    uint8_t op = data[0];
    size_t pos = 1;

    if (op == CETCD_APPLY_LEAVE_JOINT) {
        if (g_rpc_raft) (void)cetcd_raft_leave_joint(g_rpc_raft);
        if (g_rpc_cluster) (void)cetcd_cluster_persist_clear_joint(g_rpc_cluster);
        return 0;
    }

    if (op == CETCD_APPLY_LEASE_REVOKE) {
        if (len < 2) return -1;
        uint64_t id = 0;
        if (read_varint_(data, len, &pos, &id) != 0 || id == 0) return -1;
        return apply_lease_revoke_(id);
    }

    if (op == CETCD_APPLY_LEASE_GRANT) {
        if (len < 3) return -1;
        uint64_t id = 0, ttl = 0;
        if (read_varint_(data, len, &pos, &id) != 0 || id == 0) return -1;
        if (read_varint_(data, len, &pos, &ttl) != 0 || ttl == 0) return -1;
        if ((int64_t)ttl > CETCD_MAX_LEASE_TTL) return -1;
        if (!g_rpc_lease_mgr) return -1;
        if (cetcd_lease_exists(g_rpc_lease_mgr, (cetcd_lease_id)id))
            return 0;
        if (cetcd_lease_grant_id(g_rpc_lease_mgr, (cetcd_lease_id)id,
                                 (int64_t)ttl) == 0)
            return -1;
        return 0;
    }

    if (op == CETCD_APPLY_LEASE_KEEPALIVE) {
        if (len < 3) return -1;
        uint64_t id = 0, ttl = 0;
        if (read_varint_(data, len, &pos, &id) != 0 || id == 0) return -1;
        if (read_varint_(data, len, &pos, &ttl) != 0 || ttl == 0) return -1;
        if ((int64_t)ttl > CETCD_MAX_LEASE_TTL) return -1;
        if (!g_rpc_lease_mgr) return -1;
        if (!cetcd_lease_exists(g_rpc_lease_mgr, (cetcd_lease_id)id))
            return 0;
        if (cetcd_lease_keep_alive(g_rpc_lease_mgr, (cetcd_lease_id)id,
                                   (int64_t)ttl) != CETCD_OK)
            return -1;
        return 0;
    }

    if (op == CETCD_APPLY_COMPACT) {
        if (len < 2) return -1;
        uint64_t rev = 0;
        if (read_varint_(data, len, &pos, &rev) != 0 || rev == 0) return -1;
        if (!g_rpc_store) return -1;
        int rc = cetcd_mvcc_compact(g_rpc_store, (int64_t)rev);
        if (rc == CETCD_OK) {
            cetcd_v3rpc_watch_cancel_compacted((int64_t)rev);
            return 0;
        }
        /* WAL replay after LMDB already compacted this rev. */
        if (rc == CETCD_ERR_RANGE &&
            cetcd_mvcc_compacted_revision(g_rpc_store) >= (int64_t)rev)
            return 0;
        return -1;
    }

    if (op == CETCD_APPLY_AUTH_ENABLED) {
        if (len < 2) return -1;
        uint64_t on = 0;
        if (read_varint_(data, len, &pos, &on) != 0 || on > 1) return -1;
        if (!g_rpc_auth) return -1;
        if (on && !cetcd_auth_has_user(g_rpc_auth, "root")) return -1;
        cetcd_auth_set_enabled(g_rpc_auth, on != 0);
        if (!on) cetcd_auth_revoke_all_tokens(g_rpc_auth);
        cetcd_v3rpc_auth_persist();
        return 0;
    }

    if (op == CETCD_APPLY_MEMBER_ADD || op == CETCD_APPLY_MEMBER_REMOVE
        || op == CETCD_APPLY_MEMBER_PROMOTE || op == CETCD_APPLY_MEMBER_UPDATE) {
        if (len < 2) return -1;
        return apply_member_(op, data, len);
    }

    if (len < 2) return -1;

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
    if (op == CETCD_APPLY_AUTH_USER_ADD) {
        uint64_t hlen = 0;
        if (read_varint_(data, len, &pos, &hlen) != 0) return -1;
        if (hlen == 0 || hlen > len - pos) return -1;
        if (!g_rpc_auth || klen >= 128) return -1;
        char name[128];
        memcpy(name, key, (size_t)klen);
        name[(size_t)klen] = '\0';
        if (cetcd_auth_has_user(g_rpc_auth, name)) {
            cetcd_v3rpc_auth_persist();
            return 0;
        }
        if (cetcd_auth_add_user_hash(g_rpc_auth, name, data + pos,
                                     (size_t)hlen) != CETCD_OK)
            return -1;
        cetcd_v3rpc_auth_persist();
        return 0;
    }
    if (op == CETCD_APPLY_AUTH_USER_DELETE) {
        if (!g_rpc_auth || klen >= 128) return -1;
        char name[128];
        memcpy(name, key, (size_t)klen);
        name[(size_t)klen] = '\0';
        if (!cetcd_auth_has_user(g_rpc_auth, name)) {
            cetcd_v3rpc_auth_persist();
            return 0;
        }
        if (cetcd_auth_remove_user(g_rpc_auth, name) != CETCD_OK)
            return -1;
        cetcd_v3rpc_auth_persist();
        return 0;
    }
    if (op == CETCD_APPLY_AUTH_ROLE_ADD) {
        if (!g_rpc_auth || klen >= 128) return -1;
        char name[128];
        memcpy(name, key, (size_t)klen);
        name[(size_t)klen] = '\0';
        if (cetcd_auth_get_role(g_rpc_auth, name)) {
            cetcd_v3rpc_auth_persist();
            return 0;
        }
        if (cetcd_auth_add_role(g_rpc_auth, name, 1, 1, "/", 1) != CETCD_OK)
            return -1;
        cetcd_v3rpc_auth_persist();
        return 0;
    }
    if (op == CETCD_APPLY_AUTH_ROLE_DELETE) {
        if (!g_rpc_auth || klen >= 128) return -1;
        char name[128];
        memcpy(name, key, (size_t)klen);
        name[(size_t)klen] = '\0';
        if (!cetcd_auth_get_role(g_rpc_auth, name)) {
            cetcd_v3rpc_auth_persist();
            return 0;
        }
        if (cetcd_auth_remove_role(g_rpc_auth, name) != CETCD_OK)
            return -1;
        cetcd_v3rpc_auth_persist();
        return 0;
    }
    if (op == CETCD_APPLY_AUTH_USER_GRANT_ROLE) {
        uint64_t rlen = 0;
        if (read_varint_(data, len, &pos, &rlen) != 0) return -1;
        if (rlen == 0 || rlen > len - pos) return -1;
        if (!g_rpc_auth || klen >= 128 || rlen >= 128) return -1;
        char user[128], role[128];
        memcpy(user, key, (size_t)klen);
        user[(size_t)klen] = '\0';
        memcpy(role, data + pos, (size_t)rlen);
        role[(size_t)rlen] = '\0';
        if (cetcd_auth_grant_role(g_rpc_auth, user, role) != CETCD_OK)
            return -1;
        cetcd_v3rpc_auth_persist();
        return 0;
    }
    if (op == CETCD_APPLY_AUTH_USER_REVOKE_ROLE) {
        uint64_t rlen = 0;
        if (read_varint_(data, len, &pos, &rlen) != 0) return -1;
        if (rlen == 0 || rlen > len - pos) return -1;
        if (!g_rpc_auth || klen >= 128 || rlen >= 128) return -1;
        char user[128], role[128];
        memcpy(user, key, (size_t)klen);
        user[(size_t)klen] = '\0';
        memcpy(role, data + pos, (size_t)rlen);
        role[(size_t)rlen] = '\0';
        int rc = cetcd_auth_revoke_role(g_rpc_auth, user, role);
        if (rc != CETCD_OK && rc != CETCD_ERR_NOTFOUND)
            return -1;
        cetcd_v3rpc_auth_persist();
        return 0;
    }
    if (op == CETCD_APPLY_AUTH_ROLE_GRANT_PERM) {
        uint64_t kplen = 0, ptype = 0;
        if (read_varint_(data, len, &pos, &kplen) != 0) return -1;
        if (kplen > len - pos) return -1;
        const uint8_t *pkey = data + pos;
        pos += (size_t)kplen;
        if (read_varint_(data, len, &pos, &ptype) != 0 || ptype > 2) return -1;
        if (!g_rpc_auth || klen >= 128 || kplen >= 256) return -1;
        char name[128];
        memcpy(name, key, (size_t)klen);
        name[(size_t)klen] = '\0';
        if (!cetcd_auth_get_role(g_rpc_auth, name)) return -1;
        int rd = (ptype == 0 || ptype == 2) ? 1 : 0;
        int wr = (ptype == 1 || ptype == 2) ? 1 : 0;
        if (cetcd_auth_grant_permission(g_rpc_auth, name, rd, wr,
                kplen ? (const char *)pkey : NULL, (size_t)kplen) != CETCD_OK)
            return -1;
        cetcd_v3rpc_auth_persist();
        return 0;
    }
    if (op == CETCD_APPLY_AUTH_ROLE_REVOKE_PERM) {
        uint64_t kplen = 0;
        const uint8_t *pkey = NULL;
        if (pos < len) {
            if (read_varint_(data, len, &pos, &kplen) != 0) return -1;
            if (kplen > len - pos) return -1;
            pkey = kplen ? data + pos : NULL;
            pos += (size_t)kplen;
        }
        if (!g_rpc_auth || klen >= 128 || kplen >= 256) return -1;
        char name[128];
        memcpy(name, key, (size_t)klen);
        name[(size_t)klen] = '\0';
        if (!cetcd_auth_get_role(g_rpc_auth, name)) {
            cetcd_v3rpc_auth_persist();
            return 0;
        }
        int rc = cetcd_auth_revoke_permission_key(g_rpc_auth, name, pkey, (size_t)kplen);
        if (rc != CETCD_OK && rc != CETCD_ERR_NOTFOUND)
            return -1;
        cetcd_v3rpc_auth_persist();
        return 0;
    }
    if (op == CETCD_APPLY_AUTH_USER_CHANGE_PASS) {
        uint64_t hlen = 0;
        if (read_varint_(data, len, &pos, &hlen) != 0) return -1;
        if (hlen == 0 || hlen > len - pos) return -1;
        if (!g_rpc_auth || klen >= 128) return -1;
        char name[128];
        memcpy(name, key, (size_t)klen);
        name[(size_t)klen] = '\0';
        if (!cetcd_auth_has_user(g_rpc_auth, name)) {
            cetcd_v3rpc_auth_persist();
            return 0;
        }
        if (cetcd_auth_set_user_hash(g_rpc_auth, name, data + pos,
                                     (size_t)hlen) != CETCD_OK)
            return -1;
        cetcd_v3rpc_auth_persist();
        return 0;
    }
    return -1;
}

int cetcd_v3rpc_propose_or_apply(const uint8_t *data, size_t len) {
    if (!data || len == 0) return -1;
    if (data[0] == CETCD_APPLY_PUT && g_rpc_quota_bytes && g_rpc_auth_backend) {
        if (cetcd_backend_size(g_rpc_auth_backend) >= g_rpc_quota_bytes) {
            cetcd_v3rpc_alarm_activate(1, g_rpc_node_id ? g_rpc_node_id : 1);
            return -1;
        }
    }
    if (!g_rpc_raft)
        return cetcd_v3rpc_apply_entry(data, len) == 0 ? 1 : -1;
    if (cetcd_raft_state(g_rpc_raft) != CETCD_NODE_LEADER) return -1;
    if (cetcd_raft_propose(g_rpc_raft, data, len) != 0) return -1;
    if (g_ready_flush_fn) g_ready_flush_fn(g_ready_flush_ctx);
    if (cetcd_raft_applied(g_rpc_raft) < cetcd_raft_last_index(g_rpc_raft))
        return -1;
    return 0;
}

int cetcd_v3rpc_propose_deletes(const uint8_t *const *keys,
                                const size_t *key_lens, size_t n) {
    if (n == 0) return 0;
    if (!keys || !key_lens) return -1;
    size_t off = 0;
    while (off < n) {
        size_t chunk = n - off;
        if (chunk > 128) chunk = 128;
        const uint8_t *ops[128];
        size_t op_lens[128];
        uint8_t *owned[128];
        size_t i;
        for (i = 0; i < chunk; i++) {
            owned[i] = NULL;
            if (cetcd_apply_encode_delete(&owned[i], &op_lens[i],
                    keys[off + i], key_lens[off + i]) != 0) {
                while (i > 0) { i--; free(owned[i]); }
                return -1;
            }
            ops[i] = owned[i];
        }
        uint8_t *batch = NULL;
        size_t blen = 0;
        int enc = cetcd_apply_encode_batch(&batch, &blen, ops, op_lens, chunk);
        for (i = 0; i < chunk; i++) free(owned[i]);
        if (enc != 0) return -1;
        int rc = cetcd_v3rpc_propose_or_apply(batch, blen);
        free(batch);
        if (rc < 0) return -1;
        off += chunk;
    }
    return 0;
}
