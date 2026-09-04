#include "cetcd/lease.h"
#include "cetcd/base.h"
#include "cetcd/backend.h"
#include "cetcd/clock.h"
#include "cetcd/mvcc.h"

#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#define LEASE_BUCKET "lease"
static const uint8_t LEASE_KEY_NEXT[] = { 'n', 'e', 'x', 't' };

/* Internal lease structure matching the header contract. */
struct cetcd_lease {
    cetcd_lease_id id;
    int64_t        ttl_seconds;
    int64_t        deadline_ms; /* absolute expiry time in ms */

    /* Attached keys */
    uint8_t      **keys;      /* array of key data pointers */
    size_t        *key_lens;  /* array of key lengths */
    size_t         key_count;
    size_t         key_cap;
};

/* Lease manager implementation */
struct cetcd_lease_mgr {
    cetcd_lease   *leases;      /* dynamic array */
    size_t          count;
    size_t          cap;
    cetcd_lease_id  next_id;     /* starts at 1, increments */
    int64_t         now_ms;      /* accumulated time (ms) */
    cetcd_lease_expire_fn on_expire;
    void           *expire_udata;
    struct cetcd_backend *backend;
    int             have_bucket; /* 1 after a non-empty lease-bucket load */
};

/* Helpers */
static int ensure_cap_(cetcd_lease_mgr *mgr) {
    if (mgr->count < mgr->cap) return 0;
    size_t new_cap = (mgr->cap == 0) ? 4 : mgr->cap * 2;
    cetcd_lease *ne = (cetcd_lease *)realloc(mgr->leases, new_cap * sizeof(*ne));
    if (ne == NULL) return CETCD_ERR_NOMEM;
    mgr->leases = ne;
    mgr->cap = new_cap;
    return 0;
}

static cetcd_lease *lease_by_id_(cetcd_lease_mgr *mgr, cetcd_lease_id id) {
    for (size_t i = 0; i < mgr->count; ++i) {
        if (mgr->leases[i].id == id) return &mgr->leases[i];
    }
    return NULL;
}

static void write_le64_(uint8_t *p, uint64_t v) {
    for (int i = 0; i < 8; i++) p[i] = (uint8_t)(v >> (8 * i));
}

static uint64_t read_le64_(const uint8_t *p) {
    uint64_t v = 0;
    for (int i = 0; i < 8; i++) v |= (uint64_t)p[i] << (8 * i);
    return v;
}

static int64_t realtime_ms_(void) {
    return (int64_t)(cetcd_clock_realtime_ns() / 1000000ULL);
}

static int persist_put_(struct cetcd_backend *be, const cetcd_lease *l, int64_t now_ms,
                        cetcd_lease_id next_id) {
    if (!be || !l) return CETCD_ERR_INVAL;
    uint8_t idk[8];
    uint8_t val[16];
    uint8_t nextv[8];
    int64_t rem = l->deadline_ms - now_ms;
    if (rem < 0) rem = 0;
    write_le64_(idk, (uint64_t)l->id);
    write_le64_(val, (uint64_t)l->ttl_seconds);
    write_le64_(val + 8, (uint64_t)(realtime_ms_() + rem));
    write_le64_(nextv, (uint64_t)next_id);
    return cetcd_backend_put2(be, LEASE_BUCKET, idk, sizeof(idk), val, sizeof(val),
                              LEASE_BUCKET, LEASE_KEY_NEXT, sizeof(LEASE_KEY_NEXT),
                              nextv, sizeof(nextv));
}

static int persist_del_(struct cetcd_backend *be, cetcd_lease_id id) {
    if (!be) return CETCD_OK;
    uint8_t idk[8];
    write_le64_(idk, (uint64_t)id);
    int rc = cetcd_backend_del(be, LEASE_BUCKET, idk, sizeof(idk));
    if (rc == CETCD_ERR_NOTFOUND) return CETCD_OK;
    return rc;
}

static int persist_next_(struct cetcd_backend *be, cetcd_lease_id next_id) {
    if (!be) return CETCD_OK;
    uint8_t nextv[8];
    write_le64_(nextv, (uint64_t)next_id);
    return cetcd_backend_put(be, LEASE_BUCKET, LEASE_KEY_NEXT, sizeof(LEASE_KEY_NEXT),
                             nextv, sizeof(nextv));
}

static int lease_free_(cetcd_lease *l) {
    if (!l) return 0;
    for (size_t i = 0; i < l->key_count; ++i) {
        free(l->keys[i]);
    }
    free(l->keys);
    free(l->key_lens);
    return 0;
}

static int revoke_mem_(cetcd_lease_mgr *mgr, cetcd_lease_id id) {
    for (size_t i = 0; i < mgr->count; ++i) {
        if (mgr->leases[i].id == id) {
            lease_free_(&mgr->leases[i]);
            for (size_t j = i + 1; j < mgr->count; ++j) {
                mgr->leases[j - 1] = mgr->leases[j];
            }
            mgr->count--;
            return CETCD_OK;
        }
    }
    return CETCD_ERR_NOTFOUND;
}

static int restore_(cetcd_lease_mgr *mgr, cetcd_lease_id id,
                    int64_t granted_ttl, int64_t remaining_ms) {
    if (!mgr || id == 0 || granted_ttl <= 0 || granted_ttl > CETCD_MAX_LEASE_TTL)
        return CETCD_ERR_INVAL;
    if (cetcd_lease_exists(mgr, id)) return CETCD_ERR_EXISTS;
    if (ensure_cap_(mgr) != 0) return CETCD_ERR_NOMEM;
    if (remaining_ms < 0) remaining_ms = 0;
    cetcd_lease l;
    l.id = id;
    l.ttl_seconds = granted_ttl;
    l.deadline_ms = mgr->now_ms + remaining_ms;
    l.keys = NULL;
    l.key_lens = NULL;
    l.key_count = 0;
    l.key_cap = 0;
    mgr->leases[mgr->count++] = l;
    if (mgr->next_id <= id) mgr->next_id = id + 1;
    return CETCD_OK;
}

/* Public API */
cetcd_lease_mgr *cetcd_lease_mgr_new(cetcd_lease_expire_fn on_expire, void *udata) {
    (void)udata; /* udata is passed to callback; kept for API parity */
    cetcd_lease_mgr *mgr = (cetcd_lease_mgr *)calloc(1, sizeof(*mgr));
    if (mgr == NULL) return NULL;
    mgr->on_expire = on_expire;
    mgr->expire_udata = udata;
    mgr->now_ms = 0;
    mgr->count = 0;
    mgr->cap = 0;
    mgr->leases = NULL;
    mgr->next_id = 1;
    return mgr;
}

void cetcd_lease_mgr_free(cetcd_lease_mgr *mgr) {
    if (!mgr) return;
    for (size_t i = 0; i < mgr->count; ++i) {
        lease_free_(&mgr->leases[i]);
    }
    free(mgr->leases);
    free(mgr);
}

void cetcd_lease_mgr_set_expire(cetcd_lease_mgr *mgr,
                                 cetcd_lease_expire_fn on_expire,
                                 void *udata) {
    if (!mgr) return;
    mgr->on_expire = on_expire;
    mgr->expire_udata = udata;
}

cetcd_lease_id cetcd_lease_grant(cetcd_lease_mgr *mgr, int64_t ttl_seconds) {
    if (!mgr || ttl_seconds <= 0 || ttl_seconds > CETCD_MAX_LEASE_TTL)
        return 0; /* 0 indicates invalid id */
    if (ensure_cap_(mgr) != 0) return 0;

    cetcd_lease l;
    l.id = mgr->next_id++;
    l.ttl_seconds = ttl_seconds;
    l.deadline_ms = mgr->now_ms + ttl_seconds * 1000;
    l.keys = NULL;
    l.key_lens = NULL;
    l.key_count = 0;
    l.key_cap = 0;

    mgr->leases[mgr->count++] = l;
    if (mgr->backend && persist_put_(mgr->backend, &mgr->leases[mgr->count - 1],
                                     mgr->now_ms, mgr->next_id) != CETCD_OK) {
        (void)revoke_mem_(mgr, l.id);
        return 0;
    }
    return l.id;
}

cetcd_lease_id cetcd_lease_grant_id(cetcd_lease_mgr *mgr, cetcd_lease_id id,
                                    int64_t ttl_seconds) {
    if (!mgr || id == 0 || ttl_seconds <= 0 || ttl_seconds > CETCD_MAX_LEASE_TTL)
        return 0;
    if (cetcd_lease_exists(mgr, id)) return 0;
    if (ensure_cap_(mgr) != 0) return 0;

    cetcd_lease l;
    l.id = id;
    l.ttl_seconds = ttl_seconds;
    l.deadline_ms = mgr->now_ms + ttl_seconds * 1000;
    l.keys = NULL;
    l.key_lens = NULL;
    l.key_count = 0;
    l.key_cap = 0;

    mgr->leases[mgr->count++] = l;
    if (mgr->next_id <= id) mgr->next_id = id + 1;
    if (mgr->backend && persist_put_(mgr->backend, &mgr->leases[mgr->count - 1],
                                     mgr->now_ms, mgr->next_id) != CETCD_OK) {
        (void)revoke_mem_(mgr, l.id);
        return 0;
    }
    return l.id;
}

cetcd_lease_id cetcd_lease_next_id(const cetcd_lease_mgr *mgr) {
    return mgr ? mgr->next_id : 0;
}

int cetcd_lease_revoke(cetcd_lease_mgr *mgr, cetcd_lease_id id) {
    if (!mgr) return CETCD_ERR_INVAL;
    if (!cetcd_lease_exists(mgr, id)) return CETCD_ERR_NOTFOUND;
    if (mgr->backend) {
        int rc = persist_del_(mgr->backend, id);
        if (rc != CETCD_OK) return rc;
    }
    return revoke_mem_(mgr, id);
}

int cetcd_lease_keep_alive(cetcd_lease_mgr *mgr, cetcd_lease_id id, int64_t ttl_seconds) {
    if (!mgr) return CETCD_ERR_INVAL;
    cetcd_lease *l = lease_by_id_(mgr, id);
    if (!l) return CETCD_ERR_NOTFOUND;
    int64_t old_ttl = l->ttl_seconds;
    int64_t old_deadline = l->deadline_ms;
    l->ttl_seconds = ttl_seconds;
    l->deadline_ms = mgr->now_ms + ttl_seconds * 1000;
    if (mgr->backend && persist_put_(mgr->backend, l, mgr->now_ms, mgr->next_id) != CETCD_OK) {
        l->ttl_seconds = old_ttl;
        l->deadline_ms = old_deadline;
        return CETCD_ERR_IO;
    }
    return CETCD_OK;
}

static cetcd_lease *lease_by_id_local_(const cetcd_lease_mgr *mgr, cetcd_lease_id id);

bool cetcd_lease_exists(const cetcd_lease_mgr *mgr, cetcd_lease_id id) {
    if (!mgr) return false;
    return (lease_by_id_local_(mgr, id) != NULL);
}

/* Helpers for non-mutable access */
static cetcd_lease *lease_by_id_local_(const cetcd_lease_mgr *mgr, cetcd_lease_id id) {
    if (!mgr) return NULL;
    for (size_t i = 0; i < mgr->count; ++i) {
        if (mgr->leases[i].id == id) return &mgr->leases[i];
    }
    return NULL;
}

/* end helpers */

int64_t cetcd_lease_ttl_remaining(const cetcd_lease_mgr *mgr, cetcd_lease_id id) {
    cetcd_lease *l = NULL;
    if (!mgr) return 0;
    l = lease_by_id_local_(mgr, id);
    if (!l) return 0;
    int64_t rem = l->deadline_ms - mgr->now_ms;
    if (rem < 0) rem = 0;
    return rem / 1000; /* seconds remaining */
}

int cetcd_lease_attach_key(cetcd_lease_mgr *mgr, cetcd_lease_id id,
                          const uint8_t *key, size_t key_len) {
    if (!mgr) return CETCD_ERR_INVAL;
    cetcd_lease *l = lease_by_id_(mgr, id);
    if (!l) return CETCD_ERR_NOTFOUND;
    /* Idempotent: same key already attached → no-op. */
    for (size_t i = 0; i < l->key_count; ++i) {
        if (l->key_lens[i] == key_len &&
            ((key_len == 0) || memcmp(l->keys[i], key, key_len) == 0)) {
            return CETCD_OK;
        }
    }
    /* ensure capacity.
     *
     * Grow transactionally: allocate fresh parallel arrays, copy the live
     * entries, then free+swap. If either allocation fails the originals are
     * left untouched. The previous paired-realloc() pattern freed the
     * successful replacement on partial failure, leaving the corresponding
     * field pointing at already-released storage (realloc invalidates the
     * old pointer even when a new one is returned), so any later detach /
     * revoke / free touched dangling memory. */
    if (l->key_count == l->key_cap) {
        size_t new_cap = (l->key_cap == 0) ? 4 : l->key_cap * 2;
        if (new_cap > SIZE_MAX / sizeof(uint8_t *)) return CETCD_ERR_NOMEM;
        uint8_t **new_keys = (uint8_t **)malloc(new_cap * sizeof(uint8_t *));
        if (!new_keys) return CETCD_ERR_NOMEM;
        size_t *new_lens = (size_t *)malloc(new_cap * sizeof(size_t));
        if (!new_lens) { free(new_keys); return CETCD_ERR_NOMEM; }
        if (l->key_count) {
            memcpy(new_keys, l->keys, l->key_count * sizeof(uint8_t *));
            memcpy(new_lens, l->key_lens, l->key_count * sizeof(size_t));
        }
        free(l->keys);
        free(l->key_lens);
        l->keys = new_keys;
        l->key_lens = new_lens;
        l->key_cap = new_cap;
    }
    uint8_t *dup = NULL;
    if (key_len > 0) {
        dup = (uint8_t *)malloc(key_len);
        if (dup == NULL) return CETCD_ERR_NOMEM;
        memcpy(dup, key, key_len);
    }
    l->keys[l->key_count] = dup;
    l->key_lens[l->key_count] = key_len;
    l->key_count++;
    return CETCD_OK;
}

int cetcd_lease_detach_key(cetcd_lease_mgr *mgr, cetcd_lease_id id,
                          const uint8_t *key, size_t key_len) {
    if (!mgr) return CETCD_ERR_INVAL;
    cetcd_lease *l = lease_by_id_(mgr, id);
    if (!l) return CETCD_ERR_NOTFOUND;
    /* find matching key */
    for (size_t i = 0; i < l->key_count; ++i) {
        if (l->key_lens[i] == key_len && memcmp(l->keys[i], key, key_len) == 0) {
            /* free this key */
            free(l->keys[i]);
            /* shift remaining */
            for (size_t j = i + 1; j < l->key_count; ++j) {
                l->keys[j-1] = l->keys[j];
                l->key_lens[j-1] = l->key_lens[j];
            }
            l->key_count--;
            return CETCD_OK;
        }
    }
    return CETCD_ERR_NOTFOUND;
}

void cetcd_lease_mgr_tick(cetcd_lease_mgr *mgr, int64_t elapsed_ms) {
    if (!mgr || elapsed_ms <= 0) return;
    mgr->now_ms += elapsed_ms;
    /* expire leases whose deadline_ms <= now_ms */
    size_t i = 0;
    while (i < mgr->count) {
        cetcd_lease *l = &mgr->leases[i];
        if (l->deadline_ms <= mgr->now_ms) {
            if (mgr->backend && persist_del_(mgr->backend, l->id) != CETCD_OK) {
                ++i;
                continue;
            }
            if (mgr->on_expire) {
                mgr->on_expire(l->id, (const uint8_t *const *)l->keys, l->key_lens, l->key_count, mgr->expire_udata);
            }
            /* free attached keys and this lease */
            lease_free_(l);
            /* shift remaining leases left */
            for (size_t j = i + 1; j < mgr->count; ++j) {
                mgr->leases[j - 1] = mgr->leases[j];
            }
            mgr->count--;
            /* do not increment i, new lease at i needs checking */
            continue;
        }
        ++i;
    }
}

size_t cetcd_lease_mgr_count(const cetcd_lease_mgr *mgr) {
    return mgr ? mgr->count : 0;
}

int64_t cetcd_lease_granted_ttl(const cetcd_lease_mgr *mgr, cetcd_lease_id id) {
    if (!mgr) return 0;
    for (size_t i = 0; i < mgr->count; ++i) {
        if (mgr->leases[i].id == id) return mgr->leases[i].ttl_seconds;
    }
    return 0;
}

size_t cetcd_lease_mgr_leases(const cetcd_lease_mgr *mgr,
                               cetcd_lease_id *out, size_t cap) {
    if (!mgr) return 0;
    if (!out || cap == 0) return mgr->count;
    size_t n = (mgr->count < cap) ? mgr->count : cap;
    for (size_t i = 0; i < n; ++i) {
        out[i] = mgr->leases[i].id;
    }
    return n;
}

size_t cetcd_lease_keys(const cetcd_lease_mgr *mgr, cetcd_lease_id id,
                         const uint8_t *const **out_keys,
                         const size_t **out_lens) {
    if (!mgr || !out_keys || !out_lens) return 0;
    cetcd_lease *l = lease_by_id_local_(mgr, id);
    if (!l || l->key_count == 0) return 0;
    *out_keys = (const uint8_t *const *)l->keys;
    *out_lens = (const size_t *)l->key_lens;
    return l->key_count;
}

int cetcd_lease_reindex_from_store(cetcd_lease_mgr *mgr, cetcd_mvcc_store *store) {
    if (!mgr || !store) return CETCD_ERR_INVAL;

    static const uint8_t from_key[] = {0};
    cetcd_kv *kvs = NULL;
    size_t n = 0;
    if (cetcd_mvcc_range(store, 0, (const uint8_t *)"", 0,
                         from_key, 1, &kvs, &n) != CETCD_OK) {
        return CETCD_OK;
    }

    for (size_t i = 0; i < n; i++) {
        if (kvs[i].lease_id <= 0) continue;
        cetcd_lease_id lid = (cetcd_lease_id)kvs[i].lease_id;
        if (!cetcd_lease_exists(mgr, lid)) {
            if (mgr->have_bucket) {
                if (restore_(mgr, lid, 1, 0) != CETCD_OK)
                    continue;
            } else if (cetcd_lease_grant_id(mgr, lid, CETCD_LEASE_REBUILD_TTL) == 0) {
                continue;
            }
        }
        cetcd_lease_attach_key(mgr, lid, kvs[i].key.data, kvs[i].key.len);
    }
    if (kvs) cetcd_kv_free_contents(kvs, n);
    return CETCD_OK;
}

void cetcd_lease_mgr_set_backend(cetcd_lease_mgr *mgr, struct cetcd_backend *be) {
    if (mgr) mgr->backend = be;
}

int cetcd_lease_save(cetcd_lease_mgr *mgr, struct cetcd_backend *be) {
    if (!mgr || !be) return CETCD_ERR_INVAL;
    for (size_t i = 0; i < mgr->count; i++) {
        int rc = persist_put_(be, &mgr->leases[i], mgr->now_ms, mgr->next_id);
        if (rc != CETCD_OK) return rc;
    }
    return persist_next_(be, mgr->next_id);
}

typedef struct {
    cetcd_lease_mgr *mgr;
    int rc;
} lease_load_ctx_;

static bool load_cb_(const uint8_t *key, size_t klen,
                     const uint8_t *val, size_t vlen, void *udata) {
    lease_load_ctx_ *ctx = (lease_load_ctx_ *)udata;
    ctx->mgr->have_bucket = 1;
    if (klen == sizeof(LEASE_KEY_NEXT) &&
        memcmp(key, LEASE_KEY_NEXT, sizeof(LEASE_KEY_NEXT)) == 0) {
        if (vlen >= 8) {
            cetcd_lease_id n = (cetcd_lease_id)read_le64_(val);
            if (n > ctx->mgr->next_id) ctx->mgr->next_id = n;
        }
        return true;
    }
    if (klen != 8 || vlen != 16) return true;
    cetcd_lease_id id = (cetcd_lease_id)read_le64_(key);
    int64_t granted = (int64_t)read_le64_(val);
    int64_t unix_dl = (int64_t)read_le64_(val + 8);
    int rc = restore_(ctx->mgr, id, granted, unix_dl - realtime_ms_());
    if (rc != CETCD_OK && rc != CETCD_ERR_EXISTS)
        ctx->rc = rc;
    return true;
}

int cetcd_lease_load(cetcd_lease_mgr *mgr, struct cetcd_backend *be) {
    if (!mgr || !be) return CETCD_ERR_INVAL;
    mgr->have_bucket = 0;
    lease_load_ctx_ ctx;
    ctx.mgr = mgr;
    ctx.rc = CETCD_OK;
    int rc = cetcd_backend_foreach(be, LEASE_BUCKET, load_cb_, &ctx);
    if (rc != CETCD_OK) return rc;
    return ctx.rc;
}
