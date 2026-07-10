#include "cetcd/lease.h"
#include "cetcd/base.h"

#include <stdlib.h>
#include <string.h>
#include <stdint.h>

/* Internal lease structure matching the header contract. */
typedef struct cetcd_lease {
    cetcd_lease_id id;
    int64_t        ttl_seconds;
    int64_t        deadline_ms; /* absolute expiry time in ms */

    /* Attached keys */
    uint8_t      **keys;      /* array of key data pointers */
    size_t        *key_lens;  /* array of key lengths */
    size_t         key_count;
    size_t         key_cap;
} cetcd_lease;

/* Lease manager implementation */
typedef struct cetcd_lease_mgr {
    cetcd_lease   *leases;      /* dynamic array */
    size_t          count;
    size_t          cap;
    cetcd_lease_id  next_id;     /* starts at 1, increments */
    int64_t         now_ms;      /* accumulated time (ms) */
    cetcd_lease_expire_fn on_expire;
    void           *expire_udata;
} cetcd_lease_mgr;

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

static int lease_free_(cetcd_lease *l) {
    if (!l) return 0;
    for (size_t i = 0; i < l->key_count; ++i) {
        free(l->keys[i]);
    }
    free(l->keys);
    free(l->key_lens);
    return 0;
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
    if (!mgr || ttl_seconds <= 0) return 0; /* 0 indicates invalid id */
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
    return l.id;
}

int cetcd_lease_revoke(cetcd_lease_mgr *mgr, cetcd_lease_id id) {
    if (!mgr) return CETCD_ERR_INVAL;
    for (size_t i = 0; i < mgr->count; ++i) {
        if (mgr->leases[i].id == id) {
            /* free keys */
            lease_free_(&mgr->leases[i]);
            /* shift left */
            for (size_t j = i + 1; j < mgr->count; ++j) {
                mgr->leases[j-1] = mgr->leases[j];
            }
            mgr->count--;
            return CETCD_OK;
        }
    }
    return CETCD_ERR_NOTFOUND;
}

int cetcd_lease_keep_alive(cetcd_lease_mgr *mgr, cetcd_lease_id id, int64_t ttl_seconds) {
    if (!mgr) return CETCD_ERR_INVAL;
    cetcd_lease *l = lease_by_id_(mgr, id);
    if (!l) return CETCD_ERR_NOTFOUND;
    l->ttl_seconds = ttl_seconds;
    l->deadline_ms = mgr->now_ms + ttl_seconds * 1000;
    return CETCD_OK;
}

bool cetcd_lease_exists(const cetcd_lease_mgr *mgr, cetcd_lease_id id) {
    if (!mgr) return false;
    return (lease_by_id_((cetcd_lease_mgr *)mgr, id) != NULL);
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
    /* ensure capacity */
    if (l->key_count == l->key_cap) {
        size_t new_cap = (l->key_cap == 0) ? 4 : l->key_cap * 2;
        uint8_t **new_keys = (uint8_t **)realloc(l->keys, new_cap * sizeof(uint8_t *));
        size_t *new_lens = (size_t *)realloc(l->key_lens, new_cap * sizeof(size_t));
        if (new_keys == NULL || new_lens == NULL) {
            free(new_keys); free(new_lens); /* best effort */
            return CETCD_ERR_NOMEM;
        }
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
            /* prepare data for callback */
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
