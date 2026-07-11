#include "cetcd/base.h"
#include "cetcd/mvcc.h"
#include "cetcd/treap.h"
#include "cetcd/backend.h"
#include "cetcd/hashmap.h"
#include "cetcd/log.h"
#include <stdlib.h>
#include <string.h>

#define MVCC_BUCKET_KEY  "key"
#define MVCC_BUCKET_META "meta"
static const uint8_t MVCC_META_REV[] = "revision";


typedef struct {
    cetcd_revision create_rev;
    int64_t        version;
    cetcd_revision mod_rev;
    cetcd_slice    value;
    bool           deleted;
    int64_t        lease_id;
} key_generation;

typedef struct {
    cetcd_slice     key;
    cetcd_slice     value;
    cetcd_revision  rev;
    cetcd_event_type type;
    int64_t         version;
    cetcd_revision  create_rev;
    int64_t         lease_id;
} revision_entry;

typedef struct {
    cetcd_kv *rows;
    size_t   nr;
    size_t   cap;
} range_ctx;


static cetcd_slice dup_slice(cetcd_slice s);
static void free_key_generation(key_generation *g);
static void mvcc_notify_watchers(cetcd_mvcc_store *s, const cetcd_kv *kv,
                                  const cetcd_revision *rev, cetcd_event_type type,
                                  const cetcd_kv *prev_kv);
static bool range_collect_cb(cetcd_slice key, void *val, void *udata);
static bool free_kg_iter(cetcd_slice key, void *value, void *udata);


struct cetcd_mvcc_store {
    cetcd_treap    *index;
    int64_t         main_rev;
    revision_entry *history;
    size_t          history_count;
    size_t          history_cap;
    int64_t         compacted_rev;
    cetcd_watcher **watchers;
    size_t          watcher_count;
    size_t          watcher_cap;
    /* Streaming watchers (notification-channel based) */
    cetcd_stream_watcher **stream_watchers;
    size_t                stream_watcher_count;
    size_t                stream_watcher_cap;
    /* Optional LMDB mirror for crash recovery (not owned). */
    cetcd_backend  *backend;
};

static void write_le64_(uint8_t *p, uint64_t v) {
    p[0] = (uint8_t)(v);
    p[1] = (uint8_t)(v >> 8);
    p[2] = (uint8_t)(v >> 16);
    p[3] = (uint8_t)(v >> 24);
    p[4] = (uint8_t)(v >> 32);
    p[5] = (uint8_t)(v >> 40);
    p[6] = (uint8_t)(v >> 48);
    p[7] = (uint8_t)(v >> 56);
}

static uint64_t read_le64_(const uint8_t *p) {
    return (uint64_t)p[0]
         | ((uint64_t)p[1] << 8)
         | ((uint64_t)p[2] << 16)
         | ((uint64_t)p[3] << 24)
         | ((uint64_t)p[4] << 32)
         | ((uint64_t)p[5] << 40)
         | ((uint64_t)p[6] << 48)
         | ((uint64_t)p[7] << 56);
}

/* Persist format: 6×int64 LE + value bytes
 * create_main, create_sub, mod_main, mod_sub, version, lease_id, then value. */
static int encode_kg_(const key_generation *kg, uint8_t **out, size_t *out_len) {
    size_t vlen = kg->value.len;
    size_t total = 48 + vlen;
    uint8_t *buf = (uint8_t *)malloc(total);
    if (!buf) return CETCD_ERR_NOMEM;
    write_le64_(buf + 0,  (uint64_t)kg->create_rev.main);
    write_le64_(buf + 8,  (uint64_t)kg->create_rev.sub);
    write_le64_(buf + 16, (uint64_t)kg->mod_rev.main);
    write_le64_(buf + 24, (uint64_t)kg->mod_rev.sub);
    write_le64_(buf + 32, (uint64_t)kg->version);
    write_le64_(buf + 40, (uint64_t)kg->lease_id);
    if (vlen && kg->value.data) memcpy(buf + 48, kg->value.data, vlen);
    *out = buf;
    *out_len = total;
    return CETCD_OK;
}

static int decode_kg_(const uint8_t *data, size_t len, key_generation *kg) {
    if (!data || len < 48 || !kg) return CETCD_ERR_CORRUPT;
    memset(kg, 0, sizeof(*kg));
    kg->create_rev.main = (int64_t)read_le64_(data + 0);
    kg->create_rev.sub  = (int64_t)read_le64_(data + 8);
    kg->mod_rev.main    = (int64_t)read_le64_(data + 16);
    kg->mod_rev.sub     = (int64_t)read_le64_(data + 24);
    kg->version         = (int64_t)read_le64_(data + 32);
    kg->lease_id        = (int64_t)read_le64_(data + 40);
    kg->deleted         = false;
    size_t vlen = len - 48;
    if (vlen > 0) {
        kg->value.data = (const uint8_t *)malloc(vlen);
        if (!kg->value.data) return CETCD_ERR_NOMEM;
        memcpy((void *)kg->value.data, data + 48, vlen);
        kg->value.len = vlen;
    }
    return CETCD_OK;
}

static int persist_put_(cetcd_mvcc_store *s, const uint8_t *key, size_t key_len,
                         const key_generation *kg, int64_t rev) {
    if (!s || !s->backend || !key || !kg) return CETCD_OK;
    uint8_t *blob = NULL;
    size_t blob_len = 0;
    if (encode_kg_(kg, &blob, &blob_len) != CETCD_OK) return CETCD_ERR_NOMEM;
    uint8_t rev_buf[8];
    write_le64_(rev_buf, (uint64_t)rev);
    int rc = cetcd_backend_put2(s->backend,
                       MVCC_BUCKET_KEY, key, key_len, blob, blob_len,
                       MVCC_BUCKET_META, MVCC_META_REV, sizeof(MVCC_META_REV) - 1,
                       rev_buf, sizeof(rev_buf));
    free(blob);
    if (rc != CETCD_OK) {
        CETCD_WARN("mvcc persist put failed rc=%d key_len=%zu", rc, key_len);
    }
    return rc;
}

static int persist_del_(cetcd_mvcc_store *s, const uint8_t *key, size_t key_len,
                         int64_t rev) {
    if (!s || !s->backend || !key) return CETCD_OK;
    uint8_t rev_buf[8];
    write_le64_(rev_buf, (uint64_t)rev);
    int rc = cetcd_backend_put2(s->backend,
                       MVCC_BUCKET_KEY, key, key_len, NULL, 0,
                       MVCC_BUCKET_META, MVCC_META_REV, sizeof(MVCC_META_REV) - 1,
                       rev_buf, sizeof(rev_buf));
    if (rc != CETCD_OK) {
        CETCD_WARN("mvcc persist del failed rc=%d key_len=%zu", rc, key_len);
    }
    return rc;
}


struct cetcd_watcher {
    uint8_t       *pattern;
    size_t         pattern_len;
    bool           is_prefix;
    int64_t        start_rev;
    cetcd_watch_cb cb;
    void          *udata;
};


/* ── Streaming watcher ─────────────────────────────────────────────────── */

struct cetcd_stream_watcher {
    uint8_t       *pattern;
    size_t         pattern_len;
    bool           is_prefix;
    bool           is_range;
    uint8_t       *range_end;
    size_t         range_end_len;
    int64_t        start_rev;
    int64_t        watch_id;
    int            want_prev_kv;
    cetcd_mvcc_watch_notify *notify;
};


static cetcd_slice dup_slice(cetcd_slice s) {
    cetcd_slice out;
    out.len = s.len;
    if (s.len == 0) { out.data = NULL; return out; }
    out.data = (const uint8_t*)malloc(s.len);
    if (out.data) memcpy((void*)out.data, s.data, s.len);
    return out;
}

static void free_key_generation(key_generation *g) {
    if (!g) return;
    if (g->value.data) free((void*)g->value.data);
    free(g);
}

static bool free_kg_iter(cetcd_slice key, void *value, void *udata) {
    (void)key; (void)udata;
    free_key_generation((key_generation*)value);
    return true;
}

static bool range_collect_cb(cetcd_slice key, void *val, void *udata) {
    (void)key;
    range_ctx *ctx = (range_ctx*)udata;
    key_generation *kg = (key_generation*)val;
    if (kg->deleted) return true;
    if (ctx->nr == ctx->cap) {
        size_t nc = ctx->cap ? ctx->cap * 2 : 4;
        cetcd_kv *tmp = (cetcd_kv*)realloc(ctx->rows, nc * sizeof(*tmp));
        if (!tmp) return false;
        ctx->rows = tmp;
        ctx->cap = nc;
    }
    cetcd_kv *kv = &ctx->rows[ctx->nr++];
    kv->key = dup_slice(key);
    kv->value = dup_slice(kg->value);
    kv->create_rev = kg->create_rev;
    kv->mod_rev = kg->mod_rev;
    kv->version = kg->version;
    kv->lease_id = kg->lease_id;
    return true;
}

static void push_history(cetcd_mvcc_store *s, revision_entry e) {
    if (s->history_count == s->history_cap) {
        size_t nc = s->history_cap ? s->history_cap * 2 : 4;
        revision_entry *tmp = (revision_entry*)realloc(s->history, nc * sizeof(*tmp));
        if (!tmp) return;
        s->history = tmp;
        s->history_cap = nc;
    }
    s->history[s->history_count++] = e;
}


static bool watch_match(const cetcd_watcher *w, const uint8_t *key, size_t key_len) {
    if (w->is_prefix) {
        if (key_len < w->pattern_len) return false;
        return memcmp(key, w->pattern, w->pattern_len) == 0;
    }
    return key_len == w->pattern_len && memcmp(key, w->pattern, key_len) == 0;
}

static bool stream_watch_match(const cetcd_stream_watcher *w,
                                const uint8_t *key, size_t key_len) {
    if (w->is_range) {
        /* Range watch: key >= pattern AND key < range_end */
        if (key_len < w->pattern_len) return false;
        if (memcmp(key, w->pattern, w->pattern_len) < 0) return false;
        if (w->range_end && w->range_end_len > 0) {
            size_t cmp_len = key_len < w->range_end_len ? key_len : w->range_end_len;
            int c = memcmp(key, w->range_end, cmp_len);
            if (c >= 0) {
                if (c == 0 && key_len < w->range_end_len) return true;
                return false;
            }
        }
        return true;
    }
    if (w->is_prefix) {
        if (key_len < w->pattern_len) return false;
        return memcmp(key, w->pattern, w->pattern_len) == 0;
    }
    return key_len == w->pattern_len && memcmp(key, w->pattern, key_len) == 0;
}

/* Push a copy of an event into a notification channel and wake the consumer. */
static void notify_push(cetcd_mvcc_watch_notify *n, const cetcd_watch_event *ev) {
    if (!n || !ev) return;
    cetcd_mvcc_watch_event_node *node =
        (cetcd_mvcc_watch_event_node *)calloc(1, sizeof(*node));
    if (!node) return;
    node->event = *ev;
    /* Deep-copy slice data so the event is independent of the MVCC store. */
    node->event.kv.key   = dup_slice(ev->kv.key);
    node->event.kv.value = dup_slice(ev->kv.value);
    if (ev->has_prev_kv) {
        node->event.prev_kv.key   = dup_slice(ev->prev_kv.key);
        node->event.prev_kv.value = dup_slice(ev->prev_kv.value);
    }
    node->next = NULL;
    if (n->tail) n->tail->next = node;
    else        n->head = node;
    n->tail = node;
    n->count++;
    /* Wake the consumer (typically uv_async_send). */
    if (n->wake_cb) n->wake_cb(n->wake_cb_udata);
}

static void mvcc_notify_watchers(cetcd_mvcc_store *s, const cetcd_kv *kv,
                                  const cetcd_revision *rev, cetcd_event_type type,
                                  const cetcd_kv *prev_kv) {
    if (!s || !kv || !rev) return;
    cetcd_watch_event ev;
    ev.type = type;
    ev.kv = *kv;
    ev.rev = *rev;
    ev.has_prev_kv = 0;
    memset(&ev.prev_kv, 0, sizeof(ev.prev_kv));
    if (prev_kv) {
        ev.prev_kv = *prev_kv;
        ev.has_prev_kv = 1;
    }
    /* Callback-based watchers */
    for (size_t i = 0; i < s->watcher_count; i++) {
        cetcd_watcher *w = s->watchers[i];
        if (w->start_rev > 0 && rev->main < w->start_rev) continue;
        if (watch_match(w, kv->key.data, kv->key.len)) {
            w->cb(&ev, w->udata);
        }
    }
    /* Streaming / notification-channel watchers */
    for (size_t i = 0; i < s->stream_watcher_count; i++) {
        cetcd_stream_watcher *sw = s->stream_watchers[i];
        if (sw->start_rev > 0 && rev->main < sw->start_rev) continue;
        if (stream_watch_match(sw, kv->key.data, kv->key.len)) {
            /* Filter prev_kv based on want_prev_kv */
            cetcd_watch_event ev_copy = ev;
            if (!sw->want_prev_kv) {
                ev_copy.has_prev_kv = 0;
                memset(&ev_copy.prev_kv, 0, sizeof(ev_copy.prev_kv));
            }
            notify_push(sw->notify, &ev_copy);
        }
    }
}


cetcd_mvcc_store *cetcd_mvcc_store_new(void) {
    cetcd_mvcc_store *s = (cetcd_mvcc_store*)calloc(1, sizeof(*s));
    if (!s) return NULL;
    s->index = cetcd_treap_new();
    if (!s->index) { free(s); return NULL; }
    return s;
}

void cetcd_mvcc_store_free(cetcd_mvcc_store *s) {
    if (!s) return;
    if (s->index) {
        cetcd_treap_iter(s->index, free_kg_iter, NULL);
        cetcd_treap_free(s->index);
    }
    for (size_t i = 0; i < s->history_count; i++) {
        free((void*)s->history[i].key.data);
        free((void*)s->history[i].value.data);
    }
    free(s->history);
    for (size_t i = 0; i < s->watcher_count; i++) {
        free(s->watchers[i]->pattern);
        free(s->watchers[i]);
    }
    free(s->watchers);
    /* Free streaming watchers */
    for (size_t i = 0; i < s->stream_watcher_count; i++) {
        free(s->stream_watchers[i]->pattern);
        free(s->stream_watchers[i]->range_end);
        free(s->stream_watchers[i]);
    }
    free(s->stream_watchers);
    free(s);
}

int64_t cetcd_mvcc_revision(const cetcd_mvcc_store *s) {
    return s ? s->main_rev : 0;
}

cetcd_revision cetcd_mvcc_put(cetcd_mvcc_store *s,
                               const uint8_t *key, size_t key_len,
                               const uint8_t *val, size_t val_len,
                               int64_t lease_id) {
    cetcd_revision zero = {0, 0};
    if (!s || !key) return zero;

    cetcd_slice kslice = cetcd_slice_make(key, key_len);
    cetcd_slice vslice = cetcd_slice_make(val, val_len);
    void *existing = NULL;
    int has_existing = cetcd_treap_get(s->index, kslice, &existing) ? 1 : 0;
    key_generation *kg = has_existing ? (key_generation *)existing : NULL;

    int64_t new_main = s->main_rev + 1;
    cetcd_revision new_rev = {new_main, 0};

    /* Allocate durable payload before touching store revision / index. */
    cetcd_slice val_owned = dup_slice(vslice);
    if (vslice.len > 0 && !val_owned.data) return zero;

    key_generation *new_kg = NULL;
    key_generation preview;
    memset(&preview, 0, sizeof(preview));
    preview.value = val_owned;
    preview.deleted = false;
    preview.lease_id = lease_id;
    preview.mod_rev = new_rev;
    if (has_existing) {
        preview.create_rev = kg->create_rev;
        preview.version = kg->version + 1;
    } else {
        new_kg = (key_generation *)calloc(1, sizeof(*new_kg));
        if (!new_kg) {
            free((void *)val_owned.data);
            return zero;
        }
        new_kg->create_rev = new_rev;
        new_kg->version = 1;
        new_kg->mod_rev = new_rev;
        new_kg->value = val_owned;
        new_kg->deleted = false;
        new_kg->lease_id = lease_id;
        preview = *new_kg;
    }

    /* Fail-closed: persist first; leave memory unchanged on LMDB error. */
    if (persist_put_(s, key, key_len, &preview, new_main) != CETCD_OK) {
        if (new_kg) free_key_generation(new_kg);
        else free((void *)val_owned.data);
        return zero;
    }

    s->main_rev = new_main;

    cetcd_kv prev_evkv;
    memset(&prev_evkv, 0, sizeof(prev_evkv));
    int has_prev = 0;

    if (has_existing) {
        if (!kg->deleted) {
            prev_evkv.key = dup_slice(kslice);
            prev_evkv.value = dup_slice(kg->value);
            prev_evkv.create_rev = kg->create_rev;
            prev_evkv.mod_rev = kg->mod_rev;
            prev_evkv.version = kg->version;
            prev_evkv.lease_id = kg->lease_id;
            has_prev = 1;
        }
        if (kg->value.data) free((void *)kg->value.data);
        kg->value = val_owned;
        kg->version++;
        kg->mod_rev = new_rev;
        kg->deleted = false;
        kg->lease_id = lease_id;
    } else {
        cetcd_treap_put(s->index, kslice, new_kg);
        kg = new_kg;
    }

    revision_entry e;
    e.key = dup_slice(kslice);
    e.value = dup_slice(vslice);
    e.rev = new_rev;
    e.type = CETCD_EVENT_PUT;
    e.version = kg->version;
    e.create_rev = kg->create_rev;
    e.lease_id = lease_id;
    push_history(s, e);

    cetcd_kv evkv;
    evkv.key = dup_slice(kslice);
    evkv.value = dup_slice(kg->value);
    evkv.create_rev = kg->create_rev;
    evkv.mod_rev = new_rev;
    evkv.version = kg->version;
    evkv.lease_id = kg->lease_id;
    mvcc_notify_watchers(s, &evkv, &new_rev, CETCD_EVENT_PUT,
                         has_prev ? &prev_evkv : NULL);
    free((void *)evkv.key.data);
    free((void *)evkv.value.data);
    if (has_prev) {
        free((void *)prev_evkv.key.data);
        free((void *)prev_evkv.value.data);
    }
    return new_rev;
}

static cetcd_revision delete_one_(cetcd_mvcc_store *s,
                                   const uint8_t *key, size_t key_len,
                                   int do_persist) {
    cetcd_revision zero = {0, 0};
    if (!s || !key) return zero;

    cetcd_slice kslice = cetcd_slice_make(key, key_len);
    void *existing = NULL;
    if (!cetcd_treap_get(s->index, kslice, &existing)) return zero;
    key_generation *kg = (key_generation *)existing;
    if (kg->deleted) return zero;

    int64_t new_main = s->main_rev + 1;
    cetcd_revision new_rev = {new_main, 0};

    /* Fail-closed: durable delete before memory / watch side effects. */
    if (do_persist && persist_del_(s, key, key_len, new_main) != CETCD_OK)
        return zero;

    s->main_rev = new_main;

    cetcd_kv prev_evkv;
    memset(&prev_evkv, 0, sizeof(prev_evkv));
    prev_evkv.key = dup_slice(kslice);
    prev_evkv.value = dup_slice(kg->value);
    prev_evkv.create_rev = kg->create_rev;
    prev_evkv.mod_rev = kg->mod_rev;
    prev_evkv.version = kg->version;
    prev_evkv.lease_id = kg->lease_id;

    revision_entry e;
    e.key = dup_slice(kslice);
    e.value = (cetcd_slice){NULL, 0};
    e.rev = new_rev;
    e.type = CETCD_EVENT_DELETE;
    e.version = kg->version;
    e.create_rev = kg->create_rev;
    e.lease_id = kg->lease_id;
    push_history(s, e);

    cetcd_kv evkv;
    evkv.key = dup_slice(kslice);
    evkv.value = (cetcd_slice){NULL, 0};
    evkv.create_rev = kg->create_rev;
    evkv.mod_rev = new_rev;
    evkv.version = kg->version;
    evkv.lease_id = kg->lease_id;
    mvcc_notify_watchers(s, &evkv, &new_rev, CETCD_EVENT_DELETE, &prev_evkv);

    void *removed = NULL;
    if (cetcd_treap_del(s->index, kslice, &removed) && removed) {
        free_key_generation((key_generation *)removed);
    }

    free((void *)evkv.key.data);
    free((void *)prev_evkv.key.data);
    free((void *)prev_evkv.value.data);
    return new_rev;
}

cetcd_revision cetcd_mvcc_delete(cetcd_mvcc_store *s,
                                  const uint8_t *key, size_t key_len) {
    return delete_one_(s, key, key_len, 1);
}

cetcd_revision cetcd_mvcc_delete_keys(cetcd_mvcc_store *s,
                                       const uint8_t *const *keys,
                                       const size_t *key_lens,
                                       size_t n) {
    cetcd_revision last = {0, 0};
    if (!s || !keys || !key_lens || n == 0) return last;

    /* Collect live keys first; persist one txn, then mutate memory. */
    const uint8_t **del_keys = (const uint8_t **)calloc(n, sizeof(*del_keys));
    size_t *del_lens = (size_t *)calloc(n, sizeof(*del_lens));
    size_t ndel = 0;
    if (!del_keys || !del_lens) {
        free(del_keys);
        free(del_lens);
        /* Fallback: per-key fail-closed path */
        for (size_t i = 0; i < n; i++) {
            if (!keys[i] || key_lens[i] == 0) continue;
            cetcd_revision r = delete_one_(s, keys[i], key_lens[i], 1);
            if (r.main > last.main) last = r;
        }
        return last;
    }

    for (size_t i = 0; i < n; i++) {
        if (!keys[i] || key_lens[i] == 0) continue;
        cetcd_slice kslice = cetcd_slice_make(keys[i], key_lens[i]);
        void *existing = NULL;
        if (!cetcd_treap_get(s->index, kslice, &existing)) continue;
        key_generation *kg = (key_generation *)existing;
        if (kg->deleted) continue;
        del_keys[ndel] = keys[i];
        del_lens[ndel] = key_lens[i];
        ndel++;
    }

    if (ndel == 0) {
        free(del_keys);
        free(del_lens);
        return last;
    }

    if (s->backend) {
        int64_t final_rev = s->main_rev + (int64_t)ndel;
        uint8_t rev_buf[8];
        write_le64_(rev_buf, (uint64_t)final_rev);
        int rc = cetcd_backend_del_n(s->backend, MVCC_BUCKET_KEY,
                                     del_keys, del_lens, ndel,
                                     MVCC_BUCKET_META,
                                     MVCC_META_REV, sizeof(MVCC_META_REV) - 1,
                                     rev_buf, sizeof(rev_buf));
        if (rc != CETCD_OK) {
            CETCD_WARN("mvcc persist batch del failed rc=%d n=%zu", rc, ndel);
            free(del_keys);
            free(del_lens);
            return last; /* fail-closed: no in-memory deletes */
        }
    }

    for (size_t i = 0; i < ndel; i++) {
        cetcd_revision r = delete_one_(s, del_keys[i], del_lens[i], 0);
        if (r.main > last.main) last = r;
    }

    free(del_keys);
    free(del_lens);
    return last;
}

int cetcd_mvcc_get(cetcd_mvcc_store *s, int64_t rev,
                    const uint8_t *key, size_t key_len,
                    cetcd_kv *out) {
    if (!s || !key || !out) return CETCD_ERR_INVAL;
    cetcd_slice kslice = cetcd_slice_make(key, key_len);

    if (rev == 0) {
        void *ptr = NULL;
        if (!cetcd_treap_get(s->index, kslice, &ptr)) return CETCD_ERR_NOTFOUND;
        key_generation *kg = (key_generation*)ptr;
        if (kg->deleted) return CETCD_ERR_NOTFOUND;
        out->key = dup_slice(kslice);
        out->value = dup_slice(kg->value);
        out->create_rev = kg->create_rev;
        out->mod_rev = kg->mod_rev;
        out->version = kg->version;
        out->lease_id = kg->lease_id;
        return CETCD_OK;
    }

    {
        int i = (int)s->history_count - 1;
        for (; i >= 0; i--) {
        revision_entry *e = &s->history[i];
        if (e->rev.main > rev) continue;
        if (e->key.len == kslice.len && memcmp(e->key.data, kslice.data, kslice.len) == 0) {
            if (e->type == CETCD_EVENT_DELETE) return CETCD_ERR_NOTFOUND;
            out->key = dup_slice(e->key);
            out->value = dup_slice(e->value);
            out->create_rev = e->create_rev;
            out->mod_rev = e->rev;
            out->version = e->version;
            out->lease_id = e->lease_id;
            return CETCD_OK;
        }
        }
    }
    return CETCD_ERR_NOTFOUND;
}

static int key_in_range_(const uint8_t *key, size_t key_len,
                          const uint8_t *lo, size_t lo_len,
                          const uint8_t *hi, size_t hi_len) {
    if (lo && lo_len > 0) {
        size_t n = key_len < lo_len ? key_len : lo_len;
        int c = memcmp(key, lo, n);
        if (c < 0 || (c == 0 && key_len < lo_len)) return 0;
    }
    if (hi && hi_len > 0) {
        size_t n = key_len < hi_len ? key_len : hi_len;
        int c = memcmp(key, hi, n);
        if (c > 0 || (c == 0 && key_len >= hi_len)) return 0;
    }
    return 1;
}

int cetcd_mvcc_range(cetcd_mvcc_store *s, int64_t rev,
                      const uint8_t *key_start, size_t start_len,
                      const uint8_t *key_end, size_t end_len,
                      cetcd_kv **out, size_t *out_count) {
    if (!s || !out || !out_count) return CETCD_ERR_INVAL;
    if (rev == 0) {
        cetcd_slice lo = cetcd_slice_make(key_start, start_len);
        cetcd_slice hi = cetcd_slice_make(key_end, end_len);
        range_ctx ctx = {0};
        cetcd_treap_range(s->index, lo, hi, range_collect_cb, &ctx);
        *out = ctx.rows;
        *out_count = ctx.nr;
        return CETCD_OK;
    }

    /* Historical range: walk newest→oldest; hashmap marks first-seen keys
     * (puts and deletes) so each key is O(1) expected, not O(n) scan. */
    range_ctx ctx = {0};
    cetcd_hashmap *seen = cetcd_hashmap_new(0);
    if (!seen) return CETCD_ERR_NOMEM;

    for (int i = (int)s->history_count - 1; i >= 0; i--) {
        revision_entry *e = &s->history[i];
        if (e->rev.main > rev) continue;
        if (!key_in_range_(e->key.data, e->key.len, key_start, start_len,
                           key_end, end_len))
            continue;
        if (cetcd_hashmap_contains(seen, e->key)) continue;
        if (cetcd_hashmap_put(seen, e->key, (void *)(uintptr_t)1) != CETCD_OK) {
            cetcd_hashmap_free(seen);
            cetcd_kv_free_contents(ctx.rows, ctx.nr);
            return CETCD_ERR_NOMEM;
        }
        if (e->type == CETCD_EVENT_DELETE)
            continue; /* tombstone: suppress older puts for this key */

        if (ctx.nr == ctx.cap) {
            size_t nc = ctx.cap ? ctx.cap * 2 : 4;
            cetcd_kv *tmp = (cetcd_kv *)realloc(ctx.rows, nc * sizeof(*tmp));
            if (!tmp) {
                cetcd_hashmap_free(seen);
                cetcd_kv_free_contents(ctx.rows, ctx.nr);
                return CETCD_ERR_NOMEM;
            }
            ctx.rows = tmp;
            ctx.cap = nc;
        }
        cetcd_kv *kv = &ctx.rows[ctx.nr++];
        kv->key = dup_slice(e->key);
        kv->value = dup_slice(e->value);
        kv->create_rev = e->create_rev;
        kv->mod_rev = e->rev;
        kv->version = e->version;
        kv->lease_id = e->lease_id;
    }
    cetcd_hashmap_free(seen);
    *out = ctx.rows;
    *out_count = ctx.nr;
    return CETCD_OK;
}

void cetcd_kv_free_contents(cetcd_kv *kvs, size_t count) {
    if (!kvs) return;
    for (size_t i = 0; i < count; i++) {
        free((void*)kvs[i].key.data);
        free((void*)kvs[i].value.data);
    }
    free(kvs);
}


static cetcd_watcher *create_watcher(cetcd_mvcc_store *s,
                                      const uint8_t *pattern, size_t pattern_len,
                                      bool is_prefix, int64_t start_rev,
                                      cetcd_watch_cb cb, void *udata) {
    cetcd_watcher *w = (cetcd_watcher*)calloc(1, sizeof(*w));
    if (!w) return NULL;
    w->pattern = (uint8_t*)malloc(pattern_len);
    if (!w->pattern) { free(w); return NULL; }
    memcpy(w->pattern, pattern, pattern_len);
    w->pattern_len = pattern_len;
    w->is_prefix = is_prefix;
    w->start_rev = start_rev;
    w->cb = cb;
    w->udata = udata;

    if (s->watcher_count == s->watcher_cap) {
        size_t nc = s->watcher_cap ? s->watcher_cap * 2 : 4;
        cetcd_watcher **tmp = (cetcd_watcher**)realloc(s->watchers, nc * sizeof(*tmp));
        if (!tmp) { free(w->pattern); free(w); return NULL; }
        s->watchers = tmp;
        s->watcher_cap = nc;
    }
    s->watchers[s->watcher_count++] = w;
    return w;
}

cetcd_watcher *cetcd_mvcc_watch(cetcd_mvcc_store *s,
                                 const uint8_t *key, size_t key_len,
                                 int64_t start_rev,
                                 cetcd_watch_cb cb, void *udata) {
    return create_watcher(s, key, key_len, false, start_rev, cb, udata);
}

cetcd_watcher *cetcd_mvcc_watch_prefix(cetcd_mvcc_store *s,
                                        const uint8_t *prefix, size_t prefix_len,
                                        int64_t start_rev,
                                        cetcd_watch_cb cb, void *udata) {
    return create_watcher(s, prefix, prefix_len, true, start_rev, cb, udata);
}

void cetcd_mvcc_watch_cancel(cetcd_mvcc_store *s, cetcd_watcher *w) {
    if (!s || !w) return;
    for (size_t i = 0; i < s->watcher_count; i++) {
        if (s->watchers[i] == w) {
            free(w->pattern);
            free(w);
            s->watchers[i] = s->watchers[--s->watcher_count];
            return;
        }
    }
}

/* ── Streaming watch (notification-channel based) ─────────────────────── */

void cetcd_mvcc_watch_notify_init(cetcd_mvcc_watch_notify *n,
                                   void (*wake_cb)(void *), void *udata) {
    if (!n) return;
    n->head = NULL;
    n->tail = NULL;
    n->count = 0;
    n->wake_cb = wake_cb;
    n->wake_cb_udata = udata;
}

void cetcd_mvcc_watch_notify_destroy(cetcd_mvcc_watch_notify *n) {
    if (!n) return;
    cetcd_mvcc_watch_event_node *cur = n->head;
    while (cur) {
        cetcd_mvcc_watch_event_node *next = cur->next;
        free((void*)cur->event.kv.key.data);
        free((void*)cur->event.kv.value.data);
        if (cur->event.has_prev_kv) {
            free((void*)cur->event.prev_kv.key.data);
            free((void*)cur->event.prev_kv.value.data);
        }
        free(cur);
        cur = next;
    }
    n->head = NULL;
    n->tail = NULL;
    n->count = 0;
}

cetcd_stream_watcher *cetcd_mvcc_watch_subscribe(
    cetcd_mvcc_store *store, int64_t watch_id,
    const uint8_t *key, size_t key_len,
    const uint8_t *range_end, size_t range_end_len,
    int64_t start_rev, int want_prev_kv,
    cetcd_mvcc_watch_notify *notify)
{
    if (!store || !key || !notify) return NULL;

    cetcd_stream_watcher *sw = (cetcd_stream_watcher *)calloc(1, sizeof(*sw));
    if (!sw) return NULL;

    sw->pattern = (uint8_t *)malloc(key_len);
    if (!sw->pattern) { free(sw); return NULL; }
    memcpy(sw->pattern, key, key_len);
    sw->pattern_len = key_len;
    sw->watch_id = watch_id;
    sw->start_rev = start_rev;
    sw->want_prev_kv = want_prev_kv;
    sw->notify = notify;

    /* Determine watch type */
    if (range_end && range_end_len > 0) {
        if (range_end_len == 1 && range_end[0] == '\0') {
            /* Single '\0' byte means prefix watch */
            sw->is_prefix = true;
            sw->is_range = false;
            sw->range_end = NULL;
            sw->range_end_len = 0;
        } else {
            /* Range watch [key, range_end) */
            sw->is_prefix = false;
            sw->is_range = true;
            sw->range_end = (uint8_t *)malloc(range_end_len);
            if (!sw->range_end) { free(sw->pattern); free(sw); return NULL; }
            memcpy(sw->range_end, range_end, range_end_len);
            sw->range_end_len = range_end_len;
        }
    } else {
        /* Exact key watch */
        sw->is_prefix = false;
        sw->is_range = false;
        sw->range_end = NULL;
        sw->range_end_len = 0;
    }

    /* Add to the store's streaming watcher list */
    if (store->stream_watcher_count == store->stream_watcher_cap) {
        size_t nc = store->stream_watcher_cap ? store->stream_watcher_cap * 2 : 4;
        cetcd_stream_watcher **tmp =
            (cetcd_stream_watcher **)realloc(store->stream_watchers, nc * sizeof(*tmp));
        if (!tmp) {
            free(sw->pattern);
            free(sw->range_end);
            free(sw);
            return NULL;
        }
        store->stream_watchers = tmp;
        store->stream_watcher_cap = nc;
    }
    store->stream_watchers[store->stream_watcher_count++] = sw;
    return sw;
}

void cetcd_mvcc_watch_unsubscribe(cetcd_mvcc_store *store,
                                   cetcd_stream_watcher *w) {
    if (!store || !w) return;
    for (size_t i = 0; i < store->stream_watcher_count; i++) {
        if (store->stream_watchers[i] == w) {
            free(w->pattern);
            free(w->range_end);
            free(w);
            store->stream_watchers[i] =
                store->stream_watchers[--store->stream_watcher_count];
            return;
        }
    }
}

int cetcd_mvcc_watch_recv(cetcd_mvcc_watch_notify *notify,
                           cetcd_watch_event **events_out,
                           size_t *count_out) {
    if (!notify || !events_out || !count_out) return CETCD_ERR_INVAL;

    size_t n = notify->count;
    if (n == 0) {
        *events_out = NULL;
        *count_out = 0;
        return CETCD_OK;
    }

    cetcd_watch_event *arr = (cetcd_watch_event *)calloc(n, sizeof(*arr));
    if (!arr) return CETCD_ERR_NOMEM;

    size_t idx = 0;
    cetcd_mvcc_watch_event_node *cur = notify->head;
    while (cur) {
        cetcd_mvcc_watch_event_node *next = cur->next;
        arr[idx++] = cur->event;
        free(cur);  /* Free the node, but NOT the event data (owned by arr now) */
        cur = next;
    }
    notify->head = NULL;
    notify->tail = NULL;
    notify->count = 0;

    *events_out = arr;
    *count_out = idx;
    return CETCD_OK;
}

int cetcd_mvcc_compact(cetcd_mvcc_store *s, int64_t compact_rev) {
    if (!s || compact_rev <= 0) return CETCD_ERR_INVAL;
    if (compact_rev > s->main_rev) return CETCD_ERR_INVAL;
    if (compact_rev <= s->compacted_rev) return CETCD_OK;

    size_t keep = 0;
    for (size_t i = 0; i < s->history_count; i++) {
        if (s->history[i].rev.main > compact_rev) {
            s->history[keep++] = s->history[i];
        } else {
            free((void*)s->history[i].key.data);
            free((void*)s->history[i].value.data);
        }
    }
    s->history_count = keep;
    s->compacted_rev = compact_rev;
    return CETCD_OK;
}

int64_t cetcd_mvcc_compacted_revision(const cetcd_mvcc_store *s) {
    return s ? s->compacted_rev : 0;
}

void cetcd_mvcc_set_backend(cetcd_mvcc_store *s, cetcd_backend *be) {
    if (!s) return;
    s->backend = be;
}

typedef struct {
    cetcd_mvcc_store *store;
    int               err;
} load_ctx_;

static bool load_kv_cb_(const uint8_t *key, size_t key_len,
                         const uint8_t *val, size_t val_len,
                         void *udata) {
    load_ctx_ *ctx = (load_ctx_ *)udata;
    key_generation *kg = (key_generation *)calloc(1, sizeof(*kg));
    if (!kg) { ctx->err = CETCD_ERR_NOMEM; return false; }
    if (decode_kg_(val, val_len, kg) != CETCD_OK) {
        free(kg);
        ctx->err = CETCD_ERR_CORRUPT;
        return false;
    }
    cetcd_slice kslice = cetcd_slice_make(key, key_len);
    /* treap_put copies the key; kg is owned by the treap. */
    if (cetcd_treap_put(ctx->store->index, kslice, kg) != 0) {
        free_key_generation(kg);
        ctx->err = CETCD_ERR_NOMEM;
        return false;
    }
    if (kg->mod_rev.main > ctx->store->main_rev)
        ctx->store->main_rev = kg->mod_rev.main;

    /* Seed history so rev>0 get/range work after restart (current generation only). */
    revision_entry e;
    e.key = dup_slice(kslice);
    e.value = dup_slice(kg->value);
    e.rev = kg->mod_rev;
    e.type = CETCD_EVENT_PUT;
    e.version = kg->version;
    e.create_rev = kg->create_rev;
    e.lease_id = kg->lease_id;
    push_history(ctx->store, e);
    return true;
}

int cetcd_mvcc_load(cetcd_mvcc_store *s, cetcd_backend *be) {
    if (!s || !be) return CETCD_ERR_INVAL;
    s->backend = be;

    uint8_t *rev_buf = NULL;
    size_t rev_len = 0;
    if (cetcd_backend_get(be, MVCC_BUCKET_META,
                          MVCC_META_REV, sizeof(MVCC_META_REV) - 1,
                          &rev_buf, &rev_len) == CETCD_OK &&
        rev_buf && rev_len >= 8) {
        s->main_rev = (int64_t)read_le64_(rev_buf);
    }
    free(rev_buf);

    load_ctx_ ctx = { .store = s, .err = CETCD_OK };
    int rc = cetcd_backend_foreach(be, MVCC_BUCKET_KEY, load_kv_cb_, &ctx);
    if (rc != CETCD_OK) return rc;
    return ctx.err;
}
