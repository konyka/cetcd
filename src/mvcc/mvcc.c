#include "cetcd/base.h"
#include "cetcd/mvcc.h"
#include "cetcd/treap.h"
#include <stdlib.h>
#include <string.h>


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
                                  const cetcd_revision *rev, cetcd_event_type type);
static bool range_collect_cb(cetcd_slice key, void *val, void *udata);
static bool free_kg_iter(cetcd_slice key, void *value, void *udata);


struct cetcd_mvcc_store {
    cetcd_treap    *index;
    int64_t         main_rev;
    revision_entry *history;
    size_t          history_count;
    size_t          history_cap;
    cetcd_watcher **watchers;
    size_t          watcher_count;
    size_t          watcher_cap;
};


struct cetcd_watcher {
    uint8_t       *pattern;
    size_t         pattern_len;
    bool           is_prefix;
    int64_t        start_rev;
    cetcd_watch_cb cb;
    void          *udata;
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

static void mvcc_notify_watchers(cetcd_mvcc_store *s, const cetcd_kv *kv,
                                  const cetcd_revision *rev, cetcd_event_type type) {
    if (!s || !kv || !rev) return;
    cetcd_watch_event ev;
    ev.type = type;
    ev.kv = *kv;
    ev.rev = *rev;
    for (size_t i = 0; i < s->watcher_count; i++) {
        cetcd_watcher *w = s->watchers[i];
        if (w->start_rev > 0 && rev->main < w->start_rev) continue;
        if (watch_match(w, kv->key.data, kv->key.len)) {
            w->cb(&ev, w->udata);
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
    s->main_rev++;
    cetcd_revision new_rev = {s->main_rev, 0};

    cetcd_slice kslice = cetcd_slice_make(key, key_len);
    cetcd_slice vslice = cetcd_slice_make(val, val_len);
    void *existing = NULL;
    key_generation *kg;

    if (cetcd_treap_get(s->index, kslice, &existing)) {
        kg = (key_generation*)existing;
        if (kg->value.data) free((void*)kg->value.data);
        kg->value = dup_slice(vslice);
        kg->version++;
        kg->mod_rev = new_rev;
        kg->deleted = false;
        kg->lease_id = lease_id;
    } else {
        kg = (key_generation*)calloc(1, sizeof(*kg));
        if (!kg) return zero;
        kg->create_rev = new_rev;
        kg->version = 1;
        kg->mod_rev = new_rev;
        kg->value = dup_slice(vslice);
        kg->deleted = false;
        kg->lease_id = lease_id;
        cetcd_treap_put(s->index, kslice, kg);
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
    mvcc_notify_watchers(s, &evkv, &new_rev, CETCD_EVENT_PUT);
    free((void*)evkv.key.data);
    free((void*)evkv.value.data);
    return new_rev;
}

cetcd_revision cetcd_mvcc_delete(cetcd_mvcc_store *s,
                                  const uint8_t *key, size_t key_len) {
    cetcd_revision zero = {0, 0};
    if (!s || !key) return zero;
    s->main_rev++;
    cetcd_revision new_rev = {s->main_rev, 0};

    cetcd_slice kslice = cetcd_slice_make(key, key_len);
    void *existing = NULL;

    if (cetcd_treap_get(s->index, kslice, &existing)) {
        key_generation *kg = (key_generation*)existing;
        kg->deleted = true;
        kg->mod_rev = new_rev;

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
        mvcc_notify_watchers(s, &evkv, &new_rev, CETCD_EVENT_DELETE);
        free((void*)evkv.key.data);
    }
    return new_rev;
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

int cetcd_mvcc_range(cetcd_mvcc_store *s, int64_t rev,
                      const uint8_t *key_start, size_t start_len,
                      const uint8_t *key_end, size_t end_len,
                      cetcd_kv **out, size_t *out_count) {
    if (!s || !out || !out_count) return CETCD_ERR_INVAL;
    cetcd_slice lo = cetcd_slice_make(key_start, start_len);
    cetcd_slice hi = cetcd_slice_make(key_end, end_len);
    range_ctx ctx = {0};
    cetcd_treap_range(s->index, lo, hi, range_collect_cb, &ctx);
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
