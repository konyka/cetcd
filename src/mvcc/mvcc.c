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
};


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
    s->main_rev++;
    cetcd_revision new_rev = {s->main_rev, 0};

    cetcd_slice kslice = cetcd_slice_make(key, key_len);
    cetcd_slice vslice = cetcd_slice_make(val, val_len);
    void *existing = NULL;
    key_generation *kg;

    /* Capture previous KV for watch events before modifying */
    cetcd_kv prev_evkv;
    memset(&prev_evkv, 0, sizeof(prev_evkv));
    int has_prev = 0;

    if (cetcd_treap_get(s->index, kslice, &existing)) {
        kg = (key_generation*)existing;
        /* Save old value for prev_kv before freeing */
        if (!kg->deleted) {
            prev_evkv.key = dup_slice(kslice);
            prev_evkv.value = dup_slice(kg->value);
            prev_evkv.create_rev = kg->create_rev;
            prev_evkv.mod_rev = kg->mod_rev;
            prev_evkv.version = kg->version;
            prev_evkv.lease_id = kg->lease_id;
            has_prev = 1;
        }
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
    mvcc_notify_watchers(s, &evkv, &new_rev, CETCD_EVENT_PUT,
                         has_prev ? &prev_evkv : NULL);
    free((void*)evkv.key.data);
    free((void*)evkv.value.data);
    if (has_prev) {
        free((void*)prev_evkv.key.data);
        free((void*)prev_evkv.value.data);
    }
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

        /* Capture prev_kv for delete event BEFORE marking deleted */
        cetcd_kv prev_evkv;
        memset(&prev_evkv, 0, sizeof(prev_evkv));
        int has_prev = 0;
        if (!kg->deleted) {
            prev_evkv.key = dup_slice(kslice);
            prev_evkv.value = dup_slice(kg->value);
            prev_evkv.create_rev = kg->create_rev;
            prev_evkv.mod_rev = kg->mod_rev;
            prev_evkv.version = kg->version;
            prev_evkv.lease_id = kg->lease_id;
            has_prev = 1;
        }

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
        mvcc_notify_watchers(s, &evkv, &new_rev, CETCD_EVENT_DELETE,
                             has_prev ? &prev_evkv : NULL);
        free((void*)evkv.key.data);
        if (has_prev) {
            free((void*)prev_evkv.key.data);
            free((void*)prev_evkv.value.data);
        }
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
