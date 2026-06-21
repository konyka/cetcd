/*
 * Watch RPC handler.
 *
 * Watch is a bidirectional streaming RPC in etcd v3:
 *   - Client sends WatchRequest (create/cancel watch)
 *   - Server sends WatchResponse (events, confirmations)
 *
 * This implementation supports two modes:
 *   1. Legacy single-shot mode: when no event loop / stream writer is
 *      registered, WatchCreateRequest returns one confirmation response
 *      and immediately cancels the watcher.
 *   2. Streaming mode: when cetcd_v3rpc_set_loop() and
 *      cetcd_v3rpc_set_stream_writer() have been called, each
 *      WatchCreateRequest spawns a coroutine that yields until MVCC
 *      events arrive, then encodes and writes WatchResponses.
 */

#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <stdint.h>

#include "cetcd/v3rpc.h"
#include "cetcd/mvcc.h"
#include "cetcd/io.h"
#include "cetcd/log.h"

extern cetcd_mvcc_store *g_rpc_store;
extern cetcd_loop       *g_rpc_loop;
extern cetcd_stream_write_fn g_rpc_stream_write_fn;
extern void             *g_rpc_stream_write_ctx;

/* Forward declaration */
cetcd_rpc_bytes watch_handle_watch(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);

/* ── Minimal protobuf varint/bytes helpers ─────────────────────────────── */

static int read_varint_w(const uint8_t *buf, size_t len, size_t *pos, uint64_t *out) {
    uint64_t val = 0; int shift = 0;
    while (*pos < len) {
        uint8_t b = buf[*pos]; (*pos)++;
        val |= (uint64_t)(b & 0x7F) << shift;
        if ((b & 0x80) == 0) { *out = val; return 0; }
        shift += 7; if (shift > 63) break;
    }
    return -1;
}

static int read_bytes_w(const uint8_t *buf, size_t len, size_t *pos,
                         uint8_t **out, size_t *out_len) {
    uint64_t l = 0;
    if (read_varint_w(buf, len, pos, &l) != 0) return -1;
    if (*pos + l > len) return -1;
    uint8_t *p = (uint8_t *)malloc((size_t)l);
    if (!p && l > 0) return -1;
    if (l > 0) memcpy(p, buf + *pos, (size_t)l);
    *pos += (size_t)l;
    *out = p;
    *out_len = (size_t)l;
    return 0;
}

static size_t write_varint_w(uint8_t *buf, size_t cap, size_t pos, uint64_t val) {
    while (pos < cap) {
        uint8_t b = val & 0x7F;
        val >>= 7;
        if (val) b |= 0x80;
        buf[pos++] = b;
        if (!val) break;
    }
    return pos;
}

/* Global watch ID counter */
static int64_t g_watch_id_counter = 1;

/* ── Event encoding (protobuf) ─────────────────────────────────────────── */

static size_t encode_event(uint8_t *buf, size_t cap, size_t pos,
                            const cetcd_watch_event *ev, int want_prev_kv) {
    if (!ev) return pos;

    /* Build KeyValue:
     *   field 1 (key)             = bytes, tag = 0x0a
     *   field 2 (create_revision) = int64, tag = 0x10
     *   field 3 (mod_revision)    = int64, tag = 0x18
     *   field 4 (version)         = int64, tag = 0x20
     *   field 5 (value)           = bytes, tag = 0x2a
     */
    uint8_t kv_buf[4096];
    size_t kpos = 0;
    kv_buf[kpos++] = 0x0a;
    kpos = write_varint_w(kv_buf, sizeof(kv_buf), kpos, (uint64_t)ev->kv.key.len);
    if (ev->kv.key.len > 0) {
        memcpy(kv_buf + kpos, ev->kv.key.data, ev->kv.key.len);
        kpos += ev->kv.key.len;
    }
    kv_buf[kpos++] = 0x10;
    kpos = write_varint_w(kv_buf, sizeof(kv_buf), kpos, (uint64_t)ev->kv.create_rev.main);
    kv_buf[kpos++] = 0x18;
    kpos = write_varint_w(kv_buf, sizeof(kv_buf), kpos, (uint64_t)ev->kv.mod_rev.main);
    kv_buf[kpos++] = 0x20;
    kpos = write_varint_w(kv_buf, sizeof(kv_buf), kpos, (uint64_t)ev->kv.version);
    if (ev->type == CETCD_EVENT_PUT && ev->kv.value.len > 0) {
        kv_buf[kpos++] = 0x2a;
        kpos = write_varint_w(kv_buf, sizeof(kv_buf), kpos, (uint64_t)ev->kv.value.len);
        memcpy(kv_buf + kpos, ev->kv.value.data, ev->kv.value.len);
        kpos += ev->kv.value.len;
    }

    /* Build Event (field 11, tag = 0x5a):
     *   field 1 (type) = enum, tag = 0x08
     *   field 2 (kv)   = KeyValue, tag = 0x12
     *   field 3 (prev_kv) = KeyValue, tag = 0x1a
     */
    uint8_t ev_buf[4200];
    size_t epos = 0;
    ev_buf[epos++] = 0x08;
    ev_buf[epos++] = (uint8_t)(ev->type == CETCD_EVENT_DELETE ? 1 : 0);
    ev_buf[epos++] = 0x12;
    uint64_t kvlen = kpos;
    do {
        uint8_t b = kvlen & 0x7F; kvlen >>= 7; if (kvlen) b |= 0x80;
        ev_buf[epos++] = b;
    } while (kvlen);
    memcpy(ev_buf + epos, kv_buf, kpos);
    epos += kpos;

    if (want_prev_kv && ev->has_prev_kv) {
        uint8_t pkv_buf[4096];
        size_t pkpos = 0;
        pkv_buf[pkpos++] = 0x0a;
        pkpos = write_varint_w(pkv_buf, sizeof(pkv_buf), pkpos,
                                (uint64_t)ev->prev_kv.key.len);
        if (ev->prev_kv.key.len > 0) {
            memcpy(pkv_buf + pkpos, ev->prev_kv.key.data, ev->prev_kv.key.len);
            pkpos += ev->prev_kv.key.len;
        }
        pkv_buf[pkpos++] = 0x10;
        pkpos = write_varint_w(pkv_buf, sizeof(pkv_buf), pkpos,
                                (uint64_t)ev->prev_kv.create_rev.main);
        pkv_buf[pkpos++] = 0x18;
        pkpos = write_varint_w(pkv_buf, sizeof(pkv_buf), pkpos,
                                (uint64_t)ev->prev_kv.mod_rev.main);
        pkv_buf[pkpos++] = 0x20;
        pkpos = write_varint_w(pkv_buf, sizeof(pkv_buf), pkpos,
                                (uint64_t)ev->prev_kv.version);
        if (ev->prev_kv.value.len > 0) {
            pkv_buf[pkpos++] = 0x2a;
            pkpos = write_varint_w(pkv_buf, sizeof(pkv_buf), pkpos,
                                    (uint64_t)ev->prev_kv.value.len);
            memcpy(pkv_buf + pkpos, ev->prev_kv.value.data, ev->prev_kv.value.len);
            pkpos += ev->prev_kv.value.len;
        }
        ev_buf[epos++] = 0x1a;
        uint64_t pklen = pkpos;
        do {
            uint8_t b = pklen & 0x7F; pklen >>= 7; if (pklen) b |= 0x80;
            ev_buf[epos++] = b;
        } while (pklen);
        memcpy(ev_buf + epos, pkv_buf, pkpos);
        epos += pkpos;
    }

    /* Append event as field 11 (tag = 0x5a, length-delimited) */
    if (pos + epos + 10 > cap) return pos;  /* best-effort safety */
    buf[pos++] = 0x5a;
    uint64_t elen = epos;
    do {
        uint8_t b = elen & 0x7F; elen >>= 7; if (elen) b |= 0x80;
        buf[pos++] = b;
    } while (elen);
    memcpy(buf + pos, ev_buf, epos);
    pos += epos;
    return pos;
}

/* Encode a WatchResponse protobuf.  Caller frees out->data with free(). */
static cetcd_rpc_bytes encode_watch_response(int64_t watch_id,
                                              int created,
                                              int canceled,
                                              const cetcd_watch_event *events,
                                              size_t event_count,
                                              int64_t current_rev,
                                              int want_prev_kv) {
    cetcd_rpc_bytes out = {NULL, 0};

    /* ResponseHeader inner: field 3 = revision */
    uint8_t hdr_inner[16]; size_t hip = 0;
    hdr_inner[hip++] = 0x18;
    hip = write_varint_w(hdr_inner, sizeof(hdr_inner), hip,
                         (uint64_t)(current_rev > 0 ? current_rev : 1));

    /* Response buffer */
    size_t cap = 256 + event_count * 8192;
    uint8_t *resp = (uint8_t *)malloc(cap);
    if (!resp) return out;
    size_t rpos = 0;

    /* field 1 = header */
    resp[rpos++] = 0x0a;
    rpos = write_varint_w(resp, cap, rpos, (uint64_t)hip);
    memcpy(resp + rpos, hdr_inner, hip); rpos += hip;

    /* field 2 = watch_id */
    resp[rpos++] = 0x10;
    rpos = write_varint_w(resp, cap, rpos, (uint64_t)watch_id);

    /* field 3 = created */
    if (created) {
        resp[rpos++] = 0x18;
        resp[rpos++] = 0x01;
    }

    /* field 4 = canceled */
    if (canceled) {
        resp[rpos++] = 0x20;
        resp[rpos++] = 0x01;
    }

    /* field 11 = repeated Event */
    for (size_t i = 0; i < event_count; i++) {
        rpos = encode_event(resp, cap, rpos, &events[i], want_prev_kv);
    }

    uint8_t *final = (uint8_t *)malloc(rpos);
    if (!final) { free(resp); return out; }
    memcpy(final, resp, rpos);
    free(resp);
    out.data = final;
    out.len = rpos;
    return out;
}

/* ── Legacy single-shot event collector ────────────────────────────────── */

typedef struct {
    uint8_t *buf;
    size_t len;
    size_t cap;
    int want_prev_kv;
} watch_event_collector;

static void watch_event_cb(const cetcd_watch_event *ev, void *udata) {
    watch_event_collector *c = (watch_event_collector *)udata;
    if (!c || !ev) return;

    if (c->len + 8192 > c->cap) {
        size_t nc = c->cap ? c->cap * 2 : 256;
        while (nc < c->len + 8192) nc *= 2;
        uint8_t *nb = (uint8_t *)realloc(c->buf, nc);
        if (!nb) return;
        c->buf = nb;
        c->cap = nc;
    }
    c->len = encode_event(c->buf, c->cap, c->len, ev, c->want_prev_kv);
}

/* ── Streaming watcher state ───────────────────────────────────────────── */

typedef struct {
    cetcd_loop *loop;
    cetcd_co   *co;
} watcher_wake_ctx;

typedef struct cetcd_stream_watcher_ctx {
    int64_t                 watch_id;
    cetcd_co               *co;
    cetcd_stream_watcher   *sw;
    cetcd_mvcc_watch_notify notify;
    watcher_wake_ctx        wake_ctx;
    int                     want_prev_kv;
    volatile int            canceled;
    struct cetcd_stream_watcher_ctx *next;
} cetcd_stream_watcher_ctx;

/* Global active streaming watchers (demux by watch_id). */
static cetcd_stream_watcher_ctx *g_stream_watchers = NULL;

static void watcher_wake_cb(void *udata) {
    watcher_wake_ctx *ctx = (watcher_wake_ctx *)udata;
    if (ctx && ctx->loop && ctx->co) {
        cetcd_loop_schedule_resume(ctx->loop, ctx->co);
    }
}

static void free_stream_watcher_ctx(cetcd_stream_watcher_ctx *wctx) {
    if (!wctx) return;
    cetcd_mvcc_watch_notify_destroy(&wctx->notify);
    /* The cetcd_co wrapper is our metadata; libco owns the internal context.
     * It is safe to free it here because the coroutine function is about to
     * return and libco does not reference this wrapper during execution. */
    free(wctx->co);
    free(wctx);
}

static cetcd_stream_watcher_ctx *find_stream_watcher(int64_t watch_id) {
    cetcd_stream_watcher_ctx *cur = g_stream_watchers;
    while (cur) {
        if (cur->watch_id == watch_id) return cur;
        cur = cur->next;
    }
    return NULL;
}

static void remove_stream_watcher(cetcd_stream_watcher_ctx *wctx) {
    cetcd_stream_watcher_ctx **pp = &g_stream_watchers;
    while (*pp) {
        if (*pp == wctx) {
            *pp = wctx->next;
            return;
        }
        pp = &(*pp)->next;
    }
}

/* Watcher coroutine: yields until MVCC events arrive, then writes responses. */
static void watcher_co_fn(void *arg) {
    cetcd_stream_watcher_ctx *wctx = (cetcd_stream_watcher_ctx *)arg;
    if (!wctx) return;

    while (!wctx->canceled) {
        cetcd_watch_event *events = NULL;
        size_t count = 0;
        int rc = cetcd_mvcc_watch_recv(&wctx->notify, &events, &count);
        if (rc == CETCD_OK && count > 0) {
            cetcd_rpc_bytes resp = encode_watch_response(
                wctx->watch_id, 0, 0,
                events, count,
                g_rpc_store ? cetcd_mvcc_revision(g_rpc_store) : 1,
                wctx->want_prev_kv);
            if (resp.data && resp.len > 0 && g_rpc_stream_write_fn) {
                g_rpc_stream_write_fn(resp.data, resp.len, g_rpc_stream_write_ctx);
            }
            cetcd_rpc_bytes_free(&resp);
            /* Free event data that was deep-copied into the notification queue. */
            for (size_t i = 0; i < count; i++) {
                free((void*)events[i].kv.key.data);
                free((void*)events[i].kv.value.data);
                if (events[i].has_prev_kv) {
                    free((void*)events[i].prev_kv.key.data);
                    free((void*)events[i].prev_kv.value.data);
                }
            }
            free(events);
        }
        /* Yield and wait for the next wake-up. */
        cetcd_co_yield();
    }

    /* Coroutine is exiting; clean up the context. */
    remove_stream_watcher(wctx);
    free_stream_watcher_ctx(wctx);
}

/* ── Request parsing ───────────────────────────────────────────────────── */

typedef struct {
    uint8_t *key;
    size_t   key_len;
    uint8_t *range_end;
    size_t   range_end_len;
    int64_t  start_rev;
    int64_t  client_watch_id;
    int      want_prev_kv;
    bool     is_create;
    bool     is_cancel;
    int64_t  cancel_id;
} watch_request_parsed;

static void parse_watch_request(const uint8_t *req, size_t req_len,
                                 watch_request_parsed *out) {
    memset(out, 0, sizeof(*out));
    size_t pos = 0;
    while (pos < req_len) {
        uint8_t tag = req[pos++];
        if (tag == 0x0a) { /* WatchCreateRequest */
            out->is_create = true;
            uint64_t clen = 0;
            if (read_varint_w(req, req_len, &pos, &clen) != 0) break;
            size_t cend = pos + (size_t)clen;
            while (pos < cend) {
                uint8_t ctag = req[pos++];
                if (ctag == 0x0a) {
                    read_bytes_w(req, cend, &pos, &out->key, &out->key_len);
                } else if (ctag == 0x12) {
                    read_bytes_w(req, cend, &pos, &out->range_end, &out->range_end_len);
                } else if (ctag == 0x18) {
                    uint64_t v = 0;
                    if (read_varint_w(req, cend, &pos, &v) == 0) out->start_rev = (int64_t)v;
                } else if (ctag == 0x30) {
                    uint64_t v = 0;
                    if (read_varint_w(req, cend, &pos, &v) == 0) out->want_prev_kv = (int)v;
                } else if (ctag == 0x38) {
                    uint64_t v = 0;
                    if (read_varint_w(req, cend, &pos, &v) == 0) out->client_watch_id = (int64_t)v;
                } else {
                    uint64_t skip = 0; read_varint_w(req, cend, &pos, &skip);
                }
            }
            pos = cend;
        } else if (tag == 0x12) { /* WatchCancelRequest */
            out->is_cancel = true;
            uint64_t clen = 0;
            if (read_varint_w(req, req_len, &pos, &clen) != 0) break;
            size_t cend = pos + (size_t)clen;
            while (pos < cend) {
                uint8_t ctag = req[pos++];
                if (ctag == 0x08) {
                    uint64_t v = 0;
                    if (read_varint_w(req, cend, &pos, &v) == 0) out->cancel_id = (int64_t)v;
                } else {
                    uint64_t skip = 0; read_varint_w(req, cend, &pos, &skip);
                }
            }
            pos = cend;
        } else {
            uint64_t skip = 0; read_varint_w(req, req_len, &pos, &skip);
        }
    }
}

static void free_watch_request_parsed(watch_request_parsed *p) {
    if (!p) return;
    if (p->key) free(p->key);
    if (p->range_end) free(p->range_end);
}

/* ── Legacy single-shot handler ────────────────────────────────────────── */

static cetcd_rpc_bytes handle_legacy_watch(const watch_request_parsed *p) {
    cetcd_rpc_bytes out = {NULL, 0};
    int64_t watch_id = 0;
    int64_t current_rev = g_rpc_store ? cetcd_mvcc_revision(g_rpc_store) : 0;

    if (p->is_cancel) {
        watch_id = p->cancel_id;
        out = encode_watch_response(watch_id, 0, 1, NULL, 0, current_rev, 0);
        return out;
    }

    if (p->is_create && p->key && g_rpc_store) {
        watch_id = (p->client_watch_id > 0) ? p->client_watch_id : g_watch_id_counter++;
        watch_event_collector collector = {NULL, 0, 0, p->want_prev_kv};
        cetcd_watcher *w = cetcd_mvcc_watch(g_rpc_store, p->key, p->key_len,
                                             p->start_rev,
                                             watch_event_cb, &collector);
        out = encode_watch_response(watch_id, 1, 0,
                                    NULL, 0,
                                    cetcd_mvcc_revision(g_rpc_store),
                                    p->want_prev_kv);
        /* Append collected events */
        if (collector.len > 0 && out.data) {
            uint8_t *merged = (uint8_t *)malloc(out.len + collector.len);
            if (merged) {
                memcpy(merged, out.data, out.len);
                memcpy(merged + out.len, collector.buf, collector.len);
                free(out.data);
                out.data = merged;
                out.len += collector.len;
            }
        }
        if (collector.buf) free(collector.buf);
        if (w) cetcd_mvcc_watch_cancel(g_rpc_store, w);
        return out;
    }

    /* Fallback: created=true with no watch_id */
    out = encode_watch_response(0, 1, 0, NULL, 0, current_rev, 0);
    return out;
}

/* ── Streaming handler ─────────────────────────────────────────────────── */

static cetcd_rpc_bytes handle_streaming_watch(const watch_request_parsed *p) {
    cetcd_rpc_bytes out = {NULL, 0};
    int64_t current_rev = g_rpc_store ? cetcd_mvcc_revision(g_rpc_store) : 0;

    if (p->is_cancel) {
        int64_t watch_id = p->cancel_id;
        cetcd_stream_watcher_ctx *wctx = find_stream_watcher(watch_id);
        if (wctx) {
            /* Unregister from MVCC first so no new events arrive. */
            if (g_rpc_store && wctx->sw) {
                cetcd_mvcc_watch_unsubscribe(g_rpc_store, wctx->sw);
                wctx->sw = NULL;
            }
            wctx->canceled = 1;
            /* Wake the coroutine so it can observe the cancel and exit. */
            watcher_wake_cb(&wctx->wake_ctx);
        }
        out = encode_watch_response(watch_id, 0, 1, NULL, 0, current_rev, 0);
        return out;
    }

    if (p->is_create && p->key && g_rpc_store) {
        int64_t watch_id = (p->client_watch_id > 0)
                           ? p->client_watch_id
                           : g_watch_id_counter++;

        cetcd_stream_watcher_ctx *wctx =
            (cetcd_stream_watcher_ctx *)calloc(1, sizeof(*wctx));
        if (!wctx) {
            out = encode_watch_response(watch_id, 1, 0, NULL, 0, current_rev, 0);
            return out;
        }
        wctx->watch_id = watch_id;
        wctx->want_prev_kv = p->want_prev_kv;
        wctx->canceled = 0;

        /* Create the coroutine (not started yet). */
        wctx->co = cetcd_co_create(g_rpc_loop, watcher_co_fn, wctx,
                                    CETCD_CO_DEFAULT_STACK_SIZE);
        if (!wctx->co) {
            free(wctx);
            out = encode_watch_response(watch_id, 1, 0, NULL, 0, current_rev, 0);
            return out;
        }

        /* Set up the notification channel. */
        wctx->wake_ctx.loop = g_rpc_loop;
        wctx->wake_ctx.co   = wctx->co;
        cetcd_mvcc_watch_notify_init(&wctx->notify,
                                      watcher_wake_cb, &wctx->wake_ctx);

        /* Subscribe with MVCC. */
        wctx->sw = cetcd_mvcc_watch_subscribe(
            g_rpc_store, watch_id,
            p->key, p->key_len,
            p->range_end, p->range_end_len,
            p->start_rev, p->want_prev_kv,
            &wctx->notify);
        if (!wctx->sw) {
            free(wctx->co);
            free(wctx);
            out = encode_watch_response(watch_id, 1, 0, NULL, 0, current_rev, 0);
            return out;
        }

        /* Link into the active watcher list. */
        wctx->next = g_stream_watchers;
        g_stream_watchers = wctx;

        /* Return the create confirmation immediately. */
        out = encode_watch_response(watch_id, 1, 0, NULL, 0, current_rev, 0);
        CETCD_INFO("watch id=%lld created in streaming mode",
                   (long long)watch_id);
        return out;
    }

    /* Fallback. */
    out = encode_watch_response(0, 1, 0, NULL, 0, current_rev, 0);
    return out;
}

/* ── Main entry point ──────────────────────────────────────────────────── */

cetcd_rpc_bytes watch_handle_watch(cetcd_v3rpc *rpc,
                                    const uint8_t *req, size_t req_len) {
    (void)rpc;
    watch_request_parsed p;
    parse_watch_request(req, req_len, &p);

    cetcd_rpc_bytes out;
    if (g_rpc_loop && g_rpc_stream_write_fn) {
        out = handle_streaming_watch(&p);
    } else {
        out = handle_legacy_watch(&p);
    }

    free_watch_request_parsed(&p);
    return out;
}
