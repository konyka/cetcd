/*
 * Watch RPC handler.
 *
 * Watch is a bidirectional streaming RPC in etcd v3:
 *   - Client sends WatchRequest (create/cancel watch)
 *   - Server sends WatchResponse (events, confirmations)
 *
 * This implementation supports two modes:
 *   1. Legacy single-shot mode: when no stream writer is registered,
 *      WatchCreateRequest returns one confirmation response and immediately
 *      cancels the watcher.
 *   2. Streaming mode: when cetcd_v3rpc_set_stream_writer() has been called,
 *      each WatchCreateRequest subscribes to MVCC events via a notification
 *      channel. When events arrive, a direct callback encodes and writes
 *      WatchResponses to the client stream — no coroutines needed.
 */

#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <stdint.h>

#include "cetcd/v3rpc.h"
#include "cetcd/mvcc.h"
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
     *   field 6 (lease)           = int64, tag = 0x30
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
    if (ev->kv.lease_id > 0 && kpos + 12 < sizeof(kv_buf)) {
        kv_buf[kpos++] = 0x30;
        kpos = write_varint_w(kv_buf, sizeof(kv_buf), kpos, (uint64_t)ev->kv.lease_id);
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
        if (ev->prev_kv.lease_id > 0 && pkpos + 12 < sizeof(pkv_buf)) {
            pkv_buf[pkpos++] = 0x30;
            pkpos = write_varint_w(pkv_buf, sizeof(pkv_buf), pkpos,
                                    (uint64_t)ev->prev_kv.lease_id);
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

/* Encode a WatchResponse protobuf.  Caller frees out->data with free().
 * compact_revision > 0 emits field 5 (tag 0x28); used when start_rev is compacted. */
static cetcd_rpc_bytes encode_watch_response(int64_t watch_id,
                                              int created,
                                              int canceled,
                                              const cetcd_watch_event *events,
                                              size_t event_count,
                                              int64_t current_rev,
                                              int want_prev_kv,
                                              int64_t compact_revision) {
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

    /* field 5 = compact_revision */
    if (compact_revision > 0) {
        resp[rpos++] = 0x28;
        rpos = write_varint_w(resp, cap, rpos, (uint64_t)compact_revision);
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

/* start_rev > 0 and below compacted_rev → cannot create watch (etcd ErrCompacted). */
static int watch_start_rev_compacted_(int64_t start_rev, int64_t *compact_rev_out) {
    if (start_rev <= 0 || !g_rpc_store) return 0;
    int64_t cr = cetcd_mvcc_compacted_revision(g_rpc_store);
    if (cr > 0 && start_rev < cr) {
        if (compact_rev_out) *compact_rev_out = cr;
        return 1;
    }
    return 0;
}

/* ── Legacy single-shot event collector ────────────────────────────────── */

typedef struct {
    uint8_t *buf;
    size_t len;
    size_t cap;
    int want_prev_kv;
    int filter_noput;
    int filter_nodelete;
} watch_event_collector;

static void watch_event_cb(const cetcd_watch_event *ev, void *udata) {
    watch_event_collector *c = (watch_event_collector *)udata;
    if (!c || !ev) return;

    /* Apply filters: NOPUT filters out PUT events, NODELETE filters out DELETE events */
    if (c->filter_noput && ev->type == CETCD_EVENT_PUT) return;
    if (c->filter_nodelete && ev->type == CETCD_EVENT_DELETE) return;

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

typedef struct cetcd_stream_watcher_ctx {
    int64_t                 watch_id;
    int64_t                 start_rev; /* WatchCreate start_revision (0 = from now) */
    cetcd_stream_watcher   *sw;
    cetcd_mvcc_watch_notify notify;
    int                     want_prev_kv;
    int                     filter_noput;
    int                     filter_nodelete;
    int                     want_progress_notify;
    int                     ticks_since_progress; /* 100ms ticks since last progress */
    volatile int            canceled;
    /* Per-connection writer captured at WatchCreate (not the global). */
    cetcd_stream_write_fn   write_fn;
    void                   *write_ctx;
    struct cetcd_stream_watcher_ctx *next;
} cetcd_stream_watcher_ctx;

/* Global active streaming watchers (demux by watch_id). */
static cetcd_stream_watcher_ctx *g_stream_watchers = NULL;

static void free_stream_watcher_ctx(cetcd_stream_watcher_ctx *wctx) {
    if (!wctx) return;
    cetcd_mvcc_watch_notify_destroy(&wctx->notify);
    free(wctx);
}

/* Direct callback: called from notify_push when MVCC events arrive.
 * Encodes the events as a WatchResponse and writes it to the client stream
 * immediately — no coroutine needed. */
static void streaming_watch_notify_cb(void *udata) {
    cetcd_stream_watcher_ctx *wctx = (cetcd_stream_watcher_ctx *)udata;
    if (!wctx || wctx->canceled) return;

    cetcd_watch_event *events = NULL;
    size_t count = 0;
    int rc = cetcd_mvcc_watch_recv(&wctx->notify, &events, &count);
    if (rc != CETCD_OK || count == 0) return;

    /* Apply watch filters: NOPUT and NODELETE */
    size_t send_count = 0;
    for (size_t i = 0; i < count; i++) {
        if (wctx->filter_noput && events[i].type == CETCD_EVENT_PUT) continue;
        if (wctx->filter_nodelete && events[i].type == CETCD_EVENT_DELETE) continue;
        if (send_count != i) events[send_count] = events[i];
        send_count++;
    }
    if (send_count > 0) {
        cetcd_rpc_bytes resp = encode_watch_response(
            wctx->watch_id, 0, 0,
            events, send_count,
            g_rpc_store ? cetcd_mvcc_revision(g_rpc_store) : 1,
            wctx->want_prev_kv, 0);
        if (resp.data && resp.len > 0 && wctx->write_fn) {
            wctx->write_fn(resp.data, resp.len, wctx->write_ctx);
        }
        cetcd_rpc_bytes_free(&resp);
    }
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

/* ── Request parsing ───────────────────────────────────────────────────── */

typedef struct {
    uint8_t *key;
    size_t   key_len;
    uint8_t *range_end;
    size_t   range_end_len;
    int64_t  start_rev;
    int64_t  client_watch_id;
    int      want_prev_kv;
    int      want_progress_notify; /* field 4 (bool, tag 0x20) */
    int      filter_noput;         /* field 5 filter: NOPUT=0 */
    int      filter_nodelete;      /* field 5 filter: NODELETE=1 */
    int      fragment;             /* field 8 (bool, tag 0x40) */
    bool     is_create;
    bool     is_cancel;
    bool     is_progress;          /* WatchProgressRequest (field 3, tag 0x1a) */
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
                } else if (ctag == 0x20) {
                    /* field 4 = progress_notify (bool) */
                    uint64_t v = 0;
                    if (read_varint_w(req, cend, &pos, &v) == 0) out->want_progress_notify = (int)v;
                } else if (ctag == 0x28) {
                    /* field 5 = filters (repeated enum, non-packed: NOPUT=0, NODELETE=1) */
                    uint64_t v = 0;
                    if (read_varint_w(req, cend, &pos, &v) == 0) {
                        if (v == 0) out->filter_noput = 1;
                        else if (v == 1) out->filter_nodelete = 1;
                    }
                } else if (ctag == 0x2a) {
                    /* field 5 = filters (packed varint) */
                    uint64_t flen = 0;
                    if (read_varint_w(req, cend, &pos, &flen) == 0) {
                        size_t fend = pos + (size_t)flen;
                        while (pos < fend) {
                            uint64_t fv = 0;
                            if (read_varint_w(req, fend, &pos, &fv) == 0) {
                                if (fv == 0) out->filter_noput = 1;
                                else if (fv == 1) out->filter_nodelete = 1;
                            }
                        }
                    }
                } else if (ctag == 0x30) {
                    uint64_t v = 0;
                    if (read_varint_w(req, cend, &pos, &v) == 0) out->want_prev_kv = (int)v;
                } else if (ctag == 0x38) {
                    uint64_t v = 0;
                    if (read_varint_w(req, cend, &pos, &v) == 0) out->client_watch_id = (int64_t)v;
                } else if (ctag == 0x40) {
                    /* field 8 = fragment (bool) */
                    uint64_t v = 0;
                    if (read_varint_w(req, cend, &pos, &v) == 0) out->fragment = (int)v;
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
        } else if (tag == 0x1a) { /* WatchProgressRequest (empty message) */
            out->is_progress = true;
            uint64_t clen = 0;
            if (read_varint_w(req, req_len, &pos, &clen) != 0) break;
            pos += (size_t)clen;
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
        out = encode_watch_response(watch_id, 0, 1, NULL, 0, current_rev, 0, 0);
        return out;
    }

    if (p->is_create && p->key && g_rpc_store) {
        watch_id = (p->client_watch_id > 0) ? p->client_watch_id : g_watch_id_counter++;
        int64_t compact_rev = 0;
        if (watch_start_rev_compacted_(p->start_rev, &compact_rev)) {
            return encode_watch_response(watch_id, 1, 1, NULL, 0,
                                         current_rev, 0, compact_rev);
        }
        watch_event_collector collector = {NULL, 0, 0, p->want_prev_kv, p->filter_noput, p->filter_nodelete};
        cetcd_watcher *w = cetcd_mvcc_watch(g_rpc_store, p->key, p->key_len,
                                             p->start_rev,
                                             watch_event_cb, &collector);
        out = encode_watch_response(watch_id, 1, 0,
                                    NULL, 0,
                                    cetcd_mvcc_revision(g_rpc_store),
                                    p->want_prev_kv, 0);
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
    out = encode_watch_response(0, 1, 0, NULL, 0, current_rev, 0, 0);
    return out;
}

/* ── Streaming handler ─────────────────────────────────────────────────── */

static cetcd_rpc_bytes handle_streaming_watch(const watch_request_parsed *p) {
    cetcd_rpc_bytes out = {NULL, 0};
    int64_t current_rev = g_rpc_store ? cetcd_mvcc_revision(g_rpc_store) : 0;

    if (p->is_progress) {
        /* Client-requested progress: notify all watchers on this connection. */
        cetcd_stream_watcher_ctx *cur = g_stream_watchers;
        while (cur) {
            if (!cur->canceled && cur->write_fn &&
                cur->write_ctx == g_rpc_stream_write_ctx) {
                cetcd_rpc_bytes prog = encode_watch_response(
                    cur->watch_id, 0, 0, NULL, 0, current_rev, 0, 0);
                if (prog.data && prog.len > 0) {
                    cur->write_fn(prog.data, prog.len, cur->write_ctx);
                    cur->ticks_since_progress = 0;
                }
                cetcd_rpc_bytes_free(&prog);
            }
            cur = cur->next;
        }
        /* Unary ack is empty; progress frames already streamed. */
        return out;
    }

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
            /* Remove and free the watcher context. */
            remove_stream_watcher(wctx);
            free_stream_watcher_ctx(wctx);
        }
        out = encode_watch_response(watch_id, 0, 1, NULL, 0, current_rev, 0, 0);
        return out;
    }

    if (p->is_create && p->key && g_rpc_store) {
        int64_t watch_id = (p->client_watch_id > 0)
                           ? p->client_watch_id
                           : g_watch_id_counter++;

        int64_t compact_rev = 0;
        if (watch_start_rev_compacted_(p->start_rev, &compact_rev)) {
            return encode_watch_response(watch_id, 1, 1, NULL, 0,
                                         current_rev, 0, compact_rev);
        }

        cetcd_stream_watcher_ctx *wctx =
            (cetcd_stream_watcher_ctx *)calloc(1, sizeof(*wctx));
        if (!wctx) {
            out = encode_watch_response(watch_id, 1, 0, NULL, 0, current_rev, 0, 0);
            return out;
        }
        wctx->watch_id = watch_id;
        wctx->start_rev = p->start_rev;
        wctx->want_prev_kv = p->want_prev_kv;
        wctx->filter_noput = p->filter_noput;
        wctx->filter_nodelete = p->filter_nodelete;
        wctx->want_progress_notify = p->want_progress_notify;
        wctx->ticks_since_progress = 0;
        wctx->canceled = 0;
        /* Bind to the connection that issued this WatchCreate. */
        wctx->write_fn = g_rpc_stream_write_fn;
        wctx->write_ctx = g_rpc_stream_write_ctx;

        /* Set up the notification channel with direct callback. */
        cetcd_mvcc_watch_notify_init(&wctx->notify,
                                      streaming_watch_notify_cb, wctx);

        /* Subscribe with MVCC. */
        wctx->sw = cetcd_mvcc_watch_subscribe(
            g_rpc_store, watch_id,
            p->key, p->key_len,
            p->range_end, p->range_end_len,
            p->start_rev, p->want_prev_kv,
            &wctx->notify);
        if (!wctx->sw) {
            free(wctx);
            out = encode_watch_response(watch_id, 1, 0, NULL, 0, current_rev, 0, 0);
            return out;
        }

        /* Link into the active watcher list. */
        wctx->next = g_stream_watchers;
        g_stream_watchers = wctx;

        /* Return the create confirmation immediately. */
        out = encode_watch_response(watch_id, 1, 0, NULL, 0, current_rev, 0, 0);
        CETCD_INFO("watch id=%lld created in streaming mode",
                   (long long)watch_id);
        return out;
    }

    /* Fallback. */
    out = encode_watch_response(0, 1, 0, NULL, 0, current_rev, 0, 0);
    return out;
}

/* ── Main entry point ──────────────────────────────────────────────────── */

cetcd_rpc_bytes watch_handle_watch(cetcd_v3rpc *rpc,
                                    const uint8_t *req, size_t req_len) {
    (void)rpc;
    watch_request_parsed p;
    parse_watch_request(req, req_len, &p);

    cetcd_rpc_bytes out;
    if (g_rpc_stream_write_fn) {
        out = handle_streaming_watch(&p);
    } else {
        out = handle_legacy_watch(&p);
    }

    free_watch_request_parsed(&p);
    return out;
}

void cetcd_v3rpc_detach_stream_writer(void *write_ctx) {
    if (!write_ctx) return;
    cetcd_stream_watcher_ctx *cur = g_stream_watchers;
    while (cur) {
        cetcd_stream_watcher_ctx *next = cur->next;
        if (cur->write_ctx == write_ctx) {
            if (g_rpc_store && cur->sw) {
                cetcd_mvcc_watch_unsubscribe(g_rpc_store, cur->sw);
                cur->sw = NULL;
            }
            cur->canceled = 1;
            cur->write_fn = NULL;
            cur->write_ctx = NULL;
            remove_stream_watcher(cur);
            free_stream_watcher_ctx(cur);
        }
        cur = next;
    }
}

/* Periodic progress for watchers created with progress_notify=true.
 * Called from the server 100ms tick; emit about every 10s (100 ticks). */
#define CETCD_WATCH_PROGRESS_TICKS 100

void cetcd_v3rpc_watch_tick(void) {
    int64_t current_rev = g_rpc_store ? cetcd_mvcc_revision(g_rpc_store) : 0;
    cetcd_stream_watcher_ctx *cur = g_stream_watchers;
    while (cur) {
        cetcd_stream_watcher_ctx *next = cur->next;
        if (!cur->canceled && cur->want_progress_notify && cur->write_fn) {
            cur->ticks_since_progress++;
            if (cur->ticks_since_progress >= CETCD_WATCH_PROGRESS_TICKS) {
                cetcd_rpc_bytes prog = encode_watch_response(
                    cur->watch_id, 0, 0, NULL, 0, current_rev, 0, 0);
                if (prog.data && prog.len > 0)
                    cur->write_fn(prog.data, prog.len, cur->write_ctx);
                cetcd_rpc_bytes_free(&prog);
                cur->ticks_since_progress = 0;
            }
        }
        cur = next;
    }
}

void cetcd_v3rpc_watch_cancel_compacted(int64_t compact_rev) {
    if (compact_rev <= 0) return;
    int64_t current_rev = g_rpc_store ? cetcd_mvcc_revision(g_rpc_store) : 0;
    cetcd_stream_watcher_ctx *cur = g_stream_watchers;
    while (cur) {
        cetcd_stream_watcher_ctx *next = cur->next;
        if (!cur->canceled && cur->write_fn &&
            cur->start_rev > 0 && cur->start_rev < compact_rev) {
            cetcd_rpc_bytes cancel = encode_watch_response(
                cur->watch_id, 0, 1, NULL, 0, current_rev, 0, compact_rev);
            if (cancel.data && cancel.len > 0)
                cur->write_fn(cancel.data, cancel.len, cur->write_ctx);
            cetcd_rpc_bytes_free(&cancel);

            if (g_rpc_store && cur->sw) {
                cetcd_mvcc_watch_unsubscribe(g_rpc_store, cur->sw);
                cur->sw = NULL;
            }
            cur->canceled = 1;
            remove_stream_watcher(cur);
            free_stream_watcher_ctx(cur);
        }
        cur = next;
    }
}
