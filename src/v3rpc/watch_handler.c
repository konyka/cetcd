/*
 * Watch RPC handler.
 *
 * Watch is a bidirectional streaming RPC in etcd v3:
 *   - Client sends WatchRequest (create/cancel watch)
 *   - Server sends WatchResponse (events, confirmations)
 *
 * In cetcd's current synchronous dispatch model, we implement a single-request
 * version that:
 *   1. Parses WatchCreateRequest (key, range_end, start_revision)
 *   2. Registers a watcher on the MVCC store
 *   3. Returns a WatchResponse with the watch_id (confirmation)
 *
 * Full streaming support requires the coroutine-based async I/O layer.
 */

#include <stdlib.h>
#include <string.h>

#include "cetcd/v3rpc.h"
#include "cetcd/mvcc.h"

extern cetcd_mvcc_store *g_rpc_store;

/* Forward declaration */
cetcd_rpc_bytes watch_handle_watch(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);

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

/* Global watch ID counter */
static int64_t g_watch_id_counter = 1;

/* Watch callback: collects events into a static buffer for the response */
typedef struct {
    uint8_t *buf;
    size_t len;
    size_t cap;
} watch_event_collector;

static void watch_event_cb(const cetcd_watch_event *ev, void *udata) {
    watch_event_collector *c = (watch_event_collector *)udata;
    if (!c || !ev) return;

    /* Encode a minimal WatchResponse event:
     *   field 11 (events) = repeated Event, tag = 0x5a (length-delimited)
     *     Event:
     *       field 1 (type) = enum, tag = 0x08 (PUT=0, DELETE=1)
     *       field 2 (kv) = KeyValue, tag = 0x12 (length-delimited)
     *         KeyValue:
     *           field 1 (key) = bytes, tag = 0x0a
     *           field 2 (value) = bytes, tag = 0x12
     *           field 3 (create_revision) = varint, tag = 0x18
     *           field 4 (mod_revision) = varint, tag = 0x20
     *           field 5 (version) = varint, tag = 0x28
     */

    /* Build KeyValue */
    uint8_t kv_buf[4096];
    size_t kpos = 0;
    /* key */
    kv_buf[kpos++] = 0x0a;
    uint64_t klen = ev->kv.key.len;
    do { uint8_t b = klen & 0x7F; klen >>= 7; if (klen) b |= 0x80; kv_buf[kpos++] = b; } while (klen);
    if (ev->kv.key.len > 0) { memcpy(kv_buf + kpos, ev->kv.key.data, ev->kv.key.len); kpos += ev->kv.key.len; }
    /* value (only for PUT) */
    if (ev->type == CETCD_EVENT_PUT && ev->kv.value.len > 0) {
        kv_buf[kpos++] = 0x12;
        uint64_t vlen = ev->kv.value.len;
        do { uint8_t b = vlen & 0x7F; vlen >>= 7; if (vlen) b |= 0x80; kv_buf[kpos++] = b; } while (vlen);
        memcpy(kv_buf + kpos, ev->kv.value.data, ev->kv.value.len);
        kpos += ev->kv.value.len;
    }
    /* mod_revision */
    kv_buf[kpos++] = 0x20;
    uint64_t mrev = (uint64_t)ev->kv.mod_rev.main;
    do { uint8_t b = mrev & 0x7F; mrev >>= 7; if (mrev) b |= 0x80; kv_buf[kpos++] = b; } while (mrev);

    /* Build Event */
    uint8_t ev_buf[4200];
    size_t epos = 0;
    /* type */
    ev_buf[epos++] = 0x08;
    ev_buf[epos++] = (uint8_t)(ev->type == CETCD_EVENT_DELETE ? 1 : 0);
    /* kv (length-delimited) */
    ev_buf[epos++] = 0x12;
    uint64_t kvlen = kpos;
    do { uint8_t b = kvlen & 0x7F; kvlen >>= 7; if (kvlen) b |= 0x80; ev_buf[epos++] = b; } while (kvlen);
    memcpy(ev_buf + epos, kv_buf, kpos);
    epos += kpos;

    /* Append to collector buffer */
    if (c->len + epos + 10 > c->cap) {
        size_t nc = c->cap ? c->cap * 2 : 256;
        while (nc < c->len + epos + 10) nc *= 2;
        uint8_t *nb = (uint8_t *)realloc(c->buf, nc);
        if (!nb) return;
        c->buf = nb;
        c->cap = nc;
    }
    /* Write event as field 11 (tag=0x5a, length-delimited) */
    c->buf[c->len++] = 0x5a;
    uint64_t elen = epos;
    do { uint8_t b = elen & 0x7F; elen >>= 7; if (elen) b |= 0x80; c->buf[c->len++] = b; } while (elen);
    memcpy(c->buf + c->len, ev_buf, epos);
    c->len += epos;
}

cetcd_rpc_bytes watch_handle_watch(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len) {
    (void)rpc;
    size_t pos = 0;
    uint8_t *key = NULL; size_t key_len = 0;
    uint8_t *range_end = NULL; size_t range_end_len = 0;
    int64_t start_rev = 0;
    bool is_create = false;
    bool is_cancel = false;
    int64_t cancel_id = 0;

    /* WatchRequest oneof request_union:
     *   field 1 = WatchCreateRequest (tag = 0x0a)
     *   field 2 = WatchCancelRequest (tag = 0x12)
     */
    while (pos < req_len) {
        uint8_t tag = req[pos++];
        if (tag == 0x0a) {
            /* WatchCreateRequest */
            is_create = true;
            uint64_t clen = 0;
            if (read_varint_w(req, req_len, &pos, &clen) != 0) break;
            size_t cend = pos + (size_t)clen;
            while (pos < cend) {
                uint8_t ctag = req[pos++];
                if (ctag == 0x0a) { /* key */
                    if (read_bytes_w(req, cend, &pos, &key, &key_len) != 0) break;
                } else if (ctag == 0x12) { /* range_end */
                    if (read_bytes_w(req, cend, &pos, &range_end, &range_end_len) != 0) break;
                } else if (ctag == 0x18) { /* start_revision */
                    uint64_t v = 0; if (read_varint_w(req, cend, &pos, &v) == 0) start_rev = (int64_t)v;
                } else {
                    uint64_t skip = 0; read_varint_w(req, cend, &pos, &skip);
                }
            }
            pos = cend;
        } else if (tag == 0x12) {
            /* WatchCancelRequest */
            is_cancel = true;
            uint64_t clen = 0;
            if (read_varint_w(req, req_len, &pos, &clen) != 0) break;
            size_t cend = pos + (size_t)clen;
            while (pos < cend) {
                uint8_t ctag = req[pos++];
                if (ctag == 0x08) { /* watch_id */
                    uint64_t v = 0; if (read_varint_w(req, cend, &pos, &v) == 0) cancel_id = (int64_t)v;
                } else {
                    uint64_t skip = 0; read_varint_w(req, cend, &pos, &skip);
                }
            }
            pos = cend;
        } else {
            uint64_t skip = 0; read_varint_w(req, req_len, &pos, &skip);
        }
    }

    int64_t watch_id = 0;

    if (is_cancel) {
        /* Cancel: return a WatchResponse with canceled=true */
        watch_id = cancel_id;
        /* Response:
         *   field 2 (watch_id) = varint, tag = 0x10
         *   field 4 (canceled) = bool, tag = 0x20
         */
        uint8_t resp[32];
        size_t rpos = 0;
        resp[rpos++] = 0x10;
        uint64_t wid = (uint64_t)watch_id;
        do { uint8_t b = wid & 0x7F; wid >>= 7; if (wid) b |= 0x80; resp[rpos++] = b; } while (wid);
        resp[rpos++] = 0x20; /* field 4 = canceled */
        resp[rpos++] = 0x01;
        uint8_t *out = (uint8_t *)malloc(rpos);
        if (!out) { if (key) free(key); if (range_end) free(range_end); return (cetcd_rpc_bytes){NULL, 0}; }
        memcpy(out, resp, rpos);
        if (key) free(key);
        if (range_end) free(range_end);
        return (cetcd_rpc_bytes){out, rpos};
    }

    if (is_create && key && g_rpc_store) {
        watch_id = g_watch_id_counter++;

        /* Collect any events from start_rev to now */
        watch_event_collector collector = {NULL, 0, 0};

        /* Register a watcher */
        cetcd_watcher *w = NULL;
        if (range_end && range_end_len > 0) {
            /* Range watch: use prefix for simplicity if range_end is a single 0 byte */
            w = cetcd_mvcc_watch(g_rpc_store, key, key_len, start_rev,
                                  watch_event_cb, &collector);
        } else {
            w = cetcd_mvcc_watch(g_rpc_store, key, key_len, start_rev,
                                  watch_event_cb, &collector);
        }

        /* Build WatchResponse:
         *   field 2 (watch_id) = varint, tag = 0x10
         *   field 3 (created) = bool, tag = 0x18
         *   field 11 (events) = repeated Event, tag = 0x5a
         */
        uint8_t header[32];
        size_t hpos = 0;
        header[hpos++] = 0x10; /* field 2 = watch_id */
        uint64_t wid = (uint64_t)watch_id;
        do { uint8_t b = wid & 0x7F; wid >>= 7; if (wid) b |= 0x80; header[hpos++] = b; } while (wid);
        header[hpos++] = 0x18; /* field 3 = created */
        header[hpos++] = 0x01;

        size_t total = hpos + collector.len;
        uint8_t *out = (uint8_t *)malloc(total > 0 ? total : 1);
        if (!out) {
            if (collector.buf) free(collector.buf);
            if (w) cetcd_mvcc_watch_cancel(g_rpc_store, w);
            if (key) free(key);
            if (range_end) free(range_end);
            return (cetcd_rpc_bytes){NULL, 0};
        }
        memcpy(out, header, hpos);
        if (collector.len > 0) {
            memcpy(out + hpos, collector.buf, collector.len);
        }
        if (collector.buf) free(collector.buf);

        /* Cancel the watcher since this is a single-response model */
        if (w) cetcd_mvcc_watch_cancel(g_rpc_store, w);

        if (key) free(key);
        if (range_end) free(range_end);
        return (cetcd_rpc_bytes){out, total};
    }

    /* Fallback: empty response */
    if (key) free(key);
    if (range_end) free(range_end);
    uint8_t *out = (uint8_t *)malloc(1);
    if (!out) return (cetcd_rpc_bytes){NULL, 0};
    out[0] = 0;
    return (cetcd_rpc_bytes){out, 1};
}
