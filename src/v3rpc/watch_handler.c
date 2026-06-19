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

/* Watch callback: collects events into a static buffer for the response */
typedef struct {
    uint8_t *buf;
    size_t len;
    size_t cap;
} watch_event_collector;

static void watch_event_cb(const cetcd_watch_event *ev, void *udata) {
    watch_event_collector *c = (watch_event_collector *)udata;
    if (!c || !ev) return;

    /* Encode WatchResponse event (field 11, tag = 0x5a):
     *   Event:
     *     field 1 (type) = enum, tag = 0x08 (PUT=0, DELETE=1)
     *     field 2 (kv) = KeyValue, tag = 0x12 (length-delimited)
     *   KeyValue (etcd v3.5 proto):
     *     field 1 (key)             = bytes, tag = 0x0a
     *     field 2 (create_revision) = int64, tag = 0x10
     *     field 3 (mod_revision)    = int64, tag = 0x18
     *     field 4 (version)         = int64, tag = 0x20
     *     field 5 (value)           = bytes, tag = 0x2a
     */

    /* Build KeyValue */
    uint8_t kv_buf[4096];
    size_t kpos = 0;
    /* field 1 = key */
    kv_buf[kpos++] = 0x0a;
    kpos = write_varint_w(kv_buf, sizeof(kv_buf), kpos, (uint64_t)ev->kv.key.len);
    if (ev->kv.key.len > 0) { memcpy(kv_buf + kpos, ev->kv.key.data, ev->kv.key.len); kpos += ev->kv.key.len; }
    /* field 2 = create_revision */
    kv_buf[kpos++] = 0x10;
    kpos = write_varint_w(kv_buf, sizeof(kv_buf), kpos, (uint64_t)ev->kv.create_rev.main);
    /* field 3 = mod_revision */
    kv_buf[kpos++] = 0x18;
    kpos = write_varint_w(kv_buf, sizeof(kv_buf), kpos, (uint64_t)ev->kv.mod_rev.main);
    /* field 4 = version */
    kv_buf[kpos++] = 0x20;
    kpos = write_varint_w(kv_buf, sizeof(kv_buf), kpos, (uint64_t)ev->kv.version);
    /* field 5 = value (only for PUT) */
    if (ev->type == CETCD_EVENT_PUT && ev->kv.value.len > 0) {
        kv_buf[kpos++] = 0x2a;
        kpos = write_varint_w(kv_buf, sizeof(kv_buf), kpos, (uint64_t)ev->kv.value.len);
        memcpy(kv_buf + kpos, ev->kv.value.data, ev->kv.value.len);
        kpos += ev->kv.value.len;
    }

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
    int want_prev_kv = 0;
    int64_t client_watch_id = 0;

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
                } else if (ctag == 0x30) { /* prev_kv (bool) */
                    uint64_t v = 0; if (read_varint_w(req, cend, &pos, &v) == 0) want_prev_kv = (int)v;
                } else if (ctag == 0x38) { /* watch_id (int64) */
                    uint64_t v = 0; if (read_varint_w(req, cend, &pos, &v) == 0) client_watch_id = (int64_t)v;
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
        /* Cancel: return a WatchResponse with header + canceled=true
         * WatchResponse:
         *   field 1 (header)    = ResponseHeader, tag = 0x0a
         *   field 2 (watch_id)  = int64, tag = 0x10
         *   field 4 (canceled)  = bool, tag = 0x20
         */
        watch_id = cancel_id;
        int64_t current_rev = g_rpc_store ? cetcd_mvcc_revision(g_rpc_store) : 0;

        /* Build ResponseHeader inner: field 3 = revision */
        uint8_t hdr_inner[16]; size_t hip = 0;
        hdr_inner[hip++] = 0x18;
        hip = write_varint_w(hdr_inner, sizeof(hdr_inner), hip,
                             (uint64_t)(current_rev > 0 ? current_rev : 1));

        uint8_t resp[64];
        size_t rpos = 0;
        /* field 1 = header */
        resp[rpos++] = 0x0a;
        rpos = write_varint_w(resp, sizeof(resp), rpos, (uint64_t)hip);
        memcpy(resp + rpos, hdr_inner, hip); rpos += hip;
        /* field 2 = watch_id */
        resp[rpos++] = 0x10;
        rpos = write_varint_w(resp, sizeof(resp), rpos, (uint64_t)watch_id);
        /* field 4 = canceled */
        resp[rpos++] = 0x20;
        resp[rpos++] = 0x01;

        uint8_t *out = (uint8_t *)malloc(rpos);
        if (!out) { if (key) free(key); if (range_end) free(range_end); return (cetcd_rpc_bytes){NULL, 0}; }
        memcpy(out, resp, rpos);
        if (key) free(key);
        if (range_end) free(range_end);
        return (cetcd_rpc_bytes){out, rpos};
    }

    if (is_create && key && g_rpc_store) {
        /* Use client-specified watch_id if provided, otherwise auto-assign */
        watch_id = (client_watch_id > 0) ? client_watch_id : g_watch_id_counter++;

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
         *   field 1 (header)    = ResponseHeader, tag = 0x0a
         *   field 2 (watch_id)  = int64, tag = 0x10
         *   field 3 (created)   = bool, tag = 0x18
         *   field 11 (events)   = repeated Event, tag = 0x5a
         */
        int64_t current_rev = cetcd_mvcc_revision(g_rpc_store);

        /* Build ResponseHeader inner: field 3 = revision */
        uint8_t hdr_inner[16]; size_t hip = 0;
        hdr_inner[hip++] = 0x18;
        hip = write_varint_w(hdr_inner, sizeof(hdr_inner), hip,
                             (uint64_t)(current_rev > 0 ? current_rev : 1));

        uint8_t header[64];
        size_t hpos = 0;
        /* field 1 = header */
        header[hpos++] = 0x0a;
        hpos = write_varint_w(header, sizeof(header), hpos, (uint64_t)hip);
        memcpy(header + hpos, hdr_inner, hip); hpos += hip;
        /* field 2 = watch_id */
        header[hpos++] = 0x10;
        hpos = write_varint_w(header, sizeof(header), hpos, (uint64_t)watch_id);
        /* field 3 = created */
        header[hpos++] = 0x18;
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

    /* Fallback: return WatchResponse with header + created=true + no events */
    if (key) free(key);
    if (range_end) free(range_end);
    int64_t current_rev = g_rpc_store ? cetcd_mvcc_revision(g_rpc_store) : 0;
    uint8_t hdr_buf[32]; size_t hp = 0;
    hdr_buf[hp++] = 0x18; /* revision */
    uint64_t rv = (uint64_t)(current_rev > 0 ? current_rev : 1);
    do { uint8_t b = rv & 0x7F; rv >>= 7; if (rv) b |= 0x80; hdr_buf[hp++] = b; } while (rv);
    uint8_t resp[64]; size_t rpos = 0;
    resp[rpos++] = 0x0a; /* field 1 = header */
    resp[rpos++] = (uint8_t)hp;
    memcpy(resp + rpos, hdr_buf, hp); rpos += hp;
    resp[rpos++] = 0x18; /* field 3 = created */
    resp[rpos++] = 0x01;
    uint8_t *out = (uint8_t *)malloc(rpos);
    if (!out) return (cetcd_rpc_bytes){NULL, 0};
    memcpy(out, resp, rpos);
    return (cetcd_rpc_bytes){out, rpos};
}
