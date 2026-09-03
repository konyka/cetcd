#include "cetcd/server.h"
#include "cetcd/v3rpc.h"
#include "cetcd/raft.h"
#include "cetcd/mvcc.h"
#include "cetcd/lease.h"
#include "cetcd/auth.h"
#include "cetcd/peer.h"
#include "cetcd/snap.h"
#include "cetcd/wal.h"
#include "cetcd/backend.h"
#include "cetcd/io.h"
#include "cetcd/metrics.h"
#include "cetcd/log.h"
#include "cetcd/tls.h"
#include "cetcd/http2.h"

#include <stdlib.h>
#include <string.h>
#include <stddef.h>
#include <errno.h>
#include <uv.h>
#include "io_internal.h"

#if defined(_WIN32)
#  define WIN32_LEAN_AND_MEAN
#  include <winsock2.h>
#  include <ws2tcpip.h>
#  include <direct.h>
#  include <sys/stat.h>
#  define cetcd_mkdir(path) _mkdir(path)
#  define cetcd_close_socket(fd) closesocket(fd)
#  define CETCD_MSG_NOSIGNAL 0
#else
#  include <sys/stat.h>
#  include <unistd.h>
#  include <sys/socket.h>
#  include <netinet/in.h>
#  include <netinet/tcp.h>
#  include <arpa/inet.h>
#  include <fcntl.h>
#  define cetcd_mkdir(path) mkdir(path, 0755)
#  define cetcd_close_socket(fd) close(fd)
#  define CETCD_MSG_NOSIGNAL MSG_NOSIGNAL
#endif

struct cetcd_server {
    cetcd_server_config  cfg;
    cetcd_v3rpc         *rpc;
    cetcd_raft          *raft;
    cetcd_cluster       *cluster;
    cetcd_backend       *backend;
    cetcd_wal_encoder   *wal_enc;
    cetcd_loop          *loop;
    cetcd_tcp           *listener;
    cetcd_tcp           *peer_listener;
    uv_tcp_t             metrics_listener;
    cetcd_timer         *tick_timer;
    cetcd_metrics       *metrics;
    int                  peer_fd;
    bool                 started;
    bool                 metrics_listener_init;
    /* Nonblocking per-peer outbound transport (loop-thread only). */
    struct peer_tx_     *peer_txs;
    uint32_t             n_peer_txs;
    uint64_t             peer_drops;
    uint64_t             last_snap_index;
    cetcd_tls_ctx       *tls_client;
    cetcd_tls_ctx       *tls_peer;
    cetcd_tls_ctx       *tls_peer_out;
};

static void raft_tick_cb_(void *arg);
static void process_ready_(cetcd_server *srv);
static void persist_applied_(cetcd_backend *be, uint64_t idx) {
    if (!be) return;
    uint8_t buf[8];
    for (int i = 0; i < 8; i++)
        buf[i] = (uint8_t)((idx >> (8 * i)) & 0xFF);
    cetcd_backend_put(be, "meta",
                      (const uint8_t *)"applied_index", 13,
                      buf, 8);
}

static uint64_t load_applied_(cetcd_backend *be) {
    if (!be) return 0;
    uint8_t *val = NULL;
    size_t vlen = 0;
    if (cetcd_backend_get(be, "meta",
                          (const uint8_t *)"applied_index", 13,
                          &val, &vlen) != 0 || vlen != 8) {
        free(val);
        return 0;
    }
    uint64_t idx = 0;
    for (int i = 0; i < 8; i++)
        idx |= (uint64_t)val[i] << (8 * i);
    free(val);
    return idx;
}

static void apply_committed_(cetcd_server *srv) {
    if (!srv || !srv->raft) return;
    uint64_t from = cetcd_raft_applied(srv->raft) + 1;
    uint64_t to = cetcd_raft_committed(srv->raft);
    if (to < from) return;
    for (uint64_t i = from; i <= to; i++) {
        const cetcd_entry *e = cetcd_raft_entry_at(srv->raft, i);
        if (!e || e->type != CETCD_ENTRY_NORMAL) continue;
        cetcd_raft_set_applying(srv->raft, i);
        if (e->data.data && e->data.len > 0)
            cetcd_v3rpc_apply_entry(e->data.data, e->data.len);
        cetcd_raft_set_applying(srv->raft, 0);
    }
    persist_applied_(srv->backend, to);
}

static void replay_wal_(cetcd_server *srv, const char *wal_path) {
    if (!srv || !srv->raft || !wal_path) return;
    cetcd_wal_decoder *dec = cetcd_wal_decoder_open(wal_path);
    if (!dec) return;
    cetcd_hard_state last_hs;
    memset(&last_hs, 0, sizeof(last_hs));
    int have_hs = 0;
    cetcd_wal_record rec;
    cetcd_wal_record_init(&rec);
    while (cetcd_wal_decode(dec, &rec) == 0) {
        if (rec.type == CETCD_WAL_ENTRY && rec.data && rec.data_len > 0) {
            cetcd_entry e;
            if (cetcd_wal_decode_entry(rec.data, rec.data_len, &e) == 0) {
                cetcd_raft_restore_entry(srv->raft, &e);
                free((void *)(uintptr_t)e.data.data);
            }
        } else if (rec.type == CETCD_WAL_STATE && rec.data && rec.data_len > 0) {
            if (cetcd_wal_decode_hard_state(rec.data, rec.data_len, &last_hs) == 0)
                have_hs = 1;
        } else if (rec.type == CETCD_WAL_SNAPSHOT && rec.data && rec.data_len > 0) {
            uint64_t idx = 0, term = 0;
            if (cetcd_wal_decode_snapshot(rec.data, rec.data_len, &idx, &term) == 0
                && idx > 0) {
                if (cetcd_raft_compact(srv->raft, idx, term) == 0)
                    srv->last_snap_index = idx;
            }
        }
        cetcd_wal_record_free(&rec);
        cetcd_wal_record_init(&rec);
    }
    cetcd_wal_record_free(&rec);
    cetcd_wal_decoder_free(dec);
    if (have_hs)
        cetcd_raft_restore_hard_state(srv->raft, &last_hs);
}

static void ready_flush_cb_(void *arg) {
    process_ready_((cetcd_server *)arg);
}

static void maybe_campaign_single_(cetcd_server *srv) {
    if (!srv || !srv->raft) return;
    if (srv->cfg.n_initial_peers != 0) return;
    if (cetcd_raft_voter_count(srv->raft) > 1) return;
    if (cetcd_raft_state(srv->raft) == CETCD_NODE_LEADER) return;
    cetcd_msg hup;
    memset(&hup, 0, sizeof(hup));
    hup.type = CETCD_MSG_HUP;
    hup.from = srv->cfg.node_id;
    cetcd_raft_step(srv->raft, &hup);
}

static void peer_send_cb_(uint64_t to_id, const uint8_t *data, size_t len, void *udata);
static void on_peer_incoming_(cetcd_tcp *server, cetcd_tcp *client, void *arg);
static void on_client_conn_(cetcd_tcp *server, cetcd_tcp *client, void *arg);
static void lease_expire_cb_(cetcd_lease_id id,
                              const uint8_t *const *keys,
                              const size_t *key_lens,
                              size_t count,
                              void *udata);

/* --- Streaming watch support --- */

/* --- Nonblocking per-peer outbound transport ----------------------------
 *
 * Runs entirely on the libuv loop thread (peer_send_cb_ is invoked from
 * process_ready_, which runs in the loop), so the state below needs no
 * locking. Each peer owns a uv_tcp_t connection that is lazily established
 * on the first outbound frame and reconnected after failures. Frames are
 * heap-owned until their uv_write callback fires.
 *
 * Backpressure: the outbound queue is capped at CETCD_PEER_SENDQ_MAX. On
 * overflow the new frame is dropped and counted (peer_drops). This is safe
 * for Raft: unacked log entries are retransmitted by the next MsgApp, and a
 * peer stuck at the cap is a snapshot candidate anyway. Framing is never
 * corrupted (each frame is whole and length-prefixed). */

#define CETCD_PEER_SENDQ_MAX 256

typedef enum {
    PEER_TX_IDLE = 0,
    PEER_TX_CONNECTING,
    PEER_TX_HANDSHAKE,
    PEER_TX_CONNECTED,
    PEER_TX_CLOSING,
} peer_tx_state_;

typedef struct peer_out_frame_ {
    struct peer_out_frame_ *next;
    uint8_t                *data;
    size_t                  len;
} peer_out_frame_;

typedef struct peer_tx_ {
    cetcd_server     *srv;
    uint64_t          id;
    uv_tcp_t          tcp;
    uv_connect_t      connect_req;
    uv_write_t        write_req;
    peer_out_frame_  *head;
    peer_out_frame_  *tail;
    peer_out_frame_  *writing;
    uint32_t          depth;
    peer_tx_state_    state;
    bool              tcp_init;
    bool              write_inflight;
    bool              shutting_down;
    cetcd_tls_conn   *tls;
    int               tls_ready;
} peer_tx_;

static void on_peer_tx_connect_(uv_connect_t *req, int status);
static void on_peer_tx_write_(uv_write_t *req, int status);
static void on_peer_tx_close_(uv_handle_t *handle);
static void on_peer_tx_alloc_(uv_handle_t *handle, size_t suggested_size, uv_buf_t *buf);
static void on_peer_tx_read_(uv_stream_t *stream, ssize_t nread, const uv_buf_t *buf);
static void peer_tx_drain_(peer_tx_ *tx);
static void peer_tx_close_(peer_tx_ *tx);
static int  tls_flush_uv_(uv_stream_t *stream, cetcd_tls_conn *tls);

static peer_tx_ *peer_tx_get_(cetcd_server *srv, uint64_t id) {
    for (uint32_t i = 0; i < srv->n_peer_txs; i++) {
        if (srv->peer_txs[i].id == id) return &srv->peer_txs[i];
    }
    if (srv->n_peer_txs >= CETCD_MAX_INITIAL_PEERS) return NULL;
    const cetcd_peer_info *pi = cetcd_cluster_get_peer(srv->cluster, id);
    if (!pi) return NULL;
    peer_tx_ *tx = &srv->peer_txs[srv->n_peer_txs];
    memset(tx, 0, sizeof(*tx));
    tx->srv = srv;
    tx->id = id;
    tx->state = PEER_TX_IDLE;
    srv->n_peer_txs++;
    return tx;
}

static void peer_tx_connect_(peer_tx_ *tx) {
    if (tx->state != PEER_TX_IDLE || tx->shutting_down) return;
    if (!tx->tcp_init) {
        if (uv_tcp_init(cetcd_loop_uv(tx->srv->loop), &tx->tcp) != 0) return;
        tx->tcp.data = tx;
        tx->tcp_init = true;
    }
    const cetcd_peer_info *pi = cetcd_cluster_get_peer(tx->srv->cluster, tx->id);
    if (!pi) return;
    struct sockaddr_in sa;
    memset(&sa, 0, sizeof(sa));
    sa.sin_family = AF_INET;
    sa.sin_port = htons(pi->port);
    if (inet_pton(AF_INET, pi->addr, &sa.sin_addr) != 1) return;
    tx->connect_req.data = tx;
    if (uv_tcp_connect(&tx->connect_req, &tx->tcp, (const struct sockaddr *)&sa,
                       on_peer_tx_connect_) == 0) {
        tx->state = PEER_TX_CONNECTING;
    }
}

static int tls_take_pending_(cetcd_tls_conn *tls, uint8_t **out, size_t *out_len) {
    uint8_t *acc = NULL;
    size_t len = 0, cap = 0;
    for (;;) {
        uint8_t tmp[16384];
        int n = cetcd_tls_pending_out(tls, tmp, sizeof(tmp));
        if (n < 0) { free(acc); return -1; }
        if (n == 0) break;
        if (len + (size_t)n > cap) {
            size_t nc = cap ? cap * 2 : 32768;
            while (nc < len + (size_t)n) nc *= 2;
            uint8_t *nb = (uint8_t *)realloc(acc, nc);
            if (!nb) { free(acc); return -1; }
            acc = nb;
            cap = nc;
        }
        memcpy(acc + len, tmp, (size_t)n);
        len += (size_t)n;
    }
    *out = acc;
    *out_len = len;
    return 0;
}

static void peer_tx_drain_(peer_tx_ *tx) {
    if (tx->write_inflight || tx->state != PEER_TX_CONNECTED || !tx->head) return;
    peer_out_frame_ *f = tx->head;
    tx->head = f->next;
    if (!tx->head) tx->tail = NULL;
    tx->writing = f;
    tx->write_inflight = true;
    tx->write_req.data = tx;
    if (tx->tls) {
        int w = cetcd_tls_write(tx->tls, f->data, f->len);
        size_t flen = f->len;
        free(f->data); free(f); tx->writing = NULL;
        if (w < 0 || (size_t)w != flen) {
            tx->write_inflight = false;
            peer_tx_close_(tx);
            return;
        }
        uint8_t *cipher = NULL;
        size_t clen = 0;
        if (tls_take_pending_(tx->tls, &cipher, &clen) < 0) {
            tx->write_inflight = false;
            peer_tx_close_(tx);
            return;
        }
        if (clen == 0) {
            tx->write_inflight = false;
            peer_tx_drain_(tx);
            return;
        }
        peer_out_frame_ *cf = (peer_out_frame_ *)malloc(sizeof(*cf));
        if (!cf) {
            free(cipher);
            tx->write_inflight = false;
            peer_tx_close_(tx);
            return;
        }
        cf->next = NULL;
        cf->data = cipher;
        cf->len = clen;
        tx->writing = cf;
        uv_buf_t wb = uv_buf_init((char *)cipher, (unsigned int)clen);
        if (uv_write(&tx->write_req, (uv_stream_t *)&tx->tcp, &wb, 1, on_peer_tx_write_) != 0) {
            tx->write_inflight = false;
            free(cipher); free(cf); tx->writing = NULL;
            peer_tx_close_(tx);
        }
        return;
    }
    uv_buf_t wb = uv_buf_init((char *)f->data, (unsigned int)f->len);
    if (uv_write(&tx->write_req, (uv_stream_t *)&tx->tcp, &wb, 1, on_peer_tx_write_) != 0) {
        tx->write_inflight = false;
        free(f->data); free(f); tx->writing = NULL;
        peer_tx_close_(tx);
    }
}

static void peer_tx_close_(peer_tx_ *tx) {
    if (tx->state == PEER_TX_CLOSING || !tx->tcp_init) {
        tx->state = PEER_TX_IDLE;
        return;
    }
    tx->state = PEER_TX_CLOSING;
    uv_close((uv_handle_t *)&tx->tcp, on_peer_tx_close_);
}

static void on_peer_tx_alloc_(uv_handle_t *handle, size_t suggested_size, uv_buf_t *buf) {
    (void)handle;
    size_t cap = suggested_size > 0 ? suggested_size : 16384;
    char *slab = (char *)malloc(cap);
    if (!slab) { buf->base = NULL; buf->len = 0; return; }
    *buf = uv_buf_init(slab, (unsigned int)cap);
}

static void on_peer_tx_read_(uv_stream_t *stream, ssize_t nread, const uv_buf_t *buf) {
    peer_tx_ *tx = stream ? (peer_tx_ *)stream->data : NULL;
    if (!tx) { if (buf->base) free(buf->base); return; }
    if (nread <= 0) {
        if (nread < 0) peer_tx_close_(tx);
        if (buf->base) free(buf->base);
        return;
    }
    if (!tx->tls) { if (buf->base) free(buf->base); return; }
    int rc = cetcd_tls_feed(tx->tls, buf->base, (size_t)nread);
    if (buf->base) free(buf->base);
    if (rc != CETCD_OK) { peer_tx_close_(tx); return; }
    if (!tx->tls_ready) {
        int hs = cetcd_tls_handshake(tx->tls);
        if (tls_flush_uv_((uv_stream_t *)&tx->tcp, tx->tls) < 0 || hs < 0) {
            peer_tx_close_(tx);
            return;
        }
        if (hs == 0) return;
        tx->tls_ready = 1;
        tx->state = PEER_TX_CONNECTED;
        peer_tx_drain_(tx);
    }
    for (;;) {
        uint8_t tmp[1024];
        int n = cetcd_tls_read(tx->tls, tmp, sizeof(tmp));
        if (n < 0) { peer_tx_close_(tx); return; }
        if (n == 0) break;
    }
}

static void on_peer_tx_connect_(uv_connect_t *req, int status) {
    peer_tx_ *tx = (peer_tx_ *)req->data;
    if (!tx) return;
    if (tx->shutting_down) { peer_tx_close_(tx); return; }
    if (status != 0) { peer_tx_close_(tx); return; }
    if (tx->srv->tls_peer_out) {
        tx->tls = cetcd_tls_conn_connect(tx->srv->tls_peer_out);
        if (!tx->tls) { peer_tx_close_(tx); return; }
        if (uv_read_start((uv_stream_t *)&tx->tcp, on_peer_tx_alloc_, on_peer_tx_read_) != 0) {
            peer_tx_close_(tx);
            return;
        }
        tx->state = PEER_TX_HANDSHAKE;
        int hs = cetcd_tls_handshake(tx->tls);
        if (tls_flush_uv_((uv_stream_t *)&tx->tcp, tx->tls) < 0 || hs < 0) {
            peer_tx_close_(tx);
            return;
        }
        if (hs == 1) {
            tx->tls_ready = 1;
            tx->state = PEER_TX_CONNECTED;
            peer_tx_drain_(tx);
        }
        return;
    }
    tx->state = PEER_TX_CONNECTED;
    peer_tx_drain_(tx);
}

static void on_peer_tx_write_(uv_write_t *req, int status) {
    peer_tx_ *tx = (peer_tx_ *)req->data;
    if (!tx) return;
    if (tx->writing) { free(tx->writing->data); free(tx->writing); tx->writing = NULL; }
    tx->write_inflight = false;
    if (status < 0 || tx->shutting_down) {
        peer_tx_close_(tx);
        return;
    }
    peer_tx_drain_(tx);
}

static void on_peer_tx_close_(uv_handle_t *handle) {
    peer_tx_ *tx = (peer_tx_ *)handle->data;
    if (!tx) return;
    cetcd_tls_conn_free(tx->tls);
    tx->tls = NULL;
    tx->tls_ready = 0;
    tx->state = PEER_TX_IDLE;
    tx->tcp_init = false;
    if (!tx->shutting_down && tx->head) peer_tx_connect_(tx);
}

static void peer_tx_shutdown_all_(cetcd_server *srv) {
    if (!srv || !srv->peer_txs) return;
    for (uint32_t i = 0; i < srv->n_peer_txs; i++) {
        peer_tx_ *tx = &srv->peer_txs[i];
        tx->shutting_down = true;
        peer_out_frame_ *f = tx->head;
        while (f) {
            peer_out_frame_ *n = f->next;
            free(f->data); free(f);
            f = n;
        }
        tx->head = tx->tail = NULL;
        tx->depth = 0;
        if (tx->tcp_init && tx->state != PEER_TX_CLOSING) {
            tx->state = PEER_TX_CLOSING;
            uv_close((uv_handle_t *)&tx->tcp, on_peer_tx_close_);
        }
    }
}

static void peer_tx_free_all_(cetcd_server *srv) {
    if (!srv) return;
    free(srv->peer_txs);
    srv->peer_txs = NULL;
    srv->n_peer_txs = 0;
}

static void lease_expire_cb_(cetcd_lease_id id,
                              const uint8_t *const *keys,
                              const size_t *key_lens,
                              size_t count,
                              void *udata) {
    (void)id;
    cetcd_server *srv = (cetcd_server *)udata;
    if (!srv || !srv->rpc) return;
    /* Only the leader proposes; followers apply the same deletes from Raft. */
    if (srv->raft && cetcd_raft_state(srv->raft) != CETCD_NODE_LEADER)
        return;
    if (!keys || !key_lens || count == 0) return;
    (void)cetcd_v3rpc_propose_deletes(keys, key_lens, count);
}

/* Cleanup callback for uv_write in stream_write_ */
static void stream_write_cleanup_(uv_write_t *req, int status) {
    (void)status;
    if (req->data) free(req->data);
    free(req);
}

static int tls_flush_uv_(uv_stream_t *stream, cetcd_tls_conn *tls) {
    if (!stream || !tls) return -1;
    for (;;) {
        uint8_t tmp[16384];
        int n = cetcd_tls_pending_out(tls, tmp, sizeof(tmp));
        if (n < 0) return -1;
        if (n == 0) return 0;
        uint8_t *copy = (uint8_t *)malloc((size_t)n);
        if (!copy) return -1;
        memcpy(copy, tmp, (size_t)n);
        uv_write_t *wr = (uv_write_t *)calloc(1, sizeof(uv_write_t));
        if (!wr) { free(copy); return -1; }
        wr->data = copy;
        uv_buf_t wbuf = uv_buf_init((char *)copy, (unsigned int)n);
        if (uv_write(wr, stream, &wbuf, 1, stream_write_cleanup_) != 0) {
            free(copy);
            free(wr);
            return -1;
        }
    }
}

static void client_uv_send_(uv_stream_t *stream, uint8_t *frame, size_t total);

/* Write a WatchResponse frame to a client socket.
 * The frame format matches the gRPC-like framing:
 *   2B path_len (BE) + path + 1B compressed + 4B payload_len (BE) + payload */
static void client_stream_write_(const uint8_t *data, size_t len, void *ctx) {
    uv_stream_t *stream = (uv_stream_t *)ctx;
    if (!stream || !data || len == 0) return;

    static const char watch_path[] = "/etcdserverpb.Watch/Watch";
    uint16_t plen = (uint16_t)(sizeof(watch_path) - 1);
    uint32_t payload_len = (uint32_t)len;

    /* Allocate frame on heap since uv_write is async */
    size_t total = 2 + plen + 5 + len;
    uint8_t *frame = (uint8_t *)malloc(total);
    if (!frame) return;
    size_t pos = 0;
    frame[pos++] = (uint8_t)(plen >> 8);
    frame[pos++] = (uint8_t)(plen & 0xFF);
    memcpy(frame + pos, watch_path, plen);
    pos += plen;
    frame[pos++] = 0; /* compressed = false */
    frame[pos++] = (uint8_t)((payload_len >> 24) & 0xFF);
    frame[pos++] = (uint8_t)((payload_len >> 16) & 0xFF);
    frame[pos++] = (uint8_t)((payload_len >> 8) & 0xFF);
    frame[pos++] = (uint8_t)(payload_len & 0xFF);
    memcpy(frame + pos, data, len);

    client_uv_send_(stream, frame, total);
}

static void on_metrics_connection_(uv_stream_t *server, int status);
static void on_metrics_alloc_(uv_handle_t *handle, size_t suggested_size, uv_buf_t *buf);
static void on_metrics_read_(uv_stream_t *stream, ssize_t nread, const uv_buf_t *buf);
static void on_metrics_write_(uv_write_t *req, int status);
static void on_metrics_close_(uv_handle_t *handle);

typedef struct client_ctx_ {
    cetcd_server     *srv;
    cetcd_tcp        *client;
    cetcd_tls_conn   *tls;
    int               tls_ready;
    uint8_t          *buf;
    size_t            buf_cap;
    size_t            buf_pos;
    int               proto; /* 0 unknown, 1 custom TCP, 2 HTTP/2 */
    cetcd_h2_session *h2;
    uv_stream_t      *h2_stream;
    int32_t           h2_sid;
    char              h2_path[256];
    uint8_t          *h2_body;
    size_t            h2_body_len;
    size_t            h2_body_cap;
    int               h2_done;
    int               h2_fail;
} client_ctx_;

static uint64_t client_max_bytes_(const client_ctx_ *ctx) {
    uint64_t maxb = ctx && ctx->srv ? ctx->srv->cfg.max_request_bytes : 0;
    return maxb ? maxb : CETCD_DEFAULT_MAX_REQUEST_BYTES;
}

/* Grow the client read buffer up to max-request-bytes. -1 → close. */
static int client_buf_append_(client_ctx_ *ctx, const uint8_t *src, size_t n) {
    if (!ctx || !src) return -1;
    if (n == 0) return 0;
    uint64_t maxb = client_max_bytes_(ctx);
    if ((uint64_t)ctx->buf_pos + n > maxb) return -1;
    size_t need = ctx->buf_pos + n;
    if (need > ctx->buf_cap) {
        size_t cap = ctx->buf_cap ? ctx->buf_cap : 4096;
        while (cap < need) {
            if (cap > SIZE_MAX / 2) return -1;
            cap *= 2;
        }
        if ((uint64_t)cap > maxb) cap = (size_t)maxb;
        if (need > cap) return -1;
        uint8_t *nb = (uint8_t *)realloc(ctx->buf, cap);
        if (!nb) return -1;
        ctx->buf = nb;
        ctx->buf_cap = cap;
    }
    memcpy(ctx->buf + ctx->buf_pos, src, n);
    ctx->buf_pos += n;
    return 0;
}

static void client_uv_send_(uv_stream_t *stream, uint8_t *frame, size_t total) {
    client_ctx_ *ctx = stream ? (client_ctx_ *)stream->data : NULL;
    if (ctx && ctx->tls) {
        int w = cetcd_tls_write(ctx->tls, frame, total);
        free(frame);
        if (w < 0 || (size_t)w != total) return;
        (void)tls_flush_uv_(stream, ctx->tls);
        return;
    }
    uv_write_t *wr = (uv_write_t *)calloc(1, sizeof(uv_write_t));
    if (wr) {
        wr->data = frame;
        uv_buf_t wbuf = uv_buf_init((char *)frame, (unsigned int)total);
        if (uv_write(wr, stream, &wbuf, 1, stream_write_cleanup_) != 0) {
            free(frame); free(wr);
        }
    } else {
        free(frame);
    }
}

static void client_close_cb_(uv_handle_t *handle) {
    /* Detach any Watch streams bound to this connection before freeing. */
    cetcd_v3rpc_detach_stream_writer(handle);
    client_ctx_ *ctx = (client_ctx_ *)handle->data;
    if (ctx) {
        cetcd_tls_conn_free(ctx->tls);
        cetcd_h2_session_free(ctx->h2);
        free(ctx->h2_body);
        free(ctx->buf);
        free(ctx);
    }
}

typedef struct metrics_conn_ctx_ {
    cetcd_server *srv;
    uv_tcp_t      client;
    char          req[4096];
    size_t        req_len;
    cetcd_buf_t   resp;
    uv_write_t    write_req;
    int           pprof_seconds;  /* for /debug/pprof/profile?seconds=N */
    uv_work_t     pprof_work;
    cetcd_buf_t   pprof_body;
    int           pprof_rc;
    int           pprof_pending;
    int           pprof_abandoned;
} metrics_conn_ctx_;

static void on_metrics_close_(uv_handle_t *handle) {
    metrics_conn_ctx_ *ctx = (metrics_conn_ctx_ *)handle->data;
    if (!ctx) return;
    cetcd_buf_free(&ctx->resp);
    cetcd_buf_free(&ctx->pprof_body);
    free(ctx);
}

static void on_metrics_write_(uv_write_t *req, int status) {
    (void)status;
    metrics_conn_ctx_ *ctx = (metrics_conn_ctx_ *)req->data;
    if (!ctx) return;
    uv_close((uv_handle_t *)&ctx->client, on_metrics_close_);
}

static void metrics_send_response_(metrics_conn_ctx_ *ctx, int code,
                                    const char *status_text,
                                    const char *content_type,
                                    const uint8_t *body, size_t body_len) {
    cetcd_buf_free(&ctx->resp);
    cetcd_buf_init(&ctx->resp);
    cetcd_buf_printf(&ctx->resp,
        "HTTP/1.1 %d %s\r\n"
        "Content-Type: %s\r\n"
        "Content-Length: %zu\r\n"
        "Connection: close\r\n"
        "\r\n",
        code, status_text, content_type, body_len);
    if (body && body_len > 0) {
        cetcd_buf_append(&ctx->resp, body, body_len);
    }
    ctx->write_req.data = ctx;
    uv_buf_t wbuf = uv_buf_init((char *)ctx->resp.data, (unsigned int)ctx->resp.len);
    uv_write(&ctx->write_req, (uv_stream_t *)&ctx->client, &wbuf, 1, on_metrics_write_);
}

static void metrics_pprof_work_(uv_work_t *req) {
    metrics_conn_ctx_ *ctx = (metrics_conn_ctx_ *)req->data;
    if (!ctx) return;
    cetcd_buf_init(&ctx->pprof_body);
    ctx->pprof_rc = cetcd_pprof_profile_render(&ctx->pprof_body, ctx->pprof_seconds);
}

static void metrics_pprof_after_(uv_work_t *req, int status) {
    metrics_conn_ctx_ *ctx = (metrics_conn_ctx_ *)req->data;
    if (!ctx) return;
    ctx->pprof_pending = 0;
    if (status == UV_ECANCELED || ctx->pprof_abandoned) {
        cetcd_buf_free(&ctx->pprof_body);
        uv_close((uv_handle_t *)&ctx->client, on_metrics_close_);
        return;
    }
    if (ctx->pprof_rc == CETCD_ERR_EXISTS) {
        cetcd_buf_free(&ctx->pprof_body);
        const char *msg = "Conflict\n";
        metrics_send_response_(ctx, 409, "Conflict",
                               "text/plain",
                               (const uint8_t *)msg, strlen(msg));
        return;
    }
    if (ctx->pprof_rc != 0) {
        cetcd_buf_free(&ctx->pprof_body);
        const char *msg = "Internal Server Error\n";
        metrics_send_response_(ctx, 500, "Internal Server Error",
                               "text/plain",
                               (const uint8_t *)msg, strlen(msg));
        return;
    }
    metrics_send_response_(ctx, 200, "OK", "text/plain",
                           ctx->pprof_body.data, ctx->pprof_body.len);
    cetcd_buf_free(&ctx->pprof_body);
}

static void metrics_serve_metrics_(metrics_conn_ctx_ *ctx) {
    cetcd_buf_t body;
    cetcd_buf_init(&body);
    int rc = cetcd_metrics_render(ctx->srv->metrics, &body);
    if (rc != 0) {
        cetcd_buf_free(&body);
        const char *msg = "Internal Server Error\n";
        metrics_send_response_(ctx, 500, "Internal Server Error",
                               "text/plain",
                               (const uint8_t *)msg, strlen(msg));
        return;
    }
    metrics_send_response_(ctx, 200, "OK",
                           "text/plain; version=0.0.4; charset=utf-8",
                           body.data, body.len);
    cetcd_buf_free(&body);
}

static int metrics_parse_request_(metrics_conn_ctx_ *ctx) {
    /* Minimal parser: find the end of the request line. */
    char *end = NULL;
    size_t i;
    for (i = 0; i + 1 < ctx->req_len; i++) {
        if (ctx->req[i] == '\r' && ctx->req[i + 1] == '\n') {
            end = ctx->req + i;
            break;
        }
        if (ctx->req[i] == '\n' && (i == 0 || ctx->req[i - 1] != '\r')) {
            end = ctx->req + i;
            break;
        }
    }
    if (!end) return 0; /* need more data */

    /* Request line format: METHOD PATH HTTP/VERSION */
    char *space1 = strchr(ctx->req, ' ');
    if (!space1 || space1 >= end) return -1;
    char *space2 = strchr(space1 + 1, ' ');
    if (!space2 || space2 >= end) return -1;

    size_t path_len = (size_t)(space2 - space1 - 1);
    const char *path = space1 + 1;

    if (path_len == 8 && memcmp(path, "/metrics", 8) == 0) {
        return 1;  /* /metrics */
    }
    if (path_len >= 20 && memcmp(path, "/debug/pprof/profile", 20) == 0) {
        /* Parse ?seconds=N query parameter */
        ctx->pprof_seconds = 30;  /* etcd default */
        if (path_len > 20 && path[20] == '?') {
            const char *qs = path + 21;
            size_t qs_len = path_len - 21;
            if (qs_len > 8 && memcmp(qs, "seconds=", 8) == 0) {
                int secs = atoi(qs + 8);
                if (secs > 0 && secs <= 300) ctx->pprof_seconds = secs;
            }
        }
        return 3;  /* /debug/pprof/profile */
    }
    if (path_len == 18 && memcmp(path, "/debug/pprof/heap", 18) == 0) {
        return 4;  /* /debug/pprof/heap */
    }
    if (path_len == 24 && memcmp(path, "/debug/pprof/coroutines", 24) == 0) {
        return 5;  /* /debug/pprof/coroutines */
    }
    return 2; /* not found */
}

static void on_metrics_read_(uv_stream_t *stream, ssize_t nread, const uv_buf_t *buf) {
    metrics_conn_ctx_ *ctx = (metrics_conn_ctx_ *)stream->data;
    if (!ctx) {
        if (buf->base) free(buf->base);
        return;
    }

    if (nread <= 0) {
        if (buf->base) free(buf->base);
        if (nread < 0) {
            if (ctx->pprof_pending) {
                ctx->pprof_abandoned = 1;
                (void)uv_cancel((uv_req_t *)&ctx->pprof_work);
                return;
            }
            uv_close((uv_handle_t *)&ctx->client, on_metrics_close_);
        }
        return;
    }

    if (ctx->req_len + (size_t)nread > sizeof(ctx->req)) {
        if (buf->base) free(buf->base);
        uv_close((uv_handle_t *)&ctx->client, on_metrics_close_);
        return;
    }
    memcpy(ctx->req + ctx->req_len, buf->base, (size_t)nread);
    ctx->req_len += (size_t)nread;
    if (buf->base) free(buf->base);

    int parsed = metrics_parse_request_(ctx);
    if (parsed == 0) {
        /* Need more data; keep reading. */
        return;
    }
    if (parsed < 0) {
        const char *msg = "Bad Request\n";
        metrics_send_response_(ctx, 400, "Bad Request",
                               "text/plain",
                               (const uint8_t *)msg, strlen(msg));
        return;
    }
    if (parsed == 1) {
        metrics_serve_metrics_(ctx);
    } else if (parsed == 3) {
        /* /debug/pprof/profile — collect off the uv loop so Raft is not stalled. */
        uv_read_stop(stream);
        ctx->pprof_work.data = ctx;
        ctx->pprof_pending = 1;
        ctx->pprof_abandoned = 0;
        if (uv_queue_work(cetcd_loop_uv(ctx->srv->loop), &ctx->pprof_work,
                          metrics_pprof_work_, metrics_pprof_after_) != 0) {
            ctx->pprof_pending = 0;
            const char *msg = "Internal Server Error\n";
            metrics_send_response_(ctx, 500, "Internal Server Error",
                                   "text/plain",
                                   (const uint8_t *)msg, strlen(msg));
        }
    } else if (parsed == 4) {
        /* /debug/pprof/heap */
        cetcd_buf_t body;
        cetcd_buf_init(&body);
        int rc = cetcd_pprof_heap_render(&body);
        if (rc != 0) {
            cetcd_buf_free(&body);
            const char *msg = "Internal Server Error\n";
            metrics_send_response_(ctx, 500, "Internal Server Error",
                                   "text/plain",
                                   (const uint8_t *)msg, strlen(msg));
        } else {
            metrics_send_response_(ctx, 200, "OK", "text/plain",
                                   body.data, body.len);
            cetcd_buf_free(&body);
        }
    } else if (parsed == 5) {
        /* /debug/pprof/coroutines */
        cetcd_buf_t body;
        cetcd_buf_init(&body);
        int rc = cetcd_pprof_coroutines_render(&body);
        if (rc != 0) {
            cetcd_buf_free(&body);
            const char *msg = "Internal Server Error\n";
            metrics_send_response_(ctx, 500, "Internal Server Error",
                                   "text/plain",
                                   (const uint8_t *)msg, strlen(msg));
        } else {
            metrics_send_response_(ctx, 200, "OK", "text/plain",
                                   body.data, body.len);
            cetcd_buf_free(&body);
        }
    } else {
        const char *msg = "Not Found\n";
        metrics_send_response_(ctx, 404, "Not Found",
                               "text/plain",
                               (const uint8_t *)msg, strlen(msg));
    }
}

static void on_metrics_alloc_(uv_handle_t *handle, size_t suggested_size, uv_buf_t *buf) {
    (void)handle;
    size_t cap = suggested_size > 0 ? suggested_size : 4096;
    char *slab = (char *)malloc(cap);
    if (!slab) {
        buf->base = NULL;
        buf->len  = 0;
        return;
    }
    *buf = uv_buf_init(slab, (unsigned int)cap);
}

static void on_metrics_connection_(uv_stream_t *server, int status) {
    if (status < 0) return;
    cetcd_server *srv = (cetcd_server *)server->data;
    if (!srv) return;

    metrics_conn_ctx_ *ctx = (metrics_conn_ctx_ *)calloc(1, sizeof(*ctx));
    if (!ctx) return;
    ctx->srv = srv;
    cetcd_buf_init(&ctx->resp);

    uv_loop_t *loop = cetcd_loop_uv(srv->loop);
    uv_tcp_init(loop, &ctx->client);
    if (uv_accept(server, (uv_stream_t *)&ctx->client) != 0) {
        uv_close((uv_handle_t *)&ctx->client, on_metrics_close_);
        return;
    }
    ctx->client.data = ctx;
    uv_read_start((uv_stream_t *)&ctx->client, on_metrics_alloc_, on_metrics_read_);
}

static int h2_write_uv_(const uint8_t *buf, size_t len, void *arg) {
    uv_stream_t *stream = (uv_stream_t *)arg;
    if (!stream || !buf || len == 0) return 0;
    uint8_t *copy = (uint8_t *)malloc(len);
    if (!copy) return -1;
    memcpy(copy, buf, len);
    client_uv_send_(stream, copy, len);
    return 0;
}

static int client_h2_body_append_(client_ctx_ *ctx, const uint8_t *data, size_t len) {
    if (!ctx || len == 0) return 0;
    uint64_t maxb = client_max_bytes_(ctx);
    if ((uint64_t)ctx->h2_body_len + len > maxb) return -1;
    size_t need = ctx->h2_body_len + len;
    if (need > ctx->h2_body_cap) {
        size_t cap = ctx->h2_body_cap ? ctx->h2_body_cap : 4096;
        while (cap < need) {
            if (cap > SIZE_MAX / 2) return -1;
            cap *= 2;
        }
        uint8_t *nb = (uint8_t *)realloc(ctx->h2_body, cap);
        if (!nb) return -1;
        ctx->h2_body = nb;
        ctx->h2_body_cap = cap;
    }
    memcpy(ctx->h2_body + ctx->h2_body_len, data, len);
    ctx->h2_body_len += len;
    return 0;
}

static void client_h2_finish_(client_ctx_ *ctx) {
    if (!ctx || ctx->h2_done || !ctx->h2) return;
    ctx->h2_done = 1;

    const char *grpc_status = "0";
    uint8_t *grpc_out = NULL;
    size_t grpc_out_len = 0;
    uint8_t *msg = NULL;
    size_t msg_len = 0;

    if (ctx->h2_path[0] == '\0') {
        grpc_status = "3";
    } else if (ctx->h2_body_len > 0) {
        bool compressed = false;
        if (cetcd_grpc_decode(ctx->h2_body, ctx->h2_body_len,
                              &compressed, &msg, &msg_len) != CETCD_OK) {
            grpc_status = "3";
        } else if (compressed) {
            free(msg);
            msg = NULL;
            msg_len = 0;
            grpc_status = "12";
        }
    }

    if (grpc_status[0] == '0') {
        const char *token = cetcd_h2_req_authorization(ctx->h2);
        if (token && token[0] == '\0') token = NULL;
        /* dispatch_ex rejects NULL req_data; empty gRPC messages decode to NULL. */
        static const uint8_t empty_req = 0;
        const uint8_t *req = msg ? msg : &empty_req;
        cetcd_server_rpc_result resp = cetcd_server_handle_rpc_ex(
            ctx->srv, ctx->h2_path, req, msg_len, token);
        if (!resp.data || resp.len == 0) {
            grpc_status = "2";
        } else if (cetcd_grpc_encode(resp.data, resp.len, false,
                                     &grpc_out, &grpc_out_len) != CETCD_OK) {
            grpc_status = "2";
        }
        cetcd_server_rpc_result_free(&resp);
    }
    free(msg);

    const char *hdrs[] = {
        ":status", "200",
        "content-type", "application/grpc",
    };
    cetcd_h2_submit_response(ctx->h2, ctx->h2_sid, hdrs, 4,
                             grpc_out, grpc_out_len, false);
    /* Flush DATA before trailers; nghttp2 will not emit both if queued together. */
    if (ctx->h2_stream)
        (void)cetcd_h2_send_pending(ctx->h2, h2_write_uv_, ctx->h2_stream);
    const char *tr[] = { "grpc-status", grpc_status, "grpc-message", "" };
    cetcd_h2_submit_trailers(ctx->h2, ctx->h2_sid, tr, 4);
    free(grpc_out);
    ctx->h2_body_len = 0;
}

static void client_h2_on_request_(cetcd_h2_session *sess, int32_t stream_id,
                                  const char *method, const char *path,
                                  const char *content_type, void *udata) {
    (void)sess;
    (void)method;
    (void)content_type;
    client_ctx_ *ctx = (client_ctx_ *)udata;
    if (!ctx) return;
    ctx->h2_sid = stream_id;
    ctx->h2_path[0] = '\0';
    if (path) {
        size_t n = strlen(path);
        if (n >= sizeof(ctx->h2_path)) n = sizeof(ctx->h2_path) - 1;
        memcpy(ctx->h2_path, path, n);
        ctx->h2_path[n] = '\0';
    }
    ctx->h2_body_len = 0;
    ctx->h2_done = 0;
}

static void client_h2_on_data_(cetcd_h2_session *sess, int32_t stream_id,
                               const uint8_t *data, size_t len,
                               bool end_stream, void *udata) {
    (void)sess;
    (void)stream_id;
    client_ctx_ *ctx = (client_ctx_ *)udata;
    if (!ctx) return;
    if (data && len > 0 && client_h2_body_append_(ctx, data, len) != 0) {
        ctx->h2_fail = 1;
        return;
    }
    if (end_stream) client_h2_finish_(ctx);
}

static int client_h2_start_(client_ctx_ *ctx) {
    cetcd_h2_callbacks cbs;
    memset(&cbs, 0, sizeof(cbs));
    cbs.on_request = client_h2_on_request_;
    cbs.on_data = client_h2_on_data_;
    cbs.udata = ctx;
    ctx->h2 = cetcd_h2_session_new(&cbs);
    return ctx->h2 ? 0 : -1;
}

static void on_client_read_(uv_stream_t *stream, ssize_t nread, const uv_buf_t *buf) {
    client_ctx_ *ctx = (client_ctx_ *)stream->data;
    if (!ctx) return;
    if (nread <= 0) {
        if (nread < 0) uv_close((uv_handle_t *)stream, client_close_cb_);
        if (buf->base) free(buf->base);
        return;
    }
    if (ctx->tls) {
        int rc = cetcd_tls_feed(ctx->tls, buf->base, (size_t)nread);
        if (buf->base) free(buf->base);
        if (rc != CETCD_OK) {
            uv_close((uv_handle_t *)stream, client_close_cb_);
            return;
        }
        if (!ctx->tls_ready) {
            int hs = cetcd_tls_handshake(ctx->tls);
            if (tls_flush_uv_(stream, ctx->tls) < 0 || hs < 0) {
                uv_close((uv_handle_t *)stream, client_close_cb_);
                return;
            }
            if (hs == 0) return;
            ctx->tls_ready = 1;
        }
        for (;;) {
            uint8_t tmp[4096];
            int n = cetcd_tls_read(ctx->tls, tmp, sizeof(tmp));
            if (n < 0) {
                uv_close((uv_handle_t *)stream, client_close_cb_);
                return;
            }
            if (n == 0) break;
            if (client_buf_append_(ctx, tmp, (size_t)n) != 0) {
                uv_close((uv_handle_t *)stream, client_close_cb_);
                return;
            }
        }
    } else {
        if (client_buf_append_(ctx, (const uint8_t *)buf->base, (size_t)nread) != 0) {
            if (buf->base) free(buf->base);
            uv_close((uv_handle_t *)stream, client_close_cb_);
            return;
        }
        if (buf->base) free(buf->base);
    }

    if (ctx->proto == 0) {
        int d = cetcd_h2_detect(ctx->buf, ctx->buf_pos);
        if (d < 0) return;
        if (d == 1) {
            if (client_h2_start_(ctx) != 0) {
                uv_close((uv_handle_t *)stream, client_close_cb_);
                return;
            }
            ctx->proto = 2;
        } else {
            ctx->proto = 1;
        }
    }
    if (ctx->proto == 2) {
        if (cetcd_h2_feed(ctx->h2, ctx->buf, ctx->buf_pos) != 0 || ctx->h2_fail) {
            ctx->buf_pos = 0;
            uv_close((uv_handle_t *)stream, client_close_cb_);
            return;
        }
        ctx->buf_pos = 0;
        if (cetcd_h2_send_pending(ctx->h2, h2_write_uv_, stream) != 0) {
            uv_close((uv_handle_t *)stream, client_close_cb_);
        }
        return;
    }

    while (ctx->buf_pos >= 2) {
        uint16_t path_len = ((uint16_t)ctx->buf[0] << 8) | ctx->buf[1];
        size_t after_path = 2 + path_len;
        if (ctx->buf_pos < after_path + 1) break;

        char path[256];
        if (path_len >= sizeof(path)) { uv_close((uv_handle_t *)stream, client_close_cb_); return; }
        memcpy(path, ctx->buf + 2, path_len);
        path[path_len] = '\0';

        uint8_t flags = ctx->buf[after_path];
        size_t cursor = after_path + 1;
        char token[CETCD_AUTH_MAX_TOKEN_LEN + 1];
        token[0] = '\0';
        if (flags & 0x02) {
            if (ctx->buf_pos < cursor + 2) break;
            uint16_t tlen = ((uint16_t)ctx->buf[cursor] << 8) | ctx->buf[cursor + 1];
            cursor += 2;
            if (tlen > CETCD_AUTH_MAX_TOKEN_LEN) { uv_close((uv_handle_t *)stream, client_close_cb_); return; }
            if (ctx->buf_pos < cursor + tlen + 4) break;
            memcpy(token, ctx->buf + cursor, tlen);
            token[tlen] = '\0';
            cursor += tlen;
        }
        if (ctx->buf_pos < cursor + 4) break;

        uint32_t payload_len = ((uint32_t)ctx->buf[cursor] << 24) |
                               ((uint32_t)ctx->buf[cursor + 1] << 16) |
                               ((uint32_t)ctx->buf[cursor + 2] << 8)  |
                               ((uint32_t)ctx->buf[cursor + 3]);
        uint64_t maxb = client_max_bytes_(ctx);
        if ((uint64_t)payload_len > maxb ||
            (uint64_t)cursor + 4ull + (uint64_t)payload_len > maxb) {
            uv_close((uv_handle_t *)stream, client_close_cb_);
            return;
        }
        size_t frame_len = cursor + 4 + (size_t)payload_len;
        if (ctx->buf_pos < frame_len) break;
        const uint8_t *payload = ctx->buf + cursor + 4;

        /* For Watch RPCs, set the stream writer to this client's socket
         * so streaming events can be sent directly to this connection */
        if (strcmp(path, "/etcdserverpb.Watch/Watch") == 0) {
            cetcd_v3rpc_set_stream_writer(ctx->srv->rpc, client_stream_write_, stream);
        }

        cetcd_server_rpc_result resp = cetcd_server_handle_rpc_ex(ctx->srv,
            path, payload, payload_len, token[0] ? token : NULL);

        /* Always reply: payload_len=0 signals a domain/RPC error (handlers
         * return {NULL,0}). Omitting the frame left clients blocked on recv. */
        size_t out_len = (resp.data && resp.len > 0) ? resp.len : 0;
        size_t hdr_len = 2 + path_len + 5;
        size_t total = hdr_len + out_len;
        uint8_t *frame = (uint8_t *)malloc(total);
        if (frame) {
            size_t pos = 0;
            frame[pos++] = (uint8_t)(path_len >> 8);
            frame[pos++] = (uint8_t)(path_len & 0xFF);
            memcpy(frame + pos, path, path_len);
            pos += path_len;
            frame[pos++] = 0;
            frame[pos++] = (uint8_t)((out_len >> 24) & 0xFF);
            frame[pos++] = (uint8_t)((out_len >> 16) & 0xFF);
            frame[pos++] = (uint8_t)((out_len >> 8) & 0xFF);
            frame[pos++] = (uint8_t)(out_len & 0xFF);
            if (out_len > 0)
                memcpy(frame + pos, resp.data, out_len);

            client_uv_send_(stream, frame, total);
        }
        cetcd_server_rpc_result_free(&resp);

        /* After create-ack is queued, flush deferred Watch history replay. */
        if (strcmp(path, "/etcdserverpb.Watch/Watch") == 0)
            cetcd_v3rpc_watch_flush_replay();

        size_t remaining = ctx->buf_pos - frame_len;
        if (remaining > 0) memmove(ctx->buf, ctx->buf + frame_len, remaining);
        ctx->buf_pos = remaining;
    }
}

static void alloc_cb_(uv_handle_t *handle, size_t suggested_size, uv_buf_t *buf) {
    (void)handle;
    /* Allocate a fresh buffer per read to avoid sharing across connections. */
    size_t cap = suggested_size > 0 ? suggested_size : 65536;
    char *slab = (char *)malloc(cap);
    if (slab == NULL) {
        buf->base = NULL;
        buf->len  = 0;
        return;
    }
    *buf = uv_buf_init(slab, (unsigned int)cap);
}

static void on_client_conn_(cetcd_tcp *server, cetcd_tcp *client, void *arg) {
    (void)server;
    cetcd_server *srv = (cetcd_server *)arg;
    if (!client || !srv) return;

    client_ctx_ *ctx = (client_ctx_ *)calloc(1, sizeof(client_ctx_));
    if (!ctx) { cetcd_tcp_close(client); return; }
    ctx->srv = srv;
    ctx->client = client;
    {
        uint64_t maxb = client_max_bytes_(ctx);
        size_t init = 65536;
        if ((uint64_t)init > maxb) init = (size_t)maxb;
        if (init == 0) { free(ctx); cetcd_tcp_close(client); return; }
        ctx->buf = (uint8_t *)malloc(init);
        if (!ctx->buf) { free(ctx); cetcd_tcp_close(client); return; }
        ctx->buf_cap = init;
    }

    uv_stream_t *stream = cetcd_tcp_stream(client);
    if (stream) {
        if (srv->tls_client) {
            ctx->tls = cetcd_tls_conn_accept(srv->tls_client);
            if (!ctx->tls) {
                free(ctx->buf);
                free(ctx);
                cetcd_tcp_close(client);
                return;
            }
        }
        stream->data = ctx;
        ctx->h2_stream = stream;
        uv_read_start(stream, alloc_cb_, on_client_read_);
    }
}

static void peer_send_cb_(uint64_t to_id, const uint8_t *data, size_t len, void *udata) {
    cetcd_server *srv = (cetcd_server *)udata;
    if (!srv || !srv->cluster || len == 0) return;
    peer_tx_ *tx = peer_tx_get_(srv, to_id);
    if (!tx) return;
    if (tx->depth >= CETCD_PEER_SENDQ_MAX) {
        /* Bounded backpressure: drop and count. Raft retransmits unacked
         * entries, so this never loses committed data. */
        srv->peer_drops++;
        if (srv->metrics) cetcd_metrics_counter(srv->metrics, "raft_peer_send_dropped_total", 1);
        return;
    }
    peer_out_frame_ *f = (peer_out_frame_ *)malloc(sizeof(*f));
    if (!f) return;
    f->next = NULL;
    f->len = 4 + len;
    f->data = (uint8_t *)malloc(f->len);
    if (!f->data) { free(f); return; }
    f->data[0] = (uint8_t)((len >> 24) & 0xFF);
    f->data[1] = (uint8_t)((len >> 16) & 0xFF);
    f->data[2] = (uint8_t)((len >> 8) & 0xFF);
    f->data[3] = (uint8_t)(len & 0xFF);
    memcpy(f->data + 4, data, len);
    if (tx->tail) tx->tail->next = f; else tx->head = f;
    tx->tail = f;
    tx->depth++;

    if (tx->state == PEER_TX_IDLE) peer_tx_connect_(tx);
    else if (tx->state == PEER_TX_CONNECTED) peer_tx_drain_(tx);
}

static void on_peer_read_(uv_stream_t *stream, ssize_t nread, const uv_buf_t *buf);
static void on_peer_alloc_(uv_handle_t *handle, size_t suggested_size, uv_buf_t *buf);
static void on_peer_close_(uv_handle_t *handle);

typedef struct peer_ctx_ {
    cetcd_server     *srv;
    uv_stream_t      *stream;
    cetcd_tls_conn   *tls;
    int               tls_ready;
    uint8_t          *buf;
    size_t            buf_pos;
    size_t            buf_cap;
} peer_ctx_;

static void on_peer_close_(uv_handle_t *handle) {
    peer_ctx_ *ctx = (peer_ctx_ *)handle->data;
    if (ctx) {
        cetcd_tls_conn_free(ctx->tls);
        if (ctx->buf) free(ctx->buf);
        free(ctx);
    }
}

static void on_peer_alloc_(uv_handle_t *handle, size_t suggested_size, uv_buf_t *buf) {
    (void)handle; (void)suggested_size;
    buf->base = (char *)malloc(65536);
    buf->len = 65536;
}

static void on_peer_read_(uv_stream_t *stream, ssize_t nread, const uv_buf_t *buf) {
    peer_ctx_ *ctx = (peer_ctx_ *)stream->data;
    if (!ctx) { if (buf->base) free(buf->base); return; }

    if (nread <= 0) {
        if (nread < 0) {
            uv_close((uv_handle_t *)stream, on_peer_close_);
        }
        if (buf->base) free(buf->base);
        return;
    }

    if (ctx->tls) {
        int rc = cetcd_tls_feed(ctx->tls, buf->base, (size_t)nread);
        if (buf->base) free(buf->base);
        if (rc != CETCD_OK) {
            uv_close((uv_handle_t *)stream, on_peer_close_);
            return;
        }
        if (!ctx->tls_ready) {
            int hs = cetcd_tls_handshake(ctx->tls);
            if (tls_flush_uv_(stream, ctx->tls) < 0 || hs < 0) {
                uv_close((uv_handle_t *)stream, on_peer_close_);
                return;
            }
            if (hs == 0) return;
            ctx->tls_ready = 1;
        }
        for (;;) {
            uint8_t tmp[4096];
            int n = cetcd_tls_read(ctx->tls, tmp, sizeof(tmp));
            if (n < 0) {
                uv_close((uv_handle_t *)stream, on_peer_close_);
                return;
            }
            if (n == 0) break;
            if (ctx->buf_pos + (size_t)n > ctx->buf_cap) {
                size_t nc = ctx->buf_cap ? ctx->buf_cap * 2 : 65536;
                while (nc < ctx->buf_pos + (size_t)n) nc *= 2;
                uint8_t *nb = (uint8_t *)realloc(ctx->buf, nc);
                if (!nb) return;
                ctx->buf = nb;
                ctx->buf_cap = nc;
            }
            memcpy(ctx->buf + ctx->buf_pos, tmp, (size_t)n);
            ctx->buf_pos += (size_t)n;
        }
    } else {
        /* Append to peer buffer */
        if (ctx->buf_pos + (size_t)nread > ctx->buf_cap) {
            size_t nc = ctx->buf_cap ? ctx->buf_cap * 2 : 65536;
            while (nc < ctx->buf_pos + (size_t)nread) nc *= 2;
            uint8_t *nb = (uint8_t *)realloc(ctx->buf, nc);
            if (!nb) { if (buf->base) free(buf->base); return; }
            ctx->buf = nb;
            ctx->buf_cap = nc;
        }
        memcpy(ctx->buf + ctx->buf_pos, buf->base, (size_t)nread);
        ctx->buf_pos += (size_t)nread;
        if (buf->base) free(buf->base);
    }

    /* Decode and feed raft messages */
    cetcd_server *srv = ctx->srv;
    if (!srv || !srv->raft) return;

    /* Peer messages are framed: 4-byte big-endian length prefix + payload.
     * The payload is a peer-framed raft message (from cetcd_msg_encode). */
    size_t consumed = 0;
    while (ctx->buf_pos - consumed >= 4) {
        uint32_t msg_len = ((uint32_t)ctx->buf[consumed] << 24) |
                           ((uint32_t)ctx->buf[consumed + 1] << 16) |
                           ((uint32_t)ctx->buf[consumed + 2] << 8) |
                           ((uint32_t)ctx->buf[consumed + 3]);
        if (msg_len == 0 || msg_len > 16 * 1024 * 1024) {
            /* Invalid frame size; close connection */
            uv_close((uv_handle_t *)stream, on_peer_close_);
            return;
        }
        if (ctx->buf_pos - consumed < 4 + msg_len) break; /* need more data */

        const uint8_t *payload = ctx->buf + consumed + 4;
        /* Decode peer framing (cetcd_msg_decode) to get raw raft wire bytes */
        uint8_t *raft_wire = NULL;
        size_t raft_wire_len = 0;
        int rc = cetcd_msg_decode(payload, msg_len, &raft_wire, &raft_wire_len);
        if (rc == 0 && raft_wire && raft_wire_len > 0) {
            /* Decode raft wire format to cetcd_msg */
            cetcd_msg *rmsg = cetcd_msg_decode_wire(raft_wire, raft_wire_len);
            if (rmsg) {
                /* Feed to raft state machine */
                cetcd_raft_step(srv->raft, rmsg);
                cetcd_msg_free(rmsg);
                /* Process any ready state from this step */
                process_ready_(srv);
            }
            free(raft_wire);
        }
        consumed += 4 + msg_len;
    }

    /* Compact buffer */
    if (consumed > 0) {
        size_t remaining = ctx->buf_pos - consumed;
        if (remaining > 0) memmove(ctx->buf, ctx->buf + consumed, remaining);
        ctx->buf_pos = remaining;
    }
}

static void on_peer_incoming_(cetcd_tcp *server, cetcd_tcp *client, void *arg) {
    (void)server;
    cetcd_server *srv = (cetcd_server *)arg;
    if (!client || !srv) return;

    peer_ctx_ *ctx = (peer_ctx_ *)calloc(1, sizeof(*ctx));
    if (!ctx) { cetcd_tcp_close(client); return; }
    ctx->srv = srv;

    uv_stream_t *stream = cetcd_tcp_stream(client);
    if (stream) {
        if (srv->tls_peer) {
            ctx->tls = cetcd_tls_conn_accept(srv->tls_peer);
            if (!ctx->tls) {
                free(ctx);
                cetcd_tcp_close(client);
                return;
            }
        }
        stream->data = ctx;
        uv_read_start(stream, on_peer_alloc_, on_peer_read_);
    } else {
        free(ctx);
        cetcd_tcp_close(client);
    }
}

static int ensure_dir(const char *path) {
#if defined(_WIN32)
    struct _stat st;
    if (_stat(path, &st) == 0) return 0;
    return cetcd_mkdir(path);
#else
    struct stat st;
    if (stat(path, &st) == 0) return 0;
    return cetcd_mkdir(path);
#endif
}

cetcd_server *cetcd_server_new(const cetcd_server_config *cfg) {
    if (!cfg) return NULL;
    cetcd_server *srv = (cetcd_server *)calloc(1, sizeof(*srv));
    if (!srv) return NULL;
    memcpy(&srv->cfg, cfg, sizeof(*cfg));

    srv->rpc = cetcd_v3rpc_new();
    if (!srv->rpc) { free(srv); return NULL; }

    srv->peer_txs = (struct peer_tx_ *)calloc(CETCD_MAX_INITIAL_PEERS, sizeof(peer_tx_));
    if (!srv->peer_txs) { cetcd_v3rpc_free(srv->rpc); free(srv); return NULL; }

    cetcd_raft_config raft_cfg = {
        .id = cfg->node_id,
        .election_tick = cfg->election_tick ? cfg->election_tick : 10,
        .heartbeat_tick = cfg->heartbeat_tick ? cfg->heartbeat_tick : 1,
        .storage = NULL,
        .max_size_per_msg = 1024 * 1024,
        .max_inflight_msgs = 256,
        .check_quorum = true,
        .pre_vote = true,
        .disable_proposal_forwarding = false,
    };
    srv->raft = cetcd_raft_new(&raft_cfg);

    srv->cluster = cetcd_cluster_new(cfg->node_id);

    /* Set global cluster handle for cluster RPC handlers */
    extern cetcd_cluster *g_rpc_cluster;
    extern uint64_t       g_rpc_node_id;
    extern cetcd_raft    *g_rpc_raft;
    g_rpc_cluster = srv->cluster;
    g_rpc_node_id = cfg->node_id;
    g_rpc_raft = srv->raft;

    cetcd_v3rpc_set_ready_flush(ready_flush_cb_, srv);
    maybe_campaign_single_(srv);

    srv->metrics = cetcd_metrics_new();
    if (srv->metrics) {
        cetcd_metrics_gauge_set(srv->metrics, "cetcd_server_info", 1);
    }

    if (cfg->auth_enabled) {
        extern cetcd_auth_store *g_rpc_auth;
        if (g_rpc_auth) cetcd_auth_set_enabled(g_rpc_auth, true);
    }

    srv->started = false;
    return srv;
}

void cetcd_server_free(cetcd_server *srv) {
    if (!srv) return;
    cetcd_v3rpc_set_ready_flush(NULL, NULL);
    {
        extern cetcd_raft *g_rpc_raft;
        if (g_rpc_raft == srv->raft) g_rpc_raft = NULL;
    }
    peer_tx_shutdown_all_(srv);
    if (srv->tick_timer) { cetcd_timer_stop(srv->tick_timer); cetcd_timer_free(srv->tick_timer); }
    if (srv->peer_listener) { cetcd_tcp_free(srv->peer_listener); srv->peer_listener = NULL; }
    if (srv->listener) { cetcd_tcp_free(srv->listener); srv->listener = NULL; }
    if (srv->metrics_listener_init) {
        uv_close((uv_handle_t *)&srv->metrics_listener, NULL);
        srv->metrics_listener_init = false;
    }
    if (srv->loop) { cetcd_loop_free(srv->loop); srv->loop = NULL; }
    peer_tx_free_all_(srv);
    if (srv->wal_enc) { cetcd_wal_encoder_flush(srv->wal_enc); cetcd_wal_encoder_free(srv->wal_enc); }
    if (srv->backend) cetcd_backend_close(srv->backend);
    if (srv->cluster) cetcd_cluster_free(srv->cluster);
    if (srv->raft) cetcd_raft_free(srv->raft);
    if (srv->rpc) cetcd_v3rpc_free(srv->rpc);
    if (srv->metrics) cetcd_metrics_free(srv->metrics);
    cetcd_tls_ctx_free(srv->tls_client);
    cetcd_tls_ctx_free(srv->tls_peer);
    cetcd_tls_ctx_free(srv->tls_peer_out);
    free(srv);
}

static int load_tls_ctx_(cetcd_tls_ctx **out,
                         const char *cert, const char *key,
                         const char *ca, int client_cert_auth,
                         int client) {
    int have_cert = cert && cert[0];
    int have_key = key && key[0];
    if (have_cert != have_key) return CETCD_ERR_INVAL;
    if (!have_cert) {
        *out = NULL;
        return CETCD_OK;
    }
    if (client_cert_auth && !(ca && ca[0])) return CETCD_ERR_INVAL;
    cetcd_tls_ctx *ctx = client ? cetcd_tls_ctx_new_client() : cetcd_tls_ctx_new();
    if (!ctx) return CETCD_ERR_UNSUPPORT;
    if (cetcd_tls_set_cert(ctx, cert, key) != CETCD_OK) {
        cetcd_tls_ctx_free(ctx);
        return CETCD_ERR_IO;
    }
    if (ca && ca[0]) {
        if (cetcd_tls_set_ca(ctx, ca) != CETCD_OK) {
            cetcd_tls_ctx_free(ctx);
            return CETCD_ERR_IO;
        }
        if (client && cetcd_tls_set_verify_peer(ctx, 0) != CETCD_OK) {
            cetcd_tls_ctx_free(ctx);
            return CETCD_ERR_IO;
        }
    }
        if (client_cert_auth) {
            if (cetcd_tls_set_verify_peer(ctx, 1) != CETCD_OK) {
                cetcd_tls_ctx_free(ctx);
                return CETCD_ERR_IO;
            }
        }
        *out = ctx;
        return CETCD_OK;
}

int cetcd_server_start(cetcd_server *srv) {
    if (!srv) return CETCD_ERR_INVAL;

    if (srv->cfg.auth_token[0]) {
        extern cetcd_auth_store *g_rpc_auth;
        if (!g_rpc_auth) return CETCD_ERR_INTERNAL;
        int trc = cetcd_auth_set_token_spec(g_rpc_auth, srv->cfg.auth_token);
        if (trc != CETCD_OK) return trc;
    }
    if (srv->cfg.bcrypt_cost) {
        extern cetcd_auth_store *g_rpc_auth;
        if (!g_rpc_auth) return CETCD_ERR_INTERNAL;
        int brc = cetcd_auth_set_bcrypt_cost(g_rpc_auth, srv->cfg.bcrypt_cost);
        if (brc != CETCD_OK) return brc;
    }
    if (srv->cfg.max_request_bytes == 0)
        srv->cfg.max_request_bytes = CETCD_DEFAULT_MAX_REQUEST_BYTES;
    cetcd_v3rpc_set_quota(srv->cfg.quota_backend_bytes);

    if (!srv->tls_client && !srv->tls_peer && !srv->tls_peer_out) {
        int trc = load_tls_ctx_(&srv->tls_client,
                                srv->cfg.cert_file, srv->cfg.key_file,
                                srv->cfg.trusted_ca_file, srv->cfg.client_cert_auth, 0);
        if (trc != CETCD_OK) return trc;
        if (srv->tls_client) {
            const char *alpn[] = { "h2" };
            if (cetcd_tls_set_alpn(srv->tls_client, alpn, 1) != CETCD_OK) {
                cetcd_tls_ctx_free(srv->tls_client);
                srv->tls_client = NULL;
                return CETCD_ERR_INTERNAL;
            }
        }
        trc = load_tls_ctx_(&srv->tls_peer,
                            srv->cfg.peer_cert_file, srv->cfg.peer_key_file,
                            srv->cfg.peer_trusted_ca_file, srv->cfg.peer_client_cert_auth, 0);
        if (trc != CETCD_OK) {
            cetcd_tls_ctx_free(srv->tls_client);
            srv->tls_client = NULL;
            return trc;
        }
        if (srv->tls_peer) {
            trc = load_tls_ctx_(&srv->tls_peer_out,
                                srv->cfg.peer_cert_file, srv->cfg.peer_key_file,
                                srv->cfg.peer_trusted_ca_file, 0, 1);
            if (trc != CETCD_OK) {
                cetcd_tls_ctx_free(srv->tls_client);
                cetcd_tls_ctx_free(srv->tls_peer);
                srv->tls_client = NULL;
                srv->tls_peer = NULL;
                return trc;
            }
        }
    }

    if (srv->cfg.data_dir[0]) {
        ensure_dir(srv->cfg.data_dir);

        if (!srv->backend) {
            cetcd_backend_config be_cfg;
            memset(&be_cfg, 0, sizeof(be_cfg));
            be_cfg.path = srv->cfg.data_dir;
            be_cfg.map_size = 64 * 1024 * 1024;
            be_cfg.max_dbs = 16;
            srv->backend = cetcd_backend_open(&be_cfg);
        }

        /* Load persisted MVCC state and attach backend for incremental writes. */
        if (srv->backend && srv->rpc) {
            cetcd_mvcc_store *store = cetcd_v3rpc_store(srv->rpc);
            if (store) {
                cetcd_mvcc_load(store, srv->backend);
                /* Lease mgr: restore TTLs from the lease bucket, then attach keys. */
                cetcd_lease_mgr *leases = cetcd_v3rpc_leases(srv->rpc);
                if (leases) {
                    cetcd_lease_mgr_set_backend(leases, srv->backend);
                    (void)cetcd_lease_load(leases, srv->backend);
                    cetcd_lease_reindex_from_store(leases, store);
                }
            }
            cetcd_v3rpc_set_auth_backend(srv->rpc, srv->backend);
            extern cetcd_auth_store *g_rpc_auth;
            if (g_rpc_auth)
                (void)cetcd_auth_load(g_rpc_auth, srv->backend);
            if (srv->cluster) {
                cetcd_cluster_set_backend(srv->cluster, srv->backend);
                (void)cetcd_cluster_load(srv->cluster, srv->backend);
                if (srv->raft) {
                    size_t n = cetcd_cluster_peer_count(srv->cluster);
                    for (size_t i = 0; i < n; i++) {
                        const cetcd_peer_info *pi =
                            cetcd_cluster_get_peer_by_index(srv->cluster, i);
                        if (!pi) continue;
                        cetcd_peer_info copy = *pi;
                        (void)cetcd_raft_add_peer(srv->raft, copy.id, copy.is_learner);
                    }
                    uint64_t jids[16];
                    uint64_t jidx = 0;
                    uint32_t jn = cetcd_cluster_loaded_joint(srv->cluster, jids, 16, &jidx);
                    if (jn > 0)
                        (void)cetcd_raft_restore_joint(srv->raft, jids, jn, jidx);
                }
            }
        }

        uint64_t applied = load_applied_(srv->backend);
        if (!srv->wal_enc) {
            char wal_dir[600];
            snprintf(wal_dir, sizeof(wal_dir), "%s/wal", srv->cfg.data_dir);
            ensure_dir(wal_dir);
            replay_wal_(srv, wal_dir);
            if (srv->raft) {
                cetcd_raft_set_applied(srv->raft, applied);
                apply_committed_(srv);
                cetcd_raft_set_applied(srv->raft, cetcd_raft_committed(srv->raft));
            }
            srv->wal_enc = cetcd_wal_encoder_create(wal_dir);
        }
    }

    maybe_campaign_single_(srv);
    if (srv->raft) process_ready_(srv);

    /* Wire lease expiry → MVCC key deletion. */
    if (srv->rpc) {
        cetcd_lease_mgr *leases = cetcd_v3rpc_leases(srv->rpc);
        if (leases)
            cetcd_lease_mgr_set_expire(leases, lease_expire_cb_, srv);
    }

    for (uint32_t i = 0; i < srv->cfg.n_initial_peers; i++) {
        cetcd_cluster_add_peer(srv->cluster, &srv->cfg.initial_peers[i]);
    }

    cetcd_cluster_set_sender(srv->cluster, peer_send_cb_, srv);

    srv->started = true;
    return 0;
}

void cetcd_server_stop(cetcd_server *srv) {
    if (srv) srv->started = false;
}

int cetcd_server_apply(cetcd_server *srv) {
    if (!srv || !srv->raft) return CETCD_ERR_INVAL;
    cetcd_ready rd = cetcd_raft_ready(srv->raft);
    cetcd_ready_free(&rd);
    return 0;
}

cetcd_server_rpc_result cetcd_server_handle_rpc_ex(cetcd_server *srv,
                                                    const char *path,
                                                    const uint8_t *req,
                                                    size_t req_len,
                                                    const char *token) {
    cetcd_server_rpc_result result = {NULL, 0};
    if (!srv || !srv->rpc || !path) return result;
    if (srv->metrics) cetcd_metrics_counter(srv->metrics, "grpc_requests_total", 1);
    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch_ex(srv->rpc, path, req, req_len, token);
    result.data = resp.data;
    result.len = resp.len;
    return result;
}

cetcd_server_rpc_result cetcd_server_handle_rpc(cetcd_server *srv,
                                                  const char *path,
                                                  const uint8_t *req,
                                                  size_t req_len) {
    return cetcd_server_handle_rpc_ex(srv, path, req, req_len, NULL);
}

void cetcd_server_rpc_result_free(cetcd_server_rpc_result *r) {
    if (!r) return;
    if (r->data) { free(r->data); r->data = NULL; }
    r->len = 0;
}

void cetcd_server_tick(cetcd_server *srv) {
    if (!srv || !srv->raft) return;
    cetcd_raft_tick(srv->raft);
    if (srv->metrics) cetcd_metrics_counter(srv->metrics, "raft_ticks_total", 1);
    process_ready_(srv);
}

int cetcd_server_compact(cetcd_server *srv, int64_t rev) {
    if (!srv || !srv->rpc) return CETCD_ERR_INVAL;
    extern cetcd_mvcc_store *g_rpc_store;
    if (!g_rpc_store) return CETCD_ERR_INVAL;
    int rc = cetcd_mvcc_compact(g_rpc_store, rev);
    if (rc == CETCD_OK)
        cetcd_v3rpc_watch_cancel_compacted(rev);
    return rc;
}

cetcd_snap *cetcd_server_snapshot(cetcd_server *srv) {
    if (!srv) return NULL;
    extern cetcd_mvcc_store *g_rpc_store;
    if (!g_rpc_store) return NULL;

    cetcd_snap *snap = cetcd_snap_new();
    if (!snap) return NULL;

    int64_t rev = cetcd_mvcc_revision(g_rpc_store);
    (void)rev;

    cetcd_kv *kvs = NULL;
    size_t kv_count = 0;
    int rc = cetcd_mvcc_range(g_rpc_store, 0,
                              (const uint8_t *)"", 0,
                              (const uint8_t *)"\xff", 1,
                              &kvs, &kv_count);
    if (rc == 0 && kvs) {
        for (size_t i = 0; i < kv_count; i++) {
            cetcd_snap_add_entry(snap,
                                 kvs[i].key.data, kvs[i].key.len,
                                 kvs[i].value.data, kvs[i].value.len,
                                 kvs[i].mod_rev.main);
        }
        cetcd_kv_free_contents(kvs, kv_count);
    }
    return snap;
}

int cetcd_server_serve(cetcd_server *srv) {
    if (!srv) return CETCD_ERR_INVAL;

    srv->loop = cetcd_loop_new();
    if (!srv->loop) return CETCD_ERR_INTERNAL;

    srv->listener = cetcd_tcp_new(srv->loop);
    if (!srv->listener) { cetcd_loop_free(srv->loop); srv->loop = NULL; return CETCD_ERR_INTERNAL; }

    int rc = cetcd_tcp_bind(srv->listener, srv->cfg.listen_addr, srv->cfg.listen_port);
    if (rc != 0) {
        cetcd_tcp_free(srv->listener); srv->listener = NULL;
        cetcd_loop_free(srv->loop); srv->loop = NULL;
        return CETCD_ERR_IO;
    }

    rc = cetcd_tcp_listen(srv->listener, on_client_conn_, srv);
    if (rc != 0) {
        cetcd_tcp_free(srv->listener); srv->listener = NULL;
        cetcd_loop_free(srv->loop); srv->loop = NULL;
        return CETCD_ERR_IO;
    }

    if (srv->cfg.peer_port > 0) {
        const char *peer_addr = srv->cfg.peer_addr[0] ? srv->cfg.peer_addr : srv->cfg.listen_addr;
        srv->peer_listener = cetcd_tcp_new(srv->loop);
        if (srv->peer_listener) {
            rc = cetcd_tcp_bind(srv->peer_listener, peer_addr, srv->cfg.peer_port);
            if (rc == 0) {
                cetcd_tcp_listen(srv->peer_listener, on_peer_incoming_, srv);
            }
        }
    }

    if (srv->cfg.metrics_port > 0) {
        uv_loop_t *loop = cetcd_loop_uv(srv->loop);
        uv_tcp_init(loop, &srv->metrics_listener);
        srv->metrics_listener.data = srv;
        srv->metrics_listener_init = true;

        struct sockaddr_in addr_in;
        rc = uv_ip4_addr(srv->cfg.listen_addr, srv->cfg.metrics_port, &addr_in);
        if (rc == 0) {
            rc = uv_tcp_bind(&srv->metrics_listener, (const struct sockaddr *)&addr_in, 0);
        }
        if (rc == 0) {
            rc = uv_listen((uv_stream_t *)&srv->metrics_listener, 128, on_metrics_connection_);
        }
        if (rc != 0) {
            CETCD_WARN("failed to start metrics listener on %s:%u: %s",
                       srv->cfg.listen_addr, srv->cfg.metrics_port, uv_strerror(rc));
            uv_close((uv_handle_t *)&srv->metrics_listener, NULL);
            srv->metrics_listener_init = false;
        } else {
            CETCD_INFO("metrics server listening on %s:%u",
                       srv->cfg.listen_addr, srv->cfg.metrics_port);
        }
    }

    srv->tick_timer = cetcd_timer_new(srv->loop);
    if (srv->tick_timer) {
        cetcd_timer_start(srv->tick_timer, 100, 100, raft_tick_cb_, srv);
    }

    srv->started = true;
    /* Enable streaming watch mode by setting the event loop */
    cetcd_v3rpc_set_loop(srv->rpc, srv->loop);
    cetcd_loop_run(srv->loop);
    return 0;
}

int64_t cetcd_server_revision(const cetcd_server *srv) {
    if (!srv) return 0;
    extern cetcd_mvcc_store *g_rpc_store;
    if (!g_rpc_store) return 0;
    return cetcd_mvcc_revision(g_rpc_store);
}

bool cetcd_server_is_leader(const cetcd_server *srv) {
    if (!srv || !srv->raft) return false;
    return cetcd_raft_leader(srv->raft) == srv->cfg.node_id;
}

uint64_t cetcd_server_node_id(const cetcd_server *srv) {
    return srv ? srv->cfg.node_id : 0;
}

size_t cetcd_server_peer_count(const cetcd_server *srv) {
    if (!srv || !srv->cluster) return 0;
    return cetcd_cluster_peer_count(srv->cluster);
}

int cetcd_server_add_peer(cetcd_server *srv, const cetcd_peer_info *info) {
    if (!srv || !srv->cluster || !info) return CETCD_ERR_INVAL;
    return cetcd_cluster_add_peer(srv->cluster, info);
}

int cetcd_server_remove_peer(cetcd_server *srv, uint64_t peer_id) {
    if (!srv || !srv->cluster) return CETCD_ERR_INVAL;
    return cetcd_cluster_remove_peer(srv->cluster, peer_id);
}

int cetcd_server_propose_conf_change(cetcd_server *srv, uint64_t peer_id, int change_type) {
    if (!srv || !srv->raft) return CETCD_ERR_INVAL;
    uint8_t buf[16];
    size_t pos = 0;
    buf[pos++] = 0x08;
    buf[pos++] = (uint8_t)change_type;
    buf[pos++] = 0x10;
    do {
        uint8_t b = peer_id & 0x7F;
        peer_id >>= 7;
        if (peer_id) b |= 0x80;
        buf[pos++] = b;
    } while (peer_id);
    return cetcd_raft_propose_conf_change(srv->raft, buf, pos);
}

cetcd_metrics *cetcd_server_metrics(cetcd_server *srv) {
    return srv ? srv->metrics : NULL;
}

static void maybe_snapshot_truncate_(cetcd_server *srv) {
    if (!srv || !srv->raft || !srv->wal_enc) return;
    uint64_t count = srv->cfg.snapshot_count
        ? srv->cfg.snapshot_count
        : CETCD_DEFAULT_SNAPSHOT_COUNT;
    uint64_t applied = cetcd_raft_applied(srv->raft);
    if (applied == 0 || applied < srv->last_snap_index + count) return;
    if (cetcd_raft_last_index(srv->raft) != applied) return;
    const cetcd_entry *e = cetcd_raft_entry_at(srv->raft, applied);
    if (!e) return;
    cetcd_hard_state hs;
    cetcd_raft_copy_hard_state(srv->raft, &hs);
    if (cetcd_wal_encoder_release(srv->wal_enc, applied, e->term, &hs) != 0)
        return;
    srv->last_snap_index = applied;
    (void)cetcd_raft_compact(srv->raft, applied, e->term);
}

static void raft_tick_cb_(void *arg) {
    cetcd_server *srv = (cetcd_server *)arg;
    if (!srv) return;
    if (!srv->started) {
        cetcd_loop_stop(srv->loop);
        return;
    }
    if (!srv->raft) return;
    cetcd_raft_tick(srv->raft);
    if (srv->metrics) cetcd_metrics_counter(srv->metrics, "raft_ticks_total", 1);
    /* Advance lease deadlines (tick interval is 100ms). */
    if (srv->rpc) {
        cetcd_lease_mgr *leases = cetcd_v3rpc_leases(srv->rpc);
        if (leases) cetcd_lease_mgr_tick(leases, 100);
        cetcd_v3rpc_watch_tick();
    }
    process_ready_(srv);
}

static int maybe_propose_leave_joint_(cetcd_server *srv) {
    if (!srv || !srv->raft) return 0;
    if (cetcd_raft_state(srv->raft) != CETCD_NODE_LEADER) return 0;
    if (!cetcd_raft_in_joint(srv->raft)) return 0;
    if (cetcd_raft_leave_pending(srv->raft)) return 0;
    if (!cetcd_raft_joint_caught_up(srv->raft)) return 0;
    uint8_t tag = CETCD_APPLY_LEAVE_JOINT;
    if (cetcd_raft_propose(srv->raft, &tag, 1) != 0) return 0;
    cetcd_raft_set_leave_pending(srv->raft, 1);
    return 1;
}

static void process_ready_(cetcd_server *srv) {
    static _Thread_local int ready_depth;
    ready_depth++;
    cetcd_ready rd = cetcd_raft_ready(srv->raft);

    /* Persist new entries and HardState BEFORE sending any messages or
     * applying entries. If persistence fails we must not advertise the
     * Ready (Raft safety: messages reference durably-stored log indices). */
    bool persisted = true;
    if (rd.entries && rd.n_entries > 0 && srv->wal_enc) {
        for (uint32_t i = 0; i < rd.n_entries; i++) {
            if (cetcd_wal_encode_entry(srv->wal_enc, &rd.entries[i]) != 0) {
                persisted = false;
                break;
            }
        }
    }
    if (persisted && rd.hard_state && srv->wal_enc) {
        if (cetcd_wal_encode_hard_state(srv->wal_enc, rd.hard_state) != 0) persisted = false;
    }
    if (srv->wal_enc && persisted) {
        if (cetcd_wal_encoder_sync(srv->wal_enc) != 0) persisted = false;
    }

    /* Send outgoing messages to peers (only after a durable WAL). */
    if (persisted && rd.messages && rd.n_messages > 0) {
        for (uint32_t i = 0; i < rd.n_messages; i++) {
            uint8_t *wire = NULL;
            size_t wire_len = cetcd_msg_encode_wire(&rd.messages[i], &wire);
            if (wire && wire_len > 0) {
                uint8_t *framed = NULL;
                size_t framed_len = cetcd_msg_encode(wire, wire_len, &framed);
                if (framed && framed_len > 0) {
                    cetcd_cluster_send_msg(srv->cluster, framed, framed_len, rd.messages[i].to);
                    free(framed);
                }
                free(wire);
            }
        }
    }

    /* Apply committed entries from the in-memory log (Ready.entries are
     * only the newly persisted suffix; commit may advance without new
     * entries, e.g. after a follower AppResp). */
    if (persisted) {
        apply_committed_(srv);
        if (srv->metrics) {
            cetcd_metrics_gauge_set(srv->metrics, "raft_committed_index",
                                    (double)cetcd_raft_committed(srv->raft));
            extern cetcd_mvcc_store *g_rpc_store;
            if (g_rpc_store) {
                cetcd_metrics_gauge_set(srv->metrics, "mvcc_revision",
                                       (double)cetcd_mvcc_revision(g_rpc_store));
            }
        }
        cetcd_raft_advance(srv->raft, &rd);
        maybe_snapshot_truncate_(srv);
    }

    cetcd_ready_free(&rd);
    if (ready_depth == 1 && maybe_propose_leave_joint_(srv))
        process_ready_(srv);
    ready_depth--;
}
