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

#include <stdlib.h>
#include <string.h>
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
};

static int  ensure_dir(const char *path);
static void raft_tick_cb_(void *arg);
static void process_ready_(cetcd_server *srv);
static void peer_send_cb_(uint64_t to_id, const uint8_t *data, size_t len, void *udata);
static void on_peer_incoming_(cetcd_tcp *server, cetcd_tcp *client, void *arg);
static void on_client_conn_(cetcd_tcp *server, cetcd_tcp *client, void *arg);

/* --- Streaming watch support --- */

/* Cleanup callback for uv_write in stream_write_ */
static void stream_write_cleanup_(uv_write_t *req, int status) {
    (void)status;
    if (req->data) free(req->data);
    free(req);
}

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

    uv_buf_t wbuf = uv_buf_init((char *)frame, (unsigned int)total);
    uv_write_t *wr = (uv_write_t *)calloc(1, sizeof(uv_write_t));
    if (wr) {
        wr->data = frame;
        uv_write(wr, stream, &wbuf, 1, stream_write_cleanup_);
    } else {
        free(frame);
    }
}

static void on_metrics_connection_(uv_stream_t *server, int status);
static void on_metrics_alloc_(uv_handle_t *handle, size_t suggested_size, uv_buf_t *buf);
static void on_metrics_read_(uv_stream_t *stream, ssize_t nread, const uv_buf_t *buf);
static void on_metrics_write_(uv_write_t *req, int status);
static void on_metrics_close_(uv_handle_t *handle);

typedef struct client_ctx_ {
    cetcd_server *srv;
    cetcd_tcp    *client;
    uint8_t       buf[65536];
    size_t        buf_pos;
} client_ctx_;

static void client_close_cb_(uv_handle_t *handle) {
    client_ctx_ *ctx = (client_ctx_ *)handle->data;
    if (ctx) free(ctx);
}

typedef struct metrics_conn_ctx_ {
    cetcd_server *srv;
    uv_tcp_t      client;
    char          req[4096];
    size_t        req_len;
    cetcd_buf_t   resp;
    uv_write_t    write_req;
    int           pprof_seconds;  /* for /debug/pprof/profile?seconds=N */
} metrics_conn_ctx_;

static void on_metrics_close_(uv_handle_t *handle) {
    metrics_conn_ctx_ *ctx = (metrics_conn_ctx_ *)handle->data;
    if (!ctx) return;
    cetcd_buf_free(&ctx->resp);
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
        ctx->pprof_seconds = 5;  /* default */
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
        /* /debug/pprof/profile — CPU profiling (blocks for N seconds) */
        cetcd_buf_t body;
        cetcd_buf_init(&body);
        int rc = cetcd_pprof_profile_render(&body, ctx->pprof_seconds);
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

static void on_client_read_(uv_stream_t *stream, ssize_t nread, const uv_buf_t *buf) {
    client_ctx_ *ctx = (client_ctx_ *)stream->data;
    if (!ctx) return;
    if (nread <= 0) {
        if (nread < 0) uv_close((uv_handle_t *)stream, client_close_cb_);
        if (buf->base) free(buf->base);
        return;
    }
    if (ctx->buf_pos + (size_t)nread > sizeof(ctx->buf)) {
        if (buf->base) free(buf->base);
        uv_close((uv_handle_t *)stream, client_close_cb_);
        return;
    }
    memcpy(ctx->buf + ctx->buf_pos, buf->base, (size_t)nread);
    ctx->buf_pos += (size_t)nread;
    if (buf->base) free(buf->base);

    while (ctx->buf_pos >= 2) {
        uint16_t path_len = ((uint16_t)ctx->buf[0] << 8) | ctx->buf[1];
        size_t header = 2 + path_len;
        if (ctx->buf_pos < header + 5) break;

        char path[256];
        if (path_len >= sizeof(path)) { uv_close((uv_handle_t *)stream, client_close_cb_); return; }
        memcpy(path, ctx->buf + 2, path_len);
        path[path_len] = '\0';

        const uint8_t *grpc_hdr = ctx->buf + header;
        uint32_t payload_len = ((uint32_t)grpc_hdr[1] << 24) |
                               ((uint32_t)grpc_hdr[2] << 16) |
                               ((uint32_t)grpc_hdr[3] << 8)  |
                               ((uint32_t)grpc_hdr[4]);
        size_t frame_len = header + 5 + payload_len;
        if (ctx->buf_pos < frame_len) break;

        /* For Watch RPCs, set the stream writer to this client's socket
         * so streaming events can be sent directly to this connection */
        if (strcmp(path, "/etcdserverpb.Watch/Watch") == 0) {
            cetcd_v3rpc_set_stream_writer(ctx->srv->rpc, client_stream_write_, stream);
        }

        cetcd_server_rpc_result resp = cetcd_server_handle_rpc(ctx->srv,
            path, grpc_hdr + 5, payload_len);

        if (resp.data && resp.len > 0) {
            uint8_t resp_hdr[2 + 256 + 5];
            size_t pos = 0;
            resp_hdr[pos++] = (uint8_t)(path_len >> 8);
            resp_hdr[pos++] = (uint8_t)(path_len & 0xFF);
            memcpy(resp_hdr + pos, path, path_len);
            pos += path_len;
            resp_hdr[pos++] = 0;
            resp_hdr[pos++] = (uint8_t)((resp.len >> 24) & 0xFF);
            resp_hdr[pos++] = (uint8_t)((resp.len >> 16) & 0xFF);
            resp_hdr[pos++] = (uint8_t)((resp.len >> 8) & 0xFF);
            resp_hdr[pos++] = (uint8_t)(resp.len & 0xFF);

            uv_buf_t wbuf[2];
            wbuf[0] = uv_buf_init((char *)resp_hdr, (unsigned int)pos);
            wbuf[1] = uv_buf_init((char *)resp.data, (unsigned int)resp.len);
            uv_write_t *wr = (uv_write_t *)calloc(1, sizeof(uv_write_t));
            if (wr) {
                uv_write(wr, stream, wbuf, 2, (void (*)(uv_write_t *, int))free);
            }
        }
        cetcd_server_rpc_result_free(&resp);

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

    uv_stream_t *stream = cetcd_tcp_stream(client);
    if (stream) {
        stream->data = ctx;
        uv_read_start(stream, alloc_cb_, on_client_read_);
    }
}

static void peer_send_cb_(uint64_t to_id, const uint8_t *data, size_t len, void *udata) {
    cetcd_server *srv = (cetcd_server *)udata;
    if (!srv || !srv->cluster || len == 0) return;
    const cetcd_peer_info *pi = cetcd_cluster_get_peer(srv->cluster, to_id);
    if (!pi) return;
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return;
    struct sockaddr_in sa;
    memset(&sa, 0, sizeof(sa));
    sa.sin_family = AF_INET;
    sa.sin_port = htons(pi->port);
    inet_pton(AF_INET, pi->addr, &sa.sin_addr);
    if (connect(fd, (struct sockaddr *)&sa, sizeof(sa)) == 0) {
        /* Frame: 4-byte big-endian length prefix + payload */
        uint8_t hdr[4];
        hdr[0] = (uint8_t)((len >> 24) & 0xFF);
        hdr[1] = (uint8_t)((len >> 16) & 0xFF);
        hdr[2] = (uint8_t)((len >> 8) & 0xFF);
        hdr[3] = (uint8_t)(len & 0xFF);
        send(fd, hdr, 4, CETCD_MSG_NOSIGNAL);
        send(fd, data, len, CETCD_MSG_NOSIGNAL);
    }
    cetcd_close_socket(fd);
}

static void on_peer_read_(uv_stream_t *stream, ssize_t nread, const uv_buf_t *buf);
static void on_peer_alloc_(uv_handle_t *handle, size_t suggested_size, uv_buf_t *buf);
static void on_peer_close_(uv_handle_t *handle);

typedef struct peer_ctx_ {
    cetcd_server *srv;
    uv_stream_t  *stream;
    uint8_t      *buf;
    size_t        buf_pos;
    size_t        buf_cap;
} peer_ctx_;

static void on_peer_close_(uv_handle_t *handle) {
    peer_ctx_ *ctx = (peer_ctx_ *)handle->data;
    if (ctx) {
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
    if (srv->tick_timer) { cetcd_timer_stop(srv->tick_timer); cetcd_timer_free(srv->tick_timer); }
    if (srv->peer_listener) { cetcd_tcp_free(srv->peer_listener); srv->peer_listener = NULL; }
    if (srv->listener) { cetcd_tcp_free(srv->listener); srv->listener = NULL; }
    if (srv->metrics_listener_init) {
        uv_close((uv_handle_t *)&srv->metrics_listener, NULL);
        srv->metrics_listener_init = false;
    }
    if (srv->loop) { cetcd_loop_free(srv->loop); srv->loop = NULL; }
    if (srv->wal_enc) { cetcd_wal_encoder_flush(srv->wal_enc); cetcd_wal_encoder_free(srv->wal_enc); }
    if (srv->backend) cetcd_backend_close(srv->backend);
    if (srv->cluster) cetcd_cluster_free(srv->cluster);
    if (srv->raft) cetcd_raft_free(srv->raft);
    if (srv->rpc) cetcd_v3rpc_free(srv->rpc);
    if (srv->metrics) cetcd_metrics_free(srv->metrics);
    free(srv);
}

int cetcd_server_start(cetcd_server *srv) {
    if (!srv) return CETCD_ERR_INVAL;

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

        if (!srv->wal_enc) {
            char wal_path[600];
            snprintf(wal_path, sizeof(wal_path), "%s/wal", srv->cfg.data_dir);
            ensure_dir(srv->cfg.data_dir);
            srv->wal_enc = cetcd_wal_encoder_create(wal_path);
        }
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

cetcd_server_rpc_result cetcd_server_handle_rpc(cetcd_server *srv,
                                                  const char *path,
                                                  const uint8_t *req,
                                                  size_t req_len) {
    cetcd_server_rpc_result result = {NULL, 0};
    if (!srv || !srv->rpc || !path) return result;
    if (srv->metrics) cetcd_metrics_counter(srv->metrics, "grpc_requests_total", 1);
    cetcd_rpc_bytes resp = cetcd_v3rpc_dispatch(srv->rpc, path, req, req_len);
    result.data = resp.data;
    result.len = resp.len;
    return result;
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
}

int cetcd_server_compact(cetcd_server *srv, int64_t rev) {
    if (!srv || !srv->rpc) return CETCD_ERR_INVAL;
    extern cetcd_mvcc_store *g_rpc_store;
    if (!g_rpc_store) return CETCD_ERR_INVAL;
    return cetcd_mvcc_compact(g_rpc_store, rev);
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
    do { uint8_t b = peer_id & 0x7F; peer_id >>= 7;
         if (peer_id) b |= 0x80; buf[pos++] = b; } while (peer_id);
    return cetcd_raft_propose_conf_change(srv->raft, buf, pos);
}

cetcd_metrics *cetcd_server_metrics(cetcd_server *srv) {
    return srv ? srv->metrics : NULL;
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
    process_ready_(srv);
}

static void process_ready_(cetcd_server *srv) {
    cetcd_ready rd = cetcd_raft_ready(srv->raft);

    /* Persist new entries to WAL */
    if (rd.entries && rd.n_entries > 0 && srv->wal_enc) {
        for (uint32_t i = 0; i < rd.n_entries; i++) {
            cetcd_wal_encode_entry(srv->wal_enc, &rd.entries[i]);
        }
        cetcd_wal_encoder_flush(srv->wal_enc);
    }

    /* Persist HardState if changed */
    if (rd.hard_state && srv->wal_enc) {
        cetcd_wal_encode_hard_state(srv->wal_enc, rd.hard_state);
        cetcd_wal_encoder_flush(srv->wal_enc);
    }

    /* Send outgoing messages to peers */
    if (rd.messages && rd.n_messages > 0) {
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

    /* Apply committed entries to the MVCC store */
    if (rd.committed > 0) {
        if (srv->metrics) {
            cetcd_metrics_gauge_set(srv->metrics, "raft_committed_index", (double)rd.committed);
        }
        /* Apply entries: decode NORMAL entries as KV operations.
         * Entry data format: tag(1 byte) + key_len(varint) + key + val_len(varint) + val
         * tag: 1=Put, 2=Delete
         * This is a simplified apply — in production, entries would go through Raft consensus
         * and be applied in order. */
        extern cetcd_mvcc_store *g_rpc_store;
        if (g_rpc_store && rd.entries) {
            for (uint32_t i = 0; i < rd.n_entries; i++) {
                if (rd.entries[i].type != CETCD_ENTRY_NORMAL) continue;
                if (rd.entries[i].index > rd.committed) break;
                const uint8_t *data = rd.entries[i].data.data;
                size_t len = rd.entries[i].data.len;
                if (!data || len < 2) continue;
                /* Simple apply: data is raw KV operation */
                uint8_t op = data[0];
                size_t pos = 1;
                /* read key */
                uint64_t klen = 0; int shift = 0;
                while (pos < len) {
                    uint8_t b = data[pos++];
                    klen |= (uint64_t)(b & 0x7F) << shift;
                    if (!(b & 0x80)) break;
                    shift += 7;
                }
                if (pos + klen > len) continue;
                const uint8_t *key = data + pos;
                pos += klen;
                if (op == 1) {
                    /* Put: read value */
                    uint64_t vlen = 0; shift = 0;
                    while (pos < len) {
                        uint8_t b = data[pos++];
                        vlen |= (uint64_t)(b & 0x7F) << shift;
                        if (!(b & 0x80)) break;
                        shift += 7;
                    }
                    if (pos + vlen > len) continue;
                    cetcd_mvcc_put(g_rpc_store, key, (size_t)klen,
                                   data + pos, (size_t)vlen, 0);
                } else if (op == 2) {
                    /* Delete */
                    cetcd_mvcc_delete(g_rpc_store, key, (size_t)klen);
                }
            }
        }
        if (srv->metrics) {
            extern cetcd_mvcc_store *g_rpc_store;
            if (g_rpc_store) {
                cetcd_metrics_gauge_set(srv->metrics, "mvcc_revision",
                                       (double)cetcd_mvcc_revision(g_rpc_store));
            }
        }
    }

    cetcd_raft_advance(srv->raft, &rd);
    cetcd_ready_free(&rd);
}
