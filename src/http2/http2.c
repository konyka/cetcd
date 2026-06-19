#include "cetcd/http2.h"

#include <stdlib.h>
#include <string.h>

/* ============================================================================ */
/*  gRPC framing helpers (always available)                                     */
/* ============================================================================ */

int cetcd_grpc_encode(const uint8_t *msg, size_t msg_len,
                     bool compressed, uint8_t **out, size_t *out_len) {
    if (out == NULL || out_len == NULL) return CETCD_ERR_INVAL;
    size_t total = 1 + 4 + msg_len;
    uint8_t *buf = (uint8_t *)malloc(total);
    if (buf == NULL) return CETCD_ERR_NOMEM;
    buf[0] = compressed ? 1 : 0;
    /* big-endian length */
    uint32_t be_len = (uint32_t)msg_len;
    buf[1] = (be_len >> 24) & 0xff;
    buf[2] = (be_len >> 16) & 0xff;
    buf[3] = (be_len >> 8) & 0xff;
    buf[4] = be_len & 0xff;
    if (msg_len > 0 && msg != NULL) {
        memcpy(buf + 5, msg, msg_len);
    }
    *out = buf;
    *out_len = total;
    return CETCD_OK;
}

int cetcd_grpc_decode(const uint8_t *frame, size_t frame_len,
                     bool *compressed, uint8_t **msg, size_t *msg_len) {
    if (frame == NULL || frame_len < 5 || compressed == NULL || msg == NULL || msg_len == NULL) {
        return CETCD_ERR_INVAL;
    }
    uint8_t flag = frame[0];
    uint32_t len = ((uint32_t)frame[1] << 24) |
                   ((uint32_t)frame[2] << 16) |
                   ((uint32_t)frame[3] << 8)  |
                   ((uint32_t)frame[4]);
    if (frame_len < (size_t)(5 + len)) {
        return CETCD_ERR_INVAL;
    }
    uint8_t *payload = NULL;
    if (len > 0) {
        payload = (uint8_t *)malloc(len);
        if (payload == NULL) return CETCD_ERR_NOMEM;
        memcpy(payload, frame + 5, len);
    }
    *compressed = (flag != 0);
    *msg = payload;
    *msg_len = (size_t)len;
    return CETCD_OK;
}

/* ============================================================================ */
/*  HTTP/2 session management                                                   */
/* ============================================================================ */

#ifdef CETCD_HAS_NGHTTP2

#include <nghttp2/nghttp2.h>

/* -- Per-stream request state (simplified: one concurrent request) ---------- */

typedef struct {
    int32_t stream_id;
    char    method[16];
    char    path[256];
    char    content_type[64];
    bool    request_notified;
} h2_req_state_;

struct cetcd_h2_session {
    nghttp2_session    *ngh;
    cetcd_h2_callbacks  cbs;

    /* Current incoming request state */
    h2_req_state_       cur;

    /* Pending response body (stored for the data-provider callback) */
    uint8_t            *resp_body;
    size_t              resp_body_len;
    size_t              resp_body_pos;
};

/* -- nghttp2 callbacks ------------------------------------------------------ */

static int
h2_on_begin_headers_(nghttp2_session *session,
                      const nghttp2_frame *frame,
                      void *user_data) {
    (void)session;
    cetcd_h2_session *s = (cetcd_h2_session *)user_data;
    if (frame->hd.type == NGHTTP2_HEADERS &&
        frame->headers.cat == NGHTTP2_HCAT_REQUEST) {
        memset(&s->cur, 0, sizeof(s->cur));
        s->cur.stream_id = frame->hd.stream_id;
    }
    return 0;
}

static int
h2_on_header_(nghttp2_session *session,
               const nghttp2_frame *frame,
               const uint8_t *name, size_t namelen,
               const uint8_t *value, size_t valuelen,
               uint8_t flags, void *user_data) {
    (void)session; (void)flags;
    cetcd_h2_session *s = (cetcd_h2_session *)user_data;
    if (frame->hd.type != NGHTTP2_HEADERS) return 0;

    if (namelen == 7 && memcmp(name, ":method", 7) == 0) {
        size_t n = valuelen < sizeof(s->cur.method) - 1
                       ? valuelen : sizeof(s->cur.method) - 1;
        memcpy(s->cur.method, value, n);
        s->cur.method[n] = '\0';
    } else if (namelen == 5 && memcmp(name, ":path", 5) == 0) {
        size_t n = valuelen < sizeof(s->cur.path) - 1
                       ? valuelen : sizeof(s->cur.path) - 1;
        memcpy(s->cur.path, value, n);
        s->cur.path[n] = '\0';
    } else if (namelen == 12 && memcmp(name, "content-type", 12) == 0) {
        size_t n = valuelen < sizeof(s->cur.content_type) - 1
                       ? valuelen : sizeof(s->cur.content_type) - 1;
        memcpy(s->cur.content_type, value, n);
        s->cur.content_type[n] = '\0';
    }
    return 0;
}

static int
h2_on_data_chunk_(nghttp2_session *session,
                   uint8_t flags,
                   int32_t stream_id,
                   const uint8_t *data, size_t len,
                   void *user_data) {
    (void)session;
    cetcd_h2_session *s = (cetcd_h2_session *)user_data;
    bool end_stream = (flags & NGHTTP2_FLAG_END_STREAM) != 0;
    if (s->cbs.on_data) {
        s->cbs.on_data(s, stream_id, data, len, end_stream, s->cbs.udata);
    }
    return 0;
}

static int
h2_on_frame_recv_(nghttp2_session *session,
                   const nghttp2_frame *frame,
                   void *user_data) {
    (void)session;
    cetcd_h2_session *s = (cetcd_h2_session *)user_data;

    /* When request HEADERS are fully received, fire on_request */
    if (frame->hd.type == NGHTTP2_HEADERS &&
        frame->headers.cat == NGHTTP2_HCAT_REQUEST) {
        if (!s->cur.request_notified && s->cbs.on_request) {
            s->cbs.on_request(s, s->cur.stream_id,
                              s->cur.method[0]  ? s->cur.method  : "",
                              s->cur.path[0]    ? s->cur.path    : "",
                              s->cur.content_type[0] ? s->cur.content_type : "",
                              s->cbs.udata);
            s->cur.request_notified = true;
        }
    }
    return 0;
}

/* -- Data-provider read callback for submit_response ------------------------ */

static nghttp2_ssize
h2_data_read_cb_(nghttp2_session *session,
                  int32_t stream_id,
                  uint8_t *buf, size_t length,
                  uint32_t *data_flags,
                  nghttp2_data_source *source,
                  void *user_data) {
    (void)session; (void)stream_id; (void)source;
    cetcd_h2_session *s = (cetcd_h2_session *)user_data;

    if (s->resp_body_pos >= s->resp_body_len) {
        *data_flags |= NGHTTP2_DATA_FLAG_EOF;
        return 0;
    }
    size_t avail = s->resp_body_len - s->resp_body_pos;
    size_t to_copy = avail < length ? avail : length;
    memcpy(buf, s->resp_body + s->resp_body_pos, to_copy);
    s->resp_body_pos += to_copy;
    if (s->resp_body_pos >= s->resp_body_len) {
        *data_flags |= NGHTTP2_DATA_FLAG_EOF;
    }
    return (nghttp2_ssize)to_copy;
}

/* -- Public API ------------------------------------------------------------- */

cetcd_h2_session *
cetcd_h2_session_new(const cetcd_h2_callbacks *cbs) {
    cetcd_h2_session *s = (cetcd_h2_session *)calloc(1, sizeof(*s));
    if (s == NULL) return NULL;
    if (cbs) s->cbs = *cbs;

    nghttp2_session_callbacks *cb;
    if (nghttp2_session_callbacks_new(&cb) != 0) {
        free(s);
        return NULL;
    }

    nghttp2_session_callbacks_set_on_begin_headers_callback(cb, h2_on_begin_headers_);
    nghttp2_session_callbacks_set_on_header_callback(cb, h2_on_header_);
    nghttp2_session_callbacks_set_on_data_chunk_recv_callback(cb, h2_on_data_chunk_);
    nghttp2_session_callbacks_set_on_frame_recv_callback(cb, h2_on_frame_recv_);

    /*
     * Disable nghttp2's built-in HTTP/1.1-to-HTTP/2 message validation.
     * cetcd speaks raw gRPC over HTTP/2 and does not need full HTTP
     * semantics checking (e.g. :authority, content-length consistency).
     */
    nghttp2_option *opt;
    nghttp2_option_new(&opt);
    nghttp2_option_set_no_http_messaging(opt, 1);

    int rc = nghttp2_session_server_new2(&s->ngh, cb, s, opt);
    nghttp2_option_del(opt);
    nghttp2_session_callbacks_del(cb);
    if (rc != 0) {
        free(s);
        return NULL;
    }

    /* Submit initial SETTINGS frame */
    nghttp2_submit_settings(s->ngh, NGHTTP2_FLAG_NONE, NULL, 0);

    return s;
}

void
cetcd_h2_session_free(cetcd_h2_session *s) {
    if (s == NULL) return;
    if (s->ngh) nghttp2_session_del(s->ngh);
    if (s->resp_body) free(s->resp_body);
    free(s);
}

int
cetcd_h2_feed(cetcd_h2_session *s, const uint8_t *data, size_t len) {
    if (!s || (!data && len > 0)) return -1;
    while (len > 0) {
        nghttp2_ssize n = nghttp2_session_mem_recv2(s->ngh, data, len);
        if (n < 0) return -1;
        if (n == 0) break;  /* no progress — avoid infinite loop */
        data += (size_t)n;
        len  -= (size_t)n;
    }
    return 0;
}

int
cetcd_h2_send_pending(cetcd_h2_session *s,
                       int (*write_fn)(const uint8_t *buf, size_t len, void *ctx),
                       void *ctx) {
    if (!s) return -1;
    for (;;) {
        const uint8_t *out_data = NULL;
        nghttp2_ssize out_len = nghttp2_session_mem_send2(s->ngh, &out_data);
        if (out_len <= 0) break;
        if (write_fn(out_data, (size_t)out_len, ctx) != 0) return -1;
    }
    return 0;
}

int
cetcd_h2_submit_response(cetcd_h2_session *s, int32_t stream_id,
                           const char **headers, size_t header_count,
                           const uint8_t *body, size_t body_len,
                           bool end_stream) {
    if (!s) return -1;

    /* Build nghttp2_nv array from name/value pairs */
    size_t nv_count = header_count / 2;
    nghttp2_nv *nva = NULL;
    if (nv_count > 0) {
        nva = (nghttp2_nv *)calloc(nv_count, sizeof(nghttp2_nv));
        if (!nva) return -1;
        for (size_t i = 0; i < nv_count; i++) {
            const char *name  = headers[i * 2];
            const char *value = headers[i * 2 + 1];
            if (name && value) {
                nva[i].name     = (uint8_t *)name;
                nva[i].namelen   = strlen(name);
                nva[i].value    = (uint8_t *)value;
                nva[i].valuelen  = strlen(value);
                nva[i].flags    = NGHTTP2_NV_FLAG_NONE;
            }
        }
    }

    /* Submit response headers (END_HEADERS, END_STREAM only if no body) */
    uint8_t hdr_flags = NGHTTP2_FLAG_END_HEADERS;
    if (body_len == 0 && end_stream) {
        hdr_flags |= NGHTTP2_FLAG_END_STREAM;
    }
    nghttp2_submit_headers(s->ngh, hdr_flags, stream_id, NULL,
                           nva, nv_count, NULL);
    free(nva);

    /* Submit body data if present */
    if (body_len > 0 && body) {
        /* Free any previous pending body */
        if (s->resp_body) { free(s->resp_body); s->resp_body = NULL; }
        s->resp_body = (uint8_t *)malloc(body_len);
        if (!s->resp_body) return -1;
        memcpy(s->resp_body, body, body_len);
        s->resp_body_len  = body_len;
        s->resp_body_pos  = 0;

        nghttp2_data_provider2 dp;
        dp.read_callback = h2_data_read_cb_;
        dp.source.ptr    = s;

        uint8_t data_flags = 0;
        if (end_stream) data_flags |= NGHTTP2_FLAG_END_STREAM;
        nghttp2_submit_data2(s->ngh, data_flags, stream_id, &dp);
    }

    return 0;
}

int
cetcd_h2_submit_trailers(cetcd_h2_session *s, int32_t stream_id,
                           const char **trailers, size_t count) {
    if (!s) return -1;

    size_t nv_count = count / 2;
    nghttp2_nv *nva = NULL;
    if (nv_count > 0) {
        nva = (nghttp2_nv *)calloc(nv_count, sizeof(nghttp2_nv));
        if (!nva) return -1;
        for (size_t i = 0; i < nv_count; i++) {
            const char *name  = trailers[i * 2];
            const char *value = trailers[i * 2 + 1];
            if (name && value) {
                nva[i].name     = (uint8_t *)name;
                nva[i].namelen   = strlen(name);
                nva[i].value    = (uint8_t *)value;
                nva[i].valuelen  = strlen(value);
                nva[i].flags    = NGHTTP2_NV_FLAG_NONE;
            }
        }
    }

    /* Trailers are HEADERS with END_HEADERS and END_STREAM */
    uint8_t flags = NGHTTP2_FLAG_END_HEADERS | NGHTTP2_FLAG_END_STREAM;
    nghttp2_submit_headers(s->ngh, flags, stream_id, NULL,
                           nva, nv_count, NULL);
    free(nva);
    return 0;
}

void
cetcd_h2_session_terminate(cetcd_h2_session *s, uint32_t error_code) {
    if (!s || !s->ngh) return;
    nghttp2_session_terminate_session(s->ngh, error_code);
}

#else /* !CETCD_HAS_NGHTTP2 — minimal stub implementation */

/*
 * When nghttp2 is not available we provide safe no-op stubs so that
 * the rest of the codebase compiles and the gRPC framing helpers
 * remain fully functional.
 */

struct cetcd_h2_session {
    cetcd_h2_callbacks cbs;
};

cetcd_h2_session *
cetcd_h2_session_new(const cetcd_h2_callbacks *cbs) {
    cetcd_h2_session *s = (cetcd_h2_session *)calloc(1, sizeof(*s));
    if (s == NULL) return NULL;
    if (cbs) s->cbs = *cbs;
    return s;
}

void
cetcd_h2_session_free(cetcd_h2_session *s) {
    if (s == NULL) return;
    free(s);
}

int
cetcd_h2_feed(cetcd_h2_session *s, const uint8_t *data, size_t len) {
    (void)s; (void)data; (void)len;
    return 0;
}

int
cetcd_h2_send_pending(cetcd_h2_session *s,
                       int (*write_fn)(const uint8_t *buf, size_t len, void *ctx),
                       void *ctx) {
    (void)s; (void)write_fn; (void)ctx;
    return 0;
}

int
cetcd_h2_submit_response(cetcd_h2_session *s, int32_t stream_id,
                           const char **headers, size_t header_count,
                           const uint8_t *body, size_t body_len,
                           bool end_stream) {
    (void)s; (void)stream_id; (void)headers; (void)header_count;
    (void)body; (void)body_len; (void)end_stream;
    return 0;
}

int
cetcd_h2_submit_trailers(cetcd_h2_session *s, int32_t stream_id,
                           const char **trailers, size_t count) {
    (void)s; (void)stream_id; (void)trailers; (void)count;
    return 0;
}

void
cetcd_h2_session_terminate(cetcd_h2_session *s, uint32_t error_code) {
    (void)s; (void)error_code;
}

#endif /* CETCD_HAS_NGHTTP2 */
