#include "cetcd/base.h"
#include "cetcd/http2.h"
#include "cetcd_test.h"

#include <stdlib.h>
#include <string.h>

/* ========================================================================== */
/*  gRPC framing tests (always run)                                           */
/* ========================================================================== */

CETCD_TEST_CASE(grpc_encode_decode_roundtrip) {
    const uint8_t msg[] = {0x0a, 0x05, 'h', 'e', 'l', 'l', 'o'};
    uint8_t *frame = NULL;
    size_t frame_len = 0;

    int rc = cetcd_grpc_encode(msg, sizeof(msg), false, &frame, &frame_len);
    CETCD_ASSERT_EQ_INT(rc, CETCD_OK);
    CETCD_ASSERT_NOT_NULL(frame);
    CETCD_ASSERT_TRUE(frame_len == 1 + 4 + sizeof(msg));
    CETCD_ASSERT_EQ_INT(frame[0], 0);

    bool compressed = true;
    uint8_t *decoded = NULL;
    size_t decoded_len = 0;
    rc = cetcd_grpc_decode(frame, frame_len, &compressed, &decoded, &decoded_len);
    CETCD_ASSERT_EQ_INT(rc, CETCD_OK);
    CETCD_ASSERT_FALSE(compressed);
    CETCD_ASSERT_EQ_INT((int)decoded_len, (int)sizeof(msg));
    CETCD_ASSERT_TRUE(memcmp(decoded, msg, decoded_len) == 0);

    free(frame);
    free(decoded);
}

CETCD_TEST_CASE(grpc_encode_compressed_flag) {
    const uint8_t msg[] = {0x01};
    uint8_t *frame = NULL;
    size_t frame_len = 0;

    cetcd_grpc_encode(msg, sizeof(msg), true, &frame, &frame_len);
    CETCD_ASSERT_EQ_INT(frame[0], 1);

    free(frame);
}

CETCD_TEST_CASE(grpc_decode_too_short) {
    const uint8_t tiny[] = {0x00, 0x00, 0x00};
    bool compressed;
    uint8_t *msg = NULL;
    size_t msg_len = 0;
    int rc = cetcd_grpc_decode(tiny, sizeof(tiny), &compressed, &msg, &msg_len);
    CETCD_ASSERT_NE_INT(rc, CETCD_OK);
}

CETCD_TEST_CASE(grpc_encode_empty_message) {
    uint8_t *frame = NULL;
    size_t frame_len = 0;

    int rc = cetcd_grpc_encode(NULL, 0, false, &frame, &frame_len);
    CETCD_ASSERT_EQ_INT(rc, CETCD_OK);
    CETCD_ASSERT_TRUE(frame_len == 5);
    CETCD_ASSERT_EQ_INT(frame[0], 0);

    bool compressed = false;
    uint8_t *decoded = NULL;
    size_t decoded_len = 0;
    rc = cetcd_grpc_decode(frame, frame_len, &compressed, &decoded, &decoded_len);
    CETCD_ASSERT_EQ_INT(rc, CETCD_OK);
    CETCD_ASSERT_EQ_INT((int)decoded_len, 0);
    CETCD_ASSERT_TRUE(decoded == NULL);

    free(frame);
}

/* ========================================================================== */
/*  Session create/destroy (always run)                                       */
/* ========================================================================== */

CETCD_TEST_CASE(h2_session_create_destroy) {
    cetcd_h2_callbacks cbs = {0};
    cetcd_h2_session *s = cetcd_h2_session_new(&cbs);
    CETCD_ASSERT_NOT_NULL(s);
    cetcd_h2_session_free(s);
}

CETCD_TEST_CASE(h2_session_null_safety) {
    /* These must not crash */
    cetcd_h2_session_free(NULL);
    CETCD_ASSERT_EQ_INT(cetcd_h2_feed(NULL, NULL, 0), -1);
    CETCD_ASSERT_EQ_INT(cetcd_h2_send_pending(NULL, NULL, NULL), -1);
    CETCD_ASSERT_EQ_INT(cetcd_h2_submit_response(NULL, 0, NULL, 0, NULL, 0, false), -1);
    cetcd_h2_session_terminate(NULL, 0);
}

/* ========================================================================== */
/*  Server-side request state (for callback tests)                            */
/* ========================================================================== */

typedef struct {
    char method[16];
    char path[128];
    char content_type[64];
    bool got_request;
    bool got_data;
    bool got_end_stream;
    uint8_t data_buf[256];
    size_t data_len;
} h2_test_ctx;

static void test_on_request(cetcd_h2_session *sess, int32_t stream_id,
                             const char *method, const char *path,
                             const char *content_type, void *udata) {
    (void)sess; (void)stream_id;
    h2_test_ctx *ctx = (h2_test_ctx*)udata;
    snprintf(ctx->method, sizeof(ctx->method), "%s", method);
    snprintf(ctx->path, sizeof(ctx->path), "%s", path);
    snprintf(ctx->content_type, sizeof(ctx->content_type), "%s", content_type);
    ctx->got_request = true;
}

static void test_on_data(cetcd_h2_session *sess, int32_t stream_id,
                          const uint8_t *data, size_t len,
                          bool end_stream, void *udata) {
    (void)sess; (void)stream_id;
    h2_test_ctx *ctx = (h2_test_ctx*)udata;
    if (data && len > 0 && ctx->data_len + len < sizeof(ctx->data_buf)) {
        memcpy(ctx->data_buf + ctx->data_len, data, len);
        ctx->data_len += len;
    }
    ctx->got_data = true;
    ctx->got_end_stream = end_stream;
}

CETCD_TEST_CASE(h2_client_preface_and_request) {
    h2_test_ctx tc = {0};
    cetcd_h2_callbacks cbs = {
        .on_request = test_on_request,
        .on_data = test_on_data,
        .udata = &tc
    };
    cetcd_h2_session *s = cetcd_h2_session_new(&cbs);
    CETCD_ASSERT_NOT_NULL(s);

    const char *preface = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
    int rc = cetcd_h2_feed(s, (const uint8_t*)preface, strlen(preface));
    CETCD_ASSERT_EQ_INT(rc, 0);

    /* Feed an empty SETTINGS frame: len=0, type=4, flags=0, stream_id=0 */
    uint8_t settings_frame[] = {0,0,0, 4, 0, 0,0,0,0};
    rc = cetcd_h2_feed(s, settings_frame, sizeof(settings_frame));
    CETCD_ASSERT_EQ_INT(rc, 0);

    cetcd_h2_session_free(s);
}

/* ========================================================================== */
/*  Full HTTP/2 roundtrip tests (require nghttp2)                             */
/* ========================================================================== */

#ifdef CETCD_HAS_NGHTTP2
#include <nghttp2/nghttp2.h>

/* nghttp2 doesn't always expose NGHTTP2_NV_MAKE as a public macro */
#ifndef NGHTTP2_NV_MAKE
#define NGHTTP2_NV_MAKE(NAME, VALUE)                                         \
  {                                                                         \
    (uint8_t *)(NAME), (uint8_t *)(VALUE),                                   \
    sizeof(NAME) - 1, sizeof(VALUE) - 1,                                     \
    NGHTTP2_NV_FLAG_NONE                                                    \
  }
#endif

/* -- Collect helper: accumulates write_fn output into a buffer -------------- */

typedef struct {
    uint8_t *buf;
    size_t   len;
    size_t   cap;
} collect_ctx_;

static int collect_write_fn_(const uint8_t *buf, size_t len, void *ctx) {
    collect_ctx_ *c = (collect_ctx_ *)ctx;
    if (c->len + len > c->cap) {
        size_t nc = c->cap ? c->cap * 2 : 4096;
        while (nc < c->len + len) nc *= 2;
        uint8_t *nb = (uint8_t *)realloc(c->buf, nc);
        if (!nb) return -1;
        c->buf = nb;
        c->cap = nc;
    }
    memcpy(c->buf + c->len, buf, len);
    c->len += len;
    return 0;
}

/* -- Client-side state and callbacks --------------------------------------- */

typedef struct {
    int     status_code;
    bool    got_status;
    bool    got_data;
    bool    got_end_stream;
    uint8_t data_buf[256];
    size_t  data_len;
    char    content_type[64];
} h2_client_ctx_;

static int
client_on_header_(nghttp2_session *session,
                   const nghttp2_frame *frame,
                   const uint8_t *name, size_t namelen,
                   const uint8_t *value, size_t valuelen,
                   uint8_t flags, void *user_data) {
    (void)session; (void)flags;
    h2_client_ctx_ *c = (h2_client_ctx_ *)user_data;
    if (frame->hd.type != NGHTTP2_HEADERS) return 0;
    if (namelen == 7 && memcmp(name, ":status", 7) == 0) {
        char tmp[8];
        size_t n = valuelen < sizeof(tmp) - 1 ? valuelen : sizeof(tmp) - 1;
        memcpy(tmp, value, n);
        tmp[n] = '\0';
        c->status_code = atoi(tmp);
        c->got_status = true;
    } else if (namelen == 12 && memcmp(name, "content-type", 12) == 0) {
        size_t n = valuelen < sizeof(c->content_type) - 1
                       ? valuelen : sizeof(c->content_type) - 1;
        memcpy(c->content_type, value, n);
        c->content_type[n] = '\0';
    }
    return 0;
}

static int
client_on_data_(nghttp2_session *session,
                 uint8_t flags,
                 int32_t stream_id,
                 const uint8_t *data, size_t len,
                 void *user_data) {
    (void)session; (void)stream_id;
    h2_client_ctx_ *c = (h2_client_ctx_ *)user_data;
    if (data && len > 0 && c->data_len + len < sizeof(c->data_buf)) {
        memcpy(c->data_buf + c->data_len, data, len);
        c->data_len += len;
    }
    c->got_data = true;
    c->got_end_stream = (flags & NGHTTP2_FLAG_END_STREAM) != 0;
    return 0;
}

static int
client_on_frame_recv_(nghttp2_session *session,
                       const nghttp2_frame *frame,
                       void *user_data) {
    (void)session;
    h2_client_ctx_ *c = (h2_client_ctx_ *)user_data;
    /* Detect END_STREAM on HEADERS (trailers) or DATA frames */
    if (frame->hd.flags & NGHTTP2_FLAG_END_STREAM)
        c->got_end_stream = true;
    return 0;
}

/* -- Client request body data provider ------------------------------------- */

static nghttp2_ssize
client_body_read_cb_(nghttp2_session *session,
                      int32_t stream_id,
                      uint8_t *buf, size_t length,
                      uint32_t *data_flags,
                      nghttp2_data_source *source,
                      void *user_data) {
    (void)session; (void)stream_id; (void)source; (void)user_data;
    const char body[] = "hello-world";
    size_t bl = sizeof(body) - 1;
    if (bl > length) bl = length;
    memcpy(buf, body, bl);
    *data_flags |= NGHTTP2_DATA_FLAG_EOF;
    return (nghttp2_ssize)bl;
}

/* -- Pump helper: exchange data between client and server ------------------ */

static void
pump_sessions_(cetcd_h2_session *server, nghttp2_session *client) {
    /* Ensure client has submitted its initial SETTINGS */
    nghttp2_submit_settings(client, NGHTTP2_FLAG_NONE, NULL, 0);

    for (int round = 0; round < 50; round++) {
        bool any = false;

        /* client → server (ALL pending chunks) */
        for (;;) {
            const uint8_t *out = NULL;
            nghttp2_ssize n = nghttp2_session_mem_send2(client, &out);
            if (n <= 0) break;
            cetcd_h2_feed(server, out, (size_t)n);
            any = true;
        }

        /* server → client (ALL pending output) */
        collect_ctx_ c = {0};
        cetcd_h2_send_pending(server, collect_write_fn_, &c);
        if (c.len > 0) {
            nghttp2_session_mem_recv2(client, c.buf, c.len);
            any = true;
        }
        free(c.buf);

        if (!any) break;
    }
}

/* -- Tests ----------------------------------------------------------------- */

CETCD_TEST_CASE(h2_server_receives_request) {
    h2_test_ctx tc = {0};
    cetcd_h2_callbacks scbs = {
        .on_request = test_on_request,
        .on_data    = test_on_data,
        .udata      = &tc
    };
    cetcd_h2_session *server = cetcd_h2_session_new(&scbs);
    CETCD_ASSERT_NOT_NULL(server);

    /* Create client session */
    nghttp2_session_callbacks *ccb;
    nghttp2_session_callbacks_new(&ccb);
    nghttp2_session *client;
    nghttp2_session_client_new(&client, ccb, NULL);
    nghttp2_session_callbacks_del(ccb);

    /* Submit request with body */
    nghttp2_nv hdrs[] = {
        NGHTTP2_NV_MAKE(":method",       "POST"),
        NGHTTP2_NV_MAKE(":path",         "/etcdserverpb.KV/Put"),
        NGHTTP2_NV_MAKE(":scheme",       "http"),
        NGHTTP2_NV_MAKE("content-type",  "application/grpc"),
    };
    nghttp2_data_provider2 dp;
    dp.read_callback = client_body_read_cb_;
    dp.source.ptr    = NULL;

    int32_t sid = nghttp2_submit_request2(client, NULL, hdrs, 4, &dp, NULL);
    CETCD_ASSERT_TRUE(sid > 0);

    pump_sessions_(server, client);

    /* Verify server received request headers */
    CETCD_ASSERT_TRUE(tc.got_request);
    CETCD_ASSERT_EQ_STR(tc.method, "POST");
    CETCD_ASSERT_EQ_STR(tc.path, "/etcdserverpb.KV/Put");
    CETCD_ASSERT_EQ_STR(tc.content_type, "application/grpc");

    /* Verify server received request body */
    CETCD_ASSERT_TRUE(tc.got_data);
    CETCD_ASSERT_TRUE(tc.got_end_stream);
    CETCD_ASSERT_TRUE(tc.data_len == 11); /* "hello-world" */
    CETCD_ASSERT_TRUE(memcmp(tc.data_buf, "hello-world", 11) == 0);

    nghttp2_session_del(client);
    cetcd_h2_session_free(server);
}

CETCD_TEST_CASE(h2_full_roundtrip) {
    h2_test_ctx tc = {0};
    cetcd_h2_callbacks scbs = {
        .on_request = test_on_request,
        .on_data    = test_on_data,
        .udata      = &tc
    };
    cetcd_h2_session *server = cetcd_h2_session_new(&scbs);
    CETCD_ASSERT_NOT_NULL(server);

    /* Create client session with response callbacks */
    nghttp2_session_callbacks *ccb;
    nghttp2_session_callbacks_new(&ccb);
    nghttp2_session_callbacks_set_on_header_callback(ccb, client_on_header_);
    nghttp2_session_callbacks_set_on_data_chunk_recv_callback(ccb, client_on_data_);
    nghttp2_session_callbacks_set_on_frame_recv_callback(ccb, client_on_frame_recv_);

    h2_client_ctx_ cctx = {0};
    nghttp2_session *client;
    nghttp2_session_client_new(&client, ccb, &cctx);
    nghttp2_session_callbacks_del(ccb);

    /* Submit client request */
    nghttp2_nv hdrs[] = {
        NGHTTP2_NV_MAKE(":method",       "POST"),
        NGHTTP2_NV_MAKE(":path",         "/etcdserverpb.Lease/LeaseGrant"),
        NGHTTP2_NV_MAKE(":scheme",       "http"),
        NGHTTP2_NV_MAKE("content-type",  "application/grpc"),
    };
    nghttp2_data_provider2 dp;
    dp.read_callback = client_body_read_cb_;
    dp.source.ptr    = NULL;

    int32_t sid = nghttp2_submit_request2(client, NULL, hdrs, 4, &dp, NULL);
    CETCD_ASSERT_TRUE(sid > 0);

    /* Pump: client request → server */
    pump_sessions_(server, client);

    /* Verify server got the request */
    CETCD_ASSERT_TRUE(tc.got_request);
    CETCD_ASSERT_EQ_STR(tc.path, "/etcdserverpb.Lease/LeaseGrant");

    /* Server submits response */
    const char *resp_hdrs[] = {
        ":status",      "200",
        "content-type", "application/grpc",
    };
    const uint8_t resp_body[] = {0x08, 0x01, 0x10, 0x2a}; /* proto: id=1, ttl=42 */
    int rc = cetcd_h2_submit_response(server, sid, resp_hdrs, 4,
                                       resp_body, sizeof(resp_body), true);
    CETCD_ASSERT_EQ_INT(rc, 0);

    /* Pump: server response → client */
    pump_sessions_(server, client);

    /* Verify client received response */
    CETCD_ASSERT_TRUE(cctx.got_status);
    CETCD_ASSERT_EQ_INT(cctx.status_code, 200);
    CETCD_ASSERT_TRUE(cctx.got_data);
    CETCD_ASSERT_TRUE(cctx.got_end_stream);
    CETCD_ASSERT_EQ_INT((int)cctx.data_len, (int)sizeof(resp_body));
    CETCD_ASSERT_TRUE(memcmp(cctx.data_buf, resp_body, sizeof(resp_body)) == 0);
    CETCD_ASSERT_EQ_STR(cctx.content_type, "application/grpc");

    nghttp2_session_del(client);
    cetcd_h2_session_free(server);
}

CETCD_TEST_CASE(h2_submit_response_with_trailers) {
    h2_test_ctx tc = {0};
    cetcd_h2_callbacks scbs = {
        .on_request = test_on_request,
        .on_data    = test_on_data,
        .udata      = &tc
    };
    cetcd_h2_session *server = cetcd_h2_session_new(&scbs);
    CETCD_ASSERT_NOT_NULL(server);

    nghttp2_session_callbacks *ccb;
    nghttp2_session_callbacks_new(&ccb);
    nghttp2_session_callbacks_set_on_header_callback(ccb, client_on_header_);
    nghttp2_session_callbacks_set_on_data_chunk_recv_callback(ccb, client_on_data_);
    nghttp2_session_callbacks_set_on_frame_recv_callback(ccb, client_on_frame_recv_);

    h2_client_ctx_ cctx = {0};
    nghttp2_session *client;
    nghttp2_session_client_new(&client, ccb, &cctx);
    nghttp2_session_callbacks_del(ccb);

    /* Submit request */
    nghttp2_nv hdrs[] = {
        NGHTTP2_NV_MAKE(":method", "POST"),
        NGHTTP2_NV_MAKE(":path",   "/etcdserverpb.KV/Range"),
        NGHTTP2_NV_MAKE(":scheme", "http"),
    };
    int32_t sid = nghttp2_submit_headers(client, NGHTTP2_FLAG_END_HEADERS | NGHTTP2_FLAG_END_STREAM,
                                          -1, NULL, hdrs, 3, NULL);
    CETCD_ASSERT_TRUE(sid > 0);

    pump_sessions_(server, client);
    CETCD_ASSERT_TRUE(tc.got_request);

    /* Submit response with body but NOT end_stream (trailers will follow) */
    const char *resp_hdrs[] = {
        ":status",      "200",
        "content-type", "application/grpc",
    };
    const uint8_t resp_body[] = {0x0a, 0x02, 0x08, 0x01};
    cetcd_h2_submit_response(server, sid, resp_hdrs, 4,
                              resp_body, sizeof(resp_body), false);

    /* Pump to send response HEADERS + DATA */
    pump_sessions_(server, client);

    /* Submit trailers (END_STREAM) */
    const char *trailers[] = {
        "grpc-status", "0",
        "grpc-message", "",
    };
    int rc = cetcd_h2_submit_trailers(server, sid, trailers, 4);
    CETCD_ASSERT_EQ_INT(rc, 0);

    pump_sessions_(server, client);

    /* Client should have received response data */
    CETCD_ASSERT_TRUE(cctx.got_status);
    CETCD_ASSERT_EQ_INT(cctx.status_code, 200);
    CETCD_ASSERT_TRUE(cctx.got_data);
    CETCD_ASSERT_TRUE(cctx.got_end_stream);
    CETCD_ASSERT_EQ_INT((int)cctx.data_len, (int)sizeof(resp_body));

    nghttp2_session_del(client);
    cetcd_h2_session_free(server);
}

CETCD_TEST_CASE(h2_server_terminate) {
    cetcd_h2_session *server = cetcd_h2_session_new(NULL);
    CETCD_ASSERT_NOT_NULL(server);

    /* Should not crash */
    cetcd_h2_session_terminate(server, 0 /* NGHTTP2_NO_ERROR */);

    cetcd_h2_session_free(server);
}

#endif /* CETCD_HAS_NGHTTP2 */

/* ========================================================================== */
/*  Test list                                                                 */
/* ========================================================================== */

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(grpc_encode_decode_roundtrip),
    CETCD_TEST_ENTRY(grpc_encode_compressed_flag),
    CETCD_TEST_ENTRY(grpc_decode_too_short),
    CETCD_TEST_ENTRY(grpc_encode_empty_message),
    CETCD_TEST_ENTRY(h2_session_create_destroy),
    CETCD_TEST_ENTRY(h2_session_null_safety),
    CETCD_TEST_ENTRY(h2_client_preface_and_request),
#ifdef CETCD_HAS_NGHTTP2
    CETCD_TEST_ENTRY(h2_server_receives_request),
    CETCD_TEST_ENTRY(h2_full_roundtrip),
    CETCD_TEST_ENTRY(h2_submit_response_with_trailers),
    CETCD_TEST_ENTRY(h2_server_terminate),
#endif
CETCD_TEST_LIST_END

CETCD_TEST_MAIN()
