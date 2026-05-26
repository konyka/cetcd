#include "cetcd/base.h"
#include "cetcd/http2.h"
#include "cetcd_test.h"

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

CETCD_TEST_CASE(h2_session_create_destroy) {
    cetcd_h2_callbacks cbs = {0};
    cetcd_h2_session *s = cetcd_h2_session_new(&cbs);
    CETCD_ASSERT_NOT_NULL(s);
    cetcd_h2_session_free(s);
}

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
    memcpy(ctx->data_buf + ctx->data_len, data, len);
    ctx->data_len += len;
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

    cetcd_h2_session_free(s);
}

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(grpc_encode_decode_roundtrip),
    CETCD_TEST_ENTRY(grpc_encode_compressed_flag),
    CETCD_TEST_ENTRY(grpc_decode_too_short),
    CETCD_TEST_ENTRY(h2_session_create_destroy),
    CETCD_TEST_ENTRY(h2_client_preface_and_request),
CETCD_TEST_LIST_END

CETCD_TEST_MAIN()
