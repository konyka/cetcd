#include "cetcd/base.h"
#include "cetcd/tls.h"
#include "cetcd_test.h"

CETCD_TEST_CASE(tls_ctx_create_destroy) {
    cetcd_tls_ctx *ctx = cetcd_tls_ctx_new();
    CETCD_ASSERT_NOT_NULL(ctx);
    cetcd_tls_ctx_free(ctx);
}

CETCD_TEST_CASE(tls_ctx_set_alpn) {
    cetcd_tls_ctx *ctx = cetcd_tls_ctx_new();
    CETCD_ASSERT_NOT_NULL(ctx);
    const char *protos[] = {"h2"};
    int rc = cetcd_tls_set_alpn(ctx, protos, 1);
    CETCD_ASSERT_EQ_INT(rc, CETCD_OK);
    cetcd_tls_ctx_free(ctx);
}

CETCD_TEST_CASE(tls_ctx_set_nonexistent_cert) {
    cetcd_tls_ctx *ctx = cetcd_tls_ctx_new();
    CETCD_ASSERT_NOT_NULL(ctx);
    int rc = cetcd_tls_set_cert(ctx, "/nonexistent/cert.pem", "/nonexistent/key.pem");
    CETCD_ASSERT_NE_INT(rc, CETCD_OK);
    cetcd_tls_ctx_free(ctx);
}

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(tls_ctx_create_destroy),
    CETCD_TEST_ENTRY(tls_ctx_set_alpn),
    CETCD_TEST_ENTRY(tls_ctx_set_nonexistent_cert),
CETCD_TEST_LIST_END

CETCD_TEST_MAIN()
