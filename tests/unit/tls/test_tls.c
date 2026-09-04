#define _POSIX_C_SOURCE 200809L
#include "cetcd/base.h"
#include "cetcd/tls.h"
#include "cetcd_test.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/wait.h>

static int make_selfsigned_(char *dir, size_t dirsz) {
    char tmpl[] = "/tmp/cetcd-tls-XXXXXX";
    if (!mkdtemp(tmpl)) return -1;
    snprintf(dir, dirsz, "%s", tmpl);
    char cmd[1024];
    snprintf(cmd, sizeof(cmd),
             "openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 "
             "-days 1 -nodes -subj /CN=localhost "
             "-keyout '%s/key.pem' -out '%s/cert.pem' >/dev/null 2>&1",
             dir, dir);
    return system(cmd) == 0 ? 0 : -1;
}

static void cleanup_selfsigned_(const char *dir) {
    char path[300];
    snprintf(path, sizeof(path), "%s/cert.pem", dir);
    unlink(path);
    snprintf(path, sizeof(path), "%s/key.pem", dir);
    unlink(path);
    rmdir(dir);
}

static int pump_all_(cetcd_tls_conn *from, cetcd_tls_conn *to) {
    uint8_t buf[16384];
    int any = 0;
    for (;;) {
        int n = cetcd_tls_pending_out(from, buf, sizeof(buf));
        if (n < 0) return -1;
        if (n == 0) return any;
        if (cetcd_tls_feed(to, buf, (size_t)n) != CETCD_OK) return -1;
        any = 1;
    }
}

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

CETCD_TEST_CASE(tls_ctx_set_alpn_rejects_overlong) {
    /* ALPN protocol ids are length-prefixed by a single byte; an id over 255
     * must be rejected rather than silently truncated into an undersized
     * buffer that then leaks uninitialized memory to OpenSSL. */
    cetcd_tls_ctx *ctx = cetcd_tls_ctx_new();
    CETCD_ASSERT_NOT_NULL(ctx);
    char big[300];
    memset(big, 'a', sizeof(big) - 1);
    big[sizeof(big) - 1] = '\0';
    const char *protos[] = { big };
    int rc = cetcd_tls_set_alpn(ctx, protos, 1);
    CETCD_ASSERT_NE_INT(rc, CETCD_OK);
    cetcd_tls_ctx_free(ctx);
}

CETCD_TEST_CASE(tls_membio_handshake_wants_read) {
    cetcd_tls_ctx *ctx = cetcd_tls_ctx_new();
    CETCD_ASSERT_NOT_NULL(ctx);
    cetcd_tls_conn *cn = cetcd_tls_conn_accept(ctx);
    CETCD_ASSERT_NOT_NULL(cn);
    CETCD_ASSERT_EQ_INT(cetcd_tls_handshake(cn), 0);
    cetcd_tls_conn_free(cn);
    cetcd_tls_ctx_free(ctx);
}

CETCD_TEST_CASE(tls_membio_handshake_roundtrip) {
    char dir[128];
    CETCD_ASSERT_EQ_INT(make_selfsigned_(dir, sizeof(dir)), 0);

    char cert[300], key[300];
    snprintf(cert, sizeof(cert), "%s/cert.pem", dir);
    snprintf(key, sizeof(key), "%s/key.pem", dir);

    cetcd_tls_ctx *sctx = cetcd_tls_ctx_new();
    CETCD_ASSERT_NOT_NULL(sctx);
    CETCD_ASSERT_EQ_INT(cetcd_tls_set_cert(sctx, cert, key), CETCD_OK);
    cetcd_tls_ctx *cctx = cetcd_tls_ctx_new_client();
    CETCD_ASSERT_NOT_NULL(cctx);

    cetcd_tls_conn *srv = cetcd_tls_conn_accept(sctx);
    cetcd_tls_conn *cli = cetcd_tls_conn_connect(cctx);
    CETCD_ASSERT_NOT_NULL(srv);
    CETCD_ASSERT_NOT_NULL(cli);

    int done = 0;
    for (int i = 0; i < 64; i++) {
        int hs = cetcd_tls_handshake(cli);
        CETCD_ASSERT_TRUE(hs >= 0);
        CETCD_ASSERT_TRUE(pump_all_(cli, srv) >= 0);
        hs = cetcd_tls_handshake(srv);
        CETCD_ASSERT_TRUE(hs >= 0);
        CETCD_ASSERT_TRUE(pump_all_(srv, cli) >= 0);
        if (cetcd_tls_handshake(cli) == 1 && cetcd_tls_handshake(srv) == 1) {
            done = 1;
            break;
        }
    }
    CETCD_ASSERT_TRUE(done);

    const char ping[] = "ping";
    CETCD_ASSERT_EQ_INT(cetcd_tls_write(cli, ping, 4), 4);
    CETCD_ASSERT_TRUE(pump_all_(cli, srv) > 0);
    char got[8];
    CETCD_ASSERT_EQ_INT(cetcd_tls_read(srv, got, sizeof(got)), 4);
    CETCD_ASSERT_EQ_INT(memcmp(got, ping, 4), 0);

    const char pong[] = "pong";
    CETCD_ASSERT_EQ_INT(cetcd_tls_write(srv, pong, 4), 4);
    CETCD_ASSERT_TRUE(pump_all_(srv, cli) > 0);
    CETCD_ASSERT_EQ_INT(cetcd_tls_read(cli, got, sizeof(got)), 4);
    CETCD_ASSERT_EQ_INT(memcmp(got, pong, 4), 0);

    cetcd_tls_conn_free(cli);
    cetcd_tls_conn_free(srv);
    cetcd_tls_ctx_free(cctx);
    cetcd_tls_ctx_free(sctx);
    cleanup_selfsigned_(dir);
}

static int handshake_pump_(cetcd_tls_conn *cli, cetcd_tls_conn *srv) {
    for (int i = 0; i < 64; i++) {
        int hs = cetcd_tls_handshake(cli);
        if (hs < 0) return -1;
        if (pump_all_(cli, srv) < 0) return -1;
        hs = cetcd_tls_handshake(srv);
        if (hs < 0) return -1;
        if (pump_all_(srv, cli) < 0) return -1;
        if (cetcd_tls_handshake(cli) == 1 && cetcd_tls_handshake(srv) == 1)
            return 1;
    }
    return 0;
}

CETCD_TEST_CASE(tls_alpn_h2_negotiated) {
    char dir[128];
    CETCD_ASSERT_EQ_INT(make_selfsigned_(dir, sizeof(dir)), 0);
    char cert[300], key[300];
    snprintf(cert, sizeof(cert), "%s/cert.pem", dir);
    snprintf(key, sizeof(key), "%s/key.pem", dir);

    cetcd_tls_ctx *sctx = cetcd_tls_ctx_new();
    cetcd_tls_ctx *cctx = cetcd_tls_ctx_new_client();
    CETCD_ASSERT_NOT_NULL(sctx);
    CETCD_ASSERT_NOT_NULL(cctx);
    CETCD_ASSERT_EQ_INT(cetcd_tls_set_cert(sctx, cert, key), CETCD_OK);
    const char *h2[] = { "h2" };
    CETCD_ASSERT_EQ_INT(cetcd_tls_set_alpn(sctx, h2, 1), CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_tls_set_alpn(cctx, h2, 1), CETCD_OK);

    cetcd_tls_conn *srv = cetcd_tls_conn_accept(sctx);
    cetcd_tls_conn *cli = cetcd_tls_conn_connect(cctx);
    CETCD_ASSERT_EQ_INT(handshake_pump_(cli, srv), 1);

    const uint8_t *proto = NULL;
    unsigned int plen = 0;
    CETCD_ASSERT_EQ_INT(cetcd_tls_alpn_selected(srv, &proto, &plen), CETCD_OK);
    CETCD_ASSERT_EQ_INT((int)plen, 2);
    CETCD_ASSERT_EQ_INT(memcmp(proto, "h2", 2), 0);
    proto = NULL;
    plen = 0;
    CETCD_ASSERT_EQ_INT(cetcd_tls_alpn_selected(cli, &proto, &plen), CETCD_OK);
    CETCD_ASSERT_EQ_INT((int)plen, 2);
    CETCD_ASSERT_EQ_INT(memcmp(proto, "h2", 2), 0);

    cetcd_tls_conn_free(cli);
    cetcd_tls_conn_free(srv);
    cetcd_tls_ctx_free(cctx);
    cetcd_tls_ctx_free(sctx);
    cleanup_selfsigned_(dir);
}

CETCD_TEST_CASE(tls_alpn_omitted_by_client_still_handshakes) {
    char dir[128];
    CETCD_ASSERT_EQ_INT(make_selfsigned_(dir, sizeof(dir)), 0);
    char cert[300], key[300];
    snprintf(cert, sizeof(cert), "%s/cert.pem", dir);
    snprintf(key, sizeof(key), "%s/key.pem", dir);

    cetcd_tls_ctx *sctx = cetcd_tls_ctx_new();
    cetcd_tls_ctx *cctx = cetcd_tls_ctx_new_client();
    CETCD_ASSERT_NOT_NULL(sctx);
    CETCD_ASSERT_NOT_NULL(cctx);
    CETCD_ASSERT_EQ_INT(cetcd_tls_set_cert(sctx, cert, key), CETCD_OK);
    const char *h2[] = { "h2" };
    CETCD_ASSERT_EQ_INT(cetcd_tls_set_alpn(sctx, h2, 1), CETCD_OK);

    cetcd_tls_conn *srv = cetcd_tls_conn_accept(sctx);
    cetcd_tls_conn *cli = cetcd_tls_conn_connect(cctx);
    CETCD_ASSERT_EQ_INT(handshake_pump_(cli, srv), 1);

    const uint8_t *proto = (const uint8_t *)"x";
    unsigned int plen = 99;
    CETCD_ASSERT_EQ_INT(cetcd_tls_alpn_selected(srv, &proto, &plen), CETCD_OK);
    CETCD_ASSERT_EQ_INT((int)plen, 0);

    cetcd_tls_conn_free(cli);
    cetcd_tls_conn_free(srv);
    cetcd_tls_ctx_free(cctx);
    cetcd_tls_ctx_free(sctx);
    cleanup_selfsigned_(dir);
}

CETCD_TEST_CASE(tls_alpn_mismatch_fail_closed) {
    char dir[128];
    CETCD_ASSERT_EQ_INT(make_selfsigned_(dir, sizeof(dir)), 0);
    char cert[300], key[300];
    snprintf(cert, sizeof(cert), "%s/cert.pem", dir);
    snprintf(key, sizeof(key), "%s/key.pem", dir);

    cetcd_tls_ctx *sctx = cetcd_tls_ctx_new();
    cetcd_tls_ctx *cctx = cetcd_tls_ctx_new_client();
    CETCD_ASSERT_NOT_NULL(sctx);
    CETCD_ASSERT_NOT_NULL(cctx);
    CETCD_ASSERT_EQ_INT(cetcd_tls_set_cert(sctx, cert, key), CETCD_OK);
    const char *h2[] = { "h2" };
    const char *h1[] = { "http/1.1" };
    CETCD_ASSERT_EQ_INT(cetcd_tls_set_alpn(sctx, h2, 1), CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_tls_set_alpn(cctx, h1, 1), CETCD_OK);

    cetcd_tls_conn *srv = cetcd_tls_conn_accept(sctx);
    cetcd_tls_conn *cli = cetcd_tls_conn_connect(cctx);
    CETCD_ASSERT_EQ_INT(handshake_pump_(cli, srv), -1);

    cetcd_tls_conn_free(cli);
    cetcd_tls_conn_free(srv);
    cetcd_tls_ctx_free(cctx);
    cetcd_tls_ctx_free(sctx);
    cleanup_selfsigned_(dir);
}

CETCD_TEST_CASE(tls_alpn_selected_null_safety) {
    CETCD_ASSERT_EQ_INT(cetcd_tls_alpn_selected(NULL, NULL, NULL), CETCD_ERR_INVAL);
}

CETCD_TEST_CASE(tls_blocking_connect_roundtrip) {
    char dir[128];
    CETCD_ASSERT_EQ_INT(make_selfsigned_(dir, sizeof(dir)), 0);
    char cert[300], key[300];
    snprintf(cert, sizeof(cert), "%s/cert.pem", dir);
    snprintf(key, sizeof(key), "%s/key.pem", dir);

    int sp[2];
    CETCD_ASSERT_EQ_INT(socketpair(AF_UNIX, SOCK_STREAM, 0, sp), 0);
    pid_t pid = fork();
    CETCD_ASSERT_TRUE(pid >= 0);
    if (pid == 0) {
        alarm(2);
        close(sp[0]);
        cetcd_tls_ctx *sctx = cetcd_tls_ctx_new();
        if (!sctx || cetcd_tls_set_cert(sctx, cert, key) != CETCD_OK) _exit(1);
        cetcd_tls_conn *srv = cetcd_tls_accept(sctx, sp[1]);
        if (!srv) _exit(2);
        char got[8];
        if (cetcd_tls_read(srv, got, 4) != 4) _exit(3);
        if (memcmp(got, "ping", 4) != 0) _exit(4);
        if (cetcd_tls_write(srv, "pong", 4) != 4) _exit(5);
        cetcd_tls_conn_free(srv);
        cetcd_tls_ctx_free(sctx);
        close(sp[1]);
        _exit(0);
    }
    close(sp[1]);
    cetcd_tls_ctx *cctx = cetcd_tls_ctx_new_client();
    CETCD_ASSERT_NOT_NULL(cctx);
    CETCD_ASSERT_EQ_INT(cetcd_tls_set_ca(cctx, cert), CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_tls_set_verify_peer(cctx, 0), CETCD_OK);
    cetcd_tls_conn *cli = cetcd_tls_connect(cctx, sp[0]);
    CETCD_ASSERT_NOT_NULL(cli);
    CETCD_ASSERT_EQ_INT(cetcd_tls_write(cli, "ping", 4), 4);
    char got[8];
    CETCD_ASSERT_EQ_INT(cetcd_tls_read(cli, got, 4), 4);
    CETCD_ASSERT_EQ_INT(memcmp(got, "pong", 4), 0);
    cetcd_tls_conn_free(cli);
    cetcd_tls_ctx_free(cctx);
    close(sp[0]);
    int st = 0;
    waitpid(pid, &st, 0);
    CETCD_ASSERT_EQ_INT(WEXITSTATUS(st), 0);
    cleanup_selfsigned_(dir);
}

CETCD_TEST_CASE(tls_blocking_connect_verify_fail_closed) {
    char dir[128];
    CETCD_ASSERT_EQ_INT(make_selfsigned_(dir, sizeof(dir)), 0);
    char cert[300], key[300];
    snprintf(cert, sizeof(cert), "%s/cert.pem", dir);
    snprintf(key, sizeof(key), "%s/key.pem", dir);

    int sp[2];
    CETCD_ASSERT_EQ_INT(socketpair(AF_UNIX, SOCK_STREAM, 0, sp), 0);
    pid_t pid = fork();
    CETCD_ASSERT_TRUE(pid >= 0);
    if (pid == 0) {
        alarm(2);
        close(sp[0]);
        cetcd_tls_ctx *sctx = cetcd_tls_ctx_new();
        if (!sctx || cetcd_tls_set_cert(sctx, cert, key) != CETCD_OK) _exit(1);
        cetcd_tls_conn *srv = cetcd_tls_accept(sctx, sp[1]);
        if (srv) {
            cetcd_tls_conn_free(srv);
            cetcd_tls_ctx_free(sctx);
            close(sp[1]);
            _exit(0);
        }
        cetcd_tls_ctx_free(sctx);
        close(sp[1]);
        _exit(2);
    }
    close(sp[1]);
    cetcd_tls_ctx *cctx = cetcd_tls_ctx_new_client();
    CETCD_ASSERT_NOT_NULL(cctx);
    CETCD_ASSERT_EQ_INT(cetcd_tls_set_verify_peer(cctx, 0), CETCD_OK);
    cetcd_tls_conn *cli = cetcd_tls_connect(cctx, sp[0]);
    CETCD_ASSERT_TRUE(cli == NULL);
    cetcd_tls_ctx_free(cctx);
    close(sp[0]);
    int st = 0;
    waitpid(pid, &st, 0);
    cleanup_selfsigned_(dir);
}

CETCD_TEST_CASE(tls_blocking_connect_null_ctx) {
    CETCD_ASSERT_TRUE(cetcd_tls_connect(NULL, 0) == NULL);
}

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(tls_ctx_create_destroy),
    CETCD_TEST_ENTRY(tls_ctx_set_alpn),
    CETCD_TEST_ENTRY(tls_ctx_set_alpn_rejects_overlong),
    CETCD_TEST_ENTRY(tls_ctx_set_nonexistent_cert),
    CETCD_TEST_ENTRY(tls_membio_handshake_wants_read),
    CETCD_TEST_ENTRY(tls_membio_handshake_roundtrip),
    CETCD_TEST_ENTRY(tls_alpn_h2_negotiated),
    CETCD_TEST_ENTRY(tls_alpn_omitted_by_client_still_handshakes),
    CETCD_TEST_ENTRY(tls_alpn_mismatch_fail_closed),
    CETCD_TEST_ENTRY(tls_alpn_selected_null_safety),
    CETCD_TEST_ENTRY(tls_blocking_connect_roundtrip),
    CETCD_TEST_ENTRY(tls_blocking_connect_verify_fail_closed),
    CETCD_TEST_ENTRY(tls_blocking_connect_null_ctx),
CETCD_TEST_LIST_END

CETCD_TEST_MAIN()
