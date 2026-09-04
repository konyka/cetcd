#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>

#include "cetcd/tls.h"
#include "cetcd/base.h"

#if CETCD_HAS_OPENSSL
#include <openssl/ssl.h>
#include <openssl/err.h>
#include <openssl/x509.h>
#include <openssl/bio.h>

struct cetcd_tls_ctx {
    SSL_CTX        *ssl_ctx;
    unsigned char  *alpn;
    unsigned int    alpn_len;
};

struct cetcd_tls_conn {
    SSL *ssl;
    int  fd;
};

static cetcd_tls_ctx *ctx_new_(const SSL_METHOD *method) {
    cetcd_tls_ctx *ctx = (cetcd_tls_ctx *)calloc(1, sizeof(*ctx));
    if (ctx == NULL) return NULL;

    ctx->ssl_ctx = SSL_CTX_new(method);
    if (ctx->ssl_ctx == NULL) {
        free(ctx);
        return NULL;
    }

#if defined(SSL_CTX_set_min_proto_version)
    SSL_CTX_set_min_proto_version(ctx->ssl_ctx, TLS1_2_VERSION);
#endif
    SSL_CTX_set_options(ctx->ssl_ctx,
                        SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3 | SSL_OP_NO_TLSv1);

    return ctx;
}

cetcd_tls_ctx *cetcd_tls_ctx_new(void) {
    return ctx_new_(TLS_server_method());
}

cetcd_tls_ctx *cetcd_tls_ctx_new_client(void) {
    return ctx_new_(TLS_client_method());
}

void cetcd_tls_ctx_free(cetcd_tls_ctx *ctx) {
    if (ctx == NULL) return;
    if (ctx->ssl_ctx) SSL_CTX_free(ctx->ssl_ctx);
    free(ctx->alpn);
    free(ctx);
}

int cetcd_tls_set_cert(cetcd_tls_ctx *ctx, const char *cert_path, const char *key_path) {
    if (ctx == NULL || ctx->ssl_ctx == NULL) return CETCD_ERR_INVAL;
    if (cert_path == NULL || key_path == NULL) return CETCD_ERR_INVAL;

    if (SSL_CTX_use_certificate_file(ctx->ssl_ctx, cert_path, SSL_FILETYPE_PEM) <= 0) {
        return CETCD_ERR_IO;
    }
    if (SSL_CTX_use_PrivateKey_file(ctx->ssl_ctx, key_path, SSL_FILETYPE_PEM) <= 0) {
        return CETCD_ERR_IO;
    }
    if (!SSL_CTX_check_private_key(ctx->ssl_ctx)) {
        return CETCD_ERR_IO;
    }
    return CETCD_OK;
}

int cetcd_tls_set_ca(cetcd_tls_ctx *ctx, const char *ca_path) {
    if (ctx == NULL || ctx->ssl_ctx == NULL) return CETCD_ERR_INVAL;
    if (ca_path == NULL) return CETCD_ERR_INVAL;
    if (SSL_CTX_load_verify_locations(ctx->ssl_ctx, ca_path, NULL) == 0) {
        return CETCD_ERR_IO;
    }
    return CETCD_OK;
}

int cetcd_tls_set_verify_peer(cetcd_tls_ctx *ctx, int require_cert) {
    if (ctx == NULL || ctx->ssl_ctx == NULL) return CETCD_ERR_INVAL;
    int mode = SSL_VERIFY_PEER;
    if (require_cert) mode |= SSL_VERIFY_FAIL_IF_NO_PEER_CERT;
    SSL_CTX_set_verify(ctx->ssl_ctx, mode, NULL);
    return CETCD_OK;
}

static int suite_to_openssl_(const char *in, size_t n, char *out, size_t cap) {
    if (!in || !out || cap < 2) return -1;
    while (n > 0 && (*in == ' ' || *in == '\t')) { in++; n--; }
    while (n > 0 && (in[n - 1] == ' ' || in[n - 1] == '\t')) n--;
    if (n == 0) return -1;
    if (n >= 4 && in[0] == 'T' && in[1] == 'L' && in[2] == 'S' && in[3] == '_') {
        in += 4;
        n -= 4;
    }
    size_t o = 0;
    for (size_t i = 0; i < n; i++) {
        if (in[i] == '_' && i + 6 <= n &&
            in[i + 1] == 'W' && in[i + 2] == 'I' && in[i + 3] == 'T' &&
            in[i + 4] == 'H' && in[i + 5] == '_') {
            if (o + 1 >= cap) return -1;
            out[o++] = '-';
            i += 5;
            continue;
        }
        if (in[i] == '_') {
            if (i + 1 < n && in[i + 1] >= '0' && in[i + 1] <= '9')
                continue;
            if (o + 1 >= cap) return -1;
            out[o++] = '-';
            continue;
        }
        if (o + 1 >= cap) return -1;
        out[o++] = in[i];
    }
    if (o == 0) return -1;
    out[o] = '\0';
    return 0;
}

int cetcd_tls_set_ciphers(cetcd_tls_ctx *ctx, const char *list) {
    if (ctx == NULL || ctx->ssl_ctx == NULL || list == NULL) return CETCD_ERR_INVAL;
    char ossl[1024];
    size_t opos = 0;
    const char *p = list;
    int any = 0;
    while (*p) {
        while (*p == ' ' || *p == '\t' || *p == ',') p++;
        if (!*p) break;
        const char *start = p;
        while (*p && *p != ',') p++;
        char one[256];
        if (suite_to_openssl_(start, (size_t)(p - start), one, sizeof(one)) != 0)
            return CETCD_ERR_INVAL;
        size_t olen = strlen(one);
        if (any) {
            if (opos + 1 >= sizeof(ossl)) return CETCD_ERR_OVERFLOW;
            ossl[opos++] = ':';
        }
        if (opos + olen >= sizeof(ossl)) return CETCD_ERR_OVERFLOW;
        memcpy(ossl + opos, one, olen);
        opos += olen;
        any = 1;
    }
    if (!any) return CETCD_ERR_INVAL;
    ossl[opos] = '\0';
    if (SSL_CTX_set_cipher_list(ctx->ssl_ctx, ossl) != 1)
        return CETCD_ERR_INVAL;
    return CETCD_OK;
}

static int alpn_select_(SSL *ssl, const unsigned char **out, unsigned char *outlen,
                        const unsigned char *in, unsigned int inlen, void *arg) {
    (void)ssl;
    cetcd_tls_ctx *ctx = (cetcd_tls_ctx *)arg;
    if (!ctx || !ctx->alpn || ctx->alpn_len == 0 || !in || inlen == 0)
        return SSL_TLSEXT_ERR_NOACK;
    unsigned char *sel = NULL;
    unsigned char sel_len = 0;
    int st = SSL_select_next_proto(&sel, &sel_len,
                                   ctx->alpn, ctx->alpn_len, in, inlen);
    if (st != OPENSSL_NPN_NEGOTIATED || !sel || sel_len == 0)
        return SSL_TLSEXT_ERR_ALERT_FATAL;
    *out = sel;
    *outlen = sel_len;
    return SSL_TLSEXT_ERR_OK;
}

int cetcd_tls_set_alpn(cetcd_tls_ctx *ctx, const char **protocols, size_t count) {
    if (ctx == NULL || ctx->ssl_ctx == NULL) return CETCD_ERR_INVAL;
    if (protocols == NULL || count == 0) {
        return CETCD_OK;
    }

    size_t total = 0;
    for (size_t i = 0; i < count; ++i) {
        const char *p = protocols[i];
        if (p == NULL) return CETCD_ERR_INVAL;
        size_t plen = strlen(p);
        if (plen > 255) return CETCD_ERR_OVERFLOW;
        if (plen > SIZE_MAX - 1 || total > SIZE_MAX - (1 + plen)) return CETCD_ERR_OVERFLOW;
        total += 1 + plen;
    }
    if (total > UINT_MAX) return CETCD_ERR_OVERFLOW;
    unsigned char *buf = (unsigned char *)malloc(total);
    if (buf == NULL) return CETCD_ERR_NOMEM;
    unsigned char *p = buf;
    for (size_t i = 0; i < count; ++i) {
        size_t len = strlen(protocols[i]);
        *p++ = (unsigned char)len;
        memcpy(p, protocols[i], len);
        p += len;
    }

    /* ClientHello offer (client ctx) and server selection list. */
    int r = SSL_CTX_set_alpn_protos(ctx->ssl_ctx, buf, (unsigned int)total);
    if (r != 0) {
        free(buf);
        return CETCD_ERR_INTERNAL;
    }
    free(ctx->alpn);
    ctx->alpn = buf;
    ctx->alpn_len = (unsigned int)total;
    SSL_CTX_set_alpn_select_cb(ctx->ssl_ctx, alpn_select_, ctx);
    return CETCD_OK;
}

int cetcd_tls_alpn_selected(const cetcd_tls_conn *conn,
                            const uint8_t **proto, unsigned int *len) {
    if (conn == NULL || conn->ssl == NULL || proto == NULL || len == NULL)
        return CETCD_ERR_INVAL;
    const unsigned char *p = NULL;
    unsigned int n = 0;
    SSL_get0_alpn_selected(conn->ssl, &p, &n);
    *proto = p;
    *len = n;
    return CETCD_OK;
}

cetcd_tls_conn *cetcd_tls_accept(cetcd_tls_ctx *ctx, int fd) {
    if (ctx == NULL || ctx->ssl_ctx == NULL) return NULL;
    cetcd_tls_conn *cn = (cetcd_tls_conn *)calloc(1, sizeof(*cn));
    if (cn == NULL) return NULL;
    cn->fd = fd;
    cn->ssl = SSL_new(ctx->ssl_ctx);
    if (cn->ssl == NULL) {
        free(cn);
        return NULL;
    }
    if (SSL_set_fd(cn->ssl, fd) != 1) {
        SSL_free(cn->ssl);
        free(cn);
        return NULL;
    }
    if (SSL_accept(cn->ssl) != 1) {
        SSL_free(cn->ssl);
        free(cn);
        return NULL;
    }
    return cn;
}

cetcd_tls_conn *cetcd_tls_connect(cetcd_tls_ctx *ctx, int fd) {
    if (ctx == NULL || ctx->ssl_ctx == NULL) return NULL;
    cetcd_tls_conn *cn = (cetcd_tls_conn *)calloc(1, sizeof(*cn));
    if (cn == NULL) return NULL;
    cn->fd = fd;
    cn->ssl = SSL_new(ctx->ssl_ctx);
    if (cn->ssl == NULL) {
        free(cn);
        return NULL;
    }
    if (SSL_set_fd(cn->ssl, fd) != 1) {
        SSL_free(cn->ssl);
        free(cn);
        return NULL;
    }
    if (SSL_connect(cn->ssl) != 1) {
        SSL_free(cn->ssl);
        free(cn);
        return NULL;
    }
    return cn;
}

static cetcd_tls_conn *conn_mem_(cetcd_tls_ctx *ctx, int server) {
    if (ctx == NULL || ctx->ssl_ctx == NULL) return NULL;
    cetcd_tls_conn *cn = (cetcd_tls_conn *)calloc(1, sizeof(*cn));
    if (cn == NULL) return NULL;
    cn->fd = -1;
    cn->ssl = SSL_new(ctx->ssl_ctx);
    if (cn->ssl == NULL) {
        free(cn);
        return NULL;
    }
    BIO *rbio = BIO_new(BIO_s_mem());
    BIO *wbio = BIO_new(BIO_s_mem());
    if (rbio == NULL || wbio == NULL) {
        BIO_free(rbio);
        BIO_free(wbio);
        SSL_free(cn->ssl);
        free(cn);
        return NULL;
    }
    BIO_set_mem_eof_return(rbio, -1);
    BIO_set_mem_eof_return(wbio, -1);
    SSL_set_bio(cn->ssl, rbio, wbio);
    if (server)
        SSL_set_accept_state(cn->ssl);
    else
        SSL_set_connect_state(cn->ssl);
    return cn;
}

cetcd_tls_conn *cetcd_tls_conn_accept(cetcd_tls_ctx *ctx) {
    return conn_mem_(ctx, 1);
}

cetcd_tls_conn *cetcd_tls_conn_connect(cetcd_tls_ctx *ctx) {
    return conn_mem_(ctx, 0);
}

void cetcd_tls_conn_free(cetcd_tls_conn *conn) {
    if (conn == NULL) return;
    if (conn->ssl) SSL_free(conn->ssl);
    free(conn);
}

int cetcd_tls_feed(cetcd_tls_conn *conn, const void *data, size_t len) {
    if (conn == NULL || conn->ssl == NULL) return CETCD_ERR_INVAL;
    if (len == 0) return CETCD_OK;
    if (data == NULL) return CETCD_ERR_INVAL;
    BIO *rbio = SSL_get_rbio(conn->ssl);
    if (rbio == NULL) return CETCD_ERR_INTERNAL;
    const uint8_t *p = (const uint8_t *)data;
    size_t off = 0;
    while (off < len) {
        size_t left = len - off;
        int chunk = left > (size_t)INT_MAX ? INT_MAX : (int)left;
        int n = BIO_write(rbio, p + off, chunk);
        if (n <= 0) return CETCD_ERR_IO;
        off += (size_t)n;
    }
    return CETCD_OK;
}

int cetcd_tls_handshake(cetcd_tls_conn *conn) {
    if (conn == NULL || conn->ssl == NULL) return -1;
    if (SSL_is_init_finished(conn->ssl)) return 1;
    int r = SSL_do_handshake(conn->ssl);
    if (r == 1) return 1;
    int err = SSL_get_error(conn->ssl, r);
    if (err == SSL_ERROR_WANT_READ || err == SSL_ERROR_WANT_WRITE) return 0;
    return -1;
}

int cetcd_tls_pending_out(cetcd_tls_conn *conn, uint8_t *buf, size_t cap) {
    if (conn == NULL || conn->ssl == NULL || buf == NULL) return CETCD_ERR_INVAL;
    if (cap == 0) return 0;
    BIO *wbio = SSL_get_wbio(conn->ssl);
    if (wbio == NULL) return CETCD_ERR_INTERNAL;
    int chunk = cap > (size_t)INT_MAX ? INT_MAX : (int)cap;
    int n = BIO_read(wbio, buf, chunk);
    if (n > 0) return n;
    if (n == 0 || BIO_should_retry(wbio)) return 0;
    return CETCD_ERR_IO;
}

int cetcd_tls_read(cetcd_tls_conn *conn, void *buf, size_t len) {
    if (conn == NULL || conn->ssl == NULL) return CETCD_ERR_INVAL;
    if (buf == NULL || len == 0) return CETCD_ERR_INVAL;
    int chunk = len > (size_t)INT_MAX ? INT_MAX : (int)len;
    int r = SSL_read(conn->ssl, buf, chunk);
    if (r > 0) return r;
    int err = SSL_get_error(conn->ssl, r);
    if (err == SSL_ERROR_WANT_READ || err == SSL_ERROR_WANT_WRITE) return 0;
    return CETCD_ERR_IO;
}

int cetcd_tls_write(cetcd_tls_conn *conn, const void *buf, size_t len) {
    if (conn == NULL || conn->ssl == NULL) return CETCD_ERR_INVAL;
    if (buf == NULL && len > 0) return CETCD_ERR_INVAL;
    if (len == 0) return 0;
    int chunk = len > (size_t)INT_MAX ? INT_MAX : (int)len;
    int r = SSL_write(conn->ssl, buf, chunk);
    if (r > 0) return r;
    int err = SSL_get_error(conn->ssl, r);
    if (err == SSL_ERROR_WANT_READ || err == SSL_ERROR_WANT_WRITE) return 0;
    return CETCD_ERR_IO;
}

void cetcd_tls_shutdown(cetcd_tls_conn *conn) {
    if (conn == NULL || conn->ssl == NULL) return;
    (void)SSL_shutdown(conn->ssl);
}

#else /* CETCD_HAS_OPENSSL */
typedef struct cetcd_tls_ctx cetcd_tls_ctx;
typedef struct cetcd_tls_conn cetcd_tls_conn;

cetcd_tls_ctx *cetcd_tls_ctx_new(void) {
    return NULL;
}
cetcd_tls_ctx *cetcd_tls_ctx_new_client(void) {
    return NULL;
}
void cetcd_tls_ctx_free(cetcd_tls_ctx *ctx) {
    (void)ctx;
}
int cetcd_tls_set_cert(cetcd_tls_ctx *ctx, const char *cert_path, const char *key_path) {
    (void)ctx; (void)cert_path; (void)key_path;
    return CETCD_ERR_UNSUPPORT;
}
int cetcd_tls_set_ca(cetcd_tls_ctx *ctx, const char *ca_path) {
    (void)ctx; (void)ca_path; return CETCD_ERR_UNSUPPORT;
}
int cetcd_tls_set_alpn(cetcd_tls_ctx *ctx, const char **protocols, size_t count) {
    (void)ctx; (void)protocols; (void)count; return CETCD_ERR_UNSUPPORT;
}
int cetcd_tls_alpn_selected(const cetcd_tls_conn *conn,
                            const uint8_t **proto, unsigned int *len) {
    (void)conn; (void)proto; (void)len; return CETCD_ERR_UNSUPPORT;
}
int cetcd_tls_set_verify_peer(cetcd_tls_ctx *ctx, int require_cert) {
    (void)ctx; (void)require_cert; return CETCD_ERR_UNSUPPORT;
}
int cetcd_tls_set_ciphers(cetcd_tls_ctx *ctx, const char *list) {
    (void)ctx; (void)list; return CETCD_ERR_UNSUPPORT;
}
cetcd_tls_conn *cetcd_tls_accept(cetcd_tls_ctx *ctx, int fd) {
    (void)ctx; (void)fd; return NULL;
}
cetcd_tls_conn *cetcd_tls_connect(cetcd_tls_ctx *ctx, int fd) {
    (void)ctx; (void)fd; return NULL;
}
cetcd_tls_conn *cetcd_tls_conn_accept(cetcd_tls_ctx *ctx) {
    (void)ctx; return NULL;
}
cetcd_tls_conn *cetcd_tls_conn_connect(cetcd_tls_ctx *ctx) {
    (void)ctx; return NULL;
}
void cetcd_tls_conn_free(cetcd_tls_conn *conn) { (void)conn; }
int cetcd_tls_feed(cetcd_tls_conn *conn, const void *data, size_t len) {
    (void)conn; (void)data; (void)len; return CETCD_ERR_UNSUPPORT;
}
int cetcd_tls_handshake(cetcd_tls_conn *conn) {
    (void)conn; return -1;
}
int cetcd_tls_pending_out(cetcd_tls_conn *conn, uint8_t *buf, size_t cap) {
    (void)conn; (void)buf; (void)cap; return CETCD_ERR_UNSUPPORT;
}
int cetcd_tls_read(cetcd_tls_conn *conn, void *buf, size_t len) {
    (void)conn; (void)buf; (void)len; return CETCD_ERR_UNSUPPORT;
}
int cetcd_tls_write(cetcd_tls_conn *conn, const void *buf, size_t len) {
    (void)conn; (void)buf; (void)len; return CETCD_ERR_UNSUPPORT;
}
void cetcd_tls_shutdown(cetcd_tls_conn *conn) { (void)conn; }
#endif
