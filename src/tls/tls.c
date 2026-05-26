#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "cetcd/tls.h"
#include "cetcd/base.h"

#if CETCD_HAS_OPENSSL
#include <openssl/ssl.h>
#include <openssl/err.h>
#include <openssl/x509.h>

/* Server context */
struct cetcd_tls_ctx {
    SSL_CTX *ssl_ctx;
};

/* Per-connection */
struct cetcd_tls_conn {
    SSL *ssl;
    int  fd;
};

/* Helper: map OpenSSL errors to CETCD status */
static int ssl_err_to_cetcd_(void) {
    (void)ERR_get_error();
    return CETCD_ERR_INTERNAL;
}

/* Context lifecycle */
cetcd_tls_ctx *cetcd_tls_ctx_new(void) {
    cetcd_tls_ctx *ctx = (cetcd_tls_ctx *)calloc(1, sizeof(*ctx));
    if (ctx == NULL) return NULL;

    /* Create server method context explicitly */
    ctx->ssl_ctx = SSL_CTX_new(TLS_server_method());
    if (ctx->ssl_ctx == NULL) {
        free(ctx);
        return NULL;
    }

    /* Set minimum protocol to TLS 1.2 and disable older/less secure protocols */
#if defined(SSL_CTX_set_min_proto_version)
    SSL_CTX_set_min_proto_version(ctx->ssl_ctx, TLS1_2_VERSION);
#endif
    /* Disable legacy protocols for safety */
    SSL_CTX_set_options(ctx->ssl_ctx,
                        SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3 | SSL_OP_NO_TLSv1);

    return ctx;
}

void cetcd_tls_ctx_free(cetcd_tls_ctx *ctx) {
    if (ctx == NULL) return;
    if (ctx->ssl_ctx) SSL_CTX_free(ctx->ssl_ctx);
    free(ctx);
}

/* Certificate handling */
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

int cetcd_tls_set_alpn(cetcd_tls_ctx *ctx, const char **protocols, size_t count) {
    if (ctx == NULL || ctx->ssl_ctx == NULL) return CETCD_ERR_INVAL;
    if (protocols == NULL || count == 0) {
        /* No ALPNs configured is acceptable; treat as success */
        return CETCD_OK;
    }

    /* Compute total length: 1-byte length per protocol + protocol bytes */
    size_t total = 0;
    for (size_t i = 0; i < count; ++i) {
        const char *p = protocols[i];
        if (p == NULL) return CETCD_ERR_INVAL;
        total += 1 + strlen(p);
    }
    unsigned char *buf = (unsigned char *)malloc(total);
    if (buf == NULL) return CETCD_ERR_NOMEM;
    unsigned char *p = buf;
    for (size_t i = 0; i < count; ++i) {
        size_t len = strlen(protocols[i]);
        if (len > 255) len = 255; /* ALPN protocol length is 255 max per spec */
        *p++ = (unsigned char)len;
        memcpy(p, protocols[i], len);
        p += len;
    }

    int r = SSL_CTX_set_alpn_protos(ctx->ssl_ctx, buf, (unsigned int)total);
    free(buf);
    /* OpenSSL returns 0 on success for ALPN setting */
    if (r == 0) {
        return CETCD_OK;
    }
    return CETCD_ERR_INTERNAL;
}

/* Per-connection */
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

void cetcd_tls_conn_free(cetcd_tls_conn *conn) {
    if (conn == NULL) return;
    if (conn->ssl) SSL_free(conn->ssl);
    free(conn);
}

int cetcd_tls_read(cetcd_tls_conn *conn, void *buf, size_t len) {
    if (conn == NULL || conn->ssl == NULL) return CETCD_ERR_INVAL;
    int r = SSL_read(conn->ssl, buf, (int)len);
    if (r <= 0) {
        int err = SSL_get_error(conn->ssl, r);
        (void)err;
        return CETCD_ERR_IO;
    }
    return r;
}

int cetcd_tls_write(cetcd_tls_conn *conn, const void *buf, size_t len) {
    if (conn == NULL || conn->ssl == NULL) return CETCD_ERR_INVAL;
    int r = SSL_write(conn->ssl, buf, (int)len);
    if (r <= 0) {
        return CETCD_ERR_IO;
    }
    return r;
}

void cetcd_tls_shutdown(cetcd_tls_conn *conn) {
    if (conn == NULL || conn->ssl == NULL) return;
    (void)SSL_shutdown(conn->ssl);
}

#else /* CETCD_HAS_OPENSSL */
/* OpenSSL not available: provide minimal stubs to keep ABI stable. */
typedef struct cetcd_tls_ctx cetcd_tls_ctx;
typedef struct cetcd_tls_conn cetcd_tls_conn;

cetcd_tls_ctx *cetcd_tls_ctx_new(void) {
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
cetcd_tls_conn *cetcd_tls_accept(cetcd_tls_ctx *ctx, int fd) {
    (void)ctx; (void)fd; return NULL;
}
void cetcd_tls_conn_free(cetcd_tls_conn *conn) { (void)conn; }
int cetcd_tls_read(cetcd_tls_conn *conn, void *buf, size_t len) {
    (void)conn; (void)buf; (void)len; return CETCD_ERR_UNSUPPORT;
}
int cetcd_tls_write(cetcd_tls_conn *conn, const void *buf, size_t len) {
    (void)conn; (void)buf; (void)len; return CETCD_ERR_UNSUPPORT;
}
void cetcd_tls_shutdown(cetcd_tls_conn *conn) { (void)conn; }
#endif
