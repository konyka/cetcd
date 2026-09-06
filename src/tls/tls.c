#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>

#include "cetcd/tls.h"
#include "cetcd/base.h"

int cetcd_parse_tls_version(const char *s, int *out) {
    if (!s || !s[0] || !out) return CETCD_ERR_INVAL;
    if (strcmp(s, "TLS1.2") == 0) {
        *out = CETCD_TLS_VER_1_2;
        return CETCD_OK;
    }
    if (strcmp(s, "TLS1.3") == 0) {
        *out = CETCD_TLS_VER_1_3;
        return CETCD_OK;
    }
    return CETCD_ERR_INVAL;
}

int cetcd_tls_version_range_ok(int min_ver, int max_ver) {
    if (min_ver == CETCD_TLS_VER_UNSPEC) min_ver = CETCD_TLS_VER_1_2;
    if (min_ver != CETCD_TLS_VER_1_2 && min_ver != CETCD_TLS_VER_1_3)
        return CETCD_ERR_INVAL;
    if (max_ver == CETCD_TLS_VER_UNSPEC) return CETCD_OK;
    if (max_ver != CETCD_TLS_VER_1_2 && max_ver != CETCD_TLS_VER_1_3)
        return CETCD_ERR_INVAL;
    if (min_ver > max_ver) return CETCD_ERR_INVAL;
    return CETCD_OK;
}

int cetcd_tls_name_list_open(const char *list) {
    return !list || !list[0];
}

int cetcd_tls_name_list_has(const char *list, const char *name) {
    if (cetcd_tls_name_list_open(list)) return 1;
    if (!name || !name[0]) return 0;
    const char *p = list;
    while (*p) {
        while (*p == ' ' || *p == '\t' || *p == ',') p++;
        if (!*p) break;
        const char *s = p;
        while (*p && *p != ',') p++;
        const char *e = p;
        while (e > s && (e[-1] == ' ' || e[-1] == '\t')) e--;
        size_t n = (size_t)(e - s);
        if (n && strlen(name) == n && memcmp(s, name, n) == 0) return 1;
        if (*p == ',') p++;
    }
    return 0;
}

static int ci_eq_(const char *a, const char *b) {
    if (!a || !b) return 0;
    while (*a && *b) {
        char ca = *a, cb = *b;
        if (ca >= 'A' && ca <= 'Z') ca = (char)(ca + 32);
        if (cb >= 'A' && cb <= 'Z') cb = (char)(cb + 32);
        if (ca != cb) return 0;
        a++;
        b++;
    }
    return *a == 0 && *b == 0;
}

int cetcd_tls_hostname_matches(const char *pattern, const char *name) {
    if (!pattern || !pattern[0] || !name || !name[0]) return 0;
    if (pattern[0] == '*' && pattern[1] == '.') {
        const char *dot = strchr(name, '.');
        if (!dot || dot == name) return 0;
        return ci_eq_(dot, pattern + 1);
    }
    return ci_eq_(pattern, name);
}

static int host_list_matches_(const char *list, const char *cn,
                              const char *const *sans, size_t n_sans) {
    if (!list) return 0;
    const char *p = list;
    while (*p) {
        while (*p == ' ' || *p == '\t' || *p == ',') p++;
        if (!*p) break;
        const char *s = p;
        while (*p && *p != ',') p++;
        const char *e = p;
        while (e > s && (e[-1] == ' ' || e[-1] == '\t')) e--;
        size_t n = (size_t)(e - s);
        if (n && n < 256) {
            char tok[256];
            memcpy(tok, s, n);
            tok[n] = '\0';
            if (n_sans == 0) {
                if (cetcd_tls_hostname_matches(tok, cn)) return 1;
            } else {
                for (size_t i = 0; i < n_sans; i++) {
                    if (cetcd_tls_hostname_matches(tok, sans[i])) return 1;
                }
            }
        }
        if (*p == ',') p++;
    }
    return 0;
}

int cetcd_tls_peer_identity_ok(const char *cn_list, const char *host_list,
                               const char *cn, const char *const *sans,
                               size_t n_sans) {
    int cn_open = cetcd_tls_name_list_open(cn_list);
    int host_open = cetcd_tls_name_list_open(host_list);
    if (cn_open && host_open) return 1;
    if (!cn_open && cetcd_tls_name_list_has(cn_list, cn)) return 1;
    if (!host_open && host_list_matches_(host_list, cn, sans, n_sans)) return 1;
    return 0;
}

#if CETCD_HAS_OPENSSL
#include <openssl/ssl.h>
#include <openssl/err.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>
#include <openssl/pem.h>
#include <openssl/evp.h>
#include <openssl/bio.h>
#if OPENSSL_VERSION_NUMBER < 0x30000000L
#include <openssl/ec.h>
#endif

#if defined(_WIN32)
#  include <io.h>
#  include <ws2tcpip.h>
#  define cetcd_access _access
#  define CETCD_R_OK 4
#else
#  include <unistd.h>
#  include <sys/stat.h>
#  include <arpa/inet.h>
#  define cetcd_access access
#  define CETCD_R_OK R_OK
#endif

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

static int suite_eq_(const char *in, size_t n, const char *want) {
    size_t w = strlen(want);
    return n == w && memcmp(in, want, n) == 0;
}

static int is_tls13_suite_(const char *in, size_t n) {
    while (n > 0 && (*in == ' ' || *in == '\t')) { in++; n--; }
    while (n > 0 && (in[n - 1] == ' ' || in[n - 1] == '\t')) n--;
    return suite_eq_(in, n, "TLS_AES_128_GCM_SHA256")
        || suite_eq_(in, n, "TLS_AES_256_GCM_SHA384")
        || suite_eq_(in, n, "TLS_CHACHA20_POLY1305_SHA256")
        || suite_eq_(in, n, "TLS_AES_128_CCM_SHA256")
        || suite_eq_(in, n, "TLS_AES_128_CCM_8_SHA256");
}

static int append_colon_(char *dst, size_t cap, size_t *pos, const char *one) {
    size_t olen = strlen(one);
    if (*pos > 0) {
        if (*pos + 1 >= cap) return -1;
        dst[(*pos)++] = ':';
    }
    if (*pos + olen >= cap) return -1;
    memcpy(dst + *pos, one, olen);
    *pos += olen;
    dst[*pos] = '\0';
    return 0;
}

int cetcd_tls_set_ciphers(cetcd_tls_ctx *ctx, const char *list) {
    if (ctx == NULL || ctx->ssl_ctx == NULL || list == NULL) return CETCD_ERR_INVAL;
    char tls12[1024];
    char tls13[1024];
    size_t p12 = 0, p13 = 0;
    tls12[0] = tls13[0] = '\0';
    const char *p = list;
    while (*p) {
        while (*p == ' ' || *p == '\t' || *p == ',') p++;
        if (!*p) break;
        const char *start = p;
        while (*p && *p != ',') p++;
        size_t n = (size_t)(p - start);
        const char *s = start;
        size_t sn = n;
        while (sn > 0 && (*s == ' ' || *s == '\t')) { s++; sn--; }
        while (sn > 0 && (s[sn - 1] == ' ' || s[sn - 1] == '\t')) sn--;
        if (sn == 0) return CETCD_ERR_INVAL;
        if (is_tls13_suite_(s, sn)) {
            char one[256];
            if (sn >= sizeof(one)) return CETCD_ERR_OVERFLOW;
            memcpy(one, s, sn);
            one[sn] = '\0';
            if (append_colon_(tls13, sizeof(tls13), &p13, one) != 0)
                return CETCD_ERR_OVERFLOW;
        } else {
            char one[256];
            if (suite_to_openssl_(start, n, one, sizeof(one)) != 0)
                return CETCD_ERR_INVAL;
            if (append_colon_(tls12, sizeof(tls12), &p12, one) != 0)
                return CETCD_ERR_OVERFLOW;
        }
    }
    if (p12 == 0 && p13 == 0) return CETCD_ERR_INVAL;
    if (p13 > 0 && SSL_CTX_set_ciphersuites(ctx->ssl_ctx, tls13) != 1)
        return CETCD_ERR_INVAL;
    if (p12 > 0 && SSL_CTX_set_cipher_list(ctx->ssl_ctx, tls12) != 1)
        return CETCD_ERR_INVAL;
#if defined(SSL_CTX_set_min_proto_version)
    if (p13 > 0 && p12 == 0) {
        if (SSL_CTX_set_min_proto_version(ctx->ssl_ctx, TLS1_3_VERSION) != 1)
            return CETCD_ERR_INVAL;
    } else if (p12 > 0 && p13 == 0) {
        if (SSL_CTX_set_max_proto_version(ctx->ssl_ctx, TLS1_2_VERSION) != 1)
            return CETCD_ERR_INVAL;
    }
#endif
    return CETCD_OK;
}

int cetcd_tls_set_proto_versions(cetcd_tls_ctx *ctx, int min_ver, int max_ver) {
    if (!ctx || !ctx->ssl_ctx) return CETCD_ERR_INVAL;
    if (cetcd_tls_version_range_ok(min_ver, max_ver) != CETCD_OK)
        return CETCD_ERR_INVAL;
    if (min_ver == CETCD_TLS_VER_UNSPEC) min_ver = CETCD_TLS_VER_1_2;
#if defined(SSL_CTX_set_min_proto_version)
    int minv = (min_ver == CETCD_TLS_VER_1_3) ? TLS1_3_VERSION : TLS1_2_VERSION;
    if (SSL_CTX_set_min_proto_version(ctx->ssl_ctx, minv) != 1)
        return CETCD_ERR_INVAL;
    if (max_ver == CETCD_TLS_VER_1_2) {
        if (SSL_CTX_set_max_proto_version(ctx->ssl_ctx, TLS1_2_VERSION) != 1)
            return CETCD_ERR_INVAL;
    } else if (max_ver == CETCD_TLS_VER_1_3) {
        if (SSL_CTX_set_max_proto_version(ctx->ssl_ctx, TLS1_3_VERSION) != 1)
            return CETCD_ERR_INVAL;
    }
#else
    (void)max_ver;
#endif
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

int cetcd_tls_check_peer_identity(const cetcd_tls_conn *conn,
                                  const char *cn_list, const char *host_list) {
    if (cetcd_tls_name_list_open(cn_list) && cetcd_tls_name_list_open(host_list))
        return CETCD_OK;
    if (!conn || !conn->ssl) return CETCD_ERR_INVAL;
#if OPENSSL_VERSION_NUMBER >= 0x30000000L
    X509 *cert = SSL_get1_peer_certificate(conn->ssl);
#else
    X509 *cert = SSL_get_peer_certificate(conn->ssl);
#endif
    if (!cert) return CETCD_ERR_INVAL;
    char cn[256];
    cn[0] = '\0';
    X509_NAME *nm = X509_get_subject_name(cert);
    if (nm)
        X509_NAME_get_text_by_NID(nm, NID_commonName, cn, (int)sizeof(cn));
    const char *sans[32];
    char san_buf[32][256];
    size_t n_sans = 0;
    GENERAL_NAMES *gns = X509_get_ext_d2i(cert, NID_subject_alt_name, NULL, NULL);
    if (gns) {
        int n = sk_GENERAL_NAME_num(gns);
        for (int i = 0; i < n && n_sans < 32; i++) {
            GENERAL_NAME *gn = sk_GENERAL_NAME_value(gns, i);
            if (!gn) continue;
            if (gn->type == GEN_DNS && gn->d.dNSName && gn->d.dNSName->data &&
                gn->d.dNSName->length > 0 &&
                (size_t)gn->d.dNSName->length < sizeof(san_buf[0])) {
                memcpy(san_buf[n_sans], gn->d.dNSName->data,
                       (size_t)gn->d.dNSName->length);
                san_buf[n_sans][gn->d.dNSName->length] = '\0';
                sans[n_sans] = san_buf[n_sans];
                n_sans++;
            } else if (gn->type == GEN_IPADD && gn->d.iPAddress &&
                       gn->d.iPAddress->data) {
                san_buf[n_sans][0] = '\0';
                if (gn->d.iPAddress->length == 4)
                    inet_ntop(AF_INET, gn->d.iPAddress->data, san_buf[n_sans],
                              sizeof(san_buf[0]));
                else if (gn->d.iPAddress->length == 16)
                    inet_ntop(AF_INET6, gn->d.iPAddress->data, san_buf[n_sans],
                              sizeof(san_buf[0]));
                if (san_buf[n_sans][0]) {
                    sans[n_sans] = san_buf[n_sans];
                    n_sans++;
                }
            }
        }
        GENERAL_NAMES_free(gns);
    }
    X509_free(cert);
    return cetcd_tls_peer_identity_ok(cn_list, host_list,
                                      cn[0] ? cn : NULL, sans, n_sans)
               ? CETCD_OK : CETCD_ERR_INVAL;
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

static int file_readable_(const char *path) {
    return path && path[0] && cetcd_access(path, CETCD_R_OK) == 0;
}

static int looks_like_ip_(const char *s) {
    int digits = 0, seps = 0;
    for (const char *p = s; *p; p++) {
        if (*p >= '0' && *p <= '9') { digits++; continue; }
        if ((*p >= 'a' && *p <= 'f') || (*p >= 'A' && *p <= 'F')) continue;
        if (*p == '.' || *p == ':') { seps++; continue; }
        return 0;
    }
    return digits > 0 && seps > 0;
}

static EVP_PKEY *gen_p256_(void) {
#if OPENSSL_VERSION_NUMBER >= 0x30000000L
    return EVP_EC_gen("P-256");
#else
    EC_KEY *ec = EC_KEY_new_by_curve_name(NID_X9_62_prime256v1);
    if (!ec) return NULL;
    if (EC_KEY_generate_key(ec) != 1) {
        EC_KEY_free(ec);
        return NULL;
    }
    EVP_PKEY *pkey = EVP_PKEY_new();
    if (!pkey || EVP_PKEY_assign_EC_KEY(pkey, ec) != 1) {
        EVP_PKEY_free(pkey);
        EC_KEY_free(ec);
        return NULL;
    }
    return pkey;
#endif
}

static int write_bio_file_(const char *path, BIO *bio) {
    char *data = NULL;
    long len = BIO_get_mem_data(bio, &data);
    if (len <= 0 || !data) return -1;
    FILE *fp = fopen(path, "wb");
    if (!fp) return -1;
    size_t wr = fwrite(data, 1, (size_t)len, fp);
    int fc = fclose(fp);
    if (wr != (size_t)len || fc != 0) {
        remove(path);
        return -1;
    }
    return 0;
}

static int write_pem_pair_(const char *cert_path, const char *key_path,
                           X509 *cert, EVP_PKEY *pkey) {
    char ktmp[768], ctmp[768];
    int n = snprintf(ktmp, sizeof(ktmp), "%s.tmp", key_path);
    if (n < 0 || (size_t)n >= sizeof(ktmp)) return -1;
    n = snprintf(ctmp, sizeof(ctmp), "%s.tmp", cert_path);
    if (n < 0 || (size_t)n >= sizeof(ctmp)) return -1;

    BIO *kb = BIO_new(BIO_s_mem());
    BIO *cb = BIO_new(BIO_s_mem());
    if (!kb || !cb ||
        PEM_write_bio_PrivateKey(kb, pkey, NULL, NULL, 0, NULL, NULL) != 1 ||
        PEM_write_bio_X509(cb, cert) != 1 ||
        write_bio_file_(ktmp, kb) != 0) {
        BIO_free(kb);
        BIO_free(cb);
        remove(ktmp);
        return -1;
    }
    BIO_free(kb);
#if !defined(_WIN32)
    (void)chmod(ktmp, 0600);
#endif
    if (write_bio_file_(ctmp, cb) != 0) {
        BIO_free(cb);
        remove(ktmp);
        return -1;
    }
    BIO_free(cb);
    if (rename(ktmp, key_path) != 0) {
        remove(ktmp);
        remove(ctmp);
        return -1;
    }
    if (rename(ctmp, cert_path) != 0) {
        remove(key_path);
        remove(ctmp);
        return -1;
    }
#if !defined(_WIN32)
    (void)chmod(key_path, 0600);
#endif
    return 0;
}

int cetcd_tls_self_signed_days(uint32_t years, int *out_days) {
    if (!out_days) return CETCD_ERR_INVAL;
    if (years == 0) years = 1;
    if (years > (uint32_t)(INT_MAX / 365)) return CETCD_ERR_INVAL;
    *out_days = (int)years * 365;
    return CETCD_OK;
}

int cetcd_tls_auto_cert(const char *cert_path, const char *key_path,
                        const char *cn, const char *extra_ip) {
    return cetcd_tls_auto_cert_years(cert_path, key_path, cn, extra_ip, 0);
}

int cetcd_tls_auto_cert_years(const char *cert_path, const char *key_path,
                              const char *cn, const char *extra_ip,
                              uint32_t years) {
    if (!cert_path || !cert_path[0] || !key_path || !key_path[0])
        return CETCD_ERR_INVAL;
    int days = 0;
    if (cetcd_tls_self_signed_days(years, &days) != CETCD_OK)
        return CETCD_ERR_INVAL;
    int have_c = file_readable_(cert_path);
    int have_k = file_readable_(key_path);
    if (have_c && have_k) return CETCD_OK;
    if (have_c || have_k) return CETCD_ERR_INVAL;

    const char *name = (cn && cn[0]) ? cn : "localhost";
    EVP_PKEY *pkey = gen_p256_();
    if (!pkey) return CETCD_ERR_INTERNAL;

    X509 *cert = X509_new();
    if (!cert) {
        EVP_PKEY_free(pkey);
        return CETCD_ERR_NOMEM;
    }
    if (X509_set_version(cert, 2) != 1 ||
        ASN1_INTEGER_set(X509_get_serialNumber(cert), 1) != 1 ||
        !X509_gmtime_adj(X509_get_notBefore(cert), 0) ||
        !X509_time_adj_ex(X509_get_notAfter(cert), days, 0, NULL) ||
        X509_set_pubkey(cert, pkey) != 1) {
        X509_free(cert);
        EVP_PKEY_free(pkey);
        return CETCD_ERR_INTERNAL;
    }
    X509_NAME *nm = X509_NAME_new();
    if (!nm ||
        X509_NAME_add_entry_by_txt(nm, "CN", MBSTRING_ASC,
                                   (const unsigned char *)name, -1, -1, 0) != 1 ||
        X509_set_subject_name(cert, nm) != 1 ||
        X509_set_issuer_name(cert, nm) != 1) {
        X509_NAME_free(nm);
        X509_free(cert);
        EVP_PKEY_free(pkey);
        return CETCD_ERR_INTERNAL;
    }
    X509_NAME_free(nm);

    char san[768];
    int used = snprintf(san, sizeof(san), "DNS:localhost,DNS:%s,IP:127.0.0.1", name);
    if (used < 0 || (size_t)used >= sizeof(san)) {
        X509_free(cert);
        EVP_PKEY_free(pkey);
        return CETCD_ERR_OVERFLOW;
    }
    if (extra_ip && extra_ip[0] && looks_like_ip_(extra_ip) &&
        strcmp(extra_ip, "127.0.0.1") != 0) {
        int add = snprintf(san + used, sizeof(san) - (size_t)used,
                           ",IP:%s", extra_ip);
        if (add < 0 || (size_t)add >= sizeof(san) - (size_t)used) {
            X509_free(cert);
            EVP_PKEY_free(pkey);
            return CETCD_ERR_OVERFLOW;
        }
    }
    X509V3_CTX v3;
    X509V3_set_ctx_nodb(&v3);
    X509V3_set_ctx(&v3, cert, cert, NULL, NULL, 0);
    X509_EXTENSION *ex = X509V3_EXT_conf_nid(NULL, &v3, NID_subject_alt_name, san);
    if (!ex || X509_add_ext(cert, ex, -1) != 1) {
        X509_EXTENSION_free(ex);
        X509_free(cert);
        EVP_PKEY_free(pkey);
        return CETCD_ERR_INTERNAL;
    }
    X509_EXTENSION_free(ex);
    if (X509_sign(cert, pkey, EVP_sha256()) <= 0) {
        X509_free(cert);
        EVP_PKEY_free(pkey);
        return CETCD_ERR_INTERNAL;
    }
    int rc = write_pem_pair_(cert_path, key_path, cert, pkey);
    X509_free(cert);
    EVP_PKEY_free(pkey);
    return rc == 0 ? CETCD_OK : CETCD_ERR_IO;
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
int cetcd_tls_set_proto_versions(cetcd_tls_ctx *ctx, int min_ver, int max_ver) {
    (void)ctx;
    if (cetcd_tls_version_range_ok(min_ver, max_ver) != CETCD_OK)
        return CETCD_ERR_INVAL;
    return CETCD_ERR_UNSUPPORT;
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
int cetcd_tls_check_peer_identity(const cetcd_tls_conn *conn,
                                  const char *cn_list, const char *host_list) {
    (void)conn;
    if (cetcd_tls_name_list_open(cn_list) && cetcd_tls_name_list_open(host_list))
        return CETCD_OK;
    return CETCD_ERR_UNSUPPORT;
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
int cetcd_tls_self_signed_days(uint32_t years, int *out_days) {
    if (!out_days) return CETCD_ERR_INVAL;
    if (years == 0) years = 1;
    if (years > (uint32_t)(INT_MAX / 365)) return CETCD_ERR_INVAL;
    *out_days = (int)years * 365;
    return CETCD_OK;
}

int cetcd_tls_auto_cert(const char *cert_path, const char *key_path,
                        const char *cn, const char *extra_ip) {
    return cetcd_tls_auto_cert_years(cert_path, key_path, cn, extra_ip, 0);
}

int cetcd_tls_auto_cert_years(const char *cert_path, const char *key_path,
                              const char *cn, const char *extra_ip,
                              uint32_t years) {
    (void)cert_path; (void)key_path; (void)cn; (void)extra_ip; (void)years;
    return CETCD_ERR_UNSUPPORT;
}
#endif
