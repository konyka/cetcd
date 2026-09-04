#ifndef CETCD_TLS_H_
#define CETCD_TLS_H_

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct cetcd_tls_ctx cetcd_tls_ctx;
typedef struct cetcd_tls_conn cetcd_tls_conn;

cetcd_tls_ctx *cetcd_tls_ctx_new(void);
cetcd_tls_ctx *cetcd_tls_ctx_new_client(void);
void           cetcd_tls_ctx_free(cetcd_tls_ctx *ctx);

int cetcd_tls_set_cert(cetcd_tls_ctx *ctx, const char *cert_path, const char *key_path);
int cetcd_tls_set_ca(cetcd_tls_ctx *ctx, const char *ca_path);
int cetcd_tls_set_alpn(cetcd_tls_ctx *ctx, const char **protocols, size_t count);
/* After handshake: negotiated ALPN id (not NUL-terminated). *len == 0 if none. */
int cetcd_tls_alpn_selected(const cetcd_tls_conn *conn,
                            const uint8_t **proto, unsigned int *len);
/* require_cert: SSL_VERIFY_PEER | FAIL_IF_NO_PEER_CERT. Needs a CA. */
int cetcd_tls_set_verify_peer(cetcd_tls_ctx *ctx, int require_cert);
int cetcd_tls_set_ciphers(cetcd_tls_ctx *ctx, const char *list);

/* Blocking handshake on an fd. The caller still owns the fd. */
cetcd_tls_conn *cetcd_tls_accept(cetcd_tls_ctx *ctx, int fd);
cetcd_tls_conn *cetcd_tls_connect(cetcd_tls_ctx *ctx, int fd);

/* Non-blocking memory-BIO connections for a libuv-owned fd. */
cetcd_tls_conn *cetcd_tls_conn_accept(cetcd_tls_ctx *ctx);
cetcd_tls_conn *cetcd_tls_conn_connect(cetcd_tls_ctx *ctx);

void cetcd_tls_conn_free(cetcd_tls_conn *conn);

/* Feed inbound ciphertext. Handshake: 1 done, 0 WANT_READ/WRITE, -1 fail. */
int cetcd_tls_feed(cetcd_tls_conn *conn, const void *data, size_t len);
int cetcd_tls_handshake(cetcd_tls_conn *conn);
int cetcd_tls_pending_out(cetcd_tls_conn *conn, uint8_t *buf, size_t cap);

int  cetcd_tls_read(cetcd_tls_conn *conn, void *buf, size_t len);
int  cetcd_tls_write(cetcd_tls_conn *conn, const void *buf, size_t len);
void cetcd_tls_shutdown(cetcd_tls_conn *conn);

#ifdef __cplusplus
}
#endif
#endif
