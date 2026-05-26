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
void           cetcd_tls_ctx_free(cetcd_tls_ctx *ctx);

int cetcd_tls_set_cert(cetcd_tls_ctx *ctx, const char *cert_path, const char *key_path);
int cetcd_tls_set_ca(cetcd_tls_ctx *ctx, const char *ca_path);
int cetcd_tls_set_alpn(cetcd_tls_ctx *ctx, const char **protocols, size_t count);

cetcd_tls_conn *cetcd_tls_accept(cetcd_tls_ctx *ctx, int fd);
void            cetcd_tls_conn_free(cetcd_tls_conn *conn);

int  cetcd_tls_read(cetcd_tls_conn *conn, void *buf, size_t len);
int  cetcd_tls_write(cetcd_tls_conn *conn, const void *buf, size_t len);
void cetcd_tls_shutdown(cetcd_tls_conn *conn);

#ifdef __cplusplus
}
#endif
#endif
