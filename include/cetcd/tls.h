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

#define CETCD_TLS_VER_UNSPEC 0
#define CETCD_TLS_VER_1_2    1
#define CETCD_TLS_VER_1_3    2
/* TLS1.2 or TLS1.3 only. Empty/unknown is INVAL. */
int cetcd_parse_tls_version(const char *s, int *out);
/* Unset min → TLS1.2. Unset max is open. min > max is INVAL. */
int cetcd_tls_version_range_ok(int min_ver, int max_ver);
/* Apply min/max after ciphers. Unset min is TLS1.2. */
int cetcd_tls_set_proto_versions(cetcd_tls_ctx *ctx, int min_ver, int max_ver);

/* Mint or reuse a self-signed ECDSA P-256 cert/key pair.
 * If both files exist they are reused. One-without-the-other fail-closes.
 * cn/extra_ip become SAN entries (localhost + 127.0.0.1 always added).
 * years 0 = etcd default 1. Overflow (years*365 days) is INVAL. */
int cetcd_tls_auto_cert(const char *cert_path, const char *key_path,
                        const char *cn, const char *extra_ip);
int cetcd_tls_auto_cert_years(const char *cert_path, const char *key_path,
                              const char *cn, const char *extra_ip,
                              uint32_t years);
/* 0 years → 365. Overflow is INVAL. */
int cetcd_tls_self_signed_days(uint32_t years, int *out_days);
/* Empty/NULL = no extra identity restriction. */
int cetcd_tls_name_list_open(const char *list);
/* etcd: ClientCertAuth or a non-empty TrustedCAFile requires a client cert. */
int cetcd_tls_want_client_auth(int auth_flag, const char *trusted_ca);
/* Exact CN match. Open list is 1. Missing name is 0 if restricted. */
int cetcd_tls_name_list_has(const char *list, const char *name);
/* Case-insensitive. `*.example.com` matches one label. Empty is 0. */
int cetcd_tls_hostname_matches(const char *pattern, const char *name);
/* Restricted CN or hostname must match (OR if both set). Open+open is 1. */
int cetcd_tls_peer_identity_ok(const char *cn_list, const char *host_list,
                               const char *cn, const char *const *sans,
                               size_t n_sans);
/* etcd: ClientCertFile if set, else CertFile. Extra pair must be both or neither. */
int cetcd_tls_outbound_paths(const char *listen_cert, const char *listen_key,
                             const char *client_cert, const char *client_key,
                             const char **out_cert, const char **out_key);
/* CRL path set without that side's cert is INVAL. Empty CRL is OK. */
int cetcd_tls_crl_requires_cert(const char *crl, const char *cert);
/* 1 if serial is in the revoked list. Missing serial is 0. */
int cetcd_tls_serial_revoked(const uint8_t *const *revoked, const size_t *lens,
                             size_t n, const uint8_t *serial, size_t slen);
/* Load a PEM/DER CRL into ctx. Empty path clears. Missing file is IO. */
int cetcd_tls_set_crl(cetcd_tls_ctx *ctx, const char *path);
/* After handshake. No CRL is OK. No peer cert is OK (etcd). Revoked is INVAL. */
int cetcd_tls_check_crl(const cetcd_tls_conn *conn, const cetcd_tls_ctx *ctx);
/* After handshake. Open lists are OK. No peer cert / mismatch is INVAL. */
int cetcd_tls_check_peer_identity(const cetcd_tls_conn *conn,
                                  const char *cn_list, const char *host_list);

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
