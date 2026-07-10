#ifndef CETCD_BACKEND_H
#define CETCD_BACKEND_H

#include "cetcd/base.h"
#include "cetcd/raft.h"
#include <stdbool.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ── Backend (LMDB wrapper) ─────────────────────────────────────── */

typedef struct cetcd_backend cetcd_backend;
typedef struct cetcd_txn     cetcd_txn;

typedef struct cetcd_backend_config {
    const char *path;
    size_t      map_size;
    uint32_t    max_dbs;
} cetcd_backend_config;

cetcd_backend *cetcd_backend_open(const cetcd_backend_config *cfg);
void           cetcd_backend_close(cetcd_backend *be);

/* ── Transactions ────────────────────────────────────────────────── */

cetcd_txn *cetcd_txn_begin(cetcd_backend *be, bool read_only);
int        cetcd_txn_commit(cetcd_txn *txn);
void       cetcd_txn_abort(cetcd_txn *txn);

/* ── Key-Value ops (auto-commit) ─────────────────────────────────── */

int  cetcd_backend_put(cetcd_backend *be, const char *bucket,
                       const uint8_t *key, size_t key_len,
                       const uint8_t *val, size_t val_len);
int  cetcd_backend_get(cetcd_backend *be, const char *bucket,
                       const uint8_t *key, size_t key_len,
                       uint8_t **val, size_t *val_len);
int  cetcd_backend_del(cetcd_backend *be, const char *bucket,
                       const uint8_t *key, size_t key_len);

/* ── Transactional KV ops ────────────────────────────────────────── */

int  cetcd_txn_put(cetcd_txn *txn, const char *bucket,
                   const uint8_t *key, size_t key_len,
                   const uint8_t *val, size_t val_len);
int  cetcd_txn_get(cetcd_txn *txn, const char *bucket,
                   const uint8_t *key, size_t key_len,
                   uint8_t **val, size_t *val_len);
int  cetcd_txn_del(cetcd_txn *txn, const char *bucket,
                   const uint8_t *key, size_t key_len);

/* ── Queries ─────────────────────────────────────────────────────── */

uint64_t cetcd_backend_size(cetcd_backend *be);

/* Iterate all keys in a bucket. Callback returns false to stop early. */
typedef bool (*cetcd_backend_iter_fn)(const uint8_t *key, size_t key_len,
                                       const uint8_t *val, size_t val_len,
                                       void *udata);
int cetcd_backend_foreach(cetcd_backend *be, const char *bucket,
                           cetcd_backend_iter_fn fn, void *udata);

#ifdef __cplusplus
}
#endif

#endif /* CETCD_BACKEND_H */
