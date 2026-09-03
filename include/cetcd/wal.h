#ifndef CETCD_WAL_H
#define CETCD_WAL_H

#include "cetcd/base.h"
#include "cetcd/raft.h"
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ── WAL record types (matches etcd walpb.Record) ────────────────── */

typedef enum cetcd_wal_rec_type {
    CETCD_WAL_METADATA = 1,
    CETCD_WAL_ENTRY    = 2,
    CETCD_WAL_STATE    = 3,
    CETCD_WAL_CRC      = 4,
    CETCD_WAL_SNAPSHOT = 5,
} cetcd_wal_rec_type;

/* ── WAL record ──────────────────────────────────────────────────── */

typedef struct cetcd_wal_record {
    cetcd_wal_rec_type  type;
    uint32_t            crc;
    uint8_t            *data;
    size_t              data_len;
    size_t              data_cap;
} cetcd_wal_record;

/* ── WAL encoder ─────────────────────────────────────────────────── */

typedef struct cetcd_wal_encoder cetcd_wal_encoder;

cetcd_wal_encoder *cetcd_wal_encoder_create(const char *path);
void               cetcd_wal_encoder_free(cetcd_wal_encoder *enc);

int cetcd_wal_encode(cetcd_wal_encoder *enc, cetcd_wal_record *rec);
int cetcd_wal_encode_metadata(cetcd_wal_encoder *enc, const uint8_t *data, size_t len);
int cetcd_wal_encode_entry(cetcd_wal_encoder *enc, const cetcd_entry *entry);
int cetcd_wal_encode_hard_state(cetcd_wal_encoder *enc, const cetcd_hard_state *hs);
int cetcd_wal_encoder_flush(cetcd_wal_encoder *enc);
/* fsync the WAL file so acknowledged records survive a crash. Returns 0 on
 * success. This is the durability barrier callers must cross before sending
 * Raft messages or applying entries that depend on the persisted records. */
int cetcd_wal_encoder_sync(cetcd_wal_encoder *enc);

/* If `path` is a directory, write `{path}/0000000000000000.wal` into `out`.
 * Otherwise copy `path`. Returns 0 on success. */
int cetcd_wal_resolve_path(const char *path, char *out, size_t cap);

/* Decode a WAL_ENTRY record payload into `out`. `out->data` is malloc'd. */
int cetcd_wal_decode_entry(const uint8_t *data, size_t len, cetcd_entry *out);
int cetcd_wal_decode_hard_state(const uint8_t *data, size_t len, cetcd_hard_state *out);

/* ── WAL decoder ─────────────────────────────────────────────────── */

typedef struct cetcd_wal_decoder cetcd_wal_decoder;

cetcd_wal_decoder *cetcd_wal_decoder_open(const char *path);
void               cetcd_wal_decoder_free(cetcd_wal_decoder *dec);

int cetcd_wal_decode(cetcd_wal_decoder *dec, cetcd_wal_record *rec);

/* ── WAL record helpers ──────────────────────────────────────────── */

void cetcd_wal_record_init(cetcd_wal_record *rec);
void cetcd_wal_record_free(cetcd_wal_record *rec);

#ifdef __cplusplus
}
#endif

#endif /* CETCD_WAL_H */
