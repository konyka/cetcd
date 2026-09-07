#ifndef CETCD_SNAP_H_
#define CETCD_SNAP_H_

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct cetcd_snap cetcd_snap;

typedef struct {
    uint8_t *key;
    size_t   key_len;
    uint8_t *value;
    size_t   value_len;
    int64_t  mod_revision;
} cetcd_snap_entry;

cetcd_snap *cetcd_snap_new(void);
void        cetcd_snap_free(cetcd_snap *s);

int  cetcd_snap_add_entry(cetcd_snap *s,
                           const uint8_t *key, size_t key_len,
                           const uint8_t *value, size_t value_len,
                           int64_t mod_revision);

size_t              cetcd_snap_entry_count(const cetcd_snap *s);
cetcd_snap_entry   *cetcd_snap_get_entry(const cetcd_snap *s, size_t idx);

uint8_t *cetcd_snap_encode(const cetcd_snap *s, size_t *out_len);
cetcd_snap *cetcd_snap_decode(const uint8_t *data, size_t len);

/* Maintenance Snapshot / snapshot.kv: optional CTS1 (12B) or CTS2 (16B)
 * header, then repeated (varint key_len + key + varint val_len + val).
 * Truncated or leftover bytes fail-closed (NULL). Empty blob is valid. */
cetcd_snap *cetcd_snap_decode_kv(const uint8_t *data, size_t len);
/* Inverse of decode_kv (no header). Caller frees the buffer. */
uint8_t *cetcd_snap_encode_kv(const cetcd_snap *s, size_t *out_len);

typedef struct cetcd_snap_header {
    uint64_t revision;
    uint32_t hash;     /* CRC32C of kv blob when has_hash */
    size_t   kv_off;
    size_t   kv_len;
    int      has_hash; /* 1 = CTS2 */
} cetcd_snap_header;

/* CTS1 / CTS2 / raw kv. Truncated CTS2 header is INVAL. */
int cetcd_snap_parse_header(const uint8_t *data, size_t len,
                            cetcd_snap_header *out);
uint32_t cetcd_snap_crc32c(const uint8_t *kv, size_t len);
/* OK if CTS2 matches or file has no hash (CTS1/raw). CORRUPT on mismatch. */
int cetcd_snap_verify(const uint8_t *data, size_t len);
/* CTS2 + kv. Caller frees. */
uint8_t *cetcd_snap_encode_cts2(const uint8_t *kv, size_t kv_len,
                                uint64_t rev, size_t *out_len);

void cetcd_snap_free_entries(cetcd_snap_entry *entries, size_t count);

/* etcd `%016x-%016x.snap`. Leftover-safe hex so `123foo-456.snap` /
 * `000000000000000a` cannot become a truncated decimal and win latest.
 * Index must be > 0. */
int cetcd_parse_snap_filename(const char *name, uint64_t *term,
                              uint64_t *index);
/* leftover-safe migrate argv from `start`. Honors --data-dir / --output-dir
 * (`--flag=VALUE` or next argv). Empty `--flag=` is INVAL. A leftover
 * `--` value (`--data-dir --output-dir`) is INVAL so a flag cannot become
 * the path. --verbose[=bool]. Unknown leftover flags are INVAL. Missing
 * data-dir or output-dir is INVAL. */
int cetcd_parse_migrate_argv(int argc, char *const *argv, int start,
                             const char **data_dir, const char **output_dir,
                             int *verbose);

#ifdef __cplusplus
}
#endif
#endif
