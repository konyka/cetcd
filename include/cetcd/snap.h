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

/* Maintenance Snapshot / snapshot.kv: optional CTS1+LE-rev header, then
 * repeated (varint key_len + key + varint val_len + val). Truncated or
 * leftover bytes fail-closed (NULL). Empty blob is a valid empty snap. */
cetcd_snap *cetcd_snap_decode_kv(const uint8_t *data, size_t len);
/* Inverse of decode_kv (no CTS1 header). Caller frees the buffer. */
uint8_t *cetcd_snap_encode_kv(const cetcd_snap *s, size_t *out_len);

void cetcd_snap_free_entries(cetcd_snap_entry *entries, size_t count);

#ifdef __cplusplus
}
#endif
#endif
