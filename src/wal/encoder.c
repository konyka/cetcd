#include "cetcd/wal.h"
#include "cetcd/raft.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

struct cetcd_wal_encoder {
    FILE    *fp;
    uint32_t running_crc;
};

/* CRC-32C (Castagnoli) implemented as a bitwise routine (no table required). */
static uint32_t crc32c(uint32_t crc, const uint8_t *buf, size_t len) {
    crc = ~crc;
    for (size_t i = 0; i < len; ++i) {
        crc ^= buf[i];
        for (int b = 0; b < 8; ++b) {
            if (crc & 1)
                crc = (crc >> 1) ^ 0x82F63B78U; /* reversed poly 0x1EDC6F41 */
            else
                crc >>= 1;
        }
    }
    return ~crc;
}

/* Simple dynamic buffer used for protobuf-like encoding */
typedef struct wbuf {
    uint8_t *d;
    size_t   len;
    size_t   cap;
} wbuf;

static int wbuf_init(wbuf *w) {
    w->d = NULL;
    w->len = 0;
    w->cap = 0;
    return 0;
}

static int wbuf_free(wbuf *w) {
    if (w->d) free(w->d);
    w->d = NULL; w->len = 0; w->cap = 0;
    return 0;
}

static int wbuf_reserve(wbuf *w, size_t n) {
    if (w->cap >= n) return 0;
    size_t newcap = w->cap ? w->cap * 2 : 128;
    while (newcap < n) newcap *= 2;
    uint8_t *nd = (uint8_t*)realloc(w->d, newcap);
    if (!nd) return -1;
    w->d = nd;
    w->cap = newcap;
    return 0;
}

static int wbuf_append(wbuf *w, const void *src, size_t n) {
    if (wbuf_reserve(w, w->len + n) != 0) return -1;
    memcpy(w->d + w->len, src, n);
    w->len += n;
    return 0;
}

static int wbuf_append_byte(wbuf *w, uint8_t v) {
    return wbuf_append(w, &v, 1);
}

static int wbuf_write_varint(wbuf *w, uint64_t v) {
    uint8_t b[10]; int idx = 0;
    do {
        uint8_t byte = v & 0x7F;
        v >>= 7;
        if (v) byte |= 0x80;
        b[idx++] = byte;
    } while (v);
    return wbuf_append(w, b, (size_t)idx);
}

/* croissants: simple wrapper around a record */
static int cetcd_wal_write_frame(FILE *fp, const uint8_t *buf, size_t len, int pad) {
    uint64_t header = (uint64_t)len & 0x00FFFFFFFFFFFFFFULL;
    if (pad > 0) {
        header |= ((uint64_t)0x80 | (uint64_t)pad) << 56;
    }
    uint8_t le[8];
    for (int i = 0; i < 8; ++i) {
        le[i] = (header >> (8 * i)) & 0xFF;
    }
    if (fwrite(le, 1, 8, fp) != 8) return -1;
    if (fwrite(buf, 1, len, fp) != (size_t)len) return -1;
    if (pad > 0) {
        uint8_t zero = 0; for (int i = 0; i < pad; ++i) {
            if (fwrite(&zero, 1, 1, fp) != 1) return -1;
        }
    }
    return 0;
}

/* Public API */
cetcd_wal_encoder *cetcd_wal_encoder_create(const char *path) {
    if (!path) return NULL;
    cetcd_wal_encoder *enc = (cetcd_wal_encoder*)calloc(1, sizeof(*enc));
    if (!enc) return NULL;
    enc->fp = fopen(path, "wb");
    if (!enc->fp) { free(enc); return NULL; }
    enc->running_crc = 0;
    return enc;
}

void cetcd_wal_encoder_free(cetcd_wal_encoder *enc) {
    if (!enc) return;
    if (enc->fp) fclose(enc->fp);
    free(enc);
}

int cetcd_wal_encode(cetcd_wal_encoder *enc, cetcd_wal_record *rec) {
    if (!enc || !enc->fp || !rec) return -1;

    if (rec->data_len > 0 && rec->crc == 0) {
        rec->crc = crc32c(0, rec->data, rec->data_len);
    }

    wbuf w; wbuf_init(&w);
    wbuf_append_byte(&w, 0x08);
    wbuf_write_varint(&w, (uint64_t)rec->type);
    wbuf_append_byte(&w, 0x10);
    wbuf_write_varint(&w, (uint64_t)rec->crc);
    wbuf_append_byte(&w, 0x1a);
    wbuf_write_varint(&w, (uint64_t)rec->data_len);
    if (rec->data_len > 0) wbuf_append(&w, rec->data, rec->data_len);

    int pad = (8 - (int)(w.len % 8)) % 8;
    int ret = cetcd_wal_write_frame(enc->fp, w.d, w.len, pad);
    wbuf_free(&w);
    return ret;
}

int cetcd_wal_encode_metadata(cetcd_wal_encoder *enc, const uint8_t *data, size_t len) {
    if (!enc) return -1;
    cetcd_wal_record rec; cetcd_wal_record_init(&rec);
    rec.type = CETCD_WAL_METADATA;
    rec.data = (uint8_t*)data; rec.data_len = len; rec.crc = 0;
    int r = cetcd_wal_encode(enc, &rec);
    cetcd_wal_record_free(&rec);
    return r;
}

int cetcd_wal_encode_entry(cetcd_wal_encoder *enc, const cetcd_entry *entry) {
    if (!enc || !entry) return -1;
    // Serialize a cetcd_entry protobuf-like payload into rec.data
    wbuf w; wbuf_init(&w);
    // Field 1: term (0x08)
    wbuf_append_byte(&w, 0x08); wbuf_write_varint(&w, entry->term);
    // Field 2: index (0x10)
    wbuf_append_byte(&w, 0x10); wbuf_write_varint(&w, entry->index);
    // Field 3: type (0x18)
    wbuf_append_byte(&w, 0x18); wbuf_write_varint(&w, (uint64_t)entry->type);
    // Field 4: data (0x22) + length + payload
    wbuf_append_byte(&w, 0x22);
    wbuf_write_varint(&w, (uint64_t)entry->data.len);
    if (entry->data.len > 0) wbuf_append(&w, entry->data.data, entry->data.len);
    // Build record
    cetcd_wal_record rec; cetcd_wal_record_init(&rec);
    rec.type = CETCD_WAL_ENTRY; rec.data = w.d; rec.data_len = w.len;
    // Compute CRC from data payload
    rec.crc = crc32c(0, rec.data, rec.data_len);
    int r = cetcd_wal_encode(enc, &rec);
    // Free allocated data
    cetcd_wal_record_free(&rec);
    // w.d was moved into rec.data, so do not free here
    return r;
}

int cetcd_wal_encode_hard_state(cetcd_wal_encoder *enc, const cetcd_hard_state *hs) {
    if (!enc || !hs) return -1;
    wbuf w; wbuf_init(&w);
    // Field 1: term (0x08)
    wbuf_append_byte(&w, 0x08); wbuf_write_varint(&w, hs->term);
    // Field 2: vote (0x10)
    wbuf_append_byte(&w, 0x10); wbuf_write_varint(&w, hs->vote);
    // Field 3: commit (0x18)
    wbuf_append_byte(&w, 0x18); wbuf_write_varint(&w, hs->commit);
    cetcd_wal_record rec; cetcd_wal_record_init(&rec);
    rec.type = CETCD_WAL_STATE; rec.data = w.d; rec.data_len = w.len; rec.crc = crc32c(0, rec.data, rec.data_len);
    int r = cetcd_wal_encode(enc, &rec);
    cetcd_wal_record_free(&rec);
    return r;
}

int cetcd_wal_encoder_flush(cetcd_wal_encoder *enc) {
    if (!enc || !enc->fp) return -1;
    return fflush(enc->fp);
}

/* Record helpers */
void cetcd_wal_record_init(cetcd_wal_record *rec) {
    if (!rec) return;
    rec->type = CETCD_WAL_METADATA; /* default */
    rec->crc = 0; rec->data = NULL; rec->data_len = 0; rec->data_cap = 0;
}

void cetcd_wal_record_free(cetcd_wal_record *rec) {
    if (!rec) return;
    if (rec->data) free(rec->data);
    rec->data = NULL; rec->data_len = 0; rec->data_cap = 0; rec->crc = 0;
}
