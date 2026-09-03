#define _POSIX_C_SOURCE 200809L
#include "cetcd/wal.h"
#include "cetcd/raft.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#if defined(_WIN32)
#  include <io.h>
#  include <sys/stat.h>
#else
#  include <unistd.h>
#  include <sys/stat.h>
#endif

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
    if (n > SIZE_MAX / 2) return -1;
    size_t newcap = w->cap ? w->cap * 2 : 128;
    while (newcap < n) {
        if (newcap > SIZE_MAX / 2) { newcap = n; break; }
        newcap *= 2;
    }
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

int cetcd_wal_resolve_path(const char *path, char *out, size_t cap) {
    if (!path || !out || cap < 2) return -1;
    struct stat st;
    if (stat(path, &st) == 0 && S_ISDIR(st.st_mode)) {
        int n = snprintf(out, cap, "%s/0000000000000000.wal", path);
        if (n < 0 || (size_t)n >= cap) return -1;
        return 0;
    }
    size_t n = strlen(path);
    if (n >= cap) return -1;
    memcpy(out, path, n + 1);
    return 0;
}

static int path_is_regular_(const char *path) {
    struct stat st;
    if (stat(path, &st) != 0) return 0;
    return S_ISREG(st.st_mode);
}

/* Public API */
cetcd_wal_encoder *cetcd_wal_encoder_create(const char *path) {
    if (!path) return NULL;
    char resolved[1024];
    if (cetcd_wal_resolve_path(path, resolved, sizeof(resolved)) != 0) return NULL;
    cetcd_wal_encoder *enc = (cetcd_wal_encoder*)calloc(1, sizeof(*enc));
    if (!enc) return NULL;
    const char *mode = path_is_regular_(resolved) ? "ab" : "wb";
    enc->fp = fopen(resolved, mode);
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
    int oom = 0;
    oom |= wbuf_append_byte(&w, 0x08);
    oom |= wbuf_write_varint(&w, (uint64_t)rec->type);
    oom |= wbuf_append_byte(&w, 0x10);
    oom |= wbuf_write_varint(&w, (uint64_t)rec->crc);
    oom |= wbuf_append_byte(&w, 0x1a);
    oom |= wbuf_write_varint(&w, (uint64_t)rec->data_len);
    if (rec->data_len > 0) oom |= wbuf_append(&w, rec->data, rec->data_len);
    if (oom) { wbuf_free(&w); return -1; }

    int pad = (8 - (int)(w.len % 8)) % 8;
    int ret = cetcd_wal_write_frame(enc->fp, w.d, w.len, pad);
    wbuf_free(&w);
    return ret;
}

int cetcd_wal_encode_metadata(cetcd_wal_encoder *enc, const uint8_t *data, size_t len) {
    if (!enc) return -1;
    cetcd_wal_record rec; cetcd_wal_record_init(&rec);
    rec.type = CETCD_WAL_METADATA;
    if (len > 0) {
        rec.data = (uint8_t *)malloc(len);
        if (!rec.data) return -1;
        memcpy(rec.data, data, len);
        rec.data_cap = len;
    }
    rec.data_len = len;
    rec.crc = 0;
    int r = cetcd_wal_encode(enc, &rec);
    cetcd_wal_record_free(&rec);
    return r;
}

int cetcd_wal_encode_entry(cetcd_wal_encoder *enc, const cetcd_entry *entry) {
    if (!enc || !entry) return -1;
    wbuf w; wbuf_init(&w);
    int oom = 0;
    oom |= wbuf_append_byte(&w, 0x08); oom |= wbuf_write_varint(&w, entry->term);
    oom |= wbuf_append_byte(&w, 0x10); oom |= wbuf_write_varint(&w, entry->index);
    oom |= wbuf_append_byte(&w, 0x18); oom |= wbuf_write_varint(&w, (uint64_t)entry->type);
    oom |= wbuf_append_byte(&w, 0x22); oom |= wbuf_write_varint(&w, (uint64_t)entry->data.len);
    if (entry->data.len > 0) oom |= wbuf_append(&w, entry->data.data, entry->data.len);
    if (oom) { wbuf_free(&w); return -1; }
    cetcd_wal_record rec; cetcd_wal_record_init(&rec);
    rec.type = CETCD_WAL_ENTRY; rec.data = w.d; rec.data_len = w.len;
    rec.crc = crc32c(0, rec.data, rec.data_len);
    int r = cetcd_wal_encode(enc, &rec);
    cetcd_wal_record_free(&rec);
    return r;
}

int cetcd_wal_encode_hard_state(cetcd_wal_encoder *enc, const cetcd_hard_state *hs) {
    if (!enc || !hs) return -1;
    wbuf w; wbuf_init(&w);
    int oom = 0;
    oom |= wbuf_append_byte(&w, 0x08); oom |= wbuf_write_varint(&w, hs->term);
    oom |= wbuf_append_byte(&w, 0x10); oom |= wbuf_write_varint(&w, hs->vote);
    oom |= wbuf_append_byte(&w, 0x18); oom |= wbuf_write_varint(&w, hs->commit);
    if (oom) { wbuf_free(&w); return -1; }
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

int cetcd_wal_encoder_sync(cetcd_wal_encoder *enc) {
    if (!enc || !enc->fp) return -1;
    if (fflush(enc->fp) != 0) return -1;
#if defined(_WIN32)
    return _commit(fileno(enc->fp));
#else
    return fsync(fileno(enc->fp));
#endif
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

static int read_varint_local_(const uint8_t *buf, size_t *pos, size_t end, uint64_t *out) {
    uint64_t v = 0; int shift = 0; size_t i = *pos;
    while (i < end) {
        uint8_t b = buf[i++];
        if (shift >= 64) return -1;
        v |= (uint64_t)(b & 0x7F) << shift;
        if (!(b & 0x80)) { *pos = i; *out = v; return 0; }
        shift += 7;
    }
    return -1;
}

int cetcd_wal_decode_entry(const uint8_t *data, size_t len, cetcd_entry *out) {
    if (!data || !out) return -1;
    memset(out, 0, sizeof(*out));
    size_t pos = 0;
    while (pos < len) {
        uint64_t tag = 0;
        if (read_varint_local_(data, &pos, len, &tag) != 0) return -1;
        uint64_t field = tag >> 3;
        uint64_t wire = tag & 0x07;
        if (field == 1 && wire == 0) {
            uint64_t v = 0;
            if (read_varint_local_(data, &pos, len, &v) != 0) return -1;
            out->term = v;
        } else if (field == 2 && wire == 0) {
            uint64_t v = 0;
            if (read_varint_local_(data, &pos, len, &v) != 0) return -1;
            out->index = v;
        } else if (field == 3 && wire == 0) {
            uint64_t v = 0;
            if (read_varint_local_(data, &pos, len, &v) != 0) return -1;
            out->type = (cetcd_entry_type)v;
        } else if (field == 4 && wire == 2) {
            uint64_t l = 0;
            if (read_varint_local_(data, &pos, len, &l) != 0) return -1;
            if (l > len - pos) return -1;
            if (l > 0) {
                uint8_t *p = (uint8_t *)malloc((size_t)l);
                if (!p) return -1;
                memcpy(p, data + pos, (size_t)l);
                out->data.data = p;
                out->data.len = (size_t)l;
            }
            pos += (size_t)l;
        } else if (wire == 0) {
            uint64_t v = 0;
            if (read_varint_local_(data, &pos, len, &v) != 0) return -1;
        } else if (wire == 2) {
            uint64_t l = 0;
            if (read_varint_local_(data, &pos, len, &l) != 0) return -1;
            if (l > len - pos) return -1;
            pos += (size_t)l;
        } else {
            return -1;
        }
    }
    return 0;
}

int cetcd_wal_decode_hard_state(const uint8_t *data, size_t len, cetcd_hard_state *out) {
    if (!data || !out) return -1;
    memset(out, 0, sizeof(*out));
    size_t pos = 0;
    while (pos < len) {
        uint64_t tag = 0;
        if (read_varint_local_(data, &pos, len, &tag) != 0) return -1;
        uint64_t field = tag >> 3;
        uint64_t wire = tag & 0x07;
        if (wire != 0) return -1;
        uint64_t v = 0;
        if (read_varint_local_(data, &pos, len, &v) != 0) return -1;
        if (field == 1) out->term = v;
        else if (field == 2) out->vote = v;
        else if (field == 3) out->commit = v;
    }
    return 0;
}
