#include "cetcd/wal.h"
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

/* CRC32C helper (same as encoder) */
static uint32_t crc32c(uint32_t crc, const uint8_t *buf, size_t len) {
    crc = ~crc;
    for (size_t i = 0; i < len; ++i) {
        crc ^= buf[i];
        for (int b = 0; b < 8; ++b) {
            if (crc & 1)
                crc = (crc >> 1) ^ 0x82F63B78U;
            else
                crc >>= 1;
        }
    }
    return ~crc;
}

struct cetcd_wal_decoder {
    FILE    *fp;
    uint32_t running_crc;
};

cetcd_wal_decoder *cetcd_wal_decoder_open(const char *path) {
    if (!path) return NULL;
    char resolved[1024];
    if (cetcd_wal_resolve_path(path, resolved, sizeof(resolved)) != 0) return NULL;
    cetcd_wal_decoder *d = (cetcd_wal_decoder*)calloc(1, sizeof(*d));
    if (!d) return NULL;
    d->fp = fopen(resolved, "rb");
    if (!d->fp) { free(d); return NULL; }
    d->running_crc = 0;
    return d;
}

void cetcd_wal_decoder_free(cetcd_wal_decoder *dec) {
    if (!dec) return;
    if (dec->fp) fclose(dec->fp);
    free(dec);
}

static int read_varint(const uint8_t *buf, size_t *pos, size_t end, uint64_t *out) {
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

int cetcd_wal_decode(cetcd_wal_decoder *dec, cetcd_wal_record *rec) {
    if (!dec || !dec->fp || !rec) return -1;
    /* Read frame header (8 bytes LE) */
    uint8_t hdr[8];
    size_t readb = fread(hdr, 1, 8, dec->fp);
    if (readb == 0) return -1; /* EOF */
    if (readb != 8) return -1; /* incomplete */
    uint64_t header = 0;
    for (int i = 0; i < 8; ++i) {
        header |= ((uint64_t)hdr[i]) << (8 * i);
    }
    uint64_t plen = header & 0x00FFFFFFFFFFFFFFULL;
    int has_pad = (header & (1ull << 63)) ? 1 : 0;
    int pad = 0;
    if (has_pad) {
        pad = (int)((header >> 56) & 0x7F);
    }
    if ((size_t)plen > 0) {
        uint8_t *frame = (uint8_t*)malloc((size_t)plen);
        if (!frame) return -1;
        if (fread(frame, 1, (size_t)plen, dec->fp) != plen) { free(frame); return -1; }
        if (has_pad && pad > 0) {
            /* skip pad bytes */
            if (pad > 0) fseek(dec->fp, pad, SEEK_CUR);
        }
        // Parse protobuf-like record
        size_t pos = 0; size_t end = (size_t)plen;
        cetcd_wal_record tmp; cetcd_wal_record_init(&tmp);
        int malformed = 0;
        while (pos < end) {
            uint64_t tag;
            if (read_varint(frame, &pos, end, &tag) != 0) { malformed = 1; break; }
            uint64_t field = tag >> 3;
            uint64_t wire  = tag & 0x07;
            if (field == 1 && wire == 0) {
                uint64_t val;
                if (read_varint(frame, &pos, end, &val) != 0) { malformed = 1; break; }
                tmp.type = (cetcd_wal_rec_type)(int)val;
            } else if (field == 2 && wire == 0) {
                uint64_t val;
                if (read_varint(frame, &pos, end, &val) != 0) { malformed = 1; break; }
                tmp.crc = (uint32_t)val;
            } else if (field == 3 && wire == 2) {
                uint64_t l;
                if (read_varint(frame, &pos, end, &l) != 0) { malformed = 1; break; }
                if (l > end - pos) { malformed = 1; break; }
                size_t len = (size_t)l;
                tmp.data = (uint8_t*)malloc(len ? len : 1);
                if (!tmp.data) { malformed = 1; break; }
                if (len) memcpy(tmp.data, frame+pos, len);
                tmp.data_len = len;
                pos += len;
            } else {
                if (wire == 0) {
                    uint64_t v;
                    if (read_varint(frame, &pos, end, &v) != 0) { malformed = 1; break; }
                } else if (wire == 1) {
                    if (end - pos < 8) { malformed = 1; break; }
                    pos += 8;
                } else if (wire == 2) {
                    uint64_t l;
                    if (read_varint(frame, &pos, end, &l) != 0) { malformed = 1; break; }
                    if (l > end - pos) { malformed = 1; break; }
                    pos += (size_t)l;
                } else if (wire == 5) {
                    if (end - pos < 4) { malformed = 1; break; }
                    pos += 4;
                } else {
                    malformed = 1; break;
                }
            }
        }
        // Validate
        int ret = 0;
        if (malformed) {
            cetcd_wal_record_free(&tmp);
            free(frame);
            return -3;
        }
        if (tmp.data && tmp.data_len > 0) {
            uint32_t c = crc32c(0, tmp.data, tmp.data_len);
            if (c != tmp.crc) ret = -2; /* CRC mismatch */
        }
        *rec = tmp;
        free(frame);
        return ret;
    }
    return -1;
}
