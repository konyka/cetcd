#include "cetcd/wal.h"
#include <stdlib.h>
#include <string.h>

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
    cetcd_wal_decoder *d = (cetcd_wal_decoder*)calloc(1, sizeof(*d));
    if (!d) return NULL;
    d->fp = fopen(path, "rb");
    if (!d->fp) { free(d); return NULL; }
    d->running_crc = 0;
    return d;
}

void cetcd_wal_decoder_free(cetcd_wal_decoder *dec) {
    if (!dec) return;
    if (dec->fp) fclose(dec->fp);
    free(dec);
}

static uint64_t read_varint(const uint8_t *buf, size_t *pos, size_t end, uint64_t *out) {
    uint64_t v = 0; int shift = 0; size_t i = *pos;
    while (i < end) {
        uint8_t b = buf[i++];
        v |= (uint64_t)(b & 0x7F) << shift;
        if (!(b & 0x80)) break;
        shift += 7;
    }
    *pos = i;
    *out = v;
    return (i <= end) ? 0 : -1;
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
        while (pos < end) {
            uint64_t tag;
            if (read_varint(frame, &pos, end, &tag) != 0) break;
            uint64_t field = tag >> 3;
            uint64_t wire  = tag & 0x07;
            if (field == 1 && wire == 0) {
                uint64_t val; read_varint(frame, &pos, end, &val);
                tmp.type = (cetcd_wal_rec_type)(int)val;
            } else if (field == 2 && wire == 0) {
                uint64_t val; read_varint(frame, &pos, end, &val);
                tmp.crc = (uint32_t)val;
            } else if (field == 3 && wire == 2) {
                uint64_t l; read_varint(frame, &pos, end, &l);
                size_t len = (size_t)l;
                if (pos+len <= end) {
                    tmp.data = (uint8_t*)malloc(len);
                    if (tmp.data) memcpy(tmp.data, frame+pos, len);
                    tmp.data_len = len;
                    pos += len;
                } else {
                    /* truncated */ break;
                }
            } else {
                /* unknown field: skip depending on wire */
                if (wire == 0) { uint64_t v; read_varint(frame, &pos, end, &v); }
                else if (wire == 1) { pos += 8; }
                else if (wire == 2) { uint64_t l; read_varint(frame, &pos, end, &l); pos += (size_t)l; }
                else if (wire == 5) { pos += 4; }
            }
        }
        // Validate
        int ret = 0;
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
