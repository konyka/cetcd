#include "cetcd/snap.h"
#include "cetcd/base.h"

#include <stdlib.h>
#include <string.h>
#include <stdint.h>

struct cetcd_snap {
    cetcd_snap_entry *entries; /* dynamic array of entries */
    size_t count;
    size_t cap;
};

/* Helpers: big-endian encoding/decoding helpers. */
static uint32_t be32_read(const uint8_t *p) {
    return ((uint32_t)p[0] << 24) |
           ((uint32_t)p[1] << 16) |
           ((uint32_t)p[2] << 8)  |
           ((uint32_t)p[3]);
}

static uint64_t be64_read(const uint8_t *p) {
    return ((uint64_t)p[0] << 56) |
           ((uint64_t)p[1] << 48) |
           ((uint64_t)p[2] << 40) |
           ((uint64_t)p[3] << 32) |
           ((uint64_t)p[4] << 24) |
           ((uint64_t)p[5] << 16) |
           ((uint64_t)p[6] << 8)  |
           ((uint64_t)p[7]);
}

static void be32_write(uint8_t *dst, uint32_t v) {
    dst[0] = (uint8_t)((v >> 24) & 0xff);
    dst[1] = (uint8_t)((v >> 16) & 0xff);
    dst[2] = (uint8_t)((v >> 8) & 0xff);
    dst[3] = (uint8_t)(v & 0xff);
}

static void be64_write(uint8_t *dst, uint64_t v) {
    dst[0] = (uint8_t)((v >> 56) & 0xff);
    dst[1] = (uint8_t)((v >> 48) & 0xff);
    dst[2] = (uint8_t)((v >> 40) & 0xff);
    dst[3] = (uint8_t)((v >> 32) & 0xff);
    dst[4] = (uint8_t)((v >> 24) & 0xff);
    dst[5] = (uint8_t)((v >> 16) & 0xff);
    dst[6] = (uint8_t)((v >> 8) & 0xff);
    dst[7] = (uint8_t)(v & 0xff);
}

/* Lifecycle */
cetcd_snap *cetcd_snap_new(void) {
    cetcd_snap *s = (cetcd_snap *)calloc(1, sizeof(*s));
    if (!s) return NULL;
    s->entries = NULL;
    s->count = 0;
    s->cap = 0;
    return s;
}

void cetcd_snap_free_entries(cetcd_snap_entry *entries, size_t count) {
    if (!entries) return;
    for (size_t i = 0; i < count; ++i) {
        if (entries[i].key) free(entries[i].key);
        if (entries[i].value) free(entries[i].value);
    }
    free(entries);
}

void cetcd_snap_free(cetcd_snap *s) {
    if (!s) return;
    if (s->entries) {
        cetcd_snap_free_entries(s->entries, s->count);
    }
    free(s);
}

/* Add an entry; returns CETCD_OK on success. */
int cetcd_snap_add_entry(cetcd_snap *s,
                        const uint8_t *key, size_t key_len,
                        const uint8_t *value, size_t value_len,
                        int64_t mod_revision) {
    if (!s) return CETCD_ERR_INVAL;
    /* grow if needed */
    if (s->count >= s->cap) {
        size_t new_cap = (s->cap == 0) ? 4 : s->cap * 2;
        cetcd_snap_entry *ne = (cetcd_snap_entry *)realloc(s->entries, new_cap * sizeof(*ne));
        if (!ne) return CETCD_ERR_NOMEM;
        s->entries = ne;
        s->cap = new_cap;
    }

    cetcd_snap_entry e;
    e.key = NULL; e.value = NULL; e.key_len = key_len; e.value_len = value_len; e.mod_revision = mod_revision;
    if (key_len > 0) {
        e.key = (uint8_t *)malloc(key_len);
        if (!e.key) return CETCD_ERR_NOMEM;
        memcpy(e.key, key, key_len);
    }
    if (value_len > 0) {
        e.value = (uint8_t *)malloc(value_len);
        if (!e.value) {
            if (e.key) free(e.key);
            return CETCD_ERR_NOMEM;
        }
        memcpy(e.value, value, value_len);
    }
    s->entries[s->count] = e;
    s->count++;
    return CETCD_OK;
}

size_t cetcd_snap_entry_count(const cetcd_snap *s) {
    return s ? s->count : 0;
}

cetcd_snap_entry *cetcd_snap_get_entry(const cetcd_snap *s, size_t idx) {
    if (!s) return NULL;
    if (idx >= s->count) return NULL;
    return &s->entries[idx];
}

/* Encode to wire format as described in tests. Returns malloc'd buffer. */
uint8_t *cetcd_snap_encode(const cetcd_snap *s, size_t *out_len) {
    if (!out_len) return NULL;
    *out_len = 0;
    if (!s) return NULL;

    size_t total = 4; /* entry count */
    for (size_t i = 0; i < s->count; ++i) {
        total += 4 + s->entries[i].key_len;   /* key len + key */
        total += 4 + s->entries[i].value_len; /* value len + value */
        total += 8;                            /* mod_revision */
    }

    uint8_t *buf = (uint8_t *)malloc(total);
    if (!buf) return NULL;
    *out_len = total;
    size_t off = 0;
    be32_write(buf + off, (uint32_t)s->count); off += 4;
    for (size_t i = 0; i < s->count; ++i) {
        const cetcd_snap_entry *e = &s->entries[i];
        be32_write(buf + off, (uint32_t)e->key_len); off += 4;
        if (e->key_len > 0) memcpy(buf + off, e->key, e->key_len);
        off += e->key_len;
        be32_write(buf + off, (uint32_t)e->value_len); off += 4;
        if (e->value_len > 0) memcpy(buf + off, e->value, e->value_len);
        off += e->value_len;
        uint64_t m = (uint64_t)e->mod_revision;
        be64_write(buf + off, m); off += 8;
    }
    return buf;
}

/* Decode wire format into a new snap instance. */
cetcd_snap *cetcd_snap_decode(const uint8_t *data, size_t len) {
    if (!data || len < 4) return NULL;
    size_t off = 0;
    uint32_t count = be32_read(data + off); off += 4;
    /* basic sanity: count cannot be absurdly large given len */
    if ((size_t)count > (len - 4) / 4) {
        return NULL;
    }

    cetcd_snap *s = cetcd_snap_new();
    if (!s) return NULL;
    s->entries = NULL;
    s->count = 0;
    s->cap = count;
    if (count > 0) {
        s->entries = (cetcd_snap_entry *)calloc(count, sizeof(cetcd_snap_entry));
        if (!s->entries) {
            cetcd_snap_free(s);
            return NULL;
        }
    }

    for (size_t i = 0; i < count; ++i) {
        if (off + 4 > len) { /* key_len */
            cetcd_snap_free(s); return NULL;
        }
        uint32_t key_len = be32_read(data + off); off += 4;
        if (off + key_len > len) { cetcd_snap_free(s); return NULL; }
        uint8_t *k = NULL;
        if (key_len > 0) {
            k = (uint8_t *)malloc(key_len);
            if (!k) { cetcd_snap_free(s); return NULL; }
            memcpy(k, data + off, key_len);
        }
        off += key_len;

        if (off + 4 > len) { if (k) free(k); cetcd_snap_free(s); return NULL; }
        uint32_t value_len = be32_read(data + off); off += 4;
        if (off + value_len > len) { if (k) free(k); cetcd_snap_free(s); return NULL; }
        uint8_t *v = NULL;
        if (value_len > 0) {
            v = (uint8_t *)malloc(value_len);
            if (!v) { if (k) free(k); cetcd_snap_free(s); return NULL; }
            memcpy(v, data + off, value_len);
        }
        off += value_len;

        if (off + 8 > len) { if (k) free(k); if (v) free(v); cetcd_snap_free(s); return NULL; }
        uint64_t mrev = be64_read(data + off); off += 8;

        cetcd_snap_entry *ei = &s->entries[i];
        ei->key = k; ei->key_len = key_len; ei->value = v; ei->value_len = value_len; ei->mod_revision = (int64_t)mrev;
        s->count++;
    }
    return s;
}
