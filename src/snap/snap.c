#include "cetcd/snap.h"
#include "cetcd/base.h"
#include "cetcd/hash.h"

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
        if (s->entries[i].key_len > SIZE_MAX - total ||
            s->entries[i].value_len > SIZE_MAX - total) return NULL;
        total += 4 + s->entries[i].key_len;
        total += 4 + s->entries[i].value_len;
        total += 8;
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
        if (key_len > len - off) { cetcd_snap_free(s); return NULL; }
        uint8_t *k = NULL;
        if (key_len > 0) {
            k = (uint8_t *)malloc(key_len);
            if (!k) { cetcd_snap_free(s); return NULL; }
            memcpy(k, data + off, key_len);
        }
        off += key_len;

        if (off + 4 > len) { if (k) free(k); cetcd_snap_free(s); return NULL; }
        uint32_t value_len = be32_read(data + off); off += 4;
        if (value_len > len - off) { if (k) free(k); cetcd_snap_free(s); return NULL; }
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

static int read_varint_kv_(const uint8_t *p, size_t n, size_t *pos, uint64_t *out) {
    uint64_t v = 0;
    int shift = 0;
    while (*pos < n) {
        uint8_t b = p[(*pos)++];
        v |= (uint64_t)(b & 0x7fu) << shift;
        if ((b & 0x80u) == 0) {
            *out = v;
            return 0;
        }
        shift += 7;
        if (shift > 63) return -1;
    }
    return -1;
}

cetcd_snap *cetcd_snap_decode_kv(const uint8_t *data, size_t len) {
    if (!data && len > 0) return NULL;
    const uint8_t *p = data;
    size_t n = len;
    if (n >= 16 && p && memcmp(p, "CTS2", 4) == 0) {
        p += 16;
        n -= 16;
    } else if (n >= 12 && p && memcmp(p, "CTS1", 4) == 0) {
        p += 12;
        n -= 12;
    } else if (n >= 4 && n < 16 && p && memcmp(p, "CTS2", 4) == 0) {
        return NULL;
    }
    if (n == 0) {
        cetcd_snap *empty = cetcd_snap_new();
        return empty;
    }
    if (!p) return NULL;

    cetcd_snap *s = cetcd_snap_new();
    if (!s) return NULL;
    size_t pos = 0;
    while (pos < n) {
        uint64_t kl = 0, vl = 0;
        if (read_varint_kv_(p, n, &pos, &kl) != 0) {
            cetcd_snap_free(s);
            return NULL;
        }
        if (kl > n - pos || kl > (16u * 1024u * 1024u)) {
            cetcd_snap_free(s);
            return NULL;
        }
        const uint8_t *key = p + pos;
        pos += (size_t)kl;
        if (read_varint_kv_(p, n, &pos, &vl) != 0) {
            cetcd_snap_free(s);
            return NULL;
        }
        if (vl > n - pos || vl > (16u * 1024u * 1024u)) {
            cetcd_snap_free(s);
            return NULL;
        }
        const uint8_t *val = p + pos;
        pos += (size_t)vl;
        if (cetcd_snap_add_entry(s, key, (size_t)kl, val, (size_t)vl, 0) != CETCD_OK) {
            cetcd_snap_free(s);
            return NULL;
        }
    }
    return s;
}

static size_t write_varint_kv_(uint8_t *p, uint64_t v) {
    size_t n = 0;
    do {
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        p[n++] = b;
    } while (v);
    return n;
}

uint8_t *cetcd_snap_encode_kv(const cetcd_snap *s, size_t *out_len) {
    if (out_len) *out_len = 0;
    if (!s || !out_len) return NULL;
    size_t need = 1;
    for (size_t i = 0; i < s->count; i++) {
        need += 10 + s->entries[i].key_len + 10 + s->entries[i].value_len;
    }
    uint8_t *buf = (uint8_t *)malloc(need);
    if (!buf) return NULL;
    size_t pos = 0;
    for (size_t i = 0; i < s->count; i++) {
        const cetcd_snap_entry *e = &s->entries[i];
        pos += write_varint_kv_(buf + pos, (uint64_t)e->key_len);
        if (e->key_len && e->key) {
            memcpy(buf + pos, e->key, e->key_len);
            pos += e->key_len;
        }
        pos += write_varint_kv_(buf + pos, (uint64_t)e->value_len);
        if (e->value_len && e->value) {
            memcpy(buf + pos, e->value, e->value_len);
            pos += e->value_len;
        }
    }
    *out_len = pos;
    return buf;
}

static uint32_t le32_read_(const uint8_t *p) {
    return (uint32_t)p[0] |
           ((uint32_t)p[1] << 8) |
           ((uint32_t)p[2] << 16) |
           ((uint32_t)p[3] << 24);
}

static uint64_t le64_read_(const uint8_t *p) {
    return (uint64_t)p[0] |
           ((uint64_t)p[1] << 8) |
           ((uint64_t)p[2] << 16) |
           ((uint64_t)p[3] << 24) |
           ((uint64_t)p[4] << 32) |
           ((uint64_t)p[5] << 40) |
           ((uint64_t)p[6] << 48) |
           ((uint64_t)p[7] << 56);
}

static void le32_write_(uint8_t *dst, uint32_t v) {
    dst[0] = (uint8_t)(v & 0xff);
    dst[1] = (uint8_t)((v >> 8) & 0xff);
    dst[2] = (uint8_t)((v >> 16) & 0xff);
    dst[3] = (uint8_t)((v >> 24) & 0xff);
}

static void le64_write_(uint8_t *dst, uint64_t v) {
    dst[0] = (uint8_t)(v & 0xff);
    dst[1] = (uint8_t)((v >> 8) & 0xff);
    dst[2] = (uint8_t)((v >> 16) & 0xff);
    dst[3] = (uint8_t)((v >> 24) & 0xff);
    dst[4] = (uint8_t)((v >> 32) & 0xff);
    dst[5] = (uint8_t)((v >> 40) & 0xff);
    dst[6] = (uint8_t)((v >> 48) & 0xff);
    dst[7] = (uint8_t)((v >> 56) & 0xff);
}

int cetcd_snap_parse_header(const uint8_t *data, size_t len,
                            cetcd_snap_header *out) {
    if (!out) return CETCD_ERR_INVAL;
    memset(out, 0, sizeof(*out));
    if (!data && len > 0) return CETCD_ERR_INVAL;
    if (!data || len == 0) {
        return CETCD_OK;
    }
    if (len >= 4 && memcmp(data, "CTS2", 4) == 0) {
        if (len < 16) return CETCD_ERR_INVAL;
        out->revision = le64_read_(data + 4);
        out->hash = le32_read_(data + 12);
        out->kv_off = 16;
        out->kv_len = len - 16;
        out->has_hash = 1;
        return CETCD_OK;
    }
    if (len >= 12 && memcmp(data, "CTS1", 4) == 0) {
        out->revision = le64_read_(data + 4);
        out->kv_off = 12;
        out->kv_len = len - 12;
        return CETCD_OK;
    }
    out->kv_off = 0;
    out->kv_len = len;
    return CETCD_OK;
}

uint32_t cetcd_snap_crc32c(const uint8_t *kv, size_t len) {
    if (!kv || len == 0) return cetcd_crc32c(0, "", 0);
    return cetcd_crc32c(0, kv, len);
}

int cetcd_snap_verify(const uint8_t *data, size_t len) {
    cetcd_snap_header h;
    int rc = cetcd_snap_parse_header(data, len, &h);
    if (rc != CETCD_OK) return rc;
    if (!h.has_hash) return CETCD_OK;
    uint32_t got = cetcd_snap_crc32c(data + h.kv_off, h.kv_len);
    if (got != h.hash) return CETCD_ERR_CORRUPT;
    return CETCD_OK;
}

uint8_t *cetcd_snap_encode_cts2(const uint8_t *kv, size_t kv_len,
                                uint64_t rev, size_t *out_len) {
    if (out_len) *out_len = 0;
    if (!out_len) return NULL;
    if (!kv && kv_len > 0) return NULL;
    size_t total = 16 + kv_len;
    uint8_t *buf = (uint8_t *)malloc(total);
    if (!buf) return NULL;
    memcpy(buf, "CTS2", 4);
    le64_write_(buf + 4, rev);
    le32_write_(buf + 12, cetcd_snap_crc32c(kv, kv_len));
    if (kv_len > 0) memcpy(buf + 16, kv, kv_len);
    *out_len = total;
    return buf;
}

static int parse_hex_u64_(const char *s, const char *end, uint64_t *out) {
    uint64_t v = 0;
    const char *p;
    int digits = 0;
    if (!s || !end || !out || s >= end) return CETCD_ERR_INVAL;
    for (p = s; p < end; p++) {
        unsigned d;
        if (*p >= '0' && *p <= '9') d = (unsigned)(*p - '0');
        else if (*p >= 'a' && *p <= 'f') d = (unsigned)(*p - 'a' + 10);
        else if (*p >= 'A' && *p <= 'F') d = (unsigned)(*p - 'A' + 10);
        else return CETCD_ERR_INVAL;
        if (v > (UINT64_MAX >> 4)) return CETCD_ERR_INVAL;
        v = (v << 4) | (uint64_t)d;
        digits++;
    }
    if (digits == 0) return CETCD_ERR_INVAL;
    *out = v;
    return CETCD_OK;
}

static int parse_etcd_hex_filename_(const char *name, const char *ext,
                                    uint64_t *a, uint64_t *b, int index_gt0) {
    size_t n, elen;
    const char *dash;
    const char *dot;
    if (!name || !ext || !a || !b) return CETCD_ERR_INVAL;
    n = strlen(name);
    elen = strlen(ext);
    if (elen < 2 || n < 3 + elen) return CETCD_ERR_INVAL; /* 1-1 + ext */
    if (strcmp(name + n - elen, ext) != 0) return CETCD_ERR_INVAL;
    dash = strchr(name, '-');
    if (!dash || dash == name) return CETCD_ERR_INVAL;
    if (memchr(dash + 1, '-', (size_t)((name + n) - (dash + 1))))
        return CETCD_ERR_INVAL;
    dot = name + n - elen;
    if (dash + 1 >= dot) return CETCD_ERR_INVAL;
    if (parse_hex_u64_(name, dash, a) != CETCD_OK) return CETCD_ERR_INVAL;
    if (parse_hex_u64_(dash + 1, dot, b) != CETCD_OK) return CETCD_ERR_INVAL;
    if (index_gt0 && *b == 0) return CETCD_ERR_INVAL;
    return CETCD_OK;
}

int cetcd_parse_snap_filename(const char *name, uint64_t *term,
                              uint64_t *index) {
    return parse_etcd_hex_filename_(name, ".snap", term, index, 1);
}

int cetcd_parse_wal_filename(const char *name, uint64_t *seq,
                             uint64_t *index) {
    return parse_etcd_hex_filename_(name, ".wal", seq, index, 0);
}

static int migrate_flag_is_(const char *arg, const char *name) {
    size_t n;
    if (!arg || !name) return 0;
    n = strlen(name);
    if (strncmp(arg, name, n) != 0) return 0;
    return arg[n] == '\0' || arg[n] == '=';
}

static int migrate_take_path_(int *i, int argc, char *const *argv,
                              const char *name, const char **out) {
    const char *arg;
    size_t n;
    if (!i || !argv || !name || !out || *i < 0 || *i >= argc)
        return CETCD_ERR_INVAL;
    arg = argv[*i];
    n = strlen(name);
    if (arg[n] == '=') {
        if (!arg[n + 1]) return CETCD_ERR_INVAL;
        *out = arg + n + 1;
        return CETCD_OK;
    }
    if (*i + 1 >= argc || !argv[*i + 1] || !argv[*i + 1][0])
        return CETCD_ERR_INVAL;
    /* leftover `--flag` cannot become the path */
    if (argv[*i + 1][0] == '-' && argv[*i + 1][1] == '-')
        return CETCD_ERR_INVAL;
    *out = argv[++*i];
    return CETCD_OK;
}

static int migrate_take_bool_(const char *arg, const char *name, int *out) {
    size_t n;
    if (!arg || !name || !out) return CETCD_ERR_INVAL;
    n = strlen(name);
    if (arg[n] == '\0') {
        *out = 1;
        return CETCD_OK;
    }
    if (arg[n] != '=') return CETCD_ERR_INVAL;
    if (strcmp(arg + n + 1, "true") == 0 || strcmp(arg + n + 1, "1") == 0) {
        *out = 1;
        return CETCD_OK;
    }
    if (strcmp(arg + n + 1, "false") == 0 || strcmp(arg + n + 1, "0") == 0) {
        *out = 0;
        return CETCD_OK;
    }
    return CETCD_ERR_INVAL;
}

int cetcd_parse_migrate_argv(int argc, char *const *argv, int start,
                             const char **data_dir, const char **output_dir,
                             int *verbose) {
    int i;
    if (!argv || !data_dir || !output_dir || !verbose ||
        start < 0 || start > argc)
        return CETCD_ERR_INVAL;
    *data_dir = NULL;
    *output_dir = NULL;
    *verbose = 0;
    for (i = start; i < argc; i++) {
        if (!argv[i]) return CETCD_ERR_INVAL;
        if (strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "-h") == 0)
            continue;
        if (migrate_flag_is_(argv[i], "--data-dir")) {
            if (migrate_take_path_(&i, argc, argv, "--data-dir", data_dir)
                != CETCD_OK)
                return CETCD_ERR_INVAL;
            continue;
        }
        if (migrate_flag_is_(argv[i], "--output-dir")) {
            if (migrate_take_path_(&i, argc, argv, "--output-dir", output_dir)
                != CETCD_OK)
                return CETCD_ERR_INVAL;
            continue;
        }
        if (migrate_flag_is_(argv[i], "--verbose")) {
            if (migrate_take_bool_(argv[i], "--verbose", verbose) != CETCD_OK)
                return CETCD_ERR_INVAL;
            continue;
        }
        return CETCD_ERR_INVAL;
    }
    if (!*data_dir || !*output_dir) return CETCD_ERR_INVAL;
    return CETCD_OK;
}
