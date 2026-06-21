/*
 * cetcd-migrate — one-way data migration from etcd (bbolt + WAL + snap) to cetcd (LMDB)
 *
 * Reads etcd's bbolt database, WAL, and snapshot files directly in C and
 * writes the data into cetcd's LMDB backend using the MVCC key layout.
 *
 * Usage:
 *   cetcd-migrate --data-dir /path/to/etcd/data \
 *                 --output-dir /path/to/cetcd/data [--verbose]
 */

#include "cetcd/base.h"
#include "cetcd/backend.h"
#include "cetcd/mvcc.h"
#include "cetcd/log.h"
#include "cetcd/arena.h"
#include "cetcd/buf.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <inttypes.h>

#if defined(_WIN32)
#  include <windows.h>
#  include <direct.h>
#  include <io.h>
#  define CETCD_MKDIR(path) _mkdir(path)
#  define CETCD_ACCESS(path) _access(path, 0)
#else
#  include <dirent.h>
#  include <sys/stat.h>
#  include <unistd.h>
#  define CETCD_MKDIR(path) mkdir(path, 0755)
#  define CETCD_ACCESS(path) access(path, F_OK)
#endif

/* ── bbolt format constants ──────────────────────────────────────── */

#define BBOLT_FLAG_META     0x04
#define BBOLT_FLAG_FREELIST 0x10
#define BBOLT_FLAG_BRANCH   0x01
#define BBOLT_FLAG_LEAF     0x02
#define BBOLT_FLAG_BUCKET   0x01   /* leaf element: sub-bucket */

#define BBOLT_MAGIC         0xED0CDAEDu
#define BBOLT_VERSION       2
#define BBOLT_DEFAULT_PAGESIZE 4096
#define BBOLT_HEADER_SIZE       16   /* page header: id(8)+flags(2)+count(2)+overflow(4) */
#define BBOLT_LEAF_ELEM_SIZE    16   /* flags(4)+pos(4)+ksize(4)+vsize(4) */
#define BBOLT_BRANCH_ELEM_SIZE  20   /* pos(4)+ksize(4)+pgid(8) */
#define BBOLT_BUCKET_VAL_SIZE   16   /* root_pgid(8)+sequence(8) */

/* ── Migration statistics ────────────────────────────────────────── */

typedef struct {
    size_t   total_keys;
    int64_t  latest_rev_main;
    size_t   failed_entries;
    size_t   skipped_entries;
    uint64_t db_size;
    int64_t  snap_term;
    int64_t  snap_index;
} migrate_stats;

/* ── Parsed etcd KeyValue (from protobuf) ────────────────────────── */

typedef struct {
    uint8_t *key;
    size_t   key_len;
    uint8_t *value;
    size_t   value_len;
    int64_t  mod_revision;
    int64_t  version;
    int64_t  create_revision;
    int64_t  lease;
} etcd_kv;

/* ── bbolt reader context ────────────────────────────────────────── */

typedef struct {
    FILE       *fp;
    uint32_t    page_size;
    uint64_t    page_count;
    uint64_t    root_pgid;
    uint64_t    txid;
    cetcd_arena *arena;
} bbolt_reader;

/* ── Callback for bucket iteration ───────────────────────────────── */

typedef void (*bbolt_kv_cb)(const uint8_t *key, size_t key_len,
                             const uint8_t *val, size_t val_len,
                             void *udata);

/* ── Little-endian readers (bbolt stores integers in native LE) ─── */

static uint16_t rd_u16_le(const uint8_t *p) {
    return (uint16_t)p[0] | ((uint16_t)p[1] << 8);
}

static uint32_t rd_u32_le(const uint8_t *p) {
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) |
           ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}

static uint64_t rd_u64_le(const uint8_t *p) {
    uint64_t lo = rd_u32_le(p);
    uint64_t hi = rd_u32_le(p + 4);
    return lo | (hi << 32);
}

/* Big-endian reader for etcd revision keys */
static uint64_t rd_u64_be(const uint8_t *p) {
    uint64_t v = 0;
    for (int i = 0; i < 8; i++) v = (v << 8) | p[i];
    return v;
}

/* ── Utility: recursive mkdir ────────────────────────────────────── */

static int mkdir_p(const char *path) {
    if (!path || !*path) return -1;
    char buf[1024];
    snprintf(buf, sizeof(buf), "%s", path);
    size_t len = strlen(buf);
    while (len > 0 && (buf[len - 1] == '/' || buf[len - 1] == '\\'))
        buf[--len] = '\0';
    for (char *p = buf + 1; *p; p++) {
        if (*p == '/' || *p == '\\') {
            *p = '\0';
            CETCD_MKDIR(buf);
            *p = '/';
        }
    }
    return CETCD_MKDIR(buf) == 0 ? 0 : -1;
}

/* ── Utility: path join ──────────────────────────────────────────── */

static int path_join(char *out, size_t cap, const char *a, const char *b) {
    size_t la = strlen(a);
    if (la > 0 && a[la - 1] == '/') la--;
    return snprintf(out, cap, "%.*s/%s", (int)la, a, b);
}

/* ── Utility: check if file exists ───────────────────────────────── */

static bool file_exists(const char *path) {
#if defined(_WIN32)
    return GetFileAttributesA(path) != INVALID_FILE_ATTRIBUTES;
#else
    return CETCD_ACCESS(path) == 0;
#endif
}

/* ── Utility: directory listing for snapshot/WAL discovery ───────── */

typedef struct {
    char   **names;
    size_t   count;
    size_t   cap;
} dir_list;

static void dir_list_init(dir_list *dl) {
    dl->names = NULL; dl->count = 0; dl->cap = 0;
}

static void dir_list_add(dir_list *dl, const char *name) {
    if (dl->count == dl->cap) {
        size_t nc = dl->cap ? dl->cap * 2 : 8;
        char **tmp = (char**)realloc(dl->names, nc * sizeof(char*));
        if (!tmp) return;
        dl->names = tmp; dl->cap = nc;
    }
    dl->names[dl->count++] = strdup(name);
}

static void dir_list_free(dir_list *dl) {
    for (size_t i = 0; i < dl->count; i++) free(dl->names[i]);
    free(dl->names);
    dl->names = NULL; dl->count = 0; dl->cap = 0;
}

static int dir_list_files(const char *path, dir_list *dl) {
    dir_list_init(dl);
#if defined(_WIN32)
    char pattern[1024];
    snprintf(pattern, sizeof(pattern), "%s\\*", path);
    WIN32_FIND_DATAA fd;
    HANDLE h = FindFirstFileA(pattern, &fd);
    if (h == INVALID_HANDLE_VALUE) return -1;
    do {
        if (!(fd.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY))
            dir_list_add(dl, fd.cFileName);
    } while (FindNextFileA(h, &fd));
    FindClose(h);
#else
    DIR *d = opendir(path);
    if (!d) return -1;
    struct dirent *ent;
    while ((ent = readdir(d)) != NULL) {
        if (ent->d_type == DT_REG || ent->d_type == DT_UNKNOWN)
            dir_list_add(dl, ent->d_name);
    }
    closedir(d);
#endif
    return 0;
}

/* ── Protobuf varint encoder (write varint to cetcd_buf) ─────────── */

static int encode_varint_buf(cetcd_buf *pb, uint64_t val) {
    uint8_t vb[10];
    int vl = 0;
    while (val >= 0x80) { vb[vl++] = (uint8_t)(val | 0x80); val >>= 7; }
    vb[vl++] = (uint8_t)val;
    return cetcd_buf_append(pb, vb, vl);
}

/* ── Protobuf varint decoder ─────────────────────────────────────── */

static int pb_read_varint(const uint8_t *buf, size_t len,
                           size_t *pos, uint64_t *out) {
    uint64_t val = 0;
    int shift = 0;
    while (*pos < len) {
        uint8_t b = buf[(*pos)++];
        val |= (uint64_t)(b & 0x7F) << shift;
        if (!(b & 0x80)) { *out = val; return 0; }
        shift += 7;
        if (shift > 63) return -1;
    }
    return -1;
}

/* ── Decode etcd mvccpb.KeyValue protobuf ──────────────────────────
 *
 *  message KeyValue {
 *    bytes key             = 1;  // tag 0x0A
 *    int64 mod_revision    = 2;  // tag 0x10
 *    int64 version         = 3;  // tag 0x18
 *    bytes value           = 4;  // tag 0x22
 *    int64 create_revision = 5;  // tag 0x28
 *    int64 lease           = 6;  // tag 0x30
 *  }
 */
static int decode_etcd_kv(const uint8_t *data, size_t len, etcd_kv *kv,
                           cetcd_arena *arena) {
    memset(kv, 0, sizeof(*kv));
    size_t pos = 0;
    while (pos < len) {
        uint64_t tag_val;
        if (pb_read_varint(data, len, &pos, &tag_val) != 0) return -1;
        uint32_t field_num = (uint32_t)(tag_val >> 3);
        uint32_t wire_type = (uint32_t)(tag_val & 0x07);
        switch (field_num) {
        case 1: { /* key: bytes */
            if (wire_type != 2) return -1;
            uint64_t slen;
            if (pb_read_varint(data, len, &pos, &slen) != 0) return -1;
            if (pos + slen > len) return -1;
            kv->key_len = (size_t)slen;
            kv->key = (uint8_t*)cetcd_arena_memdup(arena, data + pos, kv->key_len);
            if (!kv->key) return -1;
            pos += kv->key_len;
            break;
        }
        case 2: { /* mod_revision: int64 */
            if (wire_type != 0) return -1;
            uint64_t v;
            if (pb_read_varint(data, len, &pos, &v) != 0) return -1;
            kv->mod_revision = (int64_t)v;
            break;
        }
        case 3: { /* version: int64 */
            if (wire_type != 0) return -1;
            uint64_t v;
            if (pb_read_varint(data, len, &pos, &v) != 0) return -1;
            kv->version = (int64_t)v;
            break;
        }
        case 4: { /* value: bytes */
            if (wire_type != 2) return -1;
            uint64_t slen;
            if (pb_read_varint(data, len, &pos, &slen) != 0) return -1;
            if (pos + slen > len) return -1;
            kv->value_len = (size_t)slen;
            kv->value = (uint8_t*)cetcd_arena_memdup(arena, data + pos, kv->value_len);
            if (!kv->value && kv->value_len > 0) return -1;
            pos += kv->value_len;
            break;
        }
        case 5: { /* create_revision: int64 */
            if (wire_type != 0) return -1;
            uint64_t v;
            if (pb_read_varint(data, len, &pos, &v) != 0) return -1;
            kv->create_revision = (int64_t)v;
            break;
        }
        case 6: { /* lease: int64 */
            if (wire_type != 0) return -1;
            uint64_t v;
            if (pb_read_varint(data, len, &pos, &v) != 0) return -1;
            kv->lease = (int64_t)v;
            break;
        }
        default: {
            /* skip unknown field */
            if (wire_type == 0) {
                uint64_t dummy;
                if (pb_read_varint(data, len, &pos, &dummy) != 0) return -1;
            } else if (wire_type == 2) {
                uint64_t slen;
                if (pb_read_varint(data, len, &pos, &slen) != 0) return -1;
                pos += (size_t)slen;
            } else {
                return -1;
            }
            break;
        }
        }
    }
    return 0;
}

/* ── bbolt: read a full page from file ───────────────────────────── */

static uint8_t *bbolt_read_page(bbolt_reader *r, uint64_t pgid) {
    long offset = (long)(pgid * r->page_size);
    if (fseek(r->fp, offset, SEEK_SET) != 0) return NULL;
    uint8_t *page = (uint8_t*)cetcd_arena_alloc(r->arena, r->page_size);
    if (!page) return NULL;
    size_t n = fread(page, 1, r->page_size, r->fp);
    if (n < BBOLT_HEADER_SIZE) return NULL;
    return page;
}

/* ── bbolt: parse page header ────────────────────────────────────── */

static void bbolt_parse_header(const uint8_t *page, uint64_t *id,
                                uint16_t *flags, uint16_t *count,
                                uint32_t *overflow) {
    if (id)       *id       = rd_u64_le(page + 0);
    if (flags)    *flags    = rd_u16_le(page + 8);
    if (count)    *count    = rd_u16_le(page + 10);
    if (overflow) *overflow = rd_u32_le(page + 12);
}

/* ── bbolt: parse meta page ──────────────────────────────────────── */

static int bbolt_parse_meta(const uint8_t *page, bbolt_reader *r) {
    uint16_t flags;
    bbolt_parse_header(page, NULL, &flags, NULL, NULL);
    if (!(flags & BBOLT_FLAG_META)) {
        CETCD_ERROR("page is not a meta page (flags=0x%04X)", flags);
        return -1;
    }
    const uint8_t *m = page + BBOLT_HEADER_SIZE;
    uint32_t magic = rd_u32_le(m + 0);
    if (magic != BBOLT_MAGIC) {
        CETCD_ERROR("invalid bbolt magic: 0x%08X (expected 0x%08X)", magic, BBOLT_MAGIC);
        return -1;
    }
    uint32_t version = rd_u32_le(m + 4);
    if (version != BBOLT_VERSION) {
        CETCD_ERROR("unsupported bbolt version: %u (expected %u)", version, BBOLT_VERSION);
        return -1;
    }
    r->page_size = rd_u32_le(m + 8);
    if (r->page_size == 0 || r->page_size > (1 << 20)) {
        CETCD_ERROR("invalid page size: %u", r->page_size);
        return -1;
    }
    /* root bucket: root_pgid at m+16, sequence at m+24 */
    r->root_pgid = rd_u64_le(m + 16);
    r->txid = rd_u64_le(m + 48);
    /* pgid high-water mark at m+40 */
    r->page_count = rd_u64_le(m + 40);
    return 0;
}

/* ── bbolt: open reader ──────────────────────────────────────────── */

static bbolt_reader *bbolt_open(const char *path) {
    FILE *fp = fopen(path, "rb");
    if (!fp) {
        CETCD_ERROR("cannot open bbolt file: %s", path);
        return NULL;
    }
    cetcd_arena *arena = cetcd_arena_new(1 << 20);  /* 1 MB blocks */
    if (!arena) { fclose(fp); return NULL; }

    bbolt_reader *r = (bbolt_reader*)calloc(1, sizeof(*r));
    if (!r) { cetcd_arena_free(arena); fclose(fp); return NULL; }
    r->fp = fp;
    r->arena = arena;
    r->page_size = BBOLT_DEFAULT_PAGESIZE;

    /* Read candidate meta pages (page 0 and page 1), pick the latest */
    uint64_t best_txid = 0;
    int best_page = -1;
    for (int i = 0; i < 2; i++) {
        uint8_t *page = bbolt_read_page(r, (uint64_t)i);
        if (!page) continue;
        uint16_t flags;
        bbolt_parse_header(page, NULL, &flags, NULL, NULL);
        if (!(flags & BBOLT_FLAG_META)) continue;
        const uint8_t *m = page + BBOLT_HEADER_SIZE;
        uint32_t magic = rd_u32_le(m);
        if (magic != BBOLT_MAGIC) continue;
        uint64_t txid = rd_u64_le(m + 48);
        if (txid > best_txid) {
            best_txid = txid;
            best_page = i;
        }
    }
    if (best_page < 0) {
        CETCD_ERROR("no valid meta page found in bbolt file");
        cetcd_arena_free(arena); free(r); fclose(fp);
        return NULL;
    }

    /* Re-read the best meta page with correct page size (meta contains it) */
    /* First pass: read with default page size to get the actual page_size */
    uint8_t *meta_page = bbolt_read_page(r, (uint64_t)best_page);
    if (!meta_page || bbolt_parse_meta(meta_page, r) != 0) {
        CETCD_ERROR("failed to parse bbolt meta page");
        cetcd_arena_free(arena); free(r); fclose(fp);
        return NULL;
    }

    CETCD_INFO("bbolt: page_size=%u root_pgid=%llu txid=%llu page_count=%llu",
               r->page_size, (unsigned long long)r->root_pgid,
               (unsigned long long)r->txid, (unsigned long long)r->page_count);
    return r;
}

static void bbolt_close(bbolt_reader *r) {
    if (!r) return;
    if (r->fp) fclose(r->fp);
    if (r->arena) cetcd_arena_free(r->arena);
    free(r);
}

/* ── bbolt: get leaf element at index ────────────────────────────── */

static int bbolt_leaf_elem_at(const uint8_t *page, uint16_t idx,
                               uint32_t *flags, const uint8_t **key,
                               size_t *key_len, const uint8_t **val,
                               size_t *val_len) {
    size_t off = BBOLT_HEADER_SIZE + (size_t)idx * BBOLT_LEAF_ELEM_SIZE;
    if (off + BBOLT_LEAF_ELEM_SIZE > 4096 * 4) return -1; /* safety */

    uint32_t eflags = rd_u32_le(page + off + 0);
    uint32_t epos   = rd_u32_le(page + off + 4);
    uint32_t eksize = rd_u32_le(page + off + 8);
    uint32_t evsize = rd_u32_le(page + off + 12);

    if (flags)   *flags   = eflags;
    if (key_len) *key_len = eksize;
    if (val_len) *val_len = evsize;

    /* key starts at: element_address + pos */
    const uint8_t *elem_start = page + off;
    if (key) *key = elem_start + epos;
    /* value starts at: element_address + pos + ksize */
    if (val) *val = elem_start + epos + eksize;
    return 0;
}

/* ── bbolt: get branch element at index ──────────────────────────── */

static int bbolt_branch_elem_at(const uint8_t *page, uint16_t idx,
                                 const uint8_t **key, size_t *key_len,
                                 uint64_t *child_pgid) {
    size_t off = BBOLT_HEADER_SIZE + (size_t)idx * BBOLT_BRANCH_ELEM_SIZE;
    uint32_t epos   = rd_u32_le(page + off + 0);
    uint32_t eksize = rd_u32_le(page + off + 4);
    uint64_t epgid  = rd_u64_le(page + off + 8);

    if (key_len) *key_len = eksize;
    if (child_pgid) *child_pgid = epgid;

    const uint8_t *elem_start = page + off;
    if (key) *key = elem_start + epos;
    return 0;
}

/* ── bbolt: iterate all key-value pairs in a bucket ──────────────── */

static int bbolt_iterate_bucket(bbolt_reader *r, uint64_t root_pgid,
                                 bbolt_kv_cb cb, void *udata) {
    uint8_t *page = bbolt_read_page(r, root_pgid);
    if (!page) {
        CETCD_ERROR("failed to read page %llu", (unsigned long long)root_pgid);
        return -1;
    }
    uint16_t flags, count;
    bbolt_parse_header(page, NULL, &flags, &count, NULL);

    if (flags & BBOLT_FLAG_LEAF) {
        for (uint16_t i = 0; i < count; i++) {
            uint32_t eflags;
            const uint8_t *key, *val;
            size_t klen, vlen;
            if (bbolt_leaf_elem_at(page, i, &eflags, &key, &klen, &val, &vlen) != 0) {
                CETCD_WARN("failed to parse leaf element %u on page %llu",
                           i, (unsigned long long)root_pgid);
                continue;
            }
            cb(key, klen, val, vlen, udata);
        }
    } else if (flags & BBOLT_FLAG_BRANCH) {
        for (uint16_t i = 0; i < count; i++) {
            const uint8_t *bkey;
            size_t bklen;
            uint64_t child_pgid;
            if (bbolt_branch_elem_at(page, i, &bkey, &bklen, &child_pgid) != 0) {
                CETCD_WARN("failed to parse branch element %u on page %llu",
                           i, (unsigned long long)root_pgid);
                continue;
            }
            if (bbolt_iterate_bucket(r, child_pgid, cb, udata) != 0)
                return -1;
        }
    } else {
        CETCD_ERROR("unexpected page flags 0x%04X on page %llu",
                     flags, (unsigned long long)root_pgid);
        return -1;
    }
    return 0;
}

/* ── bbolt: find sub-bucket by name ──────────────────────────────── */

static int bbolt_find_bucket(bbolt_reader *r, uint64_t root_pgid,
                              const char *name, uint64_t *out_pgid) {
    uint8_t *page = bbolt_read_page(r, root_pgid);
    if (!page) return -1;

    uint16_t flags, count;
    bbolt_parse_header(page, NULL, &flags, &count, NULL);
    size_t name_len = strlen(name);

    if (flags & BBOLT_FLAG_LEAF) {
        for (uint16_t i = 0; i < count; i++) {
            uint32_t eflags;
            const uint8_t *key, *val;
            size_t klen, vlen;
            if (bbolt_leaf_elem_at(page, i, &eflags, &key, &klen, &val, &vlen) != 0)
                continue;
            if ((eflags & BBOLT_FLAG_BUCKET) && klen == name_len &&
                memcmp(key, name, name_len) == 0) {
                /* val points to bucket header: root_pgid(8)+sequence(8) */
                if (vlen < BBOLT_BUCKET_VAL_SIZE) return -1;
                *out_pgid = rd_u64_le(val);
                return 0;
            }
        }
    } else if (flags & BBOLT_FLAG_BRANCH) {
        for (uint16_t i = 0; i < count; i++) {
            const uint8_t *bkey;
            size_t bklen;
            uint64_t child_pgid;
            if (bbolt_branch_elem_at(page, i, &bkey, &bklen, &child_pgid) != 0)
                continue;
            /* Branch keys are separator keys. We need to search all children. */
            if (bbolt_find_bucket(r, child_pgid, name, out_pgid) == 0)
                return 0;
        }
    }
    return -1;
}

/* ── bbolt: collect KV callback context ──────────────────────────── */

typedef struct {
    etcd_kv   *entries;
    size_t     count;
    size_t     cap;
    size_t     failed;
    cetcd_arena *arena;
    int        verbose;
} collect_ctx;

static void collect_kv(const uint8_t *key, size_t key_len,
                        const uint8_t *val, size_t val_len,
                        void *udata) {
    collect_ctx *ctx = (collect_ctx*)udata;

    /* Skip bucket entries (they are sub-bucket references, not data) */
    /* We detect this by checking if the leaf element had the bucket flag.
       Since our callback doesn't carry the flag, we heuristically skip
       entries whose values look like bucket headers (16 bytes of pgid+seq)
       and whose key is a well-known bucket name. */
    /* Actually, we only iterate inside the "key" bucket, so all entries
       here are revision→KeyValue pairs. No bucket entries exist here. */

    etcd_kv kv;
    memset(&kv, 0, sizeof(kv));
    if (decode_etcd_kv(val, val_len, &kv, ctx->arena) != 0) {
        CETCD_WARN("failed to decode KeyValue protobuf (rev_key_len=%zu)", key_len);
        ctx->failed++;
        return;
    }

    if (ctx->count == ctx->cap) {
        size_t nc = ctx->cap ? ctx->cap * 2 : 256;
        etcd_kv *tmp = (etcd_kv*)realloc(ctx->entries, nc * sizeof(etcd_kv));
        if (!tmp) { ctx->failed++; return; }
        ctx->entries = tmp; ctx->cap = nc;
    }
    ctx->entries[ctx->count++] = kv;
}

/* ── Snapshot reader: find latest snapshot ───────────────────────── */

static int find_latest_snapshot(const char *snap_dir, int64_t *term,
                                 int64_t *index) {
    dir_list dl;
    if (dir_list_files(snap_dir, &dl) != 0) {
        CETCD_WARN("cannot list snapshot directory: %s", snap_dir);
        return -1;
    }
    int64_t best_term = 0, best_index = 0;
    for (size_t i = 0; i < dl.count; i++) {
        const char *name = dl.names[i];
        size_t nlen = strlen(name);
        /* Look for <term>-<index>.snap */
        if (nlen < 6 || strcmp(name + nlen - 5, ".snap") != 0) continue;
        char *dash = strchr(name, '-');
        if (!dash) continue;
        int64_t t = atoll(name);
        int64_t idx = atoll(dash + 1);
        if (idx > best_index) {
            best_index = idx;
            best_term = t;
        }
    }
    dir_list_free(&dl);
    if (best_index > 0) {
        *term = best_term;
        *index = best_index;
        CETCD_INFO("latest snapshot: term=%lld index=%lld",
                   (long long)best_term, (long long)best_index);
        return 0;
    }
    CETCD_INFO("no snapshots found in %s", snap_dir);
    return -1;
}

/* ── WAL reader: find latest WAL file and extract term/index ───────
 *
 * etcd WAL format (simplified):
 *   64KB-preallocated segments, each record:
 *     type(1B) + crc32(4B) + data_len(varint) + data
 *
 * We scan for CETCD_WAL_ENTRY (type=2) records and extract the
 * term+index from the last one found.
 */

typedef struct {
    uint64_t term;
    uint64_t index;
} wal_entry_info;

static int read_wal_varint(FILE *fp, uint64_t *out) {
    uint64_t val = 0;
    int shift = 0;
    while (true) {
        uint8_t b;
        if (fread(&b, 1, 1, fp) != 1) return -1;
        val |= (uint64_t)(b & 0x7F) << shift;
        if (!(b & 0x80)) { *out = val; return 0; }
        shift += 7;
        if (shift > 63) return -1;
    }
}

static int scan_wal_file(const char *path, wal_entry_info *last_entry) {
    FILE *fp = fopen(path, "rb");
    if (!fp) return -1;

    /* Skip potential header/padding - try to find records */
    while (true) {
        uint8_t type;
        uint8_t crc_buf[4];
        if (fread(&type, 1, 1, fp) != 1) break;
        if (type == 0) {
            /* Padding byte, skip */
            continue;
        }
        if (fread(crc_buf, 1, 4, fp) != 4) break;
        uint64_t data_len;
        if (read_wal_varint(fp, &data_len) != 0) break;
        if (data_len > (1 << 20)) {
            /* Skip implausibly large record */
            CETCD_DEBUG("WAL: skipping large record (len=%llu)", (unsigned long long)data_len);
            if (fseek(fp, (long)data_len, SEEK_CUR) != 0) break;
            continue;
        }
        if (type == 2) { /* CETCD_WAL_ENTRY */
            uint8_t *data = (uint8_t*)malloc((size_t)data_len);
            if (!data) break;
            if (fread(data, 1, (size_t)data_len, fp) != (size_t)data_len) {
                free(data); break;
            }
            /* Try to decode entry protobuf:
             * etcd Entry: term(field 2) + index(field 3) + type(field 1) + data(field 6)
             * We extract term and index using varint parsing */
            uint64_t entry_term = 0, entry_index = 0;
            size_t pos = 0;
            while (pos < data_len) {
                uint64_t tag;
                if (pb_read_varint(data, data_len, &pos, &tag) != 0) break;
                uint32_t fnum = (uint32_t)(tag >> 3);
                uint32_t wtype = (uint32_t)(tag & 0x07);
                if (wtype == 0) {
                    uint64_t v;
                    if (pb_read_varint(data, data_len, &pos, &v) != 0) break;
                    if (fnum == 2) entry_term = v;
                    else if (fnum == 3) entry_index = v;
                } else if (wtype == 2) {
                    uint64_t slen;
                    if (pb_read_varint(data, data_len, &pos, &slen) != 0) break;
                    pos += (size_t)slen;
                } else {
                    break;
                }
            }
            last_entry->term = entry_term;
            last_entry->index = entry_index;
            free(data);
        } else {
            if (fseek(fp, (long)data_len, SEEK_CUR) != 0) break;
        }
    }
    fclose(fp);
    return 0;
}

static int find_latest_wal(const char *wal_dir, wal_entry_info *info) {
    dir_list dl;
    if (dir_list_files(wal_dir, &dl) != 0) {
        CETCD_WARN("cannot list WAL directory: %s", wal_dir);
        return -1;
    }
    memset(info, 0, sizeof(*info));
    int found = 0;
    for (size_t i = 0; i < dl.count; i++) {
        size_t nlen = strlen(dl.names[i]);
        if (nlen < 5 || strcmp(dl.names[i] + nlen - 4, ".wal") != 0) continue;
        char fullpath[1024];
        path_join(fullpath, sizeof(fullpath), wal_dir, dl.names[i]);
        wal_entry_info entry = {0, 0};
        if (scan_wal_file(fullpath, &entry) == 0 && entry.index > 0) {
            if (entry.index > info->index) {
                *info = entry;
                found = 1;
            }
        }
    }
    dir_list_free(&dl);
    if (found) {
        CETCD_INFO("WAL: latest entry term=%llu index=%llu",
                   (unsigned long long)info->term, (unsigned long long)info->index);
    } else {
        CETCD_INFO("no WAL entries found in %s", wal_dir);
    }
    return found ? 0 : -1;
}

/* ── LMDB writer: write migration data using cetcd backend API ──── */

static int write_kv_to_lmdb(cetcd_backend *be, const etcd_kv *entries,
                             size_t count, migrate_stats *stats, int verbose) {
    stats->total_keys = 0;
    stats->latest_rev_main = 0;
    stats->failed_entries = 0;

    /* We write in batches using transactions for efficiency.
     * Each batch commits after CETCD_MIGRATE_BATCH_SIZE entries. */
    const size_t batch_size = 500;
    cetcd_txn *txn = NULL;
    size_t batch_count = 0;

    for (size_t i = 0; i < count; i++) {
        const etcd_kv *kv = &entries[i];

        /* Skip tombstones (empty value with version 0) */
        if (kv->value == NULL && kv->value_len == 0 && kv->version == 0) {
            stats->skipped_entries++;
            continue;
        }

        if (!txn) {
            txn = cetcd_txn_begin(be, false);
            if (!txn) {
                CETCD_ERROR("failed to begin LMDB transaction");
                stats->failed_entries++;
                continue;
            }
        }

        /* Write to "key" bucket: revision_bytes → raw protobuf value
         * The revision key is 16 bytes: main(8 BE) + sub(8 BE).
         * We reconstruct it from mod_revision and assume sub=0. */
        uint8_t rev_key[16];
        memset(rev_key, 0, sizeof(rev_key));
        /* Big-endian encode the main revision */
        int64_t main_rev = kv->mod_revision;
        for (int b = 0; b < 8; b++)
            rev_key[7 - b] = (uint8_t)((main_rev >> (b * 8)) & 0xFF);

        /* Re-encode the KeyValue as protobuf for the "rev" bucket */
        cetcd_buf pb;
        cetcd_buf_init(&pb);
        /* field 1 (key): tag 0x0A + varint len + bytes */
        cetcd_buf_append_byte(&pb, 0x0A);
        encode_varint_buf(&pb, (uint64_t)kv->key_len);
        cetcd_buf_append(&pb, kv->key, kv->key_len);
        /* field 2 (mod_revision): tag 0x10 + varint */
        cetcd_buf_append_byte(&pb, 0x10);
        encode_varint_buf(&pb, (uint64_t)kv->mod_revision);
        /* field 3 (version): tag 0x18 + varint */
        cetcd_buf_append_byte(&pb, 0x18);
        encode_varint_buf(&pb, (uint64_t)kv->version);
        /* field 4 (value): tag 0x22 + varint len + bytes */
        cetcd_buf_append_byte(&pb, 0x22);
        encode_varint_buf(&pb, (uint64_t)kv->value_len);
        if (kv->value && kv->value_len > 0)
            cetcd_buf_append(&pb, kv->value, kv->value_len);
        /* field 5 (create_revision): tag 0x28 + varint */
        cetcd_buf_append_byte(&pb, 0x28);
        encode_varint_buf(&pb, (uint64_t)kv->create_revision);
        /* field 6 (lease): tag 0x30 + varint */
        if (kv->lease != 0) {
            cetcd_buf_append_byte(&pb, 0x30);
            encode_varint_buf(&pb, (uint64_t)kv->lease);
        }

        /* Write to "rev" bucket: revision → protobuf KeyValue */
        int rc = cetcd_txn_put(txn, "rev", rev_key, 16,
                                pb.data, pb.len);
        if (rc != CETCD_OK) {
            CETCD_WARN("failed to write rev entry (main=%lld): %d",
                       (long long)main_rev, rc);
            stats->failed_entries++;
            cetcd_buf_free(&pb);
            continue;
        }

        /* Write to "key" bucket: user_key → revision bytes */
        rc = cetcd_txn_put(txn, "key", kv->key, kv->key_len,
                            rev_key, 16);
        if (rc != CETCD_OK) {
            CETCD_WARN("failed to write key index for key_len=%zu", kv->key_len);
            stats->failed_entries++;
        }

        cetcd_buf_free(&pb);
        stats->total_keys++;
        if (main_rev > stats->latest_rev_main)
            stats->latest_rev_main = main_rev;
        batch_count++;

        if (batch_count >= batch_size) {
            rc = cetcd_txn_commit(txn);
            txn = NULL;
            batch_count = 0;
            if (rc != CETCD_OK) {
                CETCD_ERROR("failed to commit batch at entry %zu", i);
                stats->failed_entries += batch_size;
            } else if (verbose) {
                printf("  migrated %zu keys...\n", stats->total_keys);
            }
        }
    }

    /* Commit remaining batch */
    if (txn) {
        int rc = cetcd_txn_commit(txn);
        if (rc != CETCD_OK) {
            CETCD_ERROR("failed to commit final batch");
            stats->failed_entries += batch_count;
        }
    }
    return 0;
}

/* ── Print usage ─────────────────────────────────────────────────── */

static void print_usage(const char *prog) {
    printf("cetcd-migrate — migrate etcd data to cetcd (LMDB)\n");
    printf("Usage: %s [options]\n", prog);
    printf("Options:\n");
    printf("  --data-dir DIR     etcd data directory (required)\n");
    printf("  --output-dir DIR   cetcd output directory (required)\n");
    printf("  --verbose          Enable verbose output\n");
    printf("  --help             Show this help\n");
}

/* ── Main ─────────────────────────────────────────────────────────── */

int main(int argc, char **argv) {
    const char *data_dir = NULL;
    const char *output_dir = NULL;
    int verbose = 0;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--data-dir") == 0 && i + 1 < argc) {
            data_dir = argv[++i];
        } else if (strcmp(argv[i], "--output-dir") == 0 && i + 1 < argc) {
            output_dir = argv[++i];
        } else if (strcmp(argv[i], "--verbose") == 0) {
            verbose = 1;
            cetcd_log_set_level(CETCD_LOG_DEBUG);
        } else if (strcmp(argv[i], "--help") == 0) {
            print_usage(argv[0]);
            return 0;
        } else {
            fprintf(stderr, "Unknown option: %s\n", argv[i]);
            print_usage(argv[0]);
            return 1;
        }
    }

    if (!data_dir || !output_dir) {
        fprintf(stderr, "Error: --data-dir and --output-dir are required\n\n");
        print_usage(argv[0]);
        return 1;
    }

    cetcd_log_set_level(verbose ? CETCD_LOG_DEBUG : CETCD_LOG_INFO);

    CETCD_INFO("cetcd-migrate starting");
    CETCD_INFO("  data-dir   : %s", data_dir);
    CETCD_INFO("  output-dir : %s", output_dir);

    migrate_stats stats;
    memset(&stats, 0, sizeof(stats));

    /* ── Step 1: Read bbolt database ──────────────────────────────── */

    char db_path[1024];
    path_join(db_path, sizeof(db_path), data_dir, "member/snap/db");

    if (!file_exists(db_path)) {
        CETCD_FATAL("bbolt database not found: %s", db_path);
        return 1;
    }

    CETCD_INFO("reading bbolt database: %s", db_path);
    bbolt_reader *bb = bbolt_open(db_path);
    if (!bb) {
        CETCD_FATAL("failed to open bbolt database");
        return 1;
    }

    /* Find the "key" bucket in the root bucket */
    uint64_t key_bucket_pgid;
    if (bbolt_find_bucket(bb, bb->root_pgid, "key", &key_bucket_pgid) != 0) {
        CETCD_ERROR("'key' bucket not found in bbolt database");
        bbolt_close(bb);
        return 1;
    }
    CETCD_INFO("'key' bucket found at page %llu", (unsigned long long)key_bucket_pgid);

    /* Collect all KV entries */
    collect_ctx ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.arena = bb->arena;
    ctx.verbose = verbose;

    if (bbolt_iterate_bucket(bb, key_bucket_pgid, collect_kv, &ctx) != 0) {
        CETCD_ERROR("failed to iterate 'key' bucket");
        bbolt_close(bb);
        return 1;
    }

    CETCD_INFO("collected %zu entries from bbolt (%zu failed to decode)",
               ctx.count, ctx.failed);
    /* NOTE: Do NOT close bbolt reader yet — the arena owns the key/value
     * memory referenced by ctx.entries.  Close after LMDB write completes. */

    /* ── Step 2: Read snapshot info (optional) ────────────────────── */

    char snap_dir[1024];
    path_join(snap_dir, sizeof(snap_dir), data_dir, "member/snap");
    find_latest_snapshot(snap_dir, &stats.snap_term, &stats.snap_index);

    /* ── Step 3: Read WAL info (optional) ─────────────────────────── */

    char wal_dir[1024];
    path_join(wal_dir, sizeof(wal_dir), data_dir, "member/wal");
    wal_entry_info wal_info;
    if (find_latest_wal(wal_dir, &wal_info) == 0) {
        CETCD_INFO("WAL latest: term=%llu index=%llu",
                   (unsigned long long)wal_info.term,
                   (unsigned long long)wal_info.index);
    }

    /* ── Step 4: Create LMDB output and write data ────────────────── */

    if (mkdir_p(output_dir) != 0) {
        /* directory might already exist, that's ok */
        CETCD_DEBUG("output directory creation returned non-zero (may already exist)");
    }

    cetcd_backend_config be_cfg;
    memset(&be_cfg, 0, sizeof(be_cfg));
    be_cfg.path = output_dir;
    be_cfg.map_size = 256 * 1024 * 1024;  /* 256 MB */
    be_cfg.max_dbs = 16;

    cetcd_backend *be = cetcd_backend_open(&be_cfg);
    if (!be) {
        CETCD_FATAL("failed to open LMDB backend at %s", output_dir);
        free(ctx.entries);
        return 1;
    }

    CETCD_INFO("writing %zu entries to LMDB...", ctx.count);
    write_kv_to_lmdb(be, ctx.entries, ctx.count, &stats, verbose);

    stats.db_size = cetcd_backend_size(be);
    cetcd_backend_close(be);

    /* Now safe to close bbolt reader — arena memory no longer needed */
    bbolt_close(bb);
    free(ctx.entries);

    /* ── Step 5: Print verification summary ───────────────────────── */

    printf("\n");
    printf("=== Migration Summary ===\n");
    printf("  Total keys migrated : %zu\n", stats.total_keys);
    printf("  Latest revision     : %lld\n", (long long)stats.latest_rev_main);
    printf("  Failed entries      : %zu\n", stats.failed_entries);
    printf("  Skipped entries     : %zu\n", stats.skipped_entries);
    printf("  DB size             : %llu bytes (%.2f MB)\n",
           (unsigned long long)stats.db_size,
           (double)stats.db_size / (1024.0 * 1024.0));
    if (stats.snap_index > 0) {
        printf("  Snapshot term/index : %lld / %lld\n",
               (long long)stats.snap_term, (long long)stats.snap_index);
    }
    if (stats.failed_entries > 0) {
        printf("\n  WARNING: %zu entries failed to migrate!\n", stats.failed_entries);
    }
    printf("========================\n");

    return stats.failed_entries > 0 ? 1 : 0;
}
