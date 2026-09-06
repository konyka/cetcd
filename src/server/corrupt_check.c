#include "cetcd/server.h"

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#if defined(_WIN32)
#  include <io.h>
#  define cetcd_unlink(p) _unlink(p)
#else
#  include <unistd.h>
#  define cetcd_unlink(p) unlink(p)
#endif

int cetcd_parse_bool_flag(const char *s, int *out) {
    if (!s || !s[0] || !out) return CETCD_ERR_INVAL;
    if (strcmp(s, "true") == 0 || strcmp(s, "1") == 0) {
        *out = 1;
        return CETCD_OK;
    }
    if (strcmp(s, "false") == 0 || strcmp(s, "0") == 0) {
        *out = 0;
        return CETCD_OK;
    }
    return CETCD_ERR_INVAL;
}

int cetcd_backend_hash_store(const char *path, int64_t rev, uint32_t hash) {
    if (!path || !path[0] || rev < 0) return CETCD_ERR_INVAL;
    char tmp[640];
    int n = snprintf(tmp, sizeof(tmp), "%s.tmp", path);
    if (n <= 0 || (size_t)n >= sizeof(tmp)) return CETCD_ERR_INVAL;
    FILE *f = fopen(tmp, "wb");
    if (!f) return CETCD_ERR_IO;
    if (fprintf(f, "%lld %u\n", (long long)rev, (unsigned)hash) < 0 ||
        fflush(f) != 0) {
        fclose(f);
        cetcd_unlink(tmp);
        return CETCD_ERR_IO;
    }
    if (fclose(f) != 0) {
        cetcd_unlink(tmp);
        return CETCD_ERR_IO;
    }
#if defined(_WIN32)
    cetcd_unlink(path);
#endif
    if (rename(tmp, path) != 0) {
        cetcd_unlink(tmp);
        return CETCD_ERR_IO;
    }
    return CETCD_OK;
}

int cetcd_backend_hash_load(const char *path, int64_t *rev, uint32_t *hash) {
    if (!path || !path[0] || !rev || !hash) return CETCD_ERR_INVAL;
    FILE *f = fopen(path, "rb");
    if (!f) return CETCD_ERR_NOTFOUND;
    char buf[128];
    size_t n = fread(buf, 1, sizeof(buf) - 1, f);
    int ferr = ferror(f);
    fclose(f);
    if (ferr || n == 0) return CETCD_ERR_CORRUPT;
    buf[n] = '\0';

    char *end = NULL;
    errno = 0;
    unsigned long long r = strtoull(buf, &end, 10);
    if (errno == ERANGE || !end || end == buf || *end != ' ' || r > (unsigned long long)INT64_MAX)
        return CETCD_ERR_CORRUPT;
    const char *p = end + 1;
    errno = 0;
    unsigned long long h = strtoull(p, &end, 10);
    if (errno == ERANGE || !end || end == p || h > 0xFFFFFFFFULL)
        return CETCD_ERR_CORRUPT;
    if (*end == '\r') end++;
    if (*end == '\n') end++;
    if (*end) return CETCD_ERR_CORRUPT;
    *rev = (int64_t)r;
    *hash = (uint32_t)h;
    return CETCD_OK;
}

int cetcd_backend_hash_verify(const char *path, int64_t rev, uint32_t hash) {
    if (!path || !path[0] || rev < 0) return CETCD_ERR_INVAL;
    int64_t stored_rev = 0;
    uint32_t stored_hash = 0;
    int rc = cetcd_backend_hash_load(path, &stored_rev, &stored_hash);
    if (rc == CETCD_ERR_NOTFOUND)
        return cetcd_backend_hash_store(path, rev, hash);
    if (rc != CETCD_OK) return rc;
    if (rev < stored_rev) return CETCD_ERR_CORRUPT;
    if (rev == stored_rev && hash != stored_hash) return CETCD_ERR_CORRUPT;
    return cetcd_backend_hash_store(path, rev, hash);
}
