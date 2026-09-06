#include "cetcd/server.h"

#include <errno.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

int cetcd_parse_auto_compaction_mode(const char *s, cetcd_auto_compact_mode *out) {
    if (!s || !s[0] || !out) return CETCD_ERR_INVAL;
    if (strcmp(s, "periodic") == 0) {
        *out = CETCD_AUTO_COMPACT_PERIODIC;
        return CETCD_OK;
    }
    if (strcmp(s, "revision") == 0) {
        *out = CETCD_AUTO_COMPACT_REVISION;
        return CETCD_OK;
    }
    return CETCD_ERR_INVAL;
}

static int parse_go_duration_sec_(const char *s, uint64_t *out) {
    if (!s || !s[0] || !out) return -1;
    uint64_t total_ns = 0;
    const char *p = s;
    int any = 0;
    while (*p) {
        if (*p < '0' || *p > '9') return -1;
        uint64_t n = 0;
        while (*p >= '0' && *p <= '9') {
            if (n > (UINT64_MAX / 10)) return -1;
            n = n * 10 + (uint64_t)(*p - '0');
            p++;
        }
        uint64_t mul;
        if (p[0] == 'n' && p[1] == 's') { mul = 1; p += 2; }
        else if (p[0] == 'u' && p[1] == 's') { mul = 1000ULL; p += 2; }
        else if (p[0] == 'm' && p[1] == 's') { mul = 1000000ULL; p += 2; }
        else if (*p == 's') { mul = 1000000000ULL; p++; }
        else if (*p == 'm') { mul = 60ULL * 1000000000ULL; p++; }
        else if (*p == 'h') { mul = 3600ULL * 1000000000ULL; p++; }
        else return -1;
        if (mul && n > UINT64_MAX / mul) return -1;
        uint64_t add = n * mul;
        if (total_ns > UINT64_MAX - add) return -1;
        total_ns += add;
        any = 1;
    }
    if (!any) return -1;
    if (total_ns == 0) {
        *out = 0;
        return 0;
    }
    uint64_t sec = total_ns / 1000000000ULL;
    if (total_ns % 1000000000ULL) sec++; /* sub-second → at least 1s */
    *out = sec;
    return 0;
}

int cetcd_parse_auto_compaction_retention(const char *s,
                                          cetcd_auto_compact_mode mode,
                                          uint64_t *out) {
    if (!s || !s[0] || !out) return CETCD_ERR_INVAL;
    if (mode != CETCD_AUTO_COMPACT_PERIODIC &&
        mode != CETCD_AUTO_COMPACT_REVISION)
        return CETCD_ERR_INVAL;

    int all_digits = 1;
    for (const char *p = s; *p; p++) {
        if (*p < '0' || *p > '9') { all_digits = 0; break; }
    }
    if (all_digits) {
        errno = 0;
        char *end = NULL;
        unsigned long long v = strtoull(s, &end, 10);
        if (errno == ERANGE || !end || end == s || *end)
            return CETCD_ERR_INVAL;
        if (mode == CETCD_AUTO_COMPACT_PERIODIC) {
            if (v == 0) {
                *out = 0;
                return CETCD_OK;
            }
            if (v > UINT64_MAX / 3600ULL) return CETCD_ERR_RANGE;
            *out = (uint64_t)v * 3600ULL;
            return CETCD_OK;
        }
        *out = (uint64_t)v;
        return CETCD_OK;
    }
    if (mode == CETCD_AUTO_COMPACT_REVISION)
        return CETCD_ERR_INVAL;
    uint64_t sec = 0;
    if (parse_go_duration_sec_(s, &sec) != 0)
        return CETCD_ERR_INVAL;
    *out = sec;
    return CETCD_OK;
}

int64_t cetcd_auto_compact_due(cetcd_auto_compact_state *st,
                               int64_t current_rev, int64_t compacted_rev,
                               uint64_t now_ms) {
    if (!st || st->mode == CETCD_AUTO_COMPACT_OFF || st->retention == 0)
        return 0;
    if (current_rev <= 0) return 0;

    if (st->mode == CETCD_AUTO_COMPACT_REVISION) {
        if (current_rev <= (int64_t)st->retention) return 0;
        int64_t t = current_rev - (int64_t)st->retention;
        if (t <= compacted_rev) return 0;
        return t;
    }

    if (st->mode != CETCD_AUTO_COMPACT_PERIODIC) return 0;
    uint64_t interval_ms = st->retention;
    if (interval_ms > UINT64_MAX / 1000ULL) return 0;
    interval_ms *= 1000ULL;
    uint64_t start = now_ms ? now_ms : 1;
    if (st->window_start_ms == 0) {
        st->window_start_ms = start;
        st->window_rev = current_rev;
        return 0;
    }
    if (now_ms < st->window_start_ms + interval_ms) return 0;
    int64_t t = st->window_rev;
    st->window_start_ms = start;
    st->window_rev = current_rev;
    if (t <= 0 || t <= compacted_rev || t > current_rev) return 0;
    return t;
}
