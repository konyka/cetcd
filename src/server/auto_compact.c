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

static int parse_go_duration_ns_(const char *s, uint64_t *out) {
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
    *out = total_ns;
    return 0;
}

static int parse_go_duration_sec_(const char *s, uint64_t *out) {
    uint64_t ns = 0;
    if (parse_go_duration_ns_(s, &ns) != 0) return -1;
    if (ns == 0) {
        *out = 0;
        return 0;
    }
    uint64_t sec = ns / 1000000000ULL;
    if (ns % 1000000000ULL) sec++; /* sub-second → at least 1s */
    *out = sec;
    return 0;
}

int cetcd_parse_go_duration_sec(const char *s, uint64_t *out) {
    if (!s || !s[0] || !out) return CETCD_ERR_INVAL;
    if (s[0] == '0' && s[1] == '\0') {
        *out = 0;
        return CETCD_OK;
    }
    uint64_t sec = 0;
    if (parse_go_duration_sec_(s, &sec) != 0)
        return CETCD_ERR_INVAL;
    *out = sec;
    return CETCD_OK;
}

int cetcd_parse_go_duration_ms(const char *s, uint64_t *out) {
    if (!s || !s[0] || !out) return CETCD_ERR_INVAL;
    if (s[0] == '0' && s[1] == '\0') {
        *out = 0;
        return CETCD_OK;
    }
    uint64_t ns = 0;
    if (parse_go_duration_ns_(s, &ns) != 0)
        return CETCD_ERR_INVAL;
    if (ns == 0) {
        *out = 0;
        return CETCD_OK;
    }
    uint64_t ms = ns / 1000000ULL;
    if (ns % 1000000ULL) ms++;
    *out = ms;
    return CETCD_OK;
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

int cetcd_parse_compaction_batch_limit(const char *s, uint64_t *out) {
    if (!s || !s[0] || !out) return CETCD_ERR_INVAL;
    if (s[0] < '0' || s[0] > '9') return CETCD_ERR_INVAL;
    errno = 0;
    char *end = NULL;
    unsigned long long v = strtoull(s, &end, 10);
    if (errno == ERANGE || !end || end == s || *end)
        return CETCD_ERR_INVAL;
    *out = (uint64_t)v;
    return CETCD_OK;
}

int cetcd_parse_max_learners(const char *s, uint32_t *out) {
    if (!out) return CETCD_ERR_INVAL;
    uint64_t v = 0;
    int rc = cetcd_parse_compaction_batch_limit(s, &v);
    if (rc != CETCD_OK) return rc;
    if (v > (uint64_t)UINT32_MAX) return CETCD_ERR_INVAL;
    *out = (uint32_t)v;
    return CETCD_OK;
}

int64_t cetcd_auto_compact_clamp(int64_t target, int64_t compacted_rev,
                                 uint64_t batch_limit) {
    if (target <= 0) return 0;
    if (batch_limit == 0) return target;
    if (compacted_rev < 0) compacted_rev = 0;
    if (batch_limit > (uint64_t)INT64_MAX) return target;
    if (compacted_rev > INT64_MAX - (int64_t)batch_limit) return target;
    int64_t cap = compacted_rev + (int64_t)batch_limit;
    return cap < target ? cap : target;
}

int cetcd_auto_compact_sleep_ready(uint64_t last_compact_ms, uint64_t now_ms,
                                   uint64_t sleep_ms) {
    if (sleep_ms == 0 || last_compact_ms == 0) return 1;
    if (now_ms < last_compact_ms) return 1;
    return (now_ms - last_compact_ms) >= sleep_ms;
}

int64_t cetcd_auto_compact_next(cetcd_auto_compact_state *st,
                                int64_t current_rev, int64_t compacted_rev,
                                uint64_t now_ms) {
    if (!st) return 0;
    int64_t due = cetcd_auto_compact_due(st, current_rev, compacted_rev, now_ms);
    if (due > 0) st->pending = due;
    if (st->pending <= compacted_rev) {
        st->pending = 0;
        return 0;
    }
    if (!cetcd_auto_compact_sleep_ready(st->last_compact_ms, now_ms,
                                        st->sleep_interval_ms))
        return 0;
    int64_t step = cetcd_auto_compact_clamp(st->pending, compacted_rev,
                                            st->batch_limit);
    if (step > 0)
        st->last_compact_ms = now_ms ? now_ms : 1;
    return step;
}
