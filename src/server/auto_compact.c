#include "cetcd/server.h"

#include <errno.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
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

int cetcd_parse_bootstrap_defrag_mb(const char *s, uint64_t *out) {
    uint64_t v = 0;
    int rc = cetcd_parse_compaction_batch_limit(s, &v);
    if (rc != CETCD_OK) return rc;
    if (v > UINT64_MAX / (1024ULL * 1024ULL)) return CETCD_ERR_INVAL;
    if (out) *out = v;
    else return CETCD_ERR_INVAL;
    return CETCD_OK;
}

int cetcd_backend_should_defrag(uint64_t alloc_bytes, uint64_t threshold_mb) {
    if (threshold_mb == 0) return 0;
    if (threshold_mb > UINT64_MAX / (1024ULL * 1024ULL)) return 1;
    return alloc_bytes > threshold_mb * 1024ULL * 1024ULL;
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

int cetcd_parse_auth_token_ttl(const char *s, uint64_t *out) {
    if (!out) return CETCD_ERR_INVAL;
    uint64_t v = 0;
    int rc = cetcd_parse_compaction_batch_limit(s, &v);
    if (rc != CETCD_OK) return rc;
    if (v == 0) return CETCD_ERR_INVAL;
    if (v > UINT64_MAX / 1000000000ULL) return CETCD_ERR_INVAL;
    *out = v;
    return CETCD_OK;
}

int cetcd_server_want_pre_vote(int set, int enabled) {
    return set ? (enabled != 0) : 1;
}

int cetcd_server_want_tick_advance(int set, int enabled) {
    return set ? (enabled != 0) : 1;
}

int cetcd_server_want_wait_cluster_ready(int set, int enabled) {
    return set ? (enabled != 0) : 0;
}

int cetcd_server_want_enable_pprof(int set, int enabled) {
    return set ? (enabled != 0) : 0;
}

int cetcd_server_metrics_route(const char *path, size_t path_len, int enable_pprof) {
    if (!path || path_len == 0) return 2;
    if (path_len == 8 && memcmp(path, "/metrics", 8) == 0) return 1;
    if (path_len == 7 && memcmp(path, "/health", 7) == 0) return 6;
    if (path_len >= 8 && memcmp(path, "/health?", 8) == 0) return 6;
    if (path_len >= 20 && memcmp(path, "/debug/pprof/profile", 20) == 0)
        return enable_pprof ? 3 : 2;
    if (path_len == 18 && memcmp(path, "/debug/pprof/heap", 18) == 0)
        return enable_pprof ? 4 : 2;
    if (path_len == 24 && memcmp(path, "/debug/pprof/coroutines", 24) == 0)
        return enable_pprof ? 5 : 2;
    return 2;
}

int cetcd_server_health_ok(int has_leader, int nospace, int corrupt,
                           int serializable, int exclude_nospace, int exclude_corrupt,
                           char *reason, size_t reason_cap) {
    if (reason && reason_cap) reason[0] = '\0';
    if (nospace && !exclude_nospace) {
        if (reason && reason_cap >= 8) snprintf(reason, reason_cap, "NOSPACE");
        return 0;
    }
    if (corrupt && !exclude_corrupt) {
        if (reason && reason_cap >= 8) snprintf(reason, reason_cap, "CORRUPT");
        return 0;
    }
    if (!serializable && !has_leader) {
        if (reason && reason_cap >= 15)
            snprintf(reason, reason_cap, "RAFT NO LEADER");
        return 0;
    }
    return 1;
}

int cetcd_parse_health_query(const char *qs, int *serializable,
                             int *exclude_nospace, int *exclude_corrupt) {
    if (!serializable || !exclude_nospace || !exclude_corrupt)
        return CETCD_ERR_INVAL;
    *serializable = 0;
    *exclude_nospace = 0;
    *exclude_corrupt = 0;
    if (!qs || !qs[0]) return CETCD_OK;
    const char *p = qs;
    while (*p) {
        const char *amp = strchr(p, '&');
        size_t n = amp ? (size_t)(amp - p) : strlen(p);
        if (n >= 13 && memcmp(p, "serializable=", 13) == 0) {
            const char *v = p + 13;
            size_t vn = n - 13;
            if ((vn == 4 && memcmp(v, "true", 4) == 0) ||
                (vn == 1 && v[0] == '1'))
                *serializable = 1;
        } else if (n == 15 && memcmp(p, "exclude=NOSPACE", 15) == 0) {
            *exclude_nospace = 1;
        } else if (n == 15 && memcmp(p, "exclude=CORRUPT", 15) == 0) {
            *exclude_corrupt = 1;
        }
        if (!amp) break;
        p = amp + 1;
    }
    return CETCD_OK;
}

int cetcd_server_health_json(int ok, const char *reason, char *out, size_t cap) {
    if (!out || cap < 18) return CETCD_ERR_INVAL;
    int n;
    if (ok) {
        n = snprintf(out, cap, "{\"health\":\"true\"}");
    } else {
        if (!reason) reason = "";
        n = snprintf(out, cap, "{\"health\":\"false\",\"reason\":\"%s\"}", reason);
    }
    if (n < 0 || (size_t)n >= cap) return CETCD_ERR_OVERFLOW;
    return CETCD_OK;
}

int cetcd_server_should_listen_clients(int wait_ready, uint64_t leader_id) {
    return wait_ready ? (leader_id != 0) : 1;
}

int cetcd_parse_heartbeat_interval_ms(const char *s, uint64_t *out) {
    if (!out) return CETCD_ERR_INVAL;
    uint64_t v = 0;
    int rc = cetcd_parse_compaction_batch_limit(s, &v);
    if (rc != CETCD_OK) return rc;
    if (v == 0 || v > CETCD_MAX_ELECTION_MS) return CETCD_ERR_INVAL;
    *out = v;
    return CETCD_OK;
}

int cetcd_parse_election_timeout_ms(const char *s, uint64_t *out) {
    if (!out) return CETCD_ERR_INVAL;
    uint64_t v = 0;
    int rc = cetcd_parse_compaction_batch_limit(s, &v);
    if (rc != CETCD_OK) return rc;
    if (v == 0 || v > CETCD_MAX_ELECTION_MS) return CETCD_ERR_INVAL;
    *out = v;
    return CETCD_OK;
}

int cetcd_raft_timing_from_ms(uint64_t tick_ms, uint64_t election_ms,
                              uint64_t *heartbeat_tick, uint64_t *election_tick) {
    if (!heartbeat_tick || !election_tick) return CETCD_ERR_INVAL;
    if (tick_ms == 0) tick_ms = CETCD_DEFAULT_TICK_MS;
    if (election_ms == 0) election_ms = CETCD_DEFAULT_ELECTION_MS;
    if (tick_ms > CETCD_MAX_ELECTION_MS || election_ms > CETCD_MAX_ELECTION_MS)
        return CETCD_ERR_INVAL;
    if (election_ms < tick_ms) return CETCD_ERR_INVAL;
    *heartbeat_tick = 1;
    *election_tick = election_ms / tick_ms;
    if (*election_tick == 0) return CETCD_ERR_INVAL;
    return CETCD_OK;
}

uint64_t cetcd_server_tick_ms(uint64_t tick_ms) {
    return tick_ms ? tick_ms : CETCD_DEFAULT_TICK_MS;
}

int cetcd_parse_max_concurrent_streams(const char *s, uint32_t *out) {
    if (!out) return CETCD_ERR_INVAL;
    uint64_t v = 0;
    int rc = cetcd_parse_compaction_batch_limit(s, &v);
    if (rc != CETCD_OK) return rc;
    if (v == 0 || v > (uint64_t)UINT32_MAX) return CETCD_ERR_INVAL;
    *out = (uint32_t)v;
    return CETCD_OK;
}

int cetcd_parse_listen_url(const char *s, char *host, size_t host_cap,
                           uint16_t *port, int *https) {
    if (!s || !s[0] || !host || host_cap < 2 || !port || !https)
        return CETCD_ERR_INVAL;
    const char *p = s;
    if (strncmp(p, "https://", 8) == 0) {
        *https = 1;
        p += 8;
    } else if (strncmp(p, "http://", 7) == 0) {
        *https = 0;
        p += 7;
    } else {
        return CETCD_ERR_INVAL;
    }
    if (p[0] == '[' || p[0] == '\0' || p[0] == ':')
        return CETCD_ERR_INVAL;
    const char *colon = strrchr(p, ':');
    if (!colon || colon == p) return CETCD_ERR_INVAL;
    size_t hlen = (size_t)(colon - p);
    if (hlen == 0 || hlen + 1 > host_cap) return CETCD_ERR_INVAL;
    errno = 0;
    char *end = NULL;
    long v = strtol(colon + 1, &end, 10);
    if (errno == ERANGE || !end || end == colon + 1 || *end ||
        v < 1 || v > 65535)
        return CETCD_ERR_INVAL;
    memcpy(host, p, hlen);
    host[hlen] = '\0';
    *port = (uint16_t)v;
    return CETCD_OK;
}

int cetcd_parse_metrics_listen_url(const char *s, char *host, size_t host_cap,
                                   uint16_t *port) {
    if (!s || strchr(s, ',')) return CETCD_ERR_INVAL;
    int https = 0;
    int rc = cetcd_parse_listen_url(s, host, host_cap, port, &https);
    if (rc != CETCD_OK) return rc;
    if (https) return CETCD_ERR_INVAL;
    return CETCD_OK;
}

const char *cetcd_server_metrics_addr(const cetcd_server_config *cfg) {
    if (!cfg) return NULL;
    return cfg->metrics_addr[0] ? cfg->metrics_addr : cfg->listen_addr;
}

int cetcd_parse_self_signed_cert_validity(const char *s, uint32_t *out) {
    if (!out) return CETCD_ERR_INVAL;
    uint64_t v = 0;
    int rc = cetcd_parse_compaction_batch_limit(s, &v);
    if (rc != CETCD_OK) return rc;
    if (v == 0) return CETCD_ERR_INVAL;
    if (v > (uint64_t)(INT_MAX / 365)) return CETCD_ERR_INVAL;
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
