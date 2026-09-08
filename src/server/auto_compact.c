#include "cetcd/server.h"
#include "cetcd/discovery.h"

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

int cetcd_parse_metrics_level(const char *s, int *extensive) {
    if (!s || !s[0] || !extensive) return CETCD_ERR_INVAL;
    if (strcmp(s, "basic") == 0) {
        *extensive = 0;
        return CETCD_OK;
    }
    if (strcmp(s, "extensive") == 0) {
        *extensive = 1;
        return CETCD_OK;
    }
    return CETCD_ERR_INVAL;
}

int cetcd_server_want_metrics_extensive(int set, int extensive) {
    return set ? (extensive != 0) : 0;
}

int cetcd_server_want_socket_reuse_port(int set, int enabled) {
    return set ? (enabled != 0) : 0;
}

int cetcd_socket_reuse_port_apply(int enabled) {
    if (!enabled) return CETCD_OK;
#if defined(_WIN32)
    return CETCD_ERR_UNSUPPORT;
#else
    return CETCD_OK;
#endif
}

unsigned cetcd_socket_reuse_port_bind_flags(int enabled) {
    return enabled ? 2u : 0u; /* UV_TCP_REUSEPORT */
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

int cetcd_server_host_whitelist_open(const char *list) {
    if (!list || !list[0]) return 1;
    const char *p = list;
    while (*p) {
        while (*p == ' ' || *p == '\t' || *p == ',') p++;
        if (!*p) break;
        const char *s = p;
        while (*p && *p != ',') p++;
        const char *e = p;
        while (e > s && (e[-1] == ' ' || e[-1] == '\t')) e--;
        if (e == s + 1 && s[0] == '*') return 1;
        if (*p == ',') p++;
    }
    return 0;
}

int cetcd_server_host_allowed(const char *list, const char *host) {
    if (cetcd_server_host_whitelist_open(list)) return 1;
    if (!host || !host[0]) return 0;
    const char *p = list;
    while (*p) {
        while (*p == ' ' || *p == '\t' || *p == ',') p++;
        if (!*p) break;
        const char *s = p;
        while (*p && *p != ',') p++;
        const char *e = p;
        while (e > s && (e[-1] == ' ' || e[-1] == '\t')) e--;
        size_t n = (size_t)(e - s);
        if (n && strlen(host) == n && memcmp(s, host, n) == 0) return 1;
        if (*p == ',') p++;
    }
    return 0;
}

int cetcd_http_host_name(const char *hdr, char *out, size_t cap) {
    if (!hdr || !out || cap < 2) return CETCD_ERR_INVAL;
    while (*hdr == ' ' || *hdr == '\t') hdr++;
    if (!*hdr) return CETCD_ERR_INVAL;
    if (*hdr == '[') {
        const char *rb = strchr(hdr, ']');
        if (!rb || rb == hdr + 1) return CETCD_ERR_INVAL;
        size_t n = (size_t)(rb - hdr - 1);
        if (n >= cap) return CETCD_ERR_OVERFLOW;
        memcpy(out, hdr + 1, n);
        out[n] = '\0';
        return CETCD_OK;
    }
    const char *colon = strrchr(hdr, ':');
    size_t n;
    if (colon && colon[1]) {
        int digits = 1;
        for (const char *d = colon + 1; *d; d++) {
            if (*d < '0' || *d > '9') {
                digits = 0;
                break;
            }
        }
        n = digits ? (size_t)(colon - hdr) : strlen(hdr);
    } else {
        n = strlen(hdr);
    }
    if (n == 0 || n >= cap) return CETCD_ERR_INVAL;
    memcpy(out, hdr, n);
    out[n] = '\0';
    return CETCD_OK;
}

int cetcd_http_headers_complete(const char *req, size_t len) {
    if (!req || len < 2) return 0;
    size_t i;
    for (i = 0; i + 3 < len; i++) {
        if (req[i] == '\r' && req[i + 1] == '\n' &&
            req[i + 2] == '\r' && req[i + 3] == '\n')
            return 1;
    }
    for (i = 0; i + 1 < len; i++) {
        if (req[i] == '\n' && req[i + 1] == '\n') return 1;
    }
    return 0;
}

int cetcd_http_header_get(const char *req, size_t len, const char *name,
                          char *out, size_t cap) {
    if (!req || !name || !name[0] || !out || cap < 2) return CETCD_ERR_INVAL;
    out[0] = '\0';
    size_t nlen = strlen(name);
    const char *p = req;
    const char *end = req + len;
    while (p < end && *p != '\n') p++;
    if (p < end) p++;
    while (p < end) {
        if (*p == '\r' || *p == '\n') break;
        const char *line = p;
        while (p < end && *p != '\n') p++;
        size_t linelen = (size_t)(p - line);
        if (p < end) p++;
        if (linelen > 0 && line[linelen - 1] == '\r') linelen--;
        if (linelen <= nlen + 1) continue;
        size_t i;
        int match = 1;
        for (i = 0; i < nlen; i++) {
            char a = line[i], b = name[i];
            if (a >= 'A' && a <= 'Z') a = (char)(a + 32);
            if (b >= 'A' && b <= 'Z') b = (char)(b + 32);
            if (a != b) {
                match = 0;
                break;
            }
        }
        if (!match || line[nlen] != ':') continue;
        const char *v = line + nlen + 1;
        size_t vl = linelen - nlen - 1;
        while (vl && (*v == ' ' || *v == '\t')) {
            v++;
            vl--;
        }
        if (vl >= cap) return CETCD_ERR_OVERFLOW;
        memcpy(out, v, vl);
        out[vl] = '\0';
        return CETCD_OK;
    }
    return CETCD_ERR_NOTFOUND;
}

int cetcd_server_should_listen_clients(int wait_ready, uint64_t leader_id) {
    return wait_ready ? (leader_id != 0) : 1;
}

int cetcd_parse_raft_io_timeout_ms(const char *s, uint64_t *out) {
    return cetcd_parse_go_duration_ms(s, out);
}

uint64_t cetcd_server_raft_io_timeout_ms(int set, uint64_t ms) {
    uint64_t v = set ? ms : CETCD_DEFAULT_RAFT_IO_TIMEOUT_MS;
    if (v < CETCD_DEFAULT_RAFT_IO_TIMEOUT_MS)
        return CETCD_DEFAULT_RAFT_IO_TIMEOUT_MS;
    return v;
}

int cetcd_raft_io_timed_out(uint64_t last_ms, uint64_t now_ms, uint64_t timeout_ms) {
    if (!last_ms || !timeout_ms) return 0;
    return now_ms >= last_ms && (now_ms - last_ms) >= timeout_ms;
}

int cetcd_parse_snapshot_catchup_entries(const char *s, uint64_t *out) {
    return cetcd_parse_compaction_batch_limit(s, out);
}

uint64_t cetcd_server_snapshot_catchup_entries(int set, uint64_t n) {
    return set ? n : CETCD_DEFAULT_SNAPSHOT_CATCHUP_ENTRIES;
}

uint64_t cetcd_server_raft_compact_index(uint64_t applied, uint64_t catchup) {
    if (applied == 0) return 0;
    if (catchup == 0 || applied > catchup) return applied - catchup;
    return 1;
}

int cetcd_server_want_compact_hash_check(int set, int enabled) {
    return set ? (enabled ? 1 : 0) : 0;
}

uint64_t cetcd_server_compact_hash_check_ms(int set, uint64_t ms) {
    return set ? ms : CETCD_DEFAULT_COMPACT_HASH_CHECK_MS;
}

int cetcd_compact_hash_check_due(uint64_t *last_ms, uint64_t interval_ms,
                                 uint64_t now_ms) {
    if (!last_ms) return 0;
    uint64_t start = now_ms ? now_ms : 1;
    if (*last_ms == 0) {
        *last_ms = start;
        return 0;
    }
    if (interval_ms == 0) {
        *last_ms = start;
        return 1;
    }
    if (now_ms < *last_ms + interval_ms) return 0;
    *last_ms = start;
    return 1;
}

int cetcd_compact_hash_mismatch(int64_t local_rev, uint32_t local_hash,
                                int64_t remote_rev, uint32_t remote_hash) {
    if (local_rev <= 0 || remote_rev <= 0) return 0;
    if (local_rev != remote_rev) return 0;
    return local_hash != remote_hash;
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
    if (cetcd_url_is_unix(s)) return CETCD_ERR_UNSUPPORT;
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
    if (p[0] == '\0') return CETCD_ERR_INVAL;
    const char *port_s = NULL;
    size_t hlen = 0;
    if (p[0] == '[') {
        const char *rb = strchr(p, ']');
        if (!rb || rb == p + 1 || rb[1] != ':') return CETCD_ERR_INVAL;
        hlen = (size_t)(rb - p - 1);
        port_s = rb + 2;
        p += 1;
    } else {
        if (p[0] == ':') return CETCD_ERR_INVAL;
        const char *colon = strrchr(p, ':');
        if (!colon || colon == p) return CETCD_ERR_INVAL;
        hlen = (size_t)(colon - p);
        port_s = colon + 1;
    }
    if (hlen == 0 || hlen + 1 > host_cap) return CETCD_ERR_INVAL;
    errno = 0;
    char *end = NULL;
    long v = strtol(port_s, &end, 10);
    if (errno == ERANGE || !end || end == port_s || *end ||
        v < 1 || v > 65535)
        return CETCD_ERR_INVAL;
    memcpy(host, p, hlen);
    host[hlen] = '\0';
    *port = (uint16_t)v;
    if (strchr(host, ':')) {
        char addr[256], zone[64];
        if (cetcd_parse_ipv6_zone(host, addr, sizeof(addr), zone,
                                  sizeof(zone)) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

static int parse_url_list_(const char *s, cetcd_listen_url *out, size_t cap,
                           size_t *n, int allow_mixed);

int cetcd_parse_listen_urls(const char *s, cetcd_listen_url *out, size_t cap,
                            size_t *n) {
    return parse_url_list_(s, out, cap, n, 0);
}

int cetcd_apply_listen_urls(const char *s, char *host, size_t host_cap,
                            uint16_t *port, int *https,
                            cetcd_listen_url *extra, size_t extra_cap,
                            uint32_t *n_extra) {
    if (!host || host_cap < 2 || !port || !https || !n_extra)
        return CETCD_ERR_INVAL;
    cetcd_listen_url urls[CETCD_MAX_LISTEN_URLS];
    size_t n = 0;
    int rc = cetcd_parse_listen_urls(s, urls, CETCD_MAX_LISTEN_URLS, &n);
    if (rc != CETCD_OK) return rc;
    if (n > 1 && (!extra || extra_cap < n - 1)) return CETCD_ERR_OVERFLOW;
    size_t hlen = strlen(urls[0].host);
    if (hlen + 1 > host_cap) return CETCD_ERR_OVERFLOW;
    memcpy(host, urls[0].host, hlen + 1);
    *port = urls[0].port;
    *https = urls[0].https;
    *n_extra = 0;
    for (size_t i = 1; i < n; i++)
        extra[(*n_extra)++] = urls[i];
    return CETCD_OK;
}

static int parse_url_list_(const char *s, cetcd_listen_url *out, size_t cap,
                           size_t *n, int allow_mixed) {
    if (!s || !s[0] || !out || !n || cap == 0) return CETCD_ERR_INVAL;
    *n = 0;
    const char *p = s;
    while (*p) {
        while (*p == ' ' || *p == '\t') p++;
        if (*p == ',' || *p == '\0') return CETCD_ERR_INVAL;
        const char *start = p;
        while (*p && *p != ',') p++;
        size_t len = (size_t)(p - start);
        while (len && (start[len - 1] == ' ' || start[len - 1] == '\t'))
            len--;
        if (len == 0 || len >= 512) return CETCD_ERR_INVAL;
        char tok[512];
        memcpy(tok, start, len);
        tok[len] = '\0';
        if (*n >= cap) return CETCD_ERR_OVERFLOW;
        int rc = cetcd_parse_listen_url(tok, out[*n].host, sizeof(out[*n].host),
                                        &out[*n].port, &out[*n].https);
        if (rc != CETCD_OK) return rc;
        for (size_t i = 0; i < *n; i++) {
            if (out[i].port == out[*n].port &&
                strcmp(out[i].host, out[*n].host) == 0)
                return CETCD_ERR_INVAL;
        }
        if (!allow_mixed && *n > 0 && out[0].https != out[*n].https)
            return CETCD_ERR_INVAL;
        (*n)++;
        if (*p == ',') {
            p++;
            if (*p == '\0') return CETCD_ERR_INVAL;
        }
    }
    return *n ? CETCD_OK : CETCD_ERR_INVAL;
}

static int join_listen_urls_(const cetcd_listen_url *urls, size_t n,
                             char *out, size_t cap) {
    if (!urls || !n || !out || cap < 8) return CETCD_ERR_INVAL;
    size_t off = 0;
    for (size_t i = 0; i < n; i++) {
        char hp[288];
        if (cetcd_format_host_port(urls[i].host, urls[i].port, hp,
                                   sizeof(hp)) != CETCD_OK)
            return CETCD_ERR_INVAL;
        char one[320];
        int wr = snprintf(one, sizeof(one), "%s://%s",
                          urls[i].https ? "https" : "http", hp);
        if (wr < 0 || (size_t)wr >= sizeof(one)) return CETCD_ERR_OVERFLOW;
        size_t need = (size_t)wr + (i ? 1 : 0) + 1;
        if (off + need > cap) return CETCD_ERR_OVERFLOW;
        if (i) out[off++] = ',';
        memcpy(out + off, one, (size_t)wr);
        off += (size_t)wr;
    }
    out[off] = '\0';
    return CETCD_OK;
}

int cetcd_parse_advertise_urls(const char *s, char *out, size_t cap) {
    cetcd_listen_url urls[CETCD_MAX_LISTEN_URLS];
    size_t n = 0;
    int rc = parse_url_list_(s, urls, CETCD_MAX_LISTEN_URLS, &n, 1);
    if (rc != CETCD_OK) return rc;
    return join_listen_urls_(urls, n, out, cap);
}

int cetcd_advertise_urls_has_https(const char *s) {
    if (!s) return 0;
    const char *p = s;
    while (*p) {
        while (*p == ' ' || *p == '\t' || *p == ',') p++;
        if (strncmp(p, "https://", 8) == 0) return 1;
        while (*p && *p != ',') p++;
    }
    return 0;
}

int cetcd_format_listen_advertise(const char *host, uint16_t port, int https,
                                  const cetcd_listen_url *extra, uint32_t n_extra,
                                  char *out, size_t cap) {
    if (!host || !host[0] || port == 0 || !out) return CETCD_ERR_INVAL;
    cetcd_listen_url urls[CETCD_MAX_LISTEN_URLS];
    size_t n = 0;
    size_t hlen = strlen(host);
    if (hlen >= sizeof(urls[0].host)) return CETCD_ERR_OVERFLOW;
    memcpy(urls[0].host, host, hlen + 1);
    urls[0].port = port;
    urls[0].https = https ? 1 : 0;
    n = 1;
    if (n_extra && !extra) return CETCD_ERR_INVAL;
    for (uint32_t i = 0; i < n_extra; i++) {
        if (n >= CETCD_MAX_LISTEN_URLS) return CETCD_ERR_OVERFLOW;
        urls[n++] = extra[i];
    }
    return join_listen_urls_(urls, n, out, cap);
}

int cetcd_pb_append_csv_strings(uint8_t *buf, size_t cap, size_t *pos,
                                uint8_t tag, const char *csv) {
    if (!buf || !pos || cap == 0) return CETCD_ERR_INVAL;
    if (!csv || !csv[0]) return CETCD_OK;
    const char *p = csv;
    while (*p) {
        while (*p == ' ' || *p == '\t') p++;
        if (*p == ',' || *p == '\0') return CETCD_ERR_INVAL;
        const char *start = p;
        while (*p && *p != ',') p++;
        size_t len = (size_t)(p - start);
        while (len && (start[len - 1] == ' ' || start[len - 1] == '\t'))
            len--;
        if (len == 0) return CETCD_ERR_INVAL;
        if (*pos + 1 + 2 + len > cap) return CETCD_ERR_OVERFLOW;
        buf[(*pos)++] = tag;
        /* protobuf varint length (URL tokens are << 128) */
        if (len >= 128) {
            if (*pos + 2 + len > cap) return CETCD_ERR_OVERFLOW;
            buf[(*pos)++] = (uint8_t)((len & 0x7f) | 0x80);
            buf[(*pos)++] = (uint8_t)(len >> 7);
        } else {
            buf[(*pos)++] = (uint8_t)len;
        }
        memcpy(buf + *pos, start, len);
        *pos += len;
        if (*p == ',') {
            p++;
            if (*p == '\0') return CETCD_ERR_INVAL;
        }
    }
    return CETCD_OK;
}

int cetcd_parse_metrics_listen_url(const char *s, char *host, size_t host_cap,
                                   uint16_t *port) {
    if (!s || strchr(s, ',')) return CETCD_ERR_INVAL;
    int https = 0;
    return cetcd_parse_listen_url(s, host, host_cap, port, &https);
}

int cetcd_parse_metrics_listen_urls(const char *s, cetcd_listen_url *out,
                                    size_t cap, size_t *n) {
    return parse_url_list_(s, out, cap, n, 1);
}

int cetcd_apply_metrics_listen_urls(const char *s, char *host, size_t host_cap,
                                    uint16_t *port,
                                    cetcd_listen_url *extra, size_t extra_cap,
                                    uint32_t *n_extra, int *https) {
    if (!host || host_cap < 2 || !port || !n_extra) return CETCD_ERR_INVAL;
    cetcd_listen_url urls[CETCD_MAX_LISTEN_URLS];
    size_t n = 0;
    int rc = cetcd_parse_metrics_listen_urls(s, urls, CETCD_MAX_LISTEN_URLS, &n);
    if (rc != CETCD_OK) return rc;
    if (n > 1 && (!extra || extra_cap < n - 1)) return CETCD_ERR_OVERFLOW;
    size_t hlen = strlen(urls[0].host);
    if (hlen + 1 > host_cap) return CETCD_ERR_OVERFLOW;
    memcpy(host, urls[0].host, hlen + 1);
    *port = urls[0].port;
    if (https) *https = urls[0].https;
    *n_extra = 0;
    for (size_t i = 1; i < n; i++)
        extra[(*n_extra)++] = urls[i];
    return CETCD_OK;
}

int cetcd_metrics_listen_has_https(int first_https,
                                   const cetcd_listen_url *extra,
                                   uint32_t n_extra) {
    if (first_https) return 1;
    if (!extra) return 0;
    for (uint32_t i = 0; i < n_extra; i++) {
        if (extra[i].https) return 1;
    }
    return 0;
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

#if defined(_WIN32)
#  define CETCD_VER_OS_ "windows"
#elif defined(__APPLE__)
#  define CETCD_VER_OS_ "darwin"
#elif defined(__linux__)
#  define CETCD_VER_OS_ "linux"
#else
#  define CETCD_VER_OS_ "unknown"
#endif
#if defined(_M_X64) || defined(__x86_64__)
#  define CETCD_VER_ARCH_ "amd64"
#elif defined(_M_IX86) || defined(__i386__)
#  define CETCD_VER_ARCH_ "386"
#elif defined(_M_ARM64) || defined(__aarch64__)
#  define CETCD_VER_ARCH_ "arm64"
#else
#  define CETCD_VER_ARCH_ "unknown"
#endif

int cetcd_format_etcd_version(char *out, size_t cap) {
    if (!out || cap < 8) return CETCD_ERR_INVAL;
    int n = snprintf(out, cap,
                     "etcd Version: %u.%u.%u\nGit SHA: unknown\nC Standard: C11\nOS/Arch: %s/%s\n",
                     CETCD_VERSION_MAJOR, CETCD_VERSION_MINOR, CETCD_VERSION_PATCH,
                     CETCD_VER_OS_, CETCD_VER_ARCH_);
    if (n < 0 || (size_t)n >= cap) return CETCD_ERR_OVERFLOW;
    return CETCD_OK;
}

static void yaml_rtrim_(char *s) {
    size_t n = strlen(s);
    while (n > 0 && (s[n - 1] == ' ' || s[n - 1] == '\t' ||
                     s[n - 1] == '\r' || s[n - 1] == '\n')) {
        s[--n] = '\0';
    }
}

static int yaml_unquote_(char *s) {
    size_t n = strlen(s);
    if (n >= 2 && ((s[0] == '"' && s[n - 1] == '"') ||
                   (s[0] == '\'' && s[n - 1] == '\''))) {
        s[n - 1] = '\0';
        memmove(s, s + 1, n - 1);
    }
    return CETCD_OK;
}

static int yaml_key_ok_(const char *s, size_t n) {
    if (!s || n == 0) return 0;
    for (size_t i = 0; i < n; i++) {
        char c = s[i];
        if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
              (c >= '0' && c <= '9') || c == '-' || c == '_'))
            return 0;
    }
    return 1;
}

static int yaml_add_pair_(cetcd_config_pair *out, size_t cap, size_t *n,
                          const char *key, const char *val) {
    if (*n >= cap) return CETCD_ERR_OVERFLOW;
    if (!key || !key[0] || strlen(key) >= CETCD_CONFIG_KEY_MAX)
        return CETCD_ERR_INVAL;
    if (val && strlen(val) >= CETCD_CONFIG_VAL_MAX) return CETCD_ERR_OVERFLOW;
    strncpy(out[*n].key, key, CETCD_CONFIG_KEY_MAX - 1);
    out[*n].key[CETCD_CONFIG_KEY_MAX - 1] = '\0';
    out[*n].val[0] = '\0';
    if (val) strncpy(out[*n].val, val, CETCD_CONFIG_VAL_MAX - 1);
    out[*n].val[CETCD_CONFIG_VAL_MAX - 1] = '\0';
    (*n)++;
    return CETCD_OK;
}

static int yaml_append_list_(cetcd_config_pair *pair, const char *item) {
    size_t used = strlen(pair->val);
    size_t add = strlen(item);
    if (used) {
        if (used + 1 + add >= CETCD_CONFIG_VAL_MAX) return CETCD_ERR_OVERFLOW;
        pair->val[used] = ',';
        memcpy(pair->val + used + 1, item, add + 1);
    } else {
        if (add >= CETCD_CONFIG_VAL_MAX) return CETCD_ERR_OVERFLOW;
        memcpy(pair->val, item, add + 1);
    }
    return CETCD_OK;
}

int cetcd_parse_etcd_config_yaml(const char *text, cetcd_config_pair *out,
                                 size_t cap, size_t *n) {
    if (!text || !out || !n || cap == 0) return CETCD_ERR_INVAL;
    *n = 0;
    const char *p = text;
    int pending_list = 0;
    while (*p) {
        const char *eol = p;
        while (*eol && *eol != '\n') eol++;
        size_t linelen = (size_t)(eol - p);
        if (linelen >= 2048) return CETCD_ERR_OVERFLOW;
        char line[2048];
        memcpy(line, p, linelen);
        line[linelen] = '\0';
        if (*eol == '\n') p = eol + 1;
        else p = eol;
        yaml_rtrim_(line);
        char *s = line;
        int indent = 0;
        if (*s == '\t') return CETCD_ERR_INVAL;
        while (*s == ' ') {
            indent++;
            s++;
        }
        if (!*s || *s == '#') continue;
        if (strcmp(s, "---") == 0 || strcmp(s, "...") == 0) continue;
        if (s[0] == '-' && (s[1] == ' ' || s[1] == '\t')) {
            if (!pending_list || *n == 0 || indent < 1)
                return CETCD_ERR_INVAL;
            char *item = s + 2;
            while (*item == ' ' || *item == '\t') item++;
            yaml_rtrim_(item);
            yaml_unquote_(item);
            if (!item[0]) return CETCD_ERR_INVAL;
            if (yaml_append_list_(&out[*n - 1], item) != CETCD_OK)
                return CETCD_ERR_OVERFLOW;
            continue;
        }
        if (indent > 0) return CETCD_ERR_INVAL;
        pending_list = 0;
        char *colon = strchr(s, ':');
        if (!colon) return CETCD_ERR_INVAL;
        size_t klen = (size_t)(colon - s);
        while (klen > 0 && (s[klen - 1] == ' ' || s[klen - 1] == '\t'))
            klen--;
        if (!yaml_key_ok_(s, klen)) return CETCD_ERR_INVAL;
        char key[CETCD_CONFIG_KEY_MAX];
        if (klen >= sizeof(key)) return CETCD_ERR_INVAL;
        memcpy(key, s, klen);
        key[klen] = '\0';
        char *val = colon + 1;
        while (*val == ' ' || *val == '\t') val++;
        if (val[0] == '#') val[0] = '\0';
        else {
            char *hash = strstr(val, " #");
            if (hash) *hash = '\0';
        }
        yaml_rtrim_(val);
        if (val[0] == '{' || val[0] == '[' || val[0] == '|' || val[0] == '>')
            return CETCD_ERR_INVAL;
        yaml_unquote_(val);
        if (yaml_add_pair_(out, cap, n, key, val) != CETCD_OK)
            return CETCD_ERR_OVERFLOW;
        if (!val[0]) pending_list = 1;
    }
    return CETCD_OK;
}

int cetcd_config_pairs_to_flags(const cetcd_config_pair *pairs, size_t n,
                                char **argv, size_t argv_cap,
                                char *store, size_t store_cap, int *argc) {
    if (!argv || !store || !argc || argv_cap < 1) return CETCD_ERR_INVAL;
    if (n && !pairs) return CETCD_ERR_INVAL;
    int ac = (*argc > 0) ? *argc : 1;
    size_t off = 0;
    for (size_t i = 0; i < n; i++) {
        if (strcmp(pairs[i].key, "version") == 0 ||
            strcmp(pairs[i].key, "config-file") == 0)
            continue;
        size_t klen = strlen(pairs[i].key);
        if (off + 2 + klen + 1 > store_cap) return CETCD_ERR_OVERFLOW;
        if ((size_t)ac + 1 >= argv_cap) return CETCD_ERR_OVERFLOW;
        char *flag = store + off;
        flag[0] = '-';
        flag[1] = '-';
        memcpy(flag + 2, pairs[i].key, klen + 1);
        off += 2 + klen + 1;
        argv[ac++] = flag;
        if (pairs[i].val[0]) {
            size_t vlen = strlen(pairs[i].val);
            if (off + vlen + 1 > store_cap) return CETCD_ERR_OVERFLOW;
            if ((size_t)ac + 1 > argv_cap) return CETCD_ERR_OVERFLOW;
            char *val = store + off;
            memcpy(val, pairs[i].val, vlen + 1);
            off += vlen + 1;
            argv[ac++] = val;
        }
    }
    *argc = ac;
    return CETCD_OK;
}

int cetcd_read_config_file(const char *path, char *buf, size_t cap) {
    if (!path || !path[0] || !buf || cap < 2) return CETCD_ERR_INVAL;
    FILE *f = fopen(path, "rb");
    if (!f) return CETCD_ERR_IO;
    size_t n = fread(buf, 1, cap - 1, f);
    int extra = fgetc(f);
    fclose(f);
    if (extra != EOF) return CETCD_ERR_OVERFLOW;
    buf[n] = '\0';
    return CETCD_OK;
}

int cetcd_flag_to_etcd_env(const char *flag, char *out, size_t cap) {
    if (!flag || !flag[0] || !out || cap < 6) return CETCD_ERR_INVAL;
    size_t n = strlen(flag);
    if (5 + n + 1 > cap) return CETCD_ERR_OVERFLOW;
    memcpy(out, "ETCD_", 5);
    for (size_t i = 0; i < n; i++) {
        char c = flag[i];
        if (c >= 'a' && c <= 'z') c = (char)(c - 32);
        else if (c == '-') c = '_';
        else if (!((c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_'))
            return CETCD_ERR_INVAL;
        out[5 + i] = c;
    }
    out[5 + n] = '\0';
    return CETCD_OK;
}

int cetcd_etcd_env_to_flag(const char *env_key, char *out, size_t cap) {
    if (!env_key || !out || cap < 2) return CETCD_ERR_INVAL;
    if (strncmp(env_key, "ETCD_", 5) != 0 || !env_key[5])
        return CETCD_ERR_INVAL;
    const char *p = env_key + 5;
    size_t n = strlen(p);
    if (n + 1 > cap) return CETCD_ERR_OVERFLOW;
    for (size_t i = 0; i < n; i++) {
        char c = p[i];
        if (c >= 'A' && c <= 'Z') c = (char)(c + 32);
        else if (c == '_') c = '-';
        else if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-'))
            return CETCD_ERR_INVAL;
        out[i] = c;
    }
    out[n] = '\0';
    return CETCD_OK;
}

int cetcd_cli_flag_present(int argc, char *const *argv, const char *flag) {
    if (!flag || !flag[0] || !argv || argc < 2) return 0;
    size_t n = strlen(flag);
    for (int i = 1; i < argc; i++) {
        const char *a = argv[i];
        if (!a || a[0] != '-' || a[1] != '-') continue;
        a += 2;
        if (strncmp(a, flag, n) == 0 && (a[n] == '\0' || a[n] == '='))
            return 1;
    }
    return 0;
}

int cetcd_etcd_env_to_pairs(char *const *envv, int argc, char *const *argv,
                            cetcd_config_pair *out, size_t cap, size_t *n) {
    if (!out || !n || cap == 0) return CETCD_ERR_INVAL;
    *n = 0;
    if (!envv) return CETCD_OK;
    for (size_t i = 0; envv[i]; i++) {
        const char *eq = strchr(envv[i], '=');
        if (!eq) continue;
        size_t klen = (size_t)(eq - envv[i]);
        if (klen < 5 || strncmp(envv[i], "ETCD_", 5) != 0) continue;
        if (klen >= CETCD_CONFIG_KEY_MAX) return CETCD_ERR_INVAL;
        char key[CETCD_CONFIG_KEY_MAX];
        memcpy(key, envv[i], klen);
        key[klen] = '\0';
        if (strcmp(key, "ETCD_VERSION") == 0 ||
            strcmp(key, "ETCD_CONFIG_FILE") == 0)
            continue;
        const char *val = eq + 1;
        if (!val[0]) continue;
        char flag[CETCD_CONFIG_KEY_MAX];
        if (cetcd_etcd_env_to_flag(key, flag, sizeof(flag)) != CETCD_OK)
            return CETCD_ERR_INVAL;
        if (cetcd_cli_flag_present(argc, argv, flag))
            return CETCD_ERR_INVAL;
        if (*n >= cap) return CETCD_ERR_OVERFLOW;
        if (strlen(flag) >= CETCD_CONFIG_KEY_MAX) return CETCD_ERR_OVERFLOW;
        if (strlen(val) >= CETCD_CONFIG_VAL_MAX) return CETCD_ERR_OVERFLOW;
        memcpy(out[*n].key, flag, strlen(flag) + 1);
        memcpy(out[*n].val, val, strlen(val) + 1);
        (*n)++;
    }
    return CETCD_OK;
}

int cetcd_experimental_unsupported_kind(const char *arg) {
    if (!arg) return CETCD_EX_UNSUP_NONE;
    const char *p = arg;
    if (strncmp(p, "--", 2) == 0) p += 2;
    if (strncmp(p, "experimental-", 13) != 0) return CETCD_EX_UNSUP_NONE;
    p += 13;
    char name[96];
    size_t n = 0;
    while (p[n] && p[n] != '=' && n < sizeof(name) - 1) n++;
    if (n == 0 || n >= sizeof(name) - 1) return CETCD_EX_UNSUP_NONE;
    memcpy(name, p, n);
    name[n] = '\0';
    static const struct {
        const char *name;
        int kind;
    } tab[] = {
        { "enable-distributed-tracing", CETCD_EX_UNSUP_BOOL },
        { "distributed-tracing-address", CETCD_EX_UNSUP_VALUE },
        { "distributed-tracing-service-name", CETCD_EX_UNSUP_VALUE },
        { "distributed-tracing-instance-id", CETCD_EX_UNSUP_VALUE },
        { "distributed-tracing-sampling-rate", CETCD_EX_UNSUP_VALUE },
        { "stop-grpc-service-on-defrag", CETCD_EX_UNSUP_BOOL },
        { "peer-skip-client-san-verification", CETCD_EX_UNSUP_BOOL },
        { "txn-mode-write-with-shared-buffer", CETCD_EX_UNSUP_BOOL },
        { "enable-v2v3", CETCD_EX_UNSUP_VALUE },
        { "downgrade-check-time", CETCD_EX_UNSUP_VALUE },
    };
    for (size_t i = 0; i < sizeof(tab) / sizeof(tab[0]); i++) {
        if (strcmp(name, tab[i].name) == 0) return tab[i].kind;
    }
    return CETCD_EX_UNSUP_NONE;
}

int cetcd_etcd_compat_kind(const char *arg) {
    if (!arg) return CETCD_COMPAT_NONE;
    if (cetcd_cli_flag_is(arg, "--enable-grpc-gateway") ||
        cetcd_cli_flag_is(arg, "--enable-v2") ||
        cetcd_cli_flag_is(arg, "--unsafe-no-fsync"))
        return CETCD_COMPAT_BOOL_OFF;
    if (cetcd_cli_flag_is(arg, "--socket-reuse-address"))
        return CETCD_COMPAT_BOOL_ON;
    if (cetcd_cli_flag_is(arg, "--listen-client-http-urls") ||
        cetcd_cli_flag_is(arg, "--cors") ||
        cetcd_cli_flag_is(arg, "--discovery") ||
        cetcd_cli_flag_is(arg, "--discovery-proxy") ||
        cetcd_cli_flag_is(arg, "--proxy-failure-wait") ||
        cetcd_cli_flag_is(arg, "--proxy-refresh-interval") ||
        cetcd_cli_flag_is(arg, "--proxy-dial-timeout") ||
        cetcd_cli_flag_is(arg, "--proxy-write-timeout") ||
        cetcd_cli_flag_is(arg, "--proxy-read-timeout") ||
        cetcd_cli_flag_is(arg, "--max-snapshots") ||
        cetcd_cli_flag_is(arg, "--max-wals") ||
        cetcd_cli_flag_is(arg, "--client-cert-file") ||
        cetcd_cli_flag_is(arg, "--client-key-file") ||
        cetcd_cli_flag_is(arg, "--backend-batch-limit") ||
        cetcd_cli_flag_is(arg, "--backend-batch-interval") ||
        cetcd_cli_flag_is(arg, "--backend-bbolt-freelist-type"))
        return CETCD_COMPAT_VALUE;
    return CETCD_COMPAT_NONE;
}

int cetcd_parse_v2_deprecation(const char *s) {
    if (!s || !s[0]) return CETCD_ERR_INVAL;
    if (strcmp(s, "gone") == 0 ||
        strcmp(s, "write-only") == 0 ||
        strcmp(s, "write-only-drop-data") == 0 ||
        strcmp(s, "write-only-skip-check") == 0)
        return CETCD_OK;
    return CETCD_ERR_INVAL;
}

int cetcd_parse_proxy_mode(const char *s) {
    if (!s || !s[0]) return CETCD_ERR_INVAL;
    if (strcmp(s, "off") == 0) return CETCD_OK;
    return CETCD_ERR_INVAL;
}

int cetcd_parse_discovery_fallback(const char *s) {
    if (!s || !s[0]) return CETCD_ERR_INVAL;
    if (strcmp(s, "exit") == 0) return CETCD_OK;
    return CETCD_ERR_INVAL;
}

int cetcd_grpc_keepalive_kind(const char *arg) {
    if (!arg) return CETCD_KA_NONE;
    const char *p = arg;
    if (strncmp(p, "--", 2) == 0) p += 2;
    if (strncmp(p, "grpc-keepalive-", 15) != 0) return CETCD_KA_NONE;
    p += 15;
    char name[96];
    size_t n = 0;
    while (p[n] && p[n] != '=' && n < sizeof(name) - 1) n++;
    if (n == 0 || n >= sizeof(name) - 1) return CETCD_KA_NONE;
    memcpy(name, p, n);
    name[n] = '\0';
    if (strcmp(name, "time") == 0 || strcmp(name, "interval") == 0)
        return CETCD_KA_IDLE;
    if (strcmp(name, "timeout") == 0) return CETCD_KA_TIMEOUT;
    if (strcmp(name, "min-time") == 0) return CETCD_KA_MIN_TIME;
    if (strcmp(name, "permit-without-stream") == 0) return CETCD_KA_PERMIT;
    return CETCD_KA_UNKNOWN;
}

int cetcd_parse_grpc_keepalive_sec(const char *s, int min_v, int *out) {
    if (!s || !s[0] || !out || min_v < 0) return CETCD_ERR_INVAL;
    int all_digits = 1;
    for (const char *p = s; *p; p++) {
        if (*p < '0' || *p > '9') {
            all_digits = 0;
            break;
        }
    }
    uint64_t sec = 0;
    if (all_digits) {
        char *end = NULL;
        errno = 0;
        long v = strtol(s, &end, 10);
        if (errno == ERANGE || !end || end == s || *end)
            return CETCD_ERR_INVAL;
        if (v < min_v || v > 86400) return CETCD_ERR_INVAL;
        *out = (int)v;
        return CETCD_OK;
    }
    if (cetcd_parse_go_duration_sec(s, &sec) != CETCD_OK)
        return CETCD_ERR_INVAL;
    if ((int)sec < min_v || sec > 86400) return CETCD_ERR_INVAL;
    *out = (int)sec;
    return CETCD_OK;
}

uint64_t cetcd_quota_backend_bytes_effective(uint64_t n) {
    return n ? n : CETCD_DEFAULT_QUOTA_BACKEND_BYTES;
}

uint64_t cetcd_snapshot_count_effective(uint64_t n) {
    return n ? n : CETCD_DEFAULT_SNAPSHOT_COUNT;
}

int cetcd_cli_flag_is(const char *arg, const char *name) {
    size_t n;
    if (!arg || !name || name[0] != '-' || name[1] != '-') return 0;
    n = strlen(name);
    if (strncmp(arg, name, n) != 0) return 0;
    return arg[n] == '\0' || arg[n] == '=';
}

int cetcd_cli_is_long_flag(const char *arg) {
    if (!arg || arg[0] != '-' || arg[1] != '-') return 0;
    return arg[2] != '\0';
}

int cetcd_take_cli_flag_value(int *i, int argc, char *const *argv,
                              const char **out) {
    const char *eq;
    if (!i || *i < 0 || *i >= argc || !argv || !argv[*i] || !out)
        return CETCD_ERR_INVAL;
    eq = strchr(argv[*i], '=');
    if (eq) {
        if (!eq[1]) return CETCD_ERR_INVAL;
        *out = eq + 1;
        return CETCD_OK;
    }
    if (*i + 1 >= argc || !argv[*i + 1] || !argv[*i + 1][0])
        return CETCD_ERR_INVAL;
    /* leftover `--flag` cannot become the value */
    if (argv[*i + 1][0] == '-' && argv[*i + 1][1] == '-')
        return CETCD_ERR_INVAL;
    *out = argv[++(*i)];
    return CETCD_OK;
}

int cetcd_take_cli_bool_flag(int *i, int argc, char *const *argv, int *out) {
    const char *eq;
    int on = 1;
    if (!i || *i < 0 || *i >= argc || !argv || !argv[*i] || !out)
        return CETCD_ERR_INVAL;
    eq = strchr(argv[*i], '=');
    if (eq) {
        if (cetcd_parse_bool_flag(eq + 1, &on) != CETCD_OK)
            return CETCD_ERR_INVAL;
    } else if (*i + 1 < argc && argv[*i + 1][0] != '-') {
        if (cetcd_parse_bool_flag(argv[++(*i)], &on) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    *out = on;
    return CETCD_OK;
}

int cetcd_take_cli_bool_eq(int *i, int argc, char *const *argv, int *out) {
    const char *eq;
    int on = 1;
    if (!i || *i < 0 || *i >= argc || !argv || !argv[*i] || !out)
        return CETCD_ERR_INVAL;
    eq = strchr(argv[*i], '=');
    if (eq) {
        if (cetcd_parse_bool_flag(eq + 1, &on) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    *out = on;
    return CETCD_OK;
}

int cetcd_parse_command_timeout_sec(const char *s, uint64_t *out) {
    if (!s || !s[0] || !out) return CETCD_ERR_INVAL;
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
        *out = (uint64_t)v;
        return CETCD_OK;
    }
    return cetcd_parse_go_duration_sec(s, out);
}

int cetcd_parse_i64(const char *s, int64_t *out) {
    if (!s || !s[0] || !out) return CETCD_ERR_INVAL;
    errno = 0;
    char *end = NULL;
    long long v = strtoll(s, &end, 10);
    if (errno == ERANGE || !end || end == s || *end)
        return CETCD_ERR_INVAL;
    *out = (int64_t)v;
    return CETCD_OK;
}

int cetcd_encode_hashkv_request(int64_t rev, uint8_t *out, size_t cap, size_t *n) {
    if (!out || !n || cap == 0) return CETCD_ERR_INVAL;
    if (rev < 0) return CETCD_ERR_INVAL;
    *n = 0;
    if (rev == 0) return CETCD_OK;
    size_t pos = 0;
    if (pos >= cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x08; /* field 1, varint */
    uint64_t v = (uint64_t)rev;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_hashkv_request(const uint8_t *req, size_t len, int64_t *out) {
    size_t p = 0;
    if (!out) return CETCD_ERR_INVAL;
    *out = 0;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x08) {
            uint64_t v = 0;
            int shift = 0;
            int got = 0;
            while (p < len) {
                uint8_t b = req[p++];
                v |= (uint64_t)(b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    got = 1;
                    break;
                }
                shift += 7;
                if (shift > 63) return CETCD_ERR_INVAL;
            }
            if (!got) return CETCD_ERR_INVAL;
            if (v > (uint64_t)INT64_MAX) return CETCD_ERR_INVAL;
            *out = (int64_t)v;
            continue;
        }
        if ((tag & 7) == 0) {
            int shift = 0;
            int got = 0;
            while (p < len) {
                uint8_t b = req[p++];
                if ((b & 0x80) == 0) {
                    got = 1;
                    break;
                }
                shift += 7;
                if (shift > 63) return CETCD_ERR_INVAL;
            }
            if (!got) return CETCD_ERR_INVAL;
            continue;
        }
        if ((tag & 7) == 2) {
            uint64_t skip = 0;
            int shift = 0;
            int got = 0;
            while (p < len) {
                uint8_t b = req[p++];
                skip |= (uint64_t)(b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    got = 1;
                    break;
                }
                shift += 7;
                if (shift > 63) return CETCD_ERR_INVAL;
            }
            if (!got || p + skip > len) return CETCD_ERR_INVAL;
            p += (size_t)skip;
            continue;
        }
        return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_parse_compact_request(const uint8_t *req, size_t len,
                                int64_t *rev, int *physical) {
    size_t p = 0;
    if (!rev || !physical) return CETCD_ERR_INVAL;
    *rev = 0;
    *physical = 0;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x08 || tag == 0x10) {
            uint64_t v = 0;
            int shift = 0;
            int got = 0;
            while (p < len) {
                uint8_t b = req[p++];
                v |= (uint64_t)(b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    got = 1;
                    break;
                }
                shift += 7;
                if (shift > 63) return CETCD_ERR_INVAL;
            }
            if (!got) return CETCD_ERR_INVAL;
            if (tag == 0x08) {
                if (v > (uint64_t)INT64_MAX) return CETCD_ERR_INVAL;
                *rev = (int64_t)v;
            } else {
                *physical = v != 0;
            }
            continue;
        }
        if ((tag & 7) == 0) {
            int shift = 0;
            int got = 0;
            while (p < len) {
                uint8_t b = req[p++];
                if ((b & 0x80) == 0) {
                    got = 1;
                    break;
                }
                shift += 7;
                if (shift > 63) return CETCD_ERR_INVAL;
            }
            if (!got) return CETCD_ERR_INVAL;
            continue;
        }
        if ((tag & 7) == 2) {
            uint64_t skip = 0;
            int shift = 0;
            int got = 0;
            while (p < len) {
                uint8_t b = req[p++];
                skip |= (uint64_t)(b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    got = 1;
                    break;
                }
                shift += 7;
                if (shift > 63) return CETCD_ERR_INVAL;
            }
            if (!got || p + skip > len) return CETCD_ERR_INVAL;
            p += (size_t)skip;
            continue;
        }
        return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_move_leader_request(uint64_t target, uint8_t *out, size_t cap,
                                     size_t *n) {
    if (!out || !n || cap == 0) return CETCD_ERR_INVAL;
    if (target == 0) return CETCD_ERR_INVAL;
    size_t pos = 0;
    if (pos >= cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x08;
    uint64_t v = target;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_move_leader_request(const uint8_t *req, size_t len,
                                    uint64_t *out) {
    size_t p = 0;
    if (!out) return CETCD_ERR_INVAL;
    *out = 0;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x08) {
            uint64_t v = 0;
            int shift = 0;
            int got = 0;
            while (p < len) {
                uint8_t b = req[p++];
                v |= (uint64_t)(b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    got = 1;
                    break;
                }
                shift += 7;
                if (shift > 63) return CETCD_ERR_INVAL;
            }
            if (!got) return CETCD_ERR_INVAL;
            *out = v;
            continue;
        }
        if ((tag & 7) == 0) {
            int shift = 0;
            int got = 0;
            while (p < len) {
                uint8_t b = req[p++];
                if ((b & 0x80) == 0) {
                    got = 1;
                    break;
                }
                shift += 7;
                if (shift > 63) return CETCD_ERR_INVAL;
            }
            if (!got) return CETCD_ERR_INVAL;
            continue;
        }
        if ((tag & 7) == 2) {
            uint64_t skip = 0;
            int shift = 0;
            int got = 0;
            while (p < len) {
                uint8_t b = req[p++];
                skip |= (uint64_t)(b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    got = 1;
                    break;
                }
                shift += 7;
                if (shift > 63) return CETCD_ERR_INVAL;
            }
            if (!got || p + skip > len) return CETCD_ERR_INVAL;
            p += (size_t)skip;
            continue;
        }
        return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_parse_lease_grant_request(const uint8_t *req, size_t len,
                                    int64_t *ttl, int64_t *id) {
    size_t p = 0;
    if (!ttl || !id) return CETCD_ERR_INVAL;
    *ttl = 0;
    *id = 0;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x08 || tag == 0x10) {
            uint64_t v = 0;
            int shift = 0;
            int got = 0;
            while (p < len) {
                uint8_t b = req[p++];
                v |= (uint64_t)(b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    got = 1;
                    break;
                }
                shift += 7;
                if (shift > 63) return CETCD_ERR_INVAL;
            }
            if (!got) return CETCD_ERR_INVAL;
            if (v > (uint64_t)INT64_MAX) return CETCD_ERR_INVAL;
            if (tag == 0x08) *ttl = (int64_t)v;
            else *id = (int64_t)v;
            continue;
        }
        if ((tag & 7) == 0) {
            int shift = 0;
            int got = 0;
            while (p < len) {
                uint8_t b = req[p++];
                if ((b & 0x80) == 0) {
                    got = 1;
                    break;
                }
                shift += 7;
                if (shift > 63) return CETCD_ERR_INVAL;
            }
            if (!got) return CETCD_ERR_INVAL;
            continue;
        }
        if ((tag & 7) == 2) {
            uint64_t skip = 0;
            int shift = 0;
            int got = 0;
            while (p < len) {
                uint8_t b = req[p++];
                skip |= (uint64_t)(b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    got = 1;
                    break;
                }
                shift += 7;
                if (shift > 63) return CETCD_ERR_INVAL;
            }
            if (!got || p + skip > len) return CETCD_ERR_INVAL;
            p += (size_t)skip;
            continue;
        }
        return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_lease_id_request(int64_t id, uint8_t *out, size_t cap,
                                  size_t *n) {
    if (!out || !n || cap == 0) return CETCD_ERR_INVAL;
    if (id <= 0) return CETCD_ERR_INVAL;
    size_t pos = 0;
    if (pos >= cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x08;
    uint64_t v = (uint64_t)id;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_lease_id_request(const uint8_t *req, size_t len,
                                 int64_t *id, int *keys) {
    size_t p = 0;
    if (!id) return CETCD_ERR_INVAL;
    *id = 0;
    if (keys) *keys = 0;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x08 || tag == 0x10) {
            uint64_t v = 0;
            int shift = 0;
            int got = 0;
            while (p < len) {
                uint8_t b = req[p++];
                v |= (uint64_t)(b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    got = 1;
                    break;
                }
                shift += 7;
                if (shift > 63) return CETCD_ERR_INVAL;
            }
            if (!got) return CETCD_ERR_INVAL;
            if (v > (uint64_t)INT64_MAX) return CETCD_ERR_INVAL;
            if (tag == 0x08) *id = (int64_t)v;
            else if (keys) *keys = v != 0;
            continue;
        }
        if ((tag & 7) == 0) {
            int shift = 0;
            int got = 0;
            while (p < len) {
                uint8_t b = req[p++];
                if ((b & 0x80) == 0) {
                    got = 1;
                    break;
                }
                shift += 7;
                if (shift > 63) return CETCD_ERR_INVAL;
            }
            if (!got) return CETCD_ERR_INVAL;
            continue;
        }
        if ((tag & 7) == 2) {
            uint64_t skip = 0;
            int shift = 0;
            int got = 0;
            while (p < len) {
                uint8_t b = req[p++];
                skip |= (uint64_t)(b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    got = 1;
                    break;
                }
                shift += 7;
                if (shift > 63) return CETCD_ERR_INVAL;
            }
            if (!got || p + skip > len) return CETCD_ERR_INVAL;
            p += (size_t)skip;
            continue;
        }
        return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_alarm_request(int action, uint64_t member_id, int alarm,
                               uint8_t *out, size_t cap, size_t *n) {
    if (!out || !n || cap == 0) return CETCD_ERR_INVAL;
    if (action < 0 || alarm < 0) return CETCD_ERR_INVAL;
    size_t pos = 0;
    if (pos + 2 > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x08;
    uint64_t v = (uint64_t)action;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    if (pos + 2 > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x10;
    v = member_id;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    if (pos + 2 > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x18;
    v = (uint64_t)alarm;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_alarm_request(const uint8_t *req, size_t len, int *action,
                              uint64_t *member_id, int *alarm) {
    size_t p = 0;
    if (!action || !member_id || !alarm) return CETCD_ERR_INVAL;
    *action = 0;
    *member_id = 0;
    *alarm = 0;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x08 || tag == 0x10 || tag == 0x18) {
            uint64_t v = 0;
            int shift = 0;
            int got = 0;
            while (p < len) {
                uint8_t b = req[p++];
                v |= (uint64_t)(b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    got = 1;
                    break;
                }
                shift += 7;
                if (shift > 63) return CETCD_ERR_INVAL;
            }
            if (!got) return CETCD_ERR_INVAL;
            if (tag == 0x08) {
                if (v > (uint64_t)INT_MAX) return CETCD_ERR_INVAL;
                *action = (int)v;
            } else if (tag == 0x10) {
                *member_id = v;
            } else {
                if (v > (uint64_t)INT_MAX) return CETCD_ERR_INVAL;
                *alarm = (int)v;
            }
            continue;
        }
        if ((tag & 7) == 0) {
            int shift = 0;
            int got = 0;
            while (p < len) {
                uint8_t b = req[p++];
                if ((b & 0x80) == 0) {
                    got = 1;
                    break;
                }
                shift += 7;
                if (shift > 63) return CETCD_ERR_INVAL;
            }
            if (!got) return CETCD_ERR_INVAL;
            continue;
        }
        if ((tag & 7) == 2) {
            uint64_t skip = 0;
            int shift = 0;
            int got = 0;
            while (p < len) {
                uint8_t b = req[p++];
                skip |= (uint64_t)(b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    got = 1;
                    break;
                }
                shift += 7;
                if (shift > 63) return CETCD_ERR_INVAL;
            }
            if (!got || p + skip > len) return CETCD_ERR_INVAL;
            p += (size_t)skip;
            continue;
        }
        return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

void cetcd_range_request_clear(cetcd_range_request *r) {
    if (!r) return;
    free(r->key);
    free(r->range_end);
    memset(r, 0, sizeof(*r));
}

int cetcd_encode_range_request(const uint8_t *key, size_t key_len, int64_t rev,
                               uint8_t *out, size_t cap, size_t *n) {
    if (!out || !n || !key || key_len == 0) return CETCD_ERR_INVAL;
    if (rev < 0) return CETCD_ERR_INVAL;
    size_t pos = 0;
    if (pos + 2 + key_len > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x0a;
    uint64_t lv = key_len;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(lv & 0x7fu);
        lv >>= 7;
        if (lv) b |= 0x80u;
        out[pos++] = b;
    } while (lv);
    if (pos + key_len > cap) return CETCD_ERR_OVERFLOW;
    memcpy(out + pos, key, key_len);
    pos += key_len;
    if (rev > 0) {
        if (pos + 2 > cap) return CETCD_ERR_OVERFLOW;
        out[pos++] = 0x20;
        uint64_t v = (uint64_t)rev;
        do {
            if (pos >= cap) return CETCD_ERR_OVERFLOW;
            uint8_t b = (uint8_t)(v & 0x7fu);
            v >>= 7;
            if (v) b |= 0x80u;
            out[pos++] = b;
        } while (v);
    }
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_range_request(const uint8_t *req, size_t len,
                              cetcd_range_request *out) {
    size_t p = 0;
    if (!out) return CETCD_ERR_INVAL;
    memset(out, 0, sizeof(*out));
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x0a || tag == 0x12) {
            uint64_t skip = 0;
            int shift = 0;
            int got = 0;
            while (p < len) {
                uint8_t b = req[p++];
                skip |= (uint64_t)(b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    got = 1;
                    break;
                }
                shift += 7;
                if (shift > 63) {
                    cetcd_range_request_clear(out);
                    return CETCD_ERR_INVAL;
                }
            }
            if (!got || p + skip > len) {
                cetcd_range_request_clear(out);
                return CETCD_ERR_INVAL;
            }
            if (skip == 0) {
                if (tag == 0x0a) {
                    free(out->key);
                    out->key = NULL;
                    out->key_len = 0;
                } else {
                    free(out->range_end);
                    out->range_end = NULL;
                    out->range_end_len = 0;
                }
                continue;
            }
            uint8_t *copy = (uint8_t *)malloc((size_t)skip);
            if (!copy) {
                cetcd_range_request_clear(out);
                return CETCD_ERR_NOMEM;
            }
            memcpy(copy, req + p, (size_t)skip);
            p += (size_t)skip;
            if (tag == 0x0a) {
                free(out->key);
                out->key = copy;
                out->key_len = (size_t)skip;
            } else {
                free(out->range_end);
                out->range_end = copy;
                out->range_end_len = (size_t)skip;
            }
            continue;
        }
        if (tag == 0x18 || tag == 0x20 || tag == 0x28 || tag == 0x30 ||
            tag == 0x38 || tag == 0x40 || tag == 0x48 || tag == 0x50 ||
            tag == 0x58 || tag == 0x60 || tag == 0x68) {
            uint64_t v = 0;
            int shift = 0;
            int got = 0;
            while (p < len) {
                uint8_t b = req[p++];
                v |= (uint64_t)(b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    got = 1;
                    break;
                }
                shift += 7;
                if (shift > 63) {
                    cetcd_range_request_clear(out);
                    return CETCD_ERR_INVAL;
                }
            }
            if (!got || v > (uint64_t)INT64_MAX) {
                cetcd_range_request_clear(out);
                return CETCD_ERR_INVAL;
            }
            if (tag == 0x18) out->limit = (int64_t)v;
            else if (tag == 0x20) out->rev = (int64_t)v;
            else if (tag == 0x28) out->sort_order = (int)v;
            else if (tag == 0x30) out->sort_target = (int)v;
            else if (tag == 0x38) out->serializable = v != 0;
            else if (tag == 0x40) out->keys_only = v != 0;
            else if (tag == 0x48) out->count_only = v != 0;
            else if (tag == 0x50) out->min_mod_rev = (int64_t)v;
            else if (tag == 0x58) out->max_mod_rev = (int64_t)v;
            else if (tag == 0x60) out->min_create_rev = (int64_t)v;
            else out->max_create_rev = (int64_t)v;
            continue;
        }
        if ((tag & 7) == 0) {
            int shift = 0;
            int got = 0;
            while (p < len) {
                uint8_t b = req[p++];
                if ((b & 0x80) == 0) {
                    got = 1;
                    break;
                }
                shift += 7;
                if (shift > 63) {
                    cetcd_range_request_clear(out);
                    return CETCD_ERR_INVAL;
                }
            }
            if (!got) {
                cetcd_range_request_clear(out);
                return CETCD_ERR_INVAL;
            }
            continue;
        }
        if ((tag & 7) == 2) {
            uint64_t skip = 0;
            int shift = 0;
            int got = 0;
            while (p < len) {
                uint8_t b = req[p++];
                skip |= (uint64_t)(b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    got = 1;
                    break;
                }
                shift += 7;
                if (shift > 63) {
                    cetcd_range_request_clear(out);
                    return CETCD_ERR_INVAL;
                }
            }
            if (!got || p + skip > len) {
                cetcd_range_request_clear(out);
                return CETCD_ERR_INVAL;
            }
            p += (size_t)skip;
            continue;
        }
        cetcd_range_request_clear(out);
        return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

void cetcd_put_request_clear(cetcd_put_request *r) {
    if (!r) return;
    free(r->key);
    free(r->value);
    memset(r, 0, sizeof(*r));
}

int cetcd_encode_put_request(const uint8_t *key, size_t key_len,
                             const uint8_t *value, size_t value_len,
                             int64_t lease, uint8_t *out, size_t cap,
                             size_t *n) {
    if (!out || !n || !key || key_len == 0) return CETCD_ERR_INVAL;
    if (lease < 0) return CETCD_ERR_INVAL;
    size_t pos = 0;
    if (pos + 2 + key_len > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x0a;
    uint64_t lv = key_len;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(lv & 0x7fu);
        lv >>= 7;
        if (lv) b |= 0x80u;
        out[pos++] = b;
    } while (lv);
    if (pos + key_len > cap) return CETCD_ERR_OVERFLOW;
    memcpy(out + pos, key, key_len);
    pos += key_len;
    if (value && value_len) {
        if (pos + 2 + value_len > cap) return CETCD_ERR_OVERFLOW;
        out[pos++] = 0x12;
        lv = value_len;
        do {
            if (pos >= cap) return CETCD_ERR_OVERFLOW;
            uint8_t b = (uint8_t)(lv & 0x7fu);
            lv >>= 7;
            if (lv) b |= 0x80u;
            out[pos++] = b;
        } while (lv);
        if (pos + value_len > cap) return CETCD_ERR_OVERFLOW;
        memcpy(out + pos, value, value_len);
        pos += value_len;
    }
    if (lease > 0) {
        if (pos + 2 > cap) return CETCD_ERR_OVERFLOW;
        out[pos++] = 0x18;
        uint64_t v = (uint64_t)lease;
        do {
            if (pos >= cap) return CETCD_ERR_OVERFLOW;
            uint8_t b = (uint8_t)(v & 0x7fu);
            v >>= 7;
            if (v) b |= 0x80u;
            out[pos++] = b;
        } while (v);
    }
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_put_request(const uint8_t *req, size_t len,
                            cetcd_put_request *out) {
    size_t p = 0;
    if (!out) return CETCD_ERR_INVAL;
    memset(out, 0, sizeof(*out));
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x0a || tag == 0x12) {
            uint64_t skip = 0;
            int shift = 0;
            int got = 0;
            while (p < len) {
                uint8_t b = req[p++];
                skip |= (uint64_t)(b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    got = 1;
                    break;
                }
                shift += 7;
                if (shift > 63) {
                    cetcd_put_request_clear(out);
                    return CETCD_ERR_INVAL;
                }
            }
            if (!got || p + skip > len) {
                cetcd_put_request_clear(out);
                return CETCD_ERR_INVAL;
            }
            if (skip == 0) {
                if (tag == 0x0a) {
                    free(out->key);
                    out->key = NULL;
                    out->key_len = 0;
                } else {
                    free(out->value);
                    out->value = NULL;
                    out->value_len = 0;
                }
                continue;
            }
            uint8_t *copy = (uint8_t *)malloc((size_t)skip);
            if (!copy) {
                cetcd_put_request_clear(out);
                return CETCD_ERR_NOMEM;
            }
            memcpy(copy, req + p, (size_t)skip);
            p += (size_t)skip;
            if (tag == 0x0a) {
                free(out->key);
                out->key = copy;
                out->key_len = (size_t)skip;
            } else {
                free(out->value);
                out->value = copy;
                out->value_len = (size_t)skip;
            }
            continue;
        }
        if (tag == 0x18 || tag == 0x20 || tag == 0x28 || tag == 0x30) {
            uint64_t v = 0;
            int shift = 0;
            int got = 0;
            while (p < len) {
                uint8_t b = req[p++];
                v |= (uint64_t)(b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    got = 1;
                    break;
                }
                shift += 7;
                if (shift > 63) {
                    cetcd_put_request_clear(out);
                    return CETCD_ERR_INVAL;
                }
            }
            if (!got || v > (uint64_t)INT64_MAX) {
                cetcd_put_request_clear(out);
                return CETCD_ERR_INVAL;
            }
            if (tag == 0x18) out->lease = (int64_t)v;
            else if (tag == 0x20) out->prev_kv = v != 0;
            else if (tag == 0x28) out->ignore_value = v != 0;
            else out->ignore_lease = v != 0;
            continue;
        }
        if ((tag & 7) == 0) {
            int shift = 0;
            int got = 0;
            while (p < len) {
                uint8_t b = req[p++];
                if ((b & 0x80) == 0) {
                    got = 1;
                    break;
                }
                shift += 7;
                if (shift > 63) {
                    cetcd_put_request_clear(out);
                    return CETCD_ERR_INVAL;
                }
            }
            if (!got) {
                cetcd_put_request_clear(out);
                return CETCD_ERR_INVAL;
            }
            continue;
        }
        if ((tag & 7) == 2) {
            uint64_t skip = 0;
            int shift = 0;
            int got = 0;
            while (p < len) {
                uint8_t b = req[p++];
                skip |= (uint64_t)(b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    got = 1;
                    break;
                }
                shift += 7;
                if (shift > 63) {
                    cetcd_put_request_clear(out);
                    return CETCD_ERR_INVAL;
                }
            }
            if (!got || p + skip > len) {
                cetcd_put_request_clear(out);
                return CETCD_ERR_INVAL;
            }
            p += (size_t)skip;
            continue;
        }
        cetcd_put_request_clear(out);
        return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

void cetcd_delete_range_request_clear(cetcd_delete_range_request *r) {
    if (!r) return;
    free(r->key);
    free(r->range_end);
    memset(r, 0, sizeof(*r));
}

int cetcd_encode_delete_range_request(const uint8_t *key, size_t key_len,
                                      const uint8_t *range_end,
                                      size_t range_end_len, int prev_kv,
                                      uint8_t *out, size_t cap, size_t *n) {
    if (!out || !n || !key || key_len == 0) return CETCD_ERR_INVAL;
    size_t pos = 0;
    if (pos + 2 + key_len > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x0a;
    uint64_t lv = key_len;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(lv & 0x7fu);
        lv >>= 7;
        if (lv) b |= 0x80u;
        out[pos++] = b;
    } while (lv);
    if (pos + key_len > cap) return CETCD_ERR_OVERFLOW;
    memcpy(out + pos, key, key_len);
    pos += key_len;
    if (range_end && range_end_len) {
        if (pos + 2 + range_end_len > cap) return CETCD_ERR_OVERFLOW;
        out[pos++] = 0x12;
        lv = range_end_len;
        do {
            if (pos >= cap) return CETCD_ERR_OVERFLOW;
            uint8_t b = (uint8_t)(lv & 0x7fu);
            lv >>= 7;
            if (lv) b |= 0x80u;
            out[pos++] = b;
        } while (lv);
        if (pos + range_end_len > cap) return CETCD_ERR_OVERFLOW;
        memcpy(out + pos, range_end, range_end_len);
        pos += range_end_len;
    }
    if (prev_kv) {
        if (pos + 2 > cap) return CETCD_ERR_OVERFLOW;
        out[pos++] = 0x18;
        out[pos++] = 0x01;
    }
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_delete_range_request(const uint8_t *req, size_t len,
                                     cetcd_delete_range_request *out) {
    size_t p = 0;
    if (!out) return CETCD_ERR_INVAL;
    memset(out, 0, sizeof(*out));
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x0a || tag == 0x12) {
            uint64_t skip = 0;
            int shift = 0;
            int got = 0;
            while (p < len) {
                uint8_t b = req[p++];
                skip |= (uint64_t)(b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    got = 1;
                    break;
                }
                shift += 7;
                if (shift > 63) {
                    cetcd_delete_range_request_clear(out);
                    return CETCD_ERR_INVAL;
                }
            }
            if (!got || p + skip > len) {
                cetcd_delete_range_request_clear(out);
                return CETCD_ERR_INVAL;
            }
            if (skip == 0) {
                if (tag == 0x0a) {
                    free(out->key);
                    out->key = NULL;
                    out->key_len = 0;
                } else {
                    free(out->range_end);
                    out->range_end = NULL;
                    out->range_end_len = 0;
                }
                continue;
            }
            uint8_t *copy = (uint8_t *)malloc((size_t)skip);
            if (!copy) {
                cetcd_delete_range_request_clear(out);
                return CETCD_ERR_NOMEM;
            }
            memcpy(copy, req + p, (size_t)skip);
            p += (size_t)skip;
            if (tag == 0x0a) {
                free(out->key);
                out->key = copy;
                out->key_len = (size_t)skip;
            } else {
                free(out->range_end);
                out->range_end = copy;
                out->range_end_len = (size_t)skip;
            }
            continue;
        }
        if (tag == 0x18) {
            uint64_t v = 0;
            int shift = 0;
            int got = 0;
            while (p < len) {
                uint8_t b = req[p++];
                v |= (uint64_t)(b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    got = 1;
                    break;
                }
                shift += 7;
                if (shift > 63) {
                    cetcd_delete_range_request_clear(out);
                    return CETCD_ERR_INVAL;
                }
            }
            if (!got) {
                cetcd_delete_range_request_clear(out);
                return CETCD_ERR_INVAL;
            }
            out->prev_kv = v != 0;
            continue;
        }
        if ((tag & 7) == 0) {
            int shift = 0;
            int got = 0;
            while (p < len) {
                uint8_t b = req[p++];
                if ((b & 0x80) == 0) {
                    got = 1;
                    break;
                }
                shift += 7;
                if (shift > 63) {
                    cetcd_delete_range_request_clear(out);
                    return CETCD_ERR_INVAL;
                }
            }
            if (!got) {
                cetcd_delete_range_request_clear(out);
                return CETCD_ERR_INVAL;
            }
            continue;
        }
        if ((tag & 7) == 2) {
            uint64_t skip = 0;
            int shift = 0;
            int got = 0;
            while (p < len) {
                uint8_t b = req[p++];
                skip |= (uint64_t)(b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    got = 1;
                    break;
                }
                shift += 7;
                if (shift > 63) {
                    cetcd_delete_range_request_clear(out);
                    return CETCD_ERR_INVAL;
                }
            }
            if (!got || p + skip > len) {
                cetcd_delete_range_request_clear(out);
                return CETCD_ERR_INVAL;
            }
            p += (size_t)skip;
            continue;
        }
        cetcd_delete_range_request_clear(out);
        return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

void cetcd_auth_name_pass_request_clear(cetcd_auth_name_pass_request *r) {
    if (!r) return;
    free(r->name);
    free(r->password);
    memset(r, 0, sizeof(*r));
}

int cetcd_encode_auth_name_pass_request(const uint8_t *name, size_t name_len,
                                        const uint8_t *password,
                                        size_t password_len, uint8_t *out,
                                        size_t cap, size_t *n) {
    if (!out || !n || !name || name_len == 0) return CETCD_ERR_INVAL;
    size_t pos = 0;
    if (pos + 2 + name_len > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x0a;
    uint64_t lv = name_len;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(lv & 0x7fu);
        lv >>= 7;
        if (lv) b |= 0x80u;
        out[pos++] = b;
    } while (lv);
    if (pos + name_len > cap) return CETCD_ERR_OVERFLOW;
    memcpy(out + pos, name, name_len);
    pos += name_len;
    if (password && password_len) {
        if (pos + 2 + password_len > cap) return CETCD_ERR_OVERFLOW;
        out[pos++] = 0x12;
        lv = password_len;
        do {
            if (pos >= cap) return CETCD_ERR_OVERFLOW;
            uint8_t b = (uint8_t)(lv & 0x7fu);
            lv >>= 7;
            if (lv) b |= 0x80u;
            out[pos++] = b;
        } while (lv);
        if (pos + password_len > cap) return CETCD_ERR_OVERFLOW;
        memcpy(out + pos, password, password_len);
        pos += password_len;
    }
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_auth_name_pass_request(const uint8_t *req, size_t len,
                                       cetcd_auth_name_pass_request *out) {
    size_t p = 0;
    if (!out) return CETCD_ERR_INVAL;
    memset(out, 0, sizeof(*out));
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x0a || tag == 0x12) {
            uint64_t skip = 0;
            int shift = 0;
            int got = 0;
            while (p < len) {
                uint8_t b = req[p++];
                skip |= (uint64_t)(b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    got = 1;
                    break;
                }
                shift += 7;
                if (shift > 63) {
                    cetcd_auth_name_pass_request_clear(out);
                    return CETCD_ERR_INVAL;
                }
            }
            if (!got || p + skip > len) {
                cetcd_auth_name_pass_request_clear(out);
                return CETCD_ERR_INVAL;
            }
            if (skip == 0) {
                if (tag == 0x0a) {
                    free(out->name);
                    out->name = NULL;
                    out->name_len = 0;
                } else {
                    free(out->password);
                    out->password = NULL;
                    out->password_len = 0;
                }
                continue;
            }
            uint8_t *copy = (uint8_t *)malloc((size_t)skip + 1);
            if (!copy) {
                cetcd_auth_name_pass_request_clear(out);
                return CETCD_ERR_NOMEM;
            }
            memcpy(copy, req + p, (size_t)skip);
            copy[skip] = 0;
            p += (size_t)skip;
            if (tag == 0x0a) {
                free(out->name);
                out->name = copy;
                out->name_len = (size_t)skip;
            } else {
                free(out->password);
                out->password = copy;
                out->password_len = (size_t)skip;
            }
            continue;
        }
        if ((tag & 7) == 0) {
            int shift = 0;
            int got = 0;
            while (p < len) {
                uint8_t b = req[p++];
                if ((b & 0x80) == 0) {
                    got = 1;
                    break;
                }
                shift += 7;
                if (shift > 63) {
                    cetcd_auth_name_pass_request_clear(out);
                    return CETCD_ERR_INVAL;
                }
            }
            if (!got) {
                cetcd_auth_name_pass_request_clear(out);
                return CETCD_ERR_INVAL;
            }
            continue;
        }
        if ((tag & 7) == 2) {
            uint64_t skip = 0;
            int shift = 0;
            int got = 0;
            while (p < len) {
                uint8_t b = req[p++];
                skip |= (uint64_t)(b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    got = 1;
                    break;
                }
                shift += 7;
                if (shift > 63) {
                    cetcd_auth_name_pass_request_clear(out);
                    return CETCD_ERR_INVAL;
                }
            }
            if (!got || p + skip > len) {
                cetcd_auth_name_pass_request_clear(out);
                return CETCD_ERR_INVAL;
            }
            p += (size_t)skip;
            continue;
        }
        cetcd_auth_name_pass_request_clear(out);
        return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

void cetcd_auth_name_request_clear(cetcd_auth_name_request *r) {
    if (!r) return;
    free(r->name);
    memset(r, 0, sizeof(*r));
}

int cetcd_encode_auth_name_request(const uint8_t *name, size_t name_len,
                                   uint8_t *out, size_t cap, size_t *n) {
    return cetcd_encode_auth_name_pass_request(name, name_len, NULL, 0,
                                               out, cap, n);
}

int cetcd_parse_auth_name_request(const uint8_t *req, size_t len,
                                  cetcd_auth_name_request *out) {
    cetcd_auth_name_pass_request ap;
    int rc;
    if (!out) return CETCD_ERR_INVAL;
    memset(out, 0, sizeof(*out));
    rc = cetcd_parse_auth_name_pass_request(req, len, &ap);
    if (rc != CETCD_OK) return rc;
    out->name = ap.name;
    out->name_len = ap.name_len;
    ap.name = NULL;
    cetcd_auth_name_pass_request_clear(&ap);
    return CETCD_OK;
}

void cetcd_auth_role_perm_request_clear(cetcd_auth_role_perm_request *r) {
    if (!r) return;
    free(r->name);
    free(r->key);
    free(r->range_end);
    memset(r, 0, sizeof(*r));
}

static int leftover_safe_varint_at_(const uint8_t *req, size_t len, size_t *p,
                                    uint64_t *out) {
    uint64_t v = 0;
    int shift = 0;
    int got = 0;
    if (!req || !p) return CETCD_ERR_INVAL;
    while (*p < len) {
        uint8_t b = req[(*p)++];
        v |= (uint64_t)(b & 0x7F) << shift;
        if ((b & 0x80) == 0) {
            got = 1;
            break;
        }
        shift += 7;
        if (shift > 63) return CETCD_ERR_INVAL;
    }
    if (!got) return CETCD_ERR_INVAL;
    if (out) *out = v;
    return CETCD_OK;
}

static int leftover_safe_copy_bytes_at_(const uint8_t *req, size_t len,
                                        size_t *p, uint8_t **out,
                                        size_t *out_len) {
    uint64_t skip = 0;
    int rc = leftover_safe_varint_at_(req, len, p, &skip);
    if (rc != CETCD_OK) return rc;
    if (*p + skip > len) return CETCD_ERR_INVAL;
    free(*out);
    if (skip == 0) {
        *out = NULL;
        *out_len = 0;
        return CETCD_OK;
    }
    uint8_t *copy = (uint8_t *)malloc((size_t)skip + 1);
    if (!copy) {
        *out = NULL;
        *out_len = 0;
        return CETCD_ERR_NOMEM;
    }
    memcpy(copy, req + *p, (size_t)skip);
    copy[skip] = 0;
    *p += (size_t)skip;
    *out = copy;
    *out_len = (size_t)skip;
    return CETCD_OK;
}

static int leftover_safe_skip_unknown_at_(const uint8_t *req, size_t len,
                                          size_t *p, uint8_t tag) {
    if ((tag & 7) == 0)
        return leftover_safe_varint_at_(req, len, p, NULL);
    if ((tag & 7) == 2) {
        uint64_t skip = 0;
        int rc = leftover_safe_varint_at_(req, len, p, &skip);
        if (rc != CETCD_OK) return rc;
        if (*p + skip > len) return CETCD_ERR_INVAL;
        *p += (size_t)skip;
        return CETCD_OK;
    }
    return CETCD_ERR_INVAL;
}

static int write_bytes_field_(uint8_t *out, size_t cap, size_t *pos,
                              uint8_t tag, const uint8_t *s, size_t n) {
    uint64_t lv;
    if (!s || n == 0) return CETCD_OK;
    if (*pos + 2 + n > cap) return CETCD_ERR_OVERFLOW;
    out[(*pos)++] = tag;
    lv = n;
    do {
        if (*pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(lv & 0x7fu);
        lv >>= 7;
        if (lv) b |= 0x80u;
        out[(*pos)++] = b;
    } while (lv);
    if (*pos + n > cap) return CETCD_ERR_OVERFLOW;
    memcpy(out + *pos, s, n);
    *pos += n;
    return CETCD_OK;
}

int cetcd_encode_auth_role_grant_perm_request(const uint8_t *name,
                                              size_t name_len, int perm_type,
                                              const uint8_t *key,
                                              size_t key_len,
                                              const uint8_t *range_end,
                                              size_t range_end_len,
                                              uint8_t *out, size_t cap,
                                              size_t *n) {
    uint8_t perm[256];
    size_t ppos = 0;
    size_t pos = 0;
    uint64_t lv;
    int rc;
    if (!out || !n || !name || name_len == 0) return CETCD_ERR_INVAL;
    perm[ppos++] = 0x08;
    lv = (uint64_t)(uint32_t)perm_type;
    do {
        if (ppos >= sizeof(perm)) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(lv & 0x7fu);
        lv >>= 7;
        if (lv) b |= 0x80u;
        perm[ppos++] = b;
    } while (lv);
    rc = write_bytes_field_(perm, sizeof(perm), &ppos, 0x12, key, key_len);
    if (rc != CETCD_OK) return rc;
    rc = write_bytes_field_(perm, sizeof(perm), &ppos, 0x1a, range_end,
                            range_end_len);
    if (rc != CETCD_OK) return rc;
    rc = write_bytes_field_(out, cap, &pos, 0x0a, name, name_len);
    if (rc != CETCD_OK) return rc;
    if (pos + 2 + ppos > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x12;
    lv = ppos;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(lv & 0x7fu);
        lv >>= 7;
        if (lv) b |= 0x80u;
        out[pos++] = b;
    } while (lv);
    if (pos + ppos > cap) return CETCD_ERR_OVERFLOW;
    memcpy(out + pos, perm, ppos);
    pos += ppos;
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_auth_role_grant_perm_request(const uint8_t *req, size_t len,
                                             cetcd_auth_role_perm_request *out) {
    size_t p = 0;
    if (!out) return CETCD_ERR_INVAL;
    memset(out, 0, sizeof(*out));
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x0a) {
            int rc = leftover_safe_copy_bytes_at_(req, len, &p, &out->name,
                                                  &out->name_len);
            if (rc != CETCD_OK) {
                cetcd_auth_role_perm_request_clear(out);
                return rc;
            }
            continue;
        }
        if (tag == 0x12) {
            uint64_t skip = 0;
            size_t ip = 0;
            int rc = leftover_safe_varint_at_(req, len, &p, &skip);
            if (rc != CETCD_OK || p + skip > len) {
                cetcd_auth_role_perm_request_clear(out);
                return CETCD_ERR_INVAL;
            }
            const uint8_t *pl = req + p;
            p += (size_t)skip;
            while (ip < (size_t)skip) {
                uint8_t ptag = pl[ip++];
                if (ptag == 0x00)
                    continue;
                if (ptag == 0x08) {
                    uint64_t v = 0;
                    if (leftover_safe_varint_at_(pl, (size_t)skip, &ip, &v)
                        != CETCD_OK) {
                        cetcd_auth_role_perm_request_clear(out);
                        return CETCD_ERR_INVAL;
                    }
                    out->perm_type = (int)v;
                    continue;
                }
                if (ptag == 0x12) {
                    if (leftover_safe_copy_bytes_at_(pl, (size_t)skip, &ip,
                                                     &out->key, &out->key_len)
                        != CETCD_OK) {
                        cetcd_auth_role_perm_request_clear(out);
                        return CETCD_ERR_INVAL;
                    }
                    continue;
                }
                if (ptag == 0x1a) {
                    if (leftover_safe_copy_bytes_at_(pl, (size_t)skip, &ip,
                                                     &out->range_end,
                                                     &out->range_end_len)
                        != CETCD_OK) {
                        cetcd_auth_role_perm_request_clear(out);
                        return CETCD_ERR_INVAL;
                    }
                    continue;
                }
                if (leftover_safe_skip_unknown_at_(pl, (size_t)skip, &ip, ptag)
                    != CETCD_OK) {
                    cetcd_auth_role_perm_request_clear(out);
                    return CETCD_ERR_INVAL;
                }
            }
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK) {
            cetcd_auth_role_perm_request_clear(out);
            return CETCD_ERR_INVAL;
        }
    }
    return CETCD_OK;
}

int cetcd_encode_auth_role_revoke_perm_request(const uint8_t *name,
                                               size_t name_len,
                                               const uint8_t *key,
                                               size_t key_len,
                                               const uint8_t *range_end,
                                               size_t range_end_len,
                                               uint8_t *out, size_t cap,
                                               size_t *n) {
    size_t pos = 0;
    int rc;
    if (!out || !n || !name || name_len == 0) return CETCD_ERR_INVAL;
    rc = write_bytes_field_(out, cap, &pos, 0x0a, name, name_len);
    if (rc != CETCD_OK) return rc;
    rc = write_bytes_field_(out, cap, &pos, 0x12, key, key_len);
    if (rc != CETCD_OK) return rc;
    rc = write_bytes_field_(out, cap, &pos, 0x1a, range_end, range_end_len);
    if (rc != CETCD_OK) return rc;
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_auth_role_revoke_perm_request(const uint8_t *req, size_t len,
                                              cetcd_auth_role_perm_request *out) {
    size_t p = 0;
    if (!out) return CETCD_ERR_INVAL;
    memset(out, 0, sizeof(*out));
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x0a) {
            int rc = leftover_safe_copy_bytes_at_(req, len, &p, &out->name,
                                                  &out->name_len);
            if (rc != CETCD_OK) {
                cetcd_auth_role_perm_request_clear(out);
                return rc;
            }
            continue;
        }
        if (tag == 0x12) {
            int rc = leftover_safe_copy_bytes_at_(req, len, &p, &out->key,
                                                  &out->key_len);
            if (rc != CETCD_OK) {
                cetcd_auth_role_perm_request_clear(out);
                return rc;
            }
            continue;
        }
        if (tag == 0x1a) {
            int rc = leftover_safe_copy_bytes_at_(req, len, &p, &out->range_end,
                                                  &out->range_end_len);
            if (rc != CETCD_OK) {
                cetcd_auth_role_perm_request_clear(out);
                return rc;
            }
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK) {
            cetcd_auth_role_perm_request_clear(out);
            return CETCD_ERR_INVAL;
        }
    }
    return CETCD_OK;
}

void cetcd_watch_create_request_clear(cetcd_watch_create_request *r) {
    if (!r) return;
    free(r->key);
    free(r->range_end);
    memset(r, 0, sizeof(*r));
}

int cetcd_encode_watch_create_request(const uint8_t *key, size_t key_len,
                                      int64_t start_rev, uint8_t *out,
                                      size_t cap, size_t *n) {
    size_t pos = 0;
    int rc;
    if (!out || !n || !key || key_len == 0 || start_rev < 0)
        return CETCD_ERR_INVAL;
    rc = write_bytes_field_(out, cap, &pos, 0x0a, key, key_len);
    if (rc != CETCD_OK) return rc;
    if (start_rev > 0) {
        uint64_t v;
        if (pos + 2 > cap) return CETCD_ERR_OVERFLOW;
        out[pos++] = 0x18;
        v = (uint64_t)start_rev;
        do {
            if (pos >= cap) return CETCD_ERR_OVERFLOW;
            uint8_t b = (uint8_t)(v & 0x7fu);
            v >>= 7;
            if (v) b |= 0x80u;
            out[pos++] = b;
        } while (v);
    }
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_watch_create_request(const uint8_t *req, size_t len,
                                     cetcd_watch_create_request *out) {
    size_t p = 0;
    if (!out) return CETCD_ERR_INVAL;
    memset(out, 0, sizeof(*out));
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x0a) {
            int rc = leftover_safe_copy_bytes_at_(req, len, &p, &out->key,
                                                  &out->key_len);
            if (rc != CETCD_OK) {
                cetcd_watch_create_request_clear(out);
                return rc;
            }
            continue;
        }
        if (tag == 0x12) {
            int rc = leftover_safe_copy_bytes_at_(req, len, &p, &out->range_end,
                                                  &out->range_end_len);
            if (rc != CETCD_OK) {
                cetcd_watch_create_request_clear(out);
                return rc;
            }
            continue;
        }
        if (tag == 0x18 || tag == 0x20 || tag == 0x28 || tag == 0x30 ||
            tag == 0x38 || tag == 0x40) {
            uint64_t v = 0;
            if (leftover_safe_varint_at_(req, len, &p, &v) != CETCD_OK) {
                cetcd_watch_create_request_clear(out);
                return CETCD_ERR_INVAL;
            }
            if (tag == 0x18) out->start_rev = (int64_t)v;
            else if (tag == 0x20) out->progress_notify = v != 0;
            else if (tag == 0x28) {
                if (v == 0) out->filter_noput = 1;
                else if (v == 1) out->filter_nodelete = 1;
            } else if (tag == 0x30) out->prev_kv = v != 0;
            else if (tag == 0x38) out->watch_id = (int64_t)v;
            else out->fragment = v != 0;
            continue;
        }
        if (tag == 0x2a) {
            uint64_t skip = 0;
            size_t ip = 0;
            if (leftover_safe_varint_at_(req, len, &p, &skip) != CETCD_OK ||
                p + skip > len) {
                cetcd_watch_create_request_clear(out);
                return CETCD_ERR_INVAL;
            }
            const uint8_t *pl = req + p;
            p += (size_t)skip;
            while (ip < (size_t)skip) {
                uint64_t fv = 0;
                if (leftover_safe_varint_at_(pl, (size_t)skip, &ip, &fv)
                    != CETCD_OK) {
                    cetcd_watch_create_request_clear(out);
                    return CETCD_ERR_INVAL;
                }
                if (fv == 0) out->filter_noput = 1;
                else if (fv == 1) out->filter_nodelete = 1;
            }
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK) {
            cetcd_watch_create_request_clear(out);
            return CETCD_ERR_INVAL;
        }
    }
    return CETCD_OK;
}

void cetcd_user_add_request_clear(cetcd_user_add_request *r) {
    if (!r) return;
    free(r->name);
    free(r->password);
    memset(r, 0, sizeof(*r));
}

int cetcd_encode_user_add_request(const uint8_t *name, size_t name_len,
                                  const uint8_t *password, size_t password_len,
                                  int no_password, uint8_t *out, size_t cap,
                                  size_t *n) {
    if (!out || !n || !name || name_len == 0) return CETCD_ERR_INVAL;
    size_t pos = 0;
    if (pos + 2 + name_len > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x0a;
    uint64_t lv = name_len;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(lv & 0x7fu);
        lv >>= 7;
        if (lv) b |= 0x80u;
        out[pos++] = b;
    } while (lv);
    if (pos + name_len > cap) return CETCD_ERR_OVERFLOW;
    memcpy(out + pos, name, name_len);
    pos += name_len;
    if (password && password_len) {
        if (pos + 2 + password_len > cap) return CETCD_ERR_OVERFLOW;
        out[pos++] = 0x12;
        lv = password_len;
        do {
            if (pos >= cap) return CETCD_ERR_OVERFLOW;
            uint8_t b = (uint8_t)(lv & 0x7fu);
            lv >>= 7;
            if (lv) b |= 0x80u;
            out[pos++] = b;
        } while (lv);
        if (pos + password_len > cap) return CETCD_ERR_OVERFLOW;
        memcpy(out + pos, password, password_len);
        pos += password_len;
    }
    if (no_password) {
        if (pos + 4 > cap) return CETCD_ERR_OVERFLOW;
        out[pos++] = 0x1a;
        out[pos++] = 0x02;
        out[pos++] = 0x08;
        out[pos++] = 0x01;
    }
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_user_add_request(const uint8_t *req, size_t len,
                                 cetcd_user_add_request *out) {
    size_t p = 0;
    if (!out) return CETCD_ERR_INVAL;
    memset(out, 0, sizeof(*out));
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x0a || tag == 0x12) {
            uint64_t skip = 0;
            int shift = 0;
            int got = 0;
            while (p < len) {
                uint8_t b = req[p++];
                skip |= (uint64_t)(b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    got = 1;
                    break;
                }
                shift += 7;
                if (shift > 63) {
                    cetcd_user_add_request_clear(out);
                    return CETCD_ERR_INVAL;
                }
            }
            if (!got || p + skip > len) {
                cetcd_user_add_request_clear(out);
                return CETCD_ERR_INVAL;
            }
            if (skip == 0) {
                if (tag == 0x0a) {
                    free(out->name);
                    out->name = NULL;
                    out->name_len = 0;
                } else {
                    free(out->password);
                    out->password = NULL;
                    out->password_len = 0;
                }
                continue;
            }
            uint8_t *copy = (uint8_t *)malloc((size_t)skip + 1);
            if (!copy) {
                cetcd_user_add_request_clear(out);
                return CETCD_ERR_NOMEM;
            }
            memcpy(copy, req + p, (size_t)skip);
            copy[skip] = 0;
            p += (size_t)skip;
            if (tag == 0x0a) {
                free(out->name);
                out->name = copy;
                out->name_len = (size_t)skip;
            } else {
                free(out->password);
                out->password = copy;
                out->password_len = (size_t)skip;
            }
            continue;
        }
        if (tag == 0x1a) {
            uint64_t skip = 0;
            int shift = 0;
            int got = 0;
            while (p < len) {
                uint8_t b = req[p++];
                skip |= (uint64_t)(b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    got = 1;
                    break;
                }
                shift += 7;
                if (shift > 63) {
                    cetcd_user_add_request_clear(out);
                    return CETCD_ERR_INVAL;
                }
            }
            if (!got || p + skip > len) {
                cetcd_user_add_request_clear(out);
                return CETCD_ERR_INVAL;
            }
            size_t oend = p + (size_t)skip;
            while (p < oend) {
                uint8_t otag = req[p++];
                if (otag == 0x00)
                    continue;
                if (otag == 0x08) {
                    uint64_t v = 0;
                    shift = 0;
                    got = 0;
                    while (p < oend) {
                        uint8_t b = req[p++];
                        v |= (uint64_t)(b & 0x7F) << shift;
                        if ((b & 0x80) == 0) {
                            got = 1;
                            break;
                        }
                        shift += 7;
                        if (shift > 63) {
                            cetcd_user_add_request_clear(out);
                            return CETCD_ERR_INVAL;
                        }
                    }
                    if (!got) {
                        cetcd_user_add_request_clear(out);
                        return CETCD_ERR_INVAL;
                    }
                    out->no_password = v != 0;
                    continue;
                }
                if ((otag & 7) == 0) {
                    got = 0;
                    shift = 0;
                    while (p < oend) {
                        uint8_t b = req[p++];
                        if ((b & 0x80) == 0) {
                            got = 1;
                            break;
                        }
                        shift += 7;
                        if (shift > 63) {
                            cetcd_user_add_request_clear(out);
                            return CETCD_ERR_INVAL;
                        }
                    }
                    if (!got) {
                        cetcd_user_add_request_clear(out);
                        return CETCD_ERR_INVAL;
                    }
                    continue;
                }
                if ((otag & 7) == 2) {
                    uint64_t iskip = 0;
                    shift = 0;
                    got = 0;
                    while (p < oend) {
                        uint8_t b = req[p++];
                        iskip |= (uint64_t)(b & 0x7F) << shift;
                        if ((b & 0x80) == 0) {
                            got = 1;
                            break;
                        }
                        shift += 7;
                        if (shift > 63) {
                            cetcd_user_add_request_clear(out);
                            return CETCD_ERR_INVAL;
                        }
                    }
                    if (!got || p + iskip > oend) {
                        cetcd_user_add_request_clear(out);
                        return CETCD_ERR_INVAL;
                    }
                    p += (size_t)iskip;
                    continue;
                }
                cetcd_user_add_request_clear(out);
                return CETCD_ERR_INVAL;
            }
            continue;
        }
        if ((tag & 7) == 0) {
            int shift = 0;
            int got = 0;
            while (p < len) {
                uint8_t b = req[p++];
                if ((b & 0x80) == 0) {
                    got = 1;
                    break;
                }
                shift += 7;
                if (shift > 63) {
                    cetcd_user_add_request_clear(out);
                    return CETCD_ERR_INVAL;
                }
            }
            if (!got) {
                cetcd_user_add_request_clear(out);
                return CETCD_ERR_INVAL;
            }
            continue;
        }
        if ((tag & 7) == 2) {
            uint64_t skip = 0;
            int shift = 0;
            int got = 0;
            while (p < len) {
                uint8_t b = req[p++];
                skip |= (uint64_t)(b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    got = 1;
                    break;
                }
                shift += 7;
                if (shift > 63) {
                    cetcd_user_add_request_clear(out);
                    return CETCD_ERR_INVAL;
                }
            }
            if (!got || p + skip > len) {
                cetcd_user_add_request_clear(out);
                return CETCD_ERR_INVAL;
            }
            p += (size_t)skip;
            continue;
        }
        cetcd_user_add_request_clear(out);
        return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

void cetcd_txn_compare_clear(cetcd_txn_compare *c) {
    if (!c) return;
    free(c->key);
    free(c->value);
    free(c->range_end);
    memset(c, 0, sizeof(*c));
}

int cetcd_encode_txn_compare(const uint8_t *key, size_t key_len, int result,
                             int target, int64_t version, uint8_t *out,
                             size_t cap, size_t *n) {
    if (!out || !n || !key || key_len == 0) return CETCD_ERR_INVAL;
    size_t pos = 0;
    if (result) {
        if (pos + 2 > cap) return CETCD_ERR_OVERFLOW;
        out[pos++] = 0x08;
        out[pos++] = (uint8_t)result;
    }
    if (target) {
        if (pos + 2 > cap) return CETCD_ERR_OVERFLOW;
        out[pos++] = 0x10;
        out[pos++] = (uint8_t)target;
    }
    if (pos + 2 + key_len > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x1a;
    uint64_t lv = key_len;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(lv & 0x7fu);
        lv >>= 7;
        if (lv) b |= 0x80u;
        out[pos++] = b;
    } while (lv);
    if (pos + key_len > cap) return CETCD_ERR_OVERFLOW;
    memcpy(out + pos, key, key_len);
    pos += key_len;
    if (version) {
        if (pos + 2 > cap) return CETCD_ERR_OVERFLOW;
        out[pos++] = 0x20;
        uint64_t v = (uint64_t)version;
        do {
            if (pos >= cap) return CETCD_ERR_OVERFLOW;
            uint8_t b = (uint8_t)(v & 0x7fu);
            v >>= 7;
            if (v) b |= 0x80u;
            out[pos++] = b;
        } while (v);
    }
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_txn_compare(const uint8_t *req, size_t len,
                            cetcd_txn_compare *out) {
    size_t p = 0;
    if (!out) return CETCD_ERR_INVAL;
    memset(out, 0, sizeof(*out));
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x08 || tag == 0x10 || tag == 0x20 || tag == 0x28 ||
            tag == 0x30 || tag == 0x40) {
            uint64_t v = 0;
            int shift = 0;
            int got = 0;
            while (p < len) {
                uint8_t b = req[p++];
                v |= (uint64_t)(b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    got = 1;
                    break;
                }
                shift += 7;
                if (shift > 63) {
                    cetcd_txn_compare_clear(out);
                    return CETCD_ERR_INVAL;
                }
            }
            if (!got) {
                cetcd_txn_compare_clear(out);
                return CETCD_ERR_INVAL;
            }
            if (tag == 0x08) out->result = (int)v;
            else if (tag == 0x10) out->target = (int)v;
            else if (tag == 0x20) out->version = (int64_t)v;
            else if (tag == 0x28) out->create_revision = (int64_t)v;
            else if (tag == 0x30) out->mod_revision = (int64_t)v;
            else out->lease = (int64_t)v;
            continue;
        }
        if (tag == 0x1a || tag == 0x3a || tag == 0x4a) {
            uint64_t skip = 0;
            int shift = 0;
            int got = 0;
            while (p < len) {
                uint8_t b = req[p++];
                skip |= (uint64_t)(b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    got = 1;
                    break;
                }
                shift += 7;
                if (shift > 63) {
                    cetcd_txn_compare_clear(out);
                    return CETCD_ERR_INVAL;
                }
            }
            if (!got || p + skip > len) {
                cetcd_txn_compare_clear(out);
                return CETCD_ERR_INVAL;
            }
            if (skip == 0) {
                if (tag == 0x1a) {
                    free(out->key); out->key = NULL; out->key_len = 0;
                } else if (tag == 0x3a) {
                    free(out->value); out->value = NULL; out->value_len = 0;
                } else {
                    free(out->range_end); out->range_end = NULL;
                    out->range_end_len = 0;
                }
                continue;
            }
            uint8_t *copy = (uint8_t *)malloc((size_t)skip);
            if (!copy) {
                cetcd_txn_compare_clear(out);
                return CETCD_ERR_NOMEM;
            }
            memcpy(copy, req + p, (size_t)skip);
            p += (size_t)skip;
            if (tag == 0x1a) {
                free(out->key); out->key = copy; out->key_len = (size_t)skip;
            } else if (tag == 0x3a) {
                free(out->value); out->value = copy; out->value_len = (size_t)skip;
            } else {
                free(out->range_end); out->range_end = copy;
                out->range_end_len = (size_t)skip;
            }
            continue;
        }
        if ((tag & 7) == 0) {
            int shift = 0;
            int got = 0;
            while (p < len) {
                uint8_t b = req[p++];
                if ((b & 0x80) == 0) {
                    got = 1;
                    break;
                }
                shift += 7;
                if (shift > 63) {
                    cetcd_txn_compare_clear(out);
                    return CETCD_ERR_INVAL;
                }
            }
            if (!got) {
                cetcd_txn_compare_clear(out);
                return CETCD_ERR_INVAL;
            }
            continue;
        }
        if ((tag & 7) == 2) {
            uint64_t skip = 0;
            int shift = 0;
            int got = 0;
            while (p < len) {
                uint8_t b = req[p++];
                skip |= (uint64_t)(b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    got = 1;
                    break;
                }
                shift += 7;
                if (shift > 63) {
                    cetcd_txn_compare_clear(out);
                    return CETCD_ERR_INVAL;
                }
            }
            if (!got || p + skip > len) {
                cetcd_txn_compare_clear(out);
                return CETCD_ERR_INVAL;
            }
            p += (size_t)skip;
            continue;
        }
        cetcd_txn_compare_clear(out);
        return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_parse_txn_request(const uint8_t *req, size_t len,
                            size_t *n_compare, size_t *n_success,
                            size_t *n_failure) {
    size_t p = 0;
    if (!n_compare || !n_success || !n_failure) return CETCD_ERR_INVAL;
    *n_compare = 0;
    *n_success = 0;
    *n_failure = 0;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x0a || tag == 0x12 || tag == 0x1a) {
            uint64_t skip = 0;
            int shift = 0;
            int got = 0;
            while (p < len) {
                uint8_t b = req[p++];
                skip |= (uint64_t)(b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    got = 1;
                    break;
                }
                shift += 7;
                if (shift > 63) return CETCD_ERR_INVAL;
            }
            if (!got || p + skip > len) return CETCD_ERR_INVAL;
            p += (size_t)skip;
            if (tag == 0x0a) (*n_compare)++;
            else if (tag == 0x12) (*n_success)++;
            else (*n_failure)++;
            continue;
        }
        if ((tag & 7) == 0) {
            int shift = 0;
            int got = 0;
            while (p < len) {
                uint8_t b = req[p++];
                if ((b & 0x80) == 0) {
                    got = 1;
                    break;
                }
                shift += 7;
                if (shift > 63) return CETCD_ERR_INVAL;
            }
            if (!got) return CETCD_ERR_INVAL;
            continue;
        }
        if ((tag & 7) == 2) {
            uint64_t skip = 0;
            int shift = 0;
            int got = 0;
            while (p < len) {
                uint8_t b = req[p++];
                skip |= (uint64_t)(b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    got = 1;
                    break;
                }
                shift += 7;
                if (shift > 63) return CETCD_ERR_INVAL;
            }
            if (!got || p + skip > len) return CETCD_ERR_INVAL;
            p += (size_t)skip;
            continue;
        }
        return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_member_id_request(uint64_t id, uint8_t *out, size_t cap,
                                   size_t *n) {
    if (!out || !n || cap == 0) return CETCD_ERR_INVAL;
    if (id == 0) return CETCD_ERR_INVAL;
    size_t pos = 0;
    if (pos >= cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x08;
    uint64_t v = id;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_member_id_request(const uint8_t *req, size_t len,
                                  uint64_t *out) {
    size_t p = 0;
    if (!out) return CETCD_ERR_INVAL;
    *out = 0;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x08) {
            uint64_t v = 0;
            int shift = 0;
            int got = 0;
            while (p < len) {
                uint8_t b = req[p++];
                v |= (uint64_t)(b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    got = 1;
                    break;
                }
                shift += 7;
                if (shift > 63) return CETCD_ERR_INVAL;
            }
            if (!got) return CETCD_ERR_INVAL;
            *out = v;
            continue;
        }
        if ((tag & 7) == 0) {
            int shift = 0;
            int got = 0;
            while (p < len) {
                uint8_t b = req[p++];
                if ((b & 0x80) == 0) {
                    got = 1;
                    break;
                }
                shift += 7;
                if (shift > 63) return CETCD_ERR_INVAL;
            }
            if (!got) return CETCD_ERR_INVAL;
            continue;
        }
        if ((tag & 7) == 2) {
            uint64_t skip = 0;
            int shift = 0;
            int got = 0;
            while (p < len) {
                uint8_t b = req[p++];
                skip |= (uint64_t)(b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    got = 1;
                    break;
                }
                shift += 7;
                if (shift > 63) return CETCD_ERR_INVAL;
            }
            if (!got || p + skip > len) return CETCD_ERR_INVAL;
            p += (size_t)skip;
            continue;
        }
        return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_member_update_request(uint64_t id, const char *url,
                                       uint8_t *out, size_t cap, size_t *n) {
    if (!out || !n || cap == 0) return CETCD_ERR_INVAL;
    if (id == 0 || !url || !url[0]) return CETCD_ERR_INVAL;
    size_t pos = 0;
    if (pos >= cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x08;
    uint64_t v = id;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    size_t ulen = strlen(url);
    if (pos + 1 >= cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x12;
    uint64_t lv = ulen;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(lv & 0x7fu);
        lv >>= 7;
        if (lv) b |= 0x80u;
        out[pos++] = b;
    } while (lv);
    if (pos + ulen > cap) return CETCD_ERR_OVERFLOW;
    memcpy(out + pos, url, ulen);
    pos += ulen;
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_member_update_request(const uint8_t *req, size_t len,
                                      uint64_t *id, char *url, size_t url_cap) {
    size_t p = 0;
    if (!id) return CETCD_ERR_INVAL;
    *id = 0;
    if (url && url_cap)
        url[0] = '\0';
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x08) {
            uint64_t v = 0;
            int shift = 0;
            int got = 0;
            while (p < len) {
                uint8_t b = req[p++];
                v |= (uint64_t)(b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    got = 1;
                    break;
                }
                shift += 7;
                if (shift > 63) return CETCD_ERR_INVAL;
            }
            if (!got) return CETCD_ERR_INVAL;
            *id = v;
            continue;
        }
        if (tag == 0x12) {
            uint64_t skip = 0;
            int shift = 0;
            int got = 0;
            while (p < len) {
                uint8_t b = req[p++];
                skip |= (uint64_t)(b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    got = 1;
                    break;
                }
                shift += 7;
                if (shift > 63) return CETCD_ERR_INVAL;
            }
            if (!got || p + skip > len) return CETCD_ERR_INVAL;
            if (url && url_cap) {
                size_t copy = (size_t)skip < url_cap - 1 ? (size_t)skip : url_cap - 1;
                memcpy(url, req + p, copy);
                url[copy] = '\0';
            }
            p += (size_t)skip;
            continue;
        }
        if ((tag & 7) == 0) {
            int shift = 0;
            int got = 0;
            while (p < len) {
                uint8_t b = req[p++];
                if ((b & 0x80) == 0) {
                    got = 1;
                    break;
                }
                shift += 7;
                if (shift > 63) return CETCD_ERR_INVAL;
            }
            if (!got) return CETCD_ERR_INVAL;
            continue;
        }
        if ((tag & 7) == 2) {
            uint64_t skip = 0;
            int shift = 0;
            int got = 0;
            while (p < len) {
                uint8_t b = req[p++];
                skip |= (uint64_t)(b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    got = 1;
                    break;
                }
                shift += 7;
                if (shift > 63) return CETCD_ERR_INVAL;
            }
            if (!got || p + skip > len) return CETCD_ERR_INVAL;
            p += (size_t)skip;
            continue;
        }
        return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_member_add_request(const char *url, int is_learner,
                                    uint8_t *out, size_t cap, size_t *n) {
    size_t pos = 0;
    size_t ulen;
    int rc;
    if (!out || !n || !url || !url[0]) return CETCD_ERR_INVAL;
    ulen = strlen(url);
    rc = write_bytes_field_(out, cap, &pos, 0x0a, (const uint8_t *)url, ulen);
    if (rc != CETCD_OK) return rc;
    if (is_learner) {
        if (pos + 2 > cap) return CETCD_ERR_OVERFLOW;
        out[pos++] = 0x10;
        out[pos++] = 0x01;
    }
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_member_add_request(const uint8_t *req, size_t len,
                                   char *url, size_t url_cap, int *is_learner) {
    size_t p = 0;
    if (url && url_cap)
        url[0] = '\0';
    if (is_learner)
        *is_learner = 0;
    if (!url && !is_learner) return CETCD_ERR_INVAL;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x0a) {
            uint64_t skip = 0;
            int rc = leftover_safe_varint_at_(req, len, &p, &skip);
            if (rc != CETCD_OK || p + skip > len) return CETCD_ERR_INVAL;
            if (url && url_cap) {
                size_t copy = (size_t)skip < url_cap - 1 ? (size_t)skip : url_cap - 1;
                memcpy(url, req + p, copy);
                url[copy] = '\0';
            }
            p += (size_t)skip;
            continue;
        }
        if (tag == 0x10) {
            uint64_t v = 0;
            if (leftover_safe_varint_at_(req, len, &p, &v) != CETCD_OK)
                return CETCD_ERR_INVAL;
            if (is_learner) *is_learner = v != 0;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_downgrade_request(int action, const char *version,
                                   uint8_t *out, size_t cap, size_t *n) {
    size_t pos = 0;
    uint64_t v;
    if (!out || !n) return CETCD_ERR_INVAL;
    if (action < 0) return CETCD_ERR_INVAL;
    if (pos + 2 > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x08;
    v = (uint64_t)action;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    if (version && version[0]) {
        int rc = write_bytes_field_(out, cap, &pos, 0x12,
                                    (const uint8_t *)version, strlen(version));
        if (rc != CETCD_OK) return rc;
    }
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_downgrade_request(const uint8_t *req, size_t len,
                                  int *action, char *version,
                                  size_t version_cap) {
    size_t p = 0;
    if (action) *action = 0;
    if (version && version_cap)
        version[0] = '\0';
    if (!action && !version) return CETCD_ERR_INVAL;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x08) {
            uint64_t v = 0;
            if (leftover_safe_varint_at_(req, len, &p, &v) != CETCD_OK)
                return CETCD_ERR_INVAL;
            if (v > (uint64_t)INT32_MAX) return CETCD_ERR_INVAL;
            if (action) *action = (int)v;
            continue;
        }
        if (tag == 0x12) {
            uint64_t skip = 0;
            int rc = leftover_safe_varint_at_(req, len, &p, &skip);
            if (rc != CETCD_OK || p + skip > len) return CETCD_ERR_INVAL;
            if (version && version_cap) {
                if (skip >= version_cap) return CETCD_ERR_INVAL;
                memcpy(version, req + p, (size_t)skip);
                version[skip] = '\0';
            }
            p += (size_t)skip;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_downgrade_response(const char *version, uint8_t *out,
                                    size_t cap, size_t *n) {
    size_t pos = 0;
    int rc;
    if (!out || !n || !version || !version[0]) return CETCD_ERR_INVAL;
    rc = write_bytes_field_(out, cap, &pos, 0x12,
                            (const uint8_t *)version, strlen(version));
    if (rc != CETCD_OK) return rc;
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_downgrade_response(const uint8_t *req, size_t len,
                                   char *version, size_t version_cap) {
    size_t p = 0;
    if (version && version_cap)
        version[0] = '\0';
    if (!version) return CETCD_ERR_INVAL;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x12) {
            uint64_t skip = 0;
            int rc = leftover_safe_varint_at_(req, len, &p, &skip);
            if (rc != CETCD_OK || p + skip > len) return CETCD_ERR_INVAL;
            if (version_cap) {
                if (skip >= version_cap) return CETCD_ERR_INVAL;
                memcpy(version, req + p, (size_t)skip);
                version[skip] = '\0';
            }
            p += (size_t)skip;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_txn_op_put(const uint8_t *key, size_t key_len,
                            uint8_t *out, size_t cap, size_t *n) {
    uint8_t put[64];
    size_t put_n = 0;
    size_t pos = 0;
    uint64_t lv;
    int rc;
    if (!out || !n || !key || key_len == 0) return CETCD_ERR_INVAL;
    rc = write_bytes_field_(put, sizeof(put), &put_n, 0x0a, key, key_len);
    if (rc != CETCD_OK) return rc;
    if (pos + 2 + put_n > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x12; /* RequestPut */
    lv = put_n;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(lv & 0x7fu);
        lv >>= 7;
        if (lv) b |= 0x80u;
        out[pos++] = b;
    } while (lv);
    if (pos + put_n > cap) return CETCD_ERR_OVERFLOW;
    memcpy(out + pos, put, put_n);
    pos += put_n;
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_txn_op_key(const uint8_t *op, size_t len, int *want_write,
                           char *key, size_t key_cap) {
    size_t p = 0;
    if (want_write) *want_write = 1;
    if (key && key_cap)
        key[0] = '\0';
    if (!want_write && !key) return CETCD_ERR_INVAL;
    if (!op || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = op[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x0a || tag == 0x12 || tag == 0x1a) {
            uint64_t n = 0;
            size_t ip;
            const uint8_t *pl;
            int rc = leftover_safe_varint_at_(op, len, &p, &n);
            if (rc != CETCD_OK || p + n > len) return CETCD_ERR_INVAL;
            pl = op + p;
            p += (size_t)n;
            if (want_write) *want_write = (tag != 0x0a);
            ip = 0;
            while (ip < (size_t)n) {
                uint8_t t = pl[ip++];
                if (t == 0x00)
                    continue;
                if (t == 0x0a) {
                    uint64_t skip = 0;
                    rc = leftover_safe_varint_at_(pl, (size_t)n, &ip, &skip);
                    if (rc != CETCD_OK || ip + skip > (size_t)n)
                        return CETCD_ERR_INVAL;
                    if (key && key_cap) {
                        if (skip >= key_cap) return CETCD_ERR_INVAL;
                        memcpy(key, pl + ip, (size_t)skip);
                        key[skip] = '\0';
                    }
                    ip += (size_t)skip;
                    continue;
                }
                if (leftover_safe_skip_unknown_at_(pl, (size_t)n, &ip, t)
                    != CETCD_OK)
                    return CETCD_ERR_INVAL;
            }
            continue;
        }
        if (leftover_safe_skip_unknown_at_(op, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_leftover_safe_skip_field(const uint8_t *buf, size_t len,
                                   size_t *pos, uint8_t tag) {
    if (!buf || !pos) return CETCD_ERR_INVAL;
    if (tag == 0x00)
        return CETCD_OK;
    return leftover_safe_skip_unknown_at_(buf, len, pos, tag);
}

int cetcd_encode_alarm_response_member(uint64_t member_id, int alarm,
                                       uint8_t *out, size_t cap, size_t *n) {
    uint8_t inner[24];
    size_t in = 0;
    uint64_t v;
    size_t pos = 0;
    if (!out || !n || alarm < 0) return CETCD_ERR_INVAL;
    inner[in++] = 0x08;
    v = member_id;
    do {
        if (in >= sizeof(inner)) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        inner[in++] = b;
    } while (v);
    inner[in++] = 0x10;
    v = (uint64_t)alarm;
    do {
        if (in >= sizeof(inner)) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        inner[in++] = b;
    } while (v);
    if (pos + 2 + in > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x12; /* field 2 alarms */
    v = in;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    memcpy(out + pos, inner, in);
    pos += in;
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_alarm_response(const uint8_t *req, size_t len,
                               cetcd_alarm_member *out, size_t cap,
                               size_t *n) {
    size_t p = 0;
    if (!n) return CETCD_ERR_INVAL;
    *n = 0;
    if (!out && cap) return CETCD_ERR_INVAL;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x12) {
            uint64_t skip = 0;
            size_t ip = 0;
            cetcd_alarm_member cur;
            int rc = leftover_safe_varint_at_(req, len, &p, &skip);
            if (rc != CETCD_OK || p + skip > len) return CETCD_ERR_INVAL;
            memset(&cur, 0, sizeof(cur));
            while (ip < (size_t)skip) {
                uint8_t at = req[p + ip];
                ip++;
                if (at == 0x00)
                    continue;
                if (at == 0x08 || at == 0x10) {
                    uint64_t v = 0;
                    size_t qp = ip;
                    if (leftover_safe_varint_at_(req + p, (size_t)skip, &qp, &v)
                        != CETCD_OK)
                        return CETCD_ERR_INVAL;
                    ip = qp;
                    if (at == 0x08) cur.member_id = v;
                    else {
                        if (v > (uint64_t)INT_MAX) return CETCD_ERR_INVAL;
                        cur.alarm = (int)v;
                    }
                    continue;
                }
                if (leftover_safe_skip_unknown_at_(req + p, (size_t)skip, &ip,
                                                   at) != CETCD_OK)
                    return CETCD_ERR_INVAL;
            }
            if (out && *n < cap)
                out[*n] = cur;
            (*n)++;
            p += (size_t)skip;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_range_response_kv(const uint8_t *key, size_t key_len,
                                   const uint8_t *val, size_t val_len,
                                   uint8_t *out, size_t cap, size_t *n) {
    uint8_t kv[128];
    size_t kn = 0;
    size_t pos = 0;
    uint64_t lv;
    int rc;
    if (!out || !n || !key || key_len == 0) return CETCD_ERR_INVAL;
    rc = write_bytes_field_(kv, sizeof(kv), &kn, 0x0a, key, key_len);
    if (rc != CETCD_OK) return rc;
    if (val && val_len) {
        rc = write_bytes_field_(kv, sizeof(kv), &kn, 0x2a, val, val_len);
        if (rc != CETCD_OK) return rc;
    }
    if (pos + 2 + kn > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x12; /* field 2 kvs */
    lv = kn;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(lv & 0x7fu);
        lv >>= 7;
        if (lv) b |= 0x80u;
        out[pos++] = b;
    } while (lv);
    memcpy(out + pos, kv, kn);
    pos += kn;
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_range_response_kv(const uint8_t *req, size_t len,
                                  char *key, size_t key_cap, char *value,
                                  size_t value_cap, size_t *n_kvs) {
    size_t p = 0;
    if (key && key_cap)
        key[0] = '\0';
    if (value && value_cap)
        value[0] = '\0';
    if (n_kvs) *n_kvs = 0;
    if (!key && !value && !n_kvs) return CETCD_ERR_INVAL;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x12) {
            uint64_t skip = 0;
            size_t ip = 0;
            int rc = leftover_safe_varint_at_(req, len, &p, &skip);
            if (rc != CETCD_OK || p + skip > len) return CETCD_ERR_INVAL;
            if (n_kvs) (*n_kvs)++;
            while (ip < (size_t)skip) {
                uint8_t kt = req[p + ip];
                ip++;
                if (kt == 0x00)
                    continue;
                if (kt == 0x0a || kt == 0x2a) {
                    uint64_t sl = 0;
                    size_t qp = ip;
                    if (leftover_safe_varint_at_(req + p, (size_t)skip, &qp, &sl)
                        != CETCD_OK || qp + sl > (size_t)skip)
                        return CETCD_ERR_INVAL;
                    if (kt == 0x0a && key && key_cap) {
                        if (sl >= key_cap) return CETCD_ERR_INVAL;
                        memcpy(key, req + p + qp, (size_t)sl);
                        key[sl] = '\0';
                    }
                    if (kt == 0x2a && value && value_cap) {
                        if (sl >= value_cap) return CETCD_ERR_INVAL;
                        memcpy(value, req + p + qp, (size_t)sl);
                        value[sl] = '\0';
                    }
                    ip = qp + (size_t)sl;
                    continue;
                }
                if (leftover_safe_skip_unknown_at_(req + p, (size_t)skip, &ip,
                                                   kt) != CETCD_OK)
                    return CETCD_ERR_INVAL;
            }
            p += (size_t)skip;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_range_response_kv_lease(int64_t lease, uint8_t *out,
                                         size_t cap, size_t *n) {
    uint8_t inner[16];
    size_t in = 0;
    uint64_t v;
    size_t pos = 0;
    if (!out || !n) return CETCD_ERR_INVAL;
    *n = 0;
    if (lease == 0) return CETCD_OK;
    if (in + 2 > sizeof(inner)) return CETCD_ERR_OVERFLOW;
    inner[in++] = 0x30;
    v = (uint64_t)lease;
    do {
        if (in >= sizeof(inner)) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        inner[in++] = b;
    } while (v);
    if (pos + 2 + in > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x12; /* field 2 kvs */
    v = in;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    memcpy(out + pos, inner, in);
    pos += in;
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_range_response_kv_lease(const uint8_t *req, size_t len,
                                        int64_t *lease) {
    size_t p = 0;
    if (!lease) return CETCD_ERR_INVAL;
    *lease = 0;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x12) {
            uint64_t skip = 0;
            size_t ip = 0;
            int rc = leftover_safe_varint_at_(req, len, &p, &skip);
            if (rc != CETCD_OK || p + skip > len) return CETCD_ERR_INVAL;
            *lease = 0;
            while (ip < (size_t)skip) {
                uint8_t kt = req[p + ip];
                ip++;
                if (kt == 0x00)
                    continue;
                if (kt == 0x30) {
                    uint64_t v = 0;
                    size_t qp = ip;
                    if (leftover_safe_varint_at_(req + p, (size_t)skip, &qp, &v)
                        != CETCD_OK)
                        return CETCD_ERR_INVAL;
                    *lease = (int64_t)v;
                    ip = qp;
                    continue;
                }
                if (leftover_safe_skip_unknown_at_(req + p, (size_t)skip, &ip,
                                                   kt) != CETCD_OK)
                    return CETCD_ERR_INVAL;
            }
            p += (size_t)skip;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_range_response_kv_version(int64_t version, uint8_t *out,
                                           size_t cap, size_t *n) {
    uint8_t inner[16];
    size_t in = 0;
    uint64_t v;
    size_t pos = 0;
    if (!out || !n) return CETCD_ERR_INVAL;
    *n = 0;
    if (version == 0) return CETCD_OK;
    if (in + 2 > sizeof(inner)) return CETCD_ERR_OVERFLOW;
    inner[in++] = 0x20;
    v = (uint64_t)version;
    do {
        if (in >= sizeof(inner)) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        inner[in++] = b;
    } while (v);
    if (pos + 2 + in > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x12; /* field 2 kvs */
    v = in;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    memcpy(out + pos, inner, in);
    pos += in;
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_range_response_kv_version(const uint8_t *req, size_t len,
                                          int64_t *version) {
    size_t p = 0;
    if (!version) return CETCD_ERR_INVAL;
    *version = 0;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x12) {
            uint64_t skip = 0;
            size_t ip = 0;
            int rc = leftover_safe_varint_at_(req, len, &p, &skip);
            if (rc != CETCD_OK || p + skip > len) return CETCD_ERR_INVAL;
            *version = 0;
            while (ip < (size_t)skip) {
                uint8_t kt = req[p + ip];
                ip++;
                if (kt == 0x00)
                    continue;
                if (kt == 0x20) {
                    uint64_t v = 0;
                    size_t qp = ip;
                    if (leftover_safe_varint_at_(req + p, (size_t)skip, &qp, &v)
                        != CETCD_OK)
                        return CETCD_ERR_INVAL;
                    *version = (int64_t)v;
                    ip = qp;
                    continue;
                }
                if (leftover_safe_skip_unknown_at_(req + p, (size_t)skip, &ip,
                                                   kt) != CETCD_OK)
                    return CETCD_ERR_INVAL;
            }
            p += (size_t)skip;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_range_response_kv_create_rev(int64_t create_rev, uint8_t *out,
                                              size_t cap, size_t *n) {
    uint8_t inner[16];
    size_t in = 0;
    uint64_t v;
    size_t pos = 0;
    if (!out || !n) return CETCD_ERR_INVAL;
    *n = 0;
    if (create_rev == 0) return CETCD_OK;
    if (in + 2 > sizeof(inner)) return CETCD_ERR_OVERFLOW;
    inner[in++] = 0x10;
    v = (uint64_t)create_rev;
    do {
        if (in >= sizeof(inner)) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        inner[in++] = b;
    } while (v);
    if (pos + 2 + in > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x12; /* field 2 kvs */
    v = in;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    memcpy(out + pos, inner, in);
    pos += in;
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_range_response_kv_create_rev(const uint8_t *req, size_t len,
                                             int64_t *create_rev) {
    size_t p = 0;
    if (!create_rev) return CETCD_ERR_INVAL;
    *create_rev = 0;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x12) {
            uint64_t skip = 0;
            size_t ip = 0;
            int rc = leftover_safe_varint_at_(req, len, &p, &skip);
            if (rc != CETCD_OK || p + skip > len) return CETCD_ERR_INVAL;
            *create_rev = 0;
            while (ip < (size_t)skip) {
                uint8_t kt = req[p + ip];
                ip++;
                if (kt == 0x00)
                    continue;
                if (kt == 0x10) {
                    uint64_t v = 0;
                    size_t qp = ip;
                    if (leftover_safe_varint_at_(req + p, (size_t)skip, &qp, &v)
                        != CETCD_OK)
                        return CETCD_ERR_INVAL;
                    *create_rev = (int64_t)v;
                    ip = qp;
                    continue;
                }
                if (leftover_safe_skip_unknown_at_(req + p, (size_t)skip, &ip,
                                                   kt) != CETCD_OK)
                    return CETCD_ERR_INVAL;
            }
            p += (size_t)skip;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_range_response_kv_mod_rev(int64_t mod_rev, uint8_t *out,
                                           size_t cap, size_t *n) {
    uint8_t inner[16];
    size_t in = 0;
    uint64_t v;
    size_t pos = 0;
    if (!out || !n) return CETCD_ERR_INVAL;
    *n = 0;
    if (mod_rev == 0) return CETCD_OK;
    if (in + 2 > sizeof(inner)) return CETCD_ERR_OVERFLOW;
    inner[in++] = 0x18;
    v = (uint64_t)mod_rev;
    do {
        if (in >= sizeof(inner)) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        inner[in++] = b;
    } while (v);
    if (pos + 2 + in > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x12; /* field 2 kvs */
    v = in;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    memcpy(out + pos, inner, in);
    pos += in;
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_range_response_kv_mod_rev(const uint8_t *req, size_t len,
                                          int64_t *mod_rev) {
    size_t p = 0;
    if (!mod_rev) return CETCD_ERR_INVAL;
    *mod_rev = 0;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x12) {
            uint64_t skip = 0;
            size_t ip = 0;
            int rc = leftover_safe_varint_at_(req, len, &p, &skip);
            if (rc != CETCD_OK || p + skip > len) return CETCD_ERR_INVAL;
            *mod_rev = 0;
            while (ip < (size_t)skip) {
                uint8_t kt = req[p + ip];
                ip++;
                if (kt == 0x00)
                    continue;
                if (kt == 0x18) {
                    uint64_t v = 0;
                    size_t qp = ip;
                    if (leftover_safe_varint_at_(req + p, (size_t)skip, &qp, &v)
                        != CETCD_OK)
                        return CETCD_ERR_INVAL;
                    *mod_rev = (int64_t)v;
                    ip = qp;
                    continue;
                }
                if (leftover_safe_skip_unknown_at_(req + p, (size_t)skip, &ip,
                                                   kt) != CETCD_OK)
                    return CETCD_ERR_INVAL;
            }
            p += (size_t)skip;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_range_response_kv_value(const char *value, uint8_t *out,
                                         size_t cap, size_t *n) {
    uint8_t inner[64];
    size_t in = 0;
    uint64_t v;
    size_t pos = 0;
    int rc;
    if (!out || !n) return CETCD_ERR_INVAL;
    *n = 0;
    if (!value || !value[0]) return CETCD_OK;
    rc = write_bytes_field_(inner, sizeof(inner), &in, 0x2a,
                            (const uint8_t *)value, strlen(value));
    if (rc != CETCD_OK) return rc;
    if (pos + 2 + in > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x12; /* field 2 kvs */
    v = in;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    memcpy(out + pos, inner, in);
    pos += in;
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_range_response_kv_value(const uint8_t *req, size_t len,
                                        char *value, size_t value_cap) {
    size_t p = 0;
    if (!value || !value_cap) return CETCD_ERR_INVAL;
    value[0] = '\0';
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x12) {
            uint64_t skip = 0;
            size_t ip = 0;
            int rc = leftover_safe_varint_at_(req, len, &p, &skip);
            if (rc != CETCD_OK || p + skip > len) return CETCD_ERR_INVAL;
            value[0] = '\0';
            while (ip < (size_t)skip) {
                uint8_t kt = req[p + ip];
                ip++;
                if (kt == 0x00)
                    continue;
                if (kt == 0x2a) {
                    uint64_t sl = 0;
                    size_t qp = ip;
                    if (leftover_safe_varint_at_(req + p, (size_t)skip, &qp,
                                                 &sl) != CETCD_OK
                        || qp + sl > (size_t)skip)
                        return CETCD_ERR_INVAL;
                    if (sl >= value_cap) return CETCD_ERR_INVAL;
                    memcpy(value, req + p + qp, (size_t)sl);
                    value[sl] = '\0';
                    ip = qp + (size_t)sl;
                    continue;
                }
                if (leftover_safe_skip_unknown_at_(req + p, (size_t)skip, &ip,
                                                   kt) != CETCD_OK)
                    return CETCD_ERR_INVAL;
            }
            p += (size_t)skip;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_range_response_kv_key(const char *key, uint8_t *out,
                                       size_t cap, size_t *n) {
    uint8_t inner[64];
    size_t in = 0;
    uint64_t v;
    size_t pos = 0;
    int rc;
    if (!out || !n) return CETCD_ERR_INVAL;
    *n = 0;
    if (!key || !key[0]) return CETCD_OK;
    rc = write_bytes_field_(inner, sizeof(inner), &in, 0x0a,
                            (const uint8_t *)key, strlen(key));
    if (rc != CETCD_OK) return rc;
    if (pos + 2 + in > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x12; /* field 2 kvs */
    v = in;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    memcpy(out + pos, inner, in);
    pos += in;
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_range_response_kv_key(const uint8_t *req, size_t len,
                                      char *key, size_t key_cap) {
    size_t p = 0;
    if (!key || !key_cap) return CETCD_ERR_INVAL;
    key[0] = '\0';
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x12) {
            uint64_t skip = 0;
            size_t ip = 0;
            int rc = leftover_safe_varint_at_(req, len, &p, &skip);
            if (rc != CETCD_OK || p + skip > len) return CETCD_ERR_INVAL;
            key[0] = '\0';
            while (ip < (size_t)skip) {
                uint8_t kt = req[p + ip];
                ip++;
                if (kt == 0x00)
                    continue;
                if (kt == 0x0a) {
                    uint64_t sl = 0;
                    size_t qp = ip;
                    if (leftover_safe_varint_at_(req + p, (size_t)skip, &qp,
                                                 &sl) != CETCD_OK
                        || qp + sl > (size_t)skip)
                        return CETCD_ERR_INVAL;
                    if (sl >= key_cap) return CETCD_ERR_INVAL;
                    memcpy(key, req + p + qp, (size_t)sl);
                    key[sl] = '\0';
                    ip = qp + (size_t)sl;
                    continue;
                }
                if (leftover_safe_skip_unknown_at_(req + p, (size_t)skip, &ip,
                                                   kt) != CETCD_OK)
                    return CETCD_ERR_INVAL;
            }
            p += (size_t)skip;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_range_response_count(int64_t count, int more, uint8_t *out,
                                      size_t cap, size_t *n) {
    size_t pos = 0;
    uint64_t v;
    if (!out || !n || count < 0) return CETCD_ERR_INVAL;
    if (more) {
        if (pos + 2 > cap) return CETCD_ERR_OVERFLOW;
        out[pos++] = 0x18; /* field 3 more */
        out[pos++] = 0x01;
    }
    if (pos + 2 > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x20; /* field 4 count */
    v = (uint64_t)count;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_range_response_count(const uint8_t *req, size_t len,
                                     int64_t *count, int *more) {
    size_t p = 0;
    if (!count && !more) return CETCD_ERR_INVAL;
    if (count) *count = 0;
    if (more) *more = 0;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x18 || tag == 0x20) {
            uint64_t v = 0;
            if (leftover_safe_varint_at_(req, len, &p, &v) != CETCD_OK)
                return CETCD_ERR_INVAL;
            if (tag == 0x18) {
                if (more) *more = v != 0;
            } else if (count) {
                *count = (int64_t)v;
            }
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_member_list_response_member(uint64_t id, const char *url,
                                             int is_learner, uint8_t *out,
                                             size_t cap, size_t *n) {
    uint8_t inner[96];
    size_t in = 0;
    uint64_t v;
    size_t pos = 0;
    int rc;
    if (!out || !n || !url || !url[0]) return CETCD_ERR_INVAL;
    inner[in++] = 0x08;
    v = id;
    do {
        if (in >= sizeof(inner)) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        inner[in++] = b;
    } while (v);
    rc = write_bytes_field_(inner, sizeof(inner), &in, 0x1a,
                            (const uint8_t *)url, strlen(url));
    if (rc != CETCD_OK) return rc;
    if (is_learner) {
        if (in + 2 > sizeof(inner)) return CETCD_ERR_OVERFLOW;
        inner[in++] = 0x28;
        inner[in++] = 0x01;
    }
    if (pos + 2 + in > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x12; /* field 2 members */
    v = in;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    memcpy(out + pos, inner, in);
    pos += in;
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_member_list_response(const uint8_t *req, size_t len,
                                     uint64_t *id, char *url, size_t url_cap,
                                     int *is_learner, size_t *n_members) {
    size_t p = 0;
    if (id) *id = 0;
    if (url && url_cap)
        url[0] = '\0';
    if (is_learner) *is_learner = 0;
    if (n_members) *n_members = 0;
    if (!id && !url && !is_learner && !n_members) return CETCD_ERR_INVAL;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x12) {
            uint64_t skip = 0;
            size_t ip = 0;
            int rc = leftover_safe_varint_at_(req, len, &p, &skip);
            if (rc != CETCD_OK || p + skip > len) return CETCD_ERR_INVAL;
            if (n_members) (*n_members)++;
            if (id) *id = 0;
            if (url && url_cap) url[0] = '\0';
            if (is_learner) *is_learner = 0;
            while (ip < (size_t)skip) {
                uint8_t mt = req[p + ip];
                ip++;
                if (mt == 0x00)
                    continue;
                if (mt == 0x08 || mt == 0x28) {
                    uint64_t v = 0;
                    size_t qp = ip;
                    if (leftover_safe_varint_at_(req + p, (size_t)skip, &qp, &v)
                        != CETCD_OK)
                        return CETCD_ERR_INVAL;
                    ip = qp;
                    if (mt == 0x08 && id) *id = v;
                    if (mt == 0x28 && is_learner) *is_learner = v != 0;
                    continue;
                }
                if (mt == 0x12 || mt == 0x1a || mt == 0x22) {
                    uint64_t sl = 0;
                    size_t qp = ip;
                    if (leftover_safe_varint_at_(req + p, (size_t)skip, &qp, &sl)
                        != CETCD_OK || qp + sl > (size_t)skip)
                        return CETCD_ERR_INVAL;
                    if (mt == 0x1a && url && url_cap) {
                        if (sl >= url_cap) return CETCD_ERR_INVAL;
                        memcpy(url, req + p + qp, (size_t)sl);
                        url[sl] = '\0';
                    }
                    ip = qp + (size_t)sl;
                    continue;
                }
                if (leftover_safe_skip_unknown_at_(req + p, (size_t)skip, &ip,
                                                   mt) != CETCD_OK)
                    return CETCD_ERR_INVAL;
            }
            p += (size_t)skip;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_member_list_client_url(const char *url, uint8_t *out,
                                        size_t cap, size_t *n) {
    uint8_t inner[96];
    size_t in = 0;
    uint64_t v;
    size_t pos = 0;
    int rc;
    if (!out || !n || !url || !url[0]) return CETCD_ERR_INVAL;
    rc = write_bytes_field_(inner, sizeof(inner), &in, 0x22,
                            (const uint8_t *)url, strlen(url));
    if (rc != CETCD_OK) return rc;
    if (pos + 2 + in > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x12; /* field 2 members */
    v = in;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    memcpy(out + pos, inner, in);
    pos += in;
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_member_list_client_url(const uint8_t *req, size_t len,
                                       char *url, size_t url_cap) {
    size_t p = 0;
    if (url && url_cap)
        url[0] = '\0';
    if (!url) return CETCD_ERR_INVAL;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x12) {
            uint64_t skip = 0;
            size_t ip = 0;
            int rc = leftover_safe_varint_at_(req, len, &p, &skip);
            if (rc != CETCD_OK || p + skip > len) return CETCD_ERR_INVAL;
            if (url && url_cap) url[0] = '\0';
            while (ip < (size_t)skip) {
                uint8_t mt = req[p + ip];
                ip++;
                if (mt == 0x00)
                    continue;
                if (mt == 0x22) {
                    uint64_t sl = 0;
                    size_t qp = ip;
                    if (leftover_safe_varint_at_(req + p, (size_t)skip, &qp, &sl)
                        != CETCD_OK || qp + sl > (size_t)skip)
                        return CETCD_ERR_INVAL;
                    if (url_cap) {
                        if (sl >= url_cap) return CETCD_ERR_INVAL;
                        memcpy(url, req + p + qp, (size_t)sl);
                        url[sl] = '\0';
                    }
                    ip = qp + (size_t)sl;
                    continue;
                }
                if (leftover_safe_skip_unknown_at_(req + p, (size_t)skip, &ip,
                                                   mt) != CETCD_OK)
                    return CETCD_ERR_INVAL;
            }
            p += (size_t)skip;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_member_list_name(const char *name, uint8_t *out, size_t cap,
                                  size_t *n) {
    uint8_t inner[96];
    size_t in = 0;
    uint64_t v;
    size_t pos = 0;
    int rc;
    if (!out || !n || !name || !name[0]) return CETCD_ERR_INVAL;
    rc = write_bytes_field_(inner, sizeof(inner), &in, 0x12,
                            (const uint8_t *)name, strlen(name));
    if (rc != CETCD_OK) return rc;
    if (pos + 2 + in > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x12; /* field 2 members */
    v = in;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    memcpy(out + pos, inner, in);
    pos += in;
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_member_list_name(const uint8_t *req, size_t len, char *name,
                                 size_t name_cap) {
    size_t p = 0;
    if (name && name_cap)
        name[0] = '\0';
    if (!name) return CETCD_ERR_INVAL;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x12) {
            uint64_t skip = 0;
            size_t ip = 0;
            int rc = leftover_safe_varint_at_(req, len, &p, &skip);
            if (rc != CETCD_OK || p + skip > len) return CETCD_ERR_INVAL;
            if (name && name_cap) name[0] = '\0';
            while (ip < (size_t)skip) {
                uint8_t mt = req[p + ip];
                ip++;
                if (mt == 0x00)
                    continue;
                if (mt == 0x12) {
                    uint64_t sl = 0;
                    size_t qp = ip;
                    if (leftover_safe_varint_at_(req + p, (size_t)skip, &qp, &sl)
                        != CETCD_OK || qp + sl > (size_t)skip)
                        return CETCD_ERR_INVAL;
                    if (name_cap) {
                        if (sl >= name_cap) return CETCD_ERR_INVAL;
                        memcpy(name, req + p + qp, (size_t)sl);
                        name[sl] = '\0';
                    }
                    ip = qp + (size_t)sl;
                    continue;
                }
                if (leftover_safe_skip_unknown_at_(req + p, (size_t)skip, &ip,
                                                   mt) != CETCD_OK)
                    return CETCD_ERR_INVAL;
            }
            p += (size_t)skip;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_member_list_is_learner(int is_learner, uint8_t *out,
                                        size_t cap, size_t *n) {
    uint8_t inner[8];
    size_t in = 0;
    uint64_t v;
    size_t pos = 0;
    if (!out || !n) return CETCD_ERR_INVAL;
    *n = 0;
    if (!is_learner) return CETCD_OK;
    if (in + 2 > sizeof(inner)) return CETCD_ERR_OVERFLOW;
    inner[in++] = 0x28;
    inner[in++] = 0x01;
    if (pos + 2 + in > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x12; /* field 2 members */
    v = in;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    memcpy(out + pos, inner, in);
    pos += in;
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_member_list_is_learner(const uint8_t *req, size_t len,
                                       int *is_learner) {
    size_t p = 0;
    if (!is_learner) return CETCD_ERR_INVAL;
    *is_learner = 0;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x12) {
            uint64_t skip = 0;
            size_t ip = 0;
            int rc = leftover_safe_varint_at_(req, len, &p, &skip);
            if (rc != CETCD_OK || p + skip > len) return CETCD_ERR_INVAL;
            *is_learner = 0;
            while (ip < (size_t)skip) {
                uint8_t mt = req[p + ip];
                ip++;
                if (mt == 0x00)
                    continue;
                if (mt == 0x28) {
                    uint64_t v = 0;
                    size_t qp = ip;
                    if (leftover_safe_varint_at_(req + p, (size_t)skip, &qp, &v)
                        != CETCD_OK)
                        return CETCD_ERR_INVAL;
                    *is_learner = v != 0;
                    ip = qp;
                    continue;
                }
                if (leftover_safe_skip_unknown_at_(req + p, (size_t)skip, &ip,
                                                   mt) != CETCD_OK)
                    return CETCD_ERR_INVAL;
            }
            p += (size_t)skip;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_member_list_id(uint64_t id, uint8_t *out, size_t cap,
                                size_t *n) {
    uint8_t inner[16];
    size_t in = 0;
    uint64_t v;
    size_t pos = 0;
    if (!out || !n) return CETCD_ERR_INVAL;
    *n = 0;
    if (id == 0) return CETCD_OK;
    if (in + 2 > sizeof(inner)) return CETCD_ERR_OVERFLOW;
    inner[in++] = 0x08;
    v = id;
    do {
        if (in >= sizeof(inner)) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        inner[in++] = b;
    } while (v);
    if (pos + 2 + in > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x12; /* field 2 members */
    v = in;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    memcpy(out + pos, inner, in);
    pos += in;
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_member_list_id(const uint8_t *req, size_t len, uint64_t *id) {
    size_t p = 0;
    if (!id) return CETCD_ERR_INVAL;
    *id = 0;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x12) {
            uint64_t skip = 0;
            size_t ip = 0;
            int rc = leftover_safe_varint_at_(req, len, &p, &skip);
            if (rc != CETCD_OK || p + skip > len) return CETCD_ERR_INVAL;
            *id = 0;
            while (ip < (size_t)skip) {
                uint8_t mt = req[p + ip];
                ip++;
                if (mt == 0x00)
                    continue;
                if (mt == 0x08) {
                    uint64_t v = 0;
                    size_t qp = ip;
                    if (leftover_safe_varint_at_(req + p, (size_t)skip, &qp, &v)
                        != CETCD_OK)
                        return CETCD_ERR_INVAL;
                    *id = v;
                    ip = qp;
                    continue;
                }
                if (leftover_safe_skip_unknown_at_(req + p, (size_t)skip, &ip,
                                                   mt) != CETCD_OK)
                    return CETCD_ERR_INVAL;
            }
            p += (size_t)skip;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_member_list_peer_url(const char *url, uint8_t *out,
                                      size_t cap, size_t *n) {
    uint8_t inner[96];
    size_t in = 0;
    uint64_t v;
    size_t pos = 0;
    int rc;
    if (!out || !n) return CETCD_ERR_INVAL;
    *n = 0;
    if (!url || !url[0]) return CETCD_OK;
    rc = write_bytes_field_(inner, sizeof(inner), &in, 0x1a,
                            (const uint8_t *)url, strlen(url));
    if (rc != CETCD_OK) return rc;
    if (pos + 2 + in > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x12; /* field 2 members */
    v = in;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    memcpy(out + pos, inner, in);
    pos += in;
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_member_list_peer_url(const uint8_t *req, size_t len,
                                     char *url, size_t url_cap) {
    size_t p = 0;
    if (!url || !url_cap) return CETCD_ERR_INVAL;
    url[0] = '\0';
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x12) {
            uint64_t skip = 0;
            size_t ip = 0;
            int rc = leftover_safe_varint_at_(req, len, &p, &skip);
            if (rc != CETCD_OK || p + skip > len) return CETCD_ERR_INVAL;
            url[0] = '\0';
            while (ip < (size_t)skip) {
                uint8_t mt = req[p + ip];
                ip++;
                if (mt == 0x00)
                    continue;
                if (mt == 0x1a) {
                    uint64_t sl = 0;
                    size_t qp = ip;
                    if (leftover_safe_varint_at_(req + p, (size_t)skip, &qp,
                                                 &sl) != CETCD_OK
                        || qp + sl > (size_t)skip)
                        return CETCD_ERR_INVAL;
                    if (sl >= url_cap) return CETCD_ERR_INVAL;
                    memcpy(url, req + p + qp, (size_t)sl);
                    url[sl] = '\0';
                    ip = qp + (size_t)sl;
                    continue;
                }
                if (leftover_safe_skip_unknown_at_(req + p, (size_t)skip, &ip,
                                                   mt) != CETCD_OK)
                    return CETCD_ERR_INVAL;
            }
            p += (size_t)skip;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_auth_status_response(int enabled, uint8_t *out, size_t cap,
                                      size_t *n) {
    if (!out || !n || cap < 2) return CETCD_ERR_INVAL;
    out[0] = 0x10; /* field 2 enabled */
    out[1] = enabled ? 1 : 0;
    *n = 2;
    return CETCD_OK;
}

int cetcd_parse_auth_status_response(const uint8_t *req, size_t len,
                                     int *enabled) {
    size_t p = 0;
    if (!enabled) return CETCD_ERR_INVAL;
    *enabled = 0;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x10 || tag == 0x18) {
            uint64_t v = 0;
            if (leftover_safe_varint_at_(req, len, &p, &v) != CETCD_OK)
                return CETCD_ERR_INVAL;
            if (tag == 0x10) *enabled = v != 0;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_auth_status_auth_revision(uint64_t auth_rev, uint8_t *out,
                                           size_t cap, size_t *n) {
    size_t pos = 0;
    uint64_t v;
    if (!out || !n) return CETCD_ERR_INVAL;
    *n = 0;
    if (auth_rev == 0) return CETCD_OK;
    if (pos + 2 > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x18; /* field 3 authRevision */
    v = auth_rev;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_auth_status_auth_revision(const uint8_t *req, size_t len,
                                          uint64_t *auth_rev) {
    size_t p = 0;
    if (!auth_rev) return CETCD_ERR_INVAL;
    *auth_rev = 0;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x18) {
            uint64_t v = 0;
            if (leftover_safe_varint_at_(req, len, &p, &v) != CETCD_OK)
                return CETCD_ERR_INVAL;
            *auth_rev = v;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_string_list_item(const char *s, uint8_t *out, size_t cap,
                                  size_t *n) {
    size_t pos = 0;
    int rc;
    if (!out || !n || !s || !s[0]) return CETCD_ERR_INVAL;
    rc = write_bytes_field_(out, cap, &pos, 0x12, (const uint8_t *)s,
                            strlen(s));
    if (rc != CETCD_OK) return rc;
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_string_list_response(const uint8_t *req, size_t len,
                                     char *name, size_t name_cap, size_t *n) {
    size_t p = 0;
    if (name && name_cap)
        name[0] = '\0';
    if (n) *n = 0;
    if (!name && !n) return CETCD_ERR_INVAL;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x12) {
            uint64_t skip = 0;
            int rc = leftover_safe_varint_at_(req, len, &p, &skip);
            if (rc != CETCD_OK || p + skip > len) return CETCD_ERR_INVAL;
            if (n) (*n)++;
            if (name && name_cap) {
                if (skip >= name_cap) return CETCD_ERR_INVAL;
                memcpy(name, req + p, (size_t)skip);
                name[skip] = '\0';
            }
            p += (size_t)skip;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_status_response(const char *version, uint64_t db_size,
                                 int is_learner, uint8_t *out, size_t cap,
                                 size_t *n) {
    size_t pos = 0;
    uint64_t v;
    int rc;
    if (!out || !n || !version || !version[0]) return CETCD_ERR_INVAL;
    rc = write_bytes_field_(out, cap, &pos, 0x12, (const uint8_t *)version,
                            strlen(version));
    if (rc != CETCD_OK) return rc;
    if (pos + 2 > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x18; /* field 3 dbSize */
    v = db_size;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    if (is_learner) {
        if (pos + 2 > cap) return CETCD_ERR_OVERFLOW;
        out[pos++] = 0x50;
        out[pos++] = 0x01;
    }
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_status_response(const uint8_t *req, size_t len,
                                char *version, size_t version_cap,
                                uint64_t *db_size, int *is_learner) {
    size_t p = 0;
    if (version && version_cap)
        version[0] = '\0';
    if (db_size) *db_size = 0;
    if (is_learner) *is_learner = 0;
    if (!version && !db_size && !is_learner) return CETCD_ERR_INVAL;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x12 || tag == 0x42) {
            uint64_t skip = 0;
            int rc = leftover_safe_varint_at_(req, len, &p, &skip);
            if (rc != CETCD_OK || p + skip > len) return CETCD_ERR_INVAL;
            if (tag == 0x12 && version && version_cap) {
                if (skip >= version_cap) return CETCD_ERR_INVAL;
                memcpy(version, req + p, (size_t)skip);
                version[skip] = '\0';
            }
            p += (size_t)skip;
            continue;
        }
        if (tag == 0x18 || tag == 0x20 || tag == 0x28 || tag == 0x30 ||
            tag == 0x38 || tag == 0x48 || tag == 0x50) {
            uint64_t v = 0;
            if (leftover_safe_varint_at_(req, len, &p, &v) != CETCD_OK)
                return CETCD_ERR_INVAL;
            if (tag == 0x18 && db_size) *db_size = v;
            if (tag == 0x50 && is_learner) *is_learner = v != 0;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_status_errors(const char *error, uint8_t *out, size_t cap,
                               size_t *n) {
    size_t pos = 0;
    int rc;
    if (!out || !n) return CETCD_ERR_INVAL;
    *n = 0;
    if (!error || !error[0]) return CETCD_OK;
    rc = write_bytes_field_(out, cap, &pos, 0x42,
                            (const uint8_t *)error, strlen(error));
    if (rc != CETCD_OK) return rc;
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_status_errors(const uint8_t *req, size_t len, char *error,
                              size_t error_cap) {
    size_t p = 0;
    if (error && error_cap)
        error[0] = '\0';
    if (!error) return CETCD_ERR_INVAL;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x42) {
            uint64_t skip = 0;
            int rc = leftover_safe_varint_at_(req, len, &p, &skip);
            if (rc != CETCD_OK || p + skip > len) return CETCD_ERR_INVAL;
            if (error_cap) {
                if (skip >= error_cap) return CETCD_ERR_INVAL;
                memcpy(error, req + p, (size_t)skip);
                error[skip] = '\0';
            }
            p += (size_t)skip;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_status_db_size_in_use(uint64_t db_inuse, uint8_t *out,
                                       size_t cap, size_t *n) {
    size_t pos = 0;
    uint64_t v;
    if (!out || !n) return CETCD_ERR_INVAL;
    *n = 0;
    if (db_inuse == 0) return CETCD_OK;
    if (pos + 2 > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x48;
    v = db_inuse;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_status_db_size_in_use(const uint8_t *req, size_t len,
                                      uint64_t *db_inuse) {
    size_t p = 0;
    if (!db_inuse) return CETCD_ERR_INVAL;
    *db_inuse = 0;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x48) {
            uint64_t v = 0;
            if (leftover_safe_varint_at_(req, len, &p, &v) != CETCD_OK)
                return CETCD_ERR_INVAL;
            *db_inuse = v;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_status_db_size(uint64_t db_size, uint8_t *out, size_t cap,
                                size_t *n) {
    size_t pos = 0;
    uint64_t v;
    if (!out || !n) return CETCD_ERR_INVAL;
    *n = 0;
    if (db_size == 0) return CETCD_OK;
    if (pos + 2 > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x18;
    v = db_size;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_status_db_size(const uint8_t *req, size_t len,
                               uint64_t *db_size) {
    size_t p = 0;
    if (!db_size) return CETCD_ERR_INVAL;
    *db_size = 0;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x18) {
            uint64_t v = 0;
            if (leftover_safe_varint_at_(req, len, &p, &v) != CETCD_OK)
                return CETCD_ERR_INVAL;
            *db_size = v;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_status_version(const char *version, uint8_t *out, size_t cap,
                                size_t *n) {
    size_t pos = 0;
    int rc;
    if (!out || !n) return CETCD_ERR_INVAL;
    *n = 0;
    if (!version || !version[0]) return CETCD_OK;
    rc = write_bytes_field_(out, cap, &pos, 0x12,
                            (const uint8_t *)version, strlen(version));
    if (rc != CETCD_OK) return rc;
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_status_version(const uint8_t *req, size_t len, char *version,
                               size_t version_cap) {
    size_t p = 0;
    if (version && version_cap)
        version[0] = '\0';
    if (!version) return CETCD_ERR_INVAL;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x12) {
            uint64_t skip = 0;
            int rc = leftover_safe_varint_at_(req, len, &p, &skip);
            if (rc != CETCD_OK || p + skip > len) return CETCD_ERR_INVAL;
            if (version_cap) {
                if (skip >= version_cap) return CETCD_ERR_INVAL;
                memcpy(version, req + p, (size_t)skip);
                version[skip] = '\0';
            }
            p += (size_t)skip;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_status_is_learner(int is_learner, uint8_t *out, size_t cap,
                                   size_t *n) {
    size_t pos = 0;
    if (!out || !n) return CETCD_ERR_INVAL;
    *n = 0;
    if (!is_learner) return CETCD_OK;
    if (pos + 2 > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x50;
    out[pos++] = 0x01;
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_status_is_learner(const uint8_t *req, size_t len,
                                  int *is_learner) {
    size_t p = 0;
    if (!is_learner) return CETCD_ERR_INVAL;
    *is_learner = 0;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x50) {
            uint64_t v = 0;
            if (leftover_safe_varint_at_(req, len, &p, &v) != CETCD_OK)
                return CETCD_ERR_INVAL;
            *is_learner = v != 0;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_status_leader(uint64_t leader, uint8_t *out, size_t cap,
                               size_t *n) {
    size_t pos = 0;
    uint64_t v;
    if (!out || !n) return CETCD_ERR_INVAL;
    *n = 0;
    if (leader == 0) return CETCD_OK;
    if (pos + 2 > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x20;
    v = leader;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_status_leader(const uint8_t *req, size_t len, uint64_t *leader) {
    size_t p = 0;
    if (!leader) return CETCD_ERR_INVAL;
    *leader = 0;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x20) {
            uint64_t v = 0;
            if (leftover_safe_varint_at_(req, len, &p, &v) != CETCD_OK)
                return CETCD_ERR_INVAL;
            *leader = v;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_status_raft_index(uint64_t raft_index, uint8_t *out,
                                   size_t cap, size_t *n) {
    size_t pos = 0;
    uint64_t v;
    if (!out || !n) return CETCD_ERR_INVAL;
    *n = 0;
    if (raft_index == 0) return CETCD_OK;
    if (pos + 2 > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x28;
    v = raft_index;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_status_raft_index(const uint8_t *req, size_t len,
                                  uint64_t *raft_index) {
    size_t p = 0;
    if (!raft_index) return CETCD_ERR_INVAL;
    *raft_index = 0;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x28) {
            uint64_t v = 0;
            if (leftover_safe_varint_at_(req, len, &p, &v) != CETCD_OK)
                return CETCD_ERR_INVAL;
            *raft_index = v;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_status_raft_term(uint64_t raft_term, uint8_t *out, size_t cap,
                                  size_t *n) {
    size_t pos = 0;
    uint64_t v;
    if (!out || !n) return CETCD_ERR_INVAL;
    *n = 0;
    if (raft_term == 0) return CETCD_OK;
    if (pos + 2 > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x30;
    v = raft_term;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_status_raft_term(const uint8_t *req, size_t len,
                                 uint64_t *raft_term) {
    size_t p = 0;
    if (!raft_term) return CETCD_ERR_INVAL;
    *raft_term = 0;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x30) {
            uint64_t v = 0;
            if (leftover_safe_varint_at_(req, len, &p, &v) != CETCD_OK)
                return CETCD_ERR_INVAL;
            *raft_term = v;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_status_raft_applied(uint64_t raft_applied, uint8_t *out,
                                     size_t cap, size_t *n) {
    size_t pos = 0;
    uint64_t v;
    if (!out || !n) return CETCD_ERR_INVAL;
    *n = 0;
    if (raft_applied == 0) return CETCD_OK;
    if (pos + 2 > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x38;
    v = raft_applied;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_status_raft_applied(const uint8_t *req, size_t len,
                                    uint64_t *raft_applied) {
    size_t p = 0;
    if (!raft_applied) return CETCD_ERR_INVAL;
    *raft_applied = 0;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x38) {
            uint64_t v = 0;
            if (leftover_safe_varint_at_(req, len, &p, &v) != CETCD_OK)
                return CETCD_ERR_INVAL;
            *raft_applied = v;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_lease_grant_response(int64_t id, int64_t ttl, uint8_t *out,
                                      size_t cap, size_t *n) {
    size_t pos = 0;
    uint64_t v;
    if (!out || !n || id <= 0) return CETCD_ERR_INVAL;
    if (pos + 2 > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x10; /* field 2 ID */
    v = (uint64_t)id;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    if (pos + 2 > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x18; /* field 3 TTL */
    v = ttl > 0 ? (uint64_t)ttl : 0;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_lease_grant_response(const uint8_t *req, size_t len,
                                     int64_t *id, int64_t *ttl) {
    size_t p = 0;
    if (!id || !ttl) return CETCD_ERR_INVAL;
    *id = 0;
    *ttl = 0;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x10 || tag == 0x18) {
            uint64_t v = 0;
            if (leftover_safe_varint_at_(req, len, &p, &v) != CETCD_OK)
                return CETCD_ERR_INVAL;
            if (tag == 0x10) *id = (int64_t)v;
            else *ttl = (int64_t)v;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_lease_grant_error(const char *error, uint8_t *out, size_t cap,
                                   size_t *n) {
    size_t pos = 0;
    int rc;
    if (!out || !n) return CETCD_ERR_INVAL;
    *n = 0;
    if (!error || !error[0]) return CETCD_OK;
    rc = write_bytes_field_(out, cap, &pos, 0x22,
                            (const uint8_t *)error, strlen(error));
    if (rc != CETCD_OK) return rc;
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_lease_grant_error(const uint8_t *req, size_t len, char *error,
                                  size_t error_cap) {
    size_t p = 0;
    if (error && error_cap)
        error[0] = '\0';
    if (!error) return CETCD_ERR_INVAL;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x22) {
            uint64_t skip = 0;
            int rc = leftover_safe_varint_at_(req, len, &p, &skip);
            if (rc != CETCD_OK || p + skip > len) return CETCD_ERR_INVAL;
            if (error_cap) {
                if (skip >= error_cap) return CETCD_ERR_INVAL;
                memcpy(error, req + p, (size_t)skip);
                error[skip] = '\0';
            }
            p += (size_t)skip;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_lease_keepalive_response(int64_t id, int64_t ttl,
                                          uint8_t *out, size_t cap, size_t *n) {
    return cetcd_encode_lease_grant_response(id, ttl, out, cap, n);
}

int cetcd_parse_lease_keepalive_response(const uint8_t *req, size_t len,
                                         int64_t *id, int64_t *ttl) {
    return cetcd_parse_lease_grant_response(req, len, id, ttl);
}

int cetcd_encode_lease_ttl_response(int64_t id, int64_t ttl, int64_t granted,
                                    const char *key, uint8_t *out, size_t cap,
                                    size_t *n) {
    size_t pos = 0;
    uint64_t v;
    int rc;
    if (!out || !n || id <= 0) return CETCD_ERR_INVAL;
    if (pos + 2 > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x10;
    v = (uint64_t)id;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    if (pos + 2 > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x18;
    v = ttl > 0 ? (uint64_t)ttl : 0;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    if (pos + 2 > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x20;
    v = granted > 0 ? (uint64_t)granted : 0;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    if (key && key[0]) {
        rc = write_bytes_field_(out, cap, &pos, 0x2a, (const uint8_t *)key,
                                strlen(key));
        if (rc != CETCD_OK) return rc;
    }
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_lease_ttl_response(const uint8_t *req, size_t len,
                                   int64_t *id, int64_t *ttl, int64_t *granted,
                                   char *key, size_t key_cap) {
    size_t p = 0;
    if (!id || !ttl || !granted) return CETCD_ERR_INVAL;
    *id = 0;
    *ttl = 0;
    *granted = 0;
    if (key && key_cap)
        key[0] = '\0';
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x10 || tag == 0x18 || tag == 0x20) {
            uint64_t v = 0;
            if (leftover_safe_varint_at_(req, len, &p, &v) != CETCD_OK)
                return CETCD_ERR_INVAL;
            if (tag == 0x10) *id = (int64_t)v;
            else if (tag == 0x18) *ttl = (int64_t)v;
            else *granted = (int64_t)v;
            continue;
        }
        if (tag == 0x2a) {
            uint64_t skip = 0;
            int rc = leftover_safe_varint_at_(req, len, &p, &skip);
            if (rc != CETCD_OK || p + skip > len) return CETCD_ERR_INVAL;
            if (key && key_cap) {
                if (skip >= key_cap) return CETCD_ERR_INVAL;
                memcpy(key, req + p, (size_t)skip);
                key[skip] = '\0';
            }
            p += (size_t)skip;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_lease_ttl_key(const char *key, uint8_t *out, size_t cap,
                               size_t *n) {
    size_t pos = 0;
    int rc;
    if (!out || !n) return CETCD_ERR_INVAL;
    *n = 0;
    if (!key || !key[0]) return CETCD_OK;
    rc = write_bytes_field_(out, cap, &pos, 0x2a, (const uint8_t *)key,
                            strlen(key));
    if (rc != CETCD_OK) return rc;
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_lease_ttl_key(const uint8_t *req, size_t len, char *key,
                              size_t key_cap) {
    size_t p = 0;
    if (key && key_cap)
        key[0] = '\0';
    if (!key) return CETCD_ERR_INVAL;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x2a) {
            uint64_t skip = 0;
            int rc = leftover_safe_varint_at_(req, len, &p, &skip);
            if (rc != CETCD_OK || p + skip > len) return CETCD_ERR_INVAL;
            if (key_cap) {
                if (skip >= key_cap) return CETCD_ERR_INVAL;
                memcpy(key, req + p, (size_t)skip);
                key[skip] = '\0';
            }
            p += (size_t)skip;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_lease_ttl_granted(int64_t granted, uint8_t *out, size_t cap,
                                   size_t *n) {
    size_t pos = 0;
    uint64_t v;
    if (!out || !n) return CETCD_ERR_INVAL;
    *n = 0;
    if (granted == 0) return CETCD_OK;
    if (pos + 2 > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x20;
    v = (uint64_t)granted;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_lease_ttl_granted(const uint8_t *req, size_t len,
                                  int64_t *granted) {
    size_t p = 0;
    if (!granted) return CETCD_ERR_INVAL;
    *granted = 0;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x20) {
            uint64_t v = 0;
            if (leftover_safe_varint_at_(req, len, &p, &v) != CETCD_OK)
                return CETCD_ERR_INVAL;
            *granted = (int64_t)v;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_lease_ttl_remaining(int64_t ttl, uint8_t *out, size_t cap,
                                     size_t *n) {
    size_t pos = 0;
    uint64_t v;
    if (!out || !n) return CETCD_ERR_INVAL;
    *n = 0;
    if (ttl == 0) return CETCD_OK;
    if (pos + 2 > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x18;
    v = (uint64_t)ttl;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_lease_ttl_remaining(const uint8_t *req, size_t len,
                                    int64_t *ttl) {
    size_t p = 0;
    if (!ttl) return CETCD_ERR_INVAL;
    *ttl = 0;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x18) {
            uint64_t v = 0;
            if (leftover_safe_varint_at_(req, len, &p, &v) != CETCD_OK)
                return CETCD_ERR_INVAL;
            *ttl = (int64_t)v;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_lease_ttl_id(int64_t id, uint8_t *out, size_t cap, size_t *n) {
    size_t pos = 0;
    uint64_t v;
    if (!out || !n) return CETCD_ERR_INVAL;
    *n = 0;
    if (id == 0) return CETCD_OK;
    if (pos + 2 > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x10;
    v = (uint64_t)id;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_lease_ttl_id(const uint8_t *req, size_t len, int64_t *id) {
    size_t p = 0;
    if (!id) return CETCD_ERR_INVAL;
    *id = 0;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x10) {
            uint64_t v = 0;
            if (leftover_safe_varint_at_(req, len, &p, &v) != CETCD_OK)
                return CETCD_ERR_INVAL;
            *id = (int64_t)v;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_authenticate_response(const char *token, uint8_t *out,
                                       size_t cap, size_t *n) {
    size_t pos = 0;
    int rc;
    if (!out || !n || !token || !token[0]) return CETCD_ERR_INVAL;
    rc = write_bytes_field_(out, cap, &pos, 0x12, (const uint8_t *)token,
                            strlen(token));
    if (rc != CETCD_OK) return rc;
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_authenticate_response(const uint8_t *req, size_t len,
                                      char *token, size_t token_cap) {
    size_t p = 0;
    if (!token || !token_cap) return CETCD_ERR_INVAL;
    token[0] = '\0';
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x12) {
            uint64_t skip = 0;
            int rc = leftover_safe_varint_at_(req, len, &p, &skip);
            if (rc != CETCD_OK || p + skip > len) return CETCD_ERR_INVAL;
            if (skip >= token_cap) return CETCD_ERR_INVAL;
            memcpy(token, req + p, (size_t)skip);
            token[skip] = '\0';
            p += (size_t)skip;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_txn_succeeded(int succeeded, uint8_t *out, size_t cap,
                               size_t *n) {
    if (!out || !n || cap < 2) return CETCD_ERR_INVAL;
    out[0] = 0x10; /* field 2 succeeded */
    out[1] = succeeded ? 1 : 0;
    *n = 2;
    return CETCD_OK;
}

int cetcd_parse_txn_succeeded(const uint8_t *req, size_t len, int *succeeded) {
    size_t p = 0;
    if (!succeeded) return CETCD_ERR_INVAL;
    *succeeded = 0;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x10) {
            uint64_t v = 0;
            if (leftover_safe_varint_at_(req, len, &p, &v) != CETCD_OK)
                return CETCD_ERR_INVAL;
            *succeeded = v != 0;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_hash_response(uint32_t hash, int64_t compact_rev,
                               uint8_t *out, size_t cap, size_t *n) {
    size_t pos = 0;
    uint64_t v;
    if (!out || !n) return CETCD_ERR_INVAL;
    if (pos + 2 > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x10; /* field 2 hash */
    v = hash;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    if (compact_rev >= 0) {
        if (pos + 2 > cap) return CETCD_ERR_OVERFLOW;
        out[pos++] = 0x18;
        v = (uint64_t)compact_rev;
        do {
            if (pos >= cap) return CETCD_ERR_OVERFLOW;
            uint8_t b = (uint8_t)(v & 0x7fu);
            v >>= 7;
            if (v) b |= 0x80u;
            out[pos++] = b;
        } while (v);
    }
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_hash_response(const uint8_t *req, size_t len,
                              uint32_t *hash, int64_t *compact_rev) {
    size_t p = 0;
    if (!hash) return CETCD_ERR_INVAL;
    *hash = 0;
    if (compact_rev) *compact_rev = 0;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x10 || tag == 0x18) {
            uint64_t v = 0;
            if (leftover_safe_varint_at_(req, len, &p, &v) != CETCD_OK)
                return CETCD_ERR_INVAL;
            if (tag == 0x10) {
                if (v > 0xffffffffull) return CETCD_ERR_INVAL;
                *hash = (uint32_t)v;
            } else if (compact_rev) {
                *compact_rev = (int64_t)v;
            }
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_lease_list_item(int64_t id, uint8_t *out, size_t cap,
                                 size_t *n) {
    uint8_t inner[16];
    size_t in = 0;
    uint64_t v;
    size_t pos = 0;
    if (!out || !n || id <= 0) return CETCD_ERR_INVAL;
    inner[in++] = 0x08;
    v = (uint64_t)id;
    do {
        if (in >= sizeof(inner)) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        inner[in++] = b;
    } while (v);
    if (pos + 2 + in > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x12; /* field 2 leases */
    v = in;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    memcpy(out + pos, inner, in);
    pos += in;
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_lease_list_response(const uint8_t *req, size_t len,
                                    int64_t *id, size_t *n) {
    size_t p = 0;
    if (id) *id = 0;
    if (n) *n = 0;
    if (!id && !n) return CETCD_ERR_INVAL;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x12) {
            uint64_t skip = 0;
            size_t ip = 0;
            int rc = leftover_safe_varint_at_(req, len, &p, &skip);
            if (rc != CETCD_OK || p + skip > len) return CETCD_ERR_INVAL;
            if (n) (*n)++;
            if (id) *id = 0;
            while (ip < (size_t)skip) {
                uint8_t mt = req[p + ip];
                ip++;
                if (mt == 0x00)
                    continue;
                if (mt == 0x08) {
                    uint64_t v = 0;
                    size_t qp = ip;
                    if (leftover_safe_varint_at_(req + p, (size_t)skip, &qp, &v)
                        != CETCD_OK)
                        return CETCD_ERR_INVAL;
                    ip = qp;
                    if (id) *id = (int64_t)v;
                    continue;
                }
                if (leftover_safe_skip_unknown_at_(req + p, (size_t)skip, &ip,
                                                   mt) != CETCD_OK)
                    return CETCD_ERR_INVAL;
            }
            p += (size_t)skip;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_response_header(uint64_t cluster_id, uint64_t member_id,
                                 int64_t revision, uint64_t raft_term,
                                 uint8_t *out, size_t cap, size_t *n) {
    uint8_t inner[48];
    size_t in = 0;
    uint64_t v;
    size_t pos = 0;
    if (!out || !n) return CETCD_ERR_INVAL;
    inner[in++] = 0x08;
    v = cluster_id;
    do {
        if (in >= sizeof(inner)) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        inner[in++] = b;
    } while (v);
    inner[in++] = 0x10;
    v = member_id;
    do {
        if (in >= sizeof(inner)) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        inner[in++] = b;
    } while (v);
    inner[in++] = 0x18;
    v = revision > 0 ? (uint64_t)revision : 0;
    do {
        if (in >= sizeof(inner)) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        inner[in++] = b;
    } while (v);
    inner[in++] = 0x20;
    v = raft_term;
    do {
        if (in >= sizeof(inner)) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        inner[in++] = b;
    } while (v);
    if (pos + 2 + in > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x0a;
    v = in;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    memcpy(out + pos, inner, in);
    pos += in;
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_response_header(const uint8_t *req, size_t len,
                                uint64_t *cluster_id, uint64_t *member_id,
                                int64_t *revision, uint64_t *raft_term) {
    size_t p = 0;
    if (cluster_id) *cluster_id = 0;
    if (member_id) *member_id = 0;
    if (revision) *revision = 0;
    if (raft_term) *raft_term = 0;
    if (!cluster_id && !member_id && !revision && !raft_term)
        return CETCD_ERR_INVAL;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x0a) {
            uint64_t skip = 0;
            size_t ip = 0;
            int rc = leftover_safe_varint_at_(req, len, &p, &skip);
            if (rc != CETCD_OK || p + skip > len) return CETCD_ERR_INVAL;
            if (cluster_id) *cluster_id = 0;
            if (member_id) *member_id = 0;
            if (revision) *revision = 0;
            if (raft_term) *raft_term = 0;
            while (ip < (size_t)skip) {
                uint8_t ht = req[p + ip];
                ip++;
                if (ht == 0x00)
                    continue;
                if (ht == 0x08 || ht == 0x10 || ht == 0x18 || ht == 0x20) {
                    uint64_t v = 0;
                    size_t qp = ip;
                    if (leftover_safe_varint_at_(req + p, (size_t)skip, &qp, &v)
                        != CETCD_OK)
                        return CETCD_ERR_INVAL;
                    ip = qp;
                    if (ht == 0x08 && cluster_id) *cluster_id = v;
                    if (ht == 0x10 && member_id) *member_id = v;
                    if (ht == 0x18 && revision) *revision = (int64_t)v;
                    if (ht == 0x20 && raft_term) *raft_term = v;
                    continue;
                }
                if (leftover_safe_skip_unknown_at_(req + p, (size_t)skip, &ip,
                                                   ht) != CETCD_OK)
                    return CETCD_ERR_INVAL;
            }
            p += (size_t)skip;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_snapshot_response(const uint8_t *blob, size_t blob_len,
                                   uint8_t *out, size_t cap, size_t *n) {
    size_t pos = 0;
    int rc;
    if (!out || !n || !blob || blob_len == 0) return CETCD_ERR_INVAL;
    rc = write_bytes_field_(out, cap, &pos, 0x1a, blob, blob_len);
    if (rc != CETCD_OK) return rc;
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_snapshot_response(const uint8_t *req, size_t len,
                                  uint8_t *blob, size_t blob_cap, size_t *n) {
    size_t p = 0;
    if (n) *n = 0;
    if (blob && blob_cap)
        blob[0] = 0;
    if (!blob && !n) return CETCD_ERR_INVAL;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x1a) {
            uint64_t skip = 0;
            int rc = leftover_safe_varint_at_(req, len, &p, &skip);
            if (rc != CETCD_OK || p + skip > len) return CETCD_ERR_INVAL;
            if (blob && blob_cap) {
                if (skip >= blob_cap) return CETCD_ERR_INVAL;
                memcpy(blob, req + p, (size_t)skip);
                blob[skip] = 0;
            }
            if (n) *n = (size_t)skip;
            p += (size_t)skip;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_role_get_perm(int perm_type, const char *key, uint8_t *out,
                               size_t cap, size_t *n) {
    uint8_t inner[96];
    size_t in = 0;
    uint64_t v;
    size_t pos = 0;
    int rc;
    if (!out || !n || !key || !key[0]) return CETCD_ERR_INVAL;
    if (perm_type < 0 || perm_type > 2) return CETCD_ERR_INVAL;
    if (in + 2 > sizeof(inner)) return CETCD_ERR_OVERFLOW;
    inner[in++] = 0x08;
    inner[in++] = (uint8_t)perm_type;
    rc = write_bytes_field_(inner, sizeof(inner), &in, 0x12,
                            (const uint8_t *)key, strlen(key));
    if (rc != CETCD_OK) return rc;
    if (pos + 2 + in > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x12; /* field 2 perm */
    v = in;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    memcpy(out + pos, inner, in);
    pos += in;
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_role_get_response(const uint8_t *req, size_t len,
                                  int *perm_type, char *key, size_t key_cap,
                                  size_t *n) {
    size_t p = 0;
    if (perm_type) *perm_type = 0;
    if (key && key_cap)
        key[0] = '\0';
    if (n) *n = 0;
    if (!perm_type && !key && !n) return CETCD_ERR_INVAL;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x12) {
            uint64_t skip = 0;
            size_t ip = 0;
            int rc = leftover_safe_varint_at_(req, len, &p, &skip);
            if (rc != CETCD_OK || p + skip > len) return CETCD_ERR_INVAL;
            if (n) (*n)++;
            if (perm_type) *perm_type = 0;
            if (key && key_cap) key[0] = '\0';
            while (ip < (size_t)skip) {
                uint8_t mt = req[p + ip];
                ip++;
                if (mt == 0x00)
                    continue;
                if (mt == 0x08) {
                    uint64_t v = 0;
                    size_t qp = ip;
                    if (leftover_safe_varint_at_(req + p, (size_t)skip, &qp, &v)
                        != CETCD_OK)
                        return CETCD_ERR_INVAL;
                    ip = qp;
                    if (perm_type) *perm_type = (int)v;
                    continue;
                }
                if (mt == 0x12 || mt == 0x1a) {
                    uint64_t sl = 0;
                    size_t qp = ip;
                    if (leftover_safe_varint_at_(req + p, (size_t)skip, &qp, &sl)
                        != CETCD_OK || qp + sl > (size_t)skip)
                        return CETCD_ERR_INVAL;
                    if (mt == 0x12 && key && key_cap) {
                        if (sl >= key_cap) return CETCD_ERR_INVAL;
                        memcpy(key, req + p + qp, (size_t)sl);
                        key[sl] = '\0';
                    }
                    ip = qp + (size_t)sl;
                    continue;
                }
                if (leftover_safe_skip_unknown_at_(req + p, (size_t)skip, &ip,
                                                   mt) != CETCD_OK)
                    return CETCD_ERR_INVAL;
            }
            p += (size_t)skip;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_role_get_range_end(const char *range_end, uint8_t *out,
                                    size_t cap, size_t *n) {
    uint8_t inner[96];
    size_t in = 0;
    uint64_t v;
    size_t pos = 0;
    int rc;
    if (!out || !n || !range_end || !range_end[0]) return CETCD_ERR_INVAL;
    rc = write_bytes_field_(inner, sizeof(inner), &in, 0x1a,
                            (const uint8_t *)range_end, strlen(range_end));
    if (rc != CETCD_OK) return rc;
    if (pos + 2 + in > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x12; /* field 2 perm */
    v = in;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    memcpy(out + pos, inner, in);
    pos += in;
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_role_get_range_end(const uint8_t *req, size_t len,
                                   char *range_end, size_t range_end_cap) {
    size_t p = 0;
    if (range_end && range_end_cap)
        range_end[0] = '\0';
    if (!range_end) return CETCD_ERR_INVAL;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x12) {
            uint64_t skip = 0;
            size_t ip = 0;
            int rc = leftover_safe_varint_at_(req, len, &p, &skip);
            if (rc != CETCD_OK || p + skip > len) return CETCD_ERR_INVAL;
            if (range_end && range_end_cap) range_end[0] = '\0';
            while (ip < (size_t)skip) {
                uint8_t mt = req[p + ip];
                ip++;
                if (mt == 0x00)
                    continue;
                if (mt == 0x1a) {
                    uint64_t sl = 0;
                    size_t qp = ip;
                    if (leftover_safe_varint_at_(req + p, (size_t)skip, &qp, &sl)
                        != CETCD_OK || qp + sl > (size_t)skip)
                        return CETCD_ERR_INVAL;
                    if (range_end_cap) {
                        if (sl >= range_end_cap) return CETCD_ERR_INVAL;
                        memcpy(range_end, req + p + qp, (size_t)sl);
                        range_end[sl] = '\0';
                    }
                    ip = qp + (size_t)sl;
                    continue;
                }
                if (leftover_safe_skip_unknown_at_(req + p, (size_t)skip, &ip,
                                                   mt) != CETCD_OK)
                    return CETCD_ERR_INVAL;
            }
            p += (size_t)skip;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_role_get_perm_type(int perm_type, uint8_t *out, size_t cap,
                                    size_t *n) {
    uint8_t perm[8];
    size_t pn = 0;
    uint64_t v;
    size_t pos = 0;
    if (!out || !n) return CETCD_ERR_INVAL;
    *n = 0;
    if (perm_type < 0 || perm_type > 2) return CETCD_ERR_INVAL;
    if (perm_type == 0) return CETCD_OK;
    if (pn + 2 > sizeof(perm)) return CETCD_ERR_OVERFLOW;
    perm[pn++] = 0x08;
    perm[pn++] = (uint8_t)perm_type;
    if (pos + 2 + pn > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x12; /* field 2 perm */
    v = pn;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    memcpy(out + pos, perm, pn);
    pos += pn;
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_role_get_perm_type(const uint8_t *req, size_t len,
                                   int *perm_type) {
    size_t p = 0;
    if (!perm_type) return CETCD_ERR_INVAL;
    *perm_type = 0;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x12) {
            uint64_t skip = 0;
            size_t ip = 0;
            int rc = leftover_safe_varint_at_(req, len, &p, &skip);
            if (rc != CETCD_OK || p + skip > len) return CETCD_ERR_INVAL;
            *perm_type = 0;
            while (ip < (size_t)skip) {
                uint8_t mt = req[p + ip];
                ip++;
                if (mt == 0x00)
                    continue;
                if (mt == 0x08) {
                    uint64_t v = 0;
                    size_t qp = ip;
                    if (leftover_safe_varint_at_(req + p, (size_t)skip, &qp, &v)
                        != CETCD_OK)
                        return CETCD_ERR_INVAL;
                    *perm_type = (int)v;
                    ip = qp;
                    continue;
                }
                if (leftover_safe_skip_unknown_at_(req + p, (size_t)skip, &ip,
                                                   mt) != CETCD_OK)
                    return CETCD_ERR_INVAL;
            }
            p += (size_t)skip;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_role_get_perm_key(const char *key, uint8_t *out, size_t cap,
                                   size_t *n) {
    uint8_t perm[64];
    size_t pn = 0;
    uint64_t v;
    size_t pos = 0;
    int rc;
    if (!out || !n) return CETCD_ERR_INVAL;
    *n = 0;
    if (!key || !key[0]) return CETCD_OK;
    rc = write_bytes_field_(perm, sizeof(perm), &pn, 0x12,
                            (const uint8_t *)key, strlen(key));
    if (rc != CETCD_OK) return rc;
    if (pos + 2 + pn > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x12; /* field 2 perm */
    v = pn;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    memcpy(out + pos, perm, pn);
    pos += pn;
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_role_get_perm_key(const uint8_t *req, size_t len, char *key,
                                  size_t key_cap) {
    size_t p = 0;
    if (!key || !key_cap) return CETCD_ERR_INVAL;
    key[0] = '\0';
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x12) {
            uint64_t skip = 0;
            size_t ip = 0;
            int rc = leftover_safe_varint_at_(req, len, &p, &skip);
            if (rc != CETCD_OK || p + skip > len) return CETCD_ERR_INVAL;
            key[0] = '\0';
            while (ip < (size_t)skip) {
                uint8_t mt = req[p + ip];
                ip++;
                if (mt == 0x00)
                    continue;
                if (mt == 0x12) {
                    uint64_t sl = 0;
                    size_t qp = ip;
                    if (leftover_safe_varint_at_(req + p, (size_t)skip, &qp,
                                                 &sl) != CETCD_OK
                        || qp + sl > (size_t)skip)
                        return CETCD_ERR_INVAL;
                    if (sl >= key_cap) return CETCD_ERR_INVAL;
                    memcpy(key, req + p + qp, (size_t)sl);
                    key[sl] = '\0';
                    ip = qp + (size_t)sl;
                    continue;
                }
                if (leftover_safe_skip_unknown_at_(req + p, (size_t)skip, &ip,
                                                   mt) != CETCD_OK)
                    return CETCD_ERR_INVAL;
            }
            p += (size_t)skip;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_watch_event_kv(int type, const char *key, uint8_t *out,
                                size_t cap, size_t *n) {
    uint8_t kv[64];
    uint8_t ev[96];
    size_t kn = 0, en = 0;
    uint64_t v;
    size_t pos = 0;
    int rc;
    if (!out || !n || !key || !key[0]) return CETCD_ERR_INVAL;
    if (type < 0 || type > 1) return CETCD_ERR_INVAL;
    rc = write_bytes_field_(kv, sizeof(kv), &kn, 0x0a,
                            (const uint8_t *)key, strlen(key));
    if (rc != CETCD_OK) return rc;
    if (en + 2 > sizeof(ev)) return CETCD_ERR_OVERFLOW;
    ev[en++] = 0x08;
    ev[en++] = (uint8_t)type;
    if (en + 2 + kn > sizeof(ev)) return CETCD_ERR_OVERFLOW;
    ev[en++] = 0x12; /* field 2 kv */
    v = kn;
    do {
        if (en >= sizeof(ev)) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        ev[en++] = b;
    } while (v);
    memcpy(ev + en, kv, kn);
    en += kn;
    if (pos + 2 + en > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x5a; /* field 11 events */
    v = en;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    memcpy(out + pos, ev, en);
    pos += en;
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_watch_response(const uint8_t *req, size_t len, int *type,
                               char *key, size_t key_cap, size_t *n) {
    size_t p = 0;
    if (type) *type = 0;
    if (key && key_cap)
        key[0] = '\0';
    if (n) *n = 0;
    if (!type && !key && !n) return CETCD_ERR_INVAL;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x5a) {
            uint64_t skip = 0;
            size_t ip = 0;
            int rc = leftover_safe_varint_at_(req, len, &p, &skip);
            if (rc != CETCD_OK || p + skip > len) return CETCD_ERR_INVAL;
            if (n) (*n)++;
            if (type) *type = 0;
            if (key && key_cap) key[0] = '\0';
            while (ip < (size_t)skip) {
                uint8_t et = req[p + ip];
                ip++;
                if (et == 0x00)
                    continue;
                if (et == 0x08) {
                    uint64_t v = 0;
                    size_t qp = ip;
                    if (leftover_safe_varint_at_(req + p, (size_t)skip, &qp, &v)
                        != CETCD_OK)
                        return CETCD_ERR_INVAL;
                    ip = qp;
                    if (type) *type = (int)v;
                    continue;
                }
                if (et == 0x12 || et == 0x1a) {
                    uint64_t kskip = 0;
                    size_t kp = 0;
                    size_t qp = ip;
                    if (leftover_safe_varint_at_(req + p, (size_t)skip, &qp,
                                                 &kskip) != CETCD_OK
                        || qp + kskip > (size_t)skip)
                        return CETCD_ERR_INVAL;
                    ip = qp;
                    while (kp < (size_t)kskip) {
                        uint8_t kt = req[p + ip + kp];
                        kp++;
                        if (kt == 0x00)
                            continue;
                        if (kt == 0x0a || kt == 0x2a) {
                            uint64_t sl = 0;
                            size_t rp = kp;
                            if (leftover_safe_varint_at_(req + p + ip,
                                                         (size_t)kskip, &rp,
                                                         &sl) != CETCD_OK
                                || rp + sl > (size_t)kskip)
                                return CETCD_ERR_INVAL;
                            if (et == 0x12 && kt == 0x0a && key && key_cap) {
                                if (sl >= key_cap) return CETCD_ERR_INVAL;
                                memcpy(key, req + p + ip + rp, (size_t)sl);
                                key[sl] = '\0';
                            }
                            kp = rp + (size_t)sl;
                            continue;
                        }
                        if (leftover_safe_skip_unknown_at_(req + p + ip,
                                                           (size_t)kskip, &kp,
                                                           kt) != CETCD_OK)
                            return CETCD_ERR_INVAL;
                    }
                    ip += (size_t)kskip;
                    continue;
                }
                if (leftover_safe_skip_unknown_at_(req + p, (size_t)skip, &ip,
                                                   et) != CETCD_OK)
                    return CETCD_ERR_INVAL;
            }
            p += (size_t)skip;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_watch_event_kv_lease(int64_t lease, uint8_t *out, size_t cap,
                                      size_t *n) {
    uint8_t kv[16];
    uint8_t ev[24];
    size_t kn = 0, en = 0;
    uint64_t v;
    size_t pos = 0;
    if (!out || !n) return CETCD_ERR_INVAL;
    *n = 0;
    if (lease == 0) return CETCD_OK;
    if (kn + 2 > sizeof(kv)) return CETCD_ERR_OVERFLOW;
    kv[kn++] = 0x30;
    v = (uint64_t)lease;
    do {
        if (kn >= sizeof(kv)) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        kv[kn++] = b;
    } while (v);
    if (en + 2 + kn > sizeof(ev)) return CETCD_ERR_OVERFLOW;
    ev[en++] = 0x12; /* Event field 2 kv */
    v = kn;
    do {
        if (en >= sizeof(ev)) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        ev[en++] = b;
    } while (v);
    memcpy(ev + en, kv, kn);
    en += kn;
    if (pos + 2 + en > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x5a; /* field 11 events */
    v = en;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    memcpy(out + pos, ev, en);
    pos += en;
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_watch_event_kv_lease(const uint8_t *req, size_t len,
                                     int64_t *lease) {
    size_t p = 0;
    if (!lease) return CETCD_ERR_INVAL;
    *lease = 0;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x5a) {
            uint64_t skip = 0;
            size_t ip = 0;
            int rc = leftover_safe_varint_at_(req, len, &p, &skip);
            if (rc != CETCD_OK || p + skip > len) return CETCD_ERR_INVAL;
            *lease = 0;
            while (ip < (size_t)skip) {
                uint8_t et = req[p + ip];
                ip++;
                if (et == 0x00)
                    continue;
                if (et == 0x12) {
                    uint64_t kskip = 0;
                    size_t kp = 0;
                    size_t qp = ip;
                    if (leftover_safe_varint_at_(req + p, (size_t)skip, &qp,
                                                 &kskip) != CETCD_OK
                        || qp + kskip > (size_t)skip)
                        return CETCD_ERR_INVAL;
                    ip = qp;
                    *lease = 0;
                    while (kp < (size_t)kskip) {
                        uint8_t kt = req[p + ip + kp];
                        kp++;
                        if (kt == 0x00)
                            continue;
                        if (kt == 0x30) {
                            uint64_t v = 0;
                            size_t rp = kp;
                            if (leftover_safe_varint_at_(req + p + ip,
                                                         (size_t)kskip, &rp,
                                                         &v) != CETCD_OK)
                                return CETCD_ERR_INVAL;
                            *lease = (int64_t)v;
                            kp = rp;
                            continue;
                        }
                        if (leftover_safe_skip_unknown_at_(req + p + ip,
                                                           (size_t)kskip, &kp,
                                                           kt) != CETCD_OK)
                            return CETCD_ERR_INVAL;
                    }
                    ip += (size_t)kskip;
                    continue;
                }
                if (leftover_safe_skip_unknown_at_(req + p, (size_t)skip, &ip,
                                                   et) != CETCD_OK)
                    return CETCD_ERR_INVAL;
            }
            p += (size_t)skip;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_watch_event_kv_version(int64_t version, uint8_t *out,
                                        size_t cap, size_t *n) {
    uint8_t kv[16];
    uint8_t ev[24];
    size_t kn = 0, en = 0;
    uint64_t v;
    size_t pos = 0;
    if (!out || !n) return CETCD_ERR_INVAL;
    *n = 0;
    if (version == 0) return CETCD_OK;
    if (kn + 2 > sizeof(kv)) return CETCD_ERR_OVERFLOW;
    kv[kn++] = 0x20;
    v = (uint64_t)version;
    do {
        if (kn >= sizeof(kv)) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        kv[kn++] = b;
    } while (v);
    if (en + 2 + kn > sizeof(ev)) return CETCD_ERR_OVERFLOW;
    ev[en++] = 0x12; /* Event field 2 kv */
    v = kn;
    do {
        if (en >= sizeof(ev)) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        ev[en++] = b;
    } while (v);
    memcpy(ev + en, kv, kn);
    en += kn;
    if (pos + 2 + en > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x5a; /* field 11 events */
    v = en;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    memcpy(out + pos, ev, en);
    pos += en;
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_watch_event_kv_version(const uint8_t *req, size_t len,
                                       int64_t *version) {
    size_t p = 0;
    if (!version) return CETCD_ERR_INVAL;
    *version = 0;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x5a) {
            uint64_t skip = 0;
            size_t ip = 0;
            int rc = leftover_safe_varint_at_(req, len, &p, &skip);
            if (rc != CETCD_OK || p + skip > len) return CETCD_ERR_INVAL;
            *version = 0;
            while (ip < (size_t)skip) {
                uint8_t et = req[p + ip];
                ip++;
                if (et == 0x00)
                    continue;
                if (et == 0x12) {
                    uint64_t kskip = 0;
                    size_t kp = 0;
                    size_t qp = ip;
                    if (leftover_safe_varint_at_(req + p, (size_t)skip, &qp,
                                                 &kskip) != CETCD_OK
                        || qp + kskip > (size_t)skip)
                        return CETCD_ERR_INVAL;
                    ip = qp;
                    *version = 0;
                    while (kp < (size_t)kskip) {
                        uint8_t kt = req[p + ip + kp];
                        kp++;
                        if (kt == 0x00)
                            continue;
                        if (kt == 0x20) {
                            uint64_t v = 0;
                            size_t rp = kp;
                            if (leftover_safe_varint_at_(req + p + ip,
                                                         (size_t)kskip, &rp,
                                                         &v) != CETCD_OK)
                                return CETCD_ERR_INVAL;
                            *version = (int64_t)v;
                            kp = rp;
                            continue;
                        }
                        if (leftover_safe_skip_unknown_at_(req + p + ip,
                                                           (size_t)kskip, &kp,
                                                           kt) != CETCD_OK)
                            return CETCD_ERR_INVAL;
                    }
                    ip += (size_t)kskip;
                    continue;
                }
                if (leftover_safe_skip_unknown_at_(req + p, (size_t)skip, &ip,
                                                   et) != CETCD_OK)
                    return CETCD_ERR_INVAL;
            }
            p += (size_t)skip;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_watch_event_kv_create_rev(int64_t create_rev, uint8_t *out,
                                           size_t cap, size_t *n) {
    uint8_t kv[16];
    uint8_t ev[24];
    size_t kn = 0, en = 0;
    uint64_t v;
    size_t pos = 0;
    if (!out || !n) return CETCD_ERR_INVAL;
    *n = 0;
    if (create_rev == 0) return CETCD_OK;
    if (kn + 2 > sizeof(kv)) return CETCD_ERR_OVERFLOW;
    kv[kn++] = 0x10;
    v = (uint64_t)create_rev;
    do {
        if (kn >= sizeof(kv)) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        kv[kn++] = b;
    } while (v);
    if (en + 2 + kn > sizeof(ev)) return CETCD_ERR_OVERFLOW;
    ev[en++] = 0x12; /* Event field 2 kv */
    v = kn;
    do {
        if (en >= sizeof(ev)) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        ev[en++] = b;
    } while (v);
    memcpy(ev + en, kv, kn);
    en += kn;
    if (pos + 2 + en > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x5a; /* field 11 events */
    v = en;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    memcpy(out + pos, ev, en);
    pos += en;
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_watch_event_kv_create_rev(const uint8_t *req, size_t len,
                                          int64_t *create_rev) {
    size_t p = 0;
    if (!create_rev) return CETCD_ERR_INVAL;
    *create_rev = 0;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x5a) {
            uint64_t skip = 0;
            size_t ip = 0;
            int rc = leftover_safe_varint_at_(req, len, &p, &skip);
            if (rc != CETCD_OK || p + skip > len) return CETCD_ERR_INVAL;
            *create_rev = 0;
            while (ip < (size_t)skip) {
                uint8_t et = req[p + ip];
                ip++;
                if (et == 0x00)
                    continue;
                if (et == 0x12) {
                    uint64_t kskip = 0;
                    size_t kp = 0;
                    size_t qp = ip;
                    if (leftover_safe_varint_at_(req + p, (size_t)skip, &qp,
                                                 &kskip) != CETCD_OK
                        || qp + kskip > (size_t)skip)
                        return CETCD_ERR_INVAL;
                    ip = qp;
                    *create_rev = 0;
                    while (kp < (size_t)kskip) {
                        uint8_t kt = req[p + ip + kp];
                        kp++;
                        if (kt == 0x00)
                            continue;
                        if (kt == 0x10) {
                            uint64_t v = 0;
                            size_t rp = kp;
                            if (leftover_safe_varint_at_(req + p + ip,
                                                         (size_t)kskip, &rp,
                                                         &v) != CETCD_OK)
                                return CETCD_ERR_INVAL;
                            *create_rev = (int64_t)v;
                            kp = rp;
                            continue;
                        }
                        if (leftover_safe_skip_unknown_at_(req + p + ip,
                                                           (size_t)kskip, &kp,
                                                           kt) != CETCD_OK)
                            return CETCD_ERR_INVAL;
                    }
                    ip += (size_t)kskip;
                    continue;
                }
                if (leftover_safe_skip_unknown_at_(req + p, (size_t)skip, &ip,
                                                   et) != CETCD_OK)
                    return CETCD_ERR_INVAL;
            }
            p += (size_t)skip;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_watch_event_kv_mod_rev(int64_t mod_rev, uint8_t *out,
                                        size_t cap, size_t *n) {
    uint8_t kv[16];
    uint8_t ev[24];
    size_t kn = 0, en = 0;
    uint64_t v;
    size_t pos = 0;
    if (!out || !n) return CETCD_ERR_INVAL;
    *n = 0;
    if (mod_rev == 0) return CETCD_OK;
    if (kn + 2 > sizeof(kv)) return CETCD_ERR_OVERFLOW;
    kv[kn++] = 0x18;
    v = (uint64_t)mod_rev;
    do {
        if (kn >= sizeof(kv)) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        kv[kn++] = b;
    } while (v);
    if (en + 2 + kn > sizeof(ev)) return CETCD_ERR_OVERFLOW;
    ev[en++] = 0x12; /* Event field 2 kv */
    v = kn;
    do {
        if (en >= sizeof(ev)) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        ev[en++] = b;
    } while (v);
    memcpy(ev + en, kv, kn);
    en += kn;
    if (pos + 2 + en > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x5a; /* field 11 events */
    v = en;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    memcpy(out + pos, ev, en);
    pos += en;
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_watch_event_kv_mod_rev(const uint8_t *req, size_t len,
                                       int64_t *mod_rev) {
    size_t p = 0;
    if (!mod_rev) return CETCD_ERR_INVAL;
    *mod_rev = 0;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x5a) {
            uint64_t skip = 0;
            size_t ip = 0;
            int rc = leftover_safe_varint_at_(req, len, &p, &skip);
            if (rc != CETCD_OK || p + skip > len) return CETCD_ERR_INVAL;
            *mod_rev = 0;
            while (ip < (size_t)skip) {
                uint8_t et = req[p + ip];
                ip++;
                if (et == 0x00)
                    continue;
                if (et == 0x12) {
                    uint64_t kskip = 0;
                    size_t kp = 0;
                    size_t qp = ip;
                    if (leftover_safe_varint_at_(req + p, (size_t)skip, &qp,
                                                 &kskip) != CETCD_OK
                        || qp + kskip > (size_t)skip)
                        return CETCD_ERR_INVAL;
                    ip = qp;
                    *mod_rev = 0;
                    while (kp < (size_t)kskip) {
                        uint8_t kt = req[p + ip + kp];
                        kp++;
                        if (kt == 0x00)
                            continue;
                        if (kt == 0x18) {
                            uint64_t v = 0;
                            size_t rp = kp;
                            if (leftover_safe_varint_at_(req + p + ip,
                                                         (size_t)kskip, &rp,
                                                         &v) != CETCD_OK)
                                return CETCD_ERR_INVAL;
                            *mod_rev = (int64_t)v;
                            kp = rp;
                            continue;
                        }
                        if (leftover_safe_skip_unknown_at_(req + p + ip,
                                                           (size_t)kskip, &kp,
                                                           kt) != CETCD_OK)
                            return CETCD_ERR_INVAL;
                    }
                    ip += (size_t)kskip;
                    continue;
                }
                if (leftover_safe_skip_unknown_at_(req + p, (size_t)skip, &ip,
                                                   et) != CETCD_OK)
                    return CETCD_ERR_INVAL;
            }
            p += (size_t)skip;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_watch_event_kv_value(const char *value, uint8_t *out,
                                      size_t cap, size_t *n) {
    uint8_t kv[64];
    uint8_t ev[80];
    size_t kn = 0, en = 0;
    uint64_t v;
    size_t pos = 0;
    int rc;
    if (!out || !n) return CETCD_ERR_INVAL;
    *n = 0;
    if (!value || !value[0]) return CETCD_OK;
    rc = write_bytes_field_(kv, sizeof(kv), &kn, 0x2a,
                            (const uint8_t *)value, strlen(value));
    if (rc != CETCD_OK) return rc;
    if (en + 2 + kn > sizeof(ev)) return CETCD_ERR_OVERFLOW;
    ev[en++] = 0x12; /* Event field 2 kv */
    v = kn;
    do {
        if (en >= sizeof(ev)) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        ev[en++] = b;
    } while (v);
    memcpy(ev + en, kv, kn);
    en += kn;
    if (pos + 2 + en > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x5a; /* field 11 events */
    v = en;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    memcpy(out + pos, ev, en);
    pos += en;
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_watch_event_kv_value(const uint8_t *req, size_t len,
                                     char *value, size_t value_cap) {
    size_t p = 0;
    if (!value || !value_cap) return CETCD_ERR_INVAL;
    value[0] = '\0';
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x5a) {
            uint64_t skip = 0;
            size_t ip = 0;
            int rc = leftover_safe_varint_at_(req, len, &p, &skip);
            if (rc != CETCD_OK || p + skip > len) return CETCD_ERR_INVAL;
            value[0] = '\0';
            while (ip < (size_t)skip) {
                uint8_t et = req[p + ip];
                ip++;
                if (et == 0x00)
                    continue;
                if (et == 0x12) {
                    uint64_t kskip = 0;
                    size_t kp = 0;
                    size_t qp = ip;
                    if (leftover_safe_varint_at_(req + p, (size_t)skip, &qp,
                                                 &kskip) != CETCD_OK
                        || qp + kskip > (size_t)skip)
                        return CETCD_ERR_INVAL;
                    ip = qp;
                    value[0] = '\0';
                    while (kp < (size_t)kskip) {
                        uint8_t kt = req[p + ip + kp];
                        kp++;
                        if (kt == 0x00)
                            continue;
                        if (kt == 0x2a) {
                            uint64_t sl = 0;
                            size_t rp = kp;
                            if (leftover_safe_varint_at_(req + p + ip,
                                                         (size_t)kskip, &rp,
                                                         &sl) != CETCD_OK
                                || rp + sl > (size_t)kskip)
                                return CETCD_ERR_INVAL;
                            if (sl >= value_cap) return CETCD_ERR_INVAL;
                            memcpy(value, req + p + ip + rp, (size_t)sl);
                            value[sl] = '\0';
                            kp = rp + (size_t)sl;
                            continue;
                        }
                        if (leftover_safe_skip_unknown_at_(req + p + ip,
                                                           (size_t)kskip, &kp,
                                                           kt) != CETCD_OK)
                            return CETCD_ERR_INVAL;
                    }
                    ip += (size_t)kskip;
                    continue;
                }
                if (leftover_safe_skip_unknown_at_(req + p, (size_t)skip, &ip,
                                                   et) != CETCD_OK)
                    return CETCD_ERR_INVAL;
            }
            p += (size_t)skip;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_watch_event_kv_key(const char *key, uint8_t *out, size_t cap,
                                    size_t *n) {
    uint8_t kv[64];
    uint8_t ev[80];
    size_t kn = 0, en = 0;
    uint64_t v;
    size_t pos = 0;
    int rc;
    if (!out || !n) return CETCD_ERR_INVAL;
    *n = 0;
    if (!key || !key[0]) return CETCD_OK;
    rc = write_bytes_field_(kv, sizeof(kv), &kn, 0x0a,
                            (const uint8_t *)key, strlen(key));
    if (rc != CETCD_OK) return rc;
    if (en + 2 + kn > sizeof(ev)) return CETCD_ERR_OVERFLOW;
    ev[en++] = 0x12; /* Event field 2 kv */
    v = kn;
    do {
        if (en >= sizeof(ev)) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        ev[en++] = b;
    } while (v);
    memcpy(ev + en, kv, kn);
    en += kn;
    if (pos + 2 + en > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x5a; /* field 11 events */
    v = en;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    memcpy(out + pos, ev, en);
    pos += en;
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_watch_event_kv_key(const uint8_t *req, size_t len, char *key,
                                   size_t key_cap) {
    size_t p = 0;
    if (!key || !key_cap) return CETCD_ERR_INVAL;
    key[0] = '\0';
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x5a) {
            uint64_t skip = 0;
            size_t ip = 0;
            int rc = leftover_safe_varint_at_(req, len, &p, &skip);
            if (rc != CETCD_OK || p + skip > len) return CETCD_ERR_INVAL;
            key[0] = '\0';
            while (ip < (size_t)skip) {
                uint8_t et = req[p + ip];
                ip++;
                if (et == 0x00)
                    continue;
                if (et == 0x12) {
                    uint64_t kskip = 0;
                    size_t kp = 0;
                    size_t qp = ip;
                    if (leftover_safe_varint_at_(req + p, (size_t)skip, &qp,
                                                 &kskip) != CETCD_OK
                        || qp + kskip > (size_t)skip)
                        return CETCD_ERR_INVAL;
                    ip = qp;
                    key[0] = '\0';
                    while (kp < (size_t)kskip) {
                        uint8_t kt = req[p + ip + kp];
                        kp++;
                        if (kt == 0x00)
                            continue;
                        if (kt == 0x0a) {
                            uint64_t sl = 0;
                            size_t rp = kp;
                            if (leftover_safe_varint_at_(req + p + ip,
                                                         (size_t)kskip, &rp,
                                                         &sl) != CETCD_OK
                                || rp + sl > (size_t)kskip)
                                return CETCD_ERR_INVAL;
                            if (sl >= key_cap) return CETCD_ERR_INVAL;
                            memcpy(key, req + p + ip + rp, (size_t)sl);
                            key[sl] = '\0';
                            kp = rp + (size_t)sl;
                            continue;
                        }
                        if (leftover_safe_skip_unknown_at_(req + p + ip,
                                                           (size_t)kskip, &kp,
                                                           kt) != CETCD_OK)
                            return CETCD_ERR_INVAL;
                    }
                    ip += (size_t)kskip;
                    continue;
                }
                if (leftover_safe_skip_unknown_at_(req + p, (size_t)skip, &ip,
                                                   et) != CETCD_OK)
                    return CETCD_ERR_INVAL;
            }
            p += (size_t)skip;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_watch_event_prev_kv_create_rev(int64_t create_rev,
                                                uint8_t *out, size_t cap,
                                                size_t *n) {
    uint8_t kv[16];
    uint8_t ev[24];
    size_t kn = 0, en = 0;
    uint64_t v;
    size_t pos = 0;
    if (!out || !n) return CETCD_ERR_INVAL;
    *n = 0;
    if (create_rev == 0) return CETCD_OK;
    if (kn + 2 > sizeof(kv)) return CETCD_ERR_OVERFLOW;
    kv[kn++] = 0x10;
    v = (uint64_t)create_rev;
    do {
        if (kn >= sizeof(kv)) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        kv[kn++] = b;
    } while (v);
    if (en + 2 + kn > sizeof(ev)) return CETCD_ERR_OVERFLOW;
    ev[en++] = 0x1a; /* Event field 3 prev_kv */
    v = kn;
    do {
        if (en >= sizeof(ev)) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        ev[en++] = b;
    } while (v);
    memcpy(ev + en, kv, kn);
    en += kn;
    if (pos + 2 + en > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x5a; /* field 11 events */
    v = en;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    memcpy(out + pos, ev, en);
    pos += en;
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_watch_event_prev_kv_create_rev(const uint8_t *req, size_t len,
                                               int64_t *create_rev) {
    size_t p = 0;
    if (!create_rev) return CETCD_ERR_INVAL;
    *create_rev = 0;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x5a) {
            uint64_t skip = 0;
            size_t ip = 0;
            int rc = leftover_safe_varint_at_(req, len, &p, &skip);
            if (rc != CETCD_OK || p + skip > len) return CETCD_ERR_INVAL;
            *create_rev = 0;
            while (ip < (size_t)skip) {
                uint8_t et = req[p + ip];
                ip++;
                if (et == 0x00)
                    continue;
                if (et == 0x1a) {
                    uint64_t kskip = 0;
                    size_t kp = 0;
                    size_t qp = ip;
                    if (leftover_safe_varint_at_(req + p, (size_t)skip, &qp,
                                                 &kskip) != CETCD_OK
                        || qp + kskip > (size_t)skip)
                        return CETCD_ERR_INVAL;
                    ip = qp;
                    *create_rev = 0;
                    while (kp < (size_t)kskip) {
                        uint8_t kt = req[p + ip + kp];
                        kp++;
                        if (kt == 0x00)
                            continue;
                        if (kt == 0x10) {
                            uint64_t v = 0;
                            size_t rp = kp;
                            if (leftover_safe_varint_at_(req + p + ip,
                                                         (size_t)kskip, &rp,
                                                         &v) != CETCD_OK)
                                return CETCD_ERR_INVAL;
                            *create_rev = (int64_t)v;
                            kp = rp;
                            continue;
                        }
                        if (leftover_safe_skip_unknown_at_(req + p + ip,
                                                           (size_t)kskip, &kp,
                                                           kt) != CETCD_OK)
                            return CETCD_ERR_INVAL;
                    }
                    ip += (size_t)kskip;
                    continue;
                }
                if (leftover_safe_skip_unknown_at_(req + p, (size_t)skip, &ip,
                                                   et) != CETCD_OK)
                    return CETCD_ERR_INVAL;
            }
            p += (size_t)skip;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_watch_event_prev_kv_mod_rev(int64_t mod_rev, uint8_t *out,
                                             size_t cap, size_t *n) {
    uint8_t kv[16];
    uint8_t ev[24];
    size_t kn = 0, en = 0;
    uint64_t v;
    size_t pos = 0;
    if (!out || !n) return CETCD_ERR_INVAL;
    *n = 0;
    if (mod_rev == 0) return CETCD_OK;
    if (kn + 2 > sizeof(kv)) return CETCD_ERR_OVERFLOW;
    kv[kn++] = 0x18;
    v = (uint64_t)mod_rev;
    do {
        if (kn >= sizeof(kv)) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        kv[kn++] = b;
    } while (v);
    if (en + 2 + kn > sizeof(ev)) return CETCD_ERR_OVERFLOW;
    ev[en++] = 0x1a; /* Event field 3 prev_kv */
    v = kn;
    do {
        if (en >= sizeof(ev)) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        ev[en++] = b;
    } while (v);
    memcpy(ev + en, kv, kn);
    en += kn;
    if (pos + 2 + en > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x5a; /* field 11 events */
    v = en;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    memcpy(out + pos, ev, en);
    pos += en;
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_watch_event_prev_kv_mod_rev(const uint8_t *req, size_t len,
                                            int64_t *mod_rev) {
    size_t p = 0;
    if (!mod_rev) return CETCD_ERR_INVAL;
    *mod_rev = 0;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x5a) {
            uint64_t skip = 0;
            size_t ip = 0;
            int rc = leftover_safe_varint_at_(req, len, &p, &skip);
            if (rc != CETCD_OK || p + skip > len) return CETCD_ERR_INVAL;
            *mod_rev = 0;
            while (ip < (size_t)skip) {
                uint8_t et = req[p + ip];
                ip++;
                if (et == 0x00)
                    continue;
                if (et == 0x1a) {
                    uint64_t kskip = 0;
                    size_t kp = 0;
                    size_t qp = ip;
                    if (leftover_safe_varint_at_(req + p, (size_t)skip, &qp,
                                                 &kskip) != CETCD_OK
                        || qp + kskip > (size_t)skip)
                        return CETCD_ERR_INVAL;
                    ip = qp;
                    *mod_rev = 0;
                    while (kp < (size_t)kskip) {
                        uint8_t kt = req[p + ip + kp];
                        kp++;
                        if (kt == 0x00)
                            continue;
                        if (kt == 0x18) {
                            uint64_t v = 0;
                            size_t rp = kp;
                            if (leftover_safe_varint_at_(req + p + ip,
                                                         (size_t)kskip, &rp,
                                                         &v) != CETCD_OK)
                                return CETCD_ERR_INVAL;
                            *mod_rev = (int64_t)v;
                            kp = rp;
                            continue;
                        }
                        if (leftover_safe_skip_unknown_at_(req + p + ip,
                                                           (size_t)kskip, &kp,
                                                           kt) != CETCD_OK)
                            return CETCD_ERR_INVAL;
                    }
                    ip += (size_t)kskip;
                    continue;
                }
                if (leftover_safe_skip_unknown_at_(req + p, (size_t)skip, &ip,
                                                   et) != CETCD_OK)
                    return CETCD_ERR_INVAL;
            }
            p += (size_t)skip;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_watch_event_prev_kv_version(int64_t version, uint8_t *out,
                                             size_t cap, size_t *n) {
    uint8_t kv[16];
    uint8_t ev[24];
    size_t kn = 0, en = 0;
    uint64_t v;
    size_t pos = 0;
    if (!out || !n) return CETCD_ERR_INVAL;
    *n = 0;
    if (version == 0) return CETCD_OK;
    if (kn + 2 > sizeof(kv)) return CETCD_ERR_OVERFLOW;
    kv[kn++] = 0x20;
    v = (uint64_t)version;
    do {
        if (kn >= sizeof(kv)) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        kv[kn++] = b;
    } while (v);
    if (en + 2 + kn > sizeof(ev)) return CETCD_ERR_OVERFLOW;
    ev[en++] = 0x1a; /* Event field 3 prev_kv */
    v = kn;
    do {
        if (en >= sizeof(ev)) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        ev[en++] = b;
    } while (v);
    memcpy(ev + en, kv, kn);
    en += kn;
    if (pos + 2 + en > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x5a; /* field 11 events */
    v = en;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    memcpy(out + pos, ev, en);
    pos += en;
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_watch_event_prev_kv_version(const uint8_t *req, size_t len,
                                            int64_t *version) {
    size_t p = 0;
    if (!version) return CETCD_ERR_INVAL;
    *version = 0;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x5a) {
            uint64_t skip = 0;
            size_t ip = 0;
            int rc = leftover_safe_varint_at_(req, len, &p, &skip);
            if (rc != CETCD_OK || p + skip > len) return CETCD_ERR_INVAL;
            *version = 0;
            while (ip < (size_t)skip) {
                uint8_t et = req[p + ip];
                ip++;
                if (et == 0x00)
                    continue;
                if (et == 0x1a) {
                    uint64_t kskip = 0;
                    size_t kp = 0;
                    size_t qp = ip;
                    if (leftover_safe_varint_at_(req + p, (size_t)skip, &qp,
                                                 &kskip) != CETCD_OK
                        || qp + kskip > (size_t)skip)
                        return CETCD_ERR_INVAL;
                    ip = qp;
                    *version = 0;
                    while (kp < (size_t)kskip) {
                        uint8_t kt = req[p + ip + kp];
                        kp++;
                        if (kt == 0x00)
                            continue;
                        if (kt == 0x20) {
                            uint64_t v = 0;
                            size_t rp = kp;
                            if (leftover_safe_varint_at_(req + p + ip,
                                                         (size_t)kskip, &rp,
                                                         &v) != CETCD_OK)
                                return CETCD_ERR_INVAL;
                            *version = (int64_t)v;
                            kp = rp;
                            continue;
                        }
                        if (leftover_safe_skip_unknown_at_(req + p + ip,
                                                           (size_t)kskip, &kp,
                                                           kt) != CETCD_OK)
                            return CETCD_ERR_INVAL;
                    }
                    ip += (size_t)kskip;
                    continue;
                }
                if (leftover_safe_skip_unknown_at_(req + p, (size_t)skip, &ip,
                                                   et) != CETCD_OK)
                    return CETCD_ERR_INVAL;
            }
            p += (size_t)skip;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_watch_event_prev_kv_key(const char *key, uint8_t *out,
                                         size_t cap, size_t *n) {
    uint8_t kv[64];
    uint8_t ev[80];
    size_t kn = 0, en = 0;
    uint64_t v;
    size_t pos = 0;
    int rc;
    if (!out || !n) return CETCD_ERR_INVAL;
    *n = 0;
    if (!key || !key[0]) return CETCD_OK;
    rc = write_bytes_field_(kv, sizeof(kv), &kn, 0x0a,
                            (const uint8_t *)key, strlen(key));
    if (rc != CETCD_OK) return rc;
    if (en + 2 + kn > sizeof(ev)) return CETCD_ERR_OVERFLOW;
    ev[en++] = 0x1a; /* Event field 3 prev_kv */
    v = kn;
    do {
        if (en >= sizeof(ev)) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        ev[en++] = b;
    } while (v);
    memcpy(ev + en, kv, kn);
    en += kn;
    if (pos + 2 + en > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x5a; /* field 11 events */
    v = en;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    memcpy(out + pos, ev, en);
    pos += en;
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_watch_event_prev_kv_key(const uint8_t *req, size_t len,
                                        char *key, size_t key_cap) {
    size_t p = 0;
    if (!key || !key_cap) return CETCD_ERR_INVAL;
    key[0] = '\0';
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x5a) {
            uint64_t skip = 0;
            size_t ip = 0;
            int rc = leftover_safe_varint_at_(req, len, &p, &skip);
            if (rc != CETCD_OK || p + skip > len) return CETCD_ERR_INVAL;
            key[0] = '\0';
            while (ip < (size_t)skip) {
                uint8_t et = req[p + ip];
                ip++;
                if (et == 0x00)
                    continue;
                if (et == 0x1a) {
                    uint64_t kskip = 0;
                    size_t kp = 0;
                    size_t qp = ip;
                    if (leftover_safe_varint_at_(req + p, (size_t)skip, &qp,
                                                 &kskip) != CETCD_OK
                        || qp + kskip > (size_t)skip)
                        return CETCD_ERR_INVAL;
                    ip = qp;
                    key[0] = '\0';
                    while (kp < (size_t)kskip) {
                        uint8_t kt = req[p + ip + kp];
                        kp++;
                        if (kt == 0x00)
                            continue;
                        if (kt == 0x0a) {
                            uint64_t sl = 0;
                            size_t rp = kp;
                            if (leftover_safe_varint_at_(req + p + ip,
                                                         (size_t)kskip, &rp,
                                                         &sl) != CETCD_OK
                                || rp + sl > (size_t)kskip)
                                return CETCD_ERR_INVAL;
                            if (sl >= key_cap) return CETCD_ERR_INVAL;
                            memcpy(key, req + p + ip + rp, (size_t)sl);
                            key[sl] = '\0';
                            kp = rp + (size_t)sl;
                            continue;
                        }
                        if (leftover_safe_skip_unknown_at_(req + p + ip,
                                                           (size_t)kskip, &kp,
                                                           kt) != CETCD_OK)
                            return CETCD_ERR_INVAL;
                    }
                    ip += (size_t)kskip;
                    continue;
                }
                if (leftover_safe_skip_unknown_at_(req + p, (size_t)skip, &ip,
                                                   et) != CETCD_OK)
                    return CETCD_ERR_INVAL;
            }
            p += (size_t)skip;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_watch_event_prev_kv_value(const char *value, uint8_t *out,
                                           size_t cap, size_t *n) {
    uint8_t kv[64];
    uint8_t ev[80];
    size_t kn = 0, en = 0;
    uint64_t v;
    size_t pos = 0;
    int rc;
    if (!out || !n) return CETCD_ERR_INVAL;
    *n = 0;
    if (!value || !value[0]) return CETCD_OK;
    rc = write_bytes_field_(kv, sizeof(kv), &kn, 0x2a,
                            (const uint8_t *)value, strlen(value));
    if (rc != CETCD_OK) return rc;
    if (en + 2 + kn > sizeof(ev)) return CETCD_ERR_OVERFLOW;
    ev[en++] = 0x1a; /* Event field 3 prev_kv */
    v = kn;
    do {
        if (en >= sizeof(ev)) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        ev[en++] = b;
    } while (v);
    memcpy(ev + en, kv, kn);
    en += kn;
    if (pos + 2 + en > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x5a; /* field 11 events */
    v = en;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    memcpy(out + pos, ev, en);
    pos += en;
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_watch_event_prev_kv_value(const uint8_t *req, size_t len,
                                          char *value, size_t value_cap) {
    size_t p = 0;
    if (!value || !value_cap) return CETCD_ERR_INVAL;
    value[0] = '\0';
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x5a) {
            uint64_t skip = 0;
            size_t ip = 0;
            int rc = leftover_safe_varint_at_(req, len, &p, &skip);
            if (rc != CETCD_OK || p + skip > len) return CETCD_ERR_INVAL;
            value[0] = '\0';
            while (ip < (size_t)skip) {
                uint8_t et = req[p + ip];
                ip++;
                if (et == 0x00)
                    continue;
                if (et == 0x1a) {
                    uint64_t kskip = 0;
                    size_t kp = 0;
                    size_t qp = ip;
                    if (leftover_safe_varint_at_(req + p, (size_t)skip, &qp,
                                                 &kskip) != CETCD_OK
                        || qp + kskip > (size_t)skip)
                        return CETCD_ERR_INVAL;
                    ip = qp;
                    value[0] = '\0';
                    while (kp < (size_t)kskip) {
                        uint8_t kt = req[p + ip + kp];
                        kp++;
                        if (kt == 0x00)
                            continue;
                        if (kt == 0x2a) {
                            uint64_t sl = 0;
                            size_t rp = kp;
                            if (leftover_safe_varint_at_(req + p + ip,
                                                         (size_t)kskip, &rp,
                                                         &sl) != CETCD_OK
                                || rp + sl > (size_t)kskip)
                                return CETCD_ERR_INVAL;
                            if (sl >= value_cap) return CETCD_ERR_INVAL;
                            memcpy(value, req + p + ip + rp, (size_t)sl);
                            value[sl] = '\0';
                            kp = rp + (size_t)sl;
                            continue;
                        }
                        if (leftover_safe_skip_unknown_at_(req + p + ip,
                                                           (size_t)kskip, &kp,
                                                           kt) != CETCD_OK)
                            return CETCD_ERR_INVAL;
                    }
                    ip += (size_t)kskip;
                    continue;
                }
                if (leftover_safe_skip_unknown_at_(req + p, (size_t)skip, &ip,
                                                   et) != CETCD_OK)
                    return CETCD_ERR_INVAL;
            }
            p += (size_t)skip;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_watch_fragment(int fragment, uint8_t *out, size_t cap,
                                size_t *n) {
    if (!out || !n) return CETCD_ERR_INVAL;
    *n = 0;
    if (!fragment) return CETCD_OK;
    if (cap < 2) return CETCD_ERR_OVERFLOW;
    out[0] = 0x38; /* field 7 fragment */
    out[1] = 0x01;
    *n = 2;
    return CETCD_OK;
}

int cetcd_parse_watch_fragment(const uint8_t *req, size_t len, int *fragment) {
    size_t p = 0;
    if (!fragment) return CETCD_ERR_INVAL;
    *fragment = 0;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x38) {
            uint64_t v = 0;
            if (leftover_safe_varint_at_(req, len, &p, &v) != CETCD_OK)
                return CETCD_ERR_INVAL;
            *fragment = v != 0;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_watch_fragment_over_budget(size_t encoded,
                                     uint64_t max_request_bytes) {
    if (max_request_bytes == 0)
        max_request_bytes = CETCD_DEFAULT_MAX_REQUEST_BYTES;
    return (uint64_t)encoded > max_request_bytes;
}

int cetcd_encode_watch_cancel_reason(const char *reason, uint8_t *out,
                                     size_t cap, size_t *n) {
    size_t pos = 0;
    int rc;
    if (!out || !n) return CETCD_ERR_INVAL;
    *n = 0;
    if (!reason || !reason[0]) return CETCD_OK;
    rc = write_bytes_field_(out, cap, &pos, 0x32,
                            (const uint8_t *)reason, strlen(reason));
    if (rc != CETCD_OK) return rc;
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_watch_cancel_reason(const uint8_t *req, size_t len,
                                    char *reason, size_t reason_cap) {
    size_t p = 0;
    if (reason && reason_cap)
        reason[0] = '\0';
    if (!reason) return CETCD_ERR_INVAL;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x32) {
            uint64_t skip = 0;
            int rc = leftover_safe_varint_at_(req, len, &p, &skip);
            if (rc != CETCD_OK || p + skip > len) return CETCD_ERR_INVAL;
            if (reason_cap) {
                if (skip >= reason_cap) return CETCD_ERR_INVAL;
                memcpy(reason, req + p, (size_t)skip);
                reason[skip] = '\0';
            }
            p += (size_t)skip;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_delete_range_prev_kv(const char *key, uint8_t *out,
                                      size_t cap, size_t *n) {
    uint8_t kv[64];
    size_t kn = 0;
    uint64_t v;
    size_t pos = 0;
    int rc;
    if (!out || !n || !key || !key[0]) return CETCD_ERR_INVAL;
    rc = write_bytes_field_(kv, sizeof(kv), &kn, 0x0a,
                            (const uint8_t *)key, strlen(key));
    if (rc != CETCD_OK) return rc;
    if (pos + 2 + kn > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x1a; /* field 3 prev_kvs */
    v = kn;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    memcpy(out + pos, kv, kn);
    pos += kn;
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_delete_range_response(const uint8_t *req, size_t len,
                                      char *key, size_t key_cap, size_t *n) {
    size_t p = 0;
    if (key && key_cap)
        key[0] = '\0';
    if (n) *n = 0;
    if (!key && !n) return CETCD_ERR_INVAL;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x1a) {
            uint64_t skip = 0;
            size_t ip = 0;
            int rc = leftover_safe_varint_at_(req, len, &p, &skip);
            if (rc != CETCD_OK || p + skip > len) return CETCD_ERR_INVAL;
            if (n) (*n)++;
            if (key && key_cap) key[0] = '\0';
            while (ip < (size_t)skip) {
                uint8_t kt = req[p + ip];
                ip++;
                if (kt == 0x00)
                    continue;
                if (kt == 0x0a || kt == 0x2a) {
                    uint64_t sl = 0;
                    size_t qp = ip;
                    if (leftover_safe_varint_at_(req + p, (size_t)skip, &qp, &sl)
                        != CETCD_OK || qp + sl > (size_t)skip)
                        return CETCD_ERR_INVAL;
                    if (kt == 0x0a && key && key_cap) {
                        if (sl >= key_cap) return CETCD_ERR_INVAL;
                        memcpy(key, req + p + qp, (size_t)sl);
                        key[sl] = '\0';
                    }
                    ip = qp + (size_t)sl;
                    continue;
                }
                if (leftover_safe_skip_unknown_at_(req + p, (size_t)skip, &ip,
                                                   kt) != CETCD_OK)
                    return CETCD_ERR_INVAL;
            }
            p += (size_t)skip;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_delete_range_deleted(int64_t deleted, uint8_t *out,
                                      size_t cap, size_t *n) {
    size_t pos = 0;
    uint64_t v;
    if (!out || !n || deleted < 0) return CETCD_ERR_INVAL;
    if (pos + 2 > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x10; /* field 2 deleted */
    v = (uint64_t)deleted;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_delete_range_deleted(const uint8_t *req, size_t len,
                                     int64_t *deleted) {
    size_t p = 0;
    if (!deleted) return CETCD_ERR_INVAL;
    *deleted = 0;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x10) {
            uint64_t v = 0;
            if (leftover_safe_varint_at_(req, len, &p, &v) != CETCD_OK)
                return CETCD_ERR_INVAL;
            *deleted = (int64_t)v;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_delete_range_prev_kv_value(const char *value, uint8_t *out,
                                            size_t cap, size_t *n) {
    uint8_t kv[64];
    size_t kn = 0;
    uint64_t v;
    size_t pos = 0;
    int rc;
    if (!out || !n) return CETCD_ERR_INVAL;
    *n = 0;
    if (!value || !value[0]) return CETCD_OK;
    rc = write_bytes_field_(kv, sizeof(kv), &kn, 0x2a,
                            (const uint8_t *)value, strlen(value));
    if (rc != CETCD_OK) return rc;
    if (pos + 2 + kn > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x1a; /* field 3 prev_kvs */
    v = kn;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    memcpy(out + pos, kv, kn);
    pos += kn;
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_delete_range_prev_kv_value(const uint8_t *req, size_t len,
                                           char *value, size_t value_cap) {
    size_t p = 0;
    if (!value || !value_cap) return CETCD_ERR_INVAL;
    value[0] = '\0';
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x1a) {
            uint64_t skip = 0;
            size_t ip = 0;
            int rc = leftover_safe_varint_at_(req, len, &p, &skip);
            if (rc != CETCD_OK || p + skip > len) return CETCD_ERR_INVAL;
            value[0] = '\0';
            while (ip < (size_t)skip) {
                uint8_t kt = req[p + ip];
                ip++;
                if (kt == 0x00)
                    continue;
                if (kt == 0x2a) {
                    uint64_t sl = 0;
                    size_t qp = ip;
                    if (leftover_safe_varint_at_(req + p, (size_t)skip, &qp,
                                                 &sl) != CETCD_OK
                        || qp + sl > (size_t)skip)
                        return CETCD_ERR_INVAL;
                    if (sl >= value_cap) return CETCD_ERR_INVAL;
                    memcpy(value, req + p + qp, (size_t)sl);
                    value[sl] = '\0';
                    ip = qp + (size_t)sl;
                    continue;
                }
                if (leftover_safe_skip_unknown_at_(req + p, (size_t)skip, &ip,
                                                   kt) != CETCD_OK)
                    return CETCD_ERR_INVAL;
            }
            p += (size_t)skip;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_delete_range_prev_kv_lease(int64_t lease, uint8_t *out,
                                            size_t cap, size_t *n) {
    uint8_t kv[16];
    size_t kn = 0;
    uint64_t v;
    size_t pos = 0;
    if (!out || !n) return CETCD_ERR_INVAL;
    *n = 0;
    if (lease == 0) return CETCD_OK;
    if (kn + 2 > sizeof(kv)) return CETCD_ERR_OVERFLOW;
    kv[kn++] = 0x30;
    v = (uint64_t)lease;
    do {
        if (kn >= sizeof(kv)) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        kv[kn++] = b;
    } while (v);
    if (pos + 2 + kn > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x1a; /* field 3 prev_kvs */
    v = kn;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    memcpy(out + pos, kv, kn);
    pos += kn;
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_delete_range_prev_kv_lease(const uint8_t *req, size_t len,
                                           int64_t *lease) {
    size_t p = 0;
    if (!lease) return CETCD_ERR_INVAL;
    *lease = 0;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x1a) {
            uint64_t skip = 0;
            size_t ip = 0;
            int rc = leftover_safe_varint_at_(req, len, &p, &skip);
            if (rc != CETCD_OK || p + skip > len) return CETCD_ERR_INVAL;
            *lease = 0;
            while (ip < (size_t)skip) {
                uint8_t kt = req[p + ip];
                ip++;
                if (kt == 0x00)
                    continue;
                if (kt == 0x30) {
                    uint64_t v = 0;
                    size_t qp = ip;
                    if (leftover_safe_varint_at_(req + p, (size_t)skip, &qp, &v)
                        != CETCD_OK)
                        return CETCD_ERR_INVAL;
                    *lease = (int64_t)v;
                    ip = qp;
                    continue;
                }
                if (leftover_safe_skip_unknown_at_(req + p, (size_t)skip, &ip,
                                                   kt) != CETCD_OK)
                    return CETCD_ERR_INVAL;
            }
            p += (size_t)skip;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_delete_range_prev_kv_version(int64_t version, uint8_t *out,
                                              size_t cap, size_t *n) {
    uint8_t kv[16];
    size_t kn = 0;
    uint64_t v;
    size_t pos = 0;
    if (!out || !n) return CETCD_ERR_INVAL;
    *n = 0;
    if (version == 0) return CETCD_OK;
    if (kn + 2 > sizeof(kv)) return CETCD_ERR_OVERFLOW;
    kv[kn++] = 0x20;
    v = (uint64_t)version;
    do {
        if (kn >= sizeof(kv)) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        kv[kn++] = b;
    } while (v);
    if (pos + 2 + kn > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x1a; /* field 3 prev_kvs */
    v = kn;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    memcpy(out + pos, kv, kn);
    pos += kn;
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_delete_range_prev_kv_version(const uint8_t *req, size_t len,
                                             int64_t *version) {
    size_t p = 0;
    if (!version) return CETCD_ERR_INVAL;
    *version = 0;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x1a) {
            uint64_t skip = 0;
            size_t ip = 0;
            int rc = leftover_safe_varint_at_(req, len, &p, &skip);
            if (rc != CETCD_OK || p + skip > len) return CETCD_ERR_INVAL;
            *version = 0;
            while (ip < (size_t)skip) {
                uint8_t kt = req[p + ip];
                ip++;
                if (kt == 0x00)
                    continue;
                if (kt == 0x20) {
                    uint64_t v = 0;
                    size_t qp = ip;
                    if (leftover_safe_varint_at_(req + p, (size_t)skip, &qp, &v)
                        != CETCD_OK)
                        return CETCD_ERR_INVAL;
                    *version = (int64_t)v;
                    ip = qp;
                    continue;
                }
                if (leftover_safe_skip_unknown_at_(req + p, (size_t)skip, &ip,
                                                   kt) != CETCD_OK)
                    return CETCD_ERR_INVAL;
            }
            p += (size_t)skip;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_delete_range_prev_kv_create_rev(int64_t create_rev,
                                                 uint8_t *out, size_t cap,
                                                 size_t *n) {
    uint8_t kv[16];
    size_t kn = 0;
    uint64_t v;
    size_t pos = 0;
    if (!out || !n) return CETCD_ERR_INVAL;
    *n = 0;
    if (create_rev == 0) return CETCD_OK;
    if (kn + 2 > sizeof(kv)) return CETCD_ERR_OVERFLOW;
    kv[kn++] = 0x10;
    v = (uint64_t)create_rev;
    do {
        if (kn >= sizeof(kv)) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        kv[kn++] = b;
    } while (v);
    if (pos + 2 + kn > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x1a; /* field 3 prev_kvs */
    v = kn;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    memcpy(out + pos, kv, kn);
    pos += kn;
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_delete_range_prev_kv_create_rev(const uint8_t *req, size_t len,
                                                int64_t *create_rev) {
    size_t p = 0;
    if (!create_rev) return CETCD_ERR_INVAL;
    *create_rev = 0;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x1a) {
            uint64_t skip = 0;
            size_t ip = 0;
            int rc = leftover_safe_varint_at_(req, len, &p, &skip);
            if (rc != CETCD_OK || p + skip > len) return CETCD_ERR_INVAL;
            *create_rev = 0;
            while (ip < (size_t)skip) {
                uint8_t kt = req[p + ip];
                ip++;
                if (kt == 0x00)
                    continue;
                if (kt == 0x10) {
                    uint64_t v = 0;
                    size_t qp = ip;
                    if (leftover_safe_varint_at_(req + p, (size_t)skip, &qp, &v)
                        != CETCD_OK)
                        return CETCD_ERR_INVAL;
                    *create_rev = (int64_t)v;
                    ip = qp;
                    continue;
                }
                if (leftover_safe_skip_unknown_at_(req + p, (size_t)skip, &ip,
                                                   kt) != CETCD_OK)
                    return CETCD_ERR_INVAL;
            }
            p += (size_t)skip;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_delete_range_prev_kv_mod_rev(int64_t mod_rev, uint8_t *out,
                                              size_t cap, size_t *n) {
    uint8_t kv[16];
    size_t kn = 0;
    uint64_t v;
    size_t pos = 0;
    if (!out || !n) return CETCD_ERR_INVAL;
    *n = 0;
    if (mod_rev == 0) return CETCD_OK;
    if (kn + 2 > sizeof(kv)) return CETCD_ERR_OVERFLOW;
    kv[kn++] = 0x18;
    v = (uint64_t)mod_rev;
    do {
        if (kn >= sizeof(kv)) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        kv[kn++] = b;
    } while (v);
    if (pos + 2 + kn > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x1a; /* field 3 prev_kvs */
    v = kn;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    memcpy(out + pos, kv, kn);
    pos += kn;
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_delete_range_prev_kv_mod_rev(const uint8_t *req, size_t len,
                                             int64_t *mod_rev) {
    size_t p = 0;
    if (!mod_rev) return CETCD_ERR_INVAL;
    *mod_rev = 0;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x1a) {
            uint64_t skip = 0;
            size_t ip = 0;
            int rc = leftover_safe_varint_at_(req, len, &p, &skip);
            if (rc != CETCD_OK || p + skip > len) return CETCD_ERR_INVAL;
            *mod_rev = 0;
            while (ip < (size_t)skip) {
                uint8_t kt = req[p + ip];
                ip++;
                if (kt == 0x00)
                    continue;
                if (kt == 0x18) {
                    uint64_t v = 0;
                    size_t qp = ip;
                    if (leftover_safe_varint_at_(req + p, (size_t)skip, &qp, &v)
                        != CETCD_OK)
                        return CETCD_ERR_INVAL;
                    *mod_rev = (int64_t)v;
                    ip = qp;
                    continue;
                }
                if (leftover_safe_skip_unknown_at_(req + p, (size_t)skip, &ip,
                                                   kt) != CETCD_OK)
                    return CETCD_ERR_INVAL;
            }
            p += (size_t)skip;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_delete_range_prev_kv_key(const char *key, uint8_t *out,
                                          size_t cap, size_t *n) {
    uint8_t kv[64];
    size_t kn = 0;
    uint64_t v;
    size_t pos = 0;
    int rc;
    if (!out || !n) return CETCD_ERR_INVAL;
    *n = 0;
    if (!key || !key[0]) return CETCD_OK;
    rc = write_bytes_field_(kv, sizeof(kv), &kn, 0x0a,
                            (const uint8_t *)key, strlen(key));
    if (rc != CETCD_OK) return rc;
    if (pos + 2 + kn > cap) return CETCD_ERR_OVERFLOW;
    out[pos++] = 0x1a; /* field 3 prev_kvs */
    v = kn;
    do {
        if (pos >= cap) return CETCD_ERR_OVERFLOW;
        uint8_t b = (uint8_t)(v & 0x7fu);
        v >>= 7;
        if (v) b |= 0x80u;
        out[pos++] = b;
    } while (v);
    memcpy(out + pos, kv, kn);
    pos += kn;
    *n = pos;
    return CETCD_OK;
}

int cetcd_parse_delete_range_prev_kv_key(const uint8_t *req, size_t len,
                                         char *key, size_t key_cap) {
    size_t p = 0;
    if (!key || !key_cap) return CETCD_ERR_INVAL;
    key[0] = '\0';
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x1a) {
            uint64_t skip = 0;
            size_t ip = 0;
            int rc = leftover_safe_varint_at_(req, len, &p, &skip);
            if (rc != CETCD_OK || p + skip > len) return CETCD_ERR_INVAL;
            key[0] = '\0';
            while (ip < (size_t)skip) {
                uint8_t kt = req[p + ip];
                ip++;
                if (kt == 0x00)
                    continue;
                if (kt == 0x0a) {
                    uint64_t sl = 0;
                    size_t qp = ip;
                    if (leftover_safe_varint_at_(req + p, (size_t)skip, &qp,
                                                 &sl) != CETCD_OK
                        || qp + sl > (size_t)skip)
                        return CETCD_ERR_INVAL;
                    if (sl >= key_cap) return CETCD_ERR_INVAL;
                    memcpy(key, req + p + qp, (size_t)sl);
                    key[sl] = '\0';
                    ip = qp + (size_t)sl;
                    continue;
                }
                if (leftover_safe_skip_unknown_at_(req + p, (size_t)skip, &ip,
                                                   kt) != CETCD_OK)
                    return CETCD_ERR_INVAL;
            }
            p += (size_t)skip;
            continue;
        }
        if (leftover_safe_skip_unknown_at_(req, len, &p, tag) != CETCD_OK)
            return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_encode_member_list_request(int linearizable, uint8_t *out, size_t cap,
                                     size_t *n) {
    if (!out || !n || cap < 2) return CETCD_ERR_INVAL;
    out[0] = 0x08; /* field 1, varint */
    out[1] = linearizable ? 1 : 0;
    *n = 2;
    return CETCD_OK;
}

int cetcd_parse_member_list_linearizable(const uint8_t *req, size_t len,
                                         int *out) {
    size_t p = 0;
    if (!out) return CETCD_ERR_INVAL;
    *out = 0;
    if (!req || len == 0) return CETCD_OK;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x08) {
            uint64_t v = 0;
            int shift = 0;
            int got = 0;
            while (p < len) {
                uint8_t b = req[p++];
                v |= (uint64_t)(b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    got = 1;
                    break;
                }
                shift += 7;
                if (shift > 63) return CETCD_ERR_INVAL;
            }
            if (!got) return CETCD_ERR_INVAL;
            *out = v != 0;
            continue;
        }
        if ((tag & 7) == 0) {
            int shift = 0;
            int got = 0;
            while (p < len) {
                uint8_t b = req[p++];
                if ((b & 0x80) == 0) {
                    got = 1;
                    break;
                }
                shift += 7;
                if (shift > 63) return CETCD_ERR_INVAL;
            }
            if (!got) return CETCD_ERR_INVAL;
            continue;
        }
        if ((tag & 7) == 2) {
            uint64_t skip = 0;
            int shift = 0;
            int got = 0;
            while (p < len) {
                uint8_t b = req[p++];
                skip |= (uint64_t)(b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    got = 1;
                    break;
                }
                shift += 7;
                if (shift > 63) return CETCD_ERR_INVAL;
            }
            if (!got || p + skip > len) return CETCD_ERR_INVAL;
            p += (size_t)skip;
            continue;
        }
        return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

static int is_write_out_flag_(const char *arg) {
    if (!arg) return 0;
    if (cetcd_cli_flag_is(arg, "--write-out")) return 1;
    /* -w is a short flag; cetcd_cli_flag_is only matches --long. */
    if (strcmp(arg, "-w") == 0) return 1;
    if (strncmp(arg, "-w=", 3) == 0) return 1;
    return 0;
}

static int skip_write_out_arg_(int *i, int argc, char *const *argv) {
    const char *fmt = NULL;
    if (!is_write_out_flag_(argv[*i])) return 0;
    if (cetcd_take_cli_flag_value(i, argc, argv, &fmt) != CETCD_OK)
        return -1;
    return 1;
}

int cetcd_ctl_parse_compact_argv(int argc, char *const *argv, int start,
                                 int *physical, int64_t *rev) {
    int i;
    if (!argv || !physical || !rev || start < 0 || start > argc)
        return CETCD_ERR_INVAL;
    *physical = 0;
    *rev = 0;
    for (i = start; i < argc; i++) {
        int wr;
        int on = 1;
        if (!argv[i]) return CETCD_ERR_INVAL;
        wr = skip_write_out_arg_(&i, argc, argv);
        if (wr < 0) return CETCD_ERR_INVAL;
        if (wr > 0) continue;
        if (cetcd_cli_flag_is(argv[i], "--physical")) {
            if (cetcd_take_cli_bool_eq(&i, argc, argv, &on) != CETCD_OK)
                return CETCD_ERR_INVAL;
            *physical = on;
            continue;
        }
        if (argv[i][0] == '-') return CETCD_ERR_INVAL;
        if (*rev != 0) return CETCD_ERR_INVAL;
        if (cetcd_parse_i64(argv[i], rev) != CETCD_OK || *rev < 1)
            return CETCD_ERR_INVAL;
    }
    if (*rev < 1) return CETCD_ERR_INVAL;
    return CETCD_OK;
}

int cetcd_ctl_parse_maint_argv(int argc, char *const *argv, int start,
                               int allow_cluster, int *cluster) {
    int i;
    if (!argv || start < 0 || start > argc) return CETCD_ERR_INVAL;
    if (allow_cluster && !cluster) return CETCD_ERR_INVAL;
    if (cluster) *cluster = 0;
    for (i = start; i < argc; i++) {
        int wr;
        int on = 1;
        if (!argv[i]) return CETCD_ERR_INVAL;
        wr = skip_write_out_arg_(&i, argc, argv);
        if (wr < 0) return CETCD_ERR_INVAL;
        if (wr > 0) continue;
        if (allow_cluster && cetcd_cli_flag_is(argv[i], "--cluster")) {
            if (cetcd_take_cli_bool_eq(&i, argc, argv, &on) != CETCD_OK)
                return CETCD_ERR_INVAL;
            *cluster = on;
            continue;
        }
        return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_ctl_parse_defrag_argv(int argc, char *const *argv, int start,
                                int *cluster, const char **data_dir) {
    int i;
    if (!argv || !cluster || !data_dir || start < 0 || start > argc)
        return CETCD_ERR_INVAL;
    *cluster = 0;
    *data_dir = NULL;
    for (i = start; i < argc; i++) {
        int wr;
        int on = 1;
        if (!argv[i]) return CETCD_ERR_INVAL;
        wr = skip_write_out_arg_(&i, argc, argv);
        if (wr < 0) return CETCD_ERR_INVAL;
        if (wr > 0) continue;
        if (cetcd_cli_flag_is(argv[i], "--cluster")) {
            if (cetcd_take_cli_bool_eq(&i, argc, argv, &on) != CETCD_OK)
                return CETCD_ERR_INVAL;
            *cluster = on;
            continue;
        }
        if (cetcd_cli_flag_is(argv[i], "--data-dir")) {
            if (cetcd_take_cli_flag_value(&i, argc, argv, data_dir) != CETCD_OK)
                return CETCD_ERR_INVAL;
            continue;
        }
        return CETCD_ERR_INVAL;
    }
    if (*cluster && *data_dir) return CETCD_ERR_INVAL;
    return CETCD_OK;
}

int cetcd_ctl_parse_member_list_argv(int argc, char *const *argv, int start,
                                     int *linearizable) {
    int i;
    if (!argv || !linearizable || start < 0 || start > argc)
        return CETCD_ERR_INVAL;
    *linearizable = 1; /* etcdctl default */
    for (i = start; i < argc; i++) {
        int wr;
        int on = 1;
        if (!argv[i]) return CETCD_ERR_INVAL;
        wr = skip_write_out_arg_(&i, argc, argv);
        if (wr < 0) return CETCD_ERR_INVAL;
        if (wr > 0) continue;
        if (cetcd_cli_flag_is(argv[i], "--linearizable")) {
            if (cetcd_take_cli_bool_flag(&i, argc, argv, &on) != CETCD_OK)
                return CETCD_ERR_INVAL;
            *linearizable = on;
            continue;
        }
        return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

static int is_perm_type_word_(const char *s) {
    return s && (strcmp(s, "read") == 0 || strcmp(s, "write") == 0 ||
                 strcmp(s, "readwrite") == 0);
}

int cetcd_ctl_parse_role_perm_argv(int argc, char *const *argv, int start,
                                   int need_type, const char **role,
                                   const char **type, const char **key,
                                   const char **range_end, int *prefix,
                                   int *from_key) {
    int i, npos = 0, saw_ddash = 0;
    const char *pos[4];
    if (!argv || !role || !key || !range_end || !prefix || !from_key ||
        start < 0 || start > argc)
        return CETCD_ERR_INVAL;
    if (need_type && !type) return CETCD_ERR_INVAL;
    *role = NULL;
    if (type) *type = NULL;
    *key = NULL;
    *range_end = NULL;
    *prefix = 0;
    *from_key = 0;
    for (i = start; i < argc; i++) {
        int wr;
        int on = 1;
        if (!argv[i]) return CETCD_ERR_INVAL;
        if (!saw_ddash) {
            wr = skip_write_out_arg_(&i, argc, argv);
            if (wr < 0) return CETCD_ERR_INVAL;
            if (wr > 0) continue;
            if (strcmp(argv[i], "--") == 0) {
                saw_ddash = 1;
                continue;
            }
            if (cetcd_cli_flag_is(argv[i], "--prefix")) {
                if (cetcd_take_cli_bool_eq(&i, argc, argv, &on) != CETCD_OK)
                    return CETCD_ERR_INVAL;
                *prefix = on;
                continue;
            }
            if (cetcd_cli_flag_is(argv[i], "--from-key")) {
                if (cetcd_take_cli_bool_flag(&i, argc, argv, &on) != CETCD_OK)
                    return CETCD_ERR_INVAL;
                *from_key = on;
                continue;
            }
            if (cetcd_cli_flag_is(argv[i], "--range-end")) {
                if (cetcd_take_cli_flag_value(&i, argc, argv, range_end) != CETCD_OK)
                    return CETCD_ERR_INVAL;
                continue;
            }
            if (argv[i][0] == '-') return CETCD_ERR_INVAL;
        }
        if (npos >= 4) return CETCD_ERR_INVAL;
        pos[npos++] = argv[i];
    }
    if (need_type) {
        if (npos < 3) return CETCD_ERR_INVAL;
        *role = pos[0];
        *type = pos[1];
        *key = pos[2];
        if (npos == 4) {
            if (*range_end) return CETCD_ERR_INVAL;
            *range_end = pos[3];
        }
        if (!is_perm_type_word_(*type)) return CETCD_ERR_INVAL;
    } else {
        int p = 1;
        if (npos < 1) return CETCD_ERR_INVAL;
        *role = pos[0];
        if (p < npos && is_perm_type_word_(pos[p]) && p + 1 < npos)
            p++;
        if (p < npos) *key = pos[p++];
        if (p < npos) {
            if (*range_end) return CETCD_ERR_INVAL;
            *range_end = pos[p++];
        }
        if (p < npos) return CETCD_ERR_INVAL;
    }
    if (*prefix && *from_key) return CETCD_ERR_INVAL;
    if (*prefix && *range_end) return CETCD_ERR_INVAL;
    if (*from_key && *range_end) return CETCD_ERR_INVAL;
    return CETCD_OK;
}

static int parse_n_names_argv_(int argc, char *const *argv, int start,
                               const char **names, int n) {
    int i, got = 0, saw_ddash = 0;
    if (!argv || !names || n < 1 || start < 0 || start > argc)
        return CETCD_ERR_INVAL;
    for (i = 0; i < n; i++) names[i] = NULL;
    for (i = start; i < argc; i++) {
        int wr;
        if (!argv[i]) return CETCD_ERR_INVAL;
        if (!saw_ddash) {
            wr = skip_write_out_arg_(&i, argc, argv);
            if (wr < 0) return CETCD_ERR_INVAL;
            if (wr > 0) continue;
            if (strcmp(argv[i], "--") == 0) {
                saw_ddash = 1;
                continue;
            }
            if (argv[i][0] == '-') return CETCD_ERR_INVAL;
        }
        if (got >= n) return CETCD_ERR_INVAL;
        names[got++] = argv[i];
    }
    if (got != n) return CETCD_ERR_INVAL;
    return CETCD_OK;
}

int cetcd_ctl_parse_one_name_argv(int argc, char *const *argv, int start,
                                  const char **name) {
    const char *got = NULL;
    int rc;
    if (!name) return CETCD_ERR_INVAL;
    rc = parse_n_names_argv_(argc, argv, start, &got, 1);
    if (rc != CETCD_OK) return rc;
    *name = got;
    return CETCD_OK;
}

int cetcd_ctl_parse_two_name_argv(int argc, char *const *argv, int start,
                                  const char **a, const char **b) {
    const char *got[2];
    int rc;
    if (!a || !b) return CETCD_ERR_INVAL;
    rc = parse_n_names_argv_(argc, argv, start, got, 2);
    if (rc != CETCD_OK) return rc;
    *a = got[0];
    *b = got[1];
    return CETCD_OK;
}

int cetcd_ctl_parse_three_name_argv(int argc, char *const *argv, int start,
                                    const char **a, const char **b,
                                    const char **c) {
    const char *got[3];
    int rc;
    if (!a || !b || !c) return CETCD_ERR_INVAL;
    rc = parse_n_names_argv_(argc, argv, start, got, 3);
    if (rc != CETCD_OK) return rc;
    *a = got[0];
    *b = got[1];
    *c = got[2];
    return CETCD_OK;
}

int cetcd_ctl_parse_completion_argv(int argc, char *const *argv, int start,
                                    const char **shell) {
    int i, saw_ddash = 0;
    if (!argv || !shell || start < 0 || start > argc) return CETCD_ERR_INVAL;
    *shell = NULL;
    for (i = start; i < argc; i++) {
        if (!argv[i]) return CETCD_ERR_INVAL;
        if (!saw_ddash) {
            if (strcmp(argv[i], "--") == 0) {
                saw_ddash = 1;
                continue;
            }
            if (argv[i][0] == '-') return CETCD_ERR_INVAL;
        }
        if (*shell) return CETCD_ERR_INVAL;
        *shell = argv[i];
    }
    if (!*shell) return CETCD_ERR_INVAL;
    if (strcmp(*shell, "bash") != 0 && strcmp(*shell, "zsh") != 0 &&
        strcmp(*shell, "fish") != 0)
        return CETCD_ERR_INVAL;
    return CETCD_OK;
}

int cetcd_ctl_parse_check_argv(int argc, char *const *argv, int start) {
    int i;
    if (!argv || start < 0 || start > argc) return CETCD_ERR_INVAL;
    for (i = start; i < argc; i++) {
        int wr;
        const char *val = NULL;
        if (!argv[i]) return CETCD_ERR_INVAL;
        wr = skip_write_out_arg_(&i, argc, argv);
        if (wr < 0) return CETCD_ERR_INVAL;
        if (wr > 0) continue;
        if (cetcd_cli_flag_is(argv[i], "--load") ||
            cetcd_cli_flag_is(argv[i], "--prefix")) {
            if (cetcd_take_cli_flag_value(&i, argc, argv, &val) != CETCD_OK)
                return CETCD_ERR_INVAL;
            continue;
        }
        return CETCD_ERR_INVAL;
    }
    return CETCD_OK;
}

int cetcd_parse_pprof_seconds(const char *qs, size_t qs_len, int *out) {
    if (!out) return CETCD_ERR_INVAL;
    *out = 30;
    if (!qs || qs_len == 0) return CETCD_OK;
    if (qs_len >= 256) return CETCD_ERR_INVAL;
    char buf[256];
    memcpy(buf, qs, qs_len);
    buf[qs_len] = '\0';
    if (strncmp(buf, "seconds=", 8) != 0) return CETCD_OK;
    int64_t v = 0;
    if (cetcd_parse_i64(buf + 8, &v) != CETCD_OK) return CETCD_ERR_INVAL;
    if (v < 1 || v > 300) return CETCD_ERR_RANGE;
    *out = (int)v;
    return CETCD_OK;
}
