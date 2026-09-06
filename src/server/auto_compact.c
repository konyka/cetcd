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
