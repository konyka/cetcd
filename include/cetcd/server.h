#ifndef CETCD_SERVER_H_
#define CETCD_SERVER_H_

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "cetcd/base.h"
#include "cetcd/peer.h"
#include "cetcd/metrics.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct cetcd_server cetcd_server;

#define CETCD_MAX_INITIAL_PEERS 32
/* When snapshot_count is 0, rewrite the WAL after this many applies.
 * etcd 3.5 DefaultSnapshotCount. */
#define CETCD_DEFAULT_SNAPSHOT_COUNT 100000ULL
#define CETCD_DEFAULT_MAX_REQUEST_BYTES (1572864ULL) /* etcd 1.5 MiB */
/* etcd DefaultQuotaBytes: --quota-backend-bytes 0 / omitted. */
#define CETCD_DEFAULT_QUOTA_BACKEND_BYTES (2ULL * 1024 * 1024 * 1024)
#define CETCD_DEFAULT_MAX_TXN_OPS 128ULL
#define CETCD_MAX_TXN_OPS 128ULL
#define CETCD_DEFAULT_MAX_LEARNERS 1U
#define CETCD_DEFAULT_TICK_MS 100ULL
#define CETCD_DEFAULT_ELECTION_MS 1000ULL
#define CETCD_MAX_ELECTION_MS 50000ULL
#define CETCD_DEFAULT_RAFT_IO_TIMEOUT_MS 5000ULL
#define CETCD_DEFAULT_SNAPSHOT_CATCHUP_ENTRIES 5000ULL
#define CETCD_DEFAULT_COMPACT_HASH_CHECK_MS 60000ULL
#define CETCD_CONFIG_MAX_PAIRS 128
#define CETCD_CONFIG_KEY_MAX 96
#define CETCD_CONFIG_VAL_MAX 1024
#define CETCD_MAX_LISTEN_URLS 8

typedef struct cetcd_listen_url {
    char host[256];
    uint16_t port;
    int https;
} cetcd_listen_url;

typedef struct cetcd_config_pair {
    char key[CETCD_CONFIG_KEY_MAX];
    char val[CETCD_CONFIG_VAL_MAX];
} cetcd_config_pair;

typedef enum cetcd_auto_compact_mode {
    CETCD_AUTO_COMPACT_OFF = 0,
    CETCD_AUTO_COMPACT_PERIODIC = 1,
    CETCD_AUTO_COMPACT_REVISION = 2
} cetcd_auto_compact_mode;

typedef struct cetcd_auto_compact_state {
    cetcd_auto_compact_mode mode;
    uint64_t retention;       /* periodic: seconds; revision: revs to keep */
    uint64_t window_start_ms;
    int64_t  window_rev;
    uint64_t batch_limit;     /* 0 = unlimited; else max revs per compact */
    int64_t  pending;         /* last due target not yet fully compacted */
    uint64_t sleep_interval_ms; /* 0 = no wait between compact batches */
    uint64_t last_compact_ms;   /* last time next() returned a target */
} cetcd_auto_compact_state;

/* periodic | revision. Empty/unknown is INVAL. */
int cetcd_parse_auto_compaction_mode(const char *s, cetcd_auto_compact_mode *out);
/* 0 disables. periodic: Go duration or bare hours. revision: integer only. */
int cetcd_parse_auto_compaction_retention(const char *s,
                                          cetcd_auto_compact_mode mode,
                                          uint64_t *out);
/* Go duration to seconds. Bare "0" disables. Sub-second rounds up to 1s. */
int cetcd_parse_go_duration_sec(const char *s, uint64_t *out);
/* Go duration to milliseconds. Bare "0" disables. Sub-ms rounds up to 1ms. */
int cetcd_parse_go_duration_ms(const char *s, uint64_t *out);
/* 1 if a periodic check should run. interval 0 never. First call starts the window. */
int cetcd_corrupt_check_due(uint64_t *last_ms, uint64_t interval_sec,
                            uint64_t now_ms);
/* Unset → 0 (off). set uses enabled. */
int cetcd_server_want_compact_hash_check(int set, int enabled);
/* Unset → 60000. set uses ms (0 = every subsequent due). */
uint64_t cetcd_server_compact_hash_check_ms(int set, uint64_t ms);
/* 1 if a compact-hash pass should run. First call starts the window. interval 0 then always. */
int cetcd_compact_hash_check_due(uint64_t *last_ms, uint64_t interval_ms,
                                 uint64_t now_ms);
/* 1 if both revs match and are > 0 but hashes differ. */
int cetcd_compact_hash_mismatch(int64_t local_rev, uint32_t local_hash,
                                int64_t remote_rev, uint32_t remote_hash);
/* Compact target, or 0 if not due. Updates periodic window. */
int64_t cetcd_auto_compact_due(cetcd_auto_compact_state *st,
                               int64_t current_rev, int64_t compacted_rev,
                               uint64_t now_ms);
/* Integer >= 0. 0 = unlimited. Leftover text is INVAL. */
int cetcd_parse_compaction_batch_limit(const char *s, uint64_t *out);
/* Integer >= 0. 0 = unlimited. Leftover text is INVAL. */
int cetcd_parse_max_learners(const char *s, uint32_t *out);
/* Integer seconds > 0. 0 / leftover text / overflow (sec*1e9) is INVAL. */
int cetcd_parse_auth_token_ttl(const char *s, uint64_t *out);
/* Integer years > 0. 0 / leftover text / overflow (years*365 days) is INVAL. */
int cetcd_parse_self_signed_cert_validity(const char *s, uint32_t *out);
/* etcd YAML map (key: value). Comments, quotes, 2-space lists (comma-joined).
 * Nested maps / flow maps / leftover text are INVAL. */
int cetcd_parse_etcd_config_yaml(const char *text, cetcd_config_pair *out,
                                 size_t cap, size_t *n);
/* Skip version/config-file. Emit --key [value] into argv (argv[0] left alone). */
int cetcd_config_pairs_to_flags(const cetcd_config_pair *pairs, size_t n,
                                char **argv, size_t argv_cap,
                                char *store, size_t store_cap, int *argc);
/* Missing file is IO; oversized is OVERFLOW. */
int cetcd_read_config_file(const char *path, char *buf, size_t cap);
/* "etcd Version: X\\nGit SHA: unknown\\nC Standard: C11\\nOS/Arch: os/arch\\n" */
int cetcd_format_etcd_version(char *out, size_t cap);
/* Known etcd 3.5 experimental flags we cannot honor (not implemented). */
#define CETCD_EX_UNSUP_NONE  0
#define CETCD_EX_UNSUP_BOOL  1 /* true/bare is UNSUPPORT; false is OK */
#define CETCD_EX_UNSUP_VALUE 2 /* any presence is UNSUPPORT */
/* arg is --experimental-name or --experimental-name=.... Implemented
 * experimental flags are NONE. */
int cetcd_experimental_unsupported_kind(const char *arg);
/* Known etcd 3.5 flags we cannot honor without lying (gateway/v2/no-fsync). */
#define CETCD_COMPAT_NONE     0
#define CETCD_COMPAT_BOOL_OFF 1 /* false OK; true/bare fail-closed */
#define CETCD_COMPAT_BOOL_ON  2 /* true/bare OK (already on); false fail-closed */
#define CETCD_COMPAT_VALUE    3 /* any presence is UNSUPPORT */
int cetcd_etcd_compat_kind(const char *arg);
/* gone / write-only / write-only-drop-data / write-only-skip-check.
 * not-yet needs v2 and is INVAL. */
int cetcd_parse_v2_deprecation(const char *s);
/* off is OK. on / readonly need the v2 proxy and are INVAL. */
int cetcd_parse_proxy_mode(const char *s);
/* exit is OK. proxy needs the v2 proxy and is INVAL. */
int cetcd_parse_discovery_fallback(const char *s);
/* Known --grpc-keepalive-* names. Unknown names must not swallow argv. */
#define CETCD_KA_NONE     0
#define CETCD_KA_IDLE     1 /* time / interval → TCP_KEEPIDLE */
#define CETCD_KA_TIMEOUT  2 /* TCP_KEEPINTVL */
#define CETCD_KA_MIN_TIME 3 /* accepted duration; not TCP-mappable */
#define CETCD_KA_PERMIT   4 /* accepted bool; not TCP-mappable */
#define CETCD_KA_UNKNOWN  5 /* other --grpc-keepalive-* */
int cetcd_grpc_keepalive_kind(const char *arg);
/* Go duration or bare seconds. Range min_v..86400. Sub-second rounds up. */
int cetcd_parse_grpc_keepalive_sec(const char *s, int min_v, int *out);
/* "listen-client-urls" → "ETCD_LISTEN_CLIENT_URLS". */
int cetcd_flag_to_etcd_env(const char *flag, char *out, size_t cap);
/* "ETCD_LISTEN_CLIENT_URLS" → "listen-client-urls". Other prefixes are INVAL. */
int cetcd_etcd_env_to_flag(const char *env_key, char *out, size_t cap);
/* 1 if argv has --flag or --flag=. flag is without dashes. */
int cetcd_cli_flag_present(int argc, char *const *argv, const char *flag);
/* KEY=VALUE envv (NULL-terminated). Skip VERSION/CONFIG_FILE. A CLI+env
 * conflict is INVAL. Empty values are ignored. Unknown names become --flag
 * and fail later at the existing unknown-flag parser. */
int cetcd_etcd_env_to_pairs(char *const *envv, int argc, char *const *argv,
                            cetcd_config_pair *out, size_t cap, size_t *n);
/* Integer MB >= 0. 0 = off. Overflow (MB*1MiB) is INVAL. */
int cetcd_parse_bootstrap_defrag_mb(const char *s, uint64_t *out);
/* etcd: 0 / omitted → 2GiB. Other values are unchanged. */
uint64_t cetcd_quota_backend_bytes_effective(uint64_t n);
/* etcd: 0 / omitted → 100000. Other values are unchanged. */
uint64_t cetcd_snapshot_count_effective(uint64_t n);
/* 1 if arg is `--name` or `--name=...` (not `--names`). */
int cetcd_cli_flag_is(const char *arg, const char *name);
/* `--flag VALUE` or `--flag=VALUE`. Empty `--flag=` is INVAL. */
int cetcd_take_cli_flag_value(int *i, int argc, char *const *argv,
                              const char **out);
/* Bare `--flag` is true. `--flag=false` / next-arg bool. Non-bool is INVAL. */
int cetcd_take_cli_bool_flag(int *i, int argc, char *const *argv, int *out);
/* Bare `--flag` is true. `--flag=false` only (does not eat the next argv). */
int cetcd_take_cli_bool_eq(int *i, int argc, char *const *argv, int *out);
/* etcdctl --command-timeout: bare seconds or Go duration. Leftover fail-closes. */
int cetcd_parse_command_timeout_sec(const char *s, uint64_t *out);
/* Signed integer. Leftover text / empty is INVAL. */
int cetcd_parse_i64(const char *s, int64_t *out);
/* 1 if alloc_bytes > threshold_mb MiB. threshold 0 never. */
int cetcd_backend_should_defrag(uint64_t alloc_bytes, uint64_t threshold_mb);
/* Cap `target` to compacted+batch_limit. batch_limit 0 leaves target. */
int64_t cetcd_auto_compact_clamp(int64_t target, int64_t compacted_rev,
                                 uint64_t batch_limit);
/* Due target, then drain `pending` in batch_limit steps. */
int64_t cetcd_auto_compact_next(cetcd_auto_compact_state *st,
                                int64_t current_rev, int64_t compacted_rev,
                                uint64_t now_ms);
/* 1 if a compact batch may run. sleep_ms 0 always. last 0 always. */
int cetcd_auto_compact_sleep_ready(uint64_t last_compact_ms, uint64_t now_ms,
                                   uint64_t sleep_ms);

typedef struct cetcd_server_config {
    uint64_t        node_id;
    char            data_dir[512];
    char            wal_dir[512];  /* empty → {data_dir}/wal; dedicated NVMe path */
    char            listen_addr[256];
    uint16_t        listen_port;
    cetcd_listen_url extra_client_urls[CETCD_MAX_LISTEN_URLS];
    uint32_t        n_extra_client_urls;
    char            peer_addr[256];
    uint16_t        peer_port;
    cetcd_listen_url extra_peer_urls[CETCD_MAX_LISTEN_URLS];
    uint32_t        n_extra_peer_urls;
    uint16_t        metrics_port;
    char            metrics_addr[256];            /* empty → listen_addr */
    cetcd_listen_url extra_metrics_urls[CETCD_MAX_LISTEN_URLS];
    uint32_t        n_extra_metrics_urls;
    bool            metrics_port_set;             /* --metrics-port given */
    bool            metrics_urls_set;             /* --listen-metrics-urls given */
    bool            metrics_listen_https;         /* first --listen-metrics-urls is https:// */
    bool            metrics_level_set;            /* --metrics given */
    bool            metrics_extensive;            /* unset → basic (no unary histograms) */
    bool            enable_pprof_set;             /* --enable-pprof given */
    bool            enable_pprof;                 /* unset → off (etcd 3.5) */
    char            host_whitelist[512];          /* empty or * = all; metrics Host */
    uint64_t        election_tick;
    uint64_t        heartbeat_tick;
    bool            election_tick_set;            /* --election-tick given */
    bool            heartbeat_tick_set;           /* --heartbeat-tick given */
    bool            heartbeat_interval_set;       /* --heartbeat-interval given */
    bool            election_timeout_set;         /* --election-timeout given */
    uint64_t        tick_ms;                      /* 0 → 100; Raft/lease timer */
    uint64_t        election_ms;                  /* 0 → 1000 when deriving ticks */
    bool            auth_enabled;
    uint64_t        snapshot_count; /* 0 → CETCD_DEFAULT_SNAPSHOT_COUNT */
    cetcd_peer_info initial_peers[CETCD_MAX_INITIAL_PEERS];
    uint32_t        n_initial_peers;
    /* TLS: both cert and key required; empty = plaintext. Fail-closed. */
    char            cert_file[512];
    char            key_file[512];
    char            trusted_ca_file[512];
    char            client_crl_file[512]; /* inbound client CRL; empty = off */
    bool            client_cert_auth;
    char            peer_cert_file[512];
    char            peer_key_file[512];
    char            peer_client_cert_file[512]; /* outbound; empty → peer_cert_file */
    char            peer_client_key_file[512];
    char            peer_trusted_ca_file[512];
    char            peer_crl_file[512]; /* inbound peer CRL; empty = off */
    bool            peer_client_cert_auth;
    char            peer_cert_allowed_cn[512];        /* empty = no extra peer CN check */
    char            peer_cert_allowed_hostname[512];  /* empty = no extra peer SAN check */
    char            client_cert_allowed_hostname[512]; /* empty = no extra client SAN check */
    /* "simple" (default) or jwt,sign-method=HS256|RS256|ES256,priv-key=... */
    char            auth_token[512];
    uint64_t        auth_token_ttl_sec; /* 0 = unset → 300s; simple tokens only */
    int             bcrypt_cost; /* 0 = SHA-256; 4..31 = bcrypt */
    uint64_t        max_request_bytes;    /* 0 → CETCD_DEFAULT_MAX_REQUEST_BYTES */
    bool            max_concurrent_streams_set; /* --max-concurrent-streams given */
    uint32_t        max_concurrent_streams;     /* HTTP/2 SETTINGS; 0 = unset */
    uint64_t        quota_backend_bytes; /* 0 → CETCD_DEFAULT_QUOTA_BACKEND_BYTES */
    uint64_t        max_txn_ops;          /* 0 → CETCD_DEFAULT_MAX_TXN_OPS; cap CETCD_MAX_TXN_OPS */
    char            cipher_suites[512];   /* empty = OpenSSL default; requires TLS */
    bool            tls_min_version_set;  /* --tls-min-version given */
    int             tls_min_version;      /* CETCD_TLS_VER_1_2|1_3 */
    bool            tls_max_version_set;  /* --tls-max-version given */
    int             tls_max_version;      /* CETCD_TLS_VER_1_2|1_3 */
    bool            listen_https;         /* https:// client URL requires cert_file */
    bool            peer_listen_https;    /* https:// peer URL requires peer_cert_file */
    char            initial_cluster_state[16]; /* empty, "new", or "existing" (evidence / snapshot.kv / peers) */
    bool            force_new_cluster;    /* disaster recover: single-voter, keep MVCC */
    bool            initial_cluster_https; /* https:// peer URL requires peer_cert_file */
    bool            keepalive_set;         /* --grpc-keepalive-time was given */
    int             keepalive_time;        /* 0 disables; else TCP_KEEPIDLE seconds */
    int             keepalive_timeout;     /* 0 = libuv default interval; else TCP_KEEPINTVL */
    bool            auto_tls;              /* mint {data-dir}/fixtures/client.{crt,key} */
    bool            peer_auto_tls;         /* mint {data-dir}/fixtures/peer.{crt,key} */
    uint32_t        self_signed_cert_validity; /* 0 = unset → 1 year; auto-tls mint */
    char            advertise_client_urls[512]; /* MemberList clientURLs; empty → listen */
    char            advertise_peer_urls[512];   /* MemberList self peerURLs; empty → peer listen */
    char            name[128];                  /* MemberList self name; empty → "default" */
    char            initial_cluster_token[128]; /* persisted; mismatch fail-closes */
    char            discovery_srv[256];         /* DNS SRV domain; empty = unused */
    char            discovery_srv_name[64];     /* optional SRV name suffix */
    cetcd_auto_compact_mode auto_compaction_mode; /* OFF unless retention > 0 */
    uint64_t        auto_compaction_retention;    /* seconds or revisions; 0 = off */
    bool            initial_corrupt_check;        /* HashKV vs {data-dir}/backend.hash */
    uint64_t        corrupt_check_interval_sec;   /* 0 = off; periodic HashKV vs backend.hash */
    bool            compact_hash_check_set;       /* --experimental-compact-hash-check-enabled */
    bool            compact_hash_check;           /* unset → off */
    bool            compact_hash_check_time_set;  /* --experimental-compact-hash-check-time */
    uint64_t        compact_hash_check_ms;        /* unset → 60s; 0 = every tick after first */
    uint64_t        compaction_batch_limit;       /* 0 = unlimited auto-compact step */
    uint64_t        compaction_sleep_interval_ms; /* 0 = no wait between compact batches */
    uint64_t        watch_progress_interval_ms;   /* 0 = default 10s; Watch progress_notify */
    bool            warning_apply_set;            /* --experimental-warning-apply-duration given */
    uint64_t        warning_apply_ms;             /* 0 = disable; unset → 100ms */
    bool            warning_unary_set;            /* --experimental-warning-unary-request-duration given */
    uint64_t        warning_unary_ms;             /* 0 = disable; unset → 300ms */
    bool            max_learners_set;             /* --experimental-max-learners given */
    uint32_t        max_learners;                 /* 0 = unlimited; unset → 1 */
    bool            memory_mlock;                 /* --experimental-memory-mlock */
    uint64_t        bootstrap_defrag_mb;          /* 0 = off; else compact if alloc > N MiB */
    bool            pre_vote_set;                 /* --pre-vote given */
    bool            pre_vote;                     /* unset → true (etcd 3.5) */
    bool            strict_reconfig_set;          /* --strict-reconfig-check given */
    bool            strict_reconfig;              /* unset → true */
    bool            tick_advance_set;             /* --initial-election-tick-advance given */
    bool            tick_advance;                 /* unset → true (etcd 3.5) */
    bool            wait_cluster_ready;           /* --experimental-wait-cluster-ready */
    bool            socket_reuse_port_set;        /* --socket-reuse-port given */
    bool            socket_reuse_port;            /* unset → off; Windows enable fail-closes */
    bool            raft_read_timeout_set;        /* --raft-read-timeout given */
    uint64_t        raft_read_timeout_ms;         /* applied via floor helper */
    bool            raft_write_timeout_set;       /* --raft-write-timeout given */
    uint64_t        raft_write_timeout_ms;
    bool            snapshot_catchup_set;         /* --experimental-snapshot-catchup-entries given */
    uint64_t        snapshot_catchup_entries;     /* unset → 5000; 0 = compact to applied */
} cetcd_server_config;

/* true|false|1|0. Empty/unknown is INVAL. */
int cetcd_parse_bool_flag(const char *s, int *out);
/* Unset → 1 (PreVote on). set uses enabled. */
int cetcd_server_want_pre_vote(int set, int enabled);
/* Unset → 1 (AdvanceTicks on). set uses enabled. */
int cetcd_server_want_tick_advance(int set, int enabled);
/* Unset → 0 (do not wait). set uses enabled. */
int cetcd_server_want_wait_cluster_ready(int set, int enabled);
/* Unset → 0 (pprof off). set uses enabled. */
int cetcd_server_want_enable_pprof(int set, int enabled);
/* basic | extensive. Empty/unknown is INVAL. *extensive is 1 for extensive. */
int cetcd_parse_metrics_level(const char *s, int *extensive);
/* Unset → 0 (basic). set uses extensive. */
int cetcd_server_want_metrics_extensive(int set, int extensive);
/* Unset → 0 (SO_REUSEPORT off). set uses enabled. */
int cetcd_server_want_socket_reuse_port(int set, int enabled);
/* enabled 0 is OK. Windows enable is UNSUPPORT. Unix enable is OK (bind sets it). */
int cetcd_socket_reuse_port_apply(int enabled);
/* 0 or UV_TCP_REUSEPORT (2). */
unsigned cetcd_socket_reuse_port_bind_flags(int enabled);
/* 1=/metrics 2=404 3=profile 4=heap 5=coroutines 6=/health. enable_pprof 0 → pprof is 404. */
int cetcd_server_metrics_route(const char *path, size_t path_len, int enable_pprof);
/* 1 healthy. Alarms then leader. serializable skips leader. exclude skips that alarm. */
int cetcd_server_health_ok(int has_leader, int nospace, int corrupt,
                           int serializable, int exclude_nospace, int exclude_corrupt,
                           char *reason, size_t reason_cap);
/* Query after '?'. serializable=true|1; exclude=NOSPACE|CORRUPT. Unknown keys ignored. */
int cetcd_parse_health_query(const char *qs, int *serializable,
                             int *exclude_nospace, int *exclude_corrupt);
/* etcd strings: {"health":"true"} or {"health":"false","reason":"..."}. */
int cetcd_server_health_json(int ok, const char *reason, char *out, size_t cap);
/* 1 if empty or a `*` token (etcd default allow-all). */
int cetcd_server_host_whitelist_open(const char *list);
/* 1 if list is open or host matches a comma token. Missing host is denied. */
int cetcd_server_host_allowed(const char *list, const char *host);
/* Strip :port. [ipv6] keeps the inside. */
int cetcd_http_host_name(const char *hdr, char *out, size_t cap);
/* 1 if \r\n\r\n or \n\n is present. */
int cetcd_http_headers_complete(const char *req, size_t len);
/* Case-insensitive header name. Missing is NOTFOUND. */
int cetcd_http_header_get(const char *req, size_t len, const char *name,
                          char *out, size_t cap);
/* 1 if clients may listen. wait 0 always. wait 1 requires leader_id != 0. */
int cetcd_server_should_listen_clients(int wait_ready, uint64_t leader_id);
/* Go duration to ms. Bare 0 is 0. Leftover text is INVAL. */
int cetcd_parse_raft_io_timeout_ms(const char *s, uint64_t *out);
/* Unset → 5000. set uses max(ms, 5000) (etcd 3.5 floor). */
uint64_t cetcd_server_raft_io_timeout_ms(int set, uint64_t ms);
/* 1 if last_ms started and now-last >= timeout. timeout 0 or last 0 never. */
int cetcd_raft_io_timed_out(uint64_t last_ms, uint64_t now_ms, uint64_t timeout_ms);
/* Integer including 0. Leftover text is INVAL. */
int cetcd_parse_snapshot_catchup_entries(const char *s, uint64_t *out);
/* Unset → 5000. set uses n (0 = keep none). */
uint64_t cetcd_server_snapshot_catchup_entries(int set, uint64_t n);
/* Compact index: 0 applied → 0; catchup 0 → applied; else max(1, applied-catchup). */
uint64_t cetcd_server_raft_compact_index(uint64_t applied, uint64_t catchup);
/* Integer milliseconds > 0 and <= 50000. Leftover text is INVAL. */
int cetcd_parse_heartbeat_interval_ms(const char *s, uint64_t *out);
int cetcd_parse_election_timeout_ms(const char *s, uint64_t *out);
/* heartbeat_tick=1, election_tick=election_ms/tick_ms. 0 ms → defaults.
 * election_ms < tick_ms or > 50000 is INVAL. */
int cetcd_raft_timing_from_ms(uint64_t tick_ms, uint64_t election_ms,
                              uint64_t *heartbeat_tick, uint64_t *election_tick);
/* Unset → 100. */
uint64_t cetcd_server_tick_ms(uint64_t tick_ms);
/* Integer > 0. Leftover text is INVAL. */
int cetcd_parse_max_concurrent_streams(const char *s, uint32_t *out);
/* http:// or https:// host:port (1..65535). No IPv6. Leftover is INVAL. */
int cetcd_parse_listen_url(const char *s, char *host, size_t host_cap,
                           uint16_t *port, int *https);
/* Comma-separated UniqueURLs. Empty token, duplicate host:port, mixed
 * http/https, or leftover is INVAL. */
int cetcd_parse_listen_urls(const char *s, cetcd_listen_url *out, size_t cap,
                            size_t *n);
/* First URL → host/port/https; the rest → extra. */
int cetcd_apply_listen_urls(const char *s, char *host, size_t host_cap,
                            uint16_t *port, int *https,
                            cetcd_listen_url *extra, size_t extra_cap,
                            uint32_t *n_extra);
/* UniqueURLs comma list for MemberList. Mixed http/https is OK.
 * Joins as scheme://host:port. Duplicate / leftover is INVAL. */
int cetcd_parse_advertise_urls(const char *s, char *out, size_t cap);
/* 1 if any token is https://. */
int cetcd_advertise_urls_has_https(const char *s);
/* Primary + extras → comma advertise list (same scheme as listen). */
int cetcd_format_listen_advertise(const char *host, uint16_t port, int https,
                                  const cetcd_listen_url *extra, uint32_t n_extra,
                                  char *out, size_t cap);
/* Append each comma token as a protobuf repeated string (tag). */
int cetcd_pb_append_csv_strings(uint8_t *buf, size_t cap, size_t *pos,
                                uint8_t tag, const char *csv);
/* Single http(s):// URL. comma / leftover is INVAL. */
int cetcd_parse_metrics_listen_url(const char *s, char *host, size_t host_cap,
                                   uint16_t *port);
/* Comma-separated http(s):// UniqueURLs. Mixed scheme OK; duplicate / leftover INVAL. */
int cetcd_parse_metrics_listen_urls(const char *s, cetcd_listen_url *out,
                                    size_t cap, size_t *n);
/* First URL → host/port/https; the rest → extra. https may be NULL. */
int cetcd_apply_metrics_listen_urls(const char *s, char *host, size_t host_cap,
                                    uint16_t *port,
                                    cetcd_listen_url *extra, size_t extra_cap,
                                    uint32_t *n_extra, int *https);
/* 1 if the first URL or any extra is https://. */
int cetcd_metrics_listen_has_https(int first_https,
                                   const cetcd_listen_url *extra,
                                   uint32_t n_extra);
/* metrics_addr if set, else listen_addr. NULL cfg → NULL. */
const char *cetcd_server_metrics_addr(const cetcd_server_config *cfg);
/* enabled 0 is OK. Unix mlockall; Windows UNSUPPORT. mlockall failure is IO. */
int cetcd_mlock_apply(int enabled);
/* Persist / load `{rev} {hash}\n`. Missing file is NOTFOUND; garbage is CORRUPT. */
int cetcd_backend_hash_store(const char *path, int64_t rev, uint32_t hash);
int cetcd_backend_hash_load(const char *path, int64_t *rev, uint32_t *hash);
/* Missing → write. Same rev + hash mismatch or current < stored → CORRUPT.
 * Same rev + match, or current > stored → rewrite current. */
int cetcd_backend_hash_verify(const char *path, int64_t rev, uint32_t hash);

cetcd_server *cetcd_server_new(const cetcd_server_config *cfg);
void          cetcd_server_free(cetcd_server *srv);

/* Lifecycle */
int  cetcd_server_start(cetcd_server *srv);
void cetcd_server_stop(cetcd_server *srv);

/* Apply committed entries from Raft to MVCC store. Call after raft_ready. */
int  cetcd_server_apply(cetcd_server *srv);

/* Process a gRPC request: path + request bytes → response bytes.
   Synchronous dispatch through v3rpc. */
typedef struct {
    uint8_t *data;
    size_t   len;
} cetcd_server_rpc_result;

cetcd_server_rpc_result cetcd_server_handle_rpc(cetcd_server *srv,
                                                  const char *path,
                                                  const uint8_t *req,
                                                  size_t req_len);

cetcd_server_rpc_result cetcd_server_handle_rpc_ex(cetcd_server *srv,
                                                    const char *path,
                                                    const uint8_t *req,
                                                    size_t req_len,
                                                    const char *token);
void cetcd_server_rpc_result_free(cetcd_server_rpc_result *r);

/* Tick the Raft state machine (call periodically). */
void cetcd_server_tick(cetcd_server *srv);

/* Compact MVCC store up to the given revision. */
int  cetcd_server_compact(cetcd_server *srv, int64_t rev);

/* Take a snapshot of the current MVCC state. */
typedef struct cetcd_snap cetcd_snap;
cetcd_snap *cetcd_server_snapshot(cetcd_server *srv);

cetcd_metrics *cetcd_server_metrics(cetcd_server *srv);

/* Start a blocking gRPC listener on listen_addr:listen_port.
   Accepts connections, reads length-prefixed gRPC frames,
   dispatches to v3rpc, sends responses back.
   Returns when cetcd_server_stop is called from another thread. */
int cetcd_server_serve(cetcd_server *srv);

/* Queries */
int64_t  cetcd_server_revision(const cetcd_server *srv);
bool     cetcd_server_is_leader(const cetcd_server *srv);
uint64_t cetcd_server_node_id(const cetcd_server *srv);
size_t   cetcd_server_peer_count(const cetcd_server *srv);

/* Membership */
int  cetcd_server_add_peer(cetcd_server *srv, const cetcd_peer_info *info);
int  cetcd_server_remove_peer(cetcd_server *srv, uint64_t peer_id);
int  cetcd_server_propose_conf_change(cetcd_server *srv, uint64_t peer_id, int change_type);

#ifdef __cplusplus
}
#endif
#endif
