#include "cetcd/base.h"
#include "cetcd/server.h"
#include "cetcd/log.h"
#include "cetcd/metrics.h"
#include "cetcd/tls.h"

#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

static cetcd_server *g_srv = NULL;

static void on_signal(int sig) {
    (void)sig;
    if (g_srv) cetcd_server_stop(g_srv);
}

static void print_usage(const char *prog) {
    printf("cetcd v%s — pure-C etcd reimplementation\n",
           cetcd_version());
    printf("Usage: %s [options]\n", prog);
    printf("Options:\n");
    printf("  --version        Print version and exit\n");
    printf("  --config-file FILE  YAML map of flag names (other CLI flags and ETCD_* ignored)\n");
    printf("  ETCD_*              Same as --flag when no --config-file (CLI+env conflict fail-closes)\n");
    printf("  --name NAME      Member name (default: default)\n");
    printf("  --data-dir DIR   Data directory (default: ./data)\n");
    printf("  --wal-dir DIR    WAL directory (default: {data-dir}/wal; empty fail-closes)\n");
    printf("  --listen ADDR    Client listen address (default: 127.0.0.1)\n");
    printf("  --port PORT      Client listen port (default: 2379; 1..65535)\n");
    printf("  --peer ADDR      Peer listen address (default: 127.0.0.1)\n");
    printf("  --peer-port PORT Peer listen port (default: 2380; 1..65535)\n");
    printf("  --metrics-port PORT Metrics listen port (default: 2381; 0 disables; 0..65535; /metrics + /health)\n");
    printf("  --listen-metrics-urls URLS  Metrics listen URLs (http(s):// comma list; https needs --cert-file)\n");
    printf("  --metrics LEVEL   Metrics detail: basic|extensive (default basic; extensive = unary histograms)\n");
    printf("  --socket-reuse-port  SO_REUSEPORT on listeners (default off; Windows fail-closes)\n");
    printf("  --enable-pprof     Expose /debug/pprof/* on the metrics port (default off; true|false)\n");
    printf("  --host-whitelist LIST  Allowed Host names on metrics HTTP (* or empty = all)\n");
    printf("  --node-id ID     Node ID (default: 1; must be > 0)\n");
    printf("  --initial-cluster ID=ADDR:PORT,...  Initial cluster (https requires --peer-cert-file; id > 0; port 1..65535)\n");
    printf("  --election-tick N   Raft election tick (default: 10; must be > 0)\n");
    printf("  --heartbeat-tick N  Raft heartbeat tick (default: 1; must be > 0)\n");
    printf("  --heartbeat-interval MS  Raft tick period in ms (default 100; 1..50000)\n");
    printf("  --election-timeout MS  Election timeout in ms (default 1000; 1..50000; >= interval)\n");
    printf("  --raft-read-timeout DUR   Recycle a hung peer socket (default 5s; values <5s floor to 5s)\n");
    printf("  --raft-write-timeout DUR  Recycle a hung peer write (default 5s; values <5s floor to 5s)\n");
    printf("  --initial-election-tick-advance  Fast first campaign (default on; true|false)\n");
    printf("  --pre-vote          Extra Raft election phase (default on; true|false)\n");
    printf("  --log-level LVL  Log level: trace,debug,info,warn,error (default: info)\n");
    printf("  --log-format FMT Log format: text,json (etcd console = text; others fail)\n");
    printf("\n  etcd-compatible flags (accepted for compatibility):\n");
    printf("  --listen-client-urls URLS   Client listen URLs (comma list; same scheme; https requires --cert-file; port 1..65535)\n");
    printf("  --listen-peer-urls URLS     Peer listen URLs (comma list; same scheme; https requires --peer-cert-file; port 1..65535)\n");
    printf("  --advertise-client-urls URLS  MemberList clientURLs (comma list; https requires --cert-file)\n");
    printf("  --initial-advertise-peer-urls URLS  MemberList peerURLs (comma list; https requires --peer-cert-file)\n");
    printf("  --initial-cluster-state STATE  new (default) or existing (requires persisted cluster)\n");
    printf("  --initial-cluster-token TOKEN  Persist in data-dir; mismatch fail-closes\n");
    printf("  --discovery-srv DOMAIN  Bootstrap peers from DNS SRV (_etcd-server._tcp)\n");
    printf("  --discovery-srv-name NAME  Optional SRV service suffix\n");
    printf("  --snapshot-count N   Rewrite WAL after N applies (default: 100000; must be > 0)\n");
    printf("  --auto-compaction-mode MODE  periodic (default) or revision\n");
    printf("  --auto-compaction-retention N  0 disables; periodic: duration or hours; revision: revs to keep\n");
    printf("  --quota-backend-bytes N  NOSPACE when LMDB size >= N (0 / omitted = 2GiB; invalid fails)\n");
    printf("  --force-new-cluster  Keep MVCC; drop peers except self (requires persisted cluster)\n");
    printf("  --strict-reconfig-check  Reject MemberRemove that loses old quorum (default on; true|false)\n");
    printf("  --max-txn-ops N     Max compare/success/failure ops per Txn (default 128; 1..128)\n");
    printf("  --max-request-bytes N  Max client frame (default 1572864; must be > 0)\n");
    printf("  --max-concurrent-streams N  HTTP/2 SETTINGS_MAX_CONCURRENT_STREAMS (must be > 0)\n");
    printf("  --grpc-keepalive-time SEC   TCP keepalive idle on client and peer sockets (0 disables)\n");
    printf("  --grpc-keepalive-interval SEC  Same as --grpc-keepalive-time (etcd name; 0 disables)\n");
    printf("  --grpc-keepalive-timeout SEC  TCP keepalive interval (requires time or interval)\n");
    printf("  --grpc-keepalive-min-time SEC  Accepted duration (not applied; 0..86400)\n");
    printf("  --grpc-keepalive-permit-without-stream  Accepted bool (not applied; true|false)\n");
    printf("  --auth-token TYPE   simple (default) or jwt,sign-method=HS256|RS256|ES256,priv-key=PATH[,ttl=5m]\n");
    printf("  --auth-token-ttl SEC  Simple-token lifetime in seconds (default 300; must be > 0)\n");
    printf("  --bcrypt-cost N     Hash new passwords with bcrypt (4..31; 0 = SHA-256; invalid fails)\n");
    printf("  --cert-file FILE    Client TLS certificate (requires --key-file)\n");
    printf("  --key-file FILE     Client TLS private key\n");
    printf("  --trusted-ca-file FILE  Client TLS CA (also requires a client cert, like etcd)\n");
    printf("  --client-crl-file FILE  Client cert revocation list (requires --cert-file)\n");
    printf("  --client-cert-auth  Require a client certificate (true|false; CA still requires)\n");
    printf("  --auto-tls           Mint {data-dir}/fixtures/client.{crt,key} if --cert-file omitted\n");
    printf("  --self-signed-cert-validity N  Auto-TLS cert lifetime in years (default 1; must be > 0)\n");
    printf("  --peer-cert-file FILE    Peer accept TLS certificate (requires --peer-key-file)\n");
    printf("  --peer-key-file FILE     Peer accept TLS private key\n");
    printf("  --peer-client-cert-file FILE  Outbound peer TLS cert (default --peer-cert-file)\n");
    printf("  --peer-client-key-file FILE   Outbound peer TLS key (requires --peer-client-cert-file)\n");
    printf("  --peer-trusted-ca-file FILE  Peer TLS CA (also requires a peer cert on accept)\n");
    printf("  --peer-crl-file FILE  Peer cert revocation list (requires --peer-cert-file)\n");
    printf("  --peer-client-cert-auth  Require a peer certificate on accept (true|false; CA still requires)\n");
    printf("  --peer-cert-allowed-cn LIST  Allowed peer cert CNs (requires peer TLS + CA; empty = off)\n");
    printf("  --peer-cert-allowed-hostname LIST  Allowed peer SAN hostnames (requires peer TLS + CA)\n");
    printf("  --client-cert-allowed-hostname LIST  Allowed client SAN hostnames (requires client TLS + CA)\n");
    printf("  --peer-auto-tls      Mint {data-dir}/fixtures/peer.{crt,key} if --peer-cert-file omitted\n");
    printf("  --cipher-suites LIST  TLS 1.2/1.3 cipher list (IANA or OpenSSL names; requires TLS)\n");
    printf("  --tls-min-version VER  Minimum TLS version (TLS1.2 default; TLS1.3)\n");
    printf("  --tls-max-version VER  Maximum TLS version (TLS1.2 or TLS1.3; omitted open)\n");
    printf("  --logger TYPE       zap or capnslog (built-in logger; others fail)\n");
    printf("  --log-outputs LIST   stderr, stdout, file path, or journal/syslog (mixed lists fail)\n");
    printf("  --enable-log-rotation  Rotate a single --log-outputs file (default off; compress unsupported)\n");
    printf("  --log-rotation-config-json JSON  lumberjack maxsize/maxage/maxbackups/localtime (compress=true fails)\n");
    printf("  --experimental-initial-corrupt-check  HashKV vs {data-dir}/backend.hash (fail-closed)\n");
    printf("  --experimental-corrupt-check-time DUR  Periodic HashKV vs backend.hash (0 disables)\n");
    printf("  --experimental-compaction-batch-limit N  Auto-compact at most N revs/tick (0 unlimited)\n");
    printf("  --experimental-compaction-sleep-interval DUR  Wait between auto-compact batches (0 = none)\n");
    printf("  --experimental-watch-progress-notify-interval DUR  Watch progress_notify period (0 = 10s)\n");
    printf("  --experimental-warning-apply-duration DUR  Warn if apply exceeds duration (0 disables; default 100ms)\n");
    printf("  --experimental-warning-unary-request-duration DUR  Warn if unary RPC exceeds duration (0 disables; default 300ms)\n");
    printf("  --experimental-max-learners N  Cap learner MemberAdd (0 = none; omitted default 1)\n");
    printf("  --experimental-memory-mlock  Lock process memory (Unix mlockall; Windows fail-closed)\n");
    printf("  --experimental-bootstrap-defrag-threshold-megabytes N  Compact data.mdb at start if larger (0 off)\n");
    printf("  --experimental-wait-cluster-ready  Delay client listen until a Raft leader exists (true|false)\n");
    printf("  --experimental-snapshot-catchup-entries N  Raft entries kept after compact (default 5000; 0 = none)\n");
    printf("  --experimental-compact-hash-check-enabled  Leader compares follower compact HashKV (default off)\n");
    printf("  --experimental-compact-hash-check-time DUR  Compact HashKV compare period (default 1m; 0 = every tick)\n");
    printf("  --experimental-*    Unimplemented or unknown experimental flags fail at parse (false is OK)\n");
    printf("  --help           Show this help\n");
}

static int take_flag_value_(int *i, int argc, char **argv, const char **out) {
    return cetcd_take_cli_flag_value(i, argc, argv, out) == CETCD_OK ? 0 : -1;
}

static int take_bool_flag_(int *i, int argc, char **argv, int *out) {
    return cetcd_take_cli_bool_flag(i, argc, argv, out) == CETCD_OK ? 0 : -1;
}

static int print_version_(void) {
    char buf[256];
    if (cetcd_format_etcd_version(buf, sizeof(buf)) != CETCD_OK)
        return 1;
    fputs(buf, stdout);
    return 0;
}

static int apply_config_file_(const char *path, int *argc, char ***argv,
                              char **flag_argv, size_t flag_argv_cap,
                              char *store, size_t store_cap,
                              char *text, size_t text_cap,
                              cetcd_config_pair *pairs, size_t pair_cap) {
    if (cetcd_read_config_file(path, text, text_cap) != CETCD_OK) {
        fprintf(stderr, "--config-file cannot read %s\n", path);
        return 1;
    }
    size_t n = 0;
    if (cetcd_parse_etcd_config_yaml(text, pairs, pair_cap, &n) != CETCD_OK) {
        fprintf(stderr, "--config-file %s is not a valid etcd YAML map\n", path);
        return 1;
    }
    flag_argv[0] = (*argv)[0];
    int ac = 1;
    if (cetcd_config_pairs_to_flags(pairs, n, flag_argv, flag_argv_cap,
                                    store, store_cap, &ac) != CETCD_OK) {
        fprintf(stderr, "--config-file %s has too many settings\n", path);
        return 1;
    }
    *argc = ac;
    *argv = flag_argv;
    return 0;
}

#if defined(_WIN32)
#define CETCD_PROCESS_ENVIRON _environ
#else
extern char **environ;
#define CETCD_PROCESS_ENVIRON environ
#endif

static int apply_etcd_env_(int *argc, char ***argv,
                           char **flag_argv, size_t flag_argv_cap,
                           char *store, size_t store_cap,
                           cetcd_config_pair *pairs, size_t pair_cap) {
    size_t n = 0;
    if (cetcd_etcd_env_to_pairs(CETCD_PROCESS_ENVIRON, *argc, *argv,
                                pairs, pair_cap, &n) != CETCD_OK) {
        fprintf(stderr,
                "ETCD_* environment conflicts with a CLI flag or is invalid\n");
        return 1;
    }
    if (n == 0) return 0;
    if ((size_t)*argc >= flag_argv_cap) {
        fprintf(stderr, "ETCD_* environment has too many settings\n");
        return 1;
    }
    for (int i = 0; i < *argc; i++)
        flag_argv[i] = (*argv)[i];
    int ac = *argc;
    if (cetcd_config_pairs_to_flags(pairs, n, flag_argv, flag_argv_cap,
                                    store, store_cap, &ac) != CETCD_OK) {
        fprintf(stderr, "ETCD_* environment has too many settings\n");
        return 1;
    }
    *argc = ac;
    *argv = flag_argv;
    return 0;
}

static int apply_client_listen_urls_(const char *s, cetcd_server_config *cfg) {
    int https = 0;
    if (cetcd_apply_listen_urls(s, cfg->listen_addr, sizeof(cfg->listen_addr),
                                &cfg->listen_port, &https,
                                cfg->extra_client_urls, CETCD_MAX_LISTEN_URLS,
                                &cfg->n_extra_client_urls) != CETCD_OK) {
        fprintf(stderr,
                "--listen-client-urls must be a unique http(s)://host:port list "
                "(same scheme; port 1..65535)\n");
        return 1;
    }
    cfg->listen_https = https != 0;
    return 0;
}

static int apply_peer_listen_urls_(const char *s, cetcd_server_config *cfg) {
    int https = 0;
    if (cetcd_apply_listen_urls(s, cfg->peer_addr, sizeof(cfg->peer_addr),
                                &cfg->peer_port, &https,
                                cfg->extra_peer_urls, CETCD_MAX_LISTEN_URLS,
                                &cfg->n_extra_peer_urls) != CETCD_OK) {
        fprintf(stderr,
                "--listen-peer-urls must be a unique http(s)://host:port list "
                "(same scheme; port 1..65535)\n");
        return 1;
    }
    cfg->peer_listen_https = https != 0;
    return 0;
}

static int apply_advertise_client_urls_(const char *s, cetcd_server_config *cfg) {
    if (cetcd_parse_advertise_urls(s, cfg->advertise_client_urls,
                                   sizeof(cfg->advertise_client_urls)) != CETCD_OK) {
        fprintf(stderr,
                "--advertise-client-urls must be a unique http(s)://host:port list\n");
        return 1;
    }
    return 0;
}

static int apply_advertise_peer_urls_(const char *s, cetcd_server_config *cfg) {
    if (cetcd_parse_advertise_urls(s, cfg->advertise_peer_urls,
                                   sizeof(cfg->advertise_peer_urls)) != CETCD_OK) {
        fprintf(stderr,
                "--initial-advertise-peer-urls must be a unique http(s)://host:port list\n");
        return 1;
    }
    return 0;
}

int main(int argc, char **argv) {
    const char *name = "default";
    const char *data_dir = "./data";
    const char *ac_mode_s = NULL;
    const char *ac_ret_s = NULL;
    FILE *log_owned = NULL;
    const char *log_out_spec = NULL;
    int enable_log_rotation = 0;
    int enable_log_rotation_set = 0;
    cetcd_log_rotation_cfg log_rot_cfg;
    cetcd_log_rotation_cfg_default(&log_rot_cfg);

    cetcd_server_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.node_id = 1;
    strncpy(cfg.listen_addr, "127.0.0.1", sizeof(cfg.listen_addr) - 1);
    cfg.listen_port = 2379;
    strncpy(cfg.peer_addr, "127.0.0.1", sizeof(cfg.peer_addr) - 1);
    cfg.peer_port = 2380;
    cfg.metrics_port = 2381;
    cfg.election_tick = 10;
    cfg.heartbeat_tick = 1;

    {
        const char *config_file = NULL;
        for (int i = 1; i < argc; i++) {
            if (strcmp(argv[i], "--version") == 0) {
                if (i + 1 < argc && argv[i + 1][0] != '-') {
                    int b = 0;
                    if (cetcd_parse_bool_flag(argv[++i], &b) != CETCD_OK) {
                        fprintf(stderr, "--version must be true or false\n");
                        return 1;
                    }
                    if (b) return print_version_();
                } else {
                    return print_version_();
                }
            } else if (strncmp(argv[i], "--version=", 10) == 0) {
                int b = 0;
                if (cetcd_parse_bool_flag(argv[i] + 10, &b) != CETCD_OK) {
                    fprintf(stderr, "--version must be true or false\n");
                    return 1;
                }
                if (b) return print_version_();
            } else if (strcmp(argv[i], "--config-file") == 0) {
                if (i + 1 >= argc || argv[i + 1][0] == '-' || !argv[i + 1][0]) {
                    fprintf(stderr, "--config-file requires a file\n");
                    return 1;
                }
                config_file = argv[++i];
            } else if (strncmp(argv[i], "--config-file=", 14) == 0) {
                if (!argv[i][14]) {
                    fprintf(stderr, "--config-file requires a file\n");
                    return 1;
                }
                config_file = argv[i] + 14;
            }
        }
        if (!config_file) {
            const char *e = getenv("ETCD_CONFIG_FILE");
            if (e && e[0]) config_file = e;
        }
        {
            int want_help = 0;
            for (int hi = 1; hi < argc; hi++) {
                if (strcmp(argv[hi], "--help") == 0) {
                    want_help = 1;
                    break;
                }
            }
            if (config_file && want_help)
                config_file = NULL;
        }
        if (config_file) {
            static char cfg_text[65536];
            static cetcd_config_pair cfg_pairs[CETCD_CONFIG_MAX_PAIRS];
            static char cfg_store[65536];
            static char *cfg_argv[1 + CETCD_CONFIG_MAX_PAIRS * 2];
            if (apply_config_file_(config_file, &argc, &argv, cfg_argv,
                                   sizeof(cfg_argv) / sizeof(cfg_argv[0]),
                                   cfg_store, sizeof(cfg_store),
                                   cfg_text, sizeof(cfg_text),
                                   cfg_pairs, CETCD_CONFIG_MAX_PAIRS) != 0)
                return 1;
        } else {
            static cetcd_config_pair env_pairs[CETCD_CONFIG_MAX_PAIRS];
            static char env_store[65536];
            static char *env_argv[256 + CETCD_CONFIG_MAX_PAIRS * 2];
            if (apply_etcd_env_(&argc, &argv, env_argv,
                                sizeof(env_argv) / sizeof(env_argv[0]),
                                env_store, sizeof(env_store),
                                env_pairs, CETCD_CONFIG_MAX_PAIRS) != 0)
                return 1;
        }
    }

    for (int i = 1; i < argc; i++) {
        if (cetcd_cli_flag_is(argv[i], "--name")) {
            const char *s = NULL;
            if (take_flag_value_(&i, argc, argv, &s) != 0) {
                fprintf(stderr, "--name requires a value\n");
                return 1;
            }
            name = s;
        } else if (cetcd_cli_flag_is(argv[i], "--data-dir")) {
            const char *s = NULL;
            if (take_flag_value_(&i, argc, argv, &s) != 0) {
                fprintf(stderr, "--data-dir requires a path\n");
                return 1;
            }
            data_dir = s;
        } else if (cetcd_cli_flag_is(argv[i], "--wal-dir")) {
            const char *wd = NULL;
            if (take_flag_value_(&i, argc, argv, &wd) != 0 ||
                !wd[0] || strlen(wd) >= sizeof(cfg.wal_dir)) {
                fprintf(stderr, "--wal-dir must be a non-empty path\n");
                return 1;
            }
            strncpy(cfg.wal_dir, wd, sizeof(cfg.wal_dir) - 1);
        } else if (cetcd_cli_flag_is(argv[i], "--listen")) {
            const char *s = NULL;
            if (take_flag_value_(&i, argc, argv, &s) != 0) {
                fprintf(stderr, "--listen requires an address\n");
                return 1;
            }
            strncpy(cfg.listen_addr, s, sizeof(cfg.listen_addr) - 1);
        } else if (cetcd_cli_flag_is(argv[i], "--port")) {
            const char *s = NULL;
            char *end = NULL;
            errno = 0;
            if (take_flag_value_(&i, argc, argv, &s) != 0) {
                fprintf(stderr, "--port must be 1..65535\n");
                return 1;
            }
            long v = strtol(s, &end, 10);
            if (errno == ERANGE || !end || *end || v < 1 || v > 65535) {
                fprintf(stderr, "--port must be 1..65535\n");
                return 1;
            }
            cfg.listen_port = (uint16_t)v;
        } else if (cetcd_cli_flag_is(argv[i], "--peer")) {
            const char *s = NULL;
            if (take_flag_value_(&i, argc, argv, &s) != 0) {
                fprintf(stderr, "--peer requires an address\n");
                return 1;
            }
            strncpy(cfg.peer_addr, s, sizeof(cfg.peer_addr) - 1);
        } else if (cetcd_cli_flag_is(argv[i], "--peer-port")) {
            const char *s = NULL;
            char *end = NULL;
            errno = 0;
            if (take_flag_value_(&i, argc, argv, &s) != 0) {
                fprintf(stderr, "--peer-port must be 1..65535\n");
                return 1;
            }
            long v = strtol(s, &end, 10);
            if (errno == ERANGE || !end || *end || v < 1 || v > 65535) {
                fprintf(stderr, "--peer-port must be 1..65535\n");
                return 1;
            }
            cfg.peer_port = (uint16_t)v;
        } else if (cetcd_cli_flag_is(argv[i], "--metrics-port")) {
            const char *s = NULL;
            char *end = NULL;
            errno = 0;
            if (take_flag_value_(&i, argc, argv, &s) != 0) {
                fprintf(stderr, "--metrics-port must be 0..65535\n");
                return 1;
            }
            long v = strtol(s, &end, 10);
            if (errno == ERANGE || !end || *end || v < 0 || v > 65535) {
                fprintf(stderr, "--metrics-port must be 0..65535\n");
                return 1;
            }
            cfg.metrics_port = (uint16_t)v;
            cfg.metrics_port_set = true;
        } else if (strcmp(argv[i], "--metrics") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr, "--metrics must be basic or extensive\n");
                return 1;
            }
            int ext = 0;
            if (cetcd_parse_metrics_level(argv[++i], &ext) != CETCD_OK) {
                fprintf(stderr, "--metrics must be basic or extensive\n");
                return 1;
            }
            cfg.metrics_level_set = true;
            cfg.metrics_extensive = ext != 0;
        } else if (strncmp(argv[i], "--metrics=", 10) == 0) {
            int ext = 0;
            if (cetcd_parse_metrics_level(argv[i] + 10, &ext) != CETCD_OK) {
                fprintf(stderr, "--metrics must be basic or extensive\n");
                return 1;
            }
            cfg.metrics_level_set = true;
            cfg.metrics_extensive = ext != 0;
        } else if (strcmp(argv[i], "--socket-reuse-port") == 0) {
            cfg.socket_reuse_port_set = true;
            cfg.socket_reuse_port = true;
            if (i + 1 < argc && argv[i + 1][0] != '-') {
                int b = 0;
                if (cetcd_parse_bool_flag(argv[++i], &b) != CETCD_OK) {
                    fprintf(stderr, "--socket-reuse-port must be true or false\n");
                    return 1;
                }
                cfg.socket_reuse_port = b != 0;
            }
        } else if (strncmp(argv[i], "--socket-reuse-port=", 20) == 0) {
            int b = 0;
            if (cetcd_parse_bool_flag(argv[i] + 20, &b) != CETCD_OK) {
                fprintf(stderr, "--socket-reuse-port must be true or false\n");
                return 1;
            }
            cfg.socket_reuse_port_set = true;
            cfg.socket_reuse_port = b != 0;
        } else if (strcmp(argv[i], "--enable-pprof") == 0) {
            cfg.enable_pprof_set = true;
            cfg.enable_pprof = true;
            if (i + 1 < argc && argv[i + 1][0] != '-') {
                int b = 0;
                if (cetcd_parse_bool_flag(argv[++i], &b) != CETCD_OK) {
                    fprintf(stderr, "--enable-pprof must be true or false\n");
                    return 1;
                }
                cfg.enable_pprof = b != 0;
            }
        } else if (strncmp(argv[i], "--enable-pprof=", 15) == 0) {
            int b = 0;
            if (cetcd_parse_bool_flag(argv[i] + 15, &b) != CETCD_OK) {
                fprintf(stderr, "--enable-pprof must be true or false\n");
                return 1;
            }
            cfg.enable_pprof_set = true;
            cfg.enable_pprof = b != 0;
        } else if (strcmp(argv[i], "--host-whitelist") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr, "--host-whitelist requires a host list\n");
                return 1;
            }
            strncpy(cfg.host_whitelist, argv[++i], sizeof(cfg.host_whitelist) - 1);
        } else if (strncmp(argv[i], "--host-whitelist=", 17) == 0) {
            strncpy(cfg.host_whitelist, argv[i] + 17, sizeof(cfg.host_whitelist) - 1);
        } else if (strcmp(argv[i], "--listen-metrics-urls") == 0) {
            if (i + 1 >= argc || argv[i + 1][0] == '-' || !argv[i + 1][0]) {
                fprintf(stderr, "--listen-metrics-urls requires a URL list\n");
                return 1;
            }
            {
                int https = 0;
                if (cetcd_apply_metrics_listen_urls(
                        argv[++i], cfg.metrics_addr, sizeof(cfg.metrics_addr),
                        &cfg.metrics_port, cfg.extra_metrics_urls,
                        CETCD_MAX_LISTEN_URLS, &cfg.n_extra_metrics_urls,
                        &https) != CETCD_OK) {
                    fprintf(stderr,
                            "--listen-metrics-urls must be a unique http(s)://host:port list "
                            "(port 1..65535)\n");
                    return 1;
                }
                cfg.metrics_listen_https = https != 0;
            }
            cfg.metrics_urls_set = true;
        } else if (strncmp(argv[i], "--listen-metrics-urls=", 22) == 0) {
            if (!argv[i][22]) {
                fprintf(stderr, "--listen-metrics-urls requires a URL list\n");
                return 1;
            }
            {
                int https = 0;
                if (cetcd_apply_metrics_listen_urls(
                        argv[i] + 22, cfg.metrics_addr, sizeof(cfg.metrics_addr),
                        &cfg.metrics_port, cfg.extra_metrics_urls,
                        CETCD_MAX_LISTEN_URLS, &cfg.n_extra_metrics_urls,
                        &https) != CETCD_OK) {
                    fprintf(stderr,
                            "--listen-metrics-urls must be a unique http(s)://host:port list "
                            "(port 1..65535)\n");
                    return 1;
                }
                cfg.metrics_listen_https = https != 0;
            }
            cfg.metrics_urls_set = true;
        } else if (cetcd_cli_flag_is(argv[i], "--node-id")) {
            const char *s = NULL;
            char *end = NULL;
            errno = 0;
            if (take_flag_value_(&i, argc, argv, &s) != 0) {
                fprintf(stderr, "--node-id must be > 0\n");
                return 1;
            }
            unsigned long long v = strtoull(s, &end, 10);
            if (errno == ERANGE || !end || *end || v == 0) {
                fprintf(stderr, "--node-id must be > 0\n");
                return 1;
            }
            cfg.node_id = (uint64_t)v;
        } else if (cetcd_cli_flag_is(argv[i], "--initial-cluster")) {
            const char *cluster_str = NULL;
            if (take_flag_value_(&i, argc, argv, &cluster_str) != 0) {
                fprintf(stderr, "--initial-cluster requires a member list\n");
                return 1;
            }
            int https = 0;
            uint32_t n = 0;
            int prc = cetcd_parse_initial_cluster(cluster_str, cfg.initial_peers,
                                                  CETCD_MAX_INITIAL_PEERS, &n,
                                                  &https);
            if (prc == CETCD_ERR_RANGE) {
                fprintf(stderr, "--initial-cluster port must be 1..65535\n");
                return 1;
            }
            if (prc != CETCD_OK) {
                fprintf(stderr, "--initial-cluster member id must be > 0\n");
                return 1;
            }
            cfg.n_initial_peers = n;
            if (https) cfg.initial_cluster_https = true;
        } else if (strcmp(argv[i], "--log-level") == 0 ||
                   strncmp(argv[i], "--log-level=", 12) == 0) {
            const char *lvl = NULL;
            cetcd_log_level parsed = CETCD_LOG_INFO;
            if (take_flag_value_(&i, argc, argv, &lvl) != 0 ||
                cetcd_parse_log_level(lvl, &parsed) != CETCD_OK) {
                fprintf(stderr,
                        "--log-level %s is not supported (trace, debug, info, warn, error)\n",
                        lvl ? lvl : "");
                return 1;
            }
            cetcd_log_set_level(parsed);
        } else if (strcmp(argv[i], "--log-format") == 0 ||
                   strncmp(argv[i], "--log-format=", 13) == 0) {
            const char *fmt = NULL;
            cetcd_log_format parsed = CETCD_LOG_FORMAT_TEXT;
            if (take_flag_value_(&i, argc, argv, &fmt) != 0 ||
                cetcd_parse_log_format(fmt, &parsed) != CETCD_OK) {
                fprintf(stderr,
                        "--log-format %s is not supported (text, json)\n",
                        fmt ? fmt : "");
                return 1;
            }
            cetcd_log_set_format(parsed);
        } else if (strcmp(argv[i], "--help") == 0) {
            print_usage(argv[0]);
            return 0;
        } else if (strcmp(argv[i], "--version") == 0) {
            if (i + 1 < argc && argv[i + 1][0] != '-') i++;
        } else if (strncmp(argv[i], "--version=", 10) == 0) {
            /* handled in the pre-scan */
        } else if (strcmp(argv[i], "--config-file") == 0) {
            if (i + 1 < argc && argv[i + 1][0] != '-') i++;
        } else if (strncmp(argv[i], "--config-file=", 14) == 0) {
            /* handled in the pre-scan */
        } else if (strcmp(argv[i], "--listen-client-urls") == 0) {
            if (i + 1 >= argc || argv[i + 1][0] == '-' || !argv[i + 1][0]) {
                fprintf(stderr, "--listen-client-urls requires a URL list\n");
                return 1;
            }
            if (apply_client_listen_urls_(argv[++i], &cfg) != 0) return 1;
        } else if (strncmp(argv[i], "--listen-client-urls=", 21) == 0) {
            if (!argv[i][21]) {
                fprintf(stderr, "--listen-client-urls requires a URL list\n");
                return 1;
            }
            if (apply_client_listen_urls_(argv[i] + 21, &cfg) != 0) return 1;
        } else if (strcmp(argv[i], "--listen-peer-urls") == 0) {
            if (i + 1 >= argc || argv[i + 1][0] == '-' || !argv[i + 1][0]) {
                fprintf(stderr, "--listen-peer-urls requires a URL list\n");
                return 1;
            }
            if (apply_peer_listen_urls_(argv[++i], &cfg) != 0) return 1;
        } else if (strncmp(argv[i], "--listen-peer-urls=", 19) == 0) {
            if (!argv[i][19]) {
                fprintf(stderr, "--listen-peer-urls requires a URL list\n");
                return 1;
            }
            if (apply_peer_listen_urls_(argv[i] + 19, &cfg) != 0) return 1;
        } else if (cetcd_cli_flag_is(argv[i], "--election-tick")) {
            const char *s = NULL;
            char *end = NULL;
            errno = 0;
            if (take_flag_value_(&i, argc, argv, &s) != 0) {
                fprintf(stderr, "--election-tick must be > 0\n");
                return 1;
            }
            long v = strtol(s, &end, 10);
            if (errno == ERANGE || !end || *end || v < 1 || v > 0x7fffffffL) {
                fprintf(stderr, "--election-tick must be > 0\n");
                return 1;
            }
            cfg.election_tick = (uint64_t)v;
            cfg.election_tick_set = true;
        } else if (strcmp(argv[i], "--heartbeat-interval") == 0 && i + 1 < argc) {
            uint64_t ms = 0;
            if (cetcd_parse_heartbeat_interval_ms(argv[++i], &ms) != CETCD_OK) {
                fprintf(stderr, "--heartbeat-interval must be 1..50000 ms\n");
                return 1;
            }
            cfg.heartbeat_interval_set = true;
            cfg.tick_ms = ms;
        } else if (strncmp(argv[i], "--heartbeat-interval=", 21) == 0) {
            uint64_t ms = 0;
            if (cetcd_parse_heartbeat_interval_ms(argv[i] + 21, &ms) != CETCD_OK) {
                fprintf(stderr, "--heartbeat-interval must be 1..50000 ms\n");
                return 1;
            }
            cfg.heartbeat_interval_set = true;
            cfg.tick_ms = ms;
        } else if (strcmp(argv[i], "--election-timeout") == 0 && i + 1 < argc) {
            uint64_t ms = 0;
            if (cetcd_parse_election_timeout_ms(argv[++i], &ms) != CETCD_OK) {
                fprintf(stderr, "--election-timeout must be 1..50000 ms\n");
                return 1;
            }
            cfg.election_timeout_set = true;
            cfg.election_ms = ms;
        } else if (strncmp(argv[i], "--election-timeout=", 19) == 0) {
            uint64_t ms = 0;
            if (cetcd_parse_election_timeout_ms(argv[i] + 19, &ms) != CETCD_OK) {
                fprintf(stderr, "--election-timeout must be 1..50000 ms\n");
                return 1;
            }
            cfg.election_timeout_set = true;
            cfg.election_ms = ms;
        } else if (strcmp(argv[i], "--raft-read-timeout") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr, "--raft-read-timeout requires a duration\n");
                return 1;
            }
            uint64_t ms = 0;
            if (cetcd_parse_raft_io_timeout_ms(argv[++i], &ms) != CETCD_OK) {
                fprintf(stderr, "--raft-read-timeout must be a duration\n");
                return 1;
            }
            cfg.raft_read_timeout_set = true;
            cfg.raft_read_timeout_ms = ms;
        } else if (strncmp(argv[i], "--raft-read-timeout=", 20) == 0) {
            uint64_t ms = 0;
            if (cetcd_parse_raft_io_timeout_ms(argv[i] + 20, &ms) != CETCD_OK) {
                fprintf(stderr, "--raft-read-timeout must be a duration\n");
                return 1;
            }
            cfg.raft_read_timeout_set = true;
            cfg.raft_read_timeout_ms = ms;
        } else if (strcmp(argv[i], "--raft-write-timeout") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr, "--raft-write-timeout requires a duration\n");
                return 1;
            }
            uint64_t ms = 0;
            if (cetcd_parse_raft_io_timeout_ms(argv[++i], &ms) != CETCD_OK) {
                fprintf(stderr, "--raft-write-timeout must be a duration\n");
                return 1;
            }
            cfg.raft_write_timeout_set = true;
            cfg.raft_write_timeout_ms = ms;
        } else if (strncmp(argv[i], "--raft-write-timeout=", 21) == 0) {
            uint64_t ms = 0;
            if (cetcd_parse_raft_io_timeout_ms(argv[i] + 21, &ms) != CETCD_OK) {
                fprintf(stderr, "--raft-write-timeout must be a duration\n");
                return 1;
            }
            cfg.raft_write_timeout_set = true;
            cfg.raft_write_timeout_ms = ms;
        } else if (strcmp(argv[i], "--initial-election-tick-advance") == 0) {
            cfg.tick_advance_set = true;
            cfg.tick_advance = true;
            if (i + 1 < argc && argv[i + 1][0] != '-') {
                int b = 0;
                if (cetcd_parse_bool_flag(argv[++i], &b) != CETCD_OK) {
                    fprintf(stderr, "--initial-election-tick-advance must be true or false\n");
                    return 1;
                }
                cfg.tick_advance = b != 0;
            }
        } else if (strncmp(argv[i], "--initial-election-tick-advance=", 32) == 0) {
            int b = 0;
            if (cetcd_parse_bool_flag(argv[i] + 32, &b) != CETCD_OK) {
                fprintf(stderr, "--initial-election-tick-advance must be true or false\n");
                return 1;
            }
            cfg.tick_advance_set = true;
            cfg.tick_advance = b != 0;
        } else if (strcmp(argv[i], "--pre-vote") == 0) {
            cfg.pre_vote_set = true;
            cfg.pre_vote = true;
            if (i + 1 < argc && argv[i + 1][0] != '-') {
                int b = 0;
                if (cetcd_parse_bool_flag(argv[++i], &b) != CETCD_OK) {
                    fprintf(stderr, "--pre-vote must be true or false\n");
                    return 1;
                }
                cfg.pre_vote = b != 0;
            }
        } else if (strncmp(argv[i], "--pre-vote=", 11) == 0) {
            int b = 0;
            if (cetcd_parse_bool_flag(argv[i] + 11, &b) != CETCD_OK) {
                fprintf(stderr, "--pre-vote must be true or false\n");
                return 1;
            }
            cfg.pre_vote_set = true;
            cfg.pre_vote = b != 0;
        } else if (cetcd_cli_flag_is(argv[i], "--heartbeat-tick")) {
            const char *s = NULL;
            char *end = NULL;
            errno = 0;
            if (take_flag_value_(&i, argc, argv, &s) != 0) {
                fprintf(stderr, "--heartbeat-tick must be > 0\n");
                return 1;
            }
            long v = strtol(s, &end, 10);
            if (errno == ERANGE || !end || *end || v < 1 || v > 0x7fffffffL) {
                fprintf(stderr, "--heartbeat-tick must be > 0\n");
                return 1;
            }
            cfg.heartbeat_tick = (uint64_t)v;
            cfg.heartbeat_tick_set = true;
        } else if (strcmp(argv[i], "--advertise-client-urls") == 0) {
            if (i + 1 >= argc || argv[i + 1][0] == '-' || !argv[i + 1][0]) {
                fprintf(stderr, "--advertise-client-urls requires a URL list\n");
                return 1;
            }
            if (apply_advertise_client_urls_(argv[++i], &cfg) != 0) return 1;
        } else if (strncmp(argv[i], "--advertise-client-urls=", 24) == 0) {
            if (!argv[i][24]) {
                fprintf(stderr, "--advertise-client-urls requires a URL list\n");
                return 1;
            }
            if (apply_advertise_client_urls_(argv[i] + 24, &cfg) != 0) return 1;
        } else if (strcmp(argv[i], "--initial-advertise-peer-urls") == 0) {
            if (i + 1 >= argc || argv[i + 1][0] == '-' || !argv[i + 1][0]) {
                fprintf(stderr, "--initial-advertise-peer-urls requires a URL list\n");
                return 1;
            }
            if (apply_advertise_peer_urls_(argv[++i], &cfg) != 0) return 1;
        } else if (strncmp(argv[i], "--initial-advertise-peer-urls=", 30) == 0) {
            if (!argv[i][30]) {
                fprintf(stderr, "--initial-advertise-peer-urls requires a URL list\n");
                return 1;
            }
            if (apply_advertise_peer_urls_(argv[i] + 30, &cfg) != 0) return 1;
        } else if (cetcd_cli_flag_is(argv[i], "--initial-cluster-state")) {
            const char *s = NULL;
            if (take_flag_value_(&i, argc, argv, &s) != 0) {
                fprintf(stderr, "--initial-cluster-state requires a value\n");
                return 1;
            }
            strncpy(cfg.initial_cluster_state, s,
                    sizeof(cfg.initial_cluster_state) - 1);
        } else if (cetcd_cli_flag_is(argv[i], "--initial-cluster-token")) {
            const char *s = NULL;
            if (take_flag_value_(&i, argc, argv, &s) != 0) {
                fprintf(stderr, "--initial-cluster-token requires a value\n");
                return 1;
            }
            strncpy(cfg.initial_cluster_token, s,
                    sizeof(cfg.initial_cluster_token) - 1);
        } else if (cetcd_cli_flag_is(argv[i], "--snapshot-count")) {
            const char *s = NULL;
            char *end = NULL;
            errno = 0;
            if (take_flag_value_(&i, argc, argv, &s) != 0) {
                fprintf(stderr, "--snapshot-count must be > 0\n");
                return 1;
            }
            unsigned long long v = strtoull(s, &end, 10);
            if (errno == ERANGE || !end || *end || v == 0) {
                fprintf(stderr, "--snapshot-count must be > 0\n");
                return 1;
            }
            cfg.snapshot_count = (uint64_t)v;
        } else if (strcmp(argv[i], "--quota-backend-bytes") == 0 ||
                   strncmp(argv[i], "--quota-backend-bytes=", 22) == 0) {
            const char *s = NULL;
            if (take_flag_value_(&i, argc, argv, &s) != 0) {
                fprintf(stderr, "--quota-backend-bytes must be an integer (0 = 2GiB)\n");
                return 1;
            }
            char *end = NULL;
            errno = 0;
            unsigned long long v = strtoull(s, &end, 10);
            if (errno == ERANGE || !end || end == s || *end) {
                fprintf(stderr, "--quota-backend-bytes must be an integer (0 = 2GiB)\n");
                return 1;
            }
            cfg.quota_backend_bytes = (uint64_t)v;
        } else if (cetcd_cli_flag_is(argv[i], "--force-new-cluster")) {
            int on = 1;
            if (take_bool_flag_(&i, argc, argv, &on) != 0) {
                fprintf(stderr, "--force-new-cluster must be true or false\n");
                return 1;
            }
            cfg.force_new_cluster = on ? true : false;
        } else if (strcmp(argv[i], "--strict-reconfig-check") == 0) {
            cfg.strict_reconfig_set = true;
            cfg.strict_reconfig = true;
            if (i + 1 < argc && argv[i + 1][0] != '-') {
                int b = 0;
                if (cetcd_parse_bool_flag(argv[++i], &b) != CETCD_OK) {
                    fprintf(stderr, "--strict-reconfig-check must be true or false\n");
                    return 1;
                }
                cfg.strict_reconfig = b != 0;
            }
        } else if (strncmp(argv[i], "--strict-reconfig-check=", 24) == 0) {
            int b = 0;
            if (cetcd_parse_bool_flag(argv[i] + 24, &b) != CETCD_OK) {
                fprintf(stderr, "--strict-reconfig-check must be true or false\n");
                return 1;
            }
            cfg.strict_reconfig_set = true;
            cfg.strict_reconfig = b != 0;
        } else if (cetcd_cli_flag_is(argv[i], "--max-txn-ops")) {
            const char *s = NULL;
            char *end = NULL;
            errno = 0;
            if (take_flag_value_(&i, argc, argv, &s) != 0) {
                fprintf(stderr, "--max-txn-ops must be 1..128\n");
                return 1;
            }
            unsigned long long v = strtoull(s, &end, 10);
            if (errno == ERANGE || !end || end == s || *end || v < 1 || v > 128) {
                fprintf(stderr, "--max-txn-ops must be 1..128\n");
                return 1;
            }
            cfg.max_txn_ops = (uint64_t)v;
        } else if (cetcd_cli_flag_is(argv[i], "--max-request-bytes")) {
            const char *s = NULL;
            char *end = NULL;
            errno = 0;
            if (take_flag_value_(&i, argc, argv, &s) != 0) {
                fprintf(stderr, "--max-request-bytes must be > 0\n");
                return 1;
            }
            unsigned long long v = strtoull(s, &end, 10);
            if (errno == ERANGE || !end || end == s || *end || v == 0) {
                fprintf(stderr, "--max-request-bytes must be > 0\n");
                return 1;
            }
            cfg.max_request_bytes = (uint64_t)v;
        } else if (strcmp(argv[i], "--max-concurrent-streams") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr, "--max-concurrent-streams requires an integer\n");
                return 1;
            }
            uint32_t n = 0;
            if (cetcd_parse_max_concurrent_streams(argv[++i], &n) != CETCD_OK) {
                fprintf(stderr, "--max-concurrent-streams must be > 0\n");
                return 1;
            }
            cfg.max_concurrent_streams_set = true;
            cfg.max_concurrent_streams = n;
        } else if (strncmp(argv[i], "--max-concurrent-streams=", 25) == 0) {
            uint32_t n = 0;
            if (cetcd_parse_max_concurrent_streams(argv[i] + 25, &n) != CETCD_OK) {
                fprintf(stderr, "--max-concurrent-streams must be > 0\n");
                return 1;
            }
            cfg.max_concurrent_streams_set = true;
            cfg.max_concurrent_streams = n;
        } else if (cetcd_cli_flag_is(argv[i], "--auth-token")) {
            const char *s = NULL;
            if (take_flag_value_(&i, argc, argv, &s) != 0) {
                fprintf(stderr, "--auth-token requires a value\n");
                return 1;
            }
            strncpy(cfg.auth_token, s, sizeof(cfg.auth_token) - 1);
        } else if (strcmp(argv[i], "--auth-token-ttl") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr, "--auth-token-ttl requires an integer\n");
                return 1;
            }
            uint64_t sec = 0;
            if (cetcd_parse_auth_token_ttl(argv[++i], &sec) != CETCD_OK) {
                fprintf(stderr, "--auth-token-ttl must be an integer > 0\n");
                return 1;
            }
            cfg.auth_token_ttl_sec = sec;
        } else if (strncmp(argv[i], "--auth-token-ttl=", 17) == 0) {
            uint64_t sec = 0;
            if (cetcd_parse_auth_token_ttl(argv[i] + 17, &sec) != CETCD_OK) {
                fprintf(stderr, "--auth-token-ttl must be an integer > 0\n");
                return 1;
            }
            cfg.auth_token_ttl_sec = sec;
        } else if (cetcd_cli_flag_is(argv[i], "--bcrypt-cost")) {
            const char *s = NULL;
            char *end = NULL;
            errno = 0;
            if (take_flag_value_(&i, argc, argv, &s) != 0) {
                fprintf(stderr, "--bcrypt-cost must be 0 or 4..31\n");
                return 1;
            }
            long v = strtol(s, &end, 10);
            if (errno == ERANGE || !end || end == s || *end ||
                (v != 0 && (v < 4 || v > 31))) {
                fprintf(stderr, "--bcrypt-cost must be 0 or 4..31\n");
                return 1;
            }
            cfg.bcrypt_cost = (int)v;
        } else if (cetcd_cli_flag_is(argv[i], "--cert-file")) {
            const char *s = NULL;
            if (take_flag_value_(&i, argc, argv, &s) != 0) {
                fprintf(stderr, "--cert-file requires a file\n");
                return 1;
            }
            strncpy(cfg.cert_file, s, sizeof(cfg.cert_file) - 1);
        } else if (cetcd_cli_flag_is(argv[i], "--key-file")) {
            const char *s = NULL;
            if (take_flag_value_(&i, argc, argv, &s) != 0) {
                fprintf(stderr, "--key-file requires a file\n");
                return 1;
            }
            strncpy(cfg.key_file, s, sizeof(cfg.key_file) - 1);
        } else if (strcmp(argv[i], "--trusted-ca-file") == 0 ||
                   strncmp(argv[i], "--trusted-ca-file=", 18) == 0) {
            const char *v = NULL;
            if (take_flag_value_(&i, argc, argv, &v) != 0 || !v[0]) {
                fprintf(stderr, "--trusted-ca-file requires a file\n");
                return 1;
            }
            strncpy(cfg.trusted_ca_file, v, sizeof(cfg.trusted_ca_file) - 1);
        } else if (strcmp(argv[i], "--client-crl-file") == 0) {
            if (i + 1 >= argc || argv[i + 1][0] == '-' || !argv[i + 1][0]) {
                fprintf(stderr, "--client-crl-file requires a file\n");
                return 1;
            }
            strncpy(cfg.client_crl_file, argv[++i], sizeof(cfg.client_crl_file) - 1);
        } else if (strncmp(argv[i], "--client-crl-file=", 18) == 0) {
            if (!argv[i][18]) {
                fprintf(stderr, "--client-crl-file requires a file\n");
                return 1;
            }
            strncpy(cfg.client_crl_file, argv[i] + 18, sizeof(cfg.client_crl_file) - 1);
        } else if (strcmp(argv[i], "--client-cert-auth") == 0 ||
                   strncmp(argv[i], "--client-cert-auth=", 19) == 0) {
            int on = 1;
            if (take_bool_flag_(&i, argc, argv, &on) != 0) {
                fprintf(stderr, "--client-cert-auth must be true or false\n");
                return 1;
            }
            cfg.client_cert_auth = on ? true : false;
        } else if (cetcd_cli_flag_is(argv[i], "--auto-tls")) {
            int on = 1;
            if (take_bool_flag_(&i, argc, argv, &on) != 0) {
                fprintf(stderr, "--auto-tls must be true or false\n");
                return 1;
            }
            cfg.auto_tls = on ? true : false;
        } else if (strcmp(argv[i], "--self-signed-cert-validity") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr, "--self-signed-cert-validity requires an integer\n");
                return 1;
            }
            uint32_t years = 0;
            if (cetcd_parse_self_signed_cert_validity(argv[++i], &years) != CETCD_OK) {
                fprintf(stderr, "--self-signed-cert-validity must be an integer > 0\n");
                return 1;
            }
            cfg.self_signed_cert_validity = years;
        } else if (strncmp(argv[i], "--self-signed-cert-validity=", 28) == 0) {
            uint32_t years = 0;
            if (cetcd_parse_self_signed_cert_validity(argv[i] + 28, &years) != CETCD_OK) {
                fprintf(stderr, "--self-signed-cert-validity must be an integer > 0\n");
                return 1;
            }
            cfg.self_signed_cert_validity = years;
        } else if (cetcd_cli_flag_is(argv[i], "--peer-cert-file")) {
            const char *s = NULL;
            if (take_flag_value_(&i, argc, argv, &s) != 0) {
                fprintf(stderr, "--peer-cert-file requires a file\n");
                return 1;
            }
            strncpy(cfg.peer_cert_file, s, sizeof(cfg.peer_cert_file) - 1);
        } else if (cetcd_cli_flag_is(argv[i], "--peer-key-file")) {
            const char *s = NULL;
            if (take_flag_value_(&i, argc, argv, &s) != 0) {
                fprintf(stderr, "--peer-key-file requires a file\n");
                return 1;
            }
            strncpy(cfg.peer_key_file, s, sizeof(cfg.peer_key_file) - 1);
        } else if (strcmp(argv[i], "--peer-client-cert-file") == 0) {
            if (i + 1 >= argc || argv[i + 1][0] == '-' || !argv[i + 1][0]) {
                fprintf(stderr, "--peer-client-cert-file requires a file\n");
                return 1;
            }
            strncpy(cfg.peer_client_cert_file, argv[++i],
                    sizeof(cfg.peer_client_cert_file) - 1);
        } else if (strncmp(argv[i], "--peer-client-cert-file=", 24) == 0) {
            if (!argv[i][24]) {
                fprintf(stderr, "--peer-client-cert-file requires a file\n");
                return 1;
            }
            strncpy(cfg.peer_client_cert_file, argv[i] + 24,
                    sizeof(cfg.peer_client_cert_file) - 1);
        } else if (strcmp(argv[i], "--peer-client-key-file") == 0) {
            if (i + 1 >= argc || argv[i + 1][0] == '-' || !argv[i + 1][0]) {
                fprintf(stderr, "--peer-client-key-file requires a file\n");
                return 1;
            }
            strncpy(cfg.peer_client_key_file, argv[++i],
                    sizeof(cfg.peer_client_key_file) - 1);
        } else if (strncmp(argv[i], "--peer-client-key-file=", 23) == 0) {
            if (!argv[i][23]) {
                fprintf(stderr, "--peer-client-key-file requires a file\n");
                return 1;
            }
            strncpy(cfg.peer_client_key_file, argv[i] + 23,
                    sizeof(cfg.peer_client_key_file) - 1);
        } else if (strcmp(argv[i], "--peer-trusted-ca-file") == 0 ||
                   strncmp(argv[i], "--peer-trusted-ca-file=", 23) == 0) {
            const char *v = NULL;
            if (take_flag_value_(&i, argc, argv, &v) != 0 || !v[0]) {
                fprintf(stderr, "--peer-trusted-ca-file requires a file\n");
                return 1;
            }
            strncpy(cfg.peer_trusted_ca_file, v, sizeof(cfg.peer_trusted_ca_file) - 1);
        } else if (strcmp(argv[i], "--peer-crl-file") == 0) {
            if (i + 1 >= argc || argv[i + 1][0] == '-' || !argv[i + 1][0]) {
                fprintf(stderr, "--peer-crl-file requires a file\n");
                return 1;
            }
            strncpy(cfg.peer_crl_file, argv[++i], sizeof(cfg.peer_crl_file) - 1);
        } else if (strncmp(argv[i], "--peer-crl-file=", 16) == 0) {
            if (!argv[i][16]) {
                fprintf(stderr, "--peer-crl-file requires a file\n");
                return 1;
            }
            strncpy(cfg.peer_crl_file, argv[i] + 16, sizeof(cfg.peer_crl_file) - 1);
        } else if (strcmp(argv[i], "--peer-client-cert-auth") == 0 ||
                   strncmp(argv[i], "--peer-client-cert-auth=", 24) == 0) {
            int on = 1;
            if (take_bool_flag_(&i, argc, argv, &on) != 0) {
                fprintf(stderr, "--peer-client-cert-auth must be true or false\n");
                return 1;
            }
            cfg.peer_client_cert_auth = on ? true : false;
        } else if (strcmp(argv[i], "--peer-cert-allowed-cn") == 0) {
            if (i + 1 >= argc || argv[i + 1][0] == '-') {
                fprintf(stderr, "--peer-cert-allowed-cn requires a list\n");
                return 1;
            }
            strncpy(cfg.peer_cert_allowed_cn, argv[++i],
                    sizeof(cfg.peer_cert_allowed_cn) - 1);
        } else if (strncmp(argv[i], "--peer-cert-allowed-cn=", 23) == 0) {
            strncpy(cfg.peer_cert_allowed_cn, argv[i] + 23,
                    sizeof(cfg.peer_cert_allowed_cn) - 1);
        } else if (strcmp(argv[i], "--peer-cert-allowed-hostname") == 0) {
            if (i + 1 >= argc || argv[i + 1][0] == '-') {
                fprintf(stderr, "--peer-cert-allowed-hostname requires a list\n");
                return 1;
            }
            strncpy(cfg.peer_cert_allowed_hostname, argv[++i],
                    sizeof(cfg.peer_cert_allowed_hostname) - 1);
        } else if (strncmp(argv[i], "--peer-cert-allowed-hostname=", 29) == 0) {
            strncpy(cfg.peer_cert_allowed_hostname, argv[i] + 29,
                    sizeof(cfg.peer_cert_allowed_hostname) - 1);
        } else if (strcmp(argv[i], "--client-cert-allowed-hostname") == 0) {
            if (i + 1 >= argc || argv[i + 1][0] == '-') {
                fprintf(stderr, "--client-cert-allowed-hostname requires a list\n");
                return 1;
            }
            strncpy(cfg.client_cert_allowed_hostname, argv[++i],
                    sizeof(cfg.client_cert_allowed_hostname) - 1);
        } else if (strncmp(argv[i], "--client-cert-allowed-hostname=", 31) == 0) {
            strncpy(cfg.client_cert_allowed_hostname, argv[i] + 31,
                    sizeof(cfg.client_cert_allowed_hostname) - 1);
        } else if (cetcd_cli_flag_is(argv[i], "--peer-auto-tls")) {
            int on = 1;
            if (take_bool_flag_(&i, argc, argv, &on) != 0) {
                fprintf(stderr, "--peer-auto-tls must be true or false\n");
                return 1;
            }
            cfg.peer_auto_tls = on ? true : false;
        } else if (cetcd_cli_flag_is(argv[i], "--cipher-suites")) {
            const char *s = NULL;
            if (take_flag_value_(&i, argc, argv, &s) != 0) {
                fprintf(stderr, "--cipher-suites requires a list\n");
                return 1;
            }
            strncpy(cfg.cipher_suites, s, sizeof(cfg.cipher_suites) - 1);
        } else if (strcmp(argv[i], "--tls-min-version") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr, "--tls-min-version requires TLS1.2 or TLS1.3\n");
                return 1;
            }
            int v = 0;
            if (cetcd_parse_tls_version(argv[++i], &v) != CETCD_OK) {
                fprintf(stderr, "--tls-min-version must be TLS1.2 or TLS1.3\n");
                return 1;
            }
            cfg.tls_min_version_set = true;
            cfg.tls_min_version = v;
        } else if (strncmp(argv[i], "--tls-min-version=", 18) == 0) {
            int v = 0;
            if (cetcd_parse_tls_version(argv[i] + 18, &v) != CETCD_OK) {
                fprintf(stderr, "--tls-min-version must be TLS1.2 or TLS1.3\n");
                return 1;
            }
            cfg.tls_min_version_set = true;
            cfg.tls_min_version = v;
        } else if (strcmp(argv[i], "--tls-max-version") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr, "--tls-max-version requires TLS1.2 or TLS1.3\n");
                return 1;
            }
            int v = 0;
            if (cetcd_parse_tls_version(argv[++i], &v) != CETCD_OK) {
                fprintf(stderr, "--tls-max-version must be TLS1.2 or TLS1.3\n");
                return 1;
            }
            cfg.tls_max_version_set = true;
            cfg.tls_max_version = v;
        } else if (strncmp(argv[i], "--tls-max-version=", 18) == 0) {
            int v = 0;
            if (cetcd_parse_tls_version(argv[i] + 18, &v) != CETCD_OK) {
                fprintf(stderr, "--tls-max-version must be TLS1.2 or TLS1.3\n");
                return 1;
            }
            cfg.tls_max_version_set = true;
            cfg.tls_max_version = v;
        } else if (strcmp(argv[i], "--logger") == 0 ||
                   strncmp(argv[i], "--logger=", 9) == 0) {
            const char *lg = NULL;
            if (take_flag_value_(&i, argc, argv, &lg) != 0 ||
                cetcd_parse_logger(lg) != CETCD_OK) {
                fprintf(stderr, "--logger %s is not supported (zap or capnslog)\n",
                        lg ? lg : "");
                return 1;
            }
        } else if (strcmp(argv[i], "--log-outputs") == 0 ||
                   strncmp(argv[i], "--log-outputs=", 14) == 0) {
            const char *out = NULL;
            if (take_flag_value_(&i, argc, argv, &out) != 0 || !out[0]) {
                fprintf(stderr, "--log-outputs requires a target\n");
                return 1;
            }
            if (log_owned) {
                fclose(log_owned);
                log_owned = NULL;
            }
            if (cetcd_log_open_outputs(out, &log_owned) != 0) {
                fprintf(stderr, "--log-outputs %s is not supported or cannot be opened\n",
                        out);
                return 1;
            }
            log_out_spec = out;
        } else if (strcmp(argv[i], "--enable-log-rotation") == 0) {
            enable_log_rotation_set = 1;
            enable_log_rotation = 1;
            if (i + 1 < argc && argv[i + 1][0] != '-') {
                int b = 0;
                if (cetcd_parse_bool_flag(argv[++i], &b) != CETCD_OK) {
                    fprintf(stderr, "--enable-log-rotation must be true or false\n");
                    return 1;
                }
                enable_log_rotation = b != 0;
            }
        } else if (strncmp(argv[i], "--enable-log-rotation=", 22) == 0) {
            int b = 0;
            if (cetcd_parse_bool_flag(argv[i] + 22, &b) != CETCD_OK) {
                fprintf(stderr, "--enable-log-rotation must be true or false\n");
                return 1;
            }
            enable_log_rotation_set = 1;
            enable_log_rotation = b != 0;
        } else if (strcmp(argv[i], "--log-rotation-config-json") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr, "--log-rotation-config-json requires a JSON object\n");
                return 1;
            }
            if (cetcd_parse_log_rotation_json(argv[++i], &log_rot_cfg) != CETCD_OK) {
                fprintf(stderr, "--log-rotation-config-json is invalid or compress is unsupported\n");
                return 1;
            }
        } else if (strncmp(argv[i], "--log-rotation-config-json=", 27) == 0) {
            if (cetcd_parse_log_rotation_json(argv[i] + 27, &log_rot_cfg) != CETCD_OK) {
                fprintf(stderr, "--log-rotation-config-json is invalid or compress is unsupported\n");
                return 1;
            }
        } else if (cetcd_cli_flag_is(argv[i], "--discovery-srv")) {
            const char *dom = NULL;
            if (take_flag_value_(&i, argc, argv, &dom) != 0 ||
                cetcd_discovery_valid_domain(dom) != 0) {
                fprintf(stderr, "--discovery-srv domain is invalid\n");
                return 1;
            }
            strncpy(cfg.discovery_srv, dom, sizeof(cfg.discovery_srv) - 1);
        } else if (cetcd_cli_flag_is(argv[i], "--discovery-srv-name")) {
            const char *nm = NULL;
            if (take_flag_value_(&i, argc, argv, &nm) != 0 ||
                cetcd_discovery_valid_name(nm) != 0 || !nm[0]) {
                fprintf(stderr, "--discovery-srv-name is invalid\n");
                return 1;
            }
            strncpy(cfg.discovery_srv_name, nm, sizeof(cfg.discovery_srv_name) - 1);
        } else if (cetcd_grpc_keepalive_kind(argv[i]) == CETCD_KA_IDLE) {
            const char *flag = argv[i];
            const char *v = NULL;
            int sec = 0;
            if (take_flag_value_(&i, argc, argv, &v) != 0 ||
                cetcd_parse_grpc_keepalive_sec(v, 0, &sec) != CETCD_OK) {
                fprintf(stderr, "%s must be 0..86400 seconds\n", flag);
                return 1;
            }
            cfg.keepalive_time = sec;
            cfg.keepalive_set = true;
        } else if (cetcd_grpc_keepalive_kind(argv[i]) == CETCD_KA_TIMEOUT) {
            const char *flag = argv[i];
            const char *v = NULL;
            int sec = 0;
            if (take_flag_value_(&i, argc, argv, &v) != 0 ||
                cetcd_parse_grpc_keepalive_sec(v, 0, &sec) != CETCD_OK) {
                fprintf(stderr, "%s must be 0..86400 seconds\n", flag);
                return 1;
            }
            cfg.keepalive_timeout = sec;
        } else if (cetcd_grpc_keepalive_kind(argv[i]) == CETCD_KA_MIN_TIME) {
            const char *flag = argv[i];
            const char *v = NULL;
            int dummy = 0;
            if (take_flag_value_(&i, argc, argv, &v) != 0 ||
                cetcd_parse_grpc_keepalive_sec(v, 0, &dummy) != CETCD_OK) {
                fprintf(stderr, "%s must be 0..86400 seconds\n", flag);
                return 1;
            }
        } else if (cetcd_grpc_keepalive_kind(argv[i]) == CETCD_KA_PERMIT) {
            const char *flag = argv[i];
            const char *eq = strchr(argv[i], '=');
            int on = 1;
            if (eq) {
                if (cetcd_parse_bool_flag(eq + 1, &on) != CETCD_OK) {
                    fprintf(stderr, "%s must be true or false\n", flag);
                    return 1;
                }
            } else if (i + 1 < argc && argv[i + 1][0] != '-') {
                if (cetcd_parse_bool_flag(argv[++i], &on) != CETCD_OK) {
                    fprintf(stderr, "%s must be true or false\n", flag);
                    return 1;
                }
            }
            (void)on;
        } else if (cetcd_grpc_keepalive_kind(argv[i]) == CETCD_KA_UNKNOWN) {
            fprintf(stderr, "unknown flag: %s\n", argv[i]);
            return 1;
        } else if (cetcd_cli_flag_is(argv[i], "--auto-compaction-mode")) {
            if (take_flag_value_(&i, argc, argv, &ac_mode_s) != 0) {
                fprintf(stderr, "--auto-compaction-mode requires periodic or revision\n");
                return 1;
            }
            {
                cetcd_auto_compact_mode tmp = CETCD_AUTO_COMPACT_PERIODIC;
                if (cetcd_parse_auto_compaction_mode(ac_mode_s, &tmp) != CETCD_OK) {
                    fprintf(stderr,
                            "--auto-compaction-mode %s is invalid (periodic or revision)\n",
                            ac_mode_s);
                    return 1;
                }
            }
        } else if (cetcd_cli_flag_is(argv[i], "--auto-compaction-retention")) {
            if (take_flag_value_(&i, argc, argv, &ac_ret_s) != 0) {
                fprintf(stderr, "--auto-compaction-retention requires a value\n");
                return 1;
            }
        } else if (strcmp(argv[i], "--experimental-initial-corrupt-check") == 0) {
            cfg.initial_corrupt_check = true;
            if (i + 1 < argc && argv[i + 1][0] != '-') {
                int b = 0;
                if (cetcd_parse_bool_flag(argv[++i], &b) != CETCD_OK) {
                    fprintf(stderr,
                            "--experimental-initial-corrupt-check must be true or false\n");
                    return 1;
                }
                cfg.initial_corrupt_check = b != 0;
            }
        } else if (strncmp(argv[i], "--experimental-initial-corrupt-check=", 37) == 0) {
            int b = 0;
            if (cetcd_parse_bool_flag(argv[i] + 37, &b) != CETCD_OK) {
                fprintf(stderr,
                        "--experimental-initial-corrupt-check must be true or false\n");
                return 1;
            }
            cfg.initial_corrupt_check = b != 0;
        } else if (strcmp(argv[i], "--experimental-corrupt-check-time") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr, "--experimental-corrupt-check-time requires a duration\n");
                return 1;
            }
            uint64_t sec = 0;
            if (cetcd_parse_go_duration_sec(argv[++i], &sec) != CETCD_OK) {
                fprintf(stderr,
                        "--experimental-corrupt-check-time must be a duration (0 disables)\n");
                return 1;
            }
            cfg.corrupt_check_interval_sec = sec;
        } else if (strncmp(argv[i], "--experimental-corrupt-check-time=", 34) == 0) {
            uint64_t sec = 0;
            if (cetcd_parse_go_duration_sec(argv[i] + 34, &sec) != CETCD_OK) {
                fprintf(stderr,
                        "--experimental-corrupt-check-time must be a duration (0 disables)\n");
                return 1;
            }
            cfg.corrupt_check_interval_sec = sec;
        } else if (strcmp(argv[i], "--experimental-compaction-batch-limit") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr,
                        "--experimental-compaction-batch-limit requires an integer\n");
                return 1;
            }
            uint64_t n = 0;
            if (cetcd_parse_compaction_batch_limit(argv[++i], &n) != CETCD_OK) {
                fprintf(stderr,
                        "--experimental-compaction-batch-limit must be an integer (0 = unlimited)\n");
                return 1;
            }
            cfg.compaction_batch_limit = n;
        } else if (strncmp(argv[i], "--experimental-compaction-batch-limit=", 38) == 0) {
            uint64_t n = 0;
            if (cetcd_parse_compaction_batch_limit(argv[i] + 38, &n) != CETCD_OK) {
                fprintf(stderr,
                        "--experimental-compaction-batch-limit must be an integer (0 = unlimited)\n");
                return 1;
            }
            cfg.compaction_batch_limit = n;
        } else if (strcmp(argv[i], "--experimental-compaction-sleep-interval") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr,
                        "--experimental-compaction-sleep-interval requires a duration\n");
                return 1;
            }
            uint64_t ms = 0;
            if (cetcd_parse_go_duration_ms(argv[++i], &ms) != CETCD_OK) {
                fprintf(stderr,
                        "--experimental-compaction-sleep-interval must be a duration (0 = none)\n");
                return 1;
            }
            cfg.compaction_sleep_interval_ms = ms;
        } else if (strncmp(argv[i], "--experimental-compaction-sleep-interval=", 41) == 0) {
            uint64_t ms = 0;
            if (cetcd_parse_go_duration_ms(argv[i] + 41, &ms) != CETCD_OK) {
                fprintf(stderr,
                        "--experimental-compaction-sleep-interval must be a duration (0 = none)\n");
                return 1;
            }
            cfg.compaction_sleep_interval_ms = ms;
        } else if (strcmp(argv[i], "--experimental-watch-progress-notify-interval") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr,
                        "--experimental-watch-progress-notify-interval requires a duration\n");
                return 1;
            }
            uint64_t ms = 0;
            if (cetcd_parse_go_duration_ms(argv[++i], &ms) != CETCD_OK) {
                fprintf(stderr,
                        "--experimental-watch-progress-notify-interval must be a duration (0 = 10s)\n");
                return 1;
            }
            cfg.watch_progress_interval_ms = ms;
        } else if (strncmp(argv[i], "--experimental-watch-progress-notify-interval=", 46) == 0) {
            uint64_t ms = 0;
            if (cetcd_parse_go_duration_ms(argv[i] + 46, &ms) != CETCD_OK) {
                fprintf(stderr,
                        "--experimental-watch-progress-notify-interval must be a duration (0 = 10s)\n");
                return 1;
            }
            cfg.watch_progress_interval_ms = ms;
        } else if (strcmp(argv[i], "--experimental-warning-apply-duration") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr,
                        "--experimental-warning-apply-duration requires a duration\n");
                return 1;
            }
            uint64_t ms = 0;
            if (cetcd_parse_go_duration_ms(argv[++i], &ms) != CETCD_OK) {
                fprintf(stderr,
                        "--experimental-warning-apply-duration must be a duration (0 disables)\n");
                return 1;
            }
            cfg.warning_apply_set = true;
            cfg.warning_apply_ms = ms;
        } else if (strncmp(argv[i], "--experimental-warning-apply-duration=", 38) == 0) {
            uint64_t ms = 0;
            if (cetcd_parse_go_duration_ms(argv[i] + 38, &ms) != CETCD_OK) {
                fprintf(stderr,
                        "--experimental-warning-apply-duration must be a duration (0 disables)\n");
                return 1;
            }
            cfg.warning_apply_set = true;
            cfg.warning_apply_ms = ms;
        } else if (strcmp(argv[i], "--experimental-warning-unary-request-duration") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr,
                        "--experimental-warning-unary-request-duration requires a duration\n");
                return 1;
            }
            uint64_t ms = 0;
            if (cetcd_parse_go_duration_ms(argv[++i], &ms) != CETCD_OK) {
                fprintf(stderr,
                        "--experimental-warning-unary-request-duration must be a duration (0 disables)\n");
                return 1;
            }
            cfg.warning_unary_set = true;
            cfg.warning_unary_ms = ms;
        } else if (strncmp(argv[i], "--experimental-warning-unary-request-duration=", 46) == 0) {
            uint64_t ms = 0;
            if (cetcd_parse_go_duration_ms(argv[i] + 46, &ms) != CETCD_OK) {
                fprintf(stderr,
                        "--experimental-warning-unary-request-duration must be a duration (0 disables)\n");
                return 1;
            }
            cfg.warning_unary_set = true;
            cfg.warning_unary_ms = ms;
        } else if (strcmp(argv[i], "--experimental-max-learners") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr,
                        "--experimental-max-learners requires an integer\n");
                return 1;
            }
            uint32_t n = 0;
            if (cetcd_parse_max_learners(argv[++i], &n) != CETCD_OK) {
                fprintf(stderr,
                        "--experimental-max-learners must be an integer (0 = none)\n");
                return 1;
            }
            cfg.max_learners_set = true;
            cfg.max_learners = n;
        } else if (strncmp(argv[i], "--experimental-max-learners=", 28) == 0) {
            uint32_t n = 0;
            if (cetcd_parse_max_learners(argv[i] + 28, &n) != CETCD_OK) {
                fprintf(stderr,
                        "--experimental-max-learners must be an integer (0 = none)\n");
                return 1;
            }
            cfg.max_learners_set = true;
            cfg.max_learners = n;
        } else if (strcmp(argv[i], "--experimental-memory-mlock") == 0) {
            cfg.memory_mlock = true;
            if (i + 1 < argc && argv[i + 1][0] != '-') {
                int b = 0;
                if (cetcd_parse_bool_flag(argv[++i], &b) != CETCD_OK) {
                    fprintf(stderr,
                            "--experimental-memory-mlock must be true or false\n");
                    return 1;
                }
                cfg.memory_mlock = b != 0;
            }
        } else if (strncmp(argv[i], "--experimental-memory-mlock=", 28) == 0) {
            int b = 0;
            if (cetcd_parse_bool_flag(argv[i] + 28, &b) != CETCD_OK) {
                fprintf(stderr,
                        "--experimental-memory-mlock must be true or false\n");
                return 1;
            }
            cfg.memory_mlock = b != 0;
        } else if (strcmp(argv[i], "--experimental-bootstrap-defrag-threshold-megabytes") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr,
                        "--experimental-bootstrap-defrag-threshold-megabytes requires an integer\n");
                return 1;
            }
            uint64_t n = 0;
            if (cetcd_parse_bootstrap_defrag_mb(argv[++i], &n) != CETCD_OK) {
                fprintf(stderr,
                        "--experimental-bootstrap-defrag-threshold-megabytes must be an integer (0 = off)\n");
                return 1;
            }
            cfg.bootstrap_defrag_mb = n;
        } else if (strncmp(argv[i], "--experimental-bootstrap-defrag-threshold-megabytes=", 52) == 0) {
            uint64_t n = 0;
            if (cetcd_parse_bootstrap_defrag_mb(argv[i] + 52, &n) != CETCD_OK) {
                fprintf(stderr,
                        "--experimental-bootstrap-defrag-threshold-megabytes must be an integer (0 = off)\n");
                return 1;
            }
            cfg.bootstrap_defrag_mb = n;
        } else if (strcmp(argv[i], "--experimental-wait-cluster-ready") == 0) {
            cfg.wait_cluster_ready = true;
            if (i + 1 < argc && argv[i + 1][0] != '-') {
                int b = 0;
                if (cetcd_parse_bool_flag(argv[++i], &b) != CETCD_OK) {
                    fprintf(stderr,
                            "--experimental-wait-cluster-ready must be true or false\n");
                    return 1;
                }
                cfg.wait_cluster_ready = b != 0;
            }
        } else if (strncmp(argv[i], "--experimental-wait-cluster-ready=", 34) == 0) {
            int b = 0;
            if (cetcd_parse_bool_flag(argv[i] + 34, &b) != CETCD_OK) {
                fprintf(stderr,
                        "--experimental-wait-cluster-ready must be true or false\n");
                return 1;
            }
            cfg.wait_cluster_ready = b != 0;
        } else if (strcmp(argv[i], "--experimental-snapshot-catchup-entries") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr,
                        "--experimental-snapshot-catchup-entries requires an integer\n");
                return 1;
            }
            uint64_t n = 0;
            if (cetcd_parse_snapshot_catchup_entries(argv[++i], &n) != CETCD_OK) {
                fprintf(stderr,
                        "--experimental-snapshot-catchup-entries must be an integer\n");
                return 1;
            }
            cfg.snapshot_catchup_set = true;
            cfg.snapshot_catchup_entries = n;
        } else if (strncmp(argv[i], "--experimental-snapshot-catchup-entries=", 40) == 0) {
            uint64_t n = 0;
            if (cetcd_parse_snapshot_catchup_entries(argv[i] + 40, &n) != CETCD_OK) {
                fprintf(stderr,
                        "--experimental-snapshot-catchup-entries must be an integer\n");
                return 1;
            }
            cfg.snapshot_catchup_set = true;
            cfg.snapshot_catchup_entries = n;
        } else if (strcmp(argv[i], "--experimental-compact-hash-check-enabled") == 0) {
            cfg.compact_hash_check_set = true;
            cfg.compact_hash_check = true;
            if (i + 1 < argc && argv[i + 1][0] != '-') {
                int b = 0;
                if (cetcd_parse_bool_flag(argv[++i], &b) != CETCD_OK) {
                    fprintf(stderr,
                            "--experimental-compact-hash-check-enabled must be true or false\n");
                    return 1;
                }
                cfg.compact_hash_check = b != 0;
            }
        } else if (strncmp(argv[i], "--experimental-compact-hash-check-enabled=", 42) == 0) {
            int b = 0;
            if (cetcd_parse_bool_flag(argv[i] + 42, &b) != CETCD_OK) {
                fprintf(stderr,
                        "--experimental-compact-hash-check-enabled must be true or false\n");
                return 1;
            }
            cfg.compact_hash_check_set = true;
            cfg.compact_hash_check = b != 0;
        } else if (strcmp(argv[i], "--experimental-compact-hash-check-time") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr,
                        "--experimental-compact-hash-check-time requires a duration\n");
                return 1;
            }
            uint64_t ms = 0;
            if (cetcd_parse_go_duration_ms(argv[++i], &ms) != CETCD_OK) {
                fprintf(stderr,
                        "--experimental-compact-hash-check-time must be a duration\n");
                return 1;
            }
            cfg.compact_hash_check_time_set = true;
            cfg.compact_hash_check_ms = ms;
        } else if (strncmp(argv[i], "--experimental-compact-hash-check-time=", 39) == 0) {
            uint64_t ms = 0;
            if (cetcd_parse_go_duration_ms(argv[i] + 39, &ms) != CETCD_OK) {
                fprintf(stderr,
                        "--experimental-compact-hash-check-time must be a duration\n");
                return 1;
            }
            cfg.compact_hash_check_time_set = true;
            cfg.compact_hash_check_ms = ms;
        } else if (cetcd_experimental_unsupported_kind(argv[i]) ==
                   CETCD_EX_UNSUP_VALUE) {
            fprintf(stderr, "%s is not supported\n", argv[i]);
            return 1;
        } else if (cetcd_experimental_unsupported_kind(argv[i]) ==
                   CETCD_EX_UNSUP_BOOL) {
            const char *flag = argv[i];
            const char *eq = strchr(argv[i], '=');
            int on = 1;
            if (eq) {
                if (cetcd_parse_bool_flag(eq + 1, &on) != CETCD_OK) {
                    fprintf(stderr, "%s must be true or false\n", flag);
                    return 1;
                }
            } else if (i + 1 < argc && argv[i + 1][0] != '-') {
                if (cetcd_parse_bool_flag(argv[++i], &on) != CETCD_OK) {
                    fprintf(stderr, "%s must be true or false\n", flag);
                    return 1;
                }
            }
            if (on) {
                fprintf(stderr, "%s is not supported\n", flag);
                return 1;
            }
        } else if (strncmp(argv[i], "--experimental-", 15) == 0) {
            fprintf(stderr, "unknown flag: %s\n", argv[i]);
            return 1;
        } else {
            fprintf(stderr, "unknown flag: %s\n", argv[i]);
            return 1;
        }
    }
    if (cfg.keepalive_timeout > 0 && !cfg.keepalive_set) {
        fprintf(stderr, "--grpc-keepalive-timeout requires --grpc-keepalive-time or --grpc-keepalive-interval\n");
        return 1;
    }
    {
        cetcd_auto_compact_mode ac_mode = CETCD_AUTO_COMPACT_PERIODIC;
        if (ac_mode_s) {
            if (cetcd_parse_auto_compaction_mode(ac_mode_s, &ac_mode) != CETCD_OK) {
                fprintf(stderr,
                        "--auto-compaction-mode %s is invalid (periodic or revision)\n",
                        ac_mode_s);
                return 1;
            }
        }
        if (ac_ret_s) {
            uint64_t ret = 0;
            if (cetcd_parse_auto_compaction_retention(ac_ret_s, ac_mode, &ret) != CETCD_OK) {
                fprintf(stderr, "--auto-compaction-retention %s is invalid\n", ac_ret_s);
                return 1;
            }
            if (ret > 0) {
                cfg.auto_compaction_mode = ac_mode;
                cfg.auto_compaction_retention = ret;
            }
        }
    }
    strncpy(cfg.data_dir, data_dir, sizeof(cfg.data_dir) - 1);
    strncpy(cfg.name, name, sizeof(cfg.name) - 1);
    if (cfg.metrics_urls_set && cfg.metrics_port_set) {
        fprintf(stderr, "--listen-metrics-urls cannot be mixed with --metrics-port\n");
        return 1;
    }
    {
        int vmin = cfg.tls_min_version_set ? cfg.tls_min_version : CETCD_TLS_VER_1_2;
        int vmax = cfg.tls_max_version_set ? cfg.tls_max_version : CETCD_TLS_VER_UNSPEC;
        if (cetcd_tls_version_range_ok(vmin, vmax) != CETCD_OK) {
            fprintf(stderr, "--tls-min-version cannot exceed --tls-max-version\n");
            return 1;
        }
    }
    if ((cfg.heartbeat_interval_set || cfg.election_timeout_set) &&
        (cfg.election_tick_set || cfg.heartbeat_tick_set)) {
        fprintf(stderr, "--heartbeat-interval/--election-timeout cannot be mixed with --heartbeat-tick/--election-tick\n");
        return 1;
    }
    if (cfg.heartbeat_interval_set || cfg.election_timeout_set) {
        uint64_t tms = cfg.heartbeat_interval_set ? cfg.tick_ms
                                                  : CETCD_DEFAULT_TICK_MS;
        uint64_t ems = cfg.election_timeout_set ? cfg.election_ms
                                                : CETCD_DEFAULT_ELECTION_MS;
        uint64_t hb = 0, et = 0;
        if (cetcd_raft_timing_from_ms(tms, ems, &hb, &et) != CETCD_OK) {
            fprintf(stderr, "--election-timeout must be >= --heartbeat-interval and <= 50000ms\n");
            return 1;
        }
        cfg.tick_ms = tms;
        cfg.heartbeat_tick = hb;
        cfg.election_tick = et;
    }
    if (cfg.discovery_srv_name[0] && !cfg.discovery_srv[0]) {
        fprintf(stderr, "--discovery-srv-name requires --discovery-srv\n");
        return 1;
    }
    if (cfg.discovery_srv[0]) {
        if (cfg.n_initial_peers > 0) {
            fprintf(stderr, "--discovery-srv cannot be mixed with --initial-cluster\n");
            return 1;
        }
        cetcd_discovery_kind kind = (cfg.peer_listen_https || cfg.initial_cluster_https)
            ? CETCD_DISCOVERY_SERVER_SSL : CETCD_DISCOVERY_SERVER;
        cetcd_endpoint eps[CETCD_MAX_INITIAL_PEERS];
        size_t n = 0;
        const char *nm = cfg.discovery_srv_name[0] ? cfg.discovery_srv_name : NULL;
        if (cetcd_discovery_resolve(kind, nm, cfg.discovery_srv, 0,
                                    eps, CETCD_MAX_INITIAL_PEERS, &n) != 0 || n == 0) {
            fprintf(stderr, "--discovery-srv lookup failed\n");
            return 1;
        }
        for (size_t i = 0; i < n; i++) {
            uint64_t id = 0;
            if (cetcd_discovery_peer_id(eps[i].host, eps[i].port, &id) != 0) {
                fprintf(stderr, "--discovery-srv peer id failed\n");
                return 1;
            }
            for (size_t j = 0; j < i; j++) {
                if (cfg.initial_peers[j].id == id) {
                    fprintf(stderr, "--discovery-srv peer id collision\n");
                    return 1;
                }
            }
            cetcd_peer_info *pi = &cfg.initial_peers[i];
            memset(pi, 0, sizeof(*pi));
            pi->id = id;
            strncpy(pi->addr, eps[i].host, sizeof(pi->addr) - 1);
            pi->port = eps[i].port;
        }
        cfg.n_initial_peers = (uint32_t)n;
    }

    if (cetcd_log_want_rotation(enable_log_rotation_set, enable_log_rotation)) {
        char rot_path[512];
        if (cetcd_log_outputs_single_file(log_out_spec ? log_out_spec : "stderr",
                                          rot_path, sizeof(rot_path)) != CETCD_OK) {
            fprintf(stderr,
                    "--enable-log-rotation requires a single --log-outputs file path\n");
            return 1;
        }
        if (cetcd_log_enable_rotation(rot_path, &log_rot_cfg) != CETCD_OK) {
            fprintf(stderr, "--enable-log-rotation cannot attach to the log file\n");
            return 1;
        }
        log_owned = NULL;
    }

    CETCD_INFO("cetcd v%s starting", cetcd_version());
    CETCD_INFO("  name      : %s", name);
    CETCD_INFO("  node-id   : %llu", (unsigned long long)cfg.node_id);
    CETCD_INFO("  data-dir  : %s", cfg.data_dir);
    if (cfg.wal_dir[0])
        CETCD_INFO("  wal-dir   : %s", cfg.wal_dir);
    CETCD_INFO("  listen    : %s:%u", cfg.listen_addr, cfg.listen_port);
    CETCD_INFO("  peer      : %s:%u", cfg.peer_addr, cfg.peer_port);
    CETCD_INFO("  metrics   : %s:%u", cetcd_server_metrics_addr(&cfg),
               cfg.metrics_port);
    CETCD_INFO("  cluster   : %u peer(s)", cfg.n_initial_peers);
    if (cfg.cert_file[0])
        CETCD_INFO("  tls       : cert=%s", cfg.cert_file);
    if (cfg.peer_cert_file[0])
        CETCD_INFO("  peer-tls  : cert=%s", cfg.peer_cert_file);
    if (cfg.peer_client_cert_file[0])
        CETCD_INFO("  peer-out  : cert=%s", cfg.peer_client_cert_file);

    cetcd_server *srv = cetcd_server_new(&cfg);
    if (!srv) {
        CETCD_FATAL("failed to initialize server");
        return 1;
    }

    g_srv = srv;
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = on_signal;
    sigaction(SIGTERM, &sa, NULL);
    sigaction(SIGINT, &sa, NULL);

    if (cetcd_server_start(srv) != 0) {
        CETCD_FATAL("failed to start server");
        cetcd_server_free(srv);
        g_srv = NULL;
        return 1;
    }

    CETCD_INFO("server initialized, revision=%lld", (long long)cetcd_server_revision(srv));
    CETCD_INFO("ready to serve on %s:%u", cfg.listen_addr, cfg.listen_port);

    cetcd_server_serve(srv);

    CETCD_INFO("shutting down...");
    cetcd_server_free(srv);
    g_srv = NULL;

    CETCD_INFO("shutdown complete");
    cetcd_log_rotation_close();
    if (log_owned) fclose(log_owned);
    return 0;
}
