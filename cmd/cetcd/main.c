#include "cetcd/base.h"
#include "cetcd/server.h"
#include "cetcd/log.h"
#include "cetcd/metrics.h"

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
    printf("  --name NAME      Member name (default: default)\n");
    printf("  --data-dir DIR   Data directory (default: ./data)\n");
    printf("  --listen ADDR    Client listen address (default: 127.0.0.1)\n");
    printf("  --port PORT      Client listen port (default: 2379; 1..65535)\n");
    printf("  --peer ADDR      Peer listen address (default: 127.0.0.1)\n");
    printf("  --peer-port PORT Peer listen port (default: 2380; 1..65535)\n");
    printf("  --metrics-port PORT Metrics listen port (default: 2381, 0 to disable)\n");
    printf("  --node-id ID     Node ID (default: 1)\n");
    printf("  --initial-cluster NODE1=ADDR:PORT,NODE2=...  Initial cluster (https requires --peer-cert-file)\n");
    printf("  --election-tick N   Raft election tick (default: 10)\n");
    printf("  --heartbeat-tick N  Raft heartbeat tick (default: 1)\n");
    printf("  --log-level LVL  Log level: trace,debug,info,warn,error (default: info)\n");
    printf("  --log-format FMT Log format: text,json (etcd console = text; others fail)\n");
    printf("\n  etcd-compatible flags (accepted for compatibility):\n");
    printf("  --listen-client-urls URL    Client listen URL (https requires --cert-file)\n");
    printf("  --listen-peer-urls URL      Peer listen URL (https requires --peer-cert-file)\n");
    printf("  --advertise-client-urls URL  MemberList clientURLs (https requires --cert-file)\n");
    printf("  --initial-advertise-peer-urls URL  MemberList peerURLs (https requires --peer-cert-file)\n");
    printf("  --initial-cluster-state STATE  new (default); existing is not implemented\n");
    printf("  --initial-cluster-token TOKEN  Persist in data-dir; mismatch fail-closes\n");
    printf("  --snapshot-count N   Rewrite WAL after N applies (default: 10000)\n");
    printf("  --quota-backend-bytes N  NOSPACE when LMDB size >= N (0 = unlimited)\n");
    printf("  --force-new-cluster  Not implemented (fail-closed; would wipe data_dir)\n");
    printf("  --max-txn-ops N     Max compare/success/failure ops per Txn (default 128, max 128)\n");
    printf("  --max-request-bytes N  Max client frame (default 1572864); oversized closes\n");
    printf("  --grpc-keepalive-time SEC   TCP keepalive idle on client and peer sockets (0 disables)\n");
    printf("  --grpc-keepalive-timeout SEC  TCP keepalive interval (requires --grpc-keepalive-time)\n");
    printf("  --grpc-keepalive-min-time SEC  Accepted duration (not applied; 0..86400)\n");
    printf("  --grpc-keepalive-*  Other grpc-keepalive flags accepted as no-op\n");
    printf("  --auth-token TYPE   simple (default) or jwt,sign-method=HS256|RS256|ES256,priv-key=PATH[,ttl=5m]\n");
    printf("  --bcrypt-cost N     Hash new passwords with bcrypt (4..31; default SHA-256)\n");
    printf("  --cert-file FILE    Client TLS certificate (requires --key-file)\n");
    printf("  --key-file FILE     Client TLS private key\n");
    printf("  --trusted-ca-file FILE  Client TLS CA (required with --client-cert-auth)\n");
    printf("  --client-cert-auth  Require a client certificate (fail-closed)\n");
    printf("  --auto-tls           Not implemented (fail-closed without --cert-file)\n");
    printf("  --peer-cert-file FILE    Peer accept TLS certificate (requires --peer-key-file)\n");
    printf("  --peer-key-file FILE     Peer accept TLS private key\n");
    printf("  --peer-trusted-ca-file FILE  Peer TLS CA (required with --peer-client-cert-auth)\n");
    printf("  --peer-client-cert-auth  Require a peer certificate on accept (fail-closed)\n");
    printf("  --peer-auto-tls      Not implemented (fail-closed without --peer-cert-file)\n");
    printf("  --cipher-suites LIST  TLS 1.2/1.3 cipher list (IANA or OpenSSL names; requires TLS)\n");
    printf("  --logger TYPE       zap or capnslog (built-in logger; others fail)\n");
    printf("  --log-outputs LIST   stderr or stdout; a file path fail-closes\n");
    printf("  --experimental-*    Accepted but no-op\n");
    printf("  --help           Show this help\n");
}

static int parse_keepalive_sec_(const char *s, int min_v, int *out) {
    char *end = NULL;
    errno = 0;
    long v = strtol(s, &end, 10);
    if (errno == ERANGE || !end || end == s || v < min_v || v > 86400) return -1;
    if (*end == 's' || *end == 'S') end++;
    if (*end) return -1;
    *out = (int)v;
    return 0;
}

int main(int argc, char **argv) {
    const char *name = "default";
    const char *data_dir = "./data";

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

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--name") == 0 && i + 1 < argc) {
            name = argv[++i];
        } else if (strcmp(argv[i], "--data-dir") == 0 && i + 1 < argc) {
            data_dir = argv[++i];
        } else if (strcmp(argv[i], "--listen") == 0 && i + 1 < argc) {
            strncpy(cfg.listen_addr, argv[++i], sizeof(cfg.listen_addr) - 1);
        } else if (strcmp(argv[i], "--port") == 0 && i + 1 < argc) {
            char *end = NULL;
            errno = 0;
            long v = strtol(argv[++i], &end, 10);
            if (errno == ERANGE || !end || *end || v < 1 || v > 65535) {
                fprintf(stderr, "--port must be 1..65535\n");
                return 1;
            }
            cfg.listen_port = (uint16_t)v;
        } else if (strcmp(argv[i], "--peer") == 0 && i + 1 < argc) {
            strncpy(cfg.peer_addr, argv[++i], sizeof(cfg.peer_addr) - 1);
        } else if (strcmp(argv[i], "--peer-port") == 0 && i + 1 < argc) {
            char *end = NULL;
            errno = 0;
            long v = strtol(argv[++i], &end, 10);
            if (errno == ERANGE || !end || *end || v < 1 || v > 65535) {
                fprintf(stderr, "--peer-port must be 1..65535\n");
                return 1;
            }
            cfg.peer_port = (uint16_t)v;
        } else if (strcmp(argv[i], "--metrics-port") == 0 && i + 1 < argc) {
            cfg.metrics_port = (uint16_t)atoi(argv[++i]);
        } else if (strcmp(argv[i], "--node-id") == 0 && i + 1 < argc) {
            cfg.node_id = (uint64_t)atol(argv[++i]);
        } else if (strcmp(argv[i], "--initial-cluster") == 0 && i + 1 < argc) {
            const char *cluster_str = argv[++i];
            char buf[2048];
            strncpy(buf, cluster_str, sizeof(buf) - 1);
            buf[sizeof(buf) - 1] = '\0';
            char *saveptr = NULL;
            char *tok = strtok_r(buf, ",", &saveptr);
            while (tok && cfg.n_initial_peers < CETCD_MAX_INITIAL_PEERS) {
                char *eq = strchr(tok, '=');
                if (eq) {
                    *eq = '\0';
                    char *addr_part = eq + 1;
                    uint64_t nid = (uint64_t)atol(tok);
                    cetcd_peer_info *pi = &cfg.initial_peers[cfg.n_initial_peers];
                    pi->id = nid;
                    char *colon = strrchr(addr_part, ':');
                    if (colon) {
                        *colon = '\0';
                        if (strncmp(addr_part, "https://", 8) == 0) {
                            cfg.initial_cluster_https = true;
                            addr_part += 8;
                        } else if (strncmp(addr_part, "http://", 7) == 0) {
                            addr_part += 7;
                        }
                        strncpy(pi->addr, addr_part, sizeof(pi->addr) - 1);
                        pi->port = (uint16_t)atoi(colon + 1);
                    } else {
                        if (strncmp(addr_part, "https://", 8) == 0) {
                            cfg.initial_cluster_https = true;
                            addr_part += 8;
                        } else if (strncmp(addr_part, "http://", 7) == 0) {
                            addr_part += 7;
                        }
                        strncpy(pi->addr, addr_part, sizeof(pi->addr) - 1);
                        pi->port = 2380;
                    }
                    cfg.n_initial_peers++;
                }
                tok = strtok_r(NULL, ",", &saveptr);
            }
        } else if (strcmp(argv[i], "--log-level") == 0 && i + 1 < argc) {
            const char *lvl = argv[++i];
            if (strcmp(lvl, "trace") == 0) cetcd_log_set_level(CETCD_LOG_TRACE);
            else if (strcmp(lvl, "debug") == 0) cetcd_log_set_level(CETCD_LOG_DEBUG);
            else if (strcmp(lvl, "info") == 0) cetcd_log_set_level(CETCD_LOG_INFO);
            else if (strcmp(lvl, "warn") == 0 || strcmp(lvl, "warning") == 0)
                cetcd_log_set_level(CETCD_LOG_WARN);
            else if (strcmp(lvl, "error") == 0 ||
                     strcmp(lvl, "dpanic") == 0 ||
                     strcmp(lvl, "panic") == 0 ||
                     strcmp(lvl, "fatal") == 0)
                cetcd_log_set_level(CETCD_LOG_ERROR);
            else {
                fprintf(stderr,
                        "--log-level %s is not supported (trace, debug, info, warn, error)\n",
                        lvl);
                return 1;
            }
        } else if (strcmp(argv[i], "--log-format") == 0 && i + 1 < argc) {
            const char *fmt = argv[++i];
            if (strcmp(fmt, "json") == 0) cetcd_log_set_format(CETCD_LOG_FORMAT_JSON);
            else if (strcmp(fmt, "text") == 0 || strcmp(fmt, "console") == 0)
                cetcd_log_set_format(CETCD_LOG_FORMAT_TEXT);
            else {
                fprintf(stderr,
                        "--log-format %s is not supported (text, json)\n",
                        fmt);
                return 1;
            }
        } else if (strcmp(argv[i], "--help") == 0) {
            print_usage(argv[0]);
            return 0;
        } else if (strcmp(argv[i], "--listen-client-urls") == 0 && i + 1 < argc) {
            /* Parse URL format: http://addr:port */
            const char *url = argv[++i];
            const char *addr_start = url;
            if (strncmp(url, "https://", 8) == 0) {
                cfg.listen_https = true;
                addr_start = url + 8;
            } else if (strncmp(url, "http://", 7) == 0) {
                addr_start = url + 7;
            }
            const char *colon = strrchr(addr_start, ':');
            if (colon) {
                size_t alen = (size_t)(colon - addr_start);
                if (alen < sizeof(cfg.listen_addr)) {
                    memcpy(cfg.listen_addr, addr_start, alen);
                    cfg.listen_addr[alen] = '\0';
                }
                cfg.listen_port = (uint16_t)atoi(colon + 1);
            } else {
                strncpy(cfg.listen_addr, addr_start, sizeof(cfg.listen_addr) - 1);
            }
        } else if (strcmp(argv[i], "--listen-peer-urls") == 0 && i + 1 < argc) {
            const char *url = argv[++i];
            const char *addr_start = url;
            if (strncmp(url, "https://", 8) == 0) {
                cfg.peer_listen_https = true;
                addr_start = url + 8;
            } else if (strncmp(url, "http://", 7) == 0) {
                addr_start = url + 7;
            }
            const char *colon = strrchr(addr_start, ':');
            if (colon) {
                size_t alen = (size_t)(colon - addr_start);
                if (alen < sizeof(cfg.peer_addr)) {
                    memcpy(cfg.peer_addr, addr_start, alen);
                    cfg.peer_addr[alen] = '\0';
                }
                cfg.peer_port = (uint16_t)atoi(colon + 1);
            } else {
                strncpy(cfg.peer_addr, addr_start, sizeof(cfg.peer_addr) - 1);
            }
        } else if (strcmp(argv[i], "--election-tick") == 0 && i + 1 < argc) {
            cfg.election_tick = (int)atoi(argv[++i]);
            if (cfg.election_tick <= 0) cfg.election_tick = 10;
        } else if (strcmp(argv[i], "--heartbeat-tick") == 0 && i + 1 < argc) {
            cfg.heartbeat_tick = (int)atoi(argv[++i]);
            if (cfg.heartbeat_tick <= 0) cfg.heartbeat_tick = 1;
        } else if (strcmp(argv[i], "--advertise-client-urls") == 0 && i + 1 < argc) {
            const char *url = argv[++i];
            size_t n = 0;
            while (url[n] && url[n] != ',' &&
                   n + 1 < sizeof(cfg.advertise_client_urls))
                n++;
            memcpy(cfg.advertise_client_urls, url, n);
            cfg.advertise_client_urls[n] = '\0';
        } else if (strcmp(argv[i], "--initial-advertise-peer-urls") == 0 && i + 1 < argc) {
            const char *url = argv[++i];
            size_t n = 0;
            while (url[n] && url[n] != ',' &&
                   n + 1 < sizeof(cfg.advertise_peer_urls))
                n++;
            memcpy(cfg.advertise_peer_urls, url, n);
            cfg.advertise_peer_urls[n] = '\0';
        } else if (strcmp(argv[i], "--initial-cluster-state") == 0 && i + 1 < argc) {
            strncpy(cfg.initial_cluster_state, argv[++i],
                    sizeof(cfg.initial_cluster_state) - 1);
        } else if (strcmp(argv[i], "--initial-cluster-token") == 0 && i + 1 < argc) {
            strncpy(cfg.initial_cluster_token, argv[++i],
                    sizeof(cfg.initial_cluster_token) - 1);
        } else if (strcmp(argv[i], "--snapshot-count") == 0 && i + 1 < argc) {
            cfg.snapshot_count = (uint64_t)strtoull(argv[++i], NULL, 10);
        } else if (strcmp(argv[i], "--quota-backend-bytes") == 0 && i + 1 < argc) {
            cfg.quota_backend_bytes = (uint64_t)strtoull(argv[++i], NULL, 10);
        } else if (strcmp(argv[i], "--force-new-cluster") == 0) {
            cfg.force_new_cluster = true;
        } else if (strcmp(argv[i], "--max-txn-ops") == 0 && i + 1 < argc) {
            cfg.max_txn_ops = (uint64_t)strtoull(argv[++i], NULL, 10);
        } else if (strcmp(argv[i], "--max-request-bytes") == 0 && i + 1 < argc) {
            cfg.max_request_bytes = (uint64_t)strtoull(argv[++i], NULL, 10);
        } else if (strcmp(argv[i], "--auth-token") == 0 && i + 1 < argc) {
            strncpy(cfg.auth_token, argv[++i], sizeof(cfg.auth_token) - 1);
        } else if (strcmp(argv[i], "--bcrypt-cost") == 0 && i + 1 < argc) {
            cfg.bcrypt_cost = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--cert-file") == 0 && i + 1 < argc) {
            strncpy(cfg.cert_file, argv[++i], sizeof(cfg.cert_file) - 1);
        } else if (strcmp(argv[i], "--key-file") == 0 && i + 1 < argc) {
            strncpy(cfg.key_file, argv[++i], sizeof(cfg.key_file) - 1);
        } else if (strcmp(argv[i], "--trusted-ca-file") == 0 && i + 1 < argc) {
            strncpy(cfg.trusted_ca_file, argv[++i], sizeof(cfg.trusted_ca_file) - 1);
        } else if (strcmp(argv[i], "--client-cert-auth") == 0) {
            cfg.client_cert_auth = true;
        } else if (strcmp(argv[i], "--auto-tls") == 0) {
            cfg.auto_tls = true;
        } else if (strcmp(argv[i], "--peer-cert-file") == 0 && i + 1 < argc) {
            strncpy(cfg.peer_cert_file, argv[++i], sizeof(cfg.peer_cert_file) - 1);
        } else if (strcmp(argv[i], "--peer-key-file") == 0 && i + 1 < argc) {
            strncpy(cfg.peer_key_file, argv[++i], sizeof(cfg.peer_key_file) - 1);
        } else if (strcmp(argv[i], "--peer-trusted-ca-file") == 0 && i + 1 < argc) {
            strncpy(cfg.peer_trusted_ca_file, argv[++i], sizeof(cfg.peer_trusted_ca_file) - 1);
        } else if (strcmp(argv[i], "--peer-client-cert-auth") == 0) {
            cfg.peer_client_cert_auth = true;
        } else if (strcmp(argv[i], "--peer-auto-tls") == 0) {
            cfg.peer_auto_tls = true;
        } else if (strcmp(argv[i], "--cipher-suites") == 0 && i + 1 < argc) {
            strncpy(cfg.cipher_suites, argv[++i], sizeof(cfg.cipher_suites) - 1);
        } else if (strcmp(argv[i], "--logger") == 0 && i + 1 < argc) {
            const char *lg = argv[++i];
            if (strcmp(lg, "zap") != 0 && strcmp(lg, "capnslog") != 0) {
                fprintf(stderr, "--logger %s is not supported (zap or capnslog)\n",
                        lg);
                return 1;
            }
        } else if (strcmp(argv[i], "--log-outputs") == 0 && i + 1 < argc) {
            const char *out = argv[++i];
            if (strcmp(out, "stderr") == 0 || strcmp(out, "/dev/stderr") == 0) {
                cetcd_log_set_sink(stderr);
            } else if (strcmp(out, "stdout") == 0 || strcmp(out, "/dev/stdout") == 0) {
                cetcd_log_set_sink(stdout);
            } else {
                fprintf(stderr, "--log-outputs %s is not supported (stderr or stdout)\n",
                        out);
                return 1;
            }
        } else if (strcmp(argv[i], "--grpc-keepalive-time") == 0 && i + 1 < argc) {
            if (parse_keepalive_sec_(argv[++i], 0, &cfg.keepalive_time) != 0) {
                fprintf(stderr, "--grpc-keepalive-time must be 0..86400 seconds\n");
                return 1;
            }
            cfg.keepalive_set = true;
        } else if (strcmp(argv[i], "--grpc-keepalive-timeout") == 0 && i + 1 < argc) {
            if (parse_keepalive_sec_(argv[++i], 1, &cfg.keepalive_timeout) != 0) {
                fprintf(stderr, "--grpc-keepalive-timeout must be 1..86400 seconds\n");
                return 1;
            }
        } else if (strcmp(argv[i], "--grpc-keepalive-min-time") == 0 && i + 1 < argc) {
            int dummy;
            if (parse_keepalive_sec_(argv[++i], 0, &dummy) != 0) {
                fprintf(stderr, "--grpc-keepalive-min-time must be 0..86400 seconds\n");
                return 1;
            }
        } else if (strncmp(argv[i], "--grpc-keepalive-", 17) == 0 && i + 1 < argc) {
            i++; /* no-op, e.g. --grpc-keepalive-permit-without-stream */
        } else if (strncmp(argv[i], "--experimental-", 15) == 0) {
            /* no-op, accepted for etcd compatibility */
            if (i + 1 < argc && argv[i + 1][0] != '-') i++; /* skip value if present */
        } else {
            fprintf(stderr, "unknown flag: %s\n", argv[i]);
            return 1;
        }
    }
    if (cfg.keepalive_timeout > 0 && !cfg.keepalive_set) {
        fprintf(stderr, "--grpc-keepalive-timeout requires --grpc-keepalive-time\n");
        return 1;
    }
    strncpy(cfg.data_dir, data_dir, sizeof(cfg.data_dir) - 1);
    strncpy(cfg.name, name, sizeof(cfg.name) - 1);

    CETCD_INFO("cetcd v%s starting", cetcd_version());
    CETCD_INFO("  name      : %s", name);
    CETCD_INFO("  node-id   : %llu", (unsigned long long)cfg.node_id);
    CETCD_INFO("  data-dir  : %s", cfg.data_dir);
    CETCD_INFO("  listen    : %s:%u", cfg.listen_addr, cfg.listen_port);
    CETCD_INFO("  peer      : %s:%u", cfg.peer_addr, cfg.peer_port);
    CETCD_INFO("  metrics   : %s:%u", cfg.listen_addr, cfg.metrics_port);
    CETCD_INFO("  cluster   : %u peer(s)", cfg.n_initial_peers);
    if (cfg.cert_file[0])
        CETCD_INFO("  tls       : cert=%s", cfg.cert_file);
    if (cfg.peer_cert_file[0])
        CETCD_INFO("  peer-tls  : cert=%s", cfg.peer_cert_file);

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
    return 0;
}
