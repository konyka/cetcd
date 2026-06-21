#include "cetcd/base.h"
#include "cetcd/server.h"
#include "cetcd/log.h"
#include "cetcd/metrics.h"

#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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
    printf("  --port PORT      Client listen port (default: 2379)\n");
    printf("  --peer ADDR      Peer listen address (default: 127.0.0.1)\n");
    printf("  --peer-port PORT Peer listen port (default: 2380)\n");
    printf("  --metrics-port PORT Metrics listen port (default: 2381, 0 to disable)\n");
    printf("  --node-id ID     Node ID (default: 1)\n");
    printf("  --initial-cluster NODE1=ADDR:PORT,NODE2=...  Initial cluster membership\n");
    printf("  --log-level LVL  Log level: trace,debug,info,warn,error (default: info)\n");
    printf("  --log-format FMT Log format: text,json (default: text)\n");
    printf("  --help           Show this help\n");
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
            cfg.listen_port = (uint16_t)atoi(argv[++i]);
        } else if (strcmp(argv[i], "--peer") == 0 && i + 1 < argc) {
            strncpy(cfg.peer_addr, argv[++i], sizeof(cfg.peer_addr) - 1);
        } else if (strcmp(argv[i], "--peer-port") == 0 && i + 1 < argc) {
            cfg.peer_port = (uint16_t)atoi(argv[++i]);
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
                        if (strncmp(addr_part, "http://", 7) == 0) addr_part += 7;
                        else if (strncmp(addr_part, "https://", 8) == 0) addr_part += 8;
                        strncpy(pi->addr, addr_part, sizeof(pi->addr) - 1);
                        pi->port = (uint16_t)atoi(colon + 1);
                    } else {
                        if (strncmp(addr_part, "http://", 7) == 0) addr_part += 7;
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
            else if (strcmp(lvl, "warn") == 0) cetcd_log_set_level(CETCD_LOG_WARN);
            else if (strcmp(lvl, "error") == 0) cetcd_log_set_level(CETCD_LOG_ERROR);
            else cetcd_log_set_level(CETCD_LOG_INFO);
        } else if (strcmp(argv[i], "--log-format") == 0 && i + 1 < argc) {
            const char *fmt = argv[++i];
            if (strcmp(fmt, "json") == 0) cetcd_log_set_format(CETCD_LOG_FORMAT_JSON);
            else cetcd_log_set_format(CETCD_LOG_FORMAT_TEXT);
        } else if (strcmp(argv[i], "--help") == 0) {
            print_usage(argv[0]);
            return 0;
        }
    }
    strncpy(cfg.data_dir, data_dir, sizeof(cfg.data_dir) - 1);

    CETCD_INFO("cetcd v%s starting", cetcd_version());
    CETCD_INFO("  name      : %s", name);
    CETCD_INFO("  node-id   : %llu", (unsigned long long)cfg.node_id);
    CETCD_INFO("  data-dir  : %s", cfg.data_dir);
    CETCD_INFO("  listen    : %s:%u", cfg.listen_addr, cfg.listen_port);
    CETCD_INFO("  peer      : %s:%u", cfg.peer_addr, cfg.peer_port);
    CETCD_INFO("  metrics   : %s:%u", cfg.listen_addr, cfg.metrics_port);
    CETCD_INFO("  cluster   : %u peer(s)", cfg.n_initial_peers);

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

    cetcd_server_start(srv);

    CETCD_INFO("server initialized, revision=%lld", (long long)cetcd_server_revision(srv));
    CETCD_INFO("ready to serve on %s:%u", cfg.listen_addr, cfg.listen_port);

    cetcd_server_serve(srv);

    CETCD_INFO("shutting down...");
    cetcd_server_free(srv);
    g_srv = NULL;

    CETCD_INFO("shutdown complete");
    return 0;
}
