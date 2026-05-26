#include "cetcd/base.h"
#include "cetcd/server.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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
    printf("  --node-id ID     Node ID (default: 1)\n");
    printf("  --initial-cluster NODE1=ADDR:PORT,NODE2=...  Initial cluster membership\n");
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
        } else if (strcmp(argv[i], "--help") == 0) {
            print_usage(argv[0]);
            return 0;
        }
    }
    strncpy(cfg.data_dir, data_dir, sizeof(cfg.data_dir) - 1);

    printf("cetcd v%s starting\n", cetcd_version());
    printf("  name      : %s\n", name);
    printf("  node-id   : %llu\n", (unsigned long long)cfg.node_id);
    printf("  data-dir  : %s\n", cfg.data_dir);
    printf("  listen    : %s:%u\n", cfg.listen_addr, cfg.listen_port);
    printf("  peer      : %s:%u\n", cfg.peer_addr, cfg.peer_port);
    printf("  cluster   : %u peer(s)\n", cfg.n_initial_peers);

    cetcd_server *srv = cetcd_server_new(&cfg);
    if (!srv) {
        fprintf(stderr, "cetcd: failed to initialize server\n");
        return 1;
    }

    cetcd_server_start(srv);

    printf("cetcd: server initialized, revision=%lld\n",
           (long long)cetcd_server_revision(srv));
    printf("cetcd: ready to serve on %s:%u\n",
           cfg.listen_addr, cfg.listen_port);
    printf("cetcd: shutting down...\n");

    cetcd_server_stop(srv);
    cetcd_server_free(srv);

    printf("cetcd: shutdown complete\n");
    return 0;
}
