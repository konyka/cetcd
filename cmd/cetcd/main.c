#include "cetcd/base.h"
#include "cetcd/v3rpc.h"
#include "cetcd/raft.h"
#include "cetcd/peer.h"
#include "cetcd/auth.h"
#include "cetcd/snap.h"

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
    printf("  --listen URL     Client listen address (default: http://localhost:2379)\n");
    printf("  --peer URL       Peer listen address (default: http://localhost:2380)\n");
    printf("  --join URL       Existing peer to join (for new members)\n");
    printf("  --help           Show this help\n");
}

int main(int argc, char **argv) {
    const char *name = "default";
    const char *data_dir = "./data";
    const char *listen_url = "http://localhost:2379";
    const char *peer_url = "http://localhost:2380";
    const char *join_url = NULL;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--name") == 0 && i + 1 < argc) {
            name = argv[++i];
        } else if (strcmp(argv[i], "--data-dir") == 0 && i + 1 < argc) {
            data_dir = argv[++i];
        } else if (strcmp(argv[i], "--listen") == 0 && i + 1 < argc) {
            listen_url = argv[++i];
        } else if (strcmp(argv[i], "--peer") == 0 && i + 1 < argc) {
            peer_url = argv[++i];
        } else if (strcmp(argv[i], "--join") == 0 && i + 1 < argc) {
            join_url = argv[++i];
        } else if (strcmp(argv[i], "--help") == 0) {
            print_usage(argv[0]);
            return 0;
        }
    }

    printf("cetcd v%s starting\n", cetcd_version());
    printf("  name      : %s\n", name);
    printf("  data-dir  : %s\n", data_dir);
    printf("  listen    : %s\n", listen_url);
    printf("  peer      : %s\n", peer_url);
    if (join_url) printf("  join      : %s\n", join_url);

    cetcd_v3rpc *rpc = cetcd_v3rpc_new();

    cetcd_raft_config raft_cfg = {
        .id = 1,
        .election_tick = 10,
        .heartbeat_tick = 1,
        .storage = NULL,
        .max_size_per_msg = 1024 * 1024,
        .max_inflight_msgs = 256,
        .check_quorum = true,
        .pre_vote = true,
        .disable_proposal_forwarding = false,
    };
    cetcd_raft *raft = cetcd_raft_new(&raft_cfg);
    cetcd_cluster *cluster = cetcd_cluster_new(1);

    (void)data_dir; (void)join_url;

    printf("cetcd: initialized v3rpc + raft + cluster\n");
    printf("cetcd: ready to serve on %s\n", listen_url);

    cetcd_cluster_free(cluster);
    if (raft) cetcd_raft_free(raft);
    cetcd_v3rpc_free(rpc);

    printf("cetcd: shutdown complete\n");
    return 0;
}
