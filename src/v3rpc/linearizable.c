#include "cetcd/v3rpc.h"
#include "cetcd/raft.h"

extern cetcd_raft *g_rpc_raft;
extern uint64_t    g_rpc_node_id;

int cetcd_v3rpc_linearizable_ok(int serializable) {
    if (serializable) return 1;
    if (!g_rpc_raft) return 1;
    uint64_t leader = cetcd_raft_leader(g_rpc_raft);
    if (leader == 0 || g_rpc_node_id == 0) return 0;
    return leader == g_rpc_node_id;
}
