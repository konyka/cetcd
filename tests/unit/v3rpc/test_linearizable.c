#include "cetcd/v3rpc.h"
#include "cetcd/raft.h"
#include "cetcd_test.h"

#include <string.h>

#ifdef TEST_DEFINE_RPC_GLOBALS
cetcd_raft *g_rpc_raft = NULL;
uint64_t    g_rpc_node_id = 0;
#else
extern cetcd_raft *g_rpc_raft;
extern uint64_t    g_rpc_node_id;
#endif

static cetcd_raft *make_raft_(uint64_t id) {
    cetcd_raft_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.id = id;
    cfg.election_tick = 10;
    cfg.heartbeat_tick = 1;
    cfg.max_size_per_msg = 1024 * 1024;
    cfg.max_inflight_msgs = 256;
    return cetcd_raft_new(&cfg);
}

CETCD_TEST_CASE(linearizable_ok_without_raft) {
    g_rpc_raft = NULL;
    g_rpc_node_id = 0;
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_linearizable_ok(0), 1);
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_linearizable_ok(1), 1);
}

CETCD_TEST_CASE(linearizable_fail_closed_without_leader) {
    cetcd_raft *r = make_raft_(1);
    CETCD_ASSERT_NOT_NULL(r);
    g_rpc_raft = r;
    g_rpc_node_id = 1;
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_linearizable_ok(0), 0);
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_linearizable_ok(1), 1);
    g_rpc_raft = NULL;
    cetcd_raft_free(r);
}

CETCD_TEST_CASE(linearizable_ok_on_leader) {
    cetcd_raft *r = make_raft_(1);
    CETCD_ASSERT_NOT_NULL(r);
    for (int i = 0; i < 10; i++) cetcd_raft_tick(r);
    CETCD_ASSERT_TRUE(cetcd_raft_leader(r) == 1);
    g_rpc_raft = r;
    g_rpc_node_id = 1;
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_linearizable_ok(0), 1);
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_linearizable_ok(1), 1);
    g_rpc_node_id = 2;
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_linearizable_ok(0), 0);
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_linearizable_ok(1), 1);
    g_rpc_raft = NULL;
    g_rpc_node_id = 0;
    cetcd_raft_free(r);
}

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(linearizable_ok_without_raft),
    CETCD_TEST_ENTRY(linearizable_fail_closed_without_leader),
    CETCD_TEST_ENTRY(linearizable_ok_on_leader),
CETCD_TEST_LIST_END

CETCD_TEST_MAIN()
