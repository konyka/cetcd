#include "cetcd/raft.h"
#include "cetcd_test.h"
#include <stdlib.h>
#include <string.h>

/* ── Test storage (in-memory) ───────────────────────────────────── */

static cetcd_hard_state test_initial_state(void *ud) {
    (void)ud;
    cetcd_hard_state hs = {0, 0, 0};
    return hs;
}

static cetcd_entry *test_entries(void *ud, uint64_t lo, uint64_t hi,
                                  uint64_t max_size, uint32_t *count) {
    (void)ud; (void)lo; (void)hi; (void)max_size;
    *count = 0;
    return NULL;
}

static uint64_t test_term(void *ud) { (void)ud; return 0; }
static uint64_t test_first_index(void *ud) { (void)ud; return 1; }
static uint64_t test_last_index(void *ud) { (void)ud; return 0; }
static cetcd_snapshot *test_snapshot(void *ud) { (void)ud; return NULL; }

static cetcd_raft_storage test_store = {
    .user_data     = NULL,
    .initial_state = test_initial_state,
    .entries       = test_entries,
    .term          = test_term,
    .first_index   = test_first_index,
    .last_index    = test_last_index,
    .snapshot      = test_snapshot,
};

static cetcd_raft_config single_node_cfg(uint64_t id) {
    cetcd_raft_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.id             = id;
    cfg.election_tick  = 10;
    cfg.heartbeat_tick = 1;
    cfg.storage        = &test_store;
    cfg.max_size_per_msg = 1024 * 1024;
    cfg.max_inflight_msgs = 256;
    cfg.check_quorum   = true;
    cfg.pre_vote       = true;
    return cfg;
}

/* ── Election tests ─────────────────────────────────────────────── */

CETCD_TEST_CASE(single_node_becomes_leader_after_election_timeout) {
    cetcd_raft_config cfg = single_node_cfg(1);
    cetcd_raft *r = cetcd_raft_new(&cfg);
    CETCD_ASSERT_NOT_NULL(r);
    CETCD_ASSERT_TRUE(cetcd_raft_state(r) == CETCD_NODE_FOLLOWER);

    for (int i = 0; i < 10; i++) {
        cetcd_raft_tick(r);
    }
    CETCD_ASSERT_TRUE(cetcd_raft_state(r) == CETCD_NODE_LEADER);
    CETCD_ASSERT_TRUE(cetcd_raft_leader(r) == 1);

    cetcd_raft_free(r);
}

CETCD_TEST_CASE(new_raft_starts_as_follower) {
    cetcd_raft_config cfg = single_node_cfg(1);
    cetcd_raft *r = cetcd_raft_new(&cfg);
    CETCD_ASSERT_TRUE(cetcd_raft_state(r) == CETCD_NODE_FOLLOWER);
    CETCD_ASSERT_TRUE(cetcd_raft_term(r) == 0);
    CETCD_ASSERT_TRUE(cetcd_raft_leader(r) == 0);
    cetcd_raft_free(r);
}

CETCD_TEST_CASE(leader_can_propose_entries) {
    cetcd_raft_config cfg = single_node_cfg(1);
    cetcd_raft *r = cetcd_raft_new(&cfg);

    for (int i = 0; i < 10; i++) cetcd_raft_tick(r);
    CETCD_ASSERT_TRUE(cetcd_raft_state(r) == CETCD_NODE_LEADER);

    int rc = cetcd_raft_propose(r, (const uint8_t *)"hello", 5);
    CETCD_ASSERT_TRUE(rc == 0);

    cetcd_ready rd = cetcd_raft_ready(r);
    CETCD_ASSERT_TRUE(rd.n_entries >= 1);
    CETCD_ASSERT_TRUE(rd.entries != NULL);
    CETCD_ASSERT_TRUE(rd.entries[0].type == CETCD_ENTRY_NORMAL);

    cetcd_ready_free(&rd);
    cetcd_raft_free(r);
}

CETCD_TEST_CASE(advance_clears_ready) {
    cetcd_raft_config cfg = single_node_cfg(1);
    cetcd_raft *r = cetcd_raft_new(&cfg);

    for (int i = 0; i < 10; i++) cetcd_raft_tick(r);
    cetcd_ready rd1 = cetcd_raft_ready(r);
    CETCD_ASSERT_TRUE(rd1.n_entries >= 0 || rd1.hard_state != NULL);

    cetcd_raft_advance(r, &rd1);
    cetcd_ready_free(&rd1);

    cetcd_ready rd2 = cetcd_raft_ready(r);
    CETCD_ASSERT_TRUE(rd2.hard_state == NULL);
    CETCD_ASSERT_TRUE(rd2.n_entries == 0);
    CETCD_ASSERT_TRUE(rd2.n_messages == 0);

    cetcd_ready_free(&rd2);
    cetcd_raft_free(r);
}

CETCD_TEST_CASE(term_increases_on_election) {
    cetcd_raft_config cfg = single_node_cfg(1);
    cetcd_raft *r = cetcd_raft_new(&cfg);
    CETCD_ASSERT_TRUE(cetcd_raft_term(r) == 0);

    for (int i = 0; i < 10; i++) cetcd_raft_tick(r);
    CETCD_ASSERT_TRUE(cetcd_raft_term(r) >= 1);

    cetcd_raft_free(r);
}

CETCD_TEST_CASE(step_with_hup_triggers_election) {
    cetcd_raft_config cfg = single_node_cfg(1);
    cetcd_raft *r = cetcd_raft_new(&cfg);

    cetcd_msg msg;
    memset(&msg, 0, sizeof(msg));
    msg.type = CETCD_MSG_HUP;
    msg.from = 1;

    int rc = cetcd_raft_step(r, &msg);
    CETCD_ASSERT_TRUE(rc == 0);
    CETCD_ASSERT_TRUE(cetcd_raft_state(r) == CETCD_NODE_LEADER);

    cetcd_raft_free(r);
}

CETCD_TEST_CASE(config_null_storage_fails) {
    cetcd_raft_config cfg = single_node_cfg(1);
    cfg.storage = NULL;
    cetcd_raft *r = cetcd_raft_new(&cfg);
    CETCD_ASSERT_TRUE(r == NULL);
}

CETCD_TEST_CASE(free_null_is_safe) {
    cetcd_raft_free(NULL);
}

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(new_raft_starts_as_follower),
    CETCD_TEST_ENTRY(single_node_becomes_leader_after_election_timeout),
    CETCD_TEST_ENTRY(term_increases_on_election),
    CETCD_TEST_ENTRY(leader_can_propose_entries),
    CETCD_TEST_ENTRY(advance_clears_ready),
    CETCD_TEST_ENTRY(step_with_hup_triggers_election),
    CETCD_TEST_ENTRY(config_null_storage_fails),
    CETCD_TEST_ENTRY(free_null_is_safe),
CETCD_TEST_LIST_END

CETCD_TEST_MAIN()
