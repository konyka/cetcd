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
    CETCD_ASSERT_TRUE(rd.committed >= 1);

    cetcd_ready_free(&rd);
    cetcd_raft_free(r);
}

CETCD_TEST_CASE(single_node_propose_commits) {
    cetcd_raft_config cfg = single_node_cfg(1);
    cetcd_raft *r = cetcd_raft_new(&cfg);
    for (int i = 0; i < 10; i++) cetcd_raft_tick(r);
    CETCD_ASSERT_TRUE(cetcd_raft_state(r) == CETCD_NODE_LEADER);

    CETCD_ASSERT_EQ_INT(cetcd_raft_propose(r, (const uint8_t *)"a", 1), 0);
    CETCD_ASSERT_TRUE(cetcd_raft_committed(r) >= 1);
    CETCD_ASSERT_TRUE(cetcd_raft_last_index(r) >= 1);
    const cetcd_entry *e = cetcd_raft_entry_at(r, 1);
    CETCD_ASSERT_NOT_NULL(e);
    CETCD_ASSERT_EQ_INT((int)e->data.len, 1);
    CETCD_ASSERT_EQ_INT((int)e->data.data[0], (int)'a');

    /* Owned copy: caller buffer can go away. */
    uint8_t tmp[3] = {'x', 'y', 'z'};
    CETCD_ASSERT_EQ_INT(cetcd_raft_propose(r, tmp, 3), 0);
    tmp[0] = 0;
    const cetcd_entry *e2 = cetcd_raft_entry_at(r, 2);
    CETCD_ASSERT_NOT_NULL(e2);
    CETCD_ASSERT_EQ_INT((int)e2->data.len, 3);
    CETCD_ASSERT_EQ_INT((int)e2->data.data[0], (int)'x');

    cetcd_raft_free(r);
}

CETCD_TEST_CASE(restore_entry_and_hard_state) {
    cetcd_raft_config cfg = single_node_cfg(1);
    cetcd_raft *r = cetcd_raft_new(&cfg);
    cetcd_entry e;
    memset(&e, 0, sizeof(e));
    e.term = 3;
    e.index = 1;
    e.type = CETCD_ENTRY_NORMAL;
    uint8_t payload[] = {1, 2, 3};
    e.data = cetcd_slice_make(payload, 3);
    CETCD_ASSERT_EQ_INT(cetcd_raft_restore_entry(r, &e), 0);
    payload[0] = 9;
    const cetcd_entry *got = cetcd_raft_entry_at(r, 1);
    CETCD_ASSERT_NOT_NULL(got);
    CETCD_ASSERT_EQ_INT((int)got->data.data[0], 1);

    cetcd_hard_state hs = {.term = 3, .vote = 1, .commit = 1};
    cetcd_raft_restore_hard_state(r, &hs);
    CETCD_ASSERT_TRUE(cetcd_raft_term(r) == 3);
    CETCD_ASSERT_TRUE(cetcd_raft_committed(r) == 1);
    cetcd_raft_set_applied(r, 1);
    CETCD_ASSERT_TRUE(cetcd_raft_applied(r) == 1);

    cetcd_raft_free(r);
}

CETCD_TEST_CASE(advance_clears_ready) {
    cetcd_raft_config cfg = single_node_cfg(1);
    cetcd_raft *r = cetcd_raft_new(&cfg);

    for (int i = 0; i < 10; i++) cetcd_raft_tick(r);
    cetcd_ready rd1 = cetcd_raft_ready(r);
    CETCD_ASSERT_TRUE(rd1.n_entries > 0 || rd1.hard_state != NULL);

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

CETCD_TEST_CASE(config_null_storage_ok) {
    cetcd_raft_config cfg = single_node_cfg(1);
    cfg.storage = NULL;
    cetcd_raft *r = cetcd_raft_new(&cfg);
    CETCD_ASSERT_NOT_NULL(r);
    for (int i = 0; i < 10; i++) cetcd_raft_tick(r);
    CETCD_ASSERT_TRUE(cetcd_raft_state(r) == CETCD_NODE_LEADER);
    cetcd_raft_free(r);
}

CETCD_TEST_CASE(free_null_is_safe) {
    cetcd_raft_free(NULL);
}

CETCD_TEST_CASE(learner_does_not_block_single_node_commit) {
    cetcd_raft_config cfg = single_node_cfg(1);
    cetcd_raft *r = cetcd_raft_new(&cfg);
    for (int i = 0; i < 10; i++) cetcd_raft_tick(r);
    CETCD_ASSERT_TRUE(cetcd_raft_state(r) == CETCD_NODE_LEADER);
    CETCD_ASSERT_EQ_INT(cetcd_raft_add_peer(r, 2, 1), 0);
    CETCD_ASSERT_EQ_INT(cetcd_raft_propose(r, (const uint8_t *)"x", 1), 0);
    {
        cetcd_ready rd = cetcd_raft_ready(r);
        cetcd_ready_free(&rd);
    }
    CETCD_ASSERT_TRUE(cetcd_raft_committed(r) >= 1);
    CETCD_ASSERT_EQ_INT(cetcd_raft_promote(r, 2), 0);
    CETCD_ASSERT_EQ_INT(cetcd_raft_promote(r, 2), -1);
    CETCD_ASSERT_EQ_INT(cetcd_raft_promote(r, 99), -1);
    cetcd_raft_free(r);
}

CETCD_TEST_CASE(joint_add_voter_blocks_commit_until_ack) {
    cetcd_raft_config cfg = single_node_cfg(1);
    cetcd_raft *r = cetcd_raft_new(&cfg);
    for (int i = 0; i < 10; i++) cetcd_raft_tick(r);
    CETCD_ASSERT_TRUE(cetcd_raft_state(r) == CETCD_NODE_LEADER);
    CETCD_ASSERT_EQ_INT(cetcd_raft_propose(r, (const uint8_t *)"a", 1), 0);
    uint64_t committed = cetcd_raft_committed(r);
    CETCD_ASSERT_TRUE(committed >= 1);

    CETCD_ASSERT_EQ_INT(cetcd_raft_enter_joint(r), 0);
    CETCD_ASSERT_TRUE(cetcd_raft_in_joint(r));
    CETCD_ASSERT_EQ_INT(cetcd_raft_enter_joint(r), -1);
    CETCD_ASSERT_EQ_INT(cetcd_raft_add_peer(r, 2, 0), 0);

    CETCD_ASSERT_EQ_INT(cetcd_raft_propose(r, (const uint8_t *)"b", 1), 0);
    CETCD_ASSERT_TRUE(cetcd_raft_committed(r) == committed);

    cetcd_msg ack;
    memset(&ack, 0, sizeof(ack));
    ack.type = CETCD_MSG_APP_RESP;
    ack.from = 2;
    ack.to = 1;
    ack.term = cetcd_raft_term(r);
    ack.index = cetcd_raft_last_index(r);
    ack.reject = 0;
    CETCD_ASSERT_EQ_INT(cetcd_raft_step(r, &ack), 0);
    CETCD_ASSERT_TRUE(cetcd_raft_committed(r) > committed);
    CETCD_ASSERT_TRUE(cetcd_raft_joint_caught_up(r));
    CETCD_ASSERT_EQ_INT(cetcd_raft_leave_joint(r), 0);
    CETCD_ASSERT_TRUE(!cetcd_raft_in_joint(r));
    {
        cetcd_ready rd = cetcd_raft_ready(r);
        cetcd_ready_free(&rd);
    }
    cetcd_raft_free(r);
}

CETCD_TEST_CASE(joint_restore_and_leave) {
    cetcd_raft_config cfg = single_node_cfg(1);
    cetcd_raft *r = cetcd_raft_new(&cfg);
    CETCD_ASSERT_EQ_INT(cetcd_raft_add_peer(r, 2, 0), 0);
    uint64_t ids[1] = {1};
    CETCD_ASSERT_EQ_INT(cetcd_raft_restore_joint(r, ids, 1, 3), 0);
    CETCD_ASSERT_TRUE(cetcd_raft_in_joint(r));
    CETCD_ASSERT_TRUE(cetcd_raft_joint_index(r) == 3);
    uint64_t out[4];
    CETCD_ASSERT_EQ_INT((int)cetcd_raft_copy_outgoing(r, out, 4), 1);
    CETCD_ASSERT_TRUE(out[0] == 1);
    CETCD_ASSERT_EQ_INT(cetcd_raft_leave_joint(r), 0);
    CETCD_ASSERT_TRUE(!cetcd_raft_in_joint(r));
    cetcd_raft_free(r);
}

CETCD_TEST_CASE(compact_drops_prefix_keeps_dummy) {
    cetcd_raft_config cfg = single_node_cfg(1);
    cetcd_raft *r = cetcd_raft_new(&cfg);
    for (int i = 0; i < 10; i++) cetcd_raft_tick(r);
    CETCD_ASSERT_TRUE(cetcd_raft_state(r) == CETCD_NODE_LEADER);
    CETCD_ASSERT_EQ_INT(cetcd_raft_propose(r, (const uint8_t *)"a", 1), 0);
    CETCD_ASSERT_EQ_INT(cetcd_raft_propose(r, (const uint8_t *)"b", 1), 0);
    uint64_t last = cetcd_raft_last_index(r);
    CETCD_ASSERT_TRUE(last >= 2);
    uint64_t term = cetcd_raft_entry_at(r, last)->term;
    CETCD_ASSERT_EQ_INT(cetcd_raft_compact(r, last, term), 0);
    CETCD_ASSERT_TRUE(cetcd_raft_entry_at(r, last - 1) == NULL);
    const cetcd_entry *dummy = cetcd_raft_entry_at(r, last);
    CETCD_ASSERT_NOT_NULL(dummy);
    CETCD_ASSERT_EQ_INT((int)dummy->data.len, 0);
    CETCD_ASSERT_TRUE(dummy->term == term);
    CETCD_ASSERT_EQ_INT(cetcd_raft_propose(r, (const uint8_t *)"c", 1), 0);
    CETCD_ASSERT_TRUE(cetcd_raft_last_index(r) == last + 1);
    CETCD_ASSERT_NOT_NULL(cetcd_raft_entry_at(r, last + 1));
    cetcd_raft_free(r);
}

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(new_raft_starts_as_follower),
    CETCD_TEST_ENTRY(single_node_becomes_leader_after_election_timeout),
    CETCD_TEST_ENTRY(term_increases_on_election),
    CETCD_TEST_ENTRY(leader_can_propose_entries),
    CETCD_TEST_ENTRY(single_node_propose_commits),
    CETCD_TEST_ENTRY(restore_entry_and_hard_state),
    CETCD_TEST_ENTRY(advance_clears_ready),
    CETCD_TEST_ENTRY(step_with_hup_triggers_election),
    CETCD_TEST_ENTRY(config_null_storage_ok),
    CETCD_TEST_ENTRY(free_null_is_safe),
    CETCD_TEST_ENTRY(learner_does_not_block_single_node_commit),
    CETCD_TEST_ENTRY(joint_add_voter_blocks_commit_until_ack),
    CETCD_TEST_ENTRY(joint_restore_and_leave),
    CETCD_TEST_ENTRY(compact_drops_prefix_keeps_dummy),
CETCD_TEST_LIST_END

CETCD_TEST_MAIN()
