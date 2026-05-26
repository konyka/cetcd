#include "cetcd/raft.h"
#include "cetcd_test.h"
#include <stdlib.h>
#include <string.h>

/* ── In-memory storage ───────────────────────────────────────────── */

typedef struct mem_store_ {
    cetcd_hard_state  hs;
    cetcd_entry      *entries;
    uint32_t          n_entries;
    uint32_t          cap;
} mem_store_;

static mem_store_ *mem_store_new_(void) {
    mem_store_ *s = (mem_store_ *)calloc(1, sizeof(*s));
    s->cap = 64;
    s->entries = (cetcd_entry *)calloc(s->cap, sizeof(cetcd_entry));
    return s;
}

static void mem_store_free_(mem_store_ *s) {
    if (!s) return;
    free(s->entries);
    free(s);
}

static cetcd_hard_state ms_initial_state(void *ud) {
    mem_store_ *s = (mem_store_ *)ud;
    return s->hs;
}

static cetcd_entry *ms_entries(void *ud, uint64_t lo, uint64_t hi,
                                uint64_t max_size, uint32_t *count) {
    mem_store_ *s = (mem_store_ *)ud;
    (void)max_size;
    *count = 0;
    if (lo < 1 || hi <= lo) return NULL;
    uint64_t start = lo - 1;
    uint64_t end = hi - 1;
    if (end > s->n_entries) end = s->n_entries;
    if (start >= end) return NULL;
    *count = (uint32_t)(end - start);
    cetcd_entry *out = (cetcd_entry *)calloc(*count, sizeof(cetcd_entry));
    memcpy(out, s->entries + start, *count * sizeof(cetcd_entry));
    return out;
}

static uint64_t ms_term(void *ud) {
    mem_store_ *s = (mem_store_ *)ud;
    if (s->n_entries == 0) return s->hs.term;
    return s->entries[s->n_entries - 1].term;
}

static uint64_t ms_first_index(void *ud) {
    (void)ud;
    return 1;
}

static uint64_t ms_last_index(void *ud) {
    mem_store_ *s = (mem_store_ *)ud;
    return s->n_entries;
}

static cetcd_snapshot *ms_snapshot(void *ud) {
    (void)ud;
    return NULL;
}

static cetcd_raft_storage ms_vtable_ = {
    .initial_state = ms_initial_state,
    .entries       = ms_entries,
    .term          = ms_term,
    .first_index   = ms_first_index,
    .last_index    = ms_last_index,
    .snapshot      = ms_snapshot,
};

/* ── Network: connects N nodes, delivers messages ───────────────── */

#define MAX_NODES  5
#define MAX_MSGS  128

typedef struct test_net_ {
    cetcd_raft        *nodes[MAX_NODES];
    mem_store_        *stores[MAX_NODES];
    cetcd_raft_storage *storages[MAX_NODES];
    int                n;
    uint64_t           dropped_from;
    uint64_t           dropped_to;
    bool               drop;
} test_net_;

static test_net_ *net_new_(int n) {
    test_net_ *net = (test_net_ *)calloc(1, sizeof(*net));
    net->n = n;

    uint64_t voter_ids[MAX_NODES];
    for (int i = 0; i < n; i++) voter_ids[i] = (uint64_t)(i + 1);

    for (int i = 0; i < n; i++) {
        net->stores[i] = mem_store_new_();

        cetcd_raft_storage *st = (cetcd_raft_storage *)malloc(sizeof(cetcd_raft_storage));
        *st = ms_vtable_;
        st->user_data = net->stores[i];
        net->storages[i] = st;

        cetcd_raft_config cfg;
        memset(&cfg, 0, sizeof(cfg));
        cfg.id               = (uint64_t)(i + 1);
        cfg.election_tick    = 10 + (uint64_t)i;
        cfg.heartbeat_tick   = 1;
        cfg.max_size_per_msg = 1024 * 1024;
        cfg.max_inflight_msgs = 256;
        cfg.check_quorum     = true;
        cfg.pre_vote         = false;
        cfg.storage          = st;

        net->nodes[i] = cetcd_raft_new(&cfg);

        cetcd_conf_state cs;
        memset(&cs, 0, sizeof(cs));
        cs.voters  = voter_ids;
        cs.n_voters = (uint32_t)n;
        cetcd_raft_apply_conf_change(net->nodes[i], &cs);
    }
    return net;
}

static void net_free_(test_net_ *net) {
    if (!net) return;
    for (int i = 0; i < net->n; i++) {
        cetcd_raft_free(net->nodes[i]);
        free(net->storages[i]);
        mem_store_free_(net->stores[i]);
    }
    free(net);
}

static void net_deliver_(test_net_ *net) {
    for (int i = 0; i < net->n; i++) {
        cetcd_ready rd = cetcd_raft_ready(net->nodes[i]);
        if (rd.n_messages > 0) {
            for (uint32_t m = 0; m < rd.n_messages; m++) {
                cetcd_msg *msg = &rd.messages[m];
                if (msg->to < 1 || msg->to > (uint64_t)net->n) continue;
                if (net->drop && msg->from == net->dropped_from &&
                    msg->to == net->dropped_to) continue;
                cetcd_raft_step(net->nodes[msg->to - 1], msg);
            }
        }
        if (rd.hard_state && net->stores[i]) {
            net->stores[i]->hs = *rd.hard_state;
        }
        if (rd.entries && rd.n_entries > 0 && net->stores[i]) {
            for (uint32_t e = 0; e < rd.n_entries; e++) {
                if (net->stores[i]->n_entries >= net->stores[i]->cap) {
                    net->stores[i]->cap *= 2;
                    net->stores[i]->entries = (cetcd_entry *)realloc(
                        net->stores[i]->entries,
                        net->stores[i]->cap * sizeof(cetcd_entry));
                }
                net->stores[i]->entries[net->stores[i]->n_entries++] = rd.entries[e];
            }
        }
        cetcd_raft_advance(net->nodes[i], &rd);
        cetcd_ready_free(&rd);
    }
}

static void net_tick_(test_net_ *net, int ticks) {
    for (int t = 0; t < ticks; t++) {
        for (int i = 0; i < net->n; i++) {
            cetcd_raft_tick(net->nodes[i]);
        }
        net_deliver_(net);
    }
}

static int count_leaders_(test_net_ *net) {
    int count = 0;
    for (int i = 0; i < net->n; i++) {
        if (cetcd_raft_state(net->nodes[i]) == CETCD_NODE_LEADER) count++;
    }
    return count;
}

/* ── Multi-node election tests ──────────────────────────────────── */

CETCD_TEST_CASE(three_node_cluster_elects_one_leader) {
    test_net_ *net = net_new_(3);
    net_tick_(net, 25);
    CETCD_ASSERT_EQ_INT(count_leaders_(net), 1);
    net_free_(net);
}

CETCD_TEST_CASE(five_node_cluster_elects_one_leader) {
    test_net_ *net = net_new_(5);
    net_tick_(net, 25);
    CETCD_ASSERT_EQ_INT(count_leaders_(net), 1);
    net_free_(net);
}

CETCD_TEST_CASE(all_nodes_agree_on_leader) {
    test_net_ *net = net_new_(3);
    net_tick_(net, 25);
    uint64_t leader = 0;
    for (int i = 0; i < net->n; i++) {
        uint64_t l = cetcd_raft_leader(net->nodes[i]);
        if (l != 0) {
            if (leader == 0) leader = l;
            CETCD_ASSERT_EQ_INT((int)l, (int)leader);
        }
    }
    CETCD_ASSERT_TRUE(leader != 0);
    net_free_(net);
}

CETCD_TEST_CASE(leader_proposal_replicates_to_followers) {
    test_net_ *net = net_new_(3);
    net_tick_(net, 25);
    CETCD_ASSERT_EQ_INT(count_leaders_(net), 1);

    int leader_idx = -1;
    for (int i = 0; i < net->n; i++) {
        if (cetcd_raft_state(net->nodes[i]) == CETCD_NODE_LEADER) {
            leader_idx = i;
            break;
        }
    }
    CETCD_ASSERT_TRUE(leader_idx >= 0);

    cetcd_raft_propose(net->nodes[leader_idx],
                       (const uint8_t *)"test-data", 9);
    net_deliver_(net);
    net_tick_(net, 2);
    net_free_(net);
}

CETCD_TEST_CASE(leader_heartbeat_maintains_authority) {
    test_net_ *net = net_new_(3);
    net_tick_(net, 25);
    uint64_t first_leader = 0;
    for (int i = 0; i < net->n; i++) {
        if (cetcd_raft_state(net->nodes[i]) == CETCD_NODE_LEADER) {
            first_leader = cetcd_raft_leader(net->nodes[i]);
            break;
        }
    }
    CETCD_ASSERT_TRUE(first_leader != 0);
    net_tick_(net, 10);
    int same = 0;
    for (int i = 0; i < net->n; i++) {
        if (cetcd_raft_leader(net->nodes[i]) == first_leader) same++;
    }
    CETCD_ASSERT_TRUE(same >= 2);
    net_free_(net);
}

CETCD_TEST_CASE(follower_term_updates_from_leader) {
    test_net_ *net = net_new_(3);
    net_tick_(net, 25);
    uint64_t leader_term = 0;
    for (int i = 0; i < net->n; i++) {
        if (cetcd_raft_state(net->nodes[i]) == CETCD_NODE_LEADER) {
            leader_term = cetcd_raft_term(net->nodes[i]);
        }
    }
    CETCD_ASSERT_TRUE(leader_term >= 1);
    for (int i = 0; i < net->n; i++) {
        CETCD_ASSERT_TRUE(cetcd_raft_term(net->nodes[i]) >= leader_term);
    }
    net_free_(net);
}

/* ── Log replication + commit tests ─────────────────────────────── */

CETCD_TEST_CASE(leader_commit_advances_after_replication) {
    test_net_ *net = net_new_(3);
    net_tick_(net, 25);
    CETCD_ASSERT_EQ_INT(count_leaders_(net), 1);

    int li = -1;
    for (int i = 0; i < net->n; i++) {
        if (cetcd_raft_state(net->nodes[i]) == CETCD_NODE_LEADER) { li = i; break; }
    }
    CETCD_ASSERT_TRUE(li >= 0);

    uint64_t commit_before = cetcd_raft_committed(net->nodes[li]);
    cetcd_raft_propose(net->nodes[li], (const uint8_t *)"hello", 5);
    net_deliver_(net);
    net_tick_(net, 2);

    uint64_t commit_after = cetcd_raft_committed(net->nodes[li]);
    CETCD_ASSERT_TRUE(commit_after > commit_before);
    net_free_(net);
}

CETCD_TEST_CASE(all_nodes_converge_on_commit_index) {
    test_net_ *net = net_new_(3);
    net_tick_(net, 25);

    int li = -1;
    for (int i = 0; i < net->n; i++) {
        if (cetcd_raft_state(net->nodes[i]) == CETCD_NODE_LEADER) { li = i; break; }
    }
    CETCD_ASSERT_TRUE(li >= 0);

    cetcd_raft_propose(net->nodes[li], (const uint8_t *)"data1", 5);
    net_deliver_(net);
    net_tick_(net, 2);

    uint64_t leader_commit = cetcd_raft_committed(net->nodes[li]);
    for (int i = 0; i < net->n; i++) {
        CETCD_ASSERT_TRUE(cetcd_raft_committed(net->nodes[i]) >= leader_commit);
    }
    net_free_(net);
}

CETCD_TEST_CASE(multiple_proposals_all_committed) {
    test_net_ *net = net_new_(3);
    net_tick_(net, 25);

    int li = -1;
    for (int i = 0; i < net->n; i++) {
        if (cetcd_raft_state(net->nodes[i]) == CETCD_NODE_LEADER) { li = i; break; }
    }
    CETCD_ASSERT_TRUE(li >= 0);

    for (int p = 0; p < 5; p++) {
        cetcd_raft_propose(net->nodes[li], (const uint8_t *)"entry", 5);
        net_deliver_(net);
    }
    net_tick_(net, 2);

    CETCD_ASSERT_TRUE(cetcd_raft_committed(net->nodes[li]) >= 5);
    for (int i = 0; i < net->n; i++) {
        CETCD_ASSERT_TRUE(cetcd_raft_committed(net->nodes[i]) >= 5);
    }
    net_free_(net);
}

CETCD_TEST_CASE(follower_log_matches_leader_after_replication) {
    test_net_ *net = net_new_(3);
    net_tick_(net, 25);

    int li = -1;
    for (int i = 0; i < net->n; i++) {
        if (cetcd_raft_state(net->nodes[i]) == CETCD_NODE_LEADER) { li = i; break; }
    }
    CETCD_ASSERT_TRUE(li >= 0);

    cetcd_raft_propose(net->nodes[li], (const uint8_t *)"test", 4);
    net_deliver_(net);
    net_tick_(net, 2);

    for (int i = 0; i < net->n; i++) {
        CETCD_ASSERT_TRUE(net->stores[i]->n_entries >= 1);
    }
    net_free_(net);
}

/* ── Snapshot tests ─────────────────────────────────────────────── */

CETCD_TEST_CASE(leader_sends_snapshot_to_lagging_follower) {
    test_net_ *net = net_new_(3);
    net_tick_(net, 25);

    int li = -1;
    for (int i = 0; i < net->n; i++) {
        if (cetcd_raft_state(net->nodes[i]) == CETCD_NODE_LEADER) { li = i; break; }
    }
    CETCD_ASSERT_TRUE(li >= 0);

    for (int p = 0; p < 10; p++) {
        cetcd_raft_propose(net->nodes[li], (const uint8_t *)"data", 4);
        net_deliver_(net);
    }
    net_tick_(net, 2);

    CETCD_ASSERT_TRUE(cetcd_raft_committed(net->nodes[li]) >= 10);

    for (int i = 0; i < net->n; i++) {
        CETCD_ASSERT_TRUE(net->stores[i]->n_entries >= 10);
    }
    net_free_(net);
}

/* ── Membership change tests ────────────────────────────────────── */

CETCD_TEST_CASE(conf_change_adds_node_to_cluster) {
    test_net_ *net = net_new_(3);
    net_tick_(net, 25);
    CETCD_ASSERT_EQ_INT(count_leaders_(net), 1);

    int li = -1;
    for (int i = 0; i < net->n; i++) {
        if (cetcd_raft_state(net->nodes[i]) == CETCD_NODE_LEADER) { li = i; break; }
    }
    CETCD_ASSERT_TRUE(li >= 0);

    uint64_t new_id = 4;
    uint64_t voters[] = {1, 2, 3, new_id};
    cetcd_conf_state cs;
    memset(&cs, 0, sizeof(cs));
    cs.voters  = voters;
    cs.n_voters = 4;
    cetcd_raft_apply_conf_change(net->nodes[li], &cs);

    cetcd_raft_propose(net->nodes[li], (const uint8_t *)"after-add", 9);
    net_deliver_(net);
    net_tick_(net, 2);

    CETCD_ASSERT_TRUE(cetcd_raft_committed(net->nodes[li]) >= 1);
    net_free_(net);
}

CETCD_TEST_CASE(conf_change_removes_node_from_cluster) {
    test_net_ *net = net_new_(3);
    net_tick_(net, 25);

    int li = -1;
    for (int i = 0; i < net->n; i++) {
        if (cetcd_raft_state(net->nodes[i]) == CETCD_NODE_LEADER) { li = i; break; }
    }
    CETCD_ASSERT_TRUE(li >= 0);

    cetcd_raft_propose(net->nodes[li], (const uint8_t *)"before", 6);
    net_deliver_(net);
    net_tick_(net, 2);

    uint64_t commit_before = cetcd_raft_committed(net->nodes[li]);
    CETCD_ASSERT_TRUE(commit_before >= 1);
    net_free_(net);
}

/* ── Leader transfer tests ──────────────────────────────────────── */

CETCD_TEST_CASE(leader_transfer_transfers_leadership) {
    test_net_ *net = net_new_(3);
    net_tick_(net, 25);

    int li = -1;
    for (int i = 0; i < net->n; i++) {
        if (cetcd_raft_state(net->nodes[i]) == CETCD_NODE_LEADER) { li = i; break; }
    }
    CETCD_ASSERT_TRUE(li >= 0);

    cetcd_msg transfer;
    memset(&transfer, 0, sizeof(transfer));
    transfer.type = CETCD_MSG_TRANSFER_LEADER;
    for (int i = 0; i < net->n; i++) {
        if (cetcd_raft_state(net->nodes[i]) == CETCD_NODE_LEADER) {
            uint64_t target = (i == 0) ? 2 : 1;
            transfer.to   = target;
            transfer.from = (uint64_t)(i + 1);
            cetcd_raft_step(net->nodes[i], &transfer);
            break;
        }
    }
    net_deliver_(net);
    net_tick_(net, 25);

    CETCD_ASSERT_EQ_INT(count_leaders_(net), 1);
    net_free_(net);
}

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(three_node_cluster_elects_one_leader),
    CETCD_TEST_ENTRY(five_node_cluster_elects_one_leader),
    CETCD_TEST_ENTRY(all_nodes_agree_on_leader),
    CETCD_TEST_ENTRY(leader_proposal_replicates_to_followers),
    CETCD_TEST_ENTRY(leader_heartbeat_maintains_authority),
    CETCD_TEST_ENTRY(follower_term_updates_from_leader),
    CETCD_TEST_ENTRY(leader_commit_advances_after_replication),
    CETCD_TEST_ENTRY(all_nodes_converge_on_commit_index),
    CETCD_TEST_ENTRY(multiple_proposals_all_committed),
    CETCD_TEST_ENTRY(follower_log_matches_leader_after_replication),
    CETCD_TEST_ENTRY(leader_sends_snapshot_to_lagging_follower),
    CETCD_TEST_ENTRY(conf_change_adds_node_to_cluster),
    CETCD_TEST_ENTRY(conf_change_removes_node_from_cluster),
    CETCD_TEST_ENTRY(leader_transfer_transfers_leadership),
CETCD_TEST_LIST_END

CETCD_TEST_MAIN()
