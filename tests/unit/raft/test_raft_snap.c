#include "cetcd/raft.h"
#include "cetcd_test.h"

#include <stdlib.h>
#include <string.h>

static cetcd_raft *raft_new_(uint64_t id) {
    cetcd_raft_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.id = id;
    cfg.election_tick = 10;
    cfg.heartbeat_tick = 1;
    cfg.max_size_per_msg = 1024 * 1024;
    cfg.max_inflight_msgs = 256;
    return cetcd_raft_new(&cfg);
}

static void drain_(cetcd_raft *r) {
    cetcd_ready rd = cetcd_raft_ready(r);
    cetcd_ready_free(&rd);
}

static void campaign_(cetcd_raft *r) {
    cetcd_msg hup;
    memset(&hup, 0, sizeof(hup));
    hup.type = CETCD_MSG_HUP;
    hup.from = 1;
    cetcd_raft_step(r, &hup);
    drain_(r);
}

CETCD_TEST_CASE(leader_queues_snap_after_compact_for_new_peer) {
    cetcd_raft *r = raft_new_(1);
    campaign_(r);
    CETCD_ASSERT_EQ_INT(cetcd_raft_state(r), CETCD_NODE_LEADER);
    CETCD_ASSERT_EQ_INT(cetcd_raft_propose(r, (const uint8_t *)"a", 1), 0);
    drain_(r);
    CETCD_ASSERT_EQ_INT(cetcd_raft_compact(r, 1, cetcd_raft_term(r)), 0);
    CETCD_ASSERT_TRUE(cetcd_raft_compacted(r) >= 1);
    CETCD_ASSERT_EQ_INT(cetcd_raft_add_peer(r, 2, 0), 0);

    cetcd_msg beat;
    memset(&beat, 0, sizeof(beat));
    beat.type = CETCD_MSG_BEAT;
    beat.from = 1;
    cetcd_raft_step(r, &beat);

    cetcd_ready rd = cetcd_raft_ready(r);
    int found = 0;
    for (uint32_t i = 0; i < rd.n_messages; i++) {
        if (rd.messages[i].type == CETCD_MSG_SNAP &&
            rd.messages[i].to == 2 &&
            rd.messages[i].snapshot == cetcd_raft_compacted(r))
            found = 1;
    }
    CETCD_ASSERT_TRUE(found);
    cetcd_ready_free(&rd);

    cetcd_raft_step(r, &beat);
    rd = cetcd_raft_ready(r);
    int again = 0;
    for (uint32_t i = 0; i < rd.n_messages; i++) {
        if (rd.messages[i].type == CETCD_MSG_SNAP) again++;
    }
    CETCD_ASSERT_EQ_INT(again, 0);
    cetcd_ready_free(&rd);
    cetcd_raft_free(r);
}

CETCD_TEST_CASE(follower_installs_snap_and_acks) {
    cetcd_raft *f = raft_new_(2);
    cetcd_msg snap;
    memset(&snap, 0, sizeof(snap));
    snap.type = CETCD_MSG_SNAP;
    snap.to = 2;
    snap.from = 1;
    snap.term = 3;
    snap.snapshot = 5;
    snap.index = 5;
    snap.log_term = 3;
    snap.commit = 5;
    CETCD_ASSERT_EQ_INT(cetcd_raft_step(f, &snap), 0);
    CETCD_ASSERT_EQ_INT((int)cetcd_raft_compacted(f), 5);
    CETCD_ASSERT_TRUE(cetcd_raft_last_index(f) >= 5);
    CETCD_ASSERT_TRUE(cetcd_raft_committed(f) >= 5);

    cetcd_ready rd = cetcd_raft_ready(f);
    int ack = 0;
    for (uint32_t i = 0; i < rd.n_messages; i++) {
        if (rd.messages[i].type == CETCD_MSG_SNAP_STATUS &&
            rd.messages[i].to == 1 &&
            rd.messages[i].reject == 0 &&
            rd.messages[i].snapshot == 5)
            ack = 1;
    }
    CETCD_ASSERT_TRUE(ack);
    cetcd_ready_free(&rd);
    cetcd_raft_free(f);
}

CETCD_TEST_CASE(snap_index_zero_is_rejected) {
    cetcd_raft *f = raft_new_(2);
    cetcd_msg snap;
    memset(&snap, 0, sizeof(snap));
    snap.type = CETCD_MSG_SNAP;
    snap.to = 2;
    snap.from = 1;
    snap.term = 1;
    snap.snapshot = 0;
    CETCD_ASSERT_EQ_INT(cetcd_raft_step(f, &snap), 0);
    CETCD_ASSERT_EQ_INT((int)cetcd_raft_compacted(f), 0);

    cetcd_ready rd = cetcd_raft_ready(f);
    int rej = 0;
    for (uint32_t i = 0; i < rd.n_messages; i++) {
        if (rd.messages[i].type == CETCD_MSG_SNAP_STATUS &&
            rd.messages[i].reject != 0)
            rej = 1;
    }
    CETCD_ASSERT_TRUE(rej);
    cetcd_ready_free(&rd);
    cetcd_raft_free(f);
}

CETCD_TEST_CASE(leader_advances_after_snap_status) {
    cetcd_raft *r = raft_new_(1);
    campaign_(r);
    CETCD_ASSERT_EQ_INT(cetcd_raft_propose(r, (const uint8_t *)"a", 1), 0);
    drain_(r);
    CETCD_ASSERT_EQ_INT(cetcd_raft_compact(r, 1, cetcd_raft_term(r)), 0);
    CETCD_ASSERT_EQ_INT(cetcd_raft_add_peer(r, 2, 0), 0);
    cetcd_msg beat;
    memset(&beat, 0, sizeof(beat));
    beat.type = CETCD_MSG_BEAT;
    beat.from = 1;
    cetcd_raft_step(r, &beat);
    drain_(r);

    cetcd_msg st;
    memset(&st, 0, sizeof(st));
    st.type = CETCD_MSG_SNAP_STATUS;
    st.to = 1;
    st.from = 2;
    st.term = cetcd_raft_term(r);
    st.snapshot = 1;
    st.index = 1;
    st.reject = 0;
    CETCD_ASSERT_EQ_INT(cetcd_raft_step(r, &st), 0);

    cetcd_raft_step(r, &beat);
    cetcd_ready rd = cetcd_raft_ready(r);
    int snap_again = 0;
    for (uint32_t i = 0; i < rd.n_messages; i++) {
        if (rd.messages[i].type == CETCD_MSG_SNAP) snap_again++;
    }
    CETCD_ASSERT_EQ_INT(snap_again, 0);
    cetcd_ready_free(&rd);
    cetcd_raft_free(r);
}

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(leader_queues_snap_after_compact_for_new_peer),
    CETCD_TEST_ENTRY(follower_installs_snap_and_acks),
    CETCD_TEST_ENTRY(snap_index_zero_is_rejected),
    CETCD_TEST_ENTRY(leader_advances_after_snap_status),
CETCD_TEST_LIST_END

CETCD_TEST_MAIN()
