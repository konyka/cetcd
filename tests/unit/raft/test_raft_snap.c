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

CETCD_TEST_CASE(joiner_catches_up_without_snapshot) {
    cetcd_raft *ldr = raft_new_(1);
    campaign_(ldr);
    CETCD_ASSERT_EQ_INT(cetcd_raft_propose(ldr, (const uint8_t *)"a", 1), 0);
    CETCD_ASSERT_EQ_INT(cetcd_raft_propose(ldr, (const uint8_t *)"b", 1), 0);
    CETCD_ASSERT_EQ_INT(cetcd_raft_propose(ldr, (const uint8_t *)"c", 1), 0);
    drain_(ldr);
    CETCD_ASSERT_EQ_INT((int)cetcd_raft_last_index(ldr), 3);
    CETCD_ASSERT_EQ_INT((int)cetcd_raft_compacted(ldr), 0);
    CETCD_ASSERT_EQ_INT(cetcd_raft_add_peer(ldr, 2, 0), 0);

    cetcd_msg beat;
    memset(&beat, 0, sizeof(beat));
    beat.type = CETCD_MSG_BEAT;
    beat.from = 1;
    cetcd_raft_step(ldr, &beat);

    cetcd_ready rd = cetcd_raft_ready(ldr);
    cetcd_msg *app = NULL;
    for (uint32_t i = 0; i < rd.n_messages; i++) {
        if (rd.messages[i].type == CETCD_MSG_APP && rd.messages[i].to == 2)
            app = &rd.messages[i];
    }
    CETCD_ASSERT_NOT_NULL(app);
    CETCD_ASSERT_EQ_INT((int)app->index, 0);
    CETCD_ASSERT_EQ_INT((int)app->n_entries, 3);
    CETCD_ASSERT_EQ_INT((int)app->entries[0].index, 1);
    CETCD_ASSERT_EQ_INT((int)app->entries[2].index, 3);

    cetcd_raft *fol = raft_new_(2);
    CETCD_ASSERT_EQ_INT(cetcd_raft_step(fol, app), 0);
    CETCD_ASSERT_EQ_INT((int)cetcd_raft_last_index(fol), 3);

    cetcd_ready frd = cetcd_raft_ready(fol);
    int ack = 0;
    uint64_t ack_idx = 0;
    for (uint32_t i = 0; i < frd.n_messages; i++) {
        if (frd.messages[i].type == CETCD_MSG_APP_RESP &&
            frd.messages[i].reject == 0) {
            ack = 1;
            ack_idx = frd.messages[i].index;
        }
    }
    CETCD_ASSERT_TRUE(ack);
    CETCD_ASSERT_EQ_INT((int)ack_idx, 3);
    for (uint32_t i = 0; i < frd.n_messages; i++) {
        if (frd.messages[i].type == CETCD_MSG_APP_RESP)
            cetcd_raft_step(ldr, &frd.messages[i]);
    }
    cetcd_ready_free(&frd);
    cetcd_ready_free(&rd);
    drain_(ldr);

    cetcd_raft_step(ldr, &beat);
    rd = cetcd_raft_ready(ldr);
    int app_again = 0;
    for (uint32_t i = 0; i < rd.n_messages; i++) {
        if (rd.messages[i].type == CETCD_MSG_APP) app_again++;
    }
    CETCD_ASSERT_EQ_INT(app_again, 0);
    cetcd_ready_free(&rd);
    cetcd_raft_free(fol);
    cetcd_raft_free(ldr);
}

CETCD_TEST_CASE(app_reject_hint_resets_next_idx) {
    cetcd_raft *r = raft_new_(1);
    campaign_(r);
    CETCD_ASSERT_EQ_INT(cetcd_raft_propose(r, (const uint8_t *)"a", 1), 0);
    CETCD_ASSERT_EQ_INT(cetcd_raft_propose(r, (const uint8_t *)"b", 1), 0);
    CETCD_ASSERT_EQ_INT(cetcd_raft_propose(r, (const uint8_t *)"c", 1), 0);
    drain_(r);
    CETCD_ASSERT_EQ_INT(cetcd_raft_add_peer(r, 2, 0), 0);

    /* Stale accept: pretend the joiner already has index 3. */
    cetcd_msg ok;
    memset(&ok, 0, sizeof(ok));
    ok.type = CETCD_MSG_APP_RESP;
    ok.to = 1;
    ok.from = 2;
    ok.term = cetcd_raft_term(r);
    ok.index = 3;
    ok.reject = 0;
    CETCD_ASSERT_EQ_INT(cetcd_raft_step(r, &ok), 0);
    drain_(r);

    /* Empty joiner rejects and hints last index 0. */
    cetcd_msg rej;
    memset(&rej, 0, sizeof(rej));
    rej.type = CETCD_MSG_APP_RESP;
    rej.to = 1;
    rej.from = 2;
    rej.term = cetcd_raft_term(r);
    rej.index = 0;
    rej.reject = 1;
    CETCD_ASSERT_EQ_INT(cetcd_raft_step(r, &rej), 0);

    cetcd_ready rd = cetcd_raft_ready(r);
    int found = 0;
    for (uint32_t i = 0; i < rd.n_messages; i++) {
        if (rd.messages[i].type == CETCD_MSG_APP &&
            rd.messages[i].to == 2 &&
            rd.messages[i].index == 0 &&
            rd.messages[i].n_entries == 3)
            found = 1;
    }
    CETCD_ASSERT_TRUE(found);
    cetcd_ready_free(&rd);
    cetcd_raft_free(r);
}

CETCD_TEST_CASE(app_batch_respects_max_size_per_msg) {
    cetcd_raft_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.id = 1;
    cfg.election_tick = 10;
    cfg.heartbeat_tick = 1;
    cfg.max_size_per_msg = 8;
    cfg.max_inflight_msgs = 256;
    cetcd_raft *r = cetcd_raft_new(&cfg);
    campaign_(r);
    const uint8_t payload[] = "xxxxxxxx";
    CETCD_ASSERT_EQ_INT(cetcd_raft_propose(r, payload, 8), 0);
    CETCD_ASSERT_EQ_INT(cetcd_raft_propose(r, payload, 8), 0);
    CETCD_ASSERT_EQ_INT(cetcd_raft_propose(r, payload, 8), 0);
    drain_(r);
    CETCD_ASSERT_EQ_INT(cetcd_raft_add_peer(r, 2, 0), 0);

    cetcd_msg beat;
    memset(&beat, 0, sizeof(beat));
    beat.type = CETCD_MSG_BEAT;
    beat.from = 1;
    cetcd_raft_step(r, &beat);

    cetcd_ready rd = cetcd_raft_ready(r);
    int found = 0;
    for (uint32_t i = 0; i < rd.n_messages; i++) {
        if (rd.messages[i].type == CETCD_MSG_APP && rd.messages[i].to == 2) {
            CETCD_ASSERT_EQ_INT((int)rd.messages[i].n_entries, 1);
            CETCD_ASSERT_EQ_INT((int)rd.messages[i].index, 0);
            found = 1;
        }
    }
    CETCD_ASSERT_TRUE(found);
    cetcd_ready_free(&rd);
    cetcd_raft_free(r);
}

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(leader_queues_snap_after_compact_for_new_peer),
    CETCD_TEST_ENTRY(follower_installs_snap_and_acks),
    CETCD_TEST_ENTRY(snap_index_zero_is_rejected),
    CETCD_TEST_ENTRY(leader_advances_after_snap_status),
    CETCD_TEST_ENTRY(joiner_catches_up_without_snapshot),
    CETCD_TEST_ENTRY(app_reject_hint_resets_next_idx),
    CETCD_TEST_ENTRY(app_batch_respects_max_size_per_msg),
CETCD_TEST_LIST_END

CETCD_TEST_MAIN()
