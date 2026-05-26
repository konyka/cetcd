#include "cetcd/raft.h"
#include <stdlib.h>
#include <string.h>

/* ── Internal constants ─────────────────────────────────────────── */

#define MAX_PEERS_  16

/* ── Peer progress (leader tracks each follower) ────────────────── */

typedef struct peer_progress_ {
    uint64_t next_idx;
    uint64_t match_idx;
} peer_progress_;

/* ── Internal state ─────────────────────────────────────────────── */

typedef enum raft_role_ {
    ROLE_FOLLOWER      = 0,
    ROLE_CANDIDATE     = 1,
    ROLE_LEADER        = 2,
    ROLE_PRE_CANDIDATE = 3,
} raft_role_;

struct cetcd_raft {
    uint64_t              id;
    uint64_t              term;
    uint64_t              vote;
    uint64_t              commit;
    uint64_t              applied;
    uint64_t              leader_id;
    raft_role_            role;

    uint64_t              election_tick;
    uint64_t              heartbeat_tick;
    uint64_t              election_timeout;
    uint64_t              heartbeat_timeout;
    uint64_t              elapsed_ticks;
    uint64_t              heartbeat_elapsed;

    uint64_t              max_size_per_msg;
    uint64_t              max_inflight_msgs;
    bool                  check_quorum;
    bool                  pre_vote;

    cetcd_raft_storage   *storage;

    /* Cluster membership */
    uint64_t             *peers;
    uint32_t              n_peers;
    uint32_t              peers_cap;

    /* Vote tracking (candidate) */
    uint64_t             *votes_granted;
    uint32_t              n_votes_granted;

    /* Leader-only: per-follower progress */
    peer_progress_        progress[MAX_PEERS_];

    /* Log */
    uint64_t              log_last_index;
    uint64_t              log_last_term;

    /* Pending Ready state */
    cetcd_hard_state     *pending_hs;
    cetcd_entry          *pending_entries;
    uint32_t              n_pending_entries;
    uint32_t              cap_pending_entries;
    cetcd_msg            *pending_msgs;
    uint32_t              n_pending_msgs;
    uint32_t              cap_pending_msgs;
    bool                  has_pending;
};

/* ── Helpers ────────────────────────────────────────────────────── */

static uint32_t quorum_(uint32_t n) {
    return n / 2 + 1;
}

static bool has_peer_(cetcd_raft *r, uint64_t id) {
    for (uint32_t i = 0; i < r->n_peers; i++) {
        if (r->peers[i] == id) return true;
    }
    return false;
}

static void add_peer_(cetcd_raft *r, uint64_t id) {
    if (has_peer_(r, id)) return;
    if (r->n_peers >= r->peers_cap) {
        r->peers_cap = r->peers_cap ? r->peers_cap * 2 : 8;
        r->peers = (uint64_t *)realloc(r->peers, r->peers_cap * sizeof(uint64_t));
        r->votes_granted = (uint64_t *)realloc(r->votes_granted,
                                                 r->peers_cap * sizeof(uint64_t));
    }
    r->peers[r->n_peers++] = id;
}

static uint32_t cluster_size_(cetcd_raft *r) {
    return r->n_peers;
}

/* ── Lifecycle ──────────────────────────────────────────────────── */

cetcd_raft *cetcd_raft_new(cetcd_raft_config *cfg) {
    if (!cfg || !cfg->storage) return NULL;
    if (cfg->id == 0) return NULL;
    if (cfg->election_tick == 0 || cfg->heartbeat_tick == 0) return NULL;

    cetcd_raft *r = (cetcd_raft *)calloc(1, sizeof(*r));
    if (!r) return NULL;

    r->id                = cfg->id;
    r->term              = 0;
    r->vote              = 0;
    r->commit            = 0;
    r->applied           = 0;
    r->leader_id         = 0;
    r->role              = ROLE_FOLLOWER;

    r->election_tick     = cfg->election_tick;
    r->heartbeat_tick    = cfg->heartbeat_tick;
    r->election_timeout  = cfg->election_tick;
    r->heartbeat_timeout = cfg->heartbeat_tick;
    r->elapsed_ticks     = 0;
    r->heartbeat_elapsed = 0;

    r->max_size_per_msg  = cfg->max_size_per_msg;
    r->max_inflight_msgs = cfg->max_inflight_msgs;
    r->check_quorum      = cfg->check_quorum;
    r->pre_vote          = cfg->pre_vote;
    r->storage           = cfg->storage;

    /* Self is always a peer */
    add_peer_(r, cfg->id);

    r->cap_pending_entries = 16;
    r->pending_entries = (cetcd_entry *)calloc(r->cap_pending_entries, sizeof(cetcd_entry));
    r->cap_pending_msgs = 32;
    r->pending_msgs = (cetcd_msg *)calloc(r->cap_pending_msgs, sizeof(cetcd_msg));

    return r;
}

void cetcd_raft_free(cetcd_raft *r) {
    if (!r) return;
    free(r->pending_hs);
    free(r->pending_entries);
    free(r->pending_msgs);
    free(r->peers);
    free(r->votes_granted);
    free(r);
}

/* ── Pending state helpers ──────────────────────────────────────── */

static void queue_hard_state_(cetcd_raft *r) {
    if (!r->pending_hs) {
        r->pending_hs = (cetcd_hard_state *)malloc(sizeof(cetcd_hard_state));
    }
    if (r->pending_hs) {
        r->pending_hs->term   = r->term;
        r->pending_hs->vote   = r->vote;
        r->pending_hs->commit = r->commit;
    }
    r->has_pending = true;
}

static void queue_entry_(cetcd_raft *r, const cetcd_entry *e) {
    if (r->n_pending_entries >= r->cap_pending_entries) {
        r->cap_pending_entries *= 2;
        r->pending_entries = (cetcd_entry *)realloc(r->pending_entries,
            r->cap_pending_entries * sizeof(cetcd_entry));
    }
    r->pending_entries[r->n_pending_entries++] = *e;
    r->has_pending = true;
}

static void queue_msg_(cetcd_raft *r, const cetcd_msg *m) {
    if (r->n_pending_msgs >= r->cap_pending_msgs) {
        r->cap_pending_msgs *= 2;
        r->pending_msgs = (cetcd_msg *)realloc(r->pending_msgs,
            r->cap_pending_msgs * sizeof(cetcd_msg));
    }
    r->pending_msgs[r->n_pending_msgs++] = *m;
    r->has_pending = true;
}

/* ── Role transitions ──────────────────────────────────────────── */

static void become_leader_(cetcd_raft *r) {
    r->role      = ROLE_LEADER;
    r->leader_id = r->id;
    r->elapsed_ticks = 0;
    r->heartbeat_elapsed = 0;

    /* Initialize peer progress */
    for (uint32_t i = 0; i < r->n_peers && i < MAX_PEERS_; i++) {
        r->progress[i].next_idx  = r->log_last_index + 1;
        r->progress[i].match_idx = 0;
    }
}

static void become_candidate_(cetcd_raft *r) {
    r->role      = ROLE_CANDIDATE;
    r->leader_id = 0;
    r->term++;
    r->vote     = r->id;
    r->elapsed_ticks = 0;

    /* Reset votes — we vote for ourselves */
    r->n_votes_granted = 0;
    r->votes_granted[r->n_votes_granted++] = r->id;

    /* If we're the only node, become leader immediately */
    if (cluster_size_(r) == 1) {
        become_leader_(r);
        return;
    }

    /* Send vote requests to all other peers */
    for (uint32_t i = 0; i < r->n_peers; i++) {
        if (r->peers[i] == r->id) continue;
        cetcd_msg msg;
        memset(&msg, 0, sizeof(msg));
        msg.type     = CETCD_MSG_VOTE;
        msg.to       = r->peers[i];
        msg.from     = r->id;
        msg.term     = r->term;
        msg.log_term = r->log_last_term;
        msg.index    = r->log_last_index;
        queue_msg_(r, &msg);
    }
}

static void become_follower_(cetcd_raft *r, uint64_t term, uint64_t leader) {
    r->role      = ROLE_FOLLOWER;
    r->leader_id = leader;
    if (term > r->term) {
        r->term = term;
        r->vote = 0;
    }
    r->elapsed_ticks = 0;
}

/* ── Message handling ──────────────────────────────────────────── */

static int handle_vote_(cetcd_raft *r, cetcd_msg *msg) {
    /* Reject if term is stale */
    if (msg->term < r->term) {
        cetcd_msg resp;
        memset(&resp, 0, sizeof(resp));
        resp.type   = CETCD_MSG_VOTE_RESP;
        resp.to     = msg->from;
        resp.from   = r->id;
        resp.term   = r->term;
        resp.reject = 1;
        queue_msg_(r, &resp);
        return 0;
    }

    /* Step down if we see a higher term */
    if (msg->term > r->term) {
        become_follower_(r, msg->term, 0);
        queue_hard_state_(r);
    }

    /* Grant vote if:
     *   - We haven't voted this term, or we already voted for this candidate
     *   - Candidate's log is at least as up-to-date as ours */
    bool can_vote = (r->vote == 0 || r->vote == msg->from);
    bool log_ok = (msg->log_term > r->log_last_term) ||
                  (msg->log_term == r->log_last_term &&
                   msg->index >= r->log_last_index);

    cetcd_msg resp;
    memset(&resp, 0, sizeof(resp));
    resp.type   = CETCD_MSG_VOTE_RESP;
    resp.to     = msg->from;
    resp.from   = r->id;
    resp.term   = r->term;

    if (can_vote && log_ok) {
        r->vote = msg->from;
        r->elapsed_ticks = 0;
        queue_hard_state_(r);
        resp.reject = 0;
    } else {
        resp.reject = 1;
    }
    queue_msg_(r, &resp);
    return 0;
}

static int handle_vote_resp_(cetcd_raft *r, cetcd_msg *msg) {
    if (msg->term > r->term) {
        become_follower_(r, msg->term, 0);
        queue_hard_state_(r);
        return 0;
    }

    if (r->role != ROLE_CANDIDATE) return 0;

    if (!msg->reject) {
        r->votes_granted[r->n_votes_granted++] = msg->from;
        if (r->n_votes_granted >= quorum_(cluster_size_(r))) {
            become_leader_(r);
            queue_hard_state_(r);
            /* Send initial empty AppendEntries (heartbeat) */
            for (uint32_t i = 0; i < r->n_peers; i++) {
                if (r->peers[i] == r->id) continue;
                cetcd_msg hb;
                memset(&hb, 0, sizeof(hb));
                hb.type   = CETCD_MSG_HEARTBEAT;
                hb.to     = r->peers[i];
                hb.from   = r->id;
                hb.term   = r->term;
                hb.commit = r->commit;
                queue_msg_(r, &hb);
            }
        }
    }
    return 0;
}

static int handle_app_(cetcd_raft *r, cetcd_msg *msg) {
    /* Reject stale terms */
    if (msg->term < r->term) {
        cetcd_msg resp;
        memset(&resp, 0, sizeof(resp));
        resp.type   = CETCD_MSG_APP_RESP;
        resp.to     = msg->from;
        resp.from   = r->id;
        resp.term   = r->term;
        resp.reject = 1;
        queue_msg_(r, &resp);
        return 0;
    }

    /* Step down if higher term */
    if (msg->term > r->term) {
        become_follower_(r, msg->term, msg->from);
        queue_hard_state_(r);
    }

    /* Update leader */
    if (r->role != ROLE_FOLLOWER) {
        become_follower_(r, msg->term, msg->from);
    }
    r->leader_id = msg->from;
    r->elapsed_ticks = 0;

    /* Update commit */
    if (msg->commit > r->commit) {
        r->commit = msg->commit;
        queue_hard_state_(r);
    }

    /* Append entries from the message */
    for (uint32_t i = 0; i < msg->n_entries; i++) {
        r->log_last_index++;
        r->log_last_term = msg->entries[i].term;
        queue_entry_(r, &msg->entries[i]);
    }

    /* Respond with success */
    cetcd_msg resp;
    memset(&resp, 0, sizeof(resp));
    resp.type   = CETCD_MSG_APP_RESP;
    resp.to     = msg->from;
    resp.from   = r->id;
    resp.term   = r->term;
    resp.index  = r->log_last_index;
    resp.reject = 0;
    queue_msg_(r, &resp);
    return 0;
}

static int handle_app_resp_(cetcd_raft *r, cetcd_msg *msg) {
    if (r->role != ROLE_LEADER) return 0;

    if (msg->term > r->term) {
        become_follower_(r, msg->term, 0);
        queue_hard_state_(r);
        return 0;
    }

    /* Find the peer's progress slot */
    int slot = -1;
    for (uint32_t i = 0; i < r->n_peers && i < MAX_PEERS_; i++) {
        if (r->peers[i] == msg->from) { slot = (int)i; break; }
    }
    if (slot < 0) return 0;

    if (!msg->reject) {
        r->progress[slot].match_idx = msg->index;
        r->progress[slot].next_idx  = msg->index + 1;
    } else {
        if (r->progress[slot].next_idx > 1) {
            r->progress[slot].next_idx--;
        }
    }
    return 0;
}

static int handle_heartbeat_(cetcd_raft *r, cetcd_msg *msg) {
    if (msg->term < r->term) return 0;

    if (msg->term > r->term) {
        become_follower_(r, msg->term, msg->from);
        queue_hard_state_(r);
    }
    if (r->role != ROLE_FOLLOWER) {
        become_follower_(r, msg->term, msg->from);
    }
    r->leader_id = msg->from;
    r->elapsed_ticks = 0;

    if (msg->commit > r->commit) {
        r->commit = msg->commit;
        queue_hard_state_(r);
    }

    cetcd_msg resp;
    memset(&resp, 0, sizeof(resp));
    resp.type   = CETCD_MSG_HEARTBEAT_RESP;
    resp.to     = msg->from;
    resp.from   = r->id;
    resp.term   = r->term;
    queue_msg_(r, &resp);
    return 0;
}

/* ── Core API ───────────────────────────────────────────────────── */

int cetcd_raft_step(cetcd_raft *r, cetcd_msg *msg) {
    if (!r || !msg) return -1;

    switch (msg->type) {
    case CETCD_MSG_HUP:
        if (r->role != ROLE_LEADER) {
            become_candidate_(r);
            queue_hard_state_(r);
        }
        break;

    case CETCD_MSG_VOTE:
        return handle_vote_(r, msg);

    case CETCD_MSG_VOTE_RESP:
        return handle_vote_resp_(r, msg);

    case CETCD_MSG_APP:
        return handle_app_(r, msg);

    case CETCD_MSG_APP_RESP:
        return handle_app_resp_(r, msg);

    case CETCD_MSG_HEARTBEAT:
        return handle_heartbeat_(r, msg);

    case CETCD_MSG_PROP:
        if (r->role != ROLE_LEADER) return -1;
        {
            r->log_last_index++;
            cetcd_entry e;
            memset(&e, 0, sizeof(e));
            e.term  = r->term;
            e.index = r->log_last_index;
            e.type  = CETCD_ENTRY_NORMAL;
            e.data  = (cetcd_slice){.data = (uint8_t *)msg->context,
                                     .len  = msg->context_len};
            r->log_last_term = r->term;
            queue_entry_(r, &e);
            queue_hard_state_(r);

            for (uint32_t i = 0; i < r->n_peers; i++) {
                if (r->peers[i] == r->id) continue;
                if (i >= MAX_PEERS_) break;
                cetcd_msg app;
                memset(&app, 0, sizeof(app));
                app.type      = CETCD_MSG_APP;
                app.to        = r->peers[i];
                app.from      = r->id;
                app.term      = r->term;
                app.log_term  = r->log_last_term;
                app.index     = r->log_last_index;
                app.commit    = r->commit;
                cetcd_entry *ecopy = (cetcd_entry *)malloc(sizeof(cetcd_entry));
                if (ecopy) { *ecopy = e; }
                app.entries   = ecopy;
                app.n_entries = 1;
                queue_msg_(r, &app);
            }
        }
        break;

    case CETCD_MSG_BEAT:
        if (r->role != ROLE_LEADER) break;
        for (uint32_t i = 0; i < r->n_peers; i++) {
            if (r->peers[i] == r->id) continue;
            cetcd_msg hb;
            memset(&hb, 0, sizeof(hb));
            hb.type   = CETCD_MSG_HEARTBEAT;
            hb.to     = r->peers[i];
            hb.from   = r->id;
            hb.term   = r->term;
            hb.commit = r->commit;
            queue_msg_(r, &hb);
        }
        break;

    default:
        break;
    }
    return 0;
}

void cetcd_raft_tick(cetcd_raft *r) {
    if (!r) return;
    r->elapsed_ticks++;
    r->heartbeat_elapsed++;

    if (r->role == ROLE_LEADER) {
        if (r->heartbeat_elapsed >= r->heartbeat_timeout) {
            r->heartbeat_elapsed = 0;
            cetcd_msg beat;
            memset(&beat, 0, sizeof(beat));
            beat.type = CETCD_MSG_BEAT;
            beat.from = r->id;
            cetcd_raft_step(r, &beat);
        }
    } else {
        if (r->elapsed_ticks >= r->election_timeout) {
            cetcd_msg hup;
            memset(&hup, 0, sizeof(hup));
            hup.type = CETCD_MSG_HUP;
            hup.from = r->id;
            cetcd_raft_step(r, &hup);
        }
    }
}

int cetcd_raft_propose(cetcd_raft *r, const uint8_t *data, size_t len) {
    if (!r || r->role != ROLE_LEADER) return -1;
    cetcd_msg msg;
    memset(&msg, 0, sizeof(msg));
    msg.type        = CETCD_MSG_PROP;
    msg.context     = (uint8_t *)data;
    msg.context_len = len;
    return cetcd_raft_step(r, &msg);
}

int cetcd_raft_propose_conf_change(cetcd_raft *r, const uint8_t *data, size_t len) {
    (void)r; (void)data; (void)len;
    return -1;
}

void cetcd_raft_apply_conf_change(cetcd_raft *r, const cetcd_conf_state *cs) {
    if (!r || !cs) return;
    for (uint32_t i = 0; i < cs->n_voters; i++) {
        add_peer_(r, cs->voters[i]);
    }
}

cetcd_ready cetcd_raft_ready(cetcd_raft *r) {
    cetcd_ready rd;
    memset(&rd, 0, sizeof(rd));
    if (!r) return rd;

    rd.hard_state  = r->pending_hs;
    rd.entries     = r->pending_entries;
    rd.n_entries   = r->n_pending_entries;
    rd.committed   = r->commit;
    rd.snapshot    = NULL;
    rd.messages    = r->pending_msgs;
    rd.n_messages  = r->n_pending_msgs;

    if (r->has_pending) {
        rd.soft_state = (cetcd_soft_state *)malloc(sizeof(cetcd_soft_state));
        if (rd.soft_state) {
            rd.soft_state->leader_id  = r->leader_id;
            rd.soft_state->raft_state = (int)r->role;
        }
    }

    /* Detach ownership */
    r->pending_hs        = NULL;
    r->pending_entries   = (cetcd_entry *)calloc(r->cap_pending_entries, sizeof(cetcd_entry));
    r->n_pending_entries = 0;
    r->pending_msgs      = (cetcd_msg *)calloc(r->cap_pending_msgs, sizeof(cetcd_msg));
    r->n_pending_msgs    = 0;
    r->has_pending       = false;

    return rd;
}

void cetcd_raft_advance(cetcd_raft *r, const cetcd_ready *rd) {
    if (!r || !rd) return;
    if (rd->committed > r->applied) {
        r->applied = rd->committed;
    }
}

/* ── Queries ────────────────────────────────────────────────────── */

cetcd_node_state cetcd_raft_state(cetcd_raft *r) {
    if (!r) return CETCD_NODE_FOLLOWER;
    return (cetcd_node_state)r->role;
}

uint64_t cetcd_raft_leader(cetcd_raft *r) {
    if (!r) return 0;
    return r->leader_id;
}

uint64_t cetcd_raft_term(cetcd_raft *r) {
    if (!r) return 0;
    return r->term;
}

uint64_t cetcd_raft_committed(cetcd_raft *r) {
    if (!r) return 0;
    return r->commit;
}

uint64_t cetcd_raft_applied(cetcd_raft *r) {
    if (!r) return 0;
    return r->applied;
}

/* ── Ready memory management ────────────────────────────────────── */

void cetcd_ready_free(cetcd_ready *rd) {
    if (!rd) return;
    free(rd->hard_state);
    free(rd->entries);
    if (rd->messages) {
        for (uint32_t i = 0; i < rd->n_messages; i++) {
            free(rd->messages[i].entries);
        }
    }
    free(rd->messages);
    free(rd->soft_state);
    if (rd->snapshot) {
        free(rd->snapshot->conf_state.voters);
        free(rd->snapshot->conf_state.learners);
        free(rd->snapshot->data);
        free(rd->snapshot);
    }
    memset(rd, 0, sizeof(*rd));
}
