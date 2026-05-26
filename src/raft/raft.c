#include "cetcd/raft.h"
#include <stdlib.h>
#include <string.h>

/* ── Internal state ─────────────────────────────────────────────── */

typedef enum raft_role_ {
    ROLE_FOLLOWER     = 0,
    ROLE_CANDIDATE    = 1,
    ROLE_LEADER       = 2,
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

    uint64_t              max_size_per_msg;
    uint64_t              max_inflight_msgs;
    bool                  check_quorum;
    bool                  pre_vote;

    cetcd_raft_storage   *storage;

    /* Pending Ready state */
    cetcd_hard_state     *pending_hs;
    cetcd_entry          *pending_entries;
    uint32_t              n_pending_entries;
    cetcd_msg            *pending_msgs;
    uint32_t              n_pending_msgs;
    bool                  has_pending;
};

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

    r->max_size_per_msg  = cfg->max_size_per_msg;
    r->max_inflight_msgs = cfg->max_inflight_msgs;
    r->check_quorum      = cfg->check_quorum;
    r->pre_vote          = cfg->pre_vote;
    r->storage           = cfg->storage;

    return r;
}

void cetcd_raft_free(cetcd_raft *r) {
    if (!r) return;
    free(r->pending_hs);
    free(r->pending_entries);
    free(r->pending_msgs);
    free(r);
}

/* ── Core API ───────────────────────────────────────────────────── */

static void become_leader_(cetcd_raft *r) {
    r->role      = ROLE_LEADER;
    r->leader_id = r->id;
    r->elapsed_ticks = 0;
}

static void become_candidate_(cetcd_raft *r) {
    r->role      = ROLE_CANDIDATE;
    r->leader_id = 0;
    r->term++;
    r->vote     = r->id;
    r->elapsed_ticks = 0;
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

static void reset_pending_(cetcd_raft *r) {
    free(r->pending_hs);
    r->pending_hs = NULL;
    free(r->pending_entries);
    r->pending_entries = NULL;
    r->n_pending_entries = 0;
    free(r->pending_msgs);
    r->pending_msgs = NULL;
    r->n_pending_msgs = 0;
    r->has_pending = false;
}

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
    uint32_t new_count = r->n_pending_entries + 1;
    cetcd_entry *new_arr = (cetcd_entry *)realloc(r->pending_entries,
                                                    new_count * sizeof(cetcd_entry));
    if (!new_arr) return;
    r->pending_entries = new_arr;
    r->pending_entries[r->n_pending_entries] = *e;
    r->n_pending_entries = new_count;
    r->has_pending = true;
}

static void queue_msg_(cetcd_raft *r, const cetcd_msg *m) {
    uint32_t new_count = r->n_pending_msgs + 1;
    cetcd_msg *new_arr = (cetcd_msg *)realloc(r->pending_msgs,
                                                new_count * sizeof(cetcd_msg));
    if (!new_arr) return;
    r->pending_msgs = new_arr;
    r->pending_msgs[r->n_pending_msgs] = *m;
    r->n_pending_msgs = new_count;
    r->has_pending = true;
}

int cetcd_raft_step(cetcd_raft *r, cetcd_msg *msg) {
    if (!r || !msg) return -1;

    switch (msg->type) {
    case CETCD_MSG_HUP:
        if (r->role != ROLE_LEADER) {
            become_candidate_(r);
            queue_hard_state_(r);
            /* Single-node cluster: immediately become leader */
            become_leader_(r);
            queue_hard_state_(r);
        }
        break;

    case CETCD_MSG_PROP:
        if (r->role != ROLE_LEADER) return -1;
        {
            cetcd_entry e;
            e.term  = r->term;
            e.index = 0;
            e.type  = CETCD_ENTRY_NORMAL;
            e.data  = (cetcd_slice){.data = (uint8_t *)msg->context,
                                     .len  = msg->context_len};
            queue_entry_(r, &e);
            queue_hard_state_(r);
        }
        break;

    case CETCD_MSG_BEAT:
        if (r->role != ROLE_LEADER) break;
        break;

    default:
        break;
    }
    return 0;
}

void cetcd_raft_tick(cetcd_raft *r) {
    if (!r) return;
    r->elapsed_ticks++;

    if (r->role == ROLE_FOLLOWER || r->role == ROLE_PRE_CANDIDATE) {
        if (r->elapsed_ticks >= r->election_timeout) {
            become_candidate_(r);
            queue_hard_state_(r);
            /* Single-node: immediately win */
            become_leader_(r);
            queue_hard_state_(r);
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
    (void)r; (void)cs;
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

    /* Detach ownership from raft */
    r->pending_hs        = NULL;
    r->pending_entries   = NULL;
    r->n_pending_entries = 0;
    r->pending_msgs      = NULL;
    r->n_pending_msgs    = 0;
    r->has_pending       = false;

    return rd;
}

void cetcd_raft_advance(cetcd_raft *r, const cetcd_ready *rd) {
    if (!r || !rd) return;
    /* After advance, the caller has processed the ready.
     * Update applied index if entries were committed. */
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
