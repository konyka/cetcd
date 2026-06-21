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

    /* In-memory log (1-indexed: log[0] is unused sentinel) */
    cetcd_entry          *log;
    uint32_t              log_cap;
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

/* ── Log helpers ────────────────────────────────────────────────── */

static cetcd_entry *log_at_(cetcd_raft *r, uint64_t index) {
    if (index == 0 || index > r->log_last_index) return NULL;
    return &r->log[index];
}

static uint64_t log_term_at_(cetcd_raft *r, uint64_t index) {
    if (index == 0) return 0;
    cetcd_entry *e = log_at_(r, index);
    return e ? e->term : 0;
}

static void log_append_(cetcd_raft *r, const cetcd_entry *e) {
    uint64_t needed = e->index + 1;
    if (needed > r->log_cap) {
        uint32_t new_cap = r->log_cap ? r->log_cap : 64;
        while (new_cap < (uint32_t)needed) {
            if (new_cap > (uint32_t)-1 / 2) { new_cap = (uint32_t)needed; break; }
            new_cap *= 2;
        }
        cetcd_entry *new_log = (cetcd_entry *)realloc(r->log, new_cap * sizeof(cetcd_entry));
        if (new_log == NULL) return; /* allocation failure; entry not stored */
        memset(new_log + r->log_cap, 0, (new_cap - r->log_cap) * sizeof(cetcd_entry));
        r->log = new_log;
        r->log_cap = new_cap;
    }
    r->log[e->index] = *e;
    if (e->index > r->log_last_index) {
        r->log_last_index = e->index;
        r->log_last_term = e->term;
    }
}

static void log_truncate_after_(cetcd_raft *r, uint64_t index) {
    if (index >= r->log_last_index) return;
    for (uint64_t i = index + 1; i <= r->log_last_index && i < r->log_cap; i++) {
        memset(&r->log[i], 0, sizeof(cetcd_entry));
    }
    r->log_last_index = index;
    r->log_last_term = log_term_at_(r, index);
}

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
        uint32_t new_cap = r->peers_cap ? r->peers_cap * 2 : 8;
        uint64_t *new_peers = (uint64_t *)realloc(r->peers, new_cap * sizeof(uint64_t));
        if (new_peers == NULL) return;
        uint64_t *new_votes = (uint64_t *)realloc(r->votes_granted, new_cap * sizeof(uint64_t));
        if (new_votes == NULL) { r->peers = new_peers; r->peers_cap = new_cap; return; }
        r->peers = new_peers;
        r->votes_granted = new_votes;
        r->peers_cap = new_cap;
    }
    r->peers[r->n_peers++] = id;
}

static uint32_t cluster_size_(cetcd_raft *r) {
    return r->n_peers;
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
        uint32_t new_cap = r->cap_pending_entries ? r->cap_pending_entries * 2 : 16;
        cetcd_entry *new_buf = (cetcd_entry *)realloc(r->pending_entries,
            new_cap * sizeof(cetcd_entry));
        if (new_buf == NULL) return; /* drop entry on OOM */
        r->pending_entries = new_buf;
        r->cap_pending_entries = new_cap;
    }
    r->pending_entries[r->n_pending_entries++] = *e;
    r->has_pending = true;
}

static void queue_msg_(cetcd_raft *r, const cetcd_msg *m) {
    if (r->n_pending_msgs >= r->cap_pending_msgs) {
        uint32_t new_cap = r->cap_pending_msgs ? r->cap_pending_msgs * 2 : 32;
        cetcd_msg *new_buf = (cetcd_msg *)realloc(r->pending_msgs,
            new_cap * sizeof(cetcd_msg));
        if (new_buf == NULL) return; /* drop msg on OOM */
        r->pending_msgs = new_buf;
        r->cap_pending_msgs = new_cap;
    }
    r->pending_msgs[r->n_pending_msgs++] = *m;
    r->has_pending = true;
}

/* ── Commit advancement ─────────────────────────────────────────── */

static void maybe_advance_commit_(cetcd_raft *r) {
    if (r->role != ROLE_LEADER) return;

    uint64_t sorted[MAX_PEERS_ + 1];
    uint32_t n = 0;

    sorted[n++] = r->log_last_index;
    for (uint32_t i = 0; i < r->n_peers && i < MAX_PEERS_; i++) {
        sorted[n++] = r->progress[i].match_idx;
    }

    for (uint32_t i = 0; i < n - 1; i++) {
        for (uint32_t j = i + 1; j < n; j++) {
            if (sorted[j] > sorted[i]) {
                uint64_t tmp = sorted[i];
                sorted[i] = sorted[j];
                sorted[j] = tmp;
            }
        }
    }

    uint64_t new_commit = sorted[quorum_(n) - 1];

    if (new_commit > r->commit && log_term_at_(r, new_commit) == r->term) {
        r->commit = new_commit;
        queue_hard_state_(r);
    }
}

/* ── Lifecycle ──────────────────────────────────────────────────── */

cetcd_raft *cetcd_raft_new(cetcd_raft_config *cfg) {
    if (!cfg) return NULL;
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

    add_peer_(r, cfg->id);

    r->log_cap = 64;
    r->log = (cetcd_entry *)calloc(r->log_cap, sizeof(cetcd_entry));
    r->log_last_index = 0;
    r->log_last_term = 0;

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
    free(r->log);
    free(r);
}

/* ── Role transitions ──────────────────────────────────────────── */

static void become_leader_(cetcd_raft *r) {
    r->role      = ROLE_LEADER;
    r->leader_id = r->id;
    r->elapsed_ticks = 0;
    r->heartbeat_elapsed = 0;

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

    r->n_votes_granted = 0;
    r->votes_granted[r->n_votes_granted++] = r->id;

    if (cluster_size_(r) == 1) {
        become_leader_(r);
        return;
    }

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

    if (msg->term > r->term) {
        become_follower_(r, msg->term, 0);
        queue_hard_state_(r);
    }

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
        if (r->n_votes_granted < r->peers_cap)
            r->votes_granted[r->n_votes_granted++] = msg->from;
        if (r->n_votes_granted >= quorum_(cluster_size_(r))) {
            become_leader_(r);
            queue_hard_state_(r);
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

    if (msg->term > r->term) {
        become_follower_(r, msg->term, msg->from);
        queue_hard_state_(r);
    }

    if (r->role != ROLE_FOLLOWER) {
        become_follower_(r, msg->term, msg->from);
    }
    r->leader_id = msg->from;
    r->elapsed_ticks = 0;

    /* Log consistency check */
    if (msg->index > 0) {
        if (msg->index > r->log_last_index) {
            cetcd_msg resp;
            memset(&resp, 0, sizeof(resp));
            resp.type   = CETCD_MSG_APP_RESP;
            resp.to     = msg->from;
            resp.from   = r->id;
            resp.term   = r->term;
            resp.index  = r->log_last_index;
            resp.reject = 1;
            queue_msg_(r, &resp);
            return 0;
        }
        if (log_term_at_(r, msg->index) != msg->log_term) {
            cetcd_msg resp;
            memset(&resp, 0, sizeof(resp));
            resp.type   = CETCD_MSG_APP_RESP;
            resp.to     = msg->from;
            resp.from   = r->id;
            resp.term   = r->term;
            resp.index  = msg->index;
            resp.reject = 1;
            queue_msg_(r, &resp);
            return 0;
        }
    }

    /* Truncate conflicting entries and append new ones */
    if (msg->n_entries > 0) {
        uint64_t first_new = msg->index + 1;
        for (uint32_t i = 0; i < msg->n_entries; i++) {
            uint64_t idx = first_new + i;
            if (idx <= r->log_last_index) {
                if (log_term_at_(r, idx) != msg->entries[i].term) {
                    log_truncate_after_(r, idx - 1);
                }
            }
            cetcd_entry e = msg->entries[i];
            e.index = idx;
            log_append_(r, &e);
            queue_entry_(r, &e);
        }
    }

    if (msg->commit > r->commit) {
        uint64_t new_commit = msg->commit;
        if (new_commit > r->log_last_index) {
            new_commit = r->log_last_index;
        }
        if (new_commit > r->commit) {
            r->commit = new_commit;
            queue_hard_state_(r);
        }
    }

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

    int slot = -1;
    for (uint32_t i = 0; i < r->n_peers && i < MAX_PEERS_; i++) {
        if (r->peers[i] == msg->from) { slot = (int)i; break; }
    }
    if (slot < 0) return 0;

    if (!msg->reject) {
        r->progress[slot].match_idx = msg->index;
        r->progress[slot].next_idx  = msg->index + 1;
        maybe_advance_commit_(r);
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
        uint64_t new_commit = msg->commit;
        if (new_commit > r->log_last_index) {
            new_commit = r->log_last_index;
        }
        if (new_commit > r->commit) {
            r->commit = new_commit;
            queue_hard_state_(r);
        }
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
            log_append_(r, &e);
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
                app.index     = r->log_last_index - 1;
                app.commit    = r->commit;
                cetcd_entry *ecopy = (cetcd_entry *)malloc(sizeof(cetcd_entry));
                if (ecopy) { *ecopy = e; }
                app.entries   = ecopy;
                app.n_entries = 1;
                queue_msg_(r, &app);
            }

            maybe_advance_commit_(r);
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

    case CETCD_MSG_TRANSFER_LEADER:
        if (r->role != ROLE_LEADER) break;
        {
            cetcd_msg tout;
            memset(&tout, 0, sizeof(tout));
            tout.type = CETCD_MSG_TIMEOUT_NOW;
            tout.to   = msg->to;
            tout.from = r->id;
            tout.term = r->term;
            queue_msg_(r, &tout);
        }
        break;

    case CETCD_MSG_TIMEOUT_NOW:
        if (r->role == ROLE_LEADER) break;
        r->elapsed_ticks = r->election_timeout;
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
    if (!r || r->role != ROLE_LEADER) return -1;
    if (!data || len == 0) return -1;

    /* Append a conf-change entry to the log, similar to a normal proposal
     * but with type = CETCD_ENTRY_CONF_CHANGE.
     *
     * The data is the serialized ConfChange protobuf. We store it as the
     * entry data so the apply pipeline can decode and apply it. */
    r->log_last_index++;
    cetcd_entry e;
    memset(&e, 0, sizeof(e));
    e.term  = r->term;
    e.index = r->log_last_index;
    e.type  = CETCD_ENTRY_CONF_CHANGE;
    e.data.data = (uint8_t *)data;  /* borrowed; not freed by us */
    e.data.len  = len;
    r->log_last_term = r->term;
    log_append_(r, &e);
    queue_entry_(r, &e);
    queue_hard_state_(r);

    /* Broadcast to followers */
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
        app.index     = r->log_last_index - 1;
        app.commit    = r->commit;
        cetcd_entry *ecopy = (cetcd_entry *)malloc(sizeof(cetcd_entry));
        if (ecopy) { *ecopy = e; }
        app.entries   = ecopy;
        app.n_entries = 1;
        queue_msg_(r, &app);
    }

    maybe_advance_commit_(r);
    return 0;
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

/* ── Wire encoding helpers ──────────────────────────────────────── */

static size_t wire_u64_(uint8_t *buf, uint64_t val) {
    size_t n = 0;
    do { uint8_t b = val & 0x7F; val >>= 7; if (val) b |= 0x80; buf[n++] = b; } while (val);
    return n;
}

static size_t wire_u32_(uint8_t *buf, uint32_t val) {
    size_t n = 0;
    do { uint8_t b = val & 0x7F; val >>= 7; if (val) b |= 0x80; buf[n++] = b; } while (val);
    return n;
}

static uint64_t dec_u64_(const uint8_t *buf, size_t len, size_t *pos) {
    uint64_t val = 0; uint32_t shift = 0;
    while (*pos < len) {
        uint8_t b = buf[(*pos)++];
        val |= (uint64_t)(b & 0x7F) << shift;
        if (!(b & 0x80)) break;
        shift += 7;
    }
    return val;
}

static uint32_t dec_u32_(const uint8_t *buf, size_t len, size_t *pos) {
    uint32_t val = 0; uint32_t shift = 0;
    while (*pos < len) {
        uint8_t b = buf[(*pos)++];
        val |= (uint32_t)(b & 0x7F) << shift;
        if (!(b & 0x80)) break;
        shift += 7;
    }
    return val;
}

#define TAG_U64(fnum, val) do { *p++ = (uint8_t)((fnum) << 3 | 0); p += wire_u64_(p, (val)); } while(0)
#define TAG_U32(fnum, val) do { *p++ = (uint8_t)((fnum) << 3 | 0); p += wire_u32_(p, (val)); } while(0)
#define TAG_BYTES(fnum, data, len) do { \
    *p++ = (uint8_t)((fnum) << 3 | 2); p += wire_u32_(p, (len)); \
    if ((len) > 0) { memcpy(p, (data), (len)); p += (len); } \
} while(0)

size_t cetcd_msg_encode_wire(const cetcd_msg *msg, uint8_t **out) {
    if (!msg || !out) return 0;

    /* Calculate upper bound for the encoded size to avoid stack overflow. */
    size_t need = 128; /* fixed fields (type, to, from, term, log_term, index, commit, reject) */
    if (msg->snapshot) need += 16;
    if (msg->context_len > 0 && msg->context) need += 10 + msg->context_len;
    for (uint32_t i = 0; i < msg->n_entries; i++) {
        const cetcd_entry *e = &msg->entries[i];
        need += 1 + 5 + 40 + e->data.len; /* tag + len-prefix + fields + data */
    }

    uint8_t *buf = (uint8_t *)malloc(need);
    if (!buf) return 0;
    uint8_t *p = buf;

    TAG_U32(1, (uint32_t)msg->type);
    TAG_U64(2, msg->to);
    TAG_U64(3, msg->from);
    TAG_U64(4, msg->term);
    TAG_U64(5, msg->log_term);
    TAG_U64(6, msg->index);
    TAG_U64(7, msg->commit);
    TAG_U64(8, msg->reject);
    if (msg->snapshot) {
        TAG_U64(9, msg->snapshot);
    }
    if (msg->context_len > 0 && msg->context) {
        TAG_BYTES(10, msg->context, msg->context_len);
    }
    for (uint32_t i = 0; i < msg->n_entries; i++) {
        const cetcd_entry *e = &msg->entries[i];

        /* Encode entry into a temporary heap buffer, then write length-prefixed. */
        size_t entry_need = 40 + e->data.len; /* upper bound for term+index+type+data */
        uint8_t *entry_buf = (uint8_t *)malloc(entry_need);
        if (!entry_buf) { free(buf); return 0; }
        uint8_t *etp = entry_buf;
        do { *etp++ = (uint8_t)((1) << 3 | 0); etp += wire_u64_(etp, e->term); } while(0);
        do { *etp++ = (uint8_t)((2) << 3 | 0); etp += wire_u64_(etp, e->index); } while(0);
        do { *etp++ = (uint8_t)((3) << 3 | 0); etp += wire_u32_(etp, (uint32_t)e->type); } while(0);
        if (e->data.len > 0 && e->data.data) {
            do { *etp++ = (uint8_t)((4) << 3 | 2); etp += wire_u32_(etp, e->data.len); memcpy(etp, e->data.data, e->data.len); etp += e->data.len; } while(0);
        }
        uint32_t elen = (uint32_t)(etp - entry_buf);
        *p++ = (uint8_t)((11) << 3 | 2);
        p += wire_u32_(p, elen);
        memcpy(p, entry_buf, elen);
        p += elen;
        free(entry_buf);
    }

    size_t total = (size_t)(p - buf);
    *out = buf;
    return total;
}

cetcd_msg *cetcd_msg_decode_wire(const uint8_t *data, size_t len) {
    if (!data || len == 0) return NULL;
    cetcd_msg *msg = (cetcd_msg *)calloc(1, sizeof(*msg));
    if (!msg) return NULL;

    size_t pos = 0;
    cetcd_entry entries[256];
    uint32_t n_entries = 0;

    while (pos < len) {
        uint8_t tag = data[pos++];
        uint32_t fnum = tag >> 3;
        uint8_t wire = tag & 0x07;

        if (wire == 0) {
            if (fnum == 1) msg->type = (cetcd_msg_type)dec_u32_(data, len, &pos);
            else if (fnum == 2) msg->to = dec_u64_(data, len, &pos);
            else if (fnum == 3) msg->from = dec_u64_(data, len, &pos);
            else if (fnum == 4) msg->term = dec_u64_(data, len, &pos);
            else if (fnum == 5) msg->log_term = dec_u64_(data, len, &pos);
            else if (fnum == 6) msg->index = dec_u64_(data, len, &pos);
            else if (fnum == 7) msg->commit = dec_u64_(data, len, &pos);
            else if (fnum == 8) msg->reject = dec_u64_(data, len, &pos);
            else if (fnum == 9) msg->snapshot = dec_u64_(data, len, &pos);
            else { dec_u64_(data, len, &pos); }
        } else if (wire == 2) {
            uint32_t blen = dec_u32_(data, len, &pos);
            if (pos + blen > len) break;
            if (fnum == 10) {
                msg->context = (uint8_t *)malloc(blen);
                if (msg->context) { memcpy(msg->context, data + pos, blen); msg->context_len = blen; }
            } else if (fnum == 11 && n_entries < 256) {
                const uint8_t *edata = data + pos;
                size_t epos = 0;
                memset(&entries[n_entries], 0, sizeof(cetcd_entry));
                while (epos < blen) {
                    uint8_t etag = edata[epos++];
                    uint32_t ef = etag >> 3;
                    uint8_t ew = etag & 0x07;
                    if (ew == 0) {
                        if (ef == 1) entries[n_entries].term = dec_u64_(edata, blen, &epos);
                        else if (ef == 2) entries[n_entries].index = dec_u64_(edata, blen, &epos);
                        else if (ef == 3) entries[n_entries].type = (cetcd_entry_type)dec_u32_(edata, blen, &epos);
                        else { dec_u64_(edata, blen, &epos); }
                    } else if (ew == 2) {
                        uint32_t dlen = dec_u32_(edata, blen, &epos);
                        if (ef == 4 && epos + dlen <= blen) {
                            entries[n_entries].data.data = (uint8_t *)malloc(dlen);
                            if (entries[n_entries].data.data) {
                                memcpy(entries[n_entries].data.data, edata + epos, dlen);
                                entries[n_entries].data.len = dlen;
                            }
                        }
                        epos += dlen;
                    } else { break; }
                }
                n_entries++;
            }
            pos += blen;
        } else { break; }
    }

    if (n_entries > 0) {
        msg->entries = (cetcd_entry *)calloc(n_entries, sizeof(cetcd_entry));
        if (msg->entries) { memcpy(msg->entries, entries, n_entries * sizeof(cetcd_entry)); }
        msg->n_entries = n_entries;
    }
    return msg;
}

void cetcd_msg_free(cetcd_msg *msg) {
    if (!msg) return;
    if (msg->entries) {
        for (uint32_t i = 0; i < msg->n_entries; i++) {
            free(msg->entries[i].data.data);
        }
        free(msg->entries);
    }
    free(msg->context);
    free(msg);
}
