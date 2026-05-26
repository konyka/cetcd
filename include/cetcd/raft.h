#ifndef CETCD_RAFT_H
#define CETCD_RAFT_H

#include "cetcd/base.h"
#include <stdbool.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ── Entry types ─────────────────────────────────────────────────── */

typedef enum cetcd_entry_type {
    CETCD_ENTRY_NORMAL     = 0,
    CETCD_ENTRY_CONF_CHANGE = 1,
    CETCD_ENTRY_CONF_CHANGE_V2 = 2,
} cetcd_entry_type;

typedef struct cetcd_entry {
    uint64_t            term;
    uint64_t            index;
    cetcd_entry_type    type;
    cetcd_slice         data;
} cetcd_entry;

/* ── Message types ───────────────────────────────────────────────── */

typedef enum cetcd_msg_type {
    CETCD_MSG_HUP            = 0,
    CETCD_MSG_BEAT           = 1,
    CETCD_MSG_PROP           = 2,
    CETCD_MSG_APP            = 3,
    CETCD_MSG_APP_RESP       = 4,
    CETCD_MSG_VOTE           = 5,
    CETCD_MSG_VOTE_RESP      = 6,
    CETCD_MSG_SNAP           = 7,
    CETCD_MSG_HEARTBEAT      = 8,
    CETCD_MSG_HEARTBEAT_RESP = 9,
    CETCD_MSG_UNREACHABLE    = 10,
    CETCD_MSG_SNAP_STATUS    = 11,
    CETCD_MSG_CHECK_QUORUM   = 12,
    CETCD_MSG_TRANSFER_LEADER= 13,
    CETCD_MSG_TIMEOUT_NOW    = 14,
    CETCD_MSG_READ_INDEX     = 15,
    CETCD_MSG_READ_INDEX_RESP= 16,
    CETCD_MSG_PRE_VOTE       = 17,
    CETCD_MSG_PRE_VOTE_RESP  = 18,
} cetcd_msg_type;

typedef struct cetcd_msg {
    cetcd_msg_type  type;
    uint64_t        to;
    uint64_t        from;
    uint64_t        term;
    uint64_t        log_term;
    uint64_t        index;
    uint64_t        commit;
    uint64_t        reject;       /* bool: 0=accept, 1=reject */
    cetcd_entry    *entries;
    uint32_t        n_entries;
    uint64_t        snapshot;     /* snapshot index */
    /* Context for read requests */
    uint8_t        *context;
    size_t          context_len;
} cetcd_msg;

/* ── HardState / SoftState / ConfState ───────────────────────────── */

typedef struct cetcd_hard_state {
    uint64_t term;
    uint64_t vote;
    uint64_t commit;
} cetcd_hard_state;

typedef struct cetcd_soft_state {
    uint64_t leader_id;
    int      raft_state;   /* CETCD_STATE_FOLLOWER etc. */
} cetcd_soft_state;

typedef struct cetcd_conf_state {
    uint64_t *voters;
    uint32_t  n_voters;
    uint64_t *learners;
    uint32_t  n_learners;
} cetcd_conf_state;

/* ── Snapshot ────────────────────────────────────────────────────── */

typedef struct cetcd_snapshot {
    uint64_t         term;
    uint64_t         index;
    cetcd_conf_state conf_state;
    uint8_t         *data;
    size_t           data_len;
} cetcd_snapshot;

/* ── Ready ───────────────────────────────────────────────────────── */

typedef struct cetcd_ready {
    cetcd_hard_state *hard_state;      /* NULL if unchanged */
    cetcd_soft_state *soft_state;      /* NULL if unchanged */
    cetcd_entry      *entries;         /* new entries to persist */
    uint32_t          n_entries;
    uint64_t          committed;       /* new committed index */
    cetcd_snapshot   *snapshot;        /* NULL if no snapshot */
    cetcd_msg        *messages;        /* outgoing messages */
    uint32_t          n_messages;
} cetcd_ready;

/* ── Node state ──────────────────────────────────────────────────── */

typedef enum cetcd_node_state {
    CETCD_NODE_FOLLOWER     = 0,
    CETCD_NODE_CANDIDATE    = 1,
    CETCD_NODE_LEADER       = 2,
    CETCD_NODE_PRE_CANDIDATE = 3,
} cetcd_node_state;

/* ── Storage interface ───────────────────────────────────────────── */

typedef struct cetcd_raft_storage cetcd_raft_storage;

struct cetcd_raft_storage {
    void              *user_data;
    cetcd_hard_state  (*initial_state)(void *user_data);
    cetcd_entry*      (*entries)(void *user_data, uint64_t lo, uint64_t hi, uint64_t max_size, uint32_t *count);
    uint64_t          (*term)(void *user_data);
    uint64_t          (*first_index)(void *user_data);
    uint64_t          (*last_index)(void *user_data);
    cetcd_snapshot*   (*snapshot)(void *user_data);
};

/* ── Config ──────────────────────────────────────────────────────── */

typedef struct cetcd_raft_config {
    uint64_t               id;
    uint64_t               election_tick;
    uint64_t               heartbeat_tick;
    cetcd_raft_storage    *storage;
    uint64_t               max_size_per_msg;
    uint64_t               max_inflight_msgs;
    bool                   check_quorum;
    bool                   pre_vote;
    bool                   disable_proposal_forwarding;
} cetcd_raft_config;

/* ── Raft node (opaque) ──────────────────────────────────────────── */

typedef struct cetcd_raft cetcd_raft;

/* Lifecycle */
cetcd_raft    *cetcd_raft_new(cetcd_raft_config *cfg);
void           cetcd_raft_free(cetcd_raft *r);

/* Core API (mirrors go.etcd.io/raft's Step/Tick/Ready/Advance) */
int            cetcd_raft_step(cetcd_raft *r, cetcd_msg *msg);
void           cetcd_raft_tick(cetcd_raft *r);
cetcd_ready    cetcd_raft_ready(cetcd_raft *r);
void           cetcd_raft_advance(cetcd_raft *r, const cetcd_ready *rd);

/* Proposals */
int            cetcd_raft_propose(cetcd_raft *r, const uint8_t *data, size_t len);
int            cetcd_raft_propose_conf_change(cetcd_raft *r, const uint8_t *data, size_t len);
void           cetcd_raft_apply_conf_change(cetcd_raft *r, const cetcd_conf_state *cs);

/* Queries */
cetcd_node_state  cetcd_raft_state(cetcd_raft *r);
uint64_t          cetcd_raft_leader(cetcd_raft *r);
uint64_t          cetcd_raft_term(cetcd_raft *r);

/* Status / info */
uint64_t          cetcd_raft_committed(cetcd_raft *r);
uint64_t          cetcd_raft_applied(cetcd_raft *r);

/* Memory management for Ready */
void              cetcd_ready_free(cetcd_ready *rd);

/* Message serialization for peer transport */
size_t            cetcd_msg_encode_wire(const cetcd_msg *msg, uint8_t **out);
cetcd_msg        *cetcd_msg_decode_wire(const uint8_t *data, size_t len);
void              cetcd_msg_free(cetcd_msg *msg);

#ifdef __cplusplus
}
#endif

#endif /* CETCD_RAFT_H */
