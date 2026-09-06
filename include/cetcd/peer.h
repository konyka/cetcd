#ifndef CETCD_PEER_H_
#define CETCD_PEER_H_

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "cetcd/base.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct cetcd_peer cetcd_peer;
typedef struct cetcd_cluster cetcd_cluster;

typedef struct {
    uint64_t id;
    char     addr[256];
    uint16_t port;
    int      is_learner; /* 1 = non-voting; 0 = voter */
} cetcd_peer_info;

cetcd_peer *cetcd_peer_new(uint64_t id, const char *addr, uint16_t port);
void        cetcd_peer_free(cetcd_peer *p);

cetcd_cluster *cetcd_cluster_new(uint64_t self_id);
void           cetcd_cluster_free(cetcd_cluster *c);

int cetcd_cluster_add_peer(cetcd_cluster *c, const cetcd_peer_info *info);
int cetcd_cluster_remove_peer(cetcd_cluster *c, uint64_t id);

typedef void (*cetcd_peer_send_fn)(uint64_t to_id, const uint8_t *data, size_t len, void *udata);

int cetcd_cluster_set_sender(cetcd_cluster *c, cetcd_peer_send_fn fn, void *udata);

int cetcd_cluster_send_msg(cetcd_cluster *c, const uint8_t *serialized_msg, size_t len, uint64_t to_id);

size_t cetcd_msg_encode(const uint8_t *raft_msg_raw, size_t msg_len,
                         uint8_t **out);

int cetcd_msg_decode(const uint8_t *data, size_t len,
                      uint8_t **raft_msg_out, size_t *raft_msg_len);

/* 1 if `path` is the etcd rafthttp pipeline (`/raft`). */
int cetcd_peer_is_rafthttp_path(const char *path);

size_t cetcd_cluster_peer_count(const cetcd_cluster *c);

const cetcd_peer_info *cetcd_cluster_get_peer(const cetcd_cluster *c, uint64_t id);
const cetcd_peer_info *cetcd_cluster_get_peer_by_index(const cetcd_cluster *c, size_t index);
uint64_t              cetcd_cluster_self_id(const cetcd_cluster *c);
/* Self plus non-learner peers (self id in the peer list is not double-counted). */
uint32_t              cetcd_cluster_voter_count(const cetcd_cluster *c);
/* Learners in the peer list (self is not counted unless listed as a learner). */
uint32_t              cetcd_cluster_learner_count(const cetcd_cluster *c);
/* 1 if remove is safe. Learners always 1. Remaining voters must keep old quorum. */
int                   cetcd_reconfig_may_remove(uint32_t voter_count, int target_is_learner);
/* strict 0 always allows. strict 1 uses may_remove. */
int                   cetcd_reconfig_check_remove(uint32_t voter_count,
                                                  int target_is_learner,
                                                  int strict);
/* 1 if current_learners < max_learners. max_learners 0 refuses all. */
int                   cetcd_reconfig_may_add_learner(uint32_t current_learners,
                                                     uint32_t max_learners);
int                   cetcd_cluster_update_peer(cetcd_cluster *c, uint64_t id, const cetcd_peer_info *info);

/* Promote a learner to a voter. Returns NOTFOUND if missing, INVAL if already a voter. */
int cetcd_cluster_promote(cetcd_cluster *c, uint64_t id);

/* Assign an unused id when `info->id == 0` (max(self, peers)+1). */
uint64_t cetcd_cluster_alloc_id(const cetcd_cluster *c);

struct cetcd_backend;
void cetcd_cluster_set_backend(cetcd_cluster *c, struct cetcd_backend *be);
/* Persist one peer (id key). No-op without a backend. Fail-closed. */
int cetcd_cluster_persist_peer(cetcd_cluster *c, const cetcd_peer_info *info);
int cetcd_cluster_persist_del(cetcd_cluster *c, uint64_t id);
/* Restore peers from the `members` bucket (does not touch Raft). */
int cetcd_cluster_load(cetcd_cluster *c, struct cetcd_backend *be);
/* Persist / clear joint C_old (members key id=0). No-op without a backend. */
int cetcd_cluster_persist_joint(cetcd_cluster *c, const uint64_t *ids,
                                uint32_t n, uint64_t joint_index);
int cetcd_cluster_persist_clear_joint(cetcd_cluster *c);
uint32_t cetcd_cluster_loaded_joint(const cetcd_cluster *c, uint64_t *ids,
                                    uint32_t cap, uint64_t *joint_index);

/* Parse `id=host:port,id=http(s)://host:port`. id must be > 0; port 1..65535
 * (missing port → 2380). Duplicate ids, empty tokens, and overflow fail-closed.
 * RANGE = bad port; INVAL = everything else. https_out is optional. */
int cetcd_parse_initial_cluster(const char *spec,
                                cetcd_peer_info *out, uint32_t cap,
                                uint32_t *n_out, int *https_out);

#ifdef __cplusplus
}
#endif
#endif
