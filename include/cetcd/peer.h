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

size_t cetcd_cluster_peer_count(const cetcd_cluster *c);

const cetcd_peer_info *cetcd_cluster_get_peer(const cetcd_cluster *c, uint64_t id);
const cetcd_peer_info *cetcd_cluster_get_peer_by_index(const cetcd_cluster *c, size_t index);
uint64_t              cetcd_cluster_self_id(const cetcd_cluster *c);
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

#ifdef __cplusplus
}
#endif
#endif
