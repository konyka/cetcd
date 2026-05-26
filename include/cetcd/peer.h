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

#ifdef __cplusplus
}
#endif
#endif
