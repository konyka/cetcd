#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#include "cetcd/peer.h"
#include "cetcd/base.h"

/* Internal structures matching the public API as opaque types in header */
struct cetcd_peer {
    uint64_t id;
    char     addr[256];
    uint16_t port;
    int      is_learner;
};

struct cetcd_cluster {
    uint64_t           self_id;
    cetcd_peer       **peers;
    size_t             peer_count;
    size_t             peer_cap;
    cetcd_peer_send_fn send_fn;
    void              *send_udata;
};

/* cetcd_peer API */
cetcd_peer *cetcd_peer_new(uint64_t id, const char *addr, uint16_t port) {
    cetcd_peer *p = (cetcd_peer *)malloc(sizeof(cetcd_peer));
    if (!p) return NULL;
    p->id = id;
    if (addr) {
        snprintf(p->addr, sizeof(p->addr), "%s", addr);
    } else {
        p->addr[0] = '\0';
    }
    p->port = port;
    p->is_learner = 0;
    return p;
}

void cetcd_peer_free(cetcd_peer *p) {
    if (p) free(p);
}

/* cetcd_cluster API */
cetcd_cluster *cetcd_cluster_new(uint64_t self_id) {
    cetcd_cluster *c = (cetcd_cluster *)malloc(sizeof(cetcd_cluster));
    if (!c) return NULL;
    c->self_id = self_id;
    c->peers = NULL;
    c->peer_count = 0;
    c->peer_cap = 0;
    c->send_fn = NULL;
    c->send_udata = NULL;
    /* Start with a small capacity to avoid many reallocs */
    c->peer_cap = 4;
    c->peers = (cetcd_peer **)malloc(c->peer_cap * sizeof(cetcd_peer *));
    if (!c->peers) {
        free(c);
        return NULL;
    }
    for (size_t i = 0; i < c->peer_cap; ++i) c->peers[i] = NULL;
    c->peer_count = 0;
    return c;
}

void cetcd_cluster_free(cetcd_cluster *c) {
    if (!c) return;
    if (c->peers) {
        for (size_t i = 0; i < c->peer_count; ++i) {
            if (c->peers[i]) {
                cetcd_peer_free(c->peers[i]);
            }
        }
        free(c->peers);
    }
    free(c);
}

int cetcd_cluster_add_peer(cetcd_cluster *c, const cetcd_peer_info *info) {
    if (!c || !info) return CETCD_ERR_INVAL;
    /* Check for duplicates by id */
    for (size_t i = 0; i < c->peer_count; ++i) {
        if (c->peers[i] && c->peers[i]->id == info->id) {
            return CETCD_ERR_EXISTS;
        }
    }
    /* Grow if needed */
    if (c->peer_count >= c->peer_cap) {
        size_t new_cap = c->peer_cap ? c->peer_cap * 2 : 4;
        cetcd_peer **new_arr = (cetcd_peer **)realloc(c->peers, new_cap * sizeof(cetcd_peer *));
        if (!new_arr) return CETCD_ERR_NOMEM;
        c->peers = new_arr;
        for (size_t i = c->peer_cap; i < new_cap; ++i) c->peers[i] = NULL;
        c->peer_cap = new_cap;
    }
    cetcd_peer *p = cetcd_peer_new(info->id, info->addr, info->port);
    if (!p) return CETCD_ERR_NOMEM;
    p->is_learner = info->is_learner ? 1 : 0;
    c->peers[c->peer_count++] = p;
    return CETCD_OK;
}

int cetcd_cluster_remove_peer(cetcd_cluster *c, uint64_t id) {
    if (!c) return CETCD_ERR_INVAL;
    for (size_t i = 0; i < c->peer_count; ++i) {
        if (c->peers[i] && c->peers[i]->id == id) {
            /* Free the peer and fill the gap by moving last element here */
            cetcd_peer_free(c->peers[i]);
            if (i != c->peer_count - 1) {
                c->peers[i] = c->peers[c->peer_count - 1];
            }
            c->peers[c->peer_count - 1] = NULL;
            c->peer_count--;
            return CETCD_OK;
        }
    }
    return CETCD_ERR_NOTFOUND;
}

int cetcd_cluster_set_sender(cetcd_cluster *c, cetcd_peer_send_fn fn, void *udata) {
    if (!c) return CETCD_ERR_INVAL;
    c->send_fn = fn;
    c->send_udata = udata;
    return CETCD_OK;
}

int cetcd_cluster_send_msg(cetcd_cluster *c, const uint8_t *serialized_msg, size_t len, uint64_t to_id) {
    if (!c) return CETCD_ERR_INVAL;
    if (c->send_fn) {
        c->send_fn(to_id, serialized_msg, len, c->send_udata);
    }
    return CETCD_OK;
}

size_t cetcd_msg_encode(const uint8_t *raft_msg_raw, size_t msg_len, uint8_t **out) {
    if (!out) return 0;
    size_t total = 4 + msg_len;
    uint8_t *buf = (uint8_t *)malloc(total);
    if (!buf) return 0;
    uint32_t be_len = (uint32_t)msg_len;
    buf[0] = (be_len >> 24) & 0xFF;
    buf[1] = (be_len >> 16) & 0xFF;
    buf[2] = (be_len >> 8) & 0xFF;
    buf[3] = (be_len) & 0xFF;
    if (msg_len > 0 && raft_msg_raw) {
        memcpy(buf + 4, raft_msg_raw, msg_len);
    }
    *out = buf;
    return total;
}

int cetcd_msg_decode(const uint8_t *data, size_t len, uint8_t **raft_msg_out, size_t *raft_msg_len) {
    if (!data || !raft_msg_out || !raft_msg_len || len < 4) return CETCD_ERR_INVAL;
    uint32_t payload_len = ((uint32_t)data[0] << 24) |
                           ((uint32_t)data[1] << 16) |
                           ((uint32_t)data[2] << 8)  |
                           ((uint32_t)data[3]);
    if ((size_t)payload_len + 4 > len) {
        return CETCD_ERR_CORRUPT;
    }
    uint8_t *payload = (uint8_t *)malloc(payload_len);
    if (!payload) return CETCD_ERR_NOMEM;
    if (payload_len > 0) {
        memcpy(payload, data + 4, payload_len);
    }
    *raft_msg_out = payload;
    *raft_msg_len = payload_len;
    return CETCD_OK;
}

size_t cetcd_cluster_peer_count(const cetcd_cluster *c) {
    if (!c) return 0;
    return c->peer_count;
}

/* Thread-local storage avoids data races when multiple reactor threads
 * query cluster membership concurrently. */
static _Thread_local cetcd_peer_info peer_info_buf_;

const cetcd_peer_info *cetcd_cluster_get_peer(const cetcd_cluster *c, uint64_t id) {
    if (!c) return NULL;
    for (size_t i = 0; i < c->peer_count; i++) {
        if (c->peers[i] && c->peers[i]->id == id) {
            peer_info_buf_.id = c->peers[i]->id;
            strncpy(peer_info_buf_.addr, c->peers[i]->addr, sizeof(peer_info_buf_.addr) - 1);
            peer_info_buf_.addr[sizeof(peer_info_buf_.addr) - 1] = '\0';
            peer_info_buf_.port = c->peers[i]->port;
            peer_info_buf_.is_learner = c->peers[i]->is_learner;
            return &peer_info_buf_;
        }
    }
    return NULL;
}

const cetcd_peer_info *cetcd_cluster_get_peer_by_index(const cetcd_cluster *c, size_t index) {
    if (!c || index >= c->peer_count) return NULL;
    if (!c->peers[index]) return NULL;
    peer_info_buf_.id = c->peers[index]->id;
    snprintf(peer_info_buf_.addr, sizeof(peer_info_buf_.addr), "%s", c->peers[index]->addr);
    peer_info_buf_.port = c->peers[index]->port;
    peer_info_buf_.is_learner = c->peers[index]->is_learner;
    return &peer_info_buf_;
}

uint64_t cetcd_cluster_self_id(const cetcd_cluster *c) {
    if (!c) return 0;
    return c->self_id;
}

int cetcd_cluster_update_peer(cetcd_cluster *c, uint64_t id, const cetcd_peer_info *info) {
    if (!c || !info) return CETCD_ERR_INVAL;
    for (size_t i = 0; i < c->peer_count; i++) {
        if (c->peers[i] && c->peers[i]->id == id) {
            c->peers[i]->id = info->id;
            snprintf(c->peers[i]->addr, sizeof(c->peers[i]->addr), "%s", info->addr);
            c->peers[i]->port = info->port;
            c->peers[i]->is_learner = info->is_learner ? 1 : 0;
            return CETCD_OK;
        }
    }
    return CETCD_ERR_NOTFOUND;
}

uint64_t cetcd_cluster_alloc_id(const cetcd_cluster *c) {
    if (!c) return 1;
    uint64_t max = c->self_id;
    for (size_t i = 0; i < c->peer_count; i++) {
        if (c->peers[i] && c->peers[i]->id > max)
            max = c->peers[i]->id;
    }
    return max + 1;
}

int cetcd_cluster_promote(cetcd_cluster *c, uint64_t id) {
    if (!c || id == 0) return CETCD_ERR_INVAL;
    for (size_t i = 0; i < c->peer_count; i++) {
        if (c->peers[i] && c->peers[i]->id == id) {
            if (!c->peers[i]->is_learner) return CETCD_ERR_INVAL;
            c->peers[i]->is_learner = 0;
            return CETCD_OK;
        }
    }
    return CETCD_ERR_NOTFOUND;
}
