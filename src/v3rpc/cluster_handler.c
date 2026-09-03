/*
 * Cluster RPC handlers.
 *
 * Implements:
 *   - MemberList:   list all cluster members
 *   - MemberAdd:    add a new member to the cluster
 *   - MemberRemove: remove a member from the cluster
 *   - MemberUpdate: update a member's peer URLs
 *   - MemberPromote: promote a learner to voting member
 *
 * Protobuf field encoding:
 *   MemberListRequest: empty
 *   MemberListResponse:
 *     field 2 (members) = repeated Member, tag = 0x12 (length-delimited)
 *       Member:
 *         field 1 (ID)       = uint64, tag = 0x08
 *         field 2 (peerURLs) = repeated string, tag = 0x12
 *
 *   MemberAddRequest:
 *     field 1 (peerURLs)    = repeated string, tag = 0x0a
 *     field 2 (isLearner)   = bool, tag = 0x10
 *   MemberAddResponse:
 *     field 1 (header)      = ResponseHeader
 *     field 2 (member)      = Member
 *
 *   MemberRemoveRequest:
 *     field 1 (ID)          = uint64, tag = 0x08
 *   MemberRemoveResponse: header only
 *
 *   MemberUpdateRequest:
 *     field 1 (ID)          = uint64, tag = 0x08
 *     field 2 (peerURLs)    = repeated string, tag = 0x12
 *   MemberUpdateResponse: header only
 *
 *   MemberPromoteRequest:
 *     field 1 (ID)          = uint64, tag = 0x08
 *   MemberPromoteResponse: header only
 */

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#include "cetcd/v3rpc.h"
#include "cetcd/peer.h"
#include "cetcd/mvcc.h"
#include "cetcd/raft.h"

extern cetcd_cluster *g_rpc_cluster;
extern uint64_t       g_rpc_node_id;
extern cetcd_mvcc_store *g_rpc_store;
extern cetcd_raft    *g_rpc_raft;

/* Forward declarations */
cetcd_rpc_bytes cluster_handle_member_list(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes cluster_handle_member_add(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes cluster_handle_member_remove(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes cluster_handle_member_update(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes cluster_handle_member_promote(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);

static int read_varint_c(const uint8_t *buf, size_t len, size_t *pos, uint64_t *out) {
    uint64_t val = 0; int shift = 0;
    while (*pos < len) {
        uint8_t b = buf[*pos]; (*pos)++;
        val |= (uint64_t)(b & 0x7F) << shift;
        if ((b & 0x80) == 0) { *out = val; return 0; }
        shift += 7; if (shift > 63) break;
    }
    return -1;
}

static int write_varint_c(uint8_t *buf, size_t cap, size_t *pos, uint64_t val) {
    while (*pos < cap) {
        uint8_t b = val & 0x7F;
        val >>= 7;
        if (val) b |= 0x80;
        buf[(*pos)++] = b;
        if (!val) return 0;
    }
    return -1;
}

static int read_bytes_c(const uint8_t *buf, size_t len, size_t *pos,
                         uint8_t **out, size_t *out_len) {
    uint64_t l = 0;
    if (read_varint_c(buf, len, pos, &l) != 0) return -1;
    if (*pos + l > len) return -1;
    uint8_t *p = (uint8_t *)malloc((size_t)l + 1);
    if (!p) return -1;
    memcpy(p, buf + *pos, (size_t)l);
    p[(size_t)l] = '\0';
    *pos += (size_t)l;
    *out = p;
    *out_len = (size_t)l;
    return 0;
}

static cetcd_rpc_bytes make_simple_cluster_response(void) {
    /* Return a ResponseHeader (field 1) with current revision */
    int64_t current_rev = g_rpc_store ? cetcd_mvcc_revision(g_rpc_store) : 0;
    uint8_t hdr_buf[16];
    size_t hp = 0;
    hdr_buf[hp++] = 0x18; /* field 3 = revision */
    write_varint_c(hdr_buf, sizeof(hdr_buf), &hp, (uint64_t)(current_rev > 0 ? current_rev : 1));

    uint8_t *b = (uint8_t *)malloc(2 + hp);
    if (!b) return (cetcd_rpc_bytes){NULL, 0};
    size_t pos = 0;
    b[pos++] = 0x0a; /* field 1 = header */
    write_varint_c(b, 2 + hp, &pos, (uint64_t)hp);
    memcpy(b + pos, hdr_buf, hp);
    pos += hp;
    return (cetcd_rpc_bytes){b, pos};
}

/*
 * Encode a Member message into buffer.
 * Member:
 *   field 1 (ID)       = uint64, tag = 0x08
 *   field 2 (peerURLs) = repeated string, tag = 0x12
 */
static size_t encode_member(uint8_t *buf, size_t cap, uint64_t id,
                             const char *peer_addr, int is_learner) {
    size_t pos = 0;
    buf[pos++] = 0x08; /* field 1 = ID */
    write_varint_c(buf, cap, &pos, id);
    if (peer_addr && *peer_addr) {
        size_t alen = strlen(peer_addr);
        buf[pos++] = 0x12; /* field 2 = peerURLs (string) */
        write_varint_c(buf, cap, &pos, (uint64_t)alen);
        if (pos + alen < cap) {
            memcpy(buf + pos, peer_addr, alen);
            pos += alen;
        }
    }
    /* field 3 = name (string) */
    {
        const char *name = "default";
        size_t nlen = strlen(name);
        buf[pos++] = 0x1a;
        write_varint_c(buf, cap, &pos, (uint64_t)nlen);
        if (pos + nlen < cap) {
            memcpy(buf + pos, name, nlen);
            pos += nlen;
        }
    }
    /* field 4 = clientURLs (string) */
    {
        const char *client_url = "http://127.0.0.1:2379";
        size_t clen = strlen(client_url);
        buf[pos++] = 0x22;
        write_varint_c(buf, cap, &pos, (uint64_t)clen);
        if (pos + clen < cap) {
            memcpy(buf + pos, client_url, clen);
            pos += clen;
        }
    }
    if (is_learner) {
        buf[pos++] = 0x28; /* field 5 = isLearner */
        buf[pos++] = 0x01;
    }
    return pos;
}

/*
 * MemberList RPC.
 * Returns all members in the cluster.
 */
cetcd_rpc_bytes cluster_handle_member_list(cetcd_v3rpc *rpc,
                                            const uint8_t *req, size_t req_len) {
    (void)rpc; (void)req; (void)req_len;

    uint8_t buf[1024];
    size_t pos = 0;

    /* field 1 = header (ResponseHeader with revision) */
    {
        int64_t current_rev = g_rpc_store ? cetcd_mvcc_revision(g_rpc_store) : 0;
        uint8_t hdr_buf[16]; size_t hp = 0;
        hdr_buf[hp++] = 0x18; /* revision */
        write_varint_c(hdr_buf, sizeof(hdr_buf), &hp, (uint64_t)(current_rev > 0 ? current_rev : 1));
        buf[pos++] = 0x0a;
        write_varint_c(buf, sizeof(buf), &pos, (uint64_t)hp);
        memcpy(buf + pos, hdr_buf, hp); pos += hp;
    }

    /* Encode self as a member */
    if (g_rpc_node_id > 0) {
        uint8_t member_buf[256];
        size_t mlen = encode_member(member_buf, sizeof(member_buf),
                                     g_rpc_node_id, "127.0.0.1:2380", 0);
        /* field 2 (members) = repeated Member, tag = 0x12 */
        buf[pos++] = 0x12;
        write_varint_c(buf, sizeof(buf), &pos, (uint64_t)mlen);
        if (pos + mlen < sizeof(buf)) {
            memcpy(buf + pos, member_buf, mlen);
            pos += mlen;
        }
    }

    /* Add known peers from cluster */
    if (g_rpc_cluster) {
        size_t peer_count = cetcd_cluster_peer_count(g_rpc_cluster);
        for (size_t i = 0; i < peer_count && pos < sizeof(buf) - 256; i++) {
            const cetcd_peer_info *pi = cetcd_cluster_get_peer_by_index(g_rpc_cluster, i);
            if (!pi) continue;
            char peer_url[300];
            snprintf(peer_url, sizeof(peer_url), "%s:%u", pi->addr, pi->port);
            uint8_t member_buf[256];
            size_t mlen = encode_member(member_buf, sizeof(member_buf),
                                         pi->id, peer_url, pi->is_learner);
            if (pos + 2 + mlen < sizeof(buf)) {
                buf[pos++] = 0x12;
                write_varint_c(buf, sizeof(buf), &pos, (uint64_t)mlen);
                memcpy(buf + pos, member_buf, mlen);
                pos += mlen;
            }
        }
    }

    uint8_t *out = (uint8_t *)malloc(pos > 0 ? pos : 1);
    if (!out) return (cetcd_rpc_bytes){NULL, 0};
    if (pos == 0) { out[0] = 0; pos = 1; }
    memcpy(out, buf, pos);
    return (cetcd_rpc_bytes){out, pos};
}

static int parse_peer_url_(const char *url, size_t url_len,
                           char *addr, size_t addr_cap, uint16_t *port) {
    if (!url || !addr || !port || addr_cap < 2) return -1;
    char buf[256];
    size_t n = url_len < sizeof(buf) - 1 ? url_len : sizeof(buf) - 1;
    memcpy(buf, url, n);
    buf[n] = '\0';
    const char *p = buf;
    if (strncmp(p, "http://", 7) == 0) p += 7;
    else if (strncmp(p, "https://", 8) == 0) p += 8;
    char *colon = strrchr(p, ':');
    if (colon) {
        *colon = '\0';
        *port = (uint16_t)atoi(colon + 1);
    } else {
        *port = 2380;
    }
    snprintf(addr, addr_cap, "%s", p);
    return 0;
}

/*
 * MemberAdd RPC.
 * Adds a new member to the cluster.
 */
cetcd_rpc_bytes cluster_handle_member_add(cetcd_v3rpc *rpc,
                                           const uint8_t *req, size_t req_len) {
    (void)rpc;
    size_t pos = 0;
    uint8_t *peer_url = NULL; size_t peer_url_len = 0;
    int is_learner = 0;

    while (pos < req_len) {
        uint8_t tag = req[pos++];
        if (tag == 0x0a) {
            /* peerURLs: repeated string */
            if (read_bytes_c(req, req_len, &pos, &peer_url, &peer_url_len) != 0) break;
        } else if (tag == 0x10) {
            /* isLearner: bool */
            uint64_t v = 0; read_varint_c(req, req_len, &pos, &v);
            is_learner = v ? 1 : 0;
        } else {
            uint64_t skip = 0; read_varint_c(req, req_len, &pos, &skip);
        }
    }

    /* If we have a cluster, add the peer through Raft (or locally). */
    uint64_t new_id = 0;
    if (g_rpc_cluster && peer_url) {
        if (g_rpc_raft && cetcd_raft_in_joint(g_rpc_raft)) {
            free(peer_url);
            return (cetcd_rpc_bytes){NULL, 0};
        }
        cetcd_peer_info info = {0};
        info.id = cetcd_cluster_alloc_id(g_rpc_cluster);
        info.is_learner = is_learner;
        if (parse_peer_url_((const char *)peer_url, peer_url_len,
                            info.addr, sizeof(info.addr), &info.port) != 0) {
            free(peer_url);
            return (cetcd_rpc_bytes){NULL, 0};
        }
        uint8_t *entry = NULL;
        size_t elen = 0;
        if (cetcd_apply_encode_member_add(&entry, &elen, info.id, info.is_learner,
                                          info.addr, info.port) != 0) {
            free(peer_url);
            return (cetcd_rpc_bytes){NULL, 0};
        }
        int rc = cetcd_v3rpc_propose_or_apply(entry, elen);
        free(entry);
        if (rc < 0 || !cetcd_cluster_get_peer(g_rpc_cluster, info.id)) {
            free(peer_url);
            return (cetcd_rpc_bytes){NULL, 0};
        }
        new_id = info.id;
    }
    if (peer_url) free(peer_url);

    /* MemberAddResponse: field 1 (header), field 2 (member) = Member */
    uint8_t buf[256];
    size_t bpos = 0;
    /* field 1 = header */
    {
        int64_t current_rev = g_rpc_store ? cetcd_mvcc_revision(g_rpc_store) : 0;
        uint8_t hdr_buf[16]; size_t hp = 0;
        hdr_buf[hp++] = 0x18;
        write_varint_c(hdr_buf, sizeof(hdr_buf), &hp, (uint64_t)(current_rev > 0 ? current_rev : 1));
        buf[bpos++] = 0x0a;
        write_varint_c(buf, sizeof(buf), &bpos, (uint64_t)hp);
        memcpy(buf + bpos, hdr_buf, hp); bpos += hp;
    }
    if (new_id > 0) {
        uint8_t member_buf[128];
        size_t mlen = encode_member(member_buf, sizeof(member_buf), new_id, "", is_learner);
        buf[bpos++] = 0x12;
        write_varint_c(buf, sizeof(buf), &bpos, (uint64_t)mlen);
        if (bpos + mlen < sizeof(buf)) {
            memcpy(buf + bpos, member_buf, mlen);
            bpos += mlen;
        }
    }
    if (bpos == 0) return make_simple_cluster_response();
    /* bpos > 0 means we have header + member data */
    uint8_t *out = (uint8_t *)malloc(bpos);
    if (!out) return (cetcd_rpc_bytes){NULL, 0};
    memcpy(out, buf, bpos);
    return (cetcd_rpc_bytes){out, bpos};
}

/*
 * MemberRemove RPC.
 * Removes a member from the cluster.
 */
cetcd_rpc_bytes cluster_handle_member_remove(cetcd_v3rpc *rpc,
                                              const uint8_t *req, size_t req_len) {
    (void)rpc;
    size_t pos = 0;
    uint64_t member_id = 0;

    while (pos < req_len) {
        uint8_t tag = req[pos++];
        if (tag == 0x08) {
            if (read_varint_c(req, req_len, &pos, &member_id) != 0) break;
        } else {
            uint64_t skip = 0; read_varint_c(req, req_len, &pos, &skip);
        }
    }

    if (g_rpc_cluster && member_id > 0) {
        if (g_rpc_raft && cetcd_raft_in_joint(g_rpc_raft))
            return (cetcd_rpc_bytes){NULL, 0};
        uint8_t *entry = NULL;
        size_t elen = 0;
        if (cetcd_apply_encode_member_remove(&entry, &elen, member_id) != 0)
            return (cetcd_rpc_bytes){NULL, 0};
        int rc = cetcd_v3rpc_propose_or_apply(entry, elen);
        free(entry);
        if (rc < 0) return (cetcd_rpc_bytes){NULL, 0};
    }

    return make_simple_cluster_response();
}

/*
 * MemberUpdate RPC.
 * Updates a member's peer URLs.
 */
cetcd_rpc_bytes cluster_handle_member_update(cetcd_v3rpc *rpc,
                                              const uint8_t *req, size_t req_len) {
    (void)rpc;
    size_t pos = 0;
    uint64_t member_id = 0;

    while (pos < req_len) {
        uint8_t tag = req[pos++];
        if (tag == 0x08) {
            if (read_varint_c(req, req_len, &pos, &member_id) != 0) break;
        } else if (tag == 0x12) {
            /* peerURLs: skip */
            uint64_t l = 0; read_varint_c(req, req_len, &pos, &l);
            pos += (size_t)l;
        } else {
            uint64_t skip = 0; read_varint_c(req, req_len, &pos, &skip);
        }
    }

    /* Update the peer's address and port in the cluster */
    if (g_rpc_cluster && member_id > 0) {
        /* Parse new peer URL from request */
        size_t pos2 = 0;
        char new_url[256] = {0};
        while (pos2 < req_len) {
            uint8_t tag = req[pos2++];
            if (tag == 0x08) {
                uint64_t v = 0; read_varint_c(req, req_len, &pos2, &v);
            } else if (tag == 0x12) {
                uint8_t *url = NULL; size_t url_len = 0;
                if (read_bytes_c(req, req_len, &pos2, &url, &url_len) == 0 && url) {
                    size_t copy_len = url_len < sizeof(new_url) - 1 ? url_len : sizeof(new_url) - 1;
                    memcpy(new_url, url, copy_len);
                    new_url[copy_len] = '\0';
                    free(url);
                }
            } else {
                uint64_t skip = 0; read_varint_c(req, req_len, &pos2, &skip);
            }
        }
        if (new_url[0]) {
            char addr[256];
            uint16_t port = 2380;
            if (parse_peer_url_(new_url, strlen(new_url), addr, sizeof(addr), &port) != 0)
                return make_simple_cluster_response();
            uint8_t *entry = NULL;
            size_t elen = 0;
            if (cetcd_apply_encode_member_update(&entry, &elen, member_id, addr, port) != 0)
                return (cetcd_rpc_bytes){NULL, 0};
            int rc = cetcd_v3rpc_propose_or_apply(entry, elen);
            free(entry);
            if (rc < 0) return (cetcd_rpc_bytes){NULL, 0};
        }
    }
    return make_simple_cluster_response();
}

/*
 * MemberPromote RPC.
 * Promotes a learner member to a voting member.
 */
cetcd_rpc_bytes cluster_handle_member_promote(cetcd_v3rpc *rpc,
                                               const uint8_t *req, size_t req_len) {
    (void)rpc;
    size_t pos = 0;
    uint64_t member_id = 0;

    while (pos < req_len) {
        uint8_t tag = req[pos++];
        if (tag == 0x08) {
            if (read_varint_c(req, req_len, &pos, &member_id) != 0) break;
        } else {
            uint64_t skip = 0; read_varint_c(req, req_len, &pos, &skip);
        }
    }

    /* Fail-closed: missing or already-voting member is an error. */
    if (g_rpc_cluster && member_id > 0) {
        if (g_rpc_raft && cetcd_raft_in_joint(g_rpc_raft))
            return (cetcd_rpc_bytes){NULL, 0};
        const cetcd_peer_info *cur = cetcd_cluster_get_peer(g_rpc_cluster, member_id);
        if (!cur || !cur->is_learner)
            return (cetcd_rpc_bytes){NULL, 0};
        uint8_t *entry = NULL;
        size_t elen = 0;
        if (cetcd_apply_encode_member_promote(&entry, &elen, member_id) != 0)
            return (cetcd_rpc_bytes){NULL, 0};
        int rc = cetcd_v3rpc_propose_or_apply(entry, elen);
        free(entry);
        if (rc < 0) return (cetcd_rpc_bytes){NULL, 0};
    }
    return make_simple_cluster_response();
}
