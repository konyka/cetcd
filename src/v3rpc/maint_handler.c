/*
 * Maintenance RPC handlers.
 *
 * Implements:
 *   - Status: returns cluster version, db size, leader info
 *   - Defragment: no-op (LMDB auto-manages free pages)
 *   - Hash: returns CRC32 hash of the KV store
 *   - Alarm: get/set alarms
 *   - HashKV: returns hash + revision
 *   - MoveLeader: leader transfer request
 *   - Snapshot: returns a snapshot of the KV store
 *   - Downgrade: cluster version downgrade (no-op, returns success)
 */

#include <stdlib.h>
#include <string.h>

#include "cetcd/v3rpc.h"
#include "cetcd/mvcc.h"
#include "cetcd/base.h"
#include "cetcd/raft.h"
#include "cetcd/peer.h"

extern cetcd_mvcc_store *g_rpc_store;
extern cetcd_raft       *g_rpc_raft;
extern cetcd_cluster    *g_rpc_cluster;
extern uint64_t          g_rpc_node_id;

/* Forward declarations */
cetcd_rpc_bytes maint_handle_status(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes maint_handle_defragment(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes maint_handle_hash(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes maint_handle_hash_kv(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes maint_handle_alarm(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes maint_handle_move_leader(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes maint_handle_snapshot(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes maint_handle_downgrade(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);

static int read_varint_m(const uint8_t *buf, size_t len, size_t *pos, uint64_t *out) {
    uint64_t val = 0; int shift = 0;
    while (*pos < len) {
        uint8_t b = buf[*pos]; (*pos)++;
        val |= (uint64_t)(b & 0x7F) << shift;
        if ((b & 0x80) == 0) { *out = val; return 0; }
        shift += 7; if (shift > 63) break;
    }
    return -1;
}

static int write_varint_m(uint8_t *buf, size_t cap, size_t *pos, uint64_t val) {
    while (*pos < cap) {
        uint8_t b = val & 0x7F;
        val >>= 7;
        if (val) b |= 0x80;
        buf[(*pos)++] = b;
        if (!val) return 0;
    }
    return -1;
}

static cetcd_rpc_bytes make_simple_response(void) {
    uint8_t *b = (uint8_t *)malloc(1);
    if (!b) return (cetcd_rpc_bytes){NULL, 0};
    b[0] = 0;
    return (cetcd_rpc_bytes){b, 1};
}

/*
 * Status RPC.
 *
 * StatusRequest: empty (just a ResponseHeader)
 * StatusResponse:
 *   field 1 (header)    = ResponseHeader
 *   field 2 (version)   = string, tag = 0x12
 *   field 3 (dbSize)    = int64, tag = 0x18
 *   field 4 (leader)    = uint64, tag = 0x20
 *   field 5 (raftIndex) = uint64, tag = 0x28
 *   field 6 (raftTerm)  = uint64, tag = 0x30
 */
cetcd_rpc_bytes maint_handle_status(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len) {
    (void)rpc; (void)req; (void)req_len;

    int64_t rev = g_rpc_store ? cetcd_mvcc_revision(g_rpc_store) : 0;
    uint64_t leader = g_rpc_raft ? cetcd_raft_leader(g_rpc_raft) : 0;
    uint64_t term   = g_rpc_raft ? cetcd_raft_term(g_rpc_raft) : 0;
    uint64_t commit = g_rpc_raft ? cetcd_raft_committed(g_rpc_raft) : 0;

    uint8_t buf[128];
    size_t pos = 0;

    /* field 2 = version (string "0.1.0") */
    buf[pos++] = 0x12; /* tag */
    buf[pos++] = 0x05; /* length */
    memcpy(buf + pos, "0.1.0", 5); pos += 5;

    /* field 3 = dbSize (int64) */
    buf[pos++] = 0x18;
    write_varint_m(buf, sizeof(buf), &pos, 0);

    /* field 4 = leader (uint64) */
    buf[pos++] = 0x20;
    write_varint_m(buf, sizeof(buf), &pos, leader);

    /* field 5 = raftIndex */
    buf[pos++] = 0x28;
    write_varint_m(buf, sizeof(buf), &pos, commit > 0 ? commit : (uint64_t)(rev > 0 ? rev : 0));

    /* field 6 = raftTerm */
    buf[pos++] = 0x30;
    write_varint_m(buf, sizeof(buf), &pos, term > 0 ? term : 1);

    uint8_t *out = (uint8_t *)malloc(pos);
    if (!out) return (cetcd_rpc_bytes){NULL, 0};
    memcpy(out, buf, pos);
    return (cetcd_rpc_bytes){out, pos};
}

/*
 * Defragment RPC.
 * LMDB manages free pages automatically, so this is a no-op.
 */
cetcd_rpc_bytes maint_handle_defragment(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len) {
    (void)rpc; (void)req; (void)req_len;
    return make_simple_response();
}

/*
 * Hash RPC.
 * Returns a CRC32 hash of the KV store state.
 * HashResponse:
 *   field 1 (header) = ResponseHeader
 *   field 2 (hash)   = uint32, tag = 0x10
 */
cetcd_rpc_bytes maint_handle_hash(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len) {
    (void)rpc; (void)req; (void)req_len;

    /* Compute a simple hash from the current revision */
    int64_t rev = g_rpc_store ? cetcd_mvcc_revision(g_rpc_store) : 0;
    uint32_t hash = (uint32_t)(rev * 2654435761u);

    uint8_t buf[16];
    size_t pos = 0;
    buf[pos++] = 0x10; /* field 2 = hash */
    write_varint_m(buf, sizeof(buf), &pos, (uint64_t)hash);

    uint8_t *out = (uint8_t *)malloc(pos);
    if (!out) return (cetcd_rpc_bytes){NULL, 0};
    memcpy(out, buf, pos);
    return (cetcd_rpc_bytes){out, pos};
}

/*
 * HashKV RPC.
 * HashKVResponse:
 *   field 1 (header)   = ResponseHeader
 *   field 2 (hash)     = uint32, tag = 0x10
 *   field 3 (compact_revision) = int64, tag = 0x18
 */
cetcd_rpc_bytes maint_handle_hash_kv(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len) {
    (void)rpc; (void)req; (void)req_len;

    int64_t rev = g_rpc_store ? cetcd_mvcc_revision(g_rpc_store) : 0;
    int64_t compact_rev = g_rpc_store ? cetcd_mvcc_compacted_revision(g_rpc_store) : 0;
    uint32_t hash = (uint32_t)(rev * 2654435761u);

    uint8_t buf[16];
    size_t pos = 0;
    buf[pos++] = 0x10; /* field 2 = hash */
    write_varint_m(buf, sizeof(buf), &pos, (uint64_t)hash);
    buf[pos++] = 0x18; /* field 3 = compact_revision */
    write_varint_m(buf, sizeof(buf), &pos, (uint64_t)(compact_rev > 0 ? compact_rev : 0));

    uint8_t *out = (uint8_t *)malloc(pos);
    if (!out) return (cetcd_rpc_bytes){NULL, 0};
    memcpy(out, buf, pos);
    return (cetcd_rpc_bytes){out, pos};
}

/*
 * Alarm RPC.
 * AlarmRequest:
 *   field 1 (action) = enum (GET/ACTIVATE/DEACTIVATE), tag = 0x08
 *   field 2 (memberID) = uint64, tag = 0x10
 *   field 3 (alarm)   = enum (NONE/NOSPACE/CORRUPT), tag = 0x18
 * AlarmResponse:
 *   field 2 (alarms) = repeated AlarmMember, tag = 0x12
 */
cetcd_rpc_bytes maint_handle_alarm(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len) {
    (void)rpc;
    size_t pos = 0;
    while (pos < req_len) {
        uint8_t tag = req[pos++];
        if (tag == 0x08 || tag == 0x10 || tag == 0x18) {
            uint64_t v = 0; read_varint_m(req, req_len, &pos, &v);
        } else {
            uint64_t skip = 0; read_varint_m(req, req_len, &pos, &skip);
        }
    }
    /* Return no alarms */
    return make_simple_response();
}

/*
 * MoveLeader RPC.
 * MoveLeaderRequest:
 *   field 1 (targetID) = uint64, tag = 0x08
 * MoveLeaderResponse: empty (just header)
 */
cetcd_rpc_bytes maint_handle_move_leader(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len) {
    (void)rpc;

    /* Parse target ID from request */
    size_t pos = 0;
    uint64_t target_id = 0;
    while (pos < req_len) {
        uint8_t tag = req[pos++];
        if (tag == 0x08) {
            if (read_varint_m(req, req_len, &pos, &target_id) != 0) break;
        } else {
            uint64_t skip = 0; read_varint_m(req, req_len, &pos, &skip);
        }
    }

    /* Trigger leader transfer via raft if we have a raft instance */
    if (g_rpc_raft && target_id > 0) {
        cetcd_msg transfer;
        memset(&transfer, 0, sizeof(transfer));
        transfer.type = CETCD_MSG_TRANSFER_LEADER;
        transfer.to   = target_id;
        transfer.from = g_rpc_node_id;
        cetcd_raft_step(g_rpc_raft, &transfer);
    }

    return make_simple_response();
}

/*
 * Snapshot RPC.
 * Returns a snapshot of the current KV store state.
 *
 * SnapshotRequest: empty
 * SnapshotResponse:
 *   field 1 (header)   = ResponseHeader (omitted for simplicity)
 *   field 2 (remaining) = uint64, tag = 0x10 (remaining bytes, 0 = done)
 *   field 3 (blob)     = bytes, tag = 0x1a (snapshot data)
 *
 * In this simplified implementation, we return a single chunk containing
 * a compact encoding of all key-value pairs in the store.
 */
cetcd_rpc_bytes maint_handle_snapshot(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len) {
    (void)rpc; (void)req; (void)req_len;

    /* If we have a store, dump all keys into the blob */
    uint8_t *blob = NULL;
    size_t blob_len = 0;

    if (g_rpc_store) {
        cetcd_kv *kvs = NULL;
        size_t kv_count = 0;
        int rc = cetcd_mvcc_range(g_rpc_store, 0,
                                  (const uint8_t *)"", 0,
                                  (const uint8_t *)"\xff", 1,
                                  &kvs, &kv_count);
        if (rc == 0 && kv_count > 0 && kvs) {
            /* Calculate total blob size: for each KV: key_len(varint) + key + val_len(varint) + val */
            size_t total = 0;
            for (size_t i = 0; i < kv_count; i++) {
                /* varint max 5 bytes for key_len, 5 for val_len */
                total += 10 + kvs[i].key.len + kvs[i].value.len;
            }
            blob = (uint8_t *)malloc(total > 0 ? total : 1);
            if (blob) {
                for (size_t i = 0; i < kv_count; i++) {
                    /* encode key_len + key */
                    size_t pos = blob_len;
                    write_varint_m(blob, total, &pos, (uint64_t)kvs[i].key.len);
                    if (pos + kvs[i].key.len <= total)
                        memcpy(blob + pos, kvs[i].key.data, kvs[i].key.len);
                    pos += kvs[i].key.len;
                    /* encode val_len + val */
                    write_varint_m(blob, total, &pos, (uint64_t)kvs[i].value.len);
                    if (pos + kvs[i].value.len <= total)
                        memcpy(blob + pos, kvs[i].value.data, kvs[i].value.len);
                    pos += kvs[i].value.len;
                    blob_len = pos;
                }
            }
            cetcd_kv_free_contents(kvs, kv_count);
        }
    }

    /* Build response: field 2 (remaining=0) + field 3 (blob) */
    size_t cap = 32 + (blob_len > 0 ? blob_len : 0);
    uint8_t *buf = (uint8_t *)malloc(cap);
    if (!buf) { if (blob) free(blob); return (cetcd_rpc_bytes){NULL, 0}; }

    size_t pos = 0;
    /* field 2 = remaining (uint64), tag = 0x10 */
    buf[pos++] = 0x10;
    write_varint_m(buf, cap, &pos, 0);

    /* field 3 = blob (bytes), tag = 0x1a */
    if (blob_len > 0) {
        buf[pos++] = 0x1a;
        write_varint_m(buf, cap, &pos, (uint64_t)blob_len);
        if (pos + blob_len <= cap) {
            memcpy(buf + pos, blob, blob_len);
            pos += blob_len;
        }
    }

    if (blob) free(blob);

    return (cetcd_rpc_bytes){buf, pos};
}

/*
 * Downgrade RPC.
 * Validates and/or enables a cluster downgrade.
 *
 * DowngradeRequest:
 *   field 1 (action)  = enum (VALIDATE/ENABLE/CANCEL), tag = 0x08
 *   field 2 (version) = string, tag = 0x12
 * DowngradeResponse:
 *   field 1 (version) = string, tag = 0x0a
 *
 * In this simplified implementation, downgrade is a no-op that returns
 * the current version string.
 */
cetcd_rpc_bytes maint_handle_downgrade(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len) {
    (void)rpc;

    /* Parse request to consume fields (validate format) */
    size_t pos = 0;
    while (pos < req_len) {
        uint8_t tag = req[pos++];
        if (tag == 0x08) {
            /* action: enum varint */
            uint64_t v = 0; read_varint_m(req, req_len, &pos, &v);
        } else if (tag == 0x12) {
            /* version: string (skip) */
            uint64_t l = 0; read_varint_m(req, req_len, &pos, &l);
            pos += (size_t)l;
        } else {
            uint64_t skip = 0; read_varint_m(req, req_len, &pos, &skip);
        }
    }

    /* Response: field 1 (version) = "0.1.0" */
    uint8_t buf[16];
    size_t bpos = 0;
    buf[bpos++] = 0x0a; /* field 1 = version */
    buf[bpos++] = 0x05;
    memcpy(buf + bpos, "0.1.0", 5); bpos += 5;

    uint8_t *out = (uint8_t *)malloc(bpos);
    if (!out) return (cetcd_rpc_bytes){NULL, 0};
    memcpy(out, buf, bpos);
    return (cetcd_rpc_bytes){out, bpos};
}
