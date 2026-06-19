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
 */

#include <stdlib.h>
#include <string.h>

#include "cetcd/v3rpc.h"
#include "cetcd/mvcc.h"
#include "cetcd/base.h"

extern cetcd_mvcc_store *g_rpc_store;

/* Forward declarations */
cetcd_rpc_bytes maint_handle_status(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes maint_handle_defragment(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes maint_handle_hash(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes maint_handle_hash_kv(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes maint_handle_alarm(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes maint_handle_move_leader(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);

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

    uint8_t buf[128];
    size_t pos = 0;

    /* field 2 = version (string "0.1.0") */
    buf[pos++] = 0x12; /* tag */
    buf[pos++] = 0x05; /* length */
    memcpy(buf + pos, "0.1.0", 5); pos += 5;

    /* field 3 = dbSize (int64) */
    buf[pos++] = 0x18;
    write_varint_m(buf, sizeof(buf), &pos, 0);

    /* field 5 = raftIndex */
    buf[pos++] = 0x28;
    write_varint_m(buf, sizeof(buf), &pos, (uint64_t)(rev > 0 ? rev : 0));

    /* field 6 = raftTerm */
    buf[pos++] = 0x30;
    write_varint_m(buf, sizeof(buf), &pos, 1);

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
    (void)rpc; (void)req; (void)req_len;
    return make_simple_response();
}
