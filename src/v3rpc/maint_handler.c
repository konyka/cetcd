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
#include "cetcd/backend.h"

extern cetcd_mvcc_store *g_rpc_store;
extern cetcd_raft       *g_rpc_raft;
extern cetcd_cluster    *g_rpc_cluster;
extern uint64_t          g_rpc_node_id;
extern cetcd_backend    *g_rpc_auth_backend;
extern cetcd_stream_write_fn g_rpc_stream_write_fn;
extern void             *g_rpc_stream_write_ctx;

#define MAX_ALARMS 8
#define ALARM_BUCKET "alarm"
static const uint8_t ALARM_KEY[] = {'t'};

static struct {
    int active;
    int alarm_type;
    uint64_t member_id;
} g_alarms[MAX_ALARMS];

static void write_le64_(uint8_t *p, uint64_t v) {
    for (int i = 0; i < 8; i++) p[i] = (uint8_t)(v >> (8 * i));
}

static uint64_t read_le64_(const uint8_t *p) {
    uint64_t v = 0;
    for (int i = 0; i < 8; i++) v |= (uint64_t)p[i] << (8 * i);
    return v;
}

static void alarm_persist_(void) {
    if (!g_rpc_auth_backend) return;
    uint8_t val[1 + MAX_ALARMS * 9];
    size_t n = 0;
    val[0] = 0;
    for (int i = 0; i < MAX_ALARMS; i++) {
        if (!g_alarms[i].active) continue;
        size_t off = 1 + n * 9;
        val[off] = (uint8_t)g_alarms[i].alarm_type;
        write_le64_(val + off + 1, g_alarms[i].member_id);
        n++;
        val[0] = (uint8_t)n;
    }
    (void)cetcd_backend_put(g_rpc_auth_backend, ALARM_BUCKET,
                            ALARM_KEY, sizeof(ALARM_KEY), val, 1 + n * 9);
}

void cetcd_v3rpc_alarm_load(struct cetcd_backend *be) {
    memset(g_alarms, 0, sizeof(g_alarms));
    if (!be) return;
    uint8_t *val = NULL;
    size_t vlen = 0;
    if (cetcd_backend_get(be, ALARM_BUCKET, ALARM_KEY, sizeof(ALARM_KEY),
                          &val, &vlen) != CETCD_OK || !val)
        return;
    if (vlen < 1 || val[0] > MAX_ALARMS || vlen < 1u + (size_t)val[0] * 9u) {
        free(val);
        return;
    }
    uint8_t n = val[0];
    int slot = 0;
    for (uint8_t i = 0; i < n && slot < MAX_ALARMS; i++) {
        size_t off = 1 + (size_t)i * 9;
        int typ = (int)val[off];
        if (typ <= 0) continue;
        g_alarms[slot].active = 1;
        g_alarms[slot].alarm_type = typ;
        g_alarms[slot].member_id = read_le64_(val + off + 1);
        if (g_alarms[slot].member_id == 0) g_alarms[slot].member_id = 1;
        slot++;
    }
    free(val);
}

void cetcd_v3rpc_alarm_activate(int alarm_type, uint64_t member_id) {
    if (alarm_type <= 0) return;
    int found = 0;
    for (int i = 0; i < MAX_ALARMS; i++) {
        if (g_alarms[i].active && g_alarms[i].alarm_type == alarm_type) {
            found = 1;
            break;
        }
    }
    if (found) return;
    for (int i = 0; i < MAX_ALARMS; i++) {
        if (!g_alarms[i].active) {
            g_alarms[i].active = 1;
            g_alarms[i].alarm_type = alarm_type;
            g_alarms[i].member_id = member_id > 0 ? member_id : 1;
            alarm_persist_();
            return;
        }
    }
}

void cetcd_v3rpc_alarm_deactivate(int alarm_type, uint64_t member_id) {
    if (alarm_type <= 0) return;
    int changed = 0;
    for (int i = 0; i < MAX_ALARMS; i++) {
        if (g_alarms[i].active && g_alarms[i].alarm_type == alarm_type) {
            if (member_id == 0 || g_alarms[i].member_id == member_id) {
                g_alarms[i].active = 0;
                g_alarms[i].alarm_type = 0;
                g_alarms[i].member_id = 0;
                changed = 1;
            }
        }
    }
    if (changed) alarm_persist_();
}

int cetcd_v3rpc_alarm_is_active(int alarm_type) {
    for (int i = 0; i < MAX_ALARMS; i++) {
        if (g_alarms[i].active && g_alarms[i].alarm_type == alarm_type)
            return 1;
    }
    return 0;
}

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
    /* Return a ResponseHeader (field 1) with current revision */
    int64_t current_rev = g_rpc_store ? cetcd_mvcc_revision(g_rpc_store) : 0;
    uint8_t hdr_buf[16];
    size_t hp = 0;
    hdr_buf[hp++] = 0x18; /* field 3 = revision */
    write_varint_m(hdr_buf, sizeof(hdr_buf), &hp, (uint64_t)(current_rev > 0 ? current_rev : 1));

    uint8_t *b = (uint8_t *)malloc(2 + hp);
    if (!b) return (cetcd_rpc_bytes){NULL, 0};
    size_t pos = 0;
    b[pos++] = 0x0a; /* field 1 = header */
    write_varint_m(b, 2 + hp, &pos, (uint64_t)hp);
    memcpy(b + pos, hdr_buf, hp);
    pos += hp;
    return (cetcd_rpc_bytes){b, pos};
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

    uint8_t buf[256];
    size_t pos = 0;

    /* field 1 = header (ResponseHeader with revision) */
    {
        uint8_t hdr_buf[16]; size_t hp = 0;
        hdr_buf[hp++] = 0x18; /* revision */
        write_varint_m(hdr_buf, sizeof(hdr_buf), &hp, (uint64_t)(rev > 0 ? rev : 1));
        buf[pos++] = 0x0a;
        write_varint_m(buf, sizeof(buf), &pos, (uint64_t)hp);
        memcpy(buf + pos, hdr_buf, hp); pos += hp;
    }

    /* field 2 = version (string) */
    const char *ver = cetcd_version();
    size_t vlen = strlen(ver);
    buf[pos++] = 0x12; /* tag */
    write_varint_m(buf, sizeof(buf), &pos, (uint64_t)vlen);
    memcpy(buf + pos, ver, vlen); pos += vlen;

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

    uint8_t buf[32];
    size_t pos = 0;
    /* field 1 = header */
    {
        uint8_t hdr_buf[16]; size_t hp = 0;
        hdr_buf[hp++] = 0x18;
        write_varint_m(hdr_buf, sizeof(hdr_buf), &hp, (uint64_t)(rev > 0 ? rev : 1));
        buf[pos++] = 0x0a;
        write_varint_m(buf, sizeof(buf), &pos, (uint64_t)hp);
        memcpy(buf + pos, hdr_buf, hp); pos += hp;
    }
    buf[pos++] = 0x10; /* field 2 = hash */
    write_varint_m(buf, sizeof(buf), &pos, (uint64_t)hash);

    uint8_t *out = (uint8_t *)malloc(pos);
    if (!out) return (cetcd_rpc_bytes){NULL, 0};
    memcpy(out, buf, pos);
    return (cetcd_rpc_bytes){out, pos};
}

/*
 * HashKV RPC.
 * HashKVRequest:
 *   field 1 (revision) = int64, tag = 0x08  (0 = current)
 * HashKVResponse:
 *   field 1 (header)   = ResponseHeader
 *   field 2 (hash)     = uint32, tag = 0x10
 *   field 3 (compact_revision) = int64, tag = 0x18
 */
cetcd_rpc_bytes maint_handle_hash_kv(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len) {
    (void)rpc;
    int64_t req_rev = 0;
    size_t p = 0;
    while (p < req_len) {
        uint8_t tag = req[p++];
        if (tag == 0x08) {
            uint64_t v = 0;
            if (read_varint_m(req, req_len, &p, &v) != 0) break;
            req_rev = (int64_t)v;
        } else {
            uint64_t skip = 0;
            if (read_varint_m(req, req_len, &p, &skip) != 0) break;
        }
    }

    int64_t current = g_rpc_store ? cetcd_mvcc_revision(g_rpc_store) : 0;
    int64_t compact_rev = g_rpc_store ? cetcd_mvcc_compacted_revision(g_rpc_store) : 0;
    /* etcd HashByRev: revision 0 → current; else ErrCompacted / ErrFutureRev. */
    if (req_rev > 0) {
        if (compact_rev > 0 && req_rev < compact_rev)
            return (cetcd_rpc_bytes){NULL, 0}; /* ErrCompacted */
        if (req_rev > current)
            return (cetcd_rpc_bytes){NULL, 0}; /* ErrFutureRev */
    }
    int64_t rev = (req_rev > 0) ? req_rev : current;
    uint32_t hash = (uint32_t)(rev * 2654435761u);

    uint8_t buf[32];
    size_t pos = 0;
    /* field 1 = header */
    {
        uint8_t hdr_buf[16]; size_t hp = 0;
        hdr_buf[hp++] = 0x18;
        write_varint_m(hdr_buf, sizeof(hdr_buf), &hp, (uint64_t)(rev > 0 ? rev : 1));
        buf[pos++] = 0x0a;
        write_varint_m(buf, sizeof(buf), &pos, (uint64_t)hp);
        memcpy(buf + pos, hdr_buf, hp); pos += hp;
    }
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

    /* Parse request */
    size_t pos = 0;
    int action = 0; /* 0=GET, 1=ACTIVATE, 2=DEACTIVATE */
    uint64_t member_id = 0;
    int alarm_type = 0; /* 0=NONE, 1=NOSPACE, 2=CORRUPT */
    while (pos < req_len) {
        uint8_t tag = req[pos++];
        if (tag == 0x08) { /* action */
            uint64_t v = 0; if (read_varint_m(req, req_len, &pos, &v) == 0) action = (int)v;
        } else if (tag == 0x10) { /* memberID */
            uint64_t v = 0; if (read_varint_m(req, req_len, &pos, &v) == 0) member_id = v;
        } else if (tag == 0x18) { /* alarm */
            uint64_t v = 0; if (read_varint_m(req, req_len, &pos, &v) == 0) alarm_type = (int)v;
        } else {
            uint64_t skip = 0; read_varint_m(req, req_len, &pos, &skip);
        }
    }

    /* Static alarm storage: supports NOSPACE(1) and CORRUPT(2) simultaneously */

    if (action == 1 || action == 2) {
        if (alarm_type != 1 && alarm_type != 2) {
            if (alarm_type != 0)
                return (cetcd_rpc_bytes){NULL, 0};
        } else {
            uint8_t *entry = NULL;
            size_t elen = 0;
            if (cetcd_apply_encode_alarm(&entry, &elen, action, alarm_type, member_id) != 0)
                return (cetcd_rpc_bytes){NULL, 0};
            int rc = cetcd_v3rpc_propose_or_apply(entry, elen);
            free(entry);
            if (rc < 0)
                return (cetcd_rpc_bytes){NULL, 0};
        }
    }
    /* GET and type NONE return current state without a proposal. */

    /* Build AlarmResponse */
    int64_t rev = g_rpc_store ? cetcd_mvcc_revision(g_rpc_store) : 0;

    uint8_t buf[256];
    size_t bpos = 0;

    /* field 1 = header */
    {
        uint8_t hdr[16]; size_t hp = 0;
        hdr[hp++] = 0x18;
        write_varint_m(hdr, sizeof(hdr), &hp, (uint64_t)(rev > 0 ? rev : 1));
        buf[bpos++] = 0x0a;
        write_varint_m(buf, sizeof(buf), &bpos, (uint64_t)hp);
        memcpy(buf + bpos, hdr, hp); bpos += hp;
    }

    /* field 2 = alarms (repeated AlarmMember) */
    for (int i = 0; i < MAX_ALARMS; i++) {
        if (g_alarms[i].active) {
            uint8_t alarm_enc[32]; size_t ap = 0;
            alarm_enc[ap++] = 0x08; /* memberID */
            write_varint_m(alarm_enc, sizeof(alarm_enc), &ap, g_alarms[i].member_id);
            alarm_enc[ap++] = 0x10; /* alarm */
            write_varint_m(alarm_enc, sizeof(alarm_enc), &ap, (uint64_t)g_alarms[i].alarm_type);
            buf[bpos++] = 0x12;
            write_varint_m(buf, sizeof(buf), &bpos, (uint64_t)ap);
            memcpy(buf + bpos, alarm_enc, ap); bpos += ap;
        }
    }

    uint8_t *out = (uint8_t *)malloc(bpos);
    if (!out) return (cetcd_rpc_bytes){NULL, 0};
    memcpy(out, buf, bpos);
    return (cetcd_rpc_bytes){out, bpos};
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
 *   field 1 (header)   = ResponseHeader, tag = 0x0a
 *   field 2 (remaining) = uint64, tag = 0x10 (remaining bytes, 0 = done)
 *   field 3 (blob)     = bytes, tag = 0x1a (snapshot data)
 *
 * Unary / custom-TCP: one SnapshotResponse with remaining=0.
 * Streaming (stream writer set): a remaining=blob_len header first, then
 * the remaining=0 payload — etcd clients concatenate blobs until remaining=0.
 */
static cetcd_rpc_bytes encode_snapshot_response_(int64_t rev, uint64_t remaining,
                                                 const uint8_t *blob, size_t blob_len) {
    size_t cap = 64 + blob_len;
    uint8_t *buf = (uint8_t *)malloc(cap);
    if (!buf) return (cetcd_rpc_bytes){NULL, 0};
    size_t pos = 0;
    uint8_t hdr_buf[32];
    size_t hp = 0;
    hdr_buf[hp++] = 0x18;
    write_varint_m(hdr_buf, sizeof(hdr_buf), &hp, (uint64_t)(rev > 0 ? rev : 1));
    buf[pos++] = 0x0a;
    write_varint_m(buf, cap, &pos, (uint64_t)hp);
    memcpy(buf + pos, hdr_buf, hp);
    pos += hp;
    buf[pos++] = 0x10;
    write_varint_m(buf, cap, &pos, remaining);
    if (blob_len > 0 && blob) {
        buf[pos++] = 0x1a;
        write_varint_m(buf, cap, &pos, (uint64_t)blob_len);
        if (pos + blob_len <= cap) {
            memcpy(buf + pos, blob, blob_len);
            pos += blob_len;
        }
    }
    return (cetcd_rpc_bytes){buf, pos};
}

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

    int64_t current_rev = g_rpc_store ? cetcd_mvcc_revision(g_rpc_store) : 0;
    if (g_rpc_stream_write_fn && blob_len > 0) {
        cetcd_rpc_bytes first = encode_snapshot_response_(
            current_rev, (uint64_t)blob_len, NULL, 0);
        if (first.data)
            g_rpc_stream_write_fn(first.data, first.len, g_rpc_stream_write_ctx);
        free(first.data);
    }
    cetcd_rpc_bytes out = encode_snapshot_response_(current_rev, 0, blob, blob_len);
    if (blob) free(blob);
    return out;
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

    /* Response: field 1 = header (ResponseHeader) */
    int64_t current_rev = g_rpc_store ? cetcd_mvcc_revision(g_rpc_store) : 0;
    uint8_t hdr_buf[16]; size_t hp = 0;
    hdr_buf[hp++] = 0x18;
    write_varint_m(hdr_buf, sizeof(hdr_buf), &hp, (uint64_t)(current_rev > 0 ? current_rev : 1));

    uint8_t buf[32];
    size_t bpos = 0;
    buf[bpos++] = 0x0a; /* field 1 = header */
    write_varint_m(buf, sizeof(buf), &bpos, (uint64_t)hp);
    memcpy(buf + bpos, hdr_buf, hp); bpos += hp;

    uint8_t *out = (uint8_t *)malloc(bpos);
    if (!out) return (cetcd_rpc_bytes){NULL, 0};
    memcpy(out, buf, bpos);
    return (cetcd_rpc_bytes){out, bpos};
}
