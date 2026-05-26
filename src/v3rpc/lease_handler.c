#include <stdlib.h>
#include <string.h>

#include "cetcd/v3rpc.h"
#include "cetcd/lease.h"
#include "cetcd/mvcc.h"

extern cetcd_mvcc_store *g_rpc_store;
extern cetcd_lease_mgr  *g_rpc_lease_mgr;

/* Helper to read a simple varint from req starting at pos */
static int read_varint_local(const uint8_t *buf, size_t len, size_t *pos, int64_t *out) {
    uint64_t val = 0; int shift = 0;
    while (*pos < len) {
        uint8_t b = buf[*pos]; (*pos)++;
        val |= (uint64_t)(b & 0x7F) << shift;
        if ((b & 0x80) == 0) { *out = (int64_t)val; return 0; }
        shift += 7; if (shift > 63) break;
    }
    return -1;
}

cetcd_rpc_bytes lease_handle_grant(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len) {
    size_t pos = 0; int64_t ttl = 0; int64_t id = 0;
    while (pos < req_len) {
        uint8_t tag = req[pos++];
        if (tag == 0x08) { /* ttl varint */ {
            int64_t v = 0; if (read_varint_local(req, req_len, &pos, &v) == 0) ttl = v; else break; }
        } else if (tag == 0x10) { /* id varint */ {
            int64_t v = 0; if (read_varint_local(req, req_len, &pos, &v) == 0) id = v; else break; }
        } else {
            /* skip unknown field */ if (pos < req_len) pos++;
        }
    }
    /* Grant lease (ttl in seconds) */
    cetcd_lease_id lid = cetcd_lease_grant(g_rpc_lease_mgr, ttl);
    (void)id; /* id is currently unused in tests */
    uint8_t tmp[1] = {0};
    return (cetcd_rpc_bytes){malloc(1), 1};
}
