#ifndef CETCD_SERVER_H_
#define CETCD_SERVER_H_

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "cetcd/base.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct cetcd_server cetcd_server;

typedef struct cetcd_server_config {
    uint64_t node_id;
    char     data_dir[512];
    char     listen_addr[256];
    uint16_t listen_port;
    char     peer_addr[256];
    uint16_t peer_port;
    uint64_t election_tick;
    uint64_t heartbeat_tick;
    bool     auth_enabled;
} cetcd_server_config;

cetcd_server *cetcd_server_new(const cetcd_server_config *cfg);
void          cetcd_server_free(cetcd_server *srv);

/* Lifecycle */
int  cetcd_server_start(cetcd_server *srv);
void cetcd_server_stop(cetcd_server *srv);

/* Apply committed entries from Raft to MVCC store. Call after raft_ready. */
int  cetcd_server_apply(cetcd_server *srv);

/* Process a gRPC request: path + request bytes → response bytes.
   Synchronous dispatch through v3rpc. */
typedef struct {
    uint8_t *data;
    size_t   len;
} cetcd_server_rpc_result;

cetcd_server_rpc_result cetcd_server_handle_rpc(cetcd_server *srv,
                                                  const char *path,
                                                  const uint8_t *req,
                                                  size_t req_len);
void cetcd_server_rpc_result_free(cetcd_server_rpc_result *r);

/* Tick the Raft state machine (call periodically). */
void cetcd_server_tick(cetcd_server *srv);

/* Compact MVCC store up to the given revision. */
int  cetcd_server_compact(cetcd_server *srv, int64_t rev);

/* Take a snapshot of the current MVCC state. */
typedef struct cetcd_snap cetcd_snap;
cetcd_snap *cetcd_server_snapshot(cetcd_server *srv);

/* Start a blocking gRPC listener on listen_addr:listen_port.
   Accepts connections, reads length-prefixed gRPC frames,
   dispatches to v3rpc, sends responses back.
   Returns when cetcd_server_stop is called from another thread. */
int cetcd_server_serve(cetcd_server *srv);

/* Queries */
int64_t cetcd_server_revision(const cetcd_server *srv);
bool    cetcd_server_is_leader(const cetcd_server *srv);
uint64_t cetcd_server_node_id(const cetcd_server *srv);

#ifdef __cplusplus
}
#endif
#endif
