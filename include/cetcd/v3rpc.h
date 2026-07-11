#ifndef CETCD_V3RPC_H_
#define CETCD_V3RPC_H_

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "cetcd/base.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct cetcd_v3rpc cetcd_v3rpc;
typedef struct cetcd_loop  cetcd_loop;   /* defined in cetcd/io.h */

typedef struct {
    uint8_t *data;
    size_t   len;
} cetcd_rpc_bytes;

/* Callback used by streaming RPC handlers (e.g. Watch) to write a
 * single raw protobuf message back to the client.  The callback is
 * responsible for framing the message if required by the transport. */
typedef void (*cetcd_stream_write_fn)(const uint8_t *data, size_t len, void *ctx);

cetcd_v3rpc *cetcd_v3rpc_new(void);
void         cetcd_v3rpc_free(cetcd_v3rpc *rpc);

/* Dispatch a single unary request and return a single response.
 * For streaming RPCs this returns the initial control response only
 * (e.g. WatchCreateRequest confirmation); subsequent messages are
 * emitted via the registered stream writer callback. */
cetcd_rpc_bytes cetcd_v3rpc_dispatch(cetcd_v3rpc *rpc,
                                      const char *path,
                                      const uint8_t *req_data,
                                      size_t req_len);

void cetcd_rpc_bytes_free(cetcd_rpc_bytes *b);

/* Streaming support: associate the event loop and a writer callback.
 * When these are set, the Watch handler runs in bidirectional streaming
 * mode; otherwise it falls back to legacy single-shot behaviour. */
CETCD_API void cetcd_v3rpc_set_loop(cetcd_v3rpc *rpc, cetcd_loop *loop);
CETCD_API void cetcd_v3rpc_set_stream_writer(cetcd_v3rpc *rpc,
                                              cetcd_stream_write_fn fn,
                                              void *ctx);

/* Cancel streaming watchers bound to a connection's write_ctx (e.g. on close).
 * Prevents use-after-free of the socket and stops event fan-out to dead peers. */
CETCD_API void cetcd_v3rpc_detach_stream_writer(void *write_ctx);

/* Drive periodic Watch progress_notify (call from server tick, ~100ms). */
CETCD_API void cetcd_v3rpc_watch_tick(void);

/* After successful compaction, cancel streaming watches whose start_rev
 * is strictly below compact_rev (etcd ErrCompacted on active watchers). */
CETCD_API void cetcd_v3rpc_watch_cancel_compacted(int64_t compact_rev);

/* Flush deferred Watch history replay (wake notify after create-ack). */
CETCD_API void cetcd_v3rpc_watch_flush_replay(void);

/* Accessors for server wiring (persistence, lease tick). */
CETCD_API struct cetcd_mvcc_store *cetcd_v3rpc_store(cetcd_v3rpc *rpc);
CETCD_API struct cetcd_lease_mgr  *cetcd_v3rpc_leases(cetcd_v3rpc *rpc);

#ifdef __cplusplus
}
#endif
#endif
