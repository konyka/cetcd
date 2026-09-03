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

/* Dispatch with an optional bearer token (NULL if none). When auth is
 * enabled, all RPCs except Authenticate require a valid unexpired token. */
cetcd_rpc_bytes cetcd_v3rpc_dispatch_ex(cetcd_v3rpc *rpc,
                                         const char *path,
                                         const uint8_t *req_data,
                                         size_t req_len,
                                         const char *token);

/* 0 if the current request user may access `key`; -1 if denied.
 * Always 0 when auth is disabled. */
CETCD_API int cetcd_v3rpc_check_key_perm(int want_write,
                                         const uint8_t *key, size_t key_len);

struct cetcd_backend;
CETCD_API void cetcd_v3rpc_set_auth_backend(cetcd_v3rpc *rpc,
                                            struct cetcd_backend *be);
CETCD_API void cetcd_v3rpc_auth_persist(void);

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

/* Compact raft-log KV ops (not protobuf). Performance-first: one byte tag
 * plus varints, applied only after WAL sync. */
#define CETCD_APPLY_PUT          1
#define CETCD_APPLY_DELETE       2
#define CETCD_APPLY_BATCH        3
#define CETCD_APPLY_DELETE_RANGE 4
#define CETCD_APPLY_MEMBER_ADD     5
#define CETCD_APPLY_MEMBER_REMOVE  6
#define CETCD_APPLY_MEMBER_PROMOTE 7
#define CETCD_APPLY_MEMBER_UPDATE  8
#define CETCD_APPLY_LEAVE_JOINT    9

CETCD_API int cetcd_apply_encode_put(uint8_t **out, size_t *out_len,
                                     const uint8_t *key, size_t key_len,
                                     const uint8_t *val, size_t val_len,
                                     int64_t lease_id);
CETCD_API int cetcd_apply_encode_delete(uint8_t **out, size_t *out_len,
                                        const uint8_t *key, size_t key_len);
CETCD_API int cetcd_apply_encode_delete_range(uint8_t **out, size_t *out_len,
                                              const uint8_t *key, size_t key_len,
                                              const uint8_t *range_end, size_t end_len);

/* Concatenate already-encoded Put/Delete/DeleteRange ops into one entry. */
CETCD_API int cetcd_apply_encode_batch(uint8_t **out, size_t *out_len,
                                       const uint8_t *const *ops,
                                       const size_t *op_lens, size_t n);

CETCD_API int cetcd_apply_encode_member_add(uint8_t **out, size_t *out_len,
                                            uint64_t id, int is_learner,
                                            const char *addr, uint16_t port);
CETCD_API int cetcd_apply_encode_member_remove(uint8_t **out, size_t *out_len,
                                               uint64_t id);
CETCD_API int cetcd_apply_encode_member_promote(uint8_t **out, size_t *out_len,
                                                uint64_t id);
CETCD_API int cetcd_apply_encode_member_update(uint8_t **out, size_t *out_len,
                                               uint64_t id,
                                               const char *addr, uint16_t port);

/* Apply a committed NORMAL entry to MVCC (+ lease index). 0 on success. */
CETCD_API int cetcd_v3rpc_apply_entry(const uint8_t *data, size_t len);

/* Flush Ready (persist WAL + apply). Set by the server reactor. */
typedef void (*cetcd_ready_flush_fn)(void *ctx);
CETCD_API void cetcd_v3rpc_set_ready_flush(cetcd_ready_flush_fn fn, void *ctx);

/* Propose `data` if Raft is attached and this node is leader, then flush.
 * Returns 0 if the entry is applied, 1 if the caller should apply locally
 * (no Raft), -1 on not-leader / propose / apply failure. */
CETCD_API int cetcd_v3rpc_propose_or_apply(const uint8_t *data, size_t len);

/* Accessors for server wiring (persistence, lease tick). */
CETCD_API struct cetcd_mvcc_store *cetcd_v3rpc_store(cetcd_v3rpc *rpc);
CETCD_API struct cetcd_lease_mgr  *cetcd_v3rpc_leases(cetcd_v3rpc *rpc);

#ifdef __cplusplus
}
#endif
#endif
