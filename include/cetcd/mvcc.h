#ifndef CETCD_MVCC_H_
#define CETCD_MVCC_H_

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "cetcd/base.h"
#include "cetcd/slice.h"

/* Forward declare backend to avoid a hard include cycle in public headers. */
typedef struct cetcd_backend cetcd_backend;

#ifdef __cplusplus
extern "C" {
#endif

/*
 * MVCC revision: (main, sub) pair.
 *   main = monotonically increasing transaction counter
 *   sub  = operation counter within a transaction
 * Matches etcd's mvccpb.KeyValue.mod_revision / create_revision.
 */
typedef struct cetcd_revision {
    int64_t main;
    int64_t sub;
} cetcd_revision;

static inline bool cetcd_revision_eq(cetcd_revision a, cetcd_revision b) {
    return a.main == b.main && a.sub == b.sub;
}
static inline bool cetcd_revision_lt(cetcd_revision a, cetcd_revision b) {
    return a.main < b.main || (a.main == b.main && a.sub < b.sub);
}

/* Key-Value with revision metadata */
typedef struct cetcd_kv {
    cetcd_slice    key;
    cetcd_slice    value;
    cetcd_revision create_rev;
    cetcd_revision mod_rev;
    int64_t        version;   /* increments on each update to same key */
    int64_t        lease_id;  /* 0 = no lease */
} cetcd_kv;

/* Watch event types */
typedef enum cetcd_event_type {
    CETCD_EVENT_PUT    = 0,
    CETCD_EVENT_DELETE = 1,
} cetcd_event_type;

typedef struct cetcd_watch_event {
    cetcd_event_type type;
    cetcd_kv         kv;
    cetcd_revision   rev;
    cetcd_kv         prev_kv;   /* previous value (for PUT updates / DELETE) */
    int              has_prev_kv;
} cetcd_watch_event;

/* Forward declarations */
typedef struct cetcd_mvcc_store cetcd_mvcc_store;
typedef struct cetcd_watcher   cetcd_watcher;

/* --- Store lifecycle --- */

cetcd_mvcc_store *cetcd_mvcc_store_new(void);
void              cetcd_mvcc_store_free(cetcd_mvcc_store *s);
int64_t           cetcd_mvcc_revision(const cetcd_mvcc_store *s);

/* --- Write operations (advance revision) --- */

/* Put a key-value pair. Returns the new revision. */
cetcd_revision cetcd_mvcc_put(cetcd_mvcc_store *s,
                               const uint8_t *key, size_t key_len,
                               const uint8_t *val, size_t val_len,
                               int64_t lease_id);

/* Delete a key. Returns (main, count) where count = number of keys deleted. */
cetcd_revision cetcd_mvcc_delete(cetcd_mvcc_store *s,
                                  const uint8_t *key, size_t key_len);

/* Delete many keys with one LMDB transaction (memory updates still per-key).
 * Returns the last non-zero revision produced, or {0,0} if none deleted. */
cetcd_revision cetcd_mvcc_delete_keys(cetcd_mvcc_store *s,
                                       const uint8_t *const *keys,
                                       const size_t *key_lens,
                                       size_t n);

/* --- Read operations (at a given revision, 0 = current) --- */

int cetcd_mvcc_get(cetcd_mvcc_store *s, int64_t rev,
                    const uint8_t *key, size_t key_len,
                    cetcd_kv *out);

/* Range: returns count of results. Caller frees out[] via cetcd_kv_free_contents.
 * range_end of a single '\0' byte means all keys >= key_start (etcd FromKey). */
int cetcd_mvcc_range(cetcd_mvcc_store *s, int64_t rev,
                      const uint8_t *key_start, size_t start_len,
                      const uint8_t *key_end, size_t end_len,
                      cetcd_kv **out, size_t *out_count);

void cetcd_kv_free_contents(cetcd_kv *kvs, size_t count);

/* --- Watch (callback-based, single-shot) --- */

typedef void (*cetcd_watch_cb)(const cetcd_watch_event *ev, void *udata);

/* Watch a single key. Returns watcher handle or NULL. */
cetcd_watcher *cetcd_mvcc_watch(cetcd_mvcc_store *s,
                                 const uint8_t *key, size_t key_len,
                                 int64_t start_rev,
                                 cetcd_watch_cb cb, void *udata);

/* Watch a prefix. Returns watcher handle or NULL. */
cetcd_watcher *cetcd_mvcc_watch_prefix(cetcd_mvcc_store *s,
                                        const uint8_t *prefix, size_t prefix_len,
                                        int64_t start_rev,
                                        cetcd_watch_cb cb, void *udata);

void cetcd_mvcc_watch_cancel(cetcd_mvcc_store *s, cetcd_watcher *w);

/* --- Watch (streaming / notification-channel based) --- */

/* Singly-linked list node for pending watch events. */
typedef struct cetcd_mvcc_watch_event_node {
    cetcd_watch_event                  event;
    struct cetcd_mvcc_watch_event_node *next;
} cetcd_mvcc_watch_event_node;

/*
 * Notification channel: a queue of watch events plus a wake-up callback
 * (typically wrapping uv_async_send).  Single-producer (MVCC store thread),
 * single-consumer (watcher coroutine).
 */
typedef struct cetcd_mvcc_watch_notify {
    cetcd_mvcc_watch_event_node *head;
    cetcd_mvcc_watch_event_node *tail;
    size_t                       count;
    void                       (*wake_cb)(void *udata);
    void                        *wake_cb_udata;
} cetcd_mvcc_watch_notify;

/* Initialize / destroy a notification channel. */
CETCD_API void cetcd_mvcc_watch_notify_init(cetcd_mvcc_watch_notify *n,
                                             void (*wake_cb)(void *), void *udata);
CETCD_API void cetcd_mvcc_watch_notify_destroy(cetcd_mvcc_watch_notify *n);

/* Opaque streaming watcher handle. */
typedef struct cetcd_stream_watcher cetcd_stream_watcher;

/*
 * Subscribe with a notification channel.
 * Events matching (key, range_end, start_rev) are pushed into *notify
 * and wake_cb is called after each push so the event loop can resume
 * the watcher coroutine.
 *
 * range_end == NULL / range_end_len == 0  ->  exact-key watch
 * range_end points to a single '\0' byte   ->  prefix watch
 * otherwise                                ->  range [key, range_end)
 */
CETCD_API cetcd_stream_watcher *cetcd_mvcc_watch_subscribe(
    cetcd_mvcc_store *store, int64_t watch_id,
    const uint8_t *key, size_t key_len,
    const uint8_t *range_end, size_t range_end_len,
    int64_t start_rev, int want_prev_kv,
    cetcd_mvcc_watch_notify *notify);

/* Unsubscribe a streaming watcher. */
CETCD_API void cetcd_mvcc_watch_unsubscribe(cetcd_mvcc_store *store,
                                             cetcd_stream_watcher *w);

/*
 * Dequeue all pending events from a notification channel.
 * Caller must free each event's kv.key.data, kv.value.data, prev_kv data
 * and finally free the events array itself.
 * Returns CETCD_OK on success, CETCD_ERR_INVAL on bad args.
 */
CETCD_API int cetcd_mvcc_watch_recv(cetcd_mvcc_watch_notify *notify,
                                     cetcd_watch_event **events_out,
                                     size_t *count_out);

/* --- Compaction --- */

/* Compact the store by removing history entries with revision < compact_rev.
   Revision == compact_rev remains readable. Requests with rev < compacted_rev
   return CETCD_ERR_RANGE from get/range.
   Returns 0 on success, CETCD_ERR_INVAL if compact_rev > current revision. */
int cetcd_mvcc_compact(cetcd_mvcc_store *s, int64_t compact_rev);

/* Return the current compacted revision (0 if no compaction done). */
int64_t cetcd_mvcc_compacted_revision(const cetcd_mvcc_store *s);

/* --- Persistence (LMDB) --- */

/* Attach a backend for incremental persistence of put/delete.
 * When set, each put/delete persists to LMDB first (fail-closed): on error the
 * in-memory store is unchanged and the write returns revision {0,0}.
 * Pass NULL to detach. Does not take ownership of `be`. */
void cetcd_mvcc_set_backend(cetcd_mvcc_store *s, cetcd_backend *be);

/* Load current key generations and revision from backend into an empty store.
 * Returns CETCD_OK even if the bucket is empty. */
int cetcd_mvcc_load(cetcd_mvcc_store *s, cetcd_backend *be);

#ifdef __cplusplus
}
#endif
#endif
