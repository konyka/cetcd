#ifndef CETCD_MVCC_H_
#define CETCD_MVCC_H_

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "cetcd/slice.h"

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

/* --- Read operations (at a given revision, 0 = current) --- */

int cetcd_mvcc_get(cetcd_mvcc_store *s, int64_t rev,
                    const uint8_t *key, size_t key_len,
                    cetcd_kv *out);

/* Range: returns count of results. Caller frees out[] via cetcd_kv_free_contents. */
int cetcd_mvcc_range(cetcd_mvcc_store *s, int64_t rev,
                      const uint8_t *key_start, size_t start_len,
                      const uint8_t *key_end, size_t end_len,
                      cetcd_kv **out, size_t *out_count);

void cetcd_kv_free_contents(cetcd_kv *kvs, size_t count);

/* --- Watch --- */

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

/* --- Compaction --- */

/* Compact the store by removing all history entries with revision <= compact_rev.
   Returns 0 on success, CETCD_ERR_INVAL if compact_rev > current revision. */
int cetcd_mvcc_compact(cetcd_mvcc_store *s, int64_t compact_rev);

/* Return the current compacted revision (0 if no compaction done). */
int64_t cetcd_mvcc_compacted_revision(const cetcd_mvcc_store *s);

#ifdef __cplusplus
}
#endif
#endif
