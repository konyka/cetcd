#ifndef CETCD_TREAP_H_
#define CETCD_TREAP_H_

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "cetcd/slice.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Treap — randomized BST (tree-heap hybrid).
 *
 * Keys are cetcd_slice (byte strings), compared lexicographically.
 * Each node has a random priority; the heap invariant gives expected
 * O(log n) depth. Supports ordered iteration and range queries.
 *
 * Used as the in-memory index for MVCC key ordering.
 */

typedef struct cetcd_treap cetcd_treap;

/* Lifecycle */
cetcd_treap *cetcd_treap_new(void);
void         cetcd_treap_free(cetcd_treap *t);
size_t       cetcd_treap_size(const cetcd_treap *t);

/* Insert key→value. Returns CETCD_OK or CETCD_ERR_NOMEM.
 * If key exists, replaces value (does NOT duplicate keys). */
int  cetcd_treap_put(cetcd_treap *t, cetcd_slice key, void *value);

/* Lookup. Returns true and sets *val_out if found. */
bool cetcd_treap_get(const cetcd_treap *t, cetcd_slice key, void **val_out);

/* Delete by key. Returns true if removed. */
bool cetcd_treap_del(cetcd_treap *t, cetcd_slice key, void **val_out);

/* Sorted iteration (in-order, ascending by key). */
typedef bool (*cetcd_treap_iter_fn)(cetcd_slice key, void *value, void *udata);
void cetcd_treap_iter(const cetcd_treap *t, cetcd_treap_iter_fn fn, void *udata);

/* Range iteration: [lo, hi) — keys >= lo and < hi.
 * hi.len == 0 means open upper bound (all keys >= lo). */
void cetcd_treap_range(const cetcd_treap *t,
                        cetcd_slice lo, cetcd_slice hi,
                        cetcd_treap_iter_fn fn, void *udata);

#ifdef __cplusplus
}
#endif
#endif
