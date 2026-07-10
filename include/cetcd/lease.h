#ifndef CETCD_LEASE_H_
#define CETCD_LEASE_H_

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef uint64_t cetcd_lease_id;

typedef struct cetcd_lease cetcd_lease;
typedef struct cetcd_lease_mgr cetcd_lease_mgr;

/* Callback when a lease expires. Keys attached to the lease are passed. */
typedef void (*cetcd_lease_expire_fn)(cetcd_lease_id id,
                                       const uint8_t *const *keys,
                                       const size_t *key_lens,
                                       size_t count,
                                       void *udata);

cetcd_lease_mgr *cetcd_lease_mgr_new(cetcd_lease_expire_fn on_expire, void *udata);
void             cetcd_lease_mgr_free(cetcd_lease_mgr *mgr);

/* Replace the expire callback (e.g. after wiring MVCC into the server). */
void cetcd_lease_mgr_set_expire(cetcd_lease_mgr *mgr,
                                 cetcd_lease_expire_fn on_expire,
                                 void *udata);

cetcd_lease_id cetcd_lease_grant(cetcd_lease_mgr *mgr, int64_t ttl_seconds);
int            cetcd_lease_revoke(cetcd_lease_mgr *mgr, cetcd_lease_id id);
int            cetcd_lease_keep_alive(cetcd_lease_mgr *mgr, cetcd_lease_id id, int64_t ttl_seconds);

bool           cetcd_lease_exists(const cetcd_lease_mgr *mgr, cetcd_lease_id id);
int64_t        cetcd_lease_ttl_remaining(const cetcd_lease_mgr *mgr, cetcd_lease_id id);

/* Attach/detach a key to a lease (called by MVCC on put). */
int cetcd_lease_attach_key(cetcd_lease_mgr *mgr, cetcd_lease_id id,
                            const uint8_t *key, size_t key_len);
int cetcd_lease_detach_key(cetcd_lease_mgr *mgr, cetcd_lease_id id,
                            const uint8_t *key, size_t key_len);

/* Tick: advance time by `elapsed_ms`, expire any leases past due. */
void cetcd_lease_mgr_tick(cetcd_lease_mgr *mgr, int64_t elapsed_ms);

size_t cetcd_lease_mgr_count(const cetcd_lease_mgr *mgr);

/* Get the granted TTL (original) for a lease. Returns 0 if not found. */
int64_t cetcd_lease_granted_ttl(const cetcd_lease_mgr *mgr, cetcd_lease_id id);

/* Get all lease IDs. Caller provides an array of capacity `cap`.
 * Returns the number of IDs written (up to cap), or total count if cap is 0. */
size_t cetcd_lease_mgr_leases(const cetcd_lease_mgr *mgr,
                               cetcd_lease_id *out, size_t cap);

/* Get keys attached to a lease.
 * Sets *out_keys to an array of pointers (do not free), *out_lens to key lengths,
 * and returns the key count. Returns 0 if lease not found or no keys. */
size_t cetcd_lease_keys(const cetcd_lease_mgr *mgr, cetcd_lease_id id,
                         const uint8_t *const **out_keys,
                         const size_t **out_lens);

#ifdef __cplusplus
}
#endif
#endif
