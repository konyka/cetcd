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

#ifdef __cplusplus
}
#endif
#endif
