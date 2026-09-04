#ifndef CETCD_AUTH_H_
#define CETCD_AUTH_H_

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct cetcd_auth_store cetcd_auth_store;

typedef struct {
    char    name[128];
    char   *roles;
    size_t  n_roles;
    uint8_t password_hash[64];
    size_t  hash_len;
} cetcd_user;

typedef struct {
    char name[128];
    int  perm_read;
    int  perm_write;
    char key_prefix[256];
    size_t key_prefix_len;
} cetcd_role;

cetcd_auth_store *cetcd_auth_store_new(void);
void              cetcd_auth_store_free(cetcd_auth_store *s);

int cetcd_auth_add_user(cetcd_auth_store *s, const char *name,
                         const char *password);
/* Insert a user with an already-computed password hash (Raft apply). */
int cetcd_auth_add_user_hash(cetcd_auth_store *s, const char *name,
                             const uint8_t *hash, size_t hash_len);
int cetcd_auth_hash_password(cetcd_auth_store *s, const char *password,
                             uint8_t *out, size_t cap, size_t *out_len);
int cetcd_auth_remove_user(cetcd_auth_store *s, const char *name);
bool cetcd_auth_has_user(const cetcd_auth_store *s, const char *name);
int cetcd_auth_grant_role(cetcd_auth_store *s, const char *user,
                           const char *role);
int cetcd_auth_revoke_role(cetcd_auth_store *s, const char *user,
                            const char *role);

int cetcd_auth_add_role(cetcd_auth_store *s, const char *name,
                         int perm_read, int perm_write,
                         const char *key_prefix, size_t prefix_len);
int cetcd_auth_remove_role(cetcd_auth_store *s, const char *name);

bool cetcd_auth_check_password(const cetcd_auth_store *s,
                                const char *name, const char *password);

bool cetcd_auth_is_enabled(const cetcd_auth_store *s);
void cetcd_auth_set_enabled(cetcd_auth_store *s, bool enabled);

size_t cetcd_auth_user_count(const cetcd_auth_store *s);
size_t cetcd_auth_role_count(const cetcd_auth_store *s);

/* Change a user's password */
int cetcd_auth_change_password(cetcd_auth_store *s, const char *name,
                                const char *new_password);

/* Iterate users: call fn for each user, stop early if fn returns false */
typedef bool (*cetcd_auth_user_iter_fn)(const char *name, void *udata);
void cetcd_auth_user_iter(const cetcd_auth_store *s, cetcd_auth_user_iter_fn fn, void *udata);

/* Iterate roles: call fn for each role, stop early if fn returns false */
typedef bool (*cetcd_auth_role_iter_fn)(const char *name, void *udata);
void cetcd_auth_role_iter(const cetcd_auth_store *s, cetcd_auth_role_iter_fn fn, void *udata);

/* Get a user by name (returns pointer into store, do not free).
 * Returns NULL if user not found. */
const cetcd_user *cetcd_auth_get_user(const cetcd_auth_store *s, const char *name);

/* Get a role by name (returns pointer into store, do not free).
 * Returns NULL if role not found. */
const cetcd_role *cetcd_auth_get_role(const cetcd_auth_store *s, const char *name);

/* Grant (set) permission on a role.
 * perm_read/perm_write: 0 or 1.
 * key/key_len: key prefix for the permission.
 * Returns CETCD_OK on success, CETCD_ERR_NOTFOUND if role doesn't exist. */
int cetcd_auth_grant_permission(cetcd_auth_store *s, const char *role,
                                  int perm_read, int perm_write,
                                  const char *key, size_t key_len);

/* Revoke all permissions from a role.
 * Returns CETCD_OK on success, CETCD_ERR_NOTFOUND if role doesn't exist. */
int cetcd_auth_revoke_permission(cetcd_auth_store *s, const char *role);

/* ── Data-plane tokens (simple opaque tokens, O(1) hashmap lookup) ── */

#define CETCD_AUTH_DEFAULT_TOKEN_TTL_NS (300ULL * 1000000000ULL)
#define CETCD_AUTH_MAX_TOKEN_LEN 2048
#define CETCD_AUTH_JWT_DEFAULT_TTL_NS (300ULL * 1000000000ULL) /* etcd 5m */

void        cetcd_auth_set_token_ttl_ns(cetcd_auth_store *s, uint64_t ttl_ns);

/* etcd --auth-token spec: "simple" / empty, or
 * "jwt,sign-method=HS256|RS256|ES256,priv-key=PATH[,ttl=5m]". Other JWT
 * methods and jwt without a signing key fail closed. */
int         cetcd_auth_set_token_spec(cetcd_auth_store *s, const char *spec);

/* Issue a unique opaque token for `username`. Caller frees the returned
 * heap string. Returns NULL on failure (unknown user / OOM). */
char       *cetcd_auth_issue_token(cetcd_auth_store *s, const char *username);

/* Resolve token → username. Returns an interior pointer (do not free),
 * or NULL if missing/expired. Expired entries are dropped (lazy GC). */
const char *cetcd_auth_user_for_token(cetcd_auth_store *s, const char *token,
                                      uint64_t now_ns);

void        cetcd_auth_revoke_user_tokens(cetcd_auth_store *s, const char *username);
void        cetcd_auth_revoke_all_tokens(cetcd_auth_store *s);

/* Username "root", or a user holding a role named "root". */
bool        cetcd_auth_is_admin(const cetcd_auth_store *s, const char *username);

/* Key permission: empty role prefix matches all keys; otherwise the key
 * must start with the role prefix. `want_write` 0 = read, non-zero = write.
 * Superuser (admin) always succeeds. When auth is disabled, always true. */
bool        cetcd_auth_check_perm(const cetcd_auth_store *s, const char *username,
                                  const uint8_t *key, size_t key_len, int want_write);

/* ── Password hashing ──
 * Default is SHA-256 (hot-path cheap, existing LMDB records).
 * cetcd_auth_set_bcrypt_cost(4..31) hashes new passwords with bcrypt;
 * verify accepts both encodings so a restart can mix old SHA-256 users. */

#define CETCD_AUTH_BCRYPT_COST_MIN 4
#define CETCD_AUTH_BCRYPT_COST_MAX 31

int  cetcd_auth_set_bcrypt_cost(cetcd_auth_store *s, int cost);
int  cetcd_auth_bcrypt_cost(const cetcd_auth_store *s);

/* Persist / restore RBAC (users, roles, enabled) to LMDB. Fail-closed:
 * save errors are returned to the caller; load of a missing store is OK. */
struct cetcd_backend;
int cetcd_auth_save(const cetcd_auth_store *s, struct cetcd_backend *be);
int cetcd_auth_load(cetcd_auth_store *s, struct cetcd_backend *be);

#ifdef __cplusplus
}
#endif
#endif
