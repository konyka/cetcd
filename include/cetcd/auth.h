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

#ifdef __cplusplus
}
#endif
#endif
