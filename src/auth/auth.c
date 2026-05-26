#include "cetcd/auth.h"
#include "cetcd/base.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>

/*
 * Lightweight, self-contained authentication store implementation used by tests.
 * NOTE: This implementation uses a simple in-process storage backed by
 * cetcd_hashmap. Passwords are hashed with a tiny, deterministic placeholder
 * hash function to satisfy the tests. This is not cryptographically secure and
 * is intended only for unit testing purposes.
 */

/* Forward declarations of internal helpers */
static void hash_password_placeholder(const void *data, size_t len, uint8_t out[32]);
static size_t cetcd_user_roles_total_len(const cetcd_user *u);

struct cetcd_auth_store {
    cetcd_hashmap *users;   /* key: username (string), value: cetcd_user* */
    cetcd_hashmap *roles;   /* key: role name (string), value: cetcd_role* */
    bool enabled;
};

/* Helpers for role/and user management */
static cetcd_user *cetcd_user_new(const char *name, const char *password_hash_source) {
    cetcd_user *u = (cetcd_user *)calloc(1, sizeof(*u));
    if (u == NULL) return NULL;
    /* copy name */
    strncpy(u->name, name, sizeof(u->name) - 1);
    u->name[sizeof(u->name) - 1] = '\0';
    /* hash password */
    uint8_t hash32[32];
    hash_password_placeholder((const void *)password_hash_source, strlen(password_hash_source), hash32);
    memcpy(u->password_hash, hash32, 32);
    u->hash_len = 32;
    u->n_roles = 0;
    u->roles = NULL;
    return u;
}

cetcd_auth_store *cetcd_auth_store_new(void) {
    cetcd_auth_store *s = (cetcd_auth_store *)calloc(1, sizeof(*s));
    if (s == NULL) return NULL;
    s->users = cetcd_hashmap_new(8);
    s->roles = cetcd_hashmap_new(8);
    s->enabled = false;
    return s;
}

/* Callback helpers for hashmap iteration (free resources) */
static bool cetcd_free_user_cb(cetcd_slice key, void *value, void *udata) {
    (void)udata;
    cetcd_user *u = (cetcd_user *)value;
    if (u) {
        if (u->roles) free(u->roles);
        free(u);
    }
    return true;
}

static bool cetcd_free_role_cb(cetcd_slice key, void *value, void *udata) {
    (void)udata;
    cetcd_role *r = (cetcd_role *)value;
    if (r) free(r);
    return true;
}

void cetcd_auth_store_free(cetcd_auth_store *s) {
    if (s == NULL) return;
    /* Free all users */
    if (s->users) {
        cetcd_hashmap_iter(s->users, cetcd_free_user_cb, NULL);
        cetcd_hashmap_free(s->users);
    }
    /* Free all roles */
    if (s->roles) {
        cetcd_hashmap_iter(s->roles, cetcd_free_role_cb, NULL);
        cetcd_hashmap_free(s->roles);
    }
    free(s);
}

int cetcd_auth_add_user(cetcd_auth_store *s, const char *name, const char *password) {
    if (s == NULL || name == NULL || password == NULL) return CETCD_ERR_INVAL;
    /* Check existence */
    cetcd_slice key; key.data = (unsigned char *)name; key.len = strlen(name);
    void *tmp = NULL;
    if (cetcd_hashmap_get(s->users, key, &tmp)) {
        return CETCD_ERR_EXISTS;
    }
    cetcd_user *u = cetcd_user_new(name, password);
    if (u == NULL) return CETCD_ERR_NOMEM;
    int rc = cetcd_hashmap_put(s->users, key, (void *)u);
    if (rc != 0) {
        free(u->roles);
        free(u);
        return CETCD_ERR_NOMEM;
    }
    return CETCD_OK;
}

int cetcd_auth_remove_user(cetcd_auth_store *s, const char *name) {
    if (s == NULL || name == NULL) return CETCD_ERR_INVAL;
    cetcd_slice key; key.data = (unsigned char *)name; key.len = strlen(name);
    void *val = NULL;
    if (!cetcd_hashmap_get(s->users, key, &val)) {
        return CETCD_ERR_NOTFOUND;
    }
    /* Remove and free user */
    if (cetcd_hashmap_remove(s->users, key, &val)) {
        cetcd_user *u = (cetcd_user *)val;
        if (u) {
            if (u->roles) free(u->roles);
            free(u);
        }
        return CETCD_OK;
    }
    return CETCD_ERR_NOTFOUND;
}

bool cetcd_auth_has_user(const cetcd_auth_store *s, const char *name) {
    if (s == NULL || name == NULL) return false;
    cetcd_slice key; key.data = (unsigned char *)name; key.len = strlen(name);
    void *v = NULL;
    return cetcd_hashmap_get(s->users, key, &v);
}

static cetcd_role *cetcd_role_new(const char *name, int perm_read,
                                  int perm_write, const char *key_prefix,
                                  size_t prefix_len) {
    cetcd_role *r = (cetcd_role *)calloc(1, sizeof(*r));
    if (r == NULL) return NULL;
    strncpy(r->name, name, sizeof(r->name) - 1);
    r->name[sizeof(r->name) - 1] = '\0';
    r->perm_read = perm_read;
    r->perm_write = perm_write;
    if (key_prefix) {
        size_t lp = (prefix_len > 0 && prefix_len < sizeof(r->key_prefix)) ? prefix_len : strlen(key_prefix);
        strncpy(r->key_prefix, key_prefix, lp);
        r->key_prefix[lp] = '\0';
        r->key_prefix_len = lp;
    } else {
        r->key_prefix[0] = '\0';
        r->key_prefix_len = 0;
    }
    return r;
}

static cetcd_user *cetcd_find_user(cetcd_auth_store *s, const char *name) {
    cetcd_slice key; key.data = (unsigned char *)name; key.len = strlen(name);
    void *v = NULL;
    if (!cetcd_hashmap_get(s->users, key, &v)) return NULL;
    return (cetcd_user *)v;
}

static cetcd_role *cetcd_find_role(cetcd_auth_store *s, const char *name) {
    cetcd_slice key; key.data = (unsigned char *)name; key.len = strlen(name);
    void *v = NULL;
    if (!cetcd_hashmap_get(s->roles, key, &v)) return NULL;
    return (cetcd_role *)v;
}

int cetcd_auth_add_role(cetcd_auth_store *s, const char *name,
                       int perm_read, int perm_write,
                       const char *key_prefix, size_t prefix_len) {
    if (s == NULL || name == NULL) return CETCD_ERR_INVAL;
    cetcd_slice key; key.data = (unsigned char *)name; key.len = strlen(name);
    void *exists = NULL;
    if (cetcd_hashmap_get(s->roles, key, &exists)) {
        return CETCD_ERR_EXISTS;
    }
    cetcd_role *r = cetcd_role_new(name, perm_read, perm_write, key_prefix, prefix_len);
    if (r == NULL) return CETCD_ERR_NOMEM;
    int rc = cetcd_hashmap_put(s->roles, key, (void *)r);
    if (rc != 0) {
        free(r);
        return CETCD_ERR_NOMEM;
    }
    return CETCD_OK;
}

int cetcd_auth_remove_role(cetcd_auth_store *s, const char *name) {
    if (s == NULL || name == NULL) return CETCD_ERR_INVAL;
    cetcd_slice key; key.data = (unsigned char *)name; key.len = strlen(name);
    void *v = NULL;
    if (!cetcd_hashmap_remove(s->roles, key, &v)) {
        return CETCD_ERR_NOTFOUND;
    }
    free(v);
    return CETCD_OK;
}

bool cetcd_auth_check_password(const cetcd_auth_store *s,
                              const char *name, const char *password) {
    if (s == NULL || name == NULL || password == NULL) return false;
    cetcd_slice key; key.data = (unsigned char *)name; key.len = strlen(name);
    cetcd_user *u = NULL;
    void *v = NULL;
    if (!cetcd_hashmap_get((cetcd_hashmap *)s->users, key, &v)) return false;
    u = (cetcd_user *)v;
    uint8_t hash32[32];
    hash_password_placeholder((const void *)password, strlen(password), hash32);
    /* constant-time style compare simplified */
    int match = 1; for (size_t i = 0; i < 32; ++i) { if (u->password_hash[i] != hash32[i]) { match = 0; break; } }
    return match != 0;
}

bool cetcd_auth_is_enabled(const cetcd_auth_store *s) {
    return s ? s->enabled : false;
}

void cetcd_auth_set_enabled(cetcd_auth_store *s, bool enabled) {
    if (s) s->enabled = enabled;
}

size_t cetcd_auth_user_count(const cetcd_auth_store *s) {
    return s && s->users ? cetcd_hashmap_size(s->users) : 0;
}

size_t cetcd_auth_role_count(const cetcd_auth_store *s) {
    return s && s->roles ? cetcd_hashmap_size(s->roles) : 0;
}

/* Internal helpers implementation */
static void hash_password_placeholder(const void *data, size_t len, uint8_t out[32]) {
    /* Deterministic, non-cryptographic placeholder hash based on input data */
    const unsigned char *p = (const unsigned char *)data;
    /* Initialize with a simple digest derived from length and bytes */
    for (size_t i = 0; i < 32; ++i) out[i] = 0;
    for (size_t i = 0; i < len; ++i) {
        out[i % 32] ^= p[i];
        out[i % 32] ^= (uint8_t)((i * 13) & 0xFF);
    }
    /* Add a tiny mixing pass to reduce obvious collisions for different lengths */
    for (size_t i = 0; i < 32; ++i) {
        out[i] = out[i] ^ (uint8_t)((len + i) & 0xFF);
    }
}

static size_t cetcd_user_roles_total_len(const cetcd_user *u) {
    if (u == NULL || u->n_roles == 0 || u->roles == NULL) return 0;
    size_t total = 0;
    const char *p = u->roles;
    for (size_t i = 0; i < u->n_roles; ++i) {
        size_t l = strlen(p);
        total += l + 1; /* include terminator */
        p += l + 1;
    }
    return total;
}

/* Grant and revoke role implementations */
int cetcd_auth_grant_role(cetcd_auth_store *s, const char *user, const char *role) {
    if (s == NULL || user == NULL || role == NULL) return CETCD_ERR_INVAL;
    cetcd_user *u = NULL;
    void *v = NULL;
    if (!cetcd_hashmap_get(s->users, (cetcd_slice){.data=(unsigned char*)user,.len=strlen(user)}, &v)) {
        return CETCD_ERR_NOTFOUND;
    }
    u = (cetcd_user *)v;
    if (!cetcd_hashmap_get(s->roles, (cetcd_slice){.data=(unsigned char*)role,.len=strlen(role)}, &v)) {
        return CETCD_ERR_NOTFOUND;
    }
    /* Check existing */
    /* walk existing roles blob */
    if (u->roles && u->n_roles > 0) {
        const char *p = u->roles;
        for (size_t i = 0; i < u->n_roles; ++i) {
            size_t l = strlen(p);
            if (l == strlen(role) && strncmp(p, role, l) == 0) {
                return CETCD_OK; /* already granted */
            }
            p += l + 1;
        }
    }
    /* Append role name to blob */
    const char *role_name = role;
    size_t add_len = strlen(role_name) + 1; /* include null terminator */
    size_t old_len = cetcd_user_roles_total_len(u);
    size_t new_len = old_len + add_len;
    char *newbuf = NULL;
    if (new_len == 0) newbuf = NULL; else {
        newbuf = (char *)realloc(u->roles, new_len);
        if (newbuf == NULL) return CETCD_ERR_NOMEM;
        u->roles = newbuf;
        char *dest = u->roles + old_len;
        memcpy(dest, role_name, strlen(role_name));
        dest[strlen(role_name)] = '\0';
    }
    u->n_roles += 1;
    return CETCD_OK;
}

int cetcd_auth_revoke_role(cetcd_auth_store *s, const char *user, const char *role) {
    if (s == NULL || user == NULL || role == NULL) return CETCD_ERR_INVAL;
    void *v = NULL;
    if (!cetcd_hashmap_get(s->users, (cetcd_slice){.data=(unsigned char*)user,.len=strlen(user)}, &v)) {
        return CETCD_ERR_NOTFOUND;
    }
    cetcd_user *u = (cetcd_user *)v;
    if (u->n_roles == 0 || u->roles == NULL) {
        return CETCD_ERR_NOTFOUND;
    }
    /* Search for role in blob */
    char *p = u->roles;
    for (size_t i = 0; i < u->n_roles; ++i) {
        size_t l = strlen(p);
        if (l == strlen(role) && strncmp(p, role, l) == 0) {
            /* Remove by shifting memory left by (l+1) */
            size_t remove_len = l + 1;
            size_t blob_total = cetcd_user_roles_total_len(u);
            memmove(p, p + remove_len, blob_total - remove_len);
            /* Update n_roles and zero-terminate last slot */
            u->n_roles -= 1;
            if (u->n_roles == 0) {
                free(u->roles);
                u->roles = NULL;
            } else {
                /* Ensure proper terminator after memmove; last role already has terminator */
                /* Nothing else to do – blob remains valid */
            }
            return CETCD_OK;
        }
        p += l + 1;
    }
    return CETCD_ERR_NOTFOUND;
}
