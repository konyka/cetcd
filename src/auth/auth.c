#include "cetcd/auth.h"
#include "cetcd/base.h"
#include "cetcd/backend.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

/*
 * Authentication store implementation with RBAC support.
 * Uses cetcd_hashmap for in-process storage.
 * When OpenSSL is available, passwords are hashed with SHA-256 via the EVP API
 * by default. --bcrypt-cost (4..31) switches new hashes to bcrypt via libcrypt.
 * Otherwise, a deterministic non-cryptographic fallback hash is used.
 */

#if CETCD_HAS_OPENSSL
#include <openssl/evp.h>
#include <openssl/rand.h>
#include <openssl/hmac.h>
#include <openssl/pem.h>
#include <openssl/bio.h>
#include <openssl/bn.h>
#include <openssl/ecdsa.h>
#endif
#include <stdio.h>
#if CETCD_HAS_CRYPT
#include <crypt.h>
#endif

/* Forward declarations of internal helpers */
static void hash_password(const void *data, size_t len, uint8_t out[32]);
static int  fill_password_hash_(cetcd_auth_store *s, cetcd_user *u, const char *password);
static size_t cetcd_user_roles_total_len(const cetcd_user *u);

typedef struct cetcd_auth_token_ent {
    char     username[128];
    uint64_t expires_ns;
} cetcd_auth_token_ent;

#define CETCD_JWT_NONE  0
#define CETCD_JWT_HS256 1
#define CETCD_JWT_RS256 2
#define CETCD_JWT_ES256 3

struct cetcd_auth_store {
    cetcd_hashmap *users;   /* key: username (string), value: cetcd_user* */
    cetcd_hashmap *roles;   /* key: role name (string), value: cetcd_role* */
    cetcd_hashmap *tokens;  /* key: token hex, value: cetcd_auth_token_ent* */
    bool     enabled;
    uint64_t token_ttl_ns;
    uint64_t token_seq;
    int      bcrypt_cost; /* 0 = SHA-256 */
    int      jwt_alg;
    uint8_t *jwt_key;
    size_t   jwt_key_len;
#if CETCD_HAS_OPENSSL
    EVP_PKEY *jwt_pkey;
#endif
    char     jwt_user[128];
};

static cetcd_slice auth_key_(const char *s) {
    return cetcd_slice_make(s, strlen(s));
}

/* Helpers for role/and user management */
static cetcd_user *cetcd_user_new(const char *name) {
    cetcd_user *u = (cetcd_user *)calloc(1, sizeof(*u));
    if (u == NULL) return NULL;
    strncpy(u->name, name, sizeof(u->name) - 1);
    u->name[sizeof(u->name) - 1] = '\0';
    u->n_roles = 0;
    u->roles = NULL;
    return u;
}

cetcd_auth_store *cetcd_auth_store_new(void) {
    cetcd_auth_store *s = (cetcd_auth_store *)calloc(1, sizeof(*s));
    if (s == NULL) return NULL;
    s->users = cetcd_hashmap_new(8);
    s->roles = cetcd_hashmap_new(8);
    s->tokens = cetcd_hashmap_new(8);
    s->enabled = false;
    s->token_ttl_ns = CETCD_AUTH_DEFAULT_TOKEN_TTL_NS;
    if (!s->users || !s->roles || !s->tokens) {
        cetcd_auth_store_free(s);
        return NULL;
    }
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

static bool cetcd_free_token_cb(cetcd_slice key, void *value, void *udata) {
    (void)key;
    (void)udata;
    free(value);
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
    if (s->tokens) {
        cetcd_hashmap_iter(s->tokens, cetcd_free_token_cb, NULL);
        cetcd_hashmap_free(s->tokens);
    }
    free(s->jwt_key);
#if CETCD_HAS_OPENSSL
    EVP_PKEY_free(s->jwt_pkey);
#endif
    free(s);
}

int cetcd_auth_add_user(cetcd_auth_store *s, const char *name, const char *password) {
    if (s == NULL || name == NULL || password == NULL) return CETCD_ERR_INVAL;
    /* Check existence */
    cetcd_slice key = auth_key_(name);
    void *tmp = NULL;
    if (cetcd_hashmap_get(s->users, key, &tmp)) {
        return CETCD_ERR_EXISTS;
    }
    cetcd_user *u = cetcd_user_new(name);
    if (u == NULL) return CETCD_ERR_NOMEM;
    int hrc = fill_password_hash_(s, u, password);
    if (hrc != CETCD_OK) {
        free(u);
        return hrc;
    }
    int rc = cetcd_hashmap_put(s->users, key, (void *)u);
    if (rc != 0) {
        free(u->roles);
        free(u);
        return CETCD_ERR_NOMEM;
    }
    return CETCD_OK;
}

int cetcd_auth_hash_password(cetcd_auth_store *s, const char *password,
                             uint8_t *out, size_t cap, size_t *out_len) {
    if (!s || !password || !out || !out_len) return CETCD_ERR_INVAL;
    cetcd_user u;
    memset(&u, 0, sizeof(u));
    int rc = fill_password_hash_(s, &u, password);
    if (rc != CETCD_OK) return rc;
    if (u.hash_len == 0 || u.hash_len > cap) return CETCD_ERR_OVERFLOW;
    memcpy(out, u.password_hash, u.hash_len);
    *out_len = u.hash_len;
    return CETCD_OK;
}

int cetcd_auth_add_user_hash(cetcd_auth_store *s, const char *name,
                             const uint8_t *hash, size_t hash_len) {
    if (s == NULL || name == NULL || name[0] == '\0' || hash == NULL)
        return CETCD_ERR_INVAL;
    if (hash_len == 0 || hash_len > sizeof(((cetcd_user *)0)->password_hash))
        return CETCD_ERR_INVAL;
    cetcd_slice key = auth_key_(name);
    void *tmp = NULL;
    if (cetcd_hashmap_get(s->users, key, &tmp))
        return CETCD_ERR_EXISTS;
    cetcd_user *u = cetcd_user_new(name);
    if (u == NULL) return CETCD_ERR_NOMEM;
    memcpy(u->password_hash, hash, hash_len);
    u->hash_len = hash_len;
    int rc = cetcd_hashmap_put(s->users, key, (void *)u);
    if (rc != 0) {
        free(u->roles);
        free(u);
        return CETCD_ERR_NOMEM;
    }
    return CETCD_OK;
}

int cetcd_auth_set_user_hash(cetcd_auth_store *s, const char *name,
                             const uint8_t *hash, size_t hash_len) {
    if (s == NULL || name == NULL || name[0] == '\0' || hash == NULL)
        return CETCD_ERR_INVAL;
    if (hash_len == 0 || hash_len > sizeof(((cetcd_user *)0)->password_hash))
        return CETCD_ERR_INVAL;
    cetcd_slice key = auth_key_(name);
    void *tmp = NULL;
    if (!cetcd_hashmap_get(s->users, key, &tmp))
        return CETCD_ERR_NOTFOUND;
    cetcd_user *u = (cetcd_user *)tmp;
    memcpy(u->password_hash, hash, hash_len);
    u->hash_len = hash_len;
    cetcd_auth_revoke_user_tokens(s, name);
    return CETCD_OK;
}

int cetcd_auth_remove_user(cetcd_auth_store *s, const char *name) {
    if (s == NULL || name == NULL) return CETCD_ERR_INVAL;
    cetcd_slice key = auth_key_(name);
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
    cetcd_slice key = auth_key_(name);
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

static cetcd_user *cetcd_find_user(const cetcd_auth_store *s, const char *name) {
    cetcd_slice key = auth_key_(name);
    void *v = NULL;
    if (!cetcd_hashmap_get(s->users, key, &v)) return NULL;
    return (cetcd_user *)v;
}

static cetcd_role *cetcd_find_role(const cetcd_auth_store *s, const char *name) {
    cetcd_slice key = auth_key_(name);
    void *v = NULL;
    if (!cetcd_hashmap_get(s->roles, key, &v)) return NULL;
    return (cetcd_role *)v;
}

int cetcd_auth_add_role(cetcd_auth_store *s, const char *name,
                       int perm_read, int perm_write,
                       const char *key_prefix, size_t prefix_len) {
    if (s == NULL || name == NULL) return CETCD_ERR_INVAL;
    cetcd_slice key = auth_key_(name);
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
    cetcd_slice key = auth_key_(name);
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
    cetcd_slice key = auth_key_(name);
    cetcd_user *u = NULL;
    void *v = NULL;
    if (!cetcd_hashmap_get((cetcd_hashmap *)s->users, key, &v)) return false;
    u = (cetcd_user *)v;
    if (u->hash_len > 32) {
#if CETCD_HAS_CRYPT
        char setting[65];
        if (u->hash_len >= sizeof(setting)) return false;
        memcpy(setting, u->password_hash, u->hash_len);
        setting[u->hash_len] = '\0';
        struct crypt_data *cd = (struct crypt_data *)calloc(1, sizeof(*cd));
        if (!cd) return false;
        char *got = crypt_r(password, setting, cd);
        unsigned diff = 1;
        if (got && got[0] != '*') {
            size_t gl = strlen(got);
            diff = (unsigned)(gl != u->hash_len);
            size_t n = gl < u->hash_len ? gl : u->hash_len;
            size_t i;
            for (i = 0; i < n; i++)
                diff |= (unsigned)((unsigned char)got[i] ^ u->password_hash[i]);
            for (; i < u->hash_len; i++)
                diff |= (unsigned)u->password_hash[i];
        }
        free(cd);
        return diff == 0;
#else
        return false;
#endif
    }
    uint8_t hash32[32];
    hash_password((const void *)password, strlen(password), hash32);
    /* Constant-time compare: no early exit on first mismatch. */
    unsigned diff = (unsigned)(u->hash_len != 32);
    size_t n = u->hash_len < 32 ? u->hash_len : 32;
    for (size_t i = 0; i < 32; ++i) {
        uint8_t stored = (i < n) ? u->password_hash[i] : 0;
        diff |= (unsigned)(stored ^ hash32[i]);
    }
    return diff == 0;
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

/* --- Iteration helpers --- */

struct auth_user_iter_ctx {
    cetcd_auth_user_iter_fn fn;
    void *udata;
};

struct auth_role_iter_ctx {
    cetcd_auth_role_iter_fn fn;
    void *udata;
};

static bool auth_iter_name_cb(cetcd_slice key, void *value, void *udata) {
    (void)value;
    struct auth_user_iter_ctx *ctx = (struct auth_user_iter_ctx *)udata;
    char name[128];
    size_t len = key.len < sizeof(name) - 1 ? key.len : sizeof(name) - 1;
    memcpy(name, key.data, len);
    name[len] = '\0';
    return ctx->fn(name, ctx->udata);
}

void cetcd_auth_user_iter(const cetcd_auth_store *s, cetcd_auth_user_iter_fn fn, void *udata) {
    if (!s || !s->users || !fn) return;
    struct auth_user_iter_ctx ctx = { fn, udata };
    cetcd_hashmap_iter(s->users, auth_iter_name_cb, &ctx);
}

static bool auth_iter_role_name_cb(cetcd_slice key, void *value, void *udata) {
    (void)value;
    struct auth_role_iter_ctx *ctx = (struct auth_role_iter_ctx *)udata;
    char name[128];
    size_t len = key.len < sizeof(name) - 1 ? key.len : sizeof(name) - 1;
    memcpy(name, key.data, len);
    name[len] = '\0';
    return ctx->fn(name, ctx->udata);
}

void cetcd_auth_role_iter(const cetcd_auth_store *s, cetcd_auth_role_iter_fn fn, void *udata) {
    if (!s || !s->roles || !fn) return;
    struct auth_role_iter_ctx ctx = { fn, udata };
    cetcd_hashmap_iter(s->roles, auth_iter_role_name_cb, &ctx);
}

/* --- Change password --- */

int cetcd_auth_change_password(cetcd_auth_store *s, const char *name,
                                const char *new_password) {
    if (!s || !name || !new_password) return CETCD_ERR_INVAL;
    cetcd_user *u = cetcd_find_user(s, name);
    if (!u) return CETCD_ERR_NOTFOUND;
    int hrc = fill_password_hash_(s, u, new_password);
    if (hrc != CETCD_OK) return hrc;
    cetcd_auth_revoke_user_tokens(s, name);
    return CETCD_OK;
}

/* Internal helpers implementation */

#if CETCD_HAS_OPENSSL
/* SHA-256 password hashing via OpenSSL EVP API */
static void hash_password(const void *data, size_t len, uint8_t out[32]) {
    EVP_MD_CTX *ctx = EVP_MD_CTX_new();
    if (ctx == NULL) {
        /* Fallback: zero-fill on allocation failure */
        memset(out, 0, 32);
        return;
    }
    unsigned int md_len = 32;
    EVP_DigestInit_ex(ctx, EVP_sha256(), NULL);
    EVP_DigestUpdate(ctx, data, len);
    EVP_DigestFinal_ex(ctx, out, &md_len);
    EVP_MD_CTX_free(ctx);
}
#else
/* Deterministic, non-cryptographic fallback hash (when OpenSSL unavailable) */
static void hash_password(const void *data, size_t len, uint8_t out[32]) {
    const unsigned char *p = (const unsigned char *)data;
    for (size_t i = 0; i < 32; ++i) out[i] = 0;
    for (size_t i = 0; i < len; ++i) {
        out[i % 32] ^= p[i];
        out[i % 32] ^= (uint8_t)((i * 13) & 0xFF);
    }
    for (size_t i = 0; i < 32; ++i) {
        out[i] = out[i] ^ (uint8_t)((len + i) & 0xFF);
    }
}
#endif

int cetcd_auth_set_bcrypt_cost(cetcd_auth_store *s, int cost) {
    if (!s) return CETCD_ERR_INVAL;
    if (cost == 0) {
        s->bcrypt_cost = 0;
        return CETCD_OK;
    }
    if (cost < CETCD_AUTH_BCRYPT_COST_MIN || cost > CETCD_AUTH_BCRYPT_COST_MAX)
        return CETCD_ERR_INVAL;
#if !CETCD_HAS_CRYPT
    return CETCD_ERR_UNSUPPORT;
#else
    s->bcrypt_cost = cost;
    return CETCD_OK;
#endif
}

int cetcd_auth_bcrypt_cost(const cetcd_auth_store *s) {
    return s ? s->bcrypt_cost : 0;
}

static int fill_password_hash_(cetcd_auth_store *s, cetcd_user *u, const char *password) {
    if (!s || !u || !password) return CETCD_ERR_INVAL;
    if (s->bcrypt_cost == 0) {
        uint8_t hash32[32];
        hash_password((const void *)password, strlen(password), hash32);
        memcpy(u->password_hash, hash32, 32);
        u->hash_len = 32;
        return CETCD_OK;
    }
#if CETCD_HAS_CRYPT
    char setting[CRYPT_GENSALT_OUTPUT_SIZE];
    if (!crypt_gensalt_rn("$2b$", (unsigned long)s->bcrypt_cost, NULL, 0,
                          setting, (int)sizeof(setting)))
        return CETCD_ERR_IO;
    struct crypt_data *cd = (struct crypt_data *)calloc(1, sizeof(*cd));
    if (!cd) return CETCD_ERR_NOMEM;
    char *h = crypt_r(password, setting, cd);
    if (!h || h[0] == '*') {
        free(cd);
        return CETCD_ERR_IO;
    }
    size_t hl = strlen(h);
    if (hl == 0 || hl > sizeof(u->password_hash)) {
        free(cd);
        return CETCD_ERR_OVERFLOW;
    }
    memcpy(u->password_hash, h, hl);
    u->hash_len = hl;
    free(cd);
    return CETCD_OK;
#else
    return CETCD_ERR_UNSUPPORT;
#endif
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
    if (!cetcd_hashmap_get(s->users, auth_key_(user), &v)) {
        return CETCD_ERR_NOTFOUND;
    }
    u = (cetcd_user *)v;
    if (!cetcd_hashmap_get(s->roles, auth_key_(role), &v)) {
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
    if (!cetcd_hashmap_get(s->users, auth_key_(user), &v)) {
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

/* --- Get user/role --- */

const cetcd_user *cetcd_auth_get_user(const cetcd_auth_store *s, const char *name) {
    if (!s || !name) return NULL;
    return cetcd_find_user(s, name);
}

const cetcd_role *cetcd_auth_get_role(const cetcd_auth_store *s, const char *name) {
    if (!s || !name) return NULL;
    return cetcd_find_role(s, name);
}

/* --- Grant / Revoke permission --- */

int cetcd_auth_grant_permission(cetcd_auth_store *s, const char *role,
                                  int perm_read, int perm_write,
                                  const char *key, size_t key_len) {
    if (!s || !role) return CETCD_ERR_INVAL;
    cetcd_role *r = cetcd_find_role(s, role);
    if (!r) return CETCD_ERR_NOTFOUND;
    r->perm_read = perm_read ? 1 : 0;
    r->perm_write = perm_write ? 1 : 0;
    if (key && key_len > 0) {
        size_t lp = key_len < sizeof(r->key_prefix) ? key_len : sizeof(r->key_prefix) - 1;
        memcpy(r->key_prefix, key, lp);
        r->key_prefix[lp] = '\0';
        r->key_prefix_len = lp;
    } else {
        r->key_prefix[0] = '\0';
        r->key_prefix_len = 0;
    }
    return CETCD_OK;
}

int cetcd_auth_revoke_permission(cetcd_auth_store *s, const char *role) {
    return cetcd_auth_revoke_permission_key(s, role, NULL, 0);
}

int cetcd_auth_revoke_permission_key(cetcd_auth_store *s, const char *role,
                                     const uint8_t *key, size_t key_len) {
    if (!s || !role) return CETCD_ERR_INVAL;
    if (key_len > 0 && !key) return CETCD_ERR_INVAL;
    cetcd_role *r = cetcd_find_role(s, role);
    if (!r) return CETCD_ERR_NOTFOUND;
    if (key_len > 0) {
        if (r->key_prefix_len != key_len || memcmp(r->key_prefix, key, key_len) != 0)
            return CETCD_ERR_NOTFOUND;
    }
    r->perm_read = 0;
    r->perm_write = 0;
    r->key_prefix[0] = '\0';
    r->key_prefix_len = 0;
    return CETCD_OK;
}

/* ── Tokens ──────────────────────────────────────────────────────────── */

#define CETCD_AUTH_TOKEN_RAW 16
#define CETCD_AUTH_TOKEN_HEX (CETCD_AUTH_TOKEN_RAW * 2)

static void auth_fill_random_(uint8_t *out, size_t n) {
#if CETCD_HAS_OPENSSL
    if (out && n > 0 && RAND_bytes(out, (int)n) == 1)
        return;
#endif
    uint64_t t = cetcd_clock_monotonic_ns();
    uint64_t r = cetcd_clock_realtime_ns();
    for (size_t i = 0; i < n; i++) {
        t = t * 6364136223846793005ULL + (uint64_t)i + 1u;
        r ^= t >> ((i & 7) * 8);
        out[i] = (uint8_t)(t >> 24) ^ (uint8_t)(r >> (i & 7));
    }
}

static void auth_hex_encode_(const uint8_t *in, size_t n, char *out) {
    static const char hex[] = "0123456789abcdef";
    for (size_t i = 0; i < n; i++) {
        out[i * 2]     = hex[(in[i] >> 4) & 0xF];
        out[i * 2 + 1] = hex[in[i] & 0xF];
    }
    out[n * 2] = '\0';
}

static const void *find_bytes_(const void *hay, size_t hlen, const void *needle, size_t nlen) {
    if (!hay || !needle || nlen == 0 || hlen < nlen) return NULL;
    const uint8_t *p = (const uint8_t *)hay;
    const uint8_t *n = (const uint8_t *)needle;
    for (size_t i = 0; i + nlen <= hlen; i++) {
        if (memcmp(p + i, n, nlen) == 0) return p + i;
    }
    return NULL;
}

static const char b64url_[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

static size_t b64url_encode_(const uint8_t *in, size_t n, char *out, size_t cap) {
    size_t o = 0;
    for (size_t i = 0; i < n; i += 3) {
        uint32_t v = ((uint32_t)in[i]) << 16;
        if (i + 1 < n) v |= ((uint32_t)in[i + 1]) << 8;
        if (i + 2 < n) v |= in[i + 2];
        if (o + 2 > cap) return 0;
        out[o++] = b64url_[(v >> 18) & 63];
        out[o++] = b64url_[(v >> 12) & 63];
        if (i + 1 < n) {
            if (o + 1 > cap) return 0;
            out[o++] = b64url_[(v >> 6) & 63];
        }
        if (i + 2 < n) {
            if (o + 1 > cap) return 0;
            out[o++] = b64url_[v & 63];
        }
    }
    if (o < cap) out[o] = '\0';
    return o;
}

static int b64url_val_(int c) {
    if (c >= 'A' && c <= 'Z') return c - 'A';
    if (c >= 'a' && c <= 'z') return c - 'a' + 26;
    if (c >= '0' && c <= '9') return c - '0' + 52;
    if (c == '-' || c == '+') return 62;
    if (c == '_' || c == '/') return 63;
    return -1;
}

static int b64url_decode_(const char *in, size_t n, uint8_t *out, size_t cap, size_t *out_len) {
    while (n > 0 && in[n - 1] == '=') n--;
    size_t o = 0;
    for (size_t i = 0; i < n; ) {
        int a = b64url_val_((unsigned char)in[i++]);
        int b = (i < n) ? b64url_val_((unsigned char)in[i++]) : 0;
        int c = (i < n) ? b64url_val_((unsigned char)in[i++]) : -2;
        int d = (i < n) ? b64url_val_((unsigned char)in[i++]) : -2;
        if (a < 0 || b < 0) return -1;
        if (o >= cap) return -1;
        out[o++] = (uint8_t)((a << 2) | (b >> 4));
        if (c >= 0) {
            if (o >= cap) return -1;
            out[o++] = (uint8_t)(((b & 0xF) << 4) | (c >> 2));
        }
        if (d >= 0) {
            if (o >= cap) return -1;
            out[o++] = (uint8_t)(((c & 0x3) << 6) | d);
        }
    }
    if (out_len) *out_len = o;
    return 0;
}

static int parse_go_duration_ns_(const char *s, uint64_t *out) {
    if (!s || !s[0] || !out) return -1;
    uint64_t total = 0;
    const char *p = s;
    while (*p) {
        if (*p < '0' || *p > '9') return -1;
        uint64_t n = 0;
        while (*p >= '0' && *p <= '9') {
            if (n > (UINT64_MAX / 10)) return -1;
            n = n * 10 + (uint64_t)(*p - '0');
            p++;
        }
        uint64_t mul;
        if (p[0] == 'n' && p[1] == 's') { mul = 1; p += 2; }
        else if (p[0] == 'u' && p[1] == 's') { mul = 1000ULL; p += 2; }
        else if (p[0] == 'm' && p[1] == 's') { mul = 1000000ULL; p += 2; }
        else if (*p == 's') { mul = 1000000000ULL; p++; }
        else if (*p == 'm') { mul = 60ULL * 1000000000ULL; p++; }
        else if (*p == 'h') { mul = 3600ULL * 1000000000ULL; p++; }
        else return -1;
        if (mul && n > UINT64_MAX / mul) return -1;
        if (total > UINT64_MAX - n * mul) return -1;
        total += n * mul;
    }
    *out = total;
    return 0;
}

static int read_whole_file_(const char *path, uint8_t **out, size_t *out_len) {
    if (!path || !out || !out_len) return CETCD_ERR_INVAL;
    FILE *f = fopen(path, "rb");
    if (!f) return CETCD_ERR_IO;
    if (fseek(f, 0, SEEK_END) != 0) { fclose(f); return CETCD_ERR_IO; }
    long sz = ftell(f);
    if (sz <= 0 || sz > 65536) { fclose(f); return CETCD_ERR_INVAL; }
    rewind(f);
    uint8_t *buf = (uint8_t *)malloc((size_t)sz);
    if (!buf) { fclose(f); return CETCD_ERR_NOMEM; }
    if (fread(buf, 1, (size_t)sz, f) != (size_t)sz) {
        free(buf); fclose(f); return CETCD_ERR_IO;
    }
    fclose(f);
    *out = buf;
    *out_len = (size_t)sz;
    return CETCD_OK;
}

#if CETCD_HAS_OPENSSL
static int jwt_hmac_(const cetcd_auth_store *s, const uint8_t *data, size_t len,
                     uint8_t mac[32]) {
    if (!s || !s->jwt_key || s->jwt_key_len == 0 || !data) return -1;
    unsigned char macbuf[EVP_MAX_MD_SIZE];
    unsigned int mac_len = 0;
    if (!HMAC(EVP_sha256(), s->jwt_key, (int)s->jwt_key_len,
              data, len, macbuf, &mac_len) || mac_len != 32)
        return -1;
    memcpy(mac, macbuf, 32);
    return 0;
}

static int jwt_ct_eq_(const uint8_t *a, const uint8_t *b, size_t n) {
    uint8_t d = 0;
    for (size_t i = 0; i < n; i++) d |= (uint8_t)(a[i] ^ b[i]);
    return d == 0;
}

static int jwt_digest_sign_(EVP_PKEY *pkey, const uint8_t *data, size_t len,
                            uint8_t **out, size_t *out_len) {
    if (!pkey || !data || !out || !out_len) return -1;
    EVP_MD_CTX *ctx = EVP_MD_CTX_new();
    if (!ctx) return -1;
    size_t sl = 0;
    uint8_t *sig = NULL;
    if (EVP_DigestSignInit(ctx, NULL, EVP_sha256(), NULL, pkey) != 1 ||
        EVP_DigestSign(ctx, NULL, &sl, data, len) != 1) {
        EVP_MD_CTX_free(ctx);
        return -1;
    }
    sig = (uint8_t *)malloc(sl);
    if (!sig) { EVP_MD_CTX_free(ctx); return -1; }
    if (EVP_DigestSign(ctx, sig, &sl, data, len) != 1) {
        free(sig);
        EVP_MD_CTX_free(ctx);
        return -1;
    }
    EVP_MD_CTX_free(ctx);
    *out = sig;
    *out_len = sl;
    return 0;
}

static int jwt_digest_verify_(EVP_PKEY *pkey, const uint8_t *data, size_t len,
                              const uint8_t *sig, size_t sig_len) {
    if (!pkey || !data || !sig) return -1;
    EVP_MD_CTX *ctx = EVP_MD_CTX_new();
    if (!ctx) return -1;
    int ok = EVP_DigestVerifyInit(ctx, NULL, EVP_sha256(), NULL, pkey) == 1 &&
             EVP_DigestVerify(ctx, sig, sig_len, data, len) == 1;
    EVP_MD_CTX_free(ctx);
    return ok ? 0 : -1;
}

static int ecdsa_der_to_raw_(const uint8_t *der, size_t der_len, uint8_t raw[64]) {
    const unsigned char *p = der;
    ECDSA_SIG *sig = d2i_ECDSA_SIG(NULL, &p, (long)der_len);
    if (!sig) return -1;
    const BIGNUM *r = NULL, *s = NULL;
    ECDSA_SIG_get0(sig, &r, &s);
    int ok = r && s && BN_bn2binpad(r, raw, 32) == 32 &&
             BN_bn2binpad(s, raw + 32, 32) == 32;
    ECDSA_SIG_free(sig);
    return ok ? 0 : -1;
}

static int ecdsa_raw_to_der_(const uint8_t raw[64], uint8_t **der, size_t *der_len) {
    BIGNUM *r = BN_bin2bn(raw, 32, NULL);
    BIGNUM *s = BN_bin2bn(raw + 32, 32, NULL);
    ECDSA_SIG *sig = ECDSA_SIG_new();
    if (!r || !s || !sig || ECDSA_SIG_set0(sig, r, s) != 1) {
        BN_free(r); BN_free(s); ECDSA_SIG_free(sig);
        return -1;
    }
    int n = i2d_ECDSA_SIG(sig, NULL);
    if (n <= 0) { ECDSA_SIG_free(sig); return -1; }
    uint8_t *buf = (uint8_t *)malloc((size_t)n);
    if (!buf) { ECDSA_SIG_free(sig); return -1; }
    uint8_t *p = buf;
    if (i2d_ECDSA_SIG(sig, &p) != n) {
        free(buf); ECDSA_SIG_free(sig); return -1;
    }
    ECDSA_SIG_free(sig);
    *der = buf;
    *der_len = (size_t)n;
    return 0;
}

static const char *jwt_alg_name_(int alg) {
    if (alg == CETCD_JWT_HS256) return "HS256";
    if (alg == CETCD_JWT_RS256) return "RS256";
    if (alg == CETCD_JWT_ES256) return "ES256";
    return NULL;
}

static int jwt_sign_(cetcd_auth_store *s, const uint8_t *data, size_t len,
                     uint8_t **out, size_t *out_len) {
    if (s->jwt_alg == CETCD_JWT_HS256) {
        uint8_t *mac = (uint8_t *)malloc(32);
        if (!mac) return -1;
        if (jwt_hmac_(s, data, len, mac) != 0) { free(mac); return -1; }
        *out = mac;
        *out_len = 32;
        return 0;
    }
    uint8_t *sig = NULL;
    size_t sl = 0;
    if (jwt_digest_sign_(s->jwt_pkey, data, len, &sig, &sl) != 0) return -1;
    if (s->jwt_alg == CETCD_JWT_ES256) {
        uint8_t raw[64];
        if (ecdsa_der_to_raw_(sig, sl, raw) != 0) { free(sig); return -1; }
        free(sig);
        sig = (uint8_t *)malloc(64);
        if (!sig) return -1;
        memcpy(sig, raw, 64);
        sl = 64;
    }
    *out = sig;
    *out_len = sl;
    return 0;
}

static int jwt_verify_(cetcd_auth_store *s, const uint8_t *data, size_t len,
                       const uint8_t *sig, size_t sig_len) {
    if (s->jwt_alg == CETCD_JWT_HS256) {
        uint8_t expect[32];
        if (sig_len != 32) return -1;
        if (jwt_hmac_(s, data, len, expect) != 0) return -1;
        return jwt_ct_eq_(sig, expect, 32) ? 0 : -1;
    }
    if (s->jwt_alg == CETCD_JWT_ES256) {
        if (sig_len != 64) return -1;
        uint8_t *der = NULL;
        size_t der_len = 0;
        if (ecdsa_raw_to_der_(sig, &der, &der_len) != 0) return -1;
        int rc = jwt_digest_verify_(s->jwt_pkey, data, len, der, der_len);
        free(der);
        return rc;
    }
    return jwt_digest_verify_(s->jwt_pkey, data, len, sig, sig_len);
}

static int jwt_claim_str_(const char *json, size_t len, const char *key,
                          char *out, size_t cap) {
    char pat[80];
    int n = snprintf(pat, sizeof(pat), "\"%s\":\"", key);
    if (n < 0 || (size_t)n >= sizeof(pat)) return -1;
    size_t plen = (size_t)n;
    if (len < plen) return -1;
    const char *hit = NULL;
    for (size_t i = 0; i + plen <= len; i++) {
        if (memcmp(json + i, pat, plen) == 0) { hit = json + i + plen; break; }
    }
    if (!hit) return -1;
    size_t o = 0;
    const char *end = json + len;
    while (hit < end && *hit != '"') {
        if (*hit == '\\' || (unsigned char)*hit < 0x20) return -1;
        if (o + 1 >= cap) return -1;
        out[o++] = *hit++;
    }
    if (hit >= end || *hit != '"') return -1;
    out[o] = '\0';
    return 0;
}

static int jwt_claim_u64_(const char *json, size_t len, const char *key, uint64_t *out) {
    char pat[80];
    int n = snprintf(pat, sizeof(pat), "\"%s\":", key);
    if (n < 0 || (size_t)n >= sizeof(pat)) return -1;
    size_t plen = (size_t)n;
    const char *hit = NULL;
    for (size_t i = 0; i + plen <= len; i++) {
        if (memcmp(json + i, pat, plen) == 0) { hit = json + i + plen; break; }
    }
    if (!hit) return -1;
    if (*hit < '0' || *hit > '9') return -1;
    uint64_t v = 0;
    while (hit < json + len && *hit >= '0' && *hit <= '9') {
        if (v > UINT64_MAX / 10) return -1;
        v = v * 10 + (uint64_t)(*hit - '0');
        hit++;
    }
    *out = v;
    return 0;
}

static char *jwt_issue_(cetcd_auth_store *s, const char *username) {
    const char *alg = jwt_alg_name_(s->jwt_alg);
    if (!alg) return NULL;
    if (strchr(username, '"') || strchr(username, '\\')) return NULL;
    char hdr[48];
    int hn = snprintf(hdr, sizeof(hdr), "{\"alg\":\"%s\",\"typ\":\"JWT\"}", alg);
    if (hn < 0 || (size_t)hn >= sizeof(hdr)) return NULL;
    uint64_t ttl = s->token_ttl_ns ? s->token_ttl_ns : CETCD_AUTH_JWT_DEFAULT_TTL_NS;
    uint64_t exp = cetcd_clock_realtime_ns() / 1000000000ULL + ttl / 1000000000ULL;
    if (ttl / 1000000000ULL == 0) exp += 1;
    s->token_seq++;
    char payload[256];
    int pn = snprintf(payload, sizeof(payload),
                      "{\"username\":\"%s\",\"revision\":%llu,\"exp\":%llu}",
                      username, (unsigned long long)s->token_seq,
                      (unsigned long long)exp);
    if (pn < 0 || (size_t)pn >= sizeof(payload)) return NULL;
    char hb[64], pb[400];
    size_t hlen = b64url_encode_((const uint8_t *)hdr, (size_t)hn, hb, sizeof(hb));
    size_t plen = b64url_encode_((const uint8_t *)payload, (size_t)pn, pb, sizeof(pb));
    if (!hlen || !plen) return NULL;
    char signing[512];
    int sl = snprintf(signing, sizeof(signing), "%s.%s", hb, pb);
    if (sl < 0 || (size_t)sl >= sizeof(signing)) return NULL;
    uint8_t *sig = NULL;
    size_t sig_len = 0;
    if (jwt_sign_(s, (const uint8_t *)signing, (size_t)sl, &sig, &sig_len) != 0)
        return NULL;
    char sb[1024];
    size_t sblen = b64url_encode_(sig, sig_len, sb, sizeof(sb));
    free(sig);
    if (!sblen) return NULL;
    size_t tok_len = (size_t)sl + 1 + sblen;
    if (tok_len >= CETCD_AUTH_MAX_TOKEN_LEN) return NULL;
    char *tok = (char *)malloc(tok_len + 1);
    if (!tok) return NULL;
    memcpy(tok, signing, (size_t)sl);
    tok[sl] = '.';
    memcpy(tok + sl + 1, sb, sblen + 1);
    return tok;
}

static const char *jwt_user_for_(cetcd_auth_store *s, const char *token, uint64_t now_ns) {
    const char *alg = jwt_alg_name_(s->jwt_alg);
    if (!alg) return NULL;
    const char *p1 = strchr(token, '.');
    if (!p1) return NULL;
    const char *p2 = strchr(p1 + 1, '.');
    if (!p2 || strchr(p2 + 1, '.')) return NULL;
    size_t hlen = (size_t)(p1 - token);
    size_t plen = (size_t)(p2 - (p1 + 1));
    size_t slen = strlen(p2 + 1);
    if (!hlen || !plen || !slen) return NULL;
    uint8_t hdr[128], pay[512], mac[1024];
    size_t hdr_n = 0, pay_n = 0, mac_n = 0;
    if (b64url_decode_(token, hlen, hdr, sizeof(hdr) - 1, &hdr_n) != 0) return NULL;
    if (b64url_decode_(p1 + 1, plen, pay, sizeof(pay) - 1, &pay_n) != 0) return NULL;
    if (b64url_decode_(p2 + 1, slen, mac, sizeof(mac), &mac_n) != 0) return NULL;
    hdr[hdr_n] = '\0';
    pay[pay_n] = '\0';
    if (!find_bytes_(hdr, hdr_n, alg, strlen(alg)) ||
        find_bytes_(hdr, hdr_n, "\"none\"", 6))
        return NULL;
    size_t sig_in_len = hlen + 1 + plen;
    if (jwt_verify_(s, (const uint8_t *)token, sig_in_len, mac, mac_n) != 0)
        return NULL;
    char user[128];
    uint64_t rev = 0, exp = 0;
    if (jwt_claim_str_((const char *)pay, pay_n, "username", user, sizeof(user)) != 0)
        return NULL;
    if (jwt_claim_u64_((const char *)pay, pay_n, "revision", &rev) != 0) return NULL;
    if (jwt_claim_u64_((const char *)pay, pay_n, "exp", &exp) != 0) return NULL;
    (void)rev;
    uint64_t now_s = now_ns / 1000000000ULL;
    if (now_s >= exp) return NULL;
    if (!cetcd_find_user(s, user)) return NULL;
    strncpy(s->jwt_user, user, sizeof(s->jwt_user) - 1);
    s->jwt_user[sizeof(s->jwt_user) - 1] = '\0';
    return s->jwt_user;
}
#endif

static int jwt_apply_spec_(cetcd_auth_store *s, const char *sign_method,
                           const char *priv_key, const char *ttl) {
#if !CETCD_HAS_OPENSSL
    (void)s; (void)sign_method; (void)priv_key; (void)ttl;
    return CETCD_ERR_UNSUPPORT;
#else
    if (!sign_method || !sign_method[0]) return CETCD_ERR_INVAL;
    int alg = CETCD_JWT_NONE;
    if (strcmp(sign_method, "HS256") == 0) alg = CETCD_JWT_HS256;
    else if (strcmp(sign_method, "RS256") == 0) alg = CETCD_JWT_RS256;
    else if (strcmp(sign_method, "ES256") == 0) alg = CETCD_JWT_ES256;
    else return CETCD_ERR_UNSUPPORT;
    if (!priv_key || !priv_key[0]) return CETCD_ERR_INVAL;
    if (ttl && ttl[0]) {
        uint64_t ns = 0;
        if (parse_go_duration_ns_(ttl, &ns) != 0 || ns == 0)
            return CETCD_ERR_INVAL;
        s->token_ttl_ns = ns;
    } else {
        s->token_ttl_ns = CETCD_AUTH_JWT_DEFAULT_TTL_NS;
    }
    if (alg == CETCD_JWT_HS256) {
        uint8_t *key = NULL;
        size_t key_len = 0;
        int rc = read_whole_file_(priv_key, &key, &key_len);
        if (rc != CETCD_OK) return rc;
        free(s->jwt_key);
        s->jwt_key = key;
        s->jwt_key_len = key_len;
        EVP_PKEY_free(s->jwt_pkey);
        s->jwt_pkey = NULL;
        s->jwt_alg = alg;
        return CETCD_OK;
    }
    BIO *bio = BIO_new_file(priv_key, "r");
    if (!bio) return CETCD_ERR_IO;
    EVP_PKEY *pk = PEM_read_bio_PrivateKey(bio, NULL, NULL, NULL);
    BIO_free(bio);
    if (!pk) return CETCD_ERR_INVAL;
    if (alg == CETCD_JWT_RS256 && !EVP_PKEY_is_a(pk, "RSA")) {
        EVP_PKEY_free(pk);
        return CETCD_ERR_INVAL;
    }
    if (alg == CETCD_JWT_ES256 &&
        (!EVP_PKEY_is_a(pk, "EC") || EVP_PKEY_get_bits(pk) != 256)) {
        EVP_PKEY_free(pk);
        return CETCD_ERR_INVAL;
    }
    free(s->jwt_key);
    s->jwt_key = NULL;
    s->jwt_key_len = 0;
    EVP_PKEY_free(s->jwt_pkey);
    s->jwt_pkey = pk;
    s->jwt_alg = alg;
    return CETCD_OK;
#endif
}

int cetcd_auth_set_token_spec(cetcd_auth_store *s, const char *spec) {
    if (!s || !spec) return CETCD_ERR_INVAL;
    if (spec[0] == '\0' || strcmp(spec, "simple") == 0) return CETCD_OK;
    if (strncmp(spec, "simple,", 7) == 0) return CETCD_OK;
    if (strncmp(spec, "jwt", 3) != 0 || (spec[3] != '\0' && spec[3] != ','))
        return CETCD_ERR_INVAL;

    char buf[512];
    if (strlen(spec) >= sizeof(buf)) return CETCD_ERR_INVAL;
    memcpy(buf, spec, strlen(spec) + 1);
    const char *sign_method = NULL;
    const char *priv_key = NULL;
    const char *ttl = NULL;
    char *p = strchr(buf, ',');
    p = p ? p + 1 : NULL;
    while (p && *p) {
        char *next = strchr(p, ',');
        if (next) *next = '\0';
        char *eq = strchr(p, '=');
        if (!eq || eq == p) return CETCD_ERR_INVAL;
        *eq = '\0';
        const char *k = p;
        const char *v = eq + 1;
        if (strcmp(k, "sign-method") == 0) sign_method = v;
        else if (strcmp(k, "priv-key") == 0) priv_key = v;
        else if (strcmp(k, "ttl") == 0) ttl = v;
        else if (strcmp(k, "pub-key") == 0) { /* priv-key is enough to sign+verify */ }
        else return CETCD_ERR_INVAL;
        p = next ? next + 1 : NULL;
    }
    return jwt_apply_spec_(s, sign_method, priv_key, ttl);
}

void cetcd_auth_set_token_ttl_ns(cetcd_auth_store *s, uint64_t ttl_ns) {
    if (s) s->token_ttl_ns = ttl_ns ? ttl_ns : CETCD_AUTH_DEFAULT_TOKEN_TTL_NS;
}

char *cetcd_auth_issue_token(cetcd_auth_store *s, const char *username) {
    if (!s || !username || !s->tokens) return NULL;
    if (!cetcd_find_user(s, username)) return NULL;
#if CETCD_HAS_OPENSSL
    if (s->jwt_alg)
        return jwt_issue_(s, username);
#endif

    uint8_t raw[CETCD_AUTH_TOKEN_RAW];
    char hex[CETCD_AUTH_TOKEN_HEX + 1];
    cetcd_auth_token_ent *ent = (cetcd_auth_token_ent *)calloc(1, sizeof(*ent));
    if (!ent) return NULL;
    strncpy(ent->username, username, sizeof(ent->username) - 1);
    uint64_t ttl = s->token_ttl_ns ? s->token_ttl_ns : CETCD_AUTH_DEFAULT_TOKEN_TTL_NS;
    ent->expires_ns = cetcd_clock_realtime_ns() + ttl;

    /* Mix a counter so even a weak RNG still yields unique tokens. */
    s->token_seq++;
    auth_fill_random_(raw, sizeof(raw));
    raw[0] ^= (uint8_t)s->token_seq;
    raw[1] ^= (uint8_t)(s->token_seq >> 8);
    auth_hex_encode_(raw, sizeof(raw), hex);

    cetcd_slice key = cetcd_slice_make(hex, CETCD_AUTH_TOKEN_HEX);
    if (cetcd_hashmap_put(s->tokens, key, ent) != 0) {
        free(ent);
        return NULL;
    }
    char *out = (char *)malloc(CETCD_AUTH_TOKEN_HEX + 1);
    if (!out) {
        void *drop = NULL;
        cetcd_hashmap_remove(s->tokens, key, &drop);
        free(ent);
        return NULL;
    }
    memcpy(out, hex, CETCD_AUTH_TOKEN_HEX + 1);
    return out;
}

const char *cetcd_auth_user_for_token(cetcd_auth_store *s, const char *token,
                                      uint64_t now_ns) {
    if (!s || !token || token[0] == '\0') return NULL;
#if CETCD_HAS_OPENSSL
    if (s->jwt_alg)
        return jwt_user_for_(s, token, now_ns);
#endif
    if (!s->tokens) return NULL;
    size_t tlen = strlen(token);
    cetcd_slice key = cetcd_slice_make(token, tlen);
    void *v = NULL;
    if (!cetcd_hashmap_get(s->tokens, key, &v) || !v) return NULL;
    cetcd_auth_token_ent *ent = (cetcd_auth_token_ent *)v;
    if (now_ns >= ent->expires_ns) {
        void *drop = NULL;
        cetcd_hashmap_remove(s->tokens, key, &drop);
        free(ent);
        return NULL;
    }
    return ent->username;
}

struct auth_tok_collect {
    cetcd_slice *keys;
    size_t       n;
    size_t       cap;
    const char  *username;
};

static bool auth_collect_user_tokens_(cetcd_slice key, void *value, void *udata) {
    struct auth_tok_collect *c = (struct auth_tok_collect *)udata;
    cetcd_auth_token_ent *ent = (cetcd_auth_token_ent *)value;
    if (!ent || (c->username && strcmp(ent->username, c->username) != 0))
        return true;
    if (c->n == c->cap) {
        size_t ncap = c->cap ? c->cap * 2 : 8;
        cetcd_slice *nk = (cetcd_slice *)realloc(c->keys, ncap * sizeof(*nk));
        if (!nk) return false;
        c->keys = nk;
        c->cap = ncap;
    }
    /* Copy key bytes: hashmap key is valid until remove. */
    uint8_t *dup = (uint8_t *)malloc(key.len);
    if (!dup) return false;
    memcpy(dup, key.data, key.len);
    c->keys[c->n].data = dup;
    c->keys[c->n].len = key.len;
    c->n++;
    return true;
}

static void auth_drop_collected_tokens_(cetcd_auth_store *s, struct auth_tok_collect *c) {
    for (size_t i = 0; i < c->n; i++) {
        void *drop = NULL;
        if (cetcd_hashmap_remove(s->tokens, c->keys[i], &drop))
            free(drop);
        free((void *)(uintptr_t)c->keys[i].data);
    }
    free(c->keys);
}

void cetcd_auth_revoke_user_tokens(cetcd_auth_store *s, const char *username) {
    if (!s || !username) return;
    if (s->jwt_alg) return; /* etcd JWT invalidateUser is a no-op */
    if (!s->tokens) return;
    struct auth_tok_collect c;
    memset(&c, 0, sizeof(c));
    c.username = username;
    cetcd_hashmap_iter(s->tokens, auth_collect_user_tokens_, &c);
    auth_drop_collected_tokens_(s, &c);
}

void cetcd_auth_revoke_all_tokens(cetcd_auth_store *s) {
    if (!s || !s->tokens) return;
    cetcd_hashmap_iter(s->tokens, cetcd_free_token_cb, NULL);
    cetcd_hashmap_free(s->tokens);
    s->tokens = cetcd_hashmap_new(8);
}

bool cetcd_auth_is_admin(const cetcd_auth_store *s, const char *username) {
    if (!s || !username) return false;
    if (strcmp(username, "root") == 0) return true;
    const cetcd_user *u = cetcd_find_user(s, username);
    if (!u || !u->roles || u->n_roles == 0) return false;
    const char *p = u->roles;
    for (size_t i = 0; i < u->n_roles; i++) {
        size_t l = strlen(p);
        if (l == 4 && memcmp(p, "root", 4) == 0) return true;
        p += l + 1;
    }
    return false;
}

static int auth_prefix_match_(const char *prefix, size_t plen,
                              const uint8_t *key, size_t klen) {
    if (plen == 0) return 1; /* empty prefix = all keys */
    if (klen < plen) return 0;
    return memcmp(key, prefix, plen) == 0;
}

bool cetcd_auth_check_perm(const cetcd_auth_store *s, const char *username,
                           const uint8_t *key, size_t key_len, int want_write) {
    if (!s) return false;
    if (!s->enabled) return true;
    if (!username || !key) return false;
    if (cetcd_auth_is_admin(s, username)) return true;
    const cetcd_user *u = cetcd_find_user(s, username);
    if (!u) return false;
    const char *p = u->roles;
    for (size_t i = 0; i < u->n_roles; i++) {
        size_t l = strlen(p);
        cetcd_role *r = cetcd_find_role(s, p);
        p += l + 1;
        if (!r) continue;
        if (want_write) {
            if (!r->perm_write) continue;
        } else {
            if (!r->perm_read && !r->perm_write) continue;
        }
        if (auth_prefix_match_(r->key_prefix, r->key_prefix_len, key, key_len))
            return true;
    }
    return false;
}

/* ── Persistence (single-txn snapshot of users + roles + enabled) ── */

#define AUTH_BUCKET      "auth"
#define AUTH_KEY_ENABLED "enabled"
#define AUTH_KEY_USERS   "users"
#define AUTH_KEY_ROLES   "roles"

struct auth_save_ctx {
    uint8_t *buf;
    size_t   len;
    size_t   cap;
    int      err;
};

static int auth_save_grow_(struct auth_save_ctx *c, size_t need) {
    if (c->err) return -1;
    if (c->len + need <= c->cap) return 0;
    size_t ncap = c->cap ? c->cap : 256;
    while (ncap < c->len + need) {
        if (ncap > (SIZE_MAX / 2)) { c->err = 1; return -1; }
        ncap *= 2;
    }
    uint8_t *nbuf = (uint8_t *)realloc(c->buf, ncap);
    if (!nbuf) { c->err = 1; return -1; }
    c->buf = nbuf;
    c->cap = ncap;
    return 0;
}

static void auth_save_u8_(struct auth_save_ctx *c, uint8_t v) {
    if (auth_save_grow_(c, 1) != 0) return;
    c->buf[c->len++] = v;
}

static void auth_save_u16_(struct auth_save_ctx *c, uint16_t v) {
    if (auth_save_grow_(c, 2) != 0) return;
    c->buf[c->len++] = (uint8_t)(v & 0xFF);
    c->buf[c->len++] = (uint8_t)((v >> 8) & 0xFF);
}

static void auth_save_bytes_(struct auth_save_ctx *c, const void *p, size_t n) {
    if (auth_save_grow_(c, n) != 0) return;
    if (n) memcpy(c->buf + c->len, p, n);
    c->len += n;
}

static bool auth_save_user_cb_(cetcd_slice key, void *value, void *udata) {
    struct auth_save_ctx *c = (struct auth_save_ctx *)udata;
    cetcd_user *u = (cetcd_user *)value;
    if (!u) return true;
    size_t nlen = key.len < 0xFFFF ? key.len : 0xFFFF;
    size_t rlen = cetcd_user_roles_total_len(u);
    if (rlen > 0xFFFF) rlen = 0xFFFF;
    auth_save_u16_(c, (uint16_t)nlen);
    auth_save_bytes_(c, key.data, nlen);
    {
        uint8_t hl = u->hash_len > 64 ? 64 : (uint8_t)u->hash_len;
        auth_save_u8_(c, hl);
        auth_save_bytes_(c, u->password_hash, hl);
    }
    auth_save_u16_(c, (uint16_t)u->n_roles);
    auth_save_u16_(c, (uint16_t)rlen);
    if (rlen && u->roles) auth_save_bytes_(c, u->roles, rlen);
    return c->err == 0;
}

static bool auth_save_role_cb_(cetcd_slice key, void *value, void *udata) {
    struct auth_save_ctx *c = (struct auth_save_ctx *)udata;
    cetcd_role *r = (cetcd_role *)value;
    if (!r) return true;
    size_t nlen = key.len < 0xFFFF ? key.len : 0xFFFF;
    size_t plen = r->key_prefix_len < 0xFFFF ? r->key_prefix_len : 0xFFFF;
    auth_save_u16_(c, (uint16_t)nlen);
    auth_save_bytes_(c, key.data, nlen);
    auth_save_u8_(c, (uint8_t)r->perm_read);
    auth_save_u8_(c, (uint8_t)r->perm_write);
    auth_save_u16_(c, (uint16_t)plen);
    auth_save_bytes_(c, r->key_prefix, plen);
    return c->err == 0;
}

int cetcd_auth_save(const cetcd_auth_store *s, struct cetcd_backend *be) {
    if (!s || !be) return CETCD_ERR_INVAL;
    uint8_t en = s->enabled ? 1 : 0;
    struct auth_save_ctx users;
    struct auth_save_ctx roles;
    memset(&users, 0, sizeof(users));
    memset(&roles, 0, sizeof(roles));
    cetcd_hashmap_iter(s->users, auth_save_user_cb_, &users);
    cetcd_hashmap_iter(s->roles, auth_save_role_cb_, &roles);
    if (users.err || roles.err) {
        free(users.buf);
        free(roles.buf);
        return CETCD_ERR_NOMEM;
    }
    cetcd_txn *txn = cetcd_txn_begin(be, false);
    if (!txn) {
        free(users.buf);
        free(roles.buf);
        return CETCD_ERR_IO;
    }
    int rc = cetcd_txn_put(txn, AUTH_BUCKET,
                           (const uint8_t *)AUTH_KEY_ENABLED, strlen(AUTH_KEY_ENABLED),
                           &en, 1);
    if (rc == CETCD_OK)
        rc = cetcd_txn_put(txn, AUTH_BUCKET,
                           (const uint8_t *)AUTH_KEY_USERS, strlen(AUTH_KEY_USERS),
                           users.buf ? users.buf : (const uint8_t *)"",
                           users.buf ? users.len : 0);
    if (rc == CETCD_OK)
        rc = cetcd_txn_put(txn, AUTH_BUCKET,
                           (const uint8_t *)AUTH_KEY_ROLES, strlen(AUTH_KEY_ROLES),
                           roles.buf ? roles.buf : (const uint8_t *)"",
                           roles.buf ? roles.len : 0);
    free(users.buf);
    free(roles.buf);
    if (rc != CETCD_OK) {
        cetcd_txn_abort(txn);
        return rc;
    }
    return cetcd_txn_commit(txn);
}

static uint16_t auth_load_u16_(const uint8_t *p) {
    return (uint16_t)p[0] | ((uint16_t)p[1] << 8);
}

int cetcd_auth_load(cetcd_auth_store *s, struct cetcd_backend *be) {
    if (!s || !be) return CETCD_ERR_INVAL;

    uint8_t *val = NULL;
    size_t vlen = 0;
    int rc = cetcd_backend_get(be, AUTH_BUCKET,
                               (const uint8_t *)AUTH_KEY_ENABLED,
                               strlen(AUTH_KEY_ENABLED), &val, &vlen);
    if (rc == CETCD_OK && val && vlen > 0)
        s->enabled = val[0] != 0;
    else if (rc != CETCD_OK && rc != CETCD_ERR_NOTFOUND)
        return rc;
    free(val);
    val = NULL;

    rc = cetcd_backend_get(be, AUTH_BUCKET,
                           (const uint8_t *)AUTH_KEY_USERS,
                           strlen(AUTH_KEY_USERS), &val, &vlen);
    if (rc == CETCD_OK && val) {
        size_t pos = 0;
        while (pos + 2 <= vlen) {
            uint16_t nlen = auth_load_u16_(val + pos); pos += 2;
            if (pos + nlen + 1 > vlen) break;
            char name[128];
            size_t copy = nlen < sizeof(name) - 1 ? nlen : sizeof(name) - 1;
            memcpy(name, val + pos, copy);
            name[copy] = '\0';
            pos += nlen;
            uint8_t hlen = val[pos++];
            if (hlen > 64 || pos + hlen + 2 + 2 > vlen) break;
            const uint8_t *hash = val + pos;
            pos += hlen;
            uint16_t n_roles = auth_load_u16_(val + pos); pos += 2;
            uint16_t rlen = auth_load_u16_(val + pos); pos += 2;
            if (pos + rlen > vlen) break;
            cetcd_user *u = (cetcd_user *)calloc(1, sizeof(*u));
            if (!u) { free(val); return CETCD_ERR_NOMEM; }
            strncpy(u->name, name, sizeof(u->name) - 1);
            memcpy(u->password_hash, hash, hlen);
            u->hash_len = hlen;
            u->n_roles = n_roles;
            if (rlen > 0) {
                u->roles = (char *)malloc(rlen);
                if (!u->roles) { free(u); free(val); return CETCD_ERR_NOMEM; }
                memcpy(u->roles, val + pos, rlen);
            }
            pos += rlen;
            if (cetcd_hashmap_put(s->users, auth_key_(u->name), u) != 0) {
                free(u->roles);
                free(u);
                free(val);
                return CETCD_ERR_NOMEM;
            }
        }
        free(val);
        val = NULL;
    } else if (rc != CETCD_OK && rc != CETCD_ERR_NOTFOUND) {
        return rc;
    }
    free(val);
    val = NULL;

    rc = cetcd_backend_get(be, AUTH_BUCKET,
                           (const uint8_t *)AUTH_KEY_ROLES,
                           strlen(AUTH_KEY_ROLES), &val, &vlen);
    if (rc == CETCD_OK && val) {
        size_t pos = 0;
        while (pos + 2 <= vlen) {
            uint16_t nlen = auth_load_u16_(val + pos); pos += 2;
            if (pos + nlen + 1 + 1 + 2 > vlen) break;
            char name[128];
            size_t copy = nlen < sizeof(name) - 1 ? nlen : sizeof(name) - 1;
            memcpy(name, val + pos, copy);
            name[copy] = '\0';
            pos += nlen;
            uint8_t pr = val[pos++];
            uint8_t pw = val[pos++];
            uint16_t plen = auth_load_u16_(val + pos); pos += 2;
            if (pos + plen > vlen) break;
            cetcd_role *r = (cetcd_role *)calloc(1, sizeof(*r));
            if (!r) { free(val); return CETCD_ERR_NOMEM; }
            strncpy(r->name, name, sizeof(r->name) - 1);
            r->perm_read = pr ? 1 : 0;
            r->perm_write = pw ? 1 : 0;
            r->key_prefix_len = plen < sizeof(r->key_prefix) ? plen : sizeof(r->key_prefix) - 1;
            if (r->key_prefix_len)
                memcpy(r->key_prefix, val + pos, r->key_prefix_len);
            pos += plen;
            if (cetcd_hashmap_put(s->roles, auth_key_(r->name), r) != 0) {
                free(r);
                free(val);
                return CETCD_ERR_NOMEM;
            }
        }
        free(val);
        val = NULL;
    } else if (rc != CETCD_OK && rc != CETCD_ERR_NOTFOUND) {
        return rc;
    }
    free(val);
    return CETCD_OK;
}
