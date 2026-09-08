#include "cetcd/v3rpc.h"
#include "cetcd/auth.h"
#include "cetcd/mvcc.h"
#include <stdlib.h>
#include <string.h>

extern cetcd_auth_store *g_rpc_auth;
extern cetcd_mvcc_store *g_rpc_store;

cetcd_rpc_bytes auth_handle_enable(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes auth_handle_disable(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes auth_handle_authenticate(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes auth_handle_user_add(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes auth_handle_user_delete(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes auth_handle_role_add(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes auth_handle_role_grant(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes auth_handle_status(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes auth_handle_user_list(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes auth_handle_user_change_password(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes auth_handle_role_list(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes auth_handle_role_delete(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes auth_handle_user_revoke_role(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes auth_handle_user_get(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes auth_handle_role_get(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes auth_handle_role_grant_permission(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);
cetcd_rpc_bytes auth_handle_role_revoke_permission(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len);

static cetcd_rpc_bytes simple_ok_response(void) {
    /* Return a ResponseHeader with current revision.
     * ResponseHeader: field 1 (header) = length-delimited, tag = 0x0a
     *   field 3 (revision) = int64, tag = 0x18 */
    int64_t rev = g_rpc_store ? cetcd_mvcc_revision(g_rpc_store) : 0;
    uint8_t hdr_buf[32]; size_t hp = 0;
    hdr_buf[hp++] = 0x18; /* revision */
    uint64_t rv = (uint64_t)(rev > 0 ? rev : 1);
    while (hp < sizeof(hdr_buf)) {
        uint8_t b = rv & 0x7F; rv >>= 7;
        if (rv) b |= 0x80;
        hdr_buf[hp++] = b;
        if (!rv) break;
    }
    uint8_t *b = (uint8_t *)malloc(hp + 2);
    if (!b) return (cetcd_rpc_bytes){NULL, 0};
    size_t pos = 0;
    b[pos++] = 0x0a; /* field 1 = header */
    b[pos++] = (uint8_t)hp;
    memcpy(b + pos, hdr_buf, hp); pos += hp;
    return (cetcd_rpc_bytes){b, pos};
}

static cetcd_rpc_bytes make_response(const uint8_t *data, size_t len) {
    uint8_t *b = (uint8_t *)malloc(len);
    memcpy(b, data, len);
    return (cetcd_rpc_bytes){b, len};
}

/* Write a ResponseHeader (field 1, tag 0x0a) with current revision to buf at pos.
 * Returns the new position after the header. */
static size_t write_header_prefix(uint8_t *buf, size_t cap, size_t pos) {
    int64_t rev = g_rpc_store ? cetcd_mvcc_revision(g_rpc_store) : 0;
    uint8_t hdr[16]; size_t hp = 0;
    hdr[hp++] = 0x18; /* revision */
    uint64_t rv = (uint64_t)(rev > 0 ? rev : 1);
    do { uint8_t b = rv & 0x7F; rv >>= 7; if (rv) b |= 0x80; hdr[hp++] = b; } while (rv);
    buf[pos++] = 0x0a; /* field 1 = header */
    buf[pos++] = (uint8_t)hp;
    memcpy(buf + pos, hdr, hp); pos += hp;
    return pos;
}

cetcd_rpc_bytes auth_handle_enable(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len) {
    (void)rpc; (void)req; (void)req_len;
    /* etcd: AuthEnable requires a "root" user to already exist. */
    if (!g_rpc_auth || !cetcd_auth_has_user(g_rpc_auth, "root"))
        return (cetcd_rpc_bytes){NULL, 0};
    uint8_t *entry = NULL;
    size_t elen = 0;
    if (cetcd_apply_encode_auth_enabled(&entry, &elen, 1) != 0)
        return (cetcd_rpc_bytes){NULL, 0};
    int rc = cetcd_v3rpc_propose_or_apply(entry, elen);
    free(entry);
    if (rc < 0)
        return (cetcd_rpc_bytes){NULL, 0};
    return simple_ok_response();
}

cetcd_rpc_bytes auth_handle_disable(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len) {
    (void)rpc; (void)req; (void)req_len;
    if (!g_rpc_auth)
        return simple_ok_response();
    uint8_t *entry = NULL;
    size_t elen = 0;
    if (cetcd_apply_encode_auth_enabled(&entry, &elen, 0) != 0)
        return (cetcd_rpc_bytes){NULL, 0};
    int rc = cetcd_v3rpc_propose_or_apply(entry, elen);
    free(entry);
    if (rc < 0)
        return (cetcd_rpc_bytes){NULL, 0};
    return simple_ok_response();
}

/* leftover-safe AuthenticateRequest. v3rpc cannot link server. */
static void auth_name_pass_clear_(uint8_t **name, uint8_t **pass) {
    free(*name);
    free(*pass);
    *name = NULL;
    *pass = NULL;
}

static int parse_auth_name_pass_request_(const uint8_t *req, size_t len,
                                         uint8_t **name, size_t *name_len,
                                         uint8_t **pass, size_t *pass_len,
                                         int *no_password) {
    size_t p = 0;
    if (!name || !name_len || !pass || !pass_len) return -1;
    *name = NULL; *name_len = 0;
    *pass = NULL; *pass_len = 0;
    if (no_password) *no_password = 0;
    if (!req || len == 0) return 0;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x0a || tag == 0x12) {
            uint64_t skip = 0;
            int shift = 0;
            int got = 0;
            while (p < len) {
                uint8_t b = req[p++];
                skip |= (uint64_t)(b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    got = 1;
                    break;
                }
                shift += 7;
                if (shift > 63) {
                    auth_name_pass_clear_(name, pass);
                    return -1;
                }
            }
            if (!got || p + skip > len) {
                auth_name_pass_clear_(name, pass);
                return -1;
            }
            if (skip == 0) {
                if (tag == 0x0a) {
                    free(*name);
                    *name = NULL;
                    *name_len = 0;
                } else {
                    free(*pass);
                    *pass = NULL;
                    *pass_len = 0;
                }
                continue;
            }
            uint8_t *copy = (uint8_t *)malloc((size_t)skip + 1);
            if (!copy) {
                auth_name_pass_clear_(name, pass);
                return -1;
            }
            memcpy(copy, req + p, (size_t)skip);
            copy[skip] = 0;
            p += (size_t)skip;
            if (tag == 0x0a) {
                free(*name);
                *name = copy;
                *name_len = (size_t)skip;
            } else {
                free(*pass);
                *pass = copy;
                *pass_len = (size_t)skip;
            }
            continue;
        }
        if (tag == 0x1a && no_password) {
            uint64_t skip = 0;
            int shift = 0;
            int got = 0;
            while (p < len) {
                uint8_t b = req[p++];
                skip |= (uint64_t)(b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    got = 1;
                    break;
                }
                shift += 7;
                if (shift > 63) {
                    auth_name_pass_clear_(name, pass);
                    return -1;
                }
            }
            if (!got || p + skip > len) {
                auth_name_pass_clear_(name, pass);
                return -1;
            }
            size_t oend = p + (size_t)skip;
            while (p < oend) {
                uint8_t otag = req[p++];
                if (otag == 0x00)
                    continue;
                if (otag == 0x08) {
                    uint64_t v = 0;
                    shift = 0;
                    got = 0;
                    while (p < oend) {
                        uint8_t b = req[p++];
                        v |= (uint64_t)(b & 0x7F) << shift;
                        if ((b & 0x80) == 0) {
                            got = 1;
                            break;
                        }
                        shift += 7;
                        if (shift > 63) {
                            auth_name_pass_clear_(name, pass);
                            return -1;
                        }
                    }
                    if (!got) {
                        auth_name_pass_clear_(name, pass);
                        return -1;
                    }
                    *no_password = v != 0;
                    continue;
                }
                if ((otag & 7) == 0) {
                    got = 0;
                    shift = 0;
                    while (p < oend) {
                        uint8_t b = req[p++];
                        if ((b & 0x80) == 0) {
                            got = 1;
                            break;
                        }
                        shift += 7;
                        if (shift > 63) {
                            auth_name_pass_clear_(name, pass);
                            return -1;
                        }
                    }
                    if (!got) {
                        auth_name_pass_clear_(name, pass);
                        return -1;
                    }
                    continue;
                }
                if ((otag & 7) == 2) {
                    uint64_t iskip = 0;
                    shift = 0;
                    got = 0;
                    while (p < oend) {
                        uint8_t b = req[p++];
                        iskip |= (uint64_t)(b & 0x7F) << shift;
                        if ((b & 0x80) == 0) {
                            got = 1;
                            break;
                        }
                        shift += 7;
                        if (shift > 63) {
                            auth_name_pass_clear_(name, pass);
                            return -1;
                        }
                    }
                    if (!got || p + iskip > oend) {
                        auth_name_pass_clear_(name, pass);
                        return -1;
                    }
                    p += (size_t)iskip;
                    continue;
                }
                auth_name_pass_clear_(name, pass);
                return -1;
            }
            continue;
        }
        if ((tag & 7) == 0) {
            int shift = 0;
            int got = 0;
            while (p < len) {
                uint8_t b = req[p++];
                if ((b & 0x80) == 0) {
                    got = 1;
                    break;
                }
                shift += 7;
                if (shift > 63) {
                    auth_name_pass_clear_(name, pass);
                    return -1;
                }
            }
            if (!got) {
                auth_name_pass_clear_(name, pass);
                return -1;
            }
            continue;
        }
        if ((tag & 7) == 2) {
            uint64_t skip = 0;
            int shift = 0;
            int got = 0;
            while (p < len) {
                uint8_t b = req[p++];
                skip |= (uint64_t)(b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    got = 1;
                    break;
                }
                shift += 7;
                if (shift > 63) {
                    auth_name_pass_clear_(name, pass);
                    return -1;
                }
            }
            if (!got || p + skip > len) {
                auth_name_pass_clear_(name, pass);
                return -1;
            }
            p += (size_t)skip;
            continue;
        }
        auth_name_pass_clear_(name, pass);
        return -1;
    }
    return 0;
}

static int parse_auth_name_request_(const uint8_t *req, size_t len,
                                    uint8_t **name, size_t *name_len) {
    uint8_t *pass = NULL;
    size_t pass_len = 0;
    int rc = parse_auth_name_pass_request_(req, len, name, name_len,
                                           &pass, &pass_len, NULL);
    free(pass);
    return rc;
}

cetcd_rpc_bytes auth_handle_authenticate(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len) {
    (void)rpc;
    uint8_t *name = NULL; size_t name_len = 0;
    uint8_t *pass = NULL; size_t pass_len = 0;
    if (parse_auth_name_pass_request_(req, req_len, &name, &name_len,
                                      &pass, &pass_len, NULL) != 0)
        return (cetcd_rpc_bytes){NULL, 0};
    bool ok = false;
    char *tok = NULL;
    if (g_rpc_auth && name && pass) {
        ok = cetcd_auth_check_password(g_rpc_auth, (const char *)name, (const char *)pass);
        if (ok) tok = cetcd_auth_issue_token(g_rpc_auth, (const char *)name);
    }
    free(name); free(pass);
    if (!ok || !tok) {
        free(tok);
        return (cetcd_rpc_bytes){NULL, 0};
    }
    size_t tlen = strlen(tok);
    if (tlen > 64) tlen = 64;
    /* AuthenticateResponse: field 1 (header) + field 2 (token) */
    uint8_t buf[96];
    size_t bpos = write_header_prefix(buf, sizeof(buf), 0);
    buf[bpos++] = 0x12; /* field 2 = token */
    buf[bpos++] = (uint8_t)tlen;
    memcpy(buf + bpos, tok, tlen); bpos += tlen;
    free(tok);
    return make_response(buf, bpos);
}

cetcd_rpc_bytes auth_handle_user_add(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len) {
    (void)rpc;
    uint8_t *name = NULL; size_t name_len = 0;
    uint8_t *pass = NULL; size_t pass_len = 0;
    int no_password = 0;
    if (parse_auth_name_pass_request_(req, req_len, &name, &name_len,
                                      &pass, &pass_len, &no_password) != 0)
        return (cetcd_rpc_bytes){NULL, 0};
    int rc = -1;
    if (g_rpc_auth && name) {
        const char *pw = (pass || no_password) ? (const char *)pass : NULL;
        if (no_password && !pass) pw = "";
        if (pw) {
            if (cetcd_auth_has_user(g_rpc_auth, (const char *)name)) {
                free(name); free(pass);
                return (cetcd_rpc_bytes){NULL, 0};
            }
            uint8_t hash[64];
            size_t hlen = 0;
            if (cetcd_auth_hash_password(g_rpc_auth, pw, hash, sizeof(hash), &hlen) != CETCD_OK) {
                free(name); free(pass);
                return (cetcd_rpc_bytes){NULL, 0};
            }
            uint8_t *entry = NULL;
            size_t elen = 0;
            if (cetcd_apply_encode_auth_user_add(&entry, &elen,
                    name, strlen((const char *)name), hash, hlen) != 0) {
                free(name); free(pass);
                return (cetcd_rpc_bytes){NULL, 0};
            }
            rc = cetcd_v3rpc_propose_or_apply(entry, elen) < 0 ? -1 : CETCD_OK;
            free(entry);
        }
    }
    free(name); free(pass);
    if (rc != CETCD_OK) return (cetcd_rpc_bytes){NULL, 0};
    return simple_ok_response();
}

cetcd_rpc_bytes auth_handle_user_delete(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len) {
    (void)rpc;
    uint8_t *name = NULL; size_t name_len = 0;
    if (parse_auth_name_request_(req, req_len, &name, &name_len) != 0)
        return (cetcd_rpc_bytes){NULL, 0};
    if (!g_rpc_auth || !name || name_len == 0 ||
        !cetcd_auth_has_user(g_rpc_auth, (const char *)name)) {
        free(name);
        return (cetcd_rpc_bytes){NULL, 0};
    }
    uint8_t *entry = NULL;
    size_t elen = 0;
    if (cetcd_apply_encode_auth_user_delete(&entry, &elen, name, name_len) != 0) {
        free(name);
        return (cetcd_rpc_bytes){NULL, 0};
    }
    int rc = cetcd_v3rpc_propose_or_apply(entry, elen);
    free(entry);
    free(name);
    if (rc < 0) return (cetcd_rpc_bytes){NULL, 0};
    return simple_ok_response();
}

cetcd_rpc_bytes auth_handle_role_add(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len) {
    (void)rpc;
    uint8_t *name = NULL; size_t name_len = 0;
    if (parse_auth_name_request_(req, req_len, &name, &name_len) != 0)
        return (cetcd_rpc_bytes){NULL, 0};
    if (!g_rpc_auth || !name || name_len == 0 ||
        cetcd_auth_get_role(g_rpc_auth, (const char *)name)) {
        free(name);
        return (cetcd_rpc_bytes){NULL, 0};
    }
    uint8_t *entry = NULL;
    size_t elen = 0;
    if (cetcd_apply_encode_auth_role_add(&entry, &elen, name, name_len) != 0) {
        free(name);
        return (cetcd_rpc_bytes){NULL, 0};
    }
    int rc = cetcd_v3rpc_propose_or_apply(entry, elen);
    free(entry);
    free(name);
    if (rc < 0) return (cetcd_rpc_bytes){NULL, 0};
    return simple_ok_response();
}

cetcd_rpc_bytes auth_handle_role_grant(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len) {
    (void)rpc;
    uint8_t *user = NULL; size_t user_len = 0;
    uint8_t *role = NULL; size_t role_len = 0;
    if (parse_auth_name_pass_request_(req, req_len, &user, &user_len,
                                      &role, &role_len, NULL) != 0)
        return (cetcd_rpc_bytes){NULL, 0};
    if (!g_rpc_auth || !user || user_len == 0 || !role || role_len == 0 ||
        !cetcd_auth_has_user(g_rpc_auth, (const char *)user) ||
        !cetcd_auth_get_role(g_rpc_auth, (const char *)role)) {
        free(user); free(role);
        return (cetcd_rpc_bytes){NULL, 0};
    }
    uint8_t *entry = NULL;
    size_t elen = 0;
    if (cetcd_apply_encode_auth_user_grant_role(&entry, &elen,
            user, user_len, role, role_len) != 0) {
        free(user); free(role);
        return (cetcd_rpc_bytes){NULL, 0};
    }
    int rc = cetcd_v3rpc_propose_or_apply(entry, elen);
    free(entry);
    free(user); free(role);
    if (rc < 0) return (cetcd_rpc_bytes){NULL, 0};
    return simple_ok_response();
}

/* --- Additional Auth RPC handlers --- */

/*
 * AuthStatus RPC.
 * AuthStatusResponse:
 *   field 2 (enabled) = bool, tag = 0x10
 */
cetcd_rpc_bytes auth_handle_status(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len) {
    (void)rpc; (void)req; (void)req_len;
    bool enabled = g_rpc_auth ? cetcd_auth_is_enabled(g_rpc_auth) : false;
    int64_t rev = g_rpc_store ? cetcd_mvcc_revision(g_rpc_store) : 0;
    uint8_t buf[64];
    size_t pos = 0;
    /* field 1 = header (ResponseHeader with revision) */
    uint8_t hdr_buf[16]; size_t hp = 0;
    hdr_buf[hp++] = 0x18; /* revision */
    uint64_t rv = (uint64_t)(rev > 0 ? rev : 1);
    do { uint8_t b = rv & 0x7F; rv >>= 7; if (rv) b |= 0x80; hdr_buf[hp++] = b; } while (rv);
    buf[pos++] = 0x0a;
    buf[pos++] = (uint8_t)hp;
    memcpy(buf + pos, hdr_buf, hp); pos += hp;
    buf[pos++] = 0x10; /* field 2 = enabled */
    buf[pos++] = enabled ? 0x01 : 0x00;
    uint8_t *out = (uint8_t *)malloc(pos);
    if (!out) return (cetcd_rpc_bytes){NULL, 0};
    memcpy(out, buf, pos);
    return (cetcd_rpc_bytes){out, pos};
}

/*
 * UserList RPC.
 * UserListResponse:
 *   field 2 (users) = repeated string, tag = 0x12
 */
struct user_list_ctx {
    uint8_t *buf;
    size_t   cap;
    size_t   pos;
};

static bool collect_user_name(const char *name, void *udata) {
    struct user_list_ctx *ctx = (struct user_list_ctx *)udata;
    size_t nlen = strlen(name);
    if (ctx->pos + 2 + nlen >= ctx->cap) return false;
    ctx->buf[ctx->pos++] = 0x12; /* field 2 = users (string) */
    /* write length as varint */
    uint64_t l = nlen;
    while (l >= 0x80) {
        ctx->buf[ctx->pos++] = (uint8_t)(l | 0x80);
        l >>= 7;
    }
    ctx->buf[ctx->pos++] = (uint8_t)l;
    memcpy(ctx->buf + ctx->pos, name, nlen);
    ctx->pos += nlen;
    return true;
}

cetcd_rpc_bytes auth_handle_user_list(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len) {
    (void)rpc; (void)req; (void)req_len;
    uint8_t buf[1024];
    size_t start = write_header_prefix(buf, sizeof(buf), 0);
    struct user_list_ctx ctx = { buf, sizeof(buf), start };
    if (g_rpc_auth) {
        cetcd_auth_user_iter(g_rpc_auth, collect_user_name, &ctx);
    }
    if (ctx.pos == start) {
        /* No users collected — return just the header */
        uint8_t *out = (uint8_t *)malloc(start);
        if (!out) return (cetcd_rpc_bytes){NULL, 0};
        memcpy(out, buf, start);
        return (cetcd_rpc_bytes){out, start};
    }
    uint8_t *out = (uint8_t *)malloc(ctx.pos);
    if (!out) return (cetcd_rpc_bytes){NULL, 0};
    memcpy(out, buf, ctx.pos);
    return (cetcd_rpc_bytes){out, ctx.pos};
}

/*
 * UserChangePassword RPC.
 * UserChangePasswordRequest:
 *   field 1 (name)     = string, tag = 0x0a
 *   field 2 (password) = string, tag = 0x12
 */
cetcd_rpc_bytes auth_handle_user_change_password(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len) {
    (void)rpc;
    uint8_t *name = NULL; size_t name_len = 0;
    uint8_t *pass = NULL; size_t pass_len = 0;
    if (parse_auth_name_pass_request_(req, req_len, &name, &name_len,
                                      &pass, &pass_len, NULL) != 0)
        return (cetcd_rpc_bytes){NULL, 0};
    if (!g_rpc_auth || !name || name_len == 0 || !pass ||
        !cetcd_auth_has_user(g_rpc_auth, (const char *)name)) {
        free(name); free(pass);
        return (cetcd_rpc_bytes){NULL, 0};
    }
    uint8_t hash[64];
    size_t hlen = 0;
    if (cetcd_auth_hash_password(g_rpc_auth, (const char *)pass,
                                 hash, sizeof(hash), &hlen) != CETCD_OK) {
        free(name); free(pass);
        return (cetcd_rpc_bytes){NULL, 0};
    }
    uint8_t *entry = NULL;
    size_t elen = 0;
    if (cetcd_apply_encode_auth_user_change_pass(&entry, &elen,
            name, name_len, hash, hlen) != 0) {
        free(name); free(pass);
        return (cetcd_rpc_bytes){NULL, 0};
    }
    int rc = cetcd_v3rpc_propose_or_apply(entry, elen);
    free(entry);
    free(name); free(pass);
    if (rc < 0) return (cetcd_rpc_bytes){NULL, 0};
    return simple_ok_response();
}

/*
 * RoleList RPC.
 * RoleListResponse:
 *   field 2 (roles) = repeated string, tag = 0x12
 */
cetcd_rpc_bytes auth_handle_role_list(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len) {
    (void)rpc; (void)req; (void)req_len;
    uint8_t buf[1024];
    size_t start = write_header_prefix(buf, sizeof(buf), 0);
    /* Use the same collector pattern as user_list */
    struct user_list_ctx ctx = { buf, sizeof(buf), start };
    if (g_rpc_auth) {
        cetcd_auth_role_iter(g_rpc_auth, collect_user_name, &ctx);
    }
    if (ctx.pos == start) {
        /* No roles collected — return just the header */
        uint8_t *out = (uint8_t *)malloc(start);
        if (!out) return (cetcd_rpc_bytes){NULL, 0};
        memcpy(out, buf, start);
        return (cetcd_rpc_bytes){out, start};
    }
    uint8_t *out = (uint8_t *)malloc(ctx.pos);
    if (!out) return (cetcd_rpc_bytes){NULL, 0};
    memcpy(out, buf, ctx.pos);
    return (cetcd_rpc_bytes){out, ctx.pos};
}

/*
 * RoleDelete RPC.
 * AuthRoleDeleteRequest:
 *   field 1 (role) = string, tag = 0x0a
 */
cetcd_rpc_bytes auth_handle_role_delete(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len) {
    (void)rpc;
    uint8_t *name = NULL; size_t name_len = 0;
    if (parse_auth_name_request_(req, req_len, &name, &name_len) != 0)
        return (cetcd_rpc_bytes){NULL, 0};
    if (!g_rpc_auth || !name || name_len == 0 ||
        !cetcd_auth_get_role(g_rpc_auth, (const char *)name)) {
        free(name);
        return (cetcd_rpc_bytes){NULL, 0};
    }
    uint8_t *entry = NULL;
    size_t elen = 0;
    if (cetcd_apply_encode_auth_role_delete(&entry, &elen, name, name_len) != 0) {
        free(name);
        return (cetcd_rpc_bytes){NULL, 0};
    }
    int rc = cetcd_v3rpc_propose_or_apply(entry, elen);
    free(entry);
    free(name);
    if (rc < 0) return (cetcd_rpc_bytes){NULL, 0};
    return simple_ok_response();
}

/*
 * UserRevokeRole RPC.
 * AuthUserRevokeRoleRequest:
 *   field 1 (name) = string, tag = 0x0a
 *   field 2 (role) = string, tag = 0x12
 */
cetcd_rpc_bytes auth_handle_user_revoke_role(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len) {
    (void)rpc;
    uint8_t *user = NULL; size_t user_len = 0;
    uint8_t *role = NULL; size_t role_len = 0;
    if (parse_auth_name_pass_request_(req, req_len, &user, &user_len,
                                      &role, &role_len, NULL) != 0)
        return (cetcd_rpc_bytes){NULL, 0};
    if (!g_rpc_auth || !user || user_len == 0 || !role || role_len == 0) {
        free(user); free(role);
        return (cetcd_rpc_bytes){NULL, 0};
    }
    const cetcd_user *u = cetcd_auth_get_user(g_rpc_auth, (const char *)user);
    int has = 0;
    if (u && u->roles && u->n_roles > 0) {
        const char *p = u->roles;
        for (size_t i = 0; i < u->n_roles; i++) {
            if (strcmp(p, (const char *)role) == 0) { has = 1; break; }
            p += strlen(p) + 1;
        }
    }
    if (!has) {
        free(user); free(role);
        return (cetcd_rpc_bytes){NULL, 0};
    }
    uint8_t *entry = NULL;
    size_t elen = 0;
    if (cetcd_apply_encode_auth_user_revoke_role(&entry, &elen,
            user, user_len, role, role_len) != 0) {
        free(user); free(role);
        return (cetcd_rpc_bytes){NULL, 0};
    }
    int rc = cetcd_v3rpc_propose_or_apply(entry, elen);
    free(entry);
    free(user); free(role);
    if (rc < 0) return (cetcd_rpc_bytes){NULL, 0};
    return simple_ok_response();
}

/*
 * UserGet RPC.
 * AuthUserGetRequest:
 *   field 1 (name) = string, tag = 0x0a
 * AuthUserGetResponse:
 *   field 2 (roles) = repeated string, tag = 0x12
 */
cetcd_rpc_bytes auth_handle_user_get(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len) {
    (void)rpc;
    uint8_t *name = NULL; size_t name_len = 0;
    if (parse_auth_name_request_(req, req_len, &name, &name_len) != 0)
        return (cetcd_rpc_bytes){NULL, 0};
    if (!g_rpc_auth || !name) { free(name); return (cetcd_rpc_bytes){NULL, 0}; }

    const cetcd_user *u = cetcd_auth_get_user(g_rpc_auth, (const char *)name);
    free(name);
    if (!u) return (cetcd_rpc_bytes){NULL, 0};

    /* Encode roles: field 2 = repeated string, tag = 0x12 */
    uint8_t buf[1024];
    size_t header_end = write_header_prefix(buf, sizeof(buf), 0);
    size_t bpos = header_end;
    if (u->roles && u->n_roles > 0) {
        const char *p = u->roles;
        for (size_t i = 0; i < u->n_roles && bpos < sizeof(buf) - 128; i++) {
            size_t rlen = strlen(p);
            buf[bpos++] = 0x12; /* field 2 = roles (string) */
            uint64_t l = rlen;
            while (l >= 0x80) { buf[bpos++] = (uint8_t)(l | 0x80); l >>= 7; }
            buf[bpos++] = (uint8_t)l;
            if (bpos + rlen < sizeof(buf)) {
                memcpy(buf + bpos, p, rlen);
                bpos += rlen;
            }
            p += rlen + 1;
        }
    }
    /* Return response with header (even if no roles) */
    uint8_t *out = (uint8_t *)malloc(bpos);
    if (!out) return (cetcd_rpc_bytes){NULL, 0};
    memcpy(out, buf, bpos);
    return (cetcd_rpc_bytes){out, bpos};
}

/*
 * RoleGet RPC.
 * AuthRoleGetRequest:
 *   field 1 (role) = string, tag = 0x0a
 * AuthRoleGetResponse:
 *   field 2 (perm) = Permission, tag = 0x12
 *     Permission:
 *       field 1 (permType) = int (0=READ, 1=WRITE, 2=READWRITE), tag = 0x08
 *       field 2 (key)      = bytes, tag = 0x12
 *       field 3 (range_end) = bytes, tag = 0x1a
 *       leftover-safe: leftover cannot steal a printed range_end;
 *       truncated range_end fail-closes
 */
cetcd_rpc_bytes auth_handle_role_get(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len) {
    (void)rpc;
    uint8_t *name = NULL; size_t name_len = 0;
    if (parse_auth_name_request_(req, req_len, &name, &name_len) != 0)
        return (cetcd_rpc_bytes){NULL, 0};
    if (!g_rpc_auth || !name) { free(name); return (cetcd_rpc_bytes){NULL, 0}; }

    const cetcd_role *r = cetcd_auth_get_role(g_rpc_auth, (const char *)name);
    free(name);
    if (!r) return (cetcd_rpc_bytes){NULL, 0};

    /* Build Permission message */
    uint8_t perm_buf[512];
    size_t ppos = 0;
    /* field 1 = permType */
    int perm_type = 0;
    if (r->perm_read && r->perm_write) perm_type = 2;
    else if (r->perm_write) perm_type = 1;
    else if (r->perm_read) perm_type = 0;
    perm_buf[ppos++] = 0x08; /* tag */
    perm_buf[ppos++] = (uint8_t)perm_type;

    /* field 2 = key */
    if (r->key_prefix_len > 0) {
        perm_buf[ppos++] = 0x12; /* field 2 = key */
        uint64_t l = r->key_prefix_len;
        while (l >= 0x80) { perm_buf[ppos++] = (uint8_t)(l | 0x80); l >>= 7; }
        perm_buf[ppos++] = (uint8_t)l;
        memcpy(perm_buf + ppos, r->key_prefix, r->key_prefix_len);
        ppos += r->key_prefix_len;
    }
    /* field 3 = range_end (omitted = leftover prefix-only) */
    if (r->range_end_len > 0 && ppos + 6 + r->range_end_len < sizeof(perm_buf)) {
        perm_buf[ppos++] = 0x1a;
        uint64_t l = r->range_end_len;
        while (l >= 0x80) { perm_buf[ppos++] = (uint8_t)(l | 0x80); l >>= 7; }
        perm_buf[ppos++] = (uint8_t)l;
        memcpy(perm_buf + ppos, r->range_end, r->range_end_len);
        ppos += r->range_end_len;
    }

    /* Wrap in field 2 (perm) of RoleGetResponse, with header prefix */
    uint8_t *out = (uint8_t *)malloc(ppos + 16);
    if (!out) return (cetcd_rpc_bytes){NULL, 0};
    size_t opos = write_header_prefix(out, ppos + 16, 0);
    out[opos++] = 0x12; /* field 2 = perm */
    uint64_t l = ppos;
    while (l >= 0x80) { out[opos++] = (uint8_t)(l | 0x80); l >>= 7; }
    out[opos++] = (uint8_t)l;
    memcpy(out + opos, perm_buf, ppos);
    opos += ppos;
    return (cetcd_rpc_bytes){out, opos};
}

/* leftover-safe skip / copy. v3rpc cannot link server. */
static int leftover_safe_ldelim_(const uint8_t *buf, size_t len, size_t *pos,
                                 const uint8_t **payload, size_t *payload_len) {
    uint64_t n = 0;
    int shift = 0;
    int got = 0;
    if (!buf || !pos || !payload || !payload_len) return -1;
    while (*pos < len) {
        uint8_t b = buf[(*pos)++];
        n |= (uint64_t)(b & 0x7F) << shift;
        if ((b & 0x80) == 0) {
            got = 1;
            break;
        }
        shift += 7;
        if (shift > 63) return -1;
    }
    if (!got || *pos + n > len) return -1;
    *payload = buf + *pos;
    *payload_len = (size_t)n;
    *pos += (size_t)n;
    return 0;
}

static int leftover_safe_varint_(const uint8_t *buf, size_t len, size_t *pos,
                                 uint64_t *out) {
    uint64_t v = 0;
    int shift = 0;
    int got = 0;
    if (!buf || !pos) return -1;
    while (*pos < len) {
        uint8_t b = buf[(*pos)++];
        v |= (uint64_t)(b & 0x7F) << shift;
        if ((b & 0x80) == 0) {
            got = 1;
            break;
        }
        shift += 7;
        if (shift > 63) return -1;
    }
    if (!got) return -1;
    if (out) *out = v;
    return 0;
}

static int leftover_safe_copy_bytes_(const uint8_t *buf, size_t len, size_t *pos,
                                     uint8_t **out, size_t *out_len) {
    const uint8_t *pl = NULL;
    size_t n = 0;
    if (leftover_safe_ldelim_(buf, len, pos, &pl, &n) != 0) return -1;
    free(*out);
    if (n == 0) {
        *out = NULL;
        *out_len = 0;
        return 0;
    }
    uint8_t *copy = (uint8_t *)malloc(n + 1);
    if (!copy) {
        *out = NULL;
        *out_len = 0;
        return -1;
    }
    memcpy(copy, pl, n);
    copy[n] = 0;
    *out = copy;
    *out_len = n;
    return 0;
}

static int leftover_safe_skip_unknown_(const uint8_t *buf, size_t len,
                                       size_t *pos, uint8_t tag) {
    if ((tag & 7) == 0)
        return leftover_safe_varint_(buf, len, pos, NULL);
    if ((tag & 7) == 2) {
        const uint8_t *pl = NULL;
        size_t n = 0;
        return leftover_safe_ldelim_(buf, len, pos, &pl, &n);
    }
    return -1;
}

static void auth_role_perm_clear_(uint8_t **name, uint8_t **key,
                                  uint8_t **range_end) {
    free(*name);
    free(*key);
    free(*range_end);
    *name = NULL;
    *key = NULL;
    *range_end = NULL;
}

static int parse_auth_role_grant_perm_(const uint8_t *req, size_t len,
                                       uint8_t **name, size_t *name_len,
                                       int *perm_type,
                                       uint8_t **key, size_t *key_len,
                                       uint8_t **range_end,
                                       size_t *range_end_len) {
    size_t p = 0;
    if (!name || !name_len || !perm_type || !key || !key_len ||
        !range_end || !range_end_len) return -1;
    *name = NULL; *name_len = 0;
    *key = NULL; *key_len = 0;
    *range_end = NULL; *range_end_len = 0;
    *perm_type = 0;
    if (!req || len == 0) return 0;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x0a) {
            if (leftover_safe_copy_bytes_(req, len, &p, name, name_len) != 0) {
                auth_role_perm_clear_(name, key, range_end);
                return -1;
            }
            continue;
        }
        if (tag == 0x12) {
            const uint8_t *pl = NULL;
            size_t n = 0;
            size_t ip = 0;
            if (leftover_safe_ldelim_(req, len, &p, &pl, &n) != 0) {
                auth_role_perm_clear_(name, key, range_end);
                return -1;
            }
            while (ip < n) {
                uint8_t ptag = pl[ip++];
                if (ptag == 0x00)
                    continue;
                if (ptag == 0x08) {
                    uint64_t v = 0;
                    if (leftover_safe_varint_(pl, n, &ip, &v) != 0) {
                        auth_role_perm_clear_(name, key, range_end);
                        return -1;
                    }
                    *perm_type = (int)v;
                    continue;
                }
                if (ptag == 0x12) {
                    if (leftover_safe_copy_bytes_(pl, n, &ip, key, key_len) != 0) {
                        auth_role_perm_clear_(name, key, range_end);
                        return -1;
                    }
                    continue;
                }
                if (ptag == 0x1a) {
                    if (leftover_safe_copy_bytes_(pl, n, &ip, range_end,
                                                  range_end_len) != 0) {
                        auth_role_perm_clear_(name, key, range_end);
                        return -1;
                    }
                    continue;
                }
                if (leftover_safe_skip_unknown_(pl, n, &ip, ptag) != 0) {
                    auth_role_perm_clear_(name, key, range_end);
                    return -1;
                }
            }
            continue;
        }
        if (leftover_safe_skip_unknown_(req, len, &p, tag) != 0) {
            auth_role_perm_clear_(name, key, range_end);
            return -1;
        }
    }
    return 0;
}

static int parse_auth_role_revoke_perm_(const uint8_t *req, size_t len,
                                        uint8_t **name, size_t *name_len,
                                        uint8_t **key, size_t *key_len,
                                        uint8_t **range_end,
                                        size_t *range_end_len) {
    size_t p = 0;
    if (!name || !name_len || !key || !key_len ||
        !range_end || !range_end_len) return -1;
    *name = NULL; *name_len = 0;
    *key = NULL; *key_len = 0;
    *range_end = NULL; *range_end_len = 0;
    if (!req || len == 0) return 0;
    while (p < len) {
        uint8_t tag = req[p++];
        if (tag == 0x00)
            continue;
        if (tag == 0x0a) {
            if (leftover_safe_copy_bytes_(req, len, &p, name, name_len) != 0) {
                auth_role_perm_clear_(name, key, range_end);
                return -1;
            }
            continue;
        }
        if (tag == 0x12) {
            if (leftover_safe_copy_bytes_(req, len, &p, key, key_len) != 0) {
                auth_role_perm_clear_(name, key, range_end);
                return -1;
            }
            continue;
        }
        if (tag == 0x1a) {
            if (leftover_safe_copy_bytes_(req, len, &p, range_end,
                                          range_end_len) != 0) {
                auth_role_perm_clear_(name, key, range_end);
                return -1;
            }
            continue;
        }
        if (leftover_safe_skip_unknown_(req, len, &p, tag) != 0) {
            auth_role_perm_clear_(name, key, range_end);
            return -1;
        }
    }
    return 0;
}

/*
 * RoleGrantPermission RPC.
 * AuthRoleGrantPermissionRequest:
 *   field 1 (name) = string, tag = 0x0a
 *   field 2 (perm) = Permission, tag = 0x12
 *     Permission:
 *       field 1 (permType) = int, tag = 0x08
 *       field 2 (key)      = bytes, tag = 0x12
 *       field 3 (range_end) = bytes, tag = 0x1a
 * Response: empty (header only)
 */
cetcd_rpc_bytes auth_handle_role_grant_permission(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len) {
    (void)rpc;
    uint8_t *role_name = NULL; size_t role_name_len = 0;
    uint8_t *perm_key = NULL; size_t perm_key_len = 0;
    uint8_t *range_end = NULL; size_t range_end_len = 0;
    int perm_type = 0;

    if (parse_auth_role_grant_perm_(req, req_len, &role_name, &role_name_len,
                                    &perm_type, &perm_key, &perm_key_len,
                                    &range_end, &range_end_len) != 0)
        return (cetcd_rpc_bytes){NULL, 0};

    if (!g_rpc_auth || !role_name || role_name_len == 0 ||
        !cetcd_auth_get_role(g_rpc_auth, (const char *)role_name)) {
        free(role_name); free(perm_key); free(range_end);
        return (cetcd_rpc_bytes){NULL, 0};
    }
    uint8_t *entry = NULL;
    size_t elen = 0;
    if (cetcd_apply_encode_auth_role_grant_perm_range(&entry, &elen,
            role_name, role_name_len,
            perm_key, perm_key_len, perm_type,
            range_end, range_end_len) != 0) {
        free(role_name); free(perm_key); free(range_end);
        return (cetcd_rpc_bytes){NULL, 0};
    }
    int rc = cetcd_v3rpc_propose_or_apply(entry, elen);
    free(entry);
    free(role_name); free(perm_key); free(range_end);
    if (rc < 0) return (cetcd_rpc_bytes){NULL, 0};
    return simple_ok_response();
}

/*
 * RoleRevokePermission RPC.
 * AuthRoleRevokePermissionRequest:
 *   field 1 (role) = string, tag = 0x0a
 *   field 2 (key)  = bytes, tag = 0x12
 *   field 3 (range_end) = bytes, tag = 0x1a
 * Response: empty (header only)
 */
cetcd_rpc_bytes auth_handle_role_revoke_permission(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len) {
    (void)rpc;
    uint8_t *role_name = NULL; size_t role_name_len = 0;
    uint8_t *perm_key = NULL; size_t perm_key_len = 0;
    uint8_t *range_end = NULL; size_t range_end_len = 0;
    if (parse_auth_role_revoke_perm_(req, req_len, &role_name, &role_name_len,
                                     &perm_key, &perm_key_len,
                                     &range_end, &range_end_len) != 0)
        return (cetcd_rpc_bytes){NULL, 0};
    if (!g_rpc_auth || !role_name || role_name_len == 0) {
        free(role_name); free(perm_key); free(range_end);
        return (cetcd_rpc_bytes){NULL, 0};
    }
    const cetcd_role *r = cetcd_auth_get_role(g_rpc_auth, (const char *)role_name);
    if (!r) {
        free(role_name); free(perm_key); free(range_end);
        return (cetcd_rpc_bytes){NULL, 0};
    }
    if (perm_key_len > 0 &&
        (r->key_prefix_len != perm_key_len ||
         memcmp(r->key_prefix, perm_key, perm_key_len) != 0)) {
        free(role_name); free(perm_key); free(range_end);
        return (cetcd_rpc_bytes){NULL, 0};
    }
    if (range_end_len > 0 &&
        (r->range_end_len != range_end_len ||
         memcmp(r->range_end, range_end, range_end_len) != 0)) {
        free(role_name); free(perm_key); free(range_end);
        return (cetcd_rpc_bytes){NULL, 0};
    }
    uint8_t *entry = NULL;
    size_t elen = 0;
    if (cetcd_apply_encode_auth_role_revoke_perm_range(&entry, &elen,
            role_name, role_name_len, perm_key, perm_key_len,
            range_end, range_end_len) != 0) {
        free(role_name); free(perm_key); free(range_end);
        return (cetcd_rpc_bytes){NULL, 0};
    }
    int rc = cetcd_v3rpc_propose_or_apply(entry, elen);
    free(entry);
    free(role_name); free(perm_key); free(range_end);
    if (rc < 0) return (cetcd_rpc_bytes){NULL, 0};
    return simple_ok_response();
}
