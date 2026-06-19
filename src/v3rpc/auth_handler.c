#include "cetcd/v3rpc.h"
#include "cetcd/auth.h"
#include <stdlib.h>
#include <string.h>

extern cetcd_auth_store *g_rpc_auth;

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

static int read_varint(const uint8_t *buf, size_t len, size_t *pos, uint64_t *out) {
    uint64_t val = 0;
    int shift = 0;
    while (*pos < len) {
        uint8_t b = buf[*pos];
        (*pos)++;
        val |= (uint64_t)(b & 0x7F) << shift;
        if ((b & 0x80) == 0) { *out = val; return 0; }
        shift += 7;
        if (shift > 63) break;
    }
    return -1;
}

static int read_bytes_field(const uint8_t *buf, size_t len, size_t *pos,
                            uint8_t **out, size_t *out_len) {
    uint64_t l = 0;
    if (read_varint(buf, len, pos, &l) != 0) return -1;
    if (*pos + l > len) return -1;
    uint8_t *p = (uint8_t *)malloc((size_t)l + 1);
    if (!p) return -1;
    memcpy(p, buf + *pos, (size_t)l);
    p[(size_t)l] = '\0';
    *pos += (size_t)l;
    *out = p;
    *out_len = (size_t)l;
    return 0;
}

static cetcd_rpc_bytes simple_ok_response(void) {
    uint8_t *b = (uint8_t *)malloc(1);
    b[0] = 0;
    return (cetcd_rpc_bytes){b, 1};
}

static cetcd_rpc_bytes make_response(const uint8_t *data, size_t len) {
    uint8_t *b = (uint8_t *)malloc(len);
    memcpy(b, data, len);
    return (cetcd_rpc_bytes){b, len};
}

cetcd_rpc_bytes auth_handle_enable(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len) {
    (void)rpc; (void)req; (void)req_len;
    if (g_rpc_auth) cetcd_auth_set_enabled(g_rpc_auth, true);
    return simple_ok_response();
}

cetcd_rpc_bytes auth_handle_disable(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len) {
    (void)rpc; (void)req; (void)req_len;
    if (g_rpc_auth) cetcd_auth_set_enabled(g_rpc_auth, false);
    return simple_ok_response();
}

cetcd_rpc_bytes auth_handle_authenticate(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len) {
    (void)rpc;
    uint8_t *name = NULL; size_t name_len = 0;
    uint8_t *pass = NULL; size_t pass_len = 0;
    size_t pos = 0;
    while (pos < req_len) {
        uint8_t tag = req[pos++];
        if (tag == 0x0a) {
            if (read_bytes_field(req, req_len, &pos, &name, &name_len) != 0) break;
        } else if (tag == 0x12) {
            if (read_bytes_field(req, req_len, &pos, &pass, &pass_len) != 0) break;
        } else {
            uint64_t skip = 0; read_varint(req, req_len, &pos, &skip);
        }
    }
    bool ok = false;
    if (g_rpc_auth && name && pass) {
        ok = cetcd_auth_check_password(g_rpc_auth, (const char *)name, (const char *)pass);
    }
    free(name); free(pass);
    if (!ok) return (cetcd_rpc_bytes){NULL, 0};
    /* protobuf: field 1 (bytes) tag=0x0a, len=5, value="token" */
    uint8_t resp[] = {0x0a, 0x05, 't','o','k','e','n'};
    return make_response(resp, sizeof(resp));
}

cetcd_rpc_bytes auth_handle_user_add(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len) {
    (void)rpc;
    uint8_t *name = NULL; size_t name_len = 0;
    uint8_t *pass = NULL; size_t pass_len = 0;
    size_t pos = 0;
    while (pos < req_len) {
        uint8_t tag = req[pos++];
        if (tag == 0x0a) {
            if (read_bytes_field(req, req_len, &pos, &name, &name_len) != 0) break;
        } else if (tag == 0x12) {
            if (read_bytes_field(req, req_len, &pos, &pass, &pass_len) != 0) break;
        } else {
            uint64_t skip = 0; read_varint(req, req_len, &pos, &skip);
        }
    }
    int rc = -1;
    if (g_rpc_auth && name && pass) {
        rc = cetcd_auth_add_user(g_rpc_auth, (const char *)name, (const char *)pass);
    }
    free(name); free(pass);
    if (rc != CETCD_OK) return (cetcd_rpc_bytes){NULL, 0};
    return simple_ok_response();
}

cetcd_rpc_bytes auth_handle_user_delete(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len) {
    (void)rpc;
    uint8_t *name = NULL; size_t name_len = 0;
    size_t pos = 0;
    while (pos < req_len) {
        uint8_t tag = req[pos++];
        if (tag == 0x0a) {
            if (read_bytes_field(req, req_len, &pos, &name, &name_len) != 0) break;
        } else {
            uint64_t skip = 0; read_varint(req, req_len, &pos, &skip);
        }
    }
    int rc = -1;
    if (g_rpc_auth && name) {
        rc = cetcd_auth_remove_user(g_rpc_auth, (const char *)name);
    }
    free(name);
    if (rc != CETCD_OK) return (cetcd_rpc_bytes){NULL, 0};
    return simple_ok_response();
}

cetcd_rpc_bytes auth_handle_role_add(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len) {
    (void)rpc;
    uint8_t *name = NULL; size_t name_len = 0;
    size_t pos = 0;
    while (pos < req_len) {
        uint8_t tag = req[pos++];
        if (tag == 0x0a) {
            if (read_bytes_field(req, req_len, &pos, &name, &name_len) != 0) break;
        } else {
            uint64_t skip = 0; read_varint(req, req_len, &pos, &skip);
        }
    }
    int rc = -1;
    if (g_rpc_auth && name) {
        rc = cetcd_auth_add_role(g_rpc_auth, (const char *)name, 1, 1, "/", 1);
    }
    free(name);
    if (rc != CETCD_OK) return (cetcd_rpc_bytes){NULL, 0};
    return simple_ok_response();
}

cetcd_rpc_bytes auth_handle_role_grant(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len) {
    (void)rpc;
    uint8_t *user = NULL; size_t user_len = 0;
    uint8_t *role = NULL; size_t role_len = 0;
    size_t pos = 0;
    while (pos < req_len) {
        uint8_t tag = req[pos++];
        if (tag == 0x0a) {
            if (read_bytes_field(req, req_len, &pos, &user, &user_len) != 0) break;
        } else if (tag == 0x12) {
            if (read_bytes_field(req, req_len, &pos, &role, &role_len) != 0) break;
        } else {
            uint64_t skip = 0; read_varint(req, req_len, &pos, &skip);
        }
    }
    int rc = -1;
    if (g_rpc_auth && user && role) {
        rc = cetcd_auth_grant_role(g_rpc_auth, (const char *)user, (const char *)role);
    }
    free(user); free(role);
    if (rc != CETCD_OK) return (cetcd_rpc_bytes){NULL, 0};
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
    uint8_t buf[4];
    size_t pos = 0;
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
    struct user_list_ctx ctx = { buf, sizeof(buf), 0 };
    if (g_rpc_auth) {
        cetcd_auth_user_iter(g_rpc_auth, collect_user_name, &ctx);
    }
    if (ctx.pos == 0) {
        return simple_ok_response();
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
    size_t pos = 0;
    while (pos < req_len) {
        uint8_t tag = req[pos++];
        if (tag == 0x0a) {
            if (read_bytes_field(req, req_len, &pos, &name, &name_len) != 0) break;
        } else if (tag == 0x12) {
            if (read_bytes_field(req, req_len, &pos, &pass, &pass_len) != 0) break;
        } else {
            uint64_t skip = 0; read_varint(req, req_len, &pos, &skip);
        }
    }
    int rc = -1;
    if (g_rpc_auth && name && pass) {
        rc = cetcd_auth_change_password(g_rpc_auth, (const char *)name, (const char *)pass);
    }
    free(name); free(pass);
    if (rc != CETCD_OK) return (cetcd_rpc_bytes){NULL, 0};
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
    size_t pos = 0;
    /* Use the same collector pattern as user_list */
    struct user_list_ctx ctx = { buf, sizeof(buf), 0 };
    if (g_rpc_auth) {
        cetcd_auth_role_iter(g_rpc_auth, collect_user_name, &ctx);
    }
    if (ctx.pos == 0) {
        return simple_ok_response();
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
    size_t pos = 0;
    while (pos < req_len) {
        uint8_t tag = req[pos++];
        if (tag == 0x0a) {
            if (read_bytes_field(req, req_len, &pos, &name, &name_len) != 0) break;
        } else {
            uint64_t skip = 0; read_varint(req, req_len, &pos, &skip);
        }
    }
    int rc = -1;
    if (g_rpc_auth && name) {
        rc = cetcd_auth_remove_role(g_rpc_auth, (const char *)name);
    }
    free(name);
    if (rc != CETCD_OK) return (cetcd_rpc_bytes){NULL, 0};
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
    size_t pos = 0;
    while (pos < req_len) {
        uint8_t tag = req[pos++];
        if (tag == 0x0a) {
            if (read_bytes_field(req, req_len, &pos, &user, &user_len) != 0) break;
        } else if (tag == 0x12) {
            if (read_bytes_field(req, req_len, &pos, &role, &role_len) != 0) break;
        } else {
            uint64_t skip = 0; read_varint(req, req_len, &pos, &skip);
        }
    }
    int rc = -1;
    if (g_rpc_auth && user && role) {
        rc = cetcd_auth_revoke_role(g_rpc_auth, (const char *)user, (const char *)role);
    }
    free(user); free(role);
    if (rc != CETCD_OK) return (cetcd_rpc_bytes){NULL, 0};
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
    size_t pos = 0;
    while (pos < req_len) {
        uint8_t tag = req[pos++];
        if (tag == 0x0a) {
            if (read_bytes_field(req, req_len, &pos, &name, &name_len) != 0) break;
        } else {
            uint64_t skip = 0; read_varint(req, req_len, &pos, &skip);
        }
    }
    if (!g_rpc_auth || !name) { free(name); return (cetcd_rpc_bytes){NULL, 0}; }

    const cetcd_user *u = cetcd_auth_get_user(g_rpc_auth, (const char *)name);
    free(name);
    if (!u) return (cetcd_rpc_bytes){NULL, 0};

    /* Encode roles: field 2 = repeated string, tag = 0x12 */
    uint8_t buf[1024];
    size_t bpos = 0;
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
    if (bpos == 0) return simple_ok_response();
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
 *       field 2 (key)      = bytes, tag = 0x0a
 *       field 3 (range_end) = bytes, tag = 0x12
 */
cetcd_rpc_bytes auth_handle_role_get(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len) {
    (void)rpc;
    uint8_t *name = NULL; size_t name_len = 0;
    size_t pos = 0;
    while (pos < req_len) {
        uint8_t tag = req[pos++];
        if (tag == 0x0a) {
            if (read_bytes_field(req, req_len, &pos, &name, &name_len) != 0) break;
        } else {
            uint64_t skip = 0; read_varint(req, req_len, &pos, &skip);
        }
    }
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
        perm_buf[ppos++] = 0x0a; /* tag */
        uint64_t l = r->key_prefix_len;
        while (l >= 0x80) { perm_buf[ppos++] = (uint8_t)(l | 0x80); l >>= 7; }
        perm_buf[ppos++] = (uint8_t)l;
        memcpy(perm_buf + ppos, r->key_prefix, r->key_prefix_len);
        ppos += r->key_prefix_len;
    }

    /* Wrap in field 2 (perm) of RoleGetResponse */
    uint8_t *out = (uint8_t *)malloc(ppos + 4);
    if (!out) return (cetcd_rpc_bytes){NULL, 0};
    size_t opos = 0;
    out[opos++] = 0x12; /* field 2 = perm */
    uint64_t l = ppos;
    while (l >= 0x80) { out[opos++] = (uint8_t)(l | 0x80); l >>= 7; }
    out[opos++] = (uint8_t)l;
    memcpy(out + opos, perm_buf, ppos);
    opos += ppos;
    return (cetcd_rpc_bytes){out, opos};
}

/*
 * RoleGrantPermission RPC.
 * AuthRoleGrantPermissionRequest:
 *   field 1 (name) = string, tag = 0x0a
 *   field 2 (perm) = Permission, tag = 0x12
 *     Permission:
 *       field 1 (permType) = int, tag = 0x08
 *       field 2 (key)      = bytes, tag = 0x0a
 *       field 3 (range_end) = bytes, tag = 0x12
 * Response: empty (header only)
 */
cetcd_rpc_bytes auth_handle_role_grant_permission(cetcd_v3rpc *rpc, const uint8_t *req, size_t req_len) {
    (void)rpc;
    uint8_t *role_name = NULL; size_t role_name_len = 0;
    uint8_t *perm_key = NULL; size_t perm_key_len = 0;
    int perm_type = 0;
    size_t pos = 0;

    while (pos < req_len) {
        uint8_t tag = req[pos++];
        if (tag == 0x0a) {
            if (read_bytes_field(req, req_len, &pos, &role_name, &role_name_len) != 0) break;
        } else if (tag == 0x12) {
            /* field 2 = Permission (length-delimited) */
            uint64_t plen = 0;
            if (read_varint(req, req_len, &pos, &plen) != 0) break;
            size_t perm_end = pos + (size_t)plen;
            while (pos < perm_end) {
                uint8_t ptag = req[pos++];
                if (ptag == 0x08) {
                    uint64_t v = 0; read_varint(req, perm_end, &pos, &v);
                    perm_type = (int)v;
                } else if (ptag == 0x0a) {
                    if (read_bytes_field(req, perm_end, &pos, &perm_key, &perm_key_len) != 0) break;
                } else if (ptag == 0x12) {
                    uint64_t l = 0; read_varint(req, perm_end, &pos, &l);
                    pos += (size_t)l;
                } else {
                    uint64_t skip = 0; read_varint(req, perm_end, &pos, &skip);
                }
            }
            pos = perm_end;
        } else {
            uint64_t skip = 0; read_varint(req, req_len, &pos, &skip);
        }
    }

    int rc = -1;
    if (g_rpc_auth && role_name) {
        int rd = (perm_type == 0 || perm_type == 2) ? 1 : 0;
        int wr = (perm_type == 1 || perm_type == 2) ? 1 : 0;
        rc = cetcd_auth_grant_permission(g_rpc_auth, (const char *)role_name,
                                          rd, wr,
                                          perm_key ? (const char *)perm_key : NULL,
                                          perm_key_len);
    }
    free(role_name); free(perm_key);
    if (rc != CETCD_OK) return (cetcd_rpc_bytes){NULL, 0};
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
    size_t pos = 0;
    while (pos < req_len) {
        uint8_t tag = req[pos++];
        if (tag == 0x0a) {
            if (read_bytes_field(req, req_len, &pos, &role_name, &role_name_len) != 0) break;
        } else if (tag == 0x12 || tag == 0x1a) {
            uint64_t l = 0; read_varint(req, req_len, &pos, &l);
            pos += (size_t)l;
        } else {
            uint64_t skip = 0; read_varint(req, req_len, &pos, &skip);
        }
    }
    int rc = -1;
    if (g_rpc_auth && role_name) {
        rc = cetcd_auth_revoke_permission(g_rpc_auth, (const char *)role_name);
    }
    free(role_name);
    if (rc != CETCD_OK) return (cetcd_rpc_bytes){NULL, 0};
    return simple_ok_response();
}
