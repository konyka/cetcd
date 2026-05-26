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
