/*
 * cetcdctl — command-line client for cetcd
 *
 * Connects to a cetcd server using the custom gRPC-like framing protocol:
 *   Request:  2B path_len (BE) + path + 1B compressed + 4B payload_len (BE) + payload
 *   Response: same format
 *
 * Supported commands:
 *   put KEY VALUE          — store a key-value pair
 *   get KEY                — retrieve a key
 *   del KEY                — delete a key
 *   lease grant TTL        — grant a lease with given TTL (seconds)
 *   lease revoke ID        — revoke a lease by ID
 *   lease timetolive ID    — query remaining TTL of a lease
 *   lease list             — list all active leases
 *   lease keepalive ID     — keep a lease alive
 *   txn put KEY VALUE      — execute a transaction (Put)
 *   compact REV            — compact MVCC history to given revision
 *   status                 — get server status
 *   alarm                  — query alarms
 *   hash                   — get KV store hash
 *   hashkv                 — get KV store hash + compact revision
 *   defrag                 — defragment the database (no-op for LMDB)
 *   move-leader TARGET_ID  — transfer leadership to target node
 *   member list            — list cluster members
 *   member add PEER_URL    — add a cluster member
 *   member remove ID       — remove a cluster member
 *   member update ID URL   — update a member's peer URL
 *   snapshot save [FILE]   — save a snapshot to file
 *   downgrade enable VER   — enable cluster downgrade
 *   downgrade cancel       — cancel cluster downgrade
 *   downgrade validate VER — validate downgrade version
 *   auth enable            — enable authentication
 *   auth disable           — disable authentication
 *   auth status            — query auth status
 *   user add NAME PASS     — add a user
 *   user delete NAME       — delete a user
 *   user get NAME          — get user details (roles)
 *   user list              — list all users
 *   user change-password NAME PASS — change user password
 *   user grant-role NAME ROLE — grant role to user
 *   user revoke-role NAME ROLE — revoke role from user
 *   role add NAME          — add a role
 *   role delete NAME       — delete a role
 *   role get NAME          — get role details (permissions)
 *   role list              — list all roles
 *   role grant-permission ROLE TYPE KEY — grant permission
 *   role revoke-permission ROLE — revoke all permissions from role
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

static const char *g_host = "127.0.0.1";
static uint16_t    g_port = 2379;

/* --- Protobuf helpers --- */

static size_t write_varint(uint8_t *buf, size_t cap, size_t pos, uint64_t val) {
    while (pos < cap) {
        uint8_t b = val & 0x7F;
        val >>= 7;
        if (val) b |= 0x80;
        buf[pos++] = b;
        if (!val) break;
    }
    return pos;
}

static int read_varint(const uint8_t *buf, size_t len, size_t *pos, uint64_t *out) {
    uint64_t val = 0; int shift = 0;
    while (*pos < len) {
        uint8_t b = buf[(*pos)++];
        val |= (uint64_t)(b & 0x7F) << shift;
        if (!(b & 0x80)) { *out = val; return 0; }
        shift += 7;
        if (shift > 63) break;
    }
    return -1;
}

static size_t encode_bytes_field(uint8_t *buf, size_t cap, size_t pos,
                                  uint8_t tag, const uint8_t *data, size_t data_len) {
    if (pos + 1 >= cap) return pos;
    buf[pos++] = tag;
    pos = write_varint(buf, cap, pos, data_len);
    if (pos + data_len >= cap) return pos;
    memcpy(buf + pos, data, data_len);
    return pos + data_len;
}

static size_t encode_string_field(uint8_t *buf, size_t cap, size_t pos,
                                   uint8_t tag, const char *str) {
    return encode_bytes_field(buf, cap, pos, tag, (const uint8_t *)str, strlen(str));
}

static size_t encode_varint_field(uint8_t *buf, size_t cap, size_t pos,
                                   uint8_t tag, uint64_t val) {
    if (pos + 1 >= cap) return pos;
    buf[pos++] = tag;
    return write_varint(buf, cap, pos, val);
}

/* --- Wire protocol --- */

static int connect_server(void) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { perror("socket"); return -1; }
    struct sockaddr_in sa;
    memset(&sa, 0, sizeof(sa));
    sa.sin_family = AF_INET;
    sa.sin_port = htons(g_port);
    inet_pton(AF_INET, g_host, &sa.sin_addr);
    if (connect(fd, (struct sockaddr *)&sa, sizeof(sa)) != 0) {
        perror("connect");
        close(fd);
        return -1;
    }
    return fd;
}

static int send_request(int fd, const char *path,
                         const uint8_t *payload, size_t payload_len) {
    size_t path_len = strlen(path);
    uint8_t header[512];
    size_t hpos = 0;
    header[hpos++] = (uint8_t)(path_len >> 8);
    header[hpos++] = (uint8_t)(path_len & 0xFF);
    memcpy(header + hpos, path, path_len);
    hpos += path_len;
    header[hpos++] = 0; /* compressed = false */
    header[hpos++] = (uint8_t)((payload_len >> 24) & 0xFF);
    header[hpos++] = (uint8_t)((payload_len >> 16) & 0xFF);
    header[hpos++] = (uint8_t)((payload_len >> 8) & 0xFF);
    header[hpos++] = (uint8_t)(payload_len & 0xFF);

    if (send(fd, header, hpos, 0) != (ssize_t)hpos) return -1;
    if (payload_len > 0) {
        if (send(fd, payload, payload_len, 0) != (ssize_t)payload_len) return -1;
    }
    return 0;
}

static int recv_response(int fd, uint8_t *buf, size_t buf_cap) {
    /* Read header: 2B path_len + path + 1B compressed + 4B payload_len */
    uint8_t hdr[512];
    ssize_t n = recv(fd, hdr, 2, MSG_WAITALL);
    if (n != 2) return -1;
    uint16_t path_len = ((uint16_t)hdr[0] << 8) | hdr[1];
    if (path_len > 256) return -1;

    n = recv(fd, hdr, path_len + 5, MSG_WAITALL);
    if (n != path_len + 5) return -1;

    uint32_t payload_len = ((uint32_t)hdr[path_len + 1] << 24) |
                           ((uint32_t)hdr[path_len + 2] << 16) |
                           ((uint32_t)hdr[path_len + 3] << 8)  |
                           ((uint32_t)hdr[path_len + 4]);
    if (payload_len >= buf_cap) payload_len = buf_cap - 1;

    if (payload_len > 0) {
        n = recv(fd, buf, payload_len, MSG_WAITALL);
        if (n != (ssize_t)payload_len) return -1;
    }
    buf[payload_len] = '\0';
    return (int)payload_len;
}

static int do_rpc(const char *path, const uint8_t *req, size_t req_len,
                  uint8_t *resp, size_t resp_cap) {
    int fd = connect_server();
    if (fd < 0) return -1;
    if (send_request(fd, path, req, req_len) != 0) {
        close(fd);
        return -1;
    }
    int rlen = recv_response(fd, resp, resp_cap);
    close(fd);
    return rlen;
}

/* --- Response parsing helpers --- */

static void parse_range_response(const uint8_t *data, size_t len) {
    size_t pos = 0;
    int count = 0;
    while (pos < len) {
        uint8_t tag = data[pos++];
        if (tag == 0x0a) {
            /* KeyValue (length-delimited) */
            uint64_t kv_len = 0;
            if (read_varint(data, len, &pos, &kv_len) != 0) break;
            size_t kv_end = pos + (size_t)kv_len;
            /* Parse KeyValue fields */
            const uint8_t *key_data = NULL; size_t key_len = 0;
            const uint8_t *val_data = NULL; size_t val_len = 0;
            while (pos < kv_end) {
                uint8_t ktag = data[pos++];
                if (ktag == 0x0a) {
                    uint64_t l = 0; read_varint(data, kv_end, &pos, &l);
                    key_data = data + pos; key_len = (size_t)l;
                    pos += l;
                } else if (ktag == 0x12) {
                    uint64_t l = 0; read_varint(data, kv_end, &pos, &l);
                    val_data = data + pos; val_len = (size_t)l;
                    pos += l;
                } else if (ktag == 0x18 || ktag == 0x20 || ktag == 0x28) {
                    uint64_t v = 0; read_varint(data, kv_end, &pos, &v);
                } else {
                    uint64_t v = 0; read_varint(data, kv_end, &pos, &v);
                }
            }
            pos = kv_end;
            count++;
            printf("%.*s", (int)key_len, key_data);
            if (val_data && val_len > 0) {
                printf(" -> %.*s", (int)val_len, val_data);
            }
            printf("\n");
        } else if (tag == 0x10) {
            /* count (varint) */
            uint64_t v = 0; read_varint(data, len, &pos, &v);
        } else if (tag == 0x18) {
            /* more (bool) */
            uint64_t v = 0; read_varint(data, len, &pos, &v);
        } else {
            uint64_t v = 0; read_varint(data, len, &pos, &v);
        }
    }
    if (count == 0) printf("(empty)\n");
}

static void parse_status_response(const uint8_t *data, size_t len) {
    size_t pos = 0;
    while (pos < len) {
        uint8_t tag = data[pos++];
        if (tag == 0x12) {
            /* version (string) */
            uint64_t l = 0; read_varint(data, len, &pos, &l);
            printf("version: %.*s\n", (int)l, data + pos);
            pos += l;
        } else if (tag == 0x18) {
            /* dbSize (int64) */
            uint64_t v = 0; read_varint(data, len, &pos, &v);
            printf("dbSize: %llu\n", (unsigned long long)v);
        } else if (tag == 0x20) {
            /* leader (uint64) */
            uint64_t v = 0; read_varint(data, len, &pos, &v);
            printf("leader: %llu\n", (unsigned long long)v);
        } else if (tag == 0x28) {
            /* raftIndex (uint64) */
            uint64_t v = 0; read_varint(data, len, &pos, &v);
            printf("raftIndex: %llu\n", (unsigned long long)v);
        } else if (tag == 0x30) {
            /* raftTerm (uint64) */
            uint64_t v = 0; read_varint(data, len, &pos, &v);
            printf("raftTerm: %llu\n", (unsigned long long)v);
        } else {
            uint64_t v = 0; read_varint(data, len, &pos, &v);
        }
    }
}

static void parse_lease_grant_response(const uint8_t *data, size_t len) {
    size_t pos = 0;
    while (pos < len) {
        uint8_t tag = data[pos++];
        if (tag == 0x08) {
            uint64_t v = 0; read_varint(data, len, &pos, &v);
            printf("lease ID: %llu\n", (unsigned long long)v);
        } else if (tag == 0x10) {
            uint64_t v = 0; read_varint(data, len, &pos, &v);
            printf("TTL: %llu seconds\n", (unsigned long long)v);
        } else {
            uint64_t v = 0; read_varint(data, len, &pos, &v);
        }
    }
}

static void parse_lease_ttl_response(const uint8_t *data, size_t len) {
    size_t pos = 0;
    while (pos < len) {
        uint8_t tag = data[pos++];
        if (tag == 0x08) {
            uint64_t v = 0; read_varint(data, len, &pos, &v);
            printf("lease ID: %llu\n", (unsigned long long)v);
        } else if (tag == 0x10) {
            uint64_t v = 0; read_varint(data, len, &pos, &v);
            printf("remaining TTL: %lld\n", (long long)v);
        } else if (tag == 0x18) {
            uint64_t v = 0; read_varint(data, len, &pos, &v);
            printf("granted TTL: %lld\n", (long long)v);
        } else {
            uint64_t v = 0; read_varint(data, len, &pos, &v);
        }
    }
}

static void parse_member_list_response(const uint8_t *data, size_t len) {
    size_t pos = 0;
    while (pos < len) {
        uint8_t tag = data[pos++];
        if (tag == 0x12) {
            /* Member (length-delimited) */
            uint64_t mlen = 0; read_varint(data, len, &pos, &mlen);
            size_t mend = pos + (size_t)mlen;
            while (pos < mend) {
                uint8_t mtag = data[pos++];
                if (mtag == 0x08) {
                    uint64_t id = 0; read_varint(data, mend, &pos, &id);
                    printf("member ID: %llu", (unsigned long long)id);
                } else if (mtag == 0x12) {
                    uint64_t l = 0; read_varint(data, mend, &pos, &l);
                    printf(" peerURL: %.*s", (int)l, data + pos);
                    pos += l;
                } else {
                    uint64_t v = 0; read_varint(data, mend, &pos, &v);
                }
            }
            pos = mend;
            printf("\n");
        } else {
            uint64_t v = 0; read_varint(data, len, &pos, &v);
        }
    }
}

static void parse_auth_status_response(const uint8_t *data, size_t len) {
    size_t pos = 0;
    while (pos < len) {
        uint8_t tag = data[pos++];
        if (tag == 0x10) {
            uint64_t v = 0; read_varint(data, len, &pos, &v);
            printf("auth enabled: %s\n", v ? "true" : "false");
        } else {
            uint64_t v = 0; read_varint(data, len, &pos, &v);
        }
    }
}

static void parse_string_list_response(const uint8_t *data, size_t len, const char *label) {
    size_t pos = 0;
    int count = 0;
    while (pos < len) {
        uint8_t tag = data[pos++];
        if (tag == 0x12) {
            uint64_t l = 0; read_varint(data, len, &pos, &l);
            printf("%.*s\n", (int)l, data + pos);
            pos += l;
            count++;
        } else {
            uint64_t v = 0; read_varint(data, len, &pos, &v);
        }
    }
    if (count == 0) printf("(no %s)\n", label);
}

/* --- Commands --- */

static int cmd_put(int argc, char **argv) {
    if (argc < 4) { fprintf(stderr, "usage: cetcdctl put KEY VALUE\n"); return 1; }
    uint8_t req[4096], resp[4096];
    size_t pos = 0;
    pos = encode_bytes_field(req, sizeof(req), pos, 0x0a,
                             (const uint8_t *)argv[2], strlen(argv[2]));
    pos = encode_bytes_field(req, sizeof(req), pos, 0x12,
                             (const uint8_t *)argv[3], strlen(argv[3]));
    int rlen = do_rpc("/etcdserverpb.KV/Put", req, pos, resp, sizeof(resp));
    if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
    printf("OK\n");
    return 0;
}

static int cmd_get(int argc, char **argv) {
    if (argc < 3) { fprintf(stderr, "usage: cetcdctl get KEY\n"); return 1; }
    uint8_t req[1024], resp[8192];
    size_t pos = 0;
    pos = encode_bytes_field(req, sizeof(req), pos, 0x0a,
                             (const uint8_t *)argv[2], strlen(argv[2]));
    int rlen = do_rpc("/etcdserverpb.KV/Range", req, pos, resp, sizeof(resp));
    if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
    parse_range_response(resp, rlen);
    return 0;
}

static int cmd_del(int argc, char **argv) {
    if (argc < 3) { fprintf(stderr, "usage: cetcdctl del KEY\n"); return 1; }
    uint8_t req[1024], resp[4096];
    size_t pos = 0;
    pos = encode_bytes_field(req, sizeof(req), pos, 0x0a,
                             (const uint8_t *)argv[2], strlen(argv[2]));
    int rlen = do_rpc("/etcdserverpb.KV/DeleteRange", req, pos, resp, sizeof(resp));
    if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
    printf("OK\n");
    return 0;
}

static int cmd_lease(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "usage: cetcdctl lease grant TTL\n");
        fprintf(stderr, "       cetcdctl lease revoke ID\n");
        fprintf(stderr, "       cetcdctl lease timetolive ID\n");
        fprintf(stderr, "       cetcdctl lease list\n");
        fprintf(stderr, "       cetcdctl lease keepalive ID\n");
        return 1;
    }
    if (strcmp(argv[2], "grant") == 0) {
        if (argc < 4) { fprintf(stderr, "usage: cetcdctl lease grant TTL\n"); return 1; }
        uint8_t req[32], resp[256];
        size_t pos = 0;
        pos = encode_varint_field(req, sizeof(req), pos, 0x08, (uint64_t)atol(argv[3]));
        pos = encode_varint_field(req, sizeof(req), pos, 0x10, 0);
        int rlen = do_rpc("/etcdserverpb.Lease/LeaseGrant", req, pos, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        parse_lease_grant_response(resp, rlen);
    } else if (strcmp(argv[2], "revoke") == 0) {
        if (argc < 4) { fprintf(stderr, "usage: cetcdctl lease revoke ID\n"); return 1; }
        uint8_t req[32], resp[256];
        size_t pos = 0;
        pos = encode_varint_field(req, sizeof(req), pos, 0x08, (uint64_t)atol(argv[3]));
        int rlen = do_rpc("/etcdserverpb.Lease/LeaseRevoke", req, pos, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        printf("OK\n");
    } else if (strcmp(argv[2], "timetolive") == 0) {
        if (argc < 4) { fprintf(stderr, "usage: cetcdctl lease timetolive ID\n"); return 1; }
        uint8_t req[32], resp[256];
        size_t pos = 0;
        pos = encode_varint_field(req, sizeof(req), pos, 0x08, (uint64_t)atol(argv[3]));
        int rlen = do_rpc("/etcdserverpb.Lease/LeaseTimeToLive", req, pos, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        parse_lease_ttl_response(resp, rlen);
    } else if (strcmp(argv[2], "list") == 0) {
        uint8_t req[] = {0x00}, resp[4096];
        int rlen = do_rpc("/etcdserverpb.Lease/LeaseLeases", req, 1, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        /* Parse LeaseLeasesResponse: field 1 (leases) = repeated LeaseStatus */
        size_t rpos = 0;
        int count = 0;
        while (rpos < (size_t)rlen) {
            uint8_t tag = resp[rpos++];
            if (tag == 0x0a) {
                uint64_t lslen = 0; read_varint(resp, rlen, &rpos, &lslen);
                size_t lend = rpos + (size_t)lslen;
                while (rpos < lend) {
                    uint8_t ltag = resp[rpos++];
                    if (ltag == 0x08) {
                        uint64_t id = 0; read_varint(resp, lend, &rpos, &id);
                        printf("lease ID: %llu\n", (unsigned long long)id);
                        count++;
                    } else {
                        uint64_t v = 0; read_varint(resp, lend, &rpos, &v);
                    }
                }
                rpos = lend;
            } else {
                uint64_t v = 0; read_varint(resp, rlen, &rpos, &v);
            }
        }
        if (count == 0) printf("(no leases)\n");
    } else if (strcmp(argv[2], "keepalive") == 0) {
        if (argc < 4) { fprintf(stderr, "usage: cetcdctl lease keepalive ID\n"); return 1; }
        uint8_t req[32], resp[256];
        size_t pos = 0;
        pos = encode_varint_field(req, sizeof(req), pos, 0x08, (uint64_t)atol(argv[3]));
        int rlen = do_rpc("/etcdserverpb.Lease/LeaseKeepAlive", req, pos, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        /* Parse KeepAliveResponse: field 1 (ID), field 2 (TTL) */
        size_t rpos = 0;
        while (rpos < (size_t)rlen) {
            uint8_t tag = resp[rpos++];
            if (tag == 0x08) {
                uint64_t v = 0; read_varint(resp, rlen, &rpos, &v);
                printf("lease ID: %llu\n", (unsigned long long)v);
            } else if (tag == 0x10) {
                uint64_t v = 0; read_varint(resp, rlen, &rpos, &v);
                printf("TTL: %llu seconds\n", (unsigned long long)v);
            } else {
                uint64_t v = 0; read_varint(resp, rlen, &rpos, &v);
            }
        }
    } else {
        fprintf(stderr, "unknown lease subcommand: %s\n", argv[2]);
        return 1;
    }
    return 0;
}

static int cmd_compact(int argc, char **argv) {
    if (argc < 3) { fprintf(stderr, "usage: cetcdctl compact REV\n"); return 1; }
    uint8_t req[32], resp[256];
    size_t pos = 0;
    pos = encode_varint_field(req, sizeof(req), pos, 0x08, (uint64_t)atol(argv[2]));
    int rlen = do_rpc("/etcdserverpb.KV/Compact", req, pos, resp, sizeof(resp));
    if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
    printf("OK\n");
    return 0;
}

static int cmd_txn(int argc, char **argv) {
    if (argc < 5 || strcmp(argv[2], "put") != 0) {
        fprintf(stderr, "usage: cetcdctl txn put KEY VALUE\n");
        return 1;
    }
    /* Build a TxnRequest with one success op (Put) */
    uint8_t put_inner[1024];
    size_t ppos = 0;
    ppos = encode_bytes_field(put_inner, sizeof(put_inner), ppos, 0x0a,
                               (const uint8_t *)argv[3], strlen(argv[3]));
    ppos = encode_bytes_field(put_inner, sizeof(put_inner), ppos, 0x12,
                               (const uint8_t *)argv[4], strlen(argv[4]));

    uint8_t op_buf[1024];
    size_t opos = 0;
    op_buf[opos++] = 0x12; /* RequestPut tag */
    op_buf[opos++] = (uint8_t)ppos;
    memcpy(op_buf + opos, put_inner, ppos);
    opos += ppos;

    uint8_t req[2048], resp[1024];
    size_t pos = 0;
    req[pos++] = 0x12; /* field 2 = success ops */
    req[pos++] = (uint8_t)opos;
    memcpy(req + pos, op_buf, opos);
    pos += opos;

    int rlen = do_rpc("/etcdserverpb.KV/Txn", req, pos, resp, sizeof(resp));
    if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
    printf("OK\n");
    return 0;
}

static int cmd_status(int argc, char **argv) {
    (void)argc; (void)argv;
    uint8_t req[] = {0x00}, resp[1024];
    int rlen = do_rpc("/etcdserverpb.Maintenance/Status", req, 1, resp, sizeof(resp));
    if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
    parse_status_response(resp, rlen);
    return 0;
}

static int cmd_alarm(int argc, char **argv) {
    (void)argc; (void)argv;
    uint8_t req[] = {0x00}, resp[1024];
    int rlen = do_rpc("/etcdserverpb.Maintenance/Alarm", req, 1, resp, sizeof(resp));
    if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
    printf("no alarms\n");
    return 0;
}

static int cmd_snapshot(int argc, char **argv) {
    if (argc < 3 || strcmp(argv[2], "save") != 0) {
        fprintf(stderr, "usage: cetcdctl snapshot save [FILE]\n");
        return 1;
    }
    uint8_t req[] = {0x00}, resp[65536];
    int rlen = do_rpc("/etcdserverpb.Maintenance/Snapshot", req, 1, resp, sizeof(resp));
    if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
    /* If a file is specified, write the blob to it */
    if (argc >= 4) {
        FILE *f = fopen(argv[3], "wb");
        if (!f) { perror("fopen"); return 1; }
        fwrite(resp, 1, (size_t)rlen, f);
        fclose(f);
        printf("snapshot saved to %s (%d bytes)\n", argv[3], rlen);
    } else {
        printf("snapshot: %d bytes received\n", rlen);
    }
    return 0;
}

static int cmd_downgrade(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "usage: cetcdctl downgrade enable VERSION\n");
        fprintf(stderr, "       cetcdctl downgrade cancel\n");
        fprintf(stderr, "       cetcdctl downgrade validate VERSION\n");
        return 1;
    }
    uint8_t req[256], resp[256];
    size_t pos = 0;
    const char *version = "";
    uint64_t action = 1; /* ENABLE */

    if (strcmp(argv[2], "enable") == 0) {
        if (argc < 4) { fprintf(stderr, "usage: cetcdctl downgrade enable VERSION\n"); return 1; }
        action = 1; version = argv[3];
    } else if (strcmp(argv[2], "cancel") == 0) {
        action = 2;
    } else if (strcmp(argv[2], "validate") == 0) {
        if (argc < 4) { fprintf(stderr, "usage: cetcdctl downgrade validate VERSION\n"); return 1; }
        action = 0; version = argv[3];
    } else {
        fprintf(stderr, "unknown downgrade subcommand: %s\n", argv[2]);
        return 1;
    }

    pos = encode_varint_field(req, sizeof(req), pos, 0x08, action);
    if (version[0])
        pos = encode_string_field(req, sizeof(req), pos, 0x12, version);

    int rlen = do_rpc("/etcdserverpb.Maintenance/Downgrade", req, pos, resp, sizeof(resp));
    if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
    printf("OK\n");
    return 0;
}

static int cmd_member(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "usage: cetcdctl member list\n");
        fprintf(stderr, "       cetcdctl member add PEER_URL\n");
        fprintf(stderr, "       cetcdctl member remove ID\n");
        fprintf(stderr, "       cetcdctl member update ID PEER_URL\n");
        return 1;
    }
    if (strcmp(argv[2], "list") == 0) {
        uint8_t req[] = {0x00}, resp[4096];
        int rlen = do_rpc("/etcdserverpb.Cluster/MemberList", req, 1, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        parse_member_list_response(resp, rlen);
    } else if (strcmp(argv[2], "add") == 0) {
        if (argc < 4) { fprintf(stderr, "usage: cetcdctl member add PEER_URL\n"); return 1; }
        uint8_t req[512], resp[4096];
        size_t pos = 0;
        pos = encode_string_field(req, sizeof(req), pos, 0x0a, argv[3]);
        int rlen = do_rpc("/etcdserverpb.Cluster/MemberAdd", req, pos, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        parse_member_list_response(resp, rlen);
    } else if (strcmp(argv[2], "remove") == 0) {
        if (argc < 4) { fprintf(stderr, "usage: cetcdctl member remove ID\n"); return 1; }
        uint8_t req[32], resp[256];
        size_t pos = 0;
        pos = encode_varint_field(req, sizeof(req), pos, 0x08, (uint64_t)atol(argv[3]));
        int rlen = do_rpc("/etcdserverpb.Cluster/MemberRemove", req, pos, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        printf("OK\n");
    } else if (strcmp(argv[2], "update") == 0) {
        if (argc < 5) { fprintf(stderr, "usage: cetcdctl member update ID PEER_URL\n"); return 1; }
        uint8_t req[512], resp[256];
        size_t pos = 0;
        pos = encode_varint_field(req, sizeof(req), pos, 0x08, (uint64_t)atol(argv[3]));
        pos = encode_string_field(req, sizeof(req), pos, 0x12, argv[4]);
        int rlen = do_rpc("/etcdserverpb.Cluster/MemberUpdate", req, pos, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        printf("OK\n");
    } else {
        fprintf(stderr, "unknown member subcommand: %s\n", argv[2]);
        return 1;
    }
    return 0;
}

static int cmd_auth(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "usage: cetcdctl auth enable|disable|status\n");
        return 1;
    }
    uint8_t req[] = {0x00}, resp[1024];
    const char *path = NULL;
    if (strcmp(argv[2], "enable") == 0) {
        path = "/etcdserverpb.Auth/AuthEnable";
    } else if (strcmp(argv[2], "disable") == 0) {
        path = "/etcdserverpb.Auth/AuthDisable";
    } else if (strcmp(argv[2], "status") == 0) {
        path = "/etcdserverpb.Auth/AuthStatus";
    } else {
        fprintf(stderr, "unknown auth subcommand: %s\n", argv[2]);
        return 1;
    }
    int rlen = do_rpc(path, req, 1, resp, sizeof(resp));
    if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
    if (strcmp(argv[2], "status") == 0) {
        parse_auth_status_response(resp, rlen);
    } else {
        printf("OK\n");
    }
    return 0;
}

static int cmd_user(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "usage: cetcdctl user add NAME PASS\n");
        fprintf(stderr, "       cetcdctl user delete NAME\n");
        fprintf(stderr, "       cetcdctl user get NAME\n");
        fprintf(stderr, "       cetcdctl user list\n");
        fprintf(stderr, "       cetcdctl user change-password NAME PASS\n");
        fprintf(stderr, "       cetcdctl user grant-role NAME ROLE\n");
        fprintf(stderr, "       cetcdctl user revoke-role NAME ROLE\n");
        return 1;
    }
    if (strcmp(argv[2], "add") == 0) {
        if (argc < 5) { fprintf(stderr, "usage: cetcdctl user add NAME PASS\n"); return 1; }
        uint8_t req[512], resp[256];
        size_t pos = 0;
        pos = encode_string_field(req, sizeof(req), pos, 0x0a, argv[3]);
        pos = encode_string_field(req, sizeof(req), pos, 0x12, argv[4]);
        int rlen = do_rpc("/etcdserverpb.Auth/UserAdd", req, pos, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        printf("OK\n");
    } else if (strcmp(argv[2], "delete") == 0) {
        if (argc < 4) { fprintf(stderr, "usage: cetcdctl user delete NAME\n"); return 1; }
        uint8_t req[256], resp[256];
        size_t pos = 0;
        pos = encode_string_field(req, sizeof(req), pos, 0x0a, argv[3]);
        int rlen = do_rpc("/etcdserverpb.Auth/UserDelete", req, pos, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        printf("OK\n");
    } else if (strcmp(argv[2], "get") == 0) {
        if (argc < 4) { fprintf(stderr, "usage: cetcdctl user get NAME\n"); return 1; }
        uint8_t req[256], resp[4096];
        size_t pos = 0;
        pos = encode_string_field(req, sizeof(req), pos, 0x0a, argv[3]);
        int rlen = do_rpc("/etcdserverpb.Auth/UserGet", req, pos, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        printf("roles:\n");
        parse_string_list_response(resp, rlen, "roles");
    } else if (strcmp(argv[2], "list") == 0) {
        uint8_t req[] = {0x00}, resp[4096];
        int rlen = do_rpc("/etcdserverpb.Auth/UserList", req, 1, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        parse_string_list_response(resp, rlen, "users");
    } else if (strcmp(argv[2], "change-password") == 0) {
        if (argc < 5) { fprintf(stderr, "usage: cetcdctl user change-password NAME PASS\n"); return 1; }
        uint8_t req[512], resp[256];
        size_t pos = 0;
        pos = encode_string_field(req, sizeof(req), pos, 0x0a, argv[3]);
        pos = encode_string_field(req, sizeof(req), pos, 0x12, argv[4]);
        int rlen = do_rpc("/etcdserverpb.Auth/UserChangePassword", req, pos, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        printf("OK\n");
    } else if (strcmp(argv[2], "grant-role") == 0) {
        if (argc < 5) { fprintf(stderr, "usage: cetcdctl user grant-role NAME ROLE\n"); return 1; }
        uint8_t req[512], resp[256];
        size_t pos = 0;
        pos = encode_string_field(req, sizeof(req), pos, 0x0a, argv[3]);
        pos = encode_string_field(req, sizeof(req), pos, 0x12, argv[4]);
        int rlen = do_rpc("/etcdserverpb.Auth/UserGrantRole", req, pos, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        printf("OK\n");
    } else if (strcmp(argv[2], "revoke-role") == 0) {
        if (argc < 5) { fprintf(stderr, "usage: cetcdctl user revoke-role NAME ROLE\n"); return 1; }
        uint8_t req[512], resp[256];
        size_t pos = 0;
        pos = encode_string_field(req, sizeof(req), pos, 0x0a, argv[3]);
        pos = encode_string_field(req, sizeof(req), pos, 0x12, argv[4]);
        int rlen = do_rpc("/etcdserverpb.Auth/UserRevokeRole", req, pos, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        printf("OK\n");
    } else {
        fprintf(stderr, "unknown user subcommand: %s\n", argv[2]);
        return 1;
    }
    return 0;
}

static int cmd_role(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "usage: cetcdctl role add NAME\n");
        fprintf(stderr, "       cetcdctl role get NAME\n");
        fprintf(stderr, "       cetcdctl role list\n");
        fprintf(stderr, "       cetcdctl role grant-permission ROLE TYPE KEY\n");
        fprintf(stderr, "       cetcdctl role revoke-permission ROLE\n");
        return 1;
    }
    if (strcmp(argv[2], "add") == 0) {
        if (argc < 4) { fprintf(stderr, "usage: cetcdctl role add NAME\n"); return 1; }
        uint8_t req[256], resp[256];
        size_t pos = 0;
        pos = encode_string_field(req, sizeof(req), pos, 0x0a, argv[3]);
        int rlen = do_rpc("/etcdserverpb.Auth/RoleAdd", req, pos, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        printf("OK\n");
    } else if (strcmp(argv[2], "delete") == 0) {
        if (argc < 4) { fprintf(stderr, "usage: cetcdctl role delete NAME\n"); return 1; }
        uint8_t req[256], resp[256];
        size_t pos = 0;
        pos = encode_string_field(req, sizeof(req), pos, 0x0a, argv[3]);
        int rlen = do_rpc("/etcdserverpb.Auth/RoleDelete", req, pos, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        printf("OK\n");
    } else if (strcmp(argv[2], "list") == 0) {
        uint8_t req[] = {0x00}, resp[4096];
        int rlen = do_rpc("/etcdserverpb.Auth/RoleList", req, 1, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        parse_string_list_response(resp, rlen, "roles");
    } else if (strcmp(argv[2], "get") == 0) {
        if (argc < 4) { fprintf(stderr, "usage: cetcdctl role get NAME\n"); return 1; }
        uint8_t req[256], resp[1024];
        size_t pos = 0;
        pos = encode_string_field(req, sizeof(req), pos, 0x0a, argv[3]);
        int rlen = do_rpc("/etcdserverpb.Auth/RoleGet", req, pos, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        printf("role: %s\n", argv[3]);
        /* Parse permission from response */
        size_t rpos = 0;
        while (rpos < (size_t)rlen) {
            uint8_t tag = resp[rpos++];
            if (tag == 0x12) {
                /* Permission (length-delimited) */
                uint64_t plen = 0; read_varint(resp, rlen, &rpos, &plen);
                size_t pend = rpos + (size_t)plen;
                while (rpos < pend) {
                    uint8_t ptag = resp[rpos++];
                    if (ptag == 0x08) {
                        uint64_t pt = 0; read_varint(resp, pend, &rpos, &pt);
                        printf("  permType: %s\n", pt == 0 ? "READ" : pt == 1 ? "WRITE" : "READWRITE");
                    } else if (ptag == 0x0a) {
                        uint64_t l = 0; read_varint(resp, pend, &rpos, &l);
                        printf("  key: %.*s\n", (int)l, resp + rpos);
                        rpos += l;
                    } else {
                        uint64_t v = 0; read_varint(resp, pend, &rpos, &v);
                    }
                }
                rpos = pend;
            } else {
                uint64_t v = 0; read_varint(resp, rlen, &rpos, &v);
            }
        }
    } else if (strcmp(argv[2], "grant-permission") == 0) {
        if (argc < 6) {
            fprintf(stderr, "usage: cetcdctl role grant-permission ROLE TYPE KEY\n");
            fprintf(stderr, "  TYPE: read | write | readwrite\n");
            return 1;
        }
        int perm_type = 2; /* default readwrite */
        if (strcmp(argv[4], "read") == 0) perm_type = 0;
        else if (strcmp(argv[4], "write") == 0) perm_type = 1;
        else if (strcmp(argv[4], "readwrite") == 0) perm_type = 2;
        else { fprintf(stderr, "invalid TYPE: %s (use read|write|readwrite)\n", argv[4]); return 1; }

        /* Build Permission sub-message */
        uint8_t perm[256];
        size_t ppos = 0;
        perm[ppos++] = 0x08; /* field 1 = permType */
        perm[ppos++] = (uint8_t)perm_type;
        size_t klen = strlen(argv[5]);
        perm[ppos++] = 0x0a; /* field 2 = key */
        /* varint for key length */
        uint64_t l = klen;
        while (l >= 0x80) { perm[ppos++] = (uint8_t)(l | 0x80); l >>= 7; }
        perm[ppos++] = (uint8_t)l;
        memcpy(perm + ppos, argv[5], klen); ppos += klen;

        /* Build RoleGrantPermissionRequest */
        uint8_t req[512], resp[256];
        size_t pos = 0;
        pos = encode_string_field(req, sizeof(req), pos, 0x0a, argv[3]);
        req[pos++] = 0x12; /* field 2 = perm */
        l = ppos;
        while (l >= 0x80) { req[pos++] = (uint8_t)(l | 0x80); l >>= 7; }
        req[pos++] = (uint8_t)l;
        memcpy(req + pos, perm, ppos); pos += ppos;

        int rlen = do_rpc("/etcdserverpb.Auth/RoleGrantPermission", req, pos, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        printf("OK\n");
    } else if (strcmp(argv[2], "revoke-permission") == 0) {
        if (argc < 4) { fprintf(stderr, "usage: cetcdctl role revoke-permission ROLE\n"); return 1; }
        uint8_t req[256], resp[256];
        size_t pos = 0;
        pos = encode_string_field(req, sizeof(req), pos, 0x0a, argv[3]);
        int rlen = do_rpc("/etcdserverpb.Auth/RoleRevokePermission", req, pos, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        printf("OK\n");
    } else {
        fprintf(stderr, "unknown role subcommand: %s\n", argv[2]);
        return 1;
    }
    return 0;
}

static int cmd_hash(int argc, char **argv) {
    (void)argc; (void)argv;
    uint8_t req[] = {0x00}, resp[256];
    int rlen = do_rpc("/etcdserverpb.Maintenance/Hash", req, 1, resp, sizeof(resp));
    if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
    /* HashResponse: field 2 (hash) = uint32, tag = 0x10 */
    size_t rpos = 0;
    while (rpos < (size_t)rlen) {
        uint8_t tag = resp[rpos++];
        if (tag == 0x10) {
            uint64_t v = 0; read_varint(resp, rlen, &rpos, &v);
            printf("hash: %llu\n", (unsigned long long)v);
        } else {
            uint64_t v = 0; read_varint(resp, rlen, &rpos, &v);
        }
    }
    return 0;
}

static int cmd_hashkv(int argc, char **argv) {
    (void)argc; (void)argv;
    uint8_t req[] = {0x00}, resp[256];
    int rlen = do_rpc("/etcdserverpb.Maintenance/HashKV", req, 1, resp, sizeof(resp));
    if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
    /* HashKVResponse: field 2 (hash), field 3 (compact_revision) */
    size_t rpos = 0;
    while (rpos < (size_t)rlen) {
        uint8_t tag = resp[rpos++];
        if (tag == 0x10) {
            uint64_t v = 0; read_varint(resp, rlen, &rpos, &v);
            printf("hash: %llu\n", (unsigned long long)v);
        } else if (tag == 0x18) {
            uint64_t v = 0; read_varint(resp, rlen, &rpos, &v);
            printf("compact_revision: %llu\n", (unsigned long long)v);
        } else {
            uint64_t v = 0; read_varint(resp, rlen, &rpos, &v);
        }
    }
    return 0;
}

static int cmd_defrag(int argc, char **argv) {
    (void)argc; (void)argv;
    uint8_t req[] = {0x00}, resp[256];
    int rlen = do_rpc("/etcdserverpb.Maintenance/Defragment", req, 1, resp, sizeof(resp));
    if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
    printf("OK\n");
    return 0;
}

static int cmd_move_leader(int argc, char **argv) {
    if (argc < 3) { fprintf(stderr, "usage: cetcdctl move-leader TARGET_ID\n"); return 1; }
    uint8_t req[32], resp[256];
    size_t pos = 0;
    pos = encode_varint_field(req, sizeof(req), pos, 0x08, (uint64_t)atol(argv[2]));
    int rlen = do_rpc("/etcdserverpb.Maintenance/MoveLeader", req, pos, resp, sizeof(resp));
    if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
    printf("OK\n");
    return 0;
}

static void print_usage(void) {
    printf("cetcdctl — command-line client for cetcd\n\n");
    printf("Usage: cetcdctl [global options] COMMAND [args]\n\n");
    printf("Global options:\n");
    printf("  --host ADDR    Server address (default: 127.0.0.1)\n");
    printf("  --port PORT    Server port (default: 2379)\n\n");
    printf("Commands:\n");
    printf("  put KEY VALUE          Store a key-value pair\n");
    printf("  get KEY                Retrieve a key\n");
    printf("  del KEY                Delete a key\n");
    printf("  lease grant TTL        Grant a lease (TTL in seconds)\n");
    printf("  lease revoke ID        Revoke a lease by ID\n");
    printf("  lease timetolive ID    Query remaining TTL\n");
    printf("  lease list             List all active leases\n");
    printf("  lease keepalive ID     Keep a lease alive\n");
    printf("  txn put KEY VALUE      Execute a transaction (Put)\n");
    printf("  compact REV            Compact MVCC history to revision\n");
    printf("  status                 Get server status\n");
    printf("  alarm                  Query alarms\n");
    printf("  hash                   Get KV store hash\n");
    printf("  hashkv                 Get KV store hash + compact revision\n");
    printf("  defrag                 Defragment database (no-op for LMDB)\n");
    printf("  move-leader TARGET_ID  Transfer leadership to target node\n");
    printf("  member list            List cluster members\n");
    printf("  member add PEER_URL    Add a cluster member\n");
    printf("  member remove ID       Remove a cluster member\n");
    printf("  member update ID URL   Update a member's peer URL\n");
    printf("  auth enable            Enable authentication\n");
    printf("  auth disable           Disable authentication\n");
    printf("  auth status            Query auth status\n");
    printf("  user add NAME PASS     Add a user\n");
    printf("  user delete NAME       Delete a user\n");
    printf("  user get NAME          Get user details (roles)\n");
    printf("  user list              List all users\n");
    printf("  user change-password NAME PASS  Change user password\n");
    printf("  user grant-role NAME ROLE       Grant role to user\n");
    printf("  user revoke-role NAME ROLE      Revoke role from user\n");
    printf("  role add NAME          Add a role\n");
    printf("  role delete NAME       Delete a role\n");
    printf("  role get NAME          Get role details (permissions)\n");
    printf("  role list              List all roles\n");
    printf("  role grant-permission ROLE TYPE KEY\n");
    printf("                         Grant permission (read|write|readwrite)\n");
    printf("  role revoke-permission ROLE\n");
    printf("                         Revoke all permissions from role\n");
    printf("  snapshot save [FILE]   Save a snapshot to file\n");
    printf("  downgrade enable VER   Enable cluster downgrade\n");
    printf("  downgrade cancel       Cancel cluster downgrade\n");
    printf("  downgrade validate VER Validate downgrade version\n");
}

int main(int argc, char **argv) {
    /* Parse global options */
    int cmd_start = 1;
    while (cmd_start < argc) {
        if (strcmp(argv[cmd_start], "--host") == 0 && cmd_start + 1 < argc) {
            g_host = argv[cmd_start + 1];
            cmd_start += 2;
        } else if (strcmp(argv[cmd_start], "--port") == 0 && cmd_start + 1 < argc) {
            g_port = (uint16_t)atoi(argv[cmd_start + 1]);
            cmd_start += 2;
        } else if (strcmp(argv[cmd_start], "--help") == 0 || strcmp(argv[cmd_start], "-h") == 0) {
            print_usage();
            return 0;
        } else {
            break;
        }
    }

    if (cmd_start >= argc) {
        print_usage();
        return 1;
    }

    /* Shift args so command is at argv[1] */
    int new_argc = argc - cmd_start + 1;
    char **new_argv = argv + cmd_start - 1;

    const char *cmd = new_argv[1];
    if (strcmp(cmd, "put") == 0)         return cmd_put(new_argc, new_argv);
    if (strcmp(cmd, "get") == 0)         return cmd_get(new_argc, new_argv);
    if (strcmp(cmd, "del") == 0)         return cmd_del(new_argc, new_argv);
    if (strcmp(cmd, "lease") == 0)       return cmd_lease(new_argc, new_argv);
    if (strcmp(cmd, "compact") == 0)     return cmd_compact(new_argc, new_argv);
    if (strcmp(cmd, "txn") == 0)         return cmd_txn(new_argc, new_argv);
    if (strcmp(cmd, "status") == 0)      return cmd_status(new_argc, new_argv);
    if (strcmp(cmd, "alarm") == 0)       return cmd_alarm(new_argc, new_argv);
    if (strcmp(cmd, "hash") == 0)        return cmd_hash(new_argc, new_argv);
    if (strcmp(cmd, "hashkv") == 0)      return cmd_hashkv(new_argc, new_argv);
    if (strcmp(cmd, "defrag") == 0)      return cmd_defrag(new_argc, new_argv);
    if (strcmp(cmd, "move-leader") == 0) return cmd_move_leader(new_argc, new_argv);
    if (strcmp(cmd, "member") == 0)      return cmd_member(new_argc, new_argv);
    if (strcmp(cmd, "auth") == 0)        return cmd_auth(new_argc, new_argv);
    if (strcmp(cmd, "user") == 0)        return cmd_user(new_argc, new_argv);
    if (strcmp(cmd, "snapshot") == 0)   return cmd_snapshot(new_argc, new_argv);
    if (strcmp(cmd, "downgrade") == 0)  return cmd_downgrade(new_argc, new_argv);
    if (strcmp(cmd, "role") == 0)        return cmd_role(new_argc, new_argv);

    fprintf(stderr, "unknown command: %s\n", cmd);
    print_usage();
    return 1;
}
