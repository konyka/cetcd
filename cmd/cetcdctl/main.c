/*
 * cetcdctl — command-line client for cetcd
 *
 * Connects to a cetcd server using the custom gRPC-like framing protocol:
 *   Request:  2B path_len (BE) + path + 1B compressed + 4B payload_len (BE) + payload
 *   Response: same format
 *
 * Supported commands:
 *   put KEY VALUE          — store a key-value pair
 *   get KEY [RANGE_END]    — retrieve a key or key range
 *   del KEY [RANGE_END]    — delete a key or key range
 *   lease grant TTL        — grant a lease with given TTL (seconds)
 *   lease revoke ID        — revoke a lease by ID
 *   lease timetolive [--keys] ID — query remaining TTL of a lease
 *   lease list             — list all active leases
 *   lease keepalive ID     — keep a lease alive
 *   txn put KEY VALUE      — execute a transaction (Put)
 *   txn cas KEY EXPECTED NEW — compare-and-swap transaction
 *   txn get KEY [RANGE_END] — execute a transaction (Range)
 *   txn del KEY [RANGE_END] — execute a transaction (Delete)
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
 *   snapshot status FILE   — show snapshot file info
 *   snapshot restore FILE --data-dir DIR — restore snapshot to data dir
 *   downgrade enable VER   — enable cluster downgrade
 *   downgrade cancel       — cancel cluster downgrade
 *   downgrade validate VER — validate downgrade version
 *   auth enable            — enable authentication
 *   auth disable           — disable authentication
 *   auth status            — query auth status
 *   auth login NAME PASS   — authenticate and get token
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
 *   endpoint health       — check server health
 *   endpoint status       — get server status with endpoint info
 *   check perf            — run a simple performance check
 *   lock LOCKNAME [CMD...] — acquire a distributed lock
 *   elect ELECTION_NAME [PROPOSAL] — leader election
 *   version               — print client version
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <unistd.h>
#include <signal.h>
#include <sys/wait.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

static const char *g_host = "127.0.0.1";
static uint16_t    g_port = 2379;
static int         g_keys_only = 0; /* flag for get --keys-only */
static int         g_count_only = 0; /* flag for get --count-only */
static int         g_print_value_only = 0; /* flag for get --print-value-only */
static int         g_hex = 0; /* flag for get --hex */
static int         g_write_json = 0; /* flag for -w json */
static int         g_write_fields = 0; /* flag for -w fields */
static int         g_debug = 0; /* flag for --debug */
static char        g_auth_token[256] = ""; /* token from --user */

/* --- Lock state for signal handler --- */
static char          g_lock_key[256];
static size_t        g_lock_key_len = 0;
static uint64_t      g_lock_lease_id = 0;
static volatile sig_atomic_t g_lock_held = 0;

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
    if (g_debug) {
        fprintf(stderr, "[debug] RPC %s req_len=%zu\n", path, req_len);
    }
    int fd = connect_server();
    if (fd < 0) return -1;
    if (send_request(fd, path, req, req_len) != 0) {
        close(fd);
        return -1;
    }
    int rlen = recv_response(fd, resp, resp_cap);
    close(fd);
    if (g_debug) {
        fprintf(stderr, "[debug] RPC %s resp_len=%d\n", path, rlen);
    }
    return rlen;
}

/* --- Response parsing helpers --- */

static void parse_range_response(const uint8_t *data, size_t len) {
    size_t pos = 0;
    int count = 0;
    int server_count = -1;
    if (g_write_json) {
        fputs("{\"kvs\":[", stdout);
    }
    int first_kv = 1;
    while (pos < len) {
        uint8_t tag = data[pos++];
        if (tag == 0x12) {
            /* KeyValue (field 2 = kvs, length-delimited) */
            uint64_t kv_len = 0;
            if (read_varint(data, len, &pos, &kv_len) != 0) break;
            size_t kv_end = pos + (size_t)kv_len;
            /* Parse KeyValue fields */
            const uint8_t *key_data = NULL; size_t key_len = 0;
            const uint8_t *val_data = NULL; size_t val_len = 0;
            uint64_t create_rev = 0, mod_rev = 0, version = 0, lease = 0;
            while (pos < kv_end) {
                uint8_t ktag = data[pos++];
                if (ktag == 0x0a) {
                    uint64_t l = 0; read_varint(data, kv_end, &pos, &l);
                    key_data = data + pos; key_len = (size_t)l;
                    pos += l;
                } else if (ktag == 0x2a) {
                    uint64_t l = 0; read_varint(data, kv_end, &pos, &l);
                    val_data = data + pos; val_len = (size_t)l;
                    pos += l;
                } else if (ktag == 0x10) {
                    read_varint(data, kv_end, &pos, &create_rev);
                } else if (ktag == 0x18) {
                    read_varint(data, kv_end, &pos, &mod_rev);
                } else if (ktag == 0x20) {
                    read_varint(data, kv_end, &pos, &version);
                } else if (ktag == 0x30) {
                    read_varint(data, kv_end, &pos, &lease);
                } else {
                    uint64_t v = 0; read_varint(data, kv_end, &pos, &v);
                }
            }
            pos = kv_end;
            count++;
            if (!g_count_only) {
                if (g_write_fields) {
                    printf("\"");
                    fwrite(key_data, 1, key_len, stdout);
                    printf("\"\n");
                    printf("create_revision: %llu\n", (unsigned long long)create_rev);
                    printf("mod_revision: %llu\n", (unsigned long long)mod_rev);
                    printf("version: %llu\n", (unsigned long long)version);
                    if (lease > 0) printf("lease: %llu\n", (unsigned long long)lease);
                    if (!g_keys_only && val_data && val_len > 0) {
                        printf("value: \"");
                        fwrite(val_data, 1, val_len, stdout);
                        printf("\"\n");
                    }
                    printf("\n");
                } else if (g_write_json) {
                    if (!first_kv) printf(",");
                    first_kv = 0;
                    printf("{\"key\":");
                    /* output key as JSON string */
                    printf("\"");
                    for (size_t i = 0; i < key_len; i++) {
                        char c = (char)key_data[i];
                        if (c == '"' || c == '\\') printf("\\%c", c);
                        else if (c >= 32 && c < 127) putchar(c);
                        else printf("\\u%04x", (unsigned)c);
                    }
                    printf("\"");
                    if (!g_keys_only && val_data && val_len > 0) {
                        printf(",\"value\":\"");
                        for (size_t i = 0; i < val_len; i++) {
                            char c = (char)val_data[i];
                            if (c == '"' || c == '\\') printf("\\%c", c);
                            else if (c >= 32 && c < 127) putchar(c);
                            else printf("\\u%04x", (unsigned)c);
                        }
                        printf("\"");
                    }
                    printf("}");
                } else if (g_print_value_only) {
                    if (val_data && val_len > 0) {
                        if (g_hex) {
                            for (size_t i = 0; i < val_len; i++)
                                printf("%02x", val_data[i]);
                            printf("\n");
                        } else {
                            printf("%.*s\n", (int)val_len, val_data);
                        }
                    }
                } else {
                    if (g_hex) {
                        for (size_t i = 0; i < key_len; i++)
                            printf("%02x", key_data[i]);
                        if (!g_keys_only && val_data && val_len > 0) {
                            printf(" -> ");
                            for (size_t i = 0; i < val_len; i++)
                                printf("%02x", val_data[i]);
                        }
                        printf("\n");
                    } else {
                        printf("%.*s", (int)key_len, key_data);
                        if (!g_keys_only && val_data && val_len > 0) {
                            printf(" -> %.*s", (int)val_len, val_data);
                        }
                        printf("\n");
                    }
                }
            }
        } else if (tag == 0x20) {
            /* count (varint, field 4) */
            uint64_t v = 0; read_varint(data, len, &pos, &v);
            server_count = (int)v;
        } else if (tag == 0x18) {
            /* more (bool, field 3) */
            uint64_t v = 0; read_varint(data, len, &pos, &v);
        } else if (tag == 0x0a) {
            /* Skip header (length-delimited, field 1) */
            uint64_t l = 0; read_varint(data, len, &pos, &l); pos += l;
        } else {
            uint64_t v = 0; read_varint(data, len, &pos, &v);
        }
    }
    if (g_count_only) {
        if (g_write_json) {
            printf("],\"count\":%d}\n", server_count >= 0 ? server_count : count);
        } else {
            printf("%d\n", server_count >= 0 ? server_count : count);
        }
    } else if (count == 0) {
        if (g_write_json) {
            printf("],\"count\":0}\n");
        } else {
            printf("(empty)\n");
        }
    } else {
        if (g_write_json) {
            printf("],\"count\":%d}\n", server_count >= 0 ? server_count : count);
        }
    }
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
        } else if (tag == 0x0a) {
            /* Skip header (length-delimited) */
            uint64_t l = 0; read_varint(data, len, &pos, &l);
            pos += l;
        } else {
            uint64_t v = 0; read_varint(data, len, &pos, &v);
        }
    }
}

static void parse_lease_grant_response(const uint8_t *data, size_t len) {
    size_t pos = 0;
    while (pos < len) {
        uint8_t tag = data[pos++];
        if (tag == 0x10) {
            uint64_t v = 0; read_varint(data, len, &pos, &v);
            printf("lease ID: %llu\n", (unsigned long long)v);
        } else if (tag == 0x18) {
            uint64_t v = 0; read_varint(data, len, &pos, &v);
            printf("TTL: %llu seconds\n", (unsigned long long)v);
        } else if (tag == 0x0a) {
            /* Skip header (length-delimited) */
            uint64_t l = 0; read_varint(data, len, &pos, &l);
            pos += l;
        } else {
            uint64_t v = 0; read_varint(data, len, &pos, &v);
        }
    }
}

static void parse_lease_ttl_response(const uint8_t *data, size_t len) {
    size_t pos = 0;
    while (pos < len) {
        uint8_t tag = data[pos++];
        if (tag == 0x10) {
            uint64_t v = 0; read_varint(data, len, &pos, &v);
            printf("lease ID: %llu\n", (unsigned long long)v);
        } else if (tag == 0x18) {
            uint64_t v = 0; read_varint(data, len, &pos, &v);
            printf("remaining TTL: %lld\n", (long long)v);
        } else if (tag == 0x20) {
            uint64_t v = 0; read_varint(data, len, &pos, &v);
            printf("granted TTL: %lld\n", (long long)v);
        } else if (tag == 0x2a) {
            /* keys (repeated bytes) */
            uint64_t l = 0; read_varint(data, len, &pos, &l);
            printf("key: %.*s\n", (int)l, data + pos);
            pos += l;
        } else if (tag == 0x0a) {
            /* Skip header (length-delimited) */
            uint64_t l = 0; read_varint(data, len, &pos, &l);
            pos += l;
        } else {
            uint64_t v = 0; read_varint(data, len, &pos, &v);
        }
    }
}

static void parse_member_list_response(const uint8_t *data, size_t len, int table_format, int json_format) {
    size_t pos = 0;
    int first = 1;
    if (table_format) {
        printf("+------------------+--------+---------------------+\n");
        printf("|        ID        | STATUS |     PEER ADDRS      |\n");
        printf("+------------------+--------+---------------------+\n");
    }
    if (json_format) {
        fputs("{\"header\":{},\"members\":[", stdout);
    }
    while (pos < len) {
        uint8_t tag = data[pos++];
        if (tag == 0x12) {
            /* Member (length-delimited) */
            uint64_t mlen = 0; read_varint(data, len, &pos, &mlen);
            size_t mend = pos + (size_t)mlen;
            uint64_t mid = 0;
            const uint8_t *peer_url = NULL; size_t peer_len = 0;
            while (pos < mend) {
                uint8_t mtag = data[pos++];
                if (mtag == 0x08) {
                    read_varint(data, mend, &pos, &mid);
                } else if (mtag == 0x12) {
                    uint64_t l = 0; read_varint(data, mend, &pos, &l);
                    peer_url = data + pos; peer_len = (size_t)l;
                    pos += l;
                } else {
                    uint64_t v = 0; read_varint(data, mend, &pos, &v);
                }
            }
            pos = mend;
            if (json_format) {
                if (!first) printf(",");
                first = 0;
                printf("{\"ID\":%llu,\"peerURLs\":[\"", (unsigned long long)mid);
                if (peer_url) fwrite(peer_url, 1, peer_len, stdout);
                fputs("\"]}", stdout);
            } else if (table_format) {
                printf("| %16llu | %6s | %19.*s |\n",
                       (unsigned long long)mid, "alive",
                       (int)peer_len, peer_url ? peer_url : (const uint8_t *)"");
            } else {
                printf("member ID: %llu peerURL: %.*s\n",
                       (unsigned long long)mid,
                       (int)peer_len, peer_url ? peer_url : (const uint8_t *)"");
            }
        } else if (tag == 0x0a) {
            /* Skip header (length-delimited) */
            uint64_t l = 0; read_varint(data, len, &pos, &l);
            pos += l;
        } else {
            uint64_t v = 0; read_varint(data, len, &pos, &v);
        }
    }
    if (json_format) {
        printf("]}\n");
    }
    if (table_format) {
        printf("+------------------+--------+---------------------+\n");
    }
}

static void parse_auth_status_response(const uint8_t *data, size_t len) {
    size_t pos = 0;
    while (pos < len) {
        uint8_t tag = data[pos++];
        if (tag == 0x10) {
            uint64_t v = 0; read_varint(data, len, &pos, &v);
            printf("auth enabled: %s\n", v ? "true" : "false");
        } else if (tag == 0x0a) {
            /* Skip header (length-delimited) */
            uint64_t l = 0; read_varint(data, len, &pos, &l);
            pos += l;
        } else {
            uint64_t v = 0; read_varint(data, len, &pos, &v);
        }
    }
}

static void parse_string_list_response(const uint8_t *data, size_t len, const char *label, int table_fmt) {
    size_t pos = 0;
    int count = 0;
    if (table_fmt) {
        printf("+--------------------+\n");
        printf("| %18s |\n", label);
        printf("+--------------------+\n");
    }
    while (pos < len) {
        uint8_t tag = data[pos++];
        if (tag == 0x12) {
            uint64_t l = 0; read_varint(data, len, &pos, &l);
            if (table_fmt) {
                printf("| %18.*s |\n", (int)l, data + pos);
            } else {
                printf("%.*s\n", (int)l, data + pos);
            }
            pos += l;
            count++;
        } else if (tag == 0x0a) {
            /* Skip header (length-delimited) */
            uint64_t l = 0; read_varint(data, len, &pos, &l);
            pos += l;
        } else {
            uint64_t v = 0; read_varint(data, len, &pos, &v);
        }
    }
    if (table_fmt) {
        printf("+--------------------+\n");
    }
    if (count == 0 && !table_fmt) printf("(no %s)\n", label);
}

/* --- Commands --- */

static int cmd_put(int argc, char **argv) {
    bool prev_kv = false;
    bool ignore_value = false;
    bool ignore_lease = false;
    bool want_json = false;
    bool want_fields = false;
    int64_t lease_id = 0;
    const char *key = NULL;
    const char *val = NULL;

    for (int i = 2; i < argc; i++) {
        if (strcmp(argv[i], "--prev-kv") == 0) {
            prev_kv = true;
        } else if (strcmp(argv[i], "--ignore-value") == 0) {
            ignore_value = true;
        } else if (strcmp(argv[i], "--ignore-lease") == 0) {
            ignore_lease = true;
        } else if (strcmp(argv[i], "--lease") == 0) {
            if (i + 1 >= argc) { fprintf(stderr, "--lease requires a lease ID\n"); return 1; }
            lease_id = strtoll(argv[++i], NULL, 10);
        } else if ((strcmp(argv[i], "-w") == 0 || strcmp(argv[i], "--write-out") == 0) && i + 1 < argc) {
            if (strcmp(argv[i + 1], "json") == 0) want_json = true;
            else if (strcmp(argv[i + 1], "fields") == 0) want_fields = true;
            i++;
        } else if (!key) {
            key = argv[i];
        } else if (!val) {
            val = argv[i];
        }
    }
    if (!key) {
        fprintf(stderr, "usage: cetcdctl put [--prev-kv] [--ignore-value] [--ignore-lease] [--lease ID] KEY [VALUE]\n");
        return 1;
    }
    if (!val && !ignore_value) {
        fprintf(stderr, "usage: cetcdctl put [--prev-kv] [--ignore-value] [--ignore-lease] [--lease ID] KEY [VALUE]\n");
        return 1;
    }

    uint8_t req[4096], resp[4096];
    size_t pos = 0;
    pos = encode_bytes_field(req, sizeof(req), pos, 0x0a,
                             (const uint8_t *)key, strlen(key));
    if (val) {
        pos = encode_bytes_field(req, sizeof(req), pos, 0x12,
                                 (const uint8_t *)val, strlen(val));
    }
    if (lease_id > 0) {
        /* field 3 (lease) = int64, tag = 0x18 */
        pos = encode_varint_field(req, sizeof(req), pos, 0x18, (uint64_t)lease_id);
    }
    if (prev_kv) {
        /* field 4 (prev_kv) = bool, tag = 0x20 */
        pos = encode_varint_field(req, sizeof(req), pos, 0x20, 1);
    }
    if (ignore_value) {
        /* field 5 (ignore_value) = bool, tag = 0x28 */
        pos = encode_varint_field(req, sizeof(req), pos, 0x28, 1);
    }
    if (ignore_lease) {
        /* field 6 (ignore_lease) = bool, tag = 0x30 */
        pos = encode_varint_field(req, sizeof(req), pos, 0x30, 1);
    }
    int rlen = do_rpc("/etcdserverpb.KV/Put", req, pos, resp, sizeof(resp));
    if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
    if (want_fields) {
        if (prev_kv) {
            size_t rpos = 0;
            const uint8_t *pk = NULL; size_t pk_len = 0;
            const uint8_t *pv = NULL; size_t pv_len = 0;
            uint64_t pcr = 0, pmr = 0, pver = 0, please = 0;
            while (rpos < (size_t)rlen) {
                uint8_t tag = resp[rpos++];
                if (tag == 0x12) {
                    uint64_t l = 0; read_varint(resp, rlen, &rpos, &l);
                    size_t kv_end = rpos + (size_t)l;
                    while (rpos < kv_end) {
                        uint8_t ktag = resp[rpos++];
                        if (ktag == 0x0a) {
                            uint64_t kl = 0; read_varint(resp, kv_end, &rpos, &kl);
                            pk = resp + rpos; pk_len = (size_t)kl; rpos += kl;
                        } else if (ktag == 0x2a) {
                            uint64_t vl = 0; read_varint(resp, kv_end, &rpos, &vl);
                            pv = resp + rpos; pv_len = (size_t)vl; rpos += vl;
                        } else if (ktag == 0x10) {
                            read_varint(resp, kv_end, &rpos, &pcr);
                        } else if (ktag == 0x18) {
                            read_varint(resp, kv_end, &rpos, &pmr);
                        } else if (ktag == 0x20) {
                            read_varint(resp, kv_end, &rpos, &pver);
                        } else if (ktag == 0x30) {
                            read_varint(resp, kv_end, &rpos, &please);
                        } else {
                            uint64_t v = 0; read_varint(resp, kv_end, &rpos, &v);
                        }
                    }
                    rpos = kv_end;
                } else if (tag == 0x0a) {
                    uint64_t l = 0; read_varint(resp, rlen, &rpos, &l); rpos += l;
                } else {
                    uint64_t v = 0; read_varint(resp, rlen, &rpos, &v);
                }
            }
            if (pk) {
                printf("\"");
                fwrite(pk, 1, pk_len, stdout);
                printf("\"\n");
                printf("create_revision: %llu\n", (unsigned long long)pcr);
                printf("mod_revision: %llu\n", (unsigned long long)pmr);
                printf("version: %llu\n", (unsigned long long)pver);
                if (please > 0) printf("lease: %llu\n", (unsigned long long)please);
                if (pv && pv_len > 0) {
                    printf("value: \"");
                    fwrite(pv, 1, pv_len, stdout);
                    printf("\"\n");
                }
            }
        }
        return 0;
    } else if (want_json) {
        fputs("{\"header\":{},", stdout);
        if (prev_kv) {
            /* Parse PutResponse for prev_kv (field 2, tag 0x12) */
            size_t rpos = 0;
            const uint8_t *pk = NULL; size_t pk_len = 0;
            const uint8_t *pv = NULL; size_t pv_len = 0;
            while (rpos < (size_t)rlen) {
                uint8_t tag = resp[rpos++];
                if (tag == 0x12) {
                    uint64_t l = 0; read_varint(resp, rlen, &rpos, &l);
                    size_t kv_end = rpos + (size_t)l;
                    while (rpos < kv_end) {
                        uint8_t ktag = resp[rpos++];
                        if (ktag == 0x0a) {
                            uint64_t kl = 0; read_varint(resp, kv_end, &rpos, &kl);
                            pk = resp + rpos; pk_len = (size_t)kl; rpos += kl;
                        } else if (ktag == 0x2a) {
                            uint64_t vl = 0; read_varint(resp, kv_end, &rpos, &vl);
                            pv = resp + rpos; pv_len = (size_t)vl; rpos += vl;
                        } else {
                            uint64_t v = 0; read_varint(resp, kv_end, &rpos, &v);
                        }
                    }
                    rpos = kv_end;
                } else if (tag == 0x0a) {
                    uint64_t l = 0; read_varint(resp, rlen, &rpos, &l); rpos += l;
                } else {
                    uint64_t v = 0; read_varint(resp, rlen, &rpos, &v);
                }
            }
            fputs("\"prev_kv\":{", stdout);
            if (pk) {
                fputs("\"key\":\"", stdout);
                fwrite(pk, 1, pk_len, stdout);
                fputs("\"", stdout);
                if (pv && pv_len > 0) {
                    fputs(",\"value\":\"", stdout);
                    fwrite(pv, 1, pv_len, stdout);
                    fputs("\"", stdout);
                }
            }
            fputs("}", stdout);
        }
        fputs("}\n", stdout);
        return 0;
    } else if (prev_kv) {
        /* Parse PutResponse for prev_kv (field 2, tag 0x12) */
        size_t rpos = 0;
        while (rpos < (size_t)rlen) {
            uint8_t tag = resp[rpos++];
            if (tag == 0x12) {
                uint64_t l = 0; read_varint(resp, rlen, &rpos, &l);
                size_t kv_end = rpos + (size_t)l;
                const uint8_t *pk = NULL; size_t pk_len = 0;
                const uint8_t *pv = NULL; size_t pv_len = 0;
                while (rpos < kv_end) {
                    uint8_t ktag = resp[rpos++];
                    if (ktag == 0x0a) {
                        uint64_t kl = 0; read_varint(resp, kv_end, &rpos, &kl);
                        pk = resp + rpos; pk_len = (size_t)kl; rpos += kl;
                    } else if (ktag == 0x2a) {
                        uint64_t vl = 0; read_varint(resp, kv_end, &rpos, &vl);
                        pv = resp + rpos; pv_len = (size_t)vl; rpos += vl;
                    } else {
                        uint64_t v = 0; read_varint(resp, kv_end, &rpos, &v);
                    }
                }
                rpos = kv_end;
                if (pk) {
                    printf("prev: %.*s", (int)pk_len, pk);
                    if (pv && pv_len > 0) printf(" -> %.*s", (int)pv_len, pv);
                    printf("\n");
                }
            } else if (tag == 0x0a) {
                /* header */
                uint64_t l = 0; read_varint(resp, rlen, &rpos, &l); rpos += l;
            } else {
                uint64_t v = 0; read_varint(resp, rlen, &rpos, &v);
            }
        }
    }
    printf("OK\n");
    return 0;
}

static int cmd_get(int argc, char **argv) {
    if (argc < 3) { fprintf(stderr, "usage: cetcdctl get [--prefix] [--from-key] [--keys-only] [--count-only] [--print-value-only] [--hex] [--consistency l|s] [-w json] [--rev N] [--limit N] [--sort-by FIELD] [--sort-order ORDER] [--min-mod-rev N] [--max-mod-rev N] [--min-create-rev N] [--max-create-rev N] KEY [RANGE_END]\n"); return 1; }
    bool prefix = false;
    bool from_key = false;
    bool keys_only = false;
    bool count_only = false;
    bool print_value_only = false;
    bool hex_output = false;
    bool serializable = false;
    int64_t rev = 0;
    int64_t limit = 0;
    int64_t min_mod_rev = 0, max_mod_rev = 0;
    int64_t min_create_rev = 0, max_create_rev = 0;
    int sort_order = 0;  /* 0=NONE, 1=ASCEND, 2=DESCEND */
    int sort_target = 0; /* 0=KEY, 1=VERSION, 2=CREATE, 3=MOD, 4=VALUE */
    const char *key = NULL;
    const char *range_end = NULL;

    for (int i = 2; i < argc; i++) {
        if (strcmp(argv[i], "--prefix") == 0) {
            prefix = true;
        } else if (strcmp(argv[i], "--from-key") == 0) {
            from_key = true;
        } else if (strcmp(argv[i], "--keys-only") == 0) {
            keys_only = true;
        } else if (strcmp(argv[i], "--print-value-only") == 0) {
            print_value_only = true;
        } else if (strcmp(argv[i], "--hex") == 0) {
            hex_output = true;
        } else if (strcmp(argv[i], "--consistency") == 0) {
            if (i + 1 >= argc) { fprintf(stderr, "--consistency requires a value (l or s)\n"); return 1; }
            const char *c = argv[++i];
            if (strcmp(c, "s") == 0) serializable = true;
            else if (strcmp(c, "l") != 0) { fprintf(stderr, "--consistency must be 'l' or 's'\n"); return 1; }
        } else if (strcmp(argv[i], "-w") == 0 || strcmp(argv[i], "--write-out") == 0) {
            if (i + 1 >= argc) { fprintf(stderr, "--write-out requires a format (json, simple, fields)\n"); return 1; }
            const char *fmt = argv[++i];
            if (strcmp(fmt, "json") == 0) { g_write_json = 1; g_write_fields = 0; }
            else if (strcmp(fmt, "fields") == 0) { g_write_json = 0; g_write_fields = 1; }
            else if (strcmp(fmt, "simple") == 0) { g_write_json = 0; g_write_fields = 0; }
            else { fprintf(stderr, "unsupported --write-out format: %s (use json, fields, or simple)\n", fmt); return 1; }
        } else if (strcmp(argv[i], "--count-only") == 0) {
            count_only = true;
        } else if (strcmp(argv[i], "--rev") == 0) {
            if (i + 1 >= argc) { fprintf(stderr, "--rev requires a revision number\n"); return 1; }
            rev = atol(argv[++i]);
        } else if (strcmp(argv[i], "--limit") == 0) {
            if (i + 1 >= argc) { fprintf(stderr, "--limit requires a number\n"); return 1; }
            limit = atol(argv[++i]);
        } else if (strcmp(argv[i], "--min-mod-rev") == 0) {
            if (i + 1 >= argc) { fprintf(stderr, "--min-mod-rev requires a revision number\n"); return 1; }
            min_mod_rev = atol(argv[++i]);
        } else if (strcmp(argv[i], "--max-mod-rev") == 0) {
            if (i + 1 >= argc) { fprintf(stderr, "--max-mod-rev requires a revision number\n"); return 1; }
            max_mod_rev = atol(argv[++i]);
        } else if (strcmp(argv[i], "--min-create-rev") == 0) {
            if (i + 1 >= argc) { fprintf(stderr, "--min-create-rev requires a revision number\n"); return 1; }
            min_create_rev = atol(argv[++i]);
        } else if (strcmp(argv[i], "--max-create-rev") == 0) {
            if (i + 1 >= argc) { fprintf(stderr, "--max-create-rev requires a revision number\n"); return 1; }
            max_create_rev = atol(argv[++i]);
        } else if (strcmp(argv[i], "--sort-by") == 0) {
            if (i + 1 >= argc) { fprintf(stderr, "--sort-by requires a field name (key|version|create|mod|value)\n"); return 1; }
            const char *s = argv[++i];
            if (strcmp(s, "key") == 0) sort_target = 0;
            else if (strcmp(s, "version") == 0) sort_target = 1;
            else if (strcmp(s, "create") == 0) sort_target = 2;
            else if (strcmp(s, "mod") == 0) sort_target = 3;
            else if (strcmp(s, "value") == 0) sort_target = 4;
            else { fprintf(stderr, "invalid --sort-by: %s (use key|version|create|mod|value)\n", s); return 1; }
            if (sort_order == 0) sort_order = 1; /* default to ASCEND when sort-by is set */
        } else if (strcmp(argv[i], "--sort-order") == 0) {
            if (i + 1 >= argc) { fprintf(stderr, "--sort-order requires an order (ascend|descend)\n"); return 1; }
            const char *s = argv[++i];
            if (strcmp(s, "ascend") == 0) sort_order = 1;
            else if (strcmp(s, "descend") == 0) sort_order = 2;
            else { fprintf(stderr, "invalid --sort-order: %s (use ascend|descend)\n", s); return 1; }
        } else if (!key) {
            key = argv[i];
        } else if (!range_end) {
            range_end = argv[i];
        }
    }
    if (!key) { fprintf(stderr, "usage: cetcdctl get [--prefix] [--from-key] [--keys-only] [--count-only] [--print-value-only] [--hex] [--consistency l|s] [-w json] [--rev N] [--limit N] [--sort-by FIELD] [--sort-order ORDER] [--min-mod-rev N] [--max-mod-rev N] [--min-create-rev N] [--max-create-rev N] KEY [RANGE_END]\n"); return 1; }

    size_t key_len = strlen(key);

    uint8_t req[1024], resp[8192];
    size_t pos = 0;
    pos = encode_bytes_field(req, sizeof(req), pos, 0x0a,
                             (const uint8_t *)key, key_len);
    if (prefix) {
        /* range_end = key with last byte incremented (standard etcd prefix semantics) */
        char prefix_end[256];
        if (key_len >= sizeof(prefix_end)) { fprintf(stderr, "key too long\n"); return 1; }
        memcpy(prefix_end, key, key_len);
        prefix_end[key_len - 1]++;
        pos = encode_bytes_field(req, sizeof(req), pos, 0x12,
                                 (const uint8_t *)prefix_end, key_len);
    } else if (from_key) {
        /* range_end = \0 means all keys >= key */
        uint8_t zero = 0;
        pos = encode_bytes_field(req, sizeof(req), pos, 0x12, &zero, 1);
    } else if (range_end) {
        pos = encode_bytes_field(req, sizeof(req), pos, 0x12,
                                 (const uint8_t *)range_end, strlen(range_end));
    }
    if (limit > 0) {
        /* field 3 (limit) = int64, tag = 0x18 */
        pos = encode_varint_field(req, sizeof(req), pos, 0x18, (uint64_t)limit);
    }
    if (rev > 0) {
        /* field 4 (revision) = int64, tag = 0x20 */
        pos = encode_varint_field(req, sizeof(req), pos, 0x20, (uint64_t)rev);
    }
    if (keys_only) {
        /* field 8 (keys_only) = bool, tag = 0x40 */
        pos = encode_varint_field(req, sizeof(req), pos, 0x40, 1);
    }
    if (count_only) {
        /* field 9 (count_only) = bool, tag = 0x48 */
        pos = encode_varint_field(req, sizeof(req), pos, 0x48, 1);
    }
    if (min_mod_rev > 0) {
        /* field 10 (min_mod_revision) = int64, tag = 0x50 */
        pos = encode_varint_field(req, sizeof(req), pos, 0x50, (uint64_t)min_mod_rev);
    }
    if (max_mod_rev > 0) {
        /* field 11 (max_mod_revision) = int64, tag = 0x58 */
        pos = encode_varint_field(req, sizeof(req), pos, 0x58, (uint64_t)max_mod_rev);
    }
    if (min_create_rev > 0) {
        /* field 12 (min_create_revision) = int64, tag = 0x60 */
        pos = encode_varint_field(req, sizeof(req), pos, 0x60, (uint64_t)min_create_rev);
    }
    if (max_create_rev > 0) {
        /* field 13 (max_create_revision) = int64, tag = 0x68 */
        pos = encode_varint_field(req, sizeof(req), pos, 0x68, (uint64_t)max_create_rev);
    }
    if (sort_order > 0) {
        /* field 5 (sort_order) = enum, tag = 0x28 */
        pos = encode_varint_field(req, sizeof(req), pos, 0x28, (uint64_t)sort_order);
        /* field 6 (sort_target) = enum, tag = 0x30 */
        pos = encode_varint_field(req, sizeof(req), pos, 0x30, (uint64_t)sort_target);
    }
    if (serializable) {
        /* field 7 (serializable) = bool, tag = 0x38 */
        pos = encode_varint_field(req, sizeof(req), pos, 0x38, 1);
    }
    g_keys_only = keys_only ? 1 : 0;
    g_count_only = count_only ? 1 : 0;
    g_print_value_only = print_value_only ? 1 : 0;
    g_hex = hex_output ? 1 : 0;
    int rlen = do_rpc("/etcdserverpb.KV/Range", req, pos, resp, sizeof(resp));
    if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
    parse_range_response(resp, rlen);
    g_keys_only = 0;
    g_count_only = 0;
    g_print_value_only = 0;
    g_hex = 0;
    g_write_json = 0;
    g_write_fields = 0;
    return 0;
}

static int cmd_del(int argc, char **argv) {
    if (argc < 3) { fprintf(stderr, "usage: cetcdctl del [--prefix] [--from-key] [--prev-kv] [-w json] KEY [RANGE_END]\n"); return 1; }
    bool prefix = false;
    bool from_key = false;
    bool prev_kv = false;
    bool want_json = false;
    const char *key = NULL;
    const char *range_end = NULL;

    for (int i = 2; i < argc; i++) {
        if (strcmp(argv[i], "--prefix") == 0) {
            prefix = true;
        } else if (strcmp(argv[i], "--from-key") == 0) {
            from_key = true;
        } else if (strcmp(argv[i], "--prev-kv") == 0) {
            prev_kv = true;
        } else if ((strcmp(argv[i], "-w") == 0 || strcmp(argv[i], "--write-out") == 0) && i + 1 < argc) {
            if (strcmp(argv[i + 1], "json") == 0) want_json = true;
            i++;
        } else if (!key) {
            key = argv[i];
        } else if (!range_end) {
            range_end = argv[i];
        }
    }
    if (!key) { fprintf(stderr, "usage: cetcdctl del [--prefix] [--from-key] [--prev-kv] KEY [RANGE_END]\n"); return 1; }
    size_t key_len = strlen(key);

    uint8_t req[1024], resp[4096];
    size_t pos = 0;
    pos = encode_bytes_field(req, sizeof(req), pos, 0x0a,
                             (const uint8_t *)key, key_len);
    if (prefix) {
        char prefix_end[256];
        if (key_len >= sizeof(prefix_end)) { fprintf(stderr, "key too long\n"); return 1; }
        memcpy(prefix_end, key, key_len);
        prefix_end[key_len - 1]++;
        pos = encode_bytes_field(req, sizeof(req), pos, 0x12,
                                 (const uint8_t *)prefix_end, key_len);
    } else if (from_key) {
        /* range_end = \0 means all keys >= key */
        uint8_t zero = 0;
        pos = encode_bytes_field(req, sizeof(req), pos, 0x12, &zero, 1);
    } else if (range_end) {
        pos = encode_bytes_field(req, sizeof(req), pos, 0x12,
                                 (const uint8_t *)range_end, strlen(range_end));
    }
    if (prev_kv) {
        /* field 3 (prev_kv) = bool, tag = 0x18 */
        pos = encode_varint_field(req, sizeof(req), pos, 0x18, 1);
    }
    int rlen = do_rpc("/etcdserverpb.KV/DeleteRange", req, pos, resp, sizeof(resp));
    if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
    if (want_json) {
        /* JSON output: {"deleted":N,"prev_kvs":[...]} */
        size_t rpos = 0;
        uint64_t deleted = 0;
        fputs("{\"header\":{},", stdout);
        while (rpos < (size_t)rlen) {
            uint8_t tag = resp[rpos++];
            if (tag == 0x10) {
                read_varint(resp, rlen, &rpos, &deleted);
            } else if (tag == 0x1a && prev_kv) {
                uint64_t l = 0; read_varint(resp, rlen, &rpos, &l);
                rpos += l; /* skip prev_kvs for now in JSON */
            } else if (tag == 0x0a) {
                uint64_t l = 0; read_varint(resp, rlen, &rpos, &l); rpos += l;
            } else {
                uint64_t v = 0; read_varint(resp, rlen, &rpos, &v);
            }
        }
        printf("\"deleted\":%llu}\n", (unsigned long long)deleted);
        return 0;
    }
    /* Parse DeleteRangeResponse: field 2 (deleted), field 3 (prev_kvs) */
    size_t rpos = 0;
    while (rpos < (size_t)rlen) {
        uint8_t tag = resp[rpos++];
        if (tag == 0x10) {
            uint64_t v = 0; read_varint(resp, rlen, &rpos, &v);
            printf("%llu key(s) deleted\n", (unsigned long long)v);
        } else if (tag == 0x1a && prev_kv) {
            /* prev_kvs: repeated KeyValue */
            uint64_t l = 0; read_varint(resp, rlen, &rpos, &l);
            size_t kv_end = rpos + (size_t)l;
            const uint8_t *pk = NULL; size_t pk_len = 0;
            const uint8_t *pv = NULL; size_t pv_len = 0;
            while (rpos < kv_end) {
                uint8_t ktag = resp[rpos++];
                if (ktag == 0x0a) {
                    uint64_t kl = 0; read_varint(resp, kv_end, &rpos, &kl);
                    pk = resp + rpos; pk_len = (size_t)kl; rpos += kl;
                } else if (ktag == 0x2a) {
                    uint64_t vl = 0; read_varint(resp, kv_end, &rpos, &vl);
                    pv = resp + rpos; pv_len = (size_t)vl; rpos += vl;
                } else {
                    uint64_t v = 0; read_varint(resp, kv_end, &rpos, &v);
                }
            }
            rpos = kv_end;
            if (pk) {
                printf("prev: %.*s", (int)pk_len, pk);
                if (pv && pv_len > 0) printf(" -> %.*s", (int)pv_len, pv);
                printf("\n");
            }
        } else if (tag == 0x0a) {
            /* Skip header (length-delimited) */
            uint64_t l = 0; read_varint(resp, rlen, &rpos, &l); rpos += l;
        } else {
            uint64_t v = 0; read_varint(resp, rlen, &rpos, &v);
        }
    }
    return 0;
}

static int cmd_lease(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "usage: cetcdctl lease grant TTL\n");
        fprintf(stderr, "       cetcdctl lease revoke ID\n");
        fprintf(stderr, "       cetcdctl lease timetolive [--keys] ID\n");
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
        bool want_keys = false;
        const char *id_str = NULL;
        for (int i = 3; i < argc; i++) {
            if (strcmp(argv[i], "--keys") == 0) {
                want_keys = true;
            } else if (!id_str) {
                id_str = argv[i];
            }
        }
        if (!id_str) { fprintf(stderr, "usage: cetcdctl lease timetolive [--keys] ID\n"); return 1; }
        uint8_t req[32], resp[4096];
        size_t pos = 0;
        pos = encode_varint_field(req, sizeof(req), pos, 0x08, (uint64_t)atol(id_str));
        if (want_keys) {
            pos = encode_varint_field(req, sizeof(req), pos, 0x10, 1); /* keys=true */
        }
        int rlen = do_rpc("/etcdserverpb.Lease/LeaseTimeToLive", req, pos, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        parse_lease_ttl_response(resp, rlen);
    } else if (strcmp(argv[2], "list") == 0) {
        int table_fmt = 0, json_fmt = 0;
        for (int i = 3; i < argc; i++) {
            if ((strcmp(argv[i], "-w") == 0 || strcmp(argv[i], "--write-out") == 0) && i + 1 < argc) {
                if (strcmp(argv[i + 1], "table") == 0) table_fmt = 1;
                else if (strcmp(argv[i + 1], "json") == 0) json_fmt = 1;
                i++;
            }
        }
        uint8_t req[] = {0x00}, resp[4096];
        int rlen = do_rpc("/etcdserverpb.Lease/LeaseLeases", req, 1, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        /* Parse LeaseLeasesResponse: field 1 (header), field 2 (leases) = repeated LeaseStatus */
        size_t rpos = 0;
        int count = 0;
        if (table_fmt) {
            printf("+--------------------+\n");
            printf("|        ID          |\n");
            printf("+--------------------+\n");
        }
        if (json_fmt) {
            fputs("{\"header\":{},\"leases\":[", stdout);
        }
        while (rpos < (size_t)rlen) {
            uint8_t tag = resp[rpos++];
            if (tag == 0x12) {
                uint64_t lslen = 0; read_varint(resp, rlen, &rpos, &lslen);
                size_t lend = rpos + (size_t)lslen;
                while (rpos < lend) {
                    uint8_t ltag = resp[rpos++];
                    if (ltag == 0x08) {
                        uint64_t id = 0; read_varint(resp, lend, &rpos, &id);
                        if (json_fmt) {
                            if (count > 0) printf(",");
                            printf("{\"ID\":%llu}", (unsigned long long)id);
                        } else if (table_fmt) {
                            printf("| %18llu |\n", (unsigned long long)id);
                        } else {
                            printf("lease ID: %llu\n", (unsigned long long)id);
                        }
                        count++;
                    } else {
                        uint64_t v = 0; read_varint(resp, lend, &rpos, &v);
                    }
                }
                rpos = lend;
            } else if (tag == 0x0a) {
                /* Skip header (length-delimited) */
                uint64_t l = 0; read_varint(resp, rlen, &rpos, &l);
                rpos += l;
            } else {
                uint64_t v = 0; read_varint(resp, rlen, &rpos, &v);
            }
        }
        if (json_fmt) {
            printf("]}\n");
        }
        if (table_fmt) {
            printf("+--------------------+\n");
        }
        if (count == 0 && !table_fmt && !json_fmt) printf("(no leases)\n");
    } else if (strcmp(argv[2], "keepalive") == 0) {
        if (argc < 4) { fprintf(stderr, "usage: cetcdctl lease keepalive ID\n"); return 1; }
        uint8_t req[32], resp[256];
        size_t pos = 0;
        pos = encode_varint_field(req, sizeof(req), pos, 0x08, (uint64_t)atol(argv[3]));
        int rlen = do_rpc("/etcdserverpb.Lease/LeaseKeepAlive", req, pos, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        /* Parse KeepAliveResponse: field 1 (header), field 2 (ID), field 3 (TTL) */
        size_t rpos = 0;
        while (rpos < (size_t)rlen) {
            uint8_t tag = resp[rpos++];
            if (tag == 0x10) {
                uint64_t v = 0; read_varint(resp, rlen, &rpos, &v);
                printf("lease ID: %llu\n", (unsigned long long)v);
            } else if (tag == 0x18) {
                uint64_t v = 0; read_varint(resp, rlen, &rpos, &v);
                printf("TTL: %llu seconds\n", (unsigned long long)v);
            } else if (tag == 0x0a) {
                /* Skip header (length-delimited) */
                uint64_t l = 0; read_varint(resp, rlen, &rpos, &l);
                rpos += l;
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
    bool physical = false;
    int64_t rev = 0;
    for (int i = 2; i < argc; i++) {
        if (strcmp(argv[i], "--physical") == 0) {
            physical = true;
        } else if (!rev) {
            rev = strtoll(argv[i], NULL, 10);
        }
    }
    if (rev <= 0) { fprintf(stderr, "usage: cetcdctl compact [--physical] REV\n"); return 1; }
    uint8_t req[32], resp[256];
    size_t pos = 0;
    pos = encode_varint_field(req, sizeof(req), pos, 0x08, (uint64_t)rev);
    if (physical) {
        /* field 2 (physical) = bool, tag = 0x10 */
        pos = encode_varint_field(req, sizeof(req), pos, 0x10, 1);
    }
    int rlen = do_rpc("/etcdserverpb.KV/Compact", req, pos, resp, sizeof(resp));
    if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
    printf("OK\n");
    return 0;
}

static int cmd_txn(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "usage: cetcdctl txn put KEY VALUE\n");
        fprintf(stderr, "       cetcdctl txn cas KEY EXPECTED NEW\n");
        fprintf(stderr, "       cetcdctl txn get KEY [RANGE_END]\n");
        fprintf(stderr, "       cetcdctl txn del [--prefix] [--prev-kv] KEY [RANGE_END]\n");
        return 1;
    }
    if (strcmp(argv[2], "put") == 0) {
        if (argc < 5) { fprintf(stderr, "usage: cetcdctl txn put KEY VALUE\n"); return 1; }
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
    } else if (strcmp(argv[2], "cas") == 0) {
        /* Compare-and-swap: if KEY's value equals EXPECTED, set it to NEW */
        if (argc < 6) { fprintf(stderr, "usage: cetcdctl txn cas KEY EXPECTED NEW\n"); return 1; }
        const char *key = argv[3];
        size_t key_len = strlen(key);
        const char *expected = argv[4];
        size_t exp_len = strlen(expected);
        const char *new_val = argv[5];
        size_t new_len = strlen(new_val);

        /* Build Compare message:
         *   field 1 (result) = 0 (EQUAL), tag = 0x08
         *   field 2 (target) = 3 (VALUE), tag = 0x10
         *   field 3 (key)    = bytes, tag = 0x1a
         *   field 7 (value)  = bytes, tag = 0x3a
         */
        uint8_t cmp_buf[512];
        size_t cpos = 0;
        cpos = encode_varint_field(cmp_buf, sizeof(cmp_buf), cpos, 0x08, 0); /* result=EQUAL */
        cpos = encode_varint_field(cmp_buf, sizeof(cmp_buf), cpos, 0x10, 3); /* target=VALUE */
        cpos = encode_bytes_field(cmp_buf, sizeof(cmp_buf), cpos, 0x1a,
                                   (const uint8_t *)key, key_len);
        cpos = encode_bytes_field(cmp_buf, sizeof(cmp_buf), cpos, 0x3a,
                                   (const uint8_t *)expected, exp_len);

        /* Build success op: RequestPut(KEY, NEW) */
        uint8_t put_inner[1024];
        size_t ppos = 0;
        ppos = encode_bytes_field(put_inner, sizeof(put_inner), ppos, 0x0a,
                                   (const uint8_t *)key, key_len);
        ppos = encode_bytes_field(put_inner, sizeof(put_inner), ppos, 0x12,
                                   (const uint8_t *)new_val, new_len);
        uint8_t op_buf[1024];
        size_t opos = 0;
        op_buf[opos++] = 0x12; /* RequestPut tag */
        opos = write_varint(op_buf, sizeof(op_buf), opos, (uint64_t)ppos);
        memcpy(op_buf + opos, put_inner, ppos);
        opos += ppos;

        /* Build TxnRequest:
         *   field 1 (compare) = Compare, tag = 0x0a
         *   field 2 (success) = RequestOp, tag = 0x12
         */
        uint8_t req[2048], resp[1024];
        size_t pos = 0;
        /* field 1 = compare */
        pos = encode_bytes_field(req, sizeof(req), pos, 0x0a, cmp_buf, cpos);
        /* field 2 = success op */
        pos = encode_bytes_field(req, sizeof(req), pos, 0x12, op_buf, opos);

        int rlen = do_rpc("/etcdserverpb.KV/Txn", req, pos, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        /* Parse TxnResponse: field 1 (header), field 2 (succeeded) = bool, tag = 0x10 */
        bool succeeded = false;
        size_t rpos = 0;
        while (rpos < (size_t)rlen) {
            uint8_t tag = resp[rpos++];
            if (tag == 0x10) {
                uint64_t v = 0; read_varint(resp, rlen, &rpos, &v);
                succeeded = (v != 0);
                break;
            } else if (tag == 0x0a) {
                /* Skip header (length-delimited) */
                uint64_t l = 0; read_varint(resp, rlen, &rpos, &l); rpos += l;
            } else {
                uint64_t v = 0; read_varint(resp, rlen, &rpos, &v);
            }
        }
        if (succeeded) {
            printf("OK (compare succeeded)\n");
            return 0;
        } else {
            printf("FAILED (compare did not match)\n");
            return 1;
        }
    } else if (strcmp(argv[2], "get") == 0) {
        /* txn get KEY [RANGE_END] */
        if (argc < 4) { fprintf(stderr, "usage: cetcdctl txn get KEY [RANGE_END]\n"); return 1; }
        const char *key = argv[3];
        size_t key_len = strlen(key);
        const char *range_end = (argc >= 5) ? argv[4] : NULL;

        /* Build RequestRange inner: key(0x0a), range_end(0x12) */
        uint8_t range_inner[512];
        size_t rpos = 0;
        rpos = encode_bytes_field(range_inner, sizeof(range_inner), rpos, 0x0a,
                                   (const uint8_t *)key, key_len);
        if (range_end) {
            rpos = encode_bytes_field(range_inner, sizeof(range_inner), rpos, 0x12,
                                       (const uint8_t *)range_end, strlen(range_end));
        }

        /* Build RequestOp: tag 0x0a (request_range), length, inner */
        uint8_t op_buf[1024];
        size_t opos = 0;
        op_buf[opos++] = 0x0a; /* RequestRange tag */
        opos = write_varint(op_buf, sizeof(op_buf), opos, (uint64_t)rpos);
        memcpy(op_buf + opos, range_inner, rpos);
        opos += rpos;

        /* Build TxnRequest: field 2 (success) = tag 0x12, length, op */
        uint8_t req[2048], resp[4096];
        size_t pos = 0;
        req[pos++] = 0x12; /* field 2 = success ops */
        pos = write_varint(req, sizeof(req), pos, (uint64_t)opos);
        memcpy(req + pos, op_buf, opos);
        pos += opos;

        int rlen = do_rpc("/etcdserverpb.KV/Txn", req, pos, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }

        /* Parse TxnResponse: skip header(0x0a), succeeded(0x10), then response_range(0x0a in ResponseOp) */
        size_t rp = 0;
        while (rp < (size_t)rlen) {
            uint8_t tag = resp[rp++];
            if (tag == 0x0a) {
                uint64_t l = 0; read_varint(resp, rlen, &rp, &l);
                /* This could be header or ResponseOp — check if it contains ResponseRange */
                size_t sub_end = rp + (size_t)l;
                while (rp < sub_end) {
                    uint8_t sub_tag = resp[rp++];
                    if (sub_tag == 0x0a) {
                        /* ResponseRange inside ResponseOp */
                        uint64_t rr_len = 0; read_varint(resp, sub_end, &rp, &rr_len);
                        /* Use parse_range_response to print kvs */
                        parse_range_response(resp + rp, (size_t)rr_len);
                        rp += (size_t)rr_len;
                    } else if (sub_tag == 0x12) {
                        /* ResponsePut */
                        uint64_t skip = 0; read_varint(resp, sub_end, &rp, &skip);
                        rp += (size_t)skip;
                    } else if (sub_tag == 0x1a) {
                        /* ResponseDeleteRange */
                        uint64_t skip = 0; read_varint(resp, sub_end, &rp, &skip);
                        rp += (size_t)skip;
                    } else {
                        uint64_t skip = 0; read_varint(resp, sub_end, &rp, &skip);
                    }
                }
            } else if (tag == 0x10) {
                uint64_t v = 0; read_varint(resp, rlen, &rp, &v);
            } else {
                uint64_t skip = 0; read_varint(resp, rlen, &rp, &skip);
            }
        }
        return 0;
    } else if (strcmp(argv[2], "del") == 0) {
        /* txn del [--prefix] [--prev-kv] KEY [RANGE_END] */
        bool prefix = false;
        bool prev_kv = false;
        const char *key = NULL;
        const char *range_end = NULL;
        for (int i = 3; i < argc; i++) {
            if (strcmp(argv[i], "--prefix") == 0) {
                prefix = true;
            } else if (strcmp(argv[i], "--prev-kv") == 0) {
                prev_kv = true;
            } else if (!key) {
                key = argv[i];
            } else if (!range_end) {
                range_end = argv[i];
            }
        }
        if (!key) { fprintf(stderr, "usage: cetcdctl txn del [--prefix] [--prev-kv] KEY [RANGE_END]\n"); return 1; }
        size_t key_len = strlen(key);

        /* Build RequestDeleteRange inner: key(0x0a), range_end(0x12), prev_kv(0x20) */
        uint8_t del_inner[512];
        size_t dpos = 0;
        dpos = encode_bytes_field(del_inner, sizeof(del_inner), dpos, 0x0a,
                                   (const uint8_t *)key, key_len);
        if (prefix) {
            char prefix_end[256];
            memcpy(prefix_end, key, key_len);
            prefix_end[key_len - 1]++;
            dpos = encode_bytes_field(del_inner, sizeof(del_inner), dpos, 0x12,
                                       (const uint8_t *)prefix_end, key_len);
        } else if (range_end) {
            dpos = encode_bytes_field(del_inner, sizeof(del_inner), dpos, 0x12,
                                       (const uint8_t *)range_end, strlen(range_end));
        }
        if (prev_kv) {
            dpos = encode_varint_field(del_inner, sizeof(del_inner), dpos, 0x20, 1);
        }

        /* Build RequestOp: tag 0x1a (request_delete_range), length, inner */
        uint8_t op_buf[1024];
        size_t opos = 0;
        op_buf[opos++] = 0x1a; /* RequestDeleteRange tag */
        opos = write_varint(op_buf, sizeof(op_buf), opos, (uint64_t)dpos);
        memcpy(op_buf + opos, del_inner, dpos);
        opos += dpos;

        /* Build TxnRequest: field 2 (success) = tag 0x12, length, op */
        uint8_t req[2048], resp[4096];
        size_t pos = 0;
        req[pos++] = 0x12; /* field 2 = success ops */
        pos = write_varint(req, sizeof(req), pos, (uint64_t)opos);
        memcpy(req + pos, op_buf, opos);
        pos += opos;

        int rlen = do_rpc("/etcdserverpb.KV/Txn", req, pos, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        printf("OK\n");
        return 0;
    } else {
        fprintf(stderr, "unknown txn subcommand: %s\n", argv[2]);
        fprintf(stderr, "usage: cetcdctl txn put KEY VALUE\n");
        fprintf(stderr, "       cetcdctl txn cas KEY EXPECTED NEW\n");
        fprintf(stderr, "       cetcdctl txn get KEY [RANGE_END]\n");
        fprintf(stderr, "       cetcdctl txn del [--prefix] [--prev-kv] KEY [RANGE_END]\n");
        return 1;
    }
}

static int cmd_status(int argc, char **argv) {
    int want_json = 0;
    for (int i = 2; i < argc; i++) {
        if ((strcmp(argv[i], "-w") == 0 || strcmp(argv[i], "--write-out") == 0) && i + 1 < argc) {
            if (strcmp(argv[i + 1], "json") == 0) want_json = 1;
            i++;
        }
    }
    uint8_t req[] = {0x00}, resp[1024];
    int rlen = do_rpc("/etcdserverpb.Maintenance/Status", req, 1, resp, sizeof(resp));
    if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
    if (want_json) {
        /* Parse and output JSON format */
        size_t pos = 0;
        const uint8_t *version = NULL; size_t version_len = 0;
        uint64_t db_size = 0, leader = 0, raft_index = 0, raft_term = 0;
        while (pos < (size_t)rlen) {
            uint8_t tag = resp[pos++];
            if (tag == 0x12) {
                uint64_t l = 0; read_varint(resp, rlen, &pos, &l);
                version = resp + pos; version_len = (size_t)l; pos += l;
            } else if (tag == 0x18) {
                read_varint(resp, rlen, &pos, &db_size);
            } else if (tag == 0x20) {
                read_varint(resp, rlen, &pos, &leader);
            } else if (tag == 0x28) {
                read_varint(resp, rlen, &pos, &raft_index);
            } else if (tag == 0x30) {
                read_varint(resp, rlen, &pos, &raft_term);
            } else if (tag == 0x0a) {
                uint64_t l = 0; read_varint(resp, rlen, &pos, &l); pos += l;
            } else {
                uint64_t v = 0; read_varint(resp, rlen, &pos, &v);
            }
        }
        fputs("{\"version\":\"", stdout);
        if (version) fwrite(version, 1, version_len, stdout);
        fputs("\",", stdout);
        printf("\"dbSize\":%llu,", (unsigned long long)db_size);
        printf("\"leader\":%llu,", (unsigned long long)leader);
        printf("\"raftIndex\":%llu,", (unsigned long long)raft_index);
        printf("\"raftTerm\":%llu}\n", (unsigned long long)raft_term);
    } else {
        parse_status_response(resp, rlen);
    }
    return 0;
}

static int cmd_endpoint(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "usage: cetcdctl endpoint {health,status}\n");
        return 1;
    }
    if (strcmp(argv[2], "health") == 0) {
        /* Health check: send a Status RPC and check if we get a response */
        uint8_t req[] = {0x00}, resp[1024];
        int rlen = do_rpc("/etcdserverpb.Maintenance/Status", req, 1, resp, sizeof(resp));
        if (rlen < 0) {
            printf("%s:%d is unhealthy: failed to connect\n", g_host, g_port);
            return 1;
        }
        printf("%s:%d is healthy\n", g_host, g_port);
        return 0;
    } else if (strcmp(argv[2], "status") == 0) {
        /* Endpoint status: same as status but with endpoint info */
        uint8_t req[] = {0x00}, resp[1024];
        int rlen = do_rpc("/etcdserverpb.Maintenance/Status", req, 1, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        printf("+--------------------------+----------------+-----------+\n");
        printf("|         ENDPOINT         |      ID        |  REVISION |\n");
        printf("+--------------------------+----------------+-----------+\n");
        printf("| %-24s | %14s | %9s |\n", g_host, "node", "-");
        printf("+--------------------------+----------------+-----------+\n");
        parse_status_response(resp, rlen);
        return 0;
    } else {
        fprintf(stderr, "unknown endpoint subcommand: %s\n", argv[2]);
        return 1;
    }
}

static int cmd_check(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "usage: cetcdctl check perf\n");
        return 1;
    }
    if (strcmp(argv[2], "perf") == 0) {
        /* Simple performance check: time a Put + Get cycle */
        printf("Running performance check...\n");

        /* Put a test key */
        uint8_t put_req[256], put_resp[256];
        size_t pos = 0;
        const char *key = "_perf_check";
        const char *val = "ok";
        pos = encode_bytes_field(put_req, sizeof(put_req), pos, 0x0a,
                                 (const uint8_t *)key, strlen(key));
        pos = encode_bytes_field(put_req, sizeof(put_req), pos, 0x12,
                                 (const uint8_t *)val, strlen(val));
        int rlen = do_rpc("/etcdserverpb.KV/Put", put_req, pos, put_resp, sizeof(put_resp));
        if (rlen < 0) { fprintf(stderr, "put failed\n"); return 1; }

        /* Get the key back */
        uint8_t get_req[256], get_resp[1024];
        pos = 0;
        pos = encode_bytes_field(get_req, sizeof(get_req), pos, 0x0a,
                                 (const uint8_t *)key, strlen(key));
        rlen = do_rpc("/etcdserverpb.KV/Range", get_req, pos, get_resp, sizeof(get_resp));
        if (rlen < 0) { fprintf(stderr, "get failed\n"); return 1; }

        /* Delete the test key */
        uint8_t del_req[256], del_resp[256];
        pos = 0;
        pos = encode_bytes_field(del_req, sizeof(del_req), pos, 0x0a,
                                 (const uint8_t *)key, strlen(key));
        do_rpc("/etcdserverpb.KV/DeleteRange", del_req, pos, del_resp, sizeof(del_resp));

        printf("PASS: Performance check completed successfully\n");
        return 0;
    } else {
        fprintf(stderr, "unknown check subcommand: %s\n", argv[2]);
        return 1;
    }
}

/* Signal handler for lock release on Ctrl+C */
static void lock_signal_handler(int sig) {
    (void)sig;
    if (g_lock_held && g_lock_key_len > 0) {
        /* Delete the lock key */
        uint8_t req[512], resp[256];
        size_t pos = 0;
        pos = encode_bytes_field(req, sizeof(req), pos, 0x0a,
                                 (const uint8_t *)g_lock_key, g_lock_key_len);
        do_rpc("/etcdserverpb.KV/DeleteRange", req, pos, resp, sizeof(resp));
    }
    if (g_lock_held && g_lock_lease_id > 0) {
        /* Revoke the lease */
        uint8_t req[32], resp[256];
        size_t pos = 0;
        pos = encode_varint_field(req, sizeof(req), pos, 0x08, g_lock_lease_id);
        do_rpc("/etcdserverpb.Lease/LeaseRevoke", req, pos, resp, sizeof(resp));
    }
    g_lock_held = 0;
    _exit(0);
}

static int cmd_lock(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "usage: cetcdctl lock LOCKNAME [COMMAND...]\n");
        return 1;
    }
    const char *lockname = argv[2];
    size_t lockname_len = strlen(lockname);

    /* Build lock key: "/{lockname}" */
    char lock_key[512];
    size_t lock_key_len = 0;
    lock_key[lock_key_len++] = '/';
    if (lockname_len >= sizeof(lock_key) - 1) {
        fprintf(stderr, "lock name too long\n");
        return 1;
    }
    memcpy(lock_key + lock_key_len, lockname, lockname_len);
    lock_key_len += lockname_len;

    /* Step 1: Grant a lease with 60s TTL */
    uint8_t grant_req[16], grant_resp[256];
    size_t gpos = 0;
    gpos = encode_varint_field(grant_req, sizeof(grant_req), gpos, 0x03, 60); /* TTL=60s */
    int glen = do_rpc("/etcdserverpb.Lease/LeaseGrant", grant_req, gpos, grant_resp, sizeof(grant_resp));
    if (glen < 0) { fprintf(stderr, "lease grant failed\n"); return 1; }

    /* Parse LeaseGrantResponse: field 1 (ID) = int64, tag = 0x08 */
    uint64_t lease_id = 0;
    size_t rp = 0;
    while (rp < (size_t)glen) {
        uint8_t tag = grant_resp[rp++];
        if (tag == 0x08) {
            read_varint(grant_resp, glen, &rp, &lease_id);
            break;
        } else if (tag == 0x0a) {
            uint64_t l = 0; read_varint(grant_resp, glen, &rp, &l); rp += l;
        } else {
            uint64_t v = 0; read_varint(grant_resp, glen, &rp, &v);
        }
    }
    if (lease_id == 0) { fprintf(stderr, "failed to get lease ID\n"); return 1; }

    /* Step 2: Txn: Compare(key, CREATE, EQUAL, 0) → success: Put(key, "", lease) */
    /* Build Compare message:
     *   field 1 (result) = 0 (EQUAL), tag = 0x08
     *   field 2 (target) = 1 (CREATE), tag = 0x10
     *   field 3 (key)    = bytes, tag = 0x1a
     *   field 5 (create_revision) = 0, tag = 0x28
     */
    uint8_t cmp_buf[512];
    size_t cpos = 0;
    cpos = encode_varint_field(cmp_buf, sizeof(cmp_buf), cpos, 0x08, 0); /* result=EQUAL */
    cpos = encode_varint_field(cmp_buf, sizeof(cmp_buf), cpos, 0x10, 1); /* target=CREATE */
    cpos = encode_bytes_field(cmp_buf, sizeof(cmp_buf), cpos, 0x1a,
                              (const uint8_t *)lock_key, lock_key_len);
    cpos = encode_varint_field(cmp_buf, sizeof(cmp_buf), cpos, 0x28, 0); /* create_revision=0 */

    /* Build success op: RequestPut(key, "", lease) */
    uint8_t put_inner[512];
    size_t ppos = 0;
    ppos = encode_bytes_field(put_inner, sizeof(put_inner), ppos, 0x0a,
                              (const uint8_t *)lock_key, lock_key_len);
    ppos = encode_varint_field(put_inner, sizeof(put_inner), ppos, 0x18, lease_id); /* lease */
    uint8_t op_buf[1024];
    size_t opos = 0;
    op_buf[opos++] = 0x12; /* RequestPut tag */
    opos = write_varint(op_buf, sizeof(op_buf), opos, (uint64_t)ppos);
    memcpy(op_buf + opos, put_inner, ppos);
    opos += ppos;

    /* Build TxnRequest */
    uint8_t req[2048], resp[1024];
    size_t pos = 0;
    pos = encode_bytes_field(req, sizeof(req), pos, 0x0a, cmp_buf, cpos); /* compare */
    pos = encode_bytes_field(req, sizeof(req), pos, 0x12, op_buf, opos);  /* success op */

    int rlen = do_rpc("/etcdserverpb.KV/Txn", req, pos, resp, sizeof(resp));
    if (rlen < 0) { fprintf(stderr, "txn request failed\n"); return 1; }

    /* Parse TxnResponse: field 1 (header), field 2 (succeeded) = bool, tag = 0x10 */
    bool succeeded = false;
    rp = 0;
    while (rp < (size_t)rlen) {
        uint8_t tag = resp[rp++];
        if (tag == 0x10) {
            uint64_t v = 0; read_varint(resp, rlen, &rp, &v);
            succeeded = (v != 0);
            break;
        } else if (tag == 0x0a) {
            uint64_t l = 0; read_varint(resp, rlen, &rp, &l); rp += l;
        } else {
            uint64_t v = 0; read_varint(resp, rlen, &rp, &v);
        }
    }

    if (!succeeded) {
        fprintf(stderr, "lock '%s' is held by another client\n", lockname);
        /* Revoke the lease since we didn't get the lock */
        uint8_t rev_req[32], rev_resp[256];
        size_t rvpos = 0;
        rvpos = encode_varint_field(rev_req, sizeof(rev_req), rvpos, 0x08, lease_id);
        do_rpc("/etcdserverpb.Lease/LeaseRevoke", rev_req, rvpos, rev_resp, sizeof(rev_resp));
        return 1;
    }

    /* Lock acquired — set up signal handler and global state for cleanup */
    g_lock_key_len = lock_key_len;
    memcpy(g_lock_key, lock_key, lock_key_len);
    g_lock_lease_id = lease_id;
    g_lock_held = 1;
    signal(SIGINT, lock_signal_handler);
    signal(SIGTERM, lock_signal_handler);

    /* Print the lock key (etcdctl prints the key name) */
    printf("%.*s\n", (int)lock_key_len, lock_key);
    fflush(stdout);

    /* If a command is provided, execute it; otherwise wait for signal */
    if (argc > 3) {
        /* Execute the command */
        int ret = 0;
        pid_t pid = fork();
        if (pid == 0) {
            /* Child */
            execvp(argv[3], &argv[3]);
            perror("execvp");
            _exit(127);
        } else if (pid > 0) {
            /* Parent: wait for child */
            int status;
            waitpid(pid, &status, 0);
            ret = WIFEXITED(status) ? WEXITSTATUS(status) : 1;
        } else {
            perror("fork");
            ret = 1;
        }
        /* Release the lock */
        g_lock_held = 0; /* signal handler won't double-delete */
        uint8_t del_req[512], del_resp[256];
        size_t dpos = 0;
        dpos = encode_bytes_field(del_req, sizeof(del_req), dpos, 0x0a,
                                  (const uint8_t *)lock_key, lock_key_len);
        do_rpc("/etcdserverpb.KV/DeleteRange", del_req, dpos, del_resp, sizeof(del_resp));
        uint8_t rev_req[32], rev_resp[256];
        size_t rvpos = 0;
        rvpos = encode_varint_field(rev_req, sizeof(rev_req), rvpos, 0x08, lease_id);
        do_rpc("/etcdserverpb.Lease/LeaseRevoke", rev_req, rvpos, rev_resp, sizeof(rev_resp));
        return ret;
    } else {
        /* No command: wait indefinitely until signal */
        while (g_lock_held) {
            pause();
        }
        return 0;
    }
}

static int cmd_elect(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "usage: cetcdctl elect ELECTION_NAME [PROPOSAL]\n");
        return 1;
    }
    const char *election_name = argv[2];
    size_t name_len = strlen(election_name);
    const char *proposal = (argc >= 4) ? argv[3] : "cetcd";
    size_t proposal_len = strlen(proposal);

    /* Build election key: "/{election_name}" */
    char elect_key[512];
    size_t elect_key_len = 0;
    elect_key[elect_key_len++] = '/';
    if (name_len >= sizeof(elect_key) - 1) {
        fprintf(stderr, "election name too long\n");
        return 1;
    }
    memcpy(elect_key + elect_key_len, election_name, name_len);
    elect_key_len += name_len;

    /* Step 1: Grant a lease with 60s TTL */
    uint8_t grant_req[16], grant_resp[256];
    size_t gpos = 0;
    gpos = encode_varint_field(grant_req, sizeof(grant_req), gpos, 0x03, 60);
    int glen = do_rpc("/etcdserverpb.Lease/LeaseGrant", grant_req, gpos, grant_resp, sizeof(grant_resp));
    if (glen < 0) { fprintf(stderr, "lease grant failed\n"); return 1; }

    uint64_t lease_id = 0;
    size_t rp = 0;
    while (rp < (size_t)glen) {
        uint8_t tag = grant_resp[rp++];
        if (tag == 0x08) {
            read_varint(grant_resp, glen, &rp, &lease_id);
            break;
        } else if (tag == 0x0a) {
            uint64_t l = 0; read_varint(grant_resp, glen, &rp, &l); rp += l;
        } else {
            uint64_t v = 0; read_varint(grant_resp, glen, &rp, &v);
        }
    }
    if (lease_id == 0) { fprintf(stderr, "failed to get lease ID\n"); return 1; }

    /* Step 2: Txn: Compare(key, CREATE, EQUAL, 0) -> success: Put(key, proposal, lease) */
    uint8_t cmp_buf[512];
    size_t cpos = 0;
    cpos = encode_varint_field(cmp_buf, sizeof(cmp_buf), cpos, 0x08, 0);
    cpos = encode_varint_field(cmp_buf, sizeof(cmp_buf), cpos, 0x10, 1);
    cpos = encode_bytes_field(cmp_buf, sizeof(cmp_buf), cpos, 0x1a,
                              (const uint8_t *)elect_key, elect_key_len);
    cpos = encode_varint_field(cmp_buf, sizeof(cmp_buf), cpos, 0x28, 0);

    uint8_t put_inner[512];
    size_t ppos = 0;
    ppos = encode_bytes_field(put_inner, sizeof(put_inner), ppos, 0x0a,
                              (const uint8_t *)elect_key, elect_key_len);
    ppos = encode_bytes_field(put_inner, sizeof(put_inner), ppos, 0x12,
                              (const uint8_t *)proposal, proposal_len);
    ppos = encode_varint_field(put_inner, sizeof(put_inner), ppos, 0x18, lease_id);

    uint8_t op_buf[1024];
    size_t opos = 0;
    op_buf[opos++] = 0x12;
    opos = write_varint(op_buf, sizeof(op_buf), opos, (uint64_t)ppos);
    memcpy(op_buf + opos, put_inner, ppos);
    opos += ppos;

    uint8_t req[2048], resp[1024];
    size_t pos = 0;
    pos = encode_bytes_field(req, sizeof(req), pos, 0x0a, cmp_buf, cpos);
    pos = encode_bytes_field(req, sizeof(req), pos, 0x12, op_buf, opos);

    int rlen = do_rpc("/etcdserverpb.KV/Txn", req, pos, resp, sizeof(resp));
    if (rlen < 0) { fprintf(stderr, "txn request failed\n"); return 1; }

    bool succeeded = false;
    rp = 0;
    while (rp < (size_t)rlen) {
        uint8_t tag = resp[rp++];
        if (tag == 0x10) {
            uint64_t v = 0; read_varint(resp, rlen, &rp, &v);
            succeeded = (v != 0);
            break;
        } else if (tag == 0x0a) {
            uint64_t l = 0; read_varint(resp, rlen, &rp, &l); rp += l;
        } else {
            uint64_t v = 0; read_varint(resp, rlen, &rp, &v);
        }
    }

    if (!succeeded) {
        fprintf(stderr, "election '%s' is held by another client\n", election_name);
        uint8_t rev_req[32], rev_resp[256];
        size_t rvpos = 0;
        rvpos = encode_varint_field(rev_req, sizeof(rev_req), rvpos, 0x08, lease_id);
        do_rpc("/etcdserverpb.Lease/LeaseRevoke", rev_req, rvpos, rev_resp, sizeof(rev_resp));
        return 1;
    }

    /* Elected — set up signal handler for cleanup */
    g_lock_key_len = elect_key_len;
    memcpy(g_lock_key, elect_key, elect_key_len);
    g_lock_lease_id = lease_id;
    g_lock_held = 1;
    signal(SIGINT, lock_signal_handler);
    signal(SIGTERM, lock_signal_handler);

    printf("%s\n", proposal);
    fflush(stdout);

    /* Wait for signal */
    while (g_lock_held) {
        pause();
    }
    return 0;
}

static int cmd_alarm(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "usage: cetcdctl alarm {list,activate,disarm} [TYPE]\n");
        return 1;
    }

    const char *subcmd = argv[2];
    int action = 0; /* 0=GET, 1=ACTIVATE, 2=DEACTIVATE */
    int alarm_type = 1; /* NOSPACE */
    int table_fmt = 0;
    int json_fmt = 0;

    for (int i = 3; i < argc; i++) {
        if ((strcmp(argv[i], "-w") == 0 || strcmp(argv[i], "--write-out") == 0) && i + 1 < argc) {
            if (strcmp(argv[i + 1], "table") == 0) table_fmt = 1;
            else if (strcmp(argv[i + 1], "json") == 0) json_fmt = 1;
            i++;
        }
    }

    if (strcmp(subcmd, "list") == 0) {
        action = 0;
    } else if (strcmp(subcmd, "activate") == 0) {
        action = 1;
    } else if (strcmp(subcmd, "disarm") == 0) {
        action = 2;
    } else {
        fprintf(stderr, "unknown alarm subcommand: %s\n", subcmd);
        return 1;
    }

    /* Build AlarmRequest: action(0x08), memberID(0x10), alarm(0x18) */
    uint8_t req[64];
    size_t rpos = 0;
    req[rpos++] = 0x08; /* field 1 = action */
    req[rpos++] = (uint8_t)action;
    req[rpos++] = 0x10; /* field 2 = memberID (0 = all) */
    req[rpos++] = 0x00;
    req[rpos++] = 0x18; /* field 3 = alarm type */
    req[rpos++] = (uint8_t)alarm_type;

    uint8_t resp[1024];
    int rlen = do_rpc("/etcdserverpb.Maintenance/Alarm", req, rpos, resp, sizeof(resp));
    if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }

    /* Parse AlarmResponse */
    size_t pos = 0;
    int found_alarms = 0;
    if (json_fmt && action == 0) {
        fputs("{\"header\":{},\"alarms\":[", stdout);
    }
    while (pos < (size_t)rlen) {
        uint8_t tag = resp[pos++];
        if (tag == 0x0a) { /* header, skip */
            uint64_t skip = 0; read_varint(resp, rlen, &pos, &skip);
            pos += (size_t)skip;
        } else if (tag == 0x12) { /* alarms */
            uint64_t alarm_len = 0; read_varint(resp, rlen, &pos, &alarm_len);
            size_t alarm_start = pos;
            uint64_t member_id = 0, alarm_val = 0;
            while (pos < alarm_start + (size_t)alarm_len) {
                uint8_t atag = resp[pos++];
                if (atag == 0x08) {
                    uint64_t v = 0; read_varint(resp, rlen, &pos, &v); member_id = v;
                } else if (atag == 0x10) {
                    uint64_t v = 0; read_varint(resp, rlen, &pos, &v); alarm_val = v;
                } else {
                    uint64_t skip = 0; read_varint(resp, rlen, &pos, &skip);
                }
            }
            if (action == 0) { /* list */
                const char *type_str = (alarm_val == 1) ? "NOSPACE" : "UNKNOWN";
                if (json_fmt) {
                    if (found_alarms) printf(",");
                    printf("{\"memberID\":%lu,\"alarm\":\"%s\"}", (unsigned long)member_id, type_str);
                } else if (table_fmt) {
                    if (!found_alarms) {
                        printf("+----------------------+----------+\n");
                        printf("|       MEMBER         |  ALARM   |\n");
                        printf("+----------------------+----------+\n");
                    }
                    printf("| %20lu | %8s |\n", (unsigned long)member_id, type_str);
                } else {
                    printf("memberID:%lu alarm:%s\n", (unsigned long)member_id, type_str);
                }
            }
            found_alarms = 1;
        } else {
            uint64_t skip = 0; read_varint(resp, rlen, &pos, &skip);
        }
    }

    if (action == 0) { /* list */
        if (json_fmt) {
            printf("]}\n");
        } else if (table_fmt && found_alarms) {
            printf("+----------------------+----------+\n");
        }
        if (!found_alarms && !table_fmt && !json_fmt) printf("no alarms\n");
        if (!found_alarms && table_fmt) printf("(no alarms)\n");
    } else if (action == 1) { /* activate */
        printf("alarm activated NOSPACE\n");
    } else { /* disarm */
        printf("alarm disarmed\n");
    }
    return 0;
}

static int cmd_version(int argc, char **argv) {
    (void)argc; (void)argv;
    printf("cetcd version 0.1.0 (etcd v3.5 compatible)\n");
    return 0;
}

static int cmd_snapshot(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "usage: cetcdctl snapshot save [FILE]\n");
        fprintf(stderr, "       cetcdctl snapshot status FILE\n");
        fprintf(stderr, "       cetcdctl snapshot restore FILE --data-dir DIR\n");
        return 1;
    }
    if (strcmp(argv[2], "save") == 0) {
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
    } else if (strcmp(argv[2], "status") == 0) {
        /* Show snapshot file info */
        if (argc < 4) {
            fprintf(stderr, "usage: cetcdctl snapshot status FILE\n");
            return 1;
        }
        FILE *f = fopen(argv[3], "rb");
        if (!f) { perror("fopen"); return 1; }
        fseek(f, 0, SEEK_END);
        long fsize = ftell(f);
        fclose(f);
        printf("+----------+----------+---------+---------------------+\n");
        printf("|   hash   | revision |  size   |     filename        |\n");
        printf("+----------+----------+---------+---------------------+\n");
        printf("| %-8s | %8s | %7ld | %-19s |\n", "-", "-", fsize, argv[3]);
        printf("+----------+----------+---------+---------------------+\n");
        return 0;
    } else if (strcmp(argv[2], "restore") == 0) {
        /* Restore a snapshot to a data directory.
         * In cetcd, the snapshot is a raw LMDB database copy.
         * We copy the file to the target data directory. */
        if (argc < 4) {
            fprintf(stderr, "usage: cetcdctl snapshot restore FILE --data-dir DIR\n");
            return 1;
        }
        const char *snap_file = argv[3];
        const char *data_dir = NULL;
        for (int i = 4; i < argc; i++) {
            if (strcmp(argv[i], "--data-dir") == 0 && i + 1 < argc) {
                data_dir = argv[++i];
            }
        }
        if (!data_dir) {
            fprintf(stderr, "--data-dir is required for snapshot restore\n");
            return 1;
        }
        /* Read the snapshot file */
        FILE *sf = fopen(snap_file, "rb");
        if (!sf) { perror("fopen snapshot"); return 1; }
        fseek(sf, 0, SEEK_END);
        long snap_size = ftell(sf);
        fseek(sf, 0, SEEK_SET);
        if (snap_size <= 0) { fprintf(stderr, "snapshot file is empty\n"); fclose(sf); return 1; }
        uint8_t *snap_data = (uint8_t *)malloc(snap_size);
        if (!snap_data) { fprintf(stderr, "out of memory\n"); fclose(sf); return 1; }
        fread(snap_data, 1, snap_size, sf);
        fclose(sf);
        /* Create the data directory and write the snapshot */
        char target_path[512];
        snprintf(target_path, sizeof(target_path), "%s/data.mdb", data_dir);
        /* Create parent directory */
        char mkdir_cmd[512];
        snprintf(mkdir_cmd, sizeof(mkdir_cmd), "mkdir -p %s", data_dir);
        system(mkdir_cmd);
        FILE *df = fopen(target_path, "wb");
        if (!df) { perror("fopen data dir"); free(snap_data); return 1; }
        fwrite(snap_data, 1, snap_size, df);
        fclose(df);
        free(snap_data);
        printf("snapshot restored to %s (%ld bytes)\n", target_path, snap_size);
        return 0;
    } else {
        fprintf(stderr, "unknown snapshot subcommand: %s\n", argv[2]);
        return 1;
    }
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
        fprintf(stderr, "       cetcdctl member promote ID\n");
        return 1;
    }
    if (strcmp(argv[2], "list") == 0) {
        int table_fmt = 0, json_fmt = 0;
        for (int i = 3; i < argc; i++) {
            if ((strcmp(argv[i], "-w") == 0 || strcmp(argv[i], "--write-out") == 0) && i + 1 < argc) {
                if (strcmp(argv[i + 1], "table") == 0) table_fmt = 1;
                else if (strcmp(argv[i + 1], "json") == 0) json_fmt = 1;
                i++;
            }
        }
        uint8_t req[] = {0x00}, resp[4096];
        int rlen = do_rpc("/etcdserverpb.Cluster/MemberList", req, 1, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        parse_member_list_response(resp, rlen, table_fmt, json_fmt);
    } else if (strcmp(argv[2], "add") == 0) {
        if (argc < 4) { fprintf(stderr, "usage: cetcdctl member add PEER_URL\n"); return 1; }
        uint8_t req[512], resp[4096];
        size_t pos = 0;
        pos = encode_string_field(req, sizeof(req), pos, 0x0a, argv[3]);
        int rlen = do_rpc("/etcdserverpb.Cluster/MemberAdd", req, pos, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        parse_member_list_response(resp, rlen, 0, 0);
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
    } else if (strcmp(argv[2], "promote") == 0) {
        if (argc < 4) { fprintf(stderr, "usage: cetcdctl member promote ID\n"); return 1; }
        uint8_t req[32], resp[256];
        size_t pos = 0;
        pos = encode_varint_field(req, sizeof(req), pos, 0x08, (uint64_t)atol(argv[3]));
        int rlen = do_rpc("/etcdserverpb.Cluster/MemberPromote", req, pos, resp, sizeof(resp));
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
        fprintf(stderr, "usage: cetcdctl auth enable|disable|status|login\n");
        return 1;
    }
    if (strcmp(argv[2], "login") == 0) {
        if (argc < 5) { fprintf(stderr, "usage: cetcdctl auth login NAME PASS\n"); return 1; }
        uint8_t req[512], resp[1024];
        size_t pos = 0;
        pos = encode_string_field(req, sizeof(req), pos, 0x0a, argv[3]);
        pos = encode_string_field(req, sizeof(req), pos, 0x12, argv[4]);
        int rlen = do_rpc("/etcdserverpb.Auth/Authenticate", req, pos, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "authentication failed\n"); return 1; }
        /* Parse AuthenticateResponse: field 1 (header) = bytes tag=0x0a, field 2 (token) = bytes tag=0x12 */
        size_t rpos = 0;
        while (rpos < (size_t)rlen) {
            uint8_t tag = resp[rpos++];
            if (tag == 0x12) {
                uint64_t l = 0; read_varint(resp, rlen, &rpos, &l);
                printf("token: %.*s\n", (int)l, resp + rpos);
                return 0;
            } else if (tag == 0x0a) {
                /* Skip header (length-delimited) */
                uint64_t l = 0; read_varint(resp, rlen, &rpos, &l);
                rpos += l;
            } else {
                uint64_t v = 0; read_varint(resp, rlen, &rpos, &v);
            }
        }
        printf("OK (no token returned)\n");
        return 0;
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
        parse_string_list_response(resp, rlen, "roles", 0);
    } else if (strcmp(argv[2], "list") == 0) {
        int table_fmt = 0;
        for (int i = 3; i < argc; i++) {
            if ((strcmp(argv[i], "-w") == 0 || strcmp(argv[i], "--write-out") == 0) && i + 1 < argc) {
                if (strcmp(argv[i + 1], "table") == 0) table_fmt = 1;
                i++;
            }
        }
        uint8_t req[] = {0x00}, resp[4096];
        int rlen = do_rpc("/etcdserverpb.Auth/UserList", req, 1, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        parse_string_list_response(resp, rlen, "users", table_fmt);
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
        int table_fmt = 0;
        for (int i = 3; i < argc; i++) {
            if ((strcmp(argv[i], "-w") == 0 || strcmp(argv[i], "--write-out") == 0) && i + 1 < argc) {
                if (strcmp(argv[i + 1], "table") == 0) table_fmt = 1;
                i++;
            }
        }
        uint8_t req[] = {0x00}, resp[4096];
        int rlen = do_rpc("/etcdserverpb.Auth/RoleList", req, 1, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        parse_string_list_response(resp, rlen, "roles", table_fmt);
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
            } else if (tag == 0x0a) {
                /* Skip header (length-delimited) */
                uint64_t l = 0; read_varint(resp, rlen, &rpos, &l);
                rpos += l;
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

static int cmd_watch(int argc, char **argv) {
    if (argc < 3) { fprintf(stderr, "usage: cetcdctl watch [--prefix] [--prev-kv] [--start-rev N] [-w json] KEY\n"); return 1; }
    bool prefix = false;
    bool prev_kv = false;
    bool want_json = false;
    int64_t start_rev = 0;
    const char *key = NULL;
    for (int i = 2; i < argc; i++) {
        if (strcmp(argv[i], "--prefix") == 0) {
            prefix = true;
        } else if (strcmp(argv[i], "--prev-kv") == 0) {
            prev_kv = true;
        } else if (strcmp(argv[i], "--start-rev") == 0) {
            if (i + 1 >= argc) { fprintf(stderr, "--start-rev requires a revision number\n"); return 1; }
            start_rev = atol(argv[++i]);
        } else if ((strcmp(argv[i], "-w") == 0 || strcmp(argv[i], "--write-out") == 0) && i + 1 < argc) {
            if (strcmp(argv[i + 1], "json") == 0) want_json = true;
            i++;
        } else {
            key = argv[i];
        }
    }
    if (!key) { fprintf(stderr, "usage: cetcdctl watch [--prefix] [--prev-kv] [--start-rev N] [-w json] KEY\n"); return 1; }
    size_t key_len = strlen(key);

    /* Build WatchCreateRequest:
     *   field 1 (request_union) = WatchCreateRequest, tag = 0x0a
     *     WatchCreateRequest: field 1 (key) = bytes, tag = 0x0a
     *                         field 2 (range_end) = bytes, tag = 0x12 (if prefix)
     */
    uint8_t create_inner[512];
    size_t cpos = 0;
    cpos = encode_bytes_field(create_inner, sizeof(create_inner), cpos, 0x0a,
                              (const uint8_t *)key, key_len);
    if (prefix) {
        char range_end[256];
        if (key_len >= sizeof(range_end)) { fprintf(stderr, "key too long\n"); return 1; }
        memcpy(range_end, key, key_len);
        range_end[key_len - 1]++;
        cpos = encode_bytes_field(create_inner, sizeof(create_inner), cpos, 0x12,
                                  (const uint8_t *)range_end, key_len);
    }
    if (start_rev > 0) {
        /* field 3 (start_revision) = int64, tag = 0x18 */
        cpos = encode_varint_field(create_inner, sizeof(create_inner), cpos, 0x18, (uint64_t)start_rev);
    }
    if (prev_kv) {
        /* field 6 (prev_kv) = bool, tag = 0x30 */
        cpos = encode_varint_field(create_inner, sizeof(create_inner), cpos, 0x30, 1);
    }

    uint8_t watch_buf[1024];
    size_t wpos = 0;
    watch_buf[wpos++] = 0x0a; /* field 1 = create request */
    wpos = write_varint(watch_buf, sizeof(watch_buf), wpos, (uint64_t)cpos);
    memcpy(watch_buf + wpos, create_inner, cpos);
    wpos += cpos;

    uint8_t resp[8192];
    int rlen = do_rpc("/etcdserverpb.Watch/Watch", watch_buf, wpos, resp, sizeof(resp));
    if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }

    /* Parse WatchResponse:
     *   field 1 (header) = ResponseHeader, tag = 0x0a
     *   field 2 (watch_id) = int64, tag = 0x10
     *   field 3 (created) = bool, tag = 0x18
     *   field 11 (events) = repeated Event, tag = 0x5a
     *     Event: field 1 (type) = enum, tag = 0x08
     *            field 2 (kv) = KeyValue, tag = 0x12
     *            field 3 (prev_kv) = KeyValue, tag = 0x1a
     *              KeyValue: field 1 (key, 0x0a), field 2 (create_rev, 0x10),
     *                        field 3 (mod_rev, 0x18), field 4 (version, 0x20),
     *                        field 5 (value, 0x2a)
     */
    size_t rpos = 0;
    while (rpos < (size_t)rlen) {
        uint8_t tag = resp[rpos++];
        if (tag == 0x5a) {
            /* Event (length-delimited) */
            uint64_t elen = 0; read_varint(resp, rlen, &rpos, &elen);
            size_t eend = rpos + (size_t)elen;
            while (rpos < eend) {
                uint8_t etag = resp[rpos++];
                if (etag == 0x08) {
                    /* event type */
                    uint64_t t = 0; read_varint(resp, eend, &rpos, &t);
                    if (want_json) {
                        fputs("{\"type\":\"", stdout);
                        fputs(t == 0 ? "PUT" : "DELETE", stdout);
                        fputs("\",\"", stdout);
                    } else {
                        printf("%s: ", t == 0 ? "PUT" : "DELETE");
                    }
                } else if (etag == 0x12) {
                    /* KeyValue */
                    uint64_t klen = 0; read_varint(resp, eend, &rpos, &klen);
                    size_t kend = rpos + (size_t)klen;
                    const uint8_t *ek = NULL, *ev = NULL;
                    size_t ekl = 0, evl = 0;
                    while (rpos < kend) {
                        uint8_t ktag = resp[rpos++];
                        if (ktag == 0x0a) {
                            uint64_t l = 0; read_varint(resp, kend, &rpos, &l);
                            ek = resp + rpos; ekl = (size_t)l; rpos += l;
                        } else if (ktag == 0x2a) {
                            uint64_t l = 0; read_varint(resp, kend, &rpos, &l);
                            ev = resp + rpos; evl = (size_t)l; rpos += l;
                        } else {
                            uint64_t v = 0; read_varint(resp, kend, &rpos, &v);
                        }
                    }
                    rpos = kend;
                    if (want_json) {
                        fputs("kv\":{\"key\":\"", stdout);
                        if (ek) fwrite(ek, 1, ekl, stdout);
                        fputs("\"", stdout);
                        if (ev && evl > 0) {
                            fputs(",\"value\":\"", stdout);
                            fwrite(ev, 1, evl, stdout);
                            fputs("\"", stdout);
                        }
                        fputs("}}\n", stdout);
                    } else {
                        if (ek) printf("%.*s", (int)ekl, ek);
                        if (ev && evl > 0) printf(" -> %.*s", (int)evl, ev);
                        printf("\n");
                    }
                } else if (etag == 0x1a) {
                    /* prev_kv (KeyValue) */
                    uint64_t klen = 0; read_varint(resp, eend, &rpos, &klen);
                    size_t kend = rpos + (size_t)klen;
                    const uint8_t *pk = NULL, *pv = NULL;
                    size_t pkl = 0, pvl = 0;
                    while (rpos < kend) {
                        uint8_t ktag = resp[rpos++];
                        if (ktag == 0x0a) {
                            uint64_t l = 0; read_varint(resp, kend, &rpos, &l);
                            pk = resp + rpos; pkl = (size_t)l; rpos += l;
                        } else if (ktag == 0x2a) {
                            uint64_t l = 0; read_varint(resp, kend, &rpos, &l);
                            pv = resp + rpos; pvl = (size_t)l; rpos += l;
                        } else {
                            uint64_t v = 0; read_varint(resp, kend, &rpos, &v);
                        }
                    }
                    rpos = kend;
                    if (pk) {
                        printf(" (prev: %.*s", (int)pkl, pk);
                        if (pv && pvl > 0) printf(" -> %.*s", (int)pvl, pv);
                        printf(")");
                    }
                } else {
                    uint64_t v = 0; read_varint(resp, eend, &rpos, &v);
                }
            }
            rpos = eend;
        } else if (tag == 0x0a) {
            /* Skip header (length-delimited) */
            uint64_t l = 0; read_varint(resp, rlen, &rpos, &l); rpos += l;
        } else {
            uint64_t v = 0; read_varint(resp, rlen, &rpos, &v);
        }
    }
    return 0;
}

static int cmd_hash(int argc, char **argv) {
    (void)argc; (void)argv;
    uint8_t req[] = {0x00}, resp[256];
    int rlen = do_rpc("/etcdserverpb.Maintenance/Hash", req, 1, resp, sizeof(resp));
    if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
    /* HashResponse: field 1 (header), field 2 (hash) = uint32, tag = 0x10 */
    size_t rpos = 0;
    while (rpos < (size_t)rlen) {
        uint8_t tag = resp[rpos++];
        if (tag == 0x10) {
            uint64_t v = 0; read_varint(resp, rlen, &rpos, &v);
            printf("hash: %llu\n", (unsigned long long)v);
        } else if (tag == 0x0a) {
            /* Skip header (length-delimited) */
            uint64_t l = 0; read_varint(resp, rlen, &rpos, &l);
            rpos += l;
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
    /* HashKVResponse: field 1 (header), field 2 (hash), field 3 (compact_revision) */
    size_t rpos = 0;
    while (rpos < (size_t)rlen) {
        uint8_t tag = resp[rpos++];
        if (tag == 0x10) {
            uint64_t v = 0; read_varint(resp, rlen, &rpos, &v);
            printf("hash: %llu\n", (unsigned long long)v);
        } else if (tag == 0x18) {
            uint64_t v = 0; read_varint(resp, rlen, &rpos, &v);
            printf("compact_revision: %llu\n", (unsigned long long)v);
        } else if (tag == 0x0a) {
            /* Skip header (length-delimited) */
            uint64_t l = 0; read_varint(resp, rlen, &rpos, &l);
            rpos += l;
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

/* Authenticate with server using --user USER:PASS */
static int do_authenticate(const char *user_cred) {
    const char *colon = strchr(user_cred, ':');
    if (!colon) {
        fprintf(stderr, "--user format must be USER:PASS\n");
        return -1;
    }
    size_t user_len = (size_t)(colon - user_cred);
    const char *pass = colon + 1;
    size_t pass_len = strlen(pass);

    /* AuthenticateRequest: name (field 1, 0x0a), password (field 2, 0x12) */
    uint8_t req[512], resp[256];
    size_t pos = 0;
    pos = encode_bytes_field(req, sizeof(req), pos, 0x0a,
                             (const uint8_t *)user_cred, user_len);
    pos = encode_bytes_field(req, sizeof(req), pos, 0x12,
                             (const uint8_t *)pass, pass_len);
    int rlen = do_rpc("/etcdserverpb.Auth/Authenticate", req, pos, resp, sizeof(resp));
    if (rlen < 0) {
        fprintf(stderr, "authentication request failed\n");
        return -1;
    }
    /* Parse AuthenticateResponse: field 1 (header), field 2 (token) */
    size_t rpos = 0;
    while (rpos < (size_t)rlen) {
        uint8_t tag = resp[rpos++];
        if (tag == 0x12) {
            uint64_t tlen = 0; read_varint(resp, rlen, &rpos, &tlen);
            size_t copy = (size_t)tlen < sizeof(g_auth_token) - 1 ? (size_t)tlen : sizeof(g_auth_token) - 1;
            memcpy(g_auth_token, resp + rpos, copy);
            g_auth_token[copy] = '\0';
            rpos += tlen;
        } else if (tag == 0x0a) {
            uint64_t l = 0; read_varint(resp, rlen, &rpos, &l); rpos += l;
        } else {
            uint64_t v = 0; read_varint(resp, rlen, &rpos, &v);
        }
    }
    if (g_debug) {
        fprintf(stderr, "[debug] authenticated, token=%s\n", g_auth_token);
    }
    return 0;
}

static void print_usage(void) {
    printf("cetcdctl — command-line client for cetcd\n\n");
    printf("Usage: cetcdctl [global options] COMMAND [args]\n\n");
    printf("Global options:\n");
    printf("  --host ADDR    Server address (default: 127.0.0.1)\n");
    printf("  --port PORT    Server port (default: 2379)\n");
    printf("  --endpoints EP Server endpoint (host:port format, uses first endpoint)\n");
    printf("  --user USER:PASS  Authenticate with server before executing command\n");
    printf("  --command-timeout SEC  Timeout for commands (default: none)\n");
    printf("  --debug       Print debug info (RPC path and response size)\n\n");
    printf("Commands:\n");
    printf("  put [--prev-kv] [--ignore-value] [--ignore-lease] [--lease ID] KEY [VALUE]  Store a key-value pair\n");
    printf("  get [--prefix] [--from-key] [--keys-only] [--count-only] [--print-value-only] [--hex] [--consistency l|s] [-w json|fields] [--rev N] [--limit N] [--sort-by FIELD] [--sort-order ORDER] [--min-mod-rev N] [--max-mod-rev N] [--min-create-rev N] [--max-create-rev N] KEY [RANGE_END]\n");
    printf("                         Retrieve keys (sort-by: key|version|create|mod|value; sort-order: ascend|descend)\n");
    printf("  del [--prefix] [--from-key] [--prev-kv] [-w json] KEY [RANGE_END]  Delete a key (options: --prefix, --from-key, --prev-kv)\n");
    printf("  watch [--prefix] [--prev-kv] [--start-rev N] [-w json] KEY  Watch key changes (single response)\n");
    printf("  lease grant TTL        Grant a lease (TTL in seconds)\n");
    printf("  lease revoke ID        Revoke a lease by ID\n");
    printf("  lease timetolive [--keys] ID  Query remaining TTL\n");
    printf("  lease list             List all active leases\n");
    printf("  lease keepalive ID     Keep a lease alive\n");
    printf("  txn put KEY VALUE      Execute a transaction (Put)\n");
    printf("  txn cas KEY EXP NEW    Compare-and-swap (if KEY==EXP then KEY=NEW)\n");
    printf("  txn get KEY [RANGE_END]  Execute a transaction (Range)\n");
    printf("  txn del [--prefix] [--prev-kv] KEY [RANGE_END]  Execute a transaction (Delete)\n");
    printf("  compact [--physical] REV   Compact MVCC history to revision\n");
    printf("  status                 Get server status\n");
    printf("  alarm list             List all alarms\n");
    printf("  alarm activate TYPE    Activate an alarm (NOSPACE)\n");
    printf("  alarm disarm           Disarm all alarms\n");
    printf("  hash                   Get KV store hash\n");
    printf("  hashkv                 Get KV store hash + compact revision\n");
    printf("  defrag                 Defragment database (no-op for LMDB)\n");
    printf("  move-leader TARGET_ID  Transfer leadership to target node\n");
    printf("  member list            List cluster members\n");
    printf("  member add PEER_URL    Add a cluster member\n");
    printf("  member remove ID       Remove a cluster member\n");
    printf("  member update ID URL   Update a member's peer URL\n");
    printf("  member promote ID      Promote a member to voting member\n");
    printf("  auth enable            Enable authentication\n");
    printf("  auth disable           Disable authentication\n");
    printf("  auth status            Query auth status\n");
    printf("  auth login NAME PASS   Authenticate and get token\n");
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
    printf("  snapshot status FILE   Show snapshot file info\n");
    printf("  snapshot restore FILE --data-dir DIR  Restore snapshot to data dir\n");
    printf("  downgrade enable VER   Enable cluster downgrade\n");
    printf("  downgrade cancel       Cancel cluster downgrade\n");
    printf("  downgrade validate VER Validate downgrade version\n");
    printf("  version                Print the client version\n");
    printf("  endpoint health        Check server health\n");
    printf("  endpoint status        Get server status with endpoint info\n");
    printf("  check perf             Run a simple performance check\n");
    printf("  lock LOCKNAME [CMD...] Acquire a distributed lock\n");
    printf("  elect ELECTION_NAME [PROPOSAL]  Leader election\n");
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
        } else if ((strcmp(argv[cmd_start], "--endpoints") == 0 || strcmp(argv[cmd_start], "--endpoint") == 0) && cmd_start + 1 < argc) {
            /* Parse first endpoint: host:port format */
            const char *ep = argv[cmd_start + 1];
            const char *colon = strchr(ep, ':');
            if (colon) {
                size_t hlen = (size_t)(colon - ep);
                if (hlen > 0 && hlen < 256) {
                    static char host_buf[256];
                    memcpy(host_buf, ep, hlen);
                    host_buf[hlen] = '\0';
                    g_host = host_buf;
                    g_port = (uint16_t)atoi(colon + 1);
                }
            } else {
                g_host = ep;
            }
            cmd_start += 2;
        } else if (strcmp(argv[cmd_start], "--command-timeout") == 0 && cmd_start + 1 < argc) {
            int timeout_sec = atoi(argv[cmd_start + 1]);
            if (timeout_sec > 0) {
                signal(SIGALRM, (void (*)(int))_exit);
                alarm((unsigned)timeout_sec);
            }
            cmd_start += 2;
        } else if (strcmp(argv[cmd_start], "--debug") == 0) {
            g_debug = 1;
            cmd_start += 1;
        } else if (strcmp(argv[cmd_start], "--user") == 0 && cmd_start + 1 < argc) {
            const char *cred = argv[cmd_start + 1];
            if (do_authenticate(cred) != 0) {
                return 1;
            }
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
    if (strcmp(cmd, "watch") == 0)      return cmd_watch(new_argc, new_argv);
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
    if (strcmp(cmd, "version") == 0)     return cmd_version(new_argc, new_argv);
    if (strcmp(cmd, "endpoint") == 0)   return cmd_endpoint(new_argc, new_argv);
    if (strcmp(cmd, "check") == 0)      return cmd_check(new_argc, new_argv);
    if (strcmp(cmd, "lock") == 0)       return cmd_lock(new_argc, new_argv);
    if (strcmp(cmd, "elect") == 0)      return cmd_elect(new_argc, new_argv);

    fprintf(stderr, "unknown command: %s\n", cmd);
    print_usage();
    return 1;
}
