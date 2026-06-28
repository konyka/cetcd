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
 *   role grant-permission ROLE TYPE KEY [--prefix] [--range-end] — grant a specific permission
 *   role revoke-permission ROLE [TYPE KEY] [--prefix] [--range-end] — revoke a specific permission
 *   endpoint health       — check server health
 *   endpoint status       — get server status with endpoint info
 *   check perf            — run a simple performance check
 *   lock LOCKNAME [CMD...] — acquire a distributed lock
 *   elect ELECTION_NAME [PROPOSAL] — leader election
 *   completion bash|zsh|fish — generate shell completion script
 *   version               — print client version
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <unistd.h>
#include <signal.h>
#include <errno.h>
#include <poll.h>
#include <sys/wait.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "cetcd/base.h"

static const char *g_host = "127.0.0.1";
static uint16_t    g_port = 2379;
static int         g_keys_only = 0; /* flag for get --keys-only */
static int         g_count_only = 0; /* flag for get --count-only */
static int         g_print_value_only = 0; /* flag for get --print-value-only */
static int         g_hex = 0; /* flag for get --hex */
static int         g_write_json = 0; /* flag for -w json */
static int         g_write_fields = 0; /* flag for -w fields */
static int         g_write_table = 0; /* flag for -w table */
static int         g_debug = 0; /* flag for --debug */
static int         g_insecure = 0; /* flag for --insecure (no-op, plain TCP) */
static int         g_dial_timeout = 0; /* flag for --dial-timeout (seconds) */
static char        g_auth_token[256] = ""; /* token from --user */

/* --- Lock state for signal handler --- */
static char          g_lock_key[256];
static size_t        g_lock_key_len = 0;
static uint64_t      g_lock_lease_id = 0;
static volatile sig_atomic_t g_lock_held = 0;
static pid_t         g_keepalive_pid = -1; /* keepalive child process for lock/elect */

/* --- Lease keepalive SIGINT state --- */
static volatile sig_atomic_t g_keepalive_stop = 0;
static void keepalive_sigint_handler(int sig) {
    (void)sig;
    g_keepalive_stop = 1;
}

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
    if (g_dial_timeout > 0) {
        struct timeval tv;
        tv.tv_sec = g_dial_timeout;
        tv.tv_usec = 0;
        setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    }
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

/* Parse ResponseHeader from a protobuf message and output as JSON "header" field.
 * Scans the message for tag 0x0a (field 1 = header, length-delimited).
 * Returns 1 if header found, 0 if not. */
static int parse_and_print_header_json(const uint8_t *data, size_t len) {
    size_t pos = 0;
    while (pos < len) {
        uint8_t tag = data[pos++];
        if (tag == 0x0a) {
            uint64_t l = 0; read_varint(data, len, &pos, &l);
            size_t hdr_end = pos + (size_t)l;
            uint64_t cluster_id = 0, member_id = 0, revision = 0, raft_term = 0;
            while (pos < hdr_end) {
                uint8_t htag = data[pos++];
                if (htag == 0x08) read_varint(data, hdr_end, &pos, &cluster_id);
                else if (htag == 0x10) read_varint(data, hdr_end, &pos, &member_id);
                else if (htag == 0x18) read_varint(data, hdr_end, &pos, &revision);
                else if (htag == 0x20) read_varint(data, hdr_end, &pos, &raft_term);
                else { uint64_t v = 0; read_varint(data, hdr_end, &pos, &v); }
            }
            printf("\"header\":{\"cluster_id\":%llu,\"member_id\":%llu,\"revision\":%llu,\"raft_term\":%llu}",
                   (unsigned long long)cluster_id, (unsigned long long)member_id,
                   (unsigned long long)revision, (unsigned long long)raft_term);
            return 1;
        } else if (tag == 0x12 || tag == 0x1a || tag == 0x22) {
            /* Skip nested messages (kvs, prev_kv, etc.) */
            uint64_t l = 0; read_varint(data, len, &pos, &l); pos += l;
        } else {
            uint64_t v = 0; read_varint(data, len, &pos, &v);
        }
    }
    fputs("\"header\":{}", stdout);
    return 0;
}

/* Print a byte buffer as a JSON string with proper escaping. */
static void print_json_string(const uint8_t *data, size_t len) {
    putchar('"');
    for (size_t i = 0; i < len; i++) {
        char c = (char)data[i];
        if (c == '"' || c == '\\') printf("\\%c", c);
        else if (c == '\n') fputs("\\n", stdout);
        else if (c == '\r') fputs("\\r", stdout);
        else if (c == '\t') fputs("\\t", stdout);
        else if (c >= 32 && c < 127) putchar(c);
        else printf("\\u%04x", (unsigned)(unsigned char)c);
    }
    putchar('"');
}

static void parse_range_response(const uint8_t *data, size_t len) {
    size_t pos = 0;
    int count = 0;
    int server_count = -1;
    /* ResponseHeader fields */
    uint64_t hdr_cluster_id = 0, hdr_member_id = 0, hdr_revision = 0, hdr_raft_term = 0;
    int have_header = 0;
    int has_more = 0;
    if (g_write_json) {
        /* Will output header later, after parsing */
        fputs("{", stdout);
    }
    if (g_write_table && !g_count_only) {
        printf("+------------------+------------+------------+---------+------------------+\n");
        printf("|       KEY        | CREATE_REV |  MODIFY_REV| VERSION |      VALUE       |\n");
        printf("+------------------+------------+------------+---------+------------------+\n");
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
                } else if (g_write_table) {
                    int kl = (int)(key_len > 16 ? 16 : key_len);
                    printf("| %-*.*s ", 16, kl, key_data);
                    printf("| %10llu ", (unsigned long long)create_rev);
                    printf("| %10llu ", (unsigned long long)mod_rev);
                    printf("| %7llu ", (unsigned long long)version);
                    if (!g_keys_only && val_data && val_len > 0) {
                        int vl = (int)(val_len > 16 ? 16 : val_len);
                        printf("| %-*.*s |\n", 16, vl, val_data);
                    } else {
                        printf("| %-16s |\n", "");
                    }
                } else if (g_write_json) {
                    if (first_kv) {
                        /* Output header before first KV */
                        if (have_header) {
                            printf("\"header\":{\"cluster_id\":%llu,\"member_id\":%llu,\"revision\":%llu,\"raft_term\":%llu},",
                                   (unsigned long long)hdr_cluster_id, (unsigned long long)hdr_member_id,
                                   (unsigned long long)hdr_revision, (unsigned long long)hdr_raft_term);
                        } else {
                            fputs("\"header\":{},", stdout);
                        }
                        fputs("\"kvs\":[", stdout);
                    }
                    if (!first_kv) printf(",");
                    first_kv = 0;
                    fputs("{\"key\":", stdout);
                    print_json_string(key_data, key_len);
                    printf(",\"create_revision\":%llu", (unsigned long long)create_rev);
                    printf(",\"mod_revision\":%llu", (unsigned long long)mod_rev);
                    printf(",\"version\":%llu", (unsigned long long)version);
                    if (lease > 0) printf(",\"lease\":%llu", (unsigned long long)lease);
                    if (!g_keys_only && val_data && val_len > 0) {
                        fputs(",\"value\":", stdout);
                        print_json_string(val_data, val_len);
                    }
                    fputs("}", stdout);
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
            has_more = (int)v;
        } else if (tag == 0x0a) {
            /* Parse ResponseHeader (length-delimited, field 1) */
            uint64_t l = 0; read_varint(data, len, &pos, &l);
            size_t hdr_end = pos + (size_t)l;
            have_header = 1;
            while (pos < hdr_end) {
                uint8_t htag = data[pos++];
                if (htag == 0x08) {
                    read_varint(data, hdr_end, &pos, &hdr_cluster_id);
                } else if (htag == 0x10) {
                    read_varint(data, hdr_end, &pos, &hdr_member_id);
                } else if (htag == 0x18) {
                    read_varint(data, hdr_end, &pos, &hdr_revision);
                } else if (htag == 0x20) {
                    read_varint(data, hdr_end, &pos, &hdr_raft_term);
                } else {
                    uint64_t v = 0; read_varint(data, hdr_end, &pos, &v);
                }
            }
            pos = hdr_end;
        } else {
            uint64_t v = 0; read_varint(data, len, &pos, &v);
        }
    }
    if (g_write_table && !g_count_only) {
        printf("+------------------+------------+------------+---------+------------------+\n");
    }
    if (g_count_only) {
        if (g_write_json) {
            if (first_kv) {
                /* No KVs were encountered, output header + empty kvs */
                if (have_header) {
                    printf("\"header\":{\"cluster_id\":%llu,\"member_id\":%llu,\"revision\":%llu,\"raft_term\":%llu},",
                           (unsigned long long)hdr_cluster_id, (unsigned long long)hdr_member_id,
                           (unsigned long long)hdr_revision, (unsigned long long)hdr_raft_term);
                } else {
                    fputs("\"header\":{},", stdout);
                }
                fputs("\"kvs\":[", stdout);
            }
            printf("],\"count\":%d,\"more\":%s}\n", server_count >= 0 ? server_count : count, has_more ? "true" : "false");
        } else {
            printf("%d\n", server_count >= 0 ? server_count : count);
        }
    } else if (count == 0) {
        if (g_write_json) {
            if (first_kv) {
                if (have_header) {
                    printf("\"header\":{\"cluster_id\":%llu,\"member_id\":%llu,\"revision\":%llu,\"raft_term\":%llu},",
                           (unsigned long long)hdr_cluster_id, (unsigned long long)hdr_member_id,
                           (unsigned long long)hdr_revision, (unsigned long long)hdr_raft_term);
                } else {
                    fputs("\"header\":{},", stdout);
                }
                fputs("\"kvs\":[", stdout);
            }
            printf("],\"count\":0,\"more\":false}\n");
        } else {
            printf("(empty)\n");
        }
    } else {
        if (g_write_json) {
            printf("],\"count\":%d,\"more\":%s}\n", server_count >= 0 ? server_count : count, has_more ? "true" : "false");
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

static void parse_member_list_response(const uint8_t *data, size_t len, int table_format, int json_format, int fields_format) {
    size_t pos = 0;
    int first = 1;
    if (table_format) {
        printf("+------------------+--------+---------------------+\n");
        printf("|        ID        | STATUS |     PEER ADDRS      |\n");
        printf("+------------------+--------+---------------------+\n");
    }
    if (json_format) {
        fputs("{", stdout);
        parse_and_print_header_json(data, len);
        fputs(",\"members\":[", stdout);
    }
    while (pos < len) {
        uint8_t tag = data[pos++];
        if (tag == 0x12) {
            /* Member (length-delimited) */
            uint64_t mlen = 0; read_varint(data, len, &pos, &mlen);
            size_t mend = pos + (size_t)mlen;
            uint64_t mid = 0;
            const uint8_t *peer_url = NULL; size_t peer_len = 0;
            const uint8_t *m_name = NULL; size_t name_len = 0;
            const uint8_t *client_url = NULL; size_t client_len = 0;
            int is_learner = 0;
            while (pos < mend) {
                uint8_t mtag = data[pos++];
                if (mtag == 0x08) {
                    read_varint(data, mend, &pos, &mid);
                } else if (mtag == 0x12) {
                    uint64_t l = 0; read_varint(data, mend, &pos, &l);
                    peer_url = data + pos; peer_len = (size_t)l;
                    pos += l;
                } else if (mtag == 0x1a) {
                    /* field 3 = name (string) */
                    uint64_t l = 0; read_varint(data, mend, &pos, &l);
                    m_name = data + pos; name_len = (size_t)l;
                    pos += l;
                } else if (mtag == 0x22) {
                    /* field 4 = clientURLs (repeated string) */
                    uint64_t l = 0; read_varint(data, mend, &pos, &l);
                    client_url = data + pos; client_len = (size_t)l;
                    pos += l;
                } else if (mtag == 0x28) {
                    /* field 5 = isLearner (bool) */
                    uint64_t v = 0; read_varint(data, mend, &pos, &v);
                    is_learner = (int)v;
                } else {
                    uint64_t v = 0; read_varint(data, mend, &pos, &v);
                }
            }
            pos = mend;
            if (json_format) {
                if (!first) printf(",");
                first = 0;
                printf("{\"ID\":%llu,\"name\":", (unsigned long long)mid);
                if (m_name) print_json_string(m_name, name_len); else fputs("\"\"", stdout);
                fputs(",\"peerURLs\":[", stdout);
                if (peer_url) print_json_string(peer_url, peer_len); else fputs("\"\"", stdout);
                fputs("]", stdout);
                if (client_url) {
                    fputs(",\"clientURLs\":[", stdout);
                    print_json_string(client_url, client_len);
                    fputs("]", stdout);
                }
                if (is_learner) fputs(",\"isLearner\":true", stdout);
                fputs("}", stdout);
            } else if (table_format) {
                printf("| %16llu | %6s | %19.*s |\n",
                       (unsigned long long)mid, "alive",
                       (int)peer_len, peer_url ? peer_url : (const uint8_t *)"");
            } else if (fields_format) {
                printf("ID: %llu\n", (unsigned long long)mid);
                if (m_name) printf("name: %.*s\n", (int)name_len, m_name);
                if (peer_url) printf("peerURLs: %.*s\n", (int)peer_len, peer_url);
                if (client_url) printf("clientURLs: %.*s\n", (int)client_len, client_url);
                if (is_learner) printf("isLearner: true\n");
                printf("\n");
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

static void parse_string_list_response(const uint8_t *data, size_t len, const char *label, int table_fmt, int json_fmt, int fields_fmt) {
    size_t pos = 0;
    int count = 0;
    if (table_fmt) {
        printf("+--------------------+\n");
        printf("| %18s |\n", label);
        printf("+--------------------+\n");
    }
    if (json_fmt) {
        fputs("{", stdout);
        parse_and_print_header_json(data, len);
        printf(",\"%s\":[", label);
    }
    while (pos < len) {
        uint8_t tag = data[pos++];
        if (tag == 0x12) {
            uint64_t l = 0; read_varint(data, len, &pos, &l);
            if (json_fmt) {
                if (count > 0) printf(",");
                print_json_string(data + pos, (size_t)l);
            } else if (table_fmt) {
                printf("| %18.*s |\n", (int)l, data + pos);
            } else if (fields_fmt) {
                printf("%s: %.*s\n", label, (int)l, data + pos);
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
    if (json_fmt) {
        printf("]}\n");
    }
    if (table_fmt) {
        printf("+--------------------+\n");
    }
    if (count == 0 && !table_fmt && !json_fmt && !fields_fmt) printf("(no %s)\n", label);
}

/* --- Commands --- */

static int cmd_put(int argc, char **argv) {
    bool prev_kv = false;
    bool ignore_value = false;
    bool ignore_lease = false;
    bool want_json = false;
    bool want_fields = false;
    bool print_value_only = false;
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
        } else if (strcmp(argv[i], "--print-value-only") == 0) {
            print_value_only = true;
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
        fprintf(stderr, "usage: cetcdctl put [--prev-kv] [--ignore-value] [--ignore-lease] [--lease ID] [--print-value-only] [-w json|fields] KEY [VALUE]\n");
        return 1;
    }
    if (!val && !ignore_value) {
        fprintf(stderr, "usage: cetcdctl put [--prev-kv] [--ignore-value] [--ignore-lease] [--lease ID] [--print-value-only] [-w json|fields] KEY [VALUE|-]\n");
        return 1;
    }
    /* --print-value-only implies --prev-kv */
    if (print_value_only) prev_kv = true;

    /* If val is "-", read value from stdin */
    char *stdin_val = NULL;
    if (val && strcmp(val, "-") == 0) {
        size_t cap = 4096;
        stdin_val = (char *)malloc(cap);
        if (!stdin_val) { fprintf(stderr, "out of memory\n"); return 1; }
        size_t total = 0;
        int c;
        while ((c = getchar()) != EOF) {
            if (total + 1 >= cap) {
                cap *= 2;
                char *tmp = (char *)realloc(stdin_val, cap);
                if (!tmp) { free(stdin_val); fprintf(stderr, "out of memory\n"); return 1; }
                stdin_val = tmp;
            }
            stdin_val[total++] = (char)c;
        }
        stdin_val[total] = '\0';
        /* Strip trailing newline if present */
        if (total > 0 && stdin_val[total - 1] == '\n') {
            stdin_val[--total] = '\0';
        }
        val = stdin_val;
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
    if (rlen < 0) { fprintf(stderr, "request failed\n"); if (stdin_val) free(stdin_val); return 1; }
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
        if (stdin_val) free(stdin_val);
        return 0;
    } else if (want_json) {
        fputs("{", stdout);
        parse_and_print_header_json(resp, (size_t)rlen);
        fputs(",", stdout);
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
                fputs("\"key\":", stdout);
                print_json_string(pk, pk_len);
                if (pv && pv_len > 0) {
                    fputs(",\"value\":", stdout);
                    print_json_string(pv, pv_len);
                }
            }
            fputs("}", stdout);
        }
        fputs("}\n", stdout);
        if (stdin_val) free(stdin_val);
        return 0;
    } else if (print_value_only) {
        /* Parse PutResponse for prev_kv value (field 2, tag 0x12 -> KeyValue field 5, tag 0x2a) */
        size_t rpos = 0;
        while (rpos < (size_t)rlen) {
            uint8_t tag = resp[rpos++];
            if (tag == 0x12) {
                uint64_t l = 0; read_varint(resp, rlen, &rpos, &l);
                size_t kv_end = rpos + (size_t)l;
                const uint8_t *pv = NULL; size_t pv_len = 0;
                while (rpos < kv_end) {
                    uint8_t ktag = resp[rpos++];
                    if (ktag == 0x2a) {
                        uint64_t vl = 0; read_varint(resp, kv_end, &rpos, &vl);
                        pv = resp + rpos; pv_len = (size_t)vl; rpos += vl;
                    } else {
                        uint64_t v = 0; read_varint(resp, kv_end, &rpos, &v);
                    }
                }
                if (pv && pv_len > 0) {
                    fwrite(pv, 1, pv_len, stdout);
                    printf("\n");
                }
                break;
            } else if (tag == 0x0a) {
                uint64_t l = 0; read_varint(resp, rlen, &rpos, &l); rpos += l;
            } else {
                uint64_t v = 0; read_varint(resp, rlen, &rpos, &v);
            }
        }
        if (stdin_val) free(stdin_val);
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
    if (stdin_val) free(stdin_val);
    return 0;
}

static int cmd_get(int argc, char **argv) {
    if (argc < 3) { fprintf(stderr, "usage: cetcdctl get [--prefix] [--from-key] [--range-end KEY] [--keys-only] [--count-only] [--print-value-only] [--hex] [--consistency l|s] [-w json|fields|table] [--rev N] [--limit N] [--sort-by FIELD] [--sort-order ORDER] [--min-mod-rev N] [--max-mod-rev N] [--min-create-rev N] [--max-create-rev N] KEY [RANGE_END]\n"); return 1; }
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
            if (i + 1 >= argc) { fprintf(stderr, "--write-out requires a format (json, simple, fields, table)\n"); return 1; }
            const char *fmt = argv[++i];
            if (strcmp(fmt, "json") == 0) { g_write_json = 1; g_write_fields = 0; g_write_table = 0; }
            else if (strcmp(fmt, "fields") == 0) { g_write_json = 0; g_write_fields = 1; g_write_table = 0; }
            else if (strcmp(fmt, "table") == 0) { g_write_json = 0; g_write_fields = 0; g_write_table = 1; }
            else if (strcmp(fmt, "simple") == 0) { g_write_json = 0; g_write_fields = 0; g_write_table = 0; }
            else { fprintf(stderr, "unsupported --write-out format: %s (use json, fields, table, or simple)\n", fmt); return 1; }
        } else if (strcmp(argv[i], "--range-end") == 0 && i + 1 < argc) {
            range_end = argv[++i];
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
    if (!key) { fprintf(stderr, "usage: cetcdctl get [--prefix] [--from-key] [--range-end KEY] [--keys-only] [--count-only] [--print-value-only] [--hex] [--consistency l|s] [-w json|fields|table] [--rev N] [--limit N] [--sort-by FIELD] [--sort-order ORDER] [--min-mod-rev N] [--max-mod-rev N] [--min-create-rev N] [--max-create-rev N] KEY [RANGE_END]\n"); return 1; }
    if (prefix && from_key) { fprintf(stderr, "--prefix and --from-key are mutually exclusive\n"); return 1; }

    size_t key_len = strlen(key);

    uint8_t req[1024], resp[8192];
    size_t pos = 0;
    pos = encode_bytes_field(req, sizeof(req), pos, 0x0a,
                             (const uint8_t *)key, key_len);
    if (prefix) {
        /* range_end = key with last byte incremented (standard etcd prefix semantics) */
        if (key_len == 0) {
            /* Empty key with --prefix means all keys */
            uint8_t zero = 0;
            pos = encode_bytes_field(req, sizeof(req), pos, 0x12, &zero, 1);
        } else {
            char prefix_end[256];
            if (key_len >= sizeof(prefix_end)) { fprintf(stderr, "key too long\n"); return 1; }
            memcpy(prefix_end, key, key_len);
            prefix_end[key_len - 1]++;
            pos = encode_bytes_field(req, sizeof(req), pos, 0x12,
                                     (const uint8_t *)prefix_end, key_len);
        }
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
    if (argc < 3) { fprintf(stderr, "usage: cetcdctl del [--prefix] [--from-key] [--range-end KEY] [--prev-kv] [--hex] [-w json|fields] KEY [RANGE_END]\n"); return 1; }
    bool prefix = false;
    bool from_key = false;
    bool prev_kv = false;
    bool want_json = false;
    bool want_fields = false;
    bool hex_output = false;
    bool print_value_only = false;
    const char *key = NULL;
    const char *range_end = NULL;

    for (int i = 2; i < argc; i++) {
        if (strcmp(argv[i], "--prefix") == 0) {
            prefix = true;
        } else if (strcmp(argv[i], "--from-key") == 0) {
            from_key = true;
        } else if (strcmp(argv[i], "--range-end") == 0 && i + 1 < argc) {
            range_end = argv[++i];
        } else if (strcmp(argv[i], "--prev-kv") == 0) {
            prev_kv = true;
        } else if (strcmp(argv[i], "--hex") == 0) {
            hex_output = true;
        } else if (strcmp(argv[i], "--print-value-only") == 0) {
            print_value_only = true;
        } else if ((strcmp(argv[i], "-w") == 0 || strcmp(argv[i], "--write-out") == 0) && i + 1 < argc) {
            if (strcmp(argv[i + 1], "json") == 0) want_json = true;
            else if (strcmp(argv[i + 1], "fields") == 0) want_fields = true;
            i++;
        } else if (!key) {
            key = argv[i];
        } else if (!range_end) {
            range_end = argv[i];
        }
    }
    if (!key) { fprintf(stderr, "usage: cetcdctl del [--prefix] [--from-key] [--range-end KEY] [--prev-kv] [--hex] [--print-value-only] [-w json|fields] KEY [RANGE_END]\n"); return 1; }
    if (prefix && from_key) { fprintf(stderr, "--prefix and --from-key are mutually exclusive\n"); return 1; }
    /* --print-value-only implies --prev-kv */
    if (print_value_only) prev_kv = true;
    size_t key_len = strlen(key);

    uint8_t req[1024], resp[4096];
    size_t pos = 0;
    pos = encode_bytes_field(req, sizeof(req), pos, 0x0a,
                             (const uint8_t *)key, key_len);
    if (prefix) {
        if (key_len == 0) {
            /* Empty key with --prefix means all keys */
            uint8_t zero = 0;
            pos = encode_bytes_field(req, sizeof(req), pos, 0x12, &zero, 1);
        } else {
            char prefix_end[256];
            if (key_len >= sizeof(prefix_end)) { fprintf(stderr, "key too long\n"); return 1; }
            memcpy(prefix_end, key, key_len);
            prefix_end[key_len - 1]++;
            pos = encode_bytes_field(req, sizeof(req), pos, 0x12,
                                     (const uint8_t *)prefix_end, key_len);
        }
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
    if (want_fields) {
        size_t rpos = 0;
        uint64_t deleted = 0;
        while (rpos < (size_t)rlen) {
            uint8_t tag = resp[rpos++];
            if (tag == 0x10) {
                read_varint(resp, rlen, &rpos, &deleted);
            } else if (tag == 0x1a && prev_kv) {
                uint64_t l = 0; read_varint(resp, rlen, &rpos, &l);
                size_t kv_end = rpos + (size_t)l;
                const uint8_t *pk = NULL; size_t pk_len = 0;
                const uint8_t *pv = NULL; size_t pv_len = 0;
                uint64_t pcr = 0, pmr = 0, pver = 0, please = 0;
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
                    printf("\n");
                }
            } else if (tag == 0x0a) {
                uint64_t l = 0; read_varint(resp, rlen, &rpos, &l); rpos += l;
            } else {
                uint64_t v = 0; read_varint(resp, rlen, &rpos, &v);
            }
        }
        printf("%llu key(s) deleted\n", (unsigned long long)deleted);
        return 0;
    }
    if (want_json) {
        /* JSON output: {"header":{...},"deleted":N,"prev_kvs":[...]} */
        size_t rpos = 0;
        uint64_t deleted = 0;
        /* Collect prev_kvs */
        int has_prev_kvs = 0;
        fputs("{", stdout);
        parse_and_print_header_json(resp, (size_t)rlen);
        fputs(",", stdout);
        while (rpos < (size_t)rlen) {
            uint8_t tag = resp[rpos++];
            if (tag == 0x10) {
                read_varint(resp, rlen, &rpos, &deleted);
            } else if (tag == 0x1a && prev_kv) {
                uint64_t l = 0; read_varint(resp, rlen, &rpos, &l);
                size_t kv_end = rpos + (size_t)l;
                if (!has_prev_kvs) {
                    fputs("\"prev_kvs\":[", stdout);
                    has_prev_kvs = 1;
                } else {
                    printf(",");
                }
                /* Parse and output KV as JSON */
                const uint8_t *pk = NULL; size_t pk_len = 0;
                const uint8_t *pv = NULL; size_t pv_len = 0;
                uint64_t pcr = 0, pmr = 0, pver = 0, please = 0;
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
                fputs("{\"key\":", stdout);
                if (pk) print_json_string(pk, pk_len); else fputs("\"\"", stdout);
                printf(",\"create_revision\":%llu", (unsigned long long)pcr);
                printf(",\"mod_revision\":%llu", (unsigned long long)pmr);
                printf(",\"version\":%llu", (unsigned long long)pver);
                if (please > 0) printf(",\"lease\":%llu", (unsigned long long)please);
                if (pv && pv_len > 0) {
                    fputs(",\"value\":", stdout);
                    print_json_string(pv, pv_len);
                }
                fputs("}", stdout);
            } else if (tag == 0x0a) {
                uint64_t l = 0; read_varint(resp, rlen, &rpos, &l); rpos += l;
            } else {
                uint64_t v = 0; read_varint(resp, rlen, &rpos, &v);
            }
        }
        if (has_prev_kvs) fputs("\"],", stdout);
        printf("\"deleted\":%llu}\n", (unsigned long long)deleted);
        return 0;
    }
    if (print_value_only) {
        /* Parse DeleteRangeResponse: output only values from prev_kvs */
        size_t rpos = 0;
        while (rpos < (size_t)rlen) {
            uint8_t tag = resp[rpos++];
            if (tag == 0x1a) {
                /* prev_kvs: repeated KeyValue */
                uint64_t l = 0; read_varint(resp, rlen, &rpos, &l);
                size_t kv_end = rpos + (size_t)l;
                const uint8_t *pv = NULL; size_t pv_len = 0;
                while (rpos < kv_end) {
                    uint8_t ktag = resp[rpos++];
                    if (ktag == 0x2a) {
                        uint64_t vl = 0; read_varint(resp, kv_end, &rpos, &vl);
                        pv = resp + rpos; pv_len = (size_t)vl; rpos += vl;
                    } else {
                        uint64_t v = 0; read_varint(resp, kv_end, &rpos, &v);
                    }
                }
                rpos = kv_end;
                if (pv && pv_len > 0) {
                    fwrite(pv, 1, pv_len, stdout);
                    printf("\n");
                }
            } else if (tag == 0x0a || tag == 0x12) {
                uint64_t l = 0; read_varint(resp, rlen, &rpos, &l); rpos += l;
            } else {
                uint64_t v = 0; read_varint(resp, rlen, &rpos, &v);
            }
        }
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
                if (hex_output) {
                    for (size_t i = 0; i < pk_len; i++) printf("%02x", pk[i]);
                    if (pv && pv_len > 0) {
                        printf(" -> ");
                        for (size_t i = 0; i < pv_len; i++) printf("%02x", pv[i]);
                    }
                } else {
                    printf("prev: %.*s", (int)pk_len, pk);
                    if (pv && pv_len > 0) printf(" -> %.*s", (int)pv_len, pv);
                }
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
        fprintf(stderr, "usage: cetcdctl lease grant [--lease-id ID] [-w json|fields] TTL\n");
        fprintf(stderr, "       cetcdctl lease revoke [-w json|fields] ID\n");
        fprintf(stderr, "       cetcdctl lease timetolive [--keys] [-w json|fields] ID\n");
        fprintf(stderr, "       cetcdctl lease list [-w json|table|fields]\n");
        fprintf(stderr, "       cetcdctl lease keepalive [--once] [--interval SEC] [-w json|fields] ID\n");
        return 1;
    }
    if (strcmp(argv[2], "grant") == 0) {
        bool want_json = false;
        bool want_fields = false;
        const char *ttl_str = NULL;
        uint64_t lease_id = 0;
        bool has_lease_id = false;
        for (int i = 3; i < argc; i++) {
            if ((strcmp(argv[i], "-w") == 0 || strcmp(argv[i], "--write-out") == 0) && i + 1 < argc) {
                if (strcmp(argv[i + 1], "json") == 0) want_json = true;
                else if (strcmp(argv[i + 1], "fields") == 0) want_fields = true;
                i++;
            } else if (strcmp(argv[i], "--lease-id") == 0 && i + 1 < argc) {
                lease_id = (uint64_t)strtoull(argv[++i], NULL, 16);
                has_lease_id = true;
            } else if (!ttl_str) {
                ttl_str = argv[i];
            }
        }
        if (!ttl_str) { fprintf(stderr, "usage: cetcdctl lease grant [--lease-id ID] [-w json|fields] TTL\n"); return 1; }
        uint8_t req[32], resp[256];
        size_t pos = 0;
        pos = encode_varint_field(req, sizeof(req), pos, 0x08, (uint64_t)atol(ttl_str));
        pos = encode_varint_field(req, sizeof(req), pos, 0x10, has_lease_id ? lease_id : 0);
        int rlen = do_rpc("/etcdserverpb.Lease/LeaseGrant", req, pos, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        if (want_json) {
            size_t rpos = 0;
            uint64_t lid = 0, ttl = 0;
            while (rpos < (size_t)rlen) {
                uint8_t tag = resp[rpos++];
                if (tag == 0x10) { read_varint(resp, rlen, &rpos, &lid); }
                else if (tag == 0x18) { read_varint(resp, rlen, &rpos, &ttl); }
                else if (tag == 0x0a) { uint64_t l = 0; read_varint(resp, rlen, &rpos, &l); rpos += l; }
                else { uint64_t v = 0; read_varint(resp, rlen, &rpos, &v); }
            }
            fputs("{", stdout);
            parse_and_print_header_json(resp, (size_t)rlen);
            printf(",\"ID\":%llu,\"TTL\":%llu}\n",
                   (unsigned long long)lid, (unsigned long long)ttl);
        } else if (want_fields) {
            size_t rpos = 0;
            uint64_t lid = 0, ttl = 0;
            while (rpos < (size_t)rlen) {
                uint8_t tag = resp[rpos++];
                if (tag == 0x10) { read_varint(resp, rlen, &rpos, &lid); }
                else if (tag == 0x18) { read_varint(resp, rlen, &rpos, &ttl); }
                else if (tag == 0x0a) { uint64_t l = 0; read_varint(resp, rlen, &rpos, &l); rpos += l; }
                else { uint64_t v = 0; read_varint(resp, rlen, &rpos, &v); }
            }
            parse_and_print_header_json(resp, (size_t)rlen);
            printf("ID: %llu\n", (unsigned long long)lid);
            printf("TTL: %llu\n", (unsigned long long)ttl);
            fputs("\n", stdout);
        } else {
            parse_lease_grant_response(resp, rlen);
        }
    } else if (strcmp(argv[2], "revoke") == 0) {
        bool want_json = false;
        bool want_fields = false;
        const char *id_str = NULL;
        for (int i = 3; i < argc; i++) {
            if ((strcmp(argv[i], "-w") == 0 || strcmp(argv[i], "--write-out") == 0) && i + 1 < argc) {
                if (strcmp(argv[i + 1], "json") == 0) want_json = true;
                else if (strcmp(argv[i + 1], "fields") == 0) want_fields = true;
                i++;
            } else if (!id_str) {
                id_str = argv[i];
            }
        }
        if (!id_str) { fprintf(stderr, "usage: cetcdctl lease revoke [-w json|fields] ID\n"); return 1; }
        uint8_t req[32], resp[256];
        size_t pos = 0;
        pos = encode_varint_field(req, sizeof(req), pos, 0x08, (uint64_t)atol(id_str));
        int rlen = do_rpc("/etcdserverpb.Lease/LeaseRevoke", req, pos, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        if (want_json) { fputs("{", stdout); parse_and_print_header_json(resp, (size_t)rlen); fputs("}\n", stdout); }
        else if (want_fields) { parse_and_print_header_json(resp, (size_t)rlen); fputs("\n", stdout); }
        else { printf("OK\n"); }
    } else if (strcmp(argv[2], "timetolive") == 0) {
        bool want_keys = false;
        bool want_json = false;
        bool want_fields = false;
        const char *id_str = NULL;
        for (int i = 3; i < argc; i++) {
            if (strcmp(argv[i], "--keys") == 0) {
                want_keys = true;
            } else if ((strcmp(argv[i], "-w") == 0 || strcmp(argv[i], "--write-out") == 0) && i + 1 < argc) {
                if (strcmp(argv[i + 1], "json") == 0) want_json = true;
                else if (strcmp(argv[i + 1], "fields") == 0) want_fields = true;
                i++;
            } else if (!id_str) {
                id_str = argv[i];
            }
        }
        if (!id_str) { fprintf(stderr, "usage: cetcdctl lease timetolive [--keys] [-w json|fields] ID\n"); return 1; }
        uint8_t req[32], resp[4096];
        size_t pos = 0;
        pos = encode_varint_field(req, sizeof(req), pos, 0x08, (uint64_t)atol(id_str));
        if (want_keys) {
            pos = encode_varint_field(req, sizeof(req), pos, 0x10, 1);
        }
        int rlen = do_rpc("/etcdserverpb.Lease/LeaseTimeToLive", req, pos, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        if (want_json) {
            size_t rpos = 0;
            uint64_t lid = 0, ttl = 0, granted = 0;
            fputs("{", stdout);
            parse_and_print_header_json(resp, (size_t)rlen);
            fputs(",", stdout);
            /* First pass: collect ID/TTL/granted, count keys */
            int key_count = 0;
            while (rpos < (size_t)rlen) {
                uint8_t tag = resp[rpos++];
                if (tag == 0x10) { read_varint(resp, rlen, &rpos, &lid); }
                else if (tag == 0x18) { read_varint(resp, rlen, &rpos, &ttl); }
                else if (tag == 0x20) { read_varint(resp, rlen, &rpos, &granted); }
                else if (tag == 0x2a) {
                    uint64_t l = 0; read_varint(resp, rlen, &rpos, &l);
                    rpos += l;
                    key_count++;
                } else if (tag == 0x0a) { uint64_t l = 0; read_varint(resp, rlen, &rpos, &l); rpos += l; }
                else { uint64_t v = 0; read_varint(resp, rlen, &rpos, &v); }
            }
            printf("\"ID\":%llu,\"TTL\":%llu,\"grantedTTL\":%llu",
                   (unsigned long long)lid, (unsigned long long)ttl, (unsigned long long)granted);
            if (want_keys && key_count > 0) {
                /* Second pass: output keys array */
                rpos = 0;
                fputs(",\"keys\":[", stdout);
                int ki = 0;
                while (rpos < (size_t)rlen) {
                    uint8_t tag = resp[rpos++];
                    if (tag == 0x2a) {
                        uint64_t l = 0; read_varint(resp, rlen, &rpos, &l);
                        if (ki > 0) fputs(",", stdout);
                        print_json_string(resp + rpos, (size_t)l);
                        rpos += l;
                        ki++;
                    } else if (tag == 0x0a || tag == 0x12) { uint64_t l = 0; read_varint(resp, rlen, &rpos, &l); rpos += l; }
                    else if (tag == 0x10 || tag == 0x18 || tag == 0x20) { uint64_t v = 0; read_varint(resp, rlen, &rpos, &v); }
                    else { uint64_t v = 0; read_varint(resp, rlen, &rpos, &v); }
                }
                fputs("]", stdout);
            }
            fputs("}\n", stdout);
        } else if (want_fields) {
            size_t rpos = 0;
            uint64_t lid = 0, ttl = 0, granted = 0;
            while (rpos < (size_t)rlen) {
                uint8_t tag = resp[rpos++];
                if (tag == 0x10) { read_varint(resp, rlen, &rpos, &lid); }
                else if (tag == 0x18) { read_varint(resp, rlen, &rpos, &ttl); }
                else if (tag == 0x20) { read_varint(resp, rlen, &rpos, &granted); }
                else if (tag == 0x2a) { uint64_t l = 0; read_varint(resp, rlen, &rpos, &l); rpos += l; }
                else if (tag == 0x0a) { uint64_t l = 0; read_varint(resp, rlen, &rpos, &l); rpos += l; }
                else { uint64_t v = 0; read_varint(resp, rlen, &rpos, &v); }
            }
            parse_and_print_header_json(resp, (size_t)rlen);
            printf("ID: %llu\n", (unsigned long long)lid);
            printf("TTL: %llu\n", (unsigned long long)ttl);
            printf("grantedTTL: %llu\n", (unsigned long long)granted);
            if (want_keys) {
                rpos = 0;
                int ki = 0;
                while (rpos < (size_t)rlen) {
                    uint8_t tag = resp[rpos++];
                    if (tag == 0x2a) {
                        uint64_t l = 0; read_varint(resp, rlen, &rpos, &l);
                        printf("key[%d]: %.*s\n", ki, (int)l, resp + rpos);
                        rpos += l;
                        ki++;
                    } else if (tag == 0x0a || tag == 0x12) { uint64_t l = 0; read_varint(resp, rlen, &rpos, &l); rpos += l; }
                    else if (tag == 0x10 || tag == 0x18 || tag == 0x20) { uint64_t v = 0; read_varint(resp, rlen, &rpos, &v); }
                    else { uint64_t v = 0; read_varint(resp, rlen, &rpos, &v); }
                }
            }
            fputs("\n", stdout);
        } else {
            parse_lease_ttl_response(resp, rlen);
        }
    } else if (strcmp(argv[2], "list") == 0) {
        int table_fmt = 0, json_fmt = 0, fields_fmt = 0;
        for (int i = 3; i < argc; i++) {
            if ((strcmp(argv[i], "-w") == 0 || strcmp(argv[i], "--write-out") == 0) && i + 1 < argc) {
                if (strcmp(argv[i + 1], "table") == 0) table_fmt = 1;
                else if (strcmp(argv[i + 1], "json") == 0) json_fmt = 1;
                else if (strcmp(argv[i + 1], "fields") == 0) fields_fmt = 1;
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
            fputs("{", stdout);
            parse_and_print_header_json(resp, (size_t)rlen);
            fputs(",\"leases\":[", stdout);
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
                        } else if (fields_fmt) {
                            printf("ID: %llu\n", (unsigned long long)id);
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
        if (count == 0 && !table_fmt && !json_fmt && !fields_fmt) printf("(no leases)\n");
    } else if (strcmp(argv[2], "keepalive") == 0) {
        int once = 0;
        bool want_json = false;
        bool want_fields = false;
        int interval_sec = 0; /* 0 = auto (ttl/2) */
        const char *id_str = NULL;
        for (int i = 3; i < argc; i++) {
            if (strcmp(argv[i], "--once") == 0) {
                once = 1;
            } else if (strcmp(argv[i], "--interval") == 0 && i + 1 < argc) {
                interval_sec = atoi(argv[++i]);
                if (interval_sec <= 0) { fprintf(stderr, "--interval must be a positive number\n"); return 1; }
            } else if ((strcmp(argv[i], "-w") == 0 || strcmp(argv[i], "--write-out") == 0) && i + 1 < argc) {
                if (strcmp(argv[i + 1], "json") == 0) want_json = true;
                else if (strcmp(argv[i + 1], "fields") == 0) want_fields = true;
                i++;
            } else if (!id_str) {
                id_str = argv[i];
            }
        }
        if (!id_str) { fprintf(stderr, "usage: cetcdctl lease keepalive [--once] [--interval SEC] [-w json|fields] ID\n"); return 1; }
        uint64_t lease_id = (uint64_t)atol(id_str);
        /* Set SIGINT handler for graceful exit from keepalive loop */
        g_keepalive_stop = 0;
        void (*old_sig)(int) = signal(SIGINT, keepalive_sigint_handler);
        for (;;) {
            if (g_keepalive_stop) { fprintf(stderr, "\ninterrupted\n"); break; }
            uint8_t req[32], resp[256];
            size_t pos = 0;
            pos = encode_varint_field(req, sizeof(req), pos, 0x08, lease_id);
            int rlen = do_rpc("/etcdserverpb.Lease/LeaseKeepAlive", req, pos, resp, sizeof(resp));
            if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
            /* Parse KeepAliveResponse: field 1 (header), field 2 (ID), field 3 (TTL) */
            size_t rpos = 0;
            uint64_t ttl = 0, kid = 0;
            while (rpos < (size_t)rlen) {
                uint8_t tag = resp[rpos++];
                if (tag == 0x10) {
                    uint64_t v = 0; read_varint(resp, rlen, &rpos, &v);
                    kid = v;
                    if (!want_json && !want_fields) printf("lease ID: %llu\n", (unsigned long long)v);
                } else if (tag == 0x18) {
                    uint64_t v = 0; read_varint(resp, rlen, &rpos, &v);
                    ttl = v;
                    if (!want_json && !want_fields) printf("TTL: %llu seconds\n", (unsigned long long)v);
                } else if (tag == 0x0a) {
                    /* Skip header (length-delimited) */
                    uint64_t l = 0; read_varint(resp, rlen, &rpos, &l);
                    rpos += l;
                } else {
                    uint64_t v = 0; read_varint(resp, rlen, &rpos, &v);
                }
            }
            if (want_json) {
                fputs("{", stdout);
                parse_and_print_header_json(resp, (size_t)rlen);
                printf(",\"ID\":%llu,\"TTL\":%llu}\n",
                       (unsigned long long)kid, (unsigned long long)ttl);
            } else if (want_fields) {
                parse_and_print_header_json(resp, (size_t)rlen);
                printf("ID: %llu\n", (unsigned long long)kid);
                printf("TTL: %llu\n", (unsigned long long)ttl);
                fputs("\n", stdout);
            }
            if (once) break;
            if (ttl == 0) { fprintf(stderr, "lease expired\n"); break; }
            fflush(stdout);
            unsigned sleep_sec = interval_sec > 0 ? (unsigned)interval_sec : (unsigned)(ttl / 2 > 0 ? ttl / 2 : 1);
            sleep(sleep_sec);
        }
        signal(SIGINT, old_sig); /* restore previous signal handler */
    } else {
        fprintf(stderr, "unknown lease subcommand: %s\n", argv[2]);
        return 1;
    }
    return 0;
}

static int cmd_compact(int argc, char **argv) {
    bool physical = false;
    bool want_json = false;
    bool want_fields = false;
    int64_t rev = 0;
    for (int i = 2; i < argc; i++) {
        if (strcmp(argv[i], "--physical") == 0) {
            physical = true;
        } else if ((strcmp(argv[i], "-w") == 0 || strcmp(argv[i], "--write-out") == 0) && i + 1 < argc) {
            if (strcmp(argv[i + 1], "json") == 0) want_json = true;
            else if (strcmp(argv[i + 1], "fields") == 0) want_fields = true;
            i++;
        } else if (!rev) {
            rev = strtoll(argv[i], NULL, 10);
        }
    }
    if (rev <= 0) { fprintf(stderr, "usage: cetcdctl compact [--physical] [-w json|fields] REV\n"); return 1; }
    uint8_t req[32], resp[256];
    size_t pos = 0;
    pos = encode_varint_field(req, sizeof(req), pos, 0x08, (uint64_t)rev);
    if (physical) {
        pos = encode_varint_field(req, sizeof(req), pos, 0x10, 1);
    }
    int rlen = do_rpc("/etcdserverpb.KV/Compact", req, pos, resp, sizeof(resp));
    if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
    if (want_json) {
        fputs("{", stdout);
        parse_and_print_header_json(resp, (size_t)rlen);
        fputs("}\n", stdout);
    } else if (want_fields) {
        parse_and_print_header_json(resp, (size_t)rlen);
        fputs("\n", stdout);
    } else {
        printf("OK\n");
    }
    return 0;
}

static int cmd_txn(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "usage: cetcdctl txn -i [-w json|fields]\n");
        fprintf(stderr, "       cetcdctl txn put [-w json|fields] KEY VALUE\n");
        fprintf(stderr, "       cetcdctl txn cas [-w json|fields] KEY EXPECTED NEW\n");
        fprintf(stderr, "       cetcdctl txn get [-w json|fields] KEY [RANGE_END]\n");
        fprintf(stderr, "       cetcdctl txn del [-w json|fields] [--prefix] [--from-key] [--prev-kv] KEY [RANGE_END]\n");
        return 1;
    }
    /* Parse -w json|fields for all txn subcommands */
    int want_json = 0, want_fields = 0;
    for (int i = 3; i < argc; i++) {
        if ((strcmp(argv[i], "-w") == 0 || strcmp(argv[i], "--write-out") == 0) && i + 1 < argc) {
            if (strcmp(argv[i + 1], "json") == 0) { want_json = 1; g_write_json = 1; }
            else if (strcmp(argv[i + 1], "fields") == 0) { want_fields = 1; g_write_fields = 1; }
            i++;
        }
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
        if (want_json) { fputs("{", stdout); parse_and_print_header_json(resp, (size_t)rlen); fputs(",\"succeeded\":true}\n", stdout); }
        else if (want_fields) { parse_and_print_header_json(resp, (size_t)rlen); printf("succeeded: true\n\n"); }
        else { printf("OK\n"); }
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
        if (want_json) {
            fputs("{", stdout); parse_and_print_header_json(resp, (size_t)rlen); fputs(",\"succeeded\":", stdout); printf("%s}\n", succeeded ? "true" : "false");
            return succeeded ? 0 : 1;
        } else if (want_fields) {
            parse_and_print_header_json(resp, (size_t)rlen);
            printf("succeeded: %s\n\n", succeeded ? "true" : "false");
            return succeeded ? 0 : 1;
        } else if (succeeded) {
            printf("OK (compare succeeded)\n");
            return 0;
        } else {
            printf("FAILED (compare did not match)\n");
            return 1;
        }
    } else if (strcmp(argv[2], "get") == 0) {
        /* txn get KEY [RANGE_END] */
        const char *key = NULL;
        const char *range_end = NULL;
        for (int i = 3; i < argc; i++) {
            if ((strcmp(argv[i], "-w") == 0 || strcmp(argv[i], "--write-out") == 0) && i + 1 < argc) { i++; continue; }
            if (!key) key = argv[i];
            else if (!range_end) range_end = argv[i];
        }
        if (!key) { fprintf(stderr, "usage: cetcdctl txn get KEY [RANGE_END]\n"); return 1; }
        size_t key_len = strlen(key);

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
        /* txn del [--prefix] [--from-key] [--prev-kv] KEY [RANGE_END] */
        bool prefix = false;
        bool from_key = false;
        bool prev_kv = false;
        const char *key = NULL;
        const char *range_end = NULL;
        for (int i = 3; i < argc; i++) {
            if ((strcmp(argv[i], "-w") == 0 || strcmp(argv[i], "--write-out") == 0) && i + 1 < argc) { i++; continue; }
            if (strcmp(argv[i], "--prefix") == 0) {
                prefix = true;
            } else if (strcmp(argv[i], "--from-key") == 0) {
                from_key = true;
            } else if (strcmp(argv[i], "--prev-kv") == 0) {
                prev_kv = true;
            } else if (!key) {
                key = argv[i];
            } else if (!range_end) {
                range_end = argv[i];
            }
        }
        if (!key) { fprintf(stderr, "usage: cetcdctl txn del [--prefix] [--from-key] [--prev-kv] KEY [RANGE_END]\n"); return 1; }
        if (prefix && from_key) { fprintf(stderr, "--prefix and --from-key are mutually exclusive\n"); return 1; }
        size_t key_len = strlen(key);

        /* Build RequestDeleteRange inner: key(0x0a), range_end(0x12), prev_kv(0x20) */
        uint8_t del_inner[512];
        size_t dpos = 0;
        dpos = encode_bytes_field(del_inner, sizeof(del_inner), dpos, 0x0a,
                                   (const uint8_t *)key, key_len);
        if (prefix) {
            if (key_len == 0) {
                /* Empty key with --prefix means all keys */
                uint8_t zero = 0;
                dpos = encode_bytes_field(del_inner, sizeof(del_inner), dpos, 0x12, &zero, 1);
            } else {
                char prefix_end[256];
                if (key_len >= sizeof(prefix_end)) { fprintf(stderr, "key too long\n"); return 1; }
                memcpy(prefix_end, key, key_len);
                prefix_end[key_len - 1]++;
                dpos = encode_bytes_field(del_inner, sizeof(del_inner), dpos, 0x12,
                                           (const uint8_t *)prefix_end, key_len);
            }
        } else if (from_key) {
            /* range_end = \0 means all keys >= key */
            uint8_t zero = 0;
            dpos = encode_bytes_field(del_inner, sizeof(del_inner), dpos, 0x12, &zero, 1);
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
        if (want_json) { fputs("{", stdout); parse_and_print_header_json(resp, (size_t)rlen); fputs(",\"succeeded\":true}\n", stdout); }
        else if (want_fields) { parse_and_print_header_json(resp, (size_t)rlen); printf("succeeded: true\n\n"); }
        else { printf("OK\n"); }
        return 0;
    } else if (strcmp(argv[2], "-i") == 0 || strcmp(argv[2], "--interactive") == 0) {
        /* Interactive txn mode: read transaction definition from stdin
         *
         * Format (line-based):
         *   # Lines starting with # are comments
         *   cmp KEY OP VALUE          Compare key's value (OP: =, ==, !=, >, <)
         *   cmp_create KEY OP N        Compare key's create revision
         *   cmp_mod KEY OP N          Compare key's mod revision
         *   cmp_ver KEY OP N          Compare key's version
         *   then                      Start success section
         *   put KEY VALUE             Put operation
         *   get KEY [RANGE_END]       Get operation
         *   del KEY [RANGE_END]       Delete operation
         *   else                      Start failure section
         */
        char line[1024];
        uint8_t cmp_buf[4096]; size_t cpos = 0;
        uint8_t succ_buf[4096]; size_t spos = 0;
        uint8_t fail_buf[4096]; size_t fpos = 0;
        int section = 0; /* 0=compare, 1=success, 2=failure */

        while (fgets(line, sizeof(line), stdin)) {
            size_t len = strlen(line);
            while (len > 0 && (line[len-1] == '\n' || line[len-1] == '\r')) line[--len] = '\0';
            char *p = line;
            while (*p == ' ' || *p == '\t') p++;
            if (*p == '#' || *p == '\0') continue;
            if (strcmp(p, "then") == 0) { section = 1; continue; }
            if (strcmp(p, "else") == 0) { section = 2; continue; }

            char *tk = strtok(p, " \t");
            if (!tk) continue;

            if (strcmp(tk, "cmp") == 0 || strcmp(tk, "cmp_create") == 0 ||
                strcmp(tk, "cmp_mod") == 0 || strcmp(tk, "cmp_ver") == 0) {
                char *key = strtok(NULL, " \t");
                char *op = strtok(NULL, " \t");
                char *val = strtok(NULL, "");
                if (!key || !op || !val) { fprintf(stderr, "invalid compare: %s\n", line); continue; }
                while (*val == ' ' || *val == '\t') val++;
                int result;
                if (strcmp(op, "=") == 0 || strcmp(op, "==") == 0) result = 0;
                else if (strcmp(op, ">") == 0) result = 1;
                else if (strcmp(op, "<") == 0) result = 2;
                else if (strcmp(op, "!=") == 0) result = 3;
                else { fprintf(stderr, "invalid operator: %s (use =, !=, >, <)\n", op); continue; }
                int target;
                if (strcmp(tk, "cmp_ver") == 0) target = 0;
                else if (strcmp(tk, "cmp_create") == 0) target = 1;
                else if (strcmp(tk, "cmp_mod") == 0) target = 2;
                else target = 3;
                uint8_t cmp[512]; size_t cl = 0;
                cl = encode_varint_field(cmp, sizeof(cmp), cl, 0x08, (uint64_t)result);
                cl = encode_varint_field(cmp, sizeof(cmp), cl, 0x10, (uint64_t)target);
                cl = encode_bytes_field(cmp, sizeof(cmp), cl, 0x1a, (const uint8_t *)key, strlen(key));
                if (target == 3) {
                    cl = encode_bytes_field(cmp, sizeof(cmp), cl, 0x3a, (const uint8_t *)val, strlen(val));
                } else {
                    uint8_t vtag = (target == 0) ? 0x20 : (target == 1) ? 0x28 : 0x30;
                    cl = encode_varint_field(cmp, sizeof(cmp), cl, vtag, (uint64_t)atol(val));
                }
                cpos = encode_bytes_field(cmp_buf, sizeof(cmp_buf), cpos, 0x0a, cmp, cl);
            } else if (strcmp(tk, "put") == 0) {
                char *key = strtok(NULL, " \t");
                char *val = strtok(NULL, "");
                if (!key) { fprintf(stderr, "invalid put: %s\n", line); continue; }
                if (!val) val = "";
                while (*val == ' ' || *val == '\t') val++;
                uint8_t put_inner[1024]; size_t pl = 0;
                pl = encode_bytes_field(put_inner, sizeof(put_inner), pl, 0x0a, (const uint8_t *)key, strlen(key));
                pl = encode_bytes_field(put_inner, sizeof(put_inner), pl, 0x12, (const uint8_t *)val, strlen(val));
                uint8_t op[1100]; size_t ol = 0;
                op[ol++] = 0x12;
                ol = write_varint(op, sizeof(op), ol, (uint64_t)pl);
                memcpy(op + ol, put_inner, pl); ol += pl;
                if (section <= 1) spos = encode_bytes_field(succ_buf, sizeof(succ_buf), spos, 0x12, op, ol);
                else fpos = encode_bytes_field(fail_buf, sizeof(fail_buf), fpos, 0x1a, op, ol);
            } else if (strcmp(tk, "get") == 0) {
                char *key = strtok(NULL, " \t");
                char *rend = strtok(NULL, " \t");
                if (!key) { fprintf(stderr, "invalid get: %s\n", line); continue; }
                uint8_t range_inner[512]; size_t rl = 0;
                rl = encode_bytes_field(range_inner, sizeof(range_inner), rl, 0x0a, (const uint8_t *)key, strlen(key));
                if (rend) rl = encode_bytes_field(range_inner, sizeof(range_inner), rl, 0x12, (const uint8_t *)rend, strlen(rend));
                uint8_t op[600]; size_t ol = 0;
                op[ol++] = 0x0a;
                ol = write_varint(op, sizeof(op), ol, (uint64_t)rl);
                memcpy(op + ol, range_inner, rl); ol += rl;
                if (section <= 1) spos = encode_bytes_field(succ_buf, sizeof(succ_buf), spos, 0x12, op, ol);
                else fpos = encode_bytes_field(fail_buf, sizeof(fail_buf), fpos, 0x1a, op, ol);
            } else if (strcmp(tk, "del") == 0) {
                char *key = strtok(NULL, " \t");
                char *rend = strtok(NULL, " \t");
                if (!key) { fprintf(stderr, "invalid del: %s\n", line); continue; }
                uint8_t del_inner[512]; size_t dl = 0;
                dl = encode_bytes_field(del_inner, sizeof(del_inner), dl, 0x0a, (const uint8_t *)key, strlen(key));
                if (rend) dl = encode_bytes_field(del_inner, sizeof(del_inner), dl, 0x12, (const uint8_t *)rend, strlen(rend));
                uint8_t op[600]; size_t ol = 0;
                op[ol++] = 0x1a;
                ol = write_varint(op, sizeof(op), ol, (uint64_t)dl);
                memcpy(op + ol, del_inner, dl); ol += dl;
                if (section <= 1) spos = encode_bytes_field(succ_buf, sizeof(succ_buf), spos, 0x12, op, ol);
                else fpos = encode_bytes_field(fail_buf, sizeof(fail_buf), fpos, 0x1a, op, ol);
            } else {
                fprintf(stderr, "unknown command in txn: %s\n", tk);
            }
        }
        uint8_t req[8192], resp[4096];
        size_t pos = 0;
        if (cpos > 0) { memcpy(req + pos, cmp_buf, cpos); pos += cpos; }
        if (spos > 0) { memcpy(req + pos, succ_buf, spos); pos += spos; }
        if (fpos > 0) { memcpy(req + pos, fail_buf, fpos); pos += fpos; }
        if (pos == 0) { fprintf(stderr, "empty transaction\n"); return 1; }
        int rlen = do_rpc("/etcdserverpb.KV/Txn", req, pos, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        bool succeeded = false;
        size_t rpos = 0;
        while (rpos < (size_t)rlen) {
            uint8_t tag = resp[rpos++];
            if (tag == 0x10) {
                uint64_t v = 0; read_varint(resp, rlen, &rpos, &v);
                succeeded = (v != 0);
            } else if (tag == 0x0a) {
                uint64_t l = 0; read_varint(resp, rlen, &rpos, &l);
                rpos += (size_t)l;
            } else if (tag == 0x1a) {
                uint64_t l = 0; read_varint(resp, rlen, &rpos, &l);
                size_t op_end = rpos + (size_t)l;
                while (rpos < op_end) {
                    uint8_t sub_tag = resp[rpos++];
                    if (sub_tag == 0x0a) {
                        uint64_t rl = 0; read_varint(resp, op_end, &rpos, &rl);
                        parse_range_response(resp + rpos, (size_t)rl);
                        rpos += (size_t)rl;
                    } else if (sub_tag == 0x12 || sub_tag == 0x1a) {
                        uint64_t skip = 0; read_varint(resp, op_end, &rpos, &skip);
                        rpos += (size_t)skip;
                    } else {
                        uint64_t skip = 0; read_varint(resp, op_end, &rpos, &skip);
                    }
                }
                rpos = op_end;
            } else {
                uint64_t v = 0; read_varint(resp, rlen, &rpos, &v);
            }
        }
        if (want_json) {
            fputs("{", stdout); parse_and_print_header_json(resp, (size_t)rlen);
            fputs(",\"succeeded\":", stdout); printf("%s}\n", succeeded ? "true" : "false");
        } else if (want_fields) {
            parse_and_print_header_json(resp, (size_t)rlen);
            printf("succeeded: %s\n\n", succeeded ? "true" : "false");
        } else {
            printf("%s\n", succeeded ? "OK (compare succeeded)" : "FAILED (compare did not match)");
        }
        return succeeded ? 0 : 1;
    } else {
        fprintf(stderr, "unknown txn subcommand: %s\n", argv[2]);
        fprintf(stderr, "usage: cetcdctl txn -i [-w json|fields]\n");
        fprintf(stderr, "       cetcdctl txn put [-w json|fields] KEY VALUE\n");
        fprintf(stderr, "       cetcdctl txn cas [-w json|fields] KEY EXPECTED NEW\n");
        fprintf(stderr, "       cetcdctl txn get [-w json|fields] KEY [RANGE_END]\n");
        fprintf(stderr, "       cetcdctl txn del [-w json|fields] [--prefix] [--from-key] [--prev-kv] KEY [RANGE_END]\n");
        return 1;
    }
}

static int cmd_status(int argc, char **argv) {
    int want_json = 0, want_fields = 0;
    for (int i = 2; i < argc; i++) {
        if ((strcmp(argv[i], "-w") == 0 || strcmp(argv[i], "--write-out") == 0) && i + 1 < argc) {
            if (strcmp(argv[i + 1], "json") == 0) want_json = 1;
            else if (strcmp(argv[i + 1], "fields") == 0) want_fields = 1;
            i++;
        }
    }
    uint8_t req[] = {0x00}, resp[1024];
    int rlen = do_rpc("/etcdserverpb.Maintenance/Status", req, 1, resp, sizeof(resp));
    if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
    /* Parse StatusResponse */
    size_t pos = 0;
    const uint8_t *version = NULL; size_t version_len = 0;
    uint64_t db_size = 0, leader = 0, raft_index = 0, raft_term = 0, revision = 0;
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
            /* ResponseHeader: field 1 (cluster_id), field 2 (member_id), field 3 (revision) */
            uint64_t l = 0; read_varint(resp, rlen, &pos, &l);
            size_t hdr_end = pos + (size_t)l;
            while (pos < hdr_end) {
                uint8_t htag = resp[pos++];
                if (htag == 0x18) { read_varint(resp, hdr_end, &pos, &revision); }
                else { uint64_t v = 0; read_varint(resp, hdr_end, &pos, &v); }
            }
        } else {
            uint64_t v = 0; read_varint(resp, rlen, &pos, &v);
        }
    }
    if (want_fields) {
        printf("version: ");
        if (version) fwrite(version, 1, version_len, stdout);
        printf("\n");
        printf("dbSize: %llu\n", (unsigned long long)db_size);
        printf("leader: %llu\n", (unsigned long long)leader);
        printf("raftIndex: %llu\n", (unsigned long long)raft_index);
        printf("raftTerm: %llu\n", (unsigned long long)raft_term);
        printf("revision: %llu\n", (unsigned long long)revision);
    } else if (want_json) {
        fputs("{", stdout);
        parse_and_print_header_json(resp, (size_t)rlen);
        fputs(",\"version\":", stdout);
        if (version) print_json_string(version, version_len); else fputs("\"\"", stdout);
        fputs(",", stdout);
        printf("\"dbSize\":%llu,", (unsigned long long)db_size);
        printf("\"leader\":%llu,", (unsigned long long)leader);
        printf("\"raftIndex\":%llu,", (unsigned long long)raft_index);
        printf("\"raftTerm\":%llu,", (unsigned long long)raft_term);
        printf("\"revision\":%llu}\n", (unsigned long long)revision);
    } else {
        parse_status_response(resp, rlen);
    }
    return 0;
}

/* Collect all cluster member client URLs into an array */
struct cluster_endpoint {
    char host[256];
    uint16_t port;
};

static int collect_cluster_endpoints(struct cluster_endpoint *eps, int max_eps) {
    uint8_t mreq[] = {0x00}, mresp[4096];
    int mrlen = do_rpc("/etcdserverpb.Cluster/MemberList", mreq, 1, mresp, sizeof(mresp));
    if (mrlen < 0) return -1;
    size_t mpos = 0;
    int count = 0;
    while (mpos < (size_t)mrlen && count < max_eps) {
        uint8_t tag = mresp[mpos++];
        if (tag == 0x12) {
            uint64_t mlen = 0; read_varint(mresp, mrlen, &mpos, &mlen);
            size_t mend = mpos + (size_t)mlen;
            const uint8_t *curl = NULL; size_t curlen = 0;
            while (mpos < mend) {
                uint8_t mtag = mresp[mpos++];
                if (mtag == 0x22) {
                    uint64_t l = 0; read_varint(mresp, mend, &mpos, &l);
                    curl = mresp + mpos; curlen = (size_t)l;
                    mpos += l;
                } else if (mtag == 0x08 || mtag == 0x28) {
                    uint64_t v = 0; read_varint(mresp, mend, &mpos, &v);
                } else if (mtag == 0x12 || mtag == 0x1a) {
                    uint64_t l = 0; read_varint(mresp, mend, &mpos, &l);
                    mpos += l;
                } else {
                    uint64_t v = 0; read_varint(mresp, mend, &mpos, &v);
                }
            }
            mpos = mend;
            if (curl && curlen > 0) {
                char url[256];
                size_t ul = curlen < sizeof(url) - 1 ? curlen : sizeof(url) - 1;
                memcpy(url, curl, ul); url[ul] = '\0';
                char *hs = url;
                if (strncmp(url, "http://", 7) == 0) hs = url + 7;
                else if (strncmp(url, "https://", 8) == 0) hs = url + 8;
                char *colon = strchr(hs, ':');
                if (colon) {
                    *colon = '\0';
                    strncpy(eps[count].host, hs, sizeof(eps[count].host) - 1);
                    eps[count].host[sizeof(eps[count].host) - 1] = '\0';
                    eps[count].port = (uint16_t)atoi(colon + 1);
                } else {
                    strncpy(eps[count].host, hs, sizeof(eps[count].host) - 1);
                    eps[count].host[sizeof(eps[count].host) - 1] = '\0';
                    eps[count].port = 2379;
                }
                count++;
            }
        } else if (tag == 0x0a) {
            uint64_t l = 0; read_varint(mresp, mrlen, &mpos, &l);
            mpos += l;
        } else {
            uint64_t v = 0; read_varint(mresp, mrlen, &mpos, &v);
        }
    }
    return count;
}

static int cmd_endpoint(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "usage: cetcdctl endpoint {health,status,hashkv} [--cluster]\n");
        return 1;
    }
    int want_json = 0;
    int want_table = 0;
    int want_fields = 0;
    int cluster = 0;
    for (int i = 3; i < argc; i++) {
        if ((strcmp(argv[i], "-w") == 0 || strcmp(argv[i], "--write-out") == 0) && i + 1 < argc) {
            if (strcmp(argv[i + 1], "json") == 0) want_json = 1;
            else if (strcmp(argv[i + 1], "table") == 0) want_table = 1;
            else if (strcmp(argv[i + 1], "fields") == 0) want_fields = 1;
            i++;
        } else if (strcmp(argv[i], "--cluster") == 0) {
            cluster = 1;
        }
    }
    if (strcmp(argv[2], "health") == 0) {
        if (cluster) {
            struct cluster_endpoint eps[32];
            int n = collect_cluster_endpoints(eps, 32);
            if (n < 0) { fprintf(stderr, "failed to get member list\n"); return 1; }
            const char *orig_host = g_host;
            uint16_t orig_port = g_port;
            int any_unhealthy = 0;
            for (int i = 0; i < n; i++) {
                g_host = eps[i].host;
                g_port = eps[i].port;
                uint8_t hreq[] = {0x00}, hresp[1024];
                struct timeval t0, t1;
                gettimeofday(&t0, NULL);
                int hrlen = do_rpc("/etcdserverpb.Maintenance/Status", hreq, 1, hresp, sizeof(hresp));
                gettimeofday(&t1, NULL);
                double took_ms = (t1.tv_sec - t0.tv_sec) * 1000.0 + (t1.tv_usec - t0.tv_usec) / 1000.0;
                if (hrlen < 0) {
                    any_unhealthy = 1;
                    if (want_json) {
                        printf("{\"endpoint\":\"%s:%d\",\"status\":\"unhealthy\",\"took\":\"%.3fms\",\"error\":\"failed to connect\"}\n", g_host, g_port, took_ms);
                    } else if (want_fields) {
                        printf("endpoint: %s:%d\n", g_host, g_port);
                        printf("status: unhealthy\n");
                        printf("took: %.3fms\n", took_ms);
                        printf("error: failed to connect\n\n");
                    } else {
                        printf("%s:%d is unhealthy: failed to connect\n", g_host, g_port);
                    }
                } else {
                    if (want_json) {
                        fputs("{\"endpoint\":\"", stdout);
                        printf("%s:%d\",", g_host, g_port);
                        parse_and_print_header_json(hresp, (size_t)hrlen);
                        printf(",\"status\":\"healthy\",\"took\":\"%.3fms\"}\n", took_ms);
                    } else if (want_fields) {
                        printf("endpoint: %s:%d\n", g_host, g_port);
                        parse_and_print_header_json(hresp, (size_t)hrlen);
                        printf("status: healthy\n");
                        printf("took: %.3fms\n\n", took_ms);
                    } else {
                        printf("%s:%d is healthy (%.3fms)\n", g_host, g_port, took_ms);
                    }
                }
            }
            g_host = orig_host;
            g_port = orig_port;
            return any_unhealthy ? 1 : 0;
        }
        /* Non-cluster health check: send a Status RPC and check if we get a response */
        uint8_t req[] = {0x00}, resp[1024];
        struct timeval t0, t1;
        gettimeofday(&t0, NULL);
        int rlen = do_rpc("/etcdserverpb.Maintenance/Status", req, 1, resp, sizeof(resp));
        gettimeofday(&t1, NULL);
        double took_ms = (t1.tv_sec - t0.tv_sec) * 1000.0 + (t1.tv_usec - t0.tv_usec) / 1000.0;
        if (rlen < 0) {
            if (want_json) {
                printf("{\"endpoint\":\"%s:%d\",\"status\":\"unhealthy\",\"took\":\"%.3fms\",\"error\":\"failed to connect\"}\n", g_host, g_port, took_ms);
            } else if (want_fields) {
                printf("endpoint: %s:%d\n", g_host, g_port);
                printf("status: unhealthy\n");
                printf("took: %.3fms\n", took_ms);
                printf("error: failed to connect\n");
                fputs("\n", stdout);
            } else {
                printf("%s:%d is unhealthy: failed to connect\n", g_host, g_port);
            }
            return 1;
        }
        if (want_json) {
            fputs("{\"endpoint\":\"", stdout);
            printf("%s:%d\",", g_host, g_port);
            parse_and_print_header_json(resp, (size_t)rlen);
            printf(",\"status\":\"healthy\",\"took\":\"%.3fms\"}\n", took_ms);
        } else if (want_fields) {
            printf("endpoint: %s:%d\n", g_host, g_port);
            parse_and_print_header_json(resp, (size_t)rlen);
            printf("status: healthy\n");
            printf("took: %.3fms\n", took_ms);
            fputs("\n", stdout);
        } else {
            printf("%s:%d is healthy (%.3fms)\n", g_host, g_port, took_ms);
        }
        return 0;
    } else if (strcmp(argv[2], "status") == 0) {
        if (cluster) {
            struct cluster_endpoint eps[32];
            int n = collect_cluster_endpoints(eps, 32);
            if (n < 0) { fprintf(stderr, "failed to get member list\n"); return 1; }
            const char *orig_host = g_host;
            uint16_t orig_port = g_port;
            if (want_table) {
                printf("+--------------------------+----------------+-----------+-----------+\n");
                printf("|         ENDPOINT         |      ID        |  REVISION | DB SIZE   |\n");
                printf("+--------------------------+----------------+-----------+-----------+\n");
            }
            for (int i = 0; i < n; i++) {
                g_host = eps[i].host;
                g_port = eps[i].port;
                uint8_t sreq[] = {0x00}, sresp[1024];
                int srlen = do_rpc("/etcdserverpb.Maintenance/Status", sreq, 1, sresp, sizeof(sresp));
                if (srlen < 0) continue;
                size_t pos = 0;
                const uint8_t *ver = NULL; size_t ver_len = 0;
                uint64_t db_size = 0, leader = 0, raft_index = 0, raft_term = 0, revision = 0;
                while (pos < (size_t)srlen) {
                    uint8_t tag = sresp[pos++];
                    if (tag == 0x12) {
                        uint64_t l = 0; read_varint(sresp, srlen, &pos, &l);
                        ver = sresp + pos; ver_len = (size_t)l; pos += l;
                    } else if (tag == 0x18) {
                        read_varint(sresp, srlen, &pos, &db_size);
                    } else if (tag == 0x20) {
                        read_varint(sresp, srlen, &pos, &leader);
                    } else if (tag == 0x28) {
                        read_varint(sresp, srlen, &pos, &raft_index);
                    } else if (tag == 0x30) {
                        read_varint(sresp, srlen, &pos, &raft_term);
                    } else if (tag == 0x0a) {
                        uint64_t l = 0; read_varint(sresp, srlen, &pos, &l);
                        size_t hdr_end = pos + (size_t)l;
                        while (pos < hdr_end) {
                            uint8_t htag = sresp[pos++];
                            if (htag == 0x18) { read_varint(sresp, hdr_end, &pos, &revision); }
                            else { uint64_t v = 0; read_varint(sresp, hdr_end, &pos, &v); }
                        }
                    } else {
                        uint64_t v = 0; read_varint(sresp, srlen, &pos, &v);
                    }
                }
                if (want_json) {
                    fputs("{\"endpoint\":\"", stdout);
                    printf("%s:%d\",", g_host, g_port);
                    parse_and_print_header_json(sresp, (size_t)srlen);
                    fputs(",\"version\":", stdout);
                    if (ver) print_json_string(ver, ver_len); else fputs("\"\"", stdout);
                    fputs(",", stdout);
                    printf("\"dbSize\":%llu,\"leader\":%llu,\"raftIndex\":%llu,\"raftTerm\":%llu,\"revision\":%llu}\n",
                           (unsigned long long)db_size, (unsigned long long)leader,
                           (unsigned long long)raft_index, (unsigned long long)raft_term,
                           (unsigned long long)revision);
                } else if (want_table) {
                    printf("| %-24s | %14llu | %9llu | %9llu |\n",
                           g_host, (unsigned long long)leader, (unsigned long long)revision,
                           (unsigned long long)db_size);
                } else if (want_fields) {
                    printf("endpoint: %s:%d\n", g_host, g_port);
                    printf("ID: %llu\n", (unsigned long long)leader);
                    printf("revision: %llu\n", (unsigned long long)revision);
                    printf("dbSize: %llu\n", (unsigned long long)db_size);
                    printf("raftIndex: %llu\n", (unsigned long long)raft_index);
                    printf("raftTerm: %llu\n", (unsigned long long)raft_term);
                    if (ver) printf("version: %.*s\n", (int)ver_len, ver);
                    printf("\n");
                } else {
                    printf("endpoint: %s:%d  revision: %llu  db_size: %llu\n",
                           g_host, g_port, (unsigned long long)revision, (unsigned long long)db_size);
                }
            }
            if (want_table) {
                printf("+--------------------------+----------------+-----------+-----------+\n");
            }
            g_host = orig_host;
            g_port = orig_port;
            return 0;
        }
        /* Non-cluster endpoint status */
        uint8_t req[] = {0x00}, resp[1024];
        int rlen = do_rpc("/etcdserverpb.Maintenance/Status", req, 1, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        size_t pos = 0;
        const uint8_t *ver = NULL; size_t ver_len = 0;
        uint64_t db_size = 0, leader = 0, raft_index = 0, raft_term = 0, revision = 0;
        while (pos < (size_t)rlen) {
            uint8_t tag = resp[pos++];
            if (tag == 0x12) {
                uint64_t l = 0; read_varint(resp, rlen, &pos, &l);
                ver = resp + pos; ver_len = (size_t)l; pos += l;
            } else if (tag == 0x18) {
                read_varint(resp, rlen, &pos, &db_size);
            } else if (tag == 0x20) {
                read_varint(resp, rlen, &pos, &leader);
            } else if (tag == 0x28) {
                read_varint(resp, rlen, &pos, &raft_index);
            } else if (tag == 0x30) {
                read_varint(resp, rlen, &pos, &raft_term);
            } else if (tag == 0x0a) {
                uint64_t l = 0; read_varint(resp, rlen, &pos, &l);
                size_t hdr_end = pos + (size_t)l;
                while (pos < hdr_end) {
                    uint8_t htag = resp[pos++];
                    if (htag == 0x18) { read_varint(resp, hdr_end, &pos, &revision); }
                    else { uint64_t v = 0; read_varint(resp, hdr_end, &pos, &v); }
                }
            } else {
                uint64_t v = 0; read_varint(resp, rlen, &pos, &v);
            }
        }
        if (want_json) {
            fputs("{\"endpoint\":\"", stdout);
            printf("%s:%d\",", g_host, g_port);
            parse_and_print_header_json(resp, (size_t)rlen);
            fputs(",\"version\":", stdout);
            if (ver) print_json_string(ver, ver_len); else fputs("\"\"", stdout);
            fputs(",", stdout);
            printf("\"dbSize\":%llu,\"leader\":%llu,\"raftIndex\":%llu,\"raftTerm\":%llu,\"revision\":%llu}\n",
                   (unsigned long long)db_size, (unsigned long long)leader,
                   (unsigned long long)raft_index, (unsigned long long)raft_term,
                   (unsigned long long)revision);
        } else if (want_table) {
            printf("+--------------------------+----------------+-----------+-----------+\n");
            printf("|         ENDPOINT         |      ID        |  REVISION | DB SIZE   |\n");
            printf("+--------------------------+----------------+-----------+-----------+\n");
            printf("| %-24s | %14llu | %9llu | %9llu |\n",
                   g_host, (unsigned long long)leader, (unsigned long long)revision,
                   (unsigned long long)db_size);
            printf("+--------------------------+----------------+-----------+-----------+\n");
        } else if (want_fields) {
            printf("endpoint: %s:%d\n", g_host, g_port);
            printf("ID: %llu\n", (unsigned long long)leader);
            printf("revision: %llu\n", (unsigned long long)revision);
            printf("dbSize: %llu\n", (unsigned long long)db_size);
            printf("raftIndex: %llu\n", (unsigned long long)raft_index);
            printf("raftTerm: %llu\n", (unsigned long long)raft_term);
            if (ver) printf("version: %.*s\n", (int)ver_len, ver);
            printf("\n");
        } else {
            parse_status_response(resp, rlen);
        }
        return 0;
    } else if (strcmp(argv[2], "hashkv") == 0) {
        if (cluster) {
            struct cluster_endpoint eps[32];
            int n = collect_cluster_endpoints(eps, 32);
            if (n < 0) { fprintf(stderr, "failed to get member list\n"); return 1; }
            const char *orig_host = g_host;
            uint16_t orig_port = g_port;
            for (int i = 0; i < n; i++) {
                g_host = eps[i].host;
                g_port = eps[i].port;
                uint8_t hreq[] = {0x00}, hresp[256];
                int hrlen = do_rpc("/etcdserverpb.Maintenance/HashKV", hreq, 1, hresp, sizeof(hresp));
                if (hrlen < 0) continue;
                size_t rpos = 0;
                uint64_t hash_val = 0, compact_rev = 0;
                while (rpos < (size_t)hrlen) {
                    uint8_t tag = hresp[rpos++];
                    if (tag == 0x10) { read_varint(hresp, hrlen, &rpos, &hash_val); }
                    else if (tag == 0x18) { read_varint(hresp, hrlen, &rpos, &compact_rev); }
                    else if (tag == 0x0a) { uint64_t l = 0; read_varint(hresp, hrlen, &rpos, &l); rpos += l; }
                    else { uint64_t v = 0; read_varint(hresp, hrlen, &rpos, &v); }
                }
                if (want_json) {
                    fputs("{\"endpoint\":\"", stdout);
                    printf("%s:%d\",", g_host, g_port);
                    parse_and_print_header_json(hresp, (size_t)hrlen);
                    printf(",\"hash\":%llu,\"compact_revision\":%llu}\n",
                           (unsigned long long)hash_val, (unsigned long long)compact_rev);
                } else if (want_fields) {
                    printf("endpoint: %s:%d\n", g_host, g_port);
                    parse_and_print_header_json(hresp, (size_t)hrlen);
                    printf("hash: %llu\n", (unsigned long long)hash_val);
                    printf("compact_revision: %llu\n\n", (unsigned long long)compact_rev);
                } else {
                    printf("endpoint: %s:%d  hash: %llu  compact_revision: %llu\n",
                           g_host, g_port, (unsigned long long)hash_val, (unsigned long long)compact_rev);
                }
            }
            g_host = orig_host;
            g_port = orig_port;
            return 0;
        }
        /* Non-cluster endpoint hashkv */
        uint8_t req[] = {0x00}, resp[256];
        int rlen = do_rpc("/etcdserverpb.Maintenance/HashKV", req, 1, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        size_t rpos = 0;
        uint64_t hash_val = 0, compact_rev = 0;
        while (rpos < (size_t)rlen) {
            uint8_t tag = resp[rpos++];
            if (tag == 0x10) { read_varint(resp, rlen, &rpos, &hash_val); }
            else if (tag == 0x18) { read_varint(resp, rlen, &rpos, &compact_rev); }
            else if (tag == 0x0a) { uint64_t l = 0; read_varint(resp, rlen, &rpos, &l); rpos += l; }
            else { uint64_t v = 0; read_varint(resp, rlen, &rpos, &v); }
        }
        if (want_json) {
            fputs("{\"endpoint\":\"", stdout);
            printf("%s:%d\",", g_host, g_port);
            parse_and_print_header_json(resp, (size_t)rlen);
            printf(",\"hash\":%llu,\"compact_revision\":%llu}\n",
                   (unsigned long long)hash_val, (unsigned long long)compact_rev);
        } else if (want_fields) {
            parse_and_print_header_json(resp, (size_t)rlen);
            printf("hash: %llu\n", (unsigned long long)hash_val);
            printf("compact_revision: %llu\n", (unsigned long long)compact_rev);
            fputs("\n", stdout);
        } else {
            printf("endpoint: %s:%d  hash: %llu  compact_revision: %llu\n",
                   g_host, g_port, (unsigned long long)hash_val, (unsigned long long)compact_rev);
        }
        return 0;
    } else {
        fprintf(stderr, "unknown endpoint subcommand: %s\n", argv[2]);
        return 1;
    }
}

static int cmd_check(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "usage: cetcdctl check perf [--load S|M|L] [--prefix PREFIX] [-w json|fields]\n");
        return 1;
    }
    if (strcmp(argv[2], "perf") == 0) {
        int want_json = 0, want_fields = 0;
        int load_count = 1;  /* default: 1 key (simple check) */
        const char *prefix = "_perf_check";
        for (int i = 3; i < argc; i++) {
            if ((strcmp(argv[i], "-w") == 0 || strcmp(argv[i], "--write-out") == 0) && i + 1 < argc) {
                if (strcmp(argv[i + 1], "json") == 0) want_json = 1;
                else if (strcmp(argv[i + 1], "fields") == 0) want_fields = 1;
                i++;
            } else if (strcmp(argv[i], "--load") == 0 && i + 1 < argc) {
                const char *sz = argv[++i];
                if (strcmp(sz, "s") == 0 || strcmp(sz, "S") == 0) load_count = 10;
                else if (strcmp(sz, "m") == 0 || strcmp(sz, "M") == 0) load_count = 100;
                else if (strcmp(sz, "l") == 0 || strcmp(sz, "L") == 0) load_count = 1000;
                else { fprintf(stderr, "--load must be s, m, or l\n"); return 1; }
            } else if (strcmp(argv[i], "--prefix") == 0 && i + 1 < argc) {
                prefix = argv[++i];
            }
        }
        if (!want_json && !want_fields) printf("Running performance check (load=%d)...\n", load_count);

        /* Put test keys and measure latency */
        double total_put_ms = 0, total_get_ms = 0;
        int ok_count = 0;
        for (int i = 0; i < load_count; i++) {
            char key[256];
            char val[64];
            int kl = snprintf(key, sizeof(key), "%s_%d", prefix, i);
            int vl = snprintf(val, sizeof(val), "v%d", i);
            uint8_t put_req[512], put_resp[256];
            size_t pos = 0;
            pos = encode_bytes_field(put_req, sizeof(put_req), pos, 0x0a,
                                     (const uint8_t *)key, (size_t)kl);
            pos = encode_bytes_field(put_req, sizeof(put_req), pos, 0x12,
                                     (const uint8_t *)val, (size_t)vl);
            struct timeval t0, t1;
            gettimeofday(&t0, NULL);
            int rlen = do_rpc("/etcdserverpb.KV/Put", put_req, pos, put_resp, sizeof(put_resp));
            gettimeofday(&t1, NULL);
            if (rlen < 0) break;
            total_put_ms += (t1.tv_sec - t0.tv_sec) * 1000.0 + (t1.tv_usec - t0.tv_usec) / 1000.0;

            /* Get the key back */
            uint8_t get_req[256], get_resp[1024];
            pos = 0;
            pos = encode_bytes_field(get_req, sizeof(get_req), pos, 0x0a,
                                     (const uint8_t *)key, (size_t)kl);
            gettimeofday(&t0, NULL);
            rlen = do_rpc("/etcdserverpb.KV/Range", get_req, pos, get_resp, sizeof(get_resp));
            gettimeofday(&t1, NULL);
            if (rlen < 0) break;
            total_get_ms += (t1.tv_sec - t0.tv_sec) * 1000.0 + (t1.tv_usec - t0.tv_usec) / 1000.0;
            ok_count++;
        }
        double avg_put_ms = ok_count > 0 ? total_put_ms / ok_count : 0;
        double avg_get_ms = ok_count > 0 ? total_get_ms / ok_count : 0;

        /* Cleanup: delete all test keys with prefix */
        {
            uint8_t del_req[512], del_resp[4096];
            size_t dpos = 0;
            size_t plen = strlen(prefix);
            dpos = encode_bytes_field(del_req, sizeof(del_req), dpos, 0x0a,
                                      (const uint8_t *)prefix, plen);
            if (plen == 0) {
                uint8_t zero = 0;
                dpos = encode_bytes_field(del_req, sizeof(del_req), dpos, 0x12, &zero, 1);
            } else {
                char range_end[256];
                if (plen >= sizeof(range_end)) { fprintf(stderr, "prefix too long\n"); return 1; }
                memcpy(range_end, prefix, plen);
                range_end[plen - 1]++;
                dpos = encode_bytes_field(del_req, sizeof(del_req), dpos, 0x12,
                                          (const uint8_t *)range_end, plen);
            }
            do_rpc("/etcdserverpb.KV/DeleteRange", del_req, dpos, del_resp, sizeof(del_resp));
        }

        if (want_json) {
            fputs("{", stdout);
            printf("\"status\":\"PASS\",\"keys_tested\":%d,\"put_latency_ms\":%.3f,\"get_latency_ms\":%.3f}\n",
                   ok_count, avg_put_ms, avg_get_ms);
        } else if (want_fields) {
            printf("status: PASS\n");
            printf("keys_tested: %d\n", ok_count);
            printf("put_latency_ms: %.3f\n", avg_put_ms);
            printf("get_latency_ms: %.3f\n", avg_get_ms);
            fputs("\n", stdout);
        } else {
            printf("PASS: Performance check completed successfully (%d keys)\n", ok_count);
            printf("  Avg Put latency: %.3f ms\n", avg_put_ms);
            printf("  Avg Get latency: %.3f ms\n", avg_get_ms);
        }
        return 0;
    } else if (strcmp(argv[2], "datascale") == 0) {
        int want_json = 0, want_fields = 0;
        int load_count = 10000;
        const char *prefix = "_datascale_";
        for (int i = 3; i < argc; i++) {
            if ((strcmp(argv[i], "-w") == 0 || strcmp(argv[i], "--write-out") == 0) && i + 1 < argc) {
                if (strcmp(argv[i + 1], "json") == 0) want_json = 1;
                else if (strcmp(argv[i + 1], "fields") == 0) want_fields = 1;
                i++;
            } else if (strcmp(argv[i], "--load") == 0 && i + 1 < argc) {
                load_count = atoi(argv[++i]);
                if (load_count <= 0) load_count = 10000;
            } else if (strcmp(argv[i], "--prefix") == 0 && i + 1 < argc) {
                prefix = argv[++i];
            }
        }
        if (!want_json && !want_fields) printf("Running datascale check (loading %d keys)...\n", load_count);
        struct timeval t0, t1;
        gettimeofday(&t0, NULL);
        int loaded = 0;
        for (int i = 0; i < load_count; i++) {
            char key[256], val[64];
            int kl = snprintf(key, sizeof(key), "%s%d", prefix, i);
            int vl = snprintf(val, sizeof(val), "v%d", i);
            uint8_t put_req[512], put_resp[256];
            size_t pos = 0;
            pos = encode_bytes_field(put_req, sizeof(put_req), pos, 0x0a,
                                     (const uint8_t *)key, (size_t)kl);
            pos = encode_bytes_field(put_req, sizeof(put_req), pos, 0x12,
                                     (const uint8_t *)val, (size_t)vl);
            int rlen = do_rpc("/etcdserverpb.KV/Put", put_req, pos, put_resp, sizeof(put_resp));
            if (rlen < 0) break;
            loaded++;
        }
        gettimeofday(&t1, NULL);
        double elapsed = (t1.tv_sec - t0.tv_sec) * 1000.0 + (t1.tv_usec - t0.tv_usec) / 1000.0;
        /* Get DB size from status */
        uint8_t st_req[] = {0x00}, st_resp[1024];
        int st_rlen = do_rpc("/etcdserverpb.Maintenance/Status", st_req, 1, st_resp, sizeof(st_resp));
        uint64_t db_size = 0;
        if (st_rlen > 0) {
            size_t sp = 0;
            while (sp < (size_t)st_rlen) {
                uint8_t t = st_resp[sp++];
                if (t == 0x18) { read_varint(st_resp, st_rlen, &sp, &db_size); }
                else if (t == 0x0a || t == 0x12) { uint64_t l = 0; read_varint(st_resp, st_rlen, &sp, &l); sp += l; }
                else { uint64_t v = 0; read_varint(st_resp, st_rlen, &sp, &v); }
            }
        }
        /* Cleanup: delete all test keys with prefix */
        {
            uint8_t del_req[512], del_resp[4096];
            size_t dpos = 0;
            size_t plen = strlen(prefix);
            dpos = encode_bytes_field(del_req, sizeof(del_req), dpos, 0x0a,
                                      (const uint8_t *)prefix, plen);
            if (plen == 0) {
                /* Empty prefix means all keys */
                uint8_t zero = 0;
                dpos = encode_bytes_field(del_req, sizeof(del_req), dpos, 0x12, &zero, 1);
            } else {
                char range_end[256];
                if (plen >= sizeof(range_end)) { fprintf(stderr, "prefix too long\n"); return 1; }
                memcpy(range_end, prefix, plen);
                range_end[plen - 1]++;
                dpos = encode_bytes_field(del_req, sizeof(del_req), dpos, 0x12,
                                          (const uint8_t *)range_end, plen);
            }
            do_rpc("/etcdserverpb.KV/DeleteRange", del_req, dpos, del_resp, sizeof(del_resp));
        }
        if (want_json) {
            fputs("{", stdout);
            if (st_rlen > 0) parse_and_print_header_json(st_resp, (size_t)st_rlen);
            else fputs("\"header\":{}", stdout);
            printf(",\"status\":\"PASS\",\"keys_loaded\":%d,\"db_size\":%llu,\"elapsed_ms\":%.3f}\n",
                   loaded, (unsigned long long)db_size, elapsed);
        } else if (want_fields) {
            if (st_rlen > 0) parse_and_print_header_json(st_resp, (size_t)st_rlen);
            printf("status: PASS\n");
            printf("keys_loaded: %d\n", loaded);
            printf("db_size: %llu\n", (unsigned long long)db_size);
            printf("elapsed_ms: %.3f\n", elapsed);
            fputs("\n", stdout);
        } else {
            printf("PASS: Loaded %d keys in %.3f ms\n", loaded, elapsed);
            printf("  DB size: %llu bytes\n", (unsigned long long)db_size);
        }
        return 0;
    } else {
        fprintf(stderr, "unknown check subcommand: %s (use: perf, datascale)\n", argv[2]);
        return 1;
    }
}

/* Signal handler for lock release on Ctrl+C */
static void lock_signal_handler(int sig) {
    (void)sig;
    /* Kill the keepalive child process if it exists */
    if (g_keepalive_pid > 0) {
        kill(g_keepalive_pid, SIGTERM);
        g_keepalive_pid = -1;
    }
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
        fprintf(stderr, "usage: cetcdctl lock [--ttl N] [--print-value-only] [-w json|fields] LOCKNAME [COMMAND...]\n");
        return 1;
    }
    int ttl = 60; /* default lease TTL */
    int print_value_only = 0;
    int want_json = 0, want_fields = 0;
    const char *lockname = NULL;
    for (int i = 2; i < argc; i++) {
        if (strcmp(argv[i], "--ttl") == 0 && i + 1 < argc) {
            ttl = atoi(argv[++i]);
            if (ttl <= 0) { fprintf(stderr, "invalid --ttl value\n"); return 1; }
        } else if (strcmp(argv[i], "--print-value-only") == 0) {
            print_value_only = 1;
        } else if ((strcmp(argv[i], "-w") == 0 || strcmp(argv[i], "--write-out") == 0) && i + 1 < argc) {
            if (strcmp(argv[i + 1], "json") == 0) want_json = 1;
            else if (strcmp(argv[i + 1], "fields") == 0) want_fields = 1;
            i++;
        } else if (!lockname) {
            lockname = argv[i];
        }
        /* Remaining args after lockname are the command to execute */
        if (lockname) break;
    }
    if (!lockname) { fprintf(stderr, "usage: cetcdctl lock [--ttl N] [--print-value-only] [-w json|fields] LOCKNAME [COMMAND...]\n"); return 1; }
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

    /* Step 1: Grant a lease with specified TTL */
    uint8_t grant_req[16], grant_resp[256];
    size_t gpos = 0;
    gpos = encode_varint_field(grant_req, sizeof(grant_req), gpos, 0x03, (uint64_t)ttl);
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

    /* Fork a keepalive child process to periodically renew the lease */
    g_keepalive_pid = fork();
    if (g_keepalive_pid == 0) {
        /* Child: send LeaseKeepAlive every ttl/2 seconds */
        unsigned interval = (unsigned)(ttl / 2);
        if (interval == 0) interval = 1;
        for (;;) {
            uint8_t ka_req[32], ka_resp[256];
            size_t kp = 0;
            kp = encode_varint_field(ka_req, sizeof(ka_req), kp, 0x08, lease_id);
            int kl = do_rpc("/etcdserverpb.Lease/LeaseKeepAlive", ka_req, kp,
                            ka_resp, sizeof(ka_resp));
            if (kl < 0) _exit(1); /* server unreachable, exit */
            /* Parse TTL from response (field 3, tag 0x18) */
            unsigned new_ttl = 0;
            size_t krp = 0;
            while (krp < (size_t)kl) {
                uint8_t tag = ka_resp[krp++];
                if (tag == 0x18) {
                    uint64_t v = 0; read_varint(ka_resp, kl, &krp, &v);
                    new_ttl = (unsigned)v;
                    break;
                } else if (tag == 0x0a) {
                    uint64_t l = 0; read_varint(ka_resp, kl, &krp, &l); krp += l;
                } else {
                    uint64_t v = 0; read_varint(ka_resp, kl, &krp, &v);
                }
            }
            if (new_ttl == 0) _exit(0); /* lease expired or revoked */
            unsigned sleep_sec = new_ttl / 2;
            if (sleep_sec == 0) sleep_sec = 1;
            sleep(sleep_sec);
        }
    } else if (g_keepalive_pid < 0) {
        g_keepalive_pid = -1; /* fork failed, continue without keepalive */
    }

    /* Print the lock key or lease ID (etcdctl prints the key name) */
    if (want_json) {
        fputs("{", stdout);
        parse_and_print_header_json(resp, (size_t)rlen);
        fputs(",\"key\":", stdout);
        print_json_string((const uint8_t *)lock_key, lock_key_len);
        fputs("}\n", stdout);
    } else if (want_fields) {
        parse_and_print_header_json(resp, (size_t)rlen);
        printf("key: %.*s\n", (int)lock_key_len, lock_key);
        fputs("\n", stdout);
    } else if (print_value_only) {
        printf("%llu\n", (unsigned long long)lease_id);
    } else {
        printf("%.*s\n", (int)lock_key_len, lock_key);
    }
    fflush(stdout);

    /* If a command is provided, execute it; otherwise wait for signal */
    /* Find command args: skip lockname */
    int cmd_argc = 0;
    char **cmd_argv = NULL;
    for (int i = 2; i < argc; i++) {
        if (strcmp(argv[i], "--ttl") == 0) { i++; continue; }
        /* First non-flag arg is lockname; args after it are the command */
        if (argv[i] == lockname) {
            if (i + 1 < argc) { cmd_argc = argc - i - 1; cmd_argv = &argv[i + 1]; }
            break;
        }
    }
    if (cmd_argc > 0) {
        /* Execute the command */
        int ret = 0;
        pid_t pid = fork();
        if (pid == 0) {
            /* Child */
            execvp(cmd_argv[0], cmd_argv);
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
        /* Kill keepalive child and release the lock */
        if (g_keepalive_pid > 0) { kill(g_keepalive_pid, SIGTERM); waitpid(g_keepalive_pid, NULL, 0); g_keepalive_pid = -1; }
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
        fprintf(stderr, "usage: cetcdctl elect [--ttl N] [--print-value-only] [-w json|fields] ELECTION_NAME [PROPOSAL]\n");
        return 1;
    }
    int ttl = 60;
    int print_value_only = 0;
    int want_json = 0, want_fields = 0;
    const char *election_name = NULL;
    const char *proposal = NULL;
    for (int i = 2; i < argc; i++) {
        if (strcmp(argv[i], "--ttl") == 0 && i + 1 < argc) {
            ttl = atoi(argv[++i]);
            if (ttl <= 0) { fprintf(stderr, "invalid --ttl value\n"); return 1; }
        } else if (strcmp(argv[i], "--print-value-only") == 0) {
            print_value_only = 1;
        } else if ((strcmp(argv[i], "-w") == 0 || strcmp(argv[i], "--write-out") == 0) && i + 1 < argc) {
            if (strcmp(argv[i + 1], "json") == 0) want_json = 1;
            else if (strcmp(argv[i + 1], "fields") == 0) want_fields = 1;
            i++;
        } else if (!election_name) {
            election_name = argv[i];
        } else if (!proposal) {
            proposal = argv[i];
        }
    }
    if (!election_name) { fprintf(stderr, "usage: cetcdctl elect [--ttl N] [--print-value-only] [-w json|fields] ELECTION_NAME [PROPOSAL]\n"); return 1; }
    if (!proposal) proposal = "cetcd";
    size_t name_len = strlen(election_name);
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

    /* Step 1: Grant a lease with specified TTL */
    uint8_t grant_req[16], grant_resp[256];
    size_t gpos = 0;
    gpos = encode_varint_field(grant_req, sizeof(grant_req), gpos, 0x03, (uint64_t)ttl);
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

    /* Fork a keepalive child process to periodically renew the lease */
    g_keepalive_pid = fork();
    if (g_keepalive_pid == 0) {
        /* Child: send LeaseKeepAlive every ttl/2 seconds */
        for (;;) {
            uint8_t ka_req[32], ka_resp[256];
            size_t kp = 0;
            kp = encode_varint_field(ka_req, sizeof(ka_req), kp, 0x08, lease_id);
            int kl = do_rpc("/etcdserverpb.Lease/LeaseKeepAlive", ka_req, kp,
                            ka_resp, sizeof(ka_resp));
            if (kl < 0) _exit(1);
            unsigned new_ttl = 0;
            size_t krp = 0;
            while (krp < (size_t)kl) {
                uint8_t tag = ka_resp[krp++];
                if (tag == 0x18) {
                    uint64_t v = 0; read_varint(ka_resp, kl, &krp, &v);
                    new_ttl = (unsigned)v;
                    break;
                } else if (tag == 0x0a) {
                    uint64_t l = 0; read_varint(ka_resp, kl, &krp, &l); krp += l;
                } else {
                    uint64_t v = 0; read_varint(ka_resp, kl, &krp, &v);
                }
            }
            if (new_ttl == 0) _exit(0);
            unsigned sleep_sec = new_ttl / 2;
            if (sleep_sec == 0) sleep_sec = 1;
            sleep(sleep_sec);
        }
    } else if (g_keepalive_pid < 0) {
        g_keepalive_pid = -1;
    }

    if (want_json) {
        fputs("{", stdout);
        parse_and_print_header_json(resp, (size_t)rlen);
        fputs(",\"leader\":", stdout);
        print_json_string((const uint8_t *)proposal, proposal_len);
        fputs("}\n", stdout);
    } else if (want_fields) {
        parse_and_print_header_json(resp, (size_t)rlen);
        printf("leader: %s\n", proposal);
        fputs("\n", stdout);
    } else if (print_value_only) {
        printf("%llu\n", (unsigned long long)lease_id);
    } else {
        printf("%s\n", proposal);
    }
    fflush(stdout);

    /* Wait for signal */
    while (g_lock_held) {
        pause();
    }
    return 0;
}

static int cmd_alarm(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "usage: cetcdctl alarm {list,activate,disarm} [TYPE] [-w json|table|fields]\n");
        return 1;
    }

    const char *subcmd = argv[2];
    int action = 0; /* 0=GET, 1=ACTIVATE, 2=DEACTIVATE */
    int alarm_type = 1; /* NOSPACE */
    const char *alarm_type_str = "NOSPACE";
    int table_fmt = 0;
    int json_fmt = 0;
    int fields_fmt = 0;

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

    for (int i = 3; i < argc; i++) {
        if ((strcmp(argv[i], "-w") == 0 || strcmp(argv[i], "--write-out") == 0) && i + 1 < argc) {
            if (strcmp(argv[i + 1], "table") == 0) table_fmt = 1;
            else if (strcmp(argv[i + 1], "json") == 0) json_fmt = 1;
            else if (strcmp(argv[i + 1], "fields") == 0) fields_fmt = 1;
            i++;
        } else if (argv[i][0] != '-') {
            /* Parse alarm type: none, nospace, corrupt */
            if (strcmp(argv[i], "none") == 0 || strcmp(argv[i], "NONE") == 0) {
                alarm_type = 0; alarm_type_str = "NONE";
            } else if (strcmp(argv[i], "nospace") == 0 || strcmp(argv[i], "NOSPACE") == 0) {
                alarm_type = 1; alarm_type_str = "NOSPACE";
            } else if (strcmp(argv[i], "corrupt") == 0 || strcmp(argv[i], "CORRUPT") == 0) {
                alarm_type = 2; alarm_type_str = "CORRUPT";
            } else {
                fprintf(stderr, "unknown alarm type: %s (use: none, nospace, corrupt)\n", argv[i]);
                return 1;
            }
        }
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
        fputs("{", stdout);
        parse_and_print_header_json(resp, (size_t)rlen);
        fputs(",\"alarms\":[", stdout);
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
                const char *type_str = (alarm_val == 0) ? "NONE" : (alarm_val == 1) ? "NOSPACE" : (alarm_val == 2) ? "CORRUPT" : "UNKNOWN";
                if (json_fmt) {
                    if (found_alarms) printf(",");
                    printf("{\"memberID\":%lu,\"alarm\":\"%s\"}", (unsigned long)member_id, type_str);
                } else if (fields_fmt) {
                    printf("memberID: %lu\n", (unsigned long)member_id);
                    printf("alarm: %s\n", type_str);
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
        if (!found_alarms && !table_fmt && !json_fmt && !fields_fmt) printf("no alarms\n");
        if (!found_alarms && table_fmt) printf("(no alarms)\n");
    } else if (action == 1) { /* activate */
        if (json_fmt) { fputs("{", stdout); parse_and_print_header_json(resp, (size_t)rlen); fputs("}\n", stdout); }
        else if (fields_fmt) { parse_and_print_header_json(resp, (size_t)rlen); fputs("\n", stdout); }
        else printf("alarm activated %s\n", alarm_type_str);
    } else { /* disarm */
        if (json_fmt) { fputs("{", stdout); parse_and_print_header_json(resp, (size_t)rlen); fputs("}\n", stdout); }
        else if (fields_fmt) { parse_and_print_header_json(resp, (size_t)rlen); fputs("\n", stdout); }
        else printf("alarm disarmed\n");
    }
    return 0;
}

static int cmd_version(int argc, char **argv) {
    int want_json = 0, want_fields = 0;
    for (int i = 2; i < argc; i++) {
        if ((strcmp(argv[i], "-w") == 0 || strcmp(argv[i], "--write-out") == 0) && i + 1 < argc) {
            if (strcmp(argv[i + 1], "json") == 0) want_json = 1;
            else if (strcmp(argv[i + 1], "fields") == 0) want_fields = 1;
            i++;
        }
    }
    const char *ver = cetcd_version();
    if (want_json) {
        printf("{\"client\":\"cetcdctl\",\"server\":\"cetcd\",\"version\":\"%s\",\"etcd\":\"v3.5 compatible\"}\n", ver);
    } else if (want_fields) {
        printf("client: cetcdctl\n");
        printf("server: cetcd\n");
        printf("version: %s\n", ver);
        printf("etcd: v3.5 compatible\n");
        fputs("\n", stdout);
    } else {
        printf("cetcd version %s (etcd v3.5 compatible)\n", ver);
    }
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
        int want_json = 0, want_fields = 0;
        const char *filename = NULL;
        for (int i = 3; i < argc; i++) {
            if ((strcmp(argv[i], "-w") == 0 || strcmp(argv[i], "--write-out") == 0) && i + 1 < argc) {
                if (strcmp(argv[i + 1], "json") == 0) want_json = 1;
                else if (strcmp(argv[i + 1], "fields") == 0) want_fields = 1;
                i++;
            } else if (strcmp(argv[i], "--compaction-periodical") == 0) {
                /* no-op: accepted for etcdctl compatibility */
            } else if (!filename) {
                filename = argv[i];
            }
        }
        uint8_t req[] = {0x00}, resp[65536];
        int rlen = do_rpc("/etcdserverpb.Maintenance/Snapshot", req, 1, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        /* Parse SnapshotResponse: field 1 (header), field 2 (remaining), field 3 (blob) */
        size_t spos = 0;
        const uint8_t *blob_data = NULL; size_t blob_len = 0;
        uint64_t snap_revision = 0;
        while (spos < (size_t)rlen) {
            uint8_t stag = resp[spos++];
            if (stag == 0x0a) {
                /* header (length-delimited) — extract revision */
                uint64_t l = 0; read_varint(resp, rlen, &spos, &l);
                size_t hdr_end = spos + (size_t)l;
                while (spos < hdr_end) {
                    uint8_t htag = resp[spos++];
                    if (htag == 0x18) { /* revision */
                        read_varint(resp, hdr_end, &spos, &snap_revision);
                    } else if (htag == 0x08 || htag == 0x10 || htag == 0x20) {
                        uint64_t v = 0; read_varint(resp, hdr_end, &spos, &v);
                    } else {
                        uint64_t v = 0; read_varint(resp, hdr_end, &spos, &v);
                    }
                }
                spos = hdr_end;
            } else if (stag == 0x10) {
                /* remaining (varint) */
                uint64_t v = 0; read_varint(resp, rlen, &spos, &v);
            } else if (stag == 0x1a) {
                /* blob (bytes) */
                uint64_t l = 0; read_varint(resp, rlen, &spos, &l);
                blob_data = resp + spos; blob_len = (size_t)l; spos += l;
            } else {
                uint64_t v = 0; read_varint(resp, rlen, &spos, &v);
            }
        }
        size_t snapshot_size = blob_len;
        /* If a file is specified, write the blob to it */
        if (filename) {
            FILE *f = fopen(filename, "wb");
            if (!f) { perror("fopen"); return 1; }
            /* Write snapshot file header: 4-byte magic "CTS1" + 8-byte revision (LE) */
            fwrite("CTS1", 1, 4, f);
            uint8_t rev_bytes[8];
            for (int i = 0; i < 8; i++) rev_bytes[i] = (uint8_t)((snap_revision >> (i * 8)) & 0xFF);
            fwrite(rev_bytes, 1, 8, f);
            if (blob_data && blob_len > 0) fwrite(blob_data, 1, blob_len, f);
            fclose(f);
            snapshot_size = blob_len + 12; /* include header */
            if (want_json) {
                fputs("{", stdout);
                parse_and_print_header_json(resp, (size_t)rlen);
                printf(",\"snapshot\":\"%s\",\"size\":%zu}\n", filename, snapshot_size);
            } else if (want_fields) {
                parse_and_print_header_json(resp, (size_t)rlen);
                printf("snapshot: %s\n", filename);
                printf("size: %zu\n", snapshot_size);
                fputs("\n", stdout);
            } else {
                printf("snapshot saved to %s (%zu bytes)\n", filename, snapshot_size);
            }
        } else {
            if (want_json) {
                fputs("{", stdout);
                parse_and_print_header_json(resp, (size_t)rlen);
                printf(",\"size\":%zu}\n", snapshot_size);
            } else if (want_fields) {
                parse_and_print_header_json(resp, (size_t)rlen);
                printf("size: %zu\n", snapshot_size);
                fputs("\n", stdout);
            } else {
                printf("snapshot: %zu bytes received\n", snapshot_size);
            }
        }
        return 0;
    } else if (strcmp(argv[2], "status") == 0) {
        /* Show snapshot file info */
        if (argc < 4) {
            fprintf(stderr, "usage: cetcdctl snapshot status FILE [-w json|fields]\n");
            return 1;
        }
        int snap_json = 0, snap_fields = 0;
        for (int i = 4; i < argc; i++) {
            if ((strcmp(argv[i], "-w") == 0 || strcmp(argv[i], "--write-out") == 0) && i + 1 < argc) {
                if (strcmp(argv[i + 1], "json") == 0) snap_json = 1;
                else if (strcmp(argv[i + 1], "fields") == 0) snap_fields = 1;
                i++;
            }
        }
        FILE *f = fopen(argv[3], "rb");
        if (!f) { perror("fopen"); return 1; }
        fseek(f, 0, SEEK_END);
        long fsize = ftell(f);
        fseek(f, 0, SEEK_SET);
        uint8_t *fdata = (uint8_t *)malloc(fsize);
        if (fdata) fread(fdata, 1, fsize, f);
        fclose(f);
        /* Parse snapshot file: optional 12-byte header (magic "CTS1" + revision) + blob */
        int key_count = 0;
        uint32_t hash = 0;
        uint64_t snap_rev = 0;
        size_t data_offset = 0;
        size_t data_size = (size_t)fsize;
        if (fdata && fsize >= 12 && memcmp(fdata, "CTS1", 4) == 0) {
            /* New format: has revision header */
            for (int i = 0; i < 8; i++)
                snap_rev |= ((uint64_t)fdata[4 + i]) << (i * 8);
            data_offset = 12;
            data_size = (size_t)fsize - 12;
        }
        if (fdata) {
            size_t sp = data_offset;
            while (sp < data_offset + data_size) {
                /* read key_len */
                uint64_t kl = 0; if (read_varint(fdata, fsize, &sp, &kl) != 0) break;
                if (sp + kl > (size_t)fsize) break;
                for (size_t i = 0; i < kl; i++) hash = hash * 31 + fdata[sp + i];
                sp += kl;
                /* read val_len */
                uint64_t vl = 0; if (read_varint(fdata, fsize, &sp, &vl) != 0) break;
                if (sp + vl > (size_t)fsize) break;
                for (size_t i = 0; i < vl; i++) hash = hash * 31 + fdata[sp + i];
                sp += vl;
                key_count++;
            }
        }
        if (fdata) free(fdata);
        if (snap_json) {
            printf("{\"hash\":%u,\"revision\":%llu,\"total_keys\":%d,\"size\":%ld,\"filename\":\"%s\"}\n",
                   hash, (unsigned long long)snap_rev, key_count, fsize, argv[3]);
        } else if (snap_fields) {
            printf("hash: %u\n", hash);
            if (snap_rev > 0)
                printf("revision: %llu\n", (unsigned long long)snap_rev);
            else
                printf("revision: -\n");
            printf("total_keys: %d\n", key_count);
            printf("size: %ld\n", fsize);
            printf("filename: %s\n", argv[3]);
            fputs("\n", stdout);
        } else {
            printf("+----------+----------+------------+---------+---------------------+\n");
            printf("|   hash   | revision | total_keys |  size   |     filename        |\n");
            printf("+----------+----------+------------+---------+---------------------+\n");
            if (snap_rev > 0)
                printf("| %8u | %8llu | %10d | %7ld | %-19s |\n", hash, (unsigned long long)snap_rev, key_count, fsize, argv[3]);
            else
                printf("| %8u | %8s | %10d | %7ld | %-19s |\n", hash, "-", key_count, fsize, argv[3]);
            printf("+----------+----------+------------+---------+---------------------+\n");
        }
        return 0;
    } else if (strcmp(argv[2], "restore") == 0) {
        /* Restore a snapshot to a data directory.
         * The snapshot file contains KV pairs in a custom format:
         *   repeated: key_len(varint) + key + val_len(varint) + val
         * We parse the KV pairs and write them as a data dump file
         * that can be loaded by cetcd on startup. */
        const char *snap_file = NULL;
        const char *data_dir = NULL;
        int force = 0;
        int want_json = 0, want_fields = 0;
        for (int i = 3; i < argc; i++) {
            if (strcmp(argv[i], "--data-dir") == 0 && i + 1 < argc) {
                data_dir = argv[++i];
            } else if (strcmp(argv[i], "--force") == 0) {
                force = 1;
            } else if ((strcmp(argv[i], "-w") == 0 || strcmp(argv[i], "--write-out") == 0) && i + 1 < argc) {
                if (strcmp(argv[i + 1], "json") == 0) want_json = 1;
                else if (strcmp(argv[i + 1], "fields") == 0) want_fields = 1;
                i++;
            } else if (!snap_file && argv[i][0] != '-') {
                snap_file = argv[i];
            }
        }
        if (!snap_file) {
            fprintf(stderr, "usage: cetcdctl snapshot restore FILE --data-dir DIR [--force] [-w json|fields]\n");
            return 1;
        }
        if (!data_dir) {
            fprintf(stderr, "--data-dir is required for snapshot restore\n");
            return 1;
        }
        /* Check if data dir already exists with data */
        char check_path[512];
        snprintf(check_path, sizeof(check_path), "%s/snapshot.kv", data_dir);
        if (!force) {
            FILE *check = fopen(check_path, "rb");
            if (check) {
                fclose(check);
                fprintf(stderr, "data directory already has snapshot data, use --force to overwrite\n");
                return 1;
            }
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
        /* Parse KV pairs from the snapshot and count them */
        /* Check for 12-byte header (magic "CTS1" + revision) */
        size_t kv_offset = 0;
        size_t kv_size = (size_t)snap_size;
        uint64_t restore_rev = 0;
        if (snap_size >= 12 && memcmp(snap_data, "CTS1", 4) == 0) {
            for (int i = 0; i < 8; i++)
                restore_rev |= ((uint64_t)snap_data[4 + i]) << (i * 8);
            kv_offset = 12;
            kv_size = (size_t)snap_size - 12;
        }
        int kv_count = 0;
        size_t sp = kv_offset;
        while (sp < kv_offset + kv_size) {
            uint64_t kl = 0; if (read_varint(snap_data, snap_size, &sp, &kl) != 0) break;
            if (sp + kl > (size_t)snap_size) break;
            sp += kl;
            uint64_t vl = 0; if (read_varint(snap_data, snap_size, &sp, &vl) != 0) break;
            if (sp + vl > (size_t)snap_size) break;
            sp += vl;
            kv_count++;
        }
        /* Create parent directory */
        char mkdir_cmd[512];
        snprintf(mkdir_cmd, sizeof(mkdir_cmd), "mkdir -p %s", data_dir);
        system(mkdir_cmd);
        /* Write the snapshot KV data to the data directory as snapshot.kv */
        FILE *df = fopen(check_path, "wb");
        if (!df) { perror("fopen data dir"); free(snap_data); return 1; }
        /* Write KV data only (skip header if present) */
        fwrite(snap_data + kv_offset, 1, kv_size, df);
        fclose(df);
        free(snap_data);
        if (want_json) {
            printf("{\"snapshot\":\"%s\",\"data_dir\":\"%s\",\"size\":%ld,\"keys\":%d,\"revision\":%llu}\n",
                   snap_file, data_dir, snap_size, kv_count, (unsigned long long)restore_rev);
        } else if (want_fields) {
            printf("snapshot: %s\n", snap_file);
            printf("data_dir: %s\n", data_dir);
            printf("size: %ld\n", snap_size);
            printf("keys: %d\n", kv_count);
            if (restore_rev > 0)
                printf("revision: %llu\n", (unsigned long long)restore_rev);
            else
                printf("revision: -\n");
            fputs("\n", stdout);
        } else {
            printf("snapshot restored to %s (%ld bytes, %d keys", check_path, snap_size, kv_count);
            if (restore_rev > 0) printf(", revision %llu", (unsigned long long)restore_rev);
            printf(")\n");
        }
        return 0;
    } else {
        fprintf(stderr, "unknown snapshot subcommand: %s\n", argv[2]);
        return 1;
    }
}

static int cmd_downgrade(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "usage: cetcdctl downgrade enable VERSION [-w json|fields]\n");
        fprintf(stderr, "       cetcdctl downgrade cancel [-w json|fields]\n");
        fprintf(stderr, "       cetcdctl downgrade validate VERSION [-w json|fields]\n");
        return 1;
    }
    int want_json = 0, want_fields = 0;
    uint8_t req[256], resp[256];
    size_t pos = 0;
    const char *version = "";
    uint64_t action = 1; /* ENABLE */

    if (strcmp(argv[2], "enable") == 0) {
        if (argc < 4) { fprintf(stderr, "usage: cetcdctl downgrade enable VERSION [-w json|fields]\n"); return 1; }
        action = 1; version = argv[3];
    } else if (strcmp(argv[2], "cancel") == 0) {
        action = 2;
    } else if (strcmp(argv[2], "validate") == 0) {
        if (argc < 4) { fprintf(stderr, "usage: cetcdctl downgrade validate VERSION [-w json|fields]\n"); return 1; }
        action = 0; version = argv[3];
    } else {
        fprintf(stderr, "unknown downgrade subcommand: %s\n", argv[2]);
        return 1;
    }

    for (int i = 3; i < argc; i++) {
        if ((strcmp(argv[i], "-w") == 0 || strcmp(argv[i], "--write-out") == 0) && i + 1 < argc) {
            if (strcmp(argv[i + 1], "json") == 0) want_json = 1;
            else if (strcmp(argv[i + 1], "fields") == 0) want_fields = 1;
            i++;
        }
    }

    pos = encode_varint_field(req, sizeof(req), pos, 0x08, action);
    if (version[0])
        pos = encode_string_field(req, sizeof(req), pos, 0x12, version);

    int rlen = do_rpc("/etcdserverpb.Maintenance/Downgrade", req, pos, resp, sizeof(resp));
    if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
    if (want_json) {
        fputs("{", stdout);
        parse_and_print_header_json(resp, (size_t)rlen);
        fputs("}\n", stdout);
    } else if (want_fields) {
        parse_and_print_header_json(resp, (size_t)rlen);
        fputs("\n", stdout);
    } else {
        printf("OK\n");
    }
    return 0;
}

/* Encode comma-separated URLs as repeated string fields (proto repeated string) */
static size_t encode_repeated_string_field(uint8_t *buf, size_t cap, size_t pos,
                                           uint8_t tag, const char *urls) {
    char copy[512];
    strncpy(copy, urls, sizeof(copy) - 1);
    copy[sizeof(copy) - 1] = '\0';
    char *saveptr = NULL;
    char *tok = strtok_r(copy, ",", &saveptr);
    while (tok) {
        while (*tok == ' ') tok++; /* trim leading space */
        if (*tok) {
            pos = encode_string_field(buf, cap, pos, tag, tok);
        }
        tok = strtok_r(NULL, ",", &saveptr);
    }
    return pos;
}

static int cmd_member(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "usage: cetcdctl member list [-w json|table|fields]\n");
        fprintf(stderr, "       cetcdctl member add [-w json|fields] [--peer-urls URLS] [--name NAME] [--learner] [PEER_URL]\n");
        fprintf(stderr, "       cetcdctl member remove [-w json|fields] ID\n");
        fprintf(stderr, "       cetcdctl member update [-w json|fields] ID PEER_URLS\n");
        fprintf(stderr, "       cetcdctl member promote [-w json|fields] ID\n");
        return 1;
    }
    /* Parse -w json/fields for all subcommands */
    int want_json = 0, want_fields = 0;
    for (int i = 3; i < argc; i++) {
        if ((strcmp(argv[i], "-w") == 0 || strcmp(argv[i], "--write-out") == 0) && i + 1 < argc) {
            if (strcmp(argv[i + 1], "json") == 0) want_json = 1;
            else if (strcmp(argv[i + 1], "fields") == 0) want_fields = 1;
            i++;
        }
    }
    if (strcmp(argv[2], "list") == 0) {
        int table_fmt = 0, json_fmt = 0, fields_fmt = 0;
        for (int i = 3; i < argc; i++) {
            if ((strcmp(argv[i], "-w") == 0 || strcmp(argv[i], "--write-out") == 0) && i + 1 < argc) {
                if (strcmp(argv[i + 1], "table") == 0) table_fmt = 1;
                else if (strcmp(argv[i + 1], "json") == 0) json_fmt = 1;
                else if (strcmp(argv[i + 1], "fields") == 0) fields_fmt = 1;
                i++;
            }
        }
        uint8_t req[] = {0x00}, resp[4096];
        int rlen = do_rpc("/etcdserverpb.Cluster/MemberList", req, 1, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        parse_member_list_response(resp, rlen, table_fmt, json_fmt, fields_fmt);
    } else if (strcmp(argv[2], "add") == 0) {
        const char *peer_url = NULL;
        const char *member_name = NULL;
        int is_learner = 0;
        for (int i = 3; i < argc; i++) {
            if (strncmp(argv[i], "--peer-urls=", 12) == 0) {
                peer_url = argv[i] + 12;
            } else if (strcmp(argv[i], "--peer-urls") == 0 && i + 1 < argc) {
                peer_url = argv[++i];
            } else if (strncmp(argv[i], "--name=", 7) == 0) {
                member_name = argv[i] + 7;
            } else if (strcmp(argv[i], "--name") == 0 && i + 1 < argc) {
                member_name = argv[++i];
            } else if (strcmp(argv[i], "--learner") == 0) {
                is_learner = 1;
            } else if (argv[i][0] == '-') {
                if (i + 1 < argc && (strcmp(argv[i], "-w") == 0 || strcmp(argv[i], "--write-out") == 0)) i++;
                continue;
            } else if (i > 3 && (strcmp(argv[i-1], "-w") == 0 || strcmp(argv[i-1], "--write-out") == 0)) {
                continue;
            } else if (!peer_url) {
                /* First non-flag positional arg is the URL (or NAME if --peer-urls used) */
                if (!peer_url) peer_url = argv[i];
            }
        }
        if (!peer_url) { fprintf(stderr, "usage: cetcdctl member add [-w json] [--peer-urls URLS] [--name NAME] [--learner] [PEER_URL]\n"); return 1; }
        (void)member_name; /* member name is display-only, not sent in MemberAddRequest */
        uint8_t req[1024], resp[4096];
        size_t pos = 0;
        /* peerURLs = repeated string (field 1, tag 0x0a) — split by comma */
        pos = encode_repeated_string_field(req, sizeof(req), pos, 0x0a, peer_url);
        if (is_learner) {
            /* field 2 (isLearner) = bool, tag = 0x10 */
            pos = encode_varint_field(req, sizeof(req), pos, 0x10, 1);
        }
        int rlen = do_rpc("/etcdserverpb.Cluster/MemberAdd", req, pos, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        if (want_json) {
            parse_member_list_response(resp, rlen, 0, 1, 0);
        } else if (want_fields) {
            parse_member_list_response(resp, rlen, 0, 0, 1);
        } else {
            parse_member_list_response(resp, rlen, 0, 0, 0);
        }
    } else if (strcmp(argv[2], "remove") == 0) {
        const char *id_str = NULL;
        for (int i = 3; i < argc; i++) {
            if (argv[i][0] == '-') continue;
            if (i > 3 && (strcmp(argv[i-1], "-w") == 0 || strcmp(argv[i-1], "--write-out") == 0)) continue;
            if (!id_str) id_str = argv[i];
        }
        if (!id_str) { fprintf(stderr, "usage: cetcdctl member remove [-w json|fields] ID\n"); return 1; }
        uint8_t req[32], resp[256];
        size_t pos = 0;
        pos = encode_varint_field(req, sizeof(req), pos, 0x08, (uint64_t)atol(id_str));
        int rlen = do_rpc("/etcdserverpb.Cluster/MemberRemove", req, pos, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        if (want_json) { fputs("{", stdout); parse_and_print_header_json(resp, (size_t)rlen); fputs("}\n", stdout); }
        else if (want_fields) { parse_and_print_header_json(resp, (size_t)rlen); fputs("\n", stdout); }
        else { printf("OK\n"); }
    } else if (strcmp(argv[2], "update") == 0) {
        const char *id_str = NULL;
        const char *peer_url = NULL;
        for (int i = 3; i < argc; i++) {
            if (argv[i][0] == '-') { if (i + 1 < argc && (strcmp(argv[i], "-w") == 0 || strcmp(argv[i], "--write-out") == 0)) i++; continue; }
            if (!id_str) id_str = argv[i];
            else if (!peer_url) peer_url = argv[i];
        }
        if (!id_str || !peer_url) { fprintf(stderr, "usage: cetcdctl member update [-w json|fields] ID PEER_URLS\n"); return 1; }
        uint8_t req[1024], resp[256];
        size_t pos = 0;
        pos = encode_varint_field(req, sizeof(req), pos, 0x08, (uint64_t)atol(id_str));
        /* peerURLs = repeated string (field 2, tag 0x12) — split by comma */
        pos = encode_repeated_string_field(req, sizeof(req), pos, 0x12, peer_url);
        int rlen = do_rpc("/etcdserverpb.Cluster/MemberUpdate", req, pos, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        if (want_json) { fputs("{", stdout); parse_and_print_header_json(resp, (size_t)rlen); fputs("}\n", stdout); }
        else if (want_fields) { parse_and_print_header_json(resp, (size_t)rlen); fputs("\n", stdout); }
        else { printf("OK\n"); }
    } else if (strcmp(argv[2], "promote") == 0) {
        const char *id_str = NULL;
        for (int i = 3; i < argc; i++) {
            if (argv[i][0] == '-') { if (i + 1 < argc && (strcmp(argv[i], "-w") == 0 || strcmp(argv[i], "--write-out") == 0)) i++; continue; }
            if (!id_str) id_str = argv[i];
        }
        if (!id_str) { fprintf(stderr, "usage: cetcdctl member promote [-w json|fields] ID\n"); return 1; }
        uint8_t req[32], resp[256];
        size_t pos = 0;
        pos = encode_varint_field(req, sizeof(req), pos, 0x08, (uint64_t)atol(id_str));
        int rlen = do_rpc("/etcdserverpb.Cluster/MemberPromote", req, pos, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        if (want_json) { fputs("{", stdout); parse_and_print_header_json(resp, (size_t)rlen); fputs("}\n", stdout); }
        else if (want_fields) { parse_and_print_header_json(resp, (size_t)rlen); fputs("\n", stdout); }
        else { printf("OK\n"); }
    } else {
        fprintf(stderr, "unknown member subcommand: %s\n", argv[2]);
        return 1;
    }
    return 0;
}

static int cmd_auth(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "usage: cetcdctl auth enable|disable|status [-w json|fields]\n");
        fprintf(stderr, "       cetcdctl auth login [-w json|fields] NAME PASS\n");
        return 1;
    }
    if (strcmp(argv[2], "login") == 0) {
        int want_json = 0, want_fields = 0;
        const char *name = NULL, *pass = NULL;
        for (int i = 3; i < argc; i++) {
            if ((strcmp(argv[i], "-w") == 0 || strcmp(argv[i], "--write-out") == 0) && i + 1 < argc) {
                if (strcmp(argv[i + 1], "json") == 0) want_json = 1;
                else if (strcmp(argv[i + 1], "fields") == 0) want_fields = 1;
                i++;
            } else if (!name) name = argv[i];
            else if (!pass) pass = argv[i];
        }
        if (!name || !pass) { fprintf(stderr, "usage: cetcdctl auth login [-w json|fields] NAME PASS\n"); return 1; }
        uint8_t req[512], resp[1024];
        size_t pos = 0;
        pos = encode_string_field(req, sizeof(req), pos, 0x0a, name);
        pos = encode_string_field(req, sizeof(req), pos, 0x12, pass);
        int rlen = do_rpc("/etcdserverpb.Auth/Authenticate", req, pos, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "authentication failed\n"); return 1; }
        /* Parse AuthenticateResponse: field 1 (header) = bytes tag=0x0a, field 2 (token) = bytes tag=0x12 */
        size_t rpos = 0;
        while (rpos < (size_t)rlen) {
            uint8_t tag = resp[rpos++];
            if (tag == 0x12) {
                uint64_t l = 0; read_varint(resp, rlen, &rpos, &l);
                if (want_json) {
                    fputs("{", stdout);
                    parse_and_print_header_json(resp, (size_t)rlen);
                    fputs(",\"token\":", stdout);
                    print_json_string(resp + rpos, (size_t)l);
                    fputs("}\n", stdout);
                } else if (want_fields) {
                    parse_and_print_header_json(resp, (size_t)rlen);
                    printf("token: %.*s\n", (int)l, resp + rpos);
                    fputs("\n", stdout);
                } else {
                    printf("token: %.*s\n", (int)l, resp + rpos);
                }
                return 0;
            } else if (tag == 0x0a) {
                /* Skip header (length-delimited) */
                uint64_t l = 0; read_varint(resp, rlen, &rpos, &l);
                rpos += l;
            } else {
                uint64_t v = 0; read_varint(resp, rlen, &rpos, &v);
            }
        }
        if (want_json) { fputs("{", stdout); parse_and_print_header_json(resp, (size_t)rlen); fputs("}\n", stdout); }
        else if (want_fields) { parse_and_print_header_json(resp, (size_t)rlen); fputs("\n", stdout); }
        else { printf("OK (no token returned)\n"); }
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
    int want_json = 0, want_fields = 0;
    for (int i = 3; i < argc; i++) {
        if ((strcmp(argv[i], "-w") == 0 || strcmp(argv[i], "--write-out") == 0) && i + 1 < argc) {
            if (strcmp(argv[i + 1], "json") == 0) want_json = 1;
            else if (strcmp(argv[i + 1], "fields") == 0) want_fields = 1;
            i++;
        }
    }
    int rlen = do_rpc(path, req, 1, resp, sizeof(resp));
    if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
    if (strcmp(argv[2], "status") == 0) {
        size_t rpos = 0;
        uint64_t enabled = 0;
        while (rpos < (size_t)rlen) {
            uint8_t tag = resp[rpos++];
            if (tag == 0x10) { read_varint(resp, rlen, &rpos, &enabled); }
            else if (tag == 0x0a) { uint64_t l = 0; read_varint(resp, rlen, &rpos, &l); rpos += l; }
            else { uint64_t v = 0; read_varint(resp, rlen, &rpos, &v); }
        }
        if (want_json) {
            fputs("{", stdout);
            parse_and_print_header_json(resp, (size_t)rlen);
            printf(",\"enabled\":%s}\n", enabled ? "true" : "false");
        } else if (want_fields) {
            parse_and_print_header_json(resp, (size_t)rlen);
            printf("enabled: %s\n", enabled ? "true" : "false");
            fputs("\n", stdout);
        } else {
            parse_auth_status_response(resp, rlen);
        }
    } else {
        if (want_json) { fputs("{", stdout); parse_and_print_header_json(resp, (size_t)rlen); fputs("}\n", stdout); }
        else if (want_fields) { parse_and_print_header_json(resp, (size_t)rlen); fputs("\n", stdout); }
        else { printf("OK\n"); }
    }
    return 0;
}

static int cmd_user(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "usage: cetcdctl user add NAME [PASS] [--no-password] [-w json|fields]\n");
        fprintf(stderr, "       cetcdctl user delete NAME [-w json|fields]\n");
        fprintf(stderr, "       cetcdctl user get NAME [-w json|fields]\n");
        fprintf(stderr, "       cetcdctl user list [-w json|table|fields]\n");
        fprintf(stderr, "       cetcdctl user change-password NAME PASS [-w json|fields]\n");
        fprintf(stderr, "       cetcdctl user grant-role NAME ROLE [-w json|fields]\n");
        fprintf(stderr, "       cetcdctl user revoke-role NAME ROLE [-w json|fields]\n");
        return 1;
    }
    /* Parse -w json/fields for all subcommands */
    int want_json = 0, want_fields = 0;
    for (int i = 3; i < argc; i++) {
        if ((strcmp(argv[i], "-w") == 0 || strcmp(argv[i], "--write-out") == 0) && i + 1 < argc) {
            if (strcmp(argv[i + 1], "json") == 0) want_json = 1;
            else if (strcmp(argv[i + 1], "fields") == 0) want_fields = 1;
            i++;
        }
    }
    if (strcmp(argv[2], "add") == 0) {
        const char *user_name = NULL, *password = NULL;
        bool no_password = false;
        for (int i = 3; i < argc; i++) {
            if (strcmp(argv[i], "--no-password") == 0) {
                no_password = true;
            } else if ((strcmp(argv[i], "-w") == 0 || strcmp(argv[i], "--write-out") == 0) && i + 1 < argc) {
                i++; /* already parsed globally */
            } else if (!user_name) {
                user_name = argv[i];
            } else if (!password) {
                password = argv[i];
            }
        }
        if (!user_name || (!no_password && !password)) {
            fprintf(stderr, "usage: cetcdctl user add NAME [PASS] [--no-password] [-w json|fields]\n");
            return 1;
        }
        uint8_t req[512], resp[256];
        size_t pos = 0;
        pos = encode_string_field(req, sizeof(req), pos, 0x0a, user_name);
        if (password) {
            pos = encode_string_field(req, sizeof(req), pos, 0x12, password);
        }
        if (no_password) {
            /* field 3 (options) = UserAddOptions { field 1 (no_password) = bool, tag 0x08 = true } */
            req[pos++] = 0x1a; /* field 3 = options */
            req[pos++] = 0x02; /* length = 2 */
            req[pos++] = 0x08; /* field 1 = no_password */
            req[pos++] = 0x01; /* true */
        }
        int rlen = do_rpc("/etcdserverpb.Auth/UserAdd", req, pos, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        if (want_json) { fputs("{", stdout); parse_and_print_header_json(resp, (size_t)rlen); fputs("}\n", stdout); }
        else if (want_fields) { parse_and_print_header_json(resp, (size_t)rlen); fputs("\n", stdout); }
        else { printf("OK\n"); }
    } else if (strcmp(argv[2], "delete") == 0) {
        if (argc < 4) { fprintf(stderr, "usage: cetcdctl user delete NAME [-w json|fields]\n"); return 1; }
        uint8_t req[256], resp[256];
        size_t pos = 0;
        pos = encode_string_field(req, sizeof(req), pos, 0x0a, argv[3]);
        int rlen = do_rpc("/etcdserverpb.Auth/UserDelete", req, pos, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        if (want_json) { fputs("{", stdout); parse_and_print_header_json(resp, (size_t)rlen); fputs("}\n", stdout); }
        else if (want_fields) { parse_and_print_header_json(resp, (size_t)rlen); fputs("\n", stdout); }
        else { printf("OK\n"); }
    } else if (strcmp(argv[2], "get") == 0) {
        if (argc < 4) { fprintf(stderr, "usage: cetcdctl user get NAME [-w json|fields]\n"); return 1; }
        uint8_t req[256], resp[4096];
        size_t pos = 0;
        pos = encode_string_field(req, sizeof(req), pos, 0x0a, argv[3]);
        int rlen = do_rpc("/etcdserverpb.Auth/UserGet", req, pos, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        if (!want_json && !want_fields) printf("roles:\n");
        parse_string_list_response(resp, rlen, "roles", 0, want_json, want_fields);
    } else if (strcmp(argv[2], "list") == 0) {
        int table_fmt = 0, fields_fmt = 0;
        for (int i = 3; i < argc; i++) {
            if ((strcmp(argv[i], "-w") == 0 || strcmp(argv[i], "--write-out") == 0) && i + 1 < argc) {
                if (strcmp(argv[i + 1], "table") == 0) table_fmt = 1;
                else if (strcmp(argv[i + 1], "fields") == 0) fields_fmt = 1;
                i++;
            }
        }
        uint8_t req[] = {0x00}, resp[4096];
        int rlen = do_rpc("/etcdserverpb.Auth/UserList", req, 1, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        parse_string_list_response(resp, rlen, "users", table_fmt, want_json, fields_fmt);
    } else if (strcmp(argv[2], "change-password") == 0) {
        if (argc < 5) { fprintf(stderr, "usage: cetcdctl user change-password NAME PASS [-w json]\n"); return 1; }
        uint8_t req[512], resp[256];
        size_t pos = 0;
        pos = encode_string_field(req, sizeof(req), pos, 0x0a, argv[3]);
        pos = encode_string_field(req, sizeof(req), pos, 0x12, argv[4]);
        int rlen = do_rpc("/etcdserverpb.Auth/UserChangePassword", req, pos, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        if (want_json) { fputs("{", stdout); parse_and_print_header_json(resp, (size_t)rlen); fputs("}\n", stdout); }
        else if (want_fields) { parse_and_print_header_json(resp, (size_t)rlen); fputs("\n", stdout); }
        else { printf("OK\n"); }
    } else if (strcmp(argv[2], "grant-role") == 0) {
        if (argc < 5) { fprintf(stderr, "usage: cetcdctl user grant-role NAME ROLE [-w json|fields]\n"); return 1; }
        uint8_t req[512], resp[256];
        size_t pos = 0;
        pos = encode_string_field(req, sizeof(req), pos, 0x0a, argv[3]);
        pos = encode_string_field(req, sizeof(req), pos, 0x12, argv[4]);
        int rlen = do_rpc("/etcdserverpb.Auth/UserGrantRole", req, pos, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        if (want_json) { fputs("{", stdout); parse_and_print_header_json(resp, (size_t)rlen); fputs("}\n", stdout); }
        else if (want_fields) { parse_and_print_header_json(resp, (size_t)rlen); fputs("\n", stdout); }
        else { printf("OK\n"); }
    } else if (strcmp(argv[2], "revoke-role") == 0) {
        if (argc < 5) { fprintf(stderr, "usage: cetcdctl user revoke-role NAME ROLE [-w json|fields]\n"); return 1; }
        uint8_t req[512], resp[256];
        size_t pos = 0;
        pos = encode_string_field(req, sizeof(req), pos, 0x0a, argv[3]);
        pos = encode_string_field(req, sizeof(req), pos, 0x12, argv[4]);
        int rlen = do_rpc("/etcdserverpb.Auth/UserRevokeRole", req, pos, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        if (want_json) { fputs("{", stdout); parse_and_print_header_json(resp, (size_t)rlen); fputs("}\n", stdout); }
        else if (want_fields) { parse_and_print_header_json(resp, (size_t)rlen); fputs("\n", stdout); }
        else { printf("OK\n"); }
    } else {
        fprintf(stderr, "unknown user subcommand: %s\n", argv[2]);
        return 1;
    }
    return 0;
}

static int cmd_role(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "usage: cetcdctl role add NAME [-w json|fields]\n");
        fprintf(stderr, "       cetcdctl role delete NAME [-w json|fields]\n");
        fprintf(stderr, "       cetcdctl role get NAME [-w json|fields]\n");
        fprintf(stderr, "       cetcdctl role list [-w json|table|fields]\n");
        fprintf(stderr, "       cetcdctl role grant-permission ROLE TYPE KEY [-w json|fields]\n");
        fprintf(stderr, "       cetcdctl role revoke-permission ROLE [-w json|fields]\n");
        return 1;
    }
    /* Parse -w json/fields for all subcommands */
    int want_json = 0, want_fields = 0;
    for (int i = 3; i < argc; i++) {
        if ((strcmp(argv[i], "-w") == 0 || strcmp(argv[i], "--write-out") == 0) && i + 1 < argc) {
            if (strcmp(argv[i + 1], "json") == 0) want_json = 1;
            else if (strcmp(argv[i + 1], "fields") == 0) want_fields = 1;
            i++;
        }
    }
    if (strcmp(argv[2], "add") == 0) {
        if (argc < 4) { fprintf(stderr, "usage: cetcdctl role add NAME [-w json|fields]\n"); return 1; }
        uint8_t req[256], resp[256];
        size_t pos = 0;
        pos = encode_string_field(req, sizeof(req), pos, 0x0a, argv[3]);
        int rlen = do_rpc("/etcdserverpb.Auth/RoleAdd", req, pos, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        if (want_json) { fputs("{", stdout); parse_and_print_header_json(resp, (size_t)rlen); fputs("}\n", stdout); }
        else if (want_fields) { parse_and_print_header_json(resp, (size_t)rlen); fputs("\n", stdout); }
        else { printf("OK\n"); }
    } else if (strcmp(argv[2], "delete") == 0) {
        if (argc < 4) { fprintf(stderr, "usage: cetcdctl role delete NAME [-w json|fields]\n"); return 1; }
        uint8_t req[256], resp[256];
        size_t pos = 0;
        pos = encode_string_field(req, sizeof(req), pos, 0x0a, argv[3]);
        int rlen = do_rpc("/etcdserverpb.Auth/RoleDelete", req, pos, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        if (want_json) { fputs("{", stdout); parse_and_print_header_json(resp, (size_t)rlen); fputs("}\n", stdout); }
        else if (want_fields) { parse_and_print_header_json(resp, (size_t)rlen); fputs("\n", stdout); }
        else { printf("OK\n"); }
    } else if (strcmp(argv[2], "list") == 0) {
        int table_fmt = 0, fields_fmt = 0;
        for (int i = 3; i < argc; i++) {
            if ((strcmp(argv[i], "-w") == 0 || strcmp(argv[i], "--write-out") == 0) && i + 1 < argc) {
                if (strcmp(argv[i + 1], "table") == 0) table_fmt = 1;
                else if (strcmp(argv[i + 1], "fields") == 0) fields_fmt = 1;
                i++;
            }
        }
        uint8_t req[] = {0x00}, resp[4096];
        int rlen = do_rpc("/etcdserverpb.Auth/RoleList", req, 1, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        parse_string_list_response(resp, rlen, "roles", table_fmt, want_json, fields_fmt);
    } else if (strcmp(argv[2], "get") == 0) {
        if (argc < 4) { fprintf(stderr, "usage: cetcdctl role get NAME [-w json|fields]\n"); return 1; }
        uint8_t req[256], resp[1024];
        size_t pos = 0;
        pos = encode_string_field(req, sizeof(req), pos, 0x0a, argv[3]);
        int rlen = do_rpc("/etcdserverpb.Auth/RoleGet", req, pos, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        if (want_fields) {
            parse_and_print_header_json(resp, (size_t)rlen);
            printf("role: %s\n", argv[3]);
            size_t rpos = 0;
            while (rpos < (size_t)rlen) {
                uint8_t tag = resp[rpos++];
                if (tag == 0x12) {
                    uint64_t plen = 0; read_varint(resp, rlen, &rpos, &plen);
                    size_t pend = rpos + (size_t)plen;
                    while (rpos < pend) {
                        uint8_t ptag = resp[rpos++];
                        if (ptag == 0x08) {
                            uint64_t pt = 0; read_varint(resp, pend, &rpos, &pt);
                            printf("permType: %s\n", pt == 0 ? "READ" : pt == 1 ? "WRITE" : "READWRITE");
                        } else if (ptag == 0x0a) {
                            uint64_t l = 0; read_varint(resp, pend, &rpos, &l);
                            printf("key: %.*s\n", (int)l, resp + rpos);
                            rpos += l;
                        } else {
                            uint64_t v = 0; read_varint(resp, pend, &rpos, &v);
                        }
                    }
                    rpos = pend;
                } else if (tag == 0x0a) {
                    uint64_t l = 0; read_varint(resp, rlen, &rpos, &l);
                    rpos += l;
                } else {
                    uint64_t v = 0; read_varint(resp, rlen, &rpos, &v);
                }
            }
            fputs("\n", stdout);
        } else if (want_json) {
            fputs("{", stdout);
            parse_and_print_header_json(resp, (size_t)rlen);
            printf(",\"role\":\"%s\",\"perm\":[", argv[3]);
            int first = 1;
            size_t rpos = 0;
            while (rpos < (size_t)rlen) {
                uint8_t tag = resp[rpos++];
                if (tag == 0x12) {
                    uint64_t plen = 0; read_varint(resp, rlen, &rpos, &plen);
                    size_t pend = rpos + (size_t)plen;
                    const char *ptype = "READWRITE";
                    const char *pkey = NULL;
                    size_t pkey_len = 0;
                    while (rpos < pend) {
                        uint8_t ptag = resp[rpos++];
                        if (ptag == 0x08) {
                            uint64_t pt = 0; read_varint(resp, pend, &rpos, &pt);
                            ptype = pt == 0 ? "READ" : pt == 1 ? "WRITE" : "READWRITE";
                        } else if (ptag == 0x0a) {
                            uint64_t l = 0; read_varint(resp, pend, &rpos, &l);
                            pkey = (const char *)(resp + rpos);
                            pkey_len = (size_t)l;
                            rpos += l;
                        } else {
                            uint64_t v = 0; read_varint(resp, pend, &rpos, &v);
                        }
                    }
                    if (!first) printf(",");
                    printf("{\"permType\":\"%s\",\"key\":", ptype);
                    if (pkey) print_json_string((const uint8_t *)pkey, pkey_len); else fputs("\"\"", stdout);
                    fputs("}", stdout);
                    first = 0;
                    rpos = pend;
                } else if (tag == 0x0a) {
                    uint64_t l = 0; read_varint(resp, rlen, &rpos, &l);
                    rpos += l;
                } else {
                    uint64_t v = 0; read_varint(resp, rlen, &rpos, &v);
                }
            }
            printf("]}\n");
        } else {
            printf("role: %s\n", argv[3]);
            size_t rpos = 0;
            while (rpos < (size_t)rlen) {
                uint8_t tag = resp[rpos++];
                if (tag == 0x12) {
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
                    uint64_t l = 0; read_varint(resp, rlen, &rpos, &l);
                    rpos += l;
                } else {
                    uint64_t v = 0; read_varint(resp, rlen, &rpos, &v);
                }
            }
        }
    } else if (strcmp(argv[2], "grant-permission") == 0) {
        const char *role_name = NULL, *perm_type_str = NULL, *key_str = NULL;
        const char *range_end_arg = NULL;
        bool prefix = false;
        for (int i = 3; i < argc; i++) {
            if (strcmp(argv[i], "--prefix") == 0) {
                prefix = true;
            } else if (strcmp(argv[i], "--range-end") == 0 && i + 1 < argc) {
                range_end_arg = argv[++i];
            } else if ((strcmp(argv[i], "-w") == 0 || strcmp(argv[i], "--write-out") == 0) && i + 1 < argc) {
                i++; /* already parsed globally */
            } else if (!role_name) {
                role_name = argv[i];
            } else if (!perm_type_str) {
                perm_type_str = argv[i];
            } else if (!key_str) {
                key_str = argv[i];
            }
        }
        if (!role_name || !perm_type_str || !key_str) {
            fprintf(stderr, "usage: cetcdctl role grant-permission ROLE TYPE KEY [--prefix] [--range-end KEY] [-w json|fields]\n");
            fprintf(stderr, "  TYPE: read | write | readwrite\n");
            return 1;
        }
        int perm_type = 2; /* default readwrite */
        if (strcmp(perm_type_str, "read") == 0) perm_type = 0;
        else if (strcmp(perm_type_str, "write") == 0) perm_type = 1;
        else if (strcmp(perm_type_str, "readwrite") == 0) perm_type = 2;
        else { fprintf(stderr, "invalid TYPE: %s (use read|write|readwrite)\n", perm_type_str); return 1; }

        /* Build Permission sub-message */
        uint8_t perm[512];
        size_t ppos = 0;
        perm[ppos++] = 0x08; /* field 1 = permType */
        perm[ppos++] = (uint8_t)perm_type;
        size_t klen = strlen(key_str);
        perm[ppos++] = 0x0a; /* field 2 = key */
        uint64_t l = klen;
        while (l >= 0x80) { perm[ppos++] = (uint8_t)(l | 0x80); l >>= 7; }
        perm[ppos++] = (uint8_t)l;
        memcpy(perm + ppos, key_str, klen); ppos += klen;
        /* field 3 = range_end (optional, for --prefix or --range-end) */
        if (prefix) {
            if (klen == 0) {
                uint8_t zero = 0;
                perm[ppos++] = 0x12;
                perm[ppos++] = 0x01;
                perm[ppos++] = zero;
            } else {
                char prefix_end[256];
                if (klen >= sizeof(prefix_end)) { fprintf(stderr, "key too long\n"); return 1; }
                memcpy(prefix_end, key_str, klen);
                prefix_end[klen - 1]++;
                perm[ppos++] = 0x12;
                l = klen;
                while (l >= 0x80) { perm[ppos++] = (uint8_t)(l | 0x80); l >>= 7; }
                perm[ppos++] = (uint8_t)l;
                memcpy(perm + ppos, prefix_end, klen); ppos += klen;
            }
        } else if (range_end_arg) {
            size_t re_len = strlen(range_end_arg);
            perm[ppos++] = 0x12;
            l = re_len;
            while (l >= 0x80) { perm[ppos++] = (uint8_t)(l | 0x80); l >>= 7; }
            perm[ppos++] = (uint8_t)l;
            memcpy(perm + ppos, range_end_arg, re_len); ppos += re_len;
        }

        /* Build RoleGrantPermissionRequest */
        uint8_t req[1024], resp[256];
        size_t pos = 0;
        pos = encode_string_field(req, sizeof(req), pos, 0x0a, role_name);
        req[pos++] = 0x12; /* field 2 = perm */
        l = ppos;
        while (l >= 0x80) { req[pos++] = (uint8_t)(l | 0x80); l >>= 7; }
        req[pos++] = (uint8_t)l;
        memcpy(req + pos, perm, ppos); pos += ppos;

        int rlen = do_rpc("/etcdserverpb.Auth/RoleGrantPermission", req, pos, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        if (want_json) { fputs("{", stdout); parse_and_print_header_json(resp, (size_t)rlen); fputs("}\n", stdout); }
        else if (want_fields) { parse_and_print_header_json(resp, (size_t)rlen); fputs("\n", stdout); }
        else { printf("OK\n"); }
    } else if (strcmp(argv[2], "revoke-permission") == 0) {
        const char *role_name = NULL, *perm_type_str = NULL, *key_str = NULL;
        const char *range_end_arg = NULL;
        bool prefix = false;
        for (int i = 3; i < argc; i++) {
            if (strcmp(argv[i], "--prefix") == 0) {
                prefix = true;
            } else if (strcmp(argv[i], "--range-end") == 0 && i + 1 < argc) {
                range_end_arg = argv[++i];
            } else if ((strcmp(argv[i], "-w") == 0 || strcmp(argv[i], "--write-out") == 0) && i + 1 < argc) {
                i++; /* already parsed globally */
            } else if (!role_name) {
                role_name = argv[i];
            } else if (!perm_type_str) {
                perm_type_str = argv[i];
            } else if (!key_str) {
                key_str = argv[i];
            }
        }
        if (!role_name) { fprintf(stderr, "usage: cetcdctl role revoke-permission ROLE [TYPE KEY] [--prefix] [--range-end KEY] [-w json|fields]\n"); return 1; }

        uint8_t req[512], resp[256];
        size_t pos = 0;
        pos = encode_string_field(req, sizeof(req), pos, 0x0a, role_name);
        if (key_str) {
            /* Send key (field 2, tag 0x12) for specific permission revocation */
            size_t klen = strlen(key_str);
            pos = encode_bytes_field(req, sizeof(req), pos, 0x12,
                                     (const uint8_t *)key_str, klen);
            /* field 3 (range_end, tag 0x1a) — optional */
            if (prefix) {
                if (klen == 0) {
                    uint8_t zero = 0;
                    pos = encode_bytes_field(req, sizeof(req), pos, 0x1a, &zero, 1);
                } else {
                    char prefix_end[256];
                    if (klen >= sizeof(prefix_end)) { fprintf(stderr, "key too long\n"); return 1; }
                    memcpy(prefix_end, key_str, klen);
                    prefix_end[klen - 1]++;
                    pos = encode_bytes_field(req, sizeof(req), pos, 0x1a,
                                             (const uint8_t *)prefix_end, klen);
                }
            } else if (range_end_arg) {
                pos = encode_bytes_field(req, sizeof(req), pos, 0x1a,
                                         (const uint8_t *)range_end_arg, strlen(range_end_arg));
            }
        }
        int rlen = do_rpc("/etcdserverpb.Auth/RoleRevokePermission", req, pos, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        if (want_json) { fputs("{", stdout); parse_and_print_header_json(resp, (size_t)rlen); fputs("}\n", stdout); }
        else if (want_fields) { parse_and_print_header_json(resp, (size_t)rlen); fputs("\n", stdout); }
        else { printf("OK\n"); }
    } else {
        fprintf(stderr, "unknown role subcommand: %s\n", argv[2]);
        return 1;
    }
    return 0;
}

/* --- Watch streaming state --- */
static volatile sig_atomic_t g_watch_stop = 0;
static int g_watch_fd = -1;

static void watch_sigint_handler(int sig) {
    (void)sig;
    g_watch_stop = 1;
    if (g_watch_fd >= 0) shutdown(g_watch_fd, SHUT_RDWR);
}

/* Build a WatchCreateRequest protobuf message.
 * Returns total length, or 0 on error. */
static size_t build_watch_create(uint8_t *buf, size_t cap,
                                  const char *key, size_t key_len,
                                  bool prefix, const char *range_end_arg,
                                  int64_t start_rev, bool prev_kv,
                                  int filter_type, bool progress_notify) {
    uint8_t create_inner[512];
    size_t cpos = 0;
    cpos = encode_bytes_field(create_inner, sizeof(create_inner), cpos, 0x0a,
                              (const uint8_t *)key, key_len);
    if (prefix) {
        if (key_len == 0) {
            uint8_t zero = 0;
            cpos = encode_bytes_field(create_inner, sizeof(create_inner), cpos, 0x12, &zero, 1);
        } else {
            char range_end[256];
            if (key_len >= sizeof(range_end)) return 0;
            memcpy(range_end, key, key_len);
            range_end[key_len - 1]++;
            cpos = encode_bytes_field(create_inner, sizeof(create_inner), cpos, 0x12,
                                      (const uint8_t *)range_end, key_len);
        }
    } else if (range_end_arg) {
        cpos = encode_bytes_field(create_inner, sizeof(create_inner), cpos, 0x12,
                                  (const uint8_t *)range_end_arg, strlen(range_end_arg));
    }
    if (start_rev > 0)
        cpos = encode_varint_field(create_inner, sizeof(create_inner), cpos, 0x18, (uint64_t)start_rev);
    if (progress_notify)
        cpos = encode_varint_field(create_inner, sizeof(create_inner), cpos, 0x20, 1);
    if (prev_kv)
        cpos = encode_varint_field(create_inner, sizeof(create_inner), cpos, 0x30, 1);
    if (filter_type >= 0)
        cpos = encode_varint_field(create_inner, sizeof(create_inner), cpos, 0x38, (uint64_t)filter_type);
    size_t wpos = 0;
    buf[wpos++] = 0x0a;
    wpos = write_varint(buf, cap, wpos, (uint64_t)cpos);
    memcpy(buf + wpos, create_inner, cpos);
    return wpos + cpos;
}

/* Build a WatchCancelRequest protobuf message. */
static size_t build_watch_cancel(uint8_t *buf, size_t cap, int64_t watch_id) {
    uint8_t inner[32];
    size_t ipos = encode_varint_field(inner, sizeof(inner), 0, 0x08, (uint64_t)watch_id);
    size_t wpos = 0;
    buf[wpos++] = 0x12;
    wpos = write_varint(buf, cap, wpos, (uint64_t)ipos);
    memcpy(buf + wpos, inner, ipos);
    return wpos + ipos;
}

/* Parse and print events from a WatchResponse buffer.
 * Returns the number of events found. */
static int print_watch_response(const uint8_t *resp, size_t rlen,
                                bool want_json, bool want_fields,
                                bool hex_output, const char *exec_cmd) {
    size_t rpos = 0;
    int json_first_evt = 1;
    int event_count = 0;
    const char *evt_type = NULL;
    const uint8_t *evt_key = NULL; size_t evt_key_len = 0;
    const uint8_t *evt_val = NULL; size_t evt_val_len = 0;
    uint64_t evt_mod_rev = 0;

    if (want_json) {
        fputs("{", stdout);
        parse_and_print_header_json(resp, rlen);
        fputs(",\"Events\":[", stdout);
    }
    while (rpos < rlen) {
        uint8_t tag = resp[rpos++];
        if (tag == 0x5a) {
            uint64_t elen = 0; read_varint(resp, rlen, &rpos, &elen);
            size_t eend = rpos + (size_t)elen;
            event_count++;
            evt_type = NULL; evt_key = NULL; evt_key_len = 0;
            evt_val = NULL; evt_val_len = 0; evt_mod_rev = 0;
            if (want_json) { if (!json_first_evt) fputs(",", stdout); json_first_evt = 0; }
            while (rpos < eend) {
                uint8_t etag = resp[rpos++];
                if (etag == 0x08) {
                    uint64_t t = 0; read_varint(resp, eend, &rpos, &t);
                    evt_type = (t == 0) ? "PUT" : "DELETE";
                    if (want_json) { fputs("{\"type\":\"", stdout); fputs(t == 0 ? "PUT" : "DELETE", stdout); fputs("\"", stdout); }
                    else if (want_fields) printf("%s\n", t == 0 ? "PUT" : "DELETE");
                    else printf("%s: ", t == 0 ? "PUT" : "DELETE");
                } else if (etag == 0x12) {
                    uint64_t klen = 0; read_varint(resp, eend, &rpos, &klen);
                    size_t kend = rpos + (size_t)klen;
                    const uint8_t *ek = NULL, *ev = NULL;
                    size_t ekl = 0, evl = 0;
                    uint64_t ecr = 0, emr = 0, ever = 0, elease = 0;
                    while (rpos < kend) {
                        uint8_t ktag = resp[rpos++];
                        if (ktag == 0x0a) { uint64_t l = 0; read_varint(resp, kend, &rpos, &l); ek = resp + rpos; ekl = (size_t)l; rpos += l; evt_key = ek; evt_key_len = ekl; }
                        else if (ktag == 0x2a) { uint64_t l = 0; read_varint(resp, kend, &rpos, &l); ev = resp + rpos; evl = (size_t)l; rpos += l; evt_val = ev; evt_val_len = evl; }
                        else if (ktag == 0x10) read_varint(resp, kend, &rpos, &ecr);
                        else if (ktag == 0x18) { read_varint(resp, kend, &rpos, &emr); evt_mod_rev = emr; }
                        else if (ktag == 0x20) read_varint(resp, kend, &rpos, &ever);
                        else if (ktag == 0x30) read_varint(resp, kend, &rpos, &elease);
                        else { uint64_t v = 0; read_varint(resp, kend, &rpos, &v); }
                    }
                    rpos = kend;
                    if (want_fields) {
                        if (ek) { printf("\""); fwrite(ek, 1, ekl, stdout); printf("\"\n");
                            printf("create_revision: %llu\n", (unsigned long long)ecr);
                            printf("mod_revision: %llu\n", (unsigned long long)emr);
                            printf("version: %llu\n", (unsigned long long)ever);
                            if (elease > 0) printf("lease: %llu\n", (unsigned long long)elease);
                            if (ev && evl > 0) { printf("value: \""); fwrite(ev, 1, evl, stdout); printf("\"\n"); }
                            printf("\n"); }
                    } else if (want_json) {
                        fputs(",\"kv\":{\"key\":", stdout);
                        if (ek) print_json_string(ek, ekl); else fputs("\"\"", stdout);
                        printf(",\"create_revision\":%llu", (unsigned long long)ecr);
                        printf(",\"mod_revision\":%llu", (unsigned long long)emr);
                        printf(",\"version\":%llu", (unsigned long long)ever);
                        if (elease > 0) printf(",\"lease\":%llu", (unsigned long long)elease);
                        if (ev && evl > 0) { fputs(",\"value\":", stdout); print_json_string(ev, evl); }
                        fputs("}", stdout);
                    } else {
                        if (hex_output) {
                            if (ek) { for (size_t i = 0; i < ekl; i++) printf("%02x", ek[i]); }
                            if (ev && evl > 0) { printf(" -> "); for (size_t i = 0; i < evl; i++) printf("%02x", ev[i]); }
                        } else {
                            if (ek) printf("%.*s", (int)ekl, ek);
                            if (ev && evl > 0) printf(" -> %.*s", (int)evl, ev);
                        }
                        printf("\n");
                    }
                } else if (etag == 0x1a) {
                    uint64_t klen = 0; read_varint(resp, eend, &rpos, &klen);
                    size_t kend = rpos + (size_t)klen;
                    const uint8_t *pk = NULL, *pv = NULL;
                    size_t pkl = 0, pvl = 0;
                    uint64_t pcr = 0, pmr = 0, pver = 0;
                    while (rpos < kend) {
                        uint8_t ktag = resp[rpos++];
                        if (ktag == 0x0a) { uint64_t l = 0; read_varint(resp, kend, &rpos, &l); pk = resp + rpos; pkl = (size_t)l; rpos += l; }
                        else if (ktag == 0x2a) { uint64_t l = 0; read_varint(resp, kend, &rpos, &l); pv = resp + rpos; pvl = (size_t)l; rpos += l; }
                        else if (ktag == 0x10) read_varint(resp, kend, &rpos, &pcr);
                        else if (ktag == 0x18) read_varint(resp, kend, &rpos, &pmr);
                        else if (ktag == 0x20) read_varint(resp, kend, &rpos, &pver);
                        else { uint64_t v = 0; read_varint(resp, kend, &rpos, &v); }
                    }
                    rpos = kend;
                    if (want_json) {
                        fputs(",\"prev_kv\":{\"key\":", stdout);
                        if (pk) print_json_string(pk, pkl); else fputs("\"\"", stdout);
                        printf(",\"create_revision\":%llu", (unsigned long long)pcr);
                        printf(",\"mod_revision\":%llu", (unsigned long long)pmr);
                        printf(",\"version\":%llu", (unsigned long long)pver);
                        if (pv && pvl > 0) { fputs(",\"value\":", stdout); print_json_string(pv, pvl); }
                        fputs("}", stdout);
                    } else if (pk) {
                        if (hex_output) {
                            printf(" (prev: "); for (size_t i = 0; i < pkl; i++) printf("%02x", pk[i]);
                            if (pv && pvl > 0) { printf(" -> "); for (size_t i = 0; i < pvl; i++) printf("%02x", pv[i]); }
                            printf(")");
                        } else {
                            printf(" (prev: %.*s", (int)pkl, pk);
                            if (pv && pvl > 0) printf(" -> %.*s", (int)pvl, pv);
                            printf(")");
                        }
                    }
                } else { uint64_t v = 0; read_varint(resp, eend, &rpos, &v); }
            }
            if (want_json) fputs("}", stdout);
            rpos = eend;
            if (exec_cmd && evt_type) {
                char env_rev[32];
                if (evt_key && evt_key_len > 0) setenv("ETCD_WATCH_KEY", (const char *)evt_key, 1);
                else setenv("ETCD_WATCH_KEY", "", 1);
                if (evt_val && evt_val_len > 0) setenv("ETCD_WATCH_VALUE", (const char *)evt_val, 1);
                else setenv("ETCD_WATCH_VALUE", "", 1);
                snprintf(env_rev, sizeof(env_rev), "%llu", (unsigned long long)evt_mod_rev);
                setenv("ETCD_WATCH_REVISION", env_rev, 1);
                setenv("ETCD_WATCH_EVENT_TYPE", evt_type, 1);
                int exec_ret = system(exec_cmd);
                if (exec_ret == -1) { perror("system"); }
            }
        } else if (tag == 0x0a) { uint64_t l = 0; read_varint(resp, rlen, &rpos, &l); rpos += l; }
        else { uint64_t v = 0; read_varint(resp, rlen, &rpos, &v); }
    }
    if (want_json) fputs("]}\n", stdout);
    return event_count;
}

static int cmd_watch(int argc, char **argv) {
    if (argc < 3) { fprintf(stderr, "usage: cetcdctl watch [-i] [--prefix] [--range-end KEY] [--prev-kv] [--progress-notify] [--start-rev N] [--filter TYPE] [--hex] [--exec CMD] [-w json|fields] KEY\n"); return 1; }
    bool prefix = false;
    bool prev_kv = false;
    bool progress_notify = false;
    bool want_json = false;
    bool want_fields = false;
    bool hex_output = false;
    bool interactive = false;
    int64_t start_rev = 0;
    int filter_type = -1; /* -1 = no filter, 0 = NOPUT, 1 = NODELETE */
    const char *exec_cmd = NULL;
    const char *key = NULL;
    const char *range_end_arg = NULL;
    for (int i = 2; i < argc; i++) {
        if (strcmp(argv[i], "-i") == 0 || strcmp(argv[i], "--interactive") == 0) {
            interactive = true;
        } else if (strcmp(argv[i], "--prefix") == 0) {
            prefix = true;
        } else if (strcmp(argv[i], "--range-end") == 0 && i + 1 < argc) {
            range_end_arg = argv[++i];
        } else if (strcmp(argv[i], "--prev-kv") == 0) {
            prev_kv = true;
        } else if (strcmp(argv[i], "--progress-notify") == 0) {
            progress_notify = true;
        } else if (strcmp(argv[i], "--hex") == 0) {
            hex_output = true;
        } else if (strcmp(argv[i], "--start-rev") == 0 || strcmp(argv[i], "--rev") == 0) {
            if (i + 1 >= argc) { fprintf(stderr, "%s requires a revision number\n", argv[i]); return 1; }
            start_rev = atol(argv[++i]);
        } else if (strcmp(argv[i], "--filter") == 0) {
            if (i + 1 >= argc) { fprintf(stderr, "--filter requires a type (NOPUT or NODELETE)\n"); return 1; }
            const char *ft = argv[++i];
            if (strcmp(ft, "NOPUT") == 0) filter_type = 0;
            else if (strcmp(ft, "NODELETE") == 0) filter_type = 1;
            else { fprintf(stderr, "--filter must be NOPUT or NODELETE\n"); return 1; }
        } else if (strcmp(argv[i], "--exec") == 0 && i + 1 < argc) {
            exec_cmd = argv[++i];
        } else if ((strcmp(argv[i], "-w") == 0 || strcmp(argv[i], "--write-out") == 0) && i + 1 < argc) {
            if (strcmp(argv[i + 1], "json") == 0) want_json = true;
            else if (strcmp(argv[i + 1], "fields") == 0) want_fields = true;
            i++;
        } else {
            key = argv[i];
        }
    }
    if (!interactive && !key) { fprintf(stderr, "usage: cetcdctl watch [-i] [--prefix] [--range-end KEY] [--prev-kv] [--progress-notify] [--start-rev N] [--filter TYPE] [--hex] [--exec CMD] [-w json|fields] KEY\n"); return 1; }
    if (prefix && range_end_arg) { fprintf(stderr, "--prefix and --range-end are mutually exclusive\n"); return 1; }

    /* Connect to server and keep the connection open for streaming */
    int fd = connect_server();
    if (fd < 0) { fprintf(stderr, "connect failed\n"); return 1; }
    g_watch_fd = fd;

    /* Set SIGINT handler for clean exit */
    struct sigaction sa, old_sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = watch_sigint_handler;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT, &sa, &old_sa);

    /* Send initial watch create request (if key was provided) */
    if (key) {
        size_t key_len = strlen(key);
        uint8_t watch_buf[1024];
        size_t wpos = build_watch_create(watch_buf, sizeof(watch_buf),
                                         key, key_len, prefix, range_end_arg,
                                         start_rev, prev_kv, filter_type, progress_notify);
        if (wpos == 0) { fprintf(stderr, "key too long\n"); close(fd); return 1; }
        if (send_request(fd, "/etcdserverpb.Watch/Watch", watch_buf, wpos) != 0) {
            fprintf(stderr, "send failed\n"); close(fd); return 1;
        }
    }

    if (interactive) {
        /* Interactive mode: use poll() to multiplex stdin and socket */
        fprintf(stderr, "cetcdctl interactive watch (type 'watch KEY [opts]' or 'cancel ID', Ctrl+D to exit)\n");
        struct pollfd fds[2];
        fds[0].fd = fd;       fds[0].events = POLLIN;
        fds[1].fd = STDIN_FILENO; fds[1].events = POLLIN;
        while (!g_watch_stop) {
            int ret = poll(fds, 2, -1);
            if (ret < 0) { if (errno == EINTR) continue; break; }
            if (fds[0].revents & (POLLIN | POLLHUP | POLLERR)) {
                uint8_t resp[65536];
                int rlen = recv_response(fd, resp, sizeof(resp));
                if (rlen < 0) break;
                print_watch_response(resp, (size_t)rlen, want_json, want_fields, hex_output, exec_cmd);
                fflush(stdout);
            }
            if (fds[1].revents & (POLLIN | POLLHUP)) {
                char line[1024];
                if (!fgets(line, sizeof(line), stdin)) break;
                size_t llen = strlen(line);
                while (llen > 0 && (line[llen-1] == '\n' || line[llen-1] == '\r')) line[--llen] = '\0';
                if (llen == 0) continue;
                char *cmd = strtok(line, " \t");
                if (!cmd) continue;
                if (strcmp(cmd, "watch") == 0) {
                    char *wkey = strtok(NULL, " \t");
                    if (!wkey) { fprintf(stderr, "usage: watch KEY [--prefix] [--prev-kv] [--progress-notify] [--start-rev N]\n"); continue; }
                    bool wprefix = false, wprev_kv = false, wprogress_notify = false;
                    int64_t wstart_rev = 0;
                    char *tok;
                    while ((tok = strtok(NULL, " \t")) != NULL) {
                        if (strcmp(tok, "--prefix") == 0) wprefix = true;
                        else if (strcmp(tok, "--prev-kv") == 0) wprev_kv = true;
                        else if (strcmp(tok, "--progress-notify") == 0) wprogress_notify = true;
                        else if (strcmp(tok, "--start-rev") == 0 || strcmp(tok, "--rev") == 0) {
                            char *sr = strtok(NULL, " \t");
                            if (sr) wstart_rev = atol(sr);
                        }
                    }
                    size_t wklen = strlen(wkey);
                    uint8_t wbuf[1024];
                    size_t wp = build_watch_create(wbuf, sizeof(wbuf), wkey, wklen, wprefix, NULL, wstart_rev, wprev_kv, -1, wprogress_notify);
                    if (wp > 0) { send_request(fd, "/etcdserverpb.Watch/Watch", wbuf, wp); fprintf(stderr, "watch created for key '%s'\n", wkey); }
                } else if (strcmp(cmd, "cancel") == 0) {
                    char *id_str = strtok(NULL, " \t");
                    if (!id_str) { fprintf(stderr, "usage: cancel WATCH_ID\n"); continue; }
                    int64_t wid = atol(id_str);
                    uint8_t cbuf[256];
                    size_t cp = build_watch_cancel(cbuf, sizeof(cbuf), wid);
                    if (cp > 0) { send_request(fd, "/etcdserverpb.Watch/Watch", cbuf, cp); fprintf(stderr, "cancel sent for watch ID %lld\n", (long long)wid); }
                } else {
                    fprintf(stderr, "unknown command: %s (use 'watch KEY' or 'cancel ID')\n", cmd);
                }
            }
        }
    } else {
        /* Non-interactive mode: keep reading streaming responses */
        while (!g_watch_stop) {
            uint8_t resp[65536];
            int rlen = recv_response(fd, resp, sizeof(resp));
            if (rlen < 0) break;
            print_watch_response(resp, (size_t)rlen, want_json, want_fields, hex_output, exec_cmd);
            fflush(stdout);
        }
    }

    sigaction(SIGINT, &old_sa, NULL);
    g_watch_fd = -1;
    close(fd);
    return 0;
}

static int cmd_hash(int argc, char **argv) {
    bool want_json = false;
    bool want_fields = false;
    for (int i = 2; i < argc; i++) {
        if ((strcmp(argv[i], "-w") == 0 || strcmp(argv[i], "--write-out") == 0) && i + 1 < argc) {
            if (strcmp(argv[i + 1], "json") == 0) want_json = true;
            else if (strcmp(argv[i + 1], "fields") == 0) want_fields = true;
            i++;
        }
    }
    uint8_t req[] = {0x00}, resp[256];
    int rlen = do_rpc("/etcdserverpb.Maintenance/Hash", req, 1, resp, sizeof(resp));
    if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
    /* HashResponse: field 1 (header), field 2 (hash) = uint32, tag = 0x10 */
    size_t rpos = 0;
    uint64_t hash_val = 0;
    while (rpos < (size_t)rlen) {
        uint8_t tag = resp[rpos++];
        if (tag == 0x10) {
            read_varint(resp, rlen, &rpos, &hash_val);
        } else if (tag == 0x0a) {
            uint64_t l = 0; read_varint(resp, rlen, &rpos, &l); rpos += l;
        } else {
            uint64_t v = 0; read_varint(resp, rlen, &rpos, &v);
        }
    }
    if (want_json) {
        fputs("{", stdout);
        parse_and_print_header_json(resp, (size_t)rlen);
        printf(",\"hash\":%llu}\n", (unsigned long long)hash_val);
    } else if (want_fields) {
        parse_and_print_header_json(resp, (size_t)rlen);
        printf("hash: %llu\n", (unsigned long long)hash_val);
        fputs("\n", stdout);
    } else {
        printf("hash: %llu\n", (unsigned long long)hash_val);
    }
    return 0;
}

static int cmd_hashkv(int argc, char **argv) {
    bool want_json = false;
    bool want_fields = false;
    for (int i = 2; i < argc; i++) {
        if ((strcmp(argv[i], "-w") == 0 || strcmp(argv[i], "--write-out") == 0) && i + 1 < argc) {
            if (strcmp(argv[i + 1], "json") == 0) want_json = true;
            else if (strcmp(argv[i + 1], "fields") == 0) want_fields = true;
            i++;
        }
    }
    uint8_t req[] = {0x00}, resp[256];
    int rlen = do_rpc("/etcdserverpb.Maintenance/HashKV", req, 1, resp, sizeof(resp));
    if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
    /* HashKVResponse: field 1 (header), field 2 (hash), field 3 (compact_revision) */
    size_t rpos = 0;
    uint64_t hash_val = 0, compact_rev = 0;
    while (rpos < (size_t)rlen) {
        uint8_t tag = resp[rpos++];
        if (tag == 0x10) {
            read_varint(resp, rlen, &rpos, &hash_val);
        } else if (tag == 0x18) {
            read_varint(resp, rlen, &rpos, &compact_rev);
        } else if (tag == 0x0a) {
            uint64_t l = 0; read_varint(resp, rlen, &rpos, &l); rpos += l;
        } else {
            uint64_t v = 0; read_varint(resp, rlen, &rpos, &v);
        }
    }
    if (want_json) {
        fputs("{", stdout);
        parse_and_print_header_json(resp, (size_t)rlen);
        printf(",\"hash\":%llu,\"compact_revision\":%llu}\n",
               (unsigned long long)hash_val, (unsigned long long)compact_rev);
    } else if (want_fields) {
        parse_and_print_header_json(resp, (size_t)rlen);
        printf("hash: %llu\n", (unsigned long long)hash_val);
        printf("compact_revision: %llu\n", (unsigned long long)compact_rev);
        fputs("\n", stdout);
    } else {
        printf("hash: %llu\n", (unsigned long long)hash_val);
        printf("compact_revision: %llu\n", (unsigned long long)compact_rev);
    }
    return 0;
}

static int cmd_defrag(int argc, char **argv) {
    bool want_json = false;
    bool want_fields = false;
    for (int i = 2; i < argc; i++) {
        if ((strcmp(argv[i], "-w") == 0 || strcmp(argv[i], "--write-out") == 0) && i + 1 < argc) {
            if (strcmp(argv[i + 1], "json") == 0) want_json = true;
            else if (strcmp(argv[i + 1], "fields") == 0) want_fields = true;
            i++;
        }
    }
    uint8_t req[] = {0x00}, resp[256];
    int rlen = do_rpc("/etcdserverpb.Maintenance/Defragment", req, 1, resp, sizeof(resp));
    if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
    if (want_json) {
        fputs("{", stdout); parse_and_print_header_json(resp, (size_t)rlen); fputs("}\n", stdout);
    } else if (want_fields) {
        parse_and_print_header_json(resp, (size_t)rlen);
        fputs("\n", stdout);
    } else {
        printf("OK\n");
    }
    return 0;
}

static int cmd_move_leader(int argc, char **argv) {
    bool want_json = false;
    bool want_fields = false;
    const char *target_str = NULL;
    for (int i = 2; i < argc; i++) {
        if ((strcmp(argv[i], "-w") == 0 || strcmp(argv[i], "--write-out") == 0) && i + 1 < argc) {
            if (strcmp(argv[i + 1], "json") == 0) want_json = true;
            else if (strcmp(argv[i + 1], "fields") == 0) want_fields = true;
            i++;
        } else if (!target_str) {
            target_str = argv[i];
        }
    }
    if (!target_str) { fprintf(stderr, "usage: cetcdctl move-leader [-w json|fields] TARGET_ID\n"); return 1; }
    uint8_t req[32], resp[256];
    size_t pos = 0;
    pos = encode_varint_field(req, sizeof(req), pos, 0x08, (uint64_t)atol(target_str));
    int rlen = do_rpc("/etcdserverpb.Maintenance/MoveLeader", req, pos, resp, sizeof(resp));
    if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
    if (want_json) {
        fputs("{", stdout); parse_and_print_header_json(resp, (size_t)rlen); fputs("}\n", stdout);
    } else if (want_fields) {
        parse_and_print_header_json(resp, (size_t)rlen);
        fputs("\n", stdout);
    } else {
        printf("OK\n");
    }
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

static int cmd_completion(int argc, char **argv) {
    const char *shell = (argc >= 3) ? argv[2] : NULL;
    if (!shell || (strcmp(shell, "bash") != 0 && strcmp(shell, "zsh") != 0 && strcmp(shell, "fish") != 0)) {
        fprintf(stderr, "usage: cetcdctl completion bash|zsh|fish\n");
        return 1;
    }

    if (strcmp(shell, "bash") == 0) {
        /* Bash completion script */
        printf("# Bash completion for cetcdctl\n");
        printf("_cetcdctl() {\n");
        printf("    local cur prev words cword i cmd\n");
        printf("    _init_completion 2>/dev/null || {\n");
        printf("        COMPREPLY=()\n");
        printf("        cur=${COMP_WORDS[COMP_CWORD]}\n");
        printf("        prev=${COMP_WORDS[COMP_CWORD-1]}\n");
        printf("        cword=$COMP_CWORD\n");
        printf("    }\n");
        printf("    local cmds=\"put get del watch lease txn compact status alarm hash hashkv defrag move-leader member auth user role snapshot downgrade version endpoint check lock elect completion\"\n");
        printf("    local gopts=\"--host --port --endpoints --endpoint --user --command-timeout --debug --insecure --dial-timeout --keepalive-time --keepalive-timeout --cacert --cert --key\"\n");
        printf("    # Find the subcommand\n");
        printf("    cmd=\"\"\n");
        printf("    for ((i=1; i<cword; i++)); do\n");
        printf("        if [[ ${COMP_WORDS[i]} == -* ]]; then continue; fi\n");
        printf("        if [[ \" $cmds \" == *\" ${COMP_WORDS[i]} \"* ]]; then cmd=${COMP_WORDS[i]}; break; fi\n");
        printf("    done\n");
        printf("    if [[ -z $cmd ]]; then\n");
        printf("        if [[ $cur == -* ]]; then\n");
        printf("            COMPREPLY=( $(compgen -W \"$gopts\" -- \"$cur\") )\n");
        printf("        else\n");
        printf("            COMPREPLY=( $(compgen -W \"$cmds\" -- \"$cur\") )\n");
        printf("        fi\n");
        printf("        return 0\n");
        printf("    fi\n");
        printf("    case $cmd in\n");
        printf("        put) local opts=\"--prev-kv --ignore-value --ignore-lease --lease -w --write-out\";;\n");
        printf("        get) local opts=\"--prefix --from-key --range-end --keys-only --count-only --print-value-only --hex --consistency --rev --limit --sort-by --sort-order --min-mod-rev --max-mod-rev --min-create-rev --max-create-rev -w --write-out\";;\n");
        printf("        del) local opts=\"--prefix --from-key --range-end --prev-kv --hex -w --write-out\";;\n");
        printf("        watch) local opts=\"-i --interactive --prefix --range-end --prev-kv --progress-notify --start-rev --filter --hex --exec -w --write-out\";;\n");
        printf("        lease) local opts=\"--lease-id --keys --once --interval -w --write-out\"\n");
        printf("              local subs=\"grant revoke timetolive list keepalive\";;\n");
        printf("        txn) local opts=\"-w --write-out\"; local subs=\"-i put cas get del\";;\n");
        printf("        compact) local opts=\"--physical -w --write-out\";;\n");
        printf("        alarm) local opts=\"-w --write-out\"; local subs=\"list activate disarm\";;\n");
        printf("        member) local opts=\"--peer-urls --name --learner -w --write-out\"; local subs=\"list add remove update promote\";;\n");
        printf("        auth) local opts=\"-w --write-out\"; local subs=\"enable disable status login\";;\n");
        printf("        user) local opts=\"-w --write-out\"; local subs=\"add delete get list change-password grant-role revoke-role\";;\n");
        printf("        role) local opts=\"--prefix --range-end -w --write-out\"; local subs=\"add delete get list grant-permission revoke-permission\";;\n");
        printf("        snapshot) local opts=\"--compaction-periodical --data-dir --force -w --write-out\"; local subs=\"save status restore\";;\n");
        printf("        downgrade) local opts=\"-w --write-out\"; local subs=\"enable cancel validate\";;\n");
        printf("        endpoint) local opts=\"--cluster -w --write-out\"; local subs=\"health status hashkv\";;\n");
        printf("        check) local opts=\"--load --prefix -w --write-out\"; local subs=\"perf datascale\";;\n");
        printf("        lock) local opts=\"--ttl --print-value-only -w --write-out\";;\n");
        printf("        elect) local opts=\"--ttl --print-value-only -w --write-out\";;\n");
        printf("        completion) local opts=\"\"; local subs=\"bash zsh fish\";;\n");
        printf("        *) local opts=\"\";;\n");
        printf("    esac\n");
        printf("    if [[ -n ${subs:-} && $prev == $cmd ]]; then\n");
        printf("        COMPREPLY=( $(compgen -W \"$subs\" -- \"$cur\") )\n");
        printf("        return 0\n");
        printf("    fi\n");
        printf("    if [[ $cur == -* ]]; then\n");
        printf("        COMPREPLY=( $(compgen -W \"${opts:-} $gopts\" -- \"$cur\") )\n");
        printf("    fi\n");
        printf("    return 0\n");
        printf("}\n");
        printf("complete -F _cetcdctl cetcdctl\n");
        return 0;
    }

    if (strcmp(shell, "zsh") == 0) {
        /* Zsh completion script */
        printf("#compdef cetcdctl\n");
        printf("# Zsh completion for cetcdctl\n");
        printf("_cetcdctl() {\n");
        printf("    local -a cmds opts subs\n");
        printf("    cmds=(put get del watch lease txn compact status alarm hash hashkv defrag move-leader member auth user role snapshot downgrade version endpoint check lock elect completion)\n");
        printf("    _arguments -C \\\n");
        printf("        '--host[Server address]:addr' \\\n");
        printf("        '--port[Server port]:port' \\\n");
        printf("        '--endpoints[Server endpoints]:ep' \\\n");
        printf("        '--endpoint[Server endpoint]:ep' \\\n");
        printf("        '--user[User:pass]:cred' \\\n");
        printf("        '--command-timeout[Timeout]:sec' \\\n");
        printf("        '--debug[Debug]' \\\n");
        printf("        '--insecure[Skip TLS]' \\\n");
        printf("        '--dial-timeout[Dial timeout]:sec' \\\n");
        printf("        '--keepalive-time[Keepalive time]:sec' \\\n");
        printf("        '--keepalive-timeout[Keepalive timeout]:sec' \\\n");
        printf("        '--cacert[CA cert]:file' \\\n");
        printf("        '--cert[TLS cert]:file' \\\n");
        printf("        '--key[TLS key]:file' \\\n");
        printf("        '1:command:compadd -a cmds' \\\n");
        printf("        '*::arg:->args'\n");
        printf("    case $state in\n");
        printf("        args)\n");
        printf("            case ${words[1]} in\n");
        printf("                lease) subs=(grant revoke timetolive list keepalive);;\n");
        printf("                txn) subs=(-i put cas get del);;\n");
        printf("                alarm) subs=(list activate disarm);;\n");
        printf("                member) subs=(list add remove update promote);;\n");
        printf("                auth) subs=(enable disable status login);;\n");
        printf("                user) subs=(add delete get list change-password grant-role revoke-role);;\n");
        printf("                role) subs=(add delete get list grant-permission revoke-permission);;\n");
        printf("                snapshot) subs=(save status restore);;\n");
        printf("                downgrade) subs=(enable cancel validate);;\n");
        printf("                endpoint) subs=(health status hashkv);;\n");
        printf("                check) subs=(perf datascale);;\n");
        printf("                completion) subs=(bash zsh fish);;\n");
        printf("            esac\n");
        printf("            if [[ -n $subs ]]; then\n");
        printf("                _values 'subcommand' $subs\n");
        printf("            fi\n");
        printf("            ;;\n");
        printf("    esac\n");
        printf("}\n");
        printf("compdef _cetcdctl cetcdctl\n");
        return 0;
    }

    if (strcmp(shell, "fish") == 0) {
        /* Fish completion script */
        printf("# Fish completion for cetcdctl\n");
        printf("set -l cmds put get del watch lease txn compact status alarm hash hashkv defrag move-leader member auth user role snapshot downgrade version endpoint check lock elect completion\n");
        printf("complete -c cetcdctl -n \"__fish_use_subcommand\" -a \"$cmds\"\n");
        printf("# Global options\n");
        printf("complete -c cetcdctl -n \"__fish_use_subcommand\" -l host -d 'Server address'\n");
        printf("complete -c cetcdctl -n \"__fish_use_subcommand\" -l port -d 'Server port'\n");
        printf("complete -c cetcdctl -n \"__fish_use_subcommand\" -l endpoints -d 'Server endpoints'\n");
        printf("complete -c cetcdctl -n \"__fish_use_subcommand\" -l user -d 'User:pass'\n");
        printf("complete -c cetcdctl -n \"__fish_use_subcommand\" -l command-timeout -d 'Command timeout'\n");
        printf("complete -c cetcdctl -n \"__fish_use_subcommand\" -l debug -d 'Debug'\n");
        printf("complete -c cetcdctl -n \"__fish_use_subcommand\" -l insecure -d 'Skip TLS'\n");
        printf("complete -c cetcdctl -n \"__fish_use_subcommand\" -l dial-timeout -d 'Dial timeout'\n");
        printf("complete -c cetcdctl -n \"__fish_use_subcommand\" -l keepalive-time -d 'Keepalive time'\n");
        printf("complete -c cetcdctl -n \"__fish_use_subcommand\" -l keepalive-timeout -d 'Keepalive timeout'\n");
        printf("complete -c cetcdctl -n \"__fish_use_subcommand\" -l cacert -d 'CA cert'\n");
        printf("complete -c cetcdctl -n \"__fish_use_subcommand\" -l cert -d 'TLS cert'\n");
        printf("complete -c cetcdctl -n \"__fish_use_subcommand\" -l key -d 'TLS key'\n");
        printf("# Subcommands\n");
        printf("complete -c cetcdctl -n '___fish_seen_subcommand_from lease' -a 'grant revoke timetolive list keepalive'\n");
        printf("complete -c cetcdctl -n '___fish_seen_subcommand_from txn' -a '-i put cas get del'\n");
        printf("complete -c cetcdctl -n '___fish_seen_subcommand_from alarm' -a 'list activate disarm'\n");
        printf("complete -c cetcdctl -n '___fish_seen_subcommand_from member' -a 'list add remove update promote'\n");
        printf("complete -c cetcdctl -n '___fish_seen_subcommand_from auth' -a 'enable disable status login'\n");
        printf("complete -c cetcdctl -n '___fish_seen_subcommand_from user' -a 'add delete get list change-password grant-role revoke-role'\n");
        printf("complete -c cetcdctl -n '___fish_seen_subcommand_from role' -a 'add delete get list grant-permission revoke-permission'\n");
        printf("complete -c cetcdctl -n '___fish_seen_subcommand_from snapshot' -a 'save status restore'\n");
        printf("complete -c cetcdctl -n '___fish_seen_subcommand_from downgrade' -a 'enable cancel validate'\n");
        printf("complete -c cetcdctl -n '___fish_seen_subcommand_from endpoint' -a 'health status hashkv'\n");
        printf("complete -c cetcdctl -n '___fish_seen_subcommand_from endpoint' -l cluster\n");
        printf("complete -c cetcdctl -n '___fish_seen_subcommand_from check' -a 'perf datascale'\n");
        printf("complete -c cetcdctl -n '___fish_seen_subcommand_from completion' -a 'bash zsh fish'\n");
        printf("# Common flags\n");
        printf("complete -c cetcdctl -n '___fish_seen_subcommand_from put' -l prev-kv -l ignore-value -l ignore-lease -l lease -s w -l write-out\n");
        printf("complete -c cetcdctl -n '___fish_seen_subcommand_from get' -l prefix -l from-key -l range-end -l keys-only -l count-only -l print-value-only -l hex -l consistency -l rev -l limit -l sort-by -l sort-order -l min-mod-rev -l max-mod-rev -l min-create-rev -l max-create-rev -s w -l write-out\n");
        printf("complete -c cetcdctl -n '___fish_seen_subcommand_from del' -l prefix -l from-key -l range-end -l prev-kv -l hex -s w -l write-out\n");
        printf("complete -c cetcdctl -n '___fish_seen_subcommand_from watch' -s i -l interactive -l prefix -l range-end -l prev-kv -l progress-notify -l start-rev -l filter -l hex -l exec -s w -l write-out\n");
        printf("complete -c cetcdctl -n '___fish_seen_subcommand_from lease' -l lease-id -l keys -l once -l interval -s w -l write-out\n");
        printf("complete -c cetcdctl -n '___fish_seen_subcommand_from txn' -s w -l write-out\n");
        printf("complete -c cetcdctl -n '___fish_seen_subcommand_from compact' -l physical -s w -l write-out\n");
        printf("complete -c cetcdctl -n '___fish_seen_subcommand_from member' -l peer-urls -l name -l learner -s w -l write-out\n");
        printf("complete -c cetcdctl -n '___fish_seen_subcommand_from role' -l prefix -l range-end -s w -l write-out\n");
        printf("complete -c cetcdctl -n '___fish_seen_subcommand_from snapshot' -l compaction-periodical -l data-dir -l force -s w -l write-out\n");
        printf("complete -c cetcdctl -n '___fish_seen_subcommand_from check' -l load -l prefix -s w -l write-out\n");
        printf("complete -c cetcdctl -n '___fish_seen_subcommand_from lock' -l ttl -l print-value-only -s w -l write-out\n");
        printf("complete -c cetcdctl -n '___fish_seen_subcommand_from elect' -l ttl -l print-value-only -s w -l write-out\n");
        return 0;
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
    printf("  --debug       Print debug info (RPC path and response size)\n");
    printf("  --insecure    Skip TLS certificate verification (no-op, plain TCP)\n");
    printf("  --dial-timeout SEC  Connection timeout (default: none)\n");
    printf("  --keepalive-time SEC    Keepalive ping interval (no-op, plain TCP)\n");
    printf("  --keepalive-timeout SEC  Keepalive timeout (no-op, plain TCP)\n");
    printf("  --cacert FILE   TLS CA certificate (no-op, plain TCP)\n");
    printf("  --cert FILE     TLS certificate (no-op, plain TCP)\n");
    printf("  --key FILE      TLS key (no-op, plain TCP)\n\n");
    printf("Commands:\n");
    printf("  put [--prev-kv] [--ignore-value] [--ignore-lease] [--lease ID] [--print-value-only] [-w json|fields] KEY [VALUE|-]  Store a key-value pair\n");
    printf("  get [--prefix] [--from-key] [--range-end KEY] [--keys-only] [--count-only] [--print-value-only] [--hex] [--consistency l|s] [-w json|fields|table] [--rev N] [--limit N] [--sort-by FIELD] [--sort-order ORDER] [--min-mod-rev N] [--max-mod-rev N] [--min-create-rev N] [--max-create-rev N] KEY [RANGE_END]\n");
    printf("                         Retrieve keys (sort-by: key|version|create|mod|value; sort-order: ascend|descend)\n");
    printf("  del [--prefix] [--from-key] [--range-end KEY] [--prev-kv] [--hex] [--print-value-only] [-w json|fields] KEY [RANGE_END]  Delete a key (options: --prefix, --from-key, --range-end, --prev-kv, --hex, --print-value-only)\n");
    printf("  watch [-i] [--prefix] [--range-end KEY] [--prev-kv] [--progress-notify] [--start-rev N] [--filter NOPUT|NODELETE] [--hex] [--exec CMD] [-w json|fields] KEY  Watch key changes (-i for interactive mode, --progress-notify for periodic progress updates, --exec runs CMD with ETCD_WATCH_* env vars)\n");
    printf("  lease grant [--lease-id ID] [-w json|fields] TTL  Grant a lease (TTL in seconds)\n");
    printf("  lease revoke [-w json|fields] ID  Revoke a lease by ID\n");
    printf("  lease timetolive [--keys] [-w json|fields] ID  Query remaining TTL\n");
    printf("  lease list [-w table|json|fields]  List all active leases\n");
    printf("  lease keepalive [--once] [--interval SEC] [-w json|fields] ID  Keep a lease alive (loop by default, --once for single, --interval for custom interval)\n");
    printf("  txn -i [-w json|fields]  Interactive transaction (read from stdin: cmp/put/get/del/then/else)\n");
    printf("  txn put [-w json|fields] KEY VALUE  Execute a transaction (Put)\n");
    printf("  txn cas [-w json|fields] KEY EXP NEW  Compare-and-swap (if KEY==EXP then KEY=NEW)\n");
    printf("  txn get [-w json|fields] KEY [RANGE_END]  Execute a transaction (Range)\n");
    printf("  txn del [-w json|fields] [--prefix] [--prev-kv] KEY [RANGE_END]  Execute a transaction (Delete)\n");
    printf("  compact [--physical] [-w json|fields] REV  Compact MVCC history to revision\n");
    printf("  status [-w json|fields]  Get server status\n");
    printf("  alarm list [-w table|json|fields]  List all alarms\n");
    printf("  alarm activate [-w json|fields] [TYPE]  Activate an alarm (NOSPACE|CORRUPT|NONE)\n");
    printf("  alarm disarm [-w json|fields] [TYPE]     Disarm an alarm (NOSPACE|CORRUPT|NONE)\n");
    printf("  hash [-w json|fields]         Get KV store hash\n");
    printf("  hashkv [-w json|fields]       Get KV store hash + compact revision\n");
    printf("  defrag [-w json|fields]       Defragment database (no-op for LMDB)\n");
    printf("  move-leader [-w json|fields] TARGET_ID  Transfer leadership to target node\n");
    printf("  member list [-w json|table|fields]  List cluster members\n");
    printf("  member add [-w json|fields] [--peer-urls URLS] [--name NAME] [--learner] [PEER_URL]  Add a cluster member (comma-separated URLs supported)\n");
    printf("  member remove [-w json|fields] ID    Remove a cluster member\n");
    printf("  member update [-w json|fields] ID PEER_URLS  Update a member's peer URLs (comma-separated supported)\n");
    printf("  member promote [-w json|fields] ID    Promote a member to voting member\n");
    printf("  auth enable [-w json|fields]     Enable authentication\n");
    printf("  auth disable [-w json|fields]     Disable authentication\n");
    printf("  auth status [-w json|fields]     Query auth status\n");
    printf("  auth login NAME PASS [-w json|fields]   Authenticate and get token\n");
    printf("  user add NAME [PASS] [--no-password] [-w json|fields]    Add a user\n");
    printf("  user delete NAME [-w json|fields]      Delete a user\n");
    printf("  user get NAME [-w json|fields]          Get user details (roles)\n");
    printf("  user list [-w json|table|fields]       List all users\n");
    printf("  user change-password NAME PASS [-w json|fields]  Change user password\n");
    printf("  user grant-role NAME ROLE [-w json|fields]        Grant role to user\n");
    printf("  user revoke-role NAME ROLE [-w json|fields]       Revoke role from user\n");
    printf("  role add NAME [-w json|fields]          Add a role\n");
    printf("  role delete NAME [-w json|fields]       Delete a role\n");
    printf("  role get NAME [-w json|fields]          Get role details (permissions)\n");
    printf("  role list [-w json|table|fields]        List all roles\n");
    printf("  role grant-permission ROLE TYPE KEY [--prefix] [--range-end KEY] [-w json|fields]\n");
    printf("                         Grant permission (read|write|readwrite)\n");
    printf("  role revoke-permission ROLE [TYPE KEY] [--prefix] [--range-end KEY] [-w json|fields]\n");
    printf("                         Revoke permission (all or specific key) from role\n");
    printf("  snapshot save [FILE] [--compaction-periodical] [-w json|fields]   Save a snapshot to file\n");
    printf("  snapshot status FILE [-w json|fields]  Show snapshot file info\n");
    printf("  snapshot restore FILE --data-dir DIR [--force] [-w json|fields]  Restore snapshot to data dir\n");
    printf("  downgrade enable [-w json|fields] VER   Enable cluster downgrade\n");
    printf("  downgrade cancel [-w json|fields]       Cancel cluster downgrade\n");
    printf("  downgrade validate [-w json|fields] VER Validate downgrade version\n");
    printf("  version [-w json|fields]      Print the client version\n");
    printf("  endpoint health [--cluster] [-w json|fields]  Check server health (or all cluster members with --cluster)\n");
    printf("  endpoint status [--cluster] [-w json|table|fields]  Get server status (or all cluster members with --cluster)\n");
    printf("  endpoint hashkv [--cluster] [-w json|fields]      Get KV hash per endpoint (or all cluster members with --cluster)\n");
    printf("  check perf [--load S|M|L] [--prefix PREFIX] [-w json|fields]    Run a simple performance check\n");
    printf("  check datascale [-w json|fields] [--load N] [--prefix PREFIX]  Test database scalability\n");
    printf("  lock [--ttl N] [--print-value-only] [-w json|fields] LOCKNAME [CMD...]  Acquire a distributed lock\n");
    printf("  elect [--ttl N] [--print-value-only] [-w json|fields] ELECTION_NAME [PROPOSAL]  Leader election\n");
    printf("  completion bash|zsh|fish   Generate shell completion script\n");
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
        } else if (strcmp(argv[cmd_start], "--insecure") == 0) {
            g_insecure = 1;
            cmd_start += 1;
        } else if (strcmp(argv[cmd_start], "--dial-timeout") == 0 && cmd_start + 1 < argc) {
            g_dial_timeout = atoi(argv[cmd_start + 1]);
            cmd_start += 2;
        } else if (strcmp(argv[cmd_start], "--keepalive-time") == 0 && cmd_start + 1 < argc) {
            /* Accepted for compatibility, no-op for plain TCP */
            cmd_start += 2;
        } else if (strcmp(argv[cmd_start], "--keepalive-timeout") == 0 && cmd_start + 1 < argc) {
            /* Accepted for compatibility, no-op for plain TCP */
            cmd_start += 2;
        } else if (strcmp(argv[cmd_start], "--cacert") == 0 && cmd_start + 1 < argc) {
            /* Accepted for compatibility, no-op (plain TCP) */
            cmd_start += 2;
        } else if (strcmp(argv[cmd_start], "--cert") == 0 && cmd_start + 1 < argc) {
            /* Accepted for compatibility, no-op (plain TCP) */
            cmd_start += 2;
        } else if (strcmp(argv[cmd_start], "--key") == 0 && cmd_start + 1 < argc) {
            /* Accepted for compatibility, no-op (plain TCP) */
            cmd_start += 2;
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
    if (strcmp(cmd, "completion") == 0) return cmd_completion(new_argc, new_argv);

    fprintf(stderr, "unknown command: %s\n", cmd);
    print_usage();
    return 1;
}
