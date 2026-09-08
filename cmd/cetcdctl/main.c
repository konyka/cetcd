/*
 * cetcdctl — command-line client for cetcd
 *
 * Connects to a cetcd server using the custom gRPC-like framing protocol:
 *   Request:  2B path_len (BE) + path + 1B flags + [2B token_len + token if flags&0x02]
 *             + 4B payload_len (BE) + payload
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
 *   defrag                 — defragment the database (LMDB compact-copy; --data-dir offline)
 *   move-leader TARGET_ID  — transfer leadership to target node
 *   member list            — list cluster members
 *   member add PEER_URL    — add a cluster member
 *   member remove ID       — remove a cluster member
 *   member update ID URL   — update a member's peer URL
 *   snapshot save [FILE]   — save a snapshot to file
 *   snapshot status FILE   — show snapshot file info
 *   snapshot restore FILE --data-dir DIR — restore snapshot (--wal-dir / --bump-revision leftover-safe)
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
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>

#include "cetcd/base.h"
#include "cetcd/auth.h"
#include "cetcd/tls.h"
#include "cetcd/peer.h"
#include "cetcd/snap.h"
#include "cetcd/server.h"
#include "cetcd/backend.h"

static const char *g_host = "127.0.0.1";
static uint16_t    g_port = 2379;

/* etcdctl JoinHostPort: IPv6 is [host]:port so ::1:2379 cannot look like port 1. */
static const char *ep_str_(void) {
    static char buf[288];
    if (cetcd_format_host_port(g_host, g_port, buf, sizeof(buf)) != CETCD_OK)
        return g_host ? g_host : "";
    return buf;
}
static cetcd_endpoint g_eps[CETCD_DISCOVERY_MAX_ENDPOINTS];
static size_t      g_n_eps;
static int         g_endpoints_set;
static const char *g_discovery_srv;
static const char *g_discovery_srv_name;
static int         g_keys_only = 0; /* flag for get --keys-only */
static int         g_count_only = 0; /* flag for get --count-only */
static int         g_print_value_only = 0; /* flag for get --print-value-only */
static int         g_hex = 0; /* flag for get --hex */
static int         g_write_json = 0; /* flag for -w json */
static int         g_write_fields = 0; /* flag for -w fields */
static int         g_write_table = 0; /* flag for -w table */
static int         g_debug = 0; /* flag for --debug */
static int         g_insecure = 0; /* skip TLS verify when TLS is on */
static int         g_insecure_transport = 0; /* force plaintext */
static int         g_endpoint_https = 0; /* --endpoints used https:// */
static int         g_dial_timeout = 0; /* flag for --dial-timeout (seconds) */
static int         g_tcp_keepalive_time = -1; /* -1 unset; 0 disable; else TCP_KEEPIDLE */
static int         g_tcp_keepalive_timeout = -1; /* -1 unset; TCP_KEEPINTVL when time > 0 */
static char        g_auth_token[CETCD_AUTH_MAX_TOKEN_LEN + 1] = ""; /* token from --user */
static char        g_password[256] = ""; /* password from --password flag */
static char        g_cacert[512] = "";
static char        g_cert[512] = "";
static char        g_key[512] = "";
static cetcd_tls_ctx  *g_tls_ctx = NULL;
static cetcd_tls_conn *g_tls = NULL;
static int             g_tls_fd = -1;
static uint64_t        g_max_call_send = 0; /* 0 = unlimited */
static uint64_t        g_max_call_recv = 0;

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

static int tls_wanted_(void) {
    return !g_insecure_transport &&
           (g_cacert[0] || (g_cert[0] && g_key[0]) || g_endpoint_https);
}

static void conn_close(int fd) {
    if (g_tls && g_tls_fd == fd) {
        cetcd_tls_shutdown(g_tls);
        cetcd_tls_conn_free(g_tls);
        g_tls = NULL;
        g_tls_fd = -1;
    }
    if (fd >= 0) close(fd);
}

static ssize_t conn_send_all(int fd, const void *buf, size_t len) {
    const uint8_t *p = (const uint8_t *)buf;
    size_t off = 0;
    while (off < len) {
        ssize_t n;
        if (g_tls && g_tls_fd == fd) {
            int w = cetcd_tls_write(g_tls, p + off, len - off);
            n = (w > 0) ? (ssize_t)w : -1;
        } else {
            n = send(fd, p + off, len - off, 0);
        }
        if (n <= 0) return -1;
        off += (size_t)n;
    }
    return (ssize_t)len;
}

static ssize_t conn_recv_all(int fd, void *buf, size_t len) {
    uint8_t *p = (uint8_t *)buf;
    size_t off = 0;
    while (off < len) {
        ssize_t n;
        if (g_tls && g_tls_fd == fd) {
            int r = cetcd_tls_read(g_tls, p + off, len - off);
            n = (r > 0) ? (ssize_t)r : -1;
        } else {
            n = recv(fd, p + off, len - off, MSG_WAITALL);
        }
        if (n <= 0) return -1;
        off += (size_t)n;
    }
    return (ssize_t)len;
}

static int tls_ctx_ensure_(void) {
    if (g_tls_ctx) return 0;
    if ((g_cert[0] && !g_key[0]) || (!g_cert[0] && g_key[0])) {
        fprintf(stderr, "tls: --cert and --key must be set together\n");
        return -1;
    }
    g_tls_ctx = cetcd_tls_ctx_new_client();
    if (!g_tls_ctx) {
        fprintf(stderr, "tls: client context unavailable\n");
        return -1;
    }
    if (g_cacert[0]) {
        if (cetcd_tls_set_ca(g_tls_ctx, g_cacert) != CETCD_OK) {
            fprintf(stderr, "tls: failed to load --cacert %s\n", g_cacert);
            cetcd_tls_ctx_free(g_tls_ctx);
            g_tls_ctx = NULL;
            return -1;
        }
        if (!g_insecure) {
            if (cetcd_tls_set_verify_peer(g_tls_ctx, 0) != CETCD_OK) {
                cetcd_tls_ctx_free(g_tls_ctx);
                g_tls_ctx = NULL;
                return -1;
            }
        }
    } else if (!g_insecure) {
        fprintf(stderr, "tls: --cert/--key require --cacert or --insecure\n");
        cetcd_tls_ctx_free(g_tls_ctx);
        g_tls_ctx = NULL;
        return -1;
    }
    if (g_cert[0] && cetcd_tls_set_cert(g_tls_ctx, g_cert, g_key) != CETCD_OK) {
        fprintf(stderr, "tls: failed to load --cert/--key\n");
        cetcd_tls_ctx_free(g_tls_ctx);
        g_tls_ctx = NULL;
        return -1;
    }
    return 0;
}

static int apply_socket_opts_(int fd) {
    if (g_dial_timeout > 0) {
        struct timeval tv;
        tv.tv_sec = g_dial_timeout;
        tv.tv_usec = 0;
        setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    }
    if (g_tcp_keepalive_time >= 0) {
        int on = g_tcp_keepalive_time > 0;
        if (setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &on, sizeof(on)) != 0) {
            perror("SO_KEEPALIVE");
            return -1;
        }
        if (on) {
#ifdef TCP_KEEPIDLE
            if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPIDLE, &g_tcp_keepalive_time,
                           sizeof(g_tcp_keepalive_time)) != 0) {
                perror("TCP_KEEPIDLE");
                return -1;
            }
#endif
#ifdef TCP_KEEPINTVL
            if (g_tcp_keepalive_timeout > 0 &&
                setsockopt(fd, IPPROTO_TCP, TCP_KEEPINTVL, &g_tcp_keepalive_timeout,
                           sizeof(g_tcp_keepalive_timeout)) != 0) {
                perror("TCP_KEEPINTVL");
                return -1;
            }
#endif
        }
    }
    return 0;
}

static int connect_one_(const cetcd_endpoint *ep) {
    struct addrinfo hints, *res = NULL, *rp;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    char portbuf[8];
    snprintf(portbuf, sizeof(portbuf), "%u", (unsigned)ep->port);
    if (getaddrinfo(ep->host, portbuf, &hints, &res) != 0 || !res) {
        perror("getaddrinfo");
        return -1;
    }
    int fd = -1;
    for (rp = res; rp; rp = rp->ai_next) {
        fd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (fd < 0) continue;
        if (apply_socket_opts_(fd) != 0) {
            close(fd);
            fd = -1;
            continue;
        }
        if (connect(fd, rp->ai_addr, rp->ai_addrlen) == 0) break;
        close(fd);
        fd = -1;
    }
    freeaddrinfo(res);
    if (fd < 0) return -1;
    if (tls_wanted_()) {
        if (tls_ctx_ensure_() != 0) {
            close(fd);
            return -1;
        }
        g_tls = cetcd_tls_connect(g_tls_ctx, fd);
        if (!g_tls) {
            fprintf(stderr, "tls: handshake failed\n");
            close(fd);
            return -1;
        }
        g_tls_fd = fd;
    }
    return fd;
}

static int ensure_endpoints_(void) {
    if (g_n_eps > 0) return 0;
    if (g_discovery_srv) {
        int ssl = (g_cacert[0] || g_cert[0] || g_endpoint_https) && !g_insecure_transport;
        cetcd_discovery_kind first = ssl ? CETCD_DISCOVERY_CLIENT_SSL
                                         : CETCD_DISCOVERY_CLIENT;
        cetcd_discovery_kind second = ssl ? CETCD_DISCOVERY_CLIENT
                                          : CETCD_DISCOVERY_CLIENT_SSL;
        size_t n = 0;
        int rc = cetcd_discovery_resolve(first, g_discovery_srv_name, g_discovery_srv,
                                         ssl ? 1 : 0, g_eps,
                                         CETCD_DISCOVERY_MAX_ENDPOINTS, &n);
        if (rc != 0)
            rc = cetcd_discovery_resolve(second, g_discovery_srv_name, g_discovery_srv,
                                         ssl ? 1 : 0, g_eps,
                                         CETCD_DISCOVERY_MAX_ENDPOINTS, &n);
        if (rc != 0 || n == 0) {
            fprintf(stderr, "--discovery-srv lookup failed\n");
            return -1;
        }
        g_n_eps = n;
        if (ssl) g_endpoint_https = 1;
        return 0;
    }
    memset(&g_eps[0], 0, sizeof(g_eps[0]));
    strncpy(g_eps[0].host, g_host, sizeof(g_eps[0].host) - 1);
    g_eps[0].port = g_port;
    g_eps[0].https = g_endpoint_https;
    g_n_eps = 1;
    return 0;
}

static int connect_server(void) {
    if (ensure_endpoints_() != 0) return -1;
    int any_https = g_endpoint_https;
    for (size_t i = 0; i < g_n_eps; i++) {
        if (g_eps[i].https) any_https = 1;
        g_endpoint_https = g_eps[i].https || any_https;
        g_host = g_eps[i].host;
        g_port = g_eps[i].port;
        int fd = connect_one_(&g_eps[i]);
        if (fd >= 0) return fd;
    }
    fprintf(stderr, "failed to connect to any endpoint\n");
    return -1;
}

static int send_request(int fd, const char *path,
                         const uint8_t *payload, size_t payload_len) {
    size_t path_len = strlen(path);
    uint8_t header[CETCD_AUTH_MAX_TOKEN_LEN + 512];
    size_t hpos = 0;
    header[hpos++] = (uint8_t)(path_len >> 8);
    header[hpos++] = (uint8_t)(path_len & 0xFF);
    memcpy(header + hpos, path, path_len);
    hpos += path_len;
    size_t token_len = g_auth_token[0] ? strlen(g_auth_token) : 0;
    if (hpos + token_len + 8 > sizeof(header)) return -1;
    uint8_t flags = 0;
    if (token_len > 0 && token_len <= CETCD_AUTH_MAX_TOKEN_LEN)
        flags |= 0x02;
    header[hpos++] = flags;
    if (flags & 0x02) {
        header[hpos++] = (uint8_t)((token_len >> 8) & 0xFF);
        header[hpos++] = (uint8_t)(token_len & 0xFF);
        memcpy(header + hpos, g_auth_token, token_len);
        hpos += token_len;
    }
    header[hpos++] = (uint8_t)((payload_len >> 24) & 0xFF);
    header[hpos++] = (uint8_t)((payload_len >> 16) & 0xFF);
    header[hpos++] = (uint8_t)((payload_len >> 8) & 0xFF);
    header[hpos++] = (uint8_t)(payload_len & 0xFF);

    if (g_max_call_send && (uint64_t)payload_len > g_max_call_send) return -1;

    if (conn_send_all(fd, header, hpos) != (ssize_t)hpos) return -1;
    if (payload_len > 0) {
        if (conn_send_all(fd, payload, payload_len) != (ssize_t)payload_len) return -1;
    }
    return 0;
}

static int recv_response(int fd, uint8_t *buf, size_t buf_cap) {
    /* Read header: 2B path_len + path + 1B compressed + 4B payload_len */
    uint8_t hdr[512];
    ssize_t n = conn_recv_all(fd, hdr, 2);
    if (n != 2) return -1;
    uint16_t path_len = ((uint16_t)hdr[0] << 8) | hdr[1];
    if (path_len > 256) return -1;

    n = conn_recv_all(fd, hdr, (size_t)path_len + 5);
    if (n != (ssize_t)path_len + 5) return -1;

    uint32_t payload_len = ((uint32_t)hdr[path_len + 1] << 24) |
                           ((uint32_t)hdr[path_len + 2] << 16) |
                           ((uint32_t)hdr[path_len + 3] << 8)  |
                           ((uint32_t)hdr[path_len + 4]);
    if (g_max_call_recv && (uint64_t)payload_len > g_max_call_recv) return -1;
    if (payload_len >= buf_cap) return -1;

    if (payload_len > 0) {
        n = conn_recv_all(fd, buf, payload_len);
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
        conn_close(fd);
        return -1;
    }
    int rlen = recv_response(fd, resp, resp_cap);
    conn_close(fd);
    if (g_debug) {
        fprintf(stderr, "[debug] RPC %s resp_len=%d\n", path, rlen);
    }
    /* Zero-length payload is the server's domain-error signal (e.g. ErrEmptyKey). */
    if (rlen <= 0) return -1;
    return rlen;
}

/* --- Response parsing helpers --- */

/* Parse ResponseHeader from a protobuf message and output as JSON "header" field.
 * Scans the message for tag 0x0a (field 1 = header, length-delimited).
 * Returns 1 if header found, 0 if not. */
static int parse_and_print_header_json(const uint8_t *data, size_t len) {
    uint64_t cluster_id = 0, member_id = 0, raft_term = 0;
    int64_t revision = 0;
    if (!data || len == 0) {
        fputs("\"header\":{}", stdout);
        return 0;
    }
    /* leftover-safe: leftover cannot steal a printed revision */
    if (cetcd_parse_response_header(data, len, &cluster_id, &member_id,
                                    &revision, &raft_term) != CETCD_OK) {
        fputs("\"header\":{}", stdout);
        return 0;
    }
    printf("\"header\":{\"cluster_id\":%llu,\"member_id\":%llu,\"revision\":%lld,\"raft_term\":%llu}",
           (unsigned long long)cluster_id, (unsigned long long)member_id,
           (long long)revision, (unsigned long long)raft_term);
    return 1;
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
    int64_t leftover_count = 0;
    int leftover_more = 0;
    int64_t leftover_lease = 0;
    /* leftover-safe: leftover cannot steal a printed lease */
    if (cetcd_parse_range_response_kv_lease(data, len, &leftover_lease)
        != CETCD_OK)
        return;
    (void)leftover_lease;
    int64_t leftover_version = 0;
    /* leftover-safe: leftover cannot steal a printed version */
    if (cetcd_parse_range_response_kv_version(data, len, &leftover_version)
        != CETCD_OK)
        return;
    (void)leftover_version;
    int64_t leftover_create = 0;
    /* leftover-safe: leftover cannot steal a printed create_revision */
    if (cetcd_parse_range_response_kv_create_rev(data, len, &leftover_create)
        != CETCD_OK)
        return;
    (void)leftover_create;
    int64_t leftover_mod = 0;
    /* leftover-safe: leftover cannot steal a printed mod_revision */
    if (cetcd_parse_range_response_kv_mod_rev(data, len, &leftover_mod)
        != CETCD_OK)
        return;
    (void)leftover_mod;
    int count_rc = cetcd_parse_range_response_count(data, len, &leftover_count,
                                                    &leftover_more);
    /* leftover-safe: leftover cannot steal a printed count or more=true */
    if (count_rc != CETCD_OK) {
        if (g_count_only) {
            fprintf(stderr, "request failed\n");
            return;
        }
    } else {
        server_count = (int)leftover_count;
        has_more = leftover_more;
    }
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
                } else if (ktag == 0x00) {
                    continue;
                } else if (cetcd_leftover_safe_skip_field(data, kv_end, &pos,
                                                          ktag) != CETCD_OK) {
                    break;
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
        } else if (tag == 0x20 || tag == 0x18) {
            /* leftover-safe-skip count/more; values already leftover-safe-parsed */
            if (cetcd_leftover_safe_skip_field(data, len, &pos, tag)
                != CETCD_OK)
                break;
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
                } else if (htag == 0x00) {
                    continue;
                } else if (cetcd_leftover_safe_skip_field(data, hdr_end, &pos,
                                                          htag) != CETCD_OK) {
                    break;
                }
            }
            pos = hdr_end;
        } else if (tag == 0x00) {
            continue;
        } else if (cetcd_leftover_safe_skip_field(data, len, &pos, tag)
                   != CETCD_OK) {
            break;
        }
    }
    if (g_write_table && !g_count_only) {
        printf("+------------------+------------+------------+---------+------------------+\n");
    }
    if (g_count_only) {
        if (g_write_json) {
            /* etcdctl format: {"header":{...},"count":N} — no kvs or more */
            if (have_header) {
                printf("{\"header\":{\"cluster_id\":%llu,\"member_id\":%llu,\"revision\":%llu,\"raft_term\":%llu},",
                       (unsigned long long)hdr_cluster_id, (unsigned long long)hdr_member_id,
                       (unsigned long long)hdr_revision, (unsigned long long)hdr_raft_term);
            } else {
                fputs("{\"header\":{},", stdout);
            }
            printf("\"count\":%d}\n", server_count >= 0 ? server_count : count);
        } else if (g_write_fields) {
            if (have_header) {
                printf("\"cluster_id\" : %llu\n", (unsigned long long)hdr_cluster_id);
                printf("\"member_id\" : %llu\n", (unsigned long long)hdr_member_id);
                printf("\"revision\" : %llu\n", (unsigned long long)hdr_revision);
                printf("\"raft_term\" : %llu\n", (unsigned long long)hdr_raft_term);
            }
            printf("\"count\" : %d\n", server_count >= 0 ? server_count : count);
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
    char leftover_err[32];
    uint64_t leftover_inuse = 0;
    leftover_err[0] = '\0';
    /* leftover-safe: leftover cannot steal a printed alarm error */
    if (cetcd_parse_status_errors(data, len, leftover_err, sizeof(leftover_err))
        != CETCD_OK)
        return;
    /* leftover-safe: leftover cannot steal a printed dbSizeInUse */
    if (cetcd_parse_status_db_size_in_use(data, len, &leftover_inuse)
        != CETCD_OK)
        return;
    uint64_t leftover_leader = 0;
    /* leftover-safe: leftover cannot steal a printed leader */
    if (cetcd_parse_status_leader(data, len, &leftover_leader) != CETCD_OK)
        return;
    uint64_t leftover_ridx = 0;
    /* leftover-safe: leftover cannot steal a printed raftIndex */
    if (cetcd_parse_status_raft_index(data, len, &leftover_ridx) != CETCD_OK)
        return;
    uint64_t leftover_rterm = 0;
    /* leftover-safe: leftover cannot steal a printed raftTerm */
    if (cetcd_parse_status_raft_term(data, len, &leftover_rterm) != CETCD_OK)
        return;
    uint64_t leftover_rapplied = 0;
    /* leftover-safe: leftover cannot steal a printed raftAppliedIndex */
    if (cetcd_parse_status_raft_applied(data, len, &leftover_rapplied)
        != CETCD_OK)
        return;
    uint64_t leftover_dbsize = 0;
    /* leftover-safe: leftover cannot steal a printed dbSize */
    if (cetcd_parse_status_db_size(data, len, &leftover_dbsize) != CETCD_OK)
        return;
    char leftover_ver[32];
    leftover_ver[0] = '\0';
    /* leftover-safe: leftover cannot steal a printed version */
    if (cetcd_parse_status_version(data, len, leftover_ver, sizeof(leftover_ver))
        != CETCD_OK)
        return;
    int leftover_learner = 0;
    /* leftover-safe: leftover cannot steal a printed isLearner */
    if (cetcd_parse_status_is_learner(data, len, &leftover_learner) != CETCD_OK)
        return;
    size_t pos = 0;
    while (pos < len) {
        uint8_t tag = data[pos++];
        if (tag == 0x12) {
            /* leftover-safe-skip field 2; leftover-safe version is printed */
            if (cetcd_leftover_safe_skip_field(data, len, &pos, tag)
                != CETCD_OK) {
                break;
            }
            if (leftover_ver[0])
                printf("version: %s\n", leftover_ver);
        } else if (tag == 0x18) {
            /* leftover-safe-skip field 3; leftover-safe dbSize is printed */
            if (cetcd_leftover_safe_skip_field(data, len, &pos, tag)
                != CETCD_OK) {
                break;
            }
            printf("dbSize: %llu\n", (unsigned long long)leftover_dbsize);
        } else if (tag == 0x20) {
            /* leftover-safe-skip field 4; leftover-safe leader is printed */
            if (cetcd_leftover_safe_skip_field(data, len, &pos, tag)
                != CETCD_OK) {
                break;
            }
            printf("leader: %llu\n", (unsigned long long)leftover_leader);
        } else if (tag == 0x28) {
            /* leftover-safe-skip field 5; leftover-safe raftIndex is printed */
            if (cetcd_leftover_safe_skip_field(data, len, &pos, tag)
                != CETCD_OK) {
                break;
            }
            printf("raftIndex: %llu\n", (unsigned long long)leftover_ridx);
        } else if (tag == 0x30) {
            /* leftover-safe-skip field 6; leftover-safe raftTerm is printed */
            if (cetcd_leftover_safe_skip_field(data, len, &pos, tag)
                != CETCD_OK) {
                break;
            }
            printf("raftTerm: %llu\n", (unsigned long long)leftover_rterm);
        } else if (tag == 0x38) {
            /* leftover-safe-skip field 7; leftover-safe raftAppliedIndex */
            if (cetcd_leftover_safe_skip_field(data, len, &pos, tag)
                != CETCD_OK) {
                break;
            }
            printf("raftAppliedIndex: %llu\n",
                   (unsigned long long)leftover_rapplied);
        } else if (tag == 0x42) {
            /* leftover-safe: leftover cannot steal a printed alarm error */
            if (cetcd_leftover_safe_skip_field(data, len, &pos, tag)
                != CETCD_OK) {
                break;
            }
        } else if (tag == 0x48) {
            /* leftover-safe-skip field 9; leftover-safe dbSizeInUse is printed */
            if (cetcd_leftover_safe_skip_field(data, len, &pos, tag)
                != CETCD_OK) {
                break;
            }
            printf("dbSizeInUse: %llu\n", (unsigned long long)leftover_inuse);
        } else if (tag == 0x50) {
            /* leftover-safe-skip field 10; leftover-safe isLearner is printed */
            if (cetcd_leftover_safe_skip_field(data, len, &pos, tag)
                != CETCD_OK) {
                break;
            }
            if (leftover_learner) printf("isLearner: true\n");
        } else if (tag == 0x0a) {
            /* Skip header (length-delimited) */
            uint64_t l = 0; read_varint(data, len, &pos, &l);
            pos += l;
        } else if (tag == 0x00) {
            continue;
        } else if (cetcd_leftover_safe_skip_field(data, len, &pos, tag)
                   != CETCD_OK) {
            break;
        }
    }
    if (leftover_err[0])
        printf("error: %s\n", leftover_err);
}

static void parse_lease_grant_response(const uint8_t *data, size_t len) {
    char leftover_err[32];
    leftover_err[0] = '\0';
    /* leftover-safe: leftover cannot steal a printed grant error */
    if (cetcd_parse_lease_grant_error(data, len, leftover_err,
                                      sizeof(leftover_err)) != CETCD_OK)
        return;
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
        } else if (tag == 0x22) {
            /* leftover-safe-skip field 4; leftover-safe error is printed */
            if (cetcd_leftover_safe_skip_field(data, len, &pos, tag)
                != CETCD_OK)
                break;
        } else if (tag == 0x00) {
            continue;
        } else if (cetcd_leftover_safe_skip_field(data, len, &pos, tag)
                   != CETCD_OK) {
            break;
        }
    }
    if (leftover_err[0])
        printf("error: %s\n", leftover_err);
}

static void parse_lease_ttl_response(const uint8_t *data, size_t len) {
    char leftover_key[256];
    leftover_key[0] = '\0';
    /* leftover-safe: leftover cannot steal a printed TTL key */
    if (cetcd_parse_lease_ttl_key(data, len, leftover_key, sizeof(leftover_key))
        != CETCD_OK)
        return;
    int64_t leftover_granted = 0;
    /* leftover-safe: leftover cannot steal a printed grantedTTL */
    if (cetcd_parse_lease_ttl_granted(data, len, &leftover_granted) != CETCD_OK)
        return;
    int64_t leftover_ttl = 0;
    /* leftover-safe: leftover cannot steal a printed remaining TTL */
    if (cetcd_parse_lease_ttl_remaining(data, len, &leftover_ttl) != CETCD_OK)
        return;
    int64_t leftover_id = 0;
    /* leftover-safe: leftover cannot steal a printed lease ID */
    if (cetcd_parse_lease_ttl_id(data, len, &leftover_id) != CETCD_OK)
        return;
    size_t pos = 0;
    while (pos < len) {
        uint8_t tag = data[pos++];
        if (tag == 0x10) {
            /* leftover-safe-skip field 2; leftover-safe ID is printed */
            if (cetcd_leftover_safe_skip_field(data, len, &pos, tag)
                != CETCD_OK) {
                break;
            }
            printf("lease ID: %llu\n", (unsigned long long)leftover_id);
        } else if (tag == 0x18) {
            /* leftover-safe-skip field 3; leftover-safe remaining TTL is printed */
            if (cetcd_leftover_safe_skip_field(data, len, &pos, tag)
                != CETCD_OK) {
                break;
            }
            printf("remaining TTL: %lld\n", (long long)leftover_ttl);
        } else if (tag == 0x20) {
            /* leftover-safe-skip field 4; leftover-safe grantedTTL is printed */
            if (cetcd_leftover_safe_skip_field(data, len, &pos, tag)
                != CETCD_OK) {
                break;
            }
            printf("granted TTL: %lld\n", (long long)leftover_granted);
        } else if (tag == 0x2a) {
            /* leftover-safe-parse already fail-closed truncated leftover */
            uint64_t l = 0; read_varint(data, len, &pos, &l);
            printf("key: %.*s\n", (int)l, data + pos);
            pos += l;
            (void)leftover_key;
        } else if (tag == 0x0a) {
            /* Skip header (length-delimited) */
            uint64_t l = 0; read_varint(data, len, &pos, &l);
            pos += l;
        } else if (tag == 0x00) {
            continue;
        } else if (cetcd_leftover_safe_skip_field(data, len, &pos, tag)
                   != CETCD_OK) {
            break;
        }
    }
}

static void parse_member_list_response(const uint8_t *data, size_t len, int table_format, int json_format, int fields_format) {
    char leftover_curl[256];
    char leftover_name[128];
    leftover_curl[0] = '\0';
    leftover_name[0] = '\0';
    /* leftover-safe: leftover cannot steal a printed client URL or name */
    if (cetcd_parse_member_list_client_url(data, len, leftover_curl,
                                           sizeof(leftover_curl)) != CETCD_OK)
        return;
    if (cetcd_parse_member_list_name(data, len, leftover_name,
                                     sizeof(leftover_name)) != CETCD_OK)
        return;
    int leftover_learner = 0;
    /* leftover-safe: leftover cannot steal a printed isLearner */
    if (cetcd_parse_member_list_is_learner(data, len, &leftover_learner)
        != CETCD_OK)
        return;
    (void)leftover_learner;
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
            const uint8_t *peer_urls[8]; size_t peer_lens[8]; int n_peer = 0;
            const uint8_t *m_name = NULL; size_t name_len = 0;
            const uint8_t *client_urls[8]; size_t client_lens[8]; int n_client = 0;
            int is_learner = 0;
            while (pos < mend) {
                uint8_t mtag = data[pos++];
                if (mtag == 0x08) {
                    read_varint(data, mend, &pos, &mid);
                } else if (mtag == 0x12) {
                    /* field 2 = name (string) */
                    uint64_t l = 0; read_varint(data, mend, &pos, &l);
                    m_name = data + pos; name_len = (size_t)l;
                    pos += l;
                } else if (mtag == 0x1a) {
                    /* field 3 = peerURLs (repeated string) */
                    uint64_t l = 0; read_varint(data, mend, &pos, &l);
                    if (n_peer < 8) {
                        peer_urls[n_peer] = data + pos;
                        peer_lens[n_peer] = (size_t)l;
                        n_peer++;
                    }
                    pos += l;
                } else if (mtag == 0x22) {
                    /* field 4 = clientURLs (repeated string) */
                    uint64_t l = 0; read_varint(data, mend, &pos, &l);
                    if (n_client < 8) {
                        client_urls[n_client] = data + pos;
                        client_lens[n_client] = (size_t)l;
                        n_client++;
                    }
                    pos += l;
                } else if (mtag == 0x28) {
                    /* field 5 = isLearner (bool) */
                    uint64_t v = 0; read_varint(data, mend, &pos, &v);
                    is_learner = (int)v;
                } else if (mtag == 0x00) {
                    continue;
                } else if (cetcd_leftover_safe_skip_field(data, mend, &pos,
                                                          mtag) != CETCD_OK) {
                    break;
                }
            }
            pos = mend;
            if (json_format) {
                int ui;
                if (!first) printf(",");
                first = 0;
                printf("{\"ID\":%llu,\"name\":", (unsigned long long)mid);
                if (m_name) print_json_string(m_name, name_len); else fputs("\"\"", stdout);
                fputs(",\"peerURLs\":[", stdout);
                for (ui = 0; ui < n_peer; ui++) {
                    if (ui) fputc(',', stdout);
                    print_json_string(peer_urls[ui], peer_lens[ui]);
                }
                fputs("]", stdout);
                if (n_client) {
                    fputs(",\"clientURLs\":[", stdout);
                    for (ui = 0; ui < n_client; ui++) {
                        if (ui) fputc(',', stdout);
                        print_json_string(client_urls[ui], client_lens[ui]);
                    }
                    fputs("]", stdout);
                }
                if (is_learner) fputs(",\"isLearner\":true", stdout);
                fputs("}", stdout);
            } else if (table_format) {
                printf("| %16llu | %6s | %19.*s |\n",
                       (unsigned long long)mid, "alive",
                       n_peer ? (int)peer_lens[0] : 0,
                       n_peer ? peer_urls[0] : (const uint8_t *)"");
            } else if (fields_format) {
                int ui;
                printf("ID: %llu\n", (unsigned long long)mid);
                if (m_name) printf("name: %.*s\n", (int)name_len, m_name);
                for (ui = 0; ui < n_peer; ui++)
                    printf("peerURLs: %.*s\n", (int)peer_lens[ui], peer_urls[ui]);
                for (ui = 0; ui < n_client; ui++)
                    printf("clientURLs: %.*s\n", (int)client_lens[ui],
                           client_urls[ui]);
                if (is_learner) printf("isLearner: true\n");
                printf("\n");
            } else {
                printf("member ID: %llu peerURL: %.*s\n",
                       (unsigned long long)mid,
                       n_peer ? (int)peer_lens[0] : 0,
                       n_peer ? peer_urls[0] : (const uint8_t *)"");
            }
        } else if (tag == 0x0a) {
            /* Skip header (length-delimited) */
            uint64_t l = 0; read_varint(data, len, &pos, &l);
            pos += l;
        } else if (tag == 0x00) {
            continue;
        } else if (cetcd_leftover_safe_skip_field(data, len, &pos, tag)
                   != CETCD_OK) {
            break;
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
        } else if (tag == 0x00) {
            continue;
        } else if (cetcd_leftover_safe_skip_field(data, len, &pos, tag)
                   != CETCD_OK) {
            break;
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
        } else if (tag == 0x00) {
            continue;
        } else if (cetcd_leftover_safe_skip_field(data, len, &pos, tag)
                   != CETCD_OK) {
            break;
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

static int cmd_flag_is_(const char *arg, const char *name) {
    size_t n;
    if (!arg || !name || name[0] != '-') return 0;
    n = strlen(name);
    if (strncmp(arg, name, n) != 0) return 0;
    return arg[n] == '\0' || arg[n] == '=';
}

static void print_unknown_maint_flag_(int argc, char **argv, int start,
                                      int allow_cluster) {
    for (int i = start; i < argc; i++) {
        if (cmd_flag_is_(argv[i], "-w") || cmd_flag_is_(argv[i], "--write-out"))
            continue;
        if (allow_cluster && cmd_flag_is_(argv[i], "--cluster"))
            continue;
        fprintf(stderr, "unknown flag: %s\n", argv[i]);
        return;
    }
    fprintf(stderr, "unknown flag\n");
}

static int take_cmd_value_(int *i, int argc, char **argv, const char **out) {
    return cetcd_take_cli_flag_value(i, argc, argv, out) == CETCD_OK ? 0 : -1;
}

static int parse_i64_(const char *s, int64_t *out) {
    return cetcd_parse_i64(s, out) == CETCD_OK ? 0 : -1;
}

static int take_hashkv_rev_(int *i, int argc, char **argv, int64_t *rev) {
    const char *s = NULL;
    if (take_cmd_value_(i, argc, argv, &s) != 0) return -1;
    int64_t v = 0;
    if (cetcd_parse_i64(s, &v) != CETCD_OK || v < 0) return -1;
    *rev = v;
    return 0;
}

static int apply_write_out_jf_(const char *fmt, int *want_json, int *want_fields) {
    if (!fmt) return -1;
    if (strcmp(fmt, "json") == 0) { *want_json = 1; *want_fields = 0; return 0; }
    if (strcmp(fmt, "fields") == 0) { *want_json = 0; *want_fields = 1; return 0; }
    if (strcmp(fmt, "simple") == 0) { *want_json = 0; *want_fields = 0; return 0; }
    return 1;
}

static int take_write_out_jf_(int *i, int argc, char **argv,
                              int *want_json, int *want_fields) {
    const char *fmt = NULL;
    if (!cmd_flag_is_(argv[*i], "-w") && !cmd_flag_is_(argv[*i], "--write-out"))
        return 0;
    if (take_cmd_value_(i, argc, argv, &fmt) != 0) return -1;
    apply_write_out_jf_(fmt, want_json, want_fields);
    return 1;
}

static int take_write_out_jtf_(int *i, int argc, char **argv,
                               int *want_json, int *want_table, int *want_fields) {
    const char *fmt = NULL;
    if (!cmd_flag_is_(argv[*i], "-w") && !cmd_flag_is_(argv[*i], "--write-out"))
        return 0;
    if (take_cmd_value_(i, argc, argv, &fmt) != 0) return -1;
    if (strcmp(fmt, "json") == 0) { *want_json = 1; *want_table = 0; *want_fields = 0; }
    else if (strcmp(fmt, "table") == 0) { *want_json = 0; *want_table = 1; *want_fields = 0; }
    else if (strcmp(fmt, "fields") == 0) { *want_json = 0; *want_table = 0; *want_fields = 1; }
    else if (strcmp(fmt, "simple") == 0) { *want_json = 0; *want_table = 0; *want_fields = 0; }
    return 1;
}

static int skip_write_out_(int *i, int argc, char **argv) {
    const char *fmt = NULL;
    if (!cmd_flag_is_(argv[*i], "-w") && !cmd_flag_is_(argv[*i], "--write-out"))
        return 0;
    if (take_cmd_value_(i, argc, argv, &fmt) != 0) return -1;
    return 1;
}

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
        int on = 1, wj = 0, wf = 0, wr;
        if (cmd_flag_is_(argv[i], "--prev-kv")) {
            if (cetcd_take_cli_bool_eq(&i, argc, argv, &on) != CETCD_OK) {
                fprintf(stderr, "--prev-kv must be true or false\n");
                return 1;
            }
            prev_kv = on != 0;
        } else if (cmd_flag_is_(argv[i], "--ignore-value")) {
            if (cetcd_take_cli_bool_eq(&i, argc, argv, &on) != CETCD_OK) {
                fprintf(stderr, "--ignore-value must be true or false\n");
                return 1;
            }
            ignore_value = on != 0;
        } else if (cmd_flag_is_(argv[i], "--ignore-lease")) {
            if (cetcd_take_cli_bool_eq(&i, argc, argv, &on) != CETCD_OK) {
                fprintf(stderr, "--ignore-lease must be true or false\n");
                return 1;
            }
            ignore_lease = on != 0;
        } else if (cmd_flag_is_(argv[i], "--lease")) {
            const char *s = NULL;
            if (take_cmd_value_(&i, argc, argv, &s) != 0) {
                fprintf(stderr, "--lease requires a lease ID\n");
                return 1;
            }
            if (cetcd_parse_i64(s, &lease_id) != CETCD_OK || lease_id < 0) {
                fprintf(stderr, "--lease must be >= 0\n");
                return 1;
            }
        } else if (cmd_flag_is_(argv[i], "--print-value-only")) {
            if (cetcd_take_cli_bool_eq(&i, argc, argv, &on) != CETCD_OK) {
                fprintf(stderr, "--print-value-only must be true or false\n");
                return 1;
            }
            print_value_only = on != 0;
        } else if ((wr = take_write_out_jf_(&i, argc, argv, &wj, &wf)) != 0) {
            if (wr < 0) { fprintf(stderr, "--write-out requires a format\n"); return 1; }
            want_json = wj != 0;
            want_fields = wf != 0;
        } else if (strcmp(argv[i], "--") == 0) {
            for (i++; i < argc; i++) {
                if (!key) key = argv[i];
                else if (!val) val = argv[i];
                else {
                    fprintf(stderr, "unknown flag: %s\n", argv[i]);
                    return 1;
                }
            }
            break;
        } else if (cetcd_cli_is_long_flag(argv[i])) {
            fprintf(stderr, "unknown flag: %s\n", argv[i]);
            return 1;
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
    int64_t leftover_lease = 0;
    /* leftover-safe: leftover cannot steal a printed lease */
    if (cetcd_parse_range_response_kv_lease(resp, (size_t)rlen, &leftover_lease)
        != CETCD_OK) {
        fprintf(stderr, "request failed\n");
        if (stdin_val) free(stdin_val);
        return 1;
    }
    (void)leftover_lease;
    int64_t leftover_version = 0;
    /* leftover-safe: leftover cannot steal a printed version */
    if (cetcd_parse_range_response_kv_version(resp, (size_t)rlen,
                                              &leftover_version) != CETCD_OK) {
        fprintf(stderr, "request failed\n");
        if (stdin_val) free(stdin_val);
        return 1;
    }
    (void)leftover_version;
    int64_t leftover_create = 0;
    /* leftover-safe: leftover cannot steal a printed create_revision */
    if (cetcd_parse_range_response_kv_create_rev(resp, (size_t)rlen,
                                                 &leftover_create) != CETCD_OK) {
        fprintf(stderr, "request failed\n");
        if (stdin_val) free(stdin_val);
        return 1;
    }
    (void)leftover_create;
    int64_t leftover_mod = 0;
    /* leftover-safe: leftover cannot steal a printed mod_revision */
    if (cetcd_parse_range_response_kv_mod_rev(resp, (size_t)rlen,
                                              &leftover_mod) != CETCD_OK) {
        fprintf(stderr, "request failed\n");
        if (stdin_val) free(stdin_val);
        return 1;
    }
    (void)leftover_mod;
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
                        } else if (ktag == 0x00) {
                            continue;
                        } else if (cetcd_leftover_safe_skip_field(resp, kv_end,
                                                                  &rpos, ktag)
                                   != CETCD_OK) {
                            break;
                        }
                    }
                    rpos = kv_end;
                } else if (tag == 0x0a) {
                    uint64_t l = 0; read_varint(resp, rlen, &rpos, &l); rpos += l;
                } else if (tag == 0x00) {
                    continue;
                } else if (cetcd_leftover_safe_skip_field(resp, (size_t)rlen,
                                                          &rpos, tag)
                           != CETCD_OK) {
                    break;
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
                        } else if (ktag == 0x00) {
                            continue;
                        } else if (cetcd_leftover_safe_skip_field(resp, kv_end,
                                                                  &rpos, ktag)
                                   != CETCD_OK) {
                            break;
                        }
                    }
                    rpos = kv_end;
                } else if (tag == 0x0a) {
                    uint64_t l = 0; read_varint(resp, rlen, &rpos, &l); rpos += l;
                } else if (tag == 0x00) {
                    continue;
                } else if (cetcd_leftover_safe_skip_field(resp, (size_t)rlen,
                                                          &rpos, tag)
                           != CETCD_OK) {
                    break;
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
                    } else if (ktag == 0x00) {
                        continue;
                    } else if (cetcd_leftover_safe_skip_field(resp, kv_end,
                                                              &rpos, ktag)
                               != CETCD_OK) {
                        break;
                    }
                }
                if (pv && pv_len > 0) {
                    fwrite(pv, 1, pv_len, stdout);
                    printf("\n");
                }
                break;
            } else if (tag == 0x0a) {
                uint64_t l = 0; read_varint(resp, rlen, &rpos, &l); rpos += l;
            } else if (tag == 0x00) {
                continue;
            } else if (cetcd_leftover_safe_skip_field(resp, (size_t)rlen,
                                                      &rpos, tag)
                       != CETCD_OK) {
                break;
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
                    } else if (ktag == 0x00) {
                        continue;
                    } else if (cetcd_leftover_safe_skip_field(resp, kv_end,
                                                              &rpos, ktag)
                               != CETCD_OK) {
                        break;
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
            } else if (tag == 0x00) {
                continue;
            } else if (cetcd_leftover_safe_skip_field(resp, (size_t)rlen,
                                                      &rpos, tag)
                       != CETCD_OK) {
                break;
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
        int on = 1;
        if (cmd_flag_is_(argv[i], "--prefix")) {
            if (cetcd_take_cli_bool_eq(&i, argc, argv, &on) != CETCD_OK) {
                fprintf(stderr, "--prefix must be true or false\n");
                return 1;
            }
            prefix = on != 0;
        } else if (cmd_flag_is_(argv[i], "--from-key")) {
            if (cetcd_take_cli_bool_eq(&i, argc, argv, &on) != CETCD_OK) {
                fprintf(stderr, "--from-key must be true or false\n");
                return 1;
            }
            from_key = on != 0;
        } else if (cmd_flag_is_(argv[i], "--keys-only")) {
            if (cetcd_take_cli_bool_eq(&i, argc, argv, &on) != CETCD_OK) {
                fprintf(stderr, "--keys-only must be true or false\n");
                return 1;
            }
            keys_only = on != 0;
        } else if (cmd_flag_is_(argv[i], "--print-value-only")) {
            if (cetcd_take_cli_bool_eq(&i, argc, argv, &on) != CETCD_OK) {
                fprintf(stderr, "--print-value-only must be true or false\n");
                return 1;
            }
            print_value_only = on != 0;
        } else if (cmd_flag_is_(argv[i], "--hex")) {
            if (cetcd_take_cli_bool_eq(&i, argc, argv, &on) != CETCD_OK) {
                fprintf(stderr, "--hex must be true or false\n");
                return 1;
            }
            hex_output = on != 0;
        } else if (cmd_flag_is_(argv[i], "--consistency")) {
            const char *c = NULL;
            if (take_cmd_value_(&i, argc, argv, &c) != 0) {
                fprintf(stderr, "--consistency requires a value (l or s)\n");
                return 1;
            }
            if (strcmp(c, "s") == 0) serializable = true;
            else if (strcmp(c, "l") != 0) { fprintf(stderr, "--consistency must be 'l' or 's'\n"); return 1; }
        } else if (cmd_flag_is_(argv[i], "-w") || cmd_flag_is_(argv[i], "--write-out")) {
            const char *fmt = NULL;
            if (take_cmd_value_(&i, argc, argv, &fmt) != 0) {
                fprintf(stderr, "--write-out requires a format (json, simple, fields, table)\n");
                return 1;
            }
            if (strcmp(fmt, "json") == 0) { g_write_json = 1; g_write_fields = 0; g_write_table = 0; }
            else if (strcmp(fmt, "fields") == 0) { g_write_json = 0; g_write_fields = 1; g_write_table = 0; }
            else if (strcmp(fmt, "table") == 0) { g_write_json = 0; g_write_fields = 0; g_write_table = 1; }
            else if (strcmp(fmt, "simple") == 0) { g_write_json = 0; g_write_fields = 0; g_write_table = 0; }
            else { fprintf(stderr, "unsupported --write-out format: %s (use json, fields, table, or simple)\n", fmt); return 1; }
        } else if (cmd_flag_is_(argv[i], "--range-end")) {
            if (take_cmd_value_(&i, argc, argv, &range_end) != 0) {
                fprintf(stderr, "--range-end requires a key\n");
                return 1;
            }
        } else if (cmd_flag_is_(argv[i], "--count-only")) {
            if (cetcd_take_cli_bool_eq(&i, argc, argv, &on) != CETCD_OK) {
                fprintf(stderr, "--count-only must be true or false\n");
                return 1;
            }
            count_only = on != 0;
        } else if (cmd_flag_is_(argv[i], "--rev")) {
            const char *s = NULL;
            if (take_cmd_value_(&i, argc, argv, &s) != 0) {
                fprintf(stderr, "--rev requires a revision number\n");
                return 1;
            }
            if (parse_i64_(s, &rev) != 0 || rev < 0) {
                fprintf(stderr, "--rev must be >= 0\n");
                return 1;
            }
        } else if (cmd_flag_is_(argv[i], "--limit")) {
            const char *s = NULL;
            if (take_cmd_value_(&i, argc, argv, &s) != 0) {
                fprintf(stderr, "--limit requires a number\n");
                return 1;
            }
            if (parse_i64_(s, &limit) != 0 || limit < 0) {
                fprintf(stderr, "--limit must be >= 0\n");
                return 1;
            }
        } else if (cmd_flag_is_(argv[i], "--min-mod-rev")) {
            const char *s = NULL;
            if (take_cmd_value_(&i, argc, argv, &s) != 0) {
                fprintf(stderr, "--min-mod-rev requires a revision number\n");
                return 1;
            }
            if (parse_i64_(s, &min_mod_rev) != 0 || min_mod_rev < 0) {
                fprintf(stderr, "--min-mod-rev must be >= 0\n");
                return 1;
            }
        } else if (cmd_flag_is_(argv[i], "--max-mod-rev")) {
            const char *s = NULL;
            if (take_cmd_value_(&i, argc, argv, &s) != 0) {
                fprintf(stderr, "--max-mod-rev requires a revision number\n");
                return 1;
            }
            if (parse_i64_(s, &max_mod_rev) != 0 || max_mod_rev < 0) {
                fprintf(stderr, "--max-mod-rev must be >= 0\n");
                return 1;
            }
        } else if (cmd_flag_is_(argv[i], "--min-create-rev")) {
            const char *s = NULL;
            if (take_cmd_value_(&i, argc, argv, &s) != 0) {
                fprintf(stderr, "--min-create-rev requires a revision number\n");
                return 1;
            }
            if (parse_i64_(s, &min_create_rev) != 0 || min_create_rev < 0) {
                fprintf(stderr, "--min-create-rev must be >= 0\n");
                return 1;
            }
        } else if (cmd_flag_is_(argv[i], "--max-create-rev")) {
            const char *s = NULL;
            if (take_cmd_value_(&i, argc, argv, &s) != 0) {
                fprintf(stderr, "--max-create-rev requires a revision number\n");
                return 1;
            }
            if (parse_i64_(s, &max_create_rev) != 0 || max_create_rev < 0) {
                fprintf(stderr, "--max-create-rev must be >= 0\n");
                return 1;
            }
        } else if (cmd_flag_is_(argv[i], "--sort-by")) {
            const char *s = NULL;
            if (take_cmd_value_(&i, argc, argv, &s) != 0) {
                fprintf(stderr, "--sort-by requires a field name (key|version|create|mod|value)\n");
                return 1;
            }
            if (strcmp(s, "key") == 0) sort_target = 0;
            else if (strcmp(s, "version") == 0) sort_target = 1;
            else if (strcmp(s, "create") == 0) sort_target = 2;
            else if (strcmp(s, "mod") == 0) sort_target = 3;
            else if (strcmp(s, "value") == 0) sort_target = 4;
            else { fprintf(stderr, "invalid --sort-by: %s (use key|version|create|mod|value)\n", s); return 1; }
            if (sort_order == 0) sort_order = 1; /* default to ASCEND when sort-by is set */
        } else if (cmd_flag_is_(argv[i], "--sort-order")) {
            const char *s = NULL;
            if (take_cmd_value_(&i, argc, argv, &s) != 0) {
                fprintf(stderr, "--sort-order requires an order (ascend|descend)\n");
                return 1;
            }
            if (strcmp(s, "ascend") == 0) sort_order = 1;
            else if (strcmp(s, "descend") == 0) sort_order = 2;
            else { fprintf(stderr, "invalid --sort-order: %s (use ascend|descend)\n", s); return 1; }
        } else if (strcmp(argv[i], "--") == 0) {
            for (i++; i < argc; i++) {
                if (!key) key = argv[i];
                else if (!range_end) range_end = argv[i];
                else {
                    fprintf(stderr, "unknown flag: %s\n", argv[i]);
                    return 1;
                }
            }
            break;
        } else if (cetcd_cli_is_long_flag(argv[i])) {
            fprintf(stderr, "unknown flag: %s\n", argv[i]);
            return 1;
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
        uint8_t prefix_end[256];
        size_t pe_len = cetcd_key_prefix_end(prefix_end, sizeof(prefix_end),
                                             cetcd_slice_make(key, key_len));
        if (pe_len == 0) { fprintf(stderr, "key too long\n"); return 1; }
        pos = encode_bytes_field(req, sizeof(req), pos, 0x12, prefix_end, pe_len);
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
        int on = 1, wj = 0, wf = 0, wr;
        if (cmd_flag_is_(argv[i], "--prefix")) {
            if (cetcd_take_cli_bool_eq(&i, argc, argv, &on) != CETCD_OK) {
                fprintf(stderr, "--prefix must be true or false\n");
                return 1;
            }
            prefix = on != 0;
        } else if (cmd_flag_is_(argv[i], "--from-key")) {
            if (cetcd_take_cli_bool_eq(&i, argc, argv, &on) != CETCD_OK) {
                fprintf(stderr, "--from-key must be true or false\n");
                return 1;
            }
            from_key = on != 0;
        } else if (cmd_flag_is_(argv[i], "--range-end")) {
            if (take_cmd_value_(&i, argc, argv, &range_end) != 0) {
                fprintf(stderr, "--range-end requires a key\n");
                return 1;
            }
        } else if (cmd_flag_is_(argv[i], "--prev-kv")) {
            if (cetcd_take_cli_bool_eq(&i, argc, argv, &on) != CETCD_OK) {
                fprintf(stderr, "--prev-kv must be true or false\n");
                return 1;
            }
            prev_kv = on != 0;
        } else if (cmd_flag_is_(argv[i], "--hex")) {
            if (cetcd_take_cli_bool_eq(&i, argc, argv, &on) != CETCD_OK) {
                fprintf(stderr, "--hex must be true or false\n");
                return 1;
            }
            hex_output = on != 0;
        } else if (cmd_flag_is_(argv[i], "--print-value-only")) {
            if (cetcd_take_cli_bool_eq(&i, argc, argv, &on) != CETCD_OK) {
                fprintf(stderr, "--print-value-only must be true or false\n");
                return 1;
            }
            print_value_only = on != 0;
        } else if ((wr = take_write_out_jf_(&i, argc, argv, &wj, &wf)) != 0) {
            if (wr < 0) { fprintf(stderr, "--write-out requires a format\n"); return 1; }
            want_json = wj != 0;
            want_fields = wf != 0;
        } else if (strcmp(argv[i], "--") == 0) {
            for (i++; i < argc; i++) {
                if (!key) key = argv[i];
                else if (!range_end) range_end = argv[i];
                else {
                    fprintf(stderr, "unknown flag: %s\n", argv[i]);
                    return 1;
                }
            }
            break;
        } else if (cetcd_cli_is_long_flag(argv[i])) {
            fprintf(stderr, "unknown flag: %s\n", argv[i]);
            return 1;
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
        uint8_t prefix_end[256];
        size_t pe_len = cetcd_key_prefix_end(prefix_end, sizeof(prefix_end),
                                             cetcd_slice_make(key, key_len));
        if (pe_len == 0) { fprintf(stderr, "key too long\n"); return 1; }
        pos = encode_bytes_field(req, sizeof(req), pos, 0x12, prefix_end, pe_len);
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
    int64_t leftover_deleted = 0;
    /* leftover-safe: leftover cannot steal a printed delete count */
    if (cetcd_parse_delete_range_deleted(resp, (size_t)rlen, &leftover_deleted)
        != CETCD_OK) {
        fprintf(stderr, "request failed\n");
        return 1;
    }
    int64_t leftover_lease = 0;
    /* leftover-safe: leftover cannot steal a printed lease */
    if (cetcd_parse_range_response_kv_lease(resp, (size_t)rlen, &leftover_lease)
        != CETCD_OK) {
        fprintf(stderr, "request failed\n");
        return 1;
    }
    (void)leftover_lease;
    int64_t leftover_version = 0;
    /* leftover-safe: leftover cannot steal a printed version */
    if (cetcd_parse_range_response_kv_version(resp, (size_t)rlen,
                                              &leftover_version) != CETCD_OK) {
        fprintf(stderr, "request failed\n");
        return 1;
    }
    (void)leftover_version;
    int64_t leftover_create = 0;
    /* leftover-safe: leftover cannot steal a printed create_revision */
    if (cetcd_parse_range_response_kv_create_rev(resp, (size_t)rlen,
                                                 &leftover_create) != CETCD_OK) {
        fprintf(stderr, "request failed\n");
        return 1;
    }
    (void)leftover_create;
    int64_t leftover_mod = 0;
    /* leftover-safe: leftover cannot steal a printed mod_revision */
    if (cetcd_parse_range_response_kv_mod_rev(resp, (size_t)rlen,
                                              &leftover_mod) != CETCD_OK) {
        fprintf(stderr, "request failed\n");
        return 1;
    }
    (void)leftover_mod;
    if (want_fields) {
        size_t rpos = 0;
        uint64_t deleted = (uint64_t)leftover_deleted;
        while (rpos < (size_t)rlen) {
            uint8_t tag = resp[rpos++];
            if (tag == 0x10) {
                if (cetcd_leftover_safe_skip_field(resp, (size_t)rlen,
                                                    &rpos, tag) != CETCD_OK)
                    break;
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
                    } else if (ktag == 0x00) {
                        continue;
                    } else if (cetcd_leftover_safe_skip_field(resp, kv_end,
                                                              &rpos, ktag)
                               != CETCD_OK) {
                        break;
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
            } else if (tag == 0x00) {
                continue;
            } else if (cetcd_leftover_safe_skip_field(resp, (size_t)rlen,
                                                      &rpos, tag)
                       != CETCD_OK) {
                break;
            }
        }
        printf("%llu key(s) deleted\n", (unsigned long long)deleted);
        return 0;
    }
    if (want_json) {
        /* JSON output: {"header":{...},"deleted":N,"prev_kvs":[...]} */
        size_t rpos = 0;
        uint64_t deleted = (uint64_t)leftover_deleted;
        /* Collect prev_kvs */
        int has_prev_kvs = 0;
        fputs("{", stdout);
        parse_and_print_header_json(resp, (size_t)rlen);
        fputs(",", stdout);
        while (rpos < (size_t)rlen) {
            uint8_t tag = resp[rpos++];
            if (tag == 0x10) {
                if (cetcd_leftover_safe_skip_field(resp, (size_t)rlen,
                                                    &rpos, tag) != CETCD_OK)
                    break;
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
                    } else if (ktag == 0x00) {
                        continue;
                    } else if (cetcd_leftover_safe_skip_field(resp, kv_end,
                                                              &rpos, ktag)
                               != CETCD_OK) {
                        break;
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
            } else if (tag == 0x00) {
                continue;
            } else if (cetcd_leftover_safe_skip_field(resp, (size_t)rlen,
                                                      &rpos, tag)
                       != CETCD_OK) {
                break;
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
                    } else if (ktag == 0x00) {
                        continue;
                    } else if (cetcd_leftover_safe_skip_field(resp, kv_end,
                                                              &rpos, ktag)
                               != CETCD_OK) {
                        break;
                    }
                }
                rpos = kv_end;
                if (pv && pv_len > 0) {
                    fwrite(pv, 1, pv_len, stdout);
                    printf("\n");
                }
            } else if (tag == 0x0a || tag == 0x12) {
                uint64_t l = 0; read_varint(resp, rlen, &rpos, &l); rpos += l;
            } else if (tag == 0x00) {
                continue;
            } else if (cetcd_leftover_safe_skip_field(resp, (size_t)rlen,
                                                      &rpos, tag)
                       != CETCD_OK) {
                break;
            }
        }
        return 0;
    }
    /* Parse DeleteRangeResponse: field 2 (deleted), field 3 (prev_kvs) */
    printf("%llu key(s) deleted\n", (unsigned long long)leftover_deleted);
    size_t rpos = 0;
    while (rpos < (size_t)rlen) {
        uint8_t tag = resp[rpos++];
        if (tag == 0x10) {
            if (cetcd_leftover_safe_skip_field(resp, (size_t)rlen,
                                                &rpos, tag) != CETCD_OK)
                break;
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
                } else if (ktag == 0x00) {
                    continue;
                } else if (cetcd_leftover_safe_skip_field(resp, kv_end,
                                                          &rpos, ktag)
                           != CETCD_OK) {
                    break;
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
        } else if (tag == 0x00) {
            continue;
        } else if (cetcd_leftover_safe_skip_field(resp, (size_t)rlen,
                                                  &rpos, tag)
                   != CETCD_OK) {
            break;
        }
    }
    return 0;
}

static int parse_positive_u64_(const char *s, uint64_t *out) {
    if (!s || !out) return -1;
    char *end = NULL;
    errno = 0;
    unsigned long long v = strtoull(s, &end, 10);
    if (errno == ERANGE || !end || end == s || *end || v < 1)
        return -1;
    *out = (uint64_t)v;
    return 0;
}

static int parse_positive_hex_u64_(const char *s, uint64_t *out) {
    if (!s || !out) return -1;
    char *end = NULL;
    errno = 0;
    unsigned long long v = strtoull(s, &end, 16);
    if (errno == ERANGE || !end || end == s || *end || v < 1)
        return -1;
    *out = (uint64_t)v;
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
        int wr = 0, sk = 0, wj = 0, wt = 0, wf = 0;
            if ((wr = take_write_out_jf_(&i, argc, argv, &wj, &wf)) != 0) {
                if (wr < 0) { fprintf(stderr, "--write-out requires a format\n"); return 1; }
                want_json = wj != 0;
                want_fields = wf != 0;
            } else if (cmd_flag_is_(argv[i], "--lease-id")) {
                const char *s = NULL;
                char *end = NULL;
                if (take_cmd_value_(&i, argc, argv, &s) != 0) {
                    fprintf(stderr, "--lease-id must be a hex integer\n");
                    return 1;
                }
                errno = 0;
                unsigned long long v = strtoull(s, &end, 16);
                if (errno == ERANGE || !end || end == s || *end) {
                    fprintf(stderr, "--lease-id must be a hex integer\n");
                    return 1;
                }
                lease_id = (uint64_t)v;
                has_lease_id = true;
            } else if (cetcd_cli_is_long_flag(argv[i])) {
                fprintf(stderr, "unknown flag: %s\n", argv[i]);
                return 1;
            } else if (!ttl_str) {
                ttl_str = argv[i];
            } else {
                fprintf(stderr, "unknown flag: %s\n", argv[i]);
                return 1;
            }
        }
        if (!ttl_str) { fprintf(stderr, "usage: cetcdctl lease grant [--lease-id ID] [-w json|fields] TTL\n"); return 1; }
        char *ttl_end = NULL;
        errno = 0;
        long ttl_sec = strtol(ttl_str, &ttl_end, 10);
        if (errno == ERANGE || !ttl_end || ttl_end == ttl_str || *ttl_end ||
            ttl_sec < 1 || ttl_sec > 0x7fffffffL) {
            fprintf(stderr, "lease grant TTL must be > 0\n");
            return 1;
        }
        uint8_t req[32], resp[256];
        size_t pos = 0;
        pos = encode_varint_field(req, sizeof(req), pos, 0x08, (uint64_t)ttl_sec);
        pos = encode_varint_field(req, sizeof(req), pos, 0x10, has_lease_id ? lease_id : 0);
        int rlen = do_rpc("/etcdserverpb.Lease/LeaseGrant", req, pos, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        if (want_json) {
            int64_t lid = 0, ttl = 0;
            char leftover_err[32];
            leftover_err[0] = '\0';
            /* leftover-safe: leftover cannot steal a printed grant ID */
            if (cetcd_parse_lease_grant_response(resp, (size_t)rlen, &lid, &ttl)
                != CETCD_OK) {
                fprintf(stderr, "request failed\n");
                return 1;
            }
            /* leftover-safe: leftover cannot steal a printed grant error */
            if (cetcd_parse_lease_grant_error(resp, (size_t)rlen, leftover_err,
                                              sizeof(leftover_err))
                != CETCD_OK) {
                fprintf(stderr, "request failed\n");
                return 1;
            }
            fputs("{", stdout);
            parse_and_print_header_json(resp, (size_t)rlen);
            printf(",\"ID\":%llu,\"TTL\":%llu",
                   (unsigned long long)lid, (unsigned long long)ttl);
            if (leftover_err[0]) {
                fputs(",\"error\":", stdout);
                print_json_string((const uint8_t *)leftover_err,
                                  strlen(leftover_err));
            }
            fputs("}\n", stdout);
        } else if (want_fields) {
            int64_t lid = 0, ttl = 0;
            char leftover_err[32];
            leftover_err[0] = '\0';
            /* leftover-safe: leftover cannot steal a printed grant ID */
            if (cetcd_parse_lease_grant_response(resp, (size_t)rlen, &lid, &ttl)
                != CETCD_OK) {
                fprintf(stderr, "request failed\n");
                return 1;
            }
            /* leftover-safe: leftover cannot steal a printed grant error */
            if (cetcd_parse_lease_grant_error(resp, (size_t)rlen, leftover_err,
                                              sizeof(leftover_err))
                != CETCD_OK) {
                fprintf(stderr, "request failed\n");
                return 1;
            }
            parse_and_print_header_json(resp, (size_t)rlen);
            printf("ID: %llu\n", (unsigned long long)lid);
            printf("TTL: %llu\n", (unsigned long long)ttl);
            if (leftover_err[0])
                printf("error: %s\n", leftover_err);
            fputs("\n", stdout);
        } else {
            parse_lease_grant_response(resp, rlen);
        }
    } else if (strcmp(argv[2], "revoke") == 0) {
        bool want_json = false;
        bool want_fields = false;
        const char *id_str = NULL;
        for (int i = 3; i < argc; i++) {
        int wr = 0, sk = 0, wj = 0, wt = 0, wf = 0;
            if ((wr = take_write_out_jf_(&i, argc, argv, &wj, &wf)) != 0) {
                if (wr < 0) { fprintf(stderr, "--write-out requires a format\n"); return 1; }
                want_json = wj != 0;
                want_fields = wf != 0;
            } else if (cetcd_cli_is_long_flag(argv[i])) {
                fprintf(stderr, "unknown flag: %s\n", argv[i]);
                return 1;
            } else if (!id_str) {
                id_str = argv[i];
            } else {
                fprintf(stderr, "unknown flag: %s\n", argv[i]);
                return 1;
            }
        }
        if (!id_str) { fprintf(stderr, "usage: cetcdctl lease revoke [-w json|fields] ID\n"); return 1; }
        uint64_t lease_id = 0;
        if (parse_positive_u64_(id_str, &lease_id) != 0) {
            fprintf(stderr, "lease revoke ID must be > 0\n");
            return 1;
        }
        uint8_t req[32], resp[256];
        size_t pos = 0;
        pos = encode_varint_field(req, sizeof(req), pos, 0x08, lease_id);
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
        int wr = 0, sk = 0, wj = 0, wt = 0, wf = 0;
            int on = 1;
            if (cmd_flag_is_(argv[i], "--keys")) {
                if (cetcd_take_cli_bool_eq(&i, argc, argv, &on) != CETCD_OK) {
                    fprintf(stderr, "--keys must be true or false\n");
                    return 1;
                }
                want_keys = on != 0;
            } else if ((wr = take_write_out_jf_(&i, argc, argv, &wj, &wf)) != 0) {
                if (wr < 0) { fprintf(stderr, "--write-out requires a format\n"); return 1; }
                want_json = wj != 0;
                want_fields = wf != 0;
            } else if (cetcd_cli_is_long_flag(argv[i])) {
                fprintf(stderr, "unknown flag: %s\n", argv[i]);
                return 1;
            } else if (!id_str) {
                id_str = argv[i];
            } else {
                fprintf(stderr, "unknown flag: %s\n", argv[i]);
                return 1;
            }
        }
        if (!id_str) { fprintf(stderr, "usage: cetcdctl lease timetolive [--keys] [-w json|fields] ID\n"); return 1; }
        uint64_t ttl_id = 0;
        if (parse_positive_u64_(id_str, &ttl_id) != 0) {
            fprintf(stderr, "lease timetolive ID must be > 0\n");
            return 1;
        }
        uint8_t req[32], resp[4096];
        size_t pos = 0;
        pos = encode_varint_field(req, sizeof(req), pos, 0x08, ttl_id);
        if (want_keys) {
            pos = encode_varint_field(req, sizeof(req), pos, 0x10, 1);
        }
        int rlen = do_rpc("/etcdserverpb.Lease/LeaseTimeToLive", req, pos, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        char leftover_key[256];
        leftover_key[0] = '\0';
        /* leftover-safe: leftover cannot steal a printed TTL key */
        if (cetcd_parse_lease_ttl_key(resp, (size_t)rlen, leftover_key,
                                      sizeof(leftover_key)) != CETCD_OK) {
            fprintf(stderr, "request failed\n");
            return 1;
        }
        int64_t leftover_granted = 0;
        /* leftover-safe: leftover cannot steal a printed grantedTTL */
        if (cetcd_parse_lease_ttl_granted(resp, (size_t)rlen, &leftover_granted)
            != CETCD_OK) {
            fprintf(stderr, "request failed\n");
            return 1;
        }
        int64_t leftover_ttl = 0;
        /* leftover-safe: leftover cannot steal a printed remaining TTL */
        if (cetcd_parse_lease_ttl_remaining(resp, (size_t)rlen, &leftover_ttl)
            != CETCD_OK) {
            fprintf(stderr, "request failed\n");
            return 1;
        }
        int64_t leftover_id = 0;
        /* leftover-safe: leftover cannot steal a printed lease ID */
        if (cetcd_parse_lease_ttl_id(resp, (size_t)rlen, &leftover_id)
            != CETCD_OK) {
            fprintf(stderr, "request failed\n");
            return 1;
        }
        if (want_json) {
            size_t rpos = 0;
            uint64_t lid = (uint64_t)leftover_id, ttl = (uint64_t)leftover_ttl, granted = (uint64_t)leftover_granted;
            fputs("{", stdout);
            parse_and_print_header_json(resp, (size_t)rlen);
            fputs(",", stdout);
            /* First pass: collect ID/TTL/granted, count keys */
            int key_count = 0;
            while (rpos < (size_t)rlen) {
                uint8_t tag = resp[rpos++];
                if (tag == 0x10) {
                    /* leftover-safe: leftover cannot steal a printed lease ID */
                    if (cetcd_leftover_safe_skip_field(resp, (size_t)rlen,
                                                        &rpos, tag)
                        != CETCD_OK) {
                        break;
                    }
                } else if (tag == 0x18) {
                    /* leftover-safe: leftover cannot steal a printed remaining TTL */
                    if (cetcd_leftover_safe_skip_field(resp, (size_t)rlen,
                                                        &rpos, tag)
                        != CETCD_OK) {
                        break;
                    }
                } else if (tag == 0x20) {
                    /* leftover-safe: leftover cannot steal a printed grantedTTL */
                    if (cetcd_leftover_safe_skip_field(resp, (size_t)rlen,
                                                        &rpos, tag)
                        != CETCD_OK) {
                        break;
                    }
                } else if (tag == 0x2a) {
                    uint64_t l = 0; read_varint(resp, rlen, &rpos, &l);
                    rpos += l;
                    key_count++;
                } else if (tag == 0x0a) { uint64_t l = 0; read_varint(resp, rlen, &rpos, &l); rpos += l; }
                else if (tag == 0x00) { continue; }
                else if (cetcd_leftover_safe_skip_field(resp, (size_t)rlen,
                                                        &rpos, tag)
                         != CETCD_OK) {
                    break;
                }
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
                    else if (tag == 0x00) { continue; }
                    else if (cetcd_leftover_safe_skip_field(resp, (size_t)rlen,
                                                            &rpos, tag)
                             != CETCD_OK) {
                        break;
                    }
                }
                fputs("]", stdout);
            }
            fputs("}\n", stdout);
        } else if (want_fields) {
            size_t rpos = 0;
            uint64_t lid = (uint64_t)leftover_id, ttl = (uint64_t)leftover_ttl, granted = (uint64_t)leftover_granted;
            while (rpos < (size_t)rlen) {
                uint8_t tag = resp[rpos++];
                if (tag == 0x10) {
                    /* leftover-safe: leftover cannot steal a printed lease ID */
                    if (cetcd_leftover_safe_skip_field(resp, (size_t)rlen,
                                                        &rpos, tag)
                        != CETCD_OK) {
                        break;
                    }
                } else if (tag == 0x18) {
                    /* leftover-safe: leftover cannot steal a printed remaining TTL */
                    if (cetcd_leftover_safe_skip_field(resp, (size_t)rlen,
                                                        &rpos, tag)
                        != CETCD_OK) {
                        break;
                    }
                } else if (tag == 0x20) {
                    /* leftover-safe: leftover cannot steal a printed grantedTTL */
                    if (cetcd_leftover_safe_skip_field(resp, (size_t)rlen,
                                                        &rpos, tag)
                        != CETCD_OK) {
                        break;
                    }
                }
                else if (tag == 0x2a) { uint64_t l = 0; read_varint(resp, rlen, &rpos, &l); rpos += l; }
                else if (tag == 0x0a) { uint64_t l = 0; read_varint(resp, rlen, &rpos, &l); rpos += l; }
                else if (tag == 0x00) { continue; }
                else if (cetcd_leftover_safe_skip_field(resp, (size_t)rlen,
                                                        &rpos, tag)
                         != CETCD_OK) {
                    break;
                }
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
                    else if (tag == 0x00) { continue; }
                    else if (cetcd_leftover_safe_skip_field(resp, (size_t)rlen,
                                                            &rpos, tag)
                             != CETCD_OK) {
                        break;
                    }
                }
            }
            fputs("\n", stdout);
        } else {
            parse_lease_ttl_response(resp, rlen);
        }
    } else if (strcmp(argv[2], "list") == 0) {
        int table_fmt = 0, json_fmt = 0, fields_fmt = 0;
        for (int i = 3; i < argc; i++) {
        int wr = 0, sk = 0, wj = 0, wt = 0, wf = 0;
            if ((wr = take_write_out_jtf_(&i, argc, argv, &json_fmt, &table_fmt, &fields_fmt)) != 0) {
                if (wr < 0) { fprintf(stderr, "--write-out requires a format\n"); return 1; }
            } else if (cetcd_cli_is_long_flag(argv[i])) {
                fprintf(stderr, "unknown flag: %s\n", argv[i]);
                return 1;
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
                    } else if (ltag == 0x00) {
                        continue;
                    } else if (cetcd_leftover_safe_skip_field(resp, lend, &rpos,
                                                              ltag)
                               != CETCD_OK) {
                        break;
                    }
                }
                rpos = lend;
            } else if (tag == 0x0a) {
                /* Skip header (length-delimited) */
                uint64_t l = 0; read_varint(resp, rlen, &rpos, &l);
                rpos += l;
            } else if (tag == 0x00) {
                continue;
            } else if (cetcd_leftover_safe_skip_field(resp, (size_t)rlen, &rpos,
                                                      tag) != CETCD_OK) {
                break;
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
            int on = 1, wj = 0, wf = 0, wr;
            if (cmd_flag_is_(argv[i], "--once")) {
                if (cetcd_take_cli_bool_eq(&i, argc, argv, &on) != CETCD_OK) {
                    fprintf(stderr, "--once must be true or false\n");
                    return 1;
                }
                once = on;
            } else if (cmd_flag_is_(argv[i], "--interval")) {
                const char *s = NULL;
                int64_t v = 0;
                if (take_cmd_value_(&i, argc, argv, &s) != 0 ||
                    cetcd_parse_i64(s, &v) != CETCD_OK ||
                    v < 1 || v > 0x7fffffffLL) {
                    fprintf(stderr, "--interval must be > 0\n");
                    return 1;
                }
                interval_sec = (int)v;
            } else if ((wr = take_write_out_jf_(&i, argc, argv, &wj, &wf)) != 0) {
                if (wr < 0) { fprintf(stderr, "--write-out requires a format\n"); return 1; }
                want_json = wj != 0;
                want_fields = wf != 0;
            } else if (cetcd_cli_is_long_flag(argv[i])) {
                fprintf(stderr, "unknown flag: %s\n", argv[i]);
                return 1;
            } else if (!id_str) {
                id_str = argv[i];
            } else {
                fprintf(stderr, "unknown flag: %s\n", argv[i]);
                return 1;
            }
        }
        if (!id_str) { fprintf(stderr, "usage: cetcdctl lease keepalive [--once] [--interval SEC] [-w json|fields] ID\n"); return 1; }
        uint64_t lease_id = 0;
        if (parse_positive_u64_(id_str, &lease_id) != 0) {
            fprintf(stderr, "lease keepalive ID must be > 0\n");
            return 1;
        }
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
            int64_t kid_i = 0, ttl_i = 0;
            if (cetcd_parse_lease_keepalive_response(resp, (size_t)rlen,
                                                     &kid_i, &ttl_i)
                != CETCD_OK) {
                fprintf(stderr, "request failed\n");
                return 1;
            }
            uint64_t kid = (uint64_t)kid_i, ttl = (uint64_t)ttl_i;
            if (!want_json && !want_fields) {
                printf("lease ID: %llu\n", (unsigned long long)kid);
                printf("TTL: %llu seconds\n", (unsigned long long)ttl);
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
    int phys = 0;
    for (int i = 2; i < argc; i++) {
        int wr = 0, sk = 0, wj = 0, wt = 0, wf = 0;
        if ((wr = take_write_out_jf_(&i, argc, argv, &wj, &wf)) != 0) {
            if (wr < 0) { fprintf(stderr, "--write-out requires a format\n"); return 1; }
            want_json = wj != 0;
            want_fields = wf != 0;
        }
    }
    if (cetcd_ctl_parse_compact_argv(argc, argv, 2, &phys, &rev) != CETCD_OK) {
        for (int i = 2; i < argc; i++) {
            if (cmd_flag_is_(argv[i], "-w") || cmd_flag_is_(argv[i], "--write-out") ||
                cmd_flag_is_(argv[i], "--physical"))
                continue;
            if (argv[i][0] == '-') {
                fprintf(stderr, "unknown flag: %s\n", argv[i]);
                return 1;
            }
        }
        fprintf(stderr, "usage: cetcdctl compact [--physical] [-w json|fields] REV\n");
        return 1;
    }
    physical = phys != 0;
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
        int wr = take_write_out_jf_(&i, argc, argv, &want_json, &want_fields);
        if (wr < 0) { fprintf(stderr, "--write-out requires a format\n"); return 1; }
        if (wr > 0) {
            if (want_json) g_write_json = 1;
            if (want_fields) g_write_fields = 1;
        }
    }
    if (strcmp(argv[2], "put") == 0) {
        const char *key = NULL, *val = NULL;
        if (cetcd_ctl_parse_two_name_argv(argc, argv, 3, &key, &val) != CETCD_OK) {
            fprintf(stderr, "usage: cetcdctl txn put KEY VALUE\n");
            return 1;
        }
        /* Build a TxnRequest with one success op (Put) */
        uint8_t put_inner[1024];
        size_t ppos = 0;
        ppos = encode_bytes_field(put_inner, sizeof(put_inner), ppos, 0x0a,
                                   (const uint8_t *)key, strlen(key));
        ppos = encode_bytes_field(put_inner, sizeof(put_inner), ppos, 0x12,
                                   (const uint8_t *)val, strlen(val));

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
        const char *key = NULL, *expected = NULL, *new_val = NULL;
        if (cetcd_ctl_parse_three_name_argv(argc, argv, 3, &key, &expected,
                                           &new_val) != CETCD_OK) {
            fprintf(stderr, "usage: cetcdctl txn cas KEY EXPECTED NEW\n");
            return 1;
        }
        size_t key_len = strlen(key);
        size_t exp_len = strlen(expected);
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
        /* leftover-safe: leftover cannot steal succeeded */
        int succeeded_i = 0;
        if (cetcd_parse_txn_succeeded(resp, (size_t)rlen, &succeeded_i)
            != CETCD_OK)
            succeeded_i = 0;
        bool succeeded = succeeded_i != 0;
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
        int wr = 0, sk = 0, wj = 0, wt = 0, wf = 0;
            if ((sk = skip_write_out_(&i, argc, argv)) != 0) { if (sk < 0) { fprintf(stderr, "--write-out requires a format\n"); return 1; } continue; }
            if (strcmp(argv[i], "--") == 0) {
                if (i + 1 < argc && !key) key = argv[++i];
                else {
                    fprintf(stderr, "unknown flag: %s\n", argv[i]);
                    return 1;
                }
            } else if (argv[i][0] == '-') {
                fprintf(stderr, "unknown flag: %s\n", argv[i]);
                return 1;
            } else if (!key) {
                key = argv[i];
            } else if (!range_end) {
                range_end = argv[i];
            } else {
                fprintf(stderr, "unknown flag: %s\n", argv[i]);
                return 1;
            }
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
                        if (cetcd_leftover_safe_skip_field(resp, sub_end, &rp,
                                                           sub_tag) != CETCD_OK)
                            break;
                    } else if (sub_tag == 0x1a) {
                        /* ResponseDeleteRange */
                        if (cetcd_leftover_safe_skip_field(resp, sub_end, &rp,
                                                           sub_tag) != CETCD_OK)
                            break;
                    } else if (sub_tag == 0x00) {
                        continue;
                    } else if (cetcd_leftover_safe_skip_field(resp, sub_end,
                                                              &rp, sub_tag)
                               != CETCD_OK) {
                        break;
                    }
                }
            } else if (tag == 0x10) {
                uint64_t v = 0; read_varint(resp, rlen, &rp, &v);
            } else if (tag == 0x00) {
                continue;
            } else if (cetcd_leftover_safe_skip_field(resp, (size_t)rlen, &rp,
                                                      tag) != CETCD_OK) {
                break;
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
        int wr = 0, sk = 0, wj = 0, wt = 0, wf = 0;
            int on = 1;
            if ((sk = skip_write_out_(&i, argc, argv)) != 0) { if (sk < 0) { fprintf(stderr, "--write-out requires a format\n"); return 1; } continue; }
            if (cmd_flag_is_(argv[i], "--prefix")) {
                if (cetcd_take_cli_bool_eq(&i, argc, argv, &on) != CETCD_OK) {
                    fprintf(stderr, "--prefix must be true or false\n");
                    return 1;
                }
                prefix = on != 0;
            } else if (cmd_flag_is_(argv[i], "--from-key")) {
                if (cetcd_take_cli_bool_eq(&i, argc, argv, &on) != CETCD_OK) {
                    fprintf(stderr, "--from-key must be true or false\n");
                    return 1;
                }
                from_key = on != 0;
            } else if (cmd_flag_is_(argv[i], "--prev-kv")) {
                if (cetcd_take_cli_bool_eq(&i, argc, argv, &on) != CETCD_OK) {
                    fprintf(stderr, "--prev-kv must be true or false\n");
                    return 1;
                }
                prev_kv = on != 0;
            } else if (argv[i][0] == '-') {
                fprintf(stderr, "unknown flag: %s\n", argv[i]);
                return 1;
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
            uint8_t prefix_end[256];
            size_t pe_len = cetcd_key_prefix_end(prefix_end, sizeof(prefix_end),
                                                 cetcd_slice_make(key, key_len));
            if (pe_len == 0) { fprintf(stderr, "key too long\n"); return 1; }
            dpos = encode_bytes_field(del_inner, sizeof(del_inner), dpos, 0x12,
                                      prefix_end, pe_len);
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
                    int64_t cv = 0;
                    if (parse_i64_(val, &cv) != 0) {
                        fprintf(stderr, "compare value must be an integer\n");
                        continue;
                    }
                    cl = encode_varint_field(cmp, sizeof(cmp), cl, vtag, (uint64_t)cv);
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
        int succeeded_i = 0;
        if (cetcd_parse_txn_succeeded(resp, (size_t)rlen, &succeeded_i)
            != CETCD_OK)
            succeeded_i = 0;
        bool succeeded = succeeded_i != 0;
        size_t rpos = 0;
        while (rpos < (size_t)rlen) {
            uint8_t tag = resp[rpos++];
            if (tag == 0x10) {
                if (cetcd_leftover_safe_skip_field(resp, (size_t)rlen, &rpos,
                                                   tag) != CETCD_OK)
                    break;
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
                        if (cetcd_leftover_safe_skip_field(resp, op_end, &rpos,
                                                           sub_tag) != CETCD_OK)
                            break;
                    } else if (sub_tag == 0x00) {
                        continue;
                    } else if (cetcd_leftover_safe_skip_field(resp, op_end,
                                                              &rpos, sub_tag)
                               != CETCD_OK) {
                        break;
                    }
                }
                rpos = op_end;
            } else if (tag == 0x00) {
                continue;
            } else if (cetcd_leftover_safe_skip_field(resp, (size_t)rlen, &rpos,
                                                      tag) != CETCD_OK) {
                break;
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
        int wr = 0, sk = 0, wj = 0, wt = 0, wf = 0;
        if ((wr = take_write_out_jf_(&i, argc, argv, &want_json, &want_fields)) != 0) {
            if (wr < 0) { fprintf(stderr, "--write-out requires a format\n"); return 1; }
        }
    }
    if (cetcd_ctl_parse_maint_argv(argc, argv, 2, 0, NULL) != CETCD_OK) {
        print_unknown_maint_flag_(argc, argv, 2, 0);
        return 1;
    }
    uint8_t req[] = {0x00}, resp[1024];
    int rlen = do_rpc("/etcdserverpb.Maintenance/Status", req, 1, resp, sizeof(resp));
    if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
    char leftover_err[32];
    leftover_err[0] = '\0';
    /* leftover-safe: leftover cannot steal a printed alarm error */
    if (cetcd_parse_status_errors(resp, (size_t)rlen, leftover_err,
                                  sizeof(leftover_err)) != CETCD_OK) {
        fprintf(stderr, "request failed\n");
        return 1;
    }
    uint64_t leftover_inuse = 0;
    /* leftover-safe: leftover cannot steal a printed dbSizeInUse */
    if (cetcd_parse_status_db_size_in_use(resp, (size_t)rlen, &leftover_inuse)
        != CETCD_OK) {
        fprintf(stderr, "request failed\n");
        return 1;
    }
    uint64_t leftover_leader = 0;
    /* leftover-safe: leftover cannot steal a printed leader */
    if (cetcd_parse_status_leader(resp, (size_t)rlen, &leftover_leader)
        != CETCD_OK) {
        fprintf(stderr, "request failed\n");
        return 1;
    }
    uint64_t leftover_ridx = 0;
    /* leftover-safe: leftover cannot steal a printed raftIndex */
    if (cetcd_parse_status_raft_index(resp, (size_t)rlen, &leftover_ridx)
        != CETCD_OK) {
        fprintf(stderr, "request failed\n");
        return 1;
    }
    uint64_t leftover_rterm = 0;
    /* leftover-safe: leftover cannot steal a printed raftTerm */
    if (cetcd_parse_status_raft_term(resp, (size_t)rlen, &leftover_rterm)
        != CETCD_OK) {
        fprintf(stderr, "request failed\n");
        return 1;
    }
    uint64_t leftover_rapplied = 0;
    /* leftover-safe: leftover cannot steal a printed raftAppliedIndex */
    if (cetcd_parse_status_raft_applied(resp, (size_t)rlen, &leftover_rapplied)
        != CETCD_OK) {
        fprintf(stderr, "request failed\n");
        return 1;
    }
    uint64_t leftover_dbsize = 0;
    /* leftover-safe: leftover cannot steal a printed dbSize */
    if (cetcd_parse_status_db_size(resp, (size_t)rlen, &leftover_dbsize)
        != CETCD_OK) {
        fprintf(stderr, "request failed\n");
        return 1;
    }
    char leftover_ver[32];
    leftover_ver[0] = '\0';
    /* leftover-safe: leftover cannot steal a printed version */
    if (cetcd_parse_status_version(resp, (size_t)rlen, leftover_ver,
                                   sizeof(leftover_ver)) != CETCD_OK) {
        fprintf(stderr, "request failed\n");
        return 1;
    }
    int leftover_learner = 0;
    /* leftover-safe: leftover cannot steal a printed isLearner */
    if (cetcd_parse_status_is_learner(resp, (size_t)rlen, &leftover_learner)
        != CETCD_OK) {
        fprintf(stderr, "request failed\n");
        return 1;
    }
    /* Parse StatusResponse */
    size_t pos = 0;
    const uint8_t *version = (const uint8_t *)leftover_ver;
    size_t version_len = strlen(leftover_ver);
    uint64_t db_size = leftover_dbsize, leader = leftover_leader, raft_index = leftover_ridx, raft_term = leftover_rterm, revision = 0;
    uint64_t raft_applied = leftover_rapplied, db_inuse = leftover_inuse, is_learner = leftover_learner;
    while (pos < (size_t)rlen) {
        uint8_t tag = resp[pos++];
        if (tag == 0x12) {
            /* leftover-safe: leftover cannot steal a printed version */
            if (cetcd_leftover_safe_skip_field(resp, (size_t)rlen, &pos, tag)
                != CETCD_OK) {
                break;
            }
        } else if (tag == 0x18) {
            /* leftover-safe: leftover cannot steal a printed dbSize */
            if (cetcd_leftover_safe_skip_field(resp, (size_t)rlen, &pos, tag)
                != CETCD_OK) {
                break;
            }
        } else if (tag == 0x20) {
            /* leftover-safe: leftover cannot steal a printed leader */
            if (cetcd_leftover_safe_skip_field(resp, (size_t)rlen, &pos, tag)
                != CETCD_OK) {
                break;
            }
        } else if (tag == 0x28) {
            /* leftover-safe: leftover cannot steal a printed raftIndex */
            if (cetcd_leftover_safe_skip_field(resp, (size_t)rlen, &pos, tag)
                != CETCD_OK) {
                break;
            }
        } else if (tag == 0x30) {
            /* leftover-safe: leftover cannot steal a printed raftTerm */
            if (cetcd_leftover_safe_skip_field(resp, (size_t)rlen, &pos, tag)
                != CETCD_OK) {
                break;
            }
        } else if (tag == 0x38) {
            /* leftover-safe: leftover cannot steal a printed raftAppliedIndex */
            if (cetcd_leftover_safe_skip_field(resp, (size_t)rlen, &pos, tag)
                != CETCD_OK) {
                break;
            }
        } else if (tag == 0x42) {
            /* leftover-safe: leftover cannot steal a printed alarm error */
            if (cetcd_leftover_safe_skip_field(resp, (size_t)rlen, &pos, tag)
                != CETCD_OK) {
                break;
            }
        } else if (tag == 0x48) {
            /* leftover-safe: leftover cannot steal a printed dbSizeInUse */
            if (cetcd_leftover_safe_skip_field(resp, (size_t)rlen, &pos, tag)
                != CETCD_OK) {
                break;
            }
        } else if (tag == 0x50) {
            /* leftover-safe: leftover cannot steal a printed isLearner */
            if (cetcd_leftover_safe_skip_field(resp, (size_t)rlen, &pos, tag)
                != CETCD_OK) {
                break;
            }
        } else if (tag == 0x0a) {
            /* ResponseHeader: field 1 (cluster_id), field 2 (member_id), field 3 (revision) */
            uint64_t l = 0; read_varint(resp, rlen, &pos, &l);
            size_t hdr_end = pos + (size_t)l;
            while (pos < hdr_end) {
                uint8_t htag = resp[pos++];
                if (htag == 0x18) { read_varint(resp, hdr_end, &pos, &revision); }
                else if (htag == 0x00) { continue; }
                else if (cetcd_leftover_safe_skip_field(resp, hdr_end, &pos,
                                                        htag) != CETCD_OK) {
                    break;
                }
            }
        } else if (tag == 0x00) {
            continue;
        } else if (cetcd_leftover_safe_skip_field(resp, (size_t)rlen, &pos, tag)
                   != CETCD_OK) {
            break;
        }
    }
    if (want_fields) {
        printf("version: ");
        if (version) fwrite(version, 1, version_len, stdout);
        printf("\n");
        printf("dbSize: %llu\n", (unsigned long long)db_size);
        printf("dbSizeInUse: %llu\n", (unsigned long long)db_inuse);
        printf("leader: %llu\n", (unsigned long long)leader);
        printf("raftIndex: %llu\n", (unsigned long long)raft_index);
        printf("raftTerm: %llu\n", (unsigned long long)raft_term);
        printf("raftAppliedIndex: %llu\n", (unsigned long long)raft_applied);
        printf("revision: %llu\n", (unsigned long long)revision);
        if (is_learner) printf("isLearner: true\n");
        if (leftover_err[0]) printf("errors: %s\n", leftover_err);
    } else if (want_json) {
        fputs("{", stdout);
        parse_and_print_header_json(resp, (size_t)rlen);
        fputs(",\"version\":", stdout);
        if (version) print_json_string(version, version_len); else fputs("\"\"", stdout);
        fputs(",", stdout);
        printf("\"dbSize\":%llu,", (unsigned long long)db_size);
        printf("\"dbSizeInUse\":%llu,", (unsigned long long)db_inuse);
        printf("\"leader\":%llu,", (unsigned long long)leader);
        printf("\"raftIndex\":%llu,", (unsigned long long)raft_index);
        printf("\"raftTerm\":%llu,", (unsigned long long)raft_term);
        printf("\"raftAppliedIndex\":%llu,", (unsigned long long)raft_applied);
        printf("\"revision\":%llu", (unsigned long long)revision);
        if (is_learner) fputs(",\"isLearner\":true", stdout);
        if (leftover_err[0]) {
            fputs(",\"errors\":[", stdout);
            print_json_string((const uint8_t *)leftover_err, strlen(leftover_err));
            fputs("]", stdout);
        }
        fputs("}\n", stdout);
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
    uint8_t mreq[8], mresp[4096];
    size_t mn = 0;
    /* etcdctl --cluster MemberList is linearizable (same default as member list). */
    if (cetcd_encode_member_list_request(1, mreq, sizeof(mreq), &mn) != CETCD_OK)
        return -1;
    int mrlen = do_rpc("/etcdserverpb.Cluster/MemberList", mreq, mn, mresp, sizeof(mresp));
    if (mrlen < 0) return -1;
    {
        char leftover_curl[256];
        char leftover_name[128];
        leftover_curl[0] = '\0';
        leftover_name[0] = '\0';
        /* leftover-safe: leftover cannot steal a used --cluster URL or name */
        if (cetcd_parse_member_list_client_url(mresp, (size_t)mrlen,
                                               leftover_curl,
                                               sizeof(leftover_curl))
            != CETCD_OK)
            return -1;
        if (cetcd_parse_member_list_name(mresp, (size_t)mrlen, leftover_name,
                                         sizeof(leftover_name)) != CETCD_OK)
            return -1;
        int leftover_learner = 0;
        /* leftover-safe: leftover cannot steal a used --cluster isLearner */
        if (cetcd_parse_member_list_is_learner(mresp, (size_t)mrlen,
                                               &leftover_learner) != CETCD_OK)
            return -1;
        (void)leftover_learner;
    }
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
                } else if (mtag == 0x00) {
                    continue;
                } else if (cetcd_leftover_safe_skip_field(mresp, mend, &mpos,
                                                          mtag) != CETCD_OK) {
                    return -1;
                }
            }
            mpos = mend;
            if (curl && curlen > 0) {
                if (cetcd_parse_host_port((const char *)curl, curlen,
                                          eps[count].host, sizeof(eps[count].host),
                                          &eps[count].port, 2379) != CETCD_OK)
                    return -1;
                count++;
            }
        } else if (tag == 0x0a) {
            uint64_t l = 0; read_varint(mresp, mrlen, &mpos, &l);
            mpos += l;
        } else if (tag == 0x00) {
            continue;
        } else if (cetcd_leftover_safe_skip_field(mresp, (size_t)mrlen, &mpos,
                                                  tag) != CETCD_OK) {
            return -1;
        }
    }
    return count;
}

static int cmd_endpoint(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "usage: cetcdctl endpoint {health,status,hashkv} [--cluster] [--rev N]\n");
        return 1;
    }
    int want_json = 0;
    int want_table = 0;
    int want_fields = 0;
    int cluster = 0;
    int64_t hashkv_rev = 0;
    for (int i = 3; i < argc; i++) {
        int wr = 0, on = 1;
        if ((wr = take_write_out_jtf_(&i, argc, argv, &want_json, &want_table, &want_fields)) != 0) {
            if (wr < 0) { fprintf(stderr, "--write-out requires a format\n"); return 1; }
        } else if (cmd_flag_is_(argv[i], "--cluster")) {
            if (cetcd_take_cli_bool_eq(&i, argc, argv, &on) != CETCD_OK) {
                fprintf(stderr, "--cluster must be true or false\n");
                return 1;
            }
            cluster = on;
        } else if (strcmp(argv[2], "hashkv") == 0 && cmd_flag_is_(argv[i], "--rev")) {
            if (take_hashkv_rev_(&i, argc, argv, &hashkv_rev) != 0) {
                fprintf(stderr, "--rev must be >= 0\n");
                return 1;
            }
        } else {
            fprintf(stderr, "unknown flag: %s\n", argv[i]);
            return 1;
        }
    }
    if (strcmp(argv[2], "health") == 0) {
        if (want_table) {
            printf("+----------------------+--------+-----------+--------------------+\n");
            printf("|      ENDPOINT        | HEALTH |   TOOK    |       ERROR        |\n");
            printf("+----------------------+--------+-----------+--------------------+\n");
        }
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
                        printf("{\"endpoint\":\"%s\",\"status\":\"unhealthy\",\"took\":\"%.3fms\",\"error\":\"failed to connect\"}\n", ep_str_(), took_ms);
                    } else if (want_fields) {
                        printf("endpoint: %s\n", ep_str_());
                        printf("status: unhealthy\n");
                        printf("took: %.3fms\n", took_ms);
                        printf("error: failed to connect\n\n");
                    } else if (want_table) {
                        char ep_addr[288]; snprintf(ep_addr, sizeof(ep_addr), "%s", ep_str_());
                        printf("| %-20s | %-6s | %7.1fms | %-18s |\n", ep_addr, "false", took_ms, "failed to connect");
                    } else {
                        printf("%s is unhealthy: failed to connect\n", ep_str_());
                    }
                } else {
                    if (want_json) {
                        fputs("{\"endpoint\":\"", stdout);
                        printf("%s\",", ep_str_());
                        parse_and_print_header_json(hresp, (size_t)hrlen);
                        printf(",\"status\":\"healthy\",\"took\":\"%.3fms\"}\n", took_ms);
                    } else if (want_fields) {
                        printf("endpoint: %s\n", ep_str_());
                        parse_and_print_header_json(hresp, (size_t)hrlen);
                        printf("status: healthy\n");
                        printf("took: %.3fms\n\n", took_ms);
                    } else if (want_table) {
                        char ep_addr2[288]; snprintf(ep_addr2, sizeof(ep_addr2), "%s", ep_str_());
                        printf("| %-20s | %-6s | %7.1fms | %-18s |\n", ep_addr2, "true", took_ms, "");
                    } else {
                        printf("%s is healthy (%.3fms)\n", ep_str_(), took_ms);
                    }
                }
            }
            if (want_table) {
                printf("+----------------------+--------+-----------+--------------------+\n");
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
                printf("{\"endpoint\":\"%s\",\"status\":\"unhealthy\",\"took\":\"%.3fms\",\"error\":\"failed to connect\"}\n", ep_str_(), took_ms);
            } else if (want_fields) {
                printf("endpoint: %s\n", ep_str_());
                printf("status: unhealthy\n");
                printf("took: %.3fms\n", took_ms);
                printf("error: failed to connect\n");
                fputs("\n", stdout);
            } else if (want_table) {
                char ep_addr[288]; snprintf(ep_addr, sizeof(ep_addr), "%s", ep_str_());
                printf("| %-20s | %-6s | %7.1fms | %-18s |\n", ep_addr, "false", took_ms, "failed to connect");
                printf("+----------------------+--------+-----------+--------------------+\n");
            } else {
                printf("%s is unhealthy: failed to connect\n", ep_str_());
            }
            return 1;
        }
        if (want_json) {
            fputs("{\"endpoint\":\"", stdout);
            printf("%s\",", ep_str_());
            parse_and_print_header_json(resp, (size_t)rlen);
            printf(",\"status\":\"healthy\",\"took\":\"%.3fms\"}\n", took_ms);
        } else if (want_fields) {
            printf("endpoint: %s\n", ep_str_());
            parse_and_print_header_json(resp, (size_t)rlen);
            printf("status: healthy\n");
            printf("took: %.3fms\n", took_ms);
            fputs("\n", stdout);
        } else if (want_table) {
            char ep_addr[288]; snprintf(ep_addr, sizeof(ep_addr), "%s", ep_str_());
            printf("| %-20s | %-6s | %7.1fms | %-18s |\n", ep_addr, "true", took_ms, "");
            printf("+----------------------+--------+-----------+--------------------+\n");
        } else {
            printf("%s is healthy (%.3fms)\n", ep_str_(), took_ms);
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
                char leftover_err[32];
                leftover_err[0] = '\0';
                /* leftover-safe: leftover cannot steal a printed alarm error */
                if (cetcd_parse_status_errors(sresp, (size_t)srlen, leftover_err,
                                              sizeof(leftover_err)) != CETCD_OK)
                    continue;
                uint64_t leftover_inuse = 0;
                /* leftover-safe: leftover cannot steal a printed dbSizeInUse */
                if (cetcd_parse_status_db_size_in_use(sresp, (size_t)srlen,
                                                      &leftover_inuse)
                    != CETCD_OK)
                    continue;
                uint64_t leftover_leader = 0;
                /* leftover-safe: leftover cannot steal a printed leader */
                if (cetcd_parse_status_leader(sresp, (size_t)srlen,
                                              &leftover_leader) != CETCD_OK)
                    continue;
                uint64_t leftover_ridx = 0;
                /* leftover-safe: leftover cannot steal a printed raftIndex */
                if (cetcd_parse_status_raft_index(sresp, (size_t)srlen,
                                                  &leftover_ridx) != CETCD_OK)
                    continue;
                uint64_t leftover_rterm = 0;
                /* leftover-safe: leftover cannot steal a printed raftTerm */
                if (cetcd_parse_status_raft_term(sresp, (size_t)srlen,
                                                 &leftover_rterm) != CETCD_OK)
                    continue;
                uint64_t leftover_rapplied = 0;
                /* leftover-safe: leftover cannot steal a printed raftAppliedIndex */
                if (cetcd_parse_status_raft_applied(sresp, (size_t)srlen,
                                                    &leftover_rapplied)
                    != CETCD_OK)
                    continue;
                (void)leftover_rapplied;
                uint64_t leftover_dbsize = 0;
                /* leftover-safe: leftover cannot steal a printed dbSize */
                if (cetcd_parse_status_db_size(sresp, (size_t)srlen,
                                               &leftover_dbsize) != CETCD_OK)
                    continue;
                char leftover_ver[32];
                leftover_ver[0] = '\0';
                /* leftover-safe: leftover cannot steal a printed version */
                if (cetcd_parse_status_version(sresp, (size_t)srlen, leftover_ver,
                                               sizeof(leftover_ver)) != CETCD_OK)
                    continue;
                int leftover_learner = 0;
                /* leftover-safe: leftover cannot steal a printed isLearner */
                if (cetcd_parse_status_is_learner(sresp, (size_t)srlen,
                                                  &leftover_learner) != CETCD_OK)
                    continue;
                (void)leftover_learner;
                size_t pos = 0;
                const uint8_t *ver = (const uint8_t *)leftover_ver;
                size_t ver_len = strlen(leftover_ver);
                uint64_t db_size = leftover_dbsize, leader = leftover_leader, raft_index = leftover_ridx, raft_term = leftover_rterm, revision = 0;
                uint64_t db_inuse = leftover_inuse;
                while (pos < (size_t)srlen) {
                    uint8_t tag = sresp[pos++];
                    if (tag == 0x12) {
                        /* leftover-safe: leftover cannot steal a printed version */
                        if (cetcd_leftover_safe_skip_field(sresp, (size_t)srlen,
                                                            &pos, tag)
                            != CETCD_OK) {
                            break;
                        }
                    } else if (tag == 0x18) {
                        /* leftover-safe: leftover cannot steal a printed dbSize */
                        if (cetcd_leftover_safe_skip_field(sresp, (size_t)srlen,
                                                            &pos, tag)
                            != CETCD_OK) {
                            break;
                        }
                    } else if (tag == 0x20) {
                        /* leftover-safe: leftover cannot steal a printed leader */
                        if (cetcd_leftover_safe_skip_field(sresp, (size_t)srlen,
                                                            &pos, tag)
                            != CETCD_OK) {
                            break;
                        }
                    } else if (tag == 0x28) {
                        /* leftover-safe: leftover cannot steal a printed raftIndex */
                        if (cetcd_leftover_safe_skip_field(sresp, (size_t)srlen,
                                                            &pos, tag)
                            != CETCD_OK) {
                            break;
                        }
                    } else if (tag == 0x30) {
                        /* leftover-safe: leftover cannot steal a printed raftTerm */
                        if (cetcd_leftover_safe_skip_field(sresp, (size_t)srlen,
                                                            &pos, tag)
                            != CETCD_OK) {
                            break;
                        }
                    } else if (tag == 0x42) {
                        /* leftover-safe: leftover cannot steal a printed alarm error */
                        if (cetcd_leftover_safe_skip_field(sresp, (size_t)srlen,
                                                            &pos, tag)
                            != CETCD_OK) {
                            break;
                        }
                    } else if (tag == 0x48) {
                        /* leftover-safe: leftover cannot steal a printed dbSizeInUse */
                        if (cetcd_leftover_safe_skip_field(sresp, (size_t)srlen,
                                                            &pos, tag)
                            != CETCD_OK) {
                            break;
                        }
                    } else if (tag == 0x0a) {
                        uint64_t l = 0; read_varint(sresp, srlen, &pos, &l);
                        size_t hdr_end = pos + (size_t)l;
                        while (pos < hdr_end) {
                            uint8_t htag = sresp[pos++];
                            if (htag == 0x18) { read_varint(sresp, hdr_end, &pos, &revision); }
                            else if (htag == 0x00) { continue; }
                            else if (cetcd_leftover_safe_skip_field(sresp, hdr_end,
                                                                    &pos, htag)
                                     != CETCD_OK) {
                                break;
                            }
                        }
                    } else if (tag == 0x00) {
                        continue;
                    } else if (cetcd_leftover_safe_skip_field(sresp, (size_t)srlen,
                                                              &pos, tag)
                               != CETCD_OK) {
                        break;
                    }
                }
                if (want_json) {
                    fputs("{\"endpoint\":\"", stdout);
                    printf("%s\",", ep_str_());
                    parse_and_print_header_json(sresp, (size_t)srlen);
                    fputs(",\"version\":", stdout);
                    if (ver) print_json_string(ver, ver_len); else fputs("\"\"", stdout);
                    fputs(",", stdout);
                    printf("\"dbSize\":%llu,\"dbSizeInUse\":%llu,\"leader\":%llu,\"raftIndex\":%llu,\"raftTerm\":%llu,\"revision\":%llu",
                           (unsigned long long)db_size, (unsigned long long)db_inuse,
                           (unsigned long long)leader,
                           (unsigned long long)raft_index, (unsigned long long)raft_term,
                           (unsigned long long)revision);
                    if (leftover_err[0]) {
                        fputs(",\"errors\":[", stdout);
                        print_json_string((const uint8_t *)leftover_err,
                                          strlen(leftover_err));
                        fputs("]", stdout);
                    }
                    fputs("}\n", stdout);
                } else if (want_table) {
                    printf("| %-24s | %14llu | %9llu | %9llu |\n",
                           ep_str_(), (unsigned long long)leader, (unsigned long long)revision,
                           (unsigned long long)db_size);
                } else if (want_fields) {
                    printf("endpoint: %s\n", ep_str_());
                    printf("ID: %llu\n", (unsigned long long)leader);
                    printf("revision: %llu\n", (unsigned long long)revision);
                    printf("dbSize: %llu\n", (unsigned long long)db_size);
                    printf("dbSizeInUse: %llu\n", (unsigned long long)db_inuse);
                    printf("raftIndex: %llu\n", (unsigned long long)raft_index);
                    printf("raftTerm: %llu\n", (unsigned long long)raft_term);
                    if (ver_len) printf("version: %.*s\n", (int)ver_len, ver);
                    if (leftover_err[0]) printf("errors: %s\n", leftover_err);
                    printf("\n");
                } else {
                    printf("endpoint: %s  revision: %llu  db_size: %llu",
                           ep_str_(), (unsigned long long)revision, (unsigned long long)db_size);
                    if (leftover_err[0])
                        printf("  error: %s", leftover_err);
                    printf("\n");
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
        char leftover_err[32];
        leftover_err[0] = '\0';
        /* leftover-safe: leftover cannot steal a printed alarm error */
        if (cetcd_parse_status_errors(resp, (size_t)rlen, leftover_err,
                                      sizeof(leftover_err)) != CETCD_OK) {
            fprintf(stderr, "request failed\n");
            return 1;
        }
        uint64_t leftover_inuse = 0;
        /* leftover-safe: leftover cannot steal a printed dbSizeInUse */
        if (cetcd_parse_status_db_size_in_use(resp, (size_t)rlen, &leftover_inuse)
            != CETCD_OK) {
            fprintf(stderr, "request failed\n");
            return 1;
        }
        uint64_t leftover_leader = 0;
        /* leftover-safe: leftover cannot steal a printed leader */
        if (cetcd_parse_status_leader(resp, (size_t)rlen, &leftover_leader)
            != CETCD_OK) {
            fprintf(stderr, "request failed\n");
            return 1;
        }
        uint64_t leftover_ridx = 0;
        /* leftover-safe: leftover cannot steal a printed raftIndex */
        if (cetcd_parse_status_raft_index(resp, (size_t)rlen, &leftover_ridx)
            != CETCD_OK) {
            fprintf(stderr, "request failed\n");
            return 1;
        }
        uint64_t leftover_rterm = 0;
        /* leftover-safe: leftover cannot steal a printed raftTerm */
        if (cetcd_parse_status_raft_term(resp, (size_t)rlen, &leftover_rterm)
            != CETCD_OK) {
            fprintf(stderr, "request failed\n");
            return 1;
        }
        uint64_t leftover_rapplied = 0;
        /* leftover-safe: leftover cannot steal a printed raftAppliedIndex */
        if (cetcd_parse_status_raft_applied(resp, (size_t)rlen, &leftover_rapplied)
            != CETCD_OK) {
            fprintf(stderr, "request failed\n");
            return 1;
        }
        (void)leftover_rapplied;
        uint64_t leftover_dbsize = 0;
        /* leftover-safe: leftover cannot steal a printed dbSize */
        if (cetcd_parse_status_db_size(resp, (size_t)rlen, &leftover_dbsize)
            != CETCD_OK) {
            fprintf(stderr, "request failed\n");
            return 1;
        }
        char leftover_ver[32];
        leftover_ver[0] = '\0';
        /* leftover-safe: leftover cannot steal a printed version */
        if (cetcd_parse_status_version(resp, (size_t)rlen, leftover_ver,
                                       sizeof(leftover_ver)) != CETCD_OK) {
            fprintf(stderr, "request failed\n");
            return 1;
        }
        int leftover_learner = 0;
        /* leftover-safe: leftover cannot steal a printed isLearner */
        if (cetcd_parse_status_is_learner(resp, (size_t)rlen, &leftover_learner)
            != CETCD_OK) {
            fprintf(stderr, "request failed\n");
            return 1;
        }
        (void)leftover_learner;
        size_t pos = 0;
        const uint8_t *ver = (const uint8_t *)leftover_ver;
        size_t ver_len = strlen(leftover_ver);
        uint64_t db_size = leftover_dbsize, leader = leftover_leader, raft_index = leftover_ridx, raft_term = leftover_rterm, revision = 0;
        uint64_t db_inuse = leftover_inuse;
        while (pos < (size_t)rlen) {
            uint8_t tag = resp[pos++];
            if (tag == 0x12) {
                /* leftover-safe: leftover cannot steal a printed version */
                if (cetcd_leftover_safe_skip_field(resp, (size_t)rlen, &pos, tag)
                    != CETCD_OK) {
                    break;
                }
            } else if (tag == 0x18) {
                /* leftover-safe: leftover cannot steal a printed dbSize */
                if (cetcd_leftover_safe_skip_field(resp, (size_t)rlen, &pos, tag)
                    != CETCD_OK) {
                    break;
                }
            } else if (tag == 0x20) {
                /* leftover-safe: leftover cannot steal a printed leader */
                if (cetcd_leftover_safe_skip_field(resp, (size_t)rlen, &pos, tag)
                    != CETCD_OK) {
                    break;
                }
            } else if (tag == 0x28) {
                /* leftover-safe: leftover cannot steal a printed raftIndex */
                if (cetcd_leftover_safe_skip_field(resp, (size_t)rlen, &pos, tag)
                    != CETCD_OK) {
                    break;
                }
            } else if (tag == 0x30) {
                /* leftover-safe: leftover cannot steal a printed raftTerm */
                if (cetcd_leftover_safe_skip_field(resp, (size_t)rlen, &pos, tag)
                    != CETCD_OK) {
                    break;
                }
            } else if (tag == 0x42) {
                /* leftover-safe: leftover cannot steal a printed alarm error */
                if (cetcd_leftover_safe_skip_field(resp, (size_t)rlen, &pos, tag)
                    != CETCD_OK) {
                    break;
                }
            } else if (tag == 0x48) {
                /* leftover-safe: leftover cannot steal a printed dbSizeInUse */
                if (cetcd_leftover_safe_skip_field(resp, (size_t)rlen, &pos, tag)
                    != CETCD_OK) {
                    break;
                }
            } else if (tag == 0x0a) {
                uint64_t l = 0; read_varint(resp, rlen, &pos, &l);
                size_t hdr_end = pos + (size_t)l;
                while (pos < hdr_end) {
                    uint8_t htag = resp[pos++];
                    if (htag == 0x18) { read_varint(resp, hdr_end, &pos, &revision); }
                    else if (htag == 0x00) { continue; }
                    else if (cetcd_leftover_safe_skip_field(resp, hdr_end, &pos,
                                                            htag) != CETCD_OK) {
                        break;
                    }
                }
            } else if (tag == 0x00) {
                continue;
            } else if (cetcd_leftover_safe_skip_field(resp, (size_t)rlen, &pos,
                                                      tag) != CETCD_OK) {
                break;
            }
        }
        if (want_json) {
            fputs("{\"endpoint\":\"", stdout);
            printf("%s\",", ep_str_());
            parse_and_print_header_json(resp, (size_t)rlen);
            fputs(",\"version\":", stdout);
            if (ver) print_json_string(ver, ver_len); else fputs("\"\"", stdout);
            fputs(",", stdout);
            printf("\"dbSize\":%llu,\"dbSizeInUse\":%llu,\"leader\":%llu,\"raftIndex\":%llu,\"raftTerm\":%llu,\"revision\":%llu",
                   (unsigned long long)db_size, (unsigned long long)db_inuse,
                   (unsigned long long)leader,
                   (unsigned long long)raft_index, (unsigned long long)raft_term,
                   (unsigned long long)revision);
            if (leftover_err[0]) {
                fputs(",\"errors\":[", stdout);
                print_json_string((const uint8_t *)leftover_err, strlen(leftover_err));
                fputs("]", stdout);
            }
            fputs("}\n", stdout);
        } else if (want_table) {
            printf("+--------------------------+----------------+-----------+-----------+\n");
            printf("|         ENDPOINT         |      ID        |  REVISION | DB SIZE   |\n");
            printf("+--------------------------+----------------+-----------+-----------+\n");
            printf("| %-24s | %14llu | %9llu | %9llu |\n",
                   ep_str_(), (unsigned long long)leader, (unsigned long long)revision,
                   (unsigned long long)db_size);
            printf("+--------------------------+----------------+-----------+-----------+\n");
        } else if (want_fields) {
            printf("endpoint: %s\n", ep_str_());
            printf("ID: %llu\n", (unsigned long long)leader);
            printf("revision: %llu\n", (unsigned long long)revision);
            printf("dbSize: %llu\n", (unsigned long long)db_size);
            printf("dbSizeInUse: %llu\n", (unsigned long long)db_inuse);
            printf("raftIndex: %llu\n", (unsigned long long)raft_index);
            printf("raftTerm: %llu\n", (unsigned long long)raft_term);
            if (ver_len) printf("version: %.*s\n", (int)ver_len, ver);
            if (leftover_err[0]) printf("errors: %s\n", leftover_err);
            printf("\n");
        } else {
            parse_status_response(resp, rlen);
        }
        return 0;
    } else if (strcmp(argv[2], "hashkv") == 0) {
        if (cluster) {
            if (want_table) {
                printf("+----------------------+------------------+--------------------+\n");
                printf("|      ENDPOINT        |       HASH       |  COMPACT_REV       |\n");
                printf("+----------------------+------------------+--------------------+\n");
            }
            struct cluster_endpoint eps[32];
            int n = collect_cluster_endpoints(eps, 32);
            if (n < 0) { fprintf(stderr, "failed to get member list\n"); return 1; }
            const char *orig_host = g_host;
            uint16_t orig_port = g_port;
            for (int i = 0; i < n; i++) {
                g_host = eps[i].host;
                g_port = eps[i].port;
                uint8_t hreq[16], hresp[256];
                size_t hn = 0;
                if (cetcd_encode_hashkv_request(hashkv_rev, hreq, sizeof(hreq),
                                                &hn) != CETCD_OK)
                    return 1;
                int hrlen = do_rpc("/etcdserverpb.Maintenance/HashKV", hreq, hn,
                                   hresp, sizeof(hresp));
                if (hrlen < 0) continue;
                uint32_t hash32 = 0;
                int64_t compact_i = 0;
                uint64_t hash_val = 0, compact_rev = 0;
                if (cetcd_parse_hash_response(hresp, (size_t)hrlen,
                                             &hash32, &compact_i)
                    != CETCD_OK)
                    continue;
                hash_val = hash32;
                compact_rev = (uint64_t)compact_i;
                if (want_json) {
                    fputs("{\"endpoint\":\"", stdout);
                    printf("%s\",", ep_str_());
                    parse_and_print_header_json(hresp, (size_t)hrlen);
                    printf(",\"hash\":%llu,\"compact_revision\":%llu}\n",
                           (unsigned long long)hash_val, (unsigned long long)compact_rev);
                } else if (want_fields) {
                    printf("endpoint: %s\n", ep_str_());
                    parse_and_print_header_json(hresp, (size_t)hrlen);
                    printf("hash: %llu\n", (unsigned long long)hash_val);
                    printf("compact_revision: %llu\n\n", (unsigned long long)compact_rev);
                } else if (want_table) {
                    char ep_addr[288]; snprintf(ep_addr, sizeof(ep_addr), "%s", ep_str_());
                    printf("| %-20s | %016llx | %18llu |\n", ep_addr, (unsigned long long)hash_val, (unsigned long long)compact_rev);
                } else {
                    printf("endpoint: %s  hash: %llu  compact_revision: %llu\n",
                           ep_str_(), (unsigned long long)hash_val, (unsigned long long)compact_rev);
                }
            }
            if (want_table) {
                printf("+----------------------+------------------+--------------------+\n");
            }
            g_host = orig_host;
            g_port = orig_port;
            return 0;
        }
        /* Non-cluster endpoint hashkv */
        uint8_t req[16], resp[256];
        size_t req_n = 0;
        if (cetcd_encode_hashkv_request(hashkv_rev, req, sizeof(req),
                                        &req_n) != CETCD_OK)
            return 1;
        int rlen = do_rpc("/etcdserverpb.Maintenance/HashKV", req, req_n, resp,
                          sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        uint32_t hash32 = 0;
        int64_t compact_i = 0;
        if (cetcd_parse_hash_response(resp, (size_t)rlen, &hash32, &compact_i)
            != CETCD_OK) {
            fprintf(stderr, "request failed\n");
            return 1;
        }
        uint64_t hash_val = hash32;
        uint64_t compact_rev = (uint64_t)compact_i;
        if (want_json) {
            fputs("{\"endpoint\":\"", stdout);
            printf("%s\",", ep_str_());
            parse_and_print_header_json(resp, (size_t)rlen);
            printf(",\"hash\":%llu,\"compact_revision\":%llu}\n",
                   (unsigned long long)hash_val, (unsigned long long)compact_rev);
        } else if (want_fields) {
            parse_and_print_header_json(resp, (size_t)rlen);
            printf("hash: %llu\n", (unsigned long long)hash_val);
            printf("compact_revision: %llu\n", (unsigned long long)compact_rev);
            fputs("\n", stdout);
        } else if (want_table) {
            printf("+----------------------+------------------+--------------------+\n");
            printf("|      ENDPOINT        |       HASH       |  COMPACT_REV       |\n");
            printf("+----------------------+------------------+--------------------+\n");
            char ep_addr[288]; snprintf(ep_addr, sizeof(ep_addr), "%s", ep_str_());
            printf("| %-20s | %016llx | %18llu |\n", ep_addr, (unsigned long long)hash_val, (unsigned long long)compact_rev);
            printf("+----------------------+------------------+--------------------+\n");
        } else {
            printf("endpoint: %s  hash: %llu  compact_revision: %llu\n",
                   ep_str_(), (unsigned long long)hash_val, (unsigned long long)compact_rev);
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
        if (cetcd_ctl_parse_check_argv(argc, argv, 3) != CETCD_OK) {
            fprintf(stderr, "unknown leftover flag (check perf --foo cannot start a write load)\n");
            return 1;
        }
        for (int i = 3; i < argc; i++) {
        int wr = 0, sk = 0, wj = 0, wt = 0, wf = 0;
            if ((wr = take_write_out_jf_(&i, argc, argv, &want_json, &want_fields)) != 0) {
                if (wr < 0) { fprintf(stderr, "--write-out requires a format\n"); return 1; }
            } else if (cmd_flag_is_(argv[i], "--load")) {
                const char *sz = NULL;
                if (take_cmd_value_(&i, argc, argv, &sz) != 0) {
                    fprintf(stderr, "--load must be s, m, or l\n");
                    return 1;
                }
                if (strcmp(sz, "s") == 0 || strcmp(sz, "S") == 0) load_count = 10;
                else if (strcmp(sz, "m") == 0 || strcmp(sz, "M") == 0) load_count = 100;
                else if (strcmp(sz, "l") == 0 || strcmp(sz, "L") == 0) load_count = 1000;
                else { fprintf(stderr, "--load must be s, m, or l\n"); return 1; }
            } else if (cmd_flag_is_(argv[i], "--prefix")) {
                if (take_cmd_value_(&i, argc, argv, &prefix) != 0) {
                    fprintf(stderr, "--prefix requires a value\n");
                    return 1;
                }
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
        if (cetcd_ctl_parse_check_argv(argc, argv, 3) != CETCD_OK) {
            fprintf(stderr, "unknown leftover flag (check datascale --foo cannot start a write load)\n");
            return 1;
        }
        for (int i = 3; i < argc; i++) {
        int wr = 0, sk = 0, wj = 0, wt = 0, wf = 0;
            if ((wr = take_write_out_jf_(&i, argc, argv, &want_json, &want_fields)) != 0) {
                if (wr < 0) { fprintf(stderr, "--write-out requires a format\n"); return 1; }
            } else if (cmd_flag_is_(argv[i], "--load")) {
                const char *s = NULL;
                int64_t v = 0;
                if (take_cmd_value_(&i, argc, argv, &s) != 0 ||
                    cetcd_parse_i64(s, &v) != CETCD_OK ||
                    v < 1 || v > 0x7fffffffLL) {
                    fprintf(stderr, "check datascale --load must be > 0\n");
                    return 1;
                }
                load_count = (int)v;
            } else if (cmd_flag_is_(argv[i], "--prefix")) {
                if (take_cmd_value_(&i, argc, argv, &prefix) != 0) {
                    fprintf(stderr, "--prefix requires a value\n");
                    return 1;
                }
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
                else if (t == 0x00) { continue; }
                else if (cetcd_leftover_safe_skip_field(st_resp, (size_t)st_rlen,
                                                        &sp, t) != CETCD_OK) {
                    break;
                }
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
        int on = 1, wj = 0, wf = 0, wr;
        if (cmd_flag_is_(argv[i], "--ttl")) {
            const char *s = NULL;
            int64_t v = 0;
            if (take_cmd_value_(&i, argc, argv, &s) != 0 ||
                cetcd_parse_i64(s, &v) != CETCD_OK ||
                v < 1 || v > 0x7fffffffLL) {
                fprintf(stderr, "--ttl must be > 0\n");
                return 1;
            }
            ttl = (int)v;
        } else if (cmd_flag_is_(argv[i], "--print-value-only")) {
            if (cetcd_take_cli_bool_eq(&i, argc, argv, &on) != CETCD_OK) {
                fprintf(stderr, "--print-value-only must be true or false\n");
                return 1;
            }
            print_value_only = on;
        } else if ((wr = take_write_out_jf_(&i, argc, argv, &wj, &wf)) != 0) {
            if (wr < 0) { fprintf(stderr, "--write-out requires a format\n"); return 1; }
            want_json = wj;
            want_fields = wf;
        } else if (argv[i][0] == '-') {
            fprintf(stderr, "unknown flag: %s\n", argv[i]);
            return 1;
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

    /* leftover-safe: leftover cannot steal a grant ID (field 2 0x10).
     * tag 0x08 is not the grant ID. leftover truncated error fail-closes. */
    int64_t lease_id = 0, grant_ttl = 0;
    char leftover_err[32];
    leftover_err[0] = '\0';
    if (cetcd_parse_lease_grant_response(grant_resp, (size_t)glen,
                                         &lease_id, &grant_ttl) != CETCD_OK
        || cetcd_parse_lease_grant_error(grant_resp, (size_t)glen, leftover_err,
                                         sizeof(leftover_err)) != CETCD_OK
        || lease_id == 0) {
        fprintf(stderr, "failed to get lease ID\n");
        return 1;
    }
    (void)grant_ttl;
    (void)leftover_err;

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

    /* leftover-safe: leftover cannot steal succeeded (false lock) */
    int succeeded_i = 0;
    if (cetcd_parse_txn_succeeded(resp, (size_t)rlen, &succeeded_i)
        != CETCD_OK)
        succeeded_i = 0;
    bool succeeded = succeeded_i != 0;

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
            int64_t ka_id = 0, ka_ttl = 0;
            if (cetcd_parse_lease_keepalive_response(ka_resp, (size_t)kl,
                                                     &ka_id, &ka_ttl)
                != CETCD_OK)
                _exit(0);
            unsigned new_ttl = ka_ttl > 0 ? (unsigned)ka_ttl : 0;
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
        int on = 1, wj = 0, wf = 0, wr;
        if (cmd_flag_is_(argv[i], "--ttl")) {
            const char *s = NULL;
            int64_t v = 0;
            if (take_cmd_value_(&i, argc, argv, &s) != 0 ||
                cetcd_parse_i64(s, &v) != CETCD_OK ||
                v < 1 || v > 0x7fffffffLL) {
                fprintf(stderr, "--ttl must be > 0\n");
                return 1;
            }
            ttl = (int)v;
        } else if (cmd_flag_is_(argv[i], "--print-value-only")) {
            if (cetcd_take_cli_bool_eq(&i, argc, argv, &on) != CETCD_OK) {
                fprintf(stderr, "--print-value-only must be true or false\n");
                return 1;
            }
            print_value_only = on;
        } else if ((wr = take_write_out_jf_(&i, argc, argv, &wj, &wf)) != 0) {
            if (wr < 0) { fprintf(stderr, "--write-out requires a format\n"); return 1; }
            want_json = wj;
            want_fields = wf;
        } else if (argv[i][0] == '-') {
            fprintf(stderr, "unknown flag: %s\n", argv[i]);
            return 1;
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

    int64_t lease_id = 0, grant_ttl = 0;
    char leftover_err[32];
    leftover_err[0] = '\0';
    if (cetcd_parse_lease_grant_response(grant_resp, (size_t)glen,
                                         &lease_id, &grant_ttl) != CETCD_OK
        || cetcd_parse_lease_grant_error(grant_resp, (size_t)glen, leftover_err,
                                         sizeof(leftover_err)) != CETCD_OK
        || lease_id == 0) {
        fprintf(stderr, "failed to get lease ID\n");
        return 1;
    }
    (void)grant_ttl;
    (void)leftover_err;

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

    int succeeded_i = 0;
    if (cetcd_parse_txn_succeeded(resp, (size_t)rlen, &succeeded_i)
        != CETCD_OK)
        succeeded_i = 0;
    bool succeeded = succeeded_i != 0;

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
            int64_t ka_id = 0, ka_ttl = 0;
            if (cetcd_parse_lease_keepalive_response(ka_resp, (size_t)kl,
                                                     &ka_id, &ka_ttl)
                != CETCD_OK)
                _exit(0);
            unsigned new_ttl = ka_ttl > 0 ? (unsigned)ka_ttl : 0;
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
        int wr = 0, sk = 0, wj = 0, wt = 0, wf = 0;
        if ((wr = take_write_out_jtf_(&i, argc, argv, &json_fmt, &table_fmt, &fields_fmt)) != 0) {
            if (wr < 0) { fprintf(stderr, "--write-out requires a format\n"); return 1; }
        } else if (argv[i][0] == '-') {
            fprintf(stderr, "unknown flag: %s\n", argv[i]);
            return 1;
        } else {
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

    /* leftover-safe AlarmResponse so leftover cannot steal type */
    cetcd_alarm_member alarms[8];
    size_t n_alarms = 0;
    if (cetcd_parse_alarm_response(resp, (size_t)rlen, alarms, 8, &n_alarms)
        != CETCD_OK) {
        fprintf(stderr, "request failed\n");
        return 1;
    }
    int found_alarms = n_alarms > 0;
    if (json_fmt && action == 0) {
        fputs("{", stdout);
        parse_and_print_header_json(resp, (size_t)rlen);
        fputs(",\"alarms\":[", stdout);
    }
    if (action == 0) {
        for (size_t i = 0; i < n_alarms; i++) {
            uint64_t member_id = alarms[i].member_id;
            int alarm_val = alarms[i].alarm;
            const char *type_str = (alarm_val == 0) ? "NONE" : (alarm_val == 1) ? "NOSPACE" : (alarm_val == 2) ? "CORRUPT" : "UNKNOWN";
            if (json_fmt) {
                if (i) printf(",");
                printf("{\"memberID\":%lu,\"alarm\":\"%s\"}", (unsigned long)member_id, type_str);
            } else if (fields_fmt) {
                printf("memberID: %lu\n", (unsigned long)member_id);
                printf("alarm: %s\n", type_str);
            } else if (table_fmt) {
                if (i == 0) {
                    printf("+----------------------+----------+\n");
                    printf("|       MEMBER         |  ALARM   |\n");
                    printf("+----------------------+----------+\n");
                }
                printf("| %20lu | %8s |\n", (unsigned long)member_id, type_str);
            } else {
                printf("memberID:%lu alarm:%s\n", (unsigned long)member_id, type_str);
            }
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
        int wr = 0, sk = 0, wj = 0, wt = 0, wf = 0;
        if ((wr = take_write_out_jf_(&i, argc, argv, &want_json, &want_fields)) != 0) {
            if (wr < 0) { fprintf(stderr, "--write-out requires a format\n"); return 1; }
        }
    }
    if (cetcd_ctl_parse_maint_argv(argc, argv, 2, 0, NULL) != CETCD_OK) {
        fprintf(stderr, "unknown leftover flag (version --foo cannot print version)\n");
        return 1;
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

static int restore_read_line_(const char *path, char *out, size_t cap) {
    FILE *f = fopen(path, "r");
    if (!f) return 1;
    if (!fgets(out, (int)cap, f)) {
        fclose(f);
        return -1;
    }
    fclose(f);
    size_t n = strlen(out);
    while (n > 0 && (out[n - 1] == '\n' || out[n - 1] == '\r'))
        out[--n] = '\0';
    return n == 0 ? -1 : 0;
}

static int restore_write_line_(const char *path, const char *text) {
    FILE *f = fopen(path, "w");
    if (!f) return -1;
    if (fprintf(f, "%s\n", text) < 0) {
        fclose(f);
        return -1;
    }
    return fclose(f) == 0 ? 0 : -1;
}

static int restore_check_mismatch_(const char *path, const char *want, int force,
                                   const char *mismatch_msg) {
    char got[2048];
    int rr = restore_read_line_(path, got, sizeof(got));
    if (rr == 1) return 0;
    if (rr < 0) {
        fprintf(stderr, "failed to read %s\n", path);
        return -1;
    }
    if (strcmp(got, want) != 0 && !force) {
        fprintf(stderr, "%s\n", mismatch_msg);
        return -1;
    }
    return 0;
}

static int restore_persist_line_(const char *path, const char *want, int force,
                                 const char *mismatch_msg) {
    char got[2048];
    int rr = restore_read_line_(path, got, sizeof(got));
    if (rr < 0) {
        fprintf(stderr, "failed to read %s\n", path);
        return -1;
    }
    if (rr == 0 && strcmp(got, want) != 0 && !force) {
        fprintf(stderr, "%s\n", mismatch_msg);
        return -1;
    }
    if (restore_write_line_(path, want) != 0) {
        perror(path);
        return -1;
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
        int want_json = 0, want_fields = 0, want_table = 0;
        const char *filename = NULL;
        for (int i = 3; i < argc; i++) {
        int wr = 0, sk = 0, wj = 0, wt = 0, wf = 0;
            if ((wr = take_write_out_jtf_(&i, argc, argv, &want_json, &want_table, &want_fields)) != 0) {
                if (wr < 0) { fprintf(stderr, "--write-out requires a format\n"); return 1; }
            } else if (cmd_flag_is_(argv[i], "--compaction-periodical")) {
                int on = 1;
                if (cetcd_take_cli_bool_eq(&i, argc, argv, &on) != CETCD_OK) {
                    fprintf(stderr, "--compaction-periodical must be true or false\n");
                    return 1;
                }
                /* no-op: accepted for etcdctl compatibility; does not compact */
            } else if (strcmp(argv[i], "--") == 0) {
                if (i + 1 < argc && !filename) filename = argv[++i];
                else {
                    fprintf(stderr, "unknown flag: %s\n", argv[i]);
                    return 1;
                }
            } else if (argv[i][0] == '-') {
                fprintf(stderr, "unknown flag: %s\n", argv[i]);
                return 1;
            } else if (!filename) {
                filename = argv[i];
            } else {
                fprintf(stderr, "unknown flag: %s\n", argv[i]);
                return 1;
            }
        }
        uint8_t req[] = {0x00}, resp[65536];
        int rlen = do_rpc("/etcdserverpb.Maintenance/Snapshot", req, 1, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        /* leftover-safe: leftover cannot steal the snapshot blob */
        uint8_t snap_blob[65536];
        size_t blob_len = 0;
        if (cetcd_parse_snapshot_response(resp, (size_t)rlen, snap_blob,
                                          sizeof(snap_blob), &blob_len)
            != CETCD_OK) {
            fprintf(stderr, "request failed\n");
            return 1;
        }
        const uint8_t *blob_data = snap_blob;
        uint64_t snap_revision = 0;
        {
            int64_t rev = 0;
            uint64_t cluster = 0, member = 0, term = 0;
            if (cetcd_parse_response_header(resp, (size_t)rlen, &cluster,
                                            &member, &rev, &term) == CETCD_OK)
                snap_revision = (uint64_t)rev;
        }
        size_t snapshot_size = blob_len;
        /* If a file is specified, write the blob to it */
        if (filename) {
            FILE *f = fopen(filename, "wb");
            if (!f) { perror("fopen"); return 1; }
            size_t enc_len = 0;
            uint8_t *enc = cetcd_snap_encode_cts2(blob_data, blob_len,
                                                 snap_revision, &enc_len);
            if (!enc) { fclose(f); fprintf(stderr, "out of memory\n"); return 1; }
            if (fwrite(enc, 1, enc_len, f) != enc_len) {
                free(enc);
                fclose(f);
                fprintf(stderr, "write snapshot failed\n");
                return 1;
            }
            free(enc);
            fclose(f);
            snapshot_size = enc_len;
            if (want_json) {
                fputs("{", stdout);
                parse_and_print_header_json(resp, (size_t)rlen);
                printf(",\"snapshot\":\"%s\",\"size\":%zu}\n", filename, snapshot_size);
            } else if (want_fields) {
                parse_and_print_header_json(resp, (size_t)rlen);
                printf("snapshot: %s\n", filename);
                printf("size: %zu\n", snapshot_size);
                fputs("\n", stdout);
            } else if (want_table) {
                printf("+------------+----------+----------------+\n");
                printf("| REVISION  |   SIZE   |    FILENAME    |\n");
                printf("+------------+----------+----------------+\n");
                printf("| %-10llu | %8zu | %-14s |\n", (unsigned long long)snap_revision, snapshot_size, filename);
                printf("+------------+----------+----------------+\n");
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
            } else if (want_table) {
                printf("+------------+----------+\n");
                printf("| REVISION  |   SIZE   |\n");
                printf("+------------+----------+\n");
                printf("| %-10llu | %8zu |\n", (unsigned long long)snap_revision, snapshot_size);
                printf("+------------+----------+\n");
            } else {
                printf("snapshot: %zu bytes received\n", snapshot_size);
            }
        }
        return 0;
    } else if (strcmp(argv[2], "status") == 0) {
        /* Show snapshot file info */
        if (argc < 4) {
            fprintf(stderr, "usage: cetcdctl snapshot status FILE [-w json|fields|table]\n");
            return 1;
        }
        if (argv[3][0] == '-' && strcmp(argv[3], "--") != 0) {
            fprintf(stderr, "unknown flag: %s\n", argv[3]);
            return 1;
        }
        int snap_json = 0, snap_fields = 0;
        for (int i = 4; i < argc; i++) {
            int wr = take_write_out_jf_(&i, argc, argv, &snap_json, &snap_fields);
            if (wr < 0) { fprintf(stderr, "--write-out requires a format\n"); return 1; }
            if (wr == 0 && argv[i][0] == '-') {
                fprintf(stderr, "unknown flag: %s\n", argv[i]);
                return 1;
            }
        }
        FILE *f = fopen(argv[3], "rb");
        if (!f) { perror("fopen"); return 1; }
        fseek(f, 0, SEEK_END);
        long fsize = ftell(f);
        fseek(f, 0, SEEK_SET);
        uint8_t *fdata = (fsize > 0) ? (uint8_t *)malloc((size_t)fsize) : NULL;
        if (fsize > 0 && !fdata) { fclose(f); fprintf(stderr, "out of memory\n"); return 1; }
        if (fdata) fread(fdata, 1, (size_t)fsize, f);
        fclose(f);
        cetcd_snap_header hdr;
        if (cetcd_snap_parse_header(fdata, fsize > 0 ? (size_t)fsize : 0, &hdr) != CETCD_OK) {
            free(fdata);
            fprintf(stderr, "snapshot header is truncated or invalid\n");
            return 1;
        }
        uint32_t hash = hdr.has_hash
            ? hdr.hash
            : cetcd_snap_crc32c(fdata ? fdata + hdr.kv_off : NULL, hdr.kv_len);
        uint64_t snap_rev = hdr.revision;
        int key_count = 0;
        cetcd_snap *parsed = cetcd_snap_decode_kv(fdata, fsize > 0 ? (size_t)fsize : 0);
        if (parsed) {
            key_count = (int)cetcd_snap_entry_count(parsed);
            cetcd_snap_free(parsed);
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
        cetcd_restore_opts ro;
        int want_json = 0, want_fields = 0;
        for (int i = 3; i < argc; i++) {
            int wr = take_write_out_jf_(&i, argc, argv, &want_json, &want_fields);
            if (wr < 0) { fprintf(stderr, "--write-out requires a format\n"); return 1; }
        }
        if (cetcd_parse_restore_argv(argc, argv, 3, &ro) != CETCD_OK) {
            fprintf(stderr,
                    "unknown leftover flag (snapshot restore --data-dir --wal-dir cannot eat a flag as the path)\n");
            fprintf(stderr, "usage: cetcdctl snapshot restore FILE --data-dir DIR [--wal-dir DIR] [--bump-revision] [--mark-compacted] [--force] [--skip-hash-check] [--initial-cluster-token TOKEN] [--initial-cluster-state new|existing] [--initial-cluster SPEC] [--name NAME] [--initial-advertise-peer-urls URL] [-w json|fields]\n");
            return 1;
        }
        const char *snap_file = ro.snap_file;
        const char *data_dir = ro.data_dir;
        const char *wal_dir = ro.wal_dir;
        const char *cluster_token = ro.cluster_token;
        const char *initial_cluster = ro.initial_cluster;
        const char *adv_peer = ro.adv_peer;
        const char *member_name = ro.member_name;
        const char *cluster_state = ro.cluster_state;
        int force = ro.force;
        int skip_hash = ro.skip_hash;
        if (initial_cluster) {
            if (!initial_cluster[0]) {
                fprintf(stderr, "--initial-cluster must not be empty\n");
                return 1;
            }
            cetcd_peer_info parsed[32];
            uint32_t pn = 0;
            int https = 0;
            int prc = cetcd_parse_initial_cluster(initial_cluster, parsed, 32,
                                                  &pn, &https);
            (void)https;
            if (prc == CETCD_ERR_RANGE) {
                fprintf(stderr, "--initial-cluster port must be 1..65535\n");
                return 1;
            }
            if (prc != CETCD_OK) {
                fprintf(stderr, "--initial-cluster member id must be > 0\n");
                return 1;
            }
        }
        if (member_name && !member_name[0]) {
            fprintf(stderr, "--name must not be empty\n");
            return 1;
        }
        if (adv_peer && !adv_peer[0]) {
            fprintf(stderr, "--initial-advertise-peer-urls must not be empty\n");
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
        int vr = cetcd_snap_verify(snap_data, (size_t)snap_size);
        if (vr == CETCD_ERR_INVAL) {
            free(snap_data);
            fprintf(stderr, "snapshot header is truncated or invalid\n");
            return 1;
        }
        if (vr == CETCD_ERR_CORRUPT && !skip_hash) {
            free(snap_data);
            fprintf(stderr, "snapshot hash mismatch, use --skip-hash-check to override\n");
            return 1;
        }
        cetcd_snap_header hdr;
        if (cetcd_snap_parse_header(snap_data, (size_t)snap_size, &hdr) != CETCD_OK) {
            free(snap_data);
            fprintf(stderr, "snapshot header is truncated or invalid\n");
            return 1;
        }
        size_t kv_offset = hdr.kv_off;
        size_t kv_size = hdr.kv_len;
        uint64_t restore_rev = hdr.revision;
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
        if (cluster_token && cluster_token[0]) {
            char tok_path[600];
            snprintf(tok_path, sizeof(tok_path), "%s/cluster_token", data_dir);
            FILE *tf = fopen(tok_path, "r");
            if (tf) {
                char got[128];
                if (!fgets(got, sizeof(got), tf)) {
                    fclose(tf);
                    free(snap_data);
                    fprintf(stderr, "failed to read cluster token\n");
                    return 1;
                }
                fclose(tf);
                size_t gl = strlen(got);
                while (gl > 0 && (got[gl - 1] == '\n' || got[gl - 1] == '\r'))
                    got[--gl] = '\0';
                if (strcmp(got, cluster_token) != 0 && !force) {
                    free(snap_data);
                    fprintf(stderr, "cluster token mismatch, use --force to overwrite\n");
                    return 1;
                }
            }
        }
        {
            char p[600];
            if (initial_cluster) {
                snprintf(p, sizeof(p), "%s/initial-cluster", data_dir);
                if (restore_check_mismatch_(p, initial_cluster, force,
                        "initial-cluster mismatch, use --force to overwrite") != 0) {
                    free(snap_data);
                    return 1;
                }
            }
            if (member_name) {
                snprintf(p, sizeof(p), "%s/name", data_dir);
                if (restore_check_mismatch_(p, member_name, force,
                        "name mismatch, use --force to overwrite") != 0) {
                    free(snap_data);
                    return 1;
                }
            }
            if (adv_peer) {
                snprintf(p, sizeof(p), "%s/initial-advertise-peer-urls", data_dir);
                if (restore_check_mismatch_(p, adv_peer, force,
                        "initial-advertise-peer-urls mismatch, use --force to overwrite") != 0) {
                    free(snap_data);
                    return 1;
                }
            }
            if (cluster_state) {
                snprintf(p, sizeof(p), "%s/initial-cluster-state", data_dir);
                if (restore_check_mismatch_(p, cluster_state, force,
                        "initial-cluster-state mismatch, use --force to overwrite") != 0) {
                    free(snap_data);
                    return 1;
                }
            }
        }
        uint64_t write_rev = 0, compact_rev = 0;
        if (cetcd_restore_revision(restore_rev, ro.bump_revision,
                                   ro.mark_compacted, &write_rev,
                                   &compact_rev) != CETCD_OK) {
            free(snap_data);
            fprintf(stderr, "--bump-revision / --mark-compacted is invalid\n");
            return 1;
        }
        /* Write the snapshot KV data to the data directory as snapshot.kv */
        FILE *df = fopen(check_path, "wb");
        if (!df) { perror("fopen data dir"); free(snap_data); return 1; }
        if (write_rev > 0) {
            size_t cts_n = 0;
            uint8_t *cts = cetcd_snap_encode_cts2(snap_data + kv_offset, kv_size,
                                                  write_rev, &cts_n);
            if (!cts) {
                fclose(df);
                free(snap_data);
                fprintf(stderr, "out of memory\n");
                return 1;
            }
            fwrite(cts, 1, cts_n, df);
            free(cts);
            restore_rev = write_rev;
        } else {
            fwrite(snap_data + kv_offset, 1, kv_size, df);
        }
        fclose(df);
        if (compact_rev > 0) {
            char p[600];
            char buf[32];
            snprintf(p, sizeof(p), "%s/mark-compacted", data_dir);
            snprintf(buf, sizeof(buf), "%llu", (unsigned long long)compact_rev);
            if (restore_persist_line_(p, buf, force,
                    "mark-compacted mismatch, use --force to overwrite") != 0) {
                free(snap_data);
                return 1;
            }
        }
        if (wal_dir) {
            char p[600];
            snprintf(p, sizeof(p), "%s/wal-dir", data_dir);
            if (restore_persist_line_(p, wal_dir, force,
                    "wal-dir mismatch, use --force to overwrite") != 0) {
                free(snap_data);
                return 1;
            }
        }
        if (cluster_token && cluster_token[0]) {
            char tok_path[600];
            snprintf(tok_path, sizeof(tok_path), "%s/cluster_token", data_dir);
            FILE *tf = fopen(tok_path, "w");
            if (!tf) {
                perror("fopen cluster_token");
                free(snap_data);
                return 1;
            }
            fprintf(tf, "%s\n", cluster_token);
            fclose(tf);
        }
        if (initial_cluster) {
            char p[600];
            snprintf(p, sizeof(p), "%s/initial-cluster", data_dir);
            if (restore_persist_line_(p, initial_cluster, force,
                    "initial-cluster mismatch, use --force to overwrite") != 0) {
                free(snap_data);
                return 1;
            }
        }
        if (member_name) {
            char p[600];
            snprintf(p, sizeof(p), "%s/name", data_dir);
            if (restore_persist_line_(p, member_name, force,
                    "name mismatch, use --force to overwrite") != 0) {
                free(snap_data);
                return 1;
            }
        }
        if (adv_peer) {
            char p[600];
            snprintf(p, sizeof(p), "%s/initial-advertise-peer-urls", data_dir);
            if (restore_persist_line_(p, adv_peer, force,
                    "initial-advertise-peer-urls mismatch, use --force to overwrite") != 0) {
                free(snap_data);
                return 1;
            }
        }
        if (cluster_state) {
            char p[600];
            snprintf(p, sizeof(p), "%s/initial-cluster-state", data_dir);
            if (restore_persist_line_(p, cluster_state, force,
                    "initial-cluster-state mismatch, use --force to overwrite") != 0) {
                free(snap_data);
                return 1;
            }
        }
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
        action = 1;
        if (cetcd_ctl_parse_one_name_argv(argc, argv, 3, &version) != CETCD_OK) {
            fprintf(stderr, "usage: cetcdctl downgrade enable VERSION [-w json|fields]\n");
            return 1;
        }
    } else if (strcmp(argv[2], "cancel") == 0) {
        action = 2;
        if (cetcd_ctl_parse_maint_argv(argc, argv, 3, 0, NULL) != CETCD_OK) {
            fprintf(stderr, "unknown flag\n");
            return 1;
        }
    } else if (strcmp(argv[2], "validate") == 0) {
        action = 0;
        if (cetcd_ctl_parse_one_name_argv(argc, argv, 3, &version) != CETCD_OK) {
            fprintf(stderr, "usage: cetcdctl downgrade validate VERSION [-w json|fields]\n");
            return 1;
        }
    } else {
        fprintf(stderr, "unknown downgrade subcommand: %s\n", argv[2]);
        return 1;
    }

    for (int i = 3; i < argc; i++) {
        int wr = 0, sk = 0, wj = 0, wt = 0, wf = 0;
        if ((wr = take_write_out_jf_(&i, argc, argv, &want_json, &want_fields)) != 0) {
            if (wr < 0) { fprintf(stderr, "--write-out requires a format\n"); return 1; }
        }
    }

    pos = encode_varint_field(req, sizeof(req), pos, 0x08, action);
    if (version[0])
        pos = encode_string_field(req, sizeof(req), pos, 0x12, version);

    int rlen = do_rpc("/etcdserverpb.Maintenance/Downgrade", req, pos, resp, sizeof(resp));
    if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
    /* leftover-safe: leftover cannot steal a printed version */
    char got_ver[32];
    if (cetcd_parse_downgrade_response(resp, (size_t)rlen, got_ver,
                                       sizeof(got_ver)) != CETCD_OK) {
        fprintf(stderr, "request failed\n");
        return 1;
    }
    if (want_json) {
        fputs("{", stdout);
        parse_and_print_header_json(resp, (size_t)rlen);
        fputs(",\"version\":", stdout);
        print_json_string((const uint8_t *)got_ver, strlen(got_ver));
        fputs("}\n", stdout);
    } else if (want_fields) {
        parse_and_print_header_json(resp, (size_t)rlen);
        printf("version: %s\n", got_ver);
        fputs("\n", stdout);
    } else if (got_ver[0]) {
        printf("%s\n", got_ver);
    } else {
        printf("OK\n");
    }
    return 0;
}

static int peer_urls_leftover_ok_(const char *urls) {
    if (!urls || !urls[0]) return -1;
    char copy[512];
    strncpy(copy, urls, sizeof(copy) - 1);
    copy[sizeof(copy) - 1] = '\0';
    char *saveptr = NULL;
    char *tok = strtok_r(copy, ",", &saveptr);
    int n = 0;
    while (tok) {
        while (*tok == ' ') tok++;
        if (*tok) {
            char addr[256];
            uint16_t port = 0;
            if (cetcd_parse_peer_url(tok, strlen(tok), addr, sizeof(addr), &port) != CETCD_OK)
                return -1;
            n++;
        }
        tok = strtok_r(NULL, ",", &saveptr);
    }
    return n > 0 ? 0 : -1;
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
        fprintf(stderr, "usage: cetcdctl member list [-w json|table|fields] [--linearizable[=bool]]\n");
        fprintf(stderr, "       cetcdctl member add [-w json|fields] [--peer-urls URLS] [--name NAME] [--learner] [PEER_URL]\n");
        fprintf(stderr, "       cetcdctl member remove [-w json|fields] ID\n");
        fprintf(stderr, "       cetcdctl member update [-w json|fields] ID PEER_URLS\n");
        fprintf(stderr, "       cetcdctl member promote [-w json|fields] ID\n");
        return 1;
    }
    /* Parse -w json/fields for all subcommands */
    int want_json = 0, want_fields = 0;
    for (int i = 3; i < argc; i++) {
        int wr = 0, sk = 0, wj = 0, wt = 0, wf = 0;
        if ((wr = take_write_out_jf_(&i, argc, argv, &want_json, &want_fields)) != 0) {
            if (wr < 0) { fprintf(stderr, "--write-out requires a format\n"); return 1; }
        }
    }
    if (strcmp(argv[2], "list") == 0) {
        int table_fmt = 0, json_fmt = 0, fields_fmt = 0;
        int linearizable = 1;
        for (int i = 3; i < argc; i++) {
        int wr = 0, sk = 0, wj = 0, wt = 0, wf = 0;
            if ((wr = take_write_out_jtf_(&i, argc, argv, &json_fmt, &table_fmt, &fields_fmt)) != 0) {
                if (wr < 0) { fprintf(stderr, "--write-out requires a format\n"); return 1; }
            }
        }
        if (cetcd_ctl_parse_member_list_argv(argc, argv, 3, &linearizable)
            != CETCD_OK) {
            fprintf(stderr,
                    "unknown leftover flag (member list --linearizable --foo cannot list members)\n");
            return 1;
        }
        uint8_t req[8], resp[4096];
        size_t rpos = 0;
        if (cetcd_encode_member_list_request(linearizable, req, sizeof(req),
                                             &rpos) != CETCD_OK) {
            fprintf(stderr, "failed to encode MemberList request\n");
            return 1;
        }
        int rlen = do_rpc("/etcdserverpb.Cluster/MemberList", req, rpos, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        {
            char leftover_curl[256];
            char leftover_name[128];
            leftover_curl[0] = '\0';
            leftover_name[0] = '\0';
            /* leftover-safe: leftover cannot steal a printed client URL or name */
            if (cetcd_parse_member_list_client_url(resp, (size_t)rlen,
                                                   leftover_curl,
                                                   sizeof(leftover_curl))
                != CETCD_OK) {
                fprintf(stderr, "request failed\n");
                return 1;
            }
            if (cetcd_parse_member_list_name(resp, (size_t)rlen, leftover_name,
                                             sizeof(leftover_name))
                != CETCD_OK) {
                fprintf(stderr, "request failed\n");
                return 1;
            }
            int leftover_learner = 0;
            /* leftover-safe: leftover cannot steal a printed isLearner */
            if (cetcd_parse_member_list_is_learner(resp, (size_t)rlen,
                                                   &leftover_learner)
                != CETCD_OK) {
                fprintf(stderr, "request failed\n");
                return 1;
            }
            (void)leftover_learner;
        }
        parse_member_list_response(resp, rlen, table_fmt, json_fmt, fields_fmt);
    } else if (strcmp(argv[2], "add") == 0) {
        const char *peer_url = NULL;
        const char *member_name = NULL;
        int is_learner = 0;
        for (int i = 3; i < argc; i++) {
            int on = 1, sk;
            if (cmd_flag_is_(argv[i], "--peer-urls")) {
                if (take_cmd_value_(&i, argc, argv, &peer_url) != 0) {
                    fprintf(stderr, "--peer-urls requires a URL\n");
                    return 1;
                }
            } else if (cmd_flag_is_(argv[i], "--name")) {
                if (take_cmd_value_(&i, argc, argv, &member_name) != 0) {
                    fprintf(stderr, "--name requires a member name\n");
                    return 1;
                }
            } else if (cmd_flag_is_(argv[i], "--learner")) {
                if (cetcd_take_cli_bool_eq(&i, argc, argv, &on) != CETCD_OK) {
                    fprintf(stderr, "--learner must be true or false\n");
                    return 1;
                }
                is_learner = on;
            } else if ((sk = skip_write_out_(&i, argc, argv)) != 0) {
                if (sk < 0) { fprintf(stderr, "--write-out requires a format\n"); return 1; }
            } else if (cetcd_cli_is_long_flag(argv[i]) || argv[i][0] == '-') {
                fprintf(stderr, "unknown flag: %s\n", argv[i]);
                return 1;
            } else if (!peer_url) {
                peer_url = argv[i];
            }
        }
        if (!peer_url) { fprintf(stderr, "usage: cetcdctl member add [-w json] [--peer-urls URLS] [--name NAME] [--learner] [PEER_URL]\n"); return 1; }
        if (peer_urls_leftover_ok_(peer_url) != 0) {
            fprintf(stderr, "member add peer URL port must be 1..65535\n");
            return 1;
        }
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
        {
            char leftover_curl[256];
            char leftover_name[128];
            leftover_curl[0] = '\0';
            leftover_name[0] = '\0';
            /* leftover-safe: leftover cannot steal a printed client URL or name */
            if (cetcd_parse_member_list_client_url(resp, (size_t)rlen,
                                                   leftover_curl,
                                                   sizeof(leftover_curl))
                != CETCD_OK) {
                fprintf(stderr, "request failed\n");
                return 1;
            }
            if (cetcd_parse_member_list_name(resp, (size_t)rlen, leftover_name,
                                             sizeof(leftover_name))
                != CETCD_OK) {
                fprintf(stderr, "request failed\n");
                return 1;
            }
            int leftover_learner = 0;
            /* leftover-safe: leftover cannot steal a printed isLearner */
            if (cetcd_parse_member_list_is_learner(resp, (size_t)rlen,
                                                   &leftover_learner)
                != CETCD_OK) {
                fprintf(stderr, "request failed\n");
                return 1;
            }
            (void)leftover_learner;
        }
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
            int sk = skip_write_out_(&i, argc, argv);
            if (sk < 0) { fprintf(stderr, "--write-out requires a format\n"); return 1; }
            if (sk > 0) continue;
            if (cetcd_cli_is_long_flag(argv[i]) || argv[i][0] == '-') {
                fprintf(stderr, "unknown flag: %s\n", argv[i]);
                return 1;
            }
            if (!id_str) id_str = argv[i];
        }
        if (!id_str) { fprintf(stderr, "usage: cetcdctl member remove [-w json|fields] ID\n"); return 1; }
        uint64_t mid = 0;
        if (parse_positive_hex_u64_(id_str, &mid) != 0) {
            fprintf(stderr, "member remove ID must be a hex integer > 0\n");
            return 1;
        }
        uint8_t req[32], resp[256];
        size_t pos = 0;
        pos = encode_varint_field(req, sizeof(req), pos, 0x08, mid);
        int rlen = do_rpc("/etcdserverpb.Cluster/MemberRemove", req, pos, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        if (want_json) { fputs("{", stdout); parse_and_print_header_json(resp, (size_t)rlen); fputs("}\n", stdout); }
        else if (want_fields) { parse_and_print_header_json(resp, (size_t)rlen); fputs("\n", stdout); }
        else { printf("OK\n"); }
    } else if (strcmp(argv[2], "update") == 0) {
        const char *id_str = NULL;
        const char *peer_url = NULL;
        for (int i = 3; i < argc; i++) {
            int sk = skip_write_out_(&i, argc, argv);
            if (sk < 0) { fprintf(stderr, "--write-out requires a format\n"); return 1; }
            if (sk > 0) continue;
            if (cetcd_cli_is_long_flag(argv[i]) || argv[i][0] == '-') {
                fprintf(stderr, "unknown flag: %s\n", argv[i]);
                return 1;
            }
            if (!id_str) id_str = argv[i];
            else if (!peer_url) peer_url = argv[i];
        }
        if (!id_str || !peer_url) { fprintf(stderr, "usage: cetcdctl member update [-w json|fields] ID PEER_URLS\n"); return 1; }
        uint64_t mid = 0;
        if (parse_positive_hex_u64_(id_str, &mid) != 0) {
            fprintf(stderr, "member update ID must be a hex integer > 0\n");
            return 1;
        }
        if (peer_urls_leftover_ok_(peer_url) != 0) {
            fprintf(stderr, "member update peer URL port must be 1..65535\n");
            return 1;
        }
        uint8_t req[1024], resp[256];
        size_t pos = 0;
        pos = encode_varint_field(req, sizeof(req), pos, 0x08, mid);
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
            int sk = skip_write_out_(&i, argc, argv);
            if (sk < 0) { fprintf(stderr, "--write-out requires a format\n"); return 1; }
            if (sk > 0) continue;
            if (cetcd_cli_is_long_flag(argv[i]) || argv[i][0] == '-') {
                fprintf(stderr, "unknown flag: %s\n", argv[i]);
                return 1;
            }
            if (!id_str) id_str = argv[i];
        }
        if (!id_str) { fprintf(stderr, "usage: cetcdctl member promote [-w json|fields] ID\n"); return 1; }
        uint64_t mid = 0;
        if (parse_positive_hex_u64_(id_str, &mid) != 0) {
            fprintf(stderr, "member promote ID must be a hex integer > 0\n");
            return 1;
        }
        uint8_t req[32], resp[256];
        size_t pos = 0;
        pos = encode_varint_field(req, sizeof(req), pos, 0x08, mid);
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
        if (cetcd_ctl_parse_two_name_argv(argc, argv, 3, &name, &pass) != CETCD_OK) {
            fprintf(stderr, "usage: cetcdctl auth login [-w json|fields] NAME PASS\n");
            return 1;
        }
        for (int i = 3; i < argc; i++) {
        int wr = 0, sk = 0, wj = 0, wt = 0, wf = 0;
            if ((wr = take_write_out_jf_(&i, argc, argv, &want_json, &want_fields)) != 0) {
                if (wr < 0) { fprintf(stderr, "--write-out requires a format\n"); return 1; }
            }
        }
        uint8_t req[512], resp[1024];
        size_t pos = 0;
        pos = encode_string_field(req, sizeof(req), pos, 0x0a, name);
        pos = encode_string_field(req, sizeof(req), pos, 0x12, pass);
        int rlen = do_rpc("/etcdserverpb.Auth/Authenticate", req, pos, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "authentication failed\n"); return 1; }
        /* leftover-safe: leftover cannot steal a printed token */
        char token[256];
        if (cetcd_parse_authenticate_response(resp, (size_t)rlen, token,
                                              sizeof(token)) != CETCD_OK) {
            fprintf(stderr, "authentication failed\n");
            return 1;
        }
        if (token[0]) {
            if (want_json) {
                fputs("{", stdout);
                parse_and_print_header_json(resp, (size_t)rlen);
                fputs(",\"token\":", stdout);
                print_json_string((const uint8_t *)token, strlen(token));
                fputs("}\n", stdout);
            } else if (want_fields) {
                parse_and_print_header_json(resp, (size_t)rlen);
                printf("token: %s\n", token);
                fputs("\n", stdout);
            } else {
                printf("token: %s\n", token);
            }
            return 0;
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
        int wr = 0, sk = 0, wj = 0, wt = 0, wf = 0;
        if ((wr = take_write_out_jf_(&i, argc, argv, &want_json, &want_fields)) != 0) {
            if (wr < 0) { fprintf(stderr, "--write-out requires a format\n"); return 1; }
        } else if (argv[i][0] == '-') {
            fprintf(stderr, "unknown flag: %s\n", argv[i]);
            return 1;
        }
    }
    int rlen = do_rpc(path, req, 1, resp, sizeof(resp));
    if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
    if (strcmp(argv[2], "status") == 0) {
        int enabled = 0;
        /* leftover-safe: leftover cannot steal printed enabled */
        if (cetcd_parse_auth_status_response(resp, (size_t)rlen, &enabled)
            != CETCD_OK) {
            fprintf(stderr, "request failed\n");
            return 1;
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
        int wr = 0, sk = 0, wj = 0, wt = 0, wf = 0;
        if ((wr = take_write_out_jf_(&i, argc, argv, &want_json, &want_fields)) != 0) {
            if (wr < 0) { fprintf(stderr, "--write-out requires a format\n"); return 1; }
        }
    }
    if (strcmp(argv[2], "add") == 0) {
        const char *user_name = NULL, *password = NULL;
        bool no_password = false;
        for (int i = 3; i < argc; i++) {
        int wr = 0, sk = 0, wj = 0, wt = 0, wf = 0;
            int on = 1;
            if (cmd_flag_is_(argv[i], "--no-password")) {
                if (cetcd_take_cli_bool_eq(&i, argc, argv, &on) != CETCD_OK) {
                    fprintf(stderr, "--no-password must be true or false\n");
                    return 1;
                }
                no_password = on != 0;
            } else if ((sk = skip_write_out_(&i, argc, argv)) != 0) {
                if (sk < 0) { fprintf(stderr, "--write-out requires a format\n"); return 1; }
            } else if (argv[i][0] == '-') {
                fprintf(stderr, "unknown flag: %s\n", argv[i]);
                return 1;
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
        const char *name = NULL;
        if (cetcd_ctl_parse_one_name_argv(argc, argv, 3, &name) != CETCD_OK) {
            fprintf(stderr, "usage: cetcdctl user delete NAME [-w json|fields]\n");
            return 1;
        }
        uint8_t req[256], resp[256];
        size_t pos = 0;
        pos = encode_string_field(req, sizeof(req), pos, 0x0a, name);
        int rlen = do_rpc("/etcdserverpb.Auth/UserDelete", req, pos, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        if (want_json) { fputs("{", stdout); parse_and_print_header_json(resp, (size_t)rlen); fputs("}\n", stdout); }
        else if (want_fields) { parse_and_print_header_json(resp, (size_t)rlen); fputs("\n", stdout); }
        else { printf("OK\n"); }
    } else if (strcmp(argv[2], "get") == 0) {
        const char *name = NULL;
        if (cetcd_ctl_parse_one_name_argv(argc, argv, 3, &name) != CETCD_OK) {
            fprintf(stderr, "usage: cetcdctl user get NAME [-w json|fields]\n");
            return 1;
        }
        uint8_t req[256], resp[4096];
        size_t pos = 0;
        pos = encode_string_field(req, sizeof(req), pos, 0x0a, name);
        int rlen = do_rpc("/etcdserverpb.Auth/UserGet", req, pos, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        if (!want_json && !want_fields) printf("roles:\n");
        parse_string_list_response(resp, rlen, "roles", 0, want_json, want_fields);
    } else if (strcmp(argv[2], "list") == 0) {
        int table_fmt = 0, fields_fmt = 0, list_json = 0;
        for (int i = 3; i < argc; i++) {
            int wr = take_write_out_jtf_(&i, argc, argv, &list_json, &table_fmt, &fields_fmt);
            if (wr < 0) { fprintf(stderr, "--write-out requires a format\n"); return 1; }
            if (list_json) want_json = 1;
        }
        uint8_t req[] = {0x00}, resp[4096];
        int rlen = do_rpc("/etcdserverpb.Auth/UserList", req, 1, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        parse_string_list_response(resp, rlen, "users", table_fmt, want_json, fields_fmt);
    } else if (strcmp(argv[2], "change-password") == 0) {
        const char *name = NULL, *pass = NULL;
        if (cetcd_ctl_parse_two_name_argv(argc, argv, 3, &name, &pass) != CETCD_OK) {
            fprintf(stderr, "usage: cetcdctl user change-password NAME PASS [-w json]\n");
            return 1;
        }
        uint8_t req[512], resp[256];
        size_t pos = 0;
        pos = encode_string_field(req, sizeof(req), pos, 0x0a, name);
        pos = encode_string_field(req, sizeof(req), pos, 0x12, pass);
        int rlen = do_rpc("/etcdserverpb.Auth/UserChangePassword", req, pos, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        if (want_json) { fputs("{", stdout); parse_and_print_header_json(resp, (size_t)rlen); fputs("}\n", stdout); }
        else if (want_fields) { parse_and_print_header_json(resp, (size_t)rlen); fputs("\n", stdout); }
        else { printf("OK\n"); }
    } else if (strcmp(argv[2], "grant-role") == 0) {
        const char *name = NULL, *role = NULL;
        if (cetcd_ctl_parse_two_name_argv(argc, argv, 3, &name, &role) != CETCD_OK) {
            fprintf(stderr, "usage: cetcdctl user grant-role NAME ROLE [-w json|fields]\n");
            return 1;
        }
        uint8_t req[512], resp[256];
        size_t pos = 0;
        pos = encode_string_field(req, sizeof(req), pos, 0x0a, name);
        pos = encode_string_field(req, sizeof(req), pos, 0x12, role);
        int rlen = do_rpc("/etcdserverpb.Auth/UserGrantRole", req, pos, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        if (want_json) { fputs("{", stdout); parse_and_print_header_json(resp, (size_t)rlen); fputs("}\n", stdout); }
        else if (want_fields) { parse_and_print_header_json(resp, (size_t)rlen); fputs("\n", stdout); }
        else { printf("OK\n"); }
    } else if (strcmp(argv[2], "revoke-role") == 0) {
        const char *name = NULL, *role = NULL;
        if (cetcd_ctl_parse_two_name_argv(argc, argv, 3, &name, &role) != CETCD_OK) {
            fprintf(stderr, "usage: cetcdctl user revoke-role NAME ROLE [-w json|fields]\n");
            return 1;
        }
        uint8_t req[512], resp[256];
        size_t pos = 0;
        pos = encode_string_field(req, sizeof(req), pos, 0x0a, name);
        pos = encode_string_field(req, sizeof(req), pos, 0x12, role);
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
        fprintf(stderr, "       cetcdctl role grant-permission ROLE TYPE KEY [--prefix|--from-key|--range-end KEY] [ENDKEY] [-w json|fields]\n");
        fprintf(stderr, "       cetcdctl role revoke-permission ROLE [KEY] [--prefix|--from-key|--range-end KEY] [ENDKEY] [-w json|fields]\n");
        return 1;
    }
    /* Parse -w json/fields for all subcommands */
    int want_json = 0, want_fields = 0;
    for (int i = 3; i < argc; i++) {
        int wr = 0, sk = 0, wj = 0, wt = 0, wf = 0;
        if ((wr = take_write_out_jf_(&i, argc, argv, &want_json, &want_fields)) != 0) {
            if (wr < 0) { fprintf(stderr, "--write-out requires a format\n"); return 1; }
        }
    }
    if (strcmp(argv[2], "add") == 0) {
        const char *name = NULL;
        if (cetcd_ctl_parse_one_name_argv(argc, argv, 3, &name) != CETCD_OK) {
            fprintf(stderr, "usage: cetcdctl role add NAME [-w json|fields]\n");
            return 1;
        }
        uint8_t req[256], resp[256];
        size_t pos = 0;
        pos = encode_string_field(req, sizeof(req), pos, 0x0a, name);
        int rlen = do_rpc("/etcdserverpb.Auth/RoleAdd", req, pos, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        if (want_json) { fputs("{", stdout); parse_and_print_header_json(resp, (size_t)rlen); fputs("}\n", stdout); }
        else if (want_fields) { parse_and_print_header_json(resp, (size_t)rlen); fputs("\n", stdout); }
        else { printf("OK\n"); }
    } else if (strcmp(argv[2], "delete") == 0) {
        const char *name = NULL;
        if (cetcd_ctl_parse_one_name_argv(argc, argv, 3, &name) != CETCD_OK) {
            fprintf(stderr, "usage: cetcdctl role delete NAME [-w json|fields]\n");
            return 1;
        }
        uint8_t req[256], resp[256];
        size_t pos = 0;
        pos = encode_string_field(req, sizeof(req), pos, 0x0a, name);
        int rlen = do_rpc("/etcdserverpb.Auth/RoleDelete", req, pos, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        if (want_json) { fputs("{", stdout); parse_and_print_header_json(resp, (size_t)rlen); fputs("}\n", stdout); }
        else if (want_fields) { parse_and_print_header_json(resp, (size_t)rlen); fputs("\n", stdout); }
        else { printf("OK\n"); }
    } else if (strcmp(argv[2], "list") == 0) {
        int table_fmt = 0, fields_fmt = 0, list_json = 0;
        for (int i = 3; i < argc; i++) {
            int wr = take_write_out_jtf_(&i, argc, argv, &list_json, &table_fmt, &fields_fmt);
            if (wr < 0) { fprintf(stderr, "--write-out requires a format\n"); return 1; }
            if (list_json) want_json = 1;
        }
        uint8_t req[] = {0x00}, resp[4096];
        int rlen = do_rpc("/etcdserverpb.Auth/RoleList", req, 1, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        parse_string_list_response(resp, rlen, "roles", table_fmt, want_json, fields_fmt);
    } else if (strcmp(argv[2], "get") == 0) {
        const char *name = NULL;
        if (cetcd_ctl_parse_one_name_argv(argc, argv, 3, &name) != CETCD_OK) {
            fprintf(stderr, "usage: cetcdctl role get NAME [-w json|fields]\n");
            return 1;
        }
        uint8_t req[256], resp[1024];
        size_t pos = 0;
        pos = encode_string_field(req, sizeof(req), pos, 0x0a, name);
        int rlen = do_rpc("/etcdserverpb.Auth/RoleGet", req, pos, resp, sizeof(resp));
        if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
        {
            char leftover_re[256];
            leftover_re[0] = '\0';
            /* leftover-safe: leftover cannot steal a printed range_end */
            if (cetcd_parse_role_get_range_end(resp, (size_t)rlen, leftover_re,
                                               sizeof(leftover_re))
                != CETCD_OK) {
                fprintf(stderr, "request failed\n");
                return 1;
            }
        }
        if (want_fields) {
            parse_and_print_header_json(resp, (size_t)rlen);
            printf("role: %s\n", name);
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
                        } else if (ptag == 0x12) {
                            uint64_t l = 0; read_varint(resp, pend, &rpos, &l);
                            printf("key: %.*s\n", (int)l, resp + rpos);
                            rpos += l;
                        } else if (ptag == 0x1a) {
                            uint64_t l = 0; read_varint(resp, pend, &rpos, &l);
                            printf("range_end: %.*s\n", (int)l, resp + rpos);
                            rpos += l;
                        } else if (ptag == 0x00) {
                            continue;
                        } else if (cetcd_leftover_safe_skip_field(resp, pend,
                                                                  &rpos, ptag)
                                   != CETCD_OK) {
                            break;
                        }
                    }
                    rpos = pend;
                } else if (tag == 0x0a) {
                    uint64_t l = 0; read_varint(resp, rlen, &rpos, &l);
                    rpos += l;
                } else if (tag == 0x00) {
                    continue;
                } else if (cetcd_leftover_safe_skip_field(resp, (size_t)rlen,
                                                          &rpos, tag)
                           != CETCD_OK) {
                    break;
                }
            }
            fputs("\n", stdout);
        } else if (want_json) {
            fputs("{", stdout);
            parse_and_print_header_json(resp, (size_t)rlen);
            printf(",\"role\":\"%s\",\"perm\":[", name);
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
                    const char *prend = NULL;
                    size_t prend_len = 0;
                    while (rpos < pend) {
                        uint8_t ptag = resp[rpos++];
                        if (ptag == 0x08) {
                            uint64_t pt = 0; read_varint(resp, pend, &rpos, &pt);
                            ptype = pt == 0 ? "READ" : pt == 1 ? "WRITE" : "READWRITE";
                        } else if (ptag == 0x12) {
                            uint64_t l = 0; read_varint(resp, pend, &rpos, &l);
                            pkey = (const char *)(resp + rpos);
                            pkey_len = (size_t)l;
                            rpos += l;
                        } else if (ptag == 0x1a) {
                            uint64_t l = 0; read_varint(resp, pend, &rpos, &l);
                            prend = (const char *)(resp + rpos);
                            prend_len = (size_t)l;
                            rpos += l;
                        } else if (ptag == 0x00) {
                            continue;
                        } else if (cetcd_leftover_safe_skip_field(resp, pend,
                                                                  &rpos, ptag)
                                   != CETCD_OK) {
                            break;
                        }
                    }
                    if (!first) printf(",");
                    printf("{\"permType\":\"%s\",\"key\":", ptype);
                    if (pkey) print_json_string((const uint8_t *)pkey, pkey_len); else fputs("\"\"", stdout);
                    if (prend) {
                        fputs(",\"range_end\":", stdout);
                        print_json_string((const uint8_t *)prend, prend_len);
                    }
                    fputs("}", stdout);
                    first = 0;
                    rpos = pend;
                } else if (tag == 0x0a) {
                    uint64_t l = 0; read_varint(resp, rlen, &rpos, &l);
                    rpos += l;
                } else if (tag == 0x00) {
                    continue;
                } else if (cetcd_leftover_safe_skip_field(resp, (size_t)rlen,
                                                          &rpos, tag)
                           != CETCD_OK) {
                    break;
                }
            }
            printf("]}\n");
        } else {
            printf("role: %s\n", name);
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
                        } else if (ptag == 0x12) {
                            uint64_t l = 0; read_varint(resp, pend, &rpos, &l);
                            printf("  key: %.*s\n", (int)l, resp + rpos);
                            rpos += l;
                        } else if (ptag == 0x1a) {
                            uint64_t l = 0; read_varint(resp, pend, &rpos, &l);
                            printf("  range_end: %.*s\n", (int)l, resp + rpos);
                            rpos += l;
                        } else if (ptag == 0x00) {
                            continue;
                        } else if (cetcd_leftover_safe_skip_field(resp, pend,
                                                                  &rpos, ptag)
                                   != CETCD_OK) {
                            break;
                        }
                    }
                    rpos = pend;
                } else if (tag == 0x0a) {
                    uint64_t l = 0; read_varint(resp, rlen, &rpos, &l);
                    rpos += l;
                } else if (tag == 0x00) {
                    continue;
                } else if (cetcd_leftover_safe_skip_field(resp, (size_t)rlen,
                                                          &rpos, tag)
                           != CETCD_OK) {
                    break;
                }
            }
        }
    } else if (strcmp(argv[2], "grant-permission") == 0) {
        const char *role_name = NULL, *perm_type_str = NULL, *key_str = NULL;
        const char *range_end_arg = NULL;
        int prefix = 0, from_key = 0;
        if (cetcd_ctl_parse_role_perm_argv(argc, argv, 3, 1, &role_name,
                                           &perm_type_str, &key_str,
                                           &range_end_arg, &prefix,
                                           &from_key) != CETCD_OK) {
            fprintf(stderr,
                    "unknown leftover flag (role grant-permission --range-end --from-key cannot grant a range)\n");
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
        perm[ppos++] = 0x12; /* field 2 = key */
        uint64_t l = klen;
        while (l >= 0x80) { perm[ppos++] = (uint8_t)(l | 0x80); l >>= 7; }
        perm[ppos++] = (uint8_t)l;
        memcpy(perm + ppos, key_str, klen); ppos += klen;
        /* field 3 = range_end (--prefix / --from-key / --range-end / ENDKEY) */
        if (from_key) {
            uint8_t z = 0;
            perm[ppos++] = 0x1a;
            perm[ppos++] = 0x01;
            perm[ppos++] = z;
        } else if (prefix) {
            uint8_t prefix_end[256];
            size_t pe_len = cetcd_key_prefix_end(prefix_end, sizeof(prefix_end),
                                                 cetcd_slice_make(key_str, klen));
            if (pe_len == 0) { fprintf(stderr, "key too long\n"); return 1; }
            perm[ppos++] = 0x1a;
            l = pe_len;
            while (l >= 0x80) { perm[ppos++] = (uint8_t)(l | 0x80); l >>= 7; }
            perm[ppos++] = (uint8_t)l;
            memcpy(perm + ppos, prefix_end, pe_len); ppos += pe_len;
        } else if (range_end_arg) {
            size_t re_len = strlen(range_end_arg);
            perm[ppos++] = 0x1a;
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
        int prefix = 0, from_key = 0;
        if (cetcd_ctl_parse_role_perm_argv(argc, argv, 3, 0, &role_name,
                                           &perm_type_str, &key_str,
                                           &range_end_arg, &prefix,
                                           &from_key) != CETCD_OK) {
            fprintf(stderr,
                    "unknown leftover flag (role revoke-permission --range-end --from-key cannot revoke a range)\n");
            return 1;
        }

        uint8_t req[512], resp[256];
        size_t pos = 0;
        pos = encode_string_field(req, sizeof(req), pos, 0x0a, role_name);
        if (key_str) {
            /* Send key (field 2, tag 0x12) for specific permission revocation */
            size_t klen = strlen(key_str);
            pos = encode_bytes_field(req, sizeof(req), pos, 0x12,
                                     (const uint8_t *)key_str, klen);
            /* field 3 (range_end, tag 0x1a) — leftover-safe --from-key / --prefix / ENDKEY */
            if (from_key) {
                uint8_t z = 0;
                pos = encode_bytes_field(req, sizeof(req), pos, 0x1a, &z, 1);
            } else if (prefix) {
                uint8_t prefix_end[256];
                size_t pe_len = cetcd_key_prefix_end(prefix_end, sizeof(prefix_end),
                                                     cetcd_slice_make(key_str, klen));
                if (pe_len == 0) { fprintf(stderr, "key too long\n"); return 1; }
                pos = encode_bytes_field(req, sizeof(req), pos, 0x1a, prefix_end, pe_len);
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
                                  int filter_type, bool progress_notify,
                                  bool fragment) {
    uint8_t create_inner[512];
    size_t cpos = 0;
    cpos = encode_bytes_field(create_inner, sizeof(create_inner), cpos, 0x0a,
                              (const uint8_t *)key, key_len);
    if (prefix) {
        uint8_t range_end[256];
        size_t pe_len = cetcd_key_prefix_end(range_end, sizeof(range_end),
                                             cetcd_slice_make(key, key_len));
        if (pe_len == 0) return 0;
        cpos = encode_bytes_field(create_inner, sizeof(create_inner), cpos, 0x12,
                                  range_end, pe_len);
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
        cpos = encode_varint_field(create_inner, sizeof(create_inner), cpos, 0x28, (uint64_t)filter_type);
    if (fragment)
        cpos = encode_varint_field(create_inner, sizeof(create_inner), cpos, 0x40, 1);
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

    char cancel_reason[128];
    cancel_reason[0] = '\0';
    if (cetcd_parse_watch_cancel_reason(resp, rlen, cancel_reason,
                                        sizeof(cancel_reason)) != CETCD_OK)
        cancel_reason[0] = '\0';
    int64_t leftover_lease = 0;
    /* leftover-safe: leftover cannot steal a printed lease */
    if (cetcd_parse_range_response_kv_lease(resp, rlen, &leftover_lease)
        != CETCD_OK)
        leftover_lease = 0;
    (void)leftover_lease;
    int64_t leftover_version = 0;
    /* leftover-safe: leftover cannot steal a printed version */
    if (cetcd_parse_range_response_kv_version(resp, rlen, &leftover_version)
        != CETCD_OK)
        leftover_version = 0;
    (void)leftover_version;
    int64_t leftover_create = 0;
    /* leftover-safe: leftover cannot steal a printed create_revision */
    if (cetcd_parse_range_response_kv_create_rev(resp, rlen, &leftover_create)
        != CETCD_OK)
        leftover_create = 0;
    (void)leftover_create;
    int64_t leftover_mod = 0;
    /* leftover-safe: leftover cannot steal a printed mod_revision */
    if (cetcd_parse_range_response_kv_mod_rev(resp, rlen, &leftover_mod)
        != CETCD_OK)
        leftover_mod = 0;
    (void)leftover_mod;

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
                        else if (ktag == 0x00) continue;
                        else if (cetcd_leftover_safe_skip_field(resp, kend,
                                                                &rpos, ktag)
                                 != CETCD_OK) {
                            break;
                        }
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
                        else if (ktag == 0x00) continue;
                        else if (cetcd_leftover_safe_skip_field(resp, kend,
                                                                &rpos, ktag)
                                 != CETCD_OK) {
                            break;
                        }
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
                } else if (etag == 0x00) {
                    continue;
                } else if (cetcd_leftover_safe_skip_field(resp, eend, &rpos,
                                                          etag) != CETCD_OK) {
                    break;
                }
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
        else if (tag == 0x00) continue;
        else if (cetcd_leftover_safe_skip_field(resp, rlen, &rpos, tag)
                 != CETCD_OK) {
            break;
        }
    }
    if (want_json) {
        if (cancel_reason[0]) {
            fputs("],\"cancel_reason\":", stdout);
            print_json_string((const uint8_t *)cancel_reason,
                              strlen(cancel_reason));
            fputs("}\n", stdout);
        } else {
            fputs("]}\n", stdout);
        }
    } else if (cancel_reason[0]) {
        if (want_fields) printf("cancel_reason: %s\n", cancel_reason);
        else printf("cancel reason: %s\n", cancel_reason);
    }
    return event_count;
}

static int cmd_watch(int argc, char **argv) {
    if (argc < 3) { fprintf(stderr, "usage: cetcdctl watch [-i] [--prefix] [--range-end KEY] [--prev-kv] [--progress-notify] [--fragment] [--start-rev N] [--filter TYPE] [--hex] [--exec CMD] [-w json|fields] KEY\n"); return 1; }
    bool prefix = false;
    bool prev_kv = false;
    bool progress_notify = false;
    bool fragment = false;
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
        int on = 1, wj = 0, wf = 0, wr;
        if (cmd_flag_is_(argv[i], "-i") || cmd_flag_is_(argv[i], "--interactive")) {
            if (cetcd_take_cli_bool_eq(&i, argc, argv, &on) != CETCD_OK) {
                fprintf(stderr, "--interactive must be true or false\n");
                return 1;
            }
            interactive = on != 0;
        } else if (cmd_flag_is_(argv[i], "--prefix")) {
            if (cetcd_take_cli_bool_eq(&i, argc, argv, &on) != CETCD_OK) {
                fprintf(stderr, "--prefix must be true or false\n");
                return 1;
            }
            prefix = on != 0;
        } else if (cmd_flag_is_(argv[i], "--range-end")) {
            if (take_cmd_value_(&i, argc, argv, &range_end_arg) != 0) {
                fprintf(stderr, "--range-end requires a key\n");
                return 1;
            }
        } else if (cmd_flag_is_(argv[i], "--prev-kv")) {
            if (cetcd_take_cli_bool_eq(&i, argc, argv, &on) != CETCD_OK) {
                fprintf(stderr, "--prev-kv must be true or false\n");
                return 1;
            }
            prev_kv = on != 0;
        } else if (cmd_flag_is_(argv[i], "--progress-notify")) {
            if (cetcd_take_cli_bool_eq(&i, argc, argv, &on) != CETCD_OK) {
                fprintf(stderr, "--progress-notify must be true or false\n");
                return 1;
            }
            progress_notify = on != 0;
        } else if (cmd_flag_is_(argv[i], "--fragment")) {
            if (cetcd_take_cli_bool_eq(&i, argc, argv, &on) != CETCD_OK) {
                fprintf(stderr, "--fragment must be true or false\n");
                return 1;
            }
            fragment = on != 0;
        } else if (cmd_flag_is_(argv[i], "--hex")) {
            if (cetcd_take_cli_bool_eq(&i, argc, argv, &on) != CETCD_OK) {
                fprintf(stderr, "--hex must be true or false\n");
                return 1;
            }
            hex_output = on != 0;
        } else if (cmd_flag_is_(argv[i], "--start-rev") || cmd_flag_is_(argv[i], "--rev")) {
            const char *s = NULL;
            if (take_cmd_value_(&i, argc, argv, &s) != 0) {
                fprintf(stderr, "--start-rev requires a revision number\n");
                return 1;
            }
            if (parse_i64_(s, &start_rev) != 0 || start_rev < 0) {
                fprintf(stderr, "--start-rev must be >= 0\n");
                return 1;
            }
        } else if (cmd_flag_is_(argv[i], "--filter")) {
            const char *ft = NULL;
            if (take_cmd_value_(&i, argc, argv, &ft) != 0) {
                fprintf(stderr, "--filter requires a type (NOPUT or NODELETE)\n");
                return 1;
            }
            if (strcmp(ft, "NOPUT") == 0) filter_type = 0;
            else if (strcmp(ft, "NODELETE") == 0) filter_type = 1;
            else { fprintf(stderr, "--filter must be NOPUT or NODELETE\n"); return 1; }
        } else if (cmd_flag_is_(argv[i], "--exec")) {
            if (take_cmd_value_(&i, argc, argv, &exec_cmd) != 0) {
                fprintf(stderr, "--exec requires a command\n");
                return 1;
            }
        } else if ((wr = take_write_out_jf_(&i, argc, argv, &wj, &wf)) != 0) {
            if (wr < 0) { fprintf(stderr, "--write-out requires a format\n"); return 1; }
            want_json = wj != 0;
            want_fields = wf != 0;
        } else if (strcmp(argv[i], "--") == 0) {
            if (i + 1 < argc && !key) key = argv[++i];
            else {
                fprintf(stderr, "unknown flag: %s\n", argv[i]);
                return 1;
            }
        } else if (argv[i][0] == '-') {
            fprintf(stderr, "unknown flag: %s\n", argv[i]);
            return 1;
        } else if (!key) {
            key = argv[i];
        } else {
            fprintf(stderr, "unknown flag: %s\n", argv[i]);
            return 1;
        }
    }
    if (!interactive && !key) { fprintf(stderr, "usage: cetcdctl watch [-i] [--prefix] [--range-end KEY] [--prev-kv] [--progress-notify] [--fragment] [--start-rev N] [--filter TYPE] [--hex] [--exec CMD] [-w json|fields] KEY\n"); return 1; }
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
                                         start_rev, prev_kv, filter_type,
                                         progress_notify, fragment);
        if (wpos == 0) { fprintf(stderr, "key too long\n"); conn_close(fd); return 1; }
        if (send_request(fd, "/etcdserverpb.Watch/Watch", watch_buf, wpos) != 0) {
            fprintf(stderr, "send failed\n"); conn_close(fd); return 1;
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
                    if (!wkey) { fprintf(stderr, "usage: watch KEY [--prefix] [--prev-kv] [--progress-notify] [--fragment] [--start-rev N]\n"); continue; }
                    bool wprefix = false, wprev_kv = false, wprogress_notify = false, wfragment = false;
                    int64_t wstart_rev = 0;
                    int wrev_ok = 1;
                    char *tok;
                    while ((tok = strtok(NULL, " \t")) != NULL) {
                        if (strcmp(tok, "--prefix") == 0) wprefix = true;
                        else if (strcmp(tok, "--prev-kv") == 0) wprev_kv = true;
                        else if (strcmp(tok, "--progress-notify") == 0) wprogress_notify = true;
                        else if (strcmp(tok, "--fragment") == 0) wfragment = true;
                        else if (strncmp(tok, "--start-rev=", 12) == 0) {
                            if (parse_i64_(tok + 12, &wstart_rev) != 0 || wstart_rev < 0) {
                                fprintf(stderr, "--start-rev must be >= 0\n");
                                wrev_ok = 0;
                                break;
                            }
                        } else if (strncmp(tok, "--rev=", 6) == 0) {
                            if (parse_i64_(tok + 6, &wstart_rev) != 0 || wstart_rev < 0) {
                                fprintf(stderr, "--start-rev must be >= 0\n");
                                wrev_ok = 0;
                                break;
                            }
                        } else if (strcmp(tok, "--start-rev") == 0 || strcmp(tok, "--rev") == 0) {
                            char *sr = strtok(NULL, " \t");
                            if (!sr || parse_i64_(sr, &wstart_rev) != 0 || wstart_rev < 0) {
                                fprintf(stderr, "--start-rev must be >= 0\n");
                                wrev_ok = 0;
                                break;
                            }
                        }
                    }
                    if (!wrev_ok) continue;
                    size_t wklen = strlen(wkey);
                    uint8_t wbuf[1024];
                    size_t wp = build_watch_create(wbuf, sizeof(wbuf), wkey, wklen, wprefix, NULL, wstart_rev, wprev_kv, -1, wprogress_notify, wfragment);
                    if (wp > 0) { send_request(fd, "/etcdserverpb.Watch/Watch", wbuf, wp); fprintf(stderr, "watch created for key '%s'\n", wkey); }
                } else if (strcmp(cmd, "cancel") == 0) {
                    char *id_str = strtok(NULL, " \t");
                    if (!id_str) { fprintf(stderr, "usage: cancel WATCH_ID\n"); continue; }
                    int64_t wid = 0;
                    if (parse_i64_(id_str, &wid) != 0 || wid < 1) {
                        fprintf(stderr, "cancel WATCH_ID must be > 0\n");
                        continue;
                    }
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
    conn_close(fd);
    return 0;
}

static int cmd_hash(int argc, char **argv) {
    bool want_json = false, want_fields = false, want_table = false;
    for (int i = 2; i < argc; i++) {
        int wr = 0, sk = 0, wj = 0, wt = 0, wf = 0;
        if ((wr = take_write_out_jtf_(&i, argc, argv, &wj, &wt, &wf)) != 0) {
            if (wr < 0) { fprintf(stderr, "--write-out requires a format\n"); return 1; }
            want_json = wj != 0; want_table = wt != 0; want_fields = wf != 0;
        }
    }
    if (cetcd_ctl_parse_maint_argv(argc, argv, 2, 0, NULL) != CETCD_OK) {
        print_unknown_maint_flag_(argc, argv, 2, 0);
        return 1;
    }
    uint8_t req[] = {0x00}, resp[256];
    int rlen = do_rpc("/etcdserverpb.Maintenance/Hash", req, 1, resp, sizeof(resp));
    if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
    uint32_t hash32 = 0;
    if (cetcd_parse_hash_response(resp, (size_t)rlen, &hash32, NULL)
        != CETCD_OK) {
        fprintf(stderr, "request failed\n");
        return 1;
    }
    uint64_t hash_val = hash32;
    if (want_json) {
        fputs("{", stdout);
        parse_and_print_header_json(resp, (size_t)rlen);
        printf(",\"hash\":%llu}\n", (unsigned long long)hash_val);
    } else if (want_fields) {
        parse_and_print_header_json(resp, (size_t)rlen);
        printf("hash: %llu\n", (unsigned long long)hash_val);
        fputs("\n", stdout);
    } else if (want_table) {
        printf("+------------------+\n");
        printf("|       HASH       |\n");
        printf("+------------------+\n");
        printf("| %016llu |\n", (unsigned long long)hash_val);
        printf("+------------------+\n");
    } else {
        printf("hash: %llu\n", (unsigned long long)hash_val);
    }
    return 0;
}

static int cmd_hashkv(int argc, char **argv) {
    bool want_json = false;
    bool want_fields = false, want_table = false;
    int64_t rev = 0;
    for (int i = 2; i < argc; i++) {
        int wr = 0, sk = 0, wj = 0, wt = 0, wf = 0;
        if ((wr = take_write_out_jtf_(&i, argc, argv, &wj, &wt, &wf)) != 0) {
            if (wr < 0) { fprintf(stderr, "--write-out requires a format\n"); return 1; }
            want_json = wj != 0; want_table = wt != 0; want_fields = wf != 0;
        } else if (cmd_flag_is_(argv[i], "--rev")) {
            if (take_hashkv_rev_(&i, argc, argv, &rev) != 0) {
                fprintf(stderr, "--rev must be >= 0\n");
                return 1;
            }
        } else {
            fprintf(stderr, "unknown flag: %s\n", argv[i]);
            return 1;
        }
    }
    uint8_t req[16], resp[256];
    size_t req_n = 0;
    if (cetcd_encode_hashkv_request(rev, req, sizeof(req), &req_n) != CETCD_OK) {
        fprintf(stderr, "--rev must be >= 0\n");
        return 1;
    }
    int rlen = do_rpc("/etcdserverpb.Maintenance/HashKV", req, req_n, resp, sizeof(resp));
    if (rlen < 0) { fprintf(stderr, "request failed\n"); return 1; }
    uint32_t hash32 = 0;
    int64_t compact_i = 0;
    if (cetcd_parse_hash_response(resp, (size_t)rlen, &hash32, &compact_i)
        != CETCD_OK) {
        fprintf(stderr, "request failed\n");
        return 1;
    }
    uint64_t hash_val = hash32;
    uint64_t compact_rev = (uint64_t)compact_i;
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
    } else if (want_table) {
        printf("+------------------+--------------------+\n");
        printf("|       HASH       |  COMPACT_REV       |\n");
        printf("+------------------+--------------------+\n");
        printf("| %016llu | %18llu |\n", (unsigned long long)hash_val, (unsigned long long)compact_rev);
        printf("+------------------+--------------------+\n");
    } else {
        printf("hash: %llu\n", (unsigned long long)hash_val);
        printf("compact_revision: %llu\n", (unsigned long long)compact_rev);
    }
    return 0;
}

static int defrag_one_(int want_json, int want_fields, int with_endpoint) {
    uint8_t req[] = {0x00}, resp[256];
    int rlen = do_rpc("/etcdserverpb.Maintenance/Defragment", req, 1, resp, sizeof(resp));
    if (rlen < 0) return -1;
    if (with_endpoint) {
        if (want_json) {
            printf("{\"endpoint\":\"%s\",", ep_str_());
            parse_and_print_header_json(resp, (size_t)rlen);
            fputs("}\n", stdout);
        } else if (want_fields) {
            printf("endpoint: %s\n", ep_str_());
            parse_and_print_header_json(resp, (size_t)rlen);
            fputs("\n", stdout);
        } else {
            printf("Finished defragmenting etcd member[%s]\n", ep_str_());
        }
    } else if (want_json) {
        fputs("{", stdout); parse_and_print_header_json(resp, (size_t)rlen); fputs("}\n", stdout);
    } else if (want_fields) {
        parse_and_print_header_json(resp, (size_t)rlen);
        fputs("\n", stdout);
    } else {
        printf("OK\n");
    }
    return 0;
}

static int cmd_defrag(int argc, char **argv) {
    bool want_json = false;
    bool want_fields = false;
    int cluster = 0;
    const char *data_dir = NULL;
    for (int i = 2; i < argc; i++) {
        int wr = 0, sk = 0, wj = 0, wt = 0, wf = 0;
        if ((wr = take_write_out_jf_(&i, argc, argv, &wj, &wf)) != 0) {
            if (wr < 0) { fprintf(stderr, "--write-out requires a format\n"); return 1; }
            want_json = wj != 0; want_fields = wf != 0;
        }
    }
    if (cetcd_ctl_parse_defrag_argv(argc, argv, 2, &cluster, &data_dir) != CETCD_OK) {
        fprintf(stderr,
                "unknown leftover flag (defrag --data-dir --cluster cannot eat a flag as the path)\n");
        return 1;
    }
    if (data_dir) {
        if (cetcd_backend_defrag_dir(data_dir) != CETCD_OK) {
            fprintf(stderr, "failed to defragment %s\n", data_dir);
            return 1;
        }
        if (want_json) {
            printf("{\"data_dir\":\"%s\"}\n", data_dir);
        } else if (want_fields) {
            printf("data_dir: %s\n\n", data_dir);
        } else {
            printf("Finished defragmenting etcd data[%s]\n", data_dir);
        }
        return 0;
    }
    if (cluster) {
        struct cluster_endpoint eps[32];
        int n = collect_cluster_endpoints(eps, 32);
        int failed = 0;
        const char *orig_host;
        uint16_t orig_port;
        if (n <= 0) { fprintf(stderr, "failed to get member list\n"); return 1; }
        orig_host = g_host;
        orig_port = g_port;
        for (int i = 0; i < n; i++) {
            g_host = eps[i].host;
            g_port = eps[i].port;
            if (defrag_one_(want_json, want_fields, 1) < 0) {
                fprintf(stderr, "Failed to defragment etcd member[%s]\n", ep_str_());
                failed = 1;
            }
        }
        g_host = orig_host;
        g_port = orig_port;
        return failed;
    }
    if (defrag_one_(want_json, want_fields, 0) < 0) {
        fprintf(stderr, "request failed\n");
        return 1;
    }
    return 0;
}

static int cmd_move_leader(int argc, char **argv) {
    bool want_json = false;
    bool want_fields = false;
    const char *target_str = NULL;
    for (int i = 2; i < argc; i++) {
        int wr = 0, sk = 0, wj = 0, wt = 0, wf = 0;
        if ((wr = take_write_out_jf_(&i, argc, argv, &wj, &wf)) != 0) {
            if (wr < 0) { fprintf(stderr, "--write-out requires a format\n"); return 1; }
            want_json = wj != 0; want_fields = wf != 0;
        } else if (argv[i][0] == '-') {
            fprintf(stderr, "unknown flag: %s\n", argv[i]);
            return 1;
        } else if (!target_str) {
            target_str = argv[i];
        } else {
            fprintf(stderr, "unknown flag: %s\n", argv[i]);
            return 1;
        }
    }
    if (!target_str) { fprintf(stderr, "usage: cetcdctl move-leader [-w json|fields] TARGET_ID\n"); return 1; }
    uint64_t target = 0;
    if (parse_positive_hex_u64_(target_str, &target) != 0) {
        fprintf(stderr, "move-leader TARGET_ID must be a hex integer > 0\n");
        return 1;
    }
    uint8_t req[32], resp[256];
    size_t pos = 0;
    pos = encode_varint_field(req, sizeof(req), pos, 0x08, target);
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
    /* leftover-safe: leftover cannot steal a used token */
    if (cetcd_parse_authenticate_response(resp, (size_t)rlen,
                                          g_auth_token, sizeof(g_auth_token))
        != CETCD_OK) {
        fprintf(stderr, "authentication request failed\n");
        return -1;
    }
    if (g_debug) {
        fprintf(stderr, "[debug] authenticated, token=%s\n", g_auth_token);
    }
    return 0;
}

static int cmd_completion(int argc, char **argv) {
    const char *shell = NULL;
    if (cetcd_ctl_parse_completion_argv(argc, argv, 2, &shell) != CETCD_OK) {
        fprintf(stderr, "unknown leftover flag (completion bash --foo cannot dump a script)\n");
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
        printf("    local gopts=\"--host --port --endpoints --endpoint --user --password --command-timeout --debug --insecure --insecure-skip-tls-verify --insecure-transport --dial-timeout --keepalive-time --keepalive-timeout --cacert --cert --key --max-call-send-msg-size --max-call-recv-msg-size --discovery-srv --discovery-srv-name\"\n");
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
        printf("        watch) local opts=\"-i --interactive --prefix --range-end --prev-kv --progress-notify --fragment --start-rev --filter --hex --exec -w --write-out\";;\n");
        printf("        lease) local opts=\"--lease-id --keys --once --interval -w --write-out\"\n");
        printf("              local subs=\"grant revoke timetolive list keepalive\";;\n");
        printf("        txn) local opts=\"-w --write-out\"; local subs=\"-i put cas get del\";;\n");
        printf("        compact) local opts=\"--physical -w --write-out\";;\n");
        printf("        status) local opts=\"-w --write-out\";;\n");
        printf("        hash) local opts=\"-w --write-out\";;\n");
        printf("        defrag) local opts=\"--cluster -w --write-out\";;\n");
        printf("        alarm) local opts=\"-w --write-out\"; local subs=\"list activate disarm\";;\n");
        printf("        member) local opts=\"--peer-urls --name --learner -w --write-out\"; local subs=\"list add remove update promote\";;\n");
        printf("        auth) local opts=\"-w --write-out\"; local subs=\"enable disable status login\";;\n");
        printf("        user) local opts=\"-w --write-out\"; local subs=\"add delete get list change-password grant-role revoke-role\";;\n");
        printf("        role) local opts=\"--prefix --range-end -w --write-out\"; local subs=\"add delete get list grant-permission revoke-permission\";;\n");
        printf("        snapshot) local opts=\"--compaction-periodical --data-dir --force -w --write-out\"; local subs=\"save status restore\";;\n");
        printf("        downgrade) local opts=\"-w --write-out\"; local subs=\"enable cancel validate\";;\n");
        printf("        hashkv) local opts=\"--rev -w --write-out\";;\n");
        printf("        endpoint) local opts=\"--cluster --rev -w --write-out\"; local subs=\"health status hashkv\";;\n");
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
        printf("        '--max-call-send-msg-size[Max send size]:bytes' \\\n");
        printf("        '--max-call-recv-msg-size[Max recv size]:bytes' \\\n");
        printf("        '--insecure-skip-tls-verify[Skip TLS verify]' \\\n");
        printf("        '--insecure-transport[Disable TLS transport]' \\\n");
        printf("        '--password[Password]:pass' \\\n");
        printf("        '--discovery-srv[Discovery SRV]:domain' \\\n");
        printf("        '--discovery-srv-name[Discovery SRV name]:name' \\\n");
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
        printf("complete -c cetcdctl -n \"__fish_use_subcommand\" -l max-call-send-msg-size -d 'Max gRPC send size'\n");
        printf("complete -c cetcdctl -n \"__fish_use_subcommand\" -l max-call-recv-msg-size -d 'Max gRPC recv size'\n");
        printf("complete -c cetcdctl -n \"__fish_use_subcommand\" -l insecure-skip-tls-verify -d 'Skip TLS verify'\n");
        printf("complete -c cetcdctl -n \"__fish_use_subcommand\" -l insecure-transport -d 'Disable TLS transport'\n");
        printf("complete -c cetcdctl -n \"__fish_use_subcommand\" -l password -d 'Password for --user'\n");
        printf("complete -c cetcdctl -n \"__fish_use_subcommand\" -l discovery-srv -d 'Discovery service'\n");
        printf("complete -c cetcdctl -n \"__fish_use_subcommand\" -l discovery-srv-name -d 'Discovery SRV name'\n");
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
        printf("complete -c cetcdctl -n '___fish_seen_subcommand_from endpoint' -l cluster -l rev\n");
        printf("complete -c cetcdctl -n '___fish_seen_subcommand_from hashkv' -l rev\n");
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
        printf("complete -c cetcdctl -n '___fish_seen_subcommand_from defrag' -l cluster -s w -l write-out\n");
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
    printf("  --port PORT    Server port (default: 2379; 1..65535)\n");
    printf("  --endpoints EP Comma-separated endpoints (failover; https requires --cacert or --insecure; port 1..65535)\n");
    printf("  --user USER:PASS  Authenticate with server before executing command\n");
    printf("  --command-timeout SEC  Timeout for commands (duration or --flag=SEC; 0 = none; leftover fails)\n");
    printf("  --debug       Print debug info (RPC path and response size)\n");
    printf("  --insecure    Skip TLS certificate verification (with --cacert/--cert)\n");
    printf("  --dial-timeout SEC  Connection timeout (0..86400; 0 = none; invalid fails)\n");
    printf("  --keepalive-time SEC    TCP keepalive idle (0 disables; requires 0..86400)\n");
    printf("  --keepalive-timeout SEC  TCP keepalive interval (requires --keepalive-time)\n");
    printf("  --cacert FILE   TLS CA certificate (enables TLS; missing file fail-closes)\n");
    printf("  --cert FILE     TLS client certificate (requires --key)\n");
    printf("  --key FILE      TLS client key (requires --cert)\n");
    printf("  --max-call-send-msg-size N  Max request payload (bytes; 0 rejected)\n");
    printf("  --max-call-recv-msg-size N  Max response payload (bytes; 0 rejected)\n");
    printf("  --insecure-skip-tls-verify  Same as --insecure\n");
    printf("  --insecure-transport  Force plaintext even if TLS flags are set (fail-closed if mixed)\n");
    printf("  --password PASS  Password for --user authentication\n");
    printf("  --discovery-srv DOMAIN  DNS SRV (_etcd-client._tcp); fail-closed on 0 records\n");
    printf("  --discovery-srv-name NAME  Optional SRV service suffix\n\n");
    printf("Commands:\n");
    printf("  put [--prev-kv] [--ignore-value] [--ignore-lease] [--lease ID] [--print-value-only] [-w json|fields] KEY [VALUE|-]  Store a key-value pair\n");
    printf("  get [--prefix] [--from-key] [--range-end KEY] [--keys-only] [--count-only] [--print-value-only] [--hex] [--consistency l|s] [-w json|fields|table] [--rev N] [--limit N] [--sort-by FIELD] [--sort-order ORDER] [--min-mod-rev N] [--max-mod-rev N] [--min-create-rev N] [--max-create-rev N] KEY [RANGE_END]\n");
    printf("                         Retrieve keys (sort-by: key|version|create|mod|value; sort-order: ascend|descend)\n");
    printf("  del [--prefix] [--from-key] [--range-end KEY] [--prev-kv] [--hex] [--print-value-only] [-w json|fields] KEY [RANGE_END]  Delete a key (options: --prefix, --from-key, --range-end, --prev-kv, --hex, --print-value-only)\n");
    printf("  watch [-i] [--prefix] [--range-end KEY] [--prev-kv] [--progress-notify] [--fragment] [--start-rev N] [--filter NOPUT|NODELETE] [--hex] [--exec CMD] [-w json|fields] KEY  Watch key changes (-i for interactive mode, --progress-notify for periodic progress updates, --fragment splits oversized WatchResponses by --max-request-bytes, --exec runs CMD with ETCD_WATCH_* env vars; --start-rev >= 0; leftover --flags fail-close)\n");
    printf("  lease grant [--lease-id ID] [-w json|fields] TTL  Grant a lease (TTL > 0; --lease-id hex)\n");
    printf("  lease revoke [-w json|fields] ID  Revoke a lease by ID (> 0)\n");
    printf("  lease timetolive [--keys] [-w json|fields] ID  Query remaining TTL (ID > 0)\n");
    printf("  lease list [-w table|json|fields]  List all active leases\n");
    printf("  lease keepalive [--once] [--interval SEC] [-w json|fields] ID  Keep a lease alive (loop by default, --once for single, --interval > 0)\n");
    printf("  txn -i [-w json|fields]  Interactive transaction (read from stdin: cmp/put/get/del/then/else)\n");
    printf("  txn put [-w json|fields] KEY VALUE  Execute a transaction (Put; leftover --flags fail-close)\n");
    printf("  txn cas [-w json|fields] KEY EXP NEW  Compare-and-swap (if KEY==EXP then KEY=NEW; leftover --flags fail-close)\n");
    printf("  txn get [-w json|fields] KEY [RANGE_END]  Execute a transaction (Range; leftover --flags fail-close)\n");
    printf("  txn del [-w json|fields] [--prefix] [--prev-kv] KEY [RANGE_END]  Execute a transaction (Delete)\n");
    printf("  compact [--physical] [-w json|fields] REV  Compact MVCC history to revision (REV > 0; leftover flags fail-close)\n");
    printf("  status [-w json|fields]  Get server status (unknown leftover flags fail-close)\n");
    printf("  alarm list [-w table|json|fields]  List all alarms (leftover --flags fail-close)\n");
    printf("  alarm activate [-w json|fields] [TYPE]  Activate an alarm (NOSPACE|CORRUPT|NONE; leftover --flags fail-close)\n");
    printf("  alarm disarm [-w json|fields] [TYPE]     Disarm an alarm (NOSPACE|CORRUPT|NONE; leftover --flags fail-close)\n");
    printf("  hash [-w json|fields|table]         Get KV store hash (unknown leftover flags fail-close)\n");
    printf("  hashkv [--rev N] [-w json|fields|table]  Get KV store hash + compact revision (N leftover-safe; 0 = current)\n");
    printf("  defrag [--cluster] [-w json|fields]  Defragment database (compact-copy; --cluster uses MemberList)\n");
    printf("  move-leader [-w json|fields] TARGET_ID  Transfer leadership to target node (ID hex > 0)\n");
    printf("  member list [-w json|table|fields]  List cluster members (leftover --flags fail-close)\n");
    printf("  member add [-w json|fields] [--peer-urls URLS] [--name NAME] [--learner] [PEER_URL]  Add a cluster member (comma-separated URLs supported; leftover --flags fail-close)\n");
    printf("  member remove [-w json|fields] ID    Remove a cluster member (ID hex > 0; leftover --flags fail-close)\n");
    printf("  member update [-w json|fields] ID PEER_URLS  Update a member's peer URLs (ID hex > 0; comma-separated supported; leftover --flags fail-close)\n");
    printf("  member promote [-w json|fields] ID    Promote a member to voting member (ID hex > 0; leftover --flags fail-close)\n");
    printf("  auth enable [-w json|fields]     Enable authentication (leftover --flags fail-close)\n");
    printf("  auth disable [-w json|fields]     Disable authentication (leftover --flags fail-close)\n");
    printf("  auth status [-w json|fields]     Query auth status (leftover --flags fail-close)\n");
    printf("  auth login NAME PASS [-w json|fields]   Authenticate and get token (leftover --flags fail-close)\n");
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
    printf("  snapshot save [FILE] [--compaction-periodical] [-w json|fields|table]   Save a snapshot to file (leftover --flags fail-close)\n");
    printf("  snapshot status FILE [-w json|fields|table]  Show snapshot file info\n");
    printf("  snapshot restore FILE --data-dir DIR [--wal-dir DIR] [--bump-revision] [--mark-compacted] [--force] [--skip-hash-check] [--initial-cluster-token TOKEN] [--initial-cluster-state new|existing] [--initial-cluster SPEC] [--name NAME] [--initial-advertise-peer-urls URL] [-w json|fields]  Restore snapshot to data dir (leftover --data-dir --wal-dir fail-close)\n");
    printf("  downgrade enable [-w json|fields] VER   Enable cluster downgrade (leftover --flags fail-close)\n");
    printf("  downgrade cancel [-w json|fields]       Cancel cluster downgrade (leftover --flags fail-close)\n");
    printf("  downgrade validate [-w json|fields] VER Validate downgrade version (leftover --flags fail-close)\n");
    printf("  version [-w json|fields]      Print the client version (leftover --flags fail-close)\n");
    printf("  endpoint health [--cluster] [-w json|fields|table]  Check server health (or all cluster members with --cluster)\n");
    printf("  endpoint status [--cluster] [-w json|table|fields]  Get server status (or all cluster members with --cluster)\n");
    printf("  endpoint hashkv [--cluster] [--rev N] [-w json|table|fields]  Get KV hash per endpoint (N leftover-safe; 0 = current)\n");
    printf("  check perf [--load S|M|L] [--prefix PREFIX] [-w json|fields]    Run a simple performance check (leftover --flags fail-close)\n");
    printf("  check datascale [-w json|fields] [--load N] [--prefix PREFIX]  Test database scalability (--load > 0; leftover --flags fail-close)\n");
    printf("  lock [--ttl N] [--print-value-only] [-w json|fields] LOCKNAME [CMD...]  Acquire a distributed lock (--ttl > 0; leftover --flags before LOCKNAME fail-close)\n");
    printf("  elect [--ttl N] [--print-value-only] [-w json|fields] ELECTION_NAME [PROPOSAL]  Leader election (--ttl > 0; leftover --flags fail-close)\n");
    printf("  completion bash|zsh|fish   Generate shell completion script (leftover --flags fail-close)\n");
}

static int take_ctl_value_(int *i, int argc, char **argv, const char **out) {
    return cetcd_take_cli_flag_value(i, argc, argv, out) == CETCD_OK ? 0 : -1;
}

static int parse_sec_range_(const char *s, long min, long max, long *out) {
    char *end = NULL;
    errno = 0;
    long v = strtol(s, &end, 10);
    if (errno == ERANGE || !end || end == s || v < min || v > max)
        return -1;
    if (*end == 's') end++;
    if (*end) return -1;
    *out = v;
    return 0;
}

int main(int argc, char **argv) {
    /* Parse global options */
    int cmd_start = 1;
    const char *user_cred = NULL;
    while (cmd_start < argc) {
        if (cetcd_cli_flag_is(argv[cmd_start], "--host")) {
            const char *s = NULL;
            if (take_ctl_value_(&cmd_start, argc, argv, &s) != 0) {
                fprintf(stderr, "--host requires a value\n");
                return 1;
            }
            g_host = s;
            cmd_start += 1;
        } else if (cetcd_cli_flag_is(argv[cmd_start], "--port")) {
            const char *s = NULL;
            if (take_ctl_value_(&cmd_start, argc, argv, &s) != 0) {
                fprintf(stderr, "--port requires a value\n");
                return 1;
            }
            char *end = NULL;
            errno = 0;
            long v = strtol(s, &end, 10);
            if (errno == ERANGE || !end || *end || v < 1 || v > 65535) {
                fprintf(stderr, "--port must be 1..65535\n");
                return 1;
            }
            g_port = (uint16_t)v;
            cmd_start += 1;
        } else if (cetcd_cli_flag_is(argv[cmd_start], "--endpoints") ||
                   cetcd_cli_flag_is(argv[cmd_start], "--endpoint")) {
            const char *s = NULL;
            if (take_ctl_value_(&cmd_start, argc, argv, &s) != 0) {
                fprintf(stderr, "--endpoints requires a value\n");
                return 1;
            }
            size_t n = 0;
            int erc = cetcd_endpoint_parse_list(s, g_eps,
                                                CETCD_DISCOVERY_MAX_ENDPOINTS, &n);
            if (erc == CETCD_ERR_UNSUPPORT) {
                fprintf(stderr, "--endpoints unix:// is not supported\n");
                return 1;
            }
            if (erc != 0) {
                fprintf(stderr, "--endpoints is invalid (host:port 1..65535, comma list)\n");
                return 1;
            }
            g_n_eps = n;
            g_endpoints_set = 1;
            g_host = g_eps[0].host;
            g_port = g_eps[0].port;
            g_endpoint_https = 0;
            for (size_t i = 0; i < n; i++) {
                if (g_eps[i].https) g_endpoint_https = 1;
            }
            cmd_start += 1;
        } else if (cetcd_cli_flag_is(argv[cmd_start], "--command-timeout")) {
            const char *ts = NULL;
            if (take_ctl_value_(&cmd_start, argc, argv, &ts) != 0) {
                fprintf(stderr, "--command-timeout requires a duration\n");
                return 1;
            }
            uint64_t timeout_sec = 0;
            if (cetcd_parse_command_timeout_sec(ts, &timeout_sec) != CETCD_OK) {
                fprintf(stderr, "--command-timeout must be a duration\n");
                return 1;
            }
            if (timeout_sec > 0) {
                signal(SIGALRM, (void (*)(int))_exit);
                alarm((unsigned)timeout_sec);
            }
            cmd_start += 1;
        } else if (cetcd_cli_flag_is(argv[cmd_start], "--debug")) {
            int on = 1;
            if (cetcd_take_cli_bool_eq(&cmd_start, argc, argv, &on) != CETCD_OK) {
                fprintf(stderr, "--debug must be true or false\n");
                return 1;
            }
            g_debug = on;
            cmd_start += 1;
        } else if (cetcd_cli_flag_is(argv[cmd_start], "--insecure-skip-tls-verify")) {
            int on = 1;
            if (cetcd_take_cli_bool_eq(&cmd_start, argc, argv, &on) != CETCD_OK) {
                fprintf(stderr, "--insecure-skip-tls-verify must be true or false\n");
                return 1;
            }
            g_insecure = on;
            cmd_start += 1;
        } else if (cetcd_cli_flag_is(argv[cmd_start], "--insecure-transport")) {
            int on = 1;
            if (cetcd_take_cli_bool_eq(&cmd_start, argc, argv, &on) != CETCD_OK) {
                fprintf(stderr, "--insecure-transport must be true or false\n");
                return 1;
            }
            g_insecure_transport = on;
            cmd_start += 1;
        } else if (cetcd_cli_flag_is(argv[cmd_start], "--insecure")) {
            int on = 1;
            if (cetcd_take_cli_bool_eq(&cmd_start, argc, argv, &on) != CETCD_OK) {
                fprintf(stderr, "--insecure must be true or false\n");
                return 1;
            }
            g_insecure = on;
            cmd_start += 1;
        } else if (cetcd_cli_flag_is(argv[cmd_start], "--dial-timeout")) {
            const char *s = NULL;
            if (take_ctl_value_(&cmd_start, argc, argv, &s) != 0) {
                fprintf(stderr, "--dial-timeout requires a value\n");
                return 1;
            }
            long v = 0;
            if (parse_sec_range_(s, 0, 86400, &v) != 0) {
                fprintf(stderr, "--dial-timeout must be 0..86400 seconds\n");
                return 1;
            }
            g_dial_timeout = (int)v;
            cmd_start += 1;
        } else if (cetcd_cli_flag_is(argv[cmd_start], "--keepalive-time")) {
            const char *s = NULL;
            if (take_ctl_value_(&cmd_start, argc, argv, &s) != 0) {
                fprintf(stderr, "--keepalive-time requires a value\n");
                return 1;
            }
            long v = 0;
            if (parse_sec_range_(s, 0, 86400, &v) != 0) {
                fprintf(stderr, "--keepalive-time must be 0..86400 seconds\n");
                return 1;
            }
            g_tcp_keepalive_time = (int)v;
            cmd_start += 1;
        } else if (cetcd_cli_flag_is(argv[cmd_start], "--keepalive-timeout")) {
            const char *s = NULL;
            if (take_ctl_value_(&cmd_start, argc, argv, &s) != 0) {
                fprintf(stderr, "--keepalive-timeout requires a value\n");
                return 1;
            }
            long v = 0;
            if (parse_sec_range_(s, 1, 86400, &v) != 0) {
                fprintf(stderr, "--keepalive-timeout must be 1..86400 seconds\n");
                return 1;
            }
            g_tcp_keepalive_timeout = (int)v;
            cmd_start += 1;
        } else if (cetcd_cli_flag_is(argv[cmd_start], "--cacert")) {
            const char *s = NULL;
            if (take_ctl_value_(&cmd_start, argc, argv, &s) != 0) {
                fprintf(stderr, "--cacert requires a file\n");
                return 1;
            }
            strncpy(g_cacert, s, sizeof(g_cacert) - 1);
            g_cacert[sizeof(g_cacert) - 1] = '\0';
            cmd_start += 1;
        } else if (cetcd_cli_flag_is(argv[cmd_start], "--cert")) {
            const char *s = NULL;
            if (take_ctl_value_(&cmd_start, argc, argv, &s) != 0) {
                fprintf(stderr, "--cert requires a file\n");
                return 1;
            }
            strncpy(g_cert, s, sizeof(g_cert) - 1);
            g_cert[sizeof(g_cert) - 1] = '\0';
            cmd_start += 1;
        } else if (cetcd_cli_flag_is(argv[cmd_start], "--key")) {
            const char *s = NULL;
            if (take_ctl_value_(&cmd_start, argc, argv, &s) != 0) {
                fprintf(stderr, "--key requires a file\n");
                return 1;
            }
            strncpy(g_key, s, sizeof(g_key) - 1);
            g_key[sizeof(g_key) - 1] = '\0';
            cmd_start += 1;
        } else if (cetcd_cli_flag_is(argv[cmd_start], "--max-call-send-msg-size")) {
            const char *s = NULL;
            if (take_ctl_value_(&cmd_start, argc, argv, &s) != 0) {
                fprintf(stderr, "--max-call-send-msg-size requires a value\n");
                return 1;
            }
            char *end = NULL;
            errno = 0;
            unsigned long long v = strtoull(s, &end, 10);
            if (errno == ERANGE || !end || *end || v == 0) {
                fprintf(stderr, "--max-call-send-msg-size must be > 0\n");
                return 1;
            }
            g_max_call_send = (uint64_t)v;
            cmd_start += 1;
        } else if (cetcd_cli_flag_is(argv[cmd_start], "--max-call-recv-msg-size")) {
            const char *s = NULL;
            if (take_ctl_value_(&cmd_start, argc, argv, &s) != 0) {
                fprintf(stderr, "--max-call-recv-msg-size requires a value\n");
                return 1;
            }
            char *end = NULL;
            errno = 0;
            unsigned long long v = strtoull(s, &end, 10);
            if (errno == ERANGE || !end || *end || v == 0) {
                fprintf(stderr, "--max-call-recv-msg-size must be > 0\n");
                return 1;
            }
            g_max_call_recv = (uint64_t)v;
            cmd_start += 1;
        } else if (cetcd_cli_flag_is(argv[cmd_start], "--password")) {
            const char *s = NULL;
            if (take_ctl_value_(&cmd_start, argc, argv, &s) != 0) {
                fprintf(stderr, "--password requires a value\n");
                return 1;
            }
            strncpy(g_password, s, sizeof(g_password) - 1);
            g_password[sizeof(g_password) - 1] = '\0';
            cmd_start += 1;
        } else if (cetcd_cli_flag_is(argv[cmd_start], "--discovery-srv-name")) {
            const char *s = NULL;
            if (take_ctl_value_(&cmd_start, argc, argv, &s) != 0) {
                fprintf(stderr, "--discovery-srv-name requires a value\n");
                return 1;
            }
            g_discovery_srv_name = s;
            if (cetcd_discovery_valid_name(g_discovery_srv_name) != 0 ||
                !g_discovery_srv_name[0]) {
                fprintf(stderr, "--discovery-srv-name is invalid\n");
                return 1;
            }
            cmd_start += 1;
        } else if (cetcd_cli_flag_is(argv[cmd_start], "--discovery-srv")) {
            const char *s = NULL;
            if (take_ctl_value_(&cmd_start, argc, argv, &s) != 0) {
                fprintf(stderr, "--discovery-srv requires a value\n");
                return 1;
            }
            g_discovery_srv = s;
            if (cetcd_discovery_valid_domain(g_discovery_srv) != 0) {
                fprintf(stderr, "--discovery-srv domain is invalid\n");
                return 1;
            }
            cmd_start += 1;
        } else if (cetcd_cli_flag_is(argv[cmd_start], "--user")) {
            const char *s = NULL;
            if (take_ctl_value_(&cmd_start, argc, argv, &s) != 0) {
                fprintf(stderr, "--user requires a value\n");
                return 1;
            }
            user_cred = s;
            cmd_start += 1;
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

    if ((g_cert[0] && !g_key[0]) || (!g_cert[0] && g_key[0])) {
        fprintf(stderr, "tls: --cert and --key must be set together\n");
        return 1;
    }
    if (g_insecure_transport && (g_cacert[0] || g_cert[0] || g_key[0])) {
        fprintf(stderr, "tls: --insecure-transport cannot be mixed with --cacert/--cert/--key\n");
        return 1;
    }
    if (g_endpoint_https && g_insecure_transport) {
        fprintf(stderr, "tls: https endpoint cannot be mixed with --insecure-transport\n");
        return 1;
    }
    if (g_endpoint_https && !g_cacert[0] && !(g_cert[0] && g_key[0]) && !g_insecure) {
        fprintf(stderr, "tls: https endpoint requires --cacert or --insecure\n");
        return 1;
    }
    if (g_tcp_keepalive_timeout >= 0 && g_tcp_keepalive_time < 0) {
        fprintf(stderr, "--keepalive-timeout requires --keepalive-time\n");
        return 1;
    }
    if (g_discovery_srv_name && !g_discovery_srv) {
        fprintf(stderr, "--discovery-srv-name requires --discovery-srv\n");
        return 1;
    }
    if (g_discovery_srv && g_endpoints_set) {
        fprintf(stderr, "--discovery-srv cannot be mixed with --endpoints\n");
        return 1;
    }
    if (user_cred) {
        if (g_password[0] && strchr(user_cred, ':') == NULL) {
            char cred_buf[512];
            snprintf(cred_buf, sizeof(cred_buf), "%s:%s", user_cred, g_password);
            if (do_authenticate(cred_buf) != 0)
                return 1;
        } else if (do_authenticate(user_cred) != 0) {
            return 1;
        }
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
