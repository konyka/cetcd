#if !defined(_WIN32)
#  ifndef _DEFAULT_SOURCE
#    define _DEFAULT_SOURCE
#  endif
#endif

#include "cetcd/discovery.h"
#include "cetcd/base.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#if defined(_WIN32)
#  ifndef WIN32_LEAN_AND_MEAN
#    define WIN32_LEAN_AND_MEAN
#  endif
#  include <winsock2.h>
#  include <ws2tcpip.h>
#  include <windows.h>
#  include <windns.h>
#else
#  include <arpa/inet.h>
#  include <netdb.h>
#  include <netinet/in.h>
#  include <resolv.h>
#  include <sys/socket.h>
#endif

#ifndef CETCD_NS_IN
#  define CETCD_NS_IN  1
#endif
#ifndef CETCD_NS_SRV
#  define CETCD_NS_SRV 33
#endif

static int is_alnum_(unsigned char c) {
    return (c >= '0' && c <= '9') ||
           (c >= 'A' && c <= 'Z') ||
           (c >= 'a' && c <= 'z');
}

static int is_dns_char_(unsigned char c) {
    return is_alnum_(c) || c == '-';
}

static size_t strip_dot_(const char *s, char *out, size_t cap) {
    if (!s || !out || cap == 0) return 0;
    size_t n = strlen(s);
    if (n > 0 && s[n - 1] == '.') n--;
    if (n + 1 > cap) return 0;
    memcpy(out, s, n);
    out[n] = '\0';
    return n;
}

int cetcd_discovery_valid_domain(const char *domain) {
    if (!domain || !domain[0]) return CETCD_ERR_INVAL;
    char buf[256];
    size_t n = strip_dot_(domain, buf, sizeof(buf));
    if (n == 0 || n > 253) return CETCD_ERR_INVAL;
    size_t i = 0;
    while (i < n) {
        size_t start = i;
        if (!is_alnum_((unsigned char)buf[i])) return CETCD_ERR_INVAL;
        i++;
        while (i < n && buf[i] != '.') {
            if (!is_dns_char_((unsigned char)buf[i])) return CETCD_ERR_INVAL;
            i++;
        }
        size_t lab = i - start;
        if (lab < 1 || lab > 63) return CETCD_ERR_INVAL;
        if (buf[start + lab - 1] == '-') return CETCD_ERR_INVAL;
        if (i < n && buf[i] == '.') {
            i++;
            if (i >= n) return CETCD_ERR_INVAL; /* trailing empty after strip */
        }
    }
    return 0;
}

int cetcd_discovery_valid_name(const char *name) {
    if (!name || !name[0]) return 0;
    size_t n = strlen(name);
    if (n > 63) return CETCD_ERR_INVAL;
    if (!is_alnum_((unsigned char)name[0]) ||
        !is_alnum_((unsigned char)name[n - 1]))
        return CETCD_ERR_INVAL;
    for (size_t i = 0; i < n; i++) {
        if (!is_dns_char_((unsigned char)name[i])) return CETCD_ERR_INVAL;
    }
    return 0;
}

static const char *kind_service_(cetcd_discovery_kind kind) {
    switch (kind) {
    case CETCD_DISCOVERY_CLIENT:     return "etcd-client";
    case CETCD_DISCOVERY_CLIENT_SSL: return "etcd-client-ssl";
    case CETCD_DISCOVERY_SERVER:     return "etcd-server";
    case CETCD_DISCOVERY_SERVER_SSL: return "etcd-server-ssl";
    }
    return NULL;
}

int cetcd_discovery_qname(cetcd_discovery_kind kind, const char *name,
                          const char *domain, char *out, size_t cap) {
    const char *svc = kind_service_(kind);
    if (!svc || !out || cap == 0) return CETCD_ERR_INVAL;
    if (cetcd_discovery_valid_name(name) != 0) return CETCD_ERR_INVAL;
    if (cetcd_discovery_valid_domain(domain) != 0) return CETCD_ERR_INVAL;
    char dom[256];
    if (strip_dot_(domain, dom, sizeof(dom)) == 0) return CETCD_ERR_INVAL;
    int n;
    if (name && name[0])
        n = snprintf(out, cap, "_%s-%s._tcp.%s", svc, name, dom);
    else
        n = snprintf(out, cap, "_%s._tcp.%s", svc, dom);
    if (n < 0 || (size_t)n >= cap) return CETCD_ERR_OVERFLOW;
    return 0;
}

static int read_u16_(const uint8_t *msg, size_t len, size_t *off, uint16_t *out) {
    if (!msg || !off || !out || *off + 2 > len) return -1;
    *out = (uint16_t)((msg[*off] << 8) | msg[*off + 1]);
    *off += 2;
    return 0;
}

static int read_name_(const uint8_t *msg, size_t len, size_t *off,
                      char *out, size_t cap, int hops) {
    if (!msg || !off || !out || cap == 0 || hops > 16) return -1;
    size_t w = 0;
    size_t pos = *off;
    int jumped = 0;
    size_t end = *off;
    for (;;) {
        if (pos >= len) return -1;
        uint8_t lab = msg[pos];
        if (lab == 0) {
            if (!jumped) end = pos + 1;
            break;
        }
        if ((lab & 0xC0) == 0xC0) {
            if (pos + 1 >= len) return -1;
            size_t ptr = (size_t)(((lab & 0x3F) << 8) | msg[pos + 1]);
            if (ptr >= len || ptr == pos) return -1;
            if (!jumped) end = pos + 2;
            jumped = 1;
            pos = ptr;
            hops++;
            if (hops > 16) return -1;
            continue;
        }
        if ((lab & 0xC0) != 0) return -1;
        pos++;
        if (pos + lab > len) return -1;
        if (w > 0) {
            if (w + 1 >= cap) return -1;
            out[w++] = '.';
        }
        if (w + lab >= cap) return -1;
        memcpy(out + w, msg + pos, lab);
        w += lab;
        pos += lab;
        if (!jumped) end = pos;
    }
    out[w] = '\0';
    *off = end;
    if (w == 0) return -1;
    /* strip trailing dot if present */
    if (out[w - 1] == '.') {
        out[w - 1] = '\0';
        if (out[0] == '\0') return -1;
    }
    return 0;
}

int cetcd_discovery_parse_message(const uint8_t *msg, size_t len,
                                  cetcd_srv_record *out, size_t cap, size_t *n) {
    if (!msg || !out || !n || cap == 0) return CETCD_ERR_INVAL;
    *n = 0;
    if (len < 12) return CETCD_ERR_RANGE;
    size_t off = 4;
    uint16_t qd = 0, an = 0, ns = 0, ar = 0;
    if (read_u16_(msg, len, &off, &qd) != 0) return CETCD_ERR_RANGE;
    if (read_u16_(msg, len, &off, &an) != 0) return CETCD_ERR_RANGE;
    if (read_u16_(msg, len, &off, &ns) != 0) return CETCD_ERR_RANGE;
    if (read_u16_(msg, len, &off, &ar) != 0) return CETCD_ERR_RANGE;
    (void)ns;
    (void)ar;
    char name[256];
    for (uint16_t i = 0; i < qd; i++) {
        if (read_name_(msg, len, &off, name, sizeof(name), 0) != 0)
            return CETCD_ERR_CORRUPT;
        uint16_t t = 0, c = 0;
        if (read_u16_(msg, len, &off, &t) != 0 ||
            read_u16_(msg, len, &off, &c) != 0)
            return CETCD_ERR_RANGE;
    }
    uint32_t total = (uint32_t)an + (uint32_t)ns + (uint32_t)ar;
    size_t got = 0;
    for (uint32_t i = 0; i < total; i++) {
        if (read_name_(msg, len, &off, name, sizeof(name), 0) != 0)
            return CETCD_ERR_CORRUPT;
        uint16_t type = 0, cls = 0, rdlen = 0;
        if (read_u16_(msg, len, &off, &type) != 0 ||
            read_u16_(msg, len, &off, &cls) != 0)
            return CETCD_ERR_RANGE;
        if (off + 4 > len) return CETCD_ERR_RANGE;
        off += 4; /* TTL */
        if (read_u16_(msg, len, &off, &rdlen) != 0) return CETCD_ERR_RANGE;
        if (off + rdlen > len) return CETCD_ERR_RANGE;
        if (type == 33 && i < an) { /* SRV in answer section */
            if (rdlen < 7) return CETCD_ERR_CORRUPT;
            if (got >= cap) return CETCD_ERR_OVERFLOW;
            size_t roff = off;
            uint16_t pri = 0, w = 0, port = 0;
            if (read_u16_(msg, len, &roff, &pri) != 0 ||
                read_u16_(msg, len, &roff, &w) != 0 ||
                read_u16_(msg, len, &roff, &port) != 0)
                return CETCD_ERR_RANGE;
            if (port < 1) return CETCD_ERR_RANGE;
            if (read_name_(msg, len, &roff, out[got].target,
                           sizeof(out[got].target), 0) != 0)
                return CETCD_ERR_CORRUPT;
            if (roff > off + rdlen) return CETCD_ERR_CORRUPT;
            out[got].priority = pri;
            out[got].weight = w;
            out[got].port = port;
            got++;
        }
        off += rdlen;
    }
    if (got == 0) return CETCD_ERR_NOTFOUND;
    *n = got;
    return 0;
}

void cetcd_discovery_sort(cetcd_srv_record *recs, size_t n) {
    if (!recs || n < 2) return;
    for (size_t i = 1; i < n; i++) {
        cetcd_srv_record key = recs[i];
        size_t j = i;
        while (j > 0) {
            const cetcd_srv_record *a = &recs[j - 1];
            int less = 0;
            if (key.priority < a->priority) less = 1;
            else if (key.priority == a->priority) {
                if (key.weight > a->weight) less = 1;
                else if (key.weight == a->weight &&
                         strcmp(key.target, a->target) < 0)
                    less = 1;
            }
            if (!less) break;
            recs[j] = recs[j - 1];
            j--;
        }
        recs[j] = key;
    }
}

int cetcd_discovery_lookup(const char *qname,
                           cetcd_srv_record *out, size_t cap, size_t *n) {
    if (!qname || !qname[0] || !out || !n || cap == 0) return CETCD_ERR_INVAL;
    *n = 0;
#if defined(_WIN32)
    PDNS_RECORD recs = NULL;
    DNS_STATUS st = DnsQuery_A(qname, DNS_TYPE_SRV, DNS_QUERY_STANDARD,
                               NULL, &recs, NULL);
    if (st != 0 || !recs) return CETCD_ERR_NOTFOUND;
    size_t got = 0;
    int rc = 0;
    for (PDNS_RECORD r = recs; r; r = r->pNext) {
        if (r->wType != DNS_TYPE_SRV) continue;
        if (got >= cap) { rc = CETCD_ERR_OVERFLOW; break; }
        if (r->Data.SRV.wPort < 1) { rc = CETCD_ERR_RANGE; break; }
        const char *tgt = r->Data.SRV.pNameTarget;
        if (!tgt || !tgt[0] || strlen(tgt) >= sizeof(out[got].target)) {
            rc = CETCD_ERR_CORRUPT;
            break;
        }
        out[got].priority = r->Data.SRV.wPriority;
        out[got].weight = r->Data.SRV.wWeight;
        out[got].port = r->Data.SRV.wPort;
        strncpy(out[got].target, tgt, sizeof(out[got].target) - 1);
        out[got].target[sizeof(out[got].target) - 1] = '\0';
        size_t tl = strlen(out[got].target);
        if (tl > 0 && out[got].target[tl - 1] == '.')
            out[got].target[tl - 1] = '\0';
        if (!out[got].target[0]) { rc = CETCD_ERR_CORRUPT; break; }
        got++;
    }
    DnsRecordListFree(recs, DnsFreeRecordList);
    if (rc != 0) return rc;
    if (got == 0) return CETCD_ERR_NOTFOUND;
    *n = got;
    cetcd_discovery_sort(out, *n);
    return 0;
#else
    uint8_t buf[4096];
    int qlen = res_query(qname, CETCD_NS_IN, CETCD_NS_SRV, buf, (int)sizeof(buf));
    if (qlen <= 0) return CETCD_ERR_NOTFOUND;
    int rc = cetcd_discovery_parse_message(buf, (size_t)qlen, out, cap, n);
    if (rc != 0) return rc;
    cetcd_discovery_sort(out, *n);
    return 0;
#endif
}

int cetcd_discovery_records_to_endpoints(const cetcd_srv_record *recs, size_t n,
                                         int https, cetcd_endpoint *out,
                                         size_t cap, size_t *nout) {
    if (!recs || !out || !nout) return CETCD_ERR_INVAL;
    if (n == 0) return CETCD_ERR_NOTFOUND;
    if (n > cap) return CETCD_ERR_OVERFLOW;
    for (size_t i = 0; i < n; i++) {
        if (recs[i].port < 1 || !recs[i].target[0]) return CETCD_ERR_INVAL;
        if (strlen(recs[i].target) >= sizeof(out[i].host)) return CETCD_ERR_OVERFLOW;
        memset(&out[i], 0, sizeof(out[i]));
        memcpy(out[i].host, recs[i].target, strlen(recs[i].target) + 1);
        out[i].port = recs[i].port;
        out[i].https = https ? 1 : 0;
    }
    *nout = n;
    return 0;
}

int cetcd_discovery_resolve(cetcd_discovery_kind kind, const char *name,
                            const char *domain, int https,
                            cetcd_endpoint *out, size_t cap, size_t *n) {
    char q[CETCD_DISCOVERY_MAX_QNAME];
    if (cetcd_discovery_qname(kind, name, domain, q, sizeof(q)) != 0)
        return CETCD_ERR_INVAL;
    cetcd_srv_record recs[CETCD_DISCOVERY_MAX_RECORDS];
    size_t nr = 0;
    int rc = cetcd_discovery_lookup(q, recs, CETCD_DISCOVERY_MAX_RECORDS, &nr);
    if (rc != 0) return rc;
    return cetcd_discovery_records_to_endpoints(recs, nr, https, out, cap, n);
}

int cetcd_discovery_peer_id(const char *target, uint16_t port, uint64_t *id) {
    if (!target || !target[0] || port < 1 || !id) return CETCD_ERR_INVAL;
    char key[280];
    int n = snprintf(key, sizeof(key), "%s:%u", target, (unsigned)port);
    if (n < 0 || (size_t)n >= sizeof(key)) return CETCD_ERR_OVERFLOW;
    uint64_t h = cetcd_hash_fnv1a64(key, (size_t)n);
    if (h == 0) h = 1;
    *id = h;
    return 0;
}

int cetcd_url_is_unix_n(const char *s, size_t n) {
    if (!s) return 0;
    if (n >= 7 && memcmp(s, "unix://", 7) == 0) return 1;
    if (n >= 8 && memcmp(s, "unixs://", 8) == 0) return 1;
    return 0;
}

int cetcd_url_is_unix(const char *s) {
    return s ? cetcd_url_is_unix_n(s, strlen(s)) : 0;
}

static const char *skip_ws_(const char *s, const char *end) {
    while (s < end && (*s == ' ' || *s == '\t')) s++;
    return s;
}

static const char *rtrim_(const char *s, const char *end) {
    while (end > s && (end[-1] == ' ' || end[-1] == '\t' || end[-1] == '/'))
        end--;
    return end;
}

static int parse_one_endpoint_(const char *s, size_t n, cetcd_endpoint *out) {
    if (!s || !out || n == 0) return CETCD_ERR_INVAL;
    const char *end = rtrim_(s, s + n);
    s = skip_ws_(s, end);
    if (s >= end) return CETCD_ERR_INVAL;
    if (cetcd_url_is_unix_n(s, (size_t)(end - s))) return CETCD_ERR_UNSUPPORT;
    int https = 0;
    if ((size_t)(end - s) >= 8 && strncmp(s, "https://", 8) == 0) {
        https = 1;
        s += 8;
    } else if ((size_t)(end - s) >= 7 && strncmp(s, "http://", 7) == 0) {
        s += 7;
    }
    s = skip_ws_(s, end);
    if (s >= end) return CETCD_ERR_INVAL;
    char host[256];
    uint16_t port = 2379;
    if (*s == '[') {
        const char *rb = memchr(s, ']', (size_t)(end - s));
        if (!rb || rb == s + 1) return CETCD_ERR_INVAL;
        size_t hlen = (size_t)(rb - s - 1);
        if (hlen == 0 || hlen >= sizeof(host)) return CETCD_ERR_OVERFLOW;
        memcpy(host, s + 1, hlen);
        host[hlen] = '\0';
        s = rb + 1;
        if (s < end) {
            if (*s != ':') return CETCD_ERR_INVAL;
            s++;
            if (s >= end) return CETCD_ERR_INVAL;
            char *ep = NULL;
            errno = 0;
            unsigned long v = strtoul(s, &ep, 10);
            if (errno == ERANGE || !ep || ep == s || ep != end || v < 1 || v > 65535)
                return CETCD_ERR_RANGE;
            port = (uint16_t)v;
        }
    } else {
        const char *colon = NULL;
        for (const char *p = s; p < end; p++) {
            if (*p == ':') colon = p;
        }
        if (colon) {
            size_t hlen = (size_t)(colon - s);
            if (hlen == 0 || hlen >= sizeof(host)) return CETCD_ERR_INVAL;
            memcpy(host, s, hlen);
            host[hlen] = '\0';
            char *ep = NULL;
            errno = 0;
            unsigned long v = strtoul(colon + 1, &ep, 10);
            if (errno == ERANGE || !ep || ep == colon + 1 || ep != end ||
                v < 1 || v > 65535)
                return CETCD_ERR_RANGE;
            port = (uint16_t)v;
        } else {
            size_t hlen = (size_t)(end - s);
            if (hlen == 0 || hlen >= sizeof(host)) return CETCD_ERR_INVAL;
            memcpy(host, s, hlen);
            host[hlen] = '\0';
        }
    }
    if (!host[0]) return CETCD_ERR_INVAL;
    memset(out, 0, sizeof(*out));
    memcpy(out->host, host, strlen(host) + 1);
    out->port = port;
    out->https = https;
    return 0;
}

#if defined(_WIN32)
static void winsock_ensure_(void) {
    static int inited;
    if (inited) return;
    WSADATA wsa;
    if (WSAStartup(MAKEWORD(2, 2), &wsa) == 0) inited = 1;
}
#else
static void winsock_ensure_(void) {}
#endif

/* Leftover-safe IPv4: 4 octets 0..255 and end of string, or leftover INVAL.
 * A short prefix (10.0.0) is "not IPv4" so a hostname can still resolve. */
static int parse_ipv4_exact_(const char *s, struct in_addr *out) {
    unsigned o[4];
    const char *p = s;
    for (int i = 0; i < 4; i++) {
        if (*p < '0' || *p > '9') return -1;
        char *ep = NULL;
        errno = 0;
        unsigned long v = strtoul(p, &ep, 10);
        if (errno == ERANGE || !ep || ep == p || v > 255) return -1;
        o[i] = (unsigned)v;
        p = ep;
        if (i < 3) {
            if (*p != '.') return -1;
            p++;
        }
    }
    if (*p) return -2; /* leftover after a complete IPv4 */
    unsigned long packed = (o[0] << 24) | (o[1] << 16) | (o[2] << 8) | o[3];
    out->s_addr = htonl((uint32_t)packed);
    return 0;
}

static void fill_v4_(struct sockaddr_storage *ss, uint16_t port,
                     const struct in_addr *a) {
    struct sockaddr_in *in = (struct sockaddr_in *)ss;
    memset(in, 0, sizeof(*in));
    in->sin_family = AF_INET;
    in->sin_port = htons(port);
    in->sin_addr = *a;
}

static void fill_v6_(struct sockaddr_storage *ss, uint16_t port,
                     const struct in6_addr *a) {
    struct sockaddr_in6 *in6 = (struct sockaddr_in6 *)ss;
    memset(in6, 0, sizeof(*in6));
    in6->sin6_family = AF_INET6;
    in6->sin6_port = htons(port);
    in6->sin6_addr = *a;
}

int cetcd_parse_ipv6_zone(const char *host, char *addr, size_t addr_cap,
                          char *zone, size_t zone_cap) {
    const char *pct;
    size_t alen;
    struct in6_addr v6;
    if (!host || !host[0] || !addr || addr_cap < 2 || !zone || zone_cap < 1)
        return CETCD_ERR_INVAL;
    winsock_ensure_();
    pct = strchr(host, '%');
    if (!pct) {
        size_t n = strlen(host);
        if (n + 1 > addr_cap) return CETCD_ERR_OVERFLOW;
        memcpy(addr, host, n + 1);
        zone[0] = '\0';
        if (strchr(addr, ':') && inet_pton(AF_INET6, addr, &v6) != 1)
            return CETCD_ERR_INVAL;
        return CETCD_OK;
    }
    alen = (size_t)(pct - host);
    if (alen == 0 || alen + 1 > addr_cap) return CETCD_ERR_INVAL;
    memcpy(addr, host, alen);
    addr[alen] = '\0';
    {
        const char *z = pct + 1;
        size_t zlen;
        if (!z[0]) return CETCD_ERR_INVAL;
        zlen = strlen(z);
        if (zlen + 1 > zone_cap) return CETCD_ERR_OVERFLOW;
        if (z[0] >= '0' && z[0] <= '9') {
            char *end = NULL;
            errno = 0;
            (void)strtoul(z, &end, 10);
            if (errno == ERANGE || !end || end == z || *end)
                return CETCD_ERR_INVAL;
        } else if ((z[0] >= 'A' && z[0] <= 'Z') ||
                   (z[0] >= 'a' && z[0] <= 'z')) {
            size_t i;
            for (i = 0; i < zlen; i++) {
                unsigned char c = (unsigned char)z[i];
                int ok = (c >= '0' && c <= '9') ||
                         (c >= 'A' && c <= 'Z') ||
                         (c >= 'a' && c <= 'z') ||
                         c == '_' || c == '-' || c == '.';
                if (!ok) return CETCD_ERR_INVAL;
            }
        } else {
            return CETCD_ERR_INVAL;
        }
        memcpy(zone, z, zlen + 1);
    }
    memset(&v6, 0, sizeof(v6));
    if (inet_pton(AF_INET6, addr, &v6) != 1) return CETCD_ERR_INVAL;
    return CETCD_OK;
}

int cetcd_host_port_resolve_n(const char *host, uint16_t port,
                              void *ss_arr, size_t cap, size_t *n) {
    if (!host || !host[0] || !ss_arr || cap == 0 || !n)
        return CETCD_ERR_INVAL;
    *n = 0;
    winsock_ensure_();
    struct sockaddr_storage *out = (struct sockaddr_storage *)ss_arr;
    struct in_addr v4;
    memset(&v4, 0, sizeof(v4));
    int v4rc = parse_ipv4_exact_(host, &v4);
    if (v4rc == -2) return CETCD_ERR_INVAL;
    if (v4rc == 0) {
        fill_v4_(&out[0], port, &v4);
        *n = 1;
        return CETCD_OK;
    }
    if (strchr(host, ':')) {
        char addr[128], zone[64];
        struct in6_addr v6;
        if (cetcd_parse_ipv6_zone(host, addr, sizeof(addr), zone,
                                  sizeof(zone)) != CETCD_OK)
            return CETCD_ERR_INVAL;
        if (!zone[0]) {
            memset(&v6, 0, sizeof(v6));
            if (inet_pton(AF_INET6, addr, &v6) != 1) return CETCD_ERR_INVAL;
            fill_v6_(&out[0], port, &v6);
            *n = 1;
            return CETCD_OK;
        }
        {
            char portbuf[8];
            int pn = snprintf(portbuf, sizeof(portbuf), "%u", (unsigned)port);
            struct addrinfo hints, *res = NULL;
            if (pn < 0 || (size_t)pn >= sizeof(portbuf))
                return CETCD_ERR_OVERFLOW;
            memset(&hints, 0, sizeof(hints));
            hints.ai_family = AF_INET6;
            hints.ai_socktype = SOCK_STREAM;
            hints.ai_protocol = IPPROTO_TCP;
            hints.ai_flags = AI_NUMERICHOST;
            if (getaddrinfo(host, portbuf, &hints, &res) != 0 || !res)
                return CETCD_ERR_INVAL;
            if (!res->ai_addr || res->ai_addrlen > sizeof(out[0])) {
                freeaddrinfo(res);
                return CETCD_ERR_INVAL;
            }
            memset(&out[0], 0, sizeof(out[0]));
            memcpy(&out[0], res->ai_addr, res->ai_addrlen);
            freeaddrinfo(res);
            *n = 1;
            return CETCD_OK;
        }
    }
    if (cetcd_discovery_valid_domain(host) != 0) return CETCD_ERR_INVAL;
    char portbuf[8];
    int pn = snprintf(portbuf, sizeof(portbuf), "%u", (unsigned)port);
    if (pn < 0 || (size_t)pn >= sizeof(portbuf)) return CETCD_ERR_OVERFLOW;
    struct addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;
    struct addrinfo *res = NULL;
    if (getaddrinfo(host, portbuf, &hints, &res) != 0 || !res)
        return CETCD_ERR_INVAL;
    int pass;
    for (pass = 0; pass < 2; pass++) {
        int want = (pass == 0) ? AF_INET : AF_INET6;
        for (struct addrinfo *p = res; p && *n < cap; p = p->ai_next) {
            if (p->ai_family != want || !p->ai_addr) continue;
            if (p->ai_addrlen > sizeof(out[0])) continue;
            memset(&out[*n], 0, sizeof(out[0]));
            memcpy(&out[*n], p->ai_addr, p->ai_addrlen);
            (*n)++;
        }
    }
    freeaddrinfo(res);
    if (*n == 0) return CETCD_ERR_INVAL;
    return CETCD_OK;
}

int cetcd_format_host_port(const char *host, uint16_t port,
                           char *out, size_t cap) {
    if (!host || !host[0] || !out || cap < 4) return CETCD_ERR_INVAL;
    int n;
    if (strchr(host, ':'))
        n = snprintf(out, cap, "[%s]:%u", host, (unsigned)port);
    else
        n = snprintf(out, cap, "%s:%u", host, (unsigned)port);
    if (n < 0 || (size_t)n >= cap) return CETCD_ERR_OVERFLOW;
    return CETCD_OK;
}

int cetcd_host_port_resolve(const char *host, uint16_t port,
                            void *ss, size_t ss_cap) {
    if (!ss || ss_cap < sizeof(struct sockaddr_storage))
        return CETCD_ERR_INVAL;
    size_t n = 0;
    int rc = cetcd_host_port_resolve_n(host, port, ss, 1, &n);
    if (rc != CETCD_OK) return rc;
    return n == 1 ? CETCD_OK : CETCD_ERR_INVAL;
}

int cetcd_endpoint_parse_list(const char *spec, cetcd_endpoint *out,
                              size_t cap, size_t *n) {
    if (!spec || !out || !n || cap == 0) return CETCD_ERR_INVAL;
    *n = 0;
    size_t slen = strlen(spec);
    if (slen == 0) return CETCD_ERR_INVAL;
    size_t got = 0;
    const char *p = spec;
    while (*p) {
        const char *comma = strchr(p, ',');
        size_t chunk = comma ? (size_t)(comma - p) : strlen(p);
        if (got >= cap) return CETCD_ERR_OVERFLOW;
        int prc = parse_one_endpoint_(p, chunk, &out[got]);
        if (prc != 0) return prc;
        got++;
        if (!comma) break;
        p = comma + 1;
        if (*p == '\0') return CETCD_ERR_INVAL; /* trailing comma */
    }
    if (got == 0) return CETCD_ERR_NOTFOUND;
    *n = got;
    return 0;
}
