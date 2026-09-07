#define _POSIX_C_SOURCE 200809L
#include "cetcd/base.h"
#include "cetcd/discovery.h"
#include "cetcd_test.h"

#include <string.h>
#include <stdint.h>
#if defined(_WIN32)
#  include <winsock2.h>
#  include <ws2tcpip.h>
#else
#  include <arpa/inet.h>
#  include <netinet/in.h>
#  include <sys/socket.h>
#endif

static void put16(uint8_t *p, uint16_t v) {
    p[0] = (uint8_t)(v >> 8);
    p[1] = (uint8_t)(v & 0xFF);
}

static size_t put_name(uint8_t *p, const char *name) {
    size_t n = 0;
    const char *s = name;
    while (*s) {
        const char *dot = strchr(s, '.');
        size_t lab = dot ? (size_t)(dot - s) : strlen(s);
        p[n++] = (uint8_t)lab;
        memcpy(p + n, s, lab);
        n += lab;
        if (!dot) break;
        s = dot + 1;
    }
    p[n++] = 0;
    return n;
}

static size_t build_srv_msg(uint8_t *out, size_t cap,
                            uint16_t pri, uint16_t w, uint16_t port,
                            const char *target) {
    uint8_t *p = out;
    memset(out, 0, cap);
    put16(p + 0, 0x1234);
    put16(p + 2, 0x8400);
    put16(p + 4, 1); /* qdcount */
    put16(p + 6, 1); /* ancount */
    p += 12;
    p += put_name(p, "_etcd-client._tcp.example.com");
    put16(p, 33); p += 2; /* SRV */
    put16(p, 1); p += 2;  /* IN */
    /* answer: pointer to offset 12 */
    p[0] = 0xC0; p[1] = 12; p += 2;
    put16(p, 33); p += 2;
    put16(p, 1); p += 2;
    p[0] = 0; p[1] = 0; p[2] = 0; p[3] = 60; p += 4;
    uint8_t rdata[300];
    size_t r = 0;
    put16(rdata + r, pri); r += 2;
    put16(rdata + r, w); r += 2;
    put16(rdata + r, port); r += 2;
    r += put_name(rdata + r, target);
    put16(p, (uint16_t)r); p += 2;
    memcpy(p, rdata, r); p += r;
    return (size_t)(p - out);
}

CETCD_TEST_CASE(domain_rejects_empty_and_bad_labels) {
    CETCD_ASSERT_TRUE(cetcd_discovery_valid_domain(NULL) != 0);
    CETCD_ASSERT_TRUE(cetcd_discovery_valid_domain("") != 0);
    CETCD_ASSERT_TRUE(cetcd_discovery_valid_domain(".") != 0);
    CETCD_ASSERT_TRUE(cetcd_discovery_valid_domain("-bad.com") != 0);
    CETCD_ASSERT_TRUE(cetcd_discovery_valid_domain("bad-.com") != 0);
    CETCD_ASSERT_TRUE(cetcd_discovery_valid_domain("has_underscore.com") != 0);
    CETCD_ASSERT_TRUE(cetcd_discovery_valid_domain("a..b") != 0);
    char longlab[80];
    memset(longlab, 'a', 64);
    longlab[64] = '.';
    memcpy(longlab + 65, "com", 4);
    CETCD_ASSERT_TRUE(cetcd_discovery_valid_domain(longlab) != 0);
}

CETCD_TEST_CASE(domain_accepts_normal_and_trailing_dot) {
    CETCD_ASSERT_EQ_INT(cetcd_discovery_valid_domain("example.com"), 0);
    CETCD_ASSERT_EQ_INT(cetcd_discovery_valid_domain("example.com."), 0);
    CETCD_ASSERT_EQ_INT(cetcd_discovery_valid_domain("a"), 0);
    CETCD_ASSERT_EQ_INT(cetcd_discovery_valid_domain("n1.svc.cluster.local"), 0);
}

CETCD_TEST_CASE(name_suffix_rules) {
    CETCD_ASSERT_EQ_INT(cetcd_discovery_valid_name(NULL), 0);
    CETCD_ASSERT_EQ_INT(cetcd_discovery_valid_name(""), 0);
    CETCD_ASSERT_EQ_INT(cetcd_discovery_valid_name("foo"), 0);
    CETCD_ASSERT_TRUE(cetcd_discovery_valid_name("-x") != 0);
    CETCD_ASSERT_TRUE(cetcd_discovery_valid_name("x.") != 0);
}

CETCD_TEST_CASE(qname_builds_service_and_suffix) {
    char q[CETCD_DISCOVERY_MAX_QNAME];
    CETCD_ASSERT_EQ_INT(cetcd_discovery_qname(CETCD_DISCOVERY_CLIENT, NULL,
                                              "example.com", q, sizeof(q)), 0);
    CETCD_ASSERT_EQ_STR(q, "_etcd-client._tcp.example.com");
    CETCD_ASSERT_EQ_INT(cetcd_discovery_qname(CETCD_DISCOVERY_CLIENT, "east",
                                              "example.com.", q, sizeof(q)), 0);
    CETCD_ASSERT_EQ_STR(q, "_etcd-client-east._tcp.example.com");
    CETCD_ASSERT_EQ_INT(cetcd_discovery_qname(CETCD_DISCOVERY_CLIENT_SSL, NULL,
                                              "example.com", q, sizeof(q)), 0);
    CETCD_ASSERT_EQ_STR(q, "_etcd-client-ssl._tcp.example.com");
    CETCD_ASSERT_EQ_INT(cetcd_discovery_qname(CETCD_DISCOVERY_SERVER, NULL,
                                              "example.com", q, sizeof(q)), 0);
    CETCD_ASSERT_EQ_STR(q, "_etcd-server._tcp.example.com");
    CETCD_ASSERT_TRUE(cetcd_discovery_qname(CETCD_DISCOVERY_CLIENT, NULL,
                                            "bad_dom", q, sizeof(q)) != 0);
    CETCD_ASSERT_TRUE(cetcd_discovery_qname(CETCD_DISCOVERY_CLIENT, NULL,
                                            "example.com", q, 8) != 0);
}

CETCD_TEST_CASE(parse_one_srv_answer) {
    uint8_t msg[512];
    size_t len = build_srv_msg(msg, sizeof(msg), 10, 20, 2379, "n1.example.com");
    cetcd_srv_record recs[4];
    size_t n = 0;
    CETCD_ASSERT_EQ_INT(cetcd_discovery_parse_message(msg, len, recs, 4, &n), 0);
    CETCD_ASSERT_EQ_INT((int)n, 1);
    CETCD_ASSERT_EQ_INT(recs[0].priority, 10);
    CETCD_ASSERT_EQ_INT(recs[0].weight, 20);
    CETCD_ASSERT_EQ_INT(recs[0].port, 2379);
    CETCD_ASSERT_EQ_STR(recs[0].target, "n1.example.com");
}

CETCD_TEST_CASE(parse_rejects_port_zero_and_truncation) {
    uint8_t msg[512];
    size_t len = build_srv_msg(msg, sizeof(msg), 1, 1, 0, "n1.example.com");
    cetcd_srv_record recs[2];
    size_t n = 99;
    CETCD_ASSERT_TRUE(cetcd_discovery_parse_message(msg, len, recs, 2, &n) != 0);

    len = build_srv_msg(msg, sizeof(msg), 1, 1, 2379, "n1.example.com");
    CETCD_ASSERT_TRUE(cetcd_discovery_parse_message(msg, 8, recs, 2, &n) != 0);
    CETCD_ASSERT_TRUE(cetcd_discovery_parse_message(msg, len - 3, recs, 2, &n) != 0);
    CETCD_ASSERT_TRUE(cetcd_discovery_parse_message(NULL, len, recs, 2, &n) != 0);
}

CETCD_TEST_CASE(parse_rejects_pointer_loop) {
    uint8_t msg[32];
    memset(msg, 0, sizeof(msg));
    put16(msg + 4, 0); /* qdcount */
    put16(msg + 6, 1); /* ancount */
    /* name pointer to itself at offset 12 */
    msg[12] = 0xC0;
    msg[13] = 12;
    cetcd_srv_record recs[1];
    size_t n = 0;
    CETCD_ASSERT_TRUE(cetcd_discovery_parse_message(msg, sizeof(msg), recs, 1, &n) != 0);
}

CETCD_TEST_CASE(sort_priority_then_weight) {
    cetcd_srv_record recs[3];
    memset(recs, 0, sizeof(recs));
    recs[0].priority = 20; recs[0].weight = 5; recs[0].port = 1;
    memcpy(recs[0].target, "c", 2);
    recs[1].priority = 10; recs[1].weight = 1; recs[1].port = 2;
    memcpy(recs[1].target, "a", 2);
    recs[2].priority = 10; recs[2].weight = 9; recs[2].port = 3;
    memcpy(recs[2].target, "b", 2);
    cetcd_discovery_sort(recs, 3);
    CETCD_ASSERT_EQ_INT(recs[0].port, 3); /* pri 10, weight 9 */
    CETCD_ASSERT_EQ_INT(recs[1].port, 2); /* pri 10, weight 1 */
    CETCD_ASSERT_EQ_INT(recs[2].port, 1); /* pri 20 */
}

CETCD_TEST_CASE(records_to_endpoints_and_peer_id) {
    cetcd_srv_record recs[2];
    memset(recs, 0, sizeof(recs));
    recs[0].port = 2379;
    memcpy(recs[0].target, "n1.example.com", 15);
    recs[1].port = 2479;
    memcpy(recs[1].target, "n2.example.com", 15);
    cetcd_endpoint eps[2];
    size_t n = 0;
    CETCD_ASSERT_EQ_INT(cetcd_discovery_records_to_endpoints(recs, 2, 1, eps, 2, &n), 0);
    CETCD_ASSERT_EQ_INT((int)n, 2);
    CETCD_ASSERT_EQ_STR(eps[0].host, "n1.example.com");
    CETCD_ASSERT_EQ_INT(eps[0].port, 2379);
    CETCD_ASSERT_EQ_INT(eps[0].https, 1);
    CETCD_ASSERT_TRUE(cetcd_discovery_records_to_endpoints(recs, 2, 0, eps, 1, &n) != 0);
    uint64_t a = 0, b = 0;
    CETCD_ASSERT_EQ_INT(cetcd_discovery_peer_id(recs[0].target, recs[0].port, &a), 0);
    CETCD_ASSERT_EQ_INT(cetcd_discovery_peer_id(recs[1].target, recs[1].port, &b), 0);
    CETCD_ASSERT_TRUE(a != 0 && b != 0 && a != b);
    CETCD_ASSERT_EQ_INT(cetcd_discovery_peer_id(recs[0].target, recs[0].port, &b), 0);
    CETCD_ASSERT_TRUE(a == b);
}

CETCD_TEST_CASE(endpoint_parse_list_variants) {
    cetcd_endpoint eps[4];
    size_t n = 0;
    CETCD_ASSERT_EQ_INT(cetcd_endpoint_parse_list("127.0.0.1:2379", eps, 4, &n), 0);
    CETCD_ASSERT_EQ_INT((int)n, 1);
    CETCD_ASSERT_EQ_STR(eps[0].host, "127.0.0.1");
    CETCD_ASSERT_EQ_INT(eps[0].port, 2379);
    CETCD_ASSERT_EQ_INT(eps[0].https, 0);

    CETCD_ASSERT_EQ_INT(cetcd_endpoint_parse_list(
        "https://n1:2379,http://n2:2380,n3", eps, 4, &n), 0);
    CETCD_ASSERT_EQ_INT((int)n, 3);
    CETCD_ASSERT_EQ_INT(eps[0].https, 1);
    CETCD_ASSERT_EQ_STR(eps[0].host, "n1");
    CETCD_ASSERT_EQ_STR(eps[1].host, "n2");
    CETCD_ASSERT_EQ_INT(eps[1].port, 2380);
    CETCD_ASSERT_EQ_STR(eps[2].host, "n3");
    CETCD_ASSERT_EQ_INT(eps[2].port, 2379);

    CETCD_ASSERT_EQ_INT(cetcd_endpoint_parse_list("[::1]:2379", eps, 4, &n), 0);
    CETCD_ASSERT_EQ_STR(eps[0].host, "::1");
    CETCD_ASSERT_EQ_INT(eps[0].port, 2379);
}

CETCD_TEST_CASE(endpoint_parse_fail_closed) {
    cetcd_endpoint eps[2];
    size_t n = 0;
    CETCD_ASSERT_TRUE(cetcd_endpoint_parse_list(NULL, eps, 2, &n) != 0);
    CETCD_ASSERT_TRUE(cetcd_endpoint_parse_list("", eps, 2, &n) != 0);
    CETCD_ASSERT_TRUE(cetcd_endpoint_parse_list("host:0", eps, 2, &n) != 0);
    CETCD_ASSERT_TRUE(cetcd_endpoint_parse_list("host:2379foo", eps, 2, &n) != 0);
    CETCD_ASSERT_TRUE(cetcd_endpoint_parse_list("host:abc", eps, 2, &n) != 0);
    CETCD_ASSERT_TRUE(cetcd_endpoint_parse_list(":2379", eps, 2, &n) != 0);
    CETCD_ASSERT_TRUE(cetcd_endpoint_parse_list("a:1,b:2", eps, 1, &n) != 0);
}

CETCD_TEST_CASE(host_port_resolve_numeric_and_localhost) {
    struct sockaddr_storage ss;
    memset(&ss, 0, sizeof(ss));
    CETCD_ASSERT_EQ_INT(cetcd_host_port_resolve("127.0.0.1", 2379, &ss,
                                                sizeof(ss)),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT((int)ss.ss_family, AF_INET);
    {
        struct sockaddr_in *in = (struct sockaddr_in *)&ss;
        CETCD_ASSERT_EQ_INT((int)ntohs(in->sin_port), 2379);
    }
    memset(&ss, 0, sizeof(ss));
    CETCD_ASSERT_EQ_INT(cetcd_host_port_resolve("::1", 2380, &ss, sizeof(ss)),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT((int)ss.ss_family, AF_INET6);
    {
        struct sockaddr_in6 *in6 = (struct sockaddr_in6 *)&ss;
        CETCD_ASSERT_EQ_INT((int)ntohs(in6->sin6_port), 2380);
    }
    memset(&ss, 0, sizeof(ss));
    CETCD_ASSERT_EQ_INT(cetcd_host_port_resolve("localhost", 2379, &ss,
                                                sizeof(ss)),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(ss.ss_family == AF_INET || ss.ss_family == AF_INET6);
    if (ss.ss_family == AF_INET) {
        struct sockaddr_in *in = (struct sockaddr_in *)&ss;
        CETCD_ASSERT_EQ_INT((int)ntohs(in->sin_port), 2379);
    } else {
        struct sockaddr_in6 *in6 = (struct sockaddr_in6 *)&ss;
        CETCD_ASSERT_EQ_INT((int)ntohs(in6->sin6_port), 2379);
    }
    struct sockaddr_storage many[8];
    size_t n = 0;
    CETCD_ASSERT_EQ_INT(cetcd_host_port_resolve_n("localhost", 2379, many, 8,
                                                  &n),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(n >= 1 && n <= 8);
    CETCD_ASSERT_EQ_INT((int)many[0].ss_family, (int)ss.ss_family);
}

CETCD_TEST_CASE(format_host_port_brackets_ipv6) {
    char out[64];
    CETCD_ASSERT_EQ_INT(cetcd_format_host_port("127.0.0.1", 2379, out,
                                               sizeof(out)),
                        CETCD_OK);
    CETCD_ASSERT_EQ_STR(out, "127.0.0.1:2379");
    CETCD_ASSERT_EQ_INT(cetcd_format_host_port("localhost", 2380, out,
                                               sizeof(out)),
                        CETCD_OK);
    CETCD_ASSERT_EQ_STR(out, "localhost:2380");
    CETCD_ASSERT_EQ_INT(cetcd_format_host_port("::1", 2379, out, sizeof(out)),
                        CETCD_OK);
    CETCD_ASSERT_EQ_STR(out, "[::1]:2379");
    CETCD_ASSERT_EQ_INT(cetcd_format_host_port("2001:db8::1", 2380, out,
                                               sizeof(out)),
                        CETCD_OK);
    CETCD_ASSERT_EQ_STR(out, "[2001:db8::1]:2380");
    cetcd_endpoint eps[1];
    size_t n = 0;
    CETCD_ASSERT_EQ_INT(cetcd_endpoint_parse_list(out, eps, 1, &n), 0);
    CETCD_ASSERT_EQ_STR(eps[0].host, "2001:db8::1");
    CETCD_ASSERT_EQ_INT((int)eps[0].port, 2380);
    CETCD_ASSERT_EQ_INT(cetcd_format_host_port("", 2379, out, sizeof(out)),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_format_host_port(NULL, 2379, out, sizeof(out)),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_format_host_port("::1", 2379, NULL, sizeof(out)),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_format_host_port("::1", 2379, out, 4),
                        CETCD_ERR_OVERFLOW);
}

CETCD_TEST_CASE(host_port_resolve_fail_closed) {
    struct sockaddr_storage ss;
    CETCD_ASSERT_EQ_INT(cetcd_host_port_resolve(NULL, 2379, &ss, sizeof(ss)),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_host_port_resolve("", 2379, &ss, sizeof(ss)),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_host_port_resolve("127.0.0.1", 2379, NULL,
                                                sizeof(ss)),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_host_port_resolve("127.0.0.1", 2379, &ss, 4),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_host_port_resolve("127.0.0.1foo", 2379, &ss,
                                                sizeof(ss)),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_host_port_resolve("::1foo", 2379, &ss,
                                                sizeof(ss)),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_host_port_resolve("local host", 2379, &ss,
                                                sizeof(ss)),
                        CETCD_ERR_INVAL);
    size_t n = 99;
    CETCD_ASSERT_EQ_INT(cetcd_host_port_resolve_n("127.0.0.1", 2379, &ss, 0,
                                                  &n),
                        CETCD_ERR_INVAL);
}

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(domain_rejects_empty_and_bad_labels),
    CETCD_TEST_ENTRY(domain_accepts_normal_and_trailing_dot),
    CETCD_TEST_ENTRY(name_suffix_rules),
    CETCD_TEST_ENTRY(qname_builds_service_and_suffix),
    CETCD_TEST_ENTRY(parse_one_srv_answer),
    CETCD_TEST_ENTRY(parse_rejects_port_zero_and_truncation),
    CETCD_TEST_ENTRY(parse_rejects_pointer_loop),
    CETCD_TEST_ENTRY(sort_priority_then_weight),
    CETCD_TEST_ENTRY(records_to_endpoints_and_peer_id),
    CETCD_TEST_ENTRY(endpoint_parse_list_variants),
    CETCD_TEST_ENTRY(endpoint_parse_fail_closed),
    CETCD_TEST_ENTRY(host_port_resolve_numeric_and_localhost),
    CETCD_TEST_ENTRY(format_host_port_brackets_ipv6),
    CETCD_TEST_ENTRY(host_port_resolve_fail_closed),
CETCD_TEST_LIST_END
CETCD_TEST_MAIN()
