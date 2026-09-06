#include "cetcd/base.h"
#include "cetcd/tls.h"
#include "cetcd_test.h"

#include <limits.h>
#include <string.h>

CETCD_TEST_CASE(tls_self_signed_days_default_and_years) {
    int days = 0;
    CETCD_ASSERT_EQ_INT(cetcd_tls_self_signed_days(0, &days), CETCD_OK);
    CETCD_ASSERT_EQ_INT(days, 365);
    CETCD_ASSERT_EQ_INT(cetcd_tls_self_signed_days(1, &days), CETCD_OK);
    CETCD_ASSERT_EQ_INT(days, 365);
    CETCD_ASSERT_EQ_INT(cetcd_tls_self_signed_days(2, &days), CETCD_OK);
    CETCD_ASSERT_EQ_INT(days, 730);
    CETCD_ASSERT_EQ_INT(cetcd_tls_self_signed_days(10, &days), CETCD_OK);
    CETCD_ASSERT_EQ_INT(days, 3650);
}

CETCD_TEST_CASE(tls_parse_version) {
    int v = 99;
    CETCD_ASSERT_EQ_INT(cetcd_parse_tls_version("TLS1.2", &v), CETCD_OK);
    CETCD_ASSERT_EQ_INT(v, CETCD_TLS_VER_1_2);
    CETCD_ASSERT_EQ_INT(cetcd_parse_tls_version("TLS1.3", &v), CETCD_OK);
    CETCD_ASSERT_EQ_INT(v, CETCD_TLS_VER_1_3);
    CETCD_ASSERT_EQ_INT(cetcd_parse_tls_version("TLS1.1", &v), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_tls_version("1.2", &v), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_tls_version("tls1.2", &v), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_tls_version("", &v), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_tls_version(NULL, &v), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_tls_version("TLS1.2", NULL), CETCD_ERR_INVAL);
}

CETCD_TEST_CASE(tls_version_range) {
    CETCD_ASSERT_EQ_INT(cetcd_tls_version_range_ok(0, 0), CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_tls_version_range_ok(CETCD_TLS_VER_1_2, 0), CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_tls_version_range_ok(CETCD_TLS_VER_1_3, 0), CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_tls_version_range_ok(CETCD_TLS_VER_1_2, CETCD_TLS_VER_1_2),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_tls_version_range_ok(CETCD_TLS_VER_1_2, CETCD_TLS_VER_1_3),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_tls_version_range_ok(CETCD_TLS_VER_1_3, CETCD_TLS_VER_1_3),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_tls_version_range_ok(CETCD_TLS_VER_1_3, CETCD_TLS_VER_1_2),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_tls_version_range_ok(9, CETCD_TLS_VER_1_3),
                        CETCD_ERR_INVAL);
}

CETCD_TEST_CASE(tls_peer_identity_lists) {
    CETCD_ASSERT_EQ_INT(cetcd_tls_name_list_open(NULL), 1);
    CETCD_ASSERT_EQ_INT(cetcd_tls_name_list_open(""), 1);
    CETCD_ASSERT_EQ_INT(cetcd_tls_name_list_open("etcd"), 0);
    CETCD_ASSERT_EQ_INT(cetcd_tls_name_list_has(NULL, "x"), 1);
    CETCD_ASSERT_EQ_INT(cetcd_tls_name_list_has("etcd,root", "etcd"), 1);
    CETCD_ASSERT_EQ_INT(cetcd_tls_name_list_has("etcd,root", "root"), 1);
    CETCD_ASSERT_EQ_INT(cetcd_tls_name_list_has(" etcd , root ", "etcd"), 1);
    CETCD_ASSERT_EQ_INT(cetcd_tls_name_list_has("etcd", "Etcd"), 0);
    CETCD_ASSERT_EQ_INT(cetcd_tls_name_list_has("etcd", NULL), 0);
    CETCD_ASSERT_EQ_INT(cetcd_tls_hostname_matches("example.com", "EXAMPLE.COM"), 1);
    CETCD_ASSERT_EQ_INT(cetcd_tls_hostname_matches("*.example.com", "foo.example.com"), 1);
    CETCD_ASSERT_EQ_INT(cetcd_tls_hostname_matches("*.example.com", "FOO.Example.COM"), 1);
    CETCD_ASSERT_EQ_INT(cetcd_tls_hostname_matches("*.example.com", "foo.bar.example.com"), 0);
    CETCD_ASSERT_EQ_INT(cetcd_tls_hostname_matches("*.example.com", "example.com"), 0);
    CETCD_ASSERT_EQ_INT(cetcd_tls_hostname_matches("", "example.com"), 0);
    const char *sans[] = { "foo.example.com", "127.0.0.1" };
    CETCD_ASSERT_EQ_INT(cetcd_tls_peer_identity_ok(NULL, NULL, "x", NULL, 0), 1);
    CETCD_ASSERT_EQ_INT(cetcd_tls_peer_identity_ok("etcd", NULL, "etcd", NULL, 0), 1);
    CETCD_ASSERT_EQ_INT(cetcd_tls_peer_identity_ok("etcd", NULL, "other", NULL, 0), 0);
    CETCD_ASSERT_EQ_INT(cetcd_tls_peer_identity_ok(NULL, "foo.example.com", "cn",
                                                   sans, 2), 1);
    CETCD_ASSERT_EQ_INT(cetcd_tls_peer_identity_ok(NULL, "*.example.com", "cn",
                                                   sans, 2), 1);
    CETCD_ASSERT_EQ_INT(cetcd_tls_peer_identity_ok(NULL, "evil.com", "cn",
                                                   sans, 2), 0);
    CETCD_ASSERT_EQ_INT(cetcd_tls_peer_identity_ok(NULL, "cn.only", "cn.only",
                                                   NULL, 0), 1);
    CETCD_ASSERT_EQ_INT(cetcd_tls_peer_identity_ok("etcd", "evil.com", "etcd",
                                                   sans, 2), 1);
    CETCD_ASSERT_EQ_INT(cetcd_tls_peer_identity_ok("nope", "foo.example.com",
                                                   "etcd", sans, 2), 1);
    CETCD_ASSERT_EQ_INT(cetcd_tls_check_peer_identity(NULL, NULL, NULL), CETCD_OK);
}

CETCD_TEST_CASE(tls_outbound_paths) {
    const char *c = NULL, *k = NULL;
    CETCD_ASSERT_EQ_INT(cetcd_tls_outbound_paths(NULL, NULL, NULL, NULL, NULL, &k),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_tls_outbound_paths(NULL, NULL, NULL, NULL, &c, NULL),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_tls_outbound_paths(NULL, NULL, NULL, NULL, &c, &k),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(c[0], 0);
    CETCD_ASSERT_EQ_INT(k[0], 0);
    CETCD_ASSERT_EQ_INT(cetcd_tls_outbound_paths("srv.crt", "srv.key", NULL, NULL,
                                                 &c, &k),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(strcmp(c, "srv.crt"), 0);
    CETCD_ASSERT_EQ_INT(strcmp(k, "srv.key"), 0);
    CETCD_ASSERT_EQ_INT(cetcd_tls_outbound_paths("srv.crt", "srv.key",
                                                 "cli.crt", "cli.key", &c, &k),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(strcmp(c, "cli.crt"), 0);
    CETCD_ASSERT_EQ_INT(strcmp(k, "cli.key"), 0);
    CETCD_ASSERT_EQ_INT(cetcd_tls_outbound_paths("srv.crt", "srv.key",
                                                 "cli.crt", NULL, &c, &k),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_tls_outbound_paths("srv.crt", "srv.key",
                                                 NULL, "cli.key", &c, &k),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_tls_outbound_paths("srv.crt", "srv.key",
                                                 "cli.crt", "", &c, &k),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_tls_outbound_paths("srv.crt", NULL, NULL, NULL,
                                                 &c, &k),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_tls_outbound_paths(NULL, "srv.key", NULL, NULL,
                                                 &c, &k),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_tls_outbound_paths("", "", "cli.crt", "cli.key",
                                                 &c, &k),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(strcmp(c, "cli.crt"), 0);
    CETCD_ASSERT_EQ_INT(strcmp(k, "cli.key"), 0);
}

CETCD_TEST_CASE(tls_crl_helpers) {
    CETCD_ASSERT_EQ_INT(cetcd_tls_crl_requires_cert(NULL, NULL), CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_tls_crl_requires_cert("", "srv.crt"), CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_tls_crl_requires_cert("rev.crl", "srv.crt"), CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_tls_crl_requires_cert("rev.crl", NULL), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_tls_crl_requires_cert("rev.crl", ""), CETCD_ERR_INVAL);
    const uint8_t s1[] = { 0x01, 0x02, 0x03 };
    const uint8_t s2[] = { 0xaa };
    const uint8_t s3[] = { 0x01, 0x02 };
    const uint8_t *rev[] = { s1, s2 };
    const size_t lens[] = { 3, 1 };
    CETCD_ASSERT_EQ_INT(cetcd_tls_serial_revoked(rev, lens, 2, s1, 3), 1);
    CETCD_ASSERT_EQ_INT(cetcd_tls_serial_revoked(rev, lens, 2, s2, 1), 1);
    CETCD_ASSERT_EQ_INT(cetcd_tls_serial_revoked(rev, lens, 2, s3, 2), 0);
    CETCD_ASSERT_EQ_INT(cetcd_tls_serial_revoked(NULL, lens, 2, s1, 3), 0);
    CETCD_ASSERT_EQ_INT(cetcd_tls_serial_revoked(rev, lens, 2, NULL, 3), 0);
    CETCD_ASSERT_EQ_INT(cetcd_tls_check_crl(NULL, NULL), CETCD_OK);
}

CETCD_TEST_CASE(tls_self_signed_days_overflow_and_null) {
    int days = 0;
    CETCD_ASSERT_EQ_INT(cetcd_tls_self_signed_days((uint32_t)(INT_MAX / 365) + 1,
                                                   &days),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_tls_self_signed_days(1, NULL), CETCD_ERR_INVAL);
}

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(tls_self_signed_days_default_and_years),
    CETCD_TEST_ENTRY(tls_parse_version),
    CETCD_TEST_ENTRY(tls_version_range),
    CETCD_TEST_ENTRY(tls_peer_identity_lists),
    CETCD_TEST_ENTRY(tls_outbound_paths),
    CETCD_TEST_ENTRY(tls_crl_helpers),
    CETCD_TEST_ENTRY(tls_self_signed_days_overflow_and_null),
CETCD_TEST_LIST_END

CETCD_TEST_MAIN()
