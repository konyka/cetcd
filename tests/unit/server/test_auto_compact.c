#include "cetcd/server.h"
#include "cetcd_test.h"

#include <stdio.h>
#include <string.h>

CETCD_TEST_CASE(auto_compact_parse_mode) {
    cetcd_auto_compact_mode m = CETCD_AUTO_COMPACT_OFF;
    CETCD_ASSERT_EQ_INT(cetcd_parse_auto_compaction_mode("periodic", &m), CETCD_OK);
    CETCD_ASSERT_EQ_INT((int)m, (int)CETCD_AUTO_COMPACT_PERIODIC);
    CETCD_ASSERT_EQ_INT(cetcd_parse_auto_compaction_mode("revision", &m), CETCD_OK);
    CETCD_ASSERT_EQ_INT((int)m, (int)CETCD_AUTO_COMPACT_REVISION);
    CETCD_ASSERT_EQ_INT(cetcd_parse_auto_compaction_mode("foo", &m), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_auto_compaction_mode("", &m), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_auto_compaction_mode(NULL, &m), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_auto_compaction_mode("periodic", NULL),
                        CETCD_ERR_INVAL);
}

CETCD_TEST_CASE(auto_compact_parse_retention_periodic) {
    uint64_t v = 99;
    CETCD_ASSERT_EQ_INT(cetcd_parse_auto_compaction_retention(
        "0", CETCD_AUTO_COMPACT_PERIODIC, &v), CETCD_OK);
    CETCD_ASSERT_TRUE(v == 0);
    CETCD_ASSERT_EQ_INT(cetcd_parse_auto_compaction_retention(
        "1", CETCD_AUTO_COMPACT_PERIODIC, &v), CETCD_OK);
    CETCD_ASSERT_TRUE(v == 3600);
    CETCD_ASSERT_EQ_INT(cetcd_parse_auto_compaction_retention(
        "1h", CETCD_AUTO_COMPACT_PERIODIC, &v), CETCD_OK);
    CETCD_ASSERT_TRUE(v == 3600);
    CETCD_ASSERT_EQ_INT(cetcd_parse_auto_compaction_retention(
        "30m", CETCD_AUTO_COMPACT_PERIODIC, &v), CETCD_OK);
    CETCD_ASSERT_TRUE(v == 1800);
    CETCD_ASSERT_EQ_INT(cetcd_parse_auto_compaction_retention(
        "1h30m", CETCD_AUTO_COMPACT_PERIODIC, &v), CETCD_OK);
    CETCD_ASSERT_TRUE(v == 5400);
    CETCD_ASSERT_EQ_INT(cetcd_parse_auto_compaction_retention(
        "10s", CETCD_AUTO_COMPACT_PERIODIC, &v), CETCD_OK);
    CETCD_ASSERT_TRUE(v == 10);
    CETCD_ASSERT_EQ_INT(cetcd_parse_auto_compaction_retention(
        "500ms", CETCD_AUTO_COMPACT_PERIODIC, &v), CETCD_OK);
    CETCD_ASSERT_TRUE(v == 1);
    CETCD_ASSERT_EQ_INT(cetcd_parse_auto_compaction_retention(
        "0s", CETCD_AUTO_COMPACT_PERIODIC, &v), CETCD_OK);
    CETCD_ASSERT_TRUE(v == 0);
    CETCD_ASSERT_EQ_INT(cetcd_parse_auto_compaction_retention(
        "abc", CETCD_AUTO_COMPACT_PERIODIC, &v), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_auto_compaction_retention(
        "-1", CETCD_AUTO_COMPACT_PERIODIC, &v), CETCD_ERR_INVAL);
}

CETCD_TEST_CASE(auto_compact_parse_retention_revision) {
    uint64_t v = 0;
    CETCD_ASSERT_EQ_INT(cetcd_parse_auto_compaction_retention(
        "1000", CETCD_AUTO_COMPACT_REVISION, &v), CETCD_OK);
    CETCD_ASSERT_TRUE(v == 1000);
    CETCD_ASSERT_EQ_INT(cetcd_parse_auto_compaction_retention(
        "0", CETCD_AUTO_COMPACT_REVISION, &v), CETCD_OK);
    CETCD_ASSERT_TRUE(v == 0);
    CETCD_ASSERT_EQ_INT(cetcd_parse_auto_compaction_retention(
        "1h", CETCD_AUTO_COMPACT_REVISION, &v), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_auto_compaction_retention(
        "10s", CETCD_AUTO_COMPACT_REVISION, &v), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_auto_compaction_retention(
        "1", CETCD_AUTO_COMPACT_OFF, &v), CETCD_ERR_INVAL);
}

CETCD_TEST_CASE(auto_compact_due_revision) {
    cetcd_auto_compact_state st;
    memset(&st, 0, sizeof(st));
    st.mode = CETCD_AUTO_COMPACT_REVISION;
    st.retention = 3;
    CETCD_ASSERT_EQ_INT((int)cetcd_auto_compact_due(&st, 2, 0, 0), 0);
    CETCD_ASSERT_EQ_INT((int)cetcd_auto_compact_due(&st, 10, 0, 0), 7);
    CETCD_ASSERT_EQ_INT((int)cetcd_auto_compact_due(&st, 10, 7, 0), 0);
    CETCD_ASSERT_EQ_INT((int)cetcd_auto_compact_due(&st, 12, 7, 0), 9);
}

CETCD_TEST_CASE(auto_compact_due_periodic) {
    cetcd_auto_compact_state st;
    memset(&st, 0, sizeof(st));
    st.mode = CETCD_AUTO_COMPACT_PERIODIC;
    st.retention = 1;
    CETCD_ASSERT_EQ_INT((int)cetcd_auto_compact_due(&st, 5, 0, 100), 0);
    CETCD_ASSERT_EQ_INT((int)st.window_rev, 5);
    CETCD_ASSERT_EQ_INT((int)cetcd_auto_compact_due(&st, 8, 0, 500), 0);
    CETCD_ASSERT_EQ_INT((int)cetcd_auto_compact_due(&st, 8, 0, 1100), 5);
    CETCD_ASSERT_EQ_INT((int)st.window_rev, 8);
    CETCD_ASSERT_EQ_INT((int)cetcd_auto_compact_due(&st, 9, 5, 1200), 0);
}

CETCD_TEST_CASE(auto_compact_parse_duration_ms) {
    uint64_t ms = 99;
    CETCD_ASSERT_EQ_INT(cetcd_parse_go_duration_ms("0", &ms), CETCD_OK);
    CETCD_ASSERT_TRUE(ms == 0);
    CETCD_ASSERT_EQ_INT(cetcd_parse_go_duration_ms("10s", &ms), CETCD_OK);
    CETCD_ASSERT_TRUE(ms == 10000);
    CETCD_ASSERT_EQ_INT(cetcd_parse_go_duration_ms("500ms", &ms), CETCD_OK);
    CETCD_ASSERT_TRUE(ms == 500);
    CETCD_ASSERT_EQ_INT(cetcd_parse_go_duration_ms("1m", &ms), CETCD_OK);
    CETCD_ASSERT_TRUE(ms == 60000);
    CETCD_ASSERT_EQ_INT(cetcd_parse_go_duration_ms("10", &ms), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_go_duration_ms("abc", &ms), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_go_duration_ms(NULL, &ms), CETCD_ERR_INVAL);
}

CETCD_TEST_CASE(auto_compact_parse_batch_limit) {
    uint64_t v = 99;
    CETCD_ASSERT_EQ_INT(cetcd_parse_compaction_batch_limit("0", &v), CETCD_OK);
    CETCD_ASSERT_TRUE(v == 0);
    CETCD_ASSERT_EQ_INT(cetcd_parse_compaction_batch_limit("1000", &v), CETCD_OK);
    CETCD_ASSERT_TRUE(v == 1000);
    CETCD_ASSERT_EQ_INT(cetcd_parse_compaction_batch_limit("1", &v), CETCD_OK);
    CETCD_ASSERT_TRUE(v == 1);
    CETCD_ASSERT_EQ_INT(cetcd_parse_compaction_batch_limit("abc", &v),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_compaction_batch_limit("10foo", &v),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_compaction_batch_limit("-1", &v),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_compaction_batch_limit("", &v),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_compaction_batch_limit(NULL, &v),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_compaction_batch_limit("1000", NULL),
                        CETCD_ERR_INVAL);
}

CETCD_TEST_CASE(auto_compact_parse_max_concurrent_streams) {
    uint32_t n = 99;
    CETCD_ASSERT_EQ_INT(cetcd_parse_max_concurrent_streams("1", &n), CETCD_OK);
    CETCD_ASSERT_EQ_INT((int)n, 1);
    CETCD_ASSERT_EQ_INT(cetcd_parse_max_concurrent_streams("100", &n), CETCD_OK);
    CETCD_ASSERT_EQ_INT((int)n, 100);
    CETCD_ASSERT_EQ_INT(cetcd_parse_max_concurrent_streams("4294967295", &n),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(n == 4294967295u);
    CETCD_ASSERT_EQ_INT(cetcd_parse_max_concurrent_streams("0", &n),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_max_concurrent_streams("4294967296", &n),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_max_concurrent_streams("abc", &n),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_max_concurrent_streams("10foo", &n),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_max_concurrent_streams("-1", &n),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_max_concurrent_streams("", &n),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_max_concurrent_streams(NULL, &n),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_max_concurrent_streams("1", NULL),
                        CETCD_ERR_INVAL);
}

CETCD_TEST_CASE(auto_compact_parse_max_learners) {
    uint32_t n = 99;
    CETCD_ASSERT_EQ_INT(cetcd_parse_max_learners("0", &n), CETCD_OK);
    CETCD_ASSERT_EQ_INT((int)n, 0);
    CETCD_ASSERT_EQ_INT(cetcd_parse_max_learners("1", &n), CETCD_OK);
    CETCD_ASSERT_EQ_INT((int)n, 1);
    CETCD_ASSERT_EQ_INT(cetcd_parse_max_learners("8", &n), CETCD_OK);
    CETCD_ASSERT_EQ_INT((int)n, 8);
    CETCD_ASSERT_EQ_INT(cetcd_parse_max_learners("abc", &n), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_max_learners("1foo", &n), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_max_learners("-1", &n), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_max_learners("", &n), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_max_learners(NULL, &n), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_max_learners("1", NULL), CETCD_ERR_INVAL);
}

CETCD_TEST_CASE(auto_compact_parse_auth_token_ttl) {
    uint64_t n = 0;
    CETCD_ASSERT_EQ_INT(cetcd_parse_auth_token_ttl("300", &n), CETCD_OK);
    CETCD_ASSERT_TRUE(n == 300);
    CETCD_ASSERT_EQ_INT(cetcd_parse_auth_token_ttl("1", &n), CETCD_OK);
    CETCD_ASSERT_TRUE(n == 1);
    CETCD_ASSERT_EQ_INT(cetcd_parse_auth_token_ttl("0", &n), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_auth_token_ttl("abc", &n), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_auth_token_ttl("10foo", &n), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_auth_token_ttl("-1", &n), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_auth_token_ttl("", &n), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_auth_token_ttl(NULL, &n), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_auth_token_ttl("300", NULL), CETCD_ERR_INVAL);
}

CETCD_TEST_CASE(auto_compact_want_pre_vote) {
    CETCD_ASSERT_EQ_INT(cetcd_server_want_pre_vote(0, 0), 1);
    CETCD_ASSERT_EQ_INT(cetcd_server_want_pre_vote(0, 1), 1);
    CETCD_ASSERT_EQ_INT(cetcd_server_want_pre_vote(1, 1), 1);
    CETCD_ASSERT_EQ_INT(cetcd_server_want_pre_vote(1, 0), 0);
}

CETCD_TEST_CASE(auto_compact_parse_raft_timing_ms) {
    uint64_t n = 0;
    CETCD_ASSERT_EQ_INT(cetcd_parse_heartbeat_interval_ms("100", &n), CETCD_OK);
    CETCD_ASSERT_EQ_INT((int)n, 100);
    CETCD_ASSERT_EQ_INT(cetcd_parse_heartbeat_interval_ms("50", &n), CETCD_OK);
    CETCD_ASSERT_EQ_INT((int)n, 50);
    CETCD_ASSERT_EQ_INT(cetcd_parse_election_timeout_ms("1000", &n), CETCD_OK);
    CETCD_ASSERT_EQ_INT((int)n, 1000);
    CETCD_ASSERT_EQ_INT(cetcd_parse_election_timeout_ms("50000", &n), CETCD_OK);
    CETCD_ASSERT_EQ_INT((int)n, 50000);
    CETCD_ASSERT_EQ_INT(cetcd_parse_heartbeat_interval_ms("0", &n),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_election_timeout_ms("0", &n),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_election_timeout_ms("50001", &n),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_heartbeat_interval_ms("abc", &n),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_heartbeat_interval_ms("100ms", &n),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_heartbeat_interval_ms(NULL, &n),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_heartbeat_interval_ms("100", NULL),
                        CETCD_ERR_INVAL);
}

CETCD_TEST_CASE(auto_compact_raft_timing_from_ms) {
    uint64_t hb = 0, et = 0;
    CETCD_ASSERT_EQ_INT(cetcd_raft_timing_from_ms(0, 0, &hb, &et), CETCD_OK);
    CETCD_ASSERT_EQ_INT((int)hb, 1);
    CETCD_ASSERT_EQ_INT((int)et, 10);
    CETCD_ASSERT_EQ_INT(cetcd_raft_timing_from_ms(50, 1000, &hb, &et),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT((int)hb, 1);
    CETCD_ASSERT_EQ_INT((int)et, 20);
    CETCD_ASSERT_EQ_INT(cetcd_raft_timing_from_ms(200, 1000, &hb, &et),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT((int)et, 5);
    CETCD_ASSERT_EQ_INT(cetcd_raft_timing_from_ms(2000, 1000, &hb, &et),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_raft_timing_from_ms(100, 50, &hb, &et),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_raft_timing_from_ms(100, 1000, NULL, &et),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT((int)cetcd_server_tick_ms(0), 100);
    CETCD_ASSERT_EQ_INT((int)cetcd_server_tick_ms(50), 50);
}

CETCD_TEST_CASE(auto_compact_parse_listen_url) {
    char host[64];
    uint16_t port = 0;
    int https = 0;
    CETCD_ASSERT_EQ_INT(cetcd_parse_listen_url("http://127.0.0.1:2381", host,
                                               sizeof(host), &port, &https),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(strcmp(host, "127.0.0.1"), 0);
    CETCD_ASSERT_EQ_INT((int)port, 2381);
    CETCD_ASSERT_EQ_INT(https, 0);
    CETCD_ASSERT_EQ_INT(cetcd_parse_listen_url("https://10.0.0.1:2379", host,
                                               sizeof(host), &port, &https),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(strcmp(host, "10.0.0.1"), 0);
    CETCD_ASSERT_EQ_INT((int)port, 2379);
    CETCD_ASSERT_EQ_INT(https, 1);
    CETCD_ASSERT_EQ_INT(cetcd_parse_listen_url("127.0.0.1:2381", host,
                                               sizeof(host), &port, &https),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_listen_url("http://127.0.0.1", host,
                                               sizeof(host), &port, &https),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_listen_url("http://127.0.0.1:0", host,
                                               sizeof(host), &port, &https),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_listen_url("http://:2381", host,
                                               sizeof(host), &port, &https),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_listen_url("http://[::1]:2381", host,
                                               sizeof(host), &port, &https),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_listen_url("unix:///tmp/m", host,
                                               sizeof(host), &port, &https),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_listen_url("http://127.0.0.1:2381/x", host,
                                               sizeof(host), &port, &https),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_listen_url(NULL, host, sizeof(host),
                                               &port, &https),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_listen_url("http://127.0.0.1:2381", NULL,
                                               sizeof(host), &port, &https),
                        CETCD_ERR_INVAL);
}

CETCD_TEST_CASE(auto_compact_parse_listen_urls) {
    cetcd_listen_url urls[4];
    size_t n = 99;
    CETCD_ASSERT_EQ_INT(cetcd_parse_listen_urls(
        "http://127.0.0.1:2379,http://10.0.0.1:2379", urls, 4, &n), CETCD_OK);
    CETCD_ASSERT_TRUE(n == 2);
    CETCD_ASSERT_EQ_INT(strcmp(urls[0].host, "127.0.0.1"), 0);
    CETCD_ASSERT_EQ_INT((int)urls[0].port, 2379);
    CETCD_ASSERT_EQ_INT(urls[0].https, 0);
    CETCD_ASSERT_EQ_INT(strcmp(urls[1].host, "10.0.0.1"), 0);
    CETCD_ASSERT_EQ_INT(cetcd_parse_listen_urls(
        " http://127.0.0.1:2379 , http://10.0.0.1:12379 ", urls, 4, &n),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(n == 2);
    CETCD_ASSERT_EQ_INT((int)urls[1].port, 12379);
    CETCD_ASSERT_EQ_INT(cetcd_parse_listen_urls(
        "https://10.0.0.1:2379", urls, 4, &n), CETCD_OK);
    CETCD_ASSERT_TRUE(n == 1);
    CETCD_ASSERT_EQ_INT(urls[0].https, 1);
    CETCD_ASSERT_EQ_INT(cetcd_parse_listen_urls(
        "http://127.0.0.1:2379,https://10.0.0.1:2379", urls, 4, &n),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_listen_urls(
        "http://127.0.0.1:2379,http://127.0.0.1:2379", urls, 4, &n),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_listen_urls(
        "http://127.0.0.1:2379,", urls, 4, &n), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_listen_urls(
        ",http://127.0.0.1:2379", urls, 4, &n), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_listen_urls("", urls, 4, &n),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_listen_urls(
        "http://127.0.0.1:2379,http://10.0.0.1:2379", urls, 1, &n),
                        CETCD_ERR_OVERFLOW);

    char host[64];
    uint16_t port = 0;
    int https = 0;
    uint32_t n_extra = 99;
    cetcd_listen_url extra[4];
    CETCD_ASSERT_EQ_INT(cetcd_apply_listen_urls(
        "http://127.0.0.1:2379,http://10.0.0.1:12379",
        host, sizeof(host), &port, &https, extra, 4, &n_extra), CETCD_OK);
    CETCD_ASSERT_EQ_INT(strcmp(host, "127.0.0.1"), 0);
    CETCD_ASSERT_EQ_INT((int)port, 2379);
    CETCD_ASSERT_EQ_INT(https, 0);
    CETCD_ASSERT_EQ_INT((int)n_extra, 1);
    CETCD_ASSERT_EQ_INT(strcmp(extra[0].host, "10.0.0.1"), 0);
    CETCD_ASSERT_EQ_INT((int)extra[0].port, 12379);
    CETCD_ASSERT_EQ_INT(cetcd_apply_listen_urls(
        "https://10.0.0.1:2379", host, sizeof(host), &port, &https,
        extra, 4, &n_extra), CETCD_OK);
    CETCD_ASSERT_EQ_INT(https, 1);
    CETCD_ASSERT_EQ_INT((int)n_extra, 0);
}

CETCD_TEST_CASE(auto_compact_parse_advertise_urls) {
    char out[256];
    CETCD_ASSERT_EQ_INT(cetcd_parse_advertise_urls(
        "http://127.0.0.1:2379,https://10.0.0.1:2379", out, sizeof(out)),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(strcmp(out,
                               "http://127.0.0.1:2379,https://10.0.0.1:2379"),
                        0);
    CETCD_ASSERT_EQ_INT(cetcd_advertise_urls_has_https(out), 1);
    CETCD_ASSERT_EQ_INT(cetcd_advertise_urls_has_https(
        "http://127.0.0.1:2379,http://10.0.0.1:12379"), 0);
    CETCD_ASSERT_EQ_INT(cetcd_parse_advertise_urls(
        "http://127.0.0.1:2379,http://127.0.0.1:2379", out, sizeof(out)),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_advertise_urls(
        "http://127.0.0.1:2379,", out, sizeof(out)), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_advertise_urls("", out, sizeof(out)),
                        CETCD_ERR_INVAL);

    cetcd_listen_url extra[1];
    memset(extra, 0, sizeof(extra));
    strncpy(extra[0].host, "10.0.0.1", sizeof(extra[0].host) - 1);
    extra[0].port = 12379;
    extra[0].https = 0;
    CETCD_ASSERT_EQ_INT(cetcd_format_listen_advertise(
        "127.0.0.1", 2379, 0, extra, 1, out, sizeof(out)), CETCD_OK);
    CETCD_ASSERT_EQ_INT(strcmp(out,
                               "http://127.0.0.1:2379,http://10.0.0.1:12379"),
                        0);

    uint8_t pb[64];
    size_t pos = 0;
    CETCD_ASSERT_EQ_INT(cetcd_pb_append_csv_strings(
        pb, sizeof(pb), &pos, 0x22,
        "http://a:1,http://b:2"), CETCD_OK);
    CETCD_ASSERT_TRUE(pos > 0);
    CETCD_ASSERT_EQ_INT((int)pb[0], 0x22);
    int tags = 0;
    for (size_t i = 0; i < pos; i++)
        if (pb[i] == 0x22) tags++;
    CETCD_ASSERT_EQ_INT(tags, 2);
    CETCD_ASSERT_TRUE(memcmp(pb + 2, "http://a:1", 10) == 0);
}

CETCD_TEST_CASE(auto_compact_parse_metrics_listen_url) {
    char host[64];
    uint16_t port = 0;
    CETCD_ASSERT_EQ_INT(cetcd_parse_metrics_listen_url("http://0.0.0.0:9090",
                                                       host, sizeof(host),
                                                       &port),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(strcmp(host, "0.0.0.0"), 0);
    CETCD_ASSERT_EQ_INT((int)port, 9090);
    CETCD_ASSERT_EQ_INT(cetcd_parse_metrics_listen_url(
                            "https://127.0.0.1:2381", host, sizeof(host),
                            &port),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(strcmp(host, "127.0.0.1"), 0);
    CETCD_ASSERT_EQ_INT((int)port, 2381);
    CETCD_ASSERT_EQ_INT(cetcd_parse_metrics_listen_url(
                            "http://127.0.0.1:2381,http://127.0.0.1:2382",
                            host, sizeof(host), &port),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_metrics_listen_url("http://127.0.0.1:abc",
                                                       host, sizeof(host),
                                                       &port),
                        CETCD_ERR_INVAL);
}

CETCD_TEST_CASE(auto_compact_parse_metrics_listen_urls) {
    cetcd_listen_url urls[4];
    size_t n = 99;
    CETCD_ASSERT_EQ_INT(cetcd_parse_metrics_listen_urls(
        "http://127.0.0.1:2381,http://10.0.0.1:2381", urls, 4, &n), CETCD_OK);
    CETCD_ASSERT_TRUE(n == 2);
    CETCD_ASSERT_EQ_INT(strcmp(urls[0].host, "127.0.0.1"), 0);
    CETCD_ASSERT_EQ_INT((int)urls[0].port, 2381);
    CETCD_ASSERT_EQ_INT(strcmp(urls[1].host, "10.0.0.1"), 0);
    CETCD_ASSERT_EQ_INT(cetcd_parse_metrics_listen_urls(
        "http://127.0.0.1:2381,https://10.0.0.1:2381", urls, 4, &n),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(n == 2);
    CETCD_ASSERT_EQ_INT(urls[0].https, 0);
    CETCD_ASSERT_EQ_INT(urls[1].https, 1);
    CETCD_ASSERT_EQ_INT(cetcd_parse_metrics_listen_urls(
        "http://127.0.0.1:2381,http://127.0.0.1:2381", urls, 4, &n),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_metrics_listen_urls(
        "https://127.0.0.1:2381", urls, 4, &n), CETCD_OK);
    CETCD_ASSERT_TRUE(n == 1);
    CETCD_ASSERT_EQ_INT(urls[0].https, 1);

    char host[64];
    uint16_t port = 0;
    int https = 99;
    uint32_t n_extra = 99;
    cetcd_listen_url extra[4];
    CETCD_ASSERT_EQ_INT(cetcd_apply_metrics_listen_urls(
        "http://0.0.0.0:9090,http://127.0.0.1:9091",
        host, sizeof(host), &port, extra, 4, &n_extra, &https), CETCD_OK);
    CETCD_ASSERT_EQ_INT(strcmp(host, "0.0.0.0"), 0);
    CETCD_ASSERT_EQ_INT((int)port, 9090);
    CETCD_ASSERT_EQ_INT(https, 0);
    CETCD_ASSERT_EQ_INT((int)n_extra, 1);
    CETCD_ASSERT_EQ_INT((int)extra[0].port, 9091);
    CETCD_ASSERT_EQ_INT(cetcd_apply_metrics_listen_urls(
        "http://127.0.0.1:2381", host, sizeof(host), &port, extra, 4, &n_extra,
        &https),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT((int)n_extra, 0);
    CETCD_ASSERT_EQ_INT(https, 0);
    CETCD_ASSERT_EQ_INT(cetcd_apply_metrics_listen_urls(
        "https://10.0.0.1:2381,http://127.0.0.1:2382",
        host, sizeof(host), &port, extra, 4, &n_extra, &https), CETCD_OK);
    CETCD_ASSERT_EQ_INT(https, 1);
    CETCD_ASSERT_EQ_INT((int)n_extra, 1);
    CETCD_ASSERT_EQ_INT(extra[0].https, 0);
    CETCD_ASSERT_EQ_INT(cetcd_metrics_listen_has_https(0, extra, n_extra), 0);
    extra[0].https = 1;
    CETCD_ASSERT_EQ_INT(cetcd_metrics_listen_has_https(0, extra, 1), 1);
    CETCD_ASSERT_EQ_INT(cetcd_metrics_listen_has_https(1, NULL, 0), 1);
    CETCD_ASSERT_EQ_INT(cetcd_metrics_listen_has_https(0, NULL, 0), 0);
}

CETCD_TEST_CASE(auto_compact_metrics_addr) {
    cetcd_server_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    strncpy(cfg.listen_addr, "127.0.0.1", sizeof(cfg.listen_addr) - 1);
    CETCD_ASSERT_EQ_INT(strcmp(cetcd_server_metrics_addr(&cfg), "127.0.0.1"), 0);
    strncpy(cfg.metrics_addr, "0.0.0.0", sizeof(cfg.metrics_addr) - 1);
    CETCD_ASSERT_EQ_INT(strcmp(cetcd_server_metrics_addr(&cfg), "0.0.0.0"), 0);
    CETCD_ASSERT_TRUE(cetcd_server_metrics_addr(NULL) == NULL);
}

CETCD_TEST_CASE(auto_compact_raft_io_timeout) {
    uint64_t v = 0;
    CETCD_ASSERT_EQ_INT(cetcd_parse_raft_io_timeout_ms("5s", &v), CETCD_OK);
    CETCD_ASSERT_TRUE(v == 5000);
    CETCD_ASSERT_EQ_INT(cetcd_parse_raft_io_timeout_ms("10s", &v), CETCD_OK);
    CETCD_ASSERT_TRUE(v == 10000);
    CETCD_ASSERT_EQ_INT(cetcd_parse_raft_io_timeout_ms("0", &v), CETCD_OK);
    CETCD_ASSERT_TRUE(v == 0);
    CETCD_ASSERT_EQ_INT(cetcd_parse_raft_io_timeout_ms("abc", &v), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_raft_io_timeout_ms("5sx", &v), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_raft_io_timeout_ms("", &v), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_raft_io_timeout_ms(NULL, &v), CETCD_ERR_INVAL);
    CETCD_ASSERT_TRUE(cetcd_server_raft_io_timeout_ms(0, 0) == 5000);
    CETCD_ASSERT_TRUE(cetcd_server_raft_io_timeout_ms(1, 0) == 5000);
    CETCD_ASSERT_TRUE(cetcd_server_raft_io_timeout_ms(1, 1000) == 5000);
    CETCD_ASSERT_TRUE(cetcd_server_raft_io_timeout_ms(1, 10000) == 10000);
    CETCD_ASSERT_EQ_INT(cetcd_raft_io_timed_out(0, 9000, 5000), 0);
    CETCD_ASSERT_EQ_INT(cetcd_raft_io_timed_out(100, 5099, 5000), 0);
    CETCD_ASSERT_EQ_INT(cetcd_raft_io_timed_out(100, 5100, 5000), 1);
    CETCD_ASSERT_EQ_INT(cetcd_raft_io_timed_out(100, 200, 0), 0);
}

CETCD_TEST_CASE(auto_compact_snapshot_catchup) {
    uint64_t v = 99;
    CETCD_ASSERT_EQ_INT(cetcd_parse_snapshot_catchup_entries("5000", &v), CETCD_OK);
    CETCD_ASSERT_TRUE(v == 5000);
    CETCD_ASSERT_EQ_INT(cetcd_parse_snapshot_catchup_entries("0", &v), CETCD_OK);
    CETCD_ASSERT_TRUE(v == 0);
    CETCD_ASSERT_EQ_INT(cetcd_parse_snapshot_catchup_entries("abc", &v), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_snapshot_catchup_entries("-1", &v), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_snapshot_catchup_entries("5x", &v), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_snapshot_catchup_entries("", &v), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_snapshot_catchup_entries(NULL, &v), CETCD_ERR_INVAL);
    CETCD_ASSERT_TRUE(cetcd_server_snapshot_catchup_entries(0, 0) == 5000);
    CETCD_ASSERT_TRUE(cetcd_server_snapshot_catchup_entries(1, 0) == 0);
    CETCD_ASSERT_TRUE(cetcd_server_snapshot_catchup_entries(1, 100) == 100);
    CETCD_ASSERT_TRUE(cetcd_server_raft_compact_index(0, 5000) == 0);
    CETCD_ASSERT_TRUE(cetcd_server_raft_compact_index(100, 0) == 100);
    CETCD_ASSERT_TRUE(cetcd_server_raft_compact_index(10000, 5000) == 5000);
    CETCD_ASSERT_TRUE(cetcd_server_raft_compact_index(100, 5000) == 1);
    CETCD_ASSERT_TRUE(cetcd_server_raft_compact_index(5000, 5000) == 1);
}

CETCD_TEST_CASE(auto_compact_compact_hash_check) {
    CETCD_ASSERT_EQ_INT(cetcd_server_want_compact_hash_check(0, 1), 0);
    CETCD_ASSERT_EQ_INT(cetcd_server_want_compact_hash_check(1, 1), 1);
    CETCD_ASSERT_EQ_INT(cetcd_server_want_compact_hash_check(1, 0), 0);
    CETCD_ASSERT_TRUE(cetcd_server_compact_hash_check_ms(0, 0) == 60000);
    CETCD_ASSERT_TRUE(cetcd_server_compact_hash_check_ms(1, 0) == 0);
    CETCD_ASSERT_TRUE(cetcd_server_compact_hash_check_ms(1, 10000) == 10000);
    uint64_t last = 0;
    CETCD_ASSERT_EQ_INT(cetcd_compact_hash_check_due(NULL, 1000, 5000), 0);
    CETCD_ASSERT_EQ_INT(cetcd_compact_hash_check_due(&last, 1000, 1000), 0);
    CETCD_ASSERT_EQ_INT(cetcd_compact_hash_check_due(&last, 1000, 1999), 0);
    CETCD_ASSERT_EQ_INT(cetcd_compact_hash_check_due(&last, 1000, 2000), 1);
    last = 1;
    CETCD_ASSERT_EQ_INT(cetcd_compact_hash_check_due(&last, 0, 2), 1);
    CETCD_ASSERT_EQ_INT(cetcd_compact_hash_mismatch(0, 1, 0, 2), 0);
    CETCD_ASSERT_EQ_INT(cetcd_compact_hash_mismatch(5, 1, 6, 2), 0);
    CETCD_ASSERT_EQ_INT(cetcd_compact_hash_mismatch(5, 1, 5, 1), 0);
    CETCD_ASSERT_EQ_INT(cetcd_compact_hash_mismatch(5, 1, 5, 2), 1);
}

CETCD_TEST_CASE(auto_compact_socket_reuse_port) {
    CETCD_ASSERT_EQ_INT(cetcd_server_want_socket_reuse_port(0, 0), 0);
    CETCD_ASSERT_EQ_INT(cetcd_server_want_socket_reuse_port(0, 1), 0);
    CETCD_ASSERT_EQ_INT(cetcd_server_want_socket_reuse_port(1, 1), 1);
    CETCD_ASSERT_EQ_INT(cetcd_server_want_socket_reuse_port(1, 0), 0);
    CETCD_ASSERT_EQ_INT(cetcd_socket_reuse_port_apply(0), CETCD_OK);
#if defined(_WIN32)
    CETCD_ASSERT_EQ_INT(cetcd_socket_reuse_port_apply(1), CETCD_ERR_UNSUPPORT);
#else
    CETCD_ASSERT_EQ_INT(cetcd_socket_reuse_port_apply(1), CETCD_OK);
#endif
    CETCD_ASSERT_TRUE(cetcd_socket_reuse_port_bind_flags(0) == 0);
    CETCD_ASSERT_TRUE(cetcd_socket_reuse_port_bind_flags(1) == 2u);
}

CETCD_TEST_CASE(auto_compact_metrics_level) {
    int ext = 1;
    CETCD_ASSERT_EQ_INT(cetcd_parse_metrics_level("basic", &ext), CETCD_OK);
    CETCD_ASSERT_EQ_INT(ext, 0);
    CETCD_ASSERT_EQ_INT(cetcd_parse_metrics_level("extensive", &ext), CETCD_OK);
    CETCD_ASSERT_EQ_INT(ext, 1);
    CETCD_ASSERT_EQ_INT(cetcd_parse_metrics_level("foo", &ext), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_metrics_level("", &ext), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_metrics_level(NULL, &ext), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_metrics_level("basic", NULL), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_metrics_level("extensive ", &ext), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_server_want_metrics_extensive(0, 0), 0);
    CETCD_ASSERT_EQ_INT(cetcd_server_want_metrics_extensive(0, 1), 0);
    CETCD_ASSERT_EQ_INT(cetcd_server_want_metrics_extensive(1, 1), 1);
    CETCD_ASSERT_EQ_INT(cetcd_server_want_metrics_extensive(1, 0), 0);
}

CETCD_TEST_CASE(auto_compact_enable_pprof) {
    CETCD_ASSERT_EQ_INT(cetcd_server_want_enable_pprof(0, 0), 0);
    CETCD_ASSERT_EQ_INT(cetcd_server_want_enable_pprof(0, 1), 0);
    CETCD_ASSERT_EQ_INT(cetcd_server_want_enable_pprof(1, 1), 1);
    CETCD_ASSERT_EQ_INT(cetcd_server_want_enable_pprof(1, 0), 0);
    CETCD_ASSERT_EQ_INT(cetcd_server_metrics_route("/metrics", 8, 0), 1);
    CETCD_ASSERT_EQ_INT(cetcd_server_metrics_route("/metrics", 8, 1), 1);
    CETCD_ASSERT_EQ_INT(cetcd_server_metrics_route("/debug/pprof/heap", 18, 0), 2);
    CETCD_ASSERT_EQ_INT(cetcd_server_metrics_route("/debug/pprof/heap", 18, 1), 4);
    CETCD_ASSERT_EQ_INT(cetcd_server_metrics_route("/debug/pprof/coroutines", 24, 0), 2);
    CETCD_ASSERT_EQ_INT(cetcd_server_metrics_route("/debug/pprof/coroutines", 24, 1), 5);
    CETCD_ASSERT_EQ_INT(cetcd_server_metrics_route("/debug/pprof/profile", 20, 0), 2);
    CETCD_ASSERT_EQ_INT(cetcd_server_metrics_route("/debug/pprof/profile", 20, 1), 3);
    CETCD_ASSERT_EQ_INT(cetcd_server_metrics_route("/debug/pprof/profile?seconds=10", 31, 1), 3);
    CETCD_ASSERT_EQ_INT(cetcd_server_metrics_route("/debug/pprof/profile?seconds=10", 31, 0), 2);
    CETCD_ASSERT_EQ_INT(cetcd_server_metrics_route("/health", 7, 1), 6);
    CETCD_ASSERT_EQ_INT(cetcd_server_metrics_route("/health?serializable=true", 25, 0), 6);
    CETCD_ASSERT_EQ_INT(cetcd_server_metrics_route(NULL, 0, 1), 2);
}

CETCD_TEST_CASE(auto_compact_health) {
    char reason[32];
    CETCD_ASSERT_EQ_INT(cetcd_server_health_ok(1, 0, 0, 0, 0, 0, reason, sizeof(reason)), 1);
    CETCD_ASSERT_EQ_INT((int)reason[0], 0);
    CETCD_ASSERT_EQ_INT(cetcd_server_health_ok(0, 0, 0, 0, 0, 0, reason, sizeof(reason)), 0);
    CETCD_ASSERT_EQ_STR(reason, "RAFT NO LEADER");
    CETCD_ASSERT_EQ_INT(cetcd_server_health_ok(0, 0, 0, 1, 0, 0, reason, sizeof(reason)), 1);
    CETCD_ASSERT_EQ_INT(cetcd_server_health_ok(1, 1, 0, 0, 0, 0, reason, sizeof(reason)), 0);
    CETCD_ASSERT_EQ_STR(reason, "NOSPACE");
    CETCD_ASSERT_EQ_INT(cetcd_server_health_ok(1, 1, 0, 0, 1, 0, reason, sizeof(reason)), 1);
    CETCD_ASSERT_EQ_INT(cetcd_server_health_ok(1, 0, 1, 0, 0, 0, reason, sizeof(reason)), 0);
    CETCD_ASSERT_EQ_STR(reason, "CORRUPT");
    CETCD_ASSERT_EQ_INT(cetcd_server_health_ok(1, 0, 1, 0, 0, 1, reason, sizeof(reason)), 1);
    CETCD_ASSERT_EQ_INT(cetcd_server_health_ok(0, 1, 1, 0, 0, 0, reason, sizeof(reason)), 0);
    CETCD_ASSERT_EQ_STR(reason, "NOSPACE");

    int ser = 9, exn = 9, exc = 9;
    CETCD_ASSERT_EQ_INT(cetcd_parse_health_query(NULL, &ser, &exn, &exc), CETCD_OK);
    CETCD_ASSERT_EQ_INT(ser, 0);
    CETCD_ASSERT_EQ_INT(exn, 0);
    CETCD_ASSERT_EQ_INT(exc, 0);
    CETCD_ASSERT_EQ_INT(cetcd_parse_health_query("serializable=true", &ser, &exn, &exc),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(ser, 1);
    CETCD_ASSERT_EQ_INT(cetcd_parse_health_query("exclude=NOSPACE&exclude=CORRUPT",
                                                 &ser, &exn, &exc), CETCD_OK);
    CETCD_ASSERT_EQ_INT(ser, 0);
    CETCD_ASSERT_EQ_INT(exn, 1);
    CETCD_ASSERT_EQ_INT(exc, 1);
    CETCD_ASSERT_EQ_INT(cetcd_parse_health_query("serializable=1&exclude=NOSPACE",
                                                 &ser, &exn, &exc), CETCD_OK);
    CETCD_ASSERT_EQ_INT(ser, 1);
    CETCD_ASSERT_EQ_INT(exn, 1);
    CETCD_ASSERT_EQ_INT(exc, 0);
    CETCD_ASSERT_EQ_INT(cetcd_parse_health_query("", &ser, &exn, &exc), CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_parse_health_query("foo=bar", &ser, &exn, &exc), CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_parse_health_query("serializable=true", NULL, &exn, &exc),
                        CETCD_ERR_INVAL);

    char json[64];
    CETCD_ASSERT_EQ_INT(cetcd_server_health_json(1, NULL, json, sizeof(json)), CETCD_OK);
    CETCD_ASSERT_EQ_STR(json, "{\"health\":\"true\"}");
    CETCD_ASSERT_EQ_INT(cetcd_server_health_json(0, "NOSPACE", json, sizeof(json)),
                        CETCD_OK);
    CETCD_ASSERT_EQ_STR(json, "{\"health\":\"false\",\"reason\":\"NOSPACE\"}");
    CETCD_ASSERT_EQ_INT(cetcd_server_health_json(1, NULL, json, 8), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_server_health_json(1, NULL, NULL, 64), CETCD_ERR_INVAL);
}

CETCD_TEST_CASE(auto_compact_host_whitelist) {
    CETCD_ASSERT_EQ_INT(cetcd_server_host_whitelist_open(NULL), 1);
    CETCD_ASSERT_EQ_INT(cetcd_server_host_whitelist_open(""), 1);
    CETCD_ASSERT_EQ_INT(cetcd_server_host_whitelist_open("*"), 1);
    CETCD_ASSERT_EQ_INT(cetcd_server_host_whitelist_open("localhost, *"), 1);
    CETCD_ASSERT_EQ_INT(cetcd_server_host_whitelist_open("localhost"), 0);
    CETCD_ASSERT_EQ_INT(cetcd_server_host_allowed(NULL, "evil"), 1);
    CETCD_ASSERT_EQ_INT(cetcd_server_host_allowed("*", "evil"), 1);
    CETCD_ASSERT_EQ_INT(cetcd_server_host_allowed("localhost,127.0.0.1",
                                                 "localhost"), 1);
    CETCD_ASSERT_EQ_INT(cetcd_server_host_allowed("localhost,127.0.0.1",
                                                 "127.0.0.1"), 1);
    CETCD_ASSERT_EQ_INT(cetcd_server_host_allowed("localhost", "evil"), 0);
    CETCD_ASSERT_EQ_INT(cetcd_server_host_allowed("localhost", NULL), 0);
    CETCD_ASSERT_EQ_INT(cetcd_server_host_allowed(" localhost , 127.0.0.1 ",
                                                 "127.0.0.1"), 1);

    char name[64];
    CETCD_ASSERT_EQ_INT(cetcd_http_host_name("localhost:2381", name, sizeof(name)),
                        CETCD_OK);
    CETCD_ASSERT_EQ_STR(name, "localhost");
    CETCD_ASSERT_EQ_INT(cetcd_http_host_name("127.0.0.1", name, sizeof(name)),
                        CETCD_OK);
    CETCD_ASSERT_EQ_STR(name, "127.0.0.1");
    CETCD_ASSERT_EQ_INT(cetcd_http_host_name("[::1]:2381", name, sizeof(name)),
                        CETCD_OK);
    CETCD_ASSERT_EQ_STR(name, "::1");
    CETCD_ASSERT_EQ_INT(cetcd_http_host_name("", name, sizeof(name)),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_http_host_name(NULL, name, sizeof(name)),
                        CETCD_ERR_INVAL);

    const char *req =
        "GET /metrics HTTP/1.1\r\nHost: localhost:2381\r\n\r\n";
    CETCD_ASSERT_EQ_INT(cetcd_http_headers_complete(req, strlen(req)), 1);
    CETCD_ASSERT_EQ_INT(cetcd_http_headers_complete("GET /metrics HTTP/1.1\r\n",
                                                    22), 0);
    CETCD_ASSERT_EQ_INT(cetcd_http_header_get(req, strlen(req), "Host",
                                             name, sizeof(name)), CETCD_OK);
    CETCD_ASSERT_EQ_STR(name, "localhost:2381");
    CETCD_ASSERT_EQ_INT(cetcd_http_header_get(req, strlen(req), "X-No",
                                             name, sizeof(name)),
                        CETCD_ERR_NOTFOUND);
    CETCD_ASSERT_EQ_INT(cetcd_http_header_get(req, strlen(req), "Host",
                                             NULL, 8), CETCD_ERR_INVAL);
}

CETCD_TEST_CASE(auto_compact_wait_cluster_ready) {
    CETCD_ASSERT_EQ_INT(cetcd_server_want_wait_cluster_ready(0, 0), 0);
    CETCD_ASSERT_EQ_INT(cetcd_server_want_wait_cluster_ready(0, 1), 0);
    CETCD_ASSERT_EQ_INT(cetcd_server_want_wait_cluster_ready(1, 1), 1);
    CETCD_ASSERT_EQ_INT(cetcd_server_want_wait_cluster_ready(1, 0), 0);
    CETCD_ASSERT_EQ_INT(cetcd_server_should_listen_clients(0, 0), 1);
    CETCD_ASSERT_EQ_INT(cetcd_server_should_listen_clients(0, 1), 1);
    CETCD_ASSERT_EQ_INT(cetcd_server_should_listen_clients(1, 0), 0);
    CETCD_ASSERT_EQ_INT(cetcd_server_should_listen_clients(1, 2), 1);
}

CETCD_TEST_CASE(auto_compact_want_tick_advance) {
    CETCD_ASSERT_EQ_INT(cetcd_server_want_tick_advance(0, 0), 1);
    CETCD_ASSERT_EQ_INT(cetcd_server_want_tick_advance(0, 1), 1);
    CETCD_ASSERT_EQ_INT(cetcd_server_want_tick_advance(1, 1), 1);
    CETCD_ASSERT_EQ_INT(cetcd_server_want_tick_advance(1, 0), 0);
}

CETCD_TEST_CASE(auto_compact_parse_self_signed_cert_validity) {
    uint32_t n = 0;
    CETCD_ASSERT_EQ_INT(cetcd_parse_self_signed_cert_validity("1", &n), CETCD_OK);
    CETCD_ASSERT_EQ_INT((int)n, 1);
    CETCD_ASSERT_EQ_INT(cetcd_parse_self_signed_cert_validity("10", &n), CETCD_OK);
    CETCD_ASSERT_EQ_INT((int)n, 10);
    CETCD_ASSERT_EQ_INT(cetcd_parse_self_signed_cert_validity("0", &n),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_self_signed_cert_validity("abc", &n),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_self_signed_cert_validity("2foo", &n),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_self_signed_cert_validity("-1", &n),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_self_signed_cert_validity("", &n),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_self_signed_cert_validity(NULL, &n),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_self_signed_cert_validity("1", NULL),
                        CETCD_ERR_INVAL);
}

CETCD_TEST_CASE(auto_compact_clamp_batch) {
    CETCD_ASSERT_EQ_INT((int)cetcd_auto_compact_clamp(5000, 0, 0), 5000);
    CETCD_ASSERT_EQ_INT((int)cetcd_auto_compact_clamp(5000, 0, 1000), 1000);
    CETCD_ASSERT_EQ_INT((int)cetcd_auto_compact_clamp(7, 5, 10), 7);
    CETCD_ASSERT_EQ_INT((int)cetcd_auto_compact_clamp(0, 0, 1000), 0);
    CETCD_ASSERT_EQ_INT((int)cetcd_auto_compact_clamp(5000, 4000, 1000), 5000);
}

CETCD_TEST_CASE(auto_compact_next_revision_batches) {
    cetcd_auto_compact_state st;
    memset(&st, 0, sizeof(st));
    st.mode = CETCD_AUTO_COMPACT_REVISION;
    st.retention = 3;
    st.batch_limit = 2;
    CETCD_ASSERT_EQ_INT((int)cetcd_auto_compact_next(&st, 10, 0, 0), 2);
    CETCD_ASSERT_EQ_INT((int)cetcd_auto_compact_next(&st, 10, 2, 0), 4);
    CETCD_ASSERT_EQ_INT((int)cetcd_auto_compact_next(&st, 10, 4, 0), 6);
    CETCD_ASSERT_EQ_INT((int)cetcd_auto_compact_next(&st, 10, 6, 0), 7);
    CETCD_ASSERT_EQ_INT((int)cetcd_auto_compact_next(&st, 10, 7, 0), 0);
}

CETCD_TEST_CASE(auto_compact_next_periodic_drains) {
    cetcd_auto_compact_state st;
    memset(&st, 0, sizeof(st));
    st.mode = CETCD_AUTO_COMPACT_PERIODIC;
    st.retention = 1;
    st.batch_limit = 2;
    CETCD_ASSERT_EQ_INT((int)cetcd_auto_compact_next(&st, 5, 0, 100), 0);
    CETCD_ASSERT_EQ_INT((int)cetcd_auto_compact_next(&st, 8, 0, 1100), 2);
    CETCD_ASSERT_EQ_INT((int)cetcd_auto_compact_next(&st, 8, 2, 1100), 4);
    CETCD_ASSERT_EQ_INT((int)cetcd_auto_compact_next(&st, 8, 4, 1100), 5);
    CETCD_ASSERT_EQ_INT((int)cetcd_auto_compact_next(&st, 8, 5, 1100), 0);
}

CETCD_TEST_CASE(auto_compact_sleep_ready) {
    CETCD_ASSERT_EQ_INT(cetcd_auto_compact_sleep_ready(0, 100, 500), 1);
    CETCD_ASSERT_EQ_INT(cetcd_auto_compact_sleep_ready(100, 150, 0), 1);
    CETCD_ASSERT_EQ_INT(cetcd_auto_compact_sleep_ready(100, 150, 100), 0);
    CETCD_ASSERT_EQ_INT(cetcd_auto_compact_sleep_ready(100, 200, 100), 1);
    CETCD_ASSERT_EQ_INT(cetcd_auto_compact_sleep_ready(100, 50, 100), 1);
}

CETCD_TEST_CASE(auto_compact_next_sleeps_between_batches) {
    cetcd_auto_compact_state st;
    memset(&st, 0, sizeof(st));
    st.mode = CETCD_AUTO_COMPACT_REVISION;
    st.retention = 3;
    st.batch_limit = 2;
    st.sleep_interval_ms = 500;
    CETCD_ASSERT_EQ_INT((int)cetcd_auto_compact_next(&st, 10, 0, 1000), 2);
    CETCD_ASSERT_EQ_INT((int)cetcd_auto_compact_next(&st, 10, 2, 1200), 0);
    CETCD_ASSERT_EQ_INT((int)cetcd_auto_compact_next(&st, 10, 2, 1500), 4);
    CETCD_ASSERT_EQ_INT((int)cetcd_auto_compact_next(&st, 10, 4, 1600), 0);
    CETCD_ASSERT_EQ_INT((int)cetcd_auto_compact_next(&st, 10, 4, 2000), 6);
}

CETCD_TEST_CASE(auto_compact_parse_bootstrap_defrag_mb) {
    uint64_t n = 99;
    CETCD_ASSERT_EQ_INT(cetcd_parse_bootstrap_defrag_mb("0", &n), CETCD_OK);
    CETCD_ASSERT_TRUE(n == 0);
    CETCD_ASSERT_EQ_INT(cetcd_parse_bootstrap_defrag_mb("2", &n), CETCD_OK);
    CETCD_ASSERT_TRUE(n == 2);
    CETCD_ASSERT_EQ_INT(cetcd_parse_bootstrap_defrag_mb("abc", &n),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_bootstrap_defrag_mb("-1", &n),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_bootstrap_defrag_mb("", &n),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_bootstrap_defrag_mb(NULL, &n),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_bootstrap_defrag_mb("1", NULL),
                        CETCD_ERR_INVAL);
}

CETCD_TEST_CASE(auto_compact_should_defrag) {
    CETCD_ASSERT_EQ_INT(cetcd_backend_should_defrag(0, 0), 0);
    CETCD_ASSERT_EQ_INT(cetcd_backend_should_defrag(8 * 1024 * 1024, 0), 0);
    CETCD_ASSERT_EQ_INT(cetcd_backend_should_defrag(1024 * 1024, 1), 0);
    CETCD_ASSERT_EQ_INT(cetcd_backend_should_defrag(1024 * 1024 + 1, 1), 1);
    CETCD_ASSERT_EQ_INT(cetcd_backend_should_defrag(2 * 1024 * 1024, 1), 1);
}

CETCD_TEST_CASE(auto_compact_due_off) {
    cetcd_auto_compact_state st;
    memset(&st, 0, sizeof(st));
    CETCD_ASSERT_EQ_INT((int)cetcd_auto_compact_due(&st, 10, 0, 1000), 0);
    st.mode = CETCD_AUTO_COMPACT_REVISION;
    st.retention = 0;
    CETCD_ASSERT_EQ_INT((int)cetcd_auto_compact_due(&st, 10, 0, 1000), 0);
    CETCD_ASSERT_EQ_INT((int)cetcd_auto_compact_due(NULL, 10, 0, 1000), 0);
}

CETCD_TEST_CASE(auto_compact_etcd_config_yaml) {
    cetcd_config_pair pairs[8];
    size_t n = 99;
    CETCD_ASSERT_EQ_INT(cetcd_parse_etcd_config_yaml(
        "name: infra1\n# comment\ndata-dir: \"/var/lib/etcd\"\n"
        "client-cert-auth: true\nlisten-client-urls:\n"
        "  - http://127.0.0.1:2379\n",
        pairs, 8, &n), CETCD_OK);
    CETCD_ASSERT_TRUE(n == 4);
    CETCD_ASSERT_EQ_INT(strcmp(pairs[0].key, "name"), 0);
    CETCD_ASSERT_EQ_INT(strcmp(pairs[0].val, "infra1"), 0);
    CETCD_ASSERT_EQ_INT(strcmp(pairs[1].key, "data-dir"), 0);
    CETCD_ASSERT_EQ_INT(strcmp(pairs[1].val, "/var/lib/etcd"), 0);
    CETCD_ASSERT_EQ_INT(strcmp(pairs[2].val, "true"), 0);
    CETCD_ASSERT_EQ_INT(strcmp(pairs[3].val, "http://127.0.0.1:2379"), 0);
    CETCD_ASSERT_EQ_INT(cetcd_parse_etcd_config_yaml("", pairs, 8, &n), CETCD_OK);
    CETCD_ASSERT_TRUE(n == 0);
    CETCD_ASSERT_EQ_INT(cetcd_parse_etcd_config_yaml("---\nname: x\n...\n",
                                                     pairs, 8, &n), CETCD_OK);
    CETCD_ASSERT_TRUE(n == 1);
    CETCD_ASSERT_EQ_INT(cetcd_parse_etcd_config_yaml("nested:\n  foo: bar\n",
                                                     pairs, 8, &n),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_etcd_config_yaml("flow: {a: 1}\n",
                                                     pairs, 8, &n),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_etcd_config_yaml("not-a-key\n",
                                                     pairs, 8, &n),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_etcd_config_yaml(NULL, pairs, 8, &n),
                        CETCD_ERR_INVAL);
}

CETCD_TEST_CASE(auto_compact_config_pairs_to_flags) {
    cetcd_config_pair pairs[3];
    memset(pairs, 0, sizeof(pairs));
    strncpy(pairs[0].key, "name", sizeof(pairs[0].key) - 1);
    strncpy(pairs[0].val, "infra1", sizeof(pairs[0].val) - 1);
    strncpy(pairs[1].key, "version", sizeof(pairs[1].key) - 1);
    strncpy(pairs[1].val, "true", sizeof(pairs[1].val) - 1);
    strncpy(pairs[2].key, "client-cert-auth", sizeof(pairs[2].key) - 1);
    char *argv[8];
    char store[256];
    argv[0] = (char *)"cetcd";
    int argc = 0;
    CETCD_ASSERT_EQ_INT(cetcd_config_pairs_to_flags(pairs, 3, argv, 8,
                                                    store, sizeof(store), &argc),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(argc, 4);
    CETCD_ASSERT_EQ_INT(strcmp(argv[1], "--name"), 0);
    CETCD_ASSERT_EQ_INT(strcmp(argv[2], "infra1"), 0);
    CETCD_ASSERT_EQ_INT(strcmp(argv[3], "--client-cert-auth"), 0);
}

CETCD_TEST_CASE(auto_compact_format_etcd_version) {
    char buf[256];
    CETCD_ASSERT_EQ_INT(cetcd_format_etcd_version(buf, sizeof(buf)), CETCD_OK);
    char ver[32];
    snprintf(ver, sizeof(ver), "%u.%u.%u",
             CETCD_VERSION_MAJOR, CETCD_VERSION_MINOR, CETCD_VERSION_PATCH);
    CETCD_ASSERT_TRUE(strncmp(buf, "etcd Version: ", 14) == 0);
    CETCD_ASSERT_TRUE(strstr(buf, ver) != NULL);
    CETCD_ASSERT_TRUE(strstr(buf, "Git SHA: unknown") != NULL);
    CETCD_ASSERT_TRUE(strstr(buf, "C Standard: C11") != NULL);
    CETCD_ASSERT_TRUE(strstr(buf, "OS/Arch: ") != NULL);
    CETCD_ASSERT_EQ_INT(cetcd_format_etcd_version(NULL, 64), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_format_etcd_version(buf, 4), CETCD_ERR_INVAL);
}

CETCD_TEST_CASE(auto_compact_etcd_env) {
    char buf[64];
    CETCD_ASSERT_EQ_INT(cetcd_flag_to_etcd_env("listen-client-urls", buf,
                                               sizeof(buf)), CETCD_OK);
    CETCD_ASSERT_EQ_INT(strcmp(buf, "ETCD_LISTEN_CLIENT_URLS"), 0);
    CETCD_ASSERT_EQ_INT(cetcd_etcd_env_to_flag("ETCD_LISTEN_CLIENT_URLS", buf,
                                               sizeof(buf)), CETCD_OK);
    CETCD_ASSERT_EQ_INT(strcmp(buf, "listen-client-urls"), 0);
    CETCD_ASSERT_EQ_INT(cetcd_etcd_env_to_flag("FOO_BAR", buf, sizeof(buf)),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_etcd_env_to_flag("ETCD_", buf, sizeof(buf)),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_flag_to_etcd_env("", buf, sizeof(buf)),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_flag_to_etcd_env("listen-client-urls", buf, 8),
                        CETCD_ERR_OVERFLOW);

    char *argv[] = {(char *)"cetcd", (char *)"--metrics", (char *)"basic", NULL};
    CETCD_ASSERT_EQ_INT(cetcd_cli_flag_present(3, argv, "metrics"), 1);
    CETCD_ASSERT_EQ_INT(cetcd_cli_flag_present(3, argv, "metrics-port"), 0);
    char *argv_eq[] = {(char *)"cetcd", (char *)"--metrics-port=2381", NULL};
    CETCD_ASSERT_EQ_INT(cetcd_cli_flag_present(2, argv_eq, "metrics"), 0);
    CETCD_ASSERT_EQ_INT(cetcd_cli_flag_present(2, argv_eq, "metrics-port"), 1);
    CETCD_ASSERT_EQ_INT(cetcd_cli_flag_present(1, argv, "name"), 0);

    cetcd_config_pair pairs[8];
    size_t n = 99;
    char *envv[] = {
        (char *)"PATH=/bin",
        (char *)"ETCD_NAME=infra1",
        (char *)"ETCD_LISTEN_CLIENT_URLS=http://127.0.0.1:2379",
        (char *)"ETCD_VERSION=3.5.0",
        (char *)"ETCD_CONFIG_FILE=/tmp/x.yaml",
        (char *)"ETCD_CLIENT_CERT_AUTH=",
        NULL
    };
    char *cli[] = {(char *)"cetcd", NULL};
    CETCD_ASSERT_EQ_INT(cetcd_etcd_env_to_pairs(envv, 1, cli, pairs, 8, &n),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(n == 2);
    CETCD_ASSERT_EQ_INT(strcmp(pairs[0].key, "name"), 0);
    CETCD_ASSERT_EQ_INT(strcmp(pairs[0].val, "infra1"), 0);
    CETCD_ASSERT_EQ_INT(strcmp(pairs[1].key, "listen-client-urls"), 0);
    CETCD_ASSERT_EQ_INT(strcmp(pairs[1].val, "http://127.0.0.1:2379"), 0);

    char *cli_name[] = {(char *)"cetcd", (char *)"--name", (char *)"x", NULL};
    CETCD_ASSERT_EQ_INT(cetcd_etcd_env_to_pairs(envv, 3, cli_name, pairs, 8, &n),
                        CETCD_ERR_INVAL);
    char *cli_eq[] = {(char *)"cetcd", (char *)"--name=x", NULL};
    CETCD_ASSERT_EQ_INT(cetcd_etcd_env_to_pairs(envv, 2, cli_eq, pairs, 8, &n),
                        CETCD_ERR_INVAL);

    char *env_mp[] = {(char *)"ETCD_METRICS_PORT=9", NULL};
    CETCD_ASSERT_EQ_INT(cetcd_etcd_env_to_pairs(env_mp, 3, argv, pairs, 8, &n),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(n == 1);
    CETCD_ASSERT_EQ_INT(strcmp(pairs[0].key, "metrics-port"), 0);

    char *flag_argv[8];
    char store[256];
    flag_argv[0] = (char *)"cetcd";
    flag_argv[1] = (char *)"--data-dir";
    flag_argv[2] = (char *)"./data";
    int argc = 3;
    CETCD_ASSERT_EQ_INT(cetcd_config_pairs_to_flags(pairs, n, flag_argv, 8,
                                                    store, sizeof(store),
                                                    &argc), CETCD_OK);
    CETCD_ASSERT_EQ_INT(argc, 5);
    CETCD_ASSERT_EQ_INT(strcmp(flag_argv[1], "--data-dir"), 0);
    CETCD_ASSERT_EQ_INT(strcmp(flag_argv[3], "--metrics-port"), 0);
    CETCD_ASSERT_EQ_INT(strcmp(flag_argv[4], "9"), 0);

    CETCD_ASSERT_EQ_INT(cetcd_etcd_env_to_pairs(NULL, 1, cli, pairs, 8, &n),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(n == 0);
}

CETCD_TEST_CASE(auto_compact_experimental_unsupported) {
    CETCD_ASSERT_EQ_INT(cetcd_experimental_unsupported_kind(
        "--experimental-enable-distributed-tracing"), CETCD_EX_UNSUP_BOOL);
    CETCD_ASSERT_EQ_INT(cetcd_experimental_unsupported_kind(
        "--experimental-enable-distributed-tracing=false"),
                        CETCD_EX_UNSUP_BOOL);
    CETCD_ASSERT_EQ_INT(cetcd_experimental_unsupported_kind(
        "--experimental-distributed-tracing-address"), CETCD_EX_UNSUP_VALUE);
    CETCD_ASSERT_EQ_INT(cetcd_experimental_unsupported_kind(
        "--experimental-enable-v2v3=foo"), CETCD_EX_UNSUP_VALUE);
    CETCD_ASSERT_EQ_INT(cetcd_experimental_unsupported_kind(
        "--experimental-downgrade-check-time=5s"), CETCD_EX_UNSUP_VALUE);
    CETCD_ASSERT_EQ_INT(cetcd_experimental_unsupported_kind(
        "--experimental-peer-skip-client-san-verification"),
                        CETCD_EX_UNSUP_BOOL);
    CETCD_ASSERT_EQ_INT(cetcd_experimental_unsupported_kind(
        "--experimental-enable-lease-checkpoint"), CETCD_EX_UNSUP_BOOL);
    CETCD_ASSERT_EQ_INT(cetcd_experimental_unsupported_kind(
        "--experimental-enable-lease-checkpoint-persist=true"),
                        CETCD_EX_UNSUP_BOOL);
    CETCD_ASSERT_EQ_INT(cetcd_experimental_unsupported_kind(
        "--experimental-stop-grpc-service-on-defrag"), CETCD_EX_UNSUP_BOOL);
    CETCD_ASSERT_EQ_INT(cetcd_experimental_unsupported_kind(
        "--experimental-txn-mode-write-with-shared-buffer"),
                        CETCD_EX_UNSUP_BOOL);
    CETCD_ASSERT_EQ_INT(cetcd_experimental_unsupported_kind(
        "--experimental-distributed-tracing-sampling-rate=1.0"),
                        CETCD_EX_UNSUP_VALUE);
    CETCD_ASSERT_EQ_INT(cetcd_experimental_unsupported_kind(
        "--experimental-compaction-batch-limit"), CETCD_EX_UNSUP_NONE);
    CETCD_ASSERT_EQ_INT(cetcd_experimental_unsupported_kind(
        "--experimental-wait-cluster-ready"), CETCD_EX_UNSUP_NONE);
    CETCD_ASSERT_EQ_INT(cetcd_experimental_unsupported_kind(
        "--experimental-initial-corrupt-check"), CETCD_EX_UNSUP_NONE);
    CETCD_ASSERT_EQ_INT(cetcd_experimental_unsupported_kind(
        "--experimental-compact-hash-check-enabled"), CETCD_EX_UNSUP_NONE);
    CETCD_ASSERT_EQ_INT(cetcd_experimental_unsupported_kind(
        "--experimental-snapshot-catchup-entries=5000"), CETCD_EX_UNSUP_NONE);
    CETCD_ASSERT_EQ_INT(cetcd_experimental_unsupported_kind(
        "--experimental-memory-mlock"), CETCD_EX_UNSUP_NONE);
    CETCD_ASSERT_EQ_INT(cetcd_experimental_unsupported_kind(
        "--experimental-typo"), CETCD_EX_UNSUP_NONE);
    CETCD_ASSERT_EQ_INT(cetcd_experimental_unsupported_kind(
        "--listen-client-urls"), CETCD_EX_UNSUP_NONE);
    CETCD_ASSERT_EQ_INT(cetcd_experimental_unsupported_kind(NULL),
                        CETCD_EX_UNSUP_NONE);
}

CETCD_TEST_CASE(auto_compact_grpc_keepalive) {
    CETCD_ASSERT_EQ_INT(cetcd_grpc_keepalive_kind("--grpc-keepalive-time"),
                        CETCD_KA_IDLE);
    CETCD_ASSERT_EQ_INT(cetcd_grpc_keepalive_kind(
        "--grpc-keepalive-interval=2h"), CETCD_KA_IDLE);
    CETCD_ASSERT_EQ_INT(cetcd_grpc_keepalive_kind(
        "--grpc-keepalive-timeout"), CETCD_KA_TIMEOUT);
    CETCD_ASSERT_EQ_INT(cetcd_grpc_keepalive_kind(
        "--grpc-keepalive-min-time=5s"), CETCD_KA_MIN_TIME);
    CETCD_ASSERT_EQ_INT(cetcd_grpc_keepalive_kind(
        "--grpc-keepalive-permit-without-stream"), CETCD_KA_PERMIT);
    CETCD_ASSERT_EQ_INT(cetcd_grpc_keepalive_kind(
        "--grpc-keepalive-max-connection-idle"), CETCD_KA_UNKNOWN);
    CETCD_ASSERT_EQ_INT(cetcd_grpc_keepalive_kind(
        "--grpc-keepalive-typo=5s"), CETCD_KA_UNKNOWN);
    CETCD_ASSERT_EQ_INT(cetcd_grpc_keepalive_kind("--listen-client-urls"),
                        CETCD_KA_NONE);
    CETCD_ASSERT_EQ_INT(cetcd_grpc_keepalive_kind(NULL), CETCD_KA_NONE);

    int sec = -1;
    CETCD_ASSERT_EQ_INT(cetcd_parse_grpc_keepalive_sec("10s", 0, &sec),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(sec, 10);
    CETCD_ASSERT_EQ_INT(cetcd_parse_grpc_keepalive_sec("10", 0, &sec),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(sec, 10);
    CETCD_ASSERT_EQ_INT(cetcd_parse_grpc_keepalive_sec("2h", 0, &sec),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(sec, 7200);
    CETCD_ASSERT_EQ_INT(cetcd_parse_grpc_keepalive_sec("10m", 0, &sec),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(sec, 600);
    CETCD_ASSERT_EQ_INT(cetcd_parse_grpc_keepalive_sec("0", 0, &sec),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(sec, 0);
    CETCD_ASSERT_EQ_INT(cetcd_parse_grpc_keepalive_sec("0s", 0, &sec),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(sec, 0);
    CETCD_ASSERT_EQ_INT(cetcd_parse_grpc_keepalive_sec("500ms", 0, &sec),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(sec, 1);
    CETCD_ASSERT_EQ_INT(cetcd_parse_grpc_keepalive_sec("0", 1, &sec),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_grpc_keepalive_sec("25h", 0, &sec),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_grpc_keepalive_sec("abc", 0, &sec),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_grpc_keepalive_sec("10sfoo", 0, &sec),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_grpc_keepalive_sec(NULL, 0, &sec),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_grpc_keepalive_sec("10s", 0, NULL),
                        CETCD_ERR_INVAL);
}

CETCD_TEST_CASE(auto_compact_snapshot_count) {
    CETCD_ASSERT_TRUE(cetcd_snapshot_count_effective(0) ==
                      CETCD_DEFAULT_SNAPSHOT_COUNT);
    CETCD_ASSERT_TRUE(cetcd_snapshot_count_effective(0) == 100000ULL);
    CETCD_ASSERT_TRUE(cetcd_snapshot_count_effective(1) == 1);
    CETCD_ASSERT_TRUE(cetcd_snapshot_count_effective(10000) == 10000);
}

CETCD_TEST_CASE(auto_compact_quota_backend_bytes) {
    CETCD_ASSERT_TRUE(cetcd_quota_backend_bytes_effective(0) ==
                      CETCD_DEFAULT_QUOTA_BACKEND_BYTES);
    CETCD_ASSERT_TRUE(cetcd_quota_backend_bytes_effective(0) ==
                      2ULL * 1024 * 1024 * 1024);
    CETCD_ASSERT_TRUE(cetcd_quota_backend_bytes_effective(1) == 1);
    CETCD_ASSERT_TRUE(cetcd_quota_backend_bytes_effective(2147483648ULL) ==
                      2147483648ULL);
}

CETCD_TEST_CASE(auto_compact_cli_equals_form) {
    const char *s = NULL;
    int i;
    char *eq[] = { "p", "--name=n1" };
    char *sp[] = { "p", "--name", "n1" };
    char *cluster[] = { "p", "--initial-cluster=n1=http://127.0.0.1:2380" };
    char *bare[] = { "p", "--name" };
    char *empty[] = { "p", "--name=" };

    CETCD_ASSERT_EQ_INT(cetcd_cli_flag_is("--name", "--name"), 1);
    CETCD_ASSERT_EQ_INT(cetcd_cli_flag_is("--name=n1", "--name"), 1);
    CETCD_ASSERT_EQ_INT(cetcd_cli_flag_is("--names", "--name"), 0);
    CETCD_ASSERT_EQ_INT(cetcd_cli_flag_is("--name-extra", "--name"), 0);
    CETCD_ASSERT_EQ_INT(cetcd_cli_flag_is("--data-dir=/var/lib/etcd",
                                         "--data-dir"), 1);
    CETCD_ASSERT_EQ_INT(cetcd_cli_flag_is(NULL, "--name"), 0);

    i = 1;
    CETCD_ASSERT_EQ_INT(cetcd_take_cli_flag_value(&i, 2, eq, &s), CETCD_OK);
    CETCD_ASSERT_EQ_STR(s, "n1");
    CETCD_ASSERT_EQ_INT(i, 1);

    i = 1;
    CETCD_ASSERT_EQ_INT(cetcd_take_cli_flag_value(&i, 3, sp, &s), CETCD_OK);
    CETCD_ASSERT_EQ_STR(s, "n1");
    CETCD_ASSERT_EQ_INT(i, 2);

    i = 1;
    CETCD_ASSERT_EQ_INT(cetcd_take_cli_flag_value(&i, 2, cluster, &s),
                        CETCD_OK);
    CETCD_ASSERT_EQ_STR(s, "n1=http://127.0.0.1:2380");

    i = 1;
    CETCD_ASSERT_EQ_INT(cetcd_take_cli_flag_value(&i, 2, bare, &s),
                        CETCD_ERR_INVAL);
    i = 1;
    CETCD_ASSERT_EQ_INT(cetcd_take_cli_flag_value(&i, 2, empty, &s),
                        CETCD_ERR_INVAL);
}

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(auto_compact_parse_mode),
    CETCD_TEST_ENTRY(auto_compact_parse_retention_periodic),
    CETCD_TEST_ENTRY(auto_compact_parse_retention_revision),
    CETCD_TEST_ENTRY(auto_compact_due_revision),
    CETCD_TEST_ENTRY(auto_compact_due_periodic),
    CETCD_TEST_ENTRY(auto_compact_parse_duration_ms),
    CETCD_TEST_ENTRY(auto_compact_parse_batch_limit),
    CETCD_TEST_ENTRY(auto_compact_parse_max_concurrent_streams),
    CETCD_TEST_ENTRY(auto_compact_parse_max_learners),
    CETCD_TEST_ENTRY(auto_compact_parse_auth_token_ttl),
    CETCD_TEST_ENTRY(auto_compact_want_pre_vote),
    CETCD_TEST_ENTRY(auto_compact_parse_raft_timing_ms),
    CETCD_TEST_ENTRY(auto_compact_raft_timing_from_ms),
    CETCD_TEST_ENTRY(auto_compact_parse_listen_url),
    CETCD_TEST_ENTRY(auto_compact_parse_listen_urls),
    CETCD_TEST_ENTRY(auto_compact_parse_advertise_urls),
    CETCD_TEST_ENTRY(auto_compact_parse_metrics_listen_url),
    CETCD_TEST_ENTRY(auto_compact_parse_metrics_listen_urls),
    CETCD_TEST_ENTRY(auto_compact_metrics_addr),
    CETCD_TEST_ENTRY(auto_compact_raft_io_timeout),
    CETCD_TEST_ENTRY(auto_compact_snapshot_catchup),
    CETCD_TEST_ENTRY(auto_compact_compact_hash_check),
    CETCD_TEST_ENTRY(auto_compact_socket_reuse_port),
    CETCD_TEST_ENTRY(auto_compact_metrics_level),
    CETCD_TEST_ENTRY(auto_compact_enable_pprof),
    CETCD_TEST_ENTRY(auto_compact_health),
    CETCD_TEST_ENTRY(auto_compact_host_whitelist),
    CETCD_TEST_ENTRY(auto_compact_wait_cluster_ready),
    CETCD_TEST_ENTRY(auto_compact_want_tick_advance),
    CETCD_TEST_ENTRY(auto_compact_parse_self_signed_cert_validity),
    CETCD_TEST_ENTRY(auto_compact_clamp_batch),
    CETCD_TEST_ENTRY(auto_compact_next_revision_batches),
    CETCD_TEST_ENTRY(auto_compact_next_periodic_drains),
    CETCD_TEST_ENTRY(auto_compact_sleep_ready),
    CETCD_TEST_ENTRY(auto_compact_next_sleeps_between_batches),
    CETCD_TEST_ENTRY(auto_compact_parse_bootstrap_defrag_mb),
    CETCD_TEST_ENTRY(auto_compact_should_defrag),
    CETCD_TEST_ENTRY(auto_compact_due_off),
    CETCD_TEST_ENTRY(auto_compact_etcd_config_yaml),
    CETCD_TEST_ENTRY(auto_compact_config_pairs_to_flags),
    CETCD_TEST_ENTRY(auto_compact_format_etcd_version),
    CETCD_TEST_ENTRY(auto_compact_etcd_env),
    CETCD_TEST_ENTRY(auto_compact_experimental_unsupported),
    CETCD_TEST_ENTRY(auto_compact_grpc_keepalive),
    CETCD_TEST_ENTRY(auto_compact_quota_backend_bytes),
    CETCD_TEST_ENTRY(auto_compact_snapshot_count),
    CETCD_TEST_ENTRY(auto_compact_cli_equals_form),
CETCD_TEST_LIST_END

CETCD_TEST_MAIN()
