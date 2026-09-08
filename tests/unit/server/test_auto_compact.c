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
    CETCD_ASSERT_EQ_INT(cetcd_parse_listen_url("http://localhost:2379", host,
                                               sizeof(host), &port, &https),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(strcmp(host, "localhost"), 0);
    CETCD_ASSERT_EQ_INT((int)port, 2379);
    CETCD_ASSERT_EQ_INT(https, 0);
    CETCD_ASSERT_EQ_INT(cetcd_parse_listen_url("http://[::1]:2381", host,
                                               sizeof(host), &port, &https),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(strcmp(host, "::1"), 0);
    CETCD_ASSERT_EQ_INT((int)port, 2381);
    CETCD_ASSERT_EQ_INT(https, 0);
    CETCD_ASSERT_EQ_INT(cetcd_parse_listen_url("https://[2001:db8::1]:2379", host,
                                               sizeof(host), &port, &https),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(strcmp(host, "2001:db8::1"), 0);
    CETCD_ASSERT_EQ_INT((int)port, 2379);
    CETCD_ASSERT_EQ_INT(https, 1);
    CETCD_ASSERT_EQ_INT(cetcd_parse_listen_url("http://[::1]:2381foo", host,
                                               sizeof(host), &port, &https),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_listen_url("http://[::1]", host,
                                               sizeof(host), &port, &https),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_listen_url("http://[::]:2381", host,
                                               sizeof(host), &port, &https),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(strcmp(host, "::"), 0);
    CETCD_ASSERT_EQ_INT(cetcd_parse_listen_url("http://[]:2381", host,
                                               sizeof(host), &port, &https),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_listen_url("http://[fe80::1%1]:2379", host,
                                               sizeof(host), &port, &https),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(strcmp(host, "fe80::1%1"), 0);
    CETCD_ASSERT_EQ_INT((int)port, 2379);
    CETCD_ASSERT_EQ_INT(cetcd_parse_listen_url("http://[fe80::1%eth0]:2379", host,
                                               sizeof(host), &port, &https),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(strcmp(host, "fe80::1%eth0"), 0);
    CETCD_ASSERT_EQ_INT(cetcd_parse_listen_url("http://[fe80::1%]:2379", host,
                                               sizeof(host), &port, &https),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_listen_url("http://[fe80::1%1foo]:2379", host,
                                               sizeof(host), &port, &https),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_listen_url("http://[::1foo]:2379", host,
                                               sizeof(host), &port, &https),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_listen_url("unix:///tmp/m", host,
                                               sizeof(host), &port, &https),
                        CETCD_ERR_UNSUPPORT);
    CETCD_ASSERT_EQ_INT(cetcd_parse_listen_url("unixs://tmp/etcd.sock", host,
                                               sizeof(host), &port, &https),
                        CETCD_ERR_UNSUPPORT);
    CETCD_ASSERT_EQ_INT(cetcd_url_is_unix("unix://localhost:2379"), 1);
    CETCD_ASSERT_EQ_INT(cetcd_url_is_unix("http://127.0.0.1:2379"), 0);
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
    CETCD_ASSERT_EQ_INT(cetcd_parse_listen_urls(
        "unix://localhost:2379", urls, 4, &n), CETCD_ERR_UNSUPPORT);

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
    CETCD_ASSERT_EQ_INT(cetcd_format_listen_advertise(
        "::1", 2379, 0, NULL, 0, out, sizeof(out)), CETCD_OK);
    CETCD_ASSERT_EQ_INT(strcmp(out, "http://[::1]:2379"), 0);

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
        "--experimental-enable-lease-checkpoint"), CETCD_EX_UNSUP_NONE);
    CETCD_ASSERT_EQ_INT(cetcd_experimental_unsupported_kind(
        "--experimental-enable-lease-checkpoint=true"), CETCD_EX_UNSUP_NONE);
    CETCD_ASSERT_EQ_INT(cetcd_experimental_unsupported_kind(
        "--experimental-enable-lease-checkpoint-persist=true"),
                        CETCD_EX_UNSUP_NONE);
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

CETCD_TEST_CASE(auto_compact_etcd_compat) {
    CETCD_ASSERT_EQ_INT(cetcd_etcd_compat_kind("--enable-grpc-gateway"),
                        CETCD_COMPAT_BOOL_OFF);
    CETCD_ASSERT_EQ_INT(cetcd_etcd_compat_kind("--enable-grpc-gateway=false"),
                        CETCD_COMPAT_BOOL_OFF);
    CETCD_ASSERT_EQ_INT(cetcd_etcd_compat_kind("--enable-v2=false"),
                        CETCD_COMPAT_BOOL_OFF);
    CETCD_ASSERT_EQ_INT(cetcd_etcd_compat_kind("--unsafe-no-fsync=true"),
                        CETCD_COMPAT_BOOL_OFF);
    CETCD_ASSERT_EQ_INT(cetcd_etcd_compat_kind("--socket-reuse-address"),
                        CETCD_COMPAT_BOOL_ON);
    CETCD_ASSERT_EQ_INT(cetcd_etcd_compat_kind("--socket-reuse-address=false"),
                        CETCD_COMPAT_BOOL_ON);
    CETCD_ASSERT_EQ_INT(cetcd_etcd_compat_kind("--listen-client-http-urls"),
                        CETCD_COMPAT_VALUE);
    CETCD_ASSERT_EQ_INT(cetcd_etcd_compat_kind(
        "--listen-client-http-urls=http://127.0.0.1:2379"),
                        CETCD_COMPAT_VALUE);
    CETCD_ASSERT_EQ_INT(cetcd_etcd_compat_kind("--cors=*"),
                        CETCD_COMPAT_VALUE);
    CETCD_ASSERT_EQ_INT(cetcd_etcd_compat_kind("--discovery=http://example"),
                        CETCD_COMPAT_VALUE);
    CETCD_ASSERT_EQ_INT(cetcd_etcd_compat_kind("--max-snapshots=5"),
                        CETCD_COMPAT_VALUE);
    CETCD_ASSERT_EQ_INT(cetcd_etcd_compat_kind("--max-wals"),
                        CETCD_COMPAT_VALUE);
    CETCD_ASSERT_EQ_INT(cetcd_etcd_compat_kind("--client-cert-file=cli.crt"),
                        CETCD_COMPAT_VALUE);
    CETCD_ASSERT_EQ_INT(cetcd_etcd_compat_kind("--client-key-file"),
                        CETCD_COMPAT_VALUE);
    CETCD_ASSERT_EQ_INT(cetcd_etcd_compat_kind("--backend-batch-limit=10000"),
                        CETCD_COMPAT_VALUE);
    CETCD_ASSERT_EQ_INT(cetcd_etcd_compat_kind("--backend-batch-interval"),
                        CETCD_COMPAT_VALUE);
    CETCD_ASSERT_EQ_INT(cetcd_etcd_compat_kind(
        "--backend-bbolt-freelist-type=map"), CETCD_COMPAT_VALUE);
    CETCD_ASSERT_EQ_INT(cetcd_etcd_compat_kind("--peer-client-cert-file"),
                        CETCD_COMPAT_NONE);
    CETCD_ASSERT_EQ_INT(cetcd_etcd_compat_kind("--enable-v2v3"),
                        CETCD_COMPAT_NONE);
    CETCD_ASSERT_EQ_INT(cetcd_etcd_compat_kind("--discovery-srv"),
                        CETCD_COMPAT_NONE);
    CETCD_ASSERT_EQ_INT(cetcd_etcd_compat_kind("--listen-client-urls"),
                        CETCD_COMPAT_NONE);
    CETCD_ASSERT_EQ_INT(cetcd_etcd_compat_kind(NULL), CETCD_COMPAT_NONE);
}

CETCD_TEST_CASE(auto_compact_v2_era_flags) {
    CETCD_ASSERT_EQ_INT(cetcd_parse_v2_deprecation("gone"), CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_parse_v2_deprecation("write-only"), CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_parse_v2_deprecation("write-only-drop-data"),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_parse_v2_deprecation("write-only-skip-check"),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_parse_v2_deprecation("not-yet"), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_v2_deprecation("abc"), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_v2_deprecation(""), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_v2_deprecation(NULL), CETCD_ERR_INVAL);

    CETCD_ASSERT_EQ_INT(cetcd_parse_proxy_mode("off"), CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_parse_proxy_mode("on"), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_proxy_mode("readonly"), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_proxy_mode(NULL), CETCD_ERR_INVAL);

    CETCD_ASSERT_EQ_INT(cetcd_parse_discovery_fallback("exit"), CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_parse_discovery_fallback("proxy"),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_discovery_fallback(""), CETCD_ERR_INVAL);
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

    CETCD_ASSERT_EQ_INT(cetcd_cli_is_long_flag("--foo"), 1);
    CETCD_ASSERT_EQ_INT(cetcd_cli_is_long_flag("--foo=bar"), 1);
    CETCD_ASSERT_EQ_INT(cetcd_cli_is_long_flag("--rev"), 1);
    CETCD_ASSERT_EQ_INT(cetcd_cli_is_long_flag("--"), 0);
    CETCD_ASSERT_EQ_INT(cetcd_cli_is_long_flag("-w"), 0);
    CETCD_ASSERT_EQ_INT(cetcd_cli_is_long_flag("-1"), 0);
    CETCD_ASSERT_EQ_INT(cetcd_cli_is_long_flag("key"), 0);
    CETCD_ASSERT_EQ_INT(cetcd_cli_is_long_flag(NULL), 0);

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

    char *eat[] = { "p", "--name", "--data-dir" };
    i = 1;
    CETCD_ASSERT_EQ_INT(cetcd_take_cli_flag_value(&i, 3, eat, &s),
                        CETCD_ERR_INVAL);

    char *ddash[] = { "p", "--name", "--" };
    i = 1;
    CETCD_ASSERT_EQ_INT(cetcd_take_cli_flag_value(&i, 3, ddash, &s),
                        CETCD_ERR_INVAL);

    char *eqdash[] = { "p", "--name=--foo" };
    i = 1;
    CETCD_ASSERT_EQ_INT(cetcd_take_cli_flag_value(&i, 2, eqdash, &s),
                        CETCD_OK);
    CETCD_ASSERT_EQ_STR(s, "--foo");

    char *hw[] = { "p", "--host-whitelist", "--name" };
    i = 1;
    CETCD_ASSERT_EQ_INT(cetcd_take_cli_flag_value(&i, 3, hw, &s),
                        CETCD_ERR_INVAL);

    char *listen[] = { "p", "--listen-client-urls", "--name" };
    i = 1;
    CETCD_ASSERT_EQ_INT(cetcd_take_cli_flag_value(&i, 3, listen, &s),
                        CETCD_ERR_INVAL);

    char *hwok[] = { "p", "--host-whitelist", "localhost" };
    i = 1;
    CETCD_ASSERT_EQ_INT(cetcd_take_cli_flag_value(&i, 3, hwok, &s),
                        CETCD_OK);
    CETCD_ASSERT_EQ_STR(s, "localhost");
}

CETCD_TEST_CASE(auto_compact_cli_bool_form) {
    int on = 0;
    int i;
    char *bare[] = { "p", "--auto-tls" };
    char *eqf[] = { "p", "--auto-tls=false" };
    char *eqt[] = { "p", "--auto-tls=true" };
    char *sp[] = { "p", "--auto-tls", "false" };
    char *bad[] = { "p", "--auto-tls=abc" };

    i = 1;
    CETCD_ASSERT_EQ_INT(cetcd_take_cli_bool_flag(&i, 2, bare, &on), CETCD_OK);
    CETCD_ASSERT_EQ_INT(on, 1);
    CETCD_ASSERT_EQ_INT(i, 1);

    i = 1;
    CETCD_ASSERT_EQ_INT(cetcd_take_cli_bool_flag(&i, 2, eqf, &on), CETCD_OK);
    CETCD_ASSERT_EQ_INT(on, 0);

    i = 1;
    CETCD_ASSERT_EQ_INT(cetcd_take_cli_bool_flag(&i, 2, eqt, &on), CETCD_OK);
    CETCD_ASSERT_EQ_INT(on, 1);

    i = 1;
    CETCD_ASSERT_EQ_INT(cetcd_take_cli_bool_flag(&i, 3, sp, &on), CETCD_OK);
    CETCD_ASSERT_EQ_INT(on, 0);
    CETCD_ASSERT_EQ_INT(i, 2);

    i = 1;
    CETCD_ASSERT_EQ_INT(cetcd_take_cli_bool_flag(&i, 2, bad, &on),
                        CETCD_ERR_INVAL);
}

CETCD_TEST_CASE(auto_compact_cli_bool_eq) {
    int on = 0;
    int i;
    char *bare[] = { "p", "--debug", "version" };
    char *eqf[] = { "p", "--debug=false", "version" };
    char *eqt[] = { "p", "--debug=true" };
    char *bad[] = { "p", "--debug=abc" };

    i = 1;
    CETCD_ASSERT_EQ_INT(cetcd_take_cli_bool_eq(&i, 3, bare, &on), CETCD_OK);
    CETCD_ASSERT_EQ_INT(on, 1);
    CETCD_ASSERT_EQ_INT(i, 1);

    i = 1;
    CETCD_ASSERT_EQ_INT(cetcd_take_cli_bool_eq(&i, 3, eqf, &on), CETCD_OK);
    CETCD_ASSERT_EQ_INT(on, 0);
    CETCD_ASSERT_EQ_INT(i, 1);

    i = 1;
    CETCD_ASSERT_EQ_INT(cetcd_take_cli_bool_eq(&i, 2, eqt, &on), CETCD_OK);
    CETCD_ASSERT_EQ_INT(on, 1);

    i = 1;
    CETCD_ASSERT_EQ_INT(cetcd_take_cli_bool_eq(&i, 2, bad, &on),
                        CETCD_ERR_INVAL);
}

CETCD_TEST_CASE(auto_compact_parse_command_timeout) {
    uint64_t n = 99;
    CETCD_ASSERT_EQ_INT(cetcd_parse_command_timeout_sec("0", &n), CETCD_OK);
    CETCD_ASSERT_TRUE(n == 0);
    CETCD_ASSERT_EQ_INT(cetcd_parse_command_timeout_sec("5", &n), CETCD_OK);
    CETCD_ASSERT_TRUE(n == 5);
    CETCD_ASSERT_EQ_INT(cetcd_parse_command_timeout_sec("5s", &n), CETCD_OK);
    CETCD_ASSERT_TRUE(n == 5);
    CETCD_ASSERT_EQ_INT(cetcd_parse_command_timeout_sec("1m", &n), CETCD_OK);
    CETCD_ASSERT_TRUE(n == 60);
    CETCD_ASSERT_EQ_INT(cetcd_parse_command_timeout_sec("500ms", &n), CETCD_OK);
    CETCD_ASSERT_TRUE(n == 1);
    CETCD_ASSERT_EQ_INT(cetcd_parse_command_timeout_sec("10foo", &n),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_command_timeout_sec("5sfoo", &n),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_command_timeout_sec("", &n),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_command_timeout_sec(NULL, &n),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_command_timeout_sec("5", NULL),
                        CETCD_ERR_INVAL);
}

CETCD_TEST_CASE(auto_compact_parse_i64) {
    int64_t n = 99;
    CETCD_ASSERT_EQ_INT(cetcd_parse_i64("0", &n), CETCD_OK);
    CETCD_ASSERT_TRUE(n == 0);
    CETCD_ASSERT_EQ_INT(cetcd_parse_i64("10", &n), CETCD_OK);
    CETCD_ASSERT_TRUE(n == 10);
    CETCD_ASSERT_EQ_INT(cetcd_parse_i64("-1", &n), CETCD_OK);
    CETCD_ASSERT_TRUE(n == -1);
    CETCD_ASSERT_EQ_INT(cetcd_parse_i64("10foo", &n), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_i64("", &n), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_i64(NULL, &n), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_i64("10", NULL), CETCD_ERR_INVAL);
}

CETCD_TEST_CASE(auto_compact_encode_hashkv_request) {
    uint8_t buf[16];
    size_t n = 99;
    CETCD_ASSERT_EQ_INT(cetcd_encode_hashkv_request(0, buf, sizeof(buf), &n),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(n == 0);
    CETCD_ASSERT_EQ_INT(cetcd_encode_hashkv_request(10, buf, sizeof(buf), &n),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(n == 2);
    CETCD_ASSERT_EQ_INT((int)buf[0], 0x08);
    CETCD_ASSERT_EQ_INT((int)buf[1], 10);
    CETCD_ASSERT_EQ_INT(cetcd_encode_hashkv_request(128, buf, sizeof(buf), &n),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(n == 3);
    CETCD_ASSERT_EQ_INT((int)buf[0], 0x08);
    CETCD_ASSERT_EQ_INT((int)buf[1], 0x80);
    CETCD_ASSERT_EQ_INT((int)buf[2], 0x01);
    CETCD_ASSERT_EQ_INT(cetcd_encode_hashkv_request(-1, buf, sizeof(buf), &n),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_encode_hashkv_request(1, NULL, sizeof(buf), &n),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_encode_hashkv_request(1, buf, sizeof(buf), NULL),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_encode_hashkv_request(1, buf, 1, &n),
                        CETCD_ERR_OVERFLOW);
    int64_t rev = 0;
    CETCD_ASSERT_EQ_INT(cetcd_parse_i64("10foo", &rev), CETCD_ERR_INVAL);
}

CETCD_TEST_CASE(auto_compact_parse_hashkv_request) {
    uint8_t buf[16];
    size_t n = 0;
    int64_t rev = 99;

    CETCD_ASSERT_EQ_INT(cetcd_parse_hashkv_request(NULL, 0, &rev), CETCD_OK);
    CETCD_ASSERT_EQ_INT((int)rev, 0);
    CETCD_ASSERT_EQ_INT(cetcd_parse_hashkv_request(buf, 0, &rev), CETCD_OK);
    CETCD_ASSERT_EQ_INT((int)rev, 0);

    uint8_t dummy[] = { 0x00 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_hashkv_request(dummy, 1, &rev), CETCD_OK);
    CETCD_ASSERT_EQ_INT((int)rev, 0);

    CETCD_ASSERT_EQ_INT(cetcd_encode_hashkv_request(10, buf, sizeof(buf), &n),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_parse_hashkv_request(buf, n, &rev), CETCD_OK);
    CETCD_ASSERT_EQ_INT((int)rev, 10);

    CETCD_ASSERT_EQ_INT(cetcd_encode_hashkv_request(128, buf, sizeof(buf), &n),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_parse_hashkv_request(buf, n, &rev), CETCD_OK);
    CETCD_ASSERT_EQ_INT((int)rev, 128);

    /* leftover truncated field 1 cannot hash the live tree */
    uint8_t trunc[] = { 0x08 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_hashkv_request(trunc, 1, &rev),
                        CETCD_ERR_INVAL);
    uint8_t trunc2[] = { 0x08, 0x80 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_hashkv_request(trunc2, 2, &rev),
                        CETCD_ERR_INVAL);

    /* leftover length-delimited field is skipped, not eaten as revision */
    uint8_t skip[] = { 0x12, 0x01, 0x00, 0x08, 0x05 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_hashkv_request(skip, sizeof(skip), &rev),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT((int)rev, 5);
    uint8_t badskip[] = { 0x12, 0x05 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_hashkv_request(badskip, 2, &rev),
                        CETCD_ERR_INVAL);

    uint8_t fixed64[] = { 0x09 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_hashkv_request(fixed64, 1, &rev),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_hashkv_request(buf, n, NULL),
                        CETCD_ERR_INVAL);
}

CETCD_TEST_CASE(auto_compact_parse_compact_request) {
    int64_t rev = 99;
    int physical = 99;

    CETCD_ASSERT_EQ_INT(cetcd_parse_compact_request(NULL, 0, &rev, &physical),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT((int)rev, 0);
    CETCD_ASSERT_EQ_INT(physical, 0);

    uint8_t dummy[] = { 0x00 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_compact_request(dummy, 1, &rev, &physical),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT((int)rev, 0);

    uint8_t ten[] = { 0x08, 0x0a };
    CETCD_ASSERT_EQ_INT(cetcd_parse_compact_request(ten, 2, &rev, &physical),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT((int)rev, 10);
    CETCD_ASSERT_EQ_INT(physical, 0);

    uint8_t phys[] = { 0x08, 0x0a, 0x10, 0x01 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_compact_request(phys, 4, &rev, &physical),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT((int)rev, 10);
    CETCD_ASSERT_EQ_INT(physical, 1);

    /* leftover truncated field 1 cannot compact */
    uint8_t trunc[] = { 0x08 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_compact_request(trunc, 1, &rev, &physical),
                        CETCD_ERR_INVAL);

    /* leftover length-delimited payload cannot overwrite revision */
    uint8_t steal[] = { 0x08, 0x01, 0x12, 0x01, 0x08 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_compact_request(steal, sizeof(steal),
                                                   &rev, &physical),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT((int)rev, 1);

    uint8_t badskip[] = { 0x08, 0x01, 0x12, 0x05 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_compact_request(badskip, 4, &rev, &physical),
                        CETCD_ERR_INVAL);

    uint8_t fixed64[] = { 0x09 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_compact_request(fixed64, 1, &rev, &physical),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_compact_request(ten, 2, NULL, &physical),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_compact_request(ten, 2, &rev, NULL),
                        CETCD_ERR_INVAL);
}

CETCD_TEST_CASE(auto_compact_parse_move_leader_request) {
    uint8_t buf[16];
    size_t n = 0;
    uint64_t id = 99;

    CETCD_ASSERT_EQ_INT(cetcd_encode_move_leader_request(0, buf, sizeof(buf),
                                                        &n),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_encode_move_leader_request(2, buf, sizeof(buf),
                                                        &n),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_parse_move_leader_request(buf, n, &id),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(id == 2);

    CETCD_ASSERT_EQ_INT(cetcd_parse_move_leader_request(NULL, 0, &id),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(id == 0);
    uint8_t dummy[] = { 0x00 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_move_leader_request(dummy, 1, &id),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(id == 0);

    /* leftover truncated field 1 cannot look like a successful transfer */
    uint8_t trunc[] = { 0x08 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_move_leader_request(trunc, 1, &id),
                        CETCD_ERR_INVAL);
    uint8_t trunc2[] = { 0x08, 0x80 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_move_leader_request(trunc2, 2, &id),
                        CETCD_ERR_INVAL);

    /* leftover length-delimited payload cannot steal the target */
    uint8_t steal[] = { 0x08, 0x02, 0x12, 0x01, 0x07 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_move_leader_request(steal, sizeof(steal),
                                                       &id),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(id == 2);

    uint8_t badskip[] = { 0x08, 0x02, 0x12, 0x05 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_move_leader_request(badskip, 4, &id),
                        CETCD_ERR_INVAL);
    uint8_t fixed64[] = { 0x09 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_move_leader_request(fixed64, 1, &id),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_move_leader_request(buf, n, NULL),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_encode_move_leader_request(1, NULL, sizeof(buf),
                                                        &n),
                        CETCD_ERR_INVAL);
}

CETCD_TEST_CASE(auto_compact_parse_member_id_request) {
    uint8_t buf[16];
    size_t n = 0;
    uint64_t id = 99;

    CETCD_ASSERT_EQ_INT(cetcd_encode_member_id_request(0, buf, sizeof(buf),
                                                      &n),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_encode_member_id_request(2, buf, sizeof(buf),
                                                      &n),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_parse_member_id_request(buf, n, &id),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(id == 2);

    CETCD_ASSERT_EQ_INT(cetcd_parse_member_id_request(NULL, 0, &id),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(id == 0);
    uint8_t dummy[] = { 0x00 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_member_id_request(dummy, 1, &id),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(id == 0);

    /* leftover truncated field 1 cannot look like a successful remove */
    uint8_t trunc[] = { 0x08 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_member_id_request(trunc, 1, &id),
                        CETCD_ERR_INVAL);
    uint8_t trunc2[] = { 0x08, 0x80 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_member_id_request(trunc2, 2, &id),
                        CETCD_ERR_INVAL);

    /* leftover length-delimited payload cannot steal the id */
    uint8_t steal[] = { 0x08, 0x02, 0x12, 0x01, 0x07 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_member_id_request(steal, sizeof(steal),
                                                     &id),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(id == 2);

    uint8_t badskip[] = { 0x08, 0x02, 0x12, 0x05 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_member_id_request(badskip, 4, &id),
                        CETCD_ERR_INVAL);
    uint8_t fixed64[] = { 0x09 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_member_id_request(fixed64, 1, &id),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_member_id_request(buf, n, NULL),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_encode_member_id_request(1, NULL, sizeof(buf),
                                                      &n),
                        CETCD_ERR_INVAL);
}

CETCD_TEST_CASE(auto_compact_parse_member_update_request) {
    uint8_t buf[64];
    size_t n = 0;
    uint64_t id = 99;
    char url[64];

    CETCD_ASSERT_EQ_INT(cetcd_encode_member_update_request(0, "127.0.0.1:2380",
                                                           buf, sizeof(buf),
                                                           &n),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_encode_member_update_request(2, "",
                                                           buf, sizeof(buf),
                                                           &n),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_encode_member_update_request(
                            2, "127.0.0.1:2380", buf, sizeof(buf), &n),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_parse_member_update_request(buf, n, &id, url,
                                                         sizeof(url)),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(id == 2);
    CETCD_ASSERT_EQ_STR(url, "127.0.0.1:2380");

    CETCD_ASSERT_EQ_INT(cetcd_parse_member_update_request(NULL, 0, &id, url,
                                                         sizeof(url)),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(id == 0);
    CETCD_ASSERT_EQ_STR(url, "");
    uint8_t dummy[] = { 0x00 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_member_update_request(dummy, 1, &id, url,
                                                         sizeof(url)),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(id == 0);

    /* leftover truncated field 1 cannot look like a successful update */
    uint8_t trunc[] = { 0x08 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_member_update_request(trunc, 1, &id, url,
                                                         sizeof(url)),
                        CETCD_ERR_INVAL);
    uint8_t trunc2[] = { 0x08, 0x80 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_member_update_request(trunc2, 2, &id, url,
                                                         sizeof(url)),
                        CETCD_ERR_INVAL);

    /* leftover length-delimited payload cannot steal the id */
    uint8_t steal[] = { 0x08, 0x02, 0x1a, 0x01, 0x07 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_member_update_request(steal, sizeof(steal),
                                                         &id, url,
                                                         sizeof(url)),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(id == 2);

    /* leftover truncated peerURL length cannot walk off the buffer */
    uint8_t badurl[] = { 0x08, 0x02, 0x12, 0x05 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_member_update_request(badurl, 4, &id, url,
                                                         sizeof(url)),
                        CETCD_ERR_INVAL);
    uint8_t fixed64[] = { 0x09 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_member_update_request(fixed64, 1, &id, url,
                                                         sizeof(url)),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_member_update_request(buf, n, NULL, url,
                                                         sizeof(url)),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_encode_member_update_request(
                            1, "x", NULL, sizeof(buf), &n),
                        CETCD_ERR_INVAL);
}

CETCD_TEST_CASE(auto_compact_parse_member_add_request) {
    uint8_t buf[64];
    size_t n = 0;
    char url[64];
    int learner = 9;

    CETCD_ASSERT_EQ_INT(cetcd_encode_member_add_request(NULL, 0, buf,
                                                       sizeof(buf), &n),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_encode_member_add_request("", 0, buf,
                                                       sizeof(buf), &n),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_encode_member_add_request(
                            "127.0.0.1:2380", 1, buf, sizeof(buf), &n),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_parse_member_add_request(buf, n, url,
                                                      sizeof(url), &learner),
                        CETCD_OK);
    CETCD_ASSERT_EQ_STR(url, "127.0.0.1:2380");
    CETCD_ASSERT_EQ_INT(learner, 1);

    CETCD_ASSERT_EQ_INT(cetcd_parse_member_add_request(NULL, 0, url,
                                                      sizeof(url), &learner),
                        CETCD_OK);
    CETCD_ASSERT_EQ_STR(url, "");
    CETCD_ASSERT_EQ_INT(learner, 0);
    uint8_t dummy[] = { 0x00 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_member_add_request(dummy, 1, url,
                                                      sizeof(url), &learner),
                        CETCD_OK);
    CETCD_ASSERT_EQ_STR(url, "");
    CETCD_ASSERT_EQ_INT(learner, 0);

    /* leftover truncated peerURL cannot look like a successful add */
    uint8_t trunc[] = { 0x0a };
    CETCD_ASSERT_EQ_INT(cetcd_parse_member_add_request(trunc, 1, url,
                                                      sizeof(url), &learner),
                        CETCD_ERR_INVAL);
    uint8_t trunc_learn[] = { 0x0a, 0x0e, '1', '2', '7', '.', '0', '.', '0',
                              '.', '1', ':', '2', '3', '8', '0', 0x10 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_member_add_request(
                            trunc_learn, sizeof(trunc_learn), url, sizeof(url),
                            &learner),
                        CETCD_ERR_INVAL);

    /* leftover cannot steal the peerURL */
    uint8_t steal_url[] = {
        0x1a, 0x10, 0x0a, 0x0e,
        '1', '2', '7', '.', '0', '.', '0', '.', '1', ':', '2', '3', '8', '0'
    };
    CETCD_ASSERT_EQ_INT(cetcd_parse_member_add_request(
                            steal_url, sizeof(steal_url), url, sizeof(url),
                            &learner),
                        CETCD_OK);
    CETCD_ASSERT_EQ_STR(url, "");

    /* leftover cannot steal isLearner */
    uint8_t steal_learn[] = {
        0x0a, 0x0e,
        '1', '2', '7', '.', '0', '.', '0', '.', '1', ':', '2', '3', '8', '0',
        0x22, 0x02, 0x10, 0x01
    };
    CETCD_ASSERT_EQ_INT(cetcd_parse_member_add_request(
                            steal_learn, sizeof(steal_learn), url, sizeof(url),
                            &learner),
                        CETCD_OK);
    CETCD_ASSERT_EQ_STR(url, "127.0.0.1:2380");
    CETCD_ASSERT_EQ_INT(learner, 0);

    uint8_t fixed64[] = { 0x09 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_member_add_request(fixed64, 1, url,
                                                      sizeof(url), &learner),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_member_add_request(buf, n, NULL, 0, NULL),
                        CETCD_ERR_INVAL);
}

CETCD_TEST_CASE(auto_compact_parse_downgrade_request) {
    uint8_t buf[64];
    size_t n = 0;
    char ver[32];
    int action = 9;

    CETCD_ASSERT_EQ_INT(cetcd_encode_downgrade_request(-1, "0.3.0", buf,
                                                      sizeof(buf), &n),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_encode_downgrade_request(0, "0.3.0", buf,
                                                      sizeof(buf), &n),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_parse_downgrade_request(buf, n, &action, ver,
                                                     sizeof(ver)),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(action, 0);
    CETCD_ASSERT_EQ_STR(ver, "0.3.0");

    CETCD_ASSERT_EQ_INT(cetcd_encode_downgrade_request(1, NULL, buf,
                                                      sizeof(buf), &n),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_parse_downgrade_request(buf, n, &action, ver,
                                                     sizeof(ver)),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(action, 1);
    CETCD_ASSERT_EQ_STR(ver, "");

    CETCD_ASSERT_EQ_INT(cetcd_parse_downgrade_request(NULL, 0, &action, ver,
                                                     sizeof(ver)),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(action, 0);
    CETCD_ASSERT_EQ_STR(ver, "");
    uint8_t dummy[] = { 0x00 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_downgrade_request(dummy, 1, &action, ver,
                                                     sizeof(ver)),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(action, 0);
    CETCD_ASSERT_EQ_STR(ver, "");

    /* leftover truncated action cannot look like VALIDATE */
    uint8_t trunc[] = { 0x08 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_downgrade_request(trunc, 1, &action, ver,
                                                     sizeof(ver)),
                        CETCD_ERR_INVAL);
    uint8_t trunc_ver[] = { 0x08, 0x00, 0x12 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_downgrade_request(trunc_ver,
                                                     sizeof(trunc_ver),
                                                     &action, ver,
                                                     sizeof(ver)),
                        CETCD_ERR_INVAL);

    /* leftover cannot steal ENABLE */
    uint8_t steal_act[] = { 0x1a, 0x02, 0x08, 0x01 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_downgrade_request(steal_act,
                                                     sizeof(steal_act),
                                                     &action, ver,
                                                     sizeof(ver)),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(action, 0);

    /* leftover cannot steal version */
    uint8_t steal_ver[] = {
        0x08, 0x00, 0x22, 0x07, 0x12, 0x05, '0', '.', '3', '.', '0'
    };
    CETCD_ASSERT_EQ_INT(cetcd_parse_downgrade_request(steal_ver,
                                                     sizeof(steal_ver),
                                                     &action, ver,
                                                     sizeof(ver)),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(action, 0);
    CETCD_ASSERT_EQ_STR(ver, "");

    uint8_t fixed64[] = { 0x09 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_downgrade_request(fixed64, 1, &action, ver,
                                                     sizeof(ver)),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_downgrade_request(buf, n, NULL, NULL, 0),
                        CETCD_ERR_INVAL);
}

CETCD_TEST_CASE(auto_compact_parse_txn_op_key) {
    uint8_t buf[64];
    size_t n = 0;
    char key[32];
    int want_write = 0;

    CETCD_ASSERT_EQ_INT(cetcd_encode_txn_op_put(NULL, 0, buf, sizeof(buf), &n),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_encode_txn_op_put((const uint8_t *)"foo", 3, buf,
                                               sizeof(buf), &n),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_parse_txn_op_key(buf, n, &want_write, key,
                                              sizeof(key)),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(want_write, 1);
    CETCD_ASSERT_EQ_STR(key, "foo");

    CETCD_ASSERT_EQ_INT(cetcd_parse_txn_op_key(NULL, 0, &want_write, key,
                                              sizeof(key)),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(want_write, 1);
    CETCD_ASSERT_EQ_STR(key, "");
    uint8_t dummy[] = { 0x00 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_txn_op_key(dummy, 1, &want_write, key,
                                              sizeof(key)),
                        CETCD_OK);
    CETCD_ASSERT_EQ_STR(key, "");

    /* leftover dummy 0x00 cannot eat the key tag */
    uint8_t dummy_key[] = {
        0x12, 0x09, 0x00, 0x0a, 0x03, 'f', 'o', 'o', 0x12, 0x01, 'v'
    };
    CETCD_ASSERT_EQ_INT(cetcd_parse_txn_op_key(dummy_key, sizeof(dummy_key),
                                              &want_write, key, sizeof(key)),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(want_write, 1);
    CETCD_ASSERT_EQ_STR(key, "foo");

    /* leftover truncated key cannot look like a missing-key allow */
    uint8_t trunc[] = { 0x12, 0x01, 0x0a };
    CETCD_ASSERT_EQ_INT(cetcd_parse_txn_op_key(trunc, sizeof(trunc),
                                              &want_write, key, sizeof(key)),
                        CETCD_ERR_INVAL);

    /* leftover cannot steal the key */
    uint8_t steal[] = {
        0x12, 0x07, 0x22, 0x05, 0x0a, 0x03, 'f', 'o', 'o'
    };
    CETCD_ASSERT_EQ_INT(cetcd_parse_txn_op_key(steal, sizeof(steal),
                                              &want_write, key, sizeof(key)),
                        CETCD_OK);
    CETCD_ASSERT_EQ_STR(key, "");

    uint8_t range[] = { 0x0a, 0x05, 0x0a, 0x03, 'f', 'o', 'o' };
    CETCD_ASSERT_EQ_INT(cetcd_parse_txn_op_key(range, sizeof(range),
                                              &want_write, key, sizeof(key)),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(want_write, 0);
    CETCD_ASSERT_EQ_STR(key, "foo");

    uint8_t fixed64[] = { 0x09 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_txn_op_key(fixed64, 1, &want_write, key,
                                              sizeof(key)),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_txn_op_key(buf, n, NULL, NULL, 0),
                        CETCD_ERR_INVAL);
}

CETCD_TEST_CASE(auto_compact_parse_lease_grant_request) {
    int64_t ttl = 99, id = 99;

    CETCD_ASSERT_EQ_INT(cetcd_parse_lease_grant_request(NULL, 0, &ttl, &id),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(ttl == 0);
    CETCD_ASSERT_TRUE(id == 0);

    uint8_t dummy[] = { 0x00 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_lease_grant_request(dummy, 1, &ttl, &id),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(ttl == 0);

    uint8_t sixty[] = { 0x08, 0x3c };
    CETCD_ASSERT_EQ_INT(cetcd_parse_lease_grant_request(sixty, 2, &ttl, &id),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(ttl == 60);
    CETCD_ASSERT_TRUE(id == 0);

    uint8_t custom[] = { 0x08, 0x3c, 0x10, 0x02 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_lease_grant_request(custom, 4, &ttl, &id),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(ttl == 60);
    CETCD_ASSERT_TRUE(id == 2);

    /* leftover truncated TTL cannot grant a 60s lease */
    uint8_t trunc[] = { 0x08 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_lease_grant_request(trunc, 1, &ttl, &id),
                        CETCD_ERR_INVAL);
    uint8_t trunc2[] = { 0x08, 0x80 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_lease_grant_request(trunc2, 2, &ttl, &id),
                        CETCD_ERR_INVAL);

    /* leftover length-delimited payload cannot steal TTL */
    uint8_t steal[] = { 0x08, 0x0a, 0x12, 0x01, 0x3c };
    CETCD_ASSERT_EQ_INT(cetcd_parse_lease_grant_request(steal, sizeof(steal),
                                                       &ttl, &id),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(ttl == 10);

    uint8_t badskip[] = { 0x08, 0x0a, 0x12, 0x05 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_lease_grant_request(badskip, 4, &ttl, &id),
                        CETCD_ERR_INVAL);
    uint8_t fixed64[] = { 0x09 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_lease_grant_request(fixed64, 1, &ttl, &id),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_lease_grant_request(sixty, 2, NULL, &id),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_lease_grant_request(sixty, 2, &ttl, NULL),
                        CETCD_ERR_INVAL);
}

CETCD_TEST_CASE(auto_compact_parse_lease_id_request) {
    uint8_t buf[16];
    size_t n = 0;
    int64_t id = 99;
    int keys = 1;

    CETCD_ASSERT_EQ_INT(cetcd_encode_lease_id_request(0, buf, sizeof(buf), &n),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_encode_lease_id_request(2, buf, sizeof(buf), &n),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_parse_lease_id_request(buf, n, &id, &keys),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(id == 2);
    CETCD_ASSERT_EQ_INT(keys, 0);

    CETCD_ASSERT_EQ_INT(cetcd_parse_lease_id_request(NULL, 0, &id, &keys),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(id == 0);
    uint8_t dummy[] = { 0x00 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_lease_id_request(dummy, 1, &id, NULL),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(id == 0);

    /* leftover truncated field 1 cannot look like a keepalive / revoke */
    uint8_t trunc[] = { 0x08 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_lease_id_request(trunc, 1, &id, NULL),
                        CETCD_ERR_INVAL);
    uint8_t trunc2[] = { 0x08, 0x80 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_lease_id_request(trunc2, 2, &id, NULL),
                        CETCD_ERR_INVAL);

    /* leftover length-delimited payload cannot inject a fake id */
    uint8_t steal[] = { 0x12, 0x02, 0x08, 0x63 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_lease_id_request(steal, sizeof(steal),
                                                    &id, NULL),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(id == 0);
    uint8_t keep[] = { 0x08, 0x02, 0x12, 0x01, 0x07 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_lease_id_request(keep, sizeof(keep),
                                                    &id, NULL),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(id == 2);

    uint8_t keys_on[] = { 0x08, 0x02, 0x10, 0x01 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_lease_id_request(keys_on, 4, &id, &keys),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(id == 2);
    CETCD_ASSERT_EQ_INT(keys, 1);

    uint8_t badskip[] = { 0x08, 0x02, 0x12, 0x05 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_lease_id_request(badskip, 4, &id, NULL),
                        CETCD_ERR_INVAL);
    uint8_t fixed64[] = { 0x09 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_lease_id_request(fixed64, 1, &id, NULL),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_lease_id_request(buf, n, NULL, NULL),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_encode_lease_id_request(1, NULL, sizeof(buf),
                                                     &n),
                        CETCD_ERR_INVAL);
}

CETCD_TEST_CASE(auto_compact_parse_alarm_request) {
    uint8_t buf[16];
    size_t n = 0;
    int action = 99;
    uint64_t member = 99;
    int alarm = 99;

    CETCD_ASSERT_EQ_INT(cetcd_encode_alarm_request(-1, 0, 1, buf, sizeof(buf),
                                                   &n),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_encode_alarm_request(1, 0, 1, buf, sizeof(buf),
                                                   &n),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_parse_alarm_request(buf, n, &action, &member,
                                                 &alarm),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(action, 1);
    CETCD_ASSERT_TRUE(member == 0);
    CETCD_ASSERT_EQ_INT(alarm, 1);

    CETCD_ASSERT_EQ_INT(cetcd_parse_alarm_request(NULL, 0, &action, &member,
                                                 &alarm),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(action, 0);
    uint8_t dummy[] = { 0x00 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_alarm_request(dummy, 1, &action, &member,
                                                 &alarm),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(action, 0);

    /* leftover truncated action cannot look like GET */
    uint8_t trunc[] = { 0x08 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_alarm_request(trunc, 1, &action, &member,
                                                 &alarm),
                        CETCD_ERR_INVAL);
    uint8_t trunc2[] = { 0x08, 0x80 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_alarm_request(trunc2, 2, &action, &member,
                                                 &alarm),
                        CETCD_ERR_INVAL);

    /* leftover length-delimited payload cannot steal ACTIVATE */
    uint8_t steal[] = { 0x08, 0x00, 0x12, 0x04, 0x08, 0x01, 0x18, 0x01 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_alarm_request(steal, sizeof(steal),
                                                 &action, &member, &alarm),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(action, 0);
    CETCD_ASSERT_EQ_INT(alarm, 0);

    uint8_t badskip[] = { 0x08, 0x00, 0x12, 0x05 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_alarm_request(badskip, 4, &action, &member,
                                                 &alarm),
                        CETCD_ERR_INVAL);
    uint8_t fixed64[] = { 0x09 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_alarm_request(fixed64, 1, &action, &member,
                                                 &alarm),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_alarm_request(buf, n, NULL, &member,
                                                 &alarm),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_encode_alarm_request(0, 0, 0, NULL, sizeof(buf),
                                                   &n),
                        CETCD_ERR_INVAL);
}

CETCD_TEST_CASE(auto_compact_parse_range_request) {
    uint8_t buf[32];
    size_t n = 0;
    cetcd_range_request rr;

    CETCD_ASSERT_EQ_INT(cetcd_encode_range_request((const uint8_t *)"k", 1, -1,
                                                   buf, sizeof(buf), &n),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_encode_range_request((const uint8_t *)"k", 1, 1,
                                                   buf, sizeof(buf), &n),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_parse_range_request(buf, n, &rr), CETCD_OK);
    CETCD_ASSERT_TRUE(rr.key_len == 1);
    CETCD_ASSERT_TRUE(rr.key && rr.key[0] == 'k');
    CETCD_ASSERT_TRUE(rr.rev == 1);
    cetcd_range_request_clear(&rr);

    CETCD_ASSERT_EQ_INT(cetcd_parse_range_request(NULL, 0, &rr), CETCD_OK);
    CETCD_ASSERT_TRUE(rr.rev == 0);
    CETCD_ASSERT_TRUE(rr.key == NULL);
    uint8_t dummy[] = { 0x00 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_range_request(dummy, 1, &rr), CETCD_OK);
    CETCD_ASSERT_TRUE(rr.rev == 0);

    /* leftover truncated --rev cannot range the live tree */
    uint8_t trunc[] = { 0x0a, 0x01, 'k', 0x20 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_range_request(trunc, 4, &rr),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_TRUE(rr.key == NULL);

    /* leftover length-delimited payload cannot steal rev */
    uint8_t steal[] = { 0x0a, 0x01, 'k', 0x20, 0x01, 0x72, 0x02, 0x20, 0x05 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_range_request(steal, sizeof(steal), &rr),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(rr.rev == 1);
    CETCD_ASSERT_TRUE(rr.limit == 0);
    cetcd_range_request_clear(&rr);

    uint8_t badskip[] = { 0x0a, 0x01, 'k', 0x72, 0x05 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_range_request(badskip, 5, &rr),
                        CETCD_ERR_INVAL);
    uint8_t fixed64[] = { 0x09 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_range_request(fixed64, 1, &rr),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_range_request(buf, n, NULL),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_encode_range_request((const uint8_t *)"k", 1, 1,
                                                   NULL, sizeof(buf), &n),
                        CETCD_ERR_INVAL);
}

CETCD_TEST_CASE(auto_compact_parse_put_request) {
    uint8_t buf[32];
    size_t n = 0;
    cetcd_put_request pr;

    CETCD_ASSERT_EQ_INT(cetcd_encode_put_request((const uint8_t *)"k", 1,
                                                 (const uint8_t *)"v", 1, -1,
                                                 buf, sizeof(buf), &n),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_encode_put_request((const uint8_t *)"k", 1,
                                                 (const uint8_t *)"v", 1, 2,
                                                 buf, sizeof(buf), &n),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_parse_put_request(buf, n, &pr), CETCD_OK);
    CETCD_ASSERT_TRUE(pr.key_len == 1 && pr.key && pr.key[0] == 'k');
    CETCD_ASSERT_TRUE(pr.value_len == 1 && pr.value && pr.value[0] == 'v');
    CETCD_ASSERT_TRUE(pr.lease == 2);
    cetcd_put_request_clear(&pr);

    CETCD_ASSERT_EQ_INT(cetcd_parse_put_request(NULL, 0, &pr), CETCD_OK);
    CETCD_ASSERT_TRUE(pr.lease == 0);
    CETCD_ASSERT_TRUE(pr.key == NULL);
    uint8_t dummy[] = { 0x00 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_put_request(dummy, 1, &pr), CETCD_OK);
    CETCD_ASSERT_TRUE(pr.lease == 0);

    /* leftover truncated --lease cannot look like a no-lease put */
    uint8_t trunc[] = { 0x0a, 0x01, 'k', 0x12, 0x01, 'v', 0x18 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_put_request(trunc, 7, &pr),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_TRUE(pr.key == NULL);

    /* leftover length-delimited payload cannot steal the lease */
    uint8_t steal[] = { 0x0a, 0x01, 'k', 0x12, 0x01, 'v',
                        0x3a, 0x02, 0x18, 0x63 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_put_request(steal, sizeof(steal), &pr),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(pr.lease == 0);
    cetcd_put_request_clear(&pr);

    uint8_t badskip[] = { 0x0a, 0x01, 'k', 0x3a, 0x05 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_put_request(badskip, 5, &pr),
                        CETCD_ERR_INVAL);
    uint8_t fixed64[] = { 0x09 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_put_request(fixed64, 1, &pr),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_put_request(buf, n, NULL),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_encode_put_request((const uint8_t *)"k", 1,
                                                 (const uint8_t *)"v", 1, 0,
                                                 NULL, sizeof(buf), &n),
                        CETCD_ERR_INVAL);
}

CETCD_TEST_CASE(auto_compact_parse_delete_range_request) {
    uint8_t buf[32];
    size_t n = 0;
    cetcd_delete_range_request dr;

    CETCD_ASSERT_EQ_INT(cetcd_encode_delete_range_request(
                            NULL, 0, NULL, 0, 0, buf, sizeof(buf), &n),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_encode_delete_range_request(
                            (const uint8_t *)"a", 1, (const uint8_t *)"z", 1, 1,
                            buf, sizeof(buf), &n),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_parse_delete_range_request(buf, n, &dr),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(dr.key_len == 1 && dr.key && dr.key[0] == 'a');
    CETCD_ASSERT_TRUE(dr.range_end_len == 1 && dr.range_end &&
                      dr.range_end[0] == 'z');
    CETCD_ASSERT_EQ_INT(dr.prev_kv, 1);
    cetcd_delete_range_request_clear(&dr);

    CETCD_ASSERT_EQ_INT(cetcd_parse_delete_range_request(NULL, 0, &dr),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(dr.key == NULL);
    uint8_t dummy[] = { 0x00 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_delete_range_request(dummy, 1, &dr),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(dr.range_end == NULL);

    /* leftover truncated range_end cannot look like a point delete */
    uint8_t trunc[] = { 0x0a, 0x01, 'a', 0x12 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_delete_range_request(trunc, 4, &dr),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_TRUE(dr.key == NULL);
    uint8_t trunc_prev[] = { 0x0a, 0x01, 'a', 0x18 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_delete_range_request(trunc_prev, 4, &dr),
                        CETCD_ERR_INVAL);

    /* leftover length-delimited payload cannot steal range_end */
    uint8_t steal[] = { 0x0a, 0x01, 'a', 0x22, 0x03, 0x12, 0x01, 'z' };
    CETCD_ASSERT_EQ_INT(cetcd_parse_delete_range_request(steal, sizeof(steal),
                                                        &dr),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(dr.range_end == NULL);
    CETCD_ASSERT_TRUE(dr.key_len == 1 && dr.key && dr.key[0] == 'a');
    cetcd_delete_range_request_clear(&dr);

    uint8_t badskip[] = { 0x0a, 0x01, 'a', 0x22, 0x05 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_delete_range_request(badskip, 5, &dr),
                        CETCD_ERR_INVAL);
    uint8_t fixed64[] = { 0x09 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_delete_range_request(fixed64, 1, &dr),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_delete_range_request(buf, n, NULL),
                        CETCD_ERR_INVAL);
}

CETCD_TEST_CASE(auto_compact_parse_auth_name_pass_request) {
    uint8_t buf[32];
    size_t n = 0;
    cetcd_auth_name_pass_request ap;

    CETCD_ASSERT_EQ_INT(cetcd_encode_auth_name_pass_request(
                            NULL, 0, NULL, 0, buf, sizeof(buf), &n),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_encode_auth_name_pass_request(
                            (const uint8_t *)"root", 4,
                            (const uint8_t *)"secret", 6, buf, sizeof(buf), &n),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_parse_auth_name_pass_request(buf, n, &ap),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(ap.name_len == 4 && ap.name &&
                      memcmp(ap.name, "root", 4) == 0);
    CETCD_ASSERT_TRUE(ap.password_len == 6 && ap.password &&
                      memcmp(ap.password, "secret", 6) == 0);
    cetcd_auth_name_pass_request_clear(&ap);

    CETCD_ASSERT_EQ_INT(cetcd_parse_auth_name_pass_request(NULL, 0, &ap),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(ap.name == NULL);
    uint8_t dummy[] = { 0x00 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_auth_name_pass_request(dummy, 1, &ap),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(ap.password == NULL);

    /* leftover truncated password cannot look like a name-only authenticate */
    uint8_t trunc[] = { 0x0a, 0x04, 'r', 'o', 'o', 't', 0x12 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_auth_name_pass_request(trunc, 7, &ap),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_TRUE(ap.name == NULL);

    /* leftover length-delimited payload cannot steal password */
    uint8_t steal[] = { 0x0a, 0x04, 'r', 'o', 'o', 't',
                        0x22, 0x08, 0x12, 0x06, 's', 'e', 'c', 'r', 'e', 't' };
    CETCD_ASSERT_EQ_INT(cetcd_parse_auth_name_pass_request(steal, sizeof(steal),
                                                          &ap),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(ap.password == NULL);
    CETCD_ASSERT_TRUE(ap.name_len == 4 && ap.name &&
                      memcmp(ap.name, "root", 4) == 0);
    cetcd_auth_name_pass_request_clear(&ap);

    uint8_t badskip[] = { 0x0a, 0x04, 'r', 'o', 'o', 't', 0x22, 0x05 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_auth_name_pass_request(badskip, 8, &ap),
                        CETCD_ERR_INVAL);
    uint8_t fixed64[] = { 0x09 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_auth_name_pass_request(fixed64, 1, &ap),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_auth_name_pass_request(buf, n, NULL),
                        CETCD_ERR_INVAL);
}

CETCD_TEST_CASE(auto_compact_parse_user_add_request) {
    uint8_t buf[32];
    size_t n = 0;
    cetcd_user_add_request ua;

    CETCD_ASSERT_EQ_INT(cetcd_encode_user_add_request(
                            NULL, 0, NULL, 0, 0, buf, sizeof(buf), &n),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_encode_user_add_request(
                            (const uint8_t *)"alice", 5,
                            (const uint8_t *)"pass", 4, 1, buf, sizeof(buf),
                            &n),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_parse_user_add_request(buf, n, &ua), CETCD_OK);
    CETCD_ASSERT_TRUE(ua.name_len == 5 && ua.name &&
                      memcmp(ua.name, "alice", 5) == 0);
    CETCD_ASSERT_TRUE(ua.password_len == 4 && ua.password &&
                      memcmp(ua.password, "pass", 4) == 0);
    CETCD_ASSERT_EQ_INT(ua.no_password, 1);
    cetcd_user_add_request_clear(&ua);

    CETCD_ASSERT_EQ_INT(cetcd_parse_user_add_request(NULL, 0, &ua), CETCD_OK);
    CETCD_ASSERT_TRUE(ua.name == NULL);
    uint8_t dummy[] = { 0x00 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_user_add_request(dummy, 1, &ua), CETCD_OK);
    CETCD_ASSERT_TRUE(ua.no_password == 0);

    /* leftover truncated password cannot look like a name-only add */
    uint8_t trunc[] = { 0x0a, 0x05, 'a', 'l', 'i', 'c', 'e', 0x12 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_user_add_request(trunc, 8, &ua),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_TRUE(ua.name == NULL);
    uint8_t trunc_opt[] = { 0x0a, 0x05, 'a', 'l', 'i', 'c', 'e', 0x1a };
    CETCD_ASSERT_EQ_INT(cetcd_parse_user_add_request(trunc_opt, 8, &ua),
                        CETCD_ERR_INVAL);

    /* leftover length-delimited payload cannot steal password */
    uint8_t steal[] = { 0x0a, 0x05, 'a', 'l', 'i', 'c', 'e',
                        0x22, 0x06, 0x12, 0x04, 'p', 'a', 's', 's' };
    CETCD_ASSERT_EQ_INT(cetcd_parse_user_add_request(steal, sizeof(steal), &ua),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(ua.password == NULL);
    CETCD_ASSERT_TRUE(ua.name_len == 5);
    cetcd_user_add_request_clear(&ua);

    /* leftover cannot steal no_password */
    uint8_t steal_np[] = { 0x0a, 0x05, 'a', 'l', 'i', 'c', 'e',
                           0x22, 0x04, 0x1a, 0x02, 0x08, 0x01 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_user_add_request(steal_np, sizeof(steal_np),
                                                    &ua),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(ua.no_password, 0);
    cetcd_user_add_request_clear(&ua);

    uint8_t fixed64[] = { 0x09 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_user_add_request(fixed64, 1, &ua),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_user_add_request(buf, n, NULL),
                        CETCD_ERR_INVAL);
}

CETCD_TEST_CASE(auto_compact_parse_txn_compare) {
    uint8_t buf[32];
    size_t n = 0;
    cetcd_txn_compare c;

    CETCD_ASSERT_EQ_INT(cetcd_encode_txn_compare(
                            NULL, 0, 0, 0, 0, buf, sizeof(buf), &n),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_encode_txn_compare(
                            (const uint8_t *)"k", 1, 3, 0, 0, buf, sizeof(buf),
                            &n),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_parse_txn_compare(buf, n, &c), CETCD_OK);
    CETCD_ASSERT_TRUE(c.key_len == 1 && c.key && c.key[0] == 'k');
    CETCD_ASSERT_EQ_INT(c.result, 3);
    cetcd_txn_compare_clear(&c);

    CETCD_ASSERT_EQ_INT(cetcd_parse_txn_compare(NULL, 0, &c), CETCD_OK);
    CETCD_ASSERT_EQ_INT(c.result, 0);
    uint8_t dummy[] = { 0x00 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_txn_compare(dummy, 1, &c), CETCD_OK);
    CETCD_ASSERT_EQ_INT(c.result, 0);

    /* leftover truncated result cannot look like EQUAL */
    uint8_t trunc[] = { 0x1a, 0x01, 'k', 0x08 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_txn_compare(trunc, 4, &c), CETCD_ERR_INVAL);
    CETCD_ASSERT_TRUE(c.key == NULL);

    /* leftover length-delimited payload cannot steal result */
    uint8_t steal[] = { 0x1a, 0x01, 'k', 0x52, 0x02, 0x08, 0x03 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_txn_compare(steal, sizeof(steal), &c),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(c.result, 0);
    CETCD_ASSERT_TRUE(c.key_len == 1 && c.key && c.key[0] == 'k');
    cetcd_txn_compare_clear(&c);

    uint8_t fixed64[] = { 0x09 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_txn_compare(fixed64, 1, &c),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_txn_compare(buf, n, NULL), CETCD_ERR_INVAL);
}

CETCD_TEST_CASE(auto_compact_parse_txn_request) {
    size_t nc = 9, ns = 9, nf = 9;

    CETCD_ASSERT_EQ_INT(cetcd_parse_txn_request(NULL, 0, NULL, &ns, &nf),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_txn_request(NULL, 0, &nc, &ns, &nf),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(nc == 0 && ns == 0 && nf == 0);
    uint8_t dummy[] = { 0x00 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_txn_request(dummy, 1, &nc, &ns, &nf),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(ns == 0);

    uint8_t one_ok[] = { 0x12, 0x08, 0x12, 0x06, 0x0a, 0x01, 'k',
                         0x12, 0x01, 'v' };
    CETCD_ASSERT_EQ_INT(cetcd_parse_txn_request(one_ok, sizeof(one_ok),
                                               &nc, &ns, &nf),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(ns == 1 && nc == 0 && nf == 0);

    /* leftover truncated success op cannot look like an empty txn */
    uint8_t trunc[] = { 0x12 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_txn_request(trunc, 1, &nc, &ns, &nf),
                        CETCD_ERR_INVAL);

    /* leftover length-delimited payload cannot inject a success op */
    uint8_t steal[] = { 0x22, 0x08, 0x12, 0x06, 0x0a, 0x01, 'x',
                        0x12, 0x01, '1' };
    CETCD_ASSERT_EQ_INT(cetcd_parse_txn_request(steal, sizeof(steal),
                                               &nc, &ns, &nf),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(ns == 0 && nc == 0 && nf == 0);

    uint8_t fixed64[] = { 0x09 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_txn_request(fixed64, 1, &nc, &ns, &nf),
                        CETCD_ERR_INVAL);
}

CETCD_TEST_CASE(auto_compact_parse_auth_name_request) {
    uint8_t buf[32];
    size_t n = 0;
    cetcd_auth_name_request an;

    CETCD_ASSERT_EQ_INT(cetcd_encode_auth_name_request(NULL, 0, buf, sizeof(buf),
                                                       &n),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_encode_auth_name_request(
                            (const uint8_t *)"alice", 5, buf, sizeof(buf), &n),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_parse_auth_name_request(buf, n, &an), CETCD_OK);
    CETCD_ASSERT_TRUE(an.name_len == 5 && an.name &&
                      memcmp(an.name, "alice", 5) == 0);
    cetcd_auth_name_request_clear(&an);

    CETCD_ASSERT_EQ_INT(cetcd_parse_auth_name_request(NULL, 0, &an), CETCD_OK);
    CETCD_ASSERT_TRUE(an.name == NULL);
    uint8_t dummy[] = { 0x00 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_auth_name_request(dummy, 1, &an), CETCD_OK);
    CETCD_ASSERT_TRUE(an.name == NULL);

    /* leftover truncated name cannot look like a successful delete */
    uint8_t trunc[] = { 0x0a };
    CETCD_ASSERT_EQ_INT(cetcd_parse_auth_name_request(trunc, 1, &an),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_TRUE(an.name == NULL);

    /* leftover length-delimited payload cannot steal the name */
    uint8_t steal[] = { 0x1a, 0x07, 0x0a, 0x05, 'a', 'l', 'i', 'c', 'e' };
    CETCD_ASSERT_EQ_INT(cetcd_parse_auth_name_request(steal, sizeof(steal), &an),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(an.name == NULL);

    uint8_t badskip[] = { 0x1a, 0x05 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_auth_name_request(badskip, 2, &an),
                        CETCD_ERR_INVAL);
    uint8_t fixed64[] = { 0x09 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_auth_name_request(fixed64, 1, &an),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_auth_name_request(buf, n, NULL),
                        CETCD_ERR_INVAL);
}

CETCD_TEST_CASE(auto_compact_parse_auth_role_grant_perm_request) {
    uint8_t buf[64];
    size_t n = 0;
    cetcd_auth_role_perm_request rp;

    CETCD_ASSERT_EQ_INT(cetcd_encode_auth_role_grant_perm_request(
                            NULL, 0, 2, NULL, 0, NULL, 0, buf, sizeof(buf), &n),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_encode_auth_role_grant_perm_request(
                            (const uint8_t *)"root", 4, 2,
                            (const uint8_t *)"/foo", 4, NULL, 0,
                            buf, sizeof(buf), &n),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_parse_auth_role_grant_perm_request(buf, n, &rp),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(rp.name_len == 4 && rp.name &&
                      memcmp(rp.name, "root", 4) == 0);
    CETCD_ASSERT_EQ_INT(rp.perm_type, 2);
    CETCD_ASSERT_TRUE(rp.key_len == 4 && rp.key &&
                      memcmp(rp.key, "/foo", 4) == 0);
    CETCD_ASSERT_TRUE(rp.range_end == NULL);
    cetcd_auth_role_perm_request_clear(&rp);

    CETCD_ASSERT_EQ_INT(cetcd_parse_auth_role_grant_perm_request(NULL, 0, &rp),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(rp.name == NULL && rp.perm_type == 0);
    uint8_t dummy[] = { 0x00 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_auth_role_grant_perm_request(dummy, 1, &rp),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(rp.name == NULL);

    /* leftover truncated name cannot look like a successful grant */
    uint8_t trunc[] = { 0x0a };
    CETCD_ASSERT_EQ_INT(cetcd_parse_auth_role_grant_perm_request(trunc, 1, &rp),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_TRUE(rp.name == NULL);
    uint8_t trunc_perm[] = { 0x0a, 0x04, 'r', 'o', 'o', 't', 0x12 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_auth_role_grant_perm_request(
                            trunc_perm, sizeof(trunc_perm), &rp),
                        CETCD_ERR_INVAL);

    /* leftover cannot steal the role name */
    uint8_t steal_name[] = { 0x1a, 0x06, 0x0a, 0x04, 'r', 'o', 'o', 't' };
    CETCD_ASSERT_EQ_INT(cetcd_parse_auth_role_grant_perm_request(
                            steal_name, sizeof(steal_name), &rp),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(rp.name == NULL);

    /* leftover cannot steal Permission.key */
    uint8_t steal_key[] = { 0x0a, 0x04, 'r', 'o', 'o', 't',
                            0x12, 0x08, 0x22, 0x06, 0x0a, 0x04, '/', 'f', 'o', 'o' };
    CETCD_ASSERT_EQ_INT(cetcd_parse_auth_role_grant_perm_request(
                            steal_key, sizeof(steal_key), &rp),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(rp.name_len == 4);
    CETCD_ASSERT_TRUE(rp.key == NULL);
    CETCD_ASSERT_EQ_INT(rp.perm_type, 0);
    cetcd_auth_role_perm_request_clear(&rp);

    /* leftover cannot steal Permission.permType */
    uint8_t steal_pt[] = { 0x0a, 0x04, 'r', 'o', 'o', 't',
                           0x12, 0x04, 0x22, 0x02, 0x08, 0x02 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_auth_role_grant_perm_request(
                            steal_pt, sizeof(steal_pt), &rp),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(rp.perm_type, 0);
    cetcd_auth_role_perm_request_clear(&rp);

    uint8_t fixed64[] = { 0x09 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_auth_role_grant_perm_request(fixed64, 1, &rp),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_auth_role_grant_perm_request(buf, n, NULL),
                        CETCD_ERR_INVAL);
}

CETCD_TEST_CASE(auto_compact_parse_auth_role_revoke_perm_request) {
    uint8_t buf[64];
    size_t n = 0;
    cetcd_auth_role_perm_request rp;

    CETCD_ASSERT_EQ_INT(cetcd_encode_auth_role_revoke_perm_request(
                            NULL, 0, NULL, 0, NULL, 0, buf, sizeof(buf), &n),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_encode_auth_role_revoke_perm_request(
                            (const uint8_t *)"root", 4,
                            (const uint8_t *)"/foo", 4,
                            (const uint8_t *)"/bar", 4,
                            buf, sizeof(buf), &n),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_parse_auth_role_revoke_perm_request(buf, n, &rp),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(rp.name_len == 4 && rp.name &&
                      memcmp(rp.name, "root", 4) == 0);
    CETCD_ASSERT_TRUE(rp.key_len == 4 && rp.key &&
                      memcmp(rp.key, "/foo", 4) == 0);
    CETCD_ASSERT_TRUE(rp.range_end_len == 4 && rp.range_end &&
                      memcmp(rp.range_end, "/bar", 4) == 0);
    cetcd_auth_role_perm_request_clear(&rp);

    CETCD_ASSERT_EQ_INT(cetcd_parse_auth_role_revoke_perm_request(NULL, 0, &rp),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(rp.name == NULL);
    uint8_t dummy[] = { 0x00 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_auth_role_revoke_perm_request(dummy, 1, &rp),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(rp.key == NULL);

    uint8_t trunc[] = { 0x0a };
    CETCD_ASSERT_EQ_INT(cetcd_parse_auth_role_revoke_perm_request(trunc, 1, &rp),
                        CETCD_ERR_INVAL);
    uint8_t trunc_key[] = { 0x0a, 0x04, 'r', 'o', 'o', 't', 0x12 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_auth_role_revoke_perm_request(
                            trunc_key, sizeof(trunc_key), &rp),
                        CETCD_ERR_INVAL);

    /* leftover cannot steal the role name */
    uint8_t steal_name[] = { 0x22, 0x06, 0x0a, 0x04, 'r', 'o', 'o', 't' };
    CETCD_ASSERT_EQ_INT(cetcd_parse_auth_role_revoke_perm_request(
                            steal_name, sizeof(steal_name), &rp),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(rp.name == NULL);

    /* leftover cannot steal key / range_end */
    uint8_t steal_key[] = { 0x0a, 0x04, 'r', 'o', 'o', 't',
                            0x22, 0x06, 0x12, 0x04, '/', 'f', 'o', 'o' };
    CETCD_ASSERT_EQ_INT(cetcd_parse_auth_role_revoke_perm_request(
                            steal_key, sizeof(steal_key), &rp),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(rp.name_len == 4);
    CETCD_ASSERT_TRUE(rp.key == NULL);
    cetcd_auth_role_perm_request_clear(&rp);
    uint8_t steal_re[] = { 0x0a, 0x04, 'r', 'o', 'o', 't',
                           0x22, 0x06, 0x1a, 0x04, '/', 'b', 'a', 'r' };
    CETCD_ASSERT_EQ_INT(cetcd_parse_auth_role_revoke_perm_request(
                            steal_re, sizeof(steal_re), &rp),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(rp.range_end == NULL);
    cetcd_auth_role_perm_request_clear(&rp);

    uint8_t fixed64[] = { 0x09 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_auth_role_revoke_perm_request(fixed64, 1,
                                                                 &rp),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_auth_role_revoke_perm_request(buf, n, NULL),
                        CETCD_ERR_INVAL);
}

CETCD_TEST_CASE(auto_compact_parse_watch_create_request) {
    uint8_t buf[32];
    size_t n = 0;
    cetcd_watch_create_request w;

    CETCD_ASSERT_EQ_INT(cetcd_encode_watch_create_request(
                            NULL, 0, 0, buf, sizeof(buf), &n),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_encode_watch_create_request(
                            (const uint8_t *)"k1", 2, 5, buf, sizeof(buf), &n),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_parse_watch_create_request(buf, n, &w), CETCD_OK);
    CETCD_ASSERT_TRUE(w.key_len == 2 && w.key && memcmp(w.key, "k1", 2) == 0);
    CETCD_ASSERT_TRUE(w.start_rev == 5);
    cetcd_watch_create_request_clear(&w);

    CETCD_ASSERT_EQ_INT(cetcd_parse_watch_create_request(NULL, 0, &w), CETCD_OK);
    CETCD_ASSERT_TRUE(w.start_rev == 0 && w.key == NULL);
    uint8_t dummy[] = { 0x00 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_watch_create_request(dummy, 1, &w),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(w.start_rev == 0);

    /* leftover truncated start_rev cannot look like from-now */
    uint8_t trunc[] = { 0x0a, 0x02, 'k', '1', 0x18 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_watch_create_request(trunc, sizeof(trunc),
                                                        &w),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_TRUE(w.key == NULL);

    /* leftover cannot steal start_rev */
    uint8_t steal[] = { 0x0a, 0x02, 'k', '1', 0x4a, 0x02, 0x18, 0x05 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_watch_create_request(steal, sizeof(steal),
                                                        &w),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(w.start_rev == 0);
    CETCD_ASSERT_TRUE(w.key_len == 2);
    cetcd_watch_create_request_clear(&w);

    /* leftover cannot steal range_end */
    uint8_t steal_re[] = { 0x0a, 0x02, 'k', '1',
                           0x4a, 0x04, 0x12, 0x02, 'k', '2' };
    CETCD_ASSERT_EQ_INT(cetcd_parse_watch_create_request(steal_re,
                                                        sizeof(steal_re), &w),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(w.range_end == NULL);
    cetcd_watch_create_request_clear(&w);

    uint8_t fixed64[] = { 0x09 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_watch_create_request(fixed64, 1, &w),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_watch_create_request(buf, n, NULL),
                        CETCD_ERR_INVAL);
}

CETCD_TEST_CASE(auto_compact_parse_compact_argv) {
    int physical = 0;
    int64_t rev = 0;
    char *ok[] = { "cetcdctl", "compact", "10" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_compact_argv(3, ok, 2, &physical, &rev),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(physical, 0);
    CETCD_ASSERT_TRUE(rev == 10);

    char *phys[] = { "cetcdctl", "compact", "--physical", "10" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_compact_argv(4, phys, 2, &physical, &rev),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(physical, 1);
    CETCD_ASSERT_TRUE(rev == 10);

    char *after[] = { "cetcdctl", "compact", "10", "--physical" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_compact_argv(4, after, 2, &physical, &rev),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(physical, 1);

    char *eqf[] = { "cetcdctl", "compact", "--physical=false", "10" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_compact_argv(4, eqf, 2, &physical, &rev),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(physical, 0);

    char *wo[] = { "cetcdctl", "compact", "-w", "json", "10" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_compact_argv(5, wo, 2, &physical, &rev),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(rev == 10);

    char *woeq[] = { "cetcdctl", "compact", "--write-out=fields", "10" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_compact_argv(4, woeq, 2, &physical, &rev),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(rev == 10);

    char *revflag[] = { "cetcdctl", "compact", "10", "--rev", "5" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_compact_argv(5, revflag, 2, &physical, &rev),
                        CETCD_ERR_INVAL);

    char *leftover[] = { "cetcdctl", "compact", "10foo" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_compact_argv(3, leftover, 2, &physical, &rev),
                        CETCD_ERR_INVAL);

    char *extra[] = { "cetcdctl", "compact", "10", "20" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_compact_argv(4, extra, 2, &physical, &rev),
                        CETCD_ERR_INVAL);

    char *norev[] = { "cetcdctl", "compact", "--physical" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_compact_argv(3, norev, 2, &physical, &rev),
                        CETCD_ERR_INVAL);

    char *zero[] = { "cetcdctl", "compact", "0" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_compact_argv(3, zero, 2, &physical, &rev),
                        CETCD_ERR_INVAL);

    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_compact_argv(3, ok, 2, NULL, &rev),
                        CETCD_ERR_INVAL);
}

CETCD_TEST_CASE(auto_compact_parse_maint_argv) {
    int cluster = 99;
    char *hash_ok[] = { "cetcdctl", "hash", "-w", "json" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_maint_argv(4, hash_ok, 2, 0, &cluster),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(cluster, 0);

    char *hash_rev[] = { "cetcdctl", "hash", "--rev", "10" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_maint_argv(4, hash_rev, 2, 0, &cluster),
                        CETCD_ERR_INVAL);

    char *st_cl[] = { "cetcdctl", "status", "--cluster" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_maint_argv(3, st_cl, 2, 0, &cluster),
                        CETCD_ERR_INVAL);

    char *df[] = { "cetcdctl", "defrag", "--cluster" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_maint_argv(3, df, 2, 1, &cluster),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(cluster, 1);

    char *df_eq[] = { "cetcdctl", "defrag", "--cluster=false" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_maint_argv(3, df_eq, 2, 1, &cluster),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(cluster, 0);

    char *df_rev[] = { "cetcdctl", "defrag", "--rev", "1" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_maint_argv(4, df_rev, 2, 1, &cluster),
                        CETCD_ERR_INVAL);

    char *bare[] = { "cetcdctl", "hash" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_maint_argv(2, bare, 2, 0, &cluster),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_maint_argv(2, bare, 2, 1, NULL),
                        CETCD_ERR_INVAL);

    char *ver_ok[] = { "cetcdctl", "version", "-w", "json" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_maint_argv(4, ver_ok, 2, 0, &cluster),
                        CETCD_OK);

    char *ver_foo[] = { "cetcdctl", "version", "--foo" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_maint_argv(3, ver_foo, 2, 0, &cluster),
                        CETCD_ERR_INVAL);

    char *ver_extra[] = { "cetcdctl", "version", "extra" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_maint_argv(3, ver_extra, 2, 0, &cluster),
                        CETCD_ERR_INVAL);
}

CETCD_TEST_CASE(auto_compact_parse_defrag_argv) {
    int cluster = 99;
    const char *dir = "SENTINEL";

    char *bare[] = { "cetcdctl", "defrag" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_defrag_argv(2, bare, 2, &cluster, &dir),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(cluster, 0);
    CETCD_ASSERT_TRUE(dir == NULL);

    char *cl[] = { "cetcdctl", "defrag", "--cluster" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_defrag_argv(3, cl, 2, &cluster, &dir),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(cluster, 1);
    CETCD_ASSERT_TRUE(dir == NULL);

    char *dd[] = { "cetcdctl", "defrag", "--data-dir", "./data" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_defrag_argv(4, dd, 2, &cluster, &dir),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(cluster, 0);
    CETCD_ASSERT_EQ_STR(dir, "./data");

    char *eq[] = { "cetcdctl", "defrag", "--data-dir=./data" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_defrag_argv(3, eq, 2, &cluster, &dir),
                        CETCD_OK);
    CETCD_ASSERT_EQ_STR(dir, "./data");

    char *wo[] = { "cetcdctl", "defrag", "-w", "json", "--data-dir", "./data" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_defrag_argv(6, wo, 2, &cluster, &dir),
                        CETCD_OK);
    CETCD_ASSERT_EQ_STR(dir, "./data");

    char *off[] = { "cetcdctl", "defrag", "--cluster=false", "--data-dir",
                    "./data" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_defrag_argv(5, off, 2, &cluster, &dir),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(cluster, 0);
    CETCD_ASSERT_EQ_STR(dir, "./data");

    char *eat[] = { "cetcdctl", "defrag", "--data-dir", "--cluster" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_defrag_argv(4, eat, 2, &cluster, &dir),
                        CETCD_ERR_INVAL);

    char *mix[] = { "cetcdctl", "defrag", "--cluster", "--data-dir", "./data" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_defrag_argv(5, mix, 2, &cluster, &dir),
                        CETCD_ERR_INVAL);

    char *empty[] = { "cetcdctl", "defrag", "--data-dir=" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_defrag_argv(3, empty, 2, &cluster, &dir),
                        CETCD_ERR_INVAL);

    char *foo[] = { "cetcdctl", "defrag", "--foo" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_defrag_argv(3, foo, 2, &cluster, &dir),
                        CETCD_ERR_INVAL);

    char *eqdash[] = { "cetcdctl", "defrag", "--data-dir=--foo" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_defrag_argv(3, eqdash, 2, &cluster, &dir),
                        CETCD_OK);
    CETCD_ASSERT_EQ_STR(dir, "--foo");

    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_defrag_argv(2, bare, 2, NULL, &dir),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_defrag_argv(2, bare, 2, &cluster, NULL),
                        CETCD_ERR_INVAL);
}

CETCD_TEST_CASE(auto_compact_parse_member_list_argv) {
    int lin = 99;

    char *bare[] = { "cetcdctl", "member", "list" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_member_list_argv(3, bare, 3, &lin),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(lin, 1);

    char *flag[] = { "cetcdctl", "member", "list", "--linearizable" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_member_list_argv(4, flag, 3, &lin),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(lin, 1);

    char *off[] = { "cetcdctl", "member", "list", "--linearizable=false" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_member_list_argv(4, off, 3, &lin),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(lin, 0);

    char *on[] = { "cetcdctl", "member", "list", "--linearizable=true" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_member_list_argv(4, on, 3, &lin),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(lin, 1);

    char *spc[] = { "cetcdctl", "member", "list", "--linearizable", "false" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_member_list_argv(5, spc, 3, &lin),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(lin, 0);

    char *wo[] = { "cetcdctl", "member", "list", "-w", "json" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_member_list_argv(5, wo, 3, &lin),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(lin, 1);

    char *woeq[] = { "cetcdctl", "member", "list", "--write-out=table",
                     "--linearizable=false" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_member_list_argv(5, woeq, 3, &lin),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(lin, 0);

    char *foo[] = { "cetcdctl", "member", "list", "--foo" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_member_list_argv(4, foo, 3, &lin),
                        CETCD_ERR_INVAL);

    char *eat[] = { "cetcdctl", "member", "list", "--linearizable", "--foo" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_member_list_argv(5, eat, 3, &lin),
                        CETCD_ERR_INVAL);

    char *extra[] = { "cetcdctl", "member", "list", "extra" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_member_list_argv(4, extra, 3, &lin),
                        CETCD_ERR_INVAL);

    char *bad[] = { "cetcdctl", "member", "list", "--linearizable=maybe" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_member_list_argv(4, bad, 3, &lin),
                        CETCD_ERR_INVAL);

    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_member_list_argv(3, bare, 3, NULL),
                        CETCD_ERR_INVAL);
}

CETCD_TEST_CASE(auto_compact_encode_member_list_request) {
    uint8_t buf[8];
    size_t n = 99;
    int lin = 99;

    /* --cluster MemberList (defrag/endpoint) leftover-safe-sends field 1 = true. */
    CETCD_ASSERT_EQ_INT(cetcd_encode_member_list_request(1, buf, sizeof(buf),
                                                         &n),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(n == 2);
    CETCD_ASSERT_EQ_INT((int)buf[0], 0x08);
    CETCD_ASSERT_EQ_INT((int)buf[1], 1);
    CETCD_ASSERT_EQ_INT(cetcd_parse_member_list_linearizable(buf, n, &lin),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(lin, 1);

    CETCD_ASSERT_EQ_INT(cetcd_encode_member_list_request(0, buf, sizeof(buf),
                                                         &n),
                        CETCD_OK);
    CETCD_ASSERT_TRUE(n == 2);
    CETCD_ASSERT_EQ_INT((int)buf[0], 0x08);
    CETCD_ASSERT_EQ_INT((int)buf[1], 0);
    CETCD_ASSERT_EQ_INT(cetcd_parse_member_list_linearizable(buf, n, &lin),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(lin, 0);

    lin = 99;
    CETCD_ASSERT_EQ_INT(cetcd_parse_member_list_linearizable(NULL, 0, &lin),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(lin, 0);

    uint8_t dummy[] = { 0x00 };
    lin = 99;
    CETCD_ASSERT_EQ_INT(cetcd_parse_member_list_linearizable(dummy, 1, &lin),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(lin, 0);

    uint8_t trunc[] = { 0x08 };
    CETCD_ASSERT_EQ_INT(cetcd_parse_member_list_linearizable(trunc, 1, &lin),
                        CETCD_ERR_INVAL);

    CETCD_ASSERT_EQ_INT(cetcd_encode_member_list_request(1, NULL, sizeof(buf),
                                                         &n),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_encode_member_list_request(1, buf, sizeof(buf),
                                                         NULL),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_encode_member_list_request(1, buf, 1, &n),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_member_list_linearizable(buf, n, NULL),
                        CETCD_ERR_INVAL);
}

CETCD_TEST_CASE(auto_compact_parse_role_perm_argv) {
    const char *role = NULL, *type = NULL, *key = NULL, *rend = NULL;
    int prefix = 99, from_key = 99;

    char *grant[] = { "cetcdctl", "role", "grant-permission", "r", "read",
                      "/foo" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_role_perm_argv(6, grant, 3, 1, &role,
                                                       &type, &key, &rend,
                                                       &prefix, &from_key),
                        CETCD_OK);
    CETCD_ASSERT_EQ_STR(role, "r");
    CETCD_ASSERT_EQ_STR(type, "read");
    CETCD_ASSERT_EQ_STR(key, "/foo");
    CETCD_ASSERT_TRUE(rend == NULL);
    CETCD_ASSERT_EQ_INT(prefix, 0);
    CETCD_ASSERT_EQ_INT(from_key, 0);

    char *end[] = { "cetcdctl", "role", "grant-permission", "r", "write", "a",
                    "b" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_role_perm_argv(7, end, 3, 1, &role,
                                                       &type, &key, &rend,
                                                       &prefix, &from_key),
                        CETCD_OK);
    CETCD_ASSERT_EQ_STR(rend, "b");

    char *fk[] = { "cetcdctl", "role", "grant-permission", "r", "read", "/foo",
                   "--from-key" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_role_perm_argv(7, fk, 3, 1, &role,
                                                       &type, &key, &rend,
                                                       &prefix, &from_key),
                        CETCD_OK);
    CETCD_ASSERT_EQ_INT(from_key, 1);
    CETCD_ASSERT_TRUE(rend == NULL);

    char *eat[] = { "cetcdctl", "role", "grant-permission", "r", "read", "/foo",
                    "--range-end", "--from-key" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_role_perm_argv(8, eat, 3, 1, &role,
                                                       &type, &key, &rend,
                                                       &prefix, &from_key),
                        CETCD_ERR_INVAL);

    char *mix[] = { "cetcdctl", "role", "grant-permission", "r", "read", "/foo",
                    "--prefix", "--from-key" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_role_perm_argv(8, mix, 3, 1, &role,
                                                       &type, &key, &rend,
                                                       &prefix, &from_key),
                        CETCD_ERR_INVAL);

    char *foo[] = { "cetcdctl", "role", "grant-permission", "r", "read", "/foo",
                    "--foo" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_role_perm_argv(7, foo, 3, 1, &role,
                                                       &type, &key, &rend,
                                                       &prefix, &from_key),
                        CETCD_ERR_INVAL);

    char *rev[] = { "cetcdctl", "role", "revoke-permission", "r", "read",
                    "/foo" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_role_perm_argv(6, rev, 3, 0, &role,
                                                       &type, &key, &rend,
                                                       &prefix, &from_key),
                        CETCD_OK);
    CETCD_ASSERT_EQ_STR(role, "r");
    CETCD_ASSERT_EQ_STR(key, "/foo");
    CETCD_ASSERT_TRUE(rend == NULL);

    char *revk[] = { "cetcdctl", "role", "revoke-permission", "r", "/foo",
                     "/bar" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_role_perm_argv(6, revk, 3, 0, &role,
                                                       &type, &key, &rend,
                                                       &prefix, &from_key),
                        CETCD_OK);
    CETCD_ASSERT_EQ_STR(key, "/foo");
    CETCD_ASSERT_EQ_STR(rend, "/bar");
}

CETCD_TEST_CASE(auto_compact_parse_one_name_argv) {
    const char *name = NULL;
    const char *a = NULL, *b = NULL;

    char *ok[] = { "cetcdctl", "member", "remove", "abc" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_one_name_argv(4, ok, 3, &name),
                        CETCD_OK);
    CETCD_ASSERT_EQ_STR(name, "abc");

    char *wo[] = { "cetcdctl", "member", "remove", "-w", "json", "abc" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_one_name_argv(6, wo, 3, &name),
                        CETCD_OK);
    CETCD_ASSERT_EQ_STR(name, "abc");

    char *ddash[] = { "cetcdctl", "watch", "--", "--foo" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_one_name_argv(4, ddash, 2, &name),
                        CETCD_OK);
    CETCD_ASSERT_EQ_STR(name, "--foo");

    char *force[] = { "cetcdctl", "member", "remove", "--force", "abc" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_one_name_argv(5, force, 3, &name),
                        CETCD_ERR_INVAL);

    char *asname[] = { "cetcdctl", "user", "add", "--foo" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_one_name_argv(4, asname, 3, &name),
                        CETCD_ERR_INVAL);

    char *extra[] = { "cetcdctl", "member", "remove", "abc", "extra" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_one_name_argv(5, extra, 3, &name),
                        CETCD_ERR_INVAL);

    char *missing[] = { "cetcdctl", "member", "remove" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_one_name_argv(3, missing, 3, &name),
                        CETCD_ERR_INVAL);

    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_one_name_argv(4, ok, 3, NULL),
                        CETCD_ERR_INVAL);

    char *upd[] = { "cetcdctl", "member", "update", "abc",
                    "http://127.0.0.1:2380" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_two_name_argv(5, upd, 3, &a, &b),
                        CETCD_OK);
    CETCD_ASSERT_EQ_STR(a, "abc");
    CETCD_ASSERT_EQ_STR(b, "http://127.0.0.1:2380");

    char *upd_flag[] = { "cetcdctl", "member", "update", "--force", "abc",
                         "http://127.0.0.1:2380" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_two_name_argv(6, upd_flag, 3, &a, &b),
                        CETCD_ERR_INVAL);

    char *txn_ok[] = { "cetcdctl", "txn", "put", "k", "v" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_two_name_argv(5, txn_ok, 3, &a, &b),
                        CETCD_OK);
    CETCD_ASSERT_EQ_STR(a, "k");
    CETCD_ASSERT_EQ_STR(b, "v");

    char *txn_wo[] = { "cetcdctl", "txn", "put", "-w", "json", "k", "v" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_two_name_argv(7, txn_wo, 3, &a, &b),
                        CETCD_OK);
    CETCD_ASSERT_EQ_STR(a, "k");
    CETCD_ASSERT_EQ_STR(b, "v");

    char *txn_foo[] = { "cetcdctl", "txn", "put", "--foo", "k", "v" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_two_name_argv(6, txn_foo, 3, &a, &b),
                        CETCD_ERR_INVAL);

    const char *c = NULL;
    char *cas_ok[] = { "cetcdctl", "txn", "cas", "k", "old", "new" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_three_name_argv(6, cas_ok, 3, &a, &b, &c),
                        CETCD_OK);
    CETCD_ASSERT_EQ_STR(a, "k");
    CETCD_ASSERT_EQ_STR(b, "old");
    CETCD_ASSERT_EQ_STR(c, "new");

    char *cas_foo[] = { "cetcdctl", "txn", "cas", "--foo", "k", "old", "new" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_three_name_argv(7, cas_foo, 3, &a, &b, &c),
                        CETCD_ERR_INVAL);

    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_three_name_argv(6, cas_ok, 3, &a, &b,
                                                       NULL),
                        CETCD_ERR_INVAL);
}

CETCD_TEST_CASE(auto_compact_parse_check_argv) {
    char *ok[] = { "cetcdctl", "check", "perf", "--load", "s" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_check_argv(5, ok, 3), CETCD_OK);

    char *wo[] = { "cetcdctl", "check", "perf", "-w", "json" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_check_argv(5, wo, 3), CETCD_OK);

    char *pref[] = { "cetcdctl", "check", "datascale", "--prefix", "p",
                     "--load", "10" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_check_argv(7, pref, 3), CETCD_OK);

    char *eq[] = { "cetcdctl", "check", "datascale", "--load=10" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_check_argv(4, eq, 3), CETCD_OK);

    char *bare[] = { "cetcdctl", "check", "perf" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_check_argv(3, bare, 3), CETCD_OK);

    char *foo[] = { "cetcdctl", "check", "perf", "--foo" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_check_argv(4, foo, 3), CETCD_ERR_INVAL);

    char *eat[] = { "cetcdctl", "check", "perf", "--load", "--prefix" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_check_argv(5, eat, 3), CETCD_ERR_INVAL);

    char *ds[] = { "cetcdctl", "check", "datascale", "--foo", "--load", "10" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_check_argv(6, ds, 3), CETCD_ERR_INVAL);

    char *empty[] = { "cetcdctl", "check", "perf", "--load=" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_check_argv(4, empty, 3),
                        CETCD_ERR_INVAL);

    char *extra[] = { "cetcdctl", "check", "perf", "extra" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_check_argv(4, extra, 3),
                        CETCD_ERR_INVAL);

    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_check_argv(3, bare, 3), CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_check_argv(3, NULL, 3), CETCD_ERR_INVAL);
}

CETCD_TEST_CASE(auto_compact_parse_completion_argv) {
    const char *shell = NULL;

    char *ok[] = { "cetcdctl", "completion", "bash" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_completion_argv(3, ok, 2, &shell),
                        CETCD_OK);
    CETCD_ASSERT_EQ_STR(shell, "bash");

    char *zsh[] = { "cetcdctl", "completion", "zsh" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_completion_argv(3, zsh, 2, &shell),
                        CETCD_OK);
    CETCD_ASSERT_EQ_STR(shell, "zsh");

    char *dd[] = { "cetcdctl", "completion", "--", "fish" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_completion_argv(4, dd, 2, &shell),
                        CETCD_OK);
    CETCD_ASSERT_EQ_STR(shell, "fish");

    char *foo[] = { "cetcdctl", "completion", "bash", "--foo" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_completion_argv(4, foo, 2, &shell),
                        CETCD_ERR_INVAL);

    char *flag[] = { "cetcdctl", "completion", "--foo" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_completion_argv(3, flag, 2, &shell),
                        CETCD_ERR_INVAL);

    char *wo[] = { "cetcdctl", "completion", "-w", "json", "bash" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_completion_argv(5, wo, 2, &shell),
                        CETCD_ERR_INVAL);

    char *ksh[] = { "cetcdctl", "completion", "ksh" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_completion_argv(3, ksh, 2, &shell),
                        CETCD_ERR_INVAL);

    char *miss[] = { "cetcdctl", "completion" };
    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_completion_argv(2, miss, 2, &shell),
                        CETCD_ERR_INVAL);

    CETCD_ASSERT_EQ_INT(cetcd_ctl_parse_completion_argv(3, ok, 2, NULL),
                        CETCD_ERR_INVAL);
}

CETCD_TEST_CASE(auto_compact_parse_pprof_seconds) {
    int secs = 0;
    CETCD_ASSERT_EQ_INT(cetcd_parse_pprof_seconds(NULL, 0, &secs), CETCD_OK);
    CETCD_ASSERT_EQ_INT(secs, 30);
    CETCD_ASSERT_EQ_INT(cetcd_parse_pprof_seconds("", 0, &secs), CETCD_OK);
    CETCD_ASSERT_EQ_INT(secs, 30);
    const char *ok = "seconds=10";
    CETCD_ASSERT_EQ_INT(cetcd_parse_pprof_seconds(ok, strlen(ok), &secs), CETCD_OK);
    CETCD_ASSERT_EQ_INT(secs, 10);
    const char *leftover = "seconds=30foo";
    CETCD_ASSERT_EQ_INT(cetcd_parse_pprof_seconds(leftover, strlen(leftover), &secs),
                        CETCD_ERR_INVAL);
    const char *zero = "seconds=0";
    CETCD_ASSERT_EQ_INT(cetcd_parse_pprof_seconds(zero, strlen(zero), &secs),
                        CETCD_ERR_RANGE);
    const char *big = "seconds=301";
    CETCD_ASSERT_EQ_INT(cetcd_parse_pprof_seconds(big, strlen(big), &secs),
                        CETCD_ERR_RANGE);
    const char *empty = "seconds=";
    CETCD_ASSERT_EQ_INT(cetcd_parse_pprof_seconds(empty, strlen(empty), &secs),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_pprof_seconds("seconds=10", 10, NULL),
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
    CETCD_TEST_ENTRY(auto_compact_etcd_compat),
    CETCD_TEST_ENTRY(auto_compact_v2_era_flags),
    CETCD_TEST_ENTRY(auto_compact_grpc_keepalive),
    CETCD_TEST_ENTRY(auto_compact_quota_backend_bytes),
    CETCD_TEST_ENTRY(auto_compact_snapshot_count),
    CETCD_TEST_ENTRY(auto_compact_cli_equals_form),
    CETCD_TEST_ENTRY(auto_compact_cli_bool_form),
    CETCD_TEST_ENTRY(auto_compact_cli_bool_eq),
    CETCD_TEST_ENTRY(auto_compact_parse_command_timeout),
    CETCD_TEST_ENTRY(auto_compact_parse_i64),
    CETCD_TEST_ENTRY(auto_compact_encode_hashkv_request),
    CETCD_TEST_ENTRY(auto_compact_parse_hashkv_request),
    CETCD_TEST_ENTRY(auto_compact_parse_compact_request),
    CETCD_TEST_ENTRY(auto_compact_parse_move_leader_request),
    CETCD_TEST_ENTRY(auto_compact_parse_member_id_request),
    CETCD_TEST_ENTRY(auto_compact_parse_member_update_request),
    CETCD_TEST_ENTRY(auto_compact_parse_member_add_request),
    CETCD_TEST_ENTRY(auto_compact_parse_downgrade_request),
    CETCD_TEST_ENTRY(auto_compact_parse_txn_op_key),
    CETCD_TEST_ENTRY(auto_compact_parse_lease_grant_request),
    CETCD_TEST_ENTRY(auto_compact_parse_lease_id_request),
    CETCD_TEST_ENTRY(auto_compact_parse_alarm_request),
    CETCD_TEST_ENTRY(auto_compact_parse_range_request),
    CETCD_TEST_ENTRY(auto_compact_parse_put_request),
    CETCD_TEST_ENTRY(auto_compact_parse_delete_range_request),
    CETCD_TEST_ENTRY(auto_compact_parse_auth_name_pass_request),
    CETCD_TEST_ENTRY(auto_compact_parse_user_add_request),
    CETCD_TEST_ENTRY(auto_compact_parse_txn_compare),
    CETCD_TEST_ENTRY(auto_compact_parse_txn_request),
    CETCD_TEST_ENTRY(auto_compact_parse_auth_name_request),
    CETCD_TEST_ENTRY(auto_compact_parse_auth_role_grant_perm_request),
    CETCD_TEST_ENTRY(auto_compact_parse_auth_role_revoke_perm_request),
    CETCD_TEST_ENTRY(auto_compact_parse_watch_create_request),
    CETCD_TEST_ENTRY(auto_compact_parse_compact_argv),
    CETCD_TEST_ENTRY(auto_compact_parse_maint_argv),
    CETCD_TEST_ENTRY(auto_compact_parse_defrag_argv),
    CETCD_TEST_ENTRY(auto_compact_parse_member_list_argv),
    CETCD_TEST_ENTRY(auto_compact_encode_member_list_request),
    CETCD_TEST_ENTRY(auto_compact_parse_role_perm_argv),
    CETCD_TEST_ENTRY(auto_compact_parse_one_name_argv),
    CETCD_TEST_ENTRY(auto_compact_parse_check_argv),
    CETCD_TEST_ENTRY(auto_compact_parse_completion_argv),
    CETCD_TEST_ENTRY(auto_compact_parse_pprof_seconds),
CETCD_TEST_LIST_END

CETCD_TEST_MAIN()
