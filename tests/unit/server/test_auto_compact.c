#include "cetcd/server.h"
#include "cetcd_test.h"

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
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_metrics_listen_url(
                            "http://127.0.0.1:2381,http://127.0.0.1:2382",
                            host, sizeof(host), &port),
                        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_metrics_listen_url("http://127.0.0.1:abc",
                                                       host, sizeof(host),
                                                       &port),
                        CETCD_ERR_INVAL);
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
    CETCD_ASSERT_EQ_INT(cetcd_server_metrics_route("/health", 7, 1), 2);
    CETCD_ASSERT_EQ_INT(cetcd_server_metrics_route(NULL, 0, 1), 2);
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
    CETCD_TEST_ENTRY(auto_compact_parse_metrics_listen_url),
    CETCD_TEST_ENTRY(auto_compact_metrics_addr),
    CETCD_TEST_ENTRY(auto_compact_enable_pprof),
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
CETCD_TEST_LIST_END

CETCD_TEST_MAIN()
