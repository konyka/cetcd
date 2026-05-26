#include "cetcd/metrics.h"
#include "cetcd_test.h"

CETCD_TEST_CASE(metrics_counter_basic) {
    cetcd_metrics *m = cetcd_metrics_new();
    CETCD_ASSERT_NOT_NULL(m);

    cetcd_metrics_counter(m, "raft_proposals_total", 1);
    cetcd_metrics_counter(m, "raft_proposals_total", 3);

    char buf[4096];
    size_t len = cetcd_metrics_render(m, buf, sizeof(buf));
    CETCD_ASSERT_TRUE(len > 0);
    CETCD_ASSERT_TRUE(strstr(buf, "raft_proposals_total 4") != NULL);

    cetcd_metrics_free(m);
}

CETCD_TEST_CASE(metrics_gauge_basic) {
    cetcd_metrics *m = cetcd_metrics_new();
    CETCD_ASSERT_NOT_NULL(m);

    cetcd_metrics_gauge_set(m, "mvcc_revision", 42);
    cetcd_metrics_gauge_set(m, "mvcc_revision", 100);
    cetcd_metrics_gauge_inc(m, "mvcc_revision");
    cetcd_metrics_gauge_dec(m, "mvcc_revision");

    char buf[4096];
    size_t len = cetcd_metrics_render(m, buf, sizeof(buf));
    CETCD_ASSERT_TRUE(len > 0);
    CETCD_ASSERT_TRUE(strstr(buf, "mvcc_revision 100") != NULL);

    cetcd_metrics_free(m);
}

CETCD_TEST_CASE(metrics_histogram_basic) {
    cetcd_metrics *m = cetcd_metrics_new();
    CETCD_ASSERT_NOT_NULL(m);

    cetcd_metrics_observe(m, "request_duration_ns", 100);
    cetcd_metrics_observe(m, "request_duration_ns", 200);
    cetcd_metrics_observe(m, "request_duration_ns", 300);

    char buf[8192];
    size_t len = cetcd_metrics_render(m, buf, sizeof(buf));
    CETCD_ASSERT_TRUE(len > 0);
    CETCD_ASSERT_TRUE(strstr(buf, "request_duration_ns_count 3") != NULL);
    CETCD_ASSERT_TRUE(strstr(buf, "request_duration_ns_sum 600") != NULL);

    cetcd_metrics_free(m);
}

CETCD_TEST_CASE(metrics_multiple_families) {
    cetcd_metrics *m = cetcd_metrics_new();
    CETCD_ASSERT_NOT_NULL(m);

    cetcd_metrics_counter(m, "http_requests_total", 5);
    cetcd_metrics_gauge_set(m, "http_connections", 10);
    cetcd_metrics_observe(m, "request_latency_ns", 50);

    char buf[8192];
    size_t len = cetcd_metrics_render(m, buf, sizeof(buf));
    CETCD_ASSERT_TRUE(len > 0);
    CETCD_ASSERT_TRUE(strstr(buf, "http_requests_total 5") != NULL);
    CETCD_ASSERT_TRUE(strstr(buf, "http_connections 10") != NULL);
    CETCD_ASSERT_TRUE(strstr(buf, "request_latency_ns_count 1") != NULL);

    cetcd_metrics_free(m);
}

CETCD_TEST_CASE(metrics_null_safety) {
    CETCD_ASSERT_EQ_INT(cetcd_metrics_render(NULL, NULL, 0), 0);
    cetcd_metrics_counter(NULL, "test", 1);
    cetcd_metrics_gauge_set(NULL, "test", 1);
    cetcd_metrics_observe(NULL, "test", 1);
    cetcd_metrics_free(NULL);
}

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(metrics_counter_basic),
    CETCD_TEST_ENTRY(metrics_gauge_basic),
    CETCD_TEST_ENTRY(metrics_histogram_basic),
    CETCD_TEST_ENTRY(metrics_multiple_families),
    CETCD_TEST_ENTRY(metrics_null_safety),
CETCD_TEST_LIST_END

CETCD_TEST_MAIN()
