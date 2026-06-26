#include "cetcd/metrics.h"
#include "cetcd/io.h"
#include "cetcd_test.h"

CETCD_TEST_CASE(metrics_counter_basic) {
    cetcd_metrics *m = cetcd_metrics_new();
    CETCD_ASSERT_NOT_NULL(m);

    cetcd_metrics_counter(m, "raft_proposals_total", 1);
    cetcd_metrics_counter(m, "raft_proposals_total", 3);

    cetcd_buf_t buf;
    cetcd_buf_init(&buf);
    CETCD_ASSERT_EQ_INT(cetcd_metrics_render(m, &buf), 0);
    CETCD_ASSERT_TRUE(buf.len > 0);
    CETCD_ASSERT_TRUE(strstr((char *)buf.data, "raft_proposals_total 4") != NULL);
    cetcd_buf_free(&buf);

    cetcd_metrics_free(m);
}

CETCD_TEST_CASE(metrics_gauge_basic) {
    cetcd_metrics *m = cetcd_metrics_new();
    CETCD_ASSERT_NOT_NULL(m);

    cetcd_metrics_gauge_set(m, "mvcc_revision", 42);
    cetcd_metrics_gauge_set(m, "mvcc_revision", 100);
    cetcd_metrics_gauge_inc(m, "mvcc_revision");
    cetcd_metrics_gauge_dec(m, "mvcc_revision");

    cetcd_buf_t buf;
    cetcd_buf_init(&buf);
    CETCD_ASSERT_EQ_INT(cetcd_metrics_render(m, &buf), 0);
    CETCD_ASSERT_TRUE(buf.len > 0);
    CETCD_ASSERT_TRUE(strstr((char *)buf.data, "mvcc_revision 100") != NULL);
    cetcd_buf_free(&buf);

    cetcd_metrics_free(m);
}

CETCD_TEST_CASE(metrics_histogram_basic) {
    cetcd_metrics *m = cetcd_metrics_new();
    CETCD_ASSERT_NOT_NULL(m);

    cetcd_metrics_observe(m, "request_duration_ns", 100);
    cetcd_metrics_observe(m, "request_duration_ns", 200);
    cetcd_metrics_observe(m, "request_duration_ns", 300);

    cetcd_buf_t buf;
    cetcd_buf_init(&buf);
    CETCD_ASSERT_EQ_INT(cetcd_metrics_render(m, &buf), 0);
    CETCD_ASSERT_TRUE(buf.len > 0);
    CETCD_ASSERT_TRUE(strstr((char *)buf.data, "request_duration_ns_count 3") != NULL);
    CETCD_ASSERT_TRUE(strstr((char *)buf.data, "request_duration_ns_sum 600") != NULL);
    cetcd_buf_free(&buf);

    cetcd_metrics_free(m);
}

CETCD_TEST_CASE(metrics_multiple_families) {
    cetcd_metrics *m = cetcd_metrics_new();
    CETCD_ASSERT_NOT_NULL(m);

    cetcd_metrics_counter(m, "http_requests_total", 5);
    cetcd_metrics_gauge_set(m, "http_connections", 10);
    cetcd_metrics_observe(m, "request_latency_ns", 50);

    cetcd_buf_t buf;
    cetcd_buf_init(&buf);
    CETCD_ASSERT_EQ_INT(cetcd_metrics_render(m, &buf), 0);
    CETCD_ASSERT_TRUE(buf.len > 0);
    CETCD_ASSERT_TRUE(strstr((char *)buf.data, "http_requests_total 5") != NULL);
    CETCD_ASSERT_TRUE(strstr((char *)buf.data, "http_connections 10") != NULL);
    CETCD_ASSERT_TRUE(strstr((char *)buf.data, "request_latency_ns_count 1") != NULL);
    cetcd_buf_free(&buf);

    cetcd_metrics_free(m);
}

CETCD_TEST_CASE(metrics_null_safety) {
    CETCD_ASSERT_EQ_INT(cetcd_metrics_render(NULL, NULL), CETCD_ERR_INVAL);
    cetcd_metrics_counter(NULL, "test", 1);
    cetcd_metrics_gauge_set(NULL, "test", 1);
    cetcd_metrics_observe(NULL, "test", 1);
    cetcd_metrics_free(NULL);
}

/* ── Prometheus text exposition format tests ───────────────────────────── */

CETCD_TEST_CASE(test_metrics_render_prometheus_format) {
    cetcd_metrics *m = cetcd_metrics_new();
    CETCD_ASSERT_NOT_NULL(m);

    /* Register a counter and a gauge */
    cetcd_metrics_counter(m, "http_requests_total", 7);
    cetcd_metrics_gauge_set(m, "db_connections", 3);

    cetcd_buf_t buf;
    cetcd_buf_init(&buf);
    CETCD_ASSERT_EQ_INT(cetcd_metrics_render(m, &buf), 0);
    CETCD_ASSERT_TRUE(buf.len > 0);

    char *out = (char *)buf.data;

    /* Verify # TYPE lines */
    CETCD_ASSERT_TRUE(strstr(out, "# TYPE http_requests_total counter") != NULL);
    CETCD_ASSERT_TRUE(strstr(out, "# TYPE db_connections gauge") != NULL);

    /* Verify # HELP lines */
    CETCD_ASSERT_TRUE(strstr(out, "# HELP http_requests_total") != NULL);
    CETCD_ASSERT_TRUE(strstr(out, "# HELP db_connections") != NULL);

    /* Verify metric values */
    CETCD_ASSERT_TRUE(strstr(out, "http_requests_total 7") != NULL);
    CETCD_ASSERT_TRUE(strstr(out, "db_connections 3") != NULL);

    cetcd_buf_free(&buf);
    cetcd_metrics_free(m);
}

CETCD_TEST_CASE(test_metrics_render_histogram) {
    cetcd_metrics *m = cetcd_metrics_new();
    CETCD_ASSERT_NOT_NULL(m);

    /* Observe some values */
    cetcd_metrics_observe(m, "request_duration_ns", 100);
    cetcd_metrics_observe(m, "request_duration_ns", 200);
    cetcd_metrics_observe(m, "request_duration_ns", 300);

    cetcd_buf_t buf;
    cetcd_buf_init(&buf);
    CETCD_ASSERT_EQ_INT(cetcd_metrics_render(m, &buf), 0);
    CETCD_ASSERT_TRUE(buf.len > 0);

    char *out = (char *)buf.data;

    /* Verify histogram TYPE line */
    CETCD_ASSERT_TRUE(strstr(out, "# TYPE request_duration_ns histogram") != NULL);

    /* Verify _bucket line */
    CETCD_ASSERT_TRUE(strstr(out, "request_duration_ns_bucket{le=\"+Inf\"} 3") != NULL);

    /* Verify _sum line */
    CETCD_ASSERT_TRUE(strstr(out, "request_duration_ns_sum 600") != NULL);

    /* Verify _count line */
    CETCD_ASSERT_TRUE(strstr(out, "request_duration_ns_count 3") != NULL);

    cetcd_buf_free(&buf);
    cetcd_metrics_free(m);
}

CETCD_TEST_CASE(test_metrics_render_empty) {
    cetcd_metrics *m = cetcd_metrics_new();
    CETCD_ASSERT_NOT_NULL(m);

    /* Render with no registered metrics */
    cetcd_buf_t buf;
    cetcd_buf_init(&buf);
    CETCD_ASSERT_EQ_INT(cetcd_metrics_render(m, &buf), 0);
    /* No crash, and output should be empty (zero length) */
    CETCD_ASSERT_TRUE(buf.len == 0);

    cetcd_buf_free(&buf);
    cetcd_metrics_free(m);
}

/* ── pprof rendering tests ─────────────────────────────────────────────── */

static int pprof_dummy_done_ = 0;
static void fn_pprof_dummy_co(void *arg) {
    (void)arg;
    pprof_dummy_done_ = 1;
}

CETCD_TEST_CASE(test_pprof_heap_render) {
    cetcd_buf_t buf;
    cetcd_buf_init(&buf);

    int rc = cetcd_pprof_heap_render(&buf);
    CETCD_ASSERT_EQ_INT(rc, 0);
    CETCD_ASSERT_TRUE(buf.len > 0);

    char *out = (char *)buf.data;

    /* Verify the output starts with the heap section marker */
    CETCD_ASSERT_TRUE(strstr(out, "--- heap") != NULL);

    /* Verify header fields are present */
    CETCD_ASSERT_TRUE(strstr(out, "Total allocated:") != NULL);
    CETCD_ASSERT_TRUE(strstr(out, "Peak allocated:") != NULL);

    /* Verify table headers */
    CETCD_ASSERT_TRUE(strstr(out, "Size class") != NULL);
    CETCD_ASSERT_TRUE(strstr(out, "Allocated") != NULL);
    CETCD_ASSERT_TRUE(strstr(out, "Free") != NULL);
    CETCD_ASSERT_TRUE(strstr(out, "Total") != NULL);

    cetcd_buf_free(&buf);
}

CETCD_TEST_CASE(test_pprof_coroutines_render) {
    /* Create a loop and a named coroutine so the registry is non-empty */
    cetcd_loop *loop = cetcd_loop_new();
    pprof_dummy_done_ = 0;
    /* Use cetcd_co_create (not started) so the coroutine stays in the
     * registry and is visible to cetcd_co_walk. */
    cetcd_co *co = cetcd_co_create(loop, fn_pprof_dummy_co, NULL,
                                    CETCD_CO_DEFAULT_STACK_SIZE);
    CETCD_ASSERT_NOT_NULL(co);
    cetcd_co_set_name(co, "pprof_test_co");

    cetcd_buf_t buf;
    cetcd_buf_init(&buf);

    int rc = cetcd_pprof_coroutines_render(&buf);
    CETCD_ASSERT_EQ_INT(rc, 0);
    CETCD_ASSERT_TRUE(buf.len > 0);

    char *out = (char *)buf.data;

    /* Verify the output starts with the coroutines section marker */
    CETCD_ASSERT_TRUE(strstr(out, "--- coroutines") != NULL);

    /* Verify header fields */
    CETCD_ASSERT_TRUE(strstr(out, "Total:") != NULL);
    CETCD_ASSERT_TRUE(strstr(out, "ID") != NULL);
    CETCD_ASSERT_TRUE(strstr(out, "State") != NULL);
    CETCD_ASSERT_TRUE(strstr(out, "Function") != NULL);

    /* Verify our named coroutine appears in the output */
    CETCD_ASSERT_TRUE(strstr(out, "pprof_test_co") != NULL);

    cetcd_buf_free(&buf);
    cetcd_co_free(co);
    cetcd_loop_free(loop);
}

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(metrics_counter_basic),
    CETCD_TEST_ENTRY(metrics_gauge_basic),
    CETCD_TEST_ENTRY(metrics_histogram_basic),
    CETCD_TEST_ENTRY(metrics_multiple_families),
    CETCD_TEST_ENTRY(metrics_null_safety),
    CETCD_TEST_ENTRY(test_metrics_render_prometheus_format),
    CETCD_TEST_ENTRY(test_metrics_render_histogram),
    CETCD_TEST_ENTRY(test_metrics_render_empty),
    CETCD_TEST_ENTRY(test_pprof_heap_render),
    CETCD_TEST_ENTRY(test_pprof_coroutines_render),
CETCD_TEST_LIST_END

CETCD_TEST_MAIN()
