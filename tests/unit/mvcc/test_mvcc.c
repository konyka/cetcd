#define _POSIX_C_SOURCE 200809L
#include "cetcd/base.h"
#include "cetcd/mvcc.h"
#include "cetcd/backend.h"
#include "cetcd_test.h"

#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static void free_kv_fields(cetcd_kv *kv) {
    free((void*)kv->key.data);
    free((void*)kv->value.data);
}

CETCD_TEST_CASE(mvcc_put_get_basic) {
    cetcd_mvcc_store *s = cetcd_mvcc_store_new();
    CETCD_ASSERT_NOT_NULL(s);
    CETCD_ASSERT_TRUE(cetcd_mvcc_revision(s) == 0);

    const uint8_t k[] = "hello";
    const uint8_t v[] = "world";
    cetcd_revision r = cetcd_mvcc_put(s, k, 5, v, 5, 0);
    CETCD_ASSERT_TRUE(r.main == 1);
    CETCD_ASSERT_TRUE(r.sub == 0);

    cetcd_kv out = {0};
    int rc = cetcd_mvcc_get(s, 0, k, 5, &out);
    CETCD_ASSERT_EQ_INT(rc, CETCD_OK);
    CETCD_ASSERT_EQ_INT((int)out.value.len, 5);
    CETCD_ASSERT_TRUE(memcmp(out.value.data, v, 5) == 0);
    CETCD_ASSERT_TRUE(out.mod_rev.main == 1);
    CETCD_ASSERT_TRUE(out.version == 1);
    free_kv_fields(&out);

    cetcd_mvcc_store_free(s);
}

CETCD_TEST_CASE(mvcc_multiple_puts_same_key) {
    cetcd_mvcc_store *s = cetcd_mvcc_store_new();
    const uint8_t k[] = "key";
    const uint8_t v1[] = "val1";
    const uint8_t v2[] = "val2";

    cetcd_mvcc_put(s, k, 3, v1, 4, 0);
    cetcd_revision r2 = cetcd_mvcc_put(s, k, 3, v2, 4, 0);
    CETCD_ASSERT_TRUE(r2.main == 2);

    cetcd_kv out = {0};
    cetcd_mvcc_get(s, 0, k, 3, &out);
    CETCD_ASSERT_EQ_INT((int)out.value.len, 4);
    CETCD_ASSERT_TRUE(memcmp(out.value.data, v2, 4) == 0);
    CETCD_ASSERT_TRUE(out.version == 2);
    CETCD_ASSERT_TRUE(out.mod_rev.main == 2);
    CETCD_ASSERT_TRUE(out.create_rev.main == 1);
    free_kv_fields(&out);

    cetcd_kv out1 = {0};
    CETCD_ASSERT_EQ_INT(cetcd_mvcc_get(s, 1, k, 3, &out1), CETCD_OK);
    CETCD_ASSERT_TRUE(memcmp(out1.value.data, v1, 4) == 0);
    free_kv_fields(&out1);

    cetcd_mvcc_store_free(s);
}

CETCD_TEST_CASE(mvcc_delete) {
    cetcd_mvcc_store *s = cetcd_mvcc_store_new();
    const uint8_t k[] = "doom";
    const uint8_t v[] = "me";

    cetcd_mvcc_put(s, k, 4, v, 2, 0);
    cetcd_revision rd = cetcd_mvcc_delete(s, k, 4);
    CETCD_ASSERT_TRUE(rd.main == 2);

    cetcd_kv out = {0};
    CETCD_ASSERT_EQ_INT(cetcd_mvcc_get(s, 0, k, 4, &out), CETCD_ERR_NOTFOUND);

    cetcd_kv out_old = {0};
    CETCD_ASSERT_EQ_INT(cetcd_mvcc_get(s, 1, k, 4, &out_old), CETCD_OK);
    CETCD_ASSERT_TRUE(memcmp(out_old.value.data, v, 2) == 0);
    free_kv_fields(&out_old);

    cetcd_mvcc_store_free(s);
}

CETCD_TEST_CASE(mvcc_range) {
    cetcd_mvcc_store *s = cetcd_mvcc_store_new();
    const uint8_t ka[] = "a";
    const uint8_t kb[] = "b";
    const uint8_t kc[] = "c";
    const uint8_t kd[] = "d";
    const uint8_t v[] = "x";

    cetcd_mvcc_put(s, ka, 1, v, 1, 0);
    cetcd_mvcc_put(s, kb, 1, v, 1, 0);
    cetcd_mvcc_put(s, kc, 1, v, 1, 0);
    cetcd_mvcc_put(s, kd, 1, v, 1, 0);

    cetcd_kv *results = NULL;
    size_t count = 0;
    int rc = cetcd_mvcc_range(s, 0, kb, 1, kd, 1, &results, &count);
    CETCD_ASSERT_EQ_INT(rc, CETCD_OK);
    CETCD_ASSERT_EQ_INT((int)count, 2);
    cetcd_kv_free_contents(results, count);

    cetcd_mvcc_store_free(s);
}

static void watch_cb(const cetcd_watch_event *ev, void *ud) {
    int *counter = (int*)ud;
    (*counter)++;
    (void)ev;
}

CETCD_TEST_CASE(mvcc_watch_single_key) {
    cetcd_mvcc_store *s = cetcd_mvcc_store_new();
    const uint8_t k[] = "watched";
    int events = 0;

    cetcd_watcher *w = cetcd_mvcc_watch(s, k, 7, 0, watch_cb, &events);
    CETCD_ASSERT_NOT_NULL(w);

    const uint8_t v[] = "hi";
    cetcd_mvcc_put(s, k, 7, v, 2, 0);
    CETCD_ASSERT_EQ_INT(events, 1);

    cetcd_mvcc_delete(s, k, 7);
    CETCD_ASSERT_EQ_INT(events, 2);

    cetcd_mvcc_watch_cancel(s, w);
    cetcd_mvcc_put(s, k, 7, v, 2, 0);
    CETCD_ASSERT_EQ_INT(events, 2);

    cetcd_mvcc_store_free(s);
}

CETCD_TEST_CASE(mvcc_watch_prefix) {
    cetcd_mvcc_store *s = cetcd_mvcc_store_new();
    int events = 0;

    const uint8_t prefix[] = "pfx/";
    cetcd_watcher *w = cetcd_mvcc_watch_prefix(s, prefix, 4, 0, watch_cb, &events);
    CETCD_ASSERT_NOT_NULL(w);

    const uint8_t k1[] = "pfx/a";
    const uint8_t k2[] = "other";
    const uint8_t v[] = "x";
    cetcd_mvcc_put(s, k1, 5, v, 1, 0);
    CETCD_ASSERT_EQ_INT(events, 1);

    cetcd_mvcc_put(s, k2, 5, v, 1, 0);
    CETCD_ASSERT_EQ_INT(events, 1);

    cetcd_mvcc_watch_cancel(s, w);
    cetcd_mvcc_store_free(s);
}

CETCD_TEST_CASE(mvcc_compact) {
    cetcd_mvcc_store *s = cetcd_mvcc_store_new();
    const uint8_t k1[] = "a";
    const uint8_t k2[] = "b";
    const uint8_t v1[] = "1";
    const uint8_t v2[] = "2";
    const uint8_t v3[] = "3";

    cetcd_mvcc_put(s, k1, 1, v1, 1, 0);  /* rev 1 */
    cetcd_mvcc_put(s, k2, 1, v2, 1, 0);  /* rev 2 */
    CETCD_ASSERT_EQ_INT((int)cetcd_mvcc_revision(s), 2);
    CETCD_ASSERT_EQ_INT((int)cetcd_mvcc_compacted_revision(s), 0);

    /* Compact to rev 1 — should remove rev 1 history, keep rev 2 */
    CETCD_ASSERT_EQ_INT(cetcd_mvcc_compact(s, 1), CETCD_OK);
    CETCD_ASSERT_EQ_INT((int)cetcd_mvcc_compacted_revision(s), 1);

    /* Current data still readable */
    cetcd_kv out;
    CETCD_ASSERT_EQ_INT(cetcd_mvcc_get(s, 0, k2, 1, &out), 0);
    free((void*)out.key.data); free((void*)out.value.data);

    /* Compact to rev 2 */
    cetcd_mvcc_put(s, k1, 1, v3, 1, 0);  /* rev 3 */
    CETCD_ASSERT_EQ_INT(cetcd_mvcc_compact(s, 2), CETCD_OK);
    CETCD_ASSERT_EQ_INT((int)cetcd_mvcc_compacted_revision(s), 2);

    CETCD_ASSERT_EQ_INT(cetcd_mvcc_compact(s, 99), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_mvcc_compact(s, 0), CETCD_ERR_INVAL);

    cetcd_mvcc_store_free(s);
}

CETCD_TEST_CASE(mvcc_range_at_revision) {
    cetcd_mvcc_store *s = cetcd_mvcc_store_new();
    const uint8_t ka[] = "a";
    const uint8_t kb[] = "b";
    const uint8_t kc[] = "c";
    const uint8_t v1[] = "1";
    const uint8_t v2[] = "2";
    const uint8_t v3[] = "3";

    cetcd_mvcc_put(s, ka, 1, v1, 1, 0); /* rev 1 */
    cetcd_mvcc_put(s, kb, 1, v2, 1, 0); /* rev 2 */
    cetcd_mvcc_put(s, ka, 1, v3, 1, 0); /* rev 3 overwrite a */
    cetcd_mvcc_delete(s, kb, 1);        /* rev 4 delete b */
    cetcd_mvcc_put(s, kc, 1, v1, 1, 0); /* rev 5 */

    cetcd_kv *results = NULL;
    size_t count = 0;
    /* At rev 2: a=1, b=2 */
    int rc = cetcd_mvcc_range(s, 2, ka, 1, (const uint8_t *)"\xff", 1, &results, &count);
    CETCD_ASSERT_EQ_INT(rc, CETCD_OK);
    CETCD_ASSERT_EQ_INT((int)count, 2);
    cetcd_kv_free_contents(results, count);

    /* At rev 4: a=3, b deleted → only a */
    results = NULL; count = 0;
    rc = cetcd_mvcc_range(s, 4, ka, 1, (const uint8_t *)"\xff", 1, &results, &count);
    CETCD_ASSERT_EQ_INT(rc, CETCD_OK);
    CETCD_ASSERT_EQ_INT((int)count, 1);
    CETCD_ASSERT_TRUE(results[0].key.len == 1 && results[0].key.data[0] == 'a');
    CETCD_ASSERT_TRUE(results[0].value.len == 1 && results[0].value.data[0] == '3');
    cetcd_kv_free_contents(results, count);

    cetcd_mvcc_store_free(s);
}

CETCD_TEST_CASE(mvcc_persist_roundtrip) {
    char path_template[] = "/tmp/cetcd-test-mvcc-XXXXXX";
    CETCD_ASSERT_NOT_NULL(mkdtemp(path_template));

    cetcd_backend_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.path = path_template;
    cfg.map_size = 16 * 1024 * 1024;
    cfg.max_dbs = 8;
    cetcd_backend *be = cetcd_backend_open(&cfg);
    CETCD_ASSERT_NOT_NULL(be);

    cetcd_mvcc_store *s1 = cetcd_mvcc_store_new();
    CETCD_ASSERT_NOT_NULL(s1);
    cetcd_mvcc_set_backend(s1, be);

    const uint8_t k[] = "persist-key";
    const uint8_t v[] = "persist-val";
    cetcd_revision r = cetcd_mvcc_put(s1, k, sizeof(k) - 1, v, sizeof(v) - 1, 0);
    CETCD_ASSERT_TRUE(r.main == 1);
    cetcd_mvcc_store_free(s1);
    cetcd_backend_close(be);

    be = cetcd_backend_open(&cfg);
    CETCD_ASSERT_NOT_NULL(be);
    cetcd_mvcc_store *s2 = cetcd_mvcc_store_new();
    CETCD_ASSERT_EQ_INT(cetcd_mvcc_load(s2, be), CETCD_OK);
    CETCD_ASSERT_TRUE(cetcd_mvcc_revision(s2) == 1);

    cetcd_kv out = {0};
    CETCD_ASSERT_EQ_INT(cetcd_mvcc_get(s2, 0, k, sizeof(k) - 1, &out), CETCD_OK);
    CETCD_ASSERT_EQ_INT((int)out.value.len, (int)(sizeof(v) - 1));
    CETCD_ASSERT_TRUE(memcmp(out.value.data, v, sizeof(v) - 1) == 0);
    free((void *)out.key.data);
    free((void *)out.value.data);

    cetcd_mvcc_store_free(s2);
    cetcd_backend_close(be);
}

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(mvcc_put_get_basic),
    CETCD_TEST_ENTRY(mvcc_multiple_puts_same_key),
    CETCD_TEST_ENTRY(mvcc_delete),
    CETCD_TEST_ENTRY(mvcc_range),
    CETCD_TEST_ENTRY(mvcc_watch_single_key),
    CETCD_TEST_ENTRY(mvcc_watch_prefix),
    CETCD_TEST_ENTRY(mvcc_compact),
    CETCD_TEST_ENTRY(mvcc_range_at_revision),
    CETCD_TEST_ENTRY(mvcc_persist_roundtrip),
CETCD_TEST_LIST_END

CETCD_TEST_MAIN()
