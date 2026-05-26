#include "cetcd/base.h"
#include "cetcd_test.h"
#include <string.h>

#define SL(s) cetcd_slice_make((s), strlen(s))

typedef struct {
    int values[16];
    int count;
} iter_helper;

static bool iter_collect(cetcd_slice key, void *val, void *ud) {
    (void)key;
    iter_helper *h = (iter_helper*)ud;
    h->values[h->count++] = (int)(intptr_t)val;
    return true;
}

CETCD_TEST_CASE(treap_put_get_del) {
    cetcd_treap *t = cetcd_treap_new();
    CETCD_ASSERT_NOT_NULL(t);
    CETCD_ASSERT_EQ_INT(cetcd_treap_size(t), 0);

    cetcd_slice k1 = SL("alpha");
    cetcd_slice k2 = SL("bravo");
    cetcd_slice k3 = SL("charlie");

    CETCD_ASSERT_EQ_INT(cetcd_treap_put(t, k1, (void*)1L), CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_treap_put(t, k2, (void*)2L), CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_treap_put(t, k3, (void*)3L), CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_treap_size(t), 3);

    void *val = NULL;
    CETCD_ASSERT_TRUE(cetcd_treap_get(t, k1, &val));
    CETCD_ASSERT_TRUE(val == (void*)1L);
    CETCD_ASSERT_TRUE(cetcd_treap_get(t, k2, &val));
    CETCD_ASSERT_TRUE(val == (void*)2L);

    CETCD_ASSERT_FALSE(cetcd_treap_get(t, SL("zzz"), NULL));

    CETCD_ASSERT_TRUE(cetcd_treap_del(t, k2, &val));
    CETCD_ASSERT_TRUE(val == (void*)2L);
    CETCD_ASSERT_EQ_INT(cetcd_treap_size(t), 2);
    CETCD_ASSERT_FALSE(cetcd_treap_del(t, k2, NULL));

    cetcd_treap_free(t);
}

CETCD_TEST_CASE(treap_put_replace) {
    cetcd_treap *t = cetcd_treap_new();
    cetcd_slice k = SL("key");
    CETCD_ASSERT_EQ_INT(cetcd_treap_put(t, k, (void*)10L), CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_treap_put(t, k, (void*)20L), CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_treap_size(t), 1);

    void *val = NULL;
    CETCD_ASSERT_TRUE(cetcd_treap_get(t, k, &val));
    CETCD_ASSERT_TRUE(val == (void*)20L);
    cetcd_treap_free(t);
}

CETCD_TEST_CASE(treap_ordered_iter) {
    cetcd_treap *t = cetcd_treap_new();
    cetcd_treap_put(t, SL("cherry"), (void*)3L);
    cetcd_treap_put(t, SL("apple"), (void*)1L);
    cetcd_treap_put(t, SL("banana"), (void*)2L);

    iter_helper h = {.count = 0};
    cetcd_treap_iter(t, iter_collect, &h);
    CETCD_ASSERT_EQ_INT(h.count, 3);
    CETCD_ASSERT_EQ_INT(h.values[0], 1);
    CETCD_ASSERT_EQ_INT(h.values[1], 2);
    CETCD_ASSERT_EQ_INT(h.values[2], 3);
    cetcd_treap_free(t);
}

CETCD_TEST_CASE(treap_range_query) {
    cetcd_treap *t = cetcd_treap_new();
    cetcd_treap_put(t, SL("a"), (void*)1L);
    cetcd_treap_put(t, SL("b"), (void*)2L);
    cetcd_treap_put(t, SL("c"), (void*)3L);
    cetcd_treap_put(t, SL("d"), (void*)4L);
    cetcd_treap_put(t, SL("e"), (void*)5L);

    iter_helper h = {.count = 0};
    cetcd_treap_range(t, SL("b"), SL("d"), iter_collect, &h);
    CETCD_ASSERT_EQ_INT(h.count, 2);
    CETCD_ASSERT_EQ_INT(h.values[0], 2);
    CETCD_ASSERT_EQ_INT(h.values[1], 3);
    cetcd_treap_free(t);
}

CETCD_TEST_CASE(treap_stress_100) {
    cetcd_treap *t = cetcd_treap_new();
    char buf[16];
    for (int i = 0; i < 100; i++) {
        snprintf(buf, sizeof(buf), "key-%04d", i);
        cetcd_slice k = cetcd_slice_from_cstr(buf);
        CETCD_ASSERT_EQ_INT(cetcd_treap_put(t, k, (void*)(intptr_t)(i + 1)), CETCD_OK);
    }
    CETCD_ASSERT_EQ_INT(cetcd_treap_size(t), 100);

    void *val = NULL;
    CETCD_ASSERT_TRUE(cetcd_treap_get(t, SL("key-0050"), &val));
    CETCD_ASSERT_TRUE(val == (void*)51L);

    CETCD_ASSERT_TRUE(cetcd_treap_del(t, SL("key-0050"), NULL));
    CETCD_ASSERT_EQ_INT(cetcd_treap_size(t), 99);
    CETCD_ASSERT_FALSE(cetcd_treap_get(t, SL("key-0050"), NULL));

    cetcd_treap_free(t);
}

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(treap_put_get_del),
    CETCD_TEST_ENTRY(treap_put_replace),
    CETCD_TEST_ENTRY(treap_ordered_iter),
    CETCD_TEST_ENTRY(treap_range_query),
    CETCD_TEST_ENTRY(treap_stress_100),
CETCD_TEST_LIST_END

CETCD_TEST_MAIN()
