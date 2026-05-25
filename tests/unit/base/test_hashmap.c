#include "cetcd/base.h"
#include "cetcd_test.h"

#include <stdio.h>
#include <string.h>

static cetcd_slice S(const char *s) { return cetcd_slice_from_cstr(s); }

CETCD_TEST_CASE(hashmap_empty_after_new) {
    cetcd_hashmap *m = cetcd_hashmap_new(0);
    CETCD_ASSERT_EQ_UINT(cetcd_hashmap_size(m), 0u);
    CETCD_ASSERT_FALSE(cetcd_hashmap_contains(m, S("missing")));
    cetcd_hashmap_free(m);
}

CETCD_TEST_CASE(hashmap_put_then_get) {
    cetcd_hashmap *m = cetcd_hashmap_new(0);
    int v = 7;
    CETCD_ASSERT_EQ_INT(cetcd_hashmap_put(m, S("foo"), &v), 0);
    CETCD_ASSERT_EQ_UINT(cetcd_hashmap_size(m), 1u);
    void *got = NULL;
    CETCD_ASSERT_TRUE(cetcd_hashmap_get(m, S("foo"), &got));
    CETCD_ASSERT_EQ_PTR(got, &v);
    cetcd_hashmap_free(m);
}

CETCD_TEST_CASE(hashmap_put_overwrites_existing) {
    cetcd_hashmap *m = cetcd_hashmap_new(0);
    int v1 = 1, v2 = 2;
    cetcd_hashmap_put(m, S("k"), &v1);
    cetcd_hashmap_put(m, S("k"), &v2);
    CETCD_ASSERT_EQ_UINT(cetcd_hashmap_size(m), 1u);
    void *got = NULL;
    cetcd_hashmap_get(m, S("k"), &got);
    CETCD_ASSERT_EQ_PTR(got, &v2);
    cetcd_hashmap_free(m);
}

CETCD_TEST_CASE(hashmap_remove_returns_value) {
    cetcd_hashmap *m = cetcd_hashmap_new(0);
    int v = 99;
    cetcd_hashmap_put(m, S("zap"), &v);
    void *out = NULL;
    CETCD_ASSERT_TRUE(cetcd_hashmap_remove(m, S("zap"), &out));
    CETCD_ASSERT_EQ_PTR(out, &v);
    CETCD_ASSERT_EQ_UINT(cetcd_hashmap_size(m), 0u);
    CETCD_ASSERT_FALSE(cetcd_hashmap_remove(m, S("zap"), NULL));
    cetcd_hashmap_free(m);
}

CETCD_TEST_CASE(hashmap_get_missing_returns_false) {
    cetcd_hashmap *m = cetcd_hashmap_new(0);
    void *got = (void *)0x1;
    CETCD_ASSERT_FALSE(cetcd_hashmap_get(m, S("nope"), &got));
    cetcd_hashmap_free(m);
}

CETCD_TEST_CASE(hashmap_grows_under_load) {
    cetcd_hashmap *m = cetcd_hashmap_new(4);
    enum { N = 2000 };
    static int values[N];
    static char keys[N][24];
    for (int i = 0; i < N; ++i) {
        values[i] = i;
        snprintf(keys[i], sizeof(keys[i]), "k%06d", i);
        CETCD_ASSERT_EQ_INT(cetcd_hashmap_put(m, S(keys[i]), &values[i]), 0);
    }
    CETCD_ASSERT_EQ_UINT(cetcd_hashmap_size(m), (size_t)N);
    for (int i = 0; i < N; ++i) {
        void *got = NULL;
        CETCD_ASSERT_TRUE(cetcd_hashmap_get(m, S(keys[i]), &got));
        CETCD_ASSERT_EQ_INT(*(int *)got, i);
    }
    cetcd_hashmap_free(m);
}

CETCD_TEST_CASE(hashmap_handles_binary_keys_with_zero_bytes) {
    cetcd_hashmap *m = cetcd_hashmap_new(0);
    const uint8_t key1[] = { 1, 0, 2 };
    const uint8_t key2[] = { 1, 0, 3 };
    int v1 = 11, v2 = 22;
    cetcd_hashmap_put(m, cetcd_slice_make(key1, sizeof(key1)), &v1);
    cetcd_hashmap_put(m, cetcd_slice_make(key2, sizeof(key2)), &v2);
    CETCD_ASSERT_EQ_UINT(cetcd_hashmap_size(m), 2u);
    void *got = NULL;
    cetcd_hashmap_get(m, cetcd_slice_make(key1, sizeof(key1)), &got);
    CETCD_ASSERT_EQ_PTR(got, &v1);
    cetcd_hashmap_get(m, cetcd_slice_make(key2, sizeof(key2)), &got);
    CETCD_ASSERT_EQ_PTR(got, &v2);
    cetcd_hashmap_free(m);
}

static bool count_cb(cetcd_slice key, void *value, void *udata) {
    (void)key; (void)value;
    int *counter = (int *)udata;
    ++(*counter);
    return true;
}

CETCD_TEST_CASE(hashmap_iter_visits_all_entries) {
    cetcd_hashmap *m = cetcd_hashmap_new(0);
    int a = 1, b = 2, c = 3;
    cetcd_hashmap_put(m, S("a"), &a);
    cetcd_hashmap_put(m, S("b"), &b);
    cetcd_hashmap_put(m, S("c"), &c);
    int n = 0;
    cetcd_hashmap_iter(m, count_cb, &n);
    CETCD_ASSERT_EQ_INT(n, 3);
    cetcd_hashmap_free(m);
}

static bool stop_after_one(cetcd_slice key, void *value, void *udata) {
    (void)key; (void)value;
    int *n = (int *)udata;
    ++(*n);
    return false;
}

CETCD_TEST_CASE(hashmap_iter_can_short_circuit) {
    cetcd_hashmap *m = cetcd_hashmap_new(0);
    int a = 1, b = 2;
    cetcd_hashmap_put(m, S("a"), &a);
    cetcd_hashmap_put(m, S("b"), &b);
    int n = 0;
    cetcd_hashmap_iter(m, stop_after_one, &n);
    CETCD_ASSERT_EQ_INT(n, 1);
    cetcd_hashmap_free(m);
}

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(hashmap_empty_after_new),
    CETCD_TEST_ENTRY(hashmap_put_then_get),
    CETCD_TEST_ENTRY(hashmap_put_overwrites_existing),
    CETCD_TEST_ENTRY(hashmap_remove_returns_value),
    CETCD_TEST_ENTRY(hashmap_get_missing_returns_false),
    CETCD_TEST_ENTRY(hashmap_grows_under_load),
    CETCD_TEST_ENTRY(hashmap_handles_binary_keys_with_zero_bytes),
    CETCD_TEST_ENTRY(hashmap_iter_visits_all_entries),
    CETCD_TEST_ENTRY(hashmap_iter_can_short_circuit),
CETCD_TEST_LIST_END
CETCD_TEST_MAIN()
