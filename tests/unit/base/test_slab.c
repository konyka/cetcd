#include "cetcd/base.h"
#include "cetcd_test.h"

#include <stdint.h>
#include <string.h>

CETCD_TEST_CASE(slab_alloc_returns_writable_object) {
    cetcd_slab *s = cetcd_slab_new(sizeof(int), 0);
    int *p = (int *)cetcd_slab_alloc(s);
    CETCD_ASSERT_NOT_NULL(p);
    *p = 42;
    CETCD_ASSERT_EQ_INT(*p, 42);
    cetcd_slab_release(s, p);
    cetcd_slab_free(s);
}

CETCD_TEST_CASE(slab_obj_size_is_reported) {
    cetcd_slab *s = cetcd_slab_new(123, 0);
    CETCD_ASSERT_TRUE(cetcd_slab_obj_size(s) >= 123u);
    cetcd_slab_free(s);
}

CETCD_TEST_CASE(slab_live_count_tracks_outstanding) {
    cetcd_slab *s = cetcd_slab_new(sizeof(double), 0);
    void *p1 = cetcd_slab_alloc(s);
    void *p2 = cetcd_slab_alloc(s);
    CETCD_ASSERT_EQ_UINT(cetcd_slab_live_count(s), 2u);
    cetcd_slab_release(s, p1);
    CETCD_ASSERT_EQ_UINT(cetcd_slab_live_count(s), 1u);
    cetcd_slab_release(s, p2);
    CETCD_ASSERT_EQ_UINT(cetcd_slab_live_count(s), 0u);
    cetcd_slab_free(s);
}

CETCD_TEST_CASE(slab_recycles_freed_object) {
    cetcd_slab *s = cetcd_slab_new(sizeof(uint64_t), 0);
    void *p1 = cetcd_slab_alloc(s);
    cetcd_slab_release(s, p1);
    void *p2 = cetcd_slab_alloc(s);
    CETCD_ASSERT_EQ_PTR(p1, p2);
    cetcd_slab_release(s, p2);
    cetcd_slab_free(s);
}

CETCD_TEST_CASE(slab_grows_across_blocks) {
    const size_t per_block = 8;
    cetcd_slab *s = cetcd_slab_new(sizeof(int), per_block);
    void *ptrs[32];
    for (size_t i = 0; i < 32; ++i) {
        ptrs[i] = cetcd_slab_alloc(s);
        CETCD_ASSERT_NOT_NULL(ptrs[i]);
    }
    CETCD_ASSERT_TRUE(cetcd_slab_block_count(s) >= 4);
    for (size_t i = 0; i < 32; ++i) cetcd_slab_release(s, ptrs[i]);
    CETCD_ASSERT_EQ_UINT(cetcd_slab_live_count(s), 0u);
    cetcd_slab_free(s);
}

CETCD_TEST_CASE(slab_handles_many_alloc_release_cycles) {
    cetcd_slab *s = cetcd_slab_new(64, 16);
    for (int round = 0; round < 100; ++round) {
        void *p = cetcd_slab_alloc(s);
        CETCD_ASSERT_NOT_NULL(p);
        memset(p, (int)round, 64);
        cetcd_slab_release(s, p);
    }
    cetcd_slab_free(s);
}

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(slab_alloc_returns_writable_object),
    CETCD_TEST_ENTRY(slab_obj_size_is_reported),
    CETCD_TEST_ENTRY(slab_live_count_tracks_outstanding),
    CETCD_TEST_ENTRY(slab_recycles_freed_object),
    CETCD_TEST_ENTRY(slab_grows_across_blocks),
    CETCD_TEST_ENTRY(slab_handles_many_alloc_release_cycles),
CETCD_TEST_LIST_END
CETCD_TEST_MAIN()
