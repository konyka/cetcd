#include "cetcd/base.h"
#include "cetcd_test.h"

#include <stdint.h>
#include <string.h>

CETCD_TEST_CASE(arena_new_and_free) {
    cetcd_arena *a = cetcd_arena_new(0);
    CETCD_ASSERT_NOT_NULL(a);
    cetcd_arena_free(a);
}

CETCD_TEST_CASE(arena_alloc_returns_writable_memory) {
    cetcd_arena *a = cetcd_arena_new(0);
    int *p = (int *)cetcd_arena_alloc(a, sizeof(int) * 16);
    CETCD_ASSERT_NOT_NULL(p);
    for (int i = 0; i < 16; ++i) p[i] = i;
    for (int i = 0; i < 16; ++i) CETCD_ASSERT_EQ_INT(p[i], i);
    cetcd_arena_free(a);
}

CETCD_TEST_CASE(arena_zero_length_returns_nonnull_or_sentinel) {
    cetcd_arena *a = cetcd_arena_new(0);
    void *p = cetcd_arena_alloc(a, 0);
    (void)p;
    cetcd_arena_free(a);
}

CETCD_TEST_CASE(arena_alloc_aligned_returns_aligned_pointers) {
    cetcd_arena *a = cetcd_arena_new(0);
    void *p1 = cetcd_arena_alloc_aligned(a, 7, 1);
    (void)p1;
    void *p16 = cetcd_arena_alloc_aligned(a, 32, 16);
    CETCD_ASSERT_NOT_NULL(p16);
    CETCD_ASSERT_EQ_UINT(((uintptr_t)p16) % 16, 0u);
    void *p64 = cetcd_arena_alloc_aligned(a, 100, 64);
    CETCD_ASSERT_NOT_NULL(p64);
    CETCD_ASSERT_EQ_UINT(((uintptr_t)p64) % 64, 0u);
    cetcd_arena_free(a);
}

CETCD_TEST_CASE(arena_strdup_copies) {
    cetcd_arena *a = cetcd_arena_new(0);
    const char *src = "hello arena";
    char *dup = cetcd_arena_strdup(a, src);
    CETCD_ASSERT_NOT_NULL(dup);
    CETCD_ASSERT_NE_INT((long long)(intptr_t)dup, (long long)(intptr_t)src);
    CETCD_ASSERT_EQ_STR(dup, src);
    cetcd_arena_free(a);
}

CETCD_TEST_CASE(arena_memdup_copies) {
    cetcd_arena *a = cetcd_arena_new(0);
    const char src[] = {1, 2, 3, 4, 0, 5};
    void *dup = cetcd_arena_memdup(a, src, sizeof(src));
    CETCD_ASSERT_NOT_NULL(dup);
    CETCD_ASSERT_EQ_MEM(dup, src, sizeof(src));
    cetcd_arena_free(a);
}

CETCD_TEST_CASE(arena_reset_reuses_first_block) {
    cetcd_arena *a = cetcd_arena_new(0);
    cetcd_arena_alloc(a, 1024);
    cetcd_arena_alloc(a, 1024);
    size_t blocks_before = cetcd_arena_block_count(a);
    cetcd_arena_reset(a);
    void *p = cetcd_arena_alloc(a, 16);
    CETCD_ASSERT_NOT_NULL(p);
    size_t blocks_after = cetcd_arena_block_count(a);
    CETCD_ASSERT_TRUE(blocks_after <= blocks_before);
    CETCD_ASSERT_TRUE(cetcd_arena_total_bytes_used(a) <= 64);
    cetcd_arena_free(a);
}

CETCD_TEST_CASE(arena_grows_when_block_full) {
    cetcd_arena *a = cetcd_arena_new(4096);
    for (int i = 0; i < 10; ++i) {
        void *p = cetcd_arena_alloc(a, 1024);
        CETCD_ASSERT_NOT_NULL(p);
    }
    CETCD_ASSERT_TRUE(cetcd_arena_block_count(a) >= 2);
    cetcd_arena_free(a);
}

CETCD_TEST_CASE(arena_supports_huge_allocation) {
    cetcd_arena *a = cetcd_arena_new(1024);
    void *p = cetcd_arena_alloc(a, 1 << 20);
    CETCD_ASSERT_NOT_NULL(p);
    memset(p, 0xAB, 1 << 20);
    cetcd_arena_free(a);
}

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(arena_new_and_free),
    CETCD_TEST_ENTRY(arena_alloc_returns_writable_memory),
    CETCD_TEST_ENTRY(arena_zero_length_returns_nonnull_or_sentinel),
    CETCD_TEST_ENTRY(arena_alloc_aligned_returns_aligned_pointers),
    CETCD_TEST_ENTRY(arena_strdup_copies),
    CETCD_TEST_ENTRY(arena_memdup_copies),
    CETCD_TEST_ENTRY(arena_reset_reuses_first_block),
    CETCD_TEST_ENTRY(arena_grows_when_block_full),
    CETCD_TEST_ENTRY(arena_supports_huge_allocation),
CETCD_TEST_LIST_END
CETCD_TEST_MAIN()
