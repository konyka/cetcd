#include "cetcd/base.h"
#include "cetcd_test.h"

#include <stdint.h>
#include <string.h>

CETCD_TEST_CASE(fnv1a64_empty_is_offset_basis) {
    /* Standard FNV-1a 64-bit offset basis. */
    uint64_t h = cetcd_hash_fnv1a64("", 0);
    CETCD_ASSERT_EQ_UINT(h, 0xcbf29ce484222325ull);
}

CETCD_TEST_CASE(fnv1a64_known_vectors) {
    /* From http://www.isthe.com/chongo/tech/comp/fnv/ test vectors. */
    CETCD_ASSERT_EQ_UINT(cetcd_hash_fnv1a64("a", 1),    0xaf63dc4c8601ec8cull);
    CETCD_ASSERT_EQ_UINT(cetcd_hash_fnv1a64("foobar", 6), 0x85944171f73967e8ull);
}

CETCD_TEST_CASE(fnv1a64_different_inputs_differ) {
    uint64_t a = cetcd_hash_fnv1a64("abc", 3);
    uint64_t b = cetcd_hash_fnv1a64("abd", 3);
    CETCD_ASSERT_NE_INT((long long)a, (long long)b);
}

CETCD_TEST_CASE(fnv1a64_seed_changes_result) {
    uint64_t a = cetcd_hash_fnv1a64_seed("abc", 3, 0);
    uint64_t b = cetcd_hash_fnv1a64_seed("abc", 3, 1);
    CETCD_ASSERT_NE_INT((long long)a, (long long)b);
}

CETCD_TEST_CASE(crc32c_empty_is_zero) {
    /* CRC32C of empty input with init 0 is 0. */
    uint32_t c = cetcd_crc32c(0, "", 0);
    CETCD_ASSERT_EQ_UINT(c, 0u);
}

CETCD_TEST_CASE(crc32c_known_vector) {
    /* CRC32C of "123456789" with init 0 = 0xE3069283 (Castagnoli). */
    uint32_t c = cetcd_crc32c(0, "123456789", 9);
    CETCD_ASSERT_EQ_UINT(c, 0xE3069283u);
}

CETCD_TEST_CASE(crc32c_is_chainable) {
    uint32_t one_shot = cetcd_crc32c(0, "hello world", 11);
    uint32_t step1   = cetcd_crc32c(0, "hello ", 6);
    uint32_t step2   = cetcd_crc32c(step1, "world", 5);
    CETCD_ASSERT_EQ_UINT(one_shot, step2);
}

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(fnv1a64_empty_is_offset_basis),
    CETCD_TEST_ENTRY(fnv1a64_known_vectors),
    CETCD_TEST_ENTRY(fnv1a64_different_inputs_differ),
    CETCD_TEST_ENTRY(fnv1a64_seed_changes_result),
    CETCD_TEST_ENTRY(crc32c_empty_is_zero),
    CETCD_TEST_ENTRY(crc32c_known_vector),
    CETCD_TEST_ENTRY(crc32c_is_chainable),
CETCD_TEST_LIST_END
CETCD_TEST_MAIN()
