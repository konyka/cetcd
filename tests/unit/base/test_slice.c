#include "cetcd/base.h"
#include "cetcd_test.h"

#include <string.h>

CETCD_TEST_CASE(slice_make_holds_pointer_and_length) {
    const char *data = "hello";
    cetcd_slice s = cetcd_slice_make(data, 5);
    CETCD_ASSERT_EQ_PTR(s.data, (const uint8_t *)data);
    CETCD_ASSERT_EQ_UINT(s.len, 5u);
}

CETCD_TEST_CASE(slice_from_cstr_uses_strlen) {
    cetcd_slice s = cetcd_slice_from_cstr("etcd");
    CETCD_ASSERT_EQ_UINT(s.len, 4u);
    CETCD_ASSERT_EQ_MEM(s.data, "etcd", 4);
}

CETCD_TEST_CASE(slice_from_cstr_handles_null) {
    cetcd_slice s = cetcd_slice_from_cstr(NULL);
    CETCD_ASSERT_EQ_UINT(s.len, 0u);
}

CETCD_TEST_CASE(slice_equal_same_bytes) {
    cetcd_slice a = cetcd_slice_from_cstr("abc");
    cetcd_slice b = cetcd_slice_from_cstr("abc");
    CETCD_ASSERT_TRUE(cetcd_slice_equal(a, b));
}

CETCD_TEST_CASE(slice_equal_different_bytes) {
    cetcd_slice a = cetcd_slice_from_cstr("abc");
    cetcd_slice b = cetcd_slice_from_cstr("abd");
    CETCD_ASSERT_FALSE(cetcd_slice_equal(a, b));
}

CETCD_TEST_CASE(slice_equal_different_lengths) {
    cetcd_slice a = cetcd_slice_from_cstr("ab");
    cetcd_slice b = cetcd_slice_from_cstr("abc");
    CETCD_ASSERT_FALSE(cetcd_slice_equal(a, b));
}

CETCD_TEST_CASE(slice_equal_empty_both) {
    cetcd_slice a = cetcd_slice_make(NULL, 0);
    cetcd_slice b = cetcd_slice_make("", 0);
    CETCD_ASSERT_TRUE(cetcd_slice_equal(a, b));
}

CETCD_TEST_CASE(slice_compare_orderings) {
    cetcd_slice a = cetcd_slice_from_cstr("abc");
    cetcd_slice b = cetcd_slice_from_cstr("abd");
    cetcd_slice c = cetcd_slice_from_cstr("abc");
    cetcd_slice d = cetcd_slice_from_cstr("abcd");
    CETCD_ASSERT_TRUE(cetcd_slice_compare(a, b) < 0);
    CETCD_ASSERT_TRUE(cetcd_slice_compare(b, a) > 0);
    CETCD_ASSERT_TRUE(cetcd_slice_compare(a, c) == 0);
    CETCD_ASSERT_TRUE(cetcd_slice_compare(a, d) < 0);
    CETCD_ASSERT_TRUE(cetcd_slice_compare(d, a) > 0);
}

CETCD_TEST_CASE(slice_has_prefix_true) {
    cetcd_slice s = cetcd_slice_from_cstr("hello world");
    CETCD_ASSERT_TRUE(cetcd_slice_has_prefix(s, cetcd_slice_from_cstr("hello")));
    CETCD_ASSERT_TRUE(cetcd_slice_has_prefix(s, cetcd_slice_from_cstr("")));
    CETCD_ASSERT_TRUE(cetcd_slice_has_prefix(s, s));
}

CETCD_TEST_CASE(slice_has_prefix_false) {
    cetcd_slice s = cetcd_slice_from_cstr("hello");
    CETCD_ASSERT_FALSE(cetcd_slice_has_prefix(s, cetcd_slice_from_cstr("world")));
    CETCD_ASSERT_FALSE(cetcd_slice_has_prefix(s, cetcd_slice_from_cstr("hello!")));
}

CETCD_TEST_CASE(slice_has_suffix) {
    cetcd_slice s = cetcd_slice_from_cstr("foo.bar");
    CETCD_ASSERT_TRUE(cetcd_slice_has_suffix(s, cetcd_slice_from_cstr("bar")));
    CETCD_ASSERT_TRUE(cetcd_slice_has_suffix(s, cetcd_slice_from_cstr("")));
    CETCD_ASSERT_FALSE(cetcd_slice_has_suffix(s, cetcd_slice_from_cstr("foo")));
    CETCD_ASSERT_FALSE(cetcd_slice_has_suffix(s, cetcd_slice_from_cstr("longer-than-s")));
}

CETCD_TEST_CASE(slice_copy_truncates_when_dst_smaller) {
    char dst[3] = {0};
    cetcd_slice s = cetcd_slice_from_cstr("hello");
    size_t n = cetcd_slice_copy(dst, sizeof(dst), s);
    CETCD_ASSERT_EQ_UINT(n, 3u);
    CETCD_ASSERT_EQ_MEM(dst, "hel", 3);
}

CETCD_TEST_CASE(slice_copy_full_when_dst_large) {
    char dst[16] = {0};
    cetcd_slice s = cetcd_slice_from_cstr("abc");
    size_t n = cetcd_slice_copy(dst, sizeof(dst), s);
    CETCD_ASSERT_EQ_UINT(n, 3u);
    CETCD_ASSERT_EQ_MEM(dst, "abc", 3);
}

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(slice_make_holds_pointer_and_length),
    CETCD_TEST_ENTRY(slice_from_cstr_uses_strlen),
    CETCD_TEST_ENTRY(slice_from_cstr_handles_null),
    CETCD_TEST_ENTRY(slice_equal_same_bytes),
    CETCD_TEST_ENTRY(slice_equal_different_bytes),
    CETCD_TEST_ENTRY(slice_equal_different_lengths),
    CETCD_TEST_ENTRY(slice_equal_empty_both),
    CETCD_TEST_ENTRY(slice_compare_orderings),
    CETCD_TEST_ENTRY(slice_has_prefix_true),
    CETCD_TEST_ENTRY(slice_has_prefix_false),
    CETCD_TEST_ENTRY(slice_has_suffix),
    CETCD_TEST_ENTRY(slice_copy_truncates_when_dst_smaller),
    CETCD_TEST_ENTRY(slice_copy_full_when_dst_large),
CETCD_TEST_LIST_END
CETCD_TEST_MAIN()
