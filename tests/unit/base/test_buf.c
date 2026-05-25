#include "cetcd/base.h"
#include "cetcd_test.h"

#include <string.h>

CETCD_TEST_CASE(buf_init_is_empty) {
    cetcd_buf b;
    cetcd_buf_init(&b);
    CETCD_ASSERT_EQ_UINT(b.len, 0u);
    CETCD_ASSERT_EQ_UINT(b.cap, 0u);
    CETCD_ASSERT_NULL(b.data);
    cetcd_buf_free(&b);
}

CETCD_TEST_CASE(buf_append_grows_capacity) {
    cetcd_buf b;
    cetcd_buf_init(&b);
    CETCD_ASSERT_EQ_INT(cetcd_buf_append(&b, "abc", 3), 0);
    CETCD_ASSERT_EQ_UINT(b.len, 3u);
    CETCD_ASSERT_TRUE(b.cap >= 3);
    CETCD_ASSERT_EQ_MEM(b.data, "abc", 3);
    cetcd_buf_free(&b);
}

CETCD_TEST_CASE(buf_append_many_small_writes) {
    cetcd_buf b;
    cetcd_buf_init(&b);
    for (int i = 0; i < 1000; ++i) {
        char c = (char)('a' + (i % 26));
        CETCD_ASSERT_EQ_INT(cetcd_buf_append_byte(&b, (uint8_t)c), 0);
    }
    CETCD_ASSERT_EQ_UINT(b.len, 1000u);
    for (int i = 0; i < 1000; ++i) {
        CETCD_ASSERT_EQ_INT(b.data[i], 'a' + (i % 26));
    }
    cetcd_buf_free(&b);
}

CETCD_TEST_CASE(buf_reserve_keeps_existing_content) {
    cetcd_buf b;
    cetcd_buf_init(&b);
    cetcd_buf_append(&b, "hello", 5);
    CETCD_ASSERT_EQ_INT(cetcd_buf_reserve(&b, 1024), 0);
    CETCD_ASSERT_EQ_UINT(b.len, 5u);
    CETCD_ASSERT_TRUE(b.cap >= 1024);
    CETCD_ASSERT_EQ_MEM(b.data, "hello", 5);
    cetcd_buf_free(&b);
}

CETCD_TEST_CASE(buf_append_slice_works) {
    cetcd_buf b;
    cetcd_buf_init(&b);
    cetcd_slice s = cetcd_slice_from_cstr("etcd");
    CETCD_ASSERT_EQ_INT(cetcd_buf_append_slice(&b, s), 0);
    CETCD_ASSERT_EQ_UINT(b.len, 4u);
    CETCD_ASSERT_EQ_MEM(b.data, "etcd", 4);
    cetcd_buf_free(&b);
}

CETCD_TEST_CASE(buf_printf_formats_correctly) {
    cetcd_buf b;
    cetcd_buf_init(&b);
    CETCD_ASSERT_EQ_INT(cetcd_buf_printf(&b, "x=%d y=%s", 42, "hi"), 0);
    CETCD_ASSERT_EQ_UINT(b.len, strlen("x=42 y=hi"));
    CETCD_ASSERT_EQ_MEM(b.data, "x=42 y=hi", strlen("x=42 y=hi"));
    cetcd_buf_free(&b);
}

CETCD_TEST_CASE(buf_reset_keeps_capacity) {
    cetcd_buf b;
    cetcd_buf_init(&b);
    cetcd_buf_append(&b, "xxxxxxxxxx", 10);
    size_t cap_before = b.cap;
    cetcd_buf_reset(&b);
    CETCD_ASSERT_EQ_UINT(b.len, 0u);
    CETCD_ASSERT_EQ_UINT(b.cap, cap_before);
    cetcd_buf_free(&b);
}

CETCD_TEST_CASE(buf_as_slice_reflects_content) {
    cetcd_buf b;
    cetcd_buf_init(&b);
    cetcd_buf_append(&b, "data", 4);
    cetcd_slice s = cetcd_buf_as_slice(&b);
    CETCD_ASSERT_EQ_UINT(s.len, 4u);
    CETCD_ASSERT_EQ_PTR(s.data, b.data);
    cetcd_buf_free(&b);
}

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(buf_init_is_empty),
    CETCD_TEST_ENTRY(buf_append_grows_capacity),
    CETCD_TEST_ENTRY(buf_append_many_small_writes),
    CETCD_TEST_ENTRY(buf_reserve_keeps_existing_content),
    CETCD_TEST_ENTRY(buf_append_slice_works),
    CETCD_TEST_ENTRY(buf_printf_formats_correctly),
    CETCD_TEST_ENTRY(buf_reset_keeps_capacity),
    CETCD_TEST_ENTRY(buf_as_slice_reflects_content),
CETCD_TEST_LIST_END
CETCD_TEST_MAIN()
