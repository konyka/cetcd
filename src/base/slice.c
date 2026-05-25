#include "cetcd/base.h"

#include <string.h>

bool cetcd_slice_equal(cetcd_slice a, cetcd_slice b) {
    if (a.len != b.len) return false;
    if (a.len == 0)     return true;
    return memcmp(a.data, b.data, a.len) == 0;
}

int cetcd_slice_compare(cetcd_slice a, cetcd_slice b) {
    size_t min_len = a.len < b.len ? a.len : b.len;
    if (min_len > 0) {
        int r = memcmp(a.data, b.data, min_len);
        if (r != 0) return r;
    }
    if (a.len < b.len) return -1;
    if (a.len > b.len) return  1;
    return 0;
}

bool cetcd_slice_has_prefix(cetcd_slice s, cetcd_slice prefix) {
    if (prefix.len > s.len) return false;
    if (prefix.len == 0)    return true;
    return memcmp(s.data, prefix.data, prefix.len) == 0;
}

bool cetcd_slice_has_suffix(cetcd_slice s, cetcd_slice suffix) {
    if (suffix.len > s.len) return false;
    if (suffix.len == 0)    return true;
    return memcmp(s.data + (s.len - suffix.len), suffix.data, suffix.len) == 0;
}

size_t cetcd_slice_copy(void *dst, size_t dst_cap, cetcd_slice src) {
    size_t n = src.len < dst_cap ? src.len : dst_cap;
    if (n > 0) memcpy(dst, src.data, n);
    return n;
}
