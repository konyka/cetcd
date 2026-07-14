#ifndef CETCD_SLICE_H_
#define CETCD_SLICE_H_

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct cetcd_slice {
    const uint8_t *data;
    size_t         len;
} cetcd_slice;

static inline cetcd_slice cetcd_slice_make(const void *p, size_t n) {
    cetcd_slice s;
    s.data = (const uint8_t *)p;
    s.len  = n;
    return s;
}

static inline cetcd_slice cetcd_slice_from_cstr(const char *s) {
    cetcd_slice out;
    out.data = (const uint8_t *)s;
    out.len  = s ? strlen(s) : 0;
    return out;
}

bool   cetcd_slice_equal(cetcd_slice a, cetcd_slice b);
int    cetcd_slice_compare(cetcd_slice a, cetcd_slice b);
bool   cetcd_slice_has_prefix(cetcd_slice s, cetcd_slice prefix);
bool   cetcd_slice_has_suffix(cetcd_slice s, cetcd_slice suffix);
size_t cetcd_slice_copy(void *dst, size_t dst_cap, cetcd_slice src);

/* etcd PrefixEnd: first key strictly after all keys with the given prefix.
 * Empty or all-0xFF keys yield a single 0x00 (FromKey / open upper bound).
 * Returns bytes written, or 0 if out_cap is too small. */
size_t cetcd_key_prefix_end(uint8_t *out, size_t out_cap, cetcd_slice key);

#ifdef __cplusplus
}
#endif
#endif
