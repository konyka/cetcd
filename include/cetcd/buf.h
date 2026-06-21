#ifndef CETCD_BUF_H_
#define CETCD_BUF_H_

#include <stddef.h>
#include <stdint.h>

#include "cetcd/slice.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct cetcd_buf {
    uint8_t *data;
    size_t   len;
    size_t   cap;
} cetcd_buf;

typedef cetcd_buf cetcd_buf_t;

void        cetcd_buf_init(cetcd_buf *b);
void        cetcd_buf_free(cetcd_buf *b);
int         cetcd_buf_reserve(cetcd_buf *b, size_t cap);
int         cetcd_buf_append(cetcd_buf *b, const void *p, size_t n);
int         cetcd_buf_append_byte(cetcd_buf *b, uint8_t v);
int         cetcd_buf_append_slice(cetcd_buf *b, cetcd_slice s);
int         cetcd_buf_append_cstr(cetcd_buf *b, const char *s);
int         cetcd_buf_printf(cetcd_buf *b, const char *fmt, ...);
void        cetcd_buf_reset(cetcd_buf *b);
cetcd_slice cetcd_buf_as_slice(const cetcd_buf *b);

#ifdef __cplusplus
}
#endif
#endif
