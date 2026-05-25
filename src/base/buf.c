#include "cetcd/base.h"

#include <stdarg.h>
#include <stdlib.h>
#include <string.h>

static int buf_grow_to_(cetcd_buf *b, size_t want) {
    if (want <= b->cap) return 0;
    size_t new_cap = b->cap == 0 ? 16 : b->cap;
    while (new_cap < want) {
        if (new_cap > (size_t)-1 / 2) {
            return CETCD_ERR_OVERFLOW;
        }
        new_cap *= 2;
    }
    uint8_t *p = (uint8_t *)realloc(b->data, new_cap);
    if (p == NULL) return CETCD_ERR_NOMEM;
    b->data = p;
    b->cap  = new_cap;
    return 0;
}

void cetcd_buf_init(cetcd_buf *b) {
    b->data = NULL;
    b->len  = 0;
    b->cap  = 0;
}

void cetcd_buf_free(cetcd_buf *b) {
    if (b == NULL) return;
    free(b->data);
    b->data = NULL;
    b->len  = 0;
    b->cap  = 0;
}

int cetcd_buf_reserve(cetcd_buf *b, size_t cap) {
    return buf_grow_to_(b, cap);
}

int cetcd_buf_append(cetcd_buf *b, const void *p, size_t n) {
    if (n == 0) return 0;
    int rc = buf_grow_to_(b, b->len + n);
    if (rc != 0) return rc;
    memcpy(b->data + b->len, p, n);
    b->len += n;
    return 0;
}

int cetcd_buf_append_byte(cetcd_buf *b, uint8_t v) {
    int rc = buf_grow_to_(b, b->len + 1);
    if (rc != 0) return rc;
    b->data[b->len++] = v;
    return 0;
}

int cetcd_buf_append_slice(cetcd_buf *b, cetcd_slice s) {
    return cetcd_buf_append(b, s.data, s.len);
}

int cetcd_buf_append_cstr(cetcd_buf *b, const char *s) {
    if (s == NULL) return 0;
    return cetcd_buf_append(b, s, strlen(s));
}

int cetcd_buf_printf(cetcd_buf *b, const char *fmt, ...) {
    va_list ap, ap2;
    va_start(ap, fmt);
    va_copy(ap2, ap);

    int needed = vsnprintf(NULL, 0, fmt, ap);
    va_end(ap);
    if (needed < 0) { va_end(ap2); return CETCD_ERR_INTERNAL; }

    int rc = buf_grow_to_(b, b->len + (size_t)needed + 1);
    if (rc != 0) { va_end(ap2); return rc; }

    vsnprintf((char *)b->data + b->len, (size_t)needed + 1, fmt, ap2);
    va_end(ap2);
    b->len += (size_t)needed;
    return 0;
}

void cetcd_buf_reset(cetcd_buf *b) {
    b->len = 0;
}

cetcd_slice cetcd_buf_as_slice(const cetcd_buf *b) {
    cetcd_slice s;
    s.data = b->data;
    s.len  = b->len;
    return s;
}
