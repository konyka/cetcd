#ifndef CETCD_IO_INTERNAL_H_
#define CETCD_IO_INTERNAL_H_

#include <uv.h>

struct cetcd_loop {
    uv_loop_t uv;
};

static inline uv_loop_t *cetcd_loop_uv(cetcd_loop *loop) {
    return loop ? &loop->uv : NULL;
}

uv_stream_t *cetcd_tcp_stream(cetcd_tcp *tcp);

#endif
