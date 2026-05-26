#ifndef CETCD_IO_INTERNAL_H_
#define CETCD_IO_INTERNAL_H_

#include <uv.h>

struct cetcd_loop {
    uv_loop_t uv;
};

static inline uv_loop_t *cetcd_loop_uv(cetcd_loop *loop) {
    return loop ? &loop->uv : NULL;
}

#endif
