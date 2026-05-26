#include <stdlib.h>
#include <uv.h>

#include "cetcd/io.h"
#include "io_internal.h"

typedef struct cetcd_timer {
    uv_timer_t handle;
    cetcd_co_fn cb;
    void *arg;
} cetcd_timer;

cetcd_timer *cetcd_timer_new(cetcd_loop *loop) {
    if (!loop) return NULL;
    cetcd_timer *t = (cetcd_timer *)calloc(1, sizeof(*t));
    if (!t) return NULL;
    uv_timer_init(cetcd_loop_uv(loop), &t->handle);
    t->handle.data = t;
    return t;
}

static void on_timer_close(uv_handle_t* handle) {
    cetcd_timer *t = (cetcd_timer *)handle->data;
    if (t) free(t);
}

static void timer_cb(uv_timer_t* handle) {
    cetcd_timer *t = (cetcd_timer *)handle->data;
    if (t && t->cb) t->cb(t->arg);
}

void cetcd_timer_free(cetcd_timer *timer) {
    if (!timer) return;
    uv_close((uv_handle_t *)&timer->handle, on_timer_close);
}

void cetcd_timer_start(cetcd_timer *timer, uint64_t timeout_ms, uint64_t repeat_ms,
                      cetcd_co_fn cb, void *arg) {
    if (!timer) return;
    timer->cb = cb;
    timer->arg = arg;
    if (timeout_ms == 0) {
        if (cb) cb(arg);
        return;
    }
    timer->handle.data = timer;
    uv_timer_start(&timer->handle, timer_cb, (uint64_t)timeout_ms, (uint64_t)repeat_ms);
}

void cetcd_timer_stop(cetcd_timer *timer) {
    if (!timer) return;
    uv_timer_stop(&timer->handle);
}
