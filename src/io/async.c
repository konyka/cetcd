#include <stdlib.h>
#include <uv.h>

#include "cetcd/io.h"
#include "io_internal.h"

typedef struct cetcd_async {
    uv_async_t handle;
    cetcd_async_cb cb;
    void *arg;
} cetcd_async;

static void on_async_cb(uv_async_t* handle) {
    cetcd_async *a = (cetcd_async *)handle->data;
    if (a && a->cb) a->cb(a, a->arg);
}

cetcd_async *cetcd_async_new(cetcd_loop *loop, cetcd_async_cb cb, void *arg) {
    if (!loop) return NULL;
    cetcd_async *a = (cetcd_async *)calloc(1, sizeof(*a));
    if (!a) return NULL;
    a->cb = cb;
    a->arg = arg;
    uv_async_init(cetcd_loop_uv(loop), &a->handle, on_async_cb);
    a->handle.data = a;
    return a;
}

static void on_async_close(uv_handle_t *handle) {
    cetcd_async *a = (cetcd_async *)handle->data;
    free(a);
}

void cetcd_async_free(cetcd_async *async) {
    if (!async) return;
    uv_close((uv_handle_t *)&async->handle, on_async_close);
}

void cetcd_async_send(cetcd_async *async) {
    if (async) {
        uv_async_send(&async->handle);
    }
}
