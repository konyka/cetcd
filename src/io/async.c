/* Lightweight async wrapper stub for Phase 0 tests. */
#include <stdlib.h>

#include "cetcd/io.h"

typedef struct cetcd_async {
    cetcd_async_cb cb;
    void *arg;
} cetcd_async;

cetcd_async *cetcd_async_new(cetcd_loop *loop, cetcd_async_cb cb, void *arg) {
    (void)loop;
    cetcd_async *a = (cetcd_async *)malloc(sizeof(*a));
    if (a) { a->cb = cb; a->arg = arg; }
    return a;
}

void cetcd_async_free(cetcd_async *async) {
    if (async) free(async);
}

void cetcd_async_send(cetcd_async *async) {
    if (async && async->cb) {
        async->cb(async, async->arg);
    }
}
