#include <stdlib.h>
#include <string.h>
#include <uv.h>
#include "cetcd/io.h"
#include "io_internal.h"

cetcd_loop *cetcd_loop_new(void) {
    cetcd_loop *l = (cetcd_loop *)calloc(1, sizeof(*l));
    if (!l) return NULL;
    int r = uv_loop_init(&l->uv);
    if (r != 0) {
        free(l);
        return NULL;
    }
    return l;
}

int cetcd_loop_run(cetcd_loop *loop) {
    if (!loop) return -1;
    int r = uv_run(&loop->uv, UV_RUN_DEFAULT);
    return (r == 0) ? 0 : r;
}

void cetcd_loop_stop(cetcd_loop *loop) {
    if (loop) uv_stop(&loop->uv);
}

void cetcd_loop_free(cetcd_loop *loop) {
    if (!loop) return;
    uv_run(&loop->uv, UV_RUN_DEFAULT);
    uv_loop_close(&loop->uv);
    free(loop);
}
