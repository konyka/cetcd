#include <stdlib.h>
#include <string.h>
#include <uv.h>
#include "cetcd/io.h"
#include "io_internal.h"

/* ── Resume-queue async callback ───────────────────────────────────────── */
static void on_resume_async_cb(uv_async_t *handle) {
    cetcd_loop *loop = (cetcd_loop *)handle->data;
    if (!loop) return;
    /* Drain the queue and resume each coroutine. */
    while (loop->resume_queue) {
        cetcd_resume_entry_ *e = loop->resume_queue;
        loop->resume_queue = e->next;
        cetcd_co *co = e->co;
        free(e);
        if (!cetcd_co_dead(co)) {
            cetcd_co_resume(co);
        }
    }
}

/* ── Loop lifecycle ────────────────────────────────────────────────────── */

cetcd_loop *cetcd_loop_new(void) {
    cetcd_loop *l = (cetcd_loop *)calloc(1, sizeof(*l));
    if (!l) return NULL;
    int r = uv_loop_init(&l->uv);
    if (r != 0) {
        free(l);
        return NULL;
    }
    l->co_sched = coroutine_open();
    if (!l->co_sched) {
        uv_loop_close(&l->uv);
        free(l);
        return NULL;
    }
    /* Initialize the resume async handle */
    uv_async_init(&l->uv, &l->resume_async, on_resume_async_cb);
    l->resume_async.data = l;
    l->resume_queue = NULL;
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

static void on_resume_async_close(uv_handle_t *handle) {
    /* Nothing to do — the loop itself owns the memory and will be freed
     * after uv_loop_close drains all handles. */
    (void)handle;
}

void cetcd_loop_free(cetcd_loop *loop) {
    if (!loop) return;
    /* Close the resume async handle before closing the loop. */
    uv_close((uv_handle_t *)&loop->resume_async, on_resume_async_close);
    /* Run the loop once more so the close callback fires. */
    uv_run(&loop->uv, UV_RUN_DEFAULT);
    /* Free any remaining resume entries. */
    while (loop->resume_queue) {
        cetcd_resume_entry_ *e = loop->resume_queue;
        loop->resume_queue = e->next;
        free(e);
    }
    uv_loop_close(&loop->uv);
    if (loop->co_sched) {
        coroutine_close(loop->co_sched);
        loop->co_sched = NULL;
    }
    free(loop);
}

/* ── Schedule coroutine resumption from a libuv callback ──────────────── */

void cetcd_loop_schedule_resume(cetcd_loop *loop, cetcd_co *co) {
    if (!loop || !co) return;
    cetcd_resume_entry_ *e = (cetcd_resume_entry_ *)malloc(sizeof(*e));
    if (!e) return;
    e->co   = co;
    e->next = loop->resume_queue;
    loop->resume_queue = e;
    uv_async_send(&loop->resume_async);
}
