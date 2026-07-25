#include <stdlib.h>
#include <string.h>
#include <uv.h>
#include "cetcd/io.h"
#include "io_internal.h"

/* ── Resume-queue async callback ───────────────────────────────────────── */
static void on_resume_async_cb(uv_async_t *handle) {
    cetcd_loop *loop = (cetcd_loop *)handle->data;
    if (!loop) return;
    /* Splice the queue into a local list under the lock, then resume each
     * coroutine OUTSIDE the lock so a coroutine body that itself schedules
     * work cannot self-deadlock. */
    cetcd_resume_entry_ *list;
    uv_mutex_lock(&loop->resume_mutex);
    list = loop->resume_queue;
    loop->resume_queue = NULL;
    uv_mutex_unlock(&loop->resume_mutex);

    while (list) {
        cetcd_resume_entry_ *e = list;
        list = e->next;
        cetcd_co *co = e->co;
        free(e);
        if (!cetcd_co_dead(co)) {
            cetcd_co_resume(co);
        }
        cetcd_co_queue_release_(co);
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
    uv_mutex_init(&l->resume_mutex);
    l->resume_closing = 0;
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
    /* Bar new producers from enqueueing before we tear the loop down. */
    uv_mutex_lock(&loop->resume_mutex);
    loop->resume_closing = 1;
    uv_mutex_unlock(&loop->resume_mutex);

    /* Close the resume async handle before closing the loop. */
    uv_close((uv_handle_t *)&loop->resume_async, on_resume_async_close);
    /* Run the loop once more so the close callback (and any final drain) fires. */
    uv_run(&loop->uv, UV_RUN_DEFAULT);
    /* Release any entries still queued (producers that raced the closing flag). */
    cetcd_resume_entry_ *e = loop->resume_queue;
    while (e) {
        cetcd_resume_entry_ *next = e->next;
        cetcd_co_queue_release_(e->co);
        free(e);
        e = next;
    }
    loop->resume_queue = NULL;
    uv_mutex_destroy(&loop->resume_mutex);
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
    e->co = co;
    e->next = NULL;

    uv_mutex_lock(&loop->resume_mutex);
    if (loop->resume_closing) {
        uv_mutex_unlock(&loop->resume_mutex);
        free(e);
        return;
    }
    cetcd_co_queue_retain_(co);
    e->next = loop->resume_queue;
    loop->resume_queue = e;
    uv_mutex_unlock(&loop->resume_mutex);

    uv_async_send(&loop->resume_async);
}
