#ifndef CETCD_IO_INTERNAL_H_
#define CETCD_IO_INTERNAL_H_

#include <uv.h>
#include "coroutine.h"  /* libco — include path set by CMake */

/* Forward declaration — defined in cetcd/io.h as opaque typedef */
struct cetcd_tcp;
struct cetcd_co;

/* Entry in the coroutine resume queue (linked list) */
typedef struct cetcd_resume_entry_ {
    struct cetcd_co *co;
    struct cetcd_resume_entry_ *next;
} cetcd_resume_entry_;

struct cetcd_loop {
    uv_loop_t             uv;
    struct schedule      *co_sched;      /* libco coroutine scheduler */
    uv_async_t            resume_async;  /* async handle for scheduled resumes */
    uv_mutex_t            resume_mutex;  /* guards resume_queue + resume_closing
                                          * and the per-coroutine queue refcount */
    int                   resume_closing;/* set during cetcd_loop_free to bar
                                          * new producers from enqueueing */
    cetcd_resume_entry_  *resume_queue;  /* pending coroutines to resume  */
};

static inline uv_loop_t *cetcd_loop_uv(struct cetcd_loop *loop) {
    return loop ? &loop->uv : NULL;
}

static inline struct schedule *cetcd_loop_sched(struct cetcd_loop *loop) {
    return loop ? loop->co_sched : NULL;
}

/* Internal coroutine refcount helpers used by the resume queue. The queue
 * owns one reference per queued entry so a cetcd_co_free() that races the
 * drain defers the actual free until the last queued entry is released. */
struct cetcd_co;
void cetcd_co_queue_retain_(struct cetcd_co *co);   /* caller holds resume_mutex */
void cetcd_co_queue_release_(struct cetcd_co *co);  /* drops one queue ref      */

uv_stream_t *cetcd_tcp_stream(struct cetcd_tcp *tcp);

/* Get the underlying file descriptor for a tcp handle (-1 on error). */
int cetcd_tcp_fd(struct cetcd_tcp *tcp);

#endif
