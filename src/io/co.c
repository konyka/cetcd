/*
 * Coroutine support backed by libco (ucontext-based stackful coroutines).
 *
 * cetcd_co_spawn creates a coroutine and immediately resumes it.
 * If the function does not call cetcd_co_yield, it runs to completion
 * (matching the previous stub behaviour).  If it does yield, control
 * returns to the caller of spawn, who can later call cetcd_co_resume.
 *
 * NOTE: ASan does not fully support makecontext/swapcontext.  Yield/resume
 * tests may need to run without sanitizers.  Basic spawn (no yield) works
 * correctly under ASan.
 */
#include <stdlib.h>
#include "cetcd/io.h"
#include "io_internal.h"

/* Persistent context stored on the heap so it survives across yield/resume. */
typedef struct cetcd_co {
    struct schedule *sched;
    int id;
    cetcd_co_fn fn;
    void *arg;
} cetcd_co;

/* Adapter: libco passes (schedule*, ud), we forward to cetcd_co_fn(ud) */
static void co_entry_(struct schedule *S, void *ud) {
    (void)S;
    cetcd_co *co = (cetcd_co *)ud;
    co->fn(co->arg);
}

cetcd_co *cetcd_co_spawn(cetcd_loop *loop, cetcd_co_fn fn, void *arg) {
    if (!loop || !fn) return NULL;
    struct schedule *S = cetcd_loop_sched(loop);
    if (!S) return NULL;

    cetcd_co *co = (cetcd_co *)calloc(1, sizeof(*co));
    if (!co) return NULL;
    co->fn = fn;
    co->arg = arg;

    int id = coroutine_new(S, co_entry_, co);
    if (id < 0) {
        free(co);
        return NULL;
    }
    co->sched = S;
    co->id = id;

    /* Run the coroutine immediately.  If it yields, it suspends here. */
    coroutine_resume(S, id);

    return co;
}

void cetcd_co_yield(cetcd_loop *loop) {
    if (!loop) return;
    struct schedule *S = cetcd_loop_sched(loop);
    if (!S) return;
    if (coroutine_running(S) >= 0) {
        coroutine_yield(S);
    }
}

void cetcd_co_resume(cetcd_co *co) {
    if (!co || !co->sched) return;
    if (coroutine_status(co->sched, co->id) != COROUTINE_DEAD) {
        coroutine_resume(co->sched, co->id);
    }
}
