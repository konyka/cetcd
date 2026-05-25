/* Lightweight timer stub using immediate invocation for Phase 0 tests. */
#include <stdlib.h>

#include "cetcd/io.h"

typedef struct cetcd_timer {
    cetcd_co_fn cb;
    void *arg;
} cetcd_timer;

cetcd_timer *cetcd_timer_new(cetcd_loop *loop) {
    (void)loop;
    cetcd_timer *t = (cetcd_timer *)malloc(sizeof(*t));
    if (t) t->cb = NULL, t->arg = NULL;
    return t;
}

void cetcd_timer_free(cetcd_timer *timer) {
    if (timer) free(timer);
}

void cetcd_timer_start(cetcd_timer *timer, uint64_t timeout_ms, uint64_t repeat_ms,
                      cetcd_co_fn cb, void *arg) {
    (void)timeout_ms; (void)repeat_ms;
    timer->cb = cb;
    timer->arg = arg;
    /* Immediately invoke to simulate timer firing in tests */
    if (cb) cb(arg);
}

void cetcd_timer_stop(cetcd_timer *timer) {
    (void)timer;
}
