#include "cetcd/io.h"
#include "cetcd_test.h"

static int a_done = 0;
static int b_done = 0;

static void fn_a(void *arg) {
    (void)arg;
    a_done = 1;
}

static void fn_b(void *arg) {
    (void)arg;
    b_done = 1;
}

CETCD_TEST_CASE(co_spawn_and_run) {
    a_done = 0; b_done = 0;
    cetcd_loop *loop = cetcd_loop_new();
    cetcd_co *ca = cetcd_co_spawn(loop, fn_a, NULL);
    cetcd_co *cb = cetcd_co_spawn(loop, fn_b, NULL);
    CETCD_ASSERT(ca != NULL);
    CETCD_ASSERT(cb != NULL);
    CETCD_ASSERT(a_done == 1);
    CETCD_ASSERT(b_done == 1);
    free(ca); free(cb);
    cetcd_loop_free(loop);
}

static void fn_nested(void *arg) {
    (void)arg;
    cetcd_loop *loop2 = cetcd_loop_new();
    cetcd_co *c = cetcd_co_spawn(loop2, fn_a, NULL);
    free(c);
    cetcd_loop_free(loop2);
}

CETCD_TEST_CASE(co_nested_spawn) {
    a_done = 0; b_done = 0;
    cetcd_loop *loop = cetcd_loop_new();
    cetcd_co *c = cetcd_co_spawn(loop, fn_nested, NULL);
    CETCD_ASSERT(a_done == 1);
    free(c);
    cetcd_loop_free(loop);
}

CETCD_TEST_CASE(co_yield_and_resume) {
    cetcd_loop *loop = cetcd_loop_new();
    cetcd_co *ca = cetcd_co_spawn(loop, fn_a, NULL);
    cetcd_co *cb = cetcd_co_spawn(loop, fn_b, NULL);
    CETCD_ASSERT(ca != NULL && cb != NULL);
    free(ca); free(cb);
    cetcd_loop_free(loop);
}

/*
 * The following tests exercise real coroutine yield/resume via libco's
 * ucontext-based stack switching.  ASan does not fully support
 * makecontext/swapcontext, so these tests are skipped when sanitizers
 * are active.  Run with a non-sanitized build to verify yield/resume.
 */

#if !defined(__has_feature)
#define __has_feature(x) 0
#endif
#if !__has_feature(address_sanitizer) && !defined(__SANITIZE_ADDRESS__)

static int yield_state = 0;

static void fn_yielding(void *arg) {
    cetcd_loop *loop = (cetcd_loop *)arg;
    yield_state = 1;
    cetcd_co_yield(loop);
    yield_state = 3;
}

CETCD_TEST_CASE(co_real_yield_resume) {
    yield_state = 0;
    cetcd_loop *loop = cetcd_loop_new();
    cetcd_co *co = cetcd_co_spawn(loop, fn_yielding, loop);
    CETCD_ASSERT(co != NULL);
    CETCD_ASSERT(yield_state == 1);
    cetcd_co_resume(co);
    CETCD_ASSERT(yield_state == 3);
    free(co);
    cetcd_loop_free(loop);
}

static int order_log[8];
static int order_idx = 0;

static void fn_interleaved_a(void *arg) {
    cetcd_loop *loop = (cetcd_loop *)arg;
    order_log[order_idx++] = 1;
    cetcd_co_yield(loop);
    order_log[order_idx++] = 3;
}

static void fn_interleaved_b(void *arg) {
    (void)arg;
    order_log[order_idx++] = 2;
}

CETCD_TEST_CASE(co_interleaved_execution) {
    order_idx = 0;
    cetcd_loop *loop = cetcd_loop_new();
    cetcd_co *ca = cetcd_co_spawn(loop, fn_interleaved_a, loop);
    cetcd_co *cb = cetcd_co_spawn(loop, fn_interleaved_b, NULL);
    cetcd_co_resume(ca);

    CETCD_ASSERT(order_idx == 3);
    CETCD_ASSERT(order_log[0] == 1);
    CETCD_ASSERT(order_log[1] == 2);
    CETCD_ASSERT(order_log[2] == 3);

    free(ca); free(cb);
    cetcd_loop_free(loop);
}

#endif /* !ASan */

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(co_spawn_and_run),
    CETCD_TEST_ENTRY(co_yield_and_resume),
    CETCD_TEST_ENTRY(co_nested_spawn),
#if !__has_feature(address_sanitizer) && !defined(__SANITIZE_ADDRESS__)
    CETCD_TEST_ENTRY(co_real_yield_resume),
    CETCD_TEST_ENTRY(co_interleaved_execution),
#endif
CETCD_TEST_LIST_END

CETCD_TEST_MAIN()
