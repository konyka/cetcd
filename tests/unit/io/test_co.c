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

CETCD_TEST_CASE(co_current_outside_coroutine) {
    /* Outside any coroutine, cetcd_co_current() should return NULL */
    CETCD_ASSERT_EQ_PTR(cetcd_co_current(), NULL);
}

#if !defined(__has_feature)
#define __has_feature(x) 0
#endif
#if !__has_feature(address_sanitizer) && !defined(__SANITIZE_ADDRESS__)

static int cr_state = 0;

static void fn_create_resume(void *arg) {
    (void)arg;
    cr_state = 42;
}

CETCD_TEST_CASE(co_create_and_resume) {
    cr_state = 0;
    cetcd_loop *loop = cetcd_loop_new();
    cetcd_co *co = cetcd_co_create(loop, fn_create_resume, NULL,
                                   CETCD_CO_DEFAULT_STACK_SIZE);
    CETCD_ASSERT(co != NULL);
    CETCD_ASSERT_EQ_INT(cr_state, 0);       /* not yet executed */
    cetcd_co_resume(co);
    CETCD_ASSERT_EQ_INT(cr_state, 42);      /* now it has run */
    CETCD_ASSERT_EQ_INT(cetcd_co_dead(co), 1);
    free(co);
    cetcd_loop_free(loop);
}

static int yr_state = 0;

static void fn_yield_cycle(void *arg) {
    (void)arg;
    yr_state = 1;
    cetcd_co_yield();
    yr_state = 2;
    cetcd_co_yield();
    yr_state = 3;
    cetcd_co_yield();
    yr_state = 4;
}

CETCD_TEST_CASE(co_yield_and_resume_cycle) {
    yr_state = 0;
    cetcd_loop *loop = cetcd_loop_new();
    cetcd_co *co = cetcd_co_create(loop, fn_yield_cycle, NULL,
                                   CETCD_CO_DEFAULT_STACK_SIZE);
    CETCD_ASSERT(co != NULL);
    cetcd_co_resume(co);
    CETCD_ASSERT_EQ_INT(yr_state, 1);
    cetcd_co_resume(co);
    CETCD_ASSERT_EQ_INT(yr_state, 2);
    cetcd_co_resume(co);
    CETCD_ASSERT_EQ_INT(yr_state, 3);
    cetcd_co_resume(co);
    CETCD_ASSERT_EQ_INT(yr_state, 4);
    CETCD_ASSERT_EQ_INT(cetcd_co_dead(co), 1);
    free(co);
    cetcd_loop_free(loop);
}

static cetcd_co *cur_co = NULL;

static void fn_capture_current_explicit(void *arg) {
    (void)arg;
    cur_co = cetcd_co_current();
}

CETCD_TEST_CASE(co_current) {
    cur_co = NULL;
    cetcd_loop *loop = cetcd_loop_new();
    cetcd_co *co = cetcd_co_create(loop, fn_capture_current_explicit, NULL,
                                   CETCD_CO_DEFAULT_STACK_SIZE);
    CETCD_ASSERT(co != NULL);
    cetcd_co_resume(co);
    CETCD_ASSERT_EQ_PTR(cur_co, co);
    CETCD_ASSERT_EQ_INT(cetcd_co_dead(co), 1);
    free(co);
    cetcd_loop_free(loop);
}

static int multi_log[6];
static int multi_idx = 0;

static void fn_multi_a(void *arg) {
    (void)arg;
    multi_log[multi_idx++] = 10;
    cetcd_co_yield();
    multi_log[multi_idx++] = 11;
}

static void fn_multi_b(void *arg) {
    (void)arg;
    multi_log[multi_idx++] = 20;
    cetcd_co_yield();
    multi_log[multi_idx++] = 21;
}

static void fn_multi_c(void *arg) {
    (void)arg;
    multi_log[multi_idx++] = 30;
}

CETCD_TEST_CASE(co_multiple_coroutines) {
    multi_idx = 0;
    memset(multi_log, 0, sizeof(multi_log));
    cetcd_loop *loop = cetcd_loop_new();

    /* Create three coroutines but do not start them */
    cetcd_co *ca = cetcd_co_create(loop, fn_multi_a, NULL,
                                    CETCD_CO_DEFAULT_STACK_SIZE);
    cetcd_co *cb = cetcd_co_create(loop, fn_multi_b, NULL,
                                    CETCD_CO_DEFAULT_STACK_SIZE);
    cetcd_co *cc = cetcd_co_create(loop, fn_multi_c, NULL,
                                    CETCD_CO_DEFAULT_STACK_SIZE);
    CETCD_ASSERT(ca != NULL && cb != NULL && cc != NULL);

    /* Resume in order c, a, b — each should run independently */
    cetcd_co_resume(cc);                   /* c runs to completion */
    CETCD_ASSERT_EQ_INT(multi_idx, 1);
    CETCD_ASSERT_EQ_INT(multi_log[0], 30);

    cetcd_co_resume(ca);                   /* a yields after first marker */
    CETCD_ASSERT_EQ_INT(multi_idx, 2);
    CETCD_ASSERT_EQ_INT(multi_log[1], 10);

    cetcd_co_resume(cb);                   /* b yields after first marker */
    CETCD_ASSERT_EQ_INT(multi_idx, 3);
    CETCD_ASSERT_EQ_INT(multi_log[2], 20);

    /* Resume a and b again — they continue from yield point */
    cetcd_co_resume(ca);
    CETCD_ASSERT_EQ_INT(multi_idx, 4);
    CETCD_ASSERT_EQ_INT(multi_log[3], 11);

    cetcd_co_resume(cb);
    CETCD_ASSERT_EQ_INT(multi_idx, 5);
    CETCD_ASSERT_EQ_INT(multi_log[4], 21);

    CETCD_ASSERT_EQ_INT(cetcd_co_dead(ca), 1);
    CETCD_ASSERT_EQ_INT(cetcd_co_dead(cb), 1);
    CETCD_ASSERT_EQ_INT(cetcd_co_dead(cc), 1);

    free(ca); free(cb); free(cc);
    cetcd_loop_free(loop);
}

static int yield_state = 0;
static cetcd_co *captured = NULL;

static void fn_yielding(void *arg) {
    (void)arg;
    yield_state = 1;
    cetcd_co_yield();
    yield_state = 3;
}

CETCD_TEST_CASE(co_real_yield_resume) {
    yield_state = 0;
    cetcd_loop *loop = cetcd_loop_new();
    cetcd_co *co = cetcd_co_spawn(loop, fn_yielding, NULL);
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
    (void)arg;
    order_log[order_idx++] = 1;
    cetcd_co_yield();
    order_log[order_idx++] = 3;
}

static void fn_interleaved_b(void *arg) {
    (void)arg;
    order_log[order_idx++] = 2;
}

CETCD_TEST_CASE(co_interleaved_execution) {
    order_idx = 0;
    cetcd_loop *loop = cetcd_loop_new();
    cetcd_co *ca = cetcd_co_spawn(loop, fn_interleaved_a, NULL);
    cetcd_co *cb = cetcd_co_spawn(loop, fn_interleaved_b, NULL);
    cetcd_co_resume(ca);

    CETCD_ASSERT(order_idx == 3);
    CETCD_ASSERT(order_log[0] == 1);
    CETCD_ASSERT(order_log[1] == 2);
    CETCD_ASSERT(order_log[2] == 3);

    free(ca); free(cb);
    cetcd_loop_free(loop);
}

CETCD_TEST_CASE(co_create_without_resume) {
    cetcd_loop *loop = cetcd_loop_new();
    cetcd_co *co = cetcd_co_create(loop, fn_a, NULL, CETCD_CO_DEFAULT_STACK_SIZE);
    CETCD_ASSERT(co != NULL);
    a_done = 0;
    CETCD_ASSERT(a_done == 0);
    cetcd_co_resume(co);
    CETCD_ASSERT(a_done == 1);
    free(co);
    cetcd_loop_free(loop);
}

static void fn_capture_current(void *arg) {
    cetcd_co **out = (cetcd_co **)arg;
    *out = cetcd_co_current();
}

CETCD_TEST_CASE(co_current_inside_coroutine) {
    captured = NULL;
    cetcd_loop *loop = cetcd_loop_new();
    cetcd_co *co = cetcd_co_create(loop, fn_capture_current,
                                    (void *)&captured,
                                    CETCD_CO_DEFAULT_STACK_SIZE);
    CETCD_ASSERT(co != NULL);
    cetcd_co_resume(co);
    CETCD_ASSERT(captured == co);
    free(co);
    cetcd_loop_free(loop);
}

#endif /* !ASan */

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(co_spawn_and_run),
    CETCD_TEST_ENTRY(co_yield_and_resume),
    CETCD_TEST_ENTRY(co_nested_spawn),
    CETCD_TEST_ENTRY(co_current_outside_coroutine),
#if !__has_feature(address_sanitizer) && !defined(__SANITIZE_ADDRESS__)
    CETCD_TEST_ENTRY(co_create_and_resume),
    CETCD_TEST_ENTRY(co_yield_and_resume_cycle),
    CETCD_TEST_ENTRY(co_current),
    CETCD_TEST_ENTRY(co_multiple_coroutines),
    CETCD_TEST_ENTRY(co_real_yield_resume),
    CETCD_TEST_ENTRY(co_interleaved_execution),
    CETCD_TEST_ENTRY(co_create_without_resume),
    CETCD_TEST_ENTRY(co_current_inside_coroutine),
#endif
CETCD_TEST_LIST_END

CETCD_TEST_MAIN()
