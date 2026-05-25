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

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(co_spawn_and_run),
    CETCD_TEST_ENTRY(co_yield_and_resume),
    CETCD_TEST_ENTRY(co_nested_spawn),
CETCD_TEST_LIST_END

CETCD_TEST_MAIN()
