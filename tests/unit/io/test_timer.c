#include "cetcd/io.h"
#include "cetcd_test.h"

static void timer_cb(void *arg) {
    if (!arg) return;
    int *flag = (int *)arg;
    *flag = 1;
}

CETCD_TEST_CASE(timer_fires_after_timeout) {
    cetcd_loop *loop = cetcd_loop_new();
    CETCD_ASSERT(loop != NULL);
    cetcd_timer *tm = cetcd_timer_new(loop);
    int fired = 0;
    cetcd_timer_start(tm, 0, 0, timer_cb, &fired);
    CETCD_ASSERT(fired == 1);
    cetcd_timer_free(tm);
    cetcd_loop_free(loop);
}

CETCD_TEST_CASE(timer_can_be_stopped) {
    cetcd_loop *loop = cetcd_loop_new();
    cetcd_timer *tm = cetcd_timer_new(loop);
    cetcd_timer_start(tm, 10, 0, timer_cb, NULL);
    cetcd_timer_stop(tm);
    cetcd_timer_free(tm);
    cetcd_loop_free(loop);
}

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(timer_fires_after_timeout),
    CETCD_TEST_ENTRY(timer_can_be_stopped),
CETCD_TEST_LIST_END

CETCD_TEST_MAIN()
