#include <stdio.h>
#include <stdlib.h>

#include "cetcd/io.h"
#include "cetcd_test.h"

CETCD_TEST_CASE(loop_can_be_created_and_freed) {
    cetcd_loop *loop = cetcd_loop_new();
    CETCD_ASSERT(loop != NULL);
    cetcd_loop_free(loop);
}

/* resume_async is always active, so cetcd_loop_run() (UV_RUN_DEFAULT) would
 * block forever unless the stop is requested from inside the loop. */
struct stop_ctx {
    cetcd_loop *loop;
    int         stopped;
};

static void stop_cb(void *arg) {
    struct stop_ctx *ctx = (struct stop_ctx *)arg;
    ctx->stopped = 1;
    cetcd_loop_stop(ctx->loop);
}

CETCD_TEST_CASE(loop_run_and_stop) {
    cetcd_loop *loop = cetcd_loop_new();
    CETCD_ASSERT(loop != NULL);

    struct stop_ctx ctx = { loop, 0 };
    cetcd_timer *tm = cetcd_timer_new(loop);
    cetcd_timer_start(tm, 5, 0, stop_cb, &ctx);

    int r = cetcd_loop_run(loop);

    CETCD_ASSERT_EQ_INT(ctx.stopped, 1);
    CETCD_ASSERT(r >= 0);

    cetcd_timer_free(tm);
    cetcd_loop_free(loop);
}

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(loop_can_be_created_and_freed),
    CETCD_TEST_ENTRY(loop_run_and_stop),
CETCD_TEST_LIST_END

CETCD_TEST_MAIN()
