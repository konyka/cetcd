#include <stdio.h>
#include <stdlib.h>

#include "cetcd/io.h"
#include "cetcd_test.h"

CETCD_TEST_CASE(loop_can_be_created_and_freed) {
    cetcd_loop *loop = cetcd_loop_new();
    CETCD_ASSERT(loop != NULL);
    cetcd_loop_free(loop);
}

CETCD_TEST_CASE(loop_run_and_stop) {
    cetcd_loop *loop = cetcd_loop_new();
    CETCD_ASSERT(loop != NULL);
    int r = cetcd_loop_run(loop);
    CETCD_ASSERT(r == 0);
    cetcd_loop_stop(loop);
    cetcd_loop_free(loop);
}

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(loop_can_be_created_and_freed),
    CETCD_TEST_ENTRY(loop_run_and_stop),
CETCD_TEST_LIST_END

CETCD_TEST_MAIN()
