#include "cetcd/v3rpc.h"
#include "cetcd_test.h"

CETCD_TEST_CASE(watch_progress_ticks_default) {
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_watch_progress_ticks_from_ms(0),
                        CETCD_WATCH_PROGRESS_TICKS_DEFAULT);
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_watch_progress_ticks_from_ms(10000), 100);
}

CETCD_TEST_CASE(watch_progress_ticks_rounds_up) {
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_watch_progress_ticks_from_ms(1), 1);
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_watch_progress_ticks_from_ms(100), 1);
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_watch_progress_ticks_from_ms(101), 2);
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_watch_progress_ticks_from_ms(500), 5);
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_watch_progress_ticks_from_ms(600000), 6000);
}

CETCD_TEST_CASE(watch_progress_ticks_uses_tick_ms) {
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_watch_progress_ticks_from_ms_tick(0, 50),
                        200);
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_watch_progress_ticks_from_ms_tick(10000, 50),
                        200);
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_watch_progress_ticks_from_ms_tick(0, 0),
                        CETCD_WATCH_PROGRESS_TICKS_DEFAULT);
}

CETCD_TEST_CASE(watch_progress_set_get) {
    cetcd_v3rpc_set_watch_progress_interval_ms(200);
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_watch_progress_ticks(), 2);
    cetcd_v3rpc_set_watch_progress_interval_ms(0);
    CETCD_ASSERT_EQ_INT(cetcd_v3rpc_watch_progress_ticks(),
                        CETCD_WATCH_PROGRESS_TICKS_DEFAULT);
}

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(watch_progress_ticks_default),
    CETCD_TEST_ENTRY(watch_progress_ticks_rounds_up),
    CETCD_TEST_ENTRY(watch_progress_ticks_uses_tick_ms),
    CETCD_TEST_ENTRY(watch_progress_set_get),
CETCD_TEST_LIST_END

CETCD_TEST_MAIN()
