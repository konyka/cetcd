#include "cetcd/base.h"
#include "cetcd_test.h"

CETCD_TEST_CASE(monotonic_is_nonzero) {
    uint64_t t = cetcd_clock_monotonic_ns();
    CETCD_ASSERT_TRUE(t > 0);
}

CETCD_TEST_CASE(monotonic_is_monotonic) {
    uint64_t a = cetcd_clock_monotonic_ns();
    uint64_t b = cetcd_clock_monotonic_ns();
    CETCD_ASSERT_TRUE(b >= a);
}

CETCD_TEST_CASE(realtime_returns_recent_unix_ns) {
    uint64_t t = cetcd_clock_realtime_ns();
    /* Sometime after 2020-01-01 UTC: 1577836800 seconds * 1e9. */
    CETCD_ASSERT_TRUE(t > 1577836800ull * 1000000000ull);
}

static uint64_t fake_mono_value = 12345;
static uint64_t fake_mono(void *self) { (void)self; return fake_mono_value; }
static uint64_t fake_real(void *self) { (void)self; return 99999; }

CETCD_TEST_CASE(injectable_clock_overrides_globals) {
    cetcd_clock_vtable vt = { fake_mono, fake_real, NULL };
    cetcd_clock_set_global(&vt);

    CETCD_ASSERT_EQ_UINT(cetcd_clock_monotonic_ns(), 12345u);
    CETCD_ASSERT_EQ_UINT(cetcd_clock_realtime_ns(),  99999u);

    fake_mono_value = 67890;
    CETCD_ASSERT_EQ_UINT(cetcd_clock_monotonic_ns(), 67890u);

    cetcd_clock_set_global(NULL);
    uint64_t after_reset = cetcd_clock_monotonic_ns();
    CETCD_ASSERT_TRUE(after_reset > 0);
    CETCD_ASSERT_NE_INT((long long)after_reset, 67890);
}

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(monotonic_is_nonzero),
    CETCD_TEST_ENTRY(monotonic_is_monotonic),
    CETCD_TEST_ENTRY(realtime_returns_recent_unix_ns),
    CETCD_TEST_ENTRY(injectable_clock_overrides_globals),
CETCD_TEST_LIST_END
CETCD_TEST_MAIN()
