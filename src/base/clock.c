#if !defined(_WIN32) && !defined(_POSIX_C_SOURCE)
#  define _POSIX_C_SOURCE 200809L
#endif

#include "cetcd/base.h"

#if defined(_WIN32)
#  define WIN32_LEAN_AND_MEAN
#  include <windows.h>
#else
#  include <time.h>
#endif

static const cetcd_clock_vtable *g_clock_vt = NULL;

static uint64_t platform_monotonic_ns(void) {
#if defined(_WIN32)
    static LARGE_INTEGER freq = {0};
    if (freq.QuadPart == 0) QueryPerformanceFrequency(&freq);
    LARGE_INTEGER now;
    QueryPerformanceCounter(&now);
    return (uint64_t)((now.QuadPart * 1000000000ull) / (uint64_t)freq.QuadPart);
#else
    struct timespec ts;
#  if defined(CLOCK_MONOTONIC)
    clock_gettime(CLOCK_MONOTONIC, &ts);
#  else
    clock_gettime(CLOCK_REALTIME, &ts);
#  endif
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
#endif
}

static uint64_t platform_realtime_ns(void) {
#if defined(_WIN32)
    FILETIME ft;
    GetSystemTimeAsFileTime(&ft);
    uint64_t t = ((uint64_t)ft.dwHighDateTime << 32) | ft.dwLowDateTime;
    /* Windows FILETIME: 100-ns ticks since 1601-01-01.
       Convert to nanoseconds since Unix epoch (1970-01-01).
       Delta = 11644473600 seconds. */
    t -= 116444736000000000ull;
    return t * 100ull;
#else
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
#endif
}

uint64_t cetcd_clock_monotonic_ns(void) {
    if (g_clock_vt && g_clock_vt->monotonic_ns) {
        return g_clock_vt->monotonic_ns(g_clock_vt->self);
    }
    return platform_monotonic_ns();
}

uint64_t cetcd_clock_realtime_ns(void) {
    if (g_clock_vt && g_clock_vt->realtime_ns) {
        return g_clock_vt->realtime_ns(g_clock_vt->self);
    }
    return platform_realtime_ns();
}

void cetcd_clock_set_global(const cetcd_clock_vtable *vt) {
    g_clock_vt = vt;
}

const cetcd_clock_vtable *cetcd_clock_global(void) {
    return g_clock_vt;
}
