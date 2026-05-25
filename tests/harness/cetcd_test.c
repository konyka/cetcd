/*
 * cetcd_test.c — implementation of the minimal test harness.
 *
 * License: Apache-2.0.
 */
#if !defined(_WIN32) && !defined(_POSIX_C_SOURCE)
#  define _POSIX_C_SOURCE 200809L
#endif

#include "cetcd_test.h"

#include <setjmp.h>
#include <stdarg.h>
#include <time.h>

static jmp_buf      cetcd_test_jmp_;
static int          cetcd_test_in_test_  = 0;
static const char  *cetcd_test_cur_name_ = NULL;

void cetcd_test_fail(const char *file, int line, const char *fmt, ...) {
    fprintf(stderr, "  FAIL [%s] %s:%d: ",
            cetcd_test_cur_name_ ? cetcd_test_cur_name_ : "?",
            file, line);

    va_list ap;
    va_start(ap, fmt);
    vfprintf(stderr, fmt, ap);
    va_end(ap);

    fputc('\n', stderr);
    fflush(stderr);

    if (cetcd_test_in_test_) {
        longjmp(cetcd_test_jmp_, 1);
    } else {
        /* Failure outside a test scope — fatal. */
        exit(2);
    }
}

static double cetcd_test_now_ms_(void) {
#if defined(CLOCK_MONOTONIC)
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        return 0.0;
    }
    return (double)ts.tv_sec * 1000.0 + (double)ts.tv_nsec / 1.0e6;
#else
    return (double)clock() * 1000.0 / (double)CLOCKS_PER_SEC;
#endif
}

int cetcd_test_run_all(const cetcd_test_entry *entries, size_t count) {
    size_t passed = 0;
    size_t failed = 0;

    fprintf(stderr, "[cetcd-test] running %zu test(s)\n", count);

    for (size_t i = 0; i < count; ++i) {
        cetcd_test_cur_name_ = entries[i].name;
        cetcd_test_in_test_  = 1;

        double t0 = cetcd_test_now_ms_();

        if (setjmp(cetcd_test_jmp_) == 0) {
            entries[i].fn();
            double t1 = cetcd_test_now_ms_();
            fprintf(stderr, "  PASS %-40s (%.2f ms)\n",
                    entries[i].name, t1 - t0);
            ++passed;
        } else {
            ++failed;
        }

        cetcd_test_in_test_  = 0;
        cetcd_test_cur_name_ = NULL;
    }

    fprintf(stderr, "[cetcd-test] %zu passed, %zu failed\n", passed, failed);
    return failed == 0 ? 0 : 1;
}
