/*
 * cetcd_test.h — minimal, dependency-free unit-test harness for cetcd.
 *
 * Why not Unity/Cmocka/Criterion right away? Phase 0 must be reproducible with
 * zero network access. This harness is ~200 LOC, ANSI C, and passes the
 * project's compile flags without exceptions. Phase 4+ may swap in Unity/CMock
 * if/when mocking becomes useful.
 *
 * Design:
 *   - Each test file declares tests with CETCD_TEST(name) { ... } macros.
 *   - The harness collects them via a constructor or an explicit registration
 *     macro list and runs them sequentially in main(), reporting via stderr.
 *
 * License: Apache-2.0 (this file is part of cetcd).
 */
#ifndef CETCD_TEST_H_
#define CETCD_TEST_H_

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef void (*cetcd_test_fn)(void);

typedef struct cetcd_test_entry {
    const char    *name;
    const char    *file;
    int            line;
    cetcd_test_fn  fn;
} cetcd_test_entry;

/*
 * Public API used by test source files.
 * - CETCD_TEST_LIST(...) registers an array of tests for a binary.
 * - CETCD_TEST_MAIN() generates the main entry point.
 * - CETCD_TEST_CASE(name) declares a static test function.
 */
#define CETCD_TEST_CASE(name) static void cetcd_test__##name(void)

/*
 * Failure infrastructure. Tests fail by calling cetcd_test_fail; we use
 * setjmp/longjmp to abort the current test cleanly without aborting the
 * process so subsequent tests still run.
 */
void cetcd_test_fail(const char *file, int line, const char *fmt, ...);
int  cetcd_test_run_all(const cetcd_test_entry *entries, size_t count);

/* Assertions. All accept an optional trailing message arg list via fprintf. */
#define CETCD_ASSERT(cond)                                                     \
    do {                                                                       \
        if (!(cond)) {                                                         \
            cetcd_test_fail(__FILE__, __LINE__, "CETCD_ASSERT(%s) failed",     \
                            #cond);                                            \
        }                                                                      \
    } while (0)

#define CETCD_ASSERT_TRUE(cond)  CETCD_ASSERT(cond)
#define CETCD_ASSERT_FALSE(cond) CETCD_ASSERT(!(cond))

#define CETCD_ASSERT_NULL(p)                                                   \
    do {                                                                       \
        const void *_p = (p);                                                  \
        if (_p != NULL) {                                                      \
            cetcd_test_fail(__FILE__, __LINE__,                                \
                "expected NULL, got %p (%s)", _p, #p);                         \
        }                                                                      \
    } while (0)

#define CETCD_ASSERT_NOT_NULL(p)                                               \
    do {                                                                       \
        const void *_p = (p);                                                  \
        if (_p == NULL) {                                                      \
            cetcd_test_fail(__FILE__, __LINE__,                                \
                "expected non-NULL (%s)", #p);                                 \
        }                                                                      \
    } while (0)

#define CETCD_ASSERT_EQ_INT(a, b)                                              \
    do {                                                                       \
        long long _a = (long long)(a);                                         \
        long long _b = (long long)(b);                                         \
        if (_a != _b) {                                                        \
            cetcd_test_fail(__FILE__, __LINE__,                                \
                "expected %s == %s, got %lld != %lld", #a, #b, _a, _b);        \
        }                                                                      \
    } while (0)

#define CETCD_ASSERT_NE_INT(a, b)                                              \
    do {                                                                       \
        long long _a = (long long)(a);                                         \
        long long _b = (long long)(b);                                         \
        if (_a == _b) {                                                        \
            cetcd_test_fail(__FILE__, __LINE__,                                \
                "expected %s != %s, both %lld", #a, #b, _a);                   \
        }                                                                      \
    } while (0)

#define CETCD_ASSERT_EQ_UINT(a, b)                                             \
    do {                                                                       \
        unsigned long long _a = (unsigned long long)(a);                       \
        unsigned long long _b = (unsigned long long)(b);                       \
        if (_a != _b) {                                                        \
            cetcd_test_fail(__FILE__, __LINE__,                                \
                "expected %s == %s, got %llu != %llu", #a, #b, _a, _b);        \
        }                                                                      \
    } while (0)

#define CETCD_ASSERT_EQ_PTR(a, b)                                              \
    do {                                                                       \
        const void *_a = (a);                                                  \
        const void *_b = (b);                                                  \
        if (_a != _b) {                                                        \
            cetcd_test_fail(__FILE__, __LINE__,                                \
                "expected %s == %s, got %p != %p", #a, #b, _a, _b);            \
        }                                                                      \
    } while (0)

#define CETCD_ASSERT_EQ_STR(a, b)                                              \
    do {                                                                       \
        const char *_a = (a);                                                  \
        const char *_b = (b);                                                  \
        if (_a == NULL || _b == NULL || strcmp(_a, _b) != 0) {                 \
            cetcd_test_fail(__FILE__, __LINE__,                                \
                "expected %s == %s, got \"%s\" != \"%s\"", #a, #b,             \
                _a ? _a : "(null)", _b ? _b : "(null)");                       \
        }                                                                      \
    } while (0)

#define CETCD_ASSERT_EQ_MEM(a, b, n)                                           \
    do {                                                                       \
        const void *_a = (a);                                                  \
        const void *_b = (b);                                                  \
        size_t _n = (size_t)(n);                                               \
        if (memcmp(_a, _b, _n) != 0) {                                         \
            cetcd_test_fail(__FILE__, __LINE__,                                \
                "memory mismatch: %s vs %s over %zu bytes", #a, #b, _n);       \
        }                                                                      \
    } while (0)

/*
 * Table-of-tests + main entry point. Usage:
 *
 *   CETCD_TEST_CASE(my_test) {
 *       CETCD_ASSERT_EQ_INT(1 + 1, 2);
 *   }
 *
 *   CETCD_TEST_LIST_BEGIN
 *       CETCD_TEST_ENTRY(my_test),
 *   CETCD_TEST_LIST_END
 *   CETCD_TEST_MAIN()
 */
#define CETCD_TEST_LIST_BEGIN                                                  \
    static const cetcd_test_entry cetcd_test_entries_[] = {
#define CETCD_TEST_LIST_END                                                    \
    };
#define CETCD_TEST_ENTRY(name)                                                 \
    { #name, __FILE__, __LINE__, cetcd_test__##name }

#define CETCD_TEST_MAIN()                                                      \
    int main(int argc, char **argv) {                                          \
        (void)argc; (void)argv;                                                \
        return cetcd_test_run_all(cetcd_test_entries_,                         \
            sizeof(cetcd_test_entries_) / sizeof(cetcd_test_entries_[0]));     \
    }

#ifdef __cplusplus
}
#endif
#endif /* CETCD_TEST_H_ */
