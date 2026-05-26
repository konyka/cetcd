#ifndef CETCD_BENCH_H_
#define CETCD_BENCH_H_

#include <stdint.h>
#include <stdio.h>

#include "cetcd/clock.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    const char *name;
    uint64_t    iterations;
    uint64_t    ns_elapsed;
    double      ns_per_op;
    double      ops_per_sec;
} cetcd_bench_result;

typedef void (*cetcd_bench_fn)(void *udata);

static inline uint64_t cetcd_bench_now_ns(void) {
    return cetcd_clock_monotonic_ns();
}

static inline cetcd_bench_result cetcd_bench_run(const char *name,
                                                   cetcd_bench_fn fn,
                                                   void *udata,
                                                   uint64_t iterations) {
    cetcd_bench_result r;
    r.name = name;
    r.iterations = iterations;
    uint64_t start = cetcd_bench_now_ns();
    for (uint64_t i = 0; i < iterations; i++) {
        fn(udata);
    }
    r.ns_elapsed = cetcd_bench_now_ns() - start;
    r.ns_per_op = (double)r.ns_elapsed / (double)r.iterations;
    r.ops_per_sec = (r.ns_elapsed > 0)
        ? (double)r.iterations / ((double)r.ns_elapsed / 1e9)
        : 0.0;
    return r;
}

static inline void cetcd_bench_print(const cetcd_bench_result *r) {
    printf("BENCH %-40s %8llu ops %10llu ns %8.1f ns/op %10.0f ops/s\n",
           r->name,
           (unsigned long long)r->iterations,
           (unsigned long long)r->ns_elapsed,
           r->ns_per_op,
           r->ops_per_sec);
}

#ifdef __cplusplus
}
#endif
#endif
