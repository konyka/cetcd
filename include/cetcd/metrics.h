#ifndef CETCD_METRICS_H_
#define CETCD_METRICS_H_

#include <stddef.h>
#include <stdint.h>

#include "cetcd/base.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct cetcd_metrics cetcd_metrics;

cetcd_metrics *cetcd_metrics_new(void);
void           cetcd_metrics_free(cetcd_metrics *m);

void cetcd_metrics_counter(cetcd_metrics *m, const char *name, double val);
void cetcd_metrics_gauge_set(cetcd_metrics *m, const char *name, double val);
void cetcd_metrics_gauge_inc(cetcd_metrics *m, const char *name);
void cetcd_metrics_gauge_dec(cetcd_metrics *m, const char *name);
void cetcd_metrics_observe(cetcd_metrics *m, const char *name, double val);

/* Render all metrics into buf in Prometheus text exposition format.
 * Returns 0 on success, or a negative cetcd_status on failure. */
CETCD_API int cetcd_metrics_render(cetcd_metrics *m, cetcd_buf_t *buf);

/* ── pprof debug endpoints ─────────────────────────────────────────────── */

/* Render heap allocation stats from the slab allocator registry.
 * Format: plain-text table of size class, allocated, free, total. */
CETCD_API int cetcd_pprof_heap_render(cetcd_buf_t *buf);

/* Render list of active coroutines from the coroutine registry.
 * Format: plain-text table of ID, state, function name. */
CETCD_API int cetcd_pprof_coroutines_render(cetcd_buf_t *buf);

/* Collect a CPU profile for the given number of seconds (default 5).
 * Uses a sampling timer at 10ms intervals. Captures call stacks using
 * platform-specific APIs (backtrace on Linux, CaptureStackBackTrace on Windows).
 * Returns folded-stack text format suitable for flamegraph.pl.
 * This function blocks the caller for the duration of profiling. */
CETCD_API int cetcd_pprof_profile_render(cetcd_buf_t *buf, int seconds);

#ifdef __cplusplus
}
#endif
#endif
