#if !defined(_WIN32) && !defined(_GNU_SOURCE)
#  define _GNU_SOURCE
#endif
#include "cetcd/metrics.h"
#include "cetcd/slab.h"
#include "cetcd/io.h"
#include "cetcd/log.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>

#ifdef _WIN32
#  define WIN32_LEAN_AND_MEAN
#  include <windows.h>
#else
#  include <execinfo.h>
#  include <dlfcn.h>
#endif

typedef enum metric_kind_ {
    METRIC_COUNTER,
    METRIC_GAUGE,
    METRIC_HISTOGRAM
} metric_kind_;

typedef struct metric_entry_ {
    char          name[128];
    metric_kind_  kind;
    double        value;
    uint64_t      count;
    double        sum;
} metric_entry_;

struct cetcd_metrics {
    metric_entry_ *entries;
    size_t         count;
    size_t         cap;
};

cetcd_metrics *cetcd_metrics_new(void) {
    cetcd_metrics *m = (cetcd_metrics *)calloc(1, sizeof(*m));
    if (!m) return NULL;
    m->cap = 32;
    m->entries = (metric_entry_ *)calloc(m->cap, sizeof(metric_entry_));
    if (!m->entries) { free(m); return NULL; }
    return m;
}

void cetcd_metrics_free(cetcd_metrics *m) {
    if (!m) return;
    free(m->entries);
    free(m);
}

static metric_entry_ *find_or_add_(cetcd_metrics *m, const char *name, metric_kind_ kind) {
    for (size_t i = 0; i < m->count; i++) {
        if (strcmp(m->entries[i].name, name) == 0) return &m->entries[i];
    }
    if (m->count >= m->cap) {
        size_t new_cap = m->cap * 2;
        metric_entry_ *ne = (metric_entry_ *)realloc(m->entries, new_cap * sizeof(metric_entry_));
        if (!ne) return NULL;
        memset(ne + m->cap, 0, (new_cap - m->cap) * sizeof(metric_entry_));
        m->entries = ne;
        m->cap = new_cap;
    }
    metric_entry_ *e = &m->entries[m->count++];
    strncpy(e->name, name, sizeof(e->name) - 1);
    e->kind = kind;
    return e;
}

void cetcd_metrics_counter(cetcd_metrics *m, const char *name, double val) {
    if (!m || !name) return;
    metric_entry_ *e = find_or_add_(m, name, METRIC_COUNTER);
    if (e) e->value += val;
}

void cetcd_metrics_gauge_set(cetcd_metrics *m, const char *name, double val) {
    if (!m || !name) return;
    metric_entry_ *e = find_or_add_(m, name, METRIC_GAUGE);
    if (e) e->value = val;
}

void cetcd_metrics_gauge_inc(cetcd_metrics *m, const char *name) {
    if (!m || !name) return;
    metric_entry_ *e = find_or_add_(m, name, METRIC_GAUGE);
    if (e) e->value += 1.0;
}

void cetcd_metrics_gauge_dec(cetcd_metrics *m, const char *name) {
    if (!m || !name) return;
    metric_entry_ *e = find_or_add_(m, name, METRIC_GAUGE);
    if (e) e->value -= 1.0;
}

void cetcd_metrics_observe(cetcd_metrics *m, const char *name, double val) {
    if (!m || !name) return;
    metric_entry_ *e = find_or_add_(m, name, METRIC_HISTOGRAM);
    if (e) { e->count++; e->sum += val; }
}

static int fmt_double_(char *buf, size_t sz, double val) {
    if (val == (double)(long long)val && val < 1e15 && val > -1e15) {
        return snprintf(buf, sz, "%lld", (long long)val);
    }
    return snprintf(buf, sz, "%.2f", val);
}

static const char *kind_name_(metric_kind_ kind) {
    switch (kind) {
        case METRIC_COUNTER:   return "counter";
        case METRIC_GAUGE:     return "gauge";
        case METRIC_HISTOGRAM: return "histogram";
        default:               return "untyped";
    }
}

int cetcd_metrics_render(cetcd_metrics *m, cetcd_buf_t *buf) {
    if (!m || !buf) return CETCD_ERR_INVAL;

    for (size_t i = 0; i < m->count; i++) {
        const metric_entry_ *e = &m->entries[i];
        int rc;

        rc = cetcd_buf_printf(buf, "# HELP %s cetcd metric %s\n", e->name, e->name);
        if (rc != 0) return rc;
        rc = cetcd_buf_printf(buf, "# TYPE %s %s\n", e->name, kind_name_(e->kind));
        if (rc != 0) return rc;

        if (e->kind == METRIC_HISTOGRAM) {
            char sbuf[64];
            int sn = fmt_double_(sbuf, sizeof(sbuf), e->sum);
            rc = cetcd_buf_printf(buf, "%s_bucket{le=\"+Inf\"} %llu\n",
                                  e->name, (unsigned long long)e->count);
            if (rc != 0) return rc;
            rc = cetcd_buf_printf(buf, "%s_sum %.*s\n", e->name, sn, sbuf);
            if (rc != 0) return rc;
            rc = cetcd_buf_printf(buf, "%s_count %llu\n",
                                  e->name, (unsigned long long)e->count);
            if (rc != 0) return rc;
        } else {
            char vbuf[64];
            int vn = fmt_double_(vbuf, sizeof(vbuf), e->value);
            rc = cetcd_buf_printf(buf, "%s %.*s\n", e->name, vn, vbuf);
            if (rc != 0) return rc;
        }
    }
    return 0;
}

/* =====================================================================
 * pprof debug endpoints
 * ===================================================================== */

/* ── Heap profiling ──────────────────────────────────────────────────── */

typedef struct heap_walk_ctx_ {
    cetcd_buf_t *buf;
    size_t       total_alloc;
    size_t       peak_alloc;
} heap_walk_ctx_;

static void heap_walk_cb_(const cetcd_slab *s, void *ud) {
    heap_walk_ctx_ *ctx = (heap_walk_ctx_ *)ud;
    cetcd_slab_stats st;
    cetcd_slab_get_stats(s, &st);
    ctx->total_alloc += st.live_count * st.obj_size;
    /* Peak is approximated as total capacity (all blocks ever allocated) */
    if (st.total_capacity * st.obj_size > ctx->peak_alloc)
        ctx->peak_alloc = st.total_capacity * st.obj_size;
    cetcd_buf_printf(ctx->buf, "%-14zu %-12zu %-8zu %zu\n",
                     st.obj_size, st.live_count,
                     st.total_capacity - st.live_count,
                     st.total_capacity);
}

int cetcd_pprof_heap_render(cetcd_buf_t *buf) {
    if (!buf) return CETCD_ERR_INVAL;

    /* Single-pass walk: format rows into temp buffer, accumulate totals */
    cetcd_buf_t rows;
    cetcd_buf_init(&rows);
    heap_walk_ctx_ walk_ctx;
    memset(&walk_ctx, 0, sizeof(walk_ctx));
    walk_ctx.buf = &rows;

    cetcd_slab_walk(heap_walk_cb_, &walk_ctx);

    /* Write header with totals */
    cetcd_buf_printf(buf, "--- heap\n");
    cetcd_buf_printf(buf, "Total allocated: %zu\n", walk_ctx.total_alloc);
    cetcd_buf_printf(buf, "Peak allocated: %zu\n", walk_ctx.peak_alloc);
    cetcd_buf_printf(buf, "\n");
    cetcd_buf_printf(buf, "%-14s %-12s %-8s %s\n",
                     "Size class", "Allocated", "Free", "Total");

    /* Append the pre-formatted rows */
    if (rows.len > 0) {
        cetcd_buf_append(buf, rows.data, rows.len);
    }
    cetcd_buf_free(&rows);
    return 0;
}

/* ── Coroutine listing ───────────────────────────────────────────────── */

static const char *co_state_name_(int state) {
    switch (state) {
        case 0: /* COROUTINE_DEAD */    return "dead";
        case 1: /* COROUTINE_READY */   return "ready";
        case 2: /* COROUTINE_RUNNING */ return "running";
        case 3: /* COROUTINE_SUSPEND */ return "yielded";
        default:                        return "unknown";
    }
}

typedef struct co_render_ctx_ {
    cetcd_buf_t *buf;
    size_t       count;
} co_render_ctx_;

static void co_render_walk_cb_(const cetcd_co_info *info, void *ud) {
    co_render_ctx_ *ctx = (co_render_ctx_ *)ud;
    ctx->count++;
    const char *name = info->name ? info->name : "-";
    cetcd_buf_printf(ctx->buf, "%-6d %-11s %s\n",
                     info->id, co_state_name_(info->state), name);
}

int cetcd_pprof_coroutines_render(cetcd_buf_t *buf) {
    if (!buf) return CETCD_ERR_INVAL;

    /* Format rows into a temp buffer so we can write the count header first */
    cetcd_buf_t rows;
    cetcd_buf_init(&rows);
    co_render_ctx_ ctx = { &rows, 0 };
    cetcd_co_walk(co_render_walk_cb_, &ctx);

    cetcd_buf_printf(buf, "--- coroutines\n");
    cetcd_buf_printf(buf, "Total: %zu\n", ctx.count);
    cetcd_buf_printf(buf, "\n");
    cetcd_buf_printf(buf, "%-6s %-11s %s\n", "ID", "State", "Function");
    if (rows.len > 0) {
        cetcd_buf_append(buf, rows.data, rows.len);
    }
    cetcd_buf_free(&rows);
    return 0;
}

/* ── CPU profiling ───────────────────────────────────────────────────── */

/* Maximum number of stack frames to capture per sample. */
#define PPROF_MAX_FRAMES 32

/* Maximum number of unique stacks to track. */
#define PPROF_MAX_STACKS 4096

typedef struct pprof_stack_entry_ {
    void  *frames[PPROF_MAX_FRAMES];
    int    n_frames;
    int    count;
} pprof_stack_entry_;

typedef struct pprof_profile_ctx_ {
    pprof_stack_entry_ stacks[PPROF_MAX_STACKS];
    int                n_stacks;
    int                total_samples;
} pprof_profile_ctx_;

/* Capture current call stack. Returns number of frames captured. */
static int capture_stack_(void **frames, int max_frames) {
#ifdef _WIN32
    return CaptureStackBackTrace(1, (ULONG)max_frames, frames, NULL);
#else
    return backtrace(frames, max_frames);
#endif
}

/* Check if two stack entries are equal. */
static int stack_eq_(const pprof_stack_entry_ *a, const pprof_stack_entry_ *b) {
    if (a->n_frames != b->n_frames) return 0;
    for (int i = 0; i < a->n_frames; i++) {
        if (a->frames[i] != b->frames[i]) return 0;
    }
    return 1;
}

/* Add a sample to the profile context. */
static void profile_add_sample_(pprof_profile_ctx_ *ctx, void **frames, int n_frames) {
    pprof_stack_entry_ key;
    memset(&key, 0, sizeof(key));
    memcpy(key.frames, frames, sizeof(void *) * (size_t)n_frames);
    key.n_frames = n_frames;

    /* Search for existing matching stack */
    for (int i = 0; i < ctx->n_stacks; i++) {
        if (stack_eq_(&ctx->stacks[i], &key)) {
            ctx->stacks[i].count++;
            ctx->total_samples++;
            return;
        }
    }

    /* Add new stack entry */
    if (ctx->n_stacks < PPROF_MAX_STACKS) {
        pprof_stack_entry_ *e = &ctx->stacks[ctx->n_stacks++];
        memcpy(e->frames, frames, sizeof(void *) * (size_t)n_frames);
        e->n_frames = n_frames;
        e->count    = 1;
        ctx->total_samples++;
    }
}

/* Format a single address as a symbol name (best-effort). */
static void fmt_symbol_(cetcd_buf_t *buf, void *addr) {
#ifdef _WIN32
    /* On Windows, just format as hex address — full symbol resolution
     * requires DbgHelp which adds complexity. */
    cetcd_buf_printf(buf, "0x%p", addr);
#else
    /* On Linux, try dladdr for symbol name */
    Dl_info info;
    if (dladdr(addr, &info) && info.dli_sname) {
        cetcd_buf_printf(buf, "%s", info.dli_sname);
    } else {
        cetcd_buf_printf(buf, "0x%lx", (unsigned long)(uintptr_t)addr);
    }
#endif
}

/* Portable sleep for milliseconds. */
static void pprof_sleep_ms_(unsigned int ms) {
#ifdef _WIN32
    Sleep(ms);
#else
    struct timespec ts;
    ts.tv_sec  = (time_t)(ms / 1000);
    ts.tv_nsec = (long)((ms % 1000) * 1000000);
    nanosleep(&ts, NULL);
#endif
}

int cetcd_pprof_profile_render(cetcd_buf_t *buf, int seconds) {
    if (!buf) return CETCD_ERR_INVAL;
    if (seconds <= 0) seconds = 5;

    pprof_profile_ctx_ ctx;
    memset(&ctx, 0, sizeof(ctx));

    int total_ticks = seconds * 100;  /* 10ms per tick = 100 ticks/sec */
    CETCD_INFO("pprof: starting CPU profile for %d seconds (%d samples)",
               seconds, total_ticks);

    for (int tick = 0; tick < total_ticks; tick++) {
        void *frames[PPROF_MAX_FRAMES];
        int n = capture_stack_(frames, PPROF_MAX_FRAMES);
        if (n > 0) {
            profile_add_sample_(&ctx, frames, n);
        }
        pprof_sleep_ms_(10);
    }

    CETCD_INFO("pprof: CPU profile complete, %d samples, %d unique stacks",
               ctx.total_samples, ctx.n_stacks);

    /* Format as folded stacks (flamegraph.pl compatible) */
    cetcd_buf_printf(buf, "--- profile\n");
    cetcd_buf_printf(buf, "Total samples: %d\n", ctx.total_samples);
    cetcd_buf_printf(buf, "\n");

    for (int i = 0; i < ctx.n_stacks; i++) {
        const pprof_stack_entry_ *e = &ctx.stacks[i];
        /* Folded stack format: frame1;frame2;frame3 count */
        for (int f = e->n_frames - 1; f >= 0; f--) {
            fmt_symbol_(buf, e->frames[f]);
            if (f > 0) cetcd_buf_append_cstr(buf, ";");
        }
        cetcd_buf_printf(buf, " %d\n", e->count);
    }

    return 0;
}
