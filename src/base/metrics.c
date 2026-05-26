#include "cetcd/metrics.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

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

size_t cetcd_metrics_render(const cetcd_metrics *m, char *buf, size_t buf_size) {
    if (!m || !buf || buf_size == 0) return 0;
    size_t pos = 0;
    for (size_t i = 0; i < m->count; i++) {
        const metric_entry_ *e = &m->entries[i];
        if (e->kind == METRIC_HISTOGRAM) {
            int n = snprintf(buf + pos, buf_size - pos, "%s_count %llu\n%s_sum ",
                             e->name, (unsigned long long)e->count, e->name);
            if (n > 0 && (size_t)n < buf_size - pos) pos += (size_t)n;
            char vbuf[64];
            int vn = fmt_double_(vbuf, sizeof(vbuf), e->sum);
            if (vn > 0 && (size_t)vn < buf_size - pos) { memcpy(buf + pos, vbuf, (size_t)vn); pos += (size_t)vn; }
            if (pos < buf_size - 1) buf[pos++] = '\n';
        } else {
            int n = snprintf(buf + pos, buf_size - pos, "%s ", e->name);
            if (n > 0 && (size_t)n < buf_size - pos) pos += (size_t)n;
            char vbuf[64];
            int vn = fmt_double_(vbuf, sizeof(vbuf), e->value);
            if (vn > 0 && (size_t)vn < buf_size - pos) { memcpy(buf + pos, vbuf, (size_t)vn); pos += (size_t)vn; }
            if (pos < buf_size - 1) buf[pos++] = '\n';
        }
    }
    buf[pos] = '\0';
    return pos;
}
