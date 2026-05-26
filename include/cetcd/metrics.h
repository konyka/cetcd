#ifndef CETCD_METRICS_H_
#define CETCD_METRICS_H_

#include <stddef.h>
#include <stdint.h>

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

size_t cetcd_metrics_render(const cetcd_metrics *m, char *buf, size_t buf_size);

#ifdef __cplusplus
}
#endif
#endif
