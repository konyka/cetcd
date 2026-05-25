#ifndef CETCD_CLOCK_H_
#define CETCD_CLOCK_H_

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

uint64_t cetcd_clock_monotonic_ns(void);
uint64_t cetcd_clock_realtime_ns(void);

typedef struct cetcd_clock_vtable cetcd_clock_vtable;
struct cetcd_clock_vtable {
    uint64_t (*monotonic_ns)(void *self);
    uint64_t (*realtime_ns)(void *self);
    void    *self;
};

void                       cetcd_clock_set_global(const cetcd_clock_vtable *vt);
const cetcd_clock_vtable  *cetcd_clock_global(void);

#ifdef __cplusplus
}
#endif
#endif
