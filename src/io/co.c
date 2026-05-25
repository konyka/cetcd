/* Lightweight stub of coroutine support for Phase 0.
 * We implement opaque handles and a no-op execution model to keep
 * the public API compiling and testable without bringing in the full
 * libuv/libco machinery yet.
 */
#include <stdlib.h>
#include "cetcd/io.h"

typedef struct cetcd_co {
    int id;
    void *arg;
    cetcd_co_fn fn;
} cetcd_co;

cetcd_co *cetcd_co_spawn(cetcd_loop *loop, cetcd_co_fn fn, void *arg) {
    (void)loop; /* unused in stub */
    cetcd_co *co = (cetcd_co *)malloc(sizeof(*co));
    if (!co) return NULL;
    co->id = 1; /* dummy id for tests */
    co->fn = fn;
    co->arg = arg;
    /* Execute immediately to simulate immediate scheduling */
    if (fn) fn(arg);
    return co;
}

void cetcd_co_yield(cetcd_loop *loop) {
    (void)loop;
}

void cetcd_co_resume(cetcd_co *co) {
    if (!co) return;
    if (co->fn) co->fn(co->arg);
}
