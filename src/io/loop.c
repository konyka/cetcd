/* Minimal stub of libcetcd io loop facade for Phase 0.
 * Keeps API surface so higher layers can compile and tests can run
 * without depending on libuv/libco yet.
 */
#include <stdlib.h>
#include "cetcd/io.h"

typedef struct cetcd_loop {
    int dummy;
} cetcd_loop;

cetcd_loop *cetcd_loop_new(void) {
    cetcd_loop *l = (cetcd_loop *)calloc(1, sizeof(*l));
    return l;
}

int cetcd_loop_run(cetcd_loop *loop) {
    (void)loop;
    return 0;
}

void cetcd_loop_stop(cetcd_loop *loop) {
    (void)loop;
}

void cetcd_loop_free(cetcd_loop *loop) {
    if (loop) free(loop);
}
