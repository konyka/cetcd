#include "cetcd/v3rpc.h"

static uint64_t g_warning_apply_ns = 0;
static uint64_t g_warning_unary_ns = 0;

int cetcd_v3rpc_apply_should_warn(uint64_t elapsed_ns, uint64_t threshold_ns) {
    return threshold_ns > 0 && elapsed_ns >= threshold_ns;
}

void cetcd_v3rpc_set_warning_apply_ns(uint64_t ns) {
    g_warning_apply_ns = ns;
}

uint64_t cetcd_v3rpc_warning_apply_ns(void) {
    return g_warning_apply_ns;
}

void cetcd_v3rpc_set_warning_unary_ns(uint64_t ns) {
    g_warning_unary_ns = ns;
}

uint64_t cetcd_v3rpc_warning_unary_ns(void) {
    return g_warning_unary_ns;
}
