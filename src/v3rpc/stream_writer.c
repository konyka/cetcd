#include "cetcd/v3rpc.h"

cetcd_stream_write_fn g_rpc_stream_write_fn = NULL;
void                 *g_rpc_stream_write_ctx = NULL;

void cetcd_v3rpc_set_stream_writer(cetcd_v3rpc *rpc,
                                    cetcd_stream_write_fn fn,
                                    void *ctx) {
    if (!rpc) return;
    g_rpc_stream_write_fn = fn;
    g_rpc_stream_write_ctx = ctx;
}

void cetcd_v3rpc_capture_stream_writer(cetcd_stream_write_fn *fn, void **ctx) {
    if (fn) *fn = g_rpc_stream_write_fn;
    if (ctx) *ctx = g_rpc_stream_write_ctx;
}
