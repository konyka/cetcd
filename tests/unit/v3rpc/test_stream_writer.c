#include "cetcd/v3rpc.h"
#include "cetcd_test.h"

#include <stdint.h>

static int g_writes_a;
static int g_writes_b;

static void write_a_(const uint8_t *data, size_t len, void *ctx) {
    (void)data;
    (void)len;
    (void)ctx;
    g_writes_a++;
}

static void write_b_(const uint8_t *data, size_t len, void *ctx) {
    (void)data;
    (void)len;
    (void)ctx;
    g_writes_b++;
}

CETCD_TEST_CASE(stream_writer_capture_survives_steal) {
    cetcd_v3rpc *rpc = (cetcd_v3rpc *)(uintptr_t)1;
    uint8_t blob[] = {0x01};

    g_writes_a = g_writes_b = 0;
    cetcd_v3rpc_set_stream_writer(rpc, write_a_, (void *)(uintptr_t)11);

    cetcd_stream_write_fn fn = NULL;
    void *ctx = NULL;
    cetcd_v3rpc_capture_stream_writer(&fn, &ctx);

    /* A multiplexed second RPC overwrites the global. */
    cetcd_v3rpc_set_stream_writer(rpc, write_b_, (void *)(uintptr_t)22);

    CETCD_ASSERT_TRUE(fn == write_a_);
    CETCD_ASSERT_TRUE(ctx == (void *)(uintptr_t)11);
    fn(blob, sizeof(blob), ctx);
    CETCD_ASSERT_EQ_INT(g_writes_a, 1);
    CETCD_ASSERT_EQ_INT(g_writes_b, 0);

    cetcd_v3rpc_set_stream_writer(rpc, NULL, NULL);
}

CETCD_TEST_CASE(stream_writer_capture_null_ok) {
    cetcd_v3rpc_capture_stream_writer(NULL, NULL);
    cetcd_v3rpc_set_stream_writer(NULL, write_a_, NULL);
}

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(stream_writer_capture_survives_steal),
    CETCD_TEST_ENTRY(stream_writer_capture_null_ok),
CETCD_TEST_LIST_END

CETCD_TEST_MAIN()
