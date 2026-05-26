#include "cetcd/base.h"
#include "cetcd/mvcc.h"
#include "cetcd/lease.h"
#include "cetcd/auth.h"
#include "cetcd/snap.h"
#include "cetcd/bench.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    cetcd_mvcc_store *store;
    uint64_t counter;
} bench_mvcc_ctx;

static void bench_mvcc_put_seq(void *udata) {
    bench_mvcc_ctx *ctx = (bench_mvcc_ctx *)udata;
    uint8_t key[8];
    uint8_t val[8];
    uint64_t c = ctx->counter++;
    memcpy(key, &c, 8);
    memcpy(val, &c, 8);
    cetcd_mvcc_put(ctx->store, key, 8, val, 8, 0);
}

static void bench_mvcc_get(void *udata) {
    bench_mvcc_ctx *ctx = (bench_mvcc_ctx *)udata;
    uint8_t key[8];
    uint64_t c = ctx->counter % 1000;
    memcpy(key, &c, 8);
    cetcd_kv out;
    if (cetcd_mvcc_get(ctx->store, 0, key, 8, &out) == 0) {
        free((void*)out.key.data);
        free((void*)out.value.data);
    }
}

static void bench_hash_fnv1a(void *udata) {
    (void)udata;
    const char *msg = "benchmark payload for fnv1a hashing speed";
    cetcd_hash_fnv1a64(msg, strlen(msg));
}

static void bench_snap_encode(void *udata) {
    cetcd_snap *s = (cetcd_snap *)udata;
    size_t len = 0;
    uint8_t *encoded = cetcd_snap_encode(s, &len);
    free(encoded);
}

static void bench_auth_check(void *udata) {
    cetcd_auth_store *store = (cetcd_auth_store *)udata;
    cetcd_auth_check_password(store, "benchuser", "benchpass123");
}

int main(void) {
    printf("\n=== cetcd microbenchmarks ===\n\n");

    {
        bench_mvcc_ctx ctx;
        ctx.store = cetcd_mvcc_store_new();
        ctx.counter = 0;
        cetcd_bench_result r = cetcd_bench_run("mvcc_put_seq", bench_mvcc_put_seq, &ctx, 100000);
        cetcd_bench_print(&r);
        cetcd_mvcc_store_free(ctx.store);
    }

    {
        bench_mvcc_ctx ctx;
        ctx.store = cetcd_mvcc_store_new();
        ctx.counter = 0;
        uint8_t key[8]; uint8_t val[8];
        for (int i = 0; i < 1000; i++) {
            uint64_t c = (uint64_t)i;
            memcpy(key, &c, 8); memcpy(val, &c, 8);
            cetcd_mvcc_put(ctx.store, key, 8, val, 8, 0);
        }
        ctx.counter = 0;
        cetcd_bench_result r = cetcd_bench_run("mvcc_get_random", bench_mvcc_get, &ctx, 100000);
        cetcd_bench_print(&r);
        cetcd_mvcc_store_free(ctx.store);
    }

    {
        cetcd_bench_result r = cetcd_bench_run("hash_fnv1a_44byte", bench_hash_fnv1a, NULL, 100000);
        cetcd_bench_print(&r);
    }

    {
        cetcd_snap *s = cetcd_snap_new();
        for (int i = 0; i < 100; i++) {
            uint8_t key[8]; uint8_t val[8];
            uint64_t c = (uint64_t)i;
            memcpy(key, &c, 8); memcpy(val, &c, 8);
            cetcd_snap_add_entry(s, key, 8, val, 8, (int64_t)i);
        }
        cetcd_bench_result r = cetcd_bench_run("snap_encode_100entries", bench_snap_encode, s, 10000);
        cetcd_bench_print(&r);
        cetcd_snap_free(s);
    }

    {
        cetcd_auth_store *store = cetcd_auth_store_new();
        cetcd_auth_add_user(store, "benchuser", "benchpass123");
        cetcd_bench_result r = cetcd_bench_run("auth_check_password", bench_auth_check, store, 100000);
        cetcd_bench_print(&r);
        cetcd_auth_store_free(store);
    }

    printf("\n");
    return 0;
}
