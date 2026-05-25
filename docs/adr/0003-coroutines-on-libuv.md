# ADR 0003 — Coroutines on top of libuv

- **Status**: Accepted (Phase 0).
- **Date**: 2026-05-25.
- **Deciders**: project author.

## Context

cetcd handles many long-lived bidirectional streams (Watch, LeaseKeepAlive, Snapshot,
RangeStream) plus burst-y short-lived RPCs. Three styles were considered:

1. **Callback-driven** (raw libuv): every async op posts a callback. Battle-tested but the
   resulting code for a gRPC bidi handler is awful — manual continuation passing, lots of
   state machines.
2. **Thread-per-connection**: simple, but at 10k+ connections the kernel stack overhead and
   context-switch cost dominate.
3. **Stackful coroutines on a reactor**: one OS thread per core runs libuv; each RPC handler
   is its own coroutine; async ops `yield`, the libuv callback resumes.

## Decision

Adopt **stackful coroutines** via [`libco`](https://github.com/Tencent/libco) (Apache-2.0,
small, multi-arch) layered on top of **libuv** (MIT, the de-facto cross-platform reactor).

```c
/* sketch of the integration seam in libcetcd_io */
typedef struct cetcd_coro cetcd_coro;

cetcd_coro *cetcd_coro_spawn(cetcd_io_loop *loop,
                              void (*entry)(void *), void *arg,
                              size_t stack_size /* 0 = default 8 KiB */);

/* Async libuv wrappers that yield until the libuv callback fires. */
int  cetcd_co_read (cetcd_stream *s, void *buf, size_t n);   /* returns bytes or err */
int  cetcd_co_write(cetcd_stream *s, const void *buf, size_t n);
int  cetcd_co_sleep(uint64_t ms);
int  cetcd_co_fsync(cetcd_file *f);   /* dispatched to worker pool, coroutine resumed */
```

A gRPC bidi handler reads like straight-line code:

```c
void watch_handler(void *arg) {
    cetcd_grpc_stream *s = arg;
    cetcd_watch_request req;
    while (cetcd_co_grpc_recv(s, &req) == 0) {
        cetcd_watch_response resp;
        prepare_response(&req, &resp);
        if (cetcd_co_grpc_send(s, &resp) != 0) break;
    }
}
```

### Why libco

- ~1500 LOC, Apache-2.0, used in Tencent production at massive scale.
- Hand-written context-switch assembly for x86_64, aarch64, ARM, RISC-V, MIPS.
- Windows port uses native Fibers — no extra portability burden.
- Default stack size 128 KiB; cetcd will set 8 KiB and grow on demand via guard pages where
  the OS supports it.

### Why not just `ucontext` / `setjmp+longjmp`

- `ucontext` is POSIX-only — Windows would need a separate code path.
- `setjmp/longjmp` cannot resume a function from the middle in the C standard sense — only
  stackful coroutines do that safely.

### Why not stackless / generator-style

- The C standard doesn't have coroutines (C23's `_Coroutines` proposal didn't land). Macro
  hacks (Protothreads) work but break debuggability and forbid local-state across yields.
- All our gRPC handlers carry local state across yields. Stackful is the right fit.

## Consequences

### Positive

- One mental model: every RPC handler is a synchronous-looking function.
- Zero kernel context switches per yield (~50 ns user-space switch).
- Easy backpressure: a coroutine waiting on `cetcd_co_write` is naturally throttled.
- Per-coroutine stacks (8 KiB) → 100k concurrent watchers = ~800 MiB stack reserve, acceptable.

### Negative

- libco's per-arch assembly means we must validate every supported CPU architecture in CI.
- Stackful coroutines need conservative stack-size estimation; deep recursion will SEGV. We
  add `-Wstack-usage=4096` in CI to catch hot-path explosions.
- Debugger UX: gdb / lldb both handle libco stacks correctly today, but tooling must be
  documented.

### Mitigations

- CI matrix runs on x86_64 and cross-compiles for aarch64 (qemu-tested).
- A `CETCD_NO_COROUTINES` build option falls back to a threaded model for platforms libco
  doesn't support — at the cost of throughput.
- `cetcd_coro_spawn` accepts a custom stack size for coroutines known to recurse deeply.

## References

- libco: <https://github.com/Tencent/libco>.
- libuv: <https://docs.libuv.org/>.
- "Cooperative Multitasking in C" — various.
