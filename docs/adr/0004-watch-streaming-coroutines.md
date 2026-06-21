# ADR 0004 — Watch Streaming via Coroutines

- **Status**: Accepted.
- **Date**: 2026-06-21.
- **Deciders**: project author.

## Context

The etcd v3 `Watch` RPC is a long-lived, bidirectional gRPC stream: a client sends
`WatchCreateRequest` and `WatchCancelRequest` messages, and the server pushes
`WatchResponse` messages as matching key events occur. cetcd's earlier RPC dispatch
model handled each inbound frame as a single-shot request/response pair, which works
for `Put`/`Range` but cannot express a stream that stays open across many events.

Three implementation strategies were considered:

1. **Callback-driven state machine** inside the existing dispatch path. This keeps the
   single-shot model but fragments the watch lifecycle across many callbacks and makes
   cancellation, backpressure, and per-watcher state error-prone.
2. **Dedicated thread per watcher**. This is simple to reason about but at high
   watcher counts the kernel stack and context-switch overhead dominate.
3. **Stackful coroutine per watcher** on the libuv reactor. Each watcher becomes a
   synchronous-looking loop that yields while waiting for the next event, and is
   resumed via `uv_async_send` when MVCC produces a matching update.

## Decision

Use **libco coroutines** integrated with **libuv async handles** to implement
non-blocking bidirectional Watch streaming.

```c
/* Per-watcher coroutine sketch */
void watch_coroutine(void *arg) {
    cetcd_watch_ctx *ctx = arg;

    /* Send the WatchResponse announcing the watcher was created. */
    send_created_response(ctx);

    for (;;) {
        /* Yield until MVCC notifies us via uv_async_send. */
        cetcd_co_yield(ctx->loop);

        if (ctx->cancelled) break;

        /* Encode and write the WatchResponse containing the event(s). */
        cetcd_watch_response resp;
        build_response(ctx, &resp);
        cetcd_co_grpc_send(ctx->stream, &resp);
    }
}
```

Integration points:

- `libcetcd_io` provides `cetcd_co_yield` / `cetcd_co_resume` and an async handle
  that, when signalled from another thread or from an MVCC callback, resumes the
  coroutine on the reactor thread.
- `libcetcd_mvcc` keeps a watcher registry keyed by key and prefix. On every
  committed `Put` or `Delete`, matching watchers are notified through a lock-free
  callback that calls `uv_async_send`.
- `libcetcd_v3rpc` parses `WatchCreateRequest`, assigns a `watch_id`, spawns the
  coroutine, and forwards cancellation requests.

### Why not callback-driven

A state machine for bidirectional streams with multiple concurrent watchers, client
initiated cancellation, and progressive event delivery becomes hard to maintain and
test. The coroutine model maps directly to the stream semantics.

### Why not thread-per-watcher

At 10k+ watchers the kernel scheduler and per-thread stack usage (even 8 MiB default
stacks) make this impractical. Coroutines share a single reactor thread and use only
a few kilobytes of stack each.

## Consequences

### Positive

- **Each watcher gets a dedicated 4 KiB coroutine stack** — memory-efficient enough
  to support tens of thousands of concurrent watchers.
- **Events are pushed with minimal latency** because there is no polling; a watcher
  coroutine is resumed only when an actual event arrives.
- **Single-connection multiplexing** is natural: many watcher coroutines can coexist
  on one HTTP/2 connection without blocking each other.
- **Backpressure is cooperative**: a slow consumer simply stalls its own coroutine
  on `cetcd_co_write`; faster consumers are unaffected.

### Negative

- **Adds complexity to debugging**: a suspended watcher has its local state on a
  libco stack, which debuggers and stack traces handle less intuitively than a
  native thread stack.
- **Stack size must be bounded**: deep recursion inside a watch coroutine can
  overflow the small stack. Handler code is kept non-recursive.
- **Cross-thread signal ordering**: `uv_async_send` may coalesce; the coroutine must
  drain all pending events before yielding again to avoid missing notifications.

### Mitigations

- Watch coroutines are restricted to a well-defined call graph with no recursion.
- A debug build can capture per-coroutine stack snapshots for the
  `/debug/pprof/coroutines` endpoint.
- CI includes stress tests with thousands of concurrent watchers to validate
  memory usage and latency.

## References

- ADR 0003 — Coroutines on top of libuv.
- libco: <https://github.com/Tencent/libco>.
- libuv async handles: <https://docs.libuv.org/en/v1.x/async.html>.
- etcd v3 Watch API: `api/etcdserverpb/rpc.proto`.
