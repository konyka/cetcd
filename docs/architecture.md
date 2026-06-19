# cetcd Architecture

> **Status**: living document. See `notes.html` for delta history.

cetcd is a from-scratch reimplementation of [etcd](https://github.com/etcd-io/etcd) in pure C
(C99 floor with required C11 `<stdatomic.h>`). It targets wire compatibility with the
**etcd v3.5 stable API** so that existing tools (`etcdctl`, official client libraries) work
unchanged.

This document is the canonical reference for what cetcd *is* and *is not*, and how its
internals are organised. For deeper rationale on individual decisions, see
[`docs/adr/`](./adr/).

---

## 1. Goals and non-goals

### Goals

- **Wire compatibility** with the etcd v3.5 gRPC API — all 41 RPCs across KV, Watch, Lease,
  Cluster, Auth, Maintenance.
- **Cross-platform**: Linux (primary), macOS, FreeBSD, Windows (MSVC + MinGW-w64).
- **Performance** parity or better with Go etcd on a 3-node 70/30 Put/Range workload.
- **Pure C, C99 floor**. C11 atomics required. No GNU/MSVC extensions in public headers.
- **Distribution**: Raft consensus, joint-consensus membership changes, snapshots.
- **Testability**: TDD throughout. Deterministic Raft tests via injectable clocks/networks.
- **Observability**: structured JSON logs, Prometheus `/metrics`, profiling hooks.

### Non-goals (v0.1)

- The etcd v2 API.
- The gRPC-gateway (HTTP/JSON ↔ gRPC proxy).
- `etcdutl` (offline disk surgeon).
- gRPC reflection service.

---

## 2. Module map

```
src/
├── base/        libcetcd_base       arenas, slabs, hash, btree, refcount, errors, log, clock
├── io/          libcetcd_io         libuv event loop + libco coroutines + worker pool
├── proto/       libcetcd_proto      protobuf-c runtime + generated etcd v3 message types
├── http2/       libcetcd_http2      nghttp2 session management + gRPC framing (length-prefixed proto frames)
├── tls/         libcetcd_tls        OpenSSL 3 TLS termination + ALPN
├── raft/        libcetcd_raft       Raft logic core (no I/O, no threads)
├── wal/         libcetcd_wal        Append-only log, byte-compatible with etcd WAL
├── backend/     libcetcd_backend    LMDB-backed transactional key-value
├── mvcc/        libcetcd_mvcc       Revision index, watcher fan-out, compaction
├── lease/       libcetcd_lease      TTL tracking + lease-keys index + lease enumeration
├── auth/        libcetcd_auth       RBAC, SHA-256 password hashing
├── peer/        libcetcd_peer       Raft transport, cluster membership mgmt (rafthttp-equivalent)
├── snap/        libcetcd_snap       Snapshot file r/w and streaming
├── v3rpc/       libcetcd_v3rpc      gRPC handlers for all 41 RPCs (cluster-aware, raft-integrated)
└── server/      libcetcd_server     Main loop, apply pipeline, config, lifecycle

cmd/
├── cetcd/         daemon binary
└── cetcdctl/      client CLI (put/get/del/lease/member/auth/user/role/snapshot/hash/defrag/move-leader)
```

### Dependency direction

```
cetcd (daemon)
  └─ libcetcd_server
       ├─ libcetcd_v3rpc
       │    ├─ libcetcd_http2 ─── nghttp2
       │    ├─ libcetcd_tls   ─── OpenSSL
       │    └─ libcetcd_proto ─── protobuf-c
       ├─ libcetcd_peer
       ├─ libcetcd_raft       (pure logic, no deps below base)
       ├─ libcetcd_wal
       ├─ libcetcd_mvcc
       │    └─ libcetcd_backend ─── LMDB
       ├─ libcetcd_lease
       ├─ libcetcd_auth
       ├─ libcetcd_snap
       └─ libcetcd_io          ─── libuv + libco

libcetcd_base   (zero deps except libc)
```

No back edges. `libcetcd_base` is a leaf. Tests live alongside each module under
`tests/unit/<module>/`.

---

## 3. Concurrency model

```
┌─────────────────────────────────────────────────────────────────┐
│ Reactor thread(s) — N per machine, sharded by connection hash   │
│                                                                 │
│   libuv event loop                                              │
│     ├─ accept TCP / TLS                                         │
│     ├─ nghttp2 → gRPC frames                                    │
│     ├─ dispatch each RPC to its own libco coroutine             │
│     │     coroutine yields on:                                  │
│     │       • disk fsync       → worker pool                    │
│     │       • raft propose     → MPSC ring → raft thread        │
│     │       • LMDB read        → inline (mmap, no syscall)      │
│     └─ watcher fan-out (lock-free per-key queues)               │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ Raft thread (1)                                                 │
│   • cetcd_raft_step(node, msg)                                  │
│   • cetcd_raft_ready(node) → {entries, committed, snap, msgs}   │
│   • commit batch handed to apply coroutine on reactor           │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│ Worker pool (N = cores)                                         │
│   • WAL fsync                                                   │
│   • Snapshot create/restore                                     │
│   • LMDB compaction                                             │
└─────────────────────────────────────────────────────────────────┘
```

Rationale: see [ADR 0003 — Coroutines on libuv](./adr/0003-coroutines-on-libuv.md).

### Lock policy

- All inter-thread communication uses **MPSC/SPSC ring buffers** in `libcetcd_base`.
  No locks on the hot path between reactor ↔ raft ↔ worker pool.
- Within a reactor, coroutines are cooperatively scheduled — no locks needed for
  per-connection state.
- Cross-reactor sharing happens via the raft thread only.

---

## 4. Storage layer

### WAL (write-ahead log)

cetcd's WAL is **byte-for-byte compatible** with etcd's WAL so we can read an existing
cluster's log files. Each record is:

```
+--------+----------------+----------+-------------+
| length |   type:Int64   |   data   | crc:CRC32C  |
| 8 B LE |    8 B LE      |  N bytes |    4 B LE   |
+--------+----------------+----------+-------------+
```

Record types: `Metadata`, `Entry`, `State`, `Snapshot`, `CRC`. Segment files are named
`%016x-%016x.wal` (sequence, start-index). CRC algorithm is **Castagnoli (CRC32C)**, matching
etcd. Segment rotation at 64 MiB by default.

### Backend (LMDB)

The backend uses **LMDB** (single-writer / many-reader memory-mapped B+tree). One environment
per cetcd instance, with logical sub-databases corresponding to etcd's bbolt buckets:

- `key` — committed MVCC key-value pairs, keyed by encoded revision.
- `lease` — lease metadata.
- `auth`, `authUsers`, `authRoles` — RBAC tables.
- `members`, `cluster`, `alarm`, `meta` — cluster state.

cetcd is **not** disk-format compatible with bbolt. A one-way migrator `cetcd-migrate` ships
to convert an existing etcd data directory into a cetcd LMDB env.
See [ADR 0002 — LMDB backend](./adr/0002-lmdb-backend.md).

### MVCC

The revision index uses a **treap** (probabilistically balanced BST) mapping
`key → keyIndex { generations[] { revisions[] } }`, mirroring etcd's `mvcc/key_index.go`.
Reads at a given revision do an inorder walk; ranges use the LMDB cursor.

Watchers attach to a per-key bucket; commit notifications fan out via lock-free queues to
each watcher's coroutine.

### Snapshot

Snapshots are streamed over gRPC via the `Maintenance.Snapshot` server-streaming RPC. On
disk, snapshot files are stored as `%016x-%016x.snap` with a header `{crc:uint32, len:uint32}`
followed by an LMDB env-dump payload. A separate `etcd-snap-import` mode accepts bbolt-format
snapshots for migration.

---

## 5. Raft

cetcd ships its own Raft implementation modeled on [`go.etcd.io/raft`](https://github.com/etcd-io/raft).
See [ADR 0001 — Raft rolled in-house](./adr/0001-raft-rolled-in-house.md) for why no existing
C Raft library was chosen.

### Public API (sketch)

```c
typedef struct cetcd_raft_node cetcd_raft_node;

cetcd_raft_node *cetcd_raft_start(const cetcd_raft_config *cfg);
void             cetcd_raft_stop(cetcd_raft_node *n);

/* Feed an incoming raft message (from peer). */
int cetcd_raft_step(cetcd_raft_node *n, const cetcd_raft_msg *msg);

/* Drain pending work the embedder must persist & send. */
typedef struct {
    cetcd_raft_hard_state state;       /* persist before sending */
    cetcd_slice          *entries;     /* persist to WAL */
    size_t                num_entries;
    cetcd_raft_msg       *messages;    /* send to peers AFTER persist */
    size_t                num_messages;
    cetcd_raft_snapshot  *snapshot;    /* optional */
    /* ...committed entries ready to apply... */
} cetcd_raft_ready;

cetcd_raft_ready *cetcd_raft_ready_take(cetcd_raft_node *n);
void              cetcd_raft_advance(cetcd_raft_node *n, cetcd_raft_ready *r);
```

The Raft module **does no I/O and spawns no threads**. The embedder owns persistence
(WAL/Snapshot) and transport (`libcetcd_peer`).

---

## 6. Wire protocol

cetcd speaks etcd v3 gRPC over HTTP/2:

- 6 services: `KV`, `Watch`, `Lease`, `Cluster`, `Maintenance`, `Auth`.
- 41 RPCs (full catalogue in [`docs/wiki/Home.md`](../docs/wiki/Home.md)).
- 4 streaming RPCs: `RangeStream`, `Watch` (bidi), `LeaseKeepAlive` (bidi), `Snapshot` (server-stream).
- Protobuf wire-types are generated from etcd v3.5's `.proto` files vendored under
  `proto/v3.5/`.

The HTTP/2 layer uses nghttp2 for full session management — connection preface,
SETTINGS exchange, HPACK header compression, stream multiplexing, and DATA/HEADERS
frame processing. HTTP-level message validation is disabled via
`nghttp2_option_set_no_http_messaging` because gRPC does not require full HTTP
semantics (e.g. `:authority`, content-length consistency). When nghttp2 is not
available, a stub implementation provides safe no-ops so the rest of the codebase
compiles and the gRPC framing helpers remain functional.

The peer transport (`libcetcd_peer`) mirrors etcd's `rafthttp` over HTTP/2 streams. Peer URLs
take the form `http(s)://host:peer-port/raft/stream/{message,msgapp}/{member-id}` — same as
etcd, so cetcd nodes can act as peers in a mixed cetcd/etcd cluster (validated in Phase 7).
The cluster management API supports peer enumeration by index (`cetcd_cluster_get_peer_by_index`),
in-place peer info updates (`cetcd_cluster_update_peer`), and self-ID queries (`cetcd_cluster_self_id`).
The `MemberList` RPC iterates all peers to return a complete cluster membership view, `MemberUpdate`
actually modifies peer addresses, `Maintenance.Status` returns the real Raft leader ID and term from
the live `cetcd_raft` instance, and `Maintenance.MoveLeader` triggers `CETCD_MSG_TRANSFER_LEADER`
for actual leadership transfer.

The `Lease.LeaseLeases` RPC returns the actual list of active lease IDs via
`cetcd_lease_mgr_leases()`, and `Lease.LeaseKeepAlive` uses the original granted TTL
(obtained via `cetcd_lease_granted_ttl()`) instead of a hardcoded value, ensuring
correct renewal behavior for leases with non-default TTLs.
All Lease RPC responses (`LeaseGrant`, `LeaseKeepAlive`, `LeaseTimeToLive`,
`LeaseLeases`, `LeaseRevoke`) include a `ResponseHeader` as field 1, matching the
etcd v3.5 proto wire format. The `LeaseTimeToLive` handler also parses the `keys`
boolean field from the request and returns attached keys (field 5) when requested,
using the new `cetcd_lease_keys()` API. The `LeaseGrant` response also attaches keys
to leases via `cetcd_lease_attach_key()` when a `Put` request specifies a lease ID.

All Maintenance RPC responses (`Status`, `Hash`, `HashKV`, `Defragment`, `Alarm`,
`MoveLeader`, `Snapshot`, `Downgrade`) include a `ResponseHeader` as field 1, matching the
etcd v3.5 proto wire format. The `DowngradeResponse` now correctly returns only a header
(field 1) instead of a version string. The `make_simple_response()` helper used by
`Defragment`, `Alarm`, and `MoveLeader` returns a proper `ResponseHeader` with the current
revision.

All Cluster RPC responses (`MemberList`, `MemberAdd`, `MemberRemove`, `MemberUpdate`,
`MemberPromote`) include a `ResponseHeader` as field 1, matching the etcd v3.5 proto wire
format. The `make_simple_cluster_response()` helper used by `MemberRemove`, `MemberUpdate`,
and `MemberPromote` returns a proper `ResponseHeader` with the current revision.

The `cetcdctl` CLI has been expanded to cover the full command set: `lease list/keepalive`,
`member add/remove/update/promote`, `user delete/change-password/grant-role/revoke-role`,
`role delete`, `hash`, `hashkv`, `defrag`, `move-leader`, `get --prefix/--keys-only/--rev`,
`del --prefix/--prev-kv`, `put --prev-kv`, `watch --prefix/--prev-kv/--start-rev`, `txn cas` (compare-and-swap),
`auth login` (token-based authentication), `get --count-only/--limit N/--sort-by/--sort-order/--print-value-only`,
`put --ignore-value/--ignore-lease`.

The KV RPC handlers have been fully implemented: `Range` queries the MVCC store and returns
actual `KeyValue` protobuf messages (supporting both point-get and range queries with
`range_end`), `Put` returns a proper `PutResponse` with header revision and supports `prev_kv`
(returning the previous key-value when `prev_kv=true` is set in the request, encoded as field 2
tag 0x12 per etcd v3.5 proto), `ignore_value`
(keeping the existing value when `ignore_value=true`), and `ignore_lease` (keeping the existing
lease when `ignore_lease=true`), `DeleteRange`
supports `range_end` for range deletes and `prev_kv` for returning deleted key-values.
The `Range` handler also supports `limit` (truncating results and setting the `more` flag as
field 3 tag 0x18), `count_only` (returning only the count without kvs), `keys_only` (omitting values), and
`sort_order`/`sort_target` (sorting results by KEY, VERSION, CREATE, MOD, or VALUE in ASCEND
or DESCEND order before applying the limit). The `Range` handler also supports `min_mod_revision`,
`max_mod_revision`, `min_create_revision`, and `max_create_revision` filters (fields 10–13),
which allow filtering results by revision range before sorting and applying limits.
The `RangeResponse` kvs field correctly uses
protobuf field 2 (tag 0x12) to avoid collision with the `ResponseHeader` (field 1, tag 0x0a),
and the `more` flag correctly uses field 3 (tag 0x18).
The `Txn` handler now evaluates `Compare` clauses against the MVCC store — supporting
`EQUAL`/`GREATER`/`LESS`/`NOT_EQUAL` operators on `VERSION`, `CREATE`, `MOD`, `VALUE`, and
`LEASE` targets — and executes success or failure ops accordingly, returning a complete
`TxnResponse` with `ResponseHeader`, `succeeded` flag, and `ResponseOp` entries. The
`RequestRange` op within transactions now queries the MVCC store and returns actual key-value
data instead of an empty count. It also supports `limit` (field 3, tag 0x18) for result
truncation with the `more` flag, `keys_only` (field 8, tag 0x40) to omit values, and
`count_only` (field 9, tag 0x48) to return only the count without kvs — matching the
standalone `Range` handler. The `ResponseRange` within transactions includes a proper
`ResponseHeader` (field 1, tag 0x0a) with the current revision, `kvs` (field 2, tag 0x12),
`more` (field 3, tag 0x18), and `count` (field 4, tag 0x20) — matching the standalone
`RangeResponse` wire format. The `RequestPut` op within transactions supports `prev_kv`
(field 4, tag 0x20), `ignore_value` (field 5, tag 0x28) to keep the existing value, and
`ignore_lease` (field 6, tag 0x30) to keep the existing lease; the `ResponsePut` includes the
previous key-value (field 2, tag 0x12) when `prev_kv` is requested. The `RequestDeleteRange`
op within transactions supports `range_end` (field 2, tag 0x12) for range deletes and `prev_kv`
(field 3, tag 0x18) for returning deleted key-values; the `ResponseDeleteRange` includes a
proper `ResponseHeader` (field 1), `deleted` count (field 2, tag 0x10), and `prev_kvs`
(field 3, tag 0x1a). The `Compare` clause also supports `range_end` (field 9, tag 0x4a) for
range-based comparisons where all keys in [key, range_end) must satisfy the condition.
The `Compact` and `LeaseRevoke` responses include proper
`ResponseHeader` with the current revision. The `Snapshot` response includes a `ResponseHeader`.

All Auth RPC responses now include a proper `ResponseHeader` with the current revision.
The `Authenticate` response correctly returns the token in field 2 (tag 0x12) alongside the
header. The `AuthStatus`, `UserList`, `RoleList`, `UserGet`, and `RoleGet` responses all
include a `ResponseHeader` prefix.

The Watch handler now includes a `ResponseHeader` (field 1, tag 0x0a) in all WatchResponse
messages — create, cancel, and fallback paths. The Event `KeyValue` encoding uses correct
etcd v3.5 protobuf field numbers: key (field 1, 0x0a), create_revision (field 2, 0x10),
mod_revision (field 3, 0x18), version (field 4, 0x20), and value (field 5, 0x2a). The
`WatchCreateRequest` parser also supports `prev_kv` (field 6) and client-specified `watch_id`
(field 7). The cetcdctl `watch` command supports `--prev-kv` and `--start-rev` flags.
The Watch handler encodes `prev_kv` (field 3, tag 0x1a) in Event messages when the watcher
requests it via `prev_kv=true` and a previous value exists. The MVCC layer captures the
previous key-value before each `Put` and `Delete` operation, passing it through the watcher
callback so the Watch handler can include it in the event.

The cetcdctl response parsing for `del`, `txn cas`, and `watch` now correctly skips the
`ResponseHeader` (tag 0x0a) before parsing response-specific fields, ensuring compatibility
with the proper protobuf encoding.

---

## 7. Build, test, ship

### Build

CMake 3.21+. All third-party libraries are **vendored** under `third_party/` (pinned).
OpenSSL is the one exception: discovered via `pkg_check_modules(OPENSSL IMPORTED_TARGET openssl)`.

```sh
cmake -B build -G Ninja -DCMAKE_BUILD_TYPE=Debug -DCETCD_SANITIZERS=address,undefined
cmake --build build
ctest --test-dir build --output-on-failure
```

### Test

- Unit tests: **Unity + CMock**, one binary per `libcetcd_*`, registered with `ctest`.
- Integration: shell-driven, spawns real `cetcd` and `etcdctl` processes.
- Fuzzing: libFuzzer harnesses (`tests/fuzz/`) on proto, WAL, raft step function.
- Sanitizers: ASan, UBSan, TSan, MSan — each a separate CI job.

### CI

`.github/workflows/ci.yml` — matrix of {Linux-gcc, Linux-clang, macOS, Windows-MSVC,
Windows-MinGW, FreeBSD, Linux-cross-aarch64}. Coverage uploaded to codecov.

---

## 8. Cross-platform abstractions

`libcetcd_base/platform.h` is the only place with `#ifdef _WIN32` etc. Public headers never
include OS-specific headers.

| Abstraction         | POSIX                | Windows                 |
| ------------------- | -------------------- | ----------------------- |
| Time                | `clock_gettime`      | `QueryPerformanceCounter` |
| Thread              | `pthread_create`     | `CreateThread`          |
| Mutex / condvar     | `pthread_mutex_t`    | `SRWLOCK` / `CONDITION_VARIABLE` |
| Atomics             | C11 `<stdatomic.h>`  | C11 `<stdatomic.h>` (MSVC ≥ 2022) |
| File I/O            | `open` / `pread`     | `CreateFileW` / `ReadFileEx` |
| Sockets / event     | via libuv            | via libuv (IOCP)        |
| Coroutines          | libco (ASM per-arch) | libco (Windows fibers)  |
| Dynamic loading     | `dlopen`             | `LoadLibraryW`          |

---

## 9. Glossary

- **Revision**: monotonic global commit counter (etcd MVCC's universal clock).
- **Lease**: a TTL-bound handle keys can attach to; expires together.
- **Apply**: take a committed Raft entry → mutate the state machine (mvcc/backend).
- **Snapshot**: a serialised state-machine checkpoint allowing log truncation.
- **WAL**: write-ahead log — every Raft entry is fsync'd here before it commits.
- **Watcher**: a long-lived stream subscribed to key-range events at-and-after a revision.

---

## 10. References

- Diego Ongaro, John Ousterhout — *In Search of an Understandable Consensus Algorithm* (Raft paper), 2014.
- The etcd source tree: <https://github.com/etcd-io/etcd>.
- The etcd v3 API specification: `api/etcdserverpb/rpc.proto`.
- LMDB design notes: <https://lmdb.tech/>.
- libuv documentation: <https://docs.libuv.org/>.
