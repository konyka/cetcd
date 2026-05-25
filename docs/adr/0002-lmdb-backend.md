# ADR 0002 — LMDB as the backend store

- **Status**: Accepted (Phase 0).
- **Date**: 2026-05-25.
- **Deciders**: project author.

## Context

etcd persists its MVCC state in [bbolt](https://github.com/etcd-io/bbolt) (an etcd fork of
boltdb). bbolt is a pure-Go embedded B+tree with single-writer / many-reader semantics. We
need an equivalent for C.

Candidates surveyed:

| Option        | Pros                                                                | Cons                                                          |
| ------------- | ------------------------------------------------------------------- | ------------------------------------------------------------- |
| **LMDB**      | mmap'd, zero-copy reads, MVCC native, single-writer/many-reader matches etcd's batch-tx model, mature, OpenLDAP licence (BSD-style). | Single-writer is a real bottleneck if writes aren't batched. Disk format is **not** compatible with bbolt. |
| SQLite        | Universal, mature, supports SQL queries.                            | WAL mode adds complexity; row-oriented semantics aren't ideal for raw KV; bigger surface. |
| Custom B+tree | Total control over format, page layout, MVCC.                       | Months of work; correctness risk. |
| RocksDB-C     | LSM, batched writes, used by TiKV.                                  | C++ core, log-structured (compaction pauses), large dependency. |

## Decision

Use **LMDB** for `libcetcd_backend`. Wrap it in a thin C API that:

- batches writes per Raft-committed entry block,
- exposes `cetcd_backend_tx_begin`/`commit`/`rollback`,
- maps cetcd's logical buckets (`key`, `lease`, `auth`, …) onto LMDB sub-databases.

We **do not** target on-disk compatibility with bbolt — that would require us to implement a
read/write bbolt page format, which is a substantial reverse-engineering project on its own
and would lock us out of LMDB's mmap performance.

Instead, we ship `cetcd-migrate`: a one-way converter that reads an existing etcd data
directory (bbolt + WAL + snap) and writes a cetcd-native LMDB env. The WAL itself stays
byte-compatible with etcd (see `docs/architecture.md §4`) so the *history* is portable; only
the materialised state moves to LMDB.

## Consequences

### Positive

- LMDB's single-writer model is exactly what etcd's Raft apply loop produces: one writer at a
  time, naturally batched.
- mmap means zero-copy reads — a `cetcd_slice` (ptr+len) into the mmap region — which we ride
  all the way through the gRPC response path.
- LMDB has no background compaction; its B+tree free-list reuse keeps latency stable.
- Stable, BSD-style license; no dynamic linking caveats.
- LMDB is widely deployed (OpenLDAP, Monero, Cardano…) — bus factor is high.

### Negative

- One-way upgrade story: you cannot downgrade a cetcd cluster back to Go etcd without
  re-bootstrapping from a snapshot. Documented loudly in `docs/porting.md`.
- LMDB writes are serialised at the env level; multi-Raft (per-shard Raft groups) would need
  one env per group. Not a v0.1 concern, but flagged.
- Map size must be pre-declared (`mdb_env_set_mapsize`). We default to 8 GiB with online
  growth via `MDB_NOMETASYNC + double-then-reopen`.

### Mitigations

- Phase 9 includes a custom-B+tree spike if benchmarks show LMDB's single-writer is a
  bottleneck under our target workload.
- Migration tool is part of v0.1 acceptance criteria; tested against a real etcd snapshot in
  CI's `compat` job.

## References

- Howard Chu — *Multi-Version Concurrency Control in the LMDB Database Manager*.
- LMDB docs: <https://lmdb.tech/>.
- etcd backend code: `server/storage/backend/backend.go`.
