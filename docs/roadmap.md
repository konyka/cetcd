# cetcd remaining work

> Living list of gaps versus etcd v3.5. Items are ordered by
> **security → reliability → wire compatibility → performance extras**.
> This pass implemented Raft-applied Put/DeleteRange and WAL replay.

## Done in this pass (Raft KV + WAL replay)

Performance-first, fail-closed design:

- **Compact apply entries** — `tag + varint lengths + bytes` (Put/Delete/DeleteRange),
  not protobuf on the raft hot path. Apply is a linear scan of `applied+1..commit`
  from the in-memory log after one WAL `fsync` per Ready.
- **Single-node commit** — voter quorum is one match index per peer (leader = last_index).
  Log entries own a copy of the payload so RPC buffers are freed immediately.
- **Per-request propose** — Put/DeleteRange propose then `process_ready_` on the
  reactor thread. Not-leader or persist failure is fail-closed (empty frame).
- **WAL segment** — `{data_dir}/wal/0000000000000000.wal`, append on restart
  (`fopen` no longer truncates; a directory path resolves to that segment).
- **Replay** — restore log + HardState, skip entries at or below LMDB
  `meta.applied_index`, apply the gap so a crash between WAL sync and MVCC is repaired.
- **Txn writes** — each Put/DeleteRange in a Txn is proposed like a standalone
  write (preserves interleaved Range semantics). Tag-3 batch encoding is available
  for packing consecutive writes; nesting is rejected.

## Previously done (auth data plane)

- Opaque simple tokens, per-request TCP token, RBAC prefix match, fail-closed,
  constant-time password compare, AuthEnable requires `root`, LMDB `auth` bucket.

## Still unimplemented

### Reliability (cluster correctness)

| Gap | Why it matters | Suggested approach |
|-----|----------------|--------------------|
| Lease persistence | Restarts drop TTLs; `reindex_from_store` uses a default TTL | Persist lease id/ttl/remaining/keys in an LMDB `lease` bucket; restore before accepting traffic |
| Joint-consensus membership + learner promote | `MemberPromote` is a no-op | Raft ConfChange V2; persist members bucket |
| Snapshot → WAL truncation | Unbounded WAL growth | After `snapshot-count` applies, write snap, `wal.Release`, drop old segments |
| Nested Txn (`RequestTxn`) | etcd allows nested transactions | Fail-closed today; execute recursively with the same `MaxTxnOps` budget |
| Lease expiry / revoke still local deletes | Followers do not see expiry deletes via raft | Propose DeleteRange (or per-key Delete) from the expire callback |

### Security / ops

| Gap | Why it matters | Suggested approach |
|-----|----------------|--------------------|
| TLS on client/peer accept | `--cert-file` is no-op; traffic is plaintext | Wire `libcetcd_tls` into `on_client_conn_` / peer TCP; ALPN `h2` later. Keep plaintext as default. |
| bcrypt / JWT `--auth-token` | SHA-256 is fast but not password-hashing; JWT unused | Keep simple tokens as default (hot-path cheap). Optional bcrypt at Authenticate only. |
| `--max-request-bytes` / `--quota-backend-bytes` | DoS / disk fill. Client buffer is currently 64 KiB | Growable read buffer capped at `max_request_bytes` (etcd default 1.5 MiB); NOSPACE alarm when LMDB size exceeds quota. |
| pprof CPU profile quality | `/debug/pprof/profile` is coarse | Keep off the Raft/reactor hot path; sample in a worker. |

### Wire compatibility

| Gap | Why it matters | Suggested approach |
|-----|----------------|--------------------|
| HTTP/2 gRPC accept path | Official `etcdctl` / Go clients cannot connect | After TCP accept, detect preface `PRI * HTTP/2`; feed `libcetcd_http2`; map `:path` + data frames to `cetcd_v3rpc_dispatch_ex` with `authorization` metadata as token. Keep custom frames for `cetcdctl`. |
| `rafthttp` over HTTP/2 | Peer protocol is length-prefixed TCP | Second listener; not required for correctness of in-house Raft. |
| LeaseKeepAlive / Snapshot / RangeStream as true streams | Unary-shaped today | Same Watch writer callback model; no extra coroutines. |

### Tests / tooling

| Gap | Notes |
|-----|-------|
| `tests/fuzz/` empty | Add libFuzzer targets for WAL decode, protobuf RPC parse, auth token parse. |
| ThreadSanitizer in CI | `libtsan` not installed in some environments. |

## Non-goals (unchanged)

etcd v2 API, gRPC-gateway, `etcdutl`, gRPC reflection.
