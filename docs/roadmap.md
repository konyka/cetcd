# cetcd remaining work

> Living list of gaps versus etcd v3.5. Items are ordered by
> **security → reliability → wire compatibility → performance extras**.
> This pass implemented data-plane auth (tokens, RBAC, persist).

## Done in this pass (auth data plane)

Performance-first, fail-closed design:

- **Opaque simple tokens** — 16 random bytes as 32 hex chars, O(1) hashmap
  lookup, 5-minute TTL, lazy expiry. No JWT parse on the hot path.
- **Per-request token** on the custom TCP frame (`flags & 0x02`), so the
  reactor stays single-threaded and does not bind identity to a connection.
- **RBAC prefix match** — username `root` (or role `root`) is superuser;
  otherwise the key must start with a granted role prefix.
- **Fail-closed** — missing/expired token, unknown user, or failed prefix
  match returns an empty RPC frame (same as other domain errors).
- **Constant-time password compare** — XOR-accumulate, no early exit.
- **AuthEnable requires `root`** (etcd). Password change / AuthDisable
  revokes tokens.
- **LMDB persist** of users/roles/enabled in one txn (`auth` bucket),
  loaded on `cetcd_server_start`.

## Still unimplemented

### Reliability (cluster correctness)

| Gap | Why it matters | Suggested approach |
|-----|----------------|--------------------|
| KV writes apply directly to MVCC, not via Raft propose/commit | Multi-node puts diverge; leader crash can lose acked writes | Propose a compact entry (tag+key+val) from Put/Delete/Txn; apply only in `process_ready_` after WAL sync. Keep linearizable reads as leader-only. |
| WAL replay of applied entries on restart | Intermediate revisions are lost; only LMDB live keys reload | On start, decode WAL from snapshot index and re-apply NORMAL entries that are ahead of MVCC revision. |
| Lease persistence | Restarts drop TTLs; `reindex_from_store` uses a default TTL | Persist lease id/ttl/remaining/keys in an LMDB `lease` bucket; restore before accepting traffic. |
| Joint-consensus membership + learner promote | `MemberPromote` is a no-op | Raft ConfChange V2; persist members bucket. |
| Snapshot → WAL truncation | Unbounded WAL growth | After `snapshot-count` applies, write snap, `wal.Release`, drop old segments. |
| Nested Txn (`RequestTxn`) | etcd allows nested transactions | Fail-closed today; execute recursively with the same `MaxTxnOps` budget. |

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
