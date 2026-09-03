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
- **Lease persistence** — Grant/KeepAlive/Revoke/expire write the LMDB `lease`
  bucket (id + granted TTL + wall-clock deadline). Restart restores remaining TTL
  before accepting traffic; keys are reattached from MVCC.
- **Learner promote** — members carry `is_learner`; `MemberPromote` is fail-closed
  (missing / already-voter → empty frame). Raft learners receive logs but do not
  vote or count toward quorum, so a single voter plus learners can still commit.
- **Snapshot → WAL truncation** — after `snapshot-count` applies (default 10000),
  rewrite the WAL segment to a `SNAPSHOT` record plus HardState, then compact the
  in-memory raft log. Fail-closed: a failed rewrite leaves the old segment intact.
  Restart restores a dummy last-included index from the snapshot record and
  replays only the suffix.
- **Member persistence** — MemberAdd/Remove/Promote/Update encode compact apply
  tags, go through Raft, and persist the LMDB `members` bucket before mutating
  memory. Restart loads peers (and Raft ids) before campaign so a MemberAdd
  survives process restart.
- **Joint-consensus ConfChange V2** — voter add/promote/remove snapshots C_old
  and requires a majority of both C_old and C_new until incoming voters have
  the joint-index entry; the leader then proposes `LEAVE_JOINT`. Overlapping
  voter changes fail closed. Joint C_old is persisted (members key 0) so a
  restart mid-transition keeps both quorums.
- **Nested Txn** — `RequestTxn` executes recursively (each level still
  `max(compare,success,failure) ≤ 128`). Depth is capped at 16 to bound C
  stack. Unknown RequestOp tags stay fail-closed.
- **Lease expiry / revoke via Raft** — `LeaseRevoke` encodes apply tag 10
  (delete attached keys, then drop the lease). Leader expire proposes compact
  Deletes; followers do not delete locally. Not-leader is fail-closed.
- **TLS on client/peer accept** — `--cert-file`/`--key-file` (and peer
  equivalents) load OpenSSL server contexts at start. Traffic stays plaintext
  when omitted. Cert without key, missing files, or `--client-cert-auth`
  without a CA fail closed (no silent plaintext). Handshake uses memory BIOs
  so libuv keeps the fd; `cetcdctl` is still a plaintext client. Outbound
  `peer_tx_` remains plaintext this pass. ALPN `h2` is not advertised yet.

## Previously done (auth data plane)

- Opaque simple tokens, per-request TCP token, RBAC prefix match, fail-closed,
  constant-time password compare, AuthEnable requires `root`, LMDB `auth` bucket.

## Still unimplemented

### Reliability (cluster correctness)

None remaining in this pass.

### Security / ops

| Gap | Why it matters | Suggested approach |
|-----|----------------|--------------------|
| bcrypt / JWT `--auth-token` | SHA-256 is fast but not password-hashing; JWT unused | Keep simple tokens as default (hot-path cheap). Optional bcrypt at Authenticate only. |
| `--max-request-bytes` / `--quota-backend-bytes` | DoS / disk fill. Client buffer is currently 64 KiB | Growable read buffer capped at `max_request_bytes` (etcd default 1.5 MiB); NOSPACE alarm when LMDB size exceeds quota. |
| pprof CPU profile quality | `/debug/pprof/profile` is coarse | Keep off the Raft/reactor hot path; sample in a worker. |
| Peer TLS on outbound `peer_tx_` | Accept path encrypts inbound; Raft send is still plaintext | Client-method memory-BIO handshake on `peer_tx_connect_`, then `SSL_write` before `uv_write`. |

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
