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
  so libuv keeps the fd; `cetcdctl` is still a plaintext client.
- **Peer TLS on outbound `peer_tx_`** — when `--peer-cert-file` is set, Raft
  send does a client-method memory-BIO handshake after TCP connect, then
  `SSL_write` before `uv_write`. Missing CA skips verification (self-signed
  clusters); a configured `--peer-trusted-ca-file` verifies the remote.
  ALPN `h2` is not advertised yet.
- **bcrypt password hashing** — default remains SHA-256 (cheap, existing
  records). `--bcrypt-cost N` (4..31) hashes new passwords with `$2b$` via
  libcrypt; verify accepts both encodings. `--auth-token simple` is the
  default; `--auth-token jwt,sign-method=HS256|RS256|ES256,priv-key=PATH[,ttl=5m]`
  issues JWTs (username/revision/exp). Other sign-methods fail closed.
- **`--max-request-bytes` / `--quota-backend-bytes`** — client read buffer
  grows up to `max-request-bytes` (default 1.5 MiB); a claimed frame larger
  than the cap closes the connection. Puts fail closed with NOSPACE when
  LMDB size is at or above `quota-backend-bytes` (0 = unlimited). Deletes
  and compact Delete batches still apply so operators can recover space.
- **pprof CPU profile** — `/debug/pprof/profile` collects on a libuv worker
  (`uv_queue_work`) so the Raft reactor is not stalled. Linux samples
  on-CPU RIP/PC via `ITIMER_PROF`/`SIGPROF`; concurrent collections return
  409. Output is folded-stack text (not protobuf). Heap and coroutine
  endpoints stay instant.
- **HTTP/2 gRPC accept** — client connections that send the `PRI * HTTP/2`
  preface are demuxed from `cetcdctl` frames and fed to nghttp2. Unary
  `:path` + DATA map to `cetcd_v3rpc_dispatch_ex`; `authorization` is the
  bearer token. Responses use gRPC trailers (`grpc-status`). Watch is a
  true bidi stream (create DATA without END_STREAM; events as extra DATA;
  client END_STREAM is a send half-close). LeaseKeepAlive is the same bidi
  pattern (one DATA response per keepalive). Snapshot is a server stream
  (remaining>0 header DATA, then remaining=0 blob, then trailers).
  RangeStream is a server stream (`more=true` prelude, then the Range
  payload, then trailers). Peer listen demuxes the HTTP/2 preface and
  accepts `POST /raft` (one `cetcd_msg_encode` body → Raft step → 204).
  Client TLS (`--cert-file`) selects ALPN
  `h2` when the peer offers it; clients that omit ALPN still handshake
  (custom-frame TLS). A non-`h2` ALPN offer is fail-closed. Peer TLS
  advertises ALPN `h2`; inbound HTTP/2 POST `/raft` steps a framed raft
  message and replies 204. Outbound `peer_tx_` offers ALPN `h2` and, when
  negotiated, POSTs each queued frame to `/raft` instead of the 4-byte
  prefix. A handshake without `h2` keeps the length-prefixed send path.
- **libFuzzer harnesses** — `tests/fuzz/` covers WAL record decode, Range/Put/KV
  protobuf unpack, and `--auth-token` spec plus bearer lookup. CTest runs a
  smoke driver; `CETCD_BUILD_FUZZ=ON` (clang) builds the fuzzer binaries.
- **ThreadSanitizer in CI** — Linux clang Debug job uses `-DCETCD_SANITIZERS=thread`.
  Configure fail-closes if libtsan/compiler-rt cannot link. Forked live tests are
  excluded (`ctest -E integration_server`); Ubuntu installs `libtsan1`.
- **Compact via Raft** — `KV/Compact` encodes apply tag 11 (revision varint).
  Not-leader or a future / already-compacted revision is fail-closed. Apply is
  idempotent so WAL replay after LMDB already compacted that revision succeeds.
- **LeaseGrant via Raft** — apply tag 12 (`id` + `ttl` varints). The leader
  reserves an id (or the client-chosen id) and proposes; already-exists and
  not-leader are fail-closed. Apply is idempotent for WAL replay.
- **LeaseKeepAlive via Raft** — apply tag 13 (`id` + granted `ttl`). Missing
  leases still return TTL=0 without a proposal; not-leader with a live lease
  is fail-closed. Apply no-ops if the lease is already gone.
- **Auth UserAdd via Raft** — apply tag 14 (name + password hash, never
  plaintext). Duplicate names fail closed before propose. Apply is idempotent
  for WAL replay and persists the LMDB `auth` bucket.
- **AuthEnable / AuthDisable via Raft** — apply tag 15 (`0` disable / `1`
  enable). Enable without `root` is fail-closed before propose. Apply is
  idempotent, persists the `auth` bucket, and revoke-all-tokens on disable.
- **Auth UserDelete via Raft** — apply tag 16 (username). Missing names fail
  closed before propose. Apply is idempotent for WAL replay and persists the
  LMDB `auth` bucket.
- **Auth RoleAdd via Raft** — apply tag 17 (role name). Duplicate names fail
  closed before propose. Apply creates the default readwrite `/` permission,
  is idempotent, and persists the `auth` bucket.
- **Auth RoleDelete via Raft** — apply tag 18 (role name). Missing names fail
  closed before propose. Apply is idempotent for WAL replay and persists the
  `auth` bucket.
- **Auth UserGrantRole via Raft** — apply tag 19 (username + role). Missing
  user or role fail closed before propose. Apply is idempotent if already
  granted and persists the `auth` bucket.
- **Auth UserRevokeRole via Raft** — apply tag 20 (username + role). A missing
  binding fail-closes before propose. Apply is idempotent if already revoked.
- **Auth RoleGrantPermission via Raft** — apply tag 21 (role + key + perm
  type 0/1/2). Missing role fail-closes before propose. Apply overwrites the
  role permission and persists the `auth` bucket.
- **Auth RoleRevokePermission via Raft** — apply tag 22 (role name + optional
  key). Missing role fail-closes before propose. A key that does not match the
  role prefix fail-closes without clearing other perms. Apply is idempotent if
  the role or matching prefix is already gone.
- **Auth ChangePassword via Raft** — apply tag 23 (name + password hash, never
  plaintext). Missing user fail-closes before propose. Apply overwrites the
  hash, revokes simple tokens, and is idempotent if the user is already gone.
- **Alarm persistence** — NOSPACE/CORRUPT live in the LMDB `alarm` bucket.
  Activate (including quota NOSPACE) and disarm rewrite the table; restart
  reloads it. A truncated blob is ignored (fail-closed empty table).
- **`--max-txn-ops`** — Txn `max(compare, success, failure)` is capped (default
  128, hard max 128 for the C stack). Larger N fail-closes at start.

## Previously done (auth data plane)

- Opaque simple tokens, per-request TCP token, RBAC prefix match, fail-closed,
  constant-time password compare, AuthEnable requires `root`, LMDB `auth` bucket.

## Still unimplemented

### Reliability (cluster correctness)

None remaining in this pass.

### Security / ops

None remaining in this pass.

### Wire compatibility

None remaining in this pass.

### Tests / tooling

None remaining in this pass.

## Non-goals (unchanged)

etcd v2 API, gRPC-gateway, `etcdutl`, gRPC reflection.
