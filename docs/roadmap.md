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
  so libuv keeps the fd; `cetcdctl --cacert` (or `--cert`/`--key`) speaks TLS.
  `--insecure` skips verify; `--insecure-transport` mixed with cert flags fail-closes.
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
- **Alarm via Raft** — apply tag 24 (`action` 1/2 + type 1/2 + member id).
  GET stays a local read. Missing type or not-leader fail-closes. Apply is
  idempotent so WAL replay after LMDB already holds the flag succeeds.
  Quota NOSPACE proposes the same entry so followers share the alarm.
- **Alarm persistence** — NOSPACE/CORRUPT live in the LMDB `alarm` bucket.
  Activate (including quota NOSPACE) and disarm rewrite the table; restart
  reloads it. A truncated blob is ignored (fail-closed empty table).
- **`--max-txn-ops`** — Txn `max(compare, success, failure)` is capped (default
  128, hard max 128 for the C stack). Larger N fail-closes at start.
- **`cetcdctl` TLS** — `--cacert` and `--cert`/`--key` wrap the custom-frame
  client in a blocking TLS handshake (no ALPN, so the server keeps the
  length-prefixed path). Missing files, cert-without-key, verify failure, and
  `--insecure-transport` mixed with cert flags fail-close. `--insecure` skips
  verify when TLS is on. Plaintext remains the default.
- **`--cipher-suites`** — comma-separated IANA or OpenSSL names applied to
  client and peer TLS contexts. TLS 1.3 IANA names (`TLS_AES_*`,
  `TLS_CHACHA20_*`) go to `SSL_CTX_set_ciphersuites`; TLS 1.2 names stay on
  `SSL_CTX_set_cipher_list`. An unknown suite, an empty list, or the flag
  without `--cert-file`/`--peer-cert-file` fail-closes at start. A TLS 1.3-only
  list raises the min protocol to TLS 1.3 so TLS 1.2 is not left at the OpenSSL
  default; a TLS 1.2-only list caps the max protocol at TLS 1.2.
- **`--max-call-send-msg-size` / `--max-call-recv-msg-size`** — cap the custom-frame
  payload (not the path header). `0` or a non-integer fail-closes at parse.
  An oversized send is not written; an oversized recv is not truncated.
- **`https://` listen URLs** — `--listen-client-urls https://...` without
  `--cert-file`, `--listen-peer-urls https://...` without `--peer-cert-file`,
  or `cetcdctl --endpoints https://...` without `--cacert`/`--insecure` fail
  closed instead of speaking plaintext. `--insecure-transport` mixed with
  an https endpoint also fails. A non-port in `--listen-client-urls` used to
  bind port `0`; that now fails at parse. The same check applies to
  `--listen-peer-urls` and to `--initial-cluster` peer URLs.
- **`--initial-cluster-state` / `--force-new-cluster`** — `new` (or omitted)
  bootstraps. `existing` restarts from cluster evidence (`cluster_token`,
  `data.mdb`, WAL, or `snapshot.kv`) or joins with `--initial-cluster` peers
  (does not campaign). A blank dir without peers or a snapshot fail-closes.
  `--force-new-cluster` keeps MVCC and drops all peers except self (not a
  wipe); empty dir fail-closes. `cetcdctl snapshot restore
  --initial-cluster-state` is `new` or `existing`.
- **`--initial-cluster https://`** — a peer URL with an https scheme requires
  `--peer-cert-file`. Stripping the scheme and dialing plaintext is fail-open.
  Member ids must be `> 0`; an etcd-style name used to become Raft id `0` via
  `atol`.
- **Unknown server flags** — a typo or an unimplemented etcd flag such as
  `--wal-dir` fails at parse instead of starting with the option ignored.
- **`--log-outputs`** — `stderr`/`stdout` (and `/dev/std{err,out}`), a file
  path (append), or `journal`/`syslog` (unix dgram). Mixed comma-lists and
  open failure fail-close (no silent stderr).
- **`--discovery-srv`** — DNS SRV lookup is not implemented. The flag used to
  be ignored while the client still used `--host`/`--endpoints` (default
  127.0.0.1:2379). It now fails at parse.
- **`cetcdctl --keepalive-time` / `--keepalive-timeout`** — TCP `SO_KEEPALIVE`
  with `TCP_KEEPIDLE` / `TCP_KEEPINTVL`. Invalid durations and timeout without
  time fail at parse. Omitted flags keep the OS default.
- **`cetcdctl --command-timeout`** — integer seconds or Go duration. A typo
  used to become “no timeout”; that now fails at parse. `0` stays none.
- **`cetcdctl --dial-timeout`** — `0..86400` seconds (optional `s`). A typo
  used to become “no timeout” via `atoi`; that now fails at parse. `0` stays
  none.
- **`cetcdctl --port`** — `1..65535`. A typo used to connect to port `0`; that
  now fails at parse.
- **`cetcdctl --endpoints` / `--endpoint`** — port `1..65535`. A typo
  used to connect to port `0` via `atoi`; that now fails at parse.
- **`cetcdctl check datascale --load`** — must be `> 0`. A typo or `0` used
  to become the silent default 10000 via `atoi`; that now fails at parse.
  Omitted still defaults to 10000.
- **`cetcdctl lock --ttl` / `elect --ttl`** — must be `> 0`. A leftover such as
  `60foo` used to become `60` via `atoi`; that now fails at parse. Omitted
  still defaults to 60.
- **`cetcdctl lease keepalive --interval`** — must be `> 0`. Leftover text such
  as `5foo` used to become `5` via `atoi`; that now fails at parse. Omitted
  still uses ttl/2.
- **`cetcdctl lease grant TTL`** — must be `> 0`. A typo used to grant TTL `0`
  via `atol`; that now fails at parse. `--lease-id` must be hex; leftover text
  used to become id `0`.
- **`cetcdctl lease revoke` / `timetolive` / `keepalive` ID** — must be `> 0`.
  A typo used to become lease id `0` via `atol`; that now fails at parse.
- **`cetcdctl member remove` / `update` / `promote` ID** — hex integer `> 0`.
  `atol` used to take a decimal prefix (`8e9a…` → `8`) or id `0`; that now
  fails at parse.
- **`cetcdctl move-leader TARGET_ID`** — hex integer `> 0`. `atol` used to
  take a decimal prefix or id `0`; that now fails at parse.
- **`cetcdctl compact REV`** — must be `> 0`. Leftover text such as `10foo`
  used to compact to revision `10` via `strtoll`; that now fails at parse.
- **`cetcdctl get --rev` / `--limit` / `--*-mod-rev` / `--*-create-rev`** —
  integer `>= 0`. Leftover text used to query a truncated revision via `atol`;
  that now fails at parse. `0` stays current / unlimited.
- **`--grpc-keepalive-time` / `--grpc-keepalive-timeout`** — TCP keepalive on
  accepted client sockets, accepted peer sockets, and outbound Raft dials
  (`uv_tcp_keepalive_ex`). Timeout without time, or a non-duration value, fail
  at parse. `--grpc-keepalive-min-time` stays a no-op (not TCP-mappable) but a
  non-duration value now fails at parse.
  `--grpc-keepalive-permit-without-stream` stays a no-op (not TCP-mappable) but
  a non-boolean value now fails at parse. A bare flag is accepted.
  Other `--grpc-keepalive-*` stay a no-op and do not swallow a following flag
  such as `--help`.
- **`--auto-tls` / `--peer-auto-tls`** — mint self-signed ECDSA P-256 into
  `{data-dir}/fixtures/` when the matching cert flag is empty. Reuse if both
  files exist; one-without-the-other fail-closes. Requires `--data-dir`.
  Existing `--cert-file` / `--peer-cert-file` skip mint.
- **`--advertise-client-urls` / `--initial-advertise-peer-urls`** — MemberList
  self `clientURLs` / `peerURLs`. Omitted flags default from the listen address
  (scheme follows TLS). `https://` without the matching cert file fail-closes.
  Peers omit `clientURLs` rather than advertising a hardcoded 2379.
- **`--name`** — MemberList self `name`. Omitted or empty stays `default`.
  The flag used to be logged only while every member was named `default`.
- **`--logger`** — `zap` or `capnslog` are accepted (built-in logger). Any
  other type used to be ignored while still starting; that now fails at parse.
- **`--log-level`** — `trace`/`debug`/`info`/`warn`/`error` (etcd aliases
  `warning`/`dpanic`/`panic`/`fatal`). A typo used to become `info`; that now
  fails at parse.
- **`--log-format`** — `json` or `text` (etcd `console` = text). A typo used
  to become text; that now fails at parse.
- **`--port`** — `1..65535`. A typo used to bind port `0` (ephemeral); that
  now fails at parse.
- **`--peer-port`** — `1..65535`. A typo used to bind the Raft port on `0`;
  that now fails at parse.
- **`--metrics-port`** — `0..65535` (`0` disables). A typo used to disable
  metrics via `atoi`; that now fails at parse.
- **`--node-id`** — must be `> 0`. A typo used to become Raft id `0` via
  `atol`; that now fails at parse.
- **`--election-tick`** — must be `> 0`. A typo or `0` used to become `10`;
  that now fails at parse.
- **`--heartbeat-tick`** — must be `> 0`. A typo or `0` used to become `1`;
  that now fails at parse.
- **`--snapshot-count`** — must be `> 0`. A typo or `0` used to become the
  default 10000; that now fails at parse. Omitted still defaults to 10000.
- **`--quota-backend-bytes`** — integer bytes (`0` = unlimited). A typo used
  to become unlimited via `strtoull`; that now fails at parse.
- **`--max-txn-ops`** — `1..128`. A typo or `0` used to become the default
  128; that now fails at parse. Omitted still defaults to 128.
- **`--max-request-bytes`** — must be `> 0`. A typo or `0` used to become the
  default 1.5 MiB; that now fails at parse. Omitted still defaults.
- **`--bcrypt-cost`** — `0` (SHA-256) or `4..31`. A typo used to become SHA-256
  via `atoi`; that now fails at parse.
- **`--initial-cluster-token`** — written to `{data-dir}/cluster_token` on first
  start. A later start with a different token fail-closes so a data dir is not
  reused as a different cluster. Omitted flag stays a no-op.
  `cetcdctl snapshot restore --initial-cluster-token` writes the same file;
  a mismatch without `--force` fail-closes.
- **`--wal-dir`** — dedicated WAL directory (default `{data-dir}/wal`). Empty
  path fail-closes. Set without `--data-dir` fail-closes at start. Operators
  can put the fsync-heavy WAL on a separate NVMe without moving LMDB.
- **`--log-outputs` file / journal** — `stderr`/`stdout`/`/dev/std{err,out}`,
  a file path (append), or `journal`/`syslog` (unix dgram to
  `/run/systemd/journal/dev-log` then `/dev/log`). Mixed comma-lists and
  open failure fail-closed (no silent stderr). Windows has no unix dgram
  journal and fail-closes.
- **`--initial-cluster-state existing`** — restart from cluster evidence, or
  join from a blank dir when `--initial-cluster` lists peers (follower, no
  campaign).   `snapshot.kv` is imported into empty MVCC (corrupt blob
  fail-closes). After WAL compaction the leader sends `MsgSnap` to a
  joiner whose `next_idx` is at or below the compacted index. While the
  log is still live the leader sends `App` from `next_idx` instead.
- **`--force-new-cluster`** — disaster recovery: keep MVCC, drop every peer
  except self, clear joint config, then campaign as a single voter. Empty
  dir fail-closes (not a data wipe).
- **`--auto-tls` / `--peer-auto-tls`** — mint `{data-dir}/fixtures/client.{crt,key}`
  or `peer.{crt,key}` (ECDSA P-256, SAN localhost + 127.0.0.1). Reuse when
  both files exist. Key mode 0600 on POSIX.
- **`--discovery-srv` / `--discovery-srv-name`** — DNS SRV bootstrap.
  Server looks up `_etcd-server[-ssl][-name]._tcp.<domain>` and fills
  `--initial-cluster` with stable FNV-1a peer ids. Client looks up
  `_etcd-client[-ssl][-name]._tcp.<domain>` (SSL first when TLS is on).
  Invalid domain, 0 records, pointer loops, port 0, and id collisions
  fail-closed. Mixed with `--initial-cluster` / `--endpoints` fail-closes.
  `cetcdctl version` stays local (no lookup).
- **`cetcdctl --endpoints` failover** — the comma list is no longer first-only.
  Connect tries each endpoint in order (hostname via `getaddrinfo`). All
  failures fail-closed.
- **Empty-dir join / snapshot restore `existing`** — `cetcdctl snapshot restore
  --initial-cluster-state existing` writes `snapshot.kv`. Server start with
  `existing` imports that blob into empty MVCC, or starts as a follower when
  `--initial-cluster` has peers. Truncated `snapshot.kv` fail-closes. A blank
  dir with neither snapshot nor peers still fail-closes.
- **Live raft `MsgSnap` catch-up** — after WAL compaction a new peer's
  `next_idx` is the compacted index. The leader sends one `MsgSnap` (KV blob
  in context) per in-flight window; `snapshot==0` or a corrupt blob
  fail-closes. The follower installs the dummy last-included index and acks
  `MsgSnapStatus`.
- **Uncompacted App catch-up** — a joiner whose `next_idx` is still in the
  live log gets an `App` batch from `next_idx` (capped by `max_size_per_msg`
  and 256 entries). Heartbeat retries one in-flight App; `AppResp` reject
  uses the follower's last-index hint so a blank joiner is not probed
  decrement-by-one. A missing prev that is not the snapshot index is
  fail-closed (no hole). After `MsgSnapStatus` the suffix is sent the same way.
- **`cetcdctl snapshot restore --initial-cluster` / `--name` /
  `--initial-advertise-peer-urls`** — persist `{data-dir}/initial-cluster`,
  `name`, `initial-advertise-peer-urls`, and `initial-cluster-state`. Server
  start loads them when the CLI omits the flag. A corrupt spec, empty value,
  or CLI/file mismatch fail-closes. `initial-cluster` counts as cluster
  evidence for `--initial-cluster-state existing`.
- **Hash / HashKV CRC32C** — `Maintenance/Hash` and `HashKV` hash key+value
  pairs (Castagnoli, key order) at the requested revision instead of
  `revision * constant`. Same revision with different contents no longer
  collides. Compacted / future revisions stay fail-closed.
- **Snapshot CTS2 CRC32C** — `snapshot save` writes `CTS2` + revision +
  CRC32C of the kv blob. Restore fail-closes on a mismatch unless
  `--skip-hash-check`. Legacy `CTS1` (no stored hash) still restores.
  A truncated CTS2 header fail-closes even with skip.
- **Auto compaction** — `--auto-compaction-mode periodic|revision` and
  `--auto-compaction-retention` (0 disables; periodic is a duration or
  bare hours; revision is revisions to keep). Invalid mode or retention
  fail-closes. The leader compact-proposes on tick; followers do not.
- **Linearizable Range** — default Range / Txn RequestRange fail-closes
  on a follower or when there is no leader. `serializable=true`
  (`cetcdctl get --consistency s`) reads the local store. No Raft (unit
  tests / single dispatch) still serves locally.
- **Status dbSize** — `Maintenance/Status` reports LMDB allocated pages
  as `dbSize`, used pages as `dbSizeInUse` (same source as quota), plus
  `raftAppliedIndex`, `errors` (NOSPACE/CORRUPT), and `isLearner`.
  No backend (unit tests) stays 0. `cetcdctl status` prints the new fields.
- **`--experimental-initial-corrupt-check`** — after snapshot import and
  WAL replay, HashKV the current store and compare `{data-dir}/backend.hash`.
  Missing file writes `rev hash`. Same revision with a different hash, or
  current revision below the stored one (data loss), fail-closes. A newer
  revision rewrites the file. Other `--experimental-*` stay no-ops.
  Invalid `true`/`false` fail at parse.
- **Raft PreVote** — `pre_vote` (server default on) campaigns as
  `PRE_CANDIDATE` and sends `MsgPreVote` at `term+1` without incrementing
  the local term. A stale log or a live-leader lease (`check_quorum` and
  recent leader) rejects without disrupting the cluster. A majority grant
  then starts a real Vote. `TIMEOUT_NOW` (leader transfer) skips PreVote.
- **Strict MemberRemove** — removing a voter is fail-closed unless the
  remaining voters still satisfy the old quorum (`n/2+1`). The last voter
  and a 2-voter shrink are refused. Learners and a 3+ voter remove still
  work. Unknown member ids fail-closed. No cluster (unit dispatch) is
  unchanged.
- **Downgrade fail-closed** — `Maintenance/Downgrade` VALIDATE of
  `cetcd_version()` succeeds (already at this binary). ENABLE, CANCEL,
  and any other version fail-closed: the on-disk format cannot change
  and no downgrade is ever in progress. A fake `0.1.0` ENABLE no
  longer returns success.
- **`--experimental-corrupt-check-time`** — Go duration (`0` disables).
  After the interval the tick HashKVs the store and compares
  `{data-dir}/backend.hash` (same rules as the initial check). A
  mismatch raises the CORRUPT alarm. Missing value or a non-duration
  fail at parse (no longer swallowed by `--experimental-*`). Other
  `--experimental-*` stay no-ops.
- **`--experimental-compaction-batch-limit`** — integer (`0` = unlimited).
  Auto-compact proposes at most N revisions past the current compact
  boundary per tick, then drains the remainder on later ticks. Missing
  value or leftover text fail at parse (no longer swallowed). Other
  `--experimental-*` stay no-ops.
- **`--experimental-watch-progress-notify-interval`** — Go duration
  (`0` = default 10s). Watch `progress_notify` emits on that period
  (100ms ticks, rounded up). Missing value or a non-duration fail at
  parse (no longer swallowed). Other `--experimental-*` stay no-ops.
- **`--experimental-warning-apply-duration`** — Go duration (`0`
  disables; omitted default 100ms). A Raft apply slower than the
  threshold logs a warning. Missing value or a non-duration fail at
  parse (no longer swallowed). Other `--experimental-*` stay no-ops.
- **`--experimental-max-learners`** — integer (`0` = no new learners;
  omitted default 1). Learner `MemberAdd` fail-closes once the cluster
  already has that many learners. Voter add is unchanged. Missing
  value or leftover text fail at parse (no longer swallowed). WAL
  apply of an already-committed add still applies. Other
  `--experimental-*` stay no-ops.
- **`--experimental-compaction-sleep-interval`** — Go duration (`0` =
  no extra wait). After an auto-compact batch the next batch waits at
  least that long (ticks are skipped; the apply loop is not blocked).
  Missing value or a non-duration fail at parse (no longer swallowed).
  Other `--experimental-*` stay no-ops.

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
