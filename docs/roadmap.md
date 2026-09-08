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
- **Snapshot → WAL truncation** — after `snapshot-count` applies (default 100000),
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
  without a CA fail closed (no silent plaintext). `--trusted-ca-file` also
  requires a client certificate (etcd `TrustedCAFile`; `--client-cert-auth=false`
  does not opt out). `--peer-trusted-ca-file` does the same on peer accept.
  Handshake uses memory BIOs
  so libuv keeps the fd; `cetcdctl --cacert` (or `--cert`/`--key`) speaks TLS.
  `--insecure` skips verify; `--insecure-transport` mixed with cert flags fail-closes.
- **Peer TLS on outbound `peer_tx_`** — when `--peer-cert-file` is set, Raft
  send does a client-method memory-BIO handshake after TCP connect, then
  `SSL_write` before `uv_write`. `--peer-client-cert-file` / `--peer-client-key-file`
  override the outbound identity (omitted uses the listen pair). Missing CA
  skips verification (self-signed clusters); a configured
  `--peer-trusted-ca-file` verifies the remote. ALPN `h2` is advertised.
- **bcrypt password hashing** — default remains SHA-256 (cheap, existing
  records). `--bcrypt-cost N` (4..31) hashes new passwords with `$2b$` via
  libcrypt; verify accepts both encodings. `--auth-token simple` is the
  default; `--auth-token jwt,sign-method=HS256|RS256|ES256,priv-key=PATH[,ttl=5m]`
  issues JWTs (username/revision/exp). Other sign-methods fail closed.
  `--auth-token-ttl SEC` sets simple-token lifetime (default 300s; `> 0`;
  leftover text fail-closes). JWT `ttl=` in `--auth-token` still wins.
- **`--max-request-bytes` / `--quota-backend-bytes`** — client read buffer
  grows up to `max-request-bytes` (default 1.5 MiB); a claimed frame larger
  than the cap closes the connection. Puts fail closed with NOSPACE when
  LMDB size is at or above `quota-backend-bytes` (`0` / omitted = etcd 2GiB).
  Deletes
  and compact Delete batches still apply so operators can recover space.
- **`--max-concurrent-streams`** — integer `> 0` advertised as HTTP/2
  `SETTINGS_MAX_CONCURRENT_STREAMS`. Omitted omits the SETTINGS entry
  (nghttp2 default). `0` or leftover text fail-closes.
- **pprof CPU profile** — `/debug/pprof/profile` collects on a libuv worker
  (`uv_queue_work`) so the Raft reactor is not stalled. Linux samples
  on-CPU RIP/PC via `ITIMER_PROF`/`SIGPROF`; concurrent collections return
  409. Output is folded-stack text (not protobuf). Heap and coroutine
  endpoints stay instant. `--enable-pprof` (omitted default off) is required
  or those paths 404.
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
  `http://[::1]:2379` leftover-safe-parses and binds IPv6; leftover
  `[::1]:2379foo` fail-closes.
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
  path (append), or `journal`/`syslog`/`systemd/journal` (unix dgram).
  etcd `default` fail-closes (zap does not support it; do not create a
  file named `default`). `=` form is accepted. Mixed comma-lists and
  open failure fail-close (no silent stderr).
- **`--discovery-srv`** — DNS SRV lookup is not implemented. The flag used to
  be ignored while the client still used `--host`/`--endpoints` (default
  127.0.0.1:2379). It now fails at parse.
- **`cetcdctl --keepalive-time` / `--keepalive-timeout`** — TCP `SO_KEEPALIVE`
  with `TCP_KEEPIDLE` / `TCP_KEEPINTVL`. Invalid durations and timeout without
  time fail at parse. Omitted flags keep the OS default.
- **`cetcdctl --command-timeout`** — integer seconds or Go duration. A typo
  used to become “no timeout”; that now fails at parse. `0` stays none.
  `--command-timeout=5s` and other global `--flag=value` forms are accepted
  (empty `--flag=` fail-closes). `--debug=false` does not eat the next
  argv (the subcommand). Leftover text (`10foo`) fail-closes.
- **`cetcdctl` subcommand `--flag=value`** — put/get/del/watch/compact/lease/
  lock/elect accept etcdctl equals-form (`--lease=1`, `--rev=5`,
  `--write-out=json`, `--ttl=60`, `--start-rev=5`). `--prefix=false` does
  not eat the key. `put --lease 10foo` fail-closes instead of attaching
  truncated lease 10.
- **`cetcdctl` remaining `--write-out=` / restore `=`** — status/txn/endpoint/
  check/member/auth/user/role/snapshot/alarm/hash/defrag/move-leader honor
  `--write-out=json` instead of treating it as a key. `snapshot restore
  --data-dir=DIR` and `check datascale --load=N` accept equals-form; empty
  `--flag=` and leftover `--load=10foo` fail-close. Interactive `watch
  cancel 10foo` fail-closes instead of cancelling watch 10.
- **MemberAdd/Update peer URL port** — leftover `http://host:2380foo` fail-closes
  instead of joining or updating on truncated port `2380` via `atoi`.
  `cetcdctl endpoint --cluster` leftover-safe-parses member client URLs
  (missing port → 2379) so a typo cannot connect to a truncated port.
- **pprof `?seconds=` leftover** — `/debug/pprof/profile?seconds=30foo` is
  HTTP 400 instead of profiling for truncated 30 seconds via `atoi`.
- **IPv6 listen / peer URLs** — `http://[::1]:2379` leftover-safe-parses
  and binds/connects via `uv_ip6_addr` instead of being rejected as invalid
  or silently treated as IPv4. Advertise emits brackets. `unix://` still
  fail-closes (no unix-socket listener). Leftover `[::1]:2379foo` fail-closes.
- **etcd snapshot/WAL/bolt/client-cert leftovers** — `--max-snapshots` /
  `--max-wals` (single rewritten WAL), `--client-cert-file` /
  `--client-key-file` (no outbound client TLS), and `--backend-batch-*` /
  `--backend-bbolt-freelist-type` (LMDB) fail-close as known-unsupported
  instead of looking like a typo. Accepting etcd default `5` would be a
  no-op lie.
- **`cetcdctl --dial-timeout`** — `0..86400` seconds (optional `s`). A typo
  used to become “no timeout” via `atoi`; that now fails at parse. `0` stays
  none.
- **`cetcdctl --port`** — `1..65535`. A typo used to connect to port `0`; that
  now fails at parse.
- **`cetcdctl --endpoints` / `--endpoint`** — port `1..65535`. A typo
  used to connect to port `0` via `atoi`; that now fails at parse.
- **`cetcdctl check datascale --load`** — must be `> 0`. A typo or `0` used
  to become the silent default 10000 via `atoi`; that now fails at parse.
  Omitted still defaults to 10000. `=` form is accepted.
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
  that now fails at parse. `0` stays current / unlimited. `=` form is accepted.
- **`cetcdctl watch --start-rev`** — integer `>= 0`. Leftover text used to
  start a watch at a truncated revision via `atol`; that now fails at parse.
  `0` stays from-now. `=` form is accepted.
- **`--grpc-keepalive-time` / `--grpc-keepalive-interval` /
  `--grpc-keepalive-timeout`** — TCP keepalive on accepted client sockets,
  accepted peer sockets, and outbound Raft dials (`uv_tcp_keepalive_ex`).
  `--grpc-keepalive-interval` is the etcd name for idle (same as `--grpc-keepalive-time`).
  Go durations (`2h`, `10s`) and bare seconds are accepted (`0..86400`).
  Timeout without time/interval, or a non-duration value, fail at parse.
  `--grpc-keepalive-min-time` stays a no-op (not TCP-mappable) but a
  non-duration value now fails at parse.
  `--grpc-keepalive-permit-without-stream` stays a no-op (not TCP-mappable) but
  a non-boolean value now fails at parse. A bare flag is accepted.
  Other `--grpc-keepalive-*` are unknown flags and do not swallow the next argv
  (including `--help`).
- **`--help`** is handled in the parse loop, not a pre-scan, so an invalid flag
  that appears before `--help` still fail-closes. `--config-file` is not opened
  when `--help` is present (usage still prints).
- **Client TLS** is enabled when `cert_file` is set, not only `--listen-client-urls`
  https. Accept uses the client TLS ctx whenever it was loaded.
- **Single-node campaign** in `server_new` is skipped when `data-dir` is set so
  a join can load persisted peers before electing.
- **`--auto-tls` / `--peer-auto-tls`** — mint self-signed ECDSA P-256 into
  `{data-dir}/fixtures/` when the matching cert flag is empty. Reuse if both
  files exist; one-without-the-other fail-closes. Requires `--data-dir`.
  Existing `--cert-file` / `--peer-cert-file` skip mint. `=` form is accepted
  (`--auto-tls=false` is off, not an unknown flag).
  `--self-signed-cert-validity N` sets mint lifetime in years (omitted
  default 1, etcd-compatible; `0` / leftover text fail-closes).
- **`--advertise-client-urls` / `--initial-advertise-peer-urls`** — MemberList
  self `clientURLs` / `peerURLs` as UniqueURLs comma lists (repeated protobuf
  strings). Omitted flags default from every listen URL (scheme follows TLS).
  A comma list is no longer truncated to the first URL. Mixed http/https is
  allowed (advertise does not bind). Duplicate host:port, leftover text, or
  a missing value fail-close. Any `https://` without the matching cert file
  fail-closes. Peers omit `clientURLs` rather than advertising a hardcoded 2379.
- **`--name`** — MemberList self `name`. Omitted or empty stays `default`.
  The flag used to be logged only while every member was named `default`.
  `=` form is accepted (`--name=n1`).
- **etcd `--flag=value`** — remaining space-only value flags (`--data-dir`,
  `--wal-dir`, `--snapshot-count`, `--initial-cluster`, `--cert-file`,
  `--auto-compaction-*`, `--max-txn-ops`, …) accept `=`. `--name=` empty
  fail-closes. `--initial-cluster=n1=http://host:2380` keeps the member `=`.
- **`--logger`** — `zap` or `capnslog` are accepted (built-in logger). `=`
  form is accepted. Any other type used to be ignored while still starting;
  that now fails at parse.
- **`--log-level`** — `trace`/`debug`/`info`/`warn`/`error` (etcd aliases
  `warning`/`dpanic`/`panic`/`fatal`). `=` form is accepted (`--log-level=debug`
  is the etcd 3.5 replacement for `--debug`). A typo used to become `info`;
  that now fails at parse.
- **`--log-format`** — `json` or `text` (etcd `console` = text). `=` form is
  accepted. A typo used to become text; that now fails at parse.
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
  silent default; that now fails at parse. Omitted is etcd 3.5's 100000
  (`CETCD_DEFAULT_SNAPSHOT_COUNT`), not 10000. `=` form is accepted.
- **`--quota-backend-bytes`** — integer bytes. `0` or omitted is etcd's
  2GiB default (`CETCD_DEFAULT_QUOTA_BACKEND_BYTES`), not unlimited. `=`
  form is accepted. A typo used to become unlimited via `strtoull`; that
  now fails at parse.
- **`--max-txn-ops`** — `1..128`. A typo or `0` used to become the default
  128; that now fails at parse. Omitted still defaults to 128.
- **`--max-request-bytes`** — must be `> 0`. A typo or `0` used to become the
  default 1.5 MiB; that now fails at parse. Omitted still defaults.
- **`--bcrypt-cost`** — `0` (SHA-256) or `4..31`. A typo used to become SHA-256
  via `atoi`; that now fails at parse.
- **`--auth-token-ttl`** — integer seconds `> 0` (omitted default 300). Sets
  simple-token lifetime. JWT `--auth-token …,ttl=` still wins. `0`, leftover
  text, or a missing value fail at parse (no longer an unknown flag).
- **`--initial-cluster-token`** — written to `{data-dir}/cluster_token` on first
  start. A later start with a different token fail-closes so a data dir is not
  reused as a different cluster. Omitted flag stays a no-op.
  `cetcdctl snapshot restore --initial-cluster-token` writes the same file;
  a mismatch without `--force` fail-closes.
- **`--wal-dir`** — dedicated WAL directory (default `{data-dir}/wal`). Empty
  path fail-closes. Set without `--data-dir` fail-closes at start. Operators
  can put the fsync-heavy WAL on a separate NVMe without moving LMDB.
- **`--log-outputs` file / journal** — `stderr`/`stdout`/`/dev/std{err,out}`,
  a file path (append), or `journal`/`syslog`/`systemd/journal` (unix dgram to
  `/run/systemd/journal/dev-log` then `/dev/log`). etcd `default` fail-closes.
  `=` form is accepted. Mixed comma-lists and
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
  dir fail-closes (not a data wipe). `=` form is accepted (`=false` is off).
- **`--auto-tls` / `--peer-auto-tls`** — mint `{data-dir}/fixtures/client.{crt,key}`
  or `peer.{crt,key}` (ECDSA P-256, SAN localhost + 127.0.0.1). Reuse when
  both files exist. Key mode 0600 on POSIX.
- **`--self-signed-cert-validity`** — integer years `> 0` (omitted default 1).
  Lifetime of a newly minted auto-TLS cert. Existing fixture files are
  reused unchanged. `0`, leftover text, or a missing value fail at parse
  (no longer an unknown flag). Previously minted 10-year certs.
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
  `--pre-vote` / `--pre-vote=true|false` is no longer an unknown flag
  (omitted default on; a non-bool fail-closes).
- **Strict MemberRemove** — removing a voter is fail-closed unless the
  remaining voters still satisfy the old quorum (`n/2+1`). The last voter
  and a 2-voter shrink are refused. Learners and a 3+ voter remove still
  work. Unknown member ids fail-closed. No cluster (unit dispatch) is
  unchanged. `--strict-reconfig-check` / `--strict-reconfig-check=false`
  is no longer an unknown flag (omitted default on; a non-bool
  fail-closes). WAL apply of an already-committed remove still applies.
- **`--initial-election-tick-advance`** — omitted / bare / `true` calls
  `AdvanceTicks(election_tick-1)` after Raft create so the first campaign
  is one tick away instead of a full timeout (`election_tick <= 1` is 0).
  `--initial-election-tick-advance=false` waits the full timeout. A
  non-bool fail-closes. Existing 10-tick election tests are unchanged.
- **`--heartbeat-interval` / `--election-timeout`** — integer milliseconds
  (`1..50000`; leftover text fail-closes). Omitted defaults are 100ms /
  1000ms. `election-timeout` must be `>= heartbeat-interval`. Derived
  `heartbeat_tick=1` and `election_tick=election_ms/tick_ms`; the Raft
  timer, lease tick, and Watch `progress_notify` use that period. Mixing
  with `--heartbeat-tick` / `--election-tick` fail-closes.
- **`--listen-metrics-urls`** — UniqueURLs comma list of `http://host:port`
  (port `1..65535`). Binds `/metrics` and `/health` on every URL instead
  of `{listen-addr}:{metrics-port}`. A comma list is no longer rejected.
  `https://` terminates TLS with `--cert-file` / `--auto-tls`. Duplicate
  host:port, IPv6, a missing value, leftover text, or mixing with
  `--metrics-port` fail-closes.
- **`--experimental-wait-cluster-ready`** — bool (omitted default off;
  bare / `true` waits). Client listen is delayed until Raft has a
  leader (peer listen and ticks still run). A single-node that already
  campaigned binds immediately. A non-bool fail-closes (no longer
  swallowed). Other `--experimental-*` stay no-ops.
- **`--max-concurrent-streams`** — integer `> 0`. Advertises HTTP/2
  `SETTINGS_MAX_CONCURRENT_STREAMS` on each new session. Omitted leaves
  nghttp2's default (no extra SETTINGS entry). `0`, leftover text, or a
  value above `UINT32_MAX` fail-closes (deny-all is useless).
- **`--enable-pprof`** — bool (omitted default off; bare / `true` on).
  `/debug/pprof/profile|heap|coroutines` on the metrics port require it;
  otherwise those paths 404. `/metrics` is unchanged. A non-bool
  fail-closes (no longer an unknown flag). `?seconds=N` leftover
  (`30foo`) / `0` / `>300` is HTTP 400, not a truncated `atoi` duration.
- **`GET /health`** — metrics port returns etcd JSON
  (`{"health":"true"}` or `{"health":"false","reason":"..."}`). Unhealthy
  is HTTP 503. NOSPACE / CORRUPT then `RAFT NO LEADER`.
  `?serializable=true` skips the leader check; `exclude=NOSPACE|CORRUPT`
  skips that alarm.
- **`--tls-min-version` / `--tls-max-version`** — `TLS1.2` or `TLS1.3`
  only. Omitted min is TLS1.2; omitted max is open. `TLS1.1`, leftover
  text, or min > max fail-closes. Applied after `--cipher-suites` on
  client and peer contexts.
- **`--peer-cert-allowed-cn` / `--peer-cert-allowed-hostname` /
  `--client-cert-allowed-hostname`** — comma-separated identity
  allow-lists checked after TLS handshake (CN exact; hostname
  case-insensitive, `*.example.com` is one label). Empty omitted is
  off. A restricted list requires that side's cert + CA and requires a
  peer/client certificate. Missing value fail-closes. No match closes
  the socket. Accepting the flags as a no-op is rejected.
- **`--peer-client-cert-file` / `--peer-client-key-file`** — outbound
  peer TLS identity (etcd: ClientCertFile on PeerTLSInfo). Omitted uses
  `--peer-cert-file` / `--peer-key-file`. Cert without key, key without
  cert, override without listen TLS, missing files, or a missing value
  fail-close. Accepting the flags as a no-op is rejected (a server-only
  cert would still be presented on dial).
- **`--client-crl-file` / `--peer-crl-file`** — PEM/DER CRL loaded at
  start. After handshake, a peer cert whose serial is on the list is
  closed (etcd serial compare; no peer cert is OK). CRL without that
  side's cert, a missing file, garbage PEM/DER, or a missing value
  fail-close. Accepting the flags as a no-op is rejected (a revoked
  cert would still be accepted).
- **`--version`** — print `etcd Version:` / Git SHA / C Standard / OS/Arch
  and exit (etcd parse order: before `--config-file`). A non-bool
  `--version=…` fail-closes.
- **`--config-file`** — etcd YAML map of flag names (`key: value`,
  comments, quotes, 2-space lists joined by comma). Converted to the
  same `--key value` parser as the CLI. When set, other CLI flags are
  ignored (`--help` / `--version` still win). Nested/flow maps, a
  missing file, or a missing value fail-close. Accepting the flag as a
  no-op is rejected (a YAML listen URL would be ignored).
- **`ETCD_*` environment** — etcd `SetFlagsFromEnv`. `ETCD_LISTEN_CLIENT_URLS`
  is `--listen-client-urls` when no `--config-file` / `ETCD_CONFIG_FILE`
  is set. Empty values are ignored. `ETCD_VERSION` is not `--version`.
  A CLI flag plus the matching `ETCD_*` fail-closes (etcd FATAL). A typo
  (`ETCD_LISTEN_CLIENT_URL`) becomes an unknown flag instead of a
  silent default. When a config file is set, all other `ETCD_*` are
  ignored.
- **`--listen-client-urls` / `--listen-peer-urls` lists** — etcd
  UniqueURLs comma lists bind every `http(s)://host:port` (same scheme;
  unique host:port; port `1..65535`). A hostname (`localhost`, etcd's
  default) resolves via `getaddrinfo` (IPv4 preferred) so
  `http://localhost:2379` binds instead of failing numeric-only
  `uv_ip4_addr`. Peer and metrics connect/bind use the same helper.
  Unresolvable names fail-close. A comma list is no longer parsed
  as one garbage host. Mixed http/https, a duplicate, a trailing comma,
  leftover text, or a missing value fail-close. Accepting a list as a
  single URL is rejected.
- **IPv6 `host:port` emit** — advertise URLs, MemberList peer URLs, and
  `cetcdctl endpoint {health,status,hashkv}` use `cetcd_format_host_port`
  (`[::1]:2379`, etcd `JoinHostPort`). Unbracketed `::1:2379` is not
  leftover-safe (looks like port 1) and cannot be parsed back.
- **IPv6 zone IDs** — UniqueURLs may be `[fe80::1%1]:2379` /
  `[fe80::1%eth0]:2379`. The host leftover-safe-splits `addr%zone`
  (empty zone and numeric leftover `1foo` fail-close; named zones are
  `[A-Za-z][A-Za-z0-9_.-]*`). Resolve uses `getaddrinfo` so the
  `scope_id` is honored (not stripped and bound on every interface).
  `::1foo` is still leftover-invalid. An unresolvable zone fail-closes.
- **`hashkv --rev` / `endpoint hashkv --rev`** — leftover-safe HashKV
  revision (`>= 0`; omitted / `0` = current). `10foo` fail-closes.
  A swallowed `--rev` would hash the live tree instead of the requested
  revision. Unknown leftover flags on those commands also fail-close.
- **`compact` / `hash` / `status` / `defrag` leftover flags** — unknown
  leftover flags fail-close (`hash --rev` cannot hash the live tree;
  `status --cluster` cannot report one node; `compact 10 --rev 5`
  cannot compact the wrong revision). `defrag --cluster` honors etcd's
  MemberList walk (a swallowed `--cluster` would defrag only the
  connected member). `defrag --data-dir` leftover-safe-opens the local
  LMDB and compact-copies (`--data-dir --cluster` cannot eat a flag as
  the path; `--cluster` + `--data-dir` fail-close; missing `data.mdb`
  does not create an empty env). Accepting those flags as a no-op is
  rejected.
- **`put` / `get` / `del` / `lease` leftover `--` flags** — unknown
  leftover long flags fail-close so `put --foo k v` cannot write key
  `--foo`, `get k --foo` cannot range to `--foo`, and
  `lease grant 10 --rev` cannot grant with a swallowed flag.
  `--` still starts positionals (`put -- --foo v`). Accepting those
  flags as a key or range_end is rejected.
- **`member` / `watch` / `lock` / `elect` leftover `--` flags** — unknown
  leftover flags fail-close so `member remove --force ID` cannot
  swallow `--force` and still remove, `member add --foo URL` cannot
  silently add, and `watch --foo key` cannot watch key `--foo`.
  `lock --foo name` cannot take `--foo` as the lock name (COMMAND
  after LOCKNAME may still be `--` args). `snapshot save --foo` cannot
  write a file named `--foo`. `user add --foo` / `role add --foo`
  cannot create that name. `--` still starts positionals. Accepting
  those flags as a no-op or as NAME/ID/KEY is rejected.
- **`txn` / `auth` / `downgrade` leftover `--` flags** — unknown leftover
  flags fail-close so `txn put --foo k v` cannot write key `--foo`,
  `txn put -w json k v` cannot treat `-w` as the key, and
  `auth login --foo user pass` cannot authenticate as `--foo`.
  `downgrade enable --foo VER` cannot swallow `--foo`. `--` still
  starts positionals. Accepting those flags as KEY/NAME/VERSION is
  rejected.
- **`check perf` / `check datascale` leftover `--` flags** — unknown
  leftover flags fail-close so `check perf --foo` / `check datascale
  --foo` cannot start a write load. Accepting those flags as a no-op
  is rejected.
- **`version` leftover `--` flags** — unknown leftover flags fail-close
  so `version --foo` cannot print the client version. Accepting those
  flags as a no-op is rejected.
- **`completion` leftover `--` flags** — unknown leftover flags
  fail-close so `completion bash --foo` cannot dump a script. One of
  `bash`/`zsh`/`fish` is required. Accepting those flags as a no-op is
  rejected.
- **leftover-safe `--flag VALUE`** — `cetcd_take_cli_flag_value` fail-closes
  when the next argv is a leftover `--` (`--load --prefix`,
  `--name --data-dir`) so a flag cannot become the value. `--flag=--foo`
  is still the value `--foo`. Accepting a leftover `--` value is rejected.
- **leftover-safe server VALUE takes** — remaining cetcd `--flag VALUE`
  paths use that helper so `--host-whitelist --name` /
  `--listen-client-urls --name` cannot eat a flag as the list.
  Empty `--host-whitelist=` stays allow-all. Accepting a leftover `--`
  as a Host or URL is rejected.
- **`cetcd-migrate` snap filename** — etcd names are `%016x-%016x.snap`
  (hex). Leftover-safe hex so `123foo-456.snap` cannot win latest with
  a truncated decimal term, and `…000a.snap` is index 10 (not `atoll`
  0). Accepting `atoll` truncation is rejected.
- **`cetcd-migrate` WAL filename** — etcd names are `%016x-%016x.wal`
  (hex seq, first index). Leftover-safe hex so `123foo-456.wal` cannot
  be scanned and win latest. Suffix-only `.wal` listing is rejected.
- **`cetcd-migrate` leftover `--data-dir`** — leftover-safe so
  `--data-dir --output-dir /c` cannot eat `--output-dir` as the path.
  Honors `--flag=VALUE` (empty `--flag=` is INVAL). `--verbose[=bool]`.
  Unknown leftover flags fail-close. Missing data-dir or output-dir is
  INVAL. Accepting a leftover `--` value as the path is rejected.
- **`snapshot restore --wal-dir` / `--bump-revision`** — leftover-safe
  so `--data-dir --wal-dir` cannot eat a flag as the path. `--wal-dir`
  is persisted and loaded on start. `--bump-revision` writes CTS2 at
  old+1 and advances MVCC so a new cluster cannot reuse the snapshot
  revision. `--mark-compacted` without bump fail-closes; with bump it
  compact-marks the old revision so a watch at that rev is ErrCompacted.
  Accepting those flags as a no-op is rejected.
- **`member list --linearizable`** — leftover-safe so `member list
  --linearizable --foo` cannot list members. Default is true (etcdctl).
  Encodes MemberListRequest field 1; a follower fail-closes. Dummy
  `0x00` / omitted field is serializable. Accepting the flag as a
  no-op or swallowing leftover `--` is rejected. `defrag --cluster` /
  `endpoint --cluster` leftover-safe-send the same field 1 = true so a
  follower cannot walk a stale member list.
- **`role grant-permission --from-key` / `range_end`** — leftover-safe so
  `--range-end --from-key` cannot eat a flag as the range. Encodes
  Permission.range_end; the server leftover-safe-persists and checks
  `[key, range_end)` (single 0 = FromKey). A swallowed range_end would
  grant a prefix instead of a range. Accepting `--from-key` as a no-op
  is rejected.
- **HashKV leftover-safe revision** — leftover-safe-parses
  HashKVRequest.revision so a truncated field-1 varint (`0x08` with no
  value) cannot hash the live tree. Dummy `0x00` / omitted is current.
  A leftover length-delimited field is skipped by payload, not eaten as
  a revision. Accepting a truncated `--rev` as current is rejected.
- **Compact leftover-safe revision** — leftover-safe-parses
  CompactRequest.revision so leftover length-delimited bytes cannot
  compact a different rev. A truncated field-1 varint fail-closes.
  `physical` is leftover-safe-read and ignored (already sync; not
  defrag). Dummy `0x00` / omitted is rev 0 (ErrCompacted). Accepting
  leftover payload as the revision is rejected.
- **MoveLeader leftover-safe target** — leftover-safe-parses
  MoveLeaderRequest.targetID so a truncated field-1 varint cannot look
  like a successful transfer. Leftover length-delimited bytes cannot
  steal the target. Dummy `0x00` / omitted / `0` fail-closes (etcd
  member 0 is not a transfer). Accepting a truncated target as OK is
  rejected.
- **LeaseGrant leftover-safe TTL** — leftover-safe-parses
  LeaseGrantRequest.TTL so a truncated field-1 varint cannot grant a
  60s lease. Leftover length-delimited bytes cannot steal the TTL or
  id. Dummy `0x00` / omitted / `TTL<=0` fail-closes. Accepting a
  truncated TTL as the default 60s is rejected.
- **MemberRemove leftover-safe id** — leftover-safe-parses
  MemberRemove/Promote field 1 so a truncated varint cannot look like
  a successful remove or promote. Leftover length-delimited bytes
  cannot steal the id. Dummy `0x00` / omitted / `0` fail-closes (etcd
  member 0 is not a remove). Accepting a truncated id as OK is
  rejected.
- **MemberUpdate leftover-safe** — leftover-safe-parses
  MemberUpdate field 1/2 so a truncated varint cannot look like a
  successful update. Leftover peerURL length cannot walk off the
  buffer or steal the id. Dummy `0x00` / omitted / `0` fail-closes.
  Accepting a truncated id as OK is rejected.
- **LeaseRevoke leftover-safe id** — leftover-safe-parses
  LeaseRevoke/KeepAlive/TimeToLive field 1 so a truncated varint
  cannot look like a successful keepalive or steal a revoke. Leftover
  length-delimited bytes cannot inject a fake id. Dummy `0x00` /
  omitted / `0` fail-closes KeepAlive (no ID=0 TTL=0 success frame).
  Truncated TimeToLive fail-closes (cannot look like TTL=-1).
  Accepting a leftover payload as the id is rejected.
- **Alarm leftover-safe action** — leftover-safe-parses AlarmRequest
  so leftover length-delimited bytes cannot steal ACTIVATE. A
  truncated action varint fail-closes (cannot look like GET). Dummy
  `0x00` / omitted is GET. Accepting leftover payload as the action
  is rejected.
- **Range leftover-safe revision** — leftover-safe-parses
  RangeRequest so leftover length-delimited bytes cannot steal `rev`
  or `limit`. A truncated field-4 varint fail-closes (cannot range
  the live tree). Dummy `0x00` / omitted is rev 0 (current).
  Accepting leftover payload as the revision is rejected.
- **Put leftover-safe lease** — leftover-safe-parses PutRequest so
  leftover length-delimited bytes cannot steal the lease. A truncated
  field-3 varint fail-closes (cannot look like a no-lease put). Dummy
  `0x00` / omitted is lease 0. Accepting leftover payload as the
  lease is rejected.
- **DeleteRange leftover-safe range_end** — leftover-safe-parses
  DeleteRangeRequest so leftover length-delimited bytes cannot steal
  `range_end` and turn a point delete into a range delete. A
  truncated field-2 / field-3 fail-closes (cannot look like a point
  delete or a no-prev-kv delete). Dummy `0x00` / omitted is empty
  range_end. Accepting leftover payload as the range is rejected.
- **Txn leftover-safe embedded ops** — leftover-safe-parses
  Txn-embedded Put/Range/DeleteRange so leftover length-delimited
  bytes cannot steal `lease` / `rev` / `range_end`. A truncated
  inner field fail-closes the whole Txn (cannot look like a
  no-lease put, a live-tree range, or a point delete). Accepting
  leftover payload as an embedded field is rejected.
- **Txn leftover-safe perm key** — leftover-safe-parses the
  RequestOp key used for Txn permission checks so leftover dummy
  `0x00` cannot eat the key tag and skip the perm check. Leftover
  length-delimited bytes cannot steal the key. A truncated key
  fail-closes (cannot look like a missing-key allow). Dummy
  `0x00` / omitted is empty key. Accepting leftover payload as
  the key is rejected.
- **Range/Alarm leftover-safe response** — leftover-safe-parses
  RangeResponse KVs and AlarmResponse so leftover length-delimited
  bytes cannot steal a printed key or CORRUPT type. A truncated
  key / alarm fail-closes (cannot look like a leftover print).
  Dummy `0x00` is not a skip length. `cetcdctl get` / `txn` /
  `alarm list` leftover-safe-skip unknown fields. Accepting
  leftover payload as the printed key is rejected.
- **Range leftover-safe count / more** — leftover-safe-parses
  RangeResponse.count / more so leftover length-delimited bytes
  cannot steal a printed `get --count-only` count or `more=true`.
  A truncated count / more fail-closes (cannot print 0). Dummy
  `0x00` is not a skip length. `cetcdctl get` leftover-safe-parses
  field 3/4. Accepting leftover payload as the printed count is
  rejected.
- **Authenticate leftover-safe name/password** — leftover-safe-parses
  AuthenticateRequest so leftover length-delimited bytes cannot steal
  a name or password. A truncated password fail-closes (cannot look
  like a name-only authenticate). Dummy `0x00` / omitted is empty.
  Accepting leftover payload as the password is rejected.
- **UserAdd leftover-safe password** — leftover-safe-parses
  AuthUserAddRequest so leftover length-delimited bytes cannot steal
  a password or `no_password`. UserChangePassword leftover-safe-parses
  the same name/password fields. A truncated password / options
  fail-closes (cannot look like a name-only add). Accepting leftover
  payload as the password is rejected.
- **Txn Compare leftover-safe result** — leftover-safe-parses Compare
  so leftover length-delimited bytes cannot steal `result` and flip
  a CAS. A truncated result varint fail-closes the whole Txn (cannot
  look like EQUAL). Dummy `0x00` / omitted is EQUAL. Accepting leftover
  payload as the result is rejected.
- **TxnRequest leftover-safe ops** — leftover-safe-parses TxnRequest
  so leftover length-delimited bytes cannot inject an extra
  success/failure op. A truncated success-op length fail-closes
  (cannot look like an empty txn). Dummy `0x00` / omitted is 0 ops.
  Accepting leftover payload as a success Put is rejected.
- **Auth name leftover-safe** — leftover-safe-parses AuthUserDelete /
  RoleAdd / RoleDelete / UserGet / RoleGet so leftover length-delimited
  bytes cannot steal the name and delete or look up the wrong principal.
  UserGrantRole / UserRevokeRole leftover-safe-parses the same user/role
  strings. A truncated name fail-closes (cannot look like a successful
  delete). Dummy `0x00` / omitted is empty. Accepting leftover payload
  as the name is rejected.
- **Auth RoleGrant/RevokePermission leftover-safe** — leftover-safe-parses
  AuthRoleGrantPermissionRequest / AuthRoleRevokePermissionRequest so
  leftover length-delimited bytes cannot steal the role, Permission key,
  range_end, or permType. A truncated name / Permission / key fail-closes
  (cannot look like a successful grant). Dummy `0x00` / omitted is empty.
  Accepting leftover payload as the key is rejected.
- **WatchCreate leftover-safe start_rev** — leftover-safe-parses
  WatchCreateRequest so leftover length-delimited bytes cannot steal
  `start_rev`, `range_end`, `watch_id`, or `fragment`. A truncated
  `--start-rev` fail-closes (cannot watch the live tree). Dummy `0x00` /
  omitted is from-now. Accepting leftover payload as the start revision
  is rejected. `fragment` leftover-safe-splits oversized WatchResponses
  by `--max-request-bytes` (a no-op fragment flag is rejected).
- **MemberAdd leftover-safe peerURL** — leftover-safe-parses
  MemberAddRequest so leftover length-delimited bytes cannot steal a
  peerURL or `isLearner`. A truncated peerURL / isLearner fail-closes
  (cannot look like a successful add or a voter). Dummy `0x00` /
  omitted is empty / voter. Accepting leftover payload as the peerURL
  is rejected.
- **MemberList leftover-safe response** — leftover-safe-parses
  MemberListResponse so leftover length-delimited bytes cannot steal
  a printed peerURL, member id, or `isLearner`. A truncated peerURL /
  isLearner fail-closes (cannot print a leftover URL or look like a
  voter). Dummy `0x00` is not a skip length. `cetcdctl member list`
  leftover-safe-skips unknown fields. Accepting leftover payload as
  the printed URL is rejected. Member encodes etcd field 2 `name`
  (`0x12`) / field 3 `peerURLs` (`0x1a`) (a swapped name/peerURL
  wire is rejected).
- **MemberList leftover-safe clientURL** — leftover-safe-parses
  Member.clientURLs so leftover length-delimited bytes cannot steal a
  used `--cluster` client URL. A truncated clientURL fail-closes
  (cannot connect leftover text). Dummy `0x00` is not a skip length.
  `cetcdctl endpoint --cluster` leftover-safe-parses field 4.
  Accepting leftover payload as the connected URL is rejected.
- **MemberList leftover-safe name** — leftover-safe-parses
  Member.name so leftover length-delimited bytes cannot steal a
  printed member name. A truncated name fail-closes (cannot print
  leftover text). Dummy `0x00` is not a skip length. `cetcdctl
  member list` leftover-safe-parses field 2. Accepting leftover
  payload as the printed name is rejected.
- **Status leftover-safe version / isLearner** — leftover-safe-parses
  StatusResponse so leftover length-delimited bytes cannot steal a
  printed version, dbSize, or `isLearner`. A truncated version /
  isLearner fail-closes (cannot print leftover text or look like a
  voter). Dummy `0x00` is not a skip length. `cetcdctl endpoint
  status` leftover-safe-skips unknown fields. Accepting leftover
  payload as the printed version is rejected.
- **Status leftover-safe errors** — leftover-safe-parses
  StatusResponse.errors so leftover length-delimited bytes cannot
  steal a printed alarm error. A truncated error fail-closes (cannot
  print leftover text). Dummy `0x00` is not a skip length.
  `cetcdctl status` / `endpoint status` leftover-safe-parse field 8.
  Accepting leftover payload as the printed `NOSPACE` / `CORRUPT` is
  rejected.
- **Status leftover-safe dbSizeInUse** — leftover-safe-parses
  StatusResponse.dbSizeInUse so leftover length-delimited bytes cannot
  steal a printed used-page size. A truncated dbSizeInUse fail-closes
  (cannot look like an empty used size). Dummy `0x00` is not a skip
  length. `cetcdctl status` / `endpoint status` leftover-safe-parse
  field 9. Accepting leftover payload as the printed dbSizeInUse is
  rejected.
- **Status leftover-safe leader** — leftover-safe-parses
  StatusResponse.leader so leftover length-delimited bytes cannot
  steal a printed leader. A truncated leader fail-closes (cannot
  look like leader 0). Dummy `0x00` is not a skip length.
  `cetcdctl status` / `endpoint status` leftover-safe-parse field 4.
  Accepting leftover payload as the printed leader is rejected.
- **Status leftover-safe raftIndex** — leftover-safe-parses
  StatusResponse.raftIndex so leftover length-delimited bytes cannot
  steal a printed raftIndex. A truncated raftIndex fail-closes
  (cannot look like raftIndex 0). Dummy `0x00` is not a skip length.
  `cetcdctl status` / `endpoint status` leftover-safe-parse field 5.
  Accepting leftover payload as the printed raftIndex is rejected.
- **Status leftover-safe raftTerm** — leftover-safe-parses
  StatusResponse.raftTerm so leftover length-delimited bytes cannot
  steal a printed raftTerm. A truncated raftTerm fail-closes
  (cannot look like raftTerm 0). Dummy `0x00` is not a skip length.
  `cetcdctl status` / `endpoint status` leftover-safe-parse field 6.
  Accepting leftover payload as the printed raftTerm is rejected.
- **Status leftover-safe raftAppliedIndex** — leftover-safe-parses
  StatusResponse.raftAppliedIndex so leftover length-delimited bytes
  cannot steal a printed raftAppliedIndex. A truncated
  raftAppliedIndex fail-closes (cannot look like raftAppliedIndex 0).
  Dummy `0x00` is not a skip length. `cetcdctl status` /
  `endpoint status` leftover-safe-parse field 7. Accepting leftover
  payload as the printed raftAppliedIndex is rejected.
- **LeaseGrant / TimeToLive leftover-safe ID** — leftover-safe-parses
  LeaseGrantResponse and LeaseTimeToLiveResponse so leftover
  length-delimited bytes cannot steal a printed or lock-used ID, TTL,
  or key. A truncated ID / key fail-closes (cannot look like a missing
  grant or print a leftover key). Dummy `0x00` is not a skip length.
  Tag `0x08` is not the grant ID (`lock` / `elect` leftover-unsafe
  treated wrap `0x08` as the ID). `cetcdctl lease grant` /
  `timetolive` leftover-safe-skip unknown fields. Accepting leftover
  payload as the printed ID is rejected.
- **Authenticate leftover-safe token** — leftover-safe-parses
  AuthenticateResponse so leftover length-delimited bytes cannot steal
  a used token. A truncated token fail-closes (cannot look like no
  token). Dummy `0x00` is not a skip length. `cetcdctl auth login`
  leftover-safe-parses field 2. Accepting leftover payload as the
  token is rejected.
- **Txn leftover-safe succeeded** — leftover-safe-parses
  TxnResponse.succeeded so leftover length-delimited bytes cannot
  steal a successful lock, elect, or compare. A truncated succeeded
  fail-closes (cannot look like a held lock). Dummy `0x00` is not a
  skip length. `cetcdctl lock` / `elect` / `txn` leftover-safe-parse
  field 2. Accepting leftover payload as succeeded is rejected.
- **Hash leftover-safe printed hash** — leftover-safe-parses Hash /
  HashKV so leftover length-delimited bytes cannot steal a printed
  hash or compact_revision. A truncated hash fail-closes (cannot look
  like 0). Dummy `0x00` is not a skip length. `cetcdctl hash` /
  `hashkv` leftover-safe-parse field 2/3. Accepting leftover payload
  as the printed hash is rejected.
- **LeaseLeases leftover-safe ID** — leftover-safe-parses
  LeaseLeasesResponse so leftover length-delimited bytes cannot steal
  a printed lease ID. A truncated lease / ID fail-closes (cannot print
  leftover text). Dummy `0x00` is not a skip length. `cetcdctl lease
  list` leftover-safe-skips unknown fields. Accepting leftover payload
  as the printed ID is rejected.
- **ResponseHeader leftover-safe revision** — leftover-safe-parses
  ResponseHeader so leftover length-delimited bytes cannot steal a
  printed JSON revision. A truncated header / revision fail-closes
  (cannot print leftover text). Dummy `0x00` is not a skip length.
  `cetcdctl -w json` leftover-safe-parses field 1. Accepting leftover
  payload as the printed revision is rejected.
- **Snapshot leftover-safe blob** — leftover-safe-parses
  SnapshotResponse so leftover length-delimited bytes cannot steal
  the blob written to disk. A truncated blob fail-closes (cannot
  write leftover bytes as a snapshot). Dummy `0x00` is not a skip
  length. `cetcdctl snapshot save` leftover-safe-parses field 3.
  Accepting leftover payload as the snapshot is rejected.
- **KeepAlive leftover-safe TTL** — leftover-safe-parses
  LeaseKeepAliveResponse so leftover length-delimited bytes cannot
  steal a printed or lock-used TTL. A truncated TTL fail-closes
  (cannot steal a lock interval). Dummy `0x00` is not a skip length.
  `cetcdctl lease keepalive` / `lock` / `elect` leftover-safe-parse
  field 3. Accepting leftover payload as the TTL is rejected.
- **LeaseGrant leftover-safe printed ID** — leftover-safe-parses
  LeaseGrantResponse so leftover length-delimited bytes cannot steal
  a printed grant ID. TimeToLive leftover-safe-skips unknown fields
  so leftover cannot steal a printed TTL key. A truncated ID / key
  fail-closes. Dummy `0x00` is not a skip length. `cetcdctl lease
  grant` / `timetolive` leftover-safe-parse field 2. Accepting leftover
  payload as the printed ID is rejected.
- **LeaseGrant leftover-safe error** — leftover-safe-parses
  LeaseGrantResponse.error so leftover length-delimited bytes cannot
  steal a printed grant error. A truncated error fail-closes (cannot
  print leftover text). Dummy `0x00` is not a skip length.
  `cetcdctl lease grant` leftover-safe-parses field 4. Interop-only:
  cetcd omits field 4 on success (grant failures stay empty frames).
  Accepting leftover payload as the printed error is rejected.
- **TimeToLive leftover-safe keys** — leftover-safe-parses
  LeaseTimeToLiveResponse.keys so leftover length-delimited bytes
  cannot steal a printed TTL key. A truncated key fail-closes
  (cannot print leftover text). Dummy `0x00` is not a skip length.
  `cetcdctl lease timetolive --keys` leftover-safe-parses field 5.
  Accepting leftover payload as the printed key is rejected.
- **RoleGet leftover-safe key / permType** — leftover-safe-parses
  AuthRoleGetResponse so leftover length-delimited bytes cannot steal
  a printed Permission key or permType. A truncated perm / key
  fail-closes (cannot print leftover text). Dummy `0x00` is not a
  skip length. `cetcdctl role get` leftover-safe-skips unknown fields.
  Accepting leftover payload as the printed key is rejected.
  Permission leftover-safe-encodes etcd field 2 `key` (`0x12`) /
  field 3 `range_end` (`0x1a`) (a swapped key/range_end wire is
  rejected).
- **RoleGet leftover-safe range_end** — leftover-safe-parses
  Permission.range_end so leftover length-delimited bytes cannot
  steal a printed range_end. A truncated range_end fail-closes
  (cannot print leftover text). Dummy `0x00` is not a skip length.
  `cetcdctl role get` leftover-safe-parses field 3. Accepting leftover
  payload as the printed range is rejected.
- **Watch leftover-safe event key** — leftover-safe-parses
  WatchResponse so leftover length-delimited bytes cannot steal a
  printed Event key or type. A truncated event / kv / key fail-closes
  (cannot print leftover text or set `ETCD_WATCH_KEY`). Dummy `0x00`
  is not a skip length. `cetcdctl watch` leftover-safe-skips unknown
  fields. Accepting leftover payload as the printed key is rejected.
- **Watch leftover-safe fragment** — leftover-safe-parses
  WatchResponse.fragment so leftover length-delimited bytes cannot
  steal `fragment=true`. A truncated fragment fail-closes (cannot
  look like more frames). Dummy `0x00` is not a skip length.
  WatchCreate `fragment` leftover-safe-splits oversized frames by
  `--max-request-bytes`; one event over budget is still sent.
  `cetcdctl watch --fragment` leftover-safe-encodes field 8 (not a
  no-op). Accepting leftover payload as fragment is rejected.
- **Watch leftover-safe cancel_reason** — leftover-safe-parses
  WatchResponse.cancel_reason so leftover length-delimited bytes
  cannot steal a printed compact cancel. A truncated reason
  fail-closes (cannot print leftover text). Dummy `0x00` is not a
  skip length. Compacted WatchCreate / active-watch cancel encodes
  etcd `ErrCompacted` on field 6. `cetcdctl watch` leftover-safe-parses
  field 6. Accepting leftover payload as the printed reason is
  rejected.
- **DeleteRange leftover-safe prev_kv** — leftover-safe-parses
  DeleteRangeResponse so leftover length-delimited bytes cannot steal
  a printed prev_kv key. A truncated prev_kv / key fail-closes
  (cannot print leftover text). Dummy `0x00` is not a skip length.
  `cetcdctl del --prev-kv` / `put --prev-kv` leftover-safe-skip
  unknown fields. Accepting leftover payload as the printed key is
  rejected.
- **DeleteRange leftover-safe deleted** — leftover-safe-parses
  DeleteRangeResponse.deleted so leftover length-delimited bytes
  cannot steal a printed delete count. A truncated deleted
  fail-closes (cannot print 0). Dummy `0x00` is not a skip length.
  `cetcdctl del` leftover-safe-parses field 2. Accepting leftover
  payload as the printed count is rejected.
- **auth login leftover-safe printed token** — leftover-safe-parses
  AuthenticateResponse so leftover length-delimited bytes cannot steal
  a printed login token. AuthStatus leftover-safe-parses so leftover
  cannot steal printed `enabled`. A truncated token / enabled
  fail-closes. Dummy `0x00` is not a skip length. `cetcdctl auth login`
  / `auth status` leftover-safe-parse field 2. Accepting leftover
  payload as the printed token is rejected.
- **AuthStatus / user-role list leftover-safe** — leftover-safe-parses
  AuthStatusResponse and UserList/RoleList so leftover length-delimited
  bytes cannot steal `enabled` or a printed user/role name. A truncated
  enabled / name fail-closes (cannot look like disabled or print a
  leftover principal). Dummy `0x00` is not a skip length. `cetcdctl
  auth status` / `user list` / `role list` leftover-safe-skip unknown
  fields. Accepting leftover payload as enabled or the printed name
  is rejected.
- **Downgrade leftover-safe printed version** — leftover-safe-parses
  DowngradeResponse so leftover length-delimited bytes cannot steal
  a printed cluster version. A truncated version fail-closes (cannot
  print leftover text). Dummy `0x00` is not a skip length.
  `cetcdctl downgrade` leftover-safe-parses field 2. VALIDATE encodes
  `cetcd_version()`. Accepting leftover payload as the printed version
  is rejected.
- **Downgrade leftover-safe action/version** — leftover-safe-parses
  DowngradeRequest so leftover length-delimited bytes cannot steal
  ENABLE or a VALIDATE version. A truncated action / version
  fail-closes (cannot look like VALIDATE of `cetcd_version()`). Dummy
  `0x00` is not a skip length (cannot eat VALIDATE). Omitted / dummy
  is VALIDATE / empty version. Accepting leftover payload as the
  action or version is rejected.
- **`unix://` / `unixs://` listen** — etcd UniqueURLs may be unix
  sockets. cetcd has no unix listener, so `--listen-*-urls`,
  advertise, `--initial-cluster`, metrics, and `cetcdctl --endpoints`
  fail-close as known-unsupported (not an unknown invalid URL, and not
  host `unix://localhost`). Implementing a fake bind is rejected.
- **`--host-whitelist`** — comma-separated Host names on the metrics
  HTTP port. Empty / `*` (omitted default) allows all. A restricted list
  waits for headers and returns 403 if Host is missing or not listed
  (port stripped). Default `*` stays a fast path (request-line only).
- **`--metrics basic|extensive`** — omitted / `basic` keeps counters and
  gauges only (no per-RPC clock). `extensive` times unary dispatch and
  observes `grpc_server_handling_seconds` (Prometheus DefBuckets).
  Other values or a missing value fail at parse (no longer an unknown
  flag). Accepting `extensive` as a no-op is rejected.
- **`--socket-reuse-port`** — bool (omitted default off; bare / `true`
  sets `SO_REUSEPORT` via `UV_TCP_REUSEPORT` on client, peer, and
  metrics listeners). Windows has no `SO_REUSEPORT` and fail-closes
  at start. A non-bool fail-closes. `--socket-reuse-address` `true` /
  bare is accepted (libuv already sets `SO_REUSEADDR`); `false`
  fail-closes (cannot disable it).
- **etcd disable-forms** — `--enable-grpc-gateway=false` / `--enable-v2=false`
  / `--unsafe-no-fsync=false` are accepted (gateway, v2, and skipped
  fsync are already off). Bare / `true` fail-closes (do not pretend).
  `--listen-client-http-urls` fail-closes (gRPC-gateway is a non-goal).
- **v2-era leftovers** — `--v2-deprecation=gone|write-only` (and
  `write-only-drop-data` / `write-only-skip-check`) is accepted (v2 is
  already gone). `not-yet` fail-closes (would need a v2 store).
  `--proxy=off` and `--discovery-fallback=exit` are accepted; `on` /
  `readonly` / `proxy` fail-close.   `--discovery` (v2 URL), `--cors`,
  and `--proxy-*` timeouts fail-close. `--max-snapshots` / `--max-wals`
  (single WAL), `--client-cert-file` / `--client-key-file` (no outbound
  client TLS), and `--backend-batch-*` / `--backend-bbolt-freelist-type`
  (LMDB) fail-close as known-unsupported. `--discovery-srv` is unchanged.
- **`--enable-log-rotation` / `--log-rotation-config-json`** — omitted
  default off. Requires a single `--log-outputs` file path (stdio /
  journal / comma-list fail-close). JSON is lumberjack
  `maxsize`/`maxage`/`maxbackups`/`localtime`/`compress` (`{}` uses
  100 MiB). `compress:true` fail-closes (no gzip). Invalid JSON or a
  non-bool fail at parse.   Rotation renames the file to a timestamped
  backup and prunes by count/age.
- **`--raft-read-timeout` / `--raft-write-timeout`** — Go duration on
  each rafthttp connection (omitted default 5s). etcd 3.5 floors
  values `< 5s` (including `0`) to 5s. A stale inbound/outbound peer
  read or an in-flight outbound write is closed on the Raft tick.
  Missing value or leftover text fail at parse. Accepting the flags
  as a no-op is rejected (a hung peer socket would stay open).
- **`--experimental-snapshot-catchup-entries`** — integer entries kept
  after a WAL/raft compact so a slightly-behind follower can `App`
  instead of `MsgSnap` (omitted default 5000; `0` compact to applied).
  Compact index is `applied - N` (floor 1). Missing value or leftover
  text fail at parse (no longer swallowed). Accepting the flag as a
  no-op is rejected.
- **`--experimental-compact-hash-check-enabled` /
  `--experimental-compact-hash-check-time`** — omitted default off /
  1m. When enabled, members HashKV the compacted revision and send it
  over the peer path. The leader raises CORRUPT if a follower's hash
  at the same rev differs. Missing value or leftover text fail at
  parse (no longer swallowed). A local-only file compare is rejected
  (that would not check followers).
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
- **`--experimental-memory-mlock`** — bool (`true`/`false`/`1`/`0`;
  bare flag is true). Unix `mlockall(MCL_CURRENT|MCL_FUTURE)` at start
  (failure fail-closes). Windows is UNSUPPORT (start fail-closes).
  Invalid value fail at parse (no longer swallowed). Other
  `--experimental-*` stay no-ops.
- **`--experimental-bootstrap-defrag-threshold-megabytes`** — integer
  MiB (`0` = off). After opening LMDB, if `alloc` is larger the
  server compact-copies `data.mdb` and reopens before load. Missing
  value or leftover text fail at parse. `Maintenance/Defragment` now
  does the same compact-copy when a backend is attached (no backend
  still returns success). Other `--experimental-*` stay no-ops.
- **`--experimental-warning-unary-request-duration`** — Go duration
  (`0` disables; omitted default 300ms). A unary RPC slower than the
  threshold logs a warning. Missing value or a non-duration fail at
  parse (no longer swallowed). Other `--experimental-*` stay no-ops.
- **`--experimental-enable-lease-checkpoint` /
  `--experimental-enable-lease-checkpoint-persist`** — bool
  (`true`/`false`/`1`/`0`; bare is true). Remaining TTL is already
  raft-applied on Grant/KeepAlive and written to the LMDB `lease`
  bucket (wall-clock deadline), so restart and followers keep the
  same remaining TTL. Accepting `true` is that existing path, not a
  no-op. `false` is also accepted (we still persist; disabling would
  lose TTL on restart). A non-bool fail-closes.
- **Unimplemented `--experimental-*` fail-closed** — known etcd 3.5
  experimental flags we cannot honor (`enable-distributed-tracing` and
  its address/service-name/instance-id/sampling-rate,
  `stop-grpc-service-on-defrag`, `peer-skip-client-san-verification`,
  `txn-mode-write-with-shared-buffer`, `enable-v2v3`,
  `downgrade-check-time`) fail at parse. Bool forms accept `false`/`0`
  (explicitly off). A typo is `unknown flag`. The old catch-all no
  longer swallows the next argv (it could eat `--data-dir`). Accepting
  them as a no-op is rejected.
- **`--listen-metrics-urls` https** — `https://` terminates TLS on that
  listener with the client cert (`--cert-file` / `--auto-tls`; same CA,
  CRL, cipher, version, and client-cert-auth). Mixed http/https is
  allowed. No ALPN (HTTP/1 scrape). Missing cert fail-closes. An `http://`
  client listen is not wrapped just because metrics needs certs.
  Accepting https as a no-op or plaintext is rejected.
- **HTTP/2 stream multiplex** — each stream has its own path, token, and
  body. A second unary or Watch on the same connection cannot steal the
  first stream's `:path` or `authorization`. Peer `POST /raft` uses the
  same per-stream table so a second rafthttp request cannot steal the
  first stream's path or body. SETTINGS
  `--max-concurrent-streams` is clamped to the tracked table
  (`CETCD_H2_MAX_STREAMS`). Extra streams are RST (REFUSED_STREAM).
  A single `cur` request slot is rejected. Snapshot, RangeStream, and
  Watch capture the stream writer at handler entry so a second RPC
  cannot steal the prelude or Watch create/progress.

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
