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
(field 1) instead of a version string. The `Alarm` handler processes three actions: GET (list current alarms), ACTIVATE (add an
alarm), and DEACTIVATE (remove an alarm). It supports both NOSPACE and CORRUPT alarm types
simultaneously, with an in-memory alarm table (up to 8 entries) that allows independent
activation/deactivation of each alarm type. It returns an `AlarmResponse` with the current
alarm list (field 2, repeated AlarmMember with memberID and alarm type). The
`make_simple_response()` helper used by `Defragment` and `MoveLeader` returns a proper
`ResponseHeader` with the current revision.

All Cluster RPC responses (`MemberList`, `MemberAdd`, `MemberRemove`, `MemberUpdate`,
`MemberPromote`) include a `ResponseHeader` as field 1, matching the etcd v3.5 proto wire
format. The `make_simple_cluster_response()` helper used by `MemberRemove`, `MemberUpdate`,
and `MemberPromote` returns a proper `ResponseHeader` with the current revision.

The `cetcdctl` CLI has been expanded to cover the full command set: `lease list/keepalive`,
`member add/remove/update/promote`, `user delete/change-password/grant-role/revoke-role`,
`role delete`, `hash`, `hashkv`, `defrag`, `move-leader`, `get --prefix/--keys-only/--rev`,
`del --prefix/--prev-kv`, `put --prev-kv`, `watch --prefix/--prev-kv/--start-rev`, `txn cas` (compare-and-swap),
`auth login` (token-based authentication), `get --count-only/--limit N/--sort-by/--sort-order/--print-value-only`,
`put --ignore-value/--ignore-lease`, `get/del KEY RANGE_END` (positional range_end argument),
`get/del --from-key` (unbounded range queries), `put --lease ID` (attach lease to key),
`alarm list/activate/disarm` (alarm management), `version` (print client version),
`txn get KEY [RANGE_END]` (transactional range query), `txn del [--prefix] [--prev-kv] KEY [RANGE_END]`
(transactional delete), `get --hex` (hex output for binary data), `lease timetolive --keys ID`
(show keys attached to lease), `endpoint health/status` (server health check and status),
`check perf` (simple performance check), `compact --physical` (physically-backed compaction),
`get --consistency l|s` (linearizable or serializable read consistency),
`get -w json` (JSON output format for range queries),
`snapshot status FILE` (show snapshot file information),
`snapshot restore FILE --data-dir DIR [--force] [-w json]` (restore snapshot to data directory with optional --force to overwrite existing data)
`lock LOCKNAME [CMD...]` (distributed lock using Lease + Txn with signal-based release),
`elect ELECTION_NAME [PROPOSAL]` (leader election using Lease + Txn),
`--endpoints host:port` (global flag for specifying server endpoint),
`member list -w table` (table format output for member list),
`status -w json` (JSON output format for status query),
`put -w json` / `del -w json` / `watch -w json` (JSON output for KV operations and watch events),
`--command-timeout SEC` (global flag for command execution timeout),
`get -w fields` (fields output format showing create_revision, mod_revision, version, lease, and value),
`lease list -w table` / `lease list -w json` (table and JSON output for lease list),
`alarm list -w table` / `alarm list -w json` (table and JSON output for alarm list),
`member list -w json` (JSON output format for member list),
`put -w fields` (fields output for put response showing prev_kv metadata),
`user list -w table` / `role list -w table` (table format output for auth user and role lists),
`--user USER:PASS` (global flag for server authentication via Auth/Authenticate RPC),
`--debug` (global flag printing RPC path and response size for debugging),
`del -w fields` (fields output for delete response showing prev_kvs metadata),
`watch -w fields` (fields output for watch events showing full KV metadata),
`lease keepalive [--once]` (loop keepalive by default, `--once` for single keepalive),
`compact/hash/hashkv/defrag/move-leader -w json` (JSON output for maintenance and KV compact commands),
`lease grant/timetolive -w json` (JSON output for lease commands),
`auth status -w json` (JSON output for auth status query),
`--insecure` (global flag, accepted for etcdctl compatibility, no-op for plain TCP),
`--dial-timeout SEC` (global flag for connection timeout via SO_SNDTIMEO/SO_RCVTIMEO),
`--keepalive-time SEC` / `--keepalive-timeout SEC` (global flags accepted for etcdctl compatibility, no-op for plain TCP),
`endpoint hashkv [-w json]` (subcommand to call HashKV RPC per endpoint, with JSON output),
`endpoint health -w json` (JSON output for health check with endpoint/status/error fields),
`endpoint status -w json|table` (JSON or table output for endpoint status, now parses actual revision from ResponseHeader),
`snapshot status FILE -w json` (JSON output for snapshot status with hash/revision/size/filename),
`version -w json` (JSON output with client/version/etcd fields),
`status -w fields` (fields output format showing version, dbSize, leader, raftIndex, raftTerm, and revision from ResponseHeader),
`status -w json` now includes `revision` field from ResponseHeader.
`--cacert` / `--cert` / `--key` (global TLS flags accepted for etcdctl compatibility, no-op for plain TCP),
`txn -w json` (JSON output for all txn subcommands: put, cas, get, del — outputs `{"header":{},"succeeded":true|false}`),
`member add/remove/update/promote -w json` (JSON output for cluster member management operations),
`downgrade -w json` (JSON output for downgrade enable/cancel/validate),
`alarm list -w fields` (fields output format for alarm list with memberID and alarm type),
`lease list -w fields` (fields output format for lease list),
`get -w table` (table output format for range queries with KEY/CREATE_REV/MODIFY_REV/VERSION/VALUE columns),
`auth enable/disable -w json` (JSON output for auth enable/disable operations),
`lease revoke -w json` / `lease keepalive -w json` (JSON output for lease revoke and keepalive with ID/TTL fields),
`user add/delete/get/list/change-password/grant-role/revoke-role -w json` (JSON output for all user management commands),
`role add/delete/get/list/grant-permission/revoke-permission -w json` (JSON output for all role management commands, including permission details in role get),
`snapshot save -w json` (JSON output for snapshot save with snapshot filename and size fields),
`get -w json` enhanced (now includes ResponseHeader with cluster_id/member_id/revision/raft_term, and full KV fields: create_revision/mod_revision/version/lease in each entry),
`auth login -w json` (JSON output with token field for authentication response),
`check perf -w json` (JSON output with status and put/get latency measurements in milliseconds),
`lock --ttl N` / `elect --ttl N` (customizable lease TTL for distributed lock and leader election, default 60s),
`put -w json` enhanced (now parses ResponseHeader and outputs cluster_id/member_id/revision/raft_term),
`del -w json` enhanced (now parses ResponseHeader and outputs prev_kvs array with full KV metadata when --prev-kv is set),
All `-w json` commands now parse ResponseHeader (compact, lease revoke/timetolive/list/grant/keepalive, txn put/del, alarm list, auth enable/disable/status, user/role CRUD, member remove/update/promote, downgrade, defrag, move-leader — all output real cluster_id/member_id/revision/raft_term instead of empty `{}`).
`hash -w json` / `hashkv -w json` enhanced (now parses ResponseHeader instead of empty `{}`),
`status -w json` enhanced (now includes ResponseHeader with cluster_id/member_id/revision/raft_term before version/dbSize/leader fields),
`watch -w json` enhanced (now outputs `{"header":{...},"Events":[...]}` format with full KV fields: create_revision/mod_revision/version/lease in each event, and prev_kv support with full metadata),
`parse_string_list_response -w json` enhanced (user list/role list/user get now parse real ResponseHeader),
`snapshot save` fixed (now correctly extracts blob data from SnapshotResponse protobuf, writing only snapshot data to file instead of raw protobuf; JSON output now includes real ResponseHeader),
`check perf -w json` fixed (now includes real ResponseHeader from Put response),
`member add --peer-urls URL` / `--learner` (etcdctl-compatible flags for adding cluster members; --learner sends isLearner=true in MemberAddRequest),
`check datascale [--load N] [--prefix PREFIX] [-w json]` (new subcommand to test database scalability by loading N keys and reporting DB size and elapsed time),
`watch --filter NOPUT|NODELETE` (filter event types in watch, maps to WatchCreateRequest.filters field 7),
`alarm activate/disarm -w json` (JSON output for alarm activate/disarm with real ResponseHeader),
`member list -w json` enhanced (now parses name, clientURLs, and isLearner fields from Member proto; server also returns name="default" and clientURLs),
`snapshot status` enhanced (now parses snapshot blob to count keys and compute hash, includes total_keys column),
`endpoint health/status/hashkv -w json` enhanced (now all include real ResponseHeader in JSON output),
`get -w json` enhanced (now includes `more` field in JSON output, reflecting whether more results are available),
`snapshot restore --force` (new flag to overwrite existing snapshot data in target directory; restore now writes KV pairs to `snapshot.kv` file in proper custom format instead of incorrectly as `data.mdb`),
`version -w json` enhanced (now includes `server` field with value `cetcd`),
`snapshot restore -w json` enhanced (now includes `keys` count field in JSON output),
`print_json_string` helper (unified JSON string escaping for all -w json outputs; now used by get/put/del/watch/member/status/endpoint/auth/role/lease commands for proper handling of special characters like \" \\ \n \r \t and control characters),
`etcd-compatible server flags` (cetcd now accepts `--listen-client-urls`, `--listen-peer-urls` with URL format parsing, plus `--advertise-client-urls`, `--initial-advertise-peer-urls`, `--initial-cluster-state`, `--initial-cluster-token`, `--snapshot-count`, `--quota-backend-bytes`, `--force-new-cluster` as no-op compatibility flags),
`get/del --prefix ""` buffer overflow fix (empty key with `--prefix` now correctly uses `\0` as range_end to match all keys, instead of causing a `key_len - 1` underflow),
`get/del --prefix --from-key` mutual exclusion (returns error when both flags are specified together),
`alarm TYPE` argument parsing (`alarm activate` and `alarm disarm` now accept an optional TYPE argument: `NOSPACE`, `CORRUPT`, or `NONE`; `alarm list` output now displays `CORRUPT` alarm type correctly),
`member add --name` flag (accepted for etcdctl compatibility; the name is display-only and not sent in the MemberAddRequest proto),
`lock/elect --print-value-only` flag (when set, prints the lease ID instead of the lock key / proposal text),
`snapshot save --compaction-periodical` no-op flag (accepted for etcdctl compatibility),
`server-side \0 range_end fix` (the KV Range handler now correctly treats a `range_end` of `\0` (single null byte) as "all keys >= key" per etcd semantics, instead of treating it as a literal upper bound that excludes all printable keys),
`watch --prefix ""` buffer overflow fix (same fix as get/del: empty key with --prefix now uses `\0` as range_end),
`watch --range-end KEY` flag (alternative to --prefix for specifying an explicit range_end),
`watch --hex` flag (outputs key/value events in hexadecimal format, matching etcdctl behavior),
`put KEY -` stdin support (when VALUE is `-`, reads value from stdin, matching etcdctl behavior; trailing newline is stripped),
`server-side alarm handler enhancement` (now supports both NOSPACE and CORRUPT alarm types simultaneously, with proper activate/deactivate for each type independently; previously only NOSPACE was handled),
`txn del --prefix ""` buffer overflow fix (same `key_len - 1` underflow pattern as get/del/watch, now uses `\0` as range_end for empty prefix),
`txn del --from-key` flag (accepted for etcdctl compatibility; uses `\0` as range_end for all keys >= key),
`check datascale --prefix ""` buffer overflow fix (same pattern; cleanup delete now handles empty prefix correctly),
`del --hex` flag (outputs prev-kv key/value in hexadecimal format, matching etcdctl behavior),
`snapshot file format with revision` (snapshot files now include a 12-byte header: 4-byte magic "CTS1" + 8-byte revision in little-endian; `snapshot status` displays the revision; `snapshot restore` skips the header and writes KV data only to `snapshot.kv`; old format files without the header are still supported for backward compatibility),
`member list -w fields` (new output format showing ID, name, peerURLs, clientURLs, and isLearner for each member),
`endpoint status -w fields` (new output format showing endpoint, ID, revision, dbSize, raftIndex, raftTerm, and version),
`compact -w fields` (new output format showing ResponseHeader fields),
`defrag -w fields` (fields output showing ResponseHeader),
`move-leader -w fields` (fields output showing ResponseHeader),
`snapshot status -w fields` (fields output showing hash, revision, total_keys, size, filename),
`downgrade enable/cancel/validate -w fields` (fields output showing ResponseHeader),
`auth enable/disable/status -w fields` (fields output showing ResponseHeader and enabled status),
`user list -w fields` (fields output listing each user with label),
`role list -w fields` (fields output listing each role with label),
`lease grant --lease-id ID` (custom lease ID in hex format, matching etcdctl behavior),
`lease grant/revoke/timetolive/keepalive -w fields` (fields output showing ID, TTL, grantedTTL, and keys with --keys),
`version -w fields` (fields output showing client, server, version, etcd compatibility),
`endpoint health -w fields` (fields output showing endpoint, ResponseHeader, status, took time),
`hash/hashkv -w fields` (fields output showing hash and compact_revision),
`alarm activate/disarm -w fields` (fields output showing ResponseHeader),
`member add/remove/update/promote -w fields` (fields output showing ResponseHeader, member add uses fields format for member list),
`user add/delete/get/change-password/grant-role/revoke-role -w fields` (fields output showing ResponseHeader, user get shows roles in fields format),
`role add/delete/get/grant-permission/revoke-permission -w fields` (fields output showing ResponseHeader, role get shows permissions in fields format),
`endpoint hashkv -w fields` (fields output showing ResponseHeader, hash, and compact_revision),
`check perf -w fields` (fields output showing ResponseHeader, status, put/get latency),
`check datascale -w fields` (fields output showing ResponseHeader, status, keys_loaded, db_size, elapsed),
`snapshot save -w fields` (fields output showing ResponseHeader, snapshot filename, size),
`snapshot restore -w fields` (fields output showing snapshot, data_dir, size, keys, revision),
`auth login -w fields` (fields output showing ResponseHeader and token),
`txn put/cas/get/del -w fields` (fields output showing ResponseHeader and succeeded status),
`lock -w json|fields` (JSON output with header+key, fields output with header+key),
`elect -w json|fields` (JSON output with header+leader, fields output with header+leader),
`get --range-end KEY` (explicit range-end flag for range queries, equivalent to positional RANGE_END),
`del --range-end KEY` (explicit range-end flag for delete ranges),
`watch --exec COMMAND` (execute a shell command on each watch event with ETCD_WATCH_EVENT_TYPE, ETCD_WATCH_KEY, ETCD_WATCH_VALUE, ETCD_WATCH_REVISION environment variables),
`put -w json|fields` in usage messages (was already supported, now shown in help),
`role grant-permission --prefix` and `--range-end KEY` (prefix and range-end support for granting key-range permissions, sends Permission.range_end as field 3),
`role revoke-permission ROLE [TYPE KEY]` (revoke a specific permission by key instead of all; supports `--prefix` and `--range-end KEY` for range-based revocation, sends key as field 2 and range_end as field 3 in RoleRevokePermissionRequest),
`user add --no-password` (create users without passwords for cert-based auth, sends UserAddOptions.no_password=true as field 3 in UserAddRequest; server-side handler updated to accept empty password when no_password option is set),
`completion bash|zsh|fish` (generate shell completion scripts for bash, zsh, and fish shells, covering all cetcdctl commands, subcommands, global options, and command-specific flags),
`put --print-value-only` (output only the previous value when used with `--prev-kv`, implies `--prev-kv` if not explicitly set),
`del --print-value-only` (output only the deleted values when used with `--prev-kv`, implies `--prev-kv` if not explicitly set),
`check perf --load S|M|L` (workload size: s=10, m=100, l=1000 keys; runs multiple put/get operations and reports average latency) and `--prefix PREFIX` (key prefix for test keys),
`lease keepalive --interval SEC` (custom keepalive interval instead of default ttl/2),
`member add --peer-urls url1,url2` (comma-separated multiple peer URLs for cluster member addition; `member update` also supports comma-separated URLs),
`txn -i` (interactive transaction mode: reads a transaction definition from stdin with `cmp`/`cmp_create`/`cmp_mod`/`cmp_ver` compare conditions, `then`/`else` sections, and `put`/`get`/`del` operations; builds a full TxnRequest with compare, success, and failure op lists and sends it as a single atomic Txn RPC; supports `-w json|fields` output),
`endpoint health --cluster` (checks the health of all cluster members by first calling MemberList to discover all member client URLs, then connecting to each endpoint individually and sending a Status RPC; supports `-w json|fields` output)

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
standalone `Range` handler. It also supports `sort_order` (field 5, tag 0x28) and
`sort_target` (field 6, tag 0x30) for sorting results by KEY, VERSION, CREATE, MOD, or VALUE
in ASCEND or DESCEND order, and `min_mod_revision`/`max_mod_revision`/`min_create_revision`/
`max_create_revision` (fields 10–13, tags 0x50–0x68) for filtering results by revision range —
again matching the standalone `Range` handler. The `ResponseRange` within transactions includes a proper
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
The `WatchCreateRequest` parser also supports `progress_notify` (field 4), `filters` (field 5,
NOPUT=0 and NODELETE=1, both packed and non-packed varint encoding), and `fragment` (field 8).
When filters are specified, PUT and/or DELETE events are suppressed accordingly in both legacy
and streaming watch modes.

The cetcdctl response parsing for `del`, `txn cas`, and `watch` now correctly skips the
`ResponseHeader` (tag 0x0a) before parsing response-specific fields, ensuring compatibility
with the proper protobuf encoding.

---

## 7. Watch streaming architecture

The etcd `Watch` RPC is a long-lived, bidirectional gRPC stream. cetcd implements it
with one libco coroutine per active watcher, integrated into the libuv reactor via
`uv_async_t` handles.

### Per-watcher flow

```
Client HTTP/2 stream
  ├─ WatchCreateRequest arrives on reactor
  │     └─ spawn watch coroutine
  │           ├─ subscribe watcher to MVCC key/range
  │           ├─ encode WatchResponse (created=true, watch_id)
  │           ├─ cetcd_co_write() → yield
  │           │
  │           ▼
  │     event arrives from MVCC / another reactor
  │     uv_async_send() wakes the coroutine
  │           ├─ encode WatchResponse(events)
  │           ├─ cetcd_co_write() → yield
  │           └─ loop until cancelled or stream closed
```

1. The gRPC layer receives a `WatchCreateRequest` on an HTTP/2 stream.
2. A dedicated coroutine is spawned from the per-connection scheduler.
3. The coroutine registers a watcher with the MVCC store for the requested key or
   prefix and immediately sends the `created` response.
4. The coroutine yields; the reactor continues processing other streams.
5. When a matching `Put` or `Delete` is committed, MVCC invokes the watcher callback,
   which calls `uv_async_send()` on the async handle associated with that coroutine.
6. libuv fires the async callback on the reactor thread, resumes the coroutine, and
   the coroutine encodes a `WatchResponse` containing the event(s).
7. `cetcd_co_write()` yields until the TCP write completes, then the coroutine waits
   again for the next event.

### Multi-watcher multiplexing

A single client connection may carry many concurrent `Watch` streams, each mapped to
one HTTP/2 stream ID and one coroutine. The MVCC watcher registry is keyed by
key/prefix; a single committed modification can fan out to many watcher coroutines,
each notified independently through its own `uv_async_t`. No polling is required: an
unblocked watcher coroutine consumes no CPU until an event arrives.

### Performance characteristics

- **4 KiB coroutine stacks** by default — tens of thousands of idle watchers cost
  only a few hundred megabytes of virtual address space.
- **Lock-free scheduling** between MVCC and the reactor via `uv_async_send()`; the
  hot path is a memory write + event-loop wakeup.
- **No kernel context switches** per event: yield/resume stays in user space.
- Backpressure is natural: a slow consumer blocks its own coroutine on `co_write`,
  but does not stall other watchers or RPCs on the same connection.

---

## 8. Observability architecture

All observability endpoints are served from a dedicated HTTP listener on port `2381`
by default. The listener is a separate `uv_tcp_t` within the same libuv loop as the
gRPC listener, so it shares the reactor thread without contending with the client
port.

### Prometheus metrics

`libcetcd_base` provides a metrics registry (`cetcd_metrics`) that supports
counters, gauges, and histograms. The server updates metrics during request
processing and Raft ticks. The `/metrics` handler serialises the registry to
Prometheus text exposition format on demand.

Key metric families:

| Metric | Type | Description |
|--------|------|-------------|
| `cetcd_grpc_requests_total` | counter | Total gRPC requests by service and method |
| `cetcd_raft_ticks_total` | counter | Total Raft tick timer firings |
| `cetcd_mvcc_revision` | gauge | Current MVCC revision |
| `cetcd_lease_active` | gauge | Number of active leases |

### pprof endpoints

The same port exposes pprof-compatible endpoints under `/debug/pprof/`:

- `/debug/pprof/profile?seconds=N` — CPU profile sampled for `N` seconds.
- `/debug/pprof/heap` — heap profile in pprof protobuf format.
- `/debug/pprof/coroutines` — snapshot of active coroutines (libco) and their
  scheduling state.

These endpoints are intended for development and production troubleshooting. In
production deployments the metrics port should be bound to localhost or protected
by network policy.

---

## 9. Build, test, ship

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

## 10. Cross-platform abstractions

Platform-specific code is guarded with `#ifdef _WIN32` directly in the modules that
need it (`libcetcd_base/clock.c`, `libcetcd_io/tcp.c`, `libcetcd_server/server.c`).
Public headers never include OS-specific headers — all such includes are confined
to `.c` implementation files behind preprocessor guards.

| Abstraction         | POSIX                | Windows                 |
| ------------------- | -------------------- | ----------------------- |
| Time                | `clock_gettime`      | `QueryPerformanceCounter` |
| Thread              | `pthread_create`     | `CreateThread`          |
| Mutex / condvar     | `pthread_mutex_t`    | `SRWLOCK` / `CONDITION_VARIABLE` |
| Atomics             | C11 `<stdatomic.h>`  | C11 `<stdatomic.h>` (MSVC ≥ 2022) |
| File I/O            | `open` / `pread`     | `CreateFileW` / `ReadFileEx` |
| Directory creation  | `mkdir`              | `_mkdir`                |
| Sockets / event     | via libuv + `sys/socket` | via libuv + Winsock2 (IOCP) |
| Coroutines          | libco (ASM per-arch) | libco (Windows fibers)  |
| Dynamic loading     | `dlopen`             | `LoadLibraryW`          |

The CRC32C lookup table in `libcetcd_base/hash.c` is initialised lazily using
C11 atomics (`memory_order_release`/`acquire`) to remain thread-safe without a
mutex. The per-key watcher fan-out and cluster membership queries use
`_Thread_local` storage where re-entrancy is a concern.

---

## 11. Glossary

- **Revision**: monotonic global commit counter (etcd MVCC's universal clock).
- **Lease**: a TTL-bound handle keys can attach to; expires together.
- **Apply**: take a committed Raft entry → mutate the state machine (mvcc/backend).
- **Snapshot**: a serialised state-machine checkpoint allowing log truncation.
- **WAL**: write-ahead log — every Raft entry is fsync'd here before it commits.
- **Watcher**: a long-lived stream subscribed to key-range events at-and-after a revision.

---

## 12. References

- Diego Ongaro, John Ousterhout — *In Search of an Understandable Consensus Algorithm* (Raft paper), 2014.
- The etcd source tree: <https://github.com/etcd-io/etcd>.
- The etcd v3 API specification: `api/etcdserverpb/rpc.proto`.
- LMDB design notes: <https://lmdb.tech/>.
- libuv documentation: <https://docs.libuv.org/>.
