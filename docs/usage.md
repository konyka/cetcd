# cetcd Usage

> **Status**: actively developed. cetcd implements the etcd v3.5 RPC catalogue (protobuf
> semantics) and speaks a **custom framed TCP protocol** via `cetcdctl`. Official
> `etcdctl` / HTTP/2 gRPC clients are **partially supported**: unary RPCs and
> Watch, LeaseKeepAlive, Snapshot, and RangeStream (HTTP/2 preface on the client port; TLS selects ALPN `h2`). Snapshot / RangeStream / Watch capture the stream writer at handler entry so a second multiplexed RPC cannot steal the prelude.
> The peer port accepts HTTP/2 `POST /raft`; with peer TLS, outbound send uses the same path when ALPN negotiates `h2`.
> `cetcdctl` still uses custom frames — see
> [architecture.md §Wire protocol](./architecture.md#6-wire-protocol).

## 1. Building from source

### Prerequisites

- A C compiler with C11 support (`<stdatomic.h>` and thread-local storage):
  - Linux: gcc ≥ 9 or clang ≥ 11
  - macOS: Apple Clang 13+ (Xcode 13)
  - Windows: MSVC ≥ 19.34 (Visual Studio 2022) or MinGW-w64 gcc ≥ 11
  - FreeBSD: clang 13+
- CMake ≥ 3.21
- Ninja (recommended) or Make / MSBuild
- OpenSSL 3.0+ development headers (system dependency)
- nghttp2 development headers (system dependency, for HTTP/2 session management)
- Python 3.8+ (for protobuf-c codegen at build time)

Everything else (libuv, LMDB, protobuf-c, libco) is vendored
under `third_party/`. nghttp2 and OpenSSL are discovered via `pkg-config`;
if nghttp2 is not found, the HTTP/2 module falls back to stub no-ops
(gRPC framing helpers still work).

### Configure + build

```sh
git clone https://github.com/konyka/cetcd.git
cd cetcd
cmake -B build -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build
```

### Build options

| Option                       | Default | Description                                            |
| ---------------------------- | ------- | ------------------------------------------------------ |
| `CETCD_BUILD_TESTS`          | `ON`    | Build unit + integration tests                         |
| `CETCD_BUILD_FUZZ`           | `OFF`   | Build libFuzzer binaries (clang). Smoke drivers always run in CTest. |
| `CETCD_BUILD_BENCH`          | `OFF`   | Build benchmark suite                                  |
| `CETCD_SANITIZERS`           | (empty) | Comma-separated: `address,undefined,thread,memory`     |
| `CETCD_USE_SYSTEM_OPENSSL`   | `ON`    | Use `find_package(OpenSSL)`; otherwise vendor          |
| `CETCD_USE_SYSTEM_LMDB`      | `OFF`   | Use system LMDB instead of vendored                    |
| `CETCD_ENABLE_LTO`           | `OFF`   | Link-time optimisation (Release builds)                |
| `CETCD_ENABLE_COVERAGE`      | `OFF`   | gcov-style coverage flags                              |
| `CETCD_WERROR`                | `OFF`   | Treat project compiler warnings as build errors        |

### Sanitized debug build (developer default)

```sh
cmake -B build -G Ninja \
  -DCMAKE_BUILD_TYPE=Debug \
  -DCETCD_SANITIZERS=address,undefined
cmake --build build
ctest --test-dir build --output-on-failure
```

ThreadSanitizer (cannot combine with ASan; needs clang compiler-rt or `libtsan1`):

```sh
cmake -B build-tsan -G Ninja \
  -DCMAKE_BUILD_TYPE=Debug \
  -DCMAKE_C_COMPILER=clang \
  -DCETCD_SANITIZERS=thread
cmake --build build-tsan
ctest --test-dir build-tsan --output-on-failure -E integration_server
```

CTest always runs the `fuzz_*_smoke` drivers (empty, one-byte, and 128-byte junk).
To build the libFuzzer binaries (`fuzz_wal_decode`, `fuzz_proto_rpc`, `fuzz_auth_token`):

```sh
cmake -B build-fuzz -G Ninja -DCMAKE_C_COMPILER=clang -DCETCD_BUILD_FUZZ=ON
cmake --build build-fuzz --target fuzz_wal_decode
./build-fuzz/fuzz_wal_decode
```

### Warning-clean review build

```sh
cmake -B build-werror -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCETCD_WERROR=ON
cmake --build build-werror
```

---

## 2. Running `cetcd`

### Single-node server

```sh
./build/bin/cetcd --data-dir ./data --listen 127.0.0.1 --port 2379
# --port / --peer-port must be 1..65535; a typo is not a silent bind on port 0
# --node-id must be > 0; a typo is not Raft id 0
```

This starts a single-node cetcd server listening on port 2379 for client
requests and port 2380 for peer-to-peer (Raft) communication.

`--version` prints `etcd Version:` / `Git SHA` / `C Standard` / `OS/Arch` and
exits. `--config-file` is an etcd YAML map of flag names; when it is set,
other CLI flags and `ETCD_*` are ignored (except `--help` / `--version`). A
missing file or invalid YAML fail-closes. Without a config file, `ETCD_*`
maps to the matching `--flag` (`ETCD_LISTEN_CLIENT_URLS`). Empty values are
ignored. `ETCD_VERSION` is not `--version`. A CLI flag plus the matching
`ETCD_*` fail-closes. `ETCD_CONFIG_FILE` is `--config-file` when the flag
is omitted.

```sh
./build/bin/cetcd --version
./build/bin/cetcd --config-file ./cetcd.yaml
ETCD_LISTEN_CLIENT_URLS=http://127.0.0.1:2379 ./build/bin/cetcd
```

With `--data-dir` set, Put/DeleteRange are proposed through Raft, fsynced to
`{data-dir}/wal/0000000000000000.wal` (or `--wal-dir`), then applied to MVCC
(LMDB). Restart reloads live keys from LMDB and replays any WAL entries ahead
of `applied_index`. Put the WAL on a dedicated disk when fsync latency matters:

```sh
./build/bin/cetcd --data-dir ./data --wal-dir /fast/wal --listen 127.0.0.1 --port 2379
```

### Three-node static cluster

```sh
# Node 1
# etcd --flag=value is accepted (--name=n1 --data-dir=./data --snapshot-count=10000)
./build/bin/cetcd --name=node1 --node-id=1 --data-dir=./data1 \
  --listen 127.0.0.1 --port 2379 --peer-port 2380 \
  --initial-cluster 1=127.0.0.1:2380,2=127.0.0.1:2382,3=127.0.0.1:2384

# Node 2
./build/bin/cetcd --name node2 --node-id 2 --data-dir ./data2 \
  --listen 127.0.0.1 --port 2381 --peer-port 2382 \
  --initial-cluster 1=127.0.0.1:2380,2=127.0.0.1:2382,3=127.0.0.1:2384

# Node 3
./build/bin/cetcd --name node3 --node-id 3 --data-dir ./data3 \
  --listen 127.0.0.1 --port 2383 --peer-port 2384 \
  --initial-cluster 1=127.0.0.1:2380,2=127.0.0.1:2382,3=127.0.0.1:2384
```

The cluster uses Raft consensus for replication. Leader election happens
automatically within ~1 second of startup. `--name` is the MemberList self
name (`default` if omitted). `--initial-cluster` member ids must be `> 0`
(etcd-style names are not mapped to Raft ids).

### etcd-compatible server flags

cetcd accepts several etcd server flags for migration compatibility:

```sh
# Using etcd-style URL flags
./build/bin/cetcd --listen-client-urls http://127.0.0.1:2379 \
  --listen-peer-urls http://127.0.0.1:2380 --data-dir ./data
# --listen-client-urls / --listen-peer-urls are UniqueURLs lists (same scheme; port 1..65535)
# IPv6 needs brackets: http://[::1]:2379 ; leftover 2379foo fail-closes
# IPv6 zones: http://[fe80::1%1]:2379 leftover-safe (1foo / empty zone fail-close)
# hostnames (etcd default http://localhost:2379) resolve; an unresolvable name fail-closes
# IPv6 endpoint print is [::1]:2379 (not ::1:2379) so it can be parsed back
# unix:// / unixs:// fail-close (no unix-socket listener)
# a comma list binds every URL; mixed http/https, a duplicate, or a typo fail-closes
./build/bin/cetcd --listen-client-urls http://127.0.0.1:2379,http://10.0.0.1:2379 \
  --listen-peer-urls http://127.0.0.1:2380 --data-dir ./data

# Advertise URLs go into MemberList as UniqueURLs lists (https requires matching cert files)
# a comma list is emitted as repeated clientURLs / peerURLs; leftover or a duplicate fail-closes
# --initial-cluster-token is persisted; a later mismatch fail-closes
./build/bin/cetcd --advertise-client-urls http://127.0.0.1:2379,http://10.0.0.1:2379 \
  --initial-advertise-peer-urls http://127.0.0.1:2380 \
  --initial-cluster-state new --initial-cluster-token etcd-cluster \
  --snapshot-count=100000 --data-dir ./data
# --snapshot-count must be > 0; omitted is etcd 3.5 default 100000; a typo or 0 fails
# --auto-compaction-mode periodic|revision; --auto-compaction-retention 0 disables
# periodic: 1h / 30m / bare hours; revision: revisions to keep; invalid values fail at parse
./build/bin/cetcd --auto-compaction-mode periodic --auto-compaction-retention 1h --data-dir ./data
# --initial-cluster-state existing: cluster evidence, snapshot.kv, persisted initial-cluster, or --initial-cluster peers
# After WAL compaction the leader sends MsgSnap; otherwise App from next_idx
# --force-new-cluster keeps MVCC and drops peers except self (needs cluster evidence)
# --strict-reconfig-check default on; --strict-reconfig-check=false allows a quorum-losing MemberRemove

# Backend quota (NOSPACE on Puts when LMDB size >= N; 0 / omitted = etcd 2GiB; a typo fails)
./build/bin/cetcd --quota-backend-bytes=2147483648 --max-request-bytes 1572864 \
  --data-dir ./data

# TLS on client/peer accept and outbound Raft (omit for plaintext; cert without key fails start)
# --trusted-ca-file / --peer-trusted-ca-file also require a client/peer cert (etcd)
# --client-cert-auth=false does not opt out when a CA file is set
# --peer-client-cert-file / --peer-client-key-file override the outbound identity
./build/bin/cetcd --cert-file server.crt --key-file server.key \
  --trusted-ca-file ca.crt --client-cert-auth \
  --peer-cert-file peer.crt --peer-key-file peer.key \
  --peer-client-cert-file peer-cli.crt --peer-client-key-file peer-cli.key \
  --peer-trusted-ca-file peer-ca.crt --peer-client-cert-auth \
  --client-crl-file client.crl --peer-crl-file peer.crl

# Self-signed ECDSA P-256 into {data-dir}/fixtures/ (requires --data-dir)
# --self-signed-cert-validity is years (default 1; must be > 0)
# --auto-tls=false / --peer-auto-tls=false / --force-new-cluster=false are off, not unknown
# --enable-grpc-gateway=false / --enable-v2=false / --unsafe-no-fsync=false are already-off (true fail-closes)
# --max-snapshots / --max-wals / --client-cert-file / --backend-batch-* fail-close (not unknown, not a no-op)
# --v2-deprecation=write-only / --proxy=off / --discovery-fallback=exit are accepted (not-yet / proxy=on fail-close)
./build/bin/cetcd --auto-tls --peer-auto-tls --self-signed-cert-validity 1 \
  --data-dir ./data

# TLS cipher list (IANA or OpenSSL names, including TLS 1.3; requires certs)
# a TLS 1.3-only list disables TLS 1.2 (and a TLS 1.2-only list disables TLS 1.3)
# --tls-min-version / --tls-max-version are TLS1.2 or TLS1.3 (min default TLS1.2)
# --peer-cert-allowed-cn / --*-allowed-hostname require cert+CA; mismatch closes
./build/bin/cetcd --cert-file server.crt --key-file server.key \
  --tls-min-version TLS1.2 --tls-max-version TLS1.3 \
  --cipher-suites TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384 \
  --peer-cert-file peer.crt --peer-key-file peer.key \
  --peer-trusted-ca-file ca.crt --peer-cert-allowed-cn etcd

# https listen URLs require TLS certs (plaintext is not a silent fallback)
./build/bin/cetcd --listen-client-urls https://127.0.0.1:2379 \
  --listen-peer-urls https://127.0.0.1:2380 \
  --cert-file server.crt --key-file server.key \
  --peer-cert-file peer.crt --peer-key-file peer.key --data-dir ./data

# https peer URLs in --initial-cluster also require --peer-cert-file
# --initial-cluster peer URL port must be 1..65535; a typo is not a silent Raft bind on 0
./build/bin/cetcd --initial-cluster 1=https://127.0.0.1:2380 \
  --peer-cert-file peer.crt --peer-key-file peer.key --data-dir ./data

# Limit flags: --max-request-bytes, --max-txn-ops, --max-concurrent-streams
# --max-txn-ops must be 1..128; a typo or 0 is not the silent default 128
# --max-request-bytes must be > 0; a typo or 0 is not the silent default 1.5 MiB
# --max-concurrent-streams must be > 0; omitted leaves nghttp2's default
# (advertised N is clamped to CETCD_H2_MAX_STREAMS; each client and peer stream is tracked)
./build/bin/cetcd --max-txn-ops 128 --max-request-bytes 1572864 \
  --max-concurrent-streams 100 \
  --auth-token 'jwt,sign-method=RS256,priv-key=./jwt.pem,ttl=5m' \
  --auth-token-ttl 300 --bcrypt-cost 10
# --auth-token-ttl is simple-token lifetime in seconds (default 300; must be > 0)
# --bcrypt-cost is 0 or 4..31; a typo is not silent SHA-256

# gRPC keepalive applies TCP keepalive on client and peer sockets
# --grpc-keepalive-interval is the etcd name (same TCP idle as --grpc-keepalive-time)
# --grpc-keepalive-min-time is validated but not applied
# --grpc-keepalive-permit-without-stream is a bool (not applied); a typo fails at parse
# other --grpc-keepalive-* are unknown flags and do not swallow the next argv
# --logger zap|capnslog is accepted; other types fail at parse
# --log-level is trace|debug|info|warn|error (etcd aliases); = form is accepted
# --log-outputs=default fail-closes (etcd zap); systemd/journal is journal
# --log-format is json|text (etcd console = text); a typo fails at parse
./build/bin/cetcd --grpc-keepalive-interval 2h --grpc-keepalive-timeout 20s \
  --grpc-keepalive-min-time 5s --grpc-keepalive-permit-without-stream true \
  --logger=zap --log-level=info --log-format=text --log-outputs=stderr
# --enable-log-rotation requires a single file --log-outputs; compress=true fails
# --log-rotation-config-json '{"maxsize":100,"maxage":0,"maxbackups":0,"localtime":false,"compress":false}'

# Initial + periodic corrupt check: HashKV vs {data-dir}/backend.hash
# (mismatch or a lower current revision fail-closes; periodic raises CORRUPT).
# --experimental-compaction-batch-limit caps auto-compact revs per tick (0 unlimited).
# --experimental-compaction-sleep-interval waits between those batches (0 = none).
# --experimental-watch-progress-notify-interval sets Watch progress_notify (0 = 10s).
# --experimental-warning-apply-duration warns if apply is slower (0 disables; default 100ms).
# --experimental-warning-unary-request-duration warns if unary RPC is slower (0 disables; default 300ms).
# --experimental-max-learners caps learner MemberAdd (0 = none; omitted default 1).
# --experimental-memory-mlock locks process pages (Unix mlockall; Windows fail-closes).
# --experimental-bootstrap-defrag-threshold-megabytes compact-copies data.mdb at start (0 off).
# --experimental-snapshot-catchup-entries keeps N raft entries after compact
# (omitted default 5000; 0 = compact to applied; leftover text fails).
# --experimental-compact-hash-check-enabled compares follower compact HashKV
# (omitted default off; time default 1m; mismatch raises CORRUPT).
# --experimental-enable-lease-checkpoint persists remaining TTL (already on).
# --experimental-enable-lease-checkpoint-persist writes that TTL to LMDB (already on).
# Unimplemented or unknown --experimental-* fail at parse (=false is OK).
./build/bin/cetcd --experimental-initial-corrupt-check \
  --experimental-corrupt-check-time 10s \
  --experimental-compaction-batch-limit 1000 \
  --experimental-compaction-sleep-interval 100ms \
  --experimental-watch-progress-notify-interval 10s \
  --experimental-warning-apply-duration 100ms \
  --experimental-warning-unary-request-duration 300ms \
  --experimental-max-learners 1 \
  --experimental-memory-mlock=false \
  --experimental-bootstrap-defrag-threshold-megabytes 0 \
  --experimental-snapshot-catchup-entries 5000 \
  --experimental-compact-hash-check-enabled=false \
  --experimental-compact-hash-check-time 1m \
  --experimental-enable-lease-checkpoint \
  --experimental-enable-lease-checkpoint-persist

# Dedicated WAL directory (empty path fail-closes; requires --data-dir)
./build/bin/cetcd --data-dir ./data --wal-dir /var/lib/cetcd/wal

# DNS SRV bootstrap (cannot mix with --initial-cluster; 0 records fail-close)
# ./build/bin/cetcd --discovery-srv example.com --discovery-srv-name east

# File or journal log sink (mixed comma-lists fail-close)
# --enable-log-rotation rotates that file; requires a single file path
./build/bin/cetcd --log-outputs /var/log/cetcd.log --enable-log-rotation --data-dir ./data
# ./build/bin/cetcd --log-outputs journal --data-dir ./data

# Unknown flags fail at parse (not a silent ignore)
# ./build/bin/cetcd --not-a-real-flag  → error: unknown flag

# Raft timing parameters (actually applied; ticks must be > 0)
./build/bin/cetcd --election-tick 10 --heartbeat-tick 1 --pre-vote
# --pre-vote default on; --pre-vote=false skips PreVote (a non-bool fails)
# --initial-election-tick-advance default on (first campaign in 1 tick);
# --initial-election-tick-advance=false waits the full election timeout
# --heartbeat-interval / --election-timeout are milliseconds (1..50000);
# they set the Raft timer and election_tick (cannot mix with --*-tick)
# --raft-read-timeout / --raft-write-timeout recycle hung peer sockets
# (Go duration; omitted / <5s floor to 5s; leftover text fails)
# --experimental-wait-cluster-ready delays client listen until a leader exists
# (omitted default off; a non-bool fails)
```

---

## 3. Using `cetcdctl`

`cetcdctl` is a command-line client that speaks cetcd's gRPC protocol.
It mirrors `etcdctl` command structure for familiarity. Global and
subcommand flags accept etcdctl `--flag=value` (`--command-timeout=5s`,
`--lease=1`, `--rev=5`, `--write-out=json`, `--data-dir=DIR`); empty
`--flag=` and leftover text (`put --lease 10foo`, `watch cancel 10foo`)
fail-close. `--prefix=false` does not eat the key.

### KV operations

```sh
./build/bin/cetcdctl put foo bar
./build/bin/cetcdctl --cacert ./ca.crt put foo bar  # TLS custom-frame client
./build/bin/cetcdctl put foo bar --prev-kv         # Store and return previous value
./build/bin/cetcdctl put foo bar --prev-kv --print-value-only  # Output only previous value
./build/bin/cetcdctl put foo -                     # Read value from stdin
./build/bin/cetcdctl put --lease=1 foo bar         # Attach lease; leftover --lease 10foo fail-closes
# leftover truncated Put --lease fail-closes (cannot look like a no-lease put)
# leftover length-delimited bytes cannot steal the lease
# leftover put --foo k v / get k --foo fail-close (not a key or range_end)
# put -- --foo v writes key --foo
# leftover watch --foo / member remove --force / lock --foo name fail-close
./build/bin/cetcdctl get foo
# leftover cannot steal a printed get / txn key or alarm CORRUPT type
./build/bin/cetcdctl get --prefix foo           # Get all keys with prefix
./build/bin/cetcdctl get --prefix ""             # Get all keys (empty prefix = all)
./build/bin/cetcdctl get --from-key foo          # Get all keys >= foo
./build/bin/cetcdctl get --count-only foo        # Count matching keys only
# leftover-safe: leftover proto bytes cannot steal a printed count or more=true
./build/bin/cetcdctl get --keys-only foo         # Get keys without values
./build/bin/cetcdctl get --print-value-only foo  # Print only the value
./build/bin/cetcdctl get --hex foo               # Output in hex format
./build/bin/cetcdctl get --consistency s foo     # Serializable local read (ok on a follower)
./build/bin/cetcdctl get --consistency l foo     # Linearizable (default; fail-closes on a follower)
# --rev / --limit / --min-mod-rev and related flags must be integers >= 0; leftover text is not a truncated revision
# leftover truncated Range --rev fail-closes (cannot range the live tree)
# leftover length-delimited bytes cannot steal rev / limit
# equals-form is accepted: get --rev=5 --write-out=json foo
./build/bin/cetcdctl get --range-end zzz foo     # Get keys from foo to zzz
./build/bin/cetcdctl del foo
./build/bin/cetcdctl del --prefix foo            # Delete all keys with prefix
./build/bin/cetcdctl del --range-end zzz foo     # Delete keys from foo to zzz
./build/bin/cetcdctl del --prev-kv foo           # Return deleted key-values
./build/bin/cetcdctl del --prev-kv --print-value-only foo  # Output only deleted values
# leftover truncated DeleteRange range_end fail-closes (cannot look like a point delete)
# leftover length-delimited bytes cannot steal range_end and turn a point delete into a range delete
# leftover-safe: leftover proto bytes cannot steal a printed put/del prev key
# leftover-safe: leftover proto bytes cannot steal a printed del count
# leftover Txn-embedded Put/Range/DeleteRange cannot steal lease/rev/range_end (truncated inner field fail-closes the txn)
# leftover Txn Compare cannot steal result and flip a CAS (truncated result fail-closes)
# leftover TxnRequest cannot inject an extra success Put (truncated success-op length fail-closes)
```

### Watch streaming

`cetcdctl watch` creates a bidirectional gRPC stream to the server and receives
events in real time as keys change. Each watch request runs in its own coroutine,
so many concurrent watchers can share a single TCP connection.

```sh
# Watch a single key
./build/bin/cetcdctl watch foo

# Watch a key prefix
./build/bin/cetcdctl watch --prefix /services/

# Watch an explicit range
./build/bin/cetcdctl watch --range-end zzz foo

# Watch from a specific revision
./build/bin/cetcdctl watch --start-rev 42 foo
# --start-rev must be >= 0; leftover text is not a silent truncated revision
# leftover-safe: leftover proto bytes cannot steal start_rev / range_end
# leftover-safe: leftover proto bytes cannot steal a printed Event key
# leftover-safe: leftover proto bytes cannot steal fragment=true
# leftover-safe: leftover proto bytes cannot steal a printed compact cancel_reason
# leftover watch --foo key fail-closes (not a watch on key --foo)
# --fragment leftover-safe-splits oversized WatchResponses by --max-request-bytes

# Include the previous key-value in each event
./build/bin/cetcdctl watch --prev-kv foo

# Request periodic progress notifications from the server
./build/bin/cetcdctl watch --progress-notify foo

# Split oversized WatchResponses by --max-request-bytes (not a no-op)
./build/bin/cetcdctl watch --fragment foo

# Output events in hex format
./build/bin/cetcdctl watch --hex foo

# JSON output with full KV metadata and header
./build/bin/cetcdctl watch -w json foo

# Execute a command on each watch event
./build/bin/cetcdctl watch --exec 'echo $ETCD_WATCH_EVENT_TYPE $ETCD_WATCH_KEY' foo
# Sets ETCD_WATCH_EVENT_TYPE (PUT|DELETE), ETCD_WATCH_KEY, ETCD_WATCH_VALUE, ETCD_WATCH_REVISION env vars

# Interactive watch mode: create and cancel watches at runtime
./build/bin/cetcdctl watch -i
# Then type commands:
#   watch foo            — start watching key "foo"
#   watch bar --prefix   — start watching prefix "bar"
#   cancel 1             — cancel watch with ID 1
#   Ctrl+D                — exit interactive mode
```

The server keeps the stream open, delivering `WatchResponse` messages as matching
`Put`/`Delete` operations are committed. The client keeps the TCP connection open
and continuously reads responses, printing events as they arrive. Press `Ctrl-C`
to cancel the watch and close the stream.

### Lease management

```sh
./build/bin/cetcdctl lease grant 60                        # Grant via Raft (followers share the id)
# leftover cannot steal a printed or lock-used lease ID or TTL key
# leftover truncated LeaseGrant TTL fail-closes (cannot grant a silent 60s)
# TTL must be > 0; a typo is not a silent 0s lease
./build/bin/cetcdctl lease grant --lease-id 0x1234abcd 60  # Grant with custom lease ID (hex)
# --lease-id must be hex; leftover text is not id 0
./build/bin/cetcdctl lease grant 60 -w fields              # Grant with fields output
# leftover-safe: leftover proto bytes cannot steal a printed grant ID or TTL key
./build/bin/cetcdctl lease revoke 1                        # Revoke lease ID 1
# lease ID must be > 0; a typo is not lease id 0
# leftover truncated LeaseRevoke/KeepAlive id fail-closes (cannot look like success)
./build/bin/cetcdctl lease revoke 1 -w fields              # Revoke with fields output
./build/bin/cetcdctl lease timetolive 1                    # Check remaining TTL
./build/bin/cetcdctl lease timetolive --keys 1 -w fields   # Include keys with fields output
./build/bin/cetcdctl lease keepalive 1                     # Keepalive via Raft (followers share deadline)
# leftover cannot steal a printed or lock-used KeepAlive TTL
./build/bin/cetcdctl lease keepalive --once 1 -w fields   # Single keepalive with fields output
./build/bin/cetcdctl lease keepalive --interval 5 1        # Keep alive with custom 5-second interval
# --interval must be > 0; leftover text is not a silent truncated interval
./build/bin/cetcdctl lease list -w fields                  # List all leases in fields format
# leftover cannot steal a printed lease ID
```

### Transactions

```sh
./build/bin/cetcdctl txn put foo bar       # Transactional put
./build/bin/cetcdctl txn put -w fields foo bar  # Transactional put with fields output
# leftover txn put --foo k v / txn put -w json as KEY fail-close
# leftover dummy 0x00 cannot eat a Txn Put key tag and skip the perm check
# leftover auth login --foo / downgrade enable --foo fail-close
# leftover Authenticate proto bytes cannot steal a name, password, or printed token (truncated password fail-closes)
# leftover UserAdd / UserChangePassword proto bytes cannot steal a password or no_password
./build/bin/cetcdctl txn cas foo old new        # Compare-and-swap
./build/bin/cetcdctl txn cas -w fields foo old new  # CAS with fields output
./build/bin/cetcdctl txn get foo                # Transactional get
./build/bin/cetcdctl txn get -w fields foo       # Transactional get with fields output
./build/bin/cetcdctl txn del --prefix foo       # Transactional prefix delete
./build/bin/cetcdctl txn del -w fields --prefix foo  # Transactional delete with fields output
```

### Compaction

```sh
./build/bin/cetcdctl compact 100           # Compact via Raft (followers share compacted_rev)
# REV must be > 0; leftover text is not a silent truncated revision
# leftover flags (compact 10 --rev 5) fail-close; --physical waits (already sync)
# leftover Compact proto bytes cannot overwrite the revision (truncated field 1 fail-closes)
```

### Cluster management

```sh
./build/bin/cetcdctl status                # Status: version, dbSize, dbSizeInUse, raft index/applied, alarms
# leftover cannot steal a printed NOSPACE / CORRUPT error
./build/bin/cetcdctl alarm list                       # List alarms (persisted across restart)
# leftover truncated Alarm action fail-closes (cannot look like GET)
# leftover length-delimited bytes cannot steal ACTIVATE
./build/bin/cetcdctl alarm activate NOSPACE           # Activate NOSPACE via Raft
./build/bin/cetcdctl alarm activate CORRUPT           # Activate CORRUPT via Raft
./build/bin/cetcdctl alarm disarm                    # Disarm via Raft (followers drop the flag)
./build/bin/cetcdctl member list                     # List cluster members (linearizable; default true)
./build/bin/cetcdctl member list --linearizable=false # Serializable MemberList (ok on a follower)
# leftover member list --linearizable --foo fail-closes (cannot list members)
# leftover cannot steal a printed member name
# leftover cannot steal a printed peerURL or isLearner
./build/bin/cetcdctl member add --peer-urls http://localhost:2380 --name node2  # Add member with name
# leftover-safe: leftover proto bytes cannot steal the peerURL or isLearner
# peer URL port must be 1..65535; leftover 2380foo is not truncated port 2380
./build/bin/cetcdctl member remove 1234567890         # Remove member (refused if it would lose quorum)
# member ID is hex > 0; leftover text is not a truncated decimal id
# leftover truncated MemberRemove/Promote id fail-closes (cannot look like success)
# leftover member remove --force / member add --foo URL fail-close
./build/bin/cetcdctl member update 1234567890 http://localhost:2380  # Update member peer URL
# leftover truncated MemberUpdate id / peerURL length fail-closes (cannot look like success)
./build/bin/cetcdctl member promote 1234567890        # Promote learner to voting member
```

### Distributed locks and leader election

```sh
# Acquire a distributed lock (blocks until acquired)
# A keepalive child process is automatically forked to renew the lease
# while the lock is held, preventing it from expiring.
./build/bin/cetcdctl lock mylock               # Prints lock key, waits for signal
# leftover cannot steal Txn succeeded (false lock / elect)
./build/bin/cetcdctl lock --ttl 30 mylock     # Lock with 30s lease TTL
# --ttl must be > 0; leftover text is not a silent truncated TTL
# leftover lock --foo name / elect --foo name fail-close (not a lock/election named --foo)
./build/bin/cetcdctl lock --print-value-only mylock  # Print only lease ID
./build/bin/cetcdctl lock mylock echo done    # Run command while holding lock
./build/bin/cetcdctl lock -w json mylock       # Lock with JSON output (header+key)
./build/bin/cetcdctl lock -w fields mylock     # Lock with fields output (header+key)

# Leader election (blocks until elected)
# A keepalive child process is automatically forked to renew the lease
# while the leader holds the election, preventing it from expiring.
./build/bin/cetcdctl elect myelection         # Campaign with default proposal
./build/bin/cetcdctl elect --ttl 30 myelection "leader"  # With custom proposal
./build/bin/cetcdctl elect --print-value-only myelection  # Print only lease ID
./build/bin/cetcdctl elect -w json myelection "leader"    # Election with JSON output
./build/bin/cetcdctl elect -w fields myelection "leader"  # Election with fields output
```

### Authentication (RBAC)

```sh
# Enable authentication (requires a `root` user first)
./build/bin/cetcdctl user add root
./build/bin/cetcdctl auth enable               # Enable via Raft (followers share the switch)

# Subsequent commands must authenticate. The token is attached to each RPC.
./build/bin/cetcdctl --user root:PASS put foo bar
./build/bin/cetcdctl --user root:PASS get foo

# User management
./build/bin/cetcdctl user add root         # Create user
./build/bin/cetcdctl user add root --no-password  # Create user without password (cert-based auth)
./build/bin/cetcdctl user get root         # View user details (roles)
# leftover-safe: leftover proto bytes cannot steal the name and look up the wrong user
./build/bin/cetcdctl user list             # List all users
./build/bin/cetcdctl user change-password root NEWPASS  # Change via Raft (followers share the hash)
./build/bin/cetcdctl user grant-role root admin         # Grant via Raft (followers share the binding)
./build/bin/cetcdctl user revoke-role root admin        # Revoke via Raft (followers drop the binding)
# leftover-safe: leftover proto bytes cannot steal the user/role and grant or revoke the wrong binding
./build/bin/cetcdctl user delete root                   # Delete via Raft (followers drop the user)
# leftover-safe: leftover proto bytes cannot steal the name and delete the wrong user

# Role management
./build/bin/cetcdctl role add admin        # Create via Raft (followers share the role)
./build/bin/cetcdctl role get admin        # View role permissions
# leftover cannot steal a printed range_end
# leftover-safe: leftover proto bytes cannot steal the role name
# leftover-safe: leftover proto bytes cannot steal a printed key or permType
./build/bin/cetcdctl role list             # List all roles
./build/bin/cetcdctl role delete admin     # Delete via Raft (followers drop the role)
# leftover-safe: leftover proto bytes cannot steal the name and delete the wrong role

# Permission management
./build/bin/cetcdctl role grant-permission admin readwrite /foo    # Grant via Raft (followers share the perm)
# leftover-safe: leftover proto bytes cannot steal the role, key, range_end, or permType
./build/bin/cetcdctl role grant-permission admin read /foo --prefix  # Grant permission on a key prefix
./build/bin/cetcdctl role grant-permission admin read /foo --from-key  # FromKey: all keys >= /foo
./build/bin/cetcdctl role grant-permission admin read /foo --range-end /bar  # Grant [ /foo, /bar )
# leftover --range-end --from-key cannot eat a flag as the range; extra leftover --foo fail-closes
./build/bin/cetcdctl role revoke-permission admin                  # Revoke all perms via Raft
./build/bin/cetcdctl role revoke-permission admin readwrite /foo  # Revoke matching prefix only
./build/bin/cetcdctl role revoke-permission admin /foo --from-key  # Revoke FromKey [ /foo, +inf )
./build/bin/cetcdctl role revoke-permission admin read /foo --prefix  # Revoke prefix permission

# Disable authentication
./build/bin/cetcdctl auth disable

# JSON output for all auth commands
./build/bin/cetcdctl auth enable -w json
./build/bin/cetcdctl user list -w json
./build/bin/cetcdctl role list -w json

# Fields output for auth commands
./build/bin/cetcdctl auth status -w fields
# leftover cannot steal auth enabled or a printed user/role name
./build/bin/cetcdctl auth login -w fields root mypassword  # Login with fields output (token)
# leftover cannot steal a used Authenticate token
./build/bin/cetcdctl user list -w fields
./build/bin/cetcdctl role list -w fields
```

### Snapshot and maintenance

```sh
./build/bin/cetcdctl snapshot save backup.snap   # Save KV snapshot to file
# leftover cannot steal the snapshot blob written to disk
./build/bin/cetcdctl snapshot save backup.snap --compaction-periodical  # With etcd-compatible flag (no-op)
# leftover snapshot save --foo fail-closes (not a file named --foo)
./build/bin/cetcdctl snapshot save backup.snap -w json  # Save with JSON output
./build/bin/cetcdctl snapshot save backup.snap -w fields  # Save with fields output
./build/bin/cetcdctl snapshot status backup.snap # Show snapshot file info
./build/bin/cetcdctl snapshot restore backup.snap --data-dir /tmp/cetcd  # Restore snapshot
./build/bin/cetcdctl snapshot restore backup.snap --data-dir /tmp/cetcd --wal-dir /fast/wal
./build/bin/cetcdctl snapshot restore backup.snap --data-dir /tmp/cetcd --bump-revision --mark-compacted
# leftover --data-dir --wal-dir / --mark-compacted without --bump-revision fail-close
./build/bin/cetcdctl snapshot restore backup.snap --data-dir /tmp/cetcd --force  # Force overwrite
./build/bin/cetcdctl snapshot restore backup.snap --data-dir /tmp/cetcd -w json  # Restore with JSON output
./build/bin/cetcdctl snapshot restore backup.snap --data-dir /tmp/cetcd -w fields  # Restore with fields output
./build/bin/cetcdctl snapshot restore backup.snap --data-dir /tmp/cetcd --skip-hash-check  # Restore despite a CTS2 CRC mismatch
./build/bin/cetcdctl snapshot restore backup.snap --data-dir /tmp/cetcd --initial-cluster-token etcd-cluster  # Persist cluster token
./build/bin/cetcdctl snapshot restore backup.snap --data-dir /tmp/cetcd --initial-cluster-state new
./build/bin/cetcdctl snapshot restore backup.snap --data-dir /tmp/cetcd --initial-cluster-state existing --force
./build/bin/cetcdctl snapshot restore backup.snap --data-dir /tmp/cetcd \
  --initial-cluster 1=http://127.0.0.1:2380,2=http://127.0.0.1:2382 \
  --name n1 --initial-advertise-peer-urls http://127.0.0.1:2380 \
  --initial-cluster-state existing  # persist peers/name so start can omit them
./build/bin/cetcdctl get --prefix foo --count-only -w fields  # Count-only with fields output
./build/bin/cetcdctl endpoint health -w json  # Health check with ResponseHeader (IPv6 is [::1]:2379)
# leftover cannot steal a printed JSON revision
./build/bin/cetcdctl endpoint status -w table  # Status in table format
# leftover cannot steal a printed version, dbSize, or isLearner
# leftover cannot steal a used --cluster client URL
./build/bin/cetcdctl endpoint hashkv --rev 10 -w json  # HashKV at rev (leftover 10foo fail-closes)
./build/bin/cetcdctl endpoint hashkv -w fields  # HashKV with fields output
./build/bin/cetcdctl alarm list                           # List all alarms
./build/bin/cetcdctl alarm activate NOSPACE               # Activate NOSPACE alarm
./build/bin/cetcdctl alarm activate CORRUPT               # Activate CORRUPT alarm
./build/bin/cetcdctl alarm disarm                        # Disarm all alarms
./build/bin/cetcdctl downgrade validate 0.3.0     # Confirm cluster is already at 0.3.0
# enable / cancel / other versions fail-closed (on-disk format cannot change)
# leftover cannot steal ENABLE or a VALIDATE version; truncated action/version fail-closes
# leftover-safe: leftover proto bytes cannot steal a printed cluster version
./build/bin/cetcdctl check perf                   # Run performance check (put/get latency)
# leftover check perf --foo / check datascale --foo fail-close (cannot start a write load)
# leftover --load --prefix fail-close (cannot eat --prefix as the load)
./build/bin/cetcdctl version                 # Print client version
# leftover version --foo fail-close (cannot print version)
./build/bin/cetcdctl check perf --load s            # Small load (10 keys)
./build/bin/cetcdctl check perf --load m            # Medium load (100 keys)
./build/bin/cetcdctl check perf --load l            # Large load (1000 keys)
./build/bin/cetcdctl check perf --prefix mycheck    # Custom key prefix
./build/bin/cetcdctl check perf -w json          # Performance check with JSON output
./build/bin/cetcdctl check perf -w fields         # Performance check with fields output
./build/bin/cetcdctl check datascale --load 1000  # Test database scalability with 1000 keys
# --load must be > 0; a typo or 0 is not the silent default 10000
./build/bin/cetcdctl check datascale -w json --load 5000  # Datascale test with JSON output
./build/bin/cetcdctl check datascale -w fields --load 5000  # Datascale test with fields output
./build/bin/cetcdctl del key1 --prev-kv --hex        # Delete with prev-kv in hex format
./build/bin/cetcdctl txn put -w fields mykey myvalue        # Transactional put with fields output
./build/bin/cetcdctl txn cas -w fields mykey oldval newval  # CAS with fields output
./build/bin/cetcdctl txn get -w fields mykey              # Transactional get with fields output
./build/bin/cetcdctl txn del -w fields --prefix mykey      # Transactional delete with fields output
./build/bin/cetcdctl txn del --prefix --prev-kv mykey  # Transactional prefix delete
./build/bin/cetcdctl txn del --from-key mykey         # Transactional range delete (all keys >= mykey)
./build/bin/cetcdctl member list -w fields            # List members in fields format
./build/bin/cetcdctl endpoint status -w fields        # Endpoint status in fields format
./build/bin/cetcdctl compact -w fields 5              # Compact with fields output
./build/bin/cetcdctl defrag -w fields                # Defragment with fields output
./build/bin/cetcdctl defrag --cluster                # Defrag every linearizable MemberList client URL (follower fail-closes)
./build/bin/cetcdctl defrag --data-dir ./data        # Offline LMDB compact-copy (server stopped)
# leftover --data-dir --cluster / --cluster --data-dir fail-close
# leftover hash --rev / status --cluster / compact 10 --rev fail-close
./build/bin/cetcdctl move-leader -w fields 1234567890  # Transfer leadership with fields output
# leftover truncated MoveLeader target fail-closes (cannot look like a successful transfer)
# TARGET_ID is hex > 0; leftover text is not a truncated decimal id
./build/bin/cetcdctl snapshot status backup.snap -w fields  # Snapshot info in fields format
./build/bin/cetcdctl downgrade validate 0.3.0 -w fields  # Validate current version with fields output
```

### Shell completion

Shell completion scripts can be generated for bash, zsh, and fish:

```sh
# Bash: source the completion script
./build/bin/cetcdctl completion bash >> ~/.bashrc
# or: eval "$(./build/bin/cetcdctl completion bash)"

# Zsh: save to a file in fpath
./build/bin/cetcdctl completion zsh > ~/.zsh/completions/_cetcdctl

# Fish: save to completions directory
./build/bin/cetcdctl completion fish > ~/.config/fish/completions/cetcdctl.fish
# leftover completion bash --foo fail-close (cannot dump a script)
```

### Interactive transaction (`txn -i`)

The `txn -i` mode reads a transaction definition from stdin and executes it atomically.
The format is line-based with three sections: compare conditions, success operations,
and failure operations:

```sh
# Example: if key "counter" has value "5", increment it; otherwise create it
echo 'cmp counter = 5
then
put counter 6
else
put counter 1' | ./build/bin/cetcdctl txn -i

# Example with revision comparison and multiple operations
echo '# Compare mod revision
# If mod_revision of "lock" > 10, delete it and get the result
cmp_mod lock > 10
then
del lock
get lock
else
get lock' | ./build/bin/cetcdctl txn -i -w json
```

Supported compare commands:
- `cmp KEY OP VALUE` — compare key's value (OP: `=`, `==`, `!=`, `>`, `<`)
- `cmp_create KEY OP N` — compare key's create revision
- `cmp_mod KEY OP N` — compare key's mod revision
- `cmp_ver KEY OP N` — compare key's version

Supported operations (in `then`/`else` sections):
- `put KEY VALUE` — store a key-value pair
- `get KEY [RANGE_END]` — retrieve key(s)
- `del KEY [RANGE_END]` — delete key(s)

### Lease keepalive with custom interval

```sh
# Keep a lease alive with a 5-second interval (default is ttl/2)
./build/bin/cetcdctl lease keepalive --interval 5 123

# Single keepalive with custom interval
./build/bin/cetcdctl lease keepalive --interval 5 --once 123
```

### Member add with multiple peer URLs

```sh
# Add a member with comma-separated peer URLs
./build/bin/cetcdctl member add --peer-urls http://10.0.0.1:2380,http://10.0.0.2:2380 --name node2

# Update a member's peer URLs (also comma-separated)
./build/bin/cetcdctl member update 2 http://10.0.0.1:2380,http://10.0.0.2:2380
```

### Endpoint cluster operations

```sh
# Check health of all cluster members
# leftover member client URL ports fail-close (missing port → 2379)
./build/bin/cetcdctl endpoint health --cluster
./build/bin/cetcdctl endpoint health --cluster -w json

# Get status of all cluster members
./build/bin/cetcdctl endpoint status --cluster
./build/bin/cetcdctl endpoint status --cluster -w table

# Get KV hash of all cluster members
./build/bin/cetcdctl endpoint hashkv --cluster
./build/bin/cetcdctl endpoint hashkv --cluster -w json
```

### Global options

| Option | Default | Description |
|--------|---------|-------------|
| `--host ADDR` | 127.0.0.1 | Server address |
| `--port PORT` | 2379 | Server port (`1..65535`; a typo fail-closes) |
| `--endpoints EP` | 127.0.0.1:2379 | Comma-separated endpoints (failover in order; host:port `1..65535`; a typo fail-closes; `https://` requires `--cacert` or `--insecure`) |
| `--command-timeout SEC` | none | Timeout for commands (integer seconds or Go duration: `5s`, `1m`, `1m30s`, `500ms`; `0` = none; invalid fails) |
| `--user USER:PASS` | none | Authenticate with server before command |
| `--password PASS` | none | Password for `--user` (when USER has no `:PASS`) |
| `--debug` | off | Print RPC path and response size |
| `--insecure` | off | Skip TLS verify when TLS is on; required with `https://` endpoints if no `--cacert` |
| `--insecure-skip-tls-verify` | off | Same as `--insecure` |
| `--insecure-transport` | off | Force plaintext; fail-closes if mixed with `--cacert`/`--cert`/`--key` or `https://` endpoints |
| `--dial-timeout SEC` | none | Connection timeout (`0..86400`; optional `s`; `0` = none; invalid fails) |
| `--keepalive-time SEC` | none | TCP keepalive idle seconds (`0` disables; `0..86400`) |
| `--keepalive-timeout SEC` | none | TCP keepalive interval (requires `--keepalive-time`) |
| `--cacert FILE` | none | TLS CA certificate (enables TLS; missing file fail-closes) |
| `--cert FILE` | none | TLS client certificate (requires `--key`) |
| `--key FILE` | none | TLS client key (requires `--cert`) |
| `--max-call-send-msg-size N` | none | Max request payload in bytes (`0` rejected) |
| `--max-call-recv-msg-size N` | none | Max response payload in bytes (`0` rejected; no silent truncate) |
| `--discovery-srv DOMAIN` | none | DNS SRV `_etcd-client[-ssl]._tcp.<domain>` (0 records fail-closed; cannot mix with `--endpoints`) |
| `--discovery-srv-name NAME` | none | Optional SRV service suffix (requires `--discovery-srv`) |

### Table output formats

Several commands support `-w table` for tabular output:

```sh
# Endpoint health in table format
./build/bin/cetcdctl endpoint health -w table
./build/bin/cetcdctl endpoint health --cluster -w table

# Snapshot save in table format
./build/bin/cetcdctl snapshot save backup.snap -w table

# Snapshot status in table format (default)
./build/bin/cetcdctl snapshot status backup.snap -w table

# Hash/hashkv in table format
./build/bin/cetcdctl hash -w table
# leftover cannot steal a printed hash or compact_revision
./build/bin/cetcdctl hashkv --rev 0 -w table  # 0 / omitted = current; leftover 10foo fail-closes
# a truncated HashKV revision proto fail-closes (cannot hash the live tree)

# Endpoint hashkv in table format
./build/bin/cetcdctl endpoint hashkv -w table
./build/bin/cetcdctl endpoint hashkv --cluster -w table

# Other table-format commands
./build/bin/cetcdctl get --prefix foo -w table
./build/bin/cetcdctl member list -w table
./build/bin/cetcdctl lease list -w table
./build/bin/cetcdctl alarm list -w table
./build/bin/cetcdctl endpoint status -w table
```

---

## 4. Migrating from etcd

`cetcd-migrate` is a one-way migration tool that reads an etcd data directory
(bbolt backend, WAL, and snapshot files) and writes a cetcd-native LMDB
environment plus a cetcd-compatible WAL.

```sh
./build/bin/cetcd-migrate \
  --data-dir /path/to/etcd/data \
  --output-dir /path/to/cetcd/data \
  [--verbose]
```

| Flag | Description |
|------|-------------|
| `--data-dir PATH` | Source etcd data directory (contains `member/snap/db`, WAL segments, etc.). Leftover-safe (`--data-dir=PATH` or next argv; `--data-dir --output-dir` cannot eat a flag as the path; empty `--data-dir=` fail-closes) |
| `--output-dir PATH` | Destination directory for the converted LMDB env and WAL (same leftover-safe `=` / next-argv rules) |
| `--verbose` | Print per-key progress and summary statistics (`--verbose` / `--verbose=true` on; `--verbose=false` off) |

The destination directory must not already contain a cetcd database; the tool
refuses to overwrite existing data as a safety measure. After migration, start
`cetcd` pointing at `--output-dir` to serve the converted data.

Latest-snapshot selection leftover-safe-parses etcd `%016x-%016x.snap`
(hex). `123foo-456.snap` cannot win with a truncated decimal term, and
`000000000000000a.snap` is index 10 (not `atoll` 0). WAL listing leftover-safe-parses
`%016x-%016x.wal` so `123foo-456.wal` cannot be scanned and win latest.

> **Note:** `cetcd-migrate` performs an offline, one-way conversion. Always back up
> the original etcd data before running it.

---

## 5. Observability

### Logs

cetcd outputs structured logs to stderr. Log level can be controlled at
build time via compile definitions.

### Metrics

cetcd exposes Prometheus-compatible metrics on a dedicated HTTP listener.
Use `--metrics-port` to change the port (default: `2381`; `0` disables;
a typo fail-closes). `--listen-metrics-urls` is a UniqueURLs comma list
of `http(s)://host:port` (cannot mix with `--metrics-port`; mixed
scheme allowed; `https://` needs `--cert-file` / `--auto-tls`;
duplicate / leftover fail-closes). `--metrics basic|extensive` sets scrape detail (omitted
`basic`; `extensive` records unary `grpc_server_handling_seconds`;
other values fail-close). `--socket-reuse-port` sets `SO_REUSEPORT` on
listeners (omitted default off; Windows fail-closes; a non-bool
fail-closes).

```sh
# Start cetcd with the default metrics port
./build/bin/cetcd --data-dir ./data --listen 127.0.0.1 --port 2379

# Scrape metrics
curl http://127.0.0.1:2381/metrics
# --host-whitelist localhost,127.0.0.1 rejects other Host headers (403)
# leftover --host-whitelist --name fail-close (cannot eat a flag as the Host)
# omitted / * / empty / --host-whitelist= allows all (etcd default)
# --metrics extensive records unary grpc_server_handling_seconds histograms
# --metrics basic (omitted default) keeps counters/gauges only
# --socket-reuse-port sets SO_REUSEPORT (Windows fail-closes)

# etcd-compatible health (200 + {"health":"true"} or 503 + reason)
# ?serializable=true skips the leader check; exclude=NOSPACE|CORRUPT skips that alarm
curl http://127.0.0.1:2381/health
```

The `/metrics` endpoint returns counters, gauges, and histograms in Prometheus
text format. Key families include:

| Metric | Type | Description |
|--------|------|-------------|
| `cetcd_grpc_requests_total` | counter | Total gRPC requests by service/method |
| `cetcd_raft_ticks_total` | counter | Total Raft tick timer firings |
| `cetcd_mvcc_revision` | gauge | Current MVCC revision |
| `cetcd_lease_active` | gauge | Number of active leases |
| `grpc_server_handling_seconds` | histogram | Unary RPC duration (`--metrics=extensive` only) |

### Profiling

`--enable-pprof` exposes pprof-style endpoints on the metrics port (`2381` by default)
under `/debug/pprof/`. Omitted default is off (etcd 3.5); a non-bool fail-closes.
Without the flag those paths return 404. `/metrics` stays available.

| Endpoint | Description |
|----------|-------------|
| `GET /debug/pprof/profile?seconds=N` | CPU profile for `N` seconds (default 30; leftover `30foo` is 400) |
| `GET /debug/pprof/heap` | Heap profile (in-use and allocated) |
| `GET /debug/pprof/coroutines` | Snapshot of active libco coroutines and their states |

```sh
# Start with pprof enabled
./build/bin/cetcd --enable-pprof --data-dir ./data

# 10-second CPU profile
curl -o cpu.prof http://127.0.0.1:2381/debug/pprof/profile?seconds=10

# Heap profile
curl -o heap.prof http://127.0.0.1:2381/debug/pprof/heap

# Coroutine snapshot
curl http://127.0.0.1:2381/debug/pprof/coroutines
```

Profiles are folded-stack text (CPU) or text dumps (heap / coroutines), not
protobuf. Collection for `/debug/pprof/profile` runs off the Raft reactor
(`uv_queue_work`); a second concurrent profile returns HTTP 409.

---

## 6. Troubleshooting

### Common issues

**Connection refused**: Ensure the cetcd daemon is running and listening
on the expected port (`./build/bin/cetcd --data-dir ./data`).

**Tests fail with ASan errors**: Ensure you're using a debug build with
sanitizers enabled (`-DCMAKE_BUILD_TYPE=Debug -DCETCD_SANITIZERS=address,undefined`).

**Build fails on OpenSSL**: Ensure OpenSSL 3.0+ development headers are
installed (`pkg-config --modversion openssl`).

**HTTP/2 module is stub-only**: Install nghttp2 development headers
(`pkg-config --modversion libnghttp2`). Without nghttp2, HTTP/2 session
management is disabled and only gRPC framing helpers are available.

For further help, see `docs/architecture.md` for the system design, or
open an issue at <https://github.com/konyka/cetcd/issues>.
