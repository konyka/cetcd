# cetcd Usage

> **Status**: actively developed. cetcd supports the full etcd v3.5 gRPC API (41 RPCs).

## 1. Building from source

### Prerequisites

- A C compiler with C11 `<stdatomic.h>` support:
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
| `CETCD_BUILD_FUZZ`           | `OFF`   | Build libFuzzer fuzz targets (requires clang)          |
| `CETCD_BUILD_BENCH`          | `OFF`   | Build benchmark suite                                  |
| `CETCD_SANITIZERS`           | (empty) | Comma-separated: `address,undefined,thread,memory`     |
| `CETCD_USE_SYSTEM_OPENSSL`   | `ON`    | Use `find_package(OpenSSL)`; otherwise vendor          |
| `CETCD_USE_SYSTEM_LMDB`      | `OFF`   | Use system LMDB instead of vendored                    |
| `CETCD_ENABLE_LTO`           | `OFF`   | Link-time optimisation (Release builds)                |
| `CETCD_ENABLE_COVERAGE`      | `OFF`   | gcov-style coverage flags                              |

### Sanitized debug build (developer default)

```sh
cmake -B build -G Ninja \
  -DCMAKE_BUILD_TYPE=Debug \
  -DCETCD_SANITIZERS=address,undefined
cmake --build build
ctest --test-dir build --output-on-failure
```

---

## 2. Running `cetcd`

### Single-node server

```sh
./build/bin/cetcd --data-dir ./data --listen 127.0.0.1 --port 2379
```

This starts a single-node cetcd server listening on port 2379 for client
requests and port 2380 for peer-to-peer (Raft) communication.

### Three-node static cluster

```sh
# Node 1
./build/bin/cetcd --name node1 --data-dir ./data1 \
  --listen 127.0.0.1 --port 2379 --peer-port 2380 \
  --initial-cluster node1=127.0.0.1:2380,node2=127.0.0.1:2382,node3=127.0.0.1:2384

# Node 2
./build/bin/cetcd --name node2 --data-dir ./data2 \
  --listen 127.0.0.1 --port 2381 --peer-port 2382 \
  --initial-cluster node1=127.0.0.1:2380,node2=127.0.0.1:2382,node3=127.0.0.1:2384

# Node 3
./build/bin/cetcd --name node3 --data-dir ./data3 \
  --listen 127.0.0.1 --port 2383 --peer-port 2384 \
  --initial-cluster node1=127.0.0.1:2380,node2=127.0.0.1:2382,node3=127.0.0.1:2384
```

The cluster uses Raft consensus for replication. Leader election happens
automatically within ~1 second of startup.

### etcd-compatible server flags

cetcd accepts several etcd server flags for migration compatibility:

```sh
# Using etcd-style URL flags
./build/bin/cetcd --listen-client-urls http://127.0.0.1:2379 \
  --listen-peer-urls http://127.0.0.1:2380 --data-dir ./data

# etcd flags accepted as no-op for compatibility
./build/bin/cetcd --advertise-client-urls http://127.0.0.1:2379 \
  --initial-advertise-peer-urls http://127.0.0.1:2380 \
  --initial-cluster-state new --initial-cluster-token etcd-cluster \
  --snapshot-count 10000 --quota-backend-bytes 2147483648 \
  --data-dir ./data
```

---

## 3. Using `cetcdctl`

`cetcdctl` is a command-line client that speaks cetcd's gRPC protocol.
It mirrors `etcdctl` command structure for familiarity.

### KV operations

```sh
./build/bin/cetcdctl put foo bar
./build/bin/cetcdctl get foo
./build/bin/cetcdctl get --prefix foo           # Get all keys with prefix
./build/bin/cetcdctl get --prefix ""             # Get all keys (empty prefix = all)
./build/bin/cetcdctl get --from-key foo          # Get all keys >= foo
./build/bin/cetcdctl get --count-only foo        # Count matching keys only
./build/bin/cetcdctl get --keys-only foo         # Get keys without values
./build/bin/cetcdctl get --print-value-only foo  # Print only the value
./build/bin/cetcdctl del foo
./build/bin/cetcdctl del --prefix foo            # Delete all keys with prefix
./build/bin/cetcdctl del --prev-kv foo           # Return deleted key-values
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

# Watch from a specific revision
./build/bin/cetcdctl watch --start-rev 42 foo

# Include the previous key-value in each event
./build/bin/cetcdctl watch --prev-kv foo

# JSON output with full KV metadata and header
./build/bin/cetcdctl watch -w json foo
```

The server keeps the stream open, delivering `WatchResponse` messages as matching
`Put`/`Delete` operations are committed. Press `Ctrl-C` to cancel the watch and close
the stream.

### Lease management

```sh
./build/bin/cetcdctl lease grant 60        # Grant a 60-second lease
./build/bin/cetcdctl lease revoke 1        # Revoke lease ID 1
./build/bin/cetcdctl lease timetolive 1    # Check remaining TTL
./build/bin/cetcdctl lease keepalive 1     # Keep lease alive (loop)
./build/bin/cetcdctl lease list            # List all leases
```

### Transactions

```sh
./build/bin/cetcdctl txn put foo bar       # Transactional put
```

### Compaction

```sh
./build/bin/cetcdctl compact 100           # Compact MVCC history to revision 100
```

### Cluster management

```sh
./build/bin/cetcdctl status                # Server status (version, raft index, etc.)
./build/bin/cetcdctl alarm list                       # List all alarms
./build/bin/cetcdctl alarm activate NOSPACE           # Activate NOSPACE alarm
./build/bin/cetcdctl alarm activate CORRUPT           # Activate CORRUPT alarm
./build/bin/cetcdctl alarm disarm                    # Disarm alarms
./build/bin/cetcdctl member list                     # List cluster members
./build/bin/cetcdctl member add --peer-urls http://localhost:2380 --name node2  # Add member with name
./build/bin/cetcdctl member remove 1234567890         # Remove member by ID
./build/bin/cetcdctl member update 1234567890 http://localhost:2380  # Update member peer URL
./build/bin/cetcdctl member promote 1234567890        # Promote learner to voting member
```

### Distributed locks and leader election

```sh
# Acquire a distributed lock (blocks until acquired)
./build/bin/cetcdctl lock mylock               # Prints lock key, waits for signal
./build/bin/cetcdctl lock --ttl 30 mylock     # Lock with 30s lease TTL
./build/bin/cetcdctl lock --print-value-only mylock  # Print only lease ID
./build/bin/cetcdctl lock mylock echo done    # Run command while holding lock

# Leader election (blocks until elected)
./build/bin/cetcdctl elect myelection         # Campaign with default proposal
./build/bin/cetcdctl elect --ttl 30 myelection "leader"  # With custom proposal
./build/bin/cetcdctl elect --print-value-only myelection  # Print only lease ID
```

### Authentication (RBAC)

```sh
# Enable authentication
./build/bin/cetcdctl auth enable

# User management
./build/bin/cetcdctl user add root         # Create user
./build/bin/cetcdctl user get root         # View user details (roles)
./build/bin/cetcdctl user list             # List all users
./build/bin/cetcdctl user change-password root NEWPASS  # Change password
./build/bin/cetcdctl user grant-role root admin         # Grant role to user
./build/bin/cetcdctl user revoke-role root admin        # Revoke role from user
./build/bin/cetcdctl user delete root                   # Delete user

# Role management
./build/bin/cetcdctl role add admin        # Create role
./build/bin/cetcdctl role get admin        # View role permissions
./build/bin/cetcdctl role list             # List all roles
./build/bin/cetcdctl role delete admin     # Delete role

# Permission management
./build/bin/cetcdctl role grant-permission admin readwrite /foo
./build/bin/cetcdctl role revoke-permission admin

# Disable authentication
./build/bin/cetcdctl auth disable

# JSON output for all auth commands
./build/bin/cetcdctl auth enable -w json
./build/bin/cetcdctl user list -w json
./build/bin/cetcdctl role list -w json
```

### Snapshot and maintenance

```sh
./build/bin/cetcdctl snapshot save backup.snap   # Save KV snapshot to file
./build/bin/cetcdctl snapshot save backup.snap --compaction-periodical  # With etcd-compatible flag (no-op)
./build/bin/cetcdctl snapshot save backup.snap -w json  # Save with JSON output
./build/bin/cetcdctl snapshot status backup.snap # Show snapshot file info
./build/bin/cetcdctl snapshot restore backup.snap --data-dir /tmp/cetcd  # Restore snapshot
./build/bin/cetcdctl snapshot restore backup.snap --data-dir /tmp/cetcd --force  # Force overwrite
./build/bin/cetcdctl snapshot restore backup.snap --data-dir /tmp/cetcd -w json  # Restore with JSON output
./build/bin/cetcdctl endpoint health -w json  # Health check with ResponseHeader
./build/bin/cetcdctl endpoint status -w table  # Status in table format
./build/bin/cetcdctl endpoint hashkv -w json   # HashKV with ResponseHeader
./build/bin/cetcdctl alarm list                           # List all alarms
./build/bin/cetcdctl alarm activate NOSPACE               # Activate NOSPACE alarm
./build/bin/cetcdctl alarm activate CORRUPT               # Activate CORRUPT alarm
./build/bin/cetcdctl alarm disarm                        # Disarm all alarms
./build/bin/cetcdctl downgrade enable             # Enable cluster downgrade
./build/bin/cetcdctl downgrade cancel             # Cancel downgrade
./build/bin/cetcdctl downgrade validate           # Validate downgrade state
./build/bin/cetcdctl check perf                   # Run performance check (put/get latency)
./build/bin/cetcdctl check perf -w json          # Performance check with JSON output
./build/bin/cetcdctl check datascale --load 1000  # Test database scalability with 1000 keys
./build/bin/cetcdctl check datascale -w json --load 5000  # Datascale test with JSON output
```

### Global options

| Option | Default | Description |
|--------|---------|-------------|
| `--host ADDR` | 127.0.0.1 | Server address |
| `--port PORT` | 2379 | Server port |

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
| `--data-dir PATH` | Source etcd data directory (contains `member/snap/db`, WAL segments, etc.) |
| `--output-dir PATH` | Destination directory for the converted LMDB env and WAL |
| `--verbose` | Print per-key progress and summary statistics |

The destination directory must not already contain a cetcd database; the tool
refuses to overwrite existing data as a safety measure. After migration, start
`cetcd` pointing at `--output-dir` to serve the converted data.

> **Note:** `cetcd-migrate` performs an offline, one-way conversion. Always back up
> the original etcd data before running it.

---

## 5. Observability

### Logs

cetcd outputs structured logs to stderr. Log level can be controlled at
build time via compile definitions.

### Metrics

cetcd exposes Prometheus-compatible metrics on a dedicated HTTP listener.
Use the `--metrics-port` flag to change the port (default: `2381`).

```sh
# Start cetcd with the default metrics port
./build/bin/cetcd --data-dir ./data --listen 127.0.0.1 --port 2379

# Scrape metrics
curl http://127.0.0.1:2381/metrics
```

The `/metrics` endpoint returns counters, gauges, and histograms in Prometheus
text format. Key families include:

| Metric | Type | Description |
|--------|------|-------------|
| `cetcd_grpc_requests_total` | counter | Total gRPC requests by service/method |
| `cetcd_raft_ticks_total` | counter | Total Raft tick timer firings |
| `cetcd_mvcc_revision` | gauge | Current MVCC revision |
| `cetcd_lease_active` | gauge | Number of active leases |

### Profiling

pprof-style profiling endpoints are exposed on the metrics port (`2381` by default)
under `/debug/pprof/`.

| Endpoint | Description |
|----------|-------------|
| `GET /debug/pprof/profile?seconds=N` | CPU profile for `N` seconds (default 30) |
| `GET /debug/pprof/heap` | Heap profile (in-use and allocated) |
| `GET /debug/pprof/coroutines` | Snapshot of active libco coroutines and their states |

```sh
# 10-second CPU profile
curl -o cpu.prof http://127.0.0.1:2381/debug/pprof/profile?seconds=10

# Heap profile
curl -o heap.prof http://127.0.0.1:2381/debug/pprof/heap

# Coroutine snapshot
curl http://127.0.0.1:2381/debug/pprof/coroutines
```

Profiles are returned in pprof protocol buffer format and can be visualised with
`go tool pprof` or compatible tools.

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
