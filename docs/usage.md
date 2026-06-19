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

---

## 3. Using `cetcdctl`

`cetcdctl` is a command-line client that speaks cetcd's gRPC protocol.
It mirrors `etcdctl` command structure for familiarity.

### KV operations

```sh
./build/bin/cetcdctl put foo bar
./build/bin/cetcdctl get foo
./build/bin/cetcdctl del foo
```

### Lease management

```sh
./build/bin/cetcdctl lease grant 60        # Grant a 60-second lease
./build/bin/cetcdctl lease revoke 1        # Revoke lease ID 1
./build/bin/cetcdctl lease timetolive 1    # Check remaining TTL
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
./build/bin/cetcdctl alarm                 # Query active alarms
./build/bin/cetcdctl member list           # List cluster members
```

### Authentication (RBAC)

```sh
# Enable authentication
./build/bin/cetcdctl auth enable

# User management
./build/bin/cetcdctl user add root         # Create user
./build/bin/cetcdctl user get root         # View user details (roles)
./build/bin/cetcdctl user list             # List all users

# Role management
./build/bin/cetcdctl role add admin        # Create role
./build/bin/cetcdctl role get admin        # View role permissions
./build/bin/cetcdctl role list             # List all roles

# Permission management
./build/bin/cetcdctl role grant-permission admin readwrite /foo
./build/bin/cetcdctl role revoke-permission admin

# Disable authentication
./build/bin/cetcdctl auth disable
```

### Snapshot and maintenance

```sh
./build/bin/cetcdctl snapshot save backup.snap   # Save KV snapshot to file
./build/bin/cetcdctl downgrade enable             # Enable cluster downgrade
./build/bin/cetcdctl downgrade cancel             # Cancel downgrade
./build/bin/cetcdctl downgrade validate           # Validate downgrade state
```

### Global options

| Option | Default | Description |
|--------|---------|-------------|
| `--host ADDR` | 127.0.0.1 | Server address |
| `--port PORT` | 2379 | Server port |

---

## 4. Migrating from etcd

A one-way migration tool (`cetcd-migrate`) is planned for a future release.
It will read etcd's bbolt + WAL + snap and write a cetcd-native LMDB env
plus a compatible WAL.

```sh
# Planned (not yet available)
./build/bin/cetcd-migrate \
  --src /var/lib/etcd/member \
  --dst /var/lib/cetcd/member
```

---

## 5. Observability

### Logs

cetcd outputs structured logs to stderr. Log level can be controlled at
build time via compile definitions.

### Metrics

Prometheus-compatible metrics are planned for a future release, exposed on
a dedicated metrics listener.

### Profiling

pprof-style profiling endpoints are planned for a future release.

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
