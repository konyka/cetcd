# cetcd Usage

> **Status**: skeleton (Phase 0). Sections marked _stub_ fill in as features land.

## 1. Building from source

### Prerequisites

- A C compiler with C11 `<stdatomic.h>` support:
  - Linux: gcc ≥ 9 or clang ≥ 11
  - macOS: Apple Clang 13+ (Xcode 13)
  - Windows: MSVC ≥ 19.34 (Visual Studio 2022) or MinGW-w64 gcc ≥ 11
  - FreeBSD: clang 13+
- CMake ≥ 3.21
- Ninja (recommended) or Make / MSBuild
- OpenSSL 3.0+ development headers (only system dependency)
- Python 3.8+ (for protobuf-c codegen at build time)

Everything else (libuv, nghttp2, LMDB, protobuf-c, libco, mimalloc, Unity, CMock) is vendored
under `third_party/`.

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
cmake -B build-asan -G Ninja \
  -DCMAKE_BUILD_TYPE=Debug \
  -DCETCD_SANITIZERS=address,undefined
cmake --build build-asan
ctest --test-dir build-asan --output-on-failure
```

---

## 2. Running `cetcd`

_stub — will be filled in at Phase 6 when the daemon first runs._

Planned single-node smoke test:

```sh
./build/bin/cetcd \
  --name node1 \
  --data-dir /var/lib/cetcd/node1 \
  --listen-client-urls http://0.0.0.0:2379 \
  --listen-peer-urls   http://0.0.0.0:2380
```

Planned three-node static cluster:

```sh
./build/bin/cetcd \
  --name node1 \
  --initial-cluster node1=http://10.0.0.1:2380,node2=http://10.0.0.2:2380,node3=http://10.0.0.3:2380 \
  --initial-cluster-state new \
  ...
```

CLI flags will be a strict subset of upstream `etcd`'s flags for v0.1; the unsupported flags
will be documented and produce a clear error rather than silent fallback.

---

## 3. Using `cetcdctl`

_stub — Phase 6._

`cetcdctl` is a thin client that speaks etcd v3 gRPC. Behaviour intentionally mirrors
`etcdctl` so muscle memory transfers.

```sh
./build/bin/cetcdctl put foo bar
./build/bin/cetcdctl get foo
./build/bin/cetcdctl watch foo
```

You can also point upstream `etcdctl` at cetcd — it's wire-compatible:

```sh
ETCDCTL_API=3 etcdctl --endpoints=localhost:2379 put hello world
```

---

## 4. Migrating from etcd

_stub — Phase 9._

A one-way migration tool will be provided:

```sh
./build/bin/cetcd-migrate \
  --src /var/lib/etcd/member \
  --dst /var/lib/cetcd/member
```

It reads etcd's bbolt + WAL + snap and writes a cetcd-native LMDB env plus a compatible WAL.

---

## 5. Observability

_stub — Phase 8._

- Logs: structured JSON to stderr, level controlled by `--log-level`.
- Metrics: Prometheus exposition on `--listen-metrics-urls` (default `:2381/metrics`).
- Profiling: pprof-style endpoints on the metrics listener.

---

## 6. Troubleshooting

_stub._

For now, see `docs/architecture.md` for the system design, or open an issue with the output
of `cetcd --version` and the failing command.
