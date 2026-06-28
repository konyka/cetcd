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
./build/bin/cetcdctl put foo bar --prev-kv         # Store and return previous value
./build/bin/cetcdctl put foo bar --prev-kv --print-value-only  # Output only previous value
./build/bin/cetcdctl put foo -                     # Read value from stdin
./build/bin/cetcdctl get foo
./build/bin/cetcdctl get --prefix foo           # Get all keys with prefix
./build/bin/cetcdctl get --prefix ""             # Get all keys (empty prefix = all)
./build/bin/cetcdctl get --from-key foo          # Get all keys >= foo
./build/bin/cetcdctl get --count-only foo        # Count matching keys only
./build/bin/cetcdctl get --keys-only foo         # Get keys without values
./build/bin/cetcdctl get --print-value-only foo  # Print only the value
./build/bin/cetcdctl get --hex foo               # Output in hex format
./build/bin/cetcdctl get --range-end zzz foo     # Get keys from foo to zzz
./build/bin/cetcdctl del foo
./build/bin/cetcdctl del --prefix foo            # Delete all keys with prefix
./build/bin/cetcdctl del --range-end zzz foo     # Delete keys from foo to zzz
./build/bin/cetcdctl del --prev-kv foo           # Return deleted key-values
./build/bin/cetcdctl del --prev-kv --print-value-only foo  # Output only deleted values
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

# Include the previous key-value in each event
./build/bin/cetcdctl watch --prev-kv foo

# Request periodic progress notifications from the server
./build/bin/cetcdctl watch --progress-notify foo

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
./build/bin/cetcdctl lease grant 60                        # Grant a 60-second lease
./build/bin/cetcdctl lease grant --lease-id 0x1234abcd 60  # Grant with custom lease ID (hex)
./build/bin/cetcdctl lease grant 60 -w fields              # Grant with fields output
./build/bin/cetcdctl lease revoke 1                        # Revoke lease ID 1
./build/bin/cetcdctl lease revoke 1 -w fields              # Revoke with fields output
./build/bin/cetcdctl lease timetolive 1                    # Check remaining TTL
./build/bin/cetcdctl lease timetolive --keys 1 -w fields   # Include keys with fields output
./build/bin/cetcdctl lease keepalive 1                     # Keep lease alive (loop, Ctrl+C to stop)
./build/bin/cetcdctl lease keepalive --once 1 -w fields   # Single keepalive with fields output
./build/bin/cetcdctl lease keepalive --interval 5 1        # Keep alive with custom 5-second interval
./build/bin/cetcdctl lease list -w fields                  # List all leases in fields format
```

### Transactions

```sh
./build/bin/cetcdctl txn put foo bar       # Transactional put
./build/bin/cetcdctl txn put -w fields foo bar  # Transactional put with fields output
./build/bin/cetcdctl txn cas foo old new        # Compare-and-swap
./build/bin/cetcdctl txn cas -w fields foo old new  # CAS with fields output
./build/bin/cetcdctl txn get foo                # Transactional get
./build/bin/cetcdctl txn get -w fields foo       # Transactional get with fields output
./build/bin/cetcdctl txn del --prefix foo       # Transactional prefix delete
./build/bin/cetcdctl txn del -w fields --prefix foo  # Transactional delete with fields output
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
# A keepalive child process is automatically forked to renew the lease
# while the lock is held, preventing it from expiring.
./build/bin/cetcdctl lock mylock               # Prints lock key, waits for signal
./build/bin/cetcdctl lock --ttl 30 mylock     # Lock with 30s lease TTL
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
# Enable authentication
./build/bin/cetcdctl auth enable

# User management
./build/bin/cetcdctl user add root         # Create user
./build/bin/cetcdctl user add root --no-password  # Create user without password (cert-based auth)
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
./build/bin/cetcdctl role grant-permission admin readwrite /foo    # Grant permission on a key
./build/bin/cetcdctl role grant-permission admin read /foo --prefix  # Grant permission on a key prefix
./build/bin/cetcdctl role grant-permission admin read /foo --range-end /bar  # Grant permission on a key range
./build/bin/cetcdctl role revoke-permission admin                  # Revoke all permissions
./build/bin/cetcdctl role revoke-permission admin readwrite /foo  # Revoke specific permission
./build/bin/cetcdctl role revoke-permission admin read /foo --prefix  # Revoke prefix permission

# Disable authentication
./build/bin/cetcdctl auth disable

# JSON output for all auth commands
./build/bin/cetcdctl auth enable -w json
./build/bin/cetcdctl user list -w json
./build/bin/cetcdctl role list -w json

# Fields output for auth commands
./build/bin/cetcdctl auth status -w fields
./build/bin/cetcdctl auth login -w fields root mypassword  # Login with fields output (token)
./build/bin/cetcdctl user list -w fields
./build/bin/cetcdctl role list -w fields
```

### Snapshot and maintenance

```sh
./build/bin/cetcdctl snapshot save backup.snap   # Save KV snapshot to file
./build/bin/cetcdctl snapshot save backup.snap --compaction-periodical  # With etcd-compatible flag (no-op)
./build/bin/cetcdctl snapshot save backup.snap -w json  # Save with JSON output
./build/bin/cetcdctl snapshot save backup.snap -w fields  # Save with fields output
./build/bin/cetcdctl snapshot status backup.snap # Show snapshot file info
./build/bin/cetcdctl snapshot restore backup.snap --data-dir /tmp/cetcd  # Restore snapshot
./build/bin/cetcdctl snapshot restore backup.snap --data-dir /tmp/cetcd --force  # Force overwrite
./build/bin/cetcdctl snapshot restore backup.snap --data-dir /tmp/cetcd -w json  # Restore with JSON output
./build/bin/cetcdctl snapshot restore backup.snap --data-dir /tmp/cetcd -w fields  # Restore with fields output
./build/bin/cetcdctl endpoint health -w json  # Health check with ResponseHeader
./build/bin/cetcdctl endpoint status -w table  # Status in table format
./build/bin/cetcdctl endpoint hashkv -w json   # HashKV with ResponseHeader
./build/bin/cetcdctl endpoint hashkv -w fields  # HashKV with fields output
./build/bin/cetcdctl alarm list                           # List all alarms
./build/bin/cetcdctl alarm activate NOSPACE               # Activate NOSPACE alarm
./build/bin/cetcdctl alarm activate CORRUPT               # Activate CORRUPT alarm
./build/bin/cetcdctl alarm disarm                        # Disarm all alarms
./build/bin/cetcdctl downgrade enable             # Enable cluster downgrade
./build/bin/cetcdctl downgrade cancel             # Cancel downgrade
./build/bin/cetcdctl downgrade validate           # Validate downgrade state
./build/bin/cetcdctl check perf                   # Run performance check (put/get latency)
./build/bin/cetcdctl check perf --load s            # Small load (10 keys)
./build/bin/cetcdctl check perf --load m            # Medium load (100 keys)
./build/bin/cetcdctl check perf --load l            # Large load (1000 keys)
./build/bin/cetcdctl check perf --prefix mycheck    # Custom key prefix
./build/bin/cetcdctl check perf -w json          # Performance check with JSON output
./build/bin/cetcdctl check perf -w fields         # Performance check with fields output
./build/bin/cetcdctl check datascale --load 1000  # Test database scalability with 1000 keys
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
./build/bin/cetcdctl move-leader -w fields 1234567890  # Transfer leadership with fields output
./build/bin/cetcdctl snapshot status backup.snap -w fields  # Snapshot info in fields format
./build/bin/cetcdctl downgrade enable 3.5.0 -w fields  # Downgrade with fields output
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
| `--port PORT` | 2379 | Server port |
| `--endpoints EP` | 127.0.0.1:2379 | Server endpoint (host:port, comma-separated for multiple, http:// prefix supported) |
| `--command-timeout SEC` | none | Timeout for commands (integer seconds or Go duration: `5s`, `1m`, `1m30s`, `500ms`) |

### Table output formats

Several commands support `-w table` for tabular output:

```sh
# Endpoint health in table format
./build/bin/cetcdctl endpoint health -w table
./build/bin/cetcdctl endpoint health --cluster -w table

# Snapshot save in table format
./build/bin/cetcdctl snapshot save backup.snap -w table

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
