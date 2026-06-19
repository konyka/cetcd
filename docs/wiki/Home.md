# cetcd Wiki

> cetcd 是 etcd 的纯 C 重新实现，以 C99 为基准（需要 C11 的 `<stdatomic.h>`），目标是与 etcd v3.5 稳定 API 线兼容。

## 目录

- [项目概述](#项目概述)
- [快速开始](#快速开始)
- [模块地图](#模块地图)
- [架构设计](架构设计)
  - [并发模型](架构设计#并发模型)
  - [依赖方向](架构设计#依赖方向)
- [模块详解](模块详解)
  - [base — 基础设施](模块详解#base--基础设施)
  - [io — 事件循环与协程](模块详解#io--事件循环与协程)
  - [raft — 共识算法](模块详解#raft--共识算法)
  - [wal — 预写日志](模块详解#wal--预写日志)
  - [backend — 存储后端](模块详解#backend--存储后端)
  - [mvcc — 多版本并发控制](模块详解#mvcc--多版本并发控制)
  - [lease — 租约管理](模块详解#lease--租约管理)
  - [auth — 认证与授权](模块详解#auth--认证与授权)
  - [proto — 协议编解码](模块详解#proto--协议编解码)
  - [http2 — HTTP/2 与 gRPC](模块详解#http2--http2-与-grpc)
  - [tls — TLS 加密](模块详解#tls--tls-加密)
  - [peer — 集群通信](模块详解#peer--集群通信)
  - [snap — 快照](模块详解#snap--快照)
  - [v3rpc — gRPC 处理层](模块详解#v3rpc--grpc-处理层)
  - [server — 服务器主体](模块详解#server--服务器主体)
- [数据流](#数据流)
- [构建系统](#构建系统)
- [测试体系](#测试体系)
- [架构决策记录 (ADR)](#架构决策记录-adr)
- [术语表](#术语表)

---

## 项目概述

cetcd 从零开始重新实现了 [etcd](https://github.com/etcd-io/etcd)，使用纯 C 语言编写，目标是与 etcd v3.5 的 gRPC API **线兼容**——现有的 `etcdctl` 和官方客户端库可以直接使用。

### 核心目标

| 目标 | 说明 |
|------|------|
| **线兼容** | 支持 etcd v3.5 全部 48 个 RPC（KV、Watch、Lease、Cluster、Auth、Maintenance） |
| **跨平台** | Linux（主要）、macOS、FreeBSD、Windows（MSVC + MinGW-w64） |
| **性能对等** | 3 节点 70/30 Put/Range 工作负载下达到或超过 Go 版 etcd |
| **纯 C** | C99 基准，需要 C11 原子操作，公共头文件无 GNU/MSVC 扩展 |
| **可测试** | 全程 TDD，通过可注入时钟/网络实现确定性 Raft 测试 |
| **可观测** | 结构化 JSON 日志、Prometheus `/metrics`、性能分析钩子 |

### 版本信息

- 版本：0.1.0
- 许可证：Apache-2.0
- 仓库：<https://github.com/konyka/cetcd>

---

## 快速开始

```sh
# 配置（Debug 模式 + ASan/UBSan）
cmake -B build -G Ninja -DCMAKE_BUILD_TYPE=Debug -DCETCD_SANITIZERS=address,undefined

# 编译
cmake --build build

# 运行测试
ctest --test-dir build --output-on-failure

# 启动单节点
./build/bin/cetcd --data-dir ./data --listen 127.0.0.1 --port 2379
```

---

## 模块地图

```
src/
├── base/        libcetcd_base       竞技场、slab、哈希、Treap、引用计数、错误、日志、时钟
├── io/          libcetcd_io         libuv 事件循环 + libco 协程 + 工作线程池
├── proto/       libcetcd_proto      protobuf-c 运行时 + 生成的 etcd v3 消息类型
├── http2/       libcetcd_http2      nghttp2 + gRPC 帧（长度前缀 proto 帧）
├── tls/         libcetcd_tls        OpenSSL 3 TLS 终止 + ALPN
├── raft/        libcetcd_raft       Raft 逻辑核心（无 I/O、无线程）
├── wal/         libcetcd_wal        只追加日志，与 etcd WAL 字节兼容
├── backend/     libcetcd_backend    LMDB 支持的事务性键值存储
├── mvcc/        libcetcd_mvcc       修订索引、观察者扇出、压缩
├── lease/       libcetcd_lease      TTL 最小堆 + 租约-键索引
├── auth/        libcetcd_auth       RBAC、JWT、bcrypt 密码哈希
├── peer/        libcetcd_peer       Raft 传输层（rafthttp 等价）
├── snap/        libcetcd_snap       快照文件读写和流式传输
├── v3rpc/       libcetcd_v3rpc      全部 48 个 RPC 的 gRPC 处理器
└── server/      libcetcd_server     主循环、应用管线、配置、生命周期

cmd/
├── cetcd/         守护进程二进制
└── cetcdctl/      客户端 CLI（put/get/del/lease/txn/compact/status/alarm/member/auth/user/role/snapshot/downgrade）
```

---

## 架构设计

详见 [架构设计](架构设计)。

### 并发模型

```
┌─────────────────────────────────────────────────────────────────┐
│ Reactor 线程 — 每台机器 N 个，按连接哈希分片                       │
│                                                                 │
│   libuv 事件循环                                                │
│     ├─ 接受 TCP / TLS 连接                                     │
│     ├─ nghttp2 → gRPC 帧                                       │
│     ├─ 将每个 RPC 分发到独立的 libco 协程                       │
│     │     协程在以下操作时让出：                                  │
│     │       • 磁盘 fsync       → 工作线程池                     │
│     │       • Raft 提案        → MPSC 环形缓冲 → Raft 线程       │
│     │       • LMDB 读取        → 内联（mmap，无系统调用）        │
│     └─ 观察者扇出（每键无锁队列）                                │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ Raft 线程 (1)                                                   │
│   • cetcd_raft_step(node, msg)                                  │
│   • cetcd_raft_ready(node) → {entries, committed, snap, msgs}   │
│   • 已提交批次交给 Reactor 上的应用协程                           │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│ 工作线程池 (N = CPU 核心数)                                     │
│   • WAL fsync                                                   │
│   • 快照创建/恢复                                               │
│   • LMDB 压缩                                                   │
└─────────────────────────────────────────────────────────────────┘
```

### 锁策略

- 所有线程间通信使用 `libcetcd_base` 中的 **MPSC/SPSC 环形缓冲区**。Reactor ↔ Raft ↔ 工作池的热路径无锁。
- Reactor 内部，协程协作调度——每连接状态无需锁。
- 跨 Reactor 共享仅通过 Raft 线程进行。

### 依赖方向

```
cetcd (守护进程)
  └─ libcetcd_server
       ├─ libcetcd_v3rpc
       │    ├─ libcetcd_http2 ─── nghttp2
       │    ├─ libcetcd_tls   ─── OpenSSL
       │    └─ libcetcd_proto ─── protobuf-c
       ├─ libcetcd_peer
       ├─ libcetcd_raft       (纯逻辑，不依赖 base 以下)
       ├─ libcetcd_wal
       ├─ libcetcd_mvcc
       │    └─ libcetcd_backend ─── LMDB
       ├─ libcetcd_lease
       ├─ libcetcd_auth
       ├─ libcetcd_snap
       └─ libcetcd_io          ─── libuv + libco

libcetcd_base   (零依赖，仅 libc)
```

**无反向依赖。** `libcetcd_base` 是叶子节点。

---

## 模块详解

### base — 基础设施

**库**：`libcetcd_base`
**头文件**：`include/cetcd/base.h`（伞式头文件，包含所有子组件）
**源码**：`src/base/`

`libcetcd_base` 是整个项目的根基，零外部依赖，仅依赖 libc。它提供以下组件：

| 组件 | 头文件 | 说明 |
|------|--------|------|
| `cetcd_slice` | `slice.h` | 零拷贝字节视图（指针+长度），类似 Go 的 `[]byte` |
| `cetcd_buf` | `buf.h` | 动态增长的字节缓冲区 |
| `cetcd_clock` | `clock.h` | 跨平台高精度时钟抽象 |
| `cetcd_arena` | `arena.h` | 竞技场分配器，批量释放 |
| `cetcd_slab` | `slab.h` | Slab 分配器，固定大小对象池 |
| `cetcd_hash` | `hash.h` | 哈希函数（Murmur/FNV 等） |
| `cetcd_hashmap` | `hashmap.h` | 开放寻址哈希表 |
| `cetcd_treap` | `treap.h` | 概率平衡二叉搜索树，用于 MVCC 索引 |
| `cetcd_log` | `log.h` | 结构化日志（text/json 格式，5 级日志） |
| `cetcd_metrics` | `metrics.h` | Prometheus 兼容的指标收集（counter/gauge/histogram） |

**统一错误码**：

```c
typedef enum cetcd_status {
    CETCD_OK            =  0,
    CETCD_ERR_NOMEM     = -1,
    CETCD_ERR_INVAL     = -2,
    CETCD_ERR_RANGE     = -3,
    CETCD_ERR_NOTFOUND  = -4,
    CETCD_ERR_EXISTS    = -5,
    CETCD_ERR_IO        = -6,
    CETCD_ERR_CORRUPT   = -7,
    CETCD_ERR_INTERNAL  = -8,
    CETCD_ERR_OVERFLOW  = -9,
    CETCD_ERR_CANCELED  = -10,
    CETCD_ERR_TIMEDOUT  = -11,
    CETCD_ERR_UNSUPPORT = -12
} cetcd_status;
```

**API 导出**：Windows 使用 `__declspec(dllexport)`，其他平台使用 `__attribute__((visibility("default")))`。

---

### io — 事件循环与协程

**库**：`libcetcd_io`
**头文件**：`include/cetcd/io.h`
**源码**：`src/io/`（`loop.c`, `co.c`, `tcp.c`, `timer.c`, `async.c`）
**依赖**：libuv、libco

将 **libuv** 事件循环与 **libco** 栈式协程结合，提供同步风格的异步 I/O API：

```c
// 事件循环
cetcd_loop *cetcd_loop_new(void);
int         cetcd_loop_run(cetcd_loop *loop);    // 阻塞直到停止
void        cetcd_loop_stop(cetcd_loop *loop);

// 协程
cetcd_co *cetcd_co_spawn(cetcd_loop *loop, cetcd_co_fn fn, void *arg);
void      cetcd_co_yield(cetcd_loop *loop);      // 让出执行权
void      cetcd_co_resume(cetcd_co *co);         // 恢复特定协程

// TCP（协程感知，读写自动让出）
cetcd_tcp *cetcd_tcp_new(cetcd_loop *loop);
int        cetcd_tcp_bind(cetcd_tcp *tcp, const char *addr, uint16_t port);
int        cetcd_tcp_listen(cetcd_tcp *tcp, cetcd_tcp_conn_cb cb, void *arg);
int        cetcd_tcp_read(cetcd_tcp *tcp, void *buf, size_t len);   // 让出直到数据可用
int        cetcd_tcp_write(cetcd_tcp *tcp, const void *buf, size_t len);

// 定时器（协程感知）
cetcd_timer *cetcd_timer_new(cetcd_loop *loop);
void         cetcd_timer_start(cetcd_timer *timer, uint64_t timeout_ms,
                                uint64_t repeat_ms, cetcd_co_fn cb, void *arg);

// 跨线程信号
cetcd_async *cetcd_async_new(cetcd_loop *loop, cetcd_async_cb cb, void *arg);
void         cetcd_async_send(cetcd_async *async);   // 线程安全
```

**设计决策**：选择栈式协程而非回调或线程模型，使得 gRPC 双向流处理代码可读性如同同步代码。详见 [ADR 0003](../adr/0003-coroutines-on-libuv.md)。

---

### raft — 共识算法

**库**：`libcetcd_raft`
**头文件**：`include/cetcd/raft.h`
**源码**：`src/raft/raft.c`（~950 行）、`log.c`、`storage.c`
**依赖**：仅 `libcetcd_base`

这是项目最核心也最复杂的模块，从零实现 Raft 共识算法，API 镜像 `go.etcd.io/raft`。

#### 核心设计原则

1. **无 I/O**：Raft 模块不执行任何磁盘或网络操作
2. **无线程**：不创建任何线程
3. **完全确定性**：给定相同的随机种子，行为完全确定
4. **嵌入者拥有持久化和传输**

#### 状态机模型

```
Follower ──(选举超时)──→ PreCandidate ──(PreVote通过)──→ Candidate ──(获多数票)──→ Leader
    ↑                        │                              │                       │
    └──── 发现更高 Term ─────┴──────── 发现更高 Term ───────┘                       │
    └──────────────── 发现更高 Term ────────────────────────────────────────────────┘
```

#### 核心 API

```c
// 生命周期
cetcd_raft *cetcd_raft_new(cetcd_raft_config *cfg);
void        cetcd_raft_free(cetcd_raft *r);

// 核心循环（镜像 go.etcd.io/raft 的 Step/Tick/Ready/Advance）
int         cetcd_raft_step(cetcd_raft *r, cetcd_msg *msg);   // 馈入消息
void        cetcd_raft_tick(cetcd_raft *r);                     // 时钟驱动
cetcd_ready cetcd_raft_ready(cetcd_raft *r);                   // 取出待处理状态
void        cetcd_raft_advance(cetcd_raft *r, const cetcd_ready *rd);  // 确认已处理

// 提案
int cetcd_raft_propose(cetcd_raft *r, const uint8_t *data, size_t len);
int cetcd_raft_propose_conf_change(cetcd_raft *r, const uint8_t *data, size_t len);
```

#### Ready 模式

`cetcd_ready` 是 Raft 与嵌入者之间的契约：

```c
typedef struct cetcd_ready {
    cetcd_hard_state *hard_state;   // 持久化后再发送消息
    cetcd_soft_state *soft_state;   // 软状态（角色、leader ID）
    cetcd_entry      *entries;      // 新条目 → 持久化到 WAL
    uint32_t          n_entries;
    uint64_t          committed;    // 新的已提交索引
    cetcd_snapshot   *snapshot;     // 可选快照
    cetcd_msg        *messages;     // 持久化后发送给对等节点
    uint32_t          n_messages;
} cetcd_ready;
```

处理流程：`Ready取出 → 持久化 HardState + Entries → 发送 Messages → 应用已提交 → Advance`

#### 消息类型

| 类型 | 值 | 说明 |
|------|-----|------|
| `CETCD_MSG_HUP` | 0 | 触发选举 |
| `CETCD_MSG_BEAT` | 1 | 心跳触发 |
| `CETCD_MSG_PROP` | 2 | 客户端提案 |
| `CETCD_MSG_APP` | 3 | 追加条目 |
| `CETCD_MSG_APP_RESP` | 4 | 追加响应 |
| `CETCD_MSG_VOTE` | 5 | 请求投票 |
| `CETCD_MSG_VOTE_RESP` | 6 | 投票响应 |
| `CETCD_MSG_HEARTBEAT` | 8 | 心跳 |
| `CETCD_MSG_HEARTBEAT_RESP` | 9 | 心跳响应 |
| `CETCD_MSG_PRE_VOTE` | 17 | PreVote 请求 |
| `CETCD_MSG_PRE_VOTE_RESP` | 18 | PreVote 响应 |

#### 选举与日志复制

- **选举超时**：默认 10 tick，每 tick 100ms
- **心跳超时**：默认 1 tick
- **PreVote**：启用，避免分区节点干扰
- **CheckQuorum**：启用，Leader 主动检查存活
- **日志复制**：Leader 为每个 Follower 维护 `next_idx` 和 `match_idx`，通过 `AppResp` 推进
- **提交推进**：Leader 计算多数派 `match_idx` 的中位数，且仅提交当前 Term 的条目

#### 线路编码

Raft 消息使用自定义 Protobuf 风格的 varint 编码进行序列化（`cetcd_msg_encode_wire` / `cetcd_msg_decode_wire`），用于对等节点传输。

---

### wal — 预写日志

**库**：`libcetcd_wal`
**头文件**：`include/cetcd/wal.h`
**源码**：`src/wal/encoder.c`、`src/wal/decoder.c`
**依赖**：`libcetcd_base`、`libcetcd_raft`

WAL 与 etcd **字节级兼容**，可直接读取现有 etcd 集群的日志文件。

#### 记录格式

```
+--------+----------------+----------+-------------+
| length |   type:Int64   |   data   | crc:CRC32C  |
| 8 B LE |    8 B LE      |  N bytes |    4 B LE   |
+--------+----------------+----------+-------------+
```

#### 记录类型

| 类型 | 值 | 说明 |
|------|-----|------|
| `CETCD_WAL_METADATA` | 1 | 元数据 |
| `CETCD_WAL_ENTRY` | 2 | Raft 条目 |
| `CETCD_WAL_STATE` | 3 | HardState |
| `CETCD_WAL_CRC` | 4 | CRC 校验 |
| `CETCD_WAL_SNAPSHOT` | 5 | 快照记录 |

- 段文件命名：`%016x-%016x.wal`（序列号、起始索引）
- CRC 算法：**Castagnoli (CRC32C)**，与 etcd 一致
- 段轮转：默认 64 MiB

#### API

```c
// 编码器（写入）
cetcd_wal_encoder *cetcd_wal_encoder_create(const char *path);
int cetcd_wal_encode_entry(cetcd_wal_encoder *enc, const cetcd_entry *entry);
int cetcd_wal_encode_hard_state(cetcd_wal_encoder *enc, const cetcd_hard_state *hs);
int cetcd_wal_encoder_flush(cetcd_wal_encoder *enc);

// 解码器（读取）
cetcd_wal_decoder *cetcd_wal_decoder_open(const char *path);
int cetcd_wal_decode(cetcd_wal_decoder *dec, cetcd_wal_record *rec);
```

---

### backend — 存储后端

**库**：`libcetcd_backend`
**头文件**：`include/cetcd/backend.h`
**源码**：`src/backend/backend.c`
**依赖**：LMDB

使用 **LMDB**（单写多读的 mmap B+tree）作为持久化存储后端。

#### 设计要点

- 一个 LMDB 环境对应一个 cetcd 实例
- 逻辑子数据库映射到 etcd 的 bbolt 桶：`key`、`lease`、`auth`、`authUsers`、`authRoles`、`members`、`cluster`、`alarm`、`meta`
- **不兼容** bbolt 磁盘格式，提供 `cetcd-migrate` 工具进行单向迁移
- WAL 保持字节兼容，仅物化状态迁移至 LMDB

#### API

```c
// 生命周期
cetcd_backend *cetcd_backend_open(const cetcd_backend_config *cfg);
void           cetcd_backend_close(cetcd_backend *be);

// 事务
cetcd_txn *cetcd_txn_begin(cetcd_backend *be, bool read_only);
int        cetcd_txn_commit(cetcd_txn *txn);
void       cetcd_txn_abort(cetcd_txn *txn);

// 自动提交的 KV 操作
int cetcd_backend_put(cetcd_backend *be, const char *bucket,
                      const uint8_t *key, size_t key_len,
                      const uint8_t *val, size_t val_len);
int cetcd_backend_get(cetcd_backend *be, const char *bucket,
                      const uint8_t *key, size_t key_len,
                      uint8_t **val, size_t *val_len);
int cetcd_backend_del(cetcd_backend *be, const char *bucket,
                      const uint8_t *key, size_t key_len);

// 事务内 KV 操作
int cetcd_txn_put(cetcd_txn *txn, const char *bucket, ...);
int cetcd_txn_get(cetcd_txn *txn, const char *bucket, ...);
int cetcd_txn_del(cetcd_txn *txn, const char *bucket, ...);
```

---

### mvcc — 多版本并发控制

**库**：`libcetcd_mvcc`
**头文件**：`include/cetcd/mvcc.h`
**源码**：`src/mvcc/mvcc.c`
**依赖**：`libcetcd_base`（使用 `cetcd_treap` 作为索引）

MVCC 是 cetcd 数据模型的核心，镜像 etcd 的 `mvcc/key_index.go`。

#### 修订模型

```c
typedef struct cetcd_revision {
    int64_t main;   // 单调递增的事务计数器
    int64_t sub;    // 事务内操作计数器
} cetcd_revision;
```

- 每次 Put/Delete 递增 `main`
- 修订号 (main, sub) 全局唯一，作为 MVCC 的"时钟"

#### 数据结构

```
cetcd_mvcc_store
  ├── index: cetcd_treap   // key → key_generation 的映射
  ├── history: revision_entry[]   // 完整修改历史
  ├── watchers: cetcd_watcher[]   // 观察者列表
  └── compacted_rev: int64_t      // 已压缩到的修订号
```

- **Treap 索引**：概率平衡 BST，键为 `cetcd_slice`，值为 `key_generation`
- **key_generation**：跟踪每个键的创建修订、修改修订、版本号、删除标记
- **history**：完整的修订历史，支持按修订号查询

#### 核心 API

```c
// 写操作（推进修订号）
cetcd_revision cetcd_mvcc_put(cetcd_mvcc_store *s,
                               const uint8_t *key, size_t key_len,
                               const uint8_t *val, size_t val_len,
                               int64_t lease_id);
cetcd_revision cetcd_mvcc_delete(cetcd_mvcc_store *s,
                                  const uint8_t *key, size_t key_len);

// 读操作（在指定修订号下，0 = 最新）
int cetcd_mvcc_get(cetcd_mvcc_store *s, int64_t rev,
                    const uint8_t *key, size_t key_len, cetcd_kv *out);
int cetcd_mvcc_range(cetcd_mvcc_store *s, int64_t rev,
                      const uint8_t *key_start, size_t start_len,
                      const uint8_t *key_end, size_t end_len,
                      cetcd_kv **out, size_t *out_count);

// 观察
cetcd_watcher *cetcd_mvcc_watch(cetcd_mvcc_store *s,
                                 const uint8_t *key, size_t key_len,
                                 int64_t start_rev,
                                 cetcd_watch_cb cb, void *udata);
cetcd_watcher *cetcd_mvcc_watch_prefix(cetcd_mvcc_store *s, ...);
void cetcd_mvcc_watch_cancel(cetcd_mvcc_store *s, cetcd_watcher *w);

// 压缩
int cetcd_mvcc_compact(cetcd_mvcc_store *s, int64_t compact_rev);
```

#### 观察者机制

- Watcher 订阅特定键或前缀
- 每次 Put/Delete 时，MVCC 通知所有匹配的 Watcher
- 支持 `start_rev` 过滤，仅推送修订号 >= start_rev 的事件

---

### lease — 租约管理

**库**：`libcetcd_lease`
**头文件**：`include/cetcd/lease.h`
**源码**：`src/lease/lease.c`
**依赖**：`libcetcd_base`

#### 设计要点

- 每个租约有 ID、TTL、绝对截止时间
- 键可附加到租约上，租约到期时所有关联键自动删除
- 通过 `cetcd_lease_mgr_tick()` 推进时间并触发到期回调

#### API

```c
cetcd_lease_mgr *cetcd_lease_mgr_new(cetcd_lease_expire_fn on_expire, void *udata);

cetcd_lease_id cetcd_lease_grant(cetcd_lease_mgr *mgr, int64_t ttl_seconds);
int            cetcd_lease_revoke(cetcd_lease_mgr *mgr, cetcd_lease_id id);
int            cetcd_lease_keep_alive(cetcd_lease_mgr *mgr, cetcd_lease_id id, int64_t ttl_seconds);

int  cetcd_lease_attach_key(cetcd_lease_mgr *mgr, cetcd_lease_id id,
                             const uint8_t *key, size_t key_len);
int  cetcd_lease_detach_key(cetcd_lease_mgr *mgr, cetcd_lease_id id,
                             const uint8_t *key, size_t key_len);

void cetcd_lease_mgr_tick(cetcd_lease_mgr *mgr, int64_t elapsed_ms);
```

---

### auth — 认证与授权

**库**：`libcetcd_auth`
**头文件**：`include/cetcd/auth.h`
**源码**：`src/auth/auth.c`
**依赖**：`libcetcd_base`（使用 `cetcd_hashmap`）

实现 RBAC（基于角色的访问控制）：

- **用户 (cetcd_user)**：名称 + 密码哈希 + 角色列表
- **角色 (cetcd_role)**：名称 + 读/写权限 + 键前缀
- **全局开关**：`enabled` 标志控制认证是否激活

```c
cetcd_auth_store *cetcd_auth_store_new(void);

// 用户管理
int  cetcd_auth_add_user(cetcd_auth_store *s, const char *name, const char *password);
int  cetcd_auth_remove_user(cetcd_auth_store *s, const char *name);
bool cetcd_auth_check_password(const cetcd_auth_store *s, const char *name, const char *password);

// 角色管理
int  cetcd_auth_add_role(cetcd_auth_store *s, const char *name,
                          int perm_read, int perm_write,
                          const char *key_prefix, size_t prefix_len);

// 角色授权
int  cetcd_auth_grant_role(cetcd_auth_store *s, const char *user, const char *role);
int  cetcd_auth_revoke_role(cetcd_auth_store *s, const char *user, const char *role);

// 全局开关
bool cetcd_auth_is_enabled(const cetcd_auth_store *s);
void cetcd_auth_set_enabled(cetcd_auth_store *s, bool enabled);

// 查询
size_t cetcd_auth_user_count(const cetcd_auth_store *s);
size_t cetcd_auth_role_count(const cetcd_auth_store *s);

// 修改密码
int cetcd_auth_change_password(cetcd_auth_store *s, const char *name,
                                const char *new_password);

// 迭代用户/角色
typedef bool (*cetcd_auth_user_iter_fn)(const char *name, void *udata);
void cetcd_auth_user_iter(const cetcd_auth_store *s, cetcd_auth_user_iter_fn fn, void *udata);
typedef bool (*cetcd_auth_role_iter_fn)(const char *name, void *udata);
void cetcd_auth_role_iter(const cetcd_auth_store *s, cetcd_auth_role_iter_fn fn, void *udata);
```

> **注意**：当前实现使用占位符哈希函数，仅用于单元测试，非加密安全。生产环境需替换为 bcrypt。

---

### proto — 协议编解码

**库**：`libcetcd_proto`
**头文件**：`include/cetcd/proto.h`
**源码**：`src/proto/`（`codec.c`、`kv.pb-c.c`、`rpc.pb-c.c`、`auth.pb-c.c`）
**依赖**：protobuf-c

提供 etcd v3.5 protobuf 消息类型的 C 绑定：

```c
// 核心 KV 消息类型
Etcd__KeyValue、Etcd__Event、Etcd__RangeRequest、Etcd__RangeResponse
Etcd__PutRequest、Etcd__PutResponse、Etcd__RequestOp、Etcd__ResponseOp

// 编解码
size_t           cetcd_proto_pack(const ProtobufCMessage *msg, uint8_t *out, size_t out_len);
ProtobufCMessage *cetcd_proto_unpack(const ProtobufCMessageDescriptor *desc,
                                      uint32_t len, const uint8_t *data);
void             cetcd_proto_free(ProtobufCMessage *msg);
size_t           cetcd_proto_packed_size(const ProtobufCMessage *msg);
```

Proto 文件位于 `proto/` 目录，覆盖：
- `etcdserverpb/rpc.proto` — KV/Watch/Lease/Cluster/Auth/Maintenance RPC 定义
- `etcdserverpb/raft_internal.proto` — Raft 内部消息
- `mvccpb/kv.proto` — KeyValue/Event 消息
- `authpb/auth.proto` — 认证消息

---

### http2 — HTTP/2 与 gRPC

**库**：`libcetcd_http2`
**头文件**：`include/cetcd/http2.h`
**源码**：`src/http2/http2.c`
**依赖**：nghttp2

提供 HTTP/2 会话管理和 gRPC 帧处理：

```c
// 会话管理
cetcd_h2_session *cetcd_h2_session_new(const cetcd_h2_callbacks *cbs);
int cetcd_h2_feed(cetcd_h2_session *s, const uint8_t *data, size_t len);
int cetcd_h2_send_pending(cetcd_h2_session *s, int (*write_fn)(...), void *ctx);

// 响应
int cetcd_h2_submit_response(cetcd_h2_session *s, int32_t stream_id,
                               const char **headers, size_t header_count,
                               const uint8_t *body, size_t body_len, bool end_stream);

// gRPC 帧编解码
int cetcd_grpc_encode(const uint8_t *msg, size_t msg_len, bool compressed, uint8_t **out, size_t *out_len);
int cetcd_grpc_decode(const uint8_t *frame, size_t frame_len, bool *compressed, uint8_t **msg, size_t *msg_len);
```

回调模型：
- `on_request`：收到 HTTP/2 请求头时触发
- `on_data`：收到请求体数据时触发

---

### tls — TLS 加密

**库**：`libcetcd_tls`
**头文件**：`include/cetcd/tls.h`
**源码**：`src/tls/tls.c`
**依赖**：OpenSSL 3

```c
cetcd_tls_ctx *cetcd_tls_ctx_new(void);
int cetcd_tls_set_cert(cetcd_tls_ctx *ctx, const char *cert_path, const char *key_path);
int cetcd_tls_set_ca(cetcd_tls_ctx *ctx, const char *ca_path);
int cetcd_tls_set_alpn(cetcd_tls_ctx *ctx, const char **protocols, size_t count);

cetcd_tls_conn *cetcd_tls_accept(cetcd_tls_ctx *ctx, int fd);
int  cetcd_tls_read(cetcd_tls_conn *conn, void *buf, size_t len);
int  cetcd_tls_write(cetcd_tls_conn *conn, const void *buf, size_t len);
```

---

### peer — 集群通信

**库**：`libcetcd_peer`
**头文件**：`include/cetcd/peer.h`
**源码**：`src/peer/peer.c`
**依赖**：`libcetcd_base`

对等节点管理和 Raft 消息传输，对应 etcd 的 `rafthttp`：

```c
// 集群管理
cetcd_cluster *cetcd_cluster_new(uint64_t self_id);
int cetcd_cluster_add_peer(cetcd_cluster *c, const cetcd_peer_info *info);
int cetcd_cluster_remove_peer(cetcd_cluster *c, uint64_t id);

// 消息发送
typedef void (*cetcd_peer_send_fn)(uint64_t to_id, const uint8_t *data, size_t len, void *udata);
int cetcd_cluster_set_sender(cetcd_cluster *c, cetcd_peer_send_fn fn, void *udata);
int cetcd_cluster_send_msg(cetcd_cluster *c, const uint8_t *serialized_msg, size_t len, uint64_t to_id);

// 消息编解码
size_t cetcd_msg_encode(const uint8_t *raft_msg_raw, size_t msg_len, uint8_t **out);
int    cetcd_msg_decode(const uint8_t *data, size_t len, uint8_t **raft_msg_out, size_t *raft_msg_len);
```

---

### snap — 快照

**库**：`libcetcd_snap`
**头文件**：`include/cetcd/snap.h`
**源码**：`src/snap/snap.c`

快照的创建、编码和解码：

```c
cetcd_snap *cetcd_snap_new(void);
int  cetcd_snap_add_entry(cetcd_snap *s, const uint8_t *key, size_t key_len,
                           const uint8_t *value, size_t value_len, int64_t mod_revision);

uint8_t    *cetcd_snap_encode(const cetcd_snap *s, size_t *out_len);
cetcd_snap *cetcd_snap_decode(const uint8_t *data, size_t len);
```

快照文件格式：`%016x-%016x.snap`，头部 `{crc:uint32, len:uint32}` + LMDB 环境转储负载。

---

### v3rpc — gRPC 处理层

**库**：`libcetcd_v3rpc`
**头文件**：`include/cetcd/v3rpc.h`
**源码**：`src/v3rpc/`（`v3rpc.c`、`kv_handler.c`、`lease_handler.c`、`auth_handler.c`、`watch_handler.c`、`maint_handler.c`、`cluster_handler.c`）
**依赖**：`libcetcd_mvcc`、`libcetcd_lease`、`libcetcd_auth`、`libcetcd_peer`

gRPC 请求分发器，根据 gRPC 路径名路由到对应处理器：

```c
cetcd_v3rpc *cetcd_v3rpc_new(void);
cetcd_rpc_bytes cetcd_v3rpc_dispatch(cetcd_v3rpc *rpc,
                                      const char *path,
                                      const uint8_t *req_data,
                                      size_t req_len);
```

#### 已实现的 RPC 路径（36 个）

| 服务 | RPC | 处理器文件 | 说明 |
|------|-----|-----------|------|
| KV | `/etcdserverpb.KV/Put` | `kv_handler.c` | 写入键值对，推进 MVCC 修订号 |
| KV | `/etcdserverpb.KV/Range` | `kv_handler.c` | 范围查询 |
| KV | `/etcdserverpb.KV/DeleteRange` | `kv_handler.c` | 删除键，推进 MVCC 修订号 |
| KV | `/etcdserverpb.KV/Txn` | `kv_handler.c` | 事务：解析 compare/success/failure，执行 Put/Delete 操作 |
| KV | `/etcdserverpb.KV/Compact` | `kv_handler.c` | 压缩 MVCC 历史到指定修订号 |
| Lease | `/etcdserverpb.Lease/LeaseGrant` | `lease_handler.c` | 授予租约，返回 ID+TTL |
| Lease | `/etcdserverpb.Lease/LeaseRevoke` | `lease_handler.c` | 撤销租约 |
| Lease | `/etcdserverpb.Lease/LeaseKeepAlive` | `lease_handler.c` | 续约，返回剩余 TTL |
| Lease | `/etcdserverpb.Lease/LeaseTimeToLive` | `lease_handler.c` | 查询租约剩余时间 |
| Lease | `/etcdserverpb.Lease/LeaseLeases` | `lease_handler.c` | 列出所有租约 |
| Watch | `/etcdserverpb.Watch/Watch` | `watch_handler.c` | 创建/取消观察者，返回 watch_id+事件 |
| Auth | `/etcdserverpb.Auth/AuthEnable` | `auth_handler.c` | 启用认证 |
| Auth | `/etcdserverpb.Auth/AuthDisable` | `auth_handler.c` | 禁用认证 |
| Auth | `/etcdserverpb.Auth/AuthStatus` | `auth_handler.c` | 查询认证状态（enabled/disabled） |
| Auth | `/etcdserverpb.Auth/Authenticate` | `auth_handler.c` | 密码验证，返回 token |
| Auth | `/etcdserverpb.Auth/UserAdd` | `auth_handler.c` | 添加用户 |
| Auth | `/etcdserverpb.Auth/UserDelete` | `auth_handler.c` | 删除用户 |
| Auth | `/etcdserverpb.Auth/UserList` | `auth_handler.c` | 列出所有用户名 |
| Auth | `/etcdserverpb.Auth/UserChangePassword` | `auth_handler.c` | 修改用户密码 |
| Auth | `/etcdserverpb.Auth/UserGrantRole` | `auth_handler.c` | 授予用户角色 |
| Auth | `/etcdserverpb.Auth/UserRevokeRole` | `auth_handler.c` | 撤销用户角色 |
| Auth | `/etcdserverpb.Auth/RoleAdd` | `auth_handler.c` | 添加角色 |
| Auth | `/etcdserverpb.Auth/RoleDelete` | `auth_handler.c` | 删除角色 |
| Auth | `/etcdserverpb.Auth/RoleList` | `auth_handler.c` | 列出所有角色名 |
| Cluster | `/etcdserverpb.Cluster/MemberList` | `cluster_handler.c` | 列出集群成员 |
| Cluster | `/etcdserverpb.Cluster/MemberAdd` | `cluster_handler.c` | 添加集群成员 |
| Cluster | `/etcdserverpb.Cluster/MemberRemove` | `cluster_handler.c` | 移除集群成员 |
| Cluster | `/etcdserverpb.Cluster/MemberUpdate` | `cluster_handler.c` | 更新成员地址 |
| Cluster | `/etcdserverpb.Cluster/MemberPromote` | `cluster_handler.c` | 提升学习者为投票成员 |
| Maintenance | `/etcdserverpb.Maintenance/Status` | `maint_handler.c` | 返回版本/dbSize/raftIndex/raftTerm |
| Maintenance | `/etcdserverpb.Maintenance/Defragment` | `maint_handler.c` | 碎片整理（LMDB 自动管理，no-op） |
| Maintenance | `/etcdserverpb.Maintenance/Hash` | `maint_handler.c` | 返回 KV 存储哈希值 |
| Maintenance | `/etcdserverpb.Maintenance/HashKV` | `maint_handler.c` | 返回哈希值+压缩修订号 |
| Maintenance | `/etcdserverpb.Maintenance/Alarm` | `maint_handler.c` | 告警获取/激活/停用 |
| Maintenance | `/etcdserverpb.Maintenance/MoveLeader` | `maint_handler.c` | 领导者转移请求 |
| Maintenance | `/etcdserverpb.Maintenance/Snapshot` | `maint_handler.c` | 返回 KV 存储快照（单次返回所有键值对） |
| Maintenance | `/etcdserverpb.Maintenance/Downgrade` | `maint_handler.c` | 集群版本降级（no-op，返回当前版本） |

---

### server — 服务器主体

**库**：`libcetcd_server`
**头文件**：`include/cetcd/server.h`
**源码**：`src/server/server.c`
**依赖**：几乎所有模块

`cetcd_server` 是所有组件的组装点：

```c
struct cetcd_server {
    cetcd_server_config  cfg;
    cetcd_v3rpc         *rpc;
    cetcd_raft          *raft;
    cetcd_cluster       *cluster;
    cetcd_backend       *backend;
    cetcd_wal_encoder   *wal_enc;
    cetcd_loop          *loop;
    cetcd_tcp           *listener;
    cetcd_tcp           *peer_listener;
    cetcd_timer         *tick_timer;
    cetcd_metrics       *metrics;
    bool                 started;
};
```

#### 服务器生命周期

```
cetcd_server_new() → cetcd_server_start() → cetcd_server_serve() → cetcd_server_stop() → cetcd_server_free()
```

1. **new**：初始化 v3rpc、raft、cluster、metrics；设置全局 `g_rpc_cluster` / `g_rpc_node_id`
2. **start**：打开 backend（LMDB）、创建 WAL 编码器、添加初始对等节点、设置集群消息发送回调
3. **serve**：创建事件循环、绑定客户端/对等端口、启动 Raft tick 定时器、运行事件循环
4. **stop**：设置 `started = false`
5. **free**：逆序释放所有资源

#### Raft 驱动 (process_ready_)

- 100ms 定时器触发 `raft_tick_cb_` → `cetcd_raft_tick` + `process_ready_`
- **WAL 持久化**：将 `rd.entries` 中的新条目写入 WAL，刷新到磁盘
- **HardState 持久化**：如果 `rd.hard_state` 非空，编码并写入 WAL
- **消息发送**：将 `rd.messages` 编码为线路格式，通过 `cetcd_cluster_send_msg` 发送给对等节点
- **已提交条目应用**：将 `rd.committed` 之前的 NORMAL 条目应用到 MVCC 存储：
  - 条目数据格式：`tag(1B) + key_len(varint) + key + val_len(varint) + val`
  - tag=1: Put（`cetcd_mvcc_put`），tag=2: Delete（`cetcd_mvcc_delete`）
- 更新 metrics（`raft_committed_index`、`mvcc_revision`）
- 调用 `cetcd_raft_advance` 确认处理完成

#### 对等节点消息接收

对等节点连接到达时，`on_peer_incoming_` 创建 `peer_ctx_` 并启动 uv_read_start。
数据到达后 `on_peer_read_` 执行：
1. 解析 4 字节大端长度前缀帧
2. 调用 `cetcd_msg_decode` 解码对等节点封装
3. 调用 `cetcd_msg_decode_wire` 解码 Raft 线路格式
4. 调用 `cetcd_raft_step` 馈入 Raft 状态机
5. 调用 `process_ready_` 处理产生的状态变更

发送端 `peer_send_cb_` 同样使用 4 字节长度前缀帧，保证收发一致。

#### 客户端请求处理

1. 客户端连接 → `on_client_conn_` 创建 `client_ctx_`
2. 数据到达 → `on_client_read_` 解析帧：`path_len(2B) + path + grpc_frame(5B header + payload)`
3. 分发到 `cetcd_server_handle_rpc` → `cetcd_v3rpc_dispatch`
4. 响应原路写回

#### cetcdctl 客户端 CLI

`cmd/cetcdctl/main.c` 提供完整的命令行客户端，使用与服务器相同的自定义 gRPC 帧协议：

```
帧格式: 2B path_len(BE) + path + 1B compressed + 4B payload_len(BE) + payload
```

支持的全局选项和命令：

| 全局选项 | 默认值 | 说明 |
|---------|--------|------|
| `--host ADDR` | 127.0.0.1 | 服务器地址 |
| `--port PORT` | 2379 | 服务器端口 |

| 命令 | 说明 |
|------|------|
| `put KEY VALUE` | 存储键值对 |
| `get KEY` | 获取键值 |
| `del KEY` | 删除键 |
| `lease grant TTL` | 授予租约 |
| `lease revoke ID` | 撤销租约 |
| `lease timetolive ID` | 查询租约剩余时间 |
| `txn put KEY VALUE` | 事务写入 |
| `compact REV` | 压缩 MVCC 历史到指定修订号 |
| `status` | 获取服务器状态 |
| `alarm` | 查询告警 |
| `member list` | 列出集群成员 |
| `auth enable/disable/status` | 认证管理 |
| `user add/list` | 用户管理 |
| `role add/list` | 角色管理 |
| `snapshot save [FILE]` | 保存快照到文件 |
| `downgrade enable/cancel/validate` | 集群版本降级 |

---

## 数据流

### 写请求流程

```
Client → TCP → gRPC帧解码 → cetcd_server_handle_rpc()
    → cetcd_v3rpc_dispatch() → kv_handle_put()
    → cetcd_mvcc_put() → 更新 treap 索引 + history
    → 通知 watchers
    ← 编码 gRPC 响应 ←
```

### Raft 复制流程

```
Leader 收到客户端请求
  → cetcd_raft_propose() → 追加到本地日志
  → cetcd_raft_ready() 取出待发消息
  → cetcd_msg_encode_wire() 序列化
  → cetcd_cluster_send_msg() 发送给 Follower

Follower 收到 App 消息
  → cetcd_raft_step() → handle_app_()
  → 追加条目 + 返回 AppResp
  → cetcd_raft_ready() 取出 HardState 更新

Leader 收到 AppResp
  → 更新 progress[slot].match_idx
  → maybe_advance_commit_() → 推进 commit
```

### 读请求流程

```
Client → cetcd_v3rpc_dispatch() → kv_handle_range()
  → cetcd_mvcc_range() → cetcd_treap_range() 遍历
  ← 收集结果返回 ←
```

---

## 构建系统

### CMake 配置

- **最低版本**：CMake 3.21
- **C 标准**：C99（需要 C11 `<stdatomic.h>`）
- **强制 out-of-source 构建**
- **默认构建类型**：Release

### 构建选项

| 选项 | 默认值 | 说明 |
|------|--------|------|
| `CETCD_BUILD_TESTS` | ON | 构建单元+集成测试 |
| `CETCD_BUILD_FUZZ` | OFF | 构建 libFuzzer 模糊测试目标 |
| `CETCD_BUILD_BENCH` | OFF | 构建基准测试套件 |
| `CETCD_BUILD_EXAMPLES` | OFF | 构建示例程序 |
| `CETCD_ENABLE_LTO` | OFF | Release 模式启用链接时优化 |
| `CETCD_ENABLE_COVERAGE` | OFF | 启用 gcov 覆盖率 |
| `CETCD_USE_SYSTEM_OPENSSL` | ON | 使用系统 OpenSSL |
| `CETCD_USE_SYSTEM_LMDB` | OFF | 使用系统 LMDB |
| `CETCD_WERROR` | OFF | 警告视为错误 |
| `CETCD_SANITIZERS` | "" | 逗号分隔的 sanitizer 列表 |

### 第三方依赖

| 库 | 路径 | 说明 |
|----|------|------|
| libuv | `third_party/libuv/` | 跨平台事件循环（MIT） |
| libco | `third_party/libco/` | 栈式协程（Apache-2.0） |
| LMDB | `third_party/lmdb/` | mmap B+tree 数据库 |
| protobuf-c | `third_party/protobuf-c/` | Protobuf C 运行时 |
| OpenSSL | 系统安装 | TLS 终止（≥ 3.0） |

### 编译器警告

GCC/Clang 下启用严格警告：`-Wall -Wextra -Wpedantic -Wshadow -Wpointer-arith -Wcast-qual -Wcast-align -Wstrict-prototypes -Wmissing-prototypes -Wformat=2 -fvisibility=hidden`

---

## 测试体系

| 层级 | 框架 | 位置 | 说明 |
|------|------|------|------|
| 单元测试 | Unity + CMock | `tests/unit/<module>/` | 每个模块一个二进制 |
| 集成测试 | 自定义 harness | `tests/integration/` | 启动真实 cetcd 进程 |
| 模糊测试 | libFuzzer | `tests/fuzz/` | proto/WAL/Raft 步进函数 |
| 性能测试 | 自定义 | `bench/` | 微基准测试 |

### CI 矩阵

`.github/workflows/ci.yml` 覆盖：
- Linux (gcc + clang)
- macOS
- Windows (MSVC + MinGW)
- FreeBSD
- Linux cross-aarch64

---

## 架构决策记录 (ADR)

| ADR | 标题 | 核心决策 |
|-----|------|----------|
| [0001](../adr/0001-raft-rolled-in-house.md) | 自研 Raft | 镜像 go.etcd.io/raft API，无 I/O 无线程；拒绝 canonical/raft (LGPL)、willemt/raft (功能弱)、RedisLabs/raft (许可证) |
| [0002](../adr/0002-lmdb-backend.md) | LMDB 后端 | 使用 LMDB 的 mmap + 单写多读模型；不兼容 bbolt 格式，提供单向迁移工具 |
| [0003](../adr/0003-coroutines-on-libuv.md) | 协程+libuv | 选择 libco 栈式协程 + libuv 事件循环；拒绝回调模型和线程模型 |

---

## 术语表

| 术语 | 说明 |
|------|------|
| **Revision** | 单调递增的全局提交计数器（etcd MVCC 的通用时钟） |
| **Lease** | 有 TTL 的句柄，键可附加其上，到期时一起删除 |
| **Apply** | 将已提交的 Raft 条目应用到状态机（mvcc/backend） |
| **Snapshot** | 状态机的序列化检查点，允许日志截断 |
| **WAL** | 预写日志——每个 Raft 条目在提交前先 fsync 到此 |
| **Watcher** | 长期存在的流，订阅某键范围在指定修订号之后的事件 |
| **Ready** | Raft 节点产生的待处理状态，嵌入者必须按序持久化+发送 |
| **Advance** | 告知 Raft 节点 Ready 已处理完毕，可继续推进 |
