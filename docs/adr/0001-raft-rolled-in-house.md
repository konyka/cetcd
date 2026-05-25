# ADR 0001 — Roll our own Raft, modeled on go.etcd.io/raft

- **Status**: Accepted (Phase 0).
- **Date**: 2026-05-25.
- **Deciders**: project author.

## Context

cetcd needs Raft consensus. Upstream etcd uses `go.etcd.io/raft`, which is the canonical
implementation: a deterministic state-machine API (`Step`/`Ready`/`Advance`) with zero I/O
and zero threads of its own. The embedder owns persistence and transport, which is exactly
what we want.

We surveyed existing C Raft libraries:

| Library          | License | API style                              | Verdict |
| ---------------- | ------- | -------------------------------------- | ------- |
| canonical/raft   | LGPL-3  | I/O-coupled (fsync/network as callbacks) | Reject — couples I/O into the raft core; doesn't fit etcd's Ready pattern; LGPL adds friction. |
| willemt/raft     | BSD-3   | Single-leader, simple                  | Reject — no joint consensus, weak snapshot story, low test coverage, last meaningful release stale. |
| RedisLabs/raft   | RSALv2  | Single-Raft for RedisRaft module       | Reject — non-OSI license, tightly coupled to Redis. |
| (others)         |         |                                        | braft is C++, raft-rs is Rust — out of scope. |

None match `go.etcd.io/raft`'s API contract closely enough, and none would let us share the
upstream test corpus.

## Decision

Implement `libcetcd_raft` ground-up, with an API that mirrors `go.etcd.io/raft`:

```c
cetcd_raft_node *cetcd_raft_start(const cetcd_raft_config *cfg);
int   cetcd_raft_step(cetcd_raft_node *n, const cetcd_raft_msg *msg);
cetcd_raft_ready *cetcd_raft_ready_take(cetcd_raft_node *n);
void  cetcd_raft_advance(cetcd_raft_node *n, cetcd_raft_ready *r);
```

The module:
- does no I/O,
- spawns no threads,
- is fully deterministic given seeded `rand`,
- exposes everything through one header (`include/cetcd/raft.h`).

The embedder (`libcetcd_server`) owns:
- a single dedicated Raft thread (or stays inline on the reactor — TBD by bench),
- the WAL writer (`libcetcd_wal`) for entry persistence,
- the peer transport (`libcetcd_peer`) for sending messages,
- the apply pipeline.

## Consequences

### Positive

- One Raft implementation to maintain, designed with our error handling and memory model.
- Test corpus can be ported almost line-by-line from `go.etcd.io/raft/*_test.go`
  (it's table-driven and language-neutral). We get high coverage cheaply.
- Apache-2.0 license cleanly; no copyleft worries.
- We can extend (PreVote, ReadIndex, Learner, joint consensus) on our own schedule.

### Negative

- Roughly **7000 LOC of consensus code** to write and verify — the riskiest module.
- We re-invent some battle-tested mechanics (log compaction edge cases, joint-consensus
  membership transitions).

### Mitigations

- TDD strictly: every property in the Raft paper gets at least one test before code.
- Property-based / randomised tests on small clusters (3–5 nodes) with a fake network.
- Schedule Raft as Phase 2 (right after foundations) so it gets the most stabilisation time.
- Consult Oracle on every non-trivial PR touching `libcetcd_raft`.

## References

- Diego Ongaro, John Ousterhout — *In Search of an Understandable Consensus Algorithm*, 2014.
- `go.etcd.io/raft` source: <https://github.com/etcd-io/raft>.
- canonical/raft (rejected): <https://github.com/canonical/raft>.
- willemt/raft (rejected): <https://github.com/willemt/raft>.
