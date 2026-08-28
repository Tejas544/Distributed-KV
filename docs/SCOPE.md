# Anvil — Exact Scope

This document is the contract. If something is not in here, it is not being built. If something in here is cut, it gets crossed out with a dated reason, not silently dropped.

---

## 1. Product definition

Anvil is a replicated, range-sharded, transactional key-value store. A client sees:

```cpp
// Keys and values are opaque byte strings. Keys are ordered lexicographically.
Txn      begin(IsolationLevel lvl, ReadMode mode);      // lvl ∈ {SI, SSI, StrictSerializable}
Result   Txn::get(Key k);                                // snapshot / uncertainty-aware read
Result   Txn::scan(Key lo, Key hi, Limit n, Order o);    // ordered range scan, forward + reverse
void     Txn::put(Key k, Value v);
void     Txn::del(Key k);
Result   Txn::cas(Key k, optional<Value> expected, Value v);
Status   Txn::commit();                                  // Committed | Aborted{reason} | Unknown
void     Txn::rollback();
Stream   watch(Key lo, Key hi, Timestamp from);          // ordered change feed (rangefeed)
```

Plus an admin surface: `split(key)`, `merge(range)`, `transfer_lease(range, node)`, `add_replica`, `remove_replica`, `add_learner`, `snapshot`, `compact`, `describe_ranges`, `cluster_status`.

`Unknown` is a first-class commit result. Any client that treats it as failure is buggy, and the workload generator deliberately produces it and the checker deliberately accounts for it. Most student projects pretend this state doesn't exist; it is where a large fraction of real distributed-transaction bugs live.

---

## 2. Guarantee matrix

| Mode | Isolation | Recency of reads | Mechanism | Checked by |
|---|---|---|---|---|
| `SI` | Snapshot isolation | Reads at a TSO-assigned start timestamp | Percolator 2PC: prewrite with primary lock, commit primary, lazy secondary resolution | Elle-style DSG: must show no G0/G1a/G1b/G1c; **write skew (G2-item) permitted and expected** |
| `SSI` | Serializable | Linearizable via leader lease / ReadIndex | Write intents + read refresh on timestamp push + HLC uncertainty interval restarts | DSG must be acyclic under all of {ww, wr, rw} |
| `StrictSerializable` | Serializable + real-time order | Linearizable | `SSI` plus commit-wait over a bounded-uncertainty clock oracle (simulated TrueTime), plus closed timestamps for follower reads | DSG acyclic **and** the real-time precedence edges of the wall-clock history are consistent with the serialization order |
| `BoundedStaleness` (reads only) | Snapshot at a closed timestamp | Stale by at most `max_staleness` | Follower reads below the closed timestamp | Read-only histories checked against the version log; staleness bound asserted |

**The correction that matters:** Percolator alone gives snapshot isolation, not serializability, and certainly not strict serializability. Write skew is legal under SI. Anvil therefore ships three engines behind one interface and the checker is told which level to enforce. Claiming "strictly serializable" while implementing plain Percolator is the single most common way this project class falls apart under interview questioning.

Durability: a `Committed` result means the transaction's records are fsynced on a quorum of each participating range's replicas. Anvil also supports a `--durability=async` mode purely so the simulator can prove that the async mode loses exactly the writes it is documented to lose and no others.

---

## 3. Component scope

Each component is specified as **In** (must exist), **Out** (explicitly excluded), **Stretch** (build if the phase lands early).

### 3.1 Storage engine (`core/lsm/`)

**In.** Write-ahead log with CRC32C per record, group commit, and tail truncation at the first invalid record. Skiplist memtable with an arena allocator; immutable memtable queue; flush scheduler. Block-based SSTable: 4 KiB data blocks, restart points every 16 keys, shared-prefix key compression, per-block checksums, block index, optional block compression (LZ4 + Zstd), Bloom filters and an alternative ribbon filter. Block cache with S3-FIFO eviction; separate index/filter cache. LevelDB-style `MANIFEST` and `VersionSet` with atomic version edits and reference-counted file lifetimes. Leveled compaction **and** size-tiered compaction, chosen by config; compaction priority scoring; sub-compaction parallelism (deterministic ordering under sim). Point reads, forward and reverse iterators, snapshot iterators. Full crash recovery.

**Out.** Column families, merge operators, transactions inside the engine (that is the MVCC layer's job), tiered storage to object stores, secondary indexes.

**Stretch.** Key-value separation (WiscKey-style blob files) with its own garbage collector — an excellent source of hard bugs. Universal compaction. Direct I/O path.

### 3.2 MVCC and local concurrency (`core/mvcc/`)

**In.** Key encoding `user_key || 0xFF-inverted(commit_ts)` so that a seek to `(k, read_ts)` lands on the correct version in one iterator step. Separate lock/intent column space. Snapshot reads, uncertainty-interval reads. Version GC driven by a cluster-wide safepoint (`min(active read snapshots, closed timestamp, longest-running txn start)`), executed inside compaction. Lock table with wait queues, wound-wait ordering by transaction start timestamp, and local deadlock detection by wait-for-graph cycle search.

**Out.** Row-level compression of version chains, time-travel queries beyond the GC horizon.

**Stretch.** In-memory unreplicated lock table with lease-based recovery (the CockroachDB approach), which introduces a genuinely subtle correctness question the simulator can attack.

### 3.3 Consensus (`core/raft/`)

**In.** Raft: leader election with randomised timeouts and **pre-vote**, `CheckQuorum`, term/vote durability before responding, log replication with pipelining and batching, `AppendEntries` conflict backtracking with the term-based fast path, commit index advancement restricted to the current term, log compaction, chunked snapshot install with flow control, **joint-consensus** membership change (not the one-at-a-time shortcut), non-voting learners with catch-up tracking, leader leases and `ReadIndex` for linearizable reads without a log write, leadership transfer.

**Out.** Flexible quorums, witness replicas, Raft over multiple groups sharing a heartbeat (see Stretch), Byzantine tolerance.

**Stretch.** MultiRaft heartbeat coalescing across thousands of ranges on one node — required for the sharding layer to scale and a rich source of bugs. A parallel Viewstamped Replication implementation behind the same `ReplicationProtocol` interface, so the same DST suite runs against both. Building both and diffing their bug profiles is a genuinely distinguishing result.

### 3.4 Sharding and placement (`core/shard/`)

**In.** Range descriptors `[start, end)` with generation numbers. A two-level meta index (`meta1` → `meta2` → user ranges), itself stored in ranges. Atomic range split (a transaction over the meta range plus the two new descriptors), atomic merge with lease colocation and quiescence. Replica placement driver running as its own Raft group: size-based splits, load-based splits, replica rebalancing by store capacity and QPS, lease rebalancing, dead-node replacement with a configurable grace period. Client-side range cache with generation-based invalidation and retry on `RangeKeyMismatch`.

**Out.** Zone configurations, geo-partitioning constraints, follower-read locality routing.

**Stretch.** Load-based split-point selection from a reservoir sample of accessed keys. Range quiescence (stop heartbeating idle ranges) — a classic correctness trap.

### 3.5 Distributed transactions (`core/txn/`)

**In.** All three engines from §2 behind one `TxnEngine` interface.
- Percolator path: prewrite, primary lock, commit, lazy secondary cleanup, lock TTL and resolution by a stuck reader, orphaned-lock recovery.
- SSI path: transaction records, write intents, intent resolution, timestamp push on conflict, read refresh spans, uncertainty restarts, and abort-on-refresh-failure.
- Commit-wait path: a `ClockOracle` returning `[earliest, latest]` with a bounded and *simulator-controlled* uncertainty width, plus commit-wait before ack.
- Timestamp allocation: a Raft-replicated TSO with batched allocation and monotonic recovery across failover **and** a decentralised HLC mode, switchable by config so the two can be compared.
- Parallel commit (staging transaction records, so commit latency is one round of consensus rather than two), 1PC fast path for single-range transactions, distributed deadlock detection by a wait-for-graph gossiped to a detector, transaction heartbeats and expiry.
- Change feed (`watch`) with per-range ordering, resolved-timestamp emission, and catch-up scans.

**Out.** Interactive read-modify-write over an external SQL layer, savepoints, nested transactions.

**Stretch.** Read-your-writes buffering in the client with correct interaction against scans (surprisingly hard, frequently wrong in real systems).

### 3.6 The simulator (`sim/`) — the differentiator

**In.**
- **Scheduler.** Single OS thread. C++20 coroutines as green tasks. A priority queue keyed by `(virtual_time, sequence)` — never by pointer or address. Quantized execution: a task runs until it yields at an explicit await point. Deterministic task-selection policies (FIFO, random-weighted by seed, adversarial "delay the task most likely to reveal a race").
- **Virtual clock.** Time advances only when all tasks are blocked. Per-node clock offset, drift rate, discrete jumps, freezes, and NTP-style step corrections. An adversarial mode that keeps every node's clock inside the declared uncertainty bound but maximally spread within it.
- **Network model.** Per-link latency distributions (fixed, uniform, lognormal, bimodal), reordering, duplication, drop with configurable rate, one-way ("asymmetric") partitions, healing partitions, flaky links that flap at a chosen frequency, bandwidth caps with queueing, connection reset, and half-open connections that accept writes and never deliver.
- **Disk model.** A block device state machine: writes land in a modelled page cache; `fsync` promotes a chosen subset to durable; on crash, unsynced blocks resolve to old content, new content, or a **torn** mix at sector granularity, chosen by seed. Bit rot in durable blocks. `ENOSPC`, `EIO`, `EINTR` at any call. Latency spikes and stalled devices. Directory-entry persistence modelled separately from file-content persistence, so "the file exists but is empty after crash" is reachable.
- **Process model.** Crash (immediate, no destructors), clean shutdown, restart with a fresh address space, and **pause/resume** — modelling a VM freeze or a long GC, which is how leases get violated in reality.
- **`BUGGIFY`.** `if (ANVIL_BUGGIFY) { /* rare path */ }`. Compiles to `false` in production. In simulation, each site is either enabled or disabled for the whole run (swarm testing), and enabled sites fire with a per-site probability. Every site is registered with a file/line identity so the fleet can report **BUGGIFY site activation coverage**.
- **Tracing.** A structured event log (`jsonl`) with causality links (`caused_by` event id), plus a rolling 128-bit execution digest over every scheduling decision.
- **Minimisation.** Delta debugging over the fault schedule: given a failing seed, iteratively remove faults/messages and re-run, converging on the smallest fault set that still reproduces. Also seed-space narrowing on the scheduler decision sequence.
- **Time travel.** Periodic full snapshots of simulator + node state so a failing run can be rewound to any earlier tick without replaying from zero.

**Out.** Simulating the OS scheduler at instruction granularity, hypervisor-level determinism (that is Antithesis's job), simulating actual hardware failure electronics.

**Stretch.** Coverage-guided seed selection: instrument branch coverage, keep a corpus of seeds that reached new coverage, mutate scheduler decision sequences rather than just reseeding. This turns DST into a fuzzer and is what makes "thousands of simulated hours" actually productive instead of re-exploring the same interleavings.

### 3.7 Checkers and verification (`checker/`, `spec/`)

**In.**
- **Reference model.** A deliberately naive, obviously-correct single-node implementation: an `std::map`, a serial transaction log, no concurrency. Every simulated run diffs the cluster against it.
- **Invariant evaluators.** ~60 numbered invariants ([docs/INVARIANTS.md](INVARIANTS.md)), each with a declared cost class so cheap ones run every tick and expensive ones run on epochs or at quiesce.
- **Elle-style transactional checker, written from scratch.** List-append and register workloads; version-order recovery from append histories; construction of the direct serialization graph with `ww`, `wr`, and `rw` edges; Tarjan SCC; classification of cycles into G0, G1a (aborted read), G1b (intermediate read), G1c, G-single, G2-item, G2; minimal cycle witness extraction and human-readable rendering. Handles `Unknown` commit outcomes as indeterminate.
- **Cross-validation.** The same histories fed to Jepsen's Elle (Clojure). Any disagreement is a bug in one of the two checkers and gets logged in [BUGS.md](../BUGS.md).
- **Checker mutation testing.** A corpus of synthetic histories with known injected anomalies. The checker must flag 100% of them, and must accept 100% of histories generated by the serial reference model. A checker that never fires is worse than no checker.
- **TLA+ specs** of the Raft variant (including joint consensus) and of the SSI commit protocol, model-checked with TLC over small configurations.
- **Trace validation.** Simulator traces are exported and replayed against the TLA+ spec to confirm the implementation refines the spec. This closes the gap that every "we wrote a TLA+ spec" project leaves open — the spec being correct says nothing about the code.
- **DPOR.** Dynamic partial-order reduction for exhaustive exploration of tiny configurations (3 nodes, 2 keys, 2 transactions, ≤ 6 messages). Exhaustive for the small, random for the large.

**Out.** A machine-checked proof of the implementation (Coq/Iris). Interesting, but a different degree.

### 3.8 Production runtime and benchmarks (`prod/`, `workloads/`)

**In.** The same `core/` code linked against a real runtime: `io_uring` (Linux) with an `epoll` fallback, a thread-per-core executor with message passing between shards, real file I/O with `O_DIRECT` optional, real sockets with a length-prefixed binary protocol. A deployable 5-node cluster via Docker Compose. Workloads: YCSB A–F, a TPC-C subset (New-Order, Payment, Order-Status, Delivery, Stock-Level), a bank-transfer invariant workload, list-append and register workloads for the checker. Benchmarks report throughput, p50/p99/p99.9, write amplification, space amplification, and read amplification. A **deterministic performance simulator** — modelled disk and network latency in virtual time — so performance regressions are caught reproducibly in CI, not just on a noisy laptop.

**In (closing the loop).** A real Jepsen test suite run against the real cluster with `tc`/`netem` faults. The expected and honest result is that Jepsen finds nothing the simulator missed; if it finds something, that is the most valuable bug in the ledger, because it identifies a gap in the *fault model*.

**Out.** Kubernetes operators, Prometheus/Grafana dashboards beyond a minimal metrics endpoint, cloud deployment, TLS.

---

## 4. The Runtime seam

Frozen in week 2 and changed only by explicit agreement, because all four workstreams depend on it.

```cpp
struct Runtime {
  // time
  virtual Timestamp   now() const = 0;                        // HLC / virtual
  virtual Interval    now_uncertain() const = 0;              // [earliest, latest]
  virtual Task<void>  sleep_for(Duration) = 0;
  virtual TimerId     schedule(Duration, std::move_only_function<void()>) = 0;

  // randomness — the ONLY source in the whole system
  virtual uint64_t    random_u64() = 0;
  virtual bool        buggify(BuggifySite) = 0;

  // network
  virtual Task<ConnHandle> connect(NodeId) = 0;
  virtual Task<void>       send(ConnHandle, Message) = 0;
  virtual Task<Message>    recv(ConnHandle) = 0;

  // storage — file abstraction, not block abstraction
  virtual Task<FileHandle> open(Path, OpenFlags) = 0;
  virtual Task<size_t>     pread(FileHandle, Span, Offset) = 0;
  virtual Task<void>       pwrite(FileHandle, Span, Offset) = 0;
  virtual Task<void>       fsync(FileHandle) = 0;
  virtual Task<void>       rename(Path, Path) = 0;             // atomicity is modelled
  virtual Task<void>       unlink(Path) = 0;

  // scheduling
  virtual void        spawn(Task<void>) = 0;
  virtual Task<void>  yield() = 0;
};
```

Rules that CI enforces on everything under `core/`:
1. No `#include <chrono>` system clocks, no `time()`, `gettimeofday`, `clock_gettime`.
2. No `<thread>`, `<mutex>`, `<atomic>` beyond relaxed counters in non-decision paths.
3. No `rand()`, `std::random_device`, `getrandom`.
4. No direct syscalls; all I/O through `Runtime`.
5. No iteration over `std::unordered_*` where the order affects behaviour (custom clang-tidy check; prefer `absl::btree_map` or sorted vectors).
6. No floating point in control flow (`-ffp-contract=off`; a lint pass on the AST).
7. Node-local arena allocators, reset on restart, so pointer values are seed-deterministic and "sorted by address" bugs are caught rather than hidden.

`tools/hermetic_check.py` runs `nm`/`objdump` over the `anvil_core` static library and fails the build on any denylisted symbol. This is a fifteen-line script that makes the entire determinism claim credible, and it is the first thing to write.

---

## 5. The configuration space (what swarm testing varies)

Each simulated run draws a configuration from the seed:

| Dimension | Range |
|---|---|
| Cluster size | 3, 5, 7, 9 nodes |
| Replication factor | 3, 5 |
| Ranges | 1 – 512 |
| Isolation level | SI, SSI, StrictSerializable |
| Compaction strategy | leveled, tiered |
| Timestamp source | replicated TSO, HLC |
| Clock uncertainty bound | 0 – 500 ms |
| Actual clock skew | 0 – 2× the declared bound (the simulator is allowed to *violate* the assumption, to prove the failure mode is safety-preserving or to prove it is not) |
| Network latency profile | LAN, WAN, bimodal, adversarial |
| Drop / reorder / duplicate rates | 0 – 30% |
| Partition schedule | none, single-shot, flapping, asymmetric, majority-isolating |
| Disk failure profile | clean, slow, torn-writes, bit-rot, ENOSPC, dying |
| Crash rate | 0 – 1 crash per simulated minute per node |
| Pause profile | none, 10 ms – 30 s freezes |
| BUGGIFY sites enabled | random subset, 0 – 100% |
| Workload | YCSB A–F, TPC-C, bank, list-append, register, chaos-admin (concurrent splits/merges/membership changes) |
| Membership churn | static, rolling replacement, continuous |
| Binary version mix | uniform, mixed (N-1 / N) for upgrade testing |

The "actual skew may exceed the declared bound" row is deliberate and important: it distinguishes *"this system is safe under its stated assumptions"* from *"this system silently corrupts when the assumption is violated."* Spanner's whole design rests on that bound; knowing exactly what Anvil does when it breaks is a real result.

---

## 6. Tiering (risk management, not scope reduction)

The full scope above is the target. These tiers exist so that if a phase overruns, the cut is a decision rather than an accident.

- **T0 — the project fails without it.** Runtime seam, hermetic enforcement, simulator core (net/disk/clock/crash), LSM engine with crash recovery, Raft with snapshots and membership change, MVCC + SI, Percolator 2PC, single-range and cross-range transactions, invariant catalogue, reference model, Elle-style checker, bug ledger with reproducible seeds.
- **T1 — the project is merely good without it.** SSI and strict serializability engines, range split/merge and the placement driver, parallel commit, TLA+ specs and trace validation, delta-debugging minimisation, coverage-guided seed selection, production runtime and benchmarks, Jepsen loop-closure.
- **T2 — the project becomes distinctive.** VSR as a second consensus implementation behind the same interface, DPOR exhaustive checking, key-value separation, MultiRaft, mixed-version upgrade testing, deterministic performance simulation, the trace-viewer UI, the writeup/talk.

Nothing gets promoted from T2 to "skipped" without being crossed out here with a date and a reason.

---

## 7. Team split (four workstreams)

Interfaces are frozen at the end of P0 so these run in parallel with minimal blocking.

| Stream | Owns | Depends on |
|---|---|---|
| **A — Simulator & tooling** | `sim/`, `tools/`, CI, hermetic checks, minimiser, trace viewer, seed fleet | `Runtime` interface only |
| **B — Storage** | `core/lsm/`, `core/mvcc/`, storage-level DST, compaction, benchmarks | `Runtime` |
| **C — Consensus & sharding** | `core/raft/`, `core/shard/`, placement driver, membership, snapshots | `Runtime`, `core/lsm` write API |
| **D — Transactions & verification** | `core/txn/`, `checker/`, `spec/`, reference model, workloads, the ledger | `core/mvcc`, `core/raft` interfaces |

Every stream writes its own invariants into [docs/INVARIANTS.md](INVARIANTS.md) and its own simulated-fault tests. **No stream is "done" until its layer has run 100+ simulated hours under fault injection with its invariants armed.**

---

## 8. Definition of Done (applies to every component)

1. Unit tested with the reference model as oracle.
2. Its invariants are registered, numbered, and armed in the simulator.
3. It survives its phase's fault profile for the phase's required simulated hours with zero invariant violations.
4. Every bug found is in [BUGS.md](../BUGS.md) with a pinned regression seed in `test/corpus/`.
5. Seeded-mutation test: at least five deliberate protocol bugs are injected into the component, and the harness detects all of them within the phase's detection-time budget. **A component whose deliberate bugs go undetected has not been tested; it has been run.**
6. Documented: a `docs/design/<component>.md` explaining the design and, more usefully, the alternatives rejected.

---

## 9. Non-goals

SQL and any query planner. Secondary indexes. Encryption at rest or in flight. Multi-tenancy, quotas, RBAC. Geo-partitioning policy. Backup/restore tooling beyond a raw snapshot. A production ops story (rolling-upgrade automation, observability stack). Byzantine fault tolerance. Cross-datacenter topology optimisation. Anything that adds surface area without adding a new *class* of correctness bug.

The filter for every proposed addition: **does it create a new category of interleaving that the simulator can attack?** If yes, it is in scope. If it is merely more code, it is not.

---

## <a name="toolchain"></a>10. Toolchain notes for this machine

Currently present: `g++ 15.2` (MSYS2 UCRT64), `python3`, `git`, Docker, Go, Node.

Needed and not yet installed:
- **CMake + Ninja** — build system.
- **clang/clang-tidy 18+** — required for the custom determinism lint checks and for the second-compiler determinism cross-check.
- **WSL2 (Ubuntu)** — `io_uring`, `perf`, `nm`/`objdump` behaviour matching CI, and the Linux determinism baseline. Do the real development here; MSYS2 is for convenience only.
- **Java + Clojure/Leiningen** — Jepsen and the reference Elle checker for cross-validation.
- **TLA+ Toolbox / TLC** (or `tla2tools.jar` on the JDK 25 already installed).

The determinism guarantee is per-toolchain until proven otherwise. CI must run the digest check on **two compilers and two architectures**; anything less and "deterministic" means "deterministic on my laptop."
