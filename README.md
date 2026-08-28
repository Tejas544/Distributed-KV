# Anvil

**A sharded, strictly-serializable distributed key-value store in C++20 — whose correctness is established by a deterministic simulator that hunts its own bugs.**

> Anvil is two artifacts that happen to live in one repo:
> 1. a real database (LSM engine, Raft, MVCC, distributed transactions, range sharding), and
> 2. a **protocol-aware deterministic simulation testing (DST)** harness that runs the entire cluster on one thread, controls every source of nondeterminism, injects adversarial faults, and asserts internal protocol invariants that are *invisible at the client API boundary*.
>
> The database is the well-trodden part. The simulator is the point.

---

## Status

| | |
|---|---|
| Phase | `P1 — Simulator core`, stream A complete (see [docs/ROADMAP.md](docs/ROADMAP.md)) |
| Working | Runtime seam · hermeticity gate + negative control · deterministic scheduler on virtual time · **19 fault kinds** across network, disk, clock and process · swarm-drawn fault profiles · durable replicated-counter workload · execution digest · causal trace |
| Next | P1 stream D: the reference model, the invariant framework with cost classes, and the Elle-style consistency checker |
| Language | C++20 (coroutines, concepts, ranges) + Python 3 tooling + TLA+ specs |
| Platforms | Linux x86-64 (primary), macOS arm64 (determinism cross-check), Windows via WSL2 |
| Bug ledger | [BUGS.md](BUGS.md) |
| Invariant catalogue | [docs/INVARIANTS.md](docs/INVARIANTS.md) |
| Exact scope | [docs/SCOPE.md](docs/SCOPE.md) |

---

## The thesis

Distributed databases fail in the interleavings you did not think to write a test for. The industry answer is deterministic simulation: remove parallelism, quantize execution, seed every source of randomness, and replace the network, disk, and clock with models you control. A failing run becomes a single 64-bit integer. Debugging goes from "reproduce it over a weekend" to "re-run the seed."

FoundationDB built this and Jepsen's author declined to test them, on the grounds that their own simulator had already stressed the system harder. TigerBeetle used it to reach Jepsen-passing in three years. AWS uses it. It is standard practice at the top of this industry and almost absent from student projects.

Anvil goes one level further than the standard technique. Jepsen and Antithesis test **from the outside in** — they observe client-visible histories and search for anomalies. Anvil also tests **from the inside out**: every scheduler tick, the simulator evaluates a catalogue of ~60 numbered protocol invariants over the *internal state of every node* — Raft's Leader Completeness, MVCC GC safepoints, range-descriptor coverage, Percolator primary-lock rules. A bug that corrupts the Raft log but is masked by a subsequent snapshot never reaches a client, so an outside-in checker will never see it. Anvil sees it, on the tick it happens, with the full causal trace attached.

---

## What is actually built

| Layer | Contents |
|---|---|
| **Storage engine** | LSM-tree: WAL with group commit, skiplist memtable, block-based SSTables with prefix compression + restart points, Bloom/ribbon filters, block cache (S3-FIFO), leveled **and** tiered compaction, LevelDB-style MANIFEST/VersionSet, per-block checksums, crash recovery |
| **Concurrency** | MVCC over an inverted-timestamp key encoding; snapshot isolation; SSI with read-refresh; version GC with a distributed safepoint (closed timestamps) |
| **Consensus** | Raft with pre-vote, CheckQuorum, leader leases, pipelined + batched replication, chunked snapshot transfer, log compaction, joint-consensus membership change, non-voting learners, ReadIndex and lease-based linearizable reads |
| **Sharding** | Range partitioning, two-level meta ranges, atomic split/merge, a Raft-replicated placement driver, load- and size-based rebalancing, client range cache with invalidation |
| **Transactions** | Three interchangeable engines behind one interface: **(a)** Percolator 2PC / snapshot isolation, **(b)** CockroachDB-style SSI with write intents, read refresh, and HLC uncertainty intervals → strict serializability, **(c)** Spanner-style commit-wait against a simulated TrueTime oracle → external consistency. Parallel commit, 1PC fast path, distributed deadlock detection, follower reads |
| **The simulator** | Single-threaded coroutine scheduler; modelled network (latency, reorder, drop, duplicate, asymmetric partition, flaky link, bandwidth cap); modelled block device (torn writes at sector granularity, fsync reordering, bit rot, `ENOSPC`, `EIO`, latency spikes); modelled clock (skew, drift, jump, freeze); process crash/restart/pause; `BUGGIFY` rare-path injection; swarm testing; automatic fault-schedule minimisation by delta debugging |
| **The checkers** | A from-scratch Elle-style transactional consistency checker (dependency-graph construction, Adya anomaly classification G0/G1a/G1b/G1c/G-single/G2, Tarjan SCC, witness minimisation), cross-validated against Jepsen's Elle; TLA+ specs of the Raft variant and the commit protocol, model-checked with TLC; **trace validation** linking simulator executions back to the TLA+ specs |

Full boundaries, including explicit non-goals: [docs/SCOPE.md](docs/SCOPE.md).

---

## Architecture

```
                    ┌──────────────────────────────────────────┐
   client ────────► │  anvil-client: range cache, retry,       │
                    │  txn coordinator stub, follower reads    │
                    └───────────────────┬──────────────────────┘
                                        │  (Runtime-abstracted RPC)
   ┌────────────────────────────────────┴────────────────────────────────┐
   │  node                                                                │
   │   ┌────────────┐  ┌──────────────┐  ┌───────────────────────────┐   │
   │   │ placement  │  │ txn engine   │  │ range router / lease mgr  │   │
   │   │ driver     │  │ SI | SSI |   │  └───────────┬───────────────┘   │
   │   │ (raft grp) │  │ commit-wait  │              │                   │
   │   └────────────┘  └──────┬───────┘   ┌──────────┴──────────┐        │
   │                          │           │  raft group / range │        │
   │                   ┌──────┴───────────┴─────────────────┐   │        │
   │                   │ MVCC keyspace + lock table         │   │        │
   │                   └──────────────┬─────────────────────┘   │        │
   │                   ┌──────────────┴─────────────────────────┴─────┐  │
   │                   │ LSM engine: WAL │ memtable │ SST │ compaction │  │
   │                   └──────────────┬───────────────────────────────┘  │
   └──────────────────────────────────┼─────────────────────────────────-┘
                                      │
   ══════════════════════ Runtime seam (the whole trick) ══════════════════
                                      │
        ┌─────────────────────────────┴─────────────────────────────┐
        │                                                           │
   ┌────┴───────────────────────┐              ┌────────────────────┴────┐
   │ SimRuntime                 │              │ ProdRuntime             │
   │ virtual clock, modelled    │              │ CLOCK_MONOTONIC + HLC,  │
   │ net & disk, seeded PRNG,   │              │ io_uring/epoll, real fs,│
   │ crash/pause, BUGGIFY       │              │ real threads            │
   └────────────────────────────┘              └─────────────────────────┘
```

**One rule governs the whole codebase:** everything above the seam is a pure state machine — no syscalls, no wall clock, no threads, no unseeded randomness, no iteration over unordered containers in a decision path, no floating point in control flow. CI enforces this by scanning the linked symbols of the `anvil_core` target against a denylist and by requiring bit-identical execution digests across two runs *and* across Linux x86-64 and macOS arm64.

---

## The money demo

Every failure is a seed. What runs today:

```bash
anvil-sim --seed 0x8f3a91c40d2e77b1 --verify
```

```
seed          0x8f3a91c40d2e77b1
config        0x6219fc43a0e2b4bc
digest        ae4bf7cb69a92be92738deb0d67bb50e
result        quiesced
sim-time      50 ms
events        196
laps          20
checksum      0x1f38ea6c44e1f785
verify        identical across two runs
```

The shape it grows into, once the invariant catalogue and fault models are armed:

```bash
anvil-sim --seed 0x8f3a91c40d2e77b1 --config workloads/tpcc_5node.toml
```

```
[FAIL] tick 4,182,993  sim-time 02:14:07.318
  INV-RAFT-04 (Leader Completeness) violated on n3
  entry (term=7, index=1194) committed in term 7, absent from leader of term 9
  faults active: partition{n1,n2 | n3,n4,n5}, clock_skew(n4,+840ms), BUGGIFY@raft/log.cc:412
  trace: artifacts/0x8f3a91c40d2e77b1/trace.jsonl (3.1M events)
  minimising fault schedule... 11 faults -> 3 faults in 47s
  replay: anvil-sim --seed 0x8f3a91c40d2e77b1 --replay --break-at-tick 4182993
```

Then step backwards through the causal history in the trace viewer, fix, and pin the seed into `test/corpus/` so it runs forever.

---

## Results

> **These numbers are generated by `tools/report.py` from CI artifacts on every nightly run and written into this section automatically. Do not hand-edit.** Nothing here is a claim until the ledger and the seeds behind it are in the repo.

<!-- BEGIN GENERATED RESULTS -->
```
determinism, same seed x2 ............ 10,000 / 10,000 seeds (no faults)
determinism, same seed x2 ............  2,000 / 2,000  seeds (adversary armed)
determinism, -O0 / -O2 / -O3 ......... identical rollup digest
determinism, cross-toolchain ......... (CI job armed; awaiting first matrix run)
digest non-vacuity ................... 10,000 / 10,000 distinct digests

simulation throughput ................ ~2.4M events/sec, 1 core
simulated node-hours per core-hour ... ~38,500  (counter workload, faults on)
wall-clock speedup (sim:real) ........ ~265x   (ping-pong, 5 nodes, 1 core)

fault kinds implemented .............. 19
fault kinds firing in the sweep ...... 19 / 19   (15 asserted here, 4 in disk_crash)
durability, 400 seeds under faults ... 23,783 acked increments, 0 lost
liveness, 400 seeds after healing .... 0 unconverged
media corruption ..................... 58 seeds, 100% detected by checksum

seeded durability bug A (ack pre-fsync)  detected in 218/233 crashing seeds
seeded durability bug B (no dir fsync)   detected in 233/233 crashing seeds

bugs found (S0/S1/S2/S3/S4) .......... 0/0/1/0/3   (see BUGS.md)
BUGGIFY site activation coverage ..... n/a -- no BUGGIFY sites in the core yet
invariant framework .................. (pending P1 stream D)
checker mutation score ............... (pending P7)
TLA+ trace-validation conformance .... (pending P7)
YCSB-A throughput / p99 .............. (pending P9)
TPC-C tpmC / p99 ..................... (pending P9)
```

Measured on the ping-pong and replicated-counter workloads. These are properties
of the **harness**, not of a database — there is no storage engine, consensus,
or transaction code yet. The four bugs in the ledger are three harness defects
and one workload defect, which is the expected shape this early: the adversary
currently has far more to say about the simulator than about the system under
test. See [docs/ROADMAP.md](docs/ROADMAP.md).
<!-- END GENERATED RESULTS -->

---

## Repository layout

```
anvil/
  core/           # hermetic: no syscalls, no clock, no threads. The state machines.
    runtime/      #   Runtime interface: clock, random, net, disk, spawn, timers
    lsm/          #   WAL, memtable, sstable, compaction, cache, manifest
    mvcc/         #   version store, lock table, GC safepoints
    raft/         #   consensus, membership, snapshots, leases
    shard/        #   range descriptors, split/merge, placement driver
    txn/          #   si_engine, ssi_engine, commitwait_engine, deadlock detector
  sim/            # SimRuntime: scheduler, net/disk/clock models, BUGGIFY, minimiser
  prod/           # ProdRuntime: io_uring, epoll, real fs, thread pool
  checker/        # Elle-style DSG checker, reference model, invariant evaluators
  spec/           # TLA+ / PlusCal specs + trace-validation harness
  workloads/      # YCSB A-F, TPC-C subset, list-append, register, bank transfer
  tools/          # hermetic_check.py, report.py, seed scheduler, trace viewer
  test/corpus/    # pinned regression seeds, one per fixed bug
  docs/
```

---

## Build

```bash
cmake -B build -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo -DANVIL_SANITIZER=address
```

```bash
cmake --build build -j && ctest --test-dir build --output-on-failure
```

Local toolchain notes for this machine are in [docs/SCOPE.md](docs/SCOPE.md#toolchain) — CMake and clang-tidy are not yet installed, and `io_uring` work requires WSL2 or the provided Docker image.

---

## Documents

| Document | What it is |
|---|---|
| [docs/SCOPE.md](docs/SCOPE.md) | Exact scope, guarantee matrix, interface contracts, non-goals, tiering, team split |
| [docs/ROADMAP.md](docs/ROADMAP.md) | Ten phases, deliverables, measurable exit criteria, risks |
| [docs/INVARIANTS.md](docs/INVARIANTS.md) | The numbered invariant catalogue — the heart of protocol-aware DST |
| [BUGS.md](BUGS.md) | The bug ledger. Every finding with seed, commit, root cause, and API-visibility |

---

## Non-goals

SQL, secondary indexes, a query planner, geo-replication topology optimisation, encryption at rest, multi-tenancy, and a production operations story. Anvil is a correctness artifact, not a product. See [docs/SCOPE.md](docs/SCOPE.md#non-goals) for the full list and the reasoning.

---

## References

Raft (Ongaro & Ousterhout; Ongaro's thesis) · Viewstamped Replication Revisited · Percolator (Peng & Dabek) · Spanner (Corbett et al.) · CockroachDB's parallel commits and closed timestamps · Bigtable · LevelDB/RocksDB internals · Cahill's Serializable Snapshot Isolation · Adya's thesis on isolation levels · Kingsbury & Alvaro, "Elle: Inferring Isolation Anomalies from Experimental Observations" · Will Wilson, "Testing Distributed Systems w/ Deterministic Simulation" (Strange Loop 2014) · FoundationDB's simulation architecture · TigerBeetle on protocol-aware DST · Phil Eaton's DST primer · WarpStream's DST writeup · `asatarin/testing-distributed-systems` · `ivanyu/awesome-deterministic-simulation-testing` · MIT 6.5840 · Petrov, *Database Internals* · Kleppmann, *Designing Data-Intensive Applications*
