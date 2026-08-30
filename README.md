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
| Phase | `P5 — sharding, split/merge, placement driver`, **complete** (see [docs/ROADMAP.md](docs/ROADMAP.md)) |
| Working | Runtime seam · hermeticity gate + negative control · deterministic scheduler on virtual time · **19 fault kinds** across network, disk, clock and process · invariant framework with cost classes · Elle-style consistency checker · serial reference model · **LSM engine: WAL, skiplist memtable, block-based SSTables with Bloom filters, MANIFEST/VersionSet, leveled compaction, block cache, crash recovery** · **Raft: pre-vote, CheckQuorum, pipelined replication, joint consensus, learners, log compaction, chunked snapshot install, leases, ReadIndex, leadership transfer** · **MVCC: inverted-timestamp versions, snapshot reads, write intents, wound-wait, deadlock detection, safepoint-driven GC, single-node transactions at snapshot isolation** · **sharding: MultiRaft with one Raft group per range, a Raft-replicated placement driver, atomic split and merge, range leases, a two-level meta index, a client range cache with generation invalidation, replica rebalancing and range quiescence** · execution digest · causal trace |
| Next | P6: distributed transactions -- Percolator/SI, SSI, and commit-wait |
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

seeded durability bug A (ack pre-fsync)  167/180 crashing seeds (workload + invariants)
seeded durability bug B (no dir fsync)   180/180 crashing seeds (workload + invariants)

checker soundness (INV-SIM-03) ....... 90/90 known-bad histories flagged
  correctly classified ............... 90/90 across 9 anomaly classes
checker precision (INV-SIM-04) ....... 500/500 serial histories accepted
checker mutation score ............... 13/13 non-equivalent mutants caught
  (2 further mutants confirmed equivalent -- masked by BFS pruning
   and by Tarjan discarding single-node components)

-- P2, the storage engine --
crash cycles, 60 seeds ............... 13,116 acknowledged writes, 0 lost
  deleted keys resurrected ........... 0
  orphaned files after recovery ...... 0 seeds
corruption (bit rot before crash) .... detected in 31/31 seeds
  bytes served that nobody wrote ..... 0        (INV-LSM-11)
ENOSPC + EIO sweep ................... 5,193 writes acknowledged, 0 lost
storage engine hermeticity ........... clean (141 strong undefined symbols)
seeded storage bugs .................. 10/10 caught (P2 required 8)
  via DurabilityOptions .............. 4/4
  no fsync before ack ................ 30/31 crashing seeds
  no MANIFEST fsync .................. 21/31
  no directory fsync ................. 31/31
  SSTable published before durable ... 22/31
  via source mutation ................ 6/6
  WAL salvages past a bad checksum ... caught
  tombstones dropped above bottom .... caught
  orphan sweep deletes live files .... caught
  Bloom probe count off by one ....... caught
  key comparator inverts versions .... caught
  block checksum never verified ...... caught
isolation-level discrimination ....... write skew accepted at SI, rejected at
                                       serializable; real-time violations
                                       accepted at SER, rejected at strict
cycle witnesses ...................... minimal (2 txns) even for 8-txn SCCs

-- P3, consensus --
safety, 12 seeds under full faults ... 12/12 clean -- no INV-RAFT-* violation,
                                       no lost acked write, no stale read
liveness after healing ............... leader elected in 12/12 runs
  time to a leader ................... p50 0ms, p90 495ms, max 1,195ms
client work per 12 seeds ............. 1,259 writes acknowledged, 451 linearizable reads
membership change during the workload  ~117 joint-consensus transitions
simulated node-time .................. ~1h20m per 12 seeds
INV-RAFT-* armed ..................... 16 (14 at tick class, 1 at epoch, 1 client-side)
seeded consensus bugs ................ 10/10 caught (9 by the sweep, 1 targeted)
  caught by an internal invariant .... 9/10   <- invisible at the client API
  reply before fsync ................. 7/7 seeds (INV-RAFT-09)
  learner counted in a quorum ........ 7/7 seeds (INV-RAFT-15)
  lease counted in ticks ............. 7/7 seeds (INV-RAFT-13, pause scenario)
  joint consensus exits on append .... 5/7 seeds (INV-RAFT-12)
  term and vote unsynced ............. 4/7 seeds (INV-RAFT-06, INV-RAFT-08)
  commit across terms (Figure 8) ..... 3/7 seeds (INV-RAFT-10, needs BUGGIFY)
  snapshot without truncation ........ 3/7 seeds (INV-RAFT-11)
  log unsynced ....................... 3/7 seeds (INV-RAFT-08, INV-RAFT-09)
  vote without the log restriction ... 2/7 seeds (INV-RAFT-04, INV-RAFT-05)
  append without the prev-term check . constructed case; shown non-equivalent
                                       rather than reported as a gap
pause longer than the lease .......... wall-clock lease safe; tick-counted lease
                                       caught by INV-RAFT-13

-- P4, MVCC and single-node transactions --
safety, 20 seeds under full faults ... 20/20 clean -- no INV-MVCC-* violation, no
                                       version lost that a live snapshot could
                                       still resolve
transactions ......................... 634 committed; 734 versions collected
long readers across GC passes ........ 4,060 re-reads, 6,973 versions audited
                                       against a model of every version committed
deadlock ............................. 11 wounded, 0 wait-for cycles, 0 stalls
INV-MVCC-* armed ..................... 8 (5 audited through the workload's
                                       auditor, 3 evaluated live)
isolation level, confirmed ........... one write-skew history: VALID at snapshot
                                       isolation with no G1c, INVALID at
                                       serializable with the cycle reported
                                       (T1 -rw-> T2 -rw-> T1)
seeded MVCC bugs ..................... 2/2 must-detect caught; control silent;
                                       1 classified equivalent with an argument
  gc drops the boundary version ...... 10/11 seeds (INV-MVCC-01, 9/11 API-visible)
  gc ignores the safepoint ........... 11/11 seeds (INV-MVCC-02, API-invisible)
  reads ignore intents ............... equivalent at SI under one monotonic
                                       clock; expected to become detectable in P6
sweep runtime ........................ 0.4s for 20 seeds
defects P4 found *below* P4 .......... 4 -- three in the LSM, one in the simulator

-- P5, sharding, split/merge and the placement driver --
safety, 40 seeds under full faults ... 40/40 clean -- no INV-SHARD-* violation,
                                       no account lost, the total conserved on
                                       every seed, no acknowledged transfer that
                                       the cluster later forgot
topology churn during the workload ... 563 splits, 552 merges, 73 replica
                                       changes, 1,367 leadership transfers to
                                       colocate a merge
Raft groups created / destroyed ...... 1,860 / 1,382, mid-run, under faults
transactions over a moving boundary .. 21,377 sent believing one range covered
                                       both accounts; 18,602 rejected because
                                       the topology had moved underneath them;
                                       0 applied partially
client work per 40 seeds ............. 2,342 transfers acknowledged, 1,076 lease
                                       reads
simulated node-time .................. 2h43m per 40 seeds
INV-SHARD-* armed .................... 9, plus 1 client-visible check
MultiRaft heartbeat coalescing ....... 735,397 heartbeats in 570,903 messages;
                                       13,092 ticks skipped on quiesced ranges
determinism with groups created and
  destroyed mid-run .................. 4/4 seeds reproduce exactly
clock bound, measured every tick ..... 24/40 seeds put a node's clock outside the
                                       bound its own configuration declared
                                       (worst 17.2s against 248ms); lease
                                       findings on those runs are classified
seeded sharding bugs ................. 6/6 must-detect caught; control silent;
                                       1 classified equivalent with an argument
  generation not bumped .............. 20/20 seeds (INV-SHARD-05, 4/20 API)
  voter added before catch-up ........ 18/20 seeds (INV-SHARD-06, 2/18 API)
  quiesce over a lagging replica ..... 15/20 seeds (INV-SHARD-08, API-invisible)
  split in two applies ............... 14/20 seeds (INV-SHARD-01, API-invisible)
  lease granted without waiting ...... 14/20 seeds (INV-SHARD-04, API-invisible)
  merge without colocation ........... 2/20 seeds (INV-SHARD-03) -- weakly caught
                                       by the sweep and squarely by the unit test
  stale route served ................. equivalent here: the span check already
                                       rejects every key the generation check
                                       would. Expected to become detectable in
                                       P6, where a follower can serve a key at a
                                       closed timestamp it no longer owns
sweep runtime ........................ ~60s for 40 seeds

bugs found (S0/S1/S2/S3/S4) .......... 15/12/6/0/13   (see BUGS.md; two open)
BUGGIFY site activation coverage ..... 1 site in the core (raft/send_append);
                                       makes the Figure-8 window reachable in
                                       tens of seeds instead of thousands
TLA+ trace-validation conformance .... (pending P7)
YCSB-A throughput / p99 .............. (pending P9)
TPC-C tpmC / p99 ..................... (pending P9)
```

Measured on the ping-pong, replicated-counter, key-value, replicated-KV,
MVCC-transaction and sharded-bank workloads. The transaction layer is
single-node and single-range; distributed transactions arrive in P6.

The shape of the ledger changed with P3. Of twenty-two entries, ten are now
genuine protocol or storage defects rather than harness problems, and the
consensus layer produced the project's first S0s: a quorum computed as two of
four voters, a retried append that duplicated a run of indices and cost a node
its committed log, and a crash inside a rename window that lost an entire log
file. Five of the thirteen fixed P3 bugs were invisible at the client API and
were found only by an internal invariant, which is the column the whole
technique is an argument for.

Four of the five storage bugs were the same mistake in different clothing: a
write path that is not idempotent under retry, and a size change that is not
atomic under crash. That is a more useful finding than any of them individually.

P4 repeated the pattern one level up, and the more interesting result is *where*
its findings landed. Four of the defects the MVCC sweep produced were not in
MVCC: three in the storage engine and one in the simulator, all of them past two
P2 suites and a 40-seed Raft sweep. They share a single shape -- a value read
before a `co_await` and used after it, a pointer handed out by a cache and held
across a suspension, a long operation that installs shared state and then
suspends without excluding a second one. None of them look like concurrency bugs
on the page; there is no lock to forget and no thread in sight. The MVCC layer
found them because it is the first thing in the tree that writes a key and reads
it straight back from several coroutines at once, which is the argument for
stacking layers rather than testing each in isolation.

P5 is where the bug density spiked, exactly as the roadmap predicted it would.
Seventeen ledger rows from one phase, six of them S0 -- data inside a range that
does not claim it, data in two ranges at once, and twelve accounts that existed
nowhere at all. Every one of those left the cluster answering every request it
was asked, correctly, for as long as anyone was looking.

Four of the seventeen were in the test harness rather than the system, and three
of *those* manufactured findings out of nothing: an audit that compared an
unbounded key range as a string and reported six accounts missing from a cluster
that had lost none; two checks that used a state machine's applied index as
though it were the log's, and so reported a divergence between two nodes that
were both perfectly correct. A checker over a topology that changes several
times a second turns out to be about as likely to be wrong as the topology is,
and the phase's own discipline is that a manufactured finding is written down
with the same seriousness as a real one.

Three more were the same mistake wearing different clothes, and it is the
sharding equivalent of P3's durability lesson: **reading two halves of one fact
from two different places**. A merge trigger that took its span from the
topology and its data from the machine. A lease margin that allowed one clock
bound where a handover between two clocks needs two. A placement timer that
subtracted a timestamp stamped on one node from the clock of another. Each is a
one-line fix and none of them is visible in a code review that does not ask
where each value came from.

Two findings are open, and both are filed with their evidence, their ruled-out
hypotheses and the next measurement to take rather than guessed at. ANV-0032 is a
learner that stops converging after a heal on one seed in sixteen; no safety
invariant fires and no acknowledged write is at risk. ANV-0033 is the serious
one: sixteen bytes of environment variable change a seed's digest and its event
count, so something reads an uninitialised automatic variable and the schedule
diverges. Replay is the foundation everything else rests on, and a seed that
misbehaves inside a sweep should misbehave identically when replayed alone. The
instrument that localises it -- clang with MemorySanitizer -- is not available on
this toolchain, so the bug is characterised precisely and left open rather than
papered over. Both gates stay red on them.
See [docs/ROADMAP.md](docs/ROADMAP.md).
<!-- END GENERATED RESULTS -->

---

## Repository layout

```
anvil/
  core/           # hermetic: no syscalls, no clock, no threads. The state machines.
    runtime/      #   Runtime interface: clock, random, net, disk, spawn, timers
    lsm/          #   format, WAL, memtable, sstable, version/manifest,
                  #   compaction, block cache, db  -- P2, gated as hermetic
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
| [CONTEXT.md](CONTEXT.md) | **Start here.** Engineering context: architecture, file map, APIs, conventions, current state, gotchas. Enough to make a correct change without reading the codebase. |
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
