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
| Phase | `P7 — verification depth: checker, TLA+, trace validation, DPOR, minimiser`, **complete** (see [docs/ROADMAP.md](docs/ROADMAP.md)) |
| Working | Runtime seam · hermeticity gate + negative control · deterministic scheduler on virtual time · **19 fault kinds** across network, disk, clock and process · invariant framework with cost classes · Elle-style consistency checker · serial reference model · **LSM engine: WAL, skiplist memtable, block-based SSTables with Bloom filters, MANIFEST/VersionSet, leveled compaction, block cache, crash recovery** · **Raft: pre-vote, CheckQuorum, pipelined replication, joint consensus, learners, log compaction, chunked snapshot install, leases, ReadIndex, leadership transfer** · **MVCC: inverted-timestamp versions, snapshot reads, write intents, wound-wait, deadlock detection, safepoint-driven GC, single-node transactions at snapshot isolation** · **sharding: MultiRaft with one Raft group per range, a Raft-replicated placement driver, atomic split and merge, range leases, a two-level meta index, a client range cache with generation invalidation, replica rebalancing and range quiescence** · **distributed transactions: one coordinator over three engines (Percolator/SI, SSI with read refresh, Spanner-style commit-wait), write intents and transaction records, parallel commit, cross-range transactions over a topology that splits and merges underneath them, distributed deadlock detection** · **verification: a state-space model checker over the real Raft state machine with sleep-set partial-order reduction, TLA+ specifications of Raft-with-joint-consensus and the SSI commit protocol model-checked by TLC with graded negative controls, trace validation replaying real implementation runs against those specifications, delta-debugging fault minimisation, and cross-validation against Jepsen's Elle** · execution digest · causal trace |
| Next | P8: the bug hunt at scale — a nightly seed fleet that turns compute into ledger rows, deduplicating failures by invariant and minimised fault signature |
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

> **Every number here is produced by a binary in this tree, and the binary is named beside it.** `tools/report.py` — which would transcribe them from CI artifacts automatically — does not exist yet, so until it does they are copied in by hand from the runs listed in [CLAUDE.md](CLAUDE.md) §2. That is worth saying rather than leaving the previous "do not hand-edit" note in place while hand-editing it: a claim about how a number got here is still a claim.
>
> Nothing here counts until the ledger row and the seed behind it are in the repo.

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

-- P7, verification depth --
checker mutation, at scale ........... 200/200 anomalous histories detected
  correctly named .................... 200/200 across 9 anomaly classes
  false positives .................... 0 over 10,000 reference-model histories
                                       (200,000 transactions) at all 5 levels
  discrimination pairs ............... 120/120 clean where the level permits it

cross-validation vs Jepsen Elle ...... 10,000 shared histories
  verdicts identical ................. 10,000 / 10,000
  same anomaly class ................. 10,000 / 10,000
  unexplained disagreements .......... 0
  (two reporting differences are explained and normalised rather than hidden:
   Elle names G-single as G-single-item, and *throws* on a duplicate append
   where Anvil reports it as an anomaly -- both detect it, they disagree about
   whose fault it is)

state-space search over raft.h ....... 2,202,433 distinct states, complete
  transitions ........................ 5,545,749
  terminal states .................... 5,530 distinct, search depth 78
  invariant violations ............... 0
  class .............................. 3 voters, 2 proposals/node, 2 ticks/node,
                                       <= 4 messages in flight, FIFO links
  seeded mutations detected .......... 3/3 must-detect, each with a printed
                                       counterexample path; 5 further knobs
                                       classified equivalent-in-class with a
                                       written argument each
  figure 8 ........................... caught in 4,131 states via INV-RAFT-10;
                                       the same knob is silent across all
                                       2,202,433 states at three voters, which
                                       is a measurement of how large a
                                       configuration must be before the property
                                       stops being vacuous
partial-order reduction .............. 382/382 terminal states identical to the
                                       exhaustive search over the same class;
                                       128,400 edges pruned. Validated, not
                                       asserted -- and the validation is what
                                       found ANV-0063 and ANV-0064

TLA+ (spec/, tools/tlc.sh) ........... 9 configurations, each graded
  required clean ..................... Raft              8,237,782 distinct
                                       RaftJoint        19,818,587 distinct
                                       RaftFigure8Ok     1,684,735 distinct
                                       SsiSerializable      19,065 distinct
                                       SsiParallelSnapshot  19,065 distinct
  required to fail, on a named property
    RaftFigure8 ...................... Figure8Rule       (18,240 states)
    RaftNoJointCommit ................ LeaderCompleteness
    SsiSnapshot ...................... NoWriteSkew  -- write skew is *legal* at
                                       snapshot isolation, so this is what makes
                                       the serializable run mean something
    SsiParallel ...................... NoWriteSkew  -- and this one is ANV-0065

fault-schedule minimiser ............. 11 armed features -> 1 (process.crash)
  1-minimality ....................... verified, not assumed
  predicate runs ..................... 15, in 0.016s
  graded against a known cause ....... yes; a second failure on the same
                                       workload minimises to disk.bit_rot +
                                       process.crash, which is a different and
                                       equally correct answer
  premise check ...................... a passing configuration is rejected
                                       rather than "minimised" to nothing

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

-- P6, distributed transactions --
safety, 30 seeds under full faults ... 30/30 clean -- no INV-TXN-* violation,
                                       the bank's total conserved on every seed,
                                       no acknowledged element lost or
                                       duplicated
isolation, per level ................. snapshot 10/10, serializable 10/10,
                                       strict-serializable 10/10 seeds show a
                                       history the Elle checker calls clean at
                                       the level the engine claims
client work per 30 seeds ............. 145 committed, 949 aborted, 173
                                       cross-range transactions
simulated node-time .................. 1h52m per 30 seeds
determinism, coordinators and all .... 3/3 seeds reproduce exactly, with the
                                       oracle group and every range group
                                       created and destroyed mid-run
INV-TXN-* armed ...................... 7 (5 at tick class, 2 at quiesce)
seeded transaction bugs .............. 6/6 must-detect caught, each above its
                                       own cell's null control; 3 controls and
                                       5 null controls silent
  lost update ........................ 15/15 seeds (conservation, 15/15 API)
  no refresh on push ................. 4/15 (G2-item, write-skew cell)
  terminal status is not final ....... 4/15 (INV-TXN-02 + elements, 1/4 API)
  intents invisible to readers ....... 1/15 (INV-TXN-09)
  uncertain reads never restart ...... 1/15 (real-time violation)
  uncertainty never honoured ......... 1/15 (real-time violation)
  secondaries before primary ......... classified a control with an argument:
                                       the record is written before any
                                       prewrite whatever the flag says, so the
                                       window the knob claims to widen does not
                                       exist
commit-wait vs uncertainty restart ... shown redundant with each other by direct
                                       experiment: neither one removed alone is
                                       detectable (0/40 and 0/20); both removed
                                       gives real-time violations on 5/20 and
                                       3/20 seeds
sweep runtime ........................ ~100s for 30 seeds

bugs found (S0/S1/S2/S3/S4) .......... 20/13/10/2/20   (see BUGS.md; three open)
BUGGIFY site activation coverage ..... 1 site in the core (raft/send_append);
                                       makes the Figure-8 window reachable in
                                       tens of seeds instead of thousands
TLA+ model checking .................. 9/9 configurations behaved as specified
                                       (5 required clean, 4 required to fail on
                                       a named property), ~10 min total
TLA+ trace-validation conformance .... 16/16 runs of anvil::raft::RaftNode are
                                       permitted by spec/Raft.tla
  negative control ................... 12/16 runs of a deliberately broken
                                       implementation correctly rejected; the
                                       other 4 are seeds on which the mutation
                                       never had an opportunity to manifest,
                                       counted and named rather than hidden
  scope .............................. elections, replication and the commit
                                       rule. NOT heartbeats: the implementation
                                       clamps the commit index a heartbeat
                                       advertises at the sender, the spec at the
                                       receiver -- both sound, not the same
                                       mechanism, and reconciling them needs a
                                       matchIndex variable the spec does not
                                       have. That difference is itself a finding
                                       of this exercise
YCSB-A throughput / p99 .............. (pending P9)
TPC-C tpmC / p99 ..................... (pending P9)
```

Measured on the ping-pong, replicated-counter, key-value, replicated-KV,
MVCC-transaction, sharded-bank and distributed list-append workloads.

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

P6's twelve rows split cleanly in two, and the split is the phase's result. Two
are S0s in the engine: a commit timestamp drawn from a reserved batch, so that
first-committer-wins compared against an order that had nothing to do with when
the transactions ran (ANV-0058); and a heartbeat that narrowed a staging
record's key list to its own primary, after which parallel commit's "committed
iff every listed key carries its intent" answered yes about one key, before the
other intents were written and before the commit timestamp existed (ANV-0060).
Both were visible to a black-box client and neither was visible to the invariant
armed for it.

**The other seven were in the checker, and that is the finding.** A phase whose
system under test is a topology changing several times a second produces a
checker about as likely to be wrong as the system is, and every one of the seven
has the same shape: the checker knew about fewer places than the protocol uses.
A record lives on the primary's range and the audit asked the intent's range
(ANV-0056). A key lives on the range whose *newest* descriptor claims it and the
audit ranked claimants by an applied index, which numbers entries in one range's
own log and means nothing across two (ANV-0059). Data lives in a split payload
for the width of a handover, and neither the audit's payload branch nor
INV-TXN-01 resolved intents there (ANV-0059, ANV-0062). Written down with the
same seriousness as the S0s, because a checker that manufactures a pass is
strictly worse than one that manufactures a failure -- and one of these
manufactured a *pass*, in the form of a bank total fourteen units **over** what
it started with, from an audit that had learned to lose money and not yet
learned that finding it is the same defect.

The seeded-mutation drill is where that discipline paid for itself. It reported
6/7 and was actually detecting 2, and the null control that revealed it
(ANV-0057) was the first of four corrections, each of which generalises past
this project. A cell is a *configuration*, not a workload: two rows were
switching off a mechanism that a second mechanism made redundant, and no number
of seeds makes an equivalent mutant detectable -- commit-wait and the
uncertainty restart each cover for the other, shown by removing each alone (0/40
and 0/20) and then both (5/20 and 3/20). A mechanism has to be *reachable*
before a row about it means anything, and widening the declared clock bound
without widening the skew it is declared about produced 98 restarts and zero
information. A checker starved of observations reports VALID for the same reason
an empty history does: Elle recovers a key's version order from the reads that
saw it, and a sixteen-key run that commits ten transactions handed it a graph
with two edges in it. And a detection rate is a function of throughput, which is
how a client bug that never used the leader hint every reply carried (ANV-0061)
read for a whole phase as four separate detection gaps.

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
