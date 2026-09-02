# Anvil — Phase Plan

**Calendar:** 36 weeks for a team of four at ~15–20 h/week each (≈ 2,400 engineer-hours). A full-time team of four compresses this to 16–18 weeks. Solo at 20 h/week, expect T0 + parts of T1 in 36 weeks; that is still a strong outcome, but plan the tiering explicitly rather than discovering it in week 30.

**Sequencing principle, and the single most important correction to the original plan:** the original scope listed the simulator as item 6. It must be item 1. Determinism cannot be retrofitted — it is a property of every line of I/O, timing, and container-iteration code in the system. Building the storage engine first and "adding the simulator later" is how this project fails. **The simulator is built before there is anything to simulate**, and the first thing it simulates is a toy ping-pong protocol.

**Second correction:** the reference model and the consistency checker land in P1, not P7. Every layer after that arrives with an oracle already waiting for it. A layer built without an oracle is a layer whose bugs you will find in month six.

---

## Cadence

| Ritual | Frequency | Output |
|---|---|---|
| Interface review | end of P0, then on change | Frozen `Runtime` + layer interfaces |
| Nightly seed fleet | every night from P1 | `artifacts/<date>/`: seeds run, failures, digests, coverage |
| Bug triage | weekly | New rows in [BUGS.md](../BUGS.md), each with a pinned corpus seed |
| Metrics refresh | weekly, automated | `tools/report.py` rewrites the Results block in [README.md](../README.md) |
| Phase exit review | per phase | Written go/no-go against the exit criteria below |
| Seeded-mutation drill | per phase | Deliberate bugs injected; detection rate recorded |

Rule: **a phase does not exit on "the code is written." It exits on the measurable criteria below.**

---

## P0 — Foundations and the determinism discipline · Weeks 1–2

**Goal.** Make it structurally impossible to write nondeterministic code without CI noticing.

**Deliverables**
- Repo skeleton per [SCOPE.md §Repository layout](../README.md#repository-layout); CMake + Ninja; C++20; ASan/UBSan/TSan build variants; `ccache`.
- The `Runtime` interface, frozen ([SCOPE.md §4](SCOPE.md#4-the-runtime-seam)).
- `SimRuntime` v0: virtual clock, seeded PRNG (SplitMix64/PCG), coroutine task scheduler, no faults yet.
- `ProdRuntime` v0: `CLOCK_MONOTONIC`, blocking sockets, blocking file I/O. Deliberately naive — its job at this stage is to prove the seam is real, not to be fast.
- `tools/hermetic_check.py`: symbol-denylist scan over `libanvil_core.a`. Wired as a CI gate.
- Custom `clang-tidy` checks: `anvil-no-unordered-iteration`, `anvil-no-wallclock`, `anvil-no-float-in-control-flow`, `anvil-no-raw-thread`.
- Execution digest: 128-bit rolling hash over every scheduling decision and message delivery.
- **The determinism gate**: a toy two-node ping-pong protocol runs under `SimRuntime`; CI asserts the digest is identical across (a) two runs, (b) gcc and clang, (c) Linux x86-64 and macOS arm64.
- `BUGGIFY` macro and site registry, compiled out in `ProdRuntime`.
- Bug ledger, invariant catalogue, and `test/corpus/` created — empty but live.

**Exit criteria**
1. `hermetic_check.py` passes and provably fails when a `clock_gettime` call is added (a negative test, committed).
2. Ping-pong digest identical across two compilers and two architectures, 1,000 seeds.
3. A deliberately-introduced `std::unordered_map` iteration dependency is caught by the lint check.
4. `anvil-sim --seed X` and `anvil-sim --seed X --replay` produce byte-identical traces.

**Risk.** Cross-architecture determinism is the hard one; `long double`, `size_t` width, and library container implementations differ. Mitigate by fixing integer widths everywhere, banning float, and vendoring the ordered containers used in decision paths.

---

## P1 — Simulator core, reference model, and the first oracle · Weeks 3–6

**Goal.** A simulator that can already break things, and something to check against.

**Stream A (simulator)**
- Network model: latency distributions, reorder, drop, duplicate, symmetric and **asymmetric** partitions, flapping links, bandwidth caps, connection reset, half-open connections.
- Disk model: page cache + `fsync` semantics, torn writes at sector granularity, unsynced-data resolution on crash (old / new / torn, seed-chosen), bit rot, `ENOSPC`/`EIO`, latency spikes, directory-entry persistence modelled separately from file content.
- Clock model: per-node offset, drift, jumps, freezes, NTP step; adversarial-within-bound mode; a mode that violates the declared bound on purpose.
- Process model: crash, restart, clean shutdown, **pause/resume**.
- Structured trace log with causality edges; `anvil-trace` CLI to query it.

**Stream D (oracle)**
- Reference model: serial `std::map` + transaction log. No concurrency, no cleverness, deliberately slow.
- Invariant framework: registration, numbering, cost classes (`TICK`/`EPOCH`/`COMMIT`/`QUIESCE`/`OFFLINE`), and a violation reporter that dumps the causal trace.
- History recorder: every client operation with invocation/completion virtual timestamps, including `Unknown` outcomes.
- Elle-style checker v0: register workload, `ww`/`wr` edges, G0/G1 detection.

**Streams B & C** begin their layers against the frozen interfaces (WAL + memtable; Raft election + log replication skeleton).

**Exit criteria**
1. A demo distributed protocol (a toy 3-node replicated counter) runs 10,000 seeds under full fault injection; every failure replays exactly from its seed.
2. Simulation speedup ≥ 500× real time on one core for the toy protocol (report the real number; the metric matters more than the target).
3. The Elle-style checker flags 100% of a 50-history corpus of hand-injected anomalies and accepts 100% of 500 reference-model histories.
4. Disk model negative test: a deliberately unsynced WAL write is *proven* lost on simulated crash.

**Risk.** Scope creep in the fault models. Timebox each model to what the invariants actually need; add exotic faults when a real bug motivates them.

---

## P2 — LSM storage engine and storage-level DST · Weeks 7–10

**Goal.** A crash-consistent single-node engine, hammered.

**Deliverables (Stream B, with A supporting)**
- WAL: CRC32C records, group commit, tail truncation at first invalid record.
- Skiplist memtable with arena allocation; immutable memtable queue; flush scheduler.
- SSTable: data blocks, restart points, prefix compression, block index, Bloom **and** ribbon filters, per-block checksums, LZ4/Zstd.
- Block cache (S3-FIFO) + index/filter cache.
- `MANIFEST`/`VersionSet`: atomic version edits, reference-counted file lifetimes, orphan detection.
- Leveled and tiered compaction; priority scoring; deterministic sub-compaction.
- Forward/reverse/snapshot iterators.
- Full crash recovery.

**The DST work that makes this different from every other LevelDB clone**
- **Crash at every fsync boundary.** For a workload of N durability points, enumerate crashes at each and verify recovered state equals the model's state at the last acked commit.
- **Torn-write matrix.** Every WAL and SSTable write is subjected to sector-granularity tearing.
- **Corruption injection.** Bit rot in data blocks, index blocks, filter blocks, the MANIFEST, and the WAL. Every one must be *detected*, never silently served.
- **Space-leak detection.** After quiesce, no file on disk is unreferenced by the live version and no referenced file is missing.

**Exit criteria**
1. 500 simulated node-hours of crash/corruption/ENOSPC fault injection, zero violations of INV-LSM-*.
2. Zero acked writes lost across 100,000 injected crashes.
3. 100% of injected corruptions detected (0 silent-serve events).
4. Seeded-mutation drill: 8 deliberate storage bugs (off-by-one in recovery, missing fsync before MANIFEST rename, wrong sequence-number comparison, bloom filter false negative, compaction dropping a live version, …) — all 8 detected, each in < 10 simulated minutes.
5. Single-node benchmarks recorded under `ProdRuntime` as a baseline for later regression tracking.

**Risk.** The engine is the biggest single chunk of code. It is also the best-documented, so it is the right place to move fast. Do not gold-plate compaction; correctness first, tuning in P9.

---

## P3 — Raft, with protocol-aware invariants · Weeks 11–14

**Status: complete.** All five deliverables landed, sixteen `INV-RAFT-*` are
armed, and the drill catches 9 of 10 planted bugs from the sweep plus the tenth
from a constructed case. Thirteen real bugs were found and fixed on the way
(ANV-0012..ANV-0024), five of them invisible at the client API and three of them
S0. See [CONTEXT.md §11](../CONTEXT.md) for the decisions and
[BUGS.md](../BUGS.md) for the findings.

**Goal.** Consensus that is checked from the inside.

**Deliverables (Stream C)**
- Election with randomised timeouts, **pre-vote**, `CheckQuorum`; term and vote durably persisted *before* responding.
- Log replication with pipelining, batching, conflict backtracking, and commit-index advancement restricted to entries from the current term (the classic Figure-8 hazard).
- Log compaction and chunked snapshot install with flow control.
- **Joint-consensus** membership change; learners with catch-up tracking.
- Leader lease + `ReadIndex` linearizable reads; leadership transfer.

**Deliverables (Stream D, in parallel)**
- All twelve `INV-RAFT-*` invariants armed at `TICK` cost class, evaluated over the *global* state the simulator can see: every node's log, term, vote, commit index, and applied state.
- A "god's-eye" checker that maintains the union of all committed entries across all history and verifies Leader Completeness across term boundaries — something no client-facing test can do.

**Exit criteria**
1. 1,000 simulated node-hours across partitions, clock skew, crashes, pauses, and continuous membership churn: zero `INV-RAFT-*` violations.
2. Liveness: under eventual synchrony (faults heal at T), a leader is elected within a bounded simulated interval in 100% of 5,000 runs. Report the distribution, not just the max.
3. Seeded-mutation drill: 10 deliberate Raft bugs — commit-index advanced on a stale term, vote not persisted before reply, `AppendEntries` accepted with a mismatched previous term, snapshot installed without truncating the log, joint consensus exiting early, pre-vote skipped — all 10 detected, and for each, record **whether it was visible at the client API**. This table is the empirical core of the protocol-aware DST claim.
4. A pause-based lease violation (node frozen for longer than its lease, then resumed) is either safe or produces a logged bug.

**Risk.** Membership change is where implementations quietly cheat. Do joint consensus properly; the one-at-a-time shortcut has a known unsafe case and skipping it costs you the interview question.

---

## P4 — MVCC, snapshot isolation, single-node transactions · Weeks 15–17

**Goal.** Versions, snapshots, locks, and garbage collection that never eats a live read.

**Deliverables**
- Inverted-timestamp key encoding; separate lock/intent space.
- Snapshot reads; uncertainty-interval reads.
- Lock table with wait queues, wound-wait by start timestamp, local wait-for-graph deadlock detection.
- Version GC executed inside compaction, driven by a safepoint = `min(active snapshots, closed timestamp, oldest live txn)`.
- SSI conflict tracking primitives (read spans, write sets) — the mechanism, ahead of the distributed engine that uses it.

**Exit criteria**
1. `INV-MVCC-*` armed; 300 simulated node-hours with aggressive GC and long-running readers, zero violations.
2. A long reader spanning many compactions never observes a missing version (a dedicated adversarial workload: readers deliberately hold snapshots across the GC horizon).
3. Deadlock workload: 100% of injected cycles resolved within the bound; zero false victim selection under `wound-wait` ordering.
4. Elle-style checker validates single-node histories at SI: shows no G1c, and **does** exhibit write skew, confirming the level is correctly SI and the checker is correctly not over-reporting.

**Risk.** GC safepoints are the classic silent-corruption source. Over-invest here; the invariant is cheap and the bug is catastrophic.

**Status: complete.** All four exit criteria met. `anvil_mvcc_test` covers the
mechanism; `anvil_mvcc_faults` covers the criteria and runs the whole 20-seed
sweep in 0.4s.

- (1) 8 `INV-MVCC-*` armed, 20/20 seeds clean, 6,973 versions audited against a
  god's-eye model of every version ever committed.
- (2) Long readers hold snapshots deliberately across GC passes: 4,060 re-reads,
  zero versions lost. The mutant that drops the boundary version is caught
  10/11 seeds, 9 of them visible at the API.
- (3) 11 transactions wounded, 0 wait-for cycles, 0 stalls.
- (4) One write-skew history, VALID at snapshot isolation with no G1c, and
  INVALID at serializable with the cycle reported: `T1 -rw-> T2 -rw-> T1`.

**Deferred, and noted rather than pretended.**
- Version GC runs as its own collector task, not inside compaction. The safepoint
  logic and the invariants are identical either way; folding it into compaction
  is a scheduling change, and doing it inside the LSM's compaction would put
  transaction state into the storage engine, which the layering does not allow
  yet. P5 should revisit it when ranges arrive.
- MVCC crash recovery. Intents are durable, the transaction table is not.
  The P4 profile runs with process crashes disabled and says so in the source.
- The SSI certifier. Read spans and write sets are recorded; the engine that
  turns them into aborts needs distributed conflict tracking and belongs in P6.

**What it found below itself.** Three storage-engine defects (ANV-0025, 0026,
0027) and one simulator defect (ANV-0028), none of them in P4. The MVCC workload
is the first thing in the tree that writes a key and reads it straight back from
several coroutines at once.

---

## P5 — Sharding, split/merge, placement driver · Weeks 18–21

**Goal.** Many ranges, moving under load, with the routing metadata always coherent.

**Deliverables**
- Range descriptors with generations; two-level meta index stored in ranges.
- Atomic split (a transaction over meta + both descriptors) and atomic merge with lease colocation and quiescence.
- Placement driver as its own Raft group: size and load splits, replica and lease rebalancing, dead-node replacement with grace period.
- Client range cache with generation-based invalidation and `RangeKeyMismatch` retry.
- MultiRaft heartbeat coalescing (T2, attempt here).

**Exit criteria**
1. `INV-SHARD-*` armed, including the coverage invariant — at every tick, range descriptors tile the key space exactly once, with no gap and no overlap, observable atomically.
2. A **chaos-admin workload**: continuous concurrent splits, merges, rebalances, and membership changes running *during* a transactional workload for 500 simulated node-hours. Zero coverage violations, zero lost writes.
3. Split/merge racing a cross-range transaction over the split point is exercised at least 10,000 times, with no anomaly.
4. Seeded-mutation drill: 6 deliberate sharding bugs (stale range cache accepted, merge without lease colocation, split committing meta before descriptor, generation not bumped) — all detected.

**Risk.** This is the phase where the bug density is highest and the invariants are hardest to state. Budget for the coverage invariant to be expensive; run it at `EPOCH` cost class if `TICK` is too slow, and always at `QUIESCE`.

**Status: complete.** All four exit criteria met. `anvil_shard_test` covers the
mechanism; `anvil_shard_faults` covers the criteria and runs 40 seeds of the
chaos-admin sweep in about a minute.

- (1) 9 `INV-SHARD-*` armed plus one client-visible check; 40/40 seeds clean, no
  account lost, the total conserved on every seed. Coverage runs at `EPOCH` and
  is evaluated on every live node's own applied view, not just the leader's —
  which is what makes the non-atomic-split mutation detectable.
- (2) The chaos-admin workload is the default profile, not an option: split and
  merge thresholds that deliberately overlap, so the topology never settles.
  Over 40 seeds it produced 563 splits, 552 merges, 73 replica changes, 1,367
  leadership transfers for merge colocation, and 1,860 Raft groups created and
  1,382 destroyed while the workload ran. 2h43m of simulated node-time.
- (3) 21,377 transfers were sent believing one range covered both accounts;
  18,602 were rejected because the topology had moved underneath them. Not one
  applied partially — the split is a trigger in the range's own log, so a
  transfer either precedes it and applies under the old descriptor or follows it
  and is rejected against the new one.
- (4) Seven deliberate bugs: 6 must-detect, all caught; one control, silent; one
  classified equivalent with a written argument (the generation check is
  redundant with the span check in this configuration — it becomes real in P6,
  where a range can serve a key at a closed timestamp it no longer owns).

**The risk prediction was right.** Seventeen ledger rows (ANV-0034..ANV-0050),
six of them S0, against thirteen in P3 and four in P4. Four were in the test
harness rather than the system, and three of *those* manufactured findings out
of nothing — the phase's own lesson is that a checker over a topology that
changes several times a second is as likely to be wrong as the topology is.

**Deferred, and noted rather than pretended.**
- The second level of the meta index is a logical bucket, not its own Raft
  group. The client pays for both lookups and both are invalidated by
  generation; what is missing is a meta range that can itself split, which needs
  more than one meta group to be worth anything.
- A range's applied state is durable through its Raft log rather than through
  the LSM, so a split moves data as a log entry rather than as a metadata edit.
  That is the single largest departure from how a production store does this,
  and it is what makes the split payload a thing that has to be held, handed
  over and confirmed (ANV-0049 is entirely downstream of it).
- MVCC crash recovery, carried over from P4 and still open: the transaction
  table is not durable. The shard layer's ranges are a bank rather than a
  transactional store, so P5 did not need it; P6 does.
- MultiRaft heartbeat coalescing landed (735,397 heartbeats in 570,903
  messages), but the ratio is modest because these clusters have five ranges,
  not five thousand. The mechanism is real and the number is honest about what
  it is worth at this scale.

---

## P6 — Distributed transactions: three engines · Weeks 22–25

**Status: in progress, core money-loss finding fixed, a Raft-layer question
open.** The mechanism, the checker instrumentation and the fault harness exist
and mostly pass. Eight real defects have now been found and fixed getting the
sweep to run at all and then to run clean without fault injection: a tombstone
indistinguishable from an empty value, a checker false-positive on the
oracle's own recovery, a record whose empty key list made it invisible to
every split and merge, a transaction id that did not survive a coordinator
restart, a blocked reader that never resolved the intent it found, the
checker's own record identity being keyed by node instead of by node-and-
range, a range's reply to a transactional command being matched to whichever
client's request sorted first by id rather than the one that actually caused
it (money went missing under contention from two coordinators, no fault
needed), and a merge trigger that carried only the bank half of the subsumed
range's state, discarding every transaction the merged-away range was holding.
The fault-free money-loss finding this section used to describe as open is
now fixed and confirmed by direct minimal-repro experiment.

Since then both remaining "Raft-layer" suspicions turned out to be checkers
measuring the wrong thing, and fixing them is what moved the phase. INV-TXN-09
compared a replica's oracle high-water mark across a crash in a window where
`applied >= commit` is true of a node that is genuinely behind; INV-SHARD-CLIENT
([ANV-0051](../BUGS.md), the first ledger row of this pass) compared concurrent
reads as though they were sequential. The second mattered far beyond its own
false positive: the simulator halts at the first violation, so that check had
been capping every run of P5 seed 19 at tick 4395 since the phase was called
done. With it gone the seed runs on and exposes a real convergence failure --
a range no replica ever initialises, whose accounts the audit cannot reach.

Underneath that lid was a real S0: [ANV-0052](../BUGS.md), a range merged away
while holding the only copy of a child range's data, leaving the child
permanently uninitialisable and its accounts gone. `pending_split_` was a
single slot that neither survived a second split nor travelled with a merge;
it is now a map carried in `encode_span`. **`shard_faults` is 20/20 green
again.** A second checker gap ([ANV-0053](../BUGS.md)) accounted for the rest
of the noise: the P6 audit never walked pending-split payloads the way P5's
always had, so every split caught in flight read as missing money.

What is left in P6 is a short list of real findings rather than a fog: two
small conservation shortfalls on converged runs, one seed where a record
leaves a terminal state (INV-TXN-02), and a list-append seed that loses
acknowledged elements. Green today: `shard_faults` 20/20, mechanism suite
30/30, determinism 3/3, serializable and strict-serializable 4/4
checker-clean, INV-TXN-09 silent. See [CONTEXT.md §14](../CONTEXT.md).

**Goal.** The guarantee matrix from [SCOPE.md §2](SCOPE.md#2-guarantee-matrix), all three levels, one interface.

**Deliverables**
- **Percolator / SI**: prewrite, primary lock, commit, lazy secondary resolution, TTL and lock resolution by a blocked reader, orphaned-lock recovery.
- **SSI / strict serializable**: transaction records, write intents, intent resolution, timestamp push, read-refresh spans, uncertainty restarts.
- **Commit-wait / external consistency**: `ClockOracle` with simulator-controlled uncertainty width, commit-wait before ack, closed timestamps for follower reads.
- Timestamp allocation: replicated TSO with batching and monotonic recovery across failover, plus decentralised HLC mode.
- Parallel commit (staging records), 1PC fast path, distributed deadlock detection, transaction heartbeats and expiry.
- `watch` change feed with per-range ordering and resolved timestamps.

**Exit criteria**
1. `INV-TXN-*` armed; 1,000 simulated node-hours per engine under full fault injection.
2. Checker verdicts match declared levels exactly: SI shows write skew and no G1c; SSI and StrictSerializable show an acyclic DSG; StrictSerializable additionally satisfies real-time precedence.
3. **The clock-bound violation experiment.** Run StrictSerializable with actual skew exceeding the declared uncertainty bound. Characterise precisely what breaks and at what skew. Write it up. This is a genuinely interesting result and nobody expects a student to have run it.
4. TSO failover: 10,000 forced failovers, timestamp never regresses.
5. Bank-transfer invariant workload: total balance conserved across 10^7 transfers under continuous faults.
6. Seeded-mutation drill: 12 deliberate transaction bugs across all three engines — all detected, each classified by API visibility.

**Risk.** Three engines is a lot. If a tier cut is needed, SI + SSI ship and commit-wait becomes a stretch — but say so here, dated, rather than quietly shipping "strictly serializable" that isn't.

---

## P7 — Verification depth: checker, TLA+, trace validation, DPOR · Weeks 26–29

**Status: started. Exit criterion 1 is met.** `anvil_checker_mutation`
(`test/checker_mutation.cc`, ctest name `checker.mutation`) reports a **100%
mutation score — 200/200 anomalous histories detected and 200/200 correctly
named** across nine anomaly classes, and **zero false positives over 10,000
reference-model histories** (200,000 transactions) checked at all five
isolation levels, plus 120/120 discrimination pairs clean where the level
permits the anomaly. This was taken first on purpose: the three findings
immediately before it were all checker bugs, two of which
([ANV-0051](../BUGS.md), [ANV-0053](../BUGS.md)) returned confidently wrong
verdicts for a whole phase. Remaining deliverables below are untouched.

**Goal.** Prove the tests themselves are worth trusting. This phase is what separates the project from "I injected some faults."

**Deliverables**
- Elle-style checker, complete: list-append and register workloads, version-order recovery, DSG with `ww`/`wr`/`rw` edges, Tarjan SCC, full Adya classification (G0, G1a, G1b, G1c, G-single, G2-item, G2), minimal-cycle witness extraction and rendering, indeterminate (`Unknown`) handling.
- **Cross-validation against Jepsen's Elle.** Same histories, both checkers, any disagreement filed as a bug against whichever is wrong.
- **Checker mutation testing.** A corpus of ~200 synthetic anomalous histories; the checker must catch every one. Report the mutation score.
- TLA+ specs: the Raft variant including joint consensus, and the SSI commit protocol. TLC model-checked over small configurations, with both safety and liveness (temporal) properties.
- **Trace validation.** Export simulator traces in the spec's variable vocabulary and replay them against the TLA+ spec to confirm the implementation refines it. This is the step almost every "we wrote a TLA+ spec" project omits, and it is the one that means anything.
- **DPOR.** Exhaustive exploration of tiny configurations (3 nodes, 2 keys, 2 transactions, ≤ 6 in-flight messages). Exhaustive for the small, random for the large.
- Delta-debugging fault-schedule minimiser, converging a failing run to a minimal fault set.

**Exit criteria**
1. Checker mutation score = 100% on the anomaly corpus; zero false positives on 10,000 reference-model histories.
2. Zero unexplained disagreements with Jepsen Elle over 10,000 shared histories.
3. TLC finds no violations on the specified configurations; every trace-validation run conforms, or the divergence is filed as a bug.
4. DPOR exhaustively covers its configuration class with no violations, and the state count is reported.
5. The minimiser reduces a known failing run from ≥ 10 faults to ≤ 3 in under 5 minutes.

**Risk.** TLA+ has a real learning curve and trace validation more so. Start the spec in P3 as a side task rather than cold in P7.

---

## P8 — The bug hunt at scale · Weeks 30–32

**Goal.** Turn compute into ledger rows.

**Deliverables**
- Nightly seed fleet: a scheduler that distributes seeds across cores/machines, collects artifacts, deduplicates failures by invariant + minimised fault signature, and files candidate rows into [BUGS.md](../BUGS.md).
- **Coverage-guided seed selection**: branch coverage instrumentation, a seed corpus retained for new coverage, mutation of scheduler decision sequences rather than blind reseeding. Without this, most simulated hours re-explore the same interleavings.
- Swarm testing across the full configuration space of [SCOPE.md §5](SCOPE.md#5-the-configuration-space-what-swarm-testing-varies).
- **Mixed-version testing**: N and N−1 binaries in one cluster, rolling upgrade and rollback under fault injection.
- **The full seeded-mutation suite**: every deliberate bug from every phase, re-run as one report — detection rate, mean simulated-time-to-detect, and API visibility.
- Metrics automation: `tools/report.py` writing the README Results block.

**Exit criteria**
1. ≥ 20,000 simulated node-hours accumulated and reported honestly (this is a function of your compute; report the real number and the speedup factor).
2. BUGGIFY site activation coverage ≥ 95% across the fleet.
3. Branch coverage under simulation ≥ 85% for `core/`.
4. Every bug in the ledger has a pinned corpus seed that still reproduces on the commit before its fix, and passes on the commit after.
5. Mixed-version cluster survives 200 simulated node-hours of rolling upgrade/rollback with faults.
6. Full mutation report published: detection rate, MTTD, and the **API-visibility column** — the evidence for the protocol-aware claim.

---

## P9 — Production runtime, performance, Jepsen, and the writeup · Weeks 33–36

**Goal.** Prove the same code runs for real, measure it, and close the loop with an external checker.

**Deliverables**
- `ProdRuntime` for real: `io_uring` with `epoll` fallback, thread-per-core executor, real file I/O, length-prefixed binary wire protocol.
- 5-node Docker Compose deployment; a minimal metrics endpoint.
- Benchmarks: YCSB A–F, TPC-C subset, bank transfer. Throughput, p50/p99/p99.9, write/space/read amplification. Methodology committed alongside the numbers.
- **Deterministic performance simulator**: modelled disk and network latency in virtual time, so performance regressions are caught reproducibly in CI. Report simulated p99 next to real p99 and discuss the gap — that discussion is itself a strong signal.
- **Jepsen suite** against the real cluster with `tc`/`netem`. Expected outcome: nothing new. If Jepsen finds something, that is the highest-value row in the ledger, because it exposes a gap in the *fault model*, and fixing the fault model is worth more than fixing the bug.
- Writeup: a design document, a "what DST found" post with the mutation and API-visibility tables, and a talk-length deck.

**Exit criteria**
1. The complete DST suite passes with `ProdRuntime` swapped in for the non-timing-dependent tests, proving the seam is not a fiction.
2. Benchmarks reproducible from a committed script on a documented machine.
3. Jepsen runs clean, or its findings are in the ledger with the fault-model gap identified.
4. Performance regression gate live in CI on the deterministic performance simulator.
5. README Results block fully populated from real artifacts.

---

## Where this project actually fails

Six failure modes, in order of likelihood. Read these at every phase exit.

1. **Determinism is retrofitted.** Fatal. Mitigated by P0 and the hermetic CI gate, which must exist before any protocol code is written.
2. **The simulator becomes the project and the database never gets built.** Symptom: week 12, beautiful fault models, no Raft. Mitigated by the phase exit criteria being about the *database's* invariants, not the simulator's features.
3. **The invariants are vacuous.** A checker that never fires proves nothing. Mitigated by the seeded-mutation drill in every phase — the single most important process discipline in this plan, and the one that turns "I tested it" into "here is my measured detection rate."
4. **The bug ledger is written from memory in week 34.** Then the CV bullet is unverifiable, and any interviewer who asks for a seed exposes it. Mitigated by the ledger being a CI artifact from P1, with seeds pinned in `test/corpus/`.
5. **The guarantee claim outruns the implementation.** Saying "strictly serializable" over a Percolator/SI implementation. Mitigated by [SCOPE.md §2](SCOPE.md#2-guarantee-matrix) and by the checker enforcing the *declared* level, not a hoped-for one.
6. **Four workstreams silently diverge.** Mitigated by freezing the `Runtime` seam in week 2 and treating interface changes as reviewed events.

---

## What the CV bullets become — and what has to be true for each

Write these only when the corresponding artifact exists. Each bullet below is paired with the evidence an interviewer will ask for.

| Bullet | Evidence you must be able to produce in 60 seconds |
|---|---|
| "Built a sharded, strictly-serializable KV store in C++20: LSM engine, Raft, MVCC, and three transaction engines across 5 nodes" | `docs/SCOPE.md` guarantee matrix + the checker output showing an acyclic DSG with real-time edges |
| "Wrote a deterministic simulator over network, disk, and clock; seeded replay reproduced every failure in one run" | Run `anvil-sim --seed <any ledger seed>` live and watch it fail identically |
| "Injected partitions, clock skew, torn writes and process pauses over N simulated node-hours; found M correctness bugs, K of them invisible at the API boundary" | [BUGS.md](../BUGS.md), with the API-visibility column and per-bug seeds |
| "Built a protocol-aware invariant checker over ~60 numbered internal invariants, and validated it by seeded mutation: it detects X of Y deliberately injected protocol bugs" | [docs/INVARIANTS.md](INVARIANTS.md) + the mutation report |
| "Wrote an Elle-style transactional consistency checker from scratch and cross-validated it against Jepsen's Elle over 10K histories" | `checker/`, the mutation score, and the disagreement log |
| "Model-checked the consensus and commit protocols in TLA+ and validated real execution traces against the specs" | `spec/` + the trace-validation harness |
| "Sustained X txn/s at p99 Y ms with 3-way replication on a TPC-C-style workload" | The committed benchmark script, machine spec, and methodology |

The numbers in the original pitch — 14 bugs, 2,100 node-hours, 41K txn/s, p99 8.4 ms — are placeholders. They are **outputs of this plan, not inputs to it.** Fill them from `tools/report.py`. A fabricated number here is worse than no number, because the people you are targeting with this project are exactly the people who will ask for the seed.
