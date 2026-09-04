# Anvil — Engineering Context

**Read this instead of the codebase.** It is the single onboarding document for a
new engineer or a fresh AI session. It should be enough to make a correct change
without opening more than two or three source files.

If you change architecture, add a layer, hit a non-obvious trap, or finish a
phase — **update this file in the same commit**. A context document that lags the
code is worse than none, because it is trusted.

- Last updated: **P8 in progress** (the bug hunt at scale), branch `main`
- Repo: `https://github.com/Tejas544/Distributed-KV.git`, branch `main`
- ~32,300 lines C++20 + Python tooling, 118 source files

---

## 1. What this is, in four sentences

A sharded, strictly-serializable distributed key-value store in C++20, whose
correctness is established by a deterministic simulator that hunts its own bugs.
The database (LSM engine, Raft, MVCC, distributed transactions, range sharding)
is the well-trodden part. The **simulator** is the point: it runs the whole
cluster on one thread, controls every source of nondeterminism, injects
adversarial faults, and asserts internal protocol invariants that are invisible
at the client API boundary. A failing run is reproduced from a single 64-bit
integer.

Full scope: [docs/SCOPE.md](docs/SCOPE.md). Phase plan: [docs/ROADMAP.md](docs/ROADMAP.md).
Invariant catalogue: [docs/INVARIANTS.md](docs/INVARIANTS.md). Findings: [BUGS.md](BUGS.md).

---

## 2. Build and run

CMake is the build system, but **it has never been executed on the development
machine** (no CMake installed there). Everything to date was verified by hand
with `g++`. Expect to fix something on the first real configure.

```bash
cmake --preset dev && cmake --build build/dev --parallel && ctest --test-dir build/dev --output-on-failure
```

Presets in `CMakePresets.json`: `dev` (gcc + ASan/UBSan + BUGGIFY), `fleet`
(optimised, BUGGIFY on), `digest-gcc` / `digest-clang` (the INV-SIM-01
cross-toolchain gate), `prod` (BUGGIFY compiled out), `tsan`.

### Manual build (what actually gets used day to day)

```bash
export PATH="/d/msys2/ucrt64/bin:$PATH"   # MANDATORY -- see gotcha 10.1
FLAGS="-std=c++20 -I. -DANVIL_ENABLE_BUGGIFY=1 -O2 -fno-threadsafe-statics -ffp-contract=off -fwrapv"
SRC=$(find anvil workloads -name '*.cc' ! -name sim_main.cc)
g++ $FLAGS $SRC test/lsm_crash.cc -o /tmp/lsm_crash && /tmp/lsm_crash 60
```

The suites, and what they cost:

| Binary | Argument | Runtime |
|---|---|---|
| `test/core_smoke.cc`, `test/*_test.cc` | none | under a second each |
| `test/sim_faults.cc` | seeds (60) | ~10s |
| `test/lsm_crash.cc` | seeds (40) | ~20s |
| `test/mvcc_faults.cc` | seeds (20) | <1s |
| `test/raft_faults.cc` | seeds (12) | ~30s |
| `test/shard_faults.cc` | seeds (24) | ~40s |

`anvil_raft_faults 16` is the pinned reproduction of ANV-0032 and is expected to
fail on seed 14. Everything else is expected to pass.

### The CLI

```bash
anvil-sim --seed 0x8f3a91c40d2e77b1 --workload counter --faults --verify
anvil-sim --sweep 2000                    # determinism sweep; CI compares the rollup across toolchains
anvil-sim --seed <s> --trace out.jsonl    # causal trace as JSONL
```

---

## 3. The one architectural rule

```
        anvil/core/**      pure state machines -- no syscalls, no wall clock,
                           no threads, no unseeded randomness
  ─────── Runtime seam ────────  (anvil/core/runtime/runtime.h, FROZEN)
   anvil/sim/**            SimRuntime: virtual clock, modelled net/disk/clock,
                           seeded PRNG, crash/pause, BUGGIFY
   anvil/prod/**           ProdRuntime: real clock, io_uring/epoll  (STILL A STUB)
```

Everything above the seam takes a `Runtime&` and cannot tell which
implementation it has. That is what makes a simulator-found bug a bug in
production code rather than in a test double — the difference between this
technique and mocking.

**`tools/hermetic_check.py` enforces it at link time**, by scanning the
strong-undefined symbols of `libanvil_core.a` and `libanvil_lsm.a` against a
tiered denylist in `tools/hermetic.toml`. It catches a `clock_gettime` arriving
transitively through a header five levels deep, which grep never would. A
deliberately non-hermetic archive (`test/hermetic/negative_wallclock.cc`) must be
*rejected* — a gate only ever observed to pass is indistinguishable from one that
does nothing.

---

## 4. File map

One line each. Sizes are a rough guide to where the complexity is.

### `anvil/core/` — hermetic, gated

| File | Lines | Purpose |
|---|---|---|
| `types.h` | 230 | `NodeId`/`Timestamp`/`Duration`/`Status`/`KeyRange`, strong id template. **No float anywhere.** |
| `random.h` | 173 | `DeterministicRandom` (xoshiro256\*\*). `fork(domain, instance)` gives independent substreams — see §6.2. |
| `digest.h` | 110 | 128-bit order-sensitive execution digest. Backs INV-SIM-01. |
| `buggify.h/.cc` | 198 | `ANVIL_BUGGIFY` rare-path injection; site identity is `hash(file, line)`, not registration order. |
| `runtime/task.h` | 164 | `Task<T>`: lazy coroutine with symmetric transfer. |
| `runtime/runtime.h` | 189 | **The frozen seam.** Clock, RNG, net, files, spawn, trace, panic. |

### `anvil/core/lsm/` — P2 storage engine, hermetic, gated

| File | Lines | Purpose |
|---|---|---|
| `format.h/.cc` | 258 | Explicit LE encoding, varints, CRC32C, internal keys. **Trailer sorts DESCENDING** so a seek lands on the newest version. |
| `wal.h/.cc` | 303 | `[crc32c][length][payload]`; CRC covers the length. Stops at first invalid record; distinguishes EIO from corruption. |
| `memtable.h/.cc` | 312 | Arena-backed skiplist; heights come from the run seed. |
| `sstable.h/.cc` | 796 | Blocks with restart points + prefix compression, index, Bloom filter, per-block CRC, LRU `BlockCache`. |
| `version.h/.cc` | 563 | `VersionEdit`/`Version`/`VersionSet`, MANIFEST as a checksummed edit log, `CURRENT` replaced via write→fsync→rename→fsync_dir. |
| `db.h/.cc` | 804 | Write path, flush, leveled compaction, read path, recovery, orphan sweep. |

### `anvil/core/raft/` — P3 consensus, hermetic, gated

| File | Lines | Purpose |
|---|---|---|
| `types.h` | 250 | Entries, `HardState`, `Snapshot`, `ConfChange`, messages, and **`Ready`**. Also every deliberate-bug flag, all defaulting to correct. |
| `config.h/.cc` | 290 | Membership and quorum arithmetic. Joint consensus needs a majority of *both* sets; learners are counted in nothing. `n/2` and not `(n-1)/2` — see ANV-0013. |
| `log.h/.cc` | 280 | The log over a snapshot base. Three watermarks — `writing_`, `persisted_`, `commit_` — which differ by exactly one fsync and must not be collapsed (ANV-0016). |
| `message.h/.cc` | 230 | Wire codec. Snapshot flow-control fields are encoded unconditionally (ANV-0012). |
| `raft.h/.cc` | 1,120 | The state machine. No clock, no sockets, no files. Pre-vote, CheckQuorum, pipelined replication, Figure-8 restriction, joint consensus, learners, lease, ReadIndex, transfer. Holds the project's first BUGGIFY site. |
| `storage.h/.cc` | 430 | Durable log, hard state and snapshot, on the LSM's record framing. Every size change is atomic or immediately synced. |
| `driver.h/.cc` | 320 | Touches a `Runtime` and a transport: `truncate → append → hard state → fsync → SEND → apply → advance`, in one loop, with nothing stepping in between. |
| `transport.h/.cc` | 358 | **P5.** One link per pair of nodes, shared by every group on them. Demultiplexes by group id, reconnects, and coalesces heartbeats. |

### `anvil/core/mvcc/` — P4 versions and transactions, hermetic, gated

| File | Lines | Purpose |
|---|---|---|
| `key.h/.cc` | 180 | `'d' | escape(key) | be64(~commit_ts)` and `'l' | escape(key)`. Inverted timestamps make a snapshot read a forward seek and put newest first; the escape is order-preserving, so encoded order equals user order. |
| `mvcc.h/.cc` | 420 | The version store: reads at a timestamp, intents, **atomic** multi-key resolution (`commit_all`/`abort_all` — one `WriteBatch`, see ANV-0029), and GC that keeps the boundary version. |
| `lock_table.h/.cc` | 210 | Wound-wait, with the age comparison in one place, plus the wait-for graph `find_cycle` walks in id order. |
| `txn.h/.cc` | 330 | Transactions: snapshots, first-committer-wins, the GC safepoint, and `kResolving` — the state for an outcome that is decided and whose write did not survive the disk (ANV-0030). |

### `anvil/core/shard/` — P5 ranges and placement, hermetic, gated

| File | Lines | Purpose |
|---|---|---|
| `descriptor.h/.cc` | 423 | `RangeDescriptor`, `Lease`, and the two-level meta index. **An empty `end` is +infinity, and comparing it as a string makes it the smallest value there is** — see ANV-0043. |
| `topology.h/.cc` | 765 | The placement group's replicated state machine: every descriptor in the cluster. One command is one apply, which is where a split's atomicity comes from. |
| `placement.h/.cc` | 373 | `decide()`: a pure function from replicated state to commands. Purity is INV-SHARD-09, not tidiness. Timers are counted in **log entries, not seconds** (ANV-0045). |
| `range.h/.cc` | 730 | One range's data, lease and triggers. Splits and merges are entries in *this* log, which is what makes them atomic with respect to the transfers around them. |
| `router.h/.cc` | 170 | The client range cache, invalidated by generation. |
| `store.h/.cc` | 1,378 | The node's range server: many groups, one transport, one tick loop, quiescence. The only file in the layer holding a `Runtime` — the analogue of `raft/driver.cc`. |

### `anvil/sim/` — the adversary; deliberately NOT hermetic

| File | Lines | Purpose |
|---|---|---|
| `scheduler.h/.cc` | 509 | One event queue keyed by `(virtual_time, seq)`. Evaluates tick/epoch invariants inside the run loop. |
| `fault_profile.h/.cc` | 380 | `FaultProfile::draw(seed)` — the whole adversary, drawn from the seed. `Chance` = exact rational, no float. |
| `net_model.h/.cc` | 357 | Drop, duplicate, reorder, reset, directed links (partitions, half-open), bandwidth. |
| `disk_model.h/.cc` | 564 | Page cache, sector-granularity torn writes, **directory entries tracked separately from contents**, bit rot, EIO, ENOSPC. |
| `clock_model.h/.cc` | 199 | Per-node offset/drift/NTP step/freeze; can deliberately exceed the declared uncertainty bound. |
| `process_model.h/.cc` | 208 | Crash / restart / pause. Ordering inside `crash()` is load-bearing — see the comment there. |
| `fault_injector.h/.cc` | 433 | When faults happen, plus **`unexercised_kinds()`** (INV-SIM-08). |
| `sim_runtime.h/.cc` | 294 | Per-node `Runtime` implementation. |
| `simulation.h/.cc` | 266 | Owns everything for one seed. `run()`, `run_more()`, `heal_and_settle()`. |
| `trace.h/.cc` | 200 | Causal event log with `caused_by` links, JSONL output. |
| `sim_main.cc` | 271 | The `anvil-sim` CLI. **Has its own `main()`** — exclude when bulk-compiling. |

### `anvil/checker/` — the oracles

| File | Lines | Purpose |
|---|---|---|
| `invariant.h/.cc` | 189 | Registry, cost classes, `never_fired()` (INV-SIM-05). |
| `history.h/.cc` | 255 | List-append history model + serial `ReferenceModel`. |
| `elle.h/.cc` | 644 | Version-order recovery, DSG (ww/wr/rw), iterative Tarjan, Adya classification, minimal witnesses. |
| `corpus.h/.cc` | 390 | Known-bad histories (soundness) and serial histories (precision). |
| `raft_invariants.h/.cc` | 640 | The god's-eye view: INV-RAFT-01..16 over every node's log, term, vote, commit index, configuration and lease. No hooks in the protocol — it diffs state between ticks. |
| `mvcc_invariants.h/.cc` | 520 | INV-MVCC-01..08, split into audited and live. |
| `shard_invariants.h/.cc` | 838 | INV-SHARD-01..09 over every node's topology, every range's data, and every lease. Three of them are deliberately narrower than the catalogue's original wording; §13 says why. |

### `workloads/`, `test/`, `tools/`

| File | Lines | Purpose |
|---|---|---|
| `workloads/pingpong.*` | 177 | Token ring. P0/P1 determinism vehicle. |
| `workloads/counter.*` | 598 | Durable replicated counter with WAL + recovery. Arms `INV-CTR-01..03`. |
| `workloads/raft_kv.*` | 830 | Replicated KV over Raft: clients on every node, requests over the *real* transport, session table for idempotent retries, membership churn, leadership transfer. Arms `INV-RAFT-14`. |
| `workloads/mvcc_txn.*` | 640 | Transactions against the version store, with long readers held across GC passes. |
| `workloads/shard_kv.*` | 705 | A sharded bank over many ranges. The oracle is one integer: the total never changes, and every failure this phase is about moves it. |
| `test/raft_test.cc` | 1,005 | Protocol units with no simulator underneath. Every message goes through its wire codec on every hop. |
| `test/shard_test.cc` | 898 | Sharding units: coverage, split and merge preconditions, the meta index, the range cache, the lease rule, and the placement decisions. |
| `test/shard_faults.cc` | 652 | P5 exit criteria: the chaos-admin sweep, the split point, determinism with groups created mid-run, and the seven-bug drill. |
| `test/raft_faults.cc` | 660 | P3 exit criteria: safety, liveness distribution, determinism, pause-vs-lease, and the 10-bug drill. |
| `test/lsm_test.cc` | 536 | LSM unit tests, incl. the tombstone-guard case from ANV-0011. |
| `test/lsm_crash.cc` | 453 | P2 exit criteria: crash cycles, corruption, ENOSPC/EIO, 4 durability mutations. |
| `test/sim_faults.cc` | 411 | P1 exit criteria: correctness under faults, fault coverage, 2 durability mutations. |
| `test/checker_test.cc` | 351 | INV-SIM-03/04, level-boundary discrimination. |
| `test/sim_determinism.cc` | 203 | INV-SIM-01 in-process half. |
| `test/disk_crash.cc` | 233 | Disk model negative control. |
| `test/core_smoke.cc` | 240 | Core primitives. |
| `tools/hermetic_check.py` | 501 | The link-time gate. |
| `tools/lsm_mutations.sh` | 113 | 6 source mutations of the storage engine. |

---

## 5. APIs you will actually call

```cpp
// anvil/core/shard/store.h -- one node's range server, P5
ShardStore store{&rt, self, options, DeterministicRandom{seed}};
store.start(/*bootstrap=*/true);        // idempotent; whoever leads group 1 bootstraps
store.route(request, &route);           // client side: cache, then the meta index
co_await store.send_request(request, route);
store.ranges();                         // every hosted group, for the checker

// anvil/core/raft/transport.h -- shared by every group on a node
RaftTransport transport{&rt, self, tick};
transport.register_group(GroupId{n}, handler);
transport.set_coalesce_heartbeats(true);
co_await transport.flush();             // once per tick, by whatever drives the groups

// anvil/core/runtime/runtime.h -- everything above the seam takes this
Timestamp        now();                   // this node's OPINION, not truth
TimeInterval     now_uncertain();         // [earliest, latest]; may be a lie by design
Task<void>       sleep_for(Duration);
TimerId          schedule(Duration, std::function<void()>);
std::uint64_t    random_u64();
DeterministicRandom& rng(RandomDomain);
Task<Status>     connect(NodeId, ConnHandle*);
Task<Status>     send(ConnHandle, Message);   // ok+delivered, ok+silently lost, or error
Task<Status>     recv(ConnHandle, Message*);
Task<Status>     open/pread/pwrite/fsync/ftruncate/file_size/close_file(...);
Task<Status>     rename/unlink/fsync_dir/list_dir(...);
void             spawn(Task<void>);
Task<void>       yield();
[[noreturn]] void panic(std::string_view);

// anvil/sim/simulation.h
SimConfig cfg = SimConfig::from_seed(seed);   // nodes, faults, buggify -- all from the seed
Simulation sim{cfg};
sim.set_boot(NodeId{i}, []{ /* spawn tasks; MUST perform recovery */ });
sim.invariants().arm("INV-X-01", "name", CostClass::kTick, predicate);
RunResult r  = sim.run();                          // to quiescence, deadline, or violation
RunResult r2 = sim.run_more(Duration::seconds(10));// EXTRA budget -- run() measures from t=0
RunResult r3 = sim.heal_and_settle(Duration::seconds(120));  // eventual synchrony + quiesce checks

// anvil/checker/elle.h
CheckResult res = check(history, IsolationLevel::kStrictSerializable);

// anvil/core/lsm/db.h
std::unique_ptr<Db> db;  co_await Db::open(&rt, DbOptions{}, &db);
co_await db->put/del/get/scan/flush/maybe_compact/close(...);
```

---

## 6. Rules that are not negotiable

### 6.1 Determinism
- **No floating point in any decision path.** Probabilities are exact rationals
  (`Chance{num, den}`). FP contraction differs between compilers; INV-SIM-01
  requires identical digests on gcc/x86-64 and clang/arm64.
- **No `unordered_*` iteration where order affects behaviour.** Use `std::map` /
  `std::set` / sorted vectors. This includes checker witnesses — a bug report
  that names a different cycle on each machine is not reproducible.
- **Fixed-width integers only.** `long` is 64-bit on Linux and 32-bit on Windows.
- **All randomness from the seed**, via `Runtime::random_u64()` or a forked
  `DeterministicRandom`.

### 6.2 `DeterministicRandom::fork(domain, instance)`
Each subsystem gets an independent substream. A single shared stream means adding
one draw to the compaction picker shifts every subsequent draw in Raft — and
every archived seed stops reproducing its bug. **Never renumber an existing
`RandomDomain`.**

### 6.3 Invariant cost classes
`kTick` (after every event, must be O(nodes)) · `kEpoch` (every N events) ·
`kCommit` · `kQuiesce` (only after faults heal) · `kOffline`.
An O(n²) predicate at tick class turns ~38,000 simulated node-hours per core-hour
into a few hundred.

### 6.4 Durability ordering (LSM, and the same shape will apply to Raft)
1. A write is acknowledged only after its WAL record is **durable**.
2. A file enters a version only after **both its contents and its directory
   entry** are durable.

`DurabilityOptions` exposes each guarantee as a flag so the mutation drill can
break it deliberately. **Every default is correct**; a test that flips one is
planting a bug.

---

## 7. Current state

| Phase | Status | Notes |
|---|---|---|
| P0 foundations | done | Seam frozen, hermeticity gate + negative control, digest |
| P1 stream A (faults) | done | 19 fault kinds, all firing |
| P1 stream D (oracles) | done | Invariant framework, Elle-style checker |
| **P2 LSM engine** | **done except benchmarks** | Benchmark criterion blocked on `ProdRuntime` |
| **P3 Raft** | **done, one open finding** | 16 `INV-RAFT-*` armed; 9/10 drill + 1 targeted. ANV-0032 (a learner stops converging on seed 14) is open and the gate is red on it |
| **P4 MVCC + transactions** | **done** | 8 `INV-MVCC-*` armed; snapshot isolation confirmed against the checker at two levels; whole sweep runs in <1s |
| **P5 sharding** | **done again, and for a better reason** | MultiRaft, one Raft group per range; 9 `INV-SHARD-*` armed; 20 ledger rows, 7 of them S0. `shard_faults` 20/20 green. ANV-0051 found INV-SHARD-CLIENT comparing concurrent reads as sequential; because the simulator halts on first violation that false positive had been capping seed 19 at tick 4395 for the whole phase, hiding ANV-0052 (S0) -- a range merged away while holding the only copy of a child's data. Both fixed. See §14 |
| **P6 distributed transactions** | **in progress, four findings open** | Percolator/SSI/StrictSerializable behind one `Coordinator`; 7 `INV-TXN-*` armed (01-04, 09, 11, 12). Green: `anvil_txn_test` 30/30, determinism 3/3, serializable and strict-serializable 4/4 checker-clean, INV-TXN-09 silent. The fault-free money-loss finding is fixed (two root causes), as are the two that turned out to be checkers looking in too few places. What remains: two small conservation shortfalls, one INV-TXN-02, one list-append loss. See §14 |
| **P7 verification depth** | **done** | All 5 exit criteria met: checker mutation (200/200, 0 FPs), Elle cross-validation (10,000/10,000 verdicts identical), TLA+ (9/9 configurations as specified) and trace validation (16/16 runs conform, 12/16 mutated rejected), state-space search (2.2M states complete, 0 violations, 3/3 drill), minimiser (11 features → 1, verified 1-minimal). Three ledger findings — ANV-0063, ANV-0064 (both S4, both from two instruments disagreeing) and ANV-0065 (S1, TLC produced the parallel-commit divergence P6 predicted and no seed reached) — plus seven discrepancies found by trace validation, three of them in the specification itself. See §15 |
| **P8 the bug hunt at scale** | **in progress** | The seed fleet and the unified seeded-mutation report are built; 1 of 6 exit criteria met, 1 partly. First run filed [ANV-0066] -- a stale linearizable read minimised from eleven fault features to two. See §16 |

### Measured (see README results block)

```
determinism ............ 10,000 seeds no faults; 2,000 with adversary armed
throughput ............. ~2.4M events/sec; ~38,500 simulated node-hours/core-hour
fault coverage ......... 19/19 kinds fire
LSM crash sweep ........ 13,116 acked writes, 0 lost, 0 resurrected, 0 orphans
corruption ............. detected 31/31 seeds, 0 bytes served that nobody wrote
seeded bugs ............ 10/10 caught in P2; 2/2 in P1; 13/13 checker mutants
```

### P3 results (measured over 12 seeds of the fault sweep; scale with `--seeds`)

```
safety ................. 12/12 seeds clean -- no INV-RAFT-* violation, no lost
                         acknowledged write, no stale linearizable read
liveness ............... a leader after healing in 12/12; p50 0ms, p90 495ms, max 1195ms
throughput ............. ~1,259 writes acknowledged, ~451 linearizable reads per 12 seeds
membership ............. ~117 joint-consensus transitions during the workload
node-time .............. ~1h20m simulated node-time per 12 seeds
seeded bugs ............ 9/10 from the sweep, the tenth by a constructed case
pause vs lease ......... wall-clock lease safe; tick-counted lease caught by INV-RAFT-13
```

### P4 results (measured over 20 seeds of the fault sweep)

```
safety ................. 20/20 seeds clean -- no INV-MVCC-* violation, no version
                         lost that a live snapshot could still resolve
gc ..................... 634 transactions committed, 734 versions collected,
                         4,060 long-reader re-reads across GC passes,
                         6,973 versions audited against the model
deadlock ............... 11 transactions wounded, 0 wait-for cycles, 0 stalls
isolation level ........ one write-skew history, VALID at snapshot isolation and
                         INVALID at serializable with the cycle reported
                         (T1 -rw-> T2 -rw-> T1). The level is confirmed, not asserted
seeded bugs ............ 2/2 must-detect caught; 1 control correctly silent;
                         1 classified equivalent with a written argument
runtime ................ 0.4s for the whole 20-seed sweep
```

### P5 results (measured over 40 seeds of the chaos-admin sweep)

```
safety ................. 40/40 seeds clean -- no INV-SHARD-* violation, no
                         account lost, the total conserved on every seed
topology churn ......... 563 splits, 552 merges, 73 replica changes, 1,367
                         leadership transfers to colocate a merge, and 1,860
                         Raft groups created and 1,382 destroyed mid-run
split point ............ 21,377 transfers sent believing one range covered both
                         accounts; 18,602 rejected because the topology had
                         moved underneath them; 0 applied partially
client work ............ 2,342 transfers acknowledged, 1,076 lease reads
node-time .............. 2h43m simulated node-time per 40 seeds
multiraft .............. 735,397 heartbeats carried in 570,903 messages;
                         13,092 ticks skipped on quiesced ranges
determinism ............ 4/4 seeds reproduce exactly, groups created and
                         destroyed while the run is in flight
clock .................. 24/40 seeds put a node's clock outside the bound its own
                         configuration declared (worst 17.2s against 248ms);
                         lease findings on those runs are classified, measured
                         every tick rather than inferred from a config flag
seeded bugs ............ 6/6 must-detect caught; 1 control silent; 1 classified
                         equivalent with a written argument
runtime ................ ~60s for the whole 40-seed sweep
```

**What P5 cost, and it is the number worth quoting.** Seventeen ledger rows from
one phase — more than P3's thirteen and P4's four combined — and six of them S0.
The roadmap predicted this ("the phase where bug density is highest and the
invariants are hardest to state") and it was right for a reason worth naming: a
sharding layer is the first place where *the shape of the system itself* is
under concurrent modification. Every other layer has a fixed set of participants.

Four of the seventeen were in the test harness rather than the system, and three
of those manufactured findings out of nothing. That ratio is the phase's own
lesson: a checker over a topology that changes several times a second is about
as likely to be wrong as the topology is.

**What P4 found in P2 and P1.** Four of the defects P4 surfaced were not in P4.
The MVCC workload is the first thing in the tree that writes a key and reads it
straight back from several coroutines at once, and that shook out three
storage-engine bugs (ANV-0025, 0026, 0027) and one simulator bug (ANV-0028) that
two P2 suites and a 40-seed Raft sweep had run straight past. This is the
strongest argument the project has for building layers on top of each other
rather than testing each in isolation, and it is worth saying out loud in any
write-up.

### Stubs and gaps
- **`anvil/prod/` is an empty INTERFACE target.** No `ProdRuntime` exists. This
  blocks all benchmarking and the "same code, two runtimes" claim.
- **CMake never executed.** All verification by hand with g++.
- **Cross-toolchain digest gate never run** (no clang or macOS available). The
  CI job exists; `-O0/-O2/-O3` agreement is the only proxy so far.
- **One BUGGIFY site in the core** (`send_append` shortens a batch). More are
  wanted; each needs an argument for why it cannot make correct code wrong.
- **ANV-0032 is open**: a learner stops converging after a heal on one seed in
  sixteen. No safety invariant fires. `anvil_raft_faults 16` reproduces it.
- **ANV-0033 is open, and it is the important one.** A run's outcome depends on
  the process environment: sixteen bytes of environment variable change seed 9's
  digest and its event count. Something reads an uninitialised automatic
  variable. `-ftrivial-auto-var-init=zero` makes it go away, which identifies the
  class but not the site. **This needs clang + MemorySanitizer on Linux**, which
  this toolchain does not have -- it is the single highest-value thing to do with
  a Linux box, ahead of any new feature work.
- **MVCC crash recovery is not implemented.** Intents are durable; the
  transaction table is not. The P4 profile therefore runs with process crashes
  disabled, which is a scope statement written into `test/mvcc_faults.cc` rather
  than a default anybody has to infer. P5 did not need it — its ranges are a
  bank, not a transactional store — so it is now P6's, and P6 cannot avoid it.
- **A range's data is durable through its Raft log, not through the LSM.** This
  is the largest single departure from how a production store does this, and
  almost everything awkward about P5's split path is downstream of it: the
  right-hand side's data has to be *carried* from the parent to the child as a
  log entry, which means a payload held in one range and needed by another, and
  a coupling that only works while their replica sets agree (ANV-0049). A store
  whose applied state is in the LSM makes a split a pure metadata edit. Doing
  that needs `StateMachine::apply/snapshot/restore` to become coroutines.
- **The meta index's second level is a logical bucket, not its own Raft group.**
  The client pays for both lookups and both are invalidated by generation; what
  is missing is a meta range that can itself split.
- **Merges are never abandoned automatically.** Once begun, a merge completes;
  the subsumed span rejects writes until some node leads both groups again. That
  is an availability cost taken deliberately, because an abort that races the
  survivor's absorb is a correctness hazard (ANV-0046).
- **Custom clang-tidy checks** are specified in `.clang-tidy` but unwritten.
- LSM deferred: block compression, ribbon filters, tiered compaction, reverse
  iteration, streaming k-way merge (compaction buffers its inputs).
- Raft deferred: witness replicas, quorum leases, follower reads (they need
  closed timestamps, so P6), and asynchronous log writes that let the leader ack
  before its own disk.
- Suffix truncation rewrites the whole log file. Correct, and O(log) per
  divergence; a delta scheme is the optimisation if it ever shows up.

---

## 8. How to add a phase (the workflow that has been working)

1. **Write the layer** against `Runtime&`, in `anvil/core/`. Keep it hermetic.
2. **Arm its invariants** in `docs/INVARIANTS.md` and in code, with honest cost
   classes.
3. **Write the fault test**: run under `FaultProfile::draw(seed)`, then
   `heal_and_settle()`, then assert.
4. **Run the seeded-mutation drill.** Plant deliberate bugs; every one must be
   caught. This is the step that makes everything else mean something.
5. **File ledger rows** in `BUGS.md` with a seed, and pin the seed under
   `test/corpus/`.
6. **Update this file**, the README results block, and the roadmap status.

### The drill is not optional
Of the eleven ledger entries so far, **five were found by deliberately breaking
something and discovering the suite did not notice**. In P2, four of six findings
were defects in the test suite rather than the engine — including a suite that
passed cleanly on a database with `fsync` switched off. A green run is evidence
of nothing until you have watched it go red for a reason you planted.

### Equivalent mutants are real
Two checker mutations were *not* caught, and both turned out to be genuinely
equivalent (masked by BFS pruning and by Tarjan discarding single-node
components). Verify by removing whatever masks the mutation and confirming it is
caught then. Do not report an uncaught mutation as a test gap without checking —
and do not report an equivalent one as a pass.

---

## 9. The bug ledger

[BUGS.md](BUGS.md) has 46 real entries (`ANV-0001`..`ANV-0050`, four numbers
unused) plus two format examples. Three non-negotiable rules: every row has a
**seed**, every row has a **pinned regression** in `test/corpus/`, and rows are
written **the day the bug is found**.

Severity: S0 client-visible correctness · S1 internal invariant · S2 liveness ·
S3 resource/perf · S4 test infrastructure.

The `api_visible` column is the point of the whole project — every `no` is a bug
class an outside-in checker structurally cannot find.

Highest-value entries to read before starting a new layer:
- **ANV-0001** — the scheduler discarded one event at every deadline, orphaning a
  coroutine. Four wrong hypotheses chased first because they all had the same
  symptom.
- **ANV-0005** — an armed invariant that could never fire, because a cheaper one
  shadowed it. Co-firing on an epoch boundary looked like detection.
- **ANV-0006** — the crash suite passed on a database with fsync off.
- **ANV-0011** — a guard whose situation the workload could not reach.
- **ANV-0038** — a safety margin that was right by a factor of two, and an
  invariant that could only catch it by coincidence until it was rewritten to
  check the rule instead.
- **ANV-0043** — the audit reported six missing accounts on a cluster that had
  lost none, because an unbounded key range sorts like an empty string.

---

## 10. Gotchas that have cost real time

**10.1 MSYS2 DLL shadowing (Windows dev machine).** Git for Windows'
`libstdc++-6.dll` shadows the MSYS2 UCRT64 one, and binaries segfault inside
`std::ofstream`'s constructor. The backtrace points into libstdc++ and looks like
your bug. **Always** `export PATH="/d/msys2/ucrt64/bin:$PATH"` before running
anything you built.

**10.2 `sim_main.cc` has its own `main()`.** Exclude it when bulk-compiling
sources into a test binary, or you get a duplicate-symbol link failure that
reads as a build-system problem.

**10.3 `run()` measures its budget from t=0.** Calling it twice returns instantly
the second time. Use `run_more(extra)` or `heal_and_settle(grace)`.

**10.4 Arena block size vs memtable threshold.** `Arena` allocates in 4096-byte
blocks and `memory_usage()` counts allocated blocks, so a `memtable_bytes` of
4096 flushes on the *first insert*. This silently disabled WAL testing entirely
(ANV-0006). Keep the threshold at several blocks.

**10.5 GCC `-Wnull-dereference` false positive in coroutine frames** at `-O2` on
vector copy-assignment. Restructure the copy rather than suppressing the flag.

**10.6 Shell heredocs mangle `\n` in C++ string literals.** Repeatedly produced
unterminated-string errors when patching test files via `python3 <<'PY'`. Use the
editor for anything containing escape sequences.

**10.7 `heal_and_settle` must heal *everything*.** Partitions live in the link
table; drop/duplicate/EIO/bit-rot live in the models' own fault profiles. Clearing
only the first leaves the cluster still lossy and makes every liveness assertion
unfalsifiable (ANV-0002).

**10.8 Nothing may step the state machine while a persist is in flight.** The
fsync is a real suspension. A message stepped inside it changes the log the
batch was written from, and a reply generated before the suspension then claims
entries a truncation has since removed. `RaftDriver::pump()` queues incoming
messages and fired ticks and applies them at the top of the loop; that queueing
is what makes the Ready model equivalent to a single-threaded step loop
(ANV-0015).

**10.9 A size change is not crash-safe in place.** The disk model marks *every*
sector of a file dirty on `ftruncate`, which is a fair reading of what a
filesystem may do with an uncommitted size change. A crash before the next fsync
then resolves each sector independently, and the region past the new end keeps
the old bytes — so the file comes back as the new prefix spliced onto the old
tail. Every size change in `anvil/core/raft/storage.cc` is therefore either
immediately fsynced or done as write → fsync → rename → fsync_dir.

**10.10 Quorum arithmetic on even voter counts.** The majority index is `n/2`,
not `(n-1)/2`. They agree for odd `n`, which is every hand-written test, and
differ for even `n`, which is every joint-consensus transition (ANV-0013).

**10.11 The checker must read durable state, not volatile state.** A term bump
or a vote that has not reached the disk has not reached a peer either, so losing
it in a crash is not a regression. Checking the in-memory field reports every
crash during an election as a violation (ANV-0021). The same rule produced three
sibling fixes: a node that is alive but still recovering is *unknown* rather than
empty; a commit decision is judged against the configuration in force when it was
made; and a stale leader's commit index is not the cluster's.

**10.12 A retry must resend the same request.** The workload's client drew its
operation fresh on each attempt, so a timed-out write came back as a read under
the same identity and a late reply attached to the wrong one — manufacturing
stale reads out of a perfectly healthy system (ANV-0022).

**10.13 A crashed node's ordering.** In `ProcessModel::crash()`: purge queued
events → clear network endpoints → destroy coroutine frames. Any other order is a
use-after-free that only fires under crash-heavy seeds.

**10.14 Anything derived before a `co_await` is stale after it.** This is the
single highest-yield rule in the tree and it produced three separate S0s in P4.
A sequence number read from a watermark and used after two suspensions collides
with every writer that started in between (ANV-0025). A raw pointer handed out by
a cache is valid only until the holder suspends inside it (ANV-0026). A
long-running operation that installs shared state and then suspends -- a flush
installing `immutable_` -- must exclude a second one, or the second overwrites
what the first is still reading (ANV-0027). None of these look like concurrency
bugs on the page: there is no lock to forget and no thread in sight.

**10.15 Never iterate a container across a suspension point.** The coroutine form
of mutating while iterating, and it corrupts the heap rather than failing
cleanly. Swap the container into a local first. Cost the first time: a debug
build and a `thread apply all bt` to find that the crash was nowhere near the
cause.

**10.16 A loop's termination argument has to hold for the configurations the
caller is allowed to pass.** `apply_partition` redrew until both sides of a split
were non-empty, which never happens with one node, and single-node is exactly
what P4 runs (ANV-0028). Simulated time stops advancing, so it reads as a hung
test rather than an infinite loop.

**10.17 Reach for the debugger at two hypotheses, not at zero.** ANV-0028 cost
about two hours of theorising about GC safepoints, compaction and quadratic
invariant scans, against ninety seconds of `gdb -p` to read one stack frame. The
same lesson is recorded at ANV-0023 and was not applied quickly enough the second
time. The trigger is not "I am out of ideas"; it is "I have more than two ideas
and no measurement that separates them".

**10.18 Cumulative counters cannot answer questions about phases.** "received
2607" over a whole run says nothing about whether anything arrived after the
network healed. This is currently what blocks ANV-0032. Diagnostics that will be
read after a heal need to be resettable at the heal.

**10.19 Layout-sensitive bugs defeat layout-changing bisection.** Removing
`-ftrivial-auto-var-init=zero` from one translation unit at a time to find which
one reads uninitialised memory does not work: the flag changes stack layout
across the whole program, so the garbage moves and sixteen of forty files look
guilty (ANV-0033). The oracle that *does* work is perturbing the environment --
`PADVAR=AAAAAAAAAAAAAAAA` flips the outcome on demand -- but the instrument that
actually localises it is MemorySanitizer, which needs clang on Linux.

**10.20 A checker's own bookkeeping is not evidence about the system.** The MVCC
auditor retired finished transactions to keep a per-event scan affordable, and
then reported intents whose owner it could no longer find as orphans -- a finding
manufactured entirely by the checker's memory limit. An unattributable
observation is counted separately and named as a blind spot, never reported as a
defect.

**10.21 Two halves of one fact, read from two places, will disagree.** This is
P5's version of the durability lesson and it produced three separate S0s. A merge
trigger took its span from the topology and its data from the machine, and
absorbed twelve accounts while recording that it owned six (ANV-0042). A lease
handover allowed one clock bound where the two nodes involved can be wrong in
opposite directions, so it needed two (ANV-0038). A placement timer subtracted a
timestamp stamped on one node from the clock of another, and fired instantly
whenever the skew pointed the wrong way (ANV-0045). None of them is visible in a
review that does not ask, for every value in an expression, *where did this come
from and when*.

**10.22 A lazy coroutine's parameters must be by value.** `Task` does not run
until it is awaited or spawned, so `spawn(f(x))` with `f(const T&)` captures a
reference into a frame that is gone by the time the body starts. It does not
crash: it reads plausible bytes, encodes them, and sends them. The symptom was
every client request timing out while the server counted 80 requests received and
80 replies sent (ANV-0034). This is gotcha 10.14 with the suspension moved to
before the first line of the body.

**10.23 An index handed to a state machine is not the log's applied index.**
`StateMachine::apply(index, bytes)` gives the machine the index of the last entry
it was handed. A snapshot install hands it a whole state and no index at all, so
after one the machine's number is stale beside a state that is fresh. Two
consumers made the same mistake on the same afternoon -- an invariant that
compared replicas by it and reported a divergence between two correct nodes
(ANV-0040), and a read reply that used it as a freshness measure and reported a
stale read that never happened (ANV-0041). Use `node().log().applied_index()`,
and skip a node whose `snapshot_pending()` is true.

**10.24 An empty key bound means infinity and sorts like zero.** `end == ""` is
"unbounded above", and every string comparison makes it the smallest value there
is. `survivor_end >= desc.end` is therefore true for every range at the top of
the key space regardless of what the survivor holds, which made the conservation
audit report six missing accounts on a cluster that had lost none (ANV-0043).
Sentinels that are also legal values need a helper, not a comment -- descriptor.h
had the comment and the code below it made the mistake anyway.

**10.25 An in-memory decision that must survive a crash has to be derivable.**
The store remembered "this group was merged away" in a `std::set`, and a restart
lost it: the node then saw a range the topology still listed, created it, and its
durable Raft log replayed data the survivor already held (ANV-0047). The fix is
not to persist the set but to re-derive the fact from something that already
survives -- the survivor's own applied span.

**10.26 A checker that samples per tick cannot grade a decision made between
ticks.** Three invariants had to be narrowed or re-sourced for this: the merge
precondition (which lease was in force?), the rebalance rule (which learners had
reported catching up?), and the lease sequence (which lease did this one
replace?). The last was fixed properly, by having the range machine keep the
lease it replaced so the *pair* is read from one place. The first was narrowed
and the omission written down. The temptation in the middle -- a hook in the
state machine recording its own inputs -- is the one to resist: a state machine
that carries evidence for its checker is no longer the thing that ships.

**10.27 A BUGGIFY site must never sit inside code whose output has to be a pure
function of replicated state across independent nodes.** Enablement is
deterministic (hash of seed and site id), but *firing*, given enabled, is drawn
from the run's shared RNG stream at the moment of evaluation -- so two nodes
independently evaluating the same site over identical state can still fire
differently, purely because their coroutines reached the call in a different
schedule order. A site nominated inside the placement driver's split/merge
decision made INV-SHARD-09 ("placement decisions are a function of replicated
state alone") fire on ordinary seeds with no other fault active (P8, while
adding the first BUGGIFY sites outside `raft.cc`). The safe places are a single
node's own local, unreplicated choices; never a decision more than one replica
is expected to reach identically. Documented at the macro's own definition
(`anvil/core/buggify.h`) so the next site placed gets the constraint before the
mistake, not after.

**10.28 A coroutine must never be the thing that decides to crash the node it is
hosted on.** `ProcessModel::crash(node, delay)` calls `Scheduler::
destroy_tasks_for(node)`, which destroys *every* task that node owns --
including, if the coroutine issuing the crash happens to be spawned on that same
node's `Runtime`, its own frame, mid-call. Execution returns into a destroyed
frame and segfaults with a backtrace that looks like it's pointing at scheduler
internals rather than at the real cause. Driving a schedule that crashes nodes
by design (P8's mixed-version rolling-upgrade workload) has to be done from
host-side code between `simulation.run_more()` steps, never from a spawned
per-node coroutine, regardless of which node it's hosted on -- every node is a
potential crash target, so there is no node that's safe to host it on.

**10.29 MSYS2/Git-Bash's `/tmp` and CPython's own idea of a temp directory are
not the same path on Windows, and neither is "the executable named `bash`."**
`tools/mutation_report.py` already carries this scar for `/tmp` specifically
(the shell maps it to `%TEMP%`; CPython does not); P8's ledger-seed verifier hit
the sibling version twice more in the same tool. First: `subprocess.run(cwd=
"/tmp/...")` from Python resolves that path through Win32 `CreateProcess`, which
doesn't understand it, so the child launches in the wrong directory and every
relative path inside it silently fails to resolve -- use `tempfile.
gettempdir()` for anything a Python script will later hand to `subprocess`.
Second: prepending a Unix-style entry (`/d/msys2/ucrt64/bin`) to a `PATH` string
before calling `subprocess.run(["bash", ...])` makes the executable search skip
past it (it isn't a Windows path) and fall through to Windows' own WSL `bash.exe`
stub, which fails with an unrelated-looking `execvpe` error. Resolve the real
executable once via `shutil.which()` against the *unmodified* environment and
invoke that absolute path directly.

**10.30 "Recovery takes the last valid record" is only safe if the damage that
invalidates a record is damage the record itself could not have already
survived past.** `RaftStorage::recover()` reuses the LSM WAL reader's own
contract for the hard-state file (`anvil/core/lsm/wal.h`: "reads until the
first invalid record," everything after is dropped as the tail) -- correct
for a torn write, which by construction can only ever land on the true tail of
an *unsynced* append, so nothing durable is lost by discarding it. Bit rot
does not respect that assumption: it corrupts a record that already passed
its checksum once, and can land anywhere in the file, including in the middle,
below records that were written and fsynced afterward. Discarding everything
from an interior bit-rot hit onward throws away later data that was genuinely
durable -- and for the Raft hard-state file specifically, "genuinely durable"
for a vote means "already replied to a peer" (`RaftDurability::fsync_state`:
the driver sends nothing before its fsync). `anvil/checker/raft_invariants.cc`
already encodes half of this distinction correctly: it exempts a `corrupted`
node's term and commit regressions (reasoning that what never reached disk
never reached a peer either, so losing it is invisible to the cluster) but
does *not* exempt a vote conflict -- and per the paragraph above, that
asymmetry is the checker being right, not incomplete, because a bit-rotted
vote record was, unlike an unsynced one, already relied on elsewhere.
[ANV-0067](../BUGS.md)'s minimisation (`partition + disk.bit_rot +
clock.jump`) is what pointed at this path at all -- `disk.bit_rot` being
load-bearing in a 3-feature minimal set, rather than the workload's own
`process.crash`, is what redirects the question from "is the mixed-version
schedule broken" to "is interior corruption of an already-fsynced record
handled safely," a question the workload never intended to ask and answered
anyway.

**Fixed, partially, same pass.** `lsm::WalReadResult` gained
`discarded_had_more_data`: true only when the record that failed validation
had a length field that fit inside the file and something else was durably
appended past it, which `anvil/sim/disk_model.cc`'s dirty-sector-only torn
writes and `RaftDriver::persist()`'s one-record-then-sync discipline together
guarantee can never be true for an honest torn write. `RaftStorage::recover()`
now refuses (`StatusCode::kCorruption`) rather than silently trusting the
older record when that flag is set on the state file, which keeps the node
retrying recovery forever (`RaftDriver::boot()`'s existing unbounded-retry
policy) instead of ever rejoining with a vote memory it cannot trust. Verified
against the pinned seed: clean.

The gap this does not close is the more instructive half. The discriminator
is unavoidably blind whenever *nothing* follows the bad record -- which is
exactly true both for an honest torn write (by construction, since nothing is
ever appended after an unsynced record) and for bit rot landing on the file's
true last record (already synced, nothing appended since). No signal in this
file format tells those two apart. A second failing seed (19) confirms this
is reachable, not theoretical: it minimises to `[disk.bit_rot]` alone -- no
partition, no clock fault -- and still reproduces post-fix. Closing it needs
a redundant/dual-copy hard-state write, not a smarter reader.

**The fix's own measured effect turned out to be the strongest evidence for
ENGINE, and very nearly got reported backwards.** A 300-seed before/after
comparison of `anvil_mixed_version_faults 300` shows 39/300 failing before
this change and 44/300 after -- worse, by raw count. The set difference is
what makes it legible: 10 seeds (including the pinned one) are now clean, and
15 *different* seeds newly fail, all recognisable members of the same
invariant family. This is the simulator's determinism working exactly as
gotcha 5a.3 (P7) describes: changing one node's recovery timing on the seeds
where the fix fires reshuffles that whole seed's schedule, and on 15 of them
the reshuffled schedule lands on a different instance of the same underlying
vulnerability the original schedule happened not to reach. The instinct after
the pinned seed went clean was to call the row fixed; running a large-enough
sample before writing that down is what caught that the honest claim is
"partially fixed, and the partial fix's own side effect is confirming evidence,"
not "fixed." See [ANV-0067](../BUGS.md) parts 2-3 for the full numbers.

**10.31 A lower layer refusing to lie creates upper-layer states nothing
upstream has ever had to handle, and "no regression in the suite that motivated
the fix" is not the same claim as "no regression."** ANV-0067's storage-layer
fix (10.30) makes "this Raft group's local replica can never safely recover"
a real, reachable outcome for the first time in this tree -- before it,
`RaftDriver::boot()`'s unbounded retry always eventually succeeded, so nothing
downstream had ever needed a plan for a replica that simply never comes back.
[ANV-0068](../BUGS.md) is what that plan's absence looks like: shard's
dead-replica replacement (`anvil/core/shard/placement.cc::decide()`) computes
liveness from per-*node* heartbeats (`live_nodes()`), and a node hosting many
Raft groups can have one range's replica permanently stuck while the node
itself, and every other group on it, heartbeats normally -- so the stuck
replica is invisible to the mechanism that exists to replace dead replicas,
and if enough of one range's replicas hit this independently, the range's data
is gone with no self-healing path. Found only because the regression pass
after ANV-0067's fix ran `shard_faults`, a suite ANV-0067's own fix had no
reason to touch and would never have been re-run under a narrower "does the
mixed-version workload still work" check. The generalisable rule: a
correctness fix to a shared, lower layer needs its regression check run across
every suite built on that layer, not just the one that found the bug, because
the interesting failures are exactly the ones in code that was never touched.

## 11. P3 (Raft): what was decided, and why

### The Ready model, and the one loop

`anvil/core/raft/raft.h` is a pure state machine: no clock, no sockets, no
files. Its only output is a `Ready` batch describing what must become durable and
what must then be sent. `driver.cc` is the only file in the layer that holds a
`Runtime`, and it does this and nothing else:

```
truncate -> append -> hard state -> fsync -> SEND -> apply -> advance
```

Three things follow, and all three earned their keep during P3:

1. **The durability ordering is six lines, not a discipline.** "Persist the vote
   before replying" is the shape of the loop, so the mutation that breaks it
   (`persist_before_reply`) is one line and was detected on 7 seeds out of 7.
2. **The checker needs no hooks.** `raft_invariants.cc` reads every node's state
   through const accessors and diffs it between ticks. The protocol does not
   know it is being watched, so the thing being checked is the thing that ships.
3. **Protocol tests need no simulator.** "A candidate at term 5 whose log is two
   entries behind" is a sentence in `raft_test.cc`, not a seed hunt — and the
   situations that matter most in Raft are exactly the ones a random workload
   reaches once in thousands of runs.

**Nothing may step the state machine while a persist is in flight** (gotcha 10.8).
The fsync is a real suspension; queueing messages and ticks in `pump()` is what
makes this equivalent to a single-threaded step loop.

### Cost classes, in practice

INV-RAFT-01/02/04/05/06/07/08/09/10/11/12/13/15/16 run at `kTick`, and they are
affordable because the observer keeps a cursor per node and only looks at what
changed — amortised O(1) per new entry, O(nodes) per tick. INV-RAFT-03 (full
pairwise Log Matching) is O(nodes² · log) and runs at `kEpoch`; INV-RAFT-16 is
its incremental proxy and fires within one event. Both stay armed, per ANV-0005.

The leader-side properties (09, 10, 15) are evaluated at the tick a commit index
advances, against the configuration in force at that moment. Re-deriving them
later compares a decision made under one membership against a different one, and
with churn running that reports correct commits as violations.

### The lease refuses to exist when it cannot be sound

`lease_is_sound()` turns lease reads off unless the declared clock uncertainty
is smaller than the lease and the two together fit inside the minimum election
timeout. The fault profile draws a declared bound anywhere in 1–250 ms and can
*deliberately exceed it*, so on many seeds there is simply no lease and every
read pays for a ReadIndex quorum round. That is the honest configuration: a
lease is an optimisation licensed by a clock bound, and when the bound is too
weak the licence is void. Seeds where the model exceeds its own declared bound
are classified and counted rather than failed.

### The drill, and the first BUGGIFY site

Ten deliberate bugs, one per flag in `RaftOptions` / `RaftDurability`, every
default correct. Two of them — the Figure-8 commit restriction and the append
consistency check — need a *lagging follower at the moment of an election*, which
random scheduling produces roughly once in thousands of seeds. The project's
first BUGGIFY site lives in `send_append` and shortens a batch, which is always
legal and cannot make a correct implementation wrong. With it the Figure-8
window is reachable in tens of seeds. That is the whole argument for BUGGIFY,
and it is now demonstrated rather than asserted.

The drill reports three columns: whether the suite noticed at all, whether an
internal invariant noticed, and whether a client could have seen it. The gap
between the second and third is the protocol-aware claim.

### Where P3 stands

Complete. Twelve of twelve seeds are clean under the full adversary, a leader is
elected after healing in every run, the drill catches nine of ten planted bugs
from the sweep and the tenth from a constructed case, and the pause-versus-lease
experiment discriminates the safe implementation from the unsafe one.

Thirteen real bugs were found and fixed getting there (ANV-0012..ANV-0024), five
of them invisible at the client API. Three were S0: a quorum computed as two of
four voters, a retried append that duplicated a run of indices, and a crash
inside a rename window that lost an entire log file.

**The durability lesson, in one line:** four of the five storage bugs were the
same mistake in different clothing — a write path that is not idempotent under
retry, and a size change that is not atomic under crash.

---

## 12. P4 (MVCC and transactions): what was decided, and why

### The safepoint is computed in one place, and judged when it is published

`TxnManager::safepoint()` is the only expression of "what may be collected": the
minimum of the closed timestamp, every live transaction's start, every open
snapshot, and -- added after the fact -- the highest commit. That last clamp
matters more than it looks. Without it, a moment with no live reader yields
`kMaxCommitTs`, which means "collect everything". That is defensible in the
abstract and a loaded gun in practice: any later change that makes a reader
visible slightly late turns it into total data loss, and the invariant that
would have caught it cannot even be stated against a bound of infinity.

`INV-MVCC-02` checks the safepoint **at the instant it is published**, against
the floor in force at that instant, rather than comparing the highest safepoint
ever seen against the floor as it stands later. The second form is not a weaker
check, it is an unsound one: a transaction that begins after a perfectly legal
collection lowers the floor beneath a safepoint that was correct when it was
used, and the invariant reports a bug that never existed. Same rule as gotcha
10.11 -- a decision is judged against the state in force when it was made.

### An outcome is decided in memory before it is written to disk

A commit whose batch fails is the hard case, and the obvious handling is wrong.
Returning the error and moving on leaves a transaction that is neither committed
nor aborted, holding intents nobody will ever clear and pinning the GC safepoint
at its start timestamp for the rest of the process's life. One transient EIO and
the collector never collects again (ANV-0030).

So `commit` records `commit_ts` *before* attempting the write and moves the
transaction to `kResolving` on failure; a janitor retries the batch until it
lands. Retrying is sound because resolution is one atomic batch -- it landed or
it did not, and replaying writes the same keys with the same values at the same
timestamp. `abort()` refuses a `kResolving` transaction, because letting an abort
through there would not retry a decision, it would reverse one.

The engine does not schedule its own retries. A state machine that starts its own
I/O is a state machine you cannot test deterministically, so driving the janitor
is the caller's job -- the same division as Raft's `Ready` loop.

### Atomicity comes from a primitive, not from a loop

The first commit path resolved intents one key at a time. Each write was durable
and the sequence was not atomic, so a reader scheduled inside the loop saw half a
transaction -- exactly what snapshot isolation promises cannot happen (ANV-0029).
The storage engine already had the primitive: one `WriteBatch` is one record with
one checksum. The fix was to reach for it.

### The level is confirmed, not asserted

`test_snapshot_isolation_is_the_level_claimed` runs one hand-built write-skew
history through the checker at both levels and requires VALID at snapshot
isolation and INVALID with `G2-item` at serializable. The first version of that
test built the two transactions and nothing else, and reported the history as
serializable -- correctly, because with no third transaction there is no evidence
of which version of each key came first, so there is no version order, no
anti-dependency edges and no cycle to find. An anomaly nobody observed is not in
the history. The observer transaction is what makes the claim real, and the
vacuous version is the more dangerous failure of the two because it passes.

### Equivalent mutants are classified, with the argument written down

`reads ignore intents` disables intent-blocking, and nothing in this
configuration can observe the difference: every timestamp comes from one
monotonic source, so a transaction still live when a reader takes snapshot `S`
must commit at some `C > S`, and the version is above the snapshot either way.
Rather than pretend to detect it, the drill classifies it `kEquivalent` and
asserts two things -- that it is genuinely not reported, and that the flag
genuinely disables the mechanism (`blocked_reads == 0`). It is expected to move
to `kMustDetect` in P6, where clock skew makes `C < S` possible in real time.

### Crashes are out of scope, and the scope is in the code

`test/mvcc_faults.cc` disables process crashes with a paragraph saying why: the
single node has no redundancy, and every property P4 claims is about live
transaction state that a crash destroys by definition. Before that line existed,
the workload kept reading from a dead node and produced findings that said
nothing about MVCC. Recovering intents and re-deriving outcomes after a crash is
real work and it is P5's.

### The sweep has to be fast enough to run

The P4 suite went from an eight-minute timeout to 0.4 seconds for 20 seeds, and
none of that was optimisation. Three separate causes: an invariant scanning a
transaction table nothing ever pruned, a run that only ended on a commit target
that a heavily-aborting seed never reached, and a fault injector that never
terminated on a single node (ANV-0028). A suite slow enough to be run rarely is a
suite that stops finding things, so its runtime is a property worth defending.

### Where P4 stands

- 8 `INV-MVCC-*` armed, split into audited (the version store is behind a
  coroutine and an invariant cannot await) and live.
- 20/20 seeds clean; 634 transactions committed, 734 versions collected, 4,060
  long-reader re-reads across GC passes.
- Drill: 2/2 must-detect caught, control silent, one equivalent with an argument.
- Four defects found *below* P4: three in the LSM, one in the simulator.

---

## 13. P5 (sharding): what was decided, and why

### Every range is a real Raft group, which cost a transport

P3's driver owned its own connections and its own receive loop, which works
while there is one group per node. P5 has one per range plus one for placement,
and the simulator's network gives each ordered pair of nodes a single inbox --
two receive loops on one endpoint race for the same queue. So `raft/transport.h`
appeared: one link per pair, shared by every group, demultiplexing on a group id
that now travels in the message (first, before the type byte, so routing costs
one varint and group 0 is free to mark a coalesced batch).

Three things came with it, and the second is the one that matters:

1. **Heartbeat coalescing.** With a group per range, per-group heartbeats are
   O(ranges × peers) messages a tick and almost all of them carry nothing.
   Buffering them per peer and flushing one envelope per tick makes it O(peers).
   735,397 heartbeats in 570,903 messages over the sweep -- a real mechanism and
   a modest ratio, because these clusters have five ranges rather than five
   thousand, and the honest number says so.
2. **One tick loop per node, not per group.** Coalescing is only possible if the
   groups that produce heartbeats tick together, and a timer per range is a
   scheduling cost that grows with the topology. `RaftDriver::set_external_ticks`
   exists for this.
3. **Quiescence.** A group with nothing to do stops ticking, which is the whole
   point of MultiRaft. It comes with a trap: ticking is also what drives the
   election timeout, so a *leaderless* group that quiesces can never elect a
   leader -- waking needs a message and a message needs a leader (ANV-0044).

### The topology is one replicated state machine, and one entry is one apply

Every descriptor lives in the placement group's log. A split is not "shorten the
left range, then create the right one": it is one command whose apply does both,
so no observer ever sees the key space covered twice. Writing it as two commands
is a one-line change, it is the first mutation in the drill, and the window it
opens is a few microseconds wide and completely invisible from the client.

### Splits and merges are triggers in the *range's own* log

This is what makes a split atomic with respect to the transfers around it, with
no lock anywhere: a transfer either precedes the trigger in the range's log and
applies under the old descriptor, or follows it and is rejected against the new
one. There is no third ordering. 21,377 transfers were sent believing one range
covered both their accounts and 18,602 were rejected because the topology had
moved; not one applied partially.

The corollary is a rule the layer now states outright: **a range's span changes
only through an entry that carries the data with it.** The topology's view of a
descriptor is replicated into the range too -- the range needs it to validate
requests without a round trip -- but that command carries membership and
generation only. Letting it carry the span left a range holding accounts it no
longer claimed (ANV-0037).

### Placement is a pure function, and it may not read a clock

`decide(state, options, now, cluster_size)` returns commands. Every input except
`now` is replicated, which is what makes INV-SHARD-09 -- two replicas at the same
applied index decide the same thing -- a property a checker can evaluate rather
than a claim.

`now` was the interesting part. The first version compared a descriptor's
`changed_at` (stamped by whichever node proposed the change) against the
placement leader's clock, and under a frozen clock that difference is seconds:
the merge timeout fired the instant a freeze landed, forever, in a
freeze/abort/refreeze loop (ANV-0045). Every placement timer is now counted in
**placement log entries**, which mean the same thing on every replica. Liveness
still needs a wall clock, so the placement leader restamps every heartbeat with
its own -- one clock decides who is alive, and it is the clock of whoever is
doing the deciding.

### A merge is completed, never abandoned

The survivor absorbs the subsumed range's data through an entry in its *own*
log, which the placement driver cannot see. So an abort issued because the merge
looked stalled can land after the absorb, un-freezing a range whose contents are
now in two places at once -- 4,200 in a cluster that started with 2,400
(ANV-0046). There is no automatic abort any more. The cost is availability of
the subsumed span until some node leads both groups again, which under eventual
synchrony is one election, and it is the right trade: the alternative is a
correctness hazard that appears only when the abort wins a race it usually
loses.

### The lease margin is two clock bounds, not one

The declared uncertainty is how far *one* node's clock may be from true time. A
lease handover involves two, and they can be wrong in opposite directions, so
the interval that must have elapsed since the previous expiry is two bounds wide
(ANV-0038). With one bound the implementation is safe on most seeds and wrong on
the ones where the errors point apart.

INV-SHARD-04 had to be rewritten for the same reason the bug existed: its first
form looked for two nodes *simultaneously* believing they held a lease, which
needs a lagging replica on top of the defect. Checking the rule instead of the
coincidence -- successive leases in a range's log never overlap by two bounds --
took the detection rate from 0/6 to 14/20.

### The oracle is one integer

The workload is a bank, and that is the whole reason it is one. A split that
drops a key, a merge that loses one, a write accepted against a stale
descriptor, a transfer applied twice: every failure this phase is about moves a
single number that a black-box client could compute. Three of the six S0s were
found by that number and nothing else, and the internal invariants found the
other three -- which is exactly the split the API-visibility column exists to
show.

### Where P5 stands

Complete. Forty seeds clean under the full adversary with the topology never
sitting still; the split point exercised twenty thousand times with no partial
application; determinism holding while Raft groups are created and destroyed
mid-run; six of six planted bugs caught, one control silent, one equivalent with
an argument.

Seventeen bugs found and fixed getting there (ANV-0034..ANV-0050), six of them
S0. Four were in the harness, and three of *those* manufactured findings out of
nothing -- which is the phase's own lesson, and the reason gotchas 10.23, 10.24
and 10.26 exist.

### Next: P6

Per [docs/ROADMAP.md](docs/ROADMAP.md). Three things this phase leaves on the
table: MVCC crash recovery (still open from P4, and P6 cannot avoid it), moving
a range's applied state into the LSM so that a split stops having to carry data
in a log entry, and ANV-0032, which is open and holds the Raft gate red on one
seed in sixteen. ANV-0033 remains the highest-value thing to do with a Linux box.

---

## 14. P6 (distributed transactions): where it stands, mid-phase

The mechanism (`anvil/core/txn/`: `timestamp`, `record`, `store`, `command`,
`coordinator`) and its instrumentation (`anvil/checker/txn_invariants.h/.cc`,
`workloads/txn_bank.h/.cc`) were written in one pass, before any of it had been
compiled or run. This section is the record of getting it from that state to a
build that passes its own unit suite and mostly passes its own fault sweep --
**mostly**, because one real finding is still open. None of this has been
committed yet, which is why the fixes below are not BUGS.md rows: the ledger's
own rule is that a row needs a commit to bisect against, and there is no parent
commit that has this code without the fix. If this lands as-is, the open
finding below should become the first P6 ledger row the moment it does, because
at that point a real "before" exists.

### What exists

One `Coordinator` behind `begin`/`get`/`put`/`commit`/`rollback`, with the
isolation level (`kSnapshot`/`kSerializable`/`kStrictSerializable`) as a
`CoordinatorOptions` setting rather than three codebases. A replicated
timestamp oracle (`kOracleGroup = 2`, sharing the range-per-group model P5
built) and an `HybridClock` with a declared uncertainty interval are both
wired through `ShardStore`. `test/txn_test.cc` (30 checks) covers the
mechanism directly -- the version store, the record status lattice, the
opaque command interface a range applies, the GC boundary, and both timestamp
sources -- with nothing running. `test/txn_faults.cc` runs the coordinator
over a sharded, chaos-admin cluster (topology churning the same way P5's did)
at all three levels, confirms the checker's verdict against a hand-built
history before trusting it live, and runs a nine-flag seeded-mutation drill.

### Six real defects, found getting the fault sweep to run at all

None of these needed an adversarial seed to reach; the first three were
findings before the sweep produced a single clean run.

1. **A committed tombstone was indistinguishable from a committed empty
   string.** `VersionStore`'s version chain stored a delete as an empty
   payload with nothing marking it as one, so a deleted key resurrected the
   moment anyone wrote `""` to it, and `get()` had no way to say "found, but
   deleted" at all. Fixed by giving each version entry its own tombstone flag
   ([store.h](anvil/core/txn/store.h), [store.cc](anvil/core/txn/store.cc)).
2. **INV-TXN-09 read a node's own recovery as a timestamp being reissued.**
   The oracle's high-water observer compared a just-restarted node's fresh
   `OracleMachine` (which starts at `kMinTs`) against what that same node
   reported before it crashed, with no guard for "this replica has not
   finished replaying its own log yet" -- gotcha 10.11's exact shape, on a new
   subsystem. Fixed with the same test ANV-0048 established: not just
   `ready()`, but `applied_index() >= commit_index()`
   ([txn_invariants.cc](anvil/checker/txn_invariants.cc)).
3. **A transaction record with an empty key list was invisible to every split
   and merge.** `VersionStore::encode_span`/`erase_span` locate a record by
   `keys.front()`, but the coordinator only populated `keys` when
   `parallel_commit` was on -- the common, default configuration left every
   record's key list empty, so it never migrated on a split and was silently
   dropped from every merge payload. Fixed by always seeding `keys[0]` with
   the primary, in the coordinator's `put_record` and in the version store's
   own synthetic tombstone-abort path ([coordinator.cc](anvil/core/txn/coordinator.cc),
   [store.cc](anvil/core/txn/store.cc)). The same gap meant a transactional-only
   range's `key_count()`/`median_key()` only ever looked at the P5-era
   `balances_` map, which a P6 workload never touches -- such a range could
   never report growth and therefore could never trigger a split at all.
   Fixed by summing both stores and falling back to the transactional median
   when there is no bank data ([range.h](anvil/core/shard/range.h),
   [range.cc](anvil/core/shard/range.cc)).
4. **A transaction id was a per-`Coordinator` counter, and every restart
   builds a fresh `Coordinator`.** Gotcha 10.25's exact shape: an in-memory
   decision (`next_id_`, starting at 1) that has to survive a crash but is not
   derived from anything that does. A node that crashed and rebooted mid-run
   could reissue an id its previous incarnation already held a record or an
   intent under. Fixed by using the transaction's own start timestamp as its
   id -- already unique and already durable, because that is what INV-TXN-09
   is for ([coordinator.cc](anvil/core/txn/coordinator.cc)).
5. **A blocked reader that found a committed blocker never resolved the
   intent in its way.** `resolve_blocker`'s aborted branch rolls back the
   intent it found; the committed branch just returned, so the same intent
   blocked every future reader forever unless the original owner's best-effort
   cleanup happened to succeed. Fixed by proposing the missing
   `kCommitIntent` there too ([coordinator.cc](anvil/core/txn/coordinator.cc)).
6. **The checker's own record identity was keyed by node, not by (node,
   range).** A record's primary key can move to a different range on the same
   node via a split, and comparing what the new home reports against what the
   old one last said reported a relocation as the record leaving a terminal
   state -- INV-TXN-02 fired on a transaction that never actually changed its
   mind. Fixed by resetting the tracked baseline when the reporting range
   changes, while still ignoring a different *replica* of the same tracked
   range (the trap INV-SHARD-04's first version fell into)
   ([txn_invariants.cc](anvil/checker/txn_invariants.cc)).

### Money and acknowledged writes going missing: two real causes found, fixed, and confirmed by direct experiment

The minimal reproduction the previous pass recommended was built (bank
workload only, `keys=4`, `neighbourhood=2`, three nodes, no fault injection)
and found a failing seed in under half a second of wall-clock time instead of
needing the full sweep. That, plus a debugger-style trace of the exact
requests and applies around the loss (not committed to the tree; see gotcha
10.17), found two independent, unrelated bugs, either one sufficient on its
own to lose an acknowledged commit with zero faults:

1. **A range's reply to a transactional command was matched to the wrong
   waiter.** `ShardStore::pending_txn_` was keyed by `(client << 32) | seq`
   and drained "oldest key first" on every applied `kTxn` command, on the
   reasoning that a range applies commands in the order it received them. That
   reasoning holds for one client; it does not hold for two. The key is the
   *client's* identity, not an insertion order, so `std::map`'s iteration order
   is client-id-major, not proposal-order -- under two coordinators contending
   for one range, an apply got paired with a different client's pending
   request, that client was told somebody else's result, and the request whose
   result was stolen sat out its RPC timeout having genuinely committed. From
   the coordinator's own reply cache that is indistinguishable from having
   never happened, which is exactly the `TxnOutcome::kUnknown` this surfaced
   as. Fixed by keying `pending_txn_` on `(range, the log index propose()
   actually assigned)` -- the one fact that ties a reply to the exact apply
   that decided it, with no ordering assumption at all
   ([store.h](anvil/core/shard/store.h), [store.cc](anvil/core/shard/store.cc)).
   Confirmed by direct experiment: a single-range repro (no splits at all)
   that lost money on essentially every seed was clean across 3000 seeds after
   this fix alone.
2. **A merge trigger carried only the bank half of the subsumed range's
   state.** `RangeMachine::payload()` -- documented, wrongly, as "the whole of
   this range's data, for a merge trigger" -- was `encode_payload(balances_,
   decided_)` with no `txn_` section at all, while the *split* path already
   used `encode_span()`, which does include it. The receiving side's
   `load_span(..., merge=true)` has an explicit backward-compatibility branch
   for "a payload written before P6 existed" (no trailing txn section), and a
   merge built from `payload()` hit that branch every time -- every version, live
   intent, and record on the subsumed side was silently discarded the moment
   the merge committed, with the coordinator that had been told those
   transactions committed none the wiser. Fixed by using `encode_span({}, {})`
   for the merge trigger too, and the now-dangerously-misleading `payload()`
   accessor was removed rather than left as a trap for the next caller
   ([range.h](anvil/core/shard/range.h), [store.cc](anvil/core/shard/store.cc)).
   Confirmed the same way: the split/merge repro that still lost money after
   fix 1 (with splits enabled) was clean across 3000 seeds after this fix too.

### Two checkers were measuring the wrong thing, and one of them was a lid

The `commit_index()`-goes-backwards hypothesis recorded here previously was
**wrong**, and disproving it produced the two findings that actually mattered.
`RaftLog::commit_to` is monotonic and `restore()` re-establishes the commit
index from the durable hard state, so the index never regresses within an
incarnation. What *does* happen is subtler and is the shared cause of both
findings below: the hard state is only as current as the last fsync, so a node
that had committed and applied index 4 in memory can crash and recover with the
disk saying commit = 2. It replays to exactly there and reports `applied >=
commit` -- true, and meaningless, because entries 3 and 4 are committed on the
cluster and it has not heard of them. **Every guard written as `applied >=
commit` is satisfied by a node that is genuinely behind.**

`RaftNode::can_serve_local_reads()` ([raft.h](anvil/core/raft/raft.h)) is the
condition that actually closes it -- leader, an entry from its own term
committed, and everything committed applied -- resting on Raft's leader
completeness rather than on an index comparison. Its header carries the full
argument.

3. **`INV-TXN-09` was a false positive, and the guard added for it in the
   previous pass was the wrong guard.** The observer compared each replica's
   oracle high-water mark against that replica's own past, skipping nodes that
   failed `applied >= commit` -- which, per the above, skips nothing on exactly
   the node that needs skipping. A replica's mark legitimately moves backwards
   across a crash in that window, and it is safe precisely because Raft will
   not elect a node missing committed entries, so it can never serve a
   reservation from the stale mark. Gating the comparison on
   `can_serve_local_reads()` keeps the property the invariant is named for and
   drops the reading that was never a violation
   ([txn_invariants.cc](anvil/checker/txn_invariants.cc)). INV-TXN-09 has been
   silent across the sweep since.
4. **`INV-SHARD-CLIENT` was a false positive too -- and it had been capping
   every P5 run on its seed since the phase was called done.** This is
   [ANV-0051](BUGS.md), the first ledger row of this pass, and it is bisectable
   because it reproduces on the committed tree at `de8f445`. The workload kept
   one high-water mark per range in *shared* state and asserted every read came
   back at or above it, comparing against the mark **at completion time**. Two
   clients read the same range, one is served at index 17 and the other at 18,
   the 18's reply arrives first, and the 17 is reported as a read going
   backwards -- though it was served before the 18 existed and no client ever
   saw an inversion. Real-time freshness constrains reads that are *ordered* in
   real time and says nothing about overlapping ones; the fix captures the bar
   at invocation ([shard_kv.cc](workloads/shard_kv.cc)).

   The expensive part is the second half. **The simulator stops at the first
   invariant violation** (`scheduler.cc`, `break` on fired). Seed 19 had been
   stopping at tick 4395 for the whole of P5, so nothing after that tick had
   ever run on that seed. The false positive was not noise, it was a lid.
   Confirmed by building the pristine tree at `de8f445` in a scratch worktree:
   it reproduces the violation exactly (985 acked, tick 4395), and the same
   tree with only the workload fix applied runs on and fails differently --
   see below.

### The convergence failures, and the S0 underneath them

Removing the two false positives exposed a real data-loss bug and a second
checker gap. **`shard_faults` is now fully green (20/20 seeds).**

5. **[ANV-0052](BUGS.md), S0: a range merged away while owing a child its
   data.** A range that applies a split trigger erases the right-hand half from
   itself and holds the only copy in `pending_split_`, waiting for the child's
   leader to collect it. That slot was a single `std::optional` (so a second
   split silently overwrote the first child's payload) and was not part of
   `encode_span()` (so it did not travel with a merge). When the parent was
   itself subsumed, the payload went with it and the child became permanently
   uninitialisable -- nothing held its span, no leader could propose its
   `kInit`, and its keys were gone while the topology still routed clients to
   it. On seed 19 that was r7 `[acct0010,acct0015)` at applied index 0 on all
   three replicas, five accounts short.

   Fixed by making it a map keyed by child id and carrying the entries whose
   keys fall in the span, which is correct for a split as well as a merge.
   Worth recording that **blocking the merge instead was tried first and
   livelocks**: the child cannot initialise, so the split never confirms, so
   the merge never unblocks. Two related latches were fixed alongside it --
   `init_proposed` and `freeze_proposed` were set once and never cleared, so a
   proposal truncated by a leadership change left the range waiting forever on
   an entry that no longer existed; both now re-arm once the log has applied
   past the index they were assigned.

6. **[ANV-0053](BUGS.md): the P6 audit could not see a split in flight.** The
   transactional audit read only live range machines, so a key sitting in a
   parent's pending-split payload counted as unreachable and as missing money.
   P5's `audit_conservation` has always walked pending splits; the P6 audit was
   written without that half, and this phase's topology churns continuously by
   design, so there is essentially always a split in flight when the audit
   runs. Disproved as an engine bug by measurement rather than argument: the
   same seed with a longer settle reported `total=1600/1600` at 40s, 60s, 90s
   and 200s while the range count kept moving. Money that reappears when you
   look later was never missing.

### Where P6 actually stands

Green: `shard_faults` 20/20, mechanism suite 30/30, shard units, determinism
3/3, `serializable` and `strict-serializable` 4/4 checker-clean, INV-TXN-09
silent.

Open, and now a short list of genuine findings rather than a fog:

- **Small conservation shortfalls on converged runs.** Seeds 1 and 7 at
  snapshot isolation finish `converged=yes` and short by 17 and 15 out of
  1600. Both are far smaller than an account, so this is a partial transfer
  rather than an unreachable range -- a different bug from the two above, and
  the first one to chase.

  **A plausible fix for these was tried and rejected; do not re-try it without
  reading this.** `audited_value` resolves a live intent by looking the owning
  record up in *the same range's* version store, and a cross-range
  transaction keeps its record on the primary's range -- so the lookup finds
  nothing for exactly the transactions this phase exists to exercise, and the
  audit falls back to the version from before the transaction. Making the
  lookup search the whole cluster does fix seeds 1 and 7 and silences the
  `parallel commit` control. It also **drops the seeded-mutation drill from
  7/7 to 3/7**: `no refresh on push`, `uncertain reads never restart`,
  `terminal status is not final` and `uncertainty never honoured` all stop
  being detected, because an audit that resolves every intent it can find
  reconstructs the total the engine *should* have had and hides the lost
  update the mutation caused. Adding the epoch check that
  `VersionStore::commit_intent` enforces (record epoch == intent epoch) did
  not recover the detection either. A checker that manufactures a pass is
  strictly worse than one that manufactures a failure, so this was reverted.
  The shortfall is real and the audit's blind spot is real; the fix has to be
  one that does not also blind the drill.
- **Seed 5 (serializable), 710/1600 with `converged=no`.** Much larger, and
  still correlated with convergence, so likely a third variant of a range not
  reachable at audit time rather than a transactional fault.
- **`INV-TXN-02` on seed 8 was a false positive too, and is fixed.** A crash
  rebuilds a `RangeMachine` at applied index 0 and replays its log into it, so
  for the width of that replay the replica genuinely holds an earlier version
  of every record it owns -- and the mirror compared it against what the same
  replica reported before the crash, grading recovery as a transaction changing
  its verdict. The guard is deliberately *not* a baseline reset (that was tried
  and it blinds the invariant): the remembered verdict survives the replay, and
  a record that settles on a different one once the replica is back at or past
  the index where the verdict was recorded is still caught.

  Worth writing down separately: **INV-TXN-02 has never once caught its own
  mutation.** `terminal status is not final` is detected 2/10, both times by
  the bank's conserved total and never by the invariant armed for exactly it.
  Every firing of INV-TXN-02 in this phase's history has been a false positive.
  An invariant evaluated 143,953 times per seed that has never made a true
  detection is not yet pulling its weight, and that is a finding about the
  checker rather than about the engine.
- **Two drill controls still fire** (`parallel commit`, `no commit-wait`).
  Both are configuration changes rather than bugs, so a firing control means
  the harness is attributing one of the above failures to the mutation. Expect
  these to go quiet as the findings above are fixed; re-check rather than
  assume.
7. **The list-append element loss was the audit again -- routing by a stale
   descriptor.** Seed 1 lost 14 acknowledged elements. The keys were all in
   two ranges the topology still listed as `FROZEN` with no replica anywhere,
   while the survivor's machine held eleven keys -- well beyond its own
   topology span. The merges had happened; only the descriptor removal had
   not. The audit asked the topology who owned each key, got a range that had
   been retired on every node, and reported everything ever written to it as
   lost. `holder_of` now asks the *machines* which one claims the key, by its
   own applied descriptor, which is the same two-halves-of-one-fact trap
   ANV-0042 records on the serving path and which P5's audit already walks
   around. That fixed seed 1's element loss, seed 5's 710/1600, and silenced
   the `no commit-wait` control.

   It also **surfaced an anomaly that the loss had been masking**: three
   duplicated elements on the same seed, which the Elle checker names
   precisely (`element ... on key 11 appended by both T6 and T24`). That became
   finding 8.

8. **The duplicated elements were the generator, not the engine — and gotcha
   10.25 for the third time this phase.** This is [ANV-0055](BUGS.md). An
   element id was `(node << 40) | ++element_counter`, and `element_counter` is
   a local of the `client_loop` coroutine. `boot_node` spawns that coroutine
   and `ProcessModel::crash` calls `destroy_tasks_for(node)`, so a crash
   destroys the counter and the next incarnation starts at zero and reissues
   ids the previous one had already had acknowledged.

   Confirmed by instrumenting the draw site rather than by reasoning about it:

   ```
   TRACE draw node=5 hid=6  boots=1 first=5497558138885
   TRACE draw node=5 hid=24 boots=2 first=5497558138885
   ```

   T6 and T24 are two unrelated transactions, one per incarnation, drawing the
   same number — precisely what the checker reported. And the checker was
   right to report it: [history.h](anvil/checker/history.h) states the
   precondition and both of its readings, "either the generator is broken or
   the database applied a write twice", which from a history alone are
   indistinguishable. Note this cuts the other way too — an element id that is
   unique cannot *hide* a double apply, because a doubly-applied append still
   puts the same element in the list twice under a single transaction. So the
   fix removes the collision without removing any detection.

   The id now carries the incarnation `ProcessModel` already tracks, and — the
   half that will matter next time — the workload records every id it hands out
   and reports a collision **as a harness failure in the harness's own words**.
   A generator bug that reaches the checker arrives wearing a database bug's
   clothes, and this phase has now spent four findings on false positives that
   looked exactly like real ones. Snapshot isolation went 3/4 → **4/4
   checker-clean**, with the drill unchanged mutation-for-mutation, which is
   what says the fix is orthogonal rather than a blinding.

### Closing P6: the last four findings, and what the drill was actually measuring

Everything above left four things open. All four are closed, and three of them
turned out to be the same sentence written in different places.

**9. The audit ranked claimants by applied index, across two Raft logs.** This
is [ANV-0059]. `holder_of` asked every live replica whether its descriptor
contained the key and took the highest applied index among them. An applied
index numbers entries in *one range's own log*; comparing two of them across two
ranges compares two unrelated counters -- gotcha 10.23 with the two consumers
being two logs rather than a log and a state machine. Seed 10 lost two
acknowledged elements to it:

```
node 1  range 8 [key00010,) gen=1 applied=78
           V key00013  ts=333[1125912791744546]
           V key00014  ts=333[1125912791744545]
node 2  range 3 [,)         gen=6 applied=141      <- the audit asked this one
           (nothing for key00013 or key00014)
```

141 > 78, so the audit read a replica that had not yet learned it no longer
owned the key. What *is* comparable is a descriptor generation within one range:
it is bumped by every split and merge that range takes part in, so each range now
speaks with its newest descriptor and only then is the key matched against it.
Applied index survives as the tiebreak among replicas *of the same range*, which
is the one place it means anything. Two live ranges claiming one key is now
counted and reported (`ambiguous_keys`) rather than resolved by a coin flip.

The same row carries its mirror image, which is the more useful half. The audit
had two ways to read a key -- through a range machine, where `audited_value`
resolves a committed intent the way a client's reader would, and through a split
payload, where `committed_value` did not. A cross-range transfer caught across
that seam had its credit counted and its debit skipped, and seed 7 finished
**fourteen over** its starting total. An audit that finds money is exactly as
broken as one that loses it, and the fix was not to teach the second branch the
same trick but to stop having two branches: one `owning_store(key)` now answers
with a live range's store or with a payload, and everything reads through it.

**10. A heartbeat narrowed a staging record's key list, and parallel commit's
predicate then answered yes about one key.** This is [ANV-0060], the phase's
second S0 and the one the `parallel commit` control was pointing at all along.
Every record write carries the primary in `keys`, unconditionally, because that
is how a record is located when a span is partitioned. A heartbeat is such a
write -- and `put_record` merged it as though it were a declaration:

```cpp
if (!record.keys.empty()) current.keys = record.keys;   // before
```

A kPending heartbeat does not overwrite the status, so the record stayed
kStaging with its key list cut down to `[primary]`. Since a staging record is
committed exactly when every key it lists carries its intent, that record
satisfied its own predicate the moment its primary was prewritten -- before the
other intents, before the commit timestamp, before the refresh that might yet
abort it. Seed 12: `R 164 kStaging keys=[key00009] commit=0`, one intent, other
half never written, eight units of the bank's total gone. The key list now moves
with the status, inside the branch that already knows a kPending write carries
no verdict.

INV-TXN-11 was armed for exactly this and watched it happen 143,000 times a seed
without a word, because it checked for an *empty* key list. An empty list is the
loud way for a record to be undecidable; a narrowed one is the quiet way, and it
is strictly worse -- the predicate still evaluates, over fewer keys than the
transaction wrote, and comes out committed. It now reports any intent at the
record's epoch on a key the record does not list.

**11. The retry that went back to the node which had just refused.** This is
[ANV-0061], and it is the reason everything else in the drill was unmeasurable.
Every reply carries a `leader_hint`; `on_reply` copied it into the response and
no caller read it. `send` picked its destination from the cached descriptor
every time, so all eight attempts went to the replica that had, each time, named
the alternative. Seed 1: **1,986 not-leader rejections against 321 transactions
begun, of which five committed.** Gotcha 10.12 with the repeated half being the
destination rather than the operation.

The fix took the suite from 108 committed to 147 and `lost update` from 11/15 to
15/15 with nothing about that mutation or its detector changed. **Detection rate
is a function of throughput, and a throughput bug reads as a detection gap** --
which is what four of the drill's rows had been reporting.

**12. And the drill itself was measuring the wrong thing in three more ways.**
[ANV-0057] added the null control and found that five of nine rows were scoring
the harness's own noise. Finishing the job needed three more corrections, each
of which is a general point:

*A cell is a configuration, not a (level, workload) pair.* Two rows switch off a
mechanism that a **second mechanism makes redundant**. Spanner waits out the
clock bound at commit; CockroachDB restarts a read that lands inside it. Anvil
does both, so either alone is sufficient and removing either alone is an
equivalent mutant. Measured in both directions rather than argued:

| configuration | detections |
|---|---|
| commit-wait on, uncertainty restart off | 0 / 40 seeds |
| commit-wait off, uncertainty restart on | 0 / 20 seeds |
| both off | real-time violations, 5/20 and 3/20 |

So the two uncertainty rows now run in a cell where commit-wait is off, and
`no commit-wait` is that cell's null control -- a control and a finding at once.

*A mechanism has to be reachable before a row about it means anything.* The
uncertainty window is `(start_ts, start_ts + bound]`, and at the bound
`FaultProfile::from_seed` draws (1--250 ms), across a run that commits about
eight transactions, it is never once occupied: `restarts_uncertain` is exactly
zero. Declaring a wider bound on its own does **not** fix it, and the first
attempt did exactly that and looked like it had -- 98 restarts across 15 seeds
and still no detection -- because `max_offset` is *derived* from
`declared_uncertainty` in `from_seed`. Raising only the declaration widens the
window without moving the clocks, so almost every version inside it genuinely is
in the future and skipping it is genuinely correct. The window was occupied and
vacuous at the same time. Both are now scaled together.

*A checker starved of observations reports VALID for the same reason an empty
history does.* `no refresh on push` puts the mechanism squarely under load --
sixty refresh failures a seed, five transactions per seed committing without one
-- and produced nothing in 25 seeds. Two reasons, both structural. Elle recovers
a key's version order from the reads that observed it, and with sixteen keys and
ten commits a seed the graph came out with **two edges in it**; the same run at
four keys builds twenty-five. And an anti-dependency in one direction is not a
cycle: the reciprocal write-skew pair has to happen concurrently on the same pair
of keys, which random draws will not deliver. The cell now states the two-doctors
shape instead of sampling for it, and the row detects G2-item on 4/15 seeds.

There is now also a **settle-phase reader**: one read-only transaction per key
after the heal, recorded in the history. It was independently required by three
things -- Elle's version-order recovery, the lazy intent resolution that has
nobody to trigger it once the writers stop, and the fact that nothing otherwise
exercises the serving path after a heal. The edge count in a 55-transaction
history went from single digits to 131.

### Where P6 stands

**Green, and P6 is complete.** `anvil_txn_faults 30` passes end to end:

```
under faults (3 levels x 10 seeds): 145 committed, 949 aborted,
  173 cross-range transactions, 112m simulated node-time
snapshot-isolation ....... 10/10 seeds checker-clean
serializable ............. 10/10
strict-serializable ...... 10/10
determinism .............. 3/3 seeds reproduce exactly
seeded-mutation drill .... 6/6 deliberate bugs detected above their cell's
                           null control; all five null controls silent;
                           all three configuration controls silent
```

`shard_faults` 20/20, `mvcc_faults`, `checker_mutation` (P7's first gate) and
every unit suite green beside it.

Two things are deliberately *not* claimed, and both are written down rather than
buried:

- **`secondaries before primary` is a control, not a must-detect.** The knob's
  own comment used to describe Percolator, where the primary lock *is* the
  commit record and writing it last leaves intents nobody can resolve. That is
  not this design: the record is a separate object and `commit()` writes it
  before any prewrite at all, whichever order the flag selects, so the window
  the comment describes never opens. What the flag reorders is the intents among
  themselves. It is kept as a control because the reordering must stay harmless.
- **The staging window opens earlier than parallel commit's textbook version.**
  The record goes to kStaging *before* the prewrites and with `commit_ts = 0`;
  CockroachDB's STAGING record carries the provisional commit timestamp and is
  written last. With the key list fixed the predicate is sound, but a
  coordinator can still write kAborted over a staging record whose intents are
  all present -- if the fresh timestamp draw fails, or if the read refresh does
  at serializable. No seed has produced a divergence from it. The measurement
  that would is a recovering reader racing an aborting coordinator, and it is
  the first thing to build if parallel commit is ever taken further.

---

## 15. P7 (verification depth): what was built, and what it found

P7 is the phase that asks whether the previous six phases' evidence is worth
anything. Every gate before it is Anvil checking Anvil — our corpus against our
checker, our model against our invariants — and that arrangement can catch a
mistake made once but not a mistake made consistently. So this phase builds
four instruments that are *not* the fault sweep, and the interesting result is
that three of them immediately found something.

### The shape of the phase

| # | Criterion | Instrument | State |
|---|---|---|---|
| 1 | checker mutation score 100%, zero false positives | `checker.mutation` | **met** |
| 2 | zero unexplained disagreements with Jepsen Elle | `tools/elle_cross.sh` | **met** |
| 3 | TLC finds no violations; trace validation conforms | `tools/tlc.sh`, `tools/trace_validate.sh` | **met** |
| 4 | DPOR covers its configuration class; state count reported | `verification.dpor` | **met** |
| 5 | minimiser reduces ≥10 faults to ≤3 in <5 min | `verification.minimiser` | **met** |

### What each one is

**The minimiser** (`anvil/sim/minimiser.h`, `test/minimiser_test.cc`) is Zeller's
ddmin over a set of *fault features* — one entry per knob the profile can arm,
plus BUGGIFY. The empty subset is exactly `FaultProfile::none()`, so "minimised
to nothing" and "the codebase's own control condition" are the same
configuration rather than two things that look alike.

The honest caveat is in the header and it determines how to read every result:
ddmin's 1-minimality guarantee assumes a deterministic predicate, and this one is
not. Disabling a fault does not remove an event from a fixed schedule; it changes
which dice are rolled, so the whole execution downstream diverges. Hence
`attempts` (several schedule seeds per candidate) and
`verified_one_minimal` (the closing check actually ran and every kept feature was
individually shown to be load-bearing), both reported rather than assumed.

It is graded against causes known in advance rather than against a smaller
number: `fsync_before_ack = false` acknowledges a write that is only in the page
cache, and in this model a crash is the only thing that takes the page cache
away, so the answer *must* be exactly `{process.crash}`. It is. And because a
search that had learned to answer "process.crash" would pass all of that, a third
case chases detected corruption instead, with no planted bug at all — and gets
`{disk.bit_rot, process.crash}`, which is the right pair: bit rot damages bytes
already written, and a restart is what makes the checksum fire on replay.

**The state-space search** (`anvil/sim/dpor.h`, `test/dpor_test.cc`) is a model
checker over `anvil/core/raft/raft.h` itself — the shipping state machine, no
simulator underneath. That is only possible because P3 made it a pure state
machine whose only output is a `Ready` batch; a model checker needs exactly that,
so this deliverable cost a few hundred lines rather than a rewrite.

It ships two searches, and the second is graded against the first: full
reachable-state enumeration with fingerprint deduplication (the ground truth),
and the same search under sleep-set partial-order reduction. **The reduction is
validated, not asserted** — both must report the same terminal states, which is
the same discipline as the hermeticity gate's negative control.

**The TLA+ specs** (`spec/Raft.tla`, `spec/SsiCommit.tla`) are specifications of
the safety *argument*, not transcriptions of the code. A spec that mirrors an
implementation line by line proves nothing, because it inherits the
implementation's mistakes.

**The Elle cross-validation** (`test/elle_export.cc`, `tools/elle/`) is the only
gate in the tree that is not self-referential. Anvil emits histories in Jepsen's
own format with its verdict attached; a Clojure harness runs Jepsen's Elle over
the same histories and compares.

### The results

```
checker mutation ....... 200/200 detected and correctly named across 9 anomaly
                         classes; 0 false positives over 10,000 reference-model
                         histories at all 5 levels; 120/120 discrimination pairs

elle cross-validation .. 10,000 shared histories (5,000 correct, 5,000
                         anomalous); 10,000/10,000 verdicts identical;
                         10,000/10,000 same anomaly class; 0 disagreements

state-space search ..... 2,202,433 distinct states, 5,545,749 transitions,
                         5,530 terminal states, depth 78, complete, 0 violations
                         (3 voters, 2 proposals/node, 2 ticks/node, ≤4 in flight)
                         drill: 3/3 must-detect caught, 5 equivalent-in-class
                         with a written argument each
reduction .............. 382/382 terminal states identical to the exhaustive
                         search over the same class, 128,400 edges pruned

minimiser .............. 11 armed features → 1 (`process.crash`), 1-minimality
                         verified, 15 predicate runs, 0.016s; a second failure
                         on the same workload minimises to a different cause

TLA+ ................... 9 configurations, five required clean and four
                         required to fail on a *named* property
trace validation ....... 16/16 runs of anvil::raft::RaftNode permitted by
                         spec/Raft.tla; 12/16 runs of a deliberately broken
                         implementation correctly rejected, the other 4
                         equivalent on their seed and named
```

### Three findings, and all three came from disagreement rather than from failure

**[ANV-0063]: the "exhaustive" search was visiting one state in fifteen.** The
state fingerprint was assembled from `RaftNode`'s public accessors, which omit
the leader's replication progress, the election timers, the randomised timeout,
the vote tally and the generator's own position — every one of which decides what
the node does next. Two states differing only in those were merged, the second
never expanded, and everything reachable only through it was silently outside the
search. On the small class, 1,674 distinct states became 22,989 once the
fingerprint was complete.

It could not have been found by either search alone: both reported no violations
and both were wrong. What was diagnostic is that the *reduced* search reached a
terminal state the exhaustive one had never visited, which is impossible if the
exhaustive search is exhaustive.

The fix is `RaftNode::state_digest()`, now the single definition of what a node's
state is. It is a pure observation — records nothing, changes nothing, never
consulted by the protocol — which is the distinction §10.26 draws between a
checker reading state and a state machine carrying evidence for its checker.

**[ANV-0064]: the independence relation was a claim about the encoding, not
about Raft.** Messages lived in one list numbered from one counter, and
`proposals_used` was one counter shared by every node. Two nodes stepping in
either order therefore reached states the fingerprint distinguished, so
"transitions of different nodes commute" was false and the reduction pruned real
states — 42 terminal states against the exhaustive search's 407. Per-link queues
with per-link sequence numbers and a per-node proposal budget fix it at the root.

A third instance of the same idea turned up in the *search* rather than the
state, and it is not a bug: **a bound on messages in flight is not invariant
under commutation**, because two orders ending in the same state pass through
different peaks, so one can be cut off at the bound while the other survives.
That is a constraint on where the comparison is valid, and the test enforces it
by asserting the bound never binds in the class the two searches are compared
over.

**[ANV-0065]: TLC produced the divergence P6 predicted and no seed had ever
reached.** Under parallel commit at serializable, a recovering reader evaluates
the implicit-commit predicate — every key the staging record lists carries its
intent — and commits the transaction. That path never performs the read refresh,
which the coordinator's own commit path does and which would have aborted the
transaction. Whichever gets there first decides, and one of the two outcomes is
write skew in a history the engine calls serializable.

The end of P6 names this exactly: *"a coordinator can still write kAborted over a
staging record whose intents are all present... the measurement that would show
it is a recovering reader racing an aborting coordinator, and it is the first
thing to build if parallel commit is ever taken further."* No seed produced it,
because the race is one interleaving of two transactions over two keys and the
sweep is sampling a far larger space. TLC produced it in one second and 10,881
states. `parallel_commit` is off by default, so nothing ships with it; the
finding is that the mechanism is unsound at serializable *as designed*.

### What the specifications cost, which is the part worth reading

Writing a spec that is *wrong* is easy and the failure mode is specific: it
produces a violation that looks exactly like the system being broken. Three
happened here, in one afternoon, and each took a real counterexample to see:

1. **Configuration as a global variable.** The first `Raft.tla` let any leader
   change the membership atomically. TLC immediately produced two leaders in one
   term — one leader had shrunk the cluster out from under the other. In Raft a
   configuration change is a *log entry*, and each server's configuration is
   derived from its own log, which is also what makes truncation revert it for
   free.
2. **Leader Completeness keyed on the wrong term.** The property guarded on the
   entry's term rather than on the term the commit *decision* was made in. Those
   differ exactly when an old entry becomes committed under a later leader —
   which is the Figure-8 situation — and the wrong version demands that a leader
   of term 3 hold an entry nobody committed until term 4. Raft does not promise
   that.
3. **A follower trusting `mcommit` over its whole log.** Clamping the follower's
   new commit index to `Len(log)` rather than to the last index *this message
   verified* lets a follower with a divergent tail record entries as committed
   that the leader has never had. StateMachineSafety fails, and it reads as a
   protocol bug.

All three are the same lesson the ledger keeps recording about checkers, in a
new notation: **the model was wrong in a way that looked exactly like the system
being wrong.** Roughly a third of this project's findings have been the harness
rather than the engine, and specifications are not exempt.

There is a fourth, smaller, and worth knowing: `x' = a \/ b` parses as
`(x' = a) \/ b`, because `=` binds tighter than `\/`. TLC reports it as
"successor state is not completely specified", which is a far better error than
the silent one it would be in most languages.

### And what the *searches* cost, which is a different lesson

Three separate times, a search that "could not find" something was not searching
too shallowly — it was searching a space made needlessly large.

The five-server Figure-8 counterexample is about fifteen steps deep. Breadth-first
search was still at depth 14 after ten minutes and 137 million states; random
simulation checked 112 million states without reaching it. Neither was the
answer. What worked was **removing branching**: switching heartbeats off and
restricting who may campaign — both named constants, both documented in the
`.cfg` — took that run from "not found in 112 million states" to "found in
11,746". Depth was never the problem.

The same applies to the main `Raft.cfg` run. Making handled messages actually be
*removed* (the first version left them in the set, which made `MaxMessages` bound
the total a behaviour may ever send rather than the number in flight) was
necessary for correctness and multiplied the state space; symmetry reduction over
`Server` bought a factor of about six back, and it is sound only because that
configuration has no `ConfigChoices` — a configuration change names particular
servers and destroys the symmetry. That is why joint consensus lives in
`RaftJoint.cfg` and not in `Raft.cfg`.

### Where P7 stands

**All five criteria are met**, and each is produced by something in this tree.

Criterion 3 has two halves and both landed:

- Both specifications exist, TLC model-checks them exhaustively over stated
  slices, and every configuration is graded — five required clean and four
  required to *fail on a named property*, because a specification whose
  properties have only ever been observed to hold is indistinguishable from one
  whose properties are vacuous (ANV-0005, in a new notation).
- **Trace validation** replays runs of `anvil::raft::RaftNode` against
  `spec/Raft.tla`: 16/16 conform, and 12/16 runs of a deliberately broken
  implementation are correctly rejected (the other four are seeds on which the
  mutation never had an opportunity to manifest, counted and named).

### What building trace validation cost, and why it was worth it

Seven discrepancies, every one of them real, and none visible by reading either
artefact:

*Three in the exporter's mirror of the specification's variables.* A candidate
re-campaigning at a higher term lost its own vote. A vote arriving at a node that
had already won was credited anyway, where the specification credits votes only
to candidates — and the fix has to test the role *before* the transition, because
the winning vote is consumed by a node that is a leader by the time anything
looks. And already-committed entries were re-recorded with whatever term the node
happened to hold, inventing commit decisions nobody made.

*Three in the specification.* `committedLog` had the same re-recording bug, on
both paths. `StepDown` was missing entirely — the implementation abandons an
election once a quorum has rejected it, which shortens every split-vote round by
a full timeout, and the specification had no action for it. And an
`AppendEntriesResponse` disagreed about `mmatch` on rejection.

*One genuine design difference*, and the most interesting of the seven: both
sides clamp the commit index a heartbeat advertises, and they clamp it in
different places. The implementation clamps at the **sender** —
`broadcast_heartbeat` advertises `min(pr.match, commit_index)`, never telling a
follower to apply what it is not known to hold. The specification clamps at the
**receiver** — `min(mcommit, verified)`, where `verified` is the prefix that
message actually checked. Both are sound. They are not the same mechanism, and
reconciling them needs a `matchIndex` variable the specification deliberately
does not have. So heartbeats are out of scope for the replay, and what is
validated is elections, replication and the commit rule.

That last one is the shape of finding this deliverable exists to produce: the two
artefacts agree on *what* must hold and differ on *where* it is enforced, which
no amount of reading either one would have surfaced.

None of the seven is a BUGS.md row, and the reason is the ledger's own rule
rather than modesty: a row needs a commit to bisect against, and every one of
them is in code that did not exist before this phase — the specification, the
exporter, or both. They are recorded here for the same reason P6's pre-commit
fixes are recorded in §14. The three findings that *do* have rows (ANV-0063,
ANV-0064, ANV-0065) are the ones about code or designs that predate them.

One more, smaller and purely mechanical: a use-after-free in the exporter's
driver. `message_of()` returns a pointer into the link's deque and `fire()` pops
that deque, so a pointer taken before the transition dangled during the
observation after it. It read plausible bytes rather than crashing, and the
symptom was a candidate becoming leader with one vote instead of two, sixty steps
before the replay actually stalled. CONTEXT.md gotcha 10.14 with `fire()` in the
place of the suspension point.

Long-standing blockers are unchanged: `anvil/prod/` is still an empty target,
the cross-toolchain digest gate has still never run, and **ANV-0033** — a run's
outcome depending on sixteen bytes of environment variable — is still the single
highest-value thing to do with a Linux box.

---

## 16. P8 (the bug hunt at scale): started, and what it has produced

**Status: in progress.** Two of six deliverables are built and one exit
criterion is partly met. This section says which, and what the rest would take,
because a phase claimed complete on a third of its deliverables is worth less
than a phase honestly reported as a third done.

P8's goal is one sentence: **turn compute into ledger rows.** Every phase before
it built an instrument. This one runs them, at volume, and does the three things
that decide whether a large fleet is useful or merely expensive.

### What is built

**The seed fleet** (`test/fleet.cc`, `tools/fleet.sh`, `tools/fleet_report.py`).
One process per core, each taking `seed % shards`, each writing its own JSONL: no
locking, no scheduler, and a shard that dies takes nothing with it. Five
workloads per seed — counter, raft_kv, mvcc, shard_kv, txn_bank.

Three properties, and the second is the one that makes the output usable:

1. It reports **simulated node-hours**, not wall clock. A fleet that has run for
   eight hours has said nothing until you know how much cluster-time that bought.
2. It **minimises the fault set of every failure** before recording it, through
   `anvil/sim/minimiser.h`. This is what P7's criterion 5 was for.
3. It **deduplicates by (workload, invariant, minimised signature)** and
   separates failures that are already classified from ones that are not. The
   first thing a fleet produces at volume is four hundred copies of one bug.

The fleet *records*; it does not assert. A seed that violates an invariant is a
candidate, and roughly a third of this project's candidates have been the harness
rather than the engine. The one thing it does assert is its own premise: a
workload failing on essentially every seed is a broken harness, not a discovery,
and the report says so rather than filing two thousand rows.

**The unified seeded-mutation report** (`test/drill_report.h`,
`tools/mutation_report.py`). Every phase's drill, in one table, with detection
rate, the by-invariant column and — the one that matters — **API visibility**.
Each suite keeps its own drill and its own human-readable table and gains one
machine-readable `DRILL|` line per row. Moving the drills into a single binary
would have meant six harnesses reimplemented in a seventh, which is how a report
ends up measuring something other than what ships.

### What it found

**240 seeds, 982 runs, 177.8 simulated node-hours**, at roughly 250x simulated
node-time per core. Nine distinct failure classes: two already understood, seven
new candidates. Per workload, the failure rates are worth reading as a group --
mvcc, shard_kv and txn_bank at 0%, counter at 7.0% and raft_kv at 4.1% -- because
a fleet where everything fails is a broken harness and a fleet where nothing does
is a fleet that is not looking hard enough.

The two understood classes, each with its argument in
`tools/fleet_report.py`'s table:

- **counter / INV-CTR-01 under `disk.bit_rot + process.crash`** (14 seeds, all
  1-minimality verified). A single-replica write-ahead log cannot survive a
  flipped bit in a record it already wrote -- there is nowhere to recover it
  from -- and what it must do is *notice*, which the checksum does.
- **raft_kv / INV-RAFT-13 under a signature containing a clock fault.** A lease
  is an optimisation licensed by a clock bound, and the fault profile can
  deliberately exceed the bound it declares. `raft_faults.cc` classifies these
  the same way.

The seven new classes are all raft_kv: four `INV-RAFT-09` (leader completeness)
and three `INV-RAFT-14` (a stale linearizable read), with minimal sets as sharp
as `process.crash` alone -- 1-minimality verified on six of the seven.

**One is filed in detail; the other six are a triage queue, not six ledger
rows.** [ANV-0066](../BUGS.md) is the one with the most information in it, and
the reason it is the one written up is that its minimal set *rules out* the
standard explanations. Filing six more unclassified rows would be filing noise
into the artifact whose whole value is that every row has been thought about; the
fleet's own output is the queue, and it is regenerable by one command.

Worth stating plainly: **the fleet's classification is deliberately narrower than
each suite's.** `raft_faults.cc` knows things the fleet does not, because the
fleet runs the workloads rather than the suites. So "seven new" is an
over-approximation -- which is the right direction for a fleet to be wrong in,
and reconciling the two is the next task in this phase.

### Since then: BUGGIFY plumbing, coverage, ledger-seed verification, mixed-version

This section is not rewritten wholesale here because [docs/ROADMAP.md](docs/ROADMAP.md)'s
own P8 table is now the more current account of criteria 1-6's numbers; this is
the one addition worth a full write-up, because it is the phase's most
consequential finding so far and the mechanism did not exist anywhere else in
this document.

**[ANV-0067](../BUGS.md).** `test/mixed_version_faults.cc` (P8 exit criterion
5) rolls a cluster from `pre_vote=false` to `pre_vote=true` and back --
modelled through `workloads::RaftKvConfig::node_raft_options`, a per-node
`RaftOptions` override consulted on every boot including a restart, which is
what makes "node N is now running the new binary" representable without a
wire-version field (`RaftDriver` already takes independent options per node;
nothing stopped a heterogeneous cluster from being constructible, only nothing
had ever tried). The schedule itself has to be driven from host code between
`simulation.run_more()` steps rather than from a spawned per-node coroutine --
gotcha 10.28, found by a real use-after-free.

Accumulating this criterion's 200 node-hours (1,830 seeds) failed 267 of them
(14.6%) against nearly every `INV-RAFT-*` invariant, against a P3 baseline of
zero in 1,000+. The pinned first-hit seed (24, `n2` votes for itself and then
for `n4` in the same term) was minimised with a new `--minimise` mode on the
suite binary -- the same `anvil/sim/minimiser.h` ddmin [ANV-0066](../BUGS.md)
used, adapted to hold the deliberate upgrade schedule fixed across every
candidate and vary only the seed's own network/disk/clock draw, since the
schedule is the thing under test rather than part of the adversary. 11 fault
features reduce to 3, 1-minimality verified: `partition + disk.bit_rot +
clock.jump`.

`disk.bit_rot` being load-bearing, and not `process.crash`, is what turned this
from "the mixed-version workload found something" into a specific mechanism --
gotcha 10.30 has the full argument. In short: the Raft hard-state file is
recovered the same way the LSM WAL is, by reading until the first invalid
record and dropping everything after as the tail, which is exactly right for a
torn write (which can only ever damage unsynced data) and not obviously right
for bit rot (which corrupts a record that already passed its checksum once,
anywhere in the file). An interior bit-rot hit on a node's hard-state file can
silently roll its recovered vote back below a vote it already, durably, replied
to a peer with -- and `anvil/checker/raft_invariants.cc`'s own asymmetry (it
exempts a `corrupted` node's term/commit regressions but never its vote
conflicts) turns out to be the checker already getting this right rather than
missing a case: a bit-rotted vote record was relied on elsewhere in a way an
unsynced one never could be, so forgiving it would forgive the one regression
that is not actually invisible to the cluster.

**Fixed, partially, same session.** `anvil/core/lsm/wal.h`/`.cc` gained
`WalReadResult::discarded_had_more_data`, and `RaftStorage::recover()` now
refuses to recover (`StatusCode::kCorruption`) rather than silently rolling
back a vote when it is set on the state file -- see gotcha 10.30 for the full
mechanism and why the discriminator is sound. Verified: the pinned seed no
longer reproduces INV-RAFT-07 on the patched binary.

That verification also surfaced an unrelated harness bug: `workloads/
raft_kv.cc`'s `audit_durability()`/`converged()` excluded a crashed node
(`!process().alive()`) but not a node stuck in the new permanent recovery-
retry loop, which is process-alive without ever having called
`node_.restore()` -- its default-constructed, never-populated machine was
being audited as a caught-up participant and reported as missing every
acknowledged write. Both functions now also check `RaftDriver::ready()`.

Raised to plain **S0**, not because the mechanism is more certain in
isolation, but because of what fixing it revealed: a 300-seed before/after
run shows the fix's own side effect is *more* seeds exhibiting INV-RAFT-07
(12 -> 25 distinct seeds), not fewer -- a schedule-neutral, targeted, more-
conservative-only recovery fix does not do that to a harness artifact. It
does exactly that to a real vulnerability that a lucky original schedule was
undercounting. A second sub-mechanism -- bit rot on the file's true last
record, which no signal in this format can tell apart from an honest torn
write -- remains open and is what the 15 newly-exposed seeds mostly land on
(seed 19 minimises to `[disk.bit_rot]` alone and confirms it). Gotcha 10.30
and BUGS.md's ANV-0067 parts 2-3 have the complete argument, including the
number that almost got reported the other way.

Regression-testing the fix outside `mixed_version_faults` is what caught
[ANV-0068](../BUGS.md): `shard_faults 24`, previously clean, now shows two
real account-conservation violations. Not a flaw in ANV-0067's fix -- a
pre-existing gap in shard's dead-replica replacement (node-level heartbeats
only, no per-range-replica liveness) that the fix's new "this replica can
never come back" outcome is the first thing in this tree ever able to reach.
Gotcha 10.31 has the argument; both rows are filed rather than the fix
reverted to make the second one stop being visible.

### What the very first run found, twenty-four seeds in

Two distinct failure classes, one of them already understood and one not:

- **Understood**: the counter workload's `INV-CTR-01` under
  `disk.bit_rot + process.crash`. A single-replica write-ahead log cannot survive
  a flipped bit in a record it already wrote — there is nowhere to recover it
  from — and what it must do is *notice*, which the checksum does. The
  classification table in `tools/fleet_report.py` carries that argument next to
  the entry, and the rule for adding to it is that an entry needs an argument
  which would convince somebody who wanted to believe it was a bug.

- **New**: [ANV-0066](../BUGS.md). `INV-RAFT-14` on raft_kv seed 21 —
  a linearizable read returned index 12 after a write acknowledged at index 31.
  The minimiser reduced the eleven fault features that seed draws to exactly
  **two**, `partition + disk.slow_io`, and verified 1-minimality: each of the
  other nine was individually shown not to be needed, at a cost of 45 simulation
  runs. Deterministic — two invocations produce identical event counts, detail
  and signature.

  The *absence* from the minimal set is the informative half. No clock skew and
  no clock-bound violation, so the standing classification for stale lease reads
  does not apply. No crash and no bit rot, so the corruption bucket does not
  either. Partition plus slow I/O is a configuration a production cluster is in
  on an ordinary afternoon.

  It is filed **open and unclassified**, because this project has produced this
  exact symptom from the harness twice (ANV-0022, ANV-0041) and from the engine
  never. Guessing would be the wrong move; the row names both readings and the
  reproduction is one command.

That is the argument for wiring the minimiser into the fleet rather than filing
raw seeds, and it is worth stating as a number: eleven features to two, for 45
runs. "Reproduces under seed 21 with everything on" is a row nobody picks up.

### The merged report immediately over-claimed, on the one column where that is fatal

Three P6 rows came back listed as caught-but-invisible-from-the-API. They were
caught by the Elle-style consistency checker, which analyses the **client's own
history** and nothing else -- so an anomaly it names is by construction something
an outside-in checker could have found. `client_safe()` in the txn drill covered
the bank's conserved total and the list's element accounting and left the
checker's verdict out.

That column is the entire evidence for the protocol-aware argument, and the
README says overstating it is the fastest way to lose credibility. Merging six
drills into one table is what made the mistake visible: in the P6 suite alone,
"caught by G2-item" sat next to a table full of internal invariant names and
looked like one of them.

### One mistake worth recording, because it would recur

The fleet's first version handed the minimiser a *fresh* schedule seed for its
first attempt, on the reasoning that varying the schedule is exactly what the
minimiser's own header asks for. It is — for attempts *after* the first.
Starting anywhere other than the schedule that actually failed throws away the
only run known to reproduce, and ANV-0066 came back as eleven features with
1-minimality unverified, which is precisely what a predicate that never
reproduces anything looks like. Attempt 0 is now the original run.

And a smaller one: a fleet's runs are bounded in *simulated* time, which says
nothing about how long they take. One seed spent seven wall-minutes inside a
single raft_kv simulation — legitimate, just expensive — while eleven cores sat
finished. Shards now have a wall-clock budget and keep everything they flushed.

### The exit criteria, honestly

| # | Criterion | State |
|---|---|---|
| 1 | ≥ 20,000 simulated node-hours, reported honestly | **partly** — the fleet reports node-hours and the speedup that bought them (~250x per core). 20,000 needs roughly seven wall-hours on twelve cores; what has actually been accumulated is reported in the README and it is far less |
| 2 | BUGGIFY site activation coverage ≥ 95% | **not started** — and the honest note is that the core has *one* BUGGIFY site, so this criterion currently measures almost nothing. More sites is the real work, and each needs an argument for why it cannot make correct code wrong |
| 3 | Branch coverage under simulation ≥ 85% for `core/` | **not started** — needs gcov instrumentation wired into the build |
| 4 | Every ledger bug has a pinned seed that reproduces before its fix and passes after | **not started** — the seeds exist; the before/after CI check does not |
| 5 | Mixed-version cluster survives 200 node-hours of rolling upgrade | **not started** — needs two versions of the engine in one process, which is the largest single item in the phase |
| 6 | Full mutation report: detection rate, MTTD, API visibility | **met** — `tools/mutation_report.py` |

### What the remaining four need, so the next session does not re-derive it

- **Coverage-guided seed selection** is the deliverable that would change the
  fleet from linear to super-linear, and it is the one with the most leverage
  left. It needs `--coverage` builds, per-seed branch-coverage collection, and a
  corpus that retains a seed when it reaches new coverage. Without it, most
  simulated hours re-explore the same interleavings — which is the roadmap's own
  phrasing and is correct.
- **Mixed-version testing** is the largest item and the least like the others: it
  needs the engine compiled twice into one binary under different namespaces, or
  a process boundary the simulator can drive. Worth scoping properly before
  starting.
- **`tools/report.py`** would generate the README results block. The parts that
  are already machine-readable are the fleet's JSONL, the `DRILL|` lines, and
  the TLA+ runner's table; the rest of the block is hand-transcribed and the
  README now says so rather than claiming otherwise.

---

## Appendix: the original P4 plan, for comparison

### Next: P4 (MVCC and single-node transactions)

Per [docs/ROADMAP.md](docs/ROADMAP.md): inverted-timestamp key encoding, snapshot
reads, a lock table with wound-wait, and version GC driven by a safepoint. The
warning in the roadmap is the one to heed — GC safepoints are the classic silent
corruption source, so `INV-MVCC-01` (a live snapshot never loses a version it
can still see) gets armed before the GC is written, not after.
