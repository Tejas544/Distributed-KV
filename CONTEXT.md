# Anvil — Engineering Context

**Read this instead of the codebase.** It is the single onboarding document for a
new engineer or a fresh AI session. It should be enough to make a correct change
without opening more than two or three source files.

If you change architecture, add a layer, hit a non-obvious trap, or finish a
phase — **update this file in the same commit**. A context document that lags the
code is worse than none, because it is trusted.

- Last updated: end of **P2** (LSM storage engine), commit `e693bda`
- Repo: `https://github.com/Tejas544/Distributed-KV.git`, branch `main`
- ~13,300 lines C++20 + Python tooling, 63 source files

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

### `workloads/`, `test/`, `tools/`

| File | Lines | Purpose |
|---|---|---|
| `workloads/pingpong.*` | 177 | Token ring. P0/P1 determinism vehicle. |
| `workloads/counter.*` | 598 | Durable replicated counter with WAL + recovery. Arms `INV-CTR-01..03`. |
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
| P3 Raft | **next** | 12 `INV-RAFT-*` predicates to arm |
| P4+ | not started | MVCC, sharding, transactions, verification, fleet, prod |

### Measured (see README results block)

```
determinism ............ 10,000 seeds no faults; 2,000 with adversary armed
throughput ............. ~2.4M events/sec; ~38,500 simulated node-hours/core-hour
fault coverage ......... 19/19 kinds fire
LSM crash sweep ........ 13,116 acked writes, 0 lost, 0 resurrected, 0 orphans
corruption ............. detected 31/31 seeds, 0 bytes served that nobody wrote
seeded bugs ............ 10/10 caught in P2; 2/2 in P1; 13/13 checker mutants
```

### Stubs and gaps
- **`anvil/prod/` is an empty INTERFACE target.** No `ProdRuntime` exists. This
  blocks all benchmarking and the "same code, two runtimes" claim.
- **CMake never executed.** All verification by hand with g++.
- **Cross-toolchain digest gate never run** (no clang or macOS available). The
  CI job exists; `-O0/-O2/-O3` agreement is the only proxy so far.
- **No BUGGIFY sites in the core yet.** The mechanism works; nothing uses it.
- **Custom clang-tidy checks** are specified in `.clang-tidy` but unwritten.
- LSM deferred: block compression, ribbon filters, tiered compaction, reverse
  iteration, streaming k-way merge (compaction buffers its inputs).

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

[BUGS.md](BUGS.md) has 11 real entries (`ANV-0001`..`ANV-0011`) plus two format
examples. Three non-negotiable rules: every row has a **seed**, every row has a
**pinned regression** in `test/corpus/`, and rows are written **the day the bug is
found**.

Severity: S0 client-visible correctness · S1 internal invariant · S2 liveness ·
S3 resource/perf · S4 test infrastructure.

The `api_visible` column is the point of the whole project — every `no` is a bug
class an outside-in checker structurally cannot find.

Highest-value entries to read before starting P3:
- **ANV-0001** — the scheduler discarded one event at every deadline, orphaning a
  coroutine. Four wrong hypotheses chased first because they all had the same
  symptom.
- **ANV-0005** — an armed invariant that could never fire, because a cheaper one
  shadowed it. Co-firing on an epoch boundary looked like detection.
- **ANV-0006** — the crash suite passed on a database with fsync off.
- **ANV-0011** — a guard whose situation the workload could not reach.

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

**10.8 A crashed node's ordering.** In `ProcessModel::crash()`: purge queued
events → clear network endpoints → destroy coroutine frames. Any other order is a
use-after-free that only fires under crash-heavy seeds.

---

## 11. Next up: P3 (Raft)

Per [docs/ROADMAP.md](docs/ROADMAP.md):

- Election with randomised timeouts, **pre-vote**, `CheckQuorum`; term and vote
  persisted **before** responding.
- Log replication with pipelining, batching, conflict backtracking, and commit
  restricted to current-term entries (the Figure-8 hazard).
- Log compaction, chunked snapshot install, **joint-consensus** membership change
  (not the one-at-a-time shortcut), learners.
- Leader lease + `ReadIndex` linearizable reads; leadership transfer.
- All twelve `INV-RAFT-*` from [docs/INVARIANTS.md](docs/INVARIANTS.md) armed at
  `kTick` over the *global* state — every node's log, term, vote, commit index.
- Exit criteria include a **10-bug seeded-mutation drill** where each bug is
  recorded with whether it was **visible at the client API**. That table is the
  empirical core of the protocol-aware DST claim and is the single most valuable
  artifact P3 produces.

Raft is where "invisible at the API boundary" stops being a claim about the
technique and becomes a measurable column in the ledger.
