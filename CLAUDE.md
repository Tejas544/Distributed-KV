# CLAUDE.md — how to work in this repository

This is the **operating** document: what to do, in what order, with what
commands, and what is currently true. It is deliberately short.

For *why* anything is the way it is — architecture, the file map, every gotcha
that has cost real time, and the phase-by-phase record — read
**[CONTEXT.md](CONTEXT.md)**. That is the engineering document and it is the one
that gets long. Do not duplicate it here; link to it.

**Both files are maintained in the same commit as the change they describe.** A
context document that lags the code is worse than none, because it is trusted.

---

## 1. What this project is

Anvil: a sharded, strictly-serializable distributed key-value store in C++20,
whose correctness is established by a deterministic simulator that hunts its own
bugs. The database is the well-trodden part; **the simulator and its oracles are
the point**. A failing run reproduces from a single 64-bit integer.

It is a placement-CV artifact. That sets the standard: **evidence beats volume,
and every claim must be verifiable by running something.** A number in the README
must be produced by a binary in this tree.

Scope: [docs/SCOPE.md](docs/SCOPE.md) · Plan: [docs/ROADMAP.md](docs/ROADMAP.md) ·
Invariants: [docs/INVARIANTS.md](docs/INVARIANTS.md) · Ledger: [BUGS.md](BUGS.md)

---

## 2. Build and run

CMake is the build system but **has never been executed on this machine** (no
CMake installed). Everything is verified by hand with g++ via:

```bash
bash tools/build.sh minimiser_test txn_faults
```

That compiles every source under `anvil/` and `workloads/` into an object cache
(`/tmp/anvilobj`), then links the named `test/*.cc` into `/tmp/anvilbin/anvil_*`.
It is incremental; a link is a few seconds.

Two things it already handles, and which break everything if forgotten:

- **`export PATH="/d/msys2/ucrt64/bin:$PATH"` is mandatory.** Git for Windows'
  `libstdc++-6.dll` shadows the MSYS2 one and binaries segfault inside
  `std::ofstream`'s constructor, with a backtrace that points at your code
  (CONTEXT.md gotcha 10.1).
- **`anvil/sim/sim_main.cc` has its own `main()`** and must be excluded from
  bulk compilation (gotcha 10.2).

The repository path contains a space, so compiler flags must be a bash **array**,
not a string.

### Suites and what they cost

| Binary | Argument | Runtime |
|---|---|---|
| `core_smoke`, `*_test` | none | under a second each |
| `minimiser_test` | seeds (400) | <1s |
| `elle_export` | `5000 5000 <file>` | ~1s |
| `sim_faults` | seeds (60) | ~10s |
| `lsm_crash` | seeds (40) | ~20s |
| `raft_faults` | seeds (12) | ~30s |
| `shard_faults` | seeds (24) | ~40s |
| `checker_mutation` | `200 10000` | ~1 min |
| `txn_faults` | seeds (30) | ~2.5 min |
| `dpor_test` | none | ~6.5 min |

`anvil_raft_faults 16` is the pinned reproduction of ANV-0032 and is *expected*
to fail on seed 14. Everything else is expected to pass.

`dpor_test` takes arguments only for calibration
(`dpor_test <ticks> <proposals> <in-flight> <max-states> [red-ticks red-props
red-flight red-nodes]`), which runs the sizing probe alone. With no arguments it
is the gate, and the gate's numbers are the ones the README quotes.

### The two gates that need a JVM

Not ctest targets, because making the whole suite depend on a JVM to run a unit
test is a good way to have nobody run the suite.

```bash
tools/fetch_tla.sh && tools/tlc.sh          # P7 criterion 3, model checking: ~12 min
tools/trace_validate.sh 16 250              # P7 criterion 3, conformance: ~1 min
tools/elle_cross.sh                         # P7 criterion 2: ~9 min, first run longer
```

`tools/tlc.sh` grades every configuration against a stated expectation, and
**four of the nine are required to FAIL, on a named property**. A control that
fails on the wrong property is reported as a failure, not as a pass.
`tools/elle_cross.sh` needs Leiningen; the first run downloads Elle from
Clojars.

---

## 3. Rules that are not negotiable

1. **No floating point in any decision path.** Probabilities are exact rationals
   (`Chance{num, den}`); FP contraction differs between compilers and INV-SIM-01
   requires identical digests across toolchains.
2. **No `unordered_*` iteration where order affects behaviour.** That includes
   checker witnesses — a bug report naming a different cycle on each machine is
   not reproducible.
3. **Fixed-width integers only.** `long` is 64-bit on Linux, 32-bit on Windows.
4. **All randomness from the seed.** Never renumber an existing `RandomDomain`;
   archived seeds stop reproducing their bugs.
5. **`anvil/core/**` is hermetic** — no syscalls, no wall clock, no threads, no
   unseeded randomness. `tools/hermetic_check.py` enforces it at link time and
   has a negative control that must be rejected.
6. **Every default is correct.** Deliberate-bug flags exist all over
   (`DurabilityOptions`, `RaftOptions`, `CoordinatorOptions`); a test that flips
   one is planting a bug, and it must be caught.

---

## 4. The workflow that has been working

1. Write the layer against `Runtime&`, in `anvil/core/`. Keep it hermetic.
2. Arm its invariants, in `docs/INVARIANTS.md` and in code, with honest cost
   classes (`kTick` must be O(nodes)).
3. Write the fault test: `FaultProfile::draw(seed)` → `heal_and_settle()` →
   assert.
4. **Run the seeded-mutation drill.** Plant deliberate bugs; every one must be
   caught. This is the step that makes everything else mean something.
5. File ledger rows in [BUGS.md](BUGS.md) with a seed, and pin the seed under
   `test/corpus/`.
6. Update [CONTEXT.md](CONTEXT.md), this file, the README results block, and the
   roadmap status.

**The drill is not optional.** A green run is evidence of nothing until you have
watched it go red for a reason you planted. Of the ledger's entries so far,
several were found by deliberately breaking something and discovering the suite
did not notice — including, in P2, a crash suite that passed cleanly on a
database with `fsync` switched off.

**Equivalent mutants are real, and are classified with the argument written
down** — never reported as a pass, never reported as a gap.

**A checker's own bookkeeping is not evidence about the system.** Roughly a
third of this project's findings have been the harness being wrong rather than
the engine, and three of them manufactured findings out of nothing. When
something fires, the first question is which of the two it is.

---

## 5. Where the work stands

| Phase | Status |
|---|---|
| P0 foundations | done |
| P1 simulator + oracles | done |
| P2 LSM engine | done except benchmarks (blocked on `ProdRuntime`) |
| P3 Raft | done; ANV-0032 open (a learner stops converging on seed 14) |
| P4 MVCC + transactions | done |
| P5 sharding | done |
| P6 distributed transactions | **done** — `txn_faults 30` green end to end |
| **P7 verification depth** | **done** — all 5 exit criteria met |
| P8+ | not started — next |

### P7 exit criteria and their state

| # | Criterion | State |
|---|---|---|
| 1 | Checker mutation score 100%; zero false positives over 10,000 histories | **met** — `checker.mutation`: 200/200 detected and correctly named across 9 anomaly classes, 0 FPs at all 5 levels, 120/120 discrimination pairs |
| 2 | Zero unexplained disagreements with Jepsen Elle over 10,000 shared histories | **met** — `tools/elle_cross.sh`: 10,000/10,000 verdicts identical, 10,000/10,000 same anomaly class, 0 disagreements |
| 3 | TLC finds no violations; every trace-validation run conforms | **met** — `tools/tlc.sh`: 9/9 configurations behaved as specified (5 clean, 4 required to fail on a named property). `tools/trace_validate.sh`: 16/16 implementation runs conform, 12/16 mutated runs correctly rejected |
| 4 | DPOR exhaustively covers its configuration class; state count reported | **met** — `verification.dpor`: 2,202,433 distinct states, complete, 0 violations, 3/3 seeded mutations caught |
| 5 | Minimiser reduces a known failing run from ≥10 faults to ≤3 in <5 min | **met** — `verification.minimiser`: 11 → 1 (`process.crash`), 1-minimality verified, and a second failure on the same workload minimises to a different cause |

**What trace validation does and does not cover.** It replays runs of
`anvil::raft::RaftNode` — the shipping state machine, driven by
`test/raft_model.h` with no simulator underneath — against `spec/Raft.tla`. It
does *not* cover the full simulator (no transport, disk or clock model), and it
runs the implementation configured to the specification's action set: pre-vote
off, CheckQuorum off, one entry per append, **no heartbeats**. The last is the
sharpest limit and it is a real finding rather than a shortcut: both sides clamp
the commit index a heartbeat carries, and they clamp it in different places — the
implementation at the sender (`min(pr.match, commit)`), the specification at the
receiver (`min(mcommit, verified)`). Both are sound; reconciling them needs a
matchIndex variable the specification deliberately does not have.

Long-standing blockers, unchanged: `anvil/prod/` is an empty target so nothing
can be benchmarked; the cross-toolchain digest gate has never run (no clang, no
macOS); **ANV-0033** — a run's outcome depends on sixteen bytes of environment
variable, which needs clang + MemorySanitizer on Linux and is the single
highest-value thing to do with a Linux box.

**ANV-0065 is open and is a design decision, not a patch.** Under parallel
commit at serializable, a recovering reader can implicitly commit a transaction
the coordinator's own read refresh would have aborted. `parallel_commit` is off
by default so nothing ships with it. Either the staging record carries enough for
a recovering reader to perform the refresh, or implicit commit is restricted to
snapshot isolation — do not guess between those without reading the row.

---

## 5a. Lessons P7 added, which generalise past this phase

1. **Two instruments that disagree find things neither one finds alone.** All
   three of P7's findings came from a disagreement, not from a failure: the
   exhaustive and reduced searches disagreeing about terminal states (ANV-0063,
   ANV-0064), and TLC contradicting what the fault sweep had never managed to
   produce (ANV-0065). Each individual instrument was reporting "no violations"
   and each was wrong.
2. **A specification can be wrong in a way that looks exactly like the system
   being wrong.** Three times in one afternoon: a configuration modelled as a
   global variable, Leader Completeness keyed on the entry's term rather than on
   the term the commit decision was made in, and a follower trusting `mcommit`
   over its whole log rather than over the prefix the message verified. This is
   the harness-versus-engine lesson the ledger keeps recording, in a new
   notation. Specifications are not exempt.
3. **When a search cannot find something, suspect the branching factor before
   the depth.** The five-server Figure-8 counterexample is fifteen steps deep;
   BFS was at depth 14 after 137 million states and simulation checked 112
   million without reaching it. Turning off heartbeats and restricting who may
   campaign took it to 11,746 states. Depth was never the problem.
4. **A state machine you cannot fingerprint is a state machine you cannot
   model-check** — and the fingerprint has to cover the private timers and the
   generator position, not just what the public accessors expose.
5. **An independence relation is a claim about the state representation, not
   about the system.** Two nodes commute in Raft; they did not commute in an
   encoding that threaded a global counter through both of them.
6. **Trace validation finds the differences a spec review never will.** Every
   stall it reported was a real discrepancy: three in the exporter's mirror of
   the specification's variables, three in the specification itself, and one
   genuine design difference (where a heartbeat's commit index gets clamped).
   None of them was visible by reading either artefact.

---

## 6. Working agreements with the user

- **Expand scope, do not shrink it.** When something is too large, tier it —
  ship the tier and say which tier shipped. Do not quietly drop deliverables.
- **Say what was not done, and why.** An unmet criterion written down is worth
  more than a met one that was redefined.
- **Never report a claim that no binary in this tree produces.**
