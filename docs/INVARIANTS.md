# Anvil — Invariant Catalogue

This is the heart of protocol-aware DST. Jepsen and Antithesis test from the outside in, over client-visible histories. This catalogue tests from the inside out: the simulator has a god's-eye view of every node's internal state, and these predicates are evaluated over that global state while the system runs.

**The column that matters is `API?`** — whether a violation of this invariant could ever be detected by a black-box client. Every `No` in that column is a bug class that an outside-in checker structurally cannot find. That column is the empirical justification for the whole approach, and it is also the interview answer.

---

## How invariants are declared

```cpp
ANVIL_INVARIANT(INV_RAFT_04, CostClass::TICK, "Leader Completeness")
  .over(GlobalState& g) -> Verdict {
    for (const auto& [term, entry] : g.committed_history()) {
      for (const auto& leader : g.leaders_after(term)) {
        if (!leader.log.contains(entry.index, entry.term))
          return Violation{"entry {} committed in term {} absent from leader of term {}",
                           entry.index, term, leader.term};
      }
    }
    return Ok{};
  };
```

Cost classes control when a predicate runs, so the expensive ones do not destroy simulation throughput:

| Class | When evaluated | Budget |
|---|---|---|
| `TICK` | Every scheduler step | O(nodes) or better |
| `EPOCH` | Every N ticks (config, default 1,000) | O(nodes × log) |
| `COMMIT` | On every transaction commit/abort | O(txn size) |
| `QUIESCE` | When faults heal and the system goes idle | Anything |
| `OFFLINE` | Post-hoc, over the recorded trace | Anything |

A violation dumps: the invariant id, the offending state, the active fault set, the BUGGIFY sites enabled, the causal trace slice, and the replay command.

---

## Global / cross-layer

| ID | Invariant | Class | API? |
|---|---|---|---|
| INV-GLOBAL-01 | The cluster's client-visible state is equal to the reference model's state, modulo in-flight and `Unknown` transactions | QUIESCE | Yes |
| INV-GLOBAL-02 | No write acknowledged as `Committed` is ever absent from a subsequent read at a later timestamp | COMMIT | Yes |
| INV-GLOBAL-03 | No value is ever returned that was never written by any transaction (no phantom data) | TICK | Yes |
| INV-GLOBAL-04 | An aborted transaction's writes are never visible to any reader at any timestamp | COMMIT | Yes |
| INV-GLOBAL-05 | After quiesce, total memory returns to baseline ± ε (no leak under fault churn) | QUIESCE | No |
| INV-GLOBAL-06 | After quiesce, no orphaned files, locks, intents, or Raft groups remain | QUIESCE | No |

---

## Consensus — `INV-RAFT-*`

| ID | Invariant | Class | API? |
|---|---|---|---|
| INV-RAFT-01 | **Election Safety.** At most one leader per term across the entire cluster | TICK | No |
| INV-RAFT-02 | **Leader Append-Only.** A leader never overwrites or deletes entries in its own log | TICK | No |
| INV-RAFT-03 | **Log Matching.** If two logs contain an entry with the same index and term, the logs are identical in all entries up to that index | TICK | No |
| INV-RAFT-04 | **Leader Completeness.** An entry committed in term T is present in the log of every leader of every term > T | TICK | No |
| INV-RAFT-05 | **State Machine Safety.** No two nodes apply different commands at the same log index | TICK | Sometimes |
| INV-RAFT-06 | A node's `current_term` never decreases, including across restart | TICK | No |
| INV-RAFT-07 | A node never grants two votes in the same term, including across restart (vote durability precedes the reply) | TICK | No |
| INV-RAFT-08 | `commit_index` is monotonically non-decreasing per node | TICK | No |
| INV-RAFT-09 | An entry is only marked committed if it is durably fsynced on a quorum of the current configuration | COMMIT | Sometimes |
| INV-RAFT-10 | A leader only advances `commit_index` past an entry from a previous term after committing an entry of its own term (the Figure-8 rule) | TICK | No |
| INV-RAFT-11 | A snapshot at index *i* reflects exactly the state machine after applying entries 1..*i*, and log truncation never removes an entry not covered by a durable snapshot | EPOCH | No |
| INV-RAFT-12 | During joint consensus, no two quorums drawn from C_old and C_new can be disjoint; no committed entry is lost across the transition | TICK | No |
| INV-RAFT-13 | A leader's lease never overlaps a successor's lease by more than the declared clock uncertainty bound | TICK | No |
| INV-RAFT-14 | A `ReadIndex` or lease read never returns state older than any write completed before the read was invoked | COMMIT | Yes |
| INV-RAFT-15 | A learner is never counted in any quorum until it is promoted | TICK | No |

---

## Storage engine — `INV-LSM-*`

| ID | Invariant | Class | API? |
|---|---|---|---|
| INV-LSM-01 | **No lost acked write.** Any write acknowledged after fsync is present after any crash and recovery | QUIESCE | Yes |
| INV-LSM-02 | Recovered state equals the model's state at the last durable commit point — never ahead of it, never behind it | QUIESCE | Yes |
| INV-LSM-03 | WAL: each record is either complete with a valid CRC, or the log is truncated at the first invalid record; no record after an invalid one is applied | EPOCH | No |
| INV-LSM-04 | Sequence numbers are strictly monotonic across memtable, WAL, and SSTables; no duplicate sequence for a key | TICK | No |
| INV-LSM-05 | For leveled compaction, SSTable key ranges within a level ≥ 1 are pairwise disjoint | EPOCH | No |
| INV-LSM-06 | A lookup returns the version with the highest sequence number ≤ the read snapshot across all levels; newer data always shadows older | COMMIT | Yes |
| INV-LSM-07 | Bloom/ribbon filters never produce false negatives | TICK | Sometimes |
| INV-LSM-08 | The recovered `VersionSet` references only files that exist and are complete; the MANIFEST update is crash-atomic | QUIESCE | No |
| INV-LSM-09 | No file referenced by a live version is ever deleted | TICK | No |
| INV-LSM-10 | After quiesce, no unreferenced files remain on disk (space leak) | QUIESCE | No |
| INV-LSM-11 | Every block read validates its checksum; a corrupted block is reported as an error and **never** served as data | TICK | Sometimes |
| INV-LSM-12 | Compaction preserves the visible-key set: for every key and every live snapshot, the pre- and post-compaction reads are equal | EPOCH | Sometimes |
| INV-LSM-13 | Block cache contents are consistent with the on-disk block they claim to represent (no stale-cache serve after compaction) | EPOCH | Sometimes |
| INV-LSM-14 | The immutable memtable queue is flushed in sequence order; no flush skips ahead | TICK | No |

`Sometimes` means: a violation *may* surface at the API under a specific subsequent access pattern, but usually will not — a corrupted block that is never read again, a filter false negative on a key nobody queries. These are precisely the bugs that ship.

---

## MVCC — `INV-MVCC-*`

| ID | Invariant | Class | API? |
|---|---|---|---|
| INV-MVCC-01 | **GC safepoint.** No version is removed if it is the newest version ≤ some live snapshot timestamp | TICK | Sometimes |
| INV-MVCC-02 | The GC safepoint is ≤ `min(active read snapshots, closed timestamp, oldest live transaction start)` | TICK | No |
| INV-MVCC-03 | Versions of a key are stored in strictly descending commit-timestamp order, with no duplicate commit timestamp | EPOCH | No |
| INV-MVCC-04 | A read at timestamp `ts` returns the version with the greatest `commit_ts ≤ ts`, having resolved or ignored intents per the engine's rules | COMMIT | Yes |
| INV-MVCC-05 | An intent and a committed value for the same key never coexist at the same commit timestamp | TICK | No |
| INV-MVCC-06 | The lock table's wait-for graph is acyclic, or a cycle exists and a victim has been selected within the detection interval | EPOCH | No |
| INV-MVCC-07 | Wound-wait victim selection is consistent with transaction start-timestamp order (no younger transaction wounds an older one) | TICK | No |
| INV-MVCC-08 | Every lock in the table is owned by a transaction that is live or expired-and-being-resolved; no lock is owned by a transaction unknown to the system | EPOCH | No |

---

## Transactions — `INV-TXN-*`

| ID | Invariant | Class | API? |
|---|---|---|---|
| INV-TXN-01 | **Atomicity.** For a committed transaction, all intents are eventually resolved to committed values; for an aborted one, all are removed | QUIESCE | Sometimes |
| INV-TXN-02 | **Primary-lock rule (Percolator).** A transaction is committed if and only if its primary lock has a commit record; every secondary's state is derivable from the primary | TICK | No |
| INV-TXN-03 | `commit_ts > start_ts`, and `commit_ts` is greater than every timestamp the transaction read at | COMMIT | No |
| INV-TXN-04 | No two concurrent transactions both commit a write to the same key (first-committer-wins) | COMMIT | Yes |
| INV-TXN-05 | **SSI.** No transaction commits that would close a cycle in the direct serialization graph over `ww`, `wr`, `rw` edges | OFFLINE | Yes |
| INV-TXN-06 | **Strict serializability.** If transaction A's commit is acknowledged before B is invoked in real (simulated wall) time, A precedes B in the serialization order | OFFLINE | Yes |
| INV-TXN-07 | **Uncertainty.** A read at `ts` that encounters a value with `commit_ts ∈ (ts, ts + max_offset]` either restarts or resolves the ambiguity; it never silently skips the value | COMMIT | Sometimes |
| INV-TXN-08 | **Commit-wait.** In external-consistency mode, no commit is acknowledged before `now().earliest > commit_ts` | COMMIT | No |
| INV-TXN-09 | **TSO monotonicity.** Allocated timestamps never regress, including across TSO leader failover and restart | TICK | Sometimes |
| INV-TXN-10 | No orphaned lock outlives its TTL plus the resolution interval without being resolved | QUIESCE | No |
| INV-TXN-11 | **Parallel commit.** A transaction in the `STAGING` state is treated as committed if and only if all of its writes are durably replicated; the recovery protocol reaches the same verdict as the coordinator would have | TICK | No |
| INV-TXN-12 | Distributed deadlock: any wait-for cycle spanning nodes is broken within the detection bound | EPOCH | No |
| INV-TXN-13 | A `watch` stream emits changes in commit-timestamp order per range, and its resolved timestamp never regresses and never exceeds the true closed timestamp | COMMIT | Yes |
| INV-TXN-14 | Follower reads never return data above the closed timestamp, and never violate the declared staleness bound | COMMIT | Yes |
| INV-TXN-15 | An `Unknown` commit result is eventually resolved to exactly one of `Committed` or `Aborted`, and never observed as both by different readers | QUIESCE | Yes |

---

## Sharding — `INV-SHARD-*`

| ID | Invariant | Class | API? |
|---|---|---|---|
| INV-SHARD-01 | **Coverage.** At every instant, the union of range descriptors tiles the key space exactly once — no gap, no overlap | EPOCH | Sometimes |
| INV-SHARD-02 | A split is atomic with respect to any transaction spanning the split point: the transaction sees either the pre- or post-split topology, never a mixture | TICK | Sometimes |
| INV-SHARD-03 | A merge proceeds only when both ranges are colocated, both leases are held by the same node, and both have quiesced | TICK | No |
| INV-SHARD-04 | At most one valid lease per range per instant, allowing for the clock uncertainty bound | TICK | No |
| INV-SHARD-05 | Range descriptor generations are strictly increasing; a stale-generation request is rejected, never served | TICK | Sometimes |
| INV-SHARD-06 | Rebalancing never transiently reduces a range below quorum durability | TICK | No |
| INV-SHARD-07 | The meta index is consistent with the actual range topology within the lease/uncertainty bound | EPOCH | Sometimes |
| INV-SHARD-08 | A quiesced range wakes correctly: no committed entry is missed while quiesced | TICK | No |
| INV-SHARD-09 | The placement driver's decisions are a function only of replicated state, never of a single node's local view | EPOCH | No |

---

## Liveness — `INV-LIVE-*`

Liveness is checked under *eventual synchrony*: the simulator stops injecting faults at time T and asserts progress within a bound after T. Without that discipline, liveness assertions fire spuriously and get disabled — which is how real systems end up with no liveness testing at all.

| ID | Invariant | Class | API? |
|---|---|---|---|
| INV-LIVE-01 | After faults heal, a leader is elected for every range within `bound_election` | QUIESCE | Yes |
| INV-LIVE-02 | After faults heal, every in-flight transaction reaches `Committed` or `Aborted` within `bound_txn` | QUIESCE | Yes |
| INV-LIVE-03 | After faults heal, every intent and lock is resolved within `bound_resolve` | QUIESCE | No |
| INV-LIVE-04 | Under sustained write load within the engine's rated throughput, LSM level sizes remain bounded (compaction keeps up; no unbounded write stall) | EPOCH | Sometimes |
| INV-LIVE-05 | After quiesce, the version count per key is bounded (GC makes progress) | QUIESCE | No |
| INV-LIVE-06 | No transaction is starved: a transaction that retries N times eventually commits under a fairness assumption | QUIESCE | Yes |
| INV-LIVE-07 | After faults heal, all replicas converge to the same applied index within `bound_converge` | QUIESCE | No |
| INV-LIVE-08 | The placement driver eventually restores the target replication factor for every range | QUIESCE | No |

---

## Simulator and harness — `INV-SIM-*`

Meta-invariants. These check that the *test infrastructure* is worth trusting. Skipping this section is how a project ends up with a green suite that proves nothing.

| ID | Invariant | Class | API? |
|---|---|---|---|
| INV-SIM-01 | **Determinism.** The same seed produces an identical 128-bit execution digest, across two runs, two compilers, and two architectures | OFFLINE | — |
| INV-SIM-02 | **Hermeticity.** `libanvil_core.a` links no denylisted symbol (wall clock, threads, syscalls, unseeded RNG) | OFFLINE | — |
| INV-SIM-03 | **Checker soundness.** The consistency checker flags 100% of the seeded anomalous-history corpus | OFFLINE | — |
| INV-SIM-04 | **Checker precision.** The checker accepts 100% of histories generated by the serial reference model (zero false positives) | OFFLINE | — |
| INV-SIM-05 | **Non-vacuity.** Every armed invariant has been observed to fire at least once against some seeded mutation; an invariant that has never fired is presumed vacuous and must be justified in writing | OFFLINE | — |
| INV-SIM-06 | **BUGGIFY reachability.** Every registered BUGGIFY site is activated at least once across the nightly fleet | OFFLINE | — |
| INV-SIM-07 | **Replay fidelity.** A minimised fault schedule reproduces the original violation | OFFLINE | — |
| INV-SIM-08 | **Fault-model coverage.** Every fault type in the model is exercised in the nightly fleet, and every one is observed to cause at least one client-visible retry or abort (a fault nobody notices is a fault that isn't being injected) | OFFLINE | — |

**INV-SIM-05 is the most important row in this document.** It is the difference between "I ran fault injection" and "I know my fault injection works."

---

## Invariant health dashboard

`tools/report.py` maintains this table from the nightly fleet. An invariant that has never fired and has no justification is a defect in the test suite.

| Metric | Meaning |
|---|---|
| `armed` | Invariants currently evaluated |
| `fired_ever` | Invariants that have caught at least one real or seeded bug |
| `never_fired` | Suspicious. Each needs a written justification or a new seeded mutation targeting it |
| `cost_p99_ns` | Per-invariant evaluation cost; anything dominating the tick budget gets demoted a cost class |
| `bugs_attributed` | Real bugs first caught by this invariant — the ranking that tells you where to invest next |
