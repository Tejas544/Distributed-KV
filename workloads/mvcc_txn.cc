#include "workloads/mvcc_txn.h"

#include <cstdlib>

#include <algorithm>

namespace anvil::workloads {
namespace {

std::string key_name(std::uint32_t index) { return "k" + std::to_string(index); }

// The model's answer: the newest committed value at or below `ts`, or nothing.
bool model_read(const VersionModel& model, const std::string& key, mvcc::CommitTs ts,
                std::string* value) {
  const auto it = model.find(key);
  if (it == model.end()) return false;
  // upper_bound gives the first entry strictly above ts; step back one.
  const auto above = it->second.upper_bound(ts);
  if (above == it->second.begin()) return false;
  const auto resolved = std::prev(above);
  if (resolved->second.empty()) return false;  // a tombstone
  *value = resolved->second;
  return true;
}

void note(MvccWorkloadState* state, std::string detail) {
  if (state->violations.size() < 8) state->violations.push_back(std::move(detail));
}

// The commit-timestamp source. Monotonic and unique, which INV-MVCC-03 requires
// -- two versions of one key at the same timestamp have no defined order, and a
// reader at that timestamp would resolve to whichever the encoding happened to
// put first.
mvcc::CommitTs next_ts(Runtime& rt) {
  static thread_local mvcc::CommitTs counter = 0;
  const mvcc::CommitTs from_clock = mvcc::to_commit_ts(rt.now());
  counter = std::max(counter + 1, from_clock);
  return counter;
}

// ---------------------------------------------------------------------------
// writers
// ---------------------------------------------------------------------------

Task<bool> await_resolution(Runtime& rt, MvccWorkloadState* state, TxnId id);

Task<void> writer(Runtime& rt, MvccWorkloadConfig cfg, MvccWorkloadState* state,
                  std::uint32_t which) {
  auto& rng = rt.rng(RandomDomain::kWorkload);
  for (std::uint64_t op = 0; op < cfg.ops_per_writer && !state->done; ++op) {
    co_await rt.sleep_for(cfg.writer_interval);

    const mvcc::CommitTs start = next_ts(rt);
    mvcc::Txn* txn = state->txns->begin(start, cfg.level);
    const TxnId id = txn->id;

    bool ok = true;
    std::map<std::string, std::string> staged;

    for (std::uint32_t r = 0; r < cfg.reads_per_txn && ok; ++r) {
      const std::string key = key_name(static_cast<std::uint32_t>(rng.uniform(cfg.keys)));
      bool found = false;
      std::string value;
      const Status status = co_await state->txns->read(id, key, &found, &value);
      if (!status.is_ok()) {
        ok = false;
        break;
      }
      // INV-MVCC-04, checked at the moment of the read against the oracle. The
      // transaction's own uncommitted writes are excluded, because the model
      // only knows about committed versions.
      if (!staged.contains(key)) {
        ++state->reads_checked;
        std::string expected;
        const bool expected_found = model_read(state->model, key, start, &expected);
        if (found != expected_found || (found && value != expected)) {
          // The model is updated after the commit returns, and the engine makes
          // a batch visible before that -- so a read can legitimately see a
          // value the oracle has not recorded yet. That is model lag, not a
          // violation, and counting it as one would be reporting correct
          // behaviour as a fault (ANV-0007).
          //
          // What is never legitimate: returning a value the model has at a
          // *higher* timestamp than this snapshot (reading the future), or an
          // older value than the model resolves to (a stale read). Both are
          // checked; anything else is lag and is counted separately.
          bool lag = false;
          if (found) {
            const auto entry = state->model.find(key);
            if (entry == state->model.end()) {
              lag = true;  // nothing recorded for this key yet
            } else {
              bool known_at_or_below = false;
              bool known_above = false;
              for (const auto& [ts, v] : entry->second) {
                if (v != value) continue;
                (ts <= start ? known_at_or_below : known_above) = true;
              }
              if (!known_at_or_below && !known_above) lag = true;
              if (known_above && !known_at_or_below) lag = false;  // read the future
            }
          }
          if (lag) {
            ++state->model_lag_reads;
          } else {
            ++state->wrong_reads;
            note(state, "read of " + key + " at " + std::to_string(start) + " returned " +
                            (found ? value : std::string("<none>")) + ", model says " +
                            (expected_found ? expected : std::string("<none>")));
          }
        }
      }
    }

    for (std::uint32_t w = 0; w < cfg.writes_per_txn && ok; ++w) {
      const std::string key = key_name(static_cast<std::uint32_t>(rng.uniform(cfg.keys)));
      const std::string value =
          "w" + std::to_string(which) + ":" + std::to_string(op) + ":" + std::to_string(w);
      const Status status = co_await state->txns->write(id, key, value, false);
      if (!status.is_ok()) {
        ok = false;
        if (status.code() == StatusCode::kAborted) ++state->write_conflicts;
        break;
      }
      staged[key] = value;
    }

    if (!ok) {
      co_await state->txns->abort(id);
      ++state->txns_aborted;
      ++state->retries;
      continue;
    }

    const mvcc::CommitTs commit = next_ts(rt);
    const Status status = co_await state->txns->commit(id, commit);
    if (!status.is_ok()) {
      // Two very different failures arrive through one status. A refused commit
      // is over; a commit whose write did not survive the disk is *undecided*,
      // and calling that an abort would be the client inventing an outcome the
      // engine never gave it.
      const mvcc::Txn* txn = state->txns->find(id);
      if (txn != nullptr && txn->resolving()) {
        ++state->ambiguous_commits;
        if (co_await await_resolution(rt, state, id)) {
          ++state->txns_committed;
          for (const auto& [key, value] : staged) state->model[key][commit] = value;
        } else {
          ++state->txns_aborted;
        }
        continue;
      }
      co_await state->txns->abort(id);
      ++state->txns_aborted;
      continue;
    }
    ++state->txns_committed;
    // The model is updated only after the commit has succeeded. Updating it
    // before would make the oracle describe a world the store never entered,
    // and every subsequent read would be judged against fiction.
    for (const auto& [key, value] : staged) state->model[key][commit] = value;
  }
  ++state->writers_finished;
}

// Waits for a transaction whose outcome is decided but unwritten. Returns true
// if it ended up committed. Bounded: if the disk never comes back the run ends
// with the transaction still unresolved, which is the honest outcome and which
// the auditor knows not to call an orphan.
Task<bool> await_resolution(Runtime& rt, MvccWorkloadState* state, TxnId id) {
  for (int attempt = 0; attempt < 40 && !state->done; ++attempt) {
    co_await rt.sleep_for(Duration::millis(20));
    const mvcc::Txn* txn = state->txns->find(id);
    if (txn == nullptr) co_return false;
    if (txn->state == mvcc::TxnState::kCommitted) co_return true;
    if (txn->state == mvcc::TxnState::kAborted) co_return false;
  }
  ++state->unresolved_at_end;
  co_return false;
}

// The janitor. Nothing in the engine retries a failed intent resolution on its
// own -- deliberately, because a state machine that schedules its own I/O is a
// state machine you cannot test deterministically. Driving it is the caller's
// job, and this is the caller.
Task<void> janitor(Runtime& rt, MvccWorkloadConfig cfg, MvccWorkloadState* state) {
  while (!state->done) {
    co_await rt.sleep_for(cfg.gc_interval);
    if (state->txns == nullptr) continue;
    std::uint64_t resolved = 0;
    (void)co_await state->txns->resolve_pending(&resolved);
    state->resolutions += resolved;

    // Retire what is finished. Not housekeeping for its own sake: several
    // invariants walk the transaction table on every event, and a table that
    // only ever grows turns a constant-cost check into a quadratic one. The run
    // does not fail, it just stops making progress -- the most expensive kind of
    // wrong, because it looks like a hang and gets debugged as one.
    std::vector<TxnId> finished;
    for (const auto& [id, txn] : state->txns->transactions()) {
      if (txn.live() || txn.resolving()) continue;
      finished.push_back(txn.id);
    }
    // Three audit intervals, because the auditor's orphan rule needs the same
    // key and the same owner to look wrong on two consecutive passes: an owner
    // retired between those two passes turns a resolved transaction into an
    // "unknown" one and manufactures the exact finding the rule exists to avoid.
    const Duration keep = cfg.audit_interval * 3;
    for (const TxnId id : finished) {
      if (state->txns->retire(id, keep)) ++state->retired;
    }
  }
}

// ---------------------------------------------------------------------------
// long readers -- the reason the safepoint has to be right
// ---------------------------------------------------------------------------

Task<void> long_reader(Runtime& rt, MvccWorkloadConfig cfg, MvccWorkloadState* state,
                       std::uint64_t handle) {
  while (!state->done) {
    const mvcc::CommitTs snapshot = next_ts(rt);
    state->txns->open_snapshot(handle, snapshot);

    // Hold it open across many collector passes, then verify every key still
    // resolves to what it resolved to when the snapshot was taken. This is the
    // exact scenario the roadmap asks for: "a long reader spanning many
    // compactions never observes a missing version".
    const Timestamp deadline = rt.now().advanced_by(cfg.reader_hold);
    while (rt.now() < deadline && !state->done) {
      co_await rt.sleep_for(cfg.gc_interval);

      for (std::uint32_t i = 0; i < cfg.keys; ++i) {
        const std::string key = key_name(i);
        std::string expected;
        const bool expected_found = model_read(state->model, key, snapshot, &expected);
        if (!expected_found) continue;

        mvcc::ReadResult result;
        const Status status = co_await state->store->get(key, snapshot, TxnId{}, &result);
        if (!status.is_ok()) continue;
        if (result.blocked) continue;  // an intent, not a missing version

        ++state->long_reader_checks;
        if (!result.found || result.value != expected) {
          ++state->lost_versions;
          note(state, "long reader at " + std::to_string(snapshot) + " lost " + key +
                          ": expected " + expected + ", got " +
                          (result.found ? result.value : std::string("<none>")));
          if (state->observer != nullptr) {
            state->observer->record("INV-MVCC-01",
                                    "a snapshot at " + std::to_string(snapshot) +
                                        " can no longer resolve " + key + " to " + expected);
          }
        }
      }
    }
    state->txns->close_snapshot(handle);
    co_await rt.sleep_for(cfg.writer_interval);
  }
}

// ---------------------------------------------------------------------------
// the collector
// ---------------------------------------------------------------------------

Task<void> collector(Runtime& rt, MvccWorkloadConfig cfg, MvccWorkloadState* state) {
  while (!state->done) {
    co_await rt.sleep_for(cfg.gc_interval);

    const mvcc::CommitTs safepoint =
        cfg.gc_uses_real_safepoint
            ? state->txns->safepoint()
            // The deliberate bug: collect at "now" and ignore who is still
            // reading below it. Every long reader's snapshot is then fair game.
            : next_ts(rt);

    if (state->observer != nullptr) {
      // The floor is always the manager's own computation, never the collector's
      // -- otherwise the deliberate bug would be compared against itself and
      // would agree with itself every time.
      mvcc::CommitTs floor = state->txns->safepoint();
      std::string owner = "the safepoint";
      for (const auto& [id, txn] : state->txns->transactions()) {
        if ((txn.live() || txn.resolving()) && txn.start_ts <= floor) {
          owner = "t" + std::to_string(id);
          break;
        }
      }
      state->observer->note_safepoint(safepoint, floor, owner);
    }

    std::uint64_t collected = 0;
    const Status status = co_await state->store->collect_garbage(safepoint, 64, &collected);
    if (!status.is_ok()) continue;
    ++state->gc_passes;
    state->versions_collected += collected;
  }
}

// ---------------------------------------------------------------------------
// deadlockers -- the only way to reach wound-wait
// ---------------------------------------------------------------------------

Task<void> deadlocker(Runtime& rt, MvccWorkloadConfig cfg, MvccWorkloadState* state,
                      bool forward) {
  // Two keys, taken in opposite orders by the two halves of a pair. Under
  // two-phase locking this is the textbook deadlock; under wound-wait one of
  // them is wounded instead, and INV-MVCC-06 asserts no cycle ever forms.
  const std::string first = forward ? key_name(0) : key_name(1);
  const std::string second = forward ? key_name(1) : key_name(0);

  while (!state->done) {
    co_await rt.sleep_for(cfg.deadlock_interval);

    mvcc::Txn* txn = state->txns->begin(next_ts(rt), cfg.level);
    const TxnId id = txn->id;

    Status status = co_await state->txns->write(id, first, "d1", false);
    if (status.is_ok()) {
      // A real gap between the two acquisitions, so the other half has time to
      // take the second key. Without it the deadlock window never opens and the
      // whole workload is decorative.
      co_await rt.sleep_for(cfg.writer_interval);
      status = co_await state->txns->write(id, second, "d2", false);
    }

    if (!status.is_ok()) {
      const mvcc::Txn* current = state->txns->find(id);
      if (current != nullptr && current->state == mvcc::TxnState::kWounded) ++state->wounded;
      co_await state->txns->abort(id);
      ++state->txns_aborted;
      continue;
    }
    const mvcc::CommitTs deadlock_commit = next_ts(rt);
    Status deadlock_status = co_await state->txns->commit(id, deadlock_commit);
    if (!deadlock_status.is_ok()) {
      const mvcc::Txn* txn = state->txns->find(id);
      if (txn != nullptr && txn->resolving()) {
        ++state->ambiguous_commits;
        if (co_await await_resolution(rt, state, id)) deadlock_status = Status::ok();
      }
    }
    if (deadlock_status.is_ok()) {
      ++state->txns_committed;
      state->model[first][deadlock_commit] = "d1";
      state->model[second][deadlock_commit] = "d2";
    } else {
      co_await state->txns->abort(id);
      ++state->txns_aborted;
    }
  }
}

// ---------------------------------------------------------------------------
// the auditor
// ---------------------------------------------------------------------------

Task<void> auditor(Runtime& rt, MvccWorkloadConfig cfg, MvccWorkloadState* state) {
  while (!state->done) {
    co_await rt.sleep_for(cfg.audit_interval);
    // A pass against a node that is not running answers nothing: every read
    // fails or returns empty, and "the store lost everything" is the finding it
    // would produce. Same rule as the Raft observer -- a node that is not up is
    // unknown, not wrong (ANV-0021).
    if (state->simulation != nullptr &&
        !state->simulation->process().alive(NodeId{1})) {
      continue;
    }

    std::uint64_t versions = 0;
    for (std::uint32_t i = 0; i < cfg.keys; ++i) {
      const std::string key = key_name(i);
      std::vector<std::pair<mvcc::CommitTs, std::string>> versions_of_key;
      if (!(co_await state->store->versions_of(key, &versions_of_key)).is_ok()) continue;
      versions += versions_of_key.size();

      // INV-MVCC-03: strictly descending, no duplicates. The scan returns them
      // newest-first by construction, so a non-decreasing step means either the
      // encoding or the commit-timestamp source is wrong.
      for (std::size_t v = 1; v < versions_of_key.size(); ++v) {
        if (versions_of_key[v].first >= versions_of_key[v - 1].first) {
          ++state->order_violations;
          const std::string detail =
              key + " has versions at " + std::to_string(versions_of_key[v - 1].first) +
              " then " + std::to_string(versions_of_key[v].first) + " -- not descending";
          note(state, detail);
          if (state->observer != nullptr) state->observer->record("INV-MVCC-03", detail);
        }
      }

      // INV-MVCC-05: an intent whose owner has already resolved is an orphan.
      // It blocks the key for every later reader, and the only symptom is a
      // workload that quietly stops making progress on one key.
      bool has_intent = false;
      mvcc::Intent intent;
      const bool read_ok =
          (co_await state->store->read_intent(key, &has_intent, &intent)).is_ok();
      bool suspicious = false;
      if (read_ok && has_intent) {
        const mvcc::Txn* owner = state->txns->find(intent.txn);
        if (owner == nullptr) {
          // The owner has been retired, so this intent cannot be attributed.
          // That is a limit of the checker's memory, not a fact about the
          // engine, and calling it an orphan would be reporting the auditor's
          // own bookkeeping as a bug in the system under test. Counted, so the
          // blind spot is visible rather than silent -- and bounded, because
          // retirement waits three audit intervals while the rule below needs
          // two, so a genuinely stuck intent is caught before its owner goes.
          ++state->unattributable_intents;
        } else {
          suspicious = owner->state == mvcc::TxnState::kCommitted ||
                       owner->state == mvcc::TxnState::kAborted;
        }
      }

      // An intent is an orphan only if it is *still* sitting on a resolved
      // transaction one whole audit interval later.
      //
      // One observation is not enough, and the reason is structural rather than
      // a tuning choice: reading the intent suspends, resolving it is several
      // steps, and an auditor that samples a multi-step operation at one instant
      // cannot tell "in flight" from "stuck". Demanding that the same key and
      // the same owner still look wrong on the next pass removes the in-flight
      // window entirely, at the cost of one audit interval of detection latency
      // -- which is the honest trade, because the alternative is a checker that
      // reports correct behaviour and gets switched off (ANV-0007).
      const auto previous = state->suspicious_intents.find(key);
      const bool was_suspicious =
          previous != state->suspicious_intents.end() &&
          previous->second == intent.txn.value();

      if (suspicious && was_suspicious) {
        const mvcc::Txn* owner = state->txns->find(intent.txn);
        ++state->orphan_intents;
        const std::string detail =
            key + " has held an intent from t" + std::to_string(intent.txn.value()) +
            " across a whole audit interval, and that transaction is " +
            (owner == nullptr ? "gone" : mvcc::to_string(owner->state)) +
            (owner == nullptr
                 ? std::string{}
                 : std::string{" (the key is "} +
                       (owner->writes.count(key) != 0 ? "in" : "NOT in") +
                       " its write set, which has " + std::to_string(owner->writes.size()) +
                       " key(s))");
        note(state, detail);
        if (state->observer != nullptr) state->observer->record("INV-MVCC-05", detail);
        state->suspicious_intents.erase(key);
      } else if (suspicious) {
        state->suspicious_intents[key] = intent.txn.value();
      } else {
        if (was_suspicious) ++state->transient_intents;
        state->suspicious_intents.erase(key);
      }
    }

    ++state->audits;
    state->versions_audited += versions;
    if (state->observer != nullptr) state->observer->note_audit(versions);
  }
}

Task<void> completion_monitor(Runtime& rt, MvccWorkloadConfig cfg, MvccWorkloadState* state,
                              std::uint64_t target) {
  while (!state->done) {
    co_await rt.sleep_for(cfg.audit_interval);
    // Either the work is done or the workers are. Waiting only on the commit
    // count means a seed whose transactions mostly abort never finishes: the
    // writers exhaust their operations and exit, nothing increments the counter
    // again, and the run spends the rest of max_time simulating an empty system
    // while the fault injector keeps charging for it.
    if (state->txns_committed + state->txns_aborted >= target) state->done = true;
    if (state->writers_finished >= cfg.writers) state->done = true;
  }
  // Stop the clock when the work stops. The run is bounded by max_time as a
  // backstop, but simulating the remaining seconds of an empty workload is not
  // free: the fault injector keeps firing, and its cost grows with the number of
  // files the run has produced, so a finished workload can spend longer being
  // idle than it did being busy.
  if (state->simulation != nullptr) state->simulation->scheduler().request_stop();
}

}  // namespace

// ---------------------------------------------------------------------------

void arm_read_invariants(sim::Simulation& simulation, MvccWorkloadState* state) {
  // INV-MVCC-04 is checked by the writers at the moment of each read; this
  // reports it within one event, which is what makes the failing run's causal
  // trace point at the read rather than at whatever happened next.
  simulation.invariants().arm(
      "INV-MVCC-04", "a read returns the newest version at or below its snapshot",
      checker::CostClass::kTick, [state]() -> std::optional<std::string> {
        if (state->wrong_reads == 0) return std::nullopt;
        state->wrong_reads = 0;
        return state->violations.empty() ? std::string{"a read disagreed with the model"}
                                         : state->violations.back();
      });
}

namespace {

// One boot. Re-invoked on restart, so it opens the database from whatever a
// crash left behind rather than assuming a blank machine -- the same contract
// every other workload's boot function has.
Task<void> boot_node(Runtime& runtime, sim::Simulation& sim_ref, MvccWorkloadConfig cfg,
                     MvccWorkloadState* st, checker::MvccObserver* obs) {
  ++st->boots;
  lsm::DbOptions db_options;
  std::unique_ptr<lsm::Db> db;
  // Unbounded retry, for the same reason every other boot path in this tree has
  // one: a transient device error is not a reason to never come back (ANV-0003).
  for (;;) {
    const Status status = co_await lsm::Db::open(&runtime, db_options, &db);
    if (status.is_ok()) break;
    co_await runtime.sleep_for(Duration::millis(20));
  }
  st->db = std::move(db);
  st->store = std::make_unique<mvcc::MvccStore>(&runtime, st->db.get(), cfg.mvcc);
  st->txns = std::make_unique<mvcc::TxnManager>(&runtime, st->store.get(), st->locks.get());
  if (obs != nullptr) {
    obs->configure(st->txns.get(), st->locks.get(),
                   [&sim_ref]() { return sim_ref.scheduler().tick(); });
  }

  for (std::uint32_t i = 0; i < cfg.writers; ++i) runtime.spawn(writer(runtime, cfg, st, i));
  for (std::uint32_t i = 0; i < cfg.long_readers; ++i) {
    runtime.spawn(long_reader(runtime, cfg, st, 1000 + i));
  }
  for (std::uint32_t i = 0; i < cfg.deadlockers; ++i) {
    runtime.spawn(deadlocker(runtime, cfg, st, true));
    runtime.spawn(deadlocker(runtime, cfg, st, false));
  }
  runtime.spawn(collector(runtime, cfg, st));
  runtime.spawn(janitor(runtime, cfg, st));
  runtime.spawn(auditor(runtime, cfg, st));
  runtime.spawn(completion_monitor(runtime, cfg, st, cfg.writers * cfg.ops_per_writer));
}

}  // namespace

void install(sim::Simulation& simulation, MvccWorkloadConfig config,
             MvccWorkloadState* state, checker::MvccObserver* observer) {
  state->observer = observer;
  state->simulation = &simulation;
  state->locks = std::make_unique<mvcc::LockTable>();

  Runtime& rt = simulation.node(NodeId{1});

  if (observer != nullptr) checker::arm_mvcc_invariants(simulation.invariants(), observer);
  arm_read_invariants(simulation, state);

  simulation.set_boot(NodeId{1}, [&simulation, &rt, config, state, observer]() {
    // A crash wipes the lock table and the transaction manager with the process,
    // which is correct: locks are volatile state and the intents on disk are
    // what survive. Rebuilding the table empty is what makes an orphaned intent
    // observable at all (INV-MVCC-05).
    state->locks = std::make_unique<mvcc::LockTable>();
    rt.spawn(boot_node(rt, simulation, config, state, observer));
  });

  rt.spawn(boot_node(rt, simulation, config, state, observer));
}

Task<bool> audit_everything(MvccWorkloadState* state) {
  if (state->store == nullptr) co_return false;
  bool clean = true;

  // Every timestamp the model knows about, on every key. If a version the model
  // says should be resolvable is not, the collector took something it should
  // have kept -- and nothing else in the system would ever have said so.
  for (const auto& [key, versions] : state->model) {
    for (const auto& [ts, expected] : versions) {
      mvcc::ReadResult result;
      const Status status = co_await state->store->get(key, ts, TxnId{}, &result);
      if (!status.is_ok()) continue;
      if (result.blocked) continue;
      if (!result.found || result.value != expected) {
        // Only a violation if this timestamp is still above the safepoint --
        // below it the version is legitimately collectable, and the model is
        // deliberately not pruned so the distinction stays visible.
        if (ts > state->txns->safepoint()) {
          clean = false;
          ++state->lost_versions;
          note(state, "final audit: " + key + " at " + std::to_string(ts) + " expected " +
                          expected + ", got " +
                          (result.found ? result.value : std::string("<none>")));
        }
      }
    }
  }
  co_return clean;
}

}  // namespace anvil::workloads
