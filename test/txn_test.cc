// Transaction mechanism unit tests: the version store, the record status
// lattice, the opaque command interface a range actually applies, and the two
// timestamp sources -- with nothing running.
//
// Everything in anvil/core/txn/{record,store,command,timestamp}.h is a plain
// data structure and a set of pure functions: no Raft, no Runtime, no
// coroutine anywhere near it. That is what makes it testable here rather than
// only under a simulation, and it is the same split raft_test/raft_faults and
// mvcc_test/mvcc_faults have -- "the protocol is right when nothing goes
// wrong" here, "the protocol survives an adversary, a coordinator, and a
// cluster" in test/txn_faults.cc.
//
// The status lattice is the part worth getting right on purpose rather than
// by accident: pending -> staging -> committed, pending -> aborted, and
// nothing out of a terminal state. A transition out of a terminal state is
// not a race to be resolved, it is two readers disagreeing about whether a
// transaction happened, which is why every deliberate-bug flag here has a
// test that shows the flag genuinely causing that disagreement rather than
// merely existing.

#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string>
#include <string_view>
#include <vector>

#include "anvil/core/lsm/format.h"
#include "anvil/core/txn/command.h"
#include "anvil/core/txn/record.h"
#include "anvil/core/txn/store.h"
#include "anvil/core/txn/timestamp.h"

namespace {

using anvil::Duration;
using anvil::LogIndex;
using anvil::Timestamp;
namespace txn = anvil::txn;
namespace lsm = anvil::lsm;

int g_failures = 0;

void check(bool condition, std::string_view what) {
  if (!condition) {
    std::cerr << "FAIL: " << what << "\n";
    ++g_failures;
  }
}

// ---------------------------------------------------------------------------
// pure helpers: terminal(), TxnRecord::expired()
// ---------------------------------------------------------------------------

void test_terminal_helper() {
  check(!txn::terminal(txn::TxnStatus::kPending), "pending is not terminal");
  check(!txn::terminal(txn::TxnStatus::kStaging), "staging is not terminal");
  check(txn::terminal(txn::TxnStatus::kCommitted), "committed is terminal");
  check(txn::terminal(txn::TxnStatus::kAborted), "aborted is terminal");
}

void test_record_expiry() {
  txn::TxnRecord r;
  r.heartbeat = 1000;
  r.ttl_nanos = 500;
  check(!r.expired(1500), "exactly heartbeat + ttl is not yet expired");
  check(r.expired(1501), "one nanosecond past it is expired");

  txn::TxnRecord no_ttl;
  no_ttl.heartbeat = 1000;
  no_ttl.ttl_nanos = 0;
  check(!no_ttl.expired(999'999'999ULL), "ttl_nanos == 0 means the record never expires");

  txn::TxnRecord no_heartbeat;
  no_heartbeat.heartbeat = 0;
  no_heartbeat.ttl_nanos = 500;
  check(!no_heartbeat.expired(999'999'999ULL),
        "heartbeat == 0 means it has never been set, so there is nothing to age");
}

// ---------------------------------------------------------------------------
// encoding round trips
// ---------------------------------------------------------------------------

void test_record_survives_its_own_encoding() {
  txn::TxnRecord record;
  record.id = 42;
  record.epoch = 3;
  record.status = txn::TxnStatus::kStaging;
  record.start_ts = 100;
  record.commit_ts = 0;
  record.keys = {"primary", "second", "third"};
  record.heartbeat = 5000;
  record.ttl_nanos = 1500;
  record.pushed_to = 200;

  txn::TxnRecord round;
  check(txn::decode_record(txn::encode_record(record), &round), "a record must decode");
  check(round.id == record.id && round.epoch == record.epoch, "id and epoch survive");
  check(round.status == record.status, "status survives");
  check(round.start_ts == record.start_ts && round.commit_ts == record.commit_ts,
        "timestamps survive");
  check(round.keys == record.keys, "the key list survives, in order");
  check(round.heartbeat == record.heartbeat && round.ttl_nanos == record.ttl_nanos,
        "heartbeat and ttl survive");
  check(round.pushed_to == record.pushed_to, "the push watermark survives");
}

void test_intent_survives_its_own_encoding() {
  txn::Intent intent;
  intent.txn = 7;
  intent.epoch = 2;
  intent.start_ts = 99;
  intent.tombstone = true;
  intent.value = "value with\x00 an embedded NUL";
  intent.primary = "the-primary-key";

  txn::Intent round;
  check(txn::decode_intent(txn::encode_intent(intent), &round), "an intent must decode");
  check(round.txn == intent.txn && round.epoch == intent.epoch, "txn and epoch survive");
  check(round.start_ts == intent.start_ts, "start_ts survives");
  check(round.tombstone, "the tombstone flag survives");
  check(round.primary == intent.primary, "the primary key survives");
}

void test_txn_command_survives_its_own_encoding() {
  txn::TxnCommand cmd;
  cmd.op = txn::TxnOp::kPutRecord;
  cmd.key = "primary";
  cmd.txn = 55;
  cmd.epoch = 4;
  cmd.start_ts = 10;
  cmd.commit_ts = 20;
  cmd.push_to = 30;
  cmd.tombstone = true;
  cmd.abort_expired = false;
  cmd.value = "v";
  cmd.primary = "primary";
  cmd.record.id = 55;
  cmd.record.status = txn::TxnStatus::kCommitted;
  cmd.record.commit_ts = 20;
  cmd.now = 123456789;

  txn::TxnCommand round;
  check(txn::decode_txn_command(txn::encode_txn_command(cmd), &round),
        "a transactional command must decode");
  check(round.op == cmd.op, "the op survives");
  check(round.key == cmd.key && round.txn == cmd.txn && round.epoch == cmd.epoch,
        "routing fields survive");
  check(round.start_ts == cmd.start_ts && round.commit_ts == cmd.commit_ts &&
            round.push_to == cmd.push_to,
        "every timestamp survives");
  check(round.tombstone && !round.abort_expired, "both flags survive independently");
  check(round.value == cmd.value && round.primary == cmd.primary, "value and primary survive");
  check(round.record.id == cmd.record.id && round.record.status == cmd.record.status &&
            round.record.commit_ts == cmd.record.commit_ts,
        "the nested record survives");
  check(round.now == cmd.now, "the proposer's clock reading survives");

  // A byte that names an op past the last one this build knows about must be
  // refused, not silently reinterpreted as whichever enumerator the bit
  // pattern happens to alias.
  txn::TxnCommand garbage;
  const std::string bad_op(1, static_cast<char>(5));
  check(!txn::decode_txn_command(bad_op, &garbage), "an out-of-range op is rejected");
}

void test_txn_result_survives_its_own_encoding() {
  txn::TxnResult result;
  result.outcome = txn::WriteOutcome::kLocked;
  result.status = txn::TxnStatus::kPending;
  result.commit_ts = 0;
  result.have_blocker = true;
  result.blocker = 9;
  result.blocker_epoch = 2;
  result.blocker_start = 88;
  result.blocker_primary = "blocker-primary";

  txn::TxnResult round;
  check(txn::decode_txn_result(txn::encode_txn_result(result), &round),
        "a transactional result must decode");
  check(round.outcome == result.outcome && round.status == result.status,
        "outcome and status survive");
  check(round.have_blocker, "the blocker flag survives");
  check(round.blocker == result.blocker && round.blocker_epoch == result.blocker_epoch &&
            round.blocker_start == result.blocker_start,
        "the blocker's identity survives");
  check(round.blocker_primary == result.blocker_primary, "the blocker's primary key survives");
}

// ---------------------------------------------------------------------------
// the version store: reads
// ---------------------------------------------------------------------------

void commit_version(txn::VersionStore& store, std::string_view key, txn::Ts ts,
                     std::string_view value, txn::TxnId id, bool tombstone = false) {
  txn::Intent intent;
  intent.txn = id;
  intent.epoch = 1;
  intent.start_ts = ts - 1;
  intent.tombstone = tombstone;
  intent.value.assign(value);
  intent.primary = "k";
  txn::Intent blocker;
  store.prewrite(key, intent, &blocker);
  store.commit_intent(key, id, 1, ts);
}

void test_snapshot_reads() {
  txn::VersionStore store;
  commit_version(store, "k", 10, "v10", 1);
  commit_version(store, "k", 20, "v20", 2);
  commit_version(store, "k", 30, "v30", 3);

  struct Case {
    txn::Ts read_ts;
    bool found;
    const char* value;
  };
  // A read resolves to the newest version at or *below* its snapshot. The
  // boundary cases are the ones worth writing down: exactly on a commit
  // timestamp must see that version, one below must not.
  const Case cases[] = {
      {5, false, ""},    {9, false, ""},    {10, true, "v10"}, {19, true, "v10"},
      {20, true, "v20"}, {29, true, "v20"}, {30, true, "v30"}, {99, true, "v30"},
  };
  for (const Case& c : cases) {
    const txn::ReadResult result = store.get("k", c.read_ts, /*reader=*/0, c.read_ts);
    check(result.status == txn::ReadStatus::kOk, "a plain read with no intent in the way is ok");
    check(result.found == c.found,
          "read at " + std::to_string(c.read_ts) + " should find a version: " + (c.found ? "yes" : "no"));
    if (c.found) {
      check(result.value == c.value, "read at " + std::to_string(c.read_ts) + " returns " +
                                          c.value + ", got " + result.value);
    }
  }
}

void test_tombstone_is_a_version() {
  txn::VersionStore store;
  commit_version(store, "k", 10, "v10", 1);
  commit_version(store, "k", 20, "", 2, /*tombstone=*/true);

  const txn::ReadResult after = store.get("k", 25, 0, 25);
  check(!after.found, "a read above a tombstone finds nothing");
  const txn::ReadResult before = store.get("k", 15, 0, 15);
  check(before.found && before.value == "v10",
        "and a read below the tombstone still sees the version it shadows");
}

void test_intents_block_and_read_your_own_writes() {
  txn::VersionStore store;
  commit_version(store, "k", 10, "committed", 1);

  txn::Intent intent;
  intent.txn = 5;
  intent.start_ts = 20;
  intent.value = "uncommitted";
  intent.primary = "k";
  txn::Intent blocker;
  check(store.prewrite("k", intent, &blocker) == txn::WriteOutcome::kOk, "an intent is left");

  // Its owner reads through it.
  const txn::ReadResult own = store.get("k", 25, /*reader=*/5, 25);
  check(own.found && own.value == "uncommitted",
        "a transaction sees its own uncommitted write");

  // Anybody else is blocked rather than being given a guess.
  const txn::ReadResult other = store.get("k", 25, /*reader=*/6, 25);
  check(other.status == txn::ReadStatus::kBlocked && other.blocker == 5,
        "another reader is blocked and told who by");

  // A second writer is refused and told who is in the way.
  txn::Intent rival;
  rival.txn = 7;
  rival.start_ts = 21;
  txn::Intent named_blocker;
  check(store.prewrite("k", rival, &named_blocker) == txn::WriteOutcome::kLocked &&
            named_blocker.txn == 5,
        "a second intent on the same key is refused and names the holder");

  store.commit_intent("k", 5, 1, 30);
  const txn::ReadResult resolved = store.get("k", 35, /*reader=*/6, 35);
  check(resolved.status == txn::ReadStatus::kOk && resolved.found && resolved.value == "uncommitted",
        "once committed, the value is visible to everyone above its timestamp");
  const txn::ReadResult below = store.get("k", 25, /*reader=*/6, 25);
  check(below.found && below.value == "committed",
        "and a snapshot below the commit still sees the older version");
}

void test_uncertainty_forces_a_restart() {
  txn::VersionStore store;
  commit_version(store, "k", 100, "v100", 1);

  // A read at 90 whose uncertainty window reaches 110 cannot rule out that the
  // version at 100 actually happened first in real time. It must restart, not
  // guess.
  const txn::ReadResult uncertain = store.get("k", 90, /*reader=*/2, /*uncertainty_limit=*/110);
  check(uncertain.status == txn::ReadStatus::kUncertain,
        "a version inside the uncertainty window forces a restart");
  check(uncertain.uncertain_at == 100, "and names the version that forced it");

  // Narrow the window below the version and the read proceeds, seeing nothing.
  const txn::ReadResult narrow = store.get("k", 90, /*reader=*/2, /*uncertainty_limit=*/99);
  check(narrow.status == txn::ReadStatus::kOk && !narrow.found,
        "with the window below the version, the read completes and sees nothing");
}

void test_uncertainty_disabled_silently_answers_instead_of_restarting() {
  txn::StoreOptions broken;
  broken.honour_uncertainty = false;
  txn::VersionStore store{broken};
  commit_version(store, "k", 100, "v100", 1);

  const txn::ReadResult result = store.get("k", 90, /*reader=*/2, /*uncertainty_limit=*/110);
  check(result.status == txn::ReadStatus::kOk,
        "the mutant never reports kUncertain -- there is no error to notice");
  check(!result.found,
        "and it silently answers 'nothing here', which may be wrong: the version at 100 "
        "could have been written before this read began in real time");
}

// ---------------------------------------------------------------------------
// the version store: writes
// ---------------------------------------------------------------------------

void test_prewrite_is_idempotent_under_retry() {
  txn::VersionStore store;
  txn::Intent intent;
  intent.txn = 1;
  intent.epoch = 1;
  intent.start_ts = 10;
  intent.value = "v1";
  txn::Intent blocker;
  check(store.prewrite("k", intent, &blocker) == txn::WriteOutcome::kOk, "the first prewrite lands");

  // A retry of the identical command -- same txn, same or newer epoch -- must
  // not be refused. A prewrite is retried whenever its reply is lost, and
  // treating the retry as a conflict would abort transactions for the crime of
  // a dropped packet.
  check(store.prewrite("k", intent, &blocker) == txn::WriteOutcome::kOk,
        "a same-epoch retry from the same transaction is idempotent");

  intent.epoch = 2;
  check(store.prewrite("k", intent, &blocker) == txn::WriteOutcome::kOk,
        "a newer epoch from the same transaction is accepted");

  intent.epoch = 1;
  check(store.prewrite("k", intent, &blocker) == txn::WriteOutcome::kStaleEpoch,
        "an older epoch from the same transaction is refused: that incarnation gave up");
}

void test_first_committer_wins() {
  txn::VersionStore store;
  commit_version(store, "k", 10, "v1", 1);

  txn::Intent late;
  late.txn = 2;
  late.start_ts = 5;  // started before the version at 10 committed
  txn::Intent blocker;
  check(store.prewrite("k", late, &blocker) == txn::WriteOutcome::kConflict,
        "a transaction that started before a version it did not see committed loses");

  txn::Intent onTime;
  onTime.txn = 3;
  onTime.start_ts = 15;  // started after
  check(store.prewrite("k", onTime, &blocker) == txn::WriteOutcome::kOk,
        "a transaction that started after the version is unaffected by it");
}

void test_first_committer_wins_disabled_loses_an_update() {
  txn::StoreOptions broken;
  broken.first_committer_wins = false;
  txn::VersionStore store{broken};
  commit_version(store, "k", 10, "v1", 1);

  txn::Intent late;
  late.txn = 2;
  late.start_ts = 5;
  txn::Intent blocker;
  check(store.prewrite("k", late, &blocker) == txn::WriteOutcome::kOk,
        "the mutant accepts a write that should have conflicted -- the lost update "
        "first-committer-wins exists to prevent");
}

void test_reads_respect_intents_disabled_ignores_a_live_writer() {
  txn::StoreOptions broken;
  broken.reads_respect_intents = false;
  txn::VersionStore store{broken};
  commit_version(store, "k", 10, "committed", 1);

  txn::Intent intent;
  intent.txn = 5;
  intent.start_ts = 20;
  intent.value = "uncommitted";
  txn::Intent blocker;
  store.prewrite("k", intent, &blocker);

  // With the flag on, a concurrent reader is blocked (see
  // test_intents_block_and_read_your_own_writes). With it off, the reader is
  // never told a conflicting write is in flight at all -- it silently gets the
  // older committed value instead of the chance to discover, and possibly
  // resolve, the transaction in its way.
  const txn::ReadResult result = store.get("k", 25, /*reader=*/6, 25);
  check(result.status != txn::ReadStatus::kBlocked,
        "the mutant never blocks -- the concurrent writer is invisible");
  check(result.found && result.value == "committed",
        "and it answers from the stale committed version instead");
}

// ---------------------------------------------------------------------------
// the record status lattice
// ---------------------------------------------------------------------------

void test_record_status_lattice() {
  txn::VersionStore store;

  txn::TxnRecord pending;
  pending.id = 1;
  pending.epoch = 1;
  pending.status = txn::TxnStatus::kPending;
  pending.start_ts = 5;
  check(store.put_record(pending) == txn::WriteOutcome::kOk, "a fresh record is created");
  check(store.find_record(1)->status == txn::TxnStatus::kPending, "and starts pending");

  txn::TxnRecord staging = pending;
  staging.status = txn::TxnStatus::kStaging;
  check(store.put_record(staging) == txn::WriteOutcome::kOk, "pending advances to staging");

  txn::TxnRecord committed = pending;
  committed.status = txn::TxnStatus::kCommitted;
  committed.commit_ts = 100;
  check(store.put_record(committed) == txn::WriteOutcome::kOk, "staging advances to committed");
  check(store.find_record(1)->commit_ts == 100, "with its commit timestamp recorded");

  // Idempotent replay of the exact same terminal state: not a race, a retry.
  check(store.put_record(committed) == txn::WriteOutcome::kOk,
        "replaying the same terminal record is idempotent");

  // A transition out of a terminal state is refused, whatever it claims.
  txn::TxnRecord flipped = pending;
  flipped.status = txn::TxnStatus::kAborted;
  check(store.put_record(flipped) == txn::WriteOutcome::kTerminal,
        "a committed record may not become aborted");
  check(store.find_record(1)->status == txn::TxnStatus::kCommitted,
        "and the record itself is unchanged");

  // A second, independent transaction goes straight from pending to aborted.
  txn::TxnRecord second;
  second.id = 2;
  second.epoch = 1;
  second.status = txn::TxnStatus::kPending;
  store.put_record(second);
  txn::TxnRecord second_aborted = second;
  second_aborted.status = txn::TxnStatus::kAborted;
  check(store.put_record(second_aborted) == txn::WriteOutcome::kOk,
        "pending advances directly to aborted");
  check(store.find_record(2)->status == txn::TxnStatus::kAborted, "and stays there");
}

void test_terminal_status_disabled_lets_a_committed_record_flip() {
  txn::StoreOptions broken;
  broken.terminal_status_is_final = false;
  txn::VersionStore store{broken};

  txn::TxnRecord committed;
  committed.id = 1;
  committed.epoch = 1;
  committed.status = txn::TxnStatus::kCommitted;
  committed.commit_ts = 100;
  store.put_record(committed);

  txn::TxnRecord aborted = committed;
  aborted.status = txn::TxnStatus::kAborted;
  check(store.put_record(aborted) == txn::WriteOutcome::kOk,
        "the mutant accepts a transition out of a terminal state");
  check(store.find_record(1)->status == txn::TxnStatus::kAborted,
        "and the record now disagrees with whatever already resolved it as committed -- "
        "two readers can now be told two different things happened");
}

void test_push_record_advances_and_then_aborts_on_expiry() {
  txn::VersionStore store;
  txn::TxnRecord record;
  record.id = 9;
  record.epoch = 1;
  record.status = txn::TxnStatus::kPending;
  record.heartbeat = 1000;
  record.ttl_nanos = 500;
  store.put_record(record);

  txn::TxnRecord after;
  check(store.push_record(9, /*push_to=*/50, /*now=*/1200, /*abort_expired=*/true, "k", &after) ==
            txn::WriteOutcome::kOk,
        "a push before the deadline succeeds");
  check(after.status == txn::TxnStatus::kPending && after.pushed_to == 50,
        "and only advances the push watermark, since the record is not yet expired");

  check(store.push_record(9, /*push_to=*/60, /*now=*/1600, /*abort_expired=*/true, "k", &after) ==
            txn::WriteOutcome::kOk,
        "a push past heartbeat + ttl succeeds too");
  check(after.status == txn::TxnStatus::kAborted,
        "but this time it finds the record expired and aborts it outright");

  // Once terminal, further pushes just report the verdict.
  check(store.push_record(9, /*push_to=*/70, /*now=*/1700, /*abort_expired=*/false, "k", &after) ==
            txn::WriteOutcome::kOk,
        "pushing an already-terminal record is a no-op that reports the verdict");
  check(after.status == txn::TxnStatus::kAborted, "which stays aborted");
}

void test_push_record_on_a_txn_with_no_record_creates_a_tombstone_abort() {
  txn::VersionStore store;
  check(store.find_record(42) == nullptr, "no record exists yet for this transaction");

  txn::TxnRecord after;
  check(store.push_record(42, /*push_to=*/10, /*now=*/0, /*abort_expired=*/true, "blocked-key",
                          &after) == txn::WriteOutcome::kOk,
        "pushing a transaction that never wrote its primary still succeeds");
  check(after.status == txn::TxnStatus::kAborted,
        "and leaves a tombstone: any later attempt by that transaction to commit will "
        "collide with it, which is what stops a coordinator that died before its first "
        "heartbeat from locking a key forever");
  check(store.find_record(42)->status == txn::TxnStatus::kAborted, "durably, not just in the reply");
  check(!store.find_record(42)->keys.empty() && store.find_record(42)->keys.front() == "blocked-key",
        "and carries the key it was found on as its own location, so a later split or merge "
        "does not strand or lose the tombstone the same way an empty key list would");
}

// ---------------------------------------------------------------------------
// apply_txn_command: the opaque interface a range actually calls
// ---------------------------------------------------------------------------

void test_apply_txn_command_end_to_end() {
  txn::VersionStore store;

  txn::TxnCommand prewrite;
  prewrite.op = txn::TxnOp::kPrewrite;
  prewrite.key = "a";
  prewrite.txn = 1;
  prewrite.epoch = 1;
  prewrite.start_ts = 10;
  prewrite.value = "v";
  prewrite.primary = "a";
  const txn::TxnResult prewrite_result = txn::apply_txn_command(&store, prewrite);
  check(prewrite_result.outcome == txn::WriteOutcome::kOk, "the prewrite applies");

  const txn::ReadResult own = store.get("a", 15, /*reader=*/1, 15);
  check(own.found && own.value == "v", "the writer reads its own uncommitted write");

  txn::TxnCommand put_pending;
  put_pending.op = txn::TxnOp::kPutRecord;
  put_pending.key = "a";
  put_pending.txn = 1;
  put_pending.epoch = 1;
  put_pending.record.id = 1;
  put_pending.record.epoch = 1;
  put_pending.record.status = txn::TxnStatus::kPending;
  put_pending.record.start_ts = 10;
  put_pending.record.heartbeat = 100;
  put_pending.record.ttl_nanos = 1000;
  check(txn::apply_txn_command(&store, put_pending).outcome == txn::WriteOutcome::kOk,
        "the record is created pending");

  txn::TxnCommand commit;
  commit.op = txn::TxnOp::kCommitIntent;
  commit.key = "a";
  commit.txn = 1;
  commit.epoch = 1;
  commit.commit_ts = 20;
  check(txn::apply_txn_command(&store, commit).outcome == txn::WriteOutcome::kOk,
        "the intent resolves to a version");

  txn::TxnCommand put_committed = put_pending;
  put_committed.record.status = txn::TxnStatus::kCommitted;
  put_committed.record.commit_ts = 20;
  const txn::TxnResult final_result = txn::apply_txn_command(&store, put_committed);
  check(final_result.outcome == txn::WriteOutcome::kOk &&
            final_result.status == txn::TxnStatus::kCommitted && final_result.commit_ts == 20,
        "the record moves to committed and reports its own new state");

  const txn::ReadResult everyone = store.get("a", 25, /*reader=*/2, 25);
  check(everyone.status == txn::ReadStatus::kOk && everyone.found && everyone.value == "v" &&
            everyone.commit_ts == 20,
        "and the value is now visible to anyone above the commit timestamp");
}

void test_apply_txn_command_reports_the_blocker_on_a_locked_prewrite() {
  txn::VersionStore store;

  txn::TxnCommand first;
  first.op = txn::TxnOp::kPrewrite;
  first.key = "c";
  first.txn = 10;
  first.epoch = 1;
  first.start_ts = 5;
  first.value = "x";
  first.primary = "c";
  check(txn::apply_txn_command(&store, first).outcome == txn::WriteOutcome::kOk,
        "the first prewrite lands");

  txn::TxnCommand second;
  second.op = txn::TxnOp::kPrewrite;
  second.key = "c";
  second.txn = 11;
  second.epoch = 1;
  second.start_ts = 6;
  second.value = "y";
  second.primary = "c";
  const txn::TxnResult result = txn::apply_txn_command(&store, second);
  check(result.outcome == txn::WriteOutcome::kLocked, "the second prewrite is locked out");
  check(result.have_blocker && result.blocker == 10,
        "and the caller is told exactly who to go and push, without a second round trip");
}

// ---------------------------------------------------------------------------
// splitting, merging
// ---------------------------------------------------------------------------

void test_encode_span_partitions_by_key() {
  txn::VersionStore store;
  commit_version(store, "a", 10, "va", 1);
  commit_version(store, "b", 10, "vb", 2);
  commit_version(store, "c", 10, "vc", 3);
  commit_version(store, "d", 10, "vd", 4);

  const std::string left = store.encode_span("", "c");
  txn::VersionStore left_store;
  check(left_store.load(left), "the left half decodes");
  check(left_store.key_count() == 2, "and holds exactly the keys below 'c'");
  check(left_store.get("a", 10, 0, 10).found && left_store.get("b", 10, 0, 10).found,
        "namely a and b");
  check(!left_store.get("c", 10, 0, 10).found && !left_store.get("d", 10, 0, 10).found,
        "and not c or d");

  const std::string right = store.encode_span("c", "");
  txn::VersionStore right_store;
  check(right_store.load(right), "the right half decodes");
  check(right_store.key_count() == 2, "and holds exactly c and d");
}

void test_absorb_does_not_resurrect_a_terminal_record() {
  txn::VersionStore incoming;
  txn::TxnRecord pending;
  pending.id = 1;
  pending.epoch = 1;
  pending.status = txn::TxnStatus::kPending;
  pending.keys = {"p"};
  incoming.put_record(pending);
  const std::string payload = incoming.encode_span("", "");

  txn::VersionStore survivor;
  txn::TxnRecord committed;
  committed.id = 1;
  committed.epoch = 1;
  committed.status = txn::TxnStatus::kCommitted;
  committed.commit_ts = 500;
  committed.keys = {"p"};
  survivor.put_record(committed);

  check(survivor.absorb(payload), "the merge payload decodes");
  check(survivor.find_record(1)->status == txn::TxnStatus::kCommitted,
        "a terminal verdict already present is not overwritten by an incoming pending one -- "
        "absorbing a merge payload must not un-commit a transaction");
}

// ---------------------------------------------------------------------------
// GC: the boundary version, the same property INV-MVCC-01 defends for MVCC
// ---------------------------------------------------------------------------

void test_collect_keeps_the_boundary_version() {
  txn::VersionStore store;
  commit_version(store, "k", 10, "v10", 1);
  commit_version(store, "k", 20, "v20", 2);
  commit_version(store, "k", 30, "v30", 3);

  const std::uint64_t collected = store.collect(/*safepoint=*/25);
  check(collected == 1,
        "only the unreachable version (10) goes; a reader at 25 resolves to 20, and 30 is "
        "above the safepoint (got " + std::to_string(collected) + ")");

  const txn::ReadResult at_safepoint = store.get("k", 25, 0, 25);
  check(at_safepoint.found && at_safepoint.value == "v20",
        "a reader sitting exactly on the safepoint still resolves to its version");
  const txn::ReadResult above = store.get("k", 35, 0, 35);
  check(above.found && above.value == "v30", "and a newer reader is unaffected");
}

// ---------------------------------------------------------------------------
// the hybrid logical clock
// ---------------------------------------------------------------------------

void test_hlc_is_strictly_monotonic_even_under_a_repeated_or_regressing_wall_clock() {
  txn::HybridClock clock{Duration::millis(5)};
  const txn::Ts first = clock.now(Timestamp{1000, 0});
  const txn::Ts second = clock.now(Timestamp{1000, 0});  // same wall reading twice
  check(second > first, "two readings in the same instant still produce distinct timestamps");

  const txn::Ts third = clock.now(Timestamp{500, 0});  // the wall clock went backwards
  check(third > second,
        "and a wall clock that goes backwards does not make the sequence go backwards");
}

void test_hlc_observe_pulls_the_local_clock_forward() {
  txn::HybridClock clock{Duration::millis(5)};
  const txn::Ts local = clock.now(Timestamp{100, 0});
  const txn::Ts remote = local + 1'000'000;  // a message from a node far ahead

  const txn::Ts observed = clock.observe(remote, Timestamp{100, 0});
  check(observed > remote, "observing a remote timestamp adopts it and moves strictly past it");
  const txn::Ts next = clock.now(Timestamp{100, 0});
  check(next > observed, "and the adoption sticks: the next local reading is still above it");
}

void test_hlc_interval_bounds_and_clamps() {
  txn::HybridClock clock{Duration::millis(5)};
  const txn::Ts ts = clock.now(Timestamp{10'000'000'000ULL, 0});  // 10 seconds
  const txn::TsInterval interval = clock.interval(ts);
  check(interval.earliest < ts && ts < interval.latest,
        "the reading itself sits strictly inside its own declared interval");
  const txn::Ts span = txn::pack_hlc(5'000'000ULL, 0);
  check(interval.width() == 2 * span,
        "the interval is symmetric and exactly twice the declared bound wide");

  // Clamping at the edges of the type: a bound that would push earliest below
  // kMinTs or latest above kMaxTs must clamp rather than wrap.
  const txn::TsInterval low = clock.interval(txn::kMinTs);
  check(low.earliest == txn::kMinTs, "the interval never claims a timestamp below kMinTs");
  const txn::TsInterval high = clock.interval(txn::kMaxTs);
  check(high.latest == txn::kMaxTs, "nor one above kMaxTs");
}

// ---------------------------------------------------------------------------
// the replicated timestamp oracle -- INV-TXN-09
// ---------------------------------------------------------------------------

void test_oracle_reservations_are_disjoint_and_monotonic() {
  txn::OracleMachine oracle;
  std::vector<std::pair<txn::Ts, std::uint64_t>> reservations;
  oracle.set_reserved_callback(
      [&](txn::Ts first, std::uint64_t count) { reservations.emplace_back(first, count); });

  oracle.apply(LogIndex{1}, txn::encode_reservation(10));
  check(oracle.high_water() == 11, "high_water starts at kMinTs and advances by the count");
  check(reservations.size() == 1 && reservations[0].first == txn::kMinTs + 1 &&
            reservations[0].second == 10,
        "the callback is told exactly the half-open range that was just reserved");

  oracle.apply(LogIndex{2}, txn::encode_reservation(5));
  check(oracle.high_water() == 16, "the next reservation starts exactly where the last one ended");
  check(reservations[1].first == 12 && reservations[1].second == 5,
        "with no gap and no overlap between the two batches");
}

void test_oracle_saturates_instead_of_wrapping() {
  txn::OracleMachine oracle;
  oracle.apply(LogIndex{1}, txn::encode_reservation(txn::kMaxTs));
  check(oracle.high_water() == txn::kMaxTs,
        "a reservation past the maximum saturates at kMaxTs instead of wrapping around to a "
        "small number, which would be a timestamp regression wearing a big number's clothes");
}

void test_oracle_snapshot_never_regresses_on_restore() {
  txn::OracleMachine oracle;
  oracle.apply(LogIndex{1}, txn::encode_reservation(20));
  check(oracle.high_water() == 21, "reserved up to 21");
  const std::string snapshot = oracle.snapshot();

  // A stale snapshot -- from a leader since superseded -- must not pull the
  // high-water mark backward on whoever restores it.
  std::string stale;
  lsm::put_varint64(&stale, 5);
  oracle.restore(stale);
  check(oracle.high_water() == 21,
        "restoring an older snapshot does not regress the high-water mark");

  txn::OracleMachine fresh;
  fresh.restore(snapshot);
  check(fresh.high_water() == 21, "but restoring a real, newer snapshot does adopt it");
}

}  // namespace

int main() {
  std::cout << "transaction mechanism unit tests\n";

  test_terminal_helper();
  test_record_expiry();

  test_record_survives_its_own_encoding();
  test_intent_survives_its_own_encoding();
  test_txn_command_survives_its_own_encoding();
  test_txn_result_survives_its_own_encoding();

  test_snapshot_reads();
  test_tombstone_is_a_version();
  test_intents_block_and_read_your_own_writes();
  test_uncertainty_forces_a_restart();
  test_uncertainty_disabled_silently_answers_instead_of_restarting();

  test_prewrite_is_idempotent_under_retry();
  test_first_committer_wins();
  test_first_committer_wins_disabled_loses_an_update();
  test_reads_respect_intents_disabled_ignores_a_live_writer();

  test_record_status_lattice();
  test_terminal_status_disabled_lets_a_committed_record_flip();
  test_push_record_advances_and_then_aborts_on_expiry();
  test_push_record_on_a_txn_with_no_record_creates_a_tombstone_abort();

  test_apply_txn_command_end_to_end();
  test_apply_txn_command_reports_the_blocker_on_a_locked_prewrite();

  test_encode_span_partitions_by_key();
  test_absorb_does_not_resurrect_a_terminal_record();
  test_collect_keeps_the_boundary_version();

  test_hlc_is_strictly_monotonic_even_under_a_repeated_or_regressing_wall_clock();
  test_hlc_observe_pulls_the_local_clock_forward();
  test_hlc_interval_bounds_and_clamps();
  test_oracle_reservations_are_disjoint_and_monotonic();
  test_oracle_saturates_instead_of_wrapping();
  test_oracle_snapshot_never_regresses_on_restore();

  if (g_failures == 0) {
    std::cout << "transaction mechanism unit tests: all checks passed\n";
    return EXIT_SUCCESS;
  }
  std::cerr << "transaction mechanism unit tests: " << g_failures << " check(s) failed\n";
  return EXIT_FAILURE;
}
