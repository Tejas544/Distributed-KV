// Recorded histories, and the serial model they are checked against.
//
// The workload here is **list-append**, and that choice is the single most
// important decision in the whole checker.
//
// The naive alternative is a register workload: write(k, v), read(k) -> v. It
// is easy to generate and nearly useless to check, because when a read returns
// 7 you learn only that *somebody* wrote 7 -- not when, not relative to what
// else. Recovering the version order then requires searching over every
// possible ordering, which is exponential and is why the older checkers gave up
// on anything but tiny histories.
//
// With list-append, a value is a list and a write is `append(k, e)`. A read
// returns the entire list, so the version order for that key is *directly
// observable*: if a read returns [3, 1, 4], then 3 was appended before 1 which
// was appended before 4. The dependency graph can be built by inspection rather
// than by search. Everything else in elle.h follows from that.
//
// Elements are globally unique, which is what makes `element -> writer` a
// function. A duplicate element is itself a reportable anomaly: either the
// generator is broken or the database applied a write twice.

#ifndef ANVIL_CHECKER_HISTORY_H_
#define ANVIL_CHECKER_HISTORY_H_

#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "anvil/core/types.h"

namespace anvil::checker {

using KeyId = std::uint64_t;
using Element = std::uint64_t;
using TxnId = std::uint64_t;

enum class MopType : std::uint8_t { kAppend, kRead };

struct Mop {
  MopType type = MopType::kRead;
  KeyId key = 0;
  Element element = 0;            // kAppend: what was appended
  std::vector<Element> observed;  // kRead: the whole list, in order
};

// Three outcomes, not two. A transaction whose commit never returned an answer
// is neither committed nor aborted, and a checker that forces it into one bucket
// will either invent anomalies or hide them. Real clients see this constantly
// -- a timeout on commit says nothing about whether the commit happened -- and
// pretending otherwise is how consistency checkers end up disagreeing with each
// other about perfectly correct systems.
enum class Outcome : std::uint8_t { kCommitted, kAborted, kUnknown };

const char* to_string(Outcome outcome) noexcept;

struct Txn {
  TxnId id = 0;
  std::uint64_t process = 0;  // which client issued it; concurrency comes from here
  Timestamp invoked;
  Timestamp completed;
  Outcome outcome = Outcome::kUnknown;
  std::vector<Mop> mops;

  // The last element this transaction appended to `key`, or 0 if none. Used to
  // tell an intermediate read (G1b) from a final one.
  Element final_append_to(KeyId key) const;
  bool appends_to(KeyId key) const;
};

class History {
 public:
  TxnId begin(std::uint64_t process, Timestamp at);
  void append(TxnId txn, KeyId key, Element element);
  void read(TxnId txn, KeyId key, std::vector<Element> observed);
  void complete(TxnId txn, Outcome outcome, Timestamp at);

  const std::vector<Txn>& txns() const noexcept { return txns_; }
  const Txn* find(TxnId id) const;
  std::size_t size() const noexcept { return txns_.size(); }
  bool empty() const noexcept { return txns_.empty(); }

  // Appends a fully-formed transaction. Used by the corpus, which constructs
  // pathological histories directly rather than by running anything.
  TxnId add(Txn txn);

  std::string render() const;

 private:
  Txn* mutable_find(TxnId id);

  std::vector<Txn> txns_;
  TxnId next_id_ = 1;
};

// ---------------------------------------------------------------------------
// The reference model
// ---------------------------------------------------------------------------

// A single map and no concurrency whatsoever. Deliberately the most boring
// implementation possible: its only job is to be obviously correct, so that any
// history it produces is by construction serializable and can be used to test
// the checker for *false positives*.
//
// A checker that flags nothing is useless; a checker that flags everything is
// worse, because it will be switched off. INV-SIM-03 and INV-SIM-04 are the two
// halves, and this class is the second one.
class ReferenceModel {
 public:
  // Applies a transaction serially, filling in the `observed` field of every
  // read from the current state. Returns the completed mop list.
  std::vector<Mop> apply(const std::vector<Mop>& mops);

  const std::map<KeyId, std::vector<Element>>& state() const noexcept { return state_; }
  void reset() { state_.clear(); }

 private:
  std::map<KeyId, std::vector<Element>> state_;
};

}  // namespace anvil::checker

#endif  // ANVIL_CHECKER_HISTORY_H_
