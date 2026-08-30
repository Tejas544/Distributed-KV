// Membership and quorum arithmetic.
//
// Two configurations, not one. Outside a membership change `outgoing` is empty
// and a quorum is a majority of `incoming`. During a change both are populated
// and a quorum must be a majority of *each* -- which is the entire safety
// argument for joint consensus, because any C_old-majority and any C_new-
// majority both contain a member of the joint quorum, so two leaders cannot be
// elected across the boundary and no committed entry can be lost.
//
// The one-at-a-time shortcut (add or remove a single server, no joint state) is
// deliberately not implemented. It has a known unsafe case when a second change
// starts before the first commits, and every implementation that takes the
// shortcut has to bolt on a rule -- "no new change until the previous one is
// committed and the leader has committed an entry of its own term" -- that is
// harder to state correctly than joint consensus is to implement.
//
// Learners are members that receive the log and are never counted anywhere. The
// counting rule lives in exactly one place, `has_quorum`, so INV-RAFT-15 is a
// property of one function rather than a discipline spread across the code.

#ifndef ANVIL_CORE_RAFT_CONFIG_H_
#define ANVIL_CORE_RAFT_CONFIG_H_

#include <cstdint>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "anvil/core/raft/types.h"
#include "anvil/core/types.h"

namespace anvil::raft {

class Config {
 public:
  Config() = default;

  static Config from_voters(const std::vector<std::uint64_t>& voters);
  static Config from_conf_state(const ConfState& state);
  ConfState to_conf_state() const;

  bool joint() const noexcept { return !outgoing_.empty(); }
  bool empty() const noexcept { return incoming_.empty() && outgoing_.empty(); }

  const std::set<std::uint64_t>& incoming() const noexcept { return incoming_; }
  const std::set<std::uint64_t>& outgoing() const noexcept { return outgoing_; }
  const std::set<std::uint64_t>& learners() const noexcept { return learners_; }

  bool is_voter(NodeId node) const;
  bool is_learner(NodeId node) const;
  // Voter or learner: everyone the leader replicates to.
  bool is_member(NodeId node) const;

  // Every peer this node talks to, in ascending id order. Ordered because
  // "iterate the peers" appears in the hot path of replication and an
  // unordered_set here would make message order depend on a hash seed
  // (docs/INVARIANTS.md, INV-SIM-01).
  std::vector<NodeId> members() const;
  std::vector<NodeId> voters() const;

  // The quorum rule. `acked` may contain learners and strangers; both are
  // filtered out here rather than by the caller, because a caller that forgets
  // is a silent safety bug and there is no reason to have two of them.
  //
  // `count_learners` exists only so the mutation drill can turn the filter off
  // and watch INV-RAFT-15 fire. It defaults to the correct behaviour.
  bool has_quorum(const std::set<std::uint64_t>& acked, bool count_learners = false) const;

  // The highest index replicated on a quorum, given each member's match index.
  // For a joint configuration this is the smaller of the two configurations'
  // answers -- an entry is committed only when both halves agree.
  LogIndex committed_index(const std::map<std::uint64_t, LogIndex>& match,
                           bool count_learners = false) const;

  // Applies a conf change, producing the next configuration. kEnterJoint moves
  // the current voters into `outgoing`; kLeaveJoint drops them.
  Config apply(const ConfChange& change) const;

  std::string describe() const;

  friend bool operator==(const Config&, const Config&) noexcept = default;

 private:
  static bool majority(const std::set<std::uint64_t>& voters,
                       const std::set<std::uint64_t>& acked);
  static LogIndex majority_index(const std::set<std::uint64_t>& voters,
                                 const std::map<std::uint64_t, LogIndex>& match,
                                 const std::set<std::uint64_t>* also_counted = nullptr);

  std::set<std::uint64_t> incoming_;
  std::set<std::uint64_t> outgoing_;
  std::set<std::uint64_t> learners_;
};

// Encoding for ConfState and ConfChange, used in snapshots and log entries.
std::string encode_conf_state(const ConfState& state);
bool decode_conf_state(const std::string& in, ConfState* out);
std::string encode_conf_change(const ConfChange& change);
bool decode_conf_change(const std::string& in, ConfChange* out);

}  // namespace anvil::raft

#endif  // ANVIL_CORE_RAFT_CONFIG_H_
