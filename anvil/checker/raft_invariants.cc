#include "anvil/checker/raft_invariants.h"

#include <algorithm>

#include "anvil/core/digest.h"

namespace anvil::checker {
namespace {

std::uint64_t digest_of(const raft::LogEntry& entry) {
  Digest d;
  d.mix(static_cast<std::uint64_t>(entry.type)).mix(std::string_view{entry.data});
  return d.low();
}

std::string node_name(NodeId id) { return "n" + std::to_string(id.value()); }

std::string index_term(LogIndex index, Term term) {
  return std::to_string(index.value()) + "@" + std::to_string(term.value());
}

// A short, printable description of an entry, so a Log Matching report names
// what actually differs instead of leaving it to be reproduced.
std::string shape_of(const raft::LogEntry& entry) {
  std::string out = raft::to_string(entry.type);
  out += "[";
  for (std::size_t i = 0; i < entry.data.size() && i < 12; ++i) {
    constexpr char kHex[] = "0123456789abcdef";
    const auto byte = static_cast<unsigned char>(entry.data[i]);
    out.push_back(kHex[byte >> 4]);
    out.push_back(kHex[byte & 0xF]);
  }
  out += "]";
  return out;
}

}  // namespace

// ---------------------------------------------------------------------------
// observer
// ---------------------------------------------------------------------------

void RaftObserver::configure(std::uint32_t nodes, Hooks hooks) {
  nodes_ = nodes;
  hooks_ = std::move(hooks);
  for (std::uint32_t i = 1; i <= nodes; ++i) mirrors_[i];
}

void RaftObserver::set_node(NodeId id, const raft::RaftNode* node) {
  Mirror& mirror = mirrors_[id.value()];
  mirror.node = node;
  mirror.rebooted = true;
  mirror.revision = UINT64_MAX;  // force a scan
  // A restart builds a new state machine whose configuration is re-derived from
  // durable state. The observer's own derivation has to restart with it, or the
  // first scan after every crash reports a configuration mismatch that is just
  // the two of them counting from different places.
  mirror.derived_valid = false;
}

void RaftObserver::note_corruption(NodeId id) {
  // Only when the run could actually have damaged the media. Otherwise a
  // truncated log means the tail was never durable, which is a finding.
  if (media_faults_) mirrors_[id.value()].corrupted = true;
}

LogIndex RaftObserver::commit_at_election(NodeId id) const {
  const auto it = mirrors_.find(id.value());
  return it == mirrors_.end() ? LogIndex{} : it->second.commit_at_election;
}

bool RaftObserver::corrupted(NodeId id) const {
  const auto it = mirrors_.find(id.value());
  return it != mirrors_.end() && it->second.corrupted;
}

// A node counts as observable only once it has recovered.
//
// The observer is handed the pointer at boot, and recovery is asynchronous
// because it reads a disk that can be slow, can return EIO, and may have to be
// retried. In that window the node exists, is alive, and holds nothing -- no
// configuration, no log, no term. Reading it is reading a node that has not
// started yet, and every predicate that asks "does a quorum hold this entry?"
// answers no for reasons that have nothing to do with the protocol.
//
// Not-yet-recovered is therefore treated exactly like not-running: unknown, and
// counted in the system's favour wherever that is the conservative direction.
bool ready_to_observe(const raft::RaftNode* node) {
  return node != nullptr && !node->config().empty();
}

const raft::RaftNode* RaftObserver::node(NodeId id) const {
  const auto it = mirrors_.find(id.value());
  if (it == mirrors_.end()) return nullptr;
  if (hooks_.alive && !hooks_.alive(id)) return nullptr;
  return ready_to_observe(it->second.node) ? it->second.node : nullptr;
}

std::vector<NodeId> RaftObserver::live_nodes() const {
  std::vector<NodeId> out;
  for (const auto& [id, mirror] : mirrors_) {
    if (!ready_to_observe(mirror.node)) continue;
    const NodeId node_id{id};
    if (hooks_.alive && !hooks_.alive(node_id)) continue;
    out.push_back(node_id);
  }
  return out;
}

Timestamp RaftObserver::true_now() const {
  return hooks_.true_now ? hooks_.true_now() : Timestamp{};
}

Timestamp RaftObserver::node_now(NodeId id) const {
  return hooks_.node_now ? hooks_.node_now(id) : true_now();
}

void RaftObserver::record(const std::string& id, std::string detail) {
  auto& queue = pending_[id];
  // Bounded. After the first violation the run stops anyway, and an unbounded
  // queue would let a cascade of consequences bury the cause.
  if (queue.size() < 4) queue.push_back(std::move(detail));
}

std::optional<std::string> RaftObserver::take(const std::string& id) {
  const auto it = pending_.find(id);
  if (it == pending_.end() || it->second.empty()) return std::nullopt;
  std::string out = std::move(it->second.front());
  it->second.erase(it->second.begin());
  return out;
}

void RaftObserver::refresh() {
  // One scan per scheduler event, not one per predicate. Fifteen predicates
  // each doing an O(nodes) diff would be fifteen times the cost for exactly the
  // same answer.
  const std::uint64_t tick = hooks_.tick ? hooks_.tick() : 0;
  if (tick == last_tick_) return;
  last_tick_ = tick;
  ++counters_.scans;

  for (auto& [raw_id, mirror] : mirrors_) {
    const NodeId id{raw_id};
    if (!ready_to_observe(mirror.node)) continue;
    if (hooks_.alive && !hooks_.alive(id)) continue;
    if (mirror.node->revision() == mirror.revision) continue;
    scan_node(id, mirror);
    mirror.revision = mirror.node->revision();
  }
}

// Who voted for whom in a term, and whether they held the entry in question.
// A Leader Completeness violation is either an illegitimate election or an
// illegitimate commit, and the votes are what tell the two apart.
std::string RaftObserver::vote_report(Term term, NodeId winner) const {
  std::string out = "; votes in term " + std::to_string(term.value()) + ":";
  for (const auto& [raw_id, mirror] : mirrors_) {
    const auto it = votes_.find({raw_id, term.value()});
    out += " n" + std::to_string(raw_id) + "->";
    out += it == votes_.end() ? std::string("-") : ("n" + std::to_string(it->second));
    if (it != votes_.end() && it->second == winner.value()) out += "*";
    if (mirror.node != nullptr) {
      out += "(last=" + std::to_string(mirror.node->log().last_index().value()) + "@" +
             std::to_string(mirror.node->log().last_term().value()) + ")";
    }
  }
  return out;
}

void RaftObserver::scan_node(NodeId id, Mirror& mirror) {
  const raft::RaftNode& node = *mirror.node;
  const raft::RaftLog& log = node.log();

  // ---- did the log move backwards? ---------------------------------------
  if (mirror.seen) {
    bool rewind = false;
    if (log.last_index() < mirror.scanned_to) {
      rewind = true;
    } else if (mirror.scanned_to > log.snapshot_index()) {
      Term found{};
      if (!log.term_at(mirror.scanned_to, &found) || found != mirror.scanned_to_term) {
        rewind = true;
      }
    }
    if (rewind) {
      // INV-RAFT-02. A leader never overwrites or deletes entries in its own
      // log; only a follower reconciling with a leader may. Same node, same
      // term, still leader, and the log shrank: that is the violation.
      if (mirror.role == raft::Role::kLeader && node.role() == raft::Role::kLeader &&
          mirror.role_term == node.term()) {
        record("INV-RAFT-02", node_name(id) + " was leader in term " +
                                  std::to_string(node.term().value()) +
                                  " and truncated its own log from " +
                                  std::to_string(mirror.scanned_to.value()) + " to " +
                                  std::to_string(log.last_index().value()));
      }
      mirror.scanned_to = log.snapshot_index();
      ++counters_.rescans_after_truncation;
    }
  }

  if (mirror.scanned_to < log.snapshot_index()) {
    if (mirror.seen && mirror.scanned_to.value() + 1 < log.first_index().value()) {
      // Compaction discarded entries before this scan reached them. Counted
      // rather than ignored: if it ever becomes common, the tick-class checks
      // are silently sampling instead of covering.
      ++counters_.compaction_gaps;
    }
    mirror.scanned_to = log.snapshot_index();
  }

  // ---- did a restart lose durable entries? -------------------------------
  //
  // This is INV-RAFT-09's per-node half, and it is the one that catches a
  // missing fsync. Everything this node ever had durably must still be there
  // after a restart: an entry it had persisted, and therefore may have
  // acknowledged, cannot go missing because the process died. Only detected
  // media damage excuses it.
  if (mirror.rebooted) {
    mirror.rebooted = false;
    if (!mirror.corrupted && mirror.max_persisted > log.last_index() &&
        mirror.max_persisted > log.snapshot_index()) {
      record("INV-RAFT-09",
             node_name(id) + " came back with its log ending at " +
                 std::to_string(log.last_index().value()) + " after having " +
                 std::to_string(mirror.max_persisted.value()) +
                 " durably persisted -- entries it may already have acknowledged are gone");
    }
  }
  mirror.max_persisted = std::max(mirror.max_persisted, log.persisted_index());

  // ---- new entries -------------------------------------------------------
  for (LogIndex i{mirror.scanned_to.value() + 1}; i <= log.last_index();
       i = LogIndex{i.value() + 1}) {
    const raft::LogEntry* entry = log.at(i);
    if (entry == nullptr) break;
    ++counters_.entries_scanned;

    const std::uint64_t d = digest_of(*entry);
    const auto key = std::pair<std::uint64_t, std::uint64_t>{i.value(), entry->term.value()};
    const auto [it, inserted] = entries_.emplace(key, d);
    if (!inserted && it->second != d) {
      record("INV-RAFT-16",
             node_name(id) + " holds a different command at " + index_term(i, entry->term) +
                 " than another node did -- same index, same term, two entries: n" +
                 std::to_string(entry_origin_[key]) + " had " + entry_shape_[key] +
                 ", " + node_name(id) + " has " + shape_of(*entry));
    }
    if (inserted) {
      entry_origin_[key] = id.value();
      entry_shape_[key] = shape_of(*entry);
    }
    mirror.scanned_to = i;
    mirror.scanned_to_term = entry->term;
  }

  // ---- term, vote, commit ------------------------------------------------
  //
  // All three are judged on the *durable* record, not on what this incarnation
  // happens to hold in memory. The driver sends nothing before its fsync, so a
  // decision that never reached the disk also never reached a peer; losing it
  // in a crash is invisible to the cluster and is not a regression. What would
  // be a regression is the durable record going backwards, and that is checked
  // exactly.
  const raft::HardState& durable = node.persisted_hard_state();

  if (durable.term < mirror.max_term && !mirror.corrupted) {
    record("INV-RAFT-06", node_name(id) + " durable term went back from " +
                              std::to_string(mirror.max_term.value()) + " to " +
                              std::to_string(durable.term.value()) +
                              " (a term must survive a restart)");
  }
  mirror.max_term = std::max(mirror.max_term, durable.term);

  // The other half of the same property, and the cheap one: a node must never
  // be running at a term *below* what it has already written down. That would
  // mean recovery read its own record and ignored it.
  if (node.term() < durable.term) {
    record("INV-RAFT-06", node_name(id) + " is running at term " +
                              std::to_string(node.term().value()) +
                              " with term " + std::to_string(durable.term.value()) +
                              " already durable");
  }

  if (durable.vote.valid()) {
    const auto key =
        std::pair<std::uint64_t, std::uint64_t>{id.value(), durable.term.value()};
    const auto [it, inserted] = votes_.emplace(key, durable.vote.value());
    if (!inserted && it->second != durable.vote.value()) {
      record("INV-RAFT-07", node_name(id) + " durably voted for n" +
                                std::to_string(it->second) + " and then for n" +
                                std::to_string(durable.vote.value()) + " in term " +
                                std::to_string(durable.term.value()));
    }
  }

  if (durable.commit < mirror.max_commit_durable && !mirror.corrupted) {
    record("INV-RAFT-08", node_name(id) + " durable commit index went back from " +
                              std::to_string(mirror.max_commit.value()) + " to " +
                              std::to_string(durable.commit.value()));
  }
  mirror.max_commit_durable = std::max(mirror.max_commit_durable, durable.commit);

  // ---- committed entries, and agreement about them -----------------------
  const LogIndex from{std::max(mirror.max_commit.value() + 1, log.first_index().value())};
  for (LogIndex i = from; i <= log.commit_index(); i = LogIndex{i.value() + 1}) {
    const raft::LogEntry* entry = log.at(i);
    if (entry == nullptr) continue;
    const std::uint64_t d = digest_of(*entry);
    const auto [it, inserted] =
        committed_.emplace(i.value(), std::pair<Term, std::uint64_t>{entry->term, d});
    if (inserted) committed_origin_[i.value()] = {id.value(), shape_of(*entry)};
    if (!inserted && (it->second.first != entry->term || it->second.second != d)) {
      // INV-RAFT-05, State Machine Safety: two nodes have committed different
      // commands at the same index. This is the one that eats data.
      record("INV-RAFT-05",
             node_name(id) + " committed " + index_term(i, entry->term) +
                 " where another node committed term " +
                 std::to_string(it->second.first.value()) + " -- divergent state machines");
    }
  }
  mirror.max_commit = std::max(mirror.max_commit, log.commit_index());

  // ---- leadership --------------------------------------------------------
  if (node.role() == raft::Role::kLeader) {
    const auto [it, inserted] = leaders_.emplace(node.term().value(), id.value());
    if (!inserted && it->second != id.value()) {
      record("INV-RAFT-01", "term " + std::to_string(node.term().value()) +
                                " has two leaders: n" + std::to_string(it->second) + " and " +
                                node_name(id));
    }
    if (inserted) {
      ++counters_.leaders_seen;
      ++counters_.elections_checked;
      // INV-RAFT-04, Leader Completeness. Every entry committed in an earlier
      // term must be present, unmodified, in this leader's log. No client-facing
      // test can ask this question: by the time a client could observe the
      // difference, the new leader has already overwritten the evidence.
      for (const auto& [index, value] : committed_) {
        if (value.first >= node.term()) continue;
        if (index <= log.snapshot_index().value()) continue;  // subsumed by its snapshot
        const raft::LogEntry* entry = log.at(LogIndex{index});
        if (entry == nullptr) {
          record("INV-RAFT-04",
                 node_name(id) + " became leader in term " +
                     std::to_string(node.term().value()) + " without committed entry " +
                     std::to_string(index) + " (term " + std::to_string(value.first.value()) +
                     ")");
          break;
        }
        if (entry->term != value.first || digest_of(*entry) != value.second) {
          const auto& origin = committed_origin_[index];
          record("INV-RAFT-04",
                 node_name(id) + " became leader in term " +
                     std::to_string(node.term().value()) +
                     " holding a different entry at index " + std::to_string(index) +
                     " than the one committed: n" + std::to_string(origin.first) +
                     " committed " + index_term(LogIndex{index}, value.first) + " " +
                     origin.second + ", " + node_name(id) + " holds " +
                     index_term(LogIndex{index}, entry->term) + " " + shape_of(*entry) +
                     "; its log is [" + std::to_string(log.first_index().value()) + ".." +
                     std::to_string(log.last_index().value()) + "] snap=" +
                     std::to_string(log.snapshot_index().value()) + " commit=" +
                     std::to_string(log.commit_index().value()) + vote_report(node.term(), id));
          break;
        }
      }
    }
    if (mirror.role != raft::Role::kLeader || mirror.role_term != node.term()) {
      // Newly leader: remember where its commit index started. A leader
      // *inherits* a commit index that may point at an older term's entry, and
      // that is legal; what is not legal is advancing onto one.
      mirror.commit_at_election = log.commit_index();
      mirror.commit_seen_as_leader = log.commit_index();
    } else if (log.commit_index() > mirror.commit_seen_as_leader) {
      // Both leader-side properties are judged here, at the tick the commit
      // advanced, and against the configuration in force at that moment.
      // Re-deriving them later would compare a decision made under one
      // membership against a different one -- and with membership churn
      // running, "a voter that has since become a learner" turns a correct
      // commit into a reported violation.
      Term at_commit{};
      if (log.term_at(log.commit_index(), &at_commit) && at_commit != node.term()) {
        record("INV-RAFT-10",
               node_name(id) + " (leader, term " + std::to_string(node.term().value()) +
                   ") advanced its commit index to " +
                   std::to_string(log.commit_index().value()) + ", an entry from term " +
                   std::to_string(at_commit.value()) + " -- the Figure-8 hazard");
      }

      // INV-RAFT-09, checked at the instant of the decision rather than long
      // afterwards. A quorum must *durably* hold the entry being committed;
      // nodes that are down or still recovering are unknown and counted in the
      // system's favour, so a report here means the live evidence proves no
      // quorum has it.
      {
        Term at_commit{};
        if (log.term_at(log.commit_index(), &at_commit)) {
          std::set<std::uint64_t> holders;
          std::string witness;
          for (std::uint32_t i = 1; i <= nodes_; ++i) {
            const auto peer = mirrors_.find(i);
            const raft::RaftNode* other =
                peer == mirrors_.end() ? nullptr : peer->second.node;
            if (!ready_to_observe(other) ||
                (hooks_.alive && !hooks_.alive(NodeId{i}))) {
              holders.insert(i);  // unknown
              witness += " n" + std::to_string(i) + "=?";
              continue;
            }
            const bool has = log.commit_index() <= other->log().snapshot_index() ||
                             (other->log().persisted_index() >= log.commit_index() &&
                              other->log().match(log.commit_index(), at_commit));
            if (has) holders.insert(i);
            witness += " n" + std::to_string(i) + "=" +
                       std::to_string(other->log().persisted_index().value()) + "/" +
                       std::to_string(other->log().last_index().value());
          }
          if (!node.config().has_quorum(holders)) {
            record("INV-RAFT-09",
                   node_name(id) + " committed " +
                       index_term(log.commit_index(), at_commit) +
                       " without a durable quorum of " + node.config().describe() +
                       "; persisted/last per node:" + witness);
          }
        }
      }

      if (!node.config().learners().empty()) {
        std::map<std::uint64_t, LogIndex> match;
        for (const auto& [peer, progress] : node.progress()) {
          match.emplace(peer, progress.match);
        }
        const LogIndex voters_only = node.config().committed_index(match, false);
        if (log.commit_index() > voters_only) {
          record("INV-RAFT-15",
                 node_name(id) + " advanced its commit index to " +
                     std::to_string(log.commit_index().value()) +
                     " where the voters alone reach only " +
                     std::to_string(voters_only.value()) + " -- a learner was counted in " +
                     node.config().describe());
        }
      }
      mirror.commit_seen_as_leader = log.commit_index();
    }
  }

  // ---- configuration derived from the committed prefix -------------------
  //
  // Seeding waits for the node to have a configuration at all. A RaftNode
  // exists before it has recovered -- the observer is handed the pointer at
  // boot, and recovery is asynchronous because it reads the disk -- so its
  // configuration is briefly empty. Seeding from that and then deriving forward
  // reports a mismatch against the bootstrap membership on the very first tick
  // of every run, which is a checker bug that looks exactly like a protocol
  // bug and fires before anything interesting can happen.
  if (!mirror.derived_valid || mirror.derived_config.empty()) {
    mirror.derived_config = node.config();
    mirror.derived_to = log.applied_index();
    mirror.derived_valid = !node.config().empty();
  } else {
    if (log.snapshot_index() > mirror.derived_to) {
      // A snapshot carries the membership with it; there are no conf-change
      // entries left to replay, so the snapshot is the authority.
      mirror.derived_config = node.config();
      mirror.derived_to = log.snapshot_index();
    }
    for (LogIndex i{mirror.derived_to.value() + 1}; i <= log.applied_index();
         i = LogIndex{i.value() + 1}) {
      const raft::LogEntry* entry = log.at(i);
      mirror.derived_to = i;
      if (entry == nullptr || entry->type != raft::EntryType::kConfChange) continue;
      raft::ConfChange change;
      if (!raft::decode_conf_change(entry->data, &change)) continue;
      mirror.derived_config = mirror.derived_config.apply(change);
    }
    if (!(mirror.derived_config == node.config())) {
      // INV-RAFT-12. A node's configuration must be a function of its committed
      // prefix and nothing else. Applying a membership change on append rather
      // than on commit is exactly how joint consensus gets exited early, and it
      // produces two nodes computing quorums over sets that need not intersect.
      record("INV-RAFT-12",
             node_name(id) + " holds " + node.config().describe() +
                 " but its committed prefix implies " + mirror.derived_config.describe());
      mirror.derived_config = node.config();  // resynchronise; report once
    }
  }

  mirror.max_persisted = std::max(mirror.max_persisted, log.persisted_index());
  mirror.role = node.role();
  mirror.role_term = node.term();
  mirror.last_index = log.last_index();
  mirror.seen = true;
}

// ---------------------------------------------------------------------------
// the predicates
// ---------------------------------------------------------------------------

namespace {

using Result = std::optional<std::string>;

// Every predicate begins with refresh(). It is a no-op after the first call in
// a tick, so the order in which they are armed does not matter -- which is
// deliberate: an invariant suite whose correctness depends on registration
// order is one refactor away from silently checking nothing.
Predicate queued(RaftObserver* obs, std::string id) {
  return [obs, id]() -> Result {
    obs->refresh();
    return obs->take(id);
  };
}

}  // namespace

void arm_raft_invariants(InvariantRegistry& registry, RaftObserver* observer) {
  registry.arm("INV-RAFT-01", "at most one leader per term", CostClass::kTick,
               queued(observer, "INV-RAFT-01"));
  registry.arm("INV-RAFT-02", "a leader never overwrites its own log", CostClass::kTick,
               queued(observer, "INV-RAFT-02"));
  registry.arm("INV-RAFT-04", "leader completeness across terms", CostClass::kTick,
               queued(observer, "INV-RAFT-04"));
  registry.arm("INV-RAFT-05", "no two nodes apply different commands at one index",
               CostClass::kTick, queued(observer, "INV-RAFT-05"));
  registry.arm("INV-RAFT-06", "a term never decreases, including across restart",
               CostClass::kTick, queued(observer, "INV-RAFT-06"));
  registry.arm("INV-RAFT-07", "a node never grants two votes in one term", CostClass::kTick,
               queued(observer, "INV-RAFT-07"));
  registry.arm("INV-RAFT-08", "commit index is monotonic per node", CostClass::kTick,
               queued(observer, "INV-RAFT-08"));
  registry.arm("INV-RAFT-10", "commit only advances onto a current-term entry",
               CostClass::kTick, queued(observer, "INV-RAFT-10"));
  registry.arm("INV-RAFT-12", "configuration follows only from the committed prefix",
               CostClass::kTick, queued(observer, "INV-RAFT-12"));
  registry.arm("INV-RAFT-16", "one index and one term means one entry", CostClass::kTick,
               queued(observer, "INV-RAFT-16"));

  // ---- live predicates ---------------------------------------------------

  // INV-RAFT-11. A durable snapshot that the log has not been truncated behind
  // means recovery will replay entries the snapshot already contains, on top of
  // it. The state machine ends up having applied them twice.
  registry.arm(
      "INV-RAFT-11", "log truncation is covered by a durable snapshot", CostClass::kTick,
      [observer]() -> Result {
        observer->refresh();
        for (const NodeId id : observer->live_nodes()) {
          const raft::RaftNode* node = observer->node(id);
          if (node == nullptr) continue;
          if (node->snapshot().index > node->log().snapshot_index()) {
            return node_name(id) + " has a durable snapshot at " +
                   std::to_string(node->snapshot().index.value()) +
                   " but its log still starts at " +
                   std::to_string(node->log().first_index().value());
          }
          if (node->log().snapshot_index() > node->log().applied_index()) {
            return node_name(id) + " truncated its log to " +
                   std::to_string(node->log().snapshot_index().value()) +
                   " past its applied index " +
                   std::to_string(node->log().applied_index().value());
          }
        }
        return std::nullopt;
      });

  // INV-RAFT-13. Two live leases at the same true instant means two nodes both
  // believe they may serve a linearizable read without asking anybody. The
  // declared clock-uncertainty bound is the allowance; anything beyond it is
  // the bound being wrong, which is the interesting result.
  registry.arm(
      "INV-RAFT-13", "leader leases do not overlap beyond the uncertainty bound",
      CostClass::kTick, [observer]() -> Result {
        observer->refresh();
        struct Holder {
          NodeId id;
          Term term;
          std::int64_t remaining = 0;
        };
        std::vector<Holder> holders;
        for (const NodeId id : observer->live_nodes()) {
          const raft::RaftNode* node = observer->node(id);
          if (node == nullptr || node->role() != raft::Role::kLeader) continue;
          const Timestamp own = observer->node_now(id);
          if (!node->lease_valid(own)) continue;
          const std::int64_t remaining =
              node->options().lease_uses_wall_clock
                  ? static_cast<std::int64_t>(node->lease_expiry().physical) -
                        static_cast<std::int64_t>(own.physical)
                  : node->options().lease_duration.nanos();
          holders.push_back(Holder{id, node->term(), remaining});
        }
        if (holders.size() < 2) return std::nullopt;

        // The overlap, measured in true time: how long both of them would go on
        // believing it.
        std::int64_t overlap = INT64_MAX;
        for (const Holder& holder : holders) overlap = std::min(overlap, holder.remaining);
        const raft::RaftNode* first = observer->node(holders.front().id);
        const std::int64_t bound =
            first == nullptr ? 0 : first->options().max_clock_uncertainty.nanos();
        if (overlap <= bound) return std::nullopt;

        std::string detail = "leases held simultaneously for a further " +
                             std::to_string(overlap / 1'000'000) + "ms by";
        for (const Holder& holder : holders) {
          detail += " " + node_name(holder.id) + "(term " +
                    std::to_string(holder.term.value()) + ")";
        }
        return detail;
      });

  // INV-RAFT-15 is evaluated inside the scan, at the tick a leader's commit
  // index advances -- the only moment at which the question "could the voters
  // alone have reached this?" has an unambiguous answer. See scan_node().
  registry.arm("INV-RAFT-15", "a learner is never counted in a quorum", CostClass::kTick,
               queued(observer, "INV-RAFT-15"));

  // ---- epoch class: the expensive, real properties ------------------------

  // INV-RAFT-03, Log Matching in full. INV-RAFT-16 is the cheap inductive proxy
  // that fires within one event; this is the property itself, and it exists
  // separately because a proxy that has never been shown to catch something the
  // real check cannot is decoration (ANV-0005).
  registry.arm(
      "INV-RAFT-03", "logs agreeing at an index agree on the whole prefix", CostClass::kEpoch,
      [observer]() -> Result {
        observer->refresh();
        const std::vector<NodeId> live = observer->live_nodes();
        for (std::size_t a = 0; a < live.size(); ++a) {
          for (std::size_t b = a + 1; b < live.size(); ++b) {
            const raft::RaftNode* left = observer->node(live[a]);
            const raft::RaftNode* right = observer->node(live[b]);
            if (left == nullptr || right == nullptr) continue;
            const raft::RaftLog& lhs = left->log();
            const raft::RaftLog& rhs = right->log();

            const std::uint64_t floor =
                std::max(lhs.snapshot_index().value(), rhs.snapshot_index().value());
            std::uint64_t top = std::min(lhs.last_index().value(), rhs.last_index().value());
            // Find the highest index where the two agree on the term.
            while (top > floor) {
              Term lt{};
              Term rt{};
              if (lhs.term_at(LogIndex{top}, &lt) && rhs.term_at(LogIndex{top}, &rt) &&
                  lt == rt) {
                break;
              }
              --top;
            }
            for (std::uint64_t i = top; i > floor; --i) {
              const raft::LogEntry* le = lhs.at(LogIndex{i});
              const raft::LogEntry* re = rhs.at(LogIndex{i});
              if (le == nullptr || re == nullptr) break;
              if (le->term != re->term || digest_of(*le) != digest_of(*re)) {
                return node_name(live[a]) + " and " + node_name(live[b]) +
                       " hold the same term at index " + std::to_string(top) +
                       " but their logs differ at index " + std::to_string(i) + " (" +
                       index_term(LogIndex{i}, le->term) + " vs " +
                       index_term(LogIndex{i}, re->term) + ")";
              }
            }
          }
        }
        return std::nullopt;
      });

  // INV-RAFT-09 is evaluated inside the scan too, at the tick a leader's commit
  // index advances. Checking it there rather than at epoch class is what turns
  // "some entry was not durable twenty seconds ago" into a report that names
  // the decision, the entry and every node's durability state at that instant.
  registry.arm("INV-RAFT-09", "a committed entry is durable on a quorum", CostClass::kTick,
               queued(observer, "INV-RAFT-09"));
}

}  // namespace anvil::checker
