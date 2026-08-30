#include "anvil/core/raft/config.h"

#include <algorithm>

#include "anvil/core/lsm/format.h"

namespace anvil::raft {
namespace {

std::string join_ids(const std::set<std::uint64_t>& ids) {
  std::string out;
  for (const std::uint64_t id : ids) {
    if (!out.empty()) out.push_back(',');
    out += std::to_string(id);
  }
  return out;
}

}  // namespace

Config Config::from_voters(const std::vector<std::uint64_t>& voters) {
  Config cfg;
  cfg.incoming_.insert(voters.begin(), voters.end());
  return cfg;
}

Config Config::from_conf_state(const ConfState& state) {
  Config cfg;
  cfg.incoming_.insert(state.voters.begin(), state.voters.end());
  cfg.outgoing_.insert(state.voters_outgoing.begin(), state.voters_outgoing.end());
  cfg.learners_.insert(state.learners.begin(), state.learners.end());
  return cfg;
}

ConfState Config::to_conf_state() const {
  ConfState state;
  state.voters.assign(incoming_.begin(), incoming_.end());
  state.voters_outgoing.assign(outgoing_.begin(), outgoing_.end());
  state.learners.assign(learners_.begin(), learners_.end());
  return state;
}

bool Config::is_voter(NodeId node) const {
  return incoming_.contains(node.value()) || outgoing_.contains(node.value());
}

bool Config::is_learner(NodeId node) const {
  // A node promoted from learner to voter appears in both sets for exactly one
  // configuration; voter wins, because counting it as a learner would silently
  // shrink the quorum it is supposed to have joined.
  return !is_voter(node) && learners_.contains(node.value());
}

bool Config::is_member(NodeId node) const { return is_voter(node) || is_learner(node); }

std::vector<NodeId> Config::members() const {
  std::set<std::uint64_t> all = incoming_;
  all.insert(outgoing_.begin(), outgoing_.end());
  all.insert(learners_.begin(), learners_.end());
  std::vector<NodeId> out;
  out.reserve(all.size());
  for (const std::uint64_t id : all) out.emplace_back(id);
  return out;
}

std::vector<NodeId> Config::voters() const {
  std::set<std::uint64_t> all = incoming_;
  all.insert(outgoing_.begin(), outgoing_.end());
  std::vector<NodeId> out;
  out.reserve(all.size());
  for (const std::uint64_t id : all) out.emplace_back(id);
  return out;
}

bool Config::majority(const std::set<std::uint64_t>& voters,
                      const std::set<std::uint64_t>& acked) {
  if (voters.empty()) return true;  // a vacuous half of a joint config
  std::size_t hits = 0;
  for (const std::uint64_t v : voters) {
    if (acked.contains(v)) ++hits;
  }
  return hits * 2 > voters.size();
}

bool Config::has_quorum(const std::set<std::uint64_t>& acked, bool count_learners) const {
  // The learner rule lives here and only here (INV-RAFT-15).
  //
  // `count_learners` is the mutation, and it is worth being precise about what
  // the bug actually is. It is not "learners are members of the configuration"
  // -- they are. It is that a learner's acknowledgement is counted toward the
  // *voters'* majority while the denominator stays the number of voters. Three
  // voters and two learners then reach a "quorum" with one voter and one
  // learner, and two disjoint such sets exist. That is how a learner that was
  // added minutes ago and has replicated nothing ends up deciding a commit.
  const auto reached = [&](const std::set<std::uint64_t>& voters) {
    if (voters.empty()) return true;
    std::size_t hits = 0;
    for (const std::uint64_t v : voters) {
      if (acked.contains(v)) ++hits;
    }
    if (count_learners) {
      for (const std::uint64_t l : learners_) {
        if (!voters.contains(l) && acked.contains(l)) ++hits;
      }
    }
    return hits * 2 > voters.size();
  };
  if (!reached(incoming_)) return false;
  if (joint() && !reached(outgoing_)) return false;
  return true;
}

LogIndex Config::majority_index(const std::set<std::uint64_t>& voters,
                                const std::map<std::uint64_t, LogIndex>& match,
                                const std::set<std::uint64_t>* also_counted) {
  if (voters.empty()) return LogIndex{UINT64_MAX};  // vacuously satisfied
  const auto index_of = [&](std::uint64_t id) {
    const auto it = match.find(id);
    return it == match.end() ? std::uint64_t{0} : it->second.value();
  };

  std::vector<std::uint64_t> indices;
  indices.reserve(voters.size());
  for (const std::uint64_t v : voters) indices.push_back(index_of(v));
  if (also_counted != nullptr) {
    for (const std::uint64_t l : *also_counted) {
      if (!voters.contains(l)) indices.push_back(index_of(l));
    }
  }

  // Descending, then take the element at position n/2, where n is the number of
  // *voters*: the largest index that a strict majority have reached.
  //
  // n/2 and not (n-1)/2. They agree for odd n and differ for even n, which is
  // why the mistake survives every test on a three- or five-node cluster and
  // then commits an entry held by two nodes out of four. Even voter counts are
  // not exotic -- every joint-consensus transition passes through one, and a
  // four-node cluster is what you get the moment someone adds a replica.
  //
  // Keeping the denominator at the voter count while extra members contribute
  // indices is separately the learner-counting bug.
  std::sort(indices.begin(), indices.end(), std::greater<std::uint64_t>{});
  return LogIndex{indices[voters.size() / 2]};
}

LogIndex Config::committed_index(const std::map<std::uint64_t, LogIndex>& match,
                                 bool count_learners) const {
  const std::set<std::uint64_t>* extra = count_learners ? &learners_ : nullptr;
  const LogIndex a = majority_index(incoming_, match, extra);
  if (!joint()) return a.value() == UINT64_MAX ? LogIndex{0} : a;
  const LogIndex b = majority_index(outgoing_, match, extra);
  const std::uint64_t v = std::min(a.value(), b.value());
  return LogIndex{v == UINT64_MAX ? 0 : v};
}

Config Config::apply(const ConfChange& change) const {
  Config next;
  switch (change.kind) {
    case ConfChangeKind::kEnterJoint:
      next.incoming_.insert(change.voters.begin(), change.voters.end());
      next.learners_.insert(change.learners.begin(), change.learners.end());
      // The current voters become C_old. If this configuration is already
      // joint, its own outgoing set is dropped -- but the caller must not get
      // here, because a second change while joint is refused upstream.
      next.outgoing_ = incoming_;
      if (next.outgoing_ == next.incoming_) next.outgoing_.clear();
      break;
    case ConfChangeKind::kLeaveJoint:
      next.incoming_ = incoming_;
      next.learners_ = learners_;
      break;
  }
  return next;
}

std::string Config::describe() const {
  std::string out = "voters{" + join_ids(incoming_) + "}";
  if (joint()) out += " outgoing{" + join_ids(outgoing_) + "}";
  if (!learners_.empty()) out += " learners{" + join_ids(learners_) + "}";
  return out;
}

// ---------------------------------------------------------------------------
// encoding
// ---------------------------------------------------------------------------

namespace {

void put_ids(std::string* dst, const std::vector<std::uint64_t>& ids) {
  lsm::put_varint32(dst, static_cast<std::uint32_t>(ids.size()));
  for (const std::uint64_t id : ids) lsm::put_varint64(dst, id);
}

const char* get_ids(const char* p, const char* limit, std::vector<std::uint64_t>* out) {
  std::uint32_t count = 0;
  p = lsm::get_varint32(p, limit, &count);
  if (p == nullptr) return nullptr;
  out->clear();
  out->reserve(count);
  for (std::uint32_t i = 0; i < count; ++i) {
    std::uint64_t id = 0;
    p = lsm::get_varint64(p, limit, &id);
    if (p == nullptr) return nullptr;
    out->push_back(id);
  }
  return p;
}

}  // namespace

std::string encode_conf_state(const ConfState& state) {
  std::string out;
  put_ids(&out, state.voters);
  put_ids(&out, state.voters_outgoing);
  put_ids(&out, state.learners);
  return out;
}

bool decode_conf_state(const std::string& in, ConfState* out) {
  const char* p = in.data();
  const char* limit = p + in.size();
  p = get_ids(p, limit, &out->voters);
  if (p == nullptr) return false;
  p = get_ids(p, limit, &out->voters_outgoing);
  if (p == nullptr) return false;
  p = get_ids(p, limit, &out->learners);
  return p != nullptr;
}

std::string encode_conf_change(const ConfChange& change) {
  std::string out;
  out.push_back(static_cast<char>(change.kind));
  put_ids(&out, change.voters);
  put_ids(&out, change.learners);
  return out;
}

bool decode_conf_change(const std::string& in, ConfChange* out) {
  if (in.empty()) return false;
  const auto kind = static_cast<std::uint8_t>(in[0]);
  if (kind > static_cast<std::uint8_t>(ConfChangeKind::kLeaveJoint)) return false;
  out->kind = static_cast<ConfChangeKind>(kind);
  const char* p = in.data() + 1;
  const char* limit = in.data() + in.size();
  p = get_ids(p, limit, &out->voters);
  if (p == nullptr) return false;
  p = get_ids(p, limit, &out->learners);
  return p != nullptr;
}

}  // namespace anvil::raft
