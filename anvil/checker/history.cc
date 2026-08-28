#include "anvil/checker/history.h"

#include <utility>

namespace anvil::checker {

const char* to_string(Outcome outcome) noexcept {
  switch (outcome) {
    case Outcome::kCommitted: return "committed";
    case Outcome::kAborted: return "aborted";
    case Outcome::kUnknown: return "unknown";
  }
  return "?";
}

Element Txn::final_append_to(KeyId key) const {
  Element last = 0;
  for (const Mop& mop : mops) {
    if (mop.type == MopType::kAppend && mop.key == key) last = mop.element;
  }
  return last;
}

bool Txn::appends_to(KeyId key) const {
  for (const Mop& mop : mops) {
    if (mop.type == MopType::kAppend && mop.key == key) return true;
  }
  return false;
}

TxnId History::begin(std::uint64_t process, Timestamp at) {
  Txn txn;
  txn.id = next_id_++;
  txn.process = process;
  txn.invoked = at;
  txn.completed = at;
  txn.outcome = Outcome::kUnknown;
  txns_.push_back(std::move(txn));
  return txns_.back().id;
}

Txn* History::mutable_find(TxnId id) {
  for (Txn& txn : txns_) {
    if (txn.id == id) return &txn;
  }
  return nullptr;
}

const Txn* History::find(TxnId id) const {
  for (const Txn& txn : txns_) {
    if (txn.id == id) return &txn;
  }
  return nullptr;
}

void History::append(TxnId id, KeyId key, Element element) {
  Txn* txn = mutable_find(id);
  if (txn == nullptr) return;
  Mop mop;
  mop.type = MopType::kAppend;
  mop.key = key;
  mop.element = element;
  txn->mops.push_back(std::move(mop));
}

void History::read(TxnId id, KeyId key, std::vector<Element> observed) {
  Txn* txn = mutable_find(id);
  if (txn == nullptr) return;
  Mop mop;
  mop.type = MopType::kRead;
  mop.key = key;
  mop.observed = std::move(observed);
  txn->mops.push_back(std::move(mop));
}

void History::complete(TxnId id, Outcome outcome, Timestamp at) {
  Txn* txn = mutable_find(id);
  if (txn == nullptr) return;
  txn->outcome = outcome;
  txn->completed = at;
}

TxnId History::add(Txn txn) {
  if (txn.id == 0) txn.id = next_id_++;
  if (txn.id >= next_id_) next_id_ = txn.id + 1;
  const TxnId id = txn.id;
  txns_.push_back(std::move(txn));
  return id;
}

std::string History::render() const {
  std::string out;
  for (const Txn& txn : txns_) {
    out += "T" + std::to_string(txn.id) + " [p" + std::to_string(txn.process) + " " +
           to_string(txn.outcome) + "] ";
    for (const Mop& mop : txn.mops) {
      if (mop.type == MopType::kAppend) {
        out += "append(" + std::to_string(mop.key) + "," + std::to_string(mop.element) + ") ";
      } else {
        out += "read(" + std::to_string(mop.key) + ")=[";
        for (std::size_t i = 0; i < mop.observed.size(); ++i) {
          if (i > 0) out += ",";
          out += std::to_string(mop.observed[i]);
        }
        out += "] ";
      }
    }
    out += "\n";
  }
  return out;
}

// ---------------------------------------------------------------------------

std::vector<Mop> ReferenceModel::apply(const std::vector<Mop>& mops) {
  std::vector<Mop> out;
  out.reserve(mops.size());
  for (const Mop& mop : mops) {
    Mop result = mop;
    if (mop.type == MopType::kAppend) {
      state_[mop.key].push_back(mop.element);
    } else {
      const auto it = state_.find(mop.key);
      result.observed = it == state_.end() ? std::vector<Element>{} : it->second;
    }
    out.push_back(std::move(result));
  }
  return out;
}

}  // namespace anvil::checker
