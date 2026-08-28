#include "anvil/checker/invariant.h"

#include <utility>

namespace anvil::checker {

const char* to_string(CostClass cost) noexcept {
  switch (cost) {
    case CostClass::kTick: return "tick";
    case CostClass::kEpoch: return "epoch";
    case CostClass::kCommit: return "commit";
    case CostClass::kQuiesce: return "quiesce";
    case CostClass::kOffline: return "offline";
  }
  return "?";
}

std::string Violation::render() const {
  std::string out = id;
  if (!name.empty()) out += " (" + name + ")";
  out += " violated at tick " + std::to_string(tick) + ", sim-time " +
         std::to_string(when.physical / 1'000'000) + "ms";
  if (!detail.empty()) out += "\n    " + detail;
  return out;
}

void InvariantRegistry::arm(std::string id, std::string name, CostClass cost,
                            Predicate predicate) {
  Stats stats;
  stats.name = name;
  stats.cost = cost;
  stats_[id] = std::move(stats);

  Entry entry;
  entry.id = std::move(id);
  entry.name = std::move(name);
  entry.cost = cost;
  entry.predicate = std::move(predicate);
  invariants_.push_back(std::move(entry));
}

std::vector<Violation> InvariantRegistry::evaluate(CostClass cost, Timestamp now,
                                                   std::uint64_t tick) {
  std::vector<Violation> violations;
  for (const Entry& entry : invariants_) {
    if (entry.cost != cost) continue;

    Stats& stats = stats_[entry.id];
    ++stats.evaluations;

    std::optional<std::string> detail = entry.predicate();
    if (!detail.has_value()) continue;

    ++stats.violations;
    Violation violation;
    violation.id = entry.id;
    violation.name = entry.name;
    violation.detail = std::move(*detail);
    violation.when = now;
    violation.tick = tick;
    violations.push_back(std::move(violation));
  }
  return violations;
}

std::vector<std::string> InvariantRegistry::never_fired() const {
  std::vector<std::string> out;
  for (const auto& [id, stats] : stats_) {
    if (stats.violations == 0) out.push_back(id);
  }
  return out;
}

}  // namespace anvil::checker
