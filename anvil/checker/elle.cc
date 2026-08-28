#include "anvil/checker/elle.h"

#include <algorithm>
#include <deque>
#include <set>
#include <utility>

namespace anvil::checker {
namespace {

struct Edge {
  TxnId to = 0;
  EdgeType type = EdgeType::kWW;
  KeyId key = 0;

  friend bool operator<(const Edge& a, const Edge& b) {
    if (a.to != b.to) return a.to < b.to;
    if (a.type != b.type) return a.type < b.type;
    return a.key < b.key;
  }
};

// Adjacency kept in std::map/std::set, never a hash container. The witness a
// checker reports has to be identical on every machine or a bug report is not
// reproducible, and hash iteration order is exactly the kind of thing that
// makes two runs disagree about *which* of several cycles they found.
using Graph = std::map<TxnId, std::set<Edge>>;

struct VersionOrder {
  std::vector<Element> order;                   // the recovered append order
  std::map<Element, std::size_t> index;         // element -> position
  bool conflicted = false;
  std::string conflict_detail;
};

// ---------------------------------------------------------------------------
// step 1: recover the version order for each key
// ---------------------------------------------------------------------------

// Every observed list is a prefix of the true append order, so the longest
// observation *is* the order and every other must agree with it elementwise.
// A disagreement means two readers saw incompatible histories of the same key,
// which is a violation on its own and also makes the graph meaningless -- so it
// is reported and that key is skipped rather than producing nonsense edges.
std::map<KeyId, VersionOrder> recover_version_orders(const History& history) {
  std::map<KeyId, std::vector<std::vector<Element>>> observations;
  for (const Txn& txn : history.txns()) {
    for (const Mop& mop : txn.mops) {
      if (mop.type == MopType::kRead) observations[mop.key].push_back(mop.observed);
    }
  }

  std::map<KeyId, VersionOrder> orders;
  for (const auto& [key, lists] : observations) {
    VersionOrder vo;
    for (const std::vector<Element>& list : lists) {
      if (list.size() > vo.order.size()) vo.order = list;
    }
    for (const std::vector<Element>& list : lists) {
      for (std::size_t i = 0; i < list.size(); ++i) {
        if (list[i] != vo.order[i]) {
          vo.conflicted = true;
          vo.conflict_detail = "key " + std::to_string(key) + ": one read saw element " +
                               std::to_string(list[i]) + " at position " + std::to_string(i) +
                               ", another saw " + std::to_string(vo.order[i]);
          break;
        }
      }
      if (vo.conflicted) break;
    }
    for (std::size_t i = 0; i < vo.order.size(); ++i) vo.index[vo.order[i]] = i;
    orders[key] = std::move(vo);
  }
  return orders;
}

// ---------------------------------------------------------------------------
// Tarjan
// ---------------------------------------------------------------------------

class Tarjan {
 public:
  explicit Tarjan(const Graph& graph) : graph_(graph) {}

  std::vector<std::vector<TxnId>> run() {
    for (const auto& [node, _] : graph_) {
      if (!index_.contains(node)) strongconnect(node);
    }
    return std::move(components_);
  }

 private:
  void strongconnect(TxnId v) {
    // Iterative rather than recursive. A history with tens of thousands of
    // transactions in one component would blow a recursive implementation's
    // stack, and it would do it only on the large nightly runs -- the worst
    // possible place to discover a stack overflow.
    struct Frame {
      TxnId node;
      std::set<Edge>::const_iterator next;
      std::set<Edge>::const_iterator end;
    };

    std::vector<Frame> stack;
    const auto push = [&](TxnId node) {
      index_[node] = counter_;
      lowlink_[node] = counter_;
      ++counter_;
      on_stack_.insert(node);
      scc_stack_.push_back(node);
      const auto it = graph_.find(node);
      static const std::set<Edge> kEmpty;
      const std::set<Edge>& edges = it == graph_.end() ? kEmpty : it->second;
      stack.push_back(Frame{node, edges.begin(), edges.end()});
    };

    push(v);
    while (!stack.empty()) {
      Frame& frame = stack.back();
      if (frame.next != frame.end) {
        const TxnId w = frame.next->to;
        ++frame.next;
        if (!index_.contains(w)) {
          push(w);
        } else if (on_stack_.contains(w)) {
          lowlink_[frame.node] = std::min(lowlink_[frame.node], index_[w]);
        }
        continue;
      }

      const TxnId node = frame.node;
      stack.pop_back();
      if (!stack.empty()) {
        lowlink_[stack.back().node] = std::min(lowlink_[stack.back().node], lowlink_[node]);
      }

      if (lowlink_[node] == index_[node]) {
        std::vector<TxnId> component;
        for (;;) {
          const TxnId w = scc_stack_.back();
          scc_stack_.pop_back();
          on_stack_.erase(w);
          component.push_back(w);
          if (w == node) break;
        }
        if (component.size() > 1) {
          std::sort(component.begin(), component.end());
          components_.push_back(std::move(component));
        }
      }
    }
  }

  const Graph& graph_;
  std::map<TxnId, std::uint64_t> index_;
  std::map<TxnId, std::uint64_t> lowlink_;
  std::set<TxnId> on_stack_;
  std::vector<TxnId> scc_stack_;
  std::vector<std::vector<TxnId>> components_;
  std::uint64_t counter_ = 0;
};

// ---------------------------------------------------------------------------
// minimal cycle extraction
// ---------------------------------------------------------------------------

// Reports the *shortest* cycle in the component rather than the component
// itself. A 400-transaction SCC is technically a correct answer and is useless
// to a human; a three-transaction cycle with its edge types named is something
// you can actually read and act on. Ties are broken by transaction id so the
// witness is stable across runs.
bool shortest_cycle(const Graph& graph, const std::vector<TxnId>& component,
                    std::vector<TxnId>* out_txns, std::vector<EdgeType>* out_edges) {
  const std::set<TxnId> members(component.begin(), component.end());

  std::size_t best = 0;
  for (const TxnId start : component) {
    std::map<TxnId, TxnId> parent;
    std::map<TxnId, EdgeType> parent_edge;
    std::map<TxnId, std::size_t> depth;
    std::deque<TxnId> queue{start};
    depth[start] = 0;

    while (!queue.empty()) {
      const TxnId node = queue.front();
      queue.pop_front();
      if (best != 0 && depth[node] + 1 >= best) continue;

      const auto it = graph.find(node);
      if (it == graph.end()) continue;
      for (const Edge& edge : it->second) {
        if (!members.contains(edge.to)) continue;

        if (edge.to == start) {
          // Closed a cycle. Walk the parent chain back.
          std::vector<TxnId> txns{node};
          std::vector<EdgeType> edges{edge.type};
          TxnId cursor = node;
          while (cursor != start) {
            edges.push_back(parent_edge[cursor]);
            cursor = parent[cursor];
            txns.push_back(cursor);
          }
          std::reverse(txns.begin(), txns.end());
          std::reverse(edges.begin(), edges.end());
          // `edges` currently runs [start->..., ..., node->start]; reversing
          // both keeps edges[i] as the edge leaving txns[i].
          std::rotate(edges.begin(), edges.begin() + 1, edges.end());
          if (best == 0 || txns.size() < best) {
            best = txns.size();
            *out_txns = txns;
            *out_edges = edges;
          }
          continue;
        }

        if (depth.contains(edge.to)) continue;
        depth[edge.to] = depth[node] + 1;
        parent[edge.to] = node;
        parent_edge[edge.to] = edge.type;
        queue.push_back(edge.to);
      }
    }
    if (best == 2) break;  // cannot do better than a two-cycle
  }
  return best != 0;
}

Anomaly classify(const std::vector<EdgeType>& edges) {
  std::size_t rw = 0;
  std::size_t wr = 0;
  std::size_t ww = 0;
  std::size_t rt = 0;
  for (const EdgeType edge : edges) {
    switch (edge) {
      case EdgeType::kRW: ++rw; break;
      case EdgeType::kWR: ++wr; break;
      case EdgeType::kWW: ++ww; break;
      case EdgeType::kRealTime: ++rt; break;
    }
  }
  if (rt > 0) return Anomaly::kRealTimeViolation;
  if (rw >= 2) return Anomaly::kG2Item;
  if (rw == 1) return Anomaly::kGSingle;
  if (wr > 0) return Anomaly::kG1c;
  (void)ww;
  return Anomaly::kG0;
}

}  // namespace

// ---------------------------------------------------------------------------

const char* to_string(IsolationLevel level) noexcept {
  switch (level) {
    case IsolationLevel::kReadUncommitted: return "read-uncommitted";
    case IsolationLevel::kReadCommitted: return "read-committed";
    case IsolationLevel::kSnapshotIsolation: return "snapshot-isolation";
    case IsolationLevel::kSerializable: return "serializable";
    case IsolationLevel::kStrictSerializable: return "strict-serializable";
  }
  return "?";
}

const char* to_string(Anomaly anomaly) noexcept {
  switch (anomaly) {
    case Anomaly::kG0: return "G0 (write cycle)";
    case Anomaly::kG1a: return "G1a (aborted read)";
    case Anomaly::kG1b: return "G1b (intermediate read)";
    case Anomaly::kG1c: return "G1c (circular information flow)";
    case Anomaly::kGSingle: return "G-single (one anti-dependency)";
    case Anomaly::kG2Item: return "G2-item (write skew)";
    case Anomaly::kVersionOrderConflict: return "version-order conflict";
    case Anomaly::kDuplicateElement: return "duplicate element";
    case Anomaly::kRealTimeViolation: return "real-time violation";
    case Anomaly::kCount: return "?";
  }
  return "?";
}

const char* to_string(EdgeType edge) noexcept {
  switch (edge) {
    case EdgeType::kWW: return "ww";
    case EdgeType::kWR: return "wr";
    case EdgeType::kRW: return "rw";
    case EdgeType::kRealTime: return "rt";
  }
  return "?";
}

bool forbids(IsolationLevel level, Anomaly anomaly) {
  // These two are structural: a duplicate element or contradictory reads mean
  // the history itself is incoherent, at any level.
  if (anomaly == Anomaly::kDuplicateElement || anomaly == Anomaly::kVersionOrderConflict) {
    return true;
  }

  switch (level) {
    case IsolationLevel::kReadUncommitted:
      return anomaly == Anomaly::kG0;
    case IsolationLevel::kReadCommitted:
      return anomaly == Anomaly::kG0 || anomaly == Anomaly::kG1a ||
             anomaly == Anomaly::kG1b || anomaly == Anomaly::kG1c;
    case IsolationLevel::kSnapshotIsolation:
      // Everything read-committed forbids, plus G-single. G2-item -- write skew
      // -- is *permitted*, and that is not an oversight: it is the defining
      // characteristic of snapshot isolation and the reason SI is not
      // serializable. A checker that flagged it here would be wrong, and would
      // make the SI transaction engine look broken when it is behaving exactly
      // as specified.
      return anomaly != Anomaly::kG2Item && anomaly != Anomaly::kRealTimeViolation;
    case IsolationLevel::kSerializable:
      return anomaly != Anomaly::kRealTimeViolation;
    case IsolationLevel::kStrictSerializable:
      return true;
  }
  return true;
}

bool CheckResult::has(Anomaly anomaly) const {
  return std::find(anomalies.begin(), anomalies.end(), anomaly) != anomalies.end();
}

std::string CycleWitness::render() const {
  std::string out = to_string(anomaly);
  out += ":  ";
  for (std::size_t i = 0; i < txns.size(); ++i) {
    out += "T" + std::to_string(txns[i]);
    if (i < edges.size()) {
      out += " -";
      out += to_string(edges[i]);
      out += "-> ";
    }
  }
  if (!txns.empty()) out += "T" + std::to_string(txns.front());
  if (!detail.empty()) out += "\n      " + detail;
  return out;
}

std::string CheckResult::summary() const {
  std::string out = std::string{valid ? "VALID" : "INVALID"} + " at " + to_string(level) + " (" +
                    std::to_string(transactions) + " txns, " + std::to_string(edges) + " edges)";
  for (const Anomaly anomaly : anomalies) {
    out += "\n  " + std::string{to_string(anomaly)};
  }
  for (const CycleWitness& witness : witnesses) {
    out += "\n  " + witness.render();
  }
  return out;
}

// ---------------------------------------------------------------------------

CheckResult check(const History& history, IsolationLevel level) {
  CheckResult result;
  result.level = level;
  result.transactions = history.size();

  std::set<Anomaly> found;
  const auto report = [&](Anomaly anomaly) { found.insert(anomaly); };

  // -- element -> writer, and duplicate detection --------------------------
  std::map<std::pair<KeyId, Element>, TxnId> writer;
  for (const Txn& txn : history.txns()) {
    for (const Mop& mop : txn.mops) {
      if (mop.type != MopType::kAppend) continue;
      const auto entry = std::pair<KeyId, Element>{mop.key, mop.element};
      const auto it = writer.find(entry);
      if (it != writer.end() && it->second != txn.id) {
        report(Anomaly::kDuplicateElement);
        CycleWitness witness;
        witness.anomaly = Anomaly::kDuplicateElement;
        witness.txns = {it->second, txn.id};
        witness.detail = "element " + std::to_string(mop.element) + " on key " +
                         std::to_string(mop.key) + " appended by both T" +
                         std::to_string(it->second) + " and T" + std::to_string(txn.id);
        result.witnesses.push_back(std::move(witness));
        continue;
      }
      writer[entry] = txn.id;
    }
  }

  const auto orders = recover_version_orders(history);
  for (const auto& [key, order] : orders) {
    if (!order.conflicted) continue;
    report(Anomaly::kVersionOrderConflict);
    CycleWitness witness;
    witness.anomaly = Anomaly::kVersionOrderConflict;
    witness.detail = order.conflict_detail;
    result.witnesses.push_back(std::move(witness));
  }

  // -- G1a and G1b: not cycles, so checked directly ------------------------
  for (const Txn& reader : history.txns()) {
    for (const Mop& mop : reader.mops) {
      if (mop.type != MopType::kRead) continue;
      for (const Element element : mop.observed) {
        const auto it = writer.find({mop.key, element});
        if (it == writer.end()) continue;
        const Txn* source = history.find(it->second);
        if (source == nullptr || source->id == reader.id) continue;

        if (source->outcome == Outcome::kAborted) {
          report(Anomaly::kG1a);
          CycleWitness witness;
          witness.anomaly = Anomaly::kG1a;
          witness.txns = {source->id, reader.id};
          witness.detail = "T" + std::to_string(reader.id) + " read element " +
                           std::to_string(element) + " on key " + std::to_string(mop.key) +
                           ", written by T" + std::to_string(source->id) + " which ABORTED";
          result.witnesses.push_back(std::move(witness));
        }
      }

      // An intermediate read: the reader saw some of a transaction's appends to
      // this key but stopped before its last one. Only meaningful for a
      // committed writer -- an aborted one is already G1a.
      if (mop.observed.empty()) continue;
      const auto it = writer.find({mop.key, mop.observed.back()});
      if (it == writer.end()) continue;
      const Txn* source = history.find(it->second);
      if (source == nullptr || source->id == reader.id) continue;
      if (source->outcome != Outcome::kCommitted) continue;
      const Element final_element = source->final_append_to(mop.key);
      if (final_element != 0 && final_element != mop.observed.back()) {
        report(Anomaly::kG1b);
        CycleWitness witness;
        witness.anomaly = Anomaly::kG1b;
        witness.txns = {source->id, reader.id};
        witness.detail = "T" + std::to_string(reader.id) + " read key " +
                         std::to_string(mop.key) + " ending at element " +
                         std::to_string(mop.observed.back()) + ", but T" +
                         std::to_string(source->id) + " finally appended " +
                         std::to_string(final_element);
        result.witnesses.push_back(std::move(witness));
      }
    }
  }

  // -- build the graph -----------------------------------------------------
  Graph graph;
  const auto add_edge = [&](TxnId from, TxnId to, EdgeType type, KeyId key) {
    if (from == to || from == 0 || to == 0) return;  // self-edges are not anomalies
    graph[from].insert(Edge{to, type, key});
  };

  for (const auto& [key, order] : orders) {
    if (order.conflicted) continue;

    // ww: consecutive versions
    for (std::size_t i = 0; i + 1 < order.order.size(); ++i) {
      const auto a = writer.find({key, order.order[i]});
      const auto b = writer.find({key, order.order[i + 1]});
      if (a == writer.end() || b == writer.end()) continue;
      add_edge(a->second, b->second, EdgeType::kWW, key);
    }
  }

  for (const Txn& reader : history.txns()) {
    for (const Mop& mop : reader.mops) {
      if (mop.type != MopType::kRead) continue;
      const auto order_it = orders.find(mop.key);
      if (order_it == orders.end() || order_it->second.conflicted) continue;
      const VersionOrder& order = order_it->second;

      // wr: the reader observed the version written by whoever appended last
      if (!mop.observed.empty()) {
        const auto it = writer.find({mop.key, mop.observed.back()});
        if (it != writer.end()) add_edge(it->second, reader.id, EdgeType::kWR, mop.key);
      }

      // rw: the reader saw version n; whoever wrote version n+1 comes after it
      const std::size_t next = mop.observed.size();
      if (next < order.order.size()) {
        const auto it = writer.find({mop.key, order.order[next]});
        if (it != writer.end()) add_edge(reader.id, it->second, EdgeType::kRW, mop.key);
      }
    }
  }

  if (level == IsolationLevel::kStrictSerializable) {
    // Real-time precedence. If A's commit was acknowledged before B was even
    // invoked, then no serialization order that places B before A is acceptable,
    // however internally consistent it looks. This is the entire difference
    // between serializable and strictly serializable, and it is invisible to a
    // checker that only looks at data dependencies.
    for (const Txn& a : history.txns()) {
      if (a.outcome != Outcome::kCommitted) continue;
      for (const Txn& b : history.txns()) {
        if (a.id == b.id || b.outcome != Outcome::kCommitted) continue;
        if (a.completed < b.invoked) add_edge(a.id, b.id, EdgeType::kRealTime, 0);
      }
    }
  }

  for (const auto& [node, edges] : graph) {
    (void)node;
    result.edges += edges.size();
  }

  // -- cycles --------------------------------------------------------------
  Tarjan tarjan{graph};
  for (const std::vector<TxnId>& component : tarjan.run()) {
    std::vector<TxnId> txns;
    std::vector<EdgeType> edges;
    if (!shortest_cycle(graph, component, &txns, &edges)) continue;

    CycleWitness witness;
    witness.anomaly = classify(edges);
    witness.txns = std::move(txns);
    witness.edges = std::move(edges);
    report(witness.anomaly);
    result.witnesses.push_back(std::move(witness));
  }

  // -- verdict -------------------------------------------------------------
  result.anomalies.assign(found.begin(), found.end());
  result.valid = true;
  for (const Anomaly anomaly : result.anomalies) {
    if (forbids(level, anomaly)) {
      result.valid = false;
      break;
    }
  }
  return result;
}

}  // namespace anvil::checker
