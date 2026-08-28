#include "anvil/checker/corpus.h"

#include "anvil/core/random.h"

namespace anvil::checker {
namespace {

Timestamp at(std::uint64_t millis) { return Timestamp{millis * 1'000'000, 0}; }

// A transaction builder, so each pathological history reads like the anomaly it
// encodes rather than like twenty lines of struct initialisation.
class Build {
 public:
  Build(TxnId id, Outcome outcome, std::uint64_t invoked_ms, std::uint64_t completed_ms) {
    txn_.id = id;
    txn_.process = id;
    txn_.outcome = outcome;
    txn_.invoked = at(invoked_ms);
    txn_.completed = at(completed_ms);
  }

  Build& append(KeyId key, Element element) {
    Mop mop;
    mop.type = MopType::kAppend;
    mop.key = key;
    mop.element = element;
    txn_.mops.push_back(std::move(mop));
    return *this;
  }

  Build& read(KeyId key, std::vector<Element> observed) {
    Mop mop;
    mop.type = MopType::kRead;
    mop.key = key;
    mop.observed = std::move(observed);
    txn_.mops.push_back(std::move(mop));
    return *this;
  }

  Txn take() { return std::move(txn_); }

 private:
  Txn txn_;
};

struct Ids {
  KeyId x;
  KeyId y;
  Element a1, a2, b1, b2;
};

Ids ids_for(std::size_t variant) {
  const auto v = static_cast<Element>(variant);
  return Ids{
      /*x=*/1 + variant * 2,
      /*y=*/2 + variant * 2,
      /*a1=*/1000 + v * 10 + 1,
      /*a2=*/1000 + v * 10 + 2,
      /*b1=*/1000 + v * 10 + 3,
      /*b2=*/1000 + v * 10 + 4,
  };
}

// ---------------------------------------------------------------------------
// one generator per anomaly
// ---------------------------------------------------------------------------

// T1 and T2 both write x and y, but the version orders disagree about who went
// first. A pure write cycle, visible without anyone reading a value they should
// not have.
CorpusCase make_g0(std::size_t variant) {
  const Ids k = ids_for(variant);
  CorpusCase c;
  c.name = "G0 write cycle #" + std::to_string(variant);
  c.expected = Anomaly::kG0;
  c.rejected_from = IsolationLevel::kReadUncommitted;
  c.history.add(Build(1, Outcome::kCommitted, 0, 10).append(k.x, k.a1).append(k.y, k.b1).take());
  c.history.add(Build(2, Outcome::kCommitted, 1, 11).append(k.x, k.a2).append(k.y, k.b2).take());
  c.history.add(Build(3, Outcome::kCommitted, 20, 21).read(k.x, {k.a1, k.a2}).take());
  c.history.add(Build(4, Outcome::kCommitted, 22, 23).read(k.y, {k.b2, k.b1}).take());
  return c;
}

// A read of a value whose writer rolled back. Nothing subtle: the data never
// existed, and any system that returns it is broken at every level above read
// uncommitted.
CorpusCase make_g1a(std::size_t variant) {
  const Ids k = ids_for(variant);
  CorpusCase c;
  c.name = "G1a aborted read #" + std::to_string(variant);
  c.expected = Anomaly::kG1a;
  c.rejected_from = IsolationLevel::kReadCommitted;
  c.history.add(Build(1, Outcome::kAborted, 0, 10).append(k.x, k.a1).take());
  c.history.add(Build(2, Outcome::kCommitted, 12, 20).read(k.x, {k.a1}).take());
  return c;
}

// T1 appends twice to x and commits. T2 sees only the first append -- a value
// that was real for an instant inside T1 and was never a committed state.
CorpusCase make_g1b(std::size_t variant) {
  const Ids k = ids_for(variant);
  CorpusCase c;
  c.name = "G1b intermediate read #" + std::to_string(variant);
  c.expected = Anomaly::kG1b;
  c.rejected_from = IsolationLevel::kReadCommitted;
  c.history.add(Build(1, Outcome::kCommitted, 0, 10).append(k.x, k.a1).append(k.x, k.a2).take());
  c.history.add(Build(2, Outcome::kCommitted, 12, 20).read(k.x, {k.a1}).take());
  return c;
}

// Each transaction read what the other wrote. Information flowed in a circle,
// so no serial order can explain it.
CorpusCase make_g1c(std::size_t variant) {
  const Ids k = ids_for(variant);
  CorpusCase c;
  c.name = "G1c circular information flow #" + std::to_string(variant);
  c.expected = Anomaly::kG1c;
  c.rejected_from = IsolationLevel::kReadCommitted;
  c.history.add(Build(1, Outcome::kCommitted, 0, 10).append(k.x, k.a1).read(k.y, {k.b1}).take());
  c.history.add(Build(2, Outcome::kCommitted, 0, 10).append(k.y, k.b1).read(k.x, {k.a1}).take());
  return c;
}

// One anti-dependency closes the loop: T2 read T1's write on x, but read a
// version of y that T1 went on to overwrite. Snapshot isolation forbids this;
// it is the shape SI's first-committer-wins rule exists to prevent.
CorpusCase make_g_single(std::size_t variant) {
  const Ids k = ids_for(variant);
  const Element b0 = k.b2;  // the pre-existing version of y
  CorpusCase c;
  c.name = "G-single #" + std::to_string(variant);
  c.expected = Anomaly::kGSingle;
  c.rejected_from = IsolationLevel::kSnapshotIsolation;
  c.history.add(Build(1, Outcome::kCommitted, 0, 5).append(k.y, b0).take());
  c.history.add(Build(2, Outcome::kCommitted, 6, 16).append(k.x, k.a1).append(k.y, k.b1).take());
  c.history.add(Build(3, Outcome::kCommitted, 7, 17).read(k.x, {k.a1}).read(k.y, {b0}).take());
  // Establishes the full version order for y, without which the
  // anti-dependency is unobservable.
  c.history.add(Build(4, Outcome::kCommitted, 30, 31).read(k.y, {b0, k.b1}).take());
  return c;
}

// Classic write skew. Both transactions read what the other was about to write,
// and both committed. Two anti-dependencies, no read of uncommitted data --
// which is exactly why snapshot isolation permits it and serializability does
// not. This case is also a precision test: it must be ACCEPTED at SI.
CorpusCase make_g2_item(std::size_t variant) {
  const Ids k = ids_for(variant);
  CorpusCase c;
  c.name = "G2-item write skew #" + std::to_string(variant);
  c.expected = Anomaly::kG2Item;
  c.rejected_from = IsolationLevel::kSerializable;
  c.history.add(Build(1, Outcome::kCommitted, 0, 10).read(k.y, {}).append(k.x, k.a1).take());
  c.history.add(Build(2, Outcome::kCommitted, 0, 10).read(k.x, {}).append(k.y, k.b1).take());
  c.history.add(Build(3, Outcome::kCommitted, 20, 21).read(k.x, {k.a1}).read(k.y, {k.b1}).take());
  return c;
}

// Two readers disagree about the order of the same key's versions. The history
// is not merely non-serializable, it is incoherent -- there is no version order
// at all, so no graph can be built for that key.
CorpusCase make_version_conflict(std::size_t variant) {
  const Ids k = ids_for(variant);
  CorpusCase c;
  c.name = "version order conflict #" + std::to_string(variant);
  c.expected = Anomaly::kVersionOrderConflict;
  c.rejected_from = IsolationLevel::kReadUncommitted;
  c.history.add(Build(1, Outcome::kCommitted, 0, 10).append(k.x, k.a1).take());
  c.history.add(Build(2, Outcome::kCommitted, 0, 10).append(k.x, k.a2).take());
  c.history.add(Build(3, Outcome::kCommitted, 20, 21).read(k.x, {k.a1, k.a2}).take());
  c.history.add(Build(4, Outcome::kCommitted, 22, 23).read(k.x, {k.a2, k.a1}).take());
  return c;
}

// The same element appended by two transactions. Either the generator failed to
// keep elements unique or the database applied a write twice; both make the
// version order ambiguous and both are worth stopping for.
CorpusCase make_duplicate(std::size_t variant) {
  const Ids k = ids_for(variant);
  CorpusCase c;
  c.name = "duplicate element #" + std::to_string(variant);
  c.expected = Anomaly::kDuplicateElement;
  c.rejected_from = IsolationLevel::kReadUncommitted;
  c.history.add(Build(1, Outcome::kCommitted, 0, 10).append(k.x, k.a1).take());
  c.history.add(Build(2, Outcome::kCommitted, 11, 20).append(k.x, k.a1).take());
  c.history.add(Build(3, Outcome::kCommitted, 30, 31).read(k.x, {k.a1}).take());
  return c;
}

// T1 finished before T2 started, yet the version order puts T2's write first.
// Perfectly serializable -- there is a serial order that explains it -- but not
// the one real time demands. Only strict serializability rejects this, which is
// precisely the distinction the mode exists to make.
CorpusCase make_realtime(std::size_t variant) {
  const Ids k = ids_for(variant);
  CorpusCase c;
  c.name = "real-time violation #" + std::to_string(variant);
  c.expected = Anomaly::kRealTimeViolation;
  c.rejected_from = IsolationLevel::kStrictSerializable;
  c.history.add(Build(1, Outcome::kCommitted, 0, 10).append(k.x, k.a1).take());
  c.history.add(Build(2, Outcome::kCommitted, 20, 30).append(k.x, k.a2).take());
  // The observed order is a2 then a1, contradicting the real-time order.
  c.history.add(Build(3, Outcome::kCommitted, 40, 41).read(k.x, {k.a2, k.a1}).take());
  return c;
}

// A long write cycle spanning eight transactions, with a two-transaction cycle
// nested inside it. Tarjan puts all eight in one strongly connected component,
// so a checker that reports the component rather than a minimal cycle produces
// an eight-transaction "witness" that nobody can act on.
//
// This case exists because of a seeded-mutation run: breaking the cycle
// minimiser was the one deliberate defect the suite failed to catch, since
// every other corpus history has a two-cycle and therefore looks minimal even
// when minimisation is broken. A test that cannot distinguish the two was not
// testing minimisation at all.
CorpusCase make_nested_cycle(std::size_t variant) {
  constexpr std::size_t kChain = 8;
  const KeyId x = 500 + variant * 3;
  const KeyId y = 501 + variant * 3;
  const KeyId z = 502 + variant * 3;
  const auto base = static_cast<Element>(5000 + variant * 100);

  CorpusCase c;
  c.name = "nested cycle (8-txn SCC, 2-txn cycle) #" + std::to_string(variant);
  c.expected = Anomaly::kG0;
  c.rejected_from = IsolationLevel::kReadUncommitted;

  std::vector<Element> x_order;
  for (std::size_t i = 0; i < kChain; ++i) {
    const auto id = static_cast<TxnId>(i + 1);
    const Element element = base + static_cast<Element>(i);
    x_order.push_back(element);
    Build txn(id, Outcome::kCommitted, i, 10 + i);
    txn.append(x, element);
    if (i == 0) {
      txn.append(y, base + 90);  // closes the long cycle: T8 -> T1 on y
      txn.append(z, base + 92);  // closes the short cycle: T2 -> T1 on z
    } else if (i == 1) {
      txn.append(z, base + 91);
    } else if (i + 1 == kChain) {
      txn.append(y, base + 89);
    }
    c.history.add(txn.take());
  }

  c.history.add(Build(100, Outcome::kCommitted, 50, 51).read(x, x_order).take());
  c.history.add(
      Build(101, Outcome::kCommitted, 52, 53).read(y, {base + 89, base + 90}).take());
  c.history.add(
      Build(102, Outcome::kCommitted, 54, 55).read(z, {base + 91, base + 92}).take());
  return c;
}

using Generator = CorpusCase (*)(std::size_t);

constexpr Generator kGenerators[] = {
    make_g0,      make_g1a,              make_g1b,       make_g1c,     make_g_single,
    make_g2_item, make_version_conflict, make_duplicate, make_realtime, make_nested_cycle,
};

}  // namespace

std::vector<CorpusCase> anomalous_corpus(std::uint64_t seed, std::size_t count) {
  constexpr std::size_t kGeneratorCount = sizeof(kGenerators) / sizeof(kGenerators[0]);

  std::vector<CorpusCase> cases;
  cases.reserve(count);
  // The seed only shifts which variant each generator starts at, so every
  // generator is still represented in proportion. A seed that could skew the
  // mix would let a run silently stop covering an anomaly class.
  const auto offset = static_cast<std::size_t>(DeterministicRandom{seed}.uniform(97));
  for (std::size_t i = 0; i < count; ++i) {
    cases.push_back(kGenerators[i % kGeneratorCount](offset + i / kGeneratorCount));
  }
  return cases;
}

History valid_history(std::uint64_t seed, std::size_t txn_count, std::size_t keys) {
  DeterministicRandom rng{seed};
  ReferenceModel model;
  History history;

  Element next_element = 1;
  std::uint64_t clock_ms = 0;

  for (std::size_t i = 0; i < txn_count; ++i) {
    // Serial and non-overlapping: each transaction completes strictly before the
    // next is invoked. That makes the history serializable by construction --
    // and, because the real-time order matches the serial order, strictly
    // serializable too. Any complaint from the checker about a history from this
    // generator is a false positive, with no argument possible.
    const std::uint64_t invoked = clock_ms;
    clock_ms += 1 + rng.uniform(5);
    const std::uint64_t completed = clock_ms;
    clock_ms += 1 + rng.uniform(5);

    std::vector<Mop> mops;
    const std::size_t op_count = 1 + static_cast<std::size_t>(rng.uniform(4));
    for (std::size_t op = 0; op < op_count; ++op) {
      Mop mop;
      mop.key = 1 + rng.uniform(keys);
      if (rng.bernoulli(1, 2)) {
        mop.type = MopType::kAppend;
        mop.element = next_element++;
      } else {
        mop.type = MopType::kRead;
      }
      mops.push_back(std::move(mop));
    }

    const std::vector<Mop> applied = model.apply(mops);

    const TxnId id = history.begin(1 + rng.uniform(4), at(invoked));
    for (const Mop& mop : applied) {
      if (mop.type == MopType::kAppend) {
        history.append(id, mop.key, mop.element);
      } else {
        history.read(id, mop.key, mop.observed);
      }
    }
    history.complete(id, Outcome::kCommitted, at(completed));
  }

  return history;
}

}  // namespace anvil::checker
