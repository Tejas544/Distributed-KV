#include "anvil/sim/sim_runtime.h"

#include <string>
#include <utility>

namespace anvil::sim {

SimRuntime::SimRuntime(NodeId self, Scheduler* scheduler, NetworkModel* net, DiskModel* disk,
                       ClockModel* clock, std::uint64_t seed)
    : self_(self),
      scheduler_(scheduler),
      net_(net),
      disk_(disk),
      clock_(clock),
      // Fork per node as well as per domain, so node 2's draws are unaffected
      // by how many draws node 1 took. Without the node dimension, adding a
      // retry to one node's protocol would shift every other node's stream.
      seed_(DeterministicRandom{seed}.fork(RandomDomain::kApplication, self.value()).next_u64()) {}

// ---------------------------------------------------------------------------
// time
// ---------------------------------------------------------------------------

Timestamp SimRuntime::now() const {
  // This node's *opinion* of the time, not the truth. It carries this node's
  // offset and drift, may be frozen, and may have been stepped by an imaginary
  // NTP daemon. Protocol code that treats it as ground truth is exactly what
  // the clock faults exist to catch.
  return clock_->node_now(self_, scheduler_->now());
}

TimeInterval SimRuntime::now_uncertain() const {
  return clock_->node_now_uncertain(self_, scheduler_->now());
}

Task<void> SimRuntime::sleep_for(Duration d) {
  co_await ScheduledResume{scheduler_, d, self_, "sleep"};
}

Task<void> SimRuntime::sleep_until(Timestamp t) {
  // The deadline is expressed on *this node's* clock, so the wait is computed
  // against this node's reading. A node whose clock runs fast wakes early in
  // real terms -- which is the correct and slightly alarming behaviour, and is
  // how skew turns into premature lease expiry.
  const Timestamp current = now();
  const Duration delay =
      t.physical > current.physical
          ? Duration{static_cast<std::int64_t>(t.physical - current.physical)}
          : Duration{0};
  co_await ScheduledResume{scheduler_, delay, self_, "sleep_until"};
}

TimerId SimRuntime::schedule(Duration delay, std::function<void()> callback) {
  return TimerId{scheduler_->at(delay, self_, EventKind::kTimer, "timer", std::move(callback))};
}

void SimRuntime::cancel(TimerId id) { scheduler_->cancel(id.value()); }

// ---------------------------------------------------------------------------
// randomness
// ---------------------------------------------------------------------------

std::uint64_t SimRuntime::random_u64() { return rng(RandomDomain::kApplication).next_u64(); }

DeterministicRandom& SimRuntime::rng(RandomDomain domain) {
  const auto it = rngs_.find(domain);
  if (it != rngs_.end()) return it->second;
  return rngs_.emplace(domain, DeterministicRandom{seed_}.fork(domain)).first->second;
}

bool SimRuntime::buggify(const BuggifySite& site) { return buggify_fire(site); }

// ---------------------------------------------------------------------------
// network
// ---------------------------------------------------------------------------

Task<Status> SimRuntime::connect(NodeId peer, ConnHandle* out) {
  net_->connect(self_, peer);
  *out = NetworkModel::make_handle(self_, peer);
  co_return Status::ok();
}

Task<Status> SimRuntime::send(ConnHandle conn, Message msg) {
  // Now that the network can fail, this status matters. Note the three
  // outcomes protocol code has to distinguish and usually does not:
  //   ok + delivered      the happy path
  //   ok + silently lost  a drop, or a half-open link. Indistinguishable from
  //                       the happy path at the sender. This is the case that
  //                       breaks "I sent it, therefore they have it".
  //   error               partition or reset. The sender learns the send failed
  //                       but learns nothing about *earlier* sends on the same
  //                       connection, which may or may not have arrived.
  co_return net_->send(self_, NetworkModel::peer_of(conn), std::move(msg));
}

Task<Status> SimRuntime::recv(ConnHandle conn, Message* out) {
  const NodeId peer = NetworkModel::peer_of(conn);
  for (;;) {
    if (net_->try_recv(self_, peer, out)) co_return Status::ok();
    // The loop matters: a wakeup does not guarantee a message is still there
    // once P1 adds cancellation and connection reset. Spinning on a spurious
    // wakeup is cheap; assuming one cannot happen is a bug that shows up months
    // later under a fault profile nobody was thinking about today.
    co_await ParkForMessage{net_, self_, peer};
  }
}

void SimRuntime::close(ConnHandle conn) { net_->close(self_, NetworkModel::peer_of(conn)); }

// ---------------------------------------------------------------------------
// storage
//
// Every operation awaits a modelled latency before applying. Making disk I/O
// take virtual time is not cosmetic: it is what allows other tasks to run
// during a write, which is where every "we assumed this was atomic" bug lives.
// ---------------------------------------------------------------------------

Task<Status> SimRuntime::open(Path path, OpenFlags flags, FileHandle* out) {
  co_await ScheduledResume{scheduler_, disk_->io_latency(), self_, "disk_open"};
  co_return disk_->open(self_, path, flags, out);
}

Task<Status> SimRuntime::pread(FileHandle f, MutableByteView dst, std::uint64_t offset,
                               std::size_t* bytes_read) {
  co_await ScheduledResume{scheduler_, disk_->io_latency(), self_, "disk_pread"};
  co_return disk_->pread(f, dst, offset, bytes_read);
}

Task<Status> SimRuntime::pwrite(FileHandle f, ByteView src, std::uint64_t offset) {
  co_await ScheduledResume{scheduler_, disk_->io_latency(), self_, "disk_pwrite"};
  co_return disk_->pwrite(f, src, offset);
}

Task<Status> SimRuntime::fsync(FileHandle f) {
  co_await ScheduledResume{scheduler_, disk_->fsync_latency(), self_, "disk_fsync"};
  co_return disk_->fsync(f);
}

Task<Status> SimRuntime::ftruncate(FileHandle f, std::uint64_t size) {
  co_await ScheduledResume{scheduler_, disk_->io_latency(), self_, "disk_ftruncate"};
  co_return disk_->ftruncate(f, size);
}

Task<Status> SimRuntime::file_size(FileHandle f, std::uint64_t* out) {
  co_return disk_->file_size(f, out);
}

Task<Status> SimRuntime::close_file(FileHandle f) { co_return disk_->close_file(f); }

Task<Status> SimRuntime::rename(Path from, Path to) {
  co_await ScheduledResume{scheduler_, disk_->io_latency(), self_, "disk_rename"};
  co_return disk_->rename(self_, from, to);
}

Task<Status> SimRuntime::unlink(Path path) {
  co_await ScheduledResume{scheduler_, disk_->io_latency(), self_, "disk_unlink"};
  co_return disk_->unlink(self_, path);
}

Task<Status> SimRuntime::fsync_dir(Path dir) {
  co_await ScheduledResume{scheduler_, disk_->fsync_latency(), self_, "disk_fsync_dir"};
  co_return disk_->fsync_dir(self_, dir);
}

Task<Status> SimRuntime::list_dir(Path dir, std::vector<std::string>* out) {
  co_await ScheduledResume{scheduler_, disk_->io_latency(), self_, "disk_list_dir"};
  co_return disk_->list_dir(self_, dir, out);
}

// ---------------------------------------------------------------------------
// scheduling and observability
// ---------------------------------------------------------------------------

void SimRuntime::spawn(Task<void> task) { scheduler_->spawn(self_, std::move(task)); }

Task<void> SimRuntime::yield() {
  co_await ScheduledResume{scheduler_, Duration{0}, self_, "yield"};
}

void SimRuntime::trace(std::string_view event,
                       std::span<const std::pair<std::string_view, std::uint64_t>> fields) {
  if (!scheduler_->trace().recording()) return;
  // TraceEvent stores `const char*` because the overwhelming majority of names
  // are string literals and copying them per event would dominate the cost of
  // tracing. Callers passing a non-literal view need to own it; for now the
  // fields are copied and the event name is taken as-is.
  std::vector<TraceField> copied;
  copied.reserve(fields.size());
  for (const auto& [name, value] : fields) copied.emplace_back(name.data(), value);
  scheduler_->trace().emit(scheduler_->now(), self_, EventKind::kNote, event.data(), copied);
}

void SimRuntime::panic(std::string_view reason) {
  throw SimulationPanic(std::string{reason});
}

}  // namespace anvil::sim
