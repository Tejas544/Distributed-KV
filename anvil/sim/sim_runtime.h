// SimRuntime: one node's view of the simulated world.
//
// This is the other half of the seam. Protocol code holds a Runtime& and cannot
// tell whether it is talking to this class or to ProdRuntime -- which is the
// property that makes a simulator-found bug a bug in production code rather
// than in a test double.
//
// Nothing here makes decisions. Every choice about latency, ordering, failure
// and time lives in the models; SimRuntime just translates the Runtime
// vocabulary into scheduler events.

#ifndef ANVIL_SIM_SIM_RUNTIME_H_
#define ANVIL_SIM_SIM_RUNTIME_H_

#include <coroutine>
#include <cstdint>
#include <map>

#include "anvil/core/runtime/runtime.h"
#include "anvil/sim/clock_model.h"
#include "anvil/sim/disk_model.h"
#include "anvil/sim/net_model.h"
#include "anvil/sim/scheduler.h"

namespace anvil::sim {

// Parks the caller until a message arrives on this endpoint.
struct ParkForMessage {
  NetworkModel* net;
  NodeId self;
  NodeId peer;

  bool await_ready() const noexcept { return false; }
  void await_suspend(std::coroutine_handle<> h) const { net->park_receiver(self, peer, h); }
  void await_resume() const noexcept {}
};

class SimRuntime final : public Runtime {
 public:
  SimRuntime(NodeId self, Scheduler* scheduler, NetworkModel* net, DiskModel* disk,
             ClockModel* clock, std::uint64_t seed);

  NodeId self() const override { return self_; }

  Timestamp now() const override;
  TimeInterval now_uncertain() const override;
  Task<void> sleep_for(Duration d) override;
  Task<void> sleep_until(Timestamp t) override;
  TimerId schedule(Duration delay, std::function<void()> callback) override;
  void cancel(TimerId id) override;

  std::uint64_t random_u64() override;
  DeterministicRandom& rng(RandomDomain domain) override;
  bool buggify(const BuggifySite& site) override;

  Task<Status> connect(NodeId peer, ConnHandle* out) override;
  Task<Status> send(ConnHandle conn, Message msg) override;
  Task<Status> recv(ConnHandle conn, Message* out) override;
  void close(ConnHandle conn) override;

  Task<Status> open(Path path, OpenFlags flags, FileHandle* out) override;
  Task<Status> pread(FileHandle f, MutableByteView dst, std::uint64_t offset,
                     std::size_t* bytes_read) override;
  Task<Status> pwrite(FileHandle f, ByteView src, std::uint64_t offset) override;
  Task<Status> fsync(FileHandle f) override;
  Task<Status> ftruncate(FileHandle f, std::uint64_t size) override;
  Task<Status> file_size(FileHandle f, std::uint64_t* out) override;
  Task<Status> close_file(FileHandle f) override;
  Task<Status> rename(Path from, Path to) override;
  Task<Status> unlink(Path path) override;
  Task<Status> fsync_dir(Path dir) override;
  Task<Status> list_dir(Path dir, std::vector<std::string>* out) override;

  void spawn(Task<void> task) override;
  Task<void> yield() override;

  void trace(std::string_view event,
             std::span<const std::pair<std::string_view, std::uint64_t>> fields) override;
  [[noreturn]] void panic(std::string_view reason) override;

 private:
  NodeId self_;
  Scheduler* scheduler_;
  NetworkModel* net_;
  DiskModel* disk_;
  ClockModel* clock_;

  // std::map rather than an array indexed by domain: RandomDomain values are
  // sparse salts, not indices, and map nodes are address-stable so the
  // reference returned by rng() stays valid as new domains appear.
  std::map<RandomDomain, DeterministicRandom> rngs_;
  std::uint64_t seed_;
};

}  // namespace anvil::sim

#endif  // ANVIL_SIM_SIM_RUNTIME_H_
