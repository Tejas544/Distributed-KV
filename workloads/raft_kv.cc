#include "workloads/raft_kv.h"

#include <algorithm>
#include <cstring>

#include "anvil/core/lsm/format.h"
#include "anvil/core/raft/config.h"
#include "anvil/core/raft/message.h"

namespace anvil::workloads {
namespace {

using anvil::lsm::get_length_prefixed;
using anvil::lsm::get_varint64;
using anvil::lsm::put_length_prefixed;
using anvil::lsm::put_varint64;

constexpr std::uint8_t kOpWrite = 0;
constexpr std::uint8_t kOpRead = 1;

constexpr std::uint8_t kStatusOk = 0;
constexpr std::uint8_t kStatusNotLeader = 1;
constexpr std::uint8_t kStatusError = 2;

std::uint64_t request_key(std::uint64_t client, std::uint64_t seq) {
  return (client << 32) | (seq & 0xFFFFFFFFULL);
}

// ---------------------------------------------------------------------------
// wire formats
// ---------------------------------------------------------------------------

struct ClientRequest {
  std::uint8_t op = kOpWrite;
  std::uint64_t client = 0;
  std::uint64_t seq = 0;
  std::string key;
  std::string value;
};

struct ClientReply {
  std::uint8_t status = kStatusOk;
  std::uint64_t client = 0;
  std::uint64_t seq = 0;
  std::uint64_t index = 0;
  std::uint64_t leader_hint = 0;
  std::string value;
};

std::string encode_request(const ClientRequest& req) {
  std::string out;
  out.push_back(static_cast<char>(req.op));
  put_varint64(&out, req.client);
  put_varint64(&out, req.seq);
  put_length_prefixed(&out, req.key);
  put_length_prefixed(&out, req.value);
  return out;
}

bool decode_request(std::string_view in, ClientRequest* out) {
  if (in.empty()) return false;
  const char* p = in.data();
  const char* limit = p + in.size();
  out->op = static_cast<std::uint8_t>(*p++);
  p = get_varint64(p, limit, &out->client);
  if (p == nullptr) return false;
  p = get_varint64(p, limit, &out->seq);
  if (p == nullptr) return false;
  std::string_view key;
  std::string_view value;
  p = get_length_prefixed(p, limit, &key);
  if (p == nullptr) return false;
  p = get_length_prefixed(p, limit, &value);
  if (p == nullptr) return false;
  out->key.assign(key);
  out->value.assign(value);
  return true;
}

std::string encode_reply(const ClientReply& reply) {
  std::string out;
  out.push_back(static_cast<char>(reply.status));
  put_varint64(&out, reply.client);
  put_varint64(&out, reply.seq);
  put_varint64(&out, reply.index);
  put_varint64(&out, reply.leader_hint);
  put_length_prefixed(&out, reply.value);
  return out;
}

bool decode_reply(std::string_view in, ClientReply* out) {
  if (in.empty()) return false;
  const char* p = in.data();
  const char* limit = p + in.size();
  out->status = static_cast<std::uint8_t>(*p++);
  for (std::uint64_t* field : {&out->client, &out->seq, &out->index, &out->leader_hint}) {
    p = get_varint64(p, limit, field);
    if (p == nullptr) return false;
  }
  std::string_view value;
  p = get_length_prefixed(p, limit, &value);
  if (p == nullptr) return false;
  out->value.assign(value);
  return true;
}

// The replicated command: what actually goes into the Raft log.
std::string encode_command(std::uint64_t client, std::uint64_t seq, std::string_view key,
                           std::string_view value) {
  std::string out;
  put_varint64(&out, client);
  put_varint64(&out, seq);
  put_length_prefixed(&out, key);
  put_length_prefixed(&out, value);
  return out;
}

std::vector<std::byte> to_bytes(const std::string& in) {
  std::vector<std::byte> out(in.size());
  if (!in.empty()) std::memcpy(out.data(), in.data(), in.size());
  return out;
}

std::string_view as_view(const std::vector<std::byte>& bytes) {
  return std::string_view{reinterpret_cast<const char*>(bytes.data()), bytes.size()};
}

}  // namespace

// ---------------------------------------------------------------------------
// the state machine
// ---------------------------------------------------------------------------

// A map, plus the session table that makes retries safe.
//
// The session table is not an optimisation. A client whose reply was lost has
// to retry, and without deduplication by (client, sequence) the retry applies
// the command twice -- which for a register write is harmless and for anything
// else is data corruption that the workload itself manufactured. Every real
// system has this table; leaving it out would make the harness's findings
// unattributable.
class KvMachine : public raft::StateMachine {
 public:
  using ApplyCallback = std::function<void(std::uint64_t client, std::uint64_t seq,
                                           const std::string& key, LogIndex index)>;

  void set_apply_callback(ApplyCallback callback) { on_apply_ = std::move(callback); }

  void apply(LogIndex index, std::string_view command) override {
    std::uint64_t client = 0;
    std::uint64_t seq = 0;
    std::string_view key;
    std::string_view value;
    const char* p = command.data();
    const char* limit = p + command.size();
    p = get_varint64(p, limit, &client);
    if (p == nullptr) return;
    p = get_varint64(p, limit, &seq);
    if (p == nullptr) return;
    p = get_length_prefixed(p, limit, &key);
    if (p == nullptr) return;
    p = get_length_prefixed(p, limit, &value);
    if (p == nullptr) return;

    auto& session = sessions_[client];
    if (seq > session.first) {
      data_[std::string{key}] = {std::string{value}, index};
      session = {seq, index};
    }
    if (on_apply_) on_apply_(client, seq, std::string{key}, session.second);
  }

  std::string snapshot() const override {
    std::string out;
    put_varint64(&out, data_.size());
    for (const auto& [key, value] : data_) {
      put_length_prefixed(&out, key);
      put_length_prefixed(&out, value.first);
      put_varint64(&out, value.second.value());
    }
    put_varint64(&out, sessions_.size());
    for (const auto& [client, session] : sessions_) {
      put_varint64(&out, client);
      put_varint64(&out, session.first);
      put_varint64(&out, session.second.value());
    }
    return out;
  }

  void restore(std::string_view data) override {
    data_.clear();
    sessions_.clear();
    const char* p = data.data();
    const char* limit = p + data.size();
    std::uint64_t count = 0;
    p = get_varint64(p, limit, &count);
    if (p == nullptr) return;
    for (std::uint64_t i = 0; i < count; ++i) {
      std::string_view key;
      std::string_view value;
      std::uint64_t index = 0;
      p = get_length_prefixed(p, limit, &key);
      if (p == nullptr) return;
      p = get_length_prefixed(p, limit, &value);
      if (p == nullptr) return;
      p = get_varint64(p, limit, &index);
      if (p == nullptr) return;
      data_[std::string{key}] = {std::string{value}, LogIndex{index}};
    }
    p = get_varint64(p, limit, &count);
    if (p == nullptr) return;
    for (std::uint64_t i = 0; i < count; ++i) {
      std::uint64_t client = 0;
      std::uint64_t seq = 0;
      std::uint64_t index = 0;
      p = get_varint64(p, limit, &client);
      if (p == nullptr) return;
      p = get_varint64(p, limit, &seq);
      if (p == nullptr) return;
      p = get_varint64(p, limit, &index);
      if (p == nullptr) return;
      sessions_[client] = {seq, LogIndex{index}};
    }
  }

  bool get(const std::string& key, std::string* value, LogIndex* index) const {
    const auto it = data_.find(key);
    if (it == data_.end()) return false;
    *value = it->second.first;
    *index = it->second.second;
    return true;
  }

  // The answer to "have you already applied this request?", which is what makes
  // an idempotent retry answerable without re-proposing.
  bool applied(std::uint64_t client, std::uint64_t seq, LogIndex* index) const {
    const auto it = sessions_.find(client);
    if (it == sessions_.end() || it->second.first < seq) return false;
    *index = it->second.second;
    return true;
  }

  const std::map<std::string, std::pair<std::string, LogIndex>>& data() const noexcept {
    return data_;
  }

 private:
  std::map<std::string, std::pair<std::string, LogIndex>> data_;
  std::map<std::uint64_t, std::pair<std::uint64_t, LogIndex>> sessions_;
  ApplyCallback on_apply_;
};

RaftKvNode::RaftKvNode() = default;
RaftKvNode::~RaftKvNode() = default;
RaftKvNode::RaftKvNode(RaftKvNode&&) noexcept = default;
RaftKvNode& RaftKvNode::operator=(RaftKvNode&&) noexcept = default;

namespace {

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// Did any node's recovery report that durable state had to be discarded?
bool corruption_detected(const RaftKvState* state) {
  if (state->observer == nullptr) return false;
  for (std::uint32_t i = 1; i <= state->node_count; ++i) {
    if (state->observer->corrupted(NodeId{i})) return true;
  }
  return false;
}

bool node_alive(const RaftKvState* state, NodeId id) {
  if (state->simulation == nullptr) return true;
  return state->simulation->process().alive(id);
}

Timestamp true_now(const RaftKvState* state) {
  return state->simulation == nullptr ? Timestamp{} : state->simulation->scheduler().now();
}

RaftKvNode* node_of(RaftKvState* state, NodeId id) {
  const auto it = state->nodes.find(id.value());
  return it == state->nodes.end() ? nullptr : &it->second;
}

void deliver_reply(RaftKvState* state, NodeId to, const ClientReply& reply);

Task<void> send_reply(Runtime& rt, RaftKvState* state, NodeId to, ClientReply reply) {
  if (to == rt.self()) {
    deliver_reply(state, to, reply);
    co_return;
  }
  ConnHandle conn{};
  co_await rt.connect(to, &conn);
  Message envelope;
  envelope.from = rt.self();
  envelope.to = to;
  envelope.kind = MessageKind::kClientReply;
  envelope.payload = to_bytes(encode_reply(reply));
  co_await rt.send(conn, std::move(envelope));
}

void deliver_reply(RaftKvState* state, NodeId to, const ClientReply& reply) {
  RaftKvNode* node = node_of(state, to);
  if (node == nullptr) return;
  // Late replies for a request the client has already given up on are dropped.
  // A client that accepted one would record an acknowledgement for an operation
  // it had stopped waiting for, which is a harness bug that looks exactly like
  // a duplicate apply.
  if (node->inflight_seq != reply.seq || node->reply_pending) return;
  node->reply_pending = true;
  node->reply_status = reply.status;
  node->reply_index = LogIndex{reply.index};
  node->reply_value = reply.value;
  if (reply.status == kStatusNotLeader && reply.leader_hint != 0) {
    node->leader_hint = NodeId{reply.leader_hint};
  }
}

void resolve_reads(Runtime& rt, RaftKvState* state, NodeId self, LogIndex applied_hint);

// Called from the state machine when a command becomes visible locally.
void on_command_applied(Runtime& rt, RaftKvState* state, NodeId self, std::uint64_t client,
                        std::uint64_t seq, LogIndex index) {
  RaftKvNode* node = node_of(state, self);
  if (node == nullptr) return;
  const auto it = node->pending_writes.find(request_key(client, seq));
  if (it != node->pending_writes.end()) {
    ClientReply reply;
    reply.status = kStatusOk;
    reply.client = client;
    reply.seq = seq;
    reply.index = index.value();
    const NodeId to = it->second.reply_to;
    node->pending_writes.erase(it);
    rt.spawn(send_reply(rt, state, to, reply));
  }
  // The hint matters. This runs from inside apply(), before the driver has
  // advanced the applied index, so asking the node for it would report the
  // previous value and a read waiting on exactly this entry would stall until
  // the client gave up and retried.
  resolve_reads(rt, state, self, index);
}

// A ReadIndex becomes servable once the local state machine has caught up to
// the index the leader confirmed. Both halves matter: the quorum round proves
// no newer leader exists, and the applied-index wait proves this replica has
// actually caught up to what was committed at that moment.
void resolve_reads(Runtime& rt, RaftKvState* state, NodeId self, LogIndex applied_hint) {
  RaftKvNode* node = node_of(state, self);
  if (node == nullptr || node->driver == nullptr) return;
  const LogIndex applied{
      std::max(node->driver->applied_index().value(), applied_hint.value())};

  std::vector<std::uint64_t> done;
  for (auto& [context, read] : node->pending_reads) {
    if (!read.ready || applied < read.index) continue;
    ClientReply reply;
    reply.status = kStatusOk;
    reply.client = read.client;
    reply.seq = read.seq;
    std::string value;
    LogIndex written{};
    if (node->machine->get(read.key, &value, &written)) {
      reply.value = value;
      reply.index = written.value();
    }
    rt.spawn(send_reply(rt, state, read.reply_to, reply));
    done.push_back(context);
  }
  for (const std::uint64_t context : done) node->pending_reads.erase(context);
}

void serve_request(Runtime& rt, RaftKvState* state, NodeId self, NodeId from,
                   const ClientRequest& req) {
  RaftKvNode* node = node_of(state, self);
  if (node == nullptr || node->driver == nullptr || !node->driver->ready()) return;

  ClientReply reply;
  reply.client = req.client;
  reply.seq = req.seq;

  const auto not_leader = [&]() {
    reply.status = kStatusNotLeader;
    reply.leader_hint = node->driver->node().leader().value();
    rt.spawn(send_reply(rt, state, from, reply));
  };

  if (req.op == kOpRead) {
    const std::uint64_t context = node->next_read_context++;
    if (!node->driver->read_index(context).is_ok()) {
      not_leader();
      return;
    }
    RaftKvNode::PendingRead read;
    read.reply_to = from;
    read.client = req.client;
    read.seq = req.seq;
    read.key = req.key;
    node->pending_reads[context] = read;
    return;
  }

  // An already-applied request is answered from the session table. This is the
  // retry path, and it must not re-propose: the command is already in the log.
  LogIndex applied_at{};
  if (node->machine->applied(req.client, req.seq, &applied_at)) {
    reply.status = kStatusOk;
    reply.index = applied_at.value();
    rt.spawn(send_reply(rt, state, from, reply));
    return;
  }

  LogIndex assigned{};
  const Status status = node->driver->propose(
      encode_command(req.client, req.seq, req.key, req.value), &assigned);
  if (!status.is_ok()) {
    not_leader();
    return;
  }
  RaftKvNode::PendingWrite pending;
  pending.reply_to = from;
  pending.client = req.client;
  pending.seq = req.seq;
  node->pending_writes[request_key(req.client, req.seq)] = pending;
}

void handle_foreign(Runtime& rt, RaftKvState* state, NodeId self, const Message& envelope) {
  if (envelope.kind == MessageKind::kClientRequest) {
    ClientRequest req;
    if (!decode_request(as_view(envelope.payload), &req)) return;
    serve_request(rt, state, self, envelope.from, req);
    return;
  }
  ClientReply reply;
  if (!decode_reply(as_view(envelope.payload), &reply)) return;
  deliver_reply(state, self, reply);
}

Task<void> send_request(Runtime& rt, NodeId to, ClientRequest req) {
  ConnHandle conn{};
  co_await rt.connect(to, &conn);
  Message envelope;
  envelope.from = rt.self();
  envelope.to = to;
  envelope.kind = MessageKind::kClientRequest;
  envelope.payload = to_bytes(encode_request(req));
  co_await rt.send(conn, std::move(envelope));
}

// ---------------------------------------------------------------------------
// the client
// ---------------------------------------------------------------------------

Task<void> client_loop(Runtime& rt, RaftKvConfig cfg, RaftKvState* state, NodeId self) {
  RaftKvNode* node = node_of(state, self);
  if (node == nullptr) co_return;

  while (!node->driver->ready()) co_await rt.sleep_for(cfg.client_poll);

  auto& rng = rt.rng(RandomDomain::kWorkload);

  while (!state->done && node->next_seq <= cfg.ops_per_client) {
    co_await rt.sleep_for(cfg.client_interval);

    // Where to send. The hint comes from the last redirect or from this node's
    // own belief; both can be wrong, which is the point -- a client that always
    // knew the leader would never exercise the redirect path.
    NodeId target = node->driver->node().leader();
    if (!target.valid()) target = node->leader_hint;
    if (!target.valid() || !node_alive(state, target)) target = self;

    // The request is drawn once per sequence number and held across retries.
    //
    // Redrawing it would mean a timed-out write coming back as a read under the
    // same identity, and then a late reply to the first attempt gets matched to
    // the second. The client records an acknowledgement for an operation it
    // never issued, and the resulting "stale read" is entirely the harness's
    // invention. A real client retries *the same request*; so does this one.
    if (node->pending_request_seq != node->next_seq) {
      node->pending_request_seq = node->next_seq;
      node->pending_key = "k" + std::to_string(rng.uniform(cfg.keys));
      node->pending_is_read = cfg.read_percent > 0 &&
                              rng.uniform(100) < cfg.read_percent &&
                              state->writes_acked > 0;
    }

    ClientRequest req;
    req.client = self.value();
    req.seq = node->next_seq;
    req.key = node->pending_key;
    const bool is_read = node->pending_is_read;
    req.op = is_read ? kOpRead : kOpWrite;
    if (!is_read) {
      req.value = "c" + std::to_string(self.value()) + ":" + std::to_string(req.seq);
    }

    node->inflight_seq = req.seq;
    node->reply_pending = false;
    node->reply_status = kStatusError;
    node->reply_index = LogIndex{};
    node->reply_value.clear();

    // True time, not this node's opinion of it. The read-freshness property is
    // about real-time order, and a skewed clock would make the check compare
    // against the wrong set of writes.
    const Timestamp invoked = true_now(state);

    if (target == self) {
      serve_request(rt, state, self, self, req);
    } else {
      co_await send_request(rt, target, req);
    }

    const Timestamp deadline = rt.now().advanced_by(cfg.client_timeout);
    while (!node->reply_pending && rt.now() < deadline && !state->done) {
      co_await rt.sleep_for(cfg.client_poll);
    }

    if (!node->reply_pending) {
      ++state->client_timeouts;
      ++state->client_retries;
      continue;  // same sequence number: the retry is idempotent by construction
    }
    if (node->reply_status == kStatusNotLeader) {
      ++state->not_leader_replies;
      ++state->client_retries;
      continue;
    }
    if (node->reply_status != kStatusOk) {
      ++state->client_retries;
      continue;
    }

    if (is_read) {
      ++state->reads_served;
      // INV-RAFT-14, exactly. Find the newest write to this key that was
      // acknowledged before the read was invoked; the read must return that
      // value or a newer one, and the log index makes "newer" precise.
      LogIndex required{};
      bool value_was_acknowledged = node->reply_value.empty();
      const auto it = state->acked.find(req.key);
      if (it != state->acked.end()) {
        for (const AckedWrite& write : it->second) {
          if (write.when < invoked && write.index > required) required = write.index;
          if (write.value == node->reply_value) value_was_acknowledged = true;
        }
      }
      if (node->reply_index < required) {
        const bool corrupted = corruption_detected(state);
        if (!value_was_acknowledged) {
          // Bytes nobody ever wrote. No fault excuses this.
          ++state->invented_reads;
        } else if (corrupted) {
          // Real data, just not the newest, on a run where a node's durable log
          // was damaged and said so. That is durability loss the replication
          // layer could not prevent and did detect -- a different finding from
          // a protocol that returns stale reads on healthy media.
          ++state->stale_reads_after_corruption;
        } else {
          ++state->stale_reads;
        }
        if (state->violations.size() < 8) {
          state->violations.push_back(
              "n" + std::to_string(self.value()) + " read " + req.key + " at index " +
              std::to_string(node->reply_index.value()) +
              " after a write acknowledged at index " + std::to_string(required.value()) +
              (value_was_acknowledged ? " (stale)" : " (INVENTED)") +
              (corrupted ? " [durable state was corrupted on this run]" : ""));
        }
      }
    } else {
      ++state->writes_acked;
      AckedWrite write;
      write.value = req.value;
      write.index = node->reply_index;
      write.when = true_now(state);
      state->acked[req.key].push_back(write);
    }
    ++node->next_seq;
  }
}

// ---------------------------------------------------------------------------
// the administrators
// ---------------------------------------------------------------------------

// Continuous membership change. Toggles the last node between voter and
// learner, over and over, through joint consensus each time. Running this
// *during* the client workload is the point: a membership change that is only
// ever exercised on an idle cluster is a membership change that has not been
// tested.
Task<void> churn_loop(Runtime& rt, RaftKvConfig cfg, RaftKvState* state, NodeId self,
                      std::uint32_t nodes) {
  if (nodes < 4) co_return;  // never shrink a 3-node cluster below a safe quorum
  const std::uint64_t swing = nodes;  // the last node

  for (;;) {
    co_await rt.sleep_for(cfg.churn_interval);
    if (state->done) co_return;
    RaftKvNode* node = node_of(state, self);
    if (node == nullptr || node->driver == nullptr || !node->driver->ready()) continue;
    const raft::RaftNode& raft_node = node->driver->node();
    if (raft_node.role() != raft::Role::kLeader) continue;
    if (raft_node.config().joint()) continue;

    const bool is_voter = raft_node.config().incoming().contains(swing);
    raft::ConfChange change;
    change.kind = raft::ConfChangeKind::kEnterJoint;
    for (const std::uint64_t id : raft_node.config().incoming()) {
      if (is_voter && id == swing) continue;
      change.voters.push_back(id);
    }
    if (!is_voter) change.voters.push_back(swing);
    std::sort(change.voters.begin(), change.voters.end());
    for (const std::uint64_t id : raft_node.config().learners()) {
      if (!is_voter && id == swing) continue;
      change.learners.push_back(id);
    }
    if (is_voter) change.learners.push_back(swing);
    std::sort(change.learners.begin(), change.learners.end());

    if (node->driver->propose_conf_change(change).is_ok()) ++state->conf_changes_proposed;
  }
}

Task<void> transfer_loop(Runtime& rt, RaftKvConfig cfg, RaftKvState* state, NodeId self) {
  for (;;) {
    co_await rt.sleep_for(cfg.transfer_interval);
    if (state->done) co_return;
    RaftKvNode* node = node_of(state, self);
    if (node == nullptr || node->driver == nullptr || !node->driver->ready()) continue;
    const raft::RaftNode& raft_node = node->driver->node();
    if (raft_node.role() != raft::Role::kLeader) continue;

    std::vector<NodeId> candidates;
    for (const NodeId peer : raft_node.config().voters()) {
      if (peer != self && node_alive(state, peer)) candidates.push_back(peer);
    }
    if (candidates.empty()) continue;
    const std::uint64_t pick = rt.rng(RandomDomain::kWorkload).uniform(candidates.size());
    if (node->driver->transfer_leadership(candidates[pick]).is_ok()) {
      ++state->transfers_requested;
    }
  }
}

// Ends the run once every client has finished and every node has caught up.
// Without it every seed costs a full deadline, which over a fleet is most of
// the compute budget spent on nothing.
Task<void> completion_monitor(Runtime& rt, RaftKvConfig cfg, RaftKvState* state,
                              std::uint32_t nodes) {
  for (;;) {
    co_await rt.sleep_for(cfg.churn_interval);
    if (state->done) co_return;
    bool all_done = true;
    for (std::uint32_t i = 1; i <= nodes && all_done; ++i) {
      const auto it = state->nodes.find(i);
      if (it == state->nodes.end()) {
        all_done = false;
        break;
      }
      if (it->second.next_seq <= cfg.ops_per_client) all_done = false;
    }
    if (all_done) state->done = true;
  }
}

void boot_node(sim::Simulation& simulation, RaftKvConfig cfg, RaftKvState* state, NodeId self,
               raft::Config bootstrap, checker::RaftObserver* observer) {
  Runtime& rt = simulation.node(self);
  RaftKvNode& node = state->nodes[self.value()];

  // A crash destroys volatile state. Everything below is rebuilt from scratch
  // and then recovered from disk; carrying anything across would be the node
  // remembering something it has no right to remember.
  node.pending_writes.clear();
  node.pending_reads.clear();
  node.reply_pending = false;
  node.inflight_seq = 0;
  node.leader_hint = NodeId{};
  node.next_read_context = 1;
  node.booted = false;

  ++node.boots;
  if (node.driver != nullptr) {
    node.previous_persisted_high = node.driver->stats().persisted_high;
    node.previous_recovered_last = node.driver->node().log().last_index().value();
    node.previous_fsynced_high = node.driver->stats().fsynced_high;
    node.previous_rewrites = node.driver->stats().rewrites;
  }

  auto machine = std::make_unique<KvMachine>();
  KvMachine* machine_ptr = machine.get();
  machine->set_apply_callback([&rt, state, self](std::uint64_t client, std::uint64_t seq,
                                                 const std::string&, LogIndex index) {
    on_command_applied(rt, state, self, client, seq, index);
  });

  const raft::RaftOptions node_raft =
      cfg.node_raft_options ? cfg.node_raft_options(self, cfg.raft) : cfg.raft;

  const std::uint64_t node_seed = rt.rng(RandomDomain::kConsensus).next_u64();
  auto transport =
      std::make_unique<raft::RaftTransport>(&rt, self, node_raft.tick_interval);
  transport->set_foreign_handler([&rt, state, self](const Message& envelope) {
    handle_foreign(rt, state, self, envelope);
  });
  raft::RaftTransport* transport_ptr = transport.get();
  auto driver = std::make_unique<raft::RaftDriver>(&rt, transport_ptr, GroupId{1}, self, node_raft,
                                                   cfg.durability, std::move(bootstrap),
                                                   machine_ptr, DeterministicRandom{node_seed});
  raft::RaftDriver* driver_ptr = driver.get();
  // Media damage is reported the instant recovery finds it, not by a poller.
  // From here on a term or commit-index regression on this node is *detected*
  // corruption rather than a protocol defect, and conflating the two is the
  // mistake ANV-0007 was.
  driver->set_recovery_callback([state, self](bool damaged) {
    if (!damaged || state->observer == nullptr) return;
    // Bit rot, and nothing else.
    //
    // The exemption exists so that a node whose media was damaged is not
    // reported as a protocol defect (ANV-0007). Only one fault can actually do
    // that: bit rot rewrites bytes that were already durable. Torn writes and
    // reverted sectors apply exclusively to *unsynced* sectors at crash time,
    // so by construction they cannot destroy anything that was fsynced -- and
    // if something fsynced is missing anyway, that is the finding.
    //
    // Gating on torn writes as well was wrong twice over: they are enabled in
    // nearly every profile, and an unsynced write reverting to "old content"
    // that never existed leaves zeros, which fail a checksum and look exactly
    // like damage. That excused a missing fsync on every seed -- the one
    // guarantee that must never be excused.
    if (state->simulation == nullptr) return;
    if (state->simulation->faults().summary().disk.bit_rots > 0) {
      state->observer->note_corruption(self);
    }
  });
  driver->set_read_callback([&rt, state, self](raft::ReadState read) {
    RaftKvNode* n = node_of(state, self);
    if (n == nullptr) return;
    const auto it = n->pending_reads.find(read.context);
    if (it == n->pending_reads.end()) return;
    it->second.index = read.index;
    it->second.ready = true;
    resolve_reads(rt, state, self, LogIndex{});
  });

  node.machine = std::move(machine);
  // Order matters, and not in the way assignment order suggests. The previous
  // incarnation's driver deregisters its group from the previous incarnation's
  // transport when it is destroyed, so the transport must outlive it: assigning
  // over the transport first would free it under a driver that is still holding
  // a pointer. Old driver first, then old transport, then the new pair.
  node.driver.reset();
  node.transport.reset();
  node.transport = std::move(transport);
  node.driver = std::move(driver);
  node.booted = true;

  if (observer != nullptr) observer->set_node(self, &driver_ptr->node());

  rt.spawn(driver_ptr->boot());
  rt.spawn(client_loop(rt, cfg, state, self));
  if (cfg.membership_churn) {
    rt.spawn(churn_loop(rt, cfg, state, self, state->node_count));
  }
  if (cfg.leadership_transfer) rt.spawn(transfer_loop(rt, cfg, state, self));
  if (self == NodeId{1}) rt.spawn(completion_monitor(rt, cfg, state, state->node_count));
}

}  // namespace

// ---------------------------------------------------------------------------
// installation
// ---------------------------------------------------------------------------

void arm_read_invariant(sim::Simulation& simulation, RaftKvState* state) {
  // INV-RAFT-14. The client does the comparison at the moment it receives the
  // value, because that is where the real-time order is known; this predicate
  // reports it within one event, which is what makes the failing run's causal
  // trace point at the read rather than at whatever happened afterwards.
  simulation.invariants().arm(
      "INV-RAFT-14", "a linearizable read never returns state older than a completed write",
      checker::CostClass::kTick, [state]() -> std::optional<std::string> {
        if (state->stale_reads == 0 && state->invented_reads == 0) return std::nullopt;
        state->stale_reads = 0;  // report once per occurrence
        state->invented_reads = 0;
        return state->violations.empty() ? std::string{"a stale linearizable read"}
                                         : state->violations.back();
      });
}

void install(sim::Simulation& simulation, RaftKvConfig config, RaftKvState* state,
             checker::RaftObserver* observer) {
  const std::uint32_t nodes = simulation.node_count();
  if (nodes < 3) throw sim::SimulationPanic("the raft workload needs at least three nodes");
  if (config.learners >= nodes) {
    throw sim::SimulationPanic("a cluster needs at least one voter");
  }

  state->node_count = nodes;
  state->simulation = &simulation;
  state->observer = observer;

  // The lease is only as good as the clock bound the environment declares, so
  // take that bound from the environment rather than assuming one. Where the
  // declared uncertainty is too large for a lease to fit inside an election
  // timeout, lease_is_sound() turns lease reads off and every read pays for a
  // ReadIndex quorum round -- correct, slower, and honest.
  config.raft.max_clock_uncertainty = simulation.config().faults.clock.declared_uncertainty;

  raft::ConfState bootstrap_state;
  for (std::uint32_t i = 1; i <= nodes; ++i) {
    if (i > nodes - config.learners) {
      bootstrap_state.learners.push_back(i);
    } else {
      bootstrap_state.voters.push_back(i);
    }
  }
  const raft::Config bootstrap = raft::Config::from_conf_state(bootstrap_state);

  if (observer != nullptr) {
    checker::RaftObserver::Hooks hooks;
    hooks.tick = [&simulation]() { return simulation.scheduler().tick(); };
    hooks.true_now = [&simulation]() { return simulation.scheduler().now(); };
    hooks.alive = [&simulation](NodeId id) { return simulation.process().alive(id); };
    hooks.node_now = [&simulation](NodeId id) { return simulation.node(id).now(); };
    observer->configure(nodes, std::move(hooks));
    // The gate is on observed damage, decided at the call site in the recovery
    // callback; the observer simply honours what it is told.
    observer->set_media_faults_possible(true);
    checker::arm_raft_invariants(simulation.invariants(), observer);
  }
  arm_read_invariant(simulation, state);

  for (std::uint32_t i = 1; i <= nodes; ++i) {
    const NodeId self{i};
    state->nodes[i] = RaftKvNode{};
    simulation.set_boot(self, [&simulation, config, state, self, bootstrap, observer]() {
      boot_node(simulation, config, state, self, bootstrap, observer);
    });
  }
  for (std::uint32_t i = 1; i <= nodes; ++i) {
    boot_node(simulation, config, state, NodeId{i}, bootstrap, observer);
  }
}

// ---------------------------------------------------------------------------
// audits
// ---------------------------------------------------------------------------

NodeId current_leader(const RaftKvState& state, sim::Simulation& simulation) {
  for (const auto& [id, node] : state.nodes) {
    if (node.driver == nullptr) continue;
    if (!simulation.process().alive(NodeId{id})) continue;
    if (node.driver->node().role() == raft::Role::kLeader) return NodeId{id};
  }
  return NodeId{};
}

std::uint64_t audit_durability(sim::Simulation& simulation, RaftKvState* state) {
  std::uint64_t lost = 0;

  // Whose configuration decides membership. Any live node's will do once the
  // cluster has settled; the leader's is the most current.
  const NodeId leader = current_leader(*state, simulation);
  const raft::Config* config = nullptr;
  for (const auto& [id, node] : state->nodes) {
    if (node.driver == nullptr || !node.driver->ready()) continue;
    if (!simulation.process().alive(NodeId{id})) continue;
    if (config == nullptr || NodeId{id} == leader) config = &node.driver->node().config();
  }

  for (const auto& [key, writes] : state->acked) {
    if (writes.empty()) continue;
    // Only the last acknowledged write to a key is required to be visible; the
    // earlier ones were legitimately overwritten. Checking them all would be
    // checking that the register remembers its own history, which it does not
    // claim to do.
    const AckedWrite& newest = writes.back();
    for (const auto& [id, node] : state->nodes) {
      if (node.driver == nullptr || node.machine == nullptr) continue;
      // A driver stuck retrying `recover()` forever (ANV-0067's fix: recovery
      // refuses to trust a hard-state file with media damage beneath an
      // already-durable record) is process-alive but never became a real
      // cluster participant -- `node_.restore()` never ran, so its machine and
      // config are whatever a freshly-constructed node starts with, not a
      // reflection of anything the cluster actually decided. Auditing it the
      // same as a node that finished recovery would report "missing" against
      // a node that was never eligible to have the write in the first place,
      // which is a harness artifact, not data loss -- the majority that does
      // hold the write is what durability actually rests on.
      if (!node.driver->ready()) continue;
      if (!simulation.process().alive(NodeId{id})) continue;
      // A node the membership change removed is not required to hold anything.
      // The leader stops replicating to it the moment it leaves, correctly, and
      // demanding it keep up afterwards would report a working membership change
      // as data loss.
      if (config != nullptr && !config->is_member(NodeId{id})) continue;
      std::string value;
      LogIndex index{};
      const bool present = node.machine->get(key, &value, &index);
      if (!present || index < newest.index) {
        ++lost;
        if (state->violations.size() < 8) {
          // Say what the leader believes about this node as well as what the
          // node has. "n6 is behind" is a symptom shared by a dozen causes; the
          // leader's Progress separates them -- a stalled `match` with a full
          // inflight window is a wedged pipeline, a `next` that keeps resetting
          // is a probe that cannot find a match, and a healthy Progress against
          // a short log means the entries were sent and lost on the way down.
          std::string detail = "n" + std::to_string(id) + " is missing acknowledged write " +
                               key + "=" + newest.value + " (index " +
                               std::to_string(newest.index.value()) + ")";
          const auto leader_node = state->nodes.find(leader.value());
          if (leader_node != state->nodes.end() && leader_node->second.driver != nullptr) {
            const raft::RaftNode& raft_node = leader_node->second.driver->node();
            const auto pr = raft_node.progress().find(id);
            if (pr != raft_node.progress().end()) {
              static constexpr const char* kStates[] = {"probe", "replicate", "snapshot"};
              detail += "; leader n" + std::to_string(leader.value()) + " has it at match=" +
                        std::to_string(pr->second.match.value()) +
                        " next=" + std::to_string(pr->second.next.value()) + " state=" +
                        kStates[static_cast<int>(pr->second.state)] +
                        " inflight=" + std::to_string(pr->second.inflight) +
                        " idle_rounds=" + std::to_string(pr->second.idle_rounds) +
                        (pr->second.recent_active ? " active" : " silent") +
                        (raft_node.config().is_voter(NodeId{id}) ? " voter" : " learner");
            } else {
              detail += "; the leader has no progress entry for it";
            }
          }
          detail += "; that node's applied index is " + std::to_string(index.value());
          if (node.driver != nullptr) {
            detail += " (sent " + std::to_string(node.driver->stats().messages_sent) +
                      ", received " + std::to_string(node.driver->stats().messages_received) +
                      ", log ends at " +
                      std::to_string(node.driver->node().log().last_index().value()) +
                      ", send failures " +
                      std::to_string(node.driver->stats().send_failures) + ", reconnects " +
                      std::to_string(node.transport == nullptr
                                         ? 0
                                         : node.transport->stats().reconnects) +
                      ")";
            if (leader_node != state->nodes.end() && leader_node->second.driver != nullptr) {
              detail += "; the leader has sent " +
                        std::to_string(leader_node->second.driver->stats().messages_sent) +
                        " with " +
                        std::to_string(leader_node->second.driver->stats().send_failures) +
                        " failures";
            }
          }
          state->violations.push_back(detail);
        }
      }
    }
  }
  state->lost_acked_writes = lost;
  return lost;
}

bool converged(sim::Simulation& simulation, const RaftKvState& state) {
  for (const auto& [key, writes] : state.acked) {
    if (writes.empty()) continue;
    const AckedWrite& newest = writes.back();
    for (const auto& [id, node] : state.nodes) {
      if (node.driver == nullptr || node.machine == nullptr) continue;
      // Same exclusion as audit_durability, and for the same reason: a driver
      // that never finished recovering is not a participant whose absence
      // means anything.
      if (!node.driver->ready()) continue;
      if (!simulation.process().alive(NodeId{id})) continue;
      std::string value;
      LogIndex index{};
      if (!node.machine->get(key, &value, &index)) return false;
      if (index < newest.index) return false;
    }
  }
  return true;
}

}  // namespace anvil::workloads
