// Wire encoding for Raft messages.
//
// Explicit, byte-at-a-time, little-endian varints from the LSM's format.h --
// the same primitives, for the same reason. A struct memcpy'd onto the wire
// works perfectly until the day two nodes are built by different compilers, and
// then it fails as a corrupted-log bug rather than as a serialisation bug.
//
// The transport carries a MessageKind as well, which is redundant with the type
// byte in the payload. That is deliberate: the payload is the truth, and the
// MessageKind exists so the causal trace and the network model can classify a
// message without decoding it.

#ifndef ANVIL_CORE_RAFT_MESSAGE_H_
#define ANVIL_CORE_RAFT_MESSAGE_H_

#include <string>

#include "anvil/core/raft/types.h"
#include "anvil/core/runtime/runtime.h"

namespace anvil::raft {

std::string encode_message(const RaftMessage& msg);
bool decode_message(std::string_view payload, RaftMessage* out);

// Reads only the leading group varint. The transport routes on this without
// paying for a full decode, and a zero identifies a coalesced batch rather than
// a message (transport.h).
bool peek_group(std::string_view payload, GroupId* out);

// The transport-level classification, for tracing and for the network model.
MessageKind transport_kind(RaftMessageType type) noexcept;

// Convenience: RaftMessage <-> the runtime's Message envelope.
Message to_transport(const RaftMessage& msg);
bool from_transport(const Message& msg, RaftMessage* out);

}  // namespace anvil::raft

#endif  // ANVIL_CORE_RAFT_MESSAGE_H_
