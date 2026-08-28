#include "anvil/core/runtime/runtime.h"

namespace anvil {

// Out-of-line so the vtable has a single home translation unit rather than
// being emitted into every object that includes the header.
Runtime::~Runtime() = default;

}  // namespace anvil
