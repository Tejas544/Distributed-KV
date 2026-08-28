# anvil_prod -- ProdRuntime: the real thing.
#
# A naive blocking version lands late in P0 purely to prove the seam is real;
# the io_uring implementation is P9.
#
#   executor.{h,cc}    thread-per-core, message passing between shards
#   net_epoll.{h,cc}   portable fallback
#   net_uring.{h,cc}   Linux io_uring (requires WSL2 on this machine)
#   file_posix.{h,cc}  real pread/pwrite/fsync, optional O_DIRECT
#   clock_hlc.{h,cc}   CLOCK_MONOTONIC + hybrid logical clock
#
# The point of this target is not performance. It is that the same core code
# runs under both runtimes, so a bug the simulator finds is a bug in production
# code rather than in a test double. That property is what separates this from
# mocking, and P9's exit criterion 1 is that the DST suite still passes here.

add_library(anvil_prod INTERFACE)
add_library(anvil::prod ALIAS anvil_prod)
target_link_libraries(anvil_prod INTERFACE anvil::core)
