#!/usr/bin/env bash
# Incremental manual build, because CMake has never been executed on the
# development machine (CONTEXT.md §2). Compiles every source under anvil/ and
# workloads/ once into an object cache, then links whichever test binaries are
# named on the command line.
#
#   tools/build.sh                 # objects only
#   tools/build.sh txn_faults      # objects, then link test/txn_faults.cc
#   tools/build.sh dpor minimiser  # several at once
#
# Set ANVIL_OBJ to move the cache; set ANVIL_BIN to move the binaries.
set -euo pipefail

# Gotcha 10.1: Git for Windows' libstdc++-6.dll shadows the MSYS2 one and the
# binaries segfault inside std::ofstream's constructor.
export PATH="/d/msys2/ucrt64/bin:$PATH"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OBJ="${ANVIL_OBJ:-/tmp/anvilobj}"
BIN="${ANVIL_BIN:-/tmp/anvilbin}"
# An array, not a string: the repository path contains a space, and an unquoted
# expansion splits -I"D:/Placement Projects/..." into two arguments.
FLAGS=(-std=c++20 -I"$ROOT" -DANVIL_ENABLE_BUGGIFY=1 -O2
       -fno-threadsafe-statics -ffp-contract=off -fwrapv)

mkdir -p "$OBJ" "$BIN"
cd "$ROOT"

objs=()
# Gotcha 10.2: sim_main.cc has its own main().
for f in $(find anvil workloads -name '*.cc' ! -name sim_main.cc | sort); do
  o="$OBJ/$(echo "$f" | tr '/' '_' | sed 's/\.cc$/.o/')"
  objs+=("$o")
  if [ ! -f "$o" ] || [ "$f" -nt "$o" ]; then
    echo "CC $f"
    g++ "${FLAGS[@]}" -c "$f" -o "$o"
  fi
done

for t in "$@"; do
  src="test/$t.cc"
  [ -f "$src" ] || { echo "no such test: $src" >&2; exit 1; }
  echo "LD $BIN/anvil_$t"
  g++ "${FLAGS[@]}" "$src" "${objs[@]}" -o "$BIN/anvil_$t"
done
