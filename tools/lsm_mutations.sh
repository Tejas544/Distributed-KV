#!/usr/bin/env bash
#
# Seeded-mutation drill for the storage engine.
#
# Four of P2's eight deliberate bugs are planted through DurabilityOptions and
# live in test/lsm_crash.cc. The other four cannot be expressed as a flag --
# they are wrong *logic*, not a missing fsync -- so they are planted here, by
# rewriting a line of source, rebuilding, and confirming the suite goes red.
#
# The point is not the mutations. The point is that a suite which has only ever
# been observed to pass is indistinguishable from a suite that cannot fail, and
# no amount of green runs distinguishes them. This is the same discipline as the
# deliberately non-hermetic archive behind tools/hermetic_check.py.
#
# A mutation that is NOT caught is a finding: either the suite has a hole, or
# the mutation is equivalent (it cannot change observable behaviour). Both are
# worth knowing and the two are not the same thing -- see ANV-0005 for what
# happens when they get confused. Verify equivalence by removing whatever masks
# the mutation and checking it is caught then.
#
# Usage:  tools/lsm_mutations.sh [seeds]
#
set -uo pipefail

cd "$(dirname "$0")/.."
SEEDS="${1:-25}"
WORK=".mutation-work"
CXX="${CXX:-g++}"
FLAGS="-std=c++20 -I. -DANVIL_ENABLE_BUGGIFY=1 -O1"

# Everything except sim_main.cc, which carries its own main() and would collide
# with the test binaries'.
SOURCES=$(find anvil workloads -name '*.cc' ! -name 'sim_main.cc')
mkdir -p "$WORK"

build_and_test() {
  local out="$1"
  # shellcheck disable=SC2086
  $CXX $FLAGS $SOURCES test/lsm_test.cc -o "$WORK/units" 2>"$WORK/build.log" || return 2
  # shellcheck disable=SC2086
  $CXX $FLAGS $SOURCES test/lsm_crash.cc -o "$WORK/crash" 2>>"$WORK/build.log" || return 2
  "$WORK/units" >"$out" 2>&1 || return 1
  "$WORK/crash" "$SEEDS" >>"$out" 2>&1 || return 1
  return 0
}

run_mutation() {
  local name="$1" file="$2" expr="$3"
  cp "$file" "$WORK/backup"
  sed -i "$expr" "$file"

  if cmp -s "$WORK/backup" "$file"; then
    printf '  %-12s %s\n' "SED NO-OP" "$name"
    cp "$WORK/backup" "$file"
    return
  fi

  build_and_test "$WORK/result.log"
  local rc=$?
  cp "$WORK/backup" "$file"

  case $rc in
    0) printf '  %-12s %s\n' "NOT CAUGHT" "$name" ;;
    1) printf '  %-12s %s\n' "caught" "$name" ;;
    2) printf '  %-12s %s (did not compile -- not a valid mutation)\n' "BUILD FAIL" "$name" ;;
  esac
}

echo "storage-engine mutations ($SEEDS seeds each):"

# 5. Recovery scans past a record that failed its checksum instead of stopping.
#    "Salvages" data that was never durably written -- corruption wearing the
#    costume of resilience.
run_mutation "5. WAL recovery salvages past a bad checksum" \
  anvil/core/lsm/wal.cc \
  's|    if (crc != expected_crc) {|    if (false) {|'

# 6. Tombstones dropped at every level, not only the bottom-most. Resurrects
#    deleted keys from older levels, silently and permanently.
run_mutation "6. compaction drops tombstones above the bottom level" \
  anvil/core/lsm/db.cc \
  's|if (bottom_most \&\& type_of(trailer_of(key)) == ValueType::kDeletion) continue;|if (type_of(trailer_of(key)) == ValueType::kDeletion) continue;|'

# 7. The startup orphan sweep deletes the files that ARE live rather than the
#    ones that are not. The manifest then references files that no longer
#    exist -- INV-LSM-09, and unrecoverable.
run_mutation "7. orphan sweep deletes live files instead of orphans" \
  anvil/core/lsm/db.cc \
  's|if (!std::binary_search(live.begin(), live.end(), number)) out->push_back(name);|if (std::binary_search(live.begin(), live.end(), number)) out->push_back(name);|'

# 8. The reader probes one more bit than the writer set, so keys that ARE
#    present start reporting absent. A Bloom false negative is the one failure
#    mode the structure is not allowed to have, and it is invisible until
#    somebody reads a key nobody has read before.
run_mutation "8. Bloom filter probe count off by one (false negatives)" \
  anvil/core/lsm/sstable.cc \
  's|filter.push_back(static_cast<char>(k));|filter.push_back(static_cast<char>(k + 1));|'

# 9. Internal keys sort oldest-version-first, so a seek lands on a stale value.
#    Two substitutions rather than one: sed is line-oriented, and a pattern
#    spanning two lines silently matches nothing -- which reports as SED NO-OP
#    rather than as a passing mutation, but only if you check for it.
run_mutation "9. internal key comparator inverts version order" \
  anvil/core/lsm/format.cc \
  's|if (ta > tb) return -1;|if (ta > tb) return 1;|; s|if (ta < tb) return 1;|if (ta < tb) return -1;|'

# 10. Block checksums computed but never verified. Corrupt blocks served as data
#     -- the direct INV-LSM-11 violation.
run_mutation "10. block checksum computed but never verified" \
  anvil/core/lsm/sstable.cc \
  's|  if (crc32c(contents) != stored) {|  if (false) {|'

rm -rf "$WORK"
