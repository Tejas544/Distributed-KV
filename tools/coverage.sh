#!/usr/bin/env bash
# P8 exit criterion 3: branch coverage under simulation >= 85% for anvil/core/.
#
#   tools/coverage.sh                # the standard suites, standard seed counts
#   tools/coverage.sh 100            # also run a fleet pass of N seeds first
#
# Builds a separate --coverage-instrumented object/binary cache (ANVIL_COVERAGE=1
# in tools/build.sh; never touches the normal /tmp/anvilobj people rebuild
# constantly), runs every fault suite at its documented seed count so the .gcda
# files reflect real fault-injection coverage rather than a unit-test pass, then
# runs gcov once over every anvil/core/ object together -- one invocation, not
# one per file, because gcov merges shared-header counts across the .gcno files
# it is given in a single call, and anvil/core/ headers carry real inline logic
# (mvcc.h, buggify.h, scheduler.h) that a per-file invocation would undercount.
#
# gcov must be run with cwd = repo root, given the .gcno files' *absolute*
# paths: that is the invocation this script's own author verified actually
# opens the relative "anvil/core/..." source paths gcov reports against --
# every other combination tried (cwd = object dir with -s pointing at root,
# bare relative gcno names, etc.) intermittently failed to open the source and
# silently fell back to reporting 0%, which would have been a fabricated
# result printed with a straight face.
set -euo pipefail

export PATH="/d/msys2/ucrt64/bin:$PATH"   # gotcha 10.1
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FLEET_SEEDS="${1:-0}"

export ANVIL_COVERAGE=1
OBJ="${ANVIL_OBJ:-/tmp/anvilobj-cov}"
BIN="${ANVIL_BIN:-/tmp/anvilbin-cov}"
export ANVIL_OBJ="$OBJ" ANVIL_BIN="$BIN"

cd "$ROOT"

SUITES=(sim_faults lsm_crash raft_faults shard_faults txn_faults mvcc_faults fleet)
echo "building coverage variant: ${SUITES[*]}"
bash tools/build.sh "${SUITES[@]}"

# Stale .gcda from a previous coverage.sh run would let this run's numbers
# ride on top of an old one's -- gcov merges into whatever is already on disk.
find "$OBJ" -name '*.gcda' -delete

echo
echo "running fault suites (their own documented seed counts, CLAUDE.md table):"
"$BIN/anvil_sim_faults"   60 || true
"$BIN/anvil_lsm_crash"    40 || true
"$BIN/anvil_raft_faults"  12 || true
"$BIN/anvil_shard_faults" 24 || true
"$BIN/anvil_txn_faults"   30 || true
"$BIN/anvil_mvcc_faults"  20 || true

if [ "$FLEET_SEEDS" -gt 0 ]; then
  echo
  echo "running a $FLEET_SEEDS-seed fleet pass for broader path diversity:"
  rm -rf /tmp/coverage_fleet && mkdir -p /tmp/coverage_fleet
  "$BIN/anvil_fleet" --seeds "$FLEET_SEEDS" --shard 0 --shards 1 \
      --out /tmp/coverage_fleet/shard-0.jsonl --no-minimise || true
fi

echo
echo "running gcov over anvil/core/ (one invocation, so shared headers merge):"

gcno_files=()
for f in $(find anvil/core -name '*.cc' | sort); do
  mangled="$OBJ/$(echo "$f" | tr '/' '_' | sed 's/\.cc$/.gcno/')"
  [ -f "$mangled" ] && gcno_files+=("$mangled")
done
if [ "${#gcno_files[@]}" -eq 0 ]; then
  echo "no coverage data under $OBJ for anvil/core/ -- did the build/run actually happen?" >&2
  exit 1
fi

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
cp -f "${gcno_files[@]}" "$WORK/"
for g in "$WORK"/*.gcno; do
  gcda="$OBJ/$(basename "${g%.gcno}.gcda")"
  [ -f "$gcda" ] && cp -f "$gcda" "$WORK/"
done

pushd "$WORK" >/dev/null
GCOV_OUT="$(gcov -b -s "$ROOT" ./*.gcno 2>&1)"
popd >/dev/null
rm -rf "$WORK"

GCOV_TXT_FILE="$(mktemp)"
printf '%s' "$GCOV_OUT" > "$GCOV_TXT_FILE"
python3 - "$GCOV_TXT_FILE" <<'PYEOF'
import re
import sys

with open(sys.argv[1], encoding="utf-8", errors="replace") as fh:
    _stdin_text = fh.read()

root_prefix = "anvil/core/"
lines_exec = lines_total = 0
branches_exec = branches_total = 0
per_file = []

blocks = re.split(r"^File '([^']+)'\n", _stdin_text, flags=re.M)
# blocks[0] is preamble text before the first File header; skip it.
for i in range(1, len(blocks), 2):
    path, body = blocks[i], blocks[i + 1]
    if not path.startswith(root_prefix):
        continue
    lm = re.search(r"Lines executed:([\d.]+)% of (\d+)", body)
    bm = re.search(r"Branches executed:([\d.]+)% of (\d+)", body)
    if not lm:
        continue
    lpct, ltotal = float(lm.group(1)), int(lm.group(2))
    lexec = round(lpct / 100.0 * ltotal)
    lines_exec += lexec
    lines_total += ltotal
    bexec = btotal = 0
    if bm:
        bpct, btotal = float(bm.group(1)), int(bm.group(2))
        bexec = round(bpct / 100.0 * btotal)
        branches_exec += bexec
        branches_total += btotal
    per_file.append((path, lexec, ltotal, bexec, btotal))

if lines_total == 0:
    print("no anvil/core/ files appeared in gcov output -- something upstream is wrong",
          file=sys.stderr)
    sys.exit(1)

per_file.sort(key=lambda r: (r[3] / r[4] if r[4] else 1.0, r[0]))
print()
print("  per-file branch coverage (lowest first):")
for path, lexec, ltotal, bexec, btotal in per_file:
    bpct = 100.0 * bexec / btotal if btotal else 100.0
    print(f"    {bpct:5.1f}%  ({bexec:>4}/{btotal:<4} branches)  {path}")

line_pct = 100.0 * lines_exec / lines_total
branch_pct = 100.0 * branches_exec / branches_total if branches_total else 100.0
print()
print(f"  anvil/core/ line coverage ....... {lines_exec}/{lines_total} ({line_pct:.1f}%)")
print(f"  anvil/core/ branch coverage ..... {branches_exec}/{branches_total} ({branch_pct:.1f}%)")
verdict = "MEETS" if branch_pct >= 85.0 else "BELOW"
print(f"  {verdict} the >=85% branch-coverage exit criterion")
PYEOF
rm -f "$GCOV_TXT_FILE"
