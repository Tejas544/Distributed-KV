#!/usr/bin/env bash
# P7 exit criterion 3, second half: trace validation.
#
#   tools/trace_validate.sh          # 8 seeds, 150 observed states each
#   tools/trace_validate.sh 16 250   # more, and longer
#
# For each seed: run anvil::raft::RaftNode, write the run down in Raft.tla's
# variable vocabulary, and ask TLC whether the specification permits it.
#
# Then the same again with a deliberate bug compiled into the implementation.
# A conformance check that has only ever been observed to pass is
# indistinguishable from one that always passes -- the same argument as the
# hermeticity gate's negative control, and the reason ANV-0005 is in the ledger.
#
# The mutated traces are NOT all expected to be rejected. `restrict_vote_by_log`
# only diverges when a candidate whose log is behind actually asks for a vote
# from a node whose log is ahead, and on some seeds that never happens -- the
# mutation is equivalent *on that seed*. Those are counted and named rather than
# hidden, which is the same discipline the seeded-mutation drill follows.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SEEDS="${1:-8}"
STATES="${2:-150}"
JAR="${TLA_TOOLS:-/d/anvil-tools/tla2tools.jar}"
BIN="${ANVIL_BIN:-/tmp/anvilbin}/anvil_trace_export"

export PATH="/d/msys2/ucrt64/bin:$PATH"   # gotcha 10.1
[ -x "$BIN" ] || { echo "build it first: tools/build.sh trace_export" >&2; exit 2; }
[ -f "$JAR" ] || { echo "no tla2tools.jar at $JAR -- run tools/fetch_tla.sh" >&2; exit 2; }
command -v java >/dev/null || { echo "java not on PATH" >&2; exit 2; }

# Does the trace currently in spec/TraceLog.tla replay? "NotReplayed violated"
# means TLC found a path through the specification reaching the end of the
# trace, which is conformance -- see the header of spec/TraceRaft.tla for why
# the check reads inverted.
#
# Captured to a variable rather than piped, because `pipefail` is on and TLC
# exits non-zero exactly when it finds the violation -- so the pipeline reported
# failure for the outcome being looked for, and every run came back
# "DOES NOT CONFORM" while the same command by hand said otherwise.
replays() {
  local out
  out=$( cd "$ROOT/spec" && java -XX:+UseParallelGC -Xmx4g -cp "$JAR" tlc2.TLC \
             -workers 6 -config TraceRaft.cfg TraceRaft.tla 2>&1 )
  case "$out" in
    *"Invariant NotReplayed is violated"*) return 0 ;;
    *) return 1 ;;
  esac
}

echo "trace validation: $SEEDS seeds x $STATES observed states"
echo

conformed=0
for s in $(seq 1 "$SEEDS"); do
  out=$("$BIN" "$s" "$STATES" "$ROOT/spec/TraceLog.tla" none)
  steps=$(echo "$out" | grep -oE '[0-9]+ implementation steps' | cut -d' ' -f1)
  if replays; then
    conformed=$((conformed + 1))
    printf '  seed %-3s %4s implementation steps   conforms\n' "$s" "$steps"
  else
    printf '  seed %-3s %4s implementation steps   *** DOES NOT CONFORM ***\n' "$s" "$steps"
  fi
done

echo
echo "negative control: the same runs with restrict_vote_by_log switched off"
rejected=0
equivalent=""
for s in $(seq 1 "$SEEDS"); do
  "$BIN" "$s" "$STATES" "$ROOT/spec/TraceLog.tla" vote >/dev/null
  if replays; then
    equivalent="$equivalent $s"
  else
    rejected=$((rejected + 1))
  fi
done
printf '  %d/%d rejected\n' "$rejected" "$SEEDS"
[ -n "$equivalent" ] && echo "  equivalent on seeds:$equivalent (no candidate with a" \
                             "behind log ever asked a node with an ahead log for a vote)"

# Leave a clean trace behind rather than the mutated one, so that a later
# `tools/tlc.sh` or a manual TLC run does not silently replay the broken one.
"$BIN" 1 "$STATES" "$ROOT/spec/TraceLog.tla" none >/dev/null

echo
if [ "$conformed" -ne "$SEEDS" ]; then
  echo "FAILED: $((SEEDS - conformed))/$SEEDS runs are not permitted by spec/Raft.tla"
  exit 1
fi
if [ "$rejected" -eq 0 ]; then
  echo "FAILED: the negative control never fired, so conformance means nothing here"
  exit 1
fi
echo "$conformed/$SEEDS runs conform; $rejected/$SEEDS mutated runs correctly rejected"
