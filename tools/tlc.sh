#!/usr/bin/env bash
# P7 exit criterion 3: run every TLA+ configuration and grade it.
#
#   tools/tlc.sh              # every configuration
#   tools/tlc.sh SsiSnapshot  # one of them
#
# Each row below says what the run is *expected* to do. Half of them are
# expected to FAIL, and on a named invariant -- a specification whose
# properties have only ever been observed to hold is indistinguishable from one
# whose properties are vacuous, and this repository has a ledger row about
# exactly that (ANV-0005). A control that fails on the wrong invariant is
# reported as a failure, not as a pass.
#
# TLA_TOOLS points at tla2tools.jar; the default is where tools/fetch_tla.sh
# puts it.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
JAR="${TLA_TOOLS:-/d/anvil-tools/tla2tools.jar}"
SPEC="$ROOT/spec"
OUT="${TLA_OUT:-/tmp/anvil-tlc}"

if [ ! -f "$JAR" ]; then
  echo "tla2tools.jar not found at $JAR -- run tools/fetch_tla.sh, or set TLA_TOOLS" >&2
  exit 2
fi
command -v java >/dev/null || { echo "java not on PATH" >&2; exit 2; }

mkdir -p "$OUT"

# config | module | mode | expectation
#   expectation: "clean", or "violates:<InvariantName>"
#
# Every row is breadth-first and exhaustive over its slice. That is worth one
# sentence, because the first version of this file had a row running random
# simulation to find the five-server Figure-8 counterexample -- BFS had been
# at depth 14 after ten minutes and 137 million states, and simulation checked
# 112 million without reaching it either. Neither was the answer. The answer
# was that the counterexample is about fifteen steps deep in a space with
# roughly fifty enabled actions per state, so nothing finds it by walking
# further: what finds it is *removing branching*. Switching heartbeats off and
# restricting who may campaign -- both named constants, both documented in the
# .cfg -- took that run from "not found in 112 million states" to "found in
# 11,746". Depth was never the problem.
ROWS=(
  "Raft|Raft|bfs|clean"
  "RaftFigure8Ok|RaftFigure8|bfs|clean"
  "RaftJoint|Raft|bfs|clean"
  "RaftFigure8|RaftFigure8|bfs|violates:Figure8Rule"
  "RaftNoJointCommit|Raft|bfs|violates:LeaderCompleteness"
  "SsiSerializable|SsiCommit|bfs|clean"
  "SsiParallelSnapshot|SsiCommit|bfs|clean"
  "SsiSnapshot|SsiCommit|bfs|violates:NoWriteSkew"
  "SsiParallel|SsiCommit|bfs|violates:NoWriteSkew"
)

want="${1:-}"
failures=0
printf '%-22s %-6s %-28s %s\n' CONFIG MODE EXPECTED RESULT
printf '%.0s-' {1..96}; printf '\n'

for row in "${ROWS[@]}"; do
  IFS='|' read -r cfg module mode expect <<<"$row"
  [ -n "$want" ] && [ "$want" != "$cfg" ] && continue

  log="$OUT/$cfg.log"
  args=(-workers 10)

  start=$(date +%s)
  ( cd "$SPEC" && java -XX:+UseParallelGC -Xmx6g -cp "$JAR" tlc2.TLC \
        "${args[@]}" -config "$cfg.cfg" "$module.tla" ) > "$log" 2>&1
  elapsed=$(( $(date +%s) - start ))

  states=$(grep -oE '^[0-9,]+ states generated' "$log" | tail -1 | cut -d' ' -f1)
  distinct=$(grep -oE '[0-9,]+ distinct states found' "$log" | tail -1 | cut -d' ' -f1)
  violated=$(grep -oE '^Error: (Invariant|Property) [A-Za-z0-9_]+' "$log" | head -1 | awk '{print $3}')

  if [ "$expect" = clean ]; then
    if grep -q "No error has been found" "$log"; then
      result="clean: ${states:-?} states, ${distinct:-?} distinct, ${elapsed}s"
    else
      result="UNEXPECTED: ${violated:-see $log}"
      failures=$((failures + 1))
    fi
  else
    named="${expect#violates:}"
    if [ "$violated" = "$named" ]; then
      result="$named violated as required (${elapsed}s)"
    elif [ -n "$violated" ]; then
      result="WRONG PROPERTY: expected $named, got $violated"
      failures=$((failures + 1))
    else
      result="DID NOT FAIL -- the control is vacuous (${elapsed}s)"
      failures=$((failures + 1))
    fi
  fi
  printf '%-22s %-6s %-28s %s\n' "$cfg" "$mode" "$expect" "$result"
done

printf '%.0s-' {1..96}; printf '\n'
if [ "$failures" -ne 0 ]; then
  echo "$failures configuration(s) did not do what they were supposed to; logs in $OUT"
  exit 1
fi
echo "every TLA+ configuration behaved as specified; logs in $OUT"
