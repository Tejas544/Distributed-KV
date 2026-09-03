#!/usr/bin/env bash
# P8: run the seed fleet across every core, then report.
#
#   tools/fleet.sh                 # 200 seeds over $(nproc) shards
#   tools/fleet.sh 5000 12         # 5,000 seeds, 12 shards
#   FLEET_OUT=/d/fleet tools/fleet.sh 20000 12
#
# One process per shard, each writing its own JSONL file: no locking, no
# scheduler, and a shard that dies takes nothing with it. Seeds are assigned by
# `seed % shards`, so adding a shard reshuffles the assignment but never changes
# what a given seed *means* -- which is the property the whole ledger depends on.
#
# Then tools/fleet_report.py aggregates: simulated node-hours, failures
# deduplicated by (workload, invariant, minimised fault signature), and
# candidate ledger rows for the ones that are not already classified.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SEEDS="${1:-200}"
SHARDS="${2:-$(nproc 2>/dev/null || echo 4)}"
OUT="${FLEET_OUT:-/tmp/fleet}"
BIN="${ANVIL_BIN:-/tmp/anvilbin}/anvil_fleet"

export PATH="/d/msys2/ucrt64/bin:$PATH"   # gotcha 10.1
[ -x "$BIN" ] || { echo "build it first: tools/build.sh fleet" >&2; exit 2; }

mkdir -p "$OUT"
rm -f "$OUT"/shard-*.jsonl

echo "fleet: $SEEDS seeds x 5 workloads across $SHARDS shards -> $OUT"
started=$(date +%s)

# Each shard gets a wall-clock budget, and this is not belt-and-braces. A
# fleet's runs are bounded in *simulated* time, which says nothing about how
# long they take: one seed in the first smoke run spent seven wall-minutes
# inside a single raft_kv simulation -- legitimate, just expensive -- while
# eleven other cores sat finished. A killed shard keeps every line it has
# already flushed, so the cost of the timeout is the seeds it did not reach and
# nothing else, and the report says how many shards hit it.
TIMEOUT="${FLEET_SHARD_TIMEOUT:-1800}"

pids=()
for i in $(seq 0 $((SHARDS - 1))); do
  ( timeout --preserve-status -s KILL "$TIMEOUT" \
      "$BIN" --seeds "$SEEDS" --shard "$i" --shards "$SHARDS" \
             --out "$OUT/shard-$i.jsonl" ) > "$OUT/shard-$i.log" 2>&1 &
  pids+=($!)
done

failed=0
for p in "${pids[@]}"; do
  wait "$p" || failed=$((failed + 1))
done
elapsed=$(( $(date +%s) - started ))

echo
cat "$OUT"/shard-*.log
echo
if [ "$failed" -ne 0 ]; then
  echo "$failed of $SHARDS shard(s) hit the ${TIMEOUT}s budget or died. Their"
  echo "completed runs are still counted below; FLEET_SHARD_TIMEOUT raises it."
fi

python3 "$ROOT/tools/fleet_report.py" "$OUT"/shard-*.jsonl
echo
echo "wall clock: ${elapsed}s across $SHARDS shards"
