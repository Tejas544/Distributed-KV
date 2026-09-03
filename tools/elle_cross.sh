#!/usr/bin/env bash
# P7 exit criterion 2: cross-validate anvil/checker/elle.cc against Jepsen's
# Elle over 10,000 shared histories.
#
#   tools/elle_cross.sh              # 5,000 correct + 5,000 anomalous
#   tools/elle_cross.sh 500 500      # smaller, for a quick check
#
# Needs a JVM and Leiningen; tools/fetch_elle.sh installs the latter. The first
# run downloads Elle and its dependencies from Clojars and takes a few minutes.
#
# Why this exists at all: every other gate in this repository is Anvil checking
# Anvil. The mutation score is our corpus against our checker; the state-space
# search is our model against our invariants; the TLA+ specs are our
# understanding of the protocol written down a second time. All of that is worth
# doing and none of it can catch a mistake we made consistently. Elle shares no
# ancestry with our checker and has found real anomalies in real databases.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VALID="${1:-5000}"
ANOMALOUS="${2:-5000}"
FILE="${ANVIL_HISTORIES:-/tmp/anvil-histories.edn}"
BIN="${ANVIL_BIN:-/tmp/anvilbin}/anvil_elle_export"

export PATH="/d/msys2/ucrt64/bin:$PATH"   # gotcha 10.1
[ -x "$BIN" ] || { echo "build it first: tools/build.sh elle_export" >&2; exit 2; }
command -v lein >/dev/null || { echo "lein not on PATH -- see tools/fetch_elle.sh" >&2; exit 2; }

"$BIN" "$VALID" "$ANOMALOUS" "$FILE"
cd "$ROOT/tools/elle"
exec lein run -- "$FILE"
