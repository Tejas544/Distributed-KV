#!/usr/bin/env bash
# Installs Leiningen, which is all the Elle cross-validation gate needs beyond a
# JRE -- lein bootstraps its own standalone jar, and then fetches Elle and its
# dependencies from Clojars on the first `lein run`.
#
#   tools/fetch_elle.sh
#   export PATH="/d/anvil-tools/bin:$PATH"; export LEIN_HOME=/d/anvil-tools/lein-home
#   tools/elle_cross.sh
set -euo pipefail

DIR="${TLA_DIR:-/d/anvil-tools}"
mkdir -p "$DIR/bin"

echo "fetching leiningen"
curl -sSLf -o "$DIR/bin/lein" \
  https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein
chmod +x "$DIR/bin/lein"

export PATH="$DIR/bin:$PATH"
export LEIN_HOME="${LEIN_HOME:-$DIR/lein-home}"
lein version

echo
echo "now, and the first one downloads Elle from Clojars:"
echo "  export PATH=\"$DIR/bin:\$PATH\" LEIN_HOME=$LEIN_HOME"
echo "  tools/elle_cross.sh"
