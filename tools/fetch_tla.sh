#!/usr/bin/env bash
# Fetches tla2tools.jar, which is the only thing the TLA+ gate needs beyond a
# JRE. Kept as a script rather than as a vendored jar because a 2 MB binary in
# a source tree is a thing nobody upgrades and everybody trusts.
#
#   tools/fetch_tla.sh          # -> /d/anvil-tools/tla2tools.jar
#   TLA_DIR=~/tools tools/fetch_tla.sh
set -euo pipefail

DIR="${TLA_DIR:-/d/anvil-tools}"
VERSION="${TLA_VERSION:-v1.7.4}"
URL="https://github.com/tlaplus/tlaplus/releases/download/${VERSION}/tla2tools.jar"

mkdir -p "$DIR"
echo "fetching $URL"
curl -sSLf -o "$DIR/tla2tools.jar" "$URL"
ls -l "$DIR/tla2tools.jar"
echo "now: TLA_TOOLS=$DIR/tla2tools.jar tools/tlc.sh"
