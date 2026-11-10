#!/usr/bin/env bash
# The phase 1 coordination demo (docs/DEMO-COORDINATION.md): twenty scripted
# agents, each with its own token and agent session, on one repository,
# through the real leep-server, leep and leep-mcp binaries. Prints a
# narrated Markdown transcript of what each agent ran and was told, then a
# table of the nine claims and the notices each agent heard. Exits 1 when a
# claim did not hold. The same scenario runs as the leep-e2e test
# `coord_demo`, which asserts the claims.
#
# Usage:
#   scripts/demo-coordination.sh [--out FILE] [--postgres]
#
# --out also writes the transcript to FILE. --postgres keeps metadata in a
# throwaway database on the Postgres server LEEP_TEST_POSTGRES_URL names
# (dropped afterwards) instead of SQLite. Everything else lives in a
# temporary directory removed at the end. Takes a few seconds once built.
set -euo pipefail

cd "$(dirname "$0")/.."

for a in "$@"; do
  case "$a" in
    -h|--help) sed -n '2,16p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
  esac
done

cargo build --quiet --workspace
cargo run --quiet -p leep-e2e --example coord_demo -- "$@"
