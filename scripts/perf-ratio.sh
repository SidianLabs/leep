#!/usr/bin/env bash
# The ratio bench (ADR 0081; docs/PERFORMANCE.md, "Ratio bench in CI"): run
# a base server (A) and a changed one (B) in turn on this machine, A B A B,
# under a short leep-land-bench and a 200-agent leep-swarm, and compare the
# runs with leep-perf-compare. Counts gate hard; wall-time ratios gate only
# on collapses, and a check whose two A runs disagree is inconclusive.
#
# Usage:
#   scripts/perf-ratio.sh --pg postgres://user@host:5432/postgres \
#     --base DIR --pr DIR [--out DIR]
#
# --base and --pr are directories holding a release `leep-server`; --pr
# also holds the `leep-land-bench`, `leep-swarm` and `leep-perf-compare`
# that drive and judge both (the generator stays the same on both sides).
# Give the same directory twice for an A/A run. --pg is any database on a
# Postgres 16 server the user may create databases on; every run gets a
# fresh database, dropped afterwards. On 4 or more CPUs the server is
# pinned to CPUs 0-1 and the generators to 2-3, as on the dev box.
#
# To compare your working tree with its merge base locally:
#   cargo build --release -p leep-server -p leep-soak
#   mkdir -p /tmp/pr && cp target/release/leep-{server,land-bench,swarm,perf-compare} /tmp/pr/
#   git worktree add /tmp/base "$(git merge-base HEAD origin/main)"
#   (cd /tmp/base && cargo build --release -p leep-server) && mkdir -p /tmp/base-bin \
#     && cp target/release/leep-server /tmp/base-bin/    # same CARGO_TARGET_DIR
#   scripts/perf-ratio.sh --pg postgres://... --base /tmp/base-bin --pr /tmp/pr
set -euo pipefail

pg=""
base=""
pr=""
out=""
while [ $# -gt 0 ]; do
  case "$1" in
    --pg) pg=$2; shift 2 ;;
    --base) base=$2; shift 2 ;;
    --pr) pr=$2; shift 2 ;;
    --out) out=$2; shift 2 ;;
    -h|--help) sed -n '2,26p' "$0"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

die() { echo "error: $*" >&2; exit 2; }
[ -n "$pg" ] || die "--pg is required"
[ -x "$base/leep-server" ] || die "no leep-server in --base $base"
for b in leep-server leep-land-bench leep-swarm leep-perf-compare; do
  [ -x "$pr/$b" ] || die "no $b in --pr $pr"
done
command -v psql >/dev/null || die "psql is required"
out=${out:-$(mktemp -d)}
mkdir -p "$out"

# Workload. Small enough for about three minutes in all; the server's pool
# is shrunk with it, so 200 agents press it as 1,000 press 32 connections.
POOL=${LEEP_PERF_POOL:-8}
LAND_AGENTS=${LEEP_PERF_LAND_AGENTS:-32}
LAND_SECONDS=${LEEP_PERF_LAND_SECONDS:-8}
SWARM_AGENTS=${LEEP_PERF_SWARM_AGENTS:-200}
SWARM_SECONDS=${LEEP_PERF_SWARM_SECONDS:-20}
SWARM_RAMP=${LEEP_PERF_SWARM_RAMP:-5}

if [ "$(nproc)" -ge 4 ]; then
  server_cpus=(taskset -c 0,1)
  load_cpus=(taskset -c 2,3)
else
  server_cpus=()
  load_cpus=()
fi

base_url=${pg%/*}
port=17471
server_pid=""
db=""
cleanup() {
  if [ -n "$server_pid" ]; then kill "$server_pid" 2>/dev/null || true; wait "$server_pid" 2>/dev/null || true; fi
  if [ -n "$db" ]; then psql -q "$pg" -c "DROP DATABASE IF EXISTS $db WITH (FORCE)" >/dev/null 2>&1 || true; fi
}
trap cleanup EXIT

run() {
  local name=$1 bin=$2
  local dir=$out/$name
  rm -rf "$dir"; mkdir -p "$dir/blobs"
  db=leep_perf_${name}_$$
  psql -q "$pg" -c "DROP DATABASE IF EXISTS $db WITH (FORCE)" -c "CREATE DATABASE $db" >/dev/null
  local url=$base_url/$db
  port=$((port + 2))
  local api=http://127.0.0.1:$port metrics=http://127.0.0.1:$((port + 1))
  RUST_LOG=warn "${server_cpus[@]}" "$bin/leep-server" --listen "127.0.0.1:$port" \
    --metrics-listen "127.0.0.1:$((port + 1))" --meta "$url" --blobs "file://$dir/blobs" \
    --meta-pool-size "$POOL" >"$dir/server.log" 2>&1 &
  server_pid=$!
  for _ in $(seq 1 100); do
    curl -sf "$api/v1/health" >/dev/null 2>&1 && break
    sleep 0.2
  done
  curl -sf "$api/v1/health" >/dev/null || { cat "$dir/server.log" >&2; die "$name: server did not start"; }
  echo "== $name ($bin): land bench, $LAND_AGENTS agents for ${LAND_SECONDS}s"
  "${load_cpus[@]}" "$pr/leep-land-bench" --server "$api" --metrics "$metrics" --target land \
    --agents "$LAND_AGENTS" --duration "$LAND_SECONDS" --files 2 --json "$dir/land.json" \
    >"$dir/land.txt"
  echo "== $name: swarm, $SWARM_AGENTS agents for ${SWARM_SECONDS}s"
  "${load_cpus[@]}" "$pr/leep-swarm" --server "$api" --metrics "$metrics" --database-url "$url" \
    --agents "$SWARM_AGENTS" --think-ms 500 --duration "$SWARM_SECONDS" --ramp "$SWARM_RAMP" \
    --json "$dir/swarm.json" >"$dir/swarm.txt"
  kill "$server_pid"; wait "$server_pid" 2>/dev/null || true; server_pid=""
  psql -q "$pg" -c "DROP DATABASE IF EXISTS $db WITH (FORCE)" >/dev/null; db=""
}

started=$(date +%s)
run a1 "$base"
run b1 "$pr"
run a2 "$base"
run b2 "$pr"
echo "== runs took $(( $(date +%s) - started ))s; results in $out"
"$pr/leep-perf-compare" --a "$out/a1,$out/a2" --b "$out/b1,$out/b2"
