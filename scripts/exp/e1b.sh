#!/usr/bin/env bash
# E1b: does ADR 0058's split recover land throughput with search reads on?
# Experiment branch soak/e1-leader-runtime only, never merged.
#
# E1 showed that on the 2-core VPS inline search derivation (default
# --indexing local) cuts lands ~10x, and the leader's own runtime does not
# help. E1b puts the indexing in a second process on the same box, as an
# operator would on one host:
#
#   local  the test server in --indexing local (E1's sw-shared-reads arm:
#          warmer and inline derivation in the server; no indexing header).
#   split  an indexer container ($IDX_NAME, --indexing lease, its own
#          metrics port, no API port, same store and blobs) that must hold
#          all three index leases before the query server (the test server,
#          --indexing off) starts. The swarm targets the query server only
#          and sends `Leep-Indexing: stale-ok` on search, path log and blame
#          reads, as MCP does by default.
#
# Both arms: leader on the shared runtime, search reads on, 500 and 2,000
# agents, ROUNDS rounds, 120 s after a 30 s ramp, think 2,000 ms, arm order
# rotated per round, a fresh server (and indexer) and a fresh ref at Linux
# main per run. Then each of CONFLICT_ARMS once at 2,000 agents with E3's
# conflict settings (stale 120 s, re-land max 5, 8 hot files at 2%), to
# separate conflict rate from read starvation.
#
# Each run saves the swarm report (.txt/.json, with indexing and stale
# answers per route and inline derivations), docker stats (.stats, both
# containers), the server's thread CPU (.cpu), the server log tail and, in
# split runs, the indexer's log tail and both processes' index metrics.
#
# Refuses to start while E1 or E3 runs: they share $S/logs/exp.lock. Leaves
# the test server as E1/E3 do (shared runtime, --indexing local) and removes
# the indexer.
#
# Launch on the VPS (returns at once; progress in $S/logs/exp-e1b.log):
#   setsid -f bash /data/leep-scale/src-exp/scripts/exp/e1b.sh >/dev/null 2>&1 </dev/null
#
# Knobs (env): ROUNDS=3 AGENTS="500 2000" ARMS="local split"
# CONFLICT_ARMS="local split" (empty to skip) CONFLICT_AGENTS=2000
# STALE_MS=120000 RELAND_MAX=5 HOT_FILES=8 HOT_FRACTION=0.02
# SPLIT_HEADER=stale-ok DURATION=120 RAMP=30 THINK_MS=2000
# IDX_MEMORY=3g IDX_CACHE_BYTES=268435456 IDX_POOL=16
set -u
EXP=e1b
source "$(dirname "$0")/common.sh"
reexec_from_copy "$@"
take_lock

ROUNDS=${ROUNDS:-3}
AGENTS=${AGENTS:-500 2000}
ARMS=${ARMS:-local split}
CONFLICT_ARMS=${CONFLICT_ARMS-local split}
CONFLICT_AGENTS=${CONFLICT_AGENTS:-2000}
STALE_MS=${STALE_MS:-120000}
RELAND_MAX=${RELAND_MAX:-5}
HOT_FILES=${HOT_FILES:-8}
HOT_FRACTION=${HOT_FRACTION:-0.02}
SPLIT_HEADER=${SPLIT_HEADER:-stale-ok}
DURATION=${DURATION:-120}
RAMP=${RAMP:-30}
THINK_MS=${THINK_MS:-2000}
IDX_NAME=leep-scale-indexer-linux
IDX_METRICS_PORT=7073
IDX_MEMORY=${IDX_MEMORY:-3g}
IDX_CACHE_BYTES=${IDX_CACHE_BYTES:-268435456}
IDX_POOL=${IDX_POOL:-16}
KINDS="history-tree-v4 log-filter-v1 tree-trigrams-v1"

# metrics_of CONTAINER PORT: the container's /metrics, fetched from inside
# it (the image has no curl; bash's /dev/tcp needs no published port).
metrics_of() {
  sudo docker exec "$1" bash -c "exec 3<>/dev/tcp/127.0.0.1/$2 && printf 'GET /metrics HTTP/1.0\r\nHost: x\r\n\r\n' >&3 && cat <&3" 2>/dev/null
}

stop_indexer() {
  # Graceful, so it releases its leases.
  sudo docker stop -t 60 "$IDX_NAME" >/dev/null 2>&1
  sudo docker rm "$IDX_NAME" >/dev/null 2>&1
  return 0
}

# start_indexer: a fresh indexer, ready and holding every index lease.
start_indexer() {
  stop_indexer
  local t0=$SECONDS
  sudo docker run -d --name "$IDX_NAME" --network leep-scale --user 10001:10001 \
    --memory "$IDX_MEMORY" --memory-swap "$IDX_MEMORY" --ulimit nofile=65536:65536 \
    --env-file "$R/linux.env" -v "$S/blobs-leep_linux":/blobs \
    -e LEEP_LISTEN=0.0.0.0:7070 -e LEEP_BLOBS=file:///blobs -e LEEP_INSECURE_NO_AUTH=1 \
    --entrypoint leep-server "$IMG" \
    --indexing lease --request-timeout 0 --cache-bytes "$IDX_CACHE_BYTES" --warm-interval 250ms \
    --metrics-listen "0.0.0.0:$IDX_METRICS_PORT" --meta-pool-size "$IDX_POOL" >> "$L" 2>&1 \
    || { log "indexer start failed"; return 1; }
  # Ready, then every group's lease held: 6 s stale time plus a 3 s renewal
  # tick if a previous holder died without releasing.
  local i m held k
  for i in $(seq 1 900); do
    m=$(metrics_of "$IDX_NAME" "$IDX_METRICS_PORT")
    held=0
    for k in $KINDS; do
      grep -q "^leep_index_lease_held{kind=\"$k\"} 1" <<< "$m" && held=$((held + 1))
    done
    [ "$held" = 3 ] && break
    sleep 1
  done
  if [ "$held" != 3 ]; then
    log "indexer holds $held of 3 index leases after 900 s"
    return 1
  fi
  if ! sudo docker logs "$IDX_NAME" 2>&1 | sed 's/\x1b\[[0-9;]*m//g' \
      | grep "queries never derive an index" | grep -q lease; then
    log "indexer startup line does not say lease mode"
    return 1
  fi
  log "indexer up in $((SECONDS - t0)) s holding $(grep '^leep_index_lease_held' <<< "$m" | tr '\n' ' ')"
}

# start_arm ARM: the servers of an arm; sets TARGETS, METRICS and HDR.
start_arm() {
  case $1 in
    local)
      stop_indexer
      start_server shared --indexing local || return 1
      if ! sudo docker logs "$NAME" 2>&1 | sed 's/\x1b\[[0-9;]*m//g' \
          | grep -q "warming indexes of moved refs in the background"; then
        log "server is not in local indexing mode"
        return 1
      fi
      METRICS="http://$NAME:7071"
      HDR=()
      ;;
    split)
      # The indexer first, so the query server never sees a store without a
      # live lease (it would derive inline then).
      start_indexer || return 1
      start_server shared --indexing off || return 1
      if ! sudo docker logs "$NAME" 2>&1 | sed 's/\x1b\[[0-9;]*m//g' \
          | grep "queries never derive an index" | grep -q off; then
        log "query server is not in --indexing off mode"
        return 1
      fi
      METRICS="http://$NAME:7071,http://$IDX_NAME:$IDX_METRICS_PORT"
      HDR=(--indexing-header "$SPLIT_HEADER")
      ;;
    *) log "unknown arm $1"; return 1 ;;
  esac
}

# save_split PREFIX: what the indexer and the query server did on indexes.
save_split() {
  sudo docker logs --tail 200 "$IDX_NAME" > "$1.indexer.log" 2>&1
  {
    echo "# indexer ($IDX_NAME)"
    metrics_of "$IDX_NAME" "$IDX_METRICS_PORT" | grep -E '^leep_(index_|derive_total|query_)'
    echo "# query server ($NAME)"
    metrics_of "$NAME" 7071 | grep -E '^leep_(index_|derive_total|query_)'
  } > "$1.metrics" 2>&1
}

done_line() {
  echo "lands $(sed -n 4p "$1.txt" | awk -F'|' '{printf "%s, lands/s %s, batch mean %s", $2, $3, $9}'); $(grep -m1 '^indexing header' "$1.txt" | cut -d'|' -f1,3,4,5)"
}

e1b_finish() {
  stop_indexer
  finish
}

log "start: rounds $ROUNDS, agents $AGENTS, arms $ARMS, conflict arms '$CONFLICT_ARMS' at $CONFLICT_AGENTS, split header $SPLIT_HEADER"
build
trap e1b_finish EXIT
STAMP=$(date -u +%m%d%H%M)
read -ra arms <<< "$ARMS"

for round in $(seq 1 "$ROUNDS"); do
  for agents in $AGENTS; do
    for arm in $(rotate $((round - 1)) "${arms[@]}"); do
      label=sw-$arm-$agents
      start_arm "$arm" || continue
      ref=exp-e1b-$STAMP-$label-r$round
      new_ref "$ref" || continue
      log "run $label r$round on $ref"
      measure "$OUT/$label-r$round" leep-swarm \
        --server "http://$NAME:7070" --metrics "$METRICS" \
        --database-url "$DBURL" --target "$ref" --agents "$agents" \
        --think-ms "$THINK_MS" --duration "$DURATION" --ramp "$RAMP" --seed "$round" \
        --json "/out/$label-r$round.json" "${HDR[@]}"
      rc=$?
      [ "$arm" = split ] && save_split "$OUT/$label-r$round"
      log "done $label r$round (exit $rc): $(done_line "$OUT/$label-r$round")"
    done
  done
done

# Conflict pressure (E3's settings), once per arm.
for arm in $CONFLICT_ARMS; do
  label=cf-$arm-$CONFLICT_AGENTS-hot$HOT_FRACTION
  start_arm "$arm" || continue
  ref=exp-e1b-$STAMP-cf-$arm-$CONFLICT_AGENTS
  new_ref "$ref" || continue
  log "run $label on $ref"
  measure "$OUT/$label" leep-swarm \
    --server "http://$NAME:7070" --metrics "$METRICS" \
    --database-url "$DBURL" --target "$ref" --agents "$CONFLICT_AGENTS" \
    --think-ms "$THINK_MS" --duration "$DURATION" --ramp "$RAMP" --seed 1 \
    --stale-ms "$STALE_MS" --hot-files "$HOT_FILES" --hot-fraction "$HOT_FRACTION" \
    --reland-max "$RELAND_MAX" --json "/out/$label.json" "${HDR[@]}"
  rc=$?
  [ "$arm" = split ] && save_split "$OUT/$label"
  log "done $label (exit $rc): $(done_line "$OUT/$label"); logical: $(sed -n 8p "$OUT/$label.txt")"
done
