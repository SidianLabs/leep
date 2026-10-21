#!/usr/bin/env bash
# E3: land under realistic conflict pressure, leader on its own runtime.
# Experiment branch soak/e1-leader-runtime only, never merged.
#
# Agents make each change on the head they saw a uniform 0..STALE_MS ago
# (an agent that synced, worked for minutes, then landed), write HOT_FILES
# shared files (lockfile, changelog, registry style) at HOT_FRACTIONS of
# their writes, and after a conflict resolve onto the conflicting head and
# land again, up to RELAND_MAX times. 500 / 2,000 / 5,000 agents, ROUNDS
# rounds, a fresh server and ref at Linux main per run. Reports conflicts
# per land, (re-lands + resends) per logical land and time-to-landed per
# logical land beside the usual metrics.
#
# Refuses to start while E1 (or another E3) runs: they share $S/logs/exp.lock.
#
# Launch on the VPS (returns at once; progress in $S/logs/exp-e3.log):
#   setsid -f bash /data/leep-scale/src-exp/scripts/exp/e3.sh >/dev/null 2>&1 </dev/null
#
# Knobs (env): ROUNDS=3 AGENTS="500 2000 5000" HOT_FRACTIONS="0.02 0.10"
# HOT_FILES=8 STALE_MS=120000 RELAND_MAX=5 ARM=own LEADER_THREADS=2
# DURATION=120 RAMP=30 THINK_MS=2000
set -u
EXP=e3
source "$(dirname "$0")/common.sh"
reexec_from_copy "$@"
take_lock

ROUNDS=${ROUNDS:-3}
AGENTS=${AGENTS:-500 2000 5000}
HOT_FRACTIONS=${HOT_FRACTIONS:-0.02 0.10}
HOT_FILES=${HOT_FILES:-8}
STALE_MS=${STALE_MS:-120000}
RELAND_MAX=${RELAND_MAX:-5}
ARM=${ARM:-own}
DURATION=${DURATION:-120}
RAMP=${RAMP:-30}
THINK_MS=${THINK_MS:-2000}

log "start: rounds $ROUNDS, agents $AGENTS, hot $HOT_FRACTIONS of $HOT_FILES files, stale $STALE_MS ms, reland max $RELAND_MAX, arm $ARM"
build
trap finish EXIT
STAMP=$(date -u +%m%d%H%M)
read -ra hots <<< "$HOT_FRACTIONS"

for round in $(seq 1 "$ROUNDS"); do
  for agents in $AGENTS; do
    for hot in $(rotate $((round - 1)) "${hots[@]}"); do
      label=e3-$ARM-$agents-hot$hot
      start_server "$ARM" || continue
      ref=exp-e3-$STAMP-$agents-hot${hot/./}-r$round
      new_ref "$ref" || continue
      log "run $label r$round on $ref"
      measure "$OUT/$label-r$round" leep-swarm \
        --server "http://$NAME:7070" --metrics "http://$NAME:7071" \
        --database-url "$DBURL" --target "$ref" --agents "$agents" \
        --think-ms "$THINK_MS" --duration "$DURATION" --ramp "$RAMP" --seed "$round" \
        --stale-ms "$STALE_MS" --hot-files "$HOT_FILES" --hot-fraction "$hot" \
        --reland-max "$RELAND_MAX" --json "/out/$label-r$round.json"
      log "done $label r$round (exit $?): $(sed -n 4p "$OUT/$label-r$round.txt" | awk -F'|' '{printf "lands %s, lands/s %s", $2, $3}'); logical: $(sed -n 8p "$OUT/$label-r$round.txt")"
    done
  done
done
