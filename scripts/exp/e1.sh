#!/usr/bin/env bash
# E1: does isolating the land leader alone recover land throughput under
# read load? Experiment branch soak/e1-leader-runtime only, never merged.
#
# Arms: server {shared runtime (baseline), leader on its own runtime} x
# generator {reads on, search disabled}, 500 and 2,000 agents, 120 s after a
# 30 s ramp, ROUNDS rounds, arm order rotated per round, a fresh server and a
# fresh ref at Linux main per run. First, a ceiling reference without reads:
# leep-land-bench with and without the leader runtime.
#
# Launch on the VPS (returns at once; progress in $S/logs/exp-e1.log):
#   setsid -f bash /data/leep-scale/src-exp/scripts/exp/e1.sh >/dev/null 2>&1 </dev/null
#
# Knobs (env): ROUNDS=3 AGENTS="500 2000" ARMS="shared own" (add own-blocking
# for replays on blocking threads) LEADER_THREADS=2 DURATION=120 RAMP=30
# THINK_MS=2000 SKIP_LB=0 LB_AGENTS=1,16,64,256 LB_DURATION=20
set -u
EXP=e1
source "$(dirname "$0")/common.sh"
reexec_from_copy "$@"
take_lock

ROUNDS=${ROUNDS:-3}
AGENTS=${AGENTS:-500 2000}
ARMS=${ARMS:-shared own}
DURATION=${DURATION:-120}
RAMP=${RAMP:-30}
THINK_MS=${THINK_MS:-2000}
SKIP_LB=${SKIP_LB:-0}
LB_AGENTS=${LB_AGENTS:-1,16,64,256}
LB_DURATION=${LB_DURATION:-20}

log "start: rounds $ROUNDS, agents $AGENTS, arms $ARMS, leader threads $LEADER_THREADS"
build
trap finish EXIT
STAMP=$(date -u +%m%d%H%M)
read -ra arms <<< "$ARMS"

# Ceiling: lands only, no reads.
if [ "$SKIP_LB" != 1 ]; then
  for round in $(seq 1 "$ROUNDS"); do
    for arm in $(rotate $((round - 1)) "${arms[@]}"); do
      label=lb-$arm
      start_server "$arm" || continue
      ref=exp-e1-$STAMP-$label-r$round
      new_ref "$ref" || continue
      log "run $label r$round on $ref"
      measure "$OUT/$label-r$round" leep-land-bench \
        --server "http://$NAME:7070" --metrics "http://$NAME:7071" \
        --target "$ref" --agents "$LB_AGENTS" --duration "$LB_DURATION"
      log "done $label r$round (exit $?): $(grep -E '^\| 1 \| (64|256) ' "$OUT/$label-r$round.txt" | awk -F'|' '{printf "%s agents %s lands/s; ", $3, $5}')"
    done
  done
fi

# Swarm: reads on and search disabled, per server arm.
for round in $(seq 1 "$ROUNDS"); do
  for agents in $AGENTS; do
    for arm in $(rotate $((round - 1)) "${arms[@]}"); do
      for reads in $(rotate $((round - 1)) reads nosearch); do
        label=sw-$arm-$reads-$agents
        extra=()
        [ "$reads" = nosearch ] && extra=(--no-search)
        start_server "$arm" || continue
        ref=exp-e1-$STAMP-$label-r$round
        new_ref "$ref" || continue
        log "run $label r$round on $ref"
        measure "$OUT/$label-r$round" leep-swarm \
          --server "http://$NAME:7070" --metrics "http://$NAME:7071" \
          --database-url "$DBURL" --target "$ref" --agents "$agents" \
          --think-ms "$THINK_MS" --duration "$DURATION" --ramp "$RAMP" --seed "$round" \
          --json "/out/$label-r$round.json" "${extra[@]}"
        log "done $label r$round (exit $?): $(sed -n 4p "$OUT/$label-r$round.txt" | awk -F'|' '{printf "lands %s, lands/s %s, batch mean %s", $2, $3, $9}')"
      done
    done
  done
done
