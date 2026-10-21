# Shared by scripts/exp/e1.sh, e1b.sh and e3.sh (soak/e1-leader-runtime experiment
# branch, never merged). Runs on the VPS test stack only: it restarts the
# test server container leep-scale-server-linux and never touches the live
# stack (/data/leep, leep-leep-server-1, leep-postgres-1, leep-tunnel-1).
#
# Callers set EXP (e1, e1b or e3) before sourcing.

S=${S:-/data/leep-scale}
R=$S/run
L=$S/logs/exp-$EXP.log
OUT=$S/logs/exp-$EXP
B=${B:-$S/src-exp}
BRANCH=${BRANCH:-soak/e1-leader-runtime}
REPO_URL=${REPO_URL:-https://github.com/SidianLabs/leep}
NAME=leep-scale-server-linux
LOCK=$S/logs/exp.lock
LEADER_THREADS=${LEADER_THREADS:-2}

mkdir -p "$OUT"
log() { echo "$(date -u +%FT%TZ) [$EXP] $*" >> "$L"; }

# One experiment at a time: E1, E1b and E3 share the lock, and none waits.
take_lock() {
  exec 9>>"$LOCK"
  if ! flock -n 9; then
    log "refusing to start: another experiment holds $LOCK ($(cat "$LOCK.holder" 2>/dev/null))"
    echo "another experiment is running ($(cat "$LOCK.holder" 2>/dev/null)); refusing" >&2
    exit 1
  fi
  echo "$EXP pid $$ since $(date -u +%FT%TZ)" > "$LOCK.holder"
}

# Run from a copy, so checking out the branch below cannot change the script
# bash is reading.
reexec_from_copy() {
  if [ -z "${EXP_FROM_COPY:-}" ]; then
    local dir
    dir=$(mktemp -d "$OUT/.script-XXXXXX")
    cp "$(dirname "$0")/common.sh" "$0" "$dir/"
    EXP_FROM_COPY=1 exec bash "$dir/$(basename "$0")" "$@"
  fi
}

# Fetch the branch, build the server image and the generator binaries, and
# copy the binaries under $OUT so another bench rebuilding bench-target
# cannot swap them mid-experiment. Sets SHA, IMG and BIN.
build() {
  if [ ! -d "$B/.git" ]; then
    git clone -q "$REPO_URL" "$B" >> "$L" 2>&1 || { log "clone failed"; exit 1; }
  fi
  cd "$B" || exit 1
  git fetch -q "$REPO_URL" "$BRANCH" >> "$L" 2>&1 || { log "fetch failed"; exit 1; }
  git checkout -q --detach FETCH_HEAD
  SHA=$(git rev-parse --short HEAD)
  IMG=leep:$SHA
  log "branch $BRANCH at $SHA"
  if ! sudo docker image inspect "$IMG" >/dev/null 2>&1; then
    log "building image $IMG"
    sudo docker build -q -t "$IMG" . >> "$L" 2>&1 || { log "image build failed"; exit 1; }
  fi
  log "building leep-swarm and leep-land-bench"
  sudo docker run --rm --name leep-exp-build --memory 3g --cpus 2 \
    -v "$B":/src -v leep-bench-cargo:/usr/local/cargo/registry \
    -v "$S/bench-target":/src/target -w /src rust:1.97-bookworm \
    nice cargo build --release --locked -p leep-soak --bin leep-swarm --bin leep-land-bench \
    >> "$L" 2>&1 || { log "generator build failed"; exit 1; }
  BIN=$OUT/bin-$SHA
  mkdir -p "$BIN"
  sudo cp "$S/bench-target/release/leep-swarm" "$S/bench-target/release/leep-land-bench" "$BIN/"
  DBURL=$(sudo grep -m1 '^LEEP_META=' "$R/linux.env" | cut -d= -f2-)
  [ -n "$DBURL" ] || { log "no LEEP_META in $R/linux.env"; exit 1; }
}

# start_server ARM [SERVER-FLAGS...]: shared (baseline), own (leader on its
# own runtime) or own-blocking (also replays on blocking threads); further
# arguments go to leep-server after the usual flags. Restarts the test server
# and waits for /v1/ready; checks the startup line names the arm.
start_server() {
  local arm=$1 extra=() want
  shift
  case $arm in
    shared) want="leader_runtime=false leader_threads=[0-9]* replay_blocking=false" ;;
    own)
      extra=(-e LEEP_EXP_LEADER_RUNTIME=1 -e "LEEP_EXP_LEADER_THREADS=$LEADER_THREADS")
      want="leader_runtime=true leader_threads=$LEADER_THREADS replay_blocking=false" ;;
    own-blocking)
      extra=(-e LEEP_EXP_LEADER_RUNTIME=1 -e "LEEP_EXP_LEADER_THREADS=$LEADER_THREADS" -e LEEP_EXP_REPLAY_BLOCKING=1)
      want="leader_runtime=true leader_threads=$LEADER_THREADS replay_blocking=true" ;;
    *) log "unknown arm $arm"; exit 1 ;;
  esac
  sudo docker stop -t 60 "$NAME" >/dev/null 2>&1
  sudo docker rm "$NAME" >/dev/null 2>&1
  local t0=$SECONDS
  sudo docker run -d --name "$NAME" --network leep-scale --user 10001:10001 \
    --memory 5g --memory-swap 5g --ulimit nofile=65536:65536 \
    --env-file "$R/linux.env" -v "$S/blobs-leep_linux":/blobs -p 127.0.0.1:7172:7070 \
    -e LEEP_LISTEN=0.0.0.0:7070 -e LEEP_BLOBS=file:///blobs -e LEEP_INSECURE_NO_AUTH=1 \
    "${extra[@]}" --entrypoint leep-server "$IMG" \
    --request-timeout 0 --cache-bytes 536870912 --warm-interval 250ms \
    --metrics-listen 0.0.0.0:7071 --meta-pool-size 32 "$@" >> "$L" 2>&1 \
    || { log "server start failed ($arm)"; return 1; }
  local i
  for i in $(seq 1 900); do
    curl -sf http://127.0.0.1:7172/v1/ready >/dev/null 2>&1 && break
    sleep 1
  done
  if ! curl -sf http://127.0.0.1:7172/v1/ready >/dev/null 2>&1; then
    log "server not ready after 900 s ($arm)"
    return 1
  fi
  if ! sudo docker logs "$NAME" 2>&1 | sed 's/\x1b\[[0-9;]*m//g' \
      | grep "land leader experiment" | grep -q "$want"; then
    log "server arm $arm: startup line does not say '$want'"
    return 1
  fi
  MAIN=$(curl -sf http://127.0.0.1:7172/v1/refs/main | jq -r .target)
  [ -n "$MAIN" ] && [ "$MAIN" != null ] || { log "cannot read main"; return 1; }
  log "server up as $arm in $((SECONDS - t0)) s, main $MAIN"
}

# new_ref NAME: create NAME at Linux main.
new_ref() {
  curl -sf -X POST -H 'content-type: application/json' \
    -d "{\"expected\":null,\"new\":\"$MAIN\"}" \
    "http://127.0.0.1:7172/v1/refs/$1" >/dev/null \
    || { log "creating ref $1 failed"; return 1; }
}

# CPU ticks per thread name of the server process, for the share the land
# leader threads got. Threads that exit mid-run (idle blocking threads) drop
# out, so this is approximate.
thread_cpu() {
  local pid
  pid=$(sudo docker inspect -f '{{.State.Pid}}' "$NAME" 2>/dev/null) || return 0
  sudo sh -c "cat /proc/$pid/task/*/stat 2>/dev/null" \
    | awk '{ n=$2; gsub(/[()]/,"",n); c[n]+=$14+$15 } END { for (k in c) print k, c[k] }' | sort
}

# Sample the server and Postgres containers' CPU while a run lasts.
stats_loop() {
  while :; do
    echo "$(date -u +%T) $(sudo docker stats --no-stream --format '{{.Name}} {{.CPUPerc}} {{.MemUsage}}' 2>/dev/null | tr '\n' ';')"
    sleep 10
  done
}

# measure FILE-PREFIX TOOL ARGS...: run leep-swarm or leep-land-bench in a
# container on the scale network, with CPU accounting around it.
measure() {
  local prefix=$1 tool=$2; shift 2
  thread_cpu | LC_ALL=C sort > "$prefix.cpu-before"
  stats_loop > "$prefix.stats" 2>/dev/null &
  local sp=$!
  timeout 3600 sudo docker run --rm --name leep-exp-gen --network leep-scale \
    --ulimit nofile=65536:65536 -v "$BIN":/b:ro -v "$OUT":/out \
    --entrypoint "/b/$tool" "$IMG" "$@" > "$prefix.txt" 2>&1
  local rc=$?
  sudo docker rm -f leep-exp-gen >/dev/null 2>&1
  kill "$sp" 2>/dev/null; wait "$sp" 2>/dev/null
  thread_cpu | LC_ALL=C sort > "$prefix.cpu-after"
  LC_ALL=C join -a2 -e0 -o 0,1.2,2.2 "$prefix.cpu-before" "$prefix.cpu-after" \
    | awk '{ printf "%s %d\n", $1, $3 - $2 }' > "$prefix.cpu"
  rm -f "$prefix.cpu-before" "$prefix.cpu-after"
  sudo docker logs --tail 200 "$NAME" > "$prefix.server.log" 2>&1
  echo "exit $rc" >> "$prefix.txt"
  return $rc
}

# rotate N WORDS...: the words rotated left by N, so arm order changes per
# round.
rotate() {
  local n=$1; shift
  local a=("$@") k=${#a[@]} i
  for ((i = 0; i < k; i++)); do echo -n "${a[$(((i + n) % k))]} "; done
}

# Leave the test server as a plain baseline when done.
finish() {
  start_server shared || true
  log "done; test server left running $IMG as the shared-runtime baseline"
  rm -f "$LOCK.holder"
}
