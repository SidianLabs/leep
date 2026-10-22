#!/usr/bin/env bash
# The ADR 0059 phase 2 gate (section 11): measure, on one store, whether
# unreachable-changeset GC passes the 20% rule, and whether `gc --full` can be
# scheduled under the backup fence. Read-only: it runs the dry runs only.
#
# Run it on a release build, against Postgres, on a host with at least 4
# cores, never beside production servers and never on the 2-core VPS. Run it
# twice: on the swarm store (after at least 10 x 1,024 lands per ref) and on a
# store restored from a sealed backup of the Linux store. Paste the summary it
# prints into docs/PERFORMANCE.md ("Unreachable changesets (E4)").
#
# Usage:
#   LEEP_META=postgres://... LEEP_BLOBS=file:///... \
#     scripts/unreachable-gc-gate.sh --label swarm [--backups DIR | --backup-fence 6h] \
#       [--bin target/release/leep-server] [--out DIR] [--interval 24h]
#
# --backups DIR derives the backup fence the way `gc --full --backups` does;
# --interval is how often the operator would run `gc --full` (default 24h).
set -euo pipefail

label=""
bin=target/release/leep-server
out=""
backups=""
backup_fence=""
interval=24h
while [ $# -gt 0 ]; do
  case "$1" in
    --label) label=$2; shift 2 ;;
    --bin) bin=$2; shift 2 ;;
    --out) out=$2; shift 2 ;;
    --backups) backups=$2; shift 2 ;;
    --backup-fence) backup_fence=$2; shift 2 ;;
    --interval) interval=$2; shift 2 ;;
    -h|--help) sed -n '2,20p' "$0"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

die() { echo "error: $*" >&2; exit 2; }
[ -n "$label" ] || die "--label is required (swarm, linux-restored, ...)"
[ -n "${LEEP_META:-}" ] || die "LEEP_META is not set"
[ -n "${LEEP_BLOBS:-}" ] || die "LEEP_BLOBS is not set"
case "$LEEP_META" in postgres://*|postgresql://*) ;; *) die "the gate is measured on Postgres" ;; esac
[ -x "$bin" ] || die "$bin not found; cargo build --release --workspace first"
case "$bin" in *release*) ;; *) die "use a release build (got $bin)" ;; esac
cores=$(nproc)
[ "$cores" -ge 4 ] || die "$cores cores: the exact-trees mark needs a host with at least 4 (never the VPS)"
[ -n "$backups" ] || [ -n "$backup_fence" ] || die "pass --backups DIR or --backup-fence"

secs() { # 90s, 30m, 24h, 7d or bare seconds
  local v=$1 n=${1%[smhd]}
  case "$v" in
    *s) echo "$n" ;; *m) echo $((n * 60)) ;; *h) echo $((n * 3600)) ;; *d) echo $((n * 86400)) ;;
    *) echo "$v" ;;
  esac
}

out=${out:-gate-$label-$(date -u +%Y%m%dT%H%M%SZ)}
mkdir -p "$out"
srv() { nice -n 10 "$bin" --meta-pool-size 4 --meta "$LEEP_META" --blobs "$LEEP_BLOBS" "$@"; }

echo "== dry run with exact trees (a full mark; this takes a while)"
t0=$(date +%s)
srv gc --dry-run --unreachable --grace 0 --ref-log-retention 0 --exact-trees >"$out/unreachable.txt"
t1=$(date +%s)
echo "== blob GC full mark, dry run (what one gc --full mark costs)"
srv gc --dry-run --full --grace 0 >"$out/full-mark.txt"
t2=$(date +%s)
mark_secs=$((t2 - t1))
# gc --full marks twice (condemn and collect) and waits the fence between.
fence_secs=$(secs 15m)
full_secs=$((2 * mark_secs + fence_secs))

if [ -n "$backups" ]; then
  # Same rule as `gc --full --backups`: 3 x the longest sealed run of the last
  # 30 days (created_at - snapshot_taken_at), at least an hour.
  command -v jq >/dev/null || die "jq is needed to read backup manifests"
  now=$(date +%s)
  longest=0
  for m in "$backups"/snapshots/*/manifest.json; do
    [ -f "$m" ] || continue
    read -r created taken < <(jq -r '"\(.created_at) \(.snapshot_taken_at)"' "$m")
    [ "$created" -ge $((now - 30 * 86400)) ] || continue
    run=$((created - taken))
    [ "$run" -gt "$longest" ] && longest=$run
  done
  [ "$longest" -gt 0 ] || die "no sealed backup of the last 30 days in $backups"
  bf_secs=$((3 * longest)); [ "$bf_secs" -ge 3600 ] || bf_secs=3600
else
  bf_secs=$(secs "$backup_fence")
fi
interval_secs=$(secs "$interval")
tombstone_secs=$(secs 7d)

f=$out/unreachable.txt
num() { grep -m1 -F "$1" "$f" | sed -E 's/.*: ([0-9]+)[^0-9]*$/\1/'; }
lands=$(grep -m1 '^ref log entries:' "$f" | sed -E 's/.*\(([0-9]+) lands\).*/\1/')
refs=$(grep -m1 '^refs:' "$f" | sed -E 's/refs: ([0-9]+)/\1/')
write=$(num '  write path (')
log_in_write=$(num '  of which their ref log entries:')
index=$(num '  index rows (')
reclaim_meta=$(num '  unreachable, reclaimable as designed:')
reflog_bytes=$(grep -m1 '^  ref log:' "$f" | sed -E 's/.* ([0-9]+) bytes/\1/')
live=$(grep -m1 'blob store, live tree nodes' "$f")
live_nodes=$(echo "$live" | sed -E 's/.*nodes ([0-9]+), contents ([0-9]+).*/\1/')
live_contents=$(echo "$live" | sed -E 's/.*nodes ([0-9]+), contents ([0-9]+).*/\2/')
recl=$(grep -m1 'blob store, reclaimable as designed' "$f")
recl_nodes=$(echo "$recl" | sed -E 's/.*tree nodes ([0-9]+), contents ([0-9]+).*/\1/')
recl_contents=$(echo "$recl" | sed -E 's/.*tree nodes ([0-9]+), contents ([0-9]+).*/\2/')
for v in lands refs write log_in_write index reclaim_meta reflog_bytes live_nodes live_contents recl_nodes recl_contents; do
  [ -n "${!v}" ] || die "could not read $v from $f"
done

# The whole ref log, not only the land entries the write path counts.
reflog_per_land=$((reflog_bytes / (lands > 0 ? lands : 1)))
meta_denominator=$((write - log_in_write + reflog_per_land + index))
blob_growth=$((live_nodes + live_contents + recl_nodes + recl_contents))
recl_blob=$((recl_nodes + recl_contents))
pct() { awk -v a="$1" -v b="$2" 'BEGIN { printf "%.1f", (b > 0 ? 100 * a / b : 0) }'; }
meta_pct=$(pct "$reclaim_meta" "$meta_denominator")
combined_pct=$(pct $((reclaim_meta + recl_blob)) $((meta_denominator + blob_growth)))
lands_per_ref=$((lands / (refs > 0 ? refs : 1)))

schedulable=no
if [ "$full_secs" -lt "$interval_secs" ] && [ "$bf_secs" -le "$tombstone_secs" ]; then
  schedulable=yes
fi
meta_pass=$(awk -v p="$meta_pct" 'BEGIN { print (p >= 20 ? "yes" : "no") }')
combined_pass=$(awk -v p="$combined_pct" -v s="$schedulable" 'BEGIN { print (p >= 20 && s == "yes" ? "yes" : "no") }')
window_ok=$([ "$lands_per_ref" -ge 10240 ] && echo yes || echo "no (need 10,240)")

summary=$out/summary.md
{
  echo "### ADR 0059 gate: $label ($(date -u +%Y-%m-%d))"
  echo
  echo "Host: $cores cores. Store: $lands lands on $refs ref(s), $lands_per_ref per ref: $window_ok."
  echo "Release build \`$bin\`, dry run with \`--grace 0 --exact-trees\` in $((t1 - t0)) s."
  echo
  echo "| Logical bytes per land | |"
  echo "|---|---|"
  echo "| Write path, without its ref log entries | $((write - log_in_write)) |"
  echo "| Ref log, all entries | $reflog_per_land |"
  echo "| Index rows | $index |"
  echo "| **Metadata growth** | **$meta_denominator** |"
  echo "| Reclaimable metadata (ref log kept) | $reclaim_meta |"
  echo "| Live tree nodes + contents | $((live_nodes + live_contents)) |"
  echo "| Reclaimable tree nodes + contents, exact | $recl_blob |"
  echo "| **Blob-store growth** | **$blob_growth** |"
  echo
  echo "- Metadata ratio: **$meta_pct%** (20% rule met: $meta_pass)."
  echo "- Combined ratio: **$combined_pct%** (counts only if gc --full can be scheduled: $combined_pass)."
  echo "- gc --full: one mark $mark_secs s, so about $full_secs s with the 15m fence;"
  echo "  backup fence $bf_secs s; run every $interval_secs s; tombstones kept $tombstone_secs s."
  echo "  Schedulable: $schedulable (full run shorter than the interval, and backup fence"
  echo "  within the tombstone retention)."
} >"$summary"
cat "$summary"
echo
echo "raw reports in $out/"
