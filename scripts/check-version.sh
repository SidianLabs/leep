#!/usr/bin/env bash
# Fails if any workspace crate's version reaches 0.1.0. Versions move only
# when the maintainer decides (docs/ROADMAP.md, "How versions move"); to
# allow a new range, change CEILING here in the same PR as the bump.
set -euo pipefail

CEILING="0.1.0"

below() {
  # True if $1 < $2, comparing major.minor.patch numerically.
  local IFS=.
  local -a a=($1) b=($2)
  for i in 0 1 2; do
    ((10#${a[i]%%[-+]*} < 10#${b[i]%%[-+]*})) && return 0
    ((10#${a[i]%%[-+]*} > 10#${b[i]%%[-+]*})) && return 1
  done
  return 1
}

status=0
while read -r name version; do
  if ! below "$version" "$CEILING"; then
    echo "error: $name is at $version; versions must stay below $CEILING (see docs/ROADMAP.md)" >&2
    status=1
  fi
done < <(cargo metadata --no-deps --format-version 1 |
  jq -r '.packages[] | "\(.name) \(.version)"')

if [ "$status" -eq 0 ]; then
  echo "ok: every workspace crate is below $CEILING"
fi
exit "$status"
