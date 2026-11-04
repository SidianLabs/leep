#!/usr/bin/env bash
# Moves changelog fragments (changelog.d/<slug>.md) into the [Unreleased]
# section of CHANGELOG.md and deletes them (ADR 0088). Contributors add a
# fragment instead of editing CHANGELOG.md, so two changes never conflict
# over the same lines; the maintainer runs this at a release or whenever the
# fragments should be read in one place.
#
#   scripts/changelog-assemble.sh            assemble and delete the fragments
#   scripts/changelog-assemble.sh --preview  print the new [Unreleased] section, change nothing
#   scripts/changelog-assemble.sh --check    only check that every fragment is well formed
#
# A fragment is one `### Title` line, then its entry (bullets such as
# `- **Fixed.** ...`), with no other heading of level 1 to 3. Fragments go
# in file-name order, before the entries already under [Unreleased].
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
dir="$root/changelog.d"
changelog="$root/CHANGELOG.md"
mode="${1:-assemble}"
case "$mode" in
  assemble | --preview | --check) ;;
  *)
    echo "usage: $0 [--preview | --check]" >&2
    exit 2
    ;;
esac

fragments=()
if [ -d "$dir" ]; then
  while IFS= read -r -d '' f; do
    fragments+=("$f")
  done < <(find "$dir" -maxdepth 1 -type f -name '*.md' ! -name README.md -print0 | LC_ALL=C sort -z)
fi

# Prints why a fragment is malformed, if it is.
problems() {
  local f="$1" name
  name="$(basename "$f" .md)"
  if ! [[ "$name" =~ ^[a-z0-9][a-z0-9.-]*$ ]]; then
    echo "name must be lowercase letters, digits, '-' and '.' (use the review ref's <area>-<desc>)"
  fi
  if [ ! -s "$f" ]; then
    echo "is empty"
    return
  fi
  if [ -n "$(tail -c 1 "$f")" ]; then
    echo "does not end with a newline"
  fi
  if ! head -n 1 "$f" | grep -Eq '^### [^ ]'; then
    echo "first line must be '### Title'"
  fi
  if [ "$(grep -Ec '^#{1,3}( |$)' "$f")" -ne 1 ]; then
    echo "must have exactly one heading of level 1 to 3 (its '### Title'); use #### below it"
  fi
  if ! tail -n +2 "$f" | grep -q '[^[:space:]]'; then
    echo "has a title but no entry"
  fi
  if grep -Eq '^(<<<<<<<|\|\|\|\|\|\|\||>>>>>>>)( |$)' "$f"; then
    echo "has conflict markers"
  fi
  return 0
}

bad=0
for f in ${fragments[@]+"${fragments[@]}"}; do
  while IFS= read -r p; do
    [ -n "$p" ] || continue
    echo "error: ${f#"$root"/}: $p" >&2
    bad=1
  done < <(problems "$f")
done
if [ "$bad" -ne 0 ]; then
  exit 1
fi

if [ "$mode" = --check ]; then
  echo "ok: ${#fragments[@]} changelog fragment(s) well formed"
  exit 0
fi

if [ "${#fragments[@]}" -eq 0 ]; then
  echo "no changelog fragments in changelog.d/" >&2
  exit 0
fi

if ! grep -qx '## \[Unreleased\]' "$changelog"; then
  echo "error: CHANGELOG.md has no '## [Unreleased]' line" >&2
  exit 1
fi

# The fragments, each followed by one blank line.
block="$(mktemp)"
trap 'rm -f "$block" "$block.new"' EXIT
for f in "${fragments[@]}"; do
  # Drop trailing blank lines so the spacing is exactly one.
  sed -e :a -e '/^\n*$/{$d;N;ba' -e '}' "$f" >>"$block"
  echo >>"$block"
done

if [ "$mode" = --preview ]; then
  echo "## [Unreleased]"
  echo
  cat "$block"
  # What is already there, up to the next version section.
  awk 'f && /^## /{exit} f{print} /^## \[Unreleased\]$/{f=1; if ((getline) <= 0 || /^## /) exit; if ($0 != "") print}' "$changelog"
  exit 0
fi

# Insert after "## [Unreleased]" and the blank line that follows it.
awk -v block="$block" '
  done == 0 && /^## \[Unreleased\]$/ {
    print
    if ((getline line) > 0 && line != "") { pending = line }
    print ""
    while ((getline b < block) > 0) print b
    if (pending != "") print pending
    done = 1
    next
  }
  { print }
' "$changelog" >"$block.new"
# Rewrite in place, keeping the file's mode.
cat "$block.new" >"$changelog"
rm -f "${fragments[@]}"
echo "assembled ${#fragments[@]} fragment(s) into CHANGELOG.md [Unreleased]"
