#!/usr/bin/env bash
# Fails if the alert rules in docs/OPERATIONS.md ("Suggested alerts") and the
# runbooks in docs/RUNBOOKS.md ("## <AlertName>") do not match one to one, if a
# rule's runbook_url does not point at its own section, or if a link into
# RUNBOOKS.md (or out of it, into the other docs) names a heading that does not
# exist. When python3 with PyYAML is available it also checks that the rules
# block parses and every rule has a severity, a summary and a runbook_url.
set -euo pipefail

cd "$(dirname "$0")/.."

OPS=docs/OPERATIONS.md
RB=docs/RUNBOOKS.md
status=0

fail() {
  echo "error: $*" >&2
  status=1
}

[ -f "$OPS" ] || { echo "error: $OPS not found" >&2; exit 2; }
[ -f "$RB" ] || { echo "error: $RB not found" >&2; exit 2; }

# The first ```yaml block after the "Suggested alerts" heading.
rules=$(awk '/^### Suggested alerts/{s=1} s&&/^```yaml/{y=1; next} y&&/^```/{exit} y{print}' "$OPS")
if [ -z "$rules" ]; then
  echo "error: no yaml block under '### Suggested alerts' in $OPS" >&2
  exit 2
fi

alerts=$(printf '%s\n' "$rules" | sed -nE 's/^[[:space:]]*-[[:space:]]+alert:[[:space:]]*([A-Za-z0-9_]+).*/\1/p')
sections=$(sed -nE 's/^## (Leep[A-Za-z0-9]*)[[:space:]]*$/\1/p' "$RB")
urls=$(printf '%s\n' "$rules" | grep -E '^[[:space:]]*runbook_url:' | grep -oE 'RUNBOOKS\.md#[A-Za-z0-9_-]+' | sed 's/^RUNBOOKS\.md#//' || true)

dups() { sort | uniq -d; }

for d in $(printf '%s\n' "$alerts" | dups); do fail "alert $d is defined twice in $OPS"; done
for d in $(printf '%s\n' "$sections" | dups); do fail "$RB has two '## $d' sections"; done

while read -r name; do
  [ -n "$name" ] || continue
  printf '%s\n' "$sections" | grep -qx "$name" ||
    fail "alert $name in $OPS has no '## $name' section in $RB"
  lower=$(printf '%s' "$name" | tr 'A-Z' 'a-z')
  printf '%s\n' "$urls" | grep -qx "$lower" ||
    fail "alert $name has no runbook_url ending in RUNBOOKS.md#$lower"
done <<<"$alerts"

while read -r name; do
  [ -n "$name" ] || continue
  printf '%s\n' "$alerts" | grep -qx "$name" ||
    fail "$RB has a '## $name' section but $OPS has no such alert"
done <<<"$sections"

alert_slugs=$(printf '%s\n' "$alerts" | tr 'A-Z' 'a-z')
while read -r u; do
  [ -n "$u" ] || continue
  printf '%s\n' "$alert_slugs" | grep -qx "$u" ||
    fail "runbook_url anchor #$u in $OPS matches no alert"
done <<<"$urls"

# GitHub's heading anchors: lower case, punctuation dropped, spaces to hyphens.
slugs() {
  sed -nE 's/^#{1,6}[[:space:]]+(.*[^[:space:]])[[:space:]]*$/\1/p' "$1" |
    tr 'A-Z' 'a-z' | sed -E 's/[^a-z0-9 _-]//g; s/ /-/g'
}

# Links into RUNBOOKS.md from anywhere, and from RUNBOOKS.md into other docs.
check_link() { # file target-file anchor
  slugs "$2" | grep -qx "$3" || fail "$1 links to $2#$3, which is not a heading there"
}
rb_slugs=$(slugs "$RB")
while IFS= read -r f; do
  while read -r a; do
    [ -n "$a" ] || continue
    printf '%s\n' "$rb_slugs" | grep -qx "$a" || fail "$f links to RUNBOOKS.md#$a, which is not a heading there"
  done < <(grep -oE 'RUNBOOKS\.md#[A-Za-z0-9_-]+' "$f" | sed 's/^RUNBOOKS\.md#//' | sort -u)
done < <(ls README.md CONTRIBUTING.md docs/*.md 2>/dev/null | grep -v "^$RB$")
while read -r a; do
  [ -n "$a" ] || continue
  printf '%s\n' "$rb_slugs" | grep -qx "$a" || fail "$RB links to #$a, which is not a heading in $RB"
done < <(grep -oE '\]\(#[A-Za-z0-9_-]+\)' "$RB" | sed -E 's/^\]\(#//; s/\)$//' | sort -u)
while read -r doc a; do
  [ -n "$a" ] || continue
  [ -f "docs/$doc" ] || { fail "$RB links to docs/$doc, which does not exist"; continue; }
  check_link "$RB" "docs/$doc" "$a"
done < <(grep -oE '\]\([A-Z]+\.md#[A-Za-z0-9_-]+\)' "$RB" | sed -E 's/^\]\(//; s/\)$//; s/#/ /' | sort -u)

if python3 -c 'import yaml' 2>/dev/null; then
  printf '%s\n' "$rules" | python3 -I -c '
import sys, yaml
try:
    doc = yaml.safe_load(sys.stdin)
except yaml.YAMLError as e:
    print(f"error: the rules block in docs/OPERATIONS.md is not valid YAML: {e}", file=sys.stderr)
    sys.exit(1)
bad = []
seen = set()
rules = [r for g in (doc or {}).get("groups", []) for r in g.get("rules", [])]
if not rules:
    bad.append("no groups/rules found")
for r in rules:
    name = r.get("alert", "?")
    if name in seen:
        bad.append(f"{name}: defined twice")
    seen.add(name)
    if not str(r.get("expr", "")).strip():
        bad.append(f"{name}: no expr")
    sev = (r.get("labels") or {}).get("severity")
    if sev not in ("page", "ticket"):
        bad.append(f"{name}: labels.severity must be page or ticket, not {sev!r}")
    ann = r.get("annotations") or {}
    if not ann.get("summary"):
        bad.append(f"{name}: no annotations.summary")
    url = str(ann.get("runbook_url", ""))
    if not url.endswith("RUNBOOKS.md#" + name.lower()):
        bad.append(f"{name}: runbook_url must end in RUNBOOKS.md#{name.lower()}")
for b in bad:
    print(f"error: {b}", file=sys.stderr)
sys.exit(1 if bad else 0)
' || status=1
  yaml_note="rules block parsed"
else
  yaml_note="rules block not parsed (python3 with PyYAML not available)"
fi

if [ "$status" -eq 0 ]; then
  echo "ok: $(printf '%s\n' "$alerts" | grep -c .) alerts, each with a runbook, runbook_url and valid links; $yaml_note"
fi
exit "$status"
