# 0088. Changelog entries are fragments in changelog.d/
Status: Accepted
Date: 2026-11-04

## Context
Every change a user or operator would notice adds an entry under
`[Unreleased]` in `CHANGELOG.md`, and every such entry goes at the top of
that section. Two changes in flight therefore edit the same lines, and the
second to land conflicts there. In a session of five agents landing on one
protected `main`, three of four reviewed lands conflicted in
`CHANGELOG.md` and nowhere else. Each conflict cost a local resolution, a
new changeset and (before ADR 0085 and 0087) a second review. Resolving
also went wrong: both sides were sibling bullets, and a "keep both" put
one agent's bullet under another agent's heading.

Three fixes were considered:

- **A union merge for `CHANGELOG.md`** (keep both sides of every hunk)
  would make the server resolve conflicts silently, which CLAUDE.md
  forbids, and it still interleaves bullets under the wrong heading when
  the hunks touch.
- **One heading per entry** keeps bullets with their heading but still
  inserts at the same place, so it still conflicts.
- **One file per entry** never edits a line another change edits.

## Decision
A change adds `changelog.d/<slug>.md` instead of editing `CHANGELOG.md`.
The slug is the review ref without `review/` (`review/cli-review-wait`
becomes `changelog.d/cli-review-wait.md`), so it is unique among changes
in flight. A fragment is one `### Title` line followed by the entry
(bullets led by `**Added.**`, `**Fixed.**` and so on), with no other
heading of level 1 to 3. `changelog.d/README.md` says how to write one.

`scripts/changelog-assemble.sh` moves the fragments under `[Unreleased]`
in `CHANGELOG.md`, in file-name order and before the entries already
there, and deletes them; `--preview` prints the result without changing
anything, and `--check` only checks the fragments. The maintainer runs it
at a release, or whenever the entries should be read in one place.
Entries already in `CHANGELOG.md` stay where they are.

A malformed fragment fails `scripts/changelog-assemble.sh --check` and a
test in `cargo test --workspace` (in `leep-cli`'s tests, the crate
contributors already build; it reimplements the rules in Rust so Windows
CI checks them too). Fragments match the docs glob `**/*.md`, so a
fragment alone is a docs-only change under a review policy that
auto-approves those.

## Consequences
- Concurrent changes no longer conflict over the changelog; a land that
  conflicts now conflicts over code.
- `CHANGELOG.md` lags until the fragments are assembled: readers between
  releases read `changelog.d/` too. The assembled order is by file name,
  not by landing time; Keep a Changelog groups by release, not by date,
  so nothing depends on it.
- Contributors and agents follow the commit-and-pr skill, CLAUDE.md and
  CONTRIBUTING.md, which now say "add a fragment" instead of "edit
  `CHANGELOG.md`".
- A second fragment from the same change is allowed but discouraged:
  answering review edits the one fragment.
