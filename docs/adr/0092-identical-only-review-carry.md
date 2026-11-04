# 0092. Paths whose review verdicts carry only to identical changes
Status: Accepted
Date: 2026-11-04

Amends [0011](0011-review-service.md),
[0085](0085-review-verdicts-carry-across-identical-rebases.md) and
[0089](0089-review-verdicts-carry-across-client-rebases.md).

## Context
ADR 0089 lets a reviewer's verdict on X carry to a client's conflict
resolution Y when Y's file is the server's own rebase of X's change onto
Y's base, or a union of two pure insertions. That is the clean-replay trust
ADR 0011 already gives the server, and for most code it is what reviewers
want. For some files it is not. A migration, a lockfile, a wire or hashed
format, a security check: there an owner wants to have seen the exact bytes
that land, because a change elsewhere in the file can change what the
reviewed lines mean, and a re-approval is cheap next to a wrong one.

Until now the only switch was per ref: on a strict land ref (ADR 0066) only
identical changes carry. That is too coarse: a team that wants merged
carries for `src/` and identical-only carries for `migrations/` has no way
to say so.

## Decision

### 1. Where a path is declared identical-only
- **`OWNERS` lines.** A line that is exactly `carry identical` or
  `carry merged` (after trimming and dropping a `#` comment, as names are)
  is a directive, not an owner name. It applies to the directory of the
  file and everything below it. The directive for a path is the
  **strictest on the walk** owners use (deepest directory first, up to the
  first `noparent`, that file included, or the root): `carry identical` in
  any file on the walk wins, and the deepest file saying it is the one
  named; otherwise the nearest `carry merged`. A subdirectory can tighten
  its ancestors' rule, never loosen it: its owners need not own the parent
  directory, so they cannot undo what the parent's owners asked for; only
  a `noparent` file, which already cuts the parent's owners off, ends its
  reach. A file with both lines means `carry identical`.
- **Misspellings are refused, not ignored.** Any other line whose first
  word is `carry` (`carry identically`, `carry`, `carry  merged`) is
  neither a name nor a directive. Read as a name, a typo would leave the
  directory on the looser default with nothing to show for it. A land onto
  a review-protected ref of a changeset that adds or modifies such an
  `OWNERS` file is refused with 422, `changeset X has an OWNERS line that
  is no directive: src/OWNERS: "carry identically" (write ...)`, approved
  or not, and `review-status` answers the same text in a new optional
  `refused`, with `approved` false. In a head that already holds such a
  line (written before this server, or on an unprotected ref) the line
  is simply ignored.
  Directory level only; per-file patterns inside `OWNERS` are not offered.
- **Review policy file.** `identical_only_globs`: `globset` patterns,
  matched as `docs_globs` are (`*` stays within one path level), for files
  that have no `OWNERS` tree or that the operator wants fixed whatever the
  tree says (`["**/Cargo.lock", "migrations/**"]`). It needs no auto rule.
- **Strict refs** (ADR 0066, 0089), unchanged.

**The effective rule** for a path on a target ref is identical-only when
the ref is strict, or a policy glob matches, or an `OWNERS` file on the
walk says `carry identical`; otherwise rebased and union carries are
allowed as in ADR 0089. Each source can only make a path stricter: a
`carry merged` line cannot undo a policy glob, a strict ref or a
`carry identical` further up. `OWNERS` files themselves never merge (ADR
0089).

**Read from the target ref's head**, like owners (ADR 0011): a change that
adds `carry merged` is judged under the head's `carry identical`, so it
cannot loosen the rule for its own land or for a resolution stacked on it.
It needs the directory owners' approval like any `OWNERS` edit, and the
looser rule applies once it is on the head.

### 2. One scope for carrying and for closing requests
`leep_derive::review::CarryScope` holds the ref's strictness, the policy
globs and an `OwnersResolver` on the ref's head; `Repo::carry_check`,
`carried_paths` and `lands_reviewed_change` take it in place of ADR 0089's
`merges: bool`. A land's review check, `review-status?ref=`, the request
closing after a land and at listing all build it for the target ref, so a
review request for X closes by content exactly when X's verdict would carry
to the changeset that landed (ADR 0087's symmetry). When the head's tree
cannot be read while closing requests, the scope is identical-only, the
strictest any file could ask for. `GET .../reviews` names no ref, so it
applies the policy globs and the default ref rule but no `OWNERS` lines;
the land and `review-status?ref=` decide, as before.

**Taking back is unchanged.** A reject or withdrawal on Y is written on
every candidate whose approval would carry to Y on *some* ref, merges
included, whatever any ref or file declares (`CarryScope::widest`). Taking
back more than any ref would carry is the safe side (ADR 0085, 0089), so a
reject on a resolution of an identical-only path still reaches the original.

### 3. Saying why
A path that would have carried as rebased or union but is identical-only
refuses with `CarryRefusal::IdenticalOnly { path, declared_by }`, whose
text names the source: `src/a.rs is identical-only (src/OWNERS)`,
`(policy glob migrations/**)` or `(strict land ref)`. On the wire it fills
the existing `OriginStatus.differs` (the path) and `reason` (the text), and
`leep review status` prints the reason:
`replaces X, approved by rhea; not carried: src/a.rs is identical-only
(src/OWNERS) (leep diff X; leep diff Y)`. On a strict ref this replaces the
bare `P differs` that ADR 0089 printed for a path a merge would have
reproduced. `PathReview` gains optional `carry`, `identical` or `merged`,
the effective rule of each path on the asked ref, so a reviewer can see
where only an identical change keeps their verdict. Land refusals are
unchanged (they name the pending path).

### 4. What an identical-only path costs: concurrent edits
On a strict ref or an identical-only path, any concurrent edit of the file
that lands first moves the file's base under a pending client resolution,
and then nothing but an identical change carries, so the resolution needs a
fresh approval. That includes an edit no human approved: a `comment_only`,
`whitespace_only` or `docs_only` change approved by an auto rule. This is
by design. The owner asked to see the bytes that land, and the bytes moved;
the auto rule judged the editor's own change, not the pending resolution on
top of it. The cost to the owner is bounded by how often the file can be
edited, which is the editor's land rate (request limits and land admission,
ADR 0020, 0060): an agent landing comment-only edits in a loop makes owners
re-approve at most that often, and the file's history shows who moved
the base.

A future `noauto` `OWNERS` line could switch off the content auto rules for
a directory, so that every edit there needs an owner and the forced
re-approvals come only from changes an owner already saw. It is not added
here: it changes who may land, not how verdicts carry, and the policy file
already chooses which auto rules run.

## Consequences
- Owners can keep ADR 0089's convenience for most code and ask for exact
  bytes where it matters, per directory, and operators can fix it per glob.
- **Security (S-24).** The rule is monotone and read from the target's
  head, so a change cannot loosen it for itself. Tests: in
  `crates/leep-server/tests/reviews.rs`,
  `an_identical_only_path_keeps_a_rebased_resolution_pending_and_says_why`
  (and that a reject still takes back the original),
  `an_identical_only_path_refuses_a_union_but_carries_an_identical_change`,
  `a_policy_glob_outranks_carry_merged`,
  `a_change_adding_carry_merged_is_judged_under_the_heads_identical`,
  `a_strict_ref_carries_only_identical_changes` (now with the reason text),
  `a_misspelled_carry_line_is_refused_and_status_says_why`;
  in `leep-derive`, `owners::tests::carry_directives_are_whole_lines_and_identical_wins`,
  `the_strictest_carry_line_decides_and_noparent_stops_the_walk`,
  `a_subdirectory_cannot_loosen_an_ancestors_carry_identical`,
  `identical_only_is_strict_ref_or_glob_or_owners_and_only_stricter`,
  `an_identical_only_path_carries_identical_changes_only`,
  `review_requests::tests::a_request_closes_exactly_when_its_verdict_carries_under_the_carry_line`.
  ADR 0085's, 0087's and 0089's security tests still pass.
- **Compatibility.** API: `PathReview.carry` and
  `ReviewStatusResponse.refused`, `#[serde(default)]`, omitted by older
  servers. A line starting with the word `carry` is no longer an owner
  name. Nothing persisted changes. The `OWNERS` text
  gains two directive lines: a token named exactly `carry identical` or
  `carry merged` can no longer be listed as an owner. An older server reads
  the lines as an owner name nobody has: harmless beside real names (they
  still approve), but in an `OWNERS` file with no other names it makes the
  directory owned by nobody who can approve, so upgrade every server before
  adding such a file. The policy key is refused by an older server's strict
  parser (startup fails; a reload keeps the old config, ADR 0024), so
  upgrade servers before adding it.
- **Cost.** One more lookup per merged path: a glob match and the same
  per-directory `OWNERS` cache the owners walk already fills. Identical
  paths never ask.

## Alternatives considered
- **`OWNERS` only, or the policy file only.** `OWNERS` lets the owners of a
  directory decide and travels with the tree; the policy covers files with
  no `OWNERS` tree and lets an operator set a floor the tree cannot lower.
  Both, with a monotone combination, gives one answer whichever is read
  first.
- **Read the directive from the changeset being judged.** It would let a
  change loosen its own rule; owners are never read that way (ADR 0011).
- **Apply it to taking back.** Taking back less than a merging ref would
  carry could leave an approval live after its copy was rejected.
- **Per-file patterns in `OWNERS`.** More syntax in a format that older
  servers misread; revisit if the policy globs grow.
- **`noauto` now.** Above; recorded as a future option.
