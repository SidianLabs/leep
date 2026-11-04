# 0089. Review verdicts carry across client rebases the server would have made
Status: Accepted
Date: 2026-11-04

Amends [0085](0085-review-verdicts-carry-across-identical-rebases.md) and
[0087](0087-review-requests-close-by-content.md).

## Context
A third session of agents on a protected `main` (ADR 0011), with changelog
fragments (ADR 0088) in place, still lost approvals. Two agents appended a
test at the end of the same test module in `crates/leep-core/src/api.rs`,
which `rhea` owns. Both were approved. The first landed; the second
conflicted, its author deleted the markers keeping both tests, and the
resolution needed a second request and a second approval. The reviewer
re-approved after comparing the `+`/`-` lines of the two diffs by hand.
ADR 0085 carries a verdict only when the reviewed file's old and new
contents are both identical, and here the old content had moved: the other
test was now in it.

The same session showed the server's own rebase carrying verdicts without
anyone noticing: a land whose head moved is replayed with a three-way merge
and keeps the original's approval, because the land policy judges the
originals the client submitted (ADR 0011, "Exact ids"). So the server
already trusts a three-way merge of a reviewed change onto a moved base,
when the server makes it, and not when a client makes the very same bytes.

Three smaller gaps came up beside it: nothing said why an approval did not
carry (the reviewer had to guess), a land the server rebased printed a new
id with no word on the approval, and a land refusal's hint said
`leep review request @` where it knew the changeset.

## Decision

### 1. Two more ways a path reproduces a reviewed change
For a path p that changeset X changed and needed a reviewer for (not
auto-approved in X), with X.old and X.new the file in X's first parent and
in X, and Y.old and Y.new likewise for the judged changeset Y, Y reproduces
X's change at p when:

- **identical** (ADR 0085): X.old = Y.old and X.new = Y.new, content and
  kind;
- **rebased**: X.old ≠ Y.old, and `diffy::merge(X.old, Y.old, X.new)` is
  clean and its result has Y.new's hash. This is the call, argument order
  and options of the server's own rebase (`Repo::resolve3` under
  `Repo::rebase_as`, base = X's parent, ours = the new base, theirs = X),
  so Y.new is byte for byte what the server would have stored as its copy
  of X on Y's parent, and that copy would have kept X's verdicts;
- **union**: that merge conflicts, every conflict region of its diff3
  output has an empty base part (both sides only inserted lines at one
  place), and Y.new is the marked text with both sides kept, Y.old's lines
  first in every region or X.new's lines first in every region. Both
  candidates are hashed and compared with Y.new; Y's bytes are never read.
  Any region that deletes or edits a base line refuses.

Rebased and union apply only when all four sides exist with one kind that
is not a symlink (a reviewed kind change carries only identically), each
blob is valid UTF-8 and at most 2 MiB (`MAX_AUTO_BYTES`, the content
rules' bound), each of the three merged texts has at most 10,000 lines
(`MAX_CARRY_LINES`, below), the path is not an `OWNERS` file, X and Y have different
first parents, and Y modifies the file. At most 32 paths of one candidate
are merged (`MAX_CARRY_MERGED_PATHS`); with more, nothing carries from it.
The rest of ADR 0085 is unchanged: every reviewed path of X must be
reproduced (by any of the three) or nothing carries from X; candidates come
from `leep.rebased_from` and the server's rows, at most 64; at most 16 with
reviews are compared; rejects carry exactly as approvals; a reviewer's own
review of Y wins; taking back writes a reject or withdrawal on Y onto each
candidate whose approval would carry to Y.

**Never on a strict ref.** On a ref whose lands never merge file contents
(`--land-conflicts strict` or `--strict-land-ref`, ADR 0066) the server
would not have made a merged copy, so `review-status` and lands there carry
identical changes only, and review requests targeting it close by identical
changes only. Taking back always considers merges, since taking back more
than any ref would carry is the safe side. `GET .../reviews` names no ref,
so it lists carried verdicts as the server's default land policy judges
them: on a server whose default merges but with a strict ref, it may list a
verdict as carried that a land on that strict ref does not count (the land
and `review-status?ref=` are what decide).

**Why this is safe.** The hint still only picks what to compare; Y.new is a
deterministic function of three stored blobs. X.old and X.new are what the
reviewer saw. Y.old is the file in Y's first parent, which a land of Y puts
on the ref only if it is already there or lands in Y's own stack and is
judged on its own. The rebased rule is therefore exactly ADR 0011's
clean-replay trust, no more: a forged hint carries nothing whose content
the server's own rebase would not have produced. Splitting a change,
reordering hunks or changing their context gains nothing either, because
Y's whole file is compared with a computed result.

The union rule is the one extension beyond the server, and it is narrow:
the author's only freedom is one order bit for all regions, over two
insertions each of which is already on the ref (Y.old's) or reviewed
(X.new's), with nothing between them deleted or edited. The reviewer of
that session did weaker by hand. CI still runs on the landed tree. Two
insertions at one place can still interact (two match arms, two `use`
lines shadowing); that is the same "other code moved under the reviewed
code" trust ADR 0085 and 0011 already accept for clean replays.

**Hunk comparison stays rejected.** Comparing Y's own diff hunks with X's,
as the reviewer did, is forgeable without context (a reviewed `unsafe`
block, `allow(...)` or early `return` moved into another function keeps the
same `+` lines) and fails the motivating case with context (one test sits
in the other's context lines). The tests include the moved-lines attack.

**Bounds.** diffy's Myers diff costs O((N+M)·D) with no cutoff, so a byte
bound alone is not one: 2 MiB of distinct one-character lines is about
10^12 steps per merge, and `review-status` is a GET a read token can repeat,
while a land check runs under the ref's land leader. So a merge is refused,
deterministically and before any diff, when any of its three texts has more
than 10,000 lines (`MAX_CARRY_LINES`), which bounds each of the merge's two
diffs near (2·10^4)² steps; a security review suggested 20,000, and half
that keeps the worst case four times lower while real source files stay far
below it. There is no timeout: an answer that depended on timing would make
carrying, and taking back, nondeterministic. Each merge runs once: on a
blocking thread, under a process-wide limit of half the cores (at least
one, the permit held by the thread itself), with concurrent asks for the
same three contents waiting for the first instead of merging again, and the
outcome, refusals included, is cached in process by the three content
hashes in two generations of 2,048 entries (an entry asked for again moves
to the newer one, so hot entries survive). The diff3 merge runs once per
outcome: its clean result is the server's (diffy's options differ only in
marker length, which shows only in conflicts), and its conflicted result is
parsed for the unions. A path whose file did not move or whose sides cannot
merge is refused before any blob read. Nothing new is persisted. Errors
reading a blob propagate as before (a swallowed one could hide a carried
reject) and cache nothing; a blob that is missing, too big or not UTF-8
simply does not carry. A candidate with reviews that is no longer stored
(collected by the changeset sweep, its review rows kept) carries nothing,
with the reason `it is no longer stored`, instead of failing the status
request or the land.

### 2. Saying why a verdict did not carry
`ReviewStatusResponse` gains `origins` (`#[serde(default)]`, omitted when
empty): for each candidate compared, nearest first, `{from, reviews:
[{reviewer, verdict}], carried, differs?, reason?}`. `reviews` are the
candidate's verdicts by reviewers without a review of Y; candidates with
none are not listed. `differs` names the reviewed path that Y does not
reproduce: the first one the check found, only, since the check stops
there (a list, so a later server may name more); `reason` covers the rest
in words (a merge, no longer stored, more than 1,024 paths, more than 32 merged paths, more than 16 reviewed
origins, or every path auto-approved in it). `PathReview` gains
`carried_rule` (`identical`, `rebased` or `union`; omitted unless
`carried_from` is set). `leep review status` prints, per origin,
`replaces X, approved by rhea; carried` or `replaces X, approved by rhea;
not carried: P differs (leep diff X; leep diff Y)`, and a carried path's
line ends `; carried from X (rebased onto the new base)`. No "approve
again?" prompt: `leep review approve Y` is one command, and the status line
is what to check first.

### 3. The CLI around it
- A land the server rebased prints `Landed Y on REF (rebased from X;
  reviews of the original count)`.
- A land that conflicts says that reviews of the replaced changesets carry
  only where the resolution is their change rebased onto the new head, and
  other paths need review again.
- A land refused for review names the refused changeset in its hint
  (`leep review request ID`, `leep review approve ID`), taken from the
  server's message, and falls back to `@`.
- `leep review inbox --wait[=SECS]` waits until something is listed (exit
  0) or the time runs out (exit 1), with `review status --wait`'s backoff.
- Inbox entries list the changeset's files (`files: a.rs, b.rs (+3)`) from
  `ReviewRequestEntry`'s new `paths` (the first 16, in path order) and
  `changed` (how many), both optional; and an entry open to any reviewer
  says `for any reviewer, no owner pending`.
- `leep commit` on a terminal prints one line, `committed ID (N files)`;
  piped, stdout stays the full id alone.

## Consequences
- The motivating resolution (both tests kept, either order) and any
  resolution equal to the server's rebase keep their approvals; the request
  closes when it lands (ADR 0087's content rule now uses the same check).
- **Security (S-24).** Tests in `crates/leep-server/tests/reviews.rs`:
  `an_approval_carries_to_a_resolution_equal_to_the_servers_rebase`,
  `an_approval_carries_to_a_union_of_two_appends_in_either_order`,
  `a_union_that_edits_a_base_line_stays_pending`,
  `a_resolution_with_one_extra_line_stays_pending`,
  `reviewed_lines_moved_to_another_function_stay_pending`,
  `a_rebased_kind_change_stays_pending`,
  `a_reject_takes_back_through_a_rebased_carry`,
  `too_many_merged_paths_carry_nothing`, `an_oversized_blob_is_not_merged`,
  `status_names_the_origin_that_did_not_carry_and_why`,
  `a_strict_ref_carries_only_identical_changes`; in `leep-derive`,
  `a_request_closes_when_its_change_lands_rebased_by_the_client` and the
  carry module's union tests (marker look-alikes in the texts included). All
  of ADR 0085's and 0087's security tests still pass.
- **Accepted.** The union rule's one order bit, above. A union or rebase
  can carry a verdict to code whose meaning the other side's insertion
  changes; that is the moved-base trust of ADR 0011. A request whose
  reviewed change lands as a union now closes; three existing derive tests
  that used a union-shaped changelog resolution to show the docs rule's
  effect now use a resolution that is neither.
- Cost: a candidate whose reviewed files moved costs up to 32 three-way
  merges of files up to 2 MiB and 10,000 lines, once per process per
  content triple, at most half the cores at a time.
- Tests for the bounds: `carry::tests::texts_over_the_line_budget_are_not_merged`,
  `concurrent_asks_for_one_merge_compute_it_once`,
  `the_cache_keeps_hot_entries_across_a_generation`,
  `a_clean_merge_equals_diffys_default_merge`, and the server's
  `a_reviewed_origin_that_is_gone_carries_nothing_and_fails_nothing`.
- API additions only: `PathReview.carried_rule`,
  `ReviewStatusResponse.origins` (`OriginStatus`, `OriginReview`),
  `ReviewRequestEntry.paths` and `changed`, all `#[serde(default)]` and
  omitted when empty. Nothing persisted changes.

## Alternatives considered
- **Compare hunks.** Forgeable, or useless for the motivating case; above.
- **Union only, without the rebase rule.** The rebase rule is the stronger
  argument (no trust beyond the server) and covers the commonest case, a
  base that moved elsewhere in the file.
- **Ask the reviewer to confirm the old approval in one step.** The status
  line already names the origin and the two diffs; approving is one
  command.
- **A cache keyed by (X, Y, rules).** The outcome depends only on three
  contents, so the cache is keyed by those and needs no invalidation.
