# 0085. Review verdicts carry across identical rebases; policy refusals are 422
Status: Accepted
Date: 2026-11-03

## Context
ADR 0011 ("Exact ids") ties an approval to one changeset id. A clean server
replay keeps it, because the land policy judges the originals the client
submitted, not the server's copies. A land that conflicts does not: the
client resolves the conflict locally and commits a new changeset on the new
head, with a new id and no review. Agents working through Leep hit this on
nearly every land, because everyone appends to `CHANGELOG.md`: the reviewer
had approved the code, the conflict was only in the changelog (a docs path
the policy approves by itself), and the reviewer still had to diff old
against new by hand to prove the code was byte-identical and approve again.
Code-review tools solve the same problem by dismissing a stale approval only
when the reviewed content changed.

Separately, a land refused by a policy (review, signatures, forward-only)
answered `400` with the text `invalid input: changeset X needs review: ...`.
Nothing was wrong with the request; the target ref's rules were not met yet.
Agents read the `invalid input` framing as a bug in what they sent.

## Decision
**Carry by content, never by claim.** When the review policy judges a
changeset Y (in `land` and in `review-status`), a reviewer's verdict on
another changeset X counts for Y when all of these hold:

1. **X is a candidate.** Y's `leep.rebased_from` extra names X (one id or a
   comma-separated list, newest first, as the CLI writes the first-parent
   chain of a resolved stack), or the server's `rebased-origin-v1` row of a
   changeset so named does, followed transitively (a resolve of a resolve
   names the earlier resolve), nearest first, at most 64 candidates
   (`MAX_CARRY_CANDIDATES`, the CLI's own limit). The extra is written by clients and is
   a hint only: it chooses which changesets to compare and grants nothing. A
   changeset without the extra has no candidates; the server's own rebased
   copies always carry it.
2. **Y reproduces every reviewed change of X.** X is not a merge. Every path
   X changed is either among Y's changes with the identical triple (path,
   old content and kind, new content and kind), where "old" is the path in
   each changeset's **own first parent**, or one an auto rule approves in X.
   `OWNERS` files are never auto-approved. Same old and same new
   whole-content hashes mean the file is byte for byte what was reviewed and
   the base did not move under it. If any other path differs or is missing
   from Y, nothing carries from X: a reviewed path never carries without the
   others it was approved with. Verdicts count on the reproduced paths.
3. **The reviewer has no review of Y itself.** A review of Y, a withdrawal
   included, is that reviewer's word on Y and replaces anything carried.

Each candidate is checked on its own. Where several candidates reproduce a
path, each reviewer's **newest** review among them decides (by timestamp; in
the same second reject beats withdrawal beats approval), so the order a
client names candidates in never matters; a newest withdrawal carries
nothing. So a resolved stack keeps the
approval of every entry whose reviewed changes it kept exactly. An entry
whose reviewed file a lower entry also changed does not reproduce (its old
content is the lower entry's new one) and needs review again. Paths Y changes that
X did not stay pending unless their own reviews or an auto rule satisfy them;
auto rules are judged on Y's own bytes. Owners still come from the target
ref's head. Rejects carry exactly as approvals do, so a veto is not escaped by
re-committing the same change.

**Taking back.** A reject or withdrawal posted on Y is also written, as the
same reviewer's verdict with a `review-source-v1` row
(`leep:taken-back-on:<Y>`, `via` the posting token), on each candidate of Y
where that reviewer's approval would carry to Y. Otherwise a later copy that
names only X, not Y, would carry the approval the reviewer just took back on
an identical change: candidates are found from hints, never by searching for
copies. This changes X's own status too, which is the point: its reviewed
change is the one rejected. The verdict being posted is the reviewer's newest
word whatever the clocks say: an approval on X stamped later than it (servers
with skewed clocks, a clock stepped back) is taken back all the same, and the
row written on X is stamped one second past that approval when the approval
is not older, so the judge never ranks the approval above it. Taking back is
not capped by `MAX_CARRY_CHECKS`: every approval it skipped would stay live.
Its work is bounded by the 64-candidate walk, and it runs only on an
authenticated review post.

Limits of taking back, accepted:

- It follows Y's hints only. A reject on a copy with no hint, or on an
  identical copy that was approved independently and names nothing, leaves
  X approved; a later copy naming X still carries that approval.
- An author can make Y as X's change plus an extra file and name X. A
  reviewer who rejects Y only for the extra file also rejects X. The
  `leep:taken-back-on:<Y>` source row on X shows where the verdict came
  from, and the reviewer can approve X again.
- The rows are written one candidate after another. A storage failure part
  way answers 500 with some candidates written and others not; posting the
  same verdict again writes the rest.

**Bounds.** At most 64 candidates are walked (one changeset read each, and
one `rebased-origin-v1` read for each that has a hint); reviews are read
once per candidate per status request or land check; at most 16 candidates
with reviews by others are compared (`MAX_CARRY_CHECKS`), and with a 17th
nothing carries at all, since stopping early could drop a newer reject. This
holds for a resolved stack too: one with more than 16 entries reviewed by
others carries nothing, by design, and is reviewed again. A
comparison needs no judgement of X: a path whose new content differs is
refused before any read unless an auto rule could hold for it, each matching
path costs two file lookups, and only differing paths read contents for the
content rules; a candidate with more than 1,024 paths is not compared. A
status request on a changeset naming 64 approved, non-reproducing
candidates makes 174 metadata calls with four principals
(`too_many_reviewed_candidates_carry_nothing_and_the_work_is_bounded`).

`PathReview` in `GET .../review-status` gains `carried_from` (the X whose
verdict counted on that path; omitted otherwise, `#[serde(default)]`). The
`rule` stays `owner` or `rejected`. Nothing persisted changes: no new kinds
or fields (taking back writes ordinary `review-v1` and `review-source-v1`
rows). A client that resolves a land's conflicts names
the original by stamping `leep.rebased_from` on the resolving commit (the
CLI's side is a separate change); any client may.

**Policy refusals answer 422.** `leep_core::Error` gains `Policy(String)`:
a well-formed request the target ref's rules refuse. Its text has no
`invalid input:` prefix, and the server answers it with `422 Unprocessable
Entity` and the same stable `code` as before, picked by the same marker:
`needs_review` (review policy), `not_signed` (signature policy) and
`not_forward` (a non-fast-forward move of a policy-covered ref, and
`rebase-onto` of a `--protect-ref` ref). Other refusals keep their variant
and status (a tag move is still `400 tag_immutable`; deleting a protected ref
is still `400 invalid`).

## Consequences
- An agent that resolves a changelog-only conflict lands with the approval it
  already had; a reviewer re-reviews only what changed.
- **Security.** The claim in `leep.rebased_from` gains nothing by itself: a
  forged hint naming an approved X with different content, a reviewed path
  whose base moved (old differs), and a Y that drops a reviewed path all stay
  pending. Tests: `a_forged_hint_to_an_approved_changeset_gains_nothing`,
  `a_base_that_moved_under_a_reviewed_path_stays_pending`,
  `dropping_a_reviewed_path_carries_nothing`,
  `one_changed_byte_in_an_owned_file_stays_pending`.
- **What is still trusted.** A carried approval says the reviewed files are
  identical, not that the program is: a moved base on *other* files can change
  what the reviewed code does. That is the trust ADR 0011 already grants to a
  clean server replay, which keeps the approval of the submitted original even
  when the replay three-way merged an owned file the original changed; this
  ADR is strictly narrower than that (it requires the reviewed files to be
  untouched by the base). Teams that want every landed tree reviewed need a
  different policy (review of the final tree), which neither ADR provides.
- A server rebased copy now reports the original's verdicts in
  `review-status` (with `carried_from`) when it reproduces the reviewed
  changes, instead of showing them only in `GET .../reviews` with `via`.
  Land was unaffected: it judges the originals.
- Cost: a changeset without `leep.rebased_from` costs nothing more; one with
  it is bounded as above.
- **422 compatibility.** The stable `code` and the marker texts are
  unchanged, so clients that branch on them (the CLI's land hint, the MCP
  server, the soak agent) keep working. ADR 0034 maps "any other 4xx" to
  `user` and exit 1, so an older CLI still exits 1 with its hint, and the git
  mirror, which counts a land or ref move refused on `ErrorKind::User`, still
  counts it refused. A client that switched on `400` alone sees `422` and must
  go by `code`. Every `422` the server sends is a policy refusal; bad input,
  including a JSON body of the wrong shape, stays `400 invalid`. The error
  text loses its `invalid input: ` prefix.
- ADR 0011's "Exact ids" holds for the reviews stored (a review row is still
  of one id); what changed is which rows the policy reads.

## Alternatives considered
- **Trust the extra, or only the server's `rebased-origin-v1` row.** The
  conflict path produces a client commit, which the server never recorded,
  and a client can name anything; the content check makes the source of the
  claim irrelevant.
- **Carry per path, even when another reviewed path changed.** A reviewer
  approves a change as a whole; a fix in one file is often only right with the
  check in another.
- **Compare diffs (hunks) instead of whole contents.** A hunk-identical change
  on a moved base can mean something else; whole old and new contents are
  exact and need no diff.
- **403 or 409 for policy refusals.** Old CLIs map 403 to `auth` and 409 to a
  retryable conflict (exit 5), and the mirror would stop counting the land as
  refused; 422 falls into "other 4xx" everywhere.

*Amended by [ADR 0087](0087-review-requests-close-by-content.md):*
`carried_paths` returns an empty set (not `None`) when every path of X is
auto-approved there and none is reproduced; carrying and taking back skip
it, and review requests use it to close. `GET .../reviews` lists carried
verdicts with `carried_from`, and `via` only where none carry. A review
post answers `taken_back` and `on_refs`.

*Amended by [ADR 0089](0089-review-verdicts-carry-across-client-rebases.md):*
a reviewed path whose base moved is also reproduced when the judged
changeset's file is exactly the server's own rebase of the reviewed change
onto its base (`rebased`), or keeps both sides' insertions where the merge
conflicts only between pure insertions at one place (`union`); text only,
at most 2 MiB and 10,000 lines a side, never `OWNERS`, at most 32 merged paths per
candidate, and not on strict land refs. The "compare diffs (hunks)"
alternative stays rejected. `PathReview` gains `carried_rule`, and
`review-status` gains `origins`.
