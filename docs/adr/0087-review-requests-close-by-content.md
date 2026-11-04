# 0087. Review requests close when their reviewed change lands, and show who answered
Status: Accepted
Date: 2026-11-04

Amends [0085](0085-review-verdicts-carry-across-identical-rebases.md) and
[0086](0086-review-requests-and-creators.md).

## Context
A session of five agents and one reviewer on a protected `main`
(ADR 0011) worked through review requests (ADR 0086) and found:

- **Requests stayed open after their change landed.** Three of four
  requests landed as a client's conflict resolution: the reviewed code was
  byte for byte the same, the conflict was in `CHANGELOG.md`, and the
  approval carried (ADR 0085). The requests still read `open` in every
  inbox. ADR 0086 closes a request only when its changeset lands, when a
  copy the server made of it lands (the `rebased-origin-v1` row), or when
  it is on the ref's first-parent chain. A client's resolving commit is
  none of these: it names the original only in `leep.rebased_from`, and
  ADR 0086 refused to trust that hint, rightly, because a client writes it.
  But ADR 0085 had already shown how to use the hint safely: it picks
  what to compare, and content decides.
- **The inbox gave no sign a request was answered.** A request's state is
  per request; verdicts are per (changeset, reviewer). Nothing joined
  them, so a reviewer who had approved still saw the request until it
  landed.
- **`review list` on a resolved changeset said "no reviews"**, while
  `review-status` said it was approved by a carried verdict. The listing
  only followed the server's rebase rows, and its `via` said the reviews
  "do not count for landing", which was no longer true for copies that
  reproduce the reviewed change.
- **A verdict on a landed changeset was silent.** A reviewer approved a
  changeset already on `main` and was told "now approved for main". A
  withdrawal of it also withdrew, through taking back (ADR 0085), the
  approval of the original it carried from, and the answer did not say so.

## Decision

### Two close rules, both unforgeable
After a land (the task of ADR 0086), a request R for changeset X on the
land's ref also closes as `landed`, with `landed_as` = Z, when a changeset Z
the land added names X among its carry candidates
(`Repo::carry_candidates`: `leep.rebased_from` and the server's
`rebased-origin-v1` rows, transitively, at most 64) and either:

1. **Content rule.** Z lands X's reviewed change
   (`Repo::lands_reviewed_change`): `carried_paths(X, Z, rules)` holds, so
   every path of X that needed a reviewer is among Z's changes with the
   same old and new content and kind, each relative to its own first
   parent, and every other path of X is one an auto rule approves in X; and
   either at least one path is reproduced, or (X is auto-only) Z changes
   every path X changed, each in a form an auto rule approves in Z. The hint
   chooses what to compare; content decides, exactly as for a carried
   verdict. A forger who lands X's byte-identical reviewed change has put
   the reviewed content on the ref, so closing is truthful.
2. **Requester rule.** The land's actor (its token) is R's requester. This
   covers a resolved stack entry whose reviewed file a lower entry also
   changed: its old content is the lower entry's new one, so the content
   rule refuses it (as carrying does), though the requester knows it
   landed. A requester may withdraw their own request at any time, so
   this grants nothing they did not have.

**What the land added.** Z ranges over the land's new changesets only: the
ref log entry of the move to the landed changeset gives the head it moved
from (one `ref_log` read of the ref's newest 64 entries), and the walk goes
down the first-parent chain from the landed changeset to that head, at most
256 steps. Without the entry (more than 64 moves of the ref since), only the
landed changeset and the server copies directly below it count. Walking
"while each changeset has a hint", as first planned, would not do: the
server's own copies carry the hint, so on a busy ref the walk would run
through earlier lands, cost up to 256 hint expansions per land, and apply
the requester rule to changesets other agents landed (bo's forged land
naming ada's X, then ada's next land on top, would close ada's request as
landed though her change never landed).

**At listing.** A listing (ADR 0086) also closes, for open requests on the
page, by the content rule and by the server-copy row: once per ref per
listing it walks the newest 64 first-parent steps of the ref's head,
expanding the carry candidates of each changeset with a hint until 256
candidates are collected, and then checks each remaining open request
against those, oldest changeset first (so `landed_as` is the one that first
put the change there). The requester rule does not apply at listing: there
is no actor.

**Auto-only changes.** `carried_paths` returned `None` when nothing was
reproduced. It now returns an empty set when every path of X is one an
auto rule approves in X and none is reproduced: nothing reviewed was lost.
Callers that carry verdicts (`ReviewService::carried` and `take_back`) skip
an empty set, so carrying and taking back are unchanged; closing uses it as
above.

**Rules.** The auto rules live in the review policy, which `Repo` does not
hold. `close_landed_review_requests`, `review_requests` and
`request_review` take `rules: Option<&ReviewRules>`; the server passes the
running policy's (`ReviewService::rules`), and `None` means no auto rule,
which closes less, never more.

Closing stays what ADR 0086 made it: after the land's answer, in a task with
a 10-second timeout whose errors are counted and logged, one conditional
update per request. A failed content check counts as "not closed". It adds
nothing to a land's latency, failure modes or crash behaviour.

### Who answered (additive)
`ReviewRequestEntry` gains `approved_by` and `rejected_by` (token names,
sorted; `#[serde(default)]`, omitted when empty): the reviewers whose own
current verdict on the requested changeset is an approval or a reject. The
server fills them in every answer that carries entries (list, get, create,
withdraw) with one batched `get_derived_many` per 512 (changeset, reviewer)
pairs of the page (`Repo::reviews_by`). Carried verdicts are not listed
there; `review-status` judges those.

### `GET .../reviews` shows carried verdicts
`ReviewEntry` gains `carried_from` (`#[serde(default)]`, omitted when
unset). The answer lists the changeset's own reviews, then the verdicts
that carry to it (ADR 0085's `carried`: reviewers without a review of their
own, from candidates that reproduce the reviewed change), each with
`carried_from` naming the changeset reviewed and that changeset's source
row. Only when there are neither does the old fallback apply: the server
rebase chain's reviews with `via`, which, now, are exactly the ones that do
not count, because a copy that reproduced its original would have listed
them as carried. So an older client sees carried verdicts as plain reviews
(they count) and still sees the `via` note only where it is true.

### Verdicts on landed changesets: warn, do not refuse
A verdict on a changeset already landed is stored as before: it still
counts for copies (a cherry-pick to a release ref) and for changesets that
name this one, and refusing would break that. The answer says so instead:

- `AddReviewResponse` gains `on_refs`, the refs a land policy covers
  (`LandPolicy::applies_to` over the ref list) whose first-parent chain
  holds the changeset, and `taken_back`, the candidates a reject or
  withdrawal was also written on (ADR 0085). Both `#[serde(default)]`,
  omitted when empty.
- `ReviewStatusResponse` gains `on_refs`: the asked ref and the policy refs
  whose first-parent chain holds the changeset. `approved` is unchanged;
  `on_refs` says it no longer decides a land there.

Override semantics are unchanged: a reviewer's newest word wins. The
"withdrawn copy looks unapproved" report is this: a withdrawal on a copy is
that reviewer's word on it and, by taking back, on the original; the answer
now names the original in `taken_back`, and the client can say so.

## Consequences
- A request whose reviewed change lands as a client resolution, as a server
  copy, as itself, or as the requester's own resolved stack closes, at the
  land or at the next listing. One whose reviewed change was altered before
  landing, landed by someone else, stays open until withdrawn.
- **Security (S-24).** A forged `leep.rebased_from` closes another agent's
  request only if the forger lands that request's reviewed change byte for
  byte, which is the truth. Tests:
  `a_forged_rebased_from_closes_nothing` (now also with every auto rule,
  for the forger's own land, for the requester's later land on top, and at
  listing), `a_resolution_that_changed_a_reviewed_byte_stays_open`,
  `another_agents_land_naming_it_does_not`.
- **Accepted.** An auto-only request (every path docs, comment-only or
  whitespace-only in X) closes when a changeset naming X changes the same
  paths in any auto-approved form, by anyone: such paths need no
  reviewer on the ref, so the forger could land them anyway, and
  `landed_as` names what landed. Without auto rules nothing is auto-only.
- Cost: a land with open requests on its ref makes one more ref log read,
  one changeset read per changeset it added and the carry-candidate walk of
  each with a hint (bounded at 64 candidates), and one content check per
  matching open request. A listing with open requests left after the
  first-parent check reads up to 64 changesets of the ref and 256
  candidates, once per ref. `review-status` makes two more metadata calls
  (the ref list and the ref) and a first-parent check per policy ref; the
  bounded-carry measurement is 176 calls, was 174. A review post makes the
  same. A list answer makes one batched review read per page.
- Nothing persisted changes; the API additions are optional fields that
  older clients ignore and older servers omit.

## Alternatives considered
- **Trust `leep.rebased_from` for closing.** One agent's land could close
  any other agent's request; ADR 0086 refused it for that reason.
- **Close by content only.** Leaves a requester's resolved stack open when a
  lower entry changed the same file; the requester rule fixes it at no cost
  in trust.
- **Refuse verdicts on landed changesets.** Reviews of landed changesets
  matter for cherry-picks to release refs and for re-lands; a warning keeps
  them.
- **Join verdicts into request rows.** Verdicts can change after the
  request is written and are per reviewer; reading them at listing is one
  batched query and never stale.
