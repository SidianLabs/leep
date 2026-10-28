# 0063. GitHub pull request reviews satisfy the Leep review gate
Status: Proposed
Date: 2026-10-23

## Context
Leep's review service (ADR 0011) is a land gate: every path a changeset
touches needs an approval from an owner named in `OWNERS`, an auto rule,
or merge-parent cover, and an owner's reject vetoes. Reviewers are the
names of authenticated tokens.

PRODUCT.md keeps GitHub as the human review UI, and ADR 0061 pushes agents'
review refs there as pull requests. But a GitHub approval has no effect in
Leep: the mirror imports a merged pull request through `cas_ref_checked`,
and on a ref with `--require-review` the import is `refused` until someone
posts a Leep approval on every imported changeset, after the same people
already approved it on GitHub. With ADR 0062's git-primary refs the gap is
sharper: GitHub is the truth, so a change GitHub's rules let through must
not be held at Leep's gate for a second copy of the same approval.

The gate itself is worth keeping. It is what lets the operator of an agent
fleet say "nothing lands in `auth/` without its owner", enforced by the
server rather than by a merge button. What is missing is a way for the
owner's approval to arrive from where they gave it.

## Decision

### Server: reviews on behalf of a named reviewer (additive)
- The tokens file gains `"review_for": true` (default `false`). A token
  with it may post `POST /v1/changesets/{id}/reviews` with a new body
  field `reviewer` (`#[serde(default)]`), naming the principal the review
  is for, and `source`, a short string saying where it came from. The
  review row is stored under that principal, so `OWNERS` matching, the
  owner veto and `GET .../review-status` are unchanged. A body naming a
  `reviewer` from a token without the capability is 403, never silently
  taken as the token's own review.
- A new derived kind `review-source-v1`, keyed as `review-v1` is, holds
  postcard `ReviewSource { via, source, timestamp }`: the token that
  posted and its `source` string. `GET .../reviews` reports `source` and
  `via` per review when a row exists (additive fields). The audit trail
  is this row plus the request log.
- `Verdict` gains a third variant, `Withdrawn`, appended last so stored
  rows keep decoding (the `change-persistent-format` skill; FORMATS.md
  lists the variant). The policy treats it as no review. It exists
  because an approval on GitHub can be dismissed, and derived rows are
  last-writer-wins with no delete: withdrawing is a write. `leep review
  withdraw REV` and the plain endpoint accept it for a token's own reviews
  too.

### Mirror: `--github-reviews`
Needs `--ref-prefix` and `--import` (ADR 0062), plus:

- `--github-api-token-file F`: a GitHub token with read access to pull
  requests, kept out of argv like the Leep token. The mirror's git
  credential is not reused: it is a deploy key with push rights, and this
  needs none.
- `--github-users FILE`: lines `login = principal`, mapping GitHub logins
  to Leep token names. **Only mapped logins count.** An approval from an
  unmapped login is counted in a metric and ignored, so the file is the
  trust root, and nothing a stranger does on GitHub reaches the gate.

Every `--git-poll`, and on `POST /poke`, for each prefix ref whose branch
has an open pull request (`GET /repos/{o}/{r}/pulls?head={owner}:{branch}`),
the mirror reads the pull request's reviews and keeps the latest per
login. Then, for every changeset on the Leep ref that is not on the pull
request's base ref in Leep (the unlanded stack):

| latest GitHub review by a mapped login | Leep review posted for that principal |
|---|---|
| `APPROVED`, on the current head commit | `Approve`, `source = github:<o>/<r>#<pr>/reviews/<id>` |
| `CHANGES_REQUESTED` | `Reject`, same source |
| `APPROVED` on an older commit, `DISMISSED`, or none where one was posted before | `Withdrawn` |

- Stale approvals are approvals of a different change. Leep ties an
  approval to one changeset id (ADR 0011); GitHub ties it to a commit. An
  approval given on an older head is withdrawn, which matches GitHub's own
  "dismiss stale reviews" setting whether or not the team turned it on.
- The post is idempotent: the same verdict for the same principal is a
  no-op (last-writer-wins). The mirror keeps no record of what it posted;
  it reads Leep's reviews for the stack (`GET .../reviews`, with
  `via` naming the mirror's token) and posts only differences. A crash
  anywhere is recovered by the next poll from current values.
- The mirror never posts under its own name and never approves anything
  itself. A `review_for` token that is not also an owner approves nothing
  on its own account.
- Nothing lands because of this. The agent, or a person with `leep land`,
  still lands when `leep review status` says approved, as today. The gate
  is unchanged; only the way an approval arrives is new.

### Not in this ADR
- Required status checks as an approval rule. A green check is a different
  statement from an owner's approval, and the policy has no rule for it.
  If a team wants "CI green" as a condition it belongs in the land policy
  as its own rule, with its own ADR.
- GitHub teams (`@org/team`) in `OWNERS`. Owners stay principal names
  until identity (M7) gives Leep a notion of a group.
- Comments. A GitHub review's text is not copied; the `source` string
  links to it.

### Testing
- `leep-server`: `review_for` posts stored under the named reviewer with a
  source row; 403 without the capability; `Withdrawn` treated as absent by
  the policy, including after an `Approve`; old `review-v1` rows decode.
- `leep-git`: a fake GitHub API (the test already runs a local remote):
  approve, request changes, dismiss and stale-approval cases, each
  checked against `review-status`; an unmapped login ignored; a crash
  between reading GitHub and posting, recovered on the next poll.
- e2e: an agent's review ref, a mapped owner approving on the fake API,
  `leep review status` exiting 0, and the land succeeding on a
  `--require-review` ref.

## Consequences
- A team reviews once, on GitHub, and Leep's gate holds. `--require-review`
  with `OWNERS` becomes usable by a team whose reviewers never open a Leep
  CLI.
- The mapping file decides who exists. That is deliberate: it is the
  smallest identity story that is safe, and it is replaced, not extended,
  when M7 brings OIDC.
- `review_for` tokens can write a review as anyone they name. They are for
  the mirror and nothing else; the security review (SECURITY-REVIEW.md)
  gains an entry, and the audit row says which token spoke for whom.
- A persisted enum gains a variant. Servers must upgrade before any client
  or mirror posts `Withdrawn`; an old server answers 400 to it and nothing
  is stored.
- With ADR 0062, Leep's own development can require review on `leep-main`
  and have the maintainer's GitHub approval be the approval, which is the
  flow CLAUDE.md describes and the first real test of the gate.
