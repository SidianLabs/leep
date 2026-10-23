# 0062. Git-primary refs and two-way review branches in the mirror
Status: Proposed
Date: 2026-10-23

## Context
The mirror of ADR 0046 treats Leep as the source of truth and git as a
display of it. Every rule for a race follows from that: when both sides
moved, Leep's side is pushed to `leep-diverged/<branch>` and a person
merges in git; a Leep undo is `rewound` and the remote keeps its commits;
a person's push to a review branch (ADR 0061) stops that ref as
`remote-ahead` until someone lands the work in Leep by hand.

Every team in the first market ([PRODUCT.md](../PRODUCT.md)) is the other
way round. GitHub holds their people, CI, review and the merge button, and
nothing will move that. Leep is where their agents land. So the thing
people do all day, pressing merge or pushing to a branch, is exactly the
thing the mirror answers with a state that needs a person.

Leep's own development shows the cost. The flow in CLAUDE.md ("Working in
Leep") lands on `leep-main` and mirrors to GitHub, yet at the time of
writing every one of the 150 remote branches is a plain git branch, none is
a `review/` ref, and all 153 commits on GitHub `main` are squash merges
made on GitHub. The documented flow is not in use because the bridge
punishes the merge button, and the M5 self-hosting item stays open.

ADR 0049's `--on-divergence merge` and land branches help, but a mirror
merge adds a commit nobody wrote (which signature policies refuse), and
land branches ask people to learn a new branch name. Neither lets a team
keep GitHub's rules unchanged.

This ADR adds two things, both opt-in, both keeping 0046's properties
(exactly once across crashes, no loops, never force a published branch,
decisions from current values only, no conflict resolved silently):

1. **Git-primary refs.** For a `--ref`, the remote wins a race: Leep's
   unpushed lands are replayed on top of what git has, the way an agent's
   land is replayed onto a moved head.
2. **Two-way review branches.** A `--ref-prefix` ref accepts a person's
   push: new commits on the branch are imported and landed on the Leep ref,
   so a human and an agent can work on one pull request.

GitHub review approvals reaching Leep's review gate is the third part of
the same change and has its own ADR, [0063](0063-github-reviews.md).

## Decision

### `--primary git` (default `leep`), per `--ref`
`--ref NAME[:GIT_REF][@git]` marks a ref git-primary. `--primary git`
makes it the default for every `--ref`. It needs `--import`. Tags are
never git-primary (they never move). Prefix refs have their own rules
(below).

The classification table of ADR 0046 changes in two rows for such a ref:

| relation | Leep-primary (0046) | git-primary |
|---|---|---|
| `G == E` | in sync | in sync |
| `G` absent or `G ⊂ E` | push `E` | push `E` |
| `E ⊂ G`, tip from git | import, CAS fast-forward | import, CAS fast-forward |
| `E ⊂ G`, tip from Leep | `rewound`, never force | **restore**: CAS the Leep ref forward to `G`'s changeset; report `restored` |
| neither | `diverged`, push to `leep-diverged/` | **replay**: import `G`, replay Leep's unpushed lands onto it, move the ref; conflicts report `diverged` |

**Restore.** A Leep undo on a git-primary ref is undone by the mirror
within a cycle, because the remote is the truth and the remote still holds
the commits. The move is a fast-forward CAS from `L` (the changeset of `G`
is already known from the map, or is imported). An operator who wants to
move such a ref back reverts on GitHub. The ref log shows the move as
`git-mirror` with `restored` in its detail.

**Replay.** When neither side contains the other:

1. **Import `G`** exactly as a fast-forward import does, giving changeset
   `T`. Its new changesets go through the land policy at step 3, as an
   import's CAS does today.
2. **Find the stack.** `B` = the merge base of `L` and `T`. Leep's unpushed
   lands are the first-parent chain `S` from `L` down to `B`.
3. **Replay on the server** with one call, `POST /v1/refs/{name}/rebase-onto`
   (below), which rebases `S` onto `T` one changeset at a time, the way
   `land` replays an agent's stack, and moves the ref from `L` to the
   result `R` in one step.
4. **Conflicts fall back to report.** The server answers with the
   conflicting paths, squashed over the whole stack like a land's
   `Conflicts`, and stores nothing. The mirror reports `diverged` with the
   paths, pushes Leep's side to the diverged branch as 0046 does so the
   work is visible in git, and does not ask about the same `(L, G)` again
   until either side moves. Nothing is resolved by the mirror or the
   server. A person resolves by landing a merge of the diverged branch on
   GitHub, which imports as a fast-forward.
5. **Push.** The next classification finds `G ⊂ E(R)` (the imported
   changesets export to their original commits, and the copies sit above
   them) and pushes without force: a fast-forward of the remote. The
   remote is never rewritten; Leep's ref is.

**What agents see.** An agent that landed `a` holds `a` by id. After a
replay the ref holds `a'`, a rebased copy with `extra["leep.rebased_from"]
= a`, and `a` stays stored (immutable, reachable from the ref log until
changeset GC's retention passes, ADR 0059). The agent's next land replays
onto the new head as it would after any other land; the server recognises
`a` as already landed through its copy among the last `LANDED_COPY_SCAN`
(1024) changesets, so retrying a land is still idempotent. `leep log`
shows `a'` with the usual rebased marker.

**Crash points**, each recovered by the next cycle from current values:

| crash after | state found on restart | what the restart does |
|---|---|---|
| the import, before the replay | `L`, `G` unchanged; `T` exists | the import finds nothing new; replays |
| the replay, before the ref move | nothing on any ref (copies are made and the ref moved in one server call, so this crash is the server's, and the call either moved the ref or stored only copies on no ref) | replays again; unreferenced copies are collected by changeset GC |
| the ref move, before the push | `L = R` | `G ⊂ E(R)`: push |
| the push, answer lost | `G = E(R)` | in sync; nothing pushed |

If either side moves while the mirror is down, the restart replays the new
pair. Copies stored but never on a ref are never exported and never
pushed, so every changeset on the ref still has exactly one commit on the
remote.

**Deletes and tags** keep ADR 0046's rules (`deleted-in-leep`, tags never
moved). `--allow-rewind` and git-primary cannot both be set for one ref:
the first pushes a Leep undo back, the second restores it. On a
git-primary ref `--on-divergence merge` does not apply: the replay is its
divergence rule.

### `POST /v1/refs/{name}/rebase-onto` (additive)
Body `{onto, expected_base, land_id?}`; answer `{status: "moved", head,
rebased: [[original, copy], ...]}`, or the `conflicts` shape of
`LandResponse` (`onto`, `conflicts`, `clean`) with nothing moved.

- It runs **inside the land coordinator for the ref** (ADR 0056), ordered
  with lands, so it never loses a CAS race to an agent and never livelocks
  under continuous landing. The mirror's plain CAS from `L` would.
- It checks that the merge base of the current head and `onto` is
  `expected_base`; otherwise 409, and the caller classifies again. This
  is what keeps the decision "from current values": a land that arrived
  between the mirror's read and this call is in the head, so it is in the
  stack and is replayed too, and a land that moved the base is refused.
- The stack is replayed exactly as `land` replays an agent's stack,
  reusing its code: one copy per changeset, conflicts squashed over the
  stack, nothing stored on a conflict.
- **Policy.** `onto`'s changesets that were never on the ref (the import)
  are checked by the land policy, as `cas_ref_checked` checks an import
  today. The stack's originals were checked when they landed and are not
  checked again; their copies inherit their standing, as a land's copies
  do. Signatures and reviews of the originals are reported for the copies
  through `rebase_chain`, as today.
- A ref under `LEEP_PROTECT_REFS` is refused (400): forward-only means
  forward-only, and this move is not forward. The mirror reports `refused`
  with the server's reason, which says to protect the branch on GitHub
  instead, where the truth is. Git-primary and a forward-only Leep ref
  are two answers to the same question, and an operator picks one.
- `land_id` (ADR 0057) makes a retry of the same replay answer with the
  first result. The mirror derives it from `(name, L, G)`.
- The ref log records one move, operation `git-mirror`, from `L` to `R`,
  with the old head kept so `leep undo` can show what was replayed.
- A `write` token is needed, as for every ref move.

This is the one server change. It only adds to `/v1`.

### `--ref-prefix PREFIX` with `--import`: two-way review branches
ADR 0061 made prefix refs push-only. With `--import`, a prefix ref is
two-way under these rules. Loop prevention, trailers and the map are
unchanged.

- **A person's commits on top (`remote-ahead`).** The branch is `E` plus
  commits from git. The mirror imports them and moves the Leep ref forward
  by CAS from `L`, through the land policy of that ref (usually none on a
  review ref). The next export is the same commits, so nothing is pushed.
- **Both moved (an agent rebased or squashed the stack while a person
  pushed).** The mirror imports the person's commits (those on `G` and not
  on `E`), then lands them on the Leep ref with `POST /v1/land` (`create:
  false`, a `land_id` from `(ref, G)`): the server replays the person's
  stack onto Leep's new tip exactly as it replays a land branch (ADR
  0049). It then pushes with `--force-with-lease=<branch>:<G>`, a lease on
  the exact tip it imported. Anything pushed since fails the lease, and
  the next cycle imports it. A review branch is not a published branch
  that must keep its ids (0049's reasoning for land branches), so the
  person's commits getting new ids on the branch is the same thing a
  rebase of a pull request does every day.
- **Conflicts** report `diverged` with the paths and leave the branch and
  the ref alone. Nobody resolves them. The person or the agent merges the
  other side into theirs and pushes or lands; the next cycle imports a
  fast-forward.
- **A branch deleted on the remote** (a pull request closed with its
  branch) deletes the Leep ref only when the ref's tip, or its rebased
  copy, is reachable from a mirrored `--ref`: the work landed. Otherwise
  it is reported as `deleted-on-remote` and the ref is kept, since the
  work exists nowhere else. The mirror then stops tracking the branch
  until the Leep ref moves or is deleted. The mirror knows a branch was
  deleted only when it saw it in sync earlier in the same process: from
  current values alone a deleted branch and one never pushed look the
  same, so a branch deleted while the mirror was down is pushed again on
  restart. That errs toward keeping work visible, never toward losing it.
- **Leep deletes** keep ADR 0061's rule: delete the branch with a lease on
  a tip the ref held, including a tip the mirror imported for it.
- `--on-divergence` and `--land-branches` still apply only to `--ref`
  refs. The replay above is the prefix refs' own divergence rule and is
  not optional once `--import` is on: a review branch nobody can push to
  is the state this ADR exists to end.

### Status and metrics
New states `restored` and `deleted-on-remote`; `diverged` on a
git-primary ref carries the conflicting paths. `leep-git mirror status`
exits 0 on `restored` (a healthy end) and 1 on `deleted-on-remote` and on
`diverged` as before. New counters
`leep_mirror_replays_total{ref,result}` and
`leep_mirror_restores_total{ref}`; the existing `LeepMirrorStuck` alert
covers the states that need a person.

### Testing
- `leep-git` library tests, in-memory stores and local bare repositories:
  a replay after a GitHub merge, with the four invariants of ADR 0046
  (one commit per changeset on the ref, matching trees, the remote only
  ever fast-forwarded, `git fsck --strict`); a restore after `leep undo`;
  a conflict that reports and moves nothing; a person's push on a review
  branch imported as a fast-forward, and one replayed after an agent
  squash with the lease on the imported tip; a deleted branch that does
  and does not delete the ref.
- `leep-server`: `rebase-onto` ordered with concurrent lands (no lost
  update, every land either in the stack or above the result), the
  `expected_base` refusal, the protected-ref refusal, `land_id` idempotence,
  and policy applied to the imported changesets only.
- e2e (`crates/leep-e2e/tests/mirror.rs`): agents landing continuously
  while a person merges on the remote, converging with no `diverged` state;
  the seeded SIGKILL sweep and named crash points (`LEEP_MIRROR_CRASH_AT`)
  extended with `replay-imported`, `replayed` and `replay-pushed`;
  a human push to an agent's review branch during an agent squash.

## Consequences
- A team keeps GitHub exactly as it is: branch protection, required
  reviews, the merge button, pushes to branches. Agents land through Leep
  and their work shows up as fast-forwards of `main`; a merge on GitHub
  costs nobody a hand merge. This is the adoption path PRODUCT.md
  describes, made true.
- Leep's own development can run the flow CLAUDE.md documents: `leep-main`
  git-primary, review refs two-way, GitHub keeps the merge button. The M5
  self-hosting item can close on real use.
- On a git-primary ref, Leep's history is rewritten when git wins a race:
  landed changesets are replaced by rebased copies. Clients that hold the
  originals by id still have them; the ref log and `leep.rebased_from`
  show the replay. A team that wants Leep's history to be the fixed one
  keeps `--primary leep` and ADR 0046's rules. The two are different
  promises and the flag says which one a ref makes.
- Git-primary refs cannot be forward-only protected in Leep. The
  protection belongs on GitHub, where the truth is.
- A conflict is still a stop: the mirror reports the paths, pushes Leep's
  side where people can see it, and waits for a person to merge. Agents
  keep landing on the Leep ref meanwhile, and the replay picks up every
  land once the conflict is merged.
- Review branches become shared. A person's commit on an agent's branch
  may get a new id on the branch when the agent rebases, as on any pull
  request branch. The lease on the exact imported tip means the mirror
  never discards a push it has not imported.
- One new endpoint, additive. The mirror still holds no database
  credentials, and the server still has no git code.
- Remaining: ADR 0063 (GitHub approvals reach the review gate); the ADR
  0046 list (long-poll, mirroring Leep deletes, offline map seeding, the
  git wire protocol) is unchanged.
