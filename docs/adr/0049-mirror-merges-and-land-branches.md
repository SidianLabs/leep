# 0049. The git mirror merges clean divergences and lands land branches
Status: Accepted
Date: 2026-10-18

## Context
The live mirror (ADR 0046) stops a ref when both sides moved. It reports
`diverged`, pushes Leep's side to `leep-diverged/<branch>`, and waits for a
person to merge that branch in git. That is right under every land policy,
but under continuous agent landing a person's push nearly always races a
land. In the concurrent-writers e2e test the ref spends much of its time
`diverged`, and each divergence costs a person a merge in git, often for
changes that touch different files.

ADR 0046 named two answers as the next M7 steps:

- merge clean divergences automatically, opt-in;
- land branches, so people can hand work to Leep the way agents do and
  never race a land on a busy ref.

Both have to keep 0046's properties: exactly once across crashes, no loops,
never force, decisions from current values only, no server change, and no
conflict resolved silently (CLAUDE.md).

## Decision

### `--on-divergence merge` (default `report`)
When the classification of ADR 0046 says "neither contains the other", a
mirror started with `--on-divergence merge` (which needs `--import`) does
this instead of reporting at once:

1. **Refuse to merge back an undo.** If the remote's tip `G` is Leep's own
   commit that this ref moved back from (the same test as for
   fast-forward imports: an exported commit, or an imported one the ref
   log shows the ref held), merging would revert the undo with nobody
   asking. It is reported as `diverged`.
2. **Import `G`** exactly as a fast-forward import does, giving its
   changeset `T`.
3. **Merge on the server**: `POST /v1/merge` with parents `[L, T]`. Leep's
   side is the first parent, so Leep's first-parent history stays the line
   of lands and the git merge's first parent is `E`.
4. **Conflicts fall back to report.** The server answers with the
   conflicting paths and stores nothing. The mirror reports `diverged`,
   with the paths in the detail, pushes Leep's side to the diverged branch
   as before, and does not ask about the same `(L, G)` again until either
   side moves. Nothing is resolved by the mirror or the server.
5. **Land the merge by ref CAS** from `L`, through `cas_ref_checked`, so
   the land policy applies to everything the ref gains: the merge and the
   imported commits. A refusal (for example a signature policy, since the
   mirror has no key) falls back to report and is asked again every minute.
   A lost CAS is classified again at once.
6. **Push it.** The next classification finds `G` an ancestor of `E(M)`
   (imported changesets export to their original commits, and `M` has `T`
   as a parent) and pushes without force: a fast-forward of the remote.

**The merge is deterministic.** Its fields are all functions of `L` and
`G`: the parents, a fixed message naming `G`, `L` and the git branch, the
configured `--merge-author` (default `Leep git mirror
<git-mirror@leep.invalid>`), the newer of the two parents' timestamps, and
`extra["git-mirror.merged"] = G`. The server's merge of two trees is
deterministic and the merge base is unique. So the same `(L, G)` always
gives the same changeset id, and so the same commit. `--merge-author` is
part of the id and must stay fixed for the life of a mirror, like the
trailer flags. The commit carries the usual `Leep-Changeset:` trailer, so
loop prevention and the e2e history check treat it as any changeset made
in Leep.

We considered letting the server pick the time (absent `timestamp`). It
would give each retry a new id. A crash between the merge and the CAS would
then leave the restart merging again under a new id, which is harmless for
the remote but makes "the same inputs give the same commit" untrue and the
crash analysis harder.

**Crash points**, each recovered by the next cycle from current values:

| crash after | state found on restart | what the restart does |
|---|---|---|
| import, before the merge (`merge-imported`) | `L`, `G` unchanged; `T` exists | the import finds nothing new; the merge gives the same `M`; CAS; push |
| the merge, before the CAS (`merged`) | `M` stored, on no ref | the merge returns the same `M` (creating it again is a no-op); CAS; push |
| the CAS, before the push (`merge-landed`) | `L = M` | `G` is an ancestor of `E(M)`: push |
| the push, answer lost (`pushed`) | `G = E(M)` | in sync; nothing pushed |

If either side moves while the mirror is down, the restart merges the new
pair instead; a merge stored but never landed stays on no ref, is never
exported, and is never pushed. Only changesets on `L`'s history reach the
remote, so every changeset still has exactly one commit there. The ref log
shows one `git-mirror` move per merge.

### Land branches (`--land-branches`, prefix `--land-prefix`, default `leep-land/`)
A person pushes `leep-land/<ref>/<anything>` to the remote. `<ref>` is the
longest mirrored branch the name continues (`leep-land/release/1.0/fix`
lands on `release/1.0` when that is mirrored). With `--land-branches`
(which needs `--import`), every remote poll also lists that prefix, and for
each branch the mirror:

1. imports its commits as any import does, giving the head's changeset;
2. calls `POST /v1/land` with `create: false`. The server fast-forwards,
   or replays the branch's unlanded stack onto the head exactly as it
   replays an agent's land, through the land policy, and records the move
   in the ref log as `land` with the mirror's token as the actor;
3. on success, deletes the branch with `--force-with-lease` on the exact
   value it landed, so a branch someone pushed to meanwhile is kept and
   landed again on the next poll;
4. leaves the export to the normal cycle: the ref moved, so the mirror
   pushes `E` as a fast-forward of the remote branch.

Conflicts (the land's answer lists the paths) and refusals (400 or 404:
policy, a merge commit in the stack, which land does not replay, a
target that does not exist) leave the branch where it is. They appear in
`status.json` under `land_branches` with a detail that says what to do,
in `leep_mirror_land_branches_total{ref,result}` and in the gauge
`leep_mirror_land_branches_failing{ref}`. A conflict is retried every 30
seconds and a refusal every minute, and at once when the branch moves. A
branch whose name matches no mirrored ref is reported as `refused`.

**Why land and not import.** A land branch is not on the mirrored branch,
so nothing published has to keep its commit ids: rebasing is fine, and it
is what keeps people from racing agents. This is the opposite of a push
to the mirrored branch itself, which ADR 0046 imports only as a
fast-forward because the remote already published those commits.

**Crash points:**

| crash after | state found on restart | what the restart does |
|---|---|---|
| the import (`land-imported`) | branch present, nothing landed | imports nothing new; lands |
| the land, before the delete (`land-landed`) | branch present, ref holds it or its rebased copy | land answers with what is there (the head contains the changeset, or the server finds its rebased copy); deletes |
| the delete (`land-deleted`) | branch gone, Leep ahead of the remote | pushes `E` |

A land's own idempotence has one bound: the server looks for an earlier
rebased copy among the last 1024 changesets above the stack. A mirror that
stays down while more than that lands on the ref, after landing a branch
but before deleting it, may land it a second time. That is the same bound
every client retrying a land has.

**Land branches are the pusher's to fix**, not the operator's: a failing
land branch does not change the ref's state or `leep-git mirror status`'s
exit code. `LeepMirrorLandBranchFailing` is a ticket after an hour.

### Rebased copies get the trailer
The server's rebased copy of an imported changeset keeps its `extra`,
including `git.commit` naming the original commit. ADR 0046 left the
`Leep-Changeset:` trailer off every changeset with `git.commit`, which
would export such a copy as a commit that says nothing of where it came
from, and that loop prevention could recognise only through the map. The
rule is now: a changeset whose export **is** the commit its `git.commit`
names gets no trailer; any other gets one. For every changeset the mirror
could export before this change, the commit is the same (an imported
changeset that reproduces its commit, or a native one). The exception is
an imported changeset that does not reproduce its commit (submodules)
exported after the map was lost: that already diverged, and still does.

### What did not change
No server change and no new endpoint: `/v1/merge`, `/v1/land`, refs, ref
CAS and the ref log already had everything. No push uses `--force`; the
only leases are on mirror-owned refs (diverged branches, and now landed
land branches) and on `--allow-rewind` refs. `report` stays the default:
a merge adds a commit that no person or agent wrote, which teams should
choose.

## Consequences
- With `--on-divergence merge`, the concurrent-writers e2e test converges
  with no merge in git: no `diverged` state, no diverged branch. A
  conflicting divergence still stops the ref and waits for a person.
- People who want to avoid divergence entirely push to `leep-land/main/...`
  and their work lands like an agent's, rebased, with conflicts reported
  back in the status.
- The mirror's merges show up in history as merge commits by the mirror's
  author. Signature policies refuse them, so with such a policy the mode
  only adds a failed attempt per divergence before reporting it.
- Land branches need the mirror's git credential to delete branches under
  the prefix, and its Leep token to land (a `write` token, as for imports).
- Remote branches named under `--land-prefix` are taken as requests to
  land. A team turning `--land-branches` on should check none exist that
  mean something else.
- Remaining for M7: an optional long-poll on the ref log, mirroring of Leep
  ref deletes for allowed refs, seeding the map offline, and the git wire
  protocol on the server.
