# 0090. Git wire protocol on the server, starting with derived git ids
Status: Proposed (phase 0 accepted)
Date: 2026-11-04

Supersedes, for the server, the "bridge, not wire protocol" part of
[0001](0001-server-centric-design.md). Extends
[0046](0046-live-git-mirror.md) and [0049](0049-mirror-merges-and-land-branches.md).

## Context
Leep needs its own `leep` client. That is the largest barrier to adopting it:
editors, CI (`actions/checkout`), dependency bots and most coding agents
speak git. OpenAI's Pando showed the other end of the trade, a git server
that keeps the stock client and coordinates per branch. Leep already beats
git by two orders of magnitude when many agents land on one branch
(PERFORMANCE.md, "Many agents landing on one branch": 1.1 accepted pushes
a second against 271 lands at 256 agents), and enforces review on the
server (ADRs 0011, 0085 to 0089). The goal is stock `git clone`, `fetch`
and `push` against a Leep server without giving either up.

Options weighed:
- **A smart-HTTP front end in `leep-server`** (protocol v2 `upload-pack`,
  v1 `receive-pack`), with a push to a branch becoming a land.
- **A GitHub-first mirror**, GitHub primary and Leep following: already
  built (ADRs 0046, 0062, 0063), but every write still goes through GitHub's
  one-branch-at-a-time push and its retry storm.
- **A `git-remote-leep` helper**: a binary on every machine, which is the
  barrier being removed, and invisible to tools that run plain git.

## Decision
Build the smart-HTTP front end in phases. Keep the mirror for teams that
keep GitHub primary; it and the front end share one object codec, so a
changeset has one commit id everywhere. No remote helper.

The design, phase by phase:

1. **Object ids (phase 0, this change).** A new crate, `leep-gitobj`
   (depending on `leep-core` and gix's own object encoder, `gix-object`),
   holds the pure codec: commit and tree encoding, `changeset_for`,
   `CommitStyle` and trailers. `leep-git` (import, export, mirror) uses it.
   `leep-derive` derives, as pure functions of content:

   | Kind | Key | Value |
   |---|---|---|
   | `git-blob-v1` | blob hash | 20-byte sha1 of the git blob |
   | `git-tree-v1` | tree node hash | 20-byte sha1 of the git tree (a subtree with no files left out) |
   | `git-commit-v1` | changeset id | 20-byte sha1 of the commit under the store's commit style |
   | `git-oid-v1` | `Hash::of_object("git-oid", sha)` | kind byte (1 blob, 2 tree, 3 commit) and the 32-byte Leep hash |

   A changeset's commit is the mirror's rule (ADR 0049): the commit it was
   imported from when the plain encoding reproduces it, and otherwise, under
   the `trailer` style, one carrying `Leep-Changeset: <id>`. So imported
   history derives to its original shas with no recorded mapping, and the
   mirror's commits equal the derived ones. Rows are written parents first
   by the derive framework (ADR 0003, 0050), reverse rows beside forward
   ones, so a cold store derives them on demand with no backfill.

   **Commit style.** The bytes of a changeset made in Leep depend on whether
   `Leep-Changeset` and `Leep-Agent-*` trailers are written, so the style is
   recorded once per store, as one row of `git-style-v1` (`plain`,
   `trailer`, either with `+provenance`), served at `GET`/`POST
   /v1/git/style`. Whatever derives a commit id first, or the first mirror
   start, records it (the mirror's defaults when a derivation does, `trailer`
   without provenance); another style is refused. A deployment that already
   runs a mirror must record that mirror's flags before anything else
   derives git ids (open question 3); a mirror started with other flags
   refuses to start. `leep-git export` follows a recorded style; where none
   is recorded it keeps its old output (no `Leep-Changeset` trailer) and
   records nothing, since its default is not the mirror's.

2. **Fetch and clone (phase 1).** `GET /git/{repo}.git/info/refs` and
   `POST .../git-upload-pack`, protocol v2 `ls-refs` and `fetch` (`want`,
   `have`, `deepen`, `filter blob:none|blob:limit`, thin packs), wants and
   haves mapped through `git-oid-v1`, negotiation pruned with skiplists and
   generations, packs generated from the blob store (undeltified first).
   Its own read class (`git_fetch`), caps on wants, haves and rounds.

3. **Push (phase 2).** `POST .../git-receive-pack`: the pack is streamed to
   disk and indexed, new commits become changesets (map rows in the same
   write), and each ref command runs through the land path with the land
   policy, so review, signatures, OWNERS and protection apply exactly as to
   `leep land`. A ref is in `land` mode (the server replays disjoint pushes
   and may rewrite the commit: `ok` plus a sideband line naming the landed
   commit) or `fast-forward` mode (git semantics). `refs/for/<ref>` always
   lands and is never advertised. Conflicts and policy refusals are `ng`
   lines with the server's text. No `atomic`.

4. **Scale (phase 3).** Deltas, sealed packs per 4,096 first-parent
   changesets reused across clones, a warmer group for moved heads.

5. **Later.** Annotated tags, SHA-256, `actions/checkout` against Leep.

### What phase 0 decided that the design left open

- **New kinds, not the bridge's maps.** `leep-to-git-v1` and
  `git-to-leep-v1` stay what they are: records of the commit an import or a
  mirror used. They are roots of changeset garbage collection (ADR 0059,
  "Mirror maps"), so deriving them for every changeset would keep every
  changeset alive; and for a commit the export does not reproduce (one with
  submodules) the record names the original, which cannot be recomputed.
  `git-commit-v1` is derived instead and is a per-changeset kind the sweep
  deletes with its changeset.
- **Where the style lives.** Not in `store_features` (ADR 0055): that table
  lists what a server must understand to read a store, and an older server
  refuses a store naming a feature it does not know. The style is a choice,
  not a read requirement, so it is a derived-table row written once, with a
  read back after the write. Two first writers that race with different
  styles see the one that stuck and the loser refuses; a writer that read
  its own value before being overwritten is the narrow remaining window,
  which needs two operators starting differently configured tools at the
  same moment.
- **Reverse rows** are not deleted by the changeset sweep (their key is a
  git id, which the sweep does not know). A lookup checks the forward row,
  so a reverse row whose changeset is gone answers "unknown". Under the
  `plain` style, two changesets differing only in extras that are not
  exported have one commit; the reverse row names the first derived.
- **Repair.** The git kinds derive on demand and are dropped together
  (`repair drop --kind git-commit-v1` removes all four); `repair rederive`
  does not compute them, since that would read every blob of a store that
  never used git. `verify` recomputes stored rows (blob ids from the bytes,
  tree ids from their children's stored rows, commit ids by derivation) and
  checks that reverse rows agree with forward ones.

## Open questions (maintainer decisions)

1. Git protocol code inside `leep-server` (recommended: in process, behind
   `--git`, in a crate depending only on core and derive) or a sidecar.
2. Default push mode per ref: `land` (ids rewritten, Leep's throughput) or
   `fast-forward` (git semantics); recommended `land` on `main`,
   `fast-forward` on `review/*`, `refs/for/*` for agents.
3. The commit style a deployment records: it must equal its running
   mirror's flags, or `leep-main` gets a second commit id.
4. `Leep-Agent-*` trailers on pushed commits becoming hashed `agent.*`
   extras (changes `changeset_for` for new imports), or provenance only in
   an unhashed push record.
5. Accepting a pushed stack with a merge whose side parents are already on
   the head (a `git pull` merge) by replaying its first-parent diff. Touches
   `land`.
6. Git-signed commits do not satisfy `--require-signatures`; a gpg/ssh
   bridge would be its own ADR.
7. Garbage collection roots for sealed packs (phase 3).
8. Deferred: SHA-256, annotated tags, atomic pushes.

## Consequences
- One codec for every git path, so imports, the mirror, `leep-git export`
  and future clones agree on ids. The existing round-trip and mirror tests
  pass unchanged; new tests show an imported history (merges, `-0000`,
  Latin-1, symlinks, executables, type changes) derives to its original
  shas from a cold store, and that the mirror's commits equal the derived
  ones.
- Computing a store's git ids reads every blob once (one sha1 each) and
  every tree node once; per changeset after that, only the directories its
  change rewrote. Nothing computes them until asked: phase 0 adds no
  endpoint that derives them and no warmer group.
- New `/v1` endpoint `GET`/`POST /v1/git/style`; no change to `Changeset`,
  `TreeNode`, hashing or the SQL schema.
