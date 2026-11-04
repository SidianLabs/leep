# 0091. Known-good refs, check attestations and suspects
Status: Accepted
Date: 2026-11-04

## Context
Leep lands without a queue (ADR 0004, principle 6): two agents whose changes
merge cleanly both land, each tested against its own base. The pair can still
break the build together (A renames a function, B adds a caller of the old
name). Nothing on the land path can catch that without putting a test run on
it, which is the queue ADR 0004 removed, and ADR 0063 left "CI green as a land
gate" out on purpose.

What teams need is narrower:

- **A tree known to build.** Deploys, release cuts and "give me something that
  passes" want the newest changeset that passed every required check, not the
  head.
- **Who to look at when the head breaks.** With dozens of lands an hour, the
  lands between the last good changeset and a red one are the place to start,
  and the ref log (ADR 0018) already records each one with its actor.

Chromium's LKGR (last known good revision) does the first from CI results
alone; the server can do it itself, since it holds the results and the refs.

## Decision

### Check results are attestations
A check result is a derived row beside the changeset, like a review (ADR
0011): kind **`check-v1`**, key `Hash::of_object("check", changeset_id ‖
check name)`, value postcard `Check { check, outcome, by, url, summary,
timestamp }`, `Outcome` an externally tagged unit enum `Pass | Fail`
(variants append only). One row per (changeset, check); a rerun replaces it
(last writer wins), so a flaky check's retry counts and two checks never race.
Nothing hashed changes; no SQL migration.

Rows are **keyed by changeset id and never carried across rebases.** A land
that rebases makes a new changeset with a new tree; a result for the
original says nothing about the copy, so CI tests what is on the ref.

`POST /v1/changesets/{id}/checks {check, outcome, url?, summary?}` records
one. The poster is the token's name (`by`), never a body field.

### Who may attest
- The tokens file gains `"checks": ["test", "lint"]`, an allow-list of check
  names (absent: none; write role only). A name outside the list is a 403
  before anything is read. Agent tokens get no `checks`.
- **The changeset's creator cannot attest it** (403): the `created-by-v1` row
  (ADR 0086) of the changeset, or of any changeset in its rebase chain, names
  the token. A CI token that also commits cannot pass its own change. Every
  way a client makes a changeset records its creator: `POST /v1/changesets`,
  and `POST /v1/merge`, which records the caller when the merge is new
  (without that, a token could merge its own change and attest the merge).
  A server rebase links the copy to its original.
- **Only changesets a ref holds** take results (400 otherwise): some ref's
  first-parent chain, the checks policy's refs first. CI attests what
  landed or what a review ref holds, and a result cannot pin an abandoned
  changeset against GC.
- Without a tokens file the endpoint is a 400.

### Known-good refs
The checks policy file (`--checks-policy-file`, `deny_unknown_fields`,
re-read on SIGHUP and part of the config fingerprint, ADR 0024) names per ref
the checks it requires:

```json
{ "refs": { "main": { "required": ["test", "lint"] } } }
```

Each listed ref gets **`green/<ref>`**, a ref only the server moves:

- **Promotion.** After storing a `Pass`, the server checks every policy ref
  requiring that check. When the changeset is on the ref's first-parent chain,
  every required check has a `Pass` row on it, and the current green is an
  ancestor of it (or absent), it moves `green/<ref>` there with
  `cas_ref_logged` (operation `promote`, actor the token that completed it,
  message the check). A lost swap reads green again and retries. If the new
  green is no longer an ancestor, someone promoted further and nothing is
  lost. No lease, no coordinator: correctness is the CAS (ADR 0056). A result
  for a changeset at or below green, off the first-parent chain, or never
  landed promotes nothing; an older pass that arrives late never rewinds.
- **Catch-up.** At startup and after a reload that changed the checks
  policy, the server walks each policy ref's first-parent chain from the
  head down to green (at most 1,024 changesets) and promotes to the newest
  changeset that passes every required check. Dropping a flaky required
  check unblocks green at once, not at the next pass.
- **Protection.** The whole `green/` namespace is reserved, whether or not
  the policy names the ref: `land` and every raw `POST /v1/refs/...` create
  or move of a `green/` ref (forward or back, so `leep undo`) are refused
  with **422 `promotion_only`**. Otherwise a write token could plant a
  "known good" `green/<x>` for a ref added to the policy later, or wedge
  its promotion. Deleting a `green/` ref is refused while the policy names
  its ref (a `GreenRefs` land policy covers those), and allowed otherwise,
  which is the recovery path below. `promote` is a reserved ref log
  operation, like `land` and `import`. Tags and green refs cannot be in the
  policy. The rules are enforced by `land` and `cas_ref_checked`, so they
  hold for every client and for the git mirror. The offline `leep-git
  import --set-ref` writes the store directly and bypasses every ref
  policy, as it always has; it is an operator tool, not an API.
- **The checked ref is forward-only.** A ref in the checks policy is
  protected as a review ref is: it moves only forward and cannot be deleted
  through the API. Otherwise a write token could move `main` back below
  green, and green, which only moves forward, would be stuck ahead of the
  ref for good, with every later changeset off its history. So a ref with
  a green ref cannot be a `rebase-onto` target (ADR 0062), and a
  git-primary mirror ref (`--primary git`) cannot be in the checks policy.

Red is derived, never stored. Each required check is judged by its own
newest result above green on the head's first-parent chain; the head is red
when one of them is a `Fail`, and `first_failed` is where the longest such
run of failures began (the walk reads at most 1,024 changesets, and finding
the land that brought `first_failed` at most 1,024 ref moves). `GET /v1/refs/{name}/checks` reports `head`, `green`, `behind`
(generations), `required` and `red {first_failed, check, since_seq}`.

### Suspects, not culprits
`GET /v1/refs/{name}/culprits?bad=REV&good=REV` lists the **suspects**: the
lands in the range. These are the ref log entries of the ref (ADR 0018) from
the one that brought `bad` (default: the head) down to the first above `good`
(default: `green/<name>`; a sheriff names `good` to look below green), newest
first. Each has `changeset`, `seq`, `actor`, `land_id`, `created_by` (via the
rebase chain), `author`, `agent_model`, `title` and the `paths` it changed,
so the failing test's files rank them. The list is paged (`limit`, 100 by
default and at most 500; `before`, the previous page's `next`), and paths
and creators are read for the returned page only. Finding the land that
brought `bad` reads at most 1,024 ref moves and says `truncated` when it
gives up. Both ref routes are in the `log_filtered` read class (ADR 0060),
so any read token's history walks share its permits. A ref literally named
`main/checks` never answers for `main`: the suffix wins when the base ref
exists.

A suspect is a land in the range, never a verdict. **A land whose test fails
only together with a later land shows that later land as a suspect too.** An
attacker can plant a tripwire that fires only once some innocent later land
arrives, and frame that land. The API docs, CLI and MCP tool say "suspects:
lands in the range", and the next step is to read the paths and rerun, not
to revert by name.

### Clients
`leep checks post REV CHECK pass|fail [--url] [-m]`, `leep checks status
[REV]`, `leep checks ref [REF]` (exit 1 when red), `leep checks culprits
[REF] [--bad REV] [--good REV]`; `leep checkout green/main` already works.
MCP read tools `leep_checks_status` and `leep_checks_culprits`. Agents keep
basing work on the head (`sync` is unchanged): basing on green would replay
every land since, defeat the disjoint fast path (ADR 0066) and hide the
breakage from the agent's own tests.

### GC and integrity
`check-v1` is an attestation (ADR 0059 root 4): a changeset with a check row
is a GC root, so its rows are never orphaned by the sweep. Rows are keyed
by a hash of (changeset, name), not the changeset id, so they are not in
`PER_CHANGESET_KINDS`; the root makes that unnecessary, as for `review-v1`.
In practice CI attests heads, which the ref log keeps anyway, and posts
for changesets no ref holds are refused. Unlike other attestations, `repair
drop` may drop `check-v1` rows: CI reproduces a result by rerunning the
check, so dropping them is how an operator unpins changesets they keep.
`verify --derived` decodes and bounds every row and reports rows whose
changeset is gone; `repair rederive` cannot recompute them. Each GC mark
reads every `check-v1` row once to learn the check names (`check_names`),
as it does for reviews and signatures: a cost linear in the number of
results, worth a cache if results grow into the millions.

## Threat model for CI tokens
- Tokens are static and allow-listed by check name. The body never names
  the poster; `by` is stored and shown in every listing, and the promotion's
  ref log entry names the token that completed it.
- The creator cannot attest its own changeset, a rebased copy of it, or a
  merge it made through `POST /v1/merge`.
- **Recommended: one token per check name.** A promotion then needs every
  required check's token, so one leaked CI token cannot promote alone.
- A forged `Fail` only paints the ref red and adds noise to the suspects
  list. It never moves green back and never blocks a land.
- A forged `Pass` promotes untested code to `green/<ref>`. This is the trust
  any deployment already places in CI that reports to GitHub or deploys on
  green; the token allow-list, the creator rule and the per-check token keep
  it to the tokens that would be trusted anyway.
- Nothing here gates `land`. A compromised CI token cannot stop work or move
  `main`; it can only mislead consumers of `green/<ref>`.

## Recommendations on the open decisions
1. **Namespace:** fixed `green/<ref>`, like `tags/` (ADR 0015). Configurable
   names would put a second mapping in every client and in the docs. One
   fixed prefix is protected by name.
2. **Where required checks live:** in server config, not an in-tree file.
   CI identity (the tokens) is config already, and an in-tree list would
   let a change drop the checks that judge it.
3. **All or any:** all required checks. With "any", the weakest check
   decides, and a forged pass of the cheapest check would be enough.

## Not in this change
Server-guided bisect (`checks/next`), `RequireChecks` as a land policy for
release refs, the sheriff inbox item, server auto-revert (a changeset nobody
wrote, which `--require-signatures` refuses), and adopting it on
`leep-main`. `land` and `land_batch` are unchanged apart from refusing green
refs up front.

## Consequences
- A deploy or a person who needs a working tree reads `green/main`; nothing
  about landing changes.
- Every ref in the checks policy is forward-only, like a review ref.
- Recovery after an admin rolls the database back below green: drop the ref
  from the checks policy and SIGHUP; `green/<ref>` may then be deleted,
  so delete it with `POST /v1/refs/green/<ref>` (creating or moving one
  stays refused); re-add the
  ref and SIGHUP, and the catch-up promotes from the stored results.
- An older server ignores `check-v1` rows and refuses a tokens file with
  `checks` and the `--checks-policy-file` flag; upgrade every server first.
