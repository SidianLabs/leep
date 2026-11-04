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
  the token. A CI token that also commits cannot pass its own change.
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
- **Protection.** A `GreenRefs` land policy covers every `green/<ref>`:
  `land`, raw `POST /v1/refs/...` moves (forward or back, so `leep undo`)
  and deletes are refused with **422 `promotion_only`**. `promote` is a
  reserved ref log operation, like `land` and `import`. Tags and green refs
  cannot be in the policy.
- **The checked ref is forward-only.** A ref in the checks policy is
  protected as a review ref is: it moves only forward and cannot be deleted
  through the API. Otherwise a write token could move `main` back below
  green, and green, which only moves forward, would be stuck ahead of the
  ref for good, with every later changeset off its history. So a ref with
  a green ref cannot be a `rebase-onto` target (ADR 0062), and a
  git-primary mirror ref (`--primary git`) cannot be in the checks policy.

Red is derived, never stored: the head is red when the newest changeset
above green on its first-parent chain that has a result for a required check
failed one. `GET /v1/refs/{name}/checks` reports `head`, `green`, `behind`
(generations), `required` and `red {first_failed, check, since_seq}`.

### Suspects, not culprits
`GET /v1/refs/{name}/culprits?bad=REV&good=REV` lists the **suspects**: the
lands in the range. These are the ref log entries of the ref (ADR 0018) from
the one that brought `bad` (default: the head) down to the first above `good`
(default: `green/<name>`; a sheriff names `good` to look below green), newest
first. Each has `changeset`, `seq`, `actor`, `land_id`, `created_by` (via the
rebase chain), `author`, `agent_model`, `title` and the `paths` it changed,
so the failing test's files rank them. The walk reads at most 4,096 entries
and says `truncated` when it stops.

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
In practice CI attests heads, which the ref log keeps anyway. `verify
--derived` decodes and bounds every row and reports rows whose changeset is
gone; `repair` refuses to rederive or drop them.

## Threat model for CI tokens
- Tokens are static and allow-listed by check name. The body never names
  the poster; `by` is stored and shown in every listing, and the promotion's
  ref log entry names the token that completed it.
- The creator cannot attest its own changeset (or a rebased copy of it).
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
  from the checks policy and SIGHUP; `green/<ref>` is then a plain ref,
  so delete it (or CAS it back) with `POST /v1/refs/green/<ref>`; re-add the
  ref and SIGHUP, and the catch-up promotes from the stored results.
- An older server ignores `check-v1` rows and refuses a tokens file with
  `checks` and the `--checks-policy-file` flag; upgrade every server first.
