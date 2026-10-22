# 0061. The git mirror follows refs under a prefix, push-only
Status: Accepted
Date: 2026-10-22

## Context
Leep's own development moves onto a live Leep server, with GitHub kept as
the place for pull requests and CI. Agents land work on Leep refs
`review/<area>-<desc>`; the mirror has to push each of them to a GitHub
branch of the same name, so a pull request and CI run there, and then the
agent lands on `leep-main`.

The mirror of ADR 0046 cannot do that. Its refs are fixed at startup by
`--ref`, and only `--tags` finds refs as they appear. A Leep ref that is
deleted puts its ref in `deleted-in-leep`, which needs a person, and leaves
the GitHub branch behind. Review refs are created and deleted all day, and
an agent rewrites one when it rebases or squashes its stack, which ADR
0046 reports as `rewound` or `diverged`.

## Decision
`--ref-prefix PREFIX` (repeatable, for example `review/`) mirrors every
Leep ref under `PREFIX` to `refs/heads/<same name>`.

- **Discovery.** Each read of Leep's refs adds the refs under a prefix, as
  `--tags` does for tags. Each `ls-remote` also lists the remote's branches
  under the prefix, so a branch whose Leep ref was deleted while the mirror
  was down is still found.
- **Push-only.** A prefix ref is never imported, never merged, never a
  land-branch target, and gets no diverged branch. `--import`,
  `--on-divergence` and `--land-branches` apply only to `--ref` refs.
- **Rewrites.** When the remote branch is not behind the export, the mirror
  asks whether this Leep ref ever held the remote's tip (the value it last
  saw in sync, or the ref log, through the same origin test as ADR 0046).
  If it did, the branch holds nothing Leep lacks, and the export is pushed
  with `--force-with-lease=<branch>:<that tip>`. If it did not, someone
  pushed to the branch: it is reported as `remote-ahead` (or `diverged`
  when Leep moved too), and left alone.
- **Deletes.** When the Leep ref is gone, the mirror first re-reads Leep's
  refs, so a stale list never deletes a branch. It then applies the same
  test to the remote's tip and deletes the branch with a lease on it, and
  stops tracking the ref. That is a healthy end, recorded in the status
  (`deleted_branches`, `branches_deleted`) and the metric
  `leep_mirror_branches_deleted_total`. A lease the remote refuses, or a
  tip the ref never held, is reported as `remote-ahead` and nothing is
  deleted.
- **Bounds.** A prefix must end in `/`, may not be empty, `/`, under
  `tags/`, or overlap `--diverged-prefix`, `--land-prefix`, another
  `--ref-prefix`, or any `--ref` by Leep name or branch. So the mirror never
  deletes or rewinds a branch outside its prefixes, a tag, or `main`.
  `--ref` refs keep ADR 0046's rules, `deleted-in-leep` included.

This extends ADR 0046's force policy with a third kind of lease-guarded
non-fast-forward update. Every decision still uses only the two current
values, the map and the ref log, so a crash anywhere is recovered by the
next cycle: a delete the remote took leaves nothing to track, and one it
did not take is tried again.

## Consequences
- Agents can open pull requests from Leep without git credentials. The
  deploy key needs rights to create, update and delete branches under the
  prefix, which a GitHub ruleset can grant to it alone.
- A person's push to a review branch stops that ref until someone lands the
  work in Leep and deletes the branch; the mirror never loses it.
- Prefix refs add per-ref metric series that come and go with the refs.
- The status document only gains fields (`ref_prefix` on a ref,
  `branches_deleted` and `deleted_branches` at the top), so its version
  stays 1.
