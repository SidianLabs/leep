# 0028. Background index warming
Status: Accepted; amended by 0058 (the warmer tails the ref log every 250 ms and lists refs every 5 s)
Date: 2026-10-16

## Context
Derived kinds other than `root_tree` and `generation` are computed on first
read. On histories built by agents a changeset at a time that cost is spread
over many requests and nobody notices. On an imported history it is not: after
importing git/git (82,500 commits) the first `last-change` request derived the
whole path-history index inside that one request and answered after 69 minutes
(before the merge-base shortcut of #51 the projection was over 11 hours), and the first search spent
5 s deriving its trigram rows. A server restarted onto a large repository, or a
new server joining a fleet, starts in the same state. The person who asks the
first question pays for everyone, usually long past any request timeout.

A second problem shows up as soon as anything else derives: `derive` had no
coordination, so two requests deriving the same missing range (or a request
and an operator's `derive-search`) both computed every value and wrote the same
rows twice.

## Decision
1. **A warmer in every server.** `leep_derive::warm::Warmer` lists the refs
   every `--warm-interval` (default 30 s, `LEEP_WARM_INTERVAL`; 0 turns it off)
   and, for each ref whose head changed since its last pass, derives the
   path-history index (which brings generation, skiplists and last-merge) and,
   with `--warm-search` (default on), the code-search index of its tree. It
   runs from startup, so a restarted server or a fresh import is warmed without
   anyone asking. Polling, rather than hooking `land`, catches every way a ref
   moves: lands, `POST /v1/refs`, `leep-git import --set-ref`, restores, and
   other servers sharing the metadata store. A failed ref is retried on the
   next pass. Progress is visible in `leep_warm_refs_total{result}` and
   `leep_warm_duration_seconds`, and a warm that takes over a second is logged.
2. **One derivation per kind at a time.** `derive` takes a per-kind lock
   (per `Repo`, so per process) once it finds the value missing, and checks
   again after acquiring it. A second caller waits, then finds the rows the
   first stored and derives only what is still missing. A task that already
   holds a kind's lock does not take it again (a merge asks for its merge
   base's value while deriving), which a task-local list of held kinds tracks.
   Kinds depend on each other without cycles (path history on generation,
   skiplist and last-merge; skiplist on generation), so locks are taken in one
   order.

## Consequences
- After an import or a restart the warmer derives in the background; queries
  that arrive meanwhile wait on the lock for the remainder instead of starting a
  second derivation, and queries after it finish are answered from the index.
  On a history built through lands the warmer derives a handful of changesets
  per pass.
- A long derivation holds that kind's lock for its duration. Other requests
  needing the same kind for new changesets wait behind it; they would otherwise
  have derived the same range themselves.
- Servers sharing a metadata store each run a warmer. They do not coordinate
  across processes, so after an import every server derives in parallel until
  the first to finish has stored the rows; derivation is idempotent, so the
  rows agree. Operators who want one warmer can set `LEEP_WARM_INTERVAL=0` on
  the others.
- Code search is still derived per revision; warming covers ref heads only.
  `leep-server derive-search <rev>` stays for other revisions.
- Blame caches stay lazy: they are per file and per revision, and warming them
  all would cost more than it saves.
