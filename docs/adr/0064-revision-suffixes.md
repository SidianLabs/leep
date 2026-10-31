# 0064. Git-style revision suffixes, resolved on the server
Status: Accepted
Date: 2026-10-31

## Context
"The commit before `main`" had no spelling in Leep: a person or agent ran
`leep log -n 2`, copied the second id and passed it back. Git's `main~1`,
`main^2` and `HEAD~3` are the most used revision syntax after abbreviated
ids, and agents trained on git reach for them first. The CLI holds no
history, so it cannot walk parents itself without a request per step.

## Decision
- **Grammar** (`leep_core::revision`). `REV` is a base followed by zero or
  more steps, applied left to right as in git:
  - `~N`: the Nth first-parent ancestor; `~` is `~1`.
  - `^N`: the Nth parent, 1-based; `^` is `^1`, `^0` the changeset itself.
  - `main~2^2~1` chains them. `N` is decimal and fits in a `u64`.

  The base is anything `/v1/resolve` already took: a ref name, a full id, or
  a short tag name. Anything else after the first `~` or `^` (`main~x`,
  `main^{commit}`, `main^!`) is `Invalid` (400), as is git's reflog syntax
  `@{...}`, which Leep does not have; `leep reflog` is the way to it.
- **Server-side.** `GET /v1/resolve/{*rev}` walks the steps in
  `Repo::resolve`. `^N` reads the changeset's parents; `~N` rides the
  first-parent skiplist (entry `k` is `2^k` steps up), so `main~100000`
  costs about 17 reads, and nothing is capped. Walking past a root or naming
  a parent a changeset does not have is `NotFound` (404), with a message that
  names the revision and how far the walk got.
- **Clients.** The CLI's `@` (the working copy base) and short ids stay
  client-side (ADR 0035): the CLI replaces the base with its full id and
  sends `<id><suffix>`, so `@~1`, `2a954d8e^2` and `main~3` all work wherever
  a command takes a revision. The MCP server resolves through the same
  client helper, so agents get the same grammar; `leep_resolve` reports
  `via: "suffix"` for such a revision.
- **Other routes.** Every other route takes a full changeset id, which a
  client gets from `/v1/resolve` first; none of them parses a revision.

## Why on the server
One round trip however deep the walk, against one per step from the client;
every client (CLI, MCP, scripts calling `/v1` directly, `leep-server
derive-search REV`) gets the same grammar from one implementation; and the
skiplist that makes large `N` cheap exists only on the server.

## Consequences
- **Compatibility.** Additive. A revision without `~`, `^` or `@{` resolves
  exactly as before, with the same 404. `/v1/resolve` used to answer 404 for
  every revision containing them; it now answers 200, 404 or 400. No type,
  encoding or field changed. A new CLI talking to an older server gets a 404
  for a suffixed revision, as it did before.
- **Ref names.** `refname::validate` already forbids `~`, `^` and `@{` (as
  git does), so no ref created through Leep can be misread as a suffix.
  `Repo::resolve` still tries the whole string as a name before parsing a
  suffix, so a ref stored before the rule existed keeps resolving to itself.
  Past that, the suffix starts at the first `~` or `^`.
- `@` as a server-side base is not a ref and stays a 404 there: only the CLI
  has a working copy.
