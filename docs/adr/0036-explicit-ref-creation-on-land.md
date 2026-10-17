# 0036. Explicit ref creation on land
Status: Accepted
Date: 2026-10-17

## Context
`leep land --ref nosuch` created the ref `nosuch` and printed `Landed ... on
nosuch`, which reads as success (`docs/research/CLI-UX.md`, R2). A typo made
a new branch on the shared server. It could be undone (`leep branch -d`,
`leep undo`), but nothing told the user it had happened. `POST /v1/land`
has always created a missing target ref, and old clients, the MCP server, the
soak harness and scripts rely on that.

## Decision
- **API.** `LandRequest` gains `create: Option<bool>`
  (`#[serde(default)]`, omitted when unset). Absent keeps the original
  behaviour, so every existing client is unaffected: the land creates a
  missing ref. `true` is the same, said explicitly. `false` makes a land onto
  a ref that does not exist a 404 whose message carries the stable marker
  `leep_core::refname::MISSING_REF_MARKER` ("does not exist and the land may
  not create it"), with nothing moved or logged. An `Option` and not a plain
  `bool` because a plain `false` default could not tell an old client from a
  careful new one.
- **Where it is checked.** `Repo::land_with(cs, ref, actor, create)` checks
  in the land step, against the head the batch read for its
  compare-and-swap (ADR 0032), not with a separate read first. A ref deleted
  between a client's check and its land is therefore never silently
  recreated, and refs still move only through the CAS. `land` and `land_as`
  keep creating.
- **CLI.** `leep land` sends `create: false` unless `--create` is given. On
  the marker 404 it fails with exit 4 and the hint ``leep land --ref X
  --create``. One exception keeps the very first land working without a flag:
  when the target is `main` and the server has no refs at all, the CLI lands
  again with `create: true`. (If another client's first land created `main`
  in between, it lands normally.) Only `main`, because it is the default
  target every working copy starts with, and only in an empty repository,
  because that is the one case where its absence cannot be a typo. When a land
  creates a ref the CLI prints `created ref X` on stderr, so `--json` stdout
  stays one document; with `--create` it reads the ref first to know whether
  it existed.
- **Not changed.** The MCP `leep_commit` tool and the soak harness send no
  `create` and keep creating; `leep switch -c` and `leep branch` already create
  refs explicitly through `POST /v1/refs/{name}`.

## Consequences
- A mistyped `--ref` fails safely; making a branch by landing needs `--create`.
- `leep land` onto a ref that was deleted while a working copy still targets
  it now fails with exit 4 instead of recreating it; `--create` recreates it.
- An old CLI against a new server, and a new CLI against an old server (which
  ignores the unknown field), both behave as before: the ref is created.
- A future `/v2` could make "do not create" the default; `/v1` cannot.
