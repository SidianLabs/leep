# 0035. Short changeset ids
Status: Accepted
Date: 2026-10-17

## Context
Every CLI command prints 12-digit ids, but no command accepted them back:
`leep show 2a954d8ef7c7` was a 404 (`docs/research/CLI-UX.md`, R1). People
and agents copy what the CLI printed, and abbreviated hashes are git's most
used convenience. The CLI holds no history, so it cannot expand a prefix
itself; the server has to.

## Decision
- **Endpoint.** `GET /v1/resolve-prefix/{prefix}` (new, additive). `prefix` is
  4 to 64 hex digits in any case. Exactly one changeset whose id starts with
  it: 200 `{"id": ...}` (`ResolvePrefixResponse`). None: 404. Several: 400
  with `AmbiguousPrefixResponse` `{"error", "candidates", "truncated"}`,
  listing up to 10 matching ids in ascending order. A prefix shorter than 4,
  longer than 64 or not hex: 400. It is a `GET`, so read tokens may call it.
- **Why 400 for ambiguity.** The request does not identify one changeset,
  which is a caller problem like any other `Invalid` (400) in Leep, and
  retrying the same prefix never helps; Leep's 409 means a concurrent update
  that a retry can win, and 300 would be read as a redirect. The body is a
  superset of `ErrorResponse`, so a client that only knows `error` still
  shows the message.
- **Why a separate endpoint**, not a fallback in `/v1/resolve/{rev}`: the
  existing endpoint's 404 for unknown names stays exactly as it was, and a
  client can tell "no such ref" from "ambiguous id" without parsing text.
- **Store.** `MetaStore::changesets_with_prefix(prefix, limit)` returns up to
  `limit` ids that start with the prefix, ascending. `leep_core::prefix::IdPrefix`
  validates the text and turns it into the inclusive byte range
  `[prefix 00.., prefix ff..]` (an odd digit count fixes the high nibble of the
  last byte). The provided default reads one page of `list_changesets` from
  the id just below the range and keeps the matching ones; SQLite and
  Postgres override it with `SELECT id FROM changesets WHERE id >= lo AND id
  <= hi ORDER BY id LIMIT n`, a bounded range scan of the primary key. The
  server asks for 11 to tell one from several and report up to 10. The
  contract (`check_prefix_lookup`) checks even and odd lengths, limits, the
  edges of the id space and a prefix two stored ids share. No table, index or
  encoding changed.
- **CLI.** Wherever a revision is accepted, `Remote::resolve` tries `@`, a full
  64-digit id, then the server's `/v1/resolve/{rev}` (refs, ids, tags), and
  only when that is a 404 and the text is 4 to 64 hex digits,
  `/v1/resolve-prefix`. A ref therefore always wins over a short id that spells
  the same (a branch named `cafe`). An ambiguous prefix is exit 1 with the
  `ambiguous` code, the candidates on stderr and in the `--json` error's
  `candidates` (ADR 0034). An unknown one is the usual not-found (exit 4) with
  a hint. Against a server without the endpoint a short id stays a 404.
- **Printing.** `leep log` headers print the 12-digit short id, which is what
  the other commands already printed; `--json` keeps full ids everywhere, and
  `show` and `last-change` keep the full id in their header.

## Consequences
- What the CLI prints is what it accepts. Twelve digits are unique in any
  realistic history (2^48 values); if one ever is not, the user gets the
  candidates and adds digits.
- One more round trip for a short id (the ref lookup comes first), none for
  refs or full ids.
- A new backend gets correct prefix lookup from the default and can override
  it for speed.
