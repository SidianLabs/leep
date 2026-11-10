# 0096. Overlap notices and duplicate flags
Status: Accepted
Date: 2026-11-10

Builds on [ADR 0095](0095-coordination-state.md) (sessions).

## Context
ADR 0095 made the server keep sessions: who is working on what, where,
before it lands. `leep who PATH` answers when an agent thinks to ask.
Agents rarely do: they read tool results, they do not poll (`leep-mcp`
handles one request at a time, ADR 0009), and they skip optional steps.
Two agents editing one file still learn of each other when the second
land conflicts, and two agents given one task when both land.

The approved coordination design (section B.2, B.4 and B.6, phase 1)
puts the answer where agents already look: inside the answers they get
anyway, as a few ranked, deduplicated notices. This ADR is that part:
path overlap levels, the `landed` level by path, and duplicate flags by
task key and by wording. The merge dry run (`conflict`), symbol levels
(`semantic`, `landed` by symbol) and embeddings are later changes.

## Decision

### 1. Levels
A notice says how another live session's work meets mine, lowest first:

| Level | Meaning | Source |
|---|---|---|
| `near` | a path of theirs is in the same directory as one of mine, without overlapping it | `session_paths` |
| `same_file` | a path of theirs equals one of mine, or is a directory holding it, or lies below a directory of mine | `session_paths` |
| `claimed` | as `same_file`, where their path is a claim | `session_paths` |
| `possible_duplicate` | the same task key, or a title and approach much like mine | `sessions` |
| `landed` | a changeset touching my paths landed on my target ref since my base | the target ref's first-parent chain |

"My paths" are the session's scope, claims and touched paths, at most
128 probed per answer: a request's own paths (a changeset's changed
paths, a declaration's scope and claims) first, then claims, scope and
touched paths. "Live" is a session stored `active` (an idle one counts;
one past its expiry does not). A session sees each other session once,
at its highest level. More than 50 sessions at `same_file` on one of my
paths make it crowded: one notice with a `count` replaces theirs (claims
are still named).

**Path levels** come from one call, `CoordStore::near_paths` (a new
trait method in the contract): an exact probe of my paths and every
directory above them, plus one range scan below each of my paths and
below each one's directory, each scan capped at 64 rows and the whole at
512, all on the `(repo_id, path)` index of migration 0009. Postgres runs
it as one statement with a `LATERAL ... LIMIT` per range, so one crowded
directory cannot crowd out the rest; SQLite runs one short statement per
probe. No new table, column or index.

**`landed`** needs the session's `target_ref` and a base or head. Its base
is its declared base plus every parent outside the session of the
session's own changesets, walked from its head (at most 32): a merge of
the target into the work counts, so work already merged is not reported.
The server walks the target ref's first parents from its head, at most
64 changesets, until it meets one of those, and reports each changeset
not carrying the session's `agent.session` that changed a path equal to,
above or below one of mine, naming the changeset. The answer is kept per
session until the ref or the session's head moves. This reads history,
not the coordination store, so it holds across servers and restarts; the
land path itself is unchanged.

**Duplicates** are flagged when a session is declared (start or update):
live sessions with the same `task_key` (an indexed lookup, at most 20),
and, among the 200 newest active sessions, declared ones whose title and
approach are at least 60% alike by trigram Jaccard similarity, with the
search index's trigram extraction (`search::trigram::extract`) over the
lowercased words. Intents shorter than 16 bytes are never compared.
Inferred sessions are not compared by wording: their titles are commit
subjects, and two commits to one file read alike. Tests fix the
threshold with a paraphrase that must flag and a near miss (same shape,
other words) that must not. This finds "the same goal, described alike"
and no more; differently described duplicates wait for embeddings.

### 2. Notices in answers
`Notice {level, session?, principal?, changeset?, count?, by?,
similarity?, untrusted {title, path?, other_path?, task_key?}}`. The
server vouches for the level, ids, principal and counts; titles, paths
and task keys are agent-written and live in `untrusted`. Titles are cut
to 80 bytes.

Answers carry `notices` (`#[serde(default, skip_serializing_if =
"Vec::is_empty")]`, absent when empty, so old clients and old bodies are
unaffected):
- `POST /v1/changesets` (`CreateChangesetResponse`): the creator's
  session, just updated from the changeset, its paths probed first;
- `POST /v1/land`: the land's session (the caller's `Leep-Session`, else
  the submitted changeset's `agent.session`). The answer is sent as
  `LandAnswer`, which flattens `LandResponse` beside `notices`, so its
  shape is unchanged and a client reading `LandResponse` ignores the
  field;
- `POST /v1/sessions`: the declared session, with duplicates;
- `GET /v1/sessions`: the caller's session named by `Leep-Session`;
- `GET /v1/notices` (new): only that, as `NoticesResponse`.

`GET /v1/sessions/{id}/overlaps` (new) answers `OverlapsResponse
{session, overlaps}`: everything overlapping a session now, every level
and duplicates, highest first, at most 50, nothing held back as said. It
answers with `--notices off` too: it is a question, not an interruption.

**Ranking, cap, once.** Highest level first; at most 3 per answer. The
request header `Leep-Notices` picks the lowest level carried (default
`same_file`, the design's minimum, F.3) or `off`. Each server keeps, per
session, the notices it has said, keyed by level and the other session
(or landed changeset), for 30 minutes, at most 64 per session and
100,000 sessions; a notice already said is not said again, and the ones
past the cap of 3 come on a later answer. With several servers a notice
can be said once per server.

### 3. Never an answer's cost or content
Notices follow the land-equivalence rule of ADR 0095. Nothing a land
decides reads them; they are computed after the answer is decided, in
the handler, and only added beside it. Every in-band computation runs
within 150 ms (`NOTICE_BOUND`); past it, or on an error, the answer goes
out without notices and `leep_notices_omitted_total{reason}` counts it.
The property test of ADR 0095 now computes notices and overlaps on every
create, declaration and land of the populated repository, checks some
were said, and still requires the same answers, refs and ref logs on all
three repositories. A test with a coordination store whose reads never
answer checks that creates, declarations and notice reads come back
within the bound without notices.

### 4. Clients
- **CLI.** The client gathers the notices any answer carries and every
  command prints them on stderr when it ends, one line each, with every
  agent-written part and every name quoted and escaped (`{:?}`), as ADR
  0095 does for sessions. `leep status` also asks `GET /v1/notices`, best
  effort. `leep who --overlaps` lists the overlaps of your session.
  `LEEP_NOTICE_LEVEL` is sent as `Leep-Notices`.
- **MCP.** Every tool result ends with the notices its answers carried;
  when they carried none and the agent has a session, the server is asked
  (`GET /v1/notices`) at most every 30 seconds. Outside the fence go only
  the level, ids, principal, changeset and counts (a level or name that
  is not a plain word shows as `?`); titles, paths and task keys go in a
  fenced `untrusted` block under the fixed line of ADR 0095. Structured
  results gain a `notices` list. New tool `leep_overlaps`.

### 5. Measuring it
- `leep_overlap_notices_total{level}`: notices said, by level.
- `leep_duplicate_sessions_flagged_total{by = "task_key" | "similar"}`.
- `leep_notices_followed_total{level}`: notices followed, within 10
  minutes, by a declaration whose scope and claims no longer cover the
  declared path the notice was about, or by ending the session.
- `leep_notices_omitted_total{reason = "timeout" | "error" | "full"}`.
- Land conflicts per 1,000 lands, from the existing land outcome metrics,
  compared between `--notices on` and `--notices off` (`LEEP_NOTICES`),
  which leaves notices out of answers and changes nothing else.

## Consequences
- The second agent on a file is told when it commits, before it lands;
  the second agent given a task is told when it starts; an agent whose
  file changed under it is told which changeset did it.
- Each create carrying `agent.session`, each land and each declaration
  costs one `near_paths` call (one statement on Postgres) and, for
  sessions with a target ref, a ref read and up to 64 changeset reads
  when the ref moved (cached otherwise); a declaration with a title adds
  two indexed listings without path rows. All within 150 ms or skipped.
- `/v1` grows: two routes, one header, optional fields on four answers
  and the `LandAnswer` envelope. Nothing persisted changes: no migration,
  no hashed type.
- Notices are advisory and can be wrong: a path that overlaps is not a
  conflict, and wording that matches is not the same task. The levels and
  the per-session minimum keep the noise down (design F.3); the follow
  and conflict-rate metrics are how to tell whether they help.

## Not in this change
The `conflict` level (a merge dry run between two sessions' heads, which
needs content snapshots), symbol levels, embeddings, the event stream and
webhooks, a per-session stored minimum level (the header serves for
now), `visibility = 'private'`, and the fuzz target's JSON types for the
new shapes (as ADR 0095 notes for sessions).

## Alternatives considered
- **Notices on every response** (a response header on any request with
  `Leep-Session`). Computing them on every read would cost a query per
  request; answers that write, plus `GET /v1/notices` polled at most
  every 30 s by the MCP server, reach the same agents for far less.
- **`landed` from the post-land task**, matching a land's paths against
  `session_paths` and queueing notices in memory. Cheaper per answer, but
  lost on restart and seen only by the server that ran the land. Reading
  the target ref's history is bounded, works anywhere, and is cached.
- **Similarity over touched paths too** (the design's B.4 signal 2).
  Paths already have their own levels; mixing them into the duplicate
  score would make agents in one directory look like duplicates. Left
  out until a measured case needs it.
- **A stored minimum level per session.** It needs a migration; the
  header gives each client the choice without one.
