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
at its highest level, carrying the paths a lower level named when the
highest has none (a duplicate that also claims my file is one notice).
More than 50 sessions at `same_file` or above on one path of theirs make
it crowded: one notice with a `count`, keyed by that path, replaces
their rows on it, however many of my paths it holds. Claims are still
named, and a crowd member that also meets me on an uncrowded path is
named there. Sessions of my own principal with my task key are my own
earlier sessions (an agent restarted under a new key) and are never a
notice.

**Path levels** come from one call, `CoordStore::near_paths` (a new
trait method in the contract): an exact probe of my paths and every
directory above them, plus one range scan below each of my paths and
below each one's directory, each exact path and each scan capped at 64
rows (claims first on an exact path) and the whole at 512, all on the
`(repo_id, path)` index of migration 0009. The answer is ordered claims
first, then exact rows, then by path, so sessions scoped to a directory
above my file never crowd out a claim on the file itself. Postgres runs
it as one statement with a `LATERAL ... LIMIT` per exact path and per
range; SQLite runs one short statement per probe. No new table, column
or index.

**`landed`** needs the session's `target_ref` (`main` when it has none;
see the amendments) and a base or head. Its base
is its declared base plus every parent outside the session of the
session's own changesets, walked from its head (at most 32): a merge of
the target into the work counts, so work already merged is not reported.
The server walks the target ref's first parents from its head, at most
64 changesets, until it meets one of those, and reports each changeset
that changed a path equal to, above or below one of mine, naming the
changeset; one carrying the session's `agent.session` is skipped only
when the server recorded the session's principal as its creator. Both
walks read ahead in batches (the history walk's `ReadAhead`, ADR 0030,
which uses the skiplists' ancestors). Progress is kept per session as
the walk goes, so a walk cut short by the bound resumes on the next
answer; a moved ref restarts the walk but keeps the base, and a moved
head recomputes both. A changeset that is gone (collected) ends the base
walk where it is; with no base known, there is no `landed` part. This
reads history, not the coordination store, so it holds across servers
and restarts; the land path itself is unchanged.

**Duplicates** are flagged when a session is declared (start or update):
live sessions with the same `task_key` (an indexed lookup, at most 20;
since the amendments below, on every answer that carries notices), and,
among the 200 newest active declared sessions (`SessionFilter.origin`),
those whose title and approach are at least 60% alike by trigram Jaccard
similarity, with the search index's trigram extraction
(`search::trigram::extract`) over the lowercased words. Intents shorter
than 16 bytes are never compared. Two intents that both name paths
(words with a `/`, or a name and an extension) and share none are not
alike: a template filled in with other files ("Fix flaky test in
<path>") is other work. Inferred sessions are not compared by wording:
their titles are commit subjects, and two commits to one file read
alike. Tests fix the threshold with a paraphrase that must flag, a near
miss (same shape, other words) and titles sharing a long path prefix
that must not. This finds "the same goal, described alike"
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
- `GET /v1/notices` (new): only that, as `NoticesResponse`. Like every
  answer that carries notices, it marks what it returns as said: a
  client that asks and drops the answer does not hear those notices
  again for 30 minutes.

`GET /v1/sessions/{id}/overlaps` (new) answers `OverlapsResponse
{session, overlaps}`: everything overlapping a session now, every level
and duplicates, highest first, at most 50, nothing held back as said or
marked said. It answers with `--notices off` too: it is a question, not
an interruption. Reading the session and each part waits at most 2 s
(`OVERLAPS_BOUND`); a part past it is left out.

**Ranking, cap, once.** Highest level first; at most 3 per answer. The
request header `Leep-Notices` picks the lowest level carried (default
`same_file`, the design's minimum, F.3) or `off`. Each server keeps, per
session, the notices it has said, keyed by the other session (with the
level said), the landed changeset or the crowded path, for 30 minutes. A
notice already said is not said again; another session is said again
only when it comes back at a higher level. The ones past the cap of 3
come on a later answer. Only the 64 best-ranked overlaps are considered,
and a said entry is never dropped before it expires, so a session with
more overlaps than that hears the first 64 once, never in a loop.
Sessions told nothing keep no entry; at 100,000 sessions the memory is
purged of expired entries at most once a minute, and while it stays
full nothing new is said. With several servers a notice can be said once
per server.

### 3. Never an answer's cost or content
Notices follow the land-equivalence rule of ADR 0095. Nothing a land
decides reads them; they are computed after the answer is decided, in
the handler, and only added beside it; a land's notices are computed
before the hooks that end its session start. The path, `landed` and
duplicate parts run together, each within 150 ms (`NOTICE_BOUND`); a
part past it, or one that fails, is left out and counted in
`leep_notices_omitted_total{reason}`, and the others still come. A
create waits at most 250 ms for its session update (ADR 0095) and then
at most 150 ms for its notices: 400 ms in the worst case, on a store
that is failing to answer.
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
  effort, giving up after 3 s (as the MCP server does). `leep who --overlaps` lists the overlaps of your session.
  `LEEP_NOTICE_LEVEL` is sent as `Leep-Notices`.
- **MCP.** Every tool result ends with the notices its answers carried;
  when they carried none and the agent has a session, the server is asked
  (`GET /v1/notices`) at most every 30 seconds. Outside the fence go only
  the level, ids, principal, changeset and counts (a level or name that
  is not a word of letters, digits, `_`, `-` and `.` shows as `?`);
  titles, paths and task keys go in a fenced `untrusted` block under the
  fixed line of ADR 0095, with line separators and bidirectional
  controls escaped. Structured
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

## Amendments

### The first of a duplicate pair is told too (2026-11-10)
Duplicates were looked for only when a session was declared, so only the
second of a pair heard of the first: in the phase 1 demo dee, who started
`BILL-42` first, heard eli only as `same_file` with no task key, in the
answer of the call that had already landed. The task-key part now runs on
every answer that carries notices (changeset creates, lands, `GET
/v1/notices`, listings, declarations): one indexed listing of live
sessions with the session's task key, at most 20, within the same 150 ms
as the other parts, and nothing at all for a session without a key. It is
said once like any notice, and a session already told about at a lower
level (`same_file`) is told again because `possible_duplicate` is higher.
The same principal with the same key is still its own earlier session
and never flagged. Duplicates by wording still wait for a declaration:
they read 200 sessions, and inferred titles are not compared.

### Undeclared sessions hear `landed` (2026-11-10)
An inferred session had no target ref, so it had no `landed` part. Clients
now name it on creates (`Leep-Target-Ref`, ADR 0095 amendment), and a
session that still has none (an older client) is walked against the
server's default ref: the new `--default-ref` setting (`LEEP_DEFAULT_REF`,
default `main`, the ref clients land on by default; the server had no
such setting before). A session whose work lands elsewhere and says so is
never walked against it. A `landed` notice found this way carries
`landed_on` (a new optional `Notice` field) naming that ref, and clients
say "landed on main", never "your target ref": the session never said it
was.

### A duplicate's land says so (2026-11-10)
In the demo eli heard "[landed] changeset ... landed on your target ref"
when dee, its duplicate on `BILL-42`, landed the task, and nothing said
whose land it was. The task-key lookup of the first amendment now reads
the key's live sessions (at most 40, the duplicates) and its landed ones
(at most 64) in two indexed listings run together, so however many
sessions of a reused key landed they never hide a live duplicate; and the `landed` walk
keeps, for each changeset it reports, its `agent.session` and its creator
(through server rebases; when that read fails, nothing, so the
writer-chosen `agent.session` alone never tags a land, and the walk's
cache keeps that). A landed changeset whose `agent.session` is that
of a session with my task key, created by that session's principal when
the server recorded one, and not of my own principal (my own restart), is
tagged: `by: task_key`, `session` and `principal` of that session, and
`untrusted.task_key`. It is still keyed by the changeset for saying once,
and it stands only for its changeset: each land is its own notice, and
the session's live work (a `possible_duplicate` or a path level) is still
said beside it. Clients add a hint: the CLI ends the line with
"your duplicate landed this task; check whether your work is still
needed", the MCP tools say the same outside the fence and keep the task
key in the fenced row. A context pack tags its `landed` items the same way
and keeps them as lands (`landed:` ids), never folded into the duplicate.

### The MCP land waits for what it should read first (2026-11-10)
`leep_commit_and_land` created and landed in one call, so an MCP agent
heard of overlaps on its commit only after it had landed (dee heard eli
in that answer, in the phase 1 demo). The maintainer chose a pre-flight
check: the create's answer already carries the session's notices (section
2), so the tool reads them before landing and stops when one of them
should be read first. With nothing to stop for, the call is the same one
create and one land as before: the check costs no request.

**What stops a land** is one function, `Notice::stops_land` in `leep-core`,
given the files the changeset changes:

| Level | Stops when |
|---|---|
| `possible_duplicate` | always |
| `landed` | its changeset changed a file of this change, or it is a duplicate's land (`by: task_key`) |
| `claimed` | the claim is on a file of this change or a directory holding it |
| `same_file` | the other session names a file of this change itself, not only a directory holding it |
| `near`, a level the client does not know, anything `via` a related file | never |

A directory someone scoped, `near`, and related files are too common to
stop work for; they still end the result as advisory notices. The paths
compared are agent-written (`untrusted`): they decide whether to stop and
are never shown outside the fence. So that a notice names the file both
sessions work on, two rankings prefer it: among one session's rows at the
same level the server keeps the one whose path equals mine, then my
earlier path (the request's own first), not the first in index order (a
directory sorts first); and the `landed` walk names a path of mine its
changeset changed itself before one met through a directory of mine.
Only which paths a notice carries changes; levels, counts and what is
said once do not.

**Only notices not shown before stop.** The server says each notice once
per session (section 2), so the notices that stopped a call never stop
another: an agent that read them and changes its work lands. A stopped call
has created its changeset but not landed it; the answer (`status:
stopped`, an error result) names it, lists the notices that stopped it in
the fence, and says how to go on: land it with `changeset` and `force:
true`, or change the work. The MCP server remembers the changesets it
stopped (64 per process): a call with the same changes, message, parents
and metadata reuses the stopped changeset, with `force` or without, so
retrying never makes a second one; without `force` it asks `GET
/v1/notices` for anything new and lands unless that stops it.
`leep_resolve_and_land` checks the same way, keeping its conflict
remembered for the retry with `force`. An agent whose stop answer was lost
can land without having read it; the notices are advisory, and the server
said them, so this is no worse than a lost notice before.

**The CLI needs none.** `leep commit` and `leep land` are separate
commands; the notices of the create are printed when `leep commit` ends,
before the agent lands, and `leep land` refuses uncommitted work.

Also in this change: `leep_task_start` asks once more for a context pack
the server shed (`Leep-Refused`, ADR 0060) when its `Retry-After` is at
most 2 s, so a task's first call carries its context on a briefly busy
server; a longer pause, or a second shed, is reported as before.
