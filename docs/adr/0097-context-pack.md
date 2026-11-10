# 0097. Context packs
Status: Accepted
Date: 2026-11-10

Builds on [ADR 0095](0095-coordination-state.md) (sessions) and
[ADR 0096](0096-overlap-notices-and-duplicate-flags.md) (overlaps and
duplicates).

## Context
An agent starting a task knows its task and its checkout. It does not know
that another agent is editing the same file right now, that a third has the
same issue, who owns the directory and whether a review there carries
across a rebase, who changed the file last and with which model, or that
the ref is red because of a land that touched it. The server knows all of
that already; each piece is one more call the agent has to think of
(`leep who`, `leep log`, `leep review status`, `leep checks ref`), and
agents skip optional steps.

The approved coordination design (sections B.5, B.7, D.1 and D.2, phase
1) adds one question, "give me the context for this task", answered from
data the server already has, with no model, and one MCP call that starts a
task and asks it.

## Decision

### 1. `POST /v1/context`
`ContextRequest {task, paths?, ref?, session?, task_key?, budget_bytes?}`
→ `ContextPack`. `task` is plain text of at most 4 KiB; `paths` at most 64
repository paths (no control characters); `ref` defaults to `main` and must
exist (404 otherwise); `session` defaults to the `Leep-Session` header. An
empty `task` takes the caller's session's title and approach, and is 400
without one. A read token may call it: it is a query, and it changes
nothing.

### 2. Sections
Each section reuses a query the server already answers:

| Section | Source | Bound on work |
|---|---|---|
| `files` | the request's `paths` (`seed`); a literal code search for identifiers in the task (`search`): words in backticks, `snake_case`, `camelCase` and `HTTPServer`-like words, at most 4, 50 matching lines each, 32 MiB scanned each, through the search index with stale answers allowed (ADR 0058); files changed by the same changesets as the seeds (`co_change`) in the seeds' newest path-history entries, 200 changesets in all over at most 8 seeds, each counted once, changesets of more than 1,000 paths left out | 64 files; 32 from search, 32 from co-change |
| `owners` | `OwnersResolver` at the ref's head: owners, the `OWNERS` files they come from, and the `carry` directive with its file (ADR 0092) | the first 32 files |
| `history` | `last-change` per file (path-history index, stale answers allowed), with the changeset's creator (`created-by-v1`, ADR 0086), author, title, `agent.model` and `agent.session` | the first 32 files; 8 reads at once |
| `in_flight` | the path and `landed` parts of ADR 0096's overlaps for the caller's session, probing the request's paths and the session's own first, then the pack's other files (`via: related`, ranked below; see Amendments); each other session once, at its highest level | 50 (`MAX_OVERLAPS`) |
| `ref_health` | `ref_checks` (ADR 0091): head, `green/<ref>`, `behind`, `red`, the required checks; when red, the suspects (`culprits`) whose paths meet the pack's files, with only those paths | 5 suspects, 8 paths each |
| `duplicates` | ADR 0096's duplicate part: live sessions with the same task key, or a declared title and approach at least 60% alike to the task | 20 |

Without a session, the pack stands one in for the request (id 0, never
stored): the caller's principal and session key, the task as its title and
approach, `task_key`, the ref as its target, the pack's files probed. It
has no base, so there is no `landed` part. Rows of the caller's own key are
left out of overlaps and duplicates, so a session the lookup missed (it
failed or ran out of time) is never its own duplicate. The request's
`task` and `task_key`, when given, stand for the task over the session's
own.

Required checks come from the server's checks policy as the request began;
without one, red is judged on every check name the server knows, as `GET
/v1/refs/{name}/checks` does.

### 3. A read, never an interruption
A pack never marks a notice as said and writes no row. Overlaps and
duplicates are computed by the same functions notices use, without the
per-session memory (`NoticeState::pick`), so an agent that asks for a
pack still hears each notice once on its next answer. The `landed` walk
keeps its progress cache (ADR 0096), which is not what was said. Nothing
in this change is on the land path, and land answers are unchanged, so the
land-equivalence property test of ADR 0095 needs no new case.

### 4. Items, budget and cuts
Every item carries an `id` and `bytes`, the exact size of its JSON. Ids
are stable and safe to print: `file:`, `owners:` and `change:` with 12 hex
digits of a hash of the path; `session:N`; `landed:` with a changeset
prefix; `ref:` for the ref. The items together stay within
`budget_bytes`: 32 KiB by default, clamped to 1 KiB..256 KiB. Each section
first takes the longest ranked prefix of its items that fits its quota
(files 25%, owners 10%, history 20%, in-flight 20%, ref health 5%,
duplicates 20%), then the budget left over is offered, in that order, to
ref health, in-flight work, duplicates, files, history and owners: what
only the server knows before what an agent can find itself, and first the
one small item that says whether the ref is red. When the ref health item
does not fit, its suspects are dropped, oldest first, until it does
(`cut` section `suspects`), so the red state is the last thing to go. A
section is always a ranked prefix, so cutting never reorders. `cut` lists,
per section, how many items and bytes were left out; `bytes` is what was
kept. A session that is both a possible duplicate and live work on the
pack's files is listed once, under `duplicates`, with the paths, so it is
never charged twice. Agent-written names (models, session keys, owner
names) are cut to 80 bytes and an owners item lists at most 16 names and
16 `OWNERS` files, so one long value cannot take a section's quota.
The envelope (ref, head, `cut`, `omitted`) is outside the budget and
bounded by the number of sections.

### 5. Deterministic
The same request against the same state is the same bytes: every list is
ranked by a total order (files: seeds in request order, then by matches
plus co-changes, then by path; overlaps by level, then as `near_paths`
orders them; duplicates by task key, then similarity), maps are ordered,
and nothing depends on the time of the call. A test asks four times and
compares bytes.

### 6. Bounded, and partial rather than failed
Search and co-change run together, then the other five sections run
together; each section has 1 s (`SECTION_BOUND`), the caller's session
lookup 250 ms, and the whole call 2 s (`CONTEXT_BOUND`). A section that
fails or runs out of time is listed in `omitted {section, reason}`
(`timeout`, `error`, or `off` when the server keeps no sessions) and the
rest still comes. Only a bad request or a missing ref fails the call. The
route takes one `search` read-class permit (ADR 0060), since it runs a code
search, and the per-token limits of ADR 0020 apply. That one permit covers
the whole pack: its path-history pages, last-change reads, ref checks and
culprits take no `log_filtered` permit of their own, so a server with
`log_filtered=1` can run as many of those as it has `search` permits for
packs. The cost under the permit is bounded: at most 128 MiB of blobs
scanned (four searches of 32 MiB, against a plain search's 256 MiB) and 2 s.
With `search=off` only the per-token rates bound packs. A full class sheds
a pack with 503 and `Leep-Refused: read-class`; the CLI sends it again as
it does a search, and the MCP tools report it as a retryable error (a
router test checks the shed). Tests on a paused clock
check the bound with a coordination store that never answers.

### 7. Untrusted text
Everything an agent or author wrote is under `untrusted` in every item:
paths, commit titles (cut to 80 bytes), authors, models, agent session
keys, owner names and `OWNERS` file paths, a suspect's paths, and in-flight
and duplicate notices' `NoticeText`. Outside it are only what the server
vouches for: ids, sizes, levels, `why` words, counts, changeset ids,
principals (token names), times, `carry` words and check names from the
policy. A test writes newlines, ANSI and OSC sequences, a bidirectional
override and U+2028 into paths, titles, authors, models, owner names and
session text, then strips every `untrusted` object from the pack and finds
none of them. The CLI prints every such string quoted and escaped (`{:?}`);
the MCP tools print outside the fence only ids, levels, plain words and
counts, and put the rest, one JSON row per item keyed by its id, in the
fence of ADR 0095.

### 8. Clients
- **MCP.** `leep_task_start {task, task_key?, paths?, ref?, budget_bytes?}`
  declares or resumes the session (the task's first line its title, the
  rest its approach, `paths` its scope) and answers the pack for it, in one
  call (design D.1). `leep_context` answers the pack alone, for the
  agent's session when it has one. The review tools the design's loop
  needs, which ADR 0086 gave only the CLI, come with them:
  `leep_review_request {rev, reviewers?, message?, ref?}` (`POST
  /v1/review-requests`, as `leep review request`) and `leep_review_inbox
  {mine?, all?, limit?}` (as `leep review inbox`: open requests naming the
  caller that it has not answered). 24 tools.
- **CLI.** `leep context [--task T] [PATHS...] [--task-key K] [--ref R]
  [--budget N]` and `leep work start` (`leep session start`, then the pack
  for the declared title, approach, scope and claims).

### 9. Measuring it
`leep_context_packs_total`, `leep_context_bytes_total` (bytes of items
returned), `leep_context_items_cut_total{section}` and
`leep_context_sections_omitted_total{section, reason}`. Their ratio to
`leep_sessions_started_total{origin="declared"}` is roughly packs per
session.

## Consequences
- An agent's first call returns live work, owners, recent history, ref
  health and possible duplicates for its task (the phase 1 demo), with no
  change to how anything lands.
- One route and its types; no table, column, index, stored encoding or
  hashed type changes. `/v1` only grows.
- Each pack costs up to 4 searches, 8 path-history pages, 32 owners walks
  (cached per directory) and 32 last-change reads, one `near_paths` call
  and two session listings, within 2 s and one search permit. An agent
  calling it once per task costs little; one calling it in a loop is
  bounded by its token's rate limits and the search class.
- The pack is advisory and partial by design: a section past its bound is
  missing, and says so.

## Not in this change
Notes (design A.5, phase 2) and issues (A.6, phase 3) have no data yet and
are not sections; symbol definitions (B.3, phase 3) are not a seed. No
code text, embeddings or semantic retrieval: agent harnesses read code
locally, and the pack holds what only the server knows (design B.5). The
fuzz target's JSON types do not include the new shapes, as ADR 0095 notes
for sessions. A per-section budget the caller sets, and pages of a cut
section, wait for a measured need.

## Alternatives considered
- **Compute the pack in the client** from the existing routes. Ten or more
  round trips, each with its own failure, and no shared bound; agents
  would not do it.
- **Reuse `GET /v1/sessions/{id}/overlaps`** for in-flight work. It needs a
  session, and probes only the session's paths, not the files the task
  turned up.
- **Mark the pack's notices as said.** An agent that reads a pack has seen
  them, but one that drops the pack would never hear them; a read should
  not change what later answers say.
- **Cut by item count.** Items vary from 100 bytes to several KB (a
  suspect with paths); a byte budget is what a context window spends.

## Amendments

### Levels come from the agent's own paths (2026-11-10)
The phase 1 demo (docs/DEMO-COORDINATION.md) showed packs contradicting
notices: the pack probed co-changed and searched files as if they were the
agent's own, so rex's pack called sol `same_file` on `handlers.rs` "(your
`src/api/handlers.rs`)" while the notice in the same output said `near`,
and bo was told ada works on "your `keys.rs`", a file that only changed
with bo's. Now an overlap's level is ranked first by the request's paths
and the session's own (scope, claims, touched): another session met on
one of those takes the best level there, whatever it also meets on a
related file. A session met only on a related file (a `co_change` or
`search` file of the pack) keeps the level it has there but carries
`via: "related"` (a new optional `Notice` field, absent otherwise), its
`untrusted.path` is that related file, and it ranks below every overlap on
the agent's own paths whatever the levels. A related file never makes a
crowded-path count. Notices never carry `via`: they probe only the
session's and the request's own paths, so pack and notice now agree on
every session they both name. The CLI prints such an item as "(a file
related to your task: ...)" instead of "(your ...)"; the MCP tools add "on
a file related to your task, not yours" and key the fenced path
`related_path` instead of `your_path`.

