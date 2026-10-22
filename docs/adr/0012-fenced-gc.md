# 0012. Fenced two-phase blob garbage collection
Status: Accepted; the mark of both phases is incremental, and collect reads
condemnation rows in batches, since ADR 0033. Amended 2026-10-22 (ADR 0059
section 11): `gc` refuses, rather than warns about, a `--fence` the request
and statement timeouts it is given do not cover, unless `--unsafe-fence`;
and condemn and collect hold the store-wide `gc` lease, so "no two GC runs
overlap" is enforced instead of assumed
Date: 2026-10-15

## Context
ADR 0008 deletes a blob after checking, immediately before the delete, that no
claim is newer than the grace window. A claim, upload or changeset that lands
between that check and the delete loses the blob: the window is microseconds,
but nothing bounds it, and the loss is silent corruption (a stored changeset
pointing at a missing blob). Closing it needs the delete to be ordered against
every request that might rely on the blob, and neither filesystems nor object
stores offer a delete conditional on metadata.

## Decision
GC becomes condemn, fence, collect. The server refuses to rely on a condemned
blob, the fence outlasts every request that checked before the condemnation,
and the final delete is ordered against uploads by a two-flag handshake in
meta.

### State (derived data, all keyed by blob hash; no format change to existing data)
| Kind | Value | Written by |
|---|---|---|
| `blob-condemned-v1` | unix seconds, 8 bytes big-endian, from the GC host's clock | condemn |
| `blob-rescue-v1` | the condemnation time the writer observed, 8 bytes big-endian | uploads of a condemned blob; collect, for blobs a changeset references |
| `blob-doom-v1` | condemnation time (8 bytes big-endian), then one state byte: 0 deleting, 1 abandoned, 2 deleted | collect |
| `blob-claim-v1` | unchanged (ADR 0008); now also written by every upload | `missing_blobs`, `put_blob` |

A blob is **condemned** iff its condemnation time is strictly greater than its
rescue value (or it has no rescue row). A condemnation is **blocked** while it
is condemned, or while its doom row names the current condemnation time with
state *deleting*. Absent rows mean "not condemned". Rows cannot be deleted, so
they accumulate harmlessly: a re-condemnation is written strictly later than
the previous one (`max(now, previous + 1)`), which makes any older rescue
stale by construction.

The rescue value is the observed condemnation time, not the server's clock.
Using "a claim newer than the condemnation" as the rescue was rejected:
derived rows are last-writer-wins, so a slow request that read "not condemned"
could overwrite a rescue with an older claim time, silently re-condemning a
blob a client had just been told was safe. Rescue writers only write when they
saw a condemnation, and all of them write the same bytes. Server clocks play
no part in the fence.

### Phases
1. **Condemn** (`leep-server gc condemn`). Mark every stored changeset's
   reachable blobs, then for each unmarked blob older than
   `started_at - grace` with no claim at or after that cutoff, write
   `blob-condemned-v1` unless it is already condemned (its earlier time
   stands). Deletes nothing. `--dry-run` writes nothing.
2. **Fence.** Wait `--fence` (default 10m; 15m since 2026-10-21, below). `leep-server gc` sleeps; operators
   who prefer short processes run `gc condemn` and, later, `gc collect`.
3. **Collect** (`leep-server gc collect --fence 15m`). Record `started_at`,
   mark again, then for each blob with a condemnation row whose time is at
   most `started_at - fence` (younger rows are left for a later run):
   - not condemned any more (rescued): skip;
   - marked: write the rescue (so clients can reuse it) and skip;
   - store modification time newer than the condemnation: skip (a write that
     recorded no rescue);
   - otherwise: write doom *deleting*, **re-read** condemnation and rescue; if
     rescued, write doom *abandoned* and skip; else delete the blob and write
     doom *deleted*.

   A crash leaves rows that the next run resolves: condemned blobs stay
   condemned (reported missing, rescued by re-upload, re-condemned or
   collected later), and a *deleting* row left by a crashed collect is
   completed by the next collect.
4. `gc` refuses a zero fence unless `--unsafe-no-fence` (only valid with no
   server running). Phases are separate processes: collect derives "condemned
   by which run" from the stored times, so there is no run state.

### Server behavior
- `POST /v1/blobs/missing` reports a blocked blob as missing and does not claim
  it. Fence read first, then existence.
- `PUT /v1/blobs/{hash}` (`Repo::put_blob`) stores, writes a claim, and if the
  blob has a condemnation row writes the rescue and reads the doom row. On
  *deleting* it polls (up to 30s, then 409) until the state changes; on
  *deleted* it stores the bytes again, so the 204 means "present and safe".
- `POST /v1/changesets` (`Repo::create_changeset`): for each `Modify` content,
  a blocked blob is rejected exactly like a missing one (400 "missing blob");
  the client re-uploads and retries. The fence is read before the existence
  check.
- Tree nodes are written through the same rescuing write
  (`Repo::put_rescuing`), since a server-built tree node may equal a condemned
  orphan.
- Server-built changesets (`merge`, `rebase`, `land`) skip the condemned check
  because their blobs are (a) referenced by stored changesets, which every
  mark covers, or (b) the merge result blob, written with `put_blob`. The
  only server path that writes a new file blob is the merge in
  `leep-derive::merge`; `leep-git import` also uses `put_blob`. Nothing else
  calls `BlobStore::put` for blobs a changeset may reference.
- All of these read durable meta directly. `CachedBlobStore::contains` already
  bypasses the cache; derived rows are never cached.
- `--request-timeout` (env `LEEP_REQUEST_TIMEOUT`, default 5m, 0 disables)
  adds a tower-http `TimeoutLayer`: a request still running after the timeout
  is aborted with 408 and performs no further writes.

### Safety argument
Let a blob B be deleted by collect run C, condemned at time c, with C starting
(mark begins) at m >= c + fence. Suppose a stored changeset X references B
(directly, or through a tree). We show a contradiction in every case. X was
stored by `create_changeset`, `store_server_changeset` or an earlier
version, in a request R that relied on B.

Assume `fence > request timeout + clock skew + 1s`, `grace > request
timeout`, and no two GC runs overlap.

(a) X was stored before m. Then X is in the mark of C, B is marked, and C
skips it. Contradiction.

(b) R's last read of B's fence state preceded c (so R saw "not condemned").
R's changeset write happens within the request timeout of that read, so before
`c + timeout < c + fence <= m`. This is case (a). The same holds for tree
nodes and merge blobs, whose "read" is the rescuing write.

(c) R read B's fence state after c and X is stored at or after m. Then R saw B
unblocked: not condemned, so a rescue with value >= c exists, and no *deleting*
doom row for c. Collect's per-blob order is: write doom, then read the rescue.
The rescue was written by an upload U (or by collect itself, in which case
B was marked, case (a)). U's order is: write the rescue, then read doom. In
any total order of the four meta operations, either U's doom read follows
collect's doom write, or collect's rescue read follows U's rescue write
(otherwise each read would precede the other's write in a cycle). If collect's
rescue read follows U's write, collect sees the rescue and abandons: B is not
deleted. If U's doom read follows collect's doom write, U sees *deleting* and
waits; it returns only after collect finishes, then writes the bytes again if
the delete happened, so B exists when U returns. A request that relied on B
through U's rescue before that (the "missing then create across the boundary"
case) reads *deleting* as blocked, or reads *deleted/abandoned* after the
delete finished, and its existence check follows its fence read, so it sees the
bytes missing and rejects, or sees them restored. Either way B is not deleted
out from under a changeset that will be stored. Contradiction.

(d) R read the fence state after c, saw B blocked, and rejected. R stores
nothing.

`missing_blobs` then `create_changeset` across the condemnation boundary: a
"present" answer before c is a case (b) request (its create is checked again
and either passes before c, fenced, or sees the condemnation and fails). A
blob condemned between the two calls makes the create fail with "missing blob"
and the client re-uploads.

Multiple servers: every check reads shared meta; the request timeout applies
per server; the proof never used server-local state. The blob cache: a cached
blob can be served to readers after deletion, but only blobs no changeset
references are deleted, and `contains` (the only existence test used by
checks) asks the durable store.

Clock skew. The fence compares only GC-host timestamps: condemnation times
(GC clock) against `started_at` (GC clock), so skew between GC and servers does
not enter the comparison. Skew enters through the real-time bound in (b): the
request timeout is measured on the servers, the fence on the GC host, so run
with `fence >= timeout + skew + margin`; the default 10m against 5m leaves five
minutes. (Superseded for Postgres by "Statement bound" below: the fence must
also cover the statement timeout, and the default is 15m.) The store modification time check compares the blob store's clock with
the GC clock and is a second line of defence only: a skewed result keeps a
blob or defers to the rescue rows, and never deletes a blob a rescue protects.
Condemnation times never go backwards for one blob, even if the GC host's clock
steps back.

### What remains (not safety)
- Requests may take up to the timeout; a legitimate 512 MiB upload over a slow
  link can exceed 5m. Raise `--request-timeout` and `--fence` together.
- A statement already sent to Postgres when a request is aborted can still
  commit shortly afterwards. The fence margin absorbs this; do not run with
  zero margin.
- A server paused for longer than the request timeout (VM freeze) can resume a
  request after its deadline: the timeout bounds running time, not wall time
  across a suspend. Treat the margin as covering this, or restart servers
  after a long freeze.
- If GC is killed between "doom deleting" and the delete, uploads of that one
  blob answer 409 until `gc collect` runs again.
- Collect reads one meta row per blob (to find condemnations): O(blobs) point
  reads, in addition to the changeset scan.

## Consequences
- ADR 0008's residual race is closed under the stated assumptions; its other
  parts (mark, grace, listing traits) stand.
- A condemned blob that a client believed present costs the client one extra
  upload and retry, never a corrupt changeset.
- Uploads add one meta write (claim); uploads and tree-node writes add one
  meta read.
- A server built before this ADR does not read the new rows, so mixed fleets
  must be upgraded before the first condemn.
- Operators run `gc` from cron as before; the command now takes at least
  `--fence` to finish, or run `condemn` and `collect` as two scheduled jobs.

## Security review 2026-10
See [SECURITY-REVIEW.md](../SECURITY-REVIEW.md). No request can make GC
delete a reachable blob: claims only keep blobs, rescues are written only by
uploads of the exact bytes, condemned blobs are refused in changesets, and
derived indexes live in metadata, not the blob store. No client can block a
collect; an upload waits at most 30 s and then gets 409. Read tokens can
write claims through `POST /v1/blobs/missing` and so keep known orphans for
as long as they keep asking (S-11, accepted; see ADR 0007's note).

## Statement bound, 2026-10-21
"A statement already sent to Postgres when a request is aborted can still
commit shortly afterwards" understated it: sqlx sends no cancel request when a
query's future is dropped, and Postgres does not notice the client leaving, so
the statement ran to its end however long that took (a test in `leep-meta`,
`postgres_a_dropped_query_keeps_running_without_a_statement_timeout`, shows it).
The server now sets `statement_timeout` on its data pool to
`--meta-statement-timeout`, by default `--request-timeout` plus 5 s (the margin
lets the request's own 408 come first), so Postgres cancels such a statement.
Lease connections, migrations (`SET LOCAL statement_timeout = 0`) and the
command-line tools are not bounded.

For case (b) this turns an unbounded tail into a bound: a write sent before the
request's deadline commits at most `request timeout + statement timeout` after
the request's fence read. The invariant GC now relies on is therefore

    fence > request timeout + statement timeout + clock skew

with the statement term on Postgres only (SQLite statements run in-process and
end with the request). With the defaults that is 5m + 5m 5s + skew, so the
default `--fence` is now **15m** (it was 10m, which held only for statements
that finish within the request). `gc` evaluates the invariant with the
`--request-timeout` and `--meta-statement-timeout` it is given (the flags are
global, and their `LEEP_` variables apply, so run it with the servers'
environment), allowing one minute of skew, and logs a WARN naming the numbers
when `--fence` does not exceed them, or when either bound is off, since then no
fence is safe. It warns rather than refuses because those are the servers'
settings, which `gc` can only be told. Deployments that want a shorter fence can
set a short `--meta-statement-timeout` instead (a request's statements take
milliseconds; `30s` gives `fence > 5m 30s + skew`). `repack --fence` keeps its
10m default: it guards reads of loose copies, which statements do not make.
