# 0078. Checkpoint long blames and finish them past a timeout
Status: Accepted
Date: 2026-11-03

## Context
On a 1M-changeset linear history, a cold blame of a hot file (about
10,000 changers) took about 61 s. The server's request timeout is 25 s.
Every attempt answered 408, and every retry started again from nothing,
so the blame never finished:

- `blame_run` builds the whole graph of file versions (`blame_graph`,
  ADR 0065), replays it oldest first (`replay_graph`), and caches the
  answer only at the end. During the replay the only rows it wrote were
  the complete blames at the starts of walks below merges and renames. A
  linear history has none.
- `TimeoutLayer` drops the handler's future. The replay ran in that
  future, and the stretch walks are tasks that abort when they are
  dropped. So a timeout threw all the work away, and nothing was saved.

## Decision
- **Checkpoints during the replay.** The replay goes in generation order,
  so once a version is replayed, its lines are the complete blame of its
  path at its changeset, unless a bound cut it or anything it was made
  from (`complete`, as before). After every `CHECKPOINT_EVERY` (256)
  versions replayed, the next complete version on the blamed changeset's
  first-parent line (followed through renames) is written under the
  normal `blame-v2` key of that changer and path. The write goes through
  the same path as the blames already cached at walk starts. The
  encoding, the key and the kind do not change. The row is the blame a
  request for that changeset would compute and cache, so any later blame
  can use it.
  - A later walk stops there. `walk_stretch` (and the reference walk)
    look up the cached blames of the changers they find, 32 at a time, and
    end at the newest hit. The replay treats that version as a leaf. So a
    blame cut short walks and replays only the versions above its newest
    checkpoint.
  - The line is where a later walk from the top looks first. Side walks
    already have their starts cached.
  - The spacing is counted from the start of each replay, and a resumed
    replay starts at a checkpoint. So a file's checkpoints stay about one
    per 256 versions, however many blames run on it. One uninterrupted
    blame of V versions writes `floor((V - 1) / 256)` checkpoints. The top
    itself is cached by `blame_run`, as before.
  - Why 256. A row holds the whole blame of the file: the lines with their
    text, about the size of the file plus 36 bytes a line. On local
    Postgres, a whole blame of 3,000 versions of a file of 1,000 to 2,500
    lines takes about half a second, so 256 versions are a small part of
    a second of lost work. A spacing of 64 would mean four times the rows
    and the writes. Once blames are detached (below), that buys nothing
    in the usual case: a checkpoint only matters when a blame really is
    cut short, by a restart or by a blame that had to run inline.
- **Detached blames, one per (changeset, path).** `Repo::blame_held` runs
  `blame_lines` on a task of its own (`blame_flight`). A map keyed by
  changeset, path and whether the blame is bounded (ADR 0058, whose
  answer may be incomplete) holds the running blames. A request for a
  blame that is already running joins it and waits for its answer. When
  the request is dropped, by a timeout or by its client, the task goes on,
  caches its answer and its checkpoints, and ends. So a client that got
  408 and sends the request again joins the work in progress, or reads
  the cached answer if the work is done. Nothing is cancelled. Errors are
  shared too, with their kind (`NotFound`, `Invalid` and so on), so every
  waiter gets the same status.
- **Admission still holds (ADR 0074).** The read-limits middleware puts
  the request's permit in the request's extensions as a `ReadPermit`
  (an `Arc` around the semaphore permit), as well as keeping it in the
  response body. The blame handler hands a clone to the detached task
  (`BlameHold`). The semaphore slot is freed only when both the response
  and the task are done. A blame that outlives its request still counts
  against the `blame` class, so timeouts cannot be used to get around
  load shedding. A retry that joins a running blame takes a permit of its
  own while it waits, as a request waiting on the indexer does.
  - The task drops its hold and leaves the map before it publishes its
    answer. So a request sent after the answer arrives is not refused for
    a permit that is about to be freed, and it reads the cached row.
  - `leep_read_class_in_flight` still counts requests, not detached work.
    The permits, and so the shedding, count both.
- **A bound of their own.** At most `2 × max(2, cores)` blames run
  detached per process. On a server each one also holds a `blame` permit,
  which is the tighter bound by default. The slots bound the `blame=off`
  case and embedders that have no read classes. When every slot is
  taken, a blame runs inline, as before: it ends with its request, and its
  checkpoints are what survive.
- **Metrics.** The gauge `leep_blame_detached` (blames running
  detached) and the counter `leep_blame_joined_total` (requests that
  joined one).

## Consequences
- On local Postgres (`blame_past_timeout`, PERFORMANCE.md "Long blames
  past the request timeout"), a 3,000-version hot file sits in a
  12,000-changeset linear history, and a cold blame takes 0.55 to 0.62 s.
  With the request timeout at 40% of that, the attempts go 408, 408, 200,
  and the answer comes 0.53 to 0.57 s after the first one, about the cold
  time. Before this change every attempt timed out. A process killed after
  5 of the blame's 11 checkpoints is followed by a new one that answers in
  0.42 s, against 0.54 to 0.57 s from nothing. One blame writes 12 rows
  (773 KB).
- A cancelled or timed-out blame now keeps its permit until it finishes,
  so a class can stay full for up to one blame's length after its clients
  have gone. That is the work the server is really doing.
- Rows: one extra `blame-v2` row per 256 versions of a blamed file, each
  the size of one blame. `repair drop --kind blame-v2` and GC treat
  checkpoint rows like any other cached blame.
- A request that joins a bounded blame reports none of its scanned
  changesets in `Leep-Scanned`. The request that started the blame counts
  them.
- The detached task runs on the server's runtime. A shutdown drops it.
  Its checkpoints remain.

## Testing
- `blame_tests::an_interrupted_blame_resumes_from_its_checkpoints`: 600
  versions with checkpoints every 64. A blame stalled at its fourth
  checkpoint write is dropped. The next blame, in a new process, replays
  at most 344 versions, reads less than a blame from nothing, and gives
  the reference walk's answer. Uninterrupted, the blame writes
  `floor(599 / 64)` checkpoints. Without checkpoints, the answer is the
  same and only the blame's own rows are written.
- `blame_tests::concurrent_blames_share_one_computation_that_outlives_its_callers`:
  eight concurrent requests share one computation (one set of row writes)
  that goes on after the request that started it is aborted, and keeps
  that request's hold to its end. A blame dropped at once still caches
  its answer. With the one slot taken, a blame runs inline and ends with
  its caller.
- `blame_tests::blame_matches_the_definition_across_merges_and_renames`
  writes checkpoints every one to three versions, and its cached rounds
  still match the definition over merges, octopus merges and renames.
- `crates/leep-server/tests/read_limits.rs`: a cancelled or timed-out
  blame keeps the class's permit until the detached blame ends, then gives
  it back.
