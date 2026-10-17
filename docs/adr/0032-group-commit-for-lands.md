# 0032. Group commit for lands
Status: Accepted
Date: 2026-10-16

## Context
Teams run many agents that land onto the same ref. Within one server process
lands on a ref were serialized by a per-ref lock (ADR 0004 and the soak work of
ADR 0022), so they never lost the CAS to each other, but they queued.
`leep-land-bench` (`docs/PERFORMANCE.md`, "Concurrent lands on one ref") showed
throughput falling as agents were added: on Postgres 146 lands/s with 4 agents,
78 with 16 and 41 with 64 (p99 1.7 s), with no CAS loss and no retry at all.

The cost was the work each land did under the lock, which grew with the
number of agents. A land whose changeset is not a descendant of the head first
looks for a copy of its stack that an earlier attempt landed but whose answer
was lost (ADR 0021), walking the head's ancestors down to the changeset's
generation and reading each one's `rebased-origin-v1` row and parents. With N
agents a changeset sits about N lands behind the head, so each land read about
2N rows, one round trip each. A build with the scan disabled kept SQLite at
131 lands/s at 64 agents, against 51 with it. Every land also read the ref and
committed its own CAS transaction.

## Decision
- **Queue and leader.** `Repo` keeps one queue per target ref in the process.
  `land` checks the request (ref name, not a tag, changeset exists), appends it
  to the queue and, if no leader is running for that ref, spawns one. The
  leader takes up to 64 queued lands, lands them as a batch, and repeats until
  the queue is empty. It is a spawned task, so a cancelled request never
  cancels the batch other requests wait on. A leader that panics clears its
  queue, so waiting lands get an error (500) instead of hanging, and the next
  land starts a new leader.
- **Sequential semantics.** The batch reads the head once and replays each
  land in arrival order on the head the previous one left: exactly what one
  attempt of a lone land did, with the ref read replaced by the running head.
  Fast-forwards, replays, already-landed and landed-copy answers, policy
  refusals and conflicts all come out as they would if the lands ran one after
  another. A land that conflicts or is refused is answered alone and the next
  one replays onto the last good head; nothing is resolved on the server.
  Lands answered before anything in the batch moved the ref (an already-landed
  changeset, a conflict against the real head) are answered at once.
- **One swap, one log entry per land.** The batch then moves the ref once,
  from the head it read to the last land's result. The ref log keeps one entry
  per land that moved the ref, with that land's actor and `landed <id>`
  message, chained so each entry's `old` is the previous land's result. The
  log is read per entry: `leep undo` reverts the latest entry, `undo --to SEQ`
  a chosen one, and an entry names who landed what; a single entry for the
  batch would make one agent's undo revert everyone's lands and could carry
  only one actor. To keep the move atomic, `MetaStore` gains
  `cas_ref_logged_chain(name, expected, steps)`: if the ref is `expected`, it
  moves it to the last step and appends one entry per step in the same
  transaction (SQLite, Postgres: one ref statement, the advisory lock on
  Postgres as before, one multi-row insert) or under the same lock (memory).
  It returns how many steps applied. The provided default applies the steps as
  consecutive `cas_ref_logged` calls, which is correct but not atomic and can
  stop part way; the leader handles a partial answer by settling the lands
  whose moves applied and redoing the rest. A batch of one uses
  `cas_ref_logged`, so a lone land makes the same statements as before.
- **Other writers.** A lost swap (another server process, an import or a raw
  ref update moved the ref) redoes the whole batch from the new head, after
  the same jittered backoff, up to the same 32 attempts; `attempts` in each
  answer is the batch's attempt count. A commit that errors is redone from a
  fresh read, which finds each land that did apply (its changeset or its copy
  is in the head's history); three errors in a row are returned to every land
  in the batch. Exhaustion answers every land with 409 as before.
- **Cancellation.** A land whose caller went away (request timeout, client
  disconnect) before the leader reached it is dropped from the batch
  (`leep_land_cancelled_total`). Once the leader has replayed it, it stays in
  the batch, including when the batch is redone, and lands; a retry of it
  finds it landed, as for a lone land cancelled after its commit.
- **Shared reads.** The landed-copy scans of one batch attempt share the
  `rebased-origin-v1` rows they read, and the scan takes parents from the
  process-wide ancestry cache. The rows are per batch so a land never relies
  on a read older than its batch.
- **No linger.** The leader takes whatever is queued when it looks. Measured
  waits of 0.5 and 2 ms did not raise throughput beyond the noise and added
  their full length to a lone land.
- **Metrics.** `leep_land_batch_size` (histogram, buckets 1 to 64) and
  `leep_land_cancelled_total`. `leep_land_lock_wait_seconds` keeps its name
  and now measures the time a land waits in the queue before its batch starts.

## Consequences
- On Postgres, 64 agents land 154 times a second instead of 41 (p99 403 ms
  instead of 1.7 s), 16 agents 181 instead of 78; a lone land is unchanged.
  SQLite is similar. `docs/PERFORMANCE.md` has the runs.
- What still grows with the batch is the replays: a batch of 64 answers its
  last land after 64 replays. The batch bound keeps that and the commit size
  in check.
- Across server processes nothing changed: instances still race through the
  CAS. A lost race now costs a whole batch's replays, so routing lands of a hot
  ref to one instance matters more than before (`docs/RUNBOOKS.md`).
- Every `MetaStore` should override `cas_ref_logged_chain`;
  `check_meta_store_contract` checks the chained log on every backend, and the
  default is tested against the same checks. The ref log's tables and
  encodings are unchanged; a reader cannot tell a batch from lands that ran
  one after another, except that their entries share a timestamp.
- A cancelled land may still land if its batch had replayed it, as a lone
  land could after its commit; clients already treat a lost answer as unknown
  and repeat the land, which is idempotent.
