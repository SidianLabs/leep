# 0024. Config reload without restart
Status: Accepted
Date: 2026-10-16

## Context
Tokens, trusted signing keys and the review policy were read once at startup
(ADRs 0007, 0010, 0011, 0020). Rotating a token, trusting a new author key,
protecting another ref or revoking a leaked credential meant restarting the
server, which drops every in-flight request and every limiter bucket. Teams
running many agents rotate credentials often and cannot afford the restart, and
"revoke now" must not mean "after the next deploy". The roadmap (M6) asks for
reload without restart.

## Decision
- **Trigger: SIGHUP only.** There is no admin endpoint (one more authenticated
  surface that can change who may write) and no file watcher (a half-written
  file or an editor's swap file would be applied at an arbitrary moment, and
  Kubernetes-style symlink swaps make change detection fiddly). Operators
  write the files, then signal. The handler is installed before the server
  listens, so a SIGHUP never kills the process; with no file configured it is
  a logged no-op.
- **One immutable `RuntimeConfig`.** Everything a request reads from a file is
  one struct: the token table, the limiter, the trusted keys, the review
  service and the composed land policy, plus a generation number. The server
  holds it in an `arc_swap::ArcSwap` (`ConfigHandle`). Readers never block and
  never see a partial update.
- **All or nothing.** A reload reads and validates every configured file
  first, building the new config off to the side. If any file is unreadable or
  invalid, nothing changes: the old config keeps running, an `ERROR` is logged
  with the reason, and `leep_config_reload_total{result="error"}` is
  incremented. Otherwise the new config replaces the old one in a single swap,
  so tokens and policies can never be mismatched. Reloads are serialised.
- **Same checks as startup.** Startup calls the same loader with no previous
  config, so a reload re-runs every startup validation: strict JSON with
  unknown fields rejected, digest format, duplicate names, per-token limit
  validation, weak and duplicate signing keys, protected ref names (S-06),
  `--require-signatures` needing keys, review needing tokens. A file that
  would stop the server starting cannot be loaded by a reload either.
- **A request uses one snapshot.** A middleware outside authentication takes
  the current config once, when the request arrives, and stores it in the
  request. Authentication, limits and the handlers read that snapshot. A
  request in flight when a reload happens finishes under the config it
  started with, even if it is stalled reading its body; new requests see the
  new one. A token revoked by a reload therefore stops working for every
  request that arrives after the swap, while one already admitted completes.
  It is not interrupted: cutting off a half-done upload or `land` would
  trade a short window for a worse failure.
- **One policy snapshot per land.** The land policy lives in the config, not in
  `Repo`. The `land` and raw ref-update handlers call
  `Repo::with_land_policy_replaced` (a cheap copy sharing the stores and land
  locks) with the request's snapshot. `land` therefore consults one policy for
  its whole duration, across CAS retries, so `depends_on_head` caching, the
  protected-delete rule, the forward-only rule and the policy check cannot
  disagree with each other, and protection of a ref cannot change halfway
  through a land. A land that began before a reload keeps the old protection;
  one that begins after uses the new.
- **Limiter state survives when it should.** The new limiter is built from the
  old: a token whose resolved limits are unchanged keeps its slot (buckets and
  in-flight permits), including when only its digest or its position in the file changed. A token whose limits changed, or that gained limits,
  starts with a fresh slot. A removed token, or one that lost all its limits,
  drops out; requests it has in flight finish against the slot they hold. The
  anonymous table is kept (its limits come from flags). Changing a token's role
  can change its limits through the read-body default, and is handled by
  comparing resolved limits, not file fields.
- **What reloads, and what does not.** Reloaded: the tokens file (names, roles,
  digests, per-token limits), the signing keys file, the review policy file
  (rules, docs globs, protected `refs`). Not reloaded: everything that is a
  flag or environment variable, which this process read once: listen address,
  metrics listener, stores, cache size, request and body limits, the
  server-wide limit defaults, `--require-signatures` and `--require-review`.
  Whether a file is configured at all also cannot change (a server started
  without `--tokens-file` stays unauthenticated; one with it cannot drop to
  open mode by deleting the file, which is an error). The log line after each
  reload says which files are re-read.
- **Reporting.** On success one `INFO` line per kind of change, with counts and
  names only (tokens added, removed, rotated, role changed, limits changed;
  limiter state kept, reset, dropped; keys added and removed; protected refs
  added and removed; review rules changed), and a summary line with the new
  generation. Digests and tokens are never logged. Protected refs that do not
  exist are warned about, as at startup. Names that are logged are token and
  key names, which operators chose and which already appear in logs and
  reviews.
- **Metrics.** `leep_config_reload_total{result="ok"|"error"}` (both series
  exist from startup), `leep_config_generation` (1 at startup, +1 per
  successful reload; failed reloads do not advance it) and
  `leep_config_last_reload_timestamp_seconds` (the last successful load,
  including startup). The generation is per process: it resets on restart and
  advances on reloads that change nothing, so it cannot say whether two servers
  agree. *Amended:* `leep_config_info{fingerprint}` is 1 for a BLAKE3
  fingerprint of the parsed config (0 for earlier ones this process ran), and
  servers behind a balancer are compared by it. The fingerprint holds no secret
  (digests, public keys, names, numbers, truncated to 8 bytes).
- No `leep-core` type, `/v1` endpoint or file format changed. The router gains
  `router_with_config`; the older constructors build a fixed config, so
  embedders and tests are unaffected.

## Alternatives
- **Admin endpoint (`POST /v1/admin/reload`).** Needs a role above `write`,
  new authorization rules, and a way to fail closed; SIGHUP reuses the
  operator's existing control over the process.
- **File watching.** Convenient, but it applies files at arbitrary moments and
  needs debouncing; all-or-nothing across three files is only meaningful when
  the operator decides when they are all ready.
- **Reload each file independently.** A bad review policy would not block a
  token change, but a token change that adds a reviewer while the policy that
  names refs stays old is exactly the mismatch to avoid.
- **A `RwLock` around each piece, or a swappable policy inside `Repo`.** Each
  allows a request to read tokens from one generation and policy from
  another, and a land to read two policies; a snapshot per request cannot.
- **Cancel in-flight requests of revoked tokens.** Kills uploads and lands
  halfway; a request already past authentication is bounded by
  `--request-timeout`, which is the same bound GC's fence relies on.

## Consequences
- Rotating or revoking a token, trusting or distrusting an author key, and
  protecting or unprotecting a ref take effect for the next request, with no
  downtime and no lost limiter state.
- A revoked credential is usable for the rest of any request it was already
  admitted for, at most `--request-timeout` (5 minutes by default; unbounded
  if the timeout is disabled).
- Operators must write files atomically (write beside, then rename). A reload
  that reads a half-written file fails safely and logs an error, and nothing
  changes until the next SIGHUP. Single-file container bind mounts follow the
  old inode after a rename; mount the directory.
- Every request pays one atomic load and an `Arc` clone, and the limits
  middleware is always installed (it does nothing when the config has no
  limits).
- Limits and keys apply per server process, as before; in a fleet, send SIGHUP
  to every instance and compare `leep_config_info` fingerprints.
- Changing flags still needs a restart.
