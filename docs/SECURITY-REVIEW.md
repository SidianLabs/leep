# Security review, October 2026

The M5 hardening review of authentication, signing, review policies, garbage
collection, ref operations, resource use and the working copy. It covers
`main` as of the request-limits change (ADR 0020). Findings that were fixed
have a regression test; the rest are written into the ADR they belong to,
under a dated "Security review 2026-10" note.

Report new problems as described in [SECURITY.md](../SECURITY.md).

## Method

- Read the code paths, not only the ADRs: `leep-server` (`auth.rs`,
  `lib.rs`, `signing.rs`, `review.rs`, `limits.rs`, `observe.rs`, `main.rs`),
  `leep-derive` (`land.rs`, `review.rs`, `owners.rs`, `comments.rs`, `gc.rs`,
  `repo.rs`, `tree_ops.rs`, `merge.rs`, `blame.rs`, `history.rs`, search) and
  `leep-cli` (`materialize.rs`, `overlay.rs`, `state.rs`, `sign.rs`).
- For each mutating route, traced how it is classified by authentication,
  which policy checks it passes, and what it can write.
- Wrote a failing test for every suspected bypass before changing code, and
  kept it as the regression test. For the comment lexer, each class has a pair
  of inputs that differ only in code the language sees and the lexer did not.
- Measured the worst cases of the text diff and merge libraries (`similar`
  and `diffy`): both are quadratic in the number of differing lines.

## Threat model

| Actor | Can | Must not be able to |
|---|---|---|
| Holder of a `read` token | every `GET`, `POST /v1/blobs/missing` | write refs, reviews, signatures or changesets; deny service to others beyond its limits |
| Holder of a `write` token who is not an owner, reviewer or signer | create changesets, land on unprotected refs, and move or delete any unprotected ref (the ref log records it; `--protect-ref` makes a ref forward-only and undeletable without a policy, S-19), review as itself | land on a protected ref without the policy passing; delete or rewrite protected history unnoticed; crash the server |
| Malicious client | send any request body, path, header or ref name | the same as its token's role |
| Malicious repository content author | put any file contents, paths, `OWNERS` files and ref names into history | get code past review as a "comment" or "merge"; make a checkout write outside the working copy |
| Malicious or compromised server (for the CLI) | answer anything | make the CLI write outside the working copy or into `.leep` or `.git` |
| Operator mistake | misconfigure flags and files | silently lose protection or data |

Out of scope: an attacker with direct access to the database or blob store,
and servers run without a tokens file on an untrusted network.

## Findings

Severity is for a deployment that uses the feature in question (a review
policy, `comment_only`, the CLI on macOS, ...). "Fixed" means fixed in this
change with a regression test.

| ID | Area | Severity | Finding | Status |
|---|---|---|---|---|
| S-01 | Review | High | A merge could launder an old version of an owned file back as `merge_parent` | Fixed |
| S-02 | CLI | High | A checkout could write through a symlinked directory, or into `.leep/` or `.git/` | Fixed |
| S-03 | Review | High | `comment_only` lexer disagreed with the language in many more constructs | Fixed |
| S-04 | Resource use | Medium | Deep paths made tree building use quadratic memory; one request could exhaust the server | Fixed |
| S-05 | Resource use | Medium | `blame` ran an unbounded quadratic diff that the request timeout cannot stop | Fixed |
| S-06 | Operations | Low | Protected ref names were not checked, so a typo protected nothing | Fixed |
| S-07 | Review | Low | `docs_only` could auto-approve an `OWNERS` file under a docs glob | Fixed |
| S-08 | CLI | Low | The private config temp file could keep a wider mode from a leftover file | Fixed |
| S-09 | Refs | Medium | Rewinding a protected ref (raw CAS or `leep undo`) dropped reviewed or signed history without review | Fixed |
| S-10 | Resource use | Medium | Server three-way text merge is quadratic and cannot be interrupted | Deferred, ADR 0019 and 0020 |
| S-11 | GC | Low | Read tokens write blob claims, which can keep known orphans from collection | Accepted risk, ADR 0007 and 0012 |
| S-12 | Refs | Low | Any writer can delete and recreate an unprotected tag; protection is by exact name | Accepted risk, ADR 0015 |
| S-13 | Resource use | Low | A filtered `log` scans history until it finds `limit` matches | Accepted risk, ADR 0020 |
| S-14 | Review | Low | Owner matching is byte-exact; case or Unicode variants of a directory are other directories | Accepted risk, ADR 0011 |
| S-15 | Review | Info | Unowned paths accept any approval, including the pusher's own | Mitigated 2026-10-21 by `"review": false` on agent tokens (S-22), ADR 0011 |
| S-16 | Auth | Info | No issue found; the metrics listener shows token names | Note in ADR 0007 |
| S-17 | Signing | Info | No issue found | Note in ADR 0010 |
| S-18 | Refs | Info | No issue found in name validation, tag immutability, protected delete or ref log attribution | — |
| S-19 | Refs | Medium | Refs no signature or review policy names could be rewound or deleted by any writer, so `main` was unprotected on most servers | Fixed 2026-10-21, `--protect-ref`, ADR 0015 |
| S-21 | Auth | Low | Tokens had no time bound, so a token made for a short-lived agent stayed valid until removed by hand | Fixed 2026-10-21, `expires`, ADR 0007 |
| S-22 | Review | Medium | Every write token was a reviewer, so one agent could approve another's change, or its own on unowned paths | Fixed 2026-10-21, `"review": false`, ADR 0011 |

### S-01: merge-parent laundering (High, fixed)

A path of a merge was `merge_parent` whenever its new content equalled the
same path in any side parent, on the grounds that the side is judged on its
own. But a side that is already in the ref's history was judged long ago,
when the file may have had different content. A writer could create
`merge(head, old)` by hand (`POST /v1/changesets` accepts any parents) with
`auth.rs` set to its content in `old`: the fix landed since then is undone,
the path reads `merge_parent`, and no owner is asked. A side built on `old`
that touches only unrelated files works the same way.

*Fix.* Taking a side's version is covered only if the first parent left the
path as it was at the merge base of the two parents, which is exactly when a
clean three-way merge takes the side's version. Otherwise the merge discards a
change of the first parent, and the path needs an owner. Unrelated histories
have no base, so a side can only bring in paths the first parent lacks. Clean
merges made by the server are unaffected. Tests:
`merge_parent_does_not_cover_discarding_the_first_parents_change` (derive) and
`a_merge_cannot_launder_an_old_version_back_without_review` (server).

### S-02: checkout writes outside the working copy (High, fixed)

`materialize`, `overlay::apply`, `restore` and `land`'s conflict writing joined
the repository path to the root and wrote. Three ways to abuse that:

- *Symlinked directory.* With a symlink `A -> /home/user` and a file
  `a/.ssh/authorized_keys`, a case-insensitive filesystem (macOS, Windows)
  writes the symlink first and the file through it. A malicious server can do
  the same on any filesystem by listing `a` (symlink) and `a/x` together. The
  untracked-file check stopped it only when the target already existed.
- *Control directories.* A path under `.git/` (`hooks/post-checkout`) runs as
  the user if the working copy is also a git clone. `sub/.leep/config.json`
  plants a nested working copy whose server URL receives `LEEP_TOKEN`, and
  `checkout --force` overwrote the root `.leep/config.json`. The server
  accepts these paths, so the client has to refuse them.
- *Windows separators.* A component like `a\..\..\x` or `C:` escapes the root
  when joined on Windows.

*Fix.* `WorkCopy::abs_for_write` is used for every write and delete. It
refuses a component that is `.leep` or `.git` after case folding, dropping
the code points HFS+ ignores and the trailing dots and spaces Windows ignores,
or the 8.3 names `LEEP~1` and `GIT~1`; on Windows also `\`, `:` and device
names; and a path whose directory on disk is a symlink, checked right before
each write. Tests: `control_directories_are_never_written_in_any_spelling`,
`writes_never_go_through_a_symlinked_directory` (unit) and
`crates/leep-cli/tests/path_safety.rs` (a mock server serving a symlink and a
file under it, and control paths).

### S-03: `comment_only` lexer desynchronization (High, fixed)

The rule is only safe if the lexer never treats code or string content as a
comment. The fuzzer had found two such gaps; reading the language
specifications found 24 more inputs, every one of which passed as
comment-only while changing behavior:

- *Interpolation that nests quotes.* Python 3.12 f- and t-strings
  (`f"{d["a # b"]}"`), C# `$"{d["k"]}"`, Kotlin and Scala `"${m["k"]}"`, Swift
  `"\(d["k"])"`, Ruby `"#{h["k"]}"`, shell `"$(echo "x")"` and `"${y:-"x"}"`.
  The lexer ended the string at the inner quote and read the rest as code.
- *C preprocessing.* A line splice (`*\`, newline, `/`) ends a block comment
  early, `/\`, newline, `*` starts one; the trigraph `??/` is a backslash.
- *Java and Scala unicode escapes* outside comments: `"` is a quote.
- *Comment start rules.* Shell starts a comment after `;&|()<>` too, Ruby and
  Perl at any `#` outside a literal (except Perl's `$#array`). The lexer
  treated those as code and could then read a quote inside the comment as the
  start of a string.
- *Unmodeled comments.* Ruby `=begin`/`=end`, Perl POD, JavaScript HTML-like
  comments (`<!--`, `-->`), Ruby `% x ` and `%=x=` literals, and a regex after
  `if (...)` in JavaScript.
- *Comments that are code.* A cgo preamble (the comment before `import "C"`
  is compiled as C), and Rust doc comments, whose code blocks run as doctests.

*Fix.* Each class now makes the lexer give up (the path then needs a human),
except where it can decide exactly: an interpolated literal is accepted when
every expression in it closes and contains no quote, escape or comment
marker, and Rust doc comments are kept as code. A second test checks that
ordinary uses (`f"{x}"`, `"${HOME}"`, macros with ` \` continuations, `$#a`
in Perl, `(a + b) / 2`) still allow comment-only edits. Tests:
`constructs_that_desynchronize_the_lexer_are_refused` and
`ordinary_interpolation_splices_and_operators_still_work`.

The lexer remains a heuristic. ADR 0011's advice stands: pair auto-approval
with owners on sensitive trees.

### S-04: path depth exhausts memory (Medium, fixed)

`RepoPath` has no length or depth limit, and `apply_changes` keeps one map
key per changed directory holding its full path, so memory is quadratic in
depth. A single `POST /v1/changesets` from any write token with a few
megabytes of `a/a/a/...` asks for terabytes and the process is killed.

*Fix.* Client changesets are refused (400) for a path over 4096 bytes, or
when all paths together name over a million directories or 256 MiB of
directory names. Depth alone is not limited: a 4096-byte path has at most
2048 components, and the totals bound the memory. `RepoPath` itself is unchanged, so stored data
still decodes; server-built changesets come from existing trees. Test:
`create_changeset_bounds_path_length_and_directories`.

### S-05: blame CPU (Medium, fixed)

Blame diffs each version of a file with the previous one using Myers'
algorithm, quadratic in differing lines: two unrelated 40,000-line versions
took 14 seconds in a release build. The diff is synchronous, so the request
timeout cannot interrupt it, and it holds an async worker the whole time. A
writer can commit such versions on any branch and a read token can then blame
them repeatedly with different paths.

*Fix.* Each blame diff has a 500 ms deadline, after which `similar` returns a
correct but less minimal diff, which only blurs which changeset is credited
for a line. Test: `blame_of_a_pathological_rewrite_is_bounded`.

### S-06: protected ref names unchecked (Low, fixed)

`--require-review`, `--require-signatures` and the policy's `refs` match ref
names exactly and were not validated. `LEEP_REQUIRE_REVIEW="main, release"`
protects `main` and ` release` (with a space), which no ref can be called, so
`release` is silently unprotected. The server now refuses to start with a name
that fails the ref name rules, and logs a warning for a protected name that
has no ref yet. Config reload (ADR 0024) runs the same checks, so a SIGHUP
with a bad name keeps the old config. Test: `server_refuses_to_start_with_bad_review_config`.

### S-07: `OWNERS` under a docs glob (Low, fixed)

With `docs_globs: ["docs/**"]`, `docs/OWNERS` matched and was auto-approved,
so any writer could add owners for `docs/` (whose paths are auto-approved
anyway, but whose owners can veto). No auto rule now applies to a file named
`OWNERS`. Test: `owners_files_are_never_auto_approved`.

### S-08: config temp file mode (Low, fixed)

Writing `.leep/config.json` with a token created the temp file with mode 0600
only if it did not exist; a leftover temp file kept its mode. The temp file is
now removed first and created exclusively.

### S-09: rewinds of protected refs (Medium, fixed)

`cas_ref_checked` judged only what a move adds, so moving a protected ref
back to an ancestor, or across to a sibling, needed no review or signature.
Any writer could drop landed commits, including a security fix, with a raw
compare-and-swap or `leep undo`: the same harm as S-01, by another route.

*Fix.* Like branch protection refusing a force-push, a ref that any land
policy covers now only moves forward: a swap whose new value does not descend
from the current one is refused with 400 (`... only moves forward ...`).
Creating a protected ref that does not exist is unchanged, and `land` was
already forward-only. `leep undo` of a land on a protected ref is therefore
refused, and the CLI says to `leep revert` the change and land the revert,
which goes through review and signing like any change. Tests:
`protected_refs_only_move_forward` (derive),
`raw_ref_cas_cannot_rewind_a_protected_ref` (server) and the ref log
end-to-end test (`leep undo` under `--require-signatures`).

### S-10: merge CPU (Medium, deferred)

`diffy::merge` has the same quadratic worst case as the blame diff (25 s for
40,000 differing lines) and no deadline. It runs inside `merge`, `rebase` and
`land`, all of which need a write token. A deterministic cost bound is needed
first: a wall-clock deadline would make merge results depend on load, which a
version control system should not do. Until then, per-token concurrency limits
(ADR 0020) bound how many workers one token can tie up. Recorded in ADRs 0019
and 0020.

### S-11: read tokens and GC claims (Low, accepted)

`POST /v1/blobs/missing` is allowed for read tokens and records a claim for
each present blob, which keeps an unreferenced blob from being condemned for
the grace window. A read token holder who knows orphan hashes can keep them
for as long as they keep asking. That costs storage, never data: claims cannot
rescue a condemned blob or cause a delete, and condemned blobs are reported
missing without a claim. Each hash also costs one metadata write, bounded by
`--read-max-body-bytes`. Recorded in ADRs 0007 and 0012.

### S-12: tags and exact-name protection (Low, accepted)

Tags cannot move, but an unprotected tag can be deleted and created again at
any changeset by any writer, so `tags/v1.0` can be repointed at unreviewed
code. Policies take exact ref names, so protecting every release tag means
listing each one (which also makes them undeletable). Recorded in ADR 0015.

### S-13: filtered log scans (Low, accepted)

`GET /v1/log` with `author` or `extra` filters walks history until it finds
`limit` matches, so a filter that matches nothing walks all of it. The walk
awaits storage between steps, so the request timeout does stop it; rate and
concurrency limits bound repeats. Recorded in ADR 0020.

### S-14: case and Unicode variants of owned directories (Low, accepted)

`OWNERS` coverage compares paths byte for byte, as the tree does: `SRC/x.rs`
and a decomposed-Unicode spelling of `src` are not under `src/OWNERS`. On a
case- or normalization-insensitive filesystem a build tool may treat them as
the same directory. The CLI already refuses to let one overwrite the other in
a checkout (untracked-file check), but a file only in the variant directory is
covered by the parent's owners, not by `src/OWNERS`. A root `OWNERS` file
closes the gap. Recorded in ADR 0011.

### S-15 to S-18: checked, no change

- **Self-approval (S-15).** Where no `OWNERS` file applies, any principal,
  including the token that pushed the change, can approve it. This is ADR
  0011's documented opt-in model; use a root `OWNERS` file on protected refs.
  Since S-22, giving agent tokens `"review": false` keeps agents from
  approving at all, which closes the agent-approves-itself case.
- **Authentication (S-16).** Tokens are hashed and compared in constant time
  against every entry without early exit, so known and unknown tokens take the
  same time. Only `GET`, `HEAD` and the exact path `POST /v1/blobs/missing`
  are reads; every other method and path, including routes added since
  ADR 0007 (reviews, signatures, ref CAS with labels, merge, rebase, land),
  needs `write`, and an unusual spelling of a path can only be classified as
  a write. With a tokens file every route except `GET /v1/health` needs a
  token; 401 and 403 bodies are fixed strings. Metric labels are route
  templates and token names, never paths or tokens. The metrics listener is
  unauthenticated and shows token names, so keep it on a private address.
- **Signing (S-17).** The signed message is domain-separated
  (`leep-changeset-signature-v1\0` and the 32-byte id), so a signature cannot
  be replayed onto another changeset or protocol. Verification uses
  `verify_strict`, and small-order keys fail startup. Stored rows are
  re-verified against the current key set on every read and land. The policy
  never accepts a changeset through the server-written `rebased-origin-v1`
  link, and covers land, raw CAS and `undo`; rewinding (S-09) or deleting a
  protected ref is refused. Changing the keys file takes effect at restart or SIGHUP
  (ADR 0024) and applies to old signatures too, as ADR 0010 says.
- **Refs (S-18).** Every write path validates names (`cas_ref_checked`,
  `land`); tags cannot move by CAS, land or undo; deleting a protected ref is
  refused on every path; the ref log's actor is the token name and clients
  cannot use the `land` or `import` labels. `leep-git import` writes stores
  directly and remains an operator tool.

### Also checked

- **GC (ADR 0012).** No client request can cause a reachable blob to be
  deleted: claims only keep blobs, rescues are written only by uploads of the
  exact bytes, condemned blobs are refused in changesets, and derived indexes
  live in metadata, not the blob store. A client cannot block GC: an upload
  waits at most 30 s on an in-progress delete and then gets 409.
- **Search.** The `regex` crate runs in linear time; patterns are capped at
  1 KiB and 1 MiB compiled, scans at 256 MiB. A first search or path log of an
  old revision derives indexes for its history; that work is bounded by the
  repository and stored, so it is paid once.
- **Rename detection and auto rules** have pair, byte and per-file caps
  (1 MiB similarity, 2 MiB for `comment_only` and `whitespace_only`).
- **Uploads.** There is no request decompression, so there is no
  decompression bomb; body sizes are capped globally and per token.
- **Signing keys on the client** are created 0600 and refused if group or
  world readable.

## Follow-up, 2026-10-21

### S-19: refs unprotected without a policy (Medium, fixed)

Forward-only and no-delete (S-09, ADR 0015) applied only to refs a signature or
review policy covered. A server that requires neither, which is most of them,
let any write token reset `main` to an old changeset or delete it with one raw
compare-and-swap; the ref log recorded it but nothing refused it.

*Fix.* `--protect-ref REF` (`LEEP_PROTECT_REFS`) covers a ref with a land policy
that checks nothing else (`ProtectOnly`), composed with the other policies, so
the existing rules apply through the same `cas_ref_checked` path and `land` is
unchanged. It composes with `--require-review` and `--require-signatures` (all
apply), protects tags by exact name too (a protected tag cannot be deleted and
recreated elsewhere, narrowing S-12), and, like the other policy flags, is read
at startup only. Tests: `crates/leep-server/tests/protect.rs`.

### S-21: tokens without expiry (Low, fixed)

A token was valid until someone removed its entry, so credentials made for an
agent that ran for an afternoon stayed usable indefinitely. An entry may now
carry `expires` (RFC 3339); a matching token past it gets 401 `token expired at
...`. The match is unchanged (constant time over every entry), and saying
"expired" tells nothing to a caller without the token. `gen-token --expires-in`
writes the field; `leep_tokens_expiring_soon`, `leep_tokens_expired` and the
`LeepTokensExpiringSoon` alert surface tokens to rotate. Expiry reads the
server's clock. Tests: `an_expired_token_is_refused_with_401_saying_so`,
`expires_is_parsed_checked_and_reported`.

### S-22: every write token a reviewer (Medium, fixed)

Reviewers were all write tokens. Where most writers are agents, any agent could
approve any other agent's change, and on paths without `OWNERS` its own (S-15),
so a review policy checked little. A token entry may now say `"review": false`:
it cannot post reviews (403) and its reviews are neither listed nor counted.
Deployment guidance is to set it on every agent token. Turning it off hides the
token's existing rows rather than deleting them, so turning it back on restores
them. Test: `tokens_without_review_cannot_review_and_their_old_reviews_stop_counting`.
