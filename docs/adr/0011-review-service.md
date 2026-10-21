# 0011. Review service with per-directory owners and semantic auto-approval
Status: Accepted
Date: 2026-10-15

## Context
Signed changesets (ADR 0010) say who vouches for a change, but not whether
anyone who owns the touched code has looked at it. A protected ref needs to
refuse changes the right people have not approved, and it should not ask
humans to approve changes that carry no risk, such as a new comment or a docs
page, because most writers are agents that produce many small changes.

Reviews are not a function of history: an approval is something a person does
after the changeset exists. The `add-derived-data-kind` skill says that such
data does not belong in `extra`, because `extra` is hashed into the changeset
id and the approval would change the id it approves. Like signatures, reviews
are attestations stored beside the changeset. `Changeset` and every other
persisted type are unchanged.

## Decision
**Owners** come from `OWNERS` files in the tree. One owner name per line, `#`
starts a comment, blank lines are ignored, and a line `noparent` stops
inheritance. A name is a token name from the server's tokens file. The owners
of a path are the union of the `OWNERS` files in its directory and every
ancestor, up to the first `noparent` or the root.

- Owners are read from the **target ref's current head tree** at check time,
  never from the changeset being judged, so a change cannot grant itself
  ownership. A change that adds or edits an `OWNERS` file changes a path in
  that directory, so it needs approval from the directory's owners as they are
  in the head; no special case is needed.
- With no head, or when no `OWNERS` file covers a path (or the files that do
  list no names), the path has **no owners and any single approval from any
  principal satisfies it**. This keeps a fresh repository usable and makes
  adding `OWNERS` files the way a team opts in, directory by directory. A
  symlink named `OWNERS` is ignored (its content is a link target).
- Reading is iterative and caches each directory's file once per check.

**Reviews** are rows of derived kind `review-v1`: key =
`Hash::of_object("review", changeset_id ‖ reviewer name)`, value = postcard
`Review { reviewer, verdict, comment, timestamp }`, with `Verdict` an
externally tagged enum (`Approve`, `Reject`). Derived data is last-writer-wins,
so a reviewer re-posting replaces their earlier verdict (they can change their
mind, and the call is idempotent), and two reviewers touch different rows.
The reviewer is the **authenticated token's name**, passed to handlers by the
auth middleware as a `Principal` request extension; the request body cannot
name one. Reviews need authentication: without a tokens file the review
endpoints answer 400. A `read` token cannot post (the middleware already
classifies every `POST` except `blobs/missing` as a write).

The store has no prefix listing, so `GET .../reviews` does one lookup per
principal from the tokens file (write-role tokens only, since a read token
cannot have reviewed; the same trick as per-key signature lookups). The cost
is O(principals) per query. A row that does not decode, or names a different
reviewer than its key, counts as absent.

**Exact ids.** An approval is of one changeset id, which commits to its full
content. As with signatures, the policy checks the originals the client
submitted before any server replay; rebased copies carry no review of their
own. `GET .../reviews` on a copy with none reports the original's reviews with
`via` (using `rebase_chain`), which is informational only.

**Decision per path.** For each path a changeset changes (relative to its first
parent), in this order:
1. a `reject` from an owner of the path blocks it, whatever else holds; with no
   owners a `reject` from any principal blocks;
2. on a merge, a path whose new content and kind equal that path in one of
   `parents[1..]` is **covered** (`merge_parent`): that side's history is in the
   land's new history and is reviewed on its own;
3. an enabled **auto rule** that holds approves it (`auto:<rule>`);
4. an approval from an owner (or anyone, when unowned) approves it (`owner`);
5. otherwise it is `pending`.
A changeset is approved when no path is pending or rejected. The first
unsatisfied path in path order is reported.

**Auto rules** are configured in a JSON policy file
(`--review-policy-file` / `LEEP_REVIEW_POLICY_FILE`, `deny_unknown_fields`):
`{"refs": [...], "auto_approve": [...], "docs_globs": [...]}`. All rules are pure
functions of the old and new bytes and the path; the classification lives in
`leep-derive` (`review.rs`, `comments.rs`) and is unit tested.
- `docs_only`: the path matches a docs glob (`globset`, `*` stays within one
  directory level, `**` crosses levels). Adds, deletes and modifies all count.
  Needs `docs_globs`, and `docs_globs` needs the rule, so a half-written policy
  fails at startup (and on reload, ADR 0024).
- `whitespace_only`: a `Modify` of a non-symlink file, same kind as before,
  whose old and new content are equal line by line once trailing spaces, tabs
  and CRs and trailing blank lines are dropped; both must be UTF-8. Leading
  and interior whitespace are not ignored: they carry meaning in Python
  blocks, YAML and string literals.
- `comment_only`: a `Modify` of such a file whose extension is in the language
  table (`rs c h cc cpp hpp java js ts tsx jsx go swift kt scala cs` with `//`
  and `/* */`; `py sh bash rb toml yaml yml pl` with `#`) and whose old and new
  text are equal after a small lexer removes comments outside string literals.
- Adds, deletes, kind changes, symlinks, binary or non-UTF-8 files, and files
  over 2 MiB are never `comment_only` or `whitespace_only`.

*The lexer is conservative by construction*: it gives up (so the rule does not
apply) on anything it cannot classify. It respects double-quoted strings with
backslash escapes, single-quoted strings or chars per language, Rust raw
strings and lifetimes, Python and TOML triple quotes, and nested comments
where the language nests them. It refuses unterminated strings and comments,
heredocs, backticks (JavaScript templates, Go raw strings, shell command
substitution), C++ and C# raw strings, Swift raw strings, YAML block scalars,
JSX, JavaScript regex literals, Ruby and Perl regex and quote-like literals,
and comments containing line terminators or bidirectional controls that can
hide code. Shebangs, build tags and linter or coding pragmas are kept as
code, so editing them is not a comment change. Removing a comment marker turns
comment text into code and is therefore a code change.
**Whitespace in `comment_only`** is normalized, not removed. Removing it
would let `ab` pass for `a/**/b`, and a Python re-indent for a comment edit.
A whitespace run containing a newline becomes one newline plus the next
line's indentation, other runs become one space, and a comment counts as
whitespace. Whitespace inside string literals is kept verbatim. So on a known
language `comment_only` also accepts blank-line and spacing changes between
tokens, which `whitespace_only` deliberately does not, since that rule applies
to any text file.

**Enforcement.** `RequireReview` is a `LandPolicy` in `leep-server`, like
`RequireSignatures`. It is on for each ref in the policy file's `refs` and for
each `--require-review REF` (repeatable, `LEEP_REQUIRE_REVIEW`
comma-separated); both need the tokens file. It judges every changeset the land
would add, including each unlanded stack entry and merged side branches, and
fails with `Error::Invalid` (400), `changeset X needs review: path P needs
approval from one of [a, b]` (`needs approval from any reviewer` when unowned,
`rejected by a, b` for a veto). The text starts with the stable marker
`NEEDS_REVIEW_MARKER` (`needs review`), which the CLI matches to add a hint.
Raw `POST /v1/refs/{name}` goes through `cas_ref_checked` and is covered.
- **Trait change.** `LandPolicy::check_land` now also receives the ref's
  `head`, so the new-history set and the owners come from the same head, and a
  `depends_on_head()` method (default false). `land` reuses a passed decision
  across CAS retries only for policies that do not depend on the head; owners
  do, so after a lost CAS review decisions are recomputed against the new head.
  `RequireSignatures` ignores both.
- **Several policies.** `Repo::with_land_policy` called again keeps the earlier
  policies in a `PolicySet`; all that apply to the ref must pass, in the order
  set. The server sets signatures first.

**API (additive, `/v1`).** `POST /v1/changesets/{id}/reviews`
`{verdict: "approve"|"reject", comment?}` returns `{reviewer, verdict}`;
`GET .../reviews` returns `{reviews: [...], via}`; `GET
.../review-status?ref=main` returns `{changeset, target_ref, approved, paths:
[{path, rule, owners, approved_by, rejected_by}]}` with `rule` one of
`auto:comment_only`, `auto:whitespace_only`, `auto:docs_only`, `owner`,
`merge_parent`, `pending`, `rejected`. Comments are capped at 64 KiB. Status is
a read, so a `read` token may call it.

**Clients.** `leep review approve|reject|status|list REV` and `leep reviews REV`
(`status` exits 1 when not approved); `leep land` adds a hint to the policy
error. `leep-mcp` gets a read tool `leep_review_status`. The CLI still depends
only on `leep-core`; the types live in `leep_core::api` and `leep_core::review`.

## Consequences
- History, ids and existing clients are untouched. A server without a tokens
  file behaves as before, and review endpoints are a 400 there.
- Reviewers are the principals of the running server. Removing a token stops
  its reviews counting, including old ones (like removing a signing key). A
  token that never existed cannot be an owner in practice, so a typo in
  `OWNERS` makes a directory unmergeable until an approved change fixes it
  (owned by the same typo'd set; a rejected-by-nobody list with no valid name
  has no way out except an operator, who can move the ref directly in the store).
- Owners are in the tree, so they are versioned, reviewed and blamed like code.
  Their effect starts when the change that edits them lands.
- Approvals do not follow rebases. After a land replays a stack the copies are
  new ids; reviewers approve what the client submits, and the copies are
  informational via `via`.
- A merge-parent path is trusted because its side is judged separately in the
  same land. If the side's changeset is already in the ref's history, it was
  judged when it landed (or predates the policy).
- `review-status` is evaluated against the head now, so an answer can go stale
  when the head moves; `land` re-judges.
- Each land judges N changesets with M principals: O(N·M) row lookups plus the
  file reads of the auto rules. Fine for tens of principals.
- Known limitation: a changeset's `author` is a client-supplied string, so
  there is no "author cannot approve their own change" rule. An owner who is
  also the pusher can approve their own work, and nothing ties a token to the
  author field.
- `comment_only` is a lexer, not a parser. It is built to fail closed, but it
  is heuristic code; pair it with owners on sensitive trees (an owner's
  rejection still blocks an auto-approved path).
- Rejects from any principal block paths that have no owners. That is the
  mirror of "anyone may approve there"; it lets any write token stop an
  unowned path until they change their verdict.

## Alternatives considered
- **Reviews in `extra`.** Changes the id it approves; rejected for the same
  reason as signatures.
- **A per-changeset review list under one row.** A read-modify-write race
  between reviewers; one row per reviewer is a single idempotent write.
- **Owners in a server config file.** Not versioned with the code, and edited
  without review. Owners in the tree are reviewed by existing owners.
- **Owners read from the changeset.** Lets a change appoint itself.
- **Removing all whitespace for `comment_only`** (the first idea): unsound for
  token boundaries and indentation, see above.
- **Treating unowned paths as unmergeable.** Safer, but a new repository could
  not land its first change, and opting in directory by directory is easier.

## Future work
- Self-approval rules: bind author to a token or key (with signatures: "the
  signer may not be the sole approver") and optionally require a second owner.
- Team groups (`@team` names in `OWNERS`, defined in a server file or in the
  tree), per-path minimum approver counts, and `OWNERS` includes.
- A reviews index so listing is not O(principals), and dismissal of approvals
  when a path is touched again.
- Per-path signing policy alongside ownership (ADR 0010).
- Approval carried across server rebases when the replayed diff is identical.
- Gate `leep-git` import by the same policies.
- SIGHUP reload of the policy file, as for tokens.

## Security review 2026-10
See [SECURITY-REVIEW.md](../SECURITY-REVIEW.md).
- **`merge_parent` narrowed (S-01, fixed).** "That side's history is reviewed
  on its own" did not hold for a side already in the ref's history: a
  hand-made `merge(head, old)` could put back `old`'s version of an owned
  file, undoing a reviewed fix with no owner asked. A path is now covered only
  if it equals a side parent's *and* the first parent left it as it was at the
  two parents' merge base, which is when a clean three-way merge takes the
  side's version. Server-made clean merges are unaffected.
- **`comment_only` (S-03, fixed).** The lexer now also gives up on string
  interpolation that can nest quotes (Python f- and t-strings, C#, Kotlin,
  Scala, Swift, Ruby, shell) unless every interpolated expression closes and
  holds no quote, escape or comment marker; C line splices and the `??/`
  trigraph; Java and Scala `\u` escapes of ASCII characters anywhere; shell
  `#` after an operator and Ruby or Perl `#` without a blank before it; Ruby
  `=begin`, Perl POD, Ruby `% x ` literals, JavaScript HTML-like comments and
  a regex after `if (...)`; and Go files that use cgo. Rust doc comments
  (`///`, `//!`, `/** */`, `/*! */`) are now code, because their code blocks
  run as doctests.
- **No auto rule for `OWNERS` (S-07, fixed).** A docs glob such as `docs/**`
  matched `docs/OWNERS`; no rule now applies to a file named `OWNERS`.
- **Protected refs only move forward (S-09, fixed).** The policy checks what
  a move adds, so moving a protected ref back to an ancestor or across to a
  sibling (raw CAS or `leep undo`) dropped reviewed commits without any
  review. `cas_ref_checked` now refuses (400, `only moves forward`) any move
  of a ref a land policy covers whose new value does not descend from the
  current one, as branch protection refuses a force-push. Creating the ref is
  unaffected and `land` was already forward-only. To take a change back,
  revert it and land the revert, which is reviewed like any change.
- **Exact path matching (S-14, accepted).** Owners match paths byte for byte,
  as trees do, so `SRC/` or a decomposed-Unicode spelling of `src/` is not
  under `src/OWNERS`, though a case- or normalization-insensitive build may
  treat it as the same directory. Put an `OWNERS` file at the root of a
  protected tree so such variants still need an owner.
- Unowned paths still take any approval, including the pusher's own
  (S-15, as documented above); a root `OWNERS` file avoids that too.

## Reviewer capability, 2026-10-21
Every write token was a reviewer, so in a deployment of many agents with write
tokens any agent could approve another's change, and its own on paths no
`OWNERS` file covers (S-15). A tokens-file entry may now say `"review": false`
(absent means true, so existing files keep their behaviour). Such a token is
left out of `Auth::reviewers`, so it is not one of the principals whose review
rows are looked up: its reviews, including ones it posted before the change,
neither list nor count. `POST /v1/changesets/{id}/reviews` with it answers 403
`token may not review: its tokens-file entry has "review": false` before
anything is read. Everything else a write token does is unchanged. The field
reloads on SIGHUP (ADR 0024), enters the config fingerprint only when false,
and is reported as `review_changed` in the reload log. `gen-token
--no-review` writes it, and DEPLOY.md recommends it for every agent token. The
CLI now prints the server's 403 reason instead of always saying the token
lacks write access. The tokens file only gains an optional field.
Turning review off deletes nothing: the token's `review-v1` rows stay in the
store, unlisted and uncounted, and setting the field back to true (or removing
it) makes them list and count again from the next request, approvals included.
To withdraw such a token's past approvals for good, have it post a `reject` (or
another verdict) while it may still review, or rotate it to a new name.
