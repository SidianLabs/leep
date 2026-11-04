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
| S-20 | Resource use | Medium | Request bodies were read whole into memory with one 512 MiB cap per request and no bound on bodies in flight; a statement of a timed-out request ran on in Postgres | Fixed 2026-10-21, per-route caps, in-flight budget, statement timeout, ADR 0020 and 0012 |
| S-21 | Auth | Low | Tokens had no time bound, so a token made for a short-lived agent stayed valid until removed by hand | Fixed 2026-10-21, `expires`, ADR 0007 |
| S-23 | Review | High | A merge that kept its first parent's version dropped a side parent's change without review; an empty merge with an old first parent could rewind a reviewed ref | Fixed 2026-10-28, ADR 0011 |
| S-22 | Review | Medium | Every write token was a reviewer, so one agent could approve another's change, or its own on unowned paths | Fixed 2026-10-21, `"review": false`, ADR 0011 |
| S-24 | Review | Info | Verdicts carried to a re-committed changeset (ADR 0085) must not follow a forged `leep.rebased_from`, a moved base or a dropped path; the clean-replay trust of ADR 0011 remains | Designed in 2026-11-03, ADR 0085; review requests close by the same rule, 2026-11-04, ADR 0087; carrying across the server's own rebase result and unions of insertions, 2026-11-04, ADR 0089; identical-only paths, 2026-11-04, ADR 0092 |
| S-25 | Checks | Medium | CI check results promote `green/<ref>`, so a forged pass promotes untested code, a token could attest its own change, and a rewound ref would strand green | Designed in 2026-11-04: `checks` allow-list per token, creator refused, checked refs forward-only, ADR 0091 |

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

### S-23: changes a merge drops (High, fixed 2026-10-28)

A merge is judged on its changes relative to its first parent. A path where
it keeps the first parent's version is not among them, even when a side
parent changed it since their merge base, so the side's change is dropped
with nothing judged. `merge(old, head)` with no changes is a fast-forward of
the ref (the head is a parent), passes the forward-only check, and has the
tree of `old`: a writer could undo every reviewed change since `old` in one
land. The same hole let a `leep merge main` on a branch keep the branch's
version of a file `main` had fixed.

*Fix.* Paths a merge drops from a side are judged as well, with no auto rule
or `merge_parent` cover, so an owner must approve. Tests:
`a_merge_that_drops_a_sides_change_needs_review` (derive) and
`a_merge_with_an_old_first_parent_cannot_rewind_main_without_review` (server).

### S-24: carried review verdicts (Info, designed in 2026-11-03)

ADR 0085 lets a reviewer's verdict on X count for a changeset Y that names X
in `leep.rebased_from`, so a resolved land conflict keeps its approval. The
extra is written by the client, so the risk is a laundered approval: name an
approved X and land something else. The claim only picks which changesets to
compare; what decides is content. Every path X needed a reviewer for (not
auto-approved, not a merge-parent cover; `OWNERS` files always) must be among
Y's changes with the same old and new content and kind, old taken from each
one's own first parent, or nothing carries. That rules out a different
change under a forged hint, a reviewed file whose base moved (Y's change
would undo or merge with what landed since), and a Y that keeps one reviewed
file and drops the one it was approved with. Rejects carry the same way, so
re-committing does not escape a veto; a reviewer's own verdict on Y, a
withdrawal included, wins over a carried one. Several candidates (a
resolved stack) are each checked on their own, so combining them carries no
verdict to a path its own candidate did not reproduce whole.

*Follow-up (review of the first version).* Where several candidates carried
to a path, the first with a verdict decided, and the client orders the hint:
approve X, reject the identical copy X2, then name `X,X2` (or only `X`) on a
third copy, and the approval carried. Now each reviewer's newest review among
the reproducing candidates decides, whatever the order, and a reject or
withdrawal posted on a copy is also written on the candidates whose approval
by the same reviewer carried to it, so naming only `X` finds it too. The
cost was also unbounded in practice (a full judgement per candidate, merges
included); it is now capped (16 compared candidates, then none carry; no
merges; no judgement of X). Tests:
`the_newest_verdict_wins_whatever_order_the_hint_names_them_in`,
`a_newer_approval_wins_only_where_its_changeset_reproduces_the_path`,
`a_reject_on_a_copy_takes_back_the_approval_it_carried`,
`too_many_reviewed_candidates_carry_nothing_and_the_work_is_bounded`.

*Second follow-up.* Taking back skipped an approval stamped later than the
reject, and review times are each server's wall clock in seconds: with skewed
servers or a clock stepped back, approve X at t=100 and reject the copy X2
at t=98 left X approved, and newest-wins then let a copy naming both carry
the approval. The posted verdict now takes back regardless of the clocks,
stamped past the approval it replaces. Taking back also stopped after 16
candidates, leaving the 17th approval live; it is no longer capped (the
64-candidate walk bounds it). Tests:
`a_reject_takes_back_an_approval_stamped_later_by_a_skewed_clock`,
`a_reject_takes_back_every_approved_copy_it_names`.

*Accepted.* Taking back follows Y's hints: a reject on a copy that names
nothing, or on an identical copy approved independently, leaves X approved.
An author can make Y as X's change plus an extra file and name X, so a
reviewer rejecting Y for the extra file also rejects X; the
`leep:taken-back-on:<Y>` source row on X shows it. A storage failure part way
through taking back answers 500 with some candidates written; posting the
verdict again repairs it. A resolved stack with more than 16 entries
reviewed by others carries nothing, by design.

*Accepted.* A carried approval re-lands a change identical to one that was
later reverted: the identical-change rule cannot tell a reverted change from
one never landed, as landing the approved original itself again could not. The hint walk is bounded (64
candidates), ids that do not parse or do not exist are skipped, and a hint
naming Y itself is ignored. Tests in `crates/leep-server/tests/reviews.rs`:
`a_forged_hint_to_an_approved_changeset_gains_nothing`,
`a_base_that_moved_under_a_reviewed_path_stays_pending`,
`dropping_a_reviewed_path_carries_nothing`,
`one_changed_byte_in_an_owned_file_stays_pending`,
`a_reject_carries_and_the_reviewers_own_verdict_wins`,
`a_withdrawal_carries_nothing_on_either_side`.

*Remaining trust, unchanged.* An identical reviewed file on a moved base can
behave differently if *other* files changed under it. ADR 0011 already
accepts more than that for a clean server replay: the land policy judges the
submitted original, so its approval covers the copy even when the replay
three-way merged an owned file the original changed, a file content nobody
reviewed. Carrying requires the reviewed files to be untouched, so it is
strictly narrower. A policy that reviews the final tree would close both;
none exists.

*Closing review requests (ADR 0087).* Review requests now also close when a
changeset a land added names the requested changeset in `leep.rebased_from`.
The risk is the one ADR 0086 refused the hint for: one agent's land closing
another agent's request whose change never landed, so the reviewer stops
looking for it. The same rule as for carrying decides: the request closes
by content only when the named changeset's reviewed change is on the landed
changeset byte for byte (same old and new content and kind for every path
that needed a reviewer); otherwise only the request's own requester's land
closes it, and a requester may withdraw anyway. The requester rule counts
only changesets the land added: the first-parent chain from the landed
changeset down to the old head of the newest ref log entry moving the ref
there, and only when that entry names the land's actor. A land that moved
nothing (already on the ref, landed as a copy, a replayed land id) writes no
entry; then, or when the entry is out of reach, the landed changeset and the
contiguous server copies below it (at most 256) are checked by content and
copy row only, so a requester re-landing another agent's forged-hint land
closes nothing by the hint (found in review of the first version:
`a_re_land_by_the_requester_closes_nothing_by_hint`). A
listing applies the content rule only. *Accepted:* a request for an
auto-only change (every path docs, comment-only or whitespace-only) closes
when a changeset naming it changes the same paths in an auto-approved form,
by anyone, since such paths need no reviewer. Tests in
`crates/leep-derive/src/review_requests.rs`:
`a_forged_rebased_from_closes_nothing` (the forger's own land with every
auto rule, a replayed land on top, the requester's later land on top, and a
listing), `a_resolution_that_changed_a_reviewed_byte_stays_open`,
`another_agents_land_naming_it_does_not`,
`the_requesters_own_land_closes_a_stack_entry_the_content_rule_refuses`.

*Third follow-up: client rebases (ADR 0089).* A reviewed path whose base
moved now also carries when the judged changeset's file is exactly what
the server's own rebase of the reviewed change onto its first parent makes
(`diffy::merge(X.old, Y.old, X.new)`, clean, hash equal), or, where that
merge conflicts only in regions with an empty base part, one of the two
unions keeping both sides (one side first in every region). The risk is a
laundered approval through a resolution that looks like the reviewed
change: forged hints, a chosen base, reordered or moved hunks. The
analysis: the hint only picks what to compare; the result is a function of
three stored blobs, two of them what the reviewer saw and one Y's parent,
which is on the ref or lands in Y's stack and is judged itself, so the
rebased rule grants exactly the clean-replay trust ADR 0011 already gives
the server's copies, and nothing on a strict land ref (where the server
never merges contents, only identical paths carry). The union rule is the
one extension: an order bit over two insertions, each already on the ref or
reviewed, with nothing between them deleted or edited; accepted. Hunk
comparison was rejected: identical `+` lines can sit in another function.
Marker look-alikes in the texts cannot confuse the union parser (markers
are longer than any run in the inputs, runs over 64 refuse, and a text
without a final newline refuses). `OWNERS` files, symlinks, kind changes,
non-UTF-8, blobs over 2 MiB and texts over 10,000 lines never merge, and a
candidate needing more than 32 merges carries nothing. *Found in review:*
the byte bound alone left diffy's uncapped Myers diff at ~10^12 steps per
merge, reachable from a read token's `review-status` and from land checks
under the ref's land leader, and merges had no concurrency limit; now the
line budget refuses before any diff, deterministically, merges run once per
content triple (single-flight, cached, refusals included) under a limit of
half the cores, and a reviewed candidate that was collected carries nothing
instead of answering 404 or 500 (`texts_over_the_line_budget_are_not_merged`,
`concurrent_asks_for_one_merge_compute_it_once`,
`a_reviewed_origin_that_is_gone_carries_nothing_and_fails_nothing`). Requests close by the same check. Tests in
`crates/leep-server/tests/reviews.rs`:
`an_approval_carries_to_a_resolution_equal_to_the_servers_rebase`,
`an_approval_carries_to_a_union_of_two_appends_in_either_order`,
`a_union_that_edits_a_base_line_stays_pending`,
`a_resolution_with_one_extra_line_stays_pending`,
`reviewed_lines_moved_to_another_function_stay_pending`,
`a_rebased_kind_change_stays_pending`,
`a_reject_takes_back_through_a_rebased_carry`,
`too_many_merged_paths_carry_nothing`, `an_oversized_blob_is_not_merged`,
`a_strict_ref_carries_only_identical_changes`; in `leep-derive`,
`a_request_closes_when_its_change_lands_rebased_by_the_client` and
`carry::tests::marker_lookalikes_in_the_texts_do_not_confuse_the_union`.

*Fourth follow-up: identical-only paths (ADR 0092).* Owners may turn the
rebased and union carries off per directory (`OWNERS` line `carry
identical`, strictest line on the walk up to `noparent` wins, so a
subdirectory can tighten but never loosen) and operators per
glob (`identical_only_globs`). The risk is a change that loosens the rule
it is judged under. The rule is read from the target ref's head, never
from the judged changeset or its stack, and is monotone: strict ref, glob
and `OWNERS` can each only make a path identical-only, so a `carry merged`
line cannot undo a glob or a strict ref, and one added by a change applies
only once that change has landed with its owners' approval. Carrying and
request closing take one `CarryScope`, so a request closes exactly when the
verdict carries; taking back still considers merges everywhere, the safe
side. An unreadable head while closing requests counts as identical-only.
Accepted: on such a path any concurrent edit, auto-approved ones included,
forces re-approval of a pending client resolution, at most as often as the
editor may land. *Found in review:* the first version let the nearest
`carry` line decide, so a subdirectory's `carry merged` undid a parent's
`carry identical` though its owners need not own the parent; now
`carry identical` anywhere on the walk wins. And a misspelled directive
(`carry identically`) parsed as an owner name, silently leaving the
directory merged; lines starting with `carry` that are no directive are now
refused at land (422) and shown in `review-status`. Tests:
`a_subdirectory_cannot_loosen_an_ancestors_carry_identical`,
`a_misspelled_carry_line_is_refused_and_status_says_why`,
`a_change_adding_carry_merged_is_judged_under_the_heads_identical`,
`a_policy_glob_outranks_carry_merged`,
`an_identical_only_path_keeps_a_rebased_resolution_pending_and_says_why`,
`an_identical_only_path_refuses_a_union_but_carries_an_identical_change`;
in `leep-derive`,
`identical_only_is_strict_ref_or_glob_or_owners_and_only_stricter`,
`the_strictest_carry_line_decides_and_noparent_stops_the_walk`,
`a_subdirectory_cannot_loosen_an_ancestors_carry_identical`,
`a_request_closes_exactly_when_its_verdict_carries_under_the_carry_line`.

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

*Follow-up (#217).* The nightly fuzzer found the lexer disagreeing with
itself: it dropped the first line's indentation, which moved Ruby's ` =x`
to column 0, where a second pass read it as an `=begin`-style block. The
first line's indentation is now kept like any other line's, and
`strip_comments` gives up on any text whose result does not lex back to
itself, so a class of input it gets wrong in this way can never be approved.
Tests: `first_line_indentation_is_kept_so_stripping_is_idempotent` and
`stripping_is_idempotent_on_random_texts`.

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
  decompression bomb. Since S-20 every body is capped per route (256 MiB for
  a blob, 32 MiB for JSON) and all bodies in flight share a 1 GiB budget, a
  quarter of which one token may hold; per-token caps can lower that further.
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

### S-20: request body memory and orphaned statements (Medium, fixed)

The blob and changeset handlers read whole bodies into memory. The only cap was
`--max-body-bytes`, 512 MiB per request on every route, and per-token limits
are off by default, so a handful of writers uploading at once could ask for
gigabytes and get the process killed. Separately, sqlx sends no cancel request
when a query is dropped, so a statement of a request cut off by
`--request-timeout` ran on in Postgres, holding a connection and its locks.

*Fix.* Bodies are capped per route (`--max-blob-bytes` 256 MiB,
`--max-json-body-bytes` 32 MiB; 413 with the flag named). All bodies in flight
share `--max-inflight-bytes` (1 GiB; 503 with `Retry-After` when spent),
charged from the declared length after authentication, or without one the blob
cap or 1 MiB for JSON. With tokens, one token may hold at most a quarter of it
(429 beyond), so slow uploads from one token cannot hold all of it. The data
pool sets Postgres `statement_timeout` (`--meta-statement-timeout`, default the
request timeout plus 5 s), and the GC fence default rises to 15m to keep
`fence > request timeout + statement timeout + skew`, which `gc` now checks.
`GET /v1/ready`, public and unlimited, reuses its answer for a second, so
probes cannot make the server read its stores at will. Residual: anonymous
servers have no per-client share of the budget. Tests: the body-limit tests in
`crates/leep-server/tests/limits.rs`,
`postgres_statement_timeout_bounds_the_data_pool_only`,
`answers_are_reused_for_a_second`.

### S-21: tokens without expiry (Low, fixed)

A token was valid until someone removed its entry, so credentials made for an
agent that ran for an afternoon stayed usable indefinitely. An entry may now
carry `expires` (RFC 3339); a matching token past it gets 401 `token_expired`
(`token expired at ...`). The match is unchanged (constant time over every entry), and saying
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

### S-25: check attestations and known-good refs (Medium, designed in)

`green/<ref>` (ADR 0091) moves when every required check has a passing
result, so whoever may post a result decides what deploys read as good.
Only tokens whose entry lists the check name in `"checks"` may post it
(403 before any read), and the poster is the token, never a body field; `by`
is stored and shown, and the promotion's ref log entry names the token. A
token cannot attest a changeset it created, nor the server's rebased copy of
one (403, through `created-by-v1` and the rebase chain), so a CI token that
also commits cannot pass its own change. Agent tokens get no `checks`.
Deployment guidance is one token per check name, so a promotion needs every
check's token. Residual: a leaked CI token can post a false pass and promote
untested code to green, the trust CI already has when it reports to a code
host or deploys; a false fail only paints the ref red and adds noise to the
suspects. Nothing gates `land`. Results are keyed by changeset id and never
carried across rebases. `POST /v1/merge` records its caller as creator too,
so a token cannot attest a merge of its own change (found in review). The
whole `green/` namespace is the server's: no write token creates or moves a
`green/` ref, in the policy or not, so none can plant a "known good" for a
ref added later. Results are taken only for changesets a ref holds, so they
cannot pin abandoned work against GC, and `repair drop` may drop them. The
culprits walk is paged and its seek bounded, and both ref routes share the
`log_filtered` read permits, so a read token cannot make one request read
the whole ref log with ancestry checks. Every ref in the checks policy is forward-only;
otherwise a write token could move it back below green, and green, which
never rewinds, would stay ahead of it for good. The suspects list is the
lands in the range, not a verdict: a test planted to fail only once a later
land arrives frames that later land, so the docs and tools say so. Tests:
`only_allow_listed_tokens_post_and_never_on_their_own_changes`,
`green_refuses_land_cas_undo_and_delete_and_main_is_protected`,
`a_merge_made_by_a_ci_token_is_its_own_and_cannot_be_attested`,
`an_unlanded_changeset_never_promotes`,
`a_ref_named_like_a_suffix_never_shadows_the_status`,
`checks_policy_protects_its_refs_and_reloads`.
