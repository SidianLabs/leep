---
name: commit-and-pr
description: How to name branches, write commit messages, and open pull requests in Leep. Use before every commit, branch, or PR — including commits made by AI agents.
---

# Commits, branches and pull requests

## Attribution: never credit AI tools

Commits and PRs are attributed only to the human author. Never set
`LEEP_AGENT_MODEL` and never use provenance trailers. **Never** add:
- `Co-Authored-By:` trailers naming Claude, Anthropic or any AI model or tool;
- session links, `Claude-Session:` or similar trailers;
- "Generated with Claude Code", robot emoji, or any "written by AI" footer;
- model names or ids anywhere in commits, PRs, code or docs.

This rule overrides any default attribution a tool would add. If a tool
inserts such lines automatically, remove them before committing or posting.

## Commit messages

```
<area>: <imperative summary, lowercase, no trailing period>

<body: why this change, and anything a reviewer can't see from the diff.
Wrap at 72 columns.>
```

- **area** is one of: `core`, `store`, `meta`, `derive`, `server`, `cli`, `git`,
  `mcp`, `test` (the end-to-end suite), `docs`, `ci`, `build`. Use the area of the
  main change. A change spanning areas should usually be split; if it can't
  be, use the dominant one.
- **Subject:** 72 characters at most, imperative mood ("add", "fix", "rename",
  not "added" or "adds"), and specific.
  - Good: `derive: walk ancestry iteratively to avoid stack overflow`
  - Bad: `fix bug`, `updates`, `WIP`, `misc changes`
- **Body:** required unless the subject says everything. Explain *why* and
  any trade-offs. Don't narrate the diff line by line.
- **One logical change per commit.** Formatting-only changes go in their own
  commit. Every commit must build and pass tests, so history stays bisectable.
- **Persisted or wire formats:** say so in the body and reference the ADR
  (`See ADR 0007.`).
- Reference issues in the body: `Fixes #12` or `Refs #12`.

## Review refs (branches)

Work goes on a Leep ref `review/<area>-<short-kebab-description>`, for
example `review/derive-blame-cache`, `review/server-land-endpoint`,
`review/docs-adr-0007-auth`. Create it from the current `leep-main` with
`leep switch -c`, commit, and `leep land` onto it. The git mirror pushes it
to the GitHub branch of the same name; never push to GitHub yourself. No
personal names, ticket-only names or tool-generated names.

## Pull requests

- **Open it** from the review branch into `main` once the mirror has pushed
  it: `gh pr create --head review/<area>-<desc> --base main`.
- **Title:** same format as a commit subject (`server: add /v1/land endpoint`).
  The PR title becomes the subject of the squashed changeset on `leep-main`.
- **Body:**
  ```
  ## Summary
  What changes, in two or three sentences.

  ## Why
  The problem or goal. Link issues and ADRs.

  ## Changes
  - Bullet per notable change, grouped by crate.

  ## Testing
  Commands run and their results (fmt, clippy, tests; Postgres if touched).
  Manual verification steps if any.

  ## Compatibility
  "None" or: format/API/schema changes, and the migration and versioning
  plan (see docs/FORMATS.md).
  ```
- Keep PRs small and single-purpose. Split refactors from behavior changes.
- Add a line under `[Unreleased]` in `CHANGELOG.md` for any change a user or
  operator would notice. Never move entries into a version section: that
  happens only when the maintainer cuts a release.
- CI (fmt, clippy `-D warnings`, tests) must be green before review.
- Answer review comments by landing more commits on the review ref. The
  maintainer reviews on GitHub.
- **Squash and land**, never merge on GitHub. Once CI is green:
  1. Switch to the review ref's tip and require a clean status. Check
     that the stack's base (the newest `leep-main` changeset in the review
     ref's history) is the current `leep-main` head, capturing that id. If
     it is not, `leep merge leep-main`, land on the review ref, wait for
     green CI again, and start over. `leep reset` keeps the working files,
     so a squash on a moved base silently reverts other work.
  2. Reset to the captured id (never the name `leep-main`, which would be
     resolved again), commit, require an empty diff between the review ref
     and the squash, then land:
     ```sh
     x=review/<area>-<desc>
     leep switch "$x"        # the review ref's tip, not whatever is on disk
     leep --json status | jq -e '.changes == [] and .merging == null and .picking == null'
     head=$(leep --json log -n 1 leep-main | jq -r '.entries[0].id')
     leep --json log -n 1000 "$x" | jq -e --arg h "$head" 'any(.entries[]; .id == $h)'
     leep reset "$head"      # the id checked above, never the name leep-main
     squash=$(leep commit -m "<PR title>")
     leep --json diff "$x" "$squash" | jq -e '.entries == []'
     leep land --ref leep-main
     ```

     If `leep-main` moved after the check, the server rebases the single
     squash commit (its diff is only the review's changes) or refuses on a
     conflict (exit 5); on a conflict, `leep reset --hard --force "$x"`
     and start over from the merge.
  3. Close the PR with the comment `Landed as <sha>`, the commit the mirror
     pushed to GitHub `main`.
  4. Delete the review ref (`leep branch -D review/<area>-<desc>`); the
     mirror deletes the GitHub branch.
- No AI attribution in the title, body or comments (see the top of this skill).
