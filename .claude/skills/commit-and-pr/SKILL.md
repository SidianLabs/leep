---
name: commit-and-pr
description: How to name branches, write commit messages, and open pull requests in Leep. Use before every commit, branch, or PR — including commits made by AI agents.
---

# Commits, branches and pull requests

## Attribution: never credit AI tools

Commits and PRs are attributed only to the human author. **Never** add:
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

## Branch names

`<area>/<short-kebab-description>`, for example `derive/blame-cache`,
`server/land-endpoint`, `docs/adr-0007-auth`. No personal names, ticket-only
names or tool-generated names.

## Pull requests

- **Title:** same format as a commit subject (`server: add /v1/land endpoint`).
  With squash merge, the PR title becomes the commit subject on `main`.
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
- CI (fmt, clippy `-D warnings`, tests) must be green before review.
- Merge with **squash merge**. Delete the branch after merging.
- No AI attribution in the title, body or comments (see the top of this skill).
