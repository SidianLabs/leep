# Changelog fragments

Don't edit `CHANGELOG.md` in a change. Add one file here instead, named after
your review ref without the `review/` prefix: `review/cli-review-wait` becomes
`changelog.d/cli-review-wait.md`. Two changes then never edit the same lines,
so landing one never makes the other conflict
([ADR 0088](../docs/adr/0088-changelog-fragments.md)).

A fragment is one `### Title` line, then the entry:

```markdown
### Review status can wait for a verdict
- **Added.** `leep review status --wait` polls until the changeset is
  approved (exit 0), rejected or out of time (exit 1).
```

- Start each bullet with what kind of change it is: `**Added.**`,
  `**Changed.**`, `**Fixed.**`, `**Removed.**`, `**Docs.**`, `**Tests.**`.
- Use `####` or lower for anything below the title; never `#`, `##` or a
  second `###`.
- Answering review: edit your fragment, don't add a second one.

`scripts/changelog-assemble.sh --check` checks every fragment, and
`cargo test --workspace` runs the same check. The maintainer runs
`scripts/changelog-assemble.sh` (or `--preview` to read the result first)
at a release, which moves the fragments under `[Unreleased]` in
`CHANGELOG.md`, in file-name order, and deletes them.
