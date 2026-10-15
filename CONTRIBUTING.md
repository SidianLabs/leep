# Contributing to Leep

1. Read [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md). Its principles are
   load-bearing.
2. Anything that changes a persisted or wire format needs an ADR
   ([docs/adr/](docs/adr/)) and must follow [docs/FORMATS.md](docs/FORMATS.md).
3. Before opening a PR, run `cargo fmt --all`, `cargo clippy --workspace
   --all-targets -- -D warnings` and `cargo test --workspace`.
4. Keep PRs to one logical change. Follow the commit, branch and PR
   conventions in [`.claude/skills/commit-and-pr/SKILL.md`](.claude/skills/commit-and-pr/SKILL.md).
   Commits and PRs credit human authors only, never AI tools.

AI agents working on this repo: follow [CLAUDE.md](CLAUDE.md) and the skills
in `.claude/skills/`.

Notable changes go under `[Unreleased]` in [CHANGELOG.md](CHANGELOG.md)
in the same PR. Report security problems privately as described in
[SECURITY.md](SECURITY.md).

Outside contributors sign the [Contributor License Agreement](CLA.md)
before their first PR is merged. The maintainer reviews and merges every
PR.

Licensed under Apache-2.0; see [LICENSE](LICENSE).
