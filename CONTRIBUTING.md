# Contributing to Leep

1. Read [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md). Its principles are
   load-bearing.
2. Anything that changes a persisted or wire format needs an ADR
   ([docs/adr/](docs/adr/)) and must follow [docs/FORMATS.md](docs/FORMATS.md).
3. Before opening a PR, run `cargo fmt --all`, `cargo clippy --workspace
   --all-targets -- -D warnings` and `cargo test --workspace`.
   If you add or rename an alert in
   [deploy/prometheus/leep-alerts.yml](deploy/prometheus/leep-alerts.yml),
   give it a `## <AlertName>` runbook in [docs/RUNBOOKS.md](docs/RUNBOOKS.md) and
   run `scripts/check-runbooks.sh`, which CI runs too. If you change
   dependencies, also run `cargo deny check` (see "Supply-chain checks").
   Windows runs the unit and integration tests; Linux and macOS also run the e2e, soak and crash tests.
4. Keep PRs to one logical change. Follow the commit, branch and PR
   conventions in [`.claude/skills/commit-and-pr/SKILL.md`](.claude/skills/commit-and-pr/SKILL.md).
   Commits and PRs credit human authors only, never AI tools.
5. Anything that decodes bytes from outside (a stored row, a request body, a
   config file) gets a fuzz target; see [docs/FUZZING.md](docs/FUZZING.md).

AI agents working on this repo: follow [CLAUDE.md](CLAUDE.md) and the skills
in `.claude/skills/`.

Notable changes go under `[Unreleased]` in [CHANGELOG.md](CHANGELOG.md)
in the same PR. Report security problems privately as described in
[SECURITY.md](SECURITY.md).

Outside contributors sign the [Contributor License Agreement](CLA.md)
before their first PR is merged. The maintainer reviews and merges every
PR.

Licensed under Apache-2.0; see [LICENSE](LICENSE).

## Supply-chain checks

CI runs [cargo-deny](https://github.com/EmbarkStudios/cargo-deny) with
[`deny.toml`](deny.toml) on every push and pull request
(`.github/workflows/supply-chain.yml`), and the advisory check again every
Monday. To run it locally: `cargo install cargo-deny --locked`, then
`cargo deny check`.

- **advisories:** any RustSec vulnerability or yanked version fails. An
  unmaintained crate fails only when a workspace crate depends on it
  directly. Fix with `cargo update -p <crate>` where that is enough. An
  `ignore` entry needs a reason and a remove-by date in `deny.toml`.
- **licenses:** dependencies must be under a license on the `allow` list.
  Adding a license there needs a check that it is compatible with Apache-2.0.
- **bans:** no wildcard (`*`) requirements. Internal crates carry the
  workspace version in `[workspace.dependencies]`, so a version bump moves
  those lines too. Duplicate versions of a crate only warn.
- **sources:** crates.io only; a git dependency needs an entry in `deny.toml`.

Dependabot (`.github/dependabot.yml`) opens grouped minor and patch updates
for Cargo and GitHub Actions weekly. The MSRV, `rust-version` in the root
`Cargo.toml`, equals the toolchain in `rust-toolchain.toml`; bump both
together.
