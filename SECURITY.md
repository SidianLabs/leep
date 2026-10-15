# Security policy

## Supported versions

Leep has not had a release. Only the latest commit on `main` receives
security fixes. This changes when releases start; see
[docs/ROADMAP.md](docs/ROADMAP.md).

## Reporting a vulnerability

Please do not open a public issue, pull request or discussion for a
security problem.

Report it privately through GitHub: on the repository page, open the
**Security** tab and choose **Report a vulnerability**. Include what you
found, how to reproduce it, and the commit you tested.

We aim to acknowledge reports within three working days and to agree a
disclosure date with you once a fix is ready.

## Scope

In scope: the server, CLI, MCP server, git bridge, storage backends and
the formats in [docs/FORMATS.md](docs/FORMATS.md). The areas that matter
most are authentication and roles (ADR 0007), signed changesets
(ADR 0010), review policies (ADR 0011) and blob garbage collection
(ADRs 0008 and 0012), because a flaw there can let a change land without
the checks it should pass, or lose data.

Out of scope: deployments that run without authentication on an untrusted
network, which the docs warn against.
