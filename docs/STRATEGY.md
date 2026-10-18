# Strategy

How Leep becomes an independent version control system that a company can
run as its whole source-control infrastructure, and later a hosted platform.
The decision behind this is recorded in [PRODUCT.md](PRODUCT.md) (2026-10-18);
the milestones that carry it out are in [ROADMAP.md](ROADMAP.md).

## Where Leep stands

Leep follows the design of Anthropic's Scion and Meta's Mononoke: a
server-centric store with content-addressed objects, landing on the server,
and history indexes computed there. Measured on a 4-vCPU VPS (see
[PERFORMANCE.md](PERFORMANCE.md)):

- **Many writers on one trunk:** 117 lands/s with 64 agents landing on one
  ref; a single land takes about 3 ms.
- **History at the scale of Linux** (1.48M commits): last change to a file in
  2-55 ms, merge-base in 3-54 ms, blame of a long-lived file in 0.13 s once
  derived.
- **Storage:** 3.3 GB of packed blobs for all of Linux against git's 3.8 GB
  repository; the path-history index (24 GB on Linux) is the open problem.
- **Safety:** crash and fault-injection tests, fuzzed decoders, signed
  changesets, owner review and semantic auto-approval enforced by the server.

What it lacks is everything around the engine that a company relies on:
git-compatible access, multiple repositories, enterprise identity, high
availability and a review interface. That is the road below.

## What we are building

Three layers. Each is useful on its own and builds on the one before.

### 1. Leep core: open source, self-hosted, forkable

The server, CLI, MCP server, git bridge, storage backends and every format,
under Apache-2.0. A company can fork it, run it on its own machines and never
talk to us. Formats are documented so nobody is locked in.

### 2. Deployable as a company's complete source control

What a platform team needs to replace its git hosting with Leep:

| Need | Status |
|---|---|
| Huge repositories and many concurrent writers | Done; see above |
| Durability, crash safety, backup and restore, upgrades | Done (M5, M6) |
| Policy: signed changes, owner review, required approval | Done (M3) |
| Agent access: MCP, server-side search and history | Done (M3, M4) |
| git clients, IDEs and CI work unchanged | Missing: live mirror, then the git wire protocol (M7) |
| Many repositories, per-repository roles, orgs and teams | Missing (M8) |
| SSO (OIDC, SAML), audit log | Missing (M8) |
| Events and webhooks for CI and tools | Missing (M8) |
| High availability and failover; multi-region | Design allows several servers on one database; failover untested (M10) |
| Large binary files | Missing (M10) |
| A web interface for browsing and review | Read-only view planned (M9); full review UI in the platform (M12) |
| Storage efficiency on giant histories | In progress: path-history index v4 (M5) |

### 3. A hosted platform, later

Leep's own hosted service, built on the same open core: organizations,
repositories, a web interface for code, history and review, and the features
that make it the best home for agent-heavy teams (per-agent identity and
provenance, merge queues that land thousands of changes a day, agent APIs).
Self-hosted and hosted Leep run the same code; the hosted service adds
multi-tenancy, billing and operations.

## Principles

- **Independent, but git-compatible.** jj and Sapling show the path: their
  own model and workflow, yet they read and write git so nobody has to move
  everything on day one. Leep keeps its own data model and API and adds git
  access as a door in, not as its core.
- **Open core first.** Every feature a company needs to run Leep for itself
  goes in the open core. What only a hosted multi-tenant service needs can be
  separate.
- **No lock-in.** Formats are documented and stable after 1.0; export to git
  always works.
- **Agents are first-class users.** Every feature is designed for agents
  landing at high rates as well as for people.
- **Measured.** Performance claims come with benchmarks on real histories
  (git/git, Linux) in PERFORMANCE.md, and regressions are fixed before new
  features.

## Order of work

1. **Storage and query efficiency on giant histories** (M5): the path-history
   index (24 GB on Linux), an author index for `log --author`, faster cold
   listings and first blames.
2. **Git compatibility** (M7): a live two-way mirror with GitHub and GitLab
   first, so a team can adopt Leep beside its existing hosting; then the git
   wire protocol, so `git clone`, `git push`, IDEs and CI talk to Leep
   directly.
3. **Multi-repository and identity** (M8): many repositories per server,
   orgs, teams, per-repository roles, OIDC and SAML, audit log, webhooks.
4. **Collaboration** (M9): proposals (Leep's equivalent of pull requests)
   with review threads, required checks and early conflict warnings from the
   land queue; a web view of history, diffs and proposals.
5. **Enterprise operations** (M10): high availability with tested failover,
   a Helm chart, large files, regional caches.
6. **Release candidate and 1.0** (M11): frozen formats and `/v1` API.
7. **Hosted platform** (M12).

Design partners from the first market (teams running many coding agents on
one codebase) shape M8 onward.

## Open decisions

- **License of the platform layer.** The core stays Apache-2.0. The hosted
  service's own code (multi-tenant control plane, billing) can be closed or
  source-available; the CLA keeps this choice open. Decide before outside
  contributors arrive in numbers.
- **Name and governance** of the open-source project once others contribute.
