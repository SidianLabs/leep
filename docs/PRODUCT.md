# Product direction

The maintainer agreed these decisions on 2026-10-15. They guide what goes on
[the roadmap](ROADMAP.md) and how it is ordered. Changing one is a maintainer
decision: record the new decision and its date here.

## Decision of 2026-10-18: an independent system, then a platform

The maintainer widened the goal. Leep is to be an independent version control
system, as Scion, Sapling and jj are, that a company can fork and deploy as
its complete source-control infrastructure; later, Sidian Labs builds a
hosted platform on it, as GitHub is for git, to serve everyone. The first
market below is unchanged and still decides what comes first. What changes:

- **The git bridge becomes a full compatibility layer.** A live two-way
  mirror first, then the git wire protocol, so git clients, IDEs and CI can
  use a Leep server directly. Leep's own model and API stay primary.
- **A review interface is now in scope**: proposals with review threads in
  the core, a web interface in the platform. Teams may still mirror to
  GitHub or GitLab while they move.
- **Everything a company needs to run Leep for itself goes in the open
  core**: multi-repository, identity, audit, events, high availability. Only
  what a hosted multi-tenant service needs (control plane, billing) can be
  separate.

[STRATEGY.md](STRATEGY.md) lays out the layers and the order of work; the
[roadmap](ROADMAP.md) carries them as milestones M7 to M12.

## Who Leep is for first

**Teams that run many coding agents against one codebase.** A platform or
developer-productivity team at a company where dozens of agents, alongside
people, propose and land changes to the same repository every day. GitHub
stays their human-facing mirror for now.

This is where git hurts most and where Leep is already strongest:

- **Landing:** `land` with no merge queue holds up under many concurrent
  writers.
- **Provenance:** every change records which agent, model and session made it.
- **Policy:** signed changesets, owner review and semantic auto-approval are
  enforced by the server.
- **Agent access:** the MCP server, plus server-side search and history, let
  agents work without a checkout.

Monorepo-scale companies outgrowing git are the second market. Leep's
architecture serves them too, but they adopt more slowly, so features are not
prioritised for them before the first market is served.

### Not goals for now

- A desktop GUI client.
- Supporting every git workflow. The git compatibility layer covers clone,
  fetch, push and mirroring, not every git command or hosting feature.

(Until 2026-10-18 this list also excluded a review UI and serving git; see
the decision above.)

## Business model

**Open-source core, with a hosted service later.**

- The server, CLI, MCP server, git bridge, storage backends and every format
  are open source and stay that way. Anyone can self-host Leep, and the
  formats are documented so nobody is locked in.
- Sidian Labs may later run Leep as a hosted service, and may sell what large
  teams need on top of the core, such as SSO, audit logs, a multi-tenant
  control plane, regional caches and support.
- Git compatibility (M7), then multi-repository support and real identity
  (M8), are prerequisites for both self-hosted replacement and a hosted
  service. That is why they come before scale-out on the roadmap.

## License and contributions

- **License:** Apache-2.0, for the widest adoption and the easiest approval
  inside companies.
- **Contributor License Agreement:** every outside contributor signs one
  before their first contribution is merged (see [CLA.md](../CLA.md)). This
  keeps the option to change licensing terms later, for example if another
  company resells Leep as a hosted service.

## Proving it on ourselves

Leep's own development moves onto a Leep server early in M5, with GitHub as a
mirror, instead of waiting for M7. Every problem found this way is one a
design partner won't hit, and it is the most convincing demo there is.

## Design partners

Before M8, find two or three teams from the first market to run Leep for real
work. M5 to M7 (hardening, operations, git compatibility) can proceed on our
own judgement. M8 onward (identity, collaboration, the web view,
integrations) is shaped by what those teams need.

## How work is reviewed

- The maintainer reviews and merges every pull request.
- Agents, including AI coding agents, open PRs, drive them to green CI and
  answer review comments, but never merge.
- Every PR is small enough to review in one sitting.
