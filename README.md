# Leep

An open-source, server-centric version control system built for repositories
where most commits come from AI agents and CI. Inspired by Anthropic's Scion,
Meta's Mononoke and Zed's DeltaDB. Git-compatible through an import/export
bridge. Agent provenance (`agent.model`, `agent.session`) is queryable:
`leep log --agent-model M`, `leep blame --who`, `leep provenance`. Code is
searchable on the server without a checkout: `leep search -i -E 'fn\s+main'`.

Status: early development (v0.1).

- [Architecture](docs/ARCHITECTURE.md)
- [Formats and compatibility](docs/FORMATS.md)
- [Decision records](docs/adr/)
- [Roadmap](docs/ROADMAP.md)
- [Deploying with Docker](docs/DEPLOY.md)
- [Operating: logs and metrics](docs/OPERATIONS.md)
- [Runbooks: what to do when an alert fires](docs/RUNBOOKS.md)
- [The `leep` command line: exit codes and the agent contract](docs/CLI.md)
- [Using Leep from AI agents (MCP)](docs/MCP.md)
- [Contributing](CONTRIBUTING.md) and the [agent guide](CLAUDE.md)

## Layout

- `crates/leep-core`: data model, storage traits, HTTP API types
- `crates/leep-store`: blob storage backends (filesystem, S3/MinIO, LRU cache)
- `crates/leep-meta`: metadata backends (SQLite, Postgres)
- `crates/leep-derive`: derived data and repository operations
- `crates/leep-server`: the `leep-server` HTTP server
- `crates/leep-cli`: the `leep` command-line client
- `crates/leep-mcp`: the `leep-mcp` MCP server for AI agents ([docs/MCP.md](docs/MCP.md))
- `crates/leep-git`: git import/export and the live git mirror (`leep-git mirror`)
- `crates/leep-fuzz-targets` and `fuzz/`: fuzz targets ([docs/FUZZING.md](docs/FUZZING.md))

## Build

```sh
cargo build --release     # every crate, including test and benchmark tools
cargo test
```

To run a server without Docker, build only the shipped binaries and follow
[DEPLOY.md, "Without Docker"](docs/DEPLOY.md#without-docker-binaries-postgres-filesystem-blobs).

## Docker

```sh
docker build -t leep:dev .
docker run -p 127.0.0.1:7070:7070 -e LEEP_INSECURE_NO_AUTH=1 -v leep-data:/var/lib/leep leep:dev
```

See [docs/DEPLOY.md](docs/DEPLOY.md) for the Postgres compose stack and S3 storage.

## License

Apache-2.0; see [LICENSE](LICENSE). To report a security problem, see
[SECURITY.md](SECURITY.md). Changes are recorded in
[CHANGELOG.md](CHANGELOG.md).
