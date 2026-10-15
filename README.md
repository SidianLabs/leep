# Leep

An open-source, server-centric version control system built for repositories
where most commits come from AI agents and CI. Inspired by Anthropic's Scion,
Meta's Mononoke and Zed's DeltaDB. Git-compatible through an import/export
bridge.

Status: early development (v0.1).

- [Architecture](docs/ARCHITECTURE.md)
- [Formats and compatibility](docs/FORMATS.md)
- [Decision records](docs/adr/)
- [Roadmap](docs/ROADMAP.md)
- [Contributing](CONTRIBUTING.md) and the [agent guide](CLAUDE.md)

## Layout

- `crates/leep-core`: data model, storage traits, HTTP API types
- `crates/leep-store`: blob storage backends (filesystem, S3/MinIO, LRU cache)
- `crates/leep-meta`: metadata backends (SQLite, Postgres)
- `crates/leep-derive`: derived data and repository operations
- `crates/leep-server`: the `leep-server` HTTP server
- `crates/leep-cli`: the `leep` command-line client
- `crates/leep-git`: git import/export

## Build

```sh
cargo build --release
cargo test
```

The repository also still contains the earlier ELEVATE static website files at
the top level; they are unrelated to Leep.
