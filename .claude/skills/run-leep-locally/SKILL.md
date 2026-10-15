---
name: run-leep-locally
description: Start a local Leep stack (Postgres or SQLite metadata, filesystem blobs, leep-server) and drive it with the leep CLI to verify a change end to end. Use when asked to run, demo, or manually test Leep.
---

# Run Leep locally

## Metadata store

- Quickest: SQLite. Use `--meta sqlite:///tmp/leep/meta.db?mode=rwc`.
- Postgres (closer to production), if Postgres 16 is installed:
  ```sh
  export PGDATA=/tmp/leep/pg && mkdir -p $PGDATA
  /usr/lib/postgresql/16/bin/initdb -U postgres -A trust $PGDATA
  /usr/lib/postgresql/16/bin/pg_ctl -l /tmp/leep/pg.log -o "-p 5433 -k /tmp" start
  export LEEP_TEST_POSTGRES_URL=postgres://postgres@localhost:5433/postgres
  ```

## Server

```sh
cargo run -p leep-server -- \
  --listen 127.0.0.1:7070 \
  --meta "sqlite:///tmp/leep/meta.db?mode=rwc" \
  --blobs file:///tmp/leep/blobs
```
Check that `curl -s localhost:7070/v1/health` prints `"ok"`. Run `leep-server --help`
for the authoritative flags, since they may have changed since this was written.

## Client smoke test

```sh
export LEEP_SERVER=http://127.0.0.1:7070
mkdir -p /tmp/leep/wc && cd /tmp/leep/wc
leep init                       # binds this directory to the server
echo hello > a.txt
leep commit -m "first"          # uploads missing blobs, creates the changeset
leep land                       # rebase + CAS onto main
leep log && leep cat main a.txt
```
Again, run `leep --help` for the current subcommands.

## With Docker

```sh
docker build -t leep:dev .
docker run -d --name leep -p 127.0.0.1:7070:7070 -v leep-data:/var/lib/leep leep:dev
# or the Postgres stack (blobs on a volume):
docker compose -f deploy/docker-compose.yml up -d --build
```
Then use the client smoke test above against `http://127.0.0.1:7070`. Tear down
with `docker rm -f leep` or `docker compose -f deploy/docker-compose.yml down -v`.
See `docs/DEPLOY.md`.

## Clean up

Stop the server, run `pg_ctl stop` if you started Postgres, and `rm -rf /tmp/leep`.
