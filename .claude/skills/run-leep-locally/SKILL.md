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

<!-- BEGIN with-authentication -->
## With authentication

Auth is off unless the server gets a tokens file. Create tokens and a file:

```sh
cargo run -q -p leep-server -- gen-token --name dev --role write   # prints token: ... and entry: {...}
cargo run -q -p leep-server -- gen-token --name viewer --role read
echo '{"tokens":[ <entry lines, comma separated> ]}' > /tmp/leep/tokens.json
cargo run -p leep-server -- --tokens-file /tmp/leep/tokens.json ...   # same flags as above
```
The file holds `blake3:` digests only; the plaintext token is shown once, so
keep it. `GET /v1/health` stays public. Then, for the client:

```sh
export LEEP_TOKEN=<write token>            # or: leep --token ... / leep init --token ...
leep init                                   # `leep init --token T` also saves it to .leep/config.json (0600)
```
Precedence is `--token`, then `LEEP_TOKEN`, then `.leep/config.json`. With a read
token, `leep land` fails with `token lacks write access`; with none, commands
fail with `authentication required: set LEEP_TOKEN`.
<!-- END with-authentication -->

## Clean up

Stop the server, run `pg_ctl stop` if you started Postgres, and `rm -rf /tmp/leep`.
