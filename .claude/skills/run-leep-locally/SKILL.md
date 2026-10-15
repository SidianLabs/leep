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
leep branch topic && leep switch topic   # branches; `leep switch -c NAME` creates too
leep tag v1                              # tags are immutable; `leep tag -d v1` deletes
leep branch                              # `*` marks the working copy's target
leep sync                                # moves to main's head, keeping uncommitted edits
leep stash -m wip && leep stash pop      # shelve and restore uncommitted work
leep restore a.txt                       # discard edits to a file (--source REV for another revision)
leep amend -m "better message"           # fold edits into an unlanded base commit
leep reset                               # undo the last unlanded commit, keeping its changes
leep merge feature                       # local merge commit; conflicts leave markers, then `leep commit`
leep merge --abort                       # (also `cherry-pick --abort`, `revert --abort`)
leep cherry-pick <changeset>             # copy one change onto the base
leep revert <changeset>                  # undo one landed change with a new local commit
leep rebase [main]                       # move unlanded commits onto another revision
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

## With signed changesets

```sh
leep keygen --out /tmp/leep/alice.key --name alice     # prints {"name":..,"public_key":"ed25519:.."}
echo '{"keys":[ <that line> ]}' > /tmp/leep/keys.json
cargo run -p leep-server -- --signing-keys-file /tmp/leep/keys.json --require-signatures main ...
```
Then `leep commit -m x && leep land` fails with `changeset ... is not signed by a
trusted key`, while `leep commit -m x --sign /tmp/leep/alice.key && leep land`
works (`LEEP_SIGNING_KEY=/tmp/leep/alice.key` is the same as `--sign`). `leep
show main` lists `Signed-by:`. The whole unlanded stack must be signed, so
sign every commit you land. See ADR 0010.

## With review and owners

Reviews need auth, and reviewers are the write tokens' names (ADR 0011).
```sh
echo '{"refs":["main"],"auto_approve":["comment_only","docs_only"],"docs_globs":["docs/**"]}' > /tmp/leep/review.json
cargo run -p leep-server -- --tokens-file /tmp/leep/tokens.json --review-policy-file /tmp/leep/review.json   # plus the flags above
# or protect refs without a file: --require-review main  (LEEP_REQUIRE_REVIEW=main,release)
```
Commit an `OWNERS` file (`alice`, one name per line) so a path has owners.
Then `leep land` fails with `changeset ... needs review: path ... needs
approval from one of [alice]`. Another user runs `leep review approve <id>`
(`--token` or `LEEP_TOKEN` of a write token; `-m` adds a comment; `leep review
reject <id> -m why` blocks), `leep review status <id>` lists each path's rule,
owners and approvers (exit 1 when not approved), and `leep reviews <id>` lists
verdicts. Owners are read from main's head, so a change that edits `OWNERS`
needs the existing owners. Comment-only, whitespace-only and docs-only changes
land without review when enabled. See ADR 0011.

## Clean up

Stop the server, run `pg_ctl stop` if you started Postgres, and `rm -rf /tmp/leep`.
