# Deploying Leep

Authentication is **off unless you give the server a tokens file**, and
`leep-server` never terminates TLS. For anything beyond localhost, enable
tokens (below) and put a TLS-terminating reverse proxy in front: bearer
tokens must not cross untrusted networks in cleartext.

The repo `Dockerfile` builds `leep-server`, `leep` and `leep-git` into one
`debian:bookworm-slim` image. The entrypoint is `leep-server`; it runs as the
non-root user `leep` (uid 10001) and keeps state under `/var/lib/leep`.

```sh
docker build -t leep:dev .
```

## Single container (SQLite + files)

```sh
docker run -d --name leep -p 127.0.0.1:7070:7070 \
  -v leep-data:/var/lib/leep leep:dev
curl -s localhost:7070/v1/health      # "ok"
```

Drive it with the CLI from the host or from the same image:

```sh
export LEEP_SERVER=http://127.0.0.1:7070
leep init && leep log
docker run --rm --network host -e LEEP_SERVER --entrypoint leep leep:dev log
```

## Compose stack (Postgres + blob volume)

`deploy/docker-compose.yml` runs Postgres 16 for metadata and `leep-server`
with blobs on a named volume, published on `127.0.0.1:7070` only.

```sh
docker compose -f deploy/docker-compose.yml up -d --build
docker compose -f deploy/docker-compose.yml down       # keep data
docker compose -f deploy/docker-compose.yml down -v    # delete data
```

The database password in that file is a dev-only placeholder. Change it, and
move it out of the file (for example into an `.env` file or your secret
manager), before any shared use.

### Blobs in S3 or an S3-compatible store

To keep blobs in object storage instead of the volume, override `LEEP_BLOBS`
and the standard AWS credential variables on `leep-server`, for example in a
`docker-compose.override.yml`:

```yaml
services:
  leep-server:
    environment:
      LEEP_BLOBS: s3://my-bucket?region=eu-west-1
      # S3-compatible stores (Ceph RGW, Garage, SeaweedFS, ...):
      # LEEP_BLOBS: s3://leep?endpoint=http://s3.internal:9000&region=us-east-1&allow_http=true
      AWS_ACCESS_KEY_ID: ...
      AWS_SECRET_ACCESS_KEY: ...
```

The bucket must already exist. The stack does not bundle an object store,
so it depends only on the `postgres` image.

The image has no `curl`, so it declares no `HEALTHCHECK`. Probe
`GET /v1/health` from your orchestrator instead.

## Authentication

Generate tokens with the server binary and mount a tokens file holding only
their digests (see ADR 0007):

```sh
docker run --rm --entrypoint leep-server leep:dev gen-token --name ci --role write
# token: <shown once>          entry: {"name":"ci","role":"write","hash":"blake3:..."}
echo '{"tokens":[<entries, comma separated>]}' > tokens.json
docker run -d -p 127.0.0.1:7070:7070 -v leep-data:/var/lib/leep \
  -v "$PWD/tokens.json:/etc/leep/tokens.json:ro" \
  -e LEEP_TOKENS_FILE=/etc/leep/tokens.json leep:dev
```

In compose, add the same volume and `LEEP_TOKENS_FILE` to `leep-server`.
Clients pass the token with `LEEP_TOKEN`, `leep --token` or
`leep init --token`. `GET /v1/health` stays public for probes.

## Signed changesets

Optional. Authors sign changesets with Ed25519 keys; the server trusts only the
keys you list, and can refuse to land unsigned work on chosen refs (ADR 0010).

```sh
leep keygen --out ~/.config/leep/alice.key --name alice   # prints the keys-file entry
echo '{"keys":[<entries, comma separated>]}' > signing-keys.json
docker run -d -p 127.0.0.1:7070:7070 -v leep-data:/var/lib/leep \
  -v "$PWD/signing-keys.json:/etc/leep/signing-keys.json:ro" \
  -e LEEP_SIGNING_KEYS_FILE=/etc/leep/signing-keys.json \
  -e LEEP_REQUIRE_SIGNATURES=main leep:dev
```

`--signing-keys-file` enables `POST /v1/changesets/{id}/signatures`, which
needs a write token when auth is on. `--require-signatures REF` (repeatable;
the environment variable is comma-separated) makes `land` onto `REF` fail
with 400 unless every changeset it adds is signed by a listed key, and needs
the keys file. Both are read at startup; restart to change them. Authors
commit with `leep commit --sign KEYFILE` or `LEEP_SIGNING_KEY=KEYFILE`; the key
file must be mode 0600. Raw ref updates (`POST /v1/refs/{name}`) are checked
the same way. `leep-git` import writes to the stores directly and is not
covered, so run it only as an operator.

## Review and owners

Optional, and it needs authentication: reviewers are the write-role tokens, so
`OWNERS` files name token names (ADR 0011). Put `OWNERS` files in the tree
(`alice`, one name per line, `#` comments, `noparent`), then protect refs:

```sh
cat > review-policy.json <<'JSON'
{ "refs": ["main"],
  "auto_approve": ["comment_only", "whitespace_only", "docs_only"],
  "docs_globs": ["**/*.md", "docs/**"] }
JSON
docker run -d -p 127.0.0.1:7070:7070 -v leep-data:/var/lib/leep \
  -v "$PWD/tokens.json:/etc/leep/tokens.json:ro" \
  -v "$PWD/review-policy.json:/etc/leep/review-policy.json:ro" \
  -e LEEP_TOKENS_FILE=/etc/leep/tokens.json \
  -e LEEP_REVIEW_POLICY_FILE=/etc/leep/review-policy.json leep:dev
```

`--review-policy-file` sets the auto-approval rules and lists `refs` that
require review; `--require-review REF` (repeatable; the environment variable is
comma-separated) adds more refs with the same rules. Both need the tokens file
and are read at startup. `land` onto a protected ref fails with 400 unless
every changeset it adds is approved: each changed path needs an approval from
an owner of that path as listed in the ref's current head (any one approval
from anyone where no `OWNERS` file applies), or an enabled auto rule, and no
owner may have rejected it. Reviewers use `leep review approve|reject REV`;
`leep review status REV` shows what is missing. It composes with
`--require-signatures` (both must pass), and raw ref updates are checked too.
Review endpoints answer 400 on a server without a tokens file. Reviews are
looked up once per write token, so removing a token drops its reviews.

## Environment variables

| Variable | Default in image | Meaning |
|---|---|---|
| `LEEP_LISTEN` | `0.0.0.0:7070` | Listen address. |
| `LEEP_META` | `sqlite:///var/lib/leep/meta.db?mode=rwc` | Metadata store: `sqlite:`, `postgres://`, `mem://`. |
| `LEEP_BLOBS` | `file:///var/lib/leep/blobs` | Blob store: `file://`, `s3://bucket?endpoint=URL&region=R&allow_http=true`, `mem://`. |
| `LEEP_TOKENS_FILE` | none | Tokens file; enables authentication. |
| `LEEP_SIGNING_KEYS_FILE` | none | Trusted author keys file; enables signature submission. |
| `LEEP_REQUIRE_SIGNATURES` | none | Comma-separated refs that only accept signed lands; needs the keys file. |
| `LEEP_REVIEW_POLICY_FILE` | none | Review policy JSON (auto-approval rules, refs that require review); needs the tokens file. |
| `LEEP_REQUIRE_REVIEW` | none | Comma-separated refs that only accept reviewed lands; needs the tokens file. |
| `LEEP_SERVER` | none | CLI only: server URL. |
| `LEEP_SIGNING_KEY` | none | CLI only: private key file for `leep commit --sign`. |
| `LEEP_TOKEN` | none | CLI only: bearer token. |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | none | S3 credentials (standard AWS variables). |
| `LEEP_REQUEST_TIMEOUT` | `5m` | Abort any request that runs longer (`90s`, `5m`; `0` disables). `leep-server gc --fence` must exceed it; see [OPERATIONS.md](OPERATIONS.md#blob-garbage-collection). |
| `LEEP_METRICS_LISTEN` | off | Address for a separate, unauthenticated `GET /metrics` listener; keep it private. See [OPERATIONS.md](OPERATIONS.md). |
| `LEEP_LOG_FORMAT` | `text` | `text` or `json` (one object per line). |
| `RUST_LOG` | `info` | Log filter. |

`leep-server gc` takes `--meta`/`--blobs` (or their variables) plus `--grace`,
`--fence`, `--dry-run` and `--unsafe-no-fence`, all flags only; see
[OPERATIONS.md](OPERATIONS.md#blob-garbage-collection).

`--cache-bytes` and `--max-body-bytes` are flags only; pass them after the
image name, e.g. `docker run leep:dev --cache-bytes 0`.

## Persistence and backup

- SQLite + files: everything is in the `/var/lib/leep` volume. Stop the
  container (or use `sqlite3 .backup`) before copying `meta.db`.
- Postgres: `pg_dump` is the backup of record for refs, changesets and
  derived data.
  ```sh
  docker compose -f deploy/docker-compose.yml exec postgres \
    pg_dump -U leep -Fc leep > leep.dump
  ```
- Blobs: objects are content-addressed and immutable, so a plain copy or
  incremental sync is safe at any time, even while the server runs (for
  S3, `aws s3 sync` or your store's equivalent). Keep both backups: metadata that
  references a missing blob is unusable.

## Upgrades

Schema migrations run automatically when the server connects to the
metadata store. Rebuild the image, then recreate the container
(`docker compose up -d --build`). Take a backup first: there is no
automatic downgrade. Servers are stateless, so several can share one
Postgres and bucket.
