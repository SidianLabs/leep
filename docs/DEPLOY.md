# Deploying Leep

Leep v0.1 has **no authentication or TLS**. Bind it to localhost or a trusted
network, or put an authenticating reverse proxy in front. Never expose port
7070 to the internet.

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

## Environment variables

| Variable | Default in image | Meaning |
|---|---|---|
| `LEEP_LISTEN` | `0.0.0.0:7070` | Listen address. |
| `LEEP_META` | `sqlite:///var/lib/leep/meta.db?mode=rwc` | Metadata store: `sqlite:`, `postgres://`, `mem://`. |
| `LEEP_BLOBS` | `file:///var/lib/leep/blobs` | Blob store: `file://`, `s3://bucket?endpoint=URL&region=R&allow_http=true`, `mem://`. |
| `LEEP_SERVER` | none | CLI only: server URL. |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | none | S3 credentials (standard AWS variables). |
| `RUST_LOG` | `info` | Log filter. |

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
