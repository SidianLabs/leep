# syntax=docker/dockerfile:1
# Keep the builder's minor version in step with rust-toolchain.toml. That
# file is not copied: rustup would otherwise download the exact patch release
# instead of using the toolchain already in this image.
FROM rust:1.97-bookworm AS builder
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates crates
# Cache mounts keep the registry and build artifacts between builds without
# baking them into layers.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked -p leep-server -p leep-cli -p leep-git -p leep-mcp \
 && mkdir -p /out \
 && cp target/release/leep-server target/release/leep target/release/leep-git target/release/leep-mcp /out/

FROM debian:bookworm-slim
# No HEALTHCHECK: the slim image has no curl/wget and the server has no
# self-check flag. Probe from outside: GET /v1/health (liveness) and
# GET /v1/ready (readiness: both stores answer).
# git and the ssh client are for `leep-git mirror run`, which pushes and
# fetches through the system git (ADR 0046).
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates git openssh-client \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --system --uid 10001 --home-dir /var/lib/leep --shell /usr/sbin/nologin leep \
 && mkdir -p /var/lib/leep && chown leep:leep /var/lib/leep

COPY --from=builder /out/ /usr/local/bin/

ENV LEEP_LISTEN=0.0.0.0:7070 \
    LEEP_META="sqlite:///var/lib/leep/meta.db?mode=rwc" \
    LEEP_BLOBS=file:///var/lib/leep/blobs

USER leep
WORKDIR /var/lib/leep
VOLUME /var/lib/leep
EXPOSE 7070
ENTRYPOINT ["leep-server"]
