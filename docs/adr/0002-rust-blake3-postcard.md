# 0002. Rust, BLAKE3 and postcard
Status: Accepted
Date: 2026-10-15

## Context
We need predictable latency, safe concurrency, a fast hash and a compact,
deterministic encoding.

## Decision
Rust for every component. BLAKE3 for hashing, with domain separation by object
kind. postcard with `BTreeMap` maps as the canonical binary encoding; JSON
for the HTTP API.

## Consequences
Encodings are deterministic, but field and variant order are part of the
format (see FORMATS.md). Internally tagged serde enums are banned on persisted
types.
