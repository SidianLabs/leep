# 0005. HTTP/JSON API instead of gRPC
Status: Accepted
Date: 2026-10-15

## Context
gRPC needs `protoc` in every build environment and is harder for agents and
scripts to call directly.

## Decision
A versioned HTTP/JSON API (`/v1`) built with axum. Request and response types
live in `leep_core::api` and are shared by the server and the client. Blob
bodies are raw bytes.

## Consequences
The API is easy to debug and call with curl or from agents. It is slightly
less efficient than protobuf, which is acceptable because blobs are raw bytes.
A binary encoding can be added later through content negotiation.
