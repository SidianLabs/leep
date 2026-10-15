# 0007. Token authentication
Status: Accepted
Date: 2026-10-15

## Context
v0.1 has no authentication and assumes a trusted network. v0.2 needs a
minimal way to run a server where people and agents are not all trusted to
write, without committing to an identity system yet.

## Decision
- The server loads static bearer tokens from a JSON file given by
  `--tokens-file` or `LEEP_TOKENS_FILE`. It is read once at startup; changing
  tokens means restarting (SIGHUP reload is future work).
- The file holds `blake3:<hex>` digests, never plaintext. Each entry has a
  `name` (unique, logged) and a `role`: `read` or `write`. A file with plaintext
  tokens, bad digests, duplicate names or no tokens fails startup. Tokens are
  high-entropy random values, so a fast hash is sufficient.
- `leep-server hash-token` (token on stdin) prints the digest;
  `leep-server gen-token --name N --role R` prints a fresh 32-byte hex token
  once, plus the entry to add to the file.
- Presented tokens are hashed and compared against every entry in constant
  time.
- Auth is off when no file is configured; the server logs a WARN at startup.
- When on, every endpoint except `GET /v1/health` needs
  `Authorization: Bearer <token>`: missing or invalid is 401 with
  `WWW-Authenticate: Bearer`. `read` may use GET endpoints and
  `POST /v1/blobs/missing` (it only queries); other requests with a read
  token are 403.
- It is an axum middleware (`router_with_auth`); `router` is unchanged. The
  token name, never the token, is recorded in a debug-level `auth` span.
- The CLI takes the token from `--token`, then `LEEP_TOKEN`, then `token` in
  `.leep/config.json`. `leep init --token` stores it there with mode 0600.
  The CLI never prints it and maps 401/403 to fixed messages.
- No `leep-core` type changes, so this is additive under FORMATS.md rule 4.

## Consequences
- Simple to operate and audit; no database or external service.
- Tokens are coarse: two roles, no expiry, no revocation without a restart,
  and no per-user identity beyond the name.
- Tokens in `.leep/config.json` are plaintext on the client; 0600 limits
  exposure. Without TLS, bearer tokens are visible on the wire, so terminate
  TLS in front of the server.
- Future work builds on the middleware seam: OIDC or JWT validation, mTLS,
  and per-directory ACLs evaluated against changeset paths (v0.3 review and
  ownership), plus signed changesets for provenance.
