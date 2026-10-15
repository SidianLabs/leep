# 0001. Server-centric, Scion-style design
Status: Accepted
Date: 2026-10-15

## Context
Git assumes offline clients that hold full history. Leep's main clients are
always-online agents and CI jobs that need a few files at one revision, at
very high merge rates. Anthropic's Scion showed that a server-centric design
with derived data and no merge queue handles 30k merges a day on a 100 GB
history.

## Decision
The server owns history and runs every repository operation. Clients hold a
content-addressed cache and fetch on demand. Git compatibility is provided by
an import/export bridge, not by the wire protocol.

## Consequences
Clients are thin and can't work offline. Server availability matters. All
logic can evolve server-side without client upgrades.
