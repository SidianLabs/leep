# 0003. Changesets store deltas; everything else is derived
Status: Accepted
Date: 2026-10-15

## Context
Git commits point at full trees, which couples write cost to repository shape
and makes every index part of the core format.

## Decision
A changeset stores only its changed paths relative to its first parent (like
Mononoke's bonsai changesets). Trees, generation numbers, ancestry, blame and
history are derived data, computed by pure functions of the changeset and its
parents' values, and stored under `(kind, key)`.

## Consequences
New indexes ship without format or client changes. Derivation must be
iterative and idempotent. The first read of an un-derived changeset pays the
derivation cost.
