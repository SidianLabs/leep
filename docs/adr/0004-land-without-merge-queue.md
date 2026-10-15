# 0004. Land by server-side rebase and CAS, with no merge queue
Status: Accepted
Date: 2026-10-15

## Context
At agent merge rates, a merge queue that serializes on tests becomes the
bottleneck: hundreds of changes land while one test run finishes.

## Decision
`land` rebases onto the current head on the server and compare-and-swaps the
ref, retrying when it loses the race. Conflicts are returned to the client
and never auto-resolved on the server. Tests run after merge and before
deploy.

## Consequences
`main` can briefly contain a broken combination of changes. Deploy gates and
fast reverts are required. Agents resolve conflicts client-side and land
again.
