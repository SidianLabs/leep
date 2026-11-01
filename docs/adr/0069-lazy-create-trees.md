# 0069. Creates leave their tree nodes out
Status: Accepted
Date: 2026-11-01

## Context
After ADR 0068 the land leader of the 256-agent benchmark (release build,
Postgres, the server on 2 vCPU) is only 50 to 60% busy; the rest of each
agent's cycle is its create and upload. A create rebuilds the directories
on its paths, about ten tree nodes for a change three levels down, and
writes each as a loose file with `sync_all`, plus the shard directory
syncs: 2,000 to 3,000 fsyncs a second on the request path. The nodes are
mostly garbage. A land under load is a rebased copy onto a newer head,
with a different tree that the land batch stores in its pack; the
create's own nodes are referenced only by an unlanded original that
nothing reads again. They were also about 10.8 of the files each land left
behind.

A tree is a pure function of the first parent's tree and the changeset's
`changes` (`root_tree` in ARCHITECTURE §5). The nodes a create writes are
a cache of that function, which only reads of the unlanded changeset
itself use.

## Decision

### 1. A create stores no tree node
With `--lazy-create-trees on` (the default) `POST /v1/changesets` builds
the tree in memory exactly as before: the same validation, the same
delete and file/directory clash errors, the same root hash, which its
`root_tree` row holds. It stores the changeset with its rows as before,
and still flushes every content it names (ADR 0044), but writes none of
the nodes it rewrote. Changesets the server builds (merges, rebased
copies, a land's copies) and imports through the library keep storing
their trees: they are the ref's history, which most reads walk.

A create stores its tree anyway when its first parent's tree is already
`MAX_LAZY_DEPTH` (8) derivations from a stored tree, which bounds what a
read derives (section 3), and when the root it built is already stored
(an identical tree, or a root left by an interrupted write: storing the
tree again makes it whole).

`off` restores the old behaviour. Either setting reads trees written
under the other, so a server may switch at any restart, and the flag
exists to compare the two.

### 2. The invariant
"Every stored changeset's tree is stored" becomes:

> A stored changeset's tree is either **stored whole** (its root node is
> present, and so is every node below it), or its root node is **absent**
> and the tree is its first parent's tree with its `changes` applied, the
> first parent's tree being stored whole or itself derivable this way,
> within `MAX_LAZY_CHAIN` (64) first parents.

"A present root means a whole tree" is what makes absence cheap to detect
(one read of the root) and what every reader below relies on. Writes keep
it:
- A tree written loose writes its root last, after the other nodes are
  written and their shard directories flushed (`put_rescuing_many_loose`),
  so no reordering by the filesystem can leave a root without its nodes:
  an eager create pays a second, small flush for it. A land batch's pack
  is published atomically.
- A tree built on an unstored base (an eager create at the depth bound, a
  copy onto an unstored head, a merge) stores the base's nodes it keeps as
  well (`Repo::unstored_below`), so it is whole. A stored base costs
  nothing extra: one read of its root, which the build reads anyway.
- A land that moves a ref to a changeset itself (a fast-forward, the first
  land on a ref) stages its unstored nodes in the batch, the root in a
  second wave after the rest is durable (`Staging::stage_tree`); on a
  store that packs batches that wave is a second pack, so a batch with a
  fast-forward writes two packs (the nodes, then the roots). A ref's
  head is therefore stored whole after every land, so the fast path's
  read of the head and every trunk walk read stored nodes. Members of a
  fast-forwarded stack below its tip stay unstored, bounded as above, as
  does a changeset a plain ref update (`POST /v1/refs/{name}`, the git
  mirror's imports) points a ref at: readers derive it, at most
  `MAX_LAZY_DEPTH` builds away.
- Nodes a land batch stages are kept in its memo, so a staged node read
  back through the store never enters the cache of stored nodes.

The changeset format, the node format, hashing and every row are
unchanged (FORMATS.md has nothing new); there are only fewer objects.

### 3. Reads derive missing nodes
`Repo::read_tree`, `read_trees` and `read_tree_view` ask
`Repo::lazy_node` when the store lacks a node. It answers from a cache of
derived nodes (16 MiB by default, part of `--derive-cache-bytes`), or
derives the tree of the changeset that holds the node: `root_tree` (and
every batched read of `root_tree` rows that is followed by a tree read)
records root -> changeset in one map, and a derivation records each node
it builds in another, consulted first, so the busy root map never pushes
out a derived tree's nodes. A node no changeset known to hold it was
looked up for is missing, as before. A read of many nodes (a level of a
diff) derives each changeset's tree once for all of them, and a node that
was the cached ancestor a derivation started from, evicted meanwhile, is
found by deriving again from stored trees.

`Repo::derive_tree` walks first parents iteratively until a stored tree
(or one derived earlier and still cached), at most `MAX_LAZY_CHAIN`, then
builds each tree up the chain in memory with the create's own code,
reading only stored nodes and the chain's own (never deriving again, so it
never recurses; the stored directories on the changed paths are read a
level at a time in one batched read each, not one by one), and checks
each against its `root_tree` row: a mismatch
is `Storage("corrupt tree of changeset ...")` and is never served. Each
build costs what the create's build cost, without the writes.

Reads never store what they derive. Unlanded changesets are read rarely
(a client inspecting its own work, the slow path of its land), deriving a
few directories is cheaper than the fsyncs storing them would cost, and
storing would turn reads into writes and write the garbage this decision
removes. A land's slow path derives the stack's trees in memory inside
the batch, which costs about the copy's own `apply_changes`: the batch
attempt keeps every node it derived (`NodeMemo`, apart from the nodes it
may treat as stored), so the merge reads the land's tree from there, and
creates keep only the depth of their tree, so that cache is not churned by
every create. Readers do not ask the store for a node known to be
unstored: one derived already, or the root of a tree this process created
without its nodes (`Repo::lazy_first`). The first version asked the store
for every node of the land's tree before falling back to deriving it; on
the filesystem store each miss rescans `packs/`, and that, not the
derivation (about 1 ms), made a slow-path replay take 82 to 93 ms instead
of 2.3 to 4.2 ms in the 256-agent benchmark (20 ms against 2.8 ms in a
30 s local run of the same harness, 3.3 ms against 3.0 ms after).
`a_slow_path_land_stays_off_the_store_and_stores_only_its_paths` checks
that such a land misses the store at most once (for the root, when
another process made the create) and stores only the directories on its
paths. The fast path reads only the head and the land's base, both stored.

### 4. Everything else that walks trees
- **GC mark** walks a lazy changeset's derived tree: what it marks is
  what the eager tree marked, plus rows for node hashes that are not
  stored, which are harmless (a row only keeps an object). The contents
  are found either way; condemnation, rescue and collect (ADR 0012, 0059)
  are unchanged, and a node later stored by a fast-forward is written
  through the rescue check, as a create's were.
- **`gc --full --backup-fence`** keeps the trees of tombstoned changesets,
  and with them the contents a backup that copied the changeset needs.
  Once a changeset is deleted its tree no longer derives, so the
  changeset sweep stores the unstored trees of a batch it deletes (one
  pack of nodes, then the roots) before it writes their tombstones; blob
  GC frees them once the fence has passed. This writes in the background
  what the creates no longer write on the request path, and only for
  changesets the sweep deletes.
- **`gc-unreachable`** walks derived nodes for the contents they name and
  counts no tree node for them: an unstored node frees nothing.
- **`verify`** accepts an absent node when the changeset's tree derives
  and matches its row, counts it in `lazy_tree_nodes`, and checks the
  contents below it; a tree that does not derive is `missing_tree`.
  `tree-trigrams-v1` rows of unstored nodes are skipped by verify and
  repair, like rows of collected nodes.
- **`repair rederive root_tree`** recomputes through `RootTree::derive`,
  which stores the tree it builds: storing is always allowed.
- **Pack ordering** for repacks may name unstored nodes; the repack
  ignores hints for objects it does not hold.
- **Clients, the git mirror and MCP** read trees only through the
  changeset endpoints, which derive. `GET /v1/blobs/{hash}` of an unstored
  tree node answers 404: tree nodes were never part of that endpoint's
  contract for clients, and the clients here fetch only file contents.

## Consequences
- A create writes no tree node: on the filesystem store, a create three
  levels down added five tree files before and none now
  (`crates/leep-server/tests/lazy_trees.rs`); the fsyncs on the create
  path are its uploads' and nothing else.
- The first read of an unlanded changeset in a process costs up to
  `MAX_LAZY_DEPTH` in-memory tree builds and a metadata read or two per
  step; later reads hit the cache. A cold walk of a history made only of
  lazy creates (no lands) pays that per changeset.
- Fault sweeps run odd seeds with lazy creates and even seeds without
  (`LEEP_FAULT_LAZY=on|off` forces one mode); the crash e2e runs the
  server in either mode through `LEEP_LAZY_CREATE_TREES`.
- Metrics: `leep_create_lazy_trees_total`,
  `leep_lazy_tree_derivations_total`, `leep_lazy_tree_derive_seconds`,
  `leep_lazy_tree_materialized_total`.
- A fault or crash between a tree's nodes and its root leaves nodes no
  stored root names, as an interrupted create always could; GC collects
  them.
- Tools that read tree nodes straight from the blob store by hash, without
  a changeset, no longer find an unlanded changeset's nodes. Every such
  tool in this repository was changed or already tolerates it (above).
