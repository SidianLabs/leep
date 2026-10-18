# Fuzzing

Leep decodes bytes it does not control: stored rows, request bodies, files an
operator edits, and the text of whatever agents write. Every such entry point
has a fuzz target, a function `fn(&[u8])` in
[`crates/leep-fuzz-targets`](../crates/leep-fuzz-targets) that must never
panic, hang or answer 5xx, and that checks the invariants of what it decodes
(re-encoding round trips, stable hashes, parsers that agree with their own
output).

The targets run three ways:

| Where | Needs | When |
|---|---|---|
| `cargo test --workspace` (smoke) | stable | every PR, locally and in CI |
| `cargo fuzz` (libFuzzer, coverage guided) | nightly + `cargo-fuzz` | locally, and nightly in CI (`.github/workflows/fuzz.yml`) |
| `replay` example | stable | reproducing one input |

## Targets

| Target | What it covers |
|---|---|
| `changeset`, `tree_node`, `signature`, `review` | postcard decoders of persisted objects: decode never panics, a decoded value re-encodes and decodes to itself, content hashes are stable |
| `history_node` | parts of path-history packs (ADR 0045): decode never panics, nodes resolve against a keyframe or fail, and a decoded keyframe stored again (whole, or as an edit of itself) reads back the same |
| `blob_trigrams_row`, `tree_trigrams_row`, `trigram_codec` | code search rows (`blob-trigrams-v1`, `tree-trigrams-v1`): the decoders, the lazy `Probe`, and encoders against decoders |
| `gc_rows` | `blob-claim`, `-condemned`, `-rescue` and `-doom` rows |
| `skiplist_row` | the skiplist derived row |
| `api_json` | every `/v1` request body, and the response types clients parse, as JSON |
| `server_router` | the real axum router over a seeded in-memory repo, driven with arbitrary methods, paths, queries and bodies: anything below 500 is fine, 5xx is a bug |
| `owners`, `review_policy`, `signing_keys`, `auth_tokens` | `OWNERS` files and the server's three config files |
| `comment_lexer` | `strip_comments`, `comment_only` and `whitespace_only` across every supported language |
| `rename_detection` | `leep_core::rename`: pairs come from deleted to added, no path twice, input order is irrelevant |
| `ref_name`, `repo_path` | the ref name validator and `RepoPath::new` |
| `search_plan` | the query planner: a text that has a match always satisfies the trigram requirement, so the index never hides a result |
| `arg_parsers` | durations, `--extra` filters, hashes, public keys and signatures |

Ref log rows are columns in SQL, not encoded blobs, so there is nothing to
decode in Rust. The cached blame rows in `leep-derive` are private to that
crate and are not yet covered.

## Smoke tests (stable)

```sh
cargo test -p leep-fuzz-targets
```

`tests/smoke.rs` runs every target over each file in `fuzz/corpus/<target>/`,
then over seeded mutations of those files (bit flips, inserts, splices from
other corpus files, truncation) and some random bytes. Knobs:

- `LEEP_FUZZ_ITERS=N` mutations per target (default 2000; the router gets a
  tenth). `LEEP_FUZZ_ITERS=200000 cargo test --release -p leep-fuzz-targets
  --test smoke smoke_all` is a good longer run on a stable toolchain.
- `LEEP_FUZZ_SEED=N` changes the RNG seed. The default is fixed, so a failure
  is reproducible; a failing run prints the input as hex.

The suite takes a few seconds in a debug build.

## Running cargo-fuzz

The fuzz crate is `fuzz/`. It is outside the workspace (it needs libFuzzer),
so a normal build never sees it.

```sh
rustup toolchain install nightly --profile minimal
cargo install cargo-fuzz --locked
cd fuzz
cargo +nightly fuzz list
cargo +nightly fuzz run changeset -- -max_total_time=60
```

`cargo fuzz run` adds new inputs to `fuzz/corpus/<target>/` next to the
checked-in seeds. To keep your working tree clean, give libFuzzer a scratch
corpus first and the checked-in one second:

```sh
cargo +nightly fuzz run server_router /tmp/corpus/server_router corpus/server_router -- -max_total_time=300
```

CI does this (`.github/workflows/fuzz.yml`): every night and on demand, it
runs each target for a bounded time (`seconds` input, default 60; `targets`
input to pick some) and uploads `fuzz/artifacts` if anything crashes. It
never runs on pull requests.

## Reproducing and fixing a crash

1. Take the artifact file (`fuzz/artifacts/<target>/crash-...`, or the hex the
   smoke test printed, written to a file).
2. Reproduce on stable with a normal backtrace:
   `cargo run -p leep-fuzz-targets --example replay -- <target> <file>`.
3. Fix the bug in the crate where it lives, with the smallest change, and
   add a unit test there that states the property in words.
4. Add the input to the corpus as `fuzz/corpus/<target>/regress-<what>`
   (minimize it first with `cargo +nightly fuzz tmin <target> <file>`). The
   smoke test replays it on every `cargo test` from now on. Names starting
   with `seed-` are reserved for generated seeds.

## Adding a target

1. Write `pub fn name(data: &[u8])` in the matching module of
   `crates/leep-fuzz-targets/src`. Decode, return on error, assert
   invariants on success. Do not catch panics and do not read the clock, the
   environment or the file system: a target must be deterministic.
2. Add it to the `targets!` list in `src/lib.rs`.
3. Add `fuzz/fuzz_targets/<name>.rs` (copy a neighbour: two lines) and a
   `[[bin]]` entry in `fuzz/Cargo.toml`. The smoke test checks that both
   exist.
4. Add seeds in `src/seeds.rs`, built with the real encoders so they start
   valid, then run
   `LEEP_FUZZ_BLESS=1 cargo test -p leep-fuzz-targets seeds_are_up_to_date`
   to write them to `fuzz/corpus/<name>/`. Keep seeds small: the whole
   corpus stays under 2 MiB, and the smoke test enforces it.

A decoder in a private module needs a `#[doc(hidden)]` accessor so the
harness can reach it (see `leep_derive::gc::decode_row`).
