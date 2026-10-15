//! Fuzz harness functions for Leep's decoders and parsers.
//!
//! Each target is a `fn(&[u8])` that must never panic and asserts the
//! invariants of whatever it decodes. The same functions run three ways: as
//! libFuzzer targets (`fuzz/`, nightly), as a stable smoke test over the
//! checked-in corpus and seeded mutations (`tests/smoke.rs`, part of
//! `cargo test`), and by hand on a crash artifact. There is no fuzzing
//! framework dependency here. See `docs/FUZZING.md`.

mod api;
mod config;
mod objects;
mod rows;
pub mod seeds;
mod text;

/// One fuzz target: a name (also its corpus directory under `fuzz/corpus`)
/// and the function that checks one input.
pub struct Target {
    pub name: &'static str,
    pub check: fn(&[u8]),
}

macro_rules! targets {
    ($($name:ident => $m:ident :: $f:ident),* $(,)?) => {
        /// Every target. `fuzz/fuzz_targets/<name>.rs` exists for each one.
        pub static TARGETS: &[Target] =
            &[$(Target { name: stringify!($name), check: $m::$f }),*];

        /// The same functions by target name, for the libFuzzer binaries.
        pub mod check {
            $(pub use super::$m::$f as $name;)*
        }
    };
}

targets! {
    changeset => objects::changeset,
    tree_node => objects::tree_node,
    history_node => objects::history_node,
    signature => objects::signature,
    review => objects::review,
    blob_trigrams_row => rows::blob_trigrams_row,
    tree_trigrams_row => rows::tree_trigrams_row,
    trigram_codec => rows::trigram_codec,
    gc_rows => rows::gc_rows,
    skiplist_row => rows::skiplist_row,
    api_json => api::api_json,
    server_router => api::server_router,
    owners => config::owners,
    review_policy => config::review_policy,
    signing_keys => config::signing_keys,
    auth_tokens => config::auth_tokens,
    comment_lexer => text::comment_lexer,
    rename_detection => text::rename_detection,
    ref_name => text::ref_name,
    repo_path => text::repo_path,
    search_plan => text::search_plan,
    arg_parsers => text::arg_parsers,
}

/// Look a target up by name.
pub fn target(name: &str) -> Option<&'static Target> {
    TARGETS.iter().find(|t| t.name == name)
}

/// UTF-8 text of the input, lossy so every byte string reaches the parser.
fn lossy(data: &[u8]) -> std::borrow::Cow<'_, str> {
    String::from_utf8_lossy(data)
}
