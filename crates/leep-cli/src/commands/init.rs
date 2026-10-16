//! `leep init`.

use std::collections::HashSet;
use std::fs;
use std::path::PathBuf;

use clap::Args;

use super::{print_json, Ctx};
use crate::cache::Cache;
use crate::client::Client;
use crate::error::Result;
use crate::fmt::{plural, short};
use crate::materialize::{list_files, materialize, prune_empty_dirs};
use crate::state::{normalize_prefix, Config, WorkCopy};

#[derive(Args, Debug)]
pub struct InitArgs {
    /// Target ref that `land` and `sync` use.
    #[arg(long = "ref", default_value = "main")]
    pub target_ref: String,
    /// Sparse-checkout prefix (repeatable); default is the whole repository.
    #[arg(long = "prefix")]
    pub prefixes: Vec<String>,
    /// Directory to initialize (default: current directory).
    pub dir: Option<PathBuf>,
}

/// Create `.leep` in the directory and check out the ref if it exists.
///
/// `token` authenticates the requests; `store_token` also writes it to
/// `.leep/config.json` (mode 0600).
pub fn run(
    args: &InitArgs,
    server: &str,
    token: Option<String>,
    store_token: bool,
    cache: Cache,
    json: bool,
) -> Result<(Ctx, i32)> {
    let client = Client::with_token(server, token.as_deref());
    // Ask first so an unreachable server does not leave a half-made `.leep`.
    let head = client.get_ref(&args.target_ref)?.target;
    let dir = args.dir.clone().unwrap_or_else(|| PathBuf::from("."));
    fs::create_dir_all(&dir)?;
    let root = fs::canonicalize(&dir)?;
    let prefixes = args
        .prefixes
        .iter()
        .map(|p| normalize_prefix(p))
        .collect::<Result<Vec<_>>>()?;
    let wc = WorkCopy::init(
        &root,
        Config {
            server: server.to_string(),
            target_ref: args.target_ref.clone(),
            prefixes,
            token: token.filter(|_| store_token),
        },
    )?;
    let mut ctx = Ctx {
        wc,
        client,
        cache,
        json,
    };
    match head {
        Some(id) => {
            // Files already present before init, which a failed checkout must
            // leave alone even when it rewrote them with the same content.
            let existed: HashSet<String> = list_files(&ctx, &id)?
                .into_keys()
                .filter(|p| fs::symlink_metadata(root.join(p)).is_ok())
                .collect();
            let stats = match materialize(&mut ctx, id, false) {
                Ok(stats) => stats,
                Err(e) => {
                    undo_checkout(&ctx, &root, &existed);
                    return Err(e);
                }
            };
            if json {
                print_json(&serde_json::json!({ "base": id, "written": stats.written }))?;
            } else {
                println!(
                    "Initialized {} at {} ({})",
                    root.display(),
                    short(id),
                    plural(stats.written, "file")
                );
            }
        }
        None if json => print_json(&serde_json::json!({ "base": null }))?,
        None => println!(
            "Initialized {}; ref {} does not exist yet",
            root.display(),
            args.target_ref
        ),
    }
    Ok((ctx, 0))
}

/// Put the directory back as `init` found it after a failed checkout: remove
/// the files it wrote and the `.leep` it made. Otherwise a retry is refused,
/// because the ref may have moved and those files now look untracked.
fn undo_checkout(ctx: &Ctx, root: &std::path::Path, existed: &HashSet<String>) {
    for path in ctx.wc.state.files.keys().filter(|p| !existed.contains(*p)) {
        if let Ok(abs) = ctx.wc.abs_for_write(path) {
            if fs::remove_file(&abs).is_ok() {
                prune_empty_dirs(root, &abs);
            }
        }
    }
    let _ = fs::remove_dir_all(root.join(crate::state::DIR));
}
