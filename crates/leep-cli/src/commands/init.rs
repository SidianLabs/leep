//! `leep init`.

use std::fs;
use std::path::PathBuf;

use clap::Args;

use super::{print_json, Ctx};
use crate::cache::Cache;
use crate::client::Client;
use crate::error::Result;
use crate::fmt::{plural, short};
use crate::materialize::materialize;
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
pub fn run(args: &InitArgs, server: &str, cache: Cache, json: bool) -> Result<(Ctx, i32)> {
    let client = Client::new(server);
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
            let stats = materialize(&mut ctx, id, false)?;
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
