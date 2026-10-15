//! `leep diff`.

use std::fs;

use clap::Args;
use leep_core::FileKind;

use super::{print_json, Ctx, Remote};
use crate::error::{CliError, Result};
use crate::fmt::file_diff;
use crate::status::{read_content, status, Change};

#[derive(Args, Debug)]
pub struct DiffArgs {
    /// Either nothing (working copy vs base) or `FROM TO` revisions.
    #[arg(num_args = 0..=2, value_name = "FROM TO")]
    pub revs: Vec<String>,
}

pub fn run(ctx: &Ctx, args: &DiffArgs) -> Result<i32> {
    match args.revs.as_slice() {
        [] => working_copy(ctx),
        [from, to] => revisions(&ctx.remote(), from, to),
        _ => Err(CliError::user("diff takes no arguments or exactly FROM TO")),
    }
}

fn working_copy(ctx: &Ctx) -> Result<i32> {
    let st = status(&ctx.wc)?;
    let mut text = String::new();
    for e in &st.entries {
        let old = match ctx.wc.state.files.get(&e.path) {
            Some(f) => Some((ctx.cache.fetch(&ctx.client, &f.content)?, f.kind)),
            None => None,
        };
        let new = match (&e.now, e.change) {
            (Some(now), Change::Added | Change::Modified | Change::Conflicted) => {
                let abs = ctx.wc.abs(&e.path);
                Some((read_content(&abs, &fs::symlink_metadata(&abs)?)?, now.kind))
            }
            _ => None,
        };
        text.push_str(&file_diff(
            &e.path,
            old.as_ref().map(|(d, k)| (d.as_slice(), *k)),
            new.as_ref().map(|(d, k)| (d.as_slice(), *k)),
        ));
    }
    if ctx.json {
        print_json(&serde_json::json!({ "diff": text }))?;
    } else {
        print!("{text}");
    }
    Ok(0)
}

fn revisions(r: &Remote, from: &str, to: &str) -> Result<i32> {
    let from = r.resolve(from)?;
    let to = r.resolve(to)?;
    let resp = r.client.diff(Some(&from), &to, None)?;
    if r.json {
        print_json(&resp)?;
        return Ok(0);
    }
    let fetch = |f: &Option<leep_core::api::FileInfo>| -> Result<Option<(Vec<u8>, FileKind)>> {
        match f {
            Some(f) => Ok(Some((r.cache.fetch(r.client, &f.content)?, f.kind))),
            None => Ok(None),
        }
    };
    for e in &resp.entries {
        let (old, new) = (fetch(&e.old)?, fetch(&e.new)?);
        print!(
            "{}",
            file_diff(
                e.path.as_str(),
                old.as_ref().map(|(d, k)| (d.as_slice(), *k)),
                new.as_ref().map(|(d, k)| (d.as_slice(), *k)),
            )
        );
    }
    Ok(0)
}
