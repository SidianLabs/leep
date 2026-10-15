//! Read-only queries: log, show, cat, blame, refs.

use std::io::Write;

use clap::Args;
use leep_core::api::ChangesetInfo;

use super::{print_json, Remote};
use crate::error::{CliError, Result};
use crate::fmt::{short, utc_time};

#[derive(Args, Debug)]
pub struct LogArgs {
    /// Starting revision (default: working copy base, else target ref).
    pub rev: Option<String>,
    /// Only changesets touching this path.
    #[arg(long)]
    pub path: Option<String>,
    /// Maximum number of entries.
    #[arg(short = 'n', long, default_value_t = 20)]
    pub limit: u32,
}

#[derive(Args, Debug)]
pub struct ShowArgs {
    pub rev: String,
}

#[derive(Args, Debug)]
pub struct CatArgs {
    pub rev: String,
    /// Repository-relative path.
    pub path: String,
}

#[derive(Args, Debug)]
pub struct BlameArgs {
    /// `[REV] PATH`
    #[arg(num_args = 1..=2, value_name = "[REV] PATH")]
    pub args: Vec<String>,
}

fn header(info: &ChangesetInfo) {
    println!("changeset {}", info.id);
    println!("Author: {}", info.changeset.author);
    println!("Date:   {}", utc_time(info.changeset.timestamp));
    for (k, v) in &info.changeset.extra {
        println!("{k}: {v}");
    }
    println!();
    for line in info.changeset.message.lines() {
        println!("    {line}");
    }
}

pub fn log(r: &Remote, args: &LogArgs) -> Result<i32> {
    let id = r.resolve_default(args.rev.as_deref())?;
    let resp = r.client.log(&id, args.path.as_deref(), Some(args.limit))?;
    if r.json {
        print_json(&resp)?;
        return Ok(0);
    }
    for info in &resp.entries {
        header(info);
        println!();
    }
    Ok(0)
}

pub fn show(r: &Remote, args: &ShowArgs) -> Result<i32> {
    let id = r.resolve(&args.rev)?;
    let info = r.client.get_changeset(&id)?;
    if r.json {
        print_json(&info)?;
        return Ok(0);
    }
    for p in &info.changeset.parents {
        println!("parent {p}");
    }
    header(&info);
    println!();
    // Ask the server so additions and modifications are told apart.
    let parent = info.changeset.parents.first().copied();
    for e in r.client.diff(parent.as_ref(), &id, None)?.entries {
        let letter = match (&e.old, &e.new) {
            (None, Some(_)) => 'A',
            (Some(_), None) => 'D',
            _ => 'M',
        };
        println!("{letter} {}", e.path);
    }
    Ok(0)
}

pub fn cat(r: &Remote, args: &CatArgs) -> Result<i32> {
    let id = r.resolve(&args.rev)?;
    let (data, _) = r.client.file(&id, &args.path)?;
    let mut out = std::io::stdout().lock();
    out.write_all(&data)?;
    out.flush()?;
    Ok(0)
}

pub fn blame(r: &Remote, args: &BlameArgs) -> Result<i32> {
    let (rev, path) = match args.args.as_slice() {
        [p] => (None, p),
        [rev, p] => (Some(rev.as_str()), p),
        _ => return Err(CliError::user("usage: leep blame [REV] PATH")),
    };
    let id = r.resolve_default(rev)?;
    let resp = r.client.blame(&id, path)?;
    if r.json {
        print_json(&resp)?;
        return Ok(0);
    }
    for (i, l) in resp.lines.iter().enumerate() {
        println!("{} {:>4} | {}", short(l.changeset), i + 1, l.text);
    }
    Ok(0)
}

pub fn refs(r: &Remote) -> Result<i32> {
    let resp = r.client.refs()?;
    if r.json {
        print_json(&resp)?;
        return Ok(0);
    }
    for (name, id) in &resp.refs {
        println!("{name} {id}");
    }
    Ok(0)
}
