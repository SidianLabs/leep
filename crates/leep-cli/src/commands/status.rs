//! `leep status`.

use serde::Serialize;

use super::{print_json, Ctx};
use crate::error::Result;
use crate::fmt::short;
use crate::status::{status, Change};

#[derive(Serialize)]
struct Report {
    base: Option<String>,
    target_ref: String,
    changes: Vec<Entry>,
}

#[derive(Serialize)]
struct Entry {
    path: String,
    status: Change,
}

pub fn run(ctx: &Ctx) -> Result<i32> {
    let st = status(&ctx.wc)?;
    if ctx.json {
        print_json(&Report {
            base: ctx.wc.state.base.map(|b| b.to_string()),
            target_ref: ctx.wc.config.target_ref.clone(),
            changes: st
                .entries
                .iter()
                .map(|e| Entry {
                    path: e.path.clone(),
                    status: e.change,
                })
                .collect(),
        })?;
        return Ok(0);
    }
    match ctx.wc.state.base {
        Some(b) => println!("base {} (target {})", short(b), ctx.wc.config.target_ref),
        None => println!("no base yet (target {})", ctx.wc.config.target_ref),
    }
    if st.is_clean() {
        println!("nothing to commit, working copy clean");
    }
    for e in &st.entries {
        println!("{} {}", e.change.letter(), e.path);
    }
    if st.has_conflicts() {
        println!("unresolved conflicts: fix the marked files, then `leep commit`");
    }
    Ok(0)
}
