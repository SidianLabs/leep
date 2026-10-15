//! `leep reflog` and `leep undo`: see how refs moved and move one back.
//!
//! Undo is a client-side compare-and-swap against the server's ref log
//! (ADR 0018): it reads the entry, then swaps the ref from its current value
//! to the entry's old one. The swap goes through the ordinary ref endpoint, so
//! every server policy applies, and a concurrent move fails with 409 instead
//! of being overwritten. The working copy is never touched.

use clap::Args;
use leep_core::api::CasRefRequest;
use leep_core::refname::NOT_FORWARD_MARKER;
use leep_core::{ChangesetId, RefLogEntry};

use super::{print_json, Remote};
use crate::error::{CliError, Result};
use crate::fmt::{short, utc_time};

/// Most entries one server request returns.
const PAGE: usize = 1000;

#[derive(Args, Debug)]
pub struct ReflogArgs {
    /// Show only this ref (default: every ref).
    pub refname: Option<String>,
    /// How many entries to show, newest first.
    #[arg(short = 'n', long = "limit", default_value_t = 20)]
    pub limit: usize,
}

#[derive(Args, Debug)]
pub struct UndoArgs {
    /// Ref to move back (default: the working copy's target ref).
    pub refname: Option<String>,
    /// Restore the value the ref had before this log entry (see `leep
    /// reflog`), instead of before its latest move.
    #[arg(long = "to", value_name = "SEQ")]
    pub to: Option<u64>,
}

fn side(id: Option<ChangesetId>) -> String {
    id.map_or_else(|| "-".to_string(), short)
}

/// `seq  time  actor  operation  ref  old -> new  message`; `-` stands for a
/// missing actor or id (a create has no old value, a delete no new one).
fn entry_line(e: &RefLogEntry) -> String {
    let mut line = format!(
        "{}  {}  {}  {}  {}  {} -> {}",
        e.seq,
        utc_time(e.time),
        e.actor.as_deref().unwrap_or("-"),
        e.operation,
        e.name,
        side(e.old),
        side(e.new),
    );
    if let Some(m) = &e.message {
        line.push_str("  ");
        line.push_str(m);
    }
    line
}

pub fn reflog(r: &Remote, args: &ReflogArgs) -> Result<i32> {
    let mut entries = Vec::new();
    let mut before = None;
    while entries.len() < args.limit {
        let want = (args.limit - entries.len()).min(PAGE);
        let page = r
            .client
            .ref_log(args.refname.as_deref(), before, want)?
            .entries;
        let Some(last) = page.last().map(|e| e.seq) else {
            break;
        };
        let full = page.len() == want;
        entries.extend(page);
        before = Some(last);
        if !full {
            break;
        }
    }
    if r.json {
        print_json(&serde_json::json!({ "entries": entries }))?;
    } else {
        for e in &entries {
            println!("{}", entry_line(e));
        }
    }
    Ok(0)
}

pub fn undo(r: &Remote, args: &UndoArgs) -> Result<i32> {
    let name = args.refname.as_deref().unwrap_or(r.target_ref);
    let entry = match args.to {
        // The newest entry at or below SEQ, which must be SEQ itself.
        Some(seq) => r
            .client
            .ref_log(Some(name), Some(seq.saturating_add(1)), 1)?
            .entries
            .into_iter()
            .next()
            .filter(|e| e.seq == seq)
            .ok_or_else(|| CliError::user(format!("{name} has no log entry {seq}")))?,
        None => r
            .client
            .ref_log(Some(name), None, 1)?
            .entries
            .into_iter()
            .next()
            .ok_or_else(|| {
                CliError::user(format!(
                    "{name} has no log entries; it has not moved since the server began \
                     recording (see `leep reflog`)"
                ))
            })?,
    };
    let current = r.client.get_ref(name)?.target;
    if args.to.is_none() && current != entry.new {
        // The log's newest entry should describe where the ref is now.
        return Err(CliError::user(format!(
            "{name} moved while undoing it (now {}, log says {}); run `leep reflog {name}` \
             and retry",
            side(current),
            side(entry.new)
        )));
    }
    if current == entry.old {
        return Err(CliError::user(format!(
            "nothing to undo: {name} is already at {}",
            side(entry.old)
        )));
    }
    let resp = r
        .client
        .cas_ref(
            name,
            &CasRefRequest {
                expected: current,
                new: entry.old,
                operation: Some("undo".into()),
                message: Some(format!("undo of {}", entry.seq)),
            },
        )
        .map_err(|e| match e {
            // A protected ref never moves back, so the change has to be undone
            // by a new change that goes through review and signing.
            CliError::User(m) if m.contains(NOT_FORWARD_MARKER) => {
                let undone = entry.new.map_or_else(|| "<id>".into(), |id| id.to_string());
                CliError::User(format!(
                    "{m}\n{name} is protected, so undo cannot move it back. Revert the change \
                 instead: `leep revert {undone}` (once per changeset the move brought in), \
                 then `leep land`, which goes through review and signing."
                ))
            }
            e => e,
        })?;
    if !resp.ok {
        return Err(CliError::user(format!(
            "{name} changed while undoing it (409 conflict, now {}); run `leep reflog {name}` \
             and retry",
            side(resp.current)
        )));
    }
    // The entry the undo wrote; a failed read only drops the exact redo hint.
    let undo_seq = r
        .client
        .ref_log(Some(name), None, 1)
        .ok()
        .and_then(|l| l.entries.into_iter().next())
        .filter(|e| e.operation == "undo" && e.old == current && e.new == entry.old)
        .map(|e| e.seq);
    if r.json {
        print_json(&serde_json::json!({
            "name": name, "from": current, "to": entry.old,
            "undone": entry.seq, "seq": undo_seq,
        }))?;
        return Ok(0);
    }
    match (current, entry.old) {
        (Some(from), Some(to)) => println!("Moved {name} back: {} -> {}", short(from), short(to)),
        (Some(from), None) => println!("Deleted {name} (was {})", short(from)),
        (None, Some(to)) => println!("Recreated {name} at {}", short(to)),
        (None, None) => unreachable!("current == old returned above"),
    }
    match undo_seq {
        Some(seq) => println!("To redo: leep undo {name} --to {seq}"),
        None => println!("To redo: leep undo {name}"),
    }
    if name == r.target_ref && entry.old.is_some() {
        println!("The working copy is unchanged; run `leep sync` to follow {name}.");
    }
    Ok(0)
}

#[cfg(test)]
mod tests {
    use leep_core::Hash;

    use super::*;

    fn id(n: &[u8]) -> ChangesetId {
        ChangesetId(Hash::of_blob(n))
    }

    #[test]
    fn lines_show_missing_sides_and_messages() {
        let mut e = RefLogEntry {
            seq: 7,
            name: "main".into(),
            old: None,
            new: Some(id(b"a")),
            time: 0,
            actor: None,
            operation: "land".into(),
            message: None,
        };
        let a = short(id(b"a"));
        assert_eq!(
            entry_line(&e),
            format!("7  1970-01-01 00:00:00  -  land  main  - -> {a}")
        );
        e.old = e.new;
        e.new = None;
        e.actor = Some("ci".into());
        e.message = Some("undo of 3".into());
        assert_eq!(
            entry_line(&e),
            format!("7  1970-01-01 00:00:00  ci  land  main  {a} -> -  undo of 3")
        );
    }
}
