//! `leep watch [PATH...]`: coordination events as they happen (ADR 0099):
//! sessions starting, changing paths and ending (landed or abandoned), and
//! notes added, endorsed or superseded, on or around the paths given.
//!
//! It long-polls `GET /v1/events` from the tail (or `--after`), printing
//! one line per event until interrupted. Events carry no agent-written
//! text; the paths in them are agent-chosen names and are printed quoted
//! and escaped (`{:?}`). An expired cursor (the reader fell more than the
//! server's retention behind) is said and reading goes on from the oldest
//! event kept.

use std::time::Duration;

use clap::Args;
use leep_core::api::{EventEntry, EventFacts};
use leep_core::outln;

use super::session::word;
use super::Remote;
use crate::client::EventsRead;
use crate::error::{CliError, ErrorKind, Result};
use crate::fmt::{short, utc_time};

/// How long each long-poll waits on the server.
const WAIT: Duration = Duration::from_secs(30);

#[derive(Args, Debug)]
pub struct WatchArgs {
    /// Only events on or around these files or directories (at most 16).
    pub paths: Vec<String>,
    /// Only events of this kind (session.started, session.paths,
    /// session.ended, note.added, note.endorsed, note.superseded);
    /// repeatable.
    #[arg(long = "kind", value_name = "KIND")]
    pub kinds: Vec<String>,
    /// Start after this cursor (from an earlier `--json` line) instead of
    /// now.
    #[arg(long, value_name = "CURSOR")]
    pub after: Option<String>,
    /// Print the events there are now and stop, instead of waiting.
    #[arg(long)]
    pub once: bool,
}

fn unsupported(e: CliError) -> CliError {
    if e.kind() == ErrorKind::NotFound {
        return CliError::user(
            "this server keeps no events: it is older than ADR 0099, or runs with \
             --sessions off",
        );
    }
    e
}

/// What the server vouches for in an event, as words.
fn facts(f: &EventFacts) -> String {
    let mut out = Vec::new();
    if let Some(s) = &f.state {
        out.push(word(s));
    }
    if let Some(id) = &f.landed_as {
        out.push(format!("as {}", short(id)));
    }
    if let Some(o) = &f.origin {
        out.push(word(o));
    }
    if let Some(k) = &f.note_kind {
        out.push(word(k));
    }
    if let Some(t) = &f.trust {
        out.push(word(t));
    }
    if let Some(n) = f.superseded_by {
        out.push(format!("by note:{n}"));
    }
    out.join(" ")
}

/// One line: cursor, time, kind, subject, principal, facts, then the
/// paths quoted.
pub fn line(e: &EventEntry) -> String {
    let mut l = format!(
        "{}  {}  {}  {}  {}",
        e.cursor,
        utc_time(e.created),
        word(&e.kind.replace('.', "_")).replace('_', "."),
        word(&e.subject.replace(':', "_")).replace('_', ":"),
        word(&e.principal),
    );
    let f = facts(&e.facts);
    if !f.is_empty() {
        l.push_str("  ");
        l.push_str(&f);
    }
    if !e.untrusted.paths.is_empty() {
        l.push_str(&format!("  {:?}", e.untrusted.paths));
    }
    l
}

pub fn watch(r: &Remote, a: &WatchArgs) -> Result<i32> {
    let mut base: Vec<(&str, String)> = Vec::new();
    for p in &a.paths {
        base.push(("paths", p.clone()));
    }
    for k in &a.kinds {
        base.push(("kinds", k.clone()));
    }
    let mut after = match &a.after {
        Some(c) => c.clone(),
        None => match r
            .client
            .events(&base, Duration::ZERO)
            .map_err(unsupported)?
        {
            EventsRead::Page(p) => p.next,
            EventsRead::Expired(e) => e.oldest,
        },
    };
    loop {
        let mut q = base.clone();
        q.push(("after", after.clone()));
        let wait = if a.once { Duration::ZERO } else { WAIT };
        match r.client.events(&q, wait).map_err(unsupported)? {
            EventsRead::Page(p) => {
                for e in &p.events {
                    if r.json {
                        outln!("{}", serde_json::to_string(e)?);
                    } else {
                        outln!("{}", line(e));
                    }
                }
                after = p.next;
                if a.once && !p.more {
                    return Ok(0);
                }
            }
            EventsRead::Expired(e) => {
                eprintln!(
                    "the server no longer keeps events after {after}; some were missed \
                     (see `leep who` for the state now). Going on from {}.",
                    e.oldest
                );
                after = e.oldest;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lines_quote_agent_chosen_paths() {
        let e = EventEntry {
            cursor: "e7".into(),
            kind: "session.ended".into(),
            subject: "session:3".into(),
            principal: "ada".into(),
            session: Some(3),
            created: 0,
            facts: EventFacts {
                state: Some("landed".into()),
                ..Default::default()
            },
            untrusted: leep_core::api::EventText {
                paths: vec!["src/a\u{1b}[2J.rs".into()],
            },
        };
        let l = line(&e);
        assert!(
            l.starts_with("e7  1970-01-01 00:00:00  session.ended  session:3  ada  landed"),
            "{l}"
        );
        assert!(!l.contains('\u{1b}'), "{l}");
        assert!(l.ends_with(r#"["src/a\u{1b}[2J.rs"]"#), "{l}");
    }
}
