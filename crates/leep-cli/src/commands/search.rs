//! `leep search`: grep a revision on the server, without checking it out.

use clap::Args;
use leep_core::api::SearchResponse;

use super::{print_json, Remote};
use crate::client::{SearchQuery, SEARCH_PAGE};
use crate::error::{exit, Result};

#[derive(Args, Debug)]
pub struct SearchArgs {
    /// Text to find; a literal unless `-E`. Matched within single lines.
    pub query: String,
    /// Revision to search (default: working copy base, else target ref).
    #[arg(long)]
    pub rev: Option<String>,
    /// Ignore case.
    #[arg(short = 'i', long)]
    pub ignore_case: bool,
    /// Treat the query as a regular expression (Rust regex syntax).
    #[arg(short = 'E', long)]
    pub regex: bool,
    /// Only files at or below this path.
    #[arg(long)]
    pub path: Option<String>,
    /// Only paths matching this glob; `*` stays within one directory, `**`
    /// crosses them (for example `**/*.rs`).
    #[arg(long)]
    pub glob: Option<String>,
    /// Maximum matching lines (server default 100); more than one server
    /// page (1000) is fetched page by page.
    #[arg(short = 'n', long)]
    pub limit: Option<u32>,
    /// Continue after the page that returned this cursor (the `next` field
    /// of `--json` output); the query and flags must match that request's.
    #[arg(long, value_name = "CURSOR")]
    pub after: Option<String>,
}

impl SearchArgs {
    pub fn query(&self) -> SearchQuery {
        SearchQuery {
            query: self.query.clone(),
            regex: self.regex,
            ignore_case: self.ignore_case,
            path: self.path.clone(),
            glob: self.glob.clone(),
            limit: self.limit,
            after: self.after.clone(),
        }
    }
}

/// `path:line:text`, one row per matching line.
pub fn search_lines(resp: &SearchResponse) -> Vec<String> {
    resp.matches
        .iter()
        .map(|m| format!("{}:{}:{}", m.path, m.line_number, m.line))
        .collect()
}

/// Exits 1 when nothing matched (text mode), like grep.
pub fn run(r: &Remote, args: &SearchArgs) -> Result<i32> {
    let id = r.resolve_default(args.rev.as_deref())?;
    let resp = match args.limit {
        Some(n) if n > SEARCH_PAGE => r.client.search_all(&id, &args.query(), n as usize)?,
        _ => r.client.search(&id, &args.query())?,
    };
    if r.json {
        print_json(&resp)?;
        return Ok(0);
    }
    for line in search_lines(&resp) {
        println!("{line}");
    }
    if resp.truncated {
        eprintln!(
            "leep: results truncated after {} matches; narrow the query or raise -n",
            resp.matches.len()
        );
    }
    // A predicate like grep: "no match" is exit 1, not an error (ADR 0034).
    Ok(if resp.matches.is_empty() {
        exit::USER
    } else {
        exit::OK
    })
}

#[cfg(test)]
mod tests {
    use leep_core::api::{SearchMatch, SearchRange};
    use leep_core::RepoPath;

    use super::*;

    #[test]
    fn rows_are_path_line_text() {
        let resp = SearchResponse {
            matches: vec![SearchMatch {
                path: RepoPath::new("src/a.rs").unwrap(),
                line_number: 12,
                line: "  let x = 1; // a:b".into(),
                ranges: vec![SearchRange { start: 2, end: 5 }],
            }],
            ..Default::default()
        };
        assert_eq!(search_lines(&resp), ["src/a.rs:12:  let x = 1; // a:b"]);
        assert!(search_lines(&SearchResponse::default()).is_empty());
    }
}
