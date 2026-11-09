//! `leep commit`: upload new blobs, then create a changeset.

use std::collections::BTreeMap;
use std::io::IsTerminal;
use std::path::PathBuf;

use clap::Args;
use leep_core::changeset::{
    EXTRA_AGENT_MODEL, EXTRA_AGENT_PROMPT, EXTRA_AGENT_SESSION, EXTRA_AGENT_TESTS,
    EXTRA_REBASED_FROM, EXTRA_REVERT_OF,
};
use leep_core::outln;
use leep_core::{Changeset, ChangesetId};

use super::{print_json, Ctx};
use crate::error::{CliError, Result};
use crate::fmt::{plural, short};
use crate::sign::AuthorKey;
use crate::snapshot;
use crate::state::ResolvingLand;
use crate::status::status;

#[derive(Args, Debug, Default, Clone)]
pub struct CommitArgs {
    /// Commit message. Optional only when finishing a merge, cherry-pick or
    /// revert, which supply their own.
    #[arg(short, long)]
    pub message: Option<String>,
    /// Author (default: $LEEP_AUTHOR, else `leep config author`, else git's
    /// user.name and user.email, else the login name, else the token's name
    /// on the server).
    #[arg(long)]
    pub author: Option<String>,
    /// Model that produced the change.
    #[arg(long, env = "LEEP_AGENT_MODEL")]
    pub agent_model: Option<String>,
    /// Agent session identifier.
    #[arg(long, env = "LEEP_AGENT_SESSION")]
    pub agent_session: Option<String>,
    /// Prompt that led to the change.
    #[arg(long, env = "LEEP_AGENT_PROMPT")]
    pub agent_prompt: Option<String>,
    /// Test results or commands the agent ran.
    #[arg(long, env = "LEEP_AGENT_TESTS")]
    pub agent_tests: Option<String>,
    /// Sign the changeset with this private key file (from `leep keygen`).
    #[arg(long, env = "LEEP_SIGNING_KEY", value_name = "KEYFILE")]
    pub sign: Option<PathBuf>,
    /// Append the trailer `KEY: VALUE` to the message (repeatable), after
    /// those `commit.trailers` in `.leep/config.json` names.
    #[arg(long = "trailer", value_name = "KEY=VALUE")]
    pub trailers: Vec<String>,
    /// Do not warn about files that look like a conflict left unresolved.
    #[arg(long)]
    pub allow_markers: bool,
}

fn author(ctx: &Ctx, args: &CommitArgs) -> Result<String> {
    snapshot::author(ctx, args.author.as_deref())
}

/// The `agent.*` keys of the commit. Without `--agent-session` (or
/// `LEEP_AGENT_SESSION`), the session `leep session start` made here, so
/// the server joins the changeset to it (ADR 0095).
fn extra(ctx: &Ctx, args: &CommitArgs) -> BTreeMap<String, String> {
    let session = args
        .agent_session
        .clone()
        .filter(|s| !s.is_empty())
        .or_else(|| ctx.wc.state.session.clone());
    agent_extra(
        &args.agent_model,
        &session,
        &args.agent_prompt,
        &args.agent_tests,
    )
}

/// The `agent.*` keys that were given and are not empty.
pub fn agent_extra(
    model: &Option<String>,
    session: &Option<String>,
    prompt: &Option<String>,
    tests: &Option<String>,
) -> BTreeMap<String, String> {
    let mut m = BTreeMap::new();
    for (k, v) in [
        (EXTRA_AGENT_MODEL, model),
        (EXTRA_AGENT_SESSION, session),
        (EXTRA_AGENT_PROMPT, prompt),
        (EXTRA_AGENT_TESTS, tests),
    ] {
        if let Some(v) = v.as_ref().filter(|v| !v.is_empty()) {
            m.insert(k.to_string(), v.clone());
        }
    }
    m
}

/// The conflicted land this commit resolves: one is recorded, the working
/// copy is still on the head it was rebased onto, and `plain` (no merge or
/// pick under way) holds.
fn resolving(ctx: &Ctx, plain: bool) -> Option<&ResolvingLand> {
    ctx.wc
        .state
        .resolving
        .as_ref()
        .filter(|r| plain && ctx.wc.state.base == Some(r.onto))
}

pub fn run(ctx: &mut Ctx, args: &CommitArgs) -> Result<i32> {
    // Fail before uploading anything if the key cannot be used.
    let key = args
        .sign
        .as_deref()
        .filter(|p| !p.as_os_str().is_empty())
        .map(AuthorKey::load)
        .transpose()?;
    ctx.refuse_interrupted()?;
    let st = status(&ctx.wc)?;
    st.refuse_conflicts()?;
    let merge = ctx.wc.state.merge_in_progress.clone();
    let pick = ctx.wc.state.pick_in_progress.clone();
    // A merge may resolve every conflict in favour of the base and still has
    // a second parent to record.
    if st.is_clean() && merge.is_none() {
        return Err(match &pick {
            // The pick is still waiting: the user has a decision to make.
            Some(p) => CliError::user(format!(
                "nothing to commit: resolving the conflicts left the working copy unchanged; \
                 `leep {} --abort` ends it",
                if p.revert { "revert" } else { "cherry-pick" }
            )),
            None => CliError::nothing_to_do("nothing to commit, working copy clean"),
        });
    }
    let message = args
        .message
        .clone()
        .or_else(|| merge.as_ref().map(|m| m.message.clone()))
        .or_else(|| pick.as_ref().map(|p| p.message.clone()))
        .or_else(|| {
            resolving(ctx, merge.is_none() && pick.is_none()).and_then(|r| r.message.clone())
        })
        .ok_or_else(|| CliError::user("a commit message is required (-m)"))?;
    let message = with_trailers(&message, &trailers(ctx, args)?);
    if !args.allow_markers {
        for (path, line) in leftover_markers(ctx, &st) {
            eprintln!(
                "warning: {path}:{line}: a `<<<<<<<`, `=======`, `>>>>>>>` conflict looks \
                 unresolved; committing it anyway (`--allow-markers` silences this)"
            );
        }
    }

    let changes = snapshot::upload_changes(ctx, &st)?;

    let mut parents: Vec<_> = ctx.wc.state.base.into_iter().collect();
    parents.extend(merge.as_ref().map(|m| m.other));
    let mut extra = extra(ctx, args);
    if let Some(p) = &pick {
        // Same provenance a clean run records (the server's rebase for a
        // cherry-pick, the inverse changeset for a revert).
        let key = if p.revert {
            EXTRA_REVERT_OF
        } else {
            EXTRA_REBASED_FROM
        };
        extra.insert(key.to_string(), p.from.to_string());
    } else if let Some(r) = resolving(ctx, merge.is_none()) {
        // The resolution of a land's conflicts: name what it replaces, so
        // reviews of identical changes can carry over (the server decides).
        extra.insert(EXTRA_REBASED_FROM.to_string(), r.rebased_from());
    }
    let cs = Changeset {
        parents,
        author: author(ctx, args)?,
        timestamp: snapshot::now_secs(),
        message,
        changes,
        extra,
    };
    let id = ctx.client.create_changeset(&cs)?;
    // Before the working copy moves: if signing fails, nothing local changed
    // and a retry just makes a new changeset.
    let signed_by = match &key {
        Some(k) => Some(ctx.client.add_signature(&id, &k.sign(&id)).map_err(|e| {
            CliError::user(format!(
                "created {} but could not sign it: {e}; nothing was committed locally, retry",
                short(id)
            ))
        })?),
        None => None,
    };

    for e in st.entries {
        match e.now {
            Some(now) => {
                ctx.wc.state.files.insert(e.path, now);
            }
            None => {
                ctx.wc.state.files.remove(&e.path);
            }
        }
    }
    ctx.wc.state.conflicts.clear();
    ctx.wc.state.unresolved.clear();
    ctx.wc.state.merge_in_progress = None;
    ctx.wc.state.pick_in_progress = None;
    ctx.wc.state.base = Some(id);
    ctx.wc.save_state()?;

    if ctx.json {
        print_json(&leep_core::api::CreateChangesetResponse { id })?;
    } else {
        let summary = format!(
            "committed {} ({}{})",
            short(id),
            plural(cs.changes.len(), "file"),
            match cs.parents.get(1) {
                Some(other) => format!(", merge of {}", short(other)),
                None => String::new(),
            }
        );
        let (out, err) = commit_lines(id, summary, std::io::stdout().is_terminal());
        for line in out {
            outln!("{line}");
        }
        for line in err {
            eprintln!("{line}");
        }
        if let Some(s) = signed_by {
            eprintln!("signed as {} (key {})", s.name, s.key_id);
        }
    }
    Ok(0)
}

/// Largest file the conflict marker check reads.
const MAX_MARKER_SCAN: u64 = 8 << 20;

/// Files to be committed that hold a whole conflict as the CLI writes one,
/// with the line of its `<<<<<<< `: likely a resolution that kept its
/// markers. Symlinks, binary files and very large ones are not read; a
/// file that cannot be read is skipped (the upload reports it).
fn leftover_markers(ctx: &Ctx, st: &crate::status::Status) -> Vec<(String, usize)> {
    use crate::status::Change;
    let mut out = Vec::new();
    for e in &st.entries {
        let Some(now) = &e.now else { continue };
        if !matches!(e.change, Change::Added | Change::Modified)
            || now.kind == leep_core::FileKind::Symlink
            || now.size > MAX_MARKER_SCAN
        {
            continue;
        }
        let Ok(data) = std::fs::read(ctx.wc.abs(&e.path)) else {
            continue;
        };
        if data.contains(&0) {
            continue;
        }
        if let Some(line) = crate::markers::unresolved(&String::from_utf8_lossy(&data)) {
            out.push((e.path.clone(), line));
        }
    }
    out
}

/// The trailers to append: `commit.trailers` from `.leep/config.json`, then
/// `--trailer`s, as `Key: value` lines.
fn trailers(ctx: &Ctx, args: &CommitArgs) -> Result<Vec<String>> {
    let from_config = ctx.wc.config.commit.trailers.iter().map(|t| {
        parse_trailer(t)
            .map_err(|why| CliError::user(format!("`commit.trailers` in .leep/config.json: {why}")))
    });
    let from_args = args
        .trailers
        .iter()
        .map(|t| parse_trailer(t).map_err(|why| CliError::user(format!("--trailer: {why}"))));
    from_config.chain(from_args).collect()
}

/// `Key=value` or `Key: value` as the trailer line `Key: value`. A key is
/// letters, digits and `-`; the value is one non-empty line.
pub fn parse_trailer(t: &str) -> std::result::Result<String, String> {
    let split = t
        .find([':', '='])
        .ok_or_else(|| format!("{t:?} is no KEY=VALUE"))?;
    let (key, value) = (t[..split].trim(), t[split + 1..].trim());
    if key.is_empty() || !key.chars().all(|c| c.is_ascii_alphanumeric() || c == '-') {
        return Err(format!(
            "{t:?}: the key must be letters, digits and `-`, like Co-Authored-By"
        ));
    }
    if value.is_empty() || value.contains(['\n', '\r']) {
        return Err(format!("{t:?}: the value must be one non-empty line"));
    }
    Ok(format!("{key}: {value}"))
}

/// Whether `line` reads as a trailer, `Key: value`.
fn is_trailer_line(line: &str) -> bool {
    line.split_once(": ").is_some_and(|(k, v)| {
        !k.is_empty()
            && !v.trim().is_empty()
            && k.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
    })
}

/// `message` with `trailers` appended, git style: in the message's own
/// trailer block when its last paragraph is one, else in a new paragraph.
/// A trailer the message already has is not repeated.
pub fn with_trailers(message: &str, trailers: &[String]) -> String {
    let message = message.trim_end();
    let mut new: Vec<&str> = Vec::new();
    for t in trailers {
        let have = message.lines().any(|l| l.trim_end() == t) || new.contains(&t.as_str());
        if !have {
            new.push(t);
        }
    }
    if new.is_empty() {
        return message.to_string();
    }
    let last = message.rsplit("\n\n").next().unwrap_or("");
    let in_block = message.contains("\n\n") && last.lines().all(is_trailer_line);
    let sep = if message.is_empty() {
        ""
    } else if in_block {
        "\n"
    } else {
        "\n\n"
    };
    format!("{message}{sep}{}", new.join("\n"))
}

/// What `leep commit` prints to stdout and to stderr. On a terminal one
/// line says it all; piped, stdout is the full id alone, for scripts
/// (`id=$(leep commit -m ...)`), and the summary goes to stderr.
fn commit_lines(id: ChangesetId, summary: String, tty: bool) -> (Vec<String>, Vec<String>) {
    if tty {
        (vec![summary], vec![])
    } else {
        (vec![id.to_string()], vec![summary])
    }
}

#[cfg(test)]
mod tests {
    use leep_core::Hash;

    use super::*;

    #[test]
    fn trailers_parse_from_either_form_and_refuse_bad_keys() {
        assert_eq!(
            parse_trailer("Co-Authored-By=Claude <noreply@anthropic.com>"),
            Ok("Co-Authored-By: Claude <noreply@anthropic.com>".into())
        );
        assert_eq!(parse_trailer("Refs: #12"), Ok("Refs: #12".into()));
        // The first separator splits: a value may hold either.
        assert_eq!(parse_trailer("Link=a:b=c"), Ok("Link: a:b=c".into()));
        for bad in ["novalue", "=x", "Bad Key=x", "K=", "K=a\nb"] {
            assert!(parse_trailer(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn trailers_join_the_message_s_own_block_and_are_not_repeated() {
        let co = "Co-Authored-By: Claude <noreply@anthropic.com>".to_string();
        let refs = "Refs: #12".to_string();
        assert_eq!(
            with_trailers("cli: add x\n", std::slice::from_ref(&co)),
            format!("cli: add x\n\n{co}")
        );
        assert_eq!(
            with_trailers(
                "cli: add x\n\nwhy\n\nRefs: #12\n",
                &[co.clone(), refs.clone()]
            ),
            format!("cli: add x\n\nwhy\n\nRefs: #12\n{co}")
        );
        // A subject that looks like a trailer is no trailer block.
        assert_eq!(
            with_trailers("cli: add x", std::slice::from_ref(&refs)),
            "cli: add x\n\nRefs: #12"
        );
        assert_eq!(with_trailers("m", &[]), "m");
        assert_eq!(
            with_trailers("m", &[refs.clone(), refs.clone()]),
            "m\n\nRefs: #12"
        );
    }

    #[test]
    fn a_terminal_gets_one_line_and_a_pipe_the_full_id() {
        let id = ChangesetId(Hash::of_blob(b"c"));
        let summary = format!("committed {} (3 files)", short(id));
        assert_eq!(
            commit_lines(id, summary.clone(), true),
            (vec![summary.clone()], vec![])
        );
        assert_eq!(
            commit_lines(id, summary.clone(), false),
            (vec![id.to_string()], vec![summary])
        );
    }
}
