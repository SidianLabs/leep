//! `leep land`.

use std::collections::BTreeMap;
use std::fs;

use clap::Args;
use leep_core::api::{
    error_code, Conflict, LandId, LandRequest, LandResponse, ReviewStatusResponse, FEATURE_LAND_ID,
};
use leep_core::outln;
use leep_core::refname::MISSING_REF_MARKER;
use leep_core::review::NEEDS_REVIEW_MARKER;
use leep_core::signature::NOT_SIGNED_MARKER;
use leep_core::{ChangesetId, FileChange, FileKind, RepoPath};

use super::{print_json, Ctx};
use crate::client::new_land_id;
use crate::client::LogQuery;
use crate::error::{exit, CliError, ErrorKind, Result};
use crate::fmt::short;
use crate::markers::{explain, relabel, Labels};
use crate::materialize::{materialize, write_file};
use crate::state::{PendingLand, ResolvingLand};
use crate::status::status;

#[derive(Args, Debug)]
pub struct LandArgs {
    /// Ref to land on (default: the working copy's target ref).
    #[arg(long = "ref")]
    pub target_ref: Option<String>,
    /// Create the ref if it does not exist. Without it, landing onto a
    /// missing ref fails (exit 4), so a typo cannot make a branch; only the
    /// first land onto `main` of an empty repository creates it unasked.
    #[arg(long)]
    pub create: bool,
}

pub fn run(ctx: &mut Ctx, args: &LandArgs) -> Result<i32> {
    ctx.refuse_in_progress()?;
    let base = ctx
        .wc
        .state
        .base
        .ok_or_else(|| CliError::user("nothing to land: no commits yet"))?;
    let st = status(&ctx.wc)?;
    if !st.is_clean() {
        st.refuse_conflicts()?;
        return Err(CliError::user(
            "working copy has uncommitted changes; commit before landing",
        ));
    }
    let target_ref = args
        .target_ref
        .clone()
        .unwrap_or_else(|| ctx.wc.config.target_ref.clone());
    let (resp, created) = land_creating(ctx, base, &target_ref, args.create)
        .map_err(|e| review_guidance(ctx, e, &target_ref))?;
    if created {
        eprintln!("created ref {target_ref}");
    }
    if ctx.json {
        print_json(&resp)?;
    }
    match resp {
        LandResponse::Landed {
            landed,
            rebased,
            attempts,
        } => {
            if rebased {
                materialize(ctx, landed, false)?;
            }
            if ctx.wc.state.resolving.take().is_some() {
                ctx.wc.save_state()?;
            }
            if !ctx.json {
                // The head it was replayed onto, best effort.
                let onto = (rebased && landed != base)
                    .then(|| ctx.cache.changeset(&ctx.client, &landed).ok())
                    .flatten()
                    .and_then(|info| info.changeset.parents.first().copied());
                for line in landed_lines(landed, base, rebased, &target_ref, attempts, onto) {
                    outln!("{line}");
                }
            }
            Ok(0)
        }
        LandResponse::Conflicts {
            onto,
            conflicts,
            clean,
        } => {
            let from = landed_stack(ctx, base, onto);
            let message = replaced_message(ctx, &from);
            let unmerged = rebase_locally(ctx, &target_ref, onto, &conflicts, &clean)?;
            ctx.wc.state.resolving = Some(ResolvingLand {
                from,
                target_ref: target_ref.clone(),
                onto,
                message,
            });
            ctx.wc.save_state()?;
            if !ctx.json {
                outln!(
                    "Cannot land cleanly: {} conflict{} against {} on {target_ref}",
                    conflicts.len(),
                    if conflicts.len() == 1 { "" } else { "s" },
                    short(onto)
                );
                for c in &conflicts {
                    let note = if unmerged.contains(&c.path.to_string()) {
                        format!(
                            "no text merge: kept your version; `leep cat {} {}` shows the other side",
                            short(onto),
                            c.path
                        )
                    } else if c
                        .merged_with_markers
                        .as_deref()
                        .is_some_and(crate::markers::only_insertions)
                    {
                        // ADR 0089's union: say so, or the author reads
                        // both sides to find out keeping both is right.
                        "markers written; both sides added lines here; keeping both \
                         (one side's lines first) carries the review"
                            .to_string()
                    } else {
                        "markers written".to_string()
                    };
                    match mode_conflict(c) {
                        Some(m) => outln!("  C {} ({note}; {m})", c.path),
                        None => outln!("  C {} ({note})", c.path),
                    }
                }
                if conflicts.len() > unmerged.len() {
                    outln!("{}", explain(&Labels::landed(&target_ref)));
                }
                let commit = if ctx
                    .wc
                    .state
                    .resolving
                    .as_ref()
                    .is_some_and(|r| r.message.is_some())
                {
                    "`leep commit` (its message by default)"
                } else {
                    "`leep commit -m MESSAGE`"
                };
                let replaced = resolving_names(ctx);
                outln!(
                    "Your working copy is now on {} with your changes applied.\n\
                     Resolve the conflicts (`leep resolve PATH` once chosen), then run {commit} and `leep land`.\n\
                     The commit records that it replaces {replaced} (`leep.rebased_from`).\n\
                     Reviews of {replaced} carry only where your resolution is its change rebased onto {} \
                     (ADR 0089); other paths need review again.",
                    short(onto),
                    short(onto),
                );
            }
            Ok(exit::CONFLICT)
        }
    }
}

/// What a successful land says. A server-side rebase gives the copy a new
/// id, and a second line says what the rebase did: replayed the change
/// cleanly onto `onto`, the head it found, so the change is the reviewed
/// one and the original's reviews count (the land policy judged it).
fn landed_lines(
    landed: ChangesetId,
    base: ChangesetId,
    rebased: bool,
    target_ref: &str,
    attempts: u32,
    onto: Option<ChangesetId>,
) -> Vec<String> {
    let mut out = vec![format!(
        "Landed {} on {target_ref}{} ({attempts} attempt{})",
        short(landed),
        if rebased && landed == base {
            " (rebased)"
        } else {
            ""
        },
        if attempts == 1 { "" } else { "s" },
    )];
    if rebased && landed != base {
        out.push(format!(
            "rebased from {} onto {}: your changes are unchanged (a clean rebase by the \
             server), so reviews of {} count",
            short(base),
            onto.map_or_else(|| format!("the head of {target_ref}"), short),
            short(base)
        ));
    }
    out
}

/// The changeset a `needs review` refusal names (`changeset <id> needs
/// review: ...`), for the hint's commands.
fn refused_changeset(message: &str) -> Option<ChangesetId> {
    let rest = message.split("changeset ").nth(1)?;
    let (id, after) = rest.split_once(' ')?;
    if !after.starts_with(NEEDS_REVIEW_MARKER) {
        return None;
    }
    id.parse().ok()
}

/// Most changesets a land's stack is followed back through for
/// `leep.rebased_from`; the server bounds its rebase chains the same way.
const MAX_STACK: usize = 64;

/// The changesets landing `base` tried to add, newest first: its
/// first-parent chain down to the generation of its merge base with `onto`.
/// Best effort, from one merge-base call and one summary log: on any error,
/// just `base`.
fn landed_stack(ctx: &Ctx, base: ChangesetId, onto: ChangesetId) -> Vec<ChangesetId> {
    let q = LogQuery {
        limit: Some(MAX_STACK as u32 + 1),
        ..LogQuery::default()
    };
    let (Ok(fork), Ok(log)) = (
        ctx.client.merge_base(&base, &onto),
        ctx.client.log_summary(&base, &q),
    ) else {
        return vec![base];
    };
    let by_id: BTreeMap<ChangesetId, _> = log.entries.iter().map(|e| (e.id, e)).collect();
    let floor = match fork.base {
        Some(f) => match by_id.get(&f) {
            Some(e) => e.generation,
            None => match ctx.cache.changeset(&ctx.client, &f) {
                Ok(info) => info.generation,
                Err(_) => return vec![base],
            },
        },
        None => 0,
    };
    let mut out = Vec::new();
    let mut cur = by_id.get(&base);
    while let Some(e) = cur {
        if e.generation <= floor || out.len() == MAX_STACK {
            break;
        }
        out.push(e.id);
        cur = e.parents.first().and_then(|p| by_id.get(p));
    }
    if out.is_empty() {
        out.push(base);
    }
    out
}

/// The message the resolution commit defaults to: the replaced changeset's,
/// when the land replaces just one. A stack of several has no one message
/// to reuse. Best effort: `None` when it cannot be read.
fn replaced_message(ctx: &Ctx, from: &[ChangesetId]) -> Option<String> {
    match from {
        [one] => ctx
            .cache
            .changeset(&ctx.client, one)
            .ok()
            .map(|info| info.changeset.message),
        _ => None,
    }
}

/// The changesets a conflicted land replaces, for the report.
fn resolving_names(ctx: &Ctx) -> String {
    let from = ctx
        .wc
        .state
        .resolving
        .as_ref()
        .map_or(&[][..], |r| &r.from[..]);
    match from {
        [one] => short(*one),
        many => format!(
            "{} changesets ({})",
            many.len(),
            many.iter()
                .map(|id| short(*id))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

/// Land, creating the ref only if asked to or if this is the first land onto
/// `main` of an empty repository (ADR 0036). Returns whether the ref was
/// created.
fn land_creating(
    ctx: &mut Ctx,
    base: ChangesetId,
    target_ref: &str,
    create: bool,
) -> Result<(LandResponse, bool)> {
    let land_id = pending_land_id(ctx, base, target_ref)?;
    let land = |create: bool| {
        let r = ctx.client.land_resending(&LandRequest {
            changeset: base,
            target_ref: target_ref.to_string(),
            create: Some(create),
            land_id: Some(land_id.clone()),
        });
        r.map_err(|e| hint_policy(e, target_ref))
    };
    let result = land_creating_with(ctx, target_ref, create, land);
    // An answer, or an error that sending again cannot change, ends this
    // land; otherwise the next `leep land` resends it under the same id.
    if !result.as_ref().is_err_and(|e| e.is_retryable()) {
        ctx.wc.state.pending_land = None;
        ctx.wc.save_state()?;
    }
    result
}

/// What to do about a land an earlier run sent without a final answer.
#[derive(Debug, PartialEq, Eq)]
enum Pending {
    /// None for this changeset and ref: a new land, with a new id.
    New,
    /// Send it again under its id: the server answers with the earlier
    /// result if it applied.
    Resend(LandId),
    /// The server does not record land ids, and sending again could land it
    /// twice (once its copy is past the server's landed-copy scan).
    Ask,
}

fn pending(
    p: Option<&PendingLand>,
    base: ChangesetId,
    target_ref: &str,
    server_records_ids: impl FnOnce() -> Result<bool>,
) -> Result<Pending> {
    Ok(match p {
        Some(p) if p.changeset == base && p.target_ref == target_ref => {
            if server_records_ids()? {
                Pending::Resend(p.land_id.clone())
            } else {
                Pending::Ask
            }
        }
        _ => Pending::New,
    })
}

/// The id of the land of `base` onto `target_ref`: the one a land without a
/// final answer used (ADR 0057), when the server records ids, or a new one,
/// saved before it is sent.
fn pending_land_id(ctx: &mut Ctx, base: ChangesetId, target_ref: &str) -> Result<LandId> {
    let client = &ctx.client;
    match pending(ctx.wc.state.pending_land.as_ref(), base, target_ref, || {
        Ok(client
            .server_features()?
            .iter()
            .any(|f| f == FEATURE_LAND_ID))
    })? {
        Pending::Resend(id) => return Ok(id),
        Pending::Ask => {
            ctx.wc.state.pending_land = None;
            ctx.wc.save_state()?;
            return Err(CliError::user(format!(
                "an earlier `leep land` of {} onto {target_ref} got no answer, and it may have \
                 landed; this server does not record land ids, so sending it again could land it twice",
                short(base)
            ))
            .with_hint(format!(
                "`leep reflog {target_ref}` shows `landed {base}` if it did; if not, run `leep land` again"
            )));
        }
        Pending::New => {}
    }
    let land_id = new_land_id();
    ctx.wc.state.pending_land = Some(PendingLand {
        changeset: base,
        target_ref: target_ref.to_string(),
        land_id: land_id.clone(),
    });
    ctx.wc.save_state()?;
    Ok(land_id)
}

fn land_creating_with(
    ctx: &Ctx,
    target_ref: &str,
    create: bool,
    land: impl Fn(bool) -> Result<LandResponse>,
) -> Result<(LandResponse, bool)> {
    // Whether `--create` will create or just land: asked only when it may.
    let existed = if create {
        ctx.client.get_ref(target_ref)?.target.is_some()
    } else {
        true
    };
    match land(create) {
        Ok(resp) => {
            let created = !existed && landed(&resp);
            Ok((resp, created))
        }
        Err(e) if missing_ref(&e) => {
            if target_ref == "main" {
                let refs = ctx.client.refs()?.refs;
                if refs.is_empty() {
                    let resp = land(true)?;
                    let created = landed(&resp);
                    return Ok((resp, created));
                }
                // Another client's first land got there in between.
                if refs.contains_key("main") {
                    return Ok((land(false)?, false));
                }
            }
            Err(e.with_hint(format!(
                "`leep land --ref {target_ref} --create` creates it; `leep branch` lists branches"
            )))
        }
        Err(e) => Err(e),
    }
}

fn landed(resp: &LandResponse) -> bool {
    matches!(resp, LandResponse::Landed { .. })
}

/// A land refused because the ref does not exist and may not be created.
fn missing_ref(e: &CliError) -> bool {
    e.kind() == ErrorKind::NotFound && e.is_server(error_code::REF_MISSING, MISSING_REF_MARKER)
}

/// The server names the changeset a policy refused; add what to do about it.
fn hint_policy(e: CliError, target_ref: &str) -> CliError {
    if e.kind() != ErrorKind::User {
        return e;
    }
    if needs_review(&e) {
        let (approve, request) = match refused_changeset(e.message()) {
            Some(id) => (short(id), short(id)),
            None => ("<changeset>".to_string(), "@".to_string()),
        };
        let next = format!(
            "ask one to run `leep review approve {approve}` (`leep review request {request} --ref {target_ref}` asks them for you); \
             `leep review status {approve} --ref {target_ref} --wait` shows what is missing and waits for it."
        );
        return e
            .map_message(|m| {
                format!(
                    "{m}\n{target_ref} needs approval from the owners of the paths a changeset changes: {next}"
                )
            })
            .with_hint(next);
    }
    if e.is_server(error_code::NOT_SIGNED, NOT_SIGNED_MARKER) {
        return e.map_message(|m| format!(
            "{m}\n{target_ref} only accepts signed changesets: every changeset being landed needs a signature. \
             Commit with `leep commit --sign KEYFILE` (or set LEEP_SIGNING_KEY); create a key with `leep keygen`."
        ));
    }
    e
}

fn needs_review(e: &CliError) -> bool {
    e.kind() == ErrorKind::User && e.is_server(error_code::NEEDS_REVIEW, NEEDS_REVIEW_MARKER)
}

/// A land that needs review is a normal step, not a failure: say which
/// paths wait for whom and which `OWNERS` files named them, without the
/// HTTP status and request id (`--json` keeps both). Asks the server for the
/// refused changeset's review status; without it, the server's own words.
fn review_guidance(ctx: &Ctx, e: CliError, target_ref: &str) -> CliError {
    if !needs_review(&e) {
        return e;
    }
    let id = refused_changeset(e.message());
    let st = id.and_then(|id| ctx.client.review_status(&id, target_ref).ok());
    let text = needs_review_text(e.message(), id, st.as_ref());
    e.with_guidance(text)
}

/// The guidance text of a `needs review` refusal: from the review status
/// when there is one, every path still waiting with what it needs; else the
/// server's first line without its `server:` prefix and HTTP status.
fn needs_review_text(
    message: &str,
    id: Option<ChangesetId>,
    st: Option<&ReviewStatusResponse>,
) -> String {
    let waiting: Vec<(&'static str, String, &str)> = st
        .map(|st| {
            st.paths
                .iter()
                .filter_map(|p| {
                    let (state, why) = super::review::path_state(p);
                    (state != "ok").then(|| (state, why, p.path.as_str()))
                })
                .collect()
        })
        .unwrap_or_default();
    match (id, st) {
        (Some(id), Some(st)) if !waiting.is_empty() => {
            let width = waiting
                .iter()
                .map(|w| w.2.chars().count())
                .max()
                .unwrap_or(0);
            let mut out = format!(
                "{} needs review before it can land on {}:",
                short(id),
                st.target_ref
            );
            for (state, why, path) in waiting {
                out.push_str(&format!("\n  {state:<8} {path:<width$}  {why}"));
            }
            out
        }
        _ => plain_server_text(message.lines().next().unwrap_or(message)).to_string(),
    }
}

/// `server: TEXT (422 Unprocessable Entity)` as just `TEXT`.
fn plain_server_text(m: &str) -> &str {
    let m = m.strip_prefix("server: ").unwrap_or(m);
    let Some(i) = m.rfind(" (") else {
        return m;
    };
    let status = &m.as_bytes()[i + 2..];
    let is_status = m.ends_with(')')
        && status.len() > 4
        && status[..3].iter().all(u8::is_ascii_digit)
        && status[3] == b' ';
    if is_status {
        &m[..i]
    } else {
        m
    }
}

/// Move the working copy to `onto` and re-apply the local changes there:
/// `clean` as-is, conflicts as marker files (or the local version when no
/// text merge exists). The user then resolves and commits once on `onto`,
/// which lands as a fast-forward. Returns the conflicted paths without
/// markers.
fn rebase_locally(
    ctx: &mut Ctx,
    target_ref: &str,
    onto: ChangesetId,
    conflicts: &[Conflict],
    clean: &BTreeMap<RepoPath, FileChange>,
) -> Result<Vec<String>> {
    let outside: Vec<String> = clean
        .keys()
        .chain(conflicts.iter().map(|c| &c.path))
        .map(|p| p.to_string())
        .filter(|p| !ctx.wc.in_scope(p))
        .collect();
    if !outside.is_empty() {
        return Err(CliError::user(format!(
            "cannot resolve here: the conflicting stack touches paths outside this \
             sparse checkout ({}); widen it with `leep checkout @ --add-prefix PATH` and land again",
            outside.join(", ")
        )));
    }

    materialize(ctx, onto, false)?;
    for (path, change) in clean {
        apply(ctx, path.as_str(), change)?;
    }
    let mut unmerged = Vec::new();
    for c in conflicts {
        let path = c.path.to_string();
        match (&c.merged_with_markers, &c.theirs) {
            (Some(text), _) => {
                let text = relabel(text, &Labels::landed(target_ref));
                write_file(
                    &ctx.wc.abs_for_write(&path)?,
                    text.as_bytes(),
                    conflict_kind(c),
                )?;
                if !ctx.wc.state.conflicts.contains(&path) {
                    ctx.wc.state.conflicts.push(path);
                }
            }
            (None, theirs) => {
                let change = match theirs {
                    Some(i) => FileChange::Modify {
                        content: i.content,
                        kind: i.kind,
                        size: i.size,
                    },
                    None => FileChange::Delete,
                };
                apply(ctx, &path, &change)?;
                if !ctx.wc.state.unresolved.contains(&path) {
                    ctx.wc.state.unresolved.push(path.clone());
                }
                unmerged.push(path);
            }
        }
    }
    // `materialize` recorded these files' stats moments before they were
    // overwritten. Where timestamps are coarse the second write can keep the
    // same mtime and size, and status would trust the hash of `onto`'s
    // version, so the next commit would silently drop the user's change.
    // An impossible mtime makes status rehash them.
    for path in clean
        .keys()
        .map(|p| p.as_str())
        .chain(conflicts.iter().map(|c| c.path.as_str()))
    {
        if let Some(f) = ctx.wc.state.files.get_mut(path) {
            f.mtime_ns = 0;
        }
    }
    ctx.wc.save_state()?;
    Ok(unmerged)
}

/// The kind a marker file is written with: yours (`theirs`, the stack
/// being landed) where it exists, so a mode is never silently dropped.
fn conflict_kind(c: &Conflict) -> FileKind {
    c.theirs
        .as_ref()
        .or(c.ours.as_ref())
        .map_or(FileKind::Regular, |i| i.kind)
}

/// Both sides have the file with different kinds: a mode conflict the
/// markers cannot show, reported so it is not settled unseen.
fn mode_conflict(c: &Conflict) -> Option<String> {
    let mode = |k: FileKind| match k {
        FileKind::Regular => "644",
        FileKind::Executable => "755",
        FileKind::Symlink => "symlink",
    };
    match (&c.ours, &c.theirs) {
        (Some(o), Some(t)) if o.kind != t.kind => Some(format!(
            "mode differs: landed {}, yours {}; kept yours",
            mode(o.kind),
            mode(t.kind)
        )),
        _ => None,
    }
}

/// Write one change to disk without recording it in state, so it shows up as
/// a local modification.
fn apply(ctx: &Ctx, path: &str, change: &FileChange) -> Result<()> {
    let abs = ctx.wc.abs_for_write(path)?;
    match change {
        FileChange::Modify { content, kind, .. } => {
            let data = ctx.cache.fetch(&ctx.client, content)?;
            write_file(&abs, &data, *kind)
        }
        FileChange::Delete => match fs::remove_file(&abs) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use leep_core::api::FileInfo;
    use leep_core::Hash;

    #[test]
    fn a_marker_file_takes_its_kind_from_the_conflict() {
        let side = |kind| {
            Some(FileInfo {
                path: RepoPath::new("run.sh").unwrap(),
                content: Hash::of_blob(b"x"),
                kind,
                size: 1,
            })
        };
        let c = |ours, theirs| Conflict {
            path: RepoPath::new("run.sh").unwrap(),
            base: None,
            ours,
            theirs,
            merged_with_markers: Some(String::new()),
        };
        let exec = c(side(FileKind::Executable), side(FileKind::Executable));
        assert_eq!(conflict_kind(&exec), FileKind::Executable);
        assert_eq!(mode_conflict(&exec), None);
        let added = c(None, side(FileKind::Executable));
        assert_eq!(conflict_kind(&added), FileKind::Executable);
        assert_eq!(
            conflict_kind(&c(side(FileKind::Executable), None)),
            FileKind::Executable
        );
        let differ = c(side(FileKind::Executable), side(FileKind::Regular));
        assert_eq!(conflict_kind(&differ), FileKind::Regular, "yours wins");
        assert_eq!(
            mode_conflict(&differ).as_deref(),
            Some("mode differs: landed 755, yours 644; kept yours")
        );
    }

    #[test]
    fn a_pending_land_is_resent_only_to_a_server_that_records_ids() {
        let (a, b) = (
            ChangesetId(Hash::of_blob(b"a")),
            ChangesetId(Hash::of_blob(b"b")),
        );
        let id = new_land_id();
        let p = PendingLand {
            changeset: a,
            target_ref: "main".into(),
            land_id: id.clone(),
        };
        let ask = |p, cs, r: &str, has: Result<bool>| pending(p, cs, r, || has).ok();
        assert_eq!(ask(None, a, "main", Ok(true)), Some(Pending::New));
        assert_eq!(
            ask(Some(&p), a, "main", Ok(true)),
            Some(Pending::Resend(id))
        );
        assert_eq!(ask(Some(&p), a, "main", Ok(false)), Some(Pending::Ask));
        // A server that cannot be asked is not taken for an old one: the
        // land stays pending.
        let down = Err(CliError::new(ErrorKind::Unreachable, "down"));
        assert_eq!(ask(Some(&p), a, "main", down), None);
        // Another changeset or ref is another land; the server is not asked.
        let never = || -> Result<bool> { unreachable!() };
        assert_eq!(pending(Some(&p), b, "main", never).ok(), Some(Pending::New));
        assert_eq!(pending(Some(&p), a, "dev", never).ok(), Some(Pending::New));
    }

    #[test]
    fn a_review_refusal_names_the_changeset_in_its_commands() {
        let id = ChangesetId(Hash::of_blob(b"refused"));
        let e = CliError::user(format!(
            "changeset {id} needs review: path a needs approval from any reviewer"
        ))
        .with_server_code(Some(error_code::NEEDS_REVIEW.into()));
        let msg = hint_policy(e, "main").to_string();
        let s = short(id);
        assert!(msg.contains(&format!("`leep review approve {s}`")), "{msg}");
        assert!(
            msg.contains(&format!(
                "`leep review request {s} --ref main` asks them for you"
            )),
            "{msg}"
        );
        assert!(
            msg.contains(&format!("`leep review status {s} --ref main --wait`")),
            "{msg}"
        );
        assert_eq!(refused_changeset("changeset abc needs review: x"), None);
        assert_eq!(refused_changeset(&format!("changeset {id} is fine")), None);
    }

    #[test]
    fn a_rebased_land_names_the_original_whose_reviews_count() {
        let (a, b) = (
            ChangesetId(Hash::of_blob(b"a")),
            ChangesetId(Hash::of_blob(b"b")),
        );
        let h = ChangesetId(Hash::of_blob(b"h"));
        assert_eq!(
            landed_lines(a, b, true, "main", 1, Some(h)),
            [
                format!("Landed {} on main (1 attempt)", short(a)),
                format!(
                    "rebased from {} onto {}: your changes are unchanged (a clean rebase by \
                     the server), so reviews of {} count",
                    short(b),
                    short(h),
                    short(b)
                )
            ]
        );
        assert!(landed_lines(a, b, true, "main", 1, None)[1].contains("onto the head of main:"));
        assert_eq!(
            landed_lines(a, a, false, "main", 2, None),
            [format!("Landed {} on main (2 attempts)", short(a))]
        );
    }

    #[test]
    fn a_review_refusal_reads_as_guidance_without_http_noise() {
        let id = ChangesetId(Hash::of_blob(b"refused"));
        let raw = format!(
            "server: changeset {id} needs review: path a.rs needs approval from one of [rhea] \
             (422 Unprocessable Entity)"
        );
        // No status from the server: its words, without prefix and status.
        assert_eq!(
            needs_review_text(&raw, Some(id), None),
            format!("changeset {id} needs review: path a.rs needs approval from one of [rhea]")
        );
        assert_eq!(plain_server_text("server: x (y)"), "x (y)");
        assert_eq!(plain_server_text("x (4ü2 z)"), "x (4ü2 z)");
        assert_eq!(plain_server_text("x (500 Internal)"), "x");
        let p = |path: &str, rule: &str, files: &[&str]| leep_core::api::PathReview {
            path: RepoPath::new(path).unwrap(),
            rule: rule.into(),
            owners: vec!["rhea".into()],
            approved_by: vec![],
            rejected_by: vec![],
            carried_from: None,
            carried_rule: None,
            carry: None,
            owners_files: files.iter().map(|f| f.to_string()).collect(),
            carry_source: None,
        };
        let st = ReviewStatusResponse {
            changeset: id,
            target_ref: "main".into(),
            approved: false,
            paths: vec![
                p("CHANGELOG.md", "auto:docs_only", &[]),
                p("src/lib.rs", "pending", &["src/OWNERS", "OWNERS"]),
            ],
            on_refs: vec![],
            refused: None,
            origins: vec![],
        };
        assert_eq!(
            needs_review_text(&raw, Some(id), Some(&st)),
            format!(
                "{} needs review before it can land on main:\n  \
                 PENDING  src/lib.rs  needs one of: rhea (from src/OWNERS, OWNERS)",
                short(id)
            )
        );
    }

    #[test]
    fn a_review_refusal_says_how_to_ask_and_wait() {
        let e =
            CliError::user("changeset abc needs review: path a needs approval from any reviewer")
                .with_server_code(Some(error_code::NEEDS_REVIEW.into()));
        let msg = hint_policy(e, "main").to_string();
        assert!(msg.contains("`leep review approve <changeset>`"), "{msg}");
        assert!(
            msg.contains("`leep review request @ --ref main` asks them for you"),
            "{msg}"
        );
        assert!(
            msg.contains("`leep review status <changeset> --ref main --wait`"),
            "{msg}"
        );
    }
}
