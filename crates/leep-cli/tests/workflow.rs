//! Everyday working-copy workflow against the in-memory mock server: sync
//! with local changes, stash, restore, amend and reset (ADR 0017).

mod common;

use std::fs;

use common::Mock;
use leep_cli::cache::Cache;
use leep_cli::commands::amend::{self, AmendArgs};
use leep_cli::commands::checkout::{sync, SyncArgs};
use leep_cli::commands::commit::{self, CommitArgs};
use leep_cli::commands::init::{self, InitArgs};
use leep_cli::commands::land::{self, LandArgs};
use leep_cli::commands::reset::{self, ResetArgs};
use leep_cli::commands::resolve::{self, ResolveArgs};
use leep_cli::commands::restore::{self, RestoreArgs};
use leep_cli::commands::stash::{self, ApplyArg, IndexArg, PushArgs, StashArgs, StashCmd};
use leep_cli::commands::Ctx;
use leep_cli::error::CliError;
use leep_cli::state::WorkCopy;
use leep_cli::status::{status, Change};
use leep_core::changeset::{EXTRA_AGENT_MODEL, EXTRA_REBASED_FROM, EXTRA_SNAPSHOT};
use leep_core::{ChangesetId, FileKind};

struct Env {
    mock: Mock,
    cache: tempfile::TempDir,
    dirs: Vec<tempfile::TempDir>,
}

impl Env {
    fn new() -> Env {
        Env {
            mock: Mock::start(),
            cache: tempfile::tempdir().unwrap(),
            dirs: Vec::new(),
        }
    }

    fn wc(&mut self) -> Ctx {
        let dir = tempfile::tempdir().unwrap();
        let args = InitArgs {
            target_ref: "main".into(),
            prefixes: vec![],
            dir: Some(dir.path().to_path_buf()),
        };
        let ctx = init::run(
            &args,
            &self.mock.url,
            None,
            false,
            Cache::new(self.cache.path()),
            false,
        )
        .map(|(c, _)| c)
        .unwrap();
        self.dirs.push(dir);
        ctx
    }

    /// A second look at the same server for the same files, like a new process.
    fn reopen(&self, ctx: &Ctx) -> Ctx {
        Ctx {
            wc: WorkCopy::open(&ctx.wc.root).unwrap(),
            client: leep_cli::client::Client::new(&self.mock.url),
            cache: Cache::new(self.cache.path()),
            json: false,
        }
    }

    fn main(&self) -> ChangesetId {
        self.mock.data.lock().unwrap().refs["main"]
    }
}

fn write(ctx: &Ctx, rel: &str, body: &str) {
    let p = ctx.wc.abs(rel);
    fs::create_dir_all(p.parent().unwrap()).unwrap();
    fs::write(p, body).unwrap();
}

fn read(ctx: &Ctx, rel: &str) -> String {
    fs::read_to_string(ctx.wc.abs(rel)).unwrap()
}

fn exists(ctx: &Ctx, rel: &str) -> bool {
    ctx.wc.abs(rel).exists()
}

fn commit_args(msg: &str) -> CommitArgs {
    CommitArgs {
        message: Some(msg.into()),
        author: Some("tester".into()),
        ..Default::default()
    }
}

fn changes(ctx: &Ctx) -> Vec<(String, Change)> {
    status(&ctx.wc)
        .unwrap()
        .entries
        .into_iter()
        .map(|e| (e.path, e.change))
        .collect()
}

#[track_caller]
fn user_err(e: CliError) -> String {
    assert_eq!(e.exit_code(), 1, "{e}");
    e.to_string()
}

/// The message of an error with the given exit code (ADR 0034).
#[track_caller]
fn exits(code: i32, e: CliError) -> String {
    assert_eq!(e.exit_code(), code, "{e}");
    e.to_string()
}

/// The message of an error that reports conflicts to resolve (exit 5).
#[track_caller]
fn conflict_err(e: CliError) -> String {
    assert_eq!(e.exit_code(), 5, "{e}");
    e.to_string()
}

fn stash_cmd(ctx: &mut Ctx, cmd: StashCmd) -> Result<i32, CliError> {
    stash::run(
        ctx,
        &StashArgs {
            cmd: Some(cmd),
            push: PushArgs::default(),
        },
    )
}

fn stash_push(ctx: &mut Ctx, message: Option<&str>) -> Result<i32, CliError> {
    stash::run(
        ctx,
        &StashArgs {
            cmd: None,
            push: PushArgs {
                message: message.map(Into::into),
            },
        },
    )
}

fn restore_args(paths: &[&str], source: Option<&str>, force: bool) -> RestoreArgs {
    RestoreArgs {
        paths: paths.iter().map(|s| s.to_string()).collect(),
        source: source.map(Into::into),
        force,
    }
}

fn reset_args(rev: Option<&str>, hard: bool, force: bool) -> ResetArgs {
    ResetArgs {
        rev: rev.map(Into::into),
        hard,
        force,
    }
}

fn seed_two(env: &Env) -> ChangesetId {
    env.mock.seed(
        None,
        &[
            ("a.txt", "a\n", FileKind::Regular),
            ("b.txt", "b\n", FileKind::Regular),
        ],
        &[],
    )
}

fn snapshots(env: &Env, kind: &str) -> usize {
    let d = env.mock.data.lock().unwrap();
    d.changesets
        .values()
        .filter(|c| c.extra.get(EXTRA_SNAPSHOT).map(String::as_str) == Some(kind))
        // The server's rebased copies carry the marker too.
        .filter(|c| !c.extra.contains_key(EXTRA_REBASED_FROM))
        .count()
}

// ---- sync ----

#[test]
fn sync_carries_uncommitted_changes_onto_the_new_head() {
    let mut env = Env::new();
    let cs1 = seed_two(&env);
    let mut ctx = env.wc();
    write(&ctx, "b.txt", "b edited\n");
    write(&ctx, "new.txt", "brand new\n");
    fs::remove_file(ctx.wc.abs("a.txt")).unwrap();

    // Someone else lands a change to c.txt and a different file.
    let cs2 = env
        .mock
        .seed(Some(cs1), &[("c.txt", "c\n", FileKind::Regular)], &[]);
    assert_eq!(sync(&mut ctx, &SyncArgs::default()).unwrap(), 0);

    assert_eq!(ctx.wc.state.base, Some(cs2));
    assert_eq!(read(&ctx, "c.txt"), "c\n", "head's change arrives");
    assert_eq!(read(&ctx, "b.txt"), "b edited\n");
    assert_eq!(read(&ctx, "new.txt"), "brand new\n");
    assert!(!exists(&ctx, "a.txt"), "local delete kept");
    assert_eq!(
        changes(&ctx),
        [
            ("a.txt".to_string(), Change::Deleted),
            ("b.txt".to_string(), Change::Modified),
            ("new.txt".to_string(), Change::Added),
        ]
    );
    assert_eq!(snapshots(&env, "sync"), 1);
    // The state is what a later process sees, and the changes still commit.
    let mut again = env.reopen(&ctx);
    assert_eq!(again.wc.state.base, Some(cs2));
    commit::run(&mut again, &commit_args("after sync")).unwrap();
    let d = env.mock.data.lock().unwrap();
    let id = again.wc.state.base.unwrap();
    assert_eq!(d.changesets[&id].parents, [cs2]);
    assert_eq!(d.files[&id]["b.txt"].size, "b edited\n".len() as u64);
    assert!(!d.files[&id].contains_key("a.txt"));
}

#[test]
fn sync_with_conflicts_writes_markers_and_blocks_commit() {
    let mut env = Env::new();
    let cs1 = seed_two(&env);
    let mut ctx = env.wc();
    write(&ctx, "a.txt", "a mine\n");
    write(&ctx, "b.txt", "b mine\n");
    env.mock.seed(
        Some(cs1),
        &[("a.txt", "a theirs\n", FileKind::Regular)],
        &[],
    );
    assert_eq!(sync(&mut ctx, &SyncArgs::default()).unwrap(), 5);

    let text = read(&ctx, "a.txt");
    assert!(text.contains("<<<<<<<") && text.contains("a mine") && text.contains("a theirs"));
    assert_eq!(read(&ctx, "b.txt"), "b mine\n");
    assert!(changes(&ctx).contains(&("a.txt".to_string(), Change::Conflicted)));
    let e = commit::run(&mut ctx, &commit_args("no")).unwrap_err();
    assert!(conflict_err(e).contains("conflict"));
    let e = land::run(
        &mut ctx,
        &LandArgs {
            target_ref: None,
            create: false,
        },
    )
    .unwrap_err();
    assert!(conflict_err(e).contains("unresolved conflicts"));
    // Syncing again, or stashing, is refused until the markers are resolved.
    assert!(sync(&mut ctx, &SyncArgs::default()).is_err());
    assert!(stash_push(&mut ctx, None).is_err());

    // Editing the markers away resolves it.
    write(&ctx, "a.txt", "a resolved\n");
    assert_eq!(
        changes(&ctx),
        [
            ("a.txt".to_string(), Change::Modified),
            ("b.txt".to_string(), Change::Modified)
        ]
    );
    commit::run(&mut ctx, &commit_args("resolved")).unwrap();
}

#[test]
fn restore_clears_a_conflict_mark() {
    let mut env = Env::new();
    let cs1 = seed_two(&env);
    let mut ctx = env.wc();
    write(&ctx, "a.txt", "a mine\n");
    env.mock.seed(
        Some(cs1),
        &[("a.txt", "a theirs\n", FileKind::Regular)],
        &[],
    );
    assert_eq!(sync(&mut ctx, &SyncArgs::default()).unwrap(), 5);
    assert!(!ctx.wc.state.conflicts.is_empty());
    restore::run(&mut ctx, &restore_args(&["a.txt"], None, false)).unwrap();
    assert_eq!(read(&ctx, "a.txt"), "a theirs\n");
    assert!(ctx.wc.state.conflicts.is_empty());
    assert!(status(&ctx.wc).unwrap().is_clean());
}

#[test]
fn sync_moves_local_commits_and_keeps_edits() {
    let mut env = Env::new();
    let cs1 = seed_two(&env);
    let mut ctx = env.wc();
    write(&ctx, "b.txt", "b committed\n");
    commit::run(&mut ctx, &commit_args("local one")).unwrap();
    write(&ctx, "d.txt", "d committed\n");
    commit::run(&mut ctx, &commit_args("local two")).unwrap();
    let old_base = ctx.wc.state.base.unwrap();
    write(&ctx, "a.txt", "a uncommitted\n");

    let head = env
        .mock
        .seed(Some(cs1), &[("c.txt", "c\n", FileKind::Regular)], &[]);
    assert_eq!(sync(&mut ctx, &SyncArgs::default()).unwrap(), 0);

    let new_base = ctx.wc.state.base.unwrap();
    assert_ne!(new_base, old_base);
    {
        let d = env.mock.data.lock().unwrap();
        // Two commits plus the snapshot were rebased.
        assert_eq!(d.rebases.len(), 3);
        let mid = d.changesets[&new_base].parents[0];
        assert_eq!(d.changesets[&mid].parents, [head]);
        assert_eq!(d.changesets[&new_base].message, "local two");
        assert_eq!(d.refs["main"], head, "refs never move");
    }
    for (f, body) in [
        ("a.txt", "a uncommitted\n"),
        ("b.txt", "b committed\n"),
        ("c.txt", "c\n"),
        ("d.txt", "d committed\n"),
    ] {
        assert_eq!(read(&ctx, f), body);
    }
    assert_eq!(changes(&ctx), [("a.txt".to_string(), Change::Modified)]);
    // The moved stack lands as a fast-forward.
    commit::run(&mut ctx, &commit_args("three")).unwrap();
    assert_eq!(
        land::run(
            &mut ctx,
            &LandArgs {
                target_ref: None,
                create: false
            }
        )
        .unwrap(),
        0
    );
    assert_eq!(env.main(), ctx.wc.state.base.unwrap());
}

#[test]
fn clean_sync_moves_unlanded_commits_too() {
    let mut env = Env::new();
    let cs1 = seed_two(&env);
    let mut ctx = env.wc();
    write(&ctx, "b.txt", "b committed\n");
    commit::run(&mut ctx, &commit_args("local")).unwrap();
    let head = env
        .mock
        .seed(Some(cs1), &[("c.txt", "c\n", FileKind::Regular)], &[]);
    assert_eq!(sync(&mut ctx, &SyncArgs::default()).unwrap(), 0);
    let base = ctx.wc.state.base.unwrap();
    assert_eq!(
        env.mock.data.lock().unwrap().changesets[&base].parents,
        [head]
    );
    assert_eq!(read(&ctx, "b.txt"), "b committed\n");
    assert_eq!(read(&ctx, "c.txt"), "c\n");
    assert_eq!(snapshots(&env, "sync"), 0, "nothing to snapshot");
    assert!(status(&ctx.wc).unwrap().is_clean());
}

#[test]
fn sync_stops_when_a_local_commit_conflicts() {
    let mut env = Env::new();
    let cs1 = seed_two(&env);
    let mut ctx = env.wc();
    write(&ctx, "a.txt", "a local\n");
    commit::run(&mut ctx, &commit_args("local")).unwrap();
    let base = ctx.wc.state.base;
    write(&ctx, "b.txt", "b edit\n");
    env.mock
        .seed(Some(cs1), &[("a.txt", "a head\n", FileKind::Regular)], &[]);
    let e = sync(&mut ctx, &SyncArgs::default()).unwrap_err();
    let m = conflict_err(e);
    assert!(
        m.contains("conflicts") && m.contains("nothing changed"),
        "{m}"
    );
    assert_eq!(ctx.wc.state.base, base);
    assert_eq!(read(&ctx, "b.txt"), "b edit\n");
    assert_eq!(read(&ctx, "a.txt"), "a local\n");
}

#[test]
fn a_failed_sync_loses_nothing() {
    let mut env = Env::new();
    let cs1 = seed_two(&env);
    let mut ctx = env.wc();
    write(&ctx, "a.txt", "a edited\n");
    write(&ctx, "new.txt", "new\n");
    fs::remove_file(ctx.wc.abs("b.txt")).unwrap();
    let before_state = ctx.wc.state.clone();
    let before = changes(&ctx);
    env.mock
        .seed(Some(cs1), &[("c.txt", "c\n", FileKind::Regular)], &[]);

    // The server fails the rebase.
    env.mock.data.lock().unwrap().fail_rebase = true;
    let e = sync(&mut ctx, &SyncArgs::default()).unwrap_err();
    assert_eq!(e.exit_code(), 3);
    let m = e.to_string();
    assert!(
        m.contains("snapshot") && m.contains("nothing changed locally"),
        "{m}"
    );
    assert_eq!(snapshots(&env, "sync"), 1, "the snapshot id is reported");
    assert_eq!(read(&ctx, "a.txt"), "a edited\n");
    assert_eq!(read(&ctx, "new.txt"), "new\n");
    assert!(!exists(&ctx, "b.txt") && !exists(&ctx, "c.txt"));
    assert_eq!(ctx.wc.state, before_state);
    assert_eq!(WorkCopy::open(&ctx.wc.root).unwrap().state, before_state);
    assert_eq!(changes(&ctx), before);
}

#[test]
fn sync_refuses_to_clobber_an_ignored_local_file_and_changes_nothing() {
    let mut env = Env::new();
    let cs1 = seed_two(&env);
    let mut ctx = env.wc();
    write(&ctx, ".leepignore", "secret.txt\n");
    write(&ctx, "secret.txt", "my secret\n");
    write(&ctx, "a.txt", "a edited\n");
    // Head adds a file at the ignored path, and changes b.txt.
    env.mock.seed(
        Some(cs1),
        &[
            ("secret.txt", "tracked now\n", FileKind::Regular),
            ("b.txt", "b head\n", FileKind::Regular),
        ],
        &[],
    );
    let before_state = ctx.wc.state.clone();
    let e = sync(&mut ctx, &SyncArgs::default()).unwrap_err();
    assert!(user_err(e).contains("secret.txt"));
    assert_eq!(read(&ctx, "secret.txt"), "my secret\n");
    assert_eq!(read(&ctx, "a.txt"), "a edited\n");
    assert_eq!(read(&ctx, "b.txt"), "b\n", "no partial update");
    assert_eq!(ctx.wc.state, before_state);
}

#[test]
fn sync_no_rebase_keeps_the_old_rule_and_head_sync_is_a_no_op() {
    let mut env = Env::new();
    seed_two(&env);
    let mut ctx = env.wc();
    write(&ctx, "a.txt", "edited\n");
    // At head, a dirty sync has nothing to move and creates no snapshot.
    assert_eq!(sync(&mut ctx, &SyncArgs::default()).unwrap(), 0);
    assert_eq!(snapshots(&env, "sync"), 0);
    assert_eq!(read(&ctx, "a.txt"), "edited\n");
    let strict = SyncArgs { no_rebase: true };
    let e = sync(&mut ctx, &strict).unwrap_err();
    assert!(user_err(e).contains("uncommitted"));
    assert_eq!(read(&ctx, "a.txt"), "edited\n");
}

// ---- stash ----

#[test]
fn stash_push_list_show_pop_round_trip() {
    let mut env = Env::new();
    let cs1 = seed_two(&env);
    let mut ctx = env.wc();
    let e = stash_push(&mut ctx, None).unwrap_err();
    assert!(exits(6, e).contains("no local changes"));

    write(&ctx, "a.txt", "a edited\n");
    write(&ctx, "new.txt", "brand new\n");
    fs::remove_file(ctx.wc.abs("b.txt")).unwrap();
    assert_eq!(stash_push(&mut ctx, Some("my work")).unwrap(), 0);

    // Back to the base, new file included in the stash and removed.
    assert!(status(&ctx.wc).unwrap().is_clean());
    assert_eq!(read(&ctx, "a.txt"), "a\n");
    assert_eq!(read(&ctx, "b.txt"), "b\n");
    assert!(!exists(&ctx, "new.txt"));
    assert_eq!(ctx.wc.state.stashes.len(), 1);
    let s = ctx.wc.state.stashes[0].clone();
    assert_eq!(s.message, "my work");
    assert_eq!(s.base, Some(cs1));
    {
        let d = env.mock.data.lock().unwrap();
        let cs = &d.changesets[&s.id];
        assert_eq!(cs.parents, [cs1]);
        assert_eq!(cs.extra[EXTRA_SNAPSHOT], "stash");
        assert_eq!(cs.changes.len(), 3);
        assert_eq!(d.refs.len(), 1, "no ref for a stash");
    }
    // Survives a restart, and `show` reads the diff from the server.
    let mut again = env.reopen(&ctx);
    assert_eq!(again.wc.state.stashes, ctx.wc.state.stashes);
    stash_cmd(&mut again, StashCmd::List).unwrap();
    stash_cmd(&mut again, StashCmd::Show(IndexArg { index: None })).unwrap();
    assert!(stash_cmd(&mut again, StashCmd::Show(IndexArg { index: Some(3) })).is_err());

    // The world moves on before the stash comes back.
    env.mock
        .seed(Some(cs1), &[("c.txt", "c\n", FileKind::Regular)], &[]);
    sync(&mut again, &SyncArgs::default()).unwrap();
    assert_eq!(
        stash_cmd(&mut again, StashCmd::Pop(IndexArg { index: None })).unwrap(),
        0
    );
    assert!(again.wc.state.stashes.is_empty());
    assert_eq!(read(&again, "a.txt"), "a edited\n");
    assert_eq!(read(&again, "new.txt"), "brand new\n");
    assert_eq!(read(&again, "c.txt"), "c\n");
    assert!(!exists(&again, "b.txt"));
    assert_eq!(
        changes(&again),
        [
            ("a.txt".to_string(), Change::Modified),
            ("b.txt".to_string(), Change::Deleted),
            ("new.txt".to_string(), Change::Added),
        ]
    );
}

#[test]
fn stash_apply_keeps_the_entry_and_needs_a_clean_tree() {
    let mut env = Env::new();
    seed_two(&env);
    let mut ctx = env.wc();
    write(&ctx, "a.txt", "one\n");
    stash_push(&mut ctx, None).unwrap();
    write(&ctx, "b.txt", "other work\n");
    let e = stash_cmd(&mut ctx, StashCmd::Apply(ApplyArg { target: None })).unwrap_err();
    assert!(user_err(e).contains("uncommitted"));
    assert_eq!(read(&ctx, "b.txt"), "other work\n");
    stash_push(&mut ctx, Some("second")).unwrap();
    assert_eq!(ctx.wc.state.stashes[0].message, "second", "newest first");

    stash_cmd(
        &mut ctx,
        StashCmd::Apply(ApplyArg {
            target: Some("1".into()),
        }),
    )
    .unwrap();
    assert_eq!(read(&ctx, "a.txt"), "one\n");
    assert_eq!(ctx.wc.state.stashes.len(), 2, "apply keeps it");
    stash_cmd(&mut ctx, StashCmd::Drop(IndexArg { index: Some(1) })).unwrap();
    assert_eq!(ctx.wc.state.stashes.len(), 1);
    assert!(stash_cmd(&mut ctx, StashCmd::Drop(IndexArg { index: Some(1) })).is_err());
    // An id works for apply (what a failed sync prints).
    restore::run(&mut ctx, &restore_args(&["."], None, false)).unwrap();
    let id = ctx.wc.state.stashes[0].id.to_string();
    stash_cmd(&mut ctx, StashCmd::Apply(ApplyArg { target: Some(id) })).unwrap();
    assert_eq!(read(&ctx, "b.txt"), "other work\n");
}

#[test]
fn stash_pop_with_conflicts_keeps_the_stash() {
    let mut env = Env::new();
    let cs1 = seed_two(&env);
    let mut ctx = env.wc();
    write(&ctx, "a.txt", "mine\n");
    stash_push(&mut ctx, None).unwrap();
    env.mock
        .seed(Some(cs1), &[("a.txt", "theirs\n", FileKind::Regular)], &[]);
    sync(&mut ctx, &SyncArgs::default()).unwrap();
    assert_eq!(
        stash_cmd(&mut ctx, StashCmd::Pop(IndexArg { index: None })).unwrap(),
        5
    );
    assert_eq!(ctx.wc.state.stashes.len(), 1, "kept for another try");
    assert!(read(&ctx, "a.txt").contains("<<<<<<<"));
    assert!(changes(&ctx).contains(&("a.txt".to_string(), Change::Conflicted)));
    assert!(commit::run(&mut ctx, &commit_args("x")).is_err());
}

#[test]
fn stash_needs_a_base() {
    let mut env = Env::new();
    let mut ctx = env.wc();
    write(&ctx, "a.txt", "x\n");
    let e = stash_push(&mut ctx, None).unwrap_err();
    assert!(user_err(e).contains("no base"));
    assert_eq!(read(&ctx, "a.txt"), "x\n");
}

// ---- restore ----

#[test]
fn restore_discards_changes_and_leaves_new_files() {
    let mut env = Env::new();
    env.mock.seed(
        None,
        &[
            ("a.txt", "a\n", FileKind::Regular),
            ("dir/x.txt", "x\n", FileKind::Regular),
            ("dir/y.txt", "y\n", FileKind::Regular),
            ("keep.txt", "keep\n", FileKind::Regular),
        ],
        &[],
    );
    let mut ctx = env.wc();
    write(&ctx, "a.txt", "a edited\n");
    write(&ctx, "dir/x.txt", "x edited\n");
    fs::remove_file(ctx.wc.abs("dir/y.txt")).unwrap();
    write(&ctx, "dir/new.txt", "new\n");
    write(&ctx, "keep.txt", "keep edited\n");

    restore::run(&mut ctx, &restore_args(&["a.txt"], None, false)).unwrap();
    assert_eq!(read(&ctx, "a.txt"), "a\n");
    assert_eq!(
        read(&ctx, "keep.txt"),
        "keep edited\n",
        "only what was named"
    );

    // A directory restores everything under it except the new file.
    restore::run(&mut ctx, &restore_args(&["dir"], None, false)).unwrap();
    assert_eq!(read(&ctx, "dir/x.txt"), "x\n");
    assert_eq!(read(&ctx, "dir/y.txt"), "y\n", "deleted file is recreated");
    assert_eq!(read(&ctx, "dir/new.txt"), "new\n");
    assert_eq!(
        changes(&ctx),
        [
            ("dir/new.txt".to_string(), Change::Added),
            ("keep.txt".to_string(), Change::Modified)
        ]
    );
    restore::run(&mut ctx, &restore_args(&["dir"], None, true)).unwrap();
    assert!(!exists(&ctx, "dir/new.txt"), "--force removes new files");
    assert_eq!(read(&ctx, "keep.txt"), "keep edited\n");

    let e = restore::run(&mut ctx, &restore_args(&["nope.txt"], None, false)).unwrap_err();
    assert!(exits(4, e).contains("does not match"));
    // A clean path is fine and does nothing.
    restore::run(&mut ctx, &restore_args(&["a.txt"], None, false)).unwrap();
    assert_eq!(changes(&ctx), [("keep.txt".to_string(), Change::Modified)]);
}

#[test]
fn restore_source_brings_content_from_another_revision() {
    let mut env = Env::new();
    let cs1 = env.mock.seed(
        None,
        &[
            ("a.txt", "a v1\n", FileKind::Regular),
            ("old.txt", "old\n", FileKind::Regular),
        ],
        &[],
    );
    env.mock.seed(
        Some(cs1),
        &[
            ("a.txt", "a v2\n", FileKind::Regular),
            ("n.txt", "n\n", FileKind::Regular),
        ],
        &["old.txt"],
    );
    let mut ctx = env.wc();
    assert_eq!(read(&ctx, "a.txt"), "a v2\n");
    restore::run(
        &mut ctx,
        &restore_args(
            &["a.txt", "old.txt", "n.txt"],
            Some(&cs1.to_string()),
            false,
        ),
    )
    .unwrap();
    assert_eq!(read(&ctx, "a.txt"), "a v1\n");
    assert_eq!(read(&ctx, "old.txt"), "old\n");
    assert!(!exists(&ctx, "n.txt"), "absent in the source: removed");
    assert_eq!(
        changes(&ctx),
        [
            ("a.txt".to_string(), Change::Modified),
            ("n.txt".to_string(), Change::Deleted),
            ("old.txt".to_string(), Change::Added),
        ]
    );
    // The result commits like any edit.
    commit::run(&mut ctx, &commit_args("revert a bit")).unwrap();
}

#[test]
fn restore_source_will_not_overwrite_a_new_file_without_force() {
    let mut env = Env::new();
    let cs1 = env
        .mock
        .seed(None, &[("a.txt", "a\n", FileKind::Regular)], &[]);
    let head = env.mock.seed(
        Some(cs1),
        &[("late.txt", "from head\n", FileKind::Regular)],
        &[],
    );
    let mut ctx = env.wc();
    fs::remove_file(ctx.wc.abs("late.txt")).unwrap();
    fs::write(ctx.wc.abs("late.txt"), "mine\n").unwrap();
    // Make late.txt untracked by pretending the base is cs1.
    ctx.wc.state.files.remove("late.txt");
    let src = head.to_string();
    let e = restore::run(&mut ctx, &restore_args(&["late.txt"], Some(&src), false)).unwrap_err();
    assert!(user_err(e).contains("--force"));
    assert_eq!(read(&ctx, "late.txt"), "mine\n");
    restore::run(&mut ctx, &restore_args(&["late.txt"], Some(&src), true)).unwrap();
    assert_eq!(read(&ctx, "late.txt"), "from head\n");
}

// ---- amend ----

#[test]
fn amend_folds_changes_into_an_unlanded_base() {
    let mut env = Env::new();
    let cs1 = seed_two(&env);
    let mut ctx = env.wc();
    write(&ctx, "a.txt", "a v1\n");
    write(&ctx, "n.txt", "n\n");
    let mut c = commit_args("first try");
    c.agent_model = Some("model-a".into());
    commit::run(&mut ctx, &c).unwrap();
    let old = ctx.wc.state.base.unwrap();

    write(&ctx, "a.txt", "a v2\n");
    fs::remove_file(ctx.wc.abs("n.txt")).unwrap();
    write(&ctx, "m.txt", "m\n");
    assert_eq!(amend::run(&mut ctx, &AmendArgs::default()).unwrap(), 0);
    let new = ctx.wc.state.base.unwrap();
    assert_ne!(new, old);
    assert!(status(&ctx.wc).unwrap().is_clean());
    {
        let d = env.mock.data.lock().unwrap();
        let cs = &d.changesets[&new];
        assert_eq!(cs.parents, [cs1], "same parents");
        assert_eq!(cs.message, "first try", "message kept");
        assert_eq!(cs.author, "tester", "author kept");
        assert_eq!(cs.extra[EXTRA_AGENT_MODEL], "model-a");
        let paths: Vec<_> = cs.changes.keys().map(|p| p.to_string()).collect();
        assert_eq!(paths, ["a.txt", "m.txt"], "n.txt was added then deleted");
        assert!(d.changesets.contains_key(&old), "the old one stays");
    }

    // A new message and a new agent model replace the old ones; no changes needed.
    let mut args = AmendArgs {
        message: Some("better words".into()),
        ..Default::default()
    };
    args.agent_model = Some("model-b".into());
    amend::run(&mut ctx, &args).unwrap();
    let d = env.mock.data.lock().unwrap();
    let cs = &d.changesets[&ctx.wc.state.base.unwrap()];
    assert_eq!(cs.message, "better words");
    assert_eq!(cs.extra[EXTRA_AGENT_MODEL], "model-b");
    assert_eq!(cs.changes.len(), 2);
    assert_eq!(d.refs["main"], cs1, "refs never move");
}

#[test]
fn amend_drops_a_change_put_back_to_the_parent_content() {
    let mut env = Env::new();
    seed_two(&env);
    let mut ctx = env.wc();
    write(&ctx, "a.txt", "changed\n");
    commit::run(&mut ctx, &commit_args("c")).unwrap();
    write(&ctx, "a.txt", "a\n");
    amend::run(
        &mut ctx,
        &AmendArgs {
            message: Some("empty now".into()),
            ..Default::default()
        },
    )
    .unwrap();
    let d = env.mock.data.lock().unwrap();
    assert!(d.changesets[&ctx.wc.state.base.unwrap()].changes.is_empty());
}

#[test]
fn amend_refuses_a_landed_base_nothing_to_do_and_conflicts() {
    let mut env = Env::new();
    seed_two(&env);
    let mut ctx = env.wc();
    let e = amend::run(&mut ctx, &AmendArgs::default()).unwrap_err();
    assert!(
        user_err(e).contains("already landed"),
        "base is main's head"
    );

    write(&ctx, "a.txt", "x\n");
    commit::run(&mut ctx, &commit_args("c")).unwrap();
    let e = amend::run(&mut ctx, &AmendArgs::default()).unwrap_err();
    assert!(exits(6, e).contains("nothing to amend"));

    land::run(
        &mut ctx,
        &LandArgs {
            target_ref: None,
            create: false,
        },
    )
    .unwrap();
    write(&ctx, "a.txt", "more\n");
    let base = ctx.wc.state.base;
    let e = amend::run(&mut ctx, &AmendArgs::default()).unwrap_err();
    let m = user_err(e);
    assert!(m.contains("already landed") && m.contains("revert"), "{m}");
    assert_eq!(ctx.wc.state.base, base);
    assert_eq!(read(&ctx, "a.txt"), "more\n");
}

#[test]
fn amend_refuses_conflict_markers() {
    let mut env = Env::new();
    let cs1 = seed_two(&env);
    let mut ctx = env.wc();
    write(&ctx, "b.txt", "b local\n");
    commit::run(&mut ctx, &commit_args("local")).unwrap();
    write(&ctx, "a.txt", "a mine\n");
    env.mock.seed(
        Some(cs1),
        &[("a.txt", "a theirs\n", FileKind::Regular)],
        &[],
    );
    assert_eq!(sync(&mut ctx, &SyncArgs::default()).unwrap(), 5);
    let e = amend::run(&mut ctx, &AmendArgs::default()).unwrap_err();
    assert!(conflict_err(e).contains("conflict"));
}

// ---- reset ----

#[test]
fn reset_defaults_to_uncommitting_and_keeps_the_changes() {
    let mut env = Env::new();
    let cs1 = seed_two(&env);
    let mut ctx = env.wc();
    write(&ctx, "a.txt", "a c1\n");
    commit::run(&mut ctx, &commit_args("one")).unwrap();
    let c1 = ctx.wc.state.base.unwrap();
    write(&ctx, "b.txt", "b c2\n");
    write(&ctx, "n.txt", "n c2\n");
    commit::run(&mut ctx, &commit_args("two")).unwrap();
    let c2 = ctx.wc.state.base.unwrap();

    assert_eq!(
        reset::run(&mut ctx, &reset_args(None, false, false)).unwrap(),
        0
    );
    assert_eq!(ctx.wc.state.base, Some(c1));
    assert_eq!(read(&ctx, "b.txt"), "b c2\n", "files untouched");
    assert_eq!(
        changes(&ctx),
        [
            ("b.txt".to_string(), Change::Modified),
            ("n.txt".to_string(), Change::Added)
        ]
    );
    assert_eq!(env.main(), cs1, "refs never move");
    // The undone commit can be recommitted, or reached by id.
    commit::run(&mut ctx, &commit_args("two again")).unwrap();
    reset::run(&mut ctx, &reset_args(Some(&c2.to_string()), false, false)).unwrap();
    assert_eq!(ctx.wc.state.base, Some(c2));
    assert!(status(&ctx.wc).unwrap().is_clean());
}

#[test]
fn reset_soft_to_a_ref_with_same_size_edit_is_detected() {
    let mut env = Env::new();
    seed_two(&env);
    let mut ctx = env.wc();
    // Same length as the old content, so only a rehash can tell.
    write(&ctx, "a.txt", "A\n");
    commit::run(&mut ctx, &commit_args("caps")).unwrap();
    reset::run(&mut ctx, &reset_args(Some("main"), false, false)).unwrap();
    assert_eq!(changes(&ctx), [("a.txt".to_string(), Change::Modified)]);
}

#[test]
fn reset_errors_leave_everything_as_it_was() {
    let mut env = Env::new();
    seed_two(&env);
    let mut ctx = env.wc();
    // The base is a root changeset: no parent to default to.
    let e = reset::run(&mut ctx, &reset_args(None, false, false)).unwrap_err();
    assert!(user_err(e).contains("no parent"));
    write(&ctx, "a.txt", "mine\n");
    let state = ctx.wc.state.clone();
    let e = reset::run(&mut ctx, &reset_args(Some("nope"), false, false)).unwrap_err();
    assert_eq!(e.exit_code(), 4);
    assert_eq!(ctx.wc.state, state);
    let e = reset::run(&mut ctx, &reset_args(Some(&"ab".repeat(32)), true, true)).unwrap_err();
    assert_eq!(e.exit_code(), 4);
    assert_eq!(ctx.wc.state, state);
    assert_eq!(read(&ctx, "a.txt"), "mine\n");
}

#[test]
fn reset_hard_discards_only_with_force_and_keeps_new_files() {
    let mut env = Env::new();
    let cs1 = seed_two(&env);
    let mut ctx = env.wc();
    write(&ctx, "a.txt", "a c1\n");
    commit::run(&mut ctx, &commit_args("one")).unwrap();
    let c1 = ctx.wc.state.base.unwrap();
    write(&ctx, "b.txt", "b edited\n");
    write(&ctx, "n.txt", "untracked\n");
    fs::remove_file(ctx.wc.abs("a.txt")).unwrap();

    let e = reset::run(&mut ctx, &reset_args(Some("main"), true, false)).unwrap_err();
    assert!(user_err(e).contains("--force"));
    assert_eq!(ctx.wc.state.base, Some(c1));
    assert_eq!(read(&ctx, "b.txt"), "b edited\n");

    reset::run(&mut ctx, &reset_args(Some("main"), true, true)).unwrap();
    assert_eq!(ctx.wc.state.base, Some(cs1));
    assert_eq!(read(&ctx, "a.txt"), "a\n");
    assert_eq!(read(&ctx, "b.txt"), "b\n");
    assert_eq!(read(&ctx, "n.txt"), "untracked\n", "new files stay");
    assert_eq!(changes(&ctx), [("n.txt".to_string(), Change::Added)]);

    // Clean: --hard needs no force, and the default target is the parent.
    fs::remove_file(ctx.wc.abs("n.txt")).unwrap();
    write(&ctx, "a.txt", "a c3\n");
    commit::run(&mut ctx, &commit_args("three")).unwrap();
    reset::run(&mut ctx, &reset_args(None, true, false)).unwrap();
    assert_eq!(ctx.wc.state.base, Some(cs1));
    assert_eq!(read(&ctx, "a.txt"), "a\n");
}

#[test]
fn a_write_failing_midway_is_rolled_back() {
    let mut env = Env::new();
    let cs1 = seed_two(&env);
    let mut ctx = env.wc();
    // An ignored file named `sub` blocks `sub/f.txt`, which sorts after a.txt.
    write(&ctx, ".leepignore", "sub\n");
    write(&ctx, "sub", "in the way\n");
    write(&ctx, "b.txt", "b edited\n");
    env.mock.seed(
        Some(cs1),
        &[
            ("a.txt", "a head\n", FileKind::Regular),
            ("sub/f.txt", "f\n", FileKind::Regular),
        ],
        &[],
    );
    let before = ctx.wc.state.clone();
    let e = sync(&mut ctx, &SyncArgs::default()).unwrap_err();
    assert!(e.to_string().contains("snapshot"), "{e}");
    assert_eq!(read(&ctx, "a.txt"), "a\n", "the early write was undone");
    assert_eq!(read(&ctx, "b.txt"), "b edited\n");
    assert_eq!(read(&ctx, "sub"), "in the way\n");
    assert_eq!(ctx.wc.state, before);
    assert_eq!(WorkCopy::open(&ctx.wc.root).unwrap().state, before);
}

// ---- conflicts without a text merge ----

fn resolve_args(paths: &[&str], force: bool) -> ResolveArgs {
    ResolveArgs {
        paths: paths.iter().map(|s| s.to_string()).collect(),
        force,
    }
}

/// A working copy whose edit of a binary file conflicts with the head's.
fn binary_conflict(env: &mut Env) -> Ctx {
    let cs1 = env.mock.seed(
        None,
        &[
            ("img.bin", "base\0", FileKind::Regular),
            ("a.txt", "a\n", FileKind::Regular),
        ],
        &[],
    );
    let ctx = env.wc();
    write(&ctx, "img.bin", "mine\0");
    write(&ctx, "a.txt", "a mine\n");
    env.mock.seed(
        Some(cs1),
        &[("img.bin", "theirs\0", FileKind::Regular)],
        &[],
    );
    ctx
}

#[test]
fn sync_binary_conflict_blocks_commit_until_resolved() {
    let mut env = Env::new();
    let mut ctx = binary_conflict(&mut env);
    assert_eq!(sync(&mut ctx, &SyncArgs::default()).unwrap(), 5);
    assert_eq!(read(&ctx, "img.bin"), "mine\0", "local version kept");
    assert_eq!(ctx.wc.state.unresolved, ["img.bin"]);
    assert_eq!(
        changes(&ctx),
        [
            ("a.txt".to_string(), Change::Modified),
            ("img.bin".to_string(), Change::Conflicted)
        ]
    );
    let m = conflict_err(commit::run(&mut ctx, &commit_args("no")).unwrap_err());
    assert!(m.contains("img.bin") && m.contains("leep resolve"), "{m}");
    let m = conflict_err(
        land::run(
            &mut ctx,
            &LandArgs {
                target_ref: None,
                create: false,
            },
        )
        .unwrap_err(),
    );
    assert!(m.contains("unresolved conflicts in img.bin"), "{m}");
    // The state survives a restart, and the JSON status reports it.
    let mut again = env.reopen(&ctx);
    again.json = true;
    leep_cli::commands::status::run(&again).unwrap();
    assert_eq!(again.wc.state.unresolved, ["img.bin"]);

    let e = resolve::run(&mut ctx, &resolve_args(&["a.txt"], false)).unwrap_err();
    assert!(user_err(e).contains("no conflicted file"));
    resolve::run(&mut ctx, &resolve_args(&["img.bin"], false)).unwrap();
    assert!(ctx.wc.state.unresolved.is_empty());
    commit::run(&mut ctx, &commit_args("keep mine on purpose")).unwrap();
    assert_eq!(
        land::run(
            &mut ctx,
            &LandArgs {
                target_ref: None,
                create: false
            }
        )
        .unwrap(),
        0
    );
}

#[test]
fn restore_clears_an_unmerged_conflict() {
    let mut env = Env::new();
    let mut ctx = binary_conflict(&mut env);
    sync(&mut ctx, &SyncArgs::default()).unwrap();
    restore::run(&mut ctx, &restore_args(&["img.bin"], None, false)).unwrap();
    assert_eq!(read(&ctx, "img.bin"), "theirs\0");
    assert!(ctx.wc.state.unresolved.is_empty());
    assert_eq!(changes(&ctx), [("a.txt".to_string(), Change::Modified)]);
}

#[test]
fn land_binary_conflict_blocks_commit_until_resolved() {
    let mut env = Env::new();
    let cs1 = env
        .mock
        .seed(None, &[("img.bin", "base\0", FileKind::Regular)], &[]);
    let mut ctx = env.wc();
    write(&ctx, "img.bin", "mine\0");
    commit::run(&mut ctx, &commit_args("mine")).unwrap();
    let head = env.mock.seed(
        Some(cs1),
        &[("img.bin", "theirs\0", FileKind::Regular)],
        &[],
    );
    let land_args = LandArgs {
        target_ref: None,
        create: false,
    };
    assert_eq!(land::run(&mut ctx, &land_args).unwrap(), 5);
    assert_eq!(ctx.wc.state.base, Some(head));
    assert_eq!(ctx.wc.state.unresolved, ["img.bin"]);
    assert!(
        conflict_err(commit::run(&mut ctx, &commit_args("x")).unwrap_err()).contains("img.bin")
    );
    assert!(land::run(&mut ctx, &land_args).is_err());
    resolve::run(&mut ctx, &resolve_args(&["img.bin"], false)).unwrap();
    commit::run(&mut ctx, &commit_args("resolved")).unwrap();
    assert_eq!(land::run(&mut ctx, &land_args).unwrap(), 0);
}

#[test]
fn resolve_checks_markers_unless_forced() {
    let mut env = Env::new();
    let cs1 = seed_two(&env);
    let mut ctx = env.wc();
    write(&ctx, "a.txt", "mine\n");
    env.mock
        .seed(Some(cs1), &[("a.txt", "theirs\n", FileKind::Regular)], &[]);
    assert_eq!(sync(&mut ctx, &SyncArgs::default()).unwrap(), 5);
    let m = conflict_err(resolve::run(&mut ctx, &resolve_args(&["a.txt"], false)).unwrap_err());
    assert!(m.contains("markers remain"), "{m}");
    assert_eq!(ctx.wc.state.conflicts, ["a.txt"]);
    resolve::run(&mut ctx, &resolve_args(&["."], true)).unwrap();
    assert!(ctx.wc.state.conflicts.is_empty());
    assert_eq!(changes(&ctx), [("a.txt".to_string(), Change::Modified)]);
}
