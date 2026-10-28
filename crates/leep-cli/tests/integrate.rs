//! `merge`, `rebase`, `cherry-pick` and `revert` against the in-memory mock
//! server (ADR 0019).

mod common;

use std::fs;

use common::Mock;
use leep_cli::cache::Cache;
use leep_cli::commands::amend::{self, AmendArgs};
use leep_cli::commands::checkout::{checkout, sync, CheckoutArgs, SyncArgs};
use leep_cli::commands::commit::{self, CommitArgs};
use leep_cli::commands::init::{self, InitArgs};
use leep_cli::commands::land::{self, LandArgs};
use leep_cli::commands::merge::{self, MergeArgs};
use leep_cli::commands::pick::{self, CherryPickArgs, RevertArgs};
use leep_cli::commands::rebase::{self, RebaseArgs};
use leep_cli::commands::reset::{self, ResetArgs};
use leep_cli::commands::resolve::{self, ResolveArgs};
use leep_cli::commands::restore::{self, RestoreArgs};
use leep_cli::commands::stash::{self, PushArgs, StashArgs};
use leep_cli::commands::Ctx;
use leep_cli::error::CliError;
use leep_cli::state::WorkCopy;
use leep_cli::status::{status, Change};
use leep_core::changeset::{EXTRA_REBASED_FROM, EXTRA_REVERT_OF};
use leep_core::{Changeset, ChangesetId, FileChange, FileKind, Hash, RepoPath};

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

    /// A second look at the same files, like a new process.
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

    fn changeset(&self, id: ChangesetId) -> Changeset {
        self.mock.data.lock().unwrap().changesets[&id].clone()
    }
}

const REG: FileKind = FileKind::Regular;

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

/// `leep commit` with no message, as after a conflicted operation.
fn finish_args() -> CommitArgs {
    CommitArgs {
        author: Some("tester".into()),
        ..Default::default()
    }
}

fn merge_args(rev: &str) -> MergeArgs {
    MergeArgs {
        rev: Some(rev.into()),
        author: Some("tester".into()),
        ..Default::default()
    }
}

fn pick_args(rev: &str) -> CherryPickArgs {
    CherryPickArgs {
        rev: Some(rev.into()),
        ..Default::default()
    }
}

fn revert_args(rev: &str) -> RevertArgs {
    RevertArgs {
        rev: Some(rev.into()),
        author: Some("tester".into()),
        ..Default::default()
    }
}

fn rebase_args(onto: Option<&str>) -> RebaseArgs {
    RebaseArgs {
        onto: onto.map(Into::into),
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

/// The message of an error that reports conflicts to resolve (exit 5).
#[track_caller]
fn conflict_err(e: CliError) -> String {
    assert_eq!(e.exit_code(), 5, "{e}");
    e.to_string()
}

fn paths(cs: &Changeset) -> Vec<&str> {
    cs.changes.keys().map(|p| p.as_str()).collect()
}

/// `main`: a.txt and b.txt.
fn seed_two(env: &Env) -> ChangesetId {
    env.mock
        .seed(None, &[("a.txt", "a\n", REG), ("b.txt", "b\n", REG)], &[])
}

/// A merge changeset with no changes, for the "refuse merges" cases.
fn insert_merge(env: &Env, ours: ChangesetId, theirs: ChangesetId) -> ChangesetId {
    env.mock.data.lock().unwrap().insert(Changeset {
        parents: vec![ours, theirs],
        author: "seed".into(),
        timestamp: 1,
        message: "a merge".into(),
        changes: Default::default(),
        extra: Default::default(),
    })
}

/// `main` and `feature` both edit a.txt; `feature` also adds c.txt. The
/// working copy is on `main`. Returns `(main, feature)`.
fn text_conflict(env: &mut Env) -> (Ctx, ChangesetId, ChangesetId) {
    let cs1 = seed_two(env);
    let feat = env.mock.seed_as(
        "feature",
        "feature work",
        Some(cs1),
        &[("a.txt", "a feature\n", REG), ("c.txt", "c\n", REG)],
        &[],
    );
    let main = env.mock.seed(Some(cs1), &[("a.txt", "a main\n", REG)], &[]);
    (env.wc(), main, feat)
}

// ---- merge ----

#[test]
fn clean_merge_makes_a_two_parent_local_commit_that_lands() {
    let mut env = Env::new();
    let cs1 = seed_two(&env);
    let feat = env
        .mock
        .seed_as("feature", "add c", Some(cs1), &[("c.txt", "c\n", REG)], &[]);
    let main = env.mock.seed(Some(cs1), &[("d.txt", "d\n", REG)], &[]);
    let mut ctx = env.wc();

    assert_eq!(merge::run(&mut ctx, &merge_args("feature")).unwrap(), 0);
    let id = ctx.wc.state.base.unwrap();
    let cs = env.changeset(id);
    assert_eq!(cs.parents, [main, feat]);
    assert_eq!(cs.message, "Merge feature into main");
    assert_eq!(cs.author, "tester");
    assert_eq!(paths(&cs), ["c.txt"], "changes are relative to ours");
    assert_eq!(env.main(), main, "no ref moves");
    assert_eq!(read(&ctx, "c.txt"), "c\n");
    assert_eq!(read(&ctx, "d.txt"), "d\n");
    assert!(status(&ctx.wc).unwrap().is_clean());
    assert!(ctx.wc.state.merge_in_progress.is_none());
    assert_eq!(env.reopen(&ctx).wc.state.base, Some(id));

    // The merge lands like any commit; here, as a fast-forward.
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
    assert_eq!(env.main(), id);
}

#[test]
fn merge_takes_a_message_and_prints_json() {
    let mut env = Env::new();
    let cs1 = seed_two(&env);
    env.mock
        .seed_as("feature", "add c", Some(cs1), &[("c.txt", "c\n", REG)], &[]);
    env.mock.seed(Some(cs1), &[("d.txt", "d\n", REG)], &[]);
    let mut ctx = env.wc();
    ctx.json = true;
    let mut args = merge_args("feature");
    args.message = Some("bring in c".into());
    assert_eq!(merge::run(&mut ctx, &args).unwrap(), 0);
    assert_eq!(
        env.changeset(ctx.wc.state.base.unwrap()).message,
        "bring in c"
    );
}

#[test]
fn merge_fast_forwards_unless_told_not_to_and_skips_what_is_contained() {
    let mut env = Env::new();
    let cs1 = seed_two(&env);
    let feat = env
        .mock
        .seed_as("feature", "add c", Some(cs1), &[("c.txt", "c\n", REG)], &[]);
    let mut ctx = env.wc();

    assert_eq!(merge::run(&mut ctx, &merge_args("feature")).unwrap(), 0);
    assert_eq!(ctx.wc.state.base, Some(feat), "base moved to feature");
    assert_eq!(read(&ctx, "c.txt"), "c\n");
    assert!(env.mock.data.lock().unwrap().merges.is_empty());
    assert_eq!(env.main(), cs1, "no ref moves");

    // Already contained: nothing happens, with or without --no-ff.
    assert_eq!(merge::run(&mut ctx, &merge_args("main")).unwrap(), 0);
    assert_eq!(merge::run(&mut ctx, &merge_args("feature")).unwrap(), 0);
    let mut args = merge_args("feature");
    args.no_ff = true;
    assert_eq!(merge::run(&mut ctx, &args).unwrap(), 0);
    assert_eq!(ctx.wc.state.base, Some(feat));
    assert!(env.mock.data.lock().unwrap().merges.is_empty());
}

#[test]
fn merge_no_ff_makes_a_merge_commit_even_when_it_could_fast_forward() {
    let mut env = Env::new();
    let cs1 = seed_two(&env);
    let feat = env
        .mock
        .seed_as("feature", "add c", Some(cs1), &[("c.txt", "c\n", REG)], &[]);
    let mut ctx = env.wc();
    let mut args = merge_args("feature");
    args.no_ff = true;
    assert_eq!(merge::run(&mut ctx, &args).unwrap(), 0);
    let cs = env.changeset(ctx.wc.state.base.unwrap());
    assert_eq!(cs.parents, [cs1, feat]);
    assert_eq!(read(&ctx, "c.txt"), "c\n");
}

#[test]
fn merge_conflict_writes_markers_then_commit_records_both_parents() {
    let mut env = Env::new();
    let (mut ctx, main, feat) = text_conflict(&mut env);

    assert_eq!(merge::run(&mut ctx, &merge_args("feature")).unwrap(), 5);
    let text = read(&ctx, "a.txt");
    assert!(text.contains("<<<<<<<") && text.contains("a main") && text.contains("a feature"));
    assert_eq!(read(&ctx, "c.txt"), "c\n", "the clean change is applied");
    assert_eq!(ctx.wc.state.base, Some(main), "base stays the first parent");
    let m = ctx.wc.state.merge_in_progress.clone().unwrap();
    assert_eq!(
        (m.other, m.message.as_str()),
        (feat, "Merge feature into main")
    );
    assert_eq!(m.added, ["c.txt"]);
    assert_eq!(
        changes(&ctx),
        [
            ("a.txt".to_string(), Change::Conflicted),
            ("c.txt".to_string(), Change::Added)
        ]
    );
    assert_eq!(env.main(), main, "no ref moves");
    assert!(
        env.mock.data.lock().unwrap().merges.len() == 1,
        "one server call"
    );

    // Everything else waits until the merge is finished or aborted.
    let m = conflict_err(commit::run(&mut ctx, &commit_args("too soon")).unwrap_err());
    assert!(m.contains("conflict") && m.contains("a.txt"), "{m}");
    let in_progress = |e: CliError| {
        let m = conflict_err(e);
        assert!(m.contains("merge of") && m.contains("in progress"), "{m}");
    };
    in_progress(
        land::run(
            &mut ctx,
            &LandArgs {
                target_ref: None,
                create: false,
            },
        )
        .unwrap_err(),
    );
    in_progress(sync(&mut ctx, &SyncArgs::default()).unwrap_err());
    in_progress(merge::run(&mut ctx, &merge_args("feature")).unwrap_err());
    in_progress(rebase::run(&mut ctx, &rebase_args(None)).unwrap_err());
    in_progress(pick::cherry_pick(&mut ctx, &pick_args("feature")).unwrap_err());
    in_progress(pick::revert(&mut ctx, &revert_args("main")).unwrap_err());
    in_progress(amend::run(&mut ctx, &AmendArgs::default()).unwrap_err());
    in_progress(
        stash::run(
            &mut ctx,
            &StashArgs {
                cmd: None,
                push: PushArgs::default(),
            },
        )
        .unwrap_err(),
    );
    in_progress(
        reset::run(
            &mut ctx,
            &ResetArgs {
                rev: None,
                hard: false,
                force: false,
            },
        )
        .unwrap_err(),
    );
    in_progress(
        checkout(
            &mut ctx,
            &CheckoutArgs {
                rev: "main".into(),
                prefixes: vec![],
                add_prefixes: vec![],
                force: false,
            },
        )
        .unwrap_err(),
    );
    // The state survives a restart.
    assert_eq!(
        env.reopen(&ctx).wc.state.merge_in_progress.unwrap().other,
        feat
    );

    write(&ctx, "a.txt", "a resolved\n");
    assert_eq!(commit::run(&mut ctx, &finish_args()).unwrap(), 0);
    let id = ctx.wc.state.base.unwrap();
    let cs = env.changeset(id);
    assert_eq!(cs.parents, [main, feat]);
    assert_eq!(
        cs.message, "Merge feature into main",
        "stored message is the default"
    );
    assert_eq!(paths(&cs), ["a.txt", "c.txt"]);
    assert!(ctx.wc.state.merge_in_progress.is_none());
    assert!(env.reopen(&ctx).wc.state.merge_in_progress.is_none());
    assert!(status(&ctx.wc).unwrap().is_clean());
    // Lands as a fast-forward of main.
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
    assert_eq!(env.main(), id);
}

#[test]
fn commit_after_a_conflicted_merge_can_take_its_own_message() {
    let mut env = Env::new();
    let (mut ctx, _, _) = text_conflict(&mut env);
    merge::run(&mut ctx, &merge_args("feature")).unwrap();
    write(&ctx, "a.txt", "a resolved\n");
    commit::run(&mut ctx, &commit_args("merge feature, keeping both")).unwrap();
    let cs = env.changeset(ctx.wc.state.base.unwrap());
    assert_eq!(cs.message, "merge feature, keeping both");
    assert_eq!(cs.parents.len(), 2);
}

#[test]
fn a_merge_resolved_entirely_to_our_side_still_commits_as_a_merge() {
    let mut env = Env::new();
    let cs1 = seed_two(&env);
    let feat = env.mock.seed_as(
        "feature",
        "edit a",
        Some(cs1),
        &[("a.txt", "a feature\n", REG)],
        &[],
    );
    let main = env.mock.seed(Some(cs1), &[("a.txt", "a main\n", REG)], &[]);
    let mut ctx = env.wc();
    assert_eq!(merge::run(&mut ctx, &merge_args("feature")).unwrap(), 5);
    restore::run(
        &mut ctx,
        &RestoreArgs {
            paths: vec!["a.txt".into()],
            source: None,
            force: false,
        },
    )
    .unwrap();
    assert!(
        status(&ctx.wc).unwrap().is_clean(),
        "nothing differs from the base"
    );
    assert_eq!(commit::run(&mut ctx, &finish_args()).unwrap(), 0);
    let cs = env.changeset(ctx.wc.state.base.unwrap());
    assert_eq!(cs.parents, [main, feat]);
    assert!(cs.changes.is_empty(), "our side won everywhere");
}

#[test]
fn a_commit_without_a_message_is_refused_outside_a_merge() {
    let mut env = Env::new();
    seed_two(&env);
    let mut ctx = env.wc();
    write(&ctx, "a.txt", "changed\n");
    let m = user_err(commit::run(&mut ctx, &finish_args()).unwrap_err());
    assert!(m.contains("message is required"), "{m}");
}

#[test]
fn merge_binary_conflict_keeps_our_file_and_blocks_until_resolved() {
    let mut env = Env::new();
    let cs1 = env.mock.seed(
        None,
        &[("img.bin", "base\0", REG), ("a.txt", "a\n", REG)],
        &[],
    );
    let feat = env.mock.seed_as(
        "feature",
        "new image",
        Some(cs1),
        &[("img.bin", "theirs\0", REG), ("a.txt", "a feature\n", REG)],
        &[],
    );
    let main = env.mock.seed(Some(cs1), &[("img.bin", "ours\0", REG)], &[]);
    let mut ctx = env.wc();

    assert_eq!(merge::run(&mut ctx, &merge_args("feature")).unwrap(), 5);
    assert_eq!(read(&ctx, "img.bin"), "ours\0", "our version kept");
    assert_eq!(read(&ctx, "a.txt"), "a feature\n", "clean change applied");
    assert_eq!(ctx.wc.state.unresolved, ["img.bin"]);
    assert!(ctx.wc.state.conflicts.is_empty());
    assert_eq!(
        changes(&ctx),
        [
            ("a.txt".to_string(), Change::Modified),
            ("img.bin".to_string(), Change::Conflicted)
        ]
    );
    let m = conflict_err(commit::run(&mut ctx, &finish_args()).unwrap_err());
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
    assert!(m.contains("in progress"), "{m}");

    resolve::run(
        &mut ctx,
        &ResolveArgs {
            paths: vec!["img.bin".into()],
            force: false,
        },
    )
    .unwrap();
    commit::run(&mut ctx, &finish_args()).unwrap();
    let cs = env.changeset(ctx.wc.state.base.unwrap());
    assert_eq!(cs.parents, [main, feat]);
    assert_eq!(paths(&cs), ["a.txt"], "img.bin stayed as ours");
}

#[test]
fn merge_abort_restores_the_working_copy_and_removes_what_the_merge_added() {
    let mut env = Env::new();
    let (mut ctx, main, _) = text_conflict(&mut env);
    let err = merge::run(
        &mut ctx,
        &MergeArgs {
            abort: true,
            ..Default::default()
        },
    )
    .unwrap_err();
    assert!(user_err(err).contains("no merge is in progress"));

    assert_eq!(merge::run(&mut ctx, &merge_args("feature")).unwrap(), 5);
    write(&ctx, "b.txt", "stray edit\n");
    let abort = MergeArgs {
        abort: true,
        ..Default::default()
    };
    assert_eq!(merge::run(&mut ctx, &abort).unwrap(), 0);
    assert_eq!(ctx.wc.state.base, Some(main));
    assert!(ctx.wc.state.merge_in_progress.is_none());
    assert!(ctx.wc.state.conflicts.is_empty());
    assert_eq!(read(&ctx, "a.txt"), "a main\n");
    assert_eq!(read(&ctx, "b.txt"), "b\n");
    assert!(!exists(&ctx, "c.txt"), "the file the merge added is gone");
    assert!(status(&ctx.wc).unwrap().is_clean());
    assert!(env.reopen(&ctx).wc.state.merge_in_progress.is_none());
    // And the same merge can be started again.
    assert_eq!(merge::run(&mut ctx, &merge_args("feature")).unwrap(), 5);
}

#[test]
fn merge_with_a_server_that_omits_clean_changes_is_refused_and_changes_nothing() {
    let mut env = Env::new();
    let (mut ctx, main, _) = text_conflict(&mut env);
    env.mock.data.lock().unwrap().omit_merge_clean = true;
    let m = user_err(merge::run(&mut ctx, &merge_args("feature")).unwrap_err());
    assert!(m.contains("upgrade the server"), "{m}");
    assert_eq!(ctx.wc.state.base, Some(main));
    assert!(ctx.wc.state.merge_in_progress.is_none());
    assert_eq!(read(&ctx, "a.txt"), "a main\n");
    assert!(!exists(&ctx, "c.txt"));
    assert!(status(&ctx.wc).unwrap().is_clean());
}

#[test]
fn merge_conflict_with_nothing_clean_works_against_such_a_server() {
    let mut env = Env::new();
    let cs1 = seed_two(&env);
    env.mock.seed_as(
        "feature",
        "edit a",
        Some(cs1),
        &[("a.txt", "a feature\n", REG)],
        &[],
    );
    env.mock.seed(Some(cs1), &[("a.txt", "a main\n", REG)], &[]);
    let mut ctx = env.wc();
    env.mock.data.lock().unwrap().omit_merge_clean = true;
    // Nothing besides the conflict changed, so an empty `clean` is complete.
    assert_eq!(merge::run(&mut ctx, &merge_args("feature")).unwrap(), 5);
    assert!(read(&ctx, "a.txt").contains("<<<<<<<"));
}

#[test]
fn merge_signs_a_clean_result_on_request() {
    use leep_cli::sign::AuthorKey;
    let mut env = Env::new();
    let cs1 = seed_two(&env);
    env.mock
        .seed_as("feature", "add c", Some(cs1), &[("c.txt", "c\n", REG)], &[]);
    env.mock.seed(Some(cs1), &[("d.txt", "d\n", REG)], &[]);
    let mut ctx = env.wc();
    let keys = tempfile::tempdir().unwrap();
    let keyfile = keys.path().join("author.key");
    AuthorKey::generate().unwrap().save(&keyfile).unwrap();
    let mut args = merge_args("feature");
    args.sign = Some(keyfile);
    merge::run(&mut ctx, &args).unwrap();
    let id = ctx.wc.state.base.unwrap();
    let d = env.mock.data.lock().unwrap();
    assert_eq!(d.signatures.len(), 1);
    assert_eq!(d.signatures[0].0, id);
}

// ---- rebase ----

#[test]
fn rebase_moves_the_local_stack_onto_another_branch() {
    let mut env = Env::new();
    let cs1 = seed_two(&env);
    let feat = env
        .mock
        .seed_as("feature", "add c", Some(cs1), &[("c.txt", "c\n", REG)], &[]);
    let mut ctx = env.wc();
    write(&ctx, "b.txt", "b local\n");
    commit::run(&mut ctx, &commit_args("one")).unwrap();
    write(&ctx, "d.txt", "d local\n");
    commit::run(&mut ctx, &commit_args("two")).unwrap();
    let old = ctx.wc.state.base.unwrap();

    ctx.json = true;
    assert_eq!(
        rebase::run(&mut ctx, &rebase_args(Some("feature"))).unwrap(),
        0
    );
    let new = ctx.wc.state.base.unwrap();
    assert_ne!(new, old);
    let top = env.changeset(new);
    assert_eq!(top.message, "two");
    let mid = env.changeset(top.parents[0]);
    assert_eq!(
        (mid.message.as_str(), mid.parents.clone()),
        ("one", vec![feat])
    );
    for (f, body) in [
        ("a.txt", "a\n"),
        ("b.txt", "b local\n"),
        ("c.txt", "c\n"),
        ("d.txt", "d local\n"),
    ] {
        assert_eq!(read(&ctx, f), body);
    }
    assert!(status(&ctx.wc).unwrap().is_clean());
    assert_eq!(env.main(), cs1, "no ref moves");
    assert_eq!(env.mock.data.lock().unwrap().refs["feature"], feat);
    assert_eq!(env.reopen(&ctx).wc.state.base, Some(new));
}

#[test]
fn rebase_without_an_argument_goes_onto_the_target_head() {
    let mut env = Env::new();
    let cs1 = seed_two(&env);
    let mut ctx = env.wc();
    write(&ctx, "b.txt", "b local\n");
    commit::run(&mut ctx, &commit_args("local")).unwrap();
    let head = env.mock.seed(Some(cs1), &[("c.txt", "c\n", REG)], &[]);
    assert_eq!(rebase::run(&mut ctx, &rebase_args(None)).unwrap(), 0);
    let cs = env.changeset(ctx.wc.state.base.unwrap());
    assert_eq!(
        (cs.message.as_str(), cs.parents.clone()),
        ("local", vec![head])
    );
    assert_eq!(read(&ctx, "c.txt"), "c\n");
    assert_eq!(read(&ctx, "b.txt"), "b local\n");
    // The moved commit lands as a fast-forward.
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
fn rebase_fast_forwards_and_knows_when_it_is_up_to_date() {
    let mut env = Env::new();
    let cs1 = seed_two(&env);
    let feat = env
        .mock
        .seed_as("feature", "add c", Some(cs1), &[("c.txt", "c\n", REG)], &[]);
    let mut ctx = env.wc();
    assert_eq!(
        rebase::run(&mut ctx, &rebase_args(Some("main"))).unwrap(),
        0
    );
    assert_eq!(ctx.wc.state.base, Some(cs1));
    assert_eq!(
        rebase::run(&mut ctx, &rebase_args(Some("feature"))).unwrap(),
        0
    );
    assert_eq!(
        ctx.wc.state.base,
        Some(feat),
        "nothing local, so a fast-forward"
    );
    assert_eq!(read(&ctx, "c.txt"), "c\n");
    // A stack that already sits on the target is left alone.
    write(&ctx, "d.txt", "d\n");
    commit::run(&mut ctx, &commit_args("local")).unwrap();
    let base = ctx.wc.state.base;
    let rebases = env.mock.data.lock().unwrap().rebases.len();
    assert_eq!(
        rebase::run(&mut ctx, &rebase_args(Some("feature"))).unwrap(),
        0
    );
    assert_eq!(ctx.wc.state.base, base);
    assert_eq!(env.mock.data.lock().unwrap().rebases.len(), rebases + 1);
}

#[test]
fn rebase_stops_on_a_conflict_with_nothing_changed() {
    let mut env = Env::new();
    let cs1 = seed_two(&env);
    let feat = env.mock.seed_as(
        "feature",
        "feature edits a",
        Some(cs1),
        &[("a.txt", "a feature\n", REG)],
        &[],
    );
    let mut ctx = env.wc();
    write(&ctx, "b.txt", "b local\n");
    commit::run(&mut ctx, &commit_args("fine")).unwrap();
    write(&ctx, "a.txt", "a local\n");
    commit::run(&mut ctx, &commit_args("local edits a")).unwrap();
    let base = ctx.wc.state.base.unwrap();
    let state_before = ctx.wc.state.clone();

    let m = conflict_err(rebase::run(&mut ctx, &rebase_args(Some("feature"))).unwrap_err());
    assert!(m.contains("local edits a") && m.contains("a.txt"), "{m}");
    assert!(
        m.contains("nothing changed") && m.contains("cherry-pick"),
        "{m}"
    );
    assert!(
        m.contains(&base.to_string()) && m.contains(&feat.to_string()),
        "{m}"
    );
    assert_eq!(ctx.wc.state, state_before);
    assert_eq!(env.reopen(&ctx).wc.state, state_before);
    assert_eq!(read(&ctx, "a.txt"), "a local\n");
    assert!(status(&ctx.wc).unwrap().is_clean());
}

#[test]
fn rebase_refuses_a_stack_with_a_merge() {
    let mut env = Env::new();
    let cs1 = seed_two(&env);
    env.mock
        .seed_as("feature", "add c", Some(cs1), &[("c.txt", "c\n", REG)], &[]);
    let mut ctx = env.wc();
    write(&ctx, "b.txt", "b local\n");
    commit::run(&mut ctx, &commit_args("local")).unwrap();
    merge::run(&mut ctx, &merge_args("feature")).unwrap();
    let base = ctx.wc.state.base;
    env.mock.seed(Some(cs1), &[("e.txt", "e\n", REG)], &[]);
    let m = user_err(rebase::run(&mut ctx, &rebase_args(None)).unwrap_err());
    assert!(m.contains("merge"), "{m}");
    assert_eq!(ctx.wc.state.base, base);
}

// ---- cherry-pick ----

#[test]
fn cherry_pick_copies_one_change_onto_the_base() {
    let mut env = Env::new();
    let cs1 = seed_two(&env);
    let f1 = env
        .mock
        .seed_as("feature", "add c", Some(cs1), &[("c.txt", "c\n", REG)], &[]);
    let f2 = env.mock.seed_as(
        "feature",
        "add e\n\nwith a body",
        Some(f1),
        &[("e.txt", "e\n", REG)],
        &[],
    );
    let main = env.mock.seed(Some(cs1), &[("d.txt", "d\n", REG)], &[]);
    let mut ctx = env.wc();

    assert_eq!(
        pick::cherry_pick(&mut ctx, &pick_args("feature")).unwrap(),
        0
    );
    let id = ctx.wc.state.base.unwrap();
    let cs = env.changeset(id);
    assert_eq!(cs.parents, [main]);
    assert_eq!(cs.message, "add e\n\nwith a body");
    assert_eq!(cs.extra[EXTRA_REBASED_FROM], f2.to_string());
    assert_eq!(read(&ctx, "e.txt"), "e\n");
    assert!(!exists(&ctx, "c.txt"), "only the picked change comes along");
    assert_eq!(read(&ctx, "d.txt"), "d\n");
    assert!(status(&ctx.wc).unwrap().is_clean());
    assert_eq!(env.main(), main, "no ref moves");
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
    assert_eq!(env.main(), id);
}

#[test]
fn cherry_pick_refuses_merges_and_what_the_base_already_has() {
    let mut env = Env::new();
    let cs1 = seed_two(&env);
    let feat = env
        .mock
        .seed_as("feature", "add c", Some(cs1), &[("c.txt", "c\n", REG)], &[]);
    let main = env.mock.seed(Some(cs1), &[("d.txt", "d\n", REG)], &[]);
    let merged = insert_merge(&env, main, feat);
    env.mock
        .data
        .lock()
        .unwrap()
        .refs
        .insert("merged".into(), merged);
    let mut ctx = env.wc();
    let m = user_err(pick::cherry_pick(&mut ctx, &pick_args("merged")).unwrap_err());
    assert!(m.contains("is a merge"), "{m}");
    let m = user_err(pick::cherry_pick(&mut ctx, &pick_args("@")).unwrap_err());
    assert!(m.contains("already in the history"), "{m}");
    let m = user_err(pick::cherry_pick(&mut ctx, &pick_args(&cs1.to_string())).unwrap_err());
    assert!(m.contains("already in the history"), "{m}");
    assert_eq!(ctx.wc.state.base, Some(main));
}

#[test]
fn cherry_pick_conflict_then_commit_uses_the_picked_message() {
    let mut env = Env::new();
    let (mut ctx, main, feat) = text_conflict(&mut env);

    assert_eq!(
        pick::cherry_pick(&mut ctx, &pick_args("feature")).unwrap(),
        5
    );
    assert_eq!(ctx.wc.state.base, Some(main));
    let p = ctx.wc.state.pick_in_progress.clone().unwrap();
    assert_eq!(
        (p.from, p.message.as_str(), p.revert),
        (feat, "feature work", false)
    );
    assert!(read(&ctx, "a.txt").contains("<<<<<<<"));
    assert_eq!(read(&ctx, "c.txt"), "c\n");
    let m = conflict_err(commit::run(&mut ctx, &finish_args()).unwrap_err());
    assert!(m.contains("conflict"), "{m}");
    let m = conflict_err(merge::run(&mut ctx, &merge_args("feature")).unwrap_err());
    assert!(
        m.contains("cherry-pick of") && m.contains("in progress"),
        "{m}"
    );
    // The wrong abort is refused and changes nothing.
    let m = user_err(
        pick::revert(
            &mut ctx,
            &RevertArgs {
                abort: true,
                ..Default::default()
            },
        )
        .unwrap_err(),
    );
    assert!(m.contains("cherry-pick"), "{m}");
    assert!(ctx.wc.state.pick_in_progress.is_some());

    write(&ctx, "a.txt", "a both\n");
    assert_eq!(commit::run(&mut ctx, &finish_args()).unwrap(), 0);
    let cs = env.changeset(ctx.wc.state.base.unwrap());
    assert_eq!(cs.parents, [main]);
    assert_eq!(cs.message, "feature work");
    assert_eq!(cs.extra[EXTRA_REBASED_FROM], feat.to_string());
    assert!(ctx.wc.state.pick_in_progress.is_none());
    assert!(env.reopen(&ctx).wc.state.pick_in_progress.is_none());
}

#[test]
fn cherry_pick_abort_restores_everything() {
    let mut env = Env::new();
    let (mut ctx, main, _) = text_conflict(&mut env);
    let abort = CherryPickArgs {
        abort: true,
        ..Default::default()
    };
    let m = user_err(pick::cherry_pick(&mut ctx, &abort).unwrap_err());
    assert!(m.contains("no cherry-pick is in progress"), "{m}");
    pick::cherry_pick(&mut ctx, &pick_args("feature")).unwrap();
    assert_eq!(pick::cherry_pick(&mut ctx, &abort).unwrap(), 0);
    assert_eq!(ctx.wc.state.base, Some(main));
    assert!(ctx.wc.state.pick_in_progress.is_none());
    assert_eq!(read(&ctx, "a.txt"), "a main\n");
    assert!(!exists(&ctx, "c.txt"));
    assert!(status(&ctx.wc).unwrap().is_clean());
}

// ---- revert ----

/// `main`: a, b; then "edit b", "add c", "delete a". Returns the ids.
fn revert_history(env: &Env) -> (ChangesetId, ChangesetId, ChangesetId) {
    let cs1 = seed_two(env);
    let edit = env
        .mock
        .seed_as("main", "edit b", Some(cs1), &[("b.txt", "b2\n", REG)], &[]);
    let add = env
        .mock
        .seed_as("main", "add c", Some(edit), &[("c.txt", "c\n", REG)], &[]);
    let del = env
        .mock
        .seed_as("main", "delete a", Some(add), &[], &["a.txt"]);
    (edit, add, del)
}

#[test]
fn revert_undoes_an_edit_an_addition_and_a_deletion() {
    let mut env = Env::new();
    let (edit, add, del) = revert_history(&env);
    let mut ctx = env.wc();
    assert!(!exists(&ctx, "a.txt"));

    // A deletion comes back.
    assert_eq!(
        pick::revert(&mut ctx, &revert_args(&del.to_string())).unwrap(),
        0
    );
    assert_eq!(read(&ctx, "a.txt"), "a\n");
    let r1 = ctx.wc.state.base.unwrap();
    let cs = env.changeset(r1);
    assert_eq!(cs.parents, [del], "a local commit on top of the base");
    assert_eq!(
        cs.message,
        format!("Revert \"delete a\"\n\nThis reverts {del}.")
    );
    assert_eq!(cs.extra[EXTRA_REVERT_OF], del.to_string());
    assert_eq!(cs.author, "tester");

    // An addition goes away.
    assert_eq!(
        pick::revert(&mut ctx, &revert_args(&add.to_string())).unwrap(),
        0
    );
    assert!(!exists(&ctx, "c.txt"));
    // An edit is undone.
    assert_eq!(
        pick::revert(&mut ctx, &revert_args(&edit.to_string())).unwrap(),
        0
    );
    assert_eq!(read(&ctx, "b.txt"), "b\n");
    assert!(status(&ctx.wc).unwrap().is_clean());
    assert_eq!(env.main(), del, "no ref moves");

    // Each revert stored the inverse as a changeset on top of the original,
    // and rebased it; that is what makes the merge three-way.
    let d = env.mock.data.lock().unwrap();
    assert_eq!(d.rebases.len(), 3);
    let inverse = |n: usize| d.changesets[&d.rebases[n].0].clone();
    let (i0, i1, i2) = (inverse(0), inverse(1), inverse(2));
    assert_eq!(i0.parents, [del]);
    assert_eq!(
        i0.changes[&RepoPath::new("a.txt").unwrap()],
        FileChange::Modify {
            content: Hash::of_blob(b"a\n"),
            kind: REG,
            size: 2
        }
    );
    assert_eq!(i1.parents, [add]);
    assert_eq!(
        i1.changes[&RepoPath::new("c.txt").unwrap()],
        FileChange::Delete
    );
    assert_eq!(i2.parents, [edit]);
    assert_eq!(paths(&i2), ["b.txt"]);
}

#[test]
fn reverting_the_base_needs_no_rebase() {
    let mut env = Env::new();
    let (_, _, del) = revert_history(&env);
    let mut ctx = env.wc();
    assert_eq!(pick::revert(&mut ctx, &revert_args("@")).unwrap(), 0);
    let cs = env.changeset(ctx.wc.state.base.unwrap());
    assert_eq!(cs.parents, [del]);
    assert_eq!(cs.extra[EXTRA_REVERT_OF], del.to_string());
    assert_eq!(read(&ctx, "a.txt"), "a\n");
}

#[test]
fn revert_conflict_then_commit_uses_the_revert_message() {
    let mut env = Env::new();
    let cs1 = seed_two(&env);
    let edit = env
        .mock
        .seed_as("main", "edit b", Some(cs1), &[("b.txt", "b2\n", REG)], &[]);
    let later = env.mock.seed_as(
        "main",
        "edit b again",
        Some(edit),
        &[("b.txt", "b3\n", REG), ("c.txt", "c\n", REG)],
        &[],
    );
    let mut ctx = env.wc();

    assert_eq!(
        pick::revert(&mut ctx, &revert_args(&edit.to_string())).unwrap(),
        5
    );
    assert_eq!(ctx.wc.state.base, Some(later));
    let message = format!("Revert \"edit b\"\n\nThis reverts {edit}.");
    let p = ctx.wc.state.pick_in_progress.clone().unwrap();
    assert_eq!(
        (p.from, p.message.as_str(), p.revert),
        (edit, message.as_str(), true)
    );
    let text = read(&ctx, "b.txt");
    assert!(text.contains("<<<<<<<") && text.contains("b3"), "{text}");
    assert_eq!(changes(&ctx), [("b.txt".to_string(), Change::Conflicted)]);
    let m = conflict_err(commit::run(&mut ctx, &finish_args()).unwrap_err());
    assert!(m.contains("conflict") && m.contains("b.txt"), "{m}");
    let m = user_err(
        pick::cherry_pick(
            &mut ctx,
            &CherryPickArgs {
                abort: true,
                ..Default::default()
            },
        )
        .unwrap_err(),
    );
    assert!(m.contains("revert"), "{m}");

    write(&ctx, "b.txt", "b resolved\n");
    assert_eq!(commit::run(&mut ctx, &finish_args()).unwrap(), 0);
    let cs = env.changeset(ctx.wc.state.base.unwrap());
    assert_eq!(cs.parents, [later]);
    assert_eq!(cs.message, message);
    assert_eq!(cs.extra[EXTRA_REVERT_OF], edit.to_string());
    assert!(ctx.wc.state.pick_in_progress.is_none());
}

#[test]
fn revert_abort_restores_the_working_copy() {
    let mut env = Env::new();
    let cs1 = seed_two(&env);
    let edit = env
        .mock
        .seed_as("main", "edit b", Some(cs1), &[("b.txt", "b2\n", REG)], &[]);
    let later = env
        .mock
        .seed_as("main", "again", Some(edit), &[("b.txt", "b3\n", REG)], &[]);
    let mut ctx = env.wc();
    let abort = RevertArgs {
        abort: true,
        ..Default::default()
    };
    let m = user_err(pick::revert(&mut ctx, &abort).unwrap_err());
    assert!(m.contains("no revert is in progress"), "{m}");
    pick::revert(&mut ctx, &revert_args(&edit.to_string())).unwrap();
    assert_eq!(pick::revert(&mut ctx, &abort).unwrap(), 0);
    assert_eq!(ctx.wc.state.base, Some(later));
    assert_eq!(read(&ctx, "b.txt"), "b3\n");
    assert!(status(&ctx.wc).unwrap().is_clean());
    assert!(ctx.wc.state.pick_in_progress.is_none());
}

#[test]
fn revert_binary_conflict_keeps_our_file_and_needs_resolve() {
    let mut env = Env::new();
    let cs1 = env.mock.seed(None, &[("img.bin", "v1\0", REG)], &[]);
    let v2 = env.mock.seed_as(
        "main",
        "new image",
        Some(cs1),
        &[("img.bin", "v2\0", REG)],
        &[],
    );
    env.mock.seed_as(
        "main",
        "newer image",
        Some(v2),
        &[("img.bin", "v3\0", REG)],
        &[],
    );
    let mut ctx = env.wc();
    assert_eq!(
        pick::revert(&mut ctx, &revert_args(&v2.to_string())).unwrap(),
        5
    );
    assert_eq!(read(&ctx, "img.bin"), "v3\0");
    assert_eq!(ctx.wc.state.unresolved, ["img.bin"]);
    let m = conflict_err(commit::run(&mut ctx, &finish_args()).unwrap_err());
    assert!(m.contains("img.bin") && m.contains("leep resolve"), "{m}");
    resolve::run(
        &mut ctx,
        &ResolveArgs {
            paths: vec!["img.bin".into()],
            force: false,
        },
    )
    .unwrap();
    // Keeping our file leaves nothing to record: only an abort ends it.
    let m = user_err(commit::run(&mut ctx, &finish_args()).unwrap_err());
    assert!(
        m.contains("nothing to commit") && m.contains("revert --abort"),
        "{m}"
    );
}

#[test]
fn revert_refuses_merges_foreign_changesets_and_empty_ones() {
    let mut env = Env::new();
    let cs1 = seed_two(&env);
    let feat = env
        .mock
        .seed_as("feature", "add c", Some(cs1), &[("c.txt", "c\n", REG)], &[]);
    let main = env.mock.seed(Some(cs1), &[("d.txt", "d\n", REG)], &[]);
    let merged = insert_merge(&env, main, feat);
    let noop = {
        let mut d = env.mock.data.lock().unwrap();
        let id = d.insert(Changeset {
            parents: vec![merged],
            author: "seed".into(),
            timestamp: 1,
            message: "does nothing".into(),
            changes: [(
                RepoPath::new("d.txt").unwrap(),
                FileChange::Modify {
                    content: Hash::of_blob(b"d\n"),
                    kind: REG,
                    size: 2,
                },
            )]
            .into(),
            extra: Default::default(),
        });
        d.refs.insert("main".into(), id);
        id
    };
    env.mock.seed_as(
        "other",
        "elsewhere",
        Some(cs1),
        &[("z.txt", "z\n", REG)],
        &[],
    );
    let mut ctx = env.wc();
    let m = user_err(pick::revert(&mut ctx, &revert_args(&merged.to_string())).unwrap_err());
    assert!(m.contains("is a merge") && m.contains("mainline"), "{m}");
    let m = user_err(pick::revert(&mut ctx, &revert_args("@")).unwrap_err());
    assert!(m.contains("changes no files"), "{m}");
    let m = user_err(pick::revert(&mut ctx, &revert_args("other")).unwrap_err());
    assert!(m.contains("not in the history"), "{m}");
    assert_eq!(ctx.wc.state.base, Some(noop));
    assert!(env.mock.data.lock().unwrap().rebases.is_empty());
}

// ---- all of them ----

#[test]
fn every_command_needs_a_clean_working_copy_and_asks_the_server_nothing() {
    let mut env = Env::new();
    let cs1 = seed_two(&env);
    env.mock
        .seed_as("feature", "add c", Some(cs1), &[("c.txt", "c\n", REG)], &[]);
    let mut ctx = env.wc();
    write(&ctx, "a.txt", "dirty\n");
    let before = ctx.wc.state.clone();

    let mut errors = vec![
        merge::run(&mut ctx, &merge_args("feature")).unwrap_err(),
        rebase::run(&mut ctx, &rebase_args(Some("feature"))).unwrap_err(),
        pick::cherry_pick(&mut ctx, &pick_args("feature")).unwrap_err(),
        pick::revert(&mut ctx, &revert_args("@")).unwrap_err(),
    ];
    for e in errors.drain(..) {
        assert!(user_err(e).contains("uncommitted changes"));
    }
    assert_eq!(ctx.wc.state, before);
    assert_eq!(read(&ctx, "a.txt"), "dirty\n");
    let d = env.mock.data.lock().unwrap();
    assert!(d.merges.is_empty() && d.rebases.is_empty());
    assert_eq!(d.changesets.len(), 2, "no changeset was created");
}

#[test]
fn every_command_refuses_unresolved_paths_by_name() {
    let mut env = Env::new();
    let cs1 = env.mock.seed(
        None,
        &[("img.bin", "base\0", REG), ("a.txt", "a\n", REG)],
        &[],
    );
    let mut ctx = env.wc();
    write(&ctx, "img.bin", "mine\0");
    env.mock
        .seed(Some(cs1), &[("img.bin", "theirs\0", REG)], &[]);
    env.mock
        .seed_as("feature", "x", Some(cs1), &[("a.txt", "x\n", REG)], &[]);
    sync(&mut ctx, &SyncArgs::default()).unwrap();
    assert_eq!(ctx.wc.state.unresolved, ["img.bin"]);
    for e in [
        merge::run(&mut ctx, &merge_args("feature")).unwrap_err(),
        rebase::run(&mut ctx, &rebase_args(None)).unwrap_err(),
        pick::cherry_pick(&mut ctx, &pick_args("feature")).unwrap_err(),
        pick::revert(&mut ctx, &revert_args("@")).unwrap_err(),
    ] {
        let m = conflict_err(e);
        assert!(m.contains("unresolved conflicts in img.bin"), "{m}");
    }
}

#[test]
fn conflict_markers_name_the_sides_and_old_labels_still_count() {
    let mut env = Env::new();
    let (mut ctx, _main, feat) = text_conflict(&mut env);
    assert_eq!(merge::run(&mut ctx, &merge_args("feature")).unwrap(), 5);
    let text = read(&ctx, "a.txt");
    let theirs = format!(">>>>>>> {} (merging)\n", &feat.to_string()[..12]);
    assert!(text.starts_with("<<<<<<< yours\n"), "{text}");
    assert!(text.contains(&theirs), "{text}");
    assert!(!text.contains("<<<<<<< ours"), "{text}");

    let resolve_a = |ctx: &mut Ctx| {
        resolve::run(
            ctx,
            &ResolveArgs {
                paths: vec!["a.txt".into()],
                force: false,
            },
        )
    };
    let m = conflict_err(resolve_a(&mut ctx).unwrap_err());
    assert!(m.contains("markers remain"), "{m}");
    // A file with the old labels (an older client wrote it) still counts.
    write(
        &ctx,
        "a.txt",
        "<<<<<<< ours\na main\n=======\na feature\n>>>>>>> theirs\n",
    );
    let m = conflict_err(resolve_a(&mut ctx).unwrap_err());
    assert!(m.contains("markers remain"), "{m}");
    write(&ctx, "a.txt", "a both\n");
    assert_eq!(resolve_a(&mut ctx).unwrap(), 0);
}
