//! `leep reflog` and `leep undo` against the in-memory mock server.

#[allow(dead_code)]
mod common;

use std::fs;
use std::path::Path;

use common::Mock;
use leep_cli::cache::Cache;
use leep_cli::commands::branch::{self, BranchArgs};
use leep_cli::commands::commit::{self, CommitArgs};
use leep_cli::commands::init::{self, InitArgs};
use leep_cli::commands::land::{self, LandArgs};
use leep_cli::commands::reflog::{self, ReflogArgs, UndoArgs};
use leep_cli::commands::Ctx;
use leep_core::{ChangesetId, RefLogEntry};

fn init_wc(mock: &Mock, dir: &Path, cache: &Path) -> Ctx {
    let args = InitArgs {
        target_ref: "main".into(),
        prefixes: vec![],
        dir: Some(dir.to_path_buf()),
    };
    init::run(&args, &mock.url, None, false, Cache::new(cache), false)
        .map(|(c, _)| c)
        .unwrap()
}

/// Commit `body` as `f.txt` and land it on the target ref.
fn land_file(ctx: &mut Ctx, body: &str) -> ChangesetId {
    fs::write(ctx.wc.abs("f.txt"), body).unwrap();
    let args = CommitArgs {
        message: body.trim().into(),
        author: Some("tester".into()),
        ..Default::default()
    };
    commit::run(ctx, &args).unwrap();
    assert_eq!(land::run(ctx, &LandArgs { target_ref: None }).unwrap(), 0);
    ctx.wc.state.base.unwrap()
}

fn ref_of(mock: &Mock, name: &str) -> Option<ChangesetId> {
    mock.data.lock().unwrap().refs.get(name).copied()
}

fn log(mock: &Mock) -> Vec<RefLogEntry> {
    mock.data.lock().unwrap().reflog.clone()
}

fn undo(ctx: &Ctx, name: Option<&str>, to: Option<u64>) -> leep_cli::error::Result<i32> {
    let args = UndoArgs {
        refname: name.map(Into::into),
        to,
    };
    reflog::undo(&ctx.remote(), &args)
}

fn msg(e: leep_cli::error::CliError) -> String {
    assert_eq!(e.exit_code(), 1, "{e}");
    e.to_string()
}

fn setup() -> (Mock, Ctx, tempfile::TempDir, tempfile::TempDir) {
    let mock = Mock::start();
    let (wc, cache) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let ctx = init_wc(&mock, wc.path(), cache.path());
    (mock, ctx, wc, cache)
}

#[test]
fn undo_moves_the_ref_back_and_a_second_undo_redoes() {
    let (mock, mut ctx, _wc, _cache) = setup();
    let first = land_file(&mut ctx, "1\n");
    let second = land_file(&mut ctx, "2\n");
    assert_eq!(ref_of(&mock, "main"), Some(second));

    assert_eq!(undo(&ctx, None, None).unwrap(), 0);
    assert_eq!(ref_of(&mock, "main"), Some(first));
    let entries = log(&mock);
    let e = entries.last().unwrap();
    assert_eq!(
        (e.operation.as_str(), e.old, e.new),
        ("undo", Some(second), Some(first))
    );
    assert_eq!(e.message.as_deref(), Some("undo of 2"));
    assert_eq!(
        ctx.wc.state.base,
        Some(second),
        "the working copy is untouched"
    );

    // The redo is an undo of the undo.
    assert_eq!(undo(&ctx, None, None).unwrap(), 0);
    assert_eq!(ref_of(&mock, "main"), Some(second));
    assert_eq!(log(&mock).len(), 4);

    // And back to the very start: undoing the first land deletes the ref.
    assert_eq!(undo(&ctx, None, Some(1)).unwrap(), 0);
    assert_eq!(ref_of(&mock, "main"), None);
    // Undoing that delete recreates it where it was.
    assert_eq!(undo(&ctx, None, None).unwrap(), 0);
    assert_eq!(ref_of(&mock, "main"), Some(second));
}

#[test]
fn undo_recreates_a_deleted_branch_and_deletes_a_created_one() {
    let (mock, mut ctx, _wc, _cache) = setup();
    let head = land_file(&mut ctx, "1\n");
    let create = BranchArgs {
        name: Some("feature".into()),
        rev: None,
        delete: false,
        force_delete: false,
    };
    branch::branch(&ctx, &create).unwrap();
    let delete = BranchArgs {
        name: Some("feature".into()),
        rev: None,
        delete: false,
        force_delete: true,
    };
    branch::branch(&ctx, &delete).unwrap();
    assert_eq!(ref_of(&mock, "feature"), None);

    undo(&ctx, Some("feature"), None).unwrap();
    assert_eq!(ref_of(&mock, "feature"), Some(head), "delete undone");
    undo(&ctx, Some("feature"), Some(2)).unwrap();
    assert_eq!(ref_of(&mock, "feature"), None, "create undone");
    // The ref log of `feature` has all four moves; main's is separate.
    assert_eq!(
        mock.data
            .lock()
            .unwrap()
            .reflog
            .iter()
            .filter(|e| e.name == "feature")
            .count(),
        4
    );
}

#[test]
fn undo_refuses_when_there_is_nothing_to_undo_or_the_ref_moved() {
    let (mock, mut ctx, _wc, _cache) = setup();
    // A ref that predates the log has no entries.
    let seeded = mock.seed(None, &[], &[]);
    let e = msg(undo(&ctx, Some("main"), None).unwrap_err());
    assert!(e.contains("no log entries"), "{e}");
    assert_eq!(ref_of(&mock, "main"), Some(seeded));

    let a = land_file(&mut ctx, "a\n");
    let b = land_file(&mut ctx, "b\n");
    assert_ne!(a, b);

    let e = msg(undo(&ctx, None, Some(99)).unwrap_err());
    assert!(e.contains("no log entry 99"), "{e}");
    let e = msg(undo(&ctx, Some("other"), Some(1)).unwrap_err());
    assert!(e.contains("no log entry 1"), "{e}");

    // Already there: undoing entry 2 twice with --to.
    undo(&ctx, None, Some(2)).unwrap();
    let e = msg(undo(&ctx, None, Some(2)).unwrap_err());
    assert!(e.contains("nothing to undo"), "{e}");

    // A move the log does not describe (here: behind the server's back)
    // makes the default undo stop instead of guessing.
    mock.data.lock().unwrap().refs.insert("main".into(), b);
    let before = log(&mock).len();
    let e = msg(undo(&ctx, None, None).unwrap_err());
    assert!(e.contains("moved while undoing"), "{e}");
    assert_eq!(ref_of(&mock, "main"), Some(b));
    assert_eq!(log(&mock).len(), before);
}

#[test]
fn a_policy_refusal_surfaces_and_changes_nothing() {
    let (mock, mut ctx, _wc, _cache) = setup();
    let first = land_file(&mut ctx, "1\n");
    land_file(&mut ctx, "2\n");
    undo(&ctx, None, None).unwrap();
    assert_eq!(ref_of(&mock, "main"), Some(first));

    // The server now protects main, and the history undo would restore
    // is unsigned.
    mock.data
        .lock()
        .unwrap()
        .require_signed
        .insert("main".into());
    let before = log(&mock).len();
    let e = msg(undo(&ctx, None, None).unwrap_err());
    assert!(e.contains("not signed by a trusted key"), "{e}");
    assert_eq!(ref_of(&mock, "main"), Some(first));
    assert_eq!(log(&mock).len(), before);
}

#[test]
fn reflog_lists_newest_first_filters_by_ref_and_pages() {
    let (mock, mut ctx, _wc, _cache) = setup();
    for i in 0..5 {
        land_file(&mut ctx, &format!("{i}\n"));
    }
    ctx.client
        .cas_ref(
            "dev",
            &leep_core::api::CasRefRequest {
                expected: None,
                new: ref_of(&mock, "main"),
                ..Default::default()
            },
        )
        .unwrap();
    let r = ctx.remote();
    let all = ctx.client.ref_log(None, None, 50).unwrap().entries;
    assert_eq!(all.len(), 6);
    assert_eq!(all[0].name, "dev");
    assert!(all.windows(2).all(|w| w[0].seq > w[1].seq));
    let main = ctx.client.ref_log(Some("main"), None, 50).unwrap().entries;
    assert_eq!(main.len(), 5);
    let page = ctx
        .client
        .ref_log(Some("main"), Some(main[1].seq), 50)
        .unwrap()
        .entries;
    assert_eq!(page, main[2..]);
    for (name, n) in [(None, 3), (Some("main"), 100), (Some("none"), 5)] {
        let args = ReflogArgs {
            refname: name.map(Into::into),
            limit: n,
        };
        assert_eq!(reflog::reflog(&r, &args).unwrap(), 0);
    }
}
