//! End-to-end CLI flows against the in-memory mock server.

mod common;

use std::fs;
use std::path::Path;

use common::Mock;
use leep_cli::cache::Cache;
use leep_cli::commands::checkout::{checkout, sync, CheckoutArgs};
use leep_cli::commands::commit::{self, CommitArgs};
use leep_cli::commands::init::{self, InitArgs};
use leep_cli::commands::land::{self, LandArgs};
use leep_cli::commands::Ctx;
use leep_cli::status::{status, Change};
use leep_core::changeset::EXTRA_AGENT_MODEL;
use leep_core::{FileChange, FileKind, Hash};

fn init_wc(
    mock: &Mock,
    dir: &Path,
    cache: &Path,
    prefixes: &[&str],
) -> Result<Ctx, leep_cli::error::CliError> {
    let args = InitArgs {
        target_ref: "main".into(),
        prefixes: prefixes.iter().map(|s| s.to_string()).collect(),
        dir: Some(dir.to_path_buf()),
    };
    init::run(&args, &mock.url, Cache::new(cache), false).map(|(c, _)| c)
}

fn write(ctx: &Ctx, rel: &str, body: &str) {
    let p = ctx.wc.abs(rel);
    fs::create_dir_all(p.parent().unwrap()).unwrap();
    fs::write(p, body).unwrap();
}

fn read(ctx: &Ctx, rel: &str) -> String {
    fs::read_to_string(ctx.wc.abs(rel)).unwrap()
}

fn commit_args(msg: &str) -> CommitArgs {
    CommitArgs {
        message: msg.into(),
        author: Some("tester".into()),
        ..Default::default()
    }
}

#[test]
fn commit_uploads_only_missing_blobs() {
    let mock = Mock::start();
    let (wc_dir, cache) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let mut ctx = init_wc(&mock, wc_dir.path(), cache.path(), &[]).unwrap();
    assert!(ctx.wc.state.base.is_none());

    write(&ctx, "a.txt", "same\n");
    write(&ctx, "dir/b.txt", "same\n");
    let mut args = commit_args("first");
    args.agent_model = Some("test-model".into());
    assert_eq!(commit::run(&mut ctx, &args).unwrap(), 0);
    let first = ctx.wc.state.base.unwrap();
    {
        let d = mock.data.lock().unwrap();
        let cs = &d.changesets[&first];
        assert!(cs.parents.is_empty());
        assert_eq!(cs.changes.len(), 2);
        assert_eq!(cs.author, "tester");
        assert_eq!(cs.extra[EXTRA_AGENT_MODEL], "test-model");
        assert_eq!(
            d.puts,
            vec![Hash::of_blob(b"same\n")],
            "identical content uploaded once"
        );
    }
    assert!(status(&ctx.wc).unwrap().is_clean());

    // Nothing to commit is a user error.
    let e = commit::run(&mut ctx, &commit_args("noop")).unwrap_err();
    assert_eq!(e.exit_code(), 1);

    // Modify, delete, and add a file whose content the server already has.
    write(&ctx, "a.txt", "changed\n");
    fs::remove_file(ctx.wc.abs("dir/b.txt")).unwrap();
    write(&ctx, "c.txt", "same\n");
    commit::run(&mut ctx, &commit_args("second")).unwrap();
    let second = ctx.wc.state.base.unwrap();
    let d = mock.data.lock().unwrap();
    let cs = &d.changesets[&second];
    assert_eq!(cs.parents, vec![first]);
    assert!(matches!(
        cs.changes[&"dir/b.txt".try_into().unwrap()],
        FileChange::Delete
    ));
    assert!(matches!(
        cs.changes[&"c.txt".try_into().unwrap()],
        FileChange::Modify { .. }
    ));
    assert_eq!(d.puts.len(), 2, "only the new 'changed' blob was uploaded");
    assert!(!ctx.wc.state.files.contains_key("dir/b.txt"));
}

#[test]
fn checkout_materializes_prefixes_modes_and_prunes() {
    let mock = Mock::start();
    let (wc_dir, cache) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let cs1 = mock.seed(
        None,
        &[
            ("a.txt", "a\n", FileKind::Regular),
            ("run.sh", "#!/bin/sh\n", FileKind::Executable),
            ("link", "a.txt", FileKind::Symlink),
            ("sub/x.txt", "x1\n", FileKind::Regular),
            ("other/y.txt", "y\n", FileKind::Regular),
        ],
        &[],
    );

    let mut ctx = init_wc(&mock, wc_dir.path(), cache.path(), &["sub", "a.txt"]).unwrap();
    assert_eq!(ctx.wc.state.base, Some(cs1));
    assert_eq!(read(&ctx, "sub/x.txt"), "x1\n");
    assert!(ctx.wc.abs("a.txt").exists());
    assert!(!ctx.wc.abs("other").exists());
    assert!(!ctx.wc.abs("run.sh").exists());
    assert!(status(&ctx.wc).unwrap().is_clean());
    let h = Hash::of_blob(b"x1\n").to_hex();
    assert!(cache.path().join(&h[..2]).join(&h[2..]).is_file());

    // Widen to the whole repo.
    let all = CheckoutArgs {
        rev: "main".into(),
        prefixes: vec![".".into()],
        force: false,
    };
    checkout(&mut ctx, &all).unwrap();
    assert_eq!(read(&ctx, "other/y.txt"), "y\n");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = fs::metadata(ctx.wc.abs("run.sh"))
            .unwrap()
            .permissions()
            .mode();
        assert!(mode & 0o111 != 0);
        assert_eq!(
            fs::read_link(ctx.wc.abs("link")).unwrap().to_str(),
            Some("a.txt")
        );
    }
    assert!(status(&ctx.wc).unwrap().is_clean());

    // Server moves on: delete a.txt, edit sub/x.txt. Sync follows it.
    let cs2 = mock.seed(
        Some(cs1),
        &[("sub/x.txt", "x2\n", FileKind::Regular)],
        &["a.txt", "other/y.txt"],
    );
    sync(&mut ctx).unwrap();
    assert_eq!(ctx.wc.state.base, Some(cs2));
    assert!(!ctx.wc.abs("a.txt").exists());
    assert!(!ctx.wc.abs("other").exists(), "emptied directory pruned");
    assert_eq!(read(&ctx, "sub/x.txt"), "x2\n");

    // Dirty working copies are refused unless forced.
    write(&ctx, "sub/x.txt", "local edit\n");
    assert!(sync(&mut ctx).is_err());
    let back = CheckoutArgs {
        rev: "@".into(),
        prefixes: vec![],
        force: true,
    };
    checkout(&mut ctx, &back).unwrap();
    assert_eq!(read(&ctx, "sub/x.txt"), "x2\n");
}

#[test]
fn land_fast_forward_rebase_and_conflict() {
    let mock = Mock::start();
    let cache = tempfile::tempdir().unwrap();
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    mock.seed(
        None,
        &[
            ("a.txt", "a\n", FileKind::Regular),
            ("b.txt", "b\n", FileKind::Regular),
        ],
        &[],
    );
    let mut wcs: Vec<Ctx> = dirs
        .iter()
        .map(|d| init_wc(&mock, d.path(), cache.path(), &[]).unwrap())
        .collect();
    let land_args = LandArgs { target_ref: None };

    // A fast-forwards.
    write(&wcs[0], "a.txt", "a from A\n");
    commit::run(&mut wcs[0], &commit_args("A")).unwrap();
    assert_eq!(land::run(&mut wcs[0], &land_args).unwrap(), 0);
    let a_id = wcs[0].wc.state.base.unwrap();
    assert_eq!(mock.data.lock().unwrap().refs["main"], a_id);

    // B touches another file, is rebased, and its working copy catches up.
    write(&wcs[1], "b.txt", "b from B\n");
    commit::run(&mut wcs[1], &commit_args("B")).unwrap();
    let b_local = wcs[1].wc.state.base.unwrap();
    assert_eq!(land::run(&mut wcs[1], &land_args).unwrap(), 0);
    let head = mock.data.lock().unwrap().refs["main"];
    assert_ne!(head, b_local, "server rebased B");
    assert_eq!(wcs[1].wc.state.base, Some(head));
    assert_eq!(read(&wcs[1], "a.txt"), "a from A\n");
    assert_eq!(read(&wcs[1], "b.txt"), "b from B\n");
    assert!(status(&wcs[1].wc).unwrap().is_clean());

    // C edits a.txt differently from A and adds c.txt: conflict, exit 1.
    write(&wcs[2], "a.txt", "a from C\n");
    write(&wcs[2], "c.txt", "c from C\n");
    commit::run(&mut wcs[2], &commit_args("C")).unwrap();
    assert_eq!(land::run(&mut wcs[2], &land_args).unwrap(), 1);
    assert_eq!(
        mock.data.lock().unwrap().refs["main"],
        head,
        "ref unchanged"
    );
    // The working copy moved to main with C's changes re-applied on top.
    assert_eq!(wcs[2].wc.state.base, Some(head));
    assert_eq!(read(&wcs[2], "b.txt"), "b from B\n");
    assert_eq!(read(&wcs[2], "c.txt"), "c from C\n");
    let text = read(&wcs[2], "a.txt");
    assert!(text.contains("<<<<<<<") && text.contains("a from A") && text.contains("a from C"));
    let st = status(&wcs[2].wc).unwrap();
    assert!(st
        .entries
        .iter()
        .any(|e| e.path == "a.txt" && e.change == Change::Conflicted));
    assert!(commit::run(&mut wcs[2], &commit_args("again")).is_err());

    // Resolve, commit once on main, and land as a fast-forward: nothing lost.
    write(&wcs[2], "a.txt", "a resolved\n");
    commit::run(&mut wcs[2], &commit_args("C resolved")).unwrap();
    let resolved = wcs[2].wc.state.base.unwrap();
    assert_eq!(land::run(&mut wcs[2], &land_args).unwrap(), 0);
    assert_eq!(mock.data.lock().unwrap().refs["main"], resolved);
    assert_eq!(read(&wcs[2], "a.txt"), "a resolved\n");
    assert_eq!(read(&wcs[2], "b.txt"), "b from B\n");
    assert_eq!(read(&wcs[2], "c.txt"), "c from C\n");
    assert!(status(&wcs[2].wc).unwrap().is_clean());
}

#[test]
fn corrupt_download_is_rejected_and_not_cached() {
    let mock = Mock::start();
    let (wc_dir, cache) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    mock.seed(None, &[("a.txt", "precious\n", FileKind::Regular)], &[]);
    mock.data
        .lock()
        .unwrap()
        .corrupt
        .insert(Hash::of_blob(b"precious\n"));
    let err = init_wc(&mock, wc_dir.path(), cache.path(), &[])
        .err()
        .unwrap();
    assert_eq!(err.exit_code(), 2);
    assert!(err.to_string().contains("does not match"));
    let h = Hash::of_blob(b"precious\n").to_hex();
    assert!(!cache.path().join(&h[..2]).join(&h[2..]).exists());
    assert!(!wc_dir.path().join("a.txt").exists());
}
