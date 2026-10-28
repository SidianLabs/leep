//! A malicious server or repository must not make the CLI write outside the
//! working copy or into its control directories (security review 2026-10,
//! S-02). The mock server stores flat file lists, so it can serve trees no
//! real server would, like a symlink `a` and a file `a/x` side by side.

mod common;

use std::fs;
use std::path::Path;

use common::Mock;
use leep_cli::cache::Cache;
use leep_cli::commands::checkout::{checkout, CheckoutArgs};
use leep_cli::commands::init::{self, InitArgs};
use leep_cli::commands::Ctx;
use leep_core::FileKind;

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

fn checkout_rev(ctx: &mut Ctx, rev: &str, force: bool) -> String {
    let args = CheckoutArgs {
        rev: rev.into(),
        prefixes: vec![],
        add_prefixes: vec![],
        force,
    };
    checkout(ctx, &args).unwrap_err().to_string()
}

#[cfg(unix)]
#[test]
fn a_symlinked_directory_cannot_redirect_writes_outside() {
    let mock = Mock::start();
    mock.seed(None, &[("readme", "hi\n", FileKind::Regular)], &[]);
    let (wc_dir, cache) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let outside = tempfile::tempdir().unwrap();
    let mut ctx = init_wc(&mock, wc_dir.path(), cache.path());

    let target = outside.path().to_str().unwrap();
    mock.seed_on(
        "evil",
        None,
        &[
            ("a", target, FileKind::Symlink),
            ("a/pwned", "owned\n", FileKind::Regular),
        ],
        &[],
    );
    for force in [false, true] {
        let e = checkout_rev(&mut ctx, "evil", force);
        assert!(e.contains("symbolic link"), "{e}");
        assert_eq!(fs::read_dir(outside.path()).unwrap().count(), 0);
    }
}

#[test]
fn control_directories_are_never_written() {
    let mock = Mock::start();
    mock.seed(None, &[("readme", "hi\n", FileKind::Regular)], &[]);
    let (wc_dir, cache) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let mut ctx = init_wc(&mock, wc_dir.path(), cache.path());
    let config = fs::read(wc_dir.path().join(".leep/config.json")).unwrap();

    for path in [
        ".leep/config.json",
        "nested/.leep/config.json",
        ".git/hooks/post-checkout",
        ".GIT/hooks/pre-commit",
    ] {
        let rev = format!("evil-{}", path.len());
        mock.seed_on(&rev, None, &[(path, "{}", FileKind::Regular)], &[]);
        for force in [false, true] {
            let e = checkout_rev(&mut ctx, &rev, force);
            assert!(e.contains("control directory"), "{path}: {e}");
        }
        assert!(!wc_dir.path().join(".git").exists(), "{path}");
        assert!(!wc_dir.path().join("nested").exists(), "{path}");
    }
    assert_eq!(
        fs::read(wc_dir.path().join(".leep/config.json")).unwrap(),
        config
    );
}
