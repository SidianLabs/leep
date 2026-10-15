//! End-to-end CLI flows against the in-memory mock server.

mod common;

use std::fs;
use std::path::Path;

use common::Mock;
use leep_cli::cache::Cache;
use leep_cli::client::LogQuery;
use leep_cli::commands::checkout::{checkout, sync, CheckoutArgs};
use leep_cli::commands::commit::{self, CommitArgs};
use leep_cli::commands::history::{self, BlameArgs, LogArgs, ProvenanceArgs};
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
    init::run(&args, &mock.url, None, false, Cache::new(cache), false).map(|(c, _)| c)
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

mod auth {
    use super::*;
    use common::Tokens;
    use leep_cli::state::WorkCopy;

    const TOKENS: Tokens = Tokens {
        write: "write-secret",
        read: "read-secret",
    };

    fn init_with(
        mock: &Mock,
        dir: &Path,
        cache: &Path,
        token: Option<&str>,
        store: bool,
    ) -> Result<Ctx, leep_cli::error::CliError> {
        let args = InitArgs {
            target_ref: "main".into(),
            prefixes: vec![],
            dir: Some(dir.to_path_buf()),
        };
        init::run(
            &args,
            &mock.url,
            token.map(String::from),
            store,
            Cache::new(cache),
            false,
        )
        .map(|(c, _)| c)
    }

    #[test]
    fn missing_or_bad_token_is_a_clean_401() {
        let mock = Mock::start_with_auth(TOKENS);
        mock.seed(None, &[("a.txt", "hi\n", FileKind::Regular)], &[]);
        let (wc, cache) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let e = init_with(&mock, wc.path(), cache.path(), None, false)
            .err()
            .unwrap();
        assert_eq!(e.exit_code(), 1);
        assert_eq!(
            e.to_string(),
            "authentication required: set LEEP_TOKEN (or pass --token)"
        );
        assert!(
            !wc.path().join(".leep").exists(),
            "no half-made working copy"
        );

        let e = init_with(&mock, wc.path(), cache.path(), Some("wrong-secret"), false)
            .err()
            .unwrap();
        assert_eq!(e.exit_code(), 1);
        assert!(e.to_string().starts_with("authentication required"));
        assert!(!e.to_string().contains("wrong-secret"));
    }

    #[test]
    fn write_token_runs_the_full_flow_and_is_stored_privately() {
        let mock = Mock::start_with_auth(TOKENS);
        mock.seed(None, &[("a.txt", "hi\n", FileKind::Regular)], &[]);
        let (dir, cache) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let mut ctx = init_with(&mock, dir.path(), cache.path(), Some(TOKENS.write), true).unwrap();
        assert_eq!(read(&ctx, "a.txt"), "hi\n");
        write(&ctx, "b.txt", "new\n");
        assert_eq!(commit::run(&mut ctx, &commit_args("add b")).unwrap(), 0);
        assert_eq!(
            land::run(&mut ctx, &LandArgs { target_ref: None }).unwrap(),
            0
        );

        // Later commands pick the token up from the config alone.
        let wc = WorkCopy::open(dir.path()).unwrap();
        assert_eq!(wc.config.token.as_deref(), Some(TOKENS.write));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let m = fs::metadata(dir.path().join(".leep/config.json"))
                .unwrap()
                .permissions()
                .mode();
            assert_eq!(m & 0o777, 0o600);
        }
    }

    #[test]
    fn read_token_gets_the_403_message_on_writes() {
        let mock = Mock::start_with_auth(TOKENS);
        mock.seed(None, &[("a.txt", "hi\n", FileKind::Regular)], &[]);
        let (dir, cache) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let mut ctx = init_with(&mock, dir.path(), cache.path(), Some(TOKENS.read), false).unwrap();
        assert_eq!(read(&ctx, "a.txt"), "hi\n", "reads work");
        assert!(WorkCopy::open(dir.path()).unwrap().config.token.is_none());
        write(&ctx, "b.txt", "new\n");
        let e = commit::run(&mut ctx, &commit_args("add b")).unwrap_err();
        assert_eq!(e.exit_code(), 1);
        assert_eq!(e.to_string(), "token lacks write access");
    }
}

#[test]
fn provenance_queries_reach_the_server_and_decode() {
    let mock = Mock::start();
    let (wc_dir, cache) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    mock.seed(None, &[("a.txt", "a\n", FileKind::Regular)], &[]);
    let mut ctx = init_wc(&mock, wc_dir.path(), cache.path(), &[]).unwrap();

    write(&ctx, "a.txt", "by agent\n");
    let mut args = commit_args("agent work");
    args.author = Some("robot".into());
    args.agent_model = Some("m1".into());
    args.agent_session = Some("42".into());
    commit::run(&mut ctx, &args).unwrap();
    let agent_cs = ctx.wc.state.base.unwrap();
    write(&ctx, "a.txt", "by human\n");
    commit::run(&mut ctx, &commit_args("human work")).unwrap();
    let tip = ctx.wc.state.base.unwrap();

    let log_args = |f: &dyn Fn(&mut LogArgs)| {
        let mut a = LogArgs {
            rev: None,
            path: None,
            limit: 20,
            author: None,
            agent: false,
            agent_model: None,
            agent_session: None,
            extra: vec![],
        };
        f(&mut a);
        a
    };
    let r = ctx.remote();
    let last_query = |kind: &str| {
        mock.data
            .lock()
            .unwrap()
            .queries
            .iter()
            .rev()
            .find(|(k, _)| k == kind)
            .unwrap()
            .1
            .clone()
    };

    // Flags become repeated `extra` params and the filtered answer comes back.
    let a = log_args(&|a| {
        a.agent_model = Some("m1".into());
        a.agent_session = Some("42".into());
        a.extra = vec!["agent.session".into()];
    });
    assert_eq!(history::log(&r, &a).unwrap(), 0);
    let q = last_query("log");
    assert_eq!(q.matches("extra=").count(), 3, "{q}");
    let resp = ctx.client.log(&tip, &a.query().unwrap()).unwrap();
    assert_eq!(resp.entries.len(), 1);
    assert_eq!(resp.entries[0].id, agent_cs);

    let by_author = LogQuery {
        author: Some("TESTER".into()),
        ..Default::default()
    };
    let resp = ctx.client.log(&tip, &by_author).unwrap();
    assert_eq!(resp.entries.len(), 1);
    assert_eq!(resp.entries[0].id, tip);
    let none = LogQuery {
        extra: vec!["agent.model:other".into()],
        ..Default::default()
    };
    assert!(ctx.client.log(&tip, &none).unwrap().entries.is_empty());

    // Provenance and blame decode the new response shapes.
    let pa = ProvenanceArgs {
        rev: None,
        path: Some("a.txt".into()),
        limit: Some(50),
    };
    assert_eq!(history::provenance(&r, &pa).unwrap(), 0);
    let q = last_query("provenance");
    assert!(q.contains("path=a.txt") && q.contains("limit=50"), "{q}");
    let p = ctx.client.provenance(&tip, None, None).unwrap();
    assert_eq!(p.scanned, 3);
    assert!(p.humans.iter().any(|h| h.author == "robot"));
    let blame = BlameArgs {
        args: vec!["a.txt".into()],
        who: true,
    };
    assert_eq!(history::blame(&r, &blame).unwrap(), 0);
    let b = ctx.client.blame(&agent_cs, "a.txt").unwrap();
    assert_eq!(b.changesets[&agent_cs].agent["agent.model"], "m1");
    let rows = history::blame_lines(&b, true);
    assert!(rows[0].contains("robot m1"), "{}", rows[0]);
}

mod signing {
    use std::os::unix::fs::PermissionsExt;

    use ed25519_dalek::{Signature, VerifyingKey};
    use leep_cli::sign::AuthorKey;
    use leep_core::api::SignaturesResponse;
    use leep_core::signature::{parse_public_key, parse_signature_hex, signing_message};

    use super::*;

    fn make_key(dir: &Path) -> (std::path::PathBuf, AuthorKey) {
        let path = dir.join("author.key");
        let key = AuthorKey::generate().unwrap();
        key.save(&path).unwrap();
        (path, key)
    }

    fn assert_valid(req: &leep_core::api::AddSignatureRequest, id: leep_core::ChangesetId) {
        let vk = VerifyingKey::from_bytes(&parse_public_key(&req.public_key).unwrap()).unwrap();
        let sig = Signature::from_bytes(&parse_signature_hex(&req.signature).unwrap());
        vk.verify_strict(&signing_message(&id), &sig)
            .expect("server-side verification of the posted signature");
    }

    #[test]
    fn commit_sign_posts_a_signature_the_server_can_verify() {
        let mock = Mock::start();
        let (wc_dir, cache, keys) = (
            tempfile::tempdir().unwrap(),
            tempfile::tempdir().unwrap(),
            tempfile::tempdir().unwrap(),
        );
        let (keyfile, key) = make_key(keys.path());
        let mut ctx = init_wc(&mock, wc_dir.path(), cache.path(), &[]).unwrap();

        write(&ctx, "a.txt", "one\n");
        let mut args = commit_args("unsigned");
        assert_eq!(commit::run(&mut ctx, &args).unwrap(), 0);
        assert!(mock.data.lock().unwrap().signatures.is_empty());

        write(&ctx, "a.txt", "two\n");
        args = commit_args("signed");
        args.sign = Some(keyfile);
        assert_eq!(commit::run(&mut ctx, &args).unwrap(), 0);
        let id = ctx.wc.state.base.unwrap();
        let d = mock.data.lock().unwrap();
        assert_eq!(d.signatures.len(), 1);
        let (signed_id, req) = &d.signatures[0];
        assert_eq!(*signed_id, id);
        assert_eq!(req.public_key, key.public_key());
        assert_valid(req, id);
    }

    #[test]
    fn unusable_key_fails_before_anything_is_uploaded() {
        let mock = Mock::start();
        let (wc_dir, cache, keys) = (
            tempfile::tempdir().unwrap(),
            tempfile::tempdir().unwrap(),
            tempfile::tempdir().unwrap(),
        );
        let (keyfile, _) = make_key(keys.path());
        std::fs::set_permissions(&keyfile, std::fs::Permissions::from_mode(0o644)).unwrap();
        let mut ctx = init_wc(&mock, wc_dir.path(), cache.path(), &[]).unwrap();
        write(&ctx, "a.txt", "one\n");
        let mut args = commit_args("x");
        args.sign = Some(keyfile);
        let e = commit::run(&mut ctx, &args).unwrap_err();
        assert!(e.to_string().contains("too open"), "{e}");
        let d = mock.data.lock().unwrap();
        assert!(d.puts.is_empty() && d.changesets.is_empty());
        assert!(ctx.wc.state.base.is_none());
    }

    #[test]
    fn land_onto_a_signature_required_ref_explains_what_to_do() {
        let mock = Mock::start();
        mock.data
            .lock()
            .unwrap()
            .require_signed
            .insert("main".into());
        let (wc_dir, cache, keys) = (
            tempfile::tempdir().unwrap(),
            tempfile::tempdir().unwrap(),
            tempfile::tempdir().unwrap(),
        );
        let (keyfile, _) = make_key(keys.path());
        let mut ctx = init_wc(&mock, wc_dir.path(), cache.path(), &[]).unwrap();

        write(&ctx, "a.txt", "one\n");
        commit::run(&mut ctx, &commit_args("unsigned")).unwrap();
        let e = land::run(&mut ctx, &LandArgs { target_ref: None }).unwrap_err();
        let msg = e.to_string();
        assert!(msg.contains("is not signed by a trusted key"), "{msg}");
        assert!(msg.contains("leep commit --sign"), "{msg}");
        assert_eq!(e.exit_code(), 1);
        assert!(!mock.data.lock().unwrap().refs.contains_key("main"));

        write(&ctx, "a.txt", "two\n");
        let mut args = commit_args("signed");
        args.sign = Some(keyfile);
        commit::run(&mut ctx, &args).unwrap();
        assert_eq!(
            land::run(&mut ctx, &LandArgs { target_ref: None }).unwrap(),
            0
        );
        assert_eq!(
            mock.data.lock().unwrap().refs["main"],
            ctx.wc.state.base.unwrap()
        );
    }

    #[test]
    fn show_lines_name_signers_and_flag_problems() {
        use leep_core::api::SignatureInfo;
        let info = |n: &str| SignatureInfo {
            key_id: "0123456789abcdef".into(),
            name: n.into(),
            public_key: "ed25519:00".into(),
        };
        let mut r = SignaturesResponse {
            signatures: vec![],
            verified: true,
            via: None,
        };
        assert!(history::signature_lines(&r).is_empty());
        r.signatures = vec![info("alice"), info("bob")];
        assert_eq!(
            history::signature_lines(&r),
            [
                "Signed-by: alice (key 0123456789abcdef)",
                "Signed-by: bob (key 0123456789abcdef)"
            ]
        );
        let via = leep_core::ChangesetId(Hash::of_blob(b"orig"));
        r.via = Some(via);
        r.verified = false;
        let lines = history::signature_lines(&r);
        assert!(lines[2].contains(&via.to_string()), "{lines:?}");
        assert!(lines[3].contains("failed to verify"), "{lines:?}");
    }
}
