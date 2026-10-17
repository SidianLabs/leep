//! End-to-end CLI flows against the in-memory mock server.

mod common;

use std::fs;
use std::path::Path;

use common::Mock;
use leep_cli::cache::Cache;
use leep_cli::client::LogQuery;
use leep_cli::commands::branch::{self, BranchArgs, SwitchArgs, TagArgs};
use leep_cli::commands::checkout::{checkout, sync, CheckoutArgs, SyncArgs};
use leep_cli::commands::commit::{self, CommitArgs};
use leep_cli::commands::history::{self, BlameArgs, LastChangeArgs, LogArgs, ProvenanceArgs};
use leep_cli::commands::init::{self, InitArgs};
use leep_cli::commands::land::{self, LandArgs};
use leep_cli::commands::search::{self, SearchArgs};
use leep_cli::commands::Ctx;
use leep_cli::state::WorkCopy;
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
        message: Some(msg.into()),
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

    // Nothing to commit has its own exit code (ADR 0033).
    let e = commit::run(&mut ctx, &commit_args("noop")).unwrap_err();
    assert_eq!(e.exit_code(), 6);

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
    sync(&mut ctx, &SyncArgs::default()).unwrap();
    assert_eq!(ctx.wc.state.base, Some(cs2));
    assert!(!ctx.wc.abs("a.txt").exists());
    assert!(!ctx.wc.abs("other").exists(), "emptied directory pruned");
    assert_eq!(read(&ctx, "sub/x.txt"), "x2\n");

    // `--no-rebase` keeps the old rule: a dirty working copy is refused.
    write(&ctx, "sub/x.txt", "local edit\n");
    let strict = SyncArgs { no_rebase: true };
    assert!(sync(&mut ctx, &strict).is_err());
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

    // C edits a.txt differently from A and adds c.txt: conflict, exit 5.
    write(&wcs[2], "a.txt", "a from C\n");
    write(&wcs[2], "c.txt", "c from C\n");
    commit::run(&mut wcs[2], &commit_args("C")).unwrap();
    assert_eq!(land::run(&mut wcs[2], &land_args).unwrap(), 5);
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
    assert_eq!(err.exit_code(), 3);
    assert!(!err.is_retryable());
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
            follow: None,
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
    let lc = LastChangeArgs {
        path: "src".into(),
        rev: None,
    };
    assert_eq!(history::last_change(&r, &lc).unwrap(), 0);
    let q = last_query("last-change");
    assert!(q.contains("path=src"), "{q}");
    let resp = ctx.client.last_change(&tip, "src/a.rs").unwrap();
    assert_eq!((resp.path.as_str(), resp.changeset.id), ("src/a.rs", tip));
    assert!(resp.concurrent.is_empty());
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

#[test]
fn search_sends_flags_and_decodes_matches() {
    let mock = Mock::start();
    let (wc_dir, cache) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    mock.seed(
        None,
        &[
            (
                "src/a.rs",
                "fn main() {}\n// Needle here\n",
                FileKind::Regular,
            ),
            ("docs/b.md", "a needle\nsecond needle\n", FileKind::Regular),
        ],
        &[],
    );
    let ctx = init_wc(&mock, wc_dir.path(), cache.path(), &[]).unwrap();
    let tip = ctx.wc.state.base.unwrap();
    let last_query = || {
        mock.data
            .lock()
            .unwrap()
            .queries
            .iter()
            .rev()
            .find(|(k, _)| k == "search")
            .unwrap()
            .1
            .clone()
    };
    let args = |f: &dyn Fn(&mut SearchArgs)| {
        let mut a = SearchArgs {
            query: "needle".into(),
            rev: None,
            ignore_case: false,
            regex: false,
            path: None,
            glob: None,
            limit: None,
        };
        f(&mut a);
        a
    };
    let r = ctx.remote();

    // Matches exit 0 and the request carries only what was asked for.
    assert_eq!(search::run(&r, &args(&|_| {})).unwrap(), 0);
    assert_eq!(last_query(), "q=needle");
    let resp = ctx.client.search(&tip, &args(&|_| {}).query()).unwrap();
    assert_eq!(
        search::search_lines(&resp),
        ["docs/b.md:1:a needle", "docs/b.md:2:second needle"]
    );
    assert_eq!(resp.matches[0].ranges[0].start, 2);

    let a = args(&|a| {
        a.ignore_case = true;
        a.regex = true;
        a.path = Some("src".into());
        a.glob = Some("**/*.rs".into());
        a.limit = Some(5);
    });
    assert_eq!(search::run(&r, &a).unwrap(), 0);
    let q = last_query();
    for part in [
        "q=needle",
        "regex=true",
        "i=true",
        "path=src",
        "glob=",
        "limit=5",
    ] {
        assert!(q.contains(part), "{part} in {q}");
    }
    let resp = ctx.client.search(&tip, &a.query()).unwrap();
    assert_eq!(search::search_lines(&resp), ["src/a.rs:2:// Needle here"]);

    // Truncation is reported by the server and survives decoding.
    let one = ctx
        .client
        .search(&tip, &args(&|a| a.limit = Some(1)).query())
        .unwrap();
    assert!(one.truncated && one.matches.len() == 1);

    // No match is exit 1, like grep; an explicit revision works too.
    assert_eq!(
        search::run(&r, &args(&|a| a.query = "absent".into())).unwrap(),
        1
    );
    assert_eq!(
        search::run(&r, &args(&|a| a.rev = Some(tip.to_string()))).unwrap(),
        0
    );
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

fn long_text(tag: &str) -> String {
    (0..10).map(|i| format!("{tag} line {i}\n")).collect()
}

fn queries(mock: &Mock, kind: &str) -> Vec<String> {
    mock.data
        .lock()
        .unwrap()
        .queries
        .iter()
        .filter(|(k, _)| k == kind)
        .map(|(_, q)| q.clone())
        .collect()
}

#[test]
fn working_copy_status_and_diff_fold_renames() {
    use leep_cli::commands::diff::{working_diff, DiffArgs};
    use leep_cli::commands::status::{change_lines, renames};

    let mock = Mock::start();
    let (wc_dir, cache) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let mut ctx = init_wc(&mock, wc_dir.path(), cache.path(), &[]).unwrap();
    let (a, b) = (long_text("a"), long_text("b"));
    write(&ctx, "a.txt", &a);
    write(&ctx, "b.txt", &b);
    write(&ctx, "keep.txt", "k\n");
    commit::run(&mut ctx, &commit_args("base")).unwrap();

    fs::remove_file(ctx.wc.abs("a.txt")).unwrap();
    fs::remove_file(ctx.wc.abs("b.txt")).unwrap();
    write(&ctx, "dir/a.txt", &format!("{a}extra\n"));
    write(&ctx, "c.txt", &b);
    write(&ctx, "new.txt", "unrelated\n");

    // The raw status is unchanged: renames are a display layer.
    let st = status(&ctx.wc).unwrap();
    assert_eq!(st.entries.len(), 5);
    let found = renames(&ctx, &st);
    let lines = change_lines(&st, &found);
    assert_eq!(
        lines,
        [
            "renamed: b.txt -> c.txt (100%)",
            "renamed: a.txt -> dir/a.txt (96%)",
            "A new.txt"
        ]
    );

    let (text, found) = working_diff(&ctx, DiffArgs::default().threshold()).unwrap();
    assert_eq!(found.len(), 2);
    assert!(
        text.contains("renamed: a.txt -> dir/a.txt (96%)\n"),
        "{text}"
    );
    assert!(text.contains("--- a/a.txt\n+++ b/dir/a.txt\n") && text.contains("+extra"));
    assert!(text.contains("renamed: b.txt -> c.txt (100%)\n"));
    assert!(
        !text.contains("--- a/b.txt"),
        "an exact rename has no content diff"
    );
    assert!(text.contains("+unrelated"));

    // With renames off the delete and add are shown in full.
    let off = DiffArgs {
        no_renames: true,
        ..Default::default()
    };
    assert_eq!(off.threshold(), None);
    let (text, found) = working_diff(&ctx, off.threshold()).unwrap();
    assert!(found.is_empty() && !text.contains("renamed:"));
    assert!(text.contains("+++ /dev/null") && text.contains("--- /dev/null"));

    // A stricter threshold drops the edited one.
    let strict = DiffArgs {
        find_renames: Some(99),
        ..Default::default()
    };
    let (_, found) = working_diff(&ctx, strict.threshold()).unwrap();
    assert_eq!(found.len(), 1);
}

#[test]
fn diff_between_revisions_sends_rename_params() {
    use leep_cli::commands::diff::{diff_text, revisions_diff};

    let mock = Mock::start();
    let (wc_dir, cache) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let ctx = init_wc(&mock, wc_dir.path(), cache.path(), &[]).unwrap();
    let a = long_text("a");
    let first = mock.seed(None, &[("a.txt", &a, FileKind::Regular)], &[]);
    let edited = format!("{a}tail\n");
    let second = mock.seed(
        Some(first),
        &[("b.txt", &edited, FileKind::Regular)],
        &["a.txt"],
    );
    let (f, s) = (first.to_string(), second.to_string());
    let r = ctx.remote();

    let resp = revisions_diff(&r, &f, &s, Some(50)).unwrap();
    assert_eq!(resp.renames.len(), 1);
    let text = diff_text(&r, &resp).unwrap();
    assert!(text.starts_with("renamed: a.txt -> b.txt ("), "{text}");
    assert!(text.contains("+tail"));
    let q = queries(&mock, "diff").pop().unwrap();
    assert!(
        q.contains("renames=true") && q.contains("rename_threshold=50"),
        "{q}"
    );

    let resp = revisions_diff(&r, &f, &s, Some(70)).unwrap();
    assert!(queries(&mock, "diff")
        .pop()
        .unwrap()
        .contains("rename_threshold=70"));
    assert_eq!(resp.renames.len(), 1);

    // --no-renames: the request is the old one and the output the old one.
    let resp = revisions_diff(&r, &f, &s, None).unwrap();
    assert!(resp.renames.is_empty());
    assert!(!queries(&mock, "diff").pop().unwrap().contains("renames"));
    let text = diff_text(&r, &resp).unwrap();
    assert!(!text.contains("renamed:") && text.contains("+++ /dev/null"));
}

#[test]
fn diff_and_log_flags_parse() {
    use clap::Parser;
    use leep_cli::cli::{Cli, Command};

    let diff = |args: &[&str]| {
        let mut v = vec!["leep", "diff"];
        v.extend(args);
        Cli::try_parse_from(v).map(|c| match c.command {
            Command::Diff(d) => d.threshold(),
            _ => unreachable!(),
        })
    };
    assert_eq!(diff(&[]).unwrap(), Some(50));
    assert_eq!(diff(&["--no-renames"]).unwrap(), None);
    assert_eq!(diff(&["-M"]).unwrap(), Some(50));
    assert_eq!(diff(&["-M=70"]).unwrap(), Some(70));
    assert_eq!(diff(&["--find-renames=0"]).unwrap(), Some(0));
    assert_eq!(diff(&["--find-renames", "a", "b"]).unwrap(), Some(50));
    assert!(diff(&["-M=101"]).is_err());
    assert!(diff(&["-M=x"]).is_err());
    assert!(diff(&["--no-renames", "-M"]).is_err());

    let log = |args: &[&str]| {
        let mut v = vec!["leep", "log"];
        v.extend(args);
        Cli::try_parse_from(v)
    };
    assert!(log(&["--follow", "a.rs"]).is_ok());
    assert!(log(&["--follow", "a.rs", "--path", "b"]).is_err());
}

#[test]
fn log_follow_sends_the_flag_and_decodes_path_at() {
    let mock = Mock::start();
    let (wc_dir, cache) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let ctx = init_wc(&mock, wc_dir.path(), cache.path(), &[]).unwrap();
    mock.seed(None, &[("a.txt", "1\n", FileKind::Regular)], &[]);
    let r = ctx.remote();
    let id = r.resolve("main").unwrap();
    let args = LogArgs {
        rev: None,
        path: None,
        follow: Some("a.txt".into()),
        limit: 20,
        author: None,
        agent: false,
        agent_model: None,
        agent_session: None,
        extra: vec![],
    };
    let resp = r.client.log(&id, &args.query().unwrap()).unwrap();
    let q = queries(&mock, "log").pop().unwrap();
    assert!(q.contains("path=a.txt") && q.contains("follow=true"), "{q}");
    assert_eq!(resp.entries[0].path_at.as_ref().unwrap().as_str(), "a.txt");
}

fn branch_args(name: &str, rev: Option<&str>) -> BranchArgs {
    BranchArgs {
        name: Some(name.into()),
        rev: rev.map(Into::into),
        delete: false,
        force_delete: false,
    }
}

fn delete_args(name: &str, force: bool) -> BranchArgs {
    BranchArgs {
        name: Some(name.into()),
        rev: None,
        delete: !force,
        force_delete: force,
    }
}

fn switch_args(name: &str, create: bool, rev: Option<&str>) -> SwitchArgs {
    SwitchArgs {
        name: name.into(),
        rev: rev.map(Into::into),
        create,
    }
}

fn tag_args(name: &str, rev: Option<&str>, delete: bool) -> TagArgs {
    TagArgs {
        name: Some(name.into()),
        rev: rev.map(Into::into),
        delete,
    }
}

fn ref_of(mock: &Mock, name: &str) -> Option<leep_core::ChangesetId> {
    mock.data.lock().unwrap().refs.get(name).copied()
}

fn msg(e: leep_cli::error::CliError) -> String {
    assert_eq!(e.exit_code(), 1, "{e}");
    e.to_string()
}

/// The message of a not-found error (exit 4, ADR 0033).
fn missing(e: leep_cli::error::CliError) -> String {
    assert_eq!(e.exit_code(), 4, "{e}");
    e.to_string()
}

#[test]
fn branch_create_switch_land_and_delete() {
    let mock = Mock::start();
    let (wc_dir, cache) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let base = mock.seed(None, &[("a.txt", "a\n", FileKind::Regular)], &[]);
    let mut ctx = init_wc(&mock, wc_dir.path(), cache.path(), &[]).unwrap();
    assert_eq!(ctx.wc.state.base, Some(base));

    // Create at the base; a second create and bad names are refused.
    assert_eq!(
        branch::branch(&ctx, &branch_args("feature", None)).unwrap(),
        0
    );
    assert_eq!(ref_of(&mock, "feature"), Some(base));
    let e = msg(branch::branch(&ctx, &branch_args("feature", None)).unwrap_err());
    assert_eq!(e, "branch feature already exists");
    for bad in ["a..b", "x.lock", "with space"] {
        assert!(
            branch::branch(&ctx, &branch_args(bad, None)).is_err(),
            "{bad}"
        );
        assert_eq!(ref_of(&mock, bad), None);
    }
    let e = msg(branch::branch(&ctx, &branch_args("tags/x", None)).unwrap_err());
    assert!(e.contains("leep tag"), "{e}");
    let e = missing(branch::branch(&ctx, &branch_args("x", Some("nope"))).unwrap_err());
    assert!(e.contains("unknown revision"), "{e}");
    assert_eq!(
        branch::branch(
            &ctx,
            &BranchArgs {
                name: None,
                ..branch_args("", None)
            }
        )
        .unwrap(),
        0
    );

    // Switching needs a clean working copy and an existing branch.
    write(&ctx, "dirty.txt", "x\n");
    let e = msg(branch::switch(&mut ctx, &switch_args("feature", false, None)).unwrap_err());
    assert!(e.contains("uncommitted"), "{e}");
    fs::remove_file(ctx.wc.abs("dirty.txt")).unwrap();
    let e = missing(branch::switch(&mut ctx, &switch_args("nope", false, None)).unwrap_err());
    assert!(e.contains("does not exist"), "{e}");
    assert_eq!(ctx.wc.config.target_ref, "main");

    branch::switch(&mut ctx, &switch_args("feature", false, None)).unwrap();
    assert_eq!(ctx.wc.config.target_ref, "feature");
    assert_eq!(
        WorkCopy::open(wc_dir.path()).unwrap().config.target_ref,
        "feature",
        "persisted"
    );

    // Commit and land go to the branch; main stays put.
    write(&ctx, "a.txt", "on feature\n");
    commit::run(&mut ctx, &commit_args("work")).unwrap();
    let work = ctx.wc.state.base.unwrap();
    assert_eq!(
        land::run(&mut ctx, &LandArgs { target_ref: None }).unwrap(),
        0
    );
    assert_eq!(ref_of(&mock, "feature"), Some(work));
    assert_eq!(ref_of(&mock, "main"), Some(base));

    // Merged into the target: `-d` works. The target itself never deletes.
    branch::branch(&ctx, &branch_args("old", Some(&base.to_string()))).unwrap();
    assert_eq!(branch::branch(&ctx, &delete_args("old", false)).unwrap(), 0);
    assert_eq!(ref_of(&mock, "old"), None);
    let e = msg(branch::branch(&ctx, &delete_args("feature", true)).unwrap_err());
    assert!(e.contains("target"), "{e}");
    let e = missing(branch::branch(&ctx, &delete_args("gone", false)).unwrap_err());
    assert!(e.contains("does not exist"), "{e}");

    // Back on main, `feature` holds unmerged work: `-d` refuses, `-D` deletes.
    branch::switch(&mut ctx, &switch_args("main", false, None)).unwrap();
    assert_eq!(read(&ctx, "a.txt"), "a\n");
    let e = msg(branch::branch(&ctx, &delete_args("feature", false)).unwrap_err());
    assert!(e.contains("not fully merged") && e.contains("-D"), "{e}");
    assert_eq!(ref_of(&mock, "feature"), Some(work));
    assert_eq!(
        branch::branch(&ctx, &delete_args("feature", true)).unwrap(),
        0
    );
    assert_eq!(ref_of(&mock, "feature"), None);
    let e = msg(branch::branch(&ctx, &delete_args("main", true)).unwrap_err());
    assert!(e.contains("target"), "{e}");
}

#[test]
fn switch_create_starts_at_the_base_or_a_given_rev() {
    let mock = Mock::start();
    let (wc_dir, cache) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let first = mock.seed(None, &[("a.txt", "1\n", FileKind::Regular)], &[]);
    let mut ctx = init_wc(&mock, wc_dir.path(), cache.path(), &[]).unwrap();
    write(&ctx, "a.txt", "2\n");
    commit::run(&mut ctx, &commit_args("two")).unwrap();
    let second = ctx.wc.state.base.unwrap();

    // Default: at the current base, even though it is not landed anywhere.
    branch::switch(&mut ctx, &switch_args("topic", true, None)).unwrap();
    assert_eq!(ref_of(&mock, "topic"), Some(second));
    assert_eq!(ctx.wc.config.target_ref, "topic");
    assert_eq!(read(&ctx, "a.txt"), "2\n");

    // An explicit rev, here the head of main.
    branch::switch(&mut ctx, &switch_args("old", true, Some("main"))).unwrap();
    assert_eq!(ref_of(&mock, "old"), Some(first));
    assert_eq!(ctx.wc.state.base, Some(first));
    assert_eq!(read(&ctx, "a.txt"), "1\n");

    // Creating an existing branch fails before anything changes.
    let e = msg(branch::switch(&mut ctx, &switch_args("topic", true, None)).unwrap_err());
    assert_eq!(e, "branch topic already exists");
    assert_eq!(ctx.wc.config.target_ref, "old");
}

#[test]
fn tags_are_created_listed_resolved_and_deleted() {
    let mock = Mock::start();
    let (wc_dir, cache) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let first = mock.seed(None, &[("a.txt", "1\n", FileKind::Regular)], &[]);
    let mut ctx = init_wc(&mock, wc_dir.path(), cache.path(), &[]).unwrap();

    // The CLI adds the namespace; the default is the base.
    assert_eq!(branch::tag(&ctx, &tag_args("v1", None, false)).unwrap(), 0);
    assert_eq!(ref_of(&mock, "tags/v1"), Some(first));
    assert_eq!(ref_of(&mock, "v1"), None);
    let e = msg(branch::tag(&ctx, &tag_args("v1", None, false)).unwrap_err());
    assert!(e.contains("already exists") && e.contains("tag -d"), "{e}");
    assert!(branch::tag(&ctx, &tag_args("bad..name", None, false)).is_err());
    assert_eq!(
        branch::tag(
            &ctx,
            &TagArgs {
                name: None,
                rev: None,
                delete: false
            }
        )
        .unwrap(),
        0
    );

    // Tags show up under their full ref in `refs` but not as branches, and
    // resolve wherever a REV is accepted.
    write(&ctx, "a.txt", "2\n");
    commit::run(&mut ctx, &commit_args("two")).unwrap();
    assert_eq!(ctx.remote().resolve("tags/v1").unwrap(), first);
    branch::tag(&ctx, &tag_args("v2", Some("@"), false)).unwrap();
    assert_eq!(ref_of(&mock, "tags/v2"), ctx.wc.state.base);
    checkout(
        &mut ctx,
        &CheckoutArgs {
            rev: "tags/v1".into(),
            prefixes: vec![],
            force: false,
        },
    )
    .unwrap();
    assert_eq!(ctx.wc.state.base, Some(first));
    assert_eq!(read(&ctx, "a.txt"), "1\n");

    // Delete, then it can be created again somewhere else.
    assert_eq!(branch::tag(&ctx, &tag_args("v1", None, true)).unwrap(), 0);
    assert_eq!(ref_of(&mock, "tags/v1"), None);
    let e = missing(branch::tag(&ctx, &tag_args("v1", None, true)).unwrap_err());
    assert!(e.contains("does not exist"), "{e}");
    branch::tag(&ctx, &tag_args("v1", Some("tags/v2"), false)).unwrap();
    assert_eq!(ref_of(&mock, "tags/v1"), ref_of(&mock, "tags/v2"));

    // A tag is not a branch.
    let e = msg(branch::branch(&ctx, &delete_args("tags/v1", true)).unwrap_err());
    assert!(e.contains("leep tag -d v1"), "{e}");
    let e = msg(branch::switch(&mut ctx, &switch_args("tags/v1", false, None)).unwrap_err());
    assert!(e.contains("is a tag"), "{e}");
}

#[test]
fn branch_and_tag_need_a_base_or_a_rev() {
    let mock = Mock::start();
    let (wc_dir, cache) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let ctx = init_wc(&mock, wc_dir.path(), cache.path(), &[]).unwrap();
    assert!(ctx.wc.state.base.is_none());
    let e = msg(branch::branch(&ctx, &branch_args("x", None)).unwrap_err());
    assert!(e.contains("no base"), "{e}");
    let e = msg(branch::tag(&ctx, &tag_args("x", None, false)).unwrap_err());
    assert!(e.contains("no base"), "{e}");
    assert!(mock.data.lock().unwrap().refs.is_empty());
}

#[test]
fn a_failed_init_leaves_no_working_copy_and_can_be_retried() {
    let mock = Mock::start();
    let (wc_dir, cache) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let first = mock.seed(
        None,
        &[
            ("a.txt", "a\n", FileKind::Regular),
            ("keep.txt", "k\n", FileKind::Regular),
            ("z.txt", "z\n", FileKind::Regular),
        ],
        &[],
    );
    // Already in the directory before init, with the content the ref has.
    fs::write(wc_dir.path().join("keep.txt"), "k\n").unwrap();
    // The checkout fails part way: the last blob comes back with the wrong
    // bytes, after a.txt has been written.
    let bad = Hash::of_blob(b"z\n");
    mock.data.lock().unwrap().corrupt.insert(bad);
    assert!(init_wc(&mock, wc_dir.path(), cache.path(), &[]).is_err());
    assert!(
        !wc_dir.path().join(".leep").exists(),
        "a failed init must not leave a working copy behind"
    );
    assert!(
        !wc_dir.path().join("a.txt").exists(),
        "files the failed init wrote are removed"
    );
    assert_eq!(
        fs::read_to_string(wc_dir.path().join("keep.txt")).unwrap(),
        "k\n",
        "a file that was there before init is left alone"
    );

    // Meanwhile the ref moves, changing a file the failed attempt had
    // written; the retry must still succeed.
    mock.seed(Some(first), &[("a.txt", "a2\n", FileKind::Regular)], &[]);
    mock.data.lock().unwrap().corrupt.remove(&bad);
    let ctx = init_wc(&mock, wc_dir.path(), cache.path(), &[]).unwrap();
    assert_eq!(fs::read_to_string(ctx.wc.abs("a.txt")).unwrap(), "a2\n");
    assert_eq!(fs::read_to_string(ctx.wc.abs("z.txt")).unwrap(), "z\n");
    assert!(status(&ctx.wc).unwrap().is_clean());
}
