use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use leep_core::api::{Authorship, LandResponse, MergeRequest, MergeResponse, RebaseResponse};
use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
use leep_core::{
    Changeset, ChangesetId, Error, FileChange, FileKind, Hash, RepoPath, TreeEntry, TreeNode,
};

use crate::Repo;

fn repo() -> Repo {
    Repo::new(
        Arc::new(MemoryBlobStore::new()),
        Arc::new(MemoryMetaStore::new()),
    )
}

fn cas_ctx() -> leep_core::RefUpdateContext {
    leep_core::RefUpdateContext::new("cas")
}

fn rp(s: &str) -> RepoPath {
    RepoPath::new(s).unwrap()
}

async fn modify(r: &Repo, data: &[u8]) -> FileChange {
    let content = r.blobs().put(Bytes::copy_from_slice(data)).await.unwrap();
    FileChange::Modify {
        content,
        kind: FileKind::Regular,
        size: data.len() as u64,
    }
}

fn changeset(
    parents: &[ChangesetId],
    changes: BTreeMap<RepoPath, FileChange>,
    msg: &str,
) -> Changeset {
    Changeset {
        parents: parents.to_vec(),
        author: "t".into(),
        timestamp: 1,
        message: msg.into(),
        changes,
        extra: BTreeMap::new(),
    }
}

/// `Some(text)` writes a file, `None` deletes it.
async fn try_commit(
    r: &Repo,
    parents: &[ChangesetId],
    files: &[(&str, Option<&str>)],
    msg: &str,
) -> leep_core::Result<ChangesetId> {
    let mut changes = BTreeMap::new();
    for (p, c) in files {
        let ch = match c {
            Some(t) => modify(r, t.as_bytes()).await,
            None => FileChange::Delete,
        };
        changes.insert(rp(p), ch);
    }
    r.create_changeset(changeset(parents, changes, msg)).await
}

async fn commit(
    r: &Repo,
    parents: &[ChangesetId],
    files: &[(&str, Option<&str>)],
    msg: &str,
) -> ChangesetId {
    try_commit(r, parents, files, msg).await.unwrap()
}

async fn text(r: &Repo, id: ChangesetId, path: &str) -> Option<String> {
    r.read_file(id, &rp(path))
        .await
        .unwrap()
        .map(|(_, b)| String::from_utf8(b.to_vec()).unwrap())
}

fn is_invalid<T: std::fmt::Debug>(r: leep_core::Result<T>) -> bool {
    matches!(r, Err(Error::Invalid(_)))
}

fn merge_req(ours: ChangesetId, theirs: ChangesetId) -> MergeRequest {
    MergeRequest {
        parents: [ours, theirs],
        message: "merge".into(),
        authorship: Authorship {
            author: "m".into(),
            timestamp: Some(5),
        },
        extra: BTreeMap::new(),
    }
}

#[tokio::test]
async fn tree_nested_dirs_deletes_and_pruning() {
    let r = repo();
    let c1 = commit(
        &r,
        &[],
        &[
            ("top.txt", Some("t")),
            ("a/b/c.txt", Some("c")),
            ("a/x.txt", Some("x")),
            ("z/y", Some("y")),
        ],
        "c1",
    )
    .await;
    let files = r.list_files(c1, "").await.unwrap();
    let paths: Vec<_> = files.iter().map(|f| f.path.as_str()).collect();
    assert_eq!(paths, ["a/b/c.txt", "a/x.txt", "top.txt", "z/y"]);
    assert_eq!(r.list_files(c1, "a").await.unwrap().len(), 2);
    assert_eq!(r.list_files(c1, "a/b/c.txt").await.unwrap().len(), 1);
    assert!(r.list_files(c1, "nope").await.unwrap().is_empty());

    let root = r.list_dir(c1, "").await.unwrap();
    assert_eq!(root.entries.len(), 3);
    assert!(matches!(root.entries["a"], TreeEntry::Dir { .. }));
    let a = r.list_dir(c1, "a").await.unwrap();
    assert_eq!(a.path, "a");
    assert!(matches!(
        r.list_dir(c1, "top.txt").await,
        Err(Error::NotFound(_))
    ));
    assert!(matches!(
        r.list_dir(c1, "missing").await,
        Err(Error::NotFound(_))
    ));

    // Only directories on the changed path are rewritten.
    let c2 = commit(&r, &[c1], &[("z/y", Some("y2"))], "c2").await;
    let (t1, t2) = (
        r.list_dir(c1, "").await.unwrap(),
        r.list_dir(c2, "").await.unwrap(),
    );
    assert_eq!(t1.entries["a"], t2.entries["a"]);
    assert_ne!(t1.entries["z"], t2.entries["z"]);

    // Deleting the last file in a directory prunes it (and its parents).
    let c3 = commit(&r, &[c2], &[("a/b/c.txt", None)], "c3").await;
    let a = r.list_dir(c3, "a").await.unwrap();
    assert_eq!(a.entries.keys().collect::<Vec<_>>(), ["x.txt"]);
    let c4 = commit(&r, &[c3], &[("a/x.txt", None)], "c4").await;
    assert!(!r.list_dir(c4, "").await.unwrap().entries.contains_key("a"));

    // Deleting everything yields the empty tree, which is readable.
    let c5 = commit(&r, &[c4], &[("top.txt", None), ("z/y", None)], "c5").await;
    assert_eq!(r.root_tree(c5).await.unwrap(), TreeNode::empty_hash());
    assert_eq!(
        r.read_tree(TreeNode::empty_hash()).await.unwrap(),
        TreeNode::default()
    );
    assert_eq!(r.generation(c5).await.unwrap(), 5);
    assert_eq!(r.generation(c1).await.unwrap(), 1);
}

#[tokio::test]
async fn empty_root_changeset_stores_empty_tree() {
    let r = repo();
    let c = commit(&r, &[], &[], "empty").await;
    assert_eq!(r.root_tree(c).await.unwrap(), TreeNode::empty_hash());
    assert!(r.blobs().contains(&TreeNode::empty_hash()).await.unwrap());
}

/// Security review 2026-10, S-04: tree building is quadratic in path depth,
/// so client paths are bounded before anything is built.
#[tokio::test]
async fn create_changeset_bounds_path_length_and_directories() {
    let r = repo();
    // Deep is fine as long as the path fits.
    let deepest = vec!["d"; crate::repo::MAX_PATH_BYTES / 2].join("/");
    let c = commit(&r, &[], &[(deepest.as_str(), Some("ok"))], "deep").await;
    assert_eq!(text(&r, c, &deepest).await.as_deref(), Some("ok"));

    let too_long = format!("a/{}", "b".repeat(crate::repo::MAX_PATH_BYTES));
    let e = try_commit(&r, &[], &[(too_long.as_str(), Some("x"))], "m").await;
    assert!(is_invalid(e), "longer than the limit");

    // Paths that each stay within the limits but together name too much
    // directory text: about 520 KiB of prefixes per path.
    let blob = modify(&r, b"x").await;
    let deep_dirs = |n: usize| -> BTreeMap<RepoPath, FileChange> {
        (0..n)
            .map(|i| {
                let p = format!("{i:04}/{}/f", vec!["c".repeat(15); 250].join("/"));
                (rp(&p), blob.clone())
            })
            .collect()
    };
    r.create_changeset(changeset(&[], deep_dirs(8), "fine"))
        .await
        .unwrap();
    let e = r
        .create_changeset(changeset(&[], deep_dirs(600), "m"))
        .await;
    assert!(is_invalid(e), "too much directory text in one changeset");
}

#[tokio::test]
async fn create_changeset_validation() {
    let r = repo();
    let c1 = commit(&r, &[], &[("a/b", Some("x")), ("f", Some("y"))], "c1").await;

    // Idempotent.
    let again = commit(&r, &[], &[("a/b", Some("x")), ("f", Some("y"))], "c1").await;
    assert_eq!(again, c1);

    let bogus = ChangesetId(Hash::of_blob(b"nope"));
    assert!(is_invalid(try_commit(&r, &[bogus], &[], "m").await));

    let missing = FileChange::Modify {
        content: Hash::of_blob(b"unstored"),
        kind: FileKind::Regular,
        size: 8,
    };
    let cs = changeset(&[c1], BTreeMap::from([(rp("q"), missing)]), "m");
    assert!(is_invalid(r.create_changeset(cs).await));

    let content = r.blobs().put(Bytes::from_static(b"abc")).await.unwrap();
    let wrong = FileChange::Modify {
        content,
        kind: FileKind::Regular,
        size: 99,
    };
    let cs = changeset(&[c1], BTreeMap::from([(rp("q"), wrong)]), "m");
    assert!(is_invalid(r.create_changeset(cs).await));

    // Delete of an absent path, or of a directory.
    assert!(is_invalid(
        try_commit(&r, &[c1], &[("nope", None)], "m").await
    ));
    assert!(is_invalid(try_commit(&r, &[c1], &[("a", None)], "m").await));
    assert!(is_invalid(
        try_commit(&r, &[], &[("nope", None)], "m").await
    ));

    // File <-> directory clashes.
    assert!(is_invalid(
        try_commit(&r, &[c1], &[("f/g", Some("z"))], "m").await
    ));
    assert!(is_invalid(
        try_commit(&r, &[c1], &[("a", Some("z"))], "m").await
    ));
    assert!(is_invalid(
        try_commit(&r, &[], &[("p", Some("1")), ("p/q", Some("2"))], "m").await
    ));

    // Replacing a file with a directory (and back) is fine when explicit.
    let c2 = commit(&r, &[c1], &[("f", None), ("f/g", Some("z"))], "file->dir").await;
    assert_eq!(text(&r, c2, "f/g").await.as_deref(), Some("z"));
    assert!(text(&r, c2, "f").await.is_none());
    let c3 = commit(
        &r,
        &[c2],
        &[("f/g", None), ("f", Some("again"))],
        "dir->file",
    )
    .await;
    assert_eq!(text(&r, c3, "f").await.as_deref(), Some("again"));
    let c4 = commit(
        &r,
        &[c1],
        &[("a/b", None), ("a", Some("flat"))],
        "dir->file",
    )
    .await;
    assert_eq!(text(&r, c4, "a").await.as_deref(), Some("flat"));
}

#[tokio::test]
async fn info_resolve_and_lookup() {
    let r = repo();
    let c1 = commit(&r, &[], &[("a.txt", Some("hello"))], "c1").await;
    let info = r.info(c1).await.unwrap();
    assert_eq!(info.id, c1);
    assert_eq!(info.generation, 1);
    assert_eq!(info.root_tree, r.root_tree(c1).await.unwrap());
    assert!(matches!(
        r.get_changeset(ChangesetId(Hash::of_blob(b"x"))).await,
        Err(Error::NotFound(_))
    ));

    assert!(r.meta().cas_ref("main", None, Some(c1)).await.unwrap());
    assert_eq!(r.resolve("main").await.unwrap(), c1);
    assert_eq!(r.resolve(&c1.to_string()).await.unwrap(), c1);
    assert!(matches!(r.resolve("nope").await, Err(Error::NotFound(_))));
    let absent = ChangesetId(Hash::of_blob(b"x")).to_string();
    assert!(matches!(r.resolve(&absent).await, Err(Error::NotFound(_))));

    let f = r.lookup_file(c1, &rp("a.txt")).await.unwrap().unwrap();
    assert_eq!(f.size, 5);
    assert!(r.lookup_file(c1, &rp("b.txt")).await.unwrap().is_none());
    assert!(r.lookup_file(c1, &rp("a.txt/x")).await.unwrap().is_none());
    assert_eq!(text(&r, c1, "a.txt").await.as_deref(), Some("hello"));
}

#[tokio::test]
async fn diff_walks_changed_subtrees() {
    let r = repo();
    let c1 = commit(
        &r,
        &[],
        &[
            ("a/1", Some("1")),
            ("a/2", Some("2")),
            ("b/c/3", Some("3")),
            ("top", Some("t")),
        ],
        "c1",
    )
    .await;
    let c2 = commit(
        &r,
        &[c1],
        &[
            ("a/2", Some("2b")),
            ("a/new", Some("n")),
            ("b/c/3", None),
            ("top", None),
        ],
        "c2",
    )
    .await;

    let d = r.diff(Some(c1), c2, "").await.unwrap();
    let paths: Vec<_> = d.iter().map(|e| e.path.as_str()).collect();
    assert_eq!(paths, ["a/2", "a/new", "b/c/3", "top"]);
    assert!(d[0].old.is_some() && d[0].new.is_some());
    assert!(d[1].old.is_none() && d[1].new.is_some());
    assert!(d[2].old.is_some() && d[2].new.is_none());

    let d = r.diff(Some(c1), c2, "a").await.unwrap();
    assert_eq!(d.len(), 2);
    let d = r.diff(Some(c1), c2, "a/2").await.unwrap();
    assert_eq!(d.len(), 1);
    assert!(r.diff(Some(c1), c1, "").await.unwrap().is_empty());

    // from = None diffs against the empty tree.
    let d = r.diff(None, c1, "").await.unwrap();
    assert_eq!(d.len(), 4);
    assert!(d.iter().all(|e| e.old.is_none() && e.new.is_some()));

    // File <-> directory replacement shows both sides.
    let c3 = commit(
        &r,
        &[c2],
        &[
            ("a/1", None),
            ("a/2", None),
            ("a/new", None),
            ("a", Some("flat")),
        ],
        "c3",
    )
    .await;
    let d = r.diff(Some(c2), c3, "").await.unwrap();
    let got: Vec<_> = d
        .iter()
        .map(|e| (e.path.as_str(), e.old.is_some(), e.new.is_some()))
        .collect();
    assert_eq!(
        got,
        [
            ("a", false, true),
            ("a/1", true, false),
            ("a/2", true, false),
            ("a/new", true, false)
        ]
    );
}

#[tokio::test]
async fn log_with_path_filter() {
    let r = repo();
    let c1 = commit(
        &r,
        &[],
        &[("src/a.rs", Some("1")), ("doc/x", Some("1"))],
        "c1",
    )
    .await;
    let c2 = commit(&r, &[c1], &[("doc/x", Some("2"))], "c2").await;
    let c3 = commit(&r, &[c2], &[("src/a.rs", Some("2"))], "c3").await;
    let c4 = commit(&r, &[c3], &[("src/b.rs", Some("1"))], "c4").await;

    let ids =
        |v: Vec<leep_core::api::ChangesetInfo>| v.into_iter().map(|i| i.id).collect::<Vec<_>>();
    assert_eq!(ids(r.log(c4, None, 100).await.unwrap()), [c4, c3, c2, c1]);
    assert_eq!(ids(r.log(c4, None, 2).await.unwrap()), [c4, c3]);
    assert_eq!(
        ids(r.log(c4, Some("src"), 100).await.unwrap()),
        [c4, c3, c1]
    );
    assert_eq!(
        ids(r.log(c4, Some("src/a.rs"), 100).await.unwrap()),
        [c3, c1]
    );
    assert_eq!(ids(r.log(c4, Some("doc"), 100).await.unwrap()), [c2, c1]);
    assert_eq!(ids(r.log(c4, Some("doc/x"), 1).await.unwrap()), [c2]);
    assert!(r.log(c4, Some("nothing"), 100).await.unwrap().is_empty());
    let first = &r.log(c4, None, 1).await.unwrap()[0];
    assert_eq!(first.generation, 4);
}

#[tokio::test]
async fn log_follows_all_parents_of_merges() {
    let r = repo();
    let base = commit(&r, &[], &[("a", Some("1"))], "base").await;
    let l = commit(&r, &[base], &[("l", Some("1"))], "l").await;
    let rr = commit(&r, &[base], &[("r", Some("1"))], "r").await;
    let MergeResponse::Merged { id: m } = r.merge(merge_req(l, rr)).await.unwrap() else {
        panic!()
    };
    let log = r.log(m, None, 100).await.unwrap();
    assert_eq!(log.len(), 4);
    assert_eq!(log[0].id, m);
    assert_eq!(log[3].id, base);
    // The merge changed only what theirs added, relative to ours.
    let changed = r.log(m, Some("r"), 100).await.unwrap();
    assert_eq!(changed.iter().map(|i| i.id).collect::<Vec<_>>(), [m, rr]);
}

#[tokio::test]
async fn ancestry_and_merge_base_in_dags() {
    let r = repo();
    let root = commit(&r, &[], &[("a", Some("0"))], "root").await;
    let mut main = root;
    let mut main_ids = vec![root];
    for i in 0..40 {
        main = commit(
            &r,
            &[main],
            &[("a", Some(&format!("m{i}")))],
            &format!("m{i}"),
        )
        .await;
        main_ids.push(main);
    }
    // Branch from main_ids[10].
    let mut side = main_ids[10];
    for i in 0..5 {
        side = commit(
            &r,
            &[side],
            &[("s", Some(&format!("s{i}")))],
            &format!("s{i}"),
        )
        .await;
    }
    assert!(r.is_ancestor(root, main).await.unwrap());
    assert!(r.is_ancestor(main, main).await.unwrap());
    assert!(r.is_ancestor(main_ids[10], side).await.unwrap());
    assert!(r.is_ancestor(main_ids[7], main).await.unwrap());
    assert!(!r.is_ancestor(main, root).await.unwrap());
    assert!(!r.is_ancestor(side, main).await.unwrap());
    assert!(!r.is_ancestor(main_ids[11], side).await.unwrap());
    assert_eq!(r.merge_base(main, side).await.unwrap(), Some(main_ids[10]));
    assert_eq!(r.merge_base(side, main).await.unwrap(), Some(main_ids[10]));
    assert_eq!(
        r.merge_base(main, main_ids[3]).await.unwrap(),
        Some(main_ids[3])
    );
    assert_eq!(r.merge_base(side, side).await.unwrap(), Some(side));

    // Ancestry through a second parent only.
    let other = commit(&r, &[], &[("x", Some("1"))], "other root").await;
    let MergeResponse::Merged { id: m } = r.merge(merge_req(main, other)).await.unwrap() else {
        panic!()
    };
    assert!(r.is_ancestor(other, m).await.unwrap());
    assert!(r.is_ancestor(main_ids[20], m).await.unwrap());
    assert_eq!(r.merge_base(other, main).await.unwrap(), None);
    assert_eq!(r.merge_base(m, other).await.unwrap(), Some(other));

    // Criss-cross: two merges of the same pair; the best base is one of them
    // (never the older shared ancestors).
    let a = commit(&r, &[root], &[("a1", Some("1"))], "a").await;
    let b = commit(&r, &[root], &[("b1", Some("1"))], "b").await;
    let MergeResponse::Merged { id: m1 } = r.merge(merge_req(a, b)).await.unwrap() else {
        panic!()
    };
    let MergeResponse::Merged { id: m2 } = r
        .merge(MergeRequest {
            message: "other".into(),
            ..merge_req(b, a)
        })
        .await
        .unwrap()
    else {
        panic!()
    };
    let x = commit(&r, &[m1], &[("x1", Some("1"))], "x").await;
    let y = commit(&r, &[m2], &[("y1", Some("1"))], "y").await;
    let base = r.merge_base(x, y).await.unwrap().unwrap();
    assert!(base == m1 || base == m2 || base == a || base == b);
    assert_ne!(base, root);
    assert_eq!(r.generation(m1).await.unwrap(), 3);
}

#[tokio::test]
async fn merge_clean_and_trees() {
    let r = repo();
    let base = commit(
        &r,
        &[],
        &[
            ("a.txt", Some("a\n")),
            ("b.txt", Some("b\n")),
            ("del.txt", Some("d\n")),
        ],
        "base",
    )
    .await;
    let ours = commit(
        &r,
        &[base],
        &[("a.txt", Some("a2\n")), ("o.txt", Some("o\n"))],
        "ours",
    )
    .await;
    let theirs = commit(
        &r,
        &[base],
        &[
            ("b.txt", Some("b2\n")),
            ("t/n.txt", Some("n\n")),
            ("del.txt", None),
        ],
        "theirs",
    )
    .await;
    let MergeResponse::Merged { id } = r.merge(merge_req(ours, theirs)).await.unwrap() else {
        panic!()
    };
    let cs = r.get_changeset(id).await.unwrap();
    assert_eq!(cs.parents, [ours, theirs]);
    assert_eq!(cs.timestamp, 5);
    assert_eq!(cs.author, "m");
    // Recorded relative to ours: only theirs-side changes.
    let keys: Vec<_> = cs.changes.keys().map(|k| k.as_str()).collect();
    assert_eq!(keys, ["b.txt", "del.txt", "t/n.txt"]);
    assert_eq!(text(&r, id, "a.txt").await.as_deref(), Some("a2\n"));
    assert_eq!(text(&r, id, "b.txt").await.as_deref(), Some("b2\n"));
    assert_eq!(text(&r, id, "o.txt").await.as_deref(), Some("o\n"));
    assert!(text(&r, id, "del.txt").await.is_none());
    assert_eq!(text(&r, id, "t/n.txt").await.as_deref(), Some("n\n"));
    assert_eq!(r.generation(id).await.unwrap(), 3);

    // Both sides made the identical change: no conflict.
    let o2 = commit(&r, &[base], &[("a.txt", Some("same\n"))], "o2").await;
    let t2 = commit(
        &r,
        &[base],
        &[("a.txt", Some("same\n")), ("x", Some("x"))],
        "t2",
    )
    .await;
    let MergeResponse::Merged { id } = r.merge(merge_req(o2, t2)).await.unwrap() else {
        panic!()
    };
    assert_eq!(text(&r, id, "a.txt").await.as_deref(), Some("same\n"));
    // Timestamp defaults to the clock.
    let mut req = merge_req(o2, t2);
    req.authorship.timestamp = None;
    req.message = "clock".into();
    let MergeResponse::Merged { id } = r.merge(req).await.unwrap() else {
        panic!()
    };
    assert!(r.get_changeset(id).await.unwrap().timestamp > 1_600_000_000);
}

#[tokio::test]
async fn merge_text_merges_and_conflicts() {
    let r = repo();
    let body = "one\ntwo\nthree\nfour\nfive\nsix\nseven\neight\n";
    let base = commit(
        &r,
        &[],
        &[
            ("f", Some(body)),
            ("clash", Some("x\n")),
            ("bin", Some("zzz")),
        ],
        "base",
    )
    .await;
    let ours = commit(
        &r,
        &[base],
        &[
            ("f", Some(&body.replace("one", "ONE"))),
            ("clash", Some("ours\n")),
            ("bin", Some("ours")),
        ],
        "ours",
    )
    .await;
    let theirs = commit(
        &r,
        &[base],
        &[
            ("f", Some(&body.replace("eight", "EIGHT"))),
            ("clash", Some("theirs\n")),
            ("bin", Some("theirs")),
        ],
        "theirs",
    )
    .await;
    let MergeResponse::Conflicts { conflicts, clean } =
        r.merge(merge_req(ours, theirs)).await.unwrap()
    else {
        panic!()
    };
    // f merged cleanly, so only the two clashing files conflict.
    let paths: Vec<_> = conflicts.iter().map(|c| c.path.as_str()).collect();
    assert_eq!(paths, ["bin", "clash"]);
    // The clean part is relative to ours and holds f's merged content.
    assert_eq!(clean.keys().map(|p| p.as_str()).collect::<Vec<_>>(), ["f"]);
    let FileChange::Modify { content, .. } = clean[&rp("f")] else {
        panic!("f should be modified")
    };
    let merged =
        String::from_utf8(r.blobs().get(&content).await.unwrap().unwrap().to_vec()).unwrap();
    assert!(merged.starts_with("ONE\n") && merged.ends_with("EIGHT\n"));
    let clash = &conflicts[1];
    let marked = clash.merged_with_markers.as_deref().unwrap();
    assert!(marked.contains("<<<<<<<") && marked.contains("=======") && marked.contains(">>>>>>>"));
    assert!(marked.contains("ours") && marked.contains("theirs"));
    assert!(clash.base.is_some() && clash.ours.is_some() && clash.theirs.is_some());

    // Resolve the clashes on one side; the text merge result is correct.
    let ours2 = commit(
        &r,
        &[ours],
        &[("clash", Some("x\n")), ("bin", Some("zzz"))],
        "ours2",
    )
    .await;
    let MergeResponse::Merged { id } = r.merge(merge_req(ours2, theirs)).await.unwrap() else {
        panic!("expected a clean merge")
    };
    let merged = text(&r, id, "f").await.unwrap();
    assert!(merged.starts_with("ONE\n") && merged.ends_with("EIGHT\n"));
    assert_eq!(text(&r, id, "clash").await.as_deref(), Some("theirs\n"));
}

#[tokio::test]
async fn merge_binary_and_delete_conflicts() {
    let r = repo();
    let blob = |b: &[u8]| {
        let b = b.to_vec();
        let r = r.clone();
        async move { modify(&r, &b).await }
    };
    let base = r
        .create_changeset(changeset(
            &[],
            BTreeMap::from([
                (rp("bin"), blob(&[0, 159, 146, 150]).await),
                (rp("dm"), blob(b"x\n").await),
            ]),
            "base",
        ))
        .await
        .unwrap();
    let ours = r
        .create_changeset(changeset(
            &[base],
            BTreeMap::from([
                (rp("bin"), blob(&[1, 159, 146]).await),
                (rp("dm"), FileChange::Delete),
            ]),
            "ours",
        ))
        .await
        .unwrap();
    let theirs = r
        .create_changeset(changeset(
            &[base],
            BTreeMap::from([
                (rp("bin"), blob(&[2, 159, 146]).await),
                (rp("dm"), blob(b"y\n").await),
            ]),
            "theirs",
        ))
        .await
        .unwrap();
    let MergeResponse::Conflicts { conflicts, .. } =
        r.merge(merge_req(ours, theirs)).await.unwrap()
    else {
        panic!()
    };
    assert_eq!(conflicts.len(), 2);
    for c in &conflicts {
        assert!(c.merged_with_markers.is_none(), "{}", c.path);
    }
    assert!(conflicts[1].ours.is_none() && conflicts[1].theirs.is_some());
}

#[tokio::test]
async fn merge_file_dir_clash_is_a_conflict() {
    let r = repo();
    let base = commit(&r, &[], &[("keep", Some("k"))], "base").await;
    let ours = commit(&r, &[base], &[("a", Some("file"))], "ours").await;
    let theirs = commit(&r, &[base], &[("a/b", Some("nested"))], "theirs").await;
    let MergeResponse::Conflicts { conflicts, .. } =
        r.merge(merge_req(ours, theirs)).await.unwrap()
    else {
        panic!()
    };
    assert_eq!(conflicts[0].path.as_str(), "a/b");
}

#[tokio::test]
async fn merge_conflicts_carry_added_and_deleted_paths_in_clean() {
    let r = repo();
    let base = commit(
        &r,
        &[],
        &[
            ("clash", Some("x\n")),
            ("gone", Some("g\n")),
            ("same", Some("s\n")),
        ],
        "base",
    )
    .await;
    let ours = commit(
        &r,
        &[base],
        &[("clash", Some("ours\n")), ("only-ours", Some("o\n"))],
        "ours",
    )
    .await;
    let theirs = commit(
        &r,
        &[base],
        &[
            ("clash", Some("theirs\n")),
            ("gone", None),
            ("added", Some("a\n")),
        ],
        "theirs",
    )
    .await;
    let MergeResponse::Conflicts { conflicts, clean } =
        r.merge(merge_req(ours, theirs)).await.unwrap()
    else {
        panic!("expected conflicts")
    };
    assert_eq!(conflicts.len(), 1);
    let keys: Vec<_> = clean.keys().map(|p| p.as_str()).collect();
    // Only what theirs changes and ours lacks: nothing for ours' own edits.
    assert_eq!(keys, ["added", "gone"]);
    assert_eq!(clean[&rp("gone")], FileChange::Delete);
    assert!(matches!(clean[&rp("added")], FileChange::Modify { .. }));
}

/// `leep revert` stores the inverse of a change as a changeset on top of the
/// reverted one and rebases it onto the base. The first parent is the merge
/// base, so edits made since to other lines of the file survive.
#[tokio::test]
async fn rebasing_an_inverse_changeset_reverts_only_its_hunk() {
    let r = repo();
    let lines = |edits: &[(usize, &str)]| -> String {
        (1..=12)
            .map(|n| match edits.iter().find(|(i, _)| *i == n) {
                Some((_, t)) => format!("{t}\n"),
                None => format!("line {n}\n"),
            })
            .collect()
    };
    let c0 = commit(
        &r,
        &[],
        &[("f", Some(&lines(&[]))), ("g", Some("g\n"))],
        "c0",
    )
    .await;
    let reverted = commit(&r, &[c0], &[("f", Some(&lines(&[(2, "TWO")])))], "edit 2").await;
    let later = commit(
        &r,
        &[reverted],
        &[("f", Some(&lines(&[(2, "TWO"), (11, "ELEVEN")])))],
        "edit 11",
    )
    .await;
    // The inverse: what `f` was in the reverted changeset's parent.
    let inverse = commit(&r, &[reverted], &[("f", Some(&lines(&[])))], "revert").await;
    let RebaseResponse::Rebased { id } = r.rebase(inverse, later).await.unwrap() else {
        panic!("expected a clean rebase")
    };
    assert_eq!(
        text(&r, id, "f").await.unwrap(),
        lines(&[(11, "ELEVEN")]),
        "line 2 is back, line 11 keeps its later edit"
    );
    assert_eq!(text(&r, id, "g").await.as_deref(), Some("g\n"));

    // An edit of the same line since is a conflict, never a silent pick.
    let clash = commit(
        &r,
        &[reverted],
        &[("f", Some(&lines(&[(2, "TWO AGAIN")])))],
        "edit 2 again",
    )
    .await;
    let RebaseResponse::Conflicts { conflicts, clean } = r.rebase(inverse, clash).await.unwrap()
    else {
        panic!("expected a conflict")
    };
    assert_eq!(conflicts.len(), 1);
    assert!(clean.is_empty());
    assert!(conflicts[0].merged_with_markers.is_some());
}

#[tokio::test]
async fn rebase_replays_changes() {
    let r = repo();
    let base = commit(&r, &[], &[("a", Some("a\n")), ("b", Some("b\n"))], "base").await;
    let onto = commit(&r, &[base], &[("a", Some("a2\n"))], "onto").await;
    let mut cs = changeset(
        &[base],
        BTreeMap::from([
            (rp("c"), modify(&r, b"c\n").await),
            (rp("b"), FileChange::Delete),
        ]),
        "feature",
    );
    cs.timestamp = 77;
    cs.extra.insert("agent.model".into(), "m".into());
    let feature = r.create_changeset(cs).await.unwrap();

    let RebaseResponse::Rebased { id } = r.rebase(feature, onto).await.unwrap() else {
        panic!()
    };
    let new = r.get_changeset(id).await.unwrap();
    assert_eq!(new.parents, [onto]);
    assert_eq!(
        (new.author.as_str(), new.timestamp, new.message.as_str()),
        ("t", 77, "feature")
    );
    assert_eq!(new.extra["agent.model"], "m");
    assert_eq!(new.extra["leep.rebased_from"], feature.to_string());
    assert_eq!(text(&r, id, "a").await.as_deref(), Some("a2\n"));
    assert_eq!(text(&r, id, "c").await.as_deref(), Some("c\n"));
    assert!(text(&r, id, "b").await.is_none());

    // Already on `onto`: returned unchanged.
    assert_eq!(
        r.rebase(feature, base).await.unwrap(),
        RebaseResponse::Rebased { id: feature }
    );

    // Conflict.
    let clash = commit(&r, &[base], &[("a", Some("mine\n"))], "clash").await;
    let RebaseResponse::Conflicts { conflicts, .. } = r.rebase(clash, onto).await.unwrap() else {
        panic!()
    };
    assert_eq!(conflicts[0].path.as_str(), "a");

    // Merge changesets cannot be rebased.
    let MergeResponse::Merged { id: m } = r.merge(merge_req(onto, feature)).await.unwrap() else {
        panic!()
    };
    assert!(is_invalid(r.rebase(m, base).await));
}

#[tokio::test]
async fn land_basic_flow() {
    let r = repo();
    let c1 = commit(&r, &[], &[("a", Some("1\n"))], "c1").await;
    // Empty ref: lands as is.
    let resp = r.land(c1, "main").await.unwrap();
    assert_eq!(
        resp,
        LandResponse::Landed {
            landed: c1,
            rebased: false,
            attempts: 1
        }
    );
    assert_eq!(r.resolve("main").await.unwrap(), c1);
    // Fast-forward.
    let c2 = commit(&r, &[c1], &[("b", Some("2\n"))], "c2").await;
    let resp = r.land(c2, "main").await.unwrap();
    assert_eq!(
        resp,
        LandResponse::Landed {
            landed: c2,
            rebased: false,
            attempts: 1
        }
    );
    // Already landed.
    let LandResponse::Landed {
        landed, rebased, ..
    } = r.land(c1, "main").await.unwrap()
    else {
        panic!()
    };
    assert_eq!((landed, rebased), (c1, false));
    assert_eq!(r.resolve("main").await.unwrap(), c2);
    // Stale base: rebased.
    let side = commit(&r, &[c1], &[("c", Some("3\n"))], "side").await;
    let LandResponse::Landed {
        landed, rebased, ..
    } = r.land(side, "main").await.unwrap()
    else {
        panic!()
    };
    assert!(rebased);
    assert_eq!(r.resolve("main").await.unwrap(), landed);
    assert_eq!(r.get_changeset(landed).await.unwrap().parents, [c2]);
    assert_eq!(r.list_files(landed, "").await.unwrap().len(), 3);
    // Conflicts are reported and the ref does not move.
    let bad = commit(&r, &[c1], &[("a", Some("other\n"))], "bad").await;
    let c3 = commit(&r, &[landed], &[("a", Some("changed\n"))], "c3").await;
    r.land(c3, "main").await.unwrap();
    let LandResponse::Conflicts {
        onto, conflicts, ..
    } = r.land(bad, "main").await.unwrap()
    else {
        panic!()
    };
    assert_eq!(onto, c3);
    assert_eq!(conflicts[0].path.as_str(), "a");
    assert_eq!(r.resolve("main").await.unwrap(), c3);
}

#[tokio::test]
async fn land_fast_forwards_a_stack() {
    let r = repo();
    let c1 = commit(&r, &[], &[("a", Some("1\n"))], "c1").await;
    r.land(c1, "main").await.unwrap();
    let s1 = commit(&r, &[c1], &[("b", Some("2\n"))], "s1").await;
    let s2 = commit(&r, &[s1], &[("c", Some("3\n"))], "s2").await;
    // Landing the top of an unlanded stack must keep the whole stack.
    let LandResponse::Landed {
        landed, rebased, ..
    } = r.land(s2, "main").await.unwrap()
    else {
        panic!("expected land");
    };
    assert_eq!((landed, rebased), (s2, false));
    assert_eq!(text(&r, landed, "b").await.as_deref(), Some("2\n"));
    assert_eq!(text(&r, landed, "c").await.as_deref(), Some("3\n"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn land_race_twenty_tasks() {
    let r = repo();
    let base = commit(&r, &[], &[("base.txt", Some("base\n"))], "base").await;
    r.land(base, "main").await.unwrap();
    let mut ids = Vec::new();
    for i in 0..20 {
        let name = format!("dir/f{i}.txt");
        ids.push(
            commit(
                &r,
                &[base],
                &[(&name, Some(&format!("{i}\n")))],
                &format!("c{i}"),
            )
            .await,
        );
    }
    let mut tasks = Vec::new();
    for id in ids {
        let r = r.clone();
        tasks.push(tokio::spawn(async move { r.land(id, "main").await }));
    }
    for t in tasks {
        let resp = t.await.unwrap().unwrap();
        assert!(matches!(resp, LandResponse::Landed { .. }), "{resp:?}");
    }
    let head = r.resolve("main").await.unwrap();
    let files = r.list_files(head, "").await.unwrap();
    assert_eq!(files.len(), 21);
    // Linear history: base + 20 changesets, each with a single parent.
    let log = r.log(head, None, 1000).await.unwrap();
    assert_eq!(log.len(), 21);
    assert!(log.iter().all(|i| i.changeset.parents.len() <= 1));
    assert_eq!(log[0].generation, 21);
}

/// Security review 2026-10, S-05: a rewrite of a large file used to make one
/// blame request spend minutes in a quadratic diff that no timeout could stop.
#[tokio::test]
async fn blame_of_a_pathological_rewrite_is_bounded() {
    let r = repo();
    let n = 40_000;
    let old: String = (0..n).map(|i| format!("old {i}\n")).collect();
    let new: String = (0..n).map(|i| format!("new {i}\n")).collect();
    let c1 = commit(&r, &[], &[("big.txt", Some(&old))], "c1").await;
    let c2 = commit(&r, &[c1], &[("big.txt", Some(&new))], "c2").await;
    let started = std::time::Instant::now();
    let b = r.blame(c2, &rp("big.txt")).await.unwrap();
    assert!(
        started.elapsed() < std::time::Duration::from_secs(30),
        "blame took {:?}",
        started.elapsed()
    );
    assert_eq!(b.lines.len(), n);
    assert!(b.lines.iter().all(|l| l.text.starts_with("new ")));
}

#[tokio::test]
async fn blame_attributes_lines() {
    let r = repo();
    let c1 = commit(
        &r,
        &[],
        &[("f", Some("a\nb\nc\n")), ("g", Some("other\n"))],
        "c1",
    )
    .await;
    let c2 = commit(&r, &[c1], &[("f", Some("a\nB\nc\nd\n"))], "c2").await;
    let c3 = commit(&r, &[c2], &[("g", Some("other2\n"))], "c3").await;
    let c4 = commit(&r, &[c3], &[("f", Some("z\na\nB\nd\n"))], "c4").await;

    let b = r.blame(c4, &rp("f")).await.unwrap();
    let got: Vec<_> = b
        .lines
        .iter()
        .map(|l| (l.text.as_str(), l.changeset, l.origin_line))
        .collect();
    assert_eq!(
        got,
        [("z", c4, 1), ("a", c1, 1), ("B", c2, 2), ("d", c2, 4)]
    );
    // Same answer from cache and at an unchanged-file changeset.
    assert_eq!(r.blame(c4, &rp("f")).await.unwrap(), b);
    let at3 = r.blame(c3, &rp("f")).await.unwrap();
    assert_eq!(
        at3.lines
            .iter()
            .map(|l| l.text.as_str())
            .collect::<Vec<_>>(),
        ["a", "B", "c", "d"]
    );
    assert_eq!(at3.lines[2].changeset, c1);
    assert_eq!(r.blame(c2, &rp("f")).await.unwrap(), at3);
    // Incremental from a cached ancestor matches a cold computation.
    let cold = repo();
    let ids = replay(&r, &cold, c4).await;
    assert_eq!(cold.blame(ids, &rp("f")).await.unwrap().lines.len(), 4);

    assert!(matches!(
        r.blame(c4, &rp("nope")).await,
        Err(Error::NotFound(_))
    ));
    let bin = r
        .blobs()
        .put(Bytes::from_static(&[0xff, 0xfe, 0x00]))
        .await
        .unwrap();
    let cs = changeset(
        &[c4],
        BTreeMap::from([(
            rp("bin"),
            FileChange::Modify {
                content: bin,
                kind: FileKind::Regular,
                size: 3,
            },
        )]),
        "bin",
    );
    let c5 = r.create_changeset(cs).await.unwrap();
    assert!(is_invalid(r.blame(c5, &rp("bin")).await));
}

/// Copy a linear history into another repo (same ids), returning the tip.
async fn replay(from: &Repo, to: &Repo, tip: ChangesetId) -> ChangesetId {
    let mut log = from.log(tip, None, usize::MAX).await.unwrap();
    log.reverse();
    for info in log {
        for ch in info.changeset.changes.values() {
            if let FileChange::Modify { content, .. } = ch {
                let data = from.blobs().get(content).await.unwrap().unwrap();
                to.blobs().put(data).await.unwrap();
            }
        }
        assert_eq!(to.create_changeset(info.changeset).await.unwrap(), info.id);
    }
    tip
}

#[tokio::test]
async fn blame_after_delete_and_recreate() {
    let r = repo();
    let c1 = commit(&r, &[], &[("f", Some("old\n"))], "c1").await;
    let c2 = commit(&r, &[c1], &[("f", None)], "c2").await;
    let c3 = commit(&r, &[c2], &[("f", Some("old\nnew\n"))], "c3").await;
    let b = r.blame(c3, &rp("f")).await.unwrap();
    assert!(b.lines.iter().all(|l| l.changeset == c3));
}

/// Blame must not read every ancestor of a file nobody has touched for a
/// long time (the soak test found blame taking seconds on a deep history).
#[tokio::test]
async fn blame_of_an_untouched_file_does_not_walk_the_whole_history() {
    let r = repo();
    let mut tip = commit(
        &r,
        &[],
        &[("old", Some("one\ntwo\n")), ("other", Some("0\n"))],
        "root",
    )
    .await;
    tip = commit(&r, &[tip], &[("old", Some("one\ntwo\nthree\n"))], "touch").await;
    for i in 1..=300 {
        tip = commit(
            &r,
            &[tip],
            &[("other", Some(&format!("{i}\n")))],
            "edit other",
        )
        .await;
    }
    let (full, walked) = r.blame_walk(tip, &rp("old"), false, false).await.unwrap();
    let (fast, skipped) = r.blame_walk(tip, &rp("old"), true, false).await.unwrap();
    assert_eq!(fast, full);
    assert!(
        walked > 300,
        "the reference walk reads every ancestor: {walked}"
    );
    assert!(skipped <= 3, "skipping examined {skipped} changesets");
    // A repeated call is answered from the cache without any walk.
    let b = r.blame(tip, &rp("old")).await.unwrap();
    let (_, again) = r.blame_walk(tip, &rp("old"), true, true).await.unwrap();
    assert_eq!(again, 0);
    assert_eq!(b.lines.len(), 3);
}

/// Skipping is an optimisation: over histories with edits, reverts, deletes,
/// re-creations and merges it must give the answer of the full walk.
#[tokio::test]
async fn blame_skipping_agrees_with_a_full_walk() {
    struct Lcg(u64);
    impl Lcg {
        fn next(&mut self) -> usize {
            self.0 = self
                .0
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (self.0 >> 33) as usize
        }
    }
    let text = |tag: usize| -> String { (0..6).map(|i| format!("line {i} v{tag}\n")).collect() };
    for seed in 1..=6u64 {
        let r = repo();
        let mut rng = Lcg(seed);
        let names = ["a", "b", "c", "d"];
        let mut state: BTreeMap<&str, Vec<Option<String>>> = BTreeMap::new();
        let mut root_files = Vec::new();
        for n in names {
            state.insert(n, vec![Some(text(0))]);
            root_files.push((n, Some(text(0))));
        }
        let files: Vec<(&str, Option<&str>)> =
            root_files.iter().map(|(n, t)| (*n, t.as_deref())).collect();
        let mut main = commit(&r, &[], &files, "root").await;
        let mut all = vec![main];
        for step in 1..=90usize {
            let n = names[rng.next() % names.len()];
            let history = state.get_mut(n).unwrap();
            match rng.next() % 10 {
                0..=4 => {
                    // Edit (or create again) the file.
                    let new = text(step);
                    history.push(Some(new.clone()));
                    main = commit(&r, &[main], &[(n, Some(&new))], "edit").await;
                }
                5 | 6 if history.len() > 1 => {
                    // Back to an earlier version.
                    let old = history[history.len() - 2].clone();
                    history.push(old.clone());
                    main = commit(&r, &[main], &[(n, old.as_deref())], "revert").await;
                }
                7 => {
                    let gone = history.last().is_some_and(|t| t.is_none());
                    let new = (!gone).then(|| text(step));
                    history.push(new.clone());
                    main = commit(&r, &[main], &[(n, new.as_deref())], "toggle").await;
                }
                8 => {
                    // A side branch from an earlier changeset, merged back.
                    let base = all[rng.next() % all.len()];
                    let mut side = base;
                    for k in 0..1 + rng.next() % 3 {
                        let m = names[rng.next() % names.len()];
                        side = commit(
                            &r,
                            &[side],
                            &[(m, Some(&text(1000 + step * 10 + k)))],
                            "side",
                        )
                        .await;
                        all.push(side);
                    }
                    let m = names[rng.next() % names.len()];
                    let new = text(2000 + step);
                    state.get_mut(m).unwrap().push(Some(new.clone()));
                    main = commit(&r, &[main, side], &[(m, Some(&new))], "merge").await;
                }
                _ => {
                    let other = format!("e{step}");
                    main = commit(&r, &[main], &[(&other, Some("x\n"))], "unrelated").await;
                }
            }
            all.push(main);
        }
        let mut compared = 0;
        for (i, tip) in all.iter().enumerate() {
            if i % 3 != 0 && *tip != main {
                continue;
            }
            for n in names {
                let full = r.blame_walk(*tip, &rp(n), false, false).await;
                let fast = r.blame_walk(*tip, &rp(n), true, false).await;
                match (full, fast) {
                    (Ok(a), Ok(b)) => {
                        assert_eq!(a.0, b.0, "seed {seed}, {n} at {tip}");
                        compared += 1;
                    }
                    (Err(Error::NotFound(_)), Err(Error::NotFound(_))) => {}
                    (a, b) => panic!("seed {seed}, {n} at {tip}: {a:?} vs {b:?}"),
                }
            }
        }
        assert!(compared > 40, "seed {seed} compared only {compared}");
    }
}

#[tokio::test]
async fn deep_linear_chain_via_create() {
    let r = repo();
    let mut tip = commit(&r, &[], &[("f0", Some("0"))], "root").await;
    let first = tip;
    for i in 1..20_000 {
        let content = format!("{i}");
        let name = format!("d{}/f{}", i % 7, i % 50);
        tip = commit(&r, &[tip], &[(&name, Some(&content))], &format!("c{i}")).await;
    }
    assert_eq!(r.generation(tip).await.unwrap(), 20_000);
    assert!(r.is_ancestor(first, tip).await.unwrap());
    assert!(!r.is_ancestor(tip, first).await.unwrap());
    assert_eq!(r.merge_base(tip, first).await.unwrap(), Some(first));
    assert_eq!(r.log(tip, None, 3).await.unwrap().len(), 3);
}

#[tokio::test]
async fn deriving_a_deep_history_is_iterative() {
    // Changesets written straight to the meta store have no derived data; one
    // derive call at the tip must walk 20,000 ancestors without recursion.
    let r = repo();
    let meta = r.meta().clone();
    let mut parents = vec![];
    let mut tip = None;
    for i in 0..20_000u32 {
        let mut cs = changeset(&parents, BTreeMap::new(), &format!("c{i}"));
        cs.changes.insert(
            rp(&format!("f{}", i % 10)),
            FileChange::Modify {
                content: Hash::of_blob(b"x"),
                kind: FileKind::Regular,
                size: 1,
            },
        );
        let id = meta.put_changeset(&cs).await.unwrap();
        parents = vec![id];
        tip = Some(id);
    }
    let tip = tip.unwrap();
    assert_eq!(r.generation(tip).await.unwrap(), 20_000);
    let tree = r.root_tree(tip).await.unwrap();
    assert_ne!(tree, TreeNode::empty_hash());
    assert_eq!(r.list_files(tip, "").await.unwrap().len(), 10);
}

#[tokio::test]
async fn land_replays_the_whole_unlanded_stack() {
    let r = repo();
    let c1 = commit(&r, &[], &[("a", Some("1\n"))], "c1").await;
    r.land(c1, "main").await.unwrap();
    let s1 = commit(&r, &[c1], &[("b", Some("2\n"))], "s1").await;
    let s2 = commit(&r, &[s1], &[("c", Some("3\n"))], "s2").await;
    // Someone else lands first, so main is no longer an ancestor of s2.
    let other = commit(&r, &[c1], &[("d", Some("4\n"))], "other").await;
    r.land(other, "main").await.unwrap();

    let LandResponse::Landed {
        landed, rebased, ..
    } = r.land(s2, "main").await.unwrap()
    else {
        panic!("expected land");
    };
    assert!(rebased);
    for (path, want) in [("a", "1\n"), ("b", "2\n"), ("c", "3\n"), ("d", "4\n")] {
        assert_eq!(
            text(&r, landed, path).await.as_deref(),
            Some(want),
            "{path}"
        );
    }
    // Both stack entries were replayed, keeping their messages, on top of `other`.
    let top = r.get_changeset(landed).await.unwrap();
    assert_eq!(top.message, "s2");
    let below = r.get_changeset(top.parents[0]).await.unwrap();
    assert_eq!(below.message, "s1");
    assert_eq!(below.parents, [other]);
}

#[tokio::test]
async fn land_conflict_on_a_stack_reports_clean_changes_and_resolves_in_one_commit() {
    let r = repo();
    let c1 = commit(&r, &[], &[("a", Some("base\n"))], "c1").await;
    r.land(c1, "main").await.unwrap();
    let theirs = commit(&r, &[c1], &[("a", Some("main\n"))], "theirs").await;
    r.land(theirs, "main").await.unwrap();
    let s1 = commit(
        &r,
        &[c1],
        &[("a", Some("mine\n")), ("b", Some("b\n"))],
        "s1",
    )
    .await;
    let s2 = commit(&r, &[s1], &[("c", Some("c\n"))], "s2").await;

    let LandResponse::Conflicts {
        onto,
        conflicts,
        clean,
    } = r.land(s2, "main").await.unwrap()
    else {
        panic!("expected conflicts");
    };
    assert_eq!(onto, theirs);
    let paths: Vec<_> = conflicts.iter().map(|c| c.path.as_str()).collect();
    assert_eq!(paths, ["a"]);
    // Clean changes cover the whole stack, not just its top.
    let clean_paths: Vec<_> = clean.keys().map(|p| p.as_str()).collect();
    assert_eq!(clean_paths, ["b", "c"]);
    assert_eq!(
        r.resolve("main").await.unwrap(),
        theirs,
        "main must not move"
    );

    // Client resolves on top of `onto` and commits once: lands as a fast-forward.
    let mut changes = clean.clone();
    changes.insert(rp("a"), modify(&r, b"resolved\n").await);
    let fixed = r
        .create_changeset(changeset(&[onto], changes, "resolve"))
        .await
        .unwrap();
    let LandResponse::Landed {
        landed, rebased, ..
    } = r.land(fixed, "main").await.unwrap()
    else {
        panic!("expected land");
    };
    assert_eq!((landed, rebased), (fixed, false));
    for (path, want) in [("a", "resolved\n"), ("b", "b\n"), ("c", "c\n")] {
        assert_eq!(
            text(&r, landed, path).await.as_deref(),
            Some(want),
            "{path}"
        );
    }
}

#[tokio::test]
async fn land_refuses_a_stack_containing_a_merge() {
    let r = repo();
    let c1 = commit(&r, &[], &[("a", Some("1\n"))], "c1").await;
    r.land(c1, "main").await.unwrap();
    let x = commit(&r, &[c1], &[("x", Some("x\n"))], "x").await;
    let y = commit(&r, &[c1], &[("y", Some("y\n"))], "y").await;
    let MergeResponse::Merged { id: m } = r.merge(merge_req(x, y)).await.unwrap() else {
        panic!("expected merge");
    };
    let top = commit(&r, &[m], &[("z", Some("z\n"))], "top").await;
    let other = commit(&r, &[c1], &[("o", Some("o\n"))], "other").await;
    r.land(other, "main").await.unwrap();
    assert!(is_invalid(r.land(top, "main").await));
}

/// A metadata store with database-like latency on ref operations, which
/// widens the window between reading the head and swapping it.
struct SlowRefs(MemoryMetaStore);

#[async_trait::async_trait]
impl leep_core::MetaStore for SlowRefs {
    async fn put_changeset(&self, cs: &Changeset) -> leep_core::Result<ChangesetId> {
        self.0.put_changeset(cs).await
    }
    async fn get_changeset(&self, id: &ChangesetId) -> leep_core::Result<Option<Changeset>> {
        self.0.get_changeset(id).await
    }
    async fn list_changesets(
        &self,
        after: Option<ChangesetId>,
        limit: usize,
    ) -> leep_core::Result<Vec<ChangesetId>> {
        self.0.list_changesets(after, limit).await
    }
    async fn get_ref(&self, name: &str) -> leep_core::Result<Option<ChangesetId>> {
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        self.0.get_ref(name).await
    }
    async fn cas_ref_logged(
        &self,
        name: &str,
        expected: Option<ChangesetId>,
        new: Option<ChangesetId>,
        ctx: &leep_core::RefUpdateContext,
    ) -> leep_core::Result<bool> {
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        self.0.cas_ref_logged(name, expected, new, ctx).await
    }
    async fn ref_log(
        &self,
        name: Option<&str>,
        before_seq: Option<u64>,
        limit: usize,
    ) -> leep_core::Result<Vec<leep_core::RefLogEntry>> {
        self.0.ref_log(name, before_seq, limit).await
    }
    async fn list_refs(
        &self,
    ) -> leep_core::Result<std::collections::BTreeMap<String, ChangesetId>> {
        self.0.list_refs().await
    }
    async fn get_derived(
        &self,
        kind: &str,
        key: &leep_core::Hash,
    ) -> leep_core::Result<Option<Bytes>> {
        self.0.get_derived(kind, key).await
    }
    async fn put_derived(
        &self,
        kind: &str,
        key: &leep_core::Hash,
        value: Bytes,
    ) -> leep_core::Result<()> {
        self.0.put_derived(kind, key, value).await
    }
}

/// `n` changesets from one base, each adding its own file.
async fn fan_out(r: &Repo, n: usize) -> (ChangesetId, Vec<ChangesetId>) {
    let base = commit(r, &[], &[("base.txt", Some("base\n"))], "base").await;
    r.land(base, "main").await.unwrap();
    let mut ids = Vec::new();
    for i in 0..n {
        let name = format!("agents/a{i}.txt");
        ids.push(
            commit(
                r,
                &[base],
                &[(&name, Some(&format!("{i}\n")))],
                &format!("a{i}"),
            )
            .await,
        );
    }
    (base, ids)
}

async fn assert_all_landed(r: &Repo, n: usize) {
    let head = r.resolve("main").await.unwrap();
    assert_eq!(r.list_files(head, "").await.unwrap().len(), n + 1);
    let log = r.log(head, None, 10_000).await.unwrap();
    assert_eq!(log.len(), n + 1);
    assert!(log.iter().all(|i| i.changeset.parents.len() <= 1));
}

/// Many agents landing at once through one server must all succeed: lands on
/// a ref are serialized in-process instead of racing each other to the CAS.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn land_swarm_on_one_server_all_succeed() {
    let r = Repo::new(
        Arc::new(MemoryBlobStore::new()),
        Arc::new(SlowRefs(MemoryMetaStore::new())),
    );
    let (_, ids) = fan_out(&r, 60).await;
    let tasks: Vec<_> = ids
        .into_iter()
        .map(|id| {
            let r = r.clone();
            tokio::spawn(async move { r.land(id, "main").await })
        })
        .collect();
    for t in tasks {
        let resp = t.await.unwrap().unwrap();
        assert!(matches!(resp, LandResponse::Landed { .. }), "{resp:?}");
    }
    assert_all_landed(&r, 60).await;
}

/// Two server processes sharing a store only coordinate through the CAS;
/// jittered backoff keeps them from losing in lockstep.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn land_swarm_across_two_servers_all_succeed() {
    let blobs: Arc<dyn leep_core::BlobStore> = Arc::new(MemoryBlobStore::new());
    let meta: Arc<dyn leep_core::MetaStore> = Arc::new(SlowRefs(MemoryMetaStore::new()));
    let servers = [
        Repo::new(blobs.clone(), meta.clone()),
        Repo::new(blobs.clone(), meta.clone()),
    ];
    let (_, ids) = fan_out(&servers[0], 40).await;
    let tasks: Vec<_> = ids
        .into_iter()
        .enumerate()
        .map(|(i, id)| {
            let r = servers[i % 2].clone();
            tokio::spawn(async move { r.land(id, "main").await })
        })
        .collect();
    for t in tasks {
        let resp = t.await.unwrap().unwrap();
        assert!(matches!(resp, LandResponse::Landed { .. }), "{resp:?}");
    }
    assert_all_landed(&servers[0], 40).await;
}

/// Commit one file with a chosen author, timestamp and `extra`.
async fn commit_meta(
    r: &Repo,
    parents: &[ChangesetId],
    file: (&str, &str),
    author: &str,
    ts: i64,
    extra: &[(&str, &str)],
) -> ChangesetId {
    let mut cs = changeset(
        parents,
        BTreeMap::from([(rp(file.0), modify(r, file.1.as_bytes()).await)]),
        &format!("edit {}\nmore", file.0),
    );
    cs.author = author.into();
    cs.timestamp = ts;
    cs.extra = extra
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    r.create_changeset(cs).await.unwrap()
}

struct Prov {
    r: Repo,
    ids: Vec<ChangesetId>,
}

/// c0 human ann (a), c1 m1/s1 (a), c2 m1/s2 (b), c3 m2/s1 (src/b), c4 human
/// Bob (a), c5 m1/s1 (src/b), c6 session-only (b).
async fn prov_repo() -> Prov {
    let r = repo();
    type Spec<'a> = (&'a str, &'a str, &'a str, &'a [(&'a str, &'a str)]);
    let spec: [Spec; 7] = [
        ("a", "ann", "x0", &[]),
        (
            "a",
            "bot",
            "x1",
            &[("agent.model", "m1"), ("agent.session", "s1")],
        ),
        (
            "b",
            "bot",
            "x2",
            &[("agent.model", "m1"), ("agent.session", "s2")],
        ),
        (
            "src/b",
            "bot",
            "x3",
            &[("agent.model", "m2"), ("agent.session", "s1")],
        ),
        ("a", "Bob", "x4", &[("leep.rebased_from", "z")]),
        (
            "src/b",
            "bot",
            "x5",
            &[("agent.model", "m1"), ("agent.session", "s1")],
        ),
        ("b", "bot", "x6", &[("agent.session", "s9")]),
    ];
    let mut ids: Vec<ChangesetId> = Vec::new();
    for (i, (path, author, body, extra)) in spec.iter().enumerate() {
        let parents: Vec<_> = ids.last().copied().into_iter().collect();
        let id = commit_meta(&r, &parents, (path, body), author, 100 + i as i64, extra).await;
        ids.push(id);
    }
    Prov { r, ids }
}

fn extra(s: &str) -> crate::ExtraMatch {
    crate::ExtraMatch::parse(s).unwrap()
}

#[tokio::test]
async fn log_filter_by_extra_author_and_path() {
    let Prov { r, ids } = prov_repo().await;
    let tip = ids[6];
    let run = |path: Option<&'static str>, limit: usize, f: crate::LogFilter| {
        let r = &r;
        async move {
            r.log_filtered(tip, path, limit, &f)
                .await
                .unwrap()
                .into_iter()
                .map(|i| i.id)
                .collect::<Vec<_>>()
        }
    };
    let f = |author: Option<&str>, e: &[&str]| crate::LogFilter {
        author: author.map(Into::into),
        extra: e.iter().map(|s| extra(s)).collect(),
    };

    assert_eq!(run(None, 100, f(None, &[])).await.len(), 7);
    assert_eq!(
        run(None, 100, f(None, &["agent.model:m1"])).await,
        [ids[5], ids[2], ids[1]]
    );
    assert_eq!(
        run(None, 100, f(None, &["agent.model:m1", "agent.session:s1"])).await,
        [ids[5], ids[1]]
    );
    assert_eq!(
        run(None, 100, f(None, &["agent.session:s2"])).await,
        [ids[2]]
    );
    // Presence: any value, including a session without a model.
    assert_eq!(
        run(None, 100, f(None, &["agent.session"])).await,
        [ids[6], ids[5], ids[3], ids[2], ids[1]]
    );
    assert!(run(None, 100, f(None, &["agent.model:nope"]))
        .await
        .is_empty());
    assert!(run(None, 100, f(None, &["agent.prompt"])).await.is_empty());
    // Author is a case-insensitive substring.
    assert_eq!(run(None, 100, f(Some("BOB"), &[])).await, [ids[4]]);
    assert_eq!(run(None, 100, f(Some("an"), &[])).await, [ids[0]]);
    // Combined with a path and a limit that counts matches.
    assert_eq!(
        run(Some("src"), 100, f(None, &["agent.model:m1"])).await,
        [ids[5]]
    );
    assert_eq!(
        run(Some("a"), 100, f(None, &["agent.model"])).await,
        [ids[1]]
    );
    assert_eq!(
        run(None, 2, f(None, &["agent.model:m1"])).await,
        [ids[5], ids[2]]
    );
    assert!(run(Some("src"), 100, f(Some("ann"), &[])).await.is_empty());
}

#[test]
fn extra_match_parsing() {
    assert_eq!(
        extra("k:v:w"),
        crate::ExtraMatch {
            key: "k".into(),
            value: Some("v:w".into())
        }
    );
    assert_eq!(extra("k").value, None);
    assert_eq!(extra("k:").value, Some(String::new()));
    assert!(crate::ExtraMatch::parse(":x").is_err());
    assert!(crate::ExtraMatch::parse("").is_err());
}

#[tokio::test]
async fn blame_has_deduplicated_changeset_summaries() {
    let r = repo();
    let c1 = commit_meta(&r, &[], ("f", "a\nb\n"), "ann", 5, &[]).await;
    let c2 = commit_meta(
        &r,
        &[c1],
        ("f", "a\nb\nc\nd\n"),
        "bot",
        6,
        &[
            ("agent.model", "m1"),
            ("agent.session", "s1"),
            ("leep.rebased_from", "zz"),
        ],
    )
    .await;
    let b = r.blame(c2, &rp("f")).await.unwrap();
    assert_eq!(b.lines.len(), 4);
    assert_eq!(b.changesets.len(), 2, "two lines of c2 share one summary");
    assert_eq!(b.changesets[&c1].author, "ann");
    assert!(b.changesets[&c1].agent.is_empty());
    let s = &b.changesets[&c2];
    assert_eq!((s.author.as_str(), s.timestamp), ("bot", 6));
    assert_eq!(s.message_first_line, "edit f");
    assert_eq!(
        s.agent.keys().map(String::as_str).collect::<Vec<_>>(),
        ["agent.model", "agent.session"],
        "only agent.* keys"
    );
    // A second call is served from the cache and still carries summaries.
    assert_eq!(r.blame(c2, &rp("f")).await.unwrap(), b);
}

#[tokio::test]
async fn blame_cache_written_before_summaries_still_decodes() {
    let r = repo();
    let c1 = commit_meta(&r, &[], ("f", "a\n"), "ann", 5, &[]).await;
    // The pre-summary encoding was a struct of (path, lines).
    let legacy = (
        rp("f"),
        vec![leep_core::api::BlameLine {
            changeset: c1,
            origin_line: 1,
            text: "a".into(),
        }],
    );
    let mut key = c1.0.as_bytes().to_vec();
    key.extend_from_slice(b"f");
    r.meta()
        .put_derived(
            "blame",
            &Hash::of_object("blame", &key),
            Bytes::from(postcard::to_stdvec(&legacy).unwrap()),
        )
        .await
        .unwrap();
    let b = r.blame(c1, &rp("f")).await.unwrap();
    assert_eq!(b.lines.len(), 1);
    assert_eq!(b.changesets[&c1].author, "ann");
}

#[tokio::test]
async fn provenance_aggregates_agents_and_humans() {
    let Prov { r, ids } = prov_repo().await;
    let p = r.provenance(ids[6], None, 100).await.unwrap();
    assert_eq!((p.scanned, p.truncated), (7, false));
    let agents: Vec<_> = p
        .agents
        .iter()
        .map(|a| (a.model.as_str(), a.session.as_deref(), a.changesets))
        .collect();
    assert_eq!(
        agents,
        [
            ("m1", Some("s1"), 2),
            ("m1", Some("s2"), 1),
            ("m2", Some("s1"), 1),
            ("unknown", Some("s9"), 1),
        ]
    );
    let m1s1 = &p.agents[0];
    assert_eq!((m1s1.first, m1s1.last), (101, 105));
    assert_eq!(m1s1.files_touched, 2, "a and src/b");
    let humans: Vec<_> = p
        .humans
        .iter()
        .map(|h| (h.author.as_str(), h.changesets))
        .collect();
    assert_eq!(humans, [("Bob", 1), ("ann", 1)]);
    assert_eq!((p.humans[0].first, p.humans[0].last), (104, 104));

    // Path prefix narrows to changesets touching it.
    let p = r.provenance(ids[6], Some("/src/"), 100).await.unwrap();
    assert_eq!(p.scanned, 7);
    assert_eq!(p.agents.len(), 2);
    assert!(p.humans.is_empty());
    assert_eq!(p.agents[0].files_touched, 1);
    // A prefix that merely shares leading characters does not match.
    assert!(r
        .provenance(ids[6], Some("sr"), 100)
        .await
        .unwrap()
        .agents
        .is_empty());
}

#[tokio::test]
async fn provenance_truncates_at_limit() {
    let Prov { r, ids } = prov_repo().await;
    let p = r.provenance(ids[6], None, 3).await.unwrap();
    assert_eq!((p.scanned, p.truncated), (3, true));
    let total: u64 = p.agents.iter().map(|a| a.changesets).sum::<u64>()
        + p.humans.iter().map(|h| h.changesets).sum::<u64>();
    assert_eq!(total, 3);
    // Exactly the history length is not truncated.
    let p = r.provenance(ids[6], None, 7).await.unwrap();
    assert_eq!((p.scanned, p.truncated), (7, false));
    let p = r.provenance(ids[6], None, 0).await.unwrap();
    assert_eq!((p.scanned, p.truncated), (0, true));
}

/// The skiplist fast path in `merge_base` must agree with the reference walk
/// on arbitrary DAGs: branches, merges, criss-crosses and unrelated roots.
#[tokio::test]
async fn merge_base_fast_path_matches_reference_walk() {
    let mut seed: u64 = 0x5eed;
    let mut rand = move |n: usize| {
        seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        ((seed >> 33) as usize) % n
    };
    for round in 0..4 {
        let r = repo();
        let mut ids: Vec<ChangesetId> = Vec::new();
        for i in 0..300 {
            let name = format!("r{round}/f{i}");
            let parents: Vec<ChangesetId> = match (ids.len(), rand(10)) {
                (0, _) => vec![],
                // A second root now and then: unrelated histories.
                (_, 0) if i % 97 == 0 => vec![],
                (n, 0..=1) if n >= 2 => {
                    let a = ids[n - 1 - rand(n.min(20))];
                    let b = ids[rand(n)];
                    if a == b {
                        vec![a]
                    } else {
                        vec![a, b]
                    }
                }
                (n, _) => vec![ids[n - 1 - rand(n.min(8))]],
            };
            ids.push(commit(&r, &parents, &[(&name, Some("x"))], &name).await);
        }
        for _ in 0..1500 {
            let (a, b) = (ids[rand(ids.len())], ids[rand(ids.len())]);
            let fast = r.merge_base(a, b).await.unwrap();
            let walk = r.merge_base_walk(a, b).await.unwrap();
            assert_eq!(fast, walk, "merge_base({a}, {b}) in round {round}");
        }
    }
}

mod land_policy {
    use async_trait::async_trait;
    use leep_core::signature::{key_id, Signature, NOT_SIGNED_MARKER};

    use super::*;
    use crate::LandPolicy;

    const KEY: [u8; 32] = [9; 32];

    /// Requires a stored signature by one fixed key on `protected`; checks no
    /// cryptography, which belongs to the server.
    struct NeedsKey {
        protected: &'static str,
    }

    #[async_trait]
    impl LandPolicy for NeedsKey {
        fn applies_to(&self, target_ref: &str) -> bool {
            target_ref == self.protected
        }

        async fn check_land(
            &self,
            repo: &Repo,
            _target_ref: &str,
            _head: Option<ChangesetId>,
            new_history: &[ChangesetId],
        ) -> leep_core::Result<()> {
            for id in new_history {
                if repo.get_signature(*id, &key_id(&KEY)).await?.is_none() {
                    return Err(Error::Invalid(format!(
                        "changeset {id} {NOT_SIGNED_MARKER}"
                    )));
                }
            }
            Ok(())
        }
    }

    fn guarded() -> Repo {
        repo().with_land_policy(Arc::new(NeedsKey { protected: "main" }))
    }

    async fn sign(r: &Repo, id: ChangesetId) {
        let sig = Signature {
            key_id: key_id(&KEY),
            public_key: KEY,
            sig: [1; 64],
        };
        r.put_signature(id, &sig).await.unwrap();
    }

    fn assert_unsigned(err: Error, id: ChangesetId) {
        match err {
            Error::Invalid(m) => assert_eq!(m, format!("changeset {id} {NOT_SIGNED_MARKER}")),
            e => panic!("expected Invalid, got {e:?}"),
        }
    }

    #[tokio::test]
    async fn protected_ref_cannot_be_deleted() {
        let r = guarded();
        let c = commit(&r, &[], &[("a", Some("1\n"))], "c").await;
        sign(&r, c).await;
        r.land(c, "main").await.unwrap();
        match r.cas_ref_checked("main", Some(c), None, &cas_ctx()).await {
            Err(Error::Invalid(m)) => assert!(m.contains("protected"), "{m}"),
            other => panic!("{other:?}"),
        }
        assert_eq!(r.meta().get_ref("main").await.unwrap(), Some(c));
        // Refs the policy does not cover can still be deleted.
        assert!(r
            .cas_ref_checked("scratch", None, Some(c), &cas_ctx())
            .await
            .unwrap());
        assert!(r
            .cas_ref_checked("scratch", Some(c), None, &cas_ctx())
            .await
            .unwrap());
        assert_eq!(r.meta().get_ref("scratch").await.unwrap(), None);
    }

    /// Security review 2026-10, S-09: a protected ref only moves forward, so
    /// no raw swap or undo can drop signed or reviewed history.
    #[tokio::test]
    async fn protected_refs_only_move_forward() {
        let r = guarded();
        let a = commit(&r, &[], &[("a", Some("1\n"))], "a").await;
        let b = commit(&r, &[a], &[("a", Some("2\n"))], "b").await;
        let side = commit(&r, &[a], &[("a", Some("3\n"))], "side").await;
        for id in [a, b, side] {
            sign(&r, id).await;
        }
        // Creating and fast-forwarding are fine.
        assert!(r
            .cas_ref_checked("main", None, Some(a), &cas_ctx())
            .await
            .unwrap());
        assert!(r
            .cas_ref_checked("main", Some(a), Some(b), &cas_ctx())
            .await
            .unwrap());
        // Back to an ancestor, or across to a sibling, is refused.
        for to in [a, side] {
            match r
                .cas_ref_checked("main", Some(b), Some(to), &cas_ctx())
                .await
            {
                Err(Error::Invalid(m)) => {
                    assert!(m.contains(leep_core::refname::NOT_FORWARD_MARKER), "{m}")
                }
                other => panic!("{other:?}"),
            }
        }
        assert_eq!(r.meta().get_ref("main").await.unwrap(), Some(b));
        assert_eq!(r.meta().ref_log(None, None, 10).await.unwrap().len(), 2);
        // Unprotected refs still move anywhere.
        assert!(r
            .cas_ref_checked("scratch", None, Some(b), &cas_ctx())
            .await
            .unwrap());
        assert!(r
            .cas_ref_checked("scratch", Some(b), Some(a), &cas_ctx())
            .await
            .unwrap());
        assert!(r
            .cas_ref_checked("scratch", Some(a), Some(side), &cas_ctx())
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn refused_moves_record_nothing() {
        let r = guarded();
        let a = commit(&r, &[], &[("a", Some("1\n"))], "a").await;
        let b = commit(&r, &[a], &[("a", Some("2\n"))], "b").await;
        let entries = |r: &Repo| {
            let meta = r.meta().clone();
            async move { meta.ref_log(None, None, 100).await.unwrap().len() }
        };
        assert!(r.land(a, "main").await.is_err());
        assert!(r
            .cas_ref_checked("main", None, Some(a), &cas_ctx())
            .await
            .is_err());
        assert_eq!(entries(&r).await, 0, "unsigned history is refused");
        sign(&r, a).await;
        r.land_as(a, "main", Some("ci")).await.unwrap();
        assert_eq!(entries(&r).await, 1);

        assert!(r.land(b, "main").await.is_err());
        assert!(r
            .cas_ref_checked("main", Some(a), Some(b), &cas_ctx())
            .await
            .is_err());
        assert!(r
            .cas_ref_checked("main", Some(a), None, &cas_ctx())
            .await
            .is_err());
        assert_eq!(entries(&r).await, 1, "refused move and protected delete");
        // A lost CAS is not an error and not an entry either.
        assert!(!r
            .cas_ref_checked("main", None, Some(a), &cas_ctx())
            .await
            .unwrap());
        assert_eq!(entries(&r).await, 1);
    }

    #[tokio::test]
    async fn unsigned_land_is_rejected_and_ref_untouched() {
        let r = guarded();
        let c = commit(&r, &[], &[("a", Some("1\n"))], "c").await;
        assert_unsigned(r.land(c, "main").await.unwrap_err(), c);
        assert_eq!(r.meta().get_ref("main").await.unwrap(), None);
    }

    #[tokio::test]
    async fn signed_land_is_accepted() {
        let r = guarded();
        let c = commit(&r, &[], &[("a", Some("1\n"))], "c").await;
        sign(&r, c).await;
        assert!(matches!(
            r.land(c, "main").await.unwrap(),
            LandResponse::Landed { landed, .. } if landed == c
        ));
        // Landing it again adds nothing, so it needs no check and still works.
        assert!(r.land(c, "main").await.is_ok());
    }

    #[tokio::test]
    async fn one_unsigned_entry_in_a_fast_forward_stack_is_rejected() {
        let r = guarded();
        let base = commit(&r, &[], &[("a", Some("1\n"))], "base").await;
        sign(&r, base).await;
        r.land(base, "main").await.unwrap();
        let s1 = commit(&r, &[base], &[("b", Some("2\n"))], "s1").await;
        let s2 = commit(&r, &[s1], &[("c", Some("3\n"))], "s2").await;
        sign(&r, s2).await;
        assert_unsigned(r.land(s2, "main").await.unwrap_err(), s1);
        sign(&r, s1).await;
        r.land(s2, "main").await.unwrap();
        assert_eq!(r.resolve("main").await.unwrap(), s2);
    }

    #[tokio::test]
    async fn replayed_stack_checks_originals_and_records_origin() {
        let r = guarded();
        let base = commit(&r, &[], &[("a", Some("1\n"))], "base").await;
        sign(&r, base).await;
        r.land(base, "main").await.unwrap();
        let head = commit(&r, &[base], &[("h", Some("h\n"))], "head").await;
        sign(&r, head).await;
        r.land(head, "main").await.unwrap();

        let s1 = commit(&r, &[base], &[("b", Some("2\n"))], "s1").await;
        let s2 = commit(&r, &[s1], &[("c", Some("3\n"))], "s2").await;
        sign(&r, s2).await;
        assert_unsigned(r.land(s2, "main").await.unwrap_err(), s1);
        assert_eq!(r.resolve("main").await.unwrap(), head);

        sign(&r, s1).await;
        let LandResponse::Landed {
            landed, rebased, ..
        } = r.land(s2, "main").await.unwrap()
        else {
            panic!("expected land");
        };
        assert!(rebased);
        // The copies carry no signature of their own; the server recorded
        // where they came from.
        assert!(r
            .get_signature(landed, &key_id(&KEY))
            .await
            .unwrap()
            .is_none());
        assert_eq!(r.rebased_origin(landed).await.unwrap(), Some(s2));
        assert_eq!(r.rebase_chain(landed).await.unwrap(), vec![s2]);
        assert_eq!(r.rebased_origin(s2).await.unwrap(), None);
    }

    #[tokio::test]
    async fn forged_rebased_from_extra_does_not_pass_the_policy() {
        let r = guarded();
        let signed = commit(&r, &[], &[("a", Some("1\n"))], "signed").await;
        sign(&r, signed).await;
        let mut forged = changeset(&[], BTreeMap::new(), "forged");
        forged.extra.insert(
            leep_core::changeset::EXTRA_REBASED_FROM.into(),
            signed.to_string(),
        );
        let forged = r.create_changeset(forged).await.unwrap();
        assert_unsigned(r.land(forged, "main").await.unwrap_err(), forged);
        assert_eq!(r.rebased_origin(forged).await.unwrap(), None);
    }

    #[tokio::test]
    async fn unprotected_refs_are_unaffected() {
        let r = guarded();
        let c = commit(&r, &[], &[("a", Some("1\n"))], "c").await;
        r.land(c, "dev").await.unwrap();
        assert_eq!(r.resolve("dev").await.unwrap(), c);
    }

    #[tokio::test]
    async fn merge_side_branches_are_checked_too() {
        let r = guarded();
        let base = commit(&r, &[], &[("a", Some("1\n"))], "base").await;
        sign(&r, base).await;
        r.land(base, "main").await.unwrap();
        let side = commit(&r, &[base], &[("s", Some("s\n"))], "side").await;
        let mine = commit(&r, &[base], &[("m", Some("m\n"))], "mine").await;
        let merge = commit(&r, &[mine, side], &[], "merge").await;
        sign(&r, merge).await;
        sign(&r, mine).await;
        assert_unsigned(r.land(merge, "main").await.unwrap_err(), side);
    }

    #[tokio::test]
    async fn signing_an_unknown_changeset_is_not_found() {
        let r = repo();
        let sig = Signature {
            key_id: key_id(&KEY),
            public_key: KEY,
            sig: [1; 64],
        };
        let missing = ChangesetId(Hash::of_blob(b"nope"));
        assert!(matches!(
            r.put_signature(missing, &sig).await,
            Err(Error::NotFound(_))
        ));
        let c = commit(&r, &[], &[("a", Some("1\n"))], "c").await;
        r.put_signature(c, &sig).await.unwrap();
        r.put_signature(c, &sig).await.unwrap();
        assert_eq!(r.get_signature(c, &sig.key_id).await.unwrap(), Some(sig));
        assert_eq!(r.get_signature(c, "other").await.unwrap(), None);
    }
}

mod review {
    use std::sync::Mutex;

    use async_trait::async_trait;
    use leep_core::review::{Review, Verdict};

    use super::*;
    use crate::owners::OwnersResolver;
    use crate::review::{AutoRule, PathRule, ReviewRules};
    use crate::{LandPolicy, PolicySet};

    fn rev(who: &str, v: Verdict) -> Review {
        Review {
            reviewer: who.into(),
            verdict: v,
            comment: String::new(),
            timestamp: 1,
        }
    }

    async fn owners(r: &Repo, head: Option<ChangesetId>, path: &str) -> Vec<String> {
        OwnersResolver::new(r, head)
            .await
            .unwrap()
            .owners_of(&rp(path))
            .await
            .unwrap()
            .into_iter()
            .collect()
    }

    /// A head with OWNERS files at several depths.
    async fn owned_head(r: &Repo) -> ChangesetId {
        commit(
            r,
            &[],
            &[
                ("OWNERS", Some("root\n")),
                ("a/OWNERS", Some("# team\nalice\n")),
                ("a/b/OWNERS", Some("bob\nnoparent\n")),
                ("a/b/f.rs", Some("fn f() {}\n")),
                ("a/g.rs", Some("fn g() {}\n")),
                ("c/h.rs", Some("fn h() {}\n")),
                ("top.rs", Some("fn t() {}\n")),
            ],
            "head",
        )
        .await
    }

    #[tokio::test]
    async fn owners_accumulate_upward_until_noparent() {
        let r = repo();
        let head = owned_head(&r).await;
        assert_eq!(owners(&r, Some(head), "a/g.rs").await, ["alice", "root"]);
        assert_eq!(
            owners(&r, Some(head), "a/new/deeper/x").await,
            ["alice", "root"]
        );
        assert_eq!(owners(&r, Some(head), "a/b/f.rs").await, ["bob"]);
        assert_eq!(owners(&r, Some(head), "c/h.rs").await, ["root"]);
        assert_eq!(owners(&r, Some(head), "top.rs").await, ["root"]);
        // The OWNERS file itself is owned from its directory.
        assert_eq!(owners(&r, Some(head), "a/OWNERS").await, ["alice", "root"]);
        assert_eq!(owners(&r, Some(head), "a/b/OWNERS").await, ["bob"]);
        assert_eq!(owners(&r, Some(head), "OWNERS").await, ["root"]);
    }

    #[tokio::test]
    async fn no_head_or_no_owners_file_means_no_owners() {
        let r = repo();
        assert!(owners(&r, None, "a.rs").await.is_empty());
        let plain = commit(&r, &[], &[("a.rs", Some("x\n"))], "plain").await;
        assert!(owners(&r, Some(plain), "a.rs").await.is_empty());
    }

    #[tokio::test]
    async fn owners_come_from_the_given_head_not_the_change() {
        let r = repo();
        let head = owned_head(&r).await;
        // A change that adds an OWNERS file naming itself is invisible when
        // owners are read from `head`.
        let evil = commit(&r, &[head], &[("c/OWNERS", Some("mallory\n"))], "evil").await;
        assert_eq!(owners(&r, Some(head), "c/h.rs").await, ["root"]);
        assert_eq!(owners(&r, Some(evil), "c/h.rs").await, ["mallory", "root"]);
    }

    #[tokio::test]
    async fn a_symlink_named_owners_is_ignored() {
        let r = repo();
        let target = r.blobs().put(Bytes::from_static(b"mallory")).await.unwrap();
        let mut changes = BTreeMap::new();
        changes.insert(
            rp("d/OWNERS"),
            FileChange::Modify {
                content: target,
                kind: FileKind::Symlink,
                size: 7,
            },
        );
        changes.insert(rp("d/x"), modify(&r, b"x").await);
        let head = r
            .create_changeset(changeset(&[], changes, "l"))
            .await
            .unwrap();
        assert!(owners(&r, Some(head), "d/x").await.is_empty());
    }

    async fn status(
        r: &Repo,
        head: Option<ChangesetId>,
        rules: &ReviewRules,
        id: ChangesetId,
        reviews: &[Review],
    ) -> crate::review::ChangesetReview {
        let mut o = OwnersResolver::new(r, head).await.unwrap();
        r.review_changeset(id, rules, reviews, &mut o)
            .await
            .unwrap()
    }

    fn rule_of(s: &crate::review::ChangesetReview, path: &str) -> PathRule {
        s.paths
            .iter()
            .find(|p| p.path.as_str() == path)
            .unwrap()
            .rule
    }

    #[tokio::test]
    async fn owner_approval_and_rejection_per_path() {
        let r = repo();
        let head = owned_head(&r).await;
        let c = commit(
            &r,
            &[head],
            &[
                ("a/g.rs", Some("fn g2() {}\n")),
                ("a/b/f.rs", Some("fn f2() {}\n")),
            ],
            "c",
        )
        .await;
        let none = ReviewRules::default();

        let s = status(&r, Some(head), &none, c, &[]).await;
        assert!(!s.approved());
        assert_eq!(s.paths.len(), 2);
        // Path order: a/b/f.rs sorts before a/g.rs.
        assert_eq!(s.first_blocker().unwrap().path.as_str(), "a/b/f.rs");
        assert_eq!(s.paths[0].owners, ["bob"]);

        // carol is not an owner of anything here.
        let s = status(&r, Some(head), &none, c, &[rev("carol", Verdict::Approve)]).await;
        assert!(!s.approved());

        // alice owns a/g.rs but not a/b/f.rs (noparent).
        let s = status(&r, Some(head), &none, c, &[rev("alice", Verdict::Approve)]).await;
        assert_eq!(rule_of(&s, "a/g.rs"), PathRule::Owner);
        assert_eq!(rule_of(&s, "a/b/f.rs"), PathRule::Pending);

        let both = [rev("alice", Verdict::Approve), rev("bob", Verdict::Approve)];
        let s = status(&r, Some(head), &none, c, &both).await;
        assert!(s.approved());
        assert_eq!(s.paths[1].approved_by, ["alice"]);

        // An owner's reject blocks that path even beside other approvals.
        let mixed = [rev("alice", Verdict::Approve), rev("bob", Verdict::Reject)];
        let s = status(&r, Some(head), &none, c, &mixed).await;
        assert!(!s.approved());
        assert_eq!(rule_of(&s, "a/b/f.rs"), PathRule::Rejected);
        assert_eq!(s.paths[0].rejected_by, ["bob"]);
        assert_eq!(rule_of(&s, "a/g.rs"), PathRule::Owner);

        // A reject by a non-owner of the path is not a veto there.
        let s = status(
            &r,
            Some(head),
            &none,
            c,
            &[rev("alice", Verdict::Reject), rev("bob", Verdict::Approve)],
        )
        .await;
        assert_eq!(rule_of(&s, "a/b/f.rs"), PathRule::Owner);
        assert_eq!(rule_of(&s, "a/g.rs"), PathRule::Rejected);
    }

    #[tokio::test]
    async fn unowned_paths_take_any_approval_and_any_reject() {
        let r = repo();
        let head = commit(&r, &[], &[("x.rs", Some("1\n"))], "h").await;
        let c = commit(&r, &[head], &[("x.rs", Some("2\n"))], "c").await;
        let none = ReviewRules::default();
        assert!(!status(&r, Some(head), &none, c, &[]).await.approved());
        let s = status(&r, Some(head), &none, c, &[rev("zed", Verdict::Approve)]).await;
        assert!(s.approved());
        assert!(s.paths[0].owners.is_empty());
        let s = status(
            &r,
            Some(head),
            &none,
            c,
            &[rev("zed", Verdict::Approve), rev("yan", Verdict::Reject)],
        )
        .await;
        assert_eq!(rule_of(&s, "x.rs"), PathRule::Rejected);
        // No head at all: nothing is owned.
        let s = status(&r, None, &none, c, &[rev("zed", Verdict::Approve)]).await;
        assert!(s.approved());
    }

    #[tokio::test]
    async fn auto_rules_approve_by_content_and_a_reject_still_blocks() {
        let r = repo();
        let head = owned_head(&r).await;
        let rules = ReviewRules::new(
            [
                AutoRule::CommentOnly,
                AutoRule::WhitespaceOnly,
                AutoRule::DocsOnly,
            ],
            &["docs/**".into()],
        )
        .unwrap();
        let c = commit(
            &r,
            &[head],
            &[
                ("a/g.rs", Some("// why\nfn g() {}\n")),
                ("c/h.rs", Some("fn h() {}\t \r\n\n")),
                ("docs/new.md", Some("# hi\n")),
                ("top.rs", Some("fn t() { 1 }\n")),
                ("a/b/new.rs", Some("fn n() {}\n")),
            ],
            "c",
        )
        .await;
        let s = status(&r, Some(head), &rules, c, &[]).await;
        assert_eq!(rule_of(&s, "a/g.rs"), PathRule::Auto(AutoRule::CommentOnly));
        assert_eq!(
            rule_of(&s, "c/h.rs"),
            PathRule::Auto(AutoRule::WhitespaceOnly)
        );
        assert_eq!(
            rule_of(&s, "docs/new.md"),
            PathRule::Auto(AutoRule::DocsOnly)
        );
        // A real code change and an added file are not auto-approved.
        assert_eq!(rule_of(&s, "top.rs"), PathRule::Pending);
        assert_eq!(rule_of(&s, "a/b/new.rs"), PathRule::Pending);

        // alice owns a/ only: her reject blocks the comment-only change there.
        let s = status(&r, Some(head), &rules, c, &[rev("alice", Verdict::Reject)]).await;
        assert_eq!(rule_of(&s, "a/g.rs"), PathRule::Rejected);
        assert_eq!(
            rule_of(&s, "docs/new.md"),
            PathRule::Auto(AutoRule::DocsOnly)
        );
        assert_eq!(
            rule_of(&s, "c/h.rs"),
            PathRule::Auto(AutoRule::WhitespaceOnly)
        );
        // root owns everything not behind a noparent, so it vetoes docs too.
        let s = status(&r, Some(head), &rules, c, &[rev("root", Verdict::Reject)]).await;
        assert_eq!(rule_of(&s, "docs/new.md"), PathRule::Rejected);
        assert_eq!(rule_of(&s, "c/h.rs"), PathRule::Rejected);
    }

    #[tokio::test]
    async fn auto_rules_are_conservative_about_kinds_deletes_and_binary() {
        let r = repo();
        let head = commit(
            &r,
            &[],
            &[("a.rs", Some("fn a() {}\n")), ("b.rs", Some("fn b() {}\n"))],
            "h",
        )
        .await;
        let rules =
            ReviewRules::new([AutoRule::CommentOnly, AutoRule::WhitespaceOnly], &[]).unwrap();
        // Executable bit flip, delete, and a binary replacement.
        let bin = r
            .blobs()
            .put(Bytes::from_static(&[0xff, 0xfe]))
            .await
            .unwrap();
        let exec = r
            .blobs()
            .put(Bytes::from_static(b"fn a() {}\n"))
            .await
            .unwrap();
        let mut changes = BTreeMap::new();
        changes.insert(
            rp("a.rs"),
            FileChange::Modify {
                content: exec,
                kind: FileKind::Executable,
                size: 10,
            },
        );
        changes.insert(rp("b.rs"), FileChange::Delete);
        changes.insert(
            rp("c.rs"),
            FileChange::Modify {
                content: bin,
                kind: FileKind::Regular,
                size: 2,
            },
        );
        let c = r
            .create_changeset(changeset(&[head], changes, "c"))
            .await
            .unwrap();
        let s = status(&r, Some(head), &rules, c, &[]).await;
        assert!(s.paths.iter().all(|p| p.rule == PathRule::Pending), "{s:?}");
        // docs_only does cover deletes.
        let docs = ReviewRules::new([AutoRule::DocsOnly], &["**/*.rs".into()]).unwrap();
        assert!(status(&r, Some(head), &docs, c, &[]).await.approved());
    }

    #[tokio::test]
    async fn merge_paths_equal_to_a_side_parent_are_covered() {
        let r = repo();
        let base = commit(
            &r,
            &[],
            &[("a.rs", Some("1\n")), ("b.rs", Some("1\n"))],
            "base",
        )
        .await;
        let ours = commit(&r, &[base], &[("a.rs", Some("ours\n"))], "ours").await;
        let theirs = commit(
            &r,
            &[base],
            &[("b.rs", Some("theirs\n")), ("c.rs", Some("new\n"))],
            "theirs",
        )
        .await;
        // The merge takes their b.rs and c.rs and makes a fresh d.rs.
        let m = commit(
            &r,
            &[ours, theirs],
            &[
                ("b.rs", Some("theirs\n")),
                ("c.rs", Some("new\n")),
                ("d.rs", Some("fresh\n")),
            ],
            "merge",
        )
        .await;
        let none = ReviewRules::default();
        let s = status(&r, Some(base), &none, m, &[]).await;
        assert_eq!(rule_of(&s, "b.rs"), PathRule::MergeParent);
        assert_eq!(rule_of(&s, "c.rs"), PathRule::MergeParent);
        assert_eq!(rule_of(&s, "d.rs"), PathRule::Pending);
        // Same content, different kind is not covered.
        let exec = r
            .blobs()
            .put(Bytes::from_static(b"theirs\n"))
            .await
            .unwrap();
        let mut changes = BTreeMap::new();
        changes.insert(
            rp("b.rs"),
            FileChange::Modify {
                content: exec,
                kind: FileKind::Executable,
                size: 7,
            },
        );
        let m2 = r
            .create_changeset(changeset(&[ours, theirs], changes, "m2"))
            .await
            .unwrap();
        let s = status(&r, Some(base), &none, m2, &[]).await;
        assert_eq!(rule_of(&s, "b.rs"), PathRule::Pending);
    }

    /// Security review 2026-10, S-07: `docs/**` must not auto-approve
    /// `docs/OWNERS`, and no content rule applies to an `OWNERS` file.
    #[tokio::test]
    async fn owners_files_are_never_auto_approved() {
        let r = repo();
        let head = commit(
            &r,
            &[],
            &[("docs/OWNERS", Some("alice\n")), ("docs/a.md", Some("a\n"))],
            "h",
        )
        .await;
        let rules = ReviewRules::new(
            [
                AutoRule::DocsOnly,
                AutoRule::WhitespaceOnly,
                AutoRule::CommentOnly,
            ],
            &["docs/**".into()],
        )
        .unwrap();
        let c = commit(
            &r,
            &[head],
            &[
                ("docs/OWNERS", Some("alice\nmallory\n")),
                ("docs/a.md", Some("b\n")),
            ],
            "c",
        )
        .await;
        let s = status(&r, Some(head), &rules, c, &[]).await;
        assert_eq!(rule_of(&s, "docs/OWNERS"), PathRule::Pending);
        assert_eq!(rule_of(&s, "docs/a.md"), PathRule::Auto(AutoRule::DocsOnly));
        let ws = commit(&r, &[head], &[("docs/OWNERS", Some("alice  \n"))], "w").await;
        let s = status(&r, Some(head), &rules, ws, &[]).await;
        assert_eq!(rule_of(&s, "docs/OWNERS"), PathRule::Pending);
    }

    /// Taking a side's version is covered only where the first parent left
    /// the path as it was at the merge base (security review 2026-10, S-01).
    #[tokio::test]
    async fn merge_parent_does_not_cover_discarding_the_first_parents_change() {
        let r = repo();
        let base = commit(
            &r,
            &[],
            &[("a.rs", Some("old\n")), ("b.rs", Some("1\n"))],
            "base",
        )
        .await;
        let ours = commit(&r, &[base], &[("a.rs", Some("fixed\n"))], "ours").await;
        let theirs = commit(&r, &[base], &[("b.rs", Some("2\n"))], "theirs").await;
        let none = ReviewRules::default();
        // Their a.rs is the base's: taking it undoes ours.
        let m = commit(
            &r,
            &[ours, theirs],
            &[("a.rs", Some("old\n")), ("b.rs", Some("2\n"))],
            "m",
        )
        .await;
        let s = status(&r, Some(ours), &none, m, &[]).await;
        assert_eq!(rule_of(&s, "a.rs"), PathRule::Pending);
        assert_eq!(rule_of(&s, "b.rs"), PathRule::MergeParent);
        // The side is an ancestor of the first parent.
        let m = commit(&r, &[ours, base], &[("a.rs", Some("old\n"))], "m").await;
        let s = status(&r, Some(ours), &none, m, &[]).await;
        assert_eq!(rule_of(&s, "a.rs"), PathRule::Pending);
        // Deleting a file ours added since the base is not covered either.
        let added = commit(&r, &[base], &[("n.rs", Some("n\n"))], "added").await;
        let m = commit(&r, &[added, theirs], &[("n.rs", None)], "m").await;
        let s = status(&r, Some(added), &none, m, &[]).await;
        assert_eq!(rule_of(&s, "n.rs"), PathRule::Pending);
        // Unrelated histories: a side's new file is covered, replacing ours is not.
        let other = commit(
            &r,
            &[],
            &[("x.rs", Some("x\n")), ("a.rs", Some("other\n"))],
            "o",
        )
        .await;
        let m = commit(
            &r,
            &[ours, other],
            &[("x.rs", Some("x\n")), ("a.rs", Some("other\n"))],
            "m",
        )
        .await;
        let s = status(&r, Some(ours), &none, m, &[]).await;
        assert_eq!(rule_of(&s, "x.rs"), PathRule::MergeParent);
        assert_eq!(rule_of(&s, "a.rs"), PathRule::Pending);
    }

    #[tokio::test]
    async fn review_rows_overwrite_per_reviewer_and_are_validated() {
        let r = repo();
        let c = commit(&r, &[], &[("a", Some("1\n"))], "c").await;
        let d = commit(&r, &[c], &[("a", Some("2\n"))], "d").await;
        assert_eq!(r.get_review(c, "alice").await.unwrap(), None);
        r.put_review(c, &rev("alice", Verdict::Reject))
            .await
            .unwrap();
        r.put_review(c, &rev("bob", Verdict::Approve))
            .await
            .unwrap();
        r.put_review(c, &rev("alice", Verdict::Approve))
            .await
            .unwrap();
        assert_eq!(
            r.get_review(c, "alice").await.unwrap().unwrap().verdict,
            Verdict::Approve
        );
        assert_eq!(
            r.get_review(c, "bob").await.unwrap().unwrap().verdict,
            Verdict::Approve
        );
        // Another changeset and another reviewer are untouched.
        assert_eq!(r.get_review(d, "alice").await.unwrap(), None);
        assert_eq!(r.get_review(c, "carol").await.unwrap(), None);
        // Names that are prefixes of each other do not collide.
        r.put_review(c, &rev("al", Verdict::Reject)).await.unwrap();
        assert_eq!(
            r.get_review(c, "alice").await.unwrap().unwrap().verdict,
            Verdict::Approve
        );

        let missing = ChangesetId(Hash::of_blob(b"nope"));
        assert!(matches!(
            r.put_review(missing, &rev("alice", Verdict::Approve)).await,
            Err(Error::NotFound(_))
        ));
        let mut big = rev("alice", Verdict::Approve);
        big.comment = "x".repeat(leep_core::review::MAX_COMMENT_BYTES + 1);
        assert!(is_invalid(r.put_review(c, &big).await));
    }

    /// Records what it was asked and refuses ids in `deny`.
    struct Recorder {
        name: &'static str,
        log: Arc<Mutex<Vec<String>>>,
        deny: Mutex<Option<ChangesetId>>,
        refs: &'static [&'static str],
    }

    #[async_trait]
    impl LandPolicy for Recorder {
        fn applies_to(&self, target_ref: &str) -> bool {
            self.refs.contains(&target_ref)
        }

        async fn check_land(
            &self,
            _repo: &Repo,
            _target_ref: &str,
            head: Option<ChangesetId>,
            new_history: &[ChangesetId],
        ) -> leep_core::Result<()> {
            self.log.lock().unwrap().push(format!(
                "{} head={} n={}",
                self.name,
                head.is_some(),
                new_history.len()
            ));
            if let Some(d) = *self.deny.lock().unwrap() {
                if new_history.contains(&d) {
                    return Err(Error::Invalid(format!("{} refuses", self.name)));
                }
            }
            Ok(())
        }
    }

    fn recorder(
        name: &'static str,
        log: &Arc<Mutex<Vec<String>>>,
        refs: &'static [&'static str],
    ) -> Arc<Recorder> {
        Arc::new(Recorder {
            name,
            log: log.clone(),
            deny: Mutex::new(None),
            refs,
        })
    }

    #[tokio::test]
    async fn policies_compose_and_all_must_pass() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let (one, two) = (
            recorder("one", &log, &["main"]),
            recorder("two", &log, &["main", "rel"]),
        );
        let r = repo()
            .with_land_policy(one.clone())
            .with_land_policy(two.clone());
        let a = commit(&r, &[], &[("a", Some("1\n"))], "a").await;
        r.land(a, "main").await.unwrap();
        assert_eq!(
            *log.lock().unwrap(),
            ["one head=false n=1", "two head=false n=1"]
        );

        // The second policy vetoes: the first still ran, the ref is untouched.
        let b = commit(&r, &[a], &[("b", Some("1\n"))], "b").await;
        *two.deny.lock().unwrap() = Some(b);
        log.lock().unwrap().clear();
        match r.land(b, "main").await {
            Err(Error::Invalid(m)) => assert_eq!(m, "two refuses"),
            other => panic!("{other:?}"),
        }
        assert_eq!(
            *log.lock().unwrap(),
            ["one head=true n=1", "two head=true n=1"]
        );
        assert_eq!(r.meta().get_ref("main").await.unwrap(), Some(a));

        // The first vetoes: the second is never asked.
        *two.deny.lock().unwrap() = None;
        *one.deny.lock().unwrap() = Some(b);
        log.lock().unwrap().clear();
        match r.land(b, "main").await {
            Err(Error::Invalid(m)) => assert_eq!(m, "one refuses"),
            other => panic!("{other:?}"),
        }
        assert_eq!(*log.lock().unwrap(), ["one head=true n=1"]);

        // A ref only one policy covers is checked by that one alone.
        log.lock().unwrap().clear();
        r.land(b, "rel").await.unwrap();
        assert_eq!(*log.lock().unwrap(), ["two head=false n=2"]);
        // And raw CAS goes through the set too.
        *one.deny.lock().unwrap() = Some(b);
        assert!(r
            .cas_ref_checked("main", Some(a), Some(b), &cas_ctx())
            .await
            .is_err());
        assert!(r
            .cas_ref_checked("other", None, Some(b), &cas_ctx())
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn policy_sets_nest_and_report_head_dependence() {
        struct Head;
        #[async_trait]
        impl LandPolicy for Head {
            fn applies_to(&self, _: &str) -> bool {
                true
            }
            fn depends_on_head(&self) -> bool {
                true
            }
            async fn check_land(
                &self,
                _: &Repo,
                _: &str,
                _: Option<ChangesetId>,
                _: &[ChangesetId],
            ) -> leep_core::Result<()> {
                Ok(())
            }
        }
        let log = Arc::new(Mutex::new(Vec::new()));
        let plain = recorder("p", &log, &["main"]);
        assert!(!PolicySet::new(vec![plain.clone()]).depends_on_head());
        assert!(PolicySet::new(vec![plain, Arc::new(Head)]).depends_on_head());
        assert!(!PolicySet::new(vec![]).applies_to("main"));
    }
}

mod path_history {
    use leep_core::api::ChangesetInfo;

    use super::*;
    use crate::derived::{self, HistoryTree};
    use crate::LogFilter;

    fn ids(v: Vec<ChangesetInfo>) -> Vec<ChangesetId> {
        v.into_iter().map(|i| i.id).collect()
    }

    async fn indexed(r: &Repo, tip: ChangesetId, path: &str) -> Vec<ChangesetId> {
        ids(r.log(tip, Some(path), usize::MAX).await.unwrap())
    }

    #[tokio::test]
    async fn follows_changes_through_deletes_and_recreation() {
        let r = repo();
        let c1 = commit(&r, &[], &[("d/a", Some("1")), ("d/b", Some("1"))], "c1").await;
        let c2 = commit(&r, &[c1], &[("d/a", Some("2"))], "c2").await;
        let c3 = commit(&r, &[c2], &[("d/a", None)], "c3").await;
        let c4 = commit(&r, &[c3], &[("other", Some("1"))], "c4").await;
        let c5 = commit(&r, &[c4], &[("d/a", Some("3"))], "c5").await;
        let c6 = commit(&r, &[c5], &[("d/b", None), ("d/a", None)], "c6").await;

        assert_eq!(indexed(&r, c5, "d/a").await, [c5, c3, c2, c1]);
        // Deleting everything prunes `d` from the tree, but its log remains.
        assert_eq!(indexed(&r, c6, "d").await, [c6, c5, c3, c2, c1]);
        assert_eq!(indexed(&r, c6, "d/a").await, [c6, c5, c3, c2, c1]);
        assert_eq!(indexed(&r, c4, "d/a").await, [c3, c2, c1]);
        assert_eq!(indexed(&r, c6, "d/b").await, [c6, c1]);
        assert_eq!(indexed(&r, c6, "other").await, [c4]);
        assert!(indexed(&r, c6, "never").await.is_empty());
        assert!(indexed(&r, c6, "d/a/x").await.is_empty());
        // Slashes and limits behave like the scan.
        assert_eq!(indexed(&r, c6, "/d//a/").await, [c6, c5, c3, c2, c1]);
        assert_eq!(ids(r.log(c6, Some("d"), 2).await.unwrap()), [c6, c5]);
    }

    #[tokio::test]
    async fn file_and_directory_kind_changes_keep_both_lineages() {
        let r = repo();
        let c1 = commit(&r, &[], &[("p", Some("file"))], "c1").await;
        let c2 = commit(&r, &[c1], &[("p", None), ("p/x", Some("1"))], "c2").await;
        let c3 = commit(&r, &[c2], &[("p/y", Some("1"))], "c3").await;
        let c4 = commit(
            &r,
            &[c3],
            &[("p/x", None), ("p/y", None), ("p", Some("f"))],
            "c4",
        )
        .await;
        assert_eq!(indexed(&r, c3, "p").await, [c3, c2, c1]);
        assert_eq!(indexed(&r, c3, "p/x").await, [c2]);
        assert_eq!(indexed(&r, c4, "p").await, [c4, c3, c2, c1]);
        assert_eq!(indexed(&r, c4, "p/x").await, [c4, c2]);
    }

    #[tokio::test]
    async fn merge_keeps_both_sides_when_it_inherits_one() {
        let r = repo();
        let base = commit(&r, &[], &[("f", Some("0")), ("g", Some("0"))], "base").await;
        let a = commit(&r, &[base], &[("f", Some("same"))], "a").await;
        let b = commit(&r, &[base], &[("f", Some("same"))], "b").await;
        let c = commit(&r, &[base], &[("g", Some("1"))], "c").await;
        // Both sides made the same edit, so the merge changes nothing relative
        // to its first parent, yet `b` is still an ancestor that changed `f`.
        let m = commit(&r, &[a, b], &[], "m").await;
        assert_eq!(
            indexed(&r, m, "f").await,
            ids(r
                .log_scan(m, Some("f"), 99, &LogFilter::default())
                .await
                .unwrap())
        );
        let log = indexed(&r, m, "f").await;
        assert!(log.contains(&a) && log.contains(&b) && !log.contains(&m));
        assert_eq!(log.len(), 3);
        let (last, concurrent) = r.last_change(m, "f").await.unwrap();
        let mut both = vec![last.id];
        both.extend(concurrent);
        both.sort();
        let mut want = vec![a, b];
        want.sort();
        assert_eq!(both, want);

        // A merge that changes the path is itself the last change, and its
        // parents' histories stay reachable.
        let m2 = commit(
            &r,
            &[a, c],
            &[("g", Some("1")), ("f", Some("merged"))],
            "m2",
        )
        .await;
        assert_eq!(indexed(&r, m2, "f").await, [m2, a, base]);
        assert_eq!(indexed(&r, m2, "g").await, [m2, c, base]);
        let (last, concurrent) = r.last_change(m2, "f").await.unwrap();
        assert_eq!((last.id, concurrent), (m2, vec![]));
    }

    #[tokio::test]
    async fn last_change_of_files_directories_and_deleted_paths() {
        let r = repo();
        let c1 = commit(&r, &[], &[("src/a", Some("1")), ("src/b", Some("1"))], "c1").await;
        let c2 = commit(&r, &[c1], &[("src/a", Some("2"))], "c2").await;
        let c3 = commit(&r, &[c2], &[("src/b", None)], "c3").await;
        let id = |p: &str| {
            let r = r.clone();
            let p = p.to_string();
            async move { r.last_change(c3, &p).await.unwrap().0.id }
        };
        assert_eq!(id("src/a").await, c2);
        assert_eq!(id("src/b").await, c3);
        assert_eq!(id("src").await, c3);
        assert_eq!(r.last_change(c2, "src/b").await.unwrap().0.id, c1);
        assert_eq!(r.last_change(c3, "src/a/").await.unwrap().0.generation, 2);
        assert!(matches!(
            r.last_change(c3, "nope").await,
            Err(Error::NotFound(_))
        ));
    }

    #[tokio::test]
    async fn derivation_is_deterministic_and_idempotent() {
        let a = repo();
        let b = repo();
        let mut tips = Vec::new();
        for r in [&a, &b] {
            let c1 = commit(r, &[], &[("x/1", Some("1"))], "c1").await;
            let c2 = commit(r, &[c1], &[("x/2", Some("1"))], "c2").await;
            let c3 = commit(r, &[c1], &[("x/1", Some("3"))], "c3").await;
            let m = commit(r, &[c2, c3], &[], "m").await;
            tips.push(m);
        }
        assert_eq!(tips[0], tips[1]);
        let first = derived::derive::<HistoryTree>(&a, tips[0]).await.unwrap();
        assert_eq!(
            first,
            derived::derive::<HistoryTree>(&a, tips[0]).await.unwrap()
        );
        assert_eq!(
            first,
            derived::derive::<HistoryTree>(&b, tips[1]).await.unwrap()
        );
        // A second derivation after the row is gone writes the same value.
        let again = derived::derive::<HistoryTree>(&b, tips[1]).await.unwrap();
        assert_eq!(first, again);
    }

    #[tokio::test]
    async fn deriving_a_deep_history_is_iterative() {
        let r = repo();
        let meta = r.meta().clone();
        let mut parents = vec![];
        let mut tip = None;
        for i in 0..12_000u32 {
            let mut cs = changeset(&parents, BTreeMap::new(), &format!("c{i}"));
            cs.changes.insert(
                rp(&format!("d{}/f{}", i % 3, i % 10)),
                FileChange::Modify {
                    content: Hash::of_blob(format!("{i}").as_bytes()),
                    kind: FileKind::Regular,
                    size: 1,
                },
            );
            let id = meta.put_changeset(&cs).await.unwrap();
            parents = vec![id];
            tip = Some(id);
        }
        let tip = tip.unwrap();
        let log = r.log(tip, Some("d0/f0"), 3).await.unwrap();
        // i % 3 == 0 and i % 10 == 0, newest first.
        let gens: Vec<u64> = log.iter().map(|i| i.generation).collect();
        assert_eq!(gens, [11_971, 11_941, 11_911]);
    }

    /// Deterministic generator for the property test below.
    struct Rng(u64);

    impl Rng {
        fn below(&mut self, n: usize) -> usize {
            self.0 = self
                .0
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            ((self.0 >> 33) as usize) % n
        }
    }

    const POOL: &[&str] = &[
        "a", "a/b", "a/c", "d/x", "d/y/z", "d/y/w", "d/y", "e", "e/f/g", "e/f", "h",
    ];

    /// A random changeset on `parents[0]`: edits of pool paths (sometimes to
    /// content already there, so no-ops happen), and deletes of existing files.
    /// Retries when the result would clash a file with a directory.
    async fn random_commit(
        r: &Repo,
        rng: &mut Rng,
        pool: &[&str],
        max_ops: usize,
        parents: &[ChangesetId],
        n: usize,
    ) -> ChangesetId {
        loop {
            let existing: Vec<String> = match parents.first() {
                Some(p) => r
                    .list_files(*p, "")
                    .await
                    .unwrap()
                    .into_iter()
                    .map(|f| f.path.as_str().to_string())
                    .collect(),
                None => vec![],
            };
            let mut changes = BTreeMap::new();
            let ops = if parents.len() > 1 {
                rng.below(max_ops)
            } else {
                1 + rng.below(max_ops)
            };
            for _ in 0..ops {
                let path = rp(pool[rng.below(pool.len())]);
                let change = if rng.below(4) == 0 && !existing.is_empty() {
                    let victim = rp(&existing[rng.below(existing.len())]);
                    changes.insert(victim, FileChange::Delete);
                    continue;
                } else {
                    // Few distinct contents, so edits often repeat.
                    modify(r, format!("v{}", rng.below(3)).as_bytes()).await
                };
                changes.insert(path, change);
            }
            let mut cs = changeset(parents, changes, &format!("c{n}"));
            cs.author = ["alice", "bob"][rng.below(2)].into();
            if let Ok(id) = r.create_changeset(cs).await {
                return id;
            }
        }
    }

    /// The index must agree with the scan of all ancestors on random DAGs with
    /// merges (including empty ones), deletes, no-op edits, re-creation and
    /// file/directory kind changes.
    #[tokio::test]
    async fn indexed_log_matches_the_scan_on_random_histories() {
        let mut queries = vec!["", "a", "d", "d/y", "e", "e/f", "never"];
        queries.extend(POOL);
        check_index_against_scan(0xfa57109, POOL, 3, &queries).await;
    }

    /// The same with a directory of about 150 names, so its history node is
    /// split into chunks (ADR 0027) and edits, merges and deletes go through
    /// chunk rewrites and chunk lookups.
    #[tokio::test]
    async fn indexed_log_matches_the_scan_with_wide_directories() {
        let wide: Vec<String> = (0..150)
            .map(|i| format!("w/f{i:03}"))
            .chain(["w/k".into(), "w/k/x".into(), "w/m/n".into()])
            .collect();
        let mut pool: Vec<&str> = POOL.to_vec();
        pool.extend(wide.iter().map(|s| s.as_str()));
        let mut queries = vec!["", "w", "w/m", "w/f", "w/f1500", "a", "d/y"];
        queries.extend(wide.iter().step_by(25).map(|s| s.as_str()));
        queries.extend(["w/f149", "w/k", "w/k/x", "w/m/n"]);
        let split = check_index_against_scan(0x5eed_c4a7, &pool, 12, &queries).await;
        assert!(split, "no history node was split");
    }

    /// Build random histories over `pool` and compare indexed answers for
    /// `queries` with the scan. Says whether any history node was split.
    async fn check_index_against_scan(
        seed: u64,
        pool: &[&str],
        max_ops: usize,
        queries: &[&str],
    ) -> bool {
        let mut rng = Rng(seed);
        let mut split = false;
        for round in 0..6 {
            let r = repo();
            let mut all: Vec<ChangesetId> = Vec::new();
            for n in 0..90 {
                let parents: Vec<ChangesetId> = match (all.len(), rng.below(10)) {
                    (0, _) => vec![],
                    (len, 0..=2) if len >= 2 => {
                        let a = all[len - 1 - rng.below(len.min(10))];
                        let b = all[rng.below(len)];
                        let mut p = vec![a];
                        if b != a {
                            p.push(b);
                        }
                        if rng.below(8) == 0 {
                            let c = all[rng.below(len)];
                            if !p.contains(&c) {
                                p.push(c);
                            }
                        }
                        p
                    }
                    (len, _) => vec![all[len - 1 - rng.below(len.min(6))]],
                };
                all.push(random_commit(&r, &mut rng, pool, max_ops, &parents, n).await);
            }
            let alice = LogFilter {
                author: Some("alice".into()),
                ..Default::default()
            };
            for _ in 0..12 {
                let tip = all[all.len() - 1 - rng.below(all.len().min(30))];
                for path in queries {
                    for (limit, filter) in [
                        (usize::MAX, &LogFilter::default()),
                        (3, &LogFilter::default()),
                        (usize::MAX, &alice),
                    ] {
                        let want = r.log_scan(tip, Some(path), limit, filter).await.unwrap();
                        let got = r
                            .log_filtered(tip, Some(path), limit, filter)
                            .await
                            .unwrap();
                        assert_eq!(
                            ids(got),
                            ids(want),
                            "round {round}, tip {tip}, path {path:?}"
                        );
                    }
                    if path.is_empty() {
                        continue;
                    }
                    let want = r
                        .log_scan(tip, Some(path), 1, &LogFilter::default())
                        .await
                        .unwrap();
                    match r.last_change(tip, path).await {
                        Ok((last, _)) => assert_eq!(vec![last.id], ids(want)),
                        Err(Error::NotFound(_)) => assert!(want.is_empty()),
                        Err(e) => panic!("{e}"),
                    }
                }
            }
            let rows = r
                .meta()
                .list_derived(crate::derived::HISTORY_NODE_KIND, None, usize::MAX)
                .await
                .unwrap();
            split |= rows.iter().any(|(_, v)| {
                matches!(
                    crate::derived::StoredHistoryNode::decode(v),
                    Ok(crate::derived::StoredHistoryNode::Split(_))
                )
            });
        }
        split
    }
}

mod rename_follow {
    use leep_core::api::{ChangesetInfo, DiffEntry};
    use leep_core::rename::RenameOptions;

    use super::*;

    fn lines(n: usize, tag: &str) -> String {
        (0..n).map(|i| format!("{tag} line {i}\n")).collect()
    }

    fn ids(v: &[ChangesetInfo]) -> Vec<ChangesetId> {
        v.iter().map(|i| i.id).collect()
    }

    fn names(v: &[ChangesetInfo]) -> Vec<&str> {
        v.iter()
            .map(|i| i.path_at.as_ref().expect("path_at").as_str())
            .collect()
    }

    async fn follow(r: &Repo, tip: ChangesetId, path: &str) -> Vec<ChangesetInfo> {
        r.log_follow(tip, path, 100, &crate::LogFilter::default())
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn detect_renames_over_a_diff() {
        let r = repo();
        let body = lines(10, "x");
        let c1 = commit(
            &r,
            &[],
            &[("a.txt", Some(&body)), ("b.txt", Some(&lines(10, "y")))],
            "c1",
        )
        .await;
        let edited = format!("{body}extra\n");
        let c2 = commit(
            &r,
            &[c1],
            &[
                ("a.txt", None),
                ("moved/a.txt", Some(&edited)),
                ("b.txt", None),
                ("moved/b.txt", Some(&lines(10, "y"))),
            ],
            "c2",
        )
        .await;
        let entries: Vec<DiffEntry> = r.diff(Some(c1), c2, "").await.unwrap();
        assert_eq!(
            entries.len(),
            4,
            "the entries keep their delete + add shape"
        );
        let d = r
            .detect_renames(&entries, &RenameOptions::default())
            .await
            .unwrap();
        let got: Vec<_> = d
            .renames
            .iter()
            .map(|r| (r.from.as_str(), r.to.as_str(), r.exact))
            .collect();
        assert_eq!(
            got,
            [
                ("a.txt", "moved/a.txt", false),
                ("b.txt", "moved/b.txt", true)
            ]
        );
        assert!(d.renames[0].similarity >= 90 && d.renames[1].similarity == 100);
    }

    #[tokio::test]
    async fn follow_one_rename() {
        let r = repo();
        let body = lines(10, "x");
        let c1 = commit(&r, &[], &[("a.txt", Some(&body))], "add").await;
        let c2 = commit(&r, &[c1], &[("a.txt", Some(&format!("{body}2\n")))], "edit").await;
        let c3 = commit(
            &r,
            &[c2],
            &[("a.txt", None), ("b.txt", Some(&format!("{body}2\n")))],
            "rename",
        )
        .await;
        let c4 = commit(&r, &[c3], &[("b.txt", Some(&format!("{body}3\n")))], "edit").await;
        let c5 = commit(&r, &[c4], &[("other", Some("o"))], "unrelated").await;

        // Without follow the history stops at the rename.
        let plain = r.log(c5, Some("b.txt"), 100).await.unwrap();
        assert_eq!(ids(&plain), [c4, c3]);
        assert!(plain
            .iter()
            .all(|i| i.path_at.is_none() && i.renamed.is_none()));

        let log = follow(&r, c5, "b.txt").await;
        assert_eq!(ids(&log), [c4, c3, c2, c1]);
        assert_eq!(names(&log), ["b.txt", "b.txt", "a.txt", "a.txt"]);
        let ren = log[1].renamed.as_ref().expect("c3 renamed the file");
        assert_eq!((ren.from.as_str(), ren.to.as_str()), ("a.txt", "b.txt"));
        assert!(ren.exact && ren.similarity == 100);
        assert!(log[0].renamed.is_none() && log[2].renamed.is_none());
        // Entries are the same changesets the old name's own log has.
        assert_eq!(
            ids(&log[2..]),
            ids(&r.log(c2, Some("a.txt"), 100).await.unwrap())
        );

        // limit and filters count yielded entries, not walked ones.
        let two = r
            .log_follow(c5, "b.txt", 2, &crate::LogFilter::default())
            .await
            .unwrap();
        assert_eq!(ids(&two), [c4, c3]);
        let only_edits = crate::LogFilter {
            author: Some("nobody".into()),
            ..Default::default()
        };
        assert!(r
            .log_follow(c5, "b.txt", 100, &only_edits)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn follow_several_renames_and_moves_between_directories() {
        let r = repo();
        let body = lines(12, "x");
        let c1 = commit(&r, &[], &[("src/a.rs", Some(&body))], "c1").await;
        let c2 = commit(
            &r,
            &[c1],
            &[("src/a.rs", None), ("src/b.rs", Some(&body))],
            "a to b",
        )
        .await;
        let c3 = commit(
            &r,
            &[c2],
            &[("src/b.rs", Some(&format!("{body}1\n")))],
            "c3",
        )
        .await;
        let c4 = commit(
            &r,
            &[c3],
            &[
                ("src/b.rs", None),
                ("lib/c.rs", Some(&format!("{body}1\n"))),
            ],
            "b to c",
        )
        .await;
        let c5 = commit(
            &r,
            &[c4],
            &[
                ("lib/c.rs", None),
                ("lib/d.rs", Some(&format!("{body}1\n"))),
            ],
            "c to d",
        )
        .await;
        let log = follow(&r, c5, "lib/d.rs").await;
        assert_eq!(ids(&log), [c5, c4, c3, c2, c1]);
        assert_eq!(
            names(&log),
            ["lib/d.rs", "lib/c.rs", "src/b.rs", "src/b.rs", "src/a.rs"]
        );
        let renamed: Vec<_> = log
            .iter()
            .map(|i| i.renamed.as_ref().map(|r| (r.from.as_str(), r.to.as_str())))
            .collect();
        assert_eq!(
            renamed,
            [
                Some(("lib/c.rs", "lib/d.rs")),
                Some(("src/b.rs", "lib/c.rs")),
                None,
                Some(("src/a.rs", "src/b.rs")),
                None
            ]
        );
        // Following from the middle starts at that name.
        let mid = follow(&r, c3, "src/b.rs").await;
        assert_eq!(ids(&mid), [c3, c2, c1]);
    }

    #[tokio::test]
    async fn follow_rename_with_an_edit_in_the_same_changeset() {
        let r = repo();
        let body = lines(10, "x");
        let c1 = commit(&r, &[], &[("a", Some(&body))], "c1").await;
        let mut edited: Vec<String> = body.lines().map(|l| format!("{l}\n")).collect();
        edited[0] = "changed\n".into();
        edited[1] = "changed too\n".into();
        let c2 = commit(
            &r,
            &[c1],
            &[("a", None), ("b", Some(&edited.concat()))],
            "rename and edit",
        )
        .await;
        let log = follow(&r, c2, "b").await;
        assert_eq!(ids(&log), [c2, c1]);
        let ren = log[0].renamed.as_ref().unwrap();
        assert!(!ren.exact && (50..100).contains(&ren.similarity), "{ren:?}");

        // Rewriting most of the file is a new file, not a rename.
        let c3 = commit(
            &r,
            &[c2],
            &[("b", None), ("c", Some(&lines(10, "wholly different")))],
            "replace",
        )
        .await;
        let log = follow(&r, c3, "c").await;
        assert_eq!(ids(&log), [c3]);
        assert!(log[0].renamed.is_none());
    }

    #[tokio::test]
    async fn follow_without_renames_matches_the_plain_path_log() {
        let r = repo();
        let c1 = commit(&r, &[], &[("a", Some("1\n")), ("d/b", Some("1\n"))], "c1").await;
        let c2 = commit(&r, &[c1], &[("a", Some("2\n"))], "c2").await;
        let c3 = commit(&r, &[c2], &[("d/b", Some("2\n"))], "c3").await;
        let c4 = commit(&r, &[c3], &[("a", None)], "delete a").await;
        for path in ["a", "d/b", "missing"] {
            let plain = ids(&r.log(c4, Some(path), 100).await.unwrap());
            let followed = follow(&r, c4, path).await;
            assert_eq!(ids(&followed), plain, "{path}");
            assert!(followed
                .iter()
                .all(|i| i.path_at.as_ref().unwrap().as_str() == path));
        }
        // A deleted file's log starts at the deletion.
        assert_eq!(ids(&follow(&r, c4, "a").await), [c4, c2, c1]);
    }

    #[tokio::test]
    async fn follow_rejects_directories() {
        let r = repo();
        let c1 = commit(&r, &[], &[("d/a", Some("1\n"))], "c1").await;
        for dir in ["d", "d/"] {
            let e = r
                .log_follow(c1, dir, 10, &crate::LogFilter::default())
                .await;
            assert!(is_invalid(e), "{dir}");
        }
        let e = r.log_follow(c1, "", 10, &crate::LogFilter::default()).await;
        assert!(matches!(e, Err(Error::InvalidPath(..))));
    }

    #[tokio::test]
    async fn follow_across_merges() {
        let r = repo();
        let body = lines(10, "x");
        let base = commit(
            &r,
            &[],
            &[("a.txt", Some(&body)), ("side", Some("s0\n"))],
            "base",
        )
        .await;
        let renamed = commit(
            &r,
            &[base],
            &[("a.txt", None), ("b.txt", Some(&body))],
            "rename on one branch",
        )
        .await;
        let other = commit(&r, &[base], &[("side", Some("s1\n"))], "other branch").await;

        // Rename on the first-parent side: the merge leaves b.txt alone.
        let MergeResponse::Merged { id: m1 } = r.merge(merge_req(renamed, other)).await.unwrap()
        else {
            panic!("clean merge")
        };
        assert_eq!(ids(&follow(&r, m1, "b.txt").await), [renamed, base]);

        // Rename on the merged-in side: relative to the first parent the
        // merge itself deletes a.txt and adds b.txt.
        let MergeResponse::Merged { id: m2 } = r.merge(merge_req(other, renamed)).await.unwrap()
        else {
            panic!("clean merge")
        };
        let log = follow(&r, m2, "b.txt").await;
        assert_eq!(ids(&log), [m2, renamed, base]);
        assert_eq!(
            log[0].renamed.as_ref().map(|r| r.from.as_str()),
            Some("a.txt")
        );
        // The side branch's own commit is listed once, with its rename.
        assert_eq!(
            log[1].renamed.as_ref().map(|r| r.from.as_str()),
            Some("a.txt")
        );
        assert_eq!(names(&log), ["b.txt", "b.txt", "a.txt"]);

        // An edit made after the merge extends the same history.
        let c = commit(&r, &[m2], &[("b.txt", Some(&format!("{body}!\n")))], "edit").await;
        assert_eq!(ids(&follow(&r, c, "b.txt").await), [c, m2, renamed, base]);
    }

    #[tokio::test]
    async fn follow_keeps_both_lines_of_history_through_a_merge() {
        // Both branches edit the file before one of them renames it.
        let r = repo();
        let body = lines(10, "x");
        let base = commit(&r, &[], &[("a.txt", Some(&body)), ("o", Some("0"))], "base").await;
        let left = commit(&r, &[base], &[("o", Some("1"))], "left").await;
        let right = commit(
            &r,
            &[base],
            &[("a.txt", None), ("b.txt", Some(&body))],
            "right renames",
        )
        .await;
        let after = commit(
            &r,
            &[right],
            &[("b.txt", Some(&format!("{body}x\n")))],
            "after",
        )
        .await;
        let MergeResponse::Merged { id: m } = r.merge(merge_req(after, left)).await.unwrap() else {
            panic!("clean merge")
        };
        let log = follow(&r, m, "b.txt").await;
        assert_eq!(ids(&log), [after, right, base]);
    }

    #[tokio::test]
    async fn follow_a_deep_history_is_iterative() {
        let r = repo();
        let body = lines(10, "x");
        let mut tip = commit(&r, &[], &[("a", Some(&body))], "add").await;
        tip = commit(&r, &[tip], &[("a", None), ("b", Some(&body))], "rename").await;
        for i in 0..2000 {
            tip = commit(&r, &[tip], &[("b", Some(&format!("{body}{i}\n")))], "edit").await;
        }
        let log = r
            .log_follow(tip, "b", 5000, &crate::LogFilter::default())
            .await
            .unwrap();
        assert_eq!(log.len(), 2002);
        assert_eq!(log[2001].path_at.as_ref().unwrap().as_str(), "a");
    }
}

mod ref_rules {
    use super::*;

    #[tokio::test]
    async fn invalid_names_are_rejected_on_raw_updates_and_land() {
        let r = repo();
        let a = commit(&r, &[], &[("a", Some("1\n"))], "a").await;
        for bad in ["", "a b", "x..y", "/x", "x.lock", "-x", "a\\b"] {
            assert!(
                matches!(
                    r.cas_ref_checked(bad, None, Some(a), &cas_ctx()).await,
                    Err(Error::Invalid(_))
                ),
                "cas {bad:?}"
            );
            assert!(
                matches!(r.land(a, bad).await, Err(Error::Invalid(_))),
                "land {bad:?}"
            );
        }
        assert!(r.meta().list_refs().await.unwrap().is_empty());
        assert!(r
            .cas_ref_checked("team/ok-1.0", None, Some(a), &cas_ctx())
            .await
            .unwrap());
        r.land(a, "release/1.0").await.unwrap();
    }

    #[tokio::test]
    async fn a_legacy_invalid_ref_can_still_be_deleted() {
        let r = repo();
        let a = commit(&r, &[], &[("a", Some("1\n"))], "a").await;
        // Written behind the checked path, as an older server could have.
        assert!(r.meta().cas_ref("odd name", None, Some(a)).await.unwrap());
        assert!(r
            .cas_ref_checked("odd name", Some(a), None, &cas_ctx())
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn tags_can_be_created_and_deleted_but_never_moved() {
        let r = repo();
        let a = commit(&r, &[], &[("a", Some("1\n"))], "a").await;
        let b = commit(&r, &[a], &[("a", Some("2\n"))], "b").await;
        assert!(r
            .cas_ref_checked("tags/v1", None, Some(a), &cas_ctx())
            .await
            .unwrap());
        // Creating over an existing tag loses the CAS, like any ref.
        assert!(!r
            .cas_ref_checked("tags/v1", None, Some(b), &cas_ctx())
            .await
            .unwrap());
        match r
            .cas_ref_checked("tags/v1", Some(a), Some(b), &cas_ctx())
            .await
        {
            Err(Error::Invalid(m)) => assert!(m.contains("immutable"), "{m}"),
            other => panic!("{other:?}"),
        }
        assert_eq!(r.meta().get_ref("tags/v1").await.unwrap(), Some(a));
        assert!(r
            .cas_ref_checked("tags/v1", Some(a), None, &cas_ctx())
            .await
            .unwrap());
        assert!(r
            .cas_ref_checked("tags/v1", None, Some(b), &cas_ctx())
            .await
            .unwrap());
        assert_eq!(r.meta().get_ref("tags/v1").await.unwrap(), Some(b));
        // Branches still move.
        assert!(r
            .cas_ref_checked("main", None, Some(a), &cas_ctx())
            .await
            .unwrap());
        assert!(r
            .cas_ref_checked("main", Some(a), Some(b), &cas_ctx())
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn rule_violations_record_nothing() {
        let r = repo();
        let a = commit(&r, &[], &[("a", Some("1\n"))], "a").await;
        let b = commit(&r, &[a], &[("a", Some("2\n"))], "b").await;
        assert!(r
            .cas_ref_checked("tags/v1", None, Some(a), &cas_ctx())
            .await
            .unwrap());
        assert!(r
            .cas_ref_checked("tags/v1", Some(a), Some(b), &cas_ctx())
            .await
            .is_err());
        assert!(r
            .cas_ref_checked("bad name", None, Some(a), &cas_ctx())
            .await
            .is_err());
        assert!(r.land(a, "tags/v1").await.is_err());
        assert!(r.land(a, "bad name").await.is_err());
        let log = r.meta().ref_log(None, None, 100).await.unwrap();
        assert_eq!(log.len(), 1, "only the tag creation: {log:?}");
        assert_eq!(log[0].name, "tags/v1");
    }

    #[tokio::test]
    async fn land_onto_a_tag_is_refused() {
        let r = repo();
        let a = commit(&r, &[], &[("a", Some("1\n"))], "a").await;
        match r.land(a, "tags/v1").await {
            Err(Error::Invalid(m)) => assert!(m.contains("tags are immutable"), "{m}"),
            other => panic!("{other:?}"),
        }
        assert_eq!(r.meta().get_ref("tags/v1").await.unwrap(), None);
    }

    #[tokio::test]
    async fn land_without_create_refuses_a_missing_ref_and_logs_nothing() {
        let r = repo();
        let a = commit(&r, &[], &[("a", Some("1\n"))], "a").await;
        let b = commit(&r, &[a], &[("a", Some("2\n"))], "b").await;
        match r.land_with(a, "typo", None, false).await {
            Err(Error::NotFound(m)) => {
                assert!(m.contains(leep_core::refname::MISSING_REF_MARKER), "{m}")
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(r.meta().get_ref("typo").await.unwrap(), None);
        assert!(r.meta().ref_log(None, None, 10).await.unwrap().is_empty());

        // Allowed to create: it does, and then `create` no longer matters.
        r.land_with(a, "feature", None, true).await.unwrap();
        assert_eq!(r.meta().get_ref("feature").await.unwrap(), Some(a));
        let resp = r.land_with(b, "feature", None, false).await.unwrap();
        assert!(matches!(resp, LandResponse::Landed { landed, .. } if landed == b));
        // The old entry points keep creating.
        r.land(a, "legacy").await.unwrap();
        assert_eq!(r.meta().get_ref("legacy").await.unwrap(), Some(a));
    }

    #[tokio::test]
    async fn resolve_finds_tags_by_short_name_after_branches_and_ids() {
        let r = repo();
        let a = commit(&r, &[], &[("a", Some("1\n"))], "a").await;
        let b = commit(&r, &[a], &[("a", Some("2\n"))], "b").await;
        r.cas_ref_checked("tags/v1", None, Some(a), &cas_ctx())
            .await
            .unwrap();
        assert_eq!(r.resolve("tags/v1").await.unwrap(), a);
        assert_eq!(r.resolve("v1").await.unwrap(), a);
        // A branch of the same name wins over the tag.
        r.cas_ref_checked("v1", None, Some(b), &cas_ctx())
            .await
            .unwrap();
        assert_eq!(r.resolve("v1").await.unwrap(), b);
        assert_eq!(r.resolve("tags/v1").await.unwrap(), a);
        assert!(matches!(r.resolve("nope").await, Err(Error::NotFound(_))));
    }
}

mod ref_log {
    use leep_core::RefUpdateContext;

    use super::*;

    async fn log(r: &Repo) -> Vec<leep_core::RefLogEntry> {
        r.meta().ref_log(None, None, 100).await.unwrap()
    }

    #[tokio::test]
    async fn land_records_actor_operation_and_the_landed_changeset() {
        let r = repo();
        let a = commit(&r, &[], &[("a", Some("1\n"))], "a").await;
        let b = commit(&r, &[a], &[("a", Some("2\n"))], "b").await;
        r.land_as(a, "main", Some("alice")).await.unwrap();
        r.land(b, "main").await.unwrap();
        // Landing what the ref already has is not a move.
        r.land_as(a, "main", Some("alice")).await.unwrap();
        let entries = log(&r).await;
        assert_eq!(entries.len(), 2);
        let (newer, older) = (&entries[0], &entries[1]);
        assert_eq!((older.old, older.new), (None, Some(a)));
        assert_eq!((newer.old, newer.new), (Some(a), Some(b)));
        assert_eq!(older.actor.as_deref(), Some("alice"));
        assert_eq!(newer.actor, None);
        assert_eq!(older.operation, "land");
        assert_eq!(
            newer.message.as_deref(),
            Some(format!("landed {b}").as_str())
        );
    }

    #[tokio::test]
    async fn a_rebasing_land_records_the_submitted_id_and_the_new_head() {
        let r = repo();
        let base = commit(&r, &[], &[("a", Some("1\n"))], "base").await;
        let left = commit(&r, &[base], &[("l", Some("l\n"))], "left").await;
        let right = commit(&r, &[base], &[("r", Some("r\n"))], "right").await;
        r.land(left, "main").await.unwrap();
        let LandResponse::Landed {
            landed, rebased, ..
        } = r.land(right, "main").await.unwrap()
        else {
            panic!("expected a clean land");
        };
        assert!(rebased);
        let entries = log(&r).await;
        assert_eq!(entries[0].new, Some(landed));
        assert_eq!(entries[0].old, Some(left));
        assert_eq!(
            entries[0].message.as_deref(),
            Some(format!("landed {right}").as_str())
        );
    }

    #[tokio::test]
    async fn checked_cas_records_the_context_it_is_given() {
        let r = repo();
        let a = commit(&r, &[], &[("a", Some("1\n"))], "a").await;
        let ctx = RefUpdateContext::new("undo")
            .actor(Some("bob"))
            .message("undo of seq 3");
        assert!(r.cas_ref_checked("dev", None, Some(a), &ctx).await.unwrap());
        assert!(r
            .cas_ref_checked("dev", Some(a), None, &cas_ctx())
            .await
            .unwrap());
        let entries = log(&r).await;
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].operation, "cas");
        assert_eq!((entries[0].old, entries[0].new), (Some(a), None));
        assert_eq!(entries[1].operation, "undo");
        assert_eq!(entries[1].actor.as_deref(), Some("bob"));
        assert_eq!(entries[1].message.as_deref(), Some("undo of seq 3"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_lands_log_one_entry_each_and_replay_to_the_head() {
        let r = Arc::new(repo());
        let base = commit(&r, &[], &[("base", Some("b\n"))], "base").await;
        r.land(base, "main").await.unwrap();
        let mut tasks = Vec::new();
        for i in 0..16 {
            let r = r.clone();
            tasks.push(tokio::spawn(async move {
                let name = format!("f{i}");
                let c = commit(&r, &[base], &[(&name, Some("x\n"))], &name).await;
                r.land_as(c, "main", Some("w")).await.unwrap();
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }
        let entries = log(&r).await;
        assert_eq!(entries.len(), 17, "base plus one per land");
        let mut head = None;
        for e in entries.iter().rev() {
            assert_eq!(e.old, head);
            head = e.new;
        }
        assert_eq!(head, r.meta().get_ref("main").await.unwrap());
    }
}

mod warm {
    use std::sync::atomic::Ordering;

    use leep_testkit::{FaultPlan, FaultyMetaStore};

    use super::*;
    use crate::derived::{self, DerivedKind, HistoryTree};
    use crate::warm::{WarmReport, Warmer};

    async fn has_history(r: &Repo, id: ChangesetId) -> bool {
        r.meta()
            .get_derived(HistoryTree::NAME, &id.0)
            .await
            .unwrap()
            .is_some()
    }

    #[tokio::test]
    async fn a_pass_warms_moved_heads_once() {
        let r = repo();
        let mut tips = Vec::new();
        let mut prev = None;
        for i in 0..20 {
            let parents: Vec<_> = prev.into_iter().collect();
            let id = commit(&r, &parents, &[("f", Some(&i.to_string()))], "c").await;
            tips.push(id);
            prev = Some(id);
        }
        r.meta()
            .cas_ref("main", None, Some(tips[19]))
            .await
            .unwrap();
        r.meta().cas_ref("old", None, Some(tips[5])).await.unwrap();
        assert!(!has_history(&r, tips[19]).await);

        let mut w = Warmer::new(r.clone(), true);
        let report = w.pass().await.unwrap();
        assert_eq!(
            report,
            WarmReport {
                refs: 2,
                warmed: 2,
                failed: 0
            }
        );
        for &id in &tips {
            assert!(has_history(&r, id).await, "every ancestor is indexed");
        }
        let calls = r.derive_calls.load(Ordering::Relaxed);

        // Nothing moved: nothing to do.
        assert_eq!(w.pass().await.unwrap().warmed, 0);
        assert_eq!(r.derive_calls.load(Ordering::Relaxed), calls);

        // A new head costs only its own changeset.
        let next = commit(&r, &[tips[19]], &[("g", Some("1"))], "next").await;
        r.meta()
            .cas_ref("main", Some(tips[19]), Some(next))
            .await
            .unwrap();
        r.meta().cas_ref("old", Some(tips[5]), None).await.unwrap();
        let report = w.pass().await.unwrap();
        assert_eq!(
            report,
            WarmReport {
                refs: 1,
                warmed: 1,
                failed: 0
            }
        );
        assert!(has_history(&r, next).await);
        // History, generation, skiplist, last-merge and root tree: a few per kind.
        assert!(r.derive_calls.load(Ordering::Relaxed) - calls <= 8);
    }

    /// A query and the warmer deriving the same history at once derive each
    /// changeset once: the second waits on the kind's lock, then finds the rows.
    #[tokio::test]
    async fn concurrent_derivations_do_not_repeat_work() {
        let plan = FaultPlan::new(7);
        plan.jitter(4);
        let r = Repo::new(
            Arc::new(MemoryBlobStore::new()),
            Arc::new(FaultyMetaStore::new(
                Arc::new(MemoryMetaStore::new()),
                plan.clone(),
            )),
        );
        let base = commit(&r, &[], &[("a", Some("0"))], "base").await;
        let (mut left, mut right) = (base, base);
        for i in 0..30 {
            left = commit(&r, &[left], &[("l", Some(&i.to_string()))], "l").await;
            right = commit(&r, &[right], &[("r", Some(&i.to_string()))], "r").await;
        }
        let tip = commit(&r, &[left, right], &[], "merge").await;
        let before = r.derive_calls.load(Ordering::Relaxed);
        let (a, b) = tokio::join!(
            derived::derive::<HistoryTree>(&r, tip),
            derived::derive::<HistoryTree>(&r, tip)
        );
        assert_eq!(a.unwrap(), b.unwrap());
        let history_values = r
            .meta()
            .list_derived(HistoryTree::NAME, None, usize::MAX)
            .await
            .unwrap()
            .len();
        assert_eq!(history_values, 62);
        // Each changeset's history value was computed once; the other calls
        // are the kinds it needs (generation, skiplist, last-merge) once each.
        let calls = r.derive_calls.load(Ordering::Relaxed) - before;
        let alone = {
            let r2 = repo();
            let base = commit(&r2, &[], &[("a", Some("0"))], "base").await;
            let (mut left, mut right) = (base, base);
            for i in 0..30 {
                left = commit(&r2, &[left], &[("l", Some(&i.to_string()))], "l").await;
                right = commit(&r2, &[right], &[("r", Some(&i.to_string()))], "r").await;
            }
            let tip = commit(&r2, &[left, right], &[], "merge").await;
            let before = r2.derive_calls.load(Ordering::Relaxed);
            derived::derive::<HistoryTree>(&r2, tip).await.unwrap();
            r2.derive_calls.load(Ordering::Relaxed) - before
        };
        assert_eq!(
            calls, alone,
            "two concurrent derivations did the work of one"
        );
    }
}

/// Changeset creation reads its inputs in batches and writes the changeset
/// with its rows in one call (ADR 0031). The rows must be exactly what
/// deriving them one by one computes, with or without working batched reads.
mod create_rows {
    use std::collections::BTreeMap;

    use async_trait::async_trait;
    use leep_core::{DerivedRow, MetaStore, RefLogEntry, RefUpdateContext, Result};

    use super::*;
    use crate::derived::{self, DerivedKind, Generation, RootTree, SkipEntry, Skiplist};
    use crate::REBASED_ORIGIN_KIND;

    /// Refuses every batched read, so creation must fall back to single
    /// reads; batched writes take the trait's default (one call per item).
    struct NoBatchedReads(Arc<dyn MetaStore>);

    #[async_trait]
    impl MetaStore for NoBatchedReads {
        async fn put_changeset(&self, cs: &Changeset) -> Result<ChangesetId> {
            self.0.put_changeset(cs).await
        }
        async fn get_changeset(&self, id: &ChangesetId) -> Result<Option<Changeset>> {
            self.0.get_changeset(id).await
        }
        async fn get_changesets(&self, _: &[ChangesetId]) -> Result<Vec<Option<Changeset>>> {
            Err(Error::Storage("no batches".into()))
        }
        async fn list_changesets(
            &self,
            after: Option<ChangesetId>,
            limit: usize,
        ) -> Result<Vec<ChangesetId>> {
            self.0.list_changesets(after, limit).await
        }
        async fn get_ref(&self, name: &str) -> Result<Option<ChangesetId>> {
            self.0.get_ref(name).await
        }
        async fn cas_ref_logged(
            &self,
            name: &str,
            expected: Option<ChangesetId>,
            new: Option<ChangesetId>,
            ctx: &RefUpdateContext,
        ) -> Result<bool> {
            self.0.cas_ref_logged(name, expected, new, ctx).await
        }
        async fn ref_log(
            &self,
            name: Option<&str>,
            before_seq: Option<u64>,
            limit: usize,
        ) -> Result<Vec<RefLogEntry>> {
            self.0.ref_log(name, before_seq, limit).await
        }
        async fn list_refs(&self) -> Result<BTreeMap<String, ChangesetId>> {
            self.0.list_refs().await
        }
        async fn list_derived(
            &self,
            kind: &str,
            after: Option<Hash>,
            limit: usize,
        ) -> Result<Vec<(Hash, Bytes)>> {
            self.0.list_derived(kind, after, limit).await
        }
        async fn get_derived(&self, kind: &str, key: &Hash) -> Result<Option<Bytes>> {
            self.0.get_derived(kind, key).await
        }
        async fn get_derived_many(&self, _: &str, _: &[Hash]) -> Result<Vec<Option<Bytes>>> {
            Err(Error::Storage("no batches".into()))
        }
        async fn put_derived(&self, kind: &str, key: &Hash, value: Bytes) -> Result<()> {
            self.0.put_derived(kind, key, value).await
        }
    }

    /// A history with every shape creation handles: roots, nested and
    /// replaced directories, deletes, two- and three-parent merges, and
    /// server rebases (which write a link row with the copy).
    async fn build(r: &Repo) -> Vec<ChangesetId> {
        let mut ids =
            vec![commit(r, &[], &[("a/b/c", Some("0")), ("top", Some("t"))], "root").await];
        let mut main = ids[0];
        let mut side = ids[0];
        for i in 0..60usize {
            let v = i.to_string();
            let path = format!("d{}/e{}/f{}", i % 3, i % 5, i % 7);
            main = commit(r, &[main], &[(&path, Some(&v))], "main").await;
            ids.push(main);
            if i % 4 == 0 {
                let p = format!("side/{}", i % 6);
                side = commit(r, &[side], &[(&p, Some(&v))], "side").await;
                ids.push(side);
            }
            if i % 9 == 8 {
                let m = commit(r, &[main, side], &[("merged", Some(&v))], "merge").await;
                ids.push(m);
                main = m;
            }
            if i == 30 {
                // A file replaced by a directory, and a three-parent merge.
                main = commit(r, &[main], &[("top", None), ("top/x", Some("x"))], "dir").await;
                ids.push(main);
                let other = commit(r, &[ids[0]], &[("other", Some("o"))], "other").await;
                ids.push(other);
                main = commit(r, &[main, side, other], &[], "octopus").await;
                ids.push(main);
            }
            if i % 15 == 14 {
                let topic = commit(r, &[ids[ids.len() - 5]], &[("topic", Some(&v))], "topic").await;
                ids.push(topic);
                match r.rebase(topic, main).await.unwrap() {
                    RebaseResponse::Rebased { id } => {
                        ids.push(id);
                        main = id;
                    }
                    other => panic!("{other:?}"),
                }
            }
        }
        // A second root, and creating a changeset that already exists.
        ids.push(commit(r, &[], &[("z", Some("z"))], "root 2").await);
        let again = r.get_changeset(main).await.unwrap();
        assert_eq!(r.create_changeset(again).await.unwrap(), main);
        ids
    }

    async fn rows(r: &Repo, kind: &str) -> Vec<(Hash, Bytes)> {
        r.meta().list_derived(kind, None, usize::MAX).await.unwrap()
    }

    /// First-parent ancestors 1, 2, 4, ... steps up, by walking the chain:
    /// what a skiplist means, computed without any skiplist.
    async fn walked_skiplist(r: &Repo, id: ChangesetId) -> Vec<SkipEntry> {
        let mut out = Vec::new();
        let (mut cur, mut steps, mut next) = (id, 0u64, 1u64);
        while let Some(&p) = r.get_changeset(cur).await.unwrap().parents.first() {
            steps += 1;
            cur = p;
            if steps == next {
                out.push(SkipEntry {
                    id: p,
                    generation: r.generation(p).await.unwrap(),
                });
                next *= 2;
            }
        }
        out
    }

    #[tokio::test]
    async fn created_rows_match_derivation_with_and_without_batches() {
        let a = repo();
        let ids = build(&a).await;
        let b = Repo::new(
            Arc::new(MemoryBlobStore::new()),
            Arc::new(NoBatchedReads(Arc::new(MemoryMetaStore::new()))),
        );
        assert_eq!(build(&b).await, ids, "same changesets");

        // The same changesets, stored bare, with every row derived lazily.
        let c = Repo::new(a.blobs().clone(), Arc::new(MemoryMetaStore::new()));
        for &id in &ids {
            c.meta()
                .put_changeset(&a.get_changeset(id).await.unwrap())
                .await
                .unwrap();
        }
        for &id in &ids {
            derived::derive::<RootTree>(&c, id).await.unwrap();
            derived::derive::<Generation>(&c, id).await.unwrap();
            derived::derive::<Skiplist>(&c, id).await.unwrap();
        }
        for kind in [RootTree::NAME, Generation::NAME, Skiplist::NAME] {
            let want = rows(&c, kind).await;
            assert_eq!(want.len(), ids.len(), "{kind}: one row per changeset");
            assert_eq!(rows(&a, kind).await, want, "{kind} with batches");
            assert_eq!(rows(&b, kind).await, want, "{kind} without batches");
        }
        let links = rows(&a, REBASED_ORIGIN_KIND).await;
        assert_eq!(links.len(), 4, "one link per rebase");
        assert_eq!(rows(&b, REBASED_ORIGIN_KIND).await, links);
        let blobs = |r: &Repo| {
            let r = r.clone();
            async move { r.blobs().list_page(None, usize::MAX).await.unwrap() }
        };
        let hashes =
            |v: Vec<leep_core::BlobMeta>| v.into_iter().map(|m| m.hash).collect::<Vec<_>>();
        assert_eq!(
            hashes(blobs(&a).await),
            hashes(blobs(&b).await),
            "same tree nodes"
        );
        for &id in &ids {
            let stored = Skiplist::decode(
                &a.meta()
                    .get_derived("skiplist", &id.0)
                    .await
                    .unwrap()
                    .unwrap(),
            )
            .unwrap();
            assert_eq!(stored, walked_skiplist(&a, id).await, "skiplist of {id}");
        }
    }

    #[tokio::test]
    async fn rows_given_to_create_are_written_only_with_a_valid_changeset() {
        let r = repo();
        let base = commit(&r, &[], &[("f", Some("1"))], "base").await;
        let key = Hash::of_blob(b"mapping");
        let row = DerivedRow::new("test-mapping", key, Bytes::from_static(b"m"));
        let bad = changeset(
            &[ChangesetId(Hash::of_blob(b"nope"))],
            BTreeMap::new(),
            "bad",
        );
        assert!(matches!(
            r.create_changeset_with_rows(bad, vec![row.clone()]).await,
            Err(Error::Invalid(_))
        ));
        assert!(r
            .meta()
            .get_derived("test-mapping", &key)
            .await
            .unwrap()
            .is_none());
        let good = changeset(&[base], BTreeMap::new(), "good");
        let id = r.create_changeset_with_rows(good, vec![row]).await.unwrap();
        assert_eq!(
            r.meta()
                .get_derived("test-mapping", &key)
                .await
                .unwrap()
                .unwrap(),
            &b"m"[..]
        );
        assert_eq!(r.generation(id).await.unwrap(), 2);
    }
}
