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
    let MergeResponse::Conflicts { conflicts } = r.merge(merge_req(ours, theirs)).await.unwrap()
    else {
        panic!()
    };
    // f merged cleanly, so only the two clashing files conflict.
    let paths: Vec<_> = conflicts.iter().map(|c| c.path.as_str()).collect();
    assert_eq!(paths, ["bin", "clash"]);
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
    let MergeResponse::Conflicts { conflicts } = r.merge(merge_req(ours, theirs)).await.unwrap()
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
    let MergeResponse::Conflicts { conflicts } = r.merge(merge_req(ours, theirs)).await.unwrap()
    else {
        panic!()
    };
    assert_eq!(conflicts[0].path.as_str(), "a/b");
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
    async fn cas_ref(
        &self,
        name: &str,
        expected: Option<ChangesetId>,
        new: Option<ChangesetId>,
    ) -> leep_core::Result<bool> {
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        self.0.cas_ref(name, expected, new).await
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
