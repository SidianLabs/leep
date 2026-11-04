//! The git maps of ADR 0090: ids are pure functions of content and the
//! commit style, the reverse index finds what each names, and verify checks
//! the rows.

use bytes::Bytes;
use leep_core::{ChangesetId, Error, Hash};
use leep_gitobj::{CommitStyle, ObjectId};

use crate::git_map::{
    GitObject, GIT_BLOB_KIND, GIT_COMMIT_KIND, GIT_OID_KIND, GIT_STYLE_KIND, GIT_TREE_KIND,
};
use crate::integrity::{verify, Quiet, VerifyOptions};
use crate::tests::{commit, merge_req, repo};
use crate::Repo;

const KINDS: [&str; 4] = [GIT_BLOB_KIND, GIT_TREE_KIND, GIT_COMMIT_KIND, GIT_OID_KIND];

async fn drop_git_rows(r: &Repo) {
    for k in KINDS {
        r.meta().delete_derived_kind(k).await.unwrap();
    }
}

/// History with nesting, a delete, an executable and a merge.
async fn history(r: &Repo) -> Vec<ChangesetId> {
    let a = commit(
        r,
        &[],
        &[
            ("README", Some("hi\n")),
            ("src/a/b.rs", Some("fn b() {}\n")),
        ],
        "first",
    )
    .await;
    let b = commit(
        r,
        &[a],
        &[("src/a/b.rs", None), ("src/c.rs", Some("c\n"))],
        "move",
    )
    .await;
    let c = commit(r, &[a], &[("docs/x.md", Some("x\n"))], "docs").await;
    let m = r.merge(merge_req(b, c)).await.unwrap();
    let m = match m {
        leep_core::api::MergeResponse::Merged { id } => id,
        other => panic!("{other:?}"),
    };
    vec![a, b, c, m]
}

async fn ids(r: &Repo, cs: &[ChangesetId]) -> Vec<ObjectId> {
    let mut out = Vec::new();
    for id in cs {
        out.push(r.git_commit_id(*id).await.unwrap());
    }
    out
}

#[tokio::test]
async fn ids_derive_again_to_the_same_bytes() {
    let r = repo();
    let h = history(&r).await;
    let first = ids(&r, &h).await;
    drop_git_rows(&r).await;
    // From the head alone, oldest first, with one job.
    let one = r.clone().with_derive_jobs(1);
    let head = one.git_commit_id(h[3]).await.unwrap();
    assert_eq!(head, first[3]);
    assert_eq!(ids(&one, &h).await, first);
    let distinct: std::collections::HashSet<_> = first.iter().collect();
    assert_eq!(distinct.len(), 4);
}

#[tokio::test]
async fn the_reverse_index_finds_commits_trees_and_blobs() {
    let r = repo();
    let h = history(&r).await;
    let head = r.git_commit_id(h[3]).await.unwrap();
    assert_eq!(
        r.git_object(&head).await.unwrap(),
        Some(GitObject::Commit(h[3]))
    );
    let root = r.root_tree(h[3]).await.unwrap();
    let tree = r.git_tree_id(root).await.unwrap();
    assert_eq!(
        r.git_object(&tree).await.unwrap(),
        Some(GitObject::Tree(root))
    );
    let blob = Hash::of_blob(b"hi\n");
    let oid = r.git_blob_ids(&[blob]).await.unwrap()[0];
    assert_eq!(oid, leep_gitobj::blob_id(b"hi\n").unwrap());
    assert_eq!(
        r.git_object(&oid).await.unwrap(),
        Some(GitObject::Blob(blob))
    );
    assert_eq!(
        r.git_object(&leep_gitobj::blob_id(b"never stored").unwrap())
            .await
            .unwrap(),
        None
    );
}

/// The tree and blob ids are git's own: a tree with one file whose id git
/// prints for the same content (`git mktree` of `100644 blob <id>\tREADME`).
#[tokio::test]
async fn a_tree_id_is_gits() {
    let r = repo();
    let a = commit(&r, &[], &[("README", Some("hi\n"))], "first").await;
    let tree = r.git_tree_id(r.root_tree(a).await.unwrap()).await.unwrap();
    let blob = leep_gitobj::blob_id(b"hi\n").unwrap();
    assert_eq!(blob.to_string(), "45b983be36b73c0788dc9cbcb76cbb80fc7bb057");
    let mut expected = b"100644 README\0".to_vec();
    expected.extend_from_slice(blob.as_bytes());
    assert_eq!(tree, leep_gitobj::tree_id(&expected).unwrap());
}

#[tokio::test]
async fn a_changeset_with_no_files_has_the_empty_tree() {
    let r = repo();
    let a = commit(&r, &[], &[("only/one", Some("1"))], "one").await;
    let b = commit(&r, &[a], &[("only/one", None)], "none").await;
    r.git_commit_id(b).await.unwrap();
    let tree = r.git_tree_id(r.root_tree(b).await.unwrap()).await.unwrap();
    assert_eq!(tree, leep_gitobj::empty_tree_id());
}

#[tokio::test]
async fn the_style_is_written_once() {
    let r = repo();
    assert_eq!(r.git_style().await.unwrap(), None);
    let plain = CommitStyle {
        changeset_trailer: false,
        provenance_trailers: false,
    };
    assert_eq!(r.set_git_style(plain).await.unwrap(), plain);
    assert_eq!(r.set_git_style(plain).await.unwrap(), plain);
    let err = r.set_git_style(CommitStyle::default()).await.unwrap_err();
    assert!(matches!(err, Error::Conflict(_)), "{err}");
    // A fresh handle on the same store reads it back.
    let again = Repo::new(r.blobs().clone(), r.meta().clone());
    assert_eq!(again.git_style().await.unwrap(), Some(plain));
    assert!(again.set_git_style(CommitStyle::default()).await.is_err());
}

#[tokio::test]
async fn the_first_commit_id_records_the_default_style() {
    let r = repo();
    let a = commit(&r, &[], &[("f", Some("1"))], "one").await;
    r.git_commit_id(a).await.unwrap();
    assert_eq!(r.git_style().await.unwrap(), Some(CommitStyle::default()));
}

/// The style changes the ids of changesets made in Leep.
#[tokio::test]
async fn the_style_decides_a_native_commit_id() {
    let mut out = Vec::new();
    for style in [
        CommitStyle::default(),
        CommitStyle {
            changeset_trailer: false,
            provenance_trailers: false,
        },
    ] {
        let r = repo();
        r.set_git_style(style).await.unwrap();
        let a = commit(&r, &[], &[("f", Some("1"))], "one").await;
        out.push(r.git_commit_id(a).await.unwrap());
    }
    assert_ne!(out[0], out[1]);
}

#[tokio::test]
async fn a_deep_chain_derives_without_recursion() {
    let r = repo();
    let mut head = commit(&r, &[], &[("f", Some("0"))], "0").await;
    for i in 1..10_000 {
        head = commit(&r, &[head], &[("f", Some(&i.to_string()))], &i.to_string()).await;
    }
    r.git_commit_id(head).await.unwrap();
    assert_eq!(
        r.meta().count_derived(GIT_COMMIT_KIND).await.unwrap(),
        10_000
    );
}

#[tokio::test]
async fn verify_checks_the_git_rows() {
    let r = repo();
    let h = history(&r).await;
    r.meta().cas_ref("main", None, Some(h[3])).await.unwrap();
    ids(&r, &h).await;
    let opts = VerifyOptions {
        full: true,
        ..VerifyOptions::default()
    };
    let report = verify(&r, &opts, &Quiet).await.unwrap();
    assert!(report.ok(), "{:#?}", report.problems);
    for k in KINDS.into_iter().chain([GIT_STYLE_KIND]) {
        assert!(
            report.derived[k].checked > 0,
            "{k}: {:?}",
            report.derived[k]
        );
    }

    // A wrong tree id is found.
    let root = r.root_tree(h[0]).await.unwrap();
    r.meta()
        .put_derived(GIT_TREE_KIND, &root, Bytes::from_static(&[7; 20]))
        .await
        .unwrap();
    let report = verify(&r, &opts, &Quiet).await.unwrap();
    assert!(
        report
            .problems
            .iter()
            .any(|p| p.kind.as_deref() == Some(GIT_TREE_KIND)),
        "{:#?}",
        report.problems
    );
}
