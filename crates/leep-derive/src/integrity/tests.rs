//! Corruption-injection tests: damage a store in each way the fault and crash
//! tests could not catch, and check that `verify` names it exactly and
//! `repair` brings the store back (ADR 0025).

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use leep_core::api::{Authorship, MergeRequest, MergeResponse, RebaseResponse};
use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
use leep_core::review::{Review, Verdict, REVIEW_KIND};
use leep_core::signature::{key_id, Signature, SIGNATURE_KIND};
use leep_core::{
    BlobStore, Changeset, ChangesetId, FileChange, FileKind, Hash, MetaStore, RepoPath, TreeEntry,
};

use super::kinds::{self, BLAME_KIND};
use super::*;
use crate::derived::{self, HistoryTree, LastMerge, StoredHistoryNode};
use crate::gc::BLOB_CLAIM;
use crate::signatures::REBASED_ORIGIN_KIND;
use crate::{Repo, SearchQuery};

fn rp(s: &str) -> RepoPath {
    RepoPath::new(s).unwrap()
}

struct World {
    repo: Repo,
    blobs: Arc<MemoryBlobStore>,
    meta: Arc<MemoryMetaStore>,
    /// base, left1, left2, right1, right2, merge, tip
    ids: Vec<ChangesetId>,
    /// The server-made copy of right1 rebased onto left2.
    copy: ChangesetId,
}

impl World {
    fn tip(&self) -> ChangesetId {
        self.ids[6]
    }
}

/// `Some(text)` writes a file, `None` deletes it.
async fn commit(r: &Repo, parents: &[ChangesetId], files: &[(&str, Option<&[u8]>)]) -> ChangesetId {
    let mut changes = BTreeMap::new();
    for (p, c) in files {
        let ch = match c {
            Some(t) => FileChange::Modify {
                content: r.blobs().put(Bytes::copy_from_slice(t)).await.unwrap(),
                kind: FileKind::Regular,
                size: t.len() as u64,
            },
            None => FileChange::Delete,
        };
        changes.insert(rp(p), ch);
    }
    r.create_changeset(Changeset {
        parents: parents.to_vec(),
        author: "t".into(),
        timestamp: 1,
        message: format!("commit {}", changes.len()),
        changes,
        extra: BTreeMap::new(),
    })
    .await
    .unwrap()
}

async fn world() -> World {
    let blobs = Arc::new(MemoryBlobStore::new());
    let meta = Arc::new(MemoryMetaStore::new());
    let r = Repo::new(blobs.clone(), meta.clone());
    let t = |s: &'static str| Some(s.as_bytes());
    let base = commit(
        &r,
        &[],
        &[
            ("a.txt", t("a1\nshared line\n")),
            ("gone.txt", t("gone\n")),
            ("d/x.txt", t("x1\n")),
            ("d/e/y.txt", t("y1\n")),
            ("bin/data.bin", Some(&[0u8, 1, 2, 3, 255][..])),
        ],
    )
    .await;
    let left1 = commit(
        &r,
        &[base],
        &[("a.txt", t("a2\nshared line\n")), ("gone.txt", None)],
    )
    .await;
    let left2 = commit(
        &r,
        &[left1],
        &[("d/x.txt", t("x2 needle\n")), ("l.txt", t("l\n"))],
    )
    .await;
    let right1 = commit(
        &r,
        &[base],
        &[("d/e/y.txt", t("y2\n")), ("r/new.txt", t("n\n"))],
    )
    .await;
    let right2 = commit(&r, &[right1], &[("r/new.txt", None), ("r2.txt", t("r2\n"))]).await;
    let merged = match r
        .merge(MergeRequest {
            parents: [left2, right2],
            message: "merge".into(),
            authorship: Authorship {
                author: "m".into(),
                timestamp: Some(5),
            },
            extra: BTreeMap::new(),
        })
        .await
        .unwrap()
    {
        MergeResponse::Merged { id } => id,
        other => panic!("expected a clean merge: {other:?}"),
    };
    let tip = commit(
        &r,
        &[merged],
        &[("d/e/y.txt", None), ("final.txt", t("fin\n"))],
    )
    .await;
    let ids = vec![base, left1, left2, right1, right2, merged, tip];
    let copy = match r.rebase(right1, left2).await.unwrap() {
        RebaseResponse::Rebased { id } => id,
        other => panic!("expected a clean rebase: {other:?}"),
    };

    assert!(meta.cas_ref("main", None, Some(tip)).await.unwrap());
    assert!(meta.cas_ref("feature", None, Some(left2)).await.unwrap());
    assert!(meta
        .cas_ref("feature", Some(left2), Some(right2))
        .await
        .unwrap());
    assert!(meta
        .cas_ref("feature", Some(right2), Some(left2))
        .await
        .unwrap());

    // Warm every kind a server would have derived by now.
    for id in ids.iter().copied().chain([copy]) {
        derived::derive::<HistoryTree>(&r, id).await.unwrap();
        derived::derive::<LastMerge>(&r, id).await.unwrap();
        derived::derive::<derived::LogFilterIndex>(&r, id)
            .await
            .unwrap();
    }
    r.derive_search(tip).await.unwrap();
    r.derive_search(left2).await.unwrap();
    for (id, path) in [(tip, "a.txt"), (tip, "d/x.txt"), (left2, "a.txt")] {
        r.blame(id, &rp(path)).await.unwrap();
    }
    let sig = Signature {
        key_id: key_id(&[7; 32]),
        public_key: [7; 32],
        sig: [9; 64],
    };
    r.put_signature(base, &sig).await.unwrap();
    r.put_review(
        tip,
        &Review {
            reviewer: "alice".into(),
            verdict: Verdict::Approve,
            comment: "ok".into(),
            timestamp: 3,
        },
    )
    .await
    .unwrap();
    let hashes: Vec<Hash> = blobs
        .list_page(None, usize::MAX)
        .await
        .unwrap()
        .into_iter()
        .map(|m| m.hash)
        .collect();
    r.missing_blobs(&hashes).await.unwrap();
    World {
        repo: r,
        blobs,
        meta,
        ids,
        copy,
    }
}

fn full() -> VerifyOptions {
    VerifyOptions {
        full: true,
        rehash: true,
        ..VerifyOptions::default()
    }
}

async fn run_verify(w: &World, opts: &VerifyOptions) -> VerifyReport {
    verify(&w.repo, opts, &Quiet).await.unwrap()
}

async fn assert_clean(w: &World) {
    for opts in [full(), VerifyOptions::default()] {
        let r = run_verify(w, &opts).await;
        assert!(r.ok(), "expected a clean store, found: {:#?}", r.problems);
    }
}

fn codes(r: &VerifyReport) -> Vec<&'static str> {
    r.problems.iter().map(|p| p.code).collect()
}

fn only<'a>(r: &'a VerifyReport, code: &str) -> &'a Problem {
    let found: Vec<_> = r.problems.iter().filter(|p| p.code == code).collect();
    assert_eq!(found.len(), 1, "want one {code} in {:#?}", r.problems);
    found[0]
}

/// Every derived row, ref, ref log entry and blob: what a read-only command
/// must leave exactly as it found it.
async fn snapshot(w: &World) -> String {
    let mut out = String::new();
    let mut names: Vec<&str> = kinds::kinds().iter().map(|k| k.name).collect();
    names.push(BLAME_KIND);
    for kind in names {
        for (k, v) in w.meta.list_derived(kind, None, usize::MAX).await.unwrap() {
            out.push_str(&format!("{kind} {k} {v:?}\n"));
        }
    }
    for (name, id) in w.meta.list_refs().await.unwrap() {
        out.push_str(&format!("ref {name} {id}\n"));
    }
    for e in w.meta.ref_log(None, None, usize::MAX).await.unwrap() {
        out.push_str(&format!("log {e:?}\n"));
    }
    for m in w.blobs.list_page(None, usize::MAX).await.unwrap() {
        out.push_str(&format!("blob {} {}\n", m.hash, m.size));
    }
    out
}

async fn row(w: &World, kind: &str, key: &Hash) -> Bytes {
    w.meta.get_derived(kind, key).await.unwrap().expect("row")
}

async fn put_row(w: &World, kind: &str, key: &Hash, v: impl Into<Bytes>) {
    w.meta.put_derived(kind, key, v.into()).await.unwrap();
}

fn flip(b: &Bytes) -> Bytes {
    let mut v = b.to_vec();
    let last = v.len() - 1;
    v[last] ^= 1;
    Bytes::from(v)
}

async fn rederive_kinds(w: &World, kinds: &[&'static str], dry_run: bool) -> RederiveReport {
    rederive(
        &w.repo,
        &RederiveOptions {
            kinds: kinds.to_vec(),
            scope: Scope::All,
            dry_run,
            max_problems: 1000,
        },
        &Quiet,
    )
    .await
    .unwrap()
}

// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_healthy_store_verifies_clean_and_verify_writes_nothing() {
    let w = world().await;
    let before = snapshot(&w).await;
    for opts in [full(), VerifyOptions::default()] {
        let r = run_verify(&w, &opts).await;
        assert!(r.ok(), "{:#?}", r.problems);
        assert_eq!(r.refs, ["feature", "main"]);
        assert!(r.stats.changesets_reachable >= 7);
        assert!(r.stats.tree_nodes > 5);
        assert!(r.stats.blobs_checked > 5);
        assert!(r.stats.reflog_entries >= 4);
    }
    let r = run_verify(&w, &full()).await;
    assert_eq!(
        r.stats.changesets_other, 1,
        "the rebased copy, found by --full"
    );
    assert_eq!(r.stats.blobs_rehashed, r.stats.blobs_checked);
    assert!(r.stats.bytes_rehashed > 0);
    for kind in [
        "generation",
        "skiplist",
        "last_merge-v1",
        "root_tree",
        "history-tree-v3",
    ] {
        assert!(
            r.derived[kind].checked >= 8,
            "{kind}: {:?}",
            r.derived[kind]
        );
        assert_eq!(r.derived[kind].mismatched, 0);
    }
    for kind in [
        "blob-trigrams-v1",
        "tree-trigrams-v1",
        "history-node-v3",
        "blame",
        "signature-v1",
        "review-v1",
        "rebased-origin-v1",
        "blob-claim-v1",
    ] {
        assert!(r.derived[kind].checked > 0, "{kind} was not checked");
    }
    assert_eq!(snapshot(&w).await, before, "verify is read-only");
}

#[tokio::test]
async fn a_missing_blob_is_named_with_its_changeset_and_path() {
    let w = world().await;
    let victim = Hash::of_blob(b"x1\n");
    w.blobs.delete(&victim).await.unwrap();
    let r = run_verify(&w, &VerifyOptions::default()).await;
    let p = only(&r, "missing_blob");
    assert_eq!(p.key.as_deref(), Some(victim.to_hex().as_str()));
    assert_eq!(p.changeset.as_deref(), Some(w.ids[0].to_string().as_str()));
    assert_eq!(p.path.as_deref(), Some("d/x.txt"));
    assert_eq!(r.problems.len(), 1, "{:#?}", r.problems);
    // Only the refs asked for are walked; `feature` also contains the blob.
    let one = VerifyOptions {
        refs: vec!["feature".into()],
        ..VerifyOptions::default()
    };
    assert_eq!(codes(&run_verify(&w, &one).await), ["missing_blob"]);
}

#[tokio::test]
async fn a_blob_with_wrong_bytes_needs_rehash_to_be_seen() {
    let w = world().await;
    let victim = Hash::of_blob(b"y1\n");
    w.blobs.delete(&victim).await.unwrap();
    w.blobs
        .put_with_hash(victim, Bytes::from_static(b"y?\n"))
        .await
        .unwrap();
    let cheap = run_verify(&w, &VerifyOptions::default()).await;
    assert!(
        !codes(&cheap).contains(&"corrupt_blob"),
        "existence is all the default checks"
    );
    let r = run_verify(&w, &full()).await;
    let p = only(&r, "corrupt_blob");
    assert_eq!(p.key.as_deref(), Some(victim.to_hex().as_str()));
    assert_eq!(p.path.as_deref(), Some("d/e/y.txt"));

    // Right bytes for another blob but the wrong length for the entry.
    let w = world().await;
    let victim = Hash::of_blob(b"x1\n");
    w.blobs.delete(&victim).await.unwrap();
    w.blobs
        .put_with_hash(victim, Bytes::from_static(b"x1\n"))
        .await
        .unwrap();
    assert!(run_verify(&w, &full()).await.ok());
}

#[tokio::test]
async fn a_damaged_tree_node_is_named_with_its_changeset_and_directory() {
    let w = world().await;
    let root = w.repo.root_tree(w.ids[0]).await.unwrap();
    let TreeEntry::Dir { tree: d } = w.repo.read_tree(root).await.unwrap().entries["d"].clone()
    else {
        panic!("d is a directory")
    };
    // Flip one byte, keep the address.
    let bytes = w.blobs.get(&d).await.unwrap().unwrap();
    let mut bad = bytes.to_vec();
    bad[0] ^= 0x40;
    w.blobs.delete(&d).await.unwrap();
    w.blobs.put_with_hash(d, Bytes::from(bad)).await.unwrap();
    let r = run_verify(&w, &VerifyOptions::default()).await;
    let p = r
        .problems
        .iter()
        .find(|p| p.code == "corrupt_tree")
        .unwrap_or_else(|| panic!("{:#?}", r.problems));
    assert_eq!(p.key.as_deref(), Some(d.to_hex().as_str()));
    assert_eq!(p.path.as_deref(), Some("d"));
    assert_eq!(p.changeset.as_deref(), Some(w.ids[0].to_string().as_str()));

    // Gone entirely.
    let w = world().await;
    w.blobs.delete(&d).await.unwrap();
    let r = run_verify(&w, &VerifyOptions::default()).await;
    let p = r
        .problems
        .iter()
        .find(|p| p.code == "missing_tree")
        .unwrap_or_else(|| panic!("{:#?}", r.problems));
    assert_eq!(p.path.as_deref(), Some("d"));
    assert_eq!(p.changeset.as_deref(), Some(w.ids[0].to_string().as_str()));
}

#[tokio::test]
async fn a_missing_changeset_is_reported_with_the_child_that_needs_it() {
    let w = world().await;
    let (left1, left2) = (w.ids[1], w.ids[2]);
    assert!(w.meta.remove_changeset(&left1));
    let r = run_verify(&w, &full()).await;
    let p = only(&r, "missing_changeset");
    assert_eq!(p.key.as_deref(), Some(left1.0.to_hex().as_str()));
    assert_eq!(p.changeset.as_deref(), Some(left2.to_string().as_str()));
    // Rows that need the lost parent are skipped, not reported wrong. Blame
    // rows that credit the lost changeset really are wrong.
    for p in r.problems.iter().filter(|p| p.code == "derived_mismatch") {
        assert_eq!(p.kind.as_deref(), Some("blame"), "{:#?}", r.problems);
    }
    assert!(r.derived["generation"].skipped >= 1);
}

#[tokio::test]
async fn every_per_changeset_kind_is_checked_by_recomputation_and_repaired() {
    for (kind, victim) in [
        ("generation", 2),
        ("skiplist", 2),
        ("last_merge-v1", 5),
        ("root_tree", 2),
        ("history-tree-v3", 2),
    ] {
        let w = world().await;
        let id = w.ids[victim];
        let good = row(&w, kind, &id.0).await;
        put_row(&w, kind, &id.0, flip(&good)).await;

        let r = run_verify(&w, &full()).await;
        let p = only(&r, "derived_mismatch");
        assert_eq!(p.kind.as_deref(), Some(kind), "{:#?}", r.problems);
        assert_eq!(p.key.as_deref(), Some(id.0.to_hex().as_str()));
        assert_eq!(p.changeset.as_deref(), Some(id.to_string().as_str()));
        // A bad pointer can also lead nowhere; nothing else may be reported,
        // and in particular nothing for rows derived from the damaged one.
        for other in &r.problems {
            assert_eq!(other.changeset.as_deref(), Some(id.to_string().as_str()));
        }
        assert!(r.problems.len() <= 2, "{kind}: {:#?}", r.problems);

        // A dry run reports and writes nothing.
        let before = snapshot(&w).await;
        let dry = rederive_kinds(&w, &[kind], true).await;
        assert!(dry.dry_run);
        assert_eq!(dry.changed(), 1, "{kind}: {:#?}", dry.kind_reports);
        assert_eq!(snapshot(&w).await, before);

        let fixed = rederive_kinds(&w, &[kind], false).await;
        assert_eq!(fixed.changed(), 1, "{kind}: {:#?}", fixed.kind_reports);
        assert_eq!(fixed.failed(), 0);
        assert_eq!(row(&w, kind, &id.0).await, good, "{kind} is back");
        assert_clean(&w).await;
        assert_eq!(rederive_kinds(&w, &[kind], false).await.changed(), 0);
    }
}

#[tokio::test]
async fn a_damaged_tip_row_is_found_by_the_default_sample() {
    let w = world().await;
    let tip = w.tip();
    let good = row(&w, "generation", &tip.0).await;
    put_row(&w, "generation", &tip.0, flip(&good)).await;
    let r = run_verify(
        &w,
        &VerifyOptions {
            sample: 1,
            ..VerifyOptions::default()
        },
    )
    .await;
    assert_eq!(
        only(&r, "derived_mismatch").kind.as_deref(),
        Some("generation")
    );
}

#[tokio::test]
async fn a_damaged_generation_does_not_blame_the_rows_derived_from_it() {
    // Children recorded the parent's generation in their skiplists; with the
    // parent's row damaged they must still be judged against the true value.
    let w = world().await;
    let id = w.ids[1];
    put_row(&w, "generation", &id.0, 99u64.to_be_bytes().to_vec()).await;
    let r = run_verify(&w, &full()).await;
    assert_eq!(r.problems.len(), 1, "{:#?}", r.problems);
    assert_eq!(r.problems[0].kind.as_deref(), Some("generation"));
    assert!(r.problems[0].message.contains("stored 99, recomputed 2"));
    // Repairing only skiplist still checks generation first.
    let fixed = rederive_kinds(&w, &["skiplist"], false).await;
    assert!(fixed.kinds.contains(&"generation".to_string()));
    assert_clean(&w).await;
}

#[tokio::test]
async fn a_damaged_history_pointer_or_node_is_found_and_rewritten() {
    // A pointer to a node that does not exist.
    let w = world().await;
    let id = w.ids[2];
    put_row(&w, "history-tree-v3", &id.0, vec![0xAB; 32]).await;
    let r = run_verify(&w, &VerifyOptions::default()).await;
    assert!(codes(&r).contains(&"derived_mismatch"), "{:#?}", r.problems);
    assert!(codes(&r).contains(&"missing_history_node"));
    rederive_kinds(&w, &["history-tree-v3"], false).await;
    assert_clean(&w).await;

    // A node whose bytes no longer match its address.
    let w = world().await;
    let rows = w
        .meta
        .list_derived("history-node-v3", None, usize::MAX)
        .await
        .unwrap();
    let (key, value) = rows
        .iter()
        .find(|(_, v)| v.len() > 3)
        .expect("a non-trivial node")
        .clone();
    put_row(&w, "history-node-v3", &key, flip(&value)).await;
    for opts in [VerifyOptions::default(), full()] {
        let r = run_verify(&w, &opts).await;
        assert!(
            codes(&r).contains(&"corrupt_history_node"),
            "{:#?}",
            r.problems
        );
    }
    let fixed = rederive_kinds(&w, &["history-tree-v3"], false).await;
    assert_eq!(fixed.failed(), 0, "{:#?}", fixed.problems);
    assert_eq!(row(&w, "history-node-v3", &key).await, value);
    assert_clean(&w).await;
}

/// The world plus a directory wide enough that its history node is split,
/// and the address of one of its chunks.
async fn wide_world() -> (World, Hash) {
    let mut w = world().await;
    let names: Vec<String> = (0..100).map(|i| format!("w/f{i:03}")).collect();
    let files: Vec<(&str, Option<&[u8]>)> = names
        .iter()
        .map(|n| (n.as_str(), Some(&b"w\n"[..])))
        .collect();
    let wide = commit(&w.repo, &[w.tip()], &files).await;
    let tip = commit(&w.repo, &[wide], &[("w/f050", Some(b"changed\n"))]).await;
    assert!(w
        .meta
        .cas_ref("main", Some(w.tip()), Some(tip))
        .await
        .unwrap());
    w.ids.push(wide);
    w.ids[6] = tip;
    w.repo.last_change(tip, "w/f050").await.unwrap();
    let rows = w
        .meta
        .list_derived("history-node-v3", None, usize::MAX)
        .await
        .unwrap();
    let chunk = rows
        .iter()
        .find_map(|(_, v)| match StoredHistoryNode::decode(v).unwrap() {
            StoredHistoryNode::Split(refs) => Some(refs[1].node),
            _ => None,
        })
        .expect("a split node");
    (w, chunk)
}

#[tokio::test]
async fn a_damaged_or_missing_history_chunk_is_found_and_rewritten() {
    let (w, chunk) = wide_world().await;
    assert_clean(&w).await;
    let value = row(&w, "history-node-v3", &chunk).await;

    put_row(&w, "history-node-v3", &chunk, flip(&value)).await;
    let r = run_verify(&w, &full()).await;
    assert!(
        r.problems
            .iter()
            .all(|p| p.code == "corrupt_history_node" && p.key == Some(chunk.to_hex())),
        "{:#?}",
        r.problems
    );
    assert!(!r.problems.is_empty());
    let fixed = rederive_kinds(&w, &["history-tree-v3"], false).await;
    assert_eq!(fixed.failed(), 0, "{:#?}", fixed.problems);
    assert_eq!(row(&w, "history-node-v3", &chunk).await, value);
    assert_clean(&w).await;

    // Gone altogether: only the walk from the history roots can notice.
    let rows = w
        .meta
        .list_derived("history-node-v3", None, usize::MAX)
        .await
        .unwrap();
    w.meta.delete_derived_kind("history-node-v3").await.unwrap();
    for (k, v) in rows.into_iter().filter(|(k, _)| *k != chunk) {
        put_row(&w, "history-node-v3", &k, v).await;
    }
    let r = run_verify(&w, &full()).await;
    let p = only(&r, "missing_history_node");
    assert_eq!(p.key, Some(chunk.to_hex()));
    assert_eq!(r.problems.len(), 1, "{:#?}", r.problems);
    rederive_kinds(&w, &["history-tree-v3"], false).await;
    assert_eq!(row(&w, "history-node-v3", &chunk).await, value);
    assert_clean(&w).await;
}

#[tokio::test]
async fn dropping_the_retired_history_indexes_removes_only_their_rows() {
    let w = world().await;
    let current = w
        .meta
        .list_derived("history-node-v3", None, usize::MAX)
        .await
        .unwrap();
    assert!(!current.is_empty());
    for (tree, node) in [
        ("history-tree-v1", "history-node-v1"),
        ("history-tree-v2", "history-node-v2"),
    ] {
        put_row(&w, tree, &w.tip().0, vec![7; 32]).await;
        put_row(&w, node, &Hash([7; 32]), vec![0]).await;
        let report = drop_kinds(&w.repo, &[tree], false, &Quiet).await.unwrap();
        assert_eq!(report.rows[tree], 1);
        assert_eq!(report.rows[node], 1);
        for kind in [tree, node] {
            assert!(w.meta.list_derived(kind, None, 1).await.unwrap().is_empty());
        }
        assert_eq!(
            w.meta
                .list_derived("history-node-v3", None, usize::MAX)
                .await
                .unwrap(),
            current
        );
    }
}

#[tokio::test]
async fn rederiving_root_trees_restores_damaged_tree_nodes() {
    let w = world().await;
    let root = w.repo.root_tree(w.ids[0]).await.unwrap();
    let TreeEntry::Dir { tree: d } = w.repo.read_tree(root).await.unwrap().entries["d"].clone()
    else {
        panic!("d is a directory")
    };
    w.blobs.delete(&d).await.unwrap();
    assert!(!run_verify(&w, &VerifyOptions::default()).await.ok());
    let fixed = rederive_kinds(&w, &["root_tree"], false).await;
    assert_eq!(fixed.changed(), 0, "the rows were right; the node was not");
    assert_eq!(fixed.failed(), 0, "{:#?}", fixed.problems);
    assert_clean(&w).await;
}

#[tokio::test]
async fn search_rows_are_recomputed_from_blobs_and_trees() {
    let w = world().await;
    let blob = Hash::of_blob(b"x2 needle\n");
    put_row(&w, "blob-trigrams-v1", &blob, vec![0, 1]).await;
    let r = run_verify(&w, &full()).await;
    let p = only(&r, "derived_mismatch");
    assert_eq!(p.kind.as_deref(), Some("blob-trigrams-v1"));
    assert_eq!(p.key.as_deref(), Some(blob.to_hex().as_str()));
    assert!(
        r.problems.len() == 1,
        "tree rows are judged from the blobs, not from the damaged row: {:#?}",
        r.problems
    );

    let dry = rederive_kinds(&w, &["blob-trigrams-v1"], true).await;
    assert_eq!(dry.changed(), 1);
    let fixed = rederive_kinds(&w, &["blob-trigrams-v1"], false).await;
    assert_eq!(fixed.changed(), 1);
    assert_clean(&w).await;

    // A directory filter that lost its bits would hide matches from search.
    let root = w.repo.root_tree(w.tip()).await.unwrap();
    let good = row(&w, "tree-trigrams-v1", &root).await;
    put_row(&w, "tree-trigrams-v1", &root, vec![2]).await;
    let q = SearchQuery::new("needle");
    assert!(
        w.repo.search(w.tip(), &q).await.unwrap().matches.is_empty(),
        "the damaged filter really does hide the match"
    );
    let r = run_verify(&w, &full()).await;
    let p = only(&r, "derived_mismatch");
    assert_eq!(p.kind.as_deref(), Some("tree-trigrams-v1"));
    assert_eq!(p.key.as_deref(), Some(root.to_hex().as_str()));
    // Asking for tree rows alone includes the blob rows they depend on.
    let fixed = rederive_kinds(&w, &["tree-trigrams-v1"], false).await;
    assert_eq!(fixed.changed(), 1);
    assert_eq!(row(&w, "tree-trigrams-v1", &root).await, good);
    assert_eq!(w.repo.search(w.tip(), &q).await.unwrap().matches.len(), 1);
    assert_clean(&w).await;
}

#[tokio::test]
async fn blame_rows_must_match_the_file() {
    let w = world().await;
    let key = crate::blame::cache_key(w.tip(), &rp("a.txt"));
    let stored: crate::blame::CachedBlame =
        postcard::from_bytes(&row(&w, BLAME_KIND, &key).await).unwrap();
    // Wrong text on a line.
    let mut bad = stored.clone();
    bad.1[0].text = "tampered".into();
    put_row(&w, BLAME_KIND, &key, postcard::to_stdvec(&bad).unwrap()).await;
    let r = run_verify(&w, &VerifyOptions::default()).await;
    let p = only(&r, "derived_mismatch");
    assert_eq!(p.kind.as_deref(), Some(BLAME_KIND));
    assert_eq!(p.path.as_deref(), Some("a.txt"));
    assert_eq!(p.changeset.as_deref(), Some(w.tip().to_string().as_str()));
    // A line credited to a changeset that is not stored.
    let mut bad = stored;
    bad.1[0].changeset = ChangesetId(Hash::of_blob(b"nobody"));
    put_row(&w, BLAME_KIND, &key, postcard::to_stdvec(&bad).unwrap()).await;
    assert_eq!(
        codes(&run_verify(&w, &VerifyOptions::default()).await),
        ["derived_mismatch"]
    );
    // Blame is dropped, not rederived.
    assert!(rederive(
        &w.repo,
        &RederiveOptions {
            kinds: vec![BLAME_KIND],
            scope: Scope::All,
            dry_run: false,
            max_problems: 10
        },
        &Quiet
    )
    .await
    .is_err());
    drop_kinds(&w.repo, &[BLAME_KIND], false, &Quiet)
        .await
        .unwrap();
    assert_clean(&w).await;
}

#[tokio::test]
async fn attestations_and_bookkeeping_rows_are_checked_for_structure() {
    let w = world().await;
    let sig_key = w.meta.list_derived(SIGNATURE_KIND, None, 10).await.unwrap()[0].0;
    let review_key = w.meta.list_derived(REVIEW_KIND, None, 10).await.unwrap()[0].0;
    let claim_key = w.meta.list_derived(BLOB_CLAIM, None, 1).await.unwrap()[0].0;
    put_row(&w, SIGNATURE_KIND, &sig_key, vec![1, 2, 3]).await;
    put_row(&w, REVIEW_KIND, &review_key, vec![0xFF]).await;
    put_row(&w, BLOB_CLAIM, &claim_key, vec![1, 2, 3]).await;
    put_row(&w, REBASED_ORIGIN_KIND, &w.copy.0, vec![5; 32]).await;
    let r = run_verify(&w, &VerifyOptions::default()).await;
    let mut got: Vec<_> = r
        .problems
        .iter()
        .map(|p| (p.code, p.kind.clone().unwrap()))
        .collect();
    got.sort();
    assert_eq!(
        got,
        [
            ("bad_attestation", "review-v1".to_string()),
            ("bad_attestation", "signature-v1".to_string()),
            ("bad_gc_row", "blob-claim-v1".to_string()),
            ("bad_rebased_origin", "rebased-origin-v1".to_string()),
        ]
    );
}

#[tokio::test]
async fn a_full_run_matches_attestations_to_changesets_and_checks_signatures() {
    let w = world().await;
    let id = w.ids[3];
    let sig = Signature {
        key_id: key_id(&[8; 32]),
        public_key: [8; 32],
        sig: [1; 64],
    };
    w.repo.put_signature(id, &sig).await.unwrap();
    let accept: SignatureCheck = Arc::new(|_, _| Ok(()));
    let reject: SignatureCheck = Arc::new(|s, _| {
        if s.public_key == [8; 32] {
            Err("bad signature".into())
        } else {
            Ok(())
        }
    });
    let mut opts = full();
    opts.signature_check = Some(accept);
    assert!(run_verify(&w, &opts).await.ok());
    opts.signature_check = Some(reject);
    let r = run_verify(&w, &opts).await;
    let p = only(&r, "invalid_signature");
    assert_eq!(p.changeset.as_deref(), Some(id.to_string().as_str()));
    assert_eq!(r.problems.len(), 1);

    // A row whose changeset does not exist can only be damage.
    let orphan = Hash::of_blob(b"no such changeset");
    put_row(&w, SIGNATURE_KIND, &orphan, sig.encode()).await;
    put_row(
        &w,
        REVIEW_KIND,
        &orphan,
        Review {
            reviewer: "alice".into(),
            verdict: Verdict::Reject,
            comment: String::new(),
            timestamp: 1,
        }
        .encode(),
    )
    .await;
    opts.signature_check = None;
    let r = run_verify(&w, &opts).await;
    assert_eq!(
        codes(&r),
        ["bad_attestation", "bad_attestation"],
        "{:#?}",
        r.problems
    );
    assert!(r
        .problems
        .iter()
        .all(|p| p.message.contains("no stored changeset")));
    // Sampling does not look for orphans: it cannot enumerate changesets.
    assert!(run_verify(&w, &VerifyOptions::default()).await.ok());
}

#[tokio::test]
async fn the_ref_log_must_replay_to_the_refs() {
    // A ref that moved without a log entry.
    let w = world().await;
    w.meta.set_ref_unlogged("main", Some(w.ids[5]));
    let r = run_verify(&w, &VerifyOptions::default()).await;
    let p = only(&r, "reflog_divergence");
    assert_eq!(p.name.as_deref(), Some("main"));
    assert_eq!(r.problems.len(), 1);
    // Scoped to the other ref, main's log is not looked at.
    let feature = VerifyOptions {
        refs: vec!["feature".into()],
        ..VerifyOptions::default()
    };
    assert!(run_verify(&w, &feature).await.ok());

    // An entry that was lost from the middle of a ref's log.
    let w = world().await;
    let log = w.meta.ref_log(Some("feature"), None, 100).await.unwrap();
    assert_eq!(log.len(), 3);
    assert!(w.meta.remove_log_entry(log[1].seq));
    let r = run_verify(&w, &VerifyOptions::default()).await;
    let p = only(&r, "reflog_chain_break");
    assert_eq!(p.name.as_deref(), Some("feature"));

    // A ref that was deleted without a log entry.
    let w = world().await;
    w.meta.set_ref_unlogged("feature", None);
    let r = run_verify(&w, &VerifyOptions::default()).await;
    assert_eq!(
        only(&r, "reflog_divergence").name.as_deref(),
        Some("feature")
    );
}

#[tokio::test]
async fn unknown_refs_and_a_ref_to_nothing_are_reported() {
    let w = world().await;
    let opts = VerifyOptions {
        refs: vec!["nope".into()],
        ..VerifyOptions::default()
    };
    assert!(verify(&w.repo, &opts, &Quiet).await.is_err());
    let ghost = ChangesetId(Hash::of_blob(b"ghost"));
    w.meta.set_ref_unlogged("ghost", Some(ghost));
    let r = run_verify(&w, &VerifyOptions::default()).await;
    let p = r
        .problems
        .iter()
        .find(|p| p.code == "missing_changeset")
        .expect("the ref's target is missing");
    assert_eq!(p.name.as_deref(), Some("ghost"));
}

#[tokio::test]
async fn only_the_first_problems_are_kept_but_all_are_counted() {
    let w = world().await;
    for id in &w.ids {
        let good = row(&w, "generation", &id.0).await;
        put_row(&w, "generation", &id.0, flip(&good)).await;
    }
    let mut opts = full();
    opts.max_problems = 3;
    let r = run_verify(&w, &opts).await;
    assert_eq!(r.problems.len(), 3);
    assert_eq!(r.problems_total, 7);
    assert!(!r.ok());
}

#[tokio::test]
async fn missing_rows_are_not_problems_but_repair_fills_them() {
    let w = world().await;
    w.meta.delete_derived_kind("last_merge-v1").await.unwrap();
    w.meta.delete_derived_kind("history-tree-v3").await.unwrap();
    let r = run_verify(&w, &full()).await;
    assert!(r.ok(), "{:#?}", r.problems);
    assert!(r.derived["last_merge-v1"].missing >= 7);
    let fixed = rederive_kinds(&w, &["last_merge-v1", "history-tree-v3"], false).await;
    assert!(fixed.kind_reports["last_merge-v1"].filled >= 7);
    assert!(fixed.kind_reports["history-tree-v3"].filled >= 7);
    let r = run_verify(&w, &full()).await;
    assert_eq!(r.derived["last_merge-v1"].missing, 0);
}

#[tokio::test]
async fn rederive_can_be_limited_to_the_history_of_a_ref() {
    let w = world().await;
    for id in [w.ids[2], w.ids[6]] {
        let good = row(&w, "generation", &id.0).await;
        put_row(&w, "generation", &id.0, flip(&good)).await;
    }
    // `feature` is left2, whose history does not include the tip.
    let fixed = rederive(
        &w.repo,
        &RederiveOptions {
            kinds: vec!["generation"],
            scope: Scope::Refs(vec!["feature".into()]),
            dry_run: false,
            max_problems: 10,
        },
        &Quiet,
    )
    .await
    .unwrap();
    assert_eq!(fixed.changed(), 1);
    let r = run_verify(&w, &full()).await;
    assert_eq!(
        only(&r, "derived_mismatch").changeset.as_deref(),
        Some(w.tip().to_string().as_str())
    );
    assert_eq!(
        rederive_kinds(&w, &["generation"], false).await.changed(),
        1
    );
    assert_clean(&w).await;
}

// ---------------------------------------------------------------------------
// Dropping

/// Everything a user can ask that derived rows feed.
async fn answers(w: &World) -> String {
    let r = &w.repo;
    let mut out = String::new();
    for rev in w.ids.iter().chain([&w.copy]) {
        let log = r.log(*rev, None, 100).await.unwrap();
        out.push_str(&format!(
            "log {rev}: {:?}\n",
            log.iter()
                .map(|i| (i.id, i.generation, i.root_tree))
                .collect::<Vec<_>>()
        ));
        for author in ["t", "m"] {
            let filter = crate::LogFilter {
                author: Some(author.into()),
                extra: vec![],
            };
            let log = r.log_by_filter_with(*rev, 100, &filter, 0).await.unwrap();
            out.push_str(&format!(
                "log {rev} author {author}: {:?}\n",
                log.iter().map(|i| i.id).collect::<Vec<_>>()
            ));
        }
        for path in ["a.txt", "d", "d/e/y.txt", "r2.txt", "gone.txt"] {
            let log = r.log(*rev, Some(path), 100).await.unwrap();
            out.push_str(&format!(
                "log {rev} {path}: {:?}\n",
                log.iter().map(|i| i.id).collect::<Vec<_>>()
            ));
            out.push_str(&format!(
                "last-change {rev} {path}: {:?}\n",
                r.last_change(*rev, path).await.ok()
            ));
        }
    }
    for a in &w.ids {
        for b in &w.ids {
            out.push_str(&format!(
                "base {a} {b}: {:?} {}\n",
                r.merge_base(*a, *b).await.unwrap(),
                r.is_ancestor(*a, *b).await.unwrap()
            ));
        }
    }
    for (id, path) in [
        (w.tip(), "a.txt"),
        (w.tip(), "d/x.txt"),
        (w.ids[2], "a.txt"),
    ] {
        out.push_str(&format!(
            "blame {id} {path}: {:?}\n",
            r.blame(id, &rp(path)).await.unwrap()
        ));
    }
    for pat in ["needle", "shared", "fin", "x"] {
        let hits = r.search(w.tip(), &SearchQuery::new(pat)).await.unwrap();
        out.push_str(&format!("search {pat}: {:?}\n", hits.matches));
    }
    out
}

#[tokio::test]
async fn dropping_a_kind_changes_no_answer() {
    let droppable: Vec<&str> = kinds::kinds()
        .iter()
        .filter(|k| kinds::check_droppable(k.name).is_ok())
        .map(|k| k.name)
        .collect();
    assert!(droppable.len() >= 8, "{droppable:?}");
    let w = world().await;
    let before = answers(&w).await;
    let rows_before = snapshot(&w).await;
    for kind in &droppable {
        let report = drop_kinds(&w.repo, &[kind], false, &Quiet).await.unwrap();
        assert!(!report.rows.is_empty());
        assert!(report.rows[*kind] > 0, "{kind} had rows to drop");
        assert!(w.meta.list_derived(kind, None, 1).await.unwrap().is_empty());
        assert_eq!(
            answers(&w).await,
            before,
            "answers changed after dropping {kind}"
        );
        assert_clean(&w).await;
    }
    // Everything at once, then everything is derived again by asking.
    drop_kinds(&w.repo, &droppable, false, &Quiet)
        .await
        .unwrap();
    assert_eq!(answers(&w).await, before);
    assert_clean(&w).await;
    // The rows asked for came back identical; attestations were never touched.
    let after = snapshot(&w).await;
    for line in rows_before.lines().filter(|l| {
        l.starts_with("signature-v1")
            || l.starts_with("review-v1")
            || l.starts_with("rebased-origin")
            || l.starts_with("blob-claim")
            || l.starts_with("ref ")
            || l.starts_with("log ")
    }) {
        assert!(after.contains(line), "lost {line}");
    }
}

#[tokio::test]
async fn dropping_history_trees_takes_the_nodes_with_them() {
    let w = world().await;
    let report = drop_kinds(&w.repo, &["history-tree-v3"], false, &Quiet)
        .await
        .unwrap();
    assert!(report.rows["history-tree-v3"] >= 8);
    assert!(report.rows["history-node-v3"] > 0);
    assert!(w
        .meta
        .list_derived("history-node-v3", None, 1)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn dry_run_drop_counts_and_deletes_nothing() {
    let w = world().await;
    let before = snapshot(&w).await;
    let report = drop_kinds(&w.repo, &["generation", "blame"], true, &Quiet)
        .await
        .unwrap();
    assert!(report.dry_run);
    assert!(report.rows["generation"] >= 8);
    assert!(report.rows["blame"] >= 3);
    assert_eq!(snapshot(&w).await, before);
}

#[tokio::test]
async fn kinds_history_cannot_regenerate_are_refused() {
    let w = world().await;
    let before = snapshot(&w).await;
    for kind in [
        "signature-v1",
        "review-v1",
        "rebased-origin-v1",
        "blob-claim-v1",
        "blob-condemned-v1",
        "blob-rescue-v1",
        "blob-doom-v1",
        "history-node-v3",
        "ref_log",
        "unheard-of",
    ] {
        let err = drop_kinds(&w.repo, &[kind], false, &Quiet)
            .await
            .expect_err(kind)
            .to_string();
        assert!(
            err.contains("refusing")
                || err.contains("cannot be dropped")
                || err.contains("unknown"),
            "{kind}: {err}"
        );
    }
    // One refusal refuses the whole command: nothing is deleted first.
    let err = drop_kinds(&w.repo, &["generation", "review-v1"], false, &Quiet)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("review-v1"));
    assert_eq!(snapshot(&w).await, before);
    for kind in ["history-node-v3", "blame", "signature-v1", "bogus"] {
        let r = rederive(
            &w.repo,
            &RederiveOptions {
                kinds: vec![kind],
                scope: Scope::All,
                dry_run: false,
                max_problems: 10,
            },
            &Quiet,
        )
        .await;
        assert!(r.is_err(), "{kind} cannot be rederived");
    }
}

#[tokio::test]
async fn a_deep_history_verifies_without_recursing() {
    let r = Repo::new(
        Arc::new(MemoryBlobStore::new()),
        Arc::new(MemoryMetaStore::new()),
    );
    let mut parent: Vec<ChangesetId> = Vec::new();
    for i in 0..3000u32 {
        let body = format!("{i}\n");
        parent = vec![commit(&r, &parent, &[("f.txt", Some(body.as_bytes()))]).await];
    }
    r.meta()
        .cas_ref("main", None, Some(parent[0]))
        .await
        .unwrap();
    let report = verify(&r, &full(), &Quiet).await.unwrap();
    assert!(report.ok(), "{:#?}", report.problems);
    assert_eq!(report.stats.changesets_reachable, 3000);
    assert_eq!(report.derived["generation"].checked, 3000);
}
