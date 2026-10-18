//! Durability order (ADR 0044): no changeset is stored, so none is
//! acknowledged, while a blob it names (a content or a node of its tree) has
//! not been flushed by the blob store. The stores below stand in for a
//! filesystem store that defers its directory syncs: a put is "unsynced"
//! until the next `flush`, and the metadata store checks every changeset
//! write against that.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use leep_core::api::{Authorship, LandResponse, MergeRequest, MergeResponse};
use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
use leep_core::{
    BlobMeta, BlobStore, Changeset, ChangesetId, DerivedRow, FileChange, FileKind, Hash, MetaStore,
    RefLogEntry, RefStep, RefUpdateContext, RepoPath, Result, TreeEntry, TreeNode,
};

use crate::derived::RootTree;
use crate::Repo;

/// Blobs whose puts are unsynced until the next flush.
#[derive(Default)]
struct Deferred {
    inner: MemoryBlobStore,
    /// Bytes of every blob, readable without a runtime for the tree walk.
    copies: Mutex<HashMap<Hash, Bytes>>,
    /// Put through this store: any flush syncs them.
    unsynced: Mutex<HashSet<Hash>>,
    /// Put by "another process": only a flush that names them syncs them.
    elsewhere: Mutex<HashSet<Hash>>,
    flushes: Mutex<usize>,
}

impl Deferred {
    /// A blob another process wrote and has not synced: present, and only
    /// durable once a flush names it.
    async fn put_elsewhere(&self, data: &[u8]) -> Hash {
        let data = Bytes::copy_from_slice(data);
        let h = self.inner.put(data.clone()).await.unwrap();
        self.copies.lock().unwrap().insert(h, data);
        self.elsewhere.lock().unwrap().insert(h);
        h
    }

    fn is_unsynced(&self, h: &Hash) -> bool {
        self.unsynced.lock().unwrap().contains(h) || self.elsewhere.lock().unwrap().contains(h)
    }

    fn flushes(&self) -> usize {
        *self.flushes.lock().unwrap()
    }
}

#[async_trait]
impl BlobStore for Deferred {
    async fn put_with_hash(&self, hash: Hash, data: Bytes) -> Result<()> {
        self.inner.put_with_hash(hash, data.clone()).await?;
        self.copies.lock().unwrap().insert(hash, data);
        self.unsynced.lock().unwrap().insert(hash);
        Ok(())
    }

    async fn get(&self, hash: &Hash) -> Result<Option<Bytes>> {
        self.inner.get(hash).await
    }

    async fn list_page(&self, after: Option<Hash>, limit: usize) -> Result<Vec<BlobMeta>> {
        self.inner.list_page(after, limit).await
    }

    async fn delete(&self, hash: &Hash) -> Result<()> {
        self.inner.delete(hash).await
    }

    async fn flush(&self, named: &[Hash]) -> Result<()> {
        *self.flushes.lock().unwrap() += 1;
        self.unsynced.lock().unwrap().clear();
        self.elsewhere
            .lock()
            .unwrap()
            .retain(|h| !named.contains(h));
        Ok(())
    }

    /// Syncs only what it is told, so a caller that relied on another
    /// request's flush, or on a blanket one, is caught.
    async fn flush_blobs(&self, hashes: &[Hash]) -> Result<()> {
        *self.flushes.lock().unwrap() += 1;
        let named: HashSet<Hash> = hashes.iter().copied().collect();
        self.unsynced.lock().unwrap().retain(|h| !named.contains(h));
        self.elsewhere
            .lock()
            .unwrap()
            .retain(|h| !named.contains(h));
        Ok(())
    }
}

/// Refuses (records) any changeset write naming an unsynced blob.
struct Checked {
    inner: MemoryMetaStore,
    blobs: Arc<Deferred>,
    violations: Mutex<Vec<String>>,
    writes: Mutex<usize>,
}

impl Checked {
    fn check(&self, cs: &Changeset, rows: &[DerivedRow]) {
        *self.writes.lock().unwrap() += 1;
        let id = cs.id();
        let mut bad = Vec::new();
        for (path, change) in &cs.changes {
            if let FileChange::Modify { content, .. } = change {
                if self.blobs.is_unsynced(content) {
                    bad.push(format!("content of {path}"));
                }
            }
        }
        let root = rows
            .iter()
            .find(|r| r.kind == RootTree::NAME_STR && r.key == id.0)
            .map(|r| Hash(r.value.as_ref().try_into().unwrap()));
        // Walk the tree iteratively; every node must be synced.
        let mut stack: Vec<Hash> = root.into_iter().collect();
        while let Some(h) = stack.pop() {
            if self.blobs.is_unsynced(&h) {
                bad.push(format!("tree node {h}"));
            }
            let Some(bytes) = self.blobs.copies.lock().unwrap().get(&h).cloned() else {
                bad.push(format!("tree node {h} absent"));
                continue;
            };
            let node = TreeNode::decode(&bytes).unwrap();
            for e in node.entries.values() {
                if let TreeEntry::Dir { tree } = e {
                    stack.push(*tree);
                }
            }
        }
        if !bad.is_empty() {
            self.violations
                .lock()
                .unwrap()
                .push(format!("changeset {id} stored naming unsynced {bad:?}"));
        }
    }

    fn violations(&self) -> Vec<String> {
        self.violations.lock().unwrap().clone()
    }
}

#[async_trait]
impl MetaStore for Checked {
    async fn put_changeset(&self, cs: &Changeset) -> Result<ChangesetId> {
        self.check(cs, &[]);
        self.inner.put_changeset(cs).await
    }

    async fn put_changeset_with_derived(
        &self,
        cs: &Changeset,
        rows: &[DerivedRow],
    ) -> Result<ChangesetId> {
        self.check(cs, rows);
        self.inner.put_changeset_with_derived(cs, rows).await
    }

    async fn put_changesets_with_derived(
        &self,
        changesets: &[Changeset],
        rows: &[DerivedRow],
    ) -> Result<()> {
        for cs in changesets {
            self.check(cs, rows);
        }
        self.inner
            .put_changesets_with_derived(changesets, rows)
            .await
    }

    async fn get_changeset(&self, id: &ChangesetId) -> Result<Option<Changeset>> {
        self.inner.get_changeset(id).await
    }

    async fn list_changesets(
        &self,
        after: Option<ChangesetId>,
        limit: usize,
    ) -> Result<Vec<ChangesetId>> {
        self.inner.list_changesets(after, limit).await
    }

    async fn get_ref(&self, name: &str) -> Result<Option<ChangesetId>> {
        self.inner.get_ref(name).await
    }

    async fn cas_ref_logged(
        &self,
        name: &str,
        expected: Option<ChangesetId>,
        new: Option<ChangesetId>,
        ctx: &RefUpdateContext,
    ) -> Result<bool> {
        self.inner.cas_ref_logged(name, expected, new, ctx).await
    }

    async fn cas_ref_logged_chain(
        &self,
        name: &str,
        expected: Option<ChangesetId>,
        steps: &[RefStep],
    ) -> Result<usize> {
        self.inner.cas_ref_logged_chain(name, expected, steps).await
    }

    async fn ref_log(
        &self,
        name: Option<&str>,
        before_seq: Option<u64>,
        limit: usize,
    ) -> Result<Vec<RefLogEntry>> {
        self.inner.ref_log(name, before_seq, limit).await
    }

    async fn list_refs(&self) -> Result<BTreeMap<String, ChangesetId>> {
        self.inner.list_refs().await
    }

    async fn get_derived(&self, kind: &str, key: &Hash) -> Result<Option<Bytes>> {
        self.inner.get_derived(kind, key).await
    }

    async fn put_derived(&self, kind: &str, key: &Hash, value: Bytes) -> Result<()> {
        self.inner.put_derived(kind, key, value).await
    }
}

fn repo() -> (Repo, Arc<Deferred>, Arc<Checked>) {
    let blobs = Arc::new(Deferred::default());
    let meta = Arc::new(Checked {
        inner: MemoryMetaStore::new(),
        blobs: blobs.clone(),
        violations: Mutex::default(),
        writes: Mutex::default(),
    });
    (Repo::new(blobs.clone(), meta.clone()), blobs, meta)
}

fn changeset(parents: Vec<ChangesetId>, files: &[(&str, &[u8])], msg: &str) -> Changeset {
    let changes = files
        .iter()
        .map(|(p, data)| {
            (
                RepoPath::new(*p).unwrap(),
                FileChange::Modify {
                    content: Hash::of_blob(data),
                    kind: FileKind::Regular,
                    size: data.len() as u64,
                },
            )
        })
        .collect();
    Changeset {
        parents,
        author: "t".into(),
        timestamp: 1_700_000_000,
        message: msg.into(),
        changes,
        extra: BTreeMap::new(),
    }
}

async fn commit(r: &Repo, parents: Vec<ChangesetId>, files: &[(&str, &[u8])]) -> ChangesetId {
    for (_, data) in files {
        r.put_blob(Bytes::copy_from_slice(data)).await.unwrap();
    }
    let msg = format!("{files:?}");
    r.create_changeset(changeset(parents, files, &msg))
        .await
        .unwrap()
}

#[tokio::test]
async fn a_create_flushes_its_contents_and_tree_before_storing() {
    let (r, blobs, meta) = repo();
    let h = r.put_blob(Bytes::from_static(b"one\n")).await.unwrap();
    assert!(blobs.is_unsynced(&h), "an upload alone is not synced");
    let first = commit(&r, vec![], &[("a/b/one.txt", b"one\n")]).await;
    assert!(!blobs.is_unsynced(&h));
    // Uploaded to another process (or before a restart) and not synced
    // there: only a flush that names it covers it.
    let far = blobs.put_elsewhere(b"far\n").await;
    let flushes = blobs.flushes();
    r.create_changeset(changeset(vec![first], &[("a/c/far.txt", b"far\n")], "far"))
        .await
        .unwrap();
    assert!(!blobs.is_unsynced(&far));
    assert_eq!(
        blobs.flushes(),
        flushes + 1,
        "contents and tree in one flush"
    );
    assert_eq!(meta.violations(), Vec::<String>::new());
    assert_eq!(*meta.writes.lock().unwrap(), 2);
}

/// The check above is not vacuous: storing a changeset without the flush
/// is caught.
#[tokio::test]
async fn storing_without_a_flush_is_caught() {
    let (r, blobs, meta) = repo();
    let far = blobs.put_elsewhere(b"far\n").await;
    let cs = changeset(vec![], &[("far.txt", b"far\n")], "unflushed");
    r.meta().put_changeset(&cs).await.unwrap();
    assert!(blobs.is_unsynced(&far));
    assert_eq!(meta.violations().len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batched_lands_store_no_copy_before_its_tree_is_flushed() {
    let (r, _blobs, meta) = repo();
    let base = commit(&r, vec![], &[("base.txt", b"base\n")]).await;
    r.meta().cas_ref("main", None, Some(base)).await.unwrap();
    let agents: Vec<_> = (0..16)
        .map(|i| {
            let r = r.clone();
            tokio::spawn(async move {
                for j in 0..3 {
                    let path = format!("agents/a{i}/f");
                    let data = format!("agent {i} edit {j}\n");
                    let head = r.meta().get_ref("main").await.unwrap();
                    let cs =
                        commit(&r, head.into_iter().collect(), &[(&path, data.as_bytes())]).await;
                    let LandResponse::Landed { .. } = r.land(cs, "main").await.unwrap() else {
                        panic!("agents edit disjoint files");
                    };
                }
            })
        })
        .collect();
    for a in agents {
        a.await.unwrap();
    }
    assert_eq!(meta.violations(), Vec::<String>::new());
}

#[tokio::test]
async fn a_merge_flushes_the_merged_content() {
    let (r, blobs, meta) = repo();
    let base = commit(&r, vec![], &[("f.txt", b"a\nb\nc\n")]).await;
    let ours = commit(&r, vec![base], &[("f.txt", b"A\nb\nc\n")]).await;
    let theirs = commit(&r, vec![base], &[("f.txt", b"a\nb\nC\n")]).await;
    let MergeResponse::Merged { id } = r
        .merge(MergeRequest {
            parents: [ours, theirs],
            message: "merge".into(),
            authorship: Authorship {
                author: "t".into(),
                timestamp: Some(1_700_000_000),
            },
            extra: BTreeMap::new(),
        })
        .await
        .unwrap()
    else {
        panic!("the edits do not overlap");
    };
    let merged = r.get_changeset(id).await.unwrap();
    let Some(FileChange::Modify { content, .. }) = merged.changes.values().next() else {
        panic!("the merge changes f.txt");
    };
    assert!(!blobs.is_unsynced(content));
    assert_eq!(meta.violations(), Vec::<String>::new());
}
