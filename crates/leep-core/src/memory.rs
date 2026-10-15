//! In-memory stores for tests and single-process use.

use std::collections::{BTreeMap, HashMap};
use std::ops::Bound;
use std::sync::Mutex;

use async_trait::async_trait;
use bytes::Bytes;

use crate::store::BlobMeta;
use crate::{BlobStore, Changeset, ChangesetId, Hash, MetaStore, Result};

/// Unix seconds now, for blob modification times.
fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() as i64)
}

struct Entry {
    data: Bytes,
    modified: Option<i64>,
}

#[derive(Default)]
pub struct MemoryBlobStore {
    blobs: Mutex<BTreeMap<Hash, Entry>>,
}

impl MemoryBlobStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.blobs.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Override a blob's modification time (`None` = unknown), for tests of
    /// age-based behaviour. Returns false if the blob is absent.
    pub fn set_modified(&self, hash: &Hash, modified: Option<i64>) -> bool {
        match self.blobs.lock().unwrap().get_mut(hash) {
            Some(e) => {
                e.modified = modified;
                true
            }
            None => false,
        }
    }
}

#[async_trait]
impl BlobStore for MemoryBlobStore {
    async fn put_with_hash(&self, hash: Hash, data: Bytes) -> Result<()> {
        let now = Some(now_secs());
        self.blobs
            .lock()
            .unwrap()
            .entry(hash)
            .and_modify(|e| e.modified = now)
            .or_insert(Entry {
                data,
                modified: now,
            });
        Ok(())
    }

    async fn get(&self, hash: &Hash) -> Result<Option<Bytes>> {
        Ok(self.blobs.lock().unwrap().get(hash).map(|e| e.data.clone()))
    }

    async fn list_page(&self, after: Option<Hash>, limit: usize) -> Result<Vec<BlobMeta>> {
        let lower = after.map_or(Bound::Unbounded, Bound::Excluded);
        Ok(self
            .blobs
            .lock()
            .unwrap()
            .range((lower, Bound::Unbounded))
            .take(limit)
            .map(|(h, e)| BlobMeta {
                hash: *h,
                size: e.data.len() as u64,
                modified: e.modified,
            })
            .collect())
    }

    async fn delete(&self, hash: &Hash) -> Result<()> {
        self.blobs.lock().unwrap().remove(hash);
        Ok(())
    }
}

#[derive(Default)]
pub struct MemoryMetaStore {
    changesets: Mutex<BTreeMap<ChangesetId, Changeset>>,
    refs: Mutex<BTreeMap<String, ChangesetId>>,
    derived: Mutex<HashMap<(String, Hash), Bytes>>,
}

impl MemoryMetaStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl MetaStore for MemoryMetaStore {
    async fn put_changeset(&self, cs: &Changeset) -> Result<ChangesetId> {
        let id = cs.id();
        self.changesets
            .lock()
            .unwrap()
            .entry(id)
            .or_insert_with(|| cs.clone());
        Ok(id)
    }

    async fn get_changeset(&self, id: &ChangesetId) -> Result<Option<Changeset>> {
        Ok(self.changesets.lock().unwrap().get(id).cloned())
    }

    async fn list_changesets(
        &self,
        after: Option<ChangesetId>,
        limit: usize,
    ) -> Result<Vec<ChangesetId>> {
        let lower = after.map_or(Bound::Unbounded, Bound::Excluded);
        Ok(self
            .changesets
            .lock()
            .unwrap()
            .range((lower, Bound::Unbounded))
            .take(limit)
            .map(|(id, _)| *id)
            .collect())
    }

    async fn get_ref(&self, name: &str) -> Result<Option<ChangesetId>> {
        Ok(self.refs.lock().unwrap().get(name).copied())
    }

    async fn cas_ref(
        &self,
        name: &str,
        expected: Option<ChangesetId>,
        new: Option<ChangesetId>,
    ) -> Result<bool> {
        let mut refs = self.refs.lock().unwrap();
        if refs.get(name).copied() != expected {
            return Ok(false);
        }
        match new {
            Some(id) => refs.insert(name.to_string(), id),
            None => refs.remove(name),
        };
        Ok(true)
    }

    async fn list_refs(&self) -> Result<BTreeMap<String, ChangesetId>> {
        Ok(self.refs.lock().unwrap().clone())
    }

    async fn get_derived(&self, kind: &str, key: &Hash) -> Result<Option<Bytes>> {
        Ok(self
            .derived
            .lock()
            .unwrap()
            .get(&(kind.to_string(), *key))
            .cloned())
    }

    async fn put_derived(&self, kind: &str, key: &Hash, value: Bytes) -> Result<()> {
        self.derived
            .lock()
            .unwrap()
            .insert((kind.to_string(), *key), value);
        Ok(())
    }
}

/// Conformance checks every `MetaStore` implementation should pass. Backend
/// crates call this from their own tests.
pub async fn check_meta_store_contract(store: &dyn MetaStore) {
    use crate::{FileChange, FileKind, RepoPath};

    let mut changes = BTreeMap::new();
    changes.insert(
        RepoPath::new("x").unwrap(),
        FileChange::Modify {
            content: Hash::of_blob(b"x"),
            kind: FileKind::Regular,
            size: 1,
        },
    );
    let cs = Changeset {
        parents: vec![],
        author: "t".into(),
        timestamp: 1,
        message: "m".into(),
        changes,
        extra: BTreeMap::new(),
    };
    let id = store.put_changeset(&cs).await.unwrap();
    assert_eq!(id, cs.id());
    assert_eq!(
        store.put_changeset(&cs).await.unwrap(),
        id,
        "put is idempotent"
    );
    assert_eq!(store.get_changeset(&id).await.unwrap().as_ref(), Some(&cs));
    let missing = ChangesetId(Hash::of_blob(b"missing"));
    assert!(store.get_changeset(&missing).await.unwrap().is_none());

    assert!(store.get_ref("main").await.unwrap().is_none());
    assert!(
        !store.cas_ref("main", Some(id), Some(id)).await.unwrap(),
        "wrong expected"
    );
    assert!(store.cas_ref("main", None, Some(id)).await.unwrap());
    assert!(
        !store.cas_ref("main", None, Some(id)).await.unwrap(),
        "already exists"
    );
    assert_eq!(store.get_ref("main").await.unwrap(), Some(id));
    assert_eq!(store.list_refs().await.unwrap().get("main"), Some(&id));
    assert!(
        store.cas_ref("main", Some(id), None).await.unwrap(),
        "delete"
    );
    assert!(store.get_ref("main").await.unwrap().is_none());

    check_list_changesets(store).await;

    let key = Hash::of_blob(b"k");
    assert!(store.get_derived("kind", &key).await.unwrap().is_none());
    store
        .put_derived("kind", &key, Bytes::from_static(b"v"))
        .await
        .unwrap();
    store
        .put_derived("kind", &key, Bytes::from_static(b"v"))
        .await
        .unwrap();
    assert_eq!(
        store.get_derived("kind", &key).await.unwrap().unwrap(),
        &b"v"[..]
    );
    assert!(store.get_derived("other", &key).await.unwrap().is_none());
}

/// Paging over changesets: ascending, exclusive cursor, complete, no overlap.
async fn check_list_changesets(store: &dyn MetaStore) {
    let mut added = Vec::new();
    for n in 0..7 {
        let cs = Changeset {
            parents: vec![],
            author: "t".into(),
            timestamp: 1000 + n,
            message: format!("list {n}"),
            changes: BTreeMap::new(),
            extra: BTreeMap::new(),
        };
        added.push(store.put_changeset(&cs).await.unwrap());
    }
    let mut all = Vec::new();
    let mut after = None;
    loop {
        let page = store.list_changesets(after, 3).await.unwrap();
        assert!(page.len() <= 3, "page exceeds limit");
        let Some(last) = page.last().copied() else {
            break;
        };
        all.extend(page);
        after = Some(last);
    }
    assert!(all.windows(2).all(|w| w[0] < w[1]), "ascending, no repeats");
    for id in &added {
        assert!(all.contains(id), "listing is complete");
    }
    let big = store.list_changesets(None, 1000).await.unwrap();
    assert_eq!(big, all, "page size does not change the listing");
    let last = *all.last().unwrap();
    assert!(store
        .list_changesets(Some(last), 10)
        .await
        .unwrap()
        .is_empty());
    let first = store.list_changesets(None, 1).await.unwrap();
    assert_eq!(first, vec![all[0]]);
}

/// Conformance checks every `BlobStore` implementation should pass.
pub async fn check_blob_store_contract(store: &dyn BlobStore) {
    let data = Bytes::from_static(b"hello world");
    let h = store.put(data.clone()).await.unwrap();
    assert_eq!(h, Hash::of_blob(&data));
    assert_eq!(
        store.put(data.clone()).await.unwrap(),
        h,
        "put is idempotent"
    );
    assert_eq!(store.get(&h).await.unwrap().unwrap(), data);
    assert!(store.contains(&h).await.unwrap());
    let other = Hash::of_blob(b"absent");
    assert!(store.get(&other).await.unwrap().is_none());
    assert!(!store.contains(&other).await.unwrap());

    let custom = Hash::of_object("tree-v1", b"node");
    store
        .put_with_hash(custom, Bytes::from_static(b"node"))
        .await
        .unwrap();
    assert_eq!(store.get(&custom).await.unwrap().unwrap(), &b"node"[..]);

    let empty = store.put(Bytes::new()).await.unwrap();
    assert_eq!(store.get(&empty).await.unwrap().unwrap().len(), 0);

    check_blob_list_and_delete(store).await;
}

async fn list_all_blobs(store: &dyn BlobStore, page: usize) -> Vec<BlobMeta> {
    let mut all = Vec::new();
    let mut after = None;
    loop {
        let batch = store.list_page(after, page).await.unwrap();
        assert!(batch.len() <= page, "page exceeds limit");
        let Some(last) = batch.last().map(|m| m.hash) else {
            return all;
        };
        all.extend(batch);
        after = Some(last);
    }
}

/// Paging, sizes, mtimes and idempotent delete.
async fn check_blob_list_and_delete(store: &dyn BlobStore) {
    let mut ours = BTreeMap::new();
    for n in 0..20u32 {
        let data = Bytes::from(format!("list-delete blob {n}").repeat(n as usize + 1));
        ours.insert(store.put(data.clone()).await.unwrap(), data.len() as u64);
    }
    let all = list_all_blobs(store, 3).await;
    assert!(
        all.windows(2).all(|w| w[0].hash < w[1].hash),
        "ascending hash order, no repeats"
    );
    for (h, size) in &ours {
        let m = all
            .iter()
            .find(|m| m.hash == *h)
            .expect("listing is complete");
        assert_eq!(m.size, *size, "listed size");
    }
    let now = now_secs();
    for m in &all {
        if let Some(t) = m.modified {
            assert!((now - 3600..=now + 3600).contains(&t), "mtime near now");
        }
    }
    assert_eq!(
        list_all_blobs(store, 1000).await,
        all,
        "page size does not change the listing"
    );
    let first = store.list_page(None, 1).await.unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(first[0], all[0]);
    let past_end = all.last().unwrap().hash;
    assert!(store.list_page(Some(past_end), 5).await.unwrap().is_empty());

    let doomed: Vec<Hash> = ours.keys().step_by(2).copied().collect();
    for h in &doomed {
        store.delete(h).await.unwrap();
        store.delete(h).await.unwrap();
        assert!(store.get(h).await.unwrap().is_none());
        assert!(!store.contains(h).await.unwrap());
    }
    store.delete(&Hash::of_blob(b"never stored")).await.unwrap();
    let after = list_all_blobs(store, 4).await;
    for h in ours.keys() {
        let listed = after.iter().any(|m| m.hash == *h);
        assert_eq!(listed, !doomed.contains(h), "only deleted blobs vanish");
        assert_eq!(store.contains(h).await.unwrap(), !doomed.contains(h));
    }
    let again = store.put(Bytes::from("list-delete blob 0")).await.unwrap();
    assert_eq!(
        store.get(&again).await.unwrap().unwrap(),
        &b"list-delete blob 0"[..]
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn memory_stores_pass_contract() {
        check_meta_store_contract(&MemoryMetaStore::new()).await;
        check_blob_store_contract(&MemoryBlobStore::new()).await;
    }
}
