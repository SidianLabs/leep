//! In-memory stores for tests and single-process use.

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;

use async_trait::async_trait;
use bytes::Bytes;

use crate::{BlobStore, Changeset, ChangesetId, Hash, MetaStore, Result};

#[derive(Default)]
pub struct MemoryBlobStore {
    blobs: Mutex<HashMap<Hash, Bytes>>,
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
}

#[async_trait]
impl BlobStore for MemoryBlobStore {
    async fn put_with_hash(&self, hash: Hash, data: Bytes) -> Result<()> {
        self.blobs.lock().unwrap().entry(hash).or_insert(data);
        Ok(())
    }

    async fn get(&self, hash: &Hash) -> Result<Option<Bytes>> {
        Ok(self.blobs.lock().unwrap().get(hash).cloned())
    }
}

#[derive(Default)]
pub struct MemoryMetaStore {
    changesets: Mutex<HashMap<ChangesetId, Changeset>>,
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
        self.changesets.lock().unwrap().entry(id).or_insert_with(|| cs.clone());
        Ok(id)
    }

    async fn get_changeset(&self, id: &ChangesetId) -> Result<Option<Changeset>> {
        Ok(self.changesets.lock().unwrap().get(id).cloned())
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
        Ok(self.derived.lock().unwrap().get(&(kind.to_string(), *key)).cloned())
    }

    async fn put_derived(&self, kind: &str, key: &Hash, value: Bytes) -> Result<()> {
        self.derived.lock().unwrap().insert((kind.to_string(), *key), value);
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
        FileChange::Modify { content: Hash::of_blob(b"x"), kind: FileKind::Regular, size: 1 },
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
    assert_eq!(store.put_changeset(&cs).await.unwrap(), id, "put is idempotent");
    assert_eq!(store.get_changeset(&id).await.unwrap().as_ref(), Some(&cs));
    let missing = ChangesetId(Hash::of_blob(b"missing"));
    assert!(store.get_changeset(&missing).await.unwrap().is_none());

    assert!(store.get_ref("main").await.unwrap().is_none());
    assert!(!store.cas_ref("main", Some(id), Some(id)).await.unwrap(), "wrong expected");
    assert!(store.cas_ref("main", None, Some(id)).await.unwrap());
    assert!(!store.cas_ref("main", None, Some(id)).await.unwrap(), "already exists");
    assert_eq!(store.get_ref("main").await.unwrap(), Some(id));
    assert_eq!(store.list_refs().await.unwrap().get("main"), Some(&id));
    assert!(store.cas_ref("main", Some(id), None).await.unwrap(), "delete");
    assert!(store.get_ref("main").await.unwrap().is_none());

    let key = Hash::of_blob(b"k");
    assert!(store.get_derived("kind", &key).await.unwrap().is_none());
    store.put_derived("kind", &key, Bytes::from_static(b"v")).await.unwrap();
    store.put_derived("kind", &key, Bytes::from_static(b"v")).await.unwrap();
    assert_eq!(store.get_derived("kind", &key).await.unwrap().unwrap(), &b"v"[..]);
    assert!(store.get_derived("other", &key).await.unwrap().is_none());
}

/// Conformance checks every `BlobStore` implementation should pass.
pub async fn check_blob_store_contract(store: &dyn BlobStore) {
    let data = Bytes::from_static(b"hello world");
    let h = store.put(data.clone()).await.unwrap();
    assert_eq!(h, Hash::of_blob(&data));
    assert_eq!(store.put(data.clone()).await.unwrap(), h, "put is idempotent");
    assert_eq!(store.get(&h).await.unwrap().unwrap(), data);
    assert!(store.contains(&h).await.unwrap());
    let other = Hash::of_blob(b"absent");
    assert!(store.get(&other).await.unwrap().is_none());
    assert!(!store.contains(&other).await.unwrap());

    let custom = Hash::of_object("tree-v1", b"node");
    store.put_with_hash(custom, Bytes::from_static(b"node")).await.unwrap();
    assert_eq!(store.get(&custom).await.unwrap().unwrap(), &b"node"[..]);

    let empty = store.put(Bytes::new()).await.unwrap();
    assert_eq!(store.get(&empty).await.unwrap().unwrap().len(), 0);
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
