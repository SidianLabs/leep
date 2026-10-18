//! In-memory stores for tests and single-process use.

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Mutex;

use async_trait::async_trait;
use bytes::Bytes;

use crate::prefix::IdPrefix;
use crate::store::{
    check_ref_chain, unix_now, BlobMeta, DerivedRow, RefLogEntry, RefStep, RefUpdateContext,
};
use crate::{BlobStore, Changeset, ChangesetId, Hash, MetaStore, Result};

/// Unix seconds now, for blob modification times.
fn now_secs() -> i64 {
    unix_now()
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
    refs: Mutex<Refs>,
    derived: Mutex<BTreeMap<(String, Hash), Bytes>>,
}

/// Refs and their log share one lock, so a move and its entry are one step.
#[derive(Default)]
struct Refs {
    heads: BTreeMap<String, ChangesetId>,
    /// Ascending by `seq`.
    log: Vec<RefLogEntry>,
}

impl MemoryMetaStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Set a ref without logging the move, as a damaged database would show
    /// it. For tests of verification and repair only.
    #[doc(hidden)]
    pub fn set_ref_unlogged(&self, name: &str, target: Option<ChangesetId>) {
        let mut refs = self.refs.lock().unwrap();
        match target {
            Some(id) => refs.heads.insert(name.to_string(), id),
            None => refs.heads.remove(name),
        };
    }

    /// Remove one ref log entry. Later moves keep their `seq`, so the log has
    /// a hole. For tests of verification and repair only.
    #[doc(hidden)]
    pub fn remove_log_entry(&self, seq: u64) -> bool {
        let mut refs = self.refs.lock().unwrap();
        let before = refs.log.len();
        refs.log.retain(|e| e.seq != seq);
        refs.log.len() != before
    }

    /// Delete a stored changeset, which the real stores never do. For tests
    /// of verification and repair only.
    #[doc(hidden)]
    pub fn remove_changeset(&self, id: &ChangesetId) -> bool {
        self.changesets.lock().unwrap().remove(id).is_some()
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

    async fn get_changesets(&self, ids: &[ChangesetId]) -> Result<Vec<Option<Changeset>>> {
        let all = self.changesets.lock().unwrap();
        Ok(ids.iter().map(|id| all.get(id).cloned()).collect())
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

    async fn changesets_with_prefix(
        &self,
        prefix: &IdPrefix,
        limit: usize,
    ) -> Result<Vec<ChangesetId>> {
        Ok(self
            .changesets
            .lock()
            .unwrap()
            .range(prefix.lower()..=prefix.upper())
            .take(limit)
            .map(|(id, _)| *id)
            .collect())
    }

    async fn get_ref(&self, name: &str) -> Result<Option<ChangesetId>> {
        Ok(self.refs.lock().unwrap().heads.get(name).copied())
    }

    async fn cas_ref_logged(
        &self,
        name: &str,
        expected: Option<ChangesetId>,
        new: Option<ChangesetId>,
        ctx: &RefUpdateContext,
    ) -> Result<bool> {
        let mut refs = self.refs.lock().unwrap();
        if refs.heads.get(name).copied() != expected {
            return Ok(false);
        }
        if expected == new {
            return Ok(true);
        }
        match new {
            Some(id) => refs.heads.insert(name.to_string(), id),
            None => refs.heads.remove(name),
        };
        let seq = refs.log.last().map_or(0, |e| e.seq) + 1;
        refs.log.push(RefLogEntry {
            seq,
            name: name.to_string(),
            old: expected,
            new,
            time: unix_now(),
            actor: ctx.actor.clone(),
            operation: ctx.operation.clone(),
            message: ctx.message.clone(),
        });
        Ok(true)
    }

    async fn cas_ref_logged_chain(
        &self,
        name: &str,
        expected: Option<ChangesetId>,
        steps: &[RefStep],
    ) -> Result<usize> {
        check_ref_chain(expected, steps)?;
        let mut refs = self.refs.lock().unwrap();
        if refs.heads.get(name).copied() != expected {
            return Ok(0);
        }
        let mut old = expected;
        for step in steps {
            let seq = refs.log.last().map_or(0, |e| e.seq) + 1;
            refs.log.push(RefLogEntry {
                seq,
                name: name.to_string(),
                old,
                new: step.new,
                time: unix_now(),
                actor: step.ctx.actor.clone(),
                operation: step.ctx.operation.clone(),
                message: step.ctx.message.clone(),
            });
            old = step.new;
        }
        match old {
            Some(id) => refs.heads.insert(name.to_string(), id),
            None => refs.heads.remove(name),
        };
        Ok(steps.len())
    }

    async fn ref_log(
        &self,
        name: Option<&str>,
        before_seq: Option<u64>,
        limit: usize,
    ) -> Result<Vec<RefLogEntry>> {
        let refs = self.refs.lock().unwrap();
        Ok(refs
            .log
            .iter()
            .rev()
            .filter(|e| before_seq.is_none_or(|b| e.seq < b))
            .filter(|e| name.is_none_or(|n| e.name == n))
            .take(limit)
            .cloned()
            .collect())
    }

    async fn list_refs(&self) -> Result<BTreeMap<String, ChangesetId>> {
        Ok(self.refs.lock().unwrap().heads.clone())
    }

    async fn get_derived(&self, kind: &str, key: &Hash) -> Result<Option<Bytes>> {
        Ok(self
            .derived
            .lock()
            .unwrap()
            .get(&(kind.to_string(), *key))
            .cloned())
    }

    async fn get_derived_many(&self, kind: &str, keys: &[Hash]) -> Result<Vec<Option<Bytes>>> {
        let rows = self.derived.lock().unwrap();
        let mut k = (kind.to_string(), Hash([0; 32]));
        Ok(keys
            .iter()
            .map(|key| {
                k.1 = *key;
                rows.get(&k).cloned()
            })
            .collect())
    }

    async fn list_derived(
        &self,
        kind: &str,
        after: Option<Hash>,
        limit: usize,
    ) -> Result<Vec<(Hash, Bytes)>> {
        let lower = match after {
            Some(a) => Bound::Excluded((kind.to_string(), a)),
            None => Bound::Included((kind.to_string(), Hash([0; 32]))),
        };
        Ok(self
            .derived
            .lock()
            .unwrap()
            .range((lower, Bound::Unbounded))
            .take_while(|((k, _), _)| k == kind)
            .take(limit)
            .map(|((_, key), v)| (*key, v.clone()))
            .collect())
    }

    async fn delete_derived_kind(&self, kind: &str) -> Result<u64> {
        let mut rows = self.derived.lock().unwrap();
        let before = rows.len();
        rows.retain(|(k, _), _| k != kind);
        Ok((before - rows.len()) as u64)
    }

    async fn put_derived(&self, kind: &str, key: &Hash, value: Bytes) -> Result<()> {
        self.derived
            .lock()
            .unwrap()
            .insert((kind.to_string(), *key), value);
        Ok(())
    }

    async fn put_derived_many(&self, rows: &[DerivedRow]) -> Result<()> {
        let mut all = self.derived.lock().unwrap();
        for r in rows {
            all.insert((r.kind.clone(), r.key), r.value.clone());
        }
        Ok(())
    }

    /// Both maps are locked together, so no reader sees the changeset
    /// without its rows.
    async fn put_changeset_with_derived(
        &self,
        cs: &Changeset,
        rows: &[DerivedRow],
    ) -> Result<ChangesetId> {
        let id = cs.id();
        let mut changesets = self.changesets.lock().unwrap();
        let mut all = self.derived.lock().unwrap();
        changesets.entry(id).or_insert_with(|| cs.clone());
        for r in rows {
            all.insert((r.kind.clone(), r.key), r.value.clone());
        }
        Ok(id)
    }

    async fn put_changesets_with_derived(
        &self,
        changesets: &[Changeset],
        rows: &[DerivedRow],
    ) -> Result<()> {
        let mut stored = self.changesets.lock().unwrap();
        let mut all = self.derived.lock().unwrap();
        for cs in changesets {
            stored.entry(cs.id()).or_insert_with(|| cs.clone());
        }
        for r in rows {
            all.insert((r.kind.clone(), r.key), r.value.clone());
        }
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
    check_prefix_lookup(store).await;
    check_ref_log(store).await;
    check_ref_chain_log(store).await;

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

    check_list_derived(store).await;
    check_batched_reads(store).await;
    check_batched_writes(store).await;
}

/// The batched writes store what the single writes would, applied in order:
/// a key given twice keeps its last value, an empty batch writes nothing,
/// repeating a write is harmless, and rows reach exactly their kind. A
/// changeset written with its rows is readable with all of them, and writing
/// it again still writes the rows it comes with.
async fn check_batched_writes(store: &dyn MetaStore) {
    let key = |n: u32| Hash::of_blob(format!("batch write {n}").as_bytes());
    let row =
        |kind: &str, n: u32, v: &str| DerivedRow::new(kind, key(n), Bytes::from(v.to_string()));
    let get =
        |kind: &'static str, n: u32| async move { store.get_derived(kind, &key(n)).await.unwrap() };

    store.put_derived_many(&[]).await.unwrap();
    store
        .put_derived("bw-a", &key(0), Bytes::from_static(b"old"))
        .await
        .unwrap();
    let rows = [
        row("bw-a", 0, "new"),
        row("bw-a", 1, "first"),
        row("bw-b", 1, "other kind"),
        row("bw-a", 1, "last"),
        row("bw-a", 2, "two"),
    ];
    store.put_derived_many(&rows).await.unwrap();
    store.put_derived_many(&rows).await.unwrap();
    assert_eq!(get("bw-a", 0).await.unwrap(), &b"new"[..], "overwrites");
    assert_eq!(get("bw-a", 1).await.unwrap(), &b"last"[..], "last wins");
    assert_eq!(get("bw-b", 1).await.unwrap(), &b"other kind"[..]);
    assert_eq!(get("bw-a", 2).await.unwrap(), &b"two"[..]);
    assert!(get("bw-b", 0).await.is_none(), "kinds stay apart");
    assert_eq!(
        store.list_derived("bw-a", None, 10).await.unwrap().len(),
        3,
        "no stray rows"
    );
    // Larger than any reasonable statement parameter limit per batch.
    let many: Vec<DerivedRow> = (0..2500)
        .map(|n| row("bw-many", n % 2000, &format!("v{n}")))
        .collect();
    store.put_derived_many(&many).await.unwrap();
    let listed = store.list_derived("bw-many", None, 5000).await.unwrap();
    assert_eq!(listed.len(), 2000);
    for (k, v) in [(5, "v2005"), (1999, "v1999"), (0, "v2000")] {
        assert_eq!(get("bw-many", k).await.unwrap(), v.as_bytes(), "row {k}");
    }

    let cs = |n: i64| Changeset {
        parents: vec![],
        author: "t".into(),
        timestamp: 4000 + n,
        message: format!("with rows {n}"),
        changes: BTreeMap::new(),
        extra: BTreeMap::new(),
    };
    let one = cs(1);
    let rows = [row("bw-cs", 10, "ten"), row("bw-cs", 11, "eleven")];
    let id = store.put_changeset_with_derived(&one, &rows).await.unwrap();
    assert_eq!(id, one.id());
    assert_eq!(store.get_changeset(&id).await.unwrap().as_ref(), Some(&one));
    assert_eq!(get("bw-cs", 10).await.unwrap(), &b"ten"[..]);
    assert_eq!(get("bw-cs", 11).await.unwrap(), &b"eleven"[..]);
    assert_eq!(
        store.put_changeset_with_derived(&one, &rows).await.unwrap(),
        id,
        "idempotent"
    );
    // A stored changeset still gets the rows it comes with.
    store
        .put_changeset_with_derived(&one, &[row("bw-cs", 12, "twelve")])
        .await
        .unwrap();
    assert_eq!(get("bw-cs", 12).await.unwrap(), &b"twelve"[..]);
    let two = cs(2);
    let id2 = store.put_changeset_with_derived(&two, &[]).await.unwrap();
    assert_eq!(
        store.get_changeset(&id2).await.unwrap().as_ref(),
        Some(&two)
    );
    let three = cs(3);
    let id3 = store
        .put_changeset_with_derived(&three, &many)
        .await
        .unwrap();
    assert_eq!(
        store.get_changeset(&id3).await.unwrap().as_ref(),
        Some(&three)
    );
    // Many changesets and rows at once, a key given twice keeping its last
    // value, and already stored changesets accepted again.
    let (four, five) = (cs(4), cs(5));
    let batch_rows = [
        row("bw-css", 1, "first"),
        row("bw-css", 2, "two"),
        row("bw-css", 1, "last"),
    ];
    store
        .put_changesets_with_derived(&[four.clone(), five.clone(), one.clone()], &batch_rows)
        .await
        .unwrap();
    for c in [&four, &five, &one] {
        assert_eq!(
            store.get_changeset(&c.id()).await.unwrap().as_ref(),
            Some(c)
        );
    }
    assert_eq!(get("bw-css", 1).await.unwrap(), &b"last"[..]);
    assert_eq!(get("bw-css", 2).await.unwrap(), &b"two"[..]);
    store
        .put_changesets_with_derived(std::slice::from_ref(&four), &batch_rows)
        .await
        .unwrap();
    store.put_changesets_with_derived(&[], &[]).await.unwrap();
    for kind in ["bw-a", "bw-b", "bw-many", "bw-cs", "bw-css"] {
        store.delete_derived_kind(kind).await.unwrap();
    }
}

/// The batched reads answer exactly what the single reads do: one slot per
/// requested id in request order, `None` for absent ids, repeats answered at
/// every position, nothing for an empty request, and other kinds untouched.
async fn check_batched_reads(store: &dyn MetaStore) {
    let mut ids = Vec::new();
    let mut want = Vec::new();
    for n in 0..5 {
        let cs = Changeset {
            parents: ids.last().copied().into_iter().collect(),
            author: "t".into(),
            timestamp: 3000 + n,
            message: format!("batch {n}"),
            changes: BTreeMap::new(),
            extra: BTreeMap::from([("k".to_string(), n.to_string())]),
        };
        ids.push(store.put_changeset(&cs).await.unwrap());
        want.push(cs);
    }
    let absent = ChangesetId(Hash::of_blob(b"batch absent"));
    assert!(store.get_changesets(&[]).await.unwrap().is_empty());
    let ask = [ids[3], absent, ids[0], ids[3], ids[4], absent, ids[1]];
    let got = store.get_changesets(&ask).await.unwrap();
    assert_eq!(got.len(), ask.len(), "one slot per id");
    for (id, cs) in ask.iter().zip(&got) {
        assert_eq!(cs.as_ref(), store.get_changeset(id).await.unwrap().as_ref());
    }
    assert_eq!(got[0].as_ref(), Some(&want[3]));
    assert_eq!(got[1], None);
    assert_eq!(got[3], got[0], "a repeat is answered again");
    assert_eq!(
        store.get_changesets(&[absent]).await.unwrap(),
        vec![None],
        "only absent ids"
    );

    let key = |n: u32| Hash::of_blob(format!("batch key {n}").as_bytes());
    for n in 0..4 {
        store
            .put_derived("bd-a", &key(n), Bytes::from(format!("a{n}")))
            .await
            .unwrap();
    }
    store
        .put_derived("bd-b", &key(9), Bytes::from_static(b"b9"))
        .await
        .unwrap();
    assert!(store
        .get_derived_many("bd-a", &[])
        .await
        .unwrap()
        .is_empty());
    let ask = [key(2), key(9), key(0), key(2), key(7), key(3)];
    let got = store.get_derived_many("bd-a", &ask).await.unwrap();
    let want: Vec<Option<Bytes>> = vec![
        Some(Bytes::from_static(b"a2")),
        None,
        Some(Bytes::from_static(b"a0")),
        Some(Bytes::from_static(b"a2")),
        None,
        Some(Bytes::from_static(b"a3")),
    ];
    assert_eq!(got, want, "in order, absent and other-kind keys are None");
    for (k, v) in ask.iter().zip(&got) {
        assert_eq!(v, &store.get_derived("bd-a", k).await.unwrap());
    }
    assert_eq!(
        store.get_derived_many("bd-b", &[key(9)]).await.unwrap(),
        vec![Some(Bytes::from_static(b"b9"))]
    );
    // Larger than any reasonable statement parameter limit per batch.
    let many: Vec<Hash> = (0..2500).map(|n| key(n % 6)).collect();
    let got = store.get_derived_many("bd-a", &many).await.unwrap();
    assert_eq!(got.len(), many.len());
    assert!(got.iter().zip(&many).all(
        |(v, k)| v.is_some() == (*k == key(0) || *k == key(1) || *k == key(2) || *k == key(3))
    ));
    let many_ids: Vec<ChangesetId> = (0..2500)
        .map(|n| if n % 7 == 0 { absent } else { ids[n % 5] })
        .collect();
    let got = store.get_changesets(&many_ids).await.unwrap();
    assert!(got
        .iter()
        .zip(&many_ids)
        .all(|(cs, id)| cs.as_ref().map(Changeset::id) == (*id != absent).then_some(*id)));
    store.delete_derived_kind("bd-a").await.unwrap();
    store.delete_derived_kind("bd-b").await.unwrap();
}

/// Listing derived rows pages in key order without gaps or repeats, stays
/// within one kind, and deleting a kind removes exactly its rows.
async fn check_list_derived(store: &dyn MetaStore) {
    let row = |kind: &str, n: u32| {
        (
            Hash::of_blob(format!("{kind}/{n}").as_bytes()),
            Bytes::from(format!("{kind}:{n}")),
        )
    };
    assert!(store
        .list_derived("ld-none", None, 10)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(store.delete_derived_kind("ld-none").await.unwrap(), 0);

    let mut want_a: Vec<_> = (0..7).map(|n| row("ld-a", n)).collect();
    for (k, v) in &want_a {
        store.put_derived("ld-a", k, v.clone()).await.unwrap();
    }
    for n in 0..3 {
        let (k, v) = row("ld-b", n);
        store.put_derived("ld-b", &k, v).await.unwrap();
    }
    want_a.sort_by_key(|(k, _)| *k);

    let mut got = Vec::new();
    let mut after = None;
    loop {
        let page = store.list_derived("ld-a", after, 3).await.unwrap();
        assert!(page.len() <= 3, "limit is honoured");
        let Some((last, _)) = page.last().cloned() else {
            break;
        };
        after = Some(last);
        got.extend(page);
    }
    assert_eq!(got, want_a, "ascending, complete, one kind only");
    assert!(
        store
            .list_derived("ld-a", Some(want_a[6].0), 10)
            .await
            .unwrap()
            .is_empty(),
        "`after` is exclusive"
    );
    assert_eq!(
        store.list_derived("ld-a", None, 100).await.unwrap().len(),
        7
    );

    assert_eq!(store.delete_derived_kind("ld-a").await.unwrap(), 7);
    assert!(store
        .list_derived("ld-a", None, 100)
        .await
        .unwrap()
        .is_empty());
    for (k, _) in &want_a {
        assert!(store.get_derived("ld-a", k).await.unwrap().is_none());
    }
    assert_eq!(
        store.list_derived("ld-b", None, 100).await.unwrap().len(),
        3
    );
    assert_eq!(
        store.delete_derived_kind("ld-a").await.unwrap(),
        0,
        "idempotent"
    );
    assert_eq!(store.delete_derived_kind("ld-b").await.unwrap(), 3);
}

/// The ref log: one entry per change that happened, none for a lost or empty
/// swap, newest first, filterable by ref and pageable by `seq`.
async fn check_ref_log(store: &dyn MetaStore) {
    let mut ids = Vec::new();
    for n in 0..4 {
        let cs = Changeset {
            parents: vec![],
            author: "t".into(),
            timestamp: 2000 + n,
            message: format!("reflog {n}"),
            changes: BTreeMap::new(),
            extra: BTreeMap::new(),
        };
        ids.push(store.put_changeset(&cs).await.unwrap());
    }
    let (a, b, c, d) = (ids[0], ids[1], ids[2], ids[3]);
    let log =
        |name: &'static str| async move { store.ref_log(Some(name), None, 1000).await.unwrap() };

    assert!(log("rl/x").await.is_empty());
    let land = RefUpdateContext::new("land")
        .actor(Some("alice"))
        .message("landed a");
    let before = now_secs();
    assert!(store
        .cas_ref_logged("rl/x", None, Some(a), &land)
        .await
        .unwrap());
    let entries = log("rl/x").await;
    assert_eq!(entries.len(), 1, "create is logged");
    let e = &entries[0];
    assert_eq!((e.name.as_str(), e.old, e.new), ("rl/x", None, Some(a)));
    assert_eq!(e.actor.as_deref(), Some("alice"));
    assert_eq!(e.operation, "land");
    assert_eq!(e.message.as_deref(), Some("landed a"));
    assert!(
        (before - 5..=now_secs() + 5).contains(&e.time),
        "server clock time"
    );

    // Lost swaps and swaps that change nothing write nothing.
    assert!(!store.cas_ref("rl/x", None, Some(b)).await.unwrap());
    assert!(!store.cas_ref("rl/x", Some(b), Some(c)).await.unwrap());
    assert!(!store.cas_ref("rl/x", Some(b), None).await.unwrap());
    assert!(store.cas_ref("rl/x", Some(a), Some(a)).await.unwrap());
    assert!(store.cas_ref("rl/absent", None, None).await.unwrap());
    assert_eq!(log("rl/x").await.len(), 1, "no entry without a change");
    assert!(log("rl/absent").await.is_empty());

    // `cas_ref` logs with the default context.
    assert!(store.cas_ref("rl/x", Some(a), Some(b)).await.unwrap());
    assert!(store.cas_ref("rl/y", None, Some(c)).await.unwrap());
    let undo = RefUpdateContext::new("undo").actor(None::<String>);
    assert!(store
        .cas_ref_logged("rl/x", Some(b), None, &undo)
        .await
        .unwrap());
    assert!(store
        .cas_ref_logged("rl/x", None, Some(d), &RefUpdateContext::new("import"))
        .await
        .unwrap());
    // A name that merely starts with another is a different ref.
    assert!(store.cas_ref("rl/xy", None, Some(a)).await.unwrap());

    let entries = log("rl/x").await;
    assert_eq!(entries.len(), 4);
    assert!(
        entries.windows(2).all(|w| w[0].seq > w[1].seq),
        "newest first"
    );
    assert!(entries.iter().all(|e| e.name == "rl/x"), "per-ref filter");
    let steps: Vec<_> = entries.iter().rev().map(|e| (e.old, e.new)).collect();
    assert_eq!(
        steps,
        [
            (None, Some(a)),
            (Some(a), Some(b)),
            (Some(b), None),
            (None, Some(d))
        ],
        "create, move, delete and recreate replay to the head"
    );
    let ops: Vec<_> = entries.iter().rev().map(|e| e.operation.as_str()).collect();
    assert_eq!(ops, ["land", "cas", "undo", "import"]);
    assert_eq!(entries[0].actor, None);
    assert_eq!(entries[0].message, None);
    assert_eq!(store.get_ref("rl/x").await.unwrap(), Some(d));

    // Across refs: one sequence, in the order the moves happened.
    let all = store.ref_log(None, None, 1000).await.unwrap();
    assert!(all.windows(2).all(|w| w[0].seq > w[1].seq));
    let pos = |name: &str, new: Option<ChangesetId>| {
        all.iter()
            .position(|e| e.name == name && e.new == new)
            .unwrap()
    };
    assert!(pos("rl/xy", Some(a)) < pos("rl/x", Some(d)), "newest first");
    assert!(pos("rl/x", Some(d)) < pos("rl/y", Some(c)));
    assert!(pos("rl/y", Some(c)) < pos("rl/x", Some(b)));
    let ys = log("rl/y").await;
    assert_eq!((ys.len(), ys[0].old, ys[0].new), (1, None, Some(c)));

    // Paging by `before_seq` is complete, ordered and respects `limit`.
    let mut paged = Vec::new();
    let mut before = None;
    loop {
        let page = store.ref_log(Some("rl/x"), before, 3).await.unwrap();
        assert!(page.len() <= 3, "page exceeds limit");
        let Some(last) = page.last().map(|e| e.seq) else {
            break;
        };
        paged.extend(page);
        before = Some(last);
    }
    assert_eq!(paged, entries, "paging does not change the log");
    let mut paged_all = Vec::new();
    let mut before = None;
    loop {
        let page = store.ref_log(None, before, 2).await.unwrap();
        let Some(last) = page.last().map(|e| e.seq) else {
            break;
        };
        paged_all.extend(page);
        before = Some(last);
    }
    assert_eq!(paged_all, all);
    assert!(store.ref_log(None, None, 0).await.unwrap().is_empty());
    let first = store.ref_log(None, None, 1).await.unwrap();
    assert_eq!(first, all[..1]);
    let oldest = entries.last().unwrap().seq;
    assert!(store
        .ref_log(Some("rl/x"), Some(oldest), 10)
        .await
        .unwrap()
        .is_empty());
}

/// A chained swap logs one entry per step, oldest first and replaying to the
/// head, or nothing when the ref is not where the caller expects.
async fn check_ref_chain_log(store: &dyn MetaStore) {
    let mut ids = Vec::new();
    for n in 0..4 {
        let cs = Changeset {
            parents: vec![],
            author: "t".into(),
            timestamp: 3000 + n,
            message: format!("chain {n}"),
            changes: BTreeMap::new(),
            extra: BTreeMap::new(),
        };
        ids.push(store.put_changeset(&cs).await.unwrap());
    }
    let (a, b, c, d) = (ids[0], ids[1], ids[2], ids[3]);
    let step = |new: ChangesetId, who: &str| RefStep {
        new: Some(new),
        ctx: RefUpdateContext::new("land")
            .actor(Some(who))
            .message(format!("landed by {who}")),
    };
    assert!(store.cas_ref("ch/x", None, Some(a)).await.unwrap());
    let chain = [step(b, "w1"), step(c, "w2"), step(d, "w3")];

    // Not where the caller expects: nothing moves, nothing is logged.
    assert_eq!(
        store
            .cas_ref_logged_chain("ch/x", Some(c), &chain)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        store
            .cas_ref_logged_chain("ch/x", None, &chain)
            .await
            .unwrap(),
        0
    );
    assert_eq!(store.get_ref("ch/x").await.unwrap(), Some(a));
    assert_eq!(
        store.ref_log(Some("ch/x"), None, 100).await.unwrap().len(),
        1
    );

    // Steps that do not move, or a chain back to the start, are refused.
    for bad in [
        vec![],
        vec![step(a, "w")],
        vec![step(b, "w"), step(b, "w")],
        vec![step(b, "w"), step(a, "w")],
    ] {
        assert!(matches!(
            store.cas_ref_logged_chain("ch/x", Some(a), &bad).await,
            Err(crate::Error::Invalid(_))
        ));
    }
    assert_eq!(store.get_ref("ch/x").await.unwrap(), Some(a));

    assert_eq!(
        store
            .cas_ref_logged_chain("ch/x", Some(a), &chain)
            .await
            .unwrap(),
        3
    );
    assert_eq!(store.get_ref("ch/x").await.unwrap(), Some(d));
    let entries = store.ref_log(Some("ch/x"), None, 100).await.unwrap();
    let got: Vec<_> = entries
        .iter()
        .rev()
        .map(|e| (e.old, e.new, e.actor.clone().unwrap_or_default()))
        .collect();
    assert_eq!(
        got,
        [
            (None, Some(a), String::new()),
            (Some(a), Some(b), "w1".into()),
            (Some(b), Some(c), "w2".into()),
            (Some(c), Some(d), "w3".into()),
        ],
        "one entry per step, in order"
    );
    assert_eq!(entries[0].message.as_deref(), Some("landed by w3"));
    assert_eq!(entries[0].operation, "land");

    // A chain may create the ref and may end by deleting it.
    let create = [step(a, "w1"), step(b, "w2")];
    assert_eq!(
        store
            .cas_ref_logged_chain("ch/new", None, &create)
            .await
            .unwrap(),
        2
    );
    assert_eq!(store.get_ref("ch/new").await.unwrap(), Some(b));
    let delete = [
        step(c, "w1"),
        RefStep {
            new: None,
            ctx: RefUpdateContext::new("cas"),
        },
    ];
    assert_eq!(
        store
            .cas_ref_logged_chain("ch/new", Some(b), &delete)
            .await
            .unwrap(),
        2
    );
    assert_eq!(store.get_ref("ch/new").await.unwrap(), None);
    let steps: Vec<_> = store
        .ref_log(Some("ch/new"), None, 100)
        .await
        .unwrap()
        .iter()
        .rev()
        .map(|e| (e.old, e.new))
        .collect();
    assert_eq!(
        steps,
        [
            (None, Some(a)),
            (Some(a), Some(b)),
            (Some(b), Some(c)),
            (Some(c), None)
        ]
    );
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

/// Short id lookup: exactly the stored ids with the prefix, ascending, at
/// most `limit`, for even and odd lengths, at the edges of the id space, and
/// for a prefix two ids share.
async fn check_prefix_lookup(store: &dyn MetaStore) {
    let make = |n: i64| Changeset {
        parents: vec![],
        author: "t".into(),
        timestamp: n,
        message: format!("prefix {n}"),
        changes: BTreeMap::new(),
        extra: BTreeMap::new(),
    };
    // Changesets whose ids share their first four hex digits, found by
    // hashing locally (ids are deterministic), so the store holds a pair.
    let mut seen: BTreeMap<String, Changeset> = BTreeMap::new();
    let (a, b) = (0..)
        .find_map(|n| {
            let cs = make(n);
            let key = cs.id().to_string()[..4].to_string();
            seen.insert(key, cs.clone()).map(|other| (other, cs))
        })
        .unwrap();
    let a = store.put_changeset(&a).await.unwrap();
    let b = store.put_changeset(&b).await.unwrap();
    let extra = store.put_changeset(&make(-1)).await.unwrap();

    let all = store.list_changesets(None, 100_000).await.unwrap();
    let expect = |p: &IdPrefix, limit: usize| -> Vec<ChangesetId> {
        all.iter()
            .filter(|id| p.matches(id))
            .take(limit)
            .copied()
            .collect()
    };
    let lookup = |text: String, limit: usize| async move {
        let p = IdPrefix::parse(&text).unwrap();
        let got = store.changesets_with_prefix(&p, limit).await.unwrap();
        (p, got)
    };

    let shared = a.to_string()[..4].to_string();
    let (p, got) = lookup(shared.clone(), 10).await;
    assert!(got.contains(&a) && got.contains(&b), "{shared} is shared");
    assert_eq!(got, expect(&p, 10));
    assert!(got.windows(2).all(|w| w[0] < w[1]), "ascending");
    let (p, got) = lookup(shared.to_uppercase(), 1).await;
    assert_eq!(got, expect(&p, 1), "limit is respected");
    assert_eq!(got.len(), 1);
    let (_, got) = lookup(shared, 0).await;
    assert!(got.is_empty());

    for id in [a, b, extra] {
        let hex = id.to_string();
        for len in [5, 12, 63, 64] {
            let (p, got) = lookup(hex[..len].to_string(), 10).await;
            assert!(got.contains(&id), "{} finds {id}", &hex[..len]);
            assert_eq!(got, expect(&p, 10));
        }
        let (_, got) = lookup(hex[..12].to_string(), 2).await;
        assert_eq!(got, [id], "12 digits are unique here");
    }

    for edge in [
        "0000",
        "00000",
        "ffff",
        "fffff",
        &"f".repeat(64),
        &"0".repeat(64),
    ] {
        let (p, got) = lookup(edge.to_string(), 10).await;
        assert_eq!(got, expect(&p, 10), "{edge}");
    }
    // A prefix no stored id has.
    let absent = (0u32..)
        .map(|n| format!("{n:06x}"))
        .find(|p| !all.iter().any(|id| id.to_string().starts_with(p.as_str())))
        .unwrap();
    assert!(lookup(absent, 10).await.1.is_empty());
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

    let asked = [other, h, custom, other, empty];
    assert_eq!(
        store.contains_many(&asked).await.unwrap(),
        vec![false, true, true, false, true],
        "contains_many answers in order"
    );
    assert!(store.contains_many(&[]).await.unwrap().is_empty());
    let got = store.get_many(&asked).await.unwrap();
    assert_eq!(
        got,
        vec![
            None,
            Some(data.clone()),
            Some(Bytes::from_static(b"node")),
            None,
            Some(Bytes::new())
        ],
        "get_many answers in order"
    );
    assert!(store.get_many(&[]).await.unwrap().is_empty());
    store.flush(&[]).await.unwrap();
    store.flush(&[h, other]).await.unwrap();
    store.flush_blobs(&[]).await.unwrap();
    store.flush_blobs(&[h, other]).await.unwrap();

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

    /// Forwards everything but `cas_ref_logged_chain`, so the trait's
    /// default (consecutive swaps) is what runs.
    struct DefaultChain(MemoryMetaStore);

    #[async_trait]
    impl MetaStore for DefaultChain {
        async fn put_changeset(&self, cs: &Changeset) -> Result<ChangesetId> {
            self.0.put_changeset(cs).await
        }
        async fn get_changeset(&self, id: &ChangesetId) -> Result<Option<Changeset>> {
            self.0.get_changeset(id).await
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
        async fn get_derived(&self, kind: &str, key: &Hash) -> Result<Option<Bytes>> {
            self.0.get_derived(kind, key).await
        }
        async fn put_derived(&self, kind: &str, key: &Hash, value: Bytes) -> Result<()> {
            self.0.put_derived(kind, key, value).await
        }
    }

    #[tokio::test]
    async fn the_default_ref_chain_meets_the_contract() {
        check_ref_chain_log(&DefaultChain(MemoryMetaStore::new())).await;
    }
}
