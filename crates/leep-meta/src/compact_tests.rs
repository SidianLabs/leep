//! Compressed changeset rows and `compact-changesets` (ADR 0053) on both
//! backends.

use std::collections::BTreeMap;

use leep_core::memory::check_meta_store_contract;
use leep_core::{Changeset, ChangesetId, FileChange, FileKind, Hash, MetaStore, RepoPath};

use crate::codec::Codec;
use crate::compact::{run, CompactOptions, CompactReport, Rows, MIN_TRAINING_ROWS};
use crate::tests::pg_store;
use crate::*;

/// A chain of changesets shaped like imported commits: a few paths in a
/// handful of directories, messages with trailers, `git.*` extras.
pub(crate) fn history(n: usize, seed: u32) -> Vec<Changeset> {
    let mut out: Vec<Changeset> = Vec::with_capacity(n);
    for i in 0..n as u32 {
        let mut changes = BTreeMap::new();
        for f in 0..(i % 4 + 1) {
            let path = format!("drivers/net/dev{}/file{}.c", (i + seed) % 9, (i + f) % 13);
            let change = if (i + f) % 11 == 0 {
                FileChange::Delete
            } else {
                FileChange::Modify {
                    content: Hash::of_blob(&[i.to_le_bytes(), f.to_le_bytes()].concat()),
                    kind: FileKind::Regular,
                    size: u64::from(i % 5000) * 7 + 100,
                }
            };
            changes.insert(RepoPath::new(path).unwrap(), change);
        }
        let who = format!("Developer {} <dev{}@example.org>", i % 17, i % 17);
        out.push(Changeset {
            parents: out.last().map(|p| vec![p.id()]).unwrap_or_default(),
            author: who.clone(),
            timestamp: 1_600_000_000 + i64::from(i) * 600,
            message: format!(
                "net: dev{}: fix handling of case {}\n\nThe driver mishandled case {i}.\n\n\
                 Signed-off-by: {who}\nReviewed-by: Maintainer <maint@example.org>\n",
                (i + seed) % 9,
                i * 7 + seed
            ),
            changes,
            extra: BTreeMap::from([
                (
                    "git.commit".into(),
                    format!("{:040x}", u64::from(i) * 2654435761),
                ),
                (
                    "git.committer".into(),
                    format!("{who} {}", 1_600_000_000 + i),
                ),
            ]),
        });
    }
    out
}

async fn put_all(store: &dyn MetaStore, all: &[Changeset]) {
    for chunk in all.chunks(200) {
        store.put_changesets_with_derived(chunk, &[]).await.unwrap();
    }
}

async fn reads_back(store: &dyn MetaStore, all: &[Changeset]) {
    let ids: Vec<ChangesetId> = all.iter().map(Changeset::id).collect();
    for (chunk, want) in ids.chunks(300).zip(all.chunks(300)) {
        let got = store.get_changesets(chunk).await.unwrap();
        let got: Vec<Changeset> = got.into_iter().map(Option::unwrap).collect();
        assert_eq!(got, want);
    }
    let last = all.last().unwrap();
    assert_eq!(
        store.get_changeset(&last.id()).await.unwrap().as_ref(),
        Some(last)
    );
}

/// Every stored row's bytes, by id.
async fn raw(rows: &dyn Rows) -> BTreeMap<Vec<u8>, Vec<u8>> {
    rows.page(&[], usize::MAX >> 1)
        .await
        .unwrap()
        .into_iter()
        .collect()
}

async fn compact(rows: &dyn Rows, opts: CompactOptions) -> CompactReport {
    let mut calls = 0;
    let r = run(rows, &opts, &mut |_| calls += 1).await.unwrap();
    assert!(calls > 0 || r.rows == 0);
    r
}

/// `a`, `b` and `c` are store handles on one database, opened before any
/// dictionary existed, as servers running beside the compaction are.
async fn check_compaction<S: Rows + MetaStore>(a: &S, b: &S, c: &S) {
    let all = history(MIN_TRAINING_ROWS + 300, 0);
    put_all(a, &all).await;
    let ids: Vec<ChangesetId> = all.iter().map(Changeset::id).collect();
    let stored_at = a.changesets_stored_at(&ids).await.unwrap();
    assert!(stored_at.iter().all(Option::is_some));
    let plain = raw(a).await;
    assert!(plain.values().all(|d| !d.starts_with(&codec::ZSTD_MAGIC)));

    // A dry run trains and measures, and writes nothing.
    let dry = compact(
        a,
        CompactOptions {
            dry_run: true,
            ..Default::default()
        },
    )
    .await;
    assert!(dry.trained);
    assert_eq!(dry.dictionary, 1);
    assert_eq!(dry.samples, all.len() as u64);
    assert_eq!(dry.rows, all.len() as u64);
    assert_eq!(dry.rewritten, all.len() as u64);
    assert!(
        dry.bytes_after * 2 < dry.bytes_before,
        "{} -> {}",
        dry.bytes_before,
        dry.bytes_after
    );
    assert_eq!(a.max_dict().await.unwrap(), 0);
    assert_eq!(raw(a).await, plain);
    assert!(a.features().await.unwrap().is_empty());

    let r = compact(a, CompactOptions::default()).await;
    assert_eq!((r.dictionary, r.trained), (1, true));
    // Recorded, so servers that cannot read the rows refuse the store.
    assert_eq!(
        a.features().await.unwrap(),
        [crate::features::CHANGESET_ZSTD_DICT]
    );
    assert_eq!(r.rewritten, all.len() as u64);
    assert_eq!(r.bytes_before, dry.bytes_before);
    assert_eq!(r.bytes_after, dry.bytes_after);
    let packed = raw(a).await;
    assert!(packed.values().all(|d| Codec::uses(d, 1)));
    reads_back(a, &all).await;
    // Only `data` is rewritten: the store times stay (ADR 0059).
    assert_eq!(a.changesets_stored_at(&ids).await.unwrap(), stored_at);
    // The handle that predates the dictionary learns it on first read.
    assert!(!b.codec().knows(1));
    reads_back(b, &all).await;
    assert!(b.codec().knows(1));

    // Nothing left to do.
    let again = compact(a, CompactOptions::default()).await;
    assert_eq!((again.trained, again.rewritten), (false, 0));
    assert_eq!(again.bytes_after, r.bytes_after);

    // New writes are compressed by a handle that knows the dictionary...
    let more = history(5, 7);
    put_all(a, &more[..1]).await;
    let rows = raw(a).await;
    assert!(Codec::uses(&rows[more[0].id().0.as_bytes().as_slice()], 1));
    // ...and one that has not looked since stores plain rows (until its
    // next look, a minute after the last), which a later run compresses.
    assert!(!c.codec().knows(1) && !c.codec().due());
    c.put_changeset(&more[1]).await.unwrap();
    let rows = raw(a).await;
    assert_eq!(rows[more[1].id().0.as_bytes().as_slice()], more[1].encode());
    let r = compact(a, CompactOptions::default()).await;
    assert_eq!((r.trained, r.rewritten), (false, 1));
    assert!(raw(a).await.values().all(|d| Codec::uses(d, 1)));

    // A dry run of a retrain reads rows made with dictionary 1 and writes
    // nothing.
    let before = raw(a).await;
    let dry = compact(
        a,
        CompactOptions {
            retrain: true,
            dry_run: true,
            ..Default::default()
        },
    )
    .await;
    assert_eq!((dry.dictionary, dry.rewritten), (2, all.len() as u64 + 2));
    assert_eq!(raw(a).await, before);
    assert_eq!(a.max_dict().await.unwrap(), 1);

    // Retraining makes dictionary 2 and moves every row to it; the old one
    // stays, and readers learn the new one.
    let r = compact(
        a,
        CompactOptions {
            retrain: true,
            ..Default::default()
        },
    )
    .await;
    assert_eq!((r.dictionary, r.trained), (2, true));
    assert_eq!(r.rewritten, all.len() as u64 + 2);
    assert!(raw(a).await.values().all(|d| Codec::uses(d, 2)));
    reads_back(b, &all).await;
    reads_back(a, &more[..2]).await;
    assert_eq!(a.max_dict().await.unwrap(), 2);
}

/// With a dictionary in force a store still meets the contract, and the
/// rows it writes are compressed (or plain where that is smaller).
async fn check_contract_with_dictionary<S: Rows + MetaStore>(s: &S) {
    let samples: Vec<Vec<u8>> = history(MIN_TRAINING_ROWS, 5)
        .iter()
        .map(Changeset::encode)
        .collect();
    let dict = codec::train(&samples, 16 << 10, 1).unwrap();
    assert!(s.insert_dict(1, &dict).await.unwrap());
    assert!(
        !s.insert_dict(1, &dict).await.unwrap(),
        "ids are taken once"
    );
    s.load_dicts().await.unwrap();
    check_meta_store_contract(s).await;
    // The contract's changesets are tiny, mostly smaller plain; these are not.
    let real = history(3, 9);
    put_all(s, &real).await;
    reads_back(s, &real).await;
    let rows = raw(s).await;
    for c in &real {
        assert!(Codec::uses(&rows[c.id().0.as_bytes().as_slice()], 1));
    }
    for (id, data) in rows {
        let id = to_id(&id).unwrap();
        let cs = s.codec().decode_checked(&id, &data).unwrap();
        assert!(Codec::uses(&data, 1) || data == cs.encode());
    }
}

async fn check_small_store_is_left_alone<S: Rows + MetaStore>(s: &S) {
    put_all(s, &history(20, 3)).await;
    let before = raw(s).await;
    let r = compact(s, CompactOptions::default()).await;
    assert_eq!(r, CompactReport::default());
    assert_eq!(raw(s).await, before);
    assert_eq!(s.max_dict().await.unwrap(), 0);
    assert!(s.features().await.unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let url = snapshot::sqlite_file_url(&dir.path().join("m.db"), true);
    let a = SqliteMetaStore::connect(&url).await.unwrap();
    let b = SqliteMetaStore::connect(&url).await.unwrap();
    let c = SqliteMetaStore::connect(&url).await.unwrap();
    check_compaction(&a, &b, &c).await;
    let small = SqliteMetaStore::connect("sqlite::memory:").await.unwrap();
    check_small_store_is_left_alone(&small).await;
    let contract = SqliteMetaStore::connect("sqlite::memory:").await.unwrap();
    check_contract_with_dictionary(&contract).await;
    // And through the URL entry point.
    let r = compact::compact_changesets(
        &url,
        SchemaMode::RequireCurrent,
        &CompactOptions::default(),
        &mut |_| {},
    )
    .await
    .unwrap();
    assert_eq!((r.dictionary, r.rewritten), (2, 0));
    assert!(compact::compact_changesets(
        "mem://",
        SchemaMode::Migrate,
        &CompactOptions::default(),
        &mut |_| {}
    )
    .await
    .is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_compaction() {
    let Some((a, schema)) = pg_store("compact").await else {
        return;
    };
    let b = PostgresMetaStore::connect_with_schema(&schema.url, &schema.name)
        .await
        .unwrap();
    let c = PostgresMetaStore::connect_with_schema(&schema.url, &schema.name)
        .await
        .unwrap();
    check_compaction(&a, &b, &c).await;
    let Some((small, _small)) = pg_store("compactsmall").await else {
        return;
    };
    check_small_store_is_left_alone(&small).await;
    let (contract, _contract) = pg_store("compactcontract").await.unwrap();
    check_contract_with_dictionary(&contract).await;
}

/// A store opened on a schema without `changeset_dicts` (tests inject short
/// migration lists) works as before and refuses to compact.
#[tokio::test]
async fn older_schema_refuses_compaction() {
    let s = SqliteMetaStore::connect_with_migrations(
        "sqlite::memory:",
        SchemaMode::Migrate,
        &SQLITE_MIGRATIONS[..2],
    )
    .await
    .unwrap();
    put_all(&s, &history(MIN_TRAINING_ROWS, 1)).await;
    let err = run(&s, &CompactOptions::default(), &mut |_| {})
        .await
        .unwrap_err();
    assert!(err.to_string().contains("migrate"), "{err}");
    reads_back(&s, &history(MIN_TRAINING_ROWS, 1)).await;
}

// ---------------------------------------------------------------------------
// Automatic compaction (ADR 0101)

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::auto_compact::{tier_of, AutoCompactOptions, AutoCompactor, TIERS};

/// A store whose `rewrite` fails after `rewrites` successful calls, and
/// whose `insert_dict` fails while `fail_dict` is set: a compaction killed
/// inside a pass, or between recording the feature and storing the
/// dictionary.
struct Faulty<S> {
    inner: Arc<S>,
    rewrites: AtomicU64,
    fail_dict: std::sync::atomic::AtomicBool,
}

impl<S> Faulty<S> {
    fn new(inner: Arc<S>) -> Self {
        Faulty {
            inner,
            rewrites: AtomicU64::new(u64::MAX),
            fail_dict: Default::default(),
        }
    }
}

#[async_trait::async_trait]
impl<S: Rows + Send + Sync> Rows for Faulty<S> {
    fn codec(&self) -> &Codec {
        self.inner.codec()
    }
    fn has_dicts(&self) -> bool {
        self.inner.has_dicts()
    }
    async fn load_dicts(&self) -> leep_core::Result<()> {
        self.inner.load_dicts().await
    }
    async fn page(&self, after: &[u8], limit: usize) -> leep_core::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.inner.page(after, limit).await
    }
    async fn max_dict(&self) -> leep_core::Result<i64> {
        self.inner.max_dict().await
    }
    async fn insert_dict(&self, id: i64, data: &[u8]) -> leep_core::Result<bool> {
        if self.fail_dict.load(Ordering::SeqCst) {
            return Err(leep_core::Error::Storage(
                "killed before the dictionary".into(),
            ));
        }
        self.inner.insert_dict(id, data).await
    }
    async fn rewrite(&self, rows: &[(Vec<u8>, Vec<u8>)]) -> leep_core::Result<()> {
        let left = self.rewrites.load(Ordering::SeqCst);
        if left == 0 {
            return Err(leep_core::Error::Storage("killed inside the pass".into()));
        }
        self.rewrites
            .store(left.saturating_sub(1), Ordering::SeqCst);
        self.inner.rewrite(rows).await
    }
    async fn count_upto(&self, limit: u64) -> leep_core::Result<u64> {
        self.inner.count_upto(limit).await
    }
    async fn heads(
        &self,
        after: &[u8],
        limit: usize,
    ) -> leep_core::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.inner.heads(after, limit).await
    }
    async fn rows(&self, ids: &[Vec<u8>]) -> leep_core::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.inner.rows(ids).await
    }
    async fn dict_created_at(&self, id: i64) -> leep_core::Result<i64> {
        self.inner.dict_created_at(id).await
    }
    async fn try_lock(&self) -> leep_core::Result<bool> {
        self.inner.try_lock().await
    }
    async fn unlock(&self) -> leep_core::Result<()> {
        self.inner.unlock().await
    }
    async fn add_feature(&self, name: &str) -> leep_core::Result<()> {
        self.inner.add_feature(name).await
    }
    async fn features(&self) -> leep_core::Result<Vec<String>> {
        self.inner.features().await
    }
}

/// [`history`] with messages made of phrases from a set of about 45 KB:
/// more than a first-tier dictionary holds, less than a second-tier one,
/// so the second gains over the first.
fn varied_history(n: usize, seed: u64) -> Vec<Changeset> {
    let mut rng = seed | 1;
    let mut next = move || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng
    };
    let syll = [
        "ka", "lo", "mi", "ne", "ru", "sa", "to", "vi", "xe", "zu", "pre", "dor",
    ];
    let mut phrase = || -> String {
        let words: Vec<String> = (0..8)
            .map(|_| {
                (0..2 + next() % 2)
                    .map(|_| syll[(next() % syll.len() as u64) as usize])
                    .collect()
            })
            .collect();
        words.join(" ")
    };
    let phrases: Vec<String> = (0..900).map(|_| phrase()).collect();
    let mut out = history(n, seed as u32);
    let mut parent: Option<ChangesetId> = None;
    for cs in &mut out {
        let mut pick = || phrases[(next() % phrases.len() as u64) as usize].clone();
        let body: Vec<String> = (0..20).map(|_| pick()).collect();
        cs.message = format!("{}\n\n{}\n", pick(), body.join(".\n"));
        cs.parents = parent.into_iter().collect();
        parent = Some(cs.id());
    }
    out
}

fn auto_opts() -> AutoCompactOptions {
    AutoCompactOptions {
        batch: 64,
        pause: std::time::Duration::ZERO,
        idle: 0,
        settle: std::time::Duration::ZERO,
        ..Default::default()
    }
}

/// How many stored rows use dictionary `id`, and how many are plain.
async fn on_dict(rows: &dyn Rows, id: u32) -> (usize, usize) {
    let all = raw(rows).await;
    let on = all.values().filter(|d| Codec::uses(d, id)).count();
    let plain = all
        .values()
        .filter(|d| !d.starts_with(&codec::ZSTD_MAGIC))
        .count();
    (on, plain)
}

/// `a` writes as a server would; `own` is the compactor's own handle on the
/// same database, as the server opens one.
async fn check_auto_compaction<S: Rows + MetaStore + 'static>(a: &S, own: Arc<S>) {
    let all = varied_history(MIN_TRAINING_ROWS + 200, 4);
    let faulty = Arc::new(Faulty::new(own.clone()));
    let mut auto = AutoCompactor::new(faulty.clone(), auto_opts());

    // Below the first tier: nothing is trained, recorded or rewritten.
    put_all(a, &all[..50]).await;
    let r = auto.step().await.unwrap();
    assert_eq!((r.rows, r.dictionary, r.passed), (50, 0, false));
    assert_eq!(r.trained, None);
    assert!(a.features().await.unwrap().is_empty());
    assert_eq!(on_dict(a, 0).await, (0, 50));

    // Killed between the feature and the dictionary: the feature alone is
    // harmless, rows stay plain and readable, and the next step trains.
    put_all(a, &all[50..150]).await;
    faulty.fail_dict.store(true, Ordering::SeqCst);
    assert!(auto.step().await.is_err());
    assert_eq!(
        a.features().await.unwrap(),
        [crate::features::CHANGESET_ZSTD_DICT]
    );
    assert_eq!(a.max_dict().await.unwrap(), 0);
    reads_back(a, &all[..150]).await;
    faulty.fail_dict.store(false, Ordering::SeqCst);

    // Killed inside the pass after one batch: the rows rewritten and the
    // rest both read back, and the next step finishes.
    faulty.rewrites.store(1, Ordering::SeqCst);
    assert!(auto.step().await.is_err());
    let (on, plain) = on_dict(a, 1).await;
    assert!(on > 0 && plain > 0 && on + plain == 150, "{on} {plain}");
    assert!(tier_of(own.codec().newest_len()) == Some(0));
    reads_back(a, &all[..150]).await;
    faulty.rewrites.store(u64::MAX, Ordering::SeqCst);
    let r = auto.step().await.unwrap();
    assert_eq!(r.trained, None, "the stored dictionary is reused");
    assert!(r.passed && r.rewritten as usize == plain, "{r:?}");
    assert_eq!(on_dict(a, 1).await, (150, 0));
    assert!(r.bytes_after * 2 < r.bytes_before, "{r:?}");
    // The handle the server writes with learns the dictionary on read.
    reads_back(a, &all[..150]).await;

    // Covered for good (the dictionary is old enough here): later steps do
    // not pass again.
    let r = auto.step().await.unwrap();
    assert!(!r.passed, "{r:?}");

    // A restarted compactor passes once, and compresses what a server that
    // had not looked wrote plain.
    a.load_dicts().await.unwrap();
    put_all(a, &all[150..200]).await;
    let mut auto = AutoCompactor::new(faulty.clone(), auto_opts());
    let r = auto.step().await.unwrap();
    assert!(r.passed, "{r:?}");
    assert_eq!(on_dict(a, 1).await, (200, 0));

    // At 1,000 changesets the second tier's dictionary replaces it, and
    // every row moves to it.
    put_all(a, &all[200..]).await;
    let r = auto.step().await.unwrap();
    let (id, bytes, samples) = r
        .trained
        .unwrap_or_else(|| panic!("a second-tier dictionary: {r:?}"));
    assert_eq!(id, 2);
    assert!(bytes as usize > TIERS[0].dict_bytes && bytes as usize <= TIERS[1].dict_bytes);
    assert_eq!(samples, all.len() as u64);
    assert_eq!(on_dict(a, 2).await, (all.len(), 0));
    reads_back(a, &all).await;
    // Nothing more until the next tier.
    let r = auto.step().await.unwrap();
    assert_eq!((r.trained, r.rejected, r.passed), (None, false, false));
    assert_eq!(a.max_dict().await.unwrap(), 2);
}

/// A row that does not decode is left as it is and counted; the other rows
/// are compressed, and the next steps go on.
async fn check_corrupt_row_is_skipped<S: Rows + MetaStore + 'static>(a: &S, own: Arc<S>) {
    let all = varied_history(150, 9);
    put_all(a, &all).await;
    let bad = all[7].id().0.as_bytes().to_vec();
    a.rewrite(&[(bad.clone(), b"not a changeset".to_vec())])
        .await
        .unwrap();
    let mut auto = AutoCompactor::new(own.clone(), auto_opts());
    let r = auto.step().await.unwrap();
    assert!(r.trained.is_some(), "{r:?}");
    assert_eq!((r.corrupt, r.rewritten), (1, 149), "{r:?}");
    let rows = raw(a).await;
    assert_eq!(rows[&bad], b"not a changeset");
    assert_eq!(on_dict(a, 1).await, (149, 1));
    // A restarted compactor's pass skips it again rather than failing.
    let mut auto = AutoCompactor::new(own.clone(), auto_opts());
    let r = auto.step().await.unwrap();
    assert!(r.passed, "{r:?}");
    assert_eq!((r.corrupt, r.rewritten), (1, 0), "{r:?}");
}

/// A dictionary an operator trained (64 KiB by default) is in the last
/// tier: the compactor never replaces it, and only compresses new rows.
async fn check_operator_dictionary_is_kept<S: Rows + MetaStore + 'static>(a: &S, own: Arc<S>) {
    let all = history(MIN_TRAINING_ROWS + 10, 6);
    put_all(a, &all).await;
    let r = compact(a, CompactOptions::default()).await;
    assert_eq!((r.dictionary, r.trained), (1, true));
    // Written by a handle that had not looked yet: plain.
    let more = history(30, 8);
    own.put_changeset(&more[0]).await.unwrap();
    let mut auto = AutoCompactor::new(own.clone(), auto_opts());
    let r = auto.step().await.unwrap();
    assert_eq!(tier_of(own.codec().newest_len()), Some(TIERS.len() - 1));
    assert_eq!((r.trained, r.dictionary, r.rewritten), (None, 1, 1));
    assert_eq!(a.max_dict().await.unwrap(), 1);
    assert_eq!(on_dict(a, 1).await, (all.len() + 1, 0));
}

/// A dictionary that would cost more than it saves (a few small rows) is
/// not stored, and nothing is recorded.
async fn check_useless_dictionary_is_not_stored<S: Rows + MetaStore + 'static>(a: &S, own: Arc<S>) {
    // Small changesets of random-looking text: nothing repeats between rows,
    // and all of them together are smaller than a dictionary.
    let mut rng = 0x9E37_79B9_7F4A_7C15u64;
    let mut next = || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng
    };
    let all: Vec<Changeset> = (0..150)
        .map(|_| Changeset {
            parents: vec![],
            author: format!("{:016x}{:016x}", next(), next()),
            timestamp: next() as i64 >> 2,
            message: (0..2).map(|_| format!("{:016x}", next())).collect(),
            changes: BTreeMap::new(),
            extra: BTreeMap::new(),
        })
        .collect();
    put_all(a, &all).await;
    let mut auto = AutoCompactor::new(own.clone(), auto_opts());
    let r = auto.step().await.unwrap();
    assert!(
        r.rejected && r.trained.is_none() && r.dictionary == 0,
        "{r:?}"
    );
    assert_eq!(a.max_dict().await.unwrap(), 0);
    assert!(a.features().await.unwrap().is_empty());
    // Not tried again until the store has doubled.
    let r = auto.step().await.unwrap();
    assert!(!r.rejected, "{r:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_auto_compaction() {
    for (name, check) in [
        ("tiers", 0),
        ("operator", 1),
        ("useless", 2),
        ("corrupt", 3),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let url = snapshot::sqlite_file_url(&dir.path().join(format!("{name}.db")), true);
        let a = SqliteMetaStore::connect(&url).await.unwrap();
        let own = Arc::new(SqliteMetaStore::connect(&url).await.unwrap());
        match check {
            0 => check_auto_compaction(&a, own).await,
            1 => check_operator_dictionary_is_kept(&a, own).await,
            2 => check_useless_dictionary_is_not_stored(&a, own).await,
            _ => check_corrupt_row_is_skipped(&a, own).await,
        }
    }
    // A store a second connection cannot share is left alone.
    let none = AutoCompactor::connect("sqlite::memory:", auto_opts())
        .await
        .unwrap();
    assert!(none.is_none());
    assert!(AutoCompactor::connect("mem://", auto_opts())
        .await
        .unwrap()
        .is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_auto_compaction() {
    for (name, check) in [
        ("autotiers", 0),
        ("autooperator", 1),
        ("autouseless", 2),
        ("autocorrupt", 3),
    ] {
        let Some((a, schema)) = pg_store(name).await else {
            return;
        };
        let own = Arc::new(
            PostgresMetaStore::connect_with_schema(&schema.url, &schema.name)
                .await
                .unwrap(),
        );
        match check {
            0 => check_auto_compaction(&a, own).await,
            1 => check_operator_dictionary_is_kept(&a, own).await,
            2 => check_useless_dictionary_is_not_stored(&a, own).await,
            _ => check_corrupt_row_is_skipped(&a, own).await,
        }
    }
    // One step at a time per store: while another holds the lock, a step
    // does nothing.
    let Some((a, schema)) = pg_store("autolock").await else {
        return;
    };
    put_all(&a, &varied_history(150, 3)).await;
    let own = Arc::new(
        PostgresMetaStore::connect_with_schema(&schema.url, &schema.name)
            .await
            .unwrap(),
    );
    let holder = Arc::new(
        PostgresMetaStore::connect_with_schema(&schema.url, &schema.name)
            .await
            .unwrap(),
    );
    assert!(holder.try_lock().await.unwrap());
    let mut auto = AutoCompactor::new(own.clone(), auto_opts());
    let r = auto.step().await.unwrap();
    assert!(r.busy && r.trained.is_none() && !r.passed, "{r:?}");
    assert_eq!(a.max_dict().await.unwrap(), 0);
    holder.unlock().await.unwrap();
    let r = auto.step().await.unwrap();
    assert!(!r.busy && r.trained.is_some(), "{r:?}");
}
