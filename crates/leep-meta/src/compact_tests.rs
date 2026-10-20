//! Compressed changeset rows and `compact-changesets` (ADR 0053) on both
//! backends.

use std::collections::BTreeMap;

use leep_core::memory::check_meta_store_contract;
use leep_core::{Changeset, ChangesetId, FileChange, FileKind, Hash, MetaStore, RepoPath};

use crate::codec::Codec;
use crate::compact::{run, CompactOptions, CompactReport, Rows, MIN_TRAINING_ROWS};
use crate::tests::{pg_store, schema};
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
    let Some((a, url)) = pg_store("compact").await else {
        return;
    };
    let b = PostgresMetaStore::connect_with_schema(&url, &schema("compact"))
        .await
        .unwrap();
    let c = PostgresMetaStore::connect_with_schema(&url, &schema("compact"))
        .await
        .unwrap();
    check_compaction(&a, &b, &c).await;
    let Some((small, _)) = pg_store("compactsmall").await else {
        return;
    };
    check_small_store_is_left_alone(&small).await;
    let (contract, _) = pg_store("compactcontract").await.unwrap();
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
