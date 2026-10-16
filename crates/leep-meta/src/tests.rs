use std::collections::BTreeMap;
use std::sync::Arc;

use leep_core::memory::check_meta_store_contract;
use leep_core::{Changeset, MetaStore, RefUpdateContext};

use super::*;

pub(crate) fn cs(n: i64) -> Changeset {
    Changeset {
        parents: vec![],
        author: "t".into(),
        timestamp: n,
        message: format!("m{n}"),
        changes: BTreeMap::new(),
        extra: BTreeMap::new(),
    }
}

/// 32 tasks race a CAS from the same expected value; exactly one wins.
async fn check_cas_race(store: Arc<dyn MetaStore>) {
    let base = store.put_changeset(&cs(0)).await.unwrap();
    assert!(store.cas_ref("main", None, Some(base)).await.unwrap());
    let mut tasks = Vec::new();
    for i in 1..=32 {
        let store = store.clone();
        tasks.push(tokio::spawn(async move {
            let id = store.put_changeset(&cs(i)).await.unwrap();
            store.cas_ref("main", Some(base), Some(id)).await.unwrap()
        }));
    }
    let mut wins = 0;
    for t in tasks {
        wins += t.await.unwrap() as usize;
    }
    assert_eq!(wins, 1);
    // Same race for creation of an absent ref.
    let mut tasks = Vec::new();
    for i in 1..=32 {
        let store = store.clone();
        tasks.push(tokio::spawn(async move {
            let id = store.put_changeset(&cs(i)).await.unwrap();
            store.cas_ref("new", None, Some(id)).await.unwrap()
        }));
    }
    let mut wins = 0;
    for t in tasks {
        wins += t.await.unwrap() as usize;
    }
    assert_eq!(wins, 1);
}

/// 32 tasks race read-then-CAS on one ref until each has won twice: every
/// win has exactly one log entry, losses have none, and the log replays to
/// the final value.
async fn check_ref_log_race(store: Arc<dyn MetaStore>) {
    let ctx = RefUpdateContext::new("race").actor(Some("racer"));
    let base = store.put_changeset(&cs(500)).await.unwrap();
    assert!(store
        .cas_ref_logged("race", None, Some(base), &ctx)
        .await
        .unwrap());
    let mut tasks = Vec::new();
    for i in 0..32i64 {
        let store = store.clone();
        let ctx = ctx.clone();
        tasks.push(tokio::spawn(async move {
            let mut wins = 0;
            let mut losses = 0;
            while wins < 2 {
                let cur = store.get_ref("race").await.unwrap();
                // Ids must not repeat across tasks: a CAS to the value the
                // ref already holds succeeds without a log entry.
                let id = store
                    .put_changeset(&cs(1000 + i * 1_000_000 + wins + losses))
                    .await
                    .unwrap();
                if store
                    .cas_ref_logged("race", cur, Some(id), &ctx)
                    .await
                    .unwrap()
                {
                    wins += 1;
                } else {
                    losses += 1;
                }
            }
            (wins, losses)
        }));
    }
    let mut wins = 1;
    for t in tasks {
        wins += t.await.unwrap().0;
    }
    let entries = store.ref_log(Some("race"), None, 10_000).await.unwrap();
    assert_eq!(entries.len() as i64, wins, "one entry per successful CAS");
    let mut prev = None;
    for e in entries.iter().rev() {
        assert_eq!(e.old, prev, "entry {} continues the previous one", e.seq);
        prev = e.new;
    }
    assert_eq!(
        prev,
        store.get_ref("race").await.unwrap(),
        "replays to head"
    );
    assert!(entries.windows(2).all(|w| w[0].seq > w[1].seq));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_memory() {
    let s = SqliteMetaStore::connect("sqlite::memory:").await.unwrap();
    check_meta_store_contract(&s).await;
    check_cas_race(Arc::new(s)).await;
    let s = SqliteMetaStore::connect("sqlite::memory:").await.unwrap();
    check_ref_log_race(Arc::new(s)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn memory_ref_log_race() {
    check_ref_log_race(open_meta_store("mem://").await.unwrap()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_file_persists_and_races() {
    let dir = std::env::temp_dir().join(format!("leep-meta-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let url = format!("sqlite://{}?mode=rwc", dir.join("meta.db").display());
    let s = SqliteMetaStore::connect(&url).await.unwrap();
    check_meta_store_contract(&s).await;
    check_cas_race(Arc::new(SqliteMetaStore::connect(&url).await.unwrap())).await;
    let id = s.put_changeset(&cs(99)).await.unwrap();
    drop(s);
    // Reopen: idempotent migration, data survives.
    let s = SqliteMetaStore::connect(&url).await.unwrap();
    assert_eq!(s.get_changeset(&id).await.unwrap(), Some(cs(99)));
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn sqlite_detects_corruption() {
    let s = SqliteMetaStore::connect("sqlite::memory:").await.unwrap();
    let id = s.put_changeset(&cs(1)).await.unwrap();
    let other = cs(2).encode();
    sqlx::query("UPDATE changesets SET data = ?")
        .bind(other)
        .execute(&s.pool)
        .await
        .unwrap();
    assert!(matches!(
        s.get_changeset(&id).await,
        Err(leep_core::Error::Storage(_))
    ));
}

/// More rows than one delete batch, so dropping a kind takes several statements
/// and still removes exactly that kind's rows.
async fn check_delete_kind_spans_batches(store: &dyn MetaStore) {
    let n = crate::DELETE_BATCH as usize * 2 + 37;
    for i in 0..n {
        let key = leep_core::Hash::of_blob(&(i as u64).to_be_bytes());
        store
            .put_derived("batch-a", &key, bytes::Bytes::from_static(b"x"))
            .await
            .unwrap();
        if i < 5 {
            store
                .put_derived("batch-b", &key, bytes::Bytes::from_static(b"y"))
                .await
                .unwrap();
        }
    }
    assert_eq!(
        store.delete_derived_kind("batch-a").await.unwrap(),
        n as u64
    );
    assert!(store
        .list_derived("batch-a", None, 10)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        store
            .list_derived("batch-b", None, 100)
            .await
            .unwrap()
            .len(),
        5
    );
    assert_eq!(store.delete_derived_kind("batch-b").await.unwrap(), 5);
}

#[tokio::test]
async fn sqlite_delete_kind_spans_batches() {
    let s = SqliteMetaStore::connect("sqlite::memory:").await.unwrap();
    check_delete_kind_spans_batches(&s).await;
}

#[tokio::test]
async fn postgres_delete_kind_spans_batches() {
    let Some((s, _)) = pg_store("batches").await else {
        return;
    };
    check_delete_kind_spans_batches(&s).await;
}

#[tokio::test]
async fn open_dispatch() {
    let m = open_meta_store("mem://").await.unwrap();
    check_meta_store_contract(&*m).await;
    let m = open_meta_store("sqlite::memory:").await.unwrap();
    check_meta_store_contract(&*m).await;
    assert!(open_meta_store("bogus://x").await.is_err());
}

pub(crate) fn pg_url() -> Option<String> {
    let url = std::env::var("LEEP_TEST_POSTGRES_URL").ok();
    if url.is_none() {
        eprintln!("skipping: LEEP_TEST_POSTGRES_URL not set");
    }
    url
}

pub(crate) fn schema(name: &str) -> String {
    format!("leep_test_{name}_{}", std::process::id())
}

async fn pg_store(name: &str) -> Option<(PostgresMetaStore, String)> {
    let url = pg_url()?;
    let s = PostgresMetaStore::connect_with_schema(&url, &schema(name))
        .await
        .unwrap();
    Some((s, url))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_contract() {
    let Some((s, url)) = pg_store("contract").await else {
        return;
    };
    check_meta_store_contract(&s).await;
    // Re-running migrations is a no-op.
    PostgresMetaStore::connect_with_schema(&url, &schema("contract"))
        .await
        .unwrap();
    let m = open_meta_store(&url).await.unwrap();
    m.list_refs().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_cas_race() {
    let Some((s, _)) = pg_store("race").await else {
        return;
    };
    check_cas_race(Arc::new(s)).await;
    let Some((s, _)) = pg_store("lograce").await else {
        return;
    };
    check_ref_log_race(Arc::new(s)).await;
}

/// Tasks start ref moves and abandon them after a seeded number of
/// suspensions (a request timeout, a dropped connection), over a few refs
/// including deletes. However far each got, a ref and its log agree: every
/// move has exactly one entry, no entry lacks its move, and the log replays
/// to the refs.
async fn check_cancelled_cas_keeps_ref_and_log_together(store: Arc<dyn MetaStore>) {
    use leep_testkit::{cancel_after_polls, Rng};

    let mut ids = Vec::new();
    for n in 0..40 {
        ids.push(store.put_changeset(&cs(7000 + n)).await.unwrap());
    }
    let refs = ["cx/a", "cx/b", "cx/c"];
    let mut tasks = Vec::new();
    for t in 0..12u64 {
        let (store, ids) = (store.clone(), ids.clone());
        tasks.push(tokio::spawn(async move {
            let mut rng = Rng::new(t + 1);
            let ctx = RefUpdateContext::new("cancel");
            for _ in 0..30 {
                let name = refs[rng.below(refs.len())];
                let cur = store.get_ref(name).await.unwrap();
                let new = (rng.below(8) != 0).then(|| ids[rng.below(ids.len())]);
                let call = store.cas_ref_logged(name, cur, new, &ctx);
                // 0 cancels before the first await; large lets it finish.
                let _ = cancel_after_polls(call, rng.below(14)).await;
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    let mut log = store.ref_log(None, None, usize::MAX >> 1).await.unwrap();
    assert!(!log.is_empty(), "some moves completed");
    log.reverse();
    let mut replay: BTreeMap<String, leep_core::ChangesetId> = BTreeMap::new();
    for e in &log {
        assert_eq!(
            replay.get(&e.name).copied(),
            e.old,
            "entry {} of {}",
            e.seq,
            e.name
        );
        match e.new {
            Some(n) => replay.insert(e.name.clone(), n),
            None => replay.remove(&e.name),
        };
    }
    let heads = store.list_refs().await.unwrap();
    let ours = |m: &BTreeMap<String, leep_core::ChangesetId>| {
        m.iter()
            .filter(|(k, _)| k.starts_with("cx/"))
            .map(|(k, v)| (k.clone(), *v))
            .collect::<BTreeMap<_, _>>()
    };
    assert_eq!(ours(&replay), ours(&heads), "log replays to the refs");
    assert!(
        log.windows(2).all(|w| w[0].seq < w[1].seq),
        "seq increases with every entry"
    );
    // The store still works after all the abandoned requests.
    let cur = store.get_ref("cx/a").await.unwrap();
    assert!(
        store
            .cas_ref("cx/a", cur, Some(ids[0]))
            .await
            .unwrap_or(false)
            || cur == Some(ids[0])
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_ref_moves_never_split_ref_and_log() {
    check_cancelled_cas_keeps_ref_and_log_together(open_meta_store("mem://").await.unwrap()).await;
    // Not `sqlite::memory:`: sqlx discards a connection cancelled mid-query,
    // and a database that lives in that one connection goes with it.
    let dir = std::env::temp_dir().join(format!("leep-meta-cancel-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let url = format!("sqlite://{}?mode=rwc", dir.join("meta.db").display());
    let s = SqliteMetaStore::connect(&url).await.unwrap();
    check_cancelled_cas_keeps_ref_and_log_together(Arc::new(s)).await;
    std::fs::remove_dir_all(&dir).ok();
    if let Some((s, _)) = pg_store("cancel").await {
        check_cancelled_cas_keeps_ref_and_log_together(Arc::new(s)).await;
    }
}

#[tokio::test]
async fn quiet_fault_wrapper_passes_contract() {
    use leep_testkit::{FaultPlan, FaultyMetaStore};
    for url in ["mem://", "sqlite::memory:"] {
        let store = FaultyMetaStore::new(open_meta_store(url).await.unwrap(), FaultPlan::new(1));
        check_meta_store_contract(&store).await;
    }
}
