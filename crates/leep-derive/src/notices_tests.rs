//! Overlap notices and duplicate flags (ADR 0096) on the derive layer, with
//! in-memory stores: each level, ranking, the cap, saying each once,
//! duplicates by key and by similarity, and a slow store.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use leep_core::api::{DeclareSessionRequest, LandResponse, Notice};
use leep_core::changeset::EXTRA_AGENT_SESSION;
use leep_core::coord::{
    Beat, CoordStore, NearQuery, NearRow, NoticeLevel, Session, SessionEnd, SessionFilter,
    SessionState, SessionUpdate, SweepReport,
};
use leep_core::memory::{MemoryBlobStore, MemoryCoordStore, MemoryMetaStore};
use leep_core::{Changeset, ChangesetId, FileChange, FileKind, RepoPath, Result};

use crate::notices::{NoticeAsk, HOT_SESSIONS, MAX_NOTICES, NOTICE_BOUND};
use crate::sessions::SessionSeed;
use crate::Repo;

fn repo() -> Repo {
    Repo::new(
        Arc::new(MemoryBlobStore::new()),
        Arc::new(MemoryMetaStore::new()),
    )
}

fn declare(key: &str, title: &str) -> DeclareSessionRequest {
    DeclareSessionRequest {
        session: key.into(),
        title: Some(title.into()),
        target_ref: Some("main".into()),
        ..Default::default()
    }
}

fn levels(n: &[Notice]) -> Vec<&str> {
    n.iter().map(|n| n.level.as_str()).collect()
}

/// Create a changeset of `paths` by `who` under `key`, as the server does,
/// and answer its id with the notices the create carries.
async fn commit(
    r: &Repo,
    who: &str,
    key: &str,
    parents: &[ChangesetId],
    paths: &[&str],
) -> (ChangesetId, Vec<Notice>) {
    let mut changes = BTreeMap::new();
    for p in paths {
        let body = format!("{who} {p}\n");
        changes.insert(
            RepoPath::new(*p).unwrap(),
            FileChange::Modify {
                content: r.put_blob(Bytes::from(body.clone())).await.unwrap(),
                kind: FileKind::Regular,
                size: body.len() as u64,
            },
        );
    }
    let cs = Changeset {
        parents: parents.to_vec(),
        author: who.into(),
        timestamp: 1_700_000_000,
        message: format!("{who}: edit {}", paths.join(" ")),
        changes,
        extra: [(EXTRA_AGENT_SESSION.to_string(), key.to_string())].into(),
    };
    let seed = SessionSeed::of(&cs).unwrap();
    let focus = seed.touch.clone();
    let id = r.create_changeset_by(cs, Some(who)).await.unwrap();
    let s = r
        .note_created_session(seed, id, who)
        .await
        .unwrap()
        .unwrap();
    let ask = NoticeAsk {
        focus,
        ..Default::default()
    };
    (id, r.notices_for_session(&s, &ask).await)
}

#[tokio::test]
async fn two_agents_on_one_file_hear_of_each_other_once() {
    let r = repo();
    let (_, first) = commit(&r, "ada", "a-1", &[], &["src/auth/token.rs"]).await;
    assert!(first.is_empty(), "{first:?}");
    let (_, second) = commit(&r, "bo", "b-1", &[], &["src/auth/token.rs"]).await;
    assert_eq!(levels(&second), ["same_file"]);
    let n = &second[0];
    assert_eq!(n.principal.as_deref(), Some("ada"));
    assert_eq!(n.untrusted.path.as_deref(), Some("src/auth/token.rs"));
    assert_eq!(n.untrusted.title, "ada: edit src/auth/token.rs");
    // ada hears of bo on its next answer, and neither hears it twice.
    let a = r.notices_for("ada", "a-1", &NoticeAsk::default()).await;
    assert_eq!(levels(&a), ["same_file"]);
    assert_eq!(a[0].principal.as_deref(), Some("bo"));
    assert!(r
        .notices_for("ada", "a-1", &NoticeAsk::default())
        .await
        .is_empty());
    let (_, again) = commit(&r, "bo", "b-1", &[], &["src/auth/token.rs"]).await;
    assert!(again.is_empty(), "said once: {again:?}");
    // Every overlap is still listed when asked for.
    let id = r
        .sessions(&crate::sessions::SessionQuery {
            principal: Some("bo".into()),
            ..Default::default()
        })
        .await
        .unwrap()
        .sessions[0]
        .id;
    let all = r.session_overlaps(id).await.unwrap();
    assert_eq!(levels(&all.overlaps), ["same_file"]);
}

#[tokio::test]
async fn near_is_said_only_when_asked_for() {
    let r = repo();
    commit(&r, "ada", "a-1", &[], &["src/auth/a.rs"]).await;
    let (_, n) = commit(&r, "bo", "b-1", &[], &["src/auth/b.rs"]).await;
    assert!(n.is_empty(), "same_file is the default minimum: {n:?}");
    let near = NoticeAsk {
        min: NoticeLevel::Near,
        ..Default::default()
    };
    let n = r.notices_for("bo", "b-1", &near).await;
    assert_eq!(levels(&n), ["near"]);
    assert_eq!(n[0].untrusted.other_path.as_deref(), Some("src/auth/a.rs"));
    // A different directory is not near.
    commit(&r, "cy", "c-1", &[], &["src/billing/c.rs"]).await;
    assert!(r.notices_for("cy", "c-1", &near).await.is_empty());
}

#[tokio::test]
async fn a_claim_is_louder_and_ranks_first() {
    let r = repo();
    let mut a = declare("a-1", "own the token code");
    a.claims = Some(vec!["src/auth".into()]);
    r.declare_session("ada", a).await.unwrap();
    commit(&r, "cy", "c-1", &[], &["src/auth/token.rs"]).await;
    let mut b = declare("b-1", "parse tokens");
    b.scope = Some(vec!["src/auth/token.rs".into()]);
    let resp = r.declare_session("bo", b).await.unwrap();
    assert_eq!(levels(&resp.notices), ["claimed", "same_file"]);
    assert_eq!(resp.notices[0].principal.as_deref(), Some("ada"));
    assert_eq!(
        resp.notices[0].untrusted.other_path.as_deref(),
        Some("src/auth")
    );
}

/// Land `n` changesets by `who` on `main`, each changing `path`; answers
/// the new head.
async fn land_many(r: &Repo, who: &str, n: usize, path: &str) -> ChangesetId {
    let mut head = r.meta().get_ref("main").await.unwrap();
    for i in 0..n {
        let (id, _) = commit(
            r,
            who,
            &format!("{who}-{i}"),
            &head.into_iter().collect::<Vec<_>>(),
            &[path],
        )
        .await;
        let LandResponse::Landed { landed, .. } = r.land_as(id, "main", Some(who)).await.unwrap()
        else {
            panic!()
        };
        head = Some(landed);
    }
    head.unwrap()
}

/// A session based far behind a busy ref: the `landed` walk reads in
/// batches, answers again from its cache, and costs nothing once said.
#[tokio::test]
async fn a_session_far_behind_reads_its_lands_in_batches() {
    let r = repo();
    let base = land_many(&r, "root", 1, "README").await;
    let mut a = declare("a-1", "tokens");
    a.base = Some(base);
    a.scope = Some(vec!["src/auth".into()]);
    r.declare_session("ada", a).await.unwrap();
    land_many(&r, "bo", 60, "docs/x.md").await;
    land_many(&r, "cy", 1, "src/auth/token.rs").await;
    let id = mine_id(&r, "ada").await;
    let (all, reads) = crate::read_stats::measure(r.session_overlaps(id)).await;
    let landed: Vec<_> = all
        .unwrap()
        .overlaps
        .into_iter()
        .filter(|n| n.level == "landed")
        .collect();
    assert_eq!(landed.len(), 1, "{landed:?}");
    assert!(reads.rounds <= 20, "{reads:?}");
    let (_, again) = crate::read_stats::measure(r.session_overlaps(id)).await;
    assert!(again.rounds <= 3, "cached: {again:?}");
}

/// Another principal's land under my session key is not mine: only work
/// with my key and by me is skipped.
#[tokio::test]
async fn a_land_under_my_key_by_someone_else_is_named() {
    let r = repo();
    let base = land_many(&r, "root", 1, "README").await;
    let mut a = declare("a-1", "tokens");
    a.base = Some(base);
    a.scope = Some(vec!["src/auth".into()]);
    r.declare_session("ada", a).await.unwrap();
    let (theirs, _) = commit(&r, "bo", "a-1", &[base], &["src/auth/token.rs"]).await;
    r.land_as(theirs, "main", Some("bo")).await.unwrap();
    let n = r.notices_for("ada", "a-1", &NoticeAsk::default()).await;
    assert_eq!(n[0].level, "landed", "{n:?}");
    assert_eq!(n[0].changeset, r.meta().get_ref("main").await.unwrap());
}

/// A session whose head was collected still hears of its path overlaps:
/// the `landed` part failing costs only that part.
#[tokio::test]
async fn a_missing_head_costs_only_the_landed_notices() {
    let r = repo();
    let base = land_many(&r, "root", 1, "README").await;
    let mut a = declare("a-1", "tokens");
    a.base = Some(base);
    r.declare_session("ada", a).await.unwrap();
    let (head, _) = commit(&r, "ada", "a-1", &[base], &["x/a.rs"]).await;
    land_many(&r, "cy", 1, "docs/y.md").await;
    r.meta().delete_changesets(&[head], &[]).await.unwrap();
    commit(&r, "bo", "b-1", &[], &["x/a.rs"]).await;
    let n = r.notices_for("ada", "a-1", &NoticeAsk::default()).await;
    assert_eq!(levels(&n), ["same_file"], "{n:?}");
    let id = mine_id(&r, "ada").await;
    let all = r.session_overlaps(id).await.unwrap();
    assert_eq!(levels(&all.overlaps), ["same_file"]);
}

#[tokio::test]
async fn a_land_touching_my_files_is_named() {
    let r = repo();
    let (seed, _) = commit(&r, "root", "r-0", &[], &["README"]).await;
    let LandResponse::Landed { landed: base, .. } =
        r.land_as(seed, "main", Some("root")).await.unwrap()
    else {
        panic!()
    };
    let mut a = declare("a-1", "tokens");
    a.base = Some(base);
    a.scope = Some(vec!["src/auth".into()]);
    r.declare_session("ada", a).await.unwrap();
    let (theirs, _) = commit(&r, "bo", "b-1", &[base], &["src/auth/token.rs"]).await;
    let LandResponse::Landed { landed, .. } = r.land_as(theirs, "main", Some("bo")).await.unwrap()
    else {
        panic!()
    };
    // bo's session ends as landed; ada hears that it landed, naming it.
    r.note_landed(theirs, landed, Some("bo")).await.unwrap();
    let n = r.notices_for("ada", "a-1", &NoticeAsk::default()).await;
    assert_eq!(levels(&n), ["landed"]);
    assert_eq!(n[0].changeset, Some(landed));
    assert_eq!(
        n[0].untrusted.other_path.as_deref(),
        Some("src/auth/token.rs")
    );
    // A land elsewhere says nothing; once ada builds on the new head, the
    // old land is behind its base.
    let (docs, _) = commit(&r, "cy", "c-1", &[landed], &["docs/x.md"]).await;
    r.land_as(docs, "main", Some("cy")).await.unwrap();
    let head = r.meta().get_ref("main").await.unwrap().unwrap();
    let (_, mine) = commit(&r, "ada", "a-1", &[head], &["src/auth/mod.rs"]).await;
    assert!(mine.iter().all(|n| n.level != "landed"), "{mine:?}");
    let id = mine_id(&r, "ada").await;
    let all = r.session_overlaps(id).await.unwrap();
    assert!(all.overlaps.iter().all(|n| n.level != "landed"), "{all:?}");
}

async fn mine_id(r: &Repo, who: &str) -> u64 {
    r.sessions(&crate::sessions::SessionQuery {
        principal: Some(who.into()),
        ..Default::default()
    })
    .await
    .unwrap()
    .sessions[0]
        .id
}

#[tokio::test]
async fn at_most_three_ranked_and_the_rest_later() {
    let r = repo();
    for (i, who) in ["p1", "p2", "p3", "p4", "p5"].iter().enumerate() {
        let mut d = declare(&format!("k{i}"), "work");
        if i == 0 {
            d.claims = Some(vec!["lib/x.rs".into()]);
        } else {
            d.scope = Some(vec!["lib/x.rs".into()]);
        }
        r.declare_session(who, d).await.unwrap();
    }
    let (_, n) = commit(&r, "me", "m-1", &[], &["lib/x.rs"]).await;
    assert_eq!(n.len(), MAX_NOTICES);
    assert_eq!(levels(&n), ["claimed", "same_file", "same_file"]);
    let rest = r.notices_for("me", "m-1", &NoticeAsk::default()).await;
    assert_eq!(levels(&rest), ["same_file"; 2]);
    let said: std::collections::BTreeSet<_> = n.iter().chain(&rest).map(|n| n.session).collect();
    assert_eq!(said.len(), 5, "each session once");
}

#[tokio::test]
async fn a_crowded_path_is_one_count() {
    let r = repo();
    for i in 0..=HOT_SESSIONS {
        let mut d = declare(&format!("k{i}"), "hot");
        d.scope = Some(vec!["Cargo.lock".into()]);
        r.declare_session(&format!("p{i}"), d).await.unwrap();
    }
    let (_, n) = commit(&r, "me", "m-1", &[], &["Cargo.lock"]).await;
    assert_eq!(n.len(), 1, "{n:?}");
    assert_eq!(n[0].count, Some(HOT_SESSIONS as u64 + 1));
    assert!(n[0].session.is_none());
}

/// A crowded directory of theirs is one notice however many of my files
/// it holds, and a crowd member that also overlaps me elsewhere, on an
/// uncrowded path, is still named there.
#[tokio::test]
async fn a_crowded_directory_is_one_count_for_all_my_files() {
    let r = repo();
    for i in 0..=HOT_SESSIONS {
        let mut d = declare(&format!("k{i}"), "crates work");
        let mut scope = vec!["crates".to_string()];
        if i == 0 {
            scope.push("docs/guide.md".into());
        }
        d.scope = Some(scope);
        r.declare_session(&format!("p{i}"), d).await.unwrap();
    }
    let mut mine: Vec<String> = (0..100).map(|i| format!("crates/x/f{i}.rs")).collect();
    mine.push("docs/guide.md".into());
    let paths: Vec<&str> = mine.iter().map(String::as_str).collect();
    let (_, first) = commit(&r, "me", "m-1", &[], &paths).await;
    let id = mine_id(&r, "me").await;
    let all = r.session_overlaps(id).await.unwrap().overlaps;
    let hot: Vec<_> = all.iter().filter(|n| n.count.is_some()).collect();
    assert_eq!(hot.len(), 1, "{all:?}");
    assert_eq!(hot[0].count, Some(HOT_SESSIONS as u64 + 1));
    assert_eq!(hot[0].untrusted.other_path.as_deref(), Some("crates"));
    let named: Vec<_> = all.iter().filter(|n| n.session.is_some()).collect();
    assert_eq!(named.len(), 1, "{all:?}");
    assert_eq!(named[0].principal.as_deref(), Some("p0"));
    assert_eq!(
        named[0].untrusted.other_path.as_deref(),
        Some("docs/guide.md")
    );
    // Said once each, over any number of answers.
    let mut said = first.len();
    for _ in 0..5 {
        said += r
            .notices_for("me", "m-1", &NoticeAsk::default())
            .await
            .len();
    }
    assert_eq!(said, 2);
}

#[tokio::test]
async fn duplicates_by_task_key_and_by_similar_intent() {
    let r = repo();
    let mut a = declare("a-1", "Fix the flaky login test in the auth module");
    a.task_key = Some("L-12".into());
    r.declare_session("ada", a).await.unwrap();
    let mut b = declare("b-1", "Something else entirely");
    b.task_key = Some("L-12".into());
    let resp = r.declare_session("bo", b).await.unwrap();
    assert_eq!(levels(&resp.notices), ["possible_duplicate"]);
    assert_eq!(resp.notices[0].by.as_deref(), Some("task_key"));
    assert_eq!(resp.notices[0].untrusted.task_key.as_deref(), Some("L-12"));
    assert_eq!(resp.notices[0].principal.as_deref(), Some("ada"));
    // Said once: an update of the same session does not repeat it.
    let again = r
        .declare_session("bo", declare("b-1", "Something else entirely"))
        .await;
    assert!(again.unwrap().notices.is_empty());

    let c = declare("c-1", "fix flaky login test in auth module");
    let resp = r.declare_session("cy", c).await.unwrap();
    assert_eq!(levels(&resp.notices), ["possible_duplicate"]);
    assert_eq!(resp.notices[0].by.as_deref(), Some("similar"));
    assert_eq!(resp.notices[0].session, Some(mine_id(&r, "ada").await));
    assert!(resp.notices[0].similarity.unwrap() >= 60);
    // A near miss: same shape, different task.
    let d = declare("d-1", "Fix the flaky logout test in the billing module");
    let resp = r.declare_session("dee", d).await.unwrap();
    assert!(resp.notices.is_empty(), "{:?}", resp.notices);
}

#[tokio::test]
async fn off_and_ended_sessions_say_nothing() {
    let r = repo().with_notices(false);
    commit(&r, "ada", "a-1", &[], &["x/a"]).await;
    let (_, n) = commit(&r, "bo", "b-1", &[], &["x/a"]).await;
    assert!(n.is_empty());
    // Overlaps are still answered when asked for.
    let id = mine_id(&r, "bo").await;
    assert_eq!(
        levels(&r.session_overlaps(id).await.unwrap().overlaps),
        ["same_file"]
    );
    let on = repo();
    commit(&on, "ada", "a-1", &[], &["x/a"]).await;
    commit(&on, "bo", "b-1", &[], &["x/a"]).await;
    let id = mine_id(&on, "ada").await;
    on.abandon_session(id, "ada").await.unwrap();
    assert!(on
        .notices_for("ada", "a-1", &NoticeAsk::default())
        .await
        .is_empty());
    assert!(on
        .notices_for("bo", "b-1", &NoticeAsk::default())
        .await
        .is_empty());
}

/// A coordination store that answers everything but `near_paths` and
/// listings slowly.
struct Slow(MemoryCoordStore);

#[async_trait]
impl CoordStore for Slow {
    async fn upsert_session(&self, u: &SessionUpdate) -> Result<(Session, bool)> {
        self.0.upsert_session(u).await
    }
    async fn get_session(&self, id: u64) -> Result<Option<Session>> {
        self.0.get_session(id).await
    }
    async fn list_sessions(&self, f: &SessionFilter) -> Result<Vec<Session>> {
        if f.agent_session.is_none() {
            tokio::time::sleep(Duration::from_secs(30)).await;
        }
        self.0.list_sessions(f).await
    }
    async fn end_session(&self, id: u64, e: &SessionEnd) -> Result<bool> {
        self.0.end_session(id, e).await
    }
    async fn beat_sessions(&self, b: &[Beat]) -> Result<u64> {
        self.0.beat_sessions(b).await
    }
    async fn sweep_sessions(&self, now: i64, limit: usize) -> Result<SweepReport> {
        self.0.sweep_sessions(now, limit).await
    }
    async fn count_sessions(&self, s: SessionState, cap: usize) -> Result<u64> {
        self.0.count_sessions(s, cap).await
    }
    async fn near_paths(&self, q: &NearQuery) -> Result<Vec<NearRow>> {
        tokio::time::sleep(Duration::from_secs(30)).await;
        self.0.near_paths(q).await
    }
}

/// A slow store costs an answer its notices, never more than the bound.
/// On a paused clock: the bound is measured, not raced against the host.
#[tokio::test(start_paused = true)]
async fn a_slow_store_leaves_notices_out_within_the_bound() {
    let r = repo().with_coord(Some(Arc::new(Slow(MemoryCoordStore::new()))));
    let started = tokio::time::Instant::now();
    let (_, n) = commit(&r, "ada", "a-1", &[], &["x/a"]).await;
    assert!(n.is_empty());
    let mut d = declare("b-1", "a long enough title to compare");
    d.scope = Some(vec!["x/a".into()]);
    d.task_key = Some("K-1".into());
    let resp = r.declare_session("bo", d).await.unwrap();
    assert!(resp.notices.is_empty());
    assert!(r
        .notices_for("ada", "a-1", &NoticeAsk::default())
        .await
        .is_empty());
    // Three answers, each waiting at most the bound.
    assert!(
        started.elapsed() <= 3 * NOTICE_BOUND,
        "{:?}",
        started.elapsed()
    );
}
