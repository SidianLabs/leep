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
            // Through a directory: these stop no land, so the cap holds
            // (every notice that stops one comes at once).
            d.scope = Some(vec!["lib".into()]);
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

/// Overlaps asked for on purpose wait longer than notices, but not for
/// ever: each part at most `OVERLAPS_BOUND`.
#[tokio::test(start_paused = true)]
async fn overlaps_on_a_slow_store_answer_within_their_bound() {
    let r = repo().with_coord(Some(Arc::new(Slow(MemoryCoordStore::new()))));
    let mut d = declare("b-1", "a long enough title to compare");
    d.scope = Some(vec!["x/a".into()]);
    let id = r
        .declare_session_with("bo", d, None)
        .await
        .unwrap()
        .session
        .id;
    let started = tokio::time::Instant::now();
    let o = r.session_overlaps(id).await.unwrap();
    assert!(o.overlaps.is_empty());
    assert!(
        started.elapsed() <= crate::notices::OVERLAPS_BOUND,
        "{:?}",
        started.elapsed()
    );
}

/// An agent that restarts under a new session key, with the same task,
/// is not told about its own earlier session.
#[tokio::test]
async fn a_restarted_agent_is_not_its_own_duplicate() {
    let r = repo();
    let mut a = declare("a-1", "Fix the flaky login test in the auth module");
    a.task_key = Some("L-1".into());
    a.scope = Some(vec!["src/auth/login.rs".into()]);
    r.declare_session("ada", a).await.unwrap();
    let mut again = declare("a-2", "Fix the flaky login test in the auth module");
    again.task_key = Some("L-1".into());
    again.scope = Some(vec!["src/auth/login.rs".into()]);
    let resp = r.declare_session("ada", again).await.unwrap();
    assert!(resp.notices.is_empty(), "{:?}", resp.notices);
    // Another principal on the task is still flagged.
    let mut b = declare("b-1", "Something else");
    b.task_key = Some("L-1".into());
    let resp = r.declare_session("bo", b).await.unwrap();
    assert!(resp.notices.iter().any(|n| n.level == "possible_duplicate"));
}

/// Duplicates by wording are looked for among declared sessions, however
/// many inferred ones are newer.
#[tokio::test]
async fn declared_duplicates_are_found_behind_many_inferred_sessions() {
    let r = repo();
    r.declare_session(
        "ada",
        declare("a-1", "Fix the flaky login test in the auth module"),
    )
    .await
    .unwrap();
    for i in 0..=crate::notices::DUPLICATE_CANDIDATES {
        commit(
            &r,
            &format!("p{i}"),
            &format!("k{i}"),
            &[],
            &[&format!("d{i}/f.rs")],
        )
        .await;
    }
    let resp = r
        .declare_session("bo", declare("b-1", "fix flaky login test in auth module"))
        .await
        .unwrap();
    assert_eq!(levels(&resp.notices), ["possible_duplicate"]);
}

/// A session that is a duplicate and also claims my file is one notice,
/// at its highest level, naming the file.
#[tokio::test]
async fn one_notice_per_session_at_its_highest_level() {
    let r = repo();
    let mut a = declare("a-1", "own the token code");
    a.task_key = Some("L-3".into());
    a.claims = Some(vec!["src/auth/token.rs".into()]);
    r.declare_session("ada", a).await.unwrap();
    let mut b = declare("b-1", "parse tokens");
    b.task_key = Some("L-3".into());
    b.scope = Some(vec!["src/auth/token.rs".into()]);
    let resp = r.declare_session("bo", b).await.unwrap();
    assert_eq!(levels(&resp.notices), ["possible_duplicate"]);
    let n = &resp.notices[0];
    assert_eq!(n.untrusted.path.as_deref(), Some("src/auth/token.rs"));
    assert_eq!(n.untrusted.task_key.as_deref(), Some("L-3"));
}

/// The first of a duplicate pair hears of the second on its next answer
/// (a create, a land or a notice read), not only the second at its
/// declaration: the demo's dee was told `same_file` with no task key.
/// Said once, and an agent restarted under a new key is never its own
/// duplicate.
#[tokio::test]
async fn the_first_of_a_duplicate_pair_is_told_too() {
    let r = repo();
    let mut a = declare("a-1", "Fix rounding of invoice totals");
    a.task_key = Some("BILL-42".into());
    r.declare_session("ada", a).await.unwrap();
    let mut b = declare("b-1", "Invoice totals are off by one cent");
    b.task_key = Some("BILL-42".into());
    b.scope = Some(vec!["src/billing/invoice.rs".into()]);
    let told = r.declare_session("bo", b).await.unwrap().notices;
    assert_eq!(levels(&told), ["possible_duplicate"]);

    // ada's next create says so, naming bo, the key and the shared file.
    let (_, n) = commit(&r, "ada", "a-1", &[], &["src/billing/invoice.rs"]).await;
    assert_eq!(levels(&n), ["possible_duplicate"], "{n:?}");
    assert_eq!(n[0].principal.as_deref(), Some("bo"));
    assert_eq!(n[0].by.as_deref(), Some("task_key"));
    assert_eq!(n[0].untrusted.task_key.as_deref(), Some("BILL-42"));
    assert_eq!(
        n[0].untrusted.other_path.as_deref(),
        Some("src/billing/invoice.rs")
    );
    // Once.
    assert!(r
        .notices_for("ada", "a-1", &NoticeAsk::default())
        .await
        .is_empty());

    // A notice read (and a land, which asks the same way) finds it too.
    let mut c = declare("c-1", "Something else");
    c.task_key = Some("BILL-42".into());
    r.declare_session_with("cy", c, None).await.unwrap();
    let n = r.notices_for("ada", "a-1", &NoticeAsk::default()).await;
    assert_eq!(levels(&n), ["possible_duplicate"]);
    assert_eq!(n[0].principal.as_deref(), Some("cy"));

    // ada restarted under a new key: told of the others, never of itself.
    let mut again = declare("a-2", "Fix rounding of invoice totals");
    again.task_key = Some("BILL-42".into());
    r.declare_session_with("ada", again, None).await.unwrap();
    let n = r.notices_for("ada", "a-2", &NoticeAsk::default()).await;
    assert_eq!(levels(&n), ["possible_duplicate", "possible_duplicate"]);
    assert!(
        n.iter().all(|n| n.principal.as_deref() != Some("ada")),
        "{n:?}"
    );
}

/// A session only ever inferred from commits (`LEEP_AGENT_SESSION`, no
/// declaration) records the target ref its client names on the create
/// (`Leep-Target-Ref`), never over a declared one, and hears of lands on
/// it; one whose client named none (an older client) is taken to land on
/// `main`, so it hears of lands there too.
#[tokio::test]
async fn an_undeclared_session_hears_of_lands() {
    let r = repo();
    let (seed, _) = commit(&r, "root", "r-0", &[], &["README"]).await;
    let LandResponse::Landed { landed: base, .. } =
        r.land_as(seed, "main", Some("root")).await.unwrap()
    else {
        panic!()
    };
    // ada commits without declaring, from a client that names no target.
    commit(&r, "ada", "a-1", &[base], &["src/auth/token.rs"]).await;
    let (theirs, _) = commit(&r, "bo", "b-1", &[base], &["src/auth/token.rs"]).await;
    let LandResponse::Landed { landed, .. } = r.land_as(theirs, "main", Some("bo")).await.unwrap()
    else {
        panic!()
    };
    r.note_landed(theirs, landed, Some("bo")).await.unwrap();
    let n = r.notices_for("ada", "a-1", &NoticeAsk::default()).await;
    let l: Vec<_> = n.iter().filter(|n| n.level == "landed").collect();
    assert_eq!(l.len(), 1, "{n:?}");
    assert_eq!(l[0].changeset, Some(landed));

    // A client that names its target: recorded on an inferred session.
    let named = |key: &str, target: &str| {
        let content = leep_core::Hash::of_blob(b"x");
        let cs = Changeset {
            parents: vec![base],
            author: "cy".into(),
            timestamp: 1_700_000_000,
            message: "cy: edit".into(),
            changes: [(
                RepoPath::new("docs/x.md").unwrap(),
                FileChange::Modify {
                    content,
                    kind: FileKind::Regular,
                    size: 1,
                },
            )]
            .into(),
            extra: [(EXTRA_AGENT_SESSION.to_string(), key.to_string())].into(),
        };
        let mut seed = SessionSeed::of(&cs).unwrap();
        seed.target_ref = Some(target.to_string());
        (cs, seed)
    };
    r.put_blob(Bytes::from_static(b"x")).await.unwrap();
    for (key, target, want) in [
        ("c-1", "release", Some("release")),
        ("c-2", "bad ref", None),
    ] {
        let (cs, seed) = named(key, target);
        let id = r.create_changeset_by(cs, Some("cy")).await.unwrap();
        let s = r
            .note_created_session(seed, id, "cy")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(s.target_ref.as_deref(), want, "{key}");
    }
    // A declared target is never replaced by a commit's.
    r.declare_session("dee", declare("d-1", "docs"))
        .await
        .unwrap();
    let (cs, seed) = named("d-1", "release");
    let id = r.create_changeset_by(cs, Some("dee")).await.unwrap();
    let s = r
        .note_created_session(seed, id, "dee")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(s.target_ref.as_deref(), Some("main"));
}

/// A land by a session with my task key is my duplicate's: its `landed`
/// notice says `by: task_key` and names that session and the key; a land
/// by anyone else is a plain `landed` (the demo's eli was told only that a
/// changeset landed).
#[tokio::test]
async fn a_duplicates_land_is_named_as_such() {
    let r = repo();
    let (seed, _) = commit(&r, "root", "r-0", &[], &["README"]).await;
    let LandResponse::Landed { landed: base, .. } =
        r.land_as(seed, "main", Some("root")).await.unwrap()
    else {
        panic!()
    };
    let mut ids = Vec::new();
    for (who, key) in [("dee", "d-1"), ("eli", "e-1")] {
        let mut d = declare(key, "Fix invoice totals");
        d.task_key = Some("BILL-42".into());
        d.base = Some(base);
        d.scope = Some(vec!["src/billing".into()]);
        ids.push(
            r.declare_session_with(who, d, None)
                .await
                .unwrap()
                .session
                .id,
        );
    }
    // Someone else lands in billing first: plain.
    let (other, _) = commit(&r, "bo", "b-1", &[base], &["src/billing/tax.rs"]).await;
    let LandResponse::Landed { landed: l1, .. } =
        r.land_as(other, "main", Some("bo")).await.unwrap()
    else {
        panic!()
    };
    r.note_landed(other, l1, Some("bo")).await.unwrap();
    // dee lands its fix (rebased by the server onto bo's).
    let (mine, _) = commit(&r, "dee", "d-1", &[base], &["src/billing/invoice.rs"]).await;
    let LandResponse::Landed { landed: l2, .. } =
        r.land_as(mine, "main", Some("dee")).await.unwrap()
    else {
        panic!()
    };
    r.note_landed(mine, l2, Some("dee")).await.unwrap();
    let dee = ids[0];
    let n = r.notices_for("eli", "e-1", &NoticeAsk::default()).await;
    assert_eq!(levels(&n), ["landed", "landed"], "{n:?}");
    let twin = n.iter().find(|n| n.changeset == Some(l2)).unwrap();
    assert_eq!(twin.by.as_deref(), Some("task_key"));
    assert_eq!(twin.session, Some(dee));
    assert_eq!(twin.principal.as_deref(), Some("dee"));
    assert_eq!(twin.untrusted.task_key.as_deref(), Some("BILL-42"));
    let plain = n.iter().find(|n| n.changeset == Some(l1)).unwrap();
    assert!(plain.by.is_none() && plain.session.is_none(), "{plain:?}");

    // A pack lists the same land, tagged, as a land of its own.
    let mut a = crate::context::ContextAsk {
        principal: "eli".into(),
        ..Default::default()
    };
    a.req.task = "Fix invoice totals".into();
    a.req.session = Some("e-1".into());
    let p = r.context_pack(&a).await.unwrap();
    let w = p
        .in_flight
        .iter()
        .find(|w| w.notice.changeset == Some(l2))
        .unwrap_or_else(|| panic!("{:?}", p.in_flight));
    assert!(w.id.starts_with("landed:"), "{}", w.id);
    assert_eq!(w.notice.by.as_deref(), Some("task_key"));
}

/// Two lands by my duplicate are two `landed` notices, and its live work
/// is still said beside them: a tagged land names the session but stands
/// only for its changeset.
#[tokio::test]
async fn each_land_of_a_duplicate_is_its_own_notice() {
    let r = repo();
    let (seed, _) = commit(&r, "root", "r-0", &[], &["README"]).await;
    let LandResponse::Landed { landed: base, .. } =
        r.land_as(seed, "main", Some("root")).await.unwrap()
    else {
        panic!()
    };
    for (who, key) in [("dee", "d-1"), ("eli", "e-1")] {
        let mut d = declare(key, "Fix invoice totals");
        d.task_key = Some("BILL-42".into());
        d.base = Some(base);
        d.scope = Some(vec!["src/billing".into()]);
        r.declare_session_with(who, d, None).await.unwrap();
    }
    // dee lands a stack of two changesets to the file; its session stays
    // live (no land hook ran yet).
    let (one, _) = commit(&r, "dee", "d-1", &[base], &["src/billing/invoice.rs"]).await;
    let (two, _) = commit(&r, "dee", "d-1", &[one], &["src/billing/invoice.rs"]).await;
    let LandResponse::Landed { landed, .. } = r.land_as(two, "main", Some("dee")).await.unwrap()
    else {
        panic!()
    };
    let n = r.notices_for("eli", "e-1", &NoticeAsk::default()).await;
    assert_eq!(
        levels(&n),
        ["landed", "landed", "possible_duplicate"],
        "{n:?}"
    );
    let lands: Vec<_> = n.iter().filter_map(|n| n.changeset).collect();
    assert!(lands.contains(&landed) && lands.len() == 2, "{lands:?}");
    assert!(n[..2].iter().all(|n| n.by.as_deref() == Some("task_key")));
    assert_eq!(n[2].principal.as_deref(), Some("dee"));
}

/// However many sessions of a reused task key have landed, a live
/// duplicate behind them is still found.
#[tokio::test]
async fn landed_sessions_never_hide_a_live_duplicate() {
    let r = repo();
    let mut a = declare("a-1", "Fix invoice totals");
    a.task_key = Some("BILL-1".into());
    let old = r
        .declare_session_with("ada", a, None)
        .await
        .unwrap()
        .session
        .id;
    let coord = r.coord().unwrap().clone();
    for i in 0..=crate::notices::KEY_SESSIONS {
        let mut d = declare(&format!("k-{i}"), "an earlier fix");
        d.task_key = Some("BILL-1".into());
        let who = format!("p{i}");
        let id = r
            .declare_session_with(&who, d, None)
            .await
            .unwrap()
            .session
            .id;
        let end = SessionEnd {
            state: SessionState::Landed,
            at: leep_core::store::unix_now(),
            landed_as: None,
            principal: None,
            if_head: None,
        };
        assert!(coord.end_session(id, &end).await.unwrap());
    }
    let mut b = declare("b-1", "Invoice totals are off");
    b.task_key = Some("BILL-1".into());
    let n = r.declare_session("bo", b).await.unwrap().notices;
    assert_eq!(levels(&n), ["possible_duplicate"], "{n:?}");
    assert_eq!(n[0].session, Some(old));
}

/// A session that named no target ref is walked against the server's
/// default ref (`--default-ref`), not a fixed `main`, and its `landed`
/// notice says which ref (`landed_on`) instead of calling it its own.
#[tokio::test]
async fn a_session_without_a_target_hears_of_the_default_ref() {
    let r = repo().with_default_ref("trunk");
    let (seed, _) = commit(&r, "root", "r-0", &[], &["README"]).await;
    let LandResponse::Landed { landed: base, .. } =
        r.land_as(seed, "trunk", Some("root")).await.unwrap()
    else {
        panic!()
    };
    commit(&r, "ada", "a-1", &[base], &["src/x.rs"]).await;
    let (theirs, _) = commit(&r, "bo", "b-1", &[base], &["src/x.rs"]).await;
    let LandResponse::Landed { landed, .. } = r.land_as(theirs, "trunk", Some("bo")).await.unwrap()
    else {
        panic!()
    };
    r.note_landed(theirs, landed, Some("bo")).await.unwrap();
    let n = r.notices_for("ada", "a-1", &NoticeAsk::default()).await;
    let l: Vec<_> = n.iter().filter(|n| n.level == "landed").collect();
    assert_eq!(l.len(), 1, "{n:?}");
    assert_eq!(l[0].landed_on.as_deref(), Some("trunk"));
    // A session that names its target is told without it.
    let mut d = declare("c-1", "work");
    d.target_ref = Some("trunk".into());
    d.base = Some(base);
    d.scope = Some(vec!["src".into()]);
    r.declare_session_with("cy", d, None).await.unwrap();
    let n = r.notices_for("cy", "c-1", &NoticeAsk::default()).await;
    let l: Vec<_> = n.iter().filter(|n| n.level == "landed").collect();
    assert_eq!(l.len(), 1, "{n:?}");
    assert!(l[0].landed_on.is_none());
}

/// A session met both through a directory it scoped and on the file
/// itself is named on the file (the pre-flight check reads it), and one
/// met only through its directory is named on the directory.
#[tokio::test]
async fn a_notice_names_the_shared_file_over_a_directory() {
    let r = repo();
    let mut a = declare("a-1", "auth work");
    a.scope = Some(vec!["src/auth".into()]);
    r.declare_session("ada", a).await.unwrap();
    // ada touches token.rs too; bo then edits it.
    commit(&r, "ada", "a-1", &[], &["src/auth/token.rs"]).await;
    let (_, n) = commit(&r, "bo", "b-1", &[], &["src/auth/token.rs"]).await;
    assert_eq!(levels(&n), ["same_file"]);
    assert_eq!(n[0].untrusted.path.as_deref(), Some("src/auth/token.rs"));
    assert_eq!(
        n[0].untrusted.other_path.as_deref(),
        Some("src/auth/token.rs")
    );
    assert!(n[0].stops_land(&["src/auth/token.rs"]));
    // cy edits another file of ada's directory: only the scope meets it.
    let (_, n) = commit(&r, "cy", "c-1", &[], &["src/auth/keys.rs"]).await;
    assert_eq!(levels(&n), ["same_file"]);
    assert_eq!(n[0].untrusted.other_path.as_deref(), Some("src/auth"));
    assert!(!n[0].stops_land(&["src/auth/keys.rs"]));
}

/// A `landed` notice names a file of mine the land changed itself, ahead
/// of a directory of mine holding another file it changed.
#[tokio::test]
async fn a_landed_notice_names_my_file_over_my_directory() {
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
    commit(&r, "ada", "a-1", &[base], &["src/auth/token.rs"]).await;
    let (theirs, _) = commit(
        &r,
        "bo",
        "b-1",
        &[base],
        &["src/auth/keys.rs", "src/auth/token.rs"],
    )
    .await;
    let LandResponse::Landed { landed, .. } = r.land_as(theirs, "main", Some("bo")).await.unwrap()
    else {
        panic!()
    };
    r.note_landed(theirs, landed, Some("bo")).await.unwrap();
    let n = r.notices_for("ada", "a-1", &NoticeAsk::default()).await;
    let land = n.iter().find(|n| n.level == "landed").expect("{n:?}");
    assert_eq!(land.untrusted.path.as_deref(), Some("src/auth/token.rs"));
    assert_eq!(
        land.untrusted.other_path.as_deref(),
        Some("src/auth/token.rs")
    );
    assert!(land.stops_land(&["src/auth/token.rs"]));
    assert!(!land.stops_land(&["src/auth/mod.rs"]));
}

/// A session said once at a level is said again when it comes to stop a
/// land of a file it did not stop before (the pre-flight check, ADR 0096
/// amendment): ada, scoped to `src`, hears of bo on `src/b.rs`, which
/// stops nothing of hers; bo then edits `src/a.rs`, and ada's commit of
/// it hears of bo again, as a notice that stops it. Once.
#[tokio::test]
async fn a_same_file_notice_is_said_again_when_it_comes_to_stop() {
    let r = repo();
    let mut a = declare("a-1", "src work");
    a.scope = Some(vec!["src".into()]);
    r.declare_session("ada", a).await.unwrap();
    commit(&r, "bo", "b-1", &[], &["src/b.rs"]).await;
    let n = r.notices_for("ada", "a-1", &NoticeAsk::default()).await;
    assert_eq!(levels(&n), ["same_file"]);
    assert!(!n[0].stops_land(&["src/a.rs"]), "{n:?}");
    commit(&r, "bo", "b-1", &[], &["src/a.rs"]).await;
    let (_, n) = commit(&r, "ada", "a-1", &[], &["src/a.rs"]).await;
    assert_eq!(levels(&n), ["same_file"], "said again: {n:?}");
    assert_eq!(n[0].untrusted.other_path.as_deref(), Some("src/a.rs"));
    assert!(n[0].stops_land(&["src/a.rs"]));
    let (_, n) = commit(&r, "ada", "a-1", &[], &["src/a.rs"]).await;
    assert!(n.is_empty(), "once: {n:?}");
}

/// The same for a claim: bo's claim on `src/b.rs` reaches ada through her
/// scope and stops nothing; bo then claims `src/a.rs` too, and ada's commit
/// of it is told again.
#[tokio::test]
async fn a_claim_is_said_again_when_it_comes_to_stop() {
    let r = repo();
    let mut a = declare("a-1", "src work");
    a.scope = Some(vec!["src".into()]);
    r.declare_session("ada", a).await.unwrap();
    let mut b = declare("b-1", "own b");
    b.claims = Some(vec!["src/b.rs".into()]);
    r.declare_session("bo", b.clone()).await.unwrap();
    let n = r.notices_for("ada", "a-1", &NoticeAsk::default()).await;
    assert_eq!(levels(&n), ["claimed"]);
    assert!(!n[0].stops_land(&["src/a.rs"]), "{n:?}");
    b.claims = Some(vec!["src/b.rs".into(), "src/a.rs".into()]);
    r.declare_session("bo", b).await.unwrap();
    let (_, n) = commit(&r, "ada", "a-1", &[], &["src/a.rs"]).await;
    assert_eq!(levels(&n), ["claimed"], "said again: {n:?}");
    assert!(n[0].stops_land(&["src/a.rs"]), "{n:?}");
}

/// The same for a land: ada hears of bo's land on `src/a.rs` and
/// `src/b.rs` while her files are only her scope, so it stops nothing;
/// her commit of `src/a.rs` hears of it again, naming that file.
#[tokio::test]
async fn a_land_is_said_again_when_it_comes_to_stop() {
    let r = repo();
    let (seed, _) = commit(&r, "root", "r-0", &[], &["README"]).await;
    let LandResponse::Landed { landed: base, .. } =
        r.land_as(seed, "main", Some("root")).await.unwrap()
    else {
        panic!()
    };
    let mut a = declare("a-1", "src work");
    a.base = Some(base);
    a.scope = Some(vec!["src".into()]);
    r.declare_session("ada", a).await.unwrap();
    let (theirs, _) = commit(&r, "bo", "b-1", &[base], &["src/a.rs", "src/b.rs"]).await;
    let LandResponse::Landed { landed, .. } = r.land_as(theirs, "main", Some("bo")).await.unwrap()
    else {
        panic!()
    };
    r.note_landed(theirs, landed, Some("bo")).await.unwrap();
    let n = r.notices_for("ada", "a-1", &NoticeAsk::default()).await;
    assert_eq!(levels(&n), ["landed"]);
    let (_, n) = commit(&r, "ada", "a-1", &[base], &["src/a.rs"]).await;
    let land = n.iter().find(|n| n.level == "landed").expect("said again");
    assert_eq!(land.changeset, Some(landed));
    assert_eq!(land.untrusted.other_path.as_deref(), Some("src/a.rs"));
    assert!(land.stops_land(&["src/a.rs"]));
}

/// A `landed` notice names the file the request changes, not the first
/// of the session's files the land changed: ada touched `a.rs` earlier and
/// now commits `b.rs`; bo's land changed both.
#[tokio::test]
async fn a_landed_notice_names_the_requests_file() {
    let r = repo();
    let (seed, _) = commit(&r, "root", "r-0", &[], &["README"]).await;
    let LandResponse::Landed { landed: base, .. } =
        r.land_as(seed, "main", Some("root")).await.unwrap()
    else {
        panic!()
    };
    let mut a = declare("a-1", "src work");
    a.base = Some(base);
    r.declare_session("ada", a).await.unwrap();
    commit(&r, "ada", "a-1", &[base], &["src/a.rs"]).await;
    let (theirs, _) = commit(&r, "bo", "b-1", &[base], &["src/a.rs", "src/b.rs"]).await;
    let LandResponse::Landed { landed, .. } = r.land_as(theirs, "main", Some("bo")).await.unwrap()
    else {
        panic!()
    };
    r.note_landed(theirs, landed, Some("bo")).await.unwrap();
    let (_, n) = commit(&r, "ada", "a-1", &[base], &["src/b.rs"]).await;
    let land = n.iter().find(|n| n.level == "landed").expect("{n:?}");
    assert_eq!(land.untrusted.other_path.as_deref(), Some("src/b.rs"));
    assert!(land.stops_land(&["src/b.rs"]));
}

/// Every notice that stops a land of the request's files comes in the one
/// answer, past the cap of three: five agents claim ada's file.
#[tokio::test]
async fn every_notice_that_stops_comes_in_one_answer() {
    let r = repo();
    for i in 0..5 {
        let mut b = declare(&format!("b-{i}"), "own a");
        b.claims = Some(vec!["src/a.rs".into()]);
        r.declare_session(&format!("bo{i}"), b).await.unwrap();
    }
    // A near one is still capped out.
    commit(&r, "cy", "c-1", &[], &["src/c.rs"]).await;
    let (_, n) = commit(&r, "ada", "a-1", &[], &["src/a.rs"]).await;
    assert_eq!(levels(&n), ["claimed"; 5], "{n:?}");
    assert!(n.len() > MAX_NOTICES);
    let (_, n) = commit(&r, "ada", "a-1", &[], &["src/a.rs"]).await;
    assert!(n.is_empty(), "once: {n:?}");
}
