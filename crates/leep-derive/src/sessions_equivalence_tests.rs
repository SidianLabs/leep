//! The land-equivalence rule of ADR 0095: coordination state never changes
//! a land's answer. The same random land sequence runs on three
//! repositories, one with no coordination store (its tables dropped), one
//! whose sessions are declared, inferred, beaten, ended and swept at every
//! step, and one whose coordination store fails every call. Every land
//! answer, the refs and the ref log must be the same on all three.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use leep_core::api::{DeclareSessionRequest, LandResponse};
use leep_core::changeset::{EXTRA_AGENT_MODEL, EXTRA_AGENT_SESSION};
use leep_core::coord::{
    Beat, CoordStore, Session, SessionEnd, SessionFilter, SessionUpdate, SweepReport,
};
use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
use leep_core::{Changeset, ChangesetId, Error, FileChange, FileKind, MetaStore, RepoPath, Result};

use crate::land_fast_tests::{files_of, random_edits, Edits, Files, Rng};
use crate::sessions::SessionQuery;
use crate::Repo;

/// A coordination store whose tables are gone: every call fails.
struct Dropped;

fn gone() -> Error {
    Error::Storage("relation \"sessions\" does not exist".into())
}

#[async_trait]
impl CoordStore for Dropped {
    async fn upsert_session(&self, _: &SessionUpdate) -> Result<(Session, bool)> {
        Err(gone())
    }
    async fn get_session(&self, _: u64) -> Result<Option<Session>> {
        Err(gone())
    }
    async fn list_sessions(&self, _: &SessionFilter) -> Result<Vec<Session>> {
        Err(gone())
    }
    async fn end_session(&self, _: u64, _: &SessionEnd) -> Result<bool> {
        Err(gone())
    }
    async fn beat_sessions(&self, _: &[Beat]) -> Result<u64> {
        Err(gone())
    }
    async fn sweep_sessions(&self, _: i64, _: usize) -> Result<SweepReport> {
        Err(gone())
    }
    async fn count_sessions(&self, _: leep_core::coord::SessionState, _: usize) -> Result<u64> {
        Err(gone())
    }
    async fn near_paths(
        &self,
        _: &leep_core::coord::NearQuery,
    ) -> Result<Vec<leep_core::coord::NearRow>> {
        Err(gone())
    }
}

fn repo() -> Repo {
    Repo::new(
        Arc::new(MemoryBlobStore::new()),
        Arc::new(MemoryMetaStore::new()),
    )
}

const AGENTS: &[(&str, &str)] = &[("ada", "x"), ("bo", "y"), ("cy", "z"), ("dee", "x")];

async fn create(r: &Repo, parents: &[ChangesetId], edits: &Edits, agent: usize) -> ChangesetId {
    let mut changes = BTreeMap::new();
    for (path, body) in edits {
        let change = match body {
            Some((text, exec)) => FileChange::Modify {
                content: r.put_blob(Bytes::from(text.clone())).await.unwrap(),
                kind: if *exec {
                    FileKind::Executable
                } else {
                    FileKind::Regular
                },
                size: text.len() as u64,
            },
            None => FileChange::Delete,
        };
        changes.insert(RepoPath::new(path.as_str()).unwrap(), change);
    }
    let cs = Changeset {
        parents: parents.to_vec(),
        author: AGENTS[agent].0.into(),
        timestamp: 1_700_000_000,
        message: format!("{} edits\n\nbody", AGENTS[agent].0),
        changes,
        extra: [
            (EXTRA_AGENT_SESSION.to_string(), format!("s-{agent}")),
            (EXTRA_AGENT_MODEL.to_string(), "model-a".to_string()),
        ]
        .into(),
    };
    let id = r
        .create_changeset_by(cs.clone(), Some(AGENTS[agent].0))
        .await
        .unwrap();
    // What the server does after storing it; its failures never surface.
    let _ = r
        .note_created(
            crate::sessions::SessionSeed::of(&cs).unwrap(),
            id,
            AGENTS[agent].0,
        )
        .await;
    id
}

/// Coordination traffic between lands: declarations, beats, listings,
/// abandons and sweeps. Errors (from the dropped store) are ignored, as the
/// server ignores them.
async fn chatter(r: &Repo, rng: &mut Rng, agent: usize) {
    let (who, dir) = AGENTS[agent];
    if rng.below(3) == 0 {
        let req = DeclareSessionRequest {
            session: format!("s-{agent}"),
            title: Some(format!("work in {dir}")),
            task_key: Some(format!("T-{}", rng.below(3))),
            scope: Some(vec![dir.into()]),
            claims: Some(if rng.below(2) == 0 {
                vec![format!("{dir}/a")]
            } else {
                vec![]
            }),
            ..Default::default()
        };
        let _ = r.declare_session(who, req).await;
    }
    r.heartbeat(who, &format!("s-{agent}"));
    let _ = r.flush_beats().await;
    let _ = r
        .sessions(&SessionQuery {
            path: Some(dir.into()),
            ..Default::default()
        })
        .await;
    if rng.below(10) == 0 {
        if let Ok(list) = r
            .sessions(&SessionQuery {
                principal: Some(who.into()),
                ..Default::default()
            })
            .await
        {
            for s in list.sessions {
                let _ = r.abandon_session(s.id, who).await;
            }
        }
    }
    if rng.below(5) == 0 {
        let _ = r.sweep_sessions(4).await;
    }
}

fn outcome(r: &Result<LandResponse>) -> String {
    match r {
        Ok(resp) => format!("{resp:?}"),
        Err(e) => format!("error: {e}"),
    }
}

async fn ref_log(m: &dyn MetaStore) -> Vec<String> {
    m.ref_log(None, None, 10_000)
        .await
        .unwrap()
        .into_iter()
        .map(|e| {
            format!(
                "{} {} {:?} {:?} {:?} {} {:?} {:?} {:?}",
                e.seq,
                e.name,
                e.old,
                e.new,
                e.actor,
                e.operation,
                e.message,
                e.land_id,
                e.land_changeset
            )
        })
        .collect()
}

/// Answers how many lands landed, rebased and conflicted.
async fn run(seed: u64) -> [usize; 3] {
    let mut rng = Rng::new(seed);
    let without = repo().with_coord(None);
    let with = repo();
    let broken = repo().with_coord(Some(Arc::new(Dropped)));
    assert!(with.coord().is_some());
    let repos = [&without, &with, &broken];
    // Each agent's base, often stale so lands rebase and conflict.
    let mut bases: Vec<Option<ChangesetId>> = vec![None; AGENTS.len()];
    let mut landed = 0;
    let (mut rebased, mut conflicted) = (0, 0);
    for step in 0..40 {
        let agent = rng.below(AGENTS.len());
        if rng.below(3) != 0 {
            bases[agent] = without.meta().get_ref("main").await.unwrap();
        }
        let base = bases[agent];
        let files: Files = match base {
            Some(b) => files_of(&without, b).await,
            None => Files::new(),
        };
        let ops = 1 + rng.below(3);
        let edits = random_edits(&mut rng, &files, ops, AGENTS[agent].1);
        if edits.is_empty() {
            continue;
        }
        let parents: Vec<ChangesetId> = base.into_iter().collect();
        let mut ids = Vec::new();
        let chat = rng.0;
        for r in repos {
            ids.push(create(r, &parents, &edits, agent).await);
            chatter(r, &mut Rng::new(chat), agent).await;
        }
        assert!(
            ids.windows(2).all(|w| w[0] == w[1]),
            "seed {seed} step {step}"
        );
        let id = ids[0];
        let mut answers = Vec::new();
        for r in repos {
            let resp = r.land_as(id, "main", Some(AGENTS[agent].0)).await;
            if let Ok(LandResponse::Landed { landed, .. }) = &resp {
                // What the server does after a land answered.
                let _ = r.note_landed(id, *landed, Some(AGENTS[agent].0)).await;
            }
            answers.push(outcome(&resp));
        }
        assert_eq!(
            answers[0], answers[1],
            "seed {seed} step {step}: with sessions"
        );
        assert_eq!(
            answers[0], answers[2],
            "seed {seed} step {step}: store failing"
        );
        if answers[0].starts_with("Conflicts") {
            conflicted += 1;
        }
        if answers[0].contains("rebased: true") {
            rebased += 1;
        }
        if answers[0].starts_with("Landed") {
            landed += 1;
            bases[agent] = without.meta().get_ref("main").await.unwrap();
        }
    }
    assert!(landed > 0, "seed {seed}: something landed");
    let refs: Vec<_> = futures_util::future::join_all(repos.iter().map(|r| r.meta().list_refs()))
        .await
        .into_iter()
        .map(Result::unwrap)
        .collect();
    assert_eq!(refs[0], refs[1], "seed {seed}");
    assert_eq!(refs[0], refs[2], "seed {seed}");
    let logs = [
        ref_log(without.meta().as_ref()).await,
        ref_log(with.meta().as_ref()).await,
        ref_log(broken.meta().as_ref()).await,
    ];
    assert_eq!(logs[0], logs[1], "seed {seed}");
    assert_eq!(logs[0], logs[2], "seed {seed}");
    // The populated store really was populated.
    let all = with
        .sessions(&SessionQuery {
            all: true,
            limit: Some(100),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(!all.sessions.is_empty(), "seed {seed}");
    [landed, rebased, conflicted]
}

#[tokio::test]
async fn coordination_state_never_changes_a_land_answer() {
    let seeds: u64 = std::env::var("LEEP_COORD_SEEDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(24);
    let mut seen = [0; 3];
    for seed in 0..seeds {
        let n = run(seed).await;
        for (s, n) in seen.iter_mut().zip(n) {
            *s += n;
        }
    }
    // The sequences cover clean lands, rebases and conflicts.
    assert!(
        seen.iter().all(|&n| n > 0),
        "landed, rebased, conflicted: {seen:?}"
    );
}

/// Inference and land closing, end to end on the derive layer: a session
/// appears from a changeset carrying `agent.session`, lists under the paths
/// it touched, and ends as landed when its head lands.
#[tokio::test]
async fn sessions_follow_changesets_and_lands() {
    let r = repo();
    let edits: Edits = [("x/a".to_string(), Some(("1\n".to_string(), false)))].into();
    let id = create(&r, &[], &edits, 0).await;
    let live = r
        .sessions(&SessionQuery {
            path: Some("x".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(live.sessions.len(), 1);
    let s = &live.sessions[0];
    assert_eq!((s.principal.as_str(), s.session.as_str()), ("ada", "s-0"));
    assert_eq!(
        (s.origin.as_str(), s.state.as_str()),
        ("inferred", "active")
    );
    assert_eq!(s.untrusted.title, "ada edits");
    assert_eq!(s.head, Some(id));
    assert_eq!(s.touched, ["x/a"]);
    assert_eq!(s.agent_model.as_deref(), Some("model-a"));
    // Another agent's land does not end it.
    let other: Edits = [("y/b".to_string(), Some(("2\n".to_string(), false)))].into();
    let theirs = create(&r, &[], &other, 1).await;
    assert!(r.note_landed(theirs, theirs, Some("bo")).await.unwrap());
    assert_eq!(r.session(s.id).await.unwrap().state, "active");
    let LandResponse::Landed { landed, .. } = r.land_as(id, "main", Some("ada")).await.unwrap()
    else {
        panic!("lands")
    };
    assert!(r.note_landed(id, landed, Some("ada")).await.unwrap());
    let one = r.session(s.id).await.unwrap();
    assert_eq!(one.state, "landed");
    assert_eq!(one.landed_as, Some(landed));
    assert!(r
        .sessions(&SessionQuery {
            path: Some("x/a".into()),
            ..Default::default()
        })
        .await
        .unwrap()
        .sessions
        .is_empty());
    // The next commit of the same agent session revives it, fresh.
    let more: Edits = [("x/c".to_string(), Some(("3\n".to_string(), false)))].into();
    create(&r, &[landed], &more, 0).await;
    let again = r.session(s.id).await.unwrap();
    assert_eq!(
        (again.state.as_str(), again.touched.as_slice()),
        ("active", &["x/c".to_string()][..])
    );
}
