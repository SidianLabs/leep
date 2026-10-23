//! `rebase-onto` (ADR 0062): the git mirror's replay of a git-primary ref
//! onto commits git gained, ordered with the ref's lands.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use leep_core::api::{LandResponse, RebaseOntoResponse};
use leep_core::changeset::EXTRA_REBASED_FROM;
use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
use leep_core::refname::NOT_FORWARD_MARKER;
use leep_core::{ChangesetId, Error, LandId, Lease, MetaStore, Result};

use crate::land_coord::{Coordinated, CoordinatorConfig, Forwarded, LandCoordinator, LandForward};
use crate::tests::{commit, repo};
use crate::{LandPolicy, Repo, GIT_MIRROR_OPERATION};

/// `main` at `b` over `a` over `base`, with `t` (git's side, two commits)
/// also on `base`: neither contains the other.
struct Split {
    base: ChangesetId,
    a: ChangesetId,
    b: ChangesetId,
    t1: ChangesetId,
    t: ChangesetId,
}

async fn split(r: &Repo) -> Split {
    let base = commit(r, &[], &[("base", Some("0\n"))], "base").await;
    r.land(base, "main").await.unwrap();
    let a = commit(r, &[base], &[("a", Some("a\n"))], "a").await;
    let b = commit(r, &[a], &[("b", Some("b\n"))], "b").await;
    r.land(b, "main").await.unwrap();
    let t1 = commit(r, &[base], &[("t1", Some("t1\n"))], "t1").await;
    let t = commit(r, &[t1], &[("t", Some("t\n"))], "t").await;
    Split { base, a, b, t1, t }
}

fn land_id(n: u8) -> LandId {
    format!("{n:02x}112233445566778899aabbccddeeff")
        .parse()
        .unwrap()
}

/// The copy `rebase` makes of `cs` onto `onto` when the change applies as is.
async fn copy_id(r: &Repo, cs: ChangesetId, onto: ChangesetId) -> ChangesetId {
    let mut c = r.get_changeset(cs).await.unwrap();
    c.parents = vec![onto];
    c.extra.insert(EXTRA_REBASED_FROM.into(), cs.to_string());
    c.id()
}

#[tokio::test]
async fn replays_the_stack_onto_git_and_logs_one_move() {
    let r = repo();
    let s = split(&r).await;
    let RebaseOntoResponse::Moved { head, rebased } = r
        .rebase_onto("main", s.t, s.base, Some("mirror"), None)
        .await
        .unwrap()
    else {
        panic!("expected a move")
    };
    let a2 = copy_id(&r, s.a, s.t).await;
    let b2 = copy_id(&r, s.b, a2).await;
    assert_eq!(rebased, vec![(s.a, a2), (s.b, b2)]);
    assert_eq!(head, b2);
    assert_eq!(r.resolve("main").await.unwrap(), b2);
    assert_eq!(r.rebased_origin(b2).await.unwrap(), Some(s.b));
    let files: Vec<_> = r
        .list_files(head, "")
        .await
        .unwrap()
        .into_iter()
        .map(|f| f.path.as_str().to_string())
        .collect();
    assert_eq!(files, ["a", "b", "base", "t", "t1"]);
    let log = r.ref_log_page(Some("main"), None, None, 10).await.unwrap();
    let e = &log.entries[0];
    assert_eq!(
        (e.operation.as_str(), e.old, e.new, e.actor.as_deref()),
        (GIT_MIRROR_OPERATION, Some(s.b), Some(b2), Some("mirror"))
    );
    assert_eq!(log.entries.len(), 3, "two lands and one replay");
}

#[tokio::test]
async fn a_git_side_that_contains_the_head_is_a_fast_forward() {
    let r = repo();
    let s = split(&r).await;
    let on_top = commit(&r, &[s.b], &[("g", Some("g\n"))], "g").await;
    let resp = r
        .rebase_onto("main", on_top, s.b, None, None)
        .await
        .unwrap();
    assert_eq!(
        resp,
        RebaseOntoResponse::Moved {
            head: on_top,
            rebased: vec![]
        }
    );
    assert_eq!(r.resolve("main").await.unwrap(), on_top);
    // Already there: nothing moves, the same answer.
    let again = r.rebase_onto("main", s.b, s.b, None, None).await.unwrap();
    assert_eq!(
        again,
        RebaseOntoResponse::Moved {
            head: on_top,
            rebased: vec![]
        }
    );
}

#[tokio::test]
async fn a_moved_base_is_refused_and_moves_nothing() {
    let r = repo();
    let s = split(&r).await;
    let r2 = r.rebase_onto("main", s.t, s.a, None, None).await;
    assert!(matches!(r2, Err(Error::Conflict(_))), "{r2:?}");
    assert_eq!(r.resolve("main").await.unwrap(), s.b);
    assert!(matches!(
        r.rebase_onto("nope", s.t, s.base, None, None).await,
        Err(Error::NotFound(_))
    ));
    assert!(matches!(
        r.rebase_onto("tags/v1", s.t, s.base, None, None).await,
        Err(Error::Invalid(_))
    ));
}

#[tokio::test]
async fn conflicts_store_nothing_and_move_nothing() {
    let r = repo();
    let base = commit(&r, &[], &[("x", Some("0\n"))], "base").await;
    r.land(base, "main").await.unwrap();
    let a = commit(&r, &[base], &[("a", Some("a\n"))], "a").await;
    let b = commit(&r, &[a], &[("x", Some("leep\n"))], "b").await;
    r.land(b, "main").await.unwrap();
    let t = commit(&r, &[base], &[("x", Some("git\n"))], "t").await;
    let RebaseOntoResponse::Conflicts {
        onto, conflicts, ..
    } = r.rebase_onto("main", t, base, None, None).await.unwrap()
    else {
        panic!("expected conflicts")
    };
    assert_eq!(onto, t);
    assert_eq!(conflicts[0].path.as_str(), "x");
    assert_eq!(r.resolve("main").await.unwrap(), b);
    // `a` replayed cleanly before `b` conflicted; its copy was not stored.
    let a2 = copy_id(&r, a, t).await;
    assert_eq!(r.meta().get_changeset(&a2).await.unwrap(), None);
    let log = r.ref_log_page(Some("main"), None, None, 10).await.unwrap();
    assert_eq!(log.entries.len(), 2, "no move logged");
}

#[tokio::test]
async fn a_land_id_answers_a_retry_with_the_first_result() {
    let r = repo();
    let s = split(&r).await;
    let first = r
        .rebase_onto("main", s.t, s.base, None, Some(land_id(1)))
        .await
        .unwrap();
    let RebaseOntoResponse::Moved { head, .. } = first.clone() else {
        panic!()
    };
    // An agent lands on top; the retry still answers as the first call did,
    // though its expected base is no longer the merge base.
    let c = commit(&r, &[head], &[("c", Some("c\n"))], "c").await;
    r.land(c, "main").await.unwrap();
    let again = r
        .rebase_onto("main", s.t, s.base, None, Some(land_id(1)))
        .await
        .unwrap();
    assert_eq!(again, first);
    assert_eq!(r.resolve("main").await.unwrap(), c);
    // The id names that replay: another `onto` under it is refused.
    let other = r
        .rebase_onto("main", s.t1, s.base, None, Some(land_id(1)))
        .await;
    assert!(
        matches!(other, Err(Error::LandIdReused { .. })),
        "{other:?}"
    );
}

/// Records the histories it was asked about; refuses `deny`.
struct Recorder {
    seen: Mutex<Vec<Vec<ChangesetId>>>,
    deny: Option<ChangesetId>,
    protects: bool,
}

impl Recorder {
    fn new(deny: Option<ChangesetId>, protects: bool) -> Arc<Self> {
        Arc::new(Recorder {
            seen: Mutex::new(Vec::new()),
            deny,
            protects,
        })
    }
}

#[async_trait]
impl LandPolicy for Recorder {
    fn applies_to(&self, target_ref: &str) -> bool {
        target_ref == "main"
    }

    fn protects(&self, target_ref: &str) -> bool {
        self.protects && self.applies_to(target_ref)
    }

    async fn check_land(
        &self,
        _repo: &Repo,
        _target_ref: &str,
        _head: Option<ChangesetId>,
        new_history: &[ChangesetId],
    ) -> Result<()> {
        self.seen.lock().unwrap().push(new_history.to_vec());
        match self.deny {
            Some(d) if new_history.contains(&d) => Err(Error::Invalid("refused".into())),
            _ => Ok(()),
        }
    }
}

#[tokio::test]
async fn the_policy_checks_the_imported_changesets_only() {
    let plain = repo();
    let s = split(&plain).await;
    let policy = Recorder::new(None, false);
    let r = plain.clone().with_land_policy(policy.clone());
    assert!(matches!(
        r.rebase_onto("main", s.t, s.base, None, None)
            .await
            .unwrap(),
        RebaseOntoResponse::Moved { .. }
    ));
    let seen = policy.seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 1);
    let mut checked = seen[0].clone();
    checked.sort();
    let mut imported = vec![s.t1, s.t];
    imported.sort();
    assert_eq!(checked, imported, "git's changesets, not the stack's");

    // A refused import moves nothing.
    let plain = repo();
    let s = split(&plain).await;
    let r = plain
        .clone()
        .with_land_policy(Recorder::new(Some(s.t1), false));
    let refused = r.rebase_onto("main", s.t, s.base, None, None).await;
    assert!(matches!(refused, Err(Error::Invalid(_))), "{refused:?}");
    assert_eq!(r.resolve("main").await.unwrap(), s.b);
}

#[tokio::test]
async fn a_protected_ref_is_refused() {
    let plain = repo();
    let s = split(&plain).await;
    let r = plain.clone().with_land_policy(Recorder::new(None, true));
    let Err(Error::Invalid(m)) = r.rebase_onto("main", s.t, s.base, None, None).await else {
        panic!("expected a refusal")
    };
    assert!(
        m.contains(NOT_FORWARD_MARKER) && m.contains("GitHub"),
        "{m}"
    );
    assert_eq!(r.resolve("main").await.unwrap(), s.b);
}

/// Lands racing a replay are never lost: each is in the replayed stack (it
/// landed first) or above the result (it queued after), and the final head
/// holds every land's file.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lands_racing_a_replay_are_kept() {
    let r = repo();
    let s = split(&r).await;
    let mut ids = Vec::new();
    for i in 0..24 {
        let name = format!("land/{i}");
        ids.push(commit(&r, &[s.b], &[(&name, Some("x\n"))], &name).await);
    }
    let mut tasks = Vec::new();
    for (i, id) in ids.iter().copied().enumerate() {
        let r = r.clone();
        tasks.push(tokio::spawn(async move { r.land(id, "main").await }));
        if i == 8 {
            tokio::task::yield_now().await;
        }
    }
    let replay = {
        let r = r.clone();
        tokio::spawn(async move { r.rebase_onto("main", s.t, s.base, None, None).await })
    };
    let mut landed = Vec::new();
    for t in tasks {
        match t.await.unwrap().unwrap() {
            LandResponse::Landed { landed: l, .. } => landed.push(l),
            other => panic!("{other:?}"),
        }
    }
    let RebaseOntoResponse::Moved { head, rebased } = replay.await.unwrap().unwrap() else {
        panic!("expected a move")
    };
    let originals: BTreeMap<_, _> = rebased.iter().copied().collect();
    for l in &landed {
        assert!(
            originals.contains_key(l) || r.is_ancestor(head, *l).await.unwrap(),
            "{l} is neither replayed nor above the replay"
        );
    }
    let tip = r.resolve("main").await.unwrap();
    let files = r.list_files(tip, "").await.unwrap();
    for i in 0..24 {
        let name = format!("land/{i}");
        assert!(files.iter().any(|f| f.path.as_str() == name), "{name} lost");
    }
    for name in ["a", "b", "t", "t1"] {
        assert!(files.iter().any(|f| f.path.as_str() == name), "{name} lost");
    }
    let log = r.meta().ref_log(Some("main"), None, 1000).await.unwrap();
    assert_eq!(
        log.iter()
            .filter(|e| e.operation == GIT_MIRROR_OPERATION)
            .count(),
        1
    );
}

/// A follower forwards a replay to the server holding the ref's lease,
/// which queues it with its lands.
#[tokio::test]
async fn a_replay_is_forwarded_to_the_lease_holder() {
    let meta: Arc<dyn MetaStore> = Arc::new(MemoryMetaStore::new());
    let blobs = Arc::new(MemoryBlobStore::new());
    let server = |name: &str| {
        Repo::new(blobs.clone(), meta.clone()).with_land_coordinator(LandCoordinator::new(
            meta.clone(),
            CoordinatorConfig::new(name, name),
        ))
    };
    let (leader, follower) = (server("a"), server("b"));
    let s = split(&leader).await;
    // The leader's own land takes the lease.
    let c = commit(&leader, &[s.b], &[("c", Some("c\n"))], "c").await;
    leader
        .land_coordinated::<()>(c, "main", None, false, None, false, &Never)
        .await
        .unwrap();
    assert!(leader
        .land_coordinator()
        .unwrap()
        .held_epoch("main")
        .is_some());

    struct ToLeader(Repo, ChangesetId, ChangesetId);
    #[async_trait]
    impl LandForward<Result<RebaseOntoResponse>> for ToLeader {
        async fn forward(&self, _: &Lease) -> Forwarded<Result<RebaseOntoResponse>> {
            match self
                .0
                .rebase_onto_coordinated::<()>("main", self.1, self.2, None, None, true, &Never)
                .await
            {
                Ok(Coordinated::Landed(r)) => Forwarded::Answered(Ok(r)),
                Ok(Coordinated::NotLeader) => Forwarded::NotLeader,
                Ok(Coordinated::Forwarded(_)) => unreachable!("Never forwards"),
                Err(e) => Forwarded::Answered(Err(e)),
            }
        }
    }
    let r = follower
        .rebase_onto_coordinated(
            "main",
            s.t,
            s.base,
            None,
            None,
            false,
            &ToLeader(leader.clone(), s.t, s.base),
        )
        .await
        .unwrap();
    let Coordinated::Forwarded(Ok(RebaseOntoResponse::Moved { head, rebased })) = r else {
        panic!("expected the leader's answer")
    };
    assert_eq!(rebased.len(), 3, "a, b and the leader's land c");
    assert_eq!(leader.resolve("main").await.unwrap(), head);
    assert_eq!(
        follower.land_coordinator().unwrap().held_epoch("main"),
        None
    );
}

struct Never;

#[async_trait]
impl<A: Send> LandForward<A> for Never {
    async fn forward(&self, _: &Lease) -> Forwarded<A> {
        panic!("nothing is forwarded from here")
    }
}
