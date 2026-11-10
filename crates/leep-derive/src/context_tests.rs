//! Context packs (ADR 0097) on the derive layer, with in-memory stores:
//! what each section holds, byte-identical answers, cutting to a budget,
//! sections that fail or run out of time, and other agents' words kept
//! under `untrusted`.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use leep_core::api::{ContextPack, ContextRequest, DeclareSessionRequest, LandResponse};
use leep_core::changeset::{EXTRA_AGENT_MODEL, EXTRA_AGENT_SESSION};
use leep_core::coord::{
    Beat, CoordStore, NearQuery, NearRow, Session, SessionEnd, SessionFilter, SessionState,
    SessionUpdate, SweepReport,
};
use leep_core::memory::{MemoryBlobStore, MemoryCoordStore, MemoryMetaStore};
use leep_core::{Changeset, ChangesetId, Error, FileChange, FileKind, RepoPath, Result};
use serde_json::Value;

use crate::context::{ContextAsk, CONTEXT_BOUND};
use crate::notices::NoticeAsk;
use crate::Repo;

fn repo() -> Repo {
    Repo::new(
        Arc::new(MemoryBlobStore::new()),
        Arc::new(MemoryMetaStore::new()),
    )
}

/// Land a changeset of `files` (path, content) on `main` as `who`, with
/// `message` and the agent `model`.
async fn land(
    r: &Repo,
    who: &str,
    model: &str,
    message: &str,
    files: &[(&str, &str)],
) -> ChangesetId {
    land_by(r, who, who, model, message, files).await
}

/// [`land`] by the token `who` of a changeset whose author is `author`.
async fn land_by(
    r: &Repo,
    who: &str,
    author: &str,
    model: &str,
    message: &str,
    files: &[(&str, &str)],
) -> ChangesetId {
    let parent = r.meta().get_ref("main").await.unwrap();
    let mut changes = BTreeMap::new();
    for (p, body) in files {
        changes.insert(
            RepoPath::new(*p).unwrap(),
            FileChange::Modify {
                content: r.put_blob(Bytes::from(body.to_string())).await.unwrap(),
                kind: FileKind::Regular,
                size: body.len() as u64,
            },
        );
    }
    let cs = Changeset {
        parents: parent.into_iter().collect(),
        author: author.into(),
        timestamp: 1_700_000_000,
        message: message.into(),
        changes,
        extra: [
            (EXTRA_AGENT_SESSION.to_string(), format!("{who}-s")),
            (EXTRA_AGENT_MODEL.to_string(), model.to_string()),
        ]
        .into(),
    };
    let id = r.create_changeset_by(cs, Some(who)).await.unwrap();
    let LandResponse::Landed { landed, .. } = r.land_as(id, "main", Some(who)).await.unwrap()
    else {
        panic!("land")
    };
    landed
}

/// A repository with owners, two agents' landed work on the auth code and
/// a third agent's live session on the same file.
async fn busy_repo(r: &Repo) {
    land(
        r,
        "ada",
        "model-a",
        "auth: parse tokens",
        &[
            ("OWNERS", "root-owner\n"),
            ("src/auth/OWNERS", "rhea\ncarry identical\n"),
            ("src/auth/token.rs", "pub fn parse_token() {}\n"),
            ("src/auth/mod.rs", "mod token;\n"),
        ],
    )
    .await;
    land(
        r,
        "bo",
        "model-b",
        "auth: document parse_token",
        &[
            (
                "src/auth/token.rs",
                "/// Parses.\npub fn parse_token() {}\n",
            ),
            ("docs/auth.md", "Call parse_token.\n"),
        ],
    )
    .await;
    let d = DeclareSessionRequest {
        session: "cy-1".into(),
        title: Some("Speed up parse_token on big inputs".into()),
        approach: Some("Avoid the copy".into()),
        task_key: Some("T-9".into()),
        target_ref: Some("main".into()),
        scope: Some(vec!["src/auth/token.rs".into()]),
        ..Default::default()
    };
    r.declare_session_with("cy", d, None).await.unwrap();
}

fn ask(principal: &str, task: &str, paths: &[&str]) -> ContextAsk {
    ContextAsk {
        principal: principal.into(),
        req: ContextRequest {
            task: task.into(),
            paths: paths.iter().map(|p| p.to_string()).collect(),
            ..Default::default()
        },
        ..Default::default()
    }
}

fn paths_of(p: &ContextPack) -> Vec<String> {
    p.files
        .iter()
        .map(|f| f.untrusted.path.clone().unwrap())
        .collect()
}

#[tokio::test]
async fn a_pack_holds_files_owners_history_live_work_and_duplicates() {
    let r = repo();
    busy_repo(&r).await;
    let mut a = ask(
        "dee",
        "Make `parse_token` faster on big inputs",
        &["src/auth/token.rs"],
    );
    a.req.task_key = Some("T-9".into());
    let p = r.context_pack(&a).await.unwrap();
    assert!(p.omitted.is_empty() && p.cut.is_empty(), "{p:?}");
    assert_eq!(p.target_ref, "main");
    assert_eq!(p.head, r.meta().get_ref("main").await.unwrap().unwrap());

    // Files: the seed first; the search finds the identifier's other
    // files; co-changes are what changed with the seed.
    assert_eq!(paths_of(&p)[0], "src/auth/token.rs");
    assert_eq!(p.files[0].why, ["seed", "search"]);
    let by_path = |path: &str| {
        p.files
            .iter()
            .find(|f| f.untrusted.path.as_deref() == Some(path))
    };
    let docs = by_path("docs/auth.md").expect("searched and co-changed");
    assert_eq!(docs.why, ["search", "co_change"]);
    assert_eq!(by_path("src/auth/mod.rs").unwrap().why, ["co_change"]);
    assert!(
        by_path("src/auth/OWNERS").is_some(),
        "changed with the seed"
    );
    for f in &p.files {
        assert!(f.id.starts_with("file:") && f.id.len() == 17, "{}", f.id);
        assert_eq!(f.bytes, serde_json::to_vec(f).unwrap().len() as u64);
    }

    // Owners: every OWNERS file up the tree, and the strict carry rule.
    let o = &p.owners[0];
    assert_eq!(o.untrusted.path.as_deref(), Some("src/auth/token.rs"));
    assert_eq!(o.untrusted.owners, ["rhea", "root-owner"]);
    assert_eq!(o.untrusted.owners_files, ["src/auth/OWNERS", "OWNERS"]);
    assert_eq!(o.carry.as_deref(), Some("identical"));
    assert_eq!(o.untrusted.carry_file.as_deref(), Some("src/auth/OWNERS"));

    // History: who changed the file last, and with which model.
    let h = &p.history[0];
    assert_eq!(h.untrusted.path.as_deref(), Some("src/auth/token.rs"));
    assert_eq!(h.created_by.as_deref(), Some("bo"));
    assert_eq!(h.untrusted.agent_model.as_deref(), Some("model-b"));
    assert_eq!(
        h.untrusted.title.as_deref(),
        Some("auth: document parse_token")
    );
    let mod_rs = p
        .history
        .iter()
        .find(|h| h.untrusted.path.as_deref() == Some("src/auth/mod.rs"))
        .unwrap();
    assert_eq!(mod_rs.created_by.as_deref(), Some("ada"));

    // Live work: cy's session on the file, which may be the same task:
    // listed once, as a duplicate, with the path it works on.
    let mut a = a.clone();
    a.req.task_key = None;
    let other = r.context_pack(&a).await.unwrap();
    assert_eq!(other.in_flight.len(), 1, "{:?}", other.in_flight);
    let w = &other.in_flight[0];
    assert_eq!(w.notice.level, "same_file");
    assert_eq!(w.notice.principal.as_deref(), Some("cy"));
    assert!(w.id.starts_with("session:"));
    assert_eq!(
        w.notice.untrusted.other_path.as_deref(),
        Some("src/auth/token.rs")
    );
    assert!(p.in_flight.is_empty(), "{:?}", p.in_flight);
    assert_eq!(p.duplicates.len(), 1);
    let d = &p.duplicates[0];
    assert_eq!(d.id, w.id);
    assert_eq!(d.notice.by.as_deref(), Some("task_key"));
    assert_eq!(
        d.notice.untrusted.other_path.as_deref(),
        Some("src/auth/token.rs")
    );
    assert_eq!(d.bytes, serde_json::to_vec(d).unwrap().len() as u64);

    // Ref health: no checks policy, so nothing is red.
    let h = p.ref_health.as_ref().unwrap();
    assert_eq!(h.head, p.head);
    assert_eq!(h.green_ref, "green/main");
    assert!(h.red.is_none() && h.suspects.is_empty());

    // Everything is within the budget, and `bytes` adds up.
    let items: u64 = p.files.iter().map(|i| i.bytes).sum::<u64>()
        + p.owners.iter().map(|i| i.bytes).sum::<u64>()
        + p.history.iter().map(|i| i.bytes).sum::<u64>()
        + p.in_flight.iter().map(|i| i.bytes).sum::<u64>()
        + p.ref_health.iter().map(|i| i.bytes).sum::<u64>()
        + p.duplicates.iter().map(|i| i.bytes).sum::<u64>();
    assert_eq!(items, p.bytes);
    assert!(p.bytes <= p.budget_bytes);
}

/// The same request on the same state is the same bytes, and a pack is a
/// read: notices it lists are still said, once, afterwards.
#[tokio::test]
async fn packs_are_byte_identical_and_say_nothing() {
    let r = repo();
    busy_repo(&r).await;
    r.declare_session_with(
        "dee",
        DeclareSessionRequest {
            session: "d-1".into(),
            title: Some("Profile the token parser".into()),
            target_ref: Some("main".into()),
            scope: Some(vec!["src/auth".into()]),
            ..Default::default()
        },
        None,
    )
    .await
    .unwrap();
    let mut a = ask(
        "dee",
        "Look at `parse_token` and LandCoordinator",
        &["src/auth/token.rs", "docs"],
    );
    a.req.session = Some("d-1".into());
    let first = serde_json::to_vec(&r.context_pack(&a).await.unwrap()).unwrap();
    for _ in 0..3 {
        let again = serde_json::to_vec(&r.context_pack(&a).await.unwrap()).unwrap();
        assert_eq!(first, again);
    }
    let pack: ContextPack = serde_json::from_slice(&first).unwrap();
    assert!(pack.session.is_some(), "the caller's session was found");
    assert!(pack
        .in_flight
        .iter()
        .any(|w| w.notice.principal.as_deref() == Some("cy")));
    // The caller is never its own overlap.
    assert!(pack
        .in_flight
        .iter()
        .all(|w| w.notice.principal.as_deref() != Some("dee")));
    let n = r.notices_for("dee", "d-1", &NoticeAsk::default()).await;
    assert_eq!(n.len(), 1, "{n:?}");
    assert!(r
        .notices_for("dee", "d-1", &NoticeAsk::default())
        .await
        .is_empty());
}

/// A pack is cut to its budget section by section, each section a ranked
/// prefix, and says what it left out.
#[tokio::test]
async fn a_small_budget_cuts_by_section_and_reports_it() {
    let r = repo();
    let many: Vec<(String, String)> = (0..40)
        .map(|i| {
            (
                format!("src/m{i:02}.rs"),
                format!("fn many_things_{i}() {{}}\n"),
            )
        })
        .collect();
    let files: Vec<(&str, &str)> = many.iter().map(|(p, b)| (p.as_str(), b.as_str())).collect();
    land(&r, "ada", "m", "add many_things", &files).await;
    let full = r
        .context_pack(&ask("dee", "Rename many_things", &["src/m00.rs"]))
        .await
        .unwrap();
    assert!(full.cut.is_empty());
    let mut a = ask("dee", "Rename many_things", &["src/m00.rs"]);
    a.req.budget_bytes = Some(2048);
    let small = r.context_pack(&a).await.unwrap();
    assert_eq!(small.budget_bytes, 2048);
    assert!(small.bytes <= 2048 && small.bytes > 0, "{}", small.bytes);
    assert!(small.files.len() < full.files.len());
    // A ranked prefix of what an unlimited pack holds.
    assert_eq!(small.files[..], full.files[..small.files.len()]);
    assert_eq!(small.history[..], full.history[..small.history.len()]);
    let files_cut = small.cut.iter().find(|c| c.section == "files").unwrap();
    assert_eq!(
        files_cut.items as usize,
        full.files.len() - small.files.len()
    );
    let dropped: u64 = full.files[small.files.len()..]
        .iter()
        .map(|f| f.bytes)
        .sum();
    assert_eq!(files_cut.bytes, dropped);
    // A budget past the maximum is clamped, below the minimum raised.
    a.req.budget_bytes = Some(1 << 40);
    assert_eq!(r.context_pack(&a).await.unwrap().budget_bytes, 256 * 1024);
    a.req.budget_bytes = Some(1);
    assert_eq!(r.context_pack(&a).await.unwrap().budget_bytes, 1024);
}

#[tokio::test]
async fn bad_requests_are_refused() {
    let r = repo();
    land(&r, "ada", "m", "seed", &[("a", "a\n")]).await;
    let err = |a: ContextAsk| {
        let r = r.clone();
        async move { r.context_pack(&a).await.unwrap_err() }
    };
    assert!(matches!(err(ask("d", "", &[])).await, Error::Invalid(_)));
    assert!(matches!(
        err(ask("d", &"x".repeat(4097), &[])).await,
        Error::Invalid(_)
    ));
    let many: Vec<String> = (0..65).map(|i| format!("p{i}")).collect();
    let refs: Vec<&str> = many.iter().map(String::as_str).collect();
    assert!(matches!(err(ask("d", "t", &refs)).await, Error::Invalid(_)));
    assert!(err(ask("d", "t", &["a\u{1b}[2J"]))
        .await
        .to_string()
        .contains("control"));
    let mut a = ask("d", "t", &[]);
    a.req.target_ref = Some("nope".into());
    assert!(matches!(err(a).await, Error::NotFound(_)));
    // A session without a task: refused when it is not found.
    let mut a = ask("d", "", &[]);
    a.req.session = Some("none".into());
    assert!(matches!(err(a).await, Error::Invalid(_)));
}

/// A session's title stands for the task when the request has none.
#[tokio::test]
async fn the_session_title_is_the_task_when_none_is_given() {
    let r = repo();
    busy_repo(&r).await;
    let mut a = ask("cy", "", &[]);
    a.req.session = Some("cy-1".into());
    let p = r.context_pack(&a).await.unwrap();
    assert!(p.session.is_some());
    // `parse_token` from the title was searched for.
    assert!(paths_of(&p).contains(&"docs/auth.md".to_string()), "{p:?}");
}

/// A coordination store whose session reads never answer, or always fail.
struct Stuck {
    inner: MemoryCoordStore,
    fail: bool,
}

impl Stuck {
    async fn wait(&self) -> Result<()> {
        if self.fail {
            return Err(Error::Storage("coordination store down".into()));
        }
        tokio::time::sleep(Duration::from_secs(3600)).await;
        Ok(())
    }
}

#[async_trait]
impl CoordStore for Stuck {
    async fn upsert_session(&self, u: &SessionUpdate) -> Result<(Session, bool)> {
        self.inner.upsert_session(u).await
    }
    async fn get_session(&self, id: u64) -> Result<Option<Session>> {
        self.inner.get_session(id).await
    }
    async fn list_sessions(&self, f: &SessionFilter) -> Result<Vec<Session>> {
        self.wait().await?;
        self.inner.list_sessions(f).await
    }
    async fn end_session(&self, id: u64, e: &SessionEnd) -> Result<bool> {
        self.inner.end_session(id, e).await
    }
    async fn beat_sessions(&self, b: &[Beat]) -> Result<u64> {
        self.inner.beat_sessions(b).await
    }
    async fn sweep_sessions(&self, now: i64, limit: usize) -> Result<SweepReport> {
        self.inner.sweep_sessions(now, limit).await
    }
    async fn count_sessions(&self, s: SessionState, cap: usize) -> Result<u64> {
        self.inner.count_sessions(s, cap).await
    }
    async fn near_paths(&self, q: &NearQuery) -> Result<Vec<NearRow>> {
        self.wait().await?;
        self.inner.near_paths(q).await
    }
}

fn omitted(p: &ContextPack) -> Vec<(&str, &str)> {
    p.omitted
        .iter()
        .map(|o| (o.section.as_str(), o.reason.as_str()))
        .collect()
}

/// Sections whose store never answers are omitted at their bound, the
/// rest still come, and the whole call stays within its bound. On a
/// paused clock: the bound is measured, never raced against the host.
#[tokio::test(start_paused = true)]
async fn a_stuck_section_is_omitted_and_the_rest_returned() {
    let stuck = Arc::new(Stuck {
        inner: MemoryCoordStore::new(),
        fail: false,
    });
    let r = repo().with_coord(Some(stuck));
    busy_repo(&r).await;
    let mut a = ask("dee", "Make `parse_token` faster", &["src/auth/token.rs"]);
    a.req.session = Some("d-1".into());
    let started = tokio::time::Instant::now();
    let p = r.context_pack(&a).await.unwrap();
    assert!(
        started.elapsed() <= CONTEXT_BOUND,
        "{:?}",
        started.elapsed()
    );
    assert_eq!(
        omitted(&p),
        [("in_flight", "timeout"), ("duplicates", "timeout")]
    );
    assert!(p.session.is_none(), "the lookup gave up too");
    assert_eq!(paths_of(&p)[0], "src/auth/token.rs");
    assert!(!p.owners.is_empty() && !p.history.is_empty() && p.ref_health.is_some());
}

#[tokio::test]
async fn a_failing_section_is_omitted_and_the_rest_returned() {
    let r = repo().with_coord(Some(Arc::new(Stuck {
        inner: MemoryCoordStore::new(),
        fail: true,
    })));
    land(
        &r,
        "ada",
        "m",
        "seed",
        &[("src/a.rs", "fn some_thing() {}\n")],
    )
    .await;
    let p = r
        .context_pack(&ask(
            "dee",
            "fix some_thing in the parser module",
            &["src/a.rs"],
        ))
        .await
        .unwrap();
    assert_eq!(
        omitted(&p),
        [("in_flight", "error"), ("duplicates", "error")]
    );
    assert_eq!(p.history.len(), 1);
    // Without sessions the parts are off, not failing.
    let off = repo().with_coord(None);
    land(&off, "ada", "m", "seed", &[("src/a.rs", "x\n")]).await;
    let p = off
        .context_pack(&ask("dee", "fix it", &["src/a.rs"]))
        .await
        .unwrap();
    assert_eq!(omitted(&p), [("in_flight", "off"), ("duplicates", "off")]);
}

/// `v` without any `untrusted` object, anywhere.
fn strip_untrusted(v: &mut Value) {
    match v {
        Value::Object(m) => {
            m.remove("untrusted");
            m.values_mut().for_each(strip_untrusted);
        }
        Value::Array(a) => a.iter_mut().for_each(strip_untrusted),
        _ => {}
    }
}

/// What agents wrote (titles, approaches, paths, commit messages, authors,
/// models, owner names) appears only under `untrusted`, whatever it holds.
#[tokio::test]
async fn agent_text_appears_only_under_untrusted() {
    let marks = [
        "PWNED-NL",
        "PWNED-ANSI",
        "PWNED-OSC",
        "PWNED-BIDI",
        "PWNED-LS",
    ];
    let evil = [
        "line\nPWNED-NL",
        "x\u{1b}[2JPWNED-ANSI",
        "\u{1b}]0;PWNED-OSC\u{7}",
        "\u{202e}PWNED-BIDI",
        "a\u{2028}PWNED-LS",
    ];
    let r = repo();
    let dir = format!("src/{}", evil[0]);
    let file = format!("{dir}/{}.rs", evil[1]);
    let owners = format!("{dir}/OWNERS");
    let owners_text = format!("{}\n", evil[3]);
    // The token name is the server's (a principal), so plain; the author,
    // the model and the message are the agent's.
    land_by(
        &r,
        "ada",
        evil[4],
        evil[2],
        &format!("{} {}", evil[2], evil[3]),
        &[
            (file.as_str(), "fn evil_fn() {}\n"),
            (owners.as_str(), owners_text.as_str()),
            ("src/other.rs", "x\n"),
        ],
    )
    .await;
    r.declare_session_with(
        "cy",
        DeclareSessionRequest {
            session: "cy-1".into(),
            title: Some(format!("Fix evil_fn {}", evil[3])),
            approach: Some(evil[3..].join(" ")),
            task_key: Some("PWNED-KEY".into()),
            target_ref: Some("main".into()),
            scope: Some(vec!["src/other.rs".into()]),
            ..Default::default()
        },
        None,
    )
    .await
    .unwrap();
    // Request paths are checked (no control characters); the evil ones
    // come from the search, the co-change count and the session.
    let mut a = ask("dee", "Fix evil_fn PWNED-TASK", &["src/other.rs"]);
    a.req.task_key = Some("PWNED-KEY".into());
    let p = r.context_pack(&a).await.unwrap();
    // cy is a duplicate (the task key) on the file: listed once.
    assert!(!p.duplicates.is_empty() && !p.owners.is_empty());
    assert!(p.duplicates[0].notice.untrusted.other_path.is_some());
    assert!(!p.history.is_empty());
    let mut v = serde_json::to_value(&p).unwrap();
    let all = v.to_string();
    for m in marks {
        assert!(all.contains(m), "{m} is in the pack somewhere");
    }
    strip_untrusted(&mut v);
    let outside = v.to_string();
    for m in marks.iter().chain(&["PWNED-KEY", "PWNED-TASK"]) {
        assert!(!outside.contains(m), "{m} outside untrusted: {outside}");
    }
    for c in ['\u{1b}', '\u{202e}', '\u{2028}', '\u{7}'] {
        assert!(!outside.contains(c), "{c:?} outside untrusted");
    }
}

/// A red head names the suspects that touched the pack's files, and only
/// those.
#[tokio::test]
async fn a_red_head_names_the_suspects_touching_the_files() {
    use leep_core::check::{Check, Outcome};
    let r = repo();
    // Apart, so neither is a co-change of the other.
    land(&r, "ada", "m", "seed a", &[("src/a.rs", "a\n")]).await;
    land(&r, "ada", "m", "seed b", &[("src/b.rs", "b\n")]).await;
    let green = r.meta().get_ref("main").await.unwrap().unwrap();
    let check = |outcome| Check {
        check: "test".into(),
        outcome,
        by: "ci".into(),
        url: String::new(),
        summary: String::new(),
        timestamp: 1,
    };
    r.put_check(green, &check(Outcome::Pass)).await.unwrap();
    r.promote_green(
        "main",
        green,
        &["test".into()],
        &leep_core::RefUpdateContext::new("promote"),
    )
    .await
    .unwrap()
    .unwrap();
    let touching = land(&r, "bo", "model-b", "touch a", &[("src/a.rs", "a2\n")]).await;
    let head = land(&r, "cy", "model-c", "touch b", &[("src/b.rs", "b2\n")]).await;
    r.put_check(head, &check(Outcome::Fail)).await.unwrap();
    let mut a = ask("dee", "fix the failing test", &["src/a.rs"]);
    a.required_checks = vec!["test".into()];
    let p = r.context_pack(&a).await.unwrap();
    let h = p.ref_health.unwrap();
    assert_eq!(h.green, Some(green));
    assert_eq!(h.behind, Some(2));
    assert_eq!(h.required_checks, ["test"]);
    assert_eq!(h.red.as_ref().unwrap().check, "test");
    assert_eq!(h.suspects.len(), 1, "{:?}", h.suspects);
    let s = &h.suspects[0];
    assert_eq!(s.changeset, touching);
    assert_eq!(s.untrusted.paths, ["src/a.rs"]);
    assert_eq!(s.created_by.as_deref(), Some("bo"));
    assert_eq!(s.untrusted.agent_model.as_deref(), Some("model-b"));
}

/// A store whose lookup of one session by key fails, while every other
/// listing answers.
struct NoLookup(MemoryCoordStore);

#[async_trait]
impl CoordStore for NoLookup {
    async fn upsert_session(&self, u: &SessionUpdate) -> Result<(Session, bool)> {
        self.0.upsert_session(u).await
    }
    async fn get_session(&self, id: u64) -> Result<Option<Session>> {
        self.0.get_session(id).await
    }
    async fn list_sessions(&self, f: &SessionFilter) -> Result<Vec<Session>> {
        if f.agent_session.is_some() {
            return Err(Error::Storage("lookup failed".into()));
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
        self.0.near_paths(q).await
    }
}

/// When the caller's session cannot be looked up, the pack stands one in
/// under the caller's key: the caller is still never its own duplicate or
/// overlap.
#[tokio::test]
async fn a_missed_lookup_never_makes_the_caller_its_own_duplicate() {
    let r = repo().with_coord(Some(Arc::new(NoLookup(MemoryCoordStore::new()))));
    land(
        &r,
        "ada",
        "m",
        "seed",
        &[("src/a.rs", "fn some_thing() {}\n")],
    )
    .await;
    let task = "Rewrite some_thing in the parser module for speed";
    r.declare_session_with(
        "dee",
        DeclareSessionRequest {
            session: "d-1".into(),
            title: Some(task.into()),
            target_ref: Some("main".into()),
            scope: Some(vec!["src/a.rs".into()]),
            ..Default::default()
        },
        None,
    )
    .await
    .unwrap();
    let mut a = ask("dee", task, &["src/a.rs"]);
    a.req.session = Some("d-1".into());
    let p = r.context_pack(&a).await.unwrap();
    assert!(p.session.is_none(), "the lookup failed");
    assert!(p.duplicates.is_empty(), "{:?}", p.duplicates);
    assert!(p.in_flight.is_empty(), "{:?}", p.in_flight);
    // Another principal's session of the same key is someone else.
    let mut a = a.clone();
    a.principal = "eve".into();
    let p = r.context_pack(&a).await.unwrap();
    assert_eq!(p.duplicates.len(), 1);
}

/// The request's task key is the one compared, even when the caller's
/// session carries another.
#[tokio::test]
async fn the_requests_task_key_wins() {
    let r = repo();
    busy_repo(&r).await;
    r.declare_session_with(
        "dee",
        DeclareSessionRequest {
            session: "d-1".into(),
            title: Some("Other work entirely".into()),
            task_key: Some("OTHER".into()),
            ..Default::default()
        },
        None,
    )
    .await
    .unwrap();
    let mut a = ask("dee", "x", &[]);
    a.req.session = Some("d-1".into());
    a.req.task_key = Some("T-9".into());
    let p = r.context_pack(&a).await.unwrap();
    assert_eq!(p.duplicates.len(), 1, "{:?}", p.duplicates);
    assert_eq!(p.duplicates[0].notice.principal.as_deref(), Some("cy"));
}

/// An agent-written model or key of any length is cut, so one change
/// cannot fill the history section's budget by itself; owners are capped.
#[tokio::test]
async fn long_names_are_cut_and_owners_capped() {
    let r = repo();
    let owners: String = (0..40).map(|i| format!("owner-{i:02}\n")).collect();
    land(
        &r,
        "ada",
        &"m".repeat(10_000),
        "seed",
        &[("OWNERS", owners.as_str()), ("src/a.rs", "a\n")],
    )
    .await;
    let p = r
        .context_pack(&ask("dee", "fix it", &["src/a.rs"]))
        .await
        .unwrap();
    let h = &p.history[0];
    assert_eq!(h.untrusted.agent_model.as_deref().unwrap().len(), 80);
    assert!(h.bytes < 1024, "{}", h.bytes);
    assert_eq!(p.owners[0].untrusted.owners.len(), 16);
}

/// Under a tight budget a red ref keeps its item, and so its red state:
/// suspects go first, and are reported as cut.
#[tokio::test]
async fn a_tight_budget_keeps_a_red_ref_and_drops_its_suspects() {
    use leep_core::check::{Check, Outcome};
    let r = repo();
    land(&r, "ada", "m", "seed", &[("src/a.rs", "a\n")]).await;
    let green = r.meta().get_ref("main").await.unwrap().unwrap();
    let check = |outcome| Check {
        check: "test".into(),
        outcome,
        by: "ci".into(),
        url: String::new(),
        summary: String::new(),
        timestamp: 1,
    };
    r.put_check(green, &check(Outcome::Pass)).await.unwrap();
    r.promote_green(
        "main",
        green,
        &["test".into()],
        &leep_core::RefUpdateContext::new("promote"),
    )
    .await
    .unwrap()
    .unwrap();
    let mut head = green;
    for i in 0..5 {
        head = land(
            &r,
            "bo",
            &"model-".repeat(13),
            &format!("{} {i}", "touch a, at length".repeat(4)),
            &[("src/a.rs", &format!("a{i}\n"))],
        )
        .await;
    }
    r.put_check(head, &check(Outcome::Fail)).await.unwrap();
    let mut a = ask("dee", "fix the failing test", &["src/a.rs"]);
    a.required_checks = vec!["test".into()];
    let full = r.context_pack(&a).await.unwrap();
    assert_eq!(full.ref_health.as_ref().unwrap().suspects.len(), 5);
    a.req.budget_bytes = Some(1024);
    let p = r.context_pack(&a).await.unwrap();
    let h = p.ref_health.as_ref().expect("the red ref is kept");
    assert_eq!(h.red.as_ref().unwrap().check, "test");
    assert!(h.suspects.len() < 5);
    assert_eq!(h.bytes, serde_json::to_vec(h).unwrap().len() as u64);
    let cut = p.cut.iter().find(|c| c.section == "suspects").unwrap();
    assert_eq!(cut.items as usize, 5 - h.suspects.len());
    assert!(p.bytes <= 1024, "{}", p.bytes);
}
