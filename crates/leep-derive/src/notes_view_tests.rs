//! Notes as readers see them (ADR 0098, second change), with in-memory
//! stores: staleness by tree lookup at the head, a missing `at_changeset`
//! or an unreadable tree read as unknown, endorsement by human owners only,
//! and owners' reports.

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use leep_core::api::{AddNoteRequest, NoteAnchor, NoteEntry};
use leep_core::coord::{
    AnchorType, NewNote, NoteFilter, NoteKind, NoteState, NoteTrust, AGENT_NOTE_TTL_SECS,
    ENDORSED_NOTE_TTL_SECS,
};
use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
use leep_core::{Changeset, ChangesetId, Error, FileChange, FileKind, Hash, RepoPath};

use crate::notes::{NoteActor, NoteQuery};
use crate::notes_view::{NoteBudget, NoteSelect};
use crate::Repo;

fn repo() -> Repo {
    Repo::new(
        Arc::new(MemoryBlobStore::new()),
        Arc::new(MemoryMetaStore::new()),
    )
}

/// A changeset on `main`'s head writing `files` (`None` deletes), landed on
/// `main`.
async fn commit(r: &Repo, files: &[(&str, Option<&str>)]) -> ChangesetId {
    let parent = r.meta().get_ref("main").await.unwrap();
    let mut changes = BTreeMap::new();
    for (p, body) in files {
        let change = match body {
            Some(b) => FileChange::Modify {
                content: r.put_blob(Bytes::from(b.to_string())).await.unwrap(),
                kind: FileKind::Regular,
                size: b.len() as u64,
            },
            None => FileChange::Delete,
        };
        changes.insert(RepoPath::new(*p).unwrap(), change);
    }
    let cs = Changeset {
        parents: parent.into_iter().collect(),
        author: "ada".into(),
        timestamp: 1_700_000_000,
        message: "m".into(),
        changes,
        extra: Default::default(),
    };
    let id = r.create_changeset(cs).await.unwrap();
    assert!(r.meta().cas_ref("main", parent, Some(id)).await.unwrap());
    id
}

fn req(kind: &str, anchor: &str, title: &str) -> AddNoteRequest {
    AddNoteRequest {
        kind: "gotcha".into(),
        anchor: NoteAnchor {
            kind: kind.into(),
            value: anchor.into(),
        },
        at: None,
        lines: None,
        title: title.into(),
        body: String::new(),
        supersedes: None,
        agent_model: None,
    }
}

async fn note(r: &Repo, who: &str, trust: NoteTrust, anchor: &str, title: &str) -> NoteEntry {
    r.add_note(who, trust, None, req("path", anchor, title))
        .await
        .unwrap()
}

fn human(name: &str) -> NoteActor<'_> {
    NoteActor {
        name,
        human: true,
        may_review: true,
    }
}

async fn listed(r: &Repo, path: &str) -> Vec<NoteEntry> {
    r.notes(&NoteQuery {
        path: Some(path.into()),
        ..Default::default()
    })
    .await
    .unwrap()
    .notes
}

fn budget() -> NoteBudget {
    NoteBudget {
        checked: 100,
        until: tokio::time::Instant::now() + std::time::Duration::from_secs(1),
    }
}

/// A file note is stale once the file's content at the head is not what
/// it was written against, or the file is gone; a directory note once the
/// directory is gone; a changeset note never.
#[tokio::test]
async fn staleness_is_a_tree_lookup_at_the_head() {
    let r = repo();
    commit(
        &r,
        &[
            ("src/a.rs", Some("v1\n")),
            ("src/b.rs", Some("b\n")),
            ("old/x.rs", Some("x\n")),
        ],
    )
    .await;
    let a = note(&r, "ada", NoteTrust::Agent, "src/a.rs", "a").await;
    let b = note(&r, "ada", NoteTrust::Agent, "src/b.rs", "b").await;
    let dir = note(&r, "ada", NoteTrust::Agent, "old", "old dir").await;
    let head = r.meta().get_ref("main").await.unwrap().unwrap();
    let cs = r
        .add_note(
            "ada",
            NoteTrust::Agent,
            None,
            req("changeset", "main", "cs"),
        )
        .await
        .unwrap();
    let stale = |v: &[NoteEntry], id: u64| v.iter().find(|n| n.id == id).unwrap().stale;
    let all = r.notes(&NoteQuery::default()).await.unwrap().notes;
    for id in [a.id, b.id, dir.id, cs.id] {
        assert_eq!(stale(&all, id), Some(false), "note {id} at the start");
    }
    // Content changed; a sibling untouched; the directory removed.
    commit(
        &r,
        &[
            ("src/a.rs", Some("v2\n")),
            ("old/x.rs", None),
            ("new.rs", Some("n\n")),
        ],
    )
    .await;
    let all = r.notes(&NoteQuery::default()).await.unwrap().notes;
    assert_eq!(stale(&all, a.id), Some(true));
    assert_eq!(stale(&all, b.id), Some(false));
    assert_eq!(stale(&all, dir.id), Some(true));
    assert_eq!(stale(&all, cs.id), Some(false), "changeset notes never");
    // Judged at another ref's head on asking: the old head still has it.
    r.meta().cas_ref("old", None, Some(head)).await.unwrap();
    let at_old = r
        .notes(&NoteQuery {
            target_ref: Some("old".into()),
            ..Default::default()
        })
        .await
        .unwrap()
        .notes;
    assert_eq!(stale(&at_old, a.id), Some(false));
    assert!(matches!(
        r.notes(&NoteQuery {
            target_ref: Some("nonesuch".into()),
            ..Default::default()
        })
        .await,
        Err(Error::NotFound(_))
    ));
    // Back to the content it was written against: fresh again.
    commit(&r, &[("src/a.rs", Some("v1\n"))]).await;
    assert_eq!(r.note(a.id).await.unwrap().stale, Some(false));
}

/// `at_changeset` is no GC root and may be gone: a note that needs it is
/// unknown, never an error; so is every path note when the head's tree
/// cannot be read. A note that does not need it is judged as usual.
#[tokio::test]
async fn a_missing_at_changeset_or_tree_is_unknown_not_an_error() {
    let r = repo();
    commit(&r, &[("src/a.rs", Some("v1\n"))]).await;
    let gone = ChangesetId(Hash::of_blob(b"collected long ago"));
    let row = |anchor: &str, blob: Option<Hash>, lines: Option<(u32, u32)>| NewNote {
        kind: NoteKind::Finding,
        anchor_type: AnchorType::Path,
        anchor: anchor.into(),
        at_changeset: Some(gone),
        at_blob: blob,
        lines,
        title: "t".into(),
        body: String::new(),
        principal: "ada".into(),
        agent_model: None,
        session_id: None,
        trust: NoteTrust::Agent,
        supersedes: None,
        created: leep_core::store::unix_now(),
        expires: None,
        max_open: 100,
        max_per_hour: 100,
        max_per_day: 100,
    };
    let c = r.coord().unwrap().clone();
    // A file note without its content (it needs `at_changeset`).
    let no_blob = c
        .add_note(&row("src/a.rs", None, Some((1, 2))))
        .await
        .unwrap();
    // A directory that is gone (is it gone, or was it a file? unknown).
    let no_dir = c.add_note(&row("src/zz", None, None)).await.unwrap();
    // With its content: judged without `at_changeset`.
    let with_blob = c
        .add_note(&row("src/a.rs", Some(Hash::of_blob(b"v1\n")), None))
        .await
        .unwrap();
    let all = r.notes(&NoteQuery::default()).await.unwrap().notes;
    let stale = |id: u64| all.iter().find(|n| n.id == id).unwrap().stale;
    assert_eq!(stale(no_blob.id), None);
    assert_eq!(stale(no_dir.id), None);
    assert_eq!(stale(with_blob.id), Some(false));
    assert_eq!(r.note(no_blob.id).await.unwrap().stale, None);
    // A head whose tree cannot be read: every path note unknown, no error.
    let viewed = r
        .notes_for(
            NoteSelect::Filter(&NoteFilter {
                limit: 10,
                ..Default::default()
            }),
            Some(gone),
            budget(),
            leep_core::store::unix_now(),
        )
        .await
        .unwrap()
        .notes;
    assert_eq!(viewed.len(), 3);
    assert!(viewed.iter().all(|v| v.stale.is_none() && !v.endorsed()));
    // Nothing checked past the budget: unknown.
    let none = NoteBudget {
        checked: 0,
        ..budget()
    };
    let head = r.meta().get_ref("main").await.unwrap();
    let viewed = r
        .notes_for(
            NoteSelect::Paths(&["src/a.rs".to_string()]),
            head,
            none,
            leep_core::store::unix_now(),
        )
        .await
        .unwrap();
    assert!(viewed.notes.iter().all(|v| v.stale.is_none()));
    assert_eq!((viewed.unchecked, viewed.timed_out), (2, 0));
}

/// Only a human owner may endorse: a person's token, allowed to review,
/// named by OWNERS for the note's path at the default ref's head, and not
/// the note's writer; the note open, on a path and fresh. An endorsement
/// clears the expiry, holds only while the note is fresh, and is kept for
/// audit when it lapses.
#[tokio::test]
async fn only_a_human_owner_endorses() {
    let r = repo();
    commit(
        &r,
        &[
            ("src/OWNERS", Some("hugo\nrobo\n")),
            ("src/a.rs", Some("v1\n")),
            ("docs/b.md", Some("b\n")),
        ],
    )
    .await;
    let n = note(&r, "ada", NoteTrust::Agent, "src/a.rs", "lock order").await;
    let policy = |e: Error| assert!(matches!(e, Error::Policy(_)), "{e}");
    // An owner by OWNERS, but an agent's token.
    policy(
        r.endorse_note(n.id, &NoteActor::agent("robo"))
            .await
            .unwrap_err(),
    );
    // A person, but not an owner.
    policy(r.endorse_note(n.id, &human("zed")).await.unwrap_err());
    // An owner who may not review.
    let no_review = NoteActor {
        may_review: false,
        ..human("hugo")
    };
    policy(r.endorse_note(n.id, &no_review).await.unwrap_err());
    // The writer, even a human owner.
    let own = note(&r, "hugo", NoteTrust::Human, "src/a.rs", "mine").await;
    policy(r.endorse_note(own.id, &human("hugo")).await.unwrap_err());
    // A path nobody owns: nobody may.
    let docs = note(&r, "ada", NoteTrust::Agent, "docs/b.md", "d").await;
    policy(r.endorse_note(docs.id, &human("hugo")).await.unwrap_err());
    // A closed note.
    let closed = note(&r, "ada", NoteTrust::Agent, "src/a.rs", "gone").await;
    r.retract_note(closed.id, "ada").await.unwrap();
    assert!(matches!(
        r.endorse_note(closed.id, &human("hugo")).await,
        Err(Error::Conflict(_))
    ));
    // A changeset note: owners are per path.
    let cs = r
        .add_note("ada", NoteTrust::Agent, None, req("changeset", "main", "c"))
        .await
        .unwrap();
    assert!(matches!(
        r.endorse_note(cs.id, &human("hugo")).await,
        Err(Error::Invalid(_))
    ));
    assert!(matches!(
        r.endorse_note(9999, &human("hugo")).await,
        Err(Error::NotFound(_))
    ));
    // Nothing above changed the note.
    let before = r.note(n.id).await.unwrap();
    assert_eq!((before.endorsed_by, before.endorsed), (None, false));

    let e = r.endorse_note(n.id, &human("hugo")).await.unwrap();
    let at = e.endorsed_at.expect("endorsed_at");
    assert_eq!(
        (e.endorsed_by.as_deref(), e.endorsed, e.stale, e.expires),
        (
            Some("hugo"),
            true,
            Some(false),
            Some(at + ENDORSED_NOTE_TTL_SECS)
        )
    );
    // A year from the endorsement, not forever: a note left stale for good
    // is not kept for good. Endorsing again renews it.
    let again = r.endorse_note(n.id, &human("hugo")).await.unwrap();
    assert_eq!(
        (again.endorsed_by.as_deref(), again.endorsed),
        (Some("hugo"), true)
    );
    assert!(again.expires >= e.expires);
    // It outlives an agent note's life: the sweep past it keeps it.
    let swept = r
        .coord()
        .unwrap()
        .sweep_notes(e.created + AGENT_NOTE_TTL_SECS + 1, 1000)
        .await
        .unwrap();
    assert!(swept.expired >= 1, "the unendorsed agent notes went");
    assert!(r.note(n.id).await.unwrap().state == "open");
    assert!(matches!(r.note(docs.id).await, Err(Error::NotFound(_))));
    // The file changes: the endorsement lapses, and is still on record.
    commit(&r, &[("src/a.rs", Some("v2\n"))]).await;
    let lapsed = listed(&r, "src/a.rs")
        .await
        .into_iter()
        .find(|x| x.id == n.id)
        .unwrap();
    assert_eq!(
        (lapsed.endorsed_by.as_deref(), lapsed.endorsed, lapsed.stale),
        (Some("hugo"), false, Some(true))
    );
    // And a stale note cannot be endorsed.
    let fresh = note(&r, "bo", NoteTrust::Agent, "src/a.rs", "v2 note").await;
    commit(&r, &[("src/a.rs", Some("v3\n"))]).await;
    assert!(matches!(
        r.endorse_note(fresh.id, &human("hugo")).await,
        Err(Error::Conflict(_))
    ));
    // A directory note's owners are those of the files in it.
    let dir = note(&r, "bo", NoteTrust::Agent, "src", "the src rules").await;
    let d = r.endorse_note(dir.id, &human("hugo")).await.unwrap();
    assert!(d.endorsed);
    // A subdirectory's owner owns nothing above it.
    commit(
        &r,
        &[
            ("src/deep/OWNERS", Some("sub\n")),
            ("src/deep/x.rs", Some("x\n")),
        ],
    )
    .await;
    let parent = note(&r, "bo", NoteTrust::Agent, "src", "parent rules").await;
    let deep = note(&r, "bo", NoteTrust::Agent, "src/deep", "deep rules").await;
    policy(r.endorse_note(parent.id, &human("sub")).await.unwrap_err());
    assert!(
        r.endorse_note(deep.id, &human("sub"))
            .await
            .unwrap()
            .endorsed
    );
    // A directory anchor of 256 bytes, the longest: its `OWNERS` probe is
    // longer than any anchor, and still read (no 400).
    let long_dir = format!("src/{}", "d".repeat(252));
    let long_file = format!("{long_dir}/f.rs");
    commit(&r, &[(long_file.as_str(), Some("f\n"))]).await;
    let long = note(&r, "bo", NoteTrust::Agent, &long_dir, "long").await;
    assert!(
        r.endorse_note(long.id, &human("hugo"))
            .await
            .unwrap()
            .endorsed
    );
    policy(r.endorse_note(long.id, &human("zed")).await.unwrap_err());
    // An endorsed note left stale for good is not kept for good: past a
    // year from its endorsement the sweep deletes it.
    let lapsed = r.note(n.id).await.unwrap();
    assert_eq!((lapsed.stale, lapsed.endorsed), (Some(true), false));
    r.coord()
        .unwrap()
        .sweep_notes(lapsed.expires.unwrap() + 1, 1000)
        .await
        .unwrap();
    assert!(matches!(r.note(n.id).await, Err(Error::NotFound(_))));
}

/// A report by a human owner hides a human or endorsed note; anyone
/// else's only counts it. Agent notes are hidden by any report.
#[tokio::test]
async fn an_owners_report_hides_human_and_endorsed_notes() {
    let r = repo();
    commit(
        &r,
        &[("src/OWNERS", Some("hugo\n")), ("src/a.rs", Some("v1\n"))],
    )
    .await;
    let h = note(&r, "hana", NoteTrust::Human, "src/a.rs", "human").await;
    let e = note(&r, "ada", NoteTrust::Agent, "src/a.rs", "endorsed").await;
    r.endorse_note(e.id, &human("hugo")).await.unwrap();
    for id in [h.id, e.id] {
        // An agent token, a person who owns nothing, an agent owner: counted.
        for who in [
            NoteActor::agent("bo"),
            human("zed"),
            NoteActor::agent("hugo"),
        ] {
            let rep = r.report_note(id, &who).await.unwrap();
            assert!(!rep.hidden && rep.note.state == "open", "{id} by {who:?}");
        }
        let rep = r.report_note(id, &human("hugo")).await.unwrap();
        assert!(rep.hidden, "{id}");
        assert_eq!(
            (rep.note.state.as_str(), rep.note.reported_by.as_deref()),
            ("reported", Some("hugo"))
        );
    }
    // An operator's undo gives each its life back: a year from its
    // endorsement once endorsed.
    assert_eq!(r.unreport_notes(None, Some("hugo")).await.unwrap(), 2);
    let back = r.note(e.id).await.unwrap();
    assert_eq!((back.state.as_str(), back.endorsed), ("open", true));
    assert_eq!(
        back.expires,
        back.endorsed_at.map(|t| t + ENDORSED_NOTE_TTL_SECS)
    );
    let stored = r.coord().unwrap().get_note(h.id).await.unwrap().unwrap();
    assert_eq!(
        (stored.state, stored.expires),
        (
            NoteState::Open,
            Some(stored.created + leep_core::coord::HUMAN_NOTE_TTL_SECS)
        )
    );
}

/// A listing page checks every note on it (up to 100, the largest page),
/// and closed notes cost no lookup: an `all=true` page of closed notes is
/// left unjudged.
#[tokio::test(start_paused = true)]
async fn a_listing_page_checks_up_to_100_open_notes() {
    use crate::notes_view::LIST_NOTES_CHECKED;
    let r = repo();
    commit(&r, &[("src/a.rs", Some("v1\n"))]).await;
    let mut ids = Vec::new();
    for who in ["l1", "l2", "l3", "l4"] {
        for i in 0..26 {
            ids.push(
                note(&r, who, NoteTrust::Agent, "src/a.rs", &format!("{who} {i}"))
                    .await
                    .id,
            );
        }
    }
    let page = r
        .notes(&NoteQuery {
            limit: Some(100),
            ..Default::default()
        })
        .await
        .unwrap()
        .notes;
    assert_eq!(page.len(), LIST_NOTES_CHECKED);
    assert!(page.iter().all(|n| n.stale == Some(false)));
    // Past the page's bound the store is not even asked: 104 open notes,
    // and a filter of 104 rows leaves 4 unchecked.
    let head = r.meta().get_ref("main").await.unwrap();
    let all = r
        .notes_for(
            NoteSelect::Filter(&NoteFilter {
                limit: 104,
                ..Default::default()
            }),
            head,
            NoteBudget {
                checked: LIST_NOTES_CHECKED,
                ..budget()
            },
            leep_core::store::unix_now(),
        )
        .await
        .unwrap();
    assert_eq!((all.notes.len(), all.unchecked), (104, 4));
    // Closed notes are not judged, and do not use the budget.
    for (i, id) in ids.iter().enumerate() {
        let who = ["l1", "l2", "l3", "l4"][i / 26];
        r.retract_note(*id, who).await.unwrap();
    }
    let closed = r
        .notes_for(
            NoteSelect::Filter(&NoteFilter {
                limit: 104,
                ..Default::default()
            }),
            head,
            NoteBudget {
                checked: 1,
                ..budget()
            },
            leep_core::store::unix_now(),
        )
        .await
        .unwrap();
    assert_eq!((closed.unchecked, closed.timed_out), (0, 0));
    assert!(closed.notes.iter().all(|v| v.stale.is_none()));
}

/// Checks past their deadline are not run: on a paused clock, a deadline
/// already passed leaves every open path note unknown (so none is served
/// as endorsed) and counts them, while changeset notes stay fresh.
#[tokio::test(start_paused = true)]
async fn checks_past_the_deadline_are_unknown_and_counted() {
    let r = repo();
    commit(
        &r,
        &[("src/OWNERS", Some("hugo\n")), ("src/a.rs", Some("v1\n"))],
    )
    .await;
    let n = note(&r, "ada", NoteTrust::Agent, "src/a.rs", "a").await;
    r.endorse_note(n.id, &human("hugo")).await.unwrap();
    note(&r, "ada", NoteTrust::Agent, "src", "dir").await;
    let cs = r
        .add_note(
            "ada",
            NoteTrust::Agent,
            None,
            req("changeset", "main", "cs"),
        )
        .await
        .unwrap();
    let head = r.meta().get_ref("main").await.unwrap();
    let past = NoteBudget {
        checked: 100,
        until: tokio::time::Instant::now(),
    };
    tokio::time::advance(std::time::Duration::from_millis(1)).await;
    let v = r
        .notes_for(
            NoteSelect::Filter(&NoteFilter {
                limit: 10,
                ..Default::default()
            }),
            head,
            past,
            leep_core::store::unix_now(),
        )
        .await
        .unwrap();
    assert_eq!((v.timed_out, v.unchecked), (2, 0));
    for x in &v.notes {
        let want = (x.note.id == cs.id).then_some(false);
        assert_eq!(x.stale, want, "note {}", x.note.id);
        assert!(!x.endorsed());
    }
    // With time left, the same call judges them.
    let v = r
        .notes_for(
            NoteSelect::Filter(&NoteFilter {
                limit: 10,
                ..Default::default()
            }),
            head,
            budget(),
            leep_core::store::unix_now(),
        )
        .await
        .unwrap();
    assert_eq!(v.timed_out, 0);
    assert!(v.notes.iter().any(|x| x.endorsed()));
}

/// An endorsement racing a retraction or a supersession of the same note
/// never leaves an open note endorsed that its answer said was refused, nor
/// endorses the note that superseded it.
#[tokio::test]
async fn endorsing_races_retracting_and_superseding() {
    let r = repo();
    commit(
        &r,
        &[("src/OWNERS", Some("hugo\n")), ("src/a.rs", Some("v1\n"))],
    )
    .await;
    for i in 0..16 {
        let n = note(&r, &format!("w{i}"), NoteTrust::Agent, "src/a.rs", "race").await;
        let who = format!("w{i}");
        let hugo = human("hugo");
        if i % 2 == 0 {
            let (e, x) = tokio::join!(r.endorse_note(n.id, &hugo), r.retract_note(n.id, &who));
            let x = x.unwrap();
            let stored = r.coord().unwrap().get_note(n.id).await.unwrap().unwrap();
            assert_eq!(
                (x.state.as_str(), stored.state),
                ("retracted", NoteState::Retracted)
            );
            match e {
                Ok(e) => assert_eq!(stored.endorsed_by.as_deref(), Some("hugo"), "{e:?}"),
                Err(Error::Conflict(_)) => assert_eq!(stored.endorsed_by, None),
                Err(e) => panic!("{e}"),
            }
        } else {
            let mut s = req("path", "src/a.rs", "race, again");
            s.supersedes = Some(n.id);
            let (e, x) = tokio::join!(
                r.endorse_note(n.id, &hugo),
                r.add_note(&who, NoteTrust::Agent, None, s)
            );
            let newer = x.unwrap();
            let old = r.coord().unwrap().get_note(n.id).await.unwrap().unwrap();
            let new = r
                .coord()
                .unwrap()
                .get_note(newer.id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(old.state, NoteState::Superseded);
            assert_eq!(
                new.endorsed_by, None,
                "a superseding note starts unendorsed"
            );
            match e {
                Ok(_) => assert_eq!(old.endorsed_by.as_deref(), Some("hugo")),
                Err(Error::Conflict(_)) => assert_eq!(old.endorsed_by, None),
                Err(e) => panic!("{e}"),
            }
        }
    }
}

/// An operator's undo of one endorser's endorsements gives each note its
/// unendorsed life back, so it ranks and expires as before.
#[tokio::test]
async fn an_operator_unendorses_by_endorser() {
    let r = repo();
    commit(
        &r,
        &[("src/OWNERS", Some("hugo\n")), ("src/a.rs", Some("v1\n"))],
    )
    .await;
    let n = note(&r, "ada", NoteTrust::Agent, "src/a.rs", "a").await;
    r.endorse_note(n.id, &human("hugo")).await.unwrap();
    assert_eq!(r.unendorse_notes(None, Some("hugo")).await.unwrap(), 1);
    let back = r.note(n.id).await.unwrap();
    assert_eq!(
        (back.endorsed_by, back.endorsed, back.expires),
        (None, false, Some(back.created + AGENT_NOTE_TTL_SECS))
    );
    assert_eq!(r.unendorse_notes(Some(n.id), None).await.unwrap(), 0);
}
