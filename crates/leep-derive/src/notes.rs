//! Notes (ADR 0098): shared memory between agents, anchored to a path or a
//! changeset.
//!
//! A note is coordination state: advisory and expiring, and nothing a land,
//! a review or an ownership answer decides reads one. Its title, body and
//! anchor are another agent's words, returned only under `untrusted`; the
//! server vouches for the rest (id, kind, trust, state, principal, times).
//! The writer is the token, never a body field, and its trust tier is the
//! token's `kind`, fixed when the note is written.

use std::sync::Arc;

use leep_core::api::{AddNoteRequest, NoteEntry, NotesResponse, ReportNoteResponse};
use leep_core::coord::{
    check_anchor, check_key, normalize_path, AnchorType, CoordStore, NewNote, Note, NoteChange,
    NoteFilter, NoteKind, NoteState, NoteSweep, NoteTrust, SessionFilter, SessionState,
    AGENT_NOTE_TTL_SECS, HUMAN_NOTE_TTL_SECS, MAX_NOTES_PER_DAY, MAX_NOTES_PER_HOUR,
    MAX_OPEN_NOTES_PER_PRINCIPAL, MAX_PATH_BYTES, MAX_REPORTS_PER_HOUR, NOTES_PER_HOUR_MARKER,
    REPORTS_PER_HOUR_MARKER, TOO_MANY_NOTES_MARKER,
};
use leep_core::store::unix_now;
use leep_core::{Error, Result, TreeEntry};

use crate::context::SECTION_BOUND;
use crate::notes_view::{NoteBudget, NoteSelect, ViewedNote, LIST_NOTES_CHECKED};
use crate::owners::OwnersResolver;
use crate::Repo;

/// Default and largest page of a listing.
pub const DEFAULT_NOTE_LIMIT: usize = 50;
pub const MAX_NOTE_LIMIT: usize = 100;

/// Rows one sweep step deletes per part.
pub const NOTE_SWEEP_STEP: usize = 1000;

/// What `GET /v1/notes` asks for.
#[derive(Clone, Debug, Default)]
pub struct NoteQuery {
    /// Notes on this path and on the directories above it.
    pub path: Option<String>,
    /// Notes on this changeset (a full id or a ref).
    pub changeset: Option<String>,
    pub principal: Option<String>,
    /// One state; `None` with `all` false means open.
    pub state: Option<NoteState>,
    pub all: bool,
    pub before: Option<u64>,
    pub limit: Option<usize>,
    /// The ref whose head staleness is judged at; default the server's
    /// default ref.
    pub target_ref: Option<String>,
}

/// The wire form of `n` as seen at `now`, its staleness not judged (a
/// closed note, or an answer to a write): an open note past its `expires`
/// that the sweep has not deleted yet shows as `expired`.
pub fn note_entry(n: Note, now: i64) -> NoteEntry {
    ViewedNote {
        note: n,
        stale: None,
    }
    .entry(now)
}

/// Who asks to endorse or report a note: the token's name, whether it is
/// a person's (`"kind": "human"`), and whether it may review.
#[derive(Clone, Copy, Debug)]
pub struct NoteActor<'a> {
    pub name: &'a str,
    pub human: bool,
    pub may_review: bool,
}

impl<'a> NoteActor<'a> {
    /// An agent's token that may review: what most callers are.
    pub fn agent(name: &'a str) -> Self {
        NoteActor {
            name,
            human: false,
            may_review: true,
        }
    }
}

/// Why a note was refused, for `leep_notes_refused_total`.
fn refusal(e: &Error) -> Option<&'static str> {
    match e {
        Error::Invalid(m) if m.contains(TOO_MANY_NOTES_MARKER) => Some("too_many"),
        Error::Invalid(m) if m.contains(NOTES_PER_HOUR_MARKER) => Some("rate"),
        Error::Invalid(m) if m.contains(leep_core::coord::NOTES_PER_DAY_MARKER) => Some("daily"),
        Error::Invalid(_) | Error::InvalidPath(..) | Error::InvalidHash(_) => Some("invalid"),
        Error::Policy(_) | Error::Conflict(_) => Some("supersede"),
        _ => None,
    }
}

impl Repo {
    fn notes_on(&self) -> Result<&Arc<dyn CoordStore>> {
        self.coord.as_ref().ok_or_else(leep_core::coord::no_notes)
    }

    /// Write `principal`'s note from `req` (ADR 0098), with the trust tier
    /// of its token. `session_key` is the caller's `Leep-Session`: its
    /// active session, if any, is recorded, and gives the default `at` (its
    /// target ref) and model. A path anchor must exist at `at`.
    pub async fn add_note(
        &self,
        principal: &str,
        trust: NoteTrust,
        session_key: Option<&str>,
        req: AddNoteRequest,
    ) -> Result<NoteEntry> {
        let supersedes = req.supersedes;
        let r = self
            .add_note_inner(principal, trust, session_key, req)
            .await;
        match &r {
            Ok(e) => {
                self.note_added_events(e, supersedes);
                metrics::counter!("leep_notes_added_total", "trust" => e.trust.clone(), "kind" => e.kind.clone())
                    .increment(1);
            }
            Err(e) => {
                if let Some(reason) = refusal(e) {
                    metrics::counter!("leep_notes_refused_total", "reason" => reason).increment(1);
                }
            }
        }
        r
    }

    async fn add_note_inner(
        &self,
        principal: &str,
        trust: NoteTrust,
        session_key: Option<&str>,
        req: AddNoteRequest,
    ) -> Result<NoteEntry> {
        let coord = self.notes_on()?;
        let now = unix_now();
        let kind = NoteKind::parse(req.kind.trim())?;
        let anchor_type = AnchorType::parse(req.anchor.kind.trim())?;
        let ttl = match trust {
            NoteTrust::Agent => AGENT_NOTE_TTL_SECS,
            NoteTrust::Human => HUMAN_NOTE_TTL_SECS,
        };
        let anchor = match anchor_type {
            AnchorType::Path => normalize_path(&req.anchor.value),
            AnchorType::Changeset => req.anchor.value.trim().to_string(),
        };
        let mut n = NewNote {
            kind,
            anchor_type,
            anchor,
            at_changeset: None,
            at_blob: None,
            lines: req.lines.map(|[a, b]| (a, b)),
            title: req.title,
            body: req.body,
            principal: principal.to_string(),
            agent_model: req.agent_model.filter(|m| !m.is_empty()),
            session_id: None,
            trust,
            supersedes: req.supersedes,
            created: now,
            expires: Some(now + ttl),
            max_open: MAX_OPEN_NOTES_PER_PRINCIPAL,
            max_per_hour: MAX_NOTES_PER_HOUR,
            max_per_day: MAX_NOTES_PER_DAY,
        };
        if anchor_type == AnchorType::Changeset {
            // A full id or a ref is stored as the full id it names now;
            // clients expand short ids first.
            let id = self.resolve(&n.anchor).await.map_err(|e| match e {
                Error::NotFound(_) | Error::Invalid(_) | Error::InvalidHash(_) => {
                    Error::Invalid("note anchor names no changeset".into())
                }
                e => e,
            })?;
            n.anchor = id.to_string();
            n.at_changeset = Some(id);
        }
        // The text first: a bad note costs no history reads.
        n.check()?;
        let session = match session_key.filter(|k| check_key("agent session", k).is_ok()) {
            Some(key) => coord
                .list_sessions(&SessionFilter {
                    principal: Some(principal.to_string()),
                    agent_session: Some(key.to_string()),
                    states: vec![SessionState::Active],
                    limit: 1,
                    skip_paths: true,
                    ..Default::default()
                })
                .await?
                .pop(),
            None => None,
        };
        if let Some(s) = &session {
            n.session_id = Some(s.id);
            // A session's model was checked less strictly than a note's
            // text: taken only when it would pass as one.
            if n.agent_model.is_none() {
                n.agent_model = s
                    .agent_model
                    .clone()
                    .filter(|m| leep_core::coord::check_note_model(m).is_ok());
            }
        }
        if anchor_type == AnchorType::Path {
            let at = match req.at.as_deref().map(str::trim).filter(|a| !a.is_empty()) {
                Some(a) => a.to_string(),
                None => session
                    .as_ref()
                    .and_then(|s| s.target_ref.clone())
                    .unwrap_or_else(|| self.default_ref().to_string()),
            };
            let id = self.resolve(&at).await?;
            let root = self.root_tree(id).await?;
            match self.entry_at(root, &n.anchor).await? {
                None => {
                    return Err(Error::Invalid(format!(
                        "note anchor is not a file or directory at {id}"
                    )))
                }
                Some(TreeEntry::File { content, .. }) => n.at_blob = Some(content),
                Some(TreeEntry::Dir { .. }) if n.lines.is_some() => {
                    return Err(Error::Invalid(
                        "note lines need a file anchor, not a directory".into(),
                    ))
                }
                Some(TreeEntry::Dir { .. }) => {}
            }
            n.at_changeset = Some(id);
        }
        check_anchor(n.anchor_type, &n.anchor)?;
        let note = coord.add_note(&n).await?;
        // No `stale` on a write's answer: the note is fresh at `at` by
        // construction, and judging it at another ref here would only
        // confuse a writer on a feature ref. Reads judge it.
        Ok(note_entry(note, now))
    }

    /// The notes `q` asks for, newest first, each judged stale or not at
    /// the head of `q.target_ref` by [`Repo::notes_for`], as the pack does.
    pub async fn notes(&self, q: &NoteQuery) -> Result<NotesResponse> {
        self.notes_on()?;
        let limit = q
            .limit
            .unwrap_or(DEFAULT_NOTE_LIMIT)
            .clamp(1, MAX_NOTE_LIMIT);
        let path = match q.path.as_deref().map(normalize_path) {
            Some(p) if p.len() > MAX_PATH_BYTES => {
                return Err(Error::Invalid(format!(
                    "a note path is at most {MAX_PATH_BYTES} bytes"
                )))
            }
            Some(p) if !p.is_empty() => {
                leep_core::RepoPath::new(p.clone())?;
                Some(p)
            }
            _ => None,
        };
        let changeset = match q.changeset.as_deref().map(str::trim) {
            Some(c) if !c.is_empty() => Some(self.resolve(c).await?.to_string()),
            _ => None,
        };
        let states = match (q.all, q.state) {
            (true, _) => vec![],
            (false, s) => vec![s.unwrap_or(NoteState::Open)],
        };
        let filter = NoteFilter {
            path,
            changeset,
            principal: q.principal.clone(),
            states,
            before: q.before,
            limit,
        };
        let head = self.judged_at(q.target_ref.as_deref()).await?;
        let now = unix_now();
        let budget = NoteBudget {
            checked: LIST_NOTES_CHECKED,
            until: tokio::time::Instant::now() + SECTION_BOUND,
        };
        let rows = self
            .notes_for(NoteSelect::Filter(&filter), head, budget, now)
            .await?
            .notes;
        let next = (rows.len() == limit)
            .then(|| rows.last().map(|n| n.note.id))
            .flatten();
        let open_only = !q.all;
        let notes = rows
            .into_iter()
            .map(|v| v.entry(now))
            // An open note past its life is not served as open.
            .filter(|e| !(open_only && e.state == "expired"))
            .collect();
        Ok(NotesResponse { notes, next })
    }

    /// The head staleness is judged at: `target_ref`'s (404 when it names
    /// no ref), else the default ref's, else none (everything unknown).
    async fn judged_at(&self, target_ref: Option<&str>) -> Result<Option<leep_core::ChangesetId>> {
        match target_ref.map(str::trim) {
            Some(r) if !r.is_empty() => Ok(Some(
                self.meta()
                    .get_ref(r)
                    .await?
                    .ok_or_else(|| Error::NotFound(format!("ref {r:?}")))?,
            )),
            _ => self.meta().get_ref(self.default_ref()).await,
        }
    }

    /// One note, in any state, its staleness judged at the default ref's
    /// head as a listing judges it; 404 if unknown.
    pub async fn note(&self, id: u64) -> Result<NoteEntry> {
        self.note_at(id, None).await
    }

    /// One note, its staleness judged at `target_ref`'s head (default the
    /// default ref's), as `GET /v1/notes?ref=` judges it.
    pub async fn note_at(&self, id: u64, target_ref: Option<&str>) -> Result<NoteEntry> {
        let n = self.stored_note(id).await?;
        let head = self.judged_at(target_ref).await?;
        let budget = NoteBudget {
            checked: 1,
            until: tokio::time::Instant::now() + SECTION_BOUND,
        };
        let stale = self
            .staleness(std::slice::from_ref(&n), head, budget)
            .await
            .stale;
        Ok(ViewedNote {
            note: n,
            stale: stale[0],
        }
        .entry(unix_now()))
    }

    async fn stored_note(&self, id: u64) -> Result<Note> {
        self.notes_on()?
            .get_note(id)
            .await?
            .ok_or_else(|| Error::NotFound(format!("note {id}")))
    }

    /// Whether `who` is named by `OWNERS` for `n`'s path at `head` (ADR
    /// 0011's reading). A directory note's owners are those of a file in
    /// the directory.
    async fn owns(&self, head: leep_core::ChangesetId, n: &Note, who: &str) -> Result<bool> {
        if n.anchor_type != AnchorType::Path {
            return Ok(false);
        }
        let dir = n.at_blob.is_none() && n.lines.is_none();
        let probe = if dir {
            format!("{}/{}", n.anchor, leep_core::review::OWNERS_FILE)
        } else {
            n.anchor.clone()
        };
        // A probe that is no repository path (a 256-byte directory anchor
        // plus `/OWNERS`) has no owner: refused as not owned, not as a bad
        // request.
        let Ok(path) = leep_core::RepoPath::new(probe) else {
            return Ok(false);
        };
        let mut r = OwnersResolver::new(self, Some(head)).await?;
        Ok(r.owners_of(&path).await?.contains(who))
    }

    /// Whether `actor` is a human owner of `n`'s path at the default ref's
    /// head: a person's token that may review and that `OWNERS` names.
    async fn human_owner(&self, n: &Note, actor: &NoteActor<'_>) -> Result<bool> {
        if !(actor.human && actor.may_review) {
            return Ok(false);
        }
        match self.meta().get_ref(self.default_ref()).await? {
            Some(head) => self.owns(head, n, actor.name).await,
            None => Ok(false),
        }
    }

    /// A human owner vouches for note `id` (ADR 0098): the caller's token
    /// must be a person's, may review, must own the note's path at the
    /// default ref's head and must not be its writer (`Error::Policy`,
    /// 403); the note must be an open path note (`Invalid` for a changeset
    /// note) that is not stale there (`Conflict`, 409). The endorsement
    /// clears the note's expiry and lapses while the note is stale. An
    /// endorsed note is answered as it is.
    pub async fn endorse_note(&self, id: u64, actor: &NoteActor<'_>) -> Result<NoteEntry> {
        let r = self.endorse_inner(id, actor).await;
        match &r {
            Ok((e, true)) => {
                tracing::info!(note = id, by = actor.name, "note endorsed");
                metrics::counter!("leep_notes_endorsed_total").increment(1);
                self.note_endorsed_event(e, actor.name);
            }
            Ok(_) => {}
            Err(e) => {
                let reason = match e {
                    Error::Policy(_) => "endorse_forbidden",
                    Error::Conflict(_) => "endorse_conflict",
                    _ => "endorse_other",
                };
                metrics::counter!("leep_notes_refused_total", "reason" => reason).increment(1);
            }
        }
        r.map(|(e, _)| e)
    }

    async fn endorse_inner(&self, id: u64, actor: &NoteActor<'_>) -> Result<(NoteEntry, bool)> {
        let coord = self.notes_on()?;
        let n = self.stored_note(id).await?;
        // Refusals name the note by number, never by its text.
        if !actor.human {
            return Err(Error::Policy(
                "only a person's token (\"kind\": \"human\" in the tokens file) may endorse a \
                 note; an agent's never can"
                    .into(),
            ));
        }
        if !actor.may_review {
            return Err(Error::Policy(
                "this token may not review (\"review\": false), so it may not endorse".into(),
            ));
        }
        if n.principal == actor.name {
            return Err(Error::Policy(format!(
                "note {id} is yours; a note's writer cannot endorse it"
            )));
        }
        let now = unix_now();
        if n.state != NoteState::Open || n.expires.is_some_and(|e| e <= now) {
            let state = note_entry(n, now).state;
            return Err(Error::Conflict(format!(
                "note {id} is {state}, not open; only an open note can be endorsed"
            )));
        }
        if n.anchor_type != AnchorType::Path {
            return Err(Error::Invalid(format!(
                "note {id} is on a changeset; only a note on a path can be endorsed, since \
                 owners are per path"
            )));
        }
        let default_ref = self.default_ref().to_string();
        let head = self
            .meta()
            .get_ref(&default_ref)
            .await?
            .ok_or_else(|| Error::Conflict(format!("ref {default_ref:?} does not exist")))?;
        if !self.owns(head, &n, actor.name).await? {
            return Err(Error::Policy(format!(
                "{} is not named by OWNERS for note {id}'s path at {default_ref}; only an owner \
                 may endorse it",
                actor.name
            )));
        }
        // Bounded like a read, and never a 500: a tree that cannot be read
        // in time leaves staleness unknown, which is refused below.
        let until = tokio::time::Instant::now() + SECTION_BOUND;
        let checked = tokio::time::timeout_at(until, async {
            let root = self.root_tree(head).await.ok()?;
            self.stale_of(Some(root), &n).await
        })
        .await
        .ok()
        .flatten();
        match checked {
            Some(false) => {}
            Some(true) => {
                return Err(Error::Conflict(format!(
                    "note {id} is stale: its path changed at {default_ref} since it was \
                     written; ask for a new note instead"
                )))
            }
            None => {
                return Err(Error::Conflict(format!(
                    "could not tell whether note {id} is stale at {default_ref}; not endorsed"
                )))
            }
        }
        let fresh = |n: Note| {
            ViewedNote {
                note: n,
                stale: Some(false),
            }
            .entry(now)
        };
        // Endorsing an endorsed note renews its life (and names the newest
        // endorser).
        let by = actor.name.to_string();
        if coord
            .change_note(id, &NoteChange::Endorse { by: by.clone() }, now)
            .await?
        {
            let mut n = n;
            n.endorsed_by = Some(by);
            n.endorsed_at = Some(now);
            n.expires = Some(now + leep_core::coord::ENDORSED_NOTE_TTL_SECS);
            return Ok((fresh(n), true));
        }
        // A concurrent change closed it first.
        let n = self.stored_note(id).await?;
        Err(Error::Conflict(format!(
            "note {id} changed while it was endorsed; it is {}",
            n.state.as_str()
        )))
    }

    /// Retract `principal`'s note `id`. Another principal's is refused with
    /// `Error::Policy` (the server answers 403); a closed one is answered
    /// as it is.
    pub async fn retract_note(&self, id: u64, principal: &str) -> Result<NoteEntry> {
        let coord = self.notes_on()?;
        let n = coord
            .get_note(id)
            .await?
            .ok_or_else(|| Error::NotFound(format!("note {id}")))?;
        if n.principal != principal {
            return Err(Error::Policy(format!(
                "note {id} is another principal's; only its principal may retract it"
            )));
        }
        let by = principal.to_string();
        let now = unix_now();
        if coord
            .change_note(id, &NoteChange::Retract { by }, now)
            .await?
        {
            // What the conditional update wrote, without reading it back.
            let mut n = n;
            n.state = NoteState::Retracted;
            n.expires = leep_core::coord::closed_expiry(n.expires, now);
            return Ok(note_entry(n, now));
        }
        // Closed already (or by a concurrent change): as it is now.
        self.note(id).await
    }

    /// `actor` reports note `id`: an open agent note nobody endorsed is
    /// hidden at once (`reported`, with the reporter and time recorded), as
    /// is a human or endorsed one when `actor` is a human owner of its path
    /// (as for endorsing); any other report is only counted. A reporter that hid
    /// [`MAX_REPORTS_PER_HOUR`] notes in the last hour is refused. Every
    /// report is logged with its reporter (never the note's text), so an
    /// operator can find and undo one token's reports
    /// (`leep-server notes unreport`).
    pub async fn report_note(&self, id: u64, actor: &NoteActor<'_>) -> Result<ReportNoteResponse> {
        let coord = self.notes_on()?;
        let reporter = actor.name;
        let n = self.stored_note(id).await?;
        // A human or endorsed note is hidden only by a human owner of its
        // path; ownership is read only when it could matter.
        let shielded = n.trust == NoteTrust::Human || n.endorsed_by.is_some();
        let owner = shielded
            && n.state == NoteState::Open
            && self.human_owner(&n, actor).await.unwrap_or(false);
        let change = NoteChange::Report {
            by: reporter.to_string(),
            max_per_hour: MAX_REPORTS_PER_HOUR,
            owner,
        };
        let hidden = match coord.change_note(id, &change, unix_now()).await {
            Ok(h) => h,
            Err(e) => {
                if matches!(&e, Error::Invalid(m) if m.contains(REPORTS_PER_HOUR_MARKER)) {
                    metrics::counter!("leep_notes_refused_total", "reason" => "report_rate")
                        .increment(1);
                    tracing::warn!(note = id, reporter, "note report refused: hourly cap");
                }
                return Err(e);
            }
        };
        let effect = if hidden { "hidden" } else { "counted" };
        tracing::info!(note = id, reporter, effect, "note reported");
        metrics::counter!("leep_notes_reported_total", "effect" => effect).increment(1);
        Ok(ReportNoteResponse {
            note: self.note(id).await?,
            hidden,
        })
    }

    /// An operator's undo of reports: open again note `id`, or every note
    /// `reporter` hid. Answers how many.
    pub async fn unreport_notes(&self, id: Option<u64>, reporter: Option<&str>) -> Result<u64> {
        self.notes_on()?.unreport_notes(id, reporter).await
    }

    /// An operator's undo of endorsements: note `id`, or every note
    /// `endorser` endorsed (a person's token removed, re-kinded, leaked or
    /// dropped from `OWNERS`). Answers how many.
    pub async fn unendorse_notes(&self, id: Option<u64>, endorser: Option<&str>) -> Result<u64> {
        self.notes_on()?.unendorse_notes(id, endorser).await
    }

    /// An operator's mass revoke: retract every open note of `principal`.
    pub async fn revoke_notes(&self, principal: &str) -> Result<u64> {
        self.notes_on()?.revoke_notes(principal, unix_now()).await
    }

    /// Delete notes past their life, and closed ones 30 days on, in bounded
    /// steps, at most `steps` of them.
    pub async fn sweep_notes(&self, steps: usize) -> Result<NoteSweep> {
        let Some(coord) = self.coord.as_ref() else {
            return Ok(NoteSweep::default());
        };
        let now = unix_now();
        let mut total = NoteSweep::default();
        for _ in 0..steps {
            let r = coord.sweep_notes(now, NOTE_SWEEP_STEP).await?;
            total.expired += r.expired;
            total.closed += r.closed;
            if !r.full(NOTE_SWEEP_STEP) {
                break;
            }
        }
        for (reason, n) in [("expired", total.expired), ("closed", total.closed)] {
            if n > 0 {
                metrics::counter!("leep_notes_swept_total", "reason" => reason).increment(n);
            }
        }
        Ok(total)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use bytes::Bytes;
    use leep_core::api::{DeclareSessionRequest, NoteAnchor};
    use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
    use leep_core::{Changeset, ChangesetId, FileChange, FileKind, RepoPath};

    use super::*;

    fn repo() -> Repo {
        Repo::new(
            Arc::new(MemoryBlobStore::new()),
            Arc::new(MemoryMetaStore::new()),
        )
    }

    /// A changeset writing `files` on `parents`, landed on `to`.
    async fn commit(
        r: &Repo,
        parents: &[ChangesetId],
        files: &[(&str, &str)],
        to: &str,
    ) -> ChangesetId {
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
            parents: parents.to_vec(),
            author: "ada".into(),
            timestamp: 1_700_000_000,
            message: "m".into(),
            changes,
            extra: Default::default(),
        };
        let id = r.create_changeset(cs).await.unwrap();
        let old = r.meta().get_ref(to).await.unwrap();
        assert!(r.meta().cas_ref(to, old, Some(id)).await.unwrap());
        id
    }

    fn req(anchor: &str, title: &str) -> AddNoteRequest {
        AddNoteRequest {
            kind: "gotcha".into(),
            anchor: NoteAnchor {
                kind: "path".into(),
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

    /// A path note is resolved at the default ref (or `at`, or the
    /// session's target ref) and records the file's content there; a path
    /// that does not exist is refused, as are lines on a directory.
    #[tokio::test]
    async fn path_notes_resolve_their_anchor() {
        let r = repo();
        let main = commit(&r, &[], &[("src/auth/token.rs", "v1\n")], "main").await;
        let side = commit(&r, &[main], &[("src/auth/token.rs", "v2\n")], "review/x").await;
        let mut q = req("./src/auth/token.rs", "Refresh races the cache");
        q.lines = Some([3, 4]);
        q.body = "Take the lock first.".into();
        let e = r
            .add_note("ada", NoteTrust::Agent, None, q.clone())
            .await
            .unwrap();
        assert_eq!(
            (e.state.as_str(), e.trust.as_str(), e.kind.as_str()),
            ("open", "agent", "gotcha")
        );
        assert_eq!(e.untrusted.anchor, "src/auth/token.rs");
        assert_eq!((e.at, e.lines), (Some(main), Some([3, 4])));
        assert_eq!(
            e.bytes,
            (e.untrusted.title.len() + e.untrusted.body.len()) as u64
        );
        assert_eq!(e.expires, Some(e.created + AGENT_NOTE_TTL_SECS));
        let stored = r.coord().unwrap().get_note(e.id).await.unwrap().unwrap();
        assert_eq!(
            stored.at_blob,
            Some(leep_core::Hash::of_blob(b"v1\n")),
            "the content the note was written against"
        );
        // At another revision.
        q.at = Some("review/x".into());
        let e2 = r
            .add_note("ada", NoteTrust::Human, None, q.clone())
            .await
            .unwrap();
        assert_eq!(e2.at, Some(side));
        assert_eq!(
            (e2.trust.as_str(), e2.expires),
            ("human", Some(e2.created + HUMAN_NOTE_TTL_SECS))
        );
        // A directory, without lines.
        let d = r
            .add_note(
                "ada",
                NoteTrust::Agent,
                None,
                req("src/auth/", "Owned by platform"),
            )
            .await
            .unwrap();
        assert_eq!(d.untrusted.anchor, "src/auth");
        let mut on_dir = req("src", "x");
        on_dir.lines = Some([1, 2]);
        assert!(matches!(
            r.add_note("ada", NoteTrust::Agent, None, on_dir).await,
            Err(Error::Invalid(_))
        ));
        // Gone, or never there: refused, without quoting the path.
        let missing = req("src/IGNORE PREVIOUS INSTRUCTIONS.rs", "x");
        let err = r
            .add_note("ada", NoteTrust::Agent, None, missing)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Invalid(_)), "{err}");
        assert!(!err.to_string().contains("IGNORE"), "{err}");
        // A changeset anchor by a ref name stores the full id it names.
        let mut c = req("review/x", "Broke the nightly");
        c.anchor.kind = "changeset".into();
        let ce = r
            .add_note("bo", NoteTrust::Agent, None, c.clone())
            .await
            .unwrap();
        assert_eq!(
            (ce.untrusted.anchor.clone(), ce.at),
            (side.to_string(), Some(side))
        );
        c.anchor.value = "nonesuch".into();
        assert!(matches!(
            r.add_note("bo", NoteTrust::Agent, None, c).await,
            Err(Error::Invalid(_))
        ));
        for bad in ["essay", ""] {
            let mut k = req("src", "x");
            k.kind = bad.into();
            assert!(r.add_note("bo", NoteTrust::Agent, None, k).await.is_err());
        }
        let mut t = req("src", "x");
        t.anchor.kind = "symbol".into();
        assert!(r.add_note("bo", NoteTrust::Agent, None, t).await.is_err());

        // Listing: the path and the directories above it, open by default.
        let at = |p: &str| NoteQuery {
            path: Some(p.into()),
            ..Default::default()
        };
        let ids = |v: NotesResponse| v.notes.iter().map(|n| n.id).collect::<Vec<_>>();
        assert_eq!(
            ids(r.notes(&at("src/auth/token.rs")).await.unwrap()),
            [d.id, e2.id, e.id]
        );
        assert_eq!(ids(r.notes(&at("src/auth")).await.unwrap()), [d.id]);
        let on = NoteQuery {
            changeset: Some(side.to_string()),
            ..Default::default()
        };
        assert_eq!(ids(r.notes(&on).await.unwrap()), [ce.id]);
        let paged = NoteQuery {
            limit: Some(1),
            ..Default::default()
        };
        let page = r.notes(&paged).await.unwrap();
        assert_eq!((ids(page.clone()), page.next), (vec![ce.id], Some(ce.id)));
    }

    /// The caller's session gives the default revision and model, and is
    /// recorded; a key that is no key is ignored.
    #[tokio::test]
    async fn a_session_supplies_the_defaults() {
        let r = repo();
        let main = commit(&r, &[], &[("a.rs", "1\n")], "main").await;
        let side = commit(&r, &[main], &[("b.rs", "2\n")], "review/b").await;
        let decl = DeclareSessionRequest {
            session: "s-1".into(),
            title: Some("work".into()),
            target_ref: Some("review/b".into()),
            agent_model: Some("model-a".into()),
            ..Default::default()
        };
        let s = r.declare_session("ada", decl).await.unwrap().session;
        let e = r
            .add_note(
                "ada",
                NoteTrust::Agent,
                Some("s-1"),
                req("b.rs", "only on review/b"),
            )
            .await
            .unwrap();
        assert_eq!(
            (e.session, e.at, e.untrusted.agent_model.as_deref()),
            (Some(s.id), Some(side), Some("model-a"))
        );
        // Another principal's key is not this caller's session.
        assert!(
            r.add_note("bo", NoteTrust::Agent, Some("s-1"), req("b.rs", "x"))
                .await
                .is_err(),
            "b.rs is not on main"
        );
        let e = r
            .add_note("bo", NoteTrust::Agent, Some("bad key\n"), req("a.rs", "x"))
            .await
            .unwrap();
        assert_eq!((e.session, e.at), (None, Some(main)));
    }

    /// Only the author retracts; a report hides an agent note and only
    /// counts a human one; expired notes are not served as open; the sweep
    /// deletes what is due.
    #[tokio::test]
    async fn retract_report_expire_and_sweep() {
        let r = repo();
        commit(&r, &[], &[("a.rs", "1\n")], "main").await;
        let a = r
            .add_note("ada", NoteTrust::Agent, None, req("a.rs", "agent"))
            .await
            .unwrap();
        let h = r
            .add_note("hugo", NoteTrust::Human, None, req("a.rs", "human"))
            .await
            .unwrap();
        assert!(matches!(
            r.retract_note(a.id, "bo").await,
            Err(Error::Policy(_))
        ));
        let rep = r.report_note(h.id, &NoteActor::agent("bo")).await.unwrap();
        assert!(!rep.hidden && rep.note.state == "open");
        let rep = r.report_note(a.id, &NoteActor::agent("bo")).await.unwrap();
        assert!(rep.hidden && rep.note.state == "reported");
        assert_eq!(rep.note.reported_by.as_deref(), Some("bo"));
        assert!(r
            .report_note(a.id, &NoteActor::agent("bo"))
            .await
            .is_ok_and(|r| !r.hidden));
        assert!(matches!(
            r.report_note(999, &NoteActor::agent("bo")).await,
            Err(Error::NotFound(_))
        ));
        let open = r.notes(&NoteQuery::default()).await.unwrap();
        assert_eq!(open.notes.len(), 1);
        let all = NoteQuery {
            all: true,
            ..Default::default()
        };
        assert_eq!(r.notes(&all).await.unwrap().notes.len(), 2);
        let b = r
            .add_note("bo", NoteTrust::Agent, None, req("a.rs", "mine"))
            .await
            .unwrap();
        let gone = r.retract_note(b.id, "bo").await.unwrap();
        assert_eq!(gone.state, "retracted");
        // Retracting again answers it as it is.
        assert_eq!(r.retract_note(b.id, "bo").await.unwrap().state, "retracted");
        // An open note past its life shows as expired and is not served
        // as open.
        let c = r.coord().unwrap().clone();
        let mut old = leep_core::coord::NewNote {
            kind: NoteKind::Finding,
            anchor_type: AnchorType::Path,
            anchor: "a.rs".into(),
            at_changeset: None,
            at_blob: None,
            lines: None,
            title: "old".into(),
            body: String::new(),
            principal: "cy".into(),
            agent_model: None,
            session_id: None,
            trust: NoteTrust::Agent,
            supersedes: None,
            created: 1,
            expires: Some(2),
            max_open: 10,
            max_per_hour: 10,
            max_per_day: 10,
        };
        let stale = c.add_note(&old).await.unwrap();
        assert_eq!(r.note(stale.id).await.unwrap().state, "expired");
        assert!(!r
            .notes(&NoteQuery::default())
            .await
            .unwrap()
            .notes
            .iter()
            .any(|n| n.id == stale.id));
        old.expires = None;
        let kept = c.add_note(&old).await.unwrap();
        let swept = r.sweep_notes(4).await.unwrap();
        assert_eq!((swept.expired, swept.closed), (1, 0));
        assert!(matches!(r.note(stale.id).await, Err(Error::NotFound(_))));
        assert!(r.note(kept.id).await.is_ok());
        // An operator revokes every open note of a principal.
        assert_eq!(r.revoke_notes("hugo").await.unwrap(), 1);
        assert_eq!(r.note(h.id).await.unwrap().state, "retracted");
    }

    /// A listing's path is bounded like a stored one, so a long path cannot
    /// make the store bind one value per ancestor (a 64 KiB `a/a/...` would
    /// be 32,768 of them).
    #[tokio::test]
    async fn a_listing_path_is_bounded() {
        let r = repo();
        let long = "a/".repeat(32 * 1024) + "b";
        let q = NoteQuery {
            path: Some(long),
            ..Default::default()
        };
        assert!(matches!(r.notes(&q).await, Err(Error::Invalid(_))));
        let at_limit = format!("a/{}", "b".repeat(leep_core::coord::MAX_PATH_BYTES - 2));
        let q = NoteQuery {
            path: Some(at_limit),
            ..Default::default()
        };
        assert!(r.notes(&q).await.is_ok());
        // The store refuses too, whoever calls it.
        let f = leep_core::coord::NoteFilter {
            path: Some("a/".repeat(200)),
            limit: 1,
            ..Default::default()
        };
        assert!(r.coord().unwrap().list_notes(&f).await.is_err());
        let many: Vec<String> = (0..10_000).map(|i| format!("p{i}")).collect();
        assert!(r.coord().unwrap().notes_at(&many, 10).await.is_err());
    }

    #[tokio::test]
    async fn without_a_coord_store_there_are_no_notes() {
        let r = repo().with_coord(None);
        let e = r
            .add_note("ada", NoteTrust::Agent, None, req("a", "x"))
            .await
            .unwrap_err();
        assert!(matches!(e, Error::NotFound(_)), "{e}");
        assert!(r.notes(&NoteQuery::default()).await.is_err());
        assert_eq!(r.sweep_notes(1).await.unwrap(), NoteSweep::default());
    }
}
