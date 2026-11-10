//! Notes as readers see them (ADR 0098, second change): whether each is
//! stale, whether its endorsement holds, and how they rank.
//!
//! One function, [`Repo::notes_for`], answers both `GET /v1/notes` and the
//! context pack's `notes` section, so the two never disagree about a note.
//! Staleness is a tree lookup at the ref's head compared with the content
//! the note was written against (`at_blob`), not a last-change read: one
//! path walk per note through the node cache, and no index. It is bounded
//! (a fixed number of notes checked, a few at once, by a deadline) and it
//! never fails the call: a tree that cannot be read, a missing
//! `at_changeset` (it is no GC root) or a check past the deadline leaves
//! the note's staleness unknown.

use std::cmp::Reverse;

use futures_util::StreamExt;
use leep_core::api::{ContextNote, ContextNoteText, NoteEntry, NoteText};
use leep_core::coord::{
    ancestors, AnchorType, Note, NoteFilter, NoteState, NoteTrust, MAX_NOTE_ANCHORS,
};
use leep_core::{ChangesetId, Hash, Result, TreeEntry};
use tokio::time::Instant;

use crate::notices::cut;
use crate::Repo;

/// Pack files whose notes (and their directories' notes) a pack reads.
pub const PACK_NOTE_FILES: usize = 32;
/// Most notes one pack lookup reads.
pub const PACK_NOTE_ROWS: usize = 256;
/// Most notes whose staleness one pack checks; the rest stay unknown.
pub const PACK_NOTES_CHECKED: usize = 32;
/// Most notes whose staleness one listing page checks (a page's largest).
pub const LIST_NOTES_CHECKED: usize = 100;
/// Staleness lookups in flight at once.
const STALE_CONCURRENCY: usize = 8;
/// An agent's title (or a stale note's) as a pack repeats it.
pub const PACK_NOTE_TITLE_BYTES: usize = 80;
/// An endorsed or human note's body as a pack repeats it.
pub const PACK_NOTE_BODY_BYTES: usize = 1024;
/// Pack files a directory note names.
pub const PACK_NOTE_PATHS: usize = 16;

/// Which notes [`Repo::notes_for`] reads.
#[derive(Clone, Copy, Debug)]
pub enum NoteSelect<'a> {
    /// Open path notes on these files and on the directories above them
    /// (the pack's lookup, one exact probe of the anchor index), ranked.
    Paths(&'a [String]),
    /// A listing's filter (`GET /v1/notes`), in its order: newest first.
    Filter(&'a NoteFilter),
}

/// How much staleness checking one call may do.
#[derive(Clone, Copy, Debug)]
pub struct NoteBudget {
    /// Notes checked, the first ones in the answer's order.
    pub checked: usize,
    /// Checks still running then are given up: unknown.
    pub until: Instant,
}

/// A note with what a reader is told about it now.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ViewedNote {
    pub note: Note,
    /// `Some(true)` when its anchor changed since it was written, `None`
    /// when unknown.
    pub stale: Option<bool>,
}

impl ViewedNote {
    /// Whether its endorsement holds: open, endorsed, and checked fresh.
    /// A stale note is never served as endorsed, so an endorsement cannot
    /// outlive the content it covered.
    pub fn endorsed(&self) -> bool {
        self.note.state == NoteState::Open
            && self.note.endorsed_by.is_some()
            && self.stale == Some(false)
    }

    /// `endorsed`, `human` or `agent`: what ranks it, and the label of
    /// `leep_context_notes_total`.
    pub fn tier(&self) -> &'static str {
        if self.endorsed() {
            "endorsed"
        } else {
            self.note.trust.as_str()
        }
    }

    /// The wire form at `now` (an open note past its life shows as
    /// `expired`).
    pub fn entry(self, now: i64) -> NoteEntry {
        let endorsed = self.endorsed();
        let n = self.note;
        let state = if n.state == NoteState::Open && n.expires.is_some_and(|e| e <= now) {
            "expired"
        } else {
            n.state.as_str()
        };
        NoteEntry {
            id: n.id,
            kind: n.kind.as_str().to_string(),
            trust: n.trust.as_str().to_string(),
            endorsed_by: n.endorsed_by,
            endorsed_at: n.endorsed_at,
            endorsed,
            stale: self.stale,
            state: state.to_string(),
            principal: n.principal,
            anchor_type: n.anchor_type.as_str().to_string(),
            at: n.at_changeset,
            lines: n.lines.map(|(a, b)| [a, b]),
            session: n.session_id,
            created: n.created,
            expires: n.expires,
            superseded_by: n.superseded_by,
            reported_by: n.reported_by,
            bytes: (n.title.len() + n.body.len()) as u64,
            untrusted: NoteText {
                title: n.title,
                body: n.body,
                anchor: n.anchor,
                agent_model: n.agent_model,
            },
        }
    }
}

/// Components of an anchor: a deeper anchor is nearer the file.
fn depth(anchor: &str) -> usize {
    anchor.split('/').count()
}

/// The rank of a viewed note, as a total order (lower first): endorsed,
/// then human, then agent; fresh, then unknown, then stale; the nearest
/// anchor (a file before its directory); the newest.
pub fn rank_key(v: &ViewedNote) -> (u8, u8, Reverse<usize>, Reverse<u64>) {
    let tier = match v.tier() {
        "endorsed" => 0,
        "human" => 1,
        _ => 2,
    };
    let fresh = match v.stale {
        Some(false) => 0,
        None => 1,
        Some(true) => 2,
    };
    (
        tier,
        fresh,
        Reverse(depth(&v.note.anchor)),
        Reverse(v.note.id),
    )
}

/// The order staleness is checked in before it is known: the rank with
/// every note taken as fresh and its stored endorsement as holding.
fn provisional_key(n: &Note) -> (u8, Reverse<usize>, Reverse<u64>) {
    let tier = match (&n.endorsed_by, n.trust) {
        (Some(_), _) => 0,
        (None, NoteTrust::Human) => 1,
        (None, NoteTrust::Agent) => 2,
    };
    (tier, Reverse(depth(&n.anchor)), Reverse(n.id))
}

/// The anchors a pack probes: each of `paths` (at most
/// [`PACK_NOTE_FILES`]) and the directories above it, once each, in order,
/// at most [`MAX_NOTE_ANCHORS`].
fn pack_anchors(paths: &[String]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    for p in paths.iter().take(PACK_NOTE_FILES) {
        for a in std::iter::once(p.as_str()).chain(ancestors(p).into_iter().rev()) {
            if out.len() >= MAX_NOTE_ANCHORS {
                return out;
            }
            if seen.insert(a.to_string()) {
                out.push(a.to_string());
            }
        }
    }
    out
}

impl Repo {
    /// The notes `select` names, each with its staleness at `head` (unknown
    /// without one): for [`NoteSelect::Paths`] open, unexpired path notes in
    /// rank order ([`rank_key`]); for [`NoteSelect::Filter`] the listing in
    /// its own order. The first `budget.checked` are checked, in that order
    /// (for paths, the order before staleness is known), 8 at once, until
    /// `budget.until`. Only the store's read can fail the call; staleness
    /// never does.
    pub async fn notes_for(
        &self,
        select: NoteSelect<'_>,
        head: Option<ChangesetId>,
        budget: NoteBudget,
        now: i64,
    ) -> Result<Vec<ViewedNote>> {
        let coord = self.coord.as_ref().ok_or_else(leep_core::coord::no_notes)?;
        let mut rows = match select {
            NoteSelect::Paths(paths) => {
                let anchors = pack_anchors(paths);
                if anchors.is_empty() {
                    return Ok(Vec::new());
                }
                let mut rows: Vec<Note> = coord
                    .notes_at(&anchors, PACK_NOTE_ROWS)
                    .await?
                    .into_iter()
                    .filter(|n| {
                        n.anchor_type == AnchorType::Path
                            && n.state == NoteState::Open
                            && n.expires.is_none_or(|e| e > now)
                    })
                    .collect();
                rows.sort_by_key(provisional_key);
                rows
            }
            NoteSelect::Filter(f) => coord.list_notes(f).await?,
        };
        let stale = self.staleness(&rows, head, budget).await;
        let mut out: Vec<ViewedNote> = rows
            .drain(..)
            .zip(stale)
            .map(|(note, stale)| ViewedNote { note, stale })
            .collect();
        if matches!(select, NoteSelect::Paths(_)) {
            out.sort_by_key(rank_key);
        }
        Ok(out)
    }

    /// The staleness of each of `notes` at `head`: the first
    /// `budget.checked` looked up, the rest unknown.
    pub(crate) async fn staleness(
        &self,
        notes: &[Note],
        head: Option<ChangesetId>,
        budget: NoteBudget,
    ) -> Vec<Option<bool>> {
        let root = match head {
            Some(h) => tokio::time::timeout_at(budget.until, self.root_tree(h))
                .await
                .ok()
                .and_then(|r| r.ok()),
            None => None,
        };
        let checked = notes.len().min(budget.checked);
        // Owned items, so the futures name no borrowed lifetime of their
        // own (they must be `Send` for any lifetime in a handler).
        let mut out: Vec<Option<bool>> = futures_util::stream::iter(notes[..checked].to_vec())
            .map(|n| async move {
                tokio::time::timeout_at(budget.until, self.stale_of(root, &n))
                    .await
                    .ok()
                    .flatten()
            })
            .buffered(STALE_CONCURRENCY)
            .collect()
            .await;
        // Changeset anchors are never stale by content, checked or not.
        out.extend(
            notes[checked..]
                .iter()
                .map(|n| (n.anchor_type == AnchorType::Changeset).then_some(false)),
        );
        out
    }

    /// Whether `n` is stale in the tree `root`: for a file note, whether
    /// the file there is not the content it was written against (gone, or
    /// no longer a file, is stale); for a directory note, whether the
    /// directory is gone. A path note without `at_blob` is told apart by
    /// its `at_changeset`; when that is missing (it is no GC root), or a
    /// tree cannot be read, the answer is unknown, never an error.
    pub(crate) async fn stale_of(&self, root: Option<Hash>, n: &Note) -> Option<bool> {
        if n.anchor_type == AnchorType::Changeset {
            return Some(false);
        }
        let now = self.entry_at(root?, &n.anchor).await.ok()?;
        if let Some(blob) = n.at_blob {
            return Some(!matches!(now, Some(TreeEntry::File { content, .. }) if content == blob));
        }
        if matches!(now, Some(TreeEntry::Dir { .. })) && n.lines.is_none() {
            return Some(false);
        }
        // A directory gone (or now a file), or a file note whose content
        // was not recorded: what was there when it was written decides.
        let then_root = self.root_tree(n.at_changeset?).await.ok()?;
        match self.entry_at(then_root, &n.anchor).await.ok()? {
            Some(TreeEntry::Dir { .. }) => Some(!matches!(now, Some(TreeEntry::Dir { .. }))),
            Some(TreeEntry::File { content: was, .. }) => {
                Some(!matches!(now, Some(TreeEntry::File { content, .. }) if content == was))
            }
            None => None,
        }
    }
}

/// The pack's `notes` items from `viewed` (in rank order) for the pack's
/// `files`, and how many stale agent notes were left out. An endorsed
/// note, or a human one that is not stale, carries its title and its body
/// cut to 1 KiB; any other carries its title cut to 80 bytes and its id,
/// the rest one `GET /v1/notes/{id}` away. A note on a directory names the
/// pack files below it. One item per note.
pub fn pack_notes(viewed: Vec<ViewedNote>, files: &[String]) -> (Vec<ContextNote>, u64) {
    let mut suppressed = 0;
    let mut out = Vec::new();
    for v in viewed {
        if v.stale == Some(true) && v.note.trust == NoteTrust::Agent {
            suppressed += 1;
            continue;
        }
        let endorsed = v.endorsed();
        let full = endorsed || (v.note.trust == NoteTrust::Human && v.stale != Some(true));
        let n = v.note;
        let (title, body) = if full {
            (n.title.clone(), cut(&n.body, PACK_NOTE_BODY_BYTES))
        } else {
            (cut(&n.title, PACK_NOTE_TITLE_BYTES), String::new())
        };
        let more = title.len() < n.title.len() || body.len() < n.body.len();
        let under = format!("{}/", n.anchor);
        let paths: Vec<String> = files
            .iter()
            .filter(|f| f.starts_with(&under))
            .take(PACK_NOTE_PATHS)
            .cloned()
            .collect();
        out.push(crate::context::sized(
            ContextNote {
                id: format!("note:{}", n.id),
                bytes: 0,
                note: n.id,
                kind: n.kind.as_str().to_string(),
                trust: n.trust.as_str().to_string(),
                endorsed_by: if endorsed { n.endorsed_by } else { None },
                stale: v.stale,
                principal: n.principal,
                created: n.created,
                more,
                untrusted: ContextNoteText {
                    anchor: n.anchor,
                    title,
                    body,
                    paths,
                },
            },
            |i, b| i.bytes = b,
        ));
    }
    (out, suppressed)
}
