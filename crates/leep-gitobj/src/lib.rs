//! Git object encoding for Leep: the pure mapping between changesets and
//! git commits, Leep trees and git trees, and blobs and their git ids.
//!
//! Everything here is a function of its inputs, with no repository or store
//! access, so the importer, the exporter, the live mirror and the server's
//! derived git maps (ADR 0090) all give one changeset one commit id. The
//! encoders are gix's own (`gix-object`, the versions `leep-git`'s gix
//! uses), so what this crate computes is byte for byte what git writes.

use std::collections::BTreeMap;

use leep_core::{Changeset, ChangesetId, Error, FileChange, FileKind, Hash, RepoPath, Result};

pub use gix_hash::ObjectId;
pub use gix_object::tree::{Entry, EntryKind, EntryMode};

/// `extra` key holding the original commit sha.
pub const EXTRA_GIT_COMMIT: &str = "git.commit";
/// `extra` key holding `Name <email> <unix-time>` of the committer.
pub const EXTRA_GIT_COMMITTER: &str = "git.committer";
/// `extra` keys holding the `+HHMM` timezone offsets, needed to reproduce the
/// exact commit bytes.
pub const EXTRA_GIT_AUTHOR_TZ: &str = "git.author_tz";
pub const EXTRA_GIT_COMMITTER_TZ: &str = "git.committer_tz";
/// `extra` key holding the commit's `encoding` header, if any.
pub const EXTRA_GIT_ENCODING: &str = "git.encoding";
/// `extra` key holding the remaining commit headers (`gpgsig`, `mergetag`,
/// ...) in their original order, as a JSON array of `[name, value]` pairs.
/// Without them signed commits (e.g. every commit made on github.com) would
/// not round-trip to the same sha.
pub const EXTRA_GIT_HEADERS: &str = "git.extra_headers";
/// `extra` key holding the original bytes of the commit fields that are not
/// valid UTF-8, as a JSON object of base64 strings (`author`, `committer`,
/// `message`, `encoding`, `headers`). A changeset stores text, so the fields
/// it holds are lossy for such a commit; this restores them on export. Absent
/// for the usual UTF-8 commit, so its changeset id does not change.
pub const EXTRA_GIT_RAW: &str = "git.raw";

/// Trailer key the mirror uses to name the changeset a commit came from.
pub const CHANGESET_TRAILER: &str = "Leep-Changeset";

/// `-0000` means UTC with an unknown local offset. It is the same instant as
/// `+0000` but different bytes, so it must survive as its own token.
pub const NEGATIVE_ZERO_TZ: &str = "-0000";

fn gerr(e: impl std::fmt::Display) -> Error {
    Error::Other(format!("git: {e}"))
}

/// Options that change the commit bytes of changesets made in Leep. They
/// must not change for the life of a store, or the same changeset would get
/// a second commit id (ADR 0090 records them once per store).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CommitStyle {
    /// Append `agent.*` extras as `Leep-Agent-<Key>: value` trailers.
    pub provenance_trailers: bool,
    /// Name the changeset in a `Leep-Changeset:` trailer on every commit
    /// that is not the git commit the changeset was imported from.
    pub changeset_trailer: bool,
}

impl Default for CommitStyle {
    /// The live mirror's defaults.
    fn default() -> Self {
        CommitStyle {
            provenance_trailers: false,
            changeset_trailer: true,
        }
    }
}

impl CommitStyle {
    /// The stored form: `plain` or `trailer`, with `+provenance` when
    /// provenance trailers are on.
    pub fn encode(&self) -> String {
        let base = if self.changeset_trailer {
            "trailer"
        } else {
            "plain"
        };
        match self.provenance_trailers {
            true => format!("{base}+provenance"),
            false => base.to_string(),
        }
    }

    pub fn decode(s: &str) -> Result<CommitStyle> {
        let (base, provenance_trailers) = match s.strip_suffix("+provenance") {
            Some(b) => (b, true),
            None => (s, false),
        };
        let changeset_trailer = match base {
            "trailer" => true,
            "plain" => false,
            _ => return Err(Error::Invalid(format!("unknown git commit style {s:?}"))),
        };
        Ok(CommitStyle {
            provenance_trailers,
            changeset_trailer,
        })
    }
}

/// Parse `Name <email>`; falls back to the whole input as the name.
pub fn parse_ident(s: &[u8]) -> (Vec<u8>, Vec<u8>) {
    match (s.iter().rposition(|&b| b == b'<'), s.ends_with(b">")) {
        (Some(i), true) => (
            s[..i].trim_ascii_end().to_vec(),
            s[i + 1..s.len() - 1].to_vec(),
        ),
        _ => (s.to_vec(), Vec::new()),
    }
}

/// The `+HHMM` token to store for a signature whose raw `<secs> <tz>` text is
/// `raw_time`, whose decoded offset is `offset`.
pub fn tz_token(raw_time: &str, offset: i32) -> String {
    match raw_time.rsplit_once(' ') {
        Some((_, NEGATIVE_ZERO_TZ)) => NEGATIVE_ZERO_TZ.to_string(),
        _ => format_tz(offset),
    }
}

/// `+HHMM` for an offset in seconds.
pub fn format_tz(offset: i32) -> String {
    let sign = if offset < 0 { '-' } else { '+' };
    let a = offset.unsigned_abs();
    format!("{sign}{:02}{:02}", a / 3600, a % 3600 / 60)
}

/// Offset in seconds for `+HHMM`; 0 if malformed.
pub fn parse_tz(s: &str) -> i32 {
    let b = s.as_bytes();
    if b.len() != 5 || !(b[0] == b'+' || b[0] == b'-') {
        return 0;
    }
    let (Ok(h), Ok(m)) = (s[1..3].parse::<i32>(), s[3..5].parse::<i32>()) else {
        return 0;
    };
    let v = h * 3600 + m * 60;
    if b[0] == b'-' {
        -v
    } else {
        v
    }
}

fn sig(ident: &[u8], seconds: i64, tz: &str) -> gix_actor::Signature {
    let (name, email) = parse_ident(ident);
    gix_actor::Signature {
        name: name.into(),
        email: email.into(),
        time: gix_date::Time::new(seconds, parse_tz(tz)),
    }
}

/// The original bytes of commit fields that were not UTF-8 (`git.raw`).
/// A field counts only while its lossy text still equals what the changeset
/// holds, so an edited message or author is never overwritten by stale bytes.
type RawHeaders = Vec<(Vec<u8>, Vec<u8>)>;

struct RawFields(BTreeMap<String, serde_json::Value>);

impl RawFields {
    fn of(cs: &Changeset) -> Result<Self> {
        match cs.extra.get(EXTRA_GIT_RAW) {
            Some(json) => Ok(Self(serde_json::from_str(json).map_err(gerr)?)),
            None => Ok(Self(BTreeMap::new())),
        }
    }

    fn decode(v: &serde_json::Value) -> Result<Vec<u8>> {
        use base64::Engine;
        let s = v.as_str().ok_or_else(|| gerr("git.raw: not a string"))?;
        base64::engine::general_purpose::STANDARD
            .decode(s)
            .map_err(gerr)
    }

    /// The bytes of `key` if they still read as `text`.
    fn bytes(&self, key: &str, text: &str) -> Result<Option<Vec<u8>>> {
        let Some(v) = self.0.get(key) else {
            return Ok(None);
        };
        let b = Self::decode(v)?;
        Ok((String::from_utf8_lossy(&b) == text).then_some(b))
    }

    fn headers(&self, text: &[(String, String)]) -> Result<Option<RawHeaders>> {
        let Some(v) = self.0.get("headers") else {
            return Ok(None);
        };
        let pairs: Vec<[serde_json::Value; 2]> = serde_json::from_value(v.clone()).map_err(gerr)?;
        let mut out = Vec::new();
        for [k, v] in &pairs {
            out.push((Self::decode(k)?, Self::decode(v)?));
        }
        let same = out.len() == text.len()
            && out.iter().zip(text).all(|((rk, rv), (k, v))| {
                String::from_utf8_lossy(rk) == *k && String::from_utf8_lossy(rv) == *v
            });
        Ok(same.then_some(out))
    }
}

/// Rewrite the time zone of the `author` or `committer` line of a serialized
/// commit to `-0000`, which gix cannot express (it writes `+0000` for offset
/// zero).
fn set_negative_zero(commit: &mut [u8], prefix: &[u8]) {
    let mut at = 0;
    while at < commit.len() {
        let end = commit[at..]
            .iter()
            .position(|&b| b == b'\n')
            .map_or(commit.len(), |i| at + i);
        let line = &commit[at..end];
        if line.is_empty() {
            return;
        }
        if line.starts_with(prefix) && line.len() >= NEGATIVE_ZERO_TZ.len() {
            commit[end - NEGATIVE_ZERO_TZ.len()..end].copy_from_slice(NEGATIVE_ZERO_TZ.as_bytes());
            return;
        }
        at = end + 1;
    }
}

/// `agent.session_id` -> `Leep-Agent-Session-Id`.
fn trailer_key(key: &str) -> String {
    let rest = key.strip_prefix("agent.").unwrap_or(key);
    let words: Vec<String> = rest
        .split(['.', '_', '-', ' '])
        .filter(|w| !w.is_empty())
        .map(|w| {
            let mut c = w.chars();
            c.next()
                .map(|f| f.to_uppercase().collect::<String>() + c.as_str())
                .unwrap_or_default()
        })
        .collect();
    format!("Leep-Agent-{}", words.join("-"))
}

/// `message` with a `Leep-Agent-*` trailer for every `agent.*` extra.
pub fn with_trailers(message: &str, extra: &BTreeMap<String, String>) -> String {
    let lines: Vec<String> = extra
        .iter()
        .filter(|(k, _)| k.starts_with("agent."))
        .map(|(k, v)| format!("{}: {}", trailer_key(k), v.replace(['\n', '\r'], " ")))
        .collect();
    if lines.is_empty() {
        return message.to_string();
    }
    let mut m = message.trim_end_matches('\n').to_string();
    // Trailers must form their own final paragraph.
    m.push_str("\n\n");
    m.push_str(&lines.join("\n"));
    m.push('\n');
    m
}

/// Append `Leep-Changeset: <id>` to the final trailer paragraph, or start one.
fn with_changeset_trailer(message: &str, id: ChangesetId) -> String {
    let body = message.trim_end_matches('\n');
    let last = body.rsplit("\n\n").next().unwrap_or("");
    let in_trailers = !body.is_empty()
        && body.contains("\n\n")
        && last.lines().all(|l| {
            l.split_once(": ")
                .is_some_and(|(k, _)| !k.is_empty() && !k.contains(' '))
        });
    let sep = if body.is_empty() {
        ""
    } else if in_trailers {
        "\n"
    } else {
        "\n\n"
    };
    format!("{body}{sep}{CHANGESET_TRAILER}: {id}\n")
}

/// The changeset a commit message names in its `Leep-Changeset:` trailer.
pub fn changeset_trailer(message: &str) -> Option<ChangesetId> {
    let body = message.trim_end_matches('\n');
    let last = body.rsplit("\n\n").next()?;
    last.lines().rev().find_map(|l| {
        let v = l.strip_prefix(CHANGESET_TRAILER)?.strip_prefix(": ")?;
        v.trim().parse().ok()
    })
}

/// The git commit for `cs`, given its tree and parent commits. Changesets
/// imported from git carry what it takes to reproduce the original bytes
/// (committer, time zones, encoding, extra headers). `changeset_trailer`
/// names the changeset in a `Leep-Changeset:` trailer, which the live mirror
/// adds to changesets that did not come from git (ADR 0046).
pub fn commit_object(
    cs: &Changeset,
    tree: ObjectId,
    parents: Vec<ObjectId>,
    provenance_trailers: bool,
    changeset_trailer: Option<ChangesetId>,
) -> Result<Vec<u8>> {
    let raw = RawFields::of(cs)?;
    let tz = |k: &str| cs.extra.get(k).map(String::as_str).unwrap_or("+0000");
    let (author_tz, committer_tz) = (tz(EXTRA_GIT_AUTHOR_TZ), tz(EXTRA_GIT_COMMITTER_TZ));
    let author_ident = raw
        .bytes("author", &cs.author)?
        .unwrap_or_else(|| cs.author.clone().into_bytes());
    let author = sig(&author_ident, cs.timestamp, author_tz);
    let committer = match cs.extra.get(EXTRA_GIT_COMMITTER) {
        Some(c) => {
            let (ident, secs) = match c.rsplit_once(' ') {
                Some((ident, t)) => (ident, t.parse().unwrap_or(cs.timestamp)),
                None => (c.as_str(), cs.timestamp),
            };
            let ident = raw
                .bytes("committer", ident)?
                .unwrap_or_else(|| ident.as_bytes().to_vec());
            sig(&ident, secs, committer_tz)
        }
        None => author.clone(),
    };
    let text_headers: Vec<(String, String)> = match cs.extra.get(EXTRA_GIT_HEADERS) {
        Some(json) => serde_json::from_str(json).map_err(gerr)?,
        None => Vec::new(),
    };
    let extra_headers: Vec<(gix_object::bstr::BString, gix_object::bstr::BString)> =
        match raw.headers(&text_headers)? {
            Some(pairs) => pairs
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
            None => text_headers
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
        };
    let mut message = if provenance_trailers {
        with_trailers(&cs.message, &cs.extra)
    } else {
        cs.message.clone()
    };
    if let Some(id) = changeset_trailer {
        message = with_changeset_trailer(&message, id);
    }
    // Trailers rewrite the message, so the original bytes apply only when
    // none were added.
    let message = match message == cs.message {
        true => raw.bytes("message", &cs.message)?,
        false => None,
    }
    .unwrap_or_else(|| message.into_bytes());
    let encoding = match cs.extra.get(EXTRA_GIT_ENCODING) {
        Some(e) => Some(
            raw.bytes("encoding", e)?
                .unwrap_or_else(|| e.clone().into_bytes()),
        ),
        None => None,
    };
    let commit = gix_object::Commit {
        tree,
        parents: parents.into(),
        author,
        committer,
        encoding: encoding.map(Into::into),
        message: message.into(),
        extra_headers,
    };
    let mut buf = Vec::new();
    gix_object::WriteTo::write_to(&commit, &mut buf).map_err(gerr)?;
    if author_tz == NEGATIVE_ZERO_TZ {
        set_negative_zero(&mut buf, b"author ");
    }
    if committer_tz == NEGATIVE_ZERO_TZ {
        set_negative_zero(&mut buf, b"committer ");
    }
    Ok(buf)
}

/// Is `commit` (serialized) the commit `cs` was imported from?
pub fn reproduces(cs: &Changeset, commit: &[u8]) -> Result<bool> {
    let Some(want) = cs.extra.get(EXTRA_GIT_COMMIT) else {
        return Ok(false);
    };
    let oid = object_id(gix_object::Kind::Commit, commit)?;
    Ok(want.parse::<ObjectId>().is_ok_and(|w| w == oid))
}

/// The commit a store writes for changeset `id` under `style`: the commit it
/// was imported from when the plain export reproduces it, and otherwise,
/// with `style.changeset_trailer`, one naming the changeset. This is the
/// live mirror's rule (ADR 0046, 0049), so the mirror, `leep-git export`
/// and the server's derived maps agree.
pub fn commit_for(
    cs: &Changeset,
    id: ChangesetId,
    tree: ObjectId,
    parents: Vec<ObjectId>,
    style: CommitStyle,
) -> Result<Vec<u8>> {
    let commit = commit_object(cs, tree, parents.clone(), style.provenance_trailers, None)?;
    // Only a changeset whose export is the commit it came from goes without
    // the trailer. One that names a `git.commit` it does not reproduce (the
    // server's rebased copy of an imported changeset, as a land branch
    // makes) is Leep's own and must say so.
    if style.changeset_trailer && !reproduces(cs, &commit)? {
        return commit_object(cs, tree, parents, style.provenance_trailers, Some(id));
    }
    Ok(commit)
}

/// Git's tree order: by name, with a directory compared as if its name
/// ended in `/`.
pub fn sort_entries(entries: &mut [Entry]) {
    entries.sort_by(|a, b| {
        let key = |e: &Entry| {
            let mut k = e.filename.to_vec();
            if e.mode.is_tree() {
                k.push(b'/');
            }
            k
        };
        key(a).cmp(&key(b))
    });
}

/// The git entry kind of a Leep file kind.
pub fn entry_kind(k: FileKind) -> EntryKind {
    match k {
        FileKind::Regular => EntryKind::Blob,
        FileKind::Executable => EntryKind::BlobExecutable,
        FileKind::Symlink => EntryKind::Link,
    }
}

/// The Leep file kind of a git entry kind; None for trees and gitlinks.
pub fn file_kind(k: EntryKind) -> Option<FileKind> {
    match k {
        EntryKind::Blob => Some(FileKind::Regular),
        EntryKind::BlobExecutable => Some(FileKind::Executable),
        EntryKind::Link => Some(FileKind::Symlink),
        _ => None,
    }
}

/// A serialized git tree of `entries`, sorted into git's order first.
pub fn tree_object(mut entries: Vec<Entry>) -> Result<Vec<u8>> {
    sort_entries(&mut entries);
    let mut buf = Vec::new();
    gix_object::WriteTo::write_to(&gix_object::Tree { entries }, &mut buf).map_err(gerr)?;
    Ok(buf)
}

/// The sha1 git gives an object of `kind` with these bytes.
pub fn object_id(kind: gix_object::Kind, data: &[u8]) -> Result<ObjectId> {
    gix_object::compute_hash(gix_hash::Kind::Sha1, kind, data).map_err(gerr)
}

/// The git id of a blob with these bytes.
pub fn blob_id(data: &[u8]) -> Result<ObjectId> {
    object_id(gix_object::Kind::Blob, data)
}

/// The git id of a serialized tree.
pub fn tree_id(tree: &[u8]) -> Result<ObjectId> {
    object_id(gix_object::Kind::Tree, tree)
}

/// The git id of a serialized commit.
pub fn commit_id(commit: &[u8]) -> Result<ObjectId> {
    object_id(gix_object::Kind::Commit, commit)
}

pub use gix_object::Kind as ObjectKind;

/// One git commit, decoded and diffed against its first parent: what
/// [`changeset_for`] needs, read from git by the importer or the mirror.
pub struct Prepared {
    pub parents: Vec<ObjectId>,
    pub author: String,
    pub author_tz: String,
    pub time: i64,
    pub committer: String,
    pub committer_time: i64,
    pub committer_tz: String,
    pub message: String,
    pub encoding: Option<String>,
    pub headers: Vec<(String, String)>,
    /// Original bytes of the fields above that are not valid UTF-8, base64.
    pub raw: BTreeMap<&'static str, serde_json::Value>,
    pub changes: Vec<(String, Option<(FileKind, ObjectId)>)>,
    /// Paths of the gitlinks this commit adds or changes, which are dropped.
    pub submodules: Vec<String>,
}

/// The changeset for git commit `oid`, given its parents' changeset ids and
/// the stored hash and size of each blob it adds. The importer and the live
/// mirror both build changesets here, so a commit gets the same id whichever
/// of them imports it.
pub fn changeset_for(
    oid: ObjectId,
    p: Prepared,
    parents: Vec<ChangesetId>,
    blob: impl Fn(&ObjectId) -> Result<(Hash, u64)>,
) -> Result<Changeset> {
    let mut changes = BTreeMap::new();
    for (path, change) in p.changes {
        let path = RepoPath::new(path)?;
        let change = match change {
            None => FileChange::Delete,
            Some((kind, b)) => {
                let (content, size) = blob(&b)?;
                FileChange::Modify {
                    content,
                    kind,
                    size,
                }
            }
        };
        changes.insert(path, change);
    }
    let mut extra = BTreeMap::from([
        (EXTRA_GIT_COMMIT.to_string(), oid.to_string()),
        (
            EXTRA_GIT_COMMITTER.to_string(),
            format!("{} {}", p.committer, p.committer_time),
        ),
        (EXTRA_GIT_AUTHOR_TZ.to_string(), p.author_tz),
        (EXTRA_GIT_COMMITTER_TZ.to_string(), p.committer_tz),
    ]);
    if let Some(e) = p.encoding {
        extra.insert(EXTRA_GIT_ENCODING.to_string(), e);
    }
    if !p.headers.is_empty() {
        let json = serde_json::to_string(&p.headers).map_err(gerr)?;
        extra.insert(EXTRA_GIT_HEADERS.to_string(), json);
    }
    if !p.raw.is_empty() {
        let json = serde_json::to_string(&p.raw).map_err(gerr)?;
        extra.insert(EXTRA_GIT_RAW.to_string(), json);
    }
    Ok(Changeset {
        parents,
        author: p.author,
        timestamp: p.time,
        message: p.message,
        changes,
        extra,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn changeset(message: &str, raw_message: &[u8]) -> Changeset {
        use base64::Engine;
        let raw = serde_json::json!({
            "message": base64::engine::general_purpose::STANDARD.encode(raw_message),
        });
        Changeset {
            parents: vec![],
            author: "Ada <ada@example.com>".into(),
            timestamp: 1_700_000_000,
            message: message.into(),
            changes: Default::default(),
            extra: BTreeMap::from([(EXTRA_GIT_RAW.to_string(), raw.to_string())]),
        }
    }

    fn body(commit: &[u8]) -> &[u8] {
        let at = commit.windows(2).position(|w| w == b"\n\n").unwrap();
        &commit[at + 2..]
    }

    fn empty_tree() -> ObjectId {
        ObjectId::empty_tree(gix_hash::Kind::Sha1)
    }

    #[test]
    fn raw_message_applies_only_while_the_text_matches() {
        let same = changeset("caf\u{fffd}\n", b"caf\xe9\n");
        let c = commit_object(&same, empty_tree(), vec![], false, None).unwrap();
        assert_eq!(body(&c), b"caf\xe9\n");
        // Someone reworded the changeset: the old bytes must not come back.
        let edited = changeset("coffee\n", b"caf\xe9\n");
        let c = commit_object(&edited, empty_tree(), vec![], false, None).unwrap();
        assert_eq!(body(&c), b"coffee\n");
    }

    /// Git's well-known ids: the empty blob and the empty tree.
    #[test]
    fn ids_match_git() {
        assert_eq!(
            blob_id(b"").unwrap().to_string(),
            "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391"
        );
        assert_eq!(
            tree_id(&tree_object(vec![]).unwrap()).unwrap(),
            empty_tree()
        );
    }

    #[test]
    fn styles_round_trip_and_unknown_ones_are_refused() {
        for provenance_trailers in [false, true] {
            for changeset_trailer in [false, true] {
                let s = CommitStyle {
                    provenance_trailers,
                    changeset_trailer,
                };
                assert_eq!(CommitStyle::decode(&s.encode()).unwrap(), s);
            }
        }
        assert_eq!(CommitStyle::default().encode(), "trailer");
        assert!(CommitStyle::decode("fancy").is_err());
    }

    #[test]
    fn a_native_changeset_is_named_only_with_the_trailer_style() {
        let cs = Changeset {
            parents: vec![],
            author: "Ada <ada@example.com>".into(),
            timestamp: 1_700_000_000,
            message: "add\n".into(),
            changes: Default::default(),
            extra: Default::default(),
        };
        let id = cs.id();
        let plain = CommitStyle {
            changeset_trailer: false,
            ..Default::default()
        };
        let c = commit_for(&cs, id, empty_tree(), vec![], plain).unwrap();
        assert_eq!(changeset_trailer(&String::from_utf8_lossy(body(&c))), None);
        let c = commit_for(&cs, id, empty_tree(), vec![], CommitStyle::default()).unwrap();
        assert_eq!(
            changeset_trailer(&String::from_utf8_lossy(body(&c))),
            Some(id)
        );
    }
}
