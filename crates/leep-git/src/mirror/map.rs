//! The mirror's changeset <-> commit map: an append-only text file of
//! `<changeset id> <commit sha> <origin>` lines in its work directory.
//!
//! Entries from exports are a cache: the export of a changeset is a pure
//! function of the changeset, so a lost entry is recomputed to the same
//! commit. Entries from imports record which commit a changeset came from,
//! which matters when the export would not reproduce that commit exactly
//! (for example a commit with submodules), so they are synced to disk before
//! the import lands.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Seek, SeekFrom, Write};
use std::path::Path;

use gix::ObjectId;
use leep_core::ChangesetId;

/// Where a mapping came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Origin {
    /// The mirror wrote the commit from the changeset.
    Exported,
    /// The mirror created the changeset from a commit on the remote.
    Imported,
}

impl Origin {
    fn tag(self) -> char {
        match self {
            Origin::Exported => 'e',
            Origin::Imported => 'i',
        }
    }
}

pub struct Map {
    file: File,
    to_git: HashMap<ChangesetId, ObjectId>,
    to_leep: HashMap<ObjectId, (ChangesetId, Origin)>,
    unsynced: bool,
}

impl Map {
    /// Open or create the map. A torn last line (a crash mid-append) is cut
    /// off so later appends start on a fresh line.
    pub fn open(path: &Path) -> std::io::Result<Map> {
        // Opened for writing, not appending: cutting off a torn tail needs
        // `set_len`, which an append-only handle cannot do on Windows. The
        // position is moved to the end once below and never elsewhere.
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)?;
        let mut to_git = HashMap::new();
        let mut to_leep = HashMap::new();
        let mut good_len = 0u64;
        let mut reader = BufReader::new(&file);
        let mut line = String::new();
        loop {
            line.clear();
            let n = reader.read_line(&mut line)?;
            if n == 0 || !line.ends_with('\n') {
                break;
            }
            if let Some((id, oid, origin)) = parse(line.trim_end()) {
                to_git.insert(id, oid);
                to_leep.insert(oid, (id, origin));
            }
            good_len += n as u64;
        }
        drop(reader);
        if file.metadata()?.len() != good_len {
            file.set_len(good_len)?;
            file.sync_all()?;
        }
        file.seek(SeekFrom::End(0))?;
        Ok(Map {
            file,
            to_git,
            to_leep,
            unsynced: false,
        })
    }

    pub fn len(&self) -> usize {
        self.to_git.len()
    }

    pub fn is_empty(&self) -> bool {
        self.to_git.is_empty()
    }

    pub fn git(&self, id: &ChangesetId) -> Option<ObjectId> {
        self.to_git.get(id).copied()
    }

    pub fn leep(&self, oid: &ObjectId) -> Option<(ChangesetId, Origin)> {
        self.to_leep.get(oid).copied()
    }

    /// Record a mapping; a repeat of a known pair writes nothing. A changeset
    /// mapped again to another commit (its old commit was lost from the local
    /// repository and re-exported) takes the new value.
    pub fn insert(
        &mut self,
        id: ChangesetId,
        oid: ObjectId,
        origin: Origin,
    ) -> std::io::Result<()> {
        if self.to_git.get(&id) == Some(&oid) {
            return Ok(());
        }
        writeln!(self.file, "{id} {oid} {}", origin.tag())?;
        self.unsynced = true;
        self.to_git.insert(id, oid);
        self.to_leep.insert(oid, (id, origin));
        Ok(())
    }

    /// Make every recorded entry durable.
    pub fn sync(&mut self) -> std::io::Result<()> {
        if self.unsynced {
            self.file.sync_data()?;
            self.unsynced = false;
        }
        Ok(())
    }
}

fn parse(line: &str) -> Option<(ChangesetId, ObjectId, Origin)> {
    let mut it = line.split(' ');
    let id: ChangesetId = it.next()?.parse().ok()?;
    let oid = ObjectId::from_hex(it.next()?.as_bytes()).ok()?;
    let origin = match it.next()? {
        "e" => Origin::Exported,
        "i" => Origin::Imported,
        _ => return None,
    };
    Some((id, oid, origin))
}

#[cfg(test)]
mod tests {
    use super::*;
    use leep_core::Hash;

    fn id(n: u8) -> ChangesetId {
        ChangesetId(Hash([n; 32]))
    }

    fn oid(n: u8) -> ObjectId {
        ObjectId::from_bytes_or_panic(&[n; 20])
    }

    #[test]
    fn entries_survive_reopen_and_a_torn_tail_is_dropped() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("map");
        {
            let mut m = Map::open(&path).unwrap();
            m.insert(id(1), oid(1), Origin::Exported).unwrap();
            m.insert(id(2), oid(2), Origin::Imported).unwrap();
            m.insert(id(2), oid(2), Origin::Imported).unwrap();
            m.sync().unwrap();
        }
        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        f.write_all(b"abcdef 0123").unwrap();
        drop(f);

        let mut m = Map::open(&path).unwrap();
        assert_eq!(m.len(), 2);
        assert_eq!(m.git(&id(1)), Some(oid(1)));
        assert_eq!(m.leep(&oid(2)), Some((id(2), Origin::Imported)));
        m.insert(id(3), oid(3), Origin::Exported).unwrap();
        m.sync().unwrap();
        drop(m);
        let text = std::fs::read_to_string(&path).unwrap();
        assert_eq!(text.lines().count(), 3, "{text}");
        assert_eq!(Map::open(&path).unwrap().git(&id(3)), Some(oid(3)));
    }
}
