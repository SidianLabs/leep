//! Per-directory `OWNERS` files (ADR 0011).
//!
//! One owner name per line (a token name), `#` starts a comment, blank lines
//! are ignored and a line `noparent` stops inheritance from ancestor
//! directories. The owners of a path are the union over its directory and
//! every ancestor up to the first `noparent` or the root.
//!
//! Owners are always read from a tree the caller names (the target ref's
//! head), never from the change being judged, so a change cannot grant itself
//! ownership. A path no file covers has no owners.

use std::collections::{BTreeSet, HashMap};

use leep_core::review::OWNERS_FILE;
use leep_core::{ChangesetId, Error, FileKind, Hash, RepoPath, Result};

use crate::Repo;

/// Largest `OWNERS` file read, in bytes.
const MAX_OWNERS_BYTES: u64 = 1024 * 1024;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OwnersFile {
    pub owners: BTreeSet<String>,
    pub noparent: bool,
}

/// Parse the text of one `OWNERS` file. Never fails: a line is a name unless
/// it is blank, a comment or `noparent`.
pub fn parse_owners(text: &str) -> OwnersFile {
    let mut f = OwnersFile::default();
    for line in text.trim_start_matches('\u{feff}').lines() {
        let line = line.split('#').next().unwrap_or("").trim();
        match line {
            "" => {}
            "noparent" => f.noparent = true,
            name => {
                f.owners.insert(name.to_string());
            }
        }
    }
    f
}

/// Directories of `path` from the deepest up to the root (`""`).
fn ancestor_dirs(path: &RepoPath) -> Vec<&str> {
    let mut dirs = Vec::new();
    let mut cur = path.parent();
    while let Some(d) = cur {
        dirs.push(d);
        cur = d.rsplit_once('/').map(|(p, _)| p);
    }
    dirs.push("");
    dirs
}

/// Resolves owners against one tree, reading each directory's file once.
pub struct OwnersResolver<'a> {
    repo: &'a Repo,
    tree: Option<Hash>,
    files: HashMap<String, Option<OwnersFile>>,
}

impl<'a> OwnersResolver<'a> {
    /// Owners as of `head`'s tree; with no head nothing has owners.
    pub async fn new(repo: &'a Repo, head: Option<ChangesetId>) -> Result<Self> {
        let tree = match head {
            Some(h) => Some(repo.root_tree(h).await?),
            None => None,
        };
        Ok(OwnersResolver {
            repo,
            tree,
            files: HashMap::new(),
        })
    }

    async fn file_in(&mut self, dir: &str) -> Result<Option<&OwnersFile>> {
        if !self.files.contains_key(dir) {
            let parsed = self.read(dir).await?;
            self.files.insert(dir.to_string(), parsed);
        }
        Ok(self.files[dir].as_ref())
    }

    async fn read(&self, dir: &str) -> Result<Option<OwnersFile>> {
        let Some(tree) = self.tree else {
            return Ok(None);
        };
        let name = if dir.is_empty() {
            OWNERS_FILE.to_string()
        } else {
            format!("{dir}/{OWNERS_FILE}")
        };
        let Some(info) = self
            .repo
            .file_at(tree, &RepoPath::new(name.clone())?)
            .await?
        else {
            return Ok(None);
        };
        // A symlink's content is its target, not a list of owners.
        if info.kind == FileKind::Symlink {
            return Ok(None);
        }
        if info.size > MAX_OWNERS_BYTES {
            return Err(Error::Invalid(format!(
                "{name} is {} bytes; the limit is {MAX_OWNERS_BYTES}",
                info.size
            )));
        }
        let data =
            self.repo.blobs().get(&info.content).await?.ok_or_else(|| {
                Error::Storage(format!("blob {} missing for {name}", info.content))
            })?;
        Ok(Some(parse_owners(&String::from_utf8_lossy(&data))))
    }

    /// Owners of `path`, sorted; empty when no `OWNERS` file covers it.
    pub async fn owners_of(&mut self, path: &RepoPath) -> Result<BTreeSet<String>> {
        let mut owners = BTreeSet::new();
        for dir in ancestor_dirs(path) {
            if let Some(f) = self.file_in(dir).await? {
                owners.extend(f.owners.iter().cloned());
                if f.noparent {
                    break;
                }
            }
        }
        Ok(owners)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(f: &OwnersFile) -> Vec<&str> {
        f.owners.iter().map(String::as_str).collect()
    }

    #[test]
    fn parses_names_comments_blanks_and_noparent() {
        let f = parse_owners(
            "# who reviews this\nalice\n\n  bob  # second\r\n#carol\nnoparent\nalice\n",
        );
        assert_eq!(names(&f), ["alice", "bob"]);
        assert!(f.noparent);
        assert!(!parse_owners("alice\n").noparent);
        assert_eq!(parse_owners(""), OwnersFile::default());
        assert_eq!(names(&parse_owners("\u{feff}dave")), ["dave"]);
    }

    #[test]
    fn noparent_is_a_whole_line() {
        let f = parse_owners("noparent-team\nno parent\n");
        assert!(!f.noparent);
        assert_eq!(names(&f), ["no parent", "noparent-team"]);
    }

    #[test]
    fn names_may_contain_spaces_but_not_hashes() {
        assert_eq!(
            names(&parse_owners("ci bot\nweird#name\n")),
            ["ci bot", "weird"]
        );
    }

    #[test]
    fn ancestor_dirs_run_deepest_first() {
        let p = RepoPath::new("a/b/c.rs").unwrap();
        assert_eq!(ancestor_dirs(&p), ["a/b", "a", ""]);
        let p = RepoPath::new("top.rs").unwrap();
        assert_eq!(ancestor_dirs(&p), [""]);
    }
}
