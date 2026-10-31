//! The land fast path and the slow path's restricted merge (ADR 0066): each
//! must answer exactly what the full replay answers.

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
use leep_core::{Changeset, ChangesetId, FileChange, FileKind, RepoPath};

use crate::Repo;

pub(crate) struct Rng(pub(crate) u64);

impl Rng {
    pub(crate) fn new(seed: u64) -> Self {
        Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    pub(crate) fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

fn plain_repo() -> Repo {
    Repo::new(
        Arc::new(MemoryBlobStore::new()),
        Arc::new(MemoryMetaStore::new()),
    )
}

/// A file's content and whether it is executable.
pub(crate) type Body = (String, bool);
/// The files of a tree, by path.
pub(crate) type Files = BTreeMap<String, Body>;
/// A changeset's changes in model form: `None` deletes.
pub(crate) type Edits = BTreeMap<String, Option<Body>>;

/// Paths that nest inside each other, so random edits clash as files and
/// directories as well as on the same file.
const NAMES: &[&str] = &["a", "b", "c"];

fn random_path(rng: &mut Rng, shared: &str) -> String {
    let depth = 1 + rng.below(3);
    let mut parts: Vec<String> = Vec::new();
    if !shared.is_empty() && rng.below(2) == 0 {
        parts.push(shared.to_string());
    }
    for _ in 0..depth {
        parts.push(NAMES[rng.below(NAMES.len())].to_string());
    }
    parts.join("/")
}

fn within(p: &str, dir: &str) -> bool {
    p.len() > dir.len() && p.starts_with(dir) && p.as_bytes()[dir.len()] == b'/'
}

/// Random valid edits of `files`, `ops` of them: modifies (sometimes to
/// the same content, a no-op change), deletes, and adds that replace a file
/// with a directory or a directory with a file. `owned` paths are where this
/// agent writes most; `shared` is the prefix every agent may write under.
pub(crate) fn random_edits(rng: &mut Rng, files: &Files, ops: usize, shared: &str) -> Edits {
    let mut work = files.clone();
    let body = |rng: &mut Rng| -> Body { (format!("v{}\n", rng.below(4)), rng.below(8) == 0) };
    for _ in 0..ops {
        match rng.below(4) {
            0 if !work.is_empty() => {
                let k = work.keys().nth(rng.below(work.len())).unwrap().clone();
                work.remove(&k);
            }
            1 if !work.is_empty() => {
                let k = work.keys().nth(rng.below(work.len())).unwrap().clone();
                let b = body(rng);
                work.insert(k, b);
            }
            _ => {
                let p = random_path(rng, shared);
                // A file on the way, or a directory where the file goes, is
                // deleted first.
                let mut prefix = String::new();
                for comp in p.split('/') {
                    if !prefix.is_empty() {
                        prefix.push('/');
                    }
                    prefix.push_str(comp);
                    if prefix != p {
                        work.remove(&prefix);
                    }
                }
                work.retain(|k, _| !within(k, &p));
                let b = body(rng);
                work.insert(p, b);
            }
        }
    }
    let mut edits = Edits::new();
    for (k, v) in &work {
        // Unchanged files stay out, except now and then as a no-op modify.
        if files.get(k) != Some(v) || rng.below(6) == 0 {
            edits.insert(k.clone(), Some(v.clone()));
        }
    }
    for k in files.keys() {
        if !work.contains_key(k) {
            edits.insert(k.clone(), None);
        }
    }
    edits
}

pub(crate) async fn create(
    r: &Repo,
    parents: &[ChangesetId],
    edits: &Edits,
    msg: &str,
) -> ChangesetId {
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
    r.create_changeset(Changeset {
        parents: parents.to_vec(),
        author: "t".into(),
        timestamp: 1_700_000_000,
        message: msg.into(),
        changes,
        extra: BTreeMap::new(),
    })
    .await
    .unwrap()
}

/// Reading `ours` only at the paths `theirs` changed merges exactly as
/// diffing the whole of `ours` did: same changes, same conflicts, over
/// random trees with nested files, file/directory swaps, no-op modifies,
/// kind changes and text merges.
#[tokio::test]
async fn a_restricted_merge_matches_the_full_merge() {
    let mut differ = 0;
    for seed in 1..=300u64 {
        let mut rng = Rng::new(seed);
        let r = plain_repo();
        let base_files = random_edits(&mut rng, &Files::new(), 6, "")
            .into_iter()
            .filter_map(|(k, v)| Some((k, v?)))
            .collect::<Files>();
        let base_edits: Edits = base_files
            .iter()
            .map(|(k, v)| (k.clone(), Some(v.clone())))
            .collect();
        let base = create(&r, &[], &base_edits, "base").await;
        let ours_ops = rng.below(8);
        let ours = create(
            &r,
            &[base],
            &random_edits(&mut rng, &base_files, ours_ops, ""),
            "ours",
        )
        .await;
        let theirs_ops = 1 + rng.below(4);
        let theirs = create(
            &r,
            &[base],
            &random_edits(&mut rng, &base_files, theirs_ops, ""),
            "theirs",
        )
        .await;
        let (b, o, t) = (
            r.root_tree(base).await.unwrap(),
            r.root_tree(ours).await.unwrap(),
            r.root_tree(theirs).await.unwrap(),
        );
        let got = r.merge_trees(b, o, t).await.unwrap();
        let want = r.merge_trees_full(b, o, t).await.unwrap();
        assert_eq!(got.changes, want.changes, "seed {seed}");
        assert_eq!(got.conflicts, want.conflicts, "seed {seed}");
        differ += usize::from(!want.conflicts.is_empty());
    }
    assert!(differ > 20, "some merges conflicted ({differ})");
}
