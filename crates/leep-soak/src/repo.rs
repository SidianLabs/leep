//! The synthetic repository the swarm works on, and how text is edited.

use std::collections::BTreeMap;

use leep_core::{Changeset, ChangesetId, FileChange, FileKind, Hash, RepoPath};

use crate::api::{Api, Fail, R};
use crate::util::Rng;

pub const FILES_PER_DIR: usize = 100;

/// Sizes of the three parts of the tree.
#[derive(Clone, Copy, Debug)]
pub struct Layout {
    pub dirs: usize,
    pub docs: usize,
    pub assets: usize,
}

impl Layout {
    pub fn new(files: usize) -> Layout {
        let files = files.max(100);
        let docs = (files / 12).max(5);
        let assets = (files / 40).max(3);
        let src = files - docs - assets;
        Layout {
            dirs: src.div_ceil(FILES_PER_DIR).max(1),
            docs,
            assets,
        }
    }

    pub fn total(&self) -> usize {
        self.dirs * FILES_PER_DIR + self.docs + self.assets
    }
}

pub fn src_dir(d: usize) -> String {
    format!("src/m{d:02}")
}

pub fn src_path(d: usize, i: usize) -> String {
    format!("src/m{d:02}/f{i:03}.rs")
}

pub fn doc_path(i: usize) -> String {
    format!("docs/guide{i:03}.md")
}

pub fn asset_path(i: usize) -> String {
    format!("assets/img{i:03}.bin")
}

/// A few files every agent likes to touch, so real conflicts happen.
pub fn hot_files() -> Vec<String> {
    (0..8).map(|i| src_path(0, i)).collect()
}

pub fn src_text(rng: &mut Rng, d: usize, i: usize) -> String {
    let mut s = format!("//! module m{d:02}, file f{i:03}\n\nuse std::collections::BTreeMap;\n\n");
    for k in 0..4 + rng.below(8) {
        s.push_str(&function(rng, d, i, k));
    }
    s
}

fn function(rng: &mut Rng, d: usize, i: usize, k: usize) -> String {
    let mut s = format!(
        "/// Handler {k} of f{i:03}: mixes its input with constant {:x}.\npub fn handler_{d:02}_{i:03}_{k}(x: u64) -> u64 {{\n",
        rng.next_u64() & 0xffff_ffff
    );
    s.push_str("    let mut acc = x;\n");
    for _ in 0..3 + rng.below(5) {
        s.push_str(&statement(rng));
    }
    s.push_str("    acc\n}\n\n");
    s
}

fn statement(rng: &mut Rng) -> String {
    match rng.below(4) {
        0 => format!("    acc = acc.wrapping_mul({});\n", rng.below(10_000) | 1),
        1 => format!("    acc ^= acc >> {};\n", 1 + rng.below(31)),
        2 => format!("    acc = acc.wrapping_add({});\n", rng.below(1 << 20)),
        _ => format!("    acc = acc.rotate_left({});\n", 1 + rng.below(63)),
    }
}

pub fn doc_text(rng: &mut Rng, i: usize) -> String {
    let mut s = format!("# Guide {i}\n\n");
    for p in 0..3 + rng.below(6) {
        s.push_str(&format!(
            "Section {p}. The {} of component {} is set to {} by default.\n\n",
            ["limit", "timeout", "retry budget", "cache size"][rng.below(4)],
            rng.below(40),
            rng.below(5000)
        ));
    }
    s
}

pub fn asset_bytes(rng: &mut Rng) -> Vec<u8> {
    let n = 2048 + rng.below(20 * 1024);
    let mut b = rng.bytes(n);
    // A NUL near the start is what makes tools treat a file as binary.
    b[0] = 0;
    b
}

/// A new source file an agent adds.
pub fn new_src_text(rng: &mut Rng, agent: &str, n: usize) -> String {
    let mut s = format!("//! added by {agent}, number {n}\n\n");
    for k in 0..2 + rng.below(4) {
        s.push_str(&function(rng, 99, n % 1000, k));
    }
    s
}

/// Edit text the way a developer does: change, insert or drop a few lines.
pub fn edit_text(rng: &mut Rng, text: &str) -> String {
    let mut lines: Vec<String> = text.lines().map(str::to_string).collect();
    if lines.is_empty() {
        lines.push(String::new());
    }
    for _ in 0..1 + rng.below(3) {
        let at = rng.below(lines.len());
        match rng.below(4) {
            0 | 1 => {
                if lines[at].trim_start().starts_with("acc") {
                    lines[at] = statement(rng).trim_end().to_string();
                } else {
                    lines[at].push_str(&format!(" // tweak {}", rng.below(1000)));
                }
            }
            2 => lines.insert(at, format!("    // note {}", rng.below(100_000))),
            _ => {
                if lines.len() > 8 {
                    lines.remove(at);
                }
            }
        }
    }
    let mut out = lines.join("\n");
    out.push('\n');
    out
}

pub fn change_of(data: &[u8], kind: FileKind) -> FileChange {
    FileChange::Modify {
        content: Hash::of_blob(data),
        kind,
        size: data.len() as u64,
    }
}

/// Create the repository: a chain of changesets of at most 400 files each,
/// landed on `main` by creating the ref.
/// Land the synthetic repository on `main`: as its first history, or on top
/// of `onto` (the current head of an existing `main`).
pub async fn import(
    api: &Api,
    layout: Layout,
    seed: u64,
    onto: Option<ChangesetId>,
) -> R<ChangesetId> {
    let mut files: Vec<(String, Vec<u8>)> = Vec::new();
    for d in 0..layout.dirs {
        for i in 0..FILES_PER_DIR {
            let mut rng = Rng::new(seed ^ (((d * FILES_PER_DIR + i) as u64 + 1) * 0x9E37_79B1));
            files.push((src_path(d, i), src_text(&mut rng, d, i).into_bytes()));
        }
    }
    for i in 0..layout.docs {
        let mut rng = Rng::new(seed ^ ((i as u64 + 0xD0C5) * 0x7F4A));
        files.push((doc_path(i), doc_text(&mut rng, i).into_bytes()));
    }
    for i in 0..layout.assets {
        let mut rng = Rng::new(seed ^ ((i as u64 + 0xA55E7) * 0x51ED));
        files.push((asset_path(i), asset_bytes(&mut rng)));
    }
    let mut parent: Option<ChangesetId> = onto;
    for (n, chunk) in files.chunks(400).enumerate() {
        api.upload(
            &chunk
                .iter()
                .map(|(_, d)| (Hash::of_blob(d), d.clone()))
                .collect::<Vec<_>>(),
        )
        .await?;
        let changes: BTreeMap<RepoPath, FileChange> = chunk
            .iter()
            .map(|(p, d)| {
                (
                    RepoPath::new(p.as_str()).unwrap(),
                    change_of(d, FileKind::Regular),
                )
            })
            .collect();
        let cs = Changeset {
            parents: parent.into_iter().collect(),
            author: "soak-import".into(),
            timestamp: 1_700_000_000 + n as i64,
            message: format!("import part {n}"),
            changes,
            extra: BTreeMap::new(),
        };
        match api.create_changeset(&cs).await? {
            Ok(id) => parent = Some(id),
            Err(e) => {
                api.sh.violate("import", format!("import lost a blob: {e}"));
                return Err(Fail::Bad);
            }
        }
    }
    let head = parent.expect("a repository has files");
    if !api.cas_ref("main", onto, Some(head)).await? {
        api.sh.violate("import", "main moved during the import");
        return Err(Fail::Bad);
    }
    Ok(head)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layout_adds_up() {
        let l = Layout::new(3000);
        assert!(l.total() >= 3000 && l.total() < 3200, "{l:?}");
        assert!(Layout::new(1).total() >= 100);
    }

    #[test]
    fn edits_change_text_deterministically() {
        let mut a = Rng::new(3);
        let t = src_text(&mut a, 1, 2);
        let e1 = edit_text(&mut Rng::new(9), &t);
        let e2 = edit_text(&mut Rng::new(9), &t);
        assert_eq!(e1, e2);
        assert_ne!(e1, t);
        assert!(t.contains("handler_01_002_0"));
    }
}
