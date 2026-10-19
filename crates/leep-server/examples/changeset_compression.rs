//! How small changeset rows get under different encodings, as measured in
//! docs/PERFORMANCE.md ("Changeset compression"). Reads canonical changeset
//! bytes, oldest first, from one of:
//!
//! ```sh
//! # rows as stored: psql -At -c "COPY (SELECT encode(data,'hex') FROM changesets ORDER BY ctid) TO STDOUT"
//! cargo run --release -p leep-server --example changeset_compression -- --hex rows.hex
//! # changesets built from `git log` the way leep-git imports them (blob
//! # hashes and sizes are stand-ins: they need no blobs, so a blobless clone works)
//! cargo run --release -p leep-server --example changeset_compression -- --git DIR [--rev R] [--limit N]
//! ```
//!
//! and prints total bytes per encoding: plain, zstd per row, zstd per row with
//! a dictionary trained on a sample of the rows (`--dict-sizes`), the same
//! dictionary trained on the older half and applied to the newer half, a
//! dictionary trained on another history (`--other-hex`/`--other-git`), and
//! zstd over runs of consecutive rows of about 32 KB. `--write-copy FILE`
//! also writes the rows as `COPY changesets (id, data) FROM` input, to load
//! them into a database and measure the table itself.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::time::Instant;

use leep_core::{Changeset, ChangesetId, FileChange, FileKind, Hash, RepoPath};

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let get = |name: &str| {
        args.iter()
            .position(|a| a == name)
            .map(|i| args[i + 1].clone())
    };
    let limit: usize = get("--limit").map_or(usize::MAX, |s| s.parse().unwrap());
    let rev = get("--rev").unwrap_or_else(|| "HEAD".into());
    let rows = load(get("--hex"), get("--git"), &rev, limit);
    let other = get("--other-hex")
        .or(get("--other-git"))
        .map(|_| load(get("--other-hex"), get("--other-git"), "HEAD", limit));
    let dict_sizes: Vec<usize> = get("--dict-sizes")
        .unwrap_or_else(|| "16384,32768,65536,112640".into())
        .split(',')
        .map(|s| s.parse().unwrap())
        .collect();
    let level: i32 = get("--level").map_or(3, |s| s.parse().unwrap());

    if let Some(path) = get("--write-copy") {
        // `COPY changesets (id, data) FROM` input, to measure a real table.
        let mut out = String::new();
        for r in &rows {
            let id = Hash::of_object(Changeset::KIND, r);
            out.push_str(&format!("\\\\x{}\t\\\\x{}\n", id.to_hex(), hex::encode(r)));
        }
        std::fs::write(path, out).unwrap();
        if args.iter().any(|a| a == "--no-measure") {
            return;
        }
    }
    let n = rows.len();
    let raw: usize = rows.iter().map(Vec::len).sum();
    println!(
        "changesets {n}, plain {raw} bytes ({:.0} per row)",
        raw as f64 / n as f64
    );
    breakdown(&rows);
    report("plain", raw, raw, n);
    for l in [3, 9, 19] {
        let z: usize = rows
            .iter()
            .map(|r| zstd::bulk::compress(r, l).unwrap().len())
            .sum();
        report(&format!("zstd -{l} per row"), z, raw, n);
    }
    let segments = |target: usize, l: i32| {
        let mut total = 0;
        let mut buf = Vec::new();
        for r in &rows {
            buf.extend_from_slice(r);
            if buf.len() >= target {
                total += zstd::bulk::compress(&buf, l).unwrap().len();
                buf.clear();
            }
        }
        if !buf.is_empty() {
            total += zstd::bulk::compress(&buf, l).unwrap().len();
        }
        total
    };
    for l in [3, 19] {
        report(
            &format!("zstd -{l}, 32 KB segments"),
            segments(32 << 10, l),
            raw,
            n,
        );
    }

    // Training samples: every k-th row, as a sample by id order would give.
    let sample = |set: &[Vec<u8>], max: usize| -> Vec<Vec<u8>> {
        let step = (set.len() / max).max(1);
        set.iter().step_by(step).take(max).cloned().collect()
    };
    for &size in &dict_sizes {
        let t = Instant::now();
        let dict = zstd::dict::from_samples(&sample(&rows, 50_000), size).unwrap();
        let train = t.elapsed();
        let t = Instant::now();
        let z = with_dict(&rows, &dict, level);
        let comp = t.elapsed();
        report(
            &format!("zstd -{level}, {} KB dict from a sample", size >> 10),
            z,
            raw,
            n,
        );
        println!(
            "    train {:.2?}, compress {:.0} ns/row, decompress {:.0} ns/row",
            train,
            comp.as_nanos() as f64 / n as f64,
            decompress_ns(&rows, &dict, level)
        );
        let half = n / 2;
        let old = zstd::dict::from_samples(&sample(&rows[..half], 50_000), size).unwrap();
        let newer_raw: usize = rows[half..].iter().map(Vec::len).sum();
        let a = with_dict(&rows[half..], &old, level);
        let b = with_dict(&rows[half..], &dict, level);
        println!(
            "    newer half: dict from older half {:.2}x, from a sample of all {:.2}x",
            newer_raw as f64 / a as f64,
            newer_raw as f64 / b as f64
        );
        if let Some(other) = &other {
            let od = zstd::dict::from_samples(&sample(other, 50_000), size).unwrap();
            let z = with_dict(&rows, &od, level);
            report(
                &format!("  dict from the other history ({} KB)", size >> 10),
                z,
                raw,
                n,
            );
        }
    }
    for l in [1, 9, 19] {
        let dict = zstd::dict::from_samples(&sample(&rows, 50_000), 65536).unwrap();
        report(
            &format!("zstd -{l}, 64 KB dict"),
            with_dict(&rows, &dict, l),
            raw,
            n,
        );
    }
}

fn report(name: &str, bytes: usize, raw: usize, n: usize) {
    println!(
        "{name:<44} {bytes:>12} bytes  {:>7.0} per row  {:.2}x",
        bytes as f64 / n as f64,
        raw as f64 / bytes as f64
    );
}

fn with_dict(rows: &[Vec<u8>], dict: &[u8], level: i32) -> usize {
    let mut c = zstd::bulk::Compressor::with_dictionary(level, dict).unwrap();
    rows.iter().map(|r| c.compress(r).unwrap().len()).sum()
}

fn decompress_ns(rows: &[Vec<u8>], dict: &[u8], level: i32) -> f64 {
    let mut c = zstd::bulk::Compressor::with_dictionary(level, dict).unwrap();
    let z: Vec<Vec<u8>> = rows.iter().map(|r| c.compress(r).unwrap()).collect();
    let mut d = zstd::bulk::Decompressor::with_dictionary(dict).unwrap();
    let t = Instant::now();
    for (z, r) in z.iter().zip(rows) {
        let out = d.decompress(z, r.len()).unwrap();
        assert_eq!(&out, r);
    }
    t.elapsed().as_nanos() as f64 / rows.len() as f64
}

/// Bytes by field over all rows: what compression can and cannot shrink.
fn breakdown(rows: &[Vec<u8>]) {
    let (mut parents, mut hashes, mut paths, mut msg, mut author, mut extra) = (0, 0, 0, 0, 0, 0);
    let mut changes = 0;
    for r in rows {
        let cs = Changeset::decode(r).unwrap();
        parents += cs.parents.len() * 32;
        author += cs.author.len();
        msg += cs.message.len();
        changes += cs.changes.len();
        for (p, c) in &cs.changes {
            paths += p.as_str().len();
            if matches!(c, FileChange::Modify { .. }) {
                hashes += 32;
            }
        }
        extra += cs
            .extra
            .iter()
            .map(|(k, v)| k.len() + v.len())
            .sum::<usize>();
    }
    let n = rows.len() as f64;
    println!(
        "per row: parents {:.0}, content hashes {:.0}, paths {:.0} ({:.1} changes), message {:.0}, author {:.0}, extra {:.0}",
        parents as f64 / n,
        hashes as f64 / n,
        paths as f64 / n,
        changes as f64 / n,
        msg as f64 / n,
        author as f64 / n,
        extra as f64 / n
    );
}

fn load(hex_file: Option<String>, git: Option<String>, rev: &str, limit: usize) -> Vec<Vec<u8>> {
    if let Some(f) = hex_file {
        let text = std::fs::read_to_string(f).unwrap();
        return text
            .lines()
            .take(limit)
            .map(|l| hex::decode(l.trim()).unwrap())
            .collect();
    }
    let dir = git.expect("--hex or --git");
    from_git(&dir, rev, limit)
}

/// Changesets shaped like leep-git's import of each commit (first-parent
/// diff, the same `extra` keys), oldest first. A shallow clone's boundary
/// commits are skipped; their children name a stand-in id.
fn from_git(dir: &str, rev: &str, limit: usize) -> Vec<Vec<u8>> {
    let mut child = Command::new("git")
        .args([
            "--git-dir",
            dir,
            "-c",
            "core.quotePath=false",
            "log",
            "--reverse",
            "--topo-order",
            "--format=raw",
            "--raw",
            "--no-abbrev",
            "--no-renames",
            "--diff-merges=first-parent",
            rev,
        ])
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let out = BufReader::new(child.stdout.take().unwrap());
    let mut rows = Vec::new();
    let mut cur: Option<Commit> = None;
    let mut ids: BTreeMap<String, ChangesetId> = BTreeMap::new();
    let flush = |c: Commit, ids: &mut BTreeMap<String, ChangesetId>, rows: &mut Vec<Vec<u8>>| {
        if c.shallow {
            // Not emitted (git diffs it against an empty tree), but its
            // children name it as a parent.
            ids.insert(c.sha.clone(), stand_in(&c.sha));
            return;
        }
        let parents = c
            .parents
            .iter()
            .map(|p| ids.get(p).copied().unwrap_or_else(|| stand_in(p)))
            .collect();
        let mut extra = BTreeMap::from([
            ("git.commit".to_string(), c.sha.clone()),
            ("git.committer".to_string(), c.committer),
            ("git.author_tz".to_string(), c.author_tz),
            ("git.committer_tz".to_string(), c.committer_tz),
        ]);
        if !c.headers.is_empty() {
            extra.insert(
                "git.extra_headers".to_string(),
                serde_json::to_string(&c.headers).unwrap(),
            );
        }
        let cs = Changeset {
            parents,
            author: c.author,
            timestamp: c.time,
            message: c.message,
            changes: c.changes,
            extra,
        };
        ids.insert(c.sha, cs.id());
        rows.push(cs.encode());
    };
    // A shallow clone's boundary commits, whose parents are missing.
    let shallow = std::fs::read_to_string(format!("{dir}/shallow")).unwrap_or_default();
    let mut in_header = false;
    for line in out.lines() {
        let line = line.unwrap();
        if let Some(sha) = line.strip_prefix("commit ") {
            if let Some(c) = cur.take() {
                flush(c, &mut ids, &mut rows);
                if rows.len() >= limit {
                    break;
                }
            }
            cur = Some(Commit {
                sha: sha.split(' ').next().unwrap().to_string(),
                ..Default::default()
            });
            in_header = true;
            continue;
        }
        let Some(c) = cur.as_mut() else { continue };
        if in_header {
            if line.is_empty() {
                in_header = false;
                c.shallow = shallow.contains(&c.sha);
            } else if let Some(rest) = line.strip_prefix(' ') {
                if let Some(last) = c.headers.last_mut() {
                    last.1.push('\n');
                    last.1.push_str(rest);
                }
            } else {
                let (k, v) = line.split_once(' ').unwrap_or((&line, ""));
                match k {
                    "tree" => {}
                    "parent" => c.parents.push(v.to_string()),
                    "author" => {
                        let (who, time, tz) = ident(v);
                        c.author = who;
                        c.time = time;
                        c.author_tz = tz;
                    }
                    "committer" => {
                        let (who, time, tz) = ident(v);
                        c.committer = format!("{who} {time}");
                        c.committer_tz = tz;
                    }
                    _ => c.headers.push((k.to_string(), v.to_string())),
                }
            }
        } else if let Some(m) = line.strip_prefix("    ") {
            c.message.push_str(m);
            c.message.push('\n');
        } else if line == "    " || line.is_empty() {
            if !c.message.is_empty() && line.is_empty() {
                continue;
            }
        } else if let Some(raw) = line.strip_prefix(':') {
            let (meta, path) = raw.split_once('\t').unwrap();
            let f: Vec<&str> = meta.split(' ').collect();
            let Ok(path) = RepoPath::new(path) else {
                continue;
            };
            let change = if f[4] == "D" {
                FileChange::Delete
            } else {
                let kind = match f[1] {
                    "100755" => FileKind::Executable,
                    "120000" => FileKind::Symlink,
                    "160000" => continue,
                    _ => FileKind::Regular,
                };
                let oid = hex::decode(f[3]).unwrap();
                let content = Hash::of_blob(&oid);
                // A size spread like source files', from the oid.
                let size = 64u64 << (oid[0] % 12) | u64::from(oid[1]);
                FileChange::Modify {
                    content,
                    kind,
                    size,
                }
            };
            c.changes.insert(path, change);
        }
    }
    if let Some(c) = cur.take() {
        if rows.len() < limit {
            flush(c, &mut ids, &mut rows);
        }
    }
    child.kill().ok();
    child.wait().ok();
    rows
}

#[derive(Default)]
struct Commit {
    sha: String,
    parents: Vec<String>,
    shallow: bool,
    author: String,
    time: i64,
    author_tz: String,
    committer: String,
    committer_tz: String,
    headers: Vec<(String, String)>,
    message: String,
    changes: BTreeMap<RepoPath, FileChange>,
}

fn stand_in(sha: &str) -> ChangesetId {
    ChangesetId(Hash::of_blob(sha.as_bytes()))
}

fn ident(v: &str) -> (String, i64, String) {
    let mut parts = v.rsplitn(3, ' ');
    let tz = parts.next().unwrap_or("+0000").to_string();
    let time = parts.next().and_then(|t| t.parse().ok()).unwrap_or(0);
    (parts.next().unwrap_or("").to_string(), time, tz)
}
