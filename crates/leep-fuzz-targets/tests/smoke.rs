//! Stable smoke run of every fuzz target: the checked-in corpus as is, then
//! seeded mutations of it, then random bytes. Not a substitute for libFuzzer
//! (no coverage feedback), but it keeps the targets building and the corpus
//! replaying in `cargo test`, and catches the shallow bugs fast.
//!
//! Knobs: `LEEP_FUZZ_ITERS` (mutations per target, default 2000),
//! `LEEP_FUZZ_SEED` (RNG seed), `LEEP_FUZZ_BLESS=1` (rewrite `seed-*` files).

use std::fmt::Write as _;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};

use leep_fuzz_targets::{seeds, Target, TARGETS};

fn corpus_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fuzz/corpus")
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// xorshift64*: small, seedable, and good enough to mutate bytes.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n.max(1) as u64) as usize
    }
}

/// Bytes that sit on boundaries of varints, lengths and ASCII structure.
const INTERESTING: &[u8] = &[
    0, 1, 2, 3, 0x7F, 0x80, 0x81, 0xFE, 0xFF, b'"', b'\\', b'/', b'*', b'#', b'\n', b'{', b'}',
    b'[', b']', b'(', b')', b'.', b':', b'\'',
];

fn mutate(rng: &mut Rng, input: &[u8], donors: &[Vec<u8>]) -> Vec<u8> {
    let mut v = input.to_vec();
    for _ in 0..1 + rng.below(4) {
        match rng.below(8) {
            0 if !v.is_empty() => {
                let i = rng.below(v.len());
                v[i] ^= 1 << rng.below(8);
            }
            1 if !v.is_empty() => {
                let i = rng.below(v.len());
                v[i] = rng.next() as u8;
            }
            2 => {
                let i = rng.below(v.len() + 1);
                v.insert(i, INTERESTING[rng.below(INTERESTING.len())]);
            }
            3 if !v.is_empty() => {
                let i = rng.below(v.len());
                let n = 1 + rng.below(8.min(v.len() - i));
                v.drain(i..i + n);
            }
            4 => {
                let n = rng.below(v.len() + 1);
                v.truncate(n);
            }
            5 if !v.is_empty() => {
                let i = rng.below(v.len());
                let n = 1 + rng.below(16.min(v.len() - i));
                let chunk = v[i..i + n].to_vec();
                let at = rng.below(v.len() + 1);
                v.splice(at..at, chunk);
            }
            6 if !donors.is_empty() => {
                let d = &donors[rng.below(donors.len())];
                if !d.is_empty() {
                    let i = rng.below(d.len());
                    let n = 1 + rng.below(32.min(d.len() - i));
                    let at = rng.below(v.len() + 1);
                    v.splice(at..at, d[i..i + n].iter().copied());
                }
            }
            7 if !v.is_empty() => {
                let i = rng.below(v.len());
                v[i] = v[i].wrapping_add(1 + rng.below(3) as u8);
            }
            _ => {}
        }
    }
    v
}

/// Run one input; on a panic print what is needed to reproduce it.
fn run(t: &Target, input: &[u8], origin: &str) {
    if catch_unwind(AssertUnwindSafe(|| (t.check)(input))).is_err() {
        let shown = &input[..input.len().min(4096)];
        let mut hex = String::new();
        for b in shown {
            write!(hex, "{b:02x}").unwrap();
        }
        panic!(
            "target `{}` failed on {origin} ({} bytes); input in hex (first 4096 bytes): {hex}\n\
             Save it as a file and run `cargo run -p leep-fuzz-targets --example replay -- {} FILE`.",
            t.name,
            input.len(),
            t.name
        );
    }
}

fn corpus_files(dir: &Path) -> Vec<(String, Vec<u8>)> {
    let mut files: Vec<_> = std::fs::read_dir(dir)
        .map(|rd| {
            rd.filter_map(Result::ok)
                .filter(|e| e.path().is_file())
                .map(|e| {
                    (
                        e.file_name().to_string_lossy().into_owned(),
                        std::fs::read(e.path()).unwrap(),
                    )
                })
                .collect()
        })
        .unwrap_or_default();
    files.sort();
    files
}

#[test]
fn seeds_are_up_to_date() {
    let bless = env_u64("LEEP_FUZZ_BLESS", 0) == 1;
    let mut drift = Vec::new();
    let mut wanted = std::collections::BTreeSet::new();
    for s in seeds::all() {
        let dir = corpus_root().join(s.target);
        let file = dir.join(&s.name);
        wanted.insert(file.clone());
        if std::fs::read(&file).ok().as_deref() != Some(&s.bytes[..]) {
            if bless {
                std::fs::create_dir_all(&dir).unwrap();
                std::fs::write(&file, &s.bytes).unwrap();
            } else {
                drift.push(format!("{}/{}", s.target, s.name));
            }
        }
    }
    // Seeds that are no longer generated.
    for t in TARGETS {
        for (name, _) in corpus_files(&corpus_root().join(t.name)) {
            let file = corpus_root().join(t.name).join(&name);
            if name.starts_with("seed-") && !wanted.contains(&file) {
                if bless {
                    std::fs::remove_file(&file).unwrap();
                } else {
                    drift.push(format!("{}/{name} (stale)", t.name));
                }
            }
        }
    }
    assert!(
        drift.is_empty(),
        "seed corpus is out of date; run `LEEP_FUZZ_BLESS=1 cargo test -p leep-fuzz-targets \
         seeds_are_up_to_date`:\n{}",
        drift.join("\n")
    );
}

#[test]
fn every_target_has_a_corpus_and_a_fuzz_binary() {
    let fuzz_dir = corpus_root().join("..");
    for t in TARGETS {
        assert!(
            !corpus_files(&corpus_root().join(t.name)).is_empty(),
            "no corpus for {}",
            t.name
        );
        let bin = fuzz_dir.join("fuzz_targets").join(format!("{}.rs", t.name));
        let src =
            std::fs::read_to_string(&bin).unwrap_or_else(|_| panic!("missing {}", bin.display()));
        assert!(
            src.contains(&format!("check::{}(", t.name)),
            "{} does not call target {}",
            bin.display(),
            t.name
        );
    }
    // Every fuzz binary names a real target.
    for entry in std::fs::read_dir(fuzz_dir.join("fuzz_targets")).unwrap() {
        let name = entry.unwrap().file_name().to_string_lossy().into_owned();
        let stem = name.trim_end_matches(".rs");
        assert!(
            leep_fuzz_targets::target(stem).is_some(),
            "fuzz/fuzz_targets/{name} has no target"
        );
    }
}

#[test]
fn corpus_is_small() {
    let total: usize = TARGETS
        .iter()
        .flat_map(|t| corpus_files(&corpus_root().join(t.name)))
        .map(|(_, b)| b.len())
        .sum();
    assert!(
        total < 2 << 20,
        "corpus is {total} bytes; keep it under 2 MiB"
    );
}

#[test]
fn smoke_all_targets() {
    let iters = env_u64("LEEP_FUZZ_ITERS", 2000) as usize;
    let seed = env_u64("LEEP_FUZZ_SEED", 0x1EEB_F022);
    for t in TARGETS {
        let files = corpus_files(&corpus_root().join(t.name));
        let donors: Vec<Vec<u8>> = files.iter().map(|(_, b)| b.clone()).collect();
        for (name, bytes) in &files {
            run(t, bytes, &format!("corpus file {name}"));
        }
        // The router builds a repo per input, so it gets fewer rounds.
        let rounds = if t.name == "server_router" {
            iters / 10
        } else {
            iters
        };
        let mut rng = Rng(seed ^ t.name.len() as u64 ^ (t.name.as_bytes()[0] as u64) << 32 | 1);
        for i in 0..rounds {
            let base: &[u8] = if donors.is_empty() || rng.below(10) == 0 {
                &[]
            } else {
                &donors[rng.below(donors.len())]
            };
            let input = mutate(&mut rng, base, &donors);
            run(t, &input, &format!("mutation #{i} (seed {seed:#x})"));
        }
        for i in 0..rounds / 4 {
            let len = rng.below(48);
            let input: Vec<u8> = (0..len).map(|_| rng.next() as u8).collect();
            run(t, &input, &format!("random input #{i} (seed {seed:#x})"));
        }
    }
}
