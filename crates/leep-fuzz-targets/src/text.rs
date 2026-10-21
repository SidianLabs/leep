//! Text-level logic: the comment lexer, rename detection, names and paths, the
//! search planner, and the small parsers behind CLI flags.

use std::collections::{BTreeSet, HashMap};

use leep_core::api::FileInfo;
use leep_core::rename::{detect_renames, Detected, RenameOptions};
use leep_core::signature::{format_public_key, parse_public_key, parse_signature_hex};
use leep_core::{refname, ChangesetId, FileKind, Hash, RepoPath};
use leep_derive::review::{comment_only, whitespace_only};
use leep_derive::search::plan::{Compiled, MAX_LINE_BYTES};
use leep_derive::search::trigram::{extract, BlobRow, Bloom};
use leep_derive::{gc, strip_comments, ExtraMatch};

use crate::lossy;

/// Every extension the lexer knows.
pub(crate) const COMMENT_EXTENSIONS: &[&str] = &[
    "rs", "c", "h", "cc", "cpp", "hpp", "java", "js", "ts", "jsx", "tsx", "go", "swift", "kt",
    "scala", "cs", "py", "sh", "bash", "rb", "pl", "toml", "yaml", "yml",
];

/// Byte 0 picks the language. The rest is one text, or two separated by a NUL
/// (the old and new side of a change).
pub fn comment_lexer(data: &[u8]) {
    let Some((&sel, rest)) = data.split_first() else {
        return;
    };
    let ext = COMMENT_EXTENSIONS[sel as usize % COMMENT_EXTENSIONS.len()];
    let path = format!("dir/file.{ext}");
    let (a, b) = match rest.iter().position(|&c| c == 0) {
        Some(i) => (&rest[..i], &rest[i + 1..]),
        None => (rest, rest),
    };
    let (ta, tb) = (lossy(a), lossy(b));

    let sa = strip_comments(ext, &ta);
    assert_eq!(
        strip_comments(&ext.to_uppercase(), &ta),
        sa,
        "extension case"
    );
    assert_eq!(
        comment_only(&path, ta.as_bytes(), ta.as_bytes()),
        sa.is_some(),
        "a text is comment-only against itself exactly when the lexer reads it"
    );
    assert!(
        !comment_only(&format!(".{ext}"), ta.as_bytes(), ta.as_bytes()),
        "a bare extension names no language"
    );
    if let Some(s) = &sa {
        assert!(!s.contains('\0'));
        assert!(
            s.chars().count() <= ta.chars().count(),
            "stripping never adds characters"
        );
        // What is left reads the same way: no comment and no string is
        // created or lost by removing comments.
        assert_eq!(strip_comments(ext, s).as_ref(), Some(s), "idempotent");
    }
    assert_eq!(
        comment_only(&path, ta.as_bytes(), tb.as_bytes()),
        comment_only(&path, tb.as_bytes(), ta.as_bytes()),
        "symmetric"
    );
    if comment_only(&path, ta.as_bytes(), tb.as_bytes()) {
        assert_eq!(sa, strip_comments(ext, &tb));
    }

    assert!(whitespace_only(a, a) || std::str::from_utf8(a).is_err());
    assert_eq!(whitespace_only(a, b), whitespace_only(b, a), "symmetric");
    if std::str::from_utf8(a).is_ok() {
        let mut padded = a.to_vec();
        padded.extend_from_slice(b"  \t\r\n\n \n");
        assert!(whitespace_only(a, &padded), "trailing whitespace is free");
    }
}

fn file(path: &str, body: &[u8], kind: FileKind) -> FileInfo {
    FileInfo {
        path: RepoPath::new(path).unwrap(),
        content: Hash::of_blob(body),
        kind,
        size: body.len() as u64,
    }
}

/// Byte 0 and 1 give the number of deleted and added files, byte 2 the
/// threshold, byte 3 the pair limit. Three bytes per file follow (path, kind,
/// body), then the remaining input is cut into four bodies.
pub fn rename_detection(data: &[u8]) {
    const PATHS: [&str; 8] = [
        "a.txt",
        "b.txt",
        "src/a.rs",
        "src/b.rs",
        "docs/x/readme",
        "z",
        "src/z.rs",
        "n/o/p/q",
    ];
    if data.len() < 4 {
        return;
    }
    let (nd, na) = (data[0] as usize % 7, data[1] as usize % 7);
    let threshold = data[2];
    let max_pairs = if data[3] == 255 {
        u64::MAX
    } else {
        u64::from(data[3])
    };
    let specs = &data[4..];
    let split = (3 * (nd + na)).min(specs.len());
    let (specs, bodies) = specs.split_at(split);
    let chunk = bodies.len().div_ceil(4).max(1);
    let body = |i: u8| -> Vec<u8> {
        bodies
            .chunks(chunk)
            .nth(i as usize % 4)
            .unwrap_or(&[])
            .to_vec()
    };

    let mut used = BTreeSet::new();
    let mut deleted = Vec::new();
    let mut added = Vec::new();
    let mut by_hash: HashMap<Hash, Vec<u8>> = HashMap::new();
    for (n, s) in specs.chunks_exact(3).enumerate() {
        let path = PATHS[s[0] as usize % PATHS.len()];
        let kind = [FileKind::Regular, FileKind::Executable, FileKind::Symlink][s[1] as usize % 3];
        let contents = body(s[2]);
        // A path is on one side of a diff only, once.
        if !used.insert(path) {
            continue;
        }
        let f = file(path, &contents, kind);
        by_hash.insert(f.content, contents);
        if n < nd {
            deleted.push(f);
        } else {
            added.push(f);
        }
    }

    let opts = RenameOptions {
        threshold,
        max_pairs,
        max_bytes: u64::MAX,
    };
    let mut asked: HashMap<Hash, u32> = HashMap::new();
    let run = |del: Vec<FileInfo>, add: Vec<FileInfo>, asked: &mut HashMap<Hash, u32>| {
        detect_renames(del, add, opts, |h| {
            *asked.entry(*h).or_default() += 1;
            by_hash.get(h).cloned()
        })
    };
    let found = run(deleted.clone(), added.clone(), &mut asked);
    assert!(asked.values().all(|&n| n == 1), "each body is read once");

    let (mut from, mut to) = (BTreeSet::new(), BTreeSet::new());
    for r in &found.renames {
        let d = deleted
            .iter()
            .find(|f| f.path == r.from)
            .expect("from was deleted");
        let a = added.iter().find(|f| f.path == r.to).expect("to was added");
        assert!(from.insert(r.from.clone()), "{} renamed twice", r.from);
        assert!(to.insert(r.to.clone()), "{} is the target twice", r.to);
        assert!(r.similarity <= 100);
        if r.exact {
            assert_eq!((d.content, d.kind), (a.content, a.kind));
            assert_eq!(r.similarity, 100);
        } else if r.similarity == 100 && d.content == a.content {
            assert_ne!(d.kind, a.kind);
        } else {
            assert!(r.similarity >= threshold.min(100));
            assert!(d.kind != FileKind::Symlink && a.kind != FileKind::Symlink);
        }
    }
    assert!(found
        .renames
        .windows(2)
        .all(|w| (&w[0].to, &w[0].from) < (&w[1].to, &w[1].from)));

    // Inputs may arrive in any order.
    deleted.reverse();
    added.reverse();
    let swapped: Detected = run(deleted, added, &mut HashMap::new());
    assert_eq!(swapped, found, "input order must not matter");
}

pub fn ref_name(data: &[u8]) {
    let name = lossy(data);
    let _ = refname::is_tag(&name);
    if refname::validate(&name).is_err() {
        return;
    }
    assert!(!name.is_empty() && name.len() <= refname::MAX_LEN);
    assert!(name.chars().all(|c| c.is_ascii_graphic()));
    for bad in [
        "..", "@{", "//", "\\", "~", "^", ":", "?", "*", "[", ".lock/",
    ] {
        assert!(!name.contains(bad), "valid name contains {bad:?}");
    }
    assert!(!name.starts_with('/') && !name.ends_with('/') && !name.ends_with(".lock"));
    assert_ne!(name, "@");
    for part in name.split('/') {
        assert!(!part.is_empty() && !part.starts_with(['.', '-']));
    }
    // A tag is a ref like any other, so its short name must stay valid.
    if let Some(short) = name.strip_prefix(refname::TAGS_PREFIX) {
        assert_eq!(refname::tag_ref(short), *name);
    }
}

pub fn repo_path(data: &[u8]) {
    let s = lossy(data);
    let Ok(p) = RepoPath::new(s.as_ref()) else {
        return;
    };
    assert_eq!(p.as_str(), s);
    assert!(!s.contains('\0') && !s.starts_with('/') && !s.ends_with('/'));
    assert!(p
        .components()
        .all(|c| !c.is_empty() && c != "." && c != ".."));
    assert_eq!(p.components().last(), Some(p.file_name()));
    match p.parent() {
        Some(dir) => {
            assert!(p.is_within(dir));
            assert_eq!(format!("{dir}/{}", p.file_name()), s);
            assert!(RepoPath::new(dir).is_ok(), "a parent is a path");
        }
        None => assert_eq!(p.file_name(), s),
    }
    assert!(p.is_within("") && p.is_within(p.as_str()));
    assert!(!p.is_within(&format!("{s}/x")));
    let json = serde_json::to_string(&p).unwrap();
    assert_eq!(serde_json::from_str::<RepoPath>(&json).unwrap(), p);
    let bin = postcard::to_stdvec(&p).unwrap();
    assert_eq!(postcard::from_bytes::<RepoPath>(&bin).unwrap(), p);
}

/// Byte 0: bit 0 regex, bit 1 case-insensitive. Then the pattern, and after a
/// NUL the text to search (the pattern itself if there is no NUL).
///
/// The planner's contract: the trigram requirement is necessary. A text with
/// a matching line satisfies it, by its blob row and by the directory filter
/// its tree would get, so skipping files that fail it loses no match.
pub fn search_plan(data: &[u8]) {
    let Some((&flags, rest)) = data.split_first() else {
        return;
    };
    let (regex, ci) = (flags & 1 != 0, flags & 2 != 0);
    let (pat, text) = match rest.iter().position(|&c| c == 0) {
        Some(i) => (&rest[..i], &rest[i + 1..]),
        None => (rest, rest),
    };
    let (pat, text) = (lossy(pat), lossy(text));
    let Ok(c) = Compiled::new(&pat, regex, ci) else {
        return;
    };
    let path = RepoPath::new("f.txt").unwrap();
    let mut out = Vec::new();
    c.scan(&path, &text, 1000, &mut out);
    for m in &out {
        assert!(m.line_number >= 1 && m.line.len() <= MAX_LINE_BYTES);
        for r in &m.ranges {
            assert!(r.start < r.end && r.end as usize <= m.line.len());
            assert!(m.line.is_char_boundary(r.start as usize));
            assert!(m.line.is_char_boundary(r.end as usize));
        }
    }
    if out.is_empty() {
        return;
    }
    let codes = extract(text.as_bytes());
    assert!(
        c.required.eval(&|t| codes.binary_search(&t).is_ok()),
        "pattern {pat:?} matched but its trigram requirement fails"
    );
    if let BlobRow::Trigrams(_) = BlobRow::of(text.as_bytes()) {
        let row = BlobRow::of(text.as_bytes()).encode();
        let present = c.probe.blob(&row).unwrap().unwrap();
        assert!(c.required.eval(&|t| present.has(t)), "blob row of {pat:?}");
        assert_eq!(
            c.admits_blob(&row).unwrap(),
            Some(true),
            "early stop, blob row of {pat:?}"
        );
    }
    let mut bloom = Bloom::default();
    codes.iter().for_each(|&t| bloom.insert(t));
    let present = c.probe.tree(&bloom.encode()).unwrap();
    assert!(
        c.required.eval(&|t| present.has(t)),
        "tree filter of {pat:?}"
    );
    assert!(
        c.admits_tree(&bloom.encode()).unwrap(),
        "early stop, tree filter of {pat:?}"
    );
}

/// The parsers behind command-line flags and identifiers.
pub fn arg_parsers(data: &[u8]) {
    let s = lossy(data);

    if let Ok(d) = gc::parse_duration(&s) {
        let t = s.trim();
        let digits = t.trim_end_matches(['s', 'm', 'h', 'd']);
        let n: u64 = digits
            .parse()
            .expect("an accepted duration starts with a number");
        let unit = match &t[digits.len()..] {
            "" | "s" => 1,
            "m" => 60,
            "h" => 3600,
            _ => 86400,
        };
        assert_eq!(d.as_secs(), n * unit);
    }

    if let Ok(m) = ExtraMatch::parse(&s) {
        assert!(!m.key.is_empty());
        let again = match &m.value {
            Some(v) => format!("{}:{v}", m.key),
            None => m.key.clone(),
        };
        assert_eq!(ExtraMatch::parse(&again).unwrap(), m);
    }

    if let Ok(h) = s.parse::<Hash>() {
        assert_eq!(h.to_hex(), s.to_ascii_lowercase());
        assert_eq!(s.parse::<ChangesetId>().unwrap().0, h);
    } else {
        assert!(s.parse::<ChangesetId>().is_err());
    }

    if let Ok(k) = parse_public_key(&s) {
        assert!(format_public_key(&k).eq_ignore_ascii_case(&s));
    }
    if let Ok(sig) = parse_signature_hex(&s) {
        assert!(leep_core::signature::Signature {
            key_id: String::new(),
            public_key: [0; 32],
            sig,
        }
        .encode()
        .ends_with(&sig));
    }
}
