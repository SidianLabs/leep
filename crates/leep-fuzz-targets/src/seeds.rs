//! The seed corpus, built from real encoders so every seed starts out valid
//! and the fuzzers mutate from realistic bytes. `fuzz/corpus/<target>/seed-*`
//! is this output; the `seeds_are_up_to_date` test fails when they drift, and
//! `LEEP_FUZZ_BLESS=1 cargo test -p leep-fuzz-targets` rewrites them.
//!
//! Files in the corpus that do not start with `seed-` (crash regressions,
//! hand-picked finds) are never touched here.

use std::collections::BTreeMap;

use leep_core::review::{Review, Verdict};
use leep_core::signature::{format_public_key, key_id, Signature};
use leep_core::{Changeset, FileChange, FileKind, Hash, RepoPath, TreeEntry, TreeNode};
use leep_derive::derived::{ChunkRef, HistoryEntry, HistoryNode, StoredHistoryNode};
use leep_derive::search::trigram::{extract, BlobRow, Bloom};
use serde_json::json;

use crate::api::{fixture, route_index, JSON_TYPES};
use crate::text::COMMENT_EXTENSIONS;

pub struct Seed {
    pub target: &'static str,
    pub name: String,
    pub bytes: Vec<u8>,
}

struct Out(Vec<Seed>);

impl Out {
    fn add(&mut self, target: &'static str, name: &str, bytes: impl Into<Vec<u8>>) {
        self.0.push(Seed {
            target,
            name: format!("seed-{name}"),
            bytes: bytes.into(),
        });
    }
}

fn path(p: &str) -> RepoPath {
    RepoPath::new(p).unwrap()
}

fn id(n: u8) -> leep_core::ChangesetId {
    leep_core::ChangesetId(Hash::of_blob(&[n]))
}

fn with_prefix(b: u8, rest: &[u8]) -> Vec<u8> {
    let mut v = vec![b];
    v.extend_from_slice(rest);
    v
}

/// A deterministic spread of trigram codes, enough to need a dense filter.
fn many_codes(n: u64) -> Vec<u32> {
    (0..n)
        .map(|i| {
            let mut z = i.wrapping_add(0x9E37_79B9_7F4A_7C15);
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            ((z ^ (z >> 31)) & 0xFF_FFFF) as u32
        })
        .collect()
}

fn objects(out: &mut Out) {
    let f = fixture();
    let merge = Changeset {
        parents: vec![f.tip.id(), f.root.id()],
        author: "Merger <m@example.com>".into(),
        timestamp: 1_760_000_200,
        message: "merge".into(),
        changes: BTreeMap::from([(
            path("bin/run"),
            FileChange::Modify {
                content: Hash::of_blob(b"#!/bin/sh\n"),
                kind: FileKind::Executable,
                size: 10,
            },
        )]),
        extra: BTreeMap::from([("review.note".into(), "ok".into())]),
    };
    let link = Changeset {
        parents: vec![],
        author: String::new(),
        timestamp: -1,
        message: String::new(),
        changes: BTreeMap::from([
            (
                path("link"),
                FileChange::Modify {
                    content: Hash::of_blob(b"target"),
                    kind: FileKind::Symlink,
                    size: 6,
                },
            ),
            (path("gone"), FileChange::Delete),
        ]),
        extra: BTreeMap::new(),
    };
    for (name, cs) in [
        ("root", &f.root),
        ("tip", &f.tip),
        ("merge", &merge),
        ("symlink-delete", &link),
    ] {
        out.add("changeset", name, cs.encode());
    }
    let empty = Changeset {
        parents: vec![],
        author: "x".into(),
        timestamp: 0,
        message: String::new(),
        changes: BTreeMap::new(),
        extra: BTreeMap::new(),
    };
    out.add("changeset", "empty", empty.encode());

    let mut tree = TreeNode::default();
    tree.entries.insert(
        "a.txt".into(),
        TreeEntry::File {
            content: Hash::of_blob(b"hello\n"),
            kind: FileKind::Regular,
            size: 6,
        },
    );
    tree.entries.insert(
        "run.sh".into(),
        TreeEntry::File {
            content: Hash::of_blob(b"#!/bin/sh\n"),
            kind: FileKind::Executable,
            size: 10,
        },
    );
    tree.entries.insert(
        "src".into(),
        TreeEntry::Dir {
            tree: TreeNode::empty_hash(),
        },
    );
    out.add("tree_node", "mixed", tree.encode());
    out.add("tree_node", "empty", TreeNode::default().encode());

    let mut hist = HistoryNode::default();
    hist.entries.insert(
        "a".into(),
        HistoryEntry::Leaf {
            changed: vec![id(1)],
        },
    );
    hist.entries.insert(
        "d".into(),
        HistoryEntry::Tree {
            changed: vec![id(1), id(2)],
            node: HistoryNode::empty_hash(),
        },
    );
    out.add(
        "history_node",
        "leaf-and-tree",
        StoredHistoryNode::Flat(hist).encode(),
    );
    out.add(
        "history_node",
        "empty",
        StoredHistoryNode::Flat(HistoryNode::default()).encode(),
    );
    let split = StoredHistoryNode::Split(vec![
        ChunkRef {
            first: "a".into(),
            node: HistoryNode::empty_hash(),
        },
        ChunkRef {
            first: "m".into(),
            node: HistoryNode::empty_hash(),
        },
    ]);
    out.add("history_node", "split", split.encode());

    let pk = [7u8; 32];
    let sig = Signature {
        key_id: key_id(&pk),
        public_key: pk,
        sig: [9; 64],
    };
    out.add("signature", "plain", sig.encode());

    for (name, verdict, comment) in [
        ("approve", Verdict::Approve, "looks good"),
        ("reject", Verdict::Reject, ""),
    ] {
        let r = Review {
            reviewer: "alice".into(),
            verdict,
            comment: comment.into(),
            timestamp: 1_760_000_300,
        };
        out.add("review", name, r.encode());
    }
}

fn rows(out: &mut Out) {
    let text = b"fn main() {\n    println!(\"hello, world\");\n}\n";
    out.add("blob_trigrams_row", "text", BlobRow::of(text).encode());
    out.add("blob_trigrams_row", "empty", BlobRow::of(b"").encode());
    out.add(
        "blob_trigrams_row",
        "binary",
        BlobRow::of(b"\0\x01\x02").encode(),
    );
    out.add(
        "blob_trigrams_row",
        "too-large",
        BlobRow::NotIndexed(leep_derive::search::trigram::Skip::TooLarge).encode(),
    );
    out.add(
        "blob_trigrams_row",
        "multibyte",
        BlobRow::of("héllo wörld ✓ 日本語".as_bytes()).encode(),
    );

    let mut sparse = Bloom::default();
    extract(text).into_iter().for_each(|c| sparse.insert(c));
    out.add("tree_trigrams_row", "sparse", sparse.encode());
    out.add("tree_trigrams_row", "empty", Bloom::default().encode());
    let mut dense = Bloom::default();
    many_codes(30_000).into_iter().for_each(|c| dense.insert(c));
    let enc = dense.encode();
    assert_eq!(enc[0], 1, "the seed must be a dense filter");
    out.add("tree_trigrams_row", "dense", enc);

    out.add("trigram_codec", "rust", text);
    out.add("trigram_codec", "binary", b"\xff\xfe\0abc");
    out.add("trigram_codec", "repeated", b"aaaaaaaaaaaaaaaaaaaaaaaa");

    let t = 1_760_000_000i64.to_be_bytes();
    for (i, kind) in ["claim", "condemned", "rescue"].iter().enumerate() {
        out.add("gc_rows", kind, with_prefix(i as u8, &t));
    }
    for (state, name) in ["deleting", "abandoned", "deleted"].iter().enumerate() {
        let mut v = t.to_vec();
        v.push(state as u8);
        out.add("gc_rows", name, with_prefix(3, &v));
    }
    out.add(
        "gc_rows",
        "negative-time",
        with_prefix(0, &(-5i64).to_be_bytes()),
    );

    let mut skip = Vec::new();
    for (n, generation) in [(1u8, 7u64), (2, 6), (3, 4)] {
        skip.extend_from_slice(id(n).0.as_bytes());
        skip.extend_from_slice(&generation.to_be_bytes());
    }
    out.add("skiplist_row", "three", skip);
    out.add("skiplist_row", "empty", b"");
}

fn api(out: &mut Out) {
    let f = fixture();
    let (tip, root) = (f.tip.id().to_string(), f.root.id().to_string());
    let blob = Hash::of_blob(f.blobs[0]).to_string();
    let key = format!(
        "ed25519:{}",
        "5866666666666666666666666666666666666666666666666666666666666666"
    );
    let sig = "ab".repeat(64);

    let bodies = [
        ("MissingBlobsRequest", json!({ "hashes": [blob] })),
        ("Changeset", serde_json::to_value(&f.tip).unwrap()),
        (
            "AddSignatureRequest",
            json!({ "public_key": key, "signature": sig }),
        ),
        (
            "AddReviewRequest",
            json!({ "verdict": "approve", "comment": "ok" }),
        ),
        (
            "CasRefRequest",
            json!({ "expected": null, "new": tip, "operation": "cas", "message": "m" }),
        ),
        (
            "MergeRequest",
            json!({ "parents": [tip, root], "message": "merge",
                    "authorship": { "author": "a", "timestamp": 1 }, "extra": { "k": "v" } }),
        ),
        ("RebaseRequest", json!({ "changeset": tip, "onto": root })),
        (
            "LandRequest",
            json!({ "changeset": tip, "target_ref": "main" }),
        ),
        ("ErrorResponse", json!({ "error": "bad" })),
        ("MissingBlobsResponse", json!({ "missing": [blob] })),
        ("CasRefResponse", json!({ "ok": true, "current": tip })),
        (
            "LandResponse",
            json!({ "status": "landed", "landed": tip, "rebased": false, "attempts": 1 }),
        ),
        ("MergeResponse", json!({ "status": "merged", "id": tip })),
        (
            "RebaseResponse",
            json!({ "status": "conflicts", "conflicts": [], "clean": {} }),
        ),
        (
            "SignaturesResponse",
            json!({ "signatures": [{ "key_id": "k", "name": "n", "public_key": key }], "verified": true }),
        ),
        (
            "SearchResponse",
            json!({ "matches": [{ "path": "a.txt", "line_number": 1, "line": "hello",
                                  "ranges": [{ "start": 0, "end": 5 }] }], "truncated": false }),
        ),
    ];
    for (name, body) in bodies {
        let idx = JSON_TYPES
            .iter()
            .position(|(n, _)| *n == name)
            .unwrap_or_else(|| panic!("unknown JSON type {name}"));
        out.add(
            "api_json",
            name,
            with_prefix(idx as u8, body.to_string().as_bytes()),
        );
    }

    // One seed per route with default values and a plausible body.
    let bodies: BTreeMap<&str, serde_json::Value> = BTreeMap::from([
        ("/v1/blobs/missing", json!({ "hashes": [blob] })),
        ("/v1/changesets", serde_json::to_value(&f.tip).unwrap()),
        (
            "/v1/changesets/{}/signatures",
            json!({ "public_key": key, "signature": sig }),
        ),
        (
            "/v1/changesets/{}/reviews",
            json!({ "verdict": "reject", "comment": "no" }),
        ),
        ("/v1/refs/{}", json!({ "expected": null, "new": tip })),
        (
            "/v1/merge",
            json!({ "parents": [tip, root], "message": "m", "authorship": { "author": "a" } }),
        ),
        ("/v1/rebase", json!({ "changeset": tip, "onto": root })),
        (
            "/v1/land",
            json!({ "changeset": root, "target_ref": "main" }),
        ),
    ]);
    let routes = [
        ("GET", "/v1/health"),
        ("POST", "/v1/blobs/missing"),
        ("GET", "/v1/blobs/{}"),
        ("PUT", "/v1/blobs/{}"),
        ("POST", "/v1/changesets"),
        ("GET", "/v1/changesets/{}"),
        ("GET", "/v1/changesets/{}/signatures"),
        ("POST", "/v1/changesets/{}/signatures"),
        ("GET", "/v1/changesets/{}/reviews"),
        ("POST", "/v1/changesets/{}/reviews"),
        ("GET", "/v1/changesets/{}/review-status"),
        ("GET", "/v1/refs"),
        ("GET", "/v1/refs/{}"),
        ("POST", "/v1/refs/{}"),
        ("GET", "/v1/reflog?before={}&limit={}"),
        ("GET", "/v1/reflog/{}"),
        ("GET", "/v1/resolve/{}"),
        ("GET", "/v1/tree/{}?path={}"),
        ("GET", "/v1/files/{}?prefix={}"),
        ("GET", "/v1/file/{}?path={}"),
        (
            "GET",
            "/v1/log/{}?path={}&limit={}&follow={}&author={}&extra={}",
        ),
        ("GET", "/v1/last-change/{}?path={}"),
        (
            "GET",
            "/v1/diff?from={}&to={}&prefix={}&renames={}&rename_threshold={}",
        ),
        ("GET", "/v1/blame/{}?path={}"),
        ("GET", "/v1/provenance/{}?path={}&limit={}"),
        (
            "GET",
            "/v1/search/{}?q={}&regex={}&i={}&path={}&glob={}&limit={}",
        ),
        ("GET", "/v1/merge-base?a={}&b={}"),
        ("POST", "/v1/merge"),
        ("POST", "/v1/rebase"),
        ("POST", "/v1/land"),
    ];
    for (method, tpl) in routes {
        let mut v = vec![route_index(method, tpl) as u8, 0xFF];
        match (method, bodies.get(tpl)) {
            (_, Some(b)) => v.extend_from_slice(b.to_string().as_bytes()),
            ("PUT", _) => v.extend_from_slice(f.blobs[0]),
            _ => {}
        }
        let name = format!(
            "{:02}-{method}{}",
            route_index(method, tpl),
            tpl.split(['{', '?'])
                .next()
                .unwrap()
                .trim_end_matches('/')
                .replace('/', "-")
        );
        out.add("server_router", &name, v);
    }
    // Holes filled from the input: odd values, fixture ids and raw requests.
    let log = route_index(
        "GET",
        "/v1/log/{}?path={}&limit={}&follow={}&author={}&extra={}",
    );
    out.add(
        "server_router",
        "log-odd-values",
        [&[log as u8][..], b"\x00\x00\xff\x00maybe\x00\x00:"].concat(),
    );
    let tree = route_index("GET", "/v1/tree/{}?path={}");
    out.add(
        "server_router",
        "tree-root-id",
        [&[tree as u8, 1, 0][..], b"..%/x"].concat(),
    );
    out.add(
        "server_router",
        "raw-unknown",
        [&[0x80][..], b"/v1/nope"].concat(),
    );
    out.add(
        "server_router",
        "raw-long-ref",
        [&[0x81][..], b"/v1/refs/a%00b%2e%2e/..", &[0xFF], b"{}"].concat(),
    );
}

fn files(out: &mut Out) {
    out.add(
        "owners",
        "plain",
        "alice\nbob  # reviewer\n\n# nothing\nnoparent\n",
    );
    out.add(
        "owners",
        "bom-crlf",
        "\u{feff}carol\r\n  dave smith \r\n#x\r\n",
    );
    out.add("owners", "empty", "");

    out.add(
        "review_policy",
        "full",
        r#"{ "refs": ["main"], "auto_approve": ["comment_only", "whitespace_only", "docs_only"], "docs_globs": ["**/*.md", "docs/**"] }"#,
    );
    out.add("review_policy", "empty", "{}");
    out.add(
        "review_policy",
        "bad-rule",
        r#"{ "auto_approve": ["nope"] }"#,
    );
    out.add(
        "review_policy",
        "unknown-field",
        r#"{ "refs": [], "extra": 1 }"#,
    );

    let key = "ed25519:5866666666666666666666666666666666666666666666666666666666666666";
    out.add(
        "signing_keys",
        "one-key",
        format!(r#"{{ "keys": [ {{ "name": "ada", "public_key": "{key}" }} ] }}"#),
    );
    out.add("signing_keys", "empty", r#"{ "keys": [] }"#);
    out.add(
        "signing_keys",
        "short-key",
        r#"{ "keys": [ { "name": "x", "public_key": "ed25519:00" } ] }"#,
    );
    out.add(
        "signing_keys",
        "weak-key",
        format!(
            r#"{{ "keys": [ {{ "name": "w", "public_key": "{}" }} ] }}"#,
            format_public_key(&{
                let mut k = [0u8; 32];
                k[0] = 1;
                k
            })
        ),
    );

    let digest = format!("blake3:{}", Hash::of_blob(b"token").to_hex());
    out.add(
        "auth_tokens",
        "two-roles",
        format!(
            r#"{{ "tokens": [ {{ "name": "ci", "role": "write", "hash": "{digest}" }}, {{ "name": "viewer", "role": "read", "hash": "{digest}" }} ] }}"#
        ),
    );
    out.add("auth_tokens", "empty", r#"{ "tokens": [] }"#);
    out.add(
        "auth_tokens",
        "plaintext",
        r#"{ "tokens": [ { "name": "x", "role": "write", "hash": "secret" } ] }"#,
    );
}

/// One small, awkward snippet per language: markers inside strings, nested
/// and unterminated comments, raw strings, heredocs and templates.
fn snippets() -> BTreeMap<&'static str, &'static str> {
    BTreeMap::from([
        ("rs", "// c\nfn f<'a>(x: &'a str) -> char { /* /* nested */ */ let s = r#\"// not\"#; '\\'' }\n"),
        ("c", "int a = 1; /* c */ char *s = \"/* not */\"; // end\n#define X 1 // m\n"),
        ("h", "#ifndef H\n#define H /* guard */\n#endif\n"),
        ("cc", "auto s = R\"(// raw)\"; // c\n"),
        ("cpp", "int main() { return 0; } // done\n/* open"),
        ("hpp", "template <class T> struct S {}; /* c */\n"),
        ("java", "class A { String s = \"a\\u0022 // x\"; } // c\n"),
        ("js", "const r = a / b / c; // c\nconst t = `${x} // y`; /* z */\n"),
        ("ts", "let x: number = 1; // c\nconst re = /[/]*/g;\n"),
        ("jsx", "const e = <div>{/* c */}// text</div>;\n"),
        ("tsx", "const e = <A b=\"//\" />; // c\n"),
        ("go", "package main // c\nvar s = `raw // not`\n//go:build linux\n"),
        ("swift", "let s = \"\\(1) // x\" // c\n/* a /* b */ c */\n"),
        ("kt", "val s = \"\"\"raw // not\"\"\" // c\n"),
        ("scala", "val s = \"a\" // c\n/** doc */ object O\n"),
        ("cs", "var s = @\"// verbatim\"; // c\nvar i = $\"{x}\";\n"),
        ("py", "# c\ns = '# not'\nt = \"\"\"doc # not\"\"\"  # c\nif x:\n    y = 1  # tail\n"),
        ("sh", "echo '# not' # c\ncat <<EOF\n# not\nEOF\n"),
        ("bash", "x=${y#*#} # c\necho \"$((1#1))\"\n"),
        ("rb", "puts \"a#{b}\" # c\n=begin\nblock\n=end\n"),
        ("pl", "my $s = 'a#b'; # c\n=pod\ndoc\n=cut\n"),
        ("toml", "a = \"x # y\" # c\nb = '''m\n# not\n'''\n"),
        ("yaml", "a: 'x # y' # c\nb: |\n  # literal block\n"),
        ("yml", "- a # c\n- \"b # d\"\n"),
    ])
}

fn text(out: &mut Out) {
    for (n, ext) in COMMENT_EXTENSIONS.iter().enumerate() {
        let snippet = snippets()[ext];
        // The same text twice, then a comment-only edit of it.
        let edited = format!("{snippet}\n");
        let mut v = vec![n as u8];
        v.extend_from_slice(snippet.as_bytes());
        v.push(0);
        v.extend_from_slice(edited.as_bytes());
        out.add("comment_lexer", ext, v);
    }

    // Deleted and added counts, threshold, pair limit; (path, kind, body)
    // per file; then the bodies.
    let bodies = b"one\ntwo\nthree\nfour\nfive\nsix\nseven\neight\n".to_vec();
    let mut similar = vec![1, 1, 50, 255, 0, 0, 0, 3, 0, 1];
    similar.extend_from_slice(&bodies);
    out.add("rename_detection", "similar", similar);
    let mut exact = vec![2, 2, 50, 255, 0, 0, 0, 1, 0, 0, 2, 1, 1, 3, 0, 1];
    exact.extend_from_slice(&bodies);
    out.add("rename_detection", "exact-and-mode", exact);
    let mut symlinks = vec![2, 2, 0, 255, 0, 2, 0, 1, 2, 0, 2, 2, 1, 3, 2, 1];
    symlinks.extend_from_slice(&bodies);
    out.add("rename_detection", "symlinks", symlinks);
    let mut limited = vec![
        3, 3, 10, 4, 0, 0, 0, 1, 0, 1, 2, 0, 2, 4, 0, 2, 3, 0, 3, 5, 0, 3,
    ];
    limited.extend_from_slice(&bodies);
    out.add("rename_detection", "pair-limit", limited);

    for (name, s) in [
        ("branch", "feature/login"),
        ("tag", "tags/v1.0"),
        ("dotdot", "a..b"),
        ("lock", "x.lock"),
        ("at-brace", "a@{1}"),
        ("hex", &"ab".repeat(32)),
        ("space", "a b"),
        ("unicode", "caf\u{e9}"),
    ] {
        out.add("ref_name", name, s);
    }
    for (name, s) in [
        ("file", "src/lib.rs"),
        ("top", "README"),
        ("dotdot", "a/../b"),
        ("slashes", "/a//b/"),
        ("nul", "a\0b"),
        ("unicode", "d\u{e9}j\u{e0}/vu"),
    ] {
        out.add("repo_path", name, s);
    }

    for (name, flags, pattern, haystack) in [
        ("literal", 0u8, "needle", "hay needle hay\n"),
        ("literal-ci", 2, "NeEdLe", "hay needle hay\n"),
        ("regex-alt", 1, "foo|ba[rz]+", "xbazz\nfoo\n"),
        ("regex-class-ci", 3, "[a-c]x{2,3}d", "AXXD\n"),
        (
            "regex-anchor",
            1,
            "^fn \\w+\\(",
            "pub fn main() {}\nfn run() {}\n",
        ),
        (
            "regex-unicode",
            3,
            "stra\u{df}e",
            "STRASSE strasse stra\u{df}e\n",
        ),
        ("regex-group", 1, "(ab){3}c", "ababab c ababc abababc\n"),
    ] {
        let mut v = vec![flags];
        v.extend_from_slice(pattern.as_bytes());
        v.push(0);
        v.extend_from_slice(haystack.as_bytes());
        out.add("search_plan", name, v);
    }

    let hash = Hash::of_blob(b"x").to_hex();
    let key = "ed25519:5866666666666666666666666666666666666666666666666666666666666666";
    for (name, s) in [
        ("duration-days", "7d".to_string()),
        ("duration-bare", "90".to_string()),
        ("duration-huge", "99999999999999999999d".to_string()),
        ("extra-kv", "agent.model:test:x".to_string()),
        ("hash", hash.clone()),
        ("hash-upper", hash.to_uppercase()),
        ("public-key", key.to_string()),
        ("signature", "cd".repeat(64)),
    ] {
        out.add("arg_parsers", name, s);
    }
}

/// Every seed, in a stable order.
pub fn all() -> Vec<Seed> {
    let mut out = Out(Vec::new());
    objects(&mut out);
    rows(&mut out);
    api(&mut out);
    files(&mut out);
    text(&mut out);
    out.0
}
