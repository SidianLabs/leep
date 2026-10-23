//! Tools against a real in-process `leep-server` with memory stores.

mod common;

use common::{call, config, serve, start_server, text};
use leep_mcp::Server;
use leep_server::auth::{self, Auth};
use serde_json::{json, Value};

fn data(r: &Value) -> Value {
    assert_eq!(r["isError"], false, "{}", text(r));
    r["structuredContent"].clone()
}

fn commit(s: &Server, args: Value) -> Value {
    call(s, "leep_commit_and_land", args)
}

#[test]
fn commit_land_and_read_back() {
    let s = Server::new(config(&start_server()));

    // Empty repository: the first commit becomes the root of main.
    let r = commit(
        &s,
        json!({
            "message": "initial",
            "changes": [
                { "path": "a.txt", "content": "one\ntwo\nthree\n" },
                { "path": "src/run.sh", "content": "#!/bin/sh\n", "executable": true },
            ],
            "prompt": "make files", "tests": "none run",
        }),
    );
    let first = data(&r);
    assert_eq!(first["status"], "landed");
    let first_id = first["landed"].as_str().unwrap().to_string();

    let r = commit(
        &s,
        json!({ "message": "shout", "changes": [{ "path": "a.txt", "content": "one\nTWO\nthree\n" }] }),
    );
    let second_id = data(&r)["landed"].as_str().unwrap().to_string();

    let refs = data(&call(&s, "leep_refs", json!({})));
    assert_eq!(refs["refs"]["main"], second_id);

    let log = data(&call(&s, "leep_log", json!({})));
    let entries = log["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0]["id"], second_id);
    assert_eq!(entries[0]["message"], "shout");
    assert_eq!(entries[0]["files_changed"], 1);
    assert_eq!(entries[0]["author"], "tester");
    assert_eq!(entries[1]["agent"]["model"], "test-model");
    assert_eq!(entries[1]["agent"]["session"], "sess-1");
    assert_eq!(entries[1]["agent"]["prompt"], "make files");
    assert_eq!(entries[1]["agent"]["tests"], "none run");

    let by_path = data(&call(&s, "leep_log", json!({ "path": "src/run.sh" })));
    assert_eq!(by_path["entries"].as_array().unwrap().len(), 1);

    // One entry per page: the cursor brings the next page, then the end.
    assert!(log.get("next").is_none());
    let page = data(&call(&s, "leep_log", json!({ "limit": 1 })));
    assert_eq!(page["entries"][0]["id"], second_id);
    let next = page["next"].as_str().unwrap();
    let rest = data(&call(
        &s,
        "leep_log",
        json!({ "limit": 1, "rev": page["from"], "cursor": next }),
    ));
    assert_eq!(rest["entries"][0]["id"], first_id);
    assert!(rest.get("next").is_none());
    let bad = call(&s, "leep_log", json!({ "cursor": "nonsense" }));
    assert_eq!(bad["isError"], true);

    let show = data(&call(&s, "leep_show", json!({ "rev": first_id })));
    assert_eq!(show["agent"]["model"], "test-model");
    let mut changes: Vec<String> = show["changes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| {
            format!(
                "{} {}",
                c["status"].as_str().unwrap(),
                c["path"].as_str().unwrap()
            )
        })
        .collect();
    changes.sort();
    assert_eq!(changes, ["A a.txt", "A src/run.sh"]);
    let show2 = data(&call(&s, "leep_show", json!({ "rev": second_id })));
    assert_eq!(
        show2["changes"],
        json!([{ "status": "M", "path": "a.txt" }])
    );

    let files = data(&call(&s, "leep_list_files", json!({})));
    assert_eq!(files["total"], 2);
    let files = data(&call(&s, "leep_list_files", json!({ "prefix": "src" })));
    assert_eq!(files["files"][0]["path"], "src/run.sh");
    assert_eq!(files["files"][0]["kind"], "executable");

    let r = call(&s, "leep_read_file", json!({ "path": "a.txt" }));
    assert_eq!(text(&r), "one\nTWO\nthree\n");
    let r = call(
        &s,
        "leep_read_file",
        json!({ "path": "a.txt", "rev": first_id }),
    );
    assert_eq!(text(&r), "one\ntwo\nthree\n");
    let r = call(&s, "leep_read_file", json!({ "path": "missing.txt" }));
    assert_eq!(r["isError"], true);
    assert!(text(&r).contains("404"), "{}", text(&r));
    // The server's id for the failed request, for its logs; a new one per call.
    let id_of = |r: &Value| {
        let t = text(r);
        let (_, rest) = t
            .split_once("(server request id: ")
            .unwrap_or_else(|| panic!("no request id in {t}"));
        rest[..32].to_string()
    };
    let again = call(&s, "leep_read_file", json!({ "path": "missing.txt" }));
    assert_ne!(id_of(&r), id_of(&again));
    let r = call(
        &s,
        "leep_read_file",
        json!({ "path": "a.txt", "rev": "nosuchref" }),
    );
    assert_eq!(r["isError"], true);
    assert!(text(&r).contains("nosuchref"));

    let d = text(&call(&s, "leep_diff", json!({ "to": "main" })));
    assert!(d.contains("-two\n") && d.contains("+TWO\n"), "{d}");
    assert!(
        d.contains("--- a/a.txt") && d.contains("+++ b/a.txt"),
        "{d}"
    );
    let d = text(&call(
        &s,
        "leep_diff",
        json!({ "from": first_id, "to": second_id, "prefix": "src" }),
    ));
    assert_eq!(d, "No differences.");
    let d = text(&call(&s, "leep_diff", json!({ "to": first_id })));
    assert!(
        d.contains("--- /dev/null") && d.contains("+#!/bin/sh"),
        "{d}"
    );

    let b = text(&call(&s, "leep_blame", json!({ "path": "a.txt" })));
    let lines: Vec<&str> = b.lines().skip(1).collect();
    assert_eq!(lines.len(), 3);
    assert!(
        lines[0].starts_with(&first_id[..12]) && lines[0].ends_with(": one"),
        "{b}"
    );
    assert!(
        lines[1].starts_with(&second_id[..12]) && lines[1].ends_with(": TWO"),
        "{b}"
    );
}

#[test]
fn delete_and_large_file_cap() {
    let s = Server::new(config(&start_server()));
    let big = "x".repeat(300 * 1024);
    data(&commit(
        &s,
        json!({ "message": "m", "changes": [
            { "path": "big.txt", "content": big }, { "path": "gone.txt", "content": "bye\n" } ] }),
    ));
    let r = call(&s, "leep_read_file", json!({ "path": "big.txt" }));
    let t = text(&r);
    assert!(t.contains("truncated") && t.contains("256 KiB"));
    assert!(t.len() < 300 * 1024);
    data(&commit(
        &s,
        json!({ "message": "rm", "changes": [{ "path": "gone.txt", "delete": true }] }),
    ));
    let show = data(&call(&s, "leep_show", json!({ "rev": "main" })));
    assert_eq!(
        show["changes"],
        json!([{ "status": "D", "path": "gone.txt" }])
    );
}

#[test]
fn conflict_is_reported_then_resolved_retry_lands() {
    let s = Server::new(config(&start_server()));
    data(&commit(
        &s,
        json!({ "message": "base", "changes": [{ "path": "a.txt", "content": "one\ntwo\nthree\n" },
                                               { "path": "b.txt", "content": "keep\n" }] }),
    ));
    let base = data(&call(&s, "leep_refs", json!({})))["refs"]["main"]
        .as_str()
        .unwrap()
        .to_string();

    // Agent A lands first.
    let a = data(&commit(
        &s,
        json!({ "base": base, "message": "a", "changes": [{ "path": "a.txt", "content": "uno\ntwo\nthree\n" }] }),
    ));
    let head = a["landed"].as_str().unwrap().to_string();

    // Agent B edits the same line from the same base.
    let r = commit(
        &s,
        json!({ "base": base, "message": "b", "changes": [
            { "path": "a.txt", "content": "eins\ntwo\nthree\n" },
            { "path": "c.txt", "content": "new\n" } ] }),
    );
    assert_eq!(r["isError"], true);
    assert!(text(&r).starts_with("NOT LANDED"));
    let v = &r["structuredContent"];
    assert_eq!(v["status"], "conflict");
    assert_eq!(v["onto"], head);
    let c = &v["conflicts"][0];
    assert_eq!(c["path"], "a.txt");
    assert_eq!(c["base"]["text"], "one\ntwo\nthree\n");
    assert_eq!(c["ours"]["text"], "uno\ntwo\nthree\n");
    assert_eq!(c["theirs"]["text"], "eins\ntwo\nthree\n");
    assert!(c["merged_with_markers"]
        .as_str()
        .unwrap()
        .contains("<<<<<<<"));
    assert_eq!(v["clean_paths"][0]["path"], "c.txt");
    assert!(v["guidance"].as_str().unwrap().contains(&head));

    // main did not move.
    let refs = data(&call(&s, "leep_refs", json!({})));
    assert_eq!(refs["refs"]["main"], head);
    assert_eq!(
        text(&call(&s, "leep_read_file", json!({ "path": "a.txt" }))),
        "uno\ntwo\nthree\n"
    );
    assert_eq!(
        call(&s, "leep_read_file", json!({ "path": "c.txt" }))["isError"],
        true
    );

    // Retry with resolved content on the new head.
    let ok = data(&commit(
        &s,
        json!({ "base": head, "message": "b resolved", "changes": [
            { "path": "a.txt", "content": "uno eins\ntwo\nthree\n" },
            { "path": "c.txt", "content": "new\n" } ] }),
    ));
    assert_eq!(ok["status"], "landed");
    assert_eq!(ok["rebased"], false);
    assert_eq!(
        text(&call(&s, "leep_read_file", json!({ "path": "a.txt" }))),
        "uno eins\ntwo\nthree\n"
    );
    assert_eq!(
        text(&call(&s, "leep_read_file", json!({ "path": "c.txt" }))),
        "new\n"
    );
}

#[test]
fn non_overlapping_edits_rebase_automatically() {
    let s = Server::new(config(&start_server()));
    data(&commit(
        &s,
        json!({ "message": "base", "changes": [{ "path": "a.txt", "content": "1\n2\n3\n" }] }),
    ));
    let base = data(&call(&s, "leep_refs", json!({})))["refs"]["main"]
        .as_str()
        .unwrap()
        .to_string();
    data(&commit(
        &s,
        json!({ "base": base, "message": "x", "changes": [{ "path": "a.txt", "content": "one\n2\n3\n" }] }),
    ));
    let r = data(&commit(
        &s,
        json!({ "base": base, "message": "y", "changes": [{ "path": "a.txt", "content": "1\n2\nthree\n" }] }),
    ));
    assert_eq!(r["rebased"], true);
    assert_eq!(
        text(&call(&s, "leep_read_file", json!({ "path": "a.txt" }))),
        "one\n2\nthree\n"
    );
}

#[test]
fn auth_failures_are_tool_errors() {
    let write = "write-token";
    let read = "read-token";
    let tokens = json!({ "tokens": [
        { "name": "w", "role": "write", "hash": auth::hash_token(write) },
        { "name": "r", "role": "read", "hash": auth::hash_token(read) } ] });
    let auth = Auth::parse(&tokens.to_string()).unwrap();
    let url = serve(leep_server::router_with_auth(
        common::repo(),
        leep_server::DEFAULT_MAX_BODY_BYTES,
        Some(auth),
    ));
    let with = |t: Option<&str>| {
        let mut c = config(&url);
        c.token = t.map(str::to_string);
        Server::new(c)
    };
    let r = call(&with(None), "leep_refs", json!({}));
    assert_eq!(r["isError"], true);
    assert!(text(&r).contains("authentication"), "{}", text(&r));
    let r = call(&with(Some("bogus")), "leep_refs", json!({}));
    assert_eq!(r["isError"], true);
    assert!(text(&r).contains("rejected the token"));
    let r = commit(
        &with(Some(read)),
        json!({ "message": "m", "changes": [{ "path": "a", "content": "x" }] }),
    );
    assert_eq!(r["isError"], true);
    assert!(text(&r).contains("write access"), "{}", text(&r));
    let r = commit(
        &with(Some(write)),
        json!({ "message": "m", "changes": [{ "path": "a", "content": "x" }] }),
    );
    assert_eq!(r["isError"], false, "{}", text(&r));
}

/// Editing a file without `executable` keeps its kind; agents rarely restate
/// the bit, and silently dropping `+x` from a script breaks it.
#[test]
fn edits_keep_the_executable_bit_unless_told_otherwise() {
    let s = Server::new(config(&start_server()));
    let kind = |s: &Server| {
        let files = data(&call(s, "leep_list_files", json!({ "prefix": "run.sh" })));
        files["files"][0]["kind"].clone()
    };
    commit(
        &s,
        json!({ "message": "add", "changes": [{ "path": "run.sh", "content": "#!/bin/sh\n", "executable": true }] }),
    );
    commit(
        &s,
        json!({ "message": "edit", "changes": [{ "path": "run.sh", "content": "#!/bin/sh\necho hi\n" }] }),
    );
    assert_eq!(kind(&s), "executable");
    commit(
        &s,
        json!({ "message": "clear", "changes": [{ "path": "run.sh", "content": "plain\n", "executable": false }] }),
    );
    assert_eq!(kind(&s), "regular");
    commit(
        &s,
        json!({ "message": "new", "changes": [{ "path": "new.txt", "content": "x\n" }] }),
    );
    let files = data(&call(&s, "leep_list_files", json!({ "prefix": "new.txt" })));
    assert_eq!(files["files"][0]["kind"], "regular");
}

/// Agents can ask which changes a given model, session or author made.
#[test]
fn log_filters_by_agent_and_author() {
    let url = start_server();
    let mut a_cfg = config(&url);
    a_cfg.agent_model = Some("model-a".into());
    a_cfg.agent_session = Some("s-a".into());
    let mut b_cfg = config(&url);
    b_cfg.agent_model = Some("model-b".into());
    b_cfg.agent_session = Some("s-b".into());
    b_cfg.author = "Bea <bea@example.com>".into();
    let (a, b) = (Server::new(a_cfg), Server::new(b_cfg));
    commit(
        &a,
        json!({ "message": "from a", "changes": [{ "path": "a.txt", "content": "a\n" }] }),
    );
    commit(
        &b,
        json!({ "message": "from b", "changes": [{ "path": "b.txt", "content": "b\n" }] }),
    );
    commit(
        &a,
        json!({ "message": "a again", "changes": [{ "path": "a.txt", "content": "aa\n" }] }),
    );

    let messages = |args: Value| -> Vec<String> {
        let log = data(&call(&a, "leep_log", args));
        log["entries"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| e["message"].as_str().unwrap().to_string())
            .collect()
    };
    assert_eq!(
        messages(json!({ "agent_model": "model-a" })),
        ["a again", "from a"]
    );
    assert_eq!(messages(json!({ "agent_session": "s-b" })), ["from b"]);
    assert_eq!(messages(json!({ "author": "bea" })), ["from b"]);
    assert_eq!(
        messages(json!({ "agent_model": "model-a", "path": "b.txt" })),
        Vec::<String>::new()
    );
    assert_eq!(messages(json!({})).len(), 3);
}

#[test]
fn review_status_reports_owners_and_blocked_paths() {
    use leep_cli::client::Client;
    use leep_core::api::AddReviewRequest;
    use leep_core::review::Verdict;
    use leep_server::review::{RequireReview, ReviewPolicy, ReviewService};

    let tokens = json!({ "tokens": [
        { "name": "agent", "role": "write", "hash": auth::hash_token("agent-t") },
        { "name": "alice", "role": "write", "hash": auth::hash_token("alice-t") },
        { "name": "viewer", "role": "read", "hash": auth::hash_token("viewer-t") } ] });
    let auth = Auth::parse(&tokens.to_string()).unwrap();
    let policy =
        ReviewPolicy::parse(r#"{"refs":["main"],"auto_approve":["comment_only"]}"#).unwrap();
    let service = std::sync::Arc::new(ReviewService::new(auth.reviewers(), policy.rules));
    let repo = common::repo().with_land_policy(std::sync::Arc::new(RequireReview::new(
        service.clone(),
        policy.refs,
    )));
    let url = serve(leep_server::router_with_review(
        repo,
        leep_server::DEFAULT_MAX_BODY_BYTES,
        Some(auth),
        None,
        Some(service),
    ));
    let with = |t: &str| {
        let mut c = config(&url);
        c.token = Some(t.to_string());
        Server::new(c)
    };
    let agent = with("agent-t");

    // main requires review, so the agent lands on a topic ref and asks.
    let r = commit(
        &agent,
        json!({ "message": "seed", "target_ref": "topic", "create_ref": true,
                "changes": [{ "path": "OWNERS", "content": "alice\n" },
                            { "path": "a.rs", "content": "fn a() {}\n" }] }),
    );
    let seed = data(&r)["landed"].as_str().unwrap().to_string();
    let r = commit(
        &agent,
        json!({ "message": "seed main", "create_ref": true,
                "changes": [{ "path": "a.rs", "content": "x" }] }),
    );
    assert_eq!(r["isError"], true);
    assert!(text(&r).contains("needs review"), "{}", text(&r));

    let r = commit(
        &agent,
        json!({ "message": "edit", "target_ref": "topic", "base": seed,
                "changes": [{ "path": "a.rs", "content": "// why\nfn a() {}\n" },
                            { "path": "b.rs", "content": "fn b() {}\n" }] }),
    );
    let id = data(&r)["landed"].as_str().unwrap().to_string();

    // No head on main yet, so nothing is owned: b.rs wants any reviewer.
    let st = data(&call(&agent, "leep_review_status", json!({ "rev": id })));
    assert_eq!(st["approved"], false);
    assert_eq!(st["target_ref"], "main");
    assert_eq!(st["blocked_paths"], json!(["b.rs"]));
    assert_eq!(st["paths"][0]["rule"], "auto:comment_only");
    assert_eq!(st["paths"][1]["rule"], "pending");
    assert!(st["next_step"].as_str().unwrap().contains("approve"));

    // Judged against `topic`, whose head has the OWNERS file, alice is needed.
    let st = data(&call(
        &agent,
        "leep_review_status",
        json!({ "rev": id, "ref": "topic" }),
    ));
    assert_eq!(st["paths"][1]["owners"], json!(["alice"]));

    // A read token may look; an approval flips the answer.
    let viewer = with("viewer-t");
    let alice = Client::with_token(&url, Some("alice-t"));
    alice
        .add_review(
            &id.parse().unwrap(),
            &AddReviewRequest {
                verdict: Verdict::Approve,
                comment: String::new(),
                reviewer: None,
                source: None,
            },
        )
        .unwrap();
    let st = data(&call(&viewer, "leep_review_status", json!({ "rev": id })));
    assert_eq!(st["approved"], true);
    assert_eq!(st["blocked_paths"], json!([]));
    assert!(st.get("next_step").is_none());

    let r = call(&agent, "leep_review_status", json!({ "rev": "nope-ref" }));
    assert_eq!(r["isError"], true);
    let r = call(&agent, "leep_review_status", json!({}));
    assert_eq!(r["isError"], true);
}

#[test]
fn review_status_needs_an_authenticated_server() {
    let s = Server::new(config(&start_server()));
    data(&commit(
        &s,
        json!({ "message": "m", "changes": [{ "path": "a", "content": "x" }] }),
    ));
    let r = call(&s, "leep_review_status", json!({ "rev": "main" }));
    assert_eq!(r["isError"], true);
    assert!(
        text(&r).contains("reviews need authentication"),
        "{}",
        text(&r)
    );
}

#[test]
fn search_finds_lines_and_reports_errors() {
    let s = Server::new(config(&start_server()));
    let r = commit(
        &s,
        json!({
            "message": "initial",
            "changes": [
                { "path": "src/lib.rs", "content": "pub fn parse() {}\n// TODO: handle Errors\n" },
                { "path": "src/main.rs", "content": "fn main() { parse(); }\n" },
                { "path": "docs/todo.md", "content": "todo list\n" },
            ],
        }),
    );
    assert_eq!(data(&r)["status"], "landed");

    let found = data(&call(&s, "leep_search", json!({ "query": "parse" })));
    let hits: Vec<(String, u64)> = found["matches"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| {
            (
                m["path"].as_str().unwrap().to_string(),
                m["line_number"].as_u64().unwrap(),
            )
        })
        .collect();
    assert_eq!(hits, [("src/lib.rs".into(), 1), ("src/main.rs".into(), 1)]);
    assert_eq!(found["matches"][1]["line"], "fn main() { parse(); }");
    assert_eq!(
        found["matches"][1]["ranges"],
        json!([{ "start": 12, "end": 17 }])
    );
    assert_eq!(found["truncated"], false);
    assert_eq!(found["stats"]["files_scanned"], 2);
    assert!(found.get("next").is_none());
    let page = data(&call(
        &s,
        "leep_search",
        json!({ "query": "parse", "limit": 1 }),
    ));
    let rest = data(&call(
        &s,
        "leep_search",
        json!({ "query": "parse", "limit": 1, "rev": page["rev"], "cursor": page["next"] }),
    ));
    assert_eq!(page["matches"][0]["path"], "src/lib.rs");
    assert_eq!(rest["matches"][0]["path"], "src/main.rs");
    assert!(rest.get("next").is_none());
    assert_eq!(found["rev"].as_str().unwrap().len(), 64);

    // Case, regex, path and glob options.
    let ci = data(&call(
        &s,
        "leep_search",
        json!({ "query": "todo", "case_insensitive": true }),
    ));
    assert_eq!(ci["matches"].as_array().unwrap().len(), 2);
    let re = data(&call(
        &s,
        "leep_search",
        json!({ "query": "fn\\s+\\w+\\(\\)", "regex": true, "glob": "**/*.rs" }),
    ));
    assert_eq!(re["matches"].as_array().unwrap().len(), 2);
    let scoped = data(&call(
        &s,
        "leep_search",
        json!({ "query": "o", "path": "docs", "rev": "main" }),
    ));
    assert_eq!(scoped["matches"][0]["path"], "docs/todo.md");

    let cut = data(&call(
        &s,
        "leep_search",
        json!({ "query": "r", "limit": 1 }),
    ));
    assert_eq!(cut["truncated"], true);
    assert_eq!(cut["matches"].as_array().unwrap().len(), 1);
    assert!(cut["note"].as_str().unwrap().contains("Narrow"));

    // Failures are tool errors the agent can read.
    for args in [
        json!({ "query": "(", "regex": true }),
        json!({ "query": "" }),
        json!({ "query": "x", "rev": "nosuchref" }),
        json!({}),
    ] {
        let r = call(&s, "leep_search", args.clone());
        assert_eq!(r["isError"], true, "{args}");
    }
    let r = call(&s, "leep_search", json!({ "query": "(", "regex": true }));
    assert!(text(&r).contains("bad regex"), "{}", text(&r));
}

#[test]
fn renames_in_diff_and_follow_in_log() {
    let s = Server::new(config(&start_server()));
    let body: String = (0..10).map(|i| format!("line {i}\n")).collect();
    let land = |args: Value| {
        data(&commit(&s, args))["landed"]
            .as_str()
            .unwrap()
            .to_string()
    };
    let first =
        land(json!({ "message": "add", "changes": [{ "path": "old.txt", "content": body }] }));
    let second = land(json!({
        "message": "move and edit",
        "changes": [
            { "path": "old.txt", "delete": true },
            { "path": "dir/new.txt", "content": format!("{body}more\n") },
        ],
    }));

    // Off by default: the delete and the add, as before.
    let d = text(&call(&s, "leep_diff", json!({ "to": second })));
    assert!(d.contains("diff --leep a/old.txt b/old.txt"), "{d}");
    assert!(!d.contains("rename from"), "{d}");

    let d = text(&call(
        &s,
        "leep_diff",
        json!({ "to": second, "renames": true }),
    ));
    assert!(d.contains("diff --leep a/old.txt b/dir/new.txt\n"), "{d}");
    assert!(
        d.contains("rename from old.txt\nrename to dir/new.txt\nsimilarity index 9"),
        "{d}"
    );
    assert!(d.contains("+more\n") && !d.contains("b/old.txt"), "{d}");
    let d = text(&call(
        &s,
        "leep_diff",
        json!({ "to": second, "renames": true, "rename_threshold": 100 }),
    ));
    assert!(d.contains("b/old.txt") && !d.contains("rename from"), "{d}");

    // Log: plain path log stops at the move; follow continues.
    let plain = data(&call(&s, "leep_log", json!({ "path": "dir/new.txt" })));
    assert_eq!(plain["entries"].as_array().unwrap().len(), 1);
    assert!(plain["entries"][0].get("path_at").is_none());
    let log = data(&call(
        &s,
        "leep_log",
        json!({ "path": "dir/new.txt", "follow": true }),
    ));
    let entries = log["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0]["id"], second);
    assert_eq!(entries[0]["path_at"], "dir/new.txt");
    assert_eq!(entries[0]["renamed"]["from"], "old.txt");
    assert_eq!(entries[1]["id"], first);
    assert_eq!(entries[1]["path_at"], "old.txt");

    for args in [
        json!({ "follow": true }),
        json!({ "follow": true, "path": "dir" }),
    ] {
        let r = call(&s, "leep_log", args);
        assert_eq!(r["isError"], true);
    }
}

#[test]
fn landing_onto_a_missing_ref_needs_create_ref() {
    let s = Server::new(config(&start_server()));
    // First land onto main of an empty repository creates it unasked.
    let r = commit(
        &s,
        json!({ "message": "root", "changes": [{ "path": "a.txt", "content": "a\n" }] }),
    );
    assert_eq!(data(&r)["status"], "landed");

    // A mistyped ref is an error, and nothing is created.
    let r = commit(
        &s,
        json!({ "message": "typo", "target_ref": "mian",
                "changes": [{ "path": "b.txt", "content": "b\n" }] }),
    );
    assert_eq!(r["isError"], true);
    assert!(text(&r).contains("create_ref"), "{}", text(&r));
    let refs = data(&call(&s, "leep_refs", json!({})));
    assert!(refs["refs"].get("mian").is_none(), "{refs}");

    // Asked for, the ref is created.
    let r = commit(
        &s,
        json!({ "message": "topic", "target_ref": "topic", "create_ref": true,
                "changes": [{ "path": "b.txt", "content": "b\n" }] }),
    );
    let landed = data(&r)["landed"].as_str().unwrap().to_string();
    let refs = data(&call(&s, "leep_refs", json!({})));
    assert_eq!(refs["refs"]["topic"], landed.as_str());

    // An existing ref needs no flag.
    let r = commit(
        &s,
        json!({ "message": "more", "target_ref": "topic",
                "changes": [{ "path": "c.txt", "content": "c\n" }] }),
    );
    assert_eq!(data(&r)["status"], "landed");
}

#[test]
fn short_ids_resolve_in_read_tools() {
    let s = Server::new(config(&start_server()));
    let r = commit(
        &s,
        json!({ "message": "root", "changes": [{ "path": "a.txt", "content": "short\n" }] }),
    );
    let id = data(&r)["landed"].as_str().unwrap().to_string();

    let shown = data(&call(&s, "leep_show", json!({ "rev": &id[..8] })));
    assert_eq!(shown["id"], id.as_str());
    let file = call(
        &s,
        "leep_read_file",
        json!({ "rev": &id[..12], "path": "a.txt" }),
    );
    assert!(text(&file).contains("short"), "{}", text(&file));

    // Too short to be an id and not a ref: a clear error.
    let r = call(&s, "leep_show", json!({ "rev": &id[..3] }));
    assert_eq!(r["isError"], true);
    assert!(text(&r).contains("prefix"), "{}", text(&r));
}

/// A call whose answer was lost (a 408 can come after the land applied) is
/// retried with the same `request_id`, or by landing the `changeset` the
/// first call made. Neither lands the change a second time.
#[test]
fn retried_commit_and_land_lands_once() {
    let s = Server::new(config(&start_server()));
    let args = json!({
        "message": "first",
        "changes": [{ "path": "a.txt", "content": "one\n" }],
        "request_id": "req-1",
    });
    let first = data(&commit(&s, args.clone()));
    assert_eq!(first["status"], "landed");
    let again = data(&commit(&s, args));
    assert_eq!(again["changeset"], first["changeset"]);
    assert_eq!(again["landed"], first["landed"]);

    let by_id = data(&commit(&s, json!({ "changeset": first["changeset"] })));
    assert_eq!(by_id["landed"], first["landed"]);
    assert_eq!(by_id["files_changed"], 1);

    let log = data(&call(&s, "leep_log", json!({})));
    let entries = log["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["agent"]["request_id"], "req-1");

    // The same id for another ref is a different request, refused.
    let r = commit(
        &s,
        json!({
            "message": "elsewhere",
            "changes": [{ "path": "b.txt", "content": "b\n" }],
            "request_id": "req-1",
            "target_ref": "other",
            "create_ref": true,
        }),
    );
    assert_eq!(r["isError"], true);
    assert!(text(&r).contains("req-1"), "{}", text(&r));
    // A changeset and changes together are ambiguous.
    let r = commit(
        &s,
        json!({
            "changeset": first["changeset"],
            "changes": [{ "path": "b.txt", "content": "b\n" }],
        }),
    );
    assert_eq!(r["isError"], true);
}

/// ADR 0058: read tools take an answer at the newest indexed changeset when
/// the server's index lags (`answered_at`, `behind`), unless `fresh`, which
/// gets an error naming the index, the lag and when to retry.
#[test]
fn read_tools_take_stale_answers_unless_fresh() {
    use leep_core::{LeaseClaim, LeaseKey, LeaseTiming, MetaStore};
    use leep_derive::indexing::{
        Budgets, IndexGroup, IndexLeaseConfig, IndexLeases, IndexMode, IndexPolicy,
    };
    use std::sync::Arc;
    use std::time::Duration;

    let meta =
        Arc::new(leep_core::memory::MemoryMetaStore::new().with_lease_clock(Arc::new(|| 1_000)));
    let repo = leep_derive::Repo::new(
        Arc::new(leep_core::memory::MemoryBlobStore::new()),
        meta.clone(),
    );
    let leases = IndexLeases::new(meta.clone(), IndexLeaseConfig::new("query", ""));
    let policy = IndexPolicy::new(IndexMode::Off, leases)
        .with_budgets(Budgets {
            scan: 0,
            blame: 0,
            search_files: 0,
            ..Budgets::default()
        })
        .with_index_wait(Duration::ZERO, 1);
    let url = serve(leep_server::router(
        repo.clone().with_index_policy(Arc::new(policy)),
    ));
    let s = Server::new(config(&url));
    let first = data(&commit(
        &s,
        json!({ "message": "one", "changes": [{ "path": "a.txt", "content": "one\n" }] }),
    ))["landed"]
        .as_str()
        .unwrap()
        .to_string();
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let id: leep_core::ChangesetId = first.parse().unwrap();
        leep_derive::derived::derive::<leep_derive::derived::HistoryTree>(&repo, id)
            .await
            .unwrap();
        repo.derive_search(id).await.unwrap();
        for g in IndexGroup::ALL {
            let key = LeaseKey::index(g.lease_name());
            meta.acquire_lease(
                &LeaseClaim {
                    key: &key,
                    holder: "indexer",
                    address: "",
                    protocol: 1,
                    meta: "",
                },
                LeaseTiming {
                    ttl: Duration::from_secs(15),
                    stale_after: Duration::from_secs(6),
                },
            )
            .await
            .unwrap();
        }
    });
    for i in 0..3 {
        data(&commit(
            &s,
            json!({ "message": format!("m{i}"), "changes": [{ "path": "a.txt", "content": format!("one\n{i}\n") }] }),
        ));
    }
    let log = data(&call(&s, "leep_log", json!({ "path": "a.txt" })));
    assert_eq!(log["answered_at"], first);
    assert_eq!(log["behind"], 3);
    assert_eq!(log["entries"].as_array().unwrap().len(), 1);
    let found = data(&call(&s, "leep_search", json!({ "query": "one" })));
    assert_eq!(found["answered_at"], first);
    let blame = call(&s, "leep_blame", json!({ "path": "a.txt" }));
    assert_eq!(blame["isError"], false, "{}", text(&blame));
    assert!(text(&blame).contains("answered_at"), "{}", text(&blame));

    let fresh = call(&s, "leep_log", json!({ "path": "a.txt", "fresh": true }));
    assert_eq!(fresh["isError"], true);
    let msg = text(&fresh);
    assert!(
        msg.contains("history-tree-v4")
            && msg.contains("3 changesets behind")
            && msg.contains("retry in"),
        "{msg}"
    );
}

/// `leep_log` lists summaries by default (the message's first line);
/// `full` gives the whole message, with the same entries and pages.
#[test]
fn log_lists_summaries_unless_full() {
    let s = Server::new(config(&start_server()));
    for i in 0..3 {
        let r = commit(
            &s,
            json!({
                "message": format!("subject {i}\n\nbody {i}\n"),
                "changes": [{ "path": format!("f{i}.txt"), "content": "x\n" }],
                "prompt": "p",
            }),
        );
        assert_eq!(data(&r)["status"], "landed");
    }
    let short = data(&call(&s, "leep_log", json!({})));
    let full = data(&call(&s, "leep_log", json!({ "full": true })));
    let (se, fe) = (
        short["entries"].as_array().unwrap(),
        full["entries"].as_array().unwrap(),
    );
    assert_eq!(se.len(), 3);
    assert_eq!(fe.len(), 3);
    for (i, (a, b)) in se.iter().zip(fe).enumerate() {
        let n = 2 - i;
        assert_eq!(a["id"], b["id"]);
        assert_eq!(a["parents"], b["parents"]);
        assert_eq!(a["files_changed"], 1);
        assert_eq!(b["files_changed"], 1);
        assert_eq!(a["agent"], b["agent"]);
        assert_eq!(a["agent"]["prompt"], "p");
        assert_eq!(a["message"], format!("subject {n}"));
        assert_eq!(b["message"], format!("subject {n}\n\nbody {n}\n"));
    }

    // Pages: `full` with a cursor from a summary page continues it.
    let page = data(&call(&s, "leep_log", json!({ "limit": 1 })));
    let rest = data(&call(
        &s,
        "leep_log",
        json!({ "limit": 5, "full": true, "rev": page["from"], "cursor": page["next"] }),
    ));
    let ids: Vec<&Value> = rest["entries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| &e["id"])
        .collect();
    assert_eq!(ids, [&fe[1]["id"], &fe[2]["id"]]);
}

/// The land id of a `request_id` is derived from it, not kept in memory, so
/// a second MCP server process (a restart between the send and the answer)
/// given the same request is answered from the server's record and lands
/// nothing (ADR 0057).
#[test]
fn request_id_lands_once_across_mcp_restarts() {
    let url = start_server();
    let args = json!({
        "message": "feature",
        "changes": [{ "path": "f.txt", "content": "feature\n" }],
        "request_id": "req-restart",
    });
    let first = data(&commit(&Server::new(config(&url)), args.clone()));
    assert_eq!(first["status"], "landed");

    // Other lands move main on in between.
    let other = Server::new(config(&url));
    for i in 0..3 {
        data(&commit(
            &other,
            json!({ "message": format!("o{i}"), "changes": [{ "path": "o.txt", "content": format!("{i}\n") }] }),
        ));
    }
    let head = data(&call(&other, "leep_refs", json!({})))["refs"]["main"].clone();

    // A fresh process: nothing in memory about req-restart.
    let again = data(&commit(&Server::new(config(&url)), args.clone()));
    assert_eq!(again["status"], "landed");
    assert_eq!(again["from_record"], true);
    assert_eq!(again["changeset"], first["changeset"]);
    assert_eq!(again["landed"], first["landed"]);
    assert_eq!(again["files_changed"], 1);
    let refs = data(&call(&other, "leep_refs", json!({})));
    assert_eq!(refs["refs"]["main"], head, "nothing new landed");
    let log = data(&call(&other, "leep_log", json!({ "limit": 50 })));
    assert_eq!(log["entries"].as_array().unwrap().len(), 4);

    // The ref log names the land by the derived id.
    let id = leep_mcp::tools::request_land_id("tester", Some("sess-1"), "main", "req-restart")
        .to_string();
    assert_eq!(id.len(), 32);
    let reflog = leep_cli::client::Client::new(&url)
        .ref_log(Some("main"), None, 10)
        .unwrap();
    let keyed = reflog
        .entries
        .iter()
        .filter(|e| e.land_id.as_ref().map(|l| l.to_string()) == Some(id.clone()))
        .count();
    assert_eq!(keyed, 1, "{:?}", reflog.entries);

    // The same id with other changes is a mistake: reported, not landed.
    let wrong = commit(
        &Server::new(config(&url)),
        json!({
            "message": "another",
            "changes": [{ "path": "f.txt", "content": "different\n" }],
            "request_id": "req-restart",
        }),
    );
    assert_eq!(wrong["isError"], true);
    let w = &wrong["structuredContent"];
    assert_eq!(w["status"], "already_landed");
    assert_eq!(w["changeset"], first["changeset"]);
    assert_eq!(w["differing_paths"], json!(["f.txt"]));
    let refs = data(&call(&other, "leep_refs", json!({})));
    assert_eq!(refs["refs"]["main"], head);

    // Another ref derives another id: the request is new there.
    let elsewhere = data(&commit(
        &Server::new(config(&url)),
        json!({
            "message": "feature",
            "changes": [{ "path": "f.txt", "content": "feature\n" }],
            "request_id": "req-restart",
            "target_ref": "side",
            "create_ref": true,
        }),
    ));
    assert_eq!(elsewhere["status"], "landed");
    assert!(elsewhere.get("from_record").is_none());
}

/// Bytes of the `tools/call` request an agent sends for `args`.
fn request_bytes(name: &str, args: &Value) -> usize {
    json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": { "name": name, "arguments": args } })
    .to_string()
    .len()
}

/// A conflict on one file of a 20-file change is finished by sending that
/// one file: the clean 19 come from the server's `clean` map, kept by the
/// MCP server. Compares the bytes the agent sends with the old way (every
/// file again).
#[test]
fn resolve_and_land_sends_only_the_conflicting_files() {
    let url = start_server();
    let s = Server::new(config(&url));
    // 20 files of 60 lines, about 2.4 KB each.
    let file = |i: usize, tag: &str| -> String {
        (0..60)
            .map(|l| format!("{tag} file {i:02} line {l:02}: some code here\n"))
            .collect()
    };
    let path = |i: usize| format!("src/f{i:02}.rs");
    let base_changes: Vec<Value> = (0..20)
        .map(|i| json!({ "path": path(i), "content": file(i, "base") }))
        .collect();
    let base = data(&commit(
        &s,
        json!({ "message": "base", "changes": base_changes }),
    ))["landed"]
        .clone();

    // Another agent edits the first line of f00 and the last line of f01.
    let mut theirs01 = file(1, "base");
    theirs01 = theirs01.replace(
        "base file 01 line 59: some code here",
        "base file 01 line 59: edited by A",
    );
    let a = data(&commit(
        &s,
        json!({ "base": base, "message": "a", "changes": [
            { "path": path(0), "content": file(0, "base").replacen("base", "A", 1) },
            { "path": path(1), "content": theirs01 } ] }),
    ));
    let head = a["landed"].as_str().unwrap().to_string();

    // This agent rewrites the first line of all 20 files from `base`.
    let ours = |i: usize| file(i, "base").replacen("base", "B", 1);
    let changes: Vec<Value> = (0..20)
        .map(|i| json!({ "path": path(i), "content": ours(i) }))
        .collect();
    let r = commit(
        &s,
        json!({ "base": base, "message": "rename things", "changes": changes,
                "request_id": "req-20", "prompt": "rename" }),
    );
    assert_eq!(r["isError"], true);
    let v = r["structuredContent"].clone();
    assert_eq!(v["status"], "conflict");
    assert_eq!(v["conflicts"].as_array().unwrap().len(), 1);
    assert_eq!(v["clean_paths"].as_array().unwrap().len(), 19);
    let conflict_id = v["conflict_id"].as_str().unwrap().to_string();
    assert!(v["guidance"].as_str().unwrap().contains(&conflict_id));
    let resolved = file(0, "base").replacen("base", "A+B", 1);

    // Old way: every file again, on the new head.
    let old_args = json!({
        "base": head, "message": "rename things", "request_id": "req-20", "prompt": "rename",
        "changes": (0..20).map(|i| json!({
            "path": path(i),
            "content": if i == 0 { resolved.clone() } else if i == 1 {
                // The agent must merge A's edit itself, too.
                ours(1).replace("base file 01 line 59: some code here", "base file 01 line 59: edited by A")
            } else { ours(i) },
        })).collect::<Vec<_>>(),
    });
    let new_args = json!({
        "conflict_id": conflict_id,
        "resolutions": [{ "path": path(0), "content": resolved }],
    });
    let (old_bytes, new_bytes) = (
        request_bytes("leep_commit_and_land", &old_args),
        request_bytes("leep_resolve_and_land", &new_args),
    );
    eprintln!(
        "conflict retry, 20 files / 1 conflicting: leep_commit_and_land {old_bytes} bytes, \
         leep_resolve_and_land {new_bytes} bytes ({:.1}x less)",
        old_bytes as f64 / new_bytes as f64
    );
    assert!(new_bytes * 10 < old_bytes, "{old_bytes} vs {new_bytes}");

    // Missing a conflicting path: refused, nothing landed.
    let r = call(
        &s,
        "leep_resolve_and_land",
        json!({ "conflict_id": conflict_id, "resolutions": [{ "path": path(5), "content": "x\n" }] }),
    );
    assert_eq!(r["isError"], true);
    assert!(text(&r).contains("src/f00.rs"), "{}", text(&r));
    // Another MCP process does not know the conflict: it says what to do.
    let r = call(
        &Server::new(config(&url)),
        "leep_resolve_and_land",
        new_args.clone(),
    );
    assert_eq!(r["isError"], true);
    assert!(
        text(&r).contains("leep_commit_and_land") && text(&r).contains("ALL files"),
        "{}",
        text(&r)
    );

    let landed = data(&call(&s, "leep_resolve_and_land", new_args.clone()));
    assert_eq!(landed["status"], "landed");
    assert_eq!(landed["rebased"], false);
    assert_eq!(landed["files_changed"], 20);
    let read = |p: &str| text(&call(&s, "leep_read_file", json!({ "path": p })));
    assert_eq!(
        read("src/f00.rs"),
        file(0, "base").replacen("base", "A+B", 1)
    );
    // f01 merged cleanly on the server: both edits.
    let f01 = read("src/f01.rs");
    assert!(
        f01.starts_with("B file 01 line 00") && f01.contains("line 59: edited by A"),
        "{f01}"
    );
    for i in 2..20 {
        assert_eq!(read(&path(i)), ours(i));
    }
    let show = data(&call(&s, "leep_show", json!({ "rev": "main" })));
    assert_eq!(show["message"], "rename things");
    assert_eq!(show["parents"], json!([head]));
    assert_eq!(show["agent"]["request_id"], "req-20");
    assert_eq!(show["agent"]["prompt"], "rename");

    // Resolved and landed: forgotten here. A retry of the request from any
    // process is answered from the record.
    let again = call(&s, "leep_resolve_and_land", new_args);
    assert_eq!(again["isError"], true);
    // (Within the same second the rebuilt changeset is the same one, so
    // `from_record` is not asserted.)
    let retry = data(&commit(&Server::new(config(&url)), old_args));
    assert_eq!(retry["changeset"], landed["changeset"]);
    assert_eq!(retry["landed"], landed["landed"]);
    let refs = data(&call(&s, "leep_refs", json!({})));
    assert_eq!(refs["refs"]["main"], landed["landed"]);
}

/// `onto` finds the remembered conflict too, and a resolution that conflicts
/// again (the ref moved on) reports a new conflict to resolve.
#[test]
fn resolve_and_land_by_onto_and_conflicting_again() {
    let s = Server::new(config(&start_server()));
    let base = data(&commit(
        &s,
        json!({ "message": "base", "changes": [{ "path": "a.txt", "content": "1\n" },
                                               { "path": "run.sh", "content": "#!/bin/sh\n", "executable": true }] }),
    ))["landed"]
        .clone();
    let head1 = data(&commit(
        &s,
        json!({ "base": base, "message": "x", "changes": [{ "path": "a.txt", "content": "x\n" }] }),
    ))["landed"]
        .as_str()
        .unwrap()
        .to_string();
    // No request_id: the resolution is still landed at most once.
    let r = commit(
        &s,
        json!({ "base": base, "message": "y", "changes": [
            { "path": "a.txt", "content": "y\n" },
            { "path": "run.sh", "content": "#!/bin/sh\necho\n" } ] }),
    );
    assert_eq!(r["structuredContent"]["status"], "conflict");
    let first_id = r["structuredContent"]["conflict_id"].clone();

    // The ref moves again before the agent answers.
    let head2 = data(&commit(
        &s,
        json!({ "message": "z", "changes": [{ "path": "a.txt", "content": "z\n" }] }),
    ))["landed"]
        .as_str()
        .unwrap()
        .to_string();
    let again = call(
        &s,
        "leep_resolve_and_land",
        json!({ "onto": &head1[..10], "resolutions": [{ "path": "a.txt", "content": "xy\n" }] }),
    );
    let v = &again["structuredContent"];
    assert_eq!(v["status"], "conflict", "{}", text(&again));
    assert_eq!(v["onto"], head2);
    assert_ne!(v["conflict_id"], first_id);
    // The first conflict was replaced.
    let stale = call(
        &s,
        "leep_resolve_and_land",
        json!({ "conflict_id": first_id, "resolutions": [{ "path": "a.txt", "content": "xy\n" }] }),
    );
    assert_eq!(stale["isError"], true);

    let done = data(&call(
        &s,
        "leep_resolve_and_land",
        json!({ "conflict_id": v["conflict_id"], "resolutions": [{ "path": "a.txt", "content": "xyz\n" }],
                "message": "y, resolved" }),
    ));
    assert_eq!(done["status"], "landed");
    let files = data(&call(&s, "leep_list_files", json!({})));
    let run = files["files"]
        .as_array()
        .unwrap()
        .iter()
        .find(|f| f["path"] == "run.sh")
        .unwrap()
        .clone();
    assert_eq!(run["kind"], "executable", "the clean change keeps +x");
    assert_eq!(
        text(&call(&s, "leep_read_file", json!({ "path": "a.txt" }))),
        "xyz\n"
    );
    let shown = data(&call(&s, "leep_show", json!({ "rev": "main" })));
    assert_eq!(shown["message"], "y, resolved");
    // No request_id: the first conflict's key names the land through every
    // later conflict of it, and the changeset says so.
    assert_eq!(
        shown["agent"]["request_id"],
        format!("conflict:{}", first_id.as_str().unwrap())
    );
}

/// `leep_reflog` pages through a ref's moves, compactly, with land ids.
#[test]
fn reflog_lists_moves_with_land_ids_and_pages() {
    let s = Server::new(config(&start_server()));
    let mut landed = Vec::new();
    for i in 0..3 {
        let r = data(&commit(
            &s,
            json!({ "message": format!("m{i}"), "changes": [{ "path": "a.txt", "content": format!("{i}\n") }],
                    "request_id": format!("req-{i}") }),
        ));
        landed.push(r["landed"].as_str().unwrap().to_string());
    }
    data(&commit(
        &s,
        json!({ "message": "side", "changes": [{ "path": "b.txt", "content": "b\n" }],
                "target_ref": "side", "create_ref": true }),
    ));

    let log = data(&call(&s, "leep_reflog", json!({})));
    assert_eq!(log["ref"], "main");
    let entries = log["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 3);
    assert!(log.get("next").is_none());
    let newest = &entries[0];
    assert_eq!(newest["op"], "land");
    assert_eq!(newest["new"], landed[2][..12]);
    assert_eq!(newest["old"], landed[1][..12]);
    assert!(newest.get("ref").is_none());
    assert_eq!(
        newest["land_id"],
        leep_mcp::tools::request_land_id("tester", Some("sess-1"), "main", "req-2").to_string()
    );
    // Fast-forwards: the ref took the changeset itself.
    assert!(newest.get("land_changeset").is_none());
    assert_eq!(entries[2]["old"], Value::Null);
    // The short ids work as revs.
    let shown = data(&call(&s, "leep_show", json!({ "rev": newest["new"] })));
    assert_eq!(shown["id"], landed[2]);

    // Pages of one, then the end.
    let p1 = data(&call(
        &s,
        "leep_reflog",
        json!({ "limit": 1, "full_ids": true }),
    ));
    assert_eq!(p1["entries"][0]["new"], landed[2]);
    let p2 = data(&call(
        &s,
        "leep_reflog",
        json!({ "limit": 2, "full_ids": true, "cursor": p1["next"] }),
    ));
    let ids: Vec<&Value> = p2["entries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| &e["new"])
        .collect();
    assert_eq!(ids, [&json!(landed[1]), &json!(landed[0])]);
    assert!(p2.get("next").is_none());

    let all = data(&call(&s, "leep_reflog", json!({ "all": true })));
    let all = all["entries"].as_array().unwrap();
    assert_eq!(all.len(), 4);
    assert_eq!(all[0]["ref"], "side");
    let none = data(&call(&s, "leep_reflog", json!({ "ref": "nosuch" })));
    assert_eq!(none["entries"], json!([]));
    let bad = call(&s, "leep_reflog", json!({ "cursor": "garbage" }));
    assert_eq!(bad["isError"], true);
    assert!(text(&bad).contains("[code: invalid]"), "{}", text(&bad));
}

#[test]
fn resolve_merge_base_and_provenance() {
    let url = start_server();
    let s = Server::new(config(&url));
    let root = data(&commit(
        &s,
        json!({ "message": "root", "changes": [{ "path": "a.txt", "content": "a\n" }] }),
    ))["landed"]
        .as_str()
        .unwrap()
        .to_string();
    let on_main = data(&commit(
        &s,
        json!({ "message": "m", "changes": [{ "path": "src/m.rs", "content": "m\n" }] }),
    ))["landed"]
        .as_str()
        .unwrap()
        .to_string();
    let mut human = common::config(&url);
    human.author = "alice".into();
    human.agent_model = None;
    human.agent_session = None;
    let side = data(&commit(
        &Server::new(human),
        json!({ "base": root, "message": "s", "changes": [{ "path": "docs/s.md", "content": "s\n" }],
                "target_ref": "side", "create_ref": true }),
    ))["landed"]
        .as_str()
        .unwrap()
        .to_string();

    // leep_resolve: ref, full id, prefix, and errors.
    let r = data(&call(&s, "leep_resolve", json!({ "rev": "main" })));
    assert_eq!(r["id"], on_main);
    assert_eq!(r["via"], "ref");
    let r = data(&call(&s, "leep_resolve", json!({ "rev": root })));
    assert_eq!(
        (r["id"].clone(), r["via"].clone()),
        (json!(root), json!("id"))
    );
    let r = data(&call(&s, "leep_resolve", json!({ "rev": &side[..8] })));
    assert_eq!(
        (r["id"].clone(), r["via"].clone()),
        (json!(side), json!("prefix"))
    );
    let r = call(&s, "leep_resolve", json!({ "rev": "nosuch" }));
    assert_eq!(r["isError"], true);
    assert!(text(&r).contains("[code: not_found]"), "{}", text(&r));

    // leep_merge_base.
    let mb = data(&call(
        &s,
        "leep_merge_base",
        json!({ "a": "main", "b": "side" }),
    ));
    assert_eq!(mb["base"], root);
    assert_eq!(mb["a_is_ancestor_of_b"], false);
    assert_eq!(mb["b_is_ancestor_of_a"], false);
    let mb = data(&call(
        &s,
        "leep_merge_base",
        json!({ "a": root, "b": "main" }),
    ));
    assert_eq!(mb["base"], root);
    assert_eq!(mb["a_is_ancestor_of_b"], true);
    let r = call(&s, "leep_merge_base", json!({ "a": "main", "b": "nosuch" }));
    assert_eq!(r["isError"], true);

    // leep_provenance.
    let p = data(&call(&s, "leep_provenance", json!({ "rev": "side" })));
    assert_eq!(p["scanned"], 2);
    assert_eq!(p["truncated"], false);
    assert_eq!(p["agent_changesets"], 1);
    assert_eq!(p["human_changesets"], 1);
    assert_eq!(p["agents"][0]["model"], "test-model");
    assert_eq!(p["agents"][0]["session"], "sess-1");
    assert_eq!(p["humans"][0]["author"], "alice");
    let p = data(&call(
        &s,
        "leep_provenance",
        json!({ "rev": "main", "path": "/src/" }),
    ));
    assert_eq!(p["path"], "src");
    assert_eq!(p["agent_changesets"], 1);
    assert_eq!(p["agents"][0]["files_touched"], 1);
    assert_eq!(p["humans"], json!([]));
    // Only slashes: no filter at all.
    let p = data(&call(
        &s,
        "leep_provenance",
        json!({ "rev": "main", "path": "/" }),
    ));
    assert!(p.get("path").is_none());
    assert_eq!(p["scanned"], 2);
    let p = data(&call(&s, "leep_provenance", json!({ "limit": 1 })));
    assert_eq!(p["truncated"], true);
    assert!(p["note"].as_str().unwrap().contains("limit"));
}

/// The derived land id is namespaced by author and session: two agents that
/// happen to pick the same `request_id` land independently.
#[test]
fn same_request_id_from_two_authors_lands_twice() {
    let url = start_server();
    let mut bob = config(&url);
    bob.author = "bob".into();
    bob.agent_session = None;
    let mut carol = config(&url);
    carol.author = "carol".into();
    carol.agent_session = None;
    let args = |who: &str| {
        json!({
            "message": format!("{who}'s change"),
            "changes": [{ "path": format!("{who}.txt"), "content": "x\n" }],
            "request_id": "task-1",
        })
    };
    let b = data(&commit(&Server::new(bob), args("bob")));
    let c = data(&commit(&Server::new(carol), args("carol")));
    assert_eq!(b["status"], "landed");
    assert_eq!(c["status"], "landed");
    assert!(c.get("from_record").is_none());
    assert_ne!(b["landed"], c["landed"]);
    let s = Server::new(config(&url));
    let log = data(&call(&s, "leep_log", json!({})));
    assert_eq!(log["entries"].as_array().unwrap().len(), 2);
    // A session, when set, namespaces too.
    use leep_mcp::tools::request_land_id;
    let ids = [
        request_land_id("bob", None, "main", "task-1"),
        request_land_id("carol", None, "main", "task-1"),
        request_land_id("bob", Some("s1"), "main", "task-1"),
        request_land_id("bob", None, "side", "task-1"),
    ];
    for (i, a) in ids.iter().enumerate() {
        for b in &ids[i + 1..] {
            assert_ne!(a, b);
        }
    }
}

mod rewriting {
    use std::sync::atomic::{AtomicU8, Ordering};
    use std::sync::Arc;

    use axum::body::Body;
    use axum::extract::{Request, State};
    use axum::middleware::{from_fn_with_state, Next};
    use axum::response::Response;
    use serde_json::{json, Value};

    /// Flags; 0 passes through. Each strips what an older or reworded
    /// server might not send.
    /// `land_id_reused` refusals lose the changeset id from their text.
    pub const REWORD: u8 = 1;
    /// `land_id_reused` refusals lose their `land` record (pre-#130 servers).
    pub const DROP_RECORD: u8 = 2;
    /// The ref log loses its land ids, so the record cannot be found there.
    pub const DROP_LOG: u8 = 4;

    async fn rewrite(State(mode): State<Arc<AtomicU8>>, req: Request, next: Next) -> Response {
        let reflog = req.uri().path().starts_with("/v1/reflog");
        let resp = next.run(req).await;
        let m = mode.load(Ordering::SeqCst);
        if m == 0 {
            return resp;
        }
        let (mut parts, body) = resp.into_parts();
        let bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
        let mut out = bytes.to_vec();
        if let Ok(mut v) = serde_json::from_slice::<Value>(&bytes) {
            if v["code"] == "land_id_reused" {
                if m & REWORD != 0 {
                    v["error"] = json!("this land id was used before; use a new land id");
                }
                if m & DROP_RECORD != 0 {
                    v.as_object_mut().unwrap().remove("land");
                }
            }
            if m & DROP_LOG != 0 && reflog {
                for e in v["entries"].as_array_mut().into_iter().flatten() {
                    e.as_object_mut().unwrap().remove("land_id");
                    e.as_object_mut().unwrap().remove("land_changeset");
                }
            }
            out = serde_json::to_vec(&v).unwrap();
        }
        parts.headers.remove(axum::http::header::CONTENT_LENGTH);
        Response::from_parts(parts, Body::from(out))
    }

    pub fn server(mode: Arc<AtomicU8>) -> String {
        super::common::serve(
            leep_server::router(super::common::repo()).layer(from_fn_with_state(mode, rewrite)),
        )
    }
}

/// A `land_id_reused` refusal is answered from its `land` record, which needs
/// neither its text nor the ref log; from an older server without the record,
/// from the changeset its text names; failing that, from the ref log; and
/// when the log cannot say either, the tool reports that the request landed
/// before and lands nothing.
#[test]
fn land_id_refusal_is_read_from_the_record_then_text_then_ref_log() {
    use rewriting::{DROP_LOG, DROP_RECORD, REWORD};
    use std::sync::atomic::{AtomicU8, Ordering};
    use std::sync::Arc;

    let mode = Arc::new(AtomicU8::new(0));
    let url = rewriting::server(mode.clone());
    let args = json!({
        "message": "feature",
        "changes": [{ "path": "f.txt", "content": "f\n" }],
        "request_id": "req-scan",
    });
    let first = data(&commit(&Server::new(config(&url)), args.clone()));
    // Move main so a retry's fresh changeset differs from the first.
    data(&commit(
        &Server::new(config(&url)),
        json!({ "message": "o", "changes": [{ "path": "o.txt", "content": "o\n" }] }),
    ));

    for (m, path) in [
        (REWORD | DROP_LOG, "the record"),
        (DROP_RECORD | DROP_LOG, "the text of an older server"),
        (REWORD | DROP_RECORD, "the ref log"),
    ] {
        mode.store(m, Ordering::SeqCst);
        let again = data(&commit(&Server::new(config(&url)), args.clone()));
        assert_eq!(again["from_record"], true, "from {path}");
        assert_eq!(again["changeset"], first["changeset"], "from {path}");
        assert_eq!(again["landed"], first["landed"], "from {path}");
    }

    mode.store(REWORD | DROP_RECORD | DROP_LOG, Ordering::SeqCst);
    let s = Server::new(config(&url));
    let head = data(&call(&s, "leep_refs", json!({})))["refs"]["main"].clone();
    let lost = commit(&s, args);
    assert_eq!(lost["isError"], true);
    let t = text(&lost);
    assert!(
        t.contains("already landed something") && t.contains("leep_reflog"),
        "{t}"
    );
    assert!(!t.contains("changeset:"), "{t}");
    assert_eq!(
        data(&call(&s, "leep_refs", json!({})))["refs"]["main"],
        head
    );
}

/// A search the server sheds (ADR 0060) comes back at once as a retryable
/// tool error carrying `retry_after`: the tool asks once and never sleeps,
/// whatever the `Retry-After`.
#[test]
fn a_shed_search_is_a_retryable_tool_error_without_waiting() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use axum::http::{header, HeaderValue, StatusCode};
    use axum::response::IntoResponse;
    use axum::routing::get;

    let id = "ab".repeat(32);
    let searches = Arc::new(AtomicUsize::new(0));
    let seen = searches.clone();
    let resolved = id.clone();
    let app = axum::Router::new()
        .route(
            "/v1/resolve/{*rev}",
            get(move || {
                let id = resolved.clone();
                async move { axum::Json(json!({ "id": id })) }
            }),
        )
        .route(
            "/v1/search/{*rev}",
            get(move || {
                seen.fetch_add(1, Ordering::SeqCst);
                async {
                    let body = json!({
                        "error": "server busy: all 1 of this server's search permits are in use \
                                  (--read-concurrency); nothing was done, retry after 7s",
                        "code": "unavailable",
                    });
                    let mut resp =
                        (StatusCode::SERVICE_UNAVAILABLE, axum::Json(body)).into_response();
                    let h = resp.headers_mut();
                    h.insert(header::RETRY_AFTER, HeaderValue::from_static("7"));
                    h.insert("leep-refused", HeaderValue::from_static("read-class"));
                    resp
                }
            }),
        );
    let s = Server::new(config(&serve(app)));
    let started = std::time::Instant::now();
    let r = call(&s, "leep_search", json!({ "query": "parse" }));
    assert!(
        started.elapsed() < std::time::Duration::from_secs(5),
        "the tool slept"
    );
    assert_eq!(searches.load(Ordering::SeqCst), 1, "asked once");
    assert_eq!(r["isError"], true);
    let t = text(&r);
    for want in [
        "server busy",
        "Leep-Refused: read-class",
        "unavailable",
        "retryable",
        "retry_after: 7",
    ] {
        assert!(t.contains(want), "{want:?} in {t}");
    }
}

/// A land the server did not admit because its ref's queue was full (ADR
/// 0060, section 2) comes back at once as a retryable tool error carrying
/// `retry_after`; the tool never sleeps. Called again with the same
/// `request_id`, the land goes out under the same land id and lands once.
#[test]
fn a_land_not_admitted_is_a_retryable_tool_error_and_resends_under_its_id() {
    use std::sync::{Arc, Mutex};

    use axum::body::Body;
    use axum::extract::Request;
    use axum::http::{header, HeaderValue, Method, StatusCode};
    use axum::middleware::{self, Next};
    use axum::response::{IntoResponse, Response};

    let lands: Arc<Mutex<Vec<Value>>> = Arc::default();
    let seen = lands.clone();
    let app = leep_server::router(common::repo()).layer(middleware::from_fn(
        move |req: Request, next: Next| {
            let seen = seen.clone();
            async move {
                if req.method() != Method::POST || req.uri().path() != "/v1/land" {
                    return next.run(req).await;
                }
                let (parts, body) = req.into_parts();
                let bytes = axum::body::to_bytes(body, 1 << 20).await.unwrap();
                let first = {
                    let mut s = seen.lock().unwrap();
                    s.push(serde_json::from_slice(&bytes).unwrap());
                    s.len() == 1
                };
                if !first {
                    return next
                        .run(Request::from_parts(parts, Body::from(bytes)))
                        .await;
                }
                let body = json!({
                    "error": "land not admitted: the land queue of \"main\" holds 2 batches \
                              (--land-queue-batches 2); nothing was done, send it again in 3s",
                    "code": "unavailable",
                });
                let mut resp: Response =
                    (StatusCode::SERVICE_UNAVAILABLE, axum::Json(body)).into_response();
                let h = resp.headers_mut();
                h.insert(header::RETRY_AFTER, HeaderValue::from_static("3"));
                h.insert("leep-refused", HeaderValue::from_static("admission"));
                resp
            }
        },
    ));
    let s = Server::new(config(&serve(app)));
    let args = json!({
        "message": "first",
        "changes": [{ "path": "a.txt", "content": "one\n" }],
        "request_id": "req-admit",
    });
    let started = std::time::Instant::now();
    let r = commit(&s, args.clone());
    assert!(
        started.elapsed() < std::time::Duration::from_secs(2),
        "the tool slept"
    );
    assert_eq!(r["isError"], true);
    assert_eq!(lands.lock().unwrap().len(), 1, "sent once");
    let t = text(&r);
    for want in [
        "land not admitted",
        "Leep-Refused: admission",
        "unavailable",
        "retryable",
        "retry_after: 3",
    ] {
        assert!(t.contains(want), "{want:?} in {t}");
    }
    assert!(!t.contains("may have applied"), "{t}");

    let landed = data(&commit(&s, args));
    assert_eq!(landed["status"], "landed");
    // The empty repository's first land is sent without `create`, then
    // with it; every send names the same land and changeset.
    let lands = lands.lock().unwrap();
    assert!(lands.len() >= 2, "{lands:?}");
    assert!(lands[0]["land_id"].is_string());
    for l in lands.iter() {
        assert_eq!(l["land_id"], lands[0]["land_id"], "the same land id");
        assert_eq!(l["changeset"], lands[0]["changeset"]);
    }
}
