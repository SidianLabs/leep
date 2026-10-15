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
    assert_eq!(entries[0]["author"], "tester");
    assert_eq!(entries[1]["agent"]["model"], "test-model");
    assert_eq!(entries[1]["agent"]["session"], "sess-1");
    assert_eq!(entries[1]["agent"]["prompt"], "make files");
    assert_eq!(entries[1]["agent"]["tests"], "none run");

    let by_path = data(&call(&s, "leep_log", json!({ "path": "src/run.sh" })));
    assert_eq!(by_path["entries"].as_array().unwrap().len(), 1);

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
        json!({ "message": "seed", "target_ref": "topic",
                "changes": [{ "path": "OWNERS", "content": "alice\n" },
                            { "path": "a.rs", "content": "fn a() {}\n" }] }),
    );
    let seed = data(&r)["landed"].as_str().unwrap().to_string();
    let r = commit(
        &agent,
        json!({ "message": "seed main", "changes": [{ "path": "a.rs", "content": "x" }] }),
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
