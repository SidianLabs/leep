//! JSON-RPC and MCP protocol behavior, driven through the request handler.

mod common;

use common::{call, config, rpc, text};
use leep_mcp::{Server, PROTOCOL_VERSION};
use serde_json::{json, Value};

/// A server whose backend is unreachable: protocol tests never need it.
fn offline() -> Server {
    Server::new(config("http://127.0.0.1:1"))
}

#[test]
fn initialize_handshake() {
    let s = offline();
    let v = rpc(
        &s,
        json!({ "jsonrpc": "2.0", "id": 7, "method": "initialize", "params": {
            "protocolVersion": "2025-06-18", "capabilities": {},
            "clientInfo": { "name": "t", "version": "0" } } }),
    );
    assert_eq!(v["jsonrpc"], "2.0");
    assert_eq!(v["id"], 7);
    assert_eq!(v["result"]["protocolVersion"], PROTOCOL_VERSION);
    assert_eq!(v["result"]["serverInfo"]["name"], "leep-mcp");
    assert!(v["result"]["capabilities"]["tools"].is_object());
    // The initialized notification gets no reply.
    assert!(s
        .handle_line(r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#)
        .is_none());
    assert_eq!(
        rpc(&s, json!({"jsonrpc":"2.0","id":"p","method":"ping"}))["result"],
        json!({})
    );
}

#[test]
fn tools_list_schemas_are_sane() {
    let v = rpc(
        &offline(),
        json!({"jsonrpc":"2.0","id":1,"method":"tools/list"}),
    );
    let tools = v["result"]["tools"].as_array().unwrap();
    let names: Vec<&str> = tools.iter().map(|t| t["name"].as_str().unwrap()).collect();
    for expected in [
        "leep_refs",
        "leep_log",
        "leep_show",
        "leep_list_files",
        "leep_read_file",
        "leep_diff",
        "leep_blame",
        "leep_search",
        "leep_review_status",
        "leep_commit_and_land",
    ] {
        assert!(names.contains(&expected), "missing {expected}");
    }
    for t in tools {
        assert!(!t["description"].as_str().unwrap().is_empty());
        let schema = &t["inputSchema"];
        assert_eq!(schema["type"], "object", "{}", t["name"]);
        let props = schema["properties"].as_object().unwrap();
        for r in schema
            .get("required")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            assert!(
                props.contains_key(r.as_str().unwrap()),
                "{}: {r}",
                t["name"]
            );
        }
    }
}

#[test]
fn unknown_method_and_tool_are_protocol_errors() {
    let s = offline();
    let v = rpc(
        &s,
        json!({"jsonrpc":"2.0","id":3,"method":"resources/list"}),
    );
    assert_eq!(v["error"]["code"], -32601);
    assert_eq!(v["id"], 3);
    let v = rpc(
        &s,
        json!({"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"nope"}}),
    );
    assert_eq!(v["error"]["code"], -32602);
    let v = rpc(
        &s,
        json!({"jsonrpc":"2.0","id":5,"method":"tools/call","params":{}}),
    );
    assert_eq!(v["error"]["code"], -32602);
}

#[test]
fn malformed_input() {
    let s = offline();
    let v: Value = serde_json::from_str(&s.handle_line("{not json").unwrap()).unwrap();
    assert_eq!(v["error"]["code"], -32700);
    assert_eq!(v["id"], Value::Null);
    let v: Value = serde_json::from_str(&s.handle_line("[1,2]").unwrap()).unwrap();
    assert_eq!(v["error"]["code"], -32600);
    let v: Value =
        serde_json::from_str(&s.handle_line(r#"{"jsonrpc":"2.0","id":1}"#).unwrap()).unwrap();
    assert_eq!(v["error"]["code"], -32600);
    // The server keeps working afterwards.
    assert!(s
        .handle_line(r#"{"jsonrpc":"2.0","id":2,"method":"ping"}"#)
        .is_some());
}

#[test]
fn tool_failures_are_tool_errors() {
    let s = offline();
    // Bad arguments.
    let r = call(&s, "leep_read_file", json!({}));
    assert_eq!(r["isError"], true);
    assert!(text(&r).contains("invalid arguments"));
    let r = call(
        &s,
        "leep_commit_and_land",
        json!({"message":"m","changes":[{"path":"a"}]}),
    );
    assert_eq!(r["isError"], true);
    assert!(text(&r).contains("content"));
    // Unreachable server.
    let r = call(&s, "leep_refs", json!({}));
    assert_eq!(r["isError"], true);
    assert!(text(&r).contains("cannot reach"));
}
