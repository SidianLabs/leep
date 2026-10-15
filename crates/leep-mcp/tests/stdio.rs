//! Spawns the real `leep-mcp` binary and speaks MCP over its stdio.

mod common;

use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};

use serde_json::{json, Value};

#[test]
fn binary_speaks_mcp_over_stdio() {
    let url = common::start_server();
    let cache = tempfile::tempdir().unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_leep-mcp"))
        .env("LEEP_SERVER", &url)
        .env("LEEP_AUTHOR", "stdio-tester")
        .env("LEEP_AGENT_MODEL", "bin-model")
        .env("LEEP_CACHE_DIR", cache.path())
        .env_remove("LEEP_TOKEN")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap());
    let mut send = |v: Value| -> Option<Value> {
        let is_request = v.get("id").is_some();
        writeln!(stdin, "{v}").unwrap();
        stdin.flush().unwrap();
        if !is_request {
            return None;
        }
        let mut line = String::new();
        stdout.read_line(&mut line).unwrap();
        Some(serde_json::from_str(&line).unwrap_or_else(|e| panic!("{e}: {line:?}")))
    };

    let init = send(
        json!({ "jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
        "protocolVersion": "2025-06-18", "capabilities": {},
        "clientInfo": { "name": "t", "version": "0" } } }),
    )
    .unwrap();
    assert_eq!(init["result"]["protocolVersion"], "2025-06-18");
    send(json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }));

    let list = send(json!({ "jsonrpc": "2.0", "id": 2, "method": "tools/list" })).unwrap();
    assert_eq!(list["result"]["tools"].as_array().unwrap().len(), 10);

    let r = send(json!({ "jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": {
        "name": "leep_commit_and_land",
        "arguments": { "message": "via stdio", "changes": [{ "path": "hi.txt", "content": "hi\n" }] } } }))
    .unwrap();
    assert_eq!(r["result"]["isError"], false, "{r}");
    let r = send(
        json!({ "jsonrpc": "2.0", "id": 4, "method": "tools/call", "params": {
        "name": "leep_log", "arguments": {} } }),
    )
    .unwrap();
    let entry = &r["result"]["structuredContent"]["entries"][0];
    assert_eq!(entry["author"], "stdio-tester");
    assert_eq!(entry["agent"]["model"], "bin-model");

    drop(stdin); // EOF ends the server.
    assert!(child.wait().unwrap().success());
    let mut rest = String::new();
    std::io::Read::read_to_string(&mut stdout, &mut rest).unwrap();
    assert!(rest.is_empty(), "stdout carried extra output: {rest:?}");
}
