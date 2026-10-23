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
    assert_eq!(list["result"]["tools"].as_array().unwrap().len(), 15);

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

/// A client that closes our stdout has gone: the reply that fails ends the
/// process with an error status, without a panic and without waiting for
/// stdin to close.
#[test]
fn closed_stdout_ends_the_server_quietly() {
    let (reader, writer) = std::io::pipe().unwrap();
    drop(reader);
    let mut child = Command::new(env!("CARGO_BIN_EXE_leep-mcp"))
        .env("LEEP_SERVER", "http://127.0.0.1:9")
        .env_remove("LEEP_TOKEN")
        .stdin(Stdio::piped())
        .stdout(writer)
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    let init = json!({ "jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
        "protocolVersion": "2025-06-18", "capabilities": {},
        "clientInfo": { "name": "t", "version": "0" } } });
    writeln!(stdin, "{init}").unwrap();
    stdin.flush().unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    let status = loop {
        if let Some(s) = child.try_wait().unwrap() {
            break s;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "still running after its stdout closed"
        );
        std::thread::sleep(std::time::Duration::from_millis(20));
    };
    drop(stdin);
    let mut stderr = String::new();
    std::io::Read::read_to_string(&mut child.stderr.take().unwrap(), &mut stderr).unwrap();
    assert!(!stderr.contains("panicked"), "{stderr}");
    assert_eq!(status.code(), Some(1), "{stderr}");
}
