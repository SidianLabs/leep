#![allow(dead_code)]

use std::sync::Arc;

use axum::Router;
use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
use leep_derive::Repo;
use leep_mcp::{Config, Server};
use serde_json::{json, Value};

pub fn repo() -> Repo {
    Repo::new(
        Arc::new(MemoryBlobStore::new()),
        Arc::new(MemoryMetaStore::new()),
    )
}

/// Serve `app` on an ephemeral port from a background runtime that lives for
/// the rest of the test process. Returns the base URL.
pub fn serve(app: Router) -> String {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            tx.send(l.local_addr().unwrap()).unwrap();
            axum::serve(l, app).await.unwrap();
        });
    });
    format!("http://{}", rx.recv().unwrap())
}

pub fn start_server() -> String {
    serve(leep_server::router(repo()))
}

pub fn config(url: &str) -> Config {
    Config {
        server: url.into(),
        token: None,
        author: "tester".into(),
        agent_model: Some("test-model".into()),
        agent_session: Some("sess-1".into()),
        agent_prompt: None,
        agent_tests: None,
        cache: None,
    }
}

pub fn rpc(server: &Server, req: Value) -> Value {
    let reply = server
        .handle_line(&req.to_string())
        .expect("a request gets a reply");
    serde_json::from_str(&reply).unwrap()
}

/// Call a tool; returns the `result` object of the `tools/call` reply.
pub fn call(server: &Server, name: &str, args: Value) -> Value {
    let v = rpc(
        server,
        json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/call",
                "params": { "name": name, "arguments": args } }),
    );
    assert!(v.get("error").is_none(), "protocol error: {v}");
    v["result"].clone()
}

pub fn text(result: &Value) -> String {
    result["content"][0]["text"].as_str().unwrap().to_string()
}
