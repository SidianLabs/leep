//! JSON-RPC 2.0 framing and the MCP methods this server implements.

use std::collections::HashMap;
use std::sync::Mutex;

use leep_cli::client::Client;
use leep_core::{ChangesetId, LandId};
use serde_json::{json, Value};

use crate::tools::{self, ToolOutput};
use crate::Config;

pub const PROTOCOL_VERSION: &str = "2025-06-18";

const PARSE_ERROR: i64 = -32700;
const INVALID_REQUEST: i64 = -32600;
const METHOD_NOT_FOUND: i64 = -32601;
const INVALID_PARAMS: i64 = -32602;

const INSTRUCTIONS: &str = "Leep is a server-centric version control system. Read tools \
(leep_refs, leep_log, leep_show, leep_list_files, leep_read_file, leep_diff, leep_blame, \
leep_review_status) take an optional rev: a ref name (default `main`) or a full 64-hex changeset id. There is no working copy: \
leep_commit_and_land sends complete file contents, creates one changeset and lands it. If it \
reports conflicts, nothing landed; retry with base set to the reported `onto` id and resolved \
content for every file you changed.";

pub struct Server {
    pub(crate) client: Client,
    pub(crate) cfg: Config,
    /// `request_id` → the changeset `leep_commit_and_land` made for it and
    /// the ref it was landing on, so a retried request lands that changeset
    /// again instead of making a second one.
    pub(crate) requests: Mutex<HashMap<String, (ChangesetId, String)>>,
    /// `(changeset, ref)` → the land id of a land of it that got no final
    /// answer, so the call that finishes it reuses the id and the server
    /// answers with the earlier result instead of landing twice (ADR 0057).
    pub(crate) pending_lands: Mutex<HashMap<(ChangesetId, String), LandId>>,
}

impl Server {
    pub fn new(cfg: Config) -> Self {
        let client = Client::with_token(&cfg.server, cfg.token.as_deref());
        Server {
            client,
            cfg,
            requests: Mutex::default(),
            pending_lands: Mutex::default(),
        }
    }

    /// Handle one newline-delimited message. `None` means no reply is due
    /// (a notification or a stray response).
    pub fn handle_line(&self, line: &str) -> Option<String> {
        let msg: Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(e) => {
                return Some(error(
                    Value::Null,
                    PARSE_ERROR,
                    &format!("parse error: {e}"),
                ))
            }
        };
        self.handle(msg).map(|v| v.to_string())
    }

    fn handle(&self, msg: Value) -> Option<Value> {
        let Some(obj) = msg.as_object() else {
            return Some(error_v(
                Value::Null,
                INVALID_REQUEST,
                "request must be a JSON object (batches are not supported)",
            ));
        };
        let method = obj.get("method").and_then(Value::as_str);
        let Some(method) = method else {
            // A response to a request we never send, or garbage.
            if obj.contains_key("result") || obj.contains_key("error") {
                return None;
            }
            return Some(error_v(
                obj.get("id").cloned().unwrap_or(Value::Null),
                INVALID_REQUEST,
                "missing method",
            ));
        };
        let Some(id) = obj.get("id").cloned() else {
            // Notification: `notifications/initialized`, `notifications/cancelled`, ...
            return None;
        };
        if !(id.is_string() || id.is_number()) {
            return Some(error_v(
                Value::Null,
                INVALID_REQUEST,
                "id must be a string or number",
            ));
        }
        let params = obj.get("params").cloned().unwrap_or(Value::Null);
        let reply = match method {
            "initialize" => Ok(json!({
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": { "tools": { "listChanged": false } },
                "serverInfo": { "name": "leep-mcp", "version": env!("CARGO_PKG_VERSION") },
                "instructions": INSTRUCTIONS,
            })),
            "ping" => Ok(json!({})),
            "tools/list" => Ok(json!({ "tools": tools::definitions() })),
            "tools/call" => self.tools_call(params),
            other => Err((METHOD_NOT_FOUND, format!("method not found: {other}"))),
        };
        Some(match reply {
            Ok(result) => json!({ "jsonrpc": "2.0", "id": id, "result": result }),
            Err((code, m)) => error_v(id, code, &m),
        })
    }

    fn tools_call(&self, params: Value) -> Result<Value, (i64, String)> {
        let name = params.get("name").and_then(Value::as_str).ok_or((
            INVALID_PARAMS,
            "tools/call needs a string `name`".to_string(),
        ))?;
        let args = params.get("arguments").cloned().unwrap_or(Value::Null);
        let out = match self.call_tool(name, args) {
            None => return Err((INVALID_PARAMS, format!("unknown tool: {name}"))),
            Some(Ok(o)) => o,
            Some(Err(e)) => ToolOutput::error(e.0),
        };
        Ok(out.into_json())
    }
}

fn error_v(id: Value, code: i64, message: &str) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } })
}

fn error(id: Value, code: i64, message: &str) -> String {
    error_v(id, code, message).to_string()
}
