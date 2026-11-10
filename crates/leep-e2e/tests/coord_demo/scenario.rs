//! The phase 1 coordination demo (design section E): twenty scripted
//! agents on one repository, each with its own token and `agent.session`,
//! driven through the real `leep-server`, `leep` and `leep-mcp` binaries.
//!
//! [`run`] plays the scenario, writing a narrated Markdown transcript, and
//! answers a [`Report`] of what each agent was told. The test asserts the
//! report's claims; the `coord_demo` example prints the transcript. Nothing
//! here measures time: waits are bounded retries of a command, never a
//! bound on how long something took.

use std::collections::BTreeMap;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::{Barrier, Mutex};
use std::time::Duration;

use serde_json::{json, Value};

use crate::common::{assert_ok, bin, free_port, stderr, stdout, write, Client, Server, Stores};

/// How many times a check that waits on the server (a notice, an ended
/// session) asks again, and how long it sleeps between asks. Generous, so
/// a loaded machine only makes the demo slower.
const TRIES: usize = 120;
const PAUSE: Duration = Duration::from_millis(250);
/// Lines of a command's output kept in the transcript.
const SHOWN_LINES: usize = 48;

/// Where the transcript goes: kept, and echoed to stdout as it is written
/// when `echo` is set.
pub struct Narrator {
    text: Mutex<String>,
    echo: bool,
}

impl Narrator {
    pub fn new(echo: bool) -> Narrator {
        Narrator {
            text: Mutex::new(String::new()),
            echo,
        }
    }

    pub fn say(&self, s: &str) {
        self.text.lock().unwrap().push_str(s);
        if self.echo {
            print!("{s}");
            let _ = std::io::stdout().flush();
        }
    }

    pub fn text(&self) -> String {
        self.text.lock().unwrap().clone()
    }
}

/// How an agent reaches the server.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Via {
    Cli,
    Mcp,
}

/// One notice an agent was told, as it saw it.
#[derive(Clone, Debug)]
pub struct Heard {
    pub level: String,
    /// The line outside any fence: what the server vouches for.
    pub line: String,
    /// The command or tool call whose output carried it.
    pub during: String,
}

/// A leep-mcp process speaking JSON-RPC over its stdio.
struct Mcp {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    next_id: u64,
}

impl Drop for Mcp {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// A tool call's result.
pub struct ToolOut {
    pub text: String,
    pub data: Value,
    pub is_error: bool,
}

/// A command's result.
pub struct CmdOut {
    pub code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
}

pub struct Agent {
    pub name: &'static str,
    pub via: Via,
    pub model: String,
    token: String,
    wc: PathBuf,
    level: Option<&'static str>,
    pub heard: Mutex<Vec<Heard>>,
    /// Every command's stdout and tool call's text, as (what, output).
    pub said: Mutex<Vec<(String, String)>>,
    mcp: Mutex<Option<Mcp>>,
}

/// Everything an agent needs from the demo.
struct World<'a> {
    client: Client,
    n: &'a Narrator,
}

fn quote_arg(a: &str) -> String {
    if !a.is_empty()
        && a.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_./=:@".contains(&b))
    {
        a.to_string()
    } else {
        format!("{a:?}")
    }
}

/// `s` cut to [`SHOWN_LINES`] lines, saying how many were left out.
fn shown(s: &str) -> String {
    let lines: Vec<&str> = s.trim_end().lines().collect();
    if lines.len() <= SHOWN_LINES {
        return lines.join("\n");
    }
    let mut out = lines[..SHOWN_LINES].join("\n");
    out.push_str(&format!("\n... ({} more lines)", lines.len() - SHOWN_LINES));
    out
}

/// The notice lines `leep` printed on stderr (ADR 0096).
fn cli_notices(stderr: &str) -> Vec<(String, String)> {
    stderr
        .lines()
        .filter_map(|l| {
            let rest = l.strip_prefix("notice: [")?;
            let (level, _) = rest.split_once(']')?;
            Some((level.to_string(), l.to_string()))
        })
        .collect()
}

/// The numbered notice lines a leep-mcp tool result ends with.
fn mcp_notices(text: &str) -> Vec<(String, String)> {
    let Some((_, tail)) = text.split_once("Notices about other agents' work near yours") else {
        return Vec::new();
    };
    tail.lines()
        .take_while(|l| !l.starts_with("The fenced block"))
        .filter_map(|l| {
            let (num, rest) = l.split_once(". [")?;
            if num.is_empty() || !num.bytes().all(|b| b.is_ascii_digit()) {
                return None;
            }
            let (level, _) = rest.split_once(']')?;
            Some((level.to_string(), l.to_string()))
        })
        .collect()
}

impl Agent {
    fn new(
        name: &'static str,
        via: Via,
        model: &str,
        token: String,
        wc: PathBuf,
        level: Option<&'static str>,
    ) -> Agent {
        Agent {
            name,
            via,
            model: model.to_string(),
            token,
            wc,
            level,
            heard: Mutex::new(Vec::new()),
            said: Mutex::new(Vec::new()),
            mcp: Mutex::new(None),
        }
    }

    fn note(&self, during: &str, found: Vec<(String, String)>) {
        let mut h = self.heard.lock().unwrap();
        for (level, line) in found {
            h.push(Heard {
                level,
                line,
                during: during.to_string(),
            });
        }
    }

    fn header(&self) -> String {
        let via = match self.via {
            Via::Cli => "leep",
            Via::Mcp => "leep-mcp",
        };
        if self.model.is_empty() {
            return format!("**{}** ({via})", self.name);
        }
        format!("**{}** ({via}, `{}`)", self.name, self.model)
    }

    /// Run `leep` in the agent's working copy, as the agent; notices on
    /// stderr are kept. The transcript gets the command and its output.
    fn leep_quiet(&self, w: &World, args: &[&str]) -> CmdOut {
        self.leep_at(w, &self.wc, args)
    }

    /// As [`Agent::leep_quiet`], in `cwd`.
    fn leep_at(&self, w: &World, cwd: &Path, args: &[&str]) -> CmdOut {
        let mut c = w.client.cmd(cwd);
        c.env("LEEP_TOKEN", &self.token)
            .env("LEEP_AUTHOR", self.name)
            .env("LEEP_AGENT_MODEL", &self.model)
            .args(args);
        if let Some(l) = self.level {
            c.env("LEEP_NOTICE_LEVEL", l);
        }
        let out = c
            .output()
            .unwrap_or_else(|e| panic!("spawn leep {args:?}: {e}"));
        let o = CmdOut {
            code: out.status.code(),
            stdout: stdout(&out),
            stderr: stderr(&out),
        };
        let cmdline = args.iter().map(|a| quote_arg(a)).collect::<Vec<_>>();
        self.said.lock().unwrap().push((
            format!("leep {}", cmdline.join(" ")),
            format!("{}{}", o.stdout, o.stderr),
        ));
        self.note(
            &format!("leep {}", cmdline.join(" ")),
            cli_notices(&o.stderr),
        );
        o
    }

    fn leep_logged(&self, w: &World, log: &mut String, args: &[&str]) -> CmdOut {
        let o = self.leep_quiet(w, args);
        let cmdline = args.iter().map(|a| quote_arg(a)).collect::<Vec<_>>();
        let mut body = shown(&o.stdout);
        let err = o.stderr.trim_end();
        if !err.is_empty() {
            if !body.is_empty() {
                body.push('\n');
            }
            body.push_str(err);
        }
        log.push_str(&format!(
            "{}\n```console\n$ leep {}\n{}\n```\n\n",
            self.header(),
            cmdline.join(" "),
            body
        ));
        o
    }

    fn leep(&self, w: &World, args: &[&str]) -> CmdOut {
        let mut log = String::new();
        let o = self.leep_logged(w, &mut log, args);
        w.n.say(&log);
        o
    }

    /// As [`Agent::leep`], failing the demo unless it exits 0.
    fn leep_ok(&self, w: &World, args: &[&str]) -> CmdOut {
        let o = self.leep(w, args);
        assert_eq!(
            o.code,
            Some(0),
            "{} leep {args:?}\n{}\n{}",
            self.name,
            o.stdout,
            o.stderr
        );
        o
    }

    fn mcp_start(&self, w: &World) -> Mcp {
        let mut c = Command::new(bin("leep-mcp"));
        c.env("LEEP_SERVER", &w.client.server)
            .env("LEEP_TOKEN", &self.token)
            .env("LEEP_AUTHOR", self.name)
            .env("LEEP_AGENT_MODEL", &self.model)
            .env("LEEP_CACHE_DIR", &w.client.cache)
            .env_remove("LEEP_AGENT_SESSION")
            .env_remove("LEEP_NOTICE_LEVEL")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        if let Some(l) = self.level {
            c.env("LEEP_NOTICE_LEVEL", l);
        }
        let mut child = c.spawn().expect("spawn leep-mcp");
        let mut m = Mcp {
            stdin: child.stdin.take().unwrap(),
            stdout: BufReader::new(child.stdout.take().unwrap()),
            child,
            next_id: 1,
        };
        let init = m.request(
            "initialize",
            json!({ "protocolVersion": "2025-06-18", "capabilities": {},
                    "clientInfo": { "name": "coord-demo", "version": "0" } }),
        );
        assert_eq!(init["result"]["protocolVersion"], "2025-06-18", "{init}");
        writeln!(
            m.stdin,
            "{}",
            json!({ "jsonrpc": "2.0", "method": "notifications/initialized" })
        )
        .unwrap();
        m
    }

    /// Call a leep-mcp tool as the agent (one process per agent, kept for
    /// the whole demo); notices in the result are kept.
    fn tool_logged(&self, w: &World, log: &mut String, tool: &str, args: Value) -> ToolOut {
        let mut guard = self.mcp.lock().unwrap();
        if guard.is_none() {
            *guard = Some(self.mcp_start(w));
        }
        let m = guard.as_mut().unwrap();
        let reply = m.request("tools/call", json!({ "name": tool, "arguments": args }));
        drop(guard);
        let r = &reply["result"];
        assert!(r.is_object(), "{} {tool}: {reply}", self.name);
        let text = r["content"][0]["text"].as_str().unwrap_or("").to_string();
        let o = ToolOut {
            data: r["structuredContent"].clone(),
            is_error: r["isError"] == true,
            text,
        };
        self.note(tool, mcp_notices(&o.text));
        self.said
            .lock()
            .unwrap()
            .push((tool.to_string(), o.text.clone()));
        log.push_str(&format!(
            "{}\n````text\n> {tool} {args}\n{}{}\n````\n\n",
            self.header(),
            if o.is_error { "(error) " } else { "" },
            shown(&o.text)
        ));
        o
    }

    fn tool(&self, w: &World, tool: &str, args: Value) -> ToolOut {
        let mut log = String::new();
        let o = self.tool_logged(w, &mut log, tool, args);
        w.n.say(&log);
        o
    }

    pub fn levels(&self) -> BTreeMap<String, usize> {
        let mut m = BTreeMap::new();
        for h in self.heard.lock().unwrap().iter() {
            *m.entry(h.level.clone()).or_insert(0) += 1;
        }
        m
    }

    fn heard_matching(&self, f: impl Fn(&Heard) -> bool) -> Option<Heard> {
        self.heard.lock().unwrap().iter().find(|h| f(h)).cloned()
    }
}

impl Mcp {
    fn request(&mut self, method: &str, params: Value) -> Value {
        let id = self.next_id;
        self.next_id += 1;
        let req = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
        writeln!(self.stdin, "{req}").unwrap();
        self.stdin.flush().unwrap();
        let mut line = String::new();
        self.stdout.read_line(&mut line).unwrap();
        assert!(!line.is_empty(), "leep-mcp closed its stdout on {method}");
        serde_json::from_str(&line).unwrap_or_else(|e| panic!("{e}: {line:?}"))
    }
}

/// A real token and its tokens-file entry, from `leep-server gen-token`.
fn gen_token(name: &str, role: &str) -> (String, Value) {
    let out = Command::new(bin("leep-server"))
        .args(["gen-token", "--name", name, "--role", role])
        .output()
        .unwrap();
    assert_ok(&out, "leep-server gen-token");
    let text = stdout(&out);
    let field = |key: &str| {
        text.lines()
            .find_map(|l| l.strip_prefix(key))
            .unwrap_or_else(|| panic!("no {key} in {text}"))
            .trim()
            .to_string()
    };
    (
        field("token:"),
        serde_json::from_str(&field("entry:")).unwrap(),
    )
}

/// One of the agents that work alone: a directory of their own (two share
/// `src/api` and one shares nothing), a task, and an edit.
struct Solo {
    name: &'static str,
    via: Via,
    path: &'static str,
    title: &'static str,
    key: &'static str,
    level: Option<&'static str>,
}

const SOLOS: [Solo; 14] = [
    solo(
        "gus",
        "src/search/mod.rs",
        "Rank exact matches first in search",
        "SRCH-3",
    ),
    solo(
        "hal",
        "src/storage/mod.rs",
        "Batch small writes in the storage layer",
        "STO-8",
    ),
    solo(
        "ivy",
        "src/ui/mod.rs",
        "Show a spinner while the page loads",
        "UI-21",
    ),
    solo(
        "jo",
        "src/cli/mod.rs",
        "Add a --quiet flag to the command line",
        "CLI-4",
    ),
    solo(
        "kit",
        "src/metrics/mod.rs",
        "Export request latency histograms",
        "MET-2",
    ),
    solo(
        "lu",
        "src/notify/mod.rs",
        "Retry failed notification sends with backoff",
        "NOT-6",
    ),
    solo(
        "max",
        "src/export/mod.rs",
        "Stream CSV exports instead of buffering",
        "EXP-1",
    ),
    solo(
        "ned",
        "src/i18n/mod.rs",
        "Add German translations",
        "I18N-9",
    ),
    solo(
        "oz",
        "src/cache/mod.rs",
        "Evict cache entries by size, not count",
        "CAC-5",
    ),
    solo(
        "pia",
        "src/jobs/mod.rs",
        "Retry failed jobs with backoff",
        "JOB-7",
    ),
    solo(
        "quin",
        "src/admin/mod.rs",
        "Paginate the admin user list",
        "ADM-3",
    ),
    Solo {
        level: Some("near"),
        ..solo(
            "rex",
            "src/api/routes.rs",
            "Version the public routes under /v2",
            "API-10",
        )
    },
    Solo {
        via: Via::Mcp,
        level: Some("near"),
        ..solo(
            "sol",
            "src/api/handlers.rs",
            "Return 422 for invalid JSON bodies",
            "API-11",
        )
    },
    Solo {
        via: Via::Mcp,
        ..solo(
            "tess",
            "docs/billing.md",
            "Document how refunds are taxed",
            "DOC-2",
        )
    },
];

const fn solo(
    name: &'static str,
    path: &'static str,
    title: &'static str,
    key: &'static str,
) -> Solo {
    Solo {
        name,
        via: Via::Cli,
        path,
        title,
        key,
        level: None,
    }
}

/// The repository before any agent starts: one commit per area, as a
/// project grows, so a file's co-changes are its own area's.
const SEED: &[(&str, &[(&str, &str)])] = &[
    (
        "repo: skeleton",
        &[
            ("README.md", "# shop\n\nA small web shop.\n"),
            ("OWNERS", "maint\n"),
            ("docs/auth.md", "# Auth\n\nTokens are signed with rotating keys.\n"),
            ("docs/billing.md", "# Billing\n\nInvoices sum their lines.\n"),
            ("docs/api.md", "# API\n\nJSON over HTTP.\n"),
        ],
    ),
    (
        "auth: tokens, sessions and signing keys",
        &[
            ("src/auth/OWNERS", "rhea\nsam\n"),
            ("src/auth/mod.rs", "pub mod keys;\npub mod session;\npub mod token;\n"),
            (
                "src/auth/token.rs",
                "pub fn parse_token(raw: &str) -> Option<Token> {\n    let t = decode(raw)?;\n    Some(t)\n}\n\n\n\n\n\npub fn audience(t: &Token) -> &str {\n    &t.aud\n}\n",
            ),
            ("src/auth/session.rs", "pub fn new_session() {}\n"),
            ("src/auth/keys.rs", "pub fn current_key() -> Key {\n    Key::load()\n}\n"),
        ],
    ),
    (
        "billing: invoices and tax",
        &[
            ("src/billing/OWNERS", "carry identical\nmoe\n"),
            ("src/billing/mod.rs", "pub mod invoice;\npub mod tax;\n"),
            (
                "src/billing/invoice.rs",
                "pub fn total(lines: &[Line]) -> Cents {\n    lines.iter().map(|l| l.cents).sum()\n}\n",
            ),
            ("src/billing/tax.rs", "pub fn vat(c: Cents) -> Cents {\n    c / 5\n}\n"),
        ],
    ),
    (
        "api: routes and handlers",
        &[
            ("src/api/mod.rs", "pub mod handlers;\npub mod routes;\n"),
            ("src/api/routes.rs", "pub fn routes() {}\n"),
            ("src/api/handlers.rs", "pub fn create() {}\n"),
        ],
    ),
];

/// What the demo saw, for the test to judge and the example to print.
pub struct Report {
    pub transcript: String,
    pub claims: Vec<Claim>,
}

pub struct Claim {
    pub n: usize,
    pub what: &'static str,
    pub pass: bool,
    pub evidence: String,
}

impl Report {
    /// Panics naming every claim that failed.
    pub fn assert_all(&self) {
        let failed: Vec<String> = self
            .claims
            .iter()
            .filter(|c| !c.pass)
            .map(|c| format!("{}. {}: {}", c.n, c.what, c.evidence))
            .collect();
        assert!(
            failed.is_empty(),
            "claims failed:\n{}\n\ntranscript:\n{}",
            failed.join("\n"),
            self.transcript
        );
    }
}

/// Ask `f` until it answers, at most [`TRIES`] times; between asks the
/// agent runs `poll` (a command that carries notices).
fn until<T>(mut f: impl FnMut() -> Option<T>, mut poll: impl FnMut()) -> Option<T> {
    for i in 0..TRIES {
        if let Some(v) = f() {
            return Some(v);
        }
        if i + 1 < TRIES {
            std::thread::sleep(PAUSE);
            poll();
        }
    }
    None
}

fn head_of(w: &World, maint: &Agent) -> String {
    let out = maint.leep_quiet(w, &["--json", "log", "-n", "1", "main"]);
    let v: Value = serde_json::from_str(&out.stdout).unwrap();
    v["entries"][0]["id"].as_str().unwrap().to_string()
}

/// Play the demo against a fresh store. `root` holds the server log,
/// tokens, working copies and cache.
pub fn run(stores: &Stores, root: &Path, n: &Narrator) -> Report {
    // ---- Tokens: one per agent, a maintainer and a CI token.
    let mut entries = Vec::new();
    let mut tokens: BTreeMap<&str, String> = BTreeMap::new();
    let auth = ["ada", "bo", "cy"];
    let dups = ["dee", "eli"];
    let names: Vec<&'static str> = auth
        .iter()
        .chain(&dups)
        .copied()
        .chain(["fay"])
        .chain(SOLOS.iter().map(|s| s.name))
        .collect();
    assert_eq!(names.len(), 20);
    for name in names.iter().copied().chain(["maint"]) {
        let (t, e) = gen_token(name, "write");
        tokens.insert(name, t);
        entries.push(e);
    }
    let (ci_t, mut ci_e) = gen_token("ci", "write");
    ci_e["checks"] = json!(["test"]);
    ci_e["review"] = json!(false);
    entries.push(ci_e);
    let tokens_file = root.join("tokens.json");
    fs::write(&tokens_file, json!({ "tokens": entries }).to_string()).unwrap();
    let policy = root.join("checks.json");
    fs::write(
        &policy,
        r#"{ "refs": { "main": { "required": ["test"] } } }"#,
    )
    .unwrap();
    let server = Server::start_with(
        free_port(),
        stores,
        root,
        &[
            "--tokens-file",
            tokens_file.to_str().unwrap(),
            "--checks-policy-file",
            policy.to_str().unwrap(),
        ],
    );
    let w = World {
        client: Client::new(&server.url, &root.join("cache")),
        n,
    };
    let model = |i: usize| ["demo-model-a", "demo-model-b", "demo-model-c"][i % 3];
    let mut agents: BTreeMap<&str, Agent> = BTreeMap::new();
    for (i, name) in names.iter().copied().enumerate() {
        let solo = SOLOS.iter().find(|s| s.name == name);
        let via = match name {
            "dee" | "eli" | "fay" => Via::Mcp,
            _ => solo.map_or(Via::Cli, |s| s.via),
        };
        let level = solo.and_then(|s| s.level);
        agents.insert(
            name,
            Agent::new(
                name,
                via,
                model(i),
                tokens[name].clone(),
                root.join("agents").join(name),
                level,
            ),
        );
    }
    let seed_wc = root.join("seed");
    let maint = Agent::new(
        "maint",
        Via::Cli,
        "",
        tokens["maint"].clone(),
        seed_wc.clone(),
        None,
    );
    let ci_agent = Agent::new("ci", Via::Cli, "", ci_t, seed_wc.clone(), None);
    let a = |name: &str| &agents[name];

    n.say(
        "# Leep coordination demo (phase 1)\n\n\
         Twenty scripted agents share one repository on one `leep-server`. Each has its own \
         token (its principal) and its own `agent.session`. Seventeen use the `leep` CLI in a \
         working copy of their own; five (dee, eli, fay, sol, tess) speak MCP to a `leep-mcp` \
         process each. Notices print on stderr (CLI) or at the end of a tool result (MCP). \
         Models are made-up labels.\n\n",
    );

    // ---- Act 0: the repository, its owners and CI.
    n.say("## 0. The repository\n\nA maintainer (no agent session) commits the project area by area; `src/auth/OWNERS` names rhea and sam, `src/billing/OWNERS` says `carry identical`. CI requires the check `test` on `main` and passes the seed.\n\n");
    maint.leep_at(&w, root, &["init", seed_wc.to_str().unwrap()]);
    let mut seed_log = String::new();
    let mut areas: Vec<(String, Vec<(String, String)>)> = SEED
        .iter()
        .map(|(m, files)| {
            (
                m.to_string(),
                files
                    .iter()
                    .map(|(p, b)| (p.to_string(), b.to_string()))
                    .collect(),
            )
        })
        .collect();
    for s in &SOLOS {
        if s.path.starts_with("src/api/") || s.path.starts_with("docs/") {
            continue;
        }
        let area = s.path.split('/').nth(1).unwrap();
        areas.push((
            format!("{area}: first version"),
            vec![(
                s.path.to_string(),
                format!("// {area}\npub fn init() {{}}\n"),
            )],
        ));
    }
    for (msg, files) in &areas {
        for (p, b) in files {
            write(&seed_wc, p, b);
        }
        let o = maint.leep_quiet(&w, &["commit", "-m", msg]);
        assert_eq!(o.code, Some(0), "{}", o.stderr);
        let o = maint.leep_quiet(&w, &["land"]);
        assert_eq!(o.code, Some(0), "{}", o.stderr);
        seed_log.push_str(&format!("- `{msg}`: {} file(s)\n", files.len()));
    }
    n.say(&format!("{seed_log}\n"));
    let seed_head = head_of(&w, &maint);
    ci_agent.leep_ok(&w, &["checks", "post", &seed_head, "test", "pass"]);
    for name in &names {
        if agents[name].via == Via::Cli {
            let wc = agents[name].wc.clone();
            fs::create_dir_all(wc.parent().unwrap()).unwrap();
            maint.leep_at(&w, root, &["init", wc.to_str().unwrap()]);
        }
    }

    // ---- The fourteen solo agents work concurrently with acts 1 to 3.
    n.say("## 1. Fourteen agents work alone, concurrently\n\nThese fourteen run in parallel with sections 2 and 3; their transcripts follow in section 6. Each starts its task, edits one file of its own, commits and lands. rex and sol both work in `src/api/` (different files) and ask for notices down to `near`; the rest keep the default (`same_file` and up).\n\n");
    let barrier = Barrier::new(SOLOS.len() + 1);
    let solo_logs: Mutex<Vec<(usize, String, bool)>> = Mutex::new(Vec::new());
    let mut claims = Vec::new();
    std::thread::scope(|scope| {
        for (i, s) in SOLOS.iter().enumerate() {
            let w = &w;
            let agent = a(s.name);
            let barrier = &barrier;
            let solo_logs = &solo_logs;
            scope.spawn(move || {
                barrier.wait();
                let mut log = String::new();
                let landed = run_solo(w, agent, s, &mut log);
                solo_logs.lock().unwrap().push((i, log, landed));
            });
        }
        barrier.wait();

        // ---- Act 2: three agents in src/auth, two on one file.
        n.say("## 2. Three agents in `src/auth/`, two of them on one file\n\nada rotates signing keys in `keys.rs`. bo and cy both need `token.rs`: bo claims it, cy only scopes it. Neither has landed when they are warned.\n\n");
        let ada = a("ada");
        ada.leep_ok(
            &w,
            &[
                "work",
                "start",
                "-t",
                "Rotate the auth signing keys",
                "--scope",
                "src/auth/keys.rs",
                "--task",
                "AUTH-7",
            ],
        );
        write(
            &ada.wc,
            "src/auth/keys.rs",
            "pub fn current_key() -> Key {\n    Key::load_rotating()\n}\n",
        );
        ada.leep_ok(&w, &["commit", "-m", "auth: load rotating keys"]);

        let bo = a("bo");
        bo.leep_ok(
            &w,
            &[
                "work",
                "start",
                "-t",
                "Reject expired tokens in parse_token",
                "--claim",
                "src/auth/token.rs",
                "--task",
                "AUTH-9",
            ],
        );
        let cy = a("cy");
        cy.leep_ok(
            &w,
            &[
                "work",
                "start",
                "-t",
                "Check the token audience",
                "--scope",
                "src/auth/token.rs",
                "--task",
                "AUTH-11",
            ],
        );
        let token = fs::read_to_string(bo.wc.join("src/auth/token.rs")).unwrap();
        write(
            &bo.wc,
            "src/auth/token.rs",
            &token.replace(
                "    Some(t)\n",
                "    if t.expired() {\n        return None;\n    }\n    Some(t)\n",
            ),
        );
        bo.leep_ok(&w, &["commit", "-m", "auth: reject expired tokens"]);
        write(
            &cy.wc,
            "src/auth/token.rs",
            &token.replace(
                "    &t.aud\n",
                "    assert!(!t.aud.is_empty());\n    &t.aud\n",
            ),
        );
        cy.leep_ok(&w, &["commit", "-m", "auth: check the audience"]);
        bo.leep_ok(&w, &["status"]);

        // Claim 1: leep who src/auth/ shows the three.
        let who = maint.leep(&w, &["who", "src/auth/"]);
        let who_json = maint.leep_quiet(&w, &["--json", "who", "src/auth/"]);
        let v: Value = serde_json::from_str(&who_json.stdout).unwrap_or(Value::Null);
        let mut principals: Vec<String> = v["sessions"]
            .as_array()
            .map(|s| {
                s.iter()
                    .map(|s| s["principal"].as_str().unwrap_or("").to_string())
                    .collect()
            })
            .unwrap_or_default();
        principals.sort();
        claims.push(Claim {
            n: 1,
            what: "`leep who src/auth/` shows the three agents working there",
            pass: principals == ["ada", "bo", "cy"],
            evidence: format!(
                "principals {principals:?}; {} session line(s) printed",
                who.stdout.lines().filter(|l| l.starts_with('#')).count()
            ),
        });

        // Claim 2: bo and cy are each warned about the other before either lands.
        let id_of = |agent: &Agent| -> Option<u64> {
            v["sessions"]
                .as_array()?
                .iter()
                .find(|s| s["principal"] == agent.name)?["id"]
                .as_u64()
        };
        let (bo_id, cy_id) = (id_of(bo), id_of(cy));
        // By a notice, or by the pack of the same `work start`, which then
        // leaves the notice out rather than say it twice.
        let warned = |agent: &Agent, other: Option<u64>| {
            let other = other?;
            let about = |l: &str| {
                (l.contains("[same_file]") || l.contains("[claimed]"))
                    && l.contains(&format!("session #{other} "))
            };
            agent.heard_matching(|h| about(&h.line)).or_else(|| {
                agent.said.lock().unwrap().iter().find_map(|(during, out)| {
                    let line = out.lines().find(|l| l.starts_with("  [") && about(l))?;
                    Some(Heard {
                        level: "pack".into(),
                        line: format!("pack:{line}"),
                        during: during.clone(),
                    })
                })
            })
        };
        let bo_warned = until(
            || warned(bo, cy_id),
            || drop(bo.leep_quiet(&w, &["status"])),
        );
        let cy_warned = until(
            || warned(cy, bo_id),
            || drop(cy.leep_quiet(&w, &["status"])),
        );
        claims.push(Claim {
            n: 2,
            what: "two agents editing one file are warned before either lands",
            pass: bo_warned.is_some() && cy_warned.is_some(),
            evidence: format!(
                "bo during `{}`: {}\ncy during `{}`: {}",
                bo_warned.as_ref().map_or("-", |h| &h.during),
                bo_warned.as_ref().map_or("(none)", |h| &h.line),
                cy_warned.as_ref().map_or("-", |h| &h.during),
                cy_warned.as_ref().map_or("(none)", |h| &h.line),
            ),
        });

        // ---- Act 3: two agents given the same issue, over MCP.
        n.say("## 3. Two agents given the same issue (MCP)\n\ndee and eli are both handed issue `BILL-42`, worded differently. Each starts with `leep_task_start`, the one call an agent makes before it edits.\n\n");
        let dee = a("dee");
        let d = dee.tool(
            &w,
            "leep_task_start",
            json!({ "task": "Fix rounding of invoice totals\nRound half to even when summing line items",
                    "task_key": "BILL-42", "paths": ["src/billing/invoice.rs"] }),
        );
        assert!(!d.is_error, "{}", d.text);
        let eli = a("eli");
        let e = eli.tool(
            &w,
            "leep_task_start",
            json!({ "task": "Invoice totals are off by one cent",
                    "task_key": "BILL-42", "paths": ["src/billing/invoice.rs"] }),
        );
        assert!(!e.is_error, "{}", e.text);
        let dup_of_dee = |pack: &Value| {
            pack["duplicates"]
                .as_array()
                .and_then(|v| v.iter().find(|x| x["principal"] == "dee").cloned())
        };
        let mut in_pack = dup_of_dee(&e.data["context"]);
        // A section past its bound on a loaded machine is listed as
        // omitted; the pack is a read, so asking again is harmless.
        for _ in 0..5 {
            if in_pack.is_some() {
                break;
            }
            std::thread::sleep(PAUSE);
            let again = eli.tool(
                &w,
                "leep_context",
                json!({ "task": "Invoice totals are off by one cent", "task_key": "BILL-42",
                        "paths": ["src/billing/invoice.rs"] }),
            );
            in_pack = dup_of_dee(&again.data);
        }
        let as_notice =
            eli.heard_matching(|h| h.level == "possible_duplicate" && h.line.contains("of dee"));
        claims.push(Claim {
            n: 3,
            what: "two agents given the same task key are told at `leep_task_start`",
            // The pack is bounded but complete when asked again; the
            // notice part may be left out under load and come later.
            pass: in_pack.is_some(),
            evidence: format!(
                "eli's pack: {}; eli's notice: {}",
                in_pack
                    .map(|x| format!(
                        "duplicates[] session {} of {} by {}",
                        x["session"], x["principal"], x["by"]
                    ))
                    .unwrap_or("(none)".into()),
                as_notice.map_or("(none)".into(), |h| h.line)
            ),
        });
    });
    let mut logs = solo_logs.into_inner().unwrap();
    logs.sort_by_key(|l| l.0);

    // ---- Act 4: one lands; the other is told which changeset.
    n.say("## 4. One lands; the others are told which changeset\n\nbo lands `token.rs`. cy's next command names the changeset. Then dee lands the invoice fix over MCP, and eli's next tool call names it.\n\n");
    let bo = a("bo");
    let cy = a("cy");
    bo.leep_ok(&w, &["land"]);
    let bo_landed = head_of(&w, &maint);
    let cy_told = until(
        || cy.heard_matching(|h| h.level == "landed" && h.line.contains(&bo_landed[..12])),
        || drop(cy.leep(&w, &["status"])),
    );
    // CI tests the new head and fails: main is red, and bo's land is a suspect.
    n.say("CI runs `test` on the new head and it fails: `main` is red.\n\n");
    ci_agent.leep_ok(
        &w,
        &[
            "checks",
            "post",
            &bo_landed,
            "test",
            "fail",
            "-m",
            "token_expiry_test failed",
        ],
    );

    // ---- Act 5: a new agent's first call.
    n.say("## 5. A new agent's first call\n\nfay is new, and is asked to add refresh tokens to `parse_token`. Its first call is `leep_task_start`; nothing else has told it anything.\n\n");
    let fay = a("fay");
    let task = json!({ "task": "Add refresh tokens to `parse_token`\nIssue a refresh token beside each access token in src/auth/token.rs",
                       "task_key": "AUTH-12", "paths": ["src/auth/token.rs"] });
    let mut f = fay.tool(&w, "leep_task_start", task.clone());
    assert!(!f.is_error, "{}", f.text);
    let mut pack = f.data["context"].clone();
    // A section the server could not make in time is listed as omitted;
    // ask again (a pack is a read and marks nothing said).
    for _ in 0..5 {
        if pack["omitted"].as_array().is_none_or(|o| o.is_empty()) {
            break;
        }
        std::thread::sleep(PAUSE);
        f = fay.tool(
            &w,
            "leep_context",
            json!({ "task": task["task"], "task_key": "AUTH-12", "paths": ["src/auth/token.rs"] }),
        );
        pack = f.data.clone();
    }
    let owners_ok = pack["owners"].as_array().is_some_and(|o| {
        o.iter().any(|o| {
            o["untrusted"]["path"] == "src/auth/token.rs"
                && o["untrusted"]["owners"]
                    .as_array()
                    .is_some_and(|v| v.contains(&json!("rhea")))
        })
    });
    let hist = pack["history"].as_array().and_then(|h| {
        h.iter()
            .find(|h| h["untrusted"]["path"] == "src/auth/token.rs")
            .cloned()
    });
    let hist_ok = hist.as_ref().is_some_and(|h| {
        h["created_by"] == "bo" && h["untrusted"]["agent_model"] == json!(a("bo").model)
    });
    let live: Vec<String> = pack["in_flight"]
        .as_array()
        .map(|v| {
            v.iter()
                .map(|x| {
                    format!(
                        "{}:{}",
                        x["principal"].as_str().unwrap_or("?"),
                        x["level"].as_str().unwrap_or("?")
                    )
                })
                .collect()
        })
        .unwrap_or_default();
    let live_ok = live.iter().any(|l| l.starts_with("cy:"));
    let health = pack["ref_health"].clone();
    let health_ok = health["red"]["check"] == "test"
        && health["suspects"]
            .as_array()
            .is_some_and(|s| s.iter().any(|s| s["changeset"] == json!(bo_landed)));
    claims.push(Claim {
        n: 5,
        what: "a new agent's first call returns owners, history with models, live work and ref health",
        pass: owners_ok && hist_ok && live_ok && health_ok,
        evidence: format!(
            "owners ok: {owners_ok}; last change of token.rs: {} by {} model {}; in flight: {live:?}; \
             ref: red check {}, suspects {}; omitted {}",
            hist.as_ref().map_or("-".into(), |h| h["changeset"].as_str().unwrap_or("?")[..12].to_string()),
            hist.as_ref().map_or(Value::Null, |h| h["created_by"].clone()),
            hist.as_ref().map_or(Value::Null, |h| h["untrusted"]["agent_model"].clone()),
            health["red"]["check"],
            health["suspects"].as_array().map_or(0, |s| s.len()),
            pack["omitted"]
        ),
    });

    // The duplicate pair: dee lands; eli is told.
    n.say("Back to `BILL-42`: dee lands its fix. eli, about to start editing, reads the file.\n\n");
    let dee = a("dee");
    let d = dee.tool(
        &w,
        "leep_commit_and_land",
        json!({ "message": "billing: round invoice totals half to even",
                "changes": [{ "path": "src/billing/invoice.rs",
                              "content": "pub fn total(lines: &[Line]) -> Cents {\n    round_half_even(lines.iter().map(|l| l.exact).sum())\n}\n" }] }),
    );
    assert!(!d.is_error, "{}", d.text);
    let dee_landed = d.data["landed"].as_str().unwrap_or("").to_string();
    let eli = a("eli");
    eli.tool(
        &w,
        "leep_read_file",
        json!({ "path": "src/billing/invoice.rs" }),
    );
    let eli_told = until(
        || {
            eli.heard_matching(|h| {
                h.level == "landed" && !dee_landed.is_empty() && h.line.contains(&dee_landed)
            })
        },
        || drop(eli.tool(&w, "leep_refs", json!({}))),
    );
    claims.push(Claim {
        n: 4,
        what: "one lands; the other gets a `landed` notice naming the changeset",
        pass: cy_told.is_some() && eli_told.is_some(),
        evidence: format!(
            "cy during `{}`: {}\neli during `{}`: {}",
            cy_told.as_ref().map_or("-", |h| &h.during),
            cy_told.as_ref().map_or("(none)", |h| &h.line),
            eli_told.as_ref().map_or("-", |h| &h.during),
            eli_told.as_ref().map_or("(none)", |h| &h.line),
        ),
    });

    // The rest of the auth work lands.
    n.say("cy merges bo's change on the server (a clean rebase: other lines of the file) and lands; ada lands the keys.\n\n");
    cy.leep_ok(&w, &["land"]);
    a("ada").leep_ok(&w, &["land"]);

    // ---- The solo agents' transcripts and claim 6.
    n.say("## 6. The fourteen solo agents\n\n");
    let mut solo_ok = true;
    let mut solo_evidence = Vec::new();
    for (i, log, landed) in &logs {
        n.say(log);
        let s = &SOLOS[*i];
        let levels = a(s.name).levels();
        let loud: usize = levels
            .iter()
            .filter(|(l, _)| l.as_str() != "near")
            .map(|(_, c)| c)
            .sum();
        if !landed || loud > 0 {
            solo_ok = false;
            solo_evidence.push(format!("{}: landed {landed}, notices {levels:?}", s.name));
        }
    }
    let near: usize = SOLOS
        .iter()
        .map(|s| a(s.name).levels().get("near").copied().unwrap_or(0))
        .sum();
    claims.push(Claim {
        n: 6,
        what: "the other fourteen land cleanly with no notices about each other but `near`",
        pass: solo_ok,
        evidence: if solo_evidence.is_empty() {
            format!(
                "all {} landed; {near} `near` notice(s) between rex and sol, nothing louder",
                SOLOS.len()
            )
        } else {
            solo_evidence.join("; ")
        },
    });

    // ---- Claims 7 to 9: what the demo's first run found wrong reads right.
    let said = |name: &str, what: &str| -> Vec<String> {
        a(name)
            .said
            .lock()
            .unwrap()
            .iter()
            .filter(|(w, _)| w.starts_with(what))
            .map(|(_, t)| t.clone())
            .collect()
    };
    // 7: a pack and the notice in the same output agree, and a co-changed
    // file is never called the agent's own.
    // rex and sol start concurrently, so whichever starts second sees the
    // other in its pack: every pack line naming the other must say `near`,
    // as their notices do.
    let rex_start = said("rex", "leep work start").join("\n");
    let sol_start = said("sol", "leep_task_start").join("\n");
    let mut pair: Vec<String> = rex_start
        .lines()
        .filter(|l| l.starts_with("  [") && l.contains("of \"sol\""))
        .chain(
            sol_start
                .lines()
                .filter(|l| l.starts_with("- session:") && l.ends_with("of rex")),
        )
        .map(|l| l.trim().to_string())
        .collect();
    pair.dedup();
    let agree = pair.iter().all(|l| l.contains("[near]"));
    let bo_start = said("bo", "leep work start").join("\n");
    let bo_mislabelled = bo_start.contains("(your \"src/auth/keys.rs\")");
    claims.push(Claim {
        n: 7,
        what: "a pack ranks live work by the agent's own files, as its notices do",
        pass: agree && !bo_mislabelled,
        evidence: format!(
            "rex/sol pack lines about each other: {pair:?}; bo's pack calls keys.rs bo's own: \
             {bo_mislabelled}"
        ),
    });
    // 8: both of a duplicate pair are told, and the twin's land says so.
    let dee_told = a("dee").heard_matching(|h| {
        h.level == "possible_duplicate" && h.line.contains("of eli by task_key")
    });
    let eli_twin_landed = a("eli").heard_matching(|h| {
        h.level == "landed" && h.line.contains("by task_key") && h.line.contains("your duplicate")
    });
    claims.push(Claim {
        n: 8,
        what:
            "the first of a duplicate pair is told too, and its twin hears that it landed the task",
        pass: dee_told.is_some() && eli_twin_landed.is_some(),
        evidence: format!(
            "dee during `{}`: {}\neli during `{}`: {}",
            dee_told.as_ref().map_or("-", |h| &h.during),
            dee_told.as_ref().map_or("(none)", |h| &h.line),
            eli_twin_landed.as_ref().map_or("-", |h| &h.during),
            eli_twin_landed.as_ref().map_or("(none)", |h| &h.line),
        ),
    });
    // 9: nothing in one answer is said twice.
    let eli_start = said("eli", "leep_task_start").join("\n");
    let eli_dup_lines = eli_start
        .lines()
        .filter(|l| l.contains("[possible_duplicate]") && l.contains("of dee"))
        .count();
    let dee_start = said("dee", "leep_task_start").join("\n");
    let explained = dee_start
        .matches("re-request review after a rebase")
        .count();
    claims.push(Claim {
        n: 9,
        what: "a task start says each thing once: one duplicate line, one policy explanation",
        pass: eli_dup_lines == 1 && explained <= 1,
        evidence: format!(
            "eli's task start names dee as a duplicate {eli_dup_lines} time(s); dee's pack \
             explains `carry identical` {explained} time(s)"
        ),
    });

    // ---- Counts.
    let group = |name: &str| -> &'static str {
        if auth.contains(&name) {
            "src/auth"
        } else if dups.contains(&name) {
            "BILL-42"
        } else if name == "fay" {
            "newcomer"
        } else {
            "solo"
        }
    };
    let counts: Vec<(String, &'static str, BTreeMap<String, usize>)> = names
        .iter()
        .map(|name| (name.to_string(), group(name), a(name).levels()))
        .collect();
    n.say("## 7. Results\n\n| # | Claim | Result | What the agents saw |\n|---|---|---|---|\n");
    let mut sorted: Vec<&Claim> = claims.iter().collect();
    sorted.sort_by_key(|c| c.n);
    for c in sorted {
        n.say(&format!(
            "| {} | {} | {} | {} |\n",
            c.n,
            c.what,
            if c.pass { "PASS" } else { "FAIL" },
            c.evidence.replace('\n', "<br>").replace('|', "\\|")
        ));
    }
    let all_levels = [
        "landed",
        "possible_duplicate",
        "claimed",
        "same_file",
        "near",
    ];
    n.say("\n### Notices heard per agent\n\n| Agent | Via | Group | landed | possible_duplicate | claimed | same_file | near |\n|---|---|---|---|---|---|---|---|\n");
    for (name, g, levels) in &counts {
        let cells: Vec<String> = all_levels
            .iter()
            .map(|l| levels.get(*l).copied().unwrap_or(0).to_string())
            .collect();
        n.say(&format!(
            "| {name} | {} | {g} | {} |\n",
            if a(name).via == Via::Cli {
                "CLI"
            } else {
                "MCP"
            },
            cells.join(" | ")
        ));
    }
    n.say("\n");
    for name in &names {
        let h = a(name).heard.lock().unwrap().clone();
        if h.is_empty() {
            continue;
        }
        n.say(&format!("- **{name}**:\n"));
        for x in h {
            n.say(&format!("  - during `{}`: `{}`\n", x.during, x.line));
        }
    }
    n.say("\n");
    drop(agents);
    drop(server);
    Report {
        transcript: n.text(),
        claims,
    }
}

/// One solo agent's whole task; answers whether it landed. Its transcript
/// goes to `log`, printed after the concurrent part.
fn run_solo(w: &World, agent: &Agent, s: &Solo, log: &mut String) -> bool {
    let body = format!("// {}\npub fn init() {{}}\n// {}\n", s.title, s.key);
    match s.via {
        Via::Cli => {
            let args = [
                "work", "start", "-t", s.title, "--scope", s.path, "--task", s.key,
            ];
            let o = agent.leep_logged(w, log, &args);
            if o.code != Some(0) {
                return false;
            }
            write(&agent.wc, s.path, &body);
            let msg = format!(
                "{}: {}",
                s.path.split('/').nth(1).unwrap_or("docs"),
                s.title.to_lowercase()
            );
            if agent.leep_logged(w, log, &["commit", "-m", &msg]).code != Some(0) {
                return false;
            }
            let landed = agent.leep_logged(w, log, &["land"]).code == Some(0);
            agent.leep_logged(w, log, &["status"]);
            landed
        }
        Via::Mcp => {
            let o = agent.tool_logged(
                w,
                log,
                "leep_task_start",
                json!({ "task": s.title, "task_key": s.key, "paths": [s.path] }),
            );
            if o.is_error {
                return false;
            }
            let o = agent.tool_logged(
                w,
                log,
                "leep_commit_and_land",
                json!({ "message": format!("{}: {}", s.name, s.title.to_lowercase()),
                        "changes": [{ "path": s.path, "content": body }] }),
            );
            !o.is_error && o.data["status"] == "landed"
        }
    }
}
