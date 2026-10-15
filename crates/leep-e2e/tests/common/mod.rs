//! Helpers for driving the real `leep-server`, `leep` and `leep-git` binaries.

#![allow(dead_code)]

use std::fs;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

/// Tests spawn many processes; running them one at a time keeps timings stable.
pub fn serial() -> MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::new(());
    LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

/// Locate a workspace binary next to the test executable's profile directory.
pub fn bin(name: &str) -> PathBuf {
    let exe = std::env::current_exe().expect("current_exe");
    // target/<profile>/deps/<test-exe>  ->  target/<profile>/<name>
    let profile_dir = exe
        .parent()
        .and_then(Path::parent)
        .expect("test executable has no profile directory");
    let path = profile_dir.join(format!("{name}{}", std::env::consts::EXE_SUFFIX));
    assert!(
        path.is_file(),
        "binary `{name}` not found at {}; run `cargo build --workspace` first",
        path.display()
    );
    path
}

pub fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .unwrap()
        .port()
}

/// Where the server and `leep-git` keep their state.
pub struct Stores {
    pub meta: String,
    pub blobs: String,
}

impl Stores {
    pub fn sqlite(dir: &Path) -> Stores {
        Stores::with_meta(
            dir,
            format!("sqlite://{}?mode=rwc", dir.join("meta.db").display()),
        )
    }

    pub fn with_meta(dir: &Path, meta: String) -> Stores {
        // `leep-git` does not create parent directories the way the server does.
        fs::create_dir_all(dir.join("blobs")).unwrap();
        Stores {
            meta,
            blobs: format!("file://{}", dir.join("blobs").display()),
        }
    }
}

/// A running `leep-server`; killed on drop.
pub struct Server {
    child: Option<Child>,
    pub addr: SocketAddr,
    pub url: String,
    log: PathBuf,
}

impl Server {
    pub fn start(stores: &Stores, log_dir: &Path) -> Server {
        Self::start_on(free_port(), stores, log_dir)
    }

    pub fn start_on(port: u16, stores: &Stores, log_dir: &Path) -> Server {
        Self::start_with(port, stores, log_dir, &[])
    }

    /// Start with extra `leep-server` arguments (for example signing flags).
    pub fn start_with(port: u16, stores: &Stores, log_dir: &Path, extra: &[&str]) -> Server {
        let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
        let log = log_dir.join(format!("server-{port}-{}.log", std::process::id()));
        // Appended, so a restarted server keeps the log of the one before it.
        let out = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log)
            .unwrap();
        let err = out.try_clone().unwrap();
        let child = Command::new(bin("leep-server"))
            .args(["--listen", &addr.to_string()])
            .args(["--meta", &stores.meta, "--blobs", &stores.blobs])
            .args(extra)
            .env("RUST_LOG", "warn")
            .stdin(Stdio::null())
            .stdout(out)
            .stderr(err)
            .spawn()
            .expect("spawn leep-server");
        let mut s = Server {
            child: Some(child),
            addr,
            url: format!("http://{addr}"),
            log,
        };
        s.wait_healthy();
        s
    }

    fn wait_healthy(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Some(status) = self.child.as_mut().unwrap().try_wait().unwrap() {
                panic!(
                    "leep-server exited early ({status}); log:\n{}",
                    self.log_text()
                );
            }
            if health_ok(self.addr) {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "leep-server not healthy within 30s; log:\n{}",
                self.log_text()
            );
            std::thread::sleep(Duration::from_millis(25));
        }
    }

    pub fn log_text(&self) -> String {
        fs::read_to_string(&self.log).unwrap_or_default()
    }

    /// SIGKILL, so durability does not depend on a clean shutdown.
    pub fn kill(&mut self) {
        if let Some(mut c) = self.child.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.kill();
    }
}

fn health_ok(addr: SocketAddr) -> bool {
    let Ok(mut s) = TcpStream::connect_timeout(&addr, Duration::from_millis(500)) else {
        return false;
    };
    let _ = s.set_read_timeout(Some(Duration::from_secs(2)));
    let req = "GET /v1/health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    if s.write_all(req.as_bytes()).is_err() {
        return false;
    }
    let mut body = String::new();
    let _ = s.read_to_string(&mut body);
    body.starts_with("HTTP/1.1 200") && body.contains("ok")
}

/// POST a JSON body straight to the server, for requests the CLI never
/// makes. Returns the status code and the response body.
pub fn post_json(addr: SocketAddr, path: &str, body: &str) -> (u16, String) {
    let mut s = TcpStream::connect(addr).expect("connect");
    let req = format!(
        "POST {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\
         Content-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    s.write_all(req.as_bytes()).unwrap();
    let mut resp = String::new();
    s.read_to_string(&mut resp).unwrap();
    let status = resp.split_whitespace().nth(1).unwrap().parse().unwrap();
    let body = resp
        .split("\r\n\r\n")
        .nth(1)
        .unwrap_or_default()
        .to_string();
    (status, body)
}

/// One HTTP request with a binary body, for clients that must see exactly
/// what the server answered (or that it did not). Returns the status and the
/// response body; any transport failure is an `Err`, as it is for a client
/// whose server was just killed.
pub fn http(
    addr: SocketAddr,
    method: &str,
    path: &str,
    body: &[u8],
) -> std::io::Result<(u16, Vec<u8>)> {
    let mut s = TcpStream::connect_timeout(&addr, Duration::from_secs(2))?;
    s.set_read_timeout(Some(Duration::from_secs(30)))?;
    s.set_write_timeout(Some(Duration::from_secs(30)))?;
    let head = format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\
         Content-Type: application/json\r\nContent-Length: {}\r\n\r\n",
        body.len()
    );
    s.write_all(head.as_bytes())?;
    s.write_all(body)?;
    let mut resp = Vec::new();
    s.read_to_end(&mut resp)?;
    let bad = |what: &str| std::io::Error::new(std::io::ErrorKind::InvalidData, what.to_string());
    let split = resp
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or_else(|| bad("truncated response"))?;
    let head = String::from_utf8_lossy(&resp[..split]).into_owned();
    let status = head
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| bad("no status"))?;
    let body = resp[split + 4..].to_vec();
    let declared = head.lines().find_map(|l| {
        let (k, v) = l.split_once(':')?;
        k.eq_ignore_ascii_case("content-length")
            .then(|| v.trim().parse::<usize>().ok())?
    });
    if declared.is_some_and(|n| n != body.len()) {
        return Err(bad("response cut short"));
    }
    Ok((status, body))
}

/// Send a request and hang up `after` later without reading the answer, as a
/// client does when it times out: the server may be anywhere in the request,
/// including just past its commit.
pub fn http_abandon(addr: SocketAddr, method: &str, path: &str, body: &[u8], after: Duration) {
    let Ok(mut s) = TcpStream::connect_timeout(&addr, Duration::from_secs(2)) else {
        return;
    };
    let head = format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\
         Content-Type: application/json\r\nContent-Length: {}\r\n\r\n",
        body.len()
    );
    if s.write_all(head.as_bytes()).is_ok() && s.write_all(body).is_ok() {
        std::thread::sleep(after);
    }
    let _ = s.shutdown(std::net::Shutdown::Both);
}

/// GET straight from the server, for requests the CLI would retry or never
/// make. Returns the status, the raw response head (lower-cased) and the body.
pub fn http_get(addr: SocketAddr, path: &str, token: Option<&str>) -> (u16, String, String) {
    let mut s = TcpStream::connect(addr).expect("connect");
    let auth = token.map_or(String::new(), |t| format!("Authorization: Bearer {t}\r\n"));
    let req = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n{auth}\r\n");
    s.write_all(req.as_bytes()).unwrap();
    let mut resp = String::new();
    s.read_to_string(&mut resp).unwrap();
    let status = resp.split_whitespace().nth(1).unwrap().parse().unwrap();
    let (head, body) = resp.split_once("\r\n\r\n").unwrap_or((&resp, ""));
    (status, head.to_lowercase(), body.to_string())
}

/// Settings for the `leep` client shared by every working copy of a test.
#[derive(Clone)]
pub struct Client {
    pub server: String,
    pub cache: PathBuf,
}

impl Client {
    pub fn new(server: &str, cache: &Path) -> Client {
        Client {
            server: server.to_string(),
            cache: cache.to_path_buf(),
        }
    }

    pub fn cmd(&self, cwd: &Path) -> Command {
        let mut c = Command::new(bin("leep"));
        c.current_dir(cwd)
            .env("LEEP_SERVER", &self.server)
            .env("LEEP_CACHE_DIR", &self.cache)
            .env("LEEP_AUTHOR", "e2e")
            .env_remove("LEEP_AGENT_MODEL")
            .env_remove("LEEP_AGENT_SESSION")
            .env_remove("LEEP_AGENT_PROMPT")
            .env_remove("LEEP_AGENT_TESTS")
            .env_remove("LEEP_SIGNING_KEY");
        c
    }

    /// Run `leep` and return the raw output, whatever the exit code.
    pub fn run(&self, cwd: &Path, args: &[&str]) -> Output {
        self.cmd(cwd)
            .args(args)
            .output()
            .unwrap_or_else(|e| panic!("spawn leep {args:?}: {e}"))
    }

    /// Run `leep`, panicking with its output unless it exits 0.
    pub fn ok(&self, cwd: &Path, args: &[&str]) -> String {
        let out = self.run(cwd, args);
        assert_ok(&out, &format!("leep {args:?} in {}", cwd.display()));
        stdout(&out)
    }

    pub fn json(&self, cwd: &Path, args: &[&str]) -> serde_json::Value {
        let mut full = vec!["--json"];
        full.extend_from_slice(args);
        let out = self.ok(cwd, &full);
        serde_json::from_str(&out).unwrap_or_else(|e| panic!("bad json from {args:?}: {e}\n{out}"))
    }
}

pub fn stdout(out: &Output) -> String {
    String::from_utf8_lossy(&out.stdout).into_owned()
}

pub fn stderr(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}

pub fn assert_ok(out: &Output, what: &str) {
    assert!(
        out.status.success(),
        "{what} failed ({})\n--- stdout\n{}\n--- stderr\n{}",
        out.status,
        stdout(out),
        stderr(out)
    );
}

/// Run `leep-git` with the given arguments, panicking unless it succeeds.
pub fn leep_git(args: &[&str]) -> String {
    let out = Command::new(bin("leep-git"))
        .args(args)
        .env("RUST_LOG", "warn")
        .output()
        .expect("spawn leep-git");
    assert_ok(&out, &format!("leep-git {args:?}"));
    stdout(&out)
}

/// Run git with a deterministic identity and no user configuration.
pub fn git(dir: &Path, args: &[&str]) -> String {
    git_dated(dir, args, "2024-01-01T00:00:00+0000")
}

pub fn git_dated(dir: &Path, args: &[&str], date: &str) -> String {
    let out = Command::new("git")
        .current_dir(dir)
        .args(args)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_AUTHOR_NAME", "Fixture Author")
        .env("GIT_AUTHOR_EMAIL", "author@example.com")
        .env("GIT_COMMITTER_NAME", "Fixture Committer")
        .env("GIT_COMMITTER_EMAIL", "committer@example.com")
        .env("GIT_AUTHOR_DATE", date)
        .env("GIT_COMMITTER_DATE", date)
        .output()
        .unwrap_or_else(|e| panic!("spawn git {args:?}: {e}"));
    assert_ok(&out, &format!("git {args:?}"));
    stdout(&out)
}

pub fn write(root: &Path, rel: &str, body: &str) {
    let p = root.join(rel);
    fs::create_dir_all(p.parent().unwrap()).unwrap();
    fs::write(p, body).unwrap();
}

/// Count regular files below `dir` (0 if it does not exist).
pub fn count_files(dir: &Path) -> usize {
    let Ok(rd) = fs::read_dir(dir) else {
        return 0;
    };
    rd.flatten()
        .map(|e| {
            let p = e.path();
            if p.is_dir() {
                count_files(&p)
            } else {
                1
            }
        })
        .sum()
}

/// A throwaway Postgres database, dropped on `Drop`.
pub struct PgDb {
    admin_url: String,
    pub name: String,
    pub url: String,
}

impl PgDb {
    /// `None` (after printing a note) when `LEEP_TEST_POSTGRES_URL` is unset.
    pub fn create() -> Option<PgDb> {
        let Ok(admin_url) = std::env::var("LEEP_TEST_POSTGRES_URL") else {
            eprintln!("skipping Postgres variant: LEEP_TEST_POSTGRES_URL not set");
            return None;
        };
        // Schemas cannot be selected through the URL, so isolate with a database.
        let name = format!("leep_e2e_{}_{}", std::process::id(), free_port());
        run_sql(&admin_url, &format!("CREATE DATABASE {name}"));
        let (base, query) = match admin_url.split_once('?') {
            Some((b, q)) => (b, format!("?{q}")),
            None => (admin_url.as_str(), String::new()),
        };
        let root = base
            .rsplit_once('/')
            .expect("postgres url has no database")
            .0;
        let url = format!("{root}/{name}{query}");
        Some(PgDb {
            admin_url,
            name,
            url,
        })
    }
}

impl Drop for PgDb {
    fn drop(&mut self) {
        run_sql(
            &self.admin_url,
            &format!("DROP DATABASE IF EXISTS {} WITH (FORCE)", self.name),
        );
    }
}

fn run_sql(url: &str, sql: &str) {
    use sqlx::Connection;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let mut c = sqlx::PgConnection::connect(url)
            .await
            .expect("connect to postgres");
        sqlx::query(sql).execute(&mut c).await.expect("run sql");
    });
}
