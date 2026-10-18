//! What the mirror reports: a status document (written to `status.json` in
//! the work directory and served at `GET /status`), Prometheus metrics
//! rendered from it (`GET /metrics`), and the small HTTP listener that serves
//! both and takes `POST /poke` to poll the remote at once.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::Path;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// Version of the status document; bumped only for incompatible changes.
pub const STATUS_VERSION: u32 = 1;

/// Where one mirrored ref stands.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RefState {
    /// The remote ref is the export of the Leep ref.
    InSync,
    /// Leep moved and the mirror has not caught up yet (or is retrying).
    #[default]
    Pending,
    /// Both sides have commits the other lacks. Nothing moves on either side
    /// until someone merges in git; Leep's side is pushed to the diverged
    /// branch to merge from.
    Diverged,
    /// Leep moved the ref back (an undo); the remote still has the newer
    /// commits and the mirror does not force it.
    Rewound,
    /// The remote has commits Leep lacks and importing is off.
    RemoteAhead,
    /// Importing a push was refused by Leep's land policy.
    Refused,
    /// The ref was deleted in Leep; the remote branch is left alone.
    DeletedInLeep,
    /// The last attempt failed (server, remote or local); retried with backoff.
    Error,
}

impl RefState {
    pub const ALL: [RefState; 8] = [
        RefState::InSync,
        RefState::Pending,
        RefState::Diverged,
        RefState::Rewound,
        RefState::RemoteAhead,
        RefState::Refused,
        RefState::DeletedInLeep,
        RefState::Error,
    ];

    pub fn label(self) -> &'static str {
        match self {
            RefState::InSync => "in-sync",
            RefState::Pending => "pending",
            RefState::Diverged => "diverged",
            RefState::Rewound => "rewound",
            RefState::RemoteAhead => "remote-ahead",
            RefState::Refused => "refused",
            RefState::DeletedInLeep => "deleted-in-leep",
            RefState::Error => "error",
        }
    }

    /// States that clear on their own; anything else needs a person.
    pub fn healthy(self) -> bool {
        matches!(self, RefState::InSync | RefState::Pending)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Counts {
    pub ok: u64,
    pub rejected: u64,
    pub failed: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImportCounts {
    pub landed: u64,
    pub refused: u64,
    pub lost_race: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RefStatus {
    pub leep_ref: String,
    pub git_ref: String,
    pub state: RefState,
    /// Why the ref is in this state, for a person to read.
    #[serde(default)]
    pub detail: String,
    pub leep: Option<String>,
    pub exported: Option<String>,
    pub remote: Option<String>,
    /// The mirror-owned branch Leep's side was pushed to while diverged.
    #[serde(default)]
    pub diverged_branch: Option<String>,
    /// Unix seconds since the two sides first differed; null when in sync.
    pub pending_since: Option<u64>,
    pub last_sync: Option<u64>,
    #[serde(default)]
    pub failures: u32,
    #[serde(default)]
    pub pushes: Counts,
    #[serde(default)]
    pub imports: ImportCounts,
    /// Times the ref entered the diverged state.
    #[serde(default)]
    pub divergences: u64,
}

impl RefStatus {
    pub fn lag(&self, now: u64) -> u64 {
        self.pending_since.map_or(0, |t| now.saturating_sub(t))
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Status {
    pub version: u32,
    pub pid: u32,
    pub server: String,
    /// The remote URL with credentials removed.
    pub remote: String,
    pub started: u64,
    pub updated: u64,
    /// Seconds between Leep polls, so a reader can tell a stale file.
    pub interval_secs: u64,
    pub ref_log_seq: Option<u64>,
    pub refs: Vec<RefStatus>,
    pub commits_exported: u64,
    pub commits_imported: u64,
    pub cycles: u64,
    /// Failures by stage: `leep`, `git`, `export`, `import`.
    pub errors: BTreeMap<String, u64>,
}

pub fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Replace `path` atomically, so a reader never sees half a document.
pub fn write_status(path: &Path, status: &Status) -> std::io::Result<()> {
    let tmp = path.with_extension("json.tmp");
    let body = serde_json::to_vec_pretty(status)?;
    std::fs::write(&tmp, body)?;
    std::fs::rename(&tmp, path)
}

pub fn read_status(path: &Path) -> std::io::Result<Status> {
    let body = std::fs::read(path)?;
    serde_json::from_slice(&body).map_err(std::io::Error::other)
}

fn esc(v: &str) -> String {
    v.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

/// Prometheus text format for `status`.
pub fn render_metrics(s: &Status) -> String {
    let now = now();
    let mut o = String::new();
    let head = |o: &mut String, name: &str, kind: &str, help: &str| {
        let _ = writeln!(o, "# HELP {name} {help}\n# TYPE {name} {kind}");
    };
    head(
        &mut o,
        "leep_mirror_up",
        "gauge",
        "Always 1 while the mirror runs.",
    );
    let _ = writeln!(o, "leep_mirror_up 1");
    head(
        &mut o,
        "leep_mirror_ref_state",
        "gauge",
        "1 for the state each mirrored ref is in, 0 for the others.",
    );
    for r in &s.refs {
        for st in RefState::ALL {
            let _ = writeln!(
                o,
                "leep_mirror_ref_state{{ref=\"{}\",state=\"{}\"}} {}",
                esc(&r.leep_ref),
                st.label(),
                u8::from(r.state == st)
            );
        }
    }
    head(
        &mut o,
        "leep_mirror_lag_seconds",
        "gauge",
        "Seconds since the Leep ref and the remote ref first differed; 0 in sync.",
    );
    for r in &s.refs {
        let _ = writeln!(
            o,
            "leep_mirror_lag_seconds{{ref=\"{}\"}} {}",
            esc(&r.leep_ref),
            r.lag(now)
        );
    }
    head(
        &mut o,
        "leep_mirror_last_sync_timestamp_seconds",
        "gauge",
        "Unix time the ref was last seen in sync.",
    );
    for r in &s.refs {
        if let Some(t) = r.last_sync {
            let _ = writeln!(
                o,
                "leep_mirror_last_sync_timestamp_seconds{{ref=\"{}\"}} {t}",
                esc(&r.leep_ref)
            );
        }
    }
    head(
        &mut o,
        "leep_mirror_pushes_total",
        "counter",
        "Pushes to the remote by result: ok, rejected (by the remote), failed.",
    );
    for r in &s.refs {
        for (k, v) in [
            ("ok", r.pushes.ok),
            ("rejected", r.pushes.rejected),
            ("failed", r.pushes.failed),
        ] {
            let _ = writeln!(
                o,
                "leep_mirror_pushes_total{{ref=\"{}\",result=\"{k}\"}} {v}",
                esc(&r.leep_ref)
            );
        }
    }
    head(
        &mut o,
        "leep_mirror_imports_total",
        "counter",
        "Remote pushes imported into Leep by result: landed, refused (land policy), lost_race.",
    );
    for r in &s.refs {
        for (k, v) in [
            ("landed", r.imports.landed),
            ("refused", r.imports.refused),
            ("lost_race", r.imports.lost_race),
        ] {
            let _ = writeln!(
                o,
                "leep_mirror_imports_total{{ref=\"{}\",result=\"{k}\"}} {v}",
                esc(&r.leep_ref)
            );
        }
    }
    head(
        &mut o,
        "leep_mirror_divergences_total",
        "counter",
        "Times a ref entered the diverged state (both sides moved).",
    );
    for r in &s.refs {
        let _ = writeln!(
            o,
            "leep_mirror_divergences_total{{ref=\"{}\"}} {}",
            esc(&r.leep_ref),
            r.divergences
        );
    }
    head(
        &mut o,
        "leep_mirror_commits_exported_total",
        "counter",
        "Commits the mirror wrote from changesets.",
    );
    let _ = writeln!(
        o,
        "leep_mirror_commits_exported_total {}",
        s.commits_exported
    );
    head(
        &mut o,
        "leep_mirror_commits_imported_total",
        "counter",
        "Changesets the mirror created from remote commits.",
    );
    let _ = writeln!(
        o,
        "leep_mirror_commits_imported_total {}",
        s.commits_imported
    );
    head(
        &mut o,
        "leep_mirror_errors_total",
        "counter",
        "Failed steps by stage: leep, git, export, import.",
    );
    for stage in ["leep", "git", "export", "import"] {
        let _ = writeln!(
            o,
            "leep_mirror_errors_total{{stage=\"{stage}\"}} {}",
            s.errors.get(stage).copied().unwrap_or(0)
        );
    }
    head(
        &mut o,
        "leep_mirror_cycles_total",
        "counter",
        "Poll cycles run.",
    );
    let _ = writeln!(o, "leep_mirror_cycles_total {}", s.cycles);
    head(
        &mut o,
        "leep_mirror_last_cycle_timestamp_seconds",
        "gauge",
        "Unix time the last poll cycle finished.",
    );
    let _ = writeln!(o, "leep_mirror_last_cycle_timestamp_seconds {}", s.updated);
    o
}

/// Wakes the main loop early: set by `POST /poke` (for example from a
/// GitHub or GitLab push webhook) and by shutdown.
#[derive(Default)]
pub struct Wake {
    flag: Mutex<bool>,
    cv: Condvar,
}

impl Wake {
    pub fn poke(&self) {
        *self.flag.lock().unwrap() = true;
        self.cv.notify_all();
    }

    /// Sleep up to `d`; true if poked meanwhile (the flag is cleared).
    pub fn wait(&self, d: Duration) -> bool {
        let g = self.flag.lock().unwrap();
        let (mut g, _) = self.cv.wait_timeout_while(g, d, |p| !*p).unwrap();
        std::mem::take(&mut *g)
    }
}

/// Serve `/metrics`, `/status`, `/health` and `POST /poke` on `addr` from a
/// background thread. The poke takes no body and carries no instructions,
/// so it needs no authentication: at worst it makes the mirror poll sooner.
pub fn serve(
    addr: SocketAddr,
    status: Arc<Mutex<Status>>,
    wake: Arc<Wake>,
) -> std::io::Result<SocketAddr> {
    let listener = TcpListener::bind(addr)?;
    let local = listener.local_addr()?;
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let (status, wake) = (status.clone(), wake.clone());
            std::thread::spawn(move || {
                let _ = handle(stream, &status, &wake);
            });
        }
    });
    Ok(local)
}

fn handle(mut stream: TcpStream, status: &Mutex<Status>, wake: &Wake) -> std::io::Result<()> {
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut line = String::new();
    reader.read_line(&mut line)?;
    // Drain the headers; no request here has a body worth reading.
    let mut h = String::new();
    while reader.read_line(&mut h)? > 2 {
        h.clear();
    }
    let mut parts = line.split_whitespace();
    let (method, path) = (parts.next().unwrap_or(""), parts.next().unwrap_or(""));
    let path = path.split('?').next().unwrap_or("");
    let (code, ctype, body) = match (method, path) {
        ("GET", "/metrics") => (
            "200 OK",
            "text/plain; version=0.0.4",
            render_metrics(&status.lock().unwrap()),
        ),
        ("GET", "/status") => (
            "200 OK",
            "application/json",
            serde_json::to_string_pretty(&*status.lock().unwrap()).unwrap_or_default(),
        ),
        ("GET", "/health") => ("200 OK", "text/plain", "ok\n".to_string()),
        ("POST", "/poke") => {
            wake.poke();
            ("202 Accepted", "text/plain", "polling\n".to_string())
        }
        _ => ("404 Not Found", "text/plain", "not found\n".to_string()),
    };
    write!(
        stream,
        "HTTP/1.1 {code}\r\nContent-Type: {ctype}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_name_every_state_and_escape_ref_names() {
        let s = Status {
            refs: vec![RefStatus {
                leep_ref: "we\"ird".into(),
                state: RefState::Diverged,
                pending_since: Some(now() - 30),
                ..Default::default()
            }],
            ..Default::default()
        };
        let m = render_metrics(&s);
        assert!(m.contains("leep_mirror_ref_state{ref=\"we\\\"ird\",state=\"diverged\"} 1"));
        assert!(m.contains("leep_mirror_ref_state{ref=\"we\\\"ird\",state=\"in-sync\"} 0"));
        let lag: u64 = m
            .lines()
            .find(|l| l.starts_with("leep_mirror_lag_seconds{"))
            .and_then(|l| l.rsplit(' ').next())
            .and_then(|v| v.parse().ok())
            .unwrap();
        assert!((30..40).contains(&lag), "{lag}");
    }

    #[test]
    fn status_round_trips_through_its_file() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("status.json");
        let s = Status {
            version: STATUS_VERSION,
            refs: vec![RefStatus {
                leep_ref: "main".into(),
                git_ref: "refs/heads/main".into(),
                state: RefState::InSync,
                ..Default::default()
            }],
            ..Default::default()
        };
        write_status(&p, &s).unwrap();
        assert_eq!(read_status(&p).unwrap(), s);
    }
}
