//! Concurrency limits per class of expensive read (ADR 0060, section 1;
//! ADR 0074).
//!
//! ADR 0020's limits are per token, and a fleet of agents each running one
//! search or blame is within every token's limit while those reads take
//! every core and connection the lands need. Each class here has one
//! semaphore for the whole server. A request that finds its class full is
//! refused at once with `503` `unavailable`, `Retry-After` and
//! `Leep-Refused: read-class`, never queued: a queue would only move the
//! pile-up into server memory.
//!
//! The `search`, `blame` and `log_filtered` classes are built. The others
//! the ADR names are parsed, so the flag has its final shape, and may be set
//! to `off`, which is what this server does for them; a number for one of
//! them is refused at startup instead of being silently ignored.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use std::pin::Pin;
use std::task::{Context, Poll};

use axum::body::{Body, Bytes, HttpBody};
use axum::extract::{Request, State};
use axum::http::{header, HeaderValue, Method, StatusCode, Uri};
use axum::middleware::Next;
use axum::response::Response;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// A class of expensive read with a concurrency limit of its own.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadClass {
    /// `GET /v1/search/{rev}`.
    Search,
    /// `POST /v1/land/check` (ADR 0060, section 3; not built).
    LandCheck,
    /// `GET /v1/blame/{id}`.
    Blame,
    /// `GET /v1/log/{id}` with a path, `author` or `extra` filter, and
    /// `GET /v1/last-change/{id}`, which walks the same path history.
    LogFiltered,
    /// `GET /v1/diff` across many generations (not built).
    DiffLarge,
}

impl ReadClass {
    pub const ALL: [ReadClass; 5] = [
        ReadClass::Search,
        ReadClass::LandCheck,
        ReadClass::Blame,
        ReadClass::LogFiltered,
        ReadClass::DiffLarge,
    ];

    /// The name in `--read-concurrency` and in the `class` metric label.
    pub fn name(self) -> &'static str {
        match self {
            ReadClass::Search => "search",
            ReadClass::LandCheck => "land_check",
            ReadClass::Blame => "blame",
            ReadClass::LogFiltered => "log_filtered",
            ReadClass::DiffLarge => "diff_large",
        }
    }

    pub fn parse(name: &str) -> Option<ReadClass> {
        Self::ALL.into_iter().find(|c| c.name() == name)
    }

    /// Whether this server can limit the class.
    pub fn is_built(self) -> bool {
        matches!(
            self,
            ReadClass::Search | ReadClass::Blame | ReadClass::LogFiltered
        )
    }
}

/// The limit of one class: a number of requests at once, or none.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClassLimit {
    Off,
    Permits(usize),
}

/// Most permits a class may have; a larger number is the same as `off`.
const MAX_PERMITS: usize = 1 << 20;

/// The `search` class's default: half the cores the process may use, at
/// least one, so on a two-core host one core always serves lands and cheap
/// reads. `available_parallelism` reads the CPU quota on Linux.
pub fn default_search_permits() -> usize {
    (cores() / 2).max(1)
}

/// The `blame` class's default: one per core, at least two (ADR 0074). A
/// blame spends much of its time waiting on metadata and blob reads, so one
/// per core keeps the cores busy without letting blames queue for them; two
/// at least, so on a small host one long blame does not shed every other.
pub fn default_blame_permits() -> usize {
    cores().max(2)
}

/// The `log_filtered` class's default: twice [`default_blame_permits`]. A
/// path log or last-change is mostly index and changeset reads with little
/// CPU, and agents send them several times as often as blames.
pub fn default_log_filtered_permits() -> usize {
    2 * default_blame_permits()
}

fn cores() -> usize {
    std::thread::available_parallelism().map_or(1, |n| n.get())
}

/// `--search-pool-permits`' default for a metadata pool of `pool_size`
/// connections: a quarter of it, at least one. A permit holds up to two
/// connections, so searches together hold at most half of the pool.
pub fn default_search_pool_permits(pool_size: u32) -> usize {
    (pool_size as usize / 4).max(1)
}

/// The `--read-concurrency` settings. Only built classes are kept.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReadConcurrency {
    pub search: ClassLimit,
    pub blame: ClassLimit,
    pub log_filtered: ClassLimit,
}

impl Default for ReadConcurrency {
    fn default() -> Self {
        ReadConcurrency {
            search: ClassLimit::Permits(default_search_permits()),
            blame: ClassLimit::Permits(default_blame_permits()),
            log_filtered: ClassLimit::Permits(default_log_filtered_permits()),
        }
    }
}

impl ReadConcurrency {
    /// No class limited.
    pub fn off() -> Self {
        ReadConcurrency {
            search: ClassLimit::Off,
            blame: ClassLimit::Off,
            log_filtered: ClassLimit::Off,
        }
    }

    /// The limit of a built class; `Off` for the others.
    pub fn get(&self, class: ReadClass) -> ClassLimit {
        match class {
            ReadClass::Search => self.search,
            ReadClass::Blame => self.blame,
            ReadClass::LogFiltered => self.log_filtered,
            _ => ClassLimit::Off,
        }
    }

    /// The defaults with each `CLASS=N|off` of `settings` applied in order,
    /// so a later setting of a class wins.
    pub fn parse<S: AsRef<str>>(settings: &[S]) -> Result<Self, String> {
        let mut out = ReadConcurrency::default();
        for s in settings {
            let s = s.as_ref().trim();
            if s.is_empty() {
                continue;
            }
            let Some((name, value)) = s.split_once('=') else {
                return Err(format!(
                    "--read-concurrency: expected CLASS=N or CLASS=off, got {s:?}"
                ));
            };
            let names = || {
                ReadClass::ALL
                    .iter()
                    .map(|c| c.name())
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            let class = ReadClass::parse(name.trim()).ok_or_else(|| {
                format!(
                    "--read-concurrency: unknown class {:?}; the classes are {}",
                    name.trim(),
                    names()
                )
            })?;
            let limit = match value.trim() {
                "off" => ClassLimit::Off,
                v => match v.parse::<usize>() {
                    Ok(n) if (1..=MAX_PERMITS).contains(&n) => ClassLimit::Permits(n),
                    _ => {
                        return Err(format!(
                            "--read-concurrency {}: expected a whole number from 1 to \
                             {MAX_PERMITS}, or off; got {v:?}",
                            class.name()
                        ))
                    }
                },
            };
            match class {
                ReadClass::Search => out.search = limit,
                ReadClass::Blame => out.blame = limit,
                ReadClass::LogFiltered => out.log_filtered = limit,
                // Off is what this server does for them already.
                _ if limit == ClassLimit::Off => {}
                _ => {
                    return Err(format!(
                        "--read-concurrency {}: this server does not limit that class yet \
                         (ADR 0060); only search, blame and log_filtered can be limited, \
                         and the others only set to off",
                        class.name()
                    ))
                }
            }
        }
        Ok(out)
    }

    /// `search=1,blame=2,log_filtered=4` style, for the startup log.
    pub fn describe(&self) -> String {
        ReadClass::ALL
            .into_iter()
            .filter(|c| c.is_built())
            .map(|c| match self.get(c) {
                ClassLimit::Off => format!("{}=off", c.name()),
                ClassLimit::Permits(n) => format!("{}={n}", c.name()),
            })
            .collect::<Vec<_>>()
            .join(",")
    }
}

/// Request times kept per class for `Retry-After`.
const RECENT: usize = 64;
/// Bounds of `Retry-After`, in seconds. The ceiling is the CLI's
/// `MAX_RETRY_WAIT`: `leep` reports a longer wait instead of sleeping, so
/// a higher one would fail `leep search` on every refusal.
pub const MIN_RETRY_AFTER: u64 = 1;
pub const MAX_RETRY_AFTER: u64 = 10;

/// `Retry-After` for a class whose recent requests took `median` and that
/// has `permits`: about when a permit frees, between the bounds.
pub fn retry_after_secs(median: Duration, permits: usize) -> u64 {
    let secs = (median.as_secs_f64() / permits.max(1) as f64).ceil() as u64;
    secs.clamp(MIN_RETRY_AFTER, MAX_RETRY_AFTER)
}

/// One limited class.
struct Class {
    class: ReadClass,
    permits: usize,
    semaphore: Arc<Semaphore>,
    /// How long recent requests held their permit, newest last.
    recent: Mutex<VecDeque<Duration>>,
}

impl Class {
    fn new(class: ReadClass, permits: usize) -> Class {
        let name = class.name();
        metrics::gauge!("leep_read_class_permits", "class" => name).set(permits as f64);
        metrics::gauge!("leep_read_class_in_flight", "class" => name).increment(0.0);
        metrics::counter!("leep_read_class_shed_total", "class" => name).increment(0);
        metrics::counter!("leep_read_class_admitted_total", "class" => name).increment(0);
        metrics::counter!("leep_read_class_completed_total", "class" => name).increment(0);
        Class {
            class,
            permits,
            semaphore: Arc::new(Semaphore::new(permits)),
            recent: Mutex::new(VecDeque::with_capacity(RECENT)),
        }
    }

    fn record(&self, took: Duration) {
        let mut r = self.recent.lock().unwrap_or_else(|e| e.into_inner());
        if r.len() == RECENT {
            r.pop_front();
        }
        r.push_back(took);
    }

    fn median(&self) -> Duration {
        let mut times: Vec<Duration> = self
            .recent
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .copied()
            .collect();
        if times.is_empty() {
            return Duration::ZERO;
        }
        times.sort_unstable();
        times[times.len() / 2]
    }
}

/// A request's permit of its class. Held until the response body ends or is
/// dropped, so a cancelled or timed-out request gives it back.
struct Held {
    class: Arc<Class>,
    since: Instant,
    /// The handler answered 2xx; set once the response head is known.
    ok: bool,
    _permit: ReadPermit,
}

/// A request's read-class permit, also in its extensions: work the request
/// leaves running past its end (a detached blame, ADR 0078) keeps a clone,
/// so the class still counts it until it ends.
#[derive(Clone)]
pub struct ReadPermit(#[allow(dead_code)] Arc<OwnedSemaphorePermit>);

impl Drop for Held {
    fn drop(&mut self) {
        self.class.record(self.since.elapsed());
        let name = self.class.class.name();
        metrics::gauge!("leep_read_class_in_flight", "class" => name).decrement(1.0);
        if self.ok {
            metrics::counter!("leep_read_class_completed_total", "class" => name).increment(1);
        }
    }
}

/// The read classes of one server. Flags, so they do not reload.
pub struct ReadLimits {
    search: Option<Arc<Class>>,
    blame: Option<Arc<Class>>,
    log_filtered: Option<Arc<Class>>,
}

impl ReadLimits {
    pub fn new(config: &ReadConcurrency) -> Arc<ReadLimits> {
        let class = |c: ReadClass| match config.get(c) {
            ClassLimit::Off => None,
            ClassLimit::Permits(n) => Some(Arc::new(Class::new(c, n))),
        };
        Arc::new(ReadLimits {
            search: class(ReadClass::Search),
            blame: class(ReadClass::Blame),
            log_filtered: class(ReadClass::LogFiltered),
        })
    }

    /// A gate for the blame warmer (ADR 0083): each pass with work takes a
    /// `blame` permit, if one is free, so warming counts against the class
    /// and waits while requests fill it. `None` when the class is off.
    pub fn blame_gate(&self) -> Option<leep_derive::blame_warm::WarmGate> {
        let class = self.blame.clone()?;
        Some(Arc::new(move || {
            class
                .semaphore
                .clone()
                .try_acquire_owned()
                .ok()
                .map(|p| Box::new(p) as leep_derive::BlameHold)
        }))
    }

    /// The limited class a request belongs to, if any. Decided from the
    /// method, path and query alone, before any store is read.
    fn class_of(&self, method: &Method, uri: &Uri) -> Option<&Arc<Class>> {
        if method != Method::GET && method != Method::HEAD {
            return None;
        }
        let path = uri.path();
        let class = if path.starts_with("/v1/search/") {
            &self.search
        } else if path.starts_with("/v1/blame/") {
            &self.blame
        } else if path.starts_with("/v1/last-change/")
            || (path.starts_with("/v1/log/") && log_is_filtered(uri))
            || is_ref_history_walk(path)
        {
            &self.log_filtered
        } else {
            return None;
        };
        class.as_ref()
    }
}

/// `GET /v1/refs/{name}/checks` and `.../culprits` (ADR 0091) walk a ref's
/// first-parent chain and log with ancestry checks: history walks, limited
/// like a filtered log. A ref whose own name ends so is limited too, which
/// costs nothing but a permit.
fn is_ref_history_walk(path: &str) -> bool {
    path.starts_with("/v1/refs/") && (path.ends_with("/checks") || path.ends_with("/culprits"))
}

/// Whether a `GET /v1/log` names a path, `author` or `extra`, as the
/// handler reads them. A log without them is a first-parent page walk,
/// cheap and bounded by its limit, and is not limited. A query that does
/// not parse is refused by the handler before any work.
fn log_is_filtered(uri: &Uri) -> bool {
    let Ok(axum::extract::Query(pairs)) =
        axum::extract::Query::<Vec<(String, String)>>::try_from_uri(uri)
    else {
        return false;
    };
    pairs.iter().any(|(k, v)| match k.as_str() {
        "path" => !v.trim_matches('/').is_empty(),
        "author" | "extra" => true,
        _ => false,
    })
}

/// The answer to a request whose class is full.
fn refuse(class: &Class) -> Response {
    let name = class.class.name();
    metrics::counter!("leep_read_class_shed_total", "class" => name).increment(1);
    let secs = retry_after_secs(class.median(), class.permits);
    tracing::debug!(class = name, retry_after = secs, "read refused: class full");
    let mut resp = crate::auth::reject(
        StatusCode::SERVICE_UNAVAILABLE,
        leep_core::api::error_code::UNAVAILABLE,
        &format!(
            "server busy: all {} of this server's {name} permits are in use \
             (--read-concurrency); nothing was done, retry after {secs}s",
            class.permits
        ),
    );
    let h = resp.headers_mut();
    h.insert(header::RETRY_AFTER, HeaderValue::from(secs));
    h.insert(
        leep_core::api::REFUSED_HEADER,
        HeaderValue::from_static(leep_core::api::REFUSED_READ_CLASS),
    );
    resp
}

/// Axum middleware applying [`ReadLimits`]. Layer it just inside
/// [`crate::limits::enforce`], so a request spends its token's limits
/// before it is counted here, and inside authentication, so a request
/// without a valid token never holds a permit. It applies to every token,
/// `"unlimited"` ones included: the class protects the server, not a
/// token's share.
pub async fn enforce(
    State(limits): State<Arc<ReadLimits>>,
    mut req: Request,
    next: Next,
) -> Response {
    let Some(class) = limits.class_of(req.method(), req.uri()).cloned() else {
        return next.run(req).await;
    };
    let Ok(permit) = class.semaphore.clone().try_acquire_owned() else {
        return refuse(&class);
    };
    let name = class.class.name();
    metrics::gauge!("leep_read_class_in_flight", "class" => name).increment(1.0);
    metrics::counter!("leep_read_class_admitted_total", "class" => name).increment(1);
    let permit = ReadPermit(Arc::new(permit));
    req.extensions_mut().insert(permit.clone());
    let mut held = Held {
        class,
        since: Instant::now(),
        ok: false,
        _permit: permit,
    };
    let resp = next.run(req).await;
    held.ok = resp.status().is_success();
    let (parts, inner) = resp.into_parts();
    Response::from_parts(parts, Body::new(HeldBody { inner, _held: held }))
}

/// A response body that holds its request's permit until it ends or is
/// dropped. It forwards the size hint, so an answer of known length keeps
/// its `Content-Length` instead of turning chunked.
struct HeldBody {
    inner: Body,
    _held: Held,
}

impl HttpBody for HeldBody {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, axum::Error>>> {
        Pin::new(&mut self.inner).poll_frame(cx)
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn settings_parse_and_later_ones_win() {
        let d = ReadConcurrency::default();
        assert_eq!(ReadConcurrency::parse::<&str>(&[]).unwrap(), d);
        assert!(matches!(d.search, ClassLimit::Permits(n) if n >= 1));
        let p = |s: &[&str]| ReadConcurrency::parse(s);
        assert_eq!(p(&["search=3"]).unwrap().search, ClassLimit::Permits(3));
        assert_eq!(p(&[" search = off "]).unwrap().search, ClassLimit::Off);
        assert_eq!(
            p(&["search=off", "search=2"]).unwrap().search,
            ClassLimit::Permits(2)
        );
        let both = p(&["blame=3", "log_filtered=off", "blame=5"]).unwrap();
        assert_eq!(both.blame, ClassLimit::Permits(5));
        assert_eq!(both.log_filtered, ClassLimit::Off);
        assert_eq!(both.search, d.search);
        // Classes this server does not limit may be set to what it does.
        for c in ["land_check", "diff_large"] {
            assert_eq!(p(&[&format!("{c}=off")]).unwrap(), d, "{c}");
            let e = p(&[&format!("{c}=4")]).unwrap_err();
            assert!(e.contains(c) && e.contains("does not limit"), "{e}");
        }
        for (bad, want) in [
            ("search", "CLASS=N"),
            ("search=0", "whole number"),
            ("search=-1", "whole number"),
            ("search=many", "whole number"),
            ("search=2000000", "whole number"),
            ("grep=1", "unknown class"),
        ] {
            let e = p(&[bad]).unwrap_err();
            assert!(e.contains(want), "{bad}: {e}");
        }
        assert!(p(&["grep=1"]).unwrap_err().contains("log_filtered"));
    }

    #[test]
    fn defaults_follow_the_cores() {
        let cores = std::thread::available_parallelism().map_or(1, |n| n.get());
        let d = ReadConcurrency::default();
        assert_eq!(d.search, ClassLimit::Permits((cores / 2).max(1)));
        assert_eq!(d.blame, ClassLimit::Permits(cores.max(2)));
        assert_eq!(d.log_filtered, ClassLimit::Permits(2 * cores.max(2)));
        assert_eq!(
            d.describe(),
            format!(
                "search={},blame={},log_filtered={}",
                (cores / 2).max(1),
                cores.max(2),
                2 * cores.max(2)
            )
        );
        assert_eq!(
            ReadConcurrency::off().describe(),
            "search=off,blame=off,log_filtered=off"
        );
    }

    #[test]
    fn retry_after_is_the_median_over_the_permits_within_bounds() {
        let ms = Duration::from_millis;
        assert_eq!(retry_after_secs(Duration::ZERO, 1), MIN_RETRY_AFTER);
        assert_eq!(retry_after_secs(ms(450), 1), 1);
        assert_eq!(retry_after_secs(ms(4500), 1), 5);
        assert_eq!(retry_after_secs(ms(4500), 2), 3);
        assert_eq!(retry_after_secs(ms(4500), 0), 5);
        assert_eq!(
            retry_after_secs(Duration::from_secs(600), 1),
            MAX_RETRY_AFTER
        );
    }

    #[test]
    fn the_median_is_of_the_recent_requests() {
        let c = Class::new(ReadClass::Search, 1);
        assert_eq!(c.median(), Duration::ZERO);
        for s in [1, 9, 3] {
            c.record(Duration::from_secs(s));
        }
        assert_eq!(c.median(), Duration::from_secs(3));
        for _ in 0..RECENT {
            c.record(Duration::from_secs(7));
        }
        assert_eq!(c.median(), Duration::from_secs(7));
    }

    #[test]
    fn requests_are_classified_by_route_and_filter() {
        let l = ReadLimits::new(&ReadConcurrency {
            search: ClassLimit::Permits(1),
            blame: ClassLimit::Permits(1),
            log_filtered: ClassLimit::Permits(1),
        });
        let class = |m: Method, u: &str| {
            l.class_of(&m, &u.parse::<Uri>().unwrap())
                .map(|c| c.class.name())
        };
        for (m, u, want) in [
            (Method::GET, "/v1/search/main", "search"),
            (Method::GET, "/v1/search/team/x?q=a", "search"),
            (Method::GET, "/v1/blame/x?path=a.txt", "blame"),
            (Method::HEAD, "/v1/blame/x?path=a.txt", "blame"),
            (Method::GET, "/v1/log/x?path=a.txt", "log_filtered"),
            (
                Method::GET,
                "/v1/log/x?limit=5&path=d%2Fb.txt",
                "log_filtered",
            ),
            (Method::GET, "/v1/log/x?author=al", "log_filtered"),
            (Method::GET, "/v1/log/x?extra=k%3Dv", "log_filtered"),
            (Method::GET, "/v1/last-change/x?path=a.txt", "log_filtered"),
            (Method::GET, "/v1/refs/main/checks", "log_filtered"),
            (
                Method::GET,
                "/v1/refs/team/x/culprits?bad=a",
                "log_filtered",
            ),
        ] {
            assert_eq!(class(m.clone(), u), Some(want), "{m} {u}");
        }
        for (m, u) in [
            (Method::POST, "/v1/search/main"),
            (Method::GET, "/v1/refs/main"),
            (Method::GET, "/v1/changesets/abc/checks"),
            (Method::POST, "/v1/land"),
            (Method::GET, "/v1/searchy"),
            (Method::GET, "/v1/log/x"),
            (Method::GET, "/v1/log/x?limit=20&fields=summary"),
            (Method::GET, "/v1/log/x?path=&limit=2"),
            (Method::GET, "/v1/log/x?path=%2F"),
            (Method::GET, "/v1/files/x?prefix=a"),
            (Method::GET, "/v1/diff?to=x"),
            (Method::GET, "/v1/provenance/x?path=a"),
        ] {
            assert_eq!(class(m.clone(), u), None, "{m} {u}");
        }
        let off = ReadLimits::new(&ReadConcurrency::off());
        for u in ["/v1/search/main", "/v1/blame/x?path=a", "/v1/log/x?path=a"] {
            assert!(off.class_of(&Method::GET, &u.parse().unwrap()).is_none());
        }
    }
    /// The blame warmer's gate takes a `blame` permit while one is free and
    /// gives it back when dropped; with the class off there is no gate.
    #[test]
    fn the_blame_gate_shares_the_blame_permits() {
        let p = |s: &[&str]| ReadConcurrency::parse(s).unwrap();
        let limits = ReadLimits::new(&p(&["blame=2"]));
        let gate = limits.blame_gate().expect("a gate while blame is limited");
        let a = gate().expect("a free permit");
        let b = gate().expect("a second free permit");
        assert!(gate().is_none(), "the class is full");
        drop(a);
        assert!(gate().is_some());
        drop(b);
        assert!(ReadLimits::new(&p(&["blame=off"])).blame_gate().is_none());
    }
}
