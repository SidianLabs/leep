//! Concurrency limits per class of expensive read (ADR 0060, section 1).
//!
//! ADR 0020's limits are per token, and a fleet of agents each running one
//! search is within every token's limit while the searches take every core
//! the lands need. Each class here has one semaphore for the whole server.
//! A request that finds its class full is refused at once with `503`
//! `unavailable`, `Retry-After` and `Leep-Refused: read-class`, never
//! queued: a queue would only move the pile-up into server memory.
//!
//! Only the `search` class is built. The other classes the ADR names are
//! parsed, so the flag has its final shape, and may be set to `off`, which
//! is what this server does for them; a number for one of them is refused
//! at startup instead of being silently ignored.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{header, HeaderValue, Method, StatusCode};
use axum::middleware::Next;
use axum::response::Response;
use http_body_util::BodyExt;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// A class of expensive read with a concurrency limit of its own.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadClass {
    /// `GET /v1/search/{rev}`.
    Search,
    /// `POST /v1/land/check` (ADR 0060, section 3; not built).
    LandCheck,
    /// `GET /v1/blame/{id}` (not built).
    Blame,
    /// `GET /v1/log/{id}` with a path, `author` or `extra` filter (not built).
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
        self == ReadClass::Search
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
    let cores = std::thread::available_parallelism().map_or(1, |n| n.get());
    (cores / 2).max(1)
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
}

impl Default for ReadConcurrency {
    fn default() -> Self {
        ReadConcurrency {
            search: ClassLimit::Permits(default_search_permits()),
        }
    }
}

impl ReadConcurrency {
    /// No class limited.
    pub fn off() -> Self {
        ReadConcurrency {
            search: ClassLimit::Off,
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
                // Off is what this server does for them already.
                _ if limit == ClassLimit::Off => {}
                _ => {
                    return Err(format!(
                        "--read-concurrency {}: this server does not limit that class yet \
                         (ADR 0060); only search can be limited, and the others only set to off",
                        class.name()
                    ))
                }
            }
        }
        Ok(out)
    }

    /// `search=2` style, for the startup log.
    pub fn describe(&self) -> String {
        match self.search {
            ClassLimit::Off => "search=off".into(),
            ClassLimit::Permits(n) => format!("search={n}"),
        }
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
    _permit: OwnedSemaphorePermit,
}

impl Drop for Held {
    fn drop(&mut self) {
        self.class.record(self.since.elapsed());
        metrics::gauge!("leep_read_class_in_flight", "class" => self.class.class.name())
            .decrement(1.0);
    }
}

/// The read classes of one server. Flags, so they do not reload.
pub struct ReadLimits {
    search: Option<Arc<Class>>,
}

impl ReadLimits {
    pub fn new(config: &ReadConcurrency) -> Arc<ReadLimits> {
        let search = match config.search {
            ClassLimit::Off => None,
            ClassLimit::Permits(n) => Some(Arc::new(Class::new(ReadClass::Search, n))),
        };
        Arc::new(ReadLimits { search })
    }

    /// The limited class a request belongs to, if any.
    fn class_of(&self, method: &Method, path: &str) -> Option<&Arc<Class>> {
        let read = method == Method::GET || method == Method::HEAD;
        if read && path.starts_with("/v1/search/") {
            return self.search.as_ref();
        }
        None
    }
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
pub async fn enforce(State(limits): State<Arc<ReadLimits>>, req: Request, next: Next) -> Response {
    let Some(class) = limits.class_of(req.method(), req.uri().path()).cloned() else {
        return next.run(req).await;
    };
    let Ok(permit) = class.semaphore.clone().try_acquire_owned() else {
        return refuse(&class);
    };
    metrics::gauge!("leep_read_class_in_flight", "class" => class.class.name()).increment(1.0);
    let held = Held {
        class,
        since: Instant::now(),
        _permit: permit,
    };
    let resp = next.run(req).await;
    let (parts, body) = resp.into_parts();
    let body = body.map_frame(move |frame| {
        let _held = &held;
        frame
    });
    Response::from_parts(parts, Body::new(body))
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
        // Classes this server does not limit may be set to what it does.
        for c in ["land_check", "blame", "log_filtered", "diff_large"] {
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
    fn only_searches_are_classified() {
        let l = ReadLimits::new(&ReadConcurrency {
            search: ClassLimit::Permits(1),
        });
        assert!(l.class_of(&Method::GET, "/v1/search/main").is_some());
        assert!(l.class_of(&Method::GET, "/v1/search/team/x").is_some());
        for (m, p) in [
            (Method::POST, "/v1/search/main"),
            (Method::GET, "/v1/refs/main"),
            (Method::POST, "/v1/land"),
            (Method::GET, "/v1/blame/x"),
            (Method::GET, "/v1/searchy"),
        ] {
            assert!(l.class_of(&m, p).is_none(), "{m} {p}");
        }
        let off = ReadLimits::new(&ReadConcurrency::off());
        assert!(off.class_of(&Method::GET, "/v1/search/main").is_none());
    }
}
