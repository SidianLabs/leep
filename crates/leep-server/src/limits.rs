//! Per-principal request limits (ADR 0020): a token-bucket rate, a cap on
//! in-flight requests and a cap on body size, so one runaway agent cannot
//! starve the rest. Everything is off unless configured.
//!
//! A principal is a token name, or in anonymous mode (no tokens file) the
//! client address. State lives in memory per server instance, so with several
//! instances behind a balancer each one enforces its own limits.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::extract::{ConnectInfo, Request, State};
use axum::http::{header, HeaderValue, Method, StatusCode};
use axum::middleware::Next;
use axum::response::Response;
use http_body_util::{BodyExt, Limited};
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::auth::{self, Auth, Principal, Role};

/// Metrics label for every client address in anonymous mode: addresses are
/// unbounded, so they never become label values.
const ANONYMOUS: &str = "anonymous";
/// Most client addresses tracked at once in anonymous mode.
const MAX_ANONYMOUS_CLIENTS: usize = 4096;
/// Idle entries are looked for at most this often, so a flood of new
/// addresses cannot make every request scan the table.
const SWEEP_INTERVAL: Duration = Duration::from_secs(1);

const MIN_RATE: f64 = 1e-6;
const MAX_RATE: f64 = 1e9;
const MAX_CONCURRENCY: f64 = (1 << 20) as f64;
const MAX_WHOLE: f64 = (1u64 << 53) as f64;

/// Monotonic time source, injectable so tests need no sleeps.
pub trait Clock: Send + Sync {
    /// Time since a fixed origin; never goes backwards.
    fn now(&self) -> Duration;
}

pub struct SystemClock(Instant);

impl SystemClock {
    pub fn new() -> Self {
        SystemClock(Instant::now())
    }
}

impl Default for SystemClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for SystemClock {
    fn now(&self) -> Duration {
        self.0.elapsed()
    }
}

/// A clock that only moves when told to.
#[derive(Default)]
pub struct ManualClock(Mutex<Duration>);

impl ManualClock {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn advance(&self, by: Duration) {
        *lock(&self.0) += by;
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Duration {
        *lock(&self.0)
    }
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    // The guarded data is plain counters, so a panic elsewhere cannot leave
    // it in a state worth refusing to read.
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// Sustained requests per second, and how many may arrive at once.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Rate {
    pub per_sec: f64,
    pub burst: u32,
}

/// Starts full and refills continuously, so an idle principal can burst and a
/// busy one is held to the sustained rate.
#[derive(Debug)]
pub struct TokenBucket {
    rate: Rate,
    tokens: f64,
    last: Duration,
}

impl TokenBucket {
    pub fn new(rate: Rate, now: Duration) -> Self {
        TokenBucket {
            rate,
            tokens: f64::from(rate.burst),
            last: now,
        }
    }

    fn refill(&mut self, now: Duration) {
        let elapsed = now.saturating_sub(self.last);
        self.last = self.last.max(now);
        let gained = elapsed.as_secs_f64() * self.rate.per_sec;
        self.tokens = (self.tokens + gained).min(f64::from(self.rate.burst));
    }

    /// Take one request's worth, or say how long until one is available.
    pub fn try_take(&mut self, now: Duration) -> Result<(), Duration> {
        self.refill(now);
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            return Ok(());
        }
        Err(Duration::from_secs_f64(
            (1.0 - self.tokens) / self.rate.per_sec,
        ))
    }

    /// A full bucket is indistinguishable from a new one, so it can be dropped.
    pub fn is_full(&mut self, now: Duration) -> bool {
        self.refill(now);
        self.tokens >= f64::from(self.rate.burst)
    }
}

/// A limit that is a number or the word `unlimited`, so a token can opt out of
/// a server-wide default.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Quota {
    Unlimited,
    Limit(f64),
}

impl Serialize for Quota {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            Quota::Unlimited => s.serialize_str("unlimited"),
            Quota::Limit(n) => s.serialize_f64(*n),
        }
    }
}

impl<'de> Deserialize<'de> for Quota {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl Visitor<'_> for V {
            type Value = Quota;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a number or \"unlimited\"")
            }
            fn visit_f64<E: de::Error>(self, v: f64) -> Result<Quota, E> {
                Ok(Quota::Limit(v))
            }
            fn visit_i64<E: de::Error>(self, v: i64) -> Result<Quota, E> {
                Ok(Quota::Limit(v as f64))
            }
            fn visit_u64<E: de::Error>(self, v: u64) -> Result<Quota, E> {
                Ok(Quota::Limit(v as f64))
            }
            fn visit_str<E: de::Error>(self, v: &str) -> Result<Quota, E> {
                match v {
                    "unlimited" => Ok(Quota::Unlimited),
                    _ => Err(E::invalid_value(de::Unexpected::Str(v), &self)),
                }
            }
        }
        d.deserialize_any(V)
    }
}

/// The limit fields of one tokens-file entry. Absent means "use the
/// server-wide default".
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TokenLimits {
    pub rate: Option<Quota>,
    pub burst: Option<f64>,
    pub concurrency: Option<Quota>,
    pub max_body: Option<Quota>,
}

fn whole(what: &str, n: f64, max: f64) -> Result<(), String> {
    if n.is_finite() && n >= 1.0 && n.fract() == 0.0 && n <= max {
        Ok(())
    } else {
        Err(format!(
            "{what} must be a whole number from 1 to {max:.0}, got {n}"
        ))
    }
}

fn check_rate(n: f64) -> Result<(), String> {
    if n.is_finite() && (MIN_RATE..=MAX_RATE).contains(&n) {
        Ok(())
    } else {
        Err(format!(
            "rate must be a number of requests per second from {MIN_RATE} to {MAX_RATE:.0}, got {n}"
        ))
    }
}

impl TokenLimits {
    pub fn validate(&self) -> Result<(), String> {
        if let Some(Quota::Limit(r)) = self.rate {
            check_rate(r)?;
        }
        if let Some(b) = self.burst {
            whole("burst", b, f64::from(u32::MAX))?;
        }
        if let Some(Quota::Limit(c)) = self.concurrency {
            whole("concurrency", c, MAX_CONCURRENCY)?;
        }
        if let Some(Quota::Limit(m)) = self.max_body {
            whole("max_body", m, MAX_WHOLE)?;
        }
        Ok(())
    }
}

/// Server-wide settings that tokens inherit unless their entry says otherwise.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Defaults {
    /// Requests per second, per principal and per read or write class.
    pub rate: Option<f64>,
    /// Bucket size; defaults to the rate rounded up.
    pub burst: Option<u32>,
    /// Requests in flight per principal.
    pub concurrency: Option<usize>,
    /// Body size cap for `read` tokens, which should not upload.
    pub read_max_body: Option<usize>,
}

impl Defaults {
    fn validate(&self, authenticated: bool) -> Result<(), String> {
        if let Some(r) = self.rate {
            check_rate(r).map_err(|e| format!("--rate-limit: {e}"))?;
        }
        if self.burst.is_some() && self.rate.is_none() {
            return Err("--rate-burst needs --rate-limit".into());
        }
        if self.burst == Some(0) {
            return Err("--rate-burst must be at least 1".into());
        }
        if self.concurrency == Some(0) {
            return Err("--max-concurrent-per-token must be at least 1".into());
        }
        if self.read_max_body == Some(0) {
            return Err("--read-max-body-bytes must be at least 1".into());
        }
        if self.read_max_body.is_some() && !authenticated {
            return Err("--read-max-body-bytes needs --tokens-file".into());
        }
        Ok(())
    }
}

/// What applies to one principal after merging its entry over the defaults.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Limits {
    pub rate: Option<Rate>,
    pub concurrency: Option<usize>,
    pub max_body: Option<usize>,
}

impl Limits {
    fn is_empty(&self) -> bool {
        *self == Limits::default()
    }
}

fn resolve(role: Option<Role>, t: &TokenLimits, d: &Defaults) -> Result<Limits, String> {
    let per_sec = match t.rate {
        Some(Quota::Unlimited) => None,
        Some(Quota::Limit(r)) => Some(r),
        None => d.rate,
    };
    let rate = match (per_sec, t.burst) {
        (None, Some(_)) => return Err("burst needs a rate (set rate, or --rate-limit)".into()),
        (None, None) => None,
        (Some(per_sec), burst) => {
            let burst = burst.map(|b| b as u32).or(d.burst);
            Some(Rate {
                per_sec,
                burst: burst
                    .unwrap_or_else(|| per_sec.ceil().clamp(1.0, f64::from(u32::MAX)) as u32),
            })
        }
    };
    let concurrency = match t.concurrency {
        Some(Quota::Unlimited) => None,
        Some(Quota::Limit(c)) => Some(c as usize),
        None => d.concurrency,
    };
    let max_body = match t.max_body {
        Some(Quota::Unlimited) => None,
        Some(Quota::Limit(m)) => Some(m as usize),
        None if role == Some(Role::Read) => d.read_max_body,
        None => None,
    };
    Ok(Limits {
        rate,
        concurrency,
        max_body,
    })
}

/// Live state of one principal. Held by a request for as long as it is in
/// flight, so an idle entry is one nobody else points at.
struct Slot {
    label: String,
    limits: Limits,
    read: Option<Mutex<TokenBucket>>,
    write: Option<Mutex<TokenBucket>>,
    permits: Option<Arc<Semaphore>>,
}

impl Slot {
    fn new(label: &str, limits: Limits, now: Duration) -> Slot {
        let bucket = || limits.rate.map(|r| Mutex::new(TokenBucket::new(r, now)));
        Slot {
            label: label.to_string(),
            limits,
            read: bucket(),
            write: bucket(),
            permits: limits.concurrency.map(|n| Arc::new(Semaphore::new(n))),
        }
    }

    fn take(&self, write: bool, now: Duration) -> Result<(), Duration> {
        match if write { &self.write } else { &self.read } {
            Some(b) => lock(b).try_take(now),
            None => Ok(()),
        }
    }

    fn is_idle(&self, now: Duration) -> bool {
        [&self.read, &self.write]
            .into_iter()
            .flatten()
            .all(|b| lock(b).is_full(now))
    }

    fn limited(&self, reason: &'static str) {
        metrics::counter!("leep_requests_limited_total", "reason" => reason, "token" => self.label.clone())
            .increment(1);
        tracing::debug!(token = %self.label, reason, "request limited");
    }
}

/// Holds a concurrency permit, and counts the request as in flight, until the
/// response body is finished or dropped: a cancelled or timed-out request
/// drops its future and with it this guard.
struct InFlight {
    slot: Arc<Slot>,
    _permit: Option<OwnedSemaphorePermit>,
}

impl InFlight {
    /// `None` when the principal is at its concurrency limit. Never waits.
    fn enter(slot: &Arc<Slot>) -> Option<InFlight> {
        let permit = match &slot.permits {
            Some(s) => Some(s.clone().try_acquire_owned().ok()?),
            None => None,
        };
        metrics::gauge!("leep_requests_in_flight", "token" => slot.label.clone()).increment(1.0);
        Some(InFlight {
            slot: slot.clone(),
            _permit: permit,
        })
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        metrics::gauge!("leep_requests_in_flight", "token" => self.slot.label.clone())
            .decrement(1.0);
    }
}

/// Anonymous clients are keyed by address; IPv6 by /64, since one subscriber
/// holds a whole /64 and could otherwise rotate addresses past the limit.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum ClientKey {
    V4(u32),
    V6(u64),
    Unknown,
}

impl ClientKey {
    fn of(ip: Option<IpAddr>) -> ClientKey {
        match ip.map(|ip| ip.to_canonical()) {
            Some(IpAddr::V4(a)) => ClientKey::V4(a.into()),
            Some(IpAddr::V6(a)) => ClientKey::V6((u128::from(a) >> 64) as u64),
            None => ClientKey::Unknown,
        }
    }
}

struct Anonymous {
    limits: Limits,
    table: Mutex<Table>,
}

struct Table {
    by_client: HashMap<ClientKey, Arc<Slot>>,
    last_sweep: Duration,
    /// Shared by clients that arrive while the table is full of active ones,
    /// so memory stays bounded and the failure mode is over-limiting.
    overflow: Arc<Slot>,
}

/// The limits in force for one server.
pub struct Limiter {
    clock: Arc<dyn Clock>,
    tokens: HashMap<String, Arc<Slot>>,
    anonymous: Option<Anonymous>,
}

impl Limiter {
    /// `None` when nothing is limited, so the server runs no middleware.
    pub fn build(auth: Option<&Auth>, defaults: &Defaults) -> Result<Option<Limiter>, String> {
        Self::with_clock(auth, defaults, Arc::new(SystemClock::new()))
    }

    pub fn with_clock(
        auth: Option<&Auth>,
        defaults: &Defaults,
        clock: Arc<dyn Clock>,
    ) -> Result<Option<Limiter>, String> {
        defaults.validate(auth.is_some())?;
        let now = clock.now();
        let mut tokens = HashMap::new();
        let mut anonymous = None;
        match auth {
            Some(a) => {
                for (name, role, tl) in a.token_limits() {
                    let limits = resolve(Some(role), tl, defaults)
                        .map_err(|e| format!("token {name:?}: {e}"))?;
                    if !limits.is_empty() {
                        tokens.insert(name.to_string(), Arc::new(Slot::new(name, limits, now)));
                    }
                }
            }
            None => {
                let limits = resolve(None, &TokenLimits::default(), defaults)?;
                if !limits.is_empty() {
                    anonymous = Some(Anonymous {
                        limits,
                        table: Mutex::new(Table {
                            by_client: HashMap::new(),
                            last_sweep: now,
                            overflow: Arc::new(Slot::new(ANONYMOUS, limits, now)),
                        }),
                    });
                }
            }
        }
        if tokens.is_empty() && anonymous.is_none() {
            return Ok(None);
        }
        Ok(Some(Limiter {
            clock,
            tokens,
            anonymous,
        }))
    }

    /// Tokens that have any limit, for the startup log.
    pub fn limited_tokens(&self) -> usize {
        self.tokens.len()
    }

    /// Whether anonymous clients are limited per address.
    pub fn limits_anonymous(&self) -> bool {
        self.anonymous.is_some()
    }

    fn slot_for(&self, req: &Request) -> Option<Arc<Slot>> {
        if let Some(p) = req.extensions().get::<Principal>() {
            return self.tokens.get(&p.name).cloned();
        }
        let ip = req
            .extensions()
            .get::<ConnectInfo<SocketAddr>>()
            .map(|c| c.0.ip());
        self.client_slot(ClientKey::of(ip))
    }

    fn client_slot(&self, key: ClientKey) -> Option<Arc<Slot>> {
        let anon = self.anonymous.as_ref()?;
        let now = self.clock.now();
        let mut t = lock(&anon.table);
        if let Some(s) = t.by_client.get(&key) {
            return Some(s.clone());
        }
        if t.by_client.len() >= MAX_ANONYMOUS_CLIENTS
            && now.saturating_sub(t.last_sweep) >= SWEEP_INTERVAL
        {
            t.last_sweep = now;
            // Count 1 means only the table holds it, so nothing is in flight.
            t.by_client
                .retain(|_, s| Arc::strong_count(s) > 1 || !s.is_idle(now));
        }
        if t.by_client.len() >= MAX_ANONYMOUS_CLIENTS {
            return Some(t.overflow.clone());
        }
        let slot = Arc::new(Slot::new(ANONYMOUS, anon.limits, now));
        t.by_client.insert(key, slot.clone());
        Some(slot)
    }
}

fn content_length(req: &Request) -> Option<u64> {
    req.headers()
        .get(header::CONTENT_LENGTH)?
        .to_str()
        .ok()?
        .parse()
        .ok()
}

fn refuse(status: StatusCode, msg: &str, retry_after: Option<u64>) -> Response {
    let mut resp = auth::reject(status, msg);
    if let Some(s) = retry_after {
        resp.headers_mut()
            .insert(header::RETRY_AFTER, HeaderValue::from(s));
    }
    resp
}

fn too_large(slot: &Slot, max: usize) -> Response {
    slot.limited("size");
    refuse(
        StatusCode::PAYLOAD_TOO_LARGE,
        &format!("request body exceeds the limit of {max} bytes for this token"),
        None,
    )
}

/// Axum middleware enforcing a [`Limiter`]. Layer it inside authentication,
/// which supplies the [`Principal`], and inside the timeout and global body
/// limit. `GET /v1/health` is never limited, so probes keep working.
pub async fn enforce(State(limiter): State<Arc<Limiter>>, req: Request, next: Next) -> Response {
    if req.method() == Method::GET && req.uri().path() == "/v1/health" {
        return next.run(req).await;
    }
    let Some(slot) = limiter.slot_for(&req) else {
        return next.run(req).await;
    };
    let max_body = slot.limits.max_body;
    if let Some(max) = max_body {
        if content_length(&req).is_some_and(|n| n > max as u64) {
            return too_large(&slot, max);
        }
    }
    let write = auth::needs_write(req.method(), req.uri().path());
    if let Err(wait) = slot.take(write, limiter.clock.now()) {
        slot.limited("rate");
        let secs = wait.as_secs_f64().ceil().max(1.0) as u64;
        return refuse(
            StatusCode::TOO_MANY_REQUESTS,
            &format!("rate limit exceeded; retry after {secs}s"),
            Some(secs),
        );
    }
    let Some(guard) = InFlight::enter(&slot) else {
        slot.limited("concurrency");
        return refuse(
            StatusCode::TOO_MANY_REQUESTS,
            "too many concurrent requests; retry after 1s",
            Some(1),
        );
    };
    // The check above sees only a declared length; this catches chunked bodies.
    let req = match max_body {
        Some(max) => req.map(|b| Body::new(Limited::new(b, max))),
        None => req,
    };
    let resp = next.run(req).await;
    let resp = match max_body {
        // Handlers never answer 413 themselves; it is the body cap, which the
        // body extractor reports as plain text.
        Some(max) if resp.status() == StatusCode::PAYLOAD_TOO_LARGE => too_large(&slot, max),
        _ => resp,
    };
    let (parts, body) = resp.into_parts();
    let body = body.map_frame(move |frame| {
        let _held = &guard;
        frame
    });
    Response::from_parts(parts, Body::new(body))
}

#[cfg(test)]
mod tests {
    use super::*;

    const fn rate(per_sec: f64, burst: u32) -> Rate {
        Rate { per_sec, burst }
    }

    fn secs(s: f64) -> Duration {
        Duration::from_secs_f64(s)
    }

    #[test]
    fn bucket_starts_full_then_refuses() {
        let mut b = TokenBucket::new(rate(1.0, 3), Duration::ZERO);
        for _ in 0..3 {
            assert!(b.try_take(Duration::ZERO).is_ok());
        }
        assert_eq!(b.try_take(Duration::ZERO), Err(secs(1.0)));
    }

    #[test]
    fn bucket_refills_at_the_rate() {
        let mut b = TokenBucket::new(rate(2.0, 2), Duration::ZERO);
        assert!(b.try_take(Duration::ZERO).is_ok());
        assert!(b.try_take(Duration::ZERO).is_ok());
        // Half a second later one token is back, at 2 per second.
        assert_eq!(b.try_take(secs(0.25)), Err(secs(0.25)));
        assert!(b.try_take(secs(0.5)).is_ok());
        assert_eq!(b.try_take(secs(0.5)), Err(secs(0.5)));
    }

    #[test]
    fn bucket_never_exceeds_burst() {
        let mut b = TokenBucket::new(rate(10.0, 2), Duration::ZERO);
        assert!(b.try_take(Duration::ZERO).is_ok());
        // An hour idle still allows only `burst` at once.
        let later = secs(3600.0);
        assert!(b.try_take(later).is_ok());
        assert!(b.try_take(later).is_ok());
        assert!(b.try_take(later).is_err());
    }

    #[test]
    fn slow_rate_waits_longer_than_a_second() {
        let mut b = TokenBucket::new(rate(0.5, 1), Duration::ZERO);
        assert!(b.try_take(Duration::ZERO).is_ok());
        assert_eq!(b.try_take(Duration::ZERO), Err(secs(2.0)));
        assert_eq!(b.try_take(secs(1.5)), Err(secs(0.5)));
        assert!(b.try_take(secs(2.0)).is_ok());
    }

    #[test]
    fn bucket_tolerates_a_clock_that_stalls() {
        let mut b = TokenBucket::new(rate(1.0, 1), secs(10.0));
        assert!(b.try_take(secs(10.0)).is_ok());
        assert!(b.try_take(secs(5.0)).is_err());
        assert!(b.try_take(secs(11.0)).is_ok());
    }

    #[test]
    fn read_and_write_buckets_are_separate() {
        let l = Limits {
            rate: Some(rate(1.0, 1)),
            ..Default::default()
        };
        let slot = Slot::new("t", l, Duration::ZERO);
        assert!(slot.take(true, Duration::ZERO).is_ok());
        assert!(slot.take(true, Duration::ZERO).is_err());
        assert!(slot.take(false, Duration::ZERO).is_ok());
        assert!(slot.take(false, Duration::ZERO).is_err());
    }

    #[test]
    fn quota_reads_numbers_and_the_word() {
        let q = |s: &str| serde_json::from_str::<Quota>(s);
        assert_eq!(q("5").unwrap(), Quota::Limit(5.0));
        assert_eq!(q("0.5").unwrap(), Quota::Limit(0.5));
        assert_eq!(q("\"unlimited\"").unwrap(), Quota::Unlimited);
        assert!(q("\"lots\"").unwrap_err().to_string().contains("unlimited"));
        assert!(q("true").is_err());
        assert_eq!(
            serde_json::to_string(&Quota::Unlimited).unwrap(),
            "\"unlimited\""
        );
    }

    #[test]
    fn token_fields_are_validated() {
        let ok = TokenLimits {
            rate: Some(Quota::Limit(0.5)),
            burst: Some(3.0),
            concurrency: Some(Quota::Limit(2.0)),
            max_body: Some(Quota::Unlimited),
        };
        assert!(ok.validate().is_ok());
        for bad in [
            TokenLimits {
                rate: Some(Quota::Limit(0.0)),
                ..Default::default()
            },
            TokenLimits {
                rate: Some(Quota::Limit(f64::NAN)),
                ..Default::default()
            },
            TokenLimits {
                burst: Some(1.5),
                ..Default::default()
            },
            TokenLimits {
                burst: Some(0.0),
                ..Default::default()
            },
            TokenLimits {
                concurrency: Some(Quota::Limit(-1.0)),
                ..Default::default()
            },
            TokenLimits {
                max_body: Some(Quota::Limit(0.0)),
                ..Default::default()
            },
        ] {
            assert!(bad.validate().is_err(), "{bad:?}");
        }
    }

    #[test]
    fn tokens_inherit_defaults_and_can_override_them() {
        let d = Defaults {
            rate: Some(10.0),
            burst: Some(20),
            concurrency: Some(4),
            read_max_body: Some(1000),
        };
        let none = TokenLimits::default();
        let l = resolve(Some(Role::Write), &none, &d).unwrap();
        assert_eq!(l.rate, Some(rate(10.0, 20)));
        assert_eq!((l.concurrency, l.max_body), (Some(4), None));
        let l = resolve(Some(Role::Read), &none, &d).unwrap();
        assert_eq!(l.max_body, Some(1000));

        let own = TokenLimits {
            rate: Some(Quota::Limit(1.0)),
            burst: Some(2.0),
            concurrency: Some(Quota::Unlimited),
            max_body: Some(Quota::Limit(50.0)),
        };
        let l = resolve(Some(Role::Read), &own, &d).unwrap();
        assert_eq!(l.rate, Some(rate(1.0, 2)));
        assert_eq!((l.concurrency, l.max_body), (None, Some(50)));

        let exempt = TokenLimits {
            rate: Some(Quota::Unlimited),
            concurrency: Some(Quota::Unlimited),
            ..Default::default()
        };
        assert!(resolve(Some(Role::Write), &exempt, &d).unwrap().is_empty());
    }

    #[test]
    fn burst_defaults_to_the_rate_rounded_up() {
        let d = Defaults {
            rate: Some(2.5),
            ..Default::default()
        };
        let l = resolve(Some(Role::Write), &TokenLimits::default(), &d).unwrap();
        assert_eq!(l.rate, Some(rate(2.5, 3)));
        let d = Defaults {
            rate: Some(0.1),
            ..Default::default()
        };
        let l = resolve(Some(Role::Write), &TokenLimits::default(), &d).unwrap();
        assert_eq!(l.rate, Some(rate(0.1, 1)));
    }

    #[test]
    fn burst_without_a_rate_is_an_error() {
        let t = TokenLimits {
            burst: Some(5.0),
            ..Default::default()
        };
        let e = resolve(Some(Role::Write), &t, &Defaults::default()).unwrap_err();
        assert!(e.contains("burst needs a rate"), "{e}");
        let d = Defaults {
            burst: Some(5),
            ..Default::default()
        };
        assert!(d.validate(true).unwrap_err().contains("--rate-limit"));
    }

    #[test]
    fn defaults_are_validated() {
        let bad = |d: Defaults, auth: bool| d.validate(auth).unwrap_err();
        let rate = |r| Defaults {
            rate: Some(r),
            ..Default::default()
        };
        assert!(bad(rate(0.0), false).contains("--rate-limit"));
        assert!(bad(rate(-1.0), false).contains("--rate-limit"));
        assert!(bad(rate(f64::INFINITY), false).contains("--rate-limit"));
        let c = Defaults {
            concurrency: Some(0),
            ..Default::default()
        };
        assert!(bad(c, false).contains("--max-concurrent-per-token"));
        let m = Defaults {
            read_max_body: Some(10),
            ..Default::default()
        };
        assert!(bad(m.clone(), false).contains("--tokens-file"));
        assert!(m.validate(true).is_ok());
        assert!(Defaults::default().validate(false).is_ok());
    }

    #[test]
    fn nothing_configured_builds_no_limiter() {
        assert!(Limiter::build(None, &Defaults::default())
            .unwrap()
            .is_none());
    }

    fn anonymous(clock: Arc<ManualClock>) -> Limiter {
        let d = Defaults {
            rate: Some(1.0),
            ..Default::default()
        };
        Limiter::with_clock(None, &d, clock).unwrap().unwrap()
    }

    fn client(i: u32) -> ClientKey {
        ClientKey::V4(i)
    }

    fn tracked(l: &Limiter) -> usize {
        lock(&l.anonymous.as_ref().unwrap().table).by_client.len()
    }

    #[test]
    fn anonymous_clients_have_their_own_buckets() {
        let l = anonymous(Arc::new(ManualClock::new()));
        let (a, b) = (
            l.client_slot(client(1)).unwrap(),
            l.client_slot(client(2)).unwrap(),
        );
        assert!(a.take(false, Duration::ZERO).is_ok());
        assert!(a.take(false, Duration::ZERO).is_err());
        assert!(b.take(false, Duration::ZERO).is_ok());
        assert!(Arc::ptr_eq(&a, &l.client_slot(client(1)).unwrap()));
    }

    #[test]
    fn ipv6_clients_share_a_slot_per_64() {
        let key = |s: &str| ClientKey::of(Some(s.parse().unwrap()));
        assert_eq!(key("2001:db8:0:1::1"), key("2001:db8:0:1:ffff::2"));
        assert_ne!(key("2001:db8:0:1::1"), key("2001:db8:0:2::1"));
        assert_eq!(key("::ffff:10.0.0.1"), key("10.0.0.1"));
        assert_ne!(ClientKey::of(None), key("0.0.0.0"));
    }

    #[test]
    fn idle_anonymous_entries_are_evicted_and_the_table_stays_bounded() {
        let clock = Arc::new(ManualClock::new());
        let l = anonymous(clock.clone());
        // Fill the table with clients that each spent their one request.
        let mut held = Vec::new();
        for i in 0..MAX_ANONYMOUS_CLIENTS as u32 {
            let s = l.client_slot(client(i)).unwrap();
            assert!(s.take(false, clock.now()).is_ok());
            if i == 0 {
                held.push(s); // simulates a request still in flight
            }
        }
        assert_eq!(tracked(&l), MAX_ANONYMOUS_CLIENTS);

        // Nothing has refilled and the sweep interval has not passed: a new
        // client lands on the shared overflow slot and the table does not grow.
        let o1 = l.client_slot(client(100_000)).unwrap();
        let o2 = l.client_slot(client(100_001)).unwrap();
        assert!(Arc::ptr_eq(&o1, &o2));
        assert_eq!(tracked(&l), MAX_ANONYMOUS_CLIENTS);

        // After the buckets refill, a new client triggers a sweep that drops
        // every idle entry except the one still held.
        clock.advance(Duration::from_secs(2));
        let fresh = l.client_slot(client(100_002)).unwrap();
        assert!(!Arc::ptr_eq(&fresh, &o1));
        assert_eq!(tracked(&l), 2);
    }
}
