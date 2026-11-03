//! `Server-Timing` on read endpoints (ADR 0082): where a request's time
//! went, per phase, so a client or a browser's network panel can tell
//! store latency from work without the server's metrics. Durations are
//! milliseconds with three decimals; `desc` carries counts.

use std::fmt::Write;
use std::time::Duration;

use axum::http::{HeaderName, HeaderValue};
use axum::response::Response;
use leep_derive::read_stats::ReadSummary;

/// The `Server-Timing` response header.
pub const SERVER_TIMING: HeaderName = HeaderName::from_static("server-timing");

/// Phases of one request, in the order they ran.
#[derive(Default)]
pub struct ServerTiming {
    value: String,
}

impl ServerTiming {
    pub fn new() -> Self {
        Self::default()
    }

    /// A phase that took `dur`.
    pub fn phase(&mut self, name: &str, dur: Duration) -> &mut Self {
        self.push(name, dur, None);
        self
    }

    /// The metadata reads a phase made: time waited, calls and round trips.
    pub fn reads(&mut self, name: &str, r: ReadSummary) -> &mut Self {
        let desc = format!("{} calls, {} rounds", r.calls, r.rounds);
        self.push(name, r.waited, Some(&desc));
        self
    }

    fn push(&mut self, name: &str, dur: Duration, desc: Option<&str>) {
        if !self.value.is_empty() {
            self.value.push_str(", ");
        }
        let _ = write!(self.value, "{name};dur={:.3}", dur.as_secs_f64() * 1000.0);
        if let Some(d) = desc {
            let _ = write!(self.value, ";desc=\"{d}\"");
        }
    }

    /// Set the header on `resp`.
    pub fn apply(&self, resp: &mut Response) {
        if let Ok(v) = HeaderValue::from_str(&self.value) {
            resp.headers_mut().insert(SERVER_TIMING, v);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn phases_and_reads_render_in_order() {
        let mut t = ServerTiming::new();
        t.phase("resolve", Duration::from_micros(250)).reads(
            "db",
            ReadSummary {
                calls: 3,
                rounds: 1,
                waited: Duration::from_micros(1500),
            },
        );
        assert_eq!(
            t.value,
            "resolve;dur=0.250, db;dur=1.500;desc=\"3 calls, 1 rounds\""
        );
    }
}
