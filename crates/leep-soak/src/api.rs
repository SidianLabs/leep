//! HTTP client for the Leep API that measures every exchange and classifies
//! failures: throttling and restarts are retried and counted, anything else
//! unexpected is a violation.

use std::sync::Arc;
use std::time::{Duration, Instant};

use leep_core::api::{
    CasRefRequest, ChangesetInfo, CreateChangesetResponse, LandRequest, LandResponse,
    MissingBlobsRequest, MissingBlobsResponse, RefResponse, TreeResponse,
};
use leep_core::{Changeset, ChangesetId, Hash};
use reqwest::{Client, Method};
use serde::de::DeserializeOwned;

use crate::shared::Shared;

/// Longest one logical request may keep retrying before it counts as a stall.
const REQUEST_BUDGET: Duration = Duration::from_secs(90);

#[derive(Debug)]
pub enum Fail {
    /// The run is ending (deadline, or a violation stopped it).
    Stop,
    /// A request could not complete within its budget: a hang or deadlock.
    Stall,
    /// An answer nobody expected; already recorded as a violation.
    Bad,
}

pub type R<T> = Result<T, Fail>;

pub struct Resp {
    pub status: u16,
    pub body: Vec<u8>,
    pub retry_after: Option<Duration>,
    /// Carried `Leep-Refused`: refused before any work (ADR 0060).
    pub refused: bool,
    /// Exchanges it took, retries included.
    pub tries: u32,
}

impl Resp {
    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }

    pub fn json<T: DeserializeOwned>(&self) -> Result<T, String> {
        serde_json::from_slice(&self.body).map_err(|e| {
            format!(
                "bad json ({e}): {}",
                self.text().chars().take(200).collect::<String>()
            )
        })
    }
}

/// The answer to a land.
#[derive(Debug)]
#[allow(clippy::enum_variant_names)]
pub enum Landed {
    Landed {
        id: ChangesetId,
        rebased: bool,
        /// No exchange of this land was lost or retried.
        clean: bool,
    },
    Conflicts,
    /// A policy refusal (400): the message names what is missing.
    Rejected(String),
}

#[derive(Clone)]
pub struct Api {
    pub sh: Arc<Shared>,
    client: Client,
    base: String,
    token: Option<String>,
    pub who: String,
}

fn describe(e: &reqwest::Error) -> String {
    let mut s = e.to_string();
    let mut src = std::error::Error::source(e);
    while let Some(c) = src {
        s.push_str(": ");
        s.push_str(&c.to_string());
        src = c.source();
    }
    s
}

impl Api {
    pub fn new(sh: Arc<Shared>, base: &str, token: Option<String>, who: &str) -> Api {
        let client = Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(60))
            .tcp_nodelay(true)
            .build()
            .expect("http client");
        Api {
            sh,
            client,
            base: base.trim_end_matches('/').to_string(),
            token,
            who: who.to_string(),
        }
    }

    /// One exchange, measured. `Err` is a transport failure.
    async fn raw(
        &self,
        route: &'static str,
        method: &Method,
        path: &str,
        body: Option<&[u8]>,
    ) -> Result<Resp, String> {
        let t0 = Instant::now();
        let mut rq = self
            .client
            .request(method.clone(), format!("{}{path}", self.base));
        if let Some(t) = &self.token {
            rq = rq.bearer_auth(t);
        }
        if let Some(b) = body {
            rq = rq
                .header("content-type", "application/json")
                .body(b.to_vec());
        }
        let res = rq.send().await.map_err(|e| describe(&e))?;
        let status = res.status().as_u16();
        let retry_after = res
            .headers()
            .get("retry-after")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.trim().parse::<f64>().ok())
            .map(Duration::from_secs_f64);
        let refused = res.headers().contains_key(leep_core::api::REFUSED_HEADER);
        let bytes = res.bytes().await.map_err(|e| describe(&e))?;
        let dt = t0.elapsed();
        self.sh.stats.record(
            route,
            status,
            dt.as_micros().min(u128::from(u32::MAX)) as u32,
            self.sh.now_secs() as u32,
        );
        Ok(Resp {
            status,
            body: bytes.to_vec(),
            retry_after,
            refused,
            tries: 1,
        })
    }

    fn trouble(&self, what: &str, kind: &str, route: &str, path: &str, started: f64) {
        let now = self.sh.now_secs();
        if self.sh.in_chaos(now) || self.sh.in_chaos(started) {
            self.sh.stats.inc("attributed_to_restart");
        } else {
            self.sh
                .violate(kind, format!("{route} {path} by {}: {what}", self.who));
        }
    }

    async fn pause(&self, d: Duration) -> R<()> {
        let end = Instant::now() + d;
        while Instant::now() < end {
            if self.sh.stopped() {
                return Err(Fail::Stop);
            }
            tokio::time::sleep((end - Instant::now()).min(Duration::from_millis(100))).await;
        }
        Ok(())
    }

    /// Send until the server gives an answer a caller can act on: throttling
    /// (429), shed reads (503 with `Leep-Refused`), request timeouts (408),
    /// transport failures and other 5xx are waited
    /// out and counted. A transport failure or 5xx outside a restart is a
    /// violation (the retry still happens, so the run can show what follows).
    pub async fn call(
        &self,
        route: &'static str,
        method: Method,
        path: &str,
        body: Option<&[u8]>,
    ) -> R<Resp> {
        let started = self.sh.now_secs();
        let t0 = Instant::now();
        let mut tries = 0u32;
        loop {
            if self.sh.stopped() {
                return Err(Fail::Stop);
            }
            if t0.elapsed() > REQUEST_BUDGET {
                self.sh.violate(
                    "stall",
                    format!(
                        "{route} {path} by {} got no usable answer in {}s",
                        self.who,
                        REQUEST_BUDGET.as_secs()
                    ),
                );
                return Err(Fail::Stall);
            }
            tries += 1;
            match self.raw(route, &method, path, body).await {
                Err(e) => {
                    self.sh.stats.inc("transport_errors");
                    self.trouble(&e, "transport_error", route, path, started);
                    self.pause(Duration::from_millis(150 + (tries as u64 * 50).min(800)))
                        .await?;
                }
                Ok(mut r) => {
                    r.tries = tries;
                    match r.status {
                        429 => {
                            self.sh.stats.inc("throttled_429");
                            let wait = r
                                .retry_after
                                .unwrap_or(Duration::from_millis(100))
                                .clamp(Duration::from_millis(20), Duration::from_secs(2));
                            self.pause(wait).await?;
                        }
                        // A read shed because its class is full (ADR 0060):
                        // expected under load, waited out like a 429.
                        503 if r.refused => {
                            self.sh.stats.inc("shed_503");
                            let wait = r
                                .retry_after
                                .unwrap_or(Duration::from_millis(100))
                                .clamp(Duration::from_millis(20), Duration::from_secs(2));
                            self.pause(wait).await?;
                        }
                        408 => {
                            self.sh.stats.inc("timeouts_408");
                            self.pause(Duration::from_millis(200)).await?;
                        }
                        500..=599 => {
                            self.sh.stats.inc("server_errors_5xx");
                            self.trouble(
                                &format!("{} {}", r.status, r.text()),
                                "server_error",
                                route,
                                path,
                                started,
                            );
                            self.pause(Duration::from_millis(300)).await?;
                        }
                        _ => return Ok(r),
                    }
                }
            }
        }
    }

    pub fn want(&self, route: &str, path: &str, r: Resp, ok: &[u16]) -> R<Resp> {
        if ok.contains(&r.status) {
            return Ok(r);
        }
        self.sh.violate(
            "unexpected_status",
            format!(
                "{route} {path} by {}: {} {}",
                self.who,
                r.status,
                r.text().chars().take(300).collect::<String>()
            ),
        );
        Err(Fail::Bad)
    }

    pub fn parsed<T: DeserializeOwned>(&self, route: &str, path: &str, r: &Resp) -> R<T> {
        r.json().map_err(|e| {
            self.sh.violate(
                "bad_response",
                format!("{route} {path} by {}: {e}", self.who),
            );
            Fail::Bad
        })
    }

    pub async fn get_ok<T: DeserializeOwned>(&self, route: &'static str, path: &str) -> R<T> {
        let r = self.call(route, Method::GET, path, None).await?;
        let r = self.want(route, path, r, &[200])?;
        self.parsed(route, path, &r)
    }

    // ---- typed operations ----

    pub async fn get_ref(&self, name: &str) -> R<Option<ChangesetId>> {
        let r: RefResponse = self.get_ok("ref.get", &format!("/v1/refs/{name}")).await?;
        Ok(r.target)
    }

    pub async fn changeset(&self, id: ChangesetId) -> R<ChangesetInfo> {
        self.get_ok("changeset.get", &format!("/v1/changesets/{id}"))
            .await
    }

    pub async fn tree(&self, id: ChangesetId, dir: &str) -> R<TreeResponse> {
        self.get_ok("tree", &format!("/v1/tree/{id}?path={dir}"))
            .await
    }

    /// `None` when the file does not exist at `id`.
    pub async fn file(&self, id: ChangesetId, path: &str) -> R<Option<Vec<u8>>> {
        let p = format!("/v1/file/{id}?path={path}");
        let r = self.call("file", Method::GET, &p, None).await?;
        if r.status == 404 {
            return Ok(None);
        }
        Ok(Some(self.want("file", &p, r, &[200])?.body))
    }

    /// Upload what the server lacks. A 409 means a collect is finishing a
    /// delete of that blob; waiting and repeating is the documented answer.
    pub async fn upload(&self, blobs: &[(Hash, Vec<u8>)]) -> R<()> {
        if blobs.is_empty() {
            return Ok(());
        }
        let q = serde_json::to_vec(&MissingBlobsRequest {
            hashes: blobs.iter().map(|(h, _)| *h).collect(),
        })
        .unwrap();
        let r = self
            .call("blobs.missing", Method::POST, "/v1/blobs/missing", Some(&q))
            .await?;
        let r = self.want("blobs.missing", "/v1/blobs/missing", r, &[200])?;
        let missing: MissingBlobsResponse = self.parsed("blobs.missing", "", &r)?;
        for (h, data) in blobs {
            if !missing.missing.contains(h) {
                continue;
            }
            let path = format!("/v1/blobs/{h}");
            loop {
                let r = self
                    .call("blobs.put", Method::PUT, &path, Some(data))
                    .await?;
                if r.status == 409 {
                    self.sh.stats.inc("blob_put_409");
                    self.pause(Duration::from_millis(500)).await?;
                    continue;
                }
                self.want("blobs.put", &path, r, &[204])?;
                break;
            }
        }
        Ok(())
    }

    /// `Ok(Err(text))` when a blob the changeset names is not (or no longer)
    /// stored, which GC makes legitimate: the caller uploads again.
    pub async fn create_changeset(&self, cs: &Changeset) -> R<Result<ChangesetId, String>> {
        let body = serde_json::to_vec(cs).unwrap();
        let r = self
            .call(
                "changeset.create",
                Method::POST,
                "/v1/changesets",
                Some(&body),
            )
            .await?;
        if r.status == 400 && r.text().contains("missing blob") {
            self.sh.stats.inc("missing_blob_retries");
            return Ok(Err(r.text()));
        }
        let r = self.want("changeset.create", "/v1/changesets", r, &[200])?;
        let made: CreateChangesetResponse = self.parsed("changeset.create", "", &r)?;
        Ok(Ok(made.id))
    }

    pub async fn land(&self, cs: ChangesetId, target: &str) -> R<Landed> {
        let body = serde_json::to_vec(&LandRequest {
            changeset: cs,
            target_ref: target.to_string(),
            create: None,
            land_id: None,
        })
        .unwrap();
        let mut clean = true;
        loop {
            let r = self
                .call("land", Method::POST, "/v1/land", Some(&body))
                .await?;
            clean &= r.tries == 1;
            match r.status {
                200 => {
                    return match self.parsed::<LandResponse>("land", "", &r)? {
                        LandResponse::Landed {
                            landed, rebased, ..
                        } => Ok(Landed::Landed {
                            id: landed,
                            rebased,
                            clean,
                        }),
                        LandResponse::Conflicts { .. } => Ok(Landed::Conflicts),
                    };
                }
                // The CAS lost every retry: land is idempotent, ask again.
                409 => {
                    self.sh.stats.inc("land_409");
                    clean = false;
                    self.pause(Duration::from_millis(100)).await?;
                }
                400 => return Ok(Landed::Rejected(r.text())),
                _ => {
                    self.want("land", "/v1/land", r, &[200])?;
                }
            }
        }
    }

    /// Compare-and-swap a ref. A lost answer is settled by reading the ref
    /// back, because a repeated swap that already applied would get 409.
    pub async fn cas_ref(
        &self,
        name: &str,
        expected: Option<ChangesetId>,
        new: Option<ChangesetId>,
    ) -> R<bool> {
        let path = format!("/v1/refs/{name}");
        let body = serde_json::to_vec(&CasRefRequest {
            expected,
            new,
            ..Default::default()
        })
        .unwrap();
        let r = self
            .call("ref.cas", Method::POST, &path, Some(&body))
            .await?;
        let applied_unseen = r.tries > 1;
        match r.status {
            200 => Ok(true),
            409 if applied_unseen => Ok(self.get_ref(name).await? == new),
            409 => Ok(false),
            _ => {
                self.want("ref.cas", &path, r, &[200])?;
                Ok(false)
            }
        }
    }
}
