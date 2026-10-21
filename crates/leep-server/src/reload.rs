//! Config reload without restart (ADR 0024).
//!
//! Everything a request consults that comes from a file (tokens and their
//! limits, trusted signing keys, the review policy and so the land policy) is
//! one immutable [`RuntimeConfig`]. [`ConfigHandle`] holds the current one; a
//! reload (SIGHUP) reads and validates every file first and swaps in a new
//! config only if all of them are good. Each request takes one snapshot when
//! it arrives and uses it to the end, so a request never sees tokens from one
//! config and policy from another, and a long upload or `land` finishes under
//! the config it started with.
//!
//! Startup uses the same loader, so reload re-runs every startup check.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use arc_swap::ArcSwap;
use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::Response;
use leep_core::MetaStore;
use leep_derive::{LandPolicy, PolicySet};

use crate::auth::{Auth, AuthChange};
use crate::fingerprint::Canon;
use crate::limits::{Defaults, Limiter, LimiterChange};
use crate::protect::ProtectOnly;
use crate::review::{RequireReview, ReviewPolicy, ReviewService};
use crate::signing::{RequireSignatures, SigningKeys};

/// Where the reloadable config comes from: three optional files, plus the
/// flags that shape how they are used. Flags are fixed for the process; only
/// the files are re-read.
#[derive(Clone, Debug, Default)]
pub struct ReloadSources {
    pub tokens_file: Option<PathBuf>,
    pub signing_keys_file: Option<PathBuf>,
    pub review_policy_file: Option<PathBuf>,
    pub limit_defaults: Defaults,
    /// `--require-signatures`.
    pub require_signatures: Vec<String>,
    /// `--require-review`; the policy file's `refs` are added to it.
    pub require_review: Vec<String>,
    /// `--protect-ref`: forward-only and undeletable, with no other check.
    pub protect_refs: Vec<String>,
}

/// One consistent configuration. Never mutated after it is built.
pub struct RuntimeConfig {
    /// 1 at startup, plus one per successful reload. Per process: it says
    /// nothing about whether two servers run the same config; compare
    /// [`fingerprint`](Self::fingerprint) for that.
    pub generation: u64,
    /// What the config contains, as 16 hex characters (see
    /// [`crate::fingerprint`]). Safe to log and export: it holds no secret.
    pub fingerprint: String,
    pub auth: Option<Auth>,
    pub limiter: Option<Arc<Limiter>>,
    pub signing: Option<Arc<SigningKeys>>,
    pub review: Option<Arc<ReviewService>>,
    /// Signatures, review and `--protect-ref` composed, so the forward-only
    /// and protected delete rules and `land` all read the same refs.
    pub land_policy: Option<Arc<dyn LandPolicy>>,
    protected_refs: Vec<String>,
    review_rules: (Vec<String>, Vec<String>),
}

impl RuntimeConfig {
    /// A config that never reloads, for callers that hand the router parts.
    /// With `auth` and no `review`, a default review service is built from
    /// the tokens.
    pub fn fixed(
        auth: Option<Auth>,
        signing: Option<Arc<SigningKeys>>,
        review: Option<Arc<ReviewService>>,
        limiter: Option<Arc<Limiter>>,
        land_policy: Option<Arc<dyn LandPolicy>>,
    ) -> RuntimeConfig {
        let review = review.or_else(|| {
            auth.as_ref().map(|a| {
                Arc::new(ReviewService::new(
                    a.reviewers(),
                    leep_derive::review::ReviewRules::default(),
                ))
            })
        });
        // A fixed config knows only the parts it was handed, so its fingerprint
        // covers the tokens and keys; limits and protected refs are opaque here.
        let mut canon = Canon::new();
        fingerprint_parts(
            &mut canon,
            auth.as_ref(),
            &Defaults::default(),
            signing.as_deref(),
        );
        RuntimeConfig {
            generation: 1,
            fingerprint: canon.finish(),
            auth,
            limiter,
            signing,
            review,
            land_policy,
            protected_refs: Vec::new(),
            review_rules: Default::default(),
        }
    }

    /// Ref names the land policy protects, sorted, for the missing-ref warning.
    pub fn protected_refs(&self) -> &[String] {
        &self.protected_refs
    }
}

/// A loaded config and what became of limiter state.
struct Loaded {
    config: RuntimeConfig,
    limiter: LimiterChange,
}

/// The tokens, server-wide limit defaults and trusted keys. The rest of the
/// fingerprint (review policy, protected refs) is added by the caller that
/// has it.
fn fingerprint_parts(
    c: &mut Canon,
    auth: Option<&Auth>,
    defaults: &Defaults,
    signing: Option<&SigningKeys>,
) {
    match auth {
        Some(a) => {
            c.u64(1);
            a.fingerprint_into(c);
        }
        None => {
            c.u64(0);
        }
    }
    c.str("limit-defaults")
        .opt_f64(defaults.rate)
        .opt_u64(defaults.burst.map(u64::from))
        .opt_u64(defaults.concurrency.map(|n| n as u64))
        .opt_u64(defaults.read_max_body.map(|n| n as u64));
    match signing {
        Some(k) => {
            c.u64(1);
            k.fingerprint_into(c);
        }
        None => {
            c.u64(0);
        }
    }
}

fn sorted_union(a: &[String], b: &[String]) -> Vec<String> {
    let mut all: Vec<String> = a.iter().chain(b).cloned().collect();
    all.sort();
    all.dedup();
    all
}

impl ReloadSources {
    /// Whether there is any file to re-read.
    pub fn has_files(&self) -> bool {
        self.tokens_file.is_some()
            || self.signing_keys_file.is_some()
            || self.review_policy_file.is_some()
    }

    /// Read and validate every file and build the config they describe, or
    /// say why not. `prev` supplies limiter state to carry over. Nothing here
    /// touches live state, so a failure leaves the old config running.
    fn load(&self, prev: Option<&RuntimeConfig>) -> Result<Loaded, String> {
        let auth = self
            .tokens_file
            .as_deref()
            .map(Auth::from_file)
            .transpose()?;
        let (limiter, limiter_change) = Limiter::reload(
            prev.and_then(|p| p.limiter.as_deref()),
            auth.as_ref(),
            &self.limit_defaults,
        )
        .map_err(|e| format!("request limits: {e}"))?;
        let signing = match &self.signing_keys_file {
            Some(path) => Some(Arc::new(SigningKeys::from_file(path)?)),
            None if !self.require_signatures.is_empty() => {
                return Err("--require-signatures needs --signing-keys-file".into())
            }
            None => None,
        };
        let policy = match &self.review_policy_file {
            Some(path) => ReviewPolicy::from_file(path)?,
            None => ReviewPolicy::parse("{}")?,
        };
        let review_refs = sorted_union(&self.require_review, &policy.refs);
        crate::check_protected_refs("--protect-ref", &self.protect_refs)?;
        crate::check_protected_refs("--require-signatures", &self.require_signatures)?;
        crate::check_protected_refs("--require-review or the review policy", &review_refs)?;
        let review = match &auth {
            Some(a) => Some(Arc::new(ReviewService::new(a.reviewers(), policy.rules))),
            None if !review_refs.is_empty() || self.review_policy_file.is_some() => {
                return Err("--require-review and --review-policy-file need --tokens-file".into())
            }
            None => None,
        };

        let mut policies: Vec<Arc<dyn LandPolicy>> = Vec::new();
        if let (Some(keys), false) = (&signing, self.require_signatures.is_empty()) {
            policies.push(Arc::new(RequireSignatures::new(
                keys.clone(),
                self.require_signatures.clone(),
            )));
        }
        if let (Some(service), false) = (&review, review_refs.is_empty()) {
            policies.push(Arc::new(RequireReview::new(
                service.clone(),
                review_refs.clone(),
            )));
        }
        if !self.protect_refs.is_empty() {
            policies.push(Arc::new(ProtectOnly::new(self.protect_refs.clone())));
        }
        let land_policy: Option<Arc<dyn LandPolicy>> = match policies.len() {
            0 => None,
            1 => policies.pop(),
            _ => Some(Arc::new(PolicySet::new(policies))),
        };

        let mut canon = Canon::new();
        fingerprint_parts(
            &mut canon,
            auth.as_ref(),
            &self.limit_defaults,
            signing.as_deref(),
        );
        canon
            .set("auto_approve", &policy.auto_approve)
            .set("docs_globs", &policy.docs_globs)
            .set("require_review", &review_refs)
            .set("require_signatures", &self.require_signatures);
        // Only when set, so configs without it keep their fingerprint.
        if !self.protect_refs.is_empty() {
            canon.set("protect_refs", &self.protect_refs);
        }
        Ok(Loaded {
            config: RuntimeConfig {
                generation: prev.map_or(1, |p| p.generation + 1),
                fingerprint: canon.finish(),
                auth,
                limiter: limiter.map(Arc::new),
                signing,
                review,
                land_policy,
                protected_refs: sorted_union(
                    &sorted_union(&self.require_signatures, &review_refs),
                    &self.protect_refs,
                ),
                review_rules: (policy.auto_approve, policy.docs_globs),
            },
            limiter: limiter_change,
        })
    }
}

/// What a reload changed, by name. Holds no secrets and no digests.
#[derive(Debug, Default)]
pub struct ReloadReport {
    pub generation: u64,
    pub fingerprint: String,
    /// The fingerprint before this reload.
    pub previous_fingerprint: String,
    pub tokens: AuthChange,
    pub limits: LimiterChange,
    pub keys_added: Vec<String>,
    pub keys_removed: Vec<String>,
    pub protected_added: Vec<String>,
    pub protected_removed: Vec<String>,
    pub review_rules_changed: bool,
}

fn removed_and_added(old: &[String], new: &[String]) -> (Vec<String>, Vec<String>) {
    let gone = old.iter().filter(|n| !new.contains(n)).cloned().collect();
    let fresh = new.iter().filter(|n| !old.contains(n)).cloned().collect();
    (gone, fresh)
}

impl ReloadReport {
    fn between(old: &RuntimeConfig, new: &RuntimeConfig, limits: LimiterChange) -> ReloadReport {
        let tokens = match (&old.auth, &new.auth) {
            (Some(o), Some(n)) => o.diff(n),
            _ => AuthChange::default(),
        };
        let (keys_added, keys_removed) = match (&old.signing, &new.signing) {
            (Some(o), Some(n)) => o.diff(n),
            _ => Default::default(),
        };
        let (protected_removed, protected_added) =
            removed_and_added(&old.protected_refs, &new.protected_refs);
        ReloadReport {
            generation: new.generation,
            fingerprint: new.fingerprint.clone(),
            previous_fingerprint: old.fingerprint.clone(),
            tokens,
            limits,
            keys_added,
            keys_removed,
            protected_added,
            protected_removed,
            review_rules_changed: old.review_rules != new.review_rules,
        }
    }

    /// Whether anything the report covers differs.
    pub fn is_empty(&self) -> bool {
        self.tokens.is_empty()
            && self.limits.reset.is_empty()
            && self.limits.dropped.is_empty()
            && self.keys_added.is_empty()
            && self.keys_removed.is_empty()
            && self.protected_added.is_empty()
            && self.protected_removed.is_empty()
            && !self.review_rules_changed
    }

    /// One info line per kind of change. Names only.
    pub fn log(&self) {
        let t = &self.tokens;
        if !t.is_empty() {
            tracing::info!(
                added = ?t.added,
                removed = ?t.removed,
                rotated = ?t.rotated,
                role_changed = ?t.role_changed,
                limits_changed = ?t.limits_changed,
                expiry_changed = ?t.expiry_changed,
                "tokens changed"
            );
        }
        let l = &self.limits;
        if !(l.reset.is_empty() && l.dropped.is_empty()) {
            tracing::info!(
                kept = l.kept,
                reset = ?l.reset,
                dropped = ?l.dropped,
                "request limit state updated"
            );
        }
        if !(self.keys_added.is_empty() && self.keys_removed.is_empty()) {
            tracing::info!(
                added = ?self.keys_added,
                removed = ?self.keys_removed,
                "trusted signing keys changed"
            );
        }
        if !(self.protected_added.is_empty() && self.protected_removed.is_empty()) {
            tracing::info!(
                added = ?self.protected_added,
                removed = ?self.protected_removed,
                "protected refs changed"
            );
        }
        if self.review_rules_changed {
            tracing::info!("review rules changed");
        }
        tracing::info!(
            generation = self.generation,
            fingerprint = %self.fingerprint,
            previous_fingerprint = %self.previous_fingerprint,
            changed = !self.is_empty(),
            "config reloaded"
        );
    }
}

/// The live config and the means to replace it.
pub struct ConfigHandle {
    current: ArcSwap<RuntimeConfig>,
    sources: Option<ReloadSources>,
    /// Reloads build on the previous config (limiter state, generation), so
    /// two at once would race.
    serial: Mutex<()>,
}

fn now_secs() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0.0, |d| d.as_secs_f64())
}

/// Publish a loaded config. `leep_config_info` has one series per
/// fingerprint the process has run; the previous one is zeroed (the metrics
/// crate cannot remove a series) so `leep_config_info == 1` selects exactly
/// the config in force.
fn record_loaded(config: &RuntimeConfig, previous: Option<&str>) {
    if let Some(old) = previous.filter(|old| *old != config.fingerprint) {
        metrics::gauge!("leep_config_info", "fingerprint" => old.to_string()).set(0.0);
    }
    metrics::gauge!("leep_config_info", "fingerprint" => config.fingerprint.clone()).set(1.0);
    metrics::gauge!("leep_config_generation").set(config.generation as f64);
    metrics::gauge!("leep_config_last_reload_timestamp_seconds").set(now_secs());
    record_token_expiry(config, false);
}

/// Set `leep_tokens_expired` and `leep_tokens_expiring_soon` from the token
/// table, and with `warn` log the names of tokens in either group. Expiry
/// moves with the clock, not with reloads, so the server also calls this
/// periodically ([`spawn_token_expiry_gauges`]).
pub fn record_token_expiry(config: &RuntimeConfig, warn: bool) {
    let r = config
        .auth
        .as_ref()
        .map(|a| a.expiry_report(jiff::Timestamp::now()))
        .unwrap_or_default();
    metrics::gauge!("leep_tokens_expired").set(r.expired.len() as f64);
    metrics::gauge!("leep_tokens_expiring_soon").set(r.soon.len() as f64);
    if warn && !r.expired.is_empty() {
        tracing::warn!(tokens = ?r.expired, "tokens in the tokens file have expired and are refused");
    }
    if warn && !r.soon.is_empty() {
        tracing::warn!(tokens = ?r.soon, "tokens expire within 7 days");
    }
}

/// Refresh the token expiry gauges every `every` for the life of the process.
pub fn spawn_token_expiry_gauges(handle: Arc<ConfigHandle>, every: std::time::Duration) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(every);
        tick.tick().await;
        loop {
            tick.tick().await;
            record_token_expiry(&handle.current(), false);
        }
    });
}

impl ConfigHandle {
    /// Load the config for startup. The same checks as a reload, so a file
    /// that would fail a reload fails startup.
    pub fn load(sources: ReloadSources) -> Result<Arc<ConfigHandle>, String> {
        // Registered at zero so an alert on errors sees the series from the start.
        metrics::counter!("leep_config_reload_total", "result" => "ok").increment(0);
        metrics::counter!("leep_config_reload_total", "result" => "error").increment(0);
        let loaded = sources.load(None)?;
        record_loaded(&loaded.config, None);
        Ok(Arc::new(ConfigHandle {
            current: ArcSwap::from_pointee(loaded.config),
            sources: Some(sources),
            serial: Mutex::new(()),
        }))
    }

    /// A handle that holds `config` and refuses to reload.
    pub fn fixed(config: RuntimeConfig) -> Arc<ConfigHandle> {
        Arc::new(ConfigHandle {
            current: ArcSwap::from_pointee(config),
            sources: None,
            serial: Mutex::new(()),
        })
    }

    /// The config to use for one request or operation. Cheap; keep the result
    /// for as long as the operation runs.
    pub fn current(&self) -> Arc<RuntimeConfig> {
        self.current.load_full()
    }

    /// Whether this handle can reload at all.
    pub fn reloadable(&self) -> bool {
        self.sources.as_ref().is_some_and(ReloadSources::has_files)
    }

    /// Re-read the files and, only if all of them are valid, replace the
    /// config. On error the old config stays in force.
    pub fn reload(&self) -> Result<ReloadReport, String> {
        let Some(sources) = &self.sources else {
            return Err("this server's config is fixed".into());
        };
        let _serial = self.serial.lock().unwrap_or_else(|e| e.into_inner());
        let prev = self.current.load_full();
        match sources.load(Some(&prev)) {
            Ok(loaded) => {
                let report = ReloadReport::between(&prev, &loaded.config, loaded.limiter);
                record_loaded(&loaded.config, Some(&prev.fingerprint));
                self.current.store(Arc::new(loaded.config));
                metrics::counter!("leep_config_reload_total", "result" => "ok").increment(1);
                Ok(report)
            }
            Err(e) => {
                metrics::counter!("leep_config_reload_total", "result" => "error").increment(1);
                Err(e)
            }
        }
    }
}

/// Middleware that pins the request to the config current when it arrived.
/// Layer it outside authentication; everything inside reads the snapshot from
/// the request's extensions.
pub async fn snapshot(
    State(handle): State<Arc<ConfigHandle>>,
    mut req: Request,
    next: Next,
) -> Response {
    req.extensions_mut().insert(handle.current());
    next.run(req).await
}

/// Warn about protected refs that do not exist, since protection matches names
/// exactly and a typo would leave the intended ref open.
pub async fn warn_missing_refs(meta: &dyn MetaStore, config: &RuntimeConfig) {
    for name in config.protected_refs() {
        match meta.get_ref(name).await {
            Ok(None) => tracing::warn!(
                name = %name,
                "protected ref does not exist yet; check the name if it should"
            ),
            Ok(Some(_)) => {}
            Err(e) => tracing::warn!(name = %name, error = %e, "could not check protected ref"),
        }
    }
}

/// Reload on SIGHUP for the life of the process. The handler is installed
/// before this returns, so a signal sent right after startup reloads instead
/// of killing the server. Only SIGHUP triggers a reload: there is no endpoint
/// and no file watcher.
#[cfg(unix)]
pub fn spawn_sighup_reload(
    handle: Arc<ConfigHandle>,
    meta: Arc<dyn MetaStore>,
) -> std::io::Result<()> {
    use tokio::signal::unix::{signal, SignalKind};
    let mut hup = signal(SignalKind::hangup())?;
    tokio::spawn(async move {
        while hup.recv().await.is_some() {
            if !handle.reloadable() {
                tracing::info!("SIGHUP: no tokens, signing keys or review policy file to reload");
                continue;
            }
            tracing::info!("SIGHUP: reloading config");
            let h = handle.clone();
            match tokio::task::spawn_blocking(move || h.reload()).await {
                Ok(Ok(report)) => {
                    report.log();
                    warn_missing_refs(meta.as_ref(), &handle.current()).await;
                }
                Ok(Err(e)) => tracing::error!(
                    error = %e,
                    "config reload failed; keeping the previous config"
                ),
                Err(e) => tracing::error!(error = %e, "config reload task failed"),
            }
        }
    });
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::hash_token;

    fn write(dir: &tempfile::TempDir, name: &str, text: &str) -> PathBuf {
        let p = dir.path().join(name);
        std::fs::write(&p, text).unwrap();
        p
    }

    fn tokens(entries: &[(&str, &str, &str)]) -> String {
        let t: Vec<_> = entries
            .iter()
            .map(|(n, r, tok)| serde_json::json!({ "name": n, "role": r, "hash": hash_token(tok) }))
            .collect();
        serde_json::json!({ "tokens": t }).to_string()
    }

    fn key(seed: u8) -> String {
        let k = ed25519_dalek::SigningKey::from_bytes(&[seed; 32]);
        leep_core::signature::format_public_key(k.verifying_key().as_bytes())
    }

    fn keys(entries: &[(&str, &str)]) -> String {
        let k: Vec<_> = entries
            .iter()
            .map(|(n, k)| serde_json::json!({ "name": n, "public_key": k }))
            .collect();
        serde_json::json!({ "keys": k }).to_string()
    }

    struct Files {
        dir: tempfile::TempDir,
        sources: ReloadSources,
    }

    fn files() -> Files {
        let dir = tempfile::tempdir().unwrap();
        let sources = ReloadSources {
            tokens_file: Some(write(
                &dir,
                "tokens.json",
                &tokens(&[("ci", "write", "t1"), ("viewer", "read", "t2")]),
            )),
            signing_keys_file: Some(write(&dir, "keys.json", &keys(&[("alice", &key(1))]))),
            review_policy_file: Some(write(&dir, "policy.json", r#"{ "refs": ["main"] }"#)),
            require_signatures: vec!["release".into()],
            ..Default::default()
        };
        Files { dir, sources }
    }

    impl Files {
        fn put(&self, name: &str, text: &str) {
            write(&self.dir, name, text);
        }
        fn handle(&self) -> Arc<ConfigHandle> {
            ConfigHandle::load(self.sources.clone()).unwrap()
        }
    }

    #[test]
    fn startup_and_reload_share_the_checks() {
        let f = files();
        f.put("policy.json", r#"{ "refs": ["main "] }"#);
        let e = ConfigHandle::load(f.sources.clone()).err().unwrap();
        assert!(e.contains("review policy"), "{e}");
        f.put("policy.json", r#"{ "refs": ["main"] }"#);
        f.put("keys.json", &keys(&[]));
        assert!(ConfigHandle::load(f.sources.clone()).is_err());
    }

    #[test]
    fn a_bad_file_keeps_the_whole_old_config() {
        let f = files();
        let h = f.handle();
        let before = h.current();
        // The tokens file is fine and changes; the policy file is not.
        f.put("tokens.json", &tokens(&[("ci", "write", "new")]));
        f.put("policy.json", r#"{ "refs": ["main"], "nope": 1 }"#);
        let e = h.reload().unwrap_err();
        assert!(e.contains("review policy file"), "{e}");
        assert!(Arc::ptr_eq(&before, &h.current()));
        assert_eq!(h.current().generation, 1);
        assert_eq!(h.current().auth.as_ref().unwrap().len(), 2);
    }

    #[test]
    fn every_kind_of_bad_input_is_refused() {
        for (file, text) in [
            ("tokens.json", "{ not json"),
            ("tokens.json", r#"{ "tokens": [] }"#),
            (
                "tokens.json",
                r#"{ "tokens": [{ "name": "x", "role": "write", "hash": "plain" }] }"#,
            ),
            ("keys.json", &keys(&[("a", &key(1)), ("b", &key(1))])),
            // The identity point is a small-order key.
            (
                "keys.json",
                &keys(&[(
                    "weak",
                    "ed25519:0100000000000000000000000000000000000000000000000000000000000000",
                )]),
            ),
            ("policy.json", r#"{ "refs": [""] }"#),
            ("policy.json", r#"{ "refs": ["bad name"] }"#),
            ("policy.json", r#"{ "auto_approve": ["magic"] }"#),
        ] {
            let f = files();
            let h = f.handle();
            f.put(file, text);
            assert!(h.reload().is_err(), "{file}: {text}");
            assert_eq!(h.current().generation, 1, "{file}: {text}");
        }
    }

    #[test]
    fn a_missing_file_is_an_error_not_an_empty_config() {
        let f = files();
        let h = f.handle();
        std::fs::remove_file(f.sources.tokens_file.as_ref().unwrap()).unwrap();
        let e = h.reload().unwrap_err();
        assert!(e.contains("cannot read tokens file"), "{e}");
        assert!(h.current().auth.is_some());
    }

    #[test]
    fn a_good_reload_swaps_everything_and_reports_names() {
        let f = files();
        let h = f.handle();
        f.put(
            "tokens.json",
            &tokens(&[("ci", "write", "rotated"), ("bot", "write", "t3")]),
        );
        f.put("keys.json", &keys(&[("bob", &key(2))]));
        f.put(
            "policy.json",
            r#"{ "refs": ["main", "stable"], "auto_approve": ["whitespace_only"] }"#,
        );
        let r = h.reload().unwrap();
        assert_eq!(r.generation, 2);
        assert_eq!(r.tokens.added, ["bot"]);
        assert_eq!(r.tokens.removed, ["viewer"]);
        assert_eq!(r.tokens.rotated, ["ci"]);
        assert_eq!(r.keys_added, ["bob"]);
        assert_eq!(r.keys_removed, ["alice"]);
        assert_eq!(r.protected_added, ["stable"]);
        assert!(r.protected_removed.is_empty());
        assert!(r.review_rules_changed);
        let c = h.current();
        assert_eq!(c.generation, 2);
        assert_eq!(c.protected_refs(), ["main", "release", "stable"]);
        let p = c.land_policy.as_ref().unwrap();
        assert!(p.applies_to("stable") && p.applies_to("release") && p.applies_to("main"));
        assert!(!p.applies_to("dev"));
    }

    #[test]
    fn protect_refs_alone_make_a_policy_that_covers_them() {
        let sources = ReloadSources {
            protect_refs: vec!["main".into(), "release".into()],
            ..Default::default()
        };
        let h = ConfigHandle::load(sources.clone()).unwrap();
        let c = h.current();
        assert_eq!(c.protected_refs(), ["main", "release"]);
        let p = c.land_policy.as_ref().expect("a policy");
        assert!(p.applies_to("main") && p.applies_to("release"));
        assert!(!p.applies_to("dev"));
        // No tokens, keys or review: protection needs none of them.
        assert!(c.auth.is_none() && c.signing.is_none() && c.review.is_none());

        // Composed with the others, every ref stays covered.
        let f = files();
        let mut s = f.sources.clone();
        s.protect_refs = vec!["stable".into()];
        let c = ConfigHandle::load(s).unwrap().current();
        assert_eq!(c.protected_refs(), ["main", "release", "stable"]);
        let p = c.land_policy.as_ref().unwrap();
        assert!(p.applies_to("stable") && p.applies_to("main") && p.applies_to("release"));
    }

    #[test]
    fn protect_ref_names_are_checked() {
        for bad in ["main ", " release", "", "a..b"] {
            let sources = ReloadSources {
                protect_refs: vec![bad.into()],
                ..Default::default()
            };
            let e = ConfigHandle::load(sources).err().expect(bad);
            assert!(e.contains("--protect-ref"), "{bad:?}: {e}");
        }
    }

    #[test]
    fn an_unchanged_reload_reports_nothing() {
        let f = files();
        let h = f.handle();
        let r = h.reload().unwrap();
        assert!(r.is_empty(), "{r:?}");
        assert_eq!(r.generation, 2);
    }

    fn fp(f: &Files) -> String {
        f.handle().current().fingerprint.clone()
    }

    #[test]
    fn the_fingerprint_is_sixteen_hex_characters_and_stable() {
        let f = files();
        let a = fp(&f);
        assert_eq!(a.len(), 16);
        assert!(a
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)));
        assert_eq!(a, fp(&f));
        // A second set of files with the same content, elsewhere on disk.
        assert_eq!(a, fp(&files()));
    }

    #[test]
    fn order_and_formatting_do_not_change_the_fingerprint() {
        let f = files();
        let base = fp(&f);
        // Tokens reversed and pretty-printed with extra whitespace.
        let reordered = serde_json::to_string_pretty(&serde_json::json!({ "tokens": [
            { "role": "read", "hash": hash_token("t2"), "name": "viewer" },
            { "name": "ci", "hash": hash_token("t1"), "role": "write" },
        ] }))
        .unwrap();
        f.put("tokens.json", &format!("\n\n{reordered}\n"));
        f.put(
            "policy.json",
            "{\n  \"refs\": [ \"main\" ],\n  \"auto_approve\": []\n}\n",
        );
        assert_eq!(fp(&f), base);

        // Sets given in a different order or with repeats.
        let mut a = files();
        let mut b = files();
        a.sources.require_signatures = vec!["release".into(), "stable".into()];
        b.sources.require_signatures = vec!["stable".into(), "release".into(), "stable".into()];
        a.put("policy.json", r#"{ "refs": ["main", "dev"] }"#);
        b.put("policy.json", r#"{ "refs": ["dev", "main"] }"#);
        assert_eq!(fp(&a), fp(&b));
    }

    #[test]
    fn every_semantic_change_changes_the_fingerprint() {
        let base = fp(&files());
        let mut seen = vec![base.clone()];
        let mut check = |what: &str, f: Files| {
            let got = fp(&f);
            assert!(
                !seen.contains(&got),
                "{what} did not change the fingerprint"
            );
            seen.push(got);
        };

        let f = files();
        f.put(
            "tokens.json",
            &tokens(&[
                ("ci", "write", "t1"),
                ("viewer", "read", "t2"),
                ("bot", "write", "t3"),
            ]),
        );
        check("a token added", f);

        let f = files();
        f.put("tokens.json", &tokens(&[("ci", "write", "t1")]));
        check("a token removed", f);

        let f = files();
        f.put(
            "tokens.json",
            &tokens(&[("ci", "read", "t1"), ("viewer", "read", "t2")]),
        );
        check("a role changed", f);

        let f = files();
        f.put(
            "tokens.json",
            &tokens(&[("ci", "write", "new"), ("viewer", "read", "t2")]),
        );
        check("a token rotated", f);

        let f = files();
        f.put(
            "tokens.json",
            &serde_json::json!({ "tokens": [
                { "name": "ci", "role": "write", "hash": hash_token("t1"), "rate": 5 },
                { "name": "viewer", "role": "read", "hash": hash_token("t2") },
            ] })
            .to_string(),
        );
        check("a token limit added", f);

        let f = files();
        f.put(
            "tokens.json",
            &serde_json::json!({ "tokens": [
                { "name": "ci", "role": "write", "hash": hash_token("t1"), "rate": 6 },
                { "name": "viewer", "role": "read", "hash": hash_token("t2") },
            ] })
            .to_string(),
        );
        check("a token limit changed", f);

        let mut f = files();
        f.sources.limit_defaults.rate = Some(10.0);
        check("a default limit set", f);

        let f = files();
        f.put(
            "tokens.json",
            &serde_json::json!({ "tokens": [
                { "name": "ci", "role": "write", "hash": hash_token("t1"), "expires": "2030-01-01T00:00:00Z" },
                { "name": "viewer", "role": "read", "hash": hash_token("t2") },
            ] })
            .to_string(),
        );
        check("a token expiry set", f);

        let f = files();
        f.put("keys.json", &keys(&[("alice", &key(1)), ("bob", &key(2))]));
        check("a key added", f);

        let f = files();
        f.put("keys.json", &keys(&[("alice", &key(2))]));
        check("a key replaced", f);

        let f = files();
        f.put("keys.json", &keys(&[("alice-laptop", &key(1))]));
        check("a key renamed", f);

        let f = files();
        f.put("policy.json", r#"{ "refs": ["main", "stable"] }"#);
        check("a review ref added", f);

        let mut f = files();
        f.sources.require_signatures.push("stable".into());
        check("a signature ref added", f);

        let mut f = files();
        f.sources.require_review.push("dev".into());
        check("a --require-review ref added", f);

        let mut f = files();
        f.sources.protect_refs.push("stable".into());
        check("a --protect-ref ref added", f);

        let f = files();
        f.put(
            "policy.json",
            r#"{ "refs": ["main"], "auto_approve": ["comment_only"] }"#,
        );
        check("an auto rule added", f);

        let f = files();
        f.put(
            "policy.json",
            r#"{ "refs": ["main"], "auto_approve": ["docs_only"], "docs_globs": ["docs/**"] }"#,
        );
        check("docs rules added", f);

        let mut f = files();
        f.sources.signing_keys_file = None;
        f.sources.require_signatures.clear();
        check("signing turned off", f);
    }

    #[test]
    fn a_reload_without_changes_keeps_the_fingerprint() {
        let f = files();
        let h = f.handle();
        let before = h.current().fingerprint.clone();
        // Rewritten with different formatting but the same meaning.
        f.put("policy.json", "{ \"refs\":\n [\"main\"] }");
        let r = h.reload().unwrap();
        assert_eq!(r.fingerprint, before);
        assert_eq!(r.previous_fingerprint, before);
        assert_eq!(h.current().fingerprint, before);
        assert_eq!(h.current().generation, 2);
    }

    #[test]
    fn a_changing_reload_reports_both_fingerprints() {
        let f = files();
        let h = f.handle();
        let before = h.current().fingerprint.clone();
        f.put("tokens.json", &tokens(&[("ci", "write", "t1")]));
        let r = h.reload().unwrap();
        assert_eq!(r.previous_fingerprint, before);
        assert_ne!(r.fingerprint, before);
        assert_eq!(h.current().fingerprint, r.fingerprint);
    }

    #[test]
    fn a_failed_reload_keeps_the_fingerprint() {
        let f = files();
        let h = f.handle();
        let before = h.current().fingerprint.clone();
        f.put("tokens.json", "{ not json");
        assert!(h.reload().is_err());
        assert_eq!(h.current().fingerprint, before);
    }

    /// Lines of `leep_config_info` in the Prometheus text, as (fingerprint, value).
    fn info_series(text: &str) -> Vec<(String, f64)> {
        text.lines()
            .filter(|l| l.starts_with("leep_config_info{"))
            .map(|l| {
                let fp = l
                    .split("fingerprint=\"")
                    .nth(1)
                    .unwrap()
                    .split('"')
                    .next()
                    .unwrap();
                (
                    fp.to_string(),
                    l.rsplit(' ').next().unwrap().parse().unwrap(),
                )
            })
            .collect()
    }

    #[test]
    fn exactly_one_info_series_is_one_after_a_changing_reload() {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let prom = recorder.handle();
        let f = files();
        let (first, second, third) = metrics::with_local_recorder(&recorder, || {
            let h = f.handle();
            let first = h.current().fingerprint.clone();
            let only_one = |want: &str| {
                let series = info_series(&prom.render());
                let ones: Vec<_> = series.iter().filter(|(_, v)| *v == 1.0).collect();
                assert_eq!(ones.len(), 1, "{series:?}");
                assert_eq!(ones[0].0, want, "{series:?}");
                series.len()
            };
            assert_eq!(only_one(&first), 1);

            // No change: still one series.
            h.reload().unwrap();
            assert_eq!(only_one(&first), 1);

            // A change: the old series drops to 0, the new one is the one at 1.
            f.put("tokens.json", &tokens(&[("ci", "write", "t1")]));
            h.reload().unwrap();
            let second = h.current().fingerprint.clone();
            assert_ne!(first, second);
            assert_eq!(only_one(&second), 2);
            let series = info_series(&prom.render());
            assert!(series.contains(&(first.clone(), 0.0)), "{series:?}");

            // Back to the original: it is 1 again and the other is 0.
            f.put(
                "tokens.json",
                &tokens(&[("ci", "write", "t1"), ("viewer", "read", "t2")]),
            );
            h.reload().unwrap();
            let third = h.current().fingerprint.clone();
            assert_eq!(only_one(&third), 2);
            (first, second, third)
        });
        assert_eq!(first, third);
        assert_ne!(first, second);
        // The generation is a per-process count and moved on every reload.
        assert!(prom.render().contains("leep_config_generation 4"));
    }

    #[test]
    fn expiry_gauges_count_expired_and_soon_to_expire_tokens() {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let prom = recorder.handle();
        let f = files();
        let soon = (jiff::Timestamp::now() + std::time::Duration::from_secs(3600)).to_string();
        f.put(
            "tokens.json",
            &serde_json::json!({ "tokens": [
                { "name": "ci", "role": "write", "hash": hash_token("t1"), "expires": "2020-01-01T00:00:00Z" },
                { "name": "agent", "role": "write", "hash": hash_token("t3"), "expires": soon },
                { "name": "viewer", "role": "read", "hash": hash_token("t2"), "expires": "2999-01-01T00:00:00Z" },
            ] })
            .to_string(),
        );
        metrics::with_local_recorder(&recorder, || {
            let h = f.handle();
            let text = prom.render();
            assert!(text.contains("leep_tokens_expired 1"), "{text}");
            assert!(text.contains("leep_tokens_expiring_soon 1"), "{text}");
            // A reload that renews the token clears it.
            f.put("tokens.json", &tokens(&[("ci", "write", "t1")]));
            h.reload().unwrap();
            let text = prom.render();
            assert!(text.contains("leep_tokens_expired 0"), "{text}");
            assert!(text.contains("leep_tokens_expiring_soon 0"), "{text}");
        });
    }

    #[test]
    fn a_fixed_handle_does_not_reload() {
        let h = ConfigHandle::fixed(RuntimeConfig::fixed(None, None, None, None, None));
        assert!(!h.reloadable());
        assert!(h.reload().is_err());
    }

    #[test]
    fn a_name_only_key_change_still_takes_effect() {
        let f = files();
        let h = f.handle();
        f.put("keys.json", &keys(&[("alice-laptop", &key(1))]));
        let r = h.reload().unwrap();
        assert!(r.keys_added.is_empty() && r.keys_removed.is_empty());
        let c = h.current();
        let trusted = c.signing.as_ref().unwrap();
        let pk = leep_core::signature::parse_public_key(&key(1)).unwrap();
        assert_eq!(trusted.find(&pk).unwrap().name, "alice-laptop");
    }
}
