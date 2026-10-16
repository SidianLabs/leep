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
use crate::limits::{Defaults, Limiter, LimiterChange};
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
}

/// One consistent configuration. Never mutated after it is built.
pub struct RuntimeConfig {
    /// 1 at startup, plus one per successful reload.
    pub generation: u64,
    pub auth: Option<Auth>,
    pub limiter: Option<Arc<Limiter>>,
    pub signing: Option<Arc<SigningKeys>>,
    pub review: Option<Arc<ReviewService>>,
    /// Signatures and review composed, so the forward-only and protected
    /// delete rules and `land` all read the same refs.
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
        RuntimeConfig {
            generation: 1,
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
        let land_policy: Option<Arc<dyn LandPolicy>> = match policies.len() {
            0 => None,
            1 => policies.pop(),
            _ => Some(Arc::new(PolicySet::new(policies))),
        };

        Ok(Loaded {
            config: RuntimeConfig {
                generation: prev.map_or(1, |p| p.generation + 1),
                auth,
                limiter: limiter.map(Arc::new),
                signing,
                review,
                land_policy,
                protected_refs: sorted_union(&self.require_signatures, &review_refs),
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

fn record_loaded(generation: u64) {
    metrics::gauge!("leep_config_generation").set(generation as f64);
    metrics::gauge!("leep_config_last_reload_timestamp_seconds").set(now_secs());
}

impl ConfigHandle {
    /// Load the config for startup. The same checks as a reload, so a file
    /// that would fail a reload fails startup.
    pub fn load(sources: ReloadSources) -> Result<Arc<ConfigHandle>, String> {
        // Registered at zero so an alert on errors sees the series from the start.
        metrics::counter!("leep_config_reload_total", "result" => "ok").increment(0);
        metrics::counter!("leep_config_reload_total", "result" => "error").increment(0);
        let loaded = sources.load(None)?;
        record_loaded(loaded.config.generation);
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
                record_loaded(loaded.config.generation);
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
    fn an_unchanged_reload_reports_nothing() {
        let f = files();
        let h = f.handle();
        let r = h.reload().unwrap();
        assert!(r.is_empty(), "{r:?}");
        assert_eq!(r.generation, 2);
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
