//! Trusted signing keys and the `--require-signatures` land policy (ADR 0010).
//!
//! The keys file is read at startup and again on SIGHUP (ADR 0024). It is as
//! strict as the tokens file: unknown fields, malformed keys, duplicates and an
//! empty list all fail the load, and a failed reload keeps the old keys.
//!
//! ```json
//! { "keys": [
//!   { "name": "alice", "public_key": "ed25519:<64 hex>" }
//! ] }
//! ```

use std::collections::HashSet;
use std::path::Path;

use async_trait::async_trait;
use ed25519_dalek::{Signature as DalekSignature, VerifyingKey};
use leep_core::api::{SignatureInfo, SignaturesResponse};
use leep_core::signature::{
    format_public_key, key_id, signing_message, Signature, NOT_SIGNED_MARKER,
};
use leep_core::{ChangesetId, Error, Result};
use leep_derive::{LandPolicy, Repo};
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct KeyEntry {
    name: String,
    public_key: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct KeyFile {
    keys: Vec<KeyEntry>,
}

/// One trusted author key.
pub struct TrustedKey {
    pub name: String,
    pub key_id: String,
    pub public_key: [u8; 32],
    verifying: VerifyingKey,
}

/// The loaded set of trusted keys.
pub struct SigningKeys {
    keys: Vec<TrustedKey>,
}

impl SigningKeys {
    pub fn parse(text: &str) -> std::result::Result<SigningKeys, String> {
        let file: KeyFile = serde_json::from_str(text).map_err(|e| format!("bad JSON: {e}"))?;
        if file.keys.is_empty() {
            return Err("no keys listed; omit --signing-keys-file to disable signatures".into());
        }
        let (mut names, mut ids) = (HashSet::new(), HashSet::new());
        let mut keys = Vec::new();
        for (i, e) in file.keys.into_iter().enumerate() {
            if e.name.trim().is_empty() {
                return Err(format!("key #{i}: empty name"));
            }
            if !names.insert(e.name.clone()) {
                return Err(format!("duplicate key name {:?}", e.name));
            }
            let public_key =
                leep_core::signature::parse_public_key(&e.public_key).map_err(|err| {
                    format!(
                        "key {:?}: {}",
                        e.name,
                        err.to_string().trim_start_matches("invalid input: ")
                    )
                })?;
            let verifying = VerifyingKey::from_bytes(&public_key)
                .map_err(|_| format!("key {:?}: not a valid Ed25519 public key", e.name))?;
            if verifying.is_weak() {
                return Err(format!("key {:?}: weak (small-order) public key", e.name));
            }
            let id = key_id(&public_key);
            if !ids.insert(id.clone()) {
                return Err(format!(
                    "key {:?}: duplicate key (id {id}); each key may be listed once",
                    e.name
                ));
            }
            keys.push(TrustedKey {
                name: e.name,
                key_id: id,
                public_key,
                verifying,
            });
        }
        Ok(SigningKeys { keys })
    }

    pub fn from_file(path: &Path) -> std::result::Result<SigningKeys, String> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| format!("cannot read signing keys file {}: {e}", path.display()))?;
        SigningKeys::parse(&text).map_err(|e| format!("signing keys file {}: {e}", path.display()))
    }

    pub fn len(&self) -> usize {
        self.keys.len()
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// Feed the keys into a config fingerprint, sorted by key id. Public
    /// keys and their names only.
    pub(crate) fn fingerprint_into(&self, c: &mut crate::fingerprint::Canon) {
        let mut all: Vec<&TrustedKey> = self.keys.iter().collect();
        all.sort_by(|a, b| a.key_id.cmp(&b.key_id));
        c.section("keys", all.len());
        for k in all {
            c.str(&k.key_id).str(&k.name);
        }
    }

    /// Names of the keys `newer` adds and drops, by key id, sorted. A key
    /// that only changed its name is neither.
    pub fn diff(&self, newer: &SigningKeys) -> (Vec<String>, Vec<String>) {
        let missing_from = |a: &SigningKeys, b: &SigningKeys| {
            let mut names: Vec<_> = b
                .keys
                .iter()
                .filter(|k| !a.keys.iter().any(|o| o.key_id == k.key_id))
                .map(|k| k.name.clone())
                .collect();
            names.sort();
            names
        };
        (missing_from(self, newer), missing_from(newer, self))
    }

    pub fn find(&self, public_key: &[u8; 32]) -> Option<&TrustedKey> {
        self.keys.iter().find(|k| &k.public_key == public_key)
    }

    /// Check `sig` over `id` against `key` (strict: rejects malleable forms).
    pub fn verify(key: &TrustedKey, id: &ChangesetId, sig: &[u8; 64]) -> Result<()> {
        key.verifying
            .verify_strict(&signing_message(id), &DalekSignature::from_bytes(sig))
            .map_err(|_| Error::Invalid("signature does not verify".into()))
    }

    /// Stored signatures of `id` that verify, one lookup per trusted key.
    /// The flag is false when a stored row failed to decode or verify.
    pub async fn verified_for(
        &self,
        repo: &Repo,
        id: ChangesetId,
    ) -> Result<(Vec<&TrustedKey>, bool)> {
        let (mut good, mut all_ok) = (Vec::new(), true);
        for k in &self.keys {
            let Some(stored) = repo
                .get_signature(id, &k.key_id)
                .await
                .or_else(|e| match e {
                    Error::Encoding(_) => {
                        all_ok = false;
                        Ok(None)
                    }
                    e => Err(e),
                })?
            else {
                continue;
            };
            if stored.public_key == k.public_key && Self::verify(k, &id, &stored.sig).is_ok() {
                good.push(k);
            } else {
                all_ok = false;
            }
        }
        Ok((good, all_ok))
    }

    /// The `GET .../signatures` answer. A changeset with none of its own but
    /// recorded as a server-made rebase of one that has some reports those,
    /// with `via` naming the original.
    pub async fn describe(&self, repo: &Repo, id: ChangesetId) -> Result<SignaturesResponse> {
        let (own, verified) = self.verified_for(repo, id).await?;
        if !own.is_empty() {
            return Ok(response(own, verified, None));
        }
        let mut verified = verified;
        for origin in repo.rebase_chain(id).await? {
            let (found, ok) = self.verified_for(repo, origin).await?;
            verified &= ok;
            if !found.is_empty() {
                return Ok(response(found, verified, Some(origin)));
            }
        }
        Ok(response(Vec::new(), verified, None))
    }

    /// Whether `id` itself carries a valid trusted signature.
    pub async fn is_signed(&self, repo: &Repo, id: ChangesetId) -> Result<bool> {
        Ok(!self.verified_for(repo, id).await?.0.is_empty())
    }
}

fn response(
    keys: Vec<&TrustedKey>,
    verified: bool,
    via: Option<ChangesetId>,
) -> SignaturesResponse {
    SignaturesResponse {
        signatures: keys
            .into_iter()
            .map(|k| SignatureInfo {
                key_id: k.key_id.clone(),
                name: k.name.clone(),
                public_key: format_public_key(&k.public_key),
            })
            .collect(),
        verified,
        via,
    }
}

/// Convert a verified request into the stored form.
pub fn stored(key: &TrustedKey, sig: [u8; 64]) -> Signature {
    Signature {
        key_id: key.key_id.clone(),
        public_key: key.public_key,
        sig,
    }
}

/// `land` onto any of `refs` needs every changeset it adds to be signed by a
/// trusted key.
pub struct RequireSignatures {
    keys: std::sync::Arc<SigningKeys>,
    refs: HashSet<String>,
}

impl RequireSignatures {
    pub fn new(keys: std::sync::Arc<SigningKeys>, refs: impl IntoIterator<Item = String>) -> Self {
        RequireSignatures {
            keys,
            refs: refs.into_iter().collect(),
        }
    }
}

#[async_trait]
impl LandPolicy for RequireSignatures {
    fn applies_to(&self, target_ref: &str) -> bool {
        self.refs.contains(target_ref)
    }

    async fn check_land(
        &self,
        repo: &Repo,
        target_ref: &str,
        _head: Option<ChangesetId>,
        new_history: &[ChangesetId],
    ) -> Result<()> {
        for id in new_history {
            if !self.keys.is_signed(repo, *id).await? {
                tracing::info!(%id, target_ref, "land refused: unsigned changeset");
                return Err(Error::Invalid(format!(
                    "changeset {id} {NOT_SIGNED_MARKER}"
                )));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(seed: u8) -> String {
        let sk = ed25519_dalek::SigningKey::from_bytes(&[seed; 32]);
        format_public_key(sk.verifying_key().as_bytes())
    }

    fn file(entries: &[(&str, &str)]) -> String {
        let keys: Vec<_> = entries
            .iter()
            .map(|(n, k)| serde_json::json!({ "name": n, "public_key": k }))
            .collect();
        serde_json::json!({ "keys": keys }).to_string()
    }

    fn err(text: &str) -> String {
        SigningKeys::parse(text).err().expect("should be rejected")
    }

    #[test]
    fn diff_goes_by_key_and_ignores_renames() {
        let old = SigningKeys::parse(&file(&[("alice", &key(1)), ("bob", &key(2))])).unwrap();
        let new =
            SigningKeys::parse(&file(&[("alice-laptop", &key(1)), ("carol", &key(3))])).unwrap();
        let (added, removed) = old.diff(&new);
        assert_eq!(added, ["carol"]);
        assert_eq!(removed, ["bob"]);
        assert_eq!(old.diff(&old), (vec![], vec![]));
    }

    #[test]
    fn parses_a_valid_file() {
        let k = SigningKeys::parse(&file(&[("alice", &key(1)), ("bob", &key(2))])).unwrap();
        assert_eq!(k.len(), 2);
        let pk = *ed25519_dalek::SigningKey::from_bytes(&[2; 32])
            .verifying_key()
            .as_bytes();
        assert_eq!(k.find(&pk).unwrap().name, "bob");
        assert!(k.find(&[0; 32]).is_none());
    }

    #[test]
    fn rejects_bad_files() {
        assert!(err("not json").contains("bad JSON"));
        assert!(err(r#"{"keys":[]}"#).contains("no keys"));
        assert!(err(r#"{"keys":[],"extra":1}"#).contains("unknown field"));
        assert!(
            err(r#"{"keys":[{"name":"a","public_key":"x","role":"w"}]}"#).contains("unknown field")
        );
        assert!(err(&file(&[(" ", &key(1))])).contains("empty name"));
        assert!(err(&file(&[("a", &key(1)), ("a", &key(2))])).contains("duplicate key name"));
        assert!(err(&file(&[("a", &key(1)), ("b", &key(1))])).contains("duplicate key"));
        let e = err(&file(&[("a", "ed25519:abcd")]));
        assert!(e.contains("\"a\"") && e.contains("64 hex"), "{e}");
        let e = err(&file(&[("a", &"5a".repeat(32))]));
        assert!(e.contains("ed25519:"), "{e}");
    }

    #[test]
    fn rejects_points_that_are_not_valid_keys() {
        // y = 2 has no matching x on the curve.
        let mut bad = [0u8; 32];
        bad[0] = 2;
        let e = err(&file(&[("a", &format_public_key(&bad))]));
        assert!(e.contains("not a valid Ed25519"), "{e}");
        // The identity point is small-order.
        let mut id = [0u8; 32];
        id[0] = 1;
        let e = err(&file(&[("a", &format_public_key(&id))]));
        assert!(e.contains("weak"), "{e}");
    }

    #[test]
    fn missing_file_names_the_path() {
        let e = SigningKeys::from_file(Path::new("/nonexistent/keys.json"))
            .err()
            .unwrap();
        assert!(e.contains("/nonexistent/keys.json"), "{e}");
    }
}
