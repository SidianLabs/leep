//! Author keys: generation, loading and signing (ADR 0010).
//!
//! The private key file is one line, `leep-ed25519-secret:<64 hex>`, the
//! Ed25519 seed. It is created with mode 0600 and refused on load if other
//! users can read it, like an SSH key.

use std::fs;
use std::io::Write;
use std::path::Path;

use ed25519_dalek::{Signer, SigningKey};
use leep_core::api::AddSignatureRequest;
use leep_core::signature::{format_public_key, key_id, signing_message};
use leep_core::ChangesetId;

use crate::error::{CliError, Result};

const SECRET_PREFIX: &str = "leep-ed25519-secret:";

/// A loaded author key.
pub struct AuthorKey(SigningKey);

impl AuthorKey {
    /// A fresh key from system randomness.
    pub fn generate() -> Result<AuthorKey> {
        let mut seed = [0u8; 32];
        getrandom::fill(&mut seed)
            .map_err(|e| CliError::user(format!("no system randomness: {e}")))?;
        Ok(AuthorKey(SigningKey::from_bytes(&seed)))
    }

    /// `ed25519:<hex>`, the form the server's keys file lists.
    pub fn public_key(&self) -> String {
        format_public_key(self.0.verifying_key().as_bytes())
    }

    pub fn key_id(&self) -> String {
        key_id(self.0.verifying_key().as_bytes())
    }

    /// Write the key to `path` with mode 0600, refusing to overwrite.
    pub fn save(&self, path: &Path) -> Result<()> {
        let mut opts = fs::OpenOptions::new();
        opts.write(true).create_new(true);
        #[cfg(unix)]
        std::os::unix::fs::OpenOptionsExt::mode(&mut opts, 0o600);
        let mut f = opts.open(path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::AlreadyExists {
                CliError::user(format!(
                    "{} already exists; refusing to overwrite a key",
                    path.display()
                ))
            } else {
                CliError::user(format!("cannot create {}: {e}", path.display()))
            }
        })?;
        writeln!(f, "{SECRET_PREFIX}{}", hex_seed(&self.0))?;
        Ok(())
    }

    pub fn load(path: &Path) -> Result<AuthorKey> {
        let bad = |why: &str| CliError::user(format!("signing key {}: {why}", path.display()));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = fs::metadata(path)
                .map_err(|e| bad(&format!("cannot read: {e}")))?
                .permissions()
                .mode();
            if mode & 0o077 != 0 {
                return Err(bad(&format!(
                    "permissions {:o} are too open; run `chmod 600 {}`",
                    mode & 0o777,
                    path.display()
                )));
            }
        }
        let text = fs::read_to_string(path).map_err(|e| bad(&format!("cannot read: {e}")))?;
        let h = text
            .trim()
            .strip_prefix(SECRET_PREFIX)
            .ok_or_else(|| bad(&format!("expected `{SECRET_PREFIX}<64 hex>`")))?;
        let mut seed = [0u8; 32];
        if h.len() != 64 || hex::decode_to_slice(h, &mut seed).is_err() {
            return Err(bad("expected 64 hex digits after the prefix"));
        }
        Ok(AuthorKey(SigningKey::from_bytes(&seed)))
    }

    /// The request that attests to `id`.
    pub fn sign(&self, id: &ChangesetId) -> AddSignatureRequest {
        AddSignatureRequest {
            public_key: self.public_key(),
            signature: hex::encode(self.0.sign(&signing_message(id)).to_bytes()),
        }
    }
}

fn hex_seed(k: &SigningKey) -> String {
    hex::encode(k.to_bytes())
}

// Every test needs Unix file modes.
#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use ed25519_dalek::{Signature, VerifyingKey};
    use leep_core::signature::{parse_public_key, parse_signature_hex};
    use leep_core::Hash;

    #[cfg(unix)]
    #[test]
    fn saved_key_is_0600_and_round_trips_through_sign_and_verify() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("author.key");
        let key = AuthorKey::generate().unwrap();
        key.save(&path).unwrap();
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );

        let loaded = AuthorKey::load(&path).unwrap();
        assert_eq!(loaded.public_key(), key.public_key());
        assert_eq!(loaded.key_id().len(), 16);

        let id = ChangesetId(Hash::of_blob(b"cs"));
        let req = loaded.sign(&id);
        let vk = VerifyingKey::from_bytes(&parse_public_key(&req.public_key).unwrap()).unwrap();
        let sig = Signature::from_bytes(&parse_signature_hex(&req.signature).unwrap());
        vk.verify_strict(&signing_message(&id), &sig).unwrap();
        // Not valid for another changeset.
        let other = ChangesetId(Hash::of_blob(b"other"));
        assert!(vk.verify_strict(&signing_message(&other), &sig).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn refuses_overwrite_and_open_permissions_and_garbage() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("k");
        AuthorKey::generate().unwrap().save(&path).unwrap();
        let e = AuthorKey::generate().unwrap().save(&path).unwrap_err();
        assert!(e.to_string().contains("refusing to overwrite"), "{e}");

        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        let e = AuthorKey::load(&path).err().unwrap();
        assert!(e.to_string().contains("too open"), "{e}");

        fs::write(&path, "hello\n").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        let e = AuthorKey::load(&path).err().unwrap();
        assert!(e.to_string().contains("expected"), "{e}");
        assert!(AuthorKey::load(&dir.path().join("missing")).is_err());
    }
}
