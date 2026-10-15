//! Signed changesets (ADR 0010).
//!
//! A changeset's id hashes its whole encoding, so a signature cannot live
//! inside the changeset it signs. Signatures are attestations stored beside
//! the changeset, keyed by its id. This module holds only what the server and
//! the client must agree on: the signed bytes, key ids and the stored type.
//! The Ed25519 operations themselves live in `leep-server` and `leep-cli`.

use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{ChangesetId, Error, Result};

/// Prefix of the signed message; the trailing NUL separates it from the id.
/// Domain separation keeps a Leep signature from being valid for any other
/// protocol that signs 32 bytes with the same key.
pub const SIGNING_DOMAIN: &[u8] = b"leep-changeset-signature-v1\0";

/// Textual prefix of a public key (`ed25519:<64 hex>`).
pub const PUBLIC_KEY_PREFIX: &str = "ed25519:";

/// Derived-data kind holding one stored [`Signature`] per (changeset, key).
pub const SIGNATURE_KIND: &str = "signature-v1";

/// Error text shared by the server's policy and the CLI's hint.
pub const NOT_SIGNED_MARKER: &str = "is not signed by a trusted key";

/// The exact bytes a signer signs for `id`.
pub fn signing_message(id: &ChangesetId) -> Vec<u8> {
    let mut m = Vec::with_capacity(SIGNING_DOMAIN.len() + 32);
    m.extend_from_slice(SIGNING_DOMAIN);
    m.extend_from_slice(id.0.as_bytes());
    m
}

/// First 16 hex characters of `BLAKE3(public key)`: short enough to read in a
/// log line, and only a label (verification always uses the full key).
pub fn key_id(public_key: &[u8; 32]) -> String {
    blake3::hash(public_key).to_hex()[..16].to_string()
}

/// `ed25519:<hex>`.
pub fn format_public_key(public_key: &[u8; 32]) -> String {
    format!("{PUBLIC_KEY_PREFIX}{}", hex::encode(public_key))
}

/// Parse `ed25519:<64 hex>`. This checks the shape only; whether the bytes are
/// a valid curve point is decided where the key is used.
pub fn parse_public_key(s: &str) -> Result<[u8; 32]> {
    let h = s.strip_prefix(PUBLIC_KEY_PREFIX).ok_or_else(|| {
        Error::Invalid(format!(
            "public key must be `{PUBLIC_KEY_PREFIX}<64 hex digits>`"
        ))
    })?;
    let mut out = [0u8; 32];
    if h.len() != 64 || hex::decode_to_slice(h, &mut out).is_err() {
        return Err(Error::Invalid(format!(
            "public key must be `{PUBLIC_KEY_PREFIX}` followed by 64 hex digits"
        )));
    }
    Ok(out)
}

/// Parse a hex-encoded 64-byte signature.
pub fn parse_signature_hex(s: &str) -> Result<[u8; 64]> {
    let mut out = [0u8; 64];
    if s.len() != 128 || hex::decode_to_slice(s, &mut out).is_err() {
        return Err(Error::Invalid(
            "signature must be 128 hex digits (64 bytes)".into(),
        ));
    }
    Ok(out)
}

/// One stored attestation. Persisted with postcard (field order is part of
/// the format, see FORMATS.md).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Signature {
    pub key_id: String,
    pub public_key: [u8; 32],
    #[serde(with = "bytes64")]
    pub sig: [u8; 64],
}

impl Signature {
    pub fn encode(&self) -> Vec<u8> {
        postcard::to_stdvec(self).expect("signature encoding cannot fail")
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

/// serde implements arrays only up to 32 elements, so the 64-byte signature
/// goes through the byte-string path.
mod bytes64 {
    use super::*;

    pub fn serialize<S: Serializer>(v: &[u8; 64], s: S) -> std::result::Result<S::Ok, S::Error> {
        s.serialize_bytes(v)
    }

    struct V;

    impl<'de> Visitor<'de> for V {
        type Value = [u8; 64];

        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("64 bytes")
        }

        fn visit_bytes<E: de::Error>(self, v: &[u8]) -> std::result::Result<Self::Value, E> {
            v.try_into()
                .map_err(|_| E::invalid_length(v.len(), &"64 bytes"))
        }

        fn visit_seq<A: de::SeqAccess<'de>>(
            self,
            mut seq: A,
        ) -> std::result::Result<Self::Value, A::Error> {
            let mut out = [0u8; 64];
            for (i, b) in out.iter_mut().enumerate() {
                *b = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(i, &"64 bytes"))?;
            }
            Ok(out)
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> std::result::Result<[u8; 64], D::Error> {
        d.deserialize_bytes(V)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Hash;

    #[test]
    fn message_is_domain_then_id_golden() {
        let id = ChangesetId(Hash([0xab; 32]));
        assert_eq!(
            hex::encode(signing_message(&id)),
            format!(
                "6c6565702d6368616e67657365742d7369676e61747572652d763100{}",
                "ab".repeat(32)
            )
        );
    }

    #[test]
    fn key_id_is_16_hex_of_blake3() {
        let pk = [7u8; 32];
        let id = key_id(&pk);
        assert_eq!(id.len(), 16);
        assert_eq!(id, blake3::hash(&pk).to_hex()[..16]);
        assert_ne!(id, key_id(&[8u8; 32]));
        // Golden: changing the derivation would relabel every trusted key.
        assert_eq!(id, "ebaf85b465a09de2");
    }

    #[test]
    fn public_key_text_round_trip_and_rejects() {
        let pk = [0x5a; 32];
        let s = format_public_key(&pk);
        assert_eq!(s, format!("ed25519:{}", "5a".repeat(32)));
        assert_eq!(parse_public_key(&s).unwrap(), pk);
        assert!(parse_public_key(&"5a".repeat(32)).is_err());
        assert!(parse_public_key("ed25519:abcd").is_err());
        assert!(parse_public_key(&format!("ed25519:{}", "zz".repeat(32))).is_err());
    }

    #[test]
    fn signature_hex_lengths() {
        assert!(parse_signature_hex(&"00".repeat(64)).is_ok());
        assert!(parse_signature_hex(&"00".repeat(63)).is_err());
        assert!(parse_signature_hex(&"gg".repeat(64)).is_err());
    }

    #[test]
    fn stored_encoding_round_trips() {
        let s = Signature {
            key_id: key_id(&[1; 32]),
            public_key: [1; 32],
            sig: std::array::from_fn(|i| i as u8),
        };
        let bytes = s.encode();
        // len-prefixed key id (1 + 16), raw key (32), len-prefixed sig (1 + 64)
        assert_eq!(bytes.len(), 17 + 32 + 65);
        assert_eq!(Signature::decode(&bytes).unwrap(), s);
        assert!(Signature::decode(&bytes[..bytes.len() - 1]).is_err());
    }
}
