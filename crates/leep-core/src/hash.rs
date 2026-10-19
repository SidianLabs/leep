use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::Error;

/// A BLAKE3 digest. Serialized as lowercase hex in human-readable formats
/// (JSON) and as 32 raw bytes in binary formats (postcard).
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Hash(pub [u8; 32]);

impl Hash {
    /// Hash of a file's raw bytes. This is the address of a blob.
    pub fn of_blob(data: &[u8]) -> Hash {
        Hash(*blake3::hash(data).as_bytes())
    }

    /// Hash of an encoded object, domain-separated by `kind` from other
    /// object kinds. It is NOT separated from blobs: `of_blob` is bare BLAKE3,
    /// so the blob whose bytes are `"leep:" + kind + "\0" + encoded` has the
    /// same address. Writers of uploaded content must refuse such bytes; see
    /// [`is_object_preimage`] and ADR 0041.
    pub fn of_object(kind: &str, encoded: &[u8]) -> Hash {
        let mut h = blake3::Hasher::new();
        h.update(OBJECT_DOMAIN);
        h.update(kind.as_bytes());
        h.update(&[0]);
        h.update(encoded);
        Hash(*h.finalize().as_bytes())
    }

    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// Whether `data`, stored as a blob, would take the address of some
/// [`Hash::of_object`] value. Every such preimage starts with `leep:` and has
/// a NUL ending the kind, so text files that merely start with `leep:` are
/// still accepted. Uploaded blobs that match are rejected until blob and
/// object addresses are separated (ADR 0041).
pub fn is_object_preimage(data: &[u8]) -> bool {
    data.strip_prefix(OBJECT_DOMAIN)
        .is_some_and(|rest| rest.contains(&0))
}

/// The prefix [`Hash::of_object`] hashes before the kind.
pub const OBJECT_DOMAIN: &[u8] = b"leep:";

impl fmt::Display for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl fmt::Debug for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Hash({})", &self.to_hex()[..12])
    }
}

impl FromStr for Hash {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Error> {
        let bytes = hex::decode(s).map_err(|_| Error::InvalidHash(s.to_string()))?;
        let arr: [u8; 32] = bytes
            .try_into()
            .map_err(|_| Error::InvalidHash(s.to_string()))?;
        Ok(Hash(arr))
    }
}

impl Serialize for Hash {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        if s.is_human_readable() {
            // On the stack: a log or listing encodes thousands of hashes.
            let mut buf = [0u8; 64];
            hex::encode_to_slice(self.0, &mut buf).expect("64 hex digits for 32 bytes");
            s.serialize_str(std::str::from_utf8(&buf).expect("hex is ASCII"))
        } else {
            self.0.serialize(s)
        }
    }
}

impl<'de> Deserialize<'de> for Hash {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        if d.is_human_readable() {
            let s = String::deserialize(d)?;
            s.parse().map_err(serde::de::Error::custom)
        } else {
            Ok(Hash(<[u8; 32]>::deserialize(d)?))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_roundtrip_and_domain_separation() {
        let h = Hash::of_blob(b"hello");
        assert_eq!(h.to_hex().parse::<Hash>().unwrap(), h);
        assert_ne!(Hash::of_object("tree", b"hello"), h);
        let json = serde_json::to_string(&h).unwrap();
        assert_eq!(json, format!("\"{}\"", h.to_hex()));
        let bin = postcard::to_stdvec(&h).unwrap();
        assert_eq!(postcard::from_bytes::<Hash>(&bin).unwrap(), h);
    }

    #[test]
    fn object_preimages_are_recognised() {
        // The collision this guards against: a blob with these bytes has the
        // tree's address.
        let mut preimage = b"leep:tree-v1\0".to_vec();
        preimage.extend_from_slice(b"node");
        assert_eq!(
            Hash::of_blob(&preimage),
            Hash::of_object("tree-v1", b"node")
        );
        assert!(is_object_preimage(&preimage));
        assert!(is_object_preimage(b"leep:\0"));
        assert!(is_object_preimage(b"leep:any kind at all\0"));
        // Ordinary files, including ones that start with the prefix.
        assert!(!is_object_preimage(b"leep: true\n"));
        assert!(!is_object_preimage(b"leep:"));
        assert!(!is_object_preimage(b"\0leep:tree-v1\0"));
        assert!(!is_object_preimage(b""));
    }
}
