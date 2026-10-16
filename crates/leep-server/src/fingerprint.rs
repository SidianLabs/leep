//! A content fingerprint of the loaded config, exported as
//! `leep_config_info{fingerprint}`.
//!
//! `leep_config_generation` counts loads in one process, so it differs between
//! servers that run identical config (one restarted, one reloaded). The
//! fingerprint depends only on what the config *is*, so servers that agree
//! have the same value and a skew alert can compare them.
//!
//! The input is the parsed, normalized config, not the file bytes: whitespace,
//! key order and entry order that carry no meaning do not change it. It holds
//! no secret. Token entries are already `blake3:` digests (never plaintext),
//! trusted keys are public keys, and everything else is names, numbers and
//! ref names. Only the first 8 bytes of the BLAKE3 output are exported, so the
//! value reveals nothing about the inputs beyond whether they changed.

use crate::limits::Quota;

/// Bump when the encoding changes, so a new release never silently matches an
/// old one that computed the same bytes differently.
const DOMAIN: &str = "leep-config-fingerprint-v1";

/// Length-prefixed, tagged encoding, so no two different configs can produce
/// the same byte stream by shifting data between fields.
pub(crate) struct Canon {
    hasher: blake3::Hasher,
}

impl Canon {
    pub(crate) fn new() -> Canon {
        let mut c = Canon {
            hasher: blake3::Hasher::new(),
        };
        c.str(DOMAIN);
        c
    }

    pub(crate) fn u64(&mut self, n: u64) -> &mut Self {
        self.hasher.update(&n.to_le_bytes());
        self
    }

    pub(crate) fn bytes(&mut self, b: &[u8]) -> &mut Self {
        self.u64(b.len() as u64);
        self.hasher.update(b);
        self
    }

    pub(crate) fn str(&mut self, s: &str) -> &mut Self {
        self.bytes(s.as_bytes())
    }

    /// A section header: its name and how many entries follow.
    pub(crate) fn section(&mut self, name: &str, count: usize) -> &mut Self {
        self.str(name).u64(count as u64)
    }

    /// A set of strings in sorted order, without duplicates.
    pub(crate) fn set<'a>(
        &mut self,
        name: &str,
        items: impl IntoIterator<Item = &'a String>,
    ) -> &mut Self {
        let mut v: Vec<&str> = items.into_iter().map(String::as_str).collect();
        v.sort_unstable();
        v.dedup();
        self.section(name, v.len());
        for s in v {
            self.str(s);
        }
        self
    }

    pub(crate) fn opt_f64(&mut self, v: Option<f64>) -> &mut Self {
        match v {
            None => self.u64(0),
            // Bits, not text: exact and independent of float formatting.
            Some(x) => self.u64(1).u64(x.to_bits()),
        }
    }

    pub(crate) fn opt_quota(&mut self, v: Option<Quota>) -> &mut Self {
        match v {
            None => self.u64(0),
            Some(Quota::Unlimited) => self.u64(1),
            Some(Quota::Limit(x)) => self.u64(2).u64(x.to_bits()),
        }
    }

    pub(crate) fn opt_u64(&mut self, v: Option<u64>) -> &mut Self {
        match v {
            None => self.u64(0),
            Some(x) => self.u64(1).u64(x),
        }
    }

    /// 16 lowercase hex characters.
    pub(crate) fn finish(&self) -> String {
        hex::encode(&self.hasher.finalize().as_bytes()[..8])
    }
}
