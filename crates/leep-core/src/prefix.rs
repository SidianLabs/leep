//! Short changeset ids: a hex prefix of a full id, resolved by a range scan
//! over the ids' bytes (ADR 0035).

use std::fmt;

use crate::{ChangesetId, Error, Hash, Result};

/// Fewest hex digits a short id may have. Shorter prefixes match too much of
/// any real history to be worth a lookup.
pub const MIN_PREFIX_LEN: usize = 4;

/// A validated prefix of a changeset id: 4 to 64 hex digits, any case.
#[derive(Clone, PartialEq, Eq)]
pub struct IdPrefix {
    /// Lowercase.
    text: String,
    /// Smallest and largest ids that start with the prefix.
    lower: [u8; 32],
    upper: [u8; 32],
}

impl IdPrefix {
    pub fn parse(s: &str) -> Result<IdPrefix> {
        if !s.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(Error::Invalid(format!("short id {s:?} is not hexadecimal")));
        }
        if s.len() < MIN_PREFIX_LEN {
            return Err(Error::Invalid(format!(
                "short id {s:?} is too short: give at least {MIN_PREFIX_LEN} hex digits"
            )));
        }
        if s.len() > 64 {
            return Err(Error::Invalid(format!(
                "short id {s:?} is longer than a changeset id (64 hex digits)"
            )));
        }
        let text = s.to_ascii_lowercase();
        let (mut lower, mut upper) = ([0u8; 32], [0xffu8; 32]);
        for (i, c) in text.bytes().enumerate() {
            let nibble = (c as char).to_digit(16).expect("checked hex") as u8;
            let (byte, shift) = (i / 2, if i % 2 == 0 { 4 } else { 0 });
            let mask = 0x0f << shift;
            lower[byte] = (lower[byte] & !mask) | (nibble << shift);
            upper[byte] = (upper[byte] & !mask) | (nibble << shift);
        }
        Ok(IdPrefix { text, lower, upper })
    }

    pub fn as_str(&self) -> &str {
        &self.text
    }

    /// The smallest id with this prefix (the prefix padded with zeros).
    pub fn lower(&self) -> ChangesetId {
        ChangesetId(Hash(self.lower))
    }

    /// The largest id with this prefix (the prefix padded with `f`s).
    pub fn upper(&self) -> ChangesetId {
        ChangesetId(Hash(self.upper))
    }

    pub fn matches(&self, id: &ChangesetId) -> bool {
        let b = &id.0 .0;
        *b >= self.lower && *b <= self.upper
    }

    /// The id just below [`lower`](Self::lower), for an exclusive cursor such
    /// as [`crate::MetaStore::list_changesets`]'s `after`; `None` when the
    /// range starts at the very first id.
    pub fn before_lower(&self) -> Option<ChangesetId> {
        let mut b = self.lower;
        for byte in b.iter_mut().rev() {
            if *byte > 0 {
                *byte -= 1;
                return Some(ChangesetId(Hash(b)));
            }
            *byte = 0xff;
        }
        None
    }
}

impl fmt::Display for IdPrefix {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.text)
    }
}

impl fmt::Debug for IdPrefix {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IdPrefix({})", self.text)
    }
}

/// Whether `s` could be a short id: hex of a length a prefix may have. A ref
/// with such a name still wins where both are accepted.
pub fn looks_like_prefix(s: &str) -> bool {
    (MIN_PREFIX_LEN..=64).contains(&s.len()) && s.bytes().all(|b| b.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(hex: &str) -> ChangesetId {
        hex.parse().unwrap()
    }

    #[test]
    fn bounds_cover_exactly_the_ids_with_the_prefix() {
        let p = IdPrefix::parse("aBc12").unwrap();
        assert_eq!(p.as_str(), "abc12");
        assert_eq!(p.lower(), id(&format!("abc120{}", "0".repeat(58))));
        assert_eq!(p.upper(), id(&format!("abc12f{}", "f".repeat(58))));
        assert!(p.matches(&id(&format!("abc12{}", "7".repeat(59)))));
        assert!(!p.matches(&id(&format!("abc13{}", "0".repeat(59)))));
        assert!(!p.matches(&id(&format!("abc11{}", "f".repeat(59)))));
        let even = IdPrefix::parse("abcd").unwrap();
        assert_eq!(even.lower(), id(&format!("abcd{}", "0".repeat(60))));
        let full = "e".repeat(64);
        let p = IdPrefix::parse(&full).unwrap();
        assert_eq!((p.lower(), p.upper()), (id(&full), id(&full)));
    }

    #[test]
    fn before_lower_borrows_and_stops_at_zero() {
        let p = IdPrefix::parse("ab00").unwrap();
        assert_eq!(
            p.before_lower(),
            Some(id(&format!("aaff{}", "f".repeat(60))))
        );
        assert_eq!(IdPrefix::parse("0000").unwrap().before_lower(), None);
    }

    #[test]
    fn rejects_short_long_and_non_hex() {
        for bad in ["", "abc", "abcg", "xyz12", &"a".repeat(65), "ab cd"] {
            assert!(IdPrefix::parse(bad).is_err(), "{bad:?}");
        }
        assert!(looks_like_prefix("dead"));
        assert!(!looks_like_prefix("main") && !looks_like_prefix("abc"));
    }
}
