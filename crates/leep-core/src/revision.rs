//! Git-style revision suffixes (ADR 0064): `X~N`, `X^N`, chained left to
//! right as in `main~2^2~1`.
//!
//! Only the grammar lives here, so the server and the clients split a
//! revision the same way; walking the steps is `leep-derive`'s.
//!
//! `~` and `^` cannot appear in a ref name ([`crate::refname`] forbids them,
//! as git does), so a valid name never contains a suffix. Resolvers still
//! look the whole string up as a name first, for refs older than the rule.

use crate::error::{Error, Result};

/// One suffix step.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Step {
    /// `~N`: the Nth first-parent ancestor; `~` is `~1`.
    Ancestor(u64),
    /// `^N`: the Nth parent, 1-based; `^` is `^1` and `^0` the changeset
    /// itself.
    Parent(u64),
}

/// `rev` split at its first `~` or `^`: the base and the suffix, which is
/// empty when there is none. Does not check either half.
pub fn split(rev: &str) -> (&str, &str) {
    match rev.find(['~', '^']) {
        Some(i) => rev.split_at(i),
        None => (rev, ""),
    }
}

/// A revision with suffix steps: `None` when it has none. Reflog syntax
/// (`@{...}`), an empty base and a malformed suffix are `Invalid`.
pub fn parse(rev: &str) -> Result<Option<(&str, Vec<Step>)>> {
    if rev.contains("@{") {
        return Err(Error::Invalid(format!(
            "revision {rev:?}: reflog syntax `@{{...}}` is not supported; \
             `leep reflog` lists where a ref has been"
        )));
    }
    let (base, suffix) = split(rev);
    if suffix.is_empty() {
        return Ok(None);
    }
    if base.is_empty() {
        return Err(Error::Invalid(format!(
            "revision {rev:?}: a suffix needs a revision before it, as in `main~1`"
        )));
    }
    Ok(Some((base, parse_suffix(rev, suffix)?)))
}

/// The steps of `suffix`, a run of `~[N]` and `^[N]`.
fn parse_suffix(rev: &str, suffix: &str) -> Result<Vec<Step>> {
    let bad = |why: &str| Error::Invalid(format!("revision {rev:?}: {why}"));
    let mut steps = Vec::new();
    let mut rest = suffix;
    while let Some(op) = rest.chars().next() {
        rest = &rest[op.len_utf8()..];
        let digits = rest.len() - rest.trim_start_matches(|c: char| c.is_ascii_digit()).len();
        let (num, tail) = rest.split_at(digits);
        rest = tail;
        let n = if num.is_empty() {
            1
        } else {
            num.parse::<u64>()
                .map_err(|_| bad(&format!("`{op}{num}` is too large")))?
        };
        match op {
            '~' => steps.push(Step::Ancestor(n)),
            '^' => steps.push(Step::Parent(n)),
            _ => {
                return Err(bad(&format!(
                    "unexpected {op:?} in suffix {suffix:?}; only `~N` and `^N` are supported"
                )))
            }
        }
    }
    Ok(steps)
}

#[cfg(test)]
mod tests {
    use super::*;
    use Step::*;

    #[test]
    fn splits_at_first_suffix() {
        assert_eq!(split("main"), ("main", ""));
        assert_eq!(split("main~2^2"), ("main", "~2^2"));
        assert_eq!(split("@^"), ("@", "^"));
        assert_eq!(split("release/1.0~1"), ("release/1.0", "~1"));
    }

    #[test]
    fn parses_steps() {
        assert_eq!(parse("main").unwrap(), None);
        assert_eq!(parse("main~").unwrap(), Some(("main", vec![Ancestor(1)])));
        assert_eq!(parse("main~3").unwrap(), Some(("main", vec![Ancestor(3)])));
        assert_eq!(parse("main^").unwrap(), Some(("main", vec![Parent(1)])));
        assert_eq!(parse("main^0").unwrap(), Some(("main", vec![Parent(0)])));
        assert_eq!(
            parse("tags/v1~2^2~1").unwrap(),
            Some(("tags/v1", vec![Ancestor(2), Parent(2), Ancestor(1)]))
        );
        assert_eq!(
            parse("x^^~~007").unwrap(),
            Some(("x", vec![Parent(1), Parent(1), Ancestor(1), Ancestor(7)]))
        );
        assert_eq!(
            parse(&format!("x~{}", u64::MAX)).unwrap(),
            Some(("x", vec![Ancestor(u64::MAX)]))
        );
    }

    #[test]
    fn rejects_malformed() {
        for rev in [
            "main~x",
            "main^x",
            "main~1x",
            "main~-1",
            "main^{commit}",
            "main^!",
            "main~1/a",
            "main~1\u{e9}",
            "~1",
            "^",
            "main@{1}",
            "@{-1}",
            "main@{yesterday}~1",
            "main~99999999999999999999",
        ] {
            let e = parse(rev).unwrap_err();
            assert!(matches!(e, Error::Invalid(_)), "{rev:?}: {e}");
            assert!(e.to_string().contains(rev), "{rev:?}: {e}");
        }
    }
}
