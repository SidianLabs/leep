//! Format features a store's rows may use (ADR 0055). A tool that starts
//! writing rows older servers cannot read records the feature in
//! `store_features` first; a server refuses to open a store naming a feature
//! it does not know, instead of failing on the first row it cannot read.
//! Servers from before the table existed do not look, so the operator still
//! upgrades every server before such a tool runs.

use leep_core::{Error, Result};

/// The schema version that adds `store_features`.
pub const FEATURES_SCHEMA_VERSION: i64 = 4;

/// Changeset rows may be zstd frames compressed with a `changeset_dicts`
/// dictionary (ADR 0053).
pub const CHANGESET_ZSTD_DICT: &str = "changeset-zstd-dict";

/// Every feature this binary can read.
pub const KNOWN: &[&str] = &[CHANGESET_ZSTD_DICT];

/// Refuse a store that names a feature this binary does not know.
pub(crate) fn check(names: &[String]) -> Result<()> {
    let unknown: Vec<&str> = names
        .iter()
        .map(String::as_str)
        .filter(|n| !KNOWN.contains(n))
        .collect();
    if unknown.is_empty() {
        return Ok(());
    }
    Err(Error::Other(format!(
        "the metadata store uses format features this server cannot read ({}); \
         a newer leep-server wrote them. Upgrade this server (see \"Format features\" \
         in OPERATIONS.md)",
        unknown.join(", ")
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_features_pass_and_others_are_named() {
        check(&[]).unwrap();
        check(&[CHANGESET_ZSTD_DICT.to_string()]).unwrap();
        let err = check(&[
            CHANGESET_ZSTD_DICT.to_string(),
            "tree-zstd".to_string(),
            "x".to_string(),
        ])
        .unwrap_err();
        assert!(err.to_string().contains("(tree-zstd, x)"), "{err}");
    }
}
