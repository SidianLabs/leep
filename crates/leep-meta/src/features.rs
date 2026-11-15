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

/// Garbage collection marks live only in blob-store segments; the
/// `gc-mark-v1` rows are being or have been deleted (ADR 0103). A `gc` that
/// predates segments would trust what is left of the rows.
pub const GC_MARK_SEGMENTS: &str = "gc-mark-segments";

/// `skiplist` and `history-tree-v4` rows are being or have been deleted
/// once a derived segment in the blob store holds their values (ADR 0104,
/// `--derived-segments retire`). A binary that predates segments would
/// derive whole histories again, and its `gc` would delete live segments.
pub const DERIVED_SEGMENTS: &str = "derived-segments";

/// Every feature this binary can read.
pub const KNOWN: &[&str] = &[CHANGESET_ZSTD_DICT, GC_MARK_SEGMENTS, DERIVED_SEGMENTS];

/// Record `name` in the store at `url` (idempotent). `mem://` stores are
/// never shared with another binary, so there is nothing to record.
pub async fn add_store_feature(url: &str, mode: crate::SchemaMode, name: &str) -> Result<()> {
    if !KNOWN.contains(&name) {
        return Err(Error::Invalid(format!("unknown store feature {name}")));
    }
    if url.starts_with("sqlite:") {
        let store = crate::SqliteMetaStore::connect_with(url, mode).await?;
        let r = store.add_feature(name).await;
        store.pool.close().await;
        r
    } else if url.starts_with("postgres://") || url.starts_with("postgresql://") {
        let store = crate::PostgresMetaStore::connect_with(url, mode).await?;
        let r = store.add_feature(name).await;
        store.pool.close().await;
        r
    } else if url.starts_with("mem://") {
        Ok(())
    } else {
        Err(Error::Invalid(format!("unsupported meta store url: {url}")))
    }
}

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
        check(&[GC_MARK_SEGMENTS.to_string()]).unwrap();
        check(&[DERIVED_SEGMENTS.to_string()]).unwrap();
        let err = check(&[
            CHANGESET_ZSTD_DICT.to_string(),
            "tree-zstd".to_string(),
            "x".to_string(),
        ])
        .unwrap_err();
        assert!(err.to_string().contains("(tree-zstd, x)"), "{err}");
    }

    /// `gc --retire-mark-rows` records its feature by URL; the store still
    /// opens, and an unknown name is refused before anything is written.
    #[tokio::test]
    async fn a_feature_is_recorded_by_url() {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite://{}/m.db?mode=rwc", dir.path().display());
        let mode = crate::SchemaMode::Migrate;
        add_store_feature(&url, mode, GC_MARK_SEGMENTS)
            .await
            .unwrap();
        add_store_feature(&url, mode, GC_MARK_SEGMENTS)
            .await
            .unwrap();
        assert!(add_store_feature(&url, mode, "nope").await.is_err());
        let store = crate::SqliteMetaStore::connect_with(&url, mode)
            .await
            .unwrap();
        assert_eq!(store.features().await.unwrap(), [GC_MARK_SEGMENTS]);
        add_store_feature("mem://", mode, GC_MARK_SEGMENTS)
            .await
            .unwrap();
    }
}
