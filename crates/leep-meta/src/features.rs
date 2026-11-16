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

/// Rows of a derived segment family added by ADR 0105 (`changeset-v2`:
/// `generation`, `last_merge-v1`, `root_tree`, `log-filter-v1`) are being
/// or have been deleted once a segment holds their values. An ADR 0104
/// binary knows only `derived-segments`: without these rows it would
/// derive them again from the root, and its `gc` would delete the new
/// family's live segments as orphans, so it must refuse the store.
pub const DERIVED_SEGMENTS_2: &str = "derived-segments-2";

/// Rows of the derived segment family `content-v1` (ADR 0106: the code
/// search rows and their keyframes) are being or have been deleted once a
/// segment holds their values. A binary of ADR 0105 or before reads no
/// `content-v1` segment (it would derive every search row again on its
/// first probe) and its `gc` refuses the family's manifest row, so it must
/// refuse the store.
pub const DERIVED_SEGMENTS_3: &str = "derived-segments-3";

/// Every feature this binary can read.
pub const KNOWN: &[&str] = &[
    CHANGESET_ZSTD_DICT,
    GC_MARK_SEGMENTS,
    DERIVED_SEGMENTS,
    DERIVED_SEGMENTS_2,
    DERIVED_SEGMENTS_3,
];

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

/// Whether the store at `url` has `name` recorded. `mem://` stores never
/// do.
pub async fn has_store_feature(url: &str, mode: crate::SchemaMode, name: &str) -> Result<bool> {
    if url.starts_with("sqlite:") {
        let store = crate::SqliteMetaStore::connect_with(url, mode).await?;
        let r = store.features().await;
        store.pool.close().await;
        Ok(r?.iter().any(|f| f == name))
    } else if url.starts_with("postgres://") || url.starts_with("postgresql://") {
        let store = crate::PostgresMetaStore::connect_with(url, mode).await?;
        let r = store.features().await;
        store.pool.close().await;
        Ok(r?.iter().any(|f| f == name))
    } else if url.starts_with("mem://") {
        Ok(false)
    } else {
        Err(Error::Invalid(format!("unsupported meta store url: {url}")))
    }
}

/// Refuse a store that names a feature this binary does not know.
pub(crate) fn check(names: &[String]) -> Result<()> {
    check_against(KNOWN, names)
}

/// [`check`] for a binary that knows `known`: a test stands in for an older
/// binary with the list it shipped.
fn check_against(known: &[&str], names: &[String]) -> Result<()> {
    let unknown: Vec<&str> = names
        .iter()
        .map(String::as_str)
        .filter(|n| !known.contains(n))
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
        check(&[DERIVED_SEGMENTS.to_string(), DERIVED_SEGMENTS_2.to_string()]).unwrap();
        let err = check(&[
            CHANGESET_ZSTD_DICT.to_string(),
            "tree-zstd".to_string(),
            "x".to_string(),
        ])
        .unwrap_err();
        assert!(err.to_string().contains("(tree-zstd, x)"), "{err}");
    }

    /// Mixed fleets (ADR 0105): the feature list an ADR 0104 binary shipped
    /// refuses a store once `derived-segments-2` is recorded, naming it, and
    /// still opens one with only ADR 0104's feature (mode `build` of the new
    /// family records nothing, so rolling back from it stays free).
    #[test]
    fn an_adr_0104_binary_refuses_a_store_with_the_second_family_retired() {
        const ADR_0104_KNOWN: &[&str] = &[CHANGESET_ZSTD_DICT, GC_MARK_SEGMENTS, DERIVED_SEGMENTS];
        check_against(ADR_0104_KNOWN, &[DERIVED_SEGMENTS.to_string()]).unwrap();
        let err = check_against(
            ADR_0104_KNOWN,
            &[DERIVED_SEGMENTS.to_string(), DERIVED_SEGMENTS_2.to_string()],
        )
        .unwrap_err();
        assert!(err.to_string().contains("(derived-segments-2)"), "{err}");
        // Even recorded alone (only the new family retired).
        assert!(check_against(ADR_0104_KNOWN, &[DERIVED_SEGMENTS_2.to_string()]).is_err());
    }

    /// The same through a real store: once recorded by URL, a binary with
    /// ADR 0104's list refuses to open it, and this one opens it.
    #[tokio::test]
    async fn a_store_with_derived_segments_2_is_refused_by_the_old_check() {
        const ADR_0104_KNOWN: &[&str] = &[CHANGESET_ZSTD_DICT, GC_MARK_SEGMENTS, DERIVED_SEGMENTS];
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite://{}/m.db?mode=rwc", dir.path().display());
        let mode = crate::SchemaMode::Migrate;
        add_store_feature(&url, mode, DERIVED_SEGMENTS_2)
            .await
            .unwrap();
        let store = crate::SqliteMetaStore::connect_with(&url, mode)
            .await
            .unwrap();
        let names = store.features().await.unwrap();
        check(&names).unwrap();
        assert!(check_against(ADR_0104_KNOWN, &names).is_err());
        assert!(has_store_feature(&url, mode, DERIVED_SEGMENTS_2)
            .await
            .unwrap());
    }

    /// Mixed fleets (ADR 0106): a binary of ADR 0105 (which knows
    /// `derived-segments-2` but not `content-v1`) refuses a store once
    /// `derived-segments-3` is recorded, naming it, and still opens one with
    /// both earlier features (mode `build` of `content-v1` records nothing).
    #[test]
    fn an_adr_0105_binary_refuses_a_store_with_content_retired() {
        const ADR_0105_KNOWN: &[&str] = &[
            CHANGESET_ZSTD_DICT,
            GC_MARK_SEGMENTS,
            DERIVED_SEGMENTS,
            DERIVED_SEGMENTS_2,
        ];
        let before = [DERIVED_SEGMENTS.to_string(), DERIVED_SEGMENTS_2.to_string()];
        check_against(ADR_0105_KNOWN, &before).unwrap();
        let mut after = before.to_vec();
        after.push(DERIVED_SEGMENTS_3.to_string());
        let err = check_against(ADR_0105_KNOWN, &after).unwrap_err();
        assert!(err.to_string().contains("(derived-segments-3)"), "{err}");
        assert!(check_against(ADR_0105_KNOWN, &[DERIVED_SEGMENTS_3.to_string()]).is_err());
        check(&after).unwrap();
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
        assert!(has_store_feature(&url, mode, GC_MARK_SEGMENTS)
            .await
            .unwrap());
        assert!(!has_store_feature(&url, mode, DERIVED_SEGMENTS)
            .await
            .unwrap());
        add_store_feature("mem://", mode, GC_MARK_SEGMENTS)
            .await
            .unwrap();
    }
}
