//! `BlobStore` backends for Leep: local filesystem (loose objects plus
//! packs, ADR 0029), S3-compatible object storage, and a bounded LRU cache
//! that can sit in front of any of them.

mod cache;
mod fs;
mod object;
pub mod pack;
#[cfg(test)]
mod pack_tests;
mod packset;
mod repack;
pub mod sync;

use std::path::PathBuf;
use std::sync::Arc;

use leep_core::memory::MemoryBlobStore;
use leep_core::{BlobStore, Error, Result};

pub use cache::{CacheStats, CachedBlobStore};
pub use fs::FsBlobStore;
pub use object::{ObjectBlobStore, S3Config};
pub use repack::{PublishedPack, RepackHook, RepackOptions, RepackReport, RepackStep};

fn storage_err(e: impl std::fmt::Display) -> Error {
    Error::Storage(e.to_string())
}

/// The directory of a filesystem blob store URL (`file:///abs/path` or a
/// bare path), `None` for any other kind.
pub fn fs_blob_root(url: &str) -> Option<PathBuf> {
    if let Some(rest) = url.strip_prefix("file://") {
        return Some(PathBuf::from(rest));
    }
    (!url.contains("://") && url != "mem:").then(|| PathBuf::from(url))
}

/// Open a blob store from a URL: `mem://`, `file:///abs/path` (or a bare
/// path), or `s3://bucket?endpoint=..&region=..&allow_http=true`. S3
/// credentials come from the standard AWS environment variables.
pub fn open_blob_store(url: &str) -> Result<Arc<dyn BlobStore>> {
    if url == "mem://" || url == "mem:" {
        return Ok(Arc::new(MemoryBlobStore::new()));
    }
    if let Some(rest) = url.strip_prefix("file://") {
        return Ok(Arc::new(FsBlobStore::new(PathBuf::from(rest))));
    }
    if url.starts_with("s3://") {
        let u =
            url::Url::parse(url).map_err(|e| Error::Invalid(format!("bad url {url:?}: {e}")))?;
        let bucket = u
            .host_str()
            .filter(|b| !b.is_empty())
            .ok_or_else(|| Error::Invalid(format!("missing bucket in {url:?}")))?;
        let mut cfg = S3Config {
            bucket: bucket.to_string(),
            ..S3Config::default()
        };
        for (k, v) in u.query_pairs() {
            match &*k {
                "endpoint" => cfg.endpoint = Some(v.into_owned()),
                "region" => cfg.region = Some(v.into_owned()),
                "allow_http" => cfg.allow_http = matches!(&*v, "true" | "1"),
                other => return Err(Error::Invalid(format!("unknown s3 option {other:?}"))),
            }
        }
        return Ok(Arc::new(ObjectBlobStore::s3(cfg)?));
    }
    if url.contains("://") {
        return Err(Error::Invalid(format!(
            "unsupported blob store url {url:?}"
        )));
    }
    Ok(Arc::new(FsBlobStore::new(PathBuf::from(url))))
}

#[cfg(test)]
mod tests {
    use super::*;
    use leep_core::memory::check_blob_store_contract;

    #[tokio::test]
    async fn open_urls() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().to_str().unwrap();
        for url in ["mem://".to_string(), format!("file://{p}"), p.to_string()] {
            check_blob_store_contract(&*open_blob_store(&url).unwrap()).await;
        }
        let s3 = open_blob_store(
            "s3://bucket?endpoint=http://localhost:9000&region=us-east-1&allow_http=true",
        );
        assert!(s3.is_ok());
        assert!(open_blob_store("s3://").is_err());
        assert!(open_blob_store("ftp://x").is_err());
        assert!(open_blob_store("s3://b?bogus=1").is_err());
        assert_eq!(fs_blob_root(&format!("file://{p}")).unwrap(), dir.path());
        assert_eq!(fs_blob_root(p).unwrap(), dir.path());
        assert!(fs_blob_root("s3://bucket").is_none());
        assert!(fs_blob_root("mem://").is_none());
    }

    /// With nothing scheduled the fault injector is transparent: every
    /// backend still passes the contract through it.
    #[tokio::test]
    async fn quiet_fault_wrappers_pass_contract() {
        use leep_testkit::{FaultPlan, FaultyBlobStore};
        let dir = tempfile::tempdir().unwrap();
        let backends: Vec<Arc<dyn BlobStore>> = vec![
            Arc::new(FsBlobStore::new(dir.path())),
            Arc::new(ObjectBlobStore::in_memory()),
        ];
        for inner in backends {
            let store = FaultyBlobStore::new(inner, FaultPlan::new(1));
            check_blob_store_contract(&store).await;
        }
    }
}
