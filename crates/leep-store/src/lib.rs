//! `BlobStore` backends for Leep: local filesystem (loose objects plus
//! packs, ADR 0029), S3-compatible object storage, and a bounded LRU cache
//! that can sit in front of any of them.

mod cache;
mod dirsync;
mod fs;
mod object;
#[cfg(test)]
mod objpack_tests;
mod objpacks;
mod objrepack;
pub mod pack;
#[cfg(test)]
mod pack_tests;
pub mod packfiles;
mod packset;
mod packwrite;
mod repack;
pub mod sync;

use std::path::PathBuf;
use std::sync::Arc;

use leep_core::memory::MemoryBlobStore;
use leep_core::{BlobStore, Error, Result};

pub use cache::{CacheStats, CachedBlobStore};
pub use fs::FsBlobStore;
pub use object::{ObjectBlobStore, S3Config};
pub use objrepack::PublishedObjectPack;
pub use packfiles::{FsPackFiles, ObjectPackFiles, PackFiles};
pub use packwrite::plan_merge;
pub use repack::{PublishedPack, RepackHook, RepackMode, RepackOptions, RepackReport, RepackStep};

fn storage_err(e: impl std::fmt::Display) -> Error {
    Error::Storage(e.to_string())
}

/// The path a `file://` URL names, after the scheme. A Windows drive
/// letter may follow the third slash (`file:///C:/x`, RFC 8089) or stand
/// where a host would (`file://C:/x`, as people type it); `/C:/x` is no
/// path on Windows, so the slash before a drive letter is dropped.
pub fn file_url_path(rest: &str) -> PathBuf {
    let b = rest.as_bytes();
    let drive = b.len() >= 3 && b[0] == b'/' && b[1].is_ascii_alphabetic() && b[2] == b':';
    PathBuf::from(if drive { &rest[1..] } else { rest })
}

/// The directory of a filesystem blob store URL (`file:///abs/path` or a
/// bare path), `None` for any other kind.
pub fn fs_blob_root(url: &str) -> Option<PathBuf> {
    if let Some(rest) = url.strip_prefix("file://") {
        return Some(file_url_path(rest));
    }
    (!url.contains("://") && url != "mem:").then(|| PathBuf::from(url))
}

/// The object store a blob store URL names (`s3://...`), `None` for any
/// other kind. See [`open_blob_store`] for the options.
pub fn open_object_blob_store(url: &str) -> Result<Option<ObjectBlobStore>> {
    if !url.starts_with("s3://") {
        return Ok(None);
    }
    let u = url::Url::parse(url).map_err(|e| Error::Invalid(format!("bad url {url:?}: {e}")))?;
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
    Ok(Some(ObjectBlobStore::s3(cfg)?))
}

/// Open a blob store from a URL: `mem://`, `file:///abs/path` (or a bare
/// path), or `s3://bucket?endpoint=..&region=..&allow_http=true`. S3
/// credentials come from the standard AWS environment variables.
pub fn open_blob_store(url: &str) -> Result<Arc<dyn BlobStore>> {
    if url == "mem://" || url == "mem:" {
        return Ok(Arc::new(MemoryBlobStore::new()));
    }
    if let Some(rest) = url.strip_prefix("file://") {
        return Ok(Arc::new(FsBlobStore::new(file_url_path(rest))));
    }
    if let Some(store) = open_object_blob_store(url)? {
        return Ok(Arc::new(store));
    }
    if url.contains("://") {
        return Err(Error::Invalid(format!(
            "unsupported blob store url {url:?}"
        )));
    }
    Ok(Arc::new(FsBlobStore::new(PathBuf::from(url))))
}

/// The pack files of the blob store at `url`, for copying packs whole
/// (ADR 0039): `packs/` of a filesystem store, `packs/` keys of a bucket.
/// `None` for a store that keeps no packs (`mem://`).
pub fn open_pack_files(url: &str) -> Result<Option<Arc<dyn PackFiles>>> {
    if url == "mem://" || url == "mem:" {
        return Ok(None);
    }
    if let Some(store) = open_object_blob_store(url)? {
        return Ok(Some(Arc::new(ObjectPackFiles::new(store.store))));
    }
    match fs_blob_root(url) {
        Some(root) => Ok(Some(Arc::new(FsPackFiles::new(root)))),
        None => Err(Error::Invalid(format!(
            "unsupported blob store url {url:?}"
        ))),
    }
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
        // Windows drive letters, in both spellings; a Unix path is untouched.
        assert_eq!(
            fs_blob_root("file:///C:/leep/blobs").unwrap(),
            PathBuf::from("C:/leep/blobs")
        );
        assert_eq!(
            fs_blob_root("file://C:/leep/blobs").unwrap(),
            PathBuf::from("C:/leep/blobs")
        );
        assert_eq!(
            fs_blob_root("file:///var/lib/leep").unwrap(),
            PathBuf::from("/var/lib/leep")
        );
        assert_eq!(fs_blob_root("file:///c").unwrap(), PathBuf::from("/c"));
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
