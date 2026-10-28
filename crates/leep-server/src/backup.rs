//! Backup, restore and backup verification (ADR 0026).
//!
//! A backup directory holds one shared pool of blobs and any number of
//! snapshots:
//!
//! ```text
//! <out>/blobs/<2 hex>/<62 hex>          every blob any snapshot needs (loose)
//! <out>/blobs/packs/<id>.pack, .idx     packs copied whole from a packed source
//! <out>/snapshots/<UTC stamp>/meta.db   metadata as of one instant
//! <out>/snapshots/<UTC stamp>/manifest.json   written last: the snapshot exists
//! ```
//!
//! The metadata is read first, in one transaction. Blobs are copied second,
//! so every blob the snapshot references was already stored when the snapshot
//! was taken and is still there (a blob that a stored changeset references is
//! never deleted: ADR 0012). The closure check then proves it for this run by
//! walking every changeset of the snapshot against the copied blobs, and a
//! snapshot is sealed with its result.
//!
//! The pool only grows. Nothing the source deletes is deleted or hidden in
//! it (the source's pack tombstones are not copied), because since the
//! changeset sweep (ADR 0059) an older snapshot can name an object the
//! source no longer has.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use leep_core::{BlobStore, Error, Result};
use leep_derive::integrity::{self, Problem, Progress, VerifyOptions};
use leep_derive::Repo;
use leep_meta::snapshot::{self, SnapshotInfo};
use leep_meta::SchemaMode;
use leep_store::sync::{
    sync_blobs, sync_packs, PackSyncMode, PackSyncReport, SyncOptions, SyncReport,
};
use leep_store::PackFiles;
use serde::{Deserialize, Serialize};

/// Manifest layout version. A reader refuses a larger one.
pub const MANIFEST_FORMAT: u32 = 1;
pub const MANIFEST_FILE: &str = "manifest.json";
pub const META_FILE: &str = "meta.db";
pub const SNAPSHOTS_DIR: &str = "snapshots";
pub const BLOBS_DIR: &str = "blobs";

fn storage(e: impl std::fmt::Display) -> Error {
    Error::Storage(e.to_string())
}

/// What a snapshot holds, recorded when it is sealed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Manifest {
    pub format: u32,
    pub leep_version: String,
    /// Directory name, a UTC timestamp.
    pub name: String,
    /// Unix seconds when the snapshot was sealed.
    pub created_at: i64,
    /// Unix seconds of the metadata read transaction.
    pub snapshot_taken_at: i64,
    /// `sqlite` or `postgres`: what the metadata came from.
    pub source_backend: String,
    /// Metadata schema version (ADR 0023).
    pub schema_version: u64,
    pub meta_file: String,
    /// BLAKE3 of `meta.db`, checked before a restore reads it.
    pub meta_blake3: String,
    pub meta_bytes: u64,
    pub counts: Counts,
    pub ref_log_last_seq: u64,
    /// Ref tips at the snapshot instant.
    pub refs: BTreeMap<String, String>,
    pub blobs: BlobSection,
    pub check: CheckSection,
    pub elapsed_secs: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Counts {
    pub changesets: u64,
    pub refs: u64,
    pub derived: u64,
    pub ref_log: u64,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct BlobSection {
    /// False for `--no-blobs`: blobs are protected some other way.
    pub included: bool,
    /// Where the blobs were copied, as given (a path or URL).
    #[serde(default)]
    pub location: String,
    #[serde(default)]
    pub listed: u64,
    #[serde(default)]
    pub already_present: u64,
    #[serde(default)]
    pub copied: u64,
    #[serde(default)]
    pub bytes_copied: u64,
    #[serde(default)]
    pub replaced: u64,
    /// Deleted by GC between listing and reading; unreferenced by definition.
    #[serde(default)]
    pub vanished: u64,
    /// Held by earlier snapshots or deleted from the source since.
    #[serde(default)]
    pub only_in_backup: u64,
    #[serde(default)]
    pub corrupt: Vec<String>,
    /// Packs copied whole (ADR 0039); zero when either side keeps none.
    #[serde(default)]
    pub packs_listed: u64,
    #[serde(default)]
    pub packs_already_present: u64,
    #[serde(default)]
    pub packs_copied: u64,
    #[serde(default)]
    pub pack_bytes_copied: u64,
    /// Pool packs a source repack merged away, deleted because other pool
    /// packs hold all they held.
    #[serde(default)]
    pub packs_pruned: u64,
    /// Always 0 since issue #155: the pool keeps objects the source deleted,
    /// because older snapshots may need them. Kept for older manifests.
    #[serde(default)]
    pub tombstones_copied: u64,
    #[serde(default)]
    pub corrupt_packs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct CheckSection {
    /// `closure`, `closure+rehash` or `skipped`.
    pub mode: String,
    /// The closure check found nothing wrong (false when it was skipped).
    pub ok: bool,
    #[serde(default)]
    pub changesets: u64,
    #[serde(default)]
    pub tree_nodes: u64,
    #[serde(default)]
    pub blobs_checked: u64,
    #[serde(default)]
    pub blobs_rehashed: u64,
    #[serde(default)]
    pub problems: u64,
    /// Blobs the check needed that the blob copy lacked, if the check ran
    /// against it.
    #[serde(default)]
    pub first_problems: Vec<String>,
}

/// Points in `backup` that tests (and only tests) hook into.
#[async_trait]
pub trait Hooks: Send + Sync {
    /// The metadata snapshot is written; blobs are not copied yet. Anything
    /// that happens here is what happens "between the two halves" of a backup.
    async fn after_meta_snapshot(&self, _info: &SnapshotInfo) {}
}

pub struct NoHooks;

impl Hooks for NoHooks {}

pub struct BackupOptions {
    pub meta_url: String,
    pub source_blobs: Arc<dyn BlobStore>,
    /// The source's pack files, copied whole when the destination has a
    /// place for them too.
    pub source_packs: Option<Arc<dyn PackFiles>>,
    pub out: PathBuf,
    /// Where blobs go; `None` for `--no-blobs`.
    pub dest_blobs: Option<Arc<dyn BlobStore>>,
    pub dest_packs: Option<Arc<dyn PackFiles>>,
    pub dest_blobs_location: String,
    /// Run the closure check (against the copied blobs, or the source's when
    /// there are none).
    pub check: bool,
    /// The check also re-hashes every reachable blob.
    pub rehash: bool,
    pub concurrency: usize,
    pub max_problems: usize,
    /// Snapshot directory name; default the current UTC time.
    pub name: Option<String>,
}

pub struct BackupOutcome {
    pub manifest: Manifest,
    pub snapshot_dir: PathBuf,
    pub problems: Vec<Problem>,
    pub problems_total: u64,
}

impl BackupOutcome {
    /// Sealed and the closure check found nothing.
    pub fn ok(&self) -> bool {
        self.manifest.check.ok
    }
}

/// `YYYYMMDDTHHMMSSZ` for unix seconds.
pub fn utc_stamp(secs: i64) -> String {
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    // Civil date from days since 1970-01-01 (Howard Hinnant's algorithm).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}{month:02}{day:02}T{:02}{:02}{:02}Z",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    )
}

fn backend_of(meta_url: &str) -> &'static str {
    if meta_url.starts_with("sqlite:") {
        "sqlite"
    } else {
        "postgres"
    }
}

/// Fail early, and without creating anything, when the source is not there:
/// a mistyped path must not "back up" a fresh empty database.
pub fn check_source_exists(meta: &str, blobs: &str) -> Result<()> {
    if let Some(rest) = meta.strip_prefix("sqlite:") {
        let path = rest.trim_start_matches("//");
        let path = path.split('?').next().unwrap_or(path);
        let path = percent_decode(path);
        if path != ":memory:" && !path.is_empty() && !Path::new(&path).exists() {
            return Err(Error::Invalid(format!(
                "metadata database {path} does not exist (backup never creates one)"
            )));
        }
    } else if meta.starts_with("mem://") {
        return Err(Error::Invalid(
            "a mem:// store lives in one process, so there is nothing to back up".into(),
        ));
    }
    if let Some(dir) = blobs.strip_prefix("file://") {
        if !Path::new(dir).is_dir() {
            return Err(Error::Invalid(format!(
                "blob directory {dir} does not exist (backup never creates one)"
            )));
        }
    } else if blobs.starts_with("mem://") {
        return Err(Error::Invalid(
            "a mem:// store lives in one process, so there is nothing to back up".into(),
        ));
    }
    Ok(())
}

fn percent_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len() {
            if let Ok(v) = u8::from_str_radix(&s[i + 1..i + 3], 16) {
                out.push(v);
                i += 3;
                continue;
            }
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn blake3_file(path: &Path) -> Result<(String, u64)> {
    use std::io::Read;
    let mut f = std::fs::File::open(path).map_err(storage)?;
    let mut h = blake3::Hasher::new();
    let mut buf = vec![0u8; 1 << 20];
    let mut n = 0u64;
    loop {
        let k = f.read(&mut buf).map_err(storage)?;
        if k == 0 {
            break;
        }
        h.update(&buf[..k]);
        n += k as u64;
    }
    Ok((h.finalize().to_hex().to_string(), n))
}

fn write_atomically(path: &Path, bytes: &[u8]) -> Result<()> {
    use std::io::Write;
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    let tmp = PathBuf::from(tmp);
    let mut f = std::fs::File::create(&tmp).map_err(storage)?;
    f.write_all(bytes).map_err(storage)?;
    f.sync_all().map_err(storage)?;
    drop(f);
    std::fs::rename(&tmp, path).map_err(storage)?;
    if let Some(dir) = path.parent() {
        leep_core::fsync::sync_dir(dir).map_err(storage)?;
    }
    Ok(())
}

/// Walk every changeset in the snapshot at `meta_file` and check that its
/// trees and blobs exist in `blobs`: the property a restore needs. Derived
/// rows are not recomputed; `leep-server verify` does that on the restored
/// store.
pub async fn closure_check(
    meta_file: &Path,
    blobs: Arc<dyn BlobStore>,
    rehash: bool,
    max_problems: usize,
    progress: &dyn Progress,
) -> Result<integrity::VerifyReport> {
    let opened = snapshot::open_snapshot(meta_file).await?;
    let meta = leep_meta::open_meta_store_with(&opened.url, SchemaMode::RequireCurrent).await?;
    let repo = Repo::new(blobs, meta);
    let opts = VerifyOptions {
        full: true,
        rehash,
        derived: Vec::new(),
        max_problems,
        ..VerifyOptions::default()
    };
    integrity::verify(&repo, &opts, progress).await
}

fn check_section(r: Option<&integrity::VerifyReport>, rehash: bool) -> CheckSection {
    match r {
        None => CheckSection {
            mode: "skipped".into(),
            ..CheckSection::default()
        },
        Some(r) => CheckSection {
            mode: if rehash { "closure+rehash" } else { "closure" }.into(),
            ok: r.ok(),
            changesets: r.stats.changesets_reachable + r.stats.changesets_other,
            tree_nodes: r.stats.tree_nodes,
            blobs_checked: r.stats.blobs_checked,
            blobs_rehashed: r.stats.blobs_rehashed,
            problems: r.problems_total,
            first_problems: r.problems.iter().take(10).map(|p| p.to_string()).collect(),
        },
    }
}

fn blob_section(location: &str, r: &SyncReport, p: Option<&PackSyncReport>) -> BlobSection {
    let p = p.cloned().unwrap_or_default();
    BlobSection {
        packs_listed: p.listed,
        packs_already_present: p.present,
        packs_copied: p.copied,
        pack_bytes_copied: p.bytes_copied,
        packs_pruned: p.pruned,
        tombstones_copied: p.tombstones_added,
        corrupt_packs: p.corrupt,
        included: true,
        location: location.to_string(),
        listed: r.listed,
        already_present: r.present,
        copied: r.copied,
        bytes_copied: r.bytes_copied,
        replaced: r.replaced,
        vanished: r.vanished,
        only_in_backup: r.only_in_destination,
        corrupt: r.corrupt.iter().map(|h| h.to_hex()).collect(),
    }
}

fn sync_options(
    concurrency: usize,
    progress: &Arc<dyn Progress>,
    what: &'static str,
) -> SyncOptions {
    let progress = progress.clone();
    SyncOptions {
        concurrency,
        progress: Some(Arc::new(move |r: &SyncReport| {
            progress.info(&format!(
                "{what}: {} blobs listed, {} copied ({} bytes), {} already there",
                r.listed, r.copied, r.bytes_copied, r.present
            ));
        })),
    }
}

/// Copy whole packs when both sides keep them, before the object copy: a
/// destination that reads packs then lists their objects as present.
async fn copy_packs(
    src: Option<&dyn PackFiles>,
    dst: Option<&dyn PackFiles>,
    mode: PackSyncMode,
    progress: &dyn Progress,
    what: &str,
) -> Result<Option<PackSyncReport>> {
    let (Some(src), Some(dst)) = (src, dst) else {
        return Ok(None);
    };
    let r = sync_packs(src, dst, mode).await?;
    if r.listed > 0 || r.pruned > 0 {
        progress.info(&format!(
            "{what}: packs: {} in the source, {} already there, {} copied ({} bytes), {} merged away and pruned, {} tombstones copied",
            r.listed, r.present, r.copied, r.bytes_copied, r.pruned, r.tombstones_added
        ));
    }
    for id in &r.corrupt {
        progress.info(&format!(
            "{what}: WARNING: pack {id} in the source does not match its index; not copied (its objects are copied one by one if they read)"
        ));
    }
    Ok(Some(r))
}

/// Take a backup: snapshot the metadata, copy the blobs the destination
/// lacks, check the result, and seal it with a manifest.
///
/// The manifest is written last and atomically, so a snapshot directory
/// without one is an interrupted run and is ignored (and safe to delete). A
/// run whose closure check found problems is still sealed, with the problems
/// recorded: the data is worth keeping and the operator needs to be told.
pub async fn backup(
    opts: BackupOptions,
    hooks: &dyn Hooks,
    progress: Arc<dyn Progress>,
) -> Result<BackupOutcome> {
    let started = Instant::now();
    if opts.concurrency == 0 {
        return Err(Error::Invalid("--concurrency must be at least 1".into()));
    }
    let name = opts
        .name
        .clone()
        .unwrap_or_else(|| utc_stamp(leep_core::store::unix_now()));
    if name.is_empty() || name.contains(['/', '\\']) || name.starts_with('.') {
        return Err(Error::Invalid(format!("bad snapshot name {name:?}")));
    }
    let snapshot_dir = opts.out.join(SNAPSHOTS_DIR).join(&name);
    if snapshot_dir.exists() {
        return Err(Error::Invalid(format!(
            "{} already exists; wait a second or pass --name",
            snapshot_dir.display()
        )));
    }
    std::fs::create_dir_all(&snapshot_dir).map_err(storage)?;
    let meta_path = snapshot_dir.join(META_FILE);

    // 1. Metadata first. Its instant is the backup's point in time.
    progress.info("backup: reading a consistent metadata snapshot");
    let info = snapshot::export_snapshot(&opts.meta_url, &meta_path).await?;
    progress.info(&format!(
        "backup: metadata at schema v{}: {} changesets, {} refs, {} derived rows, {} ref log entries",
        info.schema_version,
        info.changesets,
        info.refs.len(),
        info.derived,
        info.ref_log
    ));
    hooks.after_meta_snapshot(&info).await;

    // 2. Blobs second: everything the snapshot references is already stored.
    let blobs = match &opts.dest_blobs {
        Some(dest) => {
            let packs = copy_packs(
                opts.source_packs.as_deref(),
                opts.dest_packs.as_deref(),
                PackSyncMode::Pool,
                &*progress,
                "backup",
            )
            .await?;
            progress.info("backup: copying blobs the backup lacks");
            let r = sync_blobs(
                &*opts.source_blobs,
                &**dest,
                &sync_options(opts.concurrency, &progress, "backup"),
            )
            .await?;
            progress.info(&format!(
                "backup: blobs: {} listed, {} already in the backup, {} copied ({} bytes), {} deleted by gc meanwhile",
                r.listed, r.present, r.copied, r.bytes_copied, r.vanished
            ));
            for h in &r.corrupt {
                progress.info(&format!(
                    "backup: WARNING: blob {h} in the source does not hash to its name; not copied"
                ));
            }
            blob_section(&opts.dest_blobs_location, &r, packs.as_ref())
        }
        None => BlobSection::default(),
    };

    // 3. Prove the snapshot is restorable from what was copied.
    let report = if opts.check {
        progress.info("backup: checking that every blob the snapshot references was copied");
        let against = opts
            .dest_blobs
            .clone()
            .unwrap_or_else(|| opts.source_blobs.clone());
        Some(
            closure_check(
                &meta_path,
                against,
                opts.rehash,
                opts.max_problems,
                &*progress,
            )
            .await?,
        )
    } else {
        None
    };

    let (meta_blake3, meta_bytes) = blake3_file(&meta_path)?;
    let manifest = Manifest {
        format: MANIFEST_FORMAT,
        leep_version: env!("CARGO_PKG_VERSION").to_string(),
        name,
        created_at: leep_core::store::unix_now(),
        snapshot_taken_at: info.taken_at,
        source_backend: backend_of(&opts.meta_url).to_string(),
        schema_version: info.schema_version,
        meta_file: META_FILE.to_string(),
        meta_blake3,
        meta_bytes,
        counts: Counts {
            changesets: info.changesets,
            refs: info.refs.len() as u64,
            derived: info.derived,
            ref_log: info.ref_log,
        },
        ref_log_last_seq: info.ref_log_last_seq,
        refs: info
            .refs
            .iter()
            .map(|(n, id)| (n.clone(), id.0.to_hex()))
            .collect(),
        blobs,
        check: check_section(report.as_ref(), opts.rehash),
        elapsed_secs: started.elapsed().as_secs_f64(),
    };
    let json = serde_json::to_vec_pretty(&manifest).map_err(storage)?;
    write_atomically(&snapshot_dir.join(MANIFEST_FILE), &json)?;
    let (problems, problems_total) = report
        .map(|r| (r.problems, r.problems_total))
        .unwrap_or_default();
    Ok(BackupOutcome {
        manifest,
        snapshot_dir,
        problems,
        problems_total,
    })
}

/// Find a sealed snapshot. `from` is a backup directory (the newest sealed
/// snapshot, or `name`) or one snapshot directory.
pub fn find_snapshot(from: &Path, name: Option<&str>) -> Result<(PathBuf, Manifest)> {
    let direct = from.join(MANIFEST_FILE);
    let dir = if direct.is_file() {
        if name.is_some_and(|n| from.file_name().is_none_or(|f| f != n)) {
            return Err(Error::Invalid(format!(
                "{} is a snapshot directory; --snapshot only applies to a backup directory",
                from.display()
            )));
        }
        from.to_path_buf()
    } else {
        let snaps = from.join(SNAPSHOTS_DIR);
        let rd = std::fs::read_dir(&snaps).map_err(|e| {
            Error::Invalid(format!(
                "{} is not a backup directory (no {}): {e}",
                from.display(),
                snaps.display()
            ))
        })?;
        let mut sealed: Vec<String> = rd
            .flatten()
            .filter(|e| e.path().join(MANIFEST_FILE).is_file())
            .filter_map(|e| e.file_name().into_string().ok())
            .collect();
        sealed.sort();
        match name {
            Some(n) if sealed.iter().any(|s| s == n) => snaps.join(n),
            Some(n) => {
                return Err(Error::NotFound(format!(
                    "no sealed snapshot {n:?} in {}; sealed: {}",
                    snaps.display(),
                    if sealed.is_empty() {
                        "none".to_string()
                    } else {
                        sealed.join(", ")
                    }
                )))
            }
            None => snaps.join(sealed.last().ok_or_else(|| {
                Error::NotFound(format!("no sealed snapshot in {}", snaps.display()))
            })?),
        }
    };
    let text = std::fs::read_to_string(dir.join(MANIFEST_FILE)).map_err(storage)?;
    let manifest: Manifest = serde_json::from_str(&text)
        .map_err(|e| Error::Invalid(format!("bad manifest in {}: {e}", dir.display())))?;
    if manifest.format > MANIFEST_FORMAT {
        return Err(Error::Invalid(format!(
            "{} has manifest format {}, newer than this server understands ({MANIFEST_FORMAT}); \
             upgrade leep-server",
            dir.display(),
            manifest.format
        )));
    }
    Ok((dir, manifest))
}

/// What a snapshot's manifest and file must satisfy before anything reads it.
async fn check_snapshot_file(dir: &Path, m: &Manifest) -> Result<PathBuf> {
    let latest = snapshot::latest_schema_version();
    let newer = |v: u64| {
        Error::Invalid(format!(
            "this backup has metadata schema v{v}, newer than this server (v{latest}); \
             restore it with a newer leep-server (ADR 0023)"
        ))
    };
    if m.schema_version > latest {
        return Err(newer(m.schema_version));
    }
    let file = dir.join(&m.meta_file);
    let (digest, bytes) = blake3_file(&file)
        .map_err(|e| Error::Invalid(format!("cannot read {}: {e}", file.display())))?;
    if digest != m.meta_blake3 || bytes != m.meta_bytes {
        return Err(Error::Invalid(format!(
            "{} does not match its manifest (BLAKE3 {digest}, {bytes} bytes; expected {}, {} \
             bytes): the file was damaged or replaced after the backup",
            file.display(),
            m.meta_blake3,
            m.meta_bytes
        )));
    }
    // The manifest is trusted for nothing the file can say itself.
    let actual = snapshot::snapshot_schema_version(&file).await?;
    if actual > latest {
        return Err(newer(actual));
    }
    if actual != m.schema_version {
        return Err(Error::Invalid(format!(
            "{} is at schema v{actual} but its manifest says v{}",
            file.display(),
            m.schema_version
        )));
    }
    Ok(file)
}

/// Where a snapshot's blobs are when the backup directory holds them.
pub fn pool_dir(snapshot_dir: &Path) -> PathBuf {
    // <out>/snapshots/<name> -> <out>/blobs
    snapshot_dir
        .parent()
        .and_then(Path::parent)
        .map_or_else(|| PathBuf::from(BLOBS_DIR), |p| p.join(BLOBS_DIR))
}

pub struct VerifyBackupOptions {
    pub from: PathBuf,
    pub snapshot: Option<String>,
    /// Blobs to check against, normally the backup's pool.
    pub blobs: Arc<dyn BlobStore>,
    pub rehash: bool,
    pub max_problems: usize,
}

pub struct VerifyBackupOutcome {
    pub manifest: Manifest,
    pub report: integrity::VerifyReport,
}

/// Check a sealed snapshot without restoring it: the metadata file matches
/// its manifest and every blob it references is in `blobs` (and, with
/// `rehash`, holds the right bytes).
pub async fn verify_backup(
    opts: VerifyBackupOptions,
    progress: &dyn Progress,
) -> Result<VerifyBackupOutcome> {
    let (dir, manifest) = find_snapshot(&opts.from, opts.snapshot.as_deref())?;
    let file = check_snapshot_file(&dir, &manifest).await?;
    let report = closure_check(&file, opts.blobs, opts.rehash, opts.max_problems, progress).await?;
    Ok(VerifyBackupOutcome { manifest, report })
}

pub struct RestoreOptions {
    pub from: PathBuf,
    pub snapshot: Option<String>,
    pub meta_url: String,
    /// What opening the target metadata store does about migrations.
    pub meta_mode: SchemaMode,
    pub target_blobs: Arc<dyn BlobStore>,
    pub target_packs: Option<Arc<dyn PackFiles>>,
    /// Blobs to restore from, normally the backup's pool; `None` when the
    /// backup holds none (`--no-blobs`) and the caller gave no source.
    pub source_blobs: Option<Arc<dyn BlobStore>>,
    /// Packs in the backup, copied whole when the target keeps packs too.
    /// A backup taken before packs were copied holds none; its objects are
    /// all loose and restore as before.
    pub source_packs: Option<Arc<dyn PackFiles>>,
    /// Allow a non-empty target: delete the metadata that is there, and
    /// accept blobs that are there.
    pub force: bool,
    /// Allow a non-empty blob store when the metadata store is empty, to
    /// continue a restore that was interrupted.
    pub resume: bool,
    /// Restore a snapshot whose closure check found problems.
    pub accept_failed_check: bool,
    /// Run the closure check on the restored stores.
    pub check: bool,
    pub rehash: bool,
    pub concurrency: usize,
    pub max_problems: usize,
}

pub struct RestoreOutcome {
    pub manifest: Manifest,
    pub blobs: Option<SyncReport>,
    pub packs: Option<PackSyncReport>,
    pub imported: snapshot::ImportInfo,
    pub report: Option<integrity::VerifyReport>,
    pub elapsed_secs: f64,
}

/// Restore a snapshot into empty stores.
///
/// Order matters for a restore that dies halfway: blobs first (idempotent,
/// whole objects only), the metadata last and in one transaction. A target
/// whose metadata is empty is therefore not a repository yet, and the same
/// command can be run again (with `--resume` if blobs are already there).
pub async fn restore(opts: RestoreOptions, progress: Arc<dyn Progress>) -> Result<RestoreOutcome> {
    let started = Instant::now();
    let (dir, manifest) = find_snapshot(&opts.from, opts.snapshot.as_deref())?;
    progress.info(&format!(
        "restore: snapshot {} (taken {}, schema v{}, {} changesets, {} refs)",
        manifest.name,
        utc_stamp(manifest.snapshot_taken_at),
        manifest.schema_version,
        manifest.counts.changesets,
        manifest.counts.refs
    ));
    if !manifest.check.ok && !opts.accept_failed_check {
        return Err(Error::Invalid(format!(
            "this snapshot's closure check {} ({} problem(s){}); a restore from it would not \
             verify. Restore an earlier snapshot, or pass --accept-failed-check",
            if manifest.check.mode == "skipped" {
                "was skipped"
            } else {
                "failed"
            },
            manifest.check.problems,
            manifest
                .check
                .first_problems
                .first()
                .map(|p| format!(": {p}"))
                .unwrap_or_default()
        )));
    }
    let file = check_snapshot_file(&dir, &manifest).await?;
    if opts.concurrency == 0 {
        return Err(Error::Invalid("--concurrency must be at least 1".into()));
    }

    // Refuse before writing anything.
    let meta_empty = snapshot::destination_is_empty(&opts.meta_url, opts.meta_mode).await?;
    if !meta_empty && !opts.force {
        return Err(Error::Invalid(
            "the destination metadata store is not empty; restore into a fresh database, or \
             pass --force to delete what is there (stop every server that uses it first)"
                .into(),
        ));
    }
    let blobs_empty = opts.target_blobs.list_page(None, 1).await?.is_empty();
    if !blobs_empty && !(opts.force || (opts.resume && meta_empty)) {
        return Err(Error::Invalid(
            "the destination blob store is not empty; restore into an empty one, pass --resume \
             to continue an interrupted restore (the metadata store must still be empty), or \
             --force"
                .into(),
        ));
    }

    let src = match (&opts.source_blobs, manifest.blobs.included) {
        (Some(src), _) => src,
        (None, true) => {
            return Err(Error::Invalid(
                "no blob source given for this backup".into(),
            ))
        }
        (None, false) => {
            return Err(Error::Invalid(
                "this backup was taken with --no-blobs; pass --blobs-from with the store that \
                 holds its blobs"
                    .into(),
            ))
        }
    };
    let pack_report = copy_packs(
        opts.source_packs.as_deref(),
        opts.target_packs.as_deref(),
        PackSyncMode::Restore,
        &*progress,
        "restore",
    )
    .await?;
    if let Some(p) = pack_report.as_ref().filter(|p| !p.corrupt.is_empty()) {
        return Err(Error::Storage(format!(
            "{} pack(s) in the backup do not match their indexes (first: {}); the backup is damaged",
            p.corrupt.len(),
            p.corrupt[0]
        )));
    }
    let blob_report = Some(
        sync_blobs(
            &**src,
            &*opts.target_blobs,
            &sync_options(opts.concurrency, &progress, "restore"),
        )
        .await?,
    );
    if let Some(r) = &blob_report {
        progress.info(&format!(
            "restore: blobs: {} in the backup, {} already there, {} copied ({} bytes)",
            r.listed, r.present, r.copied, r.bytes_copied
        ));
        if !r.corrupt.is_empty() {
            return Err(Error::Storage(format!(
                "{} blob(s) in the backup do not hash to their names (first: {}); the backup is damaged",
                r.corrupt.len(),
                r.corrupt[0]
            )));
        }
    }

    progress.info("restore: loading metadata in one transaction");
    let imported =
        snapshot::import_snapshot(&file, &opts.meta_url, opts.force, opts.meta_mode).await?;
    let c = &manifest.counts;
    if imported.changesets != c.changesets
        || imported.refs != c.refs
        || imported.derived != c.derived
        || imported.ref_log != c.ref_log
    {
        // Cannot happen with an intact file (the digest matched); a mismatch
        // means the manifest and the file disagree.
        return Err(Error::Storage(format!(
            "restored {}/{}/{}/{} changesets/refs/derived/ref log rows but the manifest lists \
             {}/{}/{}/{}",
            imported.changesets,
            imported.refs,
            imported.derived,
            imported.ref_log,
            c.changesets,
            c.refs,
            c.derived,
            c.ref_log
        )));
    }
    progress.info(&format!(
        "restore: metadata loaded: {} changesets, {} refs, {} derived rows, {} ref log entries",
        imported.changesets, imported.refs, imported.derived, imported.ref_log
    ));

    let report = if opts.check {
        progress.info("restore: checking the restored stores");
        let meta = leep_meta::open_meta_store_with(&opts.meta_url, opts.meta_mode).await?;
        let repo = Repo::new(opts.target_blobs.clone(), meta);
        let vopts = VerifyOptions {
            full: true,
            rehash: opts.rehash,
            derived: Vec::new(),
            max_problems: opts.max_problems,
            ..VerifyOptions::default()
        };
        Some(integrity::verify(&repo, &vopts, &*progress).await?)
    } else {
        None
    };
    Ok(RestoreOutcome {
        manifest,
        blobs: blob_report,
        packs: pack_report,
        imported,
        report,
        elapsed_secs: started.elapsed().as_secs_f64(),
    })
}

/// Write the Prometheus text-file metrics `node_exporter` serves from its
/// textfile directory. The last-success timestamp is carried over from the
/// previous file when this run found problems, so an alert on its age fires.
pub fn write_textfile_metrics(path: &Path, o: &BackupOutcome) -> Result<()> {
    let m = &o.manifest;
    let old = std::fs::read_to_string(path).unwrap_or_default();
    let carried = old
        .lines()
        .find(|l| l.starts_with("leep_backup_last_success_timestamp_seconds "))
        .map(str::to_string);
    let mut out = String::new();
    let mut gauge = |name: &str, help: &str, v: String| {
        out.push_str(&format!(
            "# HELP {name} {help}\n# TYPE {name} gauge\n{name} {v}\n"
        ));
    };
    gauge(
        "leep_backup_last_run_timestamp_seconds",
        "Unix time the last backup was sealed.",
        m.created_at.to_string(),
    );
    gauge(
        "leep_backup_last_run_ok",
        "1 when the last backup's closure check found nothing wrong.",
        u8::from(o.ok()).to_string(),
    );
    gauge(
        "leep_backup_snapshot_timestamp_seconds",
        "Unix time of the metadata snapshot in the last backup (its recovery point).",
        m.snapshot_taken_at.to_string(),
    );
    gauge(
        "leep_backup_duration_seconds",
        "How long the last backup took.",
        format!("{:.1}", m.elapsed_secs),
    );
    gauge(
        "leep_backup_check_problems",
        "Problems the last backup's closure check found.",
        m.check.problems.to_string(),
    );
    gauge(
        "leep_backup_changesets",
        "Changesets in the last snapshot.",
        m.counts.changesets.to_string(),
    );
    gauge(
        "leep_backup_blobs_copied",
        "Blobs the last backup copied.",
        m.blobs.copied.to_string(),
    );
    gauge(
        "leep_backup_blob_bytes_copied",
        "Bytes of blobs the last backup copied.",
        m.blobs.bytes_copied.to_string(),
    );
    let last = if o.ok() {
        Some(format!(
            "leep_backup_last_success_timestamp_seconds {}",
            m.created_at
        ))
    } else {
        carried
    };
    if let Some(line) = last {
        out.push_str(
            "# HELP leep_backup_last_success_timestamp_seconds Unix time of the last backup that passed its check.\n\
             # TYPE leep_backup_last_success_timestamp_seconds gauge\n",
        );
        out.push_str(&line);
        out.push('\n');
    }
    write_atomically(path, out.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stamps_are_utc_and_sortable() {
        assert_eq!(utc_stamp(0), "19700101T000000Z");
        assert_eq!(utc_stamp(951_782_400), "20000229T000000Z"); // leap day
        assert_eq!(utc_stamp(1_760_611_845), "20251016T105045Z");
        assert!(utc_stamp(1_000_000_000) < utc_stamp(1_000_000_001));
    }

    #[test]
    fn percent_decoding_handles_escapes_and_stray_percents() {
        assert_eq!(percent_decode("/a%20b/c%2Fd"), "/a b/c/d");
        assert_eq!(percent_decode("100%"), "100%");
        assert_eq!(percent_decode("%zz"), "%zz");
    }
}
