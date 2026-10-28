//! Temp-file names that cannot collide on a volume shared by several
//! servers. A pid and a counter are not enough: containers on one volume all
//! run as pid 1 and count from 0, so each name also carries a random
//! per-process nonce, and a file is only ever created exclusively.

use std::fs::{File, OpenOptions};
use std::io::{self, ErrorKind};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;

/// More clashes than this mean something other than bad luck.
const ATTEMPTS: usize = 64;

pub(crate) static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Random per process; falls back to a clock-and-address hash if the OS has
/// no randomness, which still differs between processes.
pub(crate) fn nonce() -> &'static str {
    static NONCE: OnceLock<String> = OnceLock::new();
    NONCE.get_or_init(|| {
        let mut b = [0u8; 8];
        if getrandom::fill(&mut b).is_err() {
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            let mut h = blake3::Hasher::new();
            h.update(&nanos.to_be_bytes());
            h.update(&std::process::id().to_be_bytes());
            h.update(&(&b as *const _ as usize).to_be_bytes());
            b.copy_from_slice(&h.finalize().as_bytes()[..8]);
        }
        hex::encode(b)
    })
}

/// `.tmp-<pid>-<nonce>-<n><suffix>`. The `.tmp-` prefix is what listings and
/// repack cleanup recognise.
pub(crate) fn temp_name(suffix: &str) -> String {
    format!(
        ".tmp-{}-{}-{}{suffix}",
        std::process::id(),
        nonce(),
        TMP_COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

/// A new temp file in `dir`, retrying with a fresh name on a clash. `create_new`
/// means an existing file is never truncated or shared.
pub(crate) fn create_exclusive(dir: &Path, suffix: &str) -> io::Result<(PathBuf, File)> {
    for _ in 0..ATTEMPTS {
        let path = dir.join(temp_name(suffix));
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Err(e) if e.kind() == ErrorKind::AlreadyExists => continue,
            Ok(f) => return Ok((path, f)),
            Err(e) => return Err(e),
        }
    }
    Err(io::Error::new(
        ErrorKind::AlreadyExists,
        "no unused temp file name",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_carry_the_nonce_and_differ() {
        let (a, b) = (temp_name(".pack"), temp_name(".pack"));
        assert_ne!(a, b);
        assert!(a.starts_with(".tmp-") && a.ends_with(".pack"));
        assert!(a.contains(nonce()));
    }

    #[test]
    fn skips_names_already_taken() {
        let dir = tempfile::tempdir().unwrap();
        let next = TMP_COUNTER.load(Ordering::Relaxed);
        for n in next..next + 8 {
            let name = format!(".tmp-{}-{}-{n}", std::process::id(), nonce());
            std::fs::write(dir.path().join(name), b"theirs").unwrap();
        }
        let (path, _f) = create_exclusive(dir.path(), "").unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"");
        let taken = std::fs::read_dir(dir.path())
            .unwrap()
            .filter(|e| std::fs::read(e.as_ref().unwrap().path()).unwrap() == b"theirs")
            .count();
        assert_eq!(taken, 8);
    }
}
