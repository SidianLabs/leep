//! Directory durability that works on every platform.

use std::path::Path;

/// Make renames and creations in `dir` durable. Windows cannot open a
/// directory as a file and has no directory fsync, so there it only checks
/// that `dir` is one (the error path then matches Unix's).
pub fn sync_dir(dir: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        std::fs::File::open(dir)?.sync_all()
    }
    #[cfg(not(unix))]
    {
        if std::fs::metadata(dir)?.is_dir() {
            Ok(())
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::NotADirectory,
                format!("{} is not a directory", dir.display()),
            ))
        }
    }
}
