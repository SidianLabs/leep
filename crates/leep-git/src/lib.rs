//! Git bridge for Leep: import a git commit DAG into Leep changesets and
//! export Leep history as git commits.
//!
//! Commit mappings are stored both ways as derived data in the `MetaStore`
//! (`git-to-leep-v1`, `leep-to-git-v1`), which makes both directions
//! incremental and idempotent.

// Output goes through `leep_core::outln!`, which exits quietly when the
// reader of stdout has gone instead of panicking like `println!`.
#![warn(clippy::print_stdout)]

mod export;
mod import;
pub mod mirror;

pub use export::{export, ExportOptions, ExportReport, CHANGESET_TRAILER};
pub use import::{import, ImportOptions, ImportReport};

use leep_core::{ChangesetId, Error, Hash, MetaStore};

/// Derived-data kind: git commit sha -> changeset id.
pub const GIT_TO_LEEP: &str = "git-to-leep-v1";
/// Derived-data kind: changeset id -> git commit sha.
pub const LEEP_TO_GIT: &str = "leep-to-git-v1";
/// `extra` key holding the original commit sha.
pub const EXTRA_GIT_COMMIT: &str = "git.commit";
/// `extra` key holding `Name <email> <unix-time>` of the committer.
pub const EXTRA_GIT_COMMITTER: &str = "git.committer";
/// `extra` keys holding the `+HHMM` timezone offsets, needed to reproduce the
/// exact commit bytes.
pub const EXTRA_GIT_AUTHOR_TZ: &str = "git.author_tz";
pub const EXTRA_GIT_COMMITTER_TZ: &str = "git.committer_tz";
/// `extra` key holding the commit's `encoding` header, if any.
pub const EXTRA_GIT_ENCODING: &str = "git.encoding";
/// `extra` key holding the remaining commit headers (`gpgsig`, `mergetag`,
/// ...) in their original order, as a JSON array of `[name, value]` pairs.
/// Without them signed commits (e.g. every commit made on github.com) would
/// not round-trip to the same sha.
pub const EXTRA_GIT_HEADERS: &str = "git.extra_headers";

/// Largest single object the bridge reads from git. gix caps allocations
/// at 16 MiB when it opens a repository owned by another user (reduced
/// trust), which rejects real blobs such as the firmware files in Linux; the
/// operator chose this repository, so allow what git itself handles.
pub(crate) const GIT_ALLOC_LIMIT: usize = 4 << 30;

/// Open a git repository with [`GIT_ALLOC_LIMIT`] instead of gix's
/// reduced-trust fallback. It is passed like `git -c`, so it also wins over
/// a smaller limit in the repository's own configuration.
pub(crate) fn open_git(git_dir: &std::path::Path) -> leep_core::Result<gix::Repository> {
    open_git_with(git_dir, gix::open::Options::default())
}

pub(crate) fn open_git_with(
    git_dir: &std::path::Path,
    options: gix::open::Options,
) -> leep_core::Result<gix::Repository> {
    let options = options.cli_overrides([format!("gitoxide.objects.allocLimit={GIT_ALLOC_LIMIT}")]);
    gix::open_opts(git_dir, options).map_err(gerr)
}

pub(crate) fn gerr(e: impl std::fmt::Display) -> Error {
    Error::Other(format!("git: {e}"))
}

pub(crate) fn git_key(sha: &[u8]) -> Hash {
    Hash::of_object("git-commit", sha)
}

pub(crate) async fn lookup_leep(
    meta: &dyn MetaStore,
    sha: &gix::ObjectId,
) -> leep_core::Result<Option<ChangesetId>> {
    let Some(v) = meta
        .get_derived(GIT_TO_LEEP, &git_key(sha.as_bytes()))
        .await?
    else {
        return Ok(None);
    };
    let arr: [u8; 32] = v
        .as_ref()
        .try_into()
        .map_err(|_| Error::Encoding("bad git-to-leep value".into()))?;
    Ok(Some(ChangesetId(Hash(arr))))
}

pub(crate) async fn lookup_git(
    meta: &dyn MetaStore,
    id: &ChangesetId,
) -> leep_core::Result<Option<gix::ObjectId>> {
    let Some(v) = meta.get_derived(LEEP_TO_GIT, &id.0).await? else {
        return Ok(None);
    };
    gix::ObjectId::try_from(v.as_ref())
        .map(Some)
        .map_err(|_| Error::Encoding("bad leep-to-git value".into()))
}

/// Parse `Name <email>`; falls back to the whole string as the name.
pub(crate) fn parse_ident(s: &str) -> (String, String) {
    match (s.rfind('<'), s.ends_with('>')) {
        (Some(i), true) => (
            s[..i].trim_end().to_string(),
            s[i + 1..s.len() - 1].to_string(),
        ),
        _ => (s.to_string(), String::new()),
    }
}

/// `+HHMM` for an offset in seconds.
pub(crate) fn format_tz(offset: i32) -> String {
    let sign = if offset < 0 { '-' } else { '+' };
    let a = offset.unsigned_abs();
    format!("{sign}{:02}{:02}", a / 3600, a % 3600 / 60)
}

/// Offset in seconds for `+HHMM`; 0 if malformed.
pub(crate) fn parse_tz(s: &str) -> i32 {
    let b = s.as_bytes();
    if b.len() != 5 || !(b[0] == b'+' || b[0] == b'-') {
        return 0;
    }
    let (Ok(h), Ok(m)) = (s[1..3].parse::<i32>(), s[3..5].parse::<i32>()) else {
        return 0;
    };
    let v = h * 3600 + m * 60;
    if b[0] == b'-' {
        -v
    } else {
        v
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn repo_with_big_blob() -> (tempfile::TempDir, gix::ObjectId, usize) {
        let dir = tempfile::tempdir().unwrap();
        let repo = gix::init_bare(dir.path()).unwrap();
        let big = vec![b'x'; 17 << 20];
        let id = repo.write_blob(&big).unwrap().detach();
        (dir, id, big.len())
    }

    /// A 16 MiB cap, as gix applies to a repository owned by another user,
    /// no longer stops the bridge from reading a larger object.
    #[test]
    fn objects_above_a_16_mib_cap_are_read() {
        let (dir, id, len) = repo_with_big_blob();
        let config = dir.path().join("config");
        let mut text = std::fs::read_to_string(&config).unwrap();
        text.push_str("[gitoxide \"objects\"]\n\tallocLimit = 16777216\n");
        std::fs::write(&config, text).unwrap();

        let plain = gix::open(dir.path()).unwrap();
        assert!(
            plain.find_object(id).is_err(),
            "the cap applies without the override"
        );
        assert_eq!(
            open_git(dir.path())
                .unwrap()
                .find_object(id)
                .unwrap()
                .data
                .len(),
            len
        );
    }

    /// Opened with reduced trust, the case of the server container reading
    /// a checkout owned by the host user.
    #[test]
    fn reduced_trust_reads_large_objects() {
        let (dir, id, len) = repo_with_big_blob();
        let reduced = gix::open::Options::default().with(gix::sec::Trust::Reduced);
        let repo = open_git_with(dir.path(), reduced).unwrap();
        assert_eq!(repo.find_object(id).unwrap().data.len(), len);
    }
}
