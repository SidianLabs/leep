//! Human-readable rendering helpers: dates, short ids and file diffs.

use leep_core::FileKind;
use similar::TextDiff;

pub fn short(id: impl std::fmt::Display) -> String {
    id.to_string().chars().take(12).collect()
}

/// `YYYY-MM-DD HH:MM:SS` in UTC for a unix timestamp.
/// `1 file`, `2 files`.
pub fn plural(n: usize, noun: &str) -> String {
    format!("{n} {noun}{}", if n == 1 { "" } else { "s" })
}

pub fn utc_time(ts: i64) -> String {
    let days = ts.div_euclid(86_400);
    let secs = ts.rem_euclid(86_400);
    // Howard Hinnant's civil_from_days.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!(
        "{y:04}-{m:02}-{d:02} {:02}:{:02}:{:02}",
        secs / 3600,
        secs % 3600 / 60,
        secs % 60
    )
}

fn kind_name(k: FileKind) -> &'static str {
    match k {
        FileKind::Regular => "regular",
        FileKind::Executable => "executable",
        FileKind::Symlink => "symlink",
    }
}

/// One side of a file diff: content and kind.
pub type Side<'a> = Option<(&'a [u8], FileKind)>;

/// Unified diff for one path; "Binary files differ" for non-UTF-8 or NUL data.
pub fn file_diff(path: &str, old: Side, new: Side) -> String {
    renamed_diff(path, path, old, new)
}

/// `renamed: old -> new (97%)`, the line `leep diff` and `leep status` print
/// for a detected rename.
pub fn rename_line(from: &str, to: &str, similarity: u8) -> String {
    format!("renamed: {from} -> {to} ({similarity}%)")
}

/// [`file_diff`] for a file that moved from `from` to `to`. An unchanged
/// content prints nothing, so an exact rename is just its `renamed:` line.
pub fn renamed_diff(from: &str, to: &str, old: Side, new: Side) -> String {
    let mut out = String::new();
    let a = old.map_or("/dev/null".to_string(), |_| format!("a/{from}"));
    let b = new.map_or("/dev/null".to_string(), |_| format!("b/{to}"));
    if let (Some((_, ok)), Some((_, nk))) = (old, new) {
        if ok != nk {
            out.push_str(&format!(
                "mode change {to}: {} -> {}\n",
                kind_name(ok),
                kind_name(nk)
            ));
        }
    }
    let old_bytes = old.map_or(&[][..], |(d, _)| d);
    let new_bytes = new.map_or(&[][..], |(d, _)| d);
    if old_bytes == new_bytes && old.is_some() && new.is_some() {
        return out;
    }
    let text = |d: &[u8]| -> Option<String> {
        if d.contains(&0) {
            None
        } else {
            String::from_utf8(d.to_vec()).ok()
        }
    };
    match (text(old_bytes), text(new_bytes)) {
        (Some(o), Some(n)) => {
            let diff = TextDiff::from_lines(&o, &n);
            out.push_str(
                &diff
                    .unified_diff()
                    .context_radius(3)
                    .header(&a, &b)
                    .to_string(),
            );
        }
        _ => out.push_str(&format!("Binary files {a} and {b} differ\n")),
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_time() {
        assert_eq!(utc_time(0), "1970-01-01 00:00:00");
        assert_eq!(utc_time(1_760_000_000), "2025-10-09 08:53:20");
    }

    #[test]
    fn text_and_binary_diffs() {
        let d = file_diff(
            "a.txt",
            Some((b"one\ntwo\n", FileKind::Regular)),
            Some((b"one\n2\n", FileKind::Regular)),
        );
        assert!(d.contains("--- a/a.txt") && d.contains("-two") && d.contains("+2"));
        let b = file_diff(
            "x.bin",
            Some((&[0, 1], FileKind::Regular)),
            Some((&[0, 2], FileKind::Regular)),
        );
        assert!(b.contains("Binary files"));
        let add = file_diff("n", None, Some((b"x\n", FileKind::Regular)));
        assert!(add.contains("--- /dev/null") && add.contains("+x"));
    }

    #[test]
    fn renamed_diffs_name_both_paths() {
        assert_eq!(rename_line("a", "d/b", 97), "renamed: a -> d/b (97%)");
        let same = Some((&b"one\n"[..], FileKind::Regular));
        assert_eq!(renamed_diff("a", "b", same, same), "");
        let d = renamed_diff("a", "b", same, Some((b"one\ntwo\n", FileKind::Regular)));
        assert!(
            d.contains("--- a/a\n+++ b/b\n") && d.contains("+two"),
            "{d}"
        );
        let m = renamed_diff(
            "run",
            "bin/run",
            same,
            Some((b"one\n", FileKind::Executable)),
        );
        assert_eq!(m, "mode change bin/run: regular -> executable\n");
    }
}
