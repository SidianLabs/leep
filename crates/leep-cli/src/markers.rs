//! Conflict marker labels. The server's three-way merge writes
//! `<<<<<<< ours` and `>>>>>>> theirs`, which say nothing about whose side is
//! whose, and when landing "ours" is the side already landed. The CLI
//! relabels them by role before writing the file (`docs/CLI.md`); the `/v1`
//! text and the `ours`/`theirs` fields are unchanged.
//!
//! Only the label after the marker changes. Conflict detection
//! ([`crate::status::CONFLICT_MARKER`]) looks at the `<<<<<<< ` prefix, so
//! files with old and new labels are both recognised.

/// What the two sides of a conflict are called in the marker lines.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Labels {
    /// The `<<<<<<<` side: the server's `ours`.
    pub ours: String,
    /// The `>>>>>>>` side: the server's `theirs`.
    pub theirs: String,
}

impl Labels {
    pub fn new(ours: impl Into<String>, theirs: impl Into<String>) -> Self {
        Labels {
            ours: ours.into(),
            theirs: theirs.into(),
        }
    }

    /// `land` and `sync`: the target ref's head against your changes.
    pub fn landed(target_ref: &str) -> Self {
        Labels::new(format!("{target_ref} (landed)"), "yours")
    }
}

const OURS: &str = "<<<<<<< ours";
const BASE: &str = "||||||| ";
const SPLIT: &str = "=======";
const THEIRS: &str = ">>>>>>> theirs";

/// `text` with the server's default labels replaced by `labels`. Only whole
/// marker lines in marker order are touched (`<<<<<<< ours`, an optional
/// `|||||||` line, `=======`, `>>>>>>> theirs`), so file content that merely
/// looks like a marker elsewhere is left alone.
pub fn relabel(text: &str, labels: &Labels) -> String {
    #[derive(PartialEq)]
    enum In {
        Outside,
        Ours,
        Theirs,
    }
    let mut state = In::Outside;
    let mut out = String::with_capacity(text.len() + 64);
    for line in text.split_inclusive('\n') {
        let (body, end) = match line.strip_suffix('\n') {
            Some(b) => (b, "\n"),
            None => (line, ""),
        };
        let replaced = match state {
            In::Outside if body == OURS => {
                state = In::Ours;
                Some(format!("<<<<<<< {}", labels.ours))
            }
            In::Ours if body == SPLIT => {
                state = In::Theirs;
                None
            }
            In::Ours if body.starts_with(BASE) => None,
            In::Theirs if body == THEIRS => {
                state = In::Outside;
                Some(format!(">>>>>>> {}", labels.theirs))
            }
            _ => None,
        };
        match replaced {
            Some(r) => {
                out.push_str(&r);
                out.push_str(end);
            }
            None => out.push_str(line),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const SERVER: &str =
        "a\n<<<<<<< ours\nfrom main\n||||||| original\nold\n=======\nmine\n>>>>>>> theirs\nz\n";

    #[test]
    fn labels_sides_by_role() {
        let got = relabel(SERVER, &Labels::landed("main"));
        assert_eq!(
            got,
            "a\n<<<<<<< main (landed)\nfrom main\n||||||| original\nold\n=======\nmine\n>>>>>>> yours\nz\n"
        );
        let two = format!("{SERVER}{SERVER}");
        let got = relabel(&two, &Labels::new("yours", "abc123 (merging)"));
        assert_eq!(got.matches("<<<<<<< yours\n").count(), 2);
        assert_eq!(got.matches(">>>>>>> abc123 (merging)\n").count(), 2);
    }

    #[test]
    fn leaves_lookalikes_and_other_labels_alone() {
        // A stray marker line outside a conflict, and no trailing newline.
        let text = ">>>>>>> theirs\nx\n<<<<<<< ours\na\n=======\nb\n>>>>>>> theirs";
        let got = relabel(text, &Labels::landed("main"));
        assert_eq!(
            got,
            ">>>>>>> theirs\nx\n<<<<<<< main (landed)\na\n=======\nb\n>>>>>>> yours"
        );
        let custom = "<<<<<<< HEAD\na\n=======\nb\n>>>>>>> other\n";
        assert_eq!(relabel(custom, &Labels::landed("main")), custom);
        assert_eq!(relabel("", &Labels::landed("main")), "");
    }

    #[test]
    fn detection_accepts_old_and_new_labels() {
        let new = relabel(SERVER, &Labels::landed("main"));
        for text in [SERVER, new.as_str()] {
            assert!(text
                .lines()
                .any(|l| l.starts_with(crate::status::CONFLICT_MARKER)));
        }
    }
}
