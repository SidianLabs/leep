//! Conflict marker labels. The server's three-way merge writes
//! `<<<<<<< ours` and `>>>>>>> theirs`, which say nothing about whose side is
//! whose, and when landing "ours" is the side already landed. The CLI
//! relabels them by role before writing the file (`docs/CLI.md`); the `/v1`
//! text and the `ours`/`theirs` fields are unchanged.
//!
//! The server writes diff3 style, with the common ancestor between the two
//! sides as `||||||| original`. Where that section is empty (both sides
//! added text where the base had none) it only adds noise, so the CLI drops
//! it; a non-empty one is relabelled `||||||| base`.
//!
//! Only the labels change. Conflict detection
//! ([`crate::status::is_marker_line`]) looks at the `<<<<<<< `, `||||||| `
//! and `>>>>>>> ` prefixes, so files with old and new labels are both
//! recognised.

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

/// Label of a non-empty base section.
const BASE_LABEL: &str = "||||||| base";

/// How to read a marker file, for the conflict report: which section is
/// which, and what resolving means.
pub fn explain(labels: &Labels) -> String {
    let (ours, theirs) = (&labels.ours, &labels.theirs);
    format!(
        "Each conflict holds both versions: from `<<<<<<< {ours}` to `=======` is {ours}, \
         from `=======` to `>>>>>>> {theirs}` is {theirs}; a `{BASE_LABEL}` section before \
         `=======`, if present, is the text both started from. Keep what you want and delete \
         the marker lines."
    )
}

/// `text` with the server's default labels replaced by `labels`, an empty
/// base section dropped and a non-empty one labelled `||||||| base`. Only
/// whole marker lines in marker order are touched (`<<<<<<< ours`, an
/// optional `|||||||` line, `=======`, `>>>>>>> theirs`), so file content
/// that merely looks like a marker elsewhere is left alone.
pub fn relabel(text: &str, labels: &Labels) -> String {
    #[derive(PartialEq)]
    enum In {
        Outside,
        Ours,
        /// Just after a base line, held back until it is known whether the
        /// section has any lines.
        BaseStart,
        Base,
        Theirs,
    }
    let mut state = In::Outside;
    let mut held = "";
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
            In::Ours | In::Base if body == SPLIT => {
                state = In::Theirs;
                None
            }
            In::BaseStart if body == SPLIT => {
                // Empty: the held base line is dropped.
                state = In::Theirs;
                None
            }
            In::Ours if body.starts_with(BASE) => {
                state = In::BaseStart;
                held = line;
                continue;
            }
            In::BaseStart => {
                state = In::Base;
                out.push_str(BASE_LABEL);
                out.push_str(if held.ends_with('\n') { "\n" } else { "" });
                None
            }
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
    if state == In::BaseStart {
        // Cut off after the base line: not a conflict the server wrote.
        out.push_str(held);
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
            "a\n<<<<<<< main (landed)\nfrom main\n||||||| base\nold\n=======\nmine\n>>>>>>> yours\nz\n"
        );
        let two = format!("{SERVER}{SERVER}");
        let got = relabel(&two, &Labels::new("yours", "abc123 (merging)"));
        assert_eq!(got.matches("<<<<<<< yours\n").count(), 2);
        assert_eq!(got.matches(">>>>>>> abc123 (merging)\n").count(), 2);
    }

    #[test]
    fn drops_an_empty_base_section() {
        let server = "<<<<<<< ours\nx\n||||||| original\n=======\ny\n>>>>>>> theirs\n";
        assert_eq!(
            relabel(server, &Labels::new("yours", "stash 1")),
            "<<<<<<< yours\nx\n=======\ny\n>>>>>>> stash 1\n"
        );
        // A base line with nothing after it is left as it was.
        let cut = "<<<<<<< ours\nx\n||||||| original";
        assert_eq!(
            relabel(cut, &Labels::landed("main")),
            "<<<<<<< main (landed)\nx\n||||||| original"
        );
        let text = explain(&Labels::landed("main"));
        assert!(text.contains("to `=======` is main (landed)"), "{text}");
        assert!(
            text.contains("`>>>>>>> yours` is yours") && text.contains("||||||| base"),
            "{text}"
        );
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
            assert!(text.lines().any(crate::status::is_marker_line));
        }
    }
}
