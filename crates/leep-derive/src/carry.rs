//! Whether a re-committed file makes the reviewed change (ADR 0089): the
//! text merges a carried verdict rests on when the file's base moved.
//!
//! A reviewed change of a file from `x_old` to `x_new`, made again by a
//! client on a base where the file is `y_old`, carries its verdict when the
//! client's result is
//!
//! - **rebased**: exactly what the server's own rebase of the reviewed
//!   change makes there, `diffy::merge(x_old, y_old, x_new)` with the
//!   server's options, which a clean server replay already trusts
//!   (ADR 0011); or
//! - **union**: the merge conflicts only where both sides inserted lines at
//!   the same place and deleted or edited nothing there, and the result
//!   keeps both insertions, `y_old`'s first in every region or `x_new`'s
//!   first in every region.
//!
//! Both are pure functions of three blobs, so outcomes are cached by their
//! hashes.

use std::collections::HashMap;
use std::sync::Mutex;

use diffy::{ConflictStyle, MergeOptions};
use leep_core::hash::is_object_preimage;
use leep_core::Hash;

/// How the judged changeset reproduces a reviewed path's change.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CarryRule {
    /// The same old and new contents (ADR 0085).
    Identical,
    /// The server's rebase of the reviewed change onto the new base.
    Rebased,
    /// Both sides' insertions at the same place, kept whole.
    Union,
}

impl CarryRule {
    /// `identical`, `rebased` or `union`, as `PathReview.carried_rule`.
    pub fn name(self) -> &'static str {
        match self {
            CarryRule::Identical => "identical",
            CarryRule::Rebased => "rebased",
            CarryRule::Union => "union",
        }
    }
}

/// What merging a reviewed change onto a moved base gives.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Merged {
    /// A clean merge, the server's rebase result.
    Clean(Hash),
    /// Conflicts only between pure insertions: the two unions.
    Union([Hash; 2]),
    /// Anything else: nothing carries.
    Neither,
}

impl Merged {
    /// How `new` (the judged changeset's content) matches, if it does.
    pub(crate) fn rule_for(self, new: Hash) -> Option<CarryRule> {
        match self {
            Merged::Clean(h) if h == new => Some(CarryRule::Rebased),
            Merged::Union(hs) if hs.contains(&new) => Some(CarryRule::Union),
            _ => None,
        }
    }
}

/// Merge `theirs` (the reviewed new text) and `ours` (the new base's text)
/// against `base` (the reviewed old text), as the server's rebase does.
pub(crate) fn merge_outcome(base: &str, ours: &str, theirs: &str) -> Merged {
    match diffy::merge(base, ours, theirs) {
        // The blob store refuses these (ADR 0041); the server's rebase
        // leaves them to the client.
        Ok(m) if is_object_preimage(m.as_bytes()) => Merged::Neither,
        Ok(m) => Merged::Clean(Hash::of_blob(m.as_bytes())),
        Err(_) => match union_of_insertions(base, ours, theirs) {
            Some([a, b]) => {
                Merged::Union([Hash::of_blob(a.as_bytes()), Hash::of_blob(b.as_bytes())])
            }
            None => Merged::Neither,
        },
    }
}

/// Longest run of one marker character at the start of a line that still
/// lets the union be computed; longer runs refuse it.
const MAX_MARKER_RUN: usize = 64;

/// The two results of keeping both sides of every conflict, `ours` first
/// and `theirs` first, when every conflict region of the merge has an empty
/// base part (both sides only inserted there). `None` when a region deletes
/// or edits a base line, when a text does not end in a newline, or when the
/// merge is clean (that is the rebased case).
pub(crate) fn union_of_insertions(base: &str, ours: &str, theirs: &str) -> Option<[String; 2]> {
    let texts = [base, ours, theirs];
    // An incomplete last line would put a marker on the same line.
    if texts.iter().any(|t| !t.is_empty() && !t.ends_with('\n')) {
        return None;
    }
    // Markers longer than any run in the texts, so no content line can be
    // read as one.
    let mut run = 0;
    for line in texts.iter().flat_map(|t| t.lines()) {
        for c in ['<', '|', '=', '>'] {
            run = run.max(line.chars().take_while(|&x| x == c).count());
        }
    }
    if run > MAX_MARKER_RUN {
        return None;
    }
    let len = (run + 1).max(7);
    let marked = MergeOptions::new()
        .set_conflict_style(ConflictStyle::Diff3)
        .set_conflict_marker_length(len)
        .merge(base, ours, theirs)
        .err()?;
    let marker = |c: char, label: &str| format!("{}{label}\n", c.to_string().repeat(len));
    let (open, mid, sep, close) = (
        marker('<', " ours"),
        marker('|', " original"),
        marker('=', ""),
        marker('>', " theirs"),
    );
    enum At {
        Out,
        Ours,
        Base,
        Theirs,
    }
    let (mut ours_first, mut theirs_first) = (String::new(), String::new());
    let (mut o, mut t) = (String::new(), String::new());
    let mut at = At::Out;
    for line in marked.split_inclusive('\n') {
        at = match at {
            At::Out if line == open => At::Ours,
            At::Out => {
                ours_first.push_str(line);
                theirs_first.push_str(line);
                At::Out
            }
            At::Ours if line == mid => At::Base,
            At::Ours => {
                o.push_str(line);
                At::Ours
            }
            // A base line in a conflict: one side deleted or edited it.
            At::Base if line == sep => At::Theirs,
            At::Base => return None,
            At::Theirs if line == close => {
                ours_first.push_str(&o);
                ours_first.push_str(&t);
                theirs_first.push_str(&t);
                theirs_first.push_str(&o);
                o.clear();
                t.clear();
                At::Out
            }
            At::Theirs => {
                t.push_str(line);
                At::Theirs
            }
        };
    }
    matches!(at, At::Out).then_some([ours_first, theirs_first])
}

/// Most merge outcomes kept; the cache starts again when full.
const CACHE_CAP: usize = 4096;

static CACHE: Mutex<Option<HashMap<[Hash; 3], Merged>>> = Mutex::new(None);

/// The cached outcome for (base, ours, theirs) content hashes.
pub(crate) fn cached(key: [Hash; 3]) -> Option<Merged> {
    let guard = CACHE.lock().unwrap_or_else(|e| e.into_inner());
    guard.as_ref().and_then(|m| m.get(&key).copied())
}

pub(crate) fn remember(key: [Hash; 3], outcome: Merged) {
    let mut guard = CACHE.lock().unwrap_or_else(|e| e.into_inner());
    let map = guard.get_or_insert_with(HashMap::new);
    if map.len() >= CACHE_CAP {
        map.clear();
    }
    map.insert(key, outcome);
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASE: &str = "fn a() {}\n\nmod tests {\n    fn t0() {}\n}\n";

    fn both(base: &str, ours: &str, theirs: &str) -> Option<[String; 2]> {
        union_of_insertions(base, ours, theirs)
    }

    #[test]
    fn two_appends_at_one_place_union_in_both_orders() {
        let ours = "fn a() {}\n\nmod tests {\n    fn t0() {}\n    fn cy() {}\n}\n";
        let theirs = "fn a() {}\n\nmod tests {\n    fn t0() {}\n    fn dee() {}\n}\n";
        let [o, t] = both(BASE, ours, theirs).unwrap();
        assert_eq!(
            o,
            "fn a() {}\n\nmod tests {\n    fn t0() {}\n    fn cy() {}\n    fn dee() {}\n}\n"
        );
        assert_eq!(
            t,
            "fn a() {}\n\nmod tests {\n    fn t0() {}\n    fn dee() {}\n    fn cy() {}\n}\n"
        );
        let m = merge_outcome(BASE, ours, theirs);
        assert_eq!(
            m.rule_for(Hash::of_blob(o.as_bytes())),
            Some(CarryRule::Union)
        );
        assert_eq!(
            m.rule_for(Hash::of_blob(t.as_bytes())),
            Some(CarryRule::Union)
        );
        assert_eq!(m.rule_for(Hash::of_blob(BASE.as_bytes())), None);
    }

    #[test]
    fn a_region_that_edits_a_base_line_is_no_union() {
        let ours = "fn a() {}\n\nmod tests {\n    fn t1() {}\n}\n";
        let theirs = "fn a() {}\n\nmod tests {\n    fn t2() {}\n}\n";
        assert_eq!(both(BASE, ours, theirs), None);
        assert_eq!(merge_outcome(BASE, ours, theirs), Merged::Neither);
    }

    #[test]
    fn a_clean_merge_is_the_rebased_case() {
        let ours = "fn a() { 1 }\n\nmod tests {\n    fn t0() {}\n}\n";
        let theirs = "fn a() {}\n\nmod tests {\n    fn t0() {}\n    fn t1() {}\n}\n";
        let merged = "fn a() { 1 }\n\nmod tests {\n    fn t0() {}\n    fn t1() {}\n}\n";
        assert_eq!(both(BASE, ours, theirs), None);
        let m = merge_outcome(BASE, ours, theirs);
        assert_eq!(m, Merged::Clean(Hash::of_blob(merged.as_bytes())));
        assert_eq!(
            m.rule_for(Hash::of_blob(merged.as_bytes())),
            Some(CarryRule::Rebased)
        );
    }

    #[test]
    fn marker_lookalikes_in_the_texts_do_not_confuse_the_union() {
        // Content lines that are default markers: the markers grow past them.
        let base = "a\n=======\nz\n";
        let ours = "a\n=======\n<<<<<<< ours\nz\n";
        let theirs = "a\n=======\n>>>>>>> theirs\nz\n";
        let [o, t] = both(base, ours, theirs).unwrap();
        assert_eq!(o, "a\n=======\n<<<<<<< ours\n>>>>>>> theirs\nz\n");
        assert_eq!(t, "a\n=======\n>>>>>>> theirs\n<<<<<<< ours\nz\n");
        // Runs too long to outgrow refuse.
        let long = format!("{}\n", "<".repeat(MAX_MARKER_RUN + 1));
        let ours = format!("a\n{long}z\n");
        assert_eq!(both("a\nz\n", &ours, "a\nq\nz\n"), None);
    }

    #[test]
    fn a_text_without_a_final_newline_is_no_union() {
        assert_eq!(both("a\nz", "a\nb\nz", "a\nc\nz"), None);
    }
}
