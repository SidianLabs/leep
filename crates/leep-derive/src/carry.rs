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
//! hashes. Each merge is bounded by [`MAX_CARRY_LINES`], runs on a blocking
//! thread under a process-wide limit, and is computed once however many
//! callers ask for it at the same time.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex, OnceLock};

use diffy::{ConflictStyle, MergeOptions};
use leep_core::hash::is_object_preimage;
use leep_core::{Error, Hash, Result};
use tokio::sync::{OnceCell, Semaphore};

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

/// Most lines any of the three texts may have for a merge to be tried.
/// diffy's Myers diff costs O((N+M)·D) with no cutoff, so a size bound
/// alone (2 MiB of one-character lines) allows ~10^12 steps; this keeps the
/// worst case of each of the merge's two diffs near (2·10^4)² steps,
/// deterministically. Real source files are far shorter.
pub const MAX_CARRY_LINES: usize = 10_000;

/// Longest run of one marker character at the start of a line that still
/// lets the union be computed; longer runs refuse it.
const MAX_MARKER_RUN: usize = 64;

/// [`Merged`] before hashing.
#[derive(Debug, PartialEq, Eq)]
enum Texts {
    Clean(String),
    Union([String; 2]),
    Neither,
}

/// Merge `theirs` (the reviewed new text) and `ours` (the new base's text)
/// against `base` (the reviewed old text), as the server's rebase does.
pub(crate) fn merge_outcome(base: &str, ours: &str, theirs: &str) -> Merged {
    match merge_texts(base, ours, theirs) {
        Texts::Clean(m) => Merged::Clean(Hash::of_blob(m.as_bytes())),
        Texts::Union([a, b]) => {
            Merged::Union([Hash::of_blob(a.as_bytes()), Hash::of_blob(b.as_bytes())])
        }
        Texts::Neither => Merged::Neither,
    }
}

/// One diff3 merge, with markers longer than any run of marker characters
/// in the texts. A clean result is the server's rebase result byte for
/// byte: diffy's default options differ only in marker length, which shows
/// only in conflicts. A conflicted one is parsed for the unions.
fn merge_texts(base: &str, ours: &str, theirs: &str) -> Texts {
    let texts = [base, ours, theirs];
    let lines = |t: &str| t.bytes().filter(|&b| b == b'\n').count() + 1;
    if texts.iter().any(|t| lines(t) > MAX_CARRY_LINES) {
        return Texts::Neither;
    }
    let mut run = 0;
    for line in texts.iter().flat_map(|t| t.lines()) {
        for c in ['<', '|', '=', '>'] {
            run = run.max(line.chars().take_while(|&x| x == c).count());
        }
    }
    // An incomplete last line would put a marker on the same line.
    let union_ok = run <= MAX_MARKER_RUN && texts.iter().all(|t| t.is_empty() || t.ends_with('\n'));
    let len = if union_ok { (run + 1).max(7) } else { 7 };
    match MergeOptions::new()
        .set_conflict_style(ConflictStyle::Diff3)
        .set_conflict_marker_length(len)
        .merge(base, ours, theirs)
    {
        // The blob store refuses these (ADR 0041); the server's rebase
        // leaves them to the client.
        Ok(m) if is_object_preimage(m.as_bytes()) => Texts::Neither,
        Ok(m) => Texts::Clean(m),
        Err(marked) if union_ok => match parse_union(&marked, len) {
            Some(both) => Texts::Union(both),
            None => Texts::Neither,
        },
        Err(_) => Texts::Neither,
    }
}

/// The two results of keeping both sides of every conflict of `marked`,
/// `ours` first and `theirs` first, when every conflict region has an empty
/// base part (both sides only inserted there). `None` when a region deletes
/// or edits a base line. `len` is the marker length, longer than any run in
/// the texts, so no content line can be read as a marker.
fn parse_union(marked: &str, len: usize) -> Option<[String; 2]> {
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

/// Outcomes kept per generation: lookups promote from the previous one, and
/// a full current generation becomes the previous, so hot entries survive.
const CACHE_GENERATION: usize = 2048;

#[derive(Default)]
struct Cache {
    current: HashMap<[Hash; 3], Merged>,
    previous: HashMap<[Hash; 3], Merged>,
}

impl Cache {
    fn insert(&mut self, key: [Hash; 3], m: Merged) {
        if self.current.len() >= CACHE_GENERATION {
            self.previous = std::mem::take(&mut self.current);
        }
        self.current.insert(key, m);
    }
}

static CACHE: Mutex<Option<Cache>> = Mutex::new(None);

/// The cached outcome for (base, ours, theirs) content hashes.
pub(crate) fn cached(key: [Hash; 3]) -> Option<Merged> {
    let mut guard = CACHE.lock().unwrap_or_else(|e| e.into_inner());
    let cache = guard.get_or_insert_with(Cache::default);
    if let Some(m) = cache.current.get(&key) {
        return Some(*m);
    }
    let m = cache.previous.remove(&key)?;
    cache.insert(key, m);
    Some(m)
}

pub(crate) fn remember(key: [Hash; 3], outcome: Merged) {
    let mut guard = CACHE.lock().unwrap_or_else(|e| e.into_inner());
    guard
        .get_or_insert_with(Cache::default)
        .insert(key, outcome);
}

type Flight = Arc<OnceCell<Merged>>;

/// Merges being computed, so a second ask for the same contents waits for
/// the first instead of merging again.
static FLIGHTS: Mutex<Option<HashMap<[Hash; 3], Flight>>> = Mutex::new(None);

/// Merges running at once, on blocking threads shared with other work
/// (the filesystem blob store): half the cores, at least one.
fn merge_permits() -> Arc<Semaphore> {
    static PERMITS: OnceLock<Arc<Semaphore>> = OnceLock::new();
    PERMITS
        .get_or_init(|| {
            let cores = std::thread::available_parallelism().map_or(1, |n| n.get());
            Arc::new(Semaphore::new((cores / 2).max(1)))
        })
        .clone()
}

/// The outcome for `key` (the base, ours and theirs content hashes):
/// cached, or being computed by another caller, or computed here from the
/// texts `read` gives (`None`: a text is missing or not UTF-8, so no text
/// merge). Each key is merged once per process while cached, on a blocking
/// thread under [`merge_permits`]. A read error is returned and caches
/// nothing.
pub(crate) async fn merged<F, Fut>(key: [Hash; 3], read: F) -> Result<Merged>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<Option<[String; 3]>>>,
{
    if let Some(m) = cached(key) {
        return Ok(m);
    }
    let flight = {
        let mut guard = FLIGHTS.lock().unwrap_or_else(|e| e.into_inner());
        guard
            .get_or_insert_with(HashMap::new)
            .entry(key)
            .or_default()
            .clone()
    };
    let out = flight
        .get_or_try_init(|| async {
            let Some([base, ours, theirs]) = read().await? else {
                return Ok(Merged::Neither);
            };
            let permit = merge_permits()
                .acquire_owned()
                .await
                .map_err(|e| Error::Other(format!("merge permits: {e}")))?;
            // The permit moves into the task, so a caller that gives up
            // does not let another merge start beside this one.
            let m = tokio::task::spawn_blocking(move || {
                let _permit = permit;
                merge_outcome(&base, &ours, &theirs)
            })
            .await
            .unwrap_or(Merged::Neither);
            Ok::<_, Error>(m)
        })
        .await
        .copied();
    if let Ok(m) = out {
        remember(key, m);
    }
    let mut guard = FLIGHTS.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(map) = guard.as_mut() {
        if map.get(&key).is_some_and(|f| Arc::ptr_eq(f, &flight)) {
            map.remove(&key);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASE: &str = "fn a() {}\n\nmod tests {\n    fn t0() {}\n}\n";

    fn both(base: &str, ours: &str, theirs: &str) -> Option<[String; 2]> {
        match merge_texts(base, ours, theirs) {
            Texts::Union(u) => Some(u),
            _ => None,
        }
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

    #[test]
    fn a_clean_merge_equals_diffys_default_merge() {
        let ours = "fn a() { 1 }\n\nmod tests {\n    fn t0() {}\n}\n";
        let theirs = "fn a() {}\n\nmod tests {\n    fn t0() {}\n    fn t1() {}\n}\n";
        assert_eq!(
            merge_texts(BASE, ours, theirs),
            Texts::Clean(diffy::merge(BASE, ours, theirs).unwrap())
        );
        // Long markers in the texts change nothing in a clean result.
        let marks = "=======\n";
        let (b, o, t) = (
            format!("{marks}{BASE}"),
            format!("{marks}{ours}"),
            format!("{marks}{theirs}"),
        );
        assert_eq!(
            merge_texts(&b, &o, &t),
            Texts::Clean(diffy::merge(&b, &o, &t).unwrap())
        );
    }

    #[test]
    fn texts_over_the_line_budget_are_not_merged() {
        let base = "x\n".repeat(MAX_CARRY_LINES);
        let ours = format!("top\n{base}");
        let theirs = format!("{base}end\n");
        // Each is one line over, though the merge would be clean.
        assert!(diffy::merge(&base, &ours, &theirs).is_ok());
        assert_eq!(merge_outcome(&base, &ours, &theirs), Merged::Neither);
        let base = "x\n".repeat(MAX_CARRY_LINES - 2);
        let ours = format!("top\n{base}");
        let theirs = format!("{base}end\n");
        assert!(matches!(
            merge_outcome(&base, &ours, &theirs),
            Merged::Clean(_)
        ));
    }

    #[test]
    fn the_cache_keeps_hot_entries_across_a_generation() {
        let key = |i: usize| {
            let h = Hash::of_blob(format!("cache-test-{i}").as_bytes());
            [h, h, h]
        };
        remember(key(0), Merged::Neither);
        for i in 1..=CACHE_GENERATION * 3 / 2 {
            remember(key(i), Merged::Neither);
            // Asked for often: promoted each time it ages out.
            assert_eq!(cached(key(0)), Some(Merged::Neither), "{i}");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_asks_for_one_merge_compute_it_once() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let reads = Arc::new(AtomicUsize::new(0));
        let h = Hash::of_blob(b"single-flight-test");
        let key = [h, Hash::of_blob(b"sf-ours"), Hash::of_blob(b"sf-theirs")];
        let mut tasks = Vec::new();
        for _ in 0..16 {
            let reads = reads.clone();
            tasks.push(tokio::spawn(async move {
                merged(key, || async move {
                    reads.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    Ok(Some([
                        "a\nb\n".to_string(),
                        "A\nb\n".to_string(),
                        "a\nb\nc\n".to_string(),
                    ]))
                })
                .await
                .unwrap()
            }));
        }
        let want = Merged::Clean(Hash::of_blob(b"A\nb\nc\n"));
        for t in tasks {
            assert_eq!(t.await.unwrap(), want);
        }
        assert_eq!(reads.load(Ordering::SeqCst), 1);
        // A read error caches nothing; the next ask reads again.
        let k2 = [Hash::of_blob(b"sf-err"), h, h];
        let e = merged(k2, || async { Err(Error::Storage("down".into())) }).await;
        assert!(e.is_err());
        assert_eq!(cached(k2), None);
        let m = merged(k2, || async { Ok(None) }).await.unwrap();
        assert_eq!(m, Merged::Neither);
        assert_eq!(cached(k2), Some(Merged::Neither));
    }
}
