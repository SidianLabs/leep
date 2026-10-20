//! Turning a query into a matcher and a trigram requirement. The requirement
//! is a necessary condition: a file that fails it cannot contain a match, so
//! it can be skipped without being read. A weaker requirement only costs
//! speed; an unsound one would change results, so every rule here may drop
//! constraints but never add one.

use std::collections::BTreeSet;

use leep_core::api::{SearchMatch, SearchRange};
use leep_core::{Error, RepoPath, Result};
use regex::{Regex, RegexBuilder};
use regex_syntax::hir::{Class, Hir, HirKind};
use regex_syntax::ParserBuilder;

use super::trigram::{code, Probe};

pub const MAX_PATTERN_BYTES: usize = 1024;
/// Longest line returned, in bytes.
pub const MAX_LINE_BYTES: usize = 1024;
/// Trigrams a requirement may name; more only slows the filter down.
const MAX_LEAVES: usize = 96;
/// Exact strings tracked per regex node, and characters a class may expand to.
const MAX_EXACT: usize = 16;
const MAX_CLASS: usize = 8;
const MAX_VARIANTS: usize = 8;

/// A boolean condition over trigram membership.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Expr {
    /// No constraint.
    True,
    Tri(u32),
    And(Vec<Expr>),
    Or(Vec<Expr>),
}

impl Expr {
    fn and(parts: Vec<Expr>) -> Expr {
        let mut out: Vec<Expr> = Vec::new();
        for p in parts {
            match p {
                Expr::True => {}
                Expr::And(inner) => out.extend(inner),
                p => out.push(p),
            }
        }
        dedup_in_place(&mut out);
        match out.len() {
            0 => Expr::True,
            1 => out.pop().expect("one"),
            _ => Expr::And(out),
        }
    }

    fn or(parts: Vec<Expr>) -> Expr {
        let mut out: Vec<Expr> = Vec::new();
        for p in parts {
            match p {
                Expr::True => return Expr::True,
                Expr::Or(inner) => out.extend(inner),
                p => out.push(p),
            }
        }
        dedup_in_place(&mut out);
        match out.len() {
            0 => Expr::True,
            1 => out.pop().expect("one"),
            _ => Expr::Or(out),
        }
    }

    pub fn is_true(&self) -> bool {
        matches!(self, Expr::True)
    }

    /// Whether a set with membership test `has` satisfies the requirement.
    pub fn eval(&self, has: &impl Fn(u32) -> bool) -> bool {
        match self {
            Expr::True => true,
            Expr::Tri(t) => has(*t),
            Expr::And(v) => v.iter().all(|e| e.eval(has)),
            Expr::Or(v) => v.iter().any(|e| e.eval(has)),
        }
    }

    /// Every trigram the expression mentions.
    pub fn trigrams(&self) -> Vec<u32> {
        match self {
            Expr::True => Vec::new(),
            Expr::Tri(t) => vec![*t],
            Expr::And(v) | Expr::Or(v) => v.iter().flat_map(Expr::trigrams).collect(),
        }
    }

    pub fn leaves(&self) -> usize {
        match self {
            Expr::True => 0,
            Expr::Tri(_) => 1,
            Expr::And(v) | Expr::Or(v) => v.iter().map(Expr::leaves).sum(),
        }
    }

    /// Dropping conjuncts keeps the requirement necessary.
    fn limited(self) -> Expr {
        match self {
            Expr::And(mut v) => {
                while v.len() > 1 && v.iter().map(Expr::leaves).sum::<usize>() > MAX_LEAVES {
                    v.pop();
                }
                Expr::And(v)
            }
            e => e,
        }
    }
}

fn dedup_in_place(v: &mut Vec<Expr>) {
    let mut seen: Vec<Expr> = Vec::with_capacity(v.len());
    v.retain(|e| {
        if seen.contains(e) {
            false
        } else {
            seen.push(e.clone());
            true
        }
    });
}

/// All trigrams of `s` as a conjunction, spread evenly if there are too many.
fn all_trigrams(s: &[u8]) -> Expr {
    if s.len() < 3 {
        return Expr::True;
    }
    let n = s.len() - 2;
    let take = n.min(MAX_LEAVES);
    let idx = (0..take).map(|i| {
        if take == 1 {
            0
        } else {
            i * (n - 1) / (take - 1)
        }
    });
    Expr::and(
        idx.map(|i| Expr::Tri(code(s[i], s[i + 1], s[i + 2])))
            .collect(),
    )
}

/// A compiled query.
pub struct Compiled {
    regex: Regex,
    /// Necessary condition on the trigrams of a file that has a match.
    pub required: Expr,
    /// Reads the trigrams of `required` out of index rows.
    pub probe: Probe,
}

impl Compiled {
    pub fn new(pattern: &str, regex: bool, case_insensitive: bool) -> Result<Self> {
        if pattern.is_empty() {
            return Err(Error::Invalid("empty search query".into()));
        }
        if pattern.len() > MAX_PATTERN_BYTES {
            return Err(Error::Invalid(format!(
                "search query is longer than {MAX_PATTERN_BYTES} bytes"
            )));
        }
        if !regex && pattern.contains('\n') {
            return Err(Error::Invalid(
                "a literal query is matched within one line and cannot contain a newline".into(),
            ));
        }
        let source = if regex {
            pattern.to_string()
        } else {
            regex::escape(pattern)
        };
        let re = RegexBuilder::new(&source)
            .case_insensitive(case_insensitive)
            .size_limit(1 << 20)
            .build()
            .map_err(|e| Error::Invalid(format!("bad regex: {e}")))?;
        let required = if regex {
            regex_requirement(&source, case_insensitive)
        } else {
            literal_requirement(pattern, case_insensitive)
        };
        let required = required.limited();
        Ok(Compiled {
            regex: re,
            probe: Probe::new(required.trigrams()),
            required,
        })
    }

    /// Append the matching lines of `text`, at most `max` of them.
    pub fn scan(&self, path: &RepoPath, text: &str, max: usize, out: &mut Vec<SearchMatch>) {
        self.scan_after(path, text, 0, max, out)
    }

    /// [`Compiled::scan`] of the lines after line `skip` (1-based), which an
    /// earlier page covered.
    pub fn scan_after(
        &self,
        path: &RepoPath,
        text: &str,
        skip: u64,
        max: usize,
        out: &mut Vec<SearchMatch>,
    ) {
        let first = out.len();
        for (i, line) in text.lines().enumerate() {
            if (i as u64) < skip {
                continue;
            }
            if out.len() - first >= max {
                break;
            }
            if !self.regex.is_match(line) {
                continue;
            }
            let cut = cut_line(line);
            let ranges = self
                .regex
                .find_iter(line)
                .filter(|m| m.start() < cut.len() && !m.is_empty())
                .map(|m| SearchRange {
                    start: m.start() as u32,
                    end: m.end().min(cut.len()) as u32,
                })
                .collect();
            out.push(SearchMatch {
                path: path.clone(),
                line_number: i as u64 + 1,
                line: cut.to_string(),
                ranges,
            });
        }
    }
}

fn cut_line(line: &str) -> &str {
    if line.len() <= MAX_LINE_BYTES {
        return line;
    }
    let mut end = MAX_LINE_BYTES;
    while !line.is_char_boundary(end) {
        end -= 1;
    }
    &line[..end]
}

/// Every character the regex crate treats as equal to `c` when ignoring case.
fn case_variants(c: char) -> Vec<char> {
    let mut buf = [0u8; 4];
    let src = regex_syntax::escape(c.encode_utf8(&mut buf));
    let mut out = vec![c];
    if let Ok(hir) = ParserBuilder::new()
        .case_insensitive(true)
        .build()
        .parse(&src)
    {
        match hir.kind() {
            HirKind::Class(Class::Unicode(u)) => {
                for r in u.iter() {
                    out.extend((r.start() as u32..=r.end() as u32).filter_map(char::from_u32));
                }
            }
            HirKind::Literal(l) => out.extend(std::str::from_utf8(&l.0).unwrap_or("").chars()),
            _ => {}
        }
    }
    out.sort_unstable();
    out.dedup();
    out
}

fn literal_requirement(lit: &str, case_insensitive: bool) -> Expr {
    if !case_insensitive {
        return all_trigrams(lit.as_bytes());
    }
    // Windows of three characters, each satisfied by any case variant. The
    // first three bytes of a variant are enough: they are in the file
    // whenever that variant is.
    let chars: Vec<char> = lit.chars().collect();
    if chars.len() < 3 {
        return Expr::True;
    }
    let n = chars.len() - 2;
    let take = n.min(MAX_LEAVES / 4);
    let mut parts = Vec::new();
    for k in 0..take {
        let i = if take == 1 {
            0
        } else {
            k * (n - 1) / (take - 1)
        };
        let vars: Vec<Vec<char>> = chars[i..i + 3].iter().map(|&c| case_variants(c)).collect();
        if vars.iter().any(|v| v.len() > MAX_VARIANTS) {
            continue;
        }
        let mut alts = Vec::new();
        for &a in &vars[0] {
            for &b in &vars[1] {
                for &c in &vars[2] {
                    let mut s = String::new();
                    s.extend([a, b, c]);
                    let b = s.as_bytes();
                    alts.push(Expr::Tri(code(b[0], b[1], b[2])));
                }
            }
        }
        parts.push(Expr::or(alts));
    }
    Expr::and(parts)
}

enum Info {
    /// The node matches exactly one of these strings.
    Exact(BTreeSet<Vec<u8>>),
    /// The text the node matches satisfies this requirement.
    Req(Expr),
}

fn exact_expr(set: &BTreeSet<Vec<u8>>) -> Expr {
    Expr::or(set.iter().map(|s| all_trigrams(s)).collect())
}

fn to_expr(info: &Info) -> Expr {
    match info {
        Info::Exact(s) => exact_expr(s),
        Info::Req(e) => e.clone(),
    }
}

fn single(s: &[u8]) -> Info {
    Info::Exact(BTreeSet::from([s.to_vec()]))
}

fn cross(a: &BTreeSet<Vec<u8>>, b: &BTreeSet<Vec<u8>>) -> Option<BTreeSet<Vec<u8>>> {
    if a.len() * b.len() > MAX_EXACT {
        return None;
    }
    let mut out = BTreeSet::new();
    for x in a {
        for y in b {
            out.insert([x.as_slice(), y.as_slice()].concat());
        }
    }
    Some(out)
}

fn analyze(hir: &Hir) -> Info {
    match hir.kind() {
        HirKind::Empty | HirKind::Look(_) => single(b""),
        HirKind::Literal(l) => single(&l.0),
        HirKind::Class(Class::Unicode(u)) => {
            let n: usize = u.iter().map(|r| r.len()).sum();
            if n == 0 || n > MAX_CLASS {
                return Info::Req(Expr::True);
            }
            let mut set = BTreeSet::new();
            for r in u.iter() {
                for c in (r.start() as u32..=r.end() as u32).filter_map(char::from_u32) {
                    set.insert(c.to_string().into_bytes());
                }
            }
            Info::Exact(set)
        }
        HirKind::Class(Class::Bytes(b)) => {
            let n: usize = b.iter().map(|r| r.len()).sum();
            if n == 0 || n > MAX_CLASS {
                return Info::Req(Expr::True);
            }
            let mut set = BTreeSet::new();
            for r in b.iter() {
                for x in r.start()..=r.end() {
                    set.insert(vec![x]);
                }
            }
            Info::Exact(set)
        }
        HirKind::Repetition(r) if r.min >= 1 => Info::Req(to_expr(&analyze(&r.sub))),
        HirKind::Repetition(_) => Info::Req(Expr::True),
        HirKind::Capture(c) => analyze(&c.sub),
        HirKind::Concat(subs) => {
            let mut cur = BTreeSet::from([Vec::new()]);
            let mut reqs = Vec::new();
            for sub in subs {
                match analyze(sub) {
                    Info::Exact(b) => match cross(&cur, &b) {
                        Some(p) => cur = p,
                        None => {
                            reqs.push(exact_expr(&cur));
                            cur = b;
                        }
                    },
                    Info::Req(e) => {
                        reqs.push(exact_expr(&cur));
                        reqs.push(e);
                        cur = BTreeSet::from([Vec::new()]);
                    }
                }
            }
            if reqs.is_empty() {
                Info::Exact(cur)
            } else {
                reqs.push(exact_expr(&cur));
                Info::Req(Expr::and(reqs))
            }
        }
        HirKind::Alternation(subs) => {
            let infos: Vec<Info> = subs.iter().map(analyze).collect();
            let mut union = BTreeSet::new();
            for i in &infos {
                match i {
                    Info::Exact(s) if union.len() + s.len() <= MAX_EXACT => {
                        union.extend(s.iter().cloned())
                    }
                    _ => {
                        return Info::Req(Expr::or(infos.iter().map(to_expr).collect()));
                    }
                }
            }
            Info::Exact(union)
        }
    }
}

/// Requirement from the regex's syntax tree, parsed with the same flags the
/// matcher is built with. A pattern this cannot parse yields no requirement.
fn regex_requirement(source: &str, case_insensitive: bool) -> Expr {
    match ParserBuilder::new()
        .case_insensitive(case_insensitive)
        .build()
        .parse(source)
    {
        Ok(hir) => to_expr(&analyze(&hir)),
        Err(_) => Expr::True,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tri(s: &str) -> Expr {
        let b = s.as_bytes();
        Expr::Tri(code(b[0], b[1], b[2]))
    }

    fn req(p: &str, regex: bool, ci: bool) -> Expr {
        Compiled::new(p, regex, ci).unwrap().required
    }

    /// Whether text with exactly the trigrams of `text` passes.
    fn passes(e: &Expr, text: &str) -> bool {
        let set = super::super::trigram::extract(text.as_bytes());
        e.eval(&|t| set.binary_search(&t).is_ok())
    }

    #[test]
    fn literal_requires_each_trigram() {
        assert_eq!(
            req("hello", false, false),
            Expr::And(vec![tri("hel"), tri("ell"), tri("llo")])
        );
        assert_eq!(
            req("abab", false, false),
            Expr::And(vec![tri("aba"), tri("bab")])
        );
        assert_eq!(req("aaaa", false, false), tri("aaa"));
        assert!(req("ab", false, false).is_true());
        // Metacharacters are literal in literal mode.
        assert_eq!(req("a.c", false, false), tri("a.c"));
    }

    #[test]
    fn long_literals_are_capped_but_still_necessary() {
        let lit: String = (0..400)
            .map(|i| (b'a' + (i * 7 % 26) as u8) as char)
            .collect();
        let e = req(&lit, false, false);
        assert!(e.leaves() <= MAX_LEAVES);
        assert!(passes(&e, &lit));
        assert!(!passes(&e, "zzz"));
    }

    #[test]
    fn case_insensitive_literal_accepts_every_case() {
        let e = req("Hello", false, true);
        for t in ["hello", "HELLO", "hElLo", "xx Hello yy"] {
            assert!(passes(&e, t), "{t}");
        }
        assert!(!passes(&e, "help me"));
        // Unicode folds are covered: K (Kelvin sign) matches k.
        assert!(passes(&req("kit", false, true), "\u{212a}IT"));
        assert!(req("hi", false, true).is_true());
    }

    #[test]
    fn regex_literals_and_alternations() {
        assert_eq!(
            req("foo.*bar", true, false),
            Expr::And(vec![tri("foo"), tri("bar")])
        );
        let alt = req("foo|bar", true, false);
        assert!(passes(&alt, "xfoox") && passes(&alt, "bar") && !passes(&alt, "fob bat"));
        let nested = req("(foo|ba)x", true, false);
        assert!(passes(&nested, "foox") && passes(&nested, "bax") && !passes(&nested, "fox"));
        // A short branch makes the whole alternation unconstrained.
        assert!(req("foo|ab", true, false).is_true());
        let cls = req("ab[cd]ef", true, false);
        assert!(passes(&cls, "abcef") && passes(&cls, "abdef") && !passes(&cls, "abxef"));
        let rep = req("(abc)+d", true, false);
        assert!(passes(&rep, "abcabcd"));
        assert_eq!(
            req("fn\\s+main", true, false),
            Expr::And(vec![tri("mai"), tri("ain")])
        );
    }

    #[test]
    fn regex_without_required_trigrams_is_unconstrained() {
        for p in [
            ".*", "a*", "\\w+", "[a-z]+", "ab", "(a|b)c?", "^$", "\\d{3}", "x?yz?",
        ] {
            assert!(req(p, true, false).is_true(), "{p}");
        }
    }

    #[test]
    fn case_insensitive_regex_expands_classes() {
        let e = req("(?i)foo.*bar", true, false);
        assert!(passes(&e, "FOO and BAR") && passes(&e, "foo bar") && !passes(&e, "foo baz"));
        let e = req("foo.*bar", true, true);
        assert!(passes(&e, "FOO BAR") && !passes(&e, "FOO"));
    }

    #[test]
    fn invalid_queries_are_rejected() {
        for (p, re) in [("", false), ("(", true), ("a\nb", false), ("[z-a]", true)] {
            assert!(
                matches!(Compiled::new(p, re, false), Err(Error::Invalid(_))),
                "{p:?}"
            );
        }
        assert!(Compiled::new(&"a".repeat(MAX_PATTERN_BYTES + 1), false, false).is_err());
        // Huge repetition exceeds the compile budget instead of eating memory.
        assert!(Compiled::new("(a{1000}){1000}", true, false).is_err());
    }

    type Row = (u64, String, Vec<(u32, u32)>);

    fn scan(c: &Compiled, text: &str) -> Vec<Row> {
        let mut out = Vec::new();
        c.scan(&RepoPath::new("f").unwrap(), text, usize::MAX, &mut out);
        out.into_iter()
            .map(|m| {
                (
                    m.line_number,
                    m.line,
                    m.ranges.iter().map(|r| (r.start, r.end)).collect(),
                )
            })
            .collect()
    }

    #[test]
    fn scan_reports_lines_and_ranges() {
        let c = Compiled::new("ab", false, false).unwrap();
        assert_eq!(
            scan(&c, "xab ab\nnone\r\nab\r\n\nlast ab"),
            [
                (1, "xab ab".to_string(), vec![(1, 3), (4, 6)]),
                (3, "ab".to_string(), vec![(0, 2)]),
                (5, "last ab".to_string(), vec![(5, 7)]),
            ]
        );
        let c = Compiled::new("AB", false, true).unwrap();
        assert_eq!(scan(&c, "xab").len(), 1);
        // Matching is per line, so `\s` cannot reach across lines.
        let c = Compiled::new("a\\sb", true, false).unwrap();
        assert!(scan(&c, "a\nb").is_empty() && scan(&c, "a b").len() == 1);
        let c = Compiled::new("^b", true, false).unwrap();
        assert_eq!(scan(&c, "a\nb")[0].0, 2);
        // An empty match selects the line but has no range.
        let c = Compiled::new("x*", true, false).unwrap();
        assert_eq!(scan(&c, "abc"), [(1, "abc".to_string(), vec![])]);
    }

    #[test]
    fn scan_stops_at_max() {
        let c = Compiled::new("a", false, false).unwrap();
        let mut out = Vec::new();
        c.scan(&RepoPath::new("f").unwrap(), "a\na\na\n", 2, &mut out);
        assert_eq!(out.len(), 2);
        c.scan(&RepoPath::new("f").unwrap(), "a\na\na\n", 2, &mut out);
        assert_eq!(out.len(), 4);
    }

    #[test]
    fn long_lines_are_cut_on_a_char_boundary() {
        let line = format!("{}{}needle", "é".repeat(600), "z");
        let c = Compiled::new("é", false, false).unwrap();
        let m = scan(&c, &line);
        assert_eq!(m[0].1.len(), MAX_LINE_BYTES);
        assert!(m[0].2.iter().all(|&(_, e)| e as usize <= MAX_LINE_BYTES));
        let c = Compiled::new("needle", false, false).unwrap();
        let m = scan(&c, &line);
        assert_eq!(m[0].1.len(), MAX_LINE_BYTES);
        assert!(m[0].2.is_empty());
    }
}
