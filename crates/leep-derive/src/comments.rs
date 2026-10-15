//! Comment stripping for the `comment_only` review rule (ADR 0011).
//!
//! A small lexer, not a parser. It finds comments outside string literals and
//! returns the code with comments removed and whitespace normalized, so two
//! versions can be compared. It must never call a code change a comment
//! change, so anything it cannot classify with confidence (an unterminated
//! string, a quote it cannot read, a heredoc, a language construct it does not
//! model) makes it give up with `None`, and the rule does not apply.
//!
//! Whitespace is normalized, not removed: a run containing a newline becomes
//! one newline plus the indentation of the line it leads to, any other run
//! becomes one space, and a comment counts as whitespace. That keeps token
//! boundaries (`ab` is not `a/**/b`), indentation (Python, YAML) and line
//! structure (Go, JavaScript and Kotlin treat newlines as statement ends).

#[derive(Clone, Copy, PartialEq, Eq)]
enum Block {
    None,
    Flat,
    Nested,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Single {
    /// `'x'` only; anything else is ambiguous.
    Char,
    /// Rust: a char literal, or else a lifetime (plain code).
    RustChar,
    Str {
        escapes: bool,
        multiline: bool,
    },
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Triple {
    None,
    /// Python: `"""` and `'''`, backslash escapes.
    Escaped,
    /// TOML: `"""` with escapes, `'''` literal.
    Toml,
}

/// How a language interpolates code into string literals. The expression
/// inside may contain quotes in all of these (`f"{d["k"]}"`, `"${m["k"]}"`,
/// `"$(echo "x")"`), so a string ends later than its first unescaped quote.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Interp {
    None,
    /// Python f- and t-strings: `{expr}`, `{{` and `}}` are literal.
    Python,
    /// C# `$"..."`: `{expr}`, `{{` and `}}` are literal.
    CSharp,
    /// Kotlin and Scala: `${expr}`.
    Dollar,
    /// Swift: `\(expr)`.
    Swift,
    /// Ruby: `#{expr}`.
    Ruby,
    /// Shell double quotes: `$(cmd)`, `$[expr]` and `${param}`.
    Shell,
}

#[derive(Clone, Copy)]
struct Lang {
    /// `#` line comments instead of `//`.
    hash: bool,
    block: Block,
    single: Single,
    triple: Triple,
    dq_multiline: bool,
    /// `#` starts a comment only at the start of a line or after whitespace.
    hash_after_space: bool,
    raw_rust: bool,
    yaml: bool,
    js: bool,
    jsx: bool,
    unicode_escapes: bool,
    heredoc: bool,
    shell: bool,
    ruby: bool,
    perl: bool,
    cpp: bool,
    csharp: bool,
    swift: bool,
    interp: Interp,
    /// Rust: doc comments are attributes, and their code blocks are tests.
    rust_doc: bool,
    /// C preprocessor line splices (`\` + newline) and trigraphs.
    splices: bool,
    /// Go: a comment before `import "C"` is C source for cgo.
    cgo: bool,
}

const BASE: Lang = Lang {
    hash: false,
    block: Block::None,
    single: Single::Char,
    triple: Triple::None,
    dq_multiline: false,
    hash_after_space: false,
    raw_rust: false,
    yaml: false,
    js: false,
    jsx: false,
    unicode_escapes: false,
    heredoc: false,
    shell: false,
    ruby: false,
    perl: false,
    cpp: false,
    csharp: false,
    swift: false,
    interp: Interp::None,
    rust_doc: false,
    splices: false,
    cgo: false,
};

const C_LIKE: Lang = Lang {
    block: Block::Flat,
    ..BASE
};

const JS: Lang = Lang {
    single: Single::Str {
        escapes: true,
        multiline: false,
    },
    js: true,
    ..C_LIKE
};

const SCRIPT_HASH: Lang = Lang {
    hash: true,
    hash_after_space: true,
    dq_multiline: true,
    ..BASE
};

fn language(ext: &str) -> Option<Lang> {
    Some(match ext {
        "rs" => Lang {
            rust_doc: true,
            block: Block::Nested,
            single: Single::RustChar,
            raw_rust: true,
            dq_multiline: true,
            ..BASE
        },
        "c" | "h" | "cc" | "cpp" | "hpp" => Lang {
            cpp: true,
            splices: true,
            ..C_LIKE
        },
        "java" => Lang {
            unicode_escapes: true,
            ..C_LIKE
        },
        "js" | "ts" => JS,
        "jsx" | "tsx" => Lang { jsx: true, ..JS },
        "go" => Lang {
            cgo: true,
            ..C_LIKE
        },
        "swift" => Lang {
            interp: Interp::Swift,
            block: Block::Nested,
            swift: true,
            ..BASE
        },
        "kt" => Lang {
            interp: Interp::Dollar,
            block: Block::Nested,
            ..BASE
        },
        "scala" => Lang {
            interp: Interp::Dollar,
            block: Block::Nested,
            unicode_escapes: true,
            ..BASE
        },
        "cs" => Lang {
            interp: Interp::CSharp,
            csharp: true,
            ..C_LIKE
        },
        "py" => Lang {
            interp: Interp::Python,
            hash: true,
            single: Single::Str {
                escapes: true,
                multiline: false,
            },
            triple: Triple::Escaped,
            ..BASE
        },
        "sh" | "bash" => Lang {
            single: Single::Str {
                escapes: false,
                multiline: true,
            },
            heredoc: true,
            interp: Interp::Shell,
            shell: true,
            ..SCRIPT_HASH
        },
        "rb" => Lang {
            single: Single::Str {
                escapes: true,
                multiline: true,
            },
            heredoc: true,
            interp: Interp::Ruby,
            ruby: true,
            ..SCRIPT_HASH
        },
        "pl" => Lang {
            single: Single::Str {
                escapes: true,
                multiline: true,
            },
            heredoc: true,
            perl: true,
            ..SCRIPT_HASH
        },
        "toml" => Lang {
            hash: true,
            single: Single::Str {
                escapes: false,
                multiline: false,
            },
            triple: Triple::Toml,
            ..BASE
        },
        "yaml" | "yml" => Lang {
            single: Single::Str {
                escapes: false,
                multiline: true,
            },
            yaml: true,
            ..SCRIPT_HASH
        },
        _ => return None,
    })
}

fn is_ws(c: char) -> bool {
    c.is_ascii_whitespace() || c == '\u{b}'
}

fn is_ident(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

/// Comments that change behavior (build tags, lint and type pragmas, coding
/// declarations) are kept as code, so editing them is not a comment change.
fn is_directive(body: &str, hash: bool) -> bool {
    let b = body.trim_start_matches(['/', '*', '!', '#', ' ', '\t']);
    const PREFIXES: &[&str] = &[
        "go:",
        "+build",
        "@",
        "<reference",
        "line ",
        "nolint",
        "NOLINT",
        "eslint",
        "prettier",
        "istanbul",
        "rustfmt",
        "clippy",
        "noqa",
        "type:",
        "pylint",
        "mypy",
        "pragma",
        "-*-",
        "coding",
        "encoding",
        "frozen_string_literal",
        "vim:",
        "shellcheck",
        "yaml-language-server",
        "webpack",
        "sourceMappingURL",
        "swift-tools-version",
        "license",
        "License",
        "SPDX",
    ];
    (!hash && body.starts_with('#')) || PREFIXES.iter().any(|p| b.starts_with(p))
}

/// Characters that end a line in some language, or that can make a comment
/// look like code to a reader.
fn suspicious_in_comment(c: char, next: Option<char>, lang: &Lang) -> bool {
    matches!(
        c,
        '\u{85}' | '\u{2028}' | '\u{2029}' | '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}'
    ) || (c == '\r' && next != Some('\n'))
        || (lang.unicode_escapes && c == '\\' && next == Some('u'))
}

struct Lexer<'a> {
    chars: Vec<char>,
    lang: &'a Lang,
    out: String,
    seen_ws: bool,
    newline: bool,
    indent: String,
}

impl Lexer<'_> {
    fn at(&self, i: usize) -> Option<char> {
        self.chars.get(i).copied()
    }

    fn starts(&self, i: usize, s: &str) -> bool {
        s.chars()
            .enumerate()
            .all(|(k, c)| self.at(i + k) == Some(c))
    }

    fn break_ws(&mut self, newline: bool) {
        self.seen_ws = true;
        if newline {
            self.newline = true;
            self.indent.clear();
        }
    }

    /// Emit pending whitespace in normalized form, then `s`. Whitespace at
    /// the start of the file is dropped.
    fn token(&mut self, s: &str) {
        if self.seen_ws && !self.out.is_empty() {
            if self.newline {
                self.out.push('\n');
                self.out.push_str(&self.indent);
            } else {
                self.out.push(' ');
            }
        }
        self.seen_ws = false;
        self.newline = false;
        self.indent.clear();
        self.out.push_str(s);
    }

    fn slice(&self, from: usize, to: usize) -> String {
        self.chars[from..to].iter().collect()
    }

    /// The last code character emitted, if any.
    fn prev_code(&self) -> Option<char> {
        self.out.chars().rev().find(|c| !is_ws(*c))
    }

    fn prev_word(&self) -> String {
        let mut w: Vec<char> = self
            .out
            .chars()
            .rev()
            .skip_while(|c| is_ws(*c))
            .take_while(|c| is_ident(*c))
            .collect();
        w.reverse();
        w.into_iter().collect()
    }

    fn line_start_or_space_before(&self, i: usize) -> bool {
        i == 0 || is_ws(self.chars[i - 1])
    }

    /// End of a line comment starting at `i` (index of the newline or EOF).
    fn line_end(&self, i: usize) -> Option<usize> {
        let mut j = i;
        while let Some(c) = self.at(j) {
            if c == '\n' || (c == '\r' && self.at(j + 1) == Some('\n')) {
                break;
            }
            if suspicious_in_comment(c, self.at(j + 1), self.lang) || self.splice_at(j) {
                return None;
            }
            j += 1;
        }
        Some(j)
    }

    /// End (exclusive) of a block comment starting at `i`.
    fn block_end(&self, i: usize) -> Option<(usize, bool)> {
        let nested = self.lang.block == Block::Nested;
        let (mut j, mut depth, mut nl) = (i + 2, 1, false);
        loop {
            let c = self.at(j)?;
            if suspicious_in_comment(c, self.at(j + 1), self.lang) || self.splice_at(j) {
                return None;
            }
            if nested && c == '/' && self.at(j + 1) == Some('*') {
                depth += 1;
                j += 2;
            } else if c == '*' && self.at(j + 1) == Some('/') {
                depth -= 1;
                j += 2;
                if depth == 0 {
                    return Some((j, nl));
                }
            } else {
                nl |= c == '\n';
                j += 1;
            }
        }
    }

    /// A C line splice starts at `i`: a backslash, optional blanks (GCC
    /// accepts them), then a newline. The preprocessor deletes it before
    /// anything else, so it can join `*` and `/` into the end of a comment,
    /// or carry a line comment onto the next line.
    fn splice_at(&self, i: usize) -> bool {
        if !self.lang.splices || self.at(i) != Some('\\') {
            return false;
        }
        let mut j = i + 1;
        while matches!(self.at(j), Some(' ' | '\t')) {
            j += 1;
        }
        matches!(self.at(j), Some('\n')) || self.starts(j, "\r\n")
    }

    /// Whether the `)` just emitted may close the head of `if`, `while`,
    /// `for` or `with`, after which a `/` starts a regex. A quote between
    /// the parentheses makes the count untrustworthy, so it answers yes.
    fn paren_closes_statement_head(&self) -> bool {
        let out: Vec<char> = self.out.chars().collect();
        let mut depth = 0usize;
        for k in (0..out.len()).rev() {
            match out[k] {
                '"' | '\'' | '`' => return true,
                ')' => depth += 1,
                '(' => {
                    let Some(d) = depth.checked_sub(1) else {
                        return true;
                    };
                    depth = d;
                    if depth == 0 {
                        let mut word: Vec<char> = out[..k]
                            .iter()
                            .rev()
                            .skip_while(|c| is_ws(**c))
                            .take_while(|c| is_ident(**c))
                            .copied()
                            .collect();
                        word.reverse();
                        let word: String = word.into_iter().collect();
                        return matches!(word.as_str(), "if" | "while" | "for" | "with");
                    }
                }
                _ => {}
            }
        }
        true
    }

    fn rest_of_line_has(&self, from: usize, c: char) -> bool {
        self.chars[from.min(self.chars.len())..]
            .iter()
            .take_while(|&&x| x != '\n')
            .any(|&x| x == c)
    }

    /// Whether the string literal `chars[start..end]`, with `q` quote
    /// characters at each end, really ends where the lexer ended it. With
    /// interpolation the expression inside may hold quotes, so the language
    /// keeps reading past what looks like the closing quote. Accept only
    /// literals whose interpolated expressions all close and contain no
    /// quote, escape or comment marker, so the bracket count can be trusted.
    fn interp_ok(&self, start: usize, end: usize, q: usize) -> bool {
        let quote = self.chars[start];
        let kind = self.lang.interp;
        let mut braces_escape = false;
        match kind {
            Interp::None => return true,
            Interp::Python => {
                let mut k = start;
                while k > 0 && self.chars[k - 1].is_ascii_alphabetic() {
                    k -= 1;
                }
                let prefix = &self.chars[k..start];
                let fstring = !prefix.is_empty()
                    && (k == 0 || !is_ident(self.chars[k - 1]))
                    && prefix.iter().all(|c| "rRbBuUfFtT".contains(*c))
                    && prefix.iter().any(|c| "fFtT".contains(*c));
                if !fstring {
                    return true;
                }
                braces_escape = true;
            }
            Interp::CSharp => {
                let mut k = start;
                while k > 0 && self.chars[k - 1] == '$' {
                    k -= 1;
                }
                if k == start {
                    return true;
                }
                if k > 0 && self.chars[k - 1] == '@' {
                    return false;
                }
                braces_escape = true;
            }
            _ if quote != '"' => return true,
            _ => {}
        }
        let body = &self.chars[start + q..end - q];
        let at = |i: usize| body.get(i).copied();
        let (mut i, mut depth, mut extra): (usize, usize, &[char]) = (0, 0, &[]);
        while i < body.len() {
            let c = body[i];
            if depth == 0 {
                let opener = match (kind, c, at(i + 1)) {
                    (Interp::Python | Interp::CSharp, '{' | '}', n) if n == Some(c) => {
                        i += 2;
                        continue;
                    }
                    (Interp::Python | Interp::CSharp, '}', _) => return false,
                    (Interp::Python, '{', _) => Some((1, &['#'][..])),
                    (Interp::CSharp, '{', _) => Some((1, &['/'][..])),
                    (Interp::Dollar, '$', Some('{')) => Some((2, &['/'][..])),
                    (Interp::Swift, '\\', Some('(')) => Some((2, &['/'][..])),
                    (Interp::Ruby, '#', Some('{')) => Some((2, &['#', '/'][..])),
                    (Interp::Shell, '$', Some('(' | '[')) => Some((2, &['#'][..])),
                    (Interp::Shell, '$', Some('{')) => Some((2, &[][..])),
                    _ => None,
                };
                if let Some((len, forbid)) = opener {
                    depth = 1;
                    extra = forbid;
                    i += len;
                } else if c == '\\' {
                    i += if braces_escape && matches!(at(i + 1), Some('{' | '}')) {
                        1
                    } else {
                        2
                    };
                } else {
                    i += 1;
                }
                continue;
            }
            if matches!(c, '"' | '\'' | '`' | '\\') || extra.contains(&c) {
                return false;
            }
            match c {
                '{' | '(' | '[' => depth += 1,
                '}' | ')' | ']' => depth -= 1,
                _ => {}
            }
            i += 1;
        }
        depth == 0
    }

    /// End (exclusive) of a quoted string whose opening quote is at `i`.
    fn quoted_end(&self, i: usize, quote: char, escapes: bool, multiline: bool) -> Option<usize> {
        let mut j = i + 1;
        loop {
            let c = self.at(j)?;
            if escapes && c == '\\' {
                self.at(j + 1)?;
                j += 2;
            } else if c == quote && self.lang.yaml && quote == '\'' && self.at(j + 1) == Some('\'')
            {
                // YAML writes a quote inside single quotes by doubling it.
                j += 2;
            } else if c == quote {
                return Some(j + 1);
            } else if c == '\n' && !multiline {
                return None;
            } else {
                j += 1;
            }
        }
    }

    fn triple_end(&self, i: usize, quote: char, escapes: bool) -> Option<usize> {
        let mut j = i + 3;
        loop {
            let c = self.at(j)?;
            if escapes && c == '\\' {
                self.at(j + 1)?;
                j += 2;
            } else if c == quote && self.at(j + 1) == Some(quote) && self.at(j + 2) == Some(quote) {
                return Some(j + 3);
            } else {
                j += 1;
            }
        }
    }

    /// End of a char literal opening at `i`, if it is one.
    fn char_literal_end(&self, i: usize) -> Option<usize> {
        let first = self.at(i + 1)?;
        if first == '\\' {
            // An escape is one character, or after `x`, `u`, `U` or an octal
            // digit a short run of digits and braces. Nothing else may sit
            // between the backslash and the closing quote: scanning for a
            // quote anywhere nearby would hide `//` or `"` inside a "literal".
            let mut j = i + 3;
            if matches!(self.at(i + 2)?, 'x' | 'u' | 'U' | '0'..='7') {
                let run = |c: char| c.is_ascii_hexdigit() || matches!(c, '{' | '}' | '_');
                while j < i + 14 && self.at(j).is_some_and(run) {
                    j += 1;
                }
            }
            return (self.at(j) == Some('\'')).then_some(j + 1);
        }
        if first == '\n' || first == '\'' {
            return None;
        }
        (self.at(i + 2) == Some('\'')).then_some(i + 3)
    }

    fn raw_string_end(&self, i: usize) -> Option<Option<usize>> {
        let prev_ok = match i.checked_sub(1).map(|p| self.chars[p]) {
            None => true,
            Some('b') => i < 2 || !is_ident(self.chars[i - 2]),
            Some(c) => !is_ident(c),
        };
        if !prev_ok {
            return None;
        }
        let mut j = i + 1;
        while self.at(j) == Some('#') {
            j += 1;
        }
        if self.at(j) != Some('"') {
            return None;
        }
        let hashes = j - i - 1;
        let mut k = j + 1;
        loop {
            match self.at(k) {
                None => return Some(None),
                Some('"') if (1..=hashes).all(|h| self.at(k + h) == Some('#')) => {
                    return Some(Some(k + 1 + hashes));
                }
                Some(_) => k += 1,
            }
        }
    }

    /// YAML starts a quoted scalar only where a scalar can start.
    fn yaml_scalar_start(&self, i: usize) -> bool {
        let mut k = i;
        let mut skipped = false;
        while k > 0 && matches!(self.chars[k - 1], ' ' | '\t') {
            k -= 1;
            skipped = true;
        }
        if k == 0 || self.chars[k - 1] == '\n' {
            return true;
        }
        match self.chars[k - 1] {
            ',' | '[' | '{' => true,
            ':' | '-' | '?' => skipped,
            _ => false,
        }
    }

    /// A YAML block scalar header (`|`, `>`, with indicators) ends its line.
    fn yaml_block_scalar(&self, i: usize) -> bool {
        let mut j = i + 1;
        while matches!(self.at(j), Some('+' | '-' | '0'..='9')) {
            j += 1;
        }
        while matches!(self.at(j), Some(' ' | '\t')) {
            j += 1;
        }
        matches!(self.at(j), None | Some('\n' | '\r' | '#'))
    }

    fn lex(mut self) -> Option<String> {
        let lang = *self.lang;
        let n = self.chars.len();
        let mut i = 0;
        while i < n {
            let c = self.chars[i];
            let next = self.at(i + 1);
            if is_ws(c) {
                if c == '\r' && next != Some('\n') {
                    return None;
                }
                // Neither the shell nor YAML counts these as blanks: they are
                // ordinary word and scalar characters, and a `#` after one is
                // not a comment. Normalizing them to a space would equate
                // `a<FF>b` with `a b`.
                if matches!(c, '\u{b}' | '\u{c}') && (lang.shell || lang.yaml) {
                    return None;
                }
                if c == '\n' {
                    self.break_ws(true);
                } else if c != '\r' {
                    self.seen_ws = true;
                    if self.newline {
                        self.indent.push(c);
                    }
                }
                i += 1;
                continue;
            }
            if c == '`' {
                return None;
            }

            // Shebang: behavior, not commentary.
            if lang.hash && i == 0 && c == '#' && next == Some('!') {
                let end = self.line_end(i)?;
                let s = self.slice(i, end);
                self.token(&s);
                i = end;
                continue;
            }

            if lang.splices
                && self.splice_at(i)
                && !(i > 0 && matches!(self.chars[i - 1], ' ' | '\t'))
            {
                return None;
            }
            // Ruby `=begin` and Perl POD blocks are comments the lexer does
            // not model; a quote inside one would desynchronize it.
            if (lang.ruby || lang.perl)
                && c == '='
                && (i == 0 || self.chars[i - 1] == '\n')
                && next.is_some_and(|n| n.is_ascii_alphabetic())
            {
                return None;
            }
            if c == '#' && lang.hash_after_space && !self.line_start_or_space_before(i) {
                let prev = self.chars[i - 1];
                // A shell word starts after an operator too (`a;#c` is a
                // comment); Ruby and Perl start a comment at any `#` outside
                // a literal, except Perl's `$#array`.
                if (lang.shell && ";&|()<>".contains(prev))
                    || lang.ruby
                    || (lang.perl && prev != '$')
                {
                    return None;
                }
            }
            let line_start = if lang.hash {
                c == '#' && (!lang.hash_after_space || self.line_start_or_space_before(i))
            } else {
                c == '/' && next == Some('/')
            };
            if line_start {
                let end = self.line_end(i)?;
                let body = self.slice(i + if lang.hash { 1 } else { 2 }, end);
                let magic_head =
                    lang.hash && self.chars[..i].iter().filter(|c| **c == '\n').count() < 2;
                let rust_doc = lang.rust_doc
                    && ((body.starts_with('/') && !body.starts_with("//"))
                        || body.starts_with('!'));
                if rust_doc
                    || is_directive(&body, lang.hash)
                    || (magic_head && body.contains("coding"))
                {
                    let s = self.slice(i, end);
                    self.token(&s);
                } else {
                    self.seen_ws = true;
                }
                i = end;
                continue;
            }
            if lang.block != Block::None && c == '/' && next == Some('*') {
                let (end, nl) = self.block_end(i)?;
                let body = self.slice(i + 2, end - 2);
                let rust_doc = lang.rust_doc
                    && ((body.starts_with('*') && !body.starts_with("**") && body != "*")
                        || body.starts_with('!'));
                if rust_doc || is_directive(&body, false) {
                    let s = self.slice(i, end);
                    self.token(&s);
                } else {
                    self.break_ws(nl);
                }
                i = end;
                continue;
            }

            match c {
                '"' => {
                    if self.starts(i, "\"\"\"") {
                        let end = match lang.triple {
                            Triple::None => return None,
                            _ => self.triple_end(i, '"', true)?,
                        };
                        if !self.interp_ok(i, end, 3) {
                            return None;
                        }
                        let s = self.slice(i, end);
                        self.token(&s);
                        i = end;
                        continue;
                    }
                    if (lang.cpp && i > 0 && self.chars[i - 1] == 'R')
                        || (lang.csharp && i > 0 && self.chars[i - 1] == '@')
                    {
                        return None;
                    }
                    if lang.yaml && !self.yaml_scalar_start(i) {
                        self.token("\"");
                        i += 1;
                        continue;
                    }
                    let end = self.quoted_end(i, '"', true, lang.dq_multiline)?;
                    if !self.interp_ok(i, end, 1) {
                        return None;
                    }
                    let s = self.slice(i, end);
                    self.token(&s);
                    i = end;
                    continue;
                }
                '\'' => {
                    if lang.yaml && !self.yaml_scalar_start(i) {
                        self.token("'");
                        i += 1;
                        continue;
                    }
                    match lang.single {
                        Single::Char | Single::RustChar => match self.char_literal_end(i) {
                            Some(end) => {
                                let s = self.slice(i, end);
                                self.token(&s);
                                i = end;
                            }
                            None if lang.single == Single::RustChar => {
                                self.token("'");
                                i += 1;
                            }
                            None => return None,
                        },
                        Single::Str { escapes, multiline } => {
                            let triple = self.starts(i, "'''") && lang.triple != Triple::None;
                            let end = if triple {
                                self.triple_end(i, '\'', lang.triple == Triple::Escaped)?
                            } else {
                                self.quoted_end(i, '\'', escapes, multiline)?
                            };
                            if !self.interp_ok(i, end, if triple { 3 } else { 1 }) {
                                return None;
                            }
                            let s = self.slice(i, end);
                            self.token(&s);
                            i = end;
                        }
                    }
                    continue;
                }
                'r' if lang.raw_rust => {
                    if let Some(end) = self.raw_string_end(i) {
                        let end = end?;
                        let s = self.slice(i, end);
                        self.token(&s);
                        i = end;
                        continue;
                    }
                }
                _ => {}
            }

            if c == '\\' && (lang.shell || lang.ruby || lang.perl) {
                // A backslash outside quotes escapes the next character, so
                // `\'` is not a quote.
                let s = self.slice(i, (i + 2).min(n));
                self.token(&s);
                i += 2;
                continue;
            }

            // Constructs the lexer does not model: refuse instead of guessing.
            let unmodeled = match c {
                '<' if lang.heredoc => next == Some('<'),
                // HTML-like comments in classic scripts (`<!--` anywhere,
                // `-->` at a line start).
                '<' if lang.js && self.starts(i, "<!--") => true,
                '-' if lang.js => self.starts(i, "-->"),
                '<' if lang.jsx => {
                    matches!(next, Some(n) if n.is_ascii_alphabetic() || n == '/' || n == '>')
                }
                '$' if lang.shell => next == Some('\''),
                '#' if lang.swift => {
                    let mut j = i;
                    while self.at(j) == Some('#') {
                        j += 1;
                    }
                    matches!(self.at(j), Some('"' | '\''))
                }
                '=' if lang.ruby || lang.perl => next == Some('~'),
                '/' if lang.ruby || lang.perl => true,
                // `%` then a blank or `=` is a literal (`% x `, `%=x=`) where an
                // operand may start; only `x% 1` is surely modulo.
                '%' if lang.ruby => match next {
                    Some(n) if !is_ws(n) && n != '=' => true,
                    _ => i == 0 || is_ws(self.chars[i - 1]),
                },
                '?' if lang.ruby => matches!(next, Some('"' | '\'' | '#')),
                '|' | '>' if lang.yaml => self.yaml_block_scalar(i),
                '_' if (lang.ruby || lang.perl) && self.starts(i, "__END__") => true,
                '_' if lang.perl && self.starts(i, "__DATA__") => true,
                'q' | 'm' | 's' | 't' | 'y' if lang.perl => self.perl_quote_like(i),
                '/' if lang.js => self.js_regex_possible(i),
                _ => false,
            };
            if unmodeled {
                return None;
            }
            let mut buf = [0u8; 4];
            self.token(c.encode_utf8(&mut buf));
            i += 1;
        }
        Some(self.out)
    }

    /// Perl quote-like operators (`q`, `qq`, `qw`, `qr`, `m`, `s`, `tr`, `y`)
    /// open literals that can hold `#` and quotes. Refuse when one is used.
    fn perl_quote_like(&self, i: usize) -> bool {
        if i > 0
            && (is_ident(self.chars[i - 1]) || matches!(self.chars[i - 1], '$' | '@' | '%' | '&'))
        {
            return false;
        }
        let mut j = i;
        while matches!(self.at(j), Some(c) if c.is_ascii_alphabetic()) {
            j += 1;
        }
        let word = self.slice(i, j);
        if !matches!(
            word.as_str(),
            "q" | "qq" | "qw" | "qr" | "qx" | "m" | "s" | "tr" | "y"
        ) {
            return false;
        }
        while matches!(self.at(j), Some(' ' | '\t')) {
            j += 1;
        }
        !matches!(
            self.at(j),
            None | Some('=' | ',' | ';' | ')' | '}' | '\n' | '\r')
        )
    }

    /// In JavaScript a `/` is division or the start of a regex literal, which
    /// can contain `//` and quotes. Refuse where a regex is possible.
    fn js_regex_possible(&self, i: usize) -> bool {
        match self.prev_code() {
            None => true,
            // `if (x) /re/` is a regex, `(a) / b` a division.
            Some(')') => self.paren_closes_statement_head() && self.rest_of_line_has(i + 1, '/'),
            Some(c) if "(,=:[!&|?{};+-*%<>~^".contains(c) => true,
            Some(c) if is_ident(c) => matches!(
                self.prev_word().as_str(),
                "return"
                    | "typeof"
                    | "case"
                    | "in"
                    | "of"
                    | "delete"
                    | "void"
                    | "throw"
                    | "new"
                    | "else"
                    | "do"
                    | "yield"
                    | "await"
                    | "instanceof"
            ),
            Some(_) => false,
        }
    }
}

/// A `\\u` escape (any number of `u`s) for an ASCII character: anything
/// that can be syntax. Escapes of other characters cannot change structure.
fn has_ascii_unicode_escape(text: &str) -> bool {
    let b = text.as_bytes();
    (0..b.len()).any(|i| {
        if b[i] != b'\\' {
            return false;
        }
        let mut j = i + 1;
        while b.get(j) == Some(&b'u') {
            j += 1;
        }
        j > i + 1
            && b.get(j..j + 4)
                .and_then(|h| std::str::from_utf8(h).ok())
                .and_then(|h| u32::from_str_radix(h, 16).ok())
                .is_some_and(|v| v < 0x80)
    })
}

/// Whether a Go file uses cgo, whose preamble comment is compiled as C.
fn imports_c(text: &str) -> bool {
    text.lines().any(|l| {
        let t = l.trim();
        t.starts_with("import \"C\"") || t == "\"C\""
    })
}

/// `text` with comments removed and whitespace normalized, for language
/// `ext` (file extension without the dot). `None` when the extension is not
/// supported or the text is ambiguous.
pub fn strip_comments(ext: &str, text: &str) -> Option<String> {
    let lang = language(&ext.to_ascii_lowercase())?;
    if text.contains('\0') {
        return None;
    }
    // A trigraph `??/` is a backslash to C compilers that honor trigraphs.
    if lang.splices && text.contains("??/") {
        return None;
    }
    // Java turns `\u0022` into a quote before it reads strings and comments.
    if lang.unicode_escapes && has_ascii_unicode_escape(text) {
        return None;
    }
    if lang.cgo && imports_c(text) {
        return None;
    }
    Lexer {
        chars: text.chars().collect(),
        lang: &lang,
        out: String::new(),
        seen_ws: false,
        newline: false,
        indent: String::new(),
    }
    .lex()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(ext: &str, t: &str) -> String {
        strip_comments(ext, t).unwrap_or_else(|| panic!("ambiguous: {t:?}"))
    }

    fn same(ext: &str, a: &str, b: &str) -> bool {
        match (strip_comments(ext, a), strip_comments(ext, b)) {
            (Some(x), Some(y)) => x == y,
            _ => false,
        }
    }

    #[test]
    fn line_and_block_comments_are_removed() {
        assert!(same("rs", "fn a() {} // note\n", "fn a() {}\n"));
        assert!(same("rs", "fn a() {}\n", "fn a() {}\n// added\n"));
        assert!(same("c", "int x; /* why */ int y;", "int x; int y;"));
        assert!(same("go", "a\n// c1\n// c2\nb\n", "a\nb\n"));
        assert!(same("py", "x = 1  # one\n", "x = 1\n"));
        assert!(same("toml", "# top\na = 1\n", "a = 1\n"));
        assert!(same("sh", "ls # list\n", "ls\n"));
        assert!(same(
            "yaml",
            "a: 1 # one\n# own line\nb: 2\n",
            "a: 1\nb: 2\n"
        ));
    }

    #[test]
    fn block_comments_span_lines() {
        let old = "a\n/* one\n   two\n   three */\nb\n";
        assert!(same("c", old, "a\nb\n"));
        assert!(same("rs", old, "a\n\nb\n"));
        // Between tokens on one line, a multi-line comment is a newline (Go
        // and others), so removing it outright is a change.
        assert!(!same("go", "x /*\n*/ y\n", "x y\n"));
        assert!(same("go", "x /*\n*/ y\n", "x\n y\n"));
    }

    #[test]
    fn rust_block_comments_nest_and_c_ones_do_not() {
        assert!(same("rs", "a /* x /* y */ z */ b", "a b"));
        // In C the first `*/` ends the comment, so `z */ b` is code.
        assert!(!same("c", "a /* x /* y */ z */ b", "a b"));
    }

    #[test]
    fn comment_markers_inside_strings_are_code() {
        assert!(!same(
            "rs",
            "let u = \"http://x\";\n",
            "let u = \"http:\";\n"
        ));
        assert!(!same("c", "s = \"/* a */\";", "s = \"\";"));
        assert!(!same("py", "s = \"# not a comment\"\n", "s = \"\"\n"));
        assert!(!same("py", "s = '# not a comment'\n", "s = ''\n"));
        assert!(!same("js", "s = '// x';\n", "s = '';\n"));
        assert_eq!(s("rs", "let u = \"a // b\"; // c\n"), "let u = \"a // b\";");
    }

    #[test]
    fn code_inside_a_string_that_looks_like_a_comment_still_counts() {
        // Only the string changed; the "comment" inside it is code.
        assert!(!same(
            "rs",
            "let s = \"x // old\";\n",
            "let s = \"x // new\";\n"
        ));
        assert!(!same("py", "q = \"a # b\"\n", "q = \"a # c\"\n"));
    }

    #[test]
    fn escapes_do_not_end_strings_early() {
        assert!(!same(
            "rs",
            "let s = \"a\\\" // b\";\n",
            "let s = \"a\\\" // c\";\n"
        ));
        assert_eq!(s("py", "x = \"a\\\\\" # c\n"), "x = \"a\\\\\"");
    }

    #[test]
    fn unterminated_input_is_ambiguous() {
        assert_eq!(strip_comments("rs", "let a = 1; /* never ends"), None);
        assert_eq!(strip_comments("rs", "/* /* */ still open"), None);
        assert_eq!(strip_comments("rs", "let s = \"open;\n"), None);
        assert_eq!(strip_comments("c", "char c = 'x;\n"), None);
        assert_eq!(strip_comments("py", "x = 'open\n"), None);
        assert_eq!(strip_comments("py", "x = \"\"\"never closed\n"), None);
    }

    #[test]
    fn removing_a_comment_marker_is_a_code_change() {
        assert!(!same("rs", "// call();\n", "call();\n"));
        assert!(!same("py", "# run()\n", "run()\n"));
        assert!(!same("c", "/* f(); */\n", "f();\n"));
        // Closing a block comment early turns the tail into code.
        assert!(!same("c", "/* a */ b */\n", "/* a b */\n"));
    }

    #[test]
    fn removing_a_comment_cannot_join_tokens() {
        assert!(!same("c", "a/**/b", "ab"));
        assert!(same("c", "a/**/b", "a b"));
    }

    #[test]
    fn indentation_is_code() {
        let old = "if x:\n    a()\n    b()\n";
        assert!(!same("py", old, "if x:\n    a()\nb()\n"));
        assert!(same("py", old, "if x:\n    a()  # why\n\n    b()\n"));
        assert!(!same("yaml", "a:\n  b: 1\n", "a:\nb: 1\n"));
    }

    #[test]
    fn rust_lifetimes_and_char_literals() {
        assert!(same(
            "rs",
            "fn f<'a>(x: &'a str) {} // c\n",
            "fn f<'a>(x: &'a str) {}\n"
        ));
        assert!(!same(
            "rs",
            "let c = '/'; let d = '/';\n",
            "let c = '/'; let d = '\\n';\n"
        ));
        assert_eq!(s("rs", "let q = '\"'; // c\n"), "let q = '\"';");
        assert_eq!(
            s("rs", "let u = '\\u{1F600}'; // c\n"),
            "let u = '\\u{1F600}';"
        );
        assert_eq!(s("rs", "let q = '\\''; // c\n"), "let q = '\\'';");
        assert_eq!(s("rs", "let x = '\\x41'; // c\n"), "let x = '\\x41';");
        assert_eq!(s("c", "char z = '\\012'; // c\n"), "char z = '\\012';");
        // Found by fuzzing: a backslash followed by junk was accepted as a char
        // literal if a quote came within a few characters, which swallowed the
        // `//` and made the text lex differently once its comments were gone.
        assert_eq!(s("rs", "f('\\r# n//b'); // c\n"), "f('\\r# n");
        assert_eq!(strip_comments("c", "f('\\q// b');\n"), None);
    }

    #[test]
    fn rust_raw_strings() {
        assert!(!same(
            "rs",
            "let s = r\"a // b\";\n",
            "let s = r\"a // c\";\n"
        ));
        assert_eq!(
            s("rs", "let s = r#\"say \"hi\" // there\"#; // c\n"),
            "let s = r#\"say \"hi\" // there\"#;"
        );
        assert_eq!(
            s("rs", "let s = br##\"x\"# // y\"##; // c\n"),
            "let s = br##\"x\"# // y\"##;"
        );
        assert_eq!(
            strip_comments("rs", "let s = r#\"never closed\"; // x\n"),
            None
        );
        // A raw identifier is not a raw string.
        assert_eq!(s("rs", "let r#type = 1; // c\n"), "let r#type = 1;");
        // An identifier ending in r is not either.
        assert_eq!(s("rs", "four\"x\" // c\n"), "four\"x\"");
    }

    #[test]
    fn single_quotes_in_c_like_languages_must_be_chars() {
        assert_eq!(s("c", "char a = 'a'; // c\n"), "char a = 'a';");
        assert_eq!(s("java", "char a = '\\n'; // c\n"), "char a = '\\n';");
        assert_eq!(strip_comments("c", "int x = 1'000; // c\n"), None);
    }

    #[test]
    fn python_strings() {
        let doc = "def f():\n    \"\"\"Doc # not a comment\n    // nor this\n    \"\"\"\n    return 1  # c\n";
        assert_eq!(
            s("py", doc),
            "def f():\n    \"\"\"Doc # not a comment\n    // nor this\n    \"\"\"\n    return 1"
        );
        assert!(!same("py", "'''a\n# b\n'''\n", "'''a\n# c\n'''\n"));
        assert_eq!(s("py", "x = 'it\\'s' # c\n"), "x = 'it\\'s'");
    }

    #[test]
    fn toml_literal_strings_have_no_escapes() {
        assert_eq!(s("toml", "p = 'C:\\' # c\n"), "p = 'C:\\'");
        assert_eq!(s("toml", "t = '''a # b''' # c\n"), "t = '''a # b'''");
    }

    #[test]
    fn hash_needs_a_word_boundary_in_shell_like_languages() {
        assert_eq!(s("sh", "echo ${#x} $# a#b # c\n"), "echo ${#x} $# a#b");
        assert!(!same("sh", "echo a#b\n", "echo a\n"));
        assert_eq!(s("yaml", "k: a#b # c\n"), "k: a#b");
        assert_eq!(s("sh", "echo \"a # b\" # c\n"), "echo \"a # b\"");
    }

    #[test]
    fn shebangs_and_magic_comments_are_code() {
        assert!(!same("sh", "#!/bin/sh\nls\n", "#!/bin/bash\nls\n"));
        assert!(!same("py", "# -*- coding: latin-1 -*-\nx=1\n", "x=1\n"));
        assert!(!same("go", "//go:build linux\npackage a\n", "package a\n"));
        assert!(!same("py", "x = 1  # noqa: E501\n", "x = 1\n"));
        assert!(!same("ts", "// @ts-ignore\nf()\n", "f()\n"));
        assert!(!same("c", "/* eslint-disable */ f();", "f();"));
        assert!(!same(
            "rs",
            "// SPDX-License-Identifier: MIT\nfn a(){}",
            "fn a(){}"
        ));
        assert!(same("rs", "// plain note\nfn a(){}", "fn a(){}"));
    }

    #[test]
    fn unmodeled_constructs_are_refused() {
        assert_eq!(strip_comments("sh", "cat <<EOF\n# body\nEOF\n"), None);
        assert_eq!(strip_comments("sh", "x=$'a\\'b' # c\n"), None);
        assert_eq!(strip_comments("go", "s := `raw // text`\n"), None);
        assert_eq!(strip_comments("js", "s = `a ${b} // c`;\n"), None);
        assert_eq!(strip_comments("js", "x = /a\\//.test(y);\n"), None);
        assert_eq!(strip_comments("js", "return /re/;\n"), None);
        assert_eq!(
            strip_comments("tsx", "const a = <div>http://x</div>;\n"),
            None
        );
        assert_eq!(strip_comments("rb", "x = %w[a # b]\n"), None);
        assert_eq!(strip_comments("rb", "x =~ /a/\n"), None);
        assert_eq!(strip_comments("pl", "$x =~ s/a/b/;\n"), None);
        assert_eq!(strip_comments("pl", "@a = qw(a # b);\n"), None);
        assert_eq!(strip_comments("cpp", "auto s = R\"(a \" // b)\";\n"), None);
        assert_eq!(strip_comments("cs", "var p = @\"C:\\\";\n"), None);
        assert_eq!(
            strip_comments("java", "String s = \"\"\"\n a\n\"\"\";\n"),
            None
        );
        assert_eq!(strip_comments("swift", "let s = #\"a\"#\n"), None);
        assert_eq!(strip_comments("yaml", "run: |\n  # kept\n  ls\n"), None);
        assert_eq!(strip_comments("yaml", "run: >-\n  a\n"), None);
        assert_eq!(strip_comments("txt", "a"), None);
    }

    #[test]
    fn vertical_tab_and_form_feed_are_not_blanks_in_shell_and_yaml() {
        // Found by fuzzing. They were treated as whitespace, so `a<FF># x`
        // lost its tail as a comment although `#` only starts one after a
        // blank, and `a<FF>b` equalled `a b`: a value change passed as a
        // comment change.
        for ext in ["sh", "bash", "yaml", "yml"] {
            assert_eq!(strip_comments(ext, "k=a\u{c}# x\n"), None, "{ext}");
            assert_eq!(strip_comments(ext, "k: a\u{b}b\n"), None, "{ext}");
            assert!(!same(ext, "k a\u{c}b\n", "k a b\n"), "{ext}");
        }
        // Languages where they do separate tokens are unchanged.
        assert!(same("c", "int\u{c}a;\n", "int a;\n"));
        assert!(same("rb", "x\u{c}# c\ny\n", "x\ny\n"));
    }

    #[test]
    fn division_is_not_a_regex() {
        assert_eq!(s("js", "x = a / b; // c\n"), "x = a / b;");
        assert_eq!(s("ts", "y = (a + b) / 2; // c\n"), "y = (a + b) / 2;");
    }

    #[test]
    fn line_terminators_that_hide_code_are_refused() {
        assert_eq!(strip_comments("js", "// note \u{2028} evil();\n"), None);
        assert_eq!(strip_comments("c", "// note \r evil();\n"), None);
        assert_eq!(strip_comments("java", "// \\u000a evil();\n"), None);
        assert_eq!(strip_comments("java", "/* \\u002a/ evil(); */\n"), None);
        assert_eq!(strip_comments("rs", "// \u{202e} hidden\nfn a(){}\n"), None);
        // CRLF files are ordinary.
        assert!(same("rs", "fn a(){} // c\r\n", "fn a(){}\n"));
    }

    #[test]
    fn nul_bytes_are_refused() {
        assert_eq!(strip_comments("rs", "a\0b"), None);
    }

    #[test]
    fn extension_matching_ignores_case() {
        assert!(strip_comments("RS", "a // b").is_some());
        assert!(strip_comments("yml", "a # b").is_some());
        assert!(strip_comments("md", "a").is_none());
    }

    #[test]
    fn yaml_quotes_inside_plain_scalars_are_text() {
        assert_eq!(
            s("yaml", "name: don't # c\nb: 'x # y'\n"),
            "name: don't\nb: 'x # y'"
        );
        assert_eq!(
            s("yaml", "list: [\"a # b\", 'c'] # d\n"),
            "list: [\"a # b\", 'c']"
        );
    }

    #[test]
    fn leading_and_trailing_comments_vanish() {
        assert_eq!(
            s("rs", "// header\n\n// more\nfn a(){}\n// tail\n"),
            "fn a(){}"
        );
        assert_eq!(s("c", "/* only */"), "");
    }

    /// Security review 2026-10, S-03: each pair differs only inside a string
    /// or in code the language reads where the lexer saw a comment, so the
    /// edit must never count as comment-only.
    #[test]
    fn constructs_that_desynchronize_the_lexer_are_refused() {
        let cases: &[(&str, &str, &str)] = &[
            // C splices and trigraphs.
            (
                "c",
                "/* x *\\\n/ evil(); /* */\n",
                "/* x *\\\n/ worse(); /* */\n",
            ),
            (
                "c",
                "/\\\n* \" */ char *s = \"// a\";\n",
                "/\\\n* \" */ char *s = \"// b\";\n",
            ),
            (
                "c",
                "// a \\\n\" x\nchar *s = \"// a\";\n",
                "// a \\\n\" x\nchar *s = \"// b\";\n",
            ),
            (
                "c",
                "char *s = \"??/\" // a\";\n",
                "char *s = \"??/\" // b\";\n",
            ),
            // Java unicode escapes outside comments.
            ("java", "s = \\u0022 // a \";\n", "s = \\u0022 // b \";\n"),
            (
                "java",
                "s = \"\\u0022\" // a \";\n",
                "s = \"\\u0022\" // b \";\n",
            ),
            ("scala", "s = \\uu0022 // a \"\n", "s = \\uu0022 // b \"\n"),
            // Shell comments after operators, and quotes inside `$(...)`.
            (
                "sh",
                "true;# don't\necho 'a # b'\n",
                "true;# don't\necho 'a # c'\n",
            ),
            (
                "sh",
                "x=\"$(echo \"a # b\")\"\n",
                "x=\"$(echo \"a # c\")\"\n",
            ),
            ("sh", "x=\"${y:-\"a # b\"}\"\n", "x=\"${y:-\"a # c\"}\"\n"),
            // Ruby and Perl comments without a blank before them, POD,
            // `=begin`, interpolation and `%` literals.
            ("rb", "x=1#it's\ny = 'a # b'\n", "x=1#it's\ny = 'a # c'\n"),
            (
                "rb",
                "x = \"#{h[\"a # b\"]}\"\n",
                "x = \"#{h[\"a # c\"]}\"\n",
            ),
            (
                "rb",
                "=begin\n\"\n=end\ns = \"a # b\"\n",
                "=begin\n\"\n=end\ns = \"a # c\"\n",
            ),
            (
                "rb",
                "x = % # ; system(\"a\")\n",
                "x = % # ; system(\"b\")\n",
            ),
            (
                "pl",
                "print 1;# it's\nmy $y = 'a # b';\n",
                "print 1;# it's\nmy $y = 'a # c';\n",
            ),
            (
                "pl",
                "=pod\n\"\n=cut\nmy $s = \"a # b\";\n",
                "=pod\n\"\n=cut\nmy $s = \"a # c\";\n",
            ),
            // Interpolated expressions that hold quotes.
            (
                "py",
                "x = f\"{d[\"a # b\"]}\"\n",
                "x = f\"{d[\"a # c\"]}\"\n",
            ),
            (
                "py",
                "x = f\"{d['}'] + d[\"a # b\"]}\"\n",
                "x = f\"{d['}'] + d[\"a # c\"]}\"\n",
            ),
            ("py", "x = rf'{d['a # b']}'\n", "x = rf'{d['a # c']}'\n"),
            (
                "py",
                "x = f\"\\{d[\"a # b\"]}\"\n",
                "x = f\"\\{d[\"a # c\"]}\"\n",
            ),
            (
                "cs",
                "var s = $\"{d[\"a // b\"]}\";\n",
                "var s = $\"{d[\"a // c\"]}\";\n",
            ),
            (
                "kt",
                "val s = \"${m[\"a // b\"]}\"\n",
                "val s = \"${m[\"a // c\"]}\"\n",
            ),
            (
                "swift",
                "let s = \"\\(d[\"a // b\"]!)\"\n",
                "let s = \"\\(d[\"a // c\"]!)\"\n",
            ),
            (
                "scala",
                "val s = s\"${m(\"a // b\")}\"\n",
                "val s = s\"${m(\"a // c\")}\"\n",
            ),
            // JavaScript HTML-like comments and a regex after `if (...)`.
            (
                "js",
                "x = 1 <!-- \"\\\ns = \"// a\";\n",
                "x = 1 <!-- \"\\\ns = \"// b\";\n",
            ),
            (
                "js",
                "if (a) /\"/.test(s); t = \"// a\";\n",
                "if (a) /\"/.test(s); t = \"// b\";\n",
            ),
            // Comments that are code: cgo preambles and Rust doctests.
            (
                "go",
                "/*\nint f() { return 1; }\n*/\nimport \"C\"\n",
                "/*\nint f() { return 2; }\n*/\nimport \"C\"\n",
            ),
            (
                "rs",
                "/// ```\n/// a();\n/// ```\nfn f(){}\n",
                "/// ```\n/// b();\n/// ```\nfn f(){}\n",
            ),
            ("rs", "//! a\nfn f(){}\n", "//! b\nfn f(){}\n"),
            ("rs", "/** a */\nfn f(){}\n", "/** b */\nfn f(){}\n"),
        ];
        for (ext, a, b) in cases {
            assert!(!same(ext, a, b), "{ext}: {a:?} vs {b:?}");
        }
    }

    /// The refusals above stay narrow: ordinary uses of the same features
    /// still allow comment-only edits.
    #[test]
    fn ordinary_interpolation_splices_and_operators_still_work() {
        let cases: &[(&str, &str, &str)] = &[
            (
                "c",
                "#define X(a) \\\n  f(a) // a\n",
                "#define X(a) \\\n  f(a) // b\n",
            ),
            (
                "java",
                "s = \"caf\\u00e9\"; // a\n",
                "s = \"caf\\u00e9\"; // b\n",
            ),
            (
                "sh",
                "echo \"${HOME}/x\" \"$(date)\" \"${v#*/}\" # a\n",
                "echo \"${HOME}/x\" \"$(date)\" \"${v#*/}\" # b\n",
            ),
            ("sh", "echo $# ${#x} # a\n", "echo $# ${#x} # b\n"),
            (
                "rb",
                "puts \"#{a}\" # a\nx = y% 2\n",
                "puts \"#{a}\" # b\nx = y% 2\n",
            ),
            ("pl", "my $n = $#a; # a\n", "my $n = $#a; # b\n"),
            (
                "py",
                "x = f\"{a} {b!r:>{w}} {{lit}}\"  # a\n",
                "x = f\"{a} {b!r:>{w}} {{lit}}\"  # b\n",
            ),
            (
                "py",
                "x = rb\"{not a field\" # a\n",
                "x = rb\"{not a field\" # b\n",
            ),
            (
                "cs",
                "var s = $\"{a} {{x}}\"; // a\n",
                "var s = $\"{a} {{x}}\"; // b\n",
            ),
            (
                "kt",
                "val s = \"${a.b} $c\" // a\n",
                "val s = \"${a.b} $c\" // b\n",
            ),
            (
                "swift",
                "let s = \"\\(a + 1)\" // a\n",
                "let s = \"\\(a + 1)\" // b\n",
            ),
            (
                "js",
                "y = (a + b) / 2; // a\nz = f(x) / g(y); // a\n",
                "y = (a + b) / 2; // b\nz = f(x) / g(y); // b\n",
            ),
            ("go", "// a\npackage main\n", "// b\npackage main\n"),
            (
                "rs",
                "// a\n//// not doc\nfn f(){}\n",
                "// b\n//// not doc either\nfn f(){}\n",
            ),
        ];
        for (ext, a, b) in cases {
            assert!(same(ext, a, b), "{ext}: {a:?} vs {b:?}");
        }
    }
}
