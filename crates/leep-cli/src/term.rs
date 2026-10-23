//! Colour and paging for human output. Colour is decided once per process
//! (`--color`, `NO_COLOR`, `CLICOLOR_FORCE`, `CLICOLOR`, `TERM`, whether
//! stdout is a terminal); JSON output is never coloured or paged. Plain ANSI
//! escapes and `std::io::IsTerminal`, so no dependency is needed.

use std::io::{IsTerminal, Write};
use std::sync::OnceLock;

/// `--color`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, clap::ValueEnum)]
pub enum ColorChoice {
    /// Colour when stdout is a terminal, unless the environment says no.
    #[default]
    Auto,
    Always,
    Never,
}

static COLOR: OnceLock<bool> = OnceLock::new();

/// Whether to colour, from the flag, an environment lookup and whether
/// stdout is a terminal. In `auto`: `NO_COLOR` (any non-empty value) wins,
/// then `CLICOLOR_FORCE` (non-empty, not `0`), then `CLICOLOR=0` and
/// `TERM=dumb` turn it off; otherwise colour exactly on a terminal.
pub fn decide_color(
    choice: ColorChoice,
    stdout_is_tty: bool,
    var: impl Fn(&str) -> Option<String>,
) -> bool {
    let set = |k: &str| var(k).filter(|v| !v.is_empty());
    match choice {
        ColorChoice::Always => true,
        ColorChoice::Never => false,
        ColorChoice::Auto => {
            if set("NO_COLOR").is_some() {
                false
            } else if set("CLICOLOR_FORCE").is_some_and(|v| v != "0") {
                true
            } else if set("CLICOLOR").is_some_and(|v| v == "0")
                || set("TERM").is_some_and(|v| v == "dumb")
            {
                false
            } else {
                stdout_is_tty
            }
        }
    }
}

/// Decide colour for this process. Called once from `main`; library callers
/// that never call it get no colour.
pub fn init(choice: ColorChoice) {
    let on = decide_color(choice, std::io::stdout().is_terminal(), |k| {
        std::env::var(k).ok()
    });
    let _ = COLOR.set(on);
}

pub fn color() -> bool {
    COLOR.get().copied().unwrap_or(false)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Style {
    Bold,
    Red,
    Green,
    Yellow,
    Cyan,
}

impl Style {
    fn code(self) -> &'static str {
        match self {
            Style::Bold => "1",
            Style::Red => "31",
            Style::Green => "32",
            Style::Yellow => "33",
            Style::Cyan => "36",
        }
    }
}

/// `s` in `style` when colour is on, else unchanged.
pub fn paint(s: &str, style: Style) -> String {
    paint_if(color(), s, style)
}

fn paint_if(on: bool, s: &str, style: Style) -> String {
    if on {
        format!("\x1b[{}m{s}\x1b[0m", style.code())
    } else {
        s.to_string()
    }
}

/// A unified diff with added lines green, removed red, hunk headers cyan
/// and file headers bold, when colour is on.
pub fn diff(text: &str) -> String {
    diff_if(color(), text)
}

fn diff_if(on: bool, text: &str) -> String {
    if !on {
        return text.to_string();
    }
    let mut out = String::with_capacity(text.len() + text.len() / 8);
    for line in text.split_inclusive('\n') {
        let (body, end) = match line.strip_suffix('\n') {
            Some(b) => (b, "\n"),
            None => (line, ""),
        };
        let style = if body.starts_with("+++ ") || body.starts_with("--- ") {
            Some(Style::Bold)
        } else if body.starts_with("@@") {
            Some(Style::Cyan)
        } else if body.starts_with('+') {
            Some(Style::Green)
        } else if body.starts_with('-') {
            Some(Style::Red)
        } else {
            None
        };
        match style {
            Some(s) => out.push_str(&paint_if(true, body, s)),
            None => out.push_str(body),
        }
        out.push_str(end);
    }
    out
}

/// The status letter of `leep status` and `leep show`, coloured.
pub fn letter(c: char) -> String {
    let style = match c {
        'A' => Style::Green,
        'M' => Style::Yellow,
        _ => Style::Red,
    };
    paint(&c.to_string(), style)
}

/// The pager to use: `LEEP_PAGER`, then `PAGER`, then `less`. An empty value
/// or `cat` means none.
pub fn pager_command(var: impl Fn(&str) -> Option<String>) -> Option<String> {
    let cmd = var("LEEP_PAGER")
        .or_else(|| var("PAGER"))
        .unwrap_or_else(|| "less".into());
    let cmd = cmd.trim().to_string();
    (!cmd.is_empty() && cmd != "cat").then_some(cmd)
}

/// Write the output of `log`, `diff` or `show`: through a pager when stdout
/// is a terminal and not `json`, else straight to stdout. A pager quit
/// early (`q`) is not an error; a closed pipe (`| head`) exits 141, as every
/// other command's output does (`leep_core::stdout`).
pub fn page(text: &str, json: bool) -> crate::error::Result<()> {
    if !json && std::io::stdout().is_terminal() {
        if let Some(cmd) = pager_command(|k| std::env::var(k).ok()) {
            if spawn_pager(&cmd, text) {
                return Ok(());
            }
        }
    }
    Ok(leep_core::stdout::write_all(text.as_bytes())?)
}

/// Run `cmd` through the shell with `text` on its stdin and wait for it.
/// `false` if it could not start, so the caller prints directly.
#[cfg(unix)]
fn spawn_pager(cmd: &str, text: &str) -> bool {
    let mut c = std::process::Command::new("sh");
    c.arg("-c").arg(cmd).stdin(std::process::Stdio::piped());
    // Quit at once on a short page, keep colours, leave the screen alone.
    if std::env::var_os("LESS").is_none() {
        c.env("LESS", "FRX");
    }
    let Ok(mut child) = c.spawn() else {
        return false;
    };
    if let Some(mut stdin) = child.stdin.take() {
        // A pager quit early closes the pipe; that is fine.
        let _ = stdin.write_all(text.as_bytes());
    }
    let _ = child.wait();
    true
}

#[cfg(not(unix))]
fn spawn_pager(_cmd: &str, _text: &str) -> bool {
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env<'a>(pairs: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
        move |k| {
            pairs
                .iter()
                .find(|(n, _)| *n == k)
                .map(|(_, v)| v.to_string())
        }
    }

    #[test]
    fn colour_follows_the_flag_then_the_environment_then_the_terminal() {
        use ColorChoice::*;
        type Vars = &'static [(&'static str, &'static str)];
        let cases: &[(ColorChoice, bool, Vars, bool)] = &[
            (Auto, true, &[], true),
            (Auto, false, &[], false),
            (Auto, true, &[("NO_COLOR", "1")], false),
            (Auto, true, &[("NO_COLOR", "")], true),
            (Auto, false, &[("CLICOLOR_FORCE", "1")], true),
            (Auto, false, &[("CLICOLOR_FORCE", "0")], false),
            (
                Auto,
                false,
                &[("CLICOLOR_FORCE", "1"), ("NO_COLOR", "1")],
                false,
            ),
            (Auto, true, &[("CLICOLOR", "0")], false),
            (Auto, true, &[("TERM", "dumb")], false),
            (Always, false, &[("NO_COLOR", "1")], true),
            (Never, true, &[("CLICOLOR_FORCE", "1")], false),
        ];
        for (choice, tty, vars, want) in cases {
            assert_eq!(
                decide_color(*choice, *tty, env(vars)),
                *want,
                "{choice:?} tty={tty} {vars:?}"
            );
        }
    }

    #[test]
    fn diff_colouring_wraps_whole_lines_only_when_on() {
        let d = "--- a/x\n+++ b/x\n@@ -1 +1 @@\n-old\n+new\n same\n";
        assert_eq!(diff_if(false, d), d);
        let c = diff_if(true, d);
        assert!(c.contains("\x1b[31m-old\x1b[0m\n"), "{c:?}");
        assert!(c.contains("\x1b[32m+new\x1b[0m\n"), "{c:?}");
        assert!(c.contains("\x1b[1m--- a/x\x1b[0m\n"), "{c:?}");
        assert!(c.ends_with(" same\n"));
        assert!(!paint("x", Style::Red).contains('\x1b'), "off unless init");
    }

    #[test]
    fn pager_comes_from_leep_pager_then_pager_then_less() {
        assert_eq!(pager_command(env(&[])).as_deref(), Some("less"));
        assert_eq!(
            pager_command(env(&[("PAGER", "more")])).as_deref(),
            Some("more")
        );
        assert_eq!(
            pager_command(env(&[("PAGER", "more"), ("LEEP_PAGER", "less -S")])).as_deref(),
            Some("less -S")
        );
        assert_eq!(pager_command(env(&[("LEEP_PAGER", "")])), None);
        assert_eq!(pager_command(env(&[("PAGER", "cat")])), None);
    }
}
