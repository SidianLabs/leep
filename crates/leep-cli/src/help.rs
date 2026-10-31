//! Help text: per-command examples, the grouped top-level command list and
//! the global options, which are documented once instead of under every
//! command.

pub const INIT: &str = "\
Examples:
  leep --server http://leep.internal:7070 init          # here, tracking main
  leep init --ref release/1.0 --prefix src --prefix docs wc";

pub const CHECKOUT: &str = "\
Examples:
  leep checkout main
  leep checkout 2a954d8ef7c7            # a short id from `leep log`
  leep checkout @ --prefix src          # widen or narrow the sparse checkout";

pub const STATUS: &str = "\
Examples:
  leep status
  leep --json status                    # `changes`, `ahead`, `on_target`, ...";

pub const DIFF: &str = "\
Examples:
  leep diff                             # uncommitted changes
  leep diff main feature                # two revisions, computed on the server
  leep diff 2a954d8e 30f9056d --no-renames";

pub const COMMIT: &str = "\
Examples:
  leep commit -m \"parse: accept empty input\"
  id=$(leep commit -m wip)              # the full id is printed on stdout
  leep commit -m \"retry on 503\" --agent-model M --agent-session S";

pub const LAND: &str = "\
Examples:
  leep land                             # onto the working copy's target ref
  leep land --ref release/1.0
  leep land --ref feature/x --create    # the ref does not exist yet
Exit 5 means conflicts: resolve them, `leep commit`, then `leep land` again.";

pub const SYNC: &str = "\
Examples:
  leep sync                             # carry uncommitted work and local commits along
  leep sync --no-rebase                 # refuse instead if anything is uncommitted or unlanded
  leep sync --no-rebase --discard       # move anyway, leaving local commits behind";

pub const BRANCH: &str = "\
Examples:
  leep branch                           # list
  leep branch feature/x                 # create at the working copy base
  leep branch hotfix 2a954d8ef7c7       # create at a revision
  leep branch -d feature/x              # delete once merged (-D: anyway)";

pub const SWITCH: &str = "\
Examples:
  leep switch feature/x
  leep switch -c feature/y              # create it at the base and switch
  leep switch -c hotfix main";

pub const LOG: &str = "\
Examples:
  leep log -n 5
  leep log main --path src/parser.rs
  leep log --follow src/new_name.rs     # across renames
  leep log --oneline --graph -n 50      # with the commit graph
  leep log --agent-model M --author alice
  leep --json log -n 1                  # full ids and changesets";

pub const SHOW: &str = "\
Examples:
  leep show main
  leep show 2a954d8ef7c7                # any unique prefix of 4+ hex digits
  leep show @                           # the working copy base
  leep show main~2                      # two first parents back
  leep show @^2                         # the base's second parent (a merge)";

pub const BISECT: &str = "\
Examples:
  leep bisect start main v1.2           # bad, then good; checks out the middle
  leep bisect good                      # the checked-out changeset works
  leep bisect bad                       # it shows the bug
  leep bisect skip                      # it cannot be tested
  leep bisect start @ main~20 && leep bisect run cargo test -p parser
  leep bisect log
  leep bisect reset                     # back to where the bisect started
`bisect run` reads the command's exit status like git: 0 good, 125 skip,
1 to 127 bad, 128 or more (or a signal) stops the bisect.";

pub const SEARCH: &str = "\
Examples:
  leep search TODO
  leep search -i -E 'fn\\s+main' --glob '**/*.rs'
  leep search retry --path src --rev release/1.0
Exit 1 when nothing matches, like grep.";

pub const BLAME: &str = "\
Examples:
  leep blame src/lib.rs
  leep blame main src/lib.rs --who      # author and agent model per line";

pub const UNDO: &str = "\
Examples:
  leep undo                             # the target ref's last move
  leep reflog main -n 5 && leep undo main --to 41";

pub const CONFIG: &str = "\
Examples:
  leep config author \"Ada Lovelace <ada@example.com>\"   # this working copy
  leep config --global author \"Ada Lovelace <ada@example.com>\"
  leep config author                    # the author in effect, and where it comes from
  leep config --unset author
The author of a new changeset is the first of: --author, LEEP_AUTHOR, this
working copy's setting, the --global one, git's user.name and user.email,
the login name, the server's name for the token.";

pub const REVIEW: &str = "\
Examples:
  leep review status @                  # who still has to approve (exit 1 if not approved)
  leep review approve 2a954d8ef7c7 -m \"looks good\"
  leep review reject 2a954d8ef7c7 -m \"breaks the parser\"";
/// Shown under the top-level help; the full contract is docs/CLI.md.
pub const EXIT_CODES: &str = "\
Exit codes (ADR 0034):
  0  success
  1  generic failure (bad input, refused operation, authentication);
     also \"no\" from `search` and `review status`
  2  usage: the command line does not parse
  3  server or network failure; running the command again may work
  4  not found (revision, ref, branch, tag, file, stash)
  5  conflict: resolve the conflicts, or retry a lost race
  6  nothing to do (`commit`, `amend`, `stash push` with nothing to record)
  130 interrupted";

/// Every command, by task, for the top-level help. A test checks that each
/// command is listed exactly once.
pub const GROUPS: &[(&str, &[&str])] = &[
    (
        "Work",
        &[
            "init", "checkout", "status", "diff", "commit", "amend", "stash", "restore", "resolve",
            "reset", "sync", "land",
        ],
    ),
    (
        "Branches",
        &[
            "branch",
            "switch",
            "tag",
            "merge",
            "rebase",
            "cherry-pick",
            "revert",
        ],
    ),
    (
        "History",
        &[
            "log",
            "show",
            "bisect",
            "cat",
            "blame",
            "last-change",
            "search",
            "provenance",
            "refs",
            "reflog",
        ],
    ),
    ("Review", &["review", "reviews"]),
    ("Admin", &["undo", "keygen", "config"]),
];

/// The global options, written out once for the top-level help. The flags
/// themselves are hidden from every command's own option list.
const GLOBAL_OPTIONS: &str = "\
Global options (every command accepts them):
      --server <URL>   Server URL; overrides LEEP_SERVER, which overrides .leep/config.json
      --token <TOKEN>  Bearer token; overrides LEEP_TOKEN and .leep/config.json.
                       Prefer LEEP_TOKEN: a flag is visible in process listings
      --json           Print results as JSON (usually the raw /v1 response); errors
                       too, as one JSON document on stdout (docs/CLI.md)
      --color <WHEN>   auto (default: on a terminal, unless NO_COLOR; CLICOLOR_FORCE
                       forces it), always or never. JSON is never coloured
  -h, --help           Print help; `leep COMMAND --help` for one command
  -V, --version        Print version

On a terminal, log, diff and show page through LEEP_PAGER, else PAGER, else
`less`; an empty LEEP_PAGER turns paging off.";

/// Footer of every command's own help, pointing at the global options.
const GLOBAL_FOOTER: &str =
    "Global options: --server URL, --token TOKEN, --json, --color WHEN (see `leep --help`)";

/// Give `cmd` the grouped top-level help and every subcommand the global
/// options footer.
pub fn decorate(cmd: clap::Command) -> clap::Command {
    let width = GROUPS
        .iter()
        .flat_map(|(_, names)| names.iter())
        .map(|n| n.len())
        .max()
        .unwrap_or(0);
    let mut list = String::new();
    for (title, names) in GROUPS {
        list.push_str(title);
        list.push_str(":\n");
        for name in *names {
            let about = cmd
                .find_subcommand(name)
                .and_then(|c| c.get_about())
                .map(|a| a.to_string())
                .unwrap_or_default();
            list.push_str(&format!("  {name:<width$}  {about}\n"));
        }
        list.push('\n');
    }
    let template = format!(
        "{{about-with-newline}}\n{{usage-heading}} {{usage}}\n\n{list}{GLOBAL_OPTIONS}{{after-help}}"
    );
    let cmd = cmd
        .help_template(template)
        .override_usage("leep [--server URL] [--token TOKEN] [--json] <COMMAND> [ARGS]");
    footers(cmd)
}

fn footers(cmd: clap::Command) -> clap::Command {
    let names: Vec<String> = cmd
        .get_subcommands()
        .map(|c| c.get_name().to_string())
        .collect();
    names.into_iter().fold(cmd, |cmd, name| {
        cmd.mut_subcommand(name, |sub| {
            let text = match sub.get_after_help() {
                Some(h) => format!("{h}\n\n{GLOBAL_FOOTER}"),
                None => GLOBAL_FOOTER.to_string(),
            };
            footers(sub.after_help(text))
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_command_is_listed_once() {
        let cmd = crate::cli::command();
        let listed: Vec<&str> = GROUPS.iter().flat_map(|(_, n)| n.iter().copied()).collect();
        for sub in cmd.get_subcommands().filter(|c| !c.is_hide_set()) {
            let n = listed.iter().filter(|l| **l == sub.get_name()).count();
            assert_eq!(n, 1, "{} is listed {n} times", sub.get_name());
        }
        for name in &listed {
            assert!(
                cmd.find_subcommand(name).is_some(),
                "{name} is not a command"
            );
        }
        cmd.debug_assert();
    }

    /// Split an example line like a shell would, for the simple quoting the
    /// examples use, dropping a trailing `# comment`.
    fn words(line: &str) -> Vec<String> {
        let (mut out, mut cur, mut quote, mut any) = (Vec::new(), String::new(), None, false);
        for c in line.chars() {
            match (quote, c) {
                (None, '#') if !any => break,
                (None, '\'' | '"') => (quote, any) = (Some(c), true),
                (Some(q), c) if c == q => quote = None,
                (None, ' ') => {
                    if any {
                        out.push(std::mem::take(&mut cur));
                        any = false;
                    }
                }
                (_, c) => {
                    cur.push(c);
                    any = true;
                }
            }
        }
        if any {
            out.push(cur);
        }
        out
    }

    #[test]
    fn examples_parse() {
        let all = [
            INIT, CHECKOUT, STATUS, DIFF, COMMIT, LAND, SYNC, BRANCH, SWITCH, LOG, SHOW, BISECT,
            SEARCH, BLAME, UNDO, REVIEW, CONFIG,
        ];
        let mut checked = 0;
        for text in all {
            assert!(text.starts_with("Examples:\n"), "{text}");
            for line in text.lines().filter(|l| l.starts_with("  leep ")) {
                if line.contains("$(") || line.contains("&&") {
                    continue;
                }
                let args = words(line.trim());
                crate::cli::command()
                    .try_get_matches_from(&args)
                    .unwrap_or_else(|e| panic!("{line}: {e}"));
                checked += 1;
            }
        }
        assert!(checked >= 30, "{checked}");
    }

    #[test]
    fn global_options_are_documented_once() {
        let mut top = crate::cli::command();
        let text = top.render_help().to_string();
        assert!(text.contains("Global options") && text.contains("--server <URL>"));
        assert!(text.contains("Work:") && text.contains("Exit codes"));
        let land = top
            .find_subcommand_mut("land")
            .unwrap()
            .render_help()
            .to_string();
        assert!(!land.contains("--server <"), "{land}");
        assert!(
            land.contains("Examples:") && land.contains("see `leep --help`"),
            "{land}"
        );
    }
}
