//! The git remote, driven through the system `git` binary so every transport
//! and credential helper git supports (https, ssh, `file://`) works without
//! Leep handling secrets itself.

use std::collections::BTreeMap;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use gix::ObjectId;

/// Remove `user:password@` from every URL in `text`, so a remote URL that
/// carries a token never reaches a log line, the status file or a metric.
pub fn redact(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(i) = rest.find("://") {
        let (head, tail) = rest.split_at(i + 3);
        out.push_str(head);
        let end = tail
            .find(|c: char| c.is_whitespace() || c == '/' || c == '\'' || c == '"')
            .unwrap_or(tail.len());
        match tail[..end].rfind('@') {
            Some(at) => {
                out.push_str("***@");
                rest = &tail[at + 1..];
            }
            None => rest = tail,
        }
    }
    out.push_str(rest);
    out
}

/// How a push ended. A rejection is the remote refusing the update (not a
/// fast-forward, or a lease that did not match); anything else that is not
/// `Ok` is a transport or local failure worth retrying.
#[derive(Debug, PartialEq, Eq)]
pub enum PushOutcome {
    Ok,
    Rejected(String),
    Failed(String),
}

/// The mirror's local bare repository and the remote it pushes to.
pub struct Remote {
    pub git_dir: PathBuf,
    url: String,
    timeout: Duration,
}

impl Remote {
    pub fn new(git_dir: &Path, url: &str, timeout: Duration) -> Remote {
        Remote {
            git_dir: git_dir.to_path_buf(),
            url: url.to_string(),
            timeout,
        }
    }

    /// The remote URL with any credentials removed.
    pub fn display_url(&self) -> String {
        redact(&self.url)
    }

    fn cmd(&self) -> Command {
        let mut c = Command::new("git");
        c.arg("--git-dir")
            .arg(&self.git_dir)
            // Never wait for a password on a terminal nobody is watching.
            .env("GIT_TERMINAL_PROMPT", "0")
            .env("GCM_INTERACTIVE", "never")
            .env("LC_ALL", "C")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        c
    }

    /// Run git with the configured timeout; `(success, stdout, stderr)`, with
    /// credentials redacted from both streams.
    fn run(&self, args: &[&str]) -> Result<(bool, String, String), String> {
        let mut child = self
            .cmd()
            .args(args)
            .spawn()
            .map_err(|e| format!("cannot run git: {e}"))?;
        let mut out = child.stdout.take().expect("piped");
        let mut err = child.stderr.take().expect("piped");
        let reader = std::thread::spawn(move || {
            let mut s = Vec::new();
            let _ = out.read_to_end(&mut s);
            s
        });
        let err_reader = std::thread::spawn(move || {
            let mut s = Vec::new();
            let _ = err.read_to_end(&mut s);
            s
        });
        let deadline = Instant::now() + self.timeout;
        let status = loop {
            match child.try_wait() {
                Ok(Some(s)) => break s,
                Ok(None) if Instant::now() >= deadline => {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(format!(
                        "git {} timed out after {}s",
                        args.first().unwrap_or(&""),
                        self.timeout.as_secs()
                    ));
                }
                Ok(None) => std::thread::sleep(Duration::from_millis(10)),
                Err(e) => return Err(format!("waiting for git: {e}")),
            }
        };
        let stdout = String::from_utf8_lossy(&reader.join().unwrap_or_default()).into_owned();
        let stderr = String::from_utf8_lossy(&err_reader.join().unwrap_or_default()).into_owned();
        Ok((status.success(), redact(&stdout), redact(stderr.trim())))
    }

    /// Current values of `refs` on the remote. Refs it does not have are
    /// absent from the map.
    pub fn ls_remote(&self, refs: &[String]) -> Result<BTreeMap<String, ObjectId>, String> {
        if refs.is_empty() {
            return Ok(BTreeMap::new());
        }
        let mut args = vec!["ls-remote", "--", self.url.as_str()];
        args.extend(refs.iter().map(String::as_str));
        let (ok, out, err) = self.run(&args)?;
        if !ok {
            return Err(format!(
                "git ls-remote {} failed: {err}",
                self.display_url()
            ));
        }
        let mut found = BTreeMap::new();
        for line in out.lines() {
            let Some((sha, name)) = line.split_once('\t') else {
                continue;
            };
            // ls-remote matches patterns against the tail of each name.
            if refs.iter().any(|r| r == name) {
                let oid = ObjectId::from_hex(sha.as_bytes())
                    .map_err(|e| format!("bad ls-remote line {line:?}: {e}"))?;
                found.insert(name.to_string(), oid);
            }
        }
        Ok(found)
    }

    /// Fetch remote refs into `refs/leep-mirror/remote/...` so their commits
    /// are local (for ancestry checks and imports) and stay reachable.
    pub fn fetch(&self, refs: &[String]) -> Result<(), String> {
        if refs.is_empty() {
            return Ok(());
        }
        let specs: Vec<String> = refs
            .iter()
            .map(|r| format!("+{r}:{}", tracking_ref(r)))
            .collect();
        let mut args = vec![
            "fetch",
            "--quiet",
            "--no-tags",
            "--no-write-fetch-head",
            "--",
        ];
        args.push(&self.url);
        args.extend(specs.iter().map(String::as_str));
        let (ok, _, err) = self.run(&args)?;
        if ok {
            Ok(())
        } else {
            Err(format!("git fetch {} failed: {err}", self.display_url()))
        }
    }

    /// Push `new` to `git_ref`, never with a force flag. With `lease`, the
    /// update applies only if the remote ref is exactly that value (or absent
    /// for `Some(None)`), which allows a non-fast-forward update the caller
    /// has decided is safe; without it the remote accepts only a
    /// fast-forward. `new = None` deletes the ref and needs a lease.
    pub fn push(
        &self,
        git_ref: &str,
        new: Option<ObjectId>,
        lease: Option<Option<ObjectId>>,
    ) -> Result<PushOutcome, String> {
        let spec = match new {
            Some(n) => format!("{n}:{git_ref}"),
            None => format!(":{git_ref}"),
        };
        let lease_arg = lease.map(|l| {
            format!(
                "--force-with-lease={git_ref}:{}",
                l.map(|o| o.to_string()).unwrap_or_default()
            )
        });
        let mut args = vec!["push", "--porcelain", "--no-verify"];
        if let Some(l) = &lease_arg {
            args.push(l);
        }
        args.push("--");
        args.push(&self.url);
        args.push(&spec);
        let (ok, out, err) = self.run(&args)?;
        // Porcelain: "<flag>\t<from>:<to>\t<summary>"; `!` is a rejection.
        let line = out
            .lines()
            .find(|l| l.split('\t').nth(1).is_some_and(|s| s.ends_with(git_ref)));
        Ok(match line {
            Some(l) if l.starts_with('!') => {
                let why = l.split('\t').nth(2).unwrap_or("rejected").to_string();
                PushOutcome::Rejected(if err.is_empty() {
                    why
                } else {
                    format!("{why}: {}", last_line(&err))
                })
            }
            Some(_) if ok => PushOutcome::Ok,
            _ if ok => PushOutcome::Ok,
            _ => PushOutcome::Failed(format!(
                "git push {} failed: {}",
                self.display_url(),
                last_line(&err)
            )),
        })
    }

    /// Is `a` an ancestor of (or equal to) `b`? Both must be local.
    pub fn is_ancestor(&self, a: ObjectId, b: ObjectId) -> Result<bool, String> {
        if a == b {
            return Ok(true);
        }
        let (a, b) = (a.to_string(), b.to_string());
        let mut c = self.cmd();
        let out = c
            .args(["merge-base", "--is-ancestor", &a, &b])
            .output()
            .map_err(|e| format!("cannot run git: {e}"))?;
        match out.status.code() {
            Some(0) => Ok(true),
            Some(1) => Ok(false),
            _ => Err(format!(
                "git merge-base --is-ancestor {a} {b} failed: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            )),
        }
    }

    /// Point a local ref at `oid`, so what the mirror wrote stays reachable
    /// if someone runs `git gc` in its repository.
    pub fn update_local_ref(&self, name: &str, oid: ObjectId) -> Result<(), String> {
        let (ok, _, err) = self.run(&["update-ref", name, &oid.to_string()])?;
        if ok {
            Ok(())
        } else {
            Err(format!("git update-ref {name} failed: {err}"))
        }
    }

    /// Pack loose objects when git thinks it is worth it.
    pub fn gc_auto(&self) {
        let _ = self.run(&["gc", "--auto", "--quiet"]);
    }
}

/// Where a fetched remote ref is kept locally.
pub fn tracking_ref(git_ref: &str) -> String {
    format!(
        "refs/leep-mirror/remote/{}",
        git_ref.strip_prefix("refs/").unwrap_or(git_ref)
    )
}

fn last_line(s: &str) -> &str {
    s.lines().rev().find(|l| !l.trim().is_empty()).unwrap_or(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redact_strips_credentials_from_urls() {
        assert_eq!(
            redact("fatal: https://bot:ghp_secret@github.com/o/r.git not found"),
            "fatal: https://***@github.com/o/r.git not found"
        );
        assert_eq!(
            redact("ssh://git@example.com:22/r and https://h/x"),
            "ssh://***@example.com:22/r and https://h/x"
        );
        assert_eq!(redact("file:///var/tmp/r.git"), "file:///var/tmp/r.git");
        assert_eq!(redact("no url here"), "no url here");
    }

    #[test]
    fn tracking_refs_live_under_the_mirror_namespace() {
        assert_eq!(
            tracking_ref("refs/heads/main"),
            "refs/leep-mirror/remote/heads/main"
        );
        assert_eq!(
            tracking_ref("refs/tags/v1"),
            "refs/leep-mirror/remote/tags/v1"
        );
    }
}
