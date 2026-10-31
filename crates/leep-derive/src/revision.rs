//! Walking git-style revision suffixes (ADR 0064); the grammar is
//! `leep_core::revision`.

use leep_core::revision::Step;
use leep_core::{ChangesetId, Error, Result};

use crate::Repo;

impl Repo {
    /// Apply `steps` to `id` left to right; `rev` names the whole revision
    /// in errors. `~N` rides the first-parent skiplist (entry `k` is `2^k`
    /// steps up), so it costs `O(log N)` reads however large `N` is.
    pub(crate) async fn walk_steps(
        &self,
        rev: &str,
        mut id: ChangesetId,
        steps: &[Step],
    ) -> Result<ChangesetId> {
        for step in steps {
            id = match *step {
                Step::Parent(0) => id,
                Step::Parent(n) => {
                    let parents = self.parents(id).await?;
                    let k = usize::try_from(n - 1).unwrap_or(usize::MAX);
                    match parents.get(k) {
                        Some(p) => *p,
                        None => {
                            return Err(Error::NotFound(format!(
                                "revision {rev:?}: changeset {id} has {} parent{}, so `^{n}` \
                                 names none",
                                parents.len(),
                                if parents.len() == 1 { "" } else { "s" }
                            )))
                        }
                    }
                }
                Step::Ancestor(n) => self.first_parent_ancestor(rev, id, n).await?,
            };
        }
        Ok(id)
    }

    async fn first_parent_ancestor(
        &self,
        rev: &str,
        start: ChangesetId,
        n: u64,
    ) -> Result<ChangesetId> {
        let (mut id, mut left) = (start, n);
        while left > 0 {
            let list = self.skiplist(id).await?;
            if list.is_empty() {
                return Err(Error::NotFound(format!(
                    "revision {rev:?}: `~{n}` walks past root changeset {id}, {} first-parent \
                     step{} up",
                    n - left,
                    if n - left == 1 { "" } else { "s" }
                )));
            }
            // Largest jump of at most `left` steps the list has.
            let k = (left.ilog2() as usize).min(list.len() - 1);
            id = list[k].id;
            left -= 1 << k;
        }
        Ok(id)
    }
}

#[cfg(test)]
mod tests {
    use leep_core::{ChangesetId, Error};

    use crate::tests::{commit, repo};
    use crate::Repo;

    async fn set(r: &Repo, name: &str, id: ChangesetId) {
        assert!(r.meta().cas_ref(name, None, Some(id)).await.unwrap());
    }

    fn not_found(e: Error, rev: &str) {
        assert!(
            matches!(&e, Error::NotFound(m) if m.contains(rev)),
            "{rev}: {e}"
        );
    }

    #[tokio::test]
    async fn tilde_walks_first_parents_on_a_linear_history() {
        let r = repo();
        let mut chain = vec![commit(&r, &[], &[("f", Some("0"))], "0").await];
        for i in 1..40 {
            let t = i.to_string();
            let c = commit(&r, &[chain[i - 1]], &[("f", Some(&t))], &t).await;
            chain.push(c);
        }
        set(&r, "main", chain[39]).await;
        for n in 0..40 {
            assert_eq!(
                r.resolve(&format!("main~{n}")).await.unwrap(),
                chain[39 - n]
            );
        }
        assert_eq!(r.resolve("main~").await.unwrap(), chain[38]);
        assert_eq!(r.resolve("main^").await.unwrap(), chain[38]);
        assert_eq!(r.resolve("main^1").await.unwrap(), chain[38]);
        assert_eq!(r.resolve("main^0").await.unwrap(), chain[39]);
        assert_eq!(r.resolve("main~~~").await.unwrap(), chain[36]);
        assert_eq!(r.resolve("main~3~4").await.unwrap(), chain[32]);
        let full = format!("{}~5", chain[20]);
        assert_eq!(r.resolve(&full).await.unwrap(), chain[15]);
        // Tags resolve by short name, then walk.
        set(&r, "tags/v1", chain[10]).await;
        assert_eq!(r.resolve("v1~10").await.unwrap(), chain[0]);
        assert_eq!(r.resolve("tags/v1^").await.unwrap(), chain[9]);
    }

    #[tokio::test]
    async fn walking_past_a_root_is_not_found() {
        let r = repo();
        let a = commit(&r, &[], &[("f", Some("a"))], "a").await;
        let b = commit(&r, &[a], &[("f", Some("b"))], "b").await;
        set(&r, "main", b).await;
        assert_eq!(r.resolve("main~1").await.unwrap(), a);
        for rev in ["main~2", "main~100", "main^^", "main^2", "main~1^1"] {
            not_found(r.resolve(rev).await.unwrap_err(), rev);
        }
        let big = format!("main~{}", u64::MAX);
        not_found(r.resolve(&big).await.unwrap_err(), &big);
        not_found(r.resolve("nope~1").await.unwrap_err(), "nope~1");
    }

    #[tokio::test]
    async fn caret_picks_merge_parents_and_chains() {
        // a - b - m - n   (main)
        //      \ /
        //       c - d     (m = merge of b and d)
        let r = repo();
        let a = commit(&r, &[], &[("f", Some("a"))], "a").await;
        let b = commit(&r, &[a], &[("f", Some("b"))], "b").await;
        let c = commit(&r, &[a], &[("g", Some("c"))], "c").await;
        let d = commit(&r, &[c], &[("g", Some("d"))], "d").await;
        let m = commit(&r, &[b, d], &[("h", Some("m"))], "m").await;
        let n = commit(&r, &[m], &[("h", Some("n"))], "n").await;
        set(&r, "main", n).await;
        assert_eq!(r.resolve("main~1").await.unwrap(), m);
        assert_eq!(r.resolve("main~2").await.unwrap(), b);
        assert_eq!(r.resolve("main~3").await.unwrap(), a);
        assert_eq!(r.resolve("main~1^2").await.unwrap(), d);
        assert_eq!(r.resolve("main^^2").await.unwrap(), d);
        assert_eq!(r.resolve("main~1^2~1").await.unwrap(), c);
        assert_eq!(r.resolve("main~1^2^0~2").await.unwrap(), a);
        assert_eq!(r.resolve("main^^1").await.unwrap(), b);
        not_found(r.resolve("main~1^3").await.unwrap_err(), "main~1^3");
        not_found(r.resolve("main^2").await.unwrap_err(), "main^2");
        not_found(r.resolve("main~1^2~3").await.unwrap_err(), "main~1^2~3");
    }

    #[tokio::test]
    async fn malformed_suffixes_and_reflog_syntax_are_invalid() {
        let r = repo();
        let a = commit(&r, &[], &[("f", Some("a"))], "a").await;
        set(&r, "main", a).await;
        for rev in [
            "main~x",
            "main^{commit}",
            "main~1-",
            "~1",
            "main@{1}",
            "@{-1}",
        ] {
            let e = r.resolve(rev).await.unwrap_err();
            assert!(matches!(e, Error::Invalid(_)), "{rev}: {e}");
        }
        // No suffix at all keeps the old answer.
        not_found(r.resolve("nope").await.unwrap_err(), "nope");
    }

    #[tokio::test]
    async fn a_stored_name_with_suffix_characters_resolves_to_itself() {
        // Names with `~` or `^` are refused today, but a ref stored before
        // the rule must not be misread as a suffix.
        let r = repo();
        let a = commit(&r, &[], &[("f", Some("a"))], "a").await;
        let b = commit(&r, &[a], &[("f", Some("b"))], "b").await;
        let c = commit(&r, &[b], &[("f", Some("c"))], "c").await;
        set(&r, "old", c).await;
        set(&r, "old~1", c).await;
        set(&r, "x^2", a).await;
        assert_eq!(r.resolve("old~1").await.unwrap(), c);
        assert_eq!(r.resolve("x^2").await.unwrap(), a);
        // Past the exact name, the suffix starts at the first `~` or `^`.
        assert_eq!(r.resolve("old~1~1").await.unwrap(), a);
    }
}
