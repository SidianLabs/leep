//! Every kind of derived row, and what verification and repair may do with it
//! (ADR 0025). The table here is the single source for `verify --derived`,
//! `repair rederive` and `repair drop`, and for the lists in the docs.

use leep_core::review::{REVIEW_KIND, REVIEW_SOURCE_KIND};
use leep_core::signature::SIGNATURE_KIND;
use leep_core::Error;

use crate::changeset_gc::{CS_CONDEMNED, CS_DOOM, CS_RESCUE, GC_FENCE_ACTIVE};
use crate::changeset_reach::CS_REACH;
use crate::derived::{
    DerivedKind, Generation, HistoryTree, LastMerge, LogFilterIndex, RootTree, Skiplist,
    HISTORY_NODE_KIND, LOG_FILTER_NAME_KIND,
};
use crate::gc::{BLOB_CLAIM, BLOB_CONDEMNED, BLOB_DOOM, BLOB_RESCUE, GC_MARK};
use crate::search::{BLOB_KIND as BLOB_TRIGRAMS, TREE_KIND as TREE_TRIGRAMS};
use crate::signatures::REBASED_ORIGIN_KIND;

/// Name of the blame cache kind, keyed by a hash of (changeset, path).
pub const BLAME_KIND: &str = "blame";

/// Name of the kind holding history-index nodes.
pub const HISTORY_NODE: &str = HISTORY_NODE_KIND;

/// How rows of a kind are found and checked.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KindClass {
    /// One row per changeset, a pure function of the changeset and its
    /// parents' rows: recomputed in history order.
    PerChangeset,
    /// Keyed by the hash of the object the row describes: recomputed from
    /// that object.
    Content,
    /// Keyed by a hash of (changeset, path): checked for paths that can be
    /// named, never enumerated.
    PerPath,
    /// Attestations written by clients: checked for structure, not
    /// recomputed.
    Attestation,
    /// Garbage collection and rebase bookkeeping: checked for structure.
    Bookkeeping,
}

/// What `repair drop` does with a kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DropPolicy {
    /// Rows derive again from history on demand; the listed kinds go with it
    /// because they only make sense together.
    Allowed { also: &'static [&'static str] },
    /// Refused, for this reason.
    Refused(&'static str),
}

/// What `repair rederive` does with a kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Rederive {
    Yes,
    /// Not directly; the text says what to do instead.
    No(&'static str),
}

#[derive(Clone, Copy, Debug)]
pub struct KindInfo {
    pub name: &'static str,
    pub class: KindClass,
    /// One line for `--help`-style listings and the docs.
    pub summary: &'static str,
    pub rederive: Rederive,
    pub drop: DropPolicy,
}

const GC_REFUSAL: &str = "garbage collection fence state (ADR 0012), not derived from history; \
    dropping it can let a collect delete a blob a client was just told it may reuse";

const CS_GC_REFUSAL: &str = "changeset garbage collection fence state (ADR 0059), not derived \
    from history; dropping it can let a collect delete a changeset a writer rescued";

/// All kinds, in the order a pass must check or rewrite them: a kind comes
/// after every kind its recomputation reads.
static KINDS: &[KindInfo] = &[
    KindInfo {
        name: Generation::NAME,
        class: KindClass::PerChangeset,
        summary: "1 + the largest parent generation",
        rederive: Rederive::Yes,
        drop: DropPolicy::Allowed { also: &[] },
    },
    KindInfo {
        name: Skiplist::NAME,
        class: KindClass::PerChangeset,
        summary: "first-parent ancestors at power-of-two distances",
        rederive: Rederive::Yes,
        drop: DropPolicy::Allowed { also: &[] },
    },
    KindInfo {
        name: LastMerge::NAME,
        class: KindClass::PerChangeset,
        summary: "generation of the nearest merge on the first-parent chain",
        rederive: Rederive::Yes,
        drop: DropPolicy::Allowed { also: &[] },
    },
    KindInfo {
        name: RootTree::NAME,
        class: KindClass::PerChangeset,
        summary: "hash of the changeset's root tree (rewriting also restores tree nodes)",
        rederive: Rederive::Yes,
        drop: DropPolicy::Allowed { also: &[] },
    },
    KindInfo {
        name: HistoryTree::NAME,
        class: KindClass::PerChangeset,
        summary: "pack of path-history nodes naming the index's root (rewriting also \
                  restores the pack's other parts)",
        rederive: Rederive::Yes,
        drop: DropPolicy::Allowed {
            also: &[HISTORY_NODE_KIND],
        },
    },
    KindInfo {
        name: LogFilterIndex::NAME,
        class: KindClass::PerChangeset,
        summary: "authors and agent values in stretches of history, for filtered logs \
                  (rewriting also restores author names)",
        rederive: Rederive::Yes,
        drop: DropPolicy::Allowed {
            also: &[LOG_FILTER_NAME_KIND],
        },
    },
    KindInfo {
        name: LOG_FILTER_NAME_KIND,
        class: KindClass::Content,
        summary: "lower-cased author name behind a log-filter fingerprint",
        rederive: Rederive::No("rewritten by `rederive --kind log-filter-v1`"),
        drop: DropPolicy::Refused(
            "log-filter-v1 names its authors by these rows; drop log-filter-v1, which removes both",
        ),
    },
    KindInfo {
        name: BLOB_TRIGRAMS,
        class: KindClass::Content,
        summary: "trigram set of one blob (code search)",
        rederive: Rederive::Yes,
        drop: DropPolicy::Allowed { also: &[] },
    },
    KindInfo {
        name: TREE_TRIGRAMS,
        class: KindClass::Content,
        summary: "bloom filter over the trigrams below one directory (code search)",
        rederive: Rederive::Yes,
        drop: DropPolicy::Allowed { also: &[] },
    },
    KindInfo {
        name: HISTORY_NODE_KIND,
        class: KindClass::Content,
        summary: "part of a changeset's pack of path-history nodes, after the first",
        rederive: Rederive::No("rewritten by `rederive --kind history-tree-v4`"),
        drop: DropPolicy::Refused(
            "history-tree-v4 rows point at these parts; drop history-tree-v4, which removes both",
        ),
    },
    KindInfo {
        name: BLAME_KIND,
        class: KindClass::PerPath,
        summary: "cached blame of one file at one changeset",
        rederive: Rederive::No(
            "rows are keyed by a hash of (changeset, path) and cannot be listed; \
             use `repair drop --kind blame`, and they are recomputed when asked for",
        ),
        drop: DropPolicy::Allowed { also: &[] },
    },
    KindInfo {
        name: SIGNATURE_KIND,
        class: KindClass::Attestation,
        summary: "a signature a client attached to a changeset",
        rederive: Rederive::No("signatures are not computed from history"),
        drop: DropPolicy::Refused(
            "signatures are attestations clients submitted; they are not computed from \
             history, so a dropped row is gone for good",
        ),
    },
    KindInfo {
        name: REVIEW_KIND,
        class: KindClass::Attestation,
        summary: "a reviewer's verdict on a changeset",
        rederive: Rederive::No("reviews are not computed from history"),
        drop: DropPolicy::Refused(
            "reviews are verdicts people recorded; they are not computed from history, \
             so a dropped row is gone for good",
        ),
    },
    KindInfo {
        name: REVIEW_SOURCE_KIND,
        class: KindClass::Attestation,
        summary: "which token posted a review, and where it said it came from",
        rederive: Rederive::No("review sources are not computed from history"),
        drop: DropPolicy::Refused(
            "review sources are audit records written with the reviews; they are not \
             computed from history, so a dropped row is gone for good",
        ),
    },
    KindInfo {
        name: REBASED_ORIGIN_KIND,
        class: KindClass::Bookkeeping,
        summary: "link from a server-made rebased copy to the changeset it came from",
        rederive: Rederive::No("written by rebase; it cannot be recomputed"),
        drop: DropPolicy::Refused(
            "written by rebase to link a server-made copy to its original; it cannot be \
             recomputed from history, and land retries and signature lookups depend on it",
        ),
    },
    KindInfo {
        name: BLOB_CLAIM,
        class: KindClass::Bookkeeping,
        summary: "when a client was last told a blob exists",
        rederive: Rederive::No("garbage collection state is not derived"),
        drop: DropPolicy::Refused(GC_REFUSAL),
    },
    KindInfo {
        name: BLOB_CONDEMNED,
        class: KindClass::Bookkeeping,
        summary: "when garbage collection condemned a blob",
        rederive: Rederive::No("garbage collection state is not derived"),
        drop: DropPolicy::Refused(GC_REFUSAL),
    },
    KindInfo {
        name: BLOB_RESCUE,
        class: KindClass::Bookkeeping,
        summary: "the condemnation an upload answered",
        rederive: Rederive::No("garbage collection state is not derived"),
        drop: DropPolicy::Refused(GC_REFUSAL),
    },
    KindInfo {
        name: BLOB_DOOM,
        class: KindClass::Bookkeeping,
        summary: "state of a blob's delete handshake",
        rederive: Rederive::No("garbage collection state is not derived"),
        drop: DropPolicy::Refused(GC_REFUSAL),
    },
    KindInfo {
        name: CS_CONDEMNED,
        class: KindClass::Bookkeeping,
        summary: "when garbage collection condemned an unreachable changeset",
        rederive: Rederive::No("garbage collection state is not derived"),
        drop: DropPolicy::Refused(CS_GC_REFUSAL),
    },
    KindInfo {
        name: CS_RESCUE,
        class: KindClass::Bookkeeping,
        summary: "the condemnation a writer answered by naming the changeset",
        rederive: Rederive::No("garbage collection state is not derived"),
        drop: DropPolicy::Refused(CS_GC_REFUSAL),
    },
    KindInfo {
        name: CS_DOOM,
        class: KindClass::Bookkeeping,
        summary: "state of a changeset's delete handshake",
        rederive: Rederive::No("garbage collection state is not derived"),
        drop: DropPolicy::Refused(CS_GC_REFUSAL),
    },
    KindInfo {
        name: crate::changeset_sweep::CS_TOMBSTONE,
        class: KindClass::Bookkeeping,
        summary: "when the changeset sweep deleted a changeset, and its root tree",
        rederive: Rederive::No("garbage collection state is not derived"),
        drop: DropPolicy::Refused(
            "gc --full keeps the root trees of changesets deleted within --backup-fence \
             (ADR 0059); dropping tombstones can delete tree nodes a backup in flight needs",
        ),
    },
    KindInfo {
        name: GC_FENCE_ACTIVE,
        class: KindClass::Bookkeeping,
        summary: "when the current changeset condemnation round began, or 0",
        rederive: Rederive::No("garbage collection state is not derived"),
        drop: DropPolicy::Refused(CS_GC_REFUSAL),
    },
    KindInfo {
        name: CS_REACH,
        class: KindClass::Bookkeeping,
        summary: "changesets a ref or ref log entry reaches, for changeset garbage collection",
        rederive: Rederive::No("rewritten by the next changeset reachability mark with --full"),
        drop: DropPolicy::Refused(
            "the reachability mark stops walking at a changeset with a row (ADR 0059); \
             dropping part of the kind can leave a row whose ancestry lost its rows",
        ),
    },
    KindInfo {
        name: GC_MARK,
        class: KindClass::Bookkeeping,
        summary: "what earlier garbage collection marks covered",
        rederive: Rederive::No("`leep-server gc --full` walks all of history and rewrites it"),
        drop: DropPolicy::Refused(
            "incremental garbage collection stops walking at a marked tree node (ADR 0033); \
             dropping part of the kind can leave a row whose subtree lost its rows, and the \
             next collect would delete blobs a changeset references",
        ),
    },
];

/// Kinds an older version wrote and this one neither reads nor writes. Their
/// rows are garbage here but an older server sharing the store still uses
/// them, so they stay until an operator runs `repair drop` on them, the only
/// command that accepts these names.
static RETIRED: &[KindInfo] = &[
    KindInfo {
        name: "history-tree-v1",
        class: KindClass::PerChangeset,
        summary: "path-history roots of the whole-node encoding, replaced by history-tree-v2 \
                  (ADR 0027) and then history-tree-v3 (ADR 0037)",
        rederive: Rederive::No("retired; this version derives history-tree-v4 instead"),
        drop: DropPolicy::Allowed {
            also: &["history-node-v1"],
        },
    },
    KindInfo {
        name: "history-node-v1",
        class: KindClass::Content,
        summary: "whole path-history nodes, replaced by history-node-v2 (ADR 0027)",
        rederive: Rederive::No("retired; this version derives history-tree-v4 instead"),
        drop: DropPolicy::Refused(
            "history-tree-v1 rows point at these nodes; drop history-tree-v1, which removes both",
        ),
    },
    KindInfo {
        name: "history-tree-v2",
        class: KindClass::PerChangeset,
        summary: "path-history roots of the chunked encoding without patches, replaced by \
                  history-tree-v3 (ADR 0037)",
        rederive: Rederive::No("retired; this version derives history-tree-v4 instead"),
        drop: DropPolicy::Allowed {
            also: &["history-node-v2"],
        },
    },
    KindInfo {
        name: "history-node-v2",
        class: KindClass::Content,
        summary: "chunked path-history nodes without patches, replaced by history-node-v3 \
                  (ADR 0037)",
        rederive: Rederive::No("retired; this version derives history-tree-v4 instead"),
        drop: DropPolicy::Refused(
            "history-tree-v2 rows point at these nodes; drop history-tree-v2, which removes both",
        ),
    },
    KindInfo {
        name: "history-tree-v3",
        class: KindClass::PerChangeset,
        summary: "path-history roots of the content-addressed encoding, one row per node, \
                  replaced by history-tree-v4 packs (ADR 0045)",
        rederive: Rederive::No("retired; this version derives history-tree-v4 instead"),
        drop: DropPolicy::Allowed {
            also: &["history-node-v3"],
        },
    },
    KindInfo {
        name: "history-node-v3",
        class: KindClass::Content,
        summary: "content-addressed path-history nodes, chunks and patches, replaced by \
                  history-node-v4 packs (ADR 0045)",
        rederive: Rederive::No("retired; this version derives history-tree-v4 instead"),
        drop: DropPolicy::Refused(
            "history-tree-v3 rows point at these nodes; drop history-tree-v3, which removes both",
        ),
    },
];

/// Every kind, in dependency order.
pub fn kinds() -> &'static [KindInfo] {
    KINDS
}

/// Kinds an older version wrote and this one no longer uses, oldest first.
/// Only `repair drop` and `upgrade-index` act on them.
pub fn retired() -> &'static [KindInfo] {
    RETIRED
}

pub fn lookup(name: &str) -> Option<&'static KindInfo> {
    KINDS.iter().find(|k| k.name == name)
}

/// Comma-separated names, for error messages.
pub fn names() -> String {
    KINDS.iter().map(|k| k.name).collect::<Vec<_>>().join(", ")
}

/// The kinds whose rows `kind`'s recomputation reads. Checking or rewriting
/// `kind` alone would compare against rows that may be damaged, so a pass
/// includes these.
pub fn dependencies(kind: &str) -> &'static [&'static str] {
    if kind == Skiplist::NAME || kind == LastMerge::NAME {
        &[Generation::NAME]
    } else if kind == HistoryTree::NAME {
        &[Generation::NAME, Skiplist::NAME, RootTree::NAME]
    } else if kind == LogFilterIndex::NAME {
        &[Generation::NAME, Skiplist::NAME, LastMerge::NAME]
    } else if kind == TREE_TRIGRAMS {
        &[BLOB_TRIGRAMS]
    } else {
        &[]
    }
}

/// `selected` plus everything it depends on, deduplicated, in table order.
pub fn with_dependencies(selected: &[&str]) -> Vec<&'static str> {
    KINDS
        .iter()
        .filter(|k| {
            selected
                .iter()
                .any(|s| *s == k.name || dependencies(s).contains(&k.name))
        })
        .map(|k| k.name)
        .collect()
}

/// Look `name` up for `repair drop`, or say why it may not be dropped.
pub fn check_droppable(name: &str) -> Result<&'static KindInfo, Error> {
    if name == "ref_log" {
        return Err(Error::Invalid(
            "ref_log is not derived data: it is the only record of how refs moved \
             and cannot be dropped"
                .into(),
        ));
    }
    let info = lookup(name)
        .or_else(|| RETIRED.iter().find(|k| k.name == name))
        .ok_or_else(|| {
            Error::Invalid(format!("unknown kind {name:?}; known kinds: {}", names()))
        })?;
    match info.drop {
        DropPolicy::Allowed { .. } => Ok(info),
        DropPolicy::Refused(why) => Err(Error::Invalid(format!("refusing to drop {name}: {why}"))),
    }
}

/// Look `name` up for `repair rederive`, or say what to do instead.
pub fn check_rederivable(name: &str) -> Result<&'static KindInfo, Error> {
    let info = lookup(name).ok_or_else(|| {
        Error::Invalid(format!("unknown kind {name:?}; known kinds: {}", names()))
    })?;
    match info.rederive {
        Rederive::Yes => Ok(info),
        Rederive::No(why) => Err(Error::Invalid(format!("cannot rederive {name}: {why}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_are_unique_and_dependencies_come_first() {
        for (i, k) in KINDS.iter().enumerate() {
            assert!(KINDS[..i].iter().all(|o| o.name != k.name), "{}", k.name);
            for d in dependencies(k.name) {
                let at = KINDS.iter().position(|o| o.name == *d).expect("known kind");
                assert!(at < i, "{d} must precede {}", k.name);
            }
        }
    }

    #[test]
    fn dependencies_expand_in_table_order() {
        assert_eq!(
            with_dependencies(&[HistoryTree::NAME]),
            [
                Generation::NAME,
                Skiplist::NAME,
                RootTree::NAME,
                HistoryTree::NAME
            ]
        );
        assert_eq!(with_dependencies(&[BLAME_KIND]), [BLAME_KIND]);
    }

    #[test]
    fn only_rederivable_kinds_may_be_dropped() {
        for k in KINDS {
            let droppable = check_droppable(k.name).is_ok();
            let attestation_or_state =
                matches!(k.class, KindClass::Attestation | KindClass::Bookkeeping);
            assert_eq!(
                droppable,
                !attestation_or_state
                    && k.name != HISTORY_NODE_KIND
                    && k.name != LOG_FILTER_NAME_KIND
            );
        }
        assert!(check_droppable("ref_log").is_err());
        assert!(check_droppable("no-such-kind").is_err());
        // Retired kinds can only be dropped, and only together.
        let old = check_droppable("history-tree-v1").unwrap();
        assert_eq!(
            old.drop,
            DropPolicy::Allowed {
                also: &["history-node-v1"]
            }
        );
        assert!(check_droppable("history-node-v1").is_err());
        assert!(check_rederivable("history-tree-v1").is_err());
        assert!(lookup("history-tree-v1").is_none());
        let v2 = check_droppable("history-tree-v2").unwrap();
        assert_eq!(
            v2.drop,
            DropPolicy::Allowed {
                also: &["history-node-v2"]
            }
        );
        assert!(check_droppable("history-node-v2").is_err());
        assert!(check_rederivable("history-tree-v2").is_err());
        assert!(lookup("history-tree-v2").is_none());
        let v3 = check_droppable("history-tree-v3").unwrap();
        assert_eq!(
            v3.drop,
            DropPolicy::Allowed {
                also: &["history-node-v3"]
            }
        );
        assert!(check_droppable("history-node-v3").is_err());
        assert!(check_rederivable("history-tree-v3").is_err());
        assert!(lookup("history-tree-v3").is_none());
        assert_eq!(HistoryTree::NAME, "history-tree-v4");
    }
}
