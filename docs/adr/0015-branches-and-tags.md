# 0015. Branches, immutable tags and ref name rules
Status: Accepted
Date: 2026-10-15

## Context
Refs could be listed and moved only through the raw API. A user could not
create, delete or switch a branch from the CLI, and there were no tags. Ref
names were unchecked, so a name with spaces, `..` or a trailing `/` could be
stored and later break URLs, shells and the git bridge.

## Decision
- One validator, `leep_core::refname::validate`, with git's rules plus two
  that avoid ambiguous revisions (`@` and 64-digit hex). The server applies it
  to every ref write: `cas_ref_checked` and `land`. Deletes skip it, so a ref
  stored before the validator can still be removed.
- Namespaces by prefix, with no new storage: a tag `v1` is the ref `tags/v1`;
  every other valid name is a branch.
- Tags are write-once, enforced in `leep-derive` so every caller is covered:
  create (`expected: null`) and delete (`new: null`) are allowed, a move and a
  `land` onto `tags/` are 400. Changing a tag means deleting and recreating it.
- A ref a land policy applies to cannot be deleted, otherwise a protected
  branch could be removed and recreated without the policy.
- `resolve` falls back to `tags/<rev>` after refs and ids, so `v1` works where
  a revision is accepted, and a branch or id of the same name wins.
- The CLI adds `leep branch`, `leep switch` and `leep tag` on top of the
  existing endpoints. All writes are compare-and-swap, and `branch -d` uses the
  merge-base endpoint to check that the branch is merged into the target.

## Consequences
- No change to stored data or to any `/v1` shape. Existing valid names keep
  working; names that were stored but are now invalid stay readable and
  deletable, and cannot be written again.
- Deleting `main` over the API is now refused when a policy covers it. Before
  it was allowed.
- Tags are not signed or annotated. That can be added later as a new object
  without changing this rule.
