use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use leep_core::prefix::IdPrefix;
use leep_core::{
    Changeset, ChangesetId, DeleteStep, Hash, LandId, LandLease, LandRecord, LeaseClaim,
    LeaseTiming, MetaStore, RefLogEntry, RefStep, RefUpdateContext, Result,
};

use crate::plan::{FaultPlan, Op};

/// A [`MetaStore`] that injects the faults of a [`FaultPlan`] around calls to
/// `inner`. `FailAfter` on `cas_ref_logged` is the ambiguous commit: the ref
/// moved and its log entry was written, and the caller saw an error.
pub struct FaultyMetaStore {
    inner: Arc<dyn MetaStore>,
    plan: Arc<FaultPlan>,
}

impl FaultyMetaStore {
    pub fn new(inner: Arc<dyn MetaStore>, plan: Arc<FaultPlan>) -> Self {
        Self { inner, plan }
    }

    pub fn plan(&self) -> &Arc<FaultPlan> {
        &self.plan
    }
}

#[async_trait]
impl MetaStore for FaultyMetaStore {
    async fn put_changeset(&self, cs: &Changeset) -> Result<ChangesetId> {
        self.plan
            .run(Op::PutChangeset, self.inner.put_changeset(cs), |_, v| v)
            .await
    }

    async fn get_changeset(&self, id: &ChangesetId) -> Result<Option<Changeset>> {
        self.plan
            .run(Op::GetChangeset, self.inner.get_changeset(id), |_, v| v)
            .await
    }

    async fn list_changesets(
        &self,
        after: Option<ChangesetId>,
        limit: usize,
    ) -> Result<Vec<ChangesetId>> {
        self.plan
            .run(
                Op::ListChangesets,
                self.inner.list_changesets(after, limit),
                |_, v| v,
            )
            .await
    }

    /// Faults as for `list_changesets`, the read it stands in for.
    async fn changesets_with_prefix(
        &self,
        prefix: &IdPrefix,
        limit: usize,
    ) -> Result<Vec<ChangesetId>> {
        self.plan
            .run(
                Op::ListChangesets,
                self.inner.changesets_with_prefix(prefix, limit),
                |_, v| v,
            )
            .await
    }

    async fn get_ref(&self, name: &str) -> Result<Option<ChangesetId>> {
        self.plan
            .run(Op::GetRef, self.inner.get_ref(name), |_, v| v)
            .await
    }

    async fn cas_ref_logged(
        &self,
        name: &str,
        expected: Option<ChangesetId>,
        new: Option<ChangesetId>,
        ctx: &RefUpdateContext,
    ) -> Result<bool> {
        self.plan
            .run(
                Op::CasRef,
                self.inner.cas_ref_logged(name, expected, new, ctx),
                |_, v| v,
            )
            .await
    }

    fn supports_land_leases(&self) -> bool {
        self.inner.supports_land_leases()
    }

    async fn land_lease(&self, ref_name: &str, timing: LeaseTiming) -> Result<Option<LandLease>> {
        self.plan
            .run(Op::Lease, self.inner.land_lease(ref_name, timing), |_, v| v)
            .await
    }

    async fn acquire_land_lease(
        &self,
        claim: &LeaseClaim<'_>,
        timing: LeaseTiming,
    ) -> Result<LandLease> {
        self.plan
            .run(
                Op::Lease,
                self.inner.acquire_land_lease(claim, timing),
                |_, v| v,
            )
            .await
    }

    async fn renew_land_leases(
        &self,
        holder: &str,
        held: &[(String, u64)],
        timing: LeaseTiming,
    ) -> Result<Vec<Option<std::time::Duration>>> {
        self.plan
            .run(
                Op::Lease,
                self.inner.renew_land_leases(holder, held, timing),
                |_, v| v,
            )
            .await
    }

    async fn release_land_lease(&self, ref_name: &str, holder: &str, epoch: u64) -> Result<bool> {
        self.plan
            .run(
                Op::Lease,
                self.inner.release_land_lease(ref_name, holder, epoch),
                |_, v| v,
            )
            .await
    }

    /// Faults as for `cas_ref_logged`; `FailAfter` applied the whole chain.
    async fn cas_ref_logged_chain(
        &self,
        name: &str,
        expected: Option<ChangesetId>,
        steps: &[RefStep],
    ) -> Result<usize> {
        self.plan
            .run(
                Op::CasRef,
                self.inner.cas_ref_logged_chain(name, expected, steps),
                |_, v| v,
            )
            .await
    }

    async fn find_lands(&self, name: &str, ids: &[LandId]) -> Result<Vec<Option<LandRecord>>> {
        self.plan
            .run(Op::FindLands, self.inner.find_lands(name, ids), |_, v| v)
            .await
    }

    async fn ref_log(
        &self,
        name: Option<&str>,
        before_seq: Option<u64>,
        limit: usize,
    ) -> Result<Vec<RefLogEntry>> {
        self.plan
            .run(
                Op::RefLog,
                self.inner.ref_log(name, before_seq, limit),
                |_, v| v,
            )
            .await
    }

    async fn list_refs(&self) -> Result<BTreeMap<String, ChangesetId>> {
        self.plan
            .run(Op::ListRefs, self.inner.list_refs(), |_, v| v)
            .await
    }

    async fn get_derived(&self, kind: &str, key: &Hash) -> Result<Option<Bytes>> {
        self.plan
            .run(
                Op::GetDerived,
                self.inner.get_derived(kind, key),
                |plan, v| v.map(|b| Bytes::from(plan.corrupt(&b))),
            )
            .await
    }

    async fn list_derived(
        &self,
        kind: &str,
        after: Option<Hash>,
        limit: usize,
    ) -> Result<Vec<(Hash, Bytes)>> {
        self.plan
            .run(
                Op::ListDerived,
                self.inner.list_derived(kind, after, limit),
                |_, v| v,
            )
            .await
    }

    async fn count_derived(&self, kind: &str) -> Result<u64> {
        self.plan
            .run(Op::ListDerived, self.inner.count_derived(kind), |_, v| v)
            .await
    }

    async fn estimate_derived(&self, kind: &str) -> Result<leep_core::RowCount> {
        self.plan
            .run(Op::ListDerived, self.inner.estimate_derived(kind), |_, v| v)
            .await
    }

    /// Each step is one faultable call; `delete_derived_kind` is their loop.
    async fn delete_derived_step(
        &self,
        kinds: &[&str],
        cursor: Option<&[u8]>,
    ) -> Result<DeleteStep> {
        self.plan
            .run(
                Op::DeleteDerivedKind,
                self.inner.delete_derived_step(kinds, cursor),
                |_, v| v,
            )
            .await
    }

    async fn put_derived(&self, kind: &str, key: &Hash, value: Bytes) -> Result<()> {
        self.plan
            .run(
                Op::PutDerived,
                self.inner.put_derived(kind, key, value),
                |_, v| v,
            )
            .await
    }
}
