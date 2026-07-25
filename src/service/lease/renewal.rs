use std::cmp::max;

use crate::decode_tso;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::service) enum GeneratorLeaseRefreshReason {
    Maintenance,
    ExplicitRenewal,
    HorizonRequired,
}

impl GeneratorLeaseRefreshReason {
    fn forces_lease_renewal(self) -> bool {
        matches!(self, Self::ExplicitRenewal)
    }

    fn forces_upper_bound(self) -> bool {
        matches!(self, Self::ExplicitRenewal | Self::HorizonRequired)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::service) struct GeneratorLeaseRefreshPlan {
    candidate_last: Option<u64>,
    previous_lease_expire_at_ms: u64,
    previous_issued_upper_bound: Option<u64>,
    next_upper_bound_base_ms: u64,
    should_renew_lease: bool,
    should_extend_upper_bound: bool,
}

impl GeneratorLeaseRefreshPlan {
    pub(in crate::service) fn build(
        lease_expire_at_ms: u64,
        last_persisted_tso: Option<u64>,
        issued_upper_bound: Option<u64>,
        local_last_issued_tso: Option<u64>,
        now_ms: u64,
        ttl_ms: u64,
        pre_borrow_ms: u64,
    ) -> Self {
        let should_renew_lease = lease_expire_at_ms <= now_ms.saturating_add(ttl_ms / 2);
        let candidate_last = merge_max(last_persisted_tso, local_last_issued_tso);
        let current_upper_bound = merge_max(issued_upper_bound, candidate_last);
        let should_extend_upper_bound = current_upper_bound
            .map(|upper_bound| {
                decode_tso(upper_bound).physical_ms <= now_ms.saturating_add(pre_borrow_ms / 2)
            })
            .unwrap_or(true);
        let next_upper_bound_base_ms = current_upper_bound
            .map(|upper_bound| decode_tso(upper_bound).physical_ms)
            .map(|upper_bound_ms| max(now_ms, upper_bound_ms))
            .unwrap_or(now_ms);

        Self {
            candidate_last,
            previous_lease_expire_at_ms: lease_expire_at_ms,
            previous_issued_upper_bound: issued_upper_bound,
            next_upper_bound_base_ms,
            should_renew_lease,
            should_extend_upper_bound,
        }
    }

    pub(in crate::service) fn candidate_last(&self) -> Option<u64> {
        self.candidate_last
    }

    pub(in crate::service) fn needs_write(&self, reason: GeneratorLeaseRefreshReason) -> bool {
        reason.forces_lease_renewal()
            || reason.forces_upper_bound()
            || self.should_renew_lease
            || self.should_extend_upper_bound
    }

    pub(in crate::service) fn should_extend_upper_bound(
        &self,
        reason: GeneratorLeaseRefreshReason,
    ) -> bool {
        reason.forces_upper_bound() || self.should_extend_upper_bound
    }

    pub(in crate::service) fn record_lease_expire_at_ms(
        &self,
        refreshed_lease_expire_at_ms: u64,
        reason: GeneratorLeaseRefreshReason,
    ) -> u64 {
        if reason.forces_lease_renewal() || self.should_renew_lease {
            refreshed_lease_expire_at_ms
        } else {
            self.previous_lease_expire_at_ms
        }
    }

    pub(in crate::service) fn next_upper_bound_base_ms(&self) -> u64 {
        self.next_upper_bound_base_ms
    }

    pub(in crate::service) fn record_issued_upper_bound(
        &self,
        next_upper_bound: Option<u64>,
        reason: GeneratorLeaseRefreshReason,
    ) -> Option<u64> {
        if self.should_extend_upper_bound(reason) {
            merge_max(self.previous_issued_upper_bound, next_upper_bound)
        } else {
            self.previous_issued_upper_bound
        }
    }
}

fn merge_max(lhs: Option<u64>, rhs: Option<u64>) -> Option<u64> {
    match (lhs, rhs) {
        (Some(a), Some(b)) => Some(max(a, b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use crate::encode_tso;

    use super::{GeneratorLeaseRefreshPlan, GeneratorLeaseRefreshReason};

    #[test]
    fn generator_lease_refresh_plan_stays_idle_with_fresh_lease_and_headroom() {
        let existing_upper_bound = encode_tso(260, 7, 0).expect("upper bound should encode");
        let plan = GeneratorLeaseRefreshPlan::build(
            300,
            Some(encode_tso(200, 7, 3).expect("persisted tso should encode")),
            Some(existing_upper_bound),
            Some(encode_tso(180, 7, 2).expect("local tso should encode")),
            100,
            200,
            100,
        );

        assert_eq!(
            plan.candidate_last(),
            Some(encode_tso(200, 7, 3).expect("persisted tso should encode"))
        );
        assert!(!plan.needs_write(GeneratorLeaseRefreshReason::Maintenance));
        assert_eq!(plan.next_upper_bound_base_ms(), 260);
        assert_eq!(
            plan.record_lease_expire_at_ms(400, GeneratorLeaseRefreshReason::Maintenance),
            300
        );
        assert_eq!(
            plan.record_issued_upper_bound(None, GeneratorLeaseRefreshReason::Maintenance),
            Some(existing_upper_bound)
        );
    }

    #[test]
    fn generator_lease_refresh_plan_prefers_newer_local_last_and_extends_upper_bound() {
        let existing_upper_bound = encode_tso(110, 9, 0).expect("upper bound should encode");
        let local_last = encode_tso(120, 9, 4).expect("local tso should encode");
        let next_upper_bound = encode_tso(170, 9, 0).expect("next upper bound should encode");
        let plan = GeneratorLeaseRefreshPlan::build(
            140,
            Some(encode_tso(100, 9, 1).expect("persisted tso should encode")),
            Some(existing_upper_bound),
            Some(local_last),
            100,
            200,
            100,
        );

        assert_eq!(plan.candidate_last(), Some(local_last));
        assert!(plan.needs_write(GeneratorLeaseRefreshReason::Maintenance));
        assert_eq!(plan.next_upper_bound_base_ms(), 120);
        assert_eq!(
            plan.record_lease_expire_at_ms(300, GeneratorLeaseRefreshReason::Maintenance),
            300
        );
        assert_eq!(
            plan.record_issued_upper_bound(
                Some(next_upper_bound),
                GeneratorLeaseRefreshReason::Maintenance,
            ),
            Some(next_upper_bound)
        );
    }

    #[test]
    fn generator_lease_refresh_plan_does_not_write_for_checkpoint_only_progress() {
        let issued_upper_bound = encode_tso(260, 9, 0).expect("upper bound should encode");
        let local_last = encode_tso(210, 9, 4).expect("local tso should encode");
        let plan = GeneratorLeaseRefreshPlan::build(
            300,
            Some(encode_tso(200, 9, 1).expect("persisted tso should encode")),
            Some(issued_upper_bound),
            Some(local_last),
            100,
            200,
            100,
        );

        assert_eq!(plan.candidate_last(), Some(local_last));
        assert!(!plan.needs_write(GeneratorLeaseRefreshReason::Maintenance));
        assert_eq!(
            plan.record_lease_expire_at_ms(300, GeneratorLeaseRefreshReason::Maintenance),
            300
        );
        assert_eq!(
            plan.record_issued_upper_bound(None, GeneratorLeaseRefreshReason::Maintenance),
            Some(issued_upper_bound)
        );
    }

    #[test]
    fn generator_lease_refresh_plan_renews_lease_without_extending_horizon() {
        let issued_upper_bound = encode_tso(260, 9, 0).expect("upper bound should encode");
        let plan = GeneratorLeaseRefreshPlan::build(
            150,
            None,
            Some(issued_upper_bound),
            None,
            100,
            100,
            100,
        );

        assert!(plan.needs_write(GeneratorLeaseRefreshReason::Maintenance));
        assert!(!plan.should_extend_upper_bound(GeneratorLeaseRefreshReason::Maintenance));
        assert_eq!(
            plan.record_lease_expire_at_ms(200, GeneratorLeaseRefreshReason::Maintenance),
            200
        );
        assert_eq!(
            plan.record_issued_upper_bound(None, GeneratorLeaseRefreshReason::Maintenance),
            Some(issued_upper_bound)
        );
    }

    #[test]
    fn generator_lease_refresh_plan_horizon_reason_does_not_renew_fresh_lease() {
        let issued_upper_bound = encode_tso(260, 9, 0).expect("upper bound should encode");
        let next_upper_bound = encode_tso(360, 9, 0).expect("upper bound should encode");
        let plan = GeneratorLeaseRefreshPlan::build(
            300,
            None,
            Some(issued_upper_bound),
            None,
            100,
            200,
            100,
        );

        assert!(plan.needs_write(GeneratorLeaseRefreshReason::HorizonRequired));
        assert_eq!(
            plan.record_lease_expire_at_ms(400, GeneratorLeaseRefreshReason::HorizonRequired),
            300
        );
        assert_eq!(
            plan.record_issued_upper_bound(
                Some(next_upper_bound),
                GeneratorLeaseRefreshReason::HorizonRequired,
            ),
            Some(next_upper_bound)
        );
    }

    #[test]
    fn generator_lease_refresh_plan_explicit_renewal_advances_lease_and_horizon() {
        let issued_upper_bound = encode_tso(260, 9, 0).expect("upper bound should encode");
        let next_upper_bound = encode_tso(360, 9, 0).expect("upper bound should encode");
        let plan = GeneratorLeaseRefreshPlan::build(
            300,
            None,
            Some(issued_upper_bound),
            None,
            100,
            200,
            100,
        );

        assert!(plan.needs_write(GeneratorLeaseRefreshReason::ExplicitRenewal));
        assert_eq!(
            plan.record_lease_expire_at_ms(400, GeneratorLeaseRefreshReason::ExplicitRenewal),
            400
        );
        assert_eq!(
            plan.record_issued_upper_bound(
                Some(next_upper_bound),
                GeneratorLeaseRefreshReason::ExplicitRenewal,
            ),
            Some(next_upper_bound)
        );
    }
}
