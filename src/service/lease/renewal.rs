use std::cmp::max;

use crate::decode_tso;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::service) struct GeneratorLeaseRefreshPlan {
    candidate_last: Option<u64>,
    previous_issued_upper_bound: Option<u64>,
    next_upper_bound_base_ms: u64,
    should_renew_lease: bool,
    should_update_last_persisted_tso: bool,
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
        let should_update_last_persisted_tso = match (last_persisted_tso, candidate_last) {
            (Some(previous), Some(next)) => next > previous,
            (None, Some(_)) => true,
            _ => false,
        };
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
            previous_issued_upper_bound: issued_upper_bound,
            next_upper_bound_base_ms,
            should_renew_lease,
            should_update_last_persisted_tso,
            should_extend_upper_bound,
        }
    }

    pub(in crate::service) fn candidate_last(&self) -> Option<u64> {
        self.candidate_last
    }

    pub(in crate::service) fn needs_write(&self) -> bool {
        self.should_renew_lease
            || self.should_update_last_persisted_tso
            || self.should_extend_upper_bound
    }

    pub(in crate::service) fn next_upper_bound_base_ms(&self) -> u64 {
        self.next_upper_bound_base_ms
    }

    pub(in crate::service) fn record_issued_upper_bound(
        &self,
        next_upper_bound: Option<u64>,
    ) -> Option<u64> {
        merge_max(self.previous_issued_upper_bound, next_upper_bound)
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

    use super::GeneratorLeaseRefreshPlan;

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
        assert!(!plan.needs_write());
        assert_eq!(plan.next_upper_bound_base_ms(), 260);
        assert_eq!(
            plan.record_issued_upper_bound(None),
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
        assert!(plan.needs_write());
        assert_eq!(plan.next_upper_bound_base_ms(), 120);
        assert_eq!(
            plan.record_issued_upper_bound(Some(next_upper_bound)),
            Some(next_upper_bound)
        );
    }
}
