use crate::runtime::TimelineState;
use crate::{ResourceTier, TimelineRoute, TsoError};

use super::TsoService;

impl TsoService {
    pub(in crate::service) fn validate_batch_for_route(
        &self,
        count: u32,
        route: &TimelineRoute,
    ) -> Result<(), TsoError> {
        let max = match route.resource_tier {
            ResourceTier::Shared => self
                .config
                .max_batch_per_request
                .min(crate::SEQUENCE_CAPACITY),
            ResourceTier::Warm | ResourceTier::Dedicated => self.config.max_batch_per_request,
        };

        if count > max {
            return Err(TsoError::BatchTooLarge {
                requested: count,
                max,
            });
        }
        Ok(())
    }

    fn effective_future_borrow_ms_for_route(&self, route: &TimelineRoute) -> u64 {
        match route.resource_tier {
            ResourceTier::Shared | ResourceTier::Warm | ResourceTier::Dedicated => {
                self.config.max_future_borrow_ms
            }
        }
    }

    pub(in crate::service) fn effective_future_borrow_ms_for_timeline_state(
        &self,
        timeline_state: &TimelineState,
        now_ms: u64,
    ) -> u64 {
        let base = self.effective_future_borrow_ms_for_route(&timeline_state.route);
        let Some(recovery_floor_tso) = timeline_state.recovery_floor_tso else {
            return base;
        };
        let recovery_physical_ms = crate::decode_tso(recovery_floor_tso).physical_ms;
        if recovery_physical_ms <= now_ms {
            return base;
        }

        let catchup_gap_ms = recovery_physical_ms - now_ms;
        base.max(catchup_gap_ms.min(self.config.recovery_catchup_budget_ms))
    }

    fn timeline_quota_capacity_for_route(&self, route: &TimelineRoute) -> Option<f64> {
        match route.resource_tier {
            ResourceTier::Shared => Some(
                self.config
                    .max_batch_per_request
                    .min(crate::SEQUENCE_CAPACITY) as f64,
            ),
            ResourceTier::Warm => Some(self.config.max_batch_per_request as f64),
            ResourceTier::Dedicated => None,
        }
    }

    fn timeline_quota_wait_ms_for_route(&self, route: &TimelineRoute, deficit: f64) -> u64 {
        let window_ms = self.effective_future_borrow_ms_for_route(route).max(1) as f64;
        let capacity = self
            .timeline_quota_capacity_for_route(route)
            .unwrap_or(0.0)
            .max(1.0);
        let refill_per_ms = capacity / window_ms;
        (deficit / refill_per_ms).ceil().max(1.0) as u64
    }

    pub(in crate::service) fn charge_timeline_quota(
        &self,
        timeline_state: &mut TimelineState,
        count: u32,
        now_ms: u64,
    ) -> Result<Option<f64>, TsoError> {
        let Some(capacity) = self.timeline_quota_capacity_for_route(&timeline_state.route) else {
            return Ok(None);
        };

        let window_ms = self
            .effective_future_borrow_ms_for_timeline_state(timeline_state, now_ms)
            .max(1) as f64;
        let refill_per_ms = capacity / window_ms;
        let elapsed_ms = timeline_state
            .timeline_quota_last_refill_ms
            .map(|last| now_ms.saturating_sub(last) as f64)
            .unwrap_or(0.0);
        let available = timeline_state
            .timeline_quota_tokens
            .unwrap_or(capacity)
            .min(capacity)
            + elapsed_ms * refill_per_ms;
        let available = available.min(capacity);
        let requested = count as f64;

        if requested > available {
            let wait_ms =
                self.timeline_quota_wait_ms_for_route(&timeline_state.route, requested - available);
            return Err(TsoError::FutureBorrowExceeded {
                requested_physical_ms: now_ms + wait_ms,
                allowed_physical_ms: now_ms,
            });
        }

        timeline_state.timeline_quota_tokens = Some((available - requested).max(0.0));
        timeline_state.timeline_quota_last_refill_ms = Some(now_ms);
        Ok(Some(requested))
    }

    pub(in crate::service) fn refund_timeline_quota(
        &self,
        timeline_state: &mut TimelineState,
        charged: Option<f64>,
    ) {
        let Some(charged) = charged else {
            return;
        };
        let Some(capacity) = self.timeline_quota_capacity_for_route(&timeline_state.route) else {
            return;
        };
        let available = timeline_state.timeline_quota_tokens.unwrap_or(capacity);
        timeline_state.timeline_quota_tokens = Some((available + charged).min(capacity));
    }
}
