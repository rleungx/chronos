use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::time::Duration;

use tokio::time::{sleep, Instant};

use crate::{TsoConfig, TsoError};

pub(super) struct MetadataContentionCoordinator {
    jitter_state: AtomicU64,
}

impl MetadataContentionCoordinator {
    pub(super) fn new(jitter_seed: u64) -> Self {
        Self {
            jitter_state: AtomicU64::new(jitter_seed),
        }
    }

    pub(super) fn retry_budget(&self, config: &TsoConfig) -> Duration {
        Duration::from_millis(
            config
                .grpc_request_timeout_ms
                .or(config.etcd_timeout_ms)
                .unwrap_or(1_000),
        )
    }

    pub(super) async fn backoff_after_contention(
        &self,
        retries: u32,
        deadline: Instant,
    ) -> Result<(), TsoError> {
        if Instant::now() >= deadline {
            return Err(TsoError::CasFailed);
        }

        if retries <= 2 {
            tokio::task::yield_now().await;
        } else {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(TsoError::CasFailed);
            }

            let delay = self
                .metadata_contention_backoff_delay(retries)
                .min(remaining);
            if !delay.is_zero() {
                sleep(delay).await;
            }
        }

        if Instant::now() >= deadline {
            Err(TsoError::CasFailed)
        } else {
            Ok(())
        }
    }

    fn metadata_contention_backoff_delay(&self, retries: u32) -> Duration {
        if retries <= 2 {
            return Duration::ZERO;
        }

        let base_ms = 1u64 << retries.saturating_sub(3).min(4);
        Duration::from_millis(self.metadata_contention_jitter_ms(base_ms))
    }

    fn metadata_contention_jitter_ms(&self, max_ms: u64) -> u64 {
        if max_ms == 0 {
            return 0;
        }

        let mut state = self.jitter_state.load(AtomicOrdering::Acquire);
        loop {
            let next = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            match self.jitter_state.compare_exchange(
                state,
                next,
                AtomicOrdering::AcqRel,
                AtomicOrdering::Acquire,
            ) {
                Ok(_) => return next % (max_ms + 1),
                Err(observed) => state = observed,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::time::{Duration, Instant};

    use crate::{TsoConfig, TsoError};

    use super::MetadataContentionCoordinator;

    #[test]
    fn metadata_contention_retry_budget_prefers_request_timeout() {
        let coordinator = MetadataContentionCoordinator::new(7);
        let config = TsoConfig {
            grpc_request_timeout_ms: Some(123),
            etcd_timeout_ms: Some(456),
            ..TsoConfig::default()
        };

        assert_eq!(
            coordinator.retry_budget(&config),
            Duration::from_millis(123)
        );
    }

    #[tokio::test]
    async fn metadata_contention_backoff_respects_expired_deadline() {
        let coordinator = MetadataContentionCoordinator::new(7);
        let result = coordinator
            .backoff_after_contention(3, Instant::now() - Duration::from_millis(1))
            .await;

        assert!(matches!(result, Err(TsoError::CasFailed)));
    }
}
