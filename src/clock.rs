use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LazyLock, MutexGuard};
use std::time::{Instant, SystemTime};

use crate::recovery::{duration_since_unix_epoch_or_zero, record_recovery_event};
use crate::{checked_physical_ms_from_unix_ms, TsoUnixMsBoundary, MAX_PHYSICAL_MS};

pub trait Clock: Send + Sync {
    fn now_ms(&self) -> u64;
}

#[derive(Debug, Default)]
pub struct SystemClock;

#[derive(Debug)]
struct SystemClockBase {
    base_relative_ms: u64,
    base_instant: Instant,
}

static SYSTEM_CLOCK_LAST_NOW_MS: AtomicU64 = AtomicU64::new(0);
static SYSTEM_CLOCK_BASE: LazyLock<SystemClockBase> = LazyLock::new(|| SystemClockBase {
    base_relative_ms: relative_ms_from_unix_ms(
        duration_since_unix_epoch_or_zero(SystemTime::now(), "clock", "system_clock_now_ms")
            .as_millis() as u64,
    ),
    base_instant: Instant::now(),
});

fn relative_ms_from_unix_ms(unix_ms: u64) -> u64 {
    match checked_physical_ms_from_unix_ms(unix_ms) {
        Ok(relative_ms) => relative_ms,
        Err(TsoUnixMsBoundary::BeforeCustomEpoch) => {
            record_recovery_event("clock", "system_clock_now_ms", "before_custom_epoch");
            0
        }
        Err(TsoUnixMsBoundary::BeyondCapacityEnvelope) => {
            record_recovery_event(
                "clock",
                "system_clock_now_ms",
                "tso_capacity_horizon_exceeded",
            );
            MAX_PHYSICAL_MS + 1
        }
    }
}

fn clamp_monotonic_ms(previous_ms: u64, observed_ms: u64) -> u64 {
    previous_ms.max(observed_ms)
}

fn monotonic_relative_ms(base_relative_ms: u64, elapsed_ms: u64) -> u64 {
    base_relative_ms
        .saturating_add(elapsed_ms)
        .min(MAX_PHYSICAL_MS + 1)
}

impl Clock for SystemClock {
    fn now_ms(&self) -> u64 {
        let base = &*SYSTEM_CLOCK_BASE;
        let unix_ms =
            duration_since_unix_epoch_or_zero(SystemTime::now(), "clock", "system_clock_now_ms")
                .as_millis() as u64;
        let observed_ms = relative_ms_from_unix_ms(unix_ms);
        let monotonic_ms = monotonic_relative_ms(
            base.base_relative_ms,
            base.base_instant.elapsed().as_millis() as u64,
        );
        let candidate_ms = observed_ms.max(monotonic_ms);
        let previous_ms = SYSTEM_CLOCK_LAST_NOW_MS.fetch_max(candidate_ms, Ordering::AcqRel);
        clamp_monotonic_ms(previous_ms, candidate_ms)
    }
}

#[derive(Debug)]
pub struct ManualClock {
    now_ms: std::sync::Mutex<u64>,
}

impl ManualClock {
    pub fn new(now_ms: u64) -> Self {
        Self {
            now_ms: std::sync::Mutex::new(now_ms),
        }
    }

    fn lock_now_ms(&self) -> MutexGuard<'_, u64> {
        match self.now_ms.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                record_recovery_event("clock", "manual_clock_lock", "mutex_poisoned");
                poisoned.into_inner()
            }
        }
    }

    pub fn set(&self, now_ms: u64) {
        *self.lock_now_ms() = now_ms;
    }

    pub fn advance(&self, delta_ms: u64) {
        let mut guard = self.lock_now_ms();
        *guard += delta_ms;
    }
}

impl Clock for ManualClock {
    fn now_ms(&self) -> u64 {
        *self.lock_now_ms()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics;
    use std::panic::{self, AssertUnwindSafe};
    use std::time::Duration;

    #[test]
    fn duration_before_unix_epoch_records_recovery_metric() {
        let before = metrics::TSO_RECOVERY_EVENTS_TOTAL
            .with_label_values(&[
                "clock",
                "system_clock_now_ms",
                "system_time_before_unix_epoch",
            ])
            .get();

        let duration = duration_since_unix_epoch_or_zero(
            SystemTime::UNIX_EPOCH - Duration::from_secs(1),
            "clock",
            "system_clock_now_ms",
        );

        assert_eq!(duration, Duration::default());
        assert!(
            metrics::TSO_RECOVERY_EVENTS_TOTAL
                .with_label_values(&[
                    "clock",
                    "system_clock_now_ms",
                    "system_time_before_unix_epoch"
                ])
                .get()
                > before
        );
    }

    #[test]
    fn time_before_custom_epoch_records_recovery_metric_and_clamps_to_zero() {
        let before = metrics::TSO_RECOVERY_EVENTS_TOTAL
            .with_label_values(&["clock", "system_clock_now_ms", "before_custom_epoch"])
            .get();

        assert_eq!(relative_ms_from_unix_ms(crate::CUSTOM_EPOCH_UNIX_MS - 1), 0);
        assert!(
            metrics::TSO_RECOVERY_EVENTS_TOTAL
                .with_label_values(&["clock", "system_clock_now_ms", "before_custom_epoch"])
                .get()
                > before
        );
    }

    #[test]
    fn time_past_tso_capacity_horizon_records_recovery_metric_and_fails_closed() {
        let before = metrics::TSO_RECOVERY_EVENTS_TOTAL
            .with_label_values(&[
                "clock",
                "system_clock_now_ms",
                "tso_capacity_horizon_exceeded",
            ])
            .get();

        let relative_ms =
            relative_ms_from_unix_ms(crate::TSO_CAPACITY_ENVELOPE.first_unencodable_unix_ms());

        assert_eq!(
            relative_ms,
            crate::TSO_CAPACITY_ENVELOPE.first_unencodable_physical_ms()
        );
        assert!(
            metrics::TSO_RECOVERY_EVENTS_TOTAL
                .with_label_values(&[
                    "clock",
                    "system_clock_now_ms",
                    "tso_capacity_horizon_exceeded"
                ])
                .get()
                > before
        );
    }

    #[test]
    fn clamp_monotonic_ms_never_moves_backwards() {
        assert_eq!(clamp_monotonic_ms(100, 80), 100);
        assert_eq!(clamp_monotonic_ms(100, 100), 100);
        assert_eq!(clamp_monotonic_ms(100, 120), 120);
    }

    #[test]
    fn monotonic_relative_ms_advances_during_wall_clock_rewind_window() {
        assert_eq!(monotonic_relative_ms(100, 0), 100);
        assert_eq!(monotonic_relative_ms(100, 25), 125);
    }

    #[test]
    fn monotonic_relative_ms_stays_failed_closed_at_capacity_horizon() {
        assert_eq!(
            monotonic_relative_ms(MAX_PHYSICAL_MS, 5),
            MAX_PHYSICAL_MS + 1
        );
        assert_eq!(
            monotonic_relative_ms(MAX_PHYSICAL_MS + 1, 50),
            MAX_PHYSICAL_MS + 1
        );
    }

    #[test]
    fn manual_clock_poison_recovers_and_records_metric() {
        let clock = ManualClock::new(42);
        let before = metrics::TSO_RECOVERY_EVENTS_TOTAL
            .with_label_values(&["clock", "manual_clock_lock", "mutex_poisoned"])
            .get();

        let _ = panic::catch_unwind(AssertUnwindSafe(|| {
            let _guard = clock.now_ms.lock().unwrap();
            panic!("poison manual clock");
        }));

        assert_eq!(clock.now_ms(), 42);
        assert!(
            metrics::TSO_RECOVERY_EVENTS_TOTAL
                .with_label_values(&["clock", "manual_clock_lock", "mutex_poisoned"])
                .get()
                > before
        );
    }
}
