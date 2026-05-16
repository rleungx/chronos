use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

use tokio::time::{sleep, Duration, Instant};
use tracing::warn;

use crate::{metrics, TsoConfig};

pub(in crate::metadata::etcd) const DEFAULT_ETCD_REQUEST_RETRY_BUDGET_MS: u64 = 1_000;
const ETCD_REQUEST_MAX_ATTEMPTS: u32 = 4;
const ETCD_REQUEST_RETRY_MIN_BACKOFF_MS: u64 = 25;
const ETCD_REQUEST_RETRY_MAX_BACKOFF_MS: u64 = 250;
const ETCD_REQUEST_RETRY_JITTER_MAX_MS: u64 = 25;
pub(in crate::metadata::etcd) const IDENTITY_KEEPALIVE_RECONNECT_MIN_BACKOFF_MS: u64 = 100;
pub(in crate::metadata::etcd) const IDENTITY_KEEPALIVE_RECONNECT_MAX_BACKOFF_MS: u64 = 500;
pub(in crate::metadata::etcd) const ROUTE_WATCH_MIN_RECONNECT_BACKOFF_MS: u64 = 100;
pub(in crate::metadata::etcd) const ROUTE_WATCH_MAX_RECONNECT_BACKOFF_MS: u64 = 5_000;

static ETCD_REQUEST_RETRY_JITTER_STATE: AtomicU64 = AtomicU64::new(0x9e37_79b9_7f4a_7c15);

pub(in crate::metadata::etcd) fn route_watch_reconnect_backoff(
    consecutive_failures: u32,
) -> Duration {
    let shift = consecutive_failures.saturating_sub(1).min(6);
    let base_ms = ROUTE_WATCH_MIN_RECONNECT_BACKOFF_MS
        .saturating_mul(1u64 << shift)
        .min(ROUTE_WATCH_MAX_RECONNECT_BACKOFF_MS);
    let jitter_ms =
        ((std::process::id() as u64).wrapping_add(consecutive_failures as u64 * 97)) % 100;
    Duration::from_millis((base_ms + jitter_ms).min(ROUTE_WATCH_MAX_RECONNECT_BACKOFF_MS))
}

pub(in crate::metadata::etcd) fn identity_keepalive_reconnect_backoff(
    consecutive_failures: u32,
) -> Duration {
    let shift = consecutive_failures.saturating_sub(1).min(3);
    let base_ms = IDENTITY_KEEPALIVE_RECONNECT_MIN_BACKOFF_MS
        .saturating_mul(1u64 << shift)
        .min(IDENTITY_KEEPALIVE_RECONNECT_MAX_BACKOFF_MS);
    let jitter_ms =
        ((std::process::id() as u64).wrapping_add(consecutive_failures as u64 * 53)) % 50;
    Duration::from_millis((base_ms + jitter_ms).min(IDENTITY_KEEPALIVE_RECONNECT_MAX_BACKOFF_MS))
}

pub(in crate::metadata::etcd) fn etcd_request_retry_budget(config: &TsoConfig) -> Duration {
    let request_budget_ms = DEFAULT_ETCD_REQUEST_RETRY_BUDGET_MS.max(
        config
            .grpc_request_timeout_ms
            .unwrap_or(DEFAULT_ETCD_REQUEST_RETRY_BUDGET_MS),
    );
    let configured_budget_ms = config
        .etcd_timeout_ms
        .map_or(request_budget_ms, |etcd_timeout_ms| {
            request_budget_ms.max(etcd_timeout_ms)
        });
    Duration::from_millis(configured_budget_ms.max(1))
}

fn next_etcd_request_retry_jitter_ms(max_ms: u64) -> u64 {
    if max_ms == 0 {
        return 0;
    }

    let mut state = ETCD_REQUEST_RETRY_JITTER_STATE.load(AtomicOrdering::Acquire);
    loop {
        let next = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1)
            .wrapping_add(std::process::id() as u64);
        match ETCD_REQUEST_RETRY_JITTER_STATE.compare_exchange(
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

fn etcd_request_retry_backoff(attempt: u32) -> Duration {
    let shift = attempt.saturating_sub(1).min(4);
    let base_ms = ETCD_REQUEST_RETRY_MIN_BACKOFF_MS
        .saturating_mul(1u64 << shift)
        .min(ETCD_REQUEST_RETRY_MAX_BACKOFF_MS);
    let jitter_ms = next_etcd_request_retry_jitter_ms(ETCD_REQUEST_RETRY_JITTER_MAX_MS);
    Duration::from_millis((base_ms + jitter_ms).min(ETCD_REQUEST_RETRY_MAX_BACKOFF_MS))
}

fn etcd_retry_reason(error: &etcd_client::Error) -> &'static str {
    match error {
        etcd_client::Error::GRpcStatus(status) => match status.code() {
            tonic::Code::Unknown => "grpc_unknown",
            tonic::Code::DeadlineExceeded => "grpc_deadline_exceeded",
            tonic::Code::ResourceExhausted => "grpc_resource_exhausted",
            tonic::Code::Aborted => "grpc_aborted",
            tonic::Code::Internal => "grpc_internal",
            tonic::Code::Unavailable => "grpc_unavailable",
            _ => "grpc_non_retryable",
        },
        etcd_client::Error::IoError(_) => "io",
        etcd_client::Error::TransportError(_) => "transport",
        etcd_client::Error::WatchError(_) => "watch",
        etcd_client::Error::LeaseKeepAliveError(_) => "lease_keepalive",
        etcd_client::Error::EndpointError(_) => "endpoint",
        etcd_client::Error::InvalidArgs(_)
        | etcd_client::Error::InvalidUri(_)
        | etcd_client::Error::Utf8Error(_)
        | etcd_client::Error::InvalidHeaderValue(_)
        | etcd_client::Error::ElectError(_)
        | etcd_client::Error::EndpointsNotManaged => "non_retryable",
    }
}

fn etcd_error_is_retryable(error: &etcd_client::Error) -> bool {
    match error {
        etcd_client::Error::GRpcStatus(status) => matches!(
            status.code(),
            tonic::Code::Unknown
                | tonic::Code::DeadlineExceeded
                | tonic::Code::ResourceExhausted
                | tonic::Code::Aborted
                | tonic::Code::Internal
                | tonic::Code::Unavailable
        ),
        etcd_client::Error::IoError(_)
        | etcd_client::Error::TransportError(_)
        | etcd_client::Error::WatchError(_)
        | etcd_client::Error::LeaseKeepAliveError(_)
        | etcd_client::Error::EndpointError(_) => true,
        etcd_client::Error::InvalidArgs(_)
        | etcd_client::Error::InvalidUri(_)
        | etcd_client::Error::Utf8Error(_)
        | etcd_client::Error::InvalidHeaderValue(_)
        | etcd_client::Error::ElectError(_)
        | etcd_client::Error::EndpointsNotManaged => false,
    }
}

pub(in crate::metadata::etcd) async fn retry_etcd_request<T, F, Fut>(
    op_label: &'static str,
    retry_budget: Duration,
    mut operation: F,
) -> Result<T, etcd_client::Error>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, etcd_client::Error>>,
{
    let mut attempt = 1u32;
    let deadline = Instant::now() + retry_budget.max(Duration::from_millis(1));
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(etcd_client::Error::GRpcStatus(
                tonic::Status::deadline_exceeded(format!(
                    "etcd request {op_label} deadline/timeout exceeded retry budget"
                )),
            ));
        }

        let attempt_result = match tokio::time::timeout(remaining, operation()).await {
            Ok(result) => result,
            Err(_) => {
                return Err(etcd_client::Error::GRpcStatus(
                    tonic::Status::deadline_exceeded(format!(
                        "etcd request {op_label} deadline/timeout exceeded retry budget"
                    )),
                ));
            }
        };

        match attempt_result {
            Ok(response) => return Ok(response),
            Err(error)
                if attempt < ETCD_REQUEST_MAX_ATTEMPTS && etcd_error_is_retryable(&error) =>
            {
                let reason = etcd_retry_reason(&error);
                let backoff = etcd_request_retry_backoff(attempt);
                metrics::TSO_METADATA_RETRIES_TOTAL
                    .with_label_values(&[op_label, reason])
                    .inc();
                warn!(
                    component = "metadata",
                    event = "etcd_request_retry",
                    result = "degraded",
                    reason = %error,
                    retry_reason = reason,
                    op_label,
                    attempt,
                    backoff_ms = backoff.as_millis() as u64
                );
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return Err(error);
                }
                sleep(backoff.min(remaining)).await;
                attempt = attempt.saturating_add(1);
            }
            Err(error) => return Err(error),
        }
    }
}
