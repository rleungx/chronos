pub(crate) mod bootstrap;
pub(crate) mod config;
pub(crate) mod preflight;
pub(crate) mod readiness_reason;
pub(crate) mod runtime;
pub(crate) mod serving_gate;
#[cfg(test)]
pub(crate) mod test_support;
#[cfg(test)]
mod tests;
pub(crate) mod transport;

#[cfg(test)]
pub(crate) use bootstrap::build_tso_service;
#[cfg(test)]
pub(crate) use bootstrap::run_metadata_startup_probe;
#[cfg(test)]
pub(crate) use config::{load_startup_config, load_tso_config};
#[cfg(test)]
pub(crate) use preflight::{
    startup_failure_stage, validate_build_identity_for_profile, validate_startup_preflight,
};
#[cfg(test)]
pub(crate) use readiness_reason::{
    startup_bootstrap_failure_reason, startup_preflight_failure_reason, ShutdownTrigger,
};
pub(crate) use runtime::run;
#[cfg(test)]
pub(crate) use runtime::{
    record_shutdown, record_startup_bootstrap_failure, record_startup_preflight_failure,
    record_worker_readiness_transition, request_shutdown, set_startup_ready,
    spawn_identity_lease_loss_monitor, supervise_critical_servers, StartupWorkerReadinessSink,
};
#[cfg(test)]
pub(crate) use transport::{build_grpc_server, wait_for_shutdown_signal};
#[cfg(test)]
pub(crate) use transport::{
    load_metrics_tls_acceptor, load_root_cert_store, metrics_handler, parse_pem_certificates,
    parse_pem_private_key, serve_metrics_listener,
};
