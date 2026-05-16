use etcd_client::{Certificate, ConnectOptions, Identity, TlsOptions};
use tokio::time::Duration;

use crate::tls::read_required_pem_file;
use crate::{TsoConfig, TsoError};

fn load_etcd_client_tls_options(config: &TsoConfig) -> Result<Option<TlsOptions>, TsoError> {
    let Some(paths) = config
        .etcd_tls_paths()
        .map_err(|error| TsoError::Internal(error.to_string()))?
    else {
        return Ok(None);
    };

    let ca = read_required_pem_file(paths.ca_file, "CHRONOS_ETCD_CA_FILE")?;
    let cert = read_required_pem_file(paths.cert_file, "CHRONOS_ETCD_CERT_FILE")?;
    let key = read_required_pem_file(paths.key_file, "CHRONOS_ETCD_KEY_FILE")?;

    Ok(Some(
        TlsOptions::new()
            .ca_certificate(Certificate::from_pem(ca))
            .identity(Identity::from_pem(cert, key)),
    ))
}

pub(in crate::metadata::etcd) fn build_etcd_connect_options(
    config: &TsoConfig,
    endpoints: &[String],
) -> Result<Option<ConnectOptions>, TsoError> {
    let mut endpoint_contract = config.clone();
    endpoint_contract.etcd_endpoints = endpoints.to_vec();
    endpoint_contract
        .validate_etcd_endpoint_contract()
        .map_err(|error| TsoError::Internal(error.to_string()))?;

    let mut options = ConnectOptions::new();
    let mut configured = false;

    if let Some(timeout_ms) = config.etcd_timeout_ms {
        let timeout = Duration::from_millis(timeout_ms);
        options = options.with_timeout(timeout).with_connect_timeout(timeout);
        configured = true;
    }

    if let Some(tls) = load_etcd_client_tls_options(config)? {
        options = options.with_tls(tls);
        configured = true;
    }

    Ok(configured.then_some(options))
}
