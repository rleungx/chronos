use std::fs;
use std::io::Cursor;

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::RootCertStore;

use crate::TsoError;

pub fn read_required_pem_file(path: &str, env_key: &str) -> Result<Vec<u8>, TsoError> {
    fs::read(path)
        .map_err(|error| TsoError::Internal(format!("{env_key} could not read {path}: {error}")))
}

pub fn parse_pem_certificates(
    pem: &[u8],
    env_key: &str,
) -> Result<Vec<CertificateDer<'static>>, TsoError> {
    let mut reader = Cursor::new(pem);
    let certs = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| {
            TsoError::Internal(format!("{env_key} could not parse certificates: {error}"))
        })?;
    if certs.is_empty() {
        return Err(TsoError::Internal(format!(
            "{env_key} did not contain any certificates"
        )));
    }
    Ok(certs)
}

pub fn parse_pem_private_key(
    pem: &[u8],
    env_key: &str,
) -> Result<PrivateKeyDer<'static>, TsoError> {
    let mut reader = Cursor::new(pem);
    rustls_pemfile::private_key(&mut reader)
        .map_err(|error| TsoError::Internal(format!("{env_key} could not parse key: {error}")))?
        .ok_or_else(|| TsoError::Internal(format!("{env_key} did not contain a private key")))
}

pub fn load_root_cert_store(pem: &[u8], env_key: &str) -> Result<RootCertStore, TsoError> {
    let mut roots = RootCertStore::empty();
    for cert in parse_pem_certificates(pem, env_key)? {
        roots.add(cert).map_err(|error| {
            TsoError::Internal(format!("{env_key} invalid certificate: {error}"))
        })?;
    }
    Ok(roots)
}
