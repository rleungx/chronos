use std::collections::HashSet;
use std::sync::Arc;

use sha2::{Digest, Sha256};
use tonic::{Request, Status};

const SHA256_FINGERPRINT_LEN: usize = 32;

#[derive(Debug, Clone)]
pub(crate) struct PeerCertAuthorizer {
    service_name: &'static str,
    allowed_leaf_fingerprints: Arc<HashSet<[u8; SHA256_FINGERPRINT_LEN]>>,
}

impl PeerCertAuthorizer {
    pub(crate) fn disabled(service_name: &'static str) -> Self {
        Self {
            service_name,
            allowed_leaf_fingerprints: Arc::new(HashSet::new()),
        }
    }

    pub(crate) fn from_allowlist(
        service_name: &'static str,
        allowlist: &[String],
    ) -> Result<Self, String> {
        validate_peer_cert_allowlist_entries(allowlist)?;
        let allowed_leaf_fingerprints = allowlist
            .iter()
            .map(|value| normalize_sha256_fingerprint(value))
            .collect::<Result<HashSet<_>, _>>()?;
        Ok(Self {
            service_name,
            allowed_leaf_fingerprints: Arc::new(allowed_leaf_fingerprints),
        })
    }

    pub(crate) fn is_enabled(&self) -> bool {
        !self.allowed_leaf_fingerprints.is_empty()
    }

    pub(crate) fn authorize<T>(&self, request: &Request<T>) -> Result<(), Status> {
        if !self.is_enabled() {
            return Ok(());
        }

        let Some(peer_certs) = request.peer_certs() else {
            return Err(Status::unauthenticated(format!(
                "mTLS peer certificate required for {}",
                self.service_name
            )));
        };

        let Some(leaf_cert) = peer_certs.first() else {
            return Err(Status::unauthenticated(format!(
                "mTLS peer certificate required for {}",
                self.service_name
            )));
        };

        let fingerprint = sha256_fingerprint(leaf_cert.as_ref());
        if self.allowed_leaf_fingerprints.contains(&fingerprint) {
            return Ok(());
        }

        Err(Status::permission_denied(format!(
            "mTLS peer certificate is not authorized for {}",
            self.service_name
        )))
    }
}

pub(crate) fn validate_peer_cert_allowlist_entries(allowlist: &[String]) -> Result<(), String> {
    for value in allowlist {
        normalize_sha256_fingerprint(value)?;
    }
    Ok(())
}

fn sha256_fingerprint(bytes: &[u8]) -> [u8; SHA256_FINGERPRINT_LEN] {
    Sha256::digest(bytes).into()
}

fn normalize_sha256_fingerprint(value: &str) -> Result<[u8; SHA256_FINGERPRINT_LEN], String> {
    let normalized = value
        .trim()
        .chars()
        .filter(|ch| !matches!(ch, ':' | '-' | ' ' | '\t' | '\n' | '\r'))
        .collect::<String>();

    if normalized.len() != SHA256_FINGERPRINT_LEN * 2 {
        return Err(format!(
            "expected 64 hexadecimal characters, got {}",
            normalized.len()
        ));
    }

    let mut bytes = [0u8; SHA256_FINGERPRINT_LEN];
    for (index, chunk) in normalized.as_bytes().chunks_exact(2).enumerate() {
        let high = decode_hex_nibble(chunk[0])?;
        let low = decode_hex_nibble(chunk[1])?;
        bytes[index] = (high << 4) | low;
    }

    Ok(bytes)
}

fn decode_hex_nibble(byte: u8) -> Result<u8, String> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err("expected hexadecimal SHA-256 fingerprint".into()),
    }
}

#[cfg(test)]
fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_sha256_fingerprint_accepts_mixed_case_and_colons() {
        let normalized = normalize_sha256_fingerprint(
            "AA:bb:CC:dd:EE:ff:00:11:22:33:44:55:66:77:88:99:aa:bb:cc:dd:ee:ff:00:11:22:33:44:55:66:77:88:99",
        )
        .unwrap();

        assert_eq!(
            encode_hex(&normalized),
            "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"
        );
    }

    #[test]
    fn normalize_sha256_fingerprint_rejects_non_hex_input() {
        let error = normalize_sha256_fingerprint(
            "zzbbccddeeff00112233445566778899aabbccddeeff00112233445566778899",
        )
        .unwrap_err();

        assert!(error.contains("hexadecimal"));
    }
}
