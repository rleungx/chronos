use crate::TsoError;

pub(super) const MAX_TIMELINE_KEY_BYTES: usize = 256;
pub(super) const MAX_CLIENT_REQUEST_ID_BYTES: usize = 128;

fn contains_control_character(value: &str) -> bool {
    value.chars().any(char::is_control)
}

pub(super) fn validate_timeline_key(timeline_key: &str) -> Result<(), TsoError> {
    if timeline_key.trim().is_empty() {
        return Err(TsoError::InvalidTimelineKey {
            reason: "must not be blank".into(),
        });
    }
    if timeline_key.len() > MAX_TIMELINE_KEY_BYTES {
        return Err(TsoError::InvalidTimelineKey {
            reason: format!("must not exceed {MAX_TIMELINE_KEY_BYTES} UTF-8 bytes"),
        });
    }
    if contains_control_character(timeline_key) {
        return Err(TsoError::InvalidTimelineKey {
            reason: "must not contain control characters".into(),
        });
    }
    Ok(())
}

pub(super) fn validate_client_request_id(client_request_id: &str) -> Result<(), TsoError> {
    if client_request_id.is_empty() {
        return Ok(());
    }
    if client_request_id.trim().is_empty() {
        return Err(TsoError::InvalidClientRequestId {
            reason: "must be empty or contain a non-whitespace character".into(),
        });
    }
    if client_request_id.len() > MAX_CLIENT_REQUEST_ID_BYTES {
        return Err(TsoError::InvalidClientRequestId {
            reason: format!("must not exceed {MAX_CLIENT_REQUEST_ID_BYTES} UTF-8 bytes"),
        });
    }
    if contains_control_character(client_request_id) {
        return Err(TsoError::InvalidClientRequestId {
            reason: "must not contain control characters".into(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timeline_keys_allow_path_separators_because_metadata_keys_are_encoded() {
        validate_timeline_key("tenant/a/timeline/b").unwrap();
    }

    #[test]
    fn external_identifiers_are_bounded_and_log_safe() {
        assert!(matches!(
            validate_timeline_key("\ninvalid"),
            Err(TsoError::InvalidTimelineKey { .. })
        ));
        assert!(matches!(
            validate_client_request_id(&"x".repeat(MAX_CLIENT_REQUEST_ID_BYTES + 1)),
            Err(TsoError::InvalidClientRequestId { .. })
        ));
    }
}
