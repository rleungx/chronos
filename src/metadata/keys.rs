fn namespace(prefix: &str, suffix: &str) -> String {
    if prefix.is_empty() {
        suffix.to_string()
    } else {
        format!("{prefix}/{suffix}")
    }
}

fn encode_component(value: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(value.len() * 2);
    for byte in value.as_bytes() {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn normalize_endpoint_component(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

pub(super) fn timeline_key(prefix: &str, timeline_key: &str) -> String {
    namespace(prefix, &format!("routes/{timeline_key}"))
}

pub(super) fn generator_key(prefix: &str, generator_id: u32) -> String {
    namespace(prefix, &format!("generators/{generator_id}"))
}

pub(super) fn generator_prefix(prefix: &str) -> String {
    namespace(prefix, "generators/")
}

pub(super) fn route_prefix(prefix: &str) -> String {
    namespace(prefix, "routes/")
}

pub(super) fn timeline_status_index_prefix(prefix: &str) -> String {
    namespace(prefix, "timeline_status/")
}

pub(super) fn timeline_status_index_marker_key(prefix: &str) -> String {
    namespace(prefix, "timeline_status/__ready")
}

pub(super) fn timeline_status_index_rebuild_lock_key(prefix: &str) -> String {
    // Keep the lock outside timeline_status/ because rebuilding deletes that entire prefix.
    namespace(prefix, "cluster/timeline_status_index_rebuild_lock")
}

pub(super) fn timeline_status_owner_index_key(
    prefix: &str,
    owner_worker_endpoint: &str,
    timeline_key: &str,
) -> String {
    namespace(
        prefix,
        &format!(
            "timeline_status/by_owner/{}/{}",
            encode_component(&normalize_endpoint_component(owner_worker_endpoint)),
            timeline_key
        ),
    )
}

pub(super) fn timeline_status_owner_index_prefix(
    prefix: &str,
    owner_worker_endpoint: &str,
) -> String {
    namespace(
        prefix,
        &format!(
            "timeline_status/by_owner/{}/",
            encode_component(&normalize_endpoint_component(owner_worker_endpoint))
        ),
    )
}

pub(super) fn timeline_status_state_index_key(
    prefix: &str,
    state: &str,
    timeline_key: &str,
) -> String {
    namespace(
        prefix,
        &format!("timeline_status/by_state/{state}/{timeline_key}"),
    )
}

pub(super) fn timeline_status_state_index_prefix(prefix: &str, state: &str) -> String {
    namespace(prefix, &format!("timeline_status/by_state/{state}/"))
}

pub(super) fn request_key(prefix: &str, timeline_key: &str, client_request_id: &str) -> String {
    namespace(
        prefix,
        &format!(
            "requests_v2/{}/{}",
            encode_component(timeline_key),
            encode_component(client_request_id)
        ),
    )
}

pub(super) fn request_prefix(prefix: &str) -> String {
    namespace(prefix, "requests_v2/")
}

pub(super) fn legacy_request_key(
    prefix: &str,
    timeline_key: &str,
    client_request_id: &str,
) -> String {
    namespace(
        prefix,
        &format!("requests/{timeline_key}/{client_request_id}"),
    )
}

pub(super) fn legacy_request_prefix(prefix: &str) -> String {
    namespace(prefix, "requests/")
}

pub(super) fn legacy_request_key_is_unambiguous(
    timeline_key: &str,
    client_request_id: &str,
) -> bool {
    !timeline_key.contains('/') && !client_request_id.contains('/')
}

pub(super) fn request_cleanup_index_key(
    prefix: &str,
    updated_at_ms: u64,
    timeline_key: &str,
    client_request_id: &str,
) -> String {
    namespace(
        prefix,
        &format!(
            "request_cleanup_v2/{updated_at_ms:020}/{}/{}",
            encode_component(timeline_key),
            encode_component(client_request_id)
        ),
    )
}

pub(super) fn request_cleanup_index_prefix(prefix: &str) -> String {
    namespace(prefix, "request_cleanup_v2/")
}

pub(super) fn request_cleanup_index_cutoff(prefix: &str, older_than_ms: u64) -> String {
    namespace(prefix, &format!("request_cleanup_v2/{older_than_ms:020}/"))
}

pub(super) fn legacy_request_cleanup_index_key(
    prefix: &str,
    updated_at_ms: u64,
    timeline_key: &str,
    client_request_id: &str,
) -> String {
    namespace(
        prefix,
        &format!("request_cleanup/{updated_at_ms:020}/{timeline_key}/{client_request_id}"),
    )
}

pub(super) fn legacy_request_cleanup_index_prefix(prefix: &str) -> String {
    namespace(prefix, "request_cleanup/")
}

pub(super) fn legacy_request_cleanup_index_cutoff(prefix: &str, older_than_ms: u64) -> String {
    namespace(prefix, &format!("request_cleanup/{older_than_ms:020}/"))
}

pub(super) fn instance_identity_key(prefix: &str, instance_id: &str) -> String {
    namespace(prefix, &format!("identity/instances/{instance_id}"))
}

pub(super) fn instance_identity_prefix(prefix: &str) -> String {
    namespace(prefix, "identity/instances/")
}

pub(super) fn ownership_plan_key(prefix: &str) -> String {
    namespace(prefix, "cluster/ownership_plan")
}

pub(super) fn cluster_format_key(prefix: &str) -> String {
    namespace(prefix, "cluster/format_version")
}

pub(super) fn timeline_creation_lock_key(prefix: &str) -> String {
    namespace(prefix, "cluster/timeline_creation_lock")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_keys_encode_each_component_without_cross_timeline_collisions() {
        let first = request_key("/chronos", "a/b", "c");
        let second = request_key("/chronos", "a", "b/c");

        assert_ne!(first, second);
        assert_eq!(
            legacy_request_key("/chronos", "a/b", "c"),
            legacy_request_key("/chronos", "a", "b/c")
        );
        assert!(!legacy_request_key_is_unambiguous("a/b", "c"));
        assert!(legacy_request_key_is_unambiguous("a", "c"));
    }

    #[test]
    fn cleanup_index_keys_encode_external_components() {
        assert_ne!(
            request_cleanup_index_key("", 42, "a/b", "c"),
            request_cleanup_index_key("", 42, "a", "b/c")
        );
    }

    #[test]
    fn status_index_rebuild_lock_is_outside_the_deleted_index_prefix() {
        assert!(!timeline_status_index_rebuild_lock_key("/chronos")
            .starts_with(&timeline_status_index_prefix("/chronos")));
    }
}
