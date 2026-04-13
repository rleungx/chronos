pub(super) fn timeline_key(prefix: &str, timeline_key: &str) -> String {
    format!("{}/routes/{}", prefix, timeline_key)
}

pub(super) fn generator_key(prefix: &str, generator_id: u32) -> String {
    format!("{}/generators/{}", prefix, generator_id)
}

pub(super) fn route_prefix(prefix: &str) -> String {
    format!("{}/routes/", prefix)
}

pub(super) fn instance_identity_key(prefix: &str, instance_id: &str) -> String {
    format!("{}/identity/instances/{}", prefix, instance_id)
}
