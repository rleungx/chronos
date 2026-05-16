pub(crate) fn normalize_endpoint(endpoint: &str) -> String {
    let endpoint = endpoint.trim();
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_owned()
    } else {
        format!("http://{endpoint}")
    }
}

pub(crate) fn normalize_endpoint_or_fallback(endpoint: &str, fallback_endpoint: &str) -> String {
    let endpoint = endpoint.trim();
    if endpoint.is_empty() {
        fallback_endpoint.to_owned()
    } else {
        normalize_endpoint(endpoint)
    }
}
