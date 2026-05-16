use std::collections::BTreeSet;
use std::env;

use crate::support_endpoint::normalize_endpoint;

pub(crate) fn parse_endpoint_list(key: &str, fallback_endpoint: &str) -> Vec<String> {
    let endpoints = env::var(key)
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|endpoint| !endpoint.is_empty())
        .map(normalize_endpoint)
        .collect::<Vec<_>>();
    if endpoints.is_empty() {
        vec![fallback_endpoint.to_owned()]
    } else {
        endpoints
    }
}

pub(crate) fn parse_endpoint_filter(key: &str) -> BTreeSet<String> {
    env::var(key)
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|endpoint| !endpoint.is_empty())
        .map(normalize_endpoint)
        .collect()
}
