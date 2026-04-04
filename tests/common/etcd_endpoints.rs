pub fn test_etcd_endpoints_csv() -> String {
    std::env::var("CHRONOS_TEST_ETCD_ENDPOINTS").unwrap_or_else(|_| "127.0.0.1:2379".into())
}

pub fn test_etcd_endpoints() -> Vec<String> {
    test_etcd_endpoints_csv()
        .split(',')
        .map(|endpoint| endpoint.trim().to_string())
        .filter(|endpoint| !endpoint.is_empty())
        .collect()
}
