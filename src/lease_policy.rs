pub(crate) fn lease_expired_with_safety_gap(
    lease_expire_at_ms: u64,
    now_ms: u64,
    safety_gap_ms: u64,
) -> bool {
    lease_expire_at_ms.saturating_add(safety_gap_ms) <= now_ms
}

#[cfg(test)]
mod tests {
    use super::lease_expired_with_safety_gap;

    #[test]
    fn lease_expiry_boundary_is_inclusive_of_safety_gap() {
        assert!(!lease_expired_with_safety_gap(100, 104, 5));
        assert!(lease_expired_with_safety_gap(100, 105, 5));
    }

    #[test]
    fn lease_expiry_check_saturates_at_u64_max() {
        assert!(!lease_expired_with_safety_gap(
            u64::MAX - 2,
            u64::MAX - 1,
            5
        ));
        assert!(lease_expired_with_safety_gap(u64::MAX - 2, u64::MAX, 5));
    }
}
