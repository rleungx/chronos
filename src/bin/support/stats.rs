pub(crate) fn percentile(sorted: &[u64], pct: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() - 1) as f64 * pct).round() as usize;
    sorted[idx]
}

#[cfg(test)]
mod tests {
    use super::percentile;

    #[test]
    fn percentile_handles_empty_and_simple_slices() {
        assert_eq!(percentile(&[], 0.95), 0);
        assert_eq!(percentile(&[10, 20, 30], 0.50), 20);
        assert_eq!(percentile(&[10, 20, 30], 0.99), 30);
    }
}
