#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BuildIdentity {
    pub version: &'static str,
    pub commit: &'static str,
}

pub fn build_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

pub fn build_commit() -> &'static str {
    option_env!("CHRONOS_BUILD_COMMIT").unwrap_or("unknown")
}

pub fn build_identity() -> BuildIdentity {
    BuildIdentity {
        version: build_version(),
        commit: build_commit(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_identity_matches_component_accessors() {
        let identity = build_identity();
        assert_eq!(identity.version, build_version());
        assert_eq!(identity.commit, build_commit());
    }
}
