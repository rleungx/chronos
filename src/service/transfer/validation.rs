use crate::TsoError;

pub(super) fn validate_transfer_target(
    local_advertise_endpoint: &str,
    timeline_key: &str,
    target_owner_endpoint: &str,
    target_generator_id: Option<u32>,
) -> Result<(), TsoError> {
    if target_owner_endpoint != local_advertise_endpoint && target_generator_id.is_none() {
        return Err(TsoError::TargetGeneratorIdRequired {
            timeline_key: timeline_key.to_owned(),
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::validate_transfer_target;
    use crate::TsoError;

    #[test]
    fn remote_target_requires_explicit_generator_id() {
        let error =
            validate_transfer_target("worker-a:50051", "timeline-a", "worker-b:50051", None)
                .unwrap_err();

        assert!(matches!(error, TsoError::TargetGeneratorIdRequired { .. }));
    }

    #[test]
    fn local_target_or_explicit_remote_generator_passes_validation() {
        assert!(
            validate_transfer_target("worker-a:50051", "timeline-a", "worker-a:50051", None)
                .is_ok()
        );
        assert!(validate_transfer_target(
            "worker-a:50051",
            "timeline-a",
            "worker-b:50051",
            Some(7),
        )
        .is_ok());
    }
}
