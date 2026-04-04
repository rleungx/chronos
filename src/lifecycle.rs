use crate::TimelineLifecycleState;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimelineServingReadiness {
    Serving,
    RequiresActivation,
    NotReady,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimelinePublicUnavailability {
    TemporarilyUnavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimelineLifecycleContract {
    state: TimelineLifecycleState,
}

impl TimelineLifecycleContract {
    pub fn classify(state: TimelineLifecycleState) -> Self {
        Self { state }
    }

    pub fn state(self) -> TimelineLifecycleState {
        self.state
    }

    pub fn serving_readiness(self) -> TimelineServingReadiness {
        match self.state {
            TimelineLifecycleState::Active => TimelineServingReadiness::Serving,
            TimelineLifecycleState::Recovering => TimelineServingReadiness::RequiresActivation,
            TimelineLifecycleState::Creating
            | TimelineLifecycleState::Draining
            | TimelineLifecycleState::Locked => TimelineServingReadiness::NotReady,
        }
    }

    pub fn should_activate_locally(self) -> bool {
        matches!(
            self.serving_readiness(),
            TimelineServingReadiness::RequiresActivation
        )
    }

    pub fn direct_public_unavailability(self) -> Option<TimelinePublicUnavailability> {
        match self.serving_readiness() {
            TimelineServingReadiness::NotReady => {
                Some(TimelinePublicUnavailability::TemporarilyUnavailable)
            }
            TimelineServingReadiness::Serving | TimelineServingReadiness::RequiresActivation => {
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::TimelineLifecycleState;

    use super::{
        TimelineLifecycleContract, TimelinePublicUnavailability, TimelineServingReadiness,
    };

    #[test]
    fn lifecycle_contract_freezes_current_state_table() {
        let cases = [
            (
                TimelineLifecycleState::Active,
                TimelineServingReadiness::Serving,
                false,
                None,
            ),
            (
                TimelineLifecycleState::Recovering,
                TimelineServingReadiness::RequiresActivation,
                true,
                None,
            ),
            (
                TimelineLifecycleState::Creating,
                TimelineServingReadiness::NotReady,
                false,
                Some(TimelinePublicUnavailability::TemporarilyUnavailable),
            ),
            (
                TimelineLifecycleState::Draining,
                TimelineServingReadiness::NotReady,
                false,
                Some(TimelinePublicUnavailability::TemporarilyUnavailable),
            ),
            (
                TimelineLifecycleState::Locked,
                TimelineServingReadiness::NotReady,
                false,
                Some(TimelinePublicUnavailability::TemporarilyUnavailable),
            ),
        ];

        for (state, readiness, should_activate, public_unavailability) in cases {
            let contract = TimelineLifecycleContract::classify(state);
            assert_eq!(contract.state(), state);
            assert_eq!(contract.serving_readiness(), readiness);
            assert_eq!(contract.should_activate_locally(), should_activate);
            assert_eq!(
                contract.direct_public_unavailability(),
                public_unavailability
            );
        }
    }
}
