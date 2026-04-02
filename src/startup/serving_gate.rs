#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CriticalStartupListener {
    Admin,
    Grpc,
}

impl CriticalStartupListener {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Admin => "admin",
            Self::Grpc => "grpc",
        }
    }
}

#[derive(Default)]
pub(crate) struct StartupServingGate {
    admin_bound: bool,
    grpc_bound: bool,
}

impl StartupServingGate {
    pub(crate) fn mark_listener_bound(&mut self, listener: CriticalStartupListener) -> bool {
        match listener {
            CriticalStartupListener::Admin => self.admin_bound = true,
            CriticalStartupListener::Grpc => self.grpc_bound = true,
        }
        self.is_ready()
    }

    pub(crate) fn is_ready(&self) -> bool {
        self.admin_bound && self.grpc_bound
    }
}

#[cfg(test)]
mod tests {
    use super::{CriticalStartupListener, StartupServingGate};

    #[test]
    fn startup_serving_gate_requires_all_critical_listeners() {
        let mut gate = StartupServingGate::default();

        assert!(!gate.is_ready());
        assert!(!gate.mark_listener_bound(CriticalStartupListener::Admin));
        assert!(!gate.is_ready());
        assert!(gate.mark_listener_bound(CriticalStartupListener::Grpc));
        assert!(gate.is_ready());
    }

    #[test]
    fn startup_serving_gate_is_order_independent() {
        let mut gate = StartupServingGate::default();

        assert!(!gate.mark_listener_bound(CriticalStartupListener::Grpc));
        assert!(gate.mark_listener_bound(CriticalStartupListener::Admin));
        assert!(gate.is_ready());
    }
}
